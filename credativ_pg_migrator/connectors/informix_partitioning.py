# credativ-pg-migrator
# Copyright (C) 2025 credativ GmbH
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
The fragmentation of Informix, and the small part of it which is a partitioning.

`development/PARTITIONING_STRATEGY.md` §2.4 puts informix fourth and gives it a verdict none of
the other eleven sources has: *"the case where the honest report is **none of this should be
reproduced**, which is worth more than a translation."* That is the shape of this module. It
reads everything `sysfragments` holds, it says what each strategy really is, and it builds a
PostgreSQL scheme only where the fragments are a range or a list over one column - which is a
minority of Informix schemes and a common enough minority to be worth doing properly.

**Fragmentation is not partitioning, and Informix never claimed it was.** §2.1: four of the
twelve engines say "partition" about where the bytes live, and Informix is the clearest of them.
A table is fragmented across **dbspaces** to spread I/O over devices; the fragment is a place,
not a class of row. PostgreSQL has tablespaces and does not need a partition to use one, and on
modern storage the placement question usually does not arise at all - so an Informix scheme
carried over as it stands produces partitions which exist for a reason which no longer exists.

**The four strategies, and what each of them is:**

  * **ROUND ROBIN** - Informix puts each new row in the next dbspace in turn. There is no key,
    no expression and nothing about a row which decides where it goes. Nothing PostgreSQL could
    be given: it routes a row by its value, and there is no value here.
  * **EXPRESSION** - one arbitrary boolean expression per dbspace, evaluated **in order**, and
    the row goes to the first fragment whose expression is true. This is the one which is
    sometimes a partitioning: `order_date < '2024-01-01'` and the chain after it *is* a range
    scheme, written the way Informix writes one. An expression which is not a range or a list
    over a single column is a rewrite and not a translation, and it is refused.
  * **RANGE / INTERVAL** (12.10 and newer) - the fragments really are ranges, and they map.
  * **LIST** (12.10 and newer) - the fragments really are lists of values, and they map.
  * **HYBRID** - two strategies at once, one inside the other. §2.2 is the argument against
    reproducing a second level at all, and here the second level is a dbspace spread; refused.

**First-match, which is what makes an expression chain readable at all.** Informix evaluates the
fragment expressions in `evalpos` order and stops at the first true one, so

    order_date < '2023-01-01'   IN dbs1,
    order_date < '2024-01-01'   IN dbs2,
    REMAINDER                   IN dbs3

is three contiguous ranges and not three overlapping ones: the second fragment really holds
2023, because the first already took everything below it. Read literally the second expression
covers the first, and copying the pair into PostgreSQL bounds would be refused by the target as
overlapping partitions. So a chain of upper bounds is read as a chain - the lower bound of a
fragment is the upper bound of the one before it, which is the same rule Oracle's `VALUES LESS
THAN` and Db2 z/OS's `ENDING AT` need - and `REMAINDER` becomes the DEFAULT partition.

Where the fragments carry both of their ends, they are taken as written and checked: two which
cannot be shown not to overlap are **refused**, because PostgreSQL refuses overlapping
partitions and the run would fail on the second `CREATE TABLE` rather than here.

Nothing in this module imports a driver, so it can be tested on a machine with no Informix
client - the same reason `oracle_partitioning.py` and `db2_partitioning.py` stand beside their
connectors rather than inside them.
"""

import re

from credativ_pg_migrator import partitioning


class UntranslatableScheme(Exception):
    """An Informix scheme, or one fragment of it, which PostgreSQL cannot be given."""


## `sysfragments.strategy`, and what each letter is. The letters are from the documentation of
## the engine and are NOT verified against a live server; what a scheme can be BUILT from is
## decided by the fragment expressions rather than by this letter, so a release which spells one
## of them differently still reads correctly - it is only the wording of the report which would
## be wrong, and an unknown letter says it is unknown.
STRATEGIES = {
    'R': 'ROUND ROBIN',
    'E': 'EXPRESSION',
    'I': 'RANGE INTERVAL',
    'L': 'LIST',
    'H': 'HYBRID',
    'T': 'TABLE',
}

## The strategies which have no key of any kind, so that no expression could be read even in
## principle.
STRATEGIES_WITHOUT_A_KEY = ('R',)

## What Informix writes for the fragment which takes every row no other fragment took.
REMAINDER = 'REMAINDER'

## The types a `date_range` can be counted in on an Informix source.
DATE_RANGE_TYPES = ('DATE', 'DATETIME')

## Informix types which the migration gives a PostgreSQL type with no default operator class of
## its own, so that a column of one cannot be a partition key whatever the entry says. A
## collection becomes an array of text and a row type a composite; BSON and JSON become `jsonb`,
## which has no default btree class either.
TYPES_WITHOUT_AN_OPERATOR_CLASS = ('BSON', 'JSON', 'LIST', 'MULTISET', 'SET', 'ROW',
                                   'COLLECTION', 'IDSSECURITYLABEL')

## One comparison of a fragment expression: `col < value`, `col >= value`, `col = value`. The
## brackets Informix wraps an expression in are taken off by strip_outer_brackets() first and
## not by this - a `\)?` at the end of it eats the closing bracket of `DATE('2024-01-01')` and
## leaves a value which cannot be read.
## Informix normalises the expression it stores, and what it normalises to differs between
## releases - which is why anything this does not match is refused with its text quoted rather
## than read approximately.
COMPARISON = re.compile(
    r"(?is)^\s*([A-Z_][A-Z_0-9$#]*)\s*(<=|>=|<>|!=|<|>|=)\s*(.+?)\s*$")

## `col IN (a, b, c)`
IN_LIST = re.compile(r"(?is)^\s*([A-Z_][A-Z_0-9$#]*)\s+IN\s*\((.*)\)\s*$")

## A literal Informix can hold in a bound, and what PostgreSQL is given for it.
NUMBER = re.compile(r"(?is)^\s*[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?\s*$")
STRING_LITERAL = re.compile(r"(?is)^\s*('(?:[^']|'')*')\s*$")
## `DATE('2024-01-01')`, `DATETIME(2024-01-01 00:00:00) YEAR TO SECOND`, `MDY(1,1,2024)`
DATE_CALL = re.compile(r"(?is)^\s*DATE\s*\(\s*('(?:[^']|'')*')\s*\)\s*$")
DATETIME_LITERAL = re.compile(r"(?is)^\s*DATETIME\s*\(\s*([^)]*?)\s*\)\s*"
                              r"(?:[A-Z]+(?:\(\d+\))?\s+TO\s+[A-Z]+(?:\(\d+\))?)?\s*$")
MDY_CALL = re.compile(r"(?is)^\s*MDY\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)\s*$")

MINVALUE = 'MINVALUE'
MAXVALUE = 'MAXVALUE'


def strip_outer_brackets(text):
    """
    One expression with the brackets Informix wrapped the whole of it in taken off.

    Only the brackets which really do wrap the whole expression: `(a < 1) AND (b > 2)` opens and
    closes twice and keeps both, and `((a < 1))` loses both of its outer pairs. Doing this with
    an optional bracket at each end of the comparison instead is what turned
    `DATE('2024-01-01')` into `DATE('2024-01-01'` - a value which cannot be read, out of a bound
    which was perfectly good.
    """
    written = (text or '').strip()
    while written.startswith('(') and written.endswith(')'):
        depth = 0
        wraps_the_whole = True
        in_literal = False
        for index, char in enumerate(written):
            if in_literal:
                if char == "'":
                    in_literal = False
                continue
            if char == "'":
                in_literal = True
            elif char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
                if depth == 0 and index < len(written) - 1:
                    wraps_the_whole = False
                    break
        if not wraps_the_whole:
            break
        written = written[1:-1].strip()
    return written


def split_top_level_and(text):
    """
    Split an expression on the ANDs which are not inside brackets or a string literal.

    A regular expression which splits on every ` AND ` cuts `region = 'X AND Y'` in half and
    reads the halves as two comparisons which are not there.
    """
    parts = []
    depth = 0
    in_literal = False
    current = []
    index = 0
    upper = text.upper()
    while index < len(text):
        char = text[index]
        if in_literal:
            current.append(char)
            if char == "'":
                in_literal = False
            index += 1
            continue
        if char == "'":
            in_literal = True
            current.append(char)
        elif char == '(':
            depth += 1
            current.append(char)
        elif char == ')':
            depth -= 1
            current.append(char)
        elif (depth == 0 and upper.startswith('AND', index)
              and (index == 0 or not text[index - 1].isalnum() and text[index - 1] != '_')
              and (index + 3 >= len(text) or not text[index + 3].isalnum() and text[index + 3] != '_')):
            parts.append(''.join(current))
            current = []
            index += 3
            continue
        else:
            current.append(char)
        index += 1
    parts.append(''.join(current))
    return [part.strip() for part in parts if part.strip()]


class Fragment:
    """One fragment of an Informix table, and what it turns out to be."""

    __slots__ = ('name', 'dbspace', 'expression', 'rows', 'evalpos', 'is_remainder',
                 'column', 'low', 'low_inclusive', 'high', 'values', 'target_bound')

    def __init__(self, name, dbspace='', expression='', rows=None, evalpos=0,
                 is_remainder=False):
        self.name = name
        self.dbspace = dbspace
        ## the expression as sysfragments holds it - reported, and the thing which is read
        self.expression = (expression or '').strip()
        ## sysfragments.nrows, which is what UPDATE STATISTICS last gathered
        self.rows = rows
        self.evalpos = evalpos
        self.is_remainder = is_remainder
        ## filled by read_fragment() where the expression turns out to be a range or a list
        self.column = ''
        self.low = None
        self.low_inclusive = True
        self.high = None
        self.values = None
        self.target_bound = ''

    def __repr__(self):
        return f'Fragment({self.name!r} {self.expression!r})'


def to_postgresql_value(value):
    """
    One value out of an Informix fragment expression, written as PostgreSQL writes it.

    Only a literal. `TODAY`, `CURRENT`, a column and a function call are all refused: a bound
    which has to be evaluated is a boundary which moves, and a partition whose boundary moves is
    not the partition the source had.
    """
    text = (value or '').strip()
    if not text:
        raise UntranslatableScheme('an empty value in a fragment expression')
    if NUMBER.match(text):
        return text
    literal = STRING_LITERAL.match(text)
    if literal:
        return literal.group(1)
    call = DATE_CALL.match(text)
    if call:
        return call.group(1)
    made = MDY_CALL.match(text)
    if made:
        month, day, year = (int(part) for part in made.groups())
        return f"'{year:04d}-{month:02d}-{day:02d}'"
    stamp = DATETIME_LITERAL.match(text)
    if stamp:
        inner = stamp.group(1).strip().strip("'")
        if not inner:
            raise UntranslatableScheme(f"an empty DATETIME in {text}")
        return "'" + inner.replace("'", "''") + "'"
    raise UntranslatableScheme(
        f"the value {text} is not a literal this migrator can write as a PostgreSQL bound. A "
        f"bound which has to be evaluated - TODAY, CURRENT, a function of the row - is a "
        f"boundary which moves, and it is not carried over")


def compare_values(left, right):
    """
    Which of two translated bounds is the lower, or None where it cannot be said.

    Two numbers are compared as numbers and two literals as text, which is exactly right for the
    ISO dates and the timestamps a fragmentation is usually written over and is the collation of
    nobody's database for arbitrary text. None is what an unanswerable comparison gives, and the
    caller refuses the scheme rather than assuming either way: PostgreSQL refuses overlapping
    partitions, and finding that out on the second CREATE TABLE of the run is what this exists
    to prevent.
    """
    if left in (MINVALUE, MAXVALUE) or right in (MINVALUE, MAXVALUE):
        return None
    if NUMBER.match(left) and NUMBER.match(right):
        return (float(left) > float(right)) - (float(left) < float(right))
    if left.startswith("'") and right.startswith("'"):
        one, other = left.strip("'"), right.strip("'")
        return (one > other) - (one < other)
    return None


def read_fragment(fragment):
    """
    What one fragment expression really is: a range over one column, a list over one column, the
    remainder, or something this migrator will not read.

    Fills the fragment in place and raises for anything it does not fully understand - with the
    expression quoted, so that whoever reads the message can see what was refused rather than
    being told that something was.
    """
    if fragment.is_remainder or not fragment.expression \
            or fragment.expression.strip().upper() == REMAINDER:
        fragment.is_remainder = True
        return fragment

    written = strip_outer_brackets(fragment.expression)
    values = IN_LIST.match(written)
    if values:
        fragment.column = values.group(1)
        fragment.values = [to_postgresql_value(item)
                           for item in partitioning.split_top_level_commas(values.group(2))
                           if item.strip()]
        if not fragment.values:
            raise UntranslatableScheme(f"the fragment expression {fragment.expression} lists no value")
        return fragment

    parts = split_top_level_and(written)
    if len(parts) > 2:
        raise UntranslatableScheme(
            f"the fragment expression {fragment.expression} is a condition of more than two "
            f"comparisons. PostgreSQL takes a range or a list and nothing else, so a scheme "
            f"written this way is a rewrite rather than a translation")
    for part in parts:
        found = COMPARISON.match(strip_outer_brackets(part))
        if not found:
            raise UntranslatableScheme(
                f"the fragment expression {fragment.expression} is not a range or a list over "
                f"one column. PostgreSQL partitions by a range of values, by a list of them or "
                f"by a hash, and an arbitrary boolean expression is none of the three")
        column, operator, value = found.group(1), found.group(2), found.group(3)
        if fragment.column and column != fragment.column:
            raise UntranslatableScheme(
                f"the fragment expression {fragment.expression} names more than one column. "
                f"A range over several columns has to be converted end by end and is not "
                f"carried over")
        fragment.column = column
        written = to_postgresql_value(value)
        if operator in ('<', '<='):
            if operator == '<=':
                raise UntranslatableScheme(
                    f"the fragment expression {fragment.expression} ends at a value INCLUSIVE "
                    f"(<=), and PostgreSQL's upper bound is always exclusive. Converting it "
                    f"needs the next value of the type, which a fragment expression does not "
                    f"say - write the scheme with target_partitioning")
            fragment.high = written
        elif operator in ('>', '>='):
            fragment.low = written
            fragment.low_inclusive = operator == '>='
        elif operator == '=':
            fragment.values = [written]
        else:
            raise UntranslatableScheme(
                f"the fragment expression {fragment.expression} compares with {operator}, which "
                f"is not a bound of a range and not a member of a list")
    if fragment.low is not None and not fragment.low_inclusive:
        raise UntranslatableScheme(
            f"the fragment expression {fragment.expression} starts AFTER a value (>), and "
            f"PostgreSQL's lower bound is always inclusive. Converting it needs the next value "
            f"of the type, which a fragment expression does not say")
    return fragment


def build_scheme(table_name, strategy, fragments, name_of_column):
    """
    What the target is given for an Informix fragmentation - or why it is given nothing.

    Returns (method, columns, notes, blockers). Every fragment which can be built carries its
    `target_bound` when this returns; a scheme with any blocker builds nothing, because half a
    partitioning is a table which refuses the rows of the half which is missing.
    """
    notes = []
    blockers = []

    if strategy in STRATEGIES_WITHOUT_A_KEY:
        return '', [], notes, [
            f"{table_name} is fragmented BY ROUND ROBIN on the source: Informix puts each new "
            f"row in the next dbspace in turn, to spread the writes over devices. There is no "
            f"partitioning key, no expression and nothing about a row which decides where it "
            f"goes - PostgreSQL routes a row by its value, and there is no value here. Set "
            f"source_partitioning: flatten for this table, or write a scheme of your own with "
            f"target_partitioning"]
    if strategy == 'H':
        return '', [], notes, [
            f"{table_name} is fragmented by a HYBRID scheme on the source - two strategies at "
            f"once, one inside the other. Only the outer one could be carried over and the "
            f"inner one is a spread over dbspaces, which PostgreSQL has no counterpart for. Set "
            f"source_partitioning: flatten for this table, or write a scheme of your own with "
            f"target_partitioning"]

    for fragment in fragments:
        try:
            read_fragment(fragment)
        except UntranslatableScheme as e:
            blockers.append(f"the fragment {fragment.name} of {table_name} cannot be carried "
                            f"over: {e}")
    if blockers:
        return '', [], notes, blockers

    real = [fragment for fragment in fragments if not fragment.is_remainder]
    remainders = [fragment for fragment in fragments if fragment.is_remainder]
    if not real:
        return '', [], notes, [
            f"{table_name} is fragmented on the source and not one of its fragments carries an "
            f"expression this migrator can read, so there is no scheme to build"]
    if len(remainders) > 1:
        return '', [], notes, [
            f"{table_name} has {len(remainders)} fragments with no expression of their own. "
            f"PostgreSQL takes one DEFAULT partition and no more"]

    columns = {fragment.column for fragment in real if fragment.column}
    if len(columns) != 1:
        return '', [], notes, [
            f"{table_name} is fragmented by expressions over {len(columns)} different columns "
            f"({', '.join(sorted(columns)) or 'none this migrator could read'}). PostgreSQL "
            f"partitions by one key, and a scheme whose fragments each test a different column "
            f"is a rewrite rather than a translation"]
    column = next(iter(columns))

    is_list = all(fragment.values is not None for fragment in real)
    is_range = all(fragment.values is None for fragment in real)
    if is_list:
        method = 'LIST'
        blockers.extend(_bound_a_list(table_name, real))
    elif is_range:
        method = 'RANGE'
        blockers.extend(_bound_a_range(table_name, real))
    else:
        return '', [], notes, [
            f"{table_name} is fragmented by expressions of which some are ranges and some are "
            f"lists of values. PostgreSQL partitions by a range or by a list and not by both at "
            f"once, so this scheme is a rewrite rather than a translation"]

    for fragment in remainders:
        fragment.target_bound = 'DEFAULT'
    if remainders:
        notes.append(
            f"the REMAINDER fragment of {table_name} becomes the DEFAULT partition of the "
            f"target, which is the same thing said the other way - and it is worth knowing what "
            f"it costs: attaching a new partition later makes PostgreSQL scan the default "
            f"partition to prove that no row in it belongs in the new one")
    return (method if not blockers else ''), ([column] if not blockers else []), notes, blockers


def _bound_a_list(table_name, fragments):
    """`FOR VALUES IN (…)` per fragment, and the values which are listed twice."""
    blockers = []
    seen = {}
    for fragment in fragments:
        for value in fragment.values:
            if value in seen:
                blockers.append(
                    f"the value {value} is listed by both {seen[value]} and {fragment.name} of "
                    f"{table_name}. Informix takes the first fragment whose expression is true "
                    f"and PostgreSQL refuses two partitions which claim the same value, so this "
                    f"scheme cannot be built as it stands")
                continue
            seen[value] = fragment.name
        fragment.target_bound = f"FOR VALUES IN ({', '.join(fragment.values)})"
    return blockers


def _bound_a_range(table_name, fragments):
    """
    `FOR VALUES FROM (…) TO (…)` per fragment, in the order Informix evaluates them.

    A fragment which writes only its upper end begins where the one before it stopped - Informix
    takes the first fragment whose expression is true, so a chain of `col < v` really is a chain
    of ranges and not a set of overlapping ones. A fragment which writes both of its ends is
    taken as written, and one which cannot be shown not to overlap the one before it is refused.
    """
    blockers = []
    previous_high = None
    for index, fragment in enumerate(fragments):
        if fragment.high is None:
            ## `col >= v` with nothing above it - the last fragment of a chain, open at the top
            fragment.high = MAXVALUE
        low = fragment.low
        if low is None:
            low = previous_high if previous_high is not None else MINVALUE
        elif previous_high is not None:
            order = compare_values(low, previous_high)
            if order is None:
                blockers.append(
                    f"the fragment {fragment.name} of {table_name} starts at {low} and the one "
                    f"before it ends at {previous_high}, and this migrator cannot tell which of "
                    f"the two is the lower - so it cannot show that the two fragments do not "
                    f"overlap. PostgreSQL refuses overlapping partitions; write the scheme with "
                    f"target_partitioning")
                continue
            if order < 0:
                blockers.append(
                    f"the fragment {fragment.name} of {table_name} starts at {low}, below the "
                    f"end of the fragment before it ({previous_high}). Informix takes the first "
                    f"fragment whose expression is true, so the two overlap and only Informix's "
                    f"order says which row goes where - PostgreSQL refuses two partitions which "
                    f"claim the same value")
                continue
        if fragment.high != MAXVALUE and low != MINVALUE:
            order = compare_values(low, fragment.high)
            if order is not None and order >= 0:
                blockers.append(
                    f"the fragment {fragment.name} of {table_name} would be given the empty "
                    f"range {low} .. {fragment.high}, which no row can fall in. Informix has "
                    f"already given its rows to a fragment above it")
                continue
        fragment.target_bound = f"FOR VALUES FROM ({low}) TO ({fragment.high})"
        previous_high = fragment.high
    return blockers


def what_the_fragmentation_is(table_name, strategy, fragments, dbspaces):
    """
    What a reader has to be told about an Informix fragmentation whatever becomes of the table -
    §4.2, and the half of the report a user reads while deciding.

    This is where §2.4's verdict lives: the honest report is usually *"none of this should be
    reproduced"*, and it is worth more than a translation would be.
    """
    notes = []
    name = STRATEGIES.get(strategy, '')
    if not name:
        notes.append(
            f"{table_name} is fragmented on the source by a strategy this migrator does not "
            f"know the name of (sysfragments.strategy is '{strategy}'). What it can be built "
            f"from is read from the fragment expressions themselves, so a scheme which is a "
            f"range or a list is still carried over - and the name of the strategy is reported "
            f"as unknown rather than guessed")
    notes.append(
        f"{table_name} is fragmented on the source: {name or 'strategy ' + str(strategy)}, "
        f"{len(fragments)} fragment(s) over {len(dbspaces)} dbspace(s)"
        + (f" ({', '.join(dbspaces)})" if dbspaces else '')
        + ". Informix fragments a table to spread its I/O over devices, so the fragment is a "
        "place and not a class of row. The placement is NOT carried over - PostgreSQL has "
        "tablespaces and does not need a partition to use one, and every partition of the "
        "target is created in the default tablespace")

    counted = [fragment for fragment in fragments if fragment.rows is not None]
    total = sum(fragment.rows for fragment in counted)
    if counted and total > 0:
        largest = max(counted, key=lambda fragment: fragment.rows)
        share = largest.rows / total
        empty = len([fragment for fragment in counted if fragment.rows == 0])
        if share >= 0.95 and len(counted) > 1:
            notes.append(
                f"{largest.rows} of {total} rows of {table_name} - {share * 100:.0f}% - sit in "
                f"the single fragment {largest.name}. A scheme that skewed prunes nothing: "
                f"almost every query reads almost the whole table whatever it filters on. The "
                f"numbers are what UPDATE STATISTICS last gathered and may be stale")
        if empty:
            notes.append(
                f"{empty} of the {len(counted)} fragments of {table_name} hold no rows at all, "
                f"by the statistics the source has gathered. A scheme with empty fragments is "
                f"usually one built for a retention policy rather than for queries")
    elif not counted:
        notes.append(
            f"the rows per fragment of {table_name} are NOT known - UPDATE STATISTICS has not "
            f"been run for it, so how the rows are spread over the fragments could not be "
            f"reported")
    return notes
