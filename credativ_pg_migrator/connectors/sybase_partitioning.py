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
The semantic partitioning of Sybase ASE, written the way PostgreSQL writes it.

`development/PARTITIONING_STRATEGY.md` §2.4 puts sybase_ase in the last group and says two
things about it which pull in opposite directions: the engagement behind this repository's
query conversion work is a Sybase ASE one, so it is wanted; and *"catalogue names not verified
against a live server"*, so what reads it is written from documentation. That second half is
this module's whole shape, and §0.9 says which parts of it a real ASE still has to confirm.

**What ASE partitions for**, and it is the same answer §2.1 gives for Informix: semantic
partitioning arrived in ASE 15 and spreads a table over **segments** - for I/O across devices
and for parallel scans. The partition is a place first and a class of row second, and
PostgreSQL neither needs nor has that. So a scheme carried over as it stands keeps the pruning
and drops the reason it was built.

**The four methods:**

  * **ROUND ROBIN** - no key at all: ASE puts each row in the next partition in turn. Nothing
    PostgreSQL could be given, and it is refused - the same finding Informix's round robin gets.
  * **RANGE** - `VALUES <= (x)`, which is an **inclusive** upper bound, over partitions which
    are contiguous and ordered. PostgreSQL's `TO (b)` is exclusive, so each end has to be
    converted rather than copied - the same trap Db2 sets with `ENDING AT … INCLUSIVE`, and the
    two share the arithmetic (`partitioning.next_discrete_value()`). Where the column type has
    no next value the scheme is refused rather than moved by a guess.
  * **LIST** - `VALUES ('DE', 'FR')`, value for value. PostgreSQL's LIST key takes exactly one
    column, so a list over more than one is refused.
  * **HASH** - the count carries over and the placement does not: ASE hashes with its own
    function and PostgreSQL with its own.

**What the connector can and cannot read, and why that shapes this module.** The partition
names, the segments and the key columns come out of `syspartitions` and `syspartitionkeys`,
which §2.4 names. The **bounds** do not have a place this migrator can point at with
confidence, so the connector tries for them and reports what it got: a scheme whose bounds were
not read is reported in full - method, key, partitions, segments, rows - and **refused for
building**, never guessed at. That is P2-8 with a sharper edge than usual: here the thing which
was not read is the thing the target would be built from.
"""

import re

from credativ_pg_migrator import partitioning


class UntranslatableScheme(Exception):
    """An ASE scheme, or one bound of it, which PostgreSQL cannot be given as it stands."""


## What this migrator calls each of ASE's four methods, and what PostgreSQL is given for it.
ROUND_ROBIN = 'ROUND ROBIN'
RANGE = 'RANGE'
LIST = 'LIST'
HASH = 'HASH'
## the method could not be told apart, because the bounds could not be read
UNKNOWN = ''

TARGET_METHOD = {RANGE: 'RANGE', LIST: 'LIST', HASH: 'HASH'}

## The types a `date_range` can be counted in on an ASE source.
DATE_RANGE_TYPES = ('DATE', 'DATETIME', 'SMALLDATETIME', 'BIGDATETIME')

## The ASE types whose values have a next one, so that `VALUES <= (x)` can be given to
## PostgreSQL as the `TO (x + 1)` which holds the same rows. `datetime` is deliberately absent:
## ASE counts it in 1/300 of a second and `bigdatetime` in microseconds, and a bound written
## without that precision does not say which of them the next value is.
INTEGER_TYPES = ('INT', 'INTEGER', 'SMALLINT', 'TINYINT', 'BIGINT',
                 'UNSIGNED INT', 'UNSIGNED SMALLINT', 'UNSIGNED BIGINT')
DECIMAL_TYPES = ('DECIMAL', 'NUMERIC')
DATE_TYPES = ('DATE',)

## ASE types which the migration gives a PostgreSQL type with no default operator class of its
## own, so that a column of one cannot be a partition key whatever the entry says.
TYPES_WITHOUT_AN_OPERATOR_CLASS = ('IMAGE', 'TEXT', 'UNITEXT', 'XML')

## What ASE writes as the condition of a partition. The DDL says
## `p1 VALUES <= (100)` for a range and `p1 VALUES ('DE', 'FR')` for a list; the catalogue may
## render either with or without the word VALUES and with or without the brackets.
RANGE_CONDITION = re.compile(r"(?is)^\s*(?:VALUES\s*)?<=\s*\(?(.*?)\)?\s*$")
LIST_CONDITION = re.compile(r"(?is)^\s*(?:VALUES\s*)?(?:IN\s*)?\((.*)\)\s*$")

## What ASE writes for the range partition which takes everything above the last bound.
MAX_VALUE_WORDS = ('MAX', 'MAXVALUE', 'MAX_VALUE')
MAXVALUE = 'MAXVALUE'
MINVALUE = 'MINVALUE'

## A literal a bound can hold. ASE and PostgreSQL spell a number and a string literal the same
## way; ASE also takes a double-quoted string when `quoted_identifier` is off, which PostgreSQL
## reads as an identifier and which is therefore rewritten.
NUMBER = re.compile(r"(?is)^\s*[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?\s*$")
STRING_LITERAL = re.compile(r"(?is)^\s*'((?:[^']|'')*)'\s*$")
DOUBLE_QUOTED = re.compile(r'(?is)^\s*"((?:[^"]|"")*)"\s*$')


def base_type_of(type_name):
    """The name of a type without the length, precision and scale written after it."""
    return re.sub(r'\s*\([^)]*\)', '', str(type_name or '')).strip().upper()


def scale_of(type_name):
    """The scale of a numeric(p,s), or 0 where none is written - which is ASE's own default."""
    found = re.search(r'\(\s*\d+\s*(?:,\s*(\d+)\s*)?\)', str(type_name or ''))
    if not found:
        return 0
    return int(found.group(1)) if found.group(1) else 0


def to_postgresql_value(value):
    """
    One value of an ASE bound, written as PostgreSQL writes it.

    A number and a single-quoted literal are already what PostgreSQL takes. A double-quoted
    string is ASE's when `quoted_identifier` is off, and PostgreSQL would read it as the name of
    a column - so it is rewritten as a literal rather than passed through.
    """
    text = (value or '').strip()
    if not text:
        raise UntranslatableScheme('the source holds no value for this bound')
    upper = text.upper()
    if upper in MAX_VALUE_WORDS:
        return MAXVALUE
    if upper == 'NULL':
        return 'NULL'
    if NUMBER.match(text):
        return text
    literal = STRING_LITERAL.match(text)
    if literal:
        return "'" + literal.group(1) + "'"
    quoted = DOUBLE_QUOTED.match(text)
    if quoted:
        return "'" + quoted.group(1).replace('""', '"').replace("'", "''") + "'"
    raise UntranslatableScheme(
        f"the bound {text} is not a literal, so it cannot be written as a PostgreSQL bound "
        f"without asking the source to evaluate it")


def successor(value, type_name):
    """
    The next value of a type after this one - what an ASE `VALUES <= (x)` has to become for
    PostgreSQL, whose upper bound is always exclusive.

    The arithmetic is `partitioning.next_discrete_value()`, which Db2 shares: `ENDING AT (x)
    INCLUSIVE` and `VALUES <= (x)` are the same sentence in two dialects. What is ASE's own is
    which of its type names count as a date and which as a whole number.
    """
    plain = base_type_of(type_name)
    kind = ''
    if plain in DATE_TYPES:
        kind = partitioning.DISCRETE_DATE
    elif plain in INTEGER_TYPES or (plain in DECIMAL_TYPES and scale_of(type_name) == 0):
        kind = partitioning.DISCRETE_INTEGER
    if kind:
        try:
            return partitioning.next_discrete_value(value, kind)
        except ValueError:
            raise UntranslatableScheme(
                f"the bound {value} is not a {plain} this migrator can read, so the exclusive "
                f"bound PostgreSQL needs - the value after it - cannot be worked out")
    raise UntranslatableScheme(
        f"the partition takes every row with VALUES <= {value}, which puts {value} itself IN it, "
        f"and PostgreSQL's upper bound is always exclusive. Converting it needs the next value "
        f"of {type_name or 'the column type'}, which only a date and a whole number have - a "
        f"{type_name or 'type'} bound moved by a guess is a partition which takes the rows of "
        f"the one beside it. Write the partitions out with target_partitioning, or set "
        f"source_partitioning: flatten for this table")


def method_of(conditions, has_key_columns, conditions_were_read):
    """
    Which of ASE's four methods a table is partitioned by, from what could be read about it.

    ASE keeps the method itself in a place this migrator cannot point at with confidence, so it
    is worked out from two things which it can read:

      * **no partitioning key at all** is ROUND ROBIN, and only ROUND ROBIN - it is the one
        method of the four with no key;
      * a key **and** a condition per partition is RANGE or LIST, told apart by the shape of the
        condition; a key and no condition is HASH, which has none.

    That last step needs the condition read to have *succeeded and answered nothing*, which is a
    different thing from a read which failed - so `conditions_were_read` is passed in rather than
    inferred from an empty list. A method worked out from a read which did not happen would be a
    guess, and a HASH built out of a RANGE nobody could read is the worst of the possible wrong
    answers: it would load every row into the wrong partition and no step of the run would fail.
    """
    if not has_key_columns:
        return ROUND_ROBIN
    if not conditions_were_read:
        return UNKNOWN
    written = [condition for condition in conditions if (condition or '').strip()]
    if not written:
        return HASH
    if all(RANGE_CONDITION.match(condition) for condition in written):
        return RANGE
    if all(LIST_CONDITION.match(condition) for condition in written):
        return LIST
    return UNKNOWN


def range_bound(previous_condition, condition, type_name):
    """
    `FOR VALUES FROM (…) TO (…)` for one ASE range partition.

    ASE writes `VALUES <= (x)` and means that x is in the partition, over partitions which are
    contiguous and ordered - so the lower bound of a partition is the upper bound of the one
    before it, and the upper bound is the value AFTER the one written.
    """
    upper = _range_value(condition)
    if upper != MAXVALUE:
        upper = successor(upper, type_name)
    if previous_condition is None:
        lower = MINVALUE
    else:
        lower = _range_value(previous_condition)
        if lower != MAXVALUE:
            lower = successor(lower, type_name)
    return f"FOR VALUES FROM ({lower}) TO ({upper})"


def _range_value(condition):
    """The value out of one `VALUES <= (x)`, written as PostgreSQL writes it."""
    found = RANGE_CONDITION.match((condition or '').strip())
    if not found:
        raise UntranslatableScheme(
            f"the partition condition {condition} is not a range bound this migrator can read")
    return to_postgresql_value(found.group(1))


def list_bound(condition, column_count):
    """`FOR VALUES IN (…)` for one ASE list partition."""
    if column_count > 1:
        raise UntranslatableScheme(
            'a LIST key over more than one column. PostgreSQL takes exactly one column in a '
            'LIST partition key')
    found = LIST_CONDITION.match((condition or '').strip())
    if not found:
        raise UntranslatableScheme(
            f"the partition condition {condition} is not a list of values this migrator can read")
    values = [to_postgresql_value(item)
              for item in partitioning.split_top_level_commas(found.group(1)) if item.strip()]
    if not values:
        raise UntranslatableScheme('a list partition with no values in the source')
    return f"FOR VALUES IN ({', '.join(values)})"


def hash_bound(position, count):
    """
    `FOR VALUES WITH (MODULUS n, REMAINDER i)` for the i-th of n hash partitions.

    The count carries over and the placement does not: ASE hashes with its own function and
    PostgreSQL with its own.
    """
    if not count or count < 1:
        raise UntranslatableScheme('a hash scheme with no partitions in the source')
    if position is None or position < 0 or position >= count:
        raise UntranslatableScheme(
            f"the hash partition at position {position} of {count} is not one this scheme has")
    return f"FOR VALUES WITH (MODULUS {count}, REMAINDER {position})"


def key_definition(method, columns, name_of_column):
    """The `PARTITION BY` clause of the target, in the names the target will have."""
    target = TARGET_METHOD.get(method)
    if not target:
        raise UntranslatableScheme(
            f"PostgreSQL partitions by RANGE, LIST and HASH; {method or 'this method'} is none "
            f"of the three")
    if not columns:
        raise UntranslatableScheme(f"a {method} scheme whose key columns the source does not hold")
    if target == 'LIST' and len(columns) > 1:
        raise UntranslatableScheme(
            f"a LIST key over {len(columns)} columns. PostgreSQL takes exactly one column in a "
            f"LIST partition key")
    written = ', '.join(f'"{name_of_column(column)}"' for column in columns)
    return f"{target} ({written})"


def build_scheme(table_name, method, columns, partitions, column_types, name_of_column,
                 conditions_were_read=True):
    """
    The bound of every partition, written the way PostgreSQL writes it, and what has to be said.

    partitions - [{'name', 'condition', 'segment', 'rows'}], in partition order

    Returns (target_key_definition, notes, blockers). Every partition which can be built carries
    its `target_bound` when this returns; a scheme with any blocker builds nothing, because half
    a partitioning is a table which refuses the rows of the half which is missing.
    """
    notes = []
    blockers = []

    if method == ROUND_ROBIN:
        return '', notes, [
            f"{table_name} is partitioned BY ROUNDROBIN on the source: ASE puts each row in the "
            f"next partition in turn, to spread the writes over segments. There is no "
            f"partitioning key and nothing about a row which decides where it goes - PostgreSQL "
            f"routes a row by its value, and there is no value here. Set source_partitioning: "
            f"flatten for this table, or write a scheme of your own with target_partitioning"]
    if method == UNKNOWN:
        return '', notes, [
            f"{table_name} is partitioned on the source over {len(partitions)} partition(s) and "
            f"this migrator could not read which method it uses"
            + ('' if conditions_were_read else
               ' - the conditions of its partitions could not be read out of this server')
            + f". RANGE, LIST and HASH are built from three different things, so building one of "
              f"them out of a scheme which might be another would put every row in the wrong "
              f"partition without a single step of the run failing. Set source_partitioning: "
              f"flatten for this table, or write the scheme out with target_partitioning"]

    target_key_definition = ''
    try:
        target_key_definition = key_definition(method, columns, name_of_column)
    except UntranslatableScheme as e:
        blockers.append(
            f"{table_name} is partitioned by {method} on the source and the same scheme cannot "
            f"be built on PostgreSQL: {e}. Set source_partitioning: flatten for this table, or "
            f"write a scheme of your own with target_partitioning")
        return '', notes, blockers

    type_name = column_types.get(columns[0], '') if columns else ''
    previous = None
    inclusive_ends = 0
    for position, partition in enumerate(partitions):
        try:
            if method == RANGE:
                lower, previous = previous, partition['condition']
                partition['target_bound'] = range_bound(lower, partition['condition'], type_name)
                if _range_value(partition['condition']) != MAXVALUE:
                    inclusive_ends += 1
            elif method == LIST:
                partition['target_bound'] = list_bound(partition['condition'], len(columns))
            else:
                partition['target_bound'] = hash_bound(position, len(partitions))
        except UntranslatableScheme as e:
            blockers.append(
                f"the partition {partition['name']} of {table_name} cannot be given to "
                f"PostgreSQL as it stands: {e}")

    if method == HASH and not blockers:
        notes.append(
            f"{table_name} is partitioned by HASH into {len(partitions)} on the source, and the "
            f"target is given {len(partitions)} hash partitions over the same column(s) - but "
            f"ASE hashes with its own function and PostgreSQL with its own, so a row which sat "
            f"in one partition on the source sits in another here. Nothing is lost by it: the "
            f"rows go in through the parent and the target routes each of them")
    if method == RANGE and inclusive_ends and not blockers:
        notes.append(
            f"{inclusive_ends} partition(s) of {table_name} are written VALUES <= a value, which "
            f"puts that value IN the partition and which PostgreSQL cannot say - its upper bound "
            f"is always exclusive. Each of them is carried over as the exclusive bound which "
            f"holds the same rows: VALUES <= (100) becomes TO (101). The partitions hold what "
            f"they held")
    if method == RANGE and partitions and not blockers:
        last = partitions[-1]
        if _range_value(last['condition']) != MAXVALUE:
            notes.append(
                f"the partitions of {table_name} end at {last['name']}, whose bound is "
                f"{last.get('target_bound', '')}. The source has no MAX partition and the target "
                f"is given none either, so a row past that bound is refused - which is what the "
                f"source does today as well. Write a target_partitioning entry with a future: "
                f"window if the migration is the moment to change that")
    if blockers:
        return '', notes, blockers
    return target_key_definition, notes, blockers


def what_the_partitioning_is(table_name, method, partitions, segments, conditions_were_read):
    """
    What a reader has to be told about an ASE scheme whatever becomes of the table - §4.2.

    ASE's semantic partitioning spreads a table over segments for I/O and for parallel scans, so
    the placement is half the reason it exists and none of it is carried over.
    """
    notes = []
    written = method or 'a method this migrator could not tell apart'
    notes.append(
        f"{table_name} is partitioned on the source: {written}, {len(partitions)} partition(s) "
        f"over {len(segments)} segment(s)" + (f" ({', '.join(segments)})" if segments else '')
        + ". ASE spreads a table over segments for I/O across devices and for parallel scans, "
          "so where a partition sits is half the reason it exists. The placement is NOT carried "
          "over - PostgreSQL has tablespaces and does not need a partition to use one, and every "
          "partition of the target is created in the default tablespace")
    if not conditions_were_read:
        notes.append(
            f"the conditions of the partitions of {table_name} could NOT be read from this "
            f"server, so what each partition holds is not known - only how many there are, what "
            f"they are called and which segments they sit in. Reading them is the part of this "
            f"connector written from the documentation of ASE rather than against a live server "
            f"(§2.4 of development/PARTITIONING_STRATEGY.md says so of its catalogue names)")

    counted = [partition for partition in partitions if partition.get('rows') is not None]
    total = sum(partition['rows'] for partition in counted)
    if counted and total > 0 and len(counted) > 1:
        largest = max(counted, key=lambda partition: partition['rows'])
        share = largest['rows'] / total
        if share >= 0.95:
            notes.append(
                f"{largest['rows']} of {total} rows of {table_name} - {share * 100:.0f}% - sit "
                f"in the single partition {largest['name']}. A scheme that skewed prunes "
                f"nothing: almost every query reads almost the whole table whatever it filters "
                f"on")
        empty = len([partition for partition in counted if partition['rows'] == 0])
        if empty:
            notes.append(
                f"{empty} of the {len(counted)} partitions of {table_name} hold no rows at all. "
                f"A scheme with empty partitions is usually one built for a retention policy "
                f"rather than for queries")
    elif not counted:
        notes.append(
            f"the rows per partition of {table_name} are NOT known, so how the rows are spread "
            f"over the partitions could not be reported")
    return notes
