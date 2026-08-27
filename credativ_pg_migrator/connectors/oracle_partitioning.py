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
The partitioning of Oracle, written the way PostgreSQL writes it.

`development/PARTITIONING_STRATEGY.md` §2.4 puts oracle second, behind postgresql, for the
reason §2.1 gives: it is the richest scheme of the twelve sources and the only one where every
case of §4.2 - composite, INTERVAL, REFERENCE, SYSTEM, global indexes - appears in one database.
The connector reads the catalogue; this module is the part which has no driver in it, so that
the translation can be tested on a machine with no Oracle client at all - the same reason
`oracle_query_conversion.py` stands beside the connector rather than inside it.

**What is translated and what is not.** Oracle says `VALUES LESS THAN (x)` and means an
exclusive upper bound over partitions which are contiguous and ordered; PostgreSQL says
`FOR VALUES FROM (a) TO (b)` and means the same thing said twice, so a range scheme maps
exactly - the lower bound of a partition is the upper bound of the one before it, and MINVALUE
opens the first. A LIST scheme maps value for value. A HASH scheme maps in **count only**:
Oracle hashes with its own function and PostgreSQL with its own, so `MODULUS n, REMAINDER i`
gives a table of n partitions whose rows are spread differently. That is not a defect of the
translation - the rows arrive through the parent and PostgreSQL routes each of them - and it is
said out loud, because a reader who believes a partition holds the same rows it held on Oracle
believes something false.

**What has no counterpart at all** raises `UntranslatableScheme`, and the run stops on it
rather than building something which is not what was asked for: REFERENCE partitioning, where
the child's partitions come from the parent's key through a foreign key and have no bound of
their own; SYSTEM partitioning, where the application names the partition on every INSERT and
there is no key; and a bound written as an expression this module does not know. §1 of the
design is why refusing is right: the source scheme is evidence, not a template, and a user who
cannot have it reproduced wants to be told so while it is still free - not after the data is
loaded.

**Sub-partitions are not carried over**, and that is a decision rather than an omission - §2.2.
`PARTITION BY RANGE (order_date) SUBPARTITION BY HASH (customer_id) SUBPARTITIONS 16` over five
years of months is 960 segments, PostgreSQL can express it, and it is almost always the wrong
thing to build: every segment is a relation with its own relcache entry, its own statistics and
its own indexes, autovacuum has 960 tables to think about instead of 60, and the two things the
hash level buys on Oracle - I/O spread across devices and partition-wise joins on the
sub-key - are not what it buys here. The first level is carried over and the run says how many
segments were left behind.
"""

import re

from credativ_pg_migrator import partitioning


class UntranslatableScheme(Exception):
    """An Oracle scheme, or one bound of it, which PostgreSQL cannot be given as it stands."""


## What ALL_PART_TABLES.PARTITIONING_TYPE answers, and what PostgreSQL has for it. RANGE, LIST
## and HASH are the three PostgreSQL has; INTERVAL is a RANGE which Oracle extends by itself,
## and the partitions which exist are ordinary range partitions - what stops is the extending.
## SYSTEM and REFERENCE have no key to migrate at all.
TRANSLATED_METHODS = ('RANGE', 'LIST', 'HASH')

## Oracle writes a DATE bound as a TO_DATE() call carrying its own format model and calendar,
## and a TIMESTAMP bound as a TO_TIMESTAMP() or a TIMESTAMP literal. Only the first argument is
## the value; the rest says how to read it, which PostgreSQL does not need to be told.
DATE_FUNCTIONS = ('TO_DATE', 'TO_TIMESTAMP', 'TO_TIMESTAMP_TZ')

## `TIMESTAMP' 2024-01-01 00:00:00'` and `DATE' 2024-01-01'` - the ANSI literal, which Oracle
## writes for a TIMESTAMP high value on some releases.
TYPED_LITERAL = re.compile(r"(?is)^\s*(DATE|TIMESTAMP)\s*('(?:[^']|'')*')\s*$")

## A call, so that TO_DATE(...) can be told from a number and from a name.
FUNCTION_CALL = re.compile(r"(?is)^\s*([A-Z_][A-Z_0-9$#]*)\s*\((.*)\)\s*$")

## A bare number, which needs nothing done to it.
NUMBER = re.compile(r"(?is)^\s*[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?\s*$")

## A string literal, in Oracle's spelling, which is PostgreSQL's spelling as well - the
## delimiter is the single quote and one inside it is written twice. `N'...'` is the national
## character set literal, and the N is what PostgreSQL does not take.
STRING_LITERAL = re.compile(r"(?is)^\s*[NQ]?('(?:[^']|'')*')\s*$")

## `HEXTORAW('DEADBEEF')` - a RAW bound, which the migration gives a bytea column. PostgreSQL
## reads '\xDEADBEEF' as the same bytes.
HEXTORAW = re.compile(r"(?is)^\s*HEXTORAW\s*\(\s*'([0-9A-F]*)'\s*\)\s*$")

## What Oracle writes where a partition takes everything above the last bound, and what
## PostgreSQL writes for the same thing.
MAXVALUE = 'MAXVALUE'
MINVALUE = 'MINVALUE'

## The word ALL_TAB_PARTITIONS.HIGH_VALUE holds for the list partition which takes every value
## no other partition lists.
DEFAULT_LIST_VALUE = 'DEFAULT'

## PostgreSQL truncates an identifier past this silently, which turns two partitions of a long
## table into one name and a collision. Oracle allows 128 since 12.2.
MAX_IDENTIFIER_LENGTH = partitioning.MAX_IDENTIFIER_LENGTH


def high_value_items(high_value):
    """
    The values of one ALL_TAB_PARTITIONS.HIGH_VALUE, which holds one per partitioning column.

    A composite range key writes them as a list - `10, TO_DATE(' 2024-01-01 ...')` - and the
    commas inside the TO_DATE() call are not separators, which is why this is not a split().
    """
    text = (high_value or '').strip()
    if not text:
        return []
    return [item.strip() for item in partitioning.split_top_level_commas(text) if item.strip()]


def to_postgresql_value(value):
    """
    One Oracle bound value, written as PostgreSQL writes it.

    Raises UntranslatableScheme for anything this module cannot answer for with certainty. A
    bound guessed wrong is a partition which takes rows that belong in the one beside it, and
    nothing later in the run would notice.
    """
    text = (value or '').strip()
    if not text:
        raise UntranslatableScheme('the source catalogue holds no value for this bound')

    upper = text.upper()
    if upper == MAXVALUE:
        return MAXVALUE
    if upper == 'NULL':
        ## Oracle sorts NULL above every value, so a RANGE key which holds NULLs keeps them in
        ## the MAXVALUE partition; a LIST partition may name NULL outright, and PostgreSQL
        ## takes it there since 11. Either way the word is the word.
        return 'NULL'
    if NUMBER.match(text):
        return text.strip()

    literal = STRING_LITERAL.match(text)
    if literal:
        ## N'...' and q'...' carry a prefix PostgreSQL does not take; the literal inside it is
        ## already written the way PostgreSQL writes one.
        return literal.group(1)

    typed = TYPED_LITERAL.match(text)
    if typed:
        return _timestamp_literal(typed.group(2))

    raw = HEXTORAW.match(text)
    if raw:
        ## the migration gives a RAW column a bytea, and '\xDEADBEEF' is how bytea reads hex
        return f"'\\x{raw.group(1)}'"

    call = FUNCTION_CALL.match(text)
    if call:
        name = call.group(1).upper()
        if name in DATE_FUNCTIONS:
            arguments = partitioning.split_top_level_commas(call.group(2))
            if not arguments:
                raise UntranslatableScheme(f"{name}() with no value in it: {text}")
            first = arguments[0].strip()
            if not STRING_LITERAL.match(first):
                raise UntranslatableScheme(
                    f"the bound {text} does not carry its value as a literal, so it cannot be "
                    f"read without asking Oracle to evaluate it")
            return _timestamp_literal(STRING_LITERAL.match(first).group(1))

    raise UntranslatableScheme(
        f"the bound {text} is an Oracle expression this migrator cannot write as a PostgreSQL "
        f"one. Write the partitions out with target_partitioning, or set source_partitioning: "
        f"flatten for this table")


def _timestamp_literal(literal):
    """
    The date or the timestamp inside an Oracle literal, with the space Oracle pads it with
    taken off.

    Oracle writes `TO_DATE(' 2024-01-01 00:00:00', ...)` - the leading blank is the sign
    position of the format model `SYYYY`, and PostgreSQL reads the literal without it. Nothing
    else about the value is touched: it is a date and a time of day in the order PostgreSQL
    reads them in.
    """
    inner = literal[1:-1].replace("''", "'").strip()
    if not inner:
        raise UntranslatableScheme('an empty date literal in the source catalogue')
    return "'" + inner.replace("'", "''") + "'"


def has_time_of_day(bound_value):
    """
    Whether a translated bound carries a time other than midnight.

    An Oracle DATE carries a time and the PostgreSQL `date` the migration gives it does not, so
    a bound of `2024-01-01 06:00:00` becomes `2024-01-01` on the target and the boundary moves
    by six hours. It is a small thing and it is not nothing: the rows of those six hours land
    in the partition below the one they were in.
    """
    text = (bound_value or '').strip().strip("'")
    parts = text.split(' ', 1)
    if len(parts) < 2:
        return False
    ## midnight is written 00:00:00, 00:00:00.000000 or 00:00 - everything in it is a zero, a
    ## colon or a point, and anything else in the time is a time of day
    return bool(parts[1].strip().translate(str.maketrans('', '', '0:.')))


def range_bound(previous_high, this_high, column_count):
    """
    `FOR VALUES FROM (…) TO (…)` for one Oracle range partition.

    previous_high - the HIGH_VALUE of the partition below this one, or None for the first
    this_high     - the HIGH_VALUE of this one
    column_count  - how many columns the key has, which is how many MINVALUEs open the first

    Oracle's ranges are contiguous and ordered by PARTITION_POSITION, and each of them holds
    every value below its own HIGH_VALUE and at or above the one before it. That is what
    PostgreSQL's inclusive FROM and exclusive TO say, so the two are the same scheme written
    twice - not a reading of it.
    """
    upper = [to_postgresql_value(item) for item in high_value_items(this_high)]
    if not upper:
        raise UntranslatableScheme('a range partition with no HIGH_VALUE in the source catalogue')
    if previous_high is None:
        lower = [MINVALUE] * max(column_count or 1, len(upper))
    else:
        lower = [to_postgresql_value(item) for item in high_value_items(previous_high)]
        if not lower:
            raise UntranslatableScheme(
                'the partition below this one has no HIGH_VALUE, so its upper bound - which is '
                'this partition\'s lower bound - is not known')

    _refuse_a_mixed_unbounded_key(lower)
    _refuse_a_mixed_unbounded_key(upper)
    return f"FOR VALUES FROM ({', '.join(lower)}) TO ({', '.join(upper)})"


def _refuse_a_mixed_unbounded_key(values):
    """
    PostgreSQL takes MINVALUE and MAXVALUE only at the end of a bound: once one column of a
    composite key is unbounded every column behind it must be as well, because everything after
    an infinity has no meaning. Oracle allows `VALUES LESS THAN (MAXVALUE, 10)` and means the
    10 to be read; there is no PostgreSQL bound which says that.
    """
    seen_unbounded = False
    for value in values:
        if value in (MINVALUE, MAXVALUE):
            seen_unbounded = True
            continue
        if seen_unbounded:
            raise UntranslatableScheme(
                f"the bound ({', '.join(values)}) writes a value after MAXVALUE or MINVALUE. "
                f"PostgreSQL takes an unbounded column only at the end of a key, so this bound "
                f"has no counterpart")


def list_bound(high_value):
    """`FOR VALUES IN (…)`, or DEFAULT for the partition which takes everything else."""
    text = (high_value or '').strip()
    if text.upper() == DEFAULT_LIST_VALUE:
        return 'DEFAULT'
    values = [to_postgresql_value(item) for item in high_value_items(text)]
    if not values:
        raise UntranslatableScheme('a list partition with no values in the source catalogue')
    return f"FOR VALUES IN ({', '.join(values)})"


def hash_bound(position, count):
    """
    `FOR VALUES WITH (MODULUS n, REMAINDER i)` for the i-th of n hash partitions.

    The count is carried over and the placement is not: Oracle's hash is Oracle's and
    PostgreSQL's is PostgreSQL's, so the same row lands in another partition of the same
    table. Nothing is lost by it - the rows go in through the parent and are routed by the
    target - and the caller says it out loud, because it is the one thing about a preserved
    hash scheme which is not what a reader would assume.
    """
    if not count or count < 1:
        raise UntranslatableScheme('a hash scheme with no partitions in the source catalogue')
    if position is None or position < 0 or position >= count:
        raise UntranslatableScheme(
            f"the hash partition at position {position} of {count} is not one this scheme has")
    return f"FOR VALUES WITH (MODULUS {count}, REMAINDER {position})"


def key_definition(method, columns, name_of_column):
    """
    The `PARTITION BY` clause of the target, in the names the target will have.

    The entry is written in the names Oracle holds - upper case, unquoted - and the clause is
    written in the names this migration gives the columns, which `names_case_handling` decides.
    `name_of_column` is the callable which answers the second from the first.
    """
    if method not in TRANSLATED_METHODS:
        raise UntranslatableScheme(
            f"PostgreSQL partitions by RANGE, LIST and HASH; Oracle's {method} has no "
            f"counterpart among them")
    if not columns:
        raise UntranslatableScheme(f"a {method} scheme whose key columns the catalogue does not hold")
    written = ', '.join(f'"{name_of_column(column)}"' for column in columns)
    return f"{method} ({written})"
