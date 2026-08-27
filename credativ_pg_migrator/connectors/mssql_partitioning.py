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
The partitioning of SQL Server, written the way PostgreSQL writes it.

`development/PARTITIONING_STRATEGY.md` §2.4 puts mssql last of the twelve and calls it *"cheap,
and the LEFT/RIGHT trap is precisely the kind of thing which is silently wrong if nobody looks."*
Both halves held. It is one join over `sys.partition_functions`, `sys.partition_range_values`,
`sys.partition_schemes` and `sys.partitions`, and the whole difficulty of the source is in one
bit of one catalogue row.

**A SQL Server partitioning is not written on the table.** It is a **partition function**, which
says where the boundaries are, and a **partition scheme**, which maps the partitions the function
makes onto filegroups; the table is then created *on* the scheme. Two of those three have a
PostgreSQL counterpart and the third does not - §4.2: *"the ranges are reproducible; the
filegroup placement is not, and is usually not wanted."*

**RANGE LEFT and RANGE RIGHT, which is the trap.** A function with the boundaries b1 < b2 < …
makes one more partition than it has boundaries, and one bit - `boundary_value_on_right` -
decides which side of a boundary the boundary value itself falls on:

| | partition 1 | partition k | last partition |
|---|---|---|---|
| **RANGE RIGHT** | `(-inf, b1)` | `[b(k-1), bk)` | `[bn, +inf)` |
| **RANGE LEFT** | `(-inf, b1]` | `(b(k-1), bk]` | `(bn, +inf)` |

`RANGE RIGHT` is `FROM (a) TO (b)` exactly - inclusive below, exclusive above - and maps with
nothing done to it. **`RANGE LEFT` is the opposite at both ends**, so every bound has to move to
the next value of the type, and where the type has no next value the scheme is refused rather
than shifted by a guess. That is the same conversion Db2's `ENDING AT … INCLUSIVE` and Sybase
ASE's `VALUES <= (x)` need, and the three share the arithmetic
(`partitioning.next_discrete_value()`).

A `RANGE LEFT` function over `datetime` is where this bites hardest and is deliberately refused:
SQL Server's `datetime` is counted in units of 1/300 of a second, which is why the boundaries of
such a scheme are written `'2023-12-31 23:59:59.997'` in the first place, and the next value
after one of those is not something a bound says.

**What SQL Server has and PostgreSQL does not**, each reported per table rather than dropped in
silence: the **filegroups** a scheme maps its partitions onto; the **per-partition compression**
of `sys.partitions.data_compression`; and a **non-aligned** unique index, which is SQL Server's
answer to the same question Oracle answers with a global index - an index which does not contain
the partitioning column and which PostgreSQL cannot have at all.

There is no LIST and no HASH: SQL Server partitions by range and by nothing else.
"""

import datetime
import decimal
import re

from credativ_pg_migrator import partitioning


class UntranslatableScheme(Exception):
    """A SQL Server scheme, or one bound of it, which PostgreSQL cannot be given as it stands."""


## SQL Server partitions by range and by nothing else - there is no LIST and no HASH.
METHOD = 'RANGE'

## The types a `date_range` can be counted in on a SQL Server source.
DATE_RANGE_TYPES = ('DATE', 'DATETIME', 'DATETIME2', 'SMALLDATETIME', 'DATETIMEOFFSET')

## The types whose values have a next one, so that a `RANGE LEFT` bound - which is inclusive
## above - can be given to PostgreSQL, whose upper bound never is.
INTEGER_TYPES = ('TINYINT', 'SMALLINT', 'INT', 'INTEGER', 'BIGINT')
DECIMAL_TYPES = ('DECIMAL', 'NUMERIC')
DATE_TYPES = ('DATE',)

## Types which the migration gives a PostgreSQL type with no default operator class of its own,
## so that a column of one cannot be a partition key whatever the entry says. SQL Server refuses
## most of them in a partition function too, and `target_partitioning` may still name one.
TYPES_WITHOUT_AN_OPERATOR_CLASS = ('XML', 'GEOMETRY', 'GEOGRAPHY', 'HIERARCHYID', 'IMAGE',
                                   'TEXT', 'NTEXT', 'SQL_VARIANT')

MINVALUE = 'MINVALUE'
MAXVALUE = 'MAXVALUE'

## What `sys.partitions.data_compression_desc` says where a partition is not compressed.
NO_COMPRESSION = 'NONE'

NUMBER = re.compile(r"(?is)^\s*[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?\s*$")


def base_type_of(type_name):
    """The name of a type without the length, precision and scale written after it."""
    return re.sub(r'\s*\([^)]*\)', '', str(type_name or '')).strip().upper()


def scale_of(type_name):
    """The scale of a decimal(p,s), or 0 where none is written."""
    found = re.search(r'\(\s*\d+\s*(?:,\s*(\d+)\s*)?\)', str(type_name or ''))
    if not found:
        return 0
    return int(found.group(1)) if found.group(1) else 0


## The types whose literal PostgreSQL takes bare. Everything else is quoted - and which of the
## two a boundary needs is decided by the DECLARED type of the partitioning column rather than
## by what the text looks like, because a `varchar` partition function whose boundaries happen
## to be '100' and '200' is a range over two STRINGS and writing them bare would make it a range
## over two numbers.
BARE_LITERAL_TYPES = INTEGER_TYPES + DECIMAL_TYPES + ('FLOAT', 'REAL', 'MONEY', 'SMALLMONEY',
                                                     'BIT')


def to_postgresql_value(value, type_name=''):
    """
    One boundary value, written as PostgreSQL writes it.

    `sys.partition_range_values.value` is a `sql_variant`. The connector asks the SERVER to
    render it as text - see `_boundary_values()` for why - so what arrives here is normally a
    string, and `type_name` is what says whether it is a number or a literal. A driver which
    answers the value as a Python object is handled too, because one which does is not wrong.
    """
    if value is None:
        raise UntranslatableScheme('the catalogue holds no value for this boundary')
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, (int, decimal.Decimal, float)):
        return str(value)
    if isinstance(value, datetime.datetime):
        return "'" + value.isoformat(sep=' ') + "'"
    if isinstance(value, (datetime.date, datetime.time)):
        return "'" + value.isoformat() + "'"
    if isinstance(value, bytes):
        ## a binary boundary, which the migration gives a bytea column
        return "'\\x" + value.hex().upper() + "'"
    text = str(value).strip()
    if not text:
        raise UntranslatableScheme('the catalogue holds an empty value for this boundary')
    if base_type_of(type_name) in BARE_LITERAL_TYPES:
        if not NUMBER.match(text):
            raise UntranslatableScheme(
                f"the boundary {text} is not a number, and the partitioning column is "
                f"{type_name}")
        return text
    return "'" + text.replace("'", "''") + "'"


def successor(value, type_name):
    """
    The next value of a type after this one - what a `RANGE LEFT` boundary has to become for
    PostgreSQL, whose upper bound is always exclusive.

    The arithmetic is `partitioning.next_discrete_value()`, which Db2 and Sybase ASE share:
    `ENDING AT (x) INCLUSIVE`, `VALUES <= (x)` and a RANGE LEFT boundary are the same sentence in
    three dialects. What is SQL Server's own is which of its type names count as a date and which
    as a whole number - and that `datetime` is not among either, which is the case this refusal
    exists for.
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
                f"the boundary {value} is not a {plain} this migrator can read, so the exclusive "
                f"bound PostgreSQL needs - the value after it - cannot be worked out")
    raise UntranslatableScheme(
        f"the partition function is RANGE LEFT, which puts the boundary value {value} in the "
        f"partition BELOW it - and PostgreSQL's upper bound is always exclusive, so every bound "
        f"of the scheme has to move to the next value of {type_name or 'the column type'}. Only "
        f"a date and a whole number have one: SQL Server counts a datetime in units of 1/300 of "
        f"a second, which is why a RANGE LEFT boundary over one is written '…23:59:59.997', and "
        f"the value after that is not something the bound says. Write the partitions out with "
        f"target_partitioning, or set source_partitioning: flatten for this table")


def range_bounds(boundaries, boundary_value_on_right, type_name):
    """
    `FOR VALUES FROM (…) TO (…)` for every partition a SQL Server function makes.

    boundaries              - the boundary values, in boundary_id order
    boundary_value_on_right - `sys.partition_functions.boundary_value_on_right`: RANGE RIGHT when
                              it is set, RANGE LEFT when it is not
    type_name               - the type of the partitioning column, which decides whether a RANGE
                              LEFT scheme can be converted at all

    A function with n boundaries makes n + 1 partitions, so this answers one more bound than it
    was given values. RANGE RIGHT is `FROM (a) TO (b)` already; RANGE LEFT is the opposite at
    both ends and every bound moves to the next value of the type.
    """
    written = [to_postgresql_value(value, type_name) for value in boundaries]
    if not boundary_value_on_right:
        ## RANGE LEFT: partition k holds (b(k-1), bk], and PostgreSQL says [a, b) - so the value
        ## which really opens partition k is the one after b(k-1), and the one which closes it
        ## is the one after bk
        written = [successor(value, type_name) for value in written]

    bounds = []
    lower = MINVALUE
    for value in written:
        bounds.append(f"FOR VALUES FROM ({lower}) TO ({value})")
        lower = value
    bounds.append(f"FOR VALUES FROM ({lower}) TO ({MAXVALUE})")
    return bounds


def key_definition(columns, name_of_column):
    """The `PARTITION BY` clause of the target, in the names the target will have."""
    if not columns:
        raise UntranslatableScheme(
            'a partitioned table whose partitioning column the catalogue does not name')
    if len(columns) > 1:
        ## SQL Server partitions on exactly one column - a function takes one input - so this is
        ## a shape the catalogue should never answer with
        raise UntranslatableScheme(
            f"a partitioning key over {len(columns)} columns. A SQL Server partition function "
            f"takes exactly one input, so this is not a scheme this migrator can read")
    written = ', '.join(f'"{name_of_column(column)}"' for column in columns)
    return f"{METHOD} ({written})"


def build_scheme(table_name, columns, partitions, boundaries, boundary_value_on_right,
                 type_name, name_of_column):
    """
    The bound of every partition, written the way PostgreSQL writes it, and what has to be said.

    partitions  - [{'name', 'number', 'rows', 'filegroup', 'compression'}] in partition_number
                  order, one per partition the function makes
    boundaries  - the boundary values, in boundary_id order

    Returns (target_key_definition, notes, blockers). Every partition which can be built carries
    its `target_bound` when this returns; a scheme with any blocker builds nothing.
    """
    notes = []
    blockers = []
    target_key_definition = ''
    try:
        target_key_definition = key_definition(columns, name_of_column)
    except UntranslatableScheme as e:
        blockers.append(
            f"{table_name} is partitioned on the source and the same scheme cannot be built on "
            f"PostgreSQL: {e}. Set source_partitioning: flatten for this table, or write a "
            f"scheme of your own with target_partitioning")
        return '', notes, blockers

    if len(partitions) != len(boundaries) + 1:
        ## a function with n boundaries makes n + 1 partitions, and a catalogue which says
        ## otherwise has not been read correctly - building from it would give the target a
        ## different scheme from the one the source has
        blockers.append(
            f"{table_name} has {len(partitions)} partition(s) and a partition function with "
            f"{len(boundaries)} boundary value(s), and a function with n boundaries makes n + 1 "
            f"partitions. The two do not agree, so what this migrator read is not the scheme the "
            f"source has - nothing is built from it")
        return '', notes, blockers

    try:
        bounds = range_bounds(boundaries, boundary_value_on_right, type_name)
    except UntranslatableScheme as e:
        blockers.append(f"the partition function of {table_name} cannot be given to PostgreSQL "
                        f"as it stands: {e}")
        return '', notes, blockers

    for partition, bound in zip(partitions, bounds):
        partition['target_bound'] = bound

    if not boundary_value_on_right:
        notes.append(
            f"the partition function of {table_name} is RANGE LEFT, which puts a boundary value "
            f"in the partition BELOW it - PostgreSQL's ranges are inclusive below and exclusive "
            f"above, which is what RANGE RIGHT means and the opposite of this at both ends. "
            f"Every bound is carried over as the one which holds the same rows: a partition "
            f"ending at 100 becomes one ending before 101. The partitions hold what they held")
    return target_key_definition, notes, blockers


def what_the_scheme_is(table_name, scheme_name, function_name, boundary_value_on_right,
                       columns, partitions, filegroups, unaligned_indexes):
    """
    What a reader has to be told about a SQL Server scheme whatever becomes of the table - §4.2.

    The partitioning is not written on the table at all: it is a function and a scheme, and one
    of the two is about where the bytes live.
    """
    notes = [
        f"{table_name} is partitioned on the source by the partition scheme {scheme_name or '?'} "
        f"over the function {function_name or '?'} - "
        f"{'RANGE RIGHT' if boundary_value_on_right else 'RANGE LEFT'} on "
        f"{', '.join(columns) or 'a column the catalogue does not name'}, "
        f"{len(partitions)} partition(s)"]
    if filegroups:
        notes.append(
            f"the partition scheme of {table_name} maps its partitions onto the filegroup(s) "
            f"{', '.join(filegroups)}. The placement is NOT carried over - PostgreSQL has "
            f"tablespaces and does not need a partition to use one, and every partition of the "
            f"target is created in the default tablespace")
    compressed = sorted({partition['compression'] for partition in partitions
                         if (partition.get('compression') or NO_COMPRESSION).upper() != NO_COMPRESSION})
    if compressed:
        notes.append(
            f"partitions of {table_name} are compressed on the source ({', '.join(compressed)}). "
            f"PostgreSQL compresses a value which does not fit a page and has no per-partition "
            f"compression of its own, so the target holds the same rows in more bytes")
    for index in unaligned_indexes:
        notes.append(
            f"{table_name} carries the NON-ALIGNED index {index['name']}"
            + (' - and it is UNIQUE' if index.get('is_unique') else '')
            + ". A non-aligned index sits on a filegroup of its own rather than on the partition "
              "scheme, which is SQL Server's answer to the question Oracle answers with a global "
              "index - and PostgreSQL has neither: an index on a partitioned table is a "
              "partitioned index, and a unique one must contain every partitioning column"
            + (". If it does not, this run refuses the table before it creates anything"
               if index.get('is_unique') else ''))

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
                f"An empty partition at each end is how a sliding window is kept, and a scheme "
                f"with many of them was built for a retention policy rather than for queries")
    return notes


def what_a_nullable_key_costs(table_name, column, is_nullable):
    """
    The finding a preserved SQL Server scheme has and the other sources hide: SQL Server puts a
    NULL in the lowest partition and PostgreSQL puts it in none.

    A RANGE partition of PostgreSQL takes no NULL - only a DEFAULT partition does, and a scheme
    carried over from a source has none. So the rows holding one are refused, one at a time, in
    the middle of the data migration. Whether there really are any is not something the
    catalogue answers, which is why this is said rather than refused.
    """
    if not is_nullable:
        return []
    return [
        f"{table_name}.{column} is NULLABLE and is the partitioning column. SQL Server puts a "
        f"row whose key is NULL in the lowest partition; PostgreSQL puts it in none at all - a "
        f"RANGE partition takes no NULL and only a DEFAULT partition does, which a scheme "
        f"carried over from a source has not got. If the column really holds a NULL those rows "
        f"cannot be loaded. Write the scheme with target_partitioning and default_partition: "
        f"true, or make the column NOT NULL on the source first"]
