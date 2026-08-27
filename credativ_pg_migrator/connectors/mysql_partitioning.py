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
The partitioning of MySQL and MariaDB, written the way PostgreSQL writes it.

`development/PARTITIONING_STRATEGY.md` §2.4: *"one query, and one implementation for both - the
two are one dialect, as `mysql_query_conversion.py` already is."* So this is one module, mixed
into both connectors, and everything it reads comes out of `information_schema.PARTITIONS`,
which holds the whole scheme: the method, the expression, the bounds of every partition and the
rows in it. Nothing here imports a driver.

**The four methods, and what becomes of each:**

| | |
|---|---|
| `RANGE`, `RANGE COLUMNS` | `VALUES LESS THAN (x)` is an exclusive upper bound over partitions which are contiguous and ordered - the same thing PostgreSQL says with `FROM (a) TO (b)`, so it maps exactly, `MAXVALUE` included |
| `LIST`, `LIST COLUMNS` | value for value. PostgreSQL takes exactly **one** column in a LIST key, so `LIST COLUMNS` over several is refused |
| `HASH`, `LINEAR HASH` | the **count** maps and the placement does not |
| `KEY`, `LINEAR KEY` | the same, and §2.4 says of it that it "has no counterpart at all" - see below |

**The hash which is not the same hash, and it is the loudest thing to say about this source.**
MySQL's `HASH` hashes an integer *expression* with MySQL's own function, `LINEAR HASH` uses a
power-of-two variant of it, and `KEY` uses the internal hashing of the storage engine over any
type at all. PostgreSQL hashes the column value with **its** own function. The same column and
the same partition count therefore do **not** put the same rows in the same partition. Nothing is
lost by that - the rows go in through the parent and the target routes each of them - but a
reader who believes a partition holds the rows it held on MySQL believes something false, and
that sentence is written per table.

§2.4 lists `KEY` as the method with no counterpart while `HASH` is one of the three which have
one. Building this made the distinction look thinner than it reads: both carry over as *n hash
partitions over the same columns*, and for both the placement of a row is another one. So they
are treated alike here, and §0.8 records the departure rather than leaving it to be discovered.
What `KEY` really has of its own is `PARTITION BY KEY()` with **no column list**, which means
"the primary key" and which `information_schema` writes as no expression at all - that one is
refused, because a key nobody wrote down is a key this migrator would have to guess.

**A partitioning expression which is not a bare column is refused**, and the reason is not that
PostgreSQL cannot express it. It can: `PARTITION BY RANGE (EXTRACT(YEAR FROM hired))` is a legal
partitioned table. What it cannot then have is a **primary key or a unique constraint of any
kind** - PostgreSQL requires every unique constraint to contain all partitioning *columns*, and
no constraint can contain an expression. MySQL's own rule is the opposite way round and lets
`PARTITION BY RANGE (YEAR(hired))` sit happily beside `PRIMARY KEY (id, hired)`, so the tables
which use this are exactly the tables which have a key to lose. The message names the two ways
out, and one of them is short: `PARTITION BY RANGE (YEAR(hired))` over yearly bounds is what
`target_partitioning` with `partitioning_columns: hired` and `date_range: year` builds.

**Sub-partitions are not carried over** - §2.2, and the same decision as for Oracle. MySQL
sub-partitions a RANGE or LIST scheme by HASH or KEY; the first level is built and the run says
how many segments were left behind.
"""

import re

from credativ_pg_migrator import partitioning


class UntranslatableScheme(Exception):
    """A MySQL scheme, or one bound of it, which PostgreSQL cannot be given as it stands."""


## `information_schema.PARTITIONS.PARTITION_METHOD`, and what PostgreSQL is given for it.
RANGE_METHODS = ('RANGE', 'RANGE COLUMNS')
LIST_METHODS = ('LIST', 'LIST COLUMNS')
HASH_METHODS = ('HASH', 'LINEAR HASH', 'KEY', 'LINEAR KEY')

## `RANGE COLUMNS`, `LIST COLUMNS` and `KEY` write their key as a list of bare columns; `RANGE`,
## `LIST` and `HASH` write one expression, which is a bare column often enough to be worth
## reading and is not always one. partition_key_columns() reads both the same way, because a
## list of one bare column and a bare column are the same text.

## What MySQL writes for the partition which takes everything above the last bound.
MAXVALUE = 'MAXVALUE'
MINVALUE = 'MINVALUE'

## The types a `date_range` can be counted in on a MySQL or MariaDB source.
DATE_RANGE_TYPES = ('DATE', 'DATETIME', 'TIMESTAMP')

## Types which the migration gives a PostgreSQL type with no default operator class of its own,
## so that a column of one cannot be a partition key whatever the entry says. JSON becomes
## `jsonb`; a spatial type becomes `point` and the rest of them arrive as text.
TYPES_WITHOUT_AN_OPERATOR_CLASS = ('JSON', 'POINT', 'GEOMETRY', 'LINESTRING', 'POLYGON',
                                   'MULTIPOINT', 'MULTILINESTRING', 'MULTIPOLYGON',
                                   'GEOMETRYCOLLECTION')

## A bare column of a partitioning expression, however MySQL delimited it: `` `store_id` `` or
## `store_id`. Anything else - a call, an arithmetic, a cast - is an expression and is refused.
BARE_COLUMN = re.compile(r"(?is)^\s*(?:`([^`]+)`|([A-Z_][A-Z_0-9$]*))\s*$")

## A literal a bound can hold. MySQL and PostgreSQL spell both the same way.
NUMBER = re.compile(r"(?is)^\s*[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?\s*$")
STRING_LITERAL = re.compile(r"(?is)^\s*('(?:[^']|'')*')\s*$")


def partition_key_columns(method, expression):
    """
    The columns of a partitioning key, out of `PARTITION_EXPRESSION`.

    `RANGE COLUMNS`, `LIST COLUMNS` and `KEY` write a list of columns; `RANGE`, `LIST` and
    `HASH` write one expression, which is a bare column often enough to be worth reading.
    Raises for an expression which is not a column - see the module docstring for why that is a
    refusal and not a limitation of PostgreSQL.
    """
    written = (expression or '').strip()
    if not written:
        raise UntranslatableScheme(
            'the source does not say which columns the table is partitioned by. '
            '`PARTITION BY KEY()` with no column list means the primary key, and '
            'information_schema writes no expression for it - a key nobody wrote down is one '
            'this migrator would have to guess at')
    columns = []
    for part in partitioning.split_top_level_commas(written):
        found = BARE_COLUMN.match(part)
        if not found:
            raise UntranslatableScheme(
                f"the table is partitioned by the expression {written}, which is not a column. "
                f"PostgreSQL can partition by an expression - and a table which does cannot then "
                f"have a primary key or a unique constraint of ANY kind, because every unique "
                f"constraint of a partitioned table has to contain all of its partitioning "
                f"columns and no constraint can contain an expression. MySQL's rule is the "
                f"other way round, so a table partitioned this way usually has a key to lose. "
                f"Write the scheme with target_partitioning - `PARTITION BY RANGE (YEAR(col))` "
                f"over yearly bounds is partitioning_columns: col with date_range: year - or "
                f"set source_partitioning: flatten for this table")
        columns.append(found.group(1) or found.group(2))
    if not columns:
        raise UntranslatableScheme(f"no column could be read out of the key {written}")
    return columns


def to_postgresql_value(value):
    """
    One value of a MySQL bound, written as PostgreSQL writes it.

    The two spell a literal the same way - the single quote delimits and one inside it is
    doubled - so there is nothing to translate in the value. What there is to do is refuse what
    is not a literal: a bound which has to be evaluated is a boundary which moves.
    """
    text = (value or '').strip()
    if not text:
        raise UntranslatableScheme('the source holds no value for this bound')
    upper = text.upper()
    if upper == MAXVALUE:
        return MAXVALUE
    if upper == 'NULL':
        ## MySQL sorts NULL below every value in a RANGE scheme, so it lands in the first
        ## partition; a LIST partition may name NULL outright, and PostgreSQL takes it there
        ## since 11
        return 'NULL'
    if NUMBER.match(text):
        return text
    literal = STRING_LITERAL.match(text)
    if literal:
        return literal.group(1)
    raise UntranslatableScheme(
        f"the bound {text} is not a literal, so it cannot be written as a PostgreSQL bound "
        f"without asking the source to evaluate it")


def range_bound(previous_description, description, column_count):
    """
    `FOR VALUES FROM (…) TO (…)` for one MySQL range partition.

    MySQL says `VALUES LESS THAN (x)` and means an exclusive upper bound over partitions which
    are contiguous and ordered by `PARTITION_ORDINAL_POSITION`. PostgreSQL says the same thing
    with both of its ends, so the lower bound of a partition is the upper bound of the one
    before it and MINVALUE opens the first - the rule Oracle's `VALUES LESS THAN` and Db2
    z/OS's `ENDING AT` need as well.
    """
    upper = [to_postgresql_value(item) for item in _description_items(description)]
    if not upper:
        raise UntranslatableScheme('a range partition with no VALUES LESS THAN in the source')
    if previous_description is None:
        lower = [MINVALUE] * max(column_count or 1, len(upper))
    else:
        lower = [to_postgresql_value(item) for item in _description_items(previous_description)]
        if not lower:
            raise UntranslatableScheme(
                'the partition below this one has no bound, so its upper end - which is this '
                "partition's lower end - is not known")
    _refuse_a_mixed_unbounded_key(lower)
    _refuse_a_mixed_unbounded_key(upper)
    return f"FOR VALUES FROM ({', '.join(lower)}) TO ({', '.join(upper)})"


def _refuse_a_mixed_unbounded_key(values):
    """
    PostgreSQL takes MAXVALUE only at the end of a bound: once one column of a composite key is
    unbounded, every column behind it must be as well. MySQL allows `VALUES LESS THAN
    (MAXVALUE, 10)` and means the 10 to be read.
    """
    seen_unbounded = False
    for value in values:
        if value in (MINVALUE, MAXVALUE):
            seen_unbounded = True
            continue
        if seen_unbounded:
            raise UntranslatableScheme(
                f"the bound ({', '.join(values)}) writes a value after MAXVALUE. PostgreSQL "
                f"takes an unbounded column only at the end of a key, so this bound has no "
                f"counterpart")


def list_bound(description, column_count):
    """
    `FOR VALUES IN (…)` for one MySQL list partition.

    `LIST` writes its values as a plain list - `1,2,3`; `LIST COLUMNS` wraps each row of values
    in brackets - `(1,'a'),(2,'b')`. PostgreSQL's LIST key takes exactly one column, so the
    second is refused above unless it names one, and here the brackets are taken off.
    """
    if column_count > 1:
        raise UntranslatableScheme(
            'a LIST COLUMNS scheme over more than one column. PostgreSQL takes exactly one '
            'column in a LIST partition key')
    values = []
    for item in _description_items(description):
        written = item.strip()
        if written.startswith('(') and written.endswith(')'):
            written = written[1:-1].strip()
        values.append(to_postgresql_value(written))
    if not values:
        raise UntranslatableScheme('a list partition with no values in the source')
    return f"FOR VALUES IN ({', '.join(values)})"


def hash_bound(position, count):
    """
    `FOR VALUES WITH (MODULUS n, REMAINDER i)` for the i-th of n hash partitions.

    The count is carried over and the placement is not: MySQL hashes with its own function - or,
    for `KEY`, with the internal one of the storage engine - and PostgreSQL with its own.
    """
    if not count or count < 1:
        raise UntranslatableScheme('a hash scheme with no partitions in the source')
    if position is None or position < 0 or position >= count:
        raise UntranslatableScheme(
            f"the hash partition at position {position} of {count} is not one this scheme has")
    return f"FOR VALUES WITH (MODULUS {count}, REMAINDER {position})"


def _description_items(description):
    """
    The values of one `PARTITION_DESCRIPTION`, which holds one per partitioning column.

    A composite range bound writes them as a list - `1990,'abc'` - and a value can hold a comma
    of its own inside a literal, which is why this is not a split().
    """
    text = (description or '').strip()
    if not text:
        return []
    return [item.strip() for item in partitioning.split_top_level_commas(text) if item.strip()]


def target_method_of(method):
    """Which of PostgreSQL's three methods a MySQL one becomes, or '' where it becomes none."""
    written = (method or '').strip().upper()
    if written in RANGE_METHODS:
        return 'RANGE'
    if written in LIST_METHODS:
        return 'LIST'
    if written in HASH_METHODS:
        return 'HASH'
    return ''


def key_definition(method, columns, name_of_column):
    """
    The `PARTITION BY` clause of the target, in the names the target will have.

    MySQL folds an unquoted name to lower case already, and `names_case_handling` may still
    change it - a MariaDB table holding `Store_Id` under `lower` gives the target `store_id`.
    """
    target_method = target_method_of(method)
    if not target_method:
        raise UntranslatableScheme(
            f"PostgreSQL partitions by RANGE, LIST and HASH; {method or 'this method'} is none "
            f"of the three")
    if not columns:
        raise UntranslatableScheme(f"a {method} scheme whose key columns the source does not hold")
    if target_method == 'LIST' and len(columns) > 1:
        raise UntranslatableScheme(
            f"a LIST key over {len(columns)} columns. PostgreSQL takes exactly one column in a "
            f"LIST partition key")
    written = ', '.join(f'"{name_of_column(column)}"' for column in columns)
    return f"{target_method} ({written})"


def build_scheme(table_name, method, columns, partitions, name_of_column):
    """
    The bound of every partition, written the way PostgreSQL writes it, and what has to be said
    about the scheme.

    partitions - [{'name', 'description', 'rows'}], in PARTITION_ORDINAL_POSITION order

    Returns (target_key_definition, notes, blockers). Every partition which can be built carries
    its `target_bound` when this returns; a scheme with any blocker builds nothing, because half
    a partitioning is a table which refuses the rows of the half which is missing.
    """
    notes = []
    blockers = []
    target_method = target_method_of(method)

    target_key_definition = ''
    try:
        target_key_definition = key_definition(method, columns, name_of_column)
    except UntranslatableScheme as e:
        blockers.append(
            f"{table_name} is partitioned by {method or 'a method the source does not name'} on "
            f"the source and the same scheme cannot be built on PostgreSQL: {e}. Set "
            f"source_partitioning: flatten for this table, or write a scheme of your own with "
            f"target_partitioning")
        return '', notes, blockers

    previous = None
    for position, partition in enumerate(partitions):
        try:
            if target_method == 'RANGE':
                lower, previous = previous, partition['description']
                partition['target_bound'] = range_bound(lower, partition['description'],
                                                        len(columns))
            elif target_method == 'LIST':
                partition['target_bound'] = list_bound(partition['description'], len(columns))
            else:
                partition['target_bound'] = hash_bound(position, len(partitions))
        except UntranslatableScheme as e:
            blockers.append(
                f"the partition {partition['name']} of {table_name} cannot be given to "
                f"PostgreSQL as it stands: {e}")

    if target_method == 'HASH' and not blockers:
        notes.append(
            f"{table_name} is partitioned by {method} into {len(partitions)} on the source, and "
            f"the target is given {len(partitions)} hash partitions over the same column(s) - "
            f"but MySQL hashes with its own function"
            + (" - for KEY, with the internal one of the storage engine -"
               if 'KEY' in (method or '').upper() else "")
            + f" and PostgreSQL with its own, so a row which sat in one partition on the source "
              f"sits in another here. Nothing is lost by it: the rows go in through the parent "
              f"and the target routes each of them")
    if target_method == 'RANGE' and partitions and not blockers:
        last = partitions[-1]
        highest = _description_items(last['description'])
        if highest and not any(value.strip().upper() == MAXVALUE for value in highest):
            notes.append(
                f"the partitions of {table_name} end at {last['name']}, whose bound is "
                f"{last.get('target_bound', '')}. The source has no MAXVALUE partition and the "
                f"target is given none either, so a row past that bound is refused with 'no "
                f"partition of relation ... found for row' - which is what the source does today "
                f"as well, with 'Table has no partition for value'. Write a target_partitioning "
                f"entry with a future: window if the migration is the moment to change that")
    if blockers:
        return '', notes, blockers
    return target_key_definition, notes, blockers


class MySqlPartitioning:
    """
    What the two connectors of this dialect read out of `information_schema`.

    §2.4: *"one query, and one implementation for both - the two are one dialect, as
    `mysql_query_conversion.py` already is."* This is that one implementation, mixed into
    `MySQLConnector` and `MariaDBConnector` exactly as the query conversion is. It uses nothing
    of a connector but `connect()`, `connection` and `config_parser`, so the module still
    imports no driver and can be tested without one.
    """

    ## The types a `date_range` can be counted in on this source.
    DATE_RANGE_TYPES = DATE_RANGE_TYPES

    def fetch_partitioning_candidates(self, schema):
        """
        The tables of one schema which are partitioned, in one query - so that a schema of three
        hundred ordinary tables costs one round trip rather than three hundred.

        A partition of a MySQL table is not a table: it has no row in
        `information_schema.TABLES` of its own, no row in the migration, and nothing asks about
        it. `information_schema.PARTITIONS` holds one row per table either way - a table which
        is not partitioned gets a single row with no method - so the method is what is tested.
        """
        query = f"""
            SELECT DISTINCT TABLE_NAME
            FROM INFORMATION_SCHEMA.PARTITIONS
            WHERE TABLE_SCHEMA = '{schema}' AND PARTITION_METHOD IS NOT NULL
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            names = {row[0] for row in cursor.fetchall() if row[0]}
            cursor.close()
            self.disconnect()
            return names
        except Exception as e:
            self.config_parser.print_log_message(
                'WARNING', f"{self.PARTITIONING_LOG_NAME}: fetch_partitioning_candidates: the "
                           f"partitioned tables of {schema} could not be listed ({e}) - every "
                           f"table is asked instead.")
            return None

    def fetch_table_partitioning(self, settings):
        """
        The partitioning of one table, out of `information_schema.PARTITIONS` - which holds the
        whole scheme: the method, the expression, the bound of every partition and its rows.

        See the contract in DatabaseConnector.fetch_table_partitioning(). Beyond it this
        connector answers `target_key_definition`, a `target_bound` per partition, `notes` and
        `blockers`: MySQL does not write a hash the way PostgreSQL hashes, its `KEY` method has
        no counterpart at all, and a partitioning expression which is not a bare column costs
        the table every unique constraint it has.
        """
        source_schema_name = settings['source_schema_name']
        source_table_name = settings['source_table_name']
        try:
            self.connect()
            cursor = self.connection.cursor()
            rows = self._partition_rows(cursor, source_schema_name, source_table_name)
            cursor.close()
            self.disconnect()
        except Exception as e:
            self.config_parser.print_log_message(
                'ERROR', f"{self.PARTITIONING_LOG_NAME}: fetch_table_partitioning: the "
                         f"partitioning of {source_schema_name}.{source_table_name} could not be "
                         f"read: {e}")
            return {}

        if not rows:
            return {}
        method = (rows[0]['method'] or '').strip().upper()
        if not method:
            ## information_schema holds one row for a table which is not partitioned as well,
            ## with no method in it
            return {}
        expression = rows[0]['expression']
        sub_method = (rows[0]['sub_method'] or '').strip().upper()
        sub_expression = rows[0]['sub_expression']

        ## one entry per partition of the FIRST level - a sub-partitioned table has one row per
        ## segment in information_schema and its rows are added up into the partition they
        ## belong to, because §2.2 is why the second level is not built
        partitions = []
        for row in rows:
            if partitions and partitions[-1]['name'] == row['name']:
                partitions[-1]['rows'] = _add_rows(partitions[-1]['rows'], row['rows'])
                partitions[-1]['segments'] += 1
                continue
            partitions.append({'name': row['name'], 'description': row['description'],
                               'rows': row['rows'], 'segments': 1, 'target_bound': ''})

        notes = []
        blockers = []
        columns = []
        try:
            columns = partition_key_columns(method, expression)
        except UntranslatableScheme as e:
            blockers.append(
                f"{source_table_name} is partitioned by {method} on the source and the same "
                f"scheme cannot be built on PostgreSQL: {e}")

        target_key_definition = ''
        if columns:
            target_key_definition, more_notes, more_blockers = build_scheme(
                source_table_name, method, columns, partitions,
                self.config_parser.convert_names_case)
            notes.extend(more_notes)
            blockers.extend(more_blockers)

        segments = sum(partition['segments'] for partition in partitions)
        engine_specific = {'partitioning_method': method,
                           'partitioning_expression': (expression or '').strip()}
        levels_below = []
        if sub_method:
            engine_specific['subpartitioning'] = {
                'method': sub_method, 'expression': (sub_expression or '').strip(),
                'segments': segments}
            levels_below = [{'level': 2, 'method': sub_method,
                             'columns': _columns_or_expression(sub_method, sub_expression),
                             'partition_count': segments}]
            notes.append(
                f"{source_table_name} is sub-partitioned on the source: {method} over "
                f"{len(partitions)} partition(s), each sub-partitioned by {sub_method}"
                + (f" ({(sub_expression or '').strip()})" if sub_expression else '')
                + f" - {segments} segments in all. Only the first level is carried over. "
                  f"PostgreSQL can express the second and it is almost always the wrong thing to "
                  f"build: every segment is a relation with its own statistics and indexes, and "
                  f"what a hash sub-level buys on MySQL - a spread of the writes over files - is "
                  f"not what it buys here")

        return {
            'is_partitioned': True,
            ## a partition of a MySQL table is not a table of the schema, so nothing this
            ## connector is asked about is ever one
            'is_partition': False,
            'parent_table': '',
            'parent_schema': '',
            'partition_bound': '',
            'method': method,
            'columns': columns,
            'key_definition': (f"{method} ({(expression or '').strip()})" if expression
                               else method),
            'target_key_definition': target_key_definition,
            'level': 1,
            'levels_below': levels_below,
            'partitions': [{
                'name': partition['name'],
                'bound': _bound_as_the_source_writes_it(method, partition['description']),
                'target_bound': partition['target_bound'],
                'is_default': False,
                ## §2.2: the second level is not carried over, so nothing walks into it
                'is_partitioned': False,
                'rows': partition['rows'],
            } for partition in partitions],
            'partition_count': len(partitions),
            'engine_specific': engine_specific,
            'notes': notes,
            'blockers': blockers,
        }

    def _partition_rows(self, cursor, schema, table):
        """
        Every row `information_schema.PARTITIONS` holds for one table, in the order the
        partitions run in.

        A sub-partitioned table has one row per segment, ordered so that the segments of one
        partition stand together - which is what lets their rows be added up into the partition
        they belong to without a second query.
        """
        cursor.execute(f"""
            SELECT PARTITION_NAME, SUBPARTITION_NAME, PARTITION_METHOD, SUBPARTITION_METHOD,
                   PARTITION_EXPRESSION, SUBPARTITION_EXPRESSION, PARTITION_DESCRIPTION,
                   TABLE_ROWS
            FROM INFORMATION_SCHEMA.PARTITIONS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            ORDER BY PARTITION_ORDINAL_POSITION, SUBPARTITION_ORDINAL_POSITION
        """)
        rows = []
        for row in cursor.fetchall():
            rows.append({
                'name': (row[0] or '').strip() if isinstance(row[0], str) else '',
                'method': row[2], 'sub_method': row[3],
                'expression': row[4], 'sub_expression': row[5],
                'description': (row[6] or '').strip() if isinstance(row[6], str) else '',
                ## TABLE_ROWS is an estimate for InnoDB - it comes out of the same sampling the
                ## optimizer uses and is reported as an estimate rather than as a count
                'rows': int(row[7]) if row[7] is not None else None,
            })
        return rows

    def fetch_partitioning_facts(self, settings):
        """
        Everything about one table which decides whether it CAN be partitioned - read before
        anything is created, which is the moment the answer is still free.

        See DatabaseConnector.fetch_partitioning_facts() for the shape. Everything comes from
        `information_schema`: nothing here reads a row of the table. MySQL keeps no NULL count
        of its own, so the null fraction is answered as NOT known for every column and the check
        which needs it says it was not made rather than that it passed.
        """
        source_schema_name = settings['source_schema_name']
        source_table_name = settings['source_table_name']
        try:
            self.connect()
            cursor = self.connection.cursor()
            row_estimate = self._row_estimate(cursor, source_schema_name, source_table_name)
            columns = self._column_facts(cursor, source_schema_name, source_table_name)
            unique_keys = self._unique_keys(cursor, source_schema_name, source_table_name)
            referenced_by = self._referencing_foreign_keys(cursor, source_schema_name,
                                                           source_table_name)
            cursor.close()
            self.disconnect()
        except Exception as e:
            self.config_parser.print_log_message(
                'WARNING', f"{self.PARTITIONING_LOG_NAME}: fetch_partitioning_facts: the facts "
                           f"of {source_schema_name}.{source_table_name} could not be read "
                           f"({e}) - the checks which need them are reported as NOT made rather "
                           f"than as passed.")
            return None

        return {
            'columns': columns,
            'unique_keys': unique_keys,
            ## neither engine has an exclusion constraint or table inheritance - absences of the
            ## engine, which are not the same as things this connector did not read
            'exclusion_constraints': [],
            'referenced_by': referenced_by,
            'inherits_from_a_plain_table': False,
            'is_a_plain_inheritance_parent': False,
            'row_estimate': row_estimate,
            'date_range_types': self.DATE_RANGE_TYPES,
        }

    def _row_estimate(self, cursor, schema, table):
        """`information_schema.TABLES.TABLE_ROWS` - an estimate for InnoDB, and said to be one."""
        cursor.execute(f"""
            SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
        """)
        row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def _column_facts(self, cursor, schema, table):
        """Every column and what PostgreSQL will be able to do with it."""
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, EXTRA, GENERATION_EXPRESSION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """)
        columns = {}
        for row in cursor.fetchall():
            name = row[0]
            data_type = (row[1] or '').strip().upper()
            extra = (row[4] or '').strip().upper()
            can_be_a_key = data_type not in TYPES_WITHOUT_AN_OPERATOR_CLASS
            columns[name] = {
                'type_name': data_type,
                'not_null': (row[3] or '').strip().upper() == 'NO',
                ## EXTRA says VIRTUAL GENERATED or STORED GENERATED; a generated column of the
                ## source is a generated column of the target, which PostgreSQL refuses in a key
                'is_generated': 'GENERATED' in extra or bool((row[5] or '').strip()),
                'has_btree_opclass': can_be_a_key,
                'has_hash_opclass': can_be_a_key,
                ## neither engine keeps a NULL count, so this is NOT known for every column -
                ## and the check which needs it says it was not made
                'null_fraction': None,
            }
        return columns

    def _unique_keys(self, cursor, schema, table):
        """
        The primary key, every unique constraint and every unique index which is not one - §3.1
        applies to all three alike, and `information_schema.STATISTICS` holds them together.
        """
        cursor.execute(f"""
            SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' AND NON_UNIQUE = 0
            ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """)
        keys = {}
        for row in cursor.fetchall():
            name = row[0]
            key = keys.setdefault(name, {'name': name, 'columns': [],
                                         'is_primary': name == 'PRIMARY'})
            if row[1]:
                key['columns'].append(row[1])
        return list(keys.values())

    def _referencing_foreign_keys(self, cursor, schema, table):
        """§3.5: a foreign key pointing AT a partitioned table needs PostgreSQL 12."""
        cursor.execute(f"""
            SELECT DISTINCT CONSTRAINT_NAME, TABLE_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE REFERENCED_TABLE_SCHEMA = '{schema}' AND REFERENCED_TABLE_NAME = '{table}'
            ORDER BY CONSTRAINT_NAME
        """)
        return [{'name': row[0], 'table': row[1]} for row in cursor.fetchall()]


def _add_rows(one, other):
    """Two row counts added, where a count which is not known keeps the sum unknown."""
    if one is None or other is None:
        return None
    return one + other


def _columns_or_expression(method, expression):
    """The key of a level as a list of columns, or the expression where it is not one."""
    try:
        return partition_key_columns(method, expression)
    except UntranslatableScheme:
        return [(expression or '').strip()] if expression else []


def _bound_as_the_source_writes_it(method, description):
    """One partition's bound in the words of the source, for the report and the protocol."""
    target_method = target_method_of(method)
    if target_method == 'RANGE':
        return f"VALUES LESS THAN ({description})" if description else 'VALUES LESS THAN'
    if target_method == 'LIST':
        return f"VALUES IN ({description})" if description else 'VALUES IN'
    return method
