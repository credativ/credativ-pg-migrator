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
The partitioning of MySQL and MariaDB: what `information_schema.PARTITIONS` says, and what
PostgreSQL is given for it.

§2.4 of development/PARTITIONING_STRATEGY.md: *"one query, and one implementation for both - the
two are one dialect, as `mysql_query_conversion.py` already is."* So every test which touches a
connector runs against **both**, from one list, and a change which repaired one of them and not
the other fails here.

Two things this file exists to hold them to. **A hash carried over is a different hash**: MySQL
hashes an integer expression with its own function and `KEY` uses the internal one of the storage
engine, PostgreSQL hashes the column value with its own, and the same count over the same column
does not put the same rows in the same partition. And **a partitioning expression which is not a
bare column costs the table every unique constraint it has** - PostgreSQL can partition by
`EXTRACT(YEAR FROM hired)` and a table which does can then have no primary key at all, while
MySQL's rule is the other way round and lets `PARTITION BY RANGE (YEAR(hired))` sit beside
`PRIMARY KEY (id, hired)`.

Nothing here needs a database or a driver.
"""

import os
import sys
import pytest
from unittest.mock import MagicMock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator import partitioning
from credativ_pg_migrator.connectors import mysql_partitioning as my
from credativ_pg_migrator.connectors.mysql_partitioning import UntranslatableScheme
from credativ_pg_migrator.connectors.mysql_connector import MySQLConnector
from credativ_pg_migrator.connectors.mariadb_connector import MariaDBConnector


## Every connector test runs against both, because §2.4 asks for one implementation and this is
## what holds it to being one.
BOTH = [MySQLConnector, MariaDBConnector]


def parts(items):
    """[(name, PARTITION_DESCRIPTION, TABLE_ROWS)] as build_scheme() wants them."""
    return [{'name': name, 'description': description, 'rows': rows,
             'segments': 1, 'target_bound': ''}
            for name, description, rows in items]


def built(method, expression, items, table='ORDERS'):
    columns = my.partition_key_columns(method, expression)
    entries = parts(items)
    key, notes, blockers = my.build_scheme(table, method, columns, entries, str.lower)
    return {'key': key, 'notes': notes, 'blockers': blockers, 'columns': columns,
            'bounds': [entry['target_bound'] for entry in entries]}


# --------------------------------------------------------------------------------------
# the key


@pytest.mark.parametrize('method, expression, expected', [
    ## MySQL 8 delimits the column in the expression it stores; 5.7 sometimes does not
    ('RANGE', '`store_id`', ['store_id']),
    ('RANGE', 'store_id', ['store_id']),
    ('RANGE', '`store_id` ', ['store_id']),
    ('RANGE COLUMNS', '`a`,`b`', ['a', 'b']),
    ('LIST COLUMNS', '`region`', ['region']),
    ('KEY', '`id`,`part`', ['id', 'part']),
])
def test_the_partitioning_columns_are_read_however_the_source_writes_them(
        method, expression, expected):
    assert my.partition_key_columns(method, expression) == expected


def test_an_expression_which_is_not_a_column_is_refused_and_the_message_says_why():
    """
    Not because PostgreSQL cannot express it - it can. A table partitioned by an expression can
    then have no primary key and no unique constraint of any kind, because every unique
    constraint of a partitioned table has to contain all of its partitioning columns and no
    constraint can contain an expression. MySQL's rule is the other way round, so the tables
    which use this are exactly the tables which have a key to lose.
    """
    with pytest.raises(UntranslatableScheme) as refused:
        my.partition_key_columns('RANGE', 'year(`hired`)')
    message = str(refused.value)
    assert 'unique constraint of ANY kind' in message
    ## and it names the short way out
    assert 'date_range: year' in message
    assert 'source_partitioning: flatten' in message


def test_a_key_with_no_column_list_is_refused_rather_than_guessed():
    """
    `PARTITION BY KEY()` means the primary key, and information_schema writes no expression for
    it. A key nobody wrote down is one this migrator would have to guess at.
    """
    with pytest.raises(UntranslatableScheme, match='PARTITION BY KEY'):
        my.partition_key_columns('KEY', '')


@pytest.mark.parametrize('method, target', [
    ('RANGE', 'RANGE'), ('RANGE COLUMNS', 'RANGE'),
    ('LIST', 'LIST'), ('LIST COLUMNS', 'LIST'),
    ('HASH', 'HASH'), ('LINEAR HASH', 'HASH'), ('KEY', 'HASH'), ('LINEAR KEY', 'HASH'),
    ('SOMETHING ELSE', ''), ('', ''),
])
def test_every_method_of_the_source_maps_to_one_of_postgresqls_three_or_to_none(method, target):
    assert my.target_method_of(method) == target


def test_a_list_key_takes_exactly_one_column():
    """`LIST COLUMNS (a, b)` has no PostgreSQL counterpart: its LIST key is one column."""
    with pytest.raises(UntranslatableScheme, match='exactly one column'):
        my.key_definition('LIST COLUMNS', ['a', 'b'], str.lower)


def test_the_key_is_written_in_the_names_the_target_will_have():
    assert my.key_definition('RANGE', ['Store_Id'], lambda name: name.lower()) == \
        'RANGE ("store_id")'


# --------------------------------------------------------------------------------------
# one value of a bound


@pytest.mark.parametrize('written, expected', [
    ('5', '5'), ('-1.5', '-1.5'), ("'abc'", "'abc'"), ("'O''BRIEN'", "'O''BRIEN'"),
    ('MAXVALUE', 'MAXVALUE'), ('maxvalue', 'MAXVALUE'), ('NULL', 'NULL'),
])
def test_a_bound_value_is_written_the_way_postgresql_writes_it(written, expected):
    assert my.to_postgresql_value(written) == expected


@pytest.mark.parametrize('written', ['TO_DAYS(NOW())', 'CURRENT_DATE', ''])
def test_a_bound_which_is_not_a_literal_is_refused(written):
    with pytest.raises(UntranslatableScheme):
        my.to_postgresql_value(written)


# --------------------------------------------------------------------------------------
# the whole scheme


def test_a_range_scheme_becomes_contiguous_postgresql_partitions():
    """
    `VALUES LESS THAN (x)` is an exclusive upper bound over partitions which are contiguous and
    ordered - which is what PostgreSQL says with both of its ends, so the two are one scheme
    written twice.
    """
    scheme = built('RANGE', '`store_id`', [('p0', '5', 10), ('p1', '10', 20),
                                           ('p2', 'MAXVALUE', 5)])
    assert scheme['key'] == 'RANGE ("store_id")'
    assert scheme['bounds'] == ['FOR VALUES FROM (MINVALUE) TO (5)',
                                'FOR VALUES FROM (5) TO (10)',
                                'FOR VALUES FROM (10) TO (MAXVALUE)']
    assert scheme['blockers'] == []


def test_a_range_columns_scheme_over_several_columns_maps_as_well():
    scheme = built('RANGE COLUMNS', '`a`,`b`',
                   [('p0', "1990,'abc'", 1), ('p1', 'MAXVALUE,MAXVALUE', 1)])
    assert scheme['key'] == 'RANGE ("a", "b")'
    assert scheme['bounds'][0] == "FOR VALUES FROM (MINVALUE, MINVALUE) TO (1990, 'abc')"


def test_a_value_written_after_maxvalue_is_refused():
    """
    PostgreSQL takes an unbounded column only at the end of a key, because nothing after an
    infinity has a meaning. MySQL allows it and means the value to be read.
    """
    scheme = built('RANGE COLUMNS', '`a`,`b`', [('p0', 'MAXVALUE,10', 1)])
    assert any('after MAXVALUE' in issue for issue in scheme['blockers'])


def test_a_list_scheme_keeps_its_values():
    scheme = built('LIST', '`region_id`', [('pNorth', '1,3,5', 1), ('pSouth', '2,4,6', 1)])
    assert scheme['key'] == 'LIST ("region_id")'
    assert scheme['bounds'] == ['FOR VALUES IN (1, 3, 5)', 'FOR VALUES IN (2, 4, 6)']


def test_a_list_columns_scheme_of_one_column_has_its_brackets_taken_off():
    """`LIST COLUMNS` wraps every row of values in brackets and `LIST` does not."""
    scheme = built('LIST COLUMNS', '`region`', [('pE', "('DE'),('FR')", 1), ('pA', "('US')", 1)])
    assert scheme['bounds'] == ["FOR VALUES IN ('DE', 'FR')", "FOR VALUES IN ('US')"]


def test_a_list_columns_scheme_over_several_columns_is_refused():
    scheme = built('LIST COLUMNS', '`a`,`b`', [('p0', "(1,'a'),(2,'b')", 1)])
    assert scheme['key'] == ''
    assert any('exactly one column' in issue for issue in scheme['blockers'])


@pytest.mark.parametrize('method', ['HASH', 'LINEAR HASH', 'KEY', 'LINEAR KEY'])
def test_a_hash_scheme_carries_its_count_and_says_the_rows_move(method):
    """
    §2.4's loudest sentence about this source: the same column and the same partition count do
    NOT put the same rows in the same partition. Nothing is lost by it - the rows go in through
    the parent and the target routes each of them - and a reader would assume the opposite.
    """
    scheme = built(method, '`store_id`', [(f'p{index}', None, 1) for index in range(4)])
    assert scheme['key'] == 'HASH ("store_id")'
    assert scheme['bounds'] == [f'FOR VALUES WITH (MODULUS 4, REMAINDER {index})'
                                for index in range(4)]
    assert any('sits in another here' in note for note in scheme['notes'])
    assert scheme['blockers'] == []


def test_the_note_about_a_key_scheme_names_the_storage_engine():
    """
    §2.4 calls KEY the method with no counterpart at all. It carries over exactly as HASH does -
    n hash partitions over the same columns, and another row in each of them - and what is its
    own is which function does the hashing.
    """
    scheme = built('KEY', '`id`', [('p0', None, 1), ('p1', None, 1)])
    note = [text for text in scheme['notes'] if 'sits in another here' in text]
    assert note and 'storage engine' in note[0]


def test_a_range_scheme_which_stops_says_where_it_stops():
    scheme = built('RANGE', '`store_id`', [('p0', '5', 1), ('p1', '10', 1)])
    note = [text for text in scheme['notes'] if 'past that bound is refused' in text]
    assert note and 'Table has no partition for value' in note[0]


def test_a_range_scheme_ending_in_maxvalue_needs_nothing_said_about_its_end():
    scheme = built('RANGE', '`store_id`', [('p0', '5', 1), ('p1', 'MAXVALUE', 1)])
    assert not [text for text in scheme['notes'] if 'past that bound' in text]


def test_a_scheme_with_a_blocker_builds_nothing_at_all():
    """Half a partitioning is a table which refuses the rows of the half which is missing."""
    scheme = built('RANGE', '`store_id`', [('p0', 'TO_DAYS(NOW())', 1)])
    assert scheme['key'] == ''
    assert scheme['blockers']


# --------------------------------------------------------------------------------------
# the two connectors, which are one implementation


class Catalogue:
    """An information_schema cursor, answering by what the statement names."""

    def __init__(self, answers, raise_on=None):
        self.answers = answers
        self.raise_on = raise_on or ()
        self.rows = []
        self.statements = []

    def execute(self, statement, binds=None):
        self.statements.append(statement)
        for marker in self.raise_on:
            if marker in statement:
                raise Exception(f"Table 'information_schema.{marker}' doesn't exist")
        for marker, rows in self.answers:
            if marker in statement:
                self.rows = rows
                return
        raise AssertionError(f'the test has no answer for this statement:\n{statement}')

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)

    def close(self):
        pass


def connector(connector_class, answers, raise_on=None):
    made = connector_class.__new__(connector_class)
    made.config_parser = MagicMock()
    made.config_parser.convert_names_case = lambda name: (name or '').lower()
    made.messages = []
    made.config_parser.print_log_message.side_effect = \
        lambda level, message: made.messages.append((level, str(message)))
    made.connect = MagicMock()
    made.disconnect = MagicMock()
    made.connection = MagicMock()
    cursor = Catalogue(answers, raise_on)
    made.connection.cursor.return_value = cursor
    made.cursor = cursor
    return made


## name, subpartition, method, sub method, expression, sub expression, description, rows
ORDERS = [('INFORMATION_SCHEMA.PARTITIONS', [
    ('p2022', None, 'RANGE', None, '`order_year`', None, '2023', 5000),
    ('p2023', None, 'RANGE', None, '`order_year`', None, '2024', 6000),
    ('pmax', None, 'RANGE', None, '`order_year`', None, 'MAXVALUE', 100),
])]


@pytest.mark.parametrize('connector_class', BOTH)
def test_the_connector_reads_a_range_scheme(connector_class):
    made = connector(connector_class, ORDERS)
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'shop', 'source_table_name': 'orders'})
    assert scheme['is_partitioned'] is True and scheme['is_partition'] is False
    assert scheme['method'] == 'RANGE' and scheme['columns'] == ['order_year']
    assert scheme['target_key_definition'] == 'RANGE ("order_year")'
    assert scheme['partition_count'] == 3


@pytest.mark.parametrize('connector_class', BOTH)
def test_every_partition_carries_both_spellings_of_its_bound(connector_class):
    made = connector(connector_class, ORDERS)
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'shop', 'source_table_name': 'orders'})
    first = scheme['partitions'][0]
    assert first['bound'] == 'VALUES LESS THAN (2023)'
    assert first['target_bound'] == 'FOR VALUES FROM (MINVALUE) TO (2023)'
    assert first['rows'] == 5000


@pytest.mark.parametrize('connector_class', BOTH)
def test_a_table_which_is_not_partitioned_answers_nothing(connector_class):
    """
    information_schema.PARTITIONS holds one row for an unpartitioned table as well, with no
    method in it - so an empty answer is not what says the table has no scheme.
    """
    made = connector(connector_class, [('INFORMATION_SCHEMA.PARTITIONS', [
        (None, None, None, None, None, None, None, 1200)])])
    assert made.fetch_table_partitioning(
        {'source_schema_name': 'shop', 'source_table_name': 'customers'}) == {}


@pytest.mark.parametrize('connector_class', BOTH)
def test_the_sub_partitions_are_reported_and_their_rows_added_into_their_partition(
        connector_class):
    """
    §2.2: a RANGE of 60 months sub-partitioned by HASH into 16 is 960 relations on the target.
    The first level is built, and the segments left behind are counted.
    """
    made = connector(connector_class, [('INFORMATION_SCHEMA.PARTITIONS', [
        ('p0', 'p0sp0', 'RANGE', 'HASH', '`y`', '`customer_id`', '2023', 100),
        ('p0', 'p0sp1', 'RANGE', 'HASH', '`y`', '`customer_id`', '2023', 200),
        ('p1', 'p1sp0', 'RANGE', 'HASH', '`y`', '`customer_id`', 'MAXVALUE', 50),
        ('p1', 'p1sp1', 'RANGE', 'HASH', '`y`', '`customer_id`', 'MAXVALUE', 60),
    ])])
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'shop', 'source_table_name': 'orders'})
    assert scheme['partition_count'] == 2
    assert [partition['rows'] for partition in scheme['partitions']] == [300, 110]
    assert all(partition['is_partitioned'] is False for partition in scheme['partitions'])
    assert scheme['engine_specific']['subpartitioning']['segments'] == 4
    assert scheme['levels_below'] == [
        {'level': 2, 'method': 'HASH', 'columns': ['customer_id'], 'partition_count': 4}]
    note = [text for text in scheme['notes'] if 'Only the first level' in text]
    assert note and '4 segments in all' in note[0]


@pytest.mark.parametrize('connector_class', BOTH)
def test_a_scheme_over_an_expression_is_refused_by_the_connector(connector_class):
    made = connector(connector_class, [('INFORMATION_SCHEMA.PARTITIONS', [
        ('p0', None, 'RANGE', None, 'year(`hired`)', None, '1991', 1),
    ])])
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'hr', 'source_table_name': 'employees'})
    assert scheme['target_key_definition'] == ''
    assert scheme['columns'] == []
    assert any('unique constraint of ANY kind' in issue for issue in scheme['blockers'])
    ## the scheme of the source is still reported as what it is
    assert scheme['key_definition'] == 'RANGE (year(`hired`))'


@pytest.mark.parametrize('connector_class', BOTH)
def test_the_partitioned_tables_of_a_schema_are_listed_in_one_query(connector_class):
    made = connector(connector_class, [
        ('PARTITION_METHOD IS NOT NULL', [('orders',), ('events',)])])
    assert made.fetch_partitioning_candidates('shop') == {'orders', 'events'}
    assert len(made.cursor.statements) == 1


@pytest.mark.parametrize('connector_class', BOTH)
def test_a_catalogue_which_cannot_be_read_answers_nothing_and_says_so(connector_class):
    made = connector(connector_class, ORDERS, raise_on=('INFORMATION_SCHEMA.PARTITIONS',))
    assert made.fetch_table_partitioning(
        {'source_schema_name': 'shop', 'source_table_name': 'orders'}) == {}
    assert any(level == 'ERROR' for level, _text in made.messages)


@pytest.mark.parametrize('connector_class', BOTH)
def test_the_connector_says_which_of_the_two_it_is(connector_class):
    """The shared implementation writes the name of the connector it was mixed into."""
    made = connector(connector_class, [], raise_on=('INFORMATION_SCHEMA.PARTITIONS',))
    made.fetch_partitioning_candidates('shop')
    assert any(made.PARTITIONING_LOG_NAME in text for _level, text in made.messages)


def test_neither_connector_says_it_cannot_read_the_partitioning_any_more():
    for connector_class in BOTH:
        assert 'table_partitioning' not in connector_class.OBJECT_KINDS_NOT_READ
        assert 'table_partitioning' not in connector_class.OBJECT_KINDS_ABSENT


def test_the_two_connectors_share_one_implementation():
    """
    §2.4 asks for one implementation for both, because the two are one dialect. A second copy is
    a second thing to repair.
    """
    assert MySQLConnector.fetch_table_partitioning is MariaDBConnector.fetch_table_partitioning
    assert MySQLConnector.fetch_partitioning_facts is MariaDBConnector.fetch_partitioning_facts


# --------------------------------------------------------------------------------------
# the facts of §4.4


FACTS = [
    ('TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES', [(400000,)]),
    ('GENERATION_EXPRESSION', [
        ## name, data type, column type, is nullable, extra, generation expression
        ('order_id', 'int', 'int', 'NO', 'auto_increment', None),
        ('order_year', 'int', 'int', 'NO', '', None),
        ('shipped_at', 'datetime', 'datetime', 'YES', '', None),
        ('total', 'decimal', 'decimal(9,2)', 'YES', 'STORED GENERATED', '`a` * `b`'),
        ('document', 'json', 'json', 'YES', '', None),
        ('note', 'varchar', 'varchar(100)', 'YES', '', None),
    ]),
    ('NON_UNIQUE = 0', [
        ('PRIMARY', 'order_id', 1),
        ('orders_ref_uq', 'order_ref', 1),
        ('orders_ref_uq', 'order_year', 2),
    ]),
    ('REFERENCED_TABLE_SCHEMA', [('order_item_fk', 'order_items')]),
]


def facts_of(connector_class, answers=None, raise_on=None):
    made = connector(connector_class, answers or FACTS, raise_on)
    return made.fetch_partitioning_facts(
        {'source_schema_name': 'shop', 'source_table_name': 'orders'})


@pytest.mark.parametrize('connector_class', BOTH)
def test_the_facts_of_a_table_come_out_of_information_schema(connector_class):
    facts = facts_of(connector_class)
    assert facts['row_estimate'] == 400000
    assert facts['columns']['order_year']['not_null'] is True
    assert facts['columns']['shipped_at']['not_null'] is False
    assert facts['columns']['note']['type_name'] == 'VARCHAR'
    assert facts['date_range_types'] == my.DATE_RANGE_TYPES


@pytest.mark.parametrize('connector_class', BOTH)
def test_neither_engine_keeps_a_null_count_so_the_check_is_reported_as_not_made(connector_class):
    """P2-8: None is read as "not known"; zero would say the column holds no NULL."""
    facts = facts_of(connector_class)
    assert all(column['null_fraction'] is None for column in facts['columns'].values())


@pytest.mark.parametrize('connector_class', BOTH)
def test_a_generated_column_is_recognised(connector_class):
    facts = facts_of(connector_class)
    assert facts['columns']['total']['is_generated'] is True
    assert facts['columns']['order_id']['is_generated'] is False


@pytest.mark.parametrize('connector_class', BOTH)
def test_a_type_which_cannot_carry_a_partition_key_says_so(connector_class):
    facts = facts_of(connector_class)
    assert facts['columns']['document']['has_btree_opclass'] is False
    assert facts['columns']['note']['has_btree_opclass'] is True


@pytest.mark.parametrize('connector_class', BOTH)
def test_every_unique_key_is_read_with_its_columns_in_order(connector_class):
    keys = {key['name']: key for key in facts_of(connector_class)['unique_keys']}
    assert keys['PRIMARY']['is_primary'] is True
    assert keys['orders_ref_uq']['is_primary'] is False
    assert keys['orders_ref_uq']['columns'] == ['order_ref', 'order_year']


@pytest.mark.parametrize('connector_class', BOTH)
def test_what_references_the_table_is_read(connector_class):
    assert facts_of(connector_class)['referenced_by'] == [
        {'name': 'order_item_fk', 'table': 'order_items'}]


@pytest.mark.parametrize('connector_class', BOTH)
def test_facts_which_cannot_be_read_are_answered_as_not_read(connector_class):
    assert facts_of(connector_class, raise_on=('GENERATION_EXPRESSION',)) is None


# --------------------------------------------------------------------------------------
# the whole way through


PARTITION_SQL = ('CREATE TABLE "{target_schema_name}"."{target_table_name}" PARTITION OF '
                 '"{target_schema_name}"."{parent_table_name}" {partition_bound}')


@pytest.mark.parametrize('connector_class', BOTH)
def test_the_create_table_of_a_preserved_table_and_its_partitions(connector_class):
    from credativ_pg_migrator.planner import Planner

    made = connector(connector_class, ORDERS)
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'shop', 'source_table_name': 'orders'})
    plan = partitioning.build_plan(
        {'orders': scheme}, ['orders'], mode_of=lambda name: 'preserve',
        target_version_num=160000)
    assert plan['orders'].issues == []

    planner = Planner.__new__(Planner)
    planner.config_parser = MagicMock()
    planner.config_parser.convert_names_case = lambda name: (name or '').lower()
    planner.target_schema_name = 'migtest'
    target = MagicMock()
    target.get_create_partition_sql.side_effect = \
        lambda settings: PARTITION_SQL.format(**{
            key: (value or '').lower() if key.endswith('table_name') else value
            for key, value in settings.items()})
    planner.target_connection = target

    clause, statements = planner.partitioning_clause_for(plan['orders'], 'orders')
    assert clause == ' PARTITION BY RANGE ("order_year")'
    assert statements == [
        'CREATE TABLE "migtest"."p2022" PARTITION OF "migtest"."orders" '
        'FOR VALUES FROM (MINVALUE) TO (2023)',
        'CREATE TABLE "migtest"."p2023" PARTITION OF "migtest"."orders" '
        'FOR VALUES FROM (2023) TO (2024)',
        'CREATE TABLE "migtest"."pmax" PARTITION OF "migtest"."orders" '
        'FOR VALUES FROM (2024) TO (MAXVALUE)',
    ]


@pytest.mark.parametrize('connector_class', BOTH)
def test_a_scheme_which_cannot_be_built_stops_only_a_run_which_would_build_it(connector_class):
    made = connector(connector_class, [('INFORMATION_SCHEMA.PARTITIONS', [
        ('p0', None, 'RANGE', None, 'year(`hired`)', None, '1991', 1),
    ])])
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'hr', 'source_table_name': 'employees'})
    preserved = partitioning.build_plan(
        {'employees': scheme}, ['employees'], mode_of=lambda name: 'preserve',
        target_version_num=160000)['employees']
    assert preserved.issues
    flattened = partitioning.build_plan(
        {'employees': scheme}, ['employees'], mode_of=lambda name: 'flatten',
        target_version_num=160000)['employees']
    assert flattened.issues == []


@pytest.mark.parametrize('connector_class', BOTH)
def test_a_preserved_key_is_checked_against_the_primary_key(connector_class):
    """
    §3.1. MySQL requires every unique key to contain the partitioning columns already, so this
    check usually passes for a preserved MySQL scheme - and it is made rather than assumed,
    because a table whose key was changed after it was partitioned is not impossible.
    """
    made = connector(connector_class, ORDERS)
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'shop', 'source_table_name': 'orders'})
    decision = partitioning.build_plan(
        {'orders': scheme}, ['orders'], mode_of=lambda name: 'preserve',
        target_version_num=160000)['orders']
    partitioning.check_preserved_keys(
        decision, [{'name': 'PRIMARY', 'columns': ['order_id'], 'is_primary': True}])
    assert any('PRIMARY' in issue and 'order_year' in issue for issue in decision.issues)
