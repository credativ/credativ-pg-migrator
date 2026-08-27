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
The partitioning of SQL Server: a partition function, a partition scheme, and one bit which
decides every bound of the table.

§2.4 of development/PARTITIONING_STRATEGY.md puts mssql last of the twelve and calls it *"cheap,
and the LEFT/RIGHT trap is precisely the kind of thing which is silently wrong if nobody looks."*
This file is the looking.

`RANGE RIGHT` puts a boundary value in the partition **above** it, which is what PostgreSQL's
`FROM (a) TO (b)` means - inclusive below, exclusive above - so it maps with nothing done to it.
`RANGE LEFT` puts it in the partition **below**, which is the opposite at both ends, so every
bound has to move to the next value of the type. A scheme copied bound for bound out of a RANGE
LEFT function is wrong by exactly one value at every boundary, in a direction no error message
would ever mention.

Nothing here needs a database or a driver.
"""

import datetime
import decimal
import os
import sys
import pytest
from unittest.mock import MagicMock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator import partitioning
from credativ_pg_migrator.connectors import mssql_partitioning as ms
from credativ_pg_migrator.connectors.mssql_partitioning import UntranslatableScheme
from credativ_pg_migrator.connectors.ms_sql_connector import MsSQLConnector


# --------------------------------------------------------------------------------------
# one boundary value, which the driver hands over as whatever it really is


@pytest.mark.parametrize('written, expected', [
    (100, '100'),
    (-1, '-1'),
    (decimal.Decimal('1.50'), '1.50'),
    (datetime.date(2024, 1, 1), "'2024-01-01'"),
    (datetime.datetime(2024, 1, 1), "'2024-01-01 00:00:00'"),
    (datetime.datetime(2023, 12, 31, 23, 59, 59, 997000), "'2023-12-31 23:59:59.997000'"),
    ('AB', "'AB'"),
    ("O'BRIEN", "'O''BRIEN'"),
    (b'\xde\xad', "'\\xDEAD'"),
])
def test_a_boundary_value_is_written_the_way_postgresql_writes_it(written, expected):
    """
    `sys.partition_range_values.value` is a `sql_variant`, so it arrives as the type it really
    is rather than as text - and each of them is rendered here rather than trusted to `str()`.
    """
    assert ms.to_postgresql_value(written) == expected


def test_a_boundary_the_catalogue_does_not_hold_is_refused():
    with pytest.raises(UntranslatableScheme):
        ms.to_postgresql_value(None)


# --------------------------------------------------------------------------------------
# RANGE RIGHT and RANGE LEFT - the trap


def test_range_right_is_what_postgresql_already_means():
    """
    RANGE RIGHT puts a boundary value in the partition above it: partition 1 is (-inf, b1) and
    partition k is [b(k-1), bk). That is `FROM (a) TO (b)` said twice.
    """
    assert ms.range_bounds([datetime.date(2023, 1, 1), datetime.date(2024, 1, 1)],
                           boundary_value_on_right=True, type_name='DATE') == [
        "FOR VALUES FROM (MINVALUE) TO ('2023-01-01')",
        "FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')",
        "FOR VALUES FROM ('2024-01-01') TO (MAXVALUE)",
    ]


def test_range_left_moves_every_bound_to_the_next_value():
    """
    RANGE LEFT puts the boundary value in the partition BELOW it: partition 1 is (-inf, b1] and
    partition k is (b(k-1), bk]. Copied across unchanged, every partition of the target would be
    short by exactly the boundary value itself - and nothing in the run would say so.
    """
    assert ms.range_bounds([100, 200], boundary_value_on_right=False, type_name='INT') == [
        'FOR VALUES FROM (MINVALUE) TO (101)',
        'FOR VALUES FROM (101) TO (201)',
        'FOR VALUES FROM (201) TO (MAXVALUE)',
    ]


def test_range_left_over_a_date_counts_in_days():
    assert ms.range_bounds([datetime.date(2023, 12, 31)],
                           boundary_value_on_right=False, type_name='DATE') == [
        "FOR VALUES FROM (MINVALUE) TO ('2024-01-01')",
        "FOR VALUES FROM ('2024-01-01') TO (MAXVALUE)",
    ]


def test_range_left_over_a_datetime_is_refused_rather_than_shifted_by_a_guess():
    """
    SQL Server counts a `datetime` in units of 1/300 of a second, which is exactly why a RANGE
    LEFT boundary over one is written '…23:59:59.997' - and the value after that is not
    something the bound says.
    """
    with pytest.raises(UntranslatableScheme, match='1/300 of a second'):
        ms.range_bounds([datetime.datetime(2023, 12, 31, 23, 59, 59, 997000)],
                        boundary_value_on_right=False, type_name='DATETIME')


def test_range_right_over_a_datetime_needs_nothing_moved():
    """The direction is what decides it, not the type - RANGE RIGHT never has to move a bound."""
    bounds = ms.range_bounds([datetime.datetime(2024, 1, 1)],
                             boundary_value_on_right=True, type_name='DATETIME')
    assert bounds[0] == "FOR VALUES FROM (MINVALUE) TO ('2024-01-01 00:00:00')"


def test_a_function_with_n_boundaries_makes_n_plus_one_partitions():
    assert len(ms.range_bounds([1, 2, 3], True, 'INT')) == 4
    assert ms.range_bounds([], True, 'INT') == ['FOR VALUES FROM (MINVALUE) TO (MAXVALUE)']


def test_the_arithmetic_is_the_one_db2_and_ase_share():
    """
    `ENDING AT (x) INCLUSIVE`, `VALUES <= (x)` and a RANGE LEFT boundary are the same sentence in
    three dialects, so it stands once in partitioning.py.
    """
    assert ms.successor('100', 'INT') == '101'
    assert ms.successor("'2023-12-31'", 'DATE') == "'2024-01-01'"
    assert partitioning.next_discrete_value('100', partitioning.DISCRETE_INTEGER) == '101'


@pytest.mark.parametrize('type_name', ['DATETIME', 'DATETIME2', 'VARCHAR', 'DECIMAL(9,2)', ''])
def test_a_type_with_no_next_value_refuses(type_name):
    with pytest.raises(UntranslatableScheme):
        ms.successor("'x'", type_name)


def test_a_key_over_more_than_one_column_is_not_a_scheme_this_migrator_reads():
    """A SQL Server partition function takes exactly one input."""
    with pytest.raises(UntranslatableScheme, match='exactly one input'):
        ms.key_definition(['a', 'b'], str.lower)


def test_the_key_is_written_in_the_names_the_target_will_have():
    assert ms.key_definition(['OrderDate'], lambda name: name.lower()) == 'RANGE ("orderdate")'


# --------------------------------------------------------------------------------------
# the whole scheme


def partitions_of(count, rows=None, filegroup='PRIMARY', compression='NONE'):
    return [{'name': f'orders_p{index}', 'number': index,
             'rows': rows[index - 1] if rows else None,
             'filegroup': filegroup, 'compression': compression, 'target_bound': ''}
            for index in range(1, count + 1)]


def test_a_range_right_scheme_is_built_with_nothing_said_about_its_bounds():
    entries = partitions_of(3)
    key, notes, blockers = ms.build_scheme(
        'orders', ['order_date'], entries,
        [datetime.date(2023, 1, 1), datetime.date(2024, 1, 1)], True, 'DATE', str.lower)
    assert key == 'RANGE ("order_date")' and blockers == []
    assert entries[0]['target_bound'] == "FOR VALUES FROM (MINVALUE) TO ('2023-01-01')"
    assert not [note for note in notes if 'RANGE LEFT' in note]


def test_a_range_left_scheme_says_that_every_bound_was_moved():
    entries = partitions_of(3)
    key, notes, blockers = ms.build_scheme(
        'orders', ['acct'], entries, [100, 200], False, 'INT', str.lower)
    assert key == 'RANGE ("acct")' and blockers == []
    note = [text for text in notes if 'RANGE LEFT' in text]
    assert note and 'ending before 101' in note[0]


def test_a_range_left_scheme_over_a_type_with_no_next_value_builds_nothing():
    entries = partitions_of(2)
    key, _notes, blockers = ms.build_scheme(
        'orders', ['created'], entries,
        [datetime.datetime(2023, 12, 31, 23, 59, 59, 997000)], False, 'DATETIME', str.lower)
    assert key == ''
    assert any('RANGE LEFT' in issue for issue in blockers)
    assert entries[0]['target_bound'] == ''


def test_a_catalogue_which_does_not_add_up_builds_nothing():
    """
    A function with n boundaries makes n + 1 partitions. A catalogue which says otherwise has
    not been read correctly, and building from it would give the target a different scheme from
    the one the source has.
    """
    key, _notes, blockers = ms.build_scheme(
        'orders', ['acct'], partitions_of(5), [100, 200], True, 'INT', str.lower)
    assert key == ''
    assert any('do not agree' in issue for issue in blockers)


def test_a_table_whose_partitioning_column_the_catalogue_does_not_name_builds_nothing():
    key, _notes, blockers = ms.build_scheme(
        'orders', [], partitions_of(1), [], True, 'INT', str.lower)
    assert key == '' and blockers


# --------------------------------------------------------------------------------------
# the report


def test_the_report_names_the_function_the_scheme_and_the_direction():
    notes = ms.what_the_scheme_is('orders', 'ps_orders', 'pf_orders', False, ['order_date'],
                                 partitions_of(3), ['FG1', 'FG2'], [])
    assert any('ps_orders' in note and 'pf_orders' in note and 'RANGE LEFT' in note
               for note in notes)


def test_the_filegroups_are_named_and_reported_as_not_carried_over():
    notes = ms.what_the_scheme_is('orders', 'ps', 'pf', True, ['d'], partitions_of(2),
                                 ['FG1', 'FG2'], [])
    assert any('FG1, FG2' in note and 'default tablespace' in note for note in notes)


def test_per_partition_compression_is_reported():
    entries = partitions_of(2, compression='PAGE')
    notes = ms.what_the_scheme_is('orders', 'ps', 'pf', True, ['d'], entries, [], [])
    assert any('PAGE' in note and 'more bytes' in note for note in notes)


def test_a_non_aligned_unique_index_is_the_one_which_cannot_be_reproduced():
    """
    SQL Server's answer to the question Oracle answers with a global index - and PostgreSQL has
    neither.
    """
    notes = ms.what_the_scheme_is('orders', 'ps', 'pf', True, ['d'], partitions_of(2), [],
                                 [{'name': 'ix_orders_ref', 'is_unique': True}])
    note = [text for text in notes if 'ix_orders_ref' in text]
    assert note and 'global index' in note[0]
    assert 'refuses the table before it creates anything' in note[0]


def test_a_scheme_where_almost_every_row_sits_in_one_partition_is_reported():
    notes = ms.what_the_scheme_is('orders', 'ps', 'pf', True, ['d'],
                                 partitions_of(2, rows=[990000, 10]), [], [])
    note = [text for text in notes if 'sit in' in text]
    assert note and 'prunes nothing' in note[0]


def test_empty_partitions_are_counted_and_the_sliding_window_is_named():
    notes = ms.what_the_scheme_is('orders', 'ps', 'pf', True, ['d'],
                                 partitions_of(3, rows=[0, 100, 0]), [], [])
    assert any('sliding window' in note for note in notes)


def test_a_nullable_partitioning_column_is_a_finding_of_its_own():
    """
    SQL Server puts a row whose key is NULL in the lowest partition and PostgreSQL puts it in
    none at all. Whether the column really holds one is not something the catalogue answers,
    which is why this is said rather than refused.
    """
    notes = ms.what_a_nullable_key_costs('orders', 'shipped_at', True)
    assert notes and 'cannot be loaded' in notes[0]
    assert 'default_partition: true' in notes[0]
    assert ms.what_a_nullable_key_costs('orders', 'order_date', False) == []


# --------------------------------------------------------------------------------------
# the connector


class Catalogue:
    """A cursor over sys.*, answering by what the statement names."""

    def __init__(self, answers, raise_on=None):
        self.answers = answers
        self.raise_on = raise_on or ()
        self.rows = []
        self.statements = []

    def execute(self, statement, binds=None):
        self.statements.append(statement)
        for marker in self.raise_on:
            if marker in statement:
                raise Exception(f"Invalid object name '{marker}'")
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


def connector(answers, raise_on=None):
    made = MsSQLConnector.__new__(MsSQLConnector)
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


## scheme, function, function_id, boundary_value_on_right, fanout, column, type,
## max_length, precision, scale, is_nullable
SCHEME_RIGHT = ('ps_orders', 'pf_orders', 7, True, 3, 'order_date', 'date', 3, 10, 0, False)
## partition_number, rows, filegroup, compression
ORDERS_PARTITIONS = [(1, 5000, 'FG1', 'NONE'), (2, 6000, 'FG2', 'NONE'), (3, 100, 'FG3', 'NONE')]
ORDERS = [
    ('sys.partition_functions pf', [SCHEME_RIGHT]),
    ('sys.partition_range_values prv',
     [(1, datetime.date(2023, 1, 1)), (2, datetime.date(2024, 1, 1))]),
    ('sys.destination_data_spaces dds', ORDERS_PARTITIONS),
    ('i.data_space_id <> data_index.data_space_id', []),
]


def scheme_of(answers=None, raise_on=None, table='orders'):
    made = connector(answers or ORDERS, raise_on)
    return made, made.fetch_table_partitioning(
        {'source_schema_name': 'dbo', 'source_table_name': table})


def test_the_connector_reads_a_range_right_scheme():
    _made, scheme = scheme_of()
    assert scheme['is_partitioned'] is True and scheme['is_partition'] is False
    assert scheme['method'] == 'RANGE' and scheme['columns'] == ['order_date']
    assert scheme['target_key_definition'] == 'RANGE ("order_date")'
    assert scheme['partition_count'] == 3
    assert scheme['blockers'] == []
    assert scheme['engine_specific']['range_direction'] == 'RANGE RIGHT'


def test_every_partition_carries_both_spellings_of_its_bound():
    """
    SQL Server writes no bound on a partition at all - the function holds the boundaries and the
    partition has only its number - so the report is given the sentence the function means for
    that number, which is what a reader is looking for.
    """
    _made, scheme = scheme_of()
    assert [partition['bound'] for partition in scheme['partitions']] == [
        '< 2023-01-01',
        '>= 2023-01-01 and < 2024-01-01',
        '>= 2024-01-01',
    ]
    assert scheme['partitions'][0]['target_bound'] == "FOR VALUES FROM (MINVALUE) TO ('2023-01-01')"
    assert scheme['partitions'][0]['rows'] == 5000


def test_a_partition_is_given_a_name_because_sql_server_only_numbers_them():
    _made, scheme = scheme_of()
    assert [partition['name'] for partition in scheme['partitions']] == [
        'orders_p1', 'orders_p2', 'orders_p3']


def test_a_range_left_scheme_is_read_and_every_bound_moves():
    left = ('ps_acct', 'pf_acct', 8, False, 3, 'acct', 'int', 4, 10, 0, False)
    _made, scheme = scheme_of([
        ('sys.partition_functions pf', [left]),
        ('sys.partition_range_values prv', [(1, 100), (2, 200)]),
        ('sys.destination_data_spaces dds', ORDERS_PARTITIONS),
        ('i.data_space_id <> data_index.data_space_id', []),
    ])
    assert scheme['engine_specific']['range_direction'] == 'RANGE LEFT'
    assert [partition['target_bound'] for partition in scheme['partitions']] == [
        'FOR VALUES FROM (MINVALUE) TO (101)',
        'FOR VALUES FROM (101) TO (201)',
        'FOR VALUES FROM (201) TO (MAXVALUE)',
    ]
    assert [partition['bound'] for partition in scheme['partitions']] == [
        '<= 100', '> 100 and <= 200', '> 200']


def test_a_table_which_sits_on_no_partition_scheme_answers_nothing():
    _made, scheme = scheme_of([('sys.partition_functions pf', [])], table='customers')
    assert scheme == {}


def test_a_nullable_partitioning_column_is_reported():
    nullable = ('ps', 'pf', 9, True, 2, 'shipped_at', 'date', 3, 10, 0, True)
    _made, scheme = scheme_of([
        ('sys.partition_functions pf', [nullable]),
        ('sys.partition_range_values prv', [(1, datetime.date(2024, 1, 1))]),
        ('sys.destination_data_spaces dds', [(1, 1, 'FG1', 'NONE'), (2, 1, 'FG1', 'NONE')]),
        ('i.data_space_id <> data_index.data_space_id', []),
    ])
    assert any('NULLABLE' in note and 'cannot be loaded' in note for note in scheme['notes'])


def test_a_non_aligned_index_is_read_and_reported():
    _made, scheme = scheme_of([
        ('sys.partition_functions pf', [SCHEME_RIGHT]),
        ('sys.partition_range_values prv',
         [(1, datetime.date(2023, 1, 1)), (2, datetime.date(2024, 1, 1))]),
        ('sys.destination_data_spaces dds', ORDERS_PARTITIONS),
        ('i.data_space_id <> data_index.data_space_id', [('ix_orders_ref', True)]),
    ])
    assert scheme['engine_specific']['unaligned_indexes'] == [
        {'name': 'ix_orders_ref', 'is_unique': True}]
    assert any('NON-ALIGNED' in note for note in scheme['notes'])


def test_the_partitioned_tables_of_a_schema_are_listed_in_one_query():
    made = connector([('sys.partition_schemes ps', [('orders',), ('events',)])])
    assert made.fetch_partitioning_candidates('dbo') == {'orders', 'events'}
    assert len(made.cursor.statements) == 1


def test_a_catalogue_which_cannot_be_read_answers_nothing_and_says_so():
    made, scheme = scheme_of(ORDERS, raise_on=('sys.partition_functions pf',))
    assert scheme == {}
    assert any(level == 'ERROR' for level, _text in made.messages)


def test_non_aligned_indexes_which_cannot_be_listed_do_not_stop_the_read():
    made, scheme = scheme_of(ORDERS, raise_on=('i.data_space_id <> data_index.data_space_id',))
    assert scheme['target_key_definition'] == 'RANGE ("order_date")'
    assert any(level == 'DEBUG' for level, _text in made.messages)


# --------------------------------------------------------------------------------------
# the facts of §4.4


FACTS = [
    ('sum(p.rows)', [(400000,)]),
    ('c.is_nullable, c.is_computed', [
        ## name, type, precision, scale, is_nullable, is_computed
        ('order_id', 'int', 10, 0, False, False),
        ('order_date', 'date', 10, 0, False, False),
        ('shipped_at', 'date', 10, 0, True, False),
        ('total', 'decimal', 9, 2, True, True),
        ('document', 'xml', 0, 0, True, False),
        ('note', 'nvarchar', 0, 0, True, False),
    ]),
    ('i.is_unique = 1', [
        ('pk_orders', True, 'order_id', 1),
        ('pk_orders', True, 'order_date', 2),
        ('ux_orders_ref', False, 'order_ref', 1),
    ]),
    ('sys.foreign_keys fk', [('fk_order_items', 'order_items')]),
]


def facts_of(answers=None, raise_on=None):
    made = connector(answers or FACTS, raise_on)
    return made.fetch_partitioning_facts(
        {'source_schema_name': 'dbo', 'source_table_name': 'orders'})


def test_the_facts_of_a_table_come_out_of_the_catalogue():
    facts = facts_of()
    assert facts['row_estimate'] == 400000
    assert facts['columns']['order_date']['type_name'] == 'DATE'
    assert facts['columns']['total']['type_name'] == 'DECIMAL(9,2)'
    assert facts['date_range_types'] == ms.DATE_RANGE_TYPES


def test_nullability_is_read():
    facts = facts_of()
    assert facts['columns']['order_date']['not_null'] is True
    assert facts['columns']['shipped_at']['not_null'] is False


def test_a_computed_column_is_a_generated_one():
    assert facts_of()['columns']['total']['is_generated'] is True
    assert facts_of()['columns']['order_id']['is_generated'] is False


def test_a_type_which_cannot_carry_a_partition_key_says_so():
    facts = facts_of()
    assert facts['columns']['document']['has_btree_opclass'] is False
    assert facts['columns']['note']['has_btree_opclass'] is True


def test_sql_server_keeps_no_null_count_so_the_check_is_reported_as_not_made():
    assert all(column['null_fraction'] is None for column in facts_of()['columns'].values())


def test_every_unique_key_is_read_with_its_columns_in_order():
    keys = {key['name']: key for key in facts_of()['unique_keys']}
    assert keys['pk_orders']['is_primary'] is True
    assert keys['pk_orders']['columns'] == ['order_id', 'order_date']
    assert keys['ux_orders_ref']['is_primary'] is False


def test_what_references_the_table_is_read():
    assert facts_of()['referenced_by'] == [{'name': 'fk_order_items', 'table': 'order_items'}]


def test_facts_which_cannot_be_read_are_answered_as_not_read():
    assert facts_of(raise_on=('c.is_nullable, c.is_computed',)) is None


# --------------------------------------------------------------------------------------
# the whole way through


PARTITION_SQL = ('CREATE TABLE "{target_schema_name}"."{target_table_name}" PARTITION OF '
                 '"{target_schema_name}"."{parent_table_name}" {partition_bound}')


def test_the_create_table_of_a_preserved_table_and_its_partitions():
    from credativ_pg_migrator.planner import Planner

    _made, scheme = scheme_of()
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
    assert clause == ' PARTITION BY RANGE ("order_date")'
    assert statements == [
        'CREATE TABLE "migtest"."orders_p1" PARTITION OF "migtest"."orders" '
        "FOR VALUES FROM (MINVALUE) TO ('2023-01-01')",
        'CREATE TABLE "migtest"."orders_p2" PARTITION OF "migtest"."orders" '
        "FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')",
        'CREATE TABLE "migtest"."orders_p3" PARTITION OF "migtest"."orders" '
        "FOR VALUES FROM ('2024-01-01') TO (MAXVALUE)",
    ]


def test_a_scheme_which_cannot_be_built_stops_only_a_run_which_would_build_it():
    left = ('ps', 'pf', 8, False, 2, 'created', 'datetime', 8, 23, 3, False)
    _made, scheme = scheme_of([
        ('sys.partition_functions pf', [left]),
        ('sys.partition_range_values prv',
         [(1, datetime.datetime(2023, 12, 31, 23, 59, 59, 997000))]),
        ('sys.destination_data_spaces dds', [(1, 1, 'FG1', 'NONE'), (2, 1, 'FG1', 'NONE')]),
        ('i.data_space_id <> data_index.data_space_id', []),
    ])
    preserved = partitioning.build_plan(
        {'orders': scheme}, ['orders'], mode_of=lambda name: 'preserve',
        target_version_num=160000)['orders']
    assert preserved.issues
    flattened = partitioning.build_plan(
        {'orders': scheme}, ['orders'], mode_of=lambda name: 'flatten',
        target_version_num=160000)['orders']
    assert flattened.issues == []
    ## and what the source really has is still said
    assert any('RANGE LEFT' in warning for warning in flattened.warnings)


def test_a_preserved_key_is_checked_against_the_primary_key():
    """
    §3.1. SQL Server requires an ALIGNED unique index to contain the partitioning column and
    permits a non-aligned one which does not - so this check is made rather than assumed.
    """
    _made, scheme = scheme_of()
    decision = partitioning.build_plan(
        {'orders': scheme}, ['orders'], mode_of=lambda name: 'preserve',
        target_version_num=160000)['orders']
    partitioning.check_preserved_keys(
        decision, [{'name': 'ux_orders_ref', 'columns': ['order_ref'], 'is_primary': False}])
    assert any('ux_orders_ref' in issue and 'order_date' in issue for issue in decision.issues)
