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
The semantic partitioning of Sybase ASE: what the catalogue says, and what PostgreSQL is given.

§2.4 of development/PARTITIONING_STRATEGY.md says two things about this source which pull in
opposite directions - the engagement behind this repository's query conversion work is a Sybase
ASE one, so it is wanted; and *"catalogue names not verified against a live server"*, so what
reads it is written from documentation. This file is what makes that safe rather than merely
admitted: the reading is tiered and its failure is a first-class outcome, and the thing which
would be BUILT out of what could not be read is refused instead.

The one distinction the whole design turns on: **a partition condition which came back empty is
not the same as a condition which could not be read.** The first means the scheme has no
conditions, which is HASH; the second means nothing at all. A HASH built out of a RANGE nobody
could read would load every row into the wrong partition and not one step of the run would fail.

And the trap it shares with Db2: ASE writes `VALUES <= (x)` and means that x is IN the
partition, while PostgreSQL's `TO (b)` means b is not - so each end is converted rather than
copied, and where the column type has no next value the scheme is refused.

Nothing here needs a database or a driver.
"""

import os
import sys
import pytest
from unittest.mock import MagicMock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator import partitioning
from credativ_pg_migrator.connectors import sybase_partitioning as ase
from credativ_pg_migrator.connectors.sybase_partitioning import UntranslatableScheme
from credativ_pg_migrator.connectors.sybase_ase_connector import SybaseASEConnector


def parts(items):
    """[(name, condition, segment, rows)] as build_scheme() wants them."""
    return [{'name': name, 'condition': condition, 'segment': segment, 'rows': rows,
             'target_bound': ''}
            for name, condition, segment, rows in items]


def built(columns, types, items, conditions_were_read=True, table='ORDERS'):
    entries = parts(items)
    method = ase.method_of([entry['condition'] for entry in entries], bool(columns),
                           conditions_were_read)
    key, notes, blockers = ase.build_scheme(table, method, columns, entries, types, str.lower,
                                            conditions_were_read)
    return {'method': method, 'key': key, 'notes': notes, 'blockers': blockers,
            'bounds': [entry['target_bound'] for entry in entries]}


# --------------------------------------------------------------------------------------
# which method a table is partitioned by, worked out from what could be read


def test_a_table_with_no_partitioning_key_is_round_robin():
    """It is the one method of the four with no key, which is what lets this be inferred."""
    assert ase.method_of(['', ''], has_key_columns=False, conditions_were_read=True) == \
        ase.ROUND_ROBIN


def test_a_key_and_no_conditions_is_hash():
    assert ase.method_of(['', '', ''], has_key_columns=True, conditions_were_read=True) == \
        ase.HASH


@pytest.mark.parametrize('conditions, expected', [
    (['VALUES <= (100)', 'VALUES <= (MAX)'], ase.RANGE),
    (['<= 100', '<= 200'], ase.RANGE),
    (["VALUES ('DE','FR')", "VALUES ('US')"], ase.LIST),
    (["('DE')", "('US')"], ase.LIST),
])
def test_a_key_and_conditions_are_range_or_list_by_the_shape_of_the_condition(
        conditions, expected):
    assert ase.method_of(conditions, has_key_columns=True, conditions_were_read=True) == expected


def test_a_condition_which_could_not_be_read_is_not_the_same_as_one_which_is_empty():
    """
    The distinction the whole design turns on. An empty condition means the scheme has none,
    which is HASH; a read which did not happen means nothing at all - and a HASH built out of a
    RANGE nobody could read would put every row in the wrong partition without one step of the
    run failing.
    """
    assert ase.method_of(['', ''], has_key_columns=True, conditions_were_read=True) == ase.HASH
    assert ase.method_of(['', ''], has_key_columns=True, conditions_were_read=False) == ase.UNKNOWN


def test_a_mixture_of_conditions_is_not_a_method_this_migrator_will_name():
    assert ase.method_of(['VALUES <= (100)', "VALUES ('DE')"],
                         has_key_columns=True, conditions_were_read=True) == ase.UNKNOWN


# --------------------------------------------------------------------------------------
# one value of a bound


@pytest.mark.parametrize('written, expected', [
    ('100', '100'), ('-1.5', '-1.5'), ("'DE'", "'DE'"), ("'O''BRIEN'", "'O''BRIEN'"),
    ('MAX', 'MAXVALUE'), ('MAXVALUE', 'MAXVALUE'), ('NULL', 'NULL'),
    ## ASE takes a double-quoted string when quoted_identifier is off, and PostgreSQL would read
    ## it as the name of a column
    ('"DE"', "'DE'"),
])
def test_a_bound_value_is_written_the_way_postgresql_writes_it(written, expected):
    assert ase.to_postgresql_value(written) == expected


@pytest.mark.parametrize('written', ['getdate()', 'col + 1', ''])
def test_a_bound_which_is_not_a_literal_is_refused(written):
    with pytest.raises(UntranslatableScheme):
        ase.to_postgresql_value(written)


# --------------------------------------------------------------------------------------
# the next value, which is what VALUES <= has to become


@pytest.mark.parametrize('value, type_name, expected', [
    ('100', 'INT', '101'),
    ('100', 'BIGINT', '101'),
    ("'2023-12-31'", 'DATE', "'2024-01-01'"),
    ("'2024-02-29'", 'DATE', "'2024-03-01'"),
    ('99', 'NUMERIC(5,0)', '100'),
    ('99', 'NUMERIC', '100'),
])
def test_the_next_value_of_a_type_which_has_one(value, type_name, expected):
    assert ase.successor(value, type_name) == expected


@pytest.mark.parametrize('type_name', [
    'VARCHAR(2)', 'NUMERIC(9,2)',
    ## ASE counts a datetime in 1/300 of a second and a bigdatetime in microseconds, and a bound
    ## written without that precision does not say which of them the next value is
    'DATETIME', 'BIGDATETIME', 'FLOAT', '',
])
def test_a_type_with_no_next_value_refuses_rather_than_moving_the_bound(type_name):
    with pytest.raises(UntranslatableScheme):
        ase.successor("'x'", type_name)


def test_the_arithmetic_is_the_one_db2_shares():
    """
    `ENDING AT (x) INCLUSIVE` and `VALUES <= (x)` are the same sentence in two dialects, so the
    arithmetic stands once in partitioning.py and what differs is only the type names.
    """
    assert partitioning.next_discrete_value('100', partitioning.DISCRETE_INTEGER) == '101'
    assert partitioning.next_discrete_value("'2023-12-31'", partitioning.DISCRETE_DATE) == \
        "'2024-01-01'"


# --------------------------------------------------------------------------------------
# the whole scheme


def test_an_inclusive_range_becomes_the_exclusive_one_which_holds_the_same_rows():
    """
    `VALUES <= (100)` puts 100 IN the partition. Copied across, the target refuses every row
    with acct = 100, 200 and so on - one row at a time, at the end of a migration which has
    already moved the rest.
    """
    scheme = built(['acct'], {'acct': 'INT'},
                   [('p1', 'VALUES <= (100)', 'seg1', 10),
                    ('p2', 'VALUES <= (200)', 'seg2', 10),
                    ('p3', 'VALUES <= (MAX)', 'seg3', 10)])
    assert scheme['method'] == ase.RANGE and scheme['key'] == 'RANGE ("acct")'
    assert scheme['bounds'] == ['FOR VALUES FROM (MINVALUE) TO (101)',
                                'FOR VALUES FROM (101) TO (201)',
                                'FOR VALUES FROM (201) TO (MAXVALUE)']
    assert scheme['blockers'] == []
    assert any('VALUES <= (100) becomes TO (101)' in note for note in scheme['notes'])


def test_a_range_over_a_date_counts_in_days():
    scheme = built(['d'], {'d': 'DATE'},
                   [('p1', "VALUES <= ('2023-12-31')", 'seg1', 1),
                    ('p2', 'VALUES <= (MAX)', 'seg2', 1)])
    assert scheme['bounds'][0] == "FOR VALUES FROM (MINVALUE) TO ('2024-01-01')"


def test_a_range_over_a_type_with_no_next_value_is_refused_rather_than_moved():
    scheme = built(['r'], {'r': 'VARCHAR(2)'}, [('p1', "VALUES <= ('DE')", 'seg1', 1)])
    assert scheme['key'] == ''
    assert any('p1' in issue and 'always exclusive' in issue for issue in scheme['blockers'])


def test_a_list_scheme_keeps_its_values():
    scheme = built(['region'], {'region': 'VARCHAR(2)'},
                   [('p1', "VALUES ('DE', 'FR')", 'seg1', 1),
                    ('p2', "VALUES ('US')", 'seg2', 1)])
    assert scheme['method'] == ase.LIST and scheme['key'] == 'LIST ("region")'
    assert scheme['bounds'] == ["FOR VALUES IN ('DE', 'FR')", "FOR VALUES IN ('US')"]


def test_a_list_key_over_more_than_one_column_is_refused():
    scheme = built(['a', 'b'], {'a': 'INT', 'b': 'INT'},
                   [('p1', "VALUES ('DE')", 'seg1', 1)])
    assert any('exactly one column' in issue for issue in scheme['blockers'])


def test_a_hash_scheme_carries_its_count_and_says_the_rows_move():
    scheme = built(['id'], {'id': 'INT'},
                   [('p1', '', 'seg1', 1), ('p2', '', 'seg2', 1), ('p3', '', 'seg3', 1)])
    assert scheme['method'] == ase.HASH and scheme['key'] == 'HASH ("id")'
    assert scheme['bounds'] == [f'FOR VALUES WITH (MODULUS 3, REMAINDER {index})'
                                for index in range(3)]
    assert any('sits in another here' in note for note in scheme['notes'])


def test_round_robin_has_no_key_at_all():
    scheme = built([], {}, [('p1', '', 'seg1', 1), ('p2', '', 'seg2', 1)])
    assert scheme['method'] == ase.ROUND_ROBIN and scheme['key'] == ''
    assert any('no partitioning key' in issue for issue in scheme['blockers'])
    assert any('source_partitioning: flatten' in issue for issue in scheme['blockers'])


def test_a_scheme_whose_conditions_could_not_be_read_builds_nothing_and_says_why():
    """
    RANGE, LIST and HASH are built from three different things. Building one of them out of a
    scheme which might be another is the one wrong answer with no symptom at all.
    """
    scheme = built(['id'], {'id': 'INT'}, [('p1', '', 'seg1', 1), ('p2', '', 'seg2', 1)],
                   conditions_were_read=False)
    assert scheme['method'] == ase.UNKNOWN and scheme['key'] == ''
    issue = scheme['blockers'][0]
    assert 'could not read which method' in issue
    assert 'wrong partition without a single step of the run failing' in issue


def test_a_range_scheme_ending_in_max_needs_nothing_said_about_its_end():
    scheme = built(['acct'], {'acct': 'INT'},
                   [('p1', 'VALUES <= (100)', 'seg1', 1), ('p2', 'VALUES <= (MAX)', 'seg2', 1)])
    assert not [note for note in scheme['notes'] if 'past that bound is refused' in note]


def test_a_range_scheme_which_stops_says_where_it_stops():
    scheme = built(['acct'], {'acct': 'INT'},
                   [('p1', 'VALUES <= (100)', 'seg1', 1), ('p2', 'VALUES <= (200)', 'seg2', 1)])
    assert any('past that bound is refused' in note for note in scheme['notes'])


# --------------------------------------------------------------------------------------
# the report


def test_the_report_says_a_partition_is_a_place_as_well_as_a_class_of_row():
    notes = ase.what_the_partitioning_is(
        'ORDERS', ase.RANGE, parts([('p1', 'VALUES <= (1)', 'seg1', 1)]), ['seg1', 'seg2'], True)
    assert any('I/O across devices' in note and 'parallel scans' in note for note in notes)
    assert any('NOT carried over' in note and 'default tablespace' in note for note in notes)


def test_the_report_says_when_the_conditions_could_not_be_read():
    """P2-8, with a sharper edge: what was not read is what the target would be built from."""
    notes = ase.what_the_partitioning_is(
        'ORDERS', ase.UNKNOWN, parts([('p1', '', 'seg1', 1)]), ['seg1'], False)
    assert any('could NOT be read from this server' in note for note in notes)
    assert any('written from the documentation of ASE' in note for note in notes)


def test_a_scheme_where_almost_every_row_sits_in_one_partition_is_reported():
    notes = ase.what_the_partitioning_is('ORDERS', ase.RANGE, parts([
        ('big', 'VALUES <= (1)', 'seg1', 990000),
        ('small', 'VALUES <= (MAX)', 'seg2', 10)]), ['seg1'], True)
    note = [text for text in notes if 'sit in' in text]
    assert note and 'prunes nothing' in note[0]


def test_row_counts_nobody_could_read_are_reported_as_not_known():
    notes = ase.what_the_partitioning_is(
        'ORDERS', ase.RANGE, parts([('p1', 'VALUES <= (1)', 'seg1', None)]), ['seg1'], True)
    assert any('NOT known' in note for note in notes)


# --------------------------------------------------------------------------------------
# the connector


class Catalogue:
    """A cursor over the ASE system catalogue, answering by what the statement names."""

    def __init__(self, answers, raise_on=None):
        self.answers = answers
        self.raise_on = raise_on or ()
        self.rows = []
        self.description = None
        self.statements = []

    def execute(self, statement, binds=None):
        self.statements.append(statement)
        for marker in self.raise_on:
            if marker in statement:
                raise Exception(f"Incorrect syntax near '{marker}'")
        for entry in self.answers:
            marker, rows = entry[0], entry[1]
            if marker in statement:
                self.rows = rows
                names = entry[2] if len(entry) > 2 else None
                self.description = [(name,) for name in names] if names else None
                return
        raise AssertionError(f'the test has no answer for this statement:\n{statement}')

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)

    def close(self):
        pass


def connector(answers, raise_on=None):
    made = SybaseASEConnector.__new__(SybaseASEConnector)
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


## partitionid, name, segment, rows
ORDERS_PARTITIONS = [(1, 'p1', 'seg1', 5000), (2, 'p2', 'seg2', 6000), (3, 'p3', 'seg3', 100)]
ORDERS = [
    ('SELECT o.id FROM sysobjects o', [(1234,)]),
    ('row_count(db_id(), p.id', ORDERS_PARTITIONS),
    ('syspartitionkeys k', [('acct',)]),
    ('p.condition FROM syspartitions',
     [(1, 'VALUES <= (100)'), (2, 'VALUES <= (200)'), (3, 'VALUES <= (MAX)')],
     ['partitionid', 'condition']),
    ('LEFT JOIN systypes t ON t.usertype = c.usertype', [('acct', 'int', None, None)]),
]


def scheme_of(answers=None, raise_on=None, table='ORDERS'):
    made = connector(answers or ORDERS, raise_on)
    return made, made.fetch_table_partitioning(
        {'source_schema_name': 'dbo', 'source_table_name': table})


def test_the_connector_reads_a_range_scheme():
    _made, scheme = scheme_of()
    assert scheme['is_partitioned'] is True and scheme['is_partition'] is False
    assert scheme['method'] == ase.RANGE and scheme['columns'] == ['acct']
    assert scheme['target_key_definition'] == 'RANGE ("acct")'
    assert scheme['partition_count'] == 3
    assert scheme['blockers'] == []


def test_every_partition_carries_both_spellings_of_its_bound_and_its_segment():
    _made, scheme = scheme_of()
    first = scheme['partitions'][0]
    assert first['bound'] == 'VALUES <= (100)'
    assert first['target_bound'] == 'FOR VALUES FROM (MINVALUE) TO (101)'
    assert first['rows'] == 5000
    assert scheme['engine_specific']['segments'] == ['seg1', 'seg2', 'seg3']


def test_a_table_with_one_data_partition_is_not_partitioned():
    """Every unpartitioned ASE table has exactly one data partition."""
    _made, scheme = scheme_of([
        ('SELECT o.id FROM sysobjects o', [(1234,)]),
        ('row_count(db_id(), p.id', [(1, 'p1', 'seg1', 100)]),
    ])
    assert scheme == {}


def test_a_table_which_is_not_there_answers_nothing():
    _made, scheme = scheme_of([('SELECT o.id FROM sysobjects o', [])])
    assert scheme == {}


def test_a_round_robin_table_is_read_reported_and_refused():
    _made, scheme = scheme_of([
        ('SELECT o.id FROM sysobjects o', [(1234,)]),
        ('row_count(db_id(), p.id', ORDERS_PARTITIONS),
        ('syspartitionkeys k', []),
        ('p.condition FROM syspartitions', [], ['partitionid', 'condition']),
        ('LEFT JOIN systypes t ON t.usertype = c.usertype', []),
    ])
    assert scheme['method'] == ase.ROUND_ROBIN
    assert scheme['target_key_definition'] == ''
    assert any('ROUNDROBIN' in issue for issue in scheme['blockers'])
    assert any('parallel scans' in note for note in scheme['notes'])


def test_the_conditions_are_read_through_the_second_way_when_the_first_is_refused():
    """
    §2.4 records that these catalogue names were never verified against a live server, so the
    read is tiered - and the second way is ASE's own reporting procedure, whose columns are
    found by NAME because their order differs between releases.
    """
    made, scheme = scheme_of([
        ('SELECT o.id FROM sysobjects o', [(1234,)]),
        ('row_count(db_id(), p.id', ORDERS_PARTITIONS),
        ('syspartitionkeys k', [('acct',)]),
        ('sp_helpartition',
         [('p1', 12, 'seg1', 'VALUES <= (100)'),
          ('p2', 13, 'seg2', 'VALUES <= (200)'),
          ('p3', 14, 'seg3', 'VALUES <= (MAX)')],
         ['partition_name', 'pages', 'segment', 'partition_conditions']),
        ('LEFT JOIN systypes t ON t.usertype = c.usertype', [('acct', 'int', None, None)]),
    ], raise_on=('p.condition',))
    assert scheme['method'] == ase.RANGE
    assert scheme['partitions'][0]['target_bound'] == 'FOR VALUES FROM (MINVALUE) TO (101)'
    assert any(level == 'DEBUG' for level, _text in made.messages)


def test_conditions_which_no_way_could_read_stop_the_run_rather_than_being_guessed():
    """
    The failure §2.4's warning makes likely, made safe: the scheme is reported in full - method
    unknown, partitions, segments, rows - and nothing is built out of what was not read.
    """
    made, scheme = scheme_of([
        ('SELECT o.id FROM sysobjects o', [(1234,)]),
        ('row_count(db_id(), p.id', ORDERS_PARTITIONS),
        ('syspartitionkeys k', [('acct',)]),
        ('LEFT JOIN systypes t ON t.usertype = c.usertype', [('acct', 'int', None, None)]),
    ], raise_on=('p.condition', 'sp_helpartition'))
    assert scheme['method'] == 'UNKNOWN'
    assert scheme['target_key_definition'] == ''
    assert scheme['partition_count'] == 3
    assert [partition['rows'] for partition in scheme['partitions']] == [5000, 6000, 100]
    assert scheme['engine_specific']['conditions_were_read'] is False
    assert any('could not read which method' in issue for issue in scheme['blockers'])
    assert any('could NOT be read from this server' in note for note in scheme['notes'])


def test_an_answer_which_holds_no_condition_column_is_not_taken_for_an_empty_one():
    """
    A result set which has no column named for a condition has not answered the question - and
    reading it as "there are no conditions" would make a RANGE scheme look like a HASH one.
    """
    made, scheme = scheme_of([
        ('SELECT o.id FROM sysobjects o', [(1234,)]),
        ('row_count(db_id(), p.id', ORDERS_PARTITIONS),
        ('syspartitionkeys k', [('acct',)]),
        ('p.condition FROM syspartitions', [(1, 'x')], ['partitionid', 'something_else']),
        ('sp_helpartition', [('p1', 12)], ['partition_name', 'pages']),
        ('LEFT JOIN systypes t ON t.usertype = c.usertype', [('acct', 'int', None, None)]),
    ])
    assert scheme['method'] == 'UNKNOWN'
    assert scheme['engine_specific']['conditions_were_read'] is False


def test_the_rows_of_a_partition_are_not_known_where_the_server_refuses_the_count():
    made, scheme = scheme_of([
        ('SELECT o.id FROM sysobjects o', [(1234,)]),
        ('NULL\n            FROM syspartitions p',
         [(1, 'p1', 'seg1', None), (2, 'p2', 'seg2', None)]),
        ('syspartitionkeys k', [('acct',)]),
        ('p.condition FROM syspartitions',
         [(1, 'VALUES <= (100)'), (2, 'VALUES <= (MAX)')], ['partitionid', 'condition']),
        ('LEFT JOIN systypes t ON t.usertype = c.usertype', [('acct', 'int', None, None)]),
    ], raise_on=('row_count(db_id(), p.id',))
    assert [partition['rows'] for partition in scheme['partitions']] == [None, None]
    assert any('NOT known' in note for note in scheme['notes'])


def test_the_partitioned_tables_of_a_schema_are_listed_in_one_query():
    made = connector([('HAVING count(*) > 1', [('ORDERS',), ('EVENTS',)])])
    assert made.fetch_partitioning_candidates('dbo') == {'ORDERS', 'EVENTS'}
    assert len(made.cursor.statements) == 1


def test_a_catalogue_which_cannot_be_read_answers_nothing_and_says_so():
    made, scheme = scheme_of(ORDERS, raise_on=('SELECT o.id FROM sysobjects o',))
    assert scheme == {}
    assert any(level == 'ERROR' for level, _text in made.messages)


# --------------------------------------------------------------------------------------
# the facts of §4.4


FACTS = [
    ('SELECT o.id FROM sysobjects o', [(1234,)]),
    ('SELECT row_count(db_id(), 1234)', [(400000,)]),
    ('c.status, c.status2', [
        ## name, type, prec, scale, status, status2
        ('order_id', 'int', None, None, 0, 0),
        ('acct', 'int', None, None, 0, 0),
        ('shipped_at', 'datetime', None, None, 0x08, 0),
        ('total', 'numeric', 9, 2, 0x08, 0x01),
        ('document', 'text', None, None, 0x08, 0),
        ('note', 'varchar', None, None, 0x08, 0),
    ]),
    ('FROM sysindexes i', [
        ('orders_pk', 1, 2 | 2048, 'order_id', None, None, None, None, None, None, None),
        ('orders_ref_uq', 2, 2, 'order_ref', 'acct', None, None, None, None, None, None),
    ]),
    ('FROM sysreferences r', [('order_item_fk', 'order_items')]),
]


def facts_of(answers=None, raise_on=None):
    made = connector(answers or FACTS, raise_on)
    return made.fetch_partitioning_facts(
        {'source_schema_name': 'dbo', 'source_table_name': 'ORDERS'})


def test_the_facts_of_a_table_come_out_of_the_system_catalogue():
    facts = facts_of()
    assert facts['row_estimate'] == 400000
    assert facts['columns']['acct']['type_name'] == 'INT'
    assert facts['columns']['total']['type_name'] == 'NUMERIC(9,2)'
    assert facts['date_range_types'] == ase.DATE_RANGE_TYPES


def test_the_nulls_allowed_flag_is_the_bit_ase_keeps_it_in():
    facts = facts_of()
    assert facts['columns']['order_id']['not_null'] is True
    assert facts['columns']['shipped_at']['not_null'] is False


def test_a_computed_column_is_a_generated_one():
    facts = facts_of()
    assert facts['columns']['total']['is_generated'] is True
    assert facts['columns']['order_id']['is_generated'] is False


def test_ase_keeps_no_null_count_so_every_column_says_the_check_was_not_made():
    assert all(column['null_fraction'] is None for column in facts_of()['columns'].values())


def test_a_type_which_cannot_carry_a_partition_key_says_so():
    facts = facts_of()
    assert facts['columns']['document']['has_btree_opclass'] is False
    assert facts['columns']['note']['has_btree_opclass'] is True


def test_every_unique_key_is_read_with_its_columns_in_order():
    keys = {key['name']: key for key in facts_of()['unique_keys']}
    assert keys['orders_pk']['is_primary'] is True
    assert keys['orders_pk']['columns'] == ['order_id']
    assert keys['orders_ref_uq']['is_primary'] is False
    assert keys['orders_ref_uq']['columns'] == ['order_ref', 'acct']


def test_what_references_the_table_is_read():
    assert facts_of()['referenced_by'] == [{'name': 'order_item_fk', 'table': 'order_items'}]


def test_facts_which_cannot_be_read_are_answered_as_not_read():
    assert facts_of(raise_on=('c.status, c.status2',)) is None


# --------------------------------------------------------------------------------------
# the whole way through


PARTITION_SQL = ('CREATE TABLE "{target_schema_name}"."{target_table_name}" PARTITION OF '
                 '"{target_schema_name}"."{parent_table_name}" {partition_bound}')


def test_the_create_table_of_a_preserved_table_and_its_partitions():
    from credativ_pg_migrator.planner import Planner

    _made, scheme = scheme_of()
    plan = partitioning.build_plan(
        {'ORDERS': scheme}, ['ORDERS'], mode_of=lambda name: 'preserve',
        target_version_num=160000)
    assert plan['ORDERS'].issues == []

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

    clause, statements = planner.partitioning_clause_for(plan['ORDERS'], 'orders')
    assert clause == ' PARTITION BY RANGE ("acct")'
    assert statements == [
        'CREATE TABLE "migtest"."p1" PARTITION OF "migtest"."orders" '
        'FOR VALUES FROM (MINVALUE) TO (101)',
        'CREATE TABLE "migtest"."p2" PARTITION OF "migtest"."orders" '
        'FOR VALUES FROM (101) TO (201)',
        'CREATE TABLE "migtest"."p3" PARTITION OF "migtest"."orders" '
        'FOR VALUES FROM (201) TO (MAXVALUE)',
    ]


def test_a_scheme_which_cannot_be_built_stops_only_a_run_which_would_build_it():
    _made, scheme = scheme_of([
        ('SELECT o.id FROM sysobjects o', [(1234,)]),
        ('row_count(db_id(), p.id', ORDERS_PARTITIONS),
        ('syspartitionkeys k', []),
        ('p.condition FROM syspartitions', [], ['partitionid', 'condition']),
        ('LEFT JOIN systypes t ON t.usertype = c.usertype', []),
    ])
    preserved = partitioning.build_plan(
        {'ORDERS': scheme}, ['ORDERS'], mode_of=lambda name: 'preserve',
        target_version_num=160000)['ORDERS']
    assert preserved.issues
    flattened = partitioning.build_plan(
        {'ORDERS': scheme}, ['ORDERS'], mode_of=lambda name: 'flatten',
        target_version_num=160000)['ORDERS']
    assert flattened.issues == []
    ## and what the source really has is still said
    assert any('parallel scans' in warning for warning in flattened.warnings)


def test_a_preserved_key_is_checked_against_the_primary_key():
    _made, scheme = scheme_of()
    decision = partitioning.build_plan(
        {'ORDERS': scheme}, ['ORDERS'], mode_of=lambda name: 'preserve',
        target_version_num=160000)['ORDERS']
    partitioning.check_preserved_keys(
        decision, [{'name': 'orders_pk', 'columns': ['order_id'], 'is_primary': True}])
    assert any('orders_pk' in issue and 'acct' in issue for issue in decision.issues)
