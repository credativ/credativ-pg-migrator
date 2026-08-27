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
The partitioning of the three Db2 connectors: what the source says, and what PostgreSQL is given.

§2.4 of development/PARTITIONING_STRATEGY.md puts the Db2 family third and calls it one family
with one shape of answer read three ways: LUW out of a live SYSCAT, z/OS and for i out of the
`PARTITION BY` clause of a DDL extract, which for those two connectors IS the catalogue.

Two things this file exists to hold them to. **Db2's upper bound is INCLUSIVE by default and
PostgreSQL's is never inclusive**, so a scheme copied bound for bound gives a target which
refuses every row of the last day of every partition - and where the type has no next value, the
scheme is refused rather than moved by a day. And **three mechanisms of Db2 all say "partition"**
- table partitioning, DPF and MDC - of which one has a counterpart; each gets its own sentence
rather than one line claiming the table is partitioned.

Nothing here needs a database or a Db2 client.
"""

import os
import sys
import pytest
from unittest.mock import MagicMock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator import partitioning
from credativ_pg_migrator.connectors import db2_partitioning
from credativ_pg_migrator.connectors.db2_partitioning import UntranslatableScheme
from credativ_pg_migrator.connectors.ibm_db2_luw_connector import IbmDb2LuwConnector
from credativ_pg_migrator.connectors.ibm_db2_zos_connector import IbmDb2ZosConnector
from credativ_pg_migrator.connectors.ibm_db2_i_connector import IbmDb2IConnector


LOWER = staticmethod(lambda name: (name or '').lower())


# --------------------------------------------------------------------------------------
# one value of a bound


@pytest.mark.parametrize('written, expected', [
    ("'2024-01-01'", "'2024-01-01'"),
    ## Db2 wraps a bound in brackets and PostgreSQL writes its own
    ("('2024-01-01')", "'2024-01-01'"),
    ('199', '199'),
    ('(199)', '199'),
    ('-1.5', '-1.5'),
    ('MAXVALUE', 'MAXVALUE'),
    ('minvalue', 'MINVALUE'),
    ("('O''BRIEN')", "'O''BRIEN'"),
])
def test_a_bound_value_is_written_the_way_postgresql_writes_it(written, expected):
    assert db2_partitioning.to_postgresql_value(written) == expected


@pytest.mark.parametrize('written', [
    ## a bound which has to be evaluated is a boundary which moves
    'CURRENT DATE',
    'YEAR(sales_date)',
    '',
])
def test_a_bound_which_is_not_a_literal_is_refused(written):
    with pytest.raises(UntranslatableScheme):
        db2_partitioning.to_postgresql_value(written)


# --------------------------------------------------------------------------------------
# the next value, which is what an INCLUSIVE end has to become


@pytest.mark.parametrize('value, type_name, expected', [
    ("'2024-12-31'", 'DATE', "'2025-01-01'"),
    ## the end of February of a leap year, which is where an arithmetic written by hand goes wrong
    ("'2024-02-29'", 'DATE', "'2024-03-01'"),
    ('199', 'INTEGER', '200'),
    ('(199)', 'SMALLINT', '200'),
    ('-1', 'BIGINT', '0'),
    ## a DECIMAL with no scale is a whole number, and Db2's default scale is 0
    ('99', 'DECIMAL(5,0)', '100'),
    ('99', 'DECIMAL', '100'),
])
def test_the_next_value_of_a_type_which_has_one(value, type_name, expected):
    assert db2_partitioning.successor(value, type_name) == expected


@pytest.mark.parametrize('type_name', [
    ## the next value of these depends on a precision the bound does not carry
    'DECIMAL(9,2)',
    'VARCHAR(10)',
    'TIMESTAMP',
    'DOUBLE',
    '',
])
def test_a_type_with_no_next_value_refuses_rather_than_moving_the_bound(type_name):
    with pytest.raises(UntranslatableScheme, match='next value|INCLUSIVE'):
        db2_partitioning.successor("'x'", type_name)


# --------------------------------------------------------------------------------------
# the bound of a whole partition - the trap of the whole family


def test_an_inclusive_end_becomes_the_exclusive_one_which_holds_the_same_rows():
    """
    `STARTING '2024-01-01' ENDING '2024-12-31'` is Db2's ordinary spelling and BOTH ends are
    inclusive. Copied across, the target refuses every row of 31 December - silently, one row at
    a time, at the end of a migration which has already moved the rest.
    """
    assert db2_partitioning.range_bound("'2024-01-01'", True, "'2024-12-31'", True, 'DATE') == (
        "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')")


def test_an_exclusive_end_is_already_what_postgresql_means():
    assert db2_partitioning.range_bound("'2024-01-01'", True, "'2025-01-01'", False, 'DATE') == (
        "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')")


def test_an_exclusive_start_is_the_value_after_it():
    """PostgreSQL's FROM is always inclusive, so a partition which starts after x starts at x+1."""
    assert db2_partitioning.range_bound('(199)', False, '(299)', False, 'INTEGER') == (
        'FOR VALUES FROM (200) TO (299)')


@pytest.mark.parametrize('low, high, expected', [
    ('MINVALUE', '(199)', 'FOR VALUES FROM (MINVALUE) TO (200)'),
    ('(199)', 'MAXVALUE', 'FOR VALUES FROM (199) TO (MAXVALUE)'),
    ## the catalogue may hold nothing at all where the DDL holds the word
    ('', '(199)', 'FOR VALUES FROM (MINVALUE) TO (200)'),
])
def test_an_open_end_stays_open(low, high, expected):
    assert db2_partitioning.range_bound(low, True, high, True, 'INTEGER') == expected


def test_an_inclusive_end_on_a_type_with_no_next_value_is_refused():
    with pytest.raises(UntranslatableScheme, match='INCLUSIVE'):
        db2_partitioning.range_bound("'a'", True, "'z'", True, 'VARCHAR(10)')


def test_a_hash_partition_becomes_one_of_a_modulus():
    assert db2_partitioning.hash_bound(0, 8) == 'FOR VALUES WITH (MODULUS 8, REMAINDER 0)'
    with pytest.raises(UntranslatableScheme):
        db2_partitioning.hash_bound(8, 8)


def test_the_key_is_written_in_the_names_the_target_will_have():
    assert db2_partitioning.key_definition(
        'RANGE', ['SALES_DATE'], lambda name: name.lower()) == 'RANGE ("sales_date")'


@pytest.mark.parametrize('method', ['SIZE', 'LIST', ''])
def test_a_method_postgresql_cannot_be_given_gets_no_key(method):
    with pytest.raises(UntranslatableScheme):
        db2_partitioning.key_definition(method, ['SALES_DATE'], str.lower)


# --------------------------------------------------------------------------------------
# the DDL, which for two of the three connectors IS the catalogue


@pytest.mark.parametrize('clause, method, columns', [
    ('PARTITION BY RANGE (SALES_DATE) (PARTITION q1 ENDING AT (1))', 'RANGE', ['SALES_DATE']),
    ## the older z/OS spelling, with no method written and a direction on the column
    ('PARTITION BY (ACCT_NUM ASC) (PARTITION 1 ENDING AT (199))', 'RANGE', ['ACCT_NUM']),
    ('PARTITION BY RANGE (A, B DESC) (PARTITION 1 ENDING AT (1, 2))', 'RANGE', ['A', 'B']),
    ('PARTITION BY HASH (CUSTOMER_ID) INTO 8 PARTITIONS', 'HASH', ['CUSTOMER_ID']),
    ## partition-by-growth, which has no key at all
    ('PARTITION BY SIZE EVERY 4G', 'SIZE', []),
    ('IN DATABASE MYDB', '', []),
])
def test_the_partition_by_clause_is_read_in_every_spelling(clause, method, columns):
    read = db2_partitioning.parse_partition_clause(clause)
    assert read['method'] == method
    assert read['columns'] == columns


def test_the_partition_list_is_taken_with_its_brackets_balanced():
    """
    The parser this replaces stopped at the first closing bracket, so a list of three partitions
    each ending AT (a number) came back as one - and the two behind it were never seen at all.
    """
    read = db2_partitioning.parse_partition_clause(
        'PARTITION BY RANGE (ACCT_NUM) '
        '(PARTITION 1 ENDING AT (199), PARTITION 2 ENDING AT (299), '
        'PARTITION 3 ENDING AT (MAXVALUE)) IN MYTS')
    assert read['ranges'].count('PARTITION') == 3
    assert len(db2_partitioning.parse_partition_list(read['ranges'])) == 3


def test_a_hash_clause_keeps_its_count():
    read = db2_partitioning.parse_partition_clause('PARTITION BY HASH (C) INTO 8 PARTITIONS')
    assert read['hash_count'] == 8
    assert read['ranges'] == 'INTO 8 PARTITIONS'


def test_a_partition_which_writes_only_its_end_starts_where_the_one_below_it_stopped():
    """
    `PARTITION n ENDING AT (x)` is z/OS's ordinary spelling and says nothing about where the
    partition begins. Db2's rule is that the ranges of a partitioned table space are contiguous
    and ordered by partition number, so the answer is in the entry above - not a guess.
    """
    written = db2_partitioning.parse_partition_list(
        'PARTITION 1 ENDING AT (199), PARTITION 2 ENDING AT (299)')
    assert written[0]['low'] == db2_partitioning.MINVALUE
    assert written[1]['low'] == '(199)'
    ## the one below ended INCLUSIVE at 199, so this one starts after it
    assert written[1]['low_inclusive'] is False


@pytest.mark.parametrize('entry, low, high, low_inclusive, high_inclusive', [
    ("STARTING '2024-01-01' ENDING '2024-12-31'", "'2024-01-01'", "'2024-12-31'", True, True),
    ("STARTING FROM ('a') INCLUSIVE ENDING AT ('b') EXCLUSIVE", "('a')", "('b')", True, False),
    ("STARTING FROM (1) EXCLUSIVE ENDING AT (9) INCLUSIVE", '(1)', '(9)', False, True),
])
def test_the_ends_are_read_in_every_spelling_db2_writes_them(
        entry, low, high, low_inclusive, high_inclusive):
    written = db2_partitioning.parse_partition_list(entry)[0]
    assert (written['low'], written['high']) == (low, high)
    assert written['low_inclusive'] is low_inclusive
    assert written['high_inclusive'] is high_inclusive


def test_a_named_partition_keeps_its_name_and_a_numbered_one_is_given_one():
    """A relation of PostgreSQL cannot be called `3`, which is what z/OS calls a partition."""
    written = db2_partitioning.parse_partition_list(
        "PARTITION q1 ENDING AT ('a'), PARTITION 2 ENDING AT ('b')")
    assert db2_partitioning.partition_name_for('SALES', 1, written[0]['name']) == 'q1'
    assert db2_partitioning.partition_name_for('SALES', 2, written[1]['name']) == 'SALES_p2'


def test_a_list_written_with_every_is_refused_rather_than_expanded():
    """
    `target_partitioning` with a `date_range` is the thing which generates a calendar of
    partitions, and it does it from the data. A second generator reading a Db2 interval would be
    a worse copy of it, and one which silently produced a different set of partitions.
    """
    with pytest.raises(UntranslatableScheme, match='EVERY'):
        db2_partitioning.parse_partition_list(
            "STARTING '2024-01-01' ENDING '2024-12-31' EVERY 1 MONTH")


def test_a_partition_with_no_end_is_refused():
    with pytest.raises(UntranslatableScheme, match='no ENDING'):
        db2_partitioning.parse_partition_list("PARTITION q1 STARTING '2024-01-01'")


# --------------------------------------------------------------------------------------
# the whole scheme, from a DDL extract


ZOS_TYPES = {'ACCT_NUM': 'INTEGER'}
ZOS_RANGES = ('PARTITION 1 ENDING AT (199), PARTITION 2 ENDING AT (299), '
              'PARTITION 3 ENDING AT (MAXVALUE)')


def test_a_zos_range_scheme_becomes_contiguous_postgresql_partitions():
    scheme = db2_partitioning.scheme_from_ddl(
        'ACCOUNTS', 'RANGE', ['ACCT_NUM'], ZOS_RANGES, ZOS_TYPES, lambda name: name.lower())
    assert scheme['is_partitioned'] is True
    assert scheme['target_key_definition'] == 'RANGE ("acct_num")'
    assert scheme['key_definition'] == 'RANGE (ACCT_NUM)'
    ## every end is INCLUSIVE, so each of them becomes the exclusive bound one past it - and the
    ## partitions meet exactly, which is what makes them the same scheme
    assert [partition['target_bound'] for partition in scheme['partitions']] == [
        'FOR VALUES FROM (MINVALUE) TO (200)',
        'FOR VALUES FROM (200) TO (300)',
        'FOR VALUES FROM (300) TO (MAXVALUE)',
    ]
    assert [partition['name'] for partition in scheme['partitions']] == [
        'ACCOUNTS_p1', 'ACCOUNTS_p2', 'ACCOUNTS_p3']
    assert scheme['blockers'] == []


def test_the_inclusive_end_is_reported_because_it_is_the_one_thing_which_was_converted():
    scheme = db2_partitioning.scheme_from_ddl(
        'ACCOUNTS', 'RANGE', ['ACCT_NUM'], ZOS_RANGES, ZOS_TYPES, lambda name: name.lower())
    note = [text for text in scheme['notes'] if 'INCLUSIVE' in text]
    assert note and 'exclusive' in note[0]


def test_a_scheme_ending_in_maxvalue_needs_nothing_said_about_where_it_stops():
    scheme = db2_partitioning.scheme_from_ddl(
        'ACCOUNTS', 'RANGE', ['ACCT_NUM'], ZOS_RANGES, ZOS_TYPES, lambda name: name.lower())
    assert not [text for text in scheme['notes'] if 'refused' in text]


def test_a_scheme_which_stops_says_where_it_stops():
    scheme = db2_partitioning.scheme_from_ddl(
        'SALES', 'RANGE', ['SALES_DATE'],
        "PARTITION q1 STARTING '2024-01-01' ENDING '2024-03-31'",
        {'SALES_DATE': 'DATE'}, lambda name: name.lower())
    assert scheme['partitions'][0]['target_bound'] == (
        "FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')")
    assert any('past that bound is refused' in text for text in scheme['notes'])


def test_a_bound_whose_type_has_no_next_value_stops_the_run_and_names_the_partition():
    scheme = db2_partitioning.scheme_from_ddl(
        'SALES', 'RANGE', ['REGION'], "PARTITION emea ENDING AT ('DE')",
        {'REGION': 'VARCHAR(2)'}, lambda name: name.lower())
    assert len(scheme['blockers']) == 1
    assert 'emea' in scheme['blockers'][0]


def test_a_range_over_more_than_one_column_is_refused_rather_than_half_converted():
    scheme = db2_partitioning.scheme_from_ddl(
        'SALES', 'RANGE', ['A', 'B'], 'PARTITION 1 ENDING AT (1, 2)',
        {'A': 'INTEGER', 'B': 'INTEGER'}, lambda name: name.lower())
    assert any('ONE column' in issue for issue in scheme['blockers'])


def test_a_hash_scheme_carries_its_count_and_says_the_rows_move():
    scheme = db2_partitioning.scheme_from_ddl(
        'EVENTS', 'HASH', ['CUSTOMER_ID'], 'INTO 4 PARTITIONS', {},
        lambda name: name.lower())
    assert [partition['target_bound'] for partition in scheme['partitions']] == [
        f'FOR VALUES WITH (MODULUS 4, REMAINDER {index})' for index in range(4)]
    assert any('sits in another here' in text for text in scheme['notes'])
    assert scheme['blockers'] == []


def test_partition_by_growth_has_no_key_and_stops_the_run():
    """
    A partition exists because the one before it filled up. PostgreSQL routes a row by its value
    and Db2 routes it by which partition still has room - there is nothing to write into a
    PARTITION BY clause.
    """
    scheme = db2_partitioning.scheme_from_ddl(
        'JOURNAL', 'SIZE', [], '', {}, lambda name: name.lower())
    assert scheme['target_key_definition'] == ''
    assert len(scheme['blockers']) == 1
    assert 'no partitioning key at all' in scheme['blockers'][0]
    assert any('filled up' in text for text in scheme['notes'])


def test_a_range_clause_with_no_partition_list_refuses_every_row_and_is_stopped():
    scheme = db2_partitioning.scheme_from_ddl(
        'SALES', 'RANGE', ['SALES_DATE'], '', {'SALES_DATE': 'DATE'}, lambda name: name.lower())
    assert any('refuses every row' in issue for issue in scheme['blockers'])


def test_the_plan_builds_a_preserved_db2_table_from_the_translated_bounds():
    scheme = db2_partitioning.scheme_from_ddl(
        'ACCOUNTS', 'RANGE', ['ACCT_NUM'], ZOS_RANGES, ZOS_TYPES, lambda name: name.lower())
    decision = partitioning.build_plan(
        {'ACCOUNTS': scheme}, ['ACCOUNTS'], mode_of=lambda name: 'preserve',
        target_version_num=160000)['ACCOUNTS']
    assert decision.action == partitioning.PRESERVE
    assert decision.target_key_definition == 'RANGE ("acct_num")'
    assert decision.issues == []
    assert [part.bound for part in decision.partitions] == [
        partition['target_bound'] for partition in scheme['partitions']]


def test_a_scheme_which_cannot_be_built_stops_only_a_run_which_would_build_it():
    scheme = db2_partitioning.scheme_from_ddl(
        'JOURNAL', 'SIZE', [], '', {}, lambda name: name.lower())
    preserved = partitioning.build_plan(
        {'JOURNAL': scheme}, ['JOURNAL'], mode_of=lambda name: 'preserve',
        target_version_num=160000)['JOURNAL']
    assert preserved.issues
    flattened = partitioning.build_plan(
        {'JOURNAL': scheme}, ['JOURNAL'], mode_of=lambda name: 'flatten',
        target_version_num=160000)['JOURNAL']
    assert flattened.issues == []
    ## and what the source really has is still said
    assert any('filled up' in warning for warning in flattened.warnings)


# --------------------------------------------------------------------------------------
# the two DDL connectors, which read it back out of the parsed extract


class ProtocolCursor:
    """A cursor over the ddl_* protocol tables, answering by what the statement names."""

    def __init__(self, answers, raise_on=None):
        self.answers = answers
        self.raise_on = raise_on or ()
        self.rows = []
        self.statements = []

    def execute(self, statement, binds=None):
        self.statements.append(statement)
        for marker in self.raise_on:
            if marker in statement:
                raise Exception(f'relation "{marker}" does not exist')
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


def ddl_connector(connector_class, answers, raise_on=None):
    made = connector_class.__new__(connector_class)
    made.config_parser = MagicMock()
    made.config_parser.convert_names_case = lambda name: (name or '').lower()
    made.messages = []
    made.config_parser.print_log_message.side_effect = \
        lambda level, message: made.messages.append((level, str(message)))
    made.protocol_schema = 'migration'
    made.migrator_tables = MagicMock()
    cursor = ProtocolCursor(answers, raise_on)
    made.migrator_tables.protocol_connection.connection.cursor.return_value = cursor
    made.cursor = cursor
    return made


DDL_ANSWERS = [
    ('source_partition_method, source_partition_columns',
     [('RANGE', 'ACCT_NUM', ZOS_RANGES)]),
    ('ddl_columns', [('ACCT_NUM', 'INTEGER'), ('BALANCE', 'DECIMAL(9,2)')]),
    ('coalesce(source_partition_method', [('ACCOUNTS',), ('JOURNAL',)]),
]


@pytest.mark.parametrize('connector_class', [IbmDb2ZosConnector, IbmDb2IConnector])
def test_a_ddl_connector_reads_the_scheme_back_out_of_the_parsed_extract(connector_class):
    made = ddl_connector(connector_class, DDL_ANSWERS)
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'PROD', 'source_table_name': 'ACCOUNTS'})
    assert scheme['method'] == 'RANGE'
    assert scheme['columns'] == ['ACCT_NUM']
    assert scheme['target_key_definition'] == 'RANGE ("acct_num")'
    assert scheme['partitions'][0]['target_bound'] == 'FOR VALUES FROM (MINVALUE) TO (200)'
    ## the bound the DDL wrote is kept beside it, for the report and the protocol
    assert 'INCLUSIVE' in scheme['partitions'][0]['bound']


@pytest.mark.parametrize('connector_class', [IbmDb2ZosConnector, IbmDb2IConnector])
def test_a_ddl_connector_lists_the_partitioned_tables_in_one_query(connector_class):
    made = ddl_connector(connector_class, DDL_ANSWERS)
    assert made.fetch_partitioning_candidates('PROD') == {'ACCOUNTS', 'JOURNAL'}
    assert len(made.cursor.statements) == 1


@pytest.mark.parametrize('connector_class', [IbmDb2ZosConnector, IbmDb2IConnector])
def test_a_table_the_ddl_does_not_partition_answers_nothing(connector_class):
    made = ddl_connector(connector_class, [
        ('source_partition_method, source_partition_columns', [(None, None, None)])])
    assert made.fetch_table_partitioning(
        {'source_schema_name': 'PROD', 'source_table_name': 'CUSTOMERS'}) == {}


@pytest.mark.parametrize('connector_class', [IbmDb2ZosConnector, IbmDb2IConnector])
def test_a_ddl_connector_which_cannot_read_the_extract_says_so(connector_class):
    made = ddl_connector(connector_class, DDL_ANSWERS, raise_on=('ddl_tables',))
    assert made.fetch_table_partitioning(
        {'source_schema_name': 'PROD', 'source_table_name': 'ACCOUNTS'}) == {}
    assert any(level == 'ERROR' for level, _text in made.messages)


@pytest.mark.parametrize('connector_class, statement', [
    (IbmDb2ZosConnector,
     "CREATE TABLE PROD.ACCOUNTS (ACCT_NUM INTEGER NOT NULL, BALANCE DECIMAL(9,2)) "
     "IN MYDB.MYTS PARTITION BY RANGE (ACCT_NUM) "
     "(PARTITION 1 ENDING AT (199), PARTITION 2 ENDING AT (299));"),
    (IbmDb2IConnector,
     "CREATE TABLE PROD.ACCOUNTS (ACCT_NUM INTEGER NOT NULL, BALANCE DECIMAL(9,2)) "
     "PARTITION BY RANGE (ACCT_NUM) "
     "(PARTITION 1 ENDING AT (199), PARTITION 2 ENDING AT (299));"),
])
def test_the_parser_of_a_ddl_connector_reads_the_whole_partition_list(connector_class, statement):
    """
    The regular expression this replaces did not know the word RANGE at all, so this clause was
    never matched - and where it did match, its bracket stopped at the first closing one and two
    partitions of three were lost.
    """
    read = db2_partitioning.parse_partition_clause(statement)
    assert read['method'] == 'RANGE' and read['columns'] == ['ACCT_NUM']
    assert len(db2_partitioning.parse_partition_list(read['ranges'])) == 2


@pytest.mark.parametrize('connector_class', [IbmDb2ZosConnector, IbmDb2IConnector])
def test_the_partitioning_is_no_longer_written_into_the_table_comment(connector_class):
    """
    §2.4: the scheme of a Db2 source reached the target as free text in a COMMENT ON, and the
    comment the DDL really carried reached nothing at all. It is read as a scheme now, and the
    comment column holds the comment.
    """
    import inspect
    source = inspect.getsource(connector_class.fetch_all_tables)
    assert 'Partition:' not in source
    assert 'source_table_comment' in source


# --------------------------------------------------------------------------------------
# Db2 for LUW, which has a live catalogue and three mechanisms in it


class Catalogue:
    """A SYSCAT cursor, answering by what the statement names."""

    def __init__(self, answers, raise_on=None):
        self.answers = answers
        self.raise_on = raise_on or ()
        self.rows = []
        self.statements = []

    def execute(self, statement, binds=None):
        self.statements.append(statement)
        for marker in self.raise_on:
            if marker in statement:
                raise Exception(f'SQL0204N {marker} is an undefined name')
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


def luw_connector(answers, raise_on=None, system_catalog='SYSCAT'):
    made = IbmDb2LuwConnector.__new__(IbmDb2LuwConnector)
    made.config_parser = MagicMock()
    made.config_parser.convert_names_case = lambda name: (name or '').lower()
    made.config_parser.get_system_catalog.return_value = system_catalog
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


## SALES: partitioned by range over SALES_DATE, three quarters, both ends INCLUSIVE - which is
## what Db2 writes when the DDL says STARTING ... ENDING ... and nothing else.
SALES = [
    ('SYSCAT.DATAPARTITIONS', [
        ('PART0', 0, "'2024-01-01'", 'Y', "'2024-03-31'", 'Y', 3, ''),
        ('PART1', 1, "'2024-04-01'", 'Y', "'2024-06-30'", 'Y', 3, ''),
        ('PART2', 2, "'2024-07-01'", 'Y', None, 'Y', 4, ''),
    ]),
    ('SYSCAT.DATAPARTITIONEXPRESSION', [('SALES_DATE',)]),
    ('PARTITION_MODE FROM SYSCAT.TABLES', [('',)]),
    ('SYSCAT.INDEXES', []),
    ('SYSCAT.COLUMNS', [('SALES_DATE', 'DATE', 4, 0), ('AMOUNT', 'DECIMAL', 9, 2)]),
]


def test_the_luw_connector_reads_a_range_scheme_out_of_syscat():
    made = luw_connector(SALES)
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'PROD', 'source_table_name': 'SALES'})
    assert scheme['is_partitioned'] is True and scheme['is_partition'] is False
    assert scheme['method'] == 'RANGE' and scheme['columns'] == ['SALES_DATE']
    assert scheme['target_key_definition'] == 'RANGE ("sales_date")'
    assert scheme['partition_count'] == 3


def test_the_inclusive_ends_of_a_luw_scheme_become_exclusive_ones():
    """
    Copied bound for bound, the target would refuse every row of 31 March and 30 June - one row
    at a time, at the end of a migration which had already moved the rest.
    """
    made = luw_connector(SALES)
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'PROD', 'source_table_name': 'SALES'})
    assert [partition['target_bound'] for partition in scheme['partitions']] == [
        "FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')",
        "FOR VALUES FROM ('2024-04-01') TO ('2024-07-01')",
        "FOR VALUES FROM ('2024-07-01') TO (MAXVALUE)",
    ]
    assert any('INCLUSIVE' in text and 'exclusive' in text for text in scheme['notes'])
    ## and the bound Db2 wrote is kept beside it
    assert 'ENDING AT' in scheme['partitions'][0]['bound']


def test_dpf_is_not_table_partitioning_and_says_so():
    """
    §4.2: the rows are spread over the physical nodes of the instance by a hash. It is a
    different mechanism which happens to use the same word, and PostgreSQL has nothing for it.
    """
    answers = [entry for entry in SALES if 'PARTITION_MODE' not in entry[0]]
    ## the distribution key is read out of SYSCAT.COLUMNS as well, so its marker goes first
    answers.insert(0, ('PARTKEYSEQ', [('CUSTOMER_ID',)]))
    answers.append(('PARTITION_MODE FROM SYSCAT.TABLES', [('H',)]))
    made = luw_connector(answers)
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'PROD', 'source_table_name': 'SALES'})
    note = [text for text in scheme['notes'] if 'DATABASE partitioned' in text]
    assert note and 'CUSTOMER_ID' in note[0]
    assert 'no counterpart for it at all' in note[0]
    assert scheme['engine_specific']['distribution'] == {'mode': 'H', 'columns': ['CUSTOMER_ID']}


def test_mdc_is_a_storage_layout_and_says_so():
    answers = [entry for entry in SALES if entry[0] != 'SYSCAT.INDEXES']
    answers.insert(0, ('INDEXTYPE = \'DIM\'', [('SALES_DIM_R', 'REGION'), ('SALES_DIM_D', 'DEPT')]))
    answers.append(('SYSCAT.INDEXES', []))
    made = luw_connector(answers)
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'PROD', 'source_table_name': 'SALES'})
    note = [text for text in scheme['notes'] if 'multi-dimensionally clustered' in text]
    assert note and 'BRIN' in note[0]


def test_a_table_which_is_only_dpf_is_not_reported_as_partitioned():
    """
    DPF and MDC are not table partitioning, so a table which has one of them and no data
    partition is not a partitioned table - and answering that it is would build a partitioned
    target for a scheme which is not one. What there is to say about it is still said.
    """
    made = luw_connector([
        ('SYSCAT.DATAPARTITIONS', []),
        ('SYSCAT.DATAPARTITIONEXPRESSION', []),
        ('PARTITION_MODE FROM SYSCAT.TABLES', [('H',)]),
        ('PARTKEYSEQ', [('CUSTOMER_ID',)]),
        ('SYSCAT.INDEXES', []),
        ('SYSCAT.COLUMNS', [('CUSTOMER_ID', 'INTEGER', 4, 0)]),
    ])
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'PROD', 'source_table_name': 'ORDERS'})
    assert scheme['is_partitioned'] is False
    assert scheme['partitions'] == []
    assert any('DPF' in text for text in scheme['notes'])


def test_an_ordinary_table_answers_nothing_at_all():
    made = luw_connector([
        ('SYSCAT.DATAPARTITIONS', []),
        ('SYSCAT.DATAPARTITIONEXPRESSION', []),
        ('PARTITION_MODE FROM SYSCAT.TABLES', [('',)]),
        ('SYSCAT.INDEXES', []),
        ('SYSCAT.COLUMNS', [('ID', 'INTEGER', 4, 0)]),
    ])
    assert made.fetch_table_partitioning(
        {'source_schema_name': 'PROD', 'source_table_name': 'CUSTOMERS'}) == {}


def test_a_detached_data_partition_is_reported():
    answers = [entry for entry in SALES if entry[0] != 'SYSCAT.DATAPARTITIONS']
    answers.insert(0, ('SYSCAT.DATAPARTITIONS', [
        ('PART0', 0, "'2024-01-01'", 'Y', "'2024-03-31'", 'Y', 3, 'D'),
    ]))
    made = luw_connector(answers)
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'PROD', 'source_table_name': 'SALES'})
    assert any('not in the attached state' in text for text in scheme['notes'])


def test_a_migration_reading_through_sysibm_says_the_scheme_was_not_read():
    """
    P2-8: the standard views describe none of the three mechanisms, and a table read through
    them is not a table which is not partitioned.
    """
    made = luw_connector(SALES, system_catalog='SYSIBM')
    assert made.fetch_table_partitioning(
        {'source_schema_name': 'PROD', 'source_table_name': 'SALES'}) == {}
    assert made.fetch_partitioning_candidates('PROD') == set()
    assert 'SYSCAT' in made.object_kind_not_read('table_partitioning')
    assert made.fetch_partitioning_facts(
        {'source_schema_name': 'PROD', 'source_table_name': 'SALES'}) is None


def test_a_catalogue_read_through_syscat_does_not_say_it_was_not_read():
    made = luw_connector(SALES)
    assert made.object_kind_not_read('table_partitioning') is None


# --------------------------------------------------------------------------------------
# the facts of §4.4, out of SYSCAT


FACTS = [
    ('CARD FROM SYSCAT.TABLES', [(400000,)]),
    ('NUMNULLS', [
        ## name, type, length, scale, nulls, generated, numnulls
        ('SALES_ID', 'INTEGER', 4, 0, 'N', '', 0),
        ('SALES_DATE', 'DATE', 4, 0, 'N', '', 0),
        ('SHIPPED_AT', 'DATE', 4, 0, 'Y', '', 41203),
        ('TOTAL', 'DECIMAL', 9, 2, 'Y', 'A', -1),
        ('DOCUMENT', 'XML', 0, 0, 'Y', '', -1),
        ('NOTE', 'VARCHAR', 100, 0, 'Y', '', -1),
    ]),
    ("UNIQUERULE IN ('P', 'U')", [
        ('SALES_PK', 'P', 'SALES_ID'),
        ('SALES_REF_UQ', 'U', 'SALES_REF'),
    ]),
    ('SYSCAT.REFERENCES', [('SALES_ITEM_FK', 'SALES_ITEMS')]),
]


def facts_of(answers=None, raise_on=None):
    made = luw_connector(answers or FACTS, raise_on)
    return made.fetch_partitioning_facts(
        {'source_schema_name': 'PROD', 'source_table_name': 'SALES'})


def test_the_facts_of_a_table_come_out_of_syscat_and_its_statistics():
    facts = facts_of()
    assert facts['row_estimate'] == 400000
    assert facts['columns']['SALES_DATE']['not_null'] is True
    assert facts['columns']['SHIPPED_AT']['null_fraction'] == pytest.approx(0.1030075)
    assert facts['columns']['TOTAL']['type_name'] == 'DECIMAL(9,2)'
    assert facts['date_range_types'] == db2_partitioning.DATE_RANGE_TYPES


def test_a_column_runstats_has_not_seen_has_a_null_fraction_which_is_not_known():
    """NUMNULLS is -1 where RUNSTATS has not run, and -1 is not zero."""
    assert facts_of()['columns']['NOTE']['null_fraction'] is None


def test_a_generated_column_is_recognised():
    assert facts_of()['columns']['TOTAL']['is_generated'] is True
    assert facts_of()['columns']['SALES_ID']['is_generated'] is False


def test_a_type_which_cannot_carry_a_partition_key_says_so():
    facts = facts_of()
    assert facts['columns']['DOCUMENT']['has_btree_opclass'] is False
    assert facts['columns']['NOTE']['has_btree_opclass'] is True


def test_every_unique_key_is_read_and_not_only_the_primary_one():
    keys = {key['name']: key for key in facts_of()['unique_keys']}
    assert keys['SALES_PK']['is_primary'] is True
    assert keys['SALES_REF_UQ']['is_primary'] is False


def test_what_references_the_table_is_read():
    assert facts_of()['referenced_by'] == [{'name': 'SALES_ITEM_FK', 'table': 'SALES_ITEMS'}]


def test_facts_which_cannot_be_read_are_answered_as_not_read():
    assert facts_of(raise_on=('NUMNULLS',)) is None


def test_a_preserved_db2_key_is_checked_against_the_primary_key():
    """
    §3.1. Db2 keeps a primary key which does not contain the partitioning column in a
    non-partitioned index, exactly as Oracle does with a global one, and PostgreSQL refuses it.
    """
    made = luw_connector(SALES)
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'PROD', 'source_table_name': 'SALES'})
    decision = partitioning.build_plan(
        {'SALES': scheme}, ['SALES'], mode_of=lambda name: 'preserve',
        target_version_num=160000)['SALES']
    partitioning.check_preserved_keys(
        decision, [{'name': 'SALES_PK', 'columns': ['SALES_ID'], 'is_primary': True}])
    assert any('SALES_PK' in issue and 'SALES_DATE' in issue for issue in decision.issues)


# --------------------------------------------------------------------------------------
# a source with no instance behind it


@pytest.mark.parametrize('connector_class', [IbmDb2ZosConnector, IbmDb2IConnector])
def test_a_ddl_source_says_it_cannot_be_asked_what_a_column_holds(connector_class):
    """
    There is no source instance: the structure comes out of `.sql` extracts and the rows out of
    CSV files. The base implementation would send a SELECT down a connection which does not
    exist, and the run would end in an AttributeError in the middle of preparing a table.
    """
    assert connector_class.CAN_PROBE_COLUMN_VALUES is False
    made = ddl_connector(connector_class, DDL_ANSWERS)
    with pytest.raises(ValueError, match='no source database to ask'):
        made.probe_column_bounds({'source_schema_name': 'PROD', 'source_table_name': 'SALES',
                                  'column_name': 'SALES_DATE'})


def test_a_date_range_against_a_source_with_no_instance_is_refused_before_anything_is_created():
    """
    A date_range works its calendar out from the values the column really holds. Against a
    source which cannot be asked there is nothing to work it out from, and what would be built
    is a partitioned table with nothing under it - which refuses every row of the migration.
    """
    verdict = partitioning.check_repartitioning(
        {'table_name': 'SALES', 'partition_by': 'RANGE',
         'partitioning_columns': 'SALES_DATE', 'date_range': 'month'},
        ['SALES_DATE'], [], target_version_num=160000,
        bounds_were_read=False, bounds_can_be_read=False)
    assert not verdict.can_be_built
    assert any('no database to ask' in issue for issue in verdict.issues)


def test_a_probe_which_merely_failed_is_still_only_a_warning():
    """
    A source which CAN be asked and answered with an error is a different finding: the check was
    not made, and P2-8 says a check which was not made must not read like one which failed.
    """
    verdict = partitioning.check_repartitioning(
        {'table_name': 'SALES', 'partition_by': 'RANGE',
         'partitioning_columns': 'SALES_DATE', 'date_range': 'month'},
        ['SALES_DATE'], [], target_version_num=160000,
        bounds_were_read=False, bounds_can_be_read=True)
    assert verdict.can_be_built
    assert any('NOT worked out' in warning for warning in verdict.warnings)


# --------------------------------------------------------------------------------------
# the statements a run really produces for a Db2 table


PARTITION_SQL = ('CREATE TABLE "{target_schema_name}"."{target_table_name}" PARTITION OF '
                 '"{target_schema_name}"."{parent_table_name}" {partition_bound}')


def test_the_create_table_of_a_preserved_db2_table_and_its_partitions():
    """
    The whole way through: what the planner appends to the CREATE TABLE of the target and what
    it hands the orchestrator to run behind it. Every end of the source is INCLUSIVE, and every
    bound here is exclusive above and inclusive below - the same rows, said the other way.
    """
    from credativ_pg_migrator.planner import Planner

    scheme = db2_partitioning.scheme_from_ddl(
        'ACCOUNTS', 'RANGE', ['ACCT_NUM'], ZOS_RANGES, ZOS_TYPES, lambda name: name.lower())
    plan = partitioning.build_plan(
        {'ACCOUNTS': scheme}, ['ACCOUNTS'], mode_of=lambda name: 'preserve',
        target_version_num=160000)

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

    clause, statements = planner.partitioning_clause_for(plan['ACCOUNTS'], 'accounts')
    assert clause == ' PARTITION BY RANGE ("acct_num")'
    assert statements == [
        'CREATE TABLE "migtest"."accounts_p1" PARTITION OF "migtest"."accounts" '
        'FOR VALUES FROM (MINVALUE) TO (200)',
        'CREATE TABLE "migtest"."accounts_p2" PARTITION OF "migtest"."accounts" '
        'FOR VALUES FROM (200) TO (300)',
        'CREATE TABLE "migtest"."accounts_p3" PARTITION OF "migtest"."accounts" '
        'FOR VALUES FROM (300) TO (MAXVALUE)',
    ]
