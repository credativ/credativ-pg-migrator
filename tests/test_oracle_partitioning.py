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
Oracle partitioning: what the catalogue says, and what PostgreSQL is given for it.

§2.4 of development/PARTITIONING_STRATEGY.md puts oracle second of the twelve sources, behind
postgresql, and this is the file which holds it to what §4.2 and §5.1 ask of it: the scheme is
read, the bounds are translated rather than copied, the mechanisms with no counterpart are
refused rather than half-built, and every one of the things which are NOT carried over -
sub-partitions, INTERVAL's automatic extension, tablespace placement, the placement of a row in
a hash partition - is said out loud.

Nothing here needs a database or an Oracle client: the connector is built with `__new__`, its
cursor is a mock answering the catalogue rows of a real schema, and `oracle_partitioning.py` -
where the translation lives - imports no driver at all.
"""

import os
import sys
import types
import pytest
from unittest.mock import MagicMock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

if 'oracledb' not in sys.modules:
    ## the connector imports python-oracledb at module level and the driver is not a dependency
    ## of this migrator - it is installed by whoever migrates an Oracle database. Nothing here
    ## reaches it, so an empty module is enough to import the file.
    sys.modules['oracledb'] = types.ModuleType('oracledb')

import datetime

from credativ_pg_migrator import partitioning
from credativ_pg_migrator.connectors import oracle_partitioning
from credativ_pg_migrator.connectors.oracle_partitioning import UntranslatableScheme
from credativ_pg_migrator.connectors.oracle_connector import OracleConnector


def a_date(text):
    """A DATE bound, exactly as ALL_TAB_PARTITIONS.HIGH_VALUE writes one."""
    return (f"TO_DATE(' {text} 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', "
            f"'NLS_CALENDAR=GREGORIAN')")


# --------------------------------------------------------------------------------------
# one value of a bound


@pytest.mark.parametrize('written, expected', [
    (a_date('2024-01-01'), "'2024-01-01 00:00:00'"),
    ## the ANSI literal, which some releases write instead
    ("TIMESTAMP' 2024-01-01 12:30:00'", "'2024-01-01 12:30:00'"),
    ("DATE' 2024-01-01'", "'2024-01-01'"),
    ("TO_TIMESTAMP(' 2024-01-01 00:00:00.000000', 'SYYYY-MM-DD HH24:MI:SS.FF')",
     "'2024-01-01 00:00:00.000000'"),
    ('MAXVALUE', 'MAXVALUE'),
    ('maxvalue', 'MAXVALUE'),
    ('1000', '1000'),
    ('-1.5', '-1.5'),
    ("'DE'", "'DE'"),
    ## Oracle escapes a quote inside a literal by doubling it, and so does PostgreSQL
    ("'O''BRIEN'", "'O''BRIEN'"),
    ## the national character set literal - the N is Oracle's and the literal is not
    ("N'DE'", "'DE'"),
    ('NULL', 'NULL'),
    ## a RAW bound: the migration gives the column a bytea, which reads hex written this way
    ("HEXTORAW('DEADBEEF')", "'\\xDEADBEEF'"),
])
def test_a_bound_value_is_written_the_way_postgresql_writes_it(written, expected):
    assert oracle_partitioning.to_postgresql_value(written) == expected


@pytest.mark.parametrize('written', [
    ## an interval bound - there is no reliable PostgreSQL spelling of Oracle's
    "TO_YMINTERVAL('01-00')",
    ## a function this module does not know: guessing it wrong is a partition which quietly
    ## takes rows belonging to the one beside it, and nothing later in the run would notice
    "SYS_EXTRACT_UTC(CURRENT_TIMESTAMP)",
    'ORDER_DATE + 1',
    '',
])
def test_a_bound_which_cannot_be_written_is_refused_rather_than_guessed(written):
    with pytest.raises(UntranslatableScheme):
        oracle_partitioning.to_postgresql_value(written)


def test_the_blank_oracle_pads_a_date_with_is_not_part_of_the_value():
    """
    `TO_DATE(' 2024-01-01 ...', 'SYYYY-...')` - the leading blank is the sign position of the
    SYYYY format model. Carried into the literal it is still read by PostgreSQL, and it is not
    what the value is.
    """
    assert oracle_partitioning.to_postgresql_value(a_date('2024-01-01')).startswith("'2024")


def test_a_high_value_of_more_than_one_column_is_not_split_on_the_comma_inside_a_call():
    """
    `10, TO_DATE(' 2024-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')` is two values and holds two
    commas which are not separators. Reading it as four answers bounds which do not exist.
    """
    items = oracle_partitioning.high_value_items(f"10, {a_date('2024-01-01')}")
    assert len(items) == 2
    assert items[0] == '10'


# --------------------------------------------------------------------------------------
# the bound of a whole partition


def test_the_first_range_partition_opens_at_minvalue():
    assert oracle_partitioning.range_bound(None, a_date('2024-01-01'), 1) == (
        "FOR VALUES FROM (MINVALUE) TO ('2024-01-01 00:00:00')")


def test_a_range_partition_starts_where_the_one_below_it_ended():
    """
    Oracle writes only the upper bound and means everything below it and at or above the bound
    of the partition before. PostgreSQL says the same thing with both ends, so the two are one
    scheme written twice - there is no gap and no overlap to work out.
    """
    assert oracle_partitioning.range_bound(a_date('2024-01-01'), a_date('2025-01-01'), 1) == (
        "FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2025-01-01 00:00:00')")


def test_the_last_range_partition_keeps_its_maxvalue():
    assert oracle_partitioning.range_bound(a_date('2025-01-01'), 'MAXVALUE', 1).endswith(
        'TO (MAXVALUE)')


def test_a_composite_range_key_opens_with_one_minvalue_per_column():
    bound = oracle_partitioning.range_bound(None, f"10, {a_date('2024-01-01')}", 2)
    assert bound == "FOR VALUES FROM (MINVALUE, MINVALUE) TO (10, '2024-01-01 00:00:00')"


def test_a_value_written_after_maxvalue_is_refused():
    """
    Oracle takes `VALUES LESS THAN (MAXVALUE, 10)` and means the 10 to be read. PostgreSQL takes
    an unbounded column only at the end of a key, because nothing after an infinity has a
    meaning - there is no bound which says what Oracle said.
    """
    with pytest.raises(UntranslatableScheme, match='after MAXVALUE'):
        oracle_partitioning.range_bound(None, 'MAXVALUE, 10', 2)


def test_a_list_partition_keeps_its_values():
    assert oracle_partitioning.list_bound("'DE', 'FR', 'UK'") == (
        "FOR VALUES IN ('DE', 'FR', 'UK')")


def test_the_list_partition_which_takes_everything_else_is_the_default_one():
    assert oracle_partitioning.list_bound('DEFAULT') == 'DEFAULT'


def test_a_hash_partition_becomes_one_of_a_modulus():
    assert oracle_partitioning.hash_bound(0, 4) == 'FOR VALUES WITH (MODULUS 4, REMAINDER 0)'
    assert oracle_partitioning.hash_bound(3, 4) == 'FOR VALUES WITH (MODULUS 4, REMAINDER 3)'


def test_a_hash_partition_outside_its_own_modulus_is_refused():
    with pytest.raises(UntranslatableScheme):
        oracle_partitioning.hash_bound(4, 4)


@pytest.mark.parametrize('bound, carries_one', [
    ("'2024-01-01 00:00:00'", False),
    ("'2024-01-01'", False),
    ("'2024-01-01 06:00:00'", True),
    ('MAXVALUE', False),
])
def test_a_bound_which_carries_a_time_of_day_is_recognised(bound, carries_one):
    """
    An Oracle DATE holds a time and the PostgreSQL `date` the migration gives it does not, so a
    boundary written at 06:00 becomes midnight and the rows of those six hours change partition.
    """
    assert oracle_partitioning.has_time_of_day(bound) is carries_one


def test_the_key_is_written_in_the_names_the_target_will_have():
    """
    Oracle holds ORDER_DATE and names_case_handling: lower gives the target order_date. An
    unquoted copy of the Oracle name in the PARTITION BY clause names a column which is not
    there.
    """
    assert oracle_partitioning.key_definition(
        'RANGE', ['ORDER_DATE'], lambda name: name.lower()) == 'RANGE ("order_date")'


@pytest.mark.parametrize('method', ['REFERENCE', 'SYSTEM', ''])
def test_a_method_postgresql_does_not_have_gets_no_key(method):
    with pytest.raises(UntranslatableScheme):
        oracle_partitioning.key_definition(method, ['ORDER_DATE'], str.lower)


# --------------------------------------------------------------------------------------
# what the connector reads out of the catalogue


class Catalogue:
    """
    A cursor answering the rows of a real Oracle schema, chosen by what the statement names.

    The markers are matched in order, so a more specific one is written first.
    """

    def __init__(self, answers, raise_on=None):
        self.answers = answers
        self.raise_on = raise_on or ()
        self.rows = []
        self.statements = []

    def execute(self, statement, binds=None):
        self.statements.append(statement)
        for marker in self.raise_on:
            if marker in statement:
                raise Exception(f'ORA-00904: {marker}: invalid identifier')
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


def connector_reading(answers, raise_on=None, names_case='lower'):
    made = OracleConnector.__new__(OracleConnector)
    made.config_parser = MagicMock()
    made.config_parser.convert_names_case = (
        (lambda name: (name or '').lower()) if names_case == 'lower' else (lambda name: name))
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


## ORDERS: RANGE (ORDER_DATE) over three partitions, sub-partitioned by HASH (CUSTOMER_ID) into
## 16 - 48 segments, which is the shape §2.2 of the design is written about. One global index.
ORDERS = [
    ('autolist', [('RANGE', 'HASH', 3, 16, 'USERS', None, None, None)]),
    ('all_subpart_key_columns', [('CUSTOMER_ID',)]),
    ('all_part_key_columns', [('ORDER_DATE',)]),
    ('all_tab_partitions', [
        ('ORDERS_2023', a_date('2024-01-01'), 1, 1200, 'USERS', 16),
        ('ORDERS_2024', a_date('2025-01-01'), 2, 900, 'TS_ARCHIVE', 16),
        ('ORDERS_MAX', 'MAXVALUE', 3, None, 'USERS', 16),
    ]),
    ('all_indexes', [('ORDERS_CUST_IX', 'NONUNIQUE')]),
]


def scheme_of(answers, raise_on=None, names_case='lower', table='ORDERS'):
    made = connector_reading(answers, raise_on, names_case)
    return made, made.fetch_table_partitioning(
        {'source_schema_name': 'SCOTT', 'source_table_name': table})


def test_the_connector_reads_a_range_scheme():
    _made, scheme = scheme_of(ORDERS)
    assert scheme['is_partitioned'] is True
    ## a partition of an Oracle table is not a table of the schema, so nothing this connector
    ## is asked about is ever one
    assert scheme['is_partition'] is False
    assert scheme['method'] == 'RANGE'
    assert scheme['columns'] == ['ORDER_DATE']
    assert scheme['key_definition'] == 'RANGE (ORDER_DATE)'
    assert scheme['target_key_definition'] == 'RANGE ("order_date")'
    assert scheme['partition_count'] == 3


def test_every_partition_carries_both_spellings_of_its_bound():
    """
    The bound of the source is what the report and the protocol show; the bound of the target is
    what the CREATE TABLE is given. A run which showed only one of them would either print
    PostgreSQL where the user has Oracle, or build Oracle where PostgreSQL has to be built.
    """
    _made, scheme = scheme_of(ORDERS)
    first = scheme['partitions'][0]
    assert first['bound'].startswith('TO_DATE(')
    assert first['target_bound'] == "FOR VALUES FROM (MINVALUE) TO ('2024-01-01 00:00:00')"
    assert scheme['partitions'][1]['target_bound'] == (
        "FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2025-01-01 00:00:00')")
    assert scheme['partitions'][2]['target_bound'].endswith('TO (MAXVALUE)')


def test_a_partition_row_count_which_nobody_gathered_is_unknown_and_not_zero():
    _made, scheme = scheme_of(ORDERS)
    assert scheme['partitions'][0]['rows'] == 1200
    assert scheme['partitions'][2]['rows'] is None


def test_no_partition_is_walked_into_because_the_second_level_is_not_carried_over():
    """
    §2.2. The planner asks about a partition which says it is itself partitioned, and an Oracle
    sub-partition is not a table it could ask about - nor one this migrator builds.
    """
    _made, scheme = scheme_of(ORDERS)
    assert all(partition['is_partitioned'] is False for partition in scheme['partitions'])


def test_the_sub_partitioning_is_reported_with_the_number_of_segments_left_behind():
    _made, scheme = scheme_of(ORDERS)
    assert scheme['engine_specific']['subpartitioning'] == {
        'method': 'HASH', 'columns': ['CUSTOMER_ID'], 'default_count': 16, 'segments': 48}
    note = [text for text in scheme['notes'] if 'sub-partitioned' in text]
    assert note and '48 segments' in note[0]


def test_the_tablespaces_of_the_partitions_are_named_and_reported_as_not_carried_over():
    _made, scheme = scheme_of(ORDERS)
    assert scheme['engine_specific']['tablespaces'] == ['TS_ARCHIVE', 'USERS']
    assert any('default tablespace' in text for text in scheme['notes'])


def test_an_ordinary_global_index_is_counted_rather_than_named():
    """
    PostgreSQL creates each of them as a partitioned index over the same columns and finds the
    same rows. Naming twenty of them one by one buries the line which matters, which is the
    unique one below.
    """
    _made, scheme = scheme_of(ORDERS)
    assert scheme['engine_specific']['global_indexes'] == [
        {'name': 'ORDERS_CUST_IX', 'is_unique': False}]
    assert any('1 GLOBAL index(es) which are not unique' in text for text in scheme['notes'])


def test_global_indexes_which_could_not_be_listed_are_not_reported_as_absent():
    """P2-8: "not read" and "there are none" must not look alike."""
    _made, scheme = scheme_of(ORDERS, raise_on=('partitioned = ',))
    assert 'global_indexes' not in scheme['engine_specific']
    assert any('NOT read whether any of them is a GLOBAL index' in text
               for text in scheme['notes'])


def test_a_global_unique_index_is_the_one_which_cannot_be_reproduced_at_all():
    answers = [entry for entry in ORDERS if entry[0] != 'all_indexes']
    answers.append(('all_indexes', [('ORDERS_NO_UQ', 'UNIQUE')]))
    _made, scheme = scheme_of(answers)
    note = [text for text in scheme['notes'] if 'ORDERS_NO_UQ' in text]
    assert note and 'no global index' in note[0]


def test_a_table_which_is_not_partitioned_answers_nothing():
    _made, scheme = scheme_of([('autolist', [])], table='CUSTOMERS')
    assert scheme == {}


# --------------------------------------------------------------------------------------
# the mechanisms which are Oracle's own


INTERVAL_ORDERS = [
    ('autolist', [('RANGE', 'NONE', 2, 0, 'USERS',
                   "NUMTOYMINTERVAL(1,'MONTH')", None, None)]),
    ('all_part_key_columns', [('ORDER_DATE',)]),
    ('all_tab_partitions', [
        ('P0', a_date('2024-01-01'), 1, 10, 'USERS', 0),
        ('SYS_P41', a_date('2024-02-01'), 2, 10, 'USERS', 0),
    ]),
    ('all_indexes', []),
]


def test_an_interval_scheme_keeps_the_partitions_it_has_and_says_what_stops():
    """
    §4.2 calls this the single most important line of the whole report for an Oracle user: what
    is automatic today stops being automatic. The partitions which exist are ordinary range
    partitions and are carried over; the extending is what PostgreSQL will not do.
    """
    _made, scheme = scheme_of(INTERVAL_ORDERS)
    assert scheme['method'] == 'RANGE'
    assert scheme['partition_count'] == 2
    assert scheme['engine_specific']['interval'] == "NUMTOYMINTERVAL(1,'MONTH')"
    note = [text for text in scheme['notes'] if 'INTERVAL' in text]
    assert note and 'BY ITSELF' in note[0]
    assert 'pg_partman' in note[0]
    ## it is a note and not a blocker: the scheme which exists CAN be built
    assert scheme['blockers'] == []


@pytest.mark.parametrize('method, expected', [
    ('REFERENCE', 'foreign key'),
    ('SYSTEM', 'no partitioning key at all'),
])
def test_a_method_with_no_key_to_migrate_stops_the_run(method, expected):
    scheme_rows = [
        ('autolist', [(method, 'NONE', 2, 0, 'USERS', None,
                       'ORDER_ITEMS_FK' if method == 'REFERENCE' else None, None)]),
        ('all_part_key_columns', []),
        ('all_tab_partitions', [('P1', None, 1, 10, 'USERS', 0),
                                ('P2', None, 2, 10, 'USERS', 0)]),
        ('all_indexes', []),
    ]
    _made, scheme = scheme_of(scheme_rows, table='ORDER_ITEMS')
    assert scheme['target_key_definition'] == ''
    assert len(scheme['blockers']) == 1
    assert expected in scheme['blockers'][0]
    assert 'source_partitioning: flatten' in scheme['blockers'][0]


def test_an_automatic_list_says_that_a_new_value_will_be_refused():
    scheme_rows = [
        ('autolist', [('LIST', 'NONE', 2, 0, 'USERS', None, None, 'YES')]),
        ('all_part_key_columns', [('REGION',)]),
        ('all_tab_partitions', [("P_EMEA", "'DE', 'FR'", 1, 10, 'USERS', 0),
                                ("P_AMER", "'US'", 2, 10, 'USERS', 0)]),
        ('all_indexes', []),
    ]
    _made, scheme = scheme_of(scheme_rows, table='CUSTOMERS')
    assert scheme['partitions'][0]['target_bound'] == "FOR VALUES IN ('DE', 'FR')"
    assert any('AUTOMATIC LIST' in text for text in scheme['notes'])


def test_a_hash_scheme_says_that_the_rows_do_not_land_where_they_landed_on_oracle():
    """
    Oracle hashes with its own function and PostgreSQL with its own. The count is carried over
    and the placement is not - which loses nothing, because the rows go in through the parent
    and the target routes each of them, and which a reader would otherwise assume the other way.
    """
    scheme_rows = [
        ('autolist', [('HASH', 'NONE', 4, 0, 'USERS', None, None, None)]),
        ('all_part_key_columns', [('CUSTOMER_ID',)]),
        ('all_tab_partitions', [(f'SYS_P{index}', None, index + 1, 100, 'USERS', 0)
                                for index in range(4)]),
        ('all_indexes', []),
    ]
    _made, scheme = scheme_of(scheme_rows, table='EVENTS')
    assert [partition['target_bound'] for partition in scheme['partitions']] == [
        f'FOR VALUES WITH (MODULUS 4, REMAINDER {index})' for index in range(4)]
    assert any('sits in another here' in text for text in scheme['notes'])


def test_a_bound_which_cannot_be_translated_stops_the_run_and_names_the_partition():
    scheme_rows = [
        ('autolist', [('RANGE', 'NONE', 1, 0, 'USERS', None, None, None)]),
        ('all_part_key_columns', [('SPAN',)]),
        ('all_tab_partitions', [('P_FIRST', "TO_YMINTERVAL('01-00')", 1, 10, 'USERS', 0)]),
        ('all_indexes', []),
    ]
    _made, scheme = scheme_of(scheme_rows, table='SPANS')
    assert len(scheme['blockers']) == 1
    assert 'P_FIRST' in scheme['blockers'][0]


def test_a_bound_at_midnight_is_not_reported_as_carrying_a_time():
    """
    Every ordinary DATE bound is written `TO_DATE(' 2024-01-01 00:00:00', 'SYYYY-MM-DD ...')`,
    and the finding is about the ones which are not at midnight. Said about all of them it is
    noise, and noise in a report is how the line which matters stops being read.
    """
    _made, scheme = scheme_of(ORDERS)
    assert not [text for text in scheme['notes'] if 'time of day' in text]


def test_a_bound_carrying_a_time_of_day_is_reported():
    scheme_rows = [
        ('autolist', [('RANGE', 'NONE', 1, 0, 'USERS', None, None, None)]),
        ('all_part_key_columns', [('ORDER_DATE',)]),
        ('all_tab_partitions', [
            ('P_SHIFT', "TO_DATE(' 2024-01-01 06:00:00', 'SYYYY-MM-DD HH24:MI:SS')",
             1, 10, 'USERS', 0)]),
        ('all_indexes', []),
    ]
    _made, scheme = scheme_of(scheme_rows, table='ORDERS')
    assert any('time of day' in text for text in scheme['notes'])


def test_a_range_scheme_ending_in_maxvalue_needs_nothing_said_about_its_end():
    _made, scheme = scheme_of(ORDERS)
    assert not [text for text in scheme['notes'] if 'refused with' in text]


def test_a_range_scheme_which_stops_says_where_it_stops():
    """
    §7 asks for the date to be said in advance rather than found when an INSERT is refused. It
    is what the source does today as well, which the note says: this is not something the
    migration took away.
    """
    _made, scheme = scheme_of(INTERVAL_ORDERS)
    note = [text for text in scheme['notes'] if 'SYS_P41' in text]
    assert note and "'2024-02-01 00:00:00'" in note[0]


def test_a_partitioned_table_with_no_partitions_refuses_every_row_and_is_stopped():
    scheme_rows = [
        ('autolist', [('RANGE', 'NONE', 0, 0, 'USERS', None, None, None)]),
        ('all_part_key_columns', [('ORDER_DATE',)]),
        ('all_tab_partitions', []),
        ('all_indexes', []),
    ]
    _made, scheme = scheme_of(scheme_rows)
    decision = plan_for(scheme)['ORDERS']
    assert any('refuses EVERY row' in issue for issue in decision.issues)


def test_a_release_without_the_recent_catalogue_columns_says_what_it_could_not_read():
    """
    ALL_PART_TABLES grew INTERVAL and REF_PTN_CONSTRAINT_NAME with 11g and AUTOLIST with 12.2,
    and an older release answers ORA-00904 for the whole statement. P2-8: what was not read is
    reported as not read, and never as a scheme which does not extend itself.
    """
    answers = [entry for entry in ORDERS if entry[0] != 'autolist']
    answers.insert(0, ('NULL, NULL, NULL', [('RANGE', 'HASH', 3, 16, 'USERS', None, None, None)]))
    _made, scheme = scheme_of(answers, raise_on=('autolist',))
    assert scheme['method'] == 'RANGE'
    assert any('was NOT read whether this scheme extends itself' in text
               for text in scheme['notes'])


def test_a_catalogue_which_cannot_be_read_answers_nothing_and_says_so():
    made = connector_reading(ORDERS, raise_on=('all_tab_partitions',))
    assert made.fetch_table_partitioning(
        {'source_schema_name': 'SCOTT', 'source_table_name': 'ORDERS'}) == {}
    assert any(level == 'ERROR' for level, _text in made.messages)


def test_the_partitioned_tables_of_a_schema_are_listed_in_one_query():
    made = connector_reading([('all_part_tables', [('ORDERS',), ('EVENTS',)])])
    assert made.fetch_partitioning_candidates('SCOTT') == {'ORDERS', 'EVENTS'}
    assert len(made.cursor.statements) == 1


def test_a_schema_which_cannot_be_listed_asks_about_every_table_instead():
    made = connector_reading([], raise_on=('all_part_tables',))
    assert made.fetch_partitioning_candidates('SCOTT') is None


# --------------------------------------------------------------------------------------
# the facts which decide whether a table CAN be partitioned - §4.4


FACTS = [
    ('num_rows FROM all_tables', [(400000,)]),
    ('all_tab_col_statistics', [
        ## name, data type, type owner, nullable, virtual, num_nulls
        ('ORDER_ID', 'NUMBER', None, 'N', 'NO', 0),
        ('ORDER_DATE', 'DATE', None, 'N', 'NO', 0),
        ('SHIPPED_AT', 'DATE', None, 'Y', 'NO', 41203),
        ('TOTAL', 'NUMBER', None, 'Y', 'YES', None),
        ('DOCUMENT', 'XMLTYPE', None, 'Y', 'NO', None),
        ('ADDRESS', 'ADDRESS_T', 'SCOTT', 'Y', 'NO', None),
        ('NOTE', 'VARCHAR2', None, 'Y', 'NO', None),
    ]),
    ('all_cons_columns', [('ORDERS_PK', 'P', 'ORDER_ID'),
                          ('ORDERS_NO_UQ', 'U', 'ORDER_NO')]),
    ('all_ind_columns', [('ORDERS_EXTRA_UQ', 'ORDER_REF')]),
    ('r_constraint_name', [('ORDER_ITEMS_FK', 'ORDER_ITEMS')]),
]


def facts_of(answers=None, raise_on=None):
    made = connector_reading(answers or FACTS, raise_on)
    return made.fetch_partitioning_facts(
        {'source_schema_name': 'SCOTT', 'source_table_name': 'ORDERS'})


def test_the_facts_of_a_table_come_out_of_the_catalogue_and_the_statistics():
    facts = facts_of()
    assert facts['row_estimate'] == 400000
    assert facts['columns']['ORDER_DATE']['not_null'] is True
    assert facts['columns']['SHIPPED_AT']['not_null'] is False
    ## 41203 of 400000, which is the number §4.3 wants said before the rows are refused one by one
    assert facts['columns']['SHIPPED_AT']['null_fraction'] == pytest.approx(0.1030075)
    assert facts['date_range_types'] == OracleConnector.DATE_RANGE_TYPES


def test_a_column_nobody_gathered_statistics_for_has_a_null_fraction_which_is_not_known():
    """
    P2-8 applied to a number: None is read by the checks as "not known" and reported as a check
    which was NOT made. Zero would be read as "there are no NULLs", which is another thing.
    """
    assert facts_of()['columns']['NOTE']['null_fraction'] is None


def test_a_virtual_column_is_a_generated_one():
    assert facts_of()['columns']['TOTAL']['is_generated'] is True
    assert facts_of()['columns']['ORDER_ID']['is_generated'] is False


@pytest.mark.parametrize('column, can_be_a_key', [
    ('ORDER_DATE', True),
    ('NOTE', True),
    ## XMLTYPE becomes `xml`, which has no default operator class of any kind
    ('DOCUMENT', False),
    ## an object type - the migration gives it a composite, which has no default class either
    ('ADDRESS', False),
])
def test_a_type_which_cannot_carry_a_partition_key_says_so(column, can_be_a_key):
    facts = facts_of()
    assert facts['columns'][column]['has_btree_opclass'] is can_be_a_key
    assert facts['columns'][column]['has_hash_opclass'] is can_be_a_key


def test_every_unique_key_is_read_and_not_only_the_primary_one():
    """
    §3.1 applies to the primary key, to every unique constraint and to every unique index which
    is not a constraint alike, and a table can have a key which extends cleanly and three unique
    indexes which do not.
    """
    keys = {key['name']: key for key in facts_of()['unique_keys']}
    assert keys['ORDERS_PK']['is_primary'] is True
    assert keys['ORDERS_PK']['columns'] == ['ORDER_ID']
    assert keys['ORDERS_NO_UQ']['is_primary'] is False
    assert 'ORDERS_EXTRA_UQ' in keys


def test_what_references_the_table_is_read_because_of_the_target_version():
    assert facts_of()['referenced_by'] == [{'name': 'ORDER_ITEMS_FK', 'table': 'ORDER_ITEMS'}]


def test_oracle_has_no_exclusion_constraint_and_no_table_inheritance():
    """
    An absence of the engine, which is not the same as something this connector did not read.
    """
    facts = facts_of()
    assert facts['exclusion_constraints'] == []
    assert facts['inherits_from_a_plain_table'] is False
    assert facts['is_a_plain_inheritance_parent'] is False


def test_facts_which_cannot_be_read_are_answered_as_not_read():
    assert facts_of(raise_on=('all_tab_col_statistics',)) is None


# --------------------------------------------------------------------------------------
# the checks of §4.4, against a real Oracle picture of a table


def entry(**overrides):
    written = {'table_name': 'ORDERS', 'partition_by': 'RANGE',
               'partitioning_columns': 'ORDER_DATE', 'date_range': 'month'}
    written.update(overrides)
    return written


def verdict_for(written=None, facts=None):
    facts = facts_of() if facts is None else facts
    ## the bounds are handed in so that these tests read what they say they read - the checks
    ## ABOUT THE COLUMN. Without them every verdict also carries the blocking issue for a probe
    ## which did not answer, and a test asserting "no issue mentions date_range" would pass or
    ## fail for a reason it never meant to be about.
    return partitioning.check_repartitioning(
        written or entry(), list(facts['columns']), None, target_version_num=160000, facts=facts,
        bounds_were_read=True,
        first_value=datetime.date(2024, 1, 1), last_value=datetime.date(2024, 6, 30))


def test_a_date_range_over_an_oracle_date_is_accepted():
    """
    The type carries a precision on Oracle - TIMESTAMP(6) - and the check has to read past it,
    or a column which carries a calendar perfectly well is refused for the way it is spelled.
    """
    assert any('ORDER_DATE is DATE' in note for note in verdict_for().notes)


def test_a_date_range_over_a_timestamp_with_a_precision_is_accepted():
    facts = facts_of()
    facts['columns']['ORDER_DATE']['type_name'] = 'TIMESTAMP(6) WITH TIME ZONE'
    assert not [issue for issue in verdict_for(facts=facts).issues if 'date_range' in issue]


def test_a_date_range_over_a_column_which_carries_no_date_is_refused():
    assert any('date_range' in issue
               for issue in verdict_for(entry(partitioning_columns='NOTE')).issues)


def test_the_column_is_found_however_the_entry_spells_it():
    """
    Oracle answers ORDER_DATE and the entry may be written order_date. A lookup which is not
    case-insensitive answers nothing for a column which is there, and every check about it is
    then skipped in silence - which reads exactly like a check which passed.
    """
    written = entry(partitioning_columns='order_date')
    assert any('is DATE' in note for note in verdict_for(written).notes)


def test_the_primary_key_which_does_not_contain_the_partitioning_column_stops_the_run():
    issues = verdict_for().issues
    assert any('ORDERS_PK' in issue and 'ORDER_DATE' in issue for issue in issues)
    assert any('ORDERS_NO_UQ' in issue for issue in issues)


def test_a_nullable_column_holding_nulls_and_no_default_partition_stops_the_run():
    written = entry(partitioning_columns='SHIPPED_AT')
    assert any('SHIPPED_AT' in issue and 'DEFAULT' in issue for issue in verdict_for(written).issues)


def test_a_generated_column_cannot_be_a_partition_key():
    written = entry(partition_by='HASH', partitioning_columns='TOTAL', date_range=None)
    assert any('GENERATED' in issue for issue in verdict_for(written).issues)


def test_a_column_whose_type_has_no_operator_class_cannot_be_one_either():
    written = entry(partition_by='LIST', partitioning_columns='DOCUMENT', date_range=None)
    assert any('operator class' in issue for issue in verdict_for(written).issues)


# --------------------------------------------------------------------------------------
# the whole way through: what the plan does with an Oracle scheme


def plan_for(scheme, mode='preserve'):
    return partitioning.build_plan(
        {'ORDERS': scheme}, ['ORDERS'], mode_of=lambda table_name: mode,
        target_version_num=160000)


def test_a_preserved_oracle_table_is_built_from_the_translated_bounds():
    _made, scheme = scheme_of(ORDERS)
    decision = plan_for(scheme)['ORDERS']
    assert decision.action == partitioning.PRESERVE
    ## the clause is written in the names of the target and the report shows the names of Oracle
    assert decision.target_key_definition == 'RANGE ("order_date")'
    assert decision.key_definition == 'RANGE (ORDER_DATE)'
    assert [part.bound for part in decision.partitions] == [
        partition['target_bound'] for partition in scheme['partitions']]
    ## and both spellings survive as far as the report
    assert decision.partitions[0].source_bound.startswith('TO_DATE(')


def test_what_the_source_scheme_costs_is_said_even_when_the_table_is_flattened():
    """
    A note is a fact about the source - the sub-partitioning, the tablespaces, the global index.
    It is worth saying whatever becomes of the table.
    """
    _made, scheme = scheme_of(ORDERS)
    decision = plan_for(scheme, mode='flatten')['ORDERS']
    assert decision.action == partitioning.FLATTEN
    assert any('sub-partitioned' in warning for warning in decision.warnings)


def test_a_scheme_which_cannot_be_built_stops_the_run_only_where_it_would_be_built():
    scheme_rows = [
        ('autolist', [('REFERENCE', 'NONE', 2, 0, 'USERS', None, 'ORDER_ITEMS_FK', None)]),
        ('all_part_key_columns', []),
        ('all_tab_partitions', [('P1', None, 1, 10, 'USERS', 0)]),
        ('all_indexes', []),
    ]
    _made, scheme = scheme_of(scheme_rows, table='ORDER_ITEMS')
    preserved = partitioning.build_plan(
        {'ORDER_ITEMS': scheme}, ['ORDER_ITEMS'], mode_of=lambda name: 'preserve',
        target_version_num=160000)['ORDER_ITEMS']
    assert preserved.issues
    flattened = partitioning.build_plan(
        {'ORDER_ITEMS': scheme}, ['ORDER_ITEMS'], mode_of=lambda name: 'flatten',
        target_version_num=160000)['ORDER_ITEMS']
    ## flattening it builds nothing which needs the scheme, so there is nothing to refuse
    assert flattened.issues == []


def test_a_partition_name_postgresql_would_truncate_is_refused():
    """
    Oracle allows 128 bytes since 12.2 and PostgreSQL allows 63, and it truncates the rest
    silently - which turns two partitions of a long table into one name and a collision found
    when the second CREATE TABLE fails.
    """
    _made, scheme = scheme_of(ORDERS)
    scheme['partitions'][0]['name'] = 'ORDERS_' + 'X' * 60
    decision = plan_for(scheme)['ORDERS']
    assert any('63' in issue for issue in decision.issues)


def test_an_oracle_scheme_says_how_many_levels_it_has_because_nothing_walks_into_it():
    """
    §4.2's headline counts the tables partitioned on more than one level. A PostgreSQL source
    answers it by having its sub-partitions walked; an Oracle sub-partition is not a table at
    all, so the catalogue row is the only place which says the second level is there.
    """
    _made, scheme = scheme_of(ORDERS)
    assert scheme['levels_below'] == [
        {'level': 2, 'method': 'HASH', 'columns': ['CUSTOMER_ID'], 'partition_count': 48}]
    decision = plan_for(scheme)['ORDERS']
    assert decision.source_level_count == 2
    ## and the target is given one of them, which is the whole of §2.2
    assert decision.target_level_count == 1

    _made, flat = scheme_of(INTERVAL_ORDERS)
    assert flat['levels_below'] == []
    assert plan_for(flat)['ORDERS'].source_level_count == 1


def test_a_preserved_oracle_key_is_checked_against_the_primary_key():
    """
    Oracle keeps a PRIMARY KEY (ORDER_ID) on a table partitioned by ORDER_DATE in a GLOBAL
    index, which is legal, ordinary, and has no counterpart in PostgreSQL. Without this the
    table is created, 400 million rows are loaded, and ADD PRIMARY KEY is refused at the end.
    """
    _made, scheme = scheme_of(ORDERS)
    decision = plan_for(scheme)['ORDERS']
    partitioning.check_preserved_keys(
        decision, [{'name': 'ORDERS_PK', 'columns': ['ORDER_ID'], 'is_primary': True}])
    assert any('ORDERS_PK' in issue and 'ORDER_DATE' in issue for issue in decision.issues)


# --------------------------------------------------------------------------------------
# the statements a run really produces for an Oracle table


def test_the_create_table_of_a_preserved_oracle_table_and_its_partitions():
    """
    The whole way through, with the target connector which really writes the statements: what
    the planner appends to the CREATE TABLE and what it hands the orchestrator to run behind it.
    """
    from credativ_pg_migrator.planner import Planner

    _made, scheme = scheme_of(ORDERS)
    plan = plan_for(scheme)

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
    assert clause == ' PARTITION BY RANGE ("order_date")'
    assert statements == [
        'CREATE TABLE "migtest"."orders_2023" PARTITION OF "migtest"."orders" '
        "FOR VALUES FROM (MINVALUE) TO ('2024-01-01 00:00:00')",
        'CREATE TABLE "migtest"."orders_2024" PARTITION OF "migtest"."orders" '
        "FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2025-01-01 00:00:00')",
        'CREATE TABLE "migtest"."orders_max" PARTITION OF "migtest"."orders" '
        "FOR VALUES FROM ('2025-01-01 00:00:00') TO (MAXVALUE)",
    ]


PARTITION_SQL = ('CREATE TABLE "{target_schema_name}"."{target_table_name}" PARTITION OF '
                 '"{target_schema_name}"."{parent_table_name}" {partition_bound}')
