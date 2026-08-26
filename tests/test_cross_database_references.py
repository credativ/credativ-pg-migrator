# SPDX-License-Identifier: GPL-3.0-or-later
"""
The database in front of a table name - `ccd..t`, `ccd.dbo.t`, `SRV1.db.dbo.t`.

Transact-SQL names a table with up to four parts and lets the middle ones be left out.
PostgreSQL has two, so a qualifier which survives the conversion reaches the target as something
it cannot read. What the migrator wrote before this:

    FROM ccd..batch_task        ->  FROM ccd.."batch_task"      syntax error
    FROM ccd.dbo.batch_task     ->  FROM ccd."ccd"."batch_task" the database in front of the
                                                                schema which replaced it
    FROM otherdb..archive       ->  FROM otherdb.."archive"     syntax error

The transform of the connectors read only the `db` part of the parsed table, and for `ccd..t`
sqlglot puts the database into `catalog` and leaves `db` as an empty STRING - falsy, so
`if schema and ...` never matched it, and `catalog` was never looked at at all.

The distinction which makes this fixable without configuration: in `ccd..t` the database is
usually the database being MIGRATED - old Transact-SQL qualifies out of habit - and the migrator
knows its name from `source.database`. Such a reference is the migration's own table and the
qualifier is dropped. A reference to another database or another server is left exactly as the
source wrote it and reported: a name the target refuses is better than one which silently reads
a different table, and `remote_objects_substitution` is still the way to rewrite it.

The analysis is development/CROSS_DATABASE_REFERENCES.md.

Nothing here connects to anything.

Run with:  python3 -m pytest tests/test_cross_database_references.py -v
"""

import os
import sys

import pytest
import sqlglot

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.query_conversion import object_references


MIGRATED_DATABASE = 'ccd'
SOURCE_SCHEMA = 'dbo'
TARGET_SCHEMA = 'target_schema'


## ------------------------------------------------------------- reading the four parts


@pytest.mark.parametrize('statement,expected', [
    ('select * from t',                     ('', '', '', 't')),
    ('select * from dbo.t',                 ('', '', 'dbo', 't')),
    ('select * from ccd..t',                ('', 'ccd', '', 't')),
    ('select * from ccd.dbo.t',             ('', 'ccd', 'dbo', 't')),
    ('select * from SRV1.otherdb.dbo.t',    ('SRV1', 'otherdb', 'dbo', 't')),
    ('select * from "ccd".."My Table"',     ('', 'ccd', '', 'My Table')),
])
def test_every_shape_of_a_transact_sql_name_is_read_the_same_way(statement, expected):
    """
    The five shapes sqlglot answers differently for - the empty `db` of the three part form is a
    plain string and the four part form keeps its schema inside the name.
    """
    table = list(sqlglot.parse_one(statement, read='tsql').find_all(sqlglot.exp.Table))[0]
    assert object_references.read_tsql_table_parts(table) == expected


## --------------------------------------------------- the database being migrated is dropped


def resolve(statement):
    expression = sqlglot.parse_one(statement, read='tsql')
    unresolved = object_references.resolve_tsql_table_references(
        expression, MIGRATED_DATABASE, SOURCE_SCHEMA)
    return expression.sql(dialect='tsql'), unresolved


@pytest.mark.parametrize('statement', [
    'select * from ccd..batch_task',
    'select * from ccd.dbo.batch_task',
    'select * from CCD..batch_task',        # the name of a database is not case sensitive here
])
def test_a_reference_to_the_migrated_database_loses_its_qualifier(statement):
    converted, unresolved = resolve(statement)
    assert converted == 'SELECT * FROM dbo.batch_task'
    assert unresolved == [], 'the migrated database is not an unresolved reference'


def test_the_owner_the_source_wrote_is_kept():
    """
    'ccd.reporting.t' keeps 'reporting' - only the database part says nothing new. The schema
    mapping of the connector decides afterwards what becomes of the owner.
    """
    converted, _ = resolve('select * from ccd.reporting.t')
    assert converted == 'SELECT * FROM reporting.t'


def test_an_omitted_owner_becomes_the_schema_of_the_source():
    """'ccd..t' names the default owner, which for the migration is the schema it reads."""
    converted, _ = resolve('select * from ccd..t')
    assert converted == 'SELECT * FROM dbo.t'


def test_a_plain_name_is_not_touched():
    for statement in ('select * from t', 'select * from dbo.t'):
        converted, unresolved = resolve(statement)
        assert converted == sqlglot.parse_one(statement, read='tsql').sql(dialect='tsql')
        assert unresolved == []


## ------------------------------------------------ another database is reported, not guessed


@pytest.mark.parametrize('statement,reference', [
    ('select * from otherdb..archive', 'otherdb.archive'),
    ('select * from otherdb.dbo.archive', 'otherdb.dbo.archive'),
    ('select * from SRV1.otherdb.dbo.t', 'SRV1.otherdb.dbo.t'),
])
def test_a_reference_to_another_database_is_reported(statement, reference):
    converted, unresolved = resolve(statement)
    assert [item['reference'] for item in unresolved] == [reference]


def test_a_reference_to_another_database_is_left_exactly_as_it_was_written():
    """
    Collapsing 'otherdb..archive' to 'otherdb.archive' would make it a schema of the target and
    the object would read a different table without a word. It is left to be refused instead.
    """
    converted, _ = resolve('select * from otherdb..archive')
    assert 'otherdb..archive' in converted


def test_the_local_tables_of_a_mixed_statement_are_still_resolved():
    converted, unresolved = resolve(
        'select * from otherdb..archive a join ccd..orders o on o.id = a.oid')
    assert 'dbo.orders' in converted
    assert 'otherdb..archive' in converted
    assert [item['reference'] for item in unresolved] == ['otherdb.archive']


def test_the_message_names_the_object_and_every_reference():
    _, unresolved = resolve('select * from otherdb..a join otherdb..b on a.id = b.id')
    message = object_references.unresolved_reference_message('caller', 'v_report', unresolved)
    assert 'v_report' in message
    assert 'otherdb.a' in message and 'otherdb.b' in message
    assert 'remote_objects_substitution' in message, 'the escape hatch has to be named'


def test_no_message_when_everything_resolved():
    assert object_references.unresolved_reference_message('caller', 'v', []) == ''


def test_without_a_known_source_database_nothing_is_dropped():
    """
    A migration which cannot say which database it reads must not guess - dropping the qualifier
    would silently point the object at a table of the target.
    """
    expression = sqlglot.parse_one('select * from ccd..t', read='tsql')
    unresolved = object_references.resolve_tsql_table_references(expression, '', SOURCE_SCHEMA)
    assert 'ccd..t' in expression.sql(dialect='tsql')
    assert [item['reference'] for item in unresolved] == ['ccd.t']


## ------------------------------------------------------- through the two real connectors


class Config:
    args = None

    def __init__(self):
        self.messages = []

    def print_log_message(self, level, message):
        self.messages.append((level, str(message)))

    def warnings(self):
        return [message for level, message in self.messages if level == 'WARNING']

    def get_source_db_name(self):
        return MIGRATED_DATABASE

    def get_source_db_type(self):
        return 'sybase_ase'

    def get_target_db_type(self):
        return 'postgresql'

    def get_connectivity(self, direction):
        return {'db_type': 'postgresql'}

    def get_names_case_handling(self):
        return 'lower'

    def convert_names_case(self, name):
        return (name or '').lower()

    def get_remote_objects_substitution(self):
        return {}

    def get_data_types_substitution(self):
        return []


def sybase_view(view_code):
    from credativ_pg_migrator.connectors.sybase_ase_connector import SybaseASEConnector

    connector = SybaseASEConnector.__new__(SybaseASEConnector)
    connector.config_parser = Config()
    connector._udt_cache = {}
    return connector.convert_view_code({
        'view_code': view_code, 'source_schema_name': SOURCE_SCHEMA,
        'target_schema_name': TARGET_SCHEMA, 'target_db_type': 'postgresql',
        'view_name': 'v_report',
    }), connector.config_parser


def ms_sql_view(view_code):
    from credativ_pg_migrator.connectors.ms_sql_connector import MsSQLConnector

    connector = MsSQLConnector.__new__(MsSQLConnector)
    connector.config_parser = Config()
    return connector.convert_view_code({
        'view_code': view_code, 'source_schema_name': SOURCE_SCHEMA,
        'target_schema_name': TARGET_SCHEMA, 'target_db_type': 'postgresql',
        'target_view_name': 'v_report',
    }), connector.config_parser


VIEW_PATHS = [pytest.param(sybase_view, id='sybase_ase'), pytest.param(ms_sql_view, id='ms_sql')]


@pytest.mark.parametrize('convert', VIEW_PATHS)
def test_the_reported_statement_reaches_the_target_readable(convert):
    """The line of the report: FROM ccd.."batch_task_ccddeletebatch"."""
    converted, _ = convert('select t.id from ccd..batch_task_ccddeletebatch t')
    assert '..' not in converted
    assert f'"{TARGET_SCHEMA}"."batch_task_ccddeletebatch"' in converted


@pytest.mark.parametrize('convert', VIEW_PATHS)
def test_the_database_is_not_written_in_front_of_the_schema_which_replaced_it(convert):
    """'ccd.dbo.t' became 'ccd."ccd"."t"' - the same name twice."""
    converted, _ = convert('select * from ccd.dbo.batch_task')
    assert f'"{TARGET_SCHEMA}"."batch_task"' in converted
    assert f'{MIGRATED_DATABASE}."' not in converted


@pytest.mark.parametrize('convert', VIEW_PATHS)
def test_an_ordinary_view_is_unchanged_by_all_of_this(convert):
    converted, config = convert('select * from dbo.orders')
    assert f'"{TARGET_SCHEMA}"."orders"' in converted
    assert config.warnings() == []


@pytest.mark.parametrize('convert', VIEW_PATHS)
def test_a_view_reading_another_database_is_warned_about_by_name(convert):
    converted, config = convert('select * from otherdb..archive')
    assert any('v_report' in warning and 'otherdb.archive' in warning
               for warning in config.warnings()), config.warnings()
