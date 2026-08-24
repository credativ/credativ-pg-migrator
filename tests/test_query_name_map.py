# SPDX-License-Identifier: GPL-3.0-or-later
"""
A converted statement names the objects the target really has.

P3-1 of development/OPEN_ISSUES.md, §7.3 and W1 of §9 of the conversion strategy. A statement
of an application names the objects of the **source**; the target holds them under a schema of
another name, under the case `names_case_handling` gave them and, where
`use_aliases_as_target_names` is set, under the alias the mapping chose. The migrator is the
only thing which knows both halves — and it did not use them: every output file said
*"name mapping: off"* and `SELECT * FROM SCOTT.ORDERS` was converted into a statement which
still says `SCOTT.ORDERS`.

Three things come out of the map, and the second and the third are why it is worth more than a
search and replace:

  * the **rewrite**, through the parsed statement, so a column named `count` is not rewritten
    inside a string literal;
  * the **unresolved-reference report** — the query which reads `AUDIT_LOG` in a run whose
    `exclude_tables` left `AUDIT_LOG` behind, said before the target test answers with a bare
    `relation "audit_log" does not exist`. The target cannot know what was left behind on
    purpose; this is the one place which can;
  * **W1** — a column whose type class changed under the migration. `NUMBER(1,0)` becomes
    `SMALLINT` and not `BOOLEAN` unless the configuration asks for it; where it was asked for,
    `WHERE FLAG = 1` stops working and no test level anywhere can see it.

Nothing here connects to anything: the protocol tables are a stub.

Run with:  python3 -m pytest tests/test_query_name_map.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.query_conversion import name_map as name_map_module
from credativ_pg_migrator.query_conversion.name_map import Column, NameMap, Table, type_class


def customers(**columns):
    base = {
        'id': Column('ID', 'id', 'NUMBER', 'NUMERIC'),
        'name': Column('NAME', 'name', 'VARCHAR2(30)', 'VARCHAR(30)'),
        'flag': Column('FLAG', 'flag', 'NUMBER(1,0)', 'SMALLINT'),
    }
    base.update(columns)
    return Table('SCOTT', 'CUSTOMERS', 'migtest', 'customers', base)


def a_map(**tables):
    known = {'customers': customers(),
             'orders': Table('SCOTT', 'ORDERS', 'migtest', 'orders', {
                 'cid': Column('CID', 'cid', 'NUMBER', 'NUMERIC'),
                 'total': Column('TOTAL', 'total', 'NUMBER', 'NUMERIC')})}
    known.update(tables)
    return NameMap('SCOTT', 'migtest', known)


def converted(sql, name_map=None):
    return name_map_module.apply(sql, name_map or a_map())


# --------------------------------------------------------------------------------------
# the rewrite


def test_the_schema_the_table_and_the_columns_are_the_ones_the_target_has():
    rewrite = converted('SELECT "ID", "NAME" FROM "SCOTT"."CUSTOMERS"')
    assert rewrite.mapped
    assert rewrite.sql == 'SELECT "id", "name" FROM "migtest"."customers"'


def test_a_name_without_a_schema_is_left_without_one():
    """
    Which schema it resolves to is the search_path of the application, and inventing one here
    would answer a question the application has not asked. The conversion warns about it
    instead, which it did before this and still does.
    """
    rewrite = converted('SELECT "ID" FROM "CUSTOMERS"')
    assert rewrite.sql == 'SELECT "id" FROM "customers"'
    assert 'migtest' not in rewrite.sql


def test_a_schema_which_was_not_the_migrated_one_is_left_alone_and_reported():
    rewrite = converted('SELECT "ID" FROM "OTHERDB"."CUSTOMERS"')
    assert '"OTHERDB"' in rewrite.sql
    assert 'OTHERDB (schema)' in rewrite.unresolved


def test_a_column_is_resolved_through_the_alias_of_its_table():
    rewrite = converted('SELECT c."ID", o."TOTAL" FROM "CUSTOMERS" c JOIN "ORDERS" o ON o."CID" = c."ID"')
    assert 'c."id"' in rewrite.sql
    assert 'o."total"' in rewrite.sql
    assert 'o."cid" = c."id"' in rewrite.sql


def test_a_column_of_the_outer_query_inside_an_exists_is_resolved():
    """Its qualifier is an alias the inner scope does not hold - the scopes above it do."""
    rewrite = converted('SELECT c."ID" FROM "CUSTOMERS" c '
                        'WHERE EXISTS (SELECT 1 FROM "ORDERS" o WHERE o."CID" = c."ID")')
    assert 'o."cid" = c."id"' in rewrite.sql


def test_an_unqualified_column_of_a_join_is_left_alone():
    """
    Guessing which of two tables a bare column came from is how a rewrite breaks a statement
    which worked. It is left as it stands and the target test answers for it.
    """
    rewrite = converted('SELECT "TOTAL" FROM "CUSTOMERS", "ORDERS"')
    assert '"TOTAL"' in rewrite.sql


def test_the_rewrite_goes_through_the_parsed_statement_and_not_through_the_text():
    """
    A column named like a common word must not be rewritten inside a string literal - which is
    the whole reason this is an AST walk and not a search and replace.
    """
    table = Table('SCOTT', 'T', 'migtest', 't', {'name': Column('NAME', 'renamed_name')})
    rewrite = converted("SELECT \"NAME\" FROM \"T\" WHERE \"NAME\" = 'NAME'",
                        NameMap('SCOTT', 'migtest', {'t': table}))
    assert "'NAME'" in rewrite.sql, 'the literal is not a column'
    assert '"renamed_name"' in rewrite.sql


def test_a_statement_which_cannot_be_read_is_answered_exactly_as_it_came_in():
    rewrite = converted('SELECT FROM WHERE ((( ')
    assert rewrite.mapped is False
    assert rewrite.sql == 'SELECT FROM WHERE ((( '


def test_the_same_rename_is_reported_once():
    rewrite = converted('SELECT "ID", "ID", "ID" FROM "CUSTOMERS"')
    assert rewrite.renamed.count('CUSTOMERS.ID -> id') == 1


# --------------------------------------------------------------------------------------
# the unresolved-reference report


def test_a_table_the_migration_does_not_know_is_named_and_not_renamed():
    """
    The query which reads AUDIT_LOG in a run whose exclude_tables left AUDIT_LOG behind. The
    target test would answer `relation "audit_log" does not exist` and say nothing about why.
    """
    rewrite = converted('SELECT * FROM "SCOTT"."AUDIT_LOG"')
    assert rewrite.unresolved == ['AUDIT_LOG']
    assert '"AUDIT_LOG"' in rewrite.sql, 'it is written out as it stands'


def test_a_column_the_migration_does_not_know_is_named_too():
    rewrite = converted('SELECT "NO_SUCH" FROM "CUSTOMERS"')
    assert 'CUSTOMERS.NO_SUCH (column)' in rewrite.unresolved


def test_each_unresolved_object_is_named_once():
    rewrite = converted('SELECT * FROM "AUDIT_LOG" a JOIN "AUDIT_LOG" b ON a.x = b.x')
    assert rewrite.unresolved.count('AUDIT_LOG') == 1


def test_a_statement_which_names_only_known_objects_reports_nothing():
    rewrite = converted('SELECT "ID" FROM "SCOTT"."CUSTOMERS"')
    assert rewrite.unresolved == []


# --------------------------------------------------------------------------------------
# W1 - a column whose type class changed


def test_a_column_which_became_a_boolean_is_reported():
    """
    The migrator's own decision coming back to bite the application, and the reason §9 singles
    W1 out: no test level anywhere can see it.
    """
    rewrite = converted('SELECT "FLAG" FROM "CUSTOMERS" WHERE "FLAG" = 1',
                        a_map(customers=customers(flag=Column('FLAG', 'flag', 'NUMBER(1,0)', 'BOOLEAN'))))
    assert len(rewrite.warnings) == 1
    warning = rewrite.warnings[0]
    assert warning.startswith('W1:')
    assert 'CUSTOMERS.FLAG' in warning
    assert 'NUMBER(1,0)' in warning and 'BOOLEAN' in warning
    assert 'FLAG IS TRUE' in warning, 'the message says what to write instead'


def test_a_boolean_which_became_a_number_is_reported_the_other_way_round():
    rewrite = converted('SELECT "FLAG" FROM "CUSTOMERS"',
                        a_map(customers=customers(flag=Column('FLAG', 'flag', 'BOOLEAN', 'SMALLINT'))))
    assert 'FLAG = 1' in rewrite.warnings[0]


def test_a_type_which_only_changed_its_name_is_not_reported():
    """
    A migration is expected to write VARCHAR2(30) as VARCHAR(30) and NUMBER as NUMERIC.
    Reporting those would bury the one which matters.
    """
    rewrite = converted('SELECT "NAME", "ID" FROM "CUSTOMERS"')
    assert rewrite.warnings == []


def test_a_column_is_reported_once_however_often_the_statement_names_it():
    rewrite = converted('SELECT "FLAG" FROM "CUSTOMERS" WHERE "FLAG" = 1 OR "FLAG" = 0',
                        a_map(customers=customers(flag=Column('FLAG', 'flag', 'NUMBER(1,0)', 'BOOLEAN'))))
    assert len(rewrite.warnings) == 1


@pytest.mark.parametrize('type_name,expected', [
    ('NUMBER(1,0)', 'number'), ('SMALLINT', 'number'), ('numeric(10,2)', 'number'),
    ('BOOLEAN', 'boolean'), ('bool', 'boolean'),
    ('VARCHAR2(30)', 'text'), ('TEXT', 'text'), ('CLOB', 'text'), ('char(2)', 'text'),
    ('TIMESTAMP(6)', 'date/time'), ('DATE', 'date/time'),
    ('BYTEA', 'binary'), ('BLOB', 'binary'), ('RAW(16)', 'binary'),
    ('UUID', 'uuid'), ('JSONB', 'json/xml'), ('XML', 'json/xml'),
    ('', ''), ('SOMETHING_ELSE', ''),
])
def test_the_classes_a_type_is_read_as(type_name, expected):
    assert type_class(type_name) == expected


def test_a_type_the_map_does_not_know_is_not_guessed_at():
    """Two types which cannot be classified are not reported as having changed class."""
    rewrite = converted('SELECT "ID" FROM "CUSTOMERS"',
                        a_map(customers=customers(id=Column('ID', 'id', 'HOUSE_TYPE', 'other_type'))))
    assert rewrite.warnings == []


# --------------------------------------------------------------------------------------
# the map itself, and what it says when it has nothing


class Config:
    def __init__(self, use_aliases=False):
        self.use_aliases = use_aliases

    def get_source_schema(self):
        return 'SCOTT'

    def get_target_schema(self):
        return 'migtest'

    def get_use_aliases_as_target_names(self):
        return self.use_aliases


class ProtocolTables:
    def __init__(self, tables=(), views=(), raises=None):
        self.tables = list(tables)
        self.views = list(views)
        self.raises = raises

    def fetch_all_tables(self, only_unfinished=False):
        if self.raises:
            raise self.raises
        return self.tables

    def fetch_all_views(self):
        return self.views

    def decode_table_row(self, row):
        return row

    def decode_view_row(self, row):
        return row


def table_row(**overrides):
    row = {
        'source_schema_name': 'SCOTT', 'source_table_name': 'CUSTOMERS',
        'target_schema_name': 'migtest', 'target_table_name': 'customers',
        'target_alias_name': '',
        'source_columns': {1: {'column_name': 'ID', 'data_type': 'NUMBER'}},
        'target_columns': {1: {'column_name': 'id', 'data_type': 'NUMERIC'}},
    }
    row.update(overrides)
    return row


def test_a_map_without_the_protocol_tables_says_which_of_the_two_it_is():
    built = name_map_module.build(None, Config())
    assert built.is_available is False
    assert 'protocol tables' in built.reason
    assert built.describe().startswith('name mapping: off')


def test_a_map_over_a_protocol_which_holds_nothing_is_not_available():
    built = name_map_module.build(ProtocolTables(), Config())
    assert built.is_available is False
    assert 'a migration has to have run' in built.reason


def test_a_protocol_which_cannot_be_read_is_reported_and_not_fatal():
    built = name_map_module.build(ProtocolTables(raises=RuntimeError('no such table')), Config())
    assert built.is_available is False
    assert 'no such table' in built.reason


def test_a_map_which_was_built_says_how_much_it_holds():
    built = name_map_module.build(ProtocolTables([table_row()]), Config())
    assert built.is_available
    assert built.describe().startswith('name mapping: on')
    assert built.table('customers').target_name == 'customers'
    assert built.table('CUSTOMERS').column('id').target_type == 'NUMERIC'


def test_the_alias_is_the_target_name_where_the_configuration_says_so():
    row = table_row(target_alias_name='kunden')
    assert name_map_module.build(ProtocolTables([row]), Config()).table('CUSTOMERS').target_name == 'customers'
    with_aliases = name_map_module.build(ProtocolTables([row]), Config(use_aliases=True))
    assert with_aliases.table('CUSTOMERS').target_name == 'kunden'


def test_a_view_is_an_object_a_statement_may_read_like_a_table():
    views = [{'source_schema_name': 'SCOTT', 'source_view_name': 'V_ACTIVE',
              'target_schema_name': 'migtest', 'target_view_name': 'v_active',
              'target_view_alias': ''}]
    built = name_map_module.build(ProtocolTables([table_row()], views), Config())
    assert built.table('V_ACTIVE').target_name == 'v_active'
    assert built.table('V_ACTIVE').kind == 'view'


def test_a_name_is_looked_up_without_regard_to_case_or_quoting():
    built = name_map_module.build(ProtocolTables([table_row()]), Config())
    for spelling in ('CUSTOMERS', 'customers', 'Customers', '"CUSTOMERS"', ' customers '):
        assert built.table(spelling) is not None, spelling


def test_a_map_which_is_not_available_changes_no_statement():
    rewrite = name_map_module.apply('SELECT * FROM "CUSTOMERS"', NameMap(reason='nothing to map'))
    assert rewrite.mapped is False
    assert rewrite.sql == 'SELECT * FROM "CUSTOMERS"'
    assert rewrite.unresolved == []


# --------------------------------------------------------------------------------------
# the pipeline, through the real convert_statement()


class SourceConnector:
    """A source whose conversion is the identity - what is asserted here is the stage after it."""

    def prepare_query_for_parsing(self, text):
        return text

    def apply_remote_objects_substitution(self, text):
        return text, []

    def convert_query_code(self, settings):
        return {'converted': True, 'code': settings['query_code'], 'warnings': []}


class WorkflowConfig(Config):
    def get_query_conversion_parameter_style(self):
        return 'auto'

    def get_query_conversion_parameter_output(self):
        return 'original'

    def get_query_conversion_target_test(self):
        return 'off'

    def get_names_case_handling(self):
        return 'lower'

    def convert_names_case(self, name):
        return (name or '').lower()

    def get_source_db_type(self):
        return 'oracle'


def converter_with(name_map):
    from credativ_pg_migrator.query_conversion.workflow import QueryConverter

    made = QueryConverter.__new__(QueryConverter)
    made.config_parser = WorkflowConfig()
    made.source_db_type = 'oracle'
    made.target_db_type = 'postgresql'
    made.source_schema = 'SCOTT'
    made.target_schema = 'migtest'
    made.name_map = name_map
    made.messages = []
    made.print_log_message = lambda level, message: made.messages.append((level, str(message)))
    made.source_connection = lambda: SourceConnector()
    made.test_on_target = lambda sql, has_parameters: ('not run', 'off', None)
    return made


def statement_of(text):
    from credativ_pg_migrator.query_conversion.splitter import Statement

    return Statement(text=text, ordinal=1, line_from=1, line_to=1, input_file='queries.sql')


def convert(text, name_map):
    made = converter_with(name_map)
    return made.convert_statement(statement_of(text), 1)


def test_a_statement_reaches_the_output_with_the_names_of_the_target():
    result = convert('SELECT "ID" FROM "SCOTT"."CUSTOMERS"', a_map())
    assert '"migtest"."customers"' in result.converted_sql
    assert '"id"' in result.converted_sql


def test_the_objects_the_migration_does_not_know_reach_the_result():
    result = convert('SELECT * FROM "SCOTT"."AUDIT_LOG"', a_map())
    assert result.unresolved_objects == ['AUDIT_LOG']
    assert any('which the migration does not know' in warning for warning in result.warnings)


def test_w1_reaches_the_result_of_the_statement():
    result = convert('SELECT "FLAG" FROM "CUSTOMERS" WHERE "FLAG" = 1',
                     a_map(customers=customers(flag=Column('FLAG', 'flag', 'NUMBER(1,0)', 'BOOLEAN'))))
    assert any(warning.startswith('W1:') for warning in result.warnings)


def test_a_run_without_a_map_converts_as_before_and_says_so():
    result = convert('SELECT "ID" FROM "SCOTT"."CUSTOMERS"', NameMap(reason='no protocol tables'))
    assert '"SCOTT"."CUSTOMERS"'.lower() in result.converted_sql.lower()
    assert result.unresolved_objects == []


def test_the_header_of_every_output_file_says_which_of_the_two_it_was():
    """
    It said "name mapping: off" in every file ever written, which was honest - and the file has
    to keep saying which of the two it is now that the other one is possible.
    """
    import inspect

    from credativ_pg_migrator.query_conversion.workflow import QueryConverter

    source = inspect.getsource(QueryConverter.run)
    assert 'self.name_map.describe()' in source
    assert "'name mapping: off - the names of the source are used as they are '" not in source
