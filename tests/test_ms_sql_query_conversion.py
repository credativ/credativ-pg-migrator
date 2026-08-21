# SPDX-License-Identifier: GPL-3.0-or-later
"""
The query conversion of MS SQL Server.

It was the first source the step was implemented for and it had no tests of its own. The
conversion is `convert_statement_code()`, which the view path and the query path both call, so
what is asserted here holds for the views of a migration as well.

Two things about it are not shared with the other sources. It is the only conversion in the
tree which asks the source database while it converts - it reads the user defined types - and
that answer is now read once per connector rather than once per statement: the query
conversion converts a whole file of statements with a pool of workers over one connector, and
fetch_user_defined_types() connects and disconnects around its query. And '*=', the outer join
of the same T-SQL family which sybase_ase rewrites, is not rewritten here - such a statement is
reported as one which cannot be read, which is asserted below so that the limitation is
recorded rather than discovered.

Nothing here connects to anything.

Run with:  python3 -m pytest tests/test_ms_sql_query_conversion.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.connectors.ms_sql_connector import MsSQLConnector
from credativ_pg_migrator.query_conversion import classifier


class RecordingLog:
    args = None

    def __init__(self):
        self.messages = []

    def print_log_message(self, level, message):
        self.messages.append((level, str(message)))

    def get_target_db_type(self):
        return 'postgresql'

    def get_connectivity(self, direction):
        return {'db_type': 'postgresql'}

    def get_remote_objects_substitution(self):
        return {}


class CountingConnector(MsSQLConnector):
    """A connector which counts how often the conversion reads the types of the source."""

    def __init__(self):
        self.config_parser = RecordingLog()
        self.udt_fetches = 0

    def fetch_user_defined_types(self, schema):
        self.udt_fetches += 1
        return {}


@pytest.fixture
def ms_sql():
    return CountingConnector()


def convert(connector, statement, source_schema='dbo', target_schema='tgt'):
    return connector.convert_query_code({
        'query_code': statement,
        'source_schema_name': source_schema,
        'target_schema_name': target_schema,
        'target_db_type': 'postgresql',
        'statement_id': 'test:1',
    })


## ---------------------------------------------------------------- the contract

def test_the_connector_says_it_can_convert_a_statement(ms_sql):
    assert ms_sql.query_conversion_supported() is True


def test_the_answer_is_a_dictionary_with_the_four_keys(ms_sql):
    answer = convert(ms_sql, 'SELECT id FROM dbo.customer')
    assert set(answer) == {'code', 'converted', 'warnings', 'error'}
    assert answer['converted'] is True
    assert answer['error'] is None


def test_a_statement_which_cannot_be_parsed_is_not_offered_as_converted(ms_sql):
    answer = convert(ms_sql, 'SELECT id FROM dbo.customer WHERE 1 = ')
    assert answer['converted'] is False
    assert answer['code'] == ''
    assert 'could not be parsed as T-SQL' in answer['error']


def test_nothing_is_ever_answered_with_the_text_it_was_given(ms_sql):
    broken = 'SELECT id FROM dbo.customer WHERE 1 = '
    answer = convert(ms_sql, broken)
    assert answer['code'] == ''


## ---------------------------------------------------------------- the types of the source

def test_the_types_of_the_source_are_read_once_and_not_once_per_statement(ms_sql):
    """
    convert_statement_code() calls _get_udt_map() every time it converts, and the query
    conversion converts a whole file of statements. It was a round trip to the source per
    statement, over a connection several workers shared.
    """
    for ordinal in range(5):
        convert(ms_sql, f'SELECT id FROM dbo.customer WHERE id = {ordinal}')
    assert ms_sql.udt_fetches == 1


def test_the_answer_is_kept_on_the_connector():
    connector = CountingConnector()
    connector._get_udt_map()
    connector._get_udt_map()
    assert connector.udt_fetches == 1
    assert connector._udt_map_cache == {}


## ---------------------------------------------------------------- the dialect

def test_top_becomes_limit(ms_sql):
    answer = convert(ms_sql, 'SELECT TOP 10 id FROM dbo.customer')
    assert 'LIMIT 10' in answer['code']
    assert 'TOP' not in answer['code'].upper()


def test_string_concatenation_with_plus_becomes_the_operator_of_the_target(ms_sql):
    answer = convert(ms_sql, "SELECT c.name + ' ' + c.surname AS full_name FROM dbo.customer c")
    assert '||' in answer['code']


def test_the_functions_of_the_source_are_mapped(ms_sql):
    answer = convert(ms_sql, 'SELECT getdate(), isnull(note, 0), len(name) FROM dbo.customer')
    code = answer['code'].lower()
    assert 'current_timestamp' in code
    assert 'coalesce' in code
    assert 'length' in code
    assert 'getdate' not in code


def test_a_niladic_function_is_not_written_as_a_call(ms_sql):
    """'user_name()' as 'CURRENT_USER()' is refused by PostgreSQL with a syntax error."""
    answer = convert(ms_sql, 'SELECT user_name() AS who')
    assert 'CURRENT_USER()' not in answer['code'].upper()


def test_datepart_becomes_extract(ms_sql):
    answer = convert(ms_sql, 'SELECT datepart(year, created) FROM dbo.customer')
    assert 'EXTRACT' in answer['code'].upper()


def test_the_source_schema_becomes_the_target_schema(ms_sql):
    answer = convert(ms_sql, 'SELECT id FROM dbo.customer')
    assert '"tgt"."customer"' in answer['code']
    assert 'dbo' not in answer['code']


def test_a_bind_parameter_carried_as_an_identifier_survives_the_conversion(ms_sql):
    answer = convert(ms_sql, 'SELECT id FROM dbo.customer WHERE name LIKE cpgm_bind_param_1')
    assert answer['converted'] is True
    assert 'cpgm_bind_param_1' in answer['code']


def test_a_statement_which_is_already_valid_postgresql_stays_readable(ms_sql):
    answer = convert(ms_sql, 'SELECT id, name FROM dbo.customer WHERE active = 1')
    assert answer['converted'] is True
    assert 'SELECT' in answer['code']
    assert 'WHERE' in answer['code']


## ---------------------------------------------------------------- what it does not do

def test_the_outer_join_written_star_equals_is_not_rewritten_here(ms_sql):
    """
    MS SQL Server read '*=' until 2005 and application files still hold it. sybase_ase, which
    is the same T-SQL family, rewrites it in prepare_query_for_parsing(); this connector has no
    such rewrite, so the statement is reported as one which cannot be read. It is reported -
    never handed back unconverted - which is what this asserts. See §17.2 of the strategy.
    """
    assert ms_sql.prepare_query_for_parsing('SELECT a FROM t1 x, t2 y WHERE x.i *= y.i') == \
        'SELECT a FROM t1 x, t2 y WHERE x.i *= y.i'
    answer = convert(ms_sql, 'SELECT c.id FROM dbo.customer c, dbo.orders o WHERE c.id *= o.cid')
    assert answer['converted'] is False
    assert answer['code'] == ''
    assert answer['error']


## ---------------------------------------------------------------- the gates, in this dialect

@pytest.mark.parametrize('statement, expected', [
    ('SELECT id INTO newtable FROM dbo.customer', 'creates and fills'),
    ('SELECT id FROM dbo.customer WITH (UPDLOCK)', 'takes locks'),
    ('SELECT NEXT VALUE FOR dbo.seq_order', 'sequence'),
    ('EXEC dbo.some_procedure', 'not a read'),
])
def test_the_gates_refuse_what_they_have_to_in_this_dialect(statement, expected):
    classification = classifier.classify(statement, 'mssql')
    assert classification.verdict == 'refused'
    assert expected in classification.reason


def test_a_nolock_hint_is_converted_and_warned_about():
    classification = classifier.classify('SELECT id FROM dbo.orders WITH (NOLOCK)', 'mssql')
    assert classification.verdict == 'select'
    assert any('NOLOCK' in warning for warning in classification.warnings)
