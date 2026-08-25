# SPDX-License-Identifier: GPL-3.0-or-later
"""
The query conversion of PostgreSQL to PostgreSQL - P3-5 of `development/OPEN_ISSUES.md`.

The other of the two sources `convert_query_code()` was missing, and the one the cost table
of §7.1 calls trivial: the statement is already the dialect of the target, so there is no
dialect to convert. What it is not is empty, and that is what this file is about.

A migration into another database writes the rows again, in an order of its own, on another
server with another catalogue and another session. Four kinds of statement read exactly that
and are accepted by the target without a word:

  * the system columns - ctid, xmin, xmax, cmin, cmax, tableoid. PostgreSQL refuses to create
    a user column of any of those names, so a name found is always the system column, and
    every one of their values is a different one after a migration. Refused,
  * currval() and lastval(), which answer what the previous statement of the same session
    did. Refused - nextval() never reaches here, gate 3 refuses it because it writes,
  * the catalogue, the session functions and a name resolved out of a string literal
    ('x'::regclass, 'english'::regconfig) - reported as warnings, because the statement is
    still the statement the application wrote,
  * an explicit COLLATE, which has to exist on the target cluster and sort the same way.

The conversion also reads the statement as PostgreSQL before it answers, so a statement no
parser can read is reported here rather than three stages further on, where the message would
name a stage it never reached.

Nothing here talks to a database.

Run with:  python3 -m pytest tests/test_postgresql_query_conversion.py -v
"""

import os
import sys
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.connectors.postgresql_connector import PostgreSQLConnector


@pytest.fixture
def connector():
    made = PostgreSQLConnector.__new__(PostgreSQLConnector)
    made.config_parser = MagicMock()
    return made


def convert(connector, sql):
    return connector.convert_query_code({
        'query_code': sql,
        'source_schema_name': 'app',
        'target_schema_name': 'migtest',
        'target_db_type': 'postgresql',
        'statement_id': 'queries.sql:1',
    })


def test_the_source_declares_that_it_converts_statements(connector):
    assert connector.query_conversion_supported() is True


# --------------------------------------------------------------------------------------
# the statement is already PostgreSQL


@pytest.mark.parametrize('sql', [
    'SELECT id, name FROM app.customers WHERE id = $1',
    'SELECT c.id FROM app.customers c JOIN app.orders o ON o.cid = c.id',
    "SELECT count(*) FILTER (WHERE state = 'open') FROM app.orders",
    'WITH recent AS (SELECT id FROM app.orders) SELECT * FROM recent',
])
def test_a_statement_of_the_source_is_answered_as_it_stands(connector, sql):
    answer = convert(connector, sql)
    assert answer['converted'] is True
    assert answer['error'] is None
    assert answer['code'] == sql


def test_the_schema_is_left_for_the_name_map(connector):
    """
    §7.3 rewrites the schema, the tables and the columns through the parsed statement and
    reports what the migration does not know. Doing it here by text would rename a name inside
    a string literal, and would leave the report with nothing to report.
    """
    answer = convert(connector, 'SELECT id FROM app.customers')
    assert answer['code'] == 'SELECT id FROM app.customers'


def test_a_statement_which_cannot_be_read_as_postgresql_is_reported(connector):
    answer = convert(connector, 'SELECT FROM WHERE')
    assert answer['converted'] is False
    assert answer['code'] == ''
    assert 'could not be read as PostgreSQL' in answer['error']


# --------------------------------------------------------------------------------------
# what a migration takes away, and the target test cannot see


@pytest.mark.parametrize('sql', [
    'SELECT ctid, id FROM app.customers',
    'SELECT id FROM app.customers WHERE xmin::text = $1',
    'SELECT tableoid, id FROM app.customers',
    'SELECT cmin, cmax FROM app.customers',
    'SELECT id FROM app.customers ORDER BY ctid',
])
def test_a_statement_which_reads_a_system_column_is_refused(connector, sql):
    """
    Every one of these is valid on the target and answers other rows there - the migration
    inserted them again, in an order of its own. There is no name clash to worry about:
    PostgreSQL refuses to create a user column called ctid or xmin, so the name is always the
    system column.
    """
    answer = convert(connector, sql)
    assert answer['converted'] is False
    assert answer['code'] == ''
    assert 'system column' in answer['error']


@pytest.mark.parametrize('sql', [
    "SELECT currval('app.customers_id_seq')",
    'SELECT lastval()',
])
def test_the_session_sequence_functions_are_refused(connector, sql):
    answer = convert(connector, sql)
    assert answer['converted'] is False
    assert 'PREVIOUS statement of the same session' in answer['error']


def test_a_system_column_named_in_a_string_literal_is_not_one(connector):
    answer = convert(connector, "SELECT id FROM app.t WHERE note = 'sorted by ctid'")
    assert answer['converted'] is True


# --------------------------------------------------------------------------------------
# what is carried over and answers about the target - the warnings


def test_a_name_inside_a_literal_cannot_be_mapped_and_is_reported(connector):
    answer = convert(connector, "SELECT to_tsvector('english'::regconfig, body) FROM app.docs")
    assert answer['converted'] is True
    assert any('inside a string literal' in warning for warning in answer['warnings'])


def test_reading_the_catalogue_answers_about_the_target(connector):
    answer = convert(connector, 'SELECT relname FROM pg_catalog.pg_class')
    assert answer['converted'] is True
    assert any('catalogue of the server' in warning for warning in answer['warnings'])


def test_the_session_functions_answer_the_target(connector):
    answer = convert(connector, 'SELECT current_database(), current_user')
    assert answer['converted'] is True
    assert any('asks about the connection' in warning for warning in answer['warnings'])


def test_an_explicit_collation_has_to_exist_on_the_target(connector):
    answer = convert(connector, 'SELECT name FROM app.t ORDER BY name COLLATE "de-DE-x-icu"')
    assert answer['converted'] is True
    assert any('names a collation explicitly' in warning for warning in answer['warnings'])


def test_a_plain_statement_carries_no_warnings_at_all(connector):
    """A warning on every statement is a warning nobody reads."""
    answer = convert(connector, 'SELECT id, name FROM app.customers WHERE id = $1')
    assert answer['warnings'] == []


# --------------------------------------------------------------------------------------
# the view path and the query path are one conversion


def test_the_view_path_goes_through_the_same_converter(connector):
    """
    §15 phase 2. What the two share for PostgreSQL is the text search references, which name
    an object of the migration from inside a string literal and can therefore not be rewritten
    as identifiers.
    """
    ddl = connector.convert_view_code({
        'view_code': "SELECT to_tsvector('german'::regconfig, body) FROM docs",
        'target_view_name': 'v_docs', 'target_schema_name': 'migtest', 'view_type': 'VIEW',
        'text_search_objects': {
            'german': {'target_schema_name': 'migtest', 'target_object_name': 'german'}},
    })
    assert '\'migtest.german\'::regconfig' in ddl
    assert 'CREATE VIEW "migtest"."v_docs" AS' in ddl


def test_a_view_without_text_search_objects_is_unchanged(connector):
    ddl = connector.convert_view_code({
        'view_code': 'SELECT id FROM customers', 'target_view_name': 'v',
        'target_schema_name': 'migtest', 'view_type': 'VIEW',
    })
    assert ddl == 'CREATE VIEW "migtest"."v" AS SELECT id FROM customers;'
