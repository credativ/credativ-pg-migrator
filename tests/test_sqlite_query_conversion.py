# SPDX-License-Identifier: GPL-3.0-or-later
"""
The query conversion of SQLite - P3-5 of `development/OPEN_ISSUES.md`.

SQLite was one of the two sources `convert_query_code()` was missing, and the cost table of
§7.1 calls it one of the two cheapest. The cheap part is the transpilation: sqlglot reads
SQLite and writes PostgreSQL. What is not cheap, and what this file is about, is the line
between the three answers that transpilation can give.

  * what it writes correctly - left alone,
  * what it writes as valid PostgreSQL which answers something ELSE. `total()` becomes
    `sum()` and answers NULL over no rows where SQLite answers 0.0; `random()` keeps its name
    and answers a number between 0 and 1 instead of a 64 bit integer; `printf()` becomes a
    `format()` PostgreSQL really has, with other format codes. Every one of these runs on the
    target and passes its test - so the conversion is REFUSED, with the reason,
  * what has no counterpart at all - the catalogue of SQLite, ROWID, GLOB, MATCH, the
    collations, the full text search.

The refusals are read on the statement of the SOURCE and not on the converted text, because
the function mapping of the connector renames several of them on the way and leaves nothing
to recognise. The warnings are the defects which survive a passing target test: LIKE is case
insensitive in SQLite, NULL sorts first there, CURRENT_TIMESTAMP is UTC, and a CAST never
fails.

Nothing here needs a database - sqlite3 is in the standard library, but the conversion is
text and does not touch it.

Run with:  python3 -m pytest tests/test_sqlite_query_conversion.py -v
"""

import os
import sys
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.connectors.sqlite_connector import SQLiteConnector


@pytest.fixture
def connector():
    made = SQLiteConnector.__new__(SQLiteConnector)
    made.config_parser = MagicMock()
    return made


def convert(connector, sql):
    return connector.convert_query_code({
        'query_code': sql,
        'source_schema_name': 'main',
        'target_schema_name': 'migtest',
        'target_db_type': 'postgresql',
        'statement_id': 'queries.sql:1',
    })


# --------------------------------------------------------------------------------------
# the connector answers the capability gate


def test_the_source_declares_that_it_converts_statements(connector):
    """
    The gate of §7.1: a source which does not answer this stops the run at the start rather
    than having its statements passed through. Until P3-5 SQLite was such a source.
    """
    assert connector.query_conversion_supported() is True


# --------------------------------------------------------------------------------------
# what is converted


@pytest.mark.parametrize('sql, expected', [
    ('SELECT id, name FROM customers WHERE id = 1',
     'SELECT id, name FROM customers WHERE id = 1'),
    ## the function mapping of the connector, which the view path applies too
    ("SELECT ifnull(city, 'n/a') FROM customers",
     "SELECT COALESCE(city, 'n/a') FROM customers"),
    ('SELECT substr(name, 1, 3) FROM customers',
     'SELECT SUBSTRING(name FROM 1 FOR 3) FROM customers'),
    ## the paging of SQLite is the paging of PostgreSQL
    ('SELECT id FROM customers LIMIT 10 OFFSET 20',
     'SELECT id FROM customers LIMIT 10 OFFSET 20'),
])
def test_a_statement_sqlglot_writes_correctly_is_converted(connector, sql, expected):
    answer = convert(connector, sql)
    assert answer['converted'] is True
    assert answer['error'] is None
    assert answer['code'] == expected


def test_the_identifier_quoting_of_sqlite_becomes_the_one_of_postgresql(connector):
    answer = convert(connector, 'SELECT `name`, [city] FROM `customers`')
    assert answer['converted'] is True
    assert '`' not in answer['code'] and '[' not in answer['code']
    assert '"name"' in answer['code'] and '"customers"' in answer['code']


def test_the_names_of_the_tables_are_left_for_the_name_map(connector):
    """
    The view path prefixes a bare name with the schema of the target itself, by text, because
    a view body has to resolve without a search_path. A statement of an application is given
    the name map of §7.3 instead, which does it through the parsed statement and reports what
    the migration does not know - so the connector must not do it a second time by text.
    """
    answer = convert(connector, 'SELECT id FROM customers')
    assert 'migtest' not in answer['code']


def test_a_statement_no_parser_can_read_is_reported_and_not_handed_back(connector):
    answer = connector.convert_query_code({
        'query_code': 'SELECT * FROM WHERE ORDER',
        'source_schema_name': 'main', 'target_schema_name': 'migtest',
    })
    assert answer['converted'] is False
    assert answer['code'] == ''
    assert 'could not be read as SQLite' in answer['error']


def test_an_empty_statement_is_not_a_conversion(connector):
    answer = convert(connector, '   ')
    assert answer['converted'] is False
    assert answer['code'] == ''


# --------------------------------------------------------------------------------------
# what is refused, and why - the answers which would be valid PostgreSQL


@pytest.mark.parametrize('sql, expected_in_reason', [
    ('SELECT total(amount) FROM orders', 'answers 0.0 over no rows'),
    ('SELECT random()', 'signed 64 bit integer'),
    ("SELECT printf('%d', n) FROM t", 'format codes of C'),
    ('SELECT changes()', 'PREVIOUS statement'),
    ('SELECT last_insert_rowid()', 'PREVIOUS statement'),
    ('SELECT sqlite_version()', 'the database engine and not about the data'),
    ('SELECT typeof(x) FROM t', 'storage class'),
    ('SELECT hex(data) FROM t', "hex() of SQLite writes '2A'"),
    ("SELECT strftime('%Y', d) FROM t", 'Julian days'),
    ("SELECT json_valid(doc) FROM t", 'operators of PostgreSQL'),
    ('SELECT likely(x) FROM t', 'no such hint'),
])
def test_a_call_which_would_answer_something_else_is_refused(connector, sql, expected_in_reason):
    """
    Each of these is accepted by PostgreSQL after the conversion and answers another value -
    which is exactly what no level of the target test can see, and why the conversion is
    refused here instead of being offered with a warning.
    """
    answer = convert(connector, sql)
    assert answer['converted'] is False
    assert answer['code'] == ''
    assert expected_in_reason in answer['error']


@pytest.mark.parametrize('sql, expected_in_reason', [
    ('SELECT rowid, name FROM customers', 'physical row number'),
    ('SELECT t.rowid FROM customers t', 'physical row number'),
    ('SELECT _rowid_ FROM customers', 'physical row number'),
    ('SELECT * FROM sqlite_master', 'catalogue of SQLite'),
    ('SELECT * FROM pragma_table_info(:t)', 'SQLite database file itself'),
    ("SELECT * FROM t WHERE name GLOB 'A*'", 'wildcards of a shell'),
    ("SELECT * FROM docs WHERE body MATCH 'sql'", 'full text index'),
    ('SELECT name COLLATE NOCASE FROM t', 'collations of SQLite'),
    ("SELECT * FROM t WHERE name REGEXP 'a'", 'the application registers itself'),
])
def test_a_construct_with_no_counterpart_is_refused(connector, sql, expected_in_reason):
    answer = convert(connector, sql)
    assert answer['converted'] is False
    assert expected_in_reason in answer['error']


def test_the_refusal_is_read_on_the_statement_of_the_source(connector):
    """
    `changes()` is renamed to the literal 0 by get_sql_functions_mapping(), which the view
    path has applied since the connector was written. Read on the converted text there would
    be nothing left to find - and a statement answering a constant 0 where the source counted
    rows is the shape of defect this repository treats as a bug.
    """
    assert connector.sqlite_conversion_blockers('SELECT changes()')
    assert connector.sqlite_conversion_blockers(
        connector._finalize_sql('SELECT changes()')) == []


def test_a_name_in_a_string_literal_is_not_a_call(connector):
    """The blockers read the statement with its literals and comments blanked out."""
    answer = convert(connector, "SELECT id FROM t WHERE note = 'call random() here'")
    assert answer['converted'] is True


def test_a_call_named_in_a_comment_is_not_a_call(connector):
    answer = convert(connector, "SELECT id FROM t -- was: total(amount)\n")
    assert answer['converted'] is True


# --------------------------------------------------------------------------------------
# what is converted and still answers something else - the warnings of §9


def test_like_is_case_insensitive_in_sqlite(connector):
    answer = convert(connector, "SELECT id FROM t WHERE name LIKE 'abc'")
    assert answer['converted'] is True
    assert any('ignores the case of the ASCII letters' in warning for warning in answer['warnings'])


def test_the_null_ordering_is_reported_only_where_the_conversion_left_it_open(connector):
    """
    sqlglot writes the NULLS clause itself where it can, and it is right to: SQLite sorts NULL
    first ascending and PostgreSQL sorts it last. Warning about something the conversion has
    already done is how a warning earns being ignored, so it is read on the CONVERTED
    statement.
    """
    written = convert(connector, 'SELECT id FROM t ORDER BY name')
    assert 'NULLS FIRST' in written['code']
    assert not any('NULLS FIRST / NULLS LAST' in warning for warning in written['warnings'])


def test_a_cast_of_sqlite_never_fails_and_a_cast_of_postgresql_does(connector):
    answer = convert(connector, 'SELECT CAST(code AS INTEGER) FROM t')
    assert answer['converted'] is True
    assert any('never fails' in warning for warning in answer['warnings'])


def test_current_timestamp_is_utc_in_sqlite(connector):
    answer = convert(connector, 'SELECT CURRENT_TIMESTAMP')
    assert any('answer UTC in SQLite' in warning for warning in answer['warnings'])


def test_the_now_modifier_is_found_although_it_stands_in_a_literal(connector):
    """
    'now' is a string literal, which the masked text blanks out - so it is read on the text as
    it stands. Without that, `datetime('now')` came through as current_timestamp with nothing
    said about the offset between the two.
    """
    answer = convert(connector, "SELECT datetime('now')")
    assert answer['converted'] is True
    assert any('answer UTC in SQLite' in warning for warning in answer['warnings'])


def test_group_concat_answers_an_order_neither_side_promises(connector):
    answer = convert(connector, 'SELECT group_concat(name) FROM t')
    assert answer['converted'] is True
    assert any('string_agg' in warning for warning in answer['warnings'])


def test_a_refused_statement_still_carries_the_warnings_of_its_source(connector):
    """The reader of a NOT CONVERTED block needs everything which is known about it."""
    answer = convert(connector, "SELECT total(x) FROM t WHERE name LIKE 'a'")
    assert answer['converted'] is False
    assert any('ignores the case of the ASCII letters' in warning for warning in answer['warnings'])


# --------------------------------------------------------------------------------------
# the view path and the query path are one conversion


def test_the_view_path_goes_through_the_same_converter(connector):
    """
    §15 phase 2: the body converter is lifted out of the view path so that the two cannot
    drift apart. What the view gets around it is the DDL and the schema qualification a view
    body needs - not another conversion.
    """
    connector.connect = MagicMock()
    connector.disconnect = MagicMock()
    connector._migrated_object_names = MagicMock(return_value=set())
    connector.config_parser.convert_names_case = lambda name: (name or '').lower()

    body = 'SELECT ifnull(city, \'n/a\') FROM customers'
    ddl = connector.convert_view_code({
        'view_code': body, 'target_view_name': 'v_customers',
        'target_schema_name': 'migtest', 'source_schema_name': 'main', 'view_type': 'VIEW',
    })
    converted = connector.convert_query_code({
        'query_code': body, 'source_schema_name': 'main', 'target_schema_name': 'migtest',
    })
    assert converted['code'] in ddl


def test_a_view_whose_query_cannot_be_parsed_still_keeps_the_text_of_the_source(connector):
    """
    The view path answered that way before the converter was shared, and a migration which
    works today must not start failing: the view is reported by the migration and its code
    stays readable in the protocol.
    """
    connector.connect = MagicMock()
    connector.disconnect = MagicMock()
    connector._migrated_object_names = MagicMock(return_value=set())

    ddl = connector.convert_view_code({
        'view_code': 'SELECT * FROM WHERE ORDER', 'target_view_name': 'v_broken',
        'target_schema_name': 'migtest', 'source_schema_name': 'main', 'view_type': 'VIEW',
    })
    assert 'CREATE VIEW "migtest"."v_broken"' in ddl
    assert 'SELECT * FROM WHERE ORDER' in ddl
    levels = [call.args[0] for call in connector.config_parser.print_log_message.call_args_list]
    assert 'WARNING' in levels
