# SPDX-License-Identifier: GPL-3.0-or-later
"""
The gates which decide whether a statement of an application is a read.

This is the safety property of the query conversion: a statement which is not a SELECT is
never converted and never sent to either database. Every construct here is one which begins
with SELECT and still writes or locks, or one which a single way of asking would let
through - which is why the question is asked in four independent ways.

Nothing in this file talks to a database.

Run with:  python3 -m pytest tests/test_query_classifier.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.query_conversion.classifier import (
    classify, classify_converted, dialect_for)
from credativ_pg_migrator.query_conversion.parameters import extract


def refuses(text, source='mssql'):
    result = classify(text, source)
    return result.verdict == 'refused'


# --------------------------------------------------------------------------------------
# what is a read


@pytest.mark.parametrize('text', [
    "SELECT a FROM t WHERE b = 1",
    "SELECT TOP 10 a FROM t ORDER BY a",
    "select a from t union select b from u",
    "SELECT a FROM t EXCEPT SELECT a FROM u",
    "WITH c AS (SELECT 1 AS x) SELECT * FROM c",
    "(SELECT a FROM t)",
    "-- name: daily\nSELECT a FROM t",
])
def test_a_read_is_recognised_as_one(text):
    assert classify(text, 'mssql').is_select, classify(text, 'mssql').reason


# --------------------------------------------------------------------------------------
# gate 1 and gate 2 - what the parser says, and what the text says


@pytest.mark.parametrize('text', [
    "INSERT INTO t (a) VALUES (1)",
    "UPDATE t SET a = 1",
    "DELETE FROM t",
    "MERGE INTO t USING u ON t.a = u.a WHEN MATCHED THEN UPDATE SET t.b = u.b",
    "TRUNCATE TABLE t",
    "CREATE TABLE t (a int)",
    "ALTER TABLE t ADD b int",
    "DROP TABLE t",
    "GRANT SELECT ON t TO bob",
    "REVOKE SELECT ON t FROM bob",
    "EXEC some_proc 1",
    "EXECUTE some_proc",
    "CALL some_proc(1)",
    "SET NOCOUNT ON",
    "USE other_database",
    "BEGIN TRANSACTION",
    "COMMIT",
    "ROLLBACK",
    "LOCK TABLE t IN EXCLUSIVE MODE",
    ## the writes which are not spelled the same way everywhere
    "REPLACE INTO t (a) VALUES (1)",
    "LOAD DATA INFILE '/tmp/x.csv' INTO TABLE t",
    "UNLOAD TO '/tmp/x.unl' SELECT * FROM t",
    "COPY t FROM '/tmp/x.csv'",
    "TRUNCATE TABLE t",
    "REFRESH TABLE mqt_orders",
    "DECLARE c CURSOR FOR SELECT a FROM t",
    "FETCH NEXT FROM c",
    "START TRANSACTION",
    "WRITETEXT t.notes @ptr 'x'",
])
def test_a_statement_which_writes_is_refused(text):
    assert refuses(text), f"not refused: {text}"


def test_a_write_is_refused_even_behind_a_comment():
    assert refuses("-- the daily update\n/* twice */ UPDATE t SET a = 1")


def test_a_write_hidden_behind_a_semicolon_is_refused():
    """A file split wrongly, or an entry written to smuggle a second statement in."""
    assert refuses("SELECT a FROM t; DELETE FROM t")


@pytest.mark.parametrize('text', [
    "UPDATE t SET a = ((( 1",
    "DELETE FROM t WHERE a IN ((( ",
    "MERGE t USING ((( ",
])
def test_a_write_the_parser_cannot_read_is_refused_by_the_text_alone(text):
    """
    This is what gate 2 is for, and the only case in which it decides on its own: the parser
    cannot say what such a statement is, and 'the migrator does not understand it' is a very
    different answer from 'it writes'. The first is reported as NOT CONVERTED and left to a
    developer, the second must never be converted at all.
    """
    result = classify(text, 'mssql')
    assert result.verdict == 'refused'
    assert result.gate == 2


# --------------------------------------------------------------------------------------
# gate 3 - the constructs which begin with SELECT and still write or lock


def test_select_into_a_new_table_is_refused():
    """It creates and fills a table. The text begins with SELECT."""
    result = classify("SELECT a, b INTO newtable FROM t", 'mssql')
    assert result.verdict == 'refused'
    assert 'INTO newtable' in result.reason


def test_select_into_a_host_variable_is_refused():
    result = classify("SELECT a INTO :hostvar FROM t", 'informix')
    assert result.verdict == 'refused'
    assert 'host variable' in result.reason


@pytest.mark.parametrize('text,source', [
    ("SELECT a, b FROM t WHERE c = 1 INTO TEMP work_table", 'informix'),
    ("SELECT a FROM t INTO SCRATCH work_table", 'informix'),
    ("SELECT a INTO :hostvar FROM t", 'ibm_db2_luw'),
    ("SELECT a INTO @result FROM t", 'sql_anywhere'),
])
def test_select_into_is_refused_although_the_statement_does_not_parse(text, source):
    """
    'INTO TEMP' is modelled by no parser, and a host variable is not SQL - so neither
    reaches the parsed statement. Both write, or are a fragment of a program, and have to be
    refused whether or not anything could read them.
    """
    result = classify(text, source)
    assert result.verdict == 'refused'
    assert result.gate == 2


@pytest.mark.parametrize('text', [
    "SELECT order_id FROM FINAL TABLE (INSERT INTO orders (order_id) VALUES (1))",
    "SELECT event_id FROM OLD TABLE (DELETE FROM customer_events WHERE event_id = 1)",
    "SELECT a FROM NEW TABLE (UPDATE t SET a = 1)",
])
def test_a_data_change_table_reference_is_refused(text):
    """
    The Db2 construct which looks the most like a read: it begins with SELECT, it gives back
    rows, and the write inside it is carried out.
    """
    result = classify(text, 'ibm_db2_luw')
    assert result.verdict == 'refused'
    assert result.gate == 2


def test_a_bare_values_statement_is_a_read():
    """It gives back the rows written into it and touches no table."""
    assert classify("VALUES (1, 'ONE')", 'ibm_db2_luw').is_select
    assert classify("VALUES (1), (2)", 'postgresql').is_select


def test_the_values_of_an_insert_is_still_an_insert():
    assert refuses("INSERT INTO t (a) VALUES (1)", 'ibm_db2_luw')


def test_a_data_modifying_cte_is_refused():
    result = classify("WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x", 'postgresql')
    assert result.verdict == 'refused'
    assert 'DELETE' in result.reason


@pytest.mark.parametrize('text,source', [
    ("SELECT a FROM t FOR UPDATE", 'oracle'),
    ("SELECT a FROM t HOLDLOCK", 'sybase_ase'),
    ("SELECT a FROM t WITH (UPDLOCK)", 'mssql'),
    ## the clause is not modelled in every dialect - it still locks
    ("SELECT a FROM t WHERE b = 1 FOR UPDATE", 'sql_anywhere'),
    ("SELECT a FROM t FOR SHARE", 'postgresql'),
    ("SELECT a FROM t LOCK IN SHARE MODE", 'mysql'),
    ("SELECT a FROM t WITH LOCK", 'ibm_db2_luw'),
])
def test_a_statement_which_locks_is_refused(text, source):
    assert refuses(text, source), f"not refused: {text}"


def test_a_locking_hint_is_refused_although_the_statement_does_not_parse():
    """
    'FROM orders o holdlock' is a hint written where a parser expects a second alias, so
    the statement cannot be read at all - and a statement which locks has to be refused
    whether or not anything could read it.
    """
    result = classify("SELECT o.order_id FROM orders o holdlock WHERE o.order_id = 1", 'sybase_ase')
    assert result.verdict == 'refused'
    assert result.gate == 2


@pytest.mark.parametrize('text,source', [
    ("SELECT nextval('s')", 'postgresql'),
    ("SELECT setval('s', 1)", 'postgresql'),
    ## not a function call at all - the pseudo column of Oracle and Db2
    ("SELECT seq_orders.NEXTVAL FROM DUAL", 'oracle'),
    ("SELECT NEXT VALUE FOR seq_orders", 'mssql'),
    ("SELECT PREVVAL FOR seq_orders FROM sysibm.sysdummy1", 'ibm_db2_luw'),
])
def test_a_statement_which_moves_a_sequence_on_is_refused(text, source):
    """Reading a sequence moves it on, so the statement writes."""
    assert refuses(text, source), f"not refused: {text}"


def test_two_statements_in_one_entry_are_refused():
    """Both of them reads, so no other gate would stop the entry."""
    result = classify("SELECT a FROM t; SELECT b FROM u", 'postgresql')
    assert result.verdict == 'refused'
    assert '2 statements' in result.reason


def test_two_statements_without_a_separator_are_not_converted_either():
    result = classify("SELECT a FROM t\nSELECT b FROM u", 'postgresql')
    assert not result.is_select


def test_nolock_is_converted_and_reported():
    """
    Not a write, so it is not refused - but it has no counterpart in PostgreSQL and is
    dropped by the conversion, which the reader of the output file has to be told.
    """
    result = classify("SELECT a FROM t WITH (NOLOCK)", 'mssql')
    assert result.is_select
    assert any('NOLOCK' in warning for warning in result.warnings)


# --------------------------------------------------------------------------------------
# a statement the parser cannot read is not guessed at


def test_a_statement_the_parser_cannot_read_is_neither_converted_nor_called_a_write():
    result = classify("SELECT ... FROM dual CONNECT BY PRIOR mgr_id = emp_id", 'oracle')
    assert result.verdict == 'unparsed'
    assert not result.is_select


def test_the_message_of_a_parse_error_stays_readable():
    """sqlglot marks the place of the error with terminal escapes; a file is not a terminal."""
    result = classify("SELECT ((( FROM", 'mssql')
    assert result.verdict == 'unparsed'
    assert '\x1b' not in result.reason
    assert '\n' not in result.reason


def test_an_empty_statement_is_refused():
    assert classify('', 'mssql').verdict == 'refused'
    assert classify('   \n  ', 'mssql').verdict == 'refused'


# --------------------------------------------------------------------------------------
# gate 4 - the converted statement, asked again


def test_the_converted_statement_is_classified_again():
    assert classify_converted('SELECT "a" FROM "s"."t"').is_select


@pytest.mark.parametrize('text', [
    'DELETE FROM "s"."t"',
    'UPDATE "s"."t" SET "a" = 1',
    'SELECT "a" INTO "s"."new" FROM "s"."t"',
])
def test_a_conversion_which_produced_something_else_is_refused(text):
    """
    A conversion is a transformation of text. What is about to be sent to PostgreSQL has to
    be what it claims to be, whatever it was before.
    """
    result = classify_converted(text)
    assert result.verdict == 'refused'
    assert result.gate == 4


def test_a_converted_statement_postgresql_cannot_read_is_refused():
    result = classify_converted('SELECT ((( broken')
    assert result.verdict == 'refused'
    assert result.gate == 4


# --------------------------------------------------------------------------------------
# the bind parameters of the application are not SQL


@pytest.mark.parametrize('text,source', [
    ("SELECT a FROM t WHERE b = %s", 'mssql'),
    ("SELECT a FROM t WHERE b = %(name)s", 'oracle'),
    ("SELECT a FROM t WHERE b = ?", 'mssql'),
    ("SELECT a FROM t WHERE b = :cust", 'oracle'),
])
def test_a_statement_with_markers_is_read_without_them(text, source):
    """
    '%s' is not SQL in any dialect, so a statement holding it cannot be parsed while the
    marker is in it - and would be reported as unreadable although nothing is wrong with it.
    The parser is given the statement with the markers replaced.
    """
    parameters, _warnings = extract(text)
    result = classify(text, source, parse_text=parameters.conversion_statement)
    assert result.is_select, result.reason


def test_a_write_with_markers_is_still_refused():
    parameters, _warnings = extract("UPDATE t SET a = %s WHERE b = %s")
    result = classify("UPDATE t SET a = %s WHERE b = %s", 'mssql',
                      parse_text=parameters.conversion_statement)
    assert result.verdict == 'refused'


def test_a_host_variable_is_still_recognised_behind_the_markers():
    """
    ':hostvar' of an embedded SQL program looks like a named bind parameter and has been
    replaced by one before the parser sees it - so the text of the application decides.
    """
    parameters, _warnings = extract("SELECT a INTO :hostvar FROM t")
    result = classify("SELECT a INTO :hostvar FROM t", 'informix',
                      parse_text=parameters.conversion_statement)
    assert result.verdict == 'refused'
    assert 'host variable' in result.reason


# --------------------------------------------------------------------------------------
# the dialects


@pytest.mark.parametrize('source,expected', [
    ('mssql', 'tsql'),
    ('sybase_ase', 'tsql'),
    ('oracle', 'oracle'),
    ('mysql', 'mysql'),
    ('mariadb', 'mysql'),
    ('postgresql', 'postgres'),
    ('sqlite', 'sqlite'),
    ('informix', None),
    ('something_else', None),
])
def test_every_source_is_read_with_the_dialect_which_fits_it(source, expected):
    assert dialect_for(source) == expected
