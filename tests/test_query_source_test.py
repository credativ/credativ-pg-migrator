# SPDX-License-Identifier: GPL-3.0-or-later
"""
The source test of the query conversion - §8.1 of the strategy, P3-5 of
`development/OPEN_ISSUES.md`.

Every block of every output file used to say `source test: not run`, because there was no
source test. What it is worth is one distinction which nothing on the target side can make:
a statement the SOURCE itself refuses was already broken, or reads an object the application
creates at run time, and reporting that as a failure of the conversion sends the reader after
the wrong thing entirely.

The rule the whole feature stands on is that it is **compile only**. The source of a
migration is a production database in every engagement this tool is used in: PREPARE,
EXPLAIN, SET NOEXEC ON and the prepareStatement of a JDBC driver compile a statement and run
none of it, `execute` is not a value the configuration takes, and every assertion below which
looks like a list of strings is really the assertion that nothing else was sent.

The SQLite mechanism is exercised against a real database file, created here - it is the one
source whose driver is in the standard library, so it is the one place where the whole route,
from the statement to the answer of a server, can be proven without an engagement's
infrastructure. The others are asserted at their statements: what is sent, in which order,
and that the session is put back whatever happened.

Run with:  python3 -m pytest tests/test_query_source_test.py -v
"""

import os
import sqlite3
import sys
import types
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

try:
    import oracledb  # noqa: F401
except ModuleNotFoundError:
    ## the Oracle driver is not a dependency of this migrator - see
    ## tests/test_oracle_fk_dependencies.py for why the stub is enough
    sys.modules['oracledb'] = types.ModuleType('oracledb')

from credativ_pg_migrator.database_connector import DatabaseConnector
from credativ_pg_migrator.connectors.postgresql_connector import PostgreSQLConnector
from credativ_pg_migrator.connectors.sqlite_connector import SQLiteConnector
from credativ_pg_migrator.query_conversion import parameters as parameters_module


def config_for(connectivity=None, timeout='30s'):
    config = MagicMock()
    config.get_connectivity.return_value = connectivity
    config.get_query_conversion_timeout.return_value = timeout
    return config


def connector_of(connector_class, connectivity=None, connection=None):
    made = connector_class.__new__(connector_class)
    made.config_parser = config_for(connectivity)
    made.source_or_target = 'source'
    made.connection = connection
    made.connect = MagicMock()
    made.disconnect = MagicMock()
    return made


# --------------------------------------------------------------------------------------
# the contract on the base connector


def bare_connector():
    """
    A connector which implements nothing of the source test - what every source answered
    before P3-5, and what a source with no compile-only mechanism still answers. Built with
    __new__ because DatabaseConnector is abstract in fifty other methods which have nothing
    to do with this.
    """
    abstract = {name: (lambda self, *args, **keywords: None)
                for name in DatabaseConnector.__abstractmethods__}
    bare = type('BareConnector', (DatabaseConnector,), abstract)
    made = bare.__new__(bare)
    made.config_parser = config_for()
    made.source_or_target = 'source'
    made.connection = None
    return made


def test_a_connector_without_a_mechanism_says_so_and_sends_nothing():
    """
    'not run' and 'OK' must never read alike: a source which cannot be compiled against says
    which of the two it is, and the block of the statement carries the reason.
    """
    made = bare_connector()
    outcome, message = made.test_query_on_source({'query_code': 'SELECT 1'})
    assert outcome == 'not run'
    assert 'no way of compiling a statement on the source without running it' in message


def test_the_base_connector_declares_no_mechanism_and_no_marker():
    made = bare_connector()
    assert made.source_test_mechanism() is None
    assert made.source_test_parameter_style() is None
    assert made.source_test_probe('SELECT 1') == ([], [])


# --------------------------------------------------------------------------------------
# SQLite - the whole route, against a real database


@pytest.fixture
def sqlite_source(tmp_path):
    path = tmp_path / 'source.db'
    database = sqlite3.connect(str(path))
    database.execute('CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)')
    database.execute("INSERT INTO customers VALUES (1, 'first')")
    database.commit()
    made = connector_of(SQLiteConnector, connection=database)
    yield made
    database.close()


def test_sqlite_compiles_the_statement_with_explain(sqlite_source):
    assert sqlite_source.source_test_mechanism() == 'EXPLAIN'
    outcome, message = sqlite_source.test_query_on_source(
        {'query_code': 'SELECT id, name FROM customers', 'parameter_count': 0})
    assert outcome == 'OK'
    assert 'EXPLAIN' in message


@pytest.mark.parametrize('sql, expected', [
    ('SELECT nope FROM customers', 'no such column'),
    ('SELECT * FROM missing_table', 'no such table'),
    ('SELECT FROM', 'syntax error'),
])
def test_sqlite_reports_what_the_source_refuses(sqlite_source, sql, expected):
    """
    This is the answer the whole feature exists for: the source says no, so the statement was
    broken before the migrator read it.
    """
    outcome, message = sqlite_source.test_query_on_source(
        {'query_code': sql, 'parameter_count': 0})
    assert outcome == 'FAILED'
    assert expected in message


def test_sqlite_compiles_a_statement_which_takes_bind_parameters(sqlite_source):
    """
    sqlite3 refuses to run a statement holding a marker without a value for it, even under
    EXPLAIN, so every marker is given NULL. Nothing of the statement is executed either way.
    """
    outcome, _message = sqlite_source.test_query_on_source(
        {'query_code': 'SELECT id FROM customers WHERE id = ? AND name = ?',
         'parameter_count': 2})
    assert outcome == 'OK'


def test_sqlite_still_resolves_the_names_of_a_statement_with_parameters(sqlite_source):
    outcome, message = sqlite_source.test_query_on_source(
        {'query_code': 'SELECT nope FROM customers WHERE id = ?', 'parameter_count': 1})
    assert outcome == 'FAILED'
    assert 'no such column' in message


def test_explain_reads_no_row_of_the_source(sqlite_source):
    """
    The promise of §8.1 in the one place it can be proven: EXPLAIN answers the byte code of
    the statement and never its rows - the table below holds a row, and nothing of it comes
    back.
    """
    cursor = sqlite_source.connection.cursor()
    cursor.execute('EXPLAIN SELECT id, name FROM customers')
    rows = cursor.fetchall()
    assert rows, 'EXPLAIN answers the byte code of the statement'
    assert not any('first' in str(value) for row in rows for value in row)


def test_sqlite_takes_the_marker_of_the_standard(sqlite_source):
    assert sqlite_source.source_test_parameter_style() == 'qmark'


# --------------------------------------------------------------------------------------
# PostgreSQL - PREPARE, inside a transaction which cannot write and is rolled back


def test_postgresql_prepares_the_statement_and_deallocates_it_again():
    made = connector_of(PostgreSQLConnector)
    statements, cleanup = made.source_test_probe('SELECT id FROM app.t;')
    assert statements[0] == 'BEGIN;'
    assert 'SET LOCAL transaction_read_only = on;' in statements
    assert "SET LOCAL statement_timeout = '30s';" in statements
    assert any(statement.startswith('PREPARE ') for statement in statements)
    assert any(statement.startswith('DEALLOCATE ') for statement in statements)
    assert cleanup == ['ROLLBACK;']


def test_the_postgresql_probe_is_the_statement_and_nothing_else():
    made = connector_of(PostgreSQLConnector)
    statements, cleanup = made.source_test_probe('SELECT id FROM app.t')
    body = [statement for statement in statements if statement.startswith('PREPARE ')][0]
    assert body.endswith('AS SELECT id FROM app.t;')
    assert not any('SELECT id' in statement for statement in cleanup)


def test_the_postgresql_probe_keeps_the_numbered_parameters():
    """PostgreSQL is the one source whose own marker is the one the statement already holds."""
    made = connector_of(PostgreSQLConnector)
    assert made.source_test_parameter_style() == 'numbered'
    statements, _cleanup = made.source_test_probe('SELECT id FROM t WHERE id = $1', 1)
    assert any('$1' in statement for statement in statements)


def test_the_source_transaction_is_rolled_back_even_when_the_statement_failed():
    """
    The cleanup runs whatever the probe did - a PREPARE which failed leaves the transaction of
    the source in an aborted state, and a connection left there answers every statement of
    the migrator behind it with 'current transaction is aborted'.
    """
    made = connector_of(PostgreSQLConnector)
    sent = []
    cursor = MagicMock()

    def execute(statement, *rest):
        sent.append(statement)
        if statement.startswith('PREPARE '):
            raise RuntimeError('syntax error at or near "FROM"')

    cursor.execute.side_effect = execute
    made.connection = MagicMock()
    made.connection.cursor.return_value = cursor

    outcome, message = made.test_query_on_source({'query_code': 'SELECT FROM', 'parameter_count': 0})
    assert outcome == 'FAILED'
    assert 'syntax error' in message
    assert sent[-1] == 'ROLLBACK;'


# --------------------------------------------------------------------------------------
# Transact-SQL - SET NOEXEC ON, and the session put back


def tsql_connector():
    from credativ_pg_migrator.connectors.sybase_ase_connector import SybaseASEConnector
    return connector_of(SybaseASEConnector)


def test_noexec_compiles_the_statement_and_is_taken_back():
    made = tsql_connector()
    assert made.source_test_mechanism() == 'SET NOEXEC ON'
    statements, cleanup = made.source_test_probe('SELECT id FROM dbo.t;')
    assert statements == ['SET NOEXEC ON', 'SELECT id FROM dbo.t']
    assert cleanup == ['SET NOEXEC OFF']


def test_a_statement_with_parameters_is_not_compiled_as_a_batch():
    """
    A bind marker has no place in a batch submitted as text, and writing a literal in its
    place would compile a different statement than the application runs. Not tested is the
    honest answer; a green OK for another statement is not.
    """
    made = tsql_connector()
    assert made.source_test_parameter_style() is None
    assert made.source_test_probe('SELECT id FROM t WHERE id = ?', 1) == ([], [])


def test_the_session_is_taken_out_of_noexec_whatever_the_statement_did():
    made = tsql_connector()
    sent = []
    cursor = MagicMock()

    def execute(statement, *rest):
        sent.append(statement)
        if statement.startswith('SELECT'):
            raise RuntimeError("Invalid column name 'nope'.")

    cursor.execute.side_effect = execute
    made.connection = MagicMock()
    made.connection.cursor.return_value = cursor

    outcome, _message = made.test_query_on_source(
        {'query_code': 'SELECT nope FROM t', 'parameter_count': 0})
    assert outcome == 'FAILED'
    assert sent == ['SET NOEXEC ON', 'SELECT nope FROM t', 'SET NOEXEC OFF']


def test_a_connection_which_cannot_be_taken_out_of_noexec_is_closed():
    """
    A connection left compile-only answers every statement behind it with nothing at all,
    which is the worst possible way for this to fail: silent and total. It is dropped instead,
    and the next statement opens a fresh one.
    """
    made = tsql_connector()
    cursor = MagicMock()

    def execute(statement, *rest):
        if statement == 'SET NOEXEC OFF':
            raise RuntimeError('connection is broken')

    cursor.execute.side_effect = execute
    made.connection = MagicMock()
    made.connection.cursor.return_value = cursor

    made.test_query_on_source({'query_code': 'SELECT 1', 'parameter_count': 0})
    assert made.disconnect.called
    assert made.connection is None


# --------------------------------------------------------------------------------------
# MySQL and MariaDB - EXPLAIN, which is not EXPLAIN ANALYZE


def mysql_connector():
    from credativ_pg_migrator.connectors.mysql_connector import MySQLConnector
    return connector_of(MySQLConnector)


def test_mysql_compiles_the_statement_with_explain():
    made = mysql_connector()
    assert made.source_test_mechanism() == 'EXPLAIN'
    statements, cleanup = made.source_test_probe('SELECT id FROM app.t;')
    assert statements == ['EXPLAIN SELECT id FROM app.t']
    assert cleanup == []


def test_mysql_never_sends_explain_analyze():
    """EXPLAIN plans the statement; EXPLAIN ANALYZE runs it. Only one of the two is ever sent."""
    made = mysql_connector()
    statements, _cleanup = made.source_test_probe('SELECT id FROM t')
    assert not any('ANALYZE' in statement.upper() for statement in statements)


# --------------------------------------------------------------------------------------
# Oracle - the parse call of the driver, and never EXPLAIN PLAN


def oracle_connector():
    from credativ_pg_migrator.connectors.oracle_connector import OracleConnector
    return connector_of(OracleConnector)


def test_oracle_parses_the_statement_with_the_driver():
    made = oracle_connector()
    assert made.source_test_mechanism() == 'Cursor.parse()'
    assert made.source_test_parameter_style() == 'oracle'
    cursor = MagicMock()
    made.connection = MagicMock()
    made.connection.cursor.return_value = cursor

    outcome, message = made.test_query_on_source(
        {'query_code': 'SELECT id FROM hr.employees;', 'parameter_count': 0})
    assert outcome == 'OK'
    assert 'Cursor.parse()' in message
    ## the trailing semicolon is not part of the statement Oracle is asked to parse
    cursor.parse.assert_called_once_with('SELECT id FROM hr.employees')
    assert not cursor.execute.called, 'nothing is executed on the source'


def test_oracle_reports_what_the_source_refuses():
    made = oracle_connector()
    cursor = MagicMock()
    cursor.parse.side_effect = RuntimeError('ORA-00942: table or view does not exist')
    made.connection = MagicMock()
    made.connection.cursor.return_value = cursor

    outcome, message = made.test_query_on_source(
        {'query_code': 'SELECT id FROM gone', 'parameter_count': 0})
    assert outcome == 'FAILED'
    assert 'ORA-00942' in message


# --------------------------------------------------------------------------------------
# JDBC - the mechanism §8.1 puts first, wherever a connector holds one


def test_a_jdbc_connection_prepares_the_statement_with_the_driver():
    """
    §8.1 ranks prepareStatement first: it resolves the names and the types without running
    anything and it takes the '?' of the standard, so a statement with bind parameters can be
    tested as well. It is written once on the base connector because five connectors of this
    migrator can be configured with `jdbc`.
    """
    made = tsql_connector()
    made.config_parser.get_connectivity.return_value = 'jdbc'
    assert made.source_test_mechanism() == 'JDBC prepareStatement'
    assert made.source_test_parameter_style() == 'qmark'

    prepared = MagicMock()
    made.connection = MagicMock()
    made.connection.jconn.prepareStatement.return_value = prepared

    outcome, message = made.test_query_on_source(
        {'query_code': 'SELECT id FROM t WHERE id = ?', 'parameter_count': 1})
    assert outcome == 'OK'
    assert 'JDBC prepareStatement' in message
    made.connection.jconn.prepareStatement.assert_called_once_with('SELECT id FROM t WHERE id = ?')
    assert prepared.close.called, 'the prepared statement is closed again'


def test_a_statement_the_jdbc_driver_refuses_is_reported_as_the_source_refusing_it():
    made = tsql_connector()
    made.config_parser.get_connectivity.return_value = 'jdbc'
    made.connection = MagicMock()
    made.connection.jconn.prepareStatement.side_effect = RuntimeError('Invalid object name t.')

    outcome, message = made.test_query_on_source({'query_code': 'SELECT 1', 'parameter_count': 0})
    assert outcome == 'FAILED'
    assert 'Invalid object name' in message


def test_jdbc_is_preferred_over_the_mechanism_of_the_source():
    """A connector which has both answers the one which resolves more and needs no cleanup."""
    made = tsql_connector()
    assert made.source_test_mechanism() == 'SET NOEXEC ON'
    made.config_parser.get_connectivity.return_value = 'jdbc'
    assert made.source_test_mechanism() == 'JDBC prepareStatement'


def test_a_source_which_cannot_be_reached_is_an_error_and_not_a_failure():
    """
    'FAILED' says the statement is broken. A source which cannot be reached says nothing about
    the statement at all, and must not be recorded as if it did.
    """
    made = tsql_connector()
    made.connect.side_effect = RuntimeError('connection refused')
    outcome, message = made.test_query_on_source({'query_code': 'SELECT 1', 'parameter_count': 0})
    assert outcome == 'ERROR'
    assert 'connection refused' in message


# --------------------------------------------------------------------------------------
# the markers, rewritten for the mechanism which is used


@pytest.mark.parametrize('style, expected', [
    ('numbered', 'SELECT id FROM t WHERE a = $1 AND b = $2'),
    ('qmark', 'SELECT id FROM t WHERE a = ? AND b = ?'),
    ('oracle', 'SELECT id FROM t WHERE a = :1 AND b = :2'),
])
def test_the_statement_is_written_with_the_marker_the_mechanism_takes(style, expected):
    assert parameters_module.to_source_test_style(
        'SELECT id FROM t WHERE a = $1 AND b = $2', style) == expected


def test_a_marker_inside_a_literal_is_text_and_is_left_alone():
    assert parameters_module.to_source_test_style(
        "SELECT id FROM t WHERE a = $1 AND note = 'costs $1'", 'qmark') == \
        "SELECT id FROM t WHERE a = ? AND note = 'costs $1'"


def test_an_unknown_style_is_refused_rather_than_guessed_at():
    with pytest.raises(ValueError, match='source test parameter style'):
        parameters_module.to_source_test_style('SELECT $1', 'pyformat')


# --------------------------------------------------------------------------------------
# every connector has decided, one way or the other


## Which source compiles a statement with what, when it is not reached over JDBC. A connector
## which is in neither table fails the test below: whether a source can be compiled against
## is a decision somebody has to make, and an empty answer looks the same as "no mechanism"
## without being it. Read out of the files with `ast`, so no driver has to be installed.
SOURCE_TEST_MECHANISMS = {
    'sqlite_connector.py': 'EXPLAIN',
    'postgresql_connector.py': 'PREPARE',
    'ms_sql_connector.py': 'SET NOEXEC ON',
    'sybase_ase_connector.py': 'SET NOEXEC ON',
    'mysql_query_conversion.py': 'EXPLAIN',
    'oracle_query_conversion.py': 'Cursor.parse()',
}

## The sources with no compile-only mechanism of their own, and why. Every one of them still
## gets the source test when it is configured with `jdbc` - prepareStatement is the mechanism
## §8.1 puts first, and it belongs to the driver and not to the dialect.
SOURCE_TEST_WITHOUT_MECHANISM = {
    'informix_connector.py':
        'SET EXPLAIN ON AVOID_EXECUTE is a session setting which writes an explain file on '
        'the server, so it is not compile-only in the sense §8.1 asks for. Over jdbc the '
        'connector is given prepareStatement.',
    'ibm_db2_luw_connector.py':
        'EXPLAIN of Db2 needs the explain tables to exist and writes a row into them. Over '
        'jdbc the connector is given prepareStatement.',
    'ibm_db2_i_connector.py': 'a DDL based connector - there is no source database to ask.',
    'ibm_db2_zos_connector.py': 'a DDL based connector - there is no source database to ask.',
    'sql_anywhere_connector.py':
        'sa_describe_query() takes the statement as a string literal, which means escaping '
        'the statement of an application into one. Over jdbc the connector is given '
        'prepareStatement.',
    'mariadb_connector.py': 'shares mysql_query_conversion.py with the MySQL connector.',
    'mysql_connector.py': 'shares mysql_query_conversion.py with the MariaDB connector.',
    'oracle_connector.py': 'the mechanism stands in oracle_query_conversion.py.',
    'db2_query_conversion.py': 'the dialect of the three Db2 connectors, which have no source '
                               'mechanism of their own.',
    'sql_anywhere_query_conversion.py': 'the dialect only - see sql_anywhere_connector.py.',
    'oracle_partitioning.py': 'not a connector - the partitioning of Oracle, written the way '
                              'PostgreSQL writes it. It has no driver in it at all.',
    'db2_partitioning.py': 'not a connector - the partitioning of the three Db2 connectors, '
                           'written the way PostgreSQL writes it. It has no driver in it at all.',
    'match_schemas.py': 'not a connector - it compares two schemas.',
    'tsql_parser.py': 'not a connector - the parser the Transact-SQL family shares.',
}


def connector_files():
    import glob
    directory = os.path.join(REPO, 'credativ_pg_migrator', 'connectors')
    return sorted(os.path.basename(path) for path in glob.glob(os.path.join(directory, '*.py'))
                  if not os.path.basename(path).startswith('__'))


def declares_a_mechanism(name):
    import ast
    path = os.path.join(REPO, 'credativ_pg_migrator', 'connectors', name)
    with open(path, encoding='utf-8') as handle:
        tree = ast.parse(handle.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in ('source_test_native_mechanism',
                                                               'source_test_mechanism'):
            return True
    return False


def test_every_connector_has_decided_whether_it_can_compile_a_statement():
    """
    A connector which declares no mechanism and stands in neither table is one nobody decided
    about, and it answers 'not run' for every statement of every run without anybody having
    said so.
    """
    undecided = []
    for name in connector_files():
        if name in SOURCE_TEST_MECHANISMS or name in SOURCE_TEST_WITHOUT_MECHANISM:
            continue
        undecided.append(name)
    assert not undecided, (
        'these connectors are in neither table of tests/test_query_source_test.py - add the '
        'mechanism they compile a statement with, or the reason they have none: '
        + ', '.join(undecided))


@pytest.mark.parametrize('name', sorted(SOURCE_TEST_MECHANISMS))
def test_a_connector_with_a_mechanism_really_declares_one(name):
    assert declares_a_mechanism(name), (
        f"{name} is listed as compiling a statement with "
        f"{SOURCE_TEST_MECHANISMS[name]} and declares no source_test_native_mechanism()")


@pytest.mark.parametrize('name', sorted(SOURCE_TEST_WITHOUT_MECHANISM))
def test_a_connector_without_a_mechanism_declares_none(name):
    """A reason which is no longer true is worse than no reason at all."""
    assert not declares_a_mechanism(name), (
        f"{name} declares a mechanism now - move it into SOURCE_TEST_MECHANISMS")


# --------------------------------------------------------------------------------------
# the run decides once, and says which of the two it was


def converter():
    from credativ_pg_migrator.query_conversion.workflow import QueryConverter

    made = QueryConverter.__new__(QueryConverter)
    made.config_parser = MagicMock()
    made.source_db_type = 'sybase_ase'
    made.target_db_type = 'postgresql'
    made.messages = []
    made.print_log_message = lambda level, message: made.messages.append((level, str(message)))
    return made


def test_a_run_with_the_source_test_off_does_not_connect_to_the_source():
    made = converter()
    made.config_parser.get_query_conversion_source_test.return_value = 'off'
    source = MagicMock()

    made.check_source_test(source)

    assert made.source_test[0] == 'off'
    assert not source.connect.called
    assert 'source test: not run' in made.describe_source_test()


def test_a_source_whose_connector_has_no_mechanism_is_said_once():
    """
    Once, and not once per statement: a warning repeated for every statement of a file is a
    warning which is scrolled past.
    """
    made = converter()
    made.config_parser.get_query_conversion_source_test.return_value = 'prepare'
    source = MagicMock()
    source.source_test_mechanism.return_value = None

    made.check_source_test(source)

    assert made.source_test[0] == 'off'
    assert 'no way of compiling a statement' in made.source_test[2]
    assert any(level == 'WARNING' for level, _message in made.messages)
    assert not source.connect.called


def test_a_source_which_cannot_be_reached_does_not_end_the_run():
    """
    The conversion and the target test do not need the source, so a source which is not
    reachable - the migration is finished and the server may be gone - costs the source test
    and nothing else. It is reported once, and every block then says 'not run' with the
    reason rather than 'FAILED'.
    """
    made = converter()
    made.config_parser.get_query_conversion_source_test.return_value = 'prepare'
    source = MagicMock()
    source.source_test_mechanism.return_value = 'SET NOEXEC ON'
    source.connect.side_effect = RuntimeError('connection refused')

    made.check_source_test(source)

    assert made.source_test[0] == 'off'
    assert 'could not be reached' in made.source_test[2]
    assert 'connection refused' in made.describe_source_test()


def test_a_run_which_can_compile_says_which_mechanism_it_uses():
    """
    What a green 'OK' proves depends on the mechanism which gave it, so the header of every
    output file names it.
    """
    made = converter()
    made.config_parser.get_query_conversion_source_test.return_value = 'prepare'
    source = MagicMock()
    source.source_test_mechanism.return_value = 'SET NOEXEC ON'

    made.check_source_test(source)

    assert made.source_test == ('prepare', 'SET NOEXEC ON', '')
    described = made.describe_source_test()
    assert 'SET NOEXEC ON' in described
    assert 'none of them is executed' in described


def test_a_statement_is_not_compiled_with_literals_in_the_place_of_its_parameters():
    """
    §8.1 tests the statement the application runs. A mechanism which submits a batch has no
    place for a marker, and putting a value there would compile a different statement - so it
    is reported as not tested, with the reason in the block.
    """
    made = converter()
    made.source_test = ('prepare', 'SET NOEXEC ON', '')
    source = MagicMock()
    source.source_test_parameter_style.return_value = None
    made.source_connection = lambda: source

    bind = types.SimpleNamespace(count=2, statement='SELECT id FROM t WHERE a = $1 AND b = $2')
    outcome, message = made.test_on_source(bind)

    assert outcome == 'not run'
    assert 'no place for a bind parameter' in message
    assert not source.test_query_on_source.called


def test_the_statement_reaches_the_source_in_the_marker_it_understands():
    made = converter()
    made.source_test = ('prepare', 'JDBC prepareStatement', '')
    source = MagicMock()
    source.source_test_parameter_style.return_value = 'qmark'
    source.test_query_on_source.return_value = ('OK', 'JDBC prepareStatement on the source')
    made.source_connection = lambda: source

    bind = types.SimpleNamespace(count=1, statement='SELECT id FROM t WHERE a = $1')
    assert made.test_on_source(bind)[0] == 'OK'
    sent = source.test_query_on_source.call_args[0][0]
    assert sent['query_code'] == 'SELECT id FROM t WHERE a = ?'
    assert sent['parameter_count'] == 1


def test_a_mechanism_which_throws_is_an_error_and_not_a_verdict():
    made = converter()
    made.source_test = ('prepare', 'EXPLAIN', '')
    source = MagicMock()
    source.source_test_parameter_style.return_value = None
    source.test_query_on_source.side_effect = RuntimeError('the driver fell over')
    made.source_connection = lambda: source

    outcome, message = made.test_on_source(types.SimpleNamespace(count=0, statement='SELECT 1'))
    assert outcome == 'ERROR'
    assert 'the driver fell over' in message


def test_the_block_of_a_statement_says_what_the_source_answered(tmp_path):
    from credativ_pg_migrator.query_conversion.splitter import Statement
    from credativ_pg_migrator.query_conversion.writer import OutputWriter, StatementResult

    statement = Statement(text='SELECT 1', ordinal=1, line_from=1, line_to=1,
                          input_file='queries.sql')
    result = StatementResult(statement, 1)
    result.source_test = ('FAILED', 'Invalid column name \'nope\'.')
    writer = OutputWriter({'directory': str(tmp_path), 'sidecar': 'off'}, lambda *args: None)

    block = writer.render_block(result)
    assert "-- source test: FAILED - Invalid column name 'nope'." in block


def test_the_summary_counts_the_statements_the_source_itself_refused():
    """
    The number a reader of a run needs: of the statements which were not converted, how many
    were broken before the migrator read them. Without it, twelve refusals read as twelve
    defects of the conversion.
    """
    from credativ_pg_migrator.query_conversion.splitter import Statement
    from credativ_pg_migrator.query_conversion.writer import (
        CONVERTED, NOT_CONVERTED, StatementResult, render_summary)

    def result_of(ordinal, status, source_test):
        made = StatementResult(
            Statement(text='SELECT 1', ordinal=ordinal, line_from=1, line_to=1,
                      input_file='queries.sql'), 3)
        made.status = status
        made.source_test = source_test
        return made

    results = [
        result_of(1, CONVERTED, ('OK', 'EXPLAIN on the source')),
        result_of(2, NOT_CONVERTED, ('FAILED', 'no such column: nope')),
        result_of(3, NOT_CONVERTED, ('not run', 'query_conversion.source_test is off')),
    ]
    summary = render_summary(results, {})

    assert '[ SOURCE TEST ]' in summary
    assert '2 statement(s) compiled against the source' in summary
    assert '1 of them the SOURCE itself refuses' in summary
    assert 'no such column: nope' in summary


def test_the_summary_says_nothing_about_a_source_test_which_did_not_run():
    """A heading with nothing under it is noise in a report which is read at the end of a run."""
    from credativ_pg_migrator.query_conversion.splitter import Statement
    from credativ_pg_migrator.query_conversion.writer import (
        CONVERTED, StatementResult, render_summary)

    made = StatementResult(
        Statement(text='SELECT 1', ordinal=1, line_from=1, line_to=1, input_file='queries.sql'), 1)
    made.status = CONVERTED
    summary = render_summary([made], {})
    assert '[ SOURCE TEST ]' not in summary


# --------------------------------------------------------------------------------------
# one connection per worker, not one per statement


def test_the_source_test_reuses_the_connection_it_already_has():
    """
    connect() opens a NEW connection in most connectors of this migrator and drops the
    reference to the old one. Called per statement, a file of five hundred queries would leave
    five hundred connections standing on a production source - so it is called only when there
    is none.
    """
    made = connector_of(SQLiteConnector, connection=MagicMock())
    for _ in range(3):
        made.test_query_on_source({'query_code': 'SELECT 1', 'parameter_count': 0})
    assert not made.connect.called


def test_a_connection_which_cannot_be_used_is_dropped_and_opened_again():
    made = connector_of(SQLiteConnector, connection=MagicMock())
    made.connection.cursor.side_effect = RuntimeError('connection already closed')

    outcome, message = made.test_query_on_source({'query_code': 'SELECT 1', 'parameter_count': 0})
    assert outcome == 'ERROR'
    assert 'connection already closed' in message
    assert made.connection is None

    ## the next statement makes a fresh one
    made.connect.side_effect = lambda: setattr(made, 'connection', MagicMock())
    assert made.test_query_on_source({'query_code': 'SELECT 1', 'parameter_count': 0})[0] == 'OK'
    assert made.connect.called


def test_a_statement_the_source_refuses_does_not_cost_the_connection():
    """A refusal is an answer about the statement. The connection is fine and is kept."""
    made = connector_of(SQLiteConnector, connection=MagicMock())
    made.connection.cursor.return_value.execute.side_effect = RuntimeError('no such column: nope')

    outcome, _message = made.test_query_on_source(
        {'query_code': 'SELECT nope FROM t', 'parameter_count': 0})
    assert outcome == 'FAILED'
    assert made.connection is not None


def test_a_statement_the_source_refuses_says_so_where_the_reader_looks():
    """
    The status of a statement says what became of the conversion, and a statement which
    converts cleanly is CONVERTED whatever the source thinks of it. Without a word in the
    block, a query the source itself refuses reads as one this step is answering for.
    """
    from credativ_pg_migrator.query_conversion.splitter import Statement

    made = converter()
    made.source_db_type = 'sqlite'
    made.source_schema = 'main'
    made.target_schema = 'migtest'
    made.name_map = None
    made.config_parser.get_query_conversion_parameter_style.return_value = 'auto'
    made.config_parser.get_query_conversion_parameter_output.return_value = 'original'
    made.config_parser.get_names_case_handling.return_value = 'lower'
    made.config_parser.convert_names_case = lambda name: (name or '').lower()
    made.config_parser.get_source_db_type.return_value = 'sqlite'

    source = MagicMock()
    source.prepare_query_for_parsing.side_effect = lambda text: text
    source.apply_remote_objects_substitution.side_effect = lambda text: (text, [])
    source.convert_query_code.side_effect = lambda settings: {
        'converted': True, 'code': settings['query_code'], 'warnings': []}
    source.source_test_parameter_style.return_value = 'qmark'
    source.test_query_on_source.return_value = ('FAILED', 'no such column: nope')
    made.source_connection = lambda: source
    made.source_test = ('prepare', 'EXPLAIN', '')
    made.test_on_target = lambda sql, has_parameters: ('not run', 'off', None)

    result = made.convert_statement(
        Statement(text='SELECT nope FROM customers', ordinal=1, line_from=1, line_to=1,
                  input_file='queries.sql'), 1)

    assert result.source_test == ('FAILED', 'no such column: nope')
    assert any('the SOURCE refuses this statement as well' in warning
               for warning in result.warnings)
