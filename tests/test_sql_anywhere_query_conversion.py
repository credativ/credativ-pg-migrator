# SPDX-License-Identifier: GPL-3.0-or-later
"""
The query conversion of SAP SQL Anywhere.

It stands in `connectors/sql_anywhere_query_conversion.py` and not in the connector, because
the connector imports `sqlanydb` - a driver installed by whoever migrates a SQL Anywhere
database, not a dependency of this migrator. These tests therefore need no SQL Anywhere
client, exactly as the ones of Db2 and Oracle need no client of theirs.

SQL Anywhere is read as T-SQL, which reads most of a statement correctly and a handful of
things wrongly - and the wrong ones are wrong in the worst way, because the statement stays
valid. `LOCATE(email, '@')` searches the second argument in the first and a T-SQL parser reads
it the other way round: the converted statement answers 0 for every row and nothing about it
looks broken. `CAST(x AS TIMESTAMP)` is a date and a time there and the row version of a table
in T-SQL, which comes out as `CAST(x AS BYTEA)`.

Run with:  python3 -m pytest tests/test_sql_anywhere_query_conversion.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.connectors.sql_anywhere_query_conversion import (
    SqlAnywhereQueryConversion, SQL_ANYWHERE_FUNCTION_MAPPING)
from credativ_pg_migrator.database_connector import DatabaseConnector
from credativ_pg_migrator.query_conversion import classifier, parameters


class RecordingLog:
    args = None

    def __init__(self):
        self.messages = []

    def print_log_message(self, level, message):
        self.messages.append((level, str(message)))

    def get_target_db_type(self):
        return 'postgresql'


class SqlAnywhereConversion(SqlAnywhereQueryConversion, DatabaseConnector):
    """The conversion with the base connector behind it and no driver in front of it."""

    def __init__(self):
        self.config_parser = RecordingLog()


## the methods a migration needs are declared abstract by the base connector and none of them
## is used here: the conversion is a transformation of text
SqlAnywhereConversion.__abstractmethods__ = frozenset()


@pytest.fixture
def sql_anywhere():
    return SqlAnywhereConversion()


def converted(sql_anywhere, statement, source_schema='DBA'):
    return sql_anywhere.convert_query_code({
        'query_code': statement, 'source_schema_name': source_schema,
        'target_schema_name': 'public', 'target_db_type': 'postgresql',
        'statement_id': 'test:1'})


def code_of(sql_anywhere, statement):
    answer = converted(sql_anywhere, statement)
    assert answer['converted'] is True, answer['error']
    return answer['code']


def expression_of(sql_anywhere, expression):
    """What the SELECT list of a converted statement holds - the expression alone."""
    code = code_of(sql_anywhere, f"SELECT {expression} FROM t")
    return code[len('SELECT '):].split(' FROM t')[0]


def parses(text):
    """Whether the statement can be read at all once it has been prepared."""
    return classifier.classify(text, 'sql_anywhere', parse_text=text).is_select


# --------------------------------------------------------------------------------------
# what a T-SQL parser cannot read at all


@pytest.mark.parametrize('statement', [
    "SELECT c.id, o.id FROM customers c, orders o WHERE c.id *= o.cid",
    "SELECT TOP 20 START AT 101 o.id FROM orders o ORDER BY o.d DESC",
    "SELECT STRING(a, ', ', b) AS full_name FROM t",
    "SELECT IF a > 1 THEN 'y' ELSE 'n' ENDIF AS f FROM t",
])
def test_a_statement_of_sql_anywhere_is_a_read_once_it_is_prepared(sql_anywhere, statement):
    """Without the preparation every one of these is 'the migrator cannot read this'."""
    assert not parses(statement), \
        'this statement would parse without the preparation - it does not belong here'
    assert parses(sql_anywhere.prepare_query_for_parsing(statement))


@pytest.mark.parametrize('statement', [
    "UPDATE customers SET last_login = NOW(*) WHERE id = 1",
    "SELECT TOP 10 START AT 5 a, b INTO work_table FROM t",
    "SELECT a FROM t WHERE b = 1 FOR UPDATE",
])
def test_the_preparation_does_not_make_a_write_look_like_a_read(sql_anywhere, statement):
    """The gates read the text of the application, and the preparation changes none of that."""
    result = classifier.classify(
        statement, 'sql_anywhere',
        parse_text=sql_anywhere.prepare_query_for_parsing(statement))
    assert result.verdict == 'refused', result.reason


def test_the_paging_of_sql_anywhere_is_moved_to_the_end(sql_anywhere):
    """
    TOP is read as the T-SQL one and becomes a LIMIT on its own; only the START AT has to be
    moved. SQL Anywhere counts its first row as 1 and PostgreSQL counts the rows it skips.
    """
    code = code_of(sql_anywhere, "SELECT TOP 5 START AT 11 a FROM t ORDER BY a")
    assert code.endswith('LIMIT 5 OFFSET 10')


def test_a_start_at_of_one_skips_nothing(sql_anywhere):
    assert 'OFFSET' not in code_of(sql_anywhere, "SELECT TOP 5 START AT 1 a FROM t")


@pytest.mark.parametrize('written,expected', [
    ("IF a > 1 THEN 'y' ELSE 'n' ENDIF", "CASE WHEN a > 1 THEN 'y' ELSE 'n' END"),
    ("IF a > 1 THEN 'y' ENDIF", "CASE WHEN a > 1 THEN 'y' END"),
])
def test_the_if_expression_becomes_a_case(sql_anywhere, written, expected):
    """IF is an expression in SQL Anywhere and the beginning of a statement everywhere else."""
    assert expression_of(sql_anywhere, f"{written} AS f").startswith(expected)


def test_an_if_inside_a_literal_is_text(sql_anywhere):
    statement = "SELECT a FROM t WHERE b = 'IF x THEN y ENDIF'"
    assert code_of(sql_anywhere, statement) == statement


def test_string_concatenates_and_is_not_a_cast(sql_anywhere):
    """
    STRING() of SQL Anywhere concatenates and skips a NULL argument, which is what concat()
    of PostgreSQL does. A T-SQL parser reads STRING as the cast of a single value and stops
    at the second argument.
    """
    assert expression_of(sql_anywhere, "STRING(a, ', ', b)") == "CONCAT(a, ', ', b)"


def test_the_count_of_no_column_is_the_count_of_the_rows(sql_anywhere):
    assert expression_of(sql_anywhere, "COUNT()") == "COUNT(*)"


@pytest.mark.parametrize('written,expected', [
    ("NOW(*)", "CURRENT_TIMESTAMP"),
    ("TODAY(*)", "CURRENT_DATE"),
])
def test_the_pseudo_functions_written_with_a_star(sql_anywhere, written, expected):
    assert expression_of(sql_anywhere, written) == expected


# --------------------------------------------------------------------------------------
# the outer join, which may become a LEFT JOIN and may never become an inner one


def test_the_outer_join_of_the_watcom_family_becomes_a_left_join(sql_anywhere):
    code = code_of(sql_anywhere, "SELECT c.id, o.id FROM customers c, orders o "
                                 "WHERE c.id *= o.cid")
    assert 'LEFT JOIN orders AS o ON c.id = o.cid' in code
    assert '*=' not in code and 'left_outer' not in code
    ## the condition was taken out of the WHERE clause, and no 'WHERE TRUE' is left behind
    assert 'WHERE' not in code


def test_an_outer_join_condition_under_an_or_is_refused(sql_anywhere):
    """
    Moving it into the ON clause makes it an AND of the join and leaves the other side of the
    OR behind: the statement stays valid and answers other rows.
    """
    answer = converted(sql_anywhere, "SELECT c.id FROM customers c, orders o "
                                     "WHERE c.id *= o.cid OR o.st *= 1")
    assert answer['converted'] is False
    assert 'outer join' in answer['error']
    assert answer['code'] == ''


# --------------------------------------------------------------------------------------
# what the parser reads and answers with something else


def test_locate_searches_the_second_argument_in_the_first(sql_anywhere):
    """
    This is the one which matters most. `LOCATE(email, '@')` looks for '@' in the address;
    POSITION of the standard is written the other way round, and a T-SQL parser reads the two
    as if they were the same - the converted statement is valid and answers 0 for every row.
    """
    assert expression_of(sql_anywhere, "LOCATE(c.email, '@')") == "POSITION('@' IN c.email)"


def test_locate_with_a_starting_position_is_refused(sql_anywhere):
    """The third argument may count from the end of the string, which POSITION cannot do."""
    answer = converted(sql_anywhere, "SELECT LOCATE(a, '@', 3) FROM t")
    assert answer['converted'] is False
    assert 'LOCATE' in answer['error']


def test_the_timestamp_of_sql_anywhere_is_a_date_and_a_time(sql_anywhere):
    """
    TIMESTAMP is a date and a time in SQL Anywhere and the row version of a table in T-SQL,
    which is a string of bytes - the cast came out as CAST(x AS BYTEA).
    """
    assert expression_of(sql_anywhere, "CAST(a AS TIMESTAMP)") == "CAST(a AS TIMESTAMP)"


def test_another_cast_is_left_alone(sql_anywhere):
    assert expression_of(sql_anywhere, "CAST(a AS varchar(20))") == "CAST(a AS VARCHAR(20))"


@pytest.mark.parametrize('written,expected', [
    ("DATEFORMAT(d, 'YYYY-MM-DD')", "TO_CHAR(d, 'YYYY-MM-DD')"),
    ("DATEFORMAT(d, 'HH:NN:SS')", "TO_CHAR(d, 'HH24:MI:SS')"),
    ("DATEFORMAT(d, 'MMM YYYY')", "TO_CHAR(d, 'Mon YYYY')"),
])
def test_the_format_of_dateformat_is_written_code_by_code(sql_anywhere, written, expected):
    """
    The hour of a twenty-four hour clock is 'HH' in SQL Anywhere and 'HH24' in PostgreSQL,
    and the minute is 'NN' there and 'MI' here. A format handed over unchanged answers
    another string, and the letters 'NN' as themselves.
    """
    assert expression_of(sql_anywhere, written) == expected


@pytest.mark.parametrize('written', [
    "DATEFORMAT(d, 'YYYY-Www')",
    "DATEFORMAT(d, x)",
])
def test_a_format_which_cannot_be_written_stops_the_conversion(sql_anywhere, written):
    answer = converted(sql_anywhere, f"SELECT {written} FROM t")
    assert answer['converted'] is False
    assert 'DATEFORMAT' in answer['error']


@pytest.mark.parametrize('written,expected', [
    ("LIST(a)", "STRING_AGG(CAST(a AS TEXT), ',')"),
    ("LIST(a, '; ')", "STRING_AGG(CAST(a AS TEXT), '; ')"),
])
def test_list_becomes_string_agg(sql_anywhere, written, expected):
    """The separator of LIST is a comma when it is not given; string_agg() needs one."""
    assert expression_of(sql_anywhere, written) == expected


# --------------------------------------------------------------------------------------
# what PostgreSQL has nothing for is not converted


@pytest.mark.parametrize('statement,expected_reason', [
    ("SELECT NUMBER(*) AS n, a FROM t", 'NUMBER(*)'),
    ("SELECT DB_PROPERTY('Name') FROM t", 'no counterpart'),
    ("SELECT CONNECTION_PROPERTY('Name') FROM t", 'no counterpart'),
    ("SELECT UUIDTOSTR(a) FROM t", 'no counterpart'),
])
def test_a_construct_without_a_counterpart_stops_the_conversion(sql_anywhere, statement,
                                                                expected_reason):
    answer = converted(sql_anywhere, statement)
    assert answer['converted'] is False
    assert answer['code'] == ''
    assert expected_reason in answer['error']


@pytest.mark.parametrize('statement', [
    "SELECT a AS number_of_rows FROM t",
    "SELECT a FROM t WHERE note = 'NUMBER(*) and DB_PROPERTY()'",
    "SELECT /* LIST() is named here */ a FROM t",
])
def test_a_name_which_reads_like_a_construct_does_not_stop_the_conversion(sql_anywhere,
                                                                          statement):
    assert converted(sql_anywhere, statement)['converted'] is True


# --------------------------------------------------------------------------------------
# what is converted and still means something else afterwards


def test_a_concatenation_written_with_a_plus_is_reported(sql_anywhere):
    """'+' concatenates strings in SQL Anywhere and adds numbers in PostgreSQL."""
    answer = converted(sql_anywhere, "SELECT a + 'x' FROM t")
    assert answer['converted'] is True
    assert any("'||'" in warning for warning in answer['warnings'])


@pytest.mark.parametrize('statement', [
    "SELECT a + b FROM t",
    "SELECT 1 + 2 FROM t",
    "SELECT a FROM t WHERE note = 'write a + b here'",
])
def test_an_addition_which_may_be_one_is_not_reported(sql_anywhere, statement):
    """
    Two columns may be numbers, and the '+' in the comment above a statement is a word. A
    warning which fires on either is a warning nobody reads.
    """
    assert converted(sql_anywhere, statement)['warnings'] == []


def test_the_xml_type_is_reported(sql_anywhere):
    answer = converted(sql_anywhere, "SELECT CAST(a AS XML) FROM t")
    assert any('XML' in warning for warning in answer['warnings'])


# --------------------------------------------------------------------------------------
# the entry point


def test_the_conversion_answers_with_the_statement(sql_anywhere):
    answer = converted(sql_anywhere, "SELECT c.id FROM customers c WHERE c.status = 'A'")
    assert answer['converted'] is True
    assert answer['error'] is None
    assert 'customers' in answer['code']


def test_the_schema_of_the_source_is_taken_out(sql_anywhere):
    """
    SQL Anywhere writes the owner in front of every name of its catalog; the migrated objects
    are reached through the search_path of the target.
    """
    code = code_of(sql_anywhere, 'SELECT a FROM "DBA".customers')
    assert '"DBA"' not in code and 'customers' in code


def test_a_statement_which_could_not_be_parsed_is_not_offered_as_converted(sql_anywhere):
    answer = converted(sql_anywhere, "SELECT FROM WHERE ((")
    assert answer['converted'] is False
    assert answer['code'] == ''
    assert 'could not be parsed' in answer['error']


def test_a_conversion_which_produced_nothing_is_not_a_conversion(sql_anywhere, monkeypatch):
    monkeypatch.setattr(SqlAnywhereConversion, 'convert_statement_with_report',
                        lambda self, settings: ('  ', {'unconverted_joins': 0, 'notes': [],
                                                       'warnings': []}))
    answer = converted(sql_anywhere, "SELECT a FROM t")
    assert answer['converted'] is False
    assert 'no statement at all' in answer['error']


def test_an_error_of_any_other_kind_is_reported_as_one(sql_anywhere, monkeypatch):
    monkeypatch.setattr(SqlAnywhereConversion, 'convert_statement_with_report',
                        lambda self, settings: (_ for _ in ()).throw(RuntimeError('boom')))
    answer = converted(sql_anywhere, "SELECT a FROM t")
    assert answer['converted'] is False
    assert 'boom' in answer['error']


def test_the_connector_answers_that_it_can_do_it(sql_anywhere):
    assert sql_anywhere.query_conversion_supported() is True


# --------------------------------------------------------------------------------------
# the function mapping, which carried six entries that were defects


def test_a_type_named_timestamp_is_not_the_current_timestamp():
    """
    'timestamp' was mapped as a whole word, so `CAST(a AS timestamp)` became
    `CAST(a AS CURRENT_TIMESTAMP)` - in a view of the migration as well as here.
    """
    assert 'timestamp' not in SQL_ANYWHERE_FUNCTION_MAPPING
    assert 'user' not in SQL_ANYWHERE_FUNCTION_MAPPING


def test_a_table_named_user_keeps_its_name(sql_anywhere):
    assert '"user"' in code_of(sql_anywhere, 'SELECT u.n FROM "user" u')


def test_the_two_word_spellings_are_still_converted():
    """'CURRENT TIMESTAMP' written with a space is the SQL Anywhere spelling of it."""
    assert SQL_ANYWHERE_FUNCTION_MAPPING['current timestamp'] == 'CURRENT_TIMESTAMP'
    assert SQL_ANYWHERE_FUNCTION_MAPPING['last user'] == 'CURRENT_USER'


def test_the_renames_which_produced_invalid_sql_are_gone():
    """
    'locate(' and 'charindex(' were renamed to 'position(', whose arguments are written with
    IN between them - `position(a, '@')` is not valid PostgreSQL at all. 'dateformat(' was
    renamed to 'to_char(' with its format handed over unchanged.
    """
    for key in ('locate(', 'charindex(', 'dateformat(', 'string('):
        assert key not in SQL_ANYWHERE_FUNCTION_MAPPING


# --------------------------------------------------------------------------------------
# the view path


def test_the_view_is_written_as_the_statement_which_creates_it(sql_anywhere):
    ddl = sql_anywhere.convert_view_code({
        'view_code': "CREATE VIEW v AS SELECT a FROM t", 'source_schema_name': 'DBA',
        'target_schema_name': 'public', 'target_db_type': 'postgresql'})
    assert ddl.upper().startswith('CREATE OR REPLACE VIEW')


def test_a_view_which_could_not_be_parsed_is_converted_as_far_as_it_can_be(sql_anywhere):
    """
    Which is what the whole conversion of a view was before it was given a parser: the
    rewrites which need none. The migration then creates the view or reports it as failed.
    """
    ddl = sql_anywhere.convert_view_code({
        'view_code': "SELECT FROM WHERE ((", 'source_schema_name': 'DBA',
        'target_schema_name': 'public', 'target_db_type': 'postgresql'})
    assert 'SELECT FROM WHERE ((' in ddl
    assert any('could not be parsed' in message
               for _level, message in sql_anywhere.config_parser.messages)


def test_the_view_and_the_statement_are_given_the_same_conversion(sql_anywhere):
    settings = {'view_code': "SELECT LOCATE(a, '@') FROM t", 'source_schema_name': 'DBA',
                'target_schema_name': 'public', 'target_db_type': 'postgresql'}
    assert "POSITION('@' IN a)" in sql_anywhere.convert_view_code(dict(settings))
    assert "POSITION('@' IN a)" in sql_anywhere.convert_statement_code(dict(settings))


# --------------------------------------------------------------------------------------
# the bind parameters survive all of it


def test_a_bind_parameter_is_carried_through_the_conversion(sql_anywhere):
    statement = "SELECT TOP 10 START AT 21 a FROM t WHERE b = ? AND c = ? ORDER BY a"
    bind_parameters, _warnings = parameters.extract(statement, 'qmark')
    answer = converted(sql_anywhere, bind_parameters.conversion_statement)
    assert answer['converted'] is True
    restored = bind_parameters.to_numbered(answer['code'])
    assert '$1' in restored and '$2' in restored
    assert restored.endswith('LIMIT 10 OFFSET 20')
