# SPDX-License-Identifier: GPL-3.0-or-later
"""
The query conversion of Oracle.

It stands in `connectors/oracle_query_conversion.py` and not in the connector, because the
connector imports `oracledb` - which is not a dependency of this migrator, it is installed by
whoever migrates an Oracle database. These tests therefore need no Oracle client, exactly as
the ones of Db2 need no `ibm_db`.

sqlglot reads Oracle, so the transpilation itself is not what is asserted here. What is
asserted is the line around it, and with Oracle that line is long: the hierarchical query,
ROWNUM, ROWID, a name over a database link and the functions PostgreSQL has nothing for stop
the conversion; `TRUNC(d, 'MM')` and `ADD_MONTHS` are written as something PostgreSQL really
has; and the `(+)` outer join is either a LEFT JOIN or a reported failure - never an inner
join, which answers fewer rows and looks healthy while doing it.

Run with:  python3 -m pytest tests/test_oracle_query_conversion.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.connectors.oracle_query_conversion import OracleQueryConversion
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


class OracleConversion(OracleQueryConversion, DatabaseConnector):
    """
    The conversion with the base connector behind it and no Oracle client in front of it -
    which is what the module is for.
    """

    def __init__(self):
        self.config_parser = RecordingLog()


## the methods a migration needs are declared abstract by the base connector and none of them
## is used here: the conversion is a transformation of text
OracleConversion.__abstractmethods__ = frozenset()


@pytest.fixture
def oracle():
    return OracleConversion()


def converted(oracle, statement, source_schema=''):
    return oracle.convert_query_code({
        'query_code': statement, 'source_schema_name': source_schema,
        'target_schema_name': 'public', 'target_db_type': 'postgresql',
        'statement_id': 'test:1'})


def code_of(oracle, statement):
    answer = converted(oracle, statement)
    assert answer['converted'] is True, answer['error']
    return answer['code']


def expression_of(oracle, expression):
    """What the SELECT list of a converted statement holds - the expression alone."""
    return code_of(oracle, f"SELECT {expression} FROM t")[len('SELECT '):].split(' FROM t')[0]


# --------------------------------------------------------------------------------------
# the (+) outer join, which may become a LEFT JOIN and may never become an inner one


def test_the_outer_join_of_oracle_becomes_a_left_join(oracle):
    code = code_of(oracle, "SELECT c.id, o.id FROM customers c, orders o "
                           "WHERE c.id = o.cid(+)")
    assert 'LEFT JOIN orders AS o ON c.id = o.cid' in code
    assert '(+)' not in code


def test_two_conditions_of_one_outer_join_are_both_moved(oracle):
    code = code_of(oracle, "SELECT c.id FROM customers c, orders o "
                           "WHERE c.id = o.cid(+) AND o.st(+) = 1")
    assert 'LEFT JOIN orders AS o ON c.id = o.cid AND o.st = 1' in code


def test_an_outer_join_condition_under_an_or_is_refused(oracle):
    """
    Moving it into the ON clause makes it an AND of the join and leaves the other side of
    the OR behind - the statement stays valid and answers other rows, which is the failure
    this whole step exists to prevent.
    """
    answer = converted(oracle, "SELECT c.id FROM customers c, orders o "
                               "WHERE c.id = o.cid(+) OR o.st(+) = 1")
    assert answer['converted'] is False
    assert 'outer join' in answer['error']


def test_an_outer_join_marked_inside_a_call_is_refused(oracle):
    """
    'UPPER(o.cid(+))' is not the shape the marking recognises. The parser drops the (+)
    without a word, and the join it belonged to is an inner join from then on - so the
    statement is counted as unconverted instead.
    """
    answer = converted(oracle, "SELECT c.id FROM customers c, orders o "
                               "WHERE UPPER(c.id) = UPPER(o.cid(+))")
    assert answer['converted'] is False
    assert 'outer join' in answer['error']


def test_a_plus_sign_inside_a_literal_is_not_an_outer_join(oracle):
    assert converted(oracle, "SELECT a FROM t WHERE note = 'a (+) sign'")['converted'] is True


# --------------------------------------------------------------------------------------
# ROWNUM, which is a limit only where the query block does not sort


def test_the_paging_idiom_of_oracle_becomes_a_limit(oracle):
    """
    'SELECT * FROM (SELECT ... ORDER BY x) WHERE ROWNUM <= 20' is how an Oracle application
    asks for the top twenty: the ORDER BY stands in the subquery, so the rows are sorted
    before they are numbered and LIMIT answers the same ones.
    """
    code = code_of(oracle, "SELECT * FROM (SELECT a FROM t ORDER BY a DESC) WHERE ROWNUM <= 20")
    assert code.endswith('LIMIT 20')
    assert 'ROWNUM' not in code
    assert 'ORDER BY a DESC' in code


@pytest.mark.parametrize('condition,expected_limit', [
    ('ROWNUM <= 10', 'LIMIT 10'),
    ('ROWNUM < 5', 'LIMIT 4'),
    ('ROWNUM = 1', 'LIMIT 1'),
])
def test_a_rownum_bound_without_an_order_by_is_a_limit(oracle, condition, expected_limit):
    assert code_of(oracle, f"SELECT a FROM t WHERE {condition}").endswith(expected_limit)


def test_the_other_conditions_of_the_where_clause_stay(oracle):
    code = code_of(oracle, "SELECT a FROM t WHERE b = 1 AND ROWNUM <= 10")
    assert 'WHERE b = 1' in code and code.endswith('LIMIT 10')


def test_a_rownum_together_with_an_order_by_is_refused(oracle):
    """
    ROWNUM is given to a row while the rows are read and LIMIT is applied after they are
    sorted, so this statement and its conversion answer another ten rows each.
    """
    answer = converted(oracle, "SELECT a FROM t WHERE ROWNUM <= 10 ORDER BY a")
    assert answer['converted'] is False
    assert 'ROWNUM' in answer['error']


@pytest.mark.parametrize('statement', [
    "SELECT ROWNUM, a FROM t",
    "SELECT a FROM t WHERE b = 1 OR ROWNUM <= 10",
    "SELECT a FROM t WHERE ROWNUM = 2",
])
def test_a_rownum_which_is_not_a_limit_is_refused(oracle, statement):
    """
    In the select list it numbers the rows, under an OR it does not bound them, and
    'ROWNUM = 2' answers no row at all - the second row only exists once the first was
    given a number.
    """
    assert converted(oracle, statement)['converted'] is False


# --------------------------------------------------------------------------------------
# what the transpiler writes as a name PostgreSQL does not have


@pytest.mark.parametrize('written,expected', [
    ("TRUNC(d, 'MM')", "DATE_TRUNC('MONTH', d)"),
    ("TRUNC(d, 'YYYY')", "DATE_TRUNC('YEAR', d)"),
    ("TRUNC(d, 'IW')", "DATE_TRUNC('WEEK', d)"),
    ("TRUNC(d, 'HH24')", "DATE_TRUNC('HOUR', d)"),
])
def test_the_format_model_of_trunc_becomes_a_field_of_postgresql(oracle, written, expected):
    """
    The transpiler writes the format model of Oracle where PostgreSQL expects the name of a
    field: DATE_TRUNC('MM', d) is refused with "timestamp units MM not recognized".
    """
    assert expression_of(oracle, written) == expected


def test_a_format_model_which_truncates_another_way_is_refused(oracle):
    """'DAY' begins the week on Sunday in Oracle and date_trunc('week') begins it on Monday."""
    answer = converted(oracle, "SELECT TRUNC(d, 'DAY') FROM t")
    assert answer['converted'] is False
    assert "'DAY'" in answer['error']


def test_rounding_a_date_is_refused(oracle):
    """ROUND(d, 'MM') can move a date forwards, which no date_trunc() does."""
    answer = converted(oracle, "SELECT ROUND(d, 'MM') FROM t")
    assert answer['converted'] is False
    assert 'rounds a date' in answer['error']


def test_add_months_becomes_an_interval(oracle):
    """
    Adding an interval of months is the same thing in PostgreSQL, down to the last day of a
    month: the 31st of January plus one month is the 28th of February in both.
    """
    assert expression_of(oracle, "ADD_MONTHS(d, 3)") == "(d + (3) * INTERVAL '1 MONTH')"


def test_add_months_of_a_negative_number_is_a_subtraction(oracle):
    assert "-12" in expression_of(oracle, "ADD_MONTHS(SYSDATE, -12)")


# --------------------------------------------------------------------------------------
# what the transpiler already writes correctly


@pytest.mark.parametrize('written,expected', [
    ("NVL(a, 'x')", "COALESCE(a, 'x')"),
    ("NVL2(a, b, c)", "CASE WHEN NOT a IS NULL THEN b ELSE c END"),
    ("DECODE(s, 1, 'a', 'b')", "CASE WHEN s = 1 THEN 'a' ELSE 'b' END"),
    ("SUBSTR(a, 1, 3)", "SUBSTRING(a FROM 1 FOR 3)"),
    ("INSTR(a, 'x')", "POSITION('x' IN a)"),
    ("LAST_DAY(d)", "CAST(DATE_TRUNC('MONTH', d) + INTERVAL '1 MONTH' - INTERVAL '1 DAY' AS DATE)"),
    ("TO_CHAR(d, 'YYYY')", "TO_CHAR(d, 'YYYY')"),
    ("GROUPING_ID(a, b)", "grouping(a, b)"),
    ("LENGTHB(a)", "octet_length(a)"),
])
def test_what_the_transpiler_writes_correctly_is_left_alone(oracle, written, expected):
    assert expression_of(oracle, written) == expected


def test_the_one_row_table_of_oracle_is_left_out(oracle):
    """PostgreSQL needs no table for a SELECT which reads none."""
    assert code_of(oracle, "SELECT SYSDATE FROM dual") == "SELECT CURRENT_TIMESTAMP"


def test_listagg_becomes_string_agg(oracle):
    code = code_of(oracle, "SELECT LISTAGG(s, ',') WITHIN GROUP (ORDER BY d) FROM t")
    assert "STRING_AGG(s, ',' ORDER BY d)" in code


def test_the_schema_of_the_source_is_replaced(oracle):
    """Oracle stores an unquoted name in upper case, so the source schema is written that way."""
    answer = converted(oracle, 'SELECT a FROM "MIGTEST".customers', source_schema='MIGTEST')
    assert '"public".customers' in answer['code']


# --------------------------------------------------------------------------------------
# what PostgreSQL has nothing for is not converted


@pytest.mark.parametrize('statement,expected_reason', [
    ("SELECT e.id FROM emp e START WITH e.mgr IS NULL CONNECT BY PRIOR e.id = e.mgr",
     'hierarchical query'),
    ("SELECT ROWID, a FROM t", 'ROWID'),
    ("SELECT COUNT(*) FROM orders@remote_erp", 'database link'),
    ("SELECT MONTHS_BETWEEN(a, b) FROM t", 'no counterpart'),
    ("SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') FROM dual", 'no counterpart'),
    ("SELECT RATIO_TO_REPORT(a) OVER () FROM t", 'no counterpart'),
    ("SELECT DBMS_LOB.SUBSTR(a, 10) FROM t", 'package of Oracle'),
    ("SELECT a FROM t SAMPLE (10)", 'MULTISET'),
])
def test_a_construct_without_a_counterpart_stops_the_conversion(oracle, statement,
                                                                expected_reason):
    answer = converted(oracle, statement)
    assert answer['converted'] is False
    assert answer['code'] == ''
    assert expected_reason in answer['error']


@pytest.mark.parametrize('statement', [
    "SELECT a AS rowid_of_source FROM t",
    "SELECT a FROM t WHERE note = 'ROWNUM and CONNECT BY are Oracle'",
    "SELECT a FROM t WHERE email = 'someone@example.com'",
])
def test_a_name_which_reads_like_a_construct_does_not_stop_the_conversion(oracle, statement):
    """A construct named in a literal, or a word which only begins like one, is not one."""
    assert converted(oracle, statement)['converted'] is True


# --------------------------------------------------------------------------------------
# what is converted and still means something else afterwards


@pytest.mark.parametrize('statement,expected_warning', [
    ("SELECT a FROM t WHERE notes = ''", 'empty string is NULL'),
    ("SELECT SYSDATE - d AS age FROM t", 'counts days'),
    ("SELECT d + 7 FROM t WHERE d > SYSDATE - 30", 'counts days'),
    ('SELECT "CUSTOMER_ID" FROM "CUSTOMERS"', 'upper case'),
    ("SELECT /*+ INDEX(t idx) */ a FROM t", 'optimizer hint'),
    ("SELECT TRUNC(d) FROM t", 'one argument'),
])
def test_what_still_means_something_else_is_reported(oracle, statement, expected_warning):
    answer = converted(oracle, statement)
    assert any(expected_warning in warning for warning in answer['warnings']), answer['warnings']


def test_a_lower_case_quoted_identifier_is_not_warned_about(oracle):
    answer = converted(oracle, 'SELECT "customer_id" FROM "customers"')
    assert answer['warnings'] == []


# --------------------------------------------------------------------------------------
# the entry point


def test_the_conversion_answers_with_the_statement(oracle):
    answer = converted(oracle, "SELECT c.id FROM customers c WHERE c.status = :status")
    assert answer['converted'] is True
    assert answer['error'] is None
    assert 'customers' in answer['code']


def test_a_statement_which_could_not_be_parsed_is_not_offered_as_converted(oracle):
    """
    The conversion of a view answers an unparsable statement with the text of the source, so
    that the migration can report the view and keep its code readable. A query of an
    application may not be answered that way - it would be handed back in the dialect of the
    source as if it had been converted.
    """
    answer = converted(oracle, "SELECT FROM WHERE ((")
    assert answer['converted'] is False
    assert answer['code'] == ''
    assert 'could not be parsed' in answer['error']


def test_a_conversion_which_produced_nothing_is_not_a_conversion(oracle, monkeypatch):
    monkeypatch.setattr(OracleConversion, 'convert_statement_with_report',
                        lambda self, settings: ('  ', {'unconverted_joins': 0, 'notes': []}))
    answer = converted(oracle, "SELECT a FROM t")
    assert answer['converted'] is False
    assert 'no statement at all' in answer['error']


def test_an_error_of_any_other_kind_is_reported_as_one(oracle, monkeypatch):
    monkeypatch.setattr(OracleConversion, 'convert_statement_with_report',
                        lambda self, settings: (_ for _ in ()).throw(RuntimeError('boom')))
    answer = converted(oracle, "SELECT a FROM t")
    assert answer['converted'] is False
    assert 'boom' in answer['error']


def test_the_warnings_are_reported_even_when_the_conversion_failed(oracle):
    answer = converted(oracle, "SELECT ROWID FROM t WHERE notes = ''")
    assert answer['converted'] is False
    assert any('empty string is NULL' in warning for warning in answer['warnings'])


def test_the_connector_answers_that_it_can_do_it(oracle):
    assert oracle.query_conversion_supported() is True


# --------------------------------------------------------------------------------------
# the view path keeps the behaviour it had


def test_the_view_is_wrapped_into_the_statement_which_creates_it(oracle):
    ddl = oracle.convert_view_code({
        'view_code': "SELECT a FROM t", 'source_schema_name': 'MIGTEST',
        'target_schema_name': 'public', 'target_db_type': 'postgresql',
        'target_view_name': 'v', 'view_type': 'VIEW'})
    assert ddl.startswith('CREATE VIEW "public"."v" AS SELECT a FROM t')
    assert ddl.endswith(';')


def test_a_view_which_could_not_be_parsed_keeps_the_text_of_the_source(oracle):
    """
    The migration reports such a view as failed and its source code stays readable in the
    protocol - which is what it did before the converter was lifted out. What changed is who
    decides: the caller does, and the query conversion decides differently.
    """
    ddl = oracle.convert_view_code({
        'view_code': "SELECT FROM WHERE ((", 'source_schema_name': 'MIGTEST',
        'target_schema_name': 'public', 'target_db_type': 'postgresql',
        'target_view_name': 'v', 'view_type': 'VIEW'})
    assert 'SELECT FROM WHERE ((' in ddl
    assert any('failed' in message for _level, message in oracle.config_parser.messages)


def test_a_view_is_told_what_the_conversion_could_not_write(oracle):
    """The query is refused for it; the view is created and the reason stands in the log."""
    oracle.convert_view_code({
        'view_code': "SELECT TRUNC(d, 'DAY') FROM t", 'source_schema_name': '',
        'target_schema_name': 'public', 'target_db_type': 'postgresql',
        'target_view_name': 'v', 'view_type': 'VIEW'})
    assert any("'DAY'" in message for _level, message in oracle.config_parser.messages)


# --------------------------------------------------------------------------------------
# the bind parameters survive all of it


def test_a_bind_parameter_is_carried_through_the_conversion(oracle):
    statement = ("SELECT a FROM t WHERE b = :name AND d >= TO_DATE(:from_date, 'YYYY-MM-DD') "
                 "AND e = :name")
    bind_parameters, _warnings = parameters.extract(statement, 'named')
    answer = converted(oracle, bind_parameters.conversion_statement)
    assert answer['converted'] is True
    restored = bind_parameters.to_numbered(answer['code'])
    ## the same name twice is one parameter, as it is for the driver which binds it
    assert restored.count('$1') == 2 and '$2' in restored
    assert 'cpgm_bind_param' not in restored


def test_a_statement_of_oracle_is_read_as_a_read(oracle):
    """The gates decide that before the conversion is asked at all."""
    statement = "SELECT c.id FROM customers c, orders o WHERE c.id = o.cid(+)"
    assert classifier.classify(statement, 'oracle').is_select
