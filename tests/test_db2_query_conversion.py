# SPDX-License-Identifier: GPL-3.0-or-later
"""
The query conversion of the three Db2 flavours - LUW, for i and for z/OS.

They are three connectors and one SQL dialect, so the part which reads that dialect stands
once, in connectors/db2_query_conversion.py, and these tests exercise it there. What each
flavour adds on top - the system names of Db2 for i - is tested through its own connector.

The rewrites here are what makes a Db2 statement readable at all: no parser of this migrator
models Db2, the statements are read as PostgreSQL, and PostgreSQL cannot read
'CURRENT DATE - 12 MONTHS' or 'WITH UR'. Without them every second statement of a Db2
application would be reported as one the migrator cannot read - an answer about the parser
rather than about the statement.

Run with:  python3 -m pytest tests/test_db2_query_conversion.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.connectors.db2_query_conversion import Db2QueryConversion
from credativ_pg_migrator.query_conversion import classifier, parameters


@pytest.fixture
def db2():
    return Db2QueryConversion()


def prepared(db2, text):
    return db2.prepare_query_for_parsing(text)


def parses(text):
    """Whether the statement can be read at all once it has been prepared."""
    return classifier.classify(text, 'ibm_db2_luw', parse_text=text).is_select


# --------------------------------------------------------------------------------------
# the special registers, which Db2 writes without parentheses


@pytest.mark.parametrize('written,expected', [
    ('CURRENT DATE', 'CURRENT_DATE'),
    ('CURRENT TIME', 'CURRENT_TIME'),
    ('CURRENT TIMESTAMP', 'CURRENT_TIMESTAMP'),
    ('CURRENT TIMESTAMP WITH TIME ZONE', 'CURRENT_TIMESTAMP'),
    ('CURRENT USER', 'CURRENT_USER'),
    ('CURRENT SCHEMA', 'CURRENT_SCHEMA'),
    ('CURRENT SQLID', 'CURRENT_USER'),
    ('CURRENT SERVER', 'current_database()'),
])
def test_a_special_register_becomes_the_name_postgresql_has(db2, written, expected):
    assert prepared(db2, f"SELECT {written} FROM T") == f"SELECT {expected} FROM T"


def test_a_register_which_has_no_equal_counterpart_is_reported(db2):
    """CURRENT SQLID decides which schema a name is resolved in; CURRENT_USER does not."""
    warnings = db2.db2_conversion_warnings("SELECT CURRENT SQLID FROM SYSIBM.SYSDUMMY1")
    assert any('CURRENT SQLID' in warning for warning in warnings)


def test_a_register_inside_a_literal_is_text(db2):
    statement = "SELECT A FROM T WHERE NOTE = 'as of CURRENT DATE'"
    assert prepared(db2, statement) == statement


# --------------------------------------------------------------------------------------
# the labelled durations


@pytest.mark.parametrize('written,expected', [
    ('CURRENT DATE - 12 MONTHS', "CURRENT_DATE - INTERVAL '12 MONTHS'"),
    ('O.ORDER_DATE + 7 DAYS', "O.ORDER_DATE + INTERVAL '7 DAYS'"),
    ('O.CREATED_AT - 1 YEAR', "O.CREATED_AT - INTERVAL '1 YEAR'"),
    ('T.STAMP + 30 MINUTES', "T.STAMP + INTERVAL '30 MINUTES'"),
])
def test_a_labelled_duration_becomes_an_interval(db2, written, expected):
    assert prepared(db2, f"SELECT A FROM T WHERE X > {written}") == \
        f"SELECT A FROM T WHERE X > {expected}"


def test_a_duration_counted_by_a_column_is_reported_instead_of_guessed(db2):
    """An interval of PostgreSQL cannot be built from a column, so this one needs a person."""
    statement = "SELECT A FROM T WHERE D > CURRENT TIMESTAMP - N DAYS"
    assert 'INTERVAL' not in prepared(db2, statement)
    assert any('labelled duration' in warning for warning in db2.db2_conversion_warnings(statement))


# --------------------------------------------------------------------------------------
# the clauses which have no counterpart


@pytest.mark.parametrize('clause', ['WITH UR', 'WITH CS', 'WITH RS', 'WITH RR'])
def test_the_isolation_clause_is_removed(db2, clause):
    statement = f"SELECT COUNT(*) FROM ORDERS WHERE STATUS = 'NEW' {clause}"
    assert prepared(db2, statement) == "SELECT COUNT(*) FROM ORDERS WHERE STATUS = 'NEW'"
    assert any(clause in warning for warning in db2.db2_conversion_warnings(statement))


def test_a_with_clause_which_is_a_cte_is_not_touched(db2):
    statement = "WITH C AS (SELECT 1 AS X) SELECT * FROM C"
    assert prepared(db2, statement) == statement


@pytest.mark.parametrize('clause,expected_warning', [
    ('OPTIMIZE FOR 10 ROWS', 'OPTIMIZE FOR'),
    ('QUERYNO 42', 'QUERYNO'),
    ('SKIP LOCKED DATA', 'SKIP LOCKED'),
    ('FOR READ ONLY', 'FOR READ ONLY'),
    ('FOR FETCH ONLY', 'FOR READ ONLY'),
])
def test_the_hints_are_removed_and_named(db2, clause, expected_warning):
    statement = f"SELECT O.ORDER_ID FROM ORDERS O {clause}"
    assert prepared(db2, statement).strip() == "SELECT O.ORDER_ID FROM ORDERS O"
    assert any(expected_warning in warning for warning in db2.db2_conversion_warnings(statement))


def test_fetch_first_rows_only_is_kept(db2):
    """It is the ANSI spelling and PostgreSQL takes it - there is nothing to convert."""
    statement = "SELECT O.ORDER_ID FROM ORDERS O FETCH FIRST 10 ROWS ONLY"
    assert prepared(db2, statement) == statement


# --------------------------------------------------------------------------------------
# SYSIBM.SYSDUMMY1 and DAYS()


def test_the_one_row_table_of_db2_is_left_out(db2):
    """PostgreSQL needs no table for a SELECT which reads none."""
    assert prepared(db2, "SELECT CURRENT DATE FROM SYSIBM.SYSDUMMY1") == "SELECT CURRENT_DATE"
    assert prepared(db2, "SELECT 1 FROM SYSIBM/SYSDUMMY1") == "SELECT 1"


def test_the_days_between_two_dates_becomes_a_subtraction(db2):
    """'DAYS(a) - DAYS(b)' is how a Db2 application counts days; PostgreSQL subtracts dates."""
    assert prepared(db2, "SELECT DAYS(CURRENT DATE) - DAYS(O.ORDER_DATE) FROM ORDERS O") == \
        "SELECT (CAST(CURRENT_DATE AS DATE) - CAST(O.ORDER_DATE AS DATE)) FROM ORDERS O"


def test_a_single_days_call_is_reported_rather_than_guessed(db2):
    statement = "SELECT DAYS(O.ORDER_DATE) FROM ORDERS O"
    assert 'DAYS(' in prepared(db2, statement)
    assert any('DAYS()' in warning for warning in db2.db2_conversion_warnings(statement))


# --------------------------------------------------------------------------------------
# what all of this is for: the statement can be read


@pytest.mark.parametrize('statement', [
    "SELECT COUNT(*) FROM ORDERS WHERE STATUS = 'NEW' WITH UR",
    "SELECT CURRENT USER AS WHO FROM SYSIBM.SYSDUMMY1",
    "SELECT O.ORDER_ID FROM ORDERS O WHERE O.ORDER_DATE >= CURRENT DATE - 12 MONTHS",
    "SELECT DAYS(CURRENT DATE) - DAYS(O.ORDER_DATE) AS AGE FROM ORDERS O",
    "SELECT O.ORDER_ID FROM ORDERS O ORDER BY O.ORDER_DATE DESC FETCH FIRST 10 ROWS ONLY OPTIMIZE FOR 10 ROWS",
    "VALUES (1, 'ONE')",
])
def test_a_statement_of_db2_is_a_read_once_it_is_prepared(db2, statement):
    """Without the preparation every one of these is 'the migrator cannot read this'."""
    assert not parses(statement) or statement.startswith('VALUES'), \
        'this statement would parse without the preparation - it does not belong here'
    assert parses(prepared(db2, statement))


@pytest.mark.parametrize('statement', [
    "UPDATE CUSTOMERS SET LAST_LOGIN = CURRENT TIMESTAMP WHERE CUSTOMER_ID = 1",
    "SELECT ORDER_ID FROM FINAL TABLE (INSERT INTO ORDERS (ORDER_ID) VALUES (1))",
    "SELECT NEXT VALUE FOR SEQ_ORDERS FROM SYSIBM.SYSDUMMY1",
    "SELECT O.ORDER_ID FROM ORDERS O WHERE O.ORDER_ID = 1 WITH RS USE AND KEEP UPDATE LOCKS",
])
def test_the_preparation_does_not_make_a_write_look_like_a_read(db2, statement):
    """The gates read the text of the application, and the preparation changes none of that."""
    result = classifier.classify(statement, 'ibm_db2_luw', parse_text=prepared(db2, statement))
    assert result.verdict == 'refused', result.reason


# --------------------------------------------------------------------------------------
# the entry point of the connectors


class FakeDb2(Db2QueryConversion):
    """A connector which converts by upper-casing - enough to see what the wrapper does."""

    class config_parser:
        @staticmethod
        def print_log_message(level, message):
            pass

    def __init__(self, answer=None, error=None):
        self.answer = answer
        self.error = error

    def convert_statement_code(self, settings):
        if self.error:
            raise self.error
        return self.answer if self.answer is not None else settings['view_code'].upper()


def convert(connector, query_code="SELECT A FROM T WITH UR"):
    return connector.convert_query_code({
        'query_code': query_code, 'source_schema_name': 'MIGTEST',
        'target_schema_name': 'migtest', 'target_db_type': 'postgresql', 'statement_id': 'x'})


def test_the_conversion_answers_with_the_statement_and_its_warnings():
    answer = convert(FakeDb2())
    assert answer['converted'] is True
    assert answer['code'] == "SELECT A FROM T WITH UR".upper()
    assert any('WITH UR' in warning for warning in answer['warnings'])


def test_a_statement_which_could_not_be_parsed_is_not_offered_as_converted():
    answer = convert(FakeDb2(error=ValueError('the statement could not be parsed: something')))
    assert answer['converted'] is False
    assert answer['code'] == ''
    assert 'could not be parsed' in answer['error']


def test_an_error_of_any_other_kind_is_reported_as_one():
    answer = convert(FakeDb2(error=RuntimeError('boom')))
    assert answer['converted'] is False
    assert 'boom' in answer['error']


def test_a_conversion_which_produced_nothing_is_not_a_conversion():
    answer = convert(FakeDb2(answer='   '))
    assert answer['converted'] is False
    assert 'no statement at all' in answer['error']


def test_the_warnings_are_reported_even_when_the_conversion_failed():
    answer = convert(FakeDb2(error=ValueError('nope')))
    assert any('WITH UR' in warning for warning in answer['warnings'])


def test_the_three_flavours_all_answer_that_they_can_do_it():
    assert Db2QueryConversion().query_conversion_supported() is True


def test_the_functions_of_db2_are_mapped_once_for_the_three_flavours():
    """
    POSSTR of Db2 takes (source, search), like STRPOS of PostgreSQL; POSITION is written the
    other way round and with IN between them. The copy of Db2 for i had it wrong, which is
    what a second copy of a mapping is for.
    """
    mapping = Db2QueryConversion().get_sql_functions_mapping({'target_db_type': 'postgresql'})
    assert mapping['POSSTR('] == 'STRPOS('
    assert mapping['SUBSTR('] == 'SUBSTRING('
    assert mapping['VALUE('] == 'COALESCE('
    assert mapping['YEAR('].startswith('EXTRACT(')


# --------------------------------------------------------------------------------------
# the bind parameters survive all of it


def test_a_bind_parameter_is_carried_through_the_preparation(db2):
    statement = "SELECT A FROM T WHERE B = ? AND D >= CURRENT DATE - 7 DAYS WITH UR"
    bind_parameters, _warnings = parameters.extract(statement)
    result = prepared(db2, bind_parameters.conversion_statement)
    assert 'cpgm_bind_param_1' in result
    assert "INTERVAL '7 DAYS'" in result
    assert 'WITH UR' not in result
