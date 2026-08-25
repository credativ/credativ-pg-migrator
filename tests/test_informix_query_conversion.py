# SPDX-License-Identifier: GPL-3.0-or-later
"""
The query conversion of IBM Informix.

No parser of this migrator models Informix, and the statements are read as PostgreSQL - which
stops at the FIRST clause in front of the select list, at the OUTER() of a FROM clause, at
TODAY and CURRENT written without parentheses, at a duration counted in UNITS, at a substring
written as a subscript and at the MATCHES operator. Every one of them is rewritten before
anything parses the statement, and the same rewrite is the conversion: they are constructs
PostgreSQL has nothing for, not a matter of taste.

What PostgreSQL really has nothing for - ROWID, DBINFO(), HEX() - is not converted at all. The
statement is reported as NOT CONVERTED with the reason, because a statement handed back with a
construct the target cannot answer would look like a conversion without being one.

Run with:  python3 -m pytest tests/test_informix_query_conversion.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.connectors.informix_connector import InformixConnector
from credativ_pg_migrator.query_conversion import classifier, parameters


class RecordingLog:
    def __init__(self):
        self.messages = []

    def print_log_message(self, level, message):
        self.messages.append((level, message))


def build_connector():
    """
    The connector without its __init__ - the conversion is a transformation of text and needs
    neither a configuration nor a connection, and the query conversion of the test suite asks
    for the preparation exactly this way.
    """
    connector = InformixConnector.__new__(InformixConnector)
    connector.config_parser = RecordingLog()
    return connector


@pytest.fixture
def informix():
    return build_connector()


def prepared(informix, text):
    return informix.prepare_query_for_parsing(text)


def parses(text):
    """Whether the statement can be read at all once it has been prepared."""
    return classifier.classify(text, 'informix', parse_text=text).is_select


def converted(informix, query_code):
    return informix.convert_query_code({
        'query_code': query_code, 'source_schema_name': 'informix',
        'target_schema_name': 'public', 'target_db_type': 'postgresql',
        'statement_id': 'test:1'})


# --------------------------------------------------------------------------------------
# FIRST and SKIP, which stand at the other end of the statement


@pytest.mark.parametrize('written,expected', [
    ('SELECT FIRST 20 a FROM t', 'SELECT a FROM t LIMIT 20'),
    ('SELECT SKIP 100 FIRST 20 a FROM t', 'SELECT a FROM t LIMIT 20 OFFSET 100'),
    ('SELECT SKIP 5 a FROM t', 'SELECT a FROM t OFFSET 5'),
    ('SELECT DISTINCT FIRST 10 a FROM t', 'SELECT DISTINCT a FROM t LIMIT 10'),
])
def test_the_paging_of_informix_is_moved_to_the_end_of_the_statement(informix, written, expected):
    assert prepared(informix, written) == expected


def test_the_limit_is_written_behind_the_order_by(informix):
    """LIMIT in front of ORDER BY would be a syntax error, and it would mean other rows."""
    statement = 'SELECT FIRST 3 a FROM t ORDER BY a DESC'
    assert prepared(informix, statement) == 'SELECT a FROM t ORDER BY a DESC LIMIT 3'


def test_the_paging_of_a_subquery_stays_inside_it(informix):
    statement = 'SELECT x.a FROM (SELECT FIRST 3 t.a FROM t ORDER BY t.a) x'
    assert prepared(informix, statement) == \
        'SELECT x.a FROM (SELECT t.a FROM t ORDER BY t.a LIMIT 3) x'


def test_paging_together_with_a_set_operator_is_left_as_it_is(informix):
    """
    Whether FIRST limits the branch it stands in or the result of the whole union is written
    nowhere, so nothing is moved and the statement is reported instead of being answered with
    another set of rows.
    """
    statement = 'SELECT FIRST 5 a FROM t UNION ALL SELECT b FROM u'
    assert prepared(informix, statement) == statement
    assert any('UNION' in warning for warning in informix.informix_conversion_warnings(statement))
    assert informix.informix_conversion_blockers(statement)


def test_an_alias_named_units_is_a_name_and_not_a_duration(informix):
    """'SUM(x) AS units' held the word which refuses a statement, and it is a column."""
    statement = 'SELECT SUM(oi.quantity) AS units FROM order_items oi'
    assert informix.informix_conversion_blockers(prepared(informix, statement)) == []


# --------------------------------------------------------------------------------------
# TODAY and CURRENT, the registers written without parentheses


@pytest.mark.parametrize('written,expected', [
    ('TODAY', 'CURRENT_DATE'),
    ('CURRENT', 'CURRENT_TIMESTAMP'),
    ('CURRENT YEAR TO SECOND', 'CURRENT_TIMESTAMP'),
    ('CURRENT YEAR TO FRACTION(3)', 'CURRENT_TIMESTAMP'),
])
def test_a_register_becomes_the_name_postgresql_has(informix, written, expected):
    assert prepared(informix, f'SELECT {written} AS now_at FROM t') == \
        f'SELECT {expected} AS now_at FROM t'


def test_a_register_which_holds_fewer_fields_is_reported(informix):
    """CURRENT YEAR TO DAY holds no time of day; CURRENT_TIMESTAMP holds all of it."""
    warnings = informix.informix_conversion_warnings('SELECT CURRENT YEAR TO DAY FROM t')
    assert any("date_trunc('day'" in warning for warning in warnings)


def test_the_current_row_of_a_window_frame_is_not_the_register(informix):
    statement = ('SELECT ROW_NUMBER() OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING '
                 'AND CURRENT ROW) FROM t')
    assert prepared(informix, statement) == statement


def test_a_register_inside_a_literal_is_text(informix):
    statement = "SELECT a FROM t WHERE note = 'as of TODAY'"
    assert prepared(informix, statement) == statement


def test_the_one_row_table_of_informix_is_left_out(informix):
    assert prepared(informix, 'SELECT TODAY FROM sysmaster:sysdual').split() == \
        ['SELECT', 'CURRENT_DATE']


def test_the_database_of_another_qualifier_is_removed_and_said(informix):
    statement = 'SELECT c.name FROM stores7:customer c'
    assert prepared(informix, statement) == 'SELECT c.name FROM customer c'
    assert any("stores7" in warning
               for warning in informix.informix_conversion_warnings(statement))


# --------------------------------------------------------------------------------------
# the durations and the literals of a dialect which counts in UNITS


@pytest.mark.parametrize('written,expected', [
    ("o.d - 7 UNITS DAY", "o.d - INTERVAL '7 day'"),
    ("o.d + 3 UNITS MONTH", "o.d + INTERVAL '3 month'"),
    ("o.d - 30 UNITS MINUTE", "o.d - INTERVAL '30 minute'"),
])
def test_a_duration_becomes_an_interval(informix, written, expected):
    assert prepared(informix, f'SELECT a FROM t WHERE {written} > b') == \
        f'SELECT a FROM t WHERE {expected} > b'


def test_a_duration_which_counts_a_column_is_multiplied_out(informix):
    """An interval of PostgreSQL is built from a literal, so the column becomes a factor."""
    assert prepared(informix, 'SELECT a FROM t WHERE d > o.days UNITS DAY') == \
        "SELECT a FROM t WHERE d > (o.days * INTERVAL '1 day')"


def test_a_duration_in_fractions_of_a_second_is_refused_rather_than_guessed(informix):
    statement = 'SELECT a FROM t WHERE d > 5 UNITS FRACTION'
    assert informix.informix_conversion_blockers(prepared(informix, statement))


@pytest.mark.parametrize('written,expected', [
    ('INTERVAL(2) DAY TO DAY', "INTERVAL '2 day'"),
    ('INTERVAL(3-6) YEAR TO MONTH', "INTERVAL '3 year 6 month'"),
    ('INTERVAL(2 15:30) DAY TO MINUTE', "INTERVAL '2 day 15 hour 30 minute'"),
    ('DATETIME(2024-01-15) YEAR TO DAY', "DATE '2024-01-15'"),
    ('DATETIME(2024-01-15 10:30:00) YEAR TO SECOND', "TIMESTAMP '2024-01-15 10:30:00'"),
    ('DATETIME(10:30:00) HOUR TO SECOND', "TIME '10:30:00'"),
])
def test_a_literal_of_informix_becomes_the_literal_postgresql_writes(informix, written, expected):
    assert prepared(informix, f'SELECT {written} AS v FROM t') == f'SELECT {expected} AS v FROM t'


def test_a_literal_which_holds_other_fields_than_its_qualifier_names_is_reported(informix):
    statement = 'SELECT INTERVAL(2 15) DAY TO SECOND AS v FROM t'
    assert prepared(informix, statement) == statement
    assert informix.informix_conversion_warnings(statement)
    assert informix.informix_conversion_blockers(prepared(informix, statement))


@pytest.mark.parametrize('written,expected', [
    ('DATETIME YEAR TO SECOND', 'TIMESTAMP'),
    ('DATETIME YEAR TO DAY', 'DATE'),
    ('INTERVAL DAY TO SECOND', 'INTERVAL'),
])
def test_the_types_are_written_the_way_postgresql_writes_them(informix, written, expected):
    assert prepared(informix, f'SELECT CAST(a AS {written}) FROM t') == \
        f'SELECT CAST(a AS {expected}) FROM t'


# --------------------------------------------------------------------------------------
# the routines and the operators of Informix


def test_the_subscript_of_informix_is_a_substring(informix):
    assert prepared(informix, 'SELECT c.last_name[1,3] FROM customers c') == \
        'SELECT SUBSTR(c.last_name, 1, 3) FROM customers c'


def test_mdy_takes_its_arguments_in_another_order(informix):
    """MDY(month, day, year) of Informix is make_date(year, month, day) of PostgreSQL."""
    assert prepared(informix, 'SELECT MDY(1, 15, 2024) FROM t') == \
        'SELECT make_date(2024, 1, 15) FROM t'


def test_decode_compares_the_way_decode_compares(informix):
    """
    'x = NULL' is never true, and a NULL matches a NULL in DECODE - a CASE written with '='
    would answer the default where the source answered the result.
    """
    result = prepared(informix, "SELECT DECODE(s, 1, 'a', 2, 'b', 'other') FROM t")
    assert result == ("SELECT (CASE WHEN s IS NOT DISTINCT FROM 1 THEN 'a' "
                      "WHEN s IS NOT DISTINCT FROM 2 THEN 'b' ELSE 'other' END) FROM t")


def test_decode_without_a_default_answers_null(informix):
    result = prepared(informix, "SELECT DECODE(s, 1, 'a') FROM t")
    assert result == "SELECT (CASE WHEN s IS NOT DISTINCT FROM 1 THEN 'a' END) FROM t"


def test_a_decode_inside_a_decode_is_converted_as_well(informix):
    result = prepared(informix, "SELECT DECODE(a, 1, DECODE(b, 1, 'x', 'y'), 'z') FROM t")
    assert result.count('CASE') == 2
    assert 'DECODE' not in result


@pytest.mark.parametrize('written,expected', [
    ('EXTEND(a, YEAR TO DAY)', 'CAST(a AS DATE)'),
    ('EXTEND(a, YEAR TO MONTH)', "CAST(date_trunc('month', CAST(a AS TIMESTAMP)) AS DATE)"),
    ('EXTEND(a, YEAR TO MINUTE)', "date_trunc('minute', CAST(a AS TIMESTAMP))"),
])
def test_extend_adjusts_the_fields_the_value_holds(informix, written, expected):
    assert prepared(informix, f'SELECT {written} FROM t') == f'SELECT {expected} FROM t'


def test_last_day_is_written_out(informix):
    result = prepared(informix, 'SELECT LAST_DAY(o.order_date) FROM orders o')
    assert "date_trunc('month'" in result and "INTERVAL '1 month - 1 day'" in result


@pytest.mark.parametrize('written,expected', [
    ("email MATCHES '*@example.com'", "email LIKE '%@example.com'"),
    ("code MATCHES 'A?'", "code LIKE 'A_'"),
    ("code MATCHES '[ABC]*'", "code SIMILAR TO '[ABC]%'"),
    ("code MATCHES '100%'", r"code LIKE '100\%'"),
])
def test_matches_is_a_rewrite_and_not_a_renaming(informix, written, expected):
    assert prepared(informix, f'SELECT a FROM t WHERE {written}') == \
        f'SELECT a FROM t WHERE {expected}'


def test_the_outer_join_of_informix_becomes_a_left_outer_join(informix):
    result = prepared(informix, 'SELECT c.id, o.id FROM customers c, OUTER(orders o) '
                                'WHERE c.id = o.customer_id')
    assert 'LEFT OUTER JOIN orders o ON (c.id = o.customer_id)' in result
    assert 'OUTER(' not in result


def test_an_outer_join_whose_conditions_cannot_be_attributed_is_refused(informix):
    """
    An OR spanning the subordinate table does not say which of its conditions belong to the
    join. Nothing is rewritten, and the statement is not offered as converted.
    """
    statement = ('SELECT c.id FROM customers c, OUTER(orders o) '
                 'WHERE c.id = o.customer_id OR o.status = 1')
    assert prepared(informix, statement) == statement
    assert any('OUTER(' in reason
               for reason in informix.informix_conversion_blockers(prepared(informix, statement)))


# --------------------------------------------------------------------------------------
# what PostgreSQL has nothing for is not converted


@pytest.mark.parametrize('statement,expected_reason', [
    ('SELECT ROWID, c.id FROM customers c', 'ctid'),
    ("SELECT DBINFO('sqlca.sqlerrd1') FROM t", 'DBINFO'),
    ('SELECT HEX(a) FROM t', 'HEX'),
    ('SELECT DBSERVERNAME FROM t', 'DBSERVERNAME'),
    ('SELECT MONTHS_BETWEEN(a, b) FROM t', 'MONTHS_BETWEEN'),
    ("SELECT a FROM t WHERE b MATCHES c.pattern", 'MATCHES'),
])
def test_a_construct_without_a_counterpart_stops_the_conversion(informix, statement,
                                                                expected_reason):
    answer = converted(informix, statement)
    assert answer['converted'] is False
    assert answer['code'] == ''
    assert expected_reason in answer['error']


@pytest.mark.parametrize('statement', [
    'SELECT SUM(x) AS units FROM t',
    'SELECT o.id AS rowid FROM orders o',
    'SELECT s.name AS sitename FROM sites s',
    "SELECT a FROM t WHERE note = 'MATCHES TODAY 3 UNITS DAY'",
])
def test_a_name_which_reads_like_a_construct_does_not_stop_the_conversion(informix, statement):
    assert converted(informix, statement)['converted'] is True


# --------------------------------------------------------------------------------------
# what all of this is for: the statement can be read


@pytest.mark.parametrize('statement', [
    'SELECT FIRST 20 p.product_id FROM products p ORDER BY p.product_id',
    'SELECT SKIP 100 FIRST 20 o.order_id FROM orders o ORDER BY o.order_date DESC',
    'SELECT TODAY AS today_is, CURRENT YEAR TO SECOND AS now_at FROM sysmaster:sysdual',
    'SELECT o.order_id FROM orders o WHERE o.order_date > TODAY - 7 UNITS DAY',
    "SELECT c.email FROM customers c WHERE c.email MATCHES '*@example.com'",
    'SELECT c.id, o.id FROM customers c, OUTER(orders o) WHERE c.id = o.customer_id',
])
def test_a_statement_of_informix_is_a_read_once_it_is_prepared(informix, statement):
    """
    Without the preparation every one of these is 'the migrator cannot read this'. The
    subscript of Informix is not among them: `c.last_name[1,3]` is read as the subscript of an
    array and parses, and it still has to be converted - PostgreSQL would answer 'cannot
    subscript type text' when the statement is sent.
    """
    assert not parses(statement), \
        'this statement would parse without the preparation - it does not belong here'
    assert parses(prepared(informix, statement))


@pytest.mark.parametrize('statement', [
    'UPDATE customers SET last_login = CURRENT WHERE customer_id = 1',
    'SELECT FIRST 10 a, b FROM t INTO TEMP work_table',
    'SELECT a INTO :hostvar FROM t WHERE b = TODAY',
    'SELECT c.id FROM customers c WHERE c.id = 1 FOR UPDATE',
    'UNLOAD TO "/tmp/x.unl" SELECT FIRST 10 a FROM t',
])
def test_the_preparation_does_not_make_a_write_look_like_a_read(informix, statement):
    """The gates read the text of the application, and the preparation changes none of that."""
    result = classifier.classify(statement, 'informix', parse_text=prepared(informix, statement))
    assert result.verdict == 'refused', result.reason


# --------------------------------------------------------------------------------------
# the entry point


def test_the_conversion_answers_with_the_statement_and_its_warnings(informix):
    answer = converted(informix, 'SELECT FIRST 5 c.name FROM stores7:customer c')
    assert answer['converted'] is True
    assert answer['code'] == 'SELECT c.name FROM customer c LIMIT 5'
    assert any('stores7' in warning for warning in answer['warnings'])


def test_an_error_of_the_conversion_is_reported_as_one(informix, monkeypatch):
    monkeypatch.setattr(InformixConnector, 'convert_statement_code',
                        lambda self, settings: (_ for _ in ()).throw(RuntimeError('boom')))
    answer = converted(informix, 'SELECT a FROM t')
    assert answer['converted'] is False
    assert 'boom' in answer['error']


def test_a_conversion_which_produced_nothing_is_not_a_conversion(informix, monkeypatch):
    monkeypatch.setattr(InformixConnector, 'convert_statement_code', lambda self, settings: '   ')
    answer = converted(informix, 'SELECT a FROM t')
    assert answer['converted'] is False
    assert 'no statement at all' in answer['error']


def test_the_warnings_are_reported_even_when_the_conversion_failed(informix):
    answer = converted(informix, 'SELECT ROWID FROM stores7:customer')
    assert answer['converted'] is False
    assert any('stores7' in warning for warning in answer['warnings'])


def test_the_connector_answers_that_it_can_do_it(informix):
    assert informix.query_conversion_supported() is True


def test_the_view_and_the_statement_are_given_the_same_conversion(informix):
    settings = {'view_code': 'SELECT FIRST 5 a FROM t', 'source_schema_name': 'informix',
                'target_schema_name': 'public', 'target_db_type': 'postgresql'}
    assert informix.convert_view_code(dict(settings)) == \
        informix.convert_statement_code(dict(settings))


def test_the_preparation_needs_nothing_of_the_migrator_around_it():
    """
    The query conversion of the test suite asks the connector for its preparation without
    building it, so the preparation may not reach for the logging of a migration which is not
    running. What it has to say is collected instead.
    """
    connector = InformixConnector.__new__(InformixConnector)
    assert not hasattr(connector, 'config_parser')
    assert connector.prepare_query_for_parsing('SELECT FIRST 5 a FROM t') == \
        'SELECT a FROM t LIMIT 5'
    assert connector.informix_conversion_warnings('SELECT a FROM stores7:t')


def test_the_conversion_of_a_view_writes_what_happened_into_the_log(informix):
    informix.convert_view_code({'view_code': 'SELECT FIRST 5 a FROM t',
                                'source_schema_name': 'informix', 'target_schema_name': 'public',
                                'target_db_type': 'postgresql'})
    assert any('LIMIT 5' in message for _level, message in informix.config_parser.messages)


# --------------------------------------------------------------------------------------
# the bind parameters survive all of it


def test_a_bind_parameter_is_carried_through_the_preparation(informix):
    statement = 'SELECT FIRST 10 a FROM t WHERE b = ? AND d >= TODAY - 7 UNITS DAY'
    bind_parameters, _warnings = parameters.extract(statement)
    result = prepared(informix, bind_parameters.conversion_statement)
    assert 'cpgm_bind_param_1' in result
    assert result.endswith('LIMIT 10')
    assert "CURRENT_DATE - INTERVAL '7 day'" in result
