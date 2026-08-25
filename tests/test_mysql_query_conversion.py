# SPDX-License-Identifier: GPL-3.0-or-later
"""
The query conversion of MySQL and MariaDB.

They are two connectors and one SQL dialect, so the conversion stands once, in
connectors/mysql_query_conversion.py, and both connectors are asked here - a mapping or a
rewrite which exists twice is a mapping which drifts, and this suite has caught that before.

This dialect is the one case where the parser of the migrator *does* model the source:
sqlglot reads MySQL and writes PostgreSQL, so most of a statement needs nothing from us.
These tests are about the rest of it, which is where the damage would be:

  * what the transpiler writes correctly is left alone - and a few of those are asserted too,
    so that a rewrite added later cannot quietly take them over;
  * what it writes as something PostgreSQL does not have (`DAY_OF_WEEK`) or does not mean
    (`CONCAT_WS` wrapped in a CASE which answers NULL) is rewritten before the statement is
    written back;
  * what has no counterpart at all stops the conversion with the reason, instead of being
    handed back with the call still in it.

Run with:  python3 -m pytest tests/test_mysql_query_conversion.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.connectors.mysql_connector import MySQLConnector
from credativ_pg_migrator.connectors.mariadb_connector import MariaDBConnector
from credativ_pg_migrator.connectors.mysql_query_conversion import MySqlQueryConversion
from credativ_pg_migrator.query_conversion import parameters


class RecordingLog:
    args = None

    def __init__(self):
        self.messages = []

    def print_log_message(self, level, message):
        self.messages.append((level, message))

    def get_target_db_type(self):
        return 'postgresql'


def build(connector_class):
    """
    The connector without its __init__ - the conversion is a transformation of text and needs
    neither a configuration nor a connection.
    """
    connector = connector_class.__new__(connector_class)
    connector.config_parser = RecordingLog()
    return connector


@pytest.fixture(params=[MySQLConnector, MariaDBConnector],
                ids=['mysql', 'mariadb'])
def connector(request):
    """Every rule here has to hold for both connectors - they are one dialect."""
    return build(request.param)


def converted(connector, statement):
    return connector.convert_query_code({
        'query_code': statement, 'source_schema_name': 'migtest',
        'target_schema_name': 'public', 'target_db_type': 'postgresql',
        'statement_id': 'test:1'})


def expression_of(connector, expression):
    """What the SELECT list of a converted statement holds - the expression alone."""
    answer = converted(connector, f"SELECT {expression} FROM t")
    assert answer['converted'] is True, answer['error']
    return answer['code'][len('SELECT '):].split(' FROM t')[0]


# --------------------------------------------------------------------------------------
# what the transpiler writes as something PostgreSQL does not mean


def test_concat_ws_keeps_skipping_a_null_argument(connector):
    """
    CONCAT_WS of MySQL skips a NULL argument and so does concat_ws() of PostgreSQL. The
    transpiler wraps the call in a CASE which answers NULL as soon as one argument is NULL -
    a statement which is valid on both sides and answers other values, which is the worst
    kind of defect this conversion can ship.
    """
    assert expression_of(connector, "CONCAT_WS(' ', a, b)") == "CONCAT_WS(' ', a, b)"


def test_concat_of_mysql_does_propagate_null(connector):
    """The other direction: CONCAT does answer NULL, and '||' of PostgreSQL does too."""
    assert expression_of(connector, "CONCAT(a, b)") == "a || b"


def test_datediff_counts_days_and_not_an_interval(connector):
    """The transpiler answers DATEDIFF with a cast of an interval to a number."""
    assert expression_of(connector, "DATEDIFF(a, b)") == \
        "(CAST(a AS DATE) - CAST(b AS DATE))"


@pytest.mark.parametrize('written,expected', [
    ("DAYOFWEEK(a)", "(EXTRACT(DOW FROM CAST(a AS DATE)) + 1)"),
    ("DAYOFYEAR(a)", "EXTRACT(DOY FROM CAST(a AS DATE))"),
    ("QUARTER(a)", "EXTRACT(QUARTER FROM a)"),
    ("WEEKDAY(a)", "(EXTRACT(ISODOW FROM a) - 1)"),
    ("WEEKOFYEAR(a)", "EXTRACT(WEEK FROM CAST(a AS DATE))"),
    ("UNIX_TIMESTAMP(a)", "EXTRACT(EPOCH FROM a)"),
])
def test_a_date_field_is_counted_the_way_postgresql_counts_it(connector, written, expected):
    """
    Each of these is a number, and each dialect counts from another end: MySQL counts the
    days of the week from Sunday as 1 and PostgreSQL from Sunday as 0, MySQL's WEEKDAY counts
    from Monday as 0. A conversion which renames the call answers a number which is right
    six days out of seven.
    """
    assert expression_of(connector, written) == expected


def test_dayname_is_not_padded(connector):
    """to_char() pads the name of a day to nine characters and MySQL does not."""
    assert expression_of(connector, "DAYNAME(a)") == "TRIM(TO_CHAR(a, 'TMDay'))"


def test_mid_is_a_substring(connector):
    assert expression_of(connector, "MID(a, 1, 2)").lower() == "substring(a from 1 for 2)"


def test_a_cast_to_an_unsigned_integer_becomes_the_signed_type_which_holds_it(connector):
    """PostgreSQL has no unsigned integer, and the transpiler answers with the MySQL type."""
    assert expression_of(connector, "CAST(a AS UNSIGNED)") == "CAST(a AS BIGINT)"
    assert expression_of(connector, "CAST(a AS SIGNED)") == "CAST(a AS BIGINT)"


@pytest.mark.parametrize('written,expected', [
    ("SUBSTRING_INDEX(a, '@', 1)", "SPLIT_PART(a, '@', 1)"),
    ("SUBSTRING_INDEX(a, '.', 2)", "ARRAY_TO_STRING((STRING_TO_ARRAY(a, '.'))[1:2], '.')"),
    ("SUBSTRING_INDEX(a, '.', -1)",
     "ARRAY_TO_STRING((STRING_TO_ARRAY(a, '.'))[CARDINALITY(STRING_TO_ARRAY(a, '.')):], '.')"),
])
def test_substring_index_becomes_the_fields_of_the_string(connector, written, expected):
    assert expression_of(connector, written) == expected


def test_substring_index_with_a_count_which_is_not_a_literal_is_refused(connector):
    """Which end is counted from decides the whole expression, and a column does not say."""
    answer = converted(connector, "SELECT SUBSTRING_INDEX(a, '.', n) FROM t")
    assert answer['converted'] is False
    assert 'SUBSTRING_INDEX' not in answer['code']


@pytest.mark.parametrize('unit,expected_part', [
    ('SECOND', 'EXTRACT(EPOCH FROM'),
    ('DAY', '/ 86400'),
    ('WEEK', '/ 604800'),
    ('MONTH', 'AGE('),
    ('YEAR', '/ 12'),
])
def test_timestampdiff_counts_in_the_unit_it_was_given(connector, unit, expected_part):
    """
    The units up to the week are whole seconds; a month is not 30 days and is counted in the
    calendar, which is what age() answers. MySQL counts towards zero, so it is a truncation.
    """
    written = expression_of(connector, f"TIMESTAMPDIFF({unit}, start_at, end_at)")
    assert expected_part in written
    assert 'TRUNC' in written or unit == 'MONTH'
    ## the order of the two: MySQL counts from the first to the second
    assert written.index('end_at') < written.index('start_at')


def test_timestampdiff_in_a_unit_which_cannot_be_counted_is_refused(connector):
    answer = converted(connector, "SELECT TIMESTAMPDIFF(MICROSECOND, a, b) FROM t")
    assert answer['converted'] is False


def test_a_format_code_without_a_counterpart_stops_the_conversion(connector):
    """
    DATE_FORMAT becomes to_char(), whose format is written another way. A code the
    transpiler cannot map is carried over unchanged, and to_char() writes it out as text -
    the statement stays valid and answers another string.
    """
    assert converted(connector, "SELECT DATE_FORMAT(a, '%X %V') FROM t")['converted'] is False
    assert expression_of(connector, "DATE_FORMAT(a, '%Y-%m')") == \
        "TO_CHAR(CAST(a AS TIMESTAMP), 'YYYY-MM')"


# --------------------------------------------------------------------------------------
# what the transpiler already writes correctly, and which no rewrite may take over


@pytest.mark.parametrize('written,expected', [
    ("IFNULL(a, b)", "COALESCE(a, b)"),
    ("IF(a > 1, 'y', 'n')", "CASE WHEN a > 1 THEN 'y' ELSE 'n' END"),
    ("LOCATE('@', a)", "POSITION('@' IN a)"),
    ("INSTR(a, '@')", "POSITION('@' IN a)"),
    ("SPACE(3)", "REPEAT(' ', 3)"),
    ("TRUNCATE(a, 2)", "TRUNC(a, 2)"),
    ("FROM_UNIXTIME(a)", "TO_TIMESTAMP(a)"),
    ("STR_TO_DATE(a, '%Y-%m-%d')", "TO_DATE(a, 'YYYY-MM-DD')"),
    ("MD5(a)", "MD5(a)"),
    ("GROUP_CONCAT(a)", "STRING_AGG(a, ',')"),
])
def test_what_the_transpiler_writes_correctly_is_left_alone(connector, written, expected):
    """MD5() is here on purpose: it answers the same hex string in both, unlike SHA2()."""
    assert expression_of(connector, written) == expected


def test_the_paging_of_mysql_is_written_the_other_way_round(connector):
    answer = converted(connector, "SELECT a FROM t ORDER BY a LIMIT 100, 20")
    assert answer['code'].endswith('LIMIT 20 OFFSET 100')


def test_the_backtick_is_not_an_identifier_quote_of_postgresql(connector):
    answer = converted(connector, "SELECT `c`.`id` FROM `customers` `c`")
    assert '`' not in answer['code']
    assert '"c"."id"' in answer['code']


def test_the_null_ordering_of_mysql_is_written_out(connector):
    """
    MySQL sorts NULL first ascending and last descending; PostgreSQL sorts it last
    ascending. The transpiler writes the ordering out, and it has to stay written out - the
    same statement would otherwise answer the rows in another order.
    """
    answer = converted(connector, "SELECT a FROM t ORDER BY a DESC")
    assert 'NULLS LAST' in answer['code']


# --------------------------------------------------------------------------------------
# what PostgreSQL has nothing for is not converted


@pytest.mark.parametrize('statement,expected_reason', [
    ("SELECT INET6_NTOA(a) FROM t", 'INET'),
    ("SELECT INET_ATON(a) FROM t", 'INET'),
    ("SELECT HEX(a) FROM t", 'HEX()'),
    ("SELECT CONV(a, 10, 2) FROM t", 'CONV()'),
    ("SELECT SHA2(a, 256) FROM t", 'hash functions'),
    ("SELECT SHA1(a) FROM t", 'hash functions'),
    ("SELECT LAST_INSERT_ID() FROM t", 'session and server functions'),
    ("SELECT ROW_COUNT() FROM t", 'session and server functions'),
    ("SELECT USER() FROM t", "'user@host'"),
    ("SELECT FIELD(a, 'x', 'y') FROM t", 'FIELD()'),
    ("SELECT TIME_TO_SEC(a) FROM t", 'date arithmetic'),
    ("SELECT YEARWEEK(a) FROM t", 'date arithmetic'),
    ("SELECT JSON_LENGTH(a) FROM t", 'JSON'),
    ("SELECT JSON_UNQUOTE(JSON_EXTRACT(a, '$.x')) FROM t", 'JSON'),
    ("SELECT FORMAT(a, 2) FROM t", 'no expression in PostgreSQL'),
    ("SELECT BIT_COUNT(a) FROM t", 'no expression in PostgreSQL'),
])
def test_a_construct_without_a_counterpart_stops_the_conversion(connector, statement,
                                                                expected_reason):
    answer = converted(connector, statement)
    assert answer['converted'] is False
    assert answer['code'] == ''
    assert expected_reason in answer['error']


@pytest.mark.parametrize('statement', [
    "SELECT a AS hex, b AS version FROM t",
    "SELECT a FROM t WHERE note = 'HEX(x) and LAST_INSERT_ID()'",
    "SELECT /* FIELD() is not called here */ a FROM t",
    "SELECT t.user FROM t",
])
def test_a_name_which_reads_like_a_construct_does_not_stop_the_conversion(connector, statement):
    """A function name in a comment, in a literal or as a column is not a call."""
    assert converted(connector, statement)['converted'] is True


# --------------------------------------------------------------------------------------
# what is converted and still means something else afterwards


@pytest.mark.parametrize('statement,expected_warning', [
    ("SELECT a FROM t WHERE b RLIKE '^x'", 'case sensitively'),
    ("SELECT a FROM t WHERE b REGEXP '^x'", 'case sensitively'),
    ("SELECT a FROM t WHERE d > '0000-00-00'", '0000-00-00'),
    ("SELECT JSON_EXTRACT(a, '$.color') FROM t", "'->>'"),
    ("SELECT MONTHNAME(a) FROM t", 'nine characters'),
    ("SELECT WEEK(a) FROM t", 'ISO week'),
])
def test_what_still_means_something_else_is_reported(connector, statement, expected_warning):
    answer = converted(connector, statement)
    assert any(expected_warning in warning for warning in answer['warnings']), answer['warnings']


def test_a_construct_named_in_a_literal_is_not_warned_about(connector):
    answer = converted(connector, "SELECT a FROM t WHERE note = 'use REGEXP for this'")
    assert answer['warnings'] == []


# --------------------------------------------------------------------------------------
# the entry point


def test_the_conversion_answers_with_the_statement(connector):
    """
    The bind parameters are taken out before anything parses the statement - '%s' is not
    SQL - so what reaches the conversion carries the name the workflow gave them.
    """
    statement = "SELECT c.id FROM customers c WHERE c.name = %s"
    bind_parameters, _warnings = parameters.extract(statement)
    answer = converted(connector, bind_parameters.conversion_statement)
    assert answer['converted'] is True
    assert answer['error'] is None
    assert 'customers' in answer['code']


def test_a_statement_which_could_not_be_parsed_is_not_offered_as_converted(connector):
    """
    This is what the lift is for. The conversion used to answer an unparsable statement with
    the text of the source - the MySQL statement, unchanged, reported as converted.
    """
    answer = converted(connector, "SELECT FROM WHERE ((")
    assert answer['converted'] is False
    assert answer['code'] == ''
    assert 'could not be parsed' in answer['error']


def test_a_conversion_which_produced_nothing_is_not_a_conversion(connector, monkeypatch):
    monkeypatch.setattr(type(connector), 'convert_statement_code',
                        lambda self, settings: '   ')
    answer = converted(connector, "SELECT a FROM t")
    assert answer['converted'] is False
    assert 'no statement at all' in answer['error']


def test_an_error_of_any_other_kind_is_reported_as_one(connector, monkeypatch):
    monkeypatch.setattr(type(connector), 'convert_statement_code',
                        lambda self, settings: (_ for _ in ()).throw(RuntimeError('boom')))
    answer = converted(connector, "SELECT a FROM t")
    assert answer['converted'] is False
    assert 'boom' in answer['error']


def test_the_warnings_are_reported_even_when_the_conversion_failed(connector):
    answer = converted(connector, "SELECT HEX(a) FROM t WHERE b RLIKE '^x'")
    assert answer['converted'] is False
    assert any('case sensitively' in warning for warning in answer['warnings'])


def test_both_connectors_answer_that_they_can_do_it(connector):
    assert connector.query_conversion_supported() is True


def test_the_functions_are_mapped_once_for_both_connectors():
    """
    Both connectors carried a copy of this mapping. A second copy is a copy which drifts -
    the third copy of the Db2 mapping had done exactly that.
    """
    settings = {'target_db_type': 'postgresql'}
    mysql = build(MySQLConnector).get_sql_functions_mapping(settings)
    mariadb = build(MariaDBConnector).get_sql_functions_mapping(settings)
    assert mysql == mariadb
    assert mysql['ifnull('] == 'coalesce('
    assert mysql['instr('] == 'strpos('


def test_the_conversion_stands_once_for_both_connectors():
    assert MySQLConnector.convert_statement_code is MySqlQueryConversion.convert_statement_code
    assert MariaDBConnector.convert_statement_code is MySqlQueryConversion.convert_statement_code


# --------------------------------------------------------------------------------------
# the view path keeps the behaviour it had


def test_the_view_and_the_statement_are_given_the_same_conversion(connector):
    settings = {'view_code': "SELECT a FROM t WHERE b = 1", 'source_schema_name': 'migtest',
                'target_schema_name': 'public', 'target_db_type': 'postgresql'}
    assert connector.convert_view_code(dict(settings)) == \
        connector.convert_statement_code(dict(settings))


def test_a_view_which_could_not_be_parsed_keeps_the_text_of_the_source(connector):
    """
    The migration reports such a view as failed and its source code stays readable in the
    protocol - which is what it did before the converter was lifted out. What changed is who
    decides: the caller does, and the query conversion decides differently.
    """
    settings = {'view_code': "SELECT FROM WHERE ((", 'source_schema_name': 'migtest',
                'target_schema_name': 'public', 'target_db_type': 'postgresql'}
    with pytest.raises(ValueError):
        connector.convert_statement_code(dict(settings))
    assert 'SELECT' in connector.convert_view_code(dict(settings))
    assert any('could not be parsed' in message
               for _level, message in connector.config_parser.messages)


def test_the_schema_of_the_source_is_replaced(connector):
    answer = converted(connector, "SELECT a FROM migtest.customers")
    assert '"public".customers' in answer['code'] or '"public"."customers"' in answer['code']


# --------------------------------------------------------------------------------------
# the bind parameters survive all of it


def test_a_bind_parameter_is_carried_through_the_conversion(connector):
    statement = "SELECT a FROM t WHERE b = %s AND c > DATEDIFF(d, %s) ORDER BY a LIMIT 10"
    bind_parameters, _warnings = parameters.extract(statement)
    answer = converted(connector, bind_parameters.conversion_statement)
    assert answer['converted'] is True
    restored = bind_parameters.to_numbered(answer['code'])
    assert '$1' in restored and '$2' in restored
    assert 'cpgm_bind_param' not in restored
