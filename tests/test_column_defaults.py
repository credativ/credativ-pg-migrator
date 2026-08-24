# SPDX-License-Identifier: GPL-3.0-or-later
"""
A column keeps the DEFAULT it had, or the run says what it lost.

P1-4 of development/OPEN_ISSUES.md — column DEFAULT clauses dropped without a trace, in the
Oracle, MS SQL Server and SQL Anywhere connectors. A default is not decoration: it is the
value **every row inserted after the migration** gets, so a dropped one is a column full of
NULLs where the source generated something, and a half-converted one is a column full of a
different value.

Two of the three had been repaired since the finding was written and this file locks that in;
the third was repaired here, and it turned out to be the opposite of what the finding said:

  * **Oracle** translates `SYS_CONTEXT('USERENV', ...)` where PostgreSQL has a counterpart
    and reports where it has none. `SYS_GUID()` on a non-UUID column was reported and
    dropped; on a BYTEA column PostgreSQL *does* have a counterpart — `SYS_GUID()` answers a
    RAW(16), and 16 random bytes are what the hexadecimal of a generated UUID decodes to.
  * **MS SQL Server** dropped the **style** argument of `CONVERT` with a warning. The style
    is what the value looks like: `CONVERT(varchar(10), getdate(), 103)` is `24/08/2026` and
    the plain CAST which stood in its place is `2026-08-24`. Every row inserted after the
    migration got the other one. The styles are written with `to_char()` now, and the ones
    which no single format can write — Transact-SQL pads the hour with a space — are reported
    with what they mean instead of being converted into something nearly right.
  * **SQL Anywhere** dropped, at INFO, every default which still held a double-quoted token,
    calling it a column reference. A DEFAULT of SQL Anywhere **cannot reference a column** —
    its grammar allows a special value, a string, a number, a constant expression,
    AUTOINCREMENT or NULL, and a constant expression there "must not reference database
    objects" — so a double-quoted token in one is a *string*, written by a database whose
    `quoted_identifier` option was off. The connector read the same syntax as a string when
    it was the whole default and as a column when it was not, so `'a' || "b"` was thrown
    away. And four special values of that grammar were not converted at all.

Nothing here connects to anything.

Run with:  python3 -m pytest tests/test_column_defaults.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)


class RecordingConfig:
    """Enough of a configuration to convert a default, recording what is said about it."""

    def __init__(self, uuid_function='gen_random_uuid()'):
        self.messages = []
        self.uuid_function = uuid_function

    def print_log_message(self, level, message):
        self.messages.append((level, str(message)))

    def levels(self, level):
        return [message for written, message in self.messages if written == level]

    def written(self):
        return ' | '.join(message for _, message in self.messages)

    def get_uuid_default_function(self, target_column_type=None):
        column_type = str(target_column_type or '').upper()
        if any(token in column_type for token in ('TEXT', 'CHAR', 'STRING')):
            return f'{self.uuid_function}::text'
        return self.uuid_function

    def get_target_db_type(self):
        return 'postgresql'

    def get_connectivity(self, direction):
        return {'db_type': 'postgresql'}

    def get_remote_objects_substitution(self):
        return {}


def connector_for(engine, class_name):
    import importlib

    try:
        module = importlib.import_module(f'credativ_pg_migrator.connectors.{engine}_connector')
        connector_class = getattr(module, class_name)
    except Exception as error:
        pytest.skip(f'{engine} needs a driver which is not installed here ({error})')
    made = connector_class.__new__(connector_class)
    made.config_parser = RecordingConfig()
    return made


def convert(connector, value, column_type=''):
    return connector.convert_default_value({
        'extracted_default_value': value,
        'column_type': column_type,
        'target_db_type': 'postgresql',
    })


# --------------------------------------------------------------------------------------
# Oracle


@pytest.fixture
def oracle():
    return connector_for('oracle', 'OracleConnector')


def test_sys_context_is_translated_where_postgresql_has_a_counterpart(oracle):
    assert convert(oracle, "SYS_CONTEXT('USERENV','IP_ADDRESS')", 'TEXT') == 'inet_client_addr()::text'
    assert convert(oracle, "SYS_CONTEXT('USERENV','CURRENT_SCHEMA')", 'TEXT') == 'current_schema'
    assert oracle.config_parser.levels('WARNING') == []


def test_sys_context_without_a_counterpart_is_reported_and_not_left_standing(oracle):
    """`SYS_CONTEXT('USERENV','TERMINAL')` is not a function PostgreSQL has at all."""
    assert convert(oracle, "SYS_CONTEXT('USERENV','TERMINAL')", 'TEXT') == ''
    written = oracle.config_parser.levels('WARNING')
    assert len(written) == 1
    assert 'TERMINAL' in written[0]


def test_sys_guid_reaches_a_uuid_or_a_text_column(oracle):
    assert convert(oracle, 'SYS_GUID()', 'UUID') == 'gen_random_uuid()'
    assert convert(oracle, 'SYS_GUID()', 'TEXT') == 'gen_random_uuid()::text'
    assert oracle.config_parser.messages == []


def test_sys_guid_reaches_a_bytea_column_too(oracle):
    """
    SYS_GUID() answers a RAW(16), which is migrated as BYTEA, and the hexadecimal of a
    generated UUID decodes to exactly 16 random bytes. This used to be dropped as "no
    PostgreSQL equivalent", leaving a column which had a unique value per row with none.
    """
    converted = convert(oracle, 'SYS_GUID()', 'BYTEA')
    assert converted == "decode(replace(gen_random_uuid()::text, '-', ''), 'hex')"
    assert oracle.config_parser.messages == []


def test_the_uuid_function_of_the_configuration_is_the_one_used(oracle):
    oracle.config_parser.uuid_function = 'uuid_generate_v4()'
    assert convert(oracle, 'SYS_GUID()', 'BYTEA') == \
        "decode(replace(uuid_generate_v4()::text, '-', ''), 'hex')"


def test_sys_guid_on_a_column_which_can_hold_neither_is_reported(oracle):
    assert convert(oracle, 'SYS_GUID()', 'INTEGER') == ''
    written = oracle.config_parser.levels('WARNING')
    assert len(written) == 1
    assert 'new rows get NULL' in written[0], 'the message has to say what the column loses'


def test_the_ordinary_oracle_defaults_are_unharmed(oracle):
    assert convert(oracle, 'SYSDATE', 'TIMESTAMP') == 'current_timestamp'
    assert convert(oracle, 'USER', 'VARCHAR(30)') == 'current_user'
    assert convert(oracle, "'ACTIVE'", 'VARCHAR(10)') == "'ACTIVE'"
    assert convert(oracle, '0', 'INTEGER') == '0'


# --------------------------------------------------------------------------------------
# MS SQL Server - the style argument of CONVERT


@pytest.fixture
def ms_sql():
    return connector_for('ms_sql', 'MsSQLConnector')


@pytest.mark.parametrize('style,expected', [
    (101, 'MM/DD/YYYY'),
    (102, 'YYYY.MM.DD'),
    (103, 'DD/MM/YYYY'),
    (104, 'DD.MM.YYYY'),
    (105, 'DD-MM-YYYY'),
    (110, 'MM-DD-YYYY'),
    (111, 'YYYY/MM/DD'),
    (112, 'YYYYMMDD'),
    (120, 'YYYY-MM-DD HH24:MI:SS'),
    (121, 'YYYY-MM-DD HH24:MI:SS.MS'),
    (23, 'YYYY-MM-DD'),
    (108, 'HH24:MI:SS'),
    (126, 'YYYY-MM-DD"T"HH24:MI:SS.MS'),
])
def test_a_convert_style_is_written_with_to_char(ms_sql, style, expected):
    """
    The style is what the value looks like. Dropping it wrote the ISO notation into every row
    which took the default, whatever the source asked for.
    """
    converted = convert(ms_sql, f'(CONVERT([varchar](30),getdate(),({style})))', 'VARCHAR(30)')
    assert converted == f"CAST(to_char(current_timestamp, '{expected}') AS VARCHAR(30))"
    assert ms_sql.config_parser.levels('WARNING') == []


def test_the_two_digit_year_styles_are_not_the_four_digit_ones(ms_sql):
    """Style 3 is dd/mm/yy and style 103 is dd/mm/yyyy - one century apart."""
    assert "'DD/MM/YY'" in convert(ms_sql, '(CONVERT([varchar](8),getdate(),(3)))', 'VARCHAR(8)')
    assert "'DD/MM/YYYY'" in convert(ms_sql, '(CONVERT([varchar](10),getdate(),(103)))', 'VARCHAR(10)')


def test_the_cast_is_kept_around_the_styled_value(ms_sql):
    """Transact-SQL truncates the styled value to the length of the target type."""
    converted = convert(ms_sql, '(CONVERT([varchar](4),getdate(),(112)))', 'VARCHAR(4)')
    assert converted.endswith('AS VARCHAR(4))')


def test_a_style_which_reads_a_string_into_a_date_is_read_the_same_way(ms_sql):
    """The other direction: there the style says how the string is to be READ."""
    assert convert(ms_sql, "(CONVERT([date],'01/04/2020',(103)))", 'DATE') == \
        "to_date('01/04/2020', 'DD/MM/YYYY')"
    assert convert(ms_sql, "(CONVERT([datetime],'01/04/2020',(103)))", 'TIMESTAMP') == \
        "to_timestamp('01/04/2020', 'DD/MM/YYYY')::TIMESTAMP"


@pytest.mark.parametrize('style', [0, 100, 9, 109, 22, 130, 131])
def test_a_style_no_single_format_can_write_is_reported_and_not_guessed(ms_sql, style):
    """
    Transact-SQL pads the hour of these with a space - `Aug 24 2026  9:30AM` - and PostgreSQL
    either pads it with a zero (HH12) or not at all (FMHH12). Writing one of them anyway would
    be a conversion which is nearly right, which is the kind this repository does not make.
    """
    converted = convert(ms_sql, f'(CONVERT([varchar](30),getdate(),({style})))', 'VARCHAR(30)')
    assert converted == 'CAST(current_timestamp AS VARCHAR(30))'
    written = ms_sql.config_parser.levels('WARNING')
    assert len(written) == 1
    assert str(style) in written[0]
    assert 'ISO notation' in written[0]


def test_a_style_nobody_knows_is_reported(ms_sql):
    convert(ms_sql, '(CONVERT([varchar](30),getdate(),(999)))', 'VARCHAR(30)')
    written = ms_sql.config_parser.levels('WARNING')
    assert len(written) == 1
    assert '999' in written[0]


def test_a_style_on_a_type_which_is_not_a_date_is_reported(ms_sql):
    """The style numbers of BINARY and MONEY are a different table and mean something else."""
    converted = convert(ms_sql, '(CONVERT([int],(1.5),(1)))', 'INTEGER')
    assert converted == 'CAST((1.5) AS INTEGER)'
    assert 'not a text, date or timestamp type' in ms_sql.config_parser.written()


def test_a_convert_without_a_style_is_still_a_plain_cast(ms_sql):
    assert convert(ms_sql, '(CONVERT([varchar](30),getdate()))', 'VARCHAR(30)') == \
        'CAST(current_timestamp AS VARCHAR(30))'
    assert ms_sql.config_parser.messages == []


def test_a_convert_inside_a_larger_default_is_converted_where_it_stands(ms_sql):
    converted = convert(ms_sql, "('x' + CONVERT([varchar](8),getdate(),(112)))", 'VARCHAR(40)')
    assert "to_char(current_timestamp, 'YYYYMMDD')" in converted


def test_the_ordinary_ms_sql_defaults_are_unharmed(ms_sql):
    assert convert(ms_sql, '(getdate())', 'TIMESTAMP') == 'current_timestamp'
    assert convert(ms_sql, '(newid())', 'UUID') == 'gen_random_uuid()'
    assert convert(ms_sql, "(N'unknown')", 'VARCHAR(20)') == "'unknown'"
    assert convert(ms_sql, '((0))', 'INTEGER') == '0'


# --------------------------------------------------------------------------------------
# SQL Anywhere


@pytest.fixture
def sql_anywhere():
    return connector_for('sql_anywhere', 'SQLAnywhereConnector')


def test_a_double_quoted_token_is_a_string_and_not_a_column(sql_anywhere):
    """
    The DEFAULT of SQL Anywhere cannot reference a column - its grammar allows a special
    value, a string, a number, a constant expression, AUTOINCREMENT or NULL, and a constant
    expression there must not reference a database object. So a double-quoted token in one
    was written by a database whose quoted_identifier option was off, and it is a string.
    """
    assert convert(sql_anywhere, '"ACTIVE"', 'VARCHAR(10)') == "'ACTIVE'"
    assert convert(sql_anywhere, "'a' || \"b\"", 'VARCHAR(20)') == "'a' || 'b'"
    assert sql_anywhere.config_parser.messages == []


def test_a_constant_expression_is_no_longer_thrown_away(sql_anywhere):
    """
    Anything which still held a double-quoted token after the conversion used to be called a
    column reference and dropped - at INFO, so the default log level did not even show it.
    """
    converted = convert(sql_anywhere, "\"one\" || '-' || \"two\"", 'VARCHAR(40)')
    assert converted == "'one' || '-' || 'two'"
    assert converted is not None


def test_a_single_quote_inside_such_a_string_survives_it(sql_anywhere):
    assert convert(sql_anywhere, '"it\'s here"', 'VARCHAR(20)') == "'it''s here'"


@pytest.mark.parametrize('special,expected', [
    ('current timestamp', 'CURRENT_TIMESTAMP'),
    ('CURRENT DATE', 'CURRENT_DATE'),
    ('current time', 'CURRENT_TIME'),
    ('current user', 'CURRENT_USER'),
    ('current database', 'current_database()'),
])
def test_the_special_values_of_the_grammar_are_converted(sql_anywhere, special, expected):
    assert convert(sql_anywhere, special, 'TIMESTAMP') == expected
    assert sql_anywhere.config_parser.levels('WARNING') == []


@pytest.mark.parametrize('special', ['current utc timestamp', 'utc timestamp'])
def test_the_utc_special_values_are_converted(sql_anywhere, special):
    """They used to be handed to PostgreSQL unchanged, which refuses `DEFAULT current utc timestamp`."""
    assert convert(sql_anywhere, special, 'TIMESTAMP') == "(now() AT TIME ZONE 'UTC')"


def test_a_timestamp_default_says_that_only_its_insert_half_was_migrated(sql_anywhere):
    """
    `DEFAULT TIMESTAMP` sets the column on every INSERT **and every UPDATE**. It used to be
    handed over as the bare word `timestamp`, which PostgreSQL reads as a type name and
    refuses. CURRENT_TIMESTAMP is the half of it a default can express.
    """
    assert convert(sql_anywhere, 'timestamp', 'TIMESTAMP') == 'CURRENT_TIMESTAMP'
    written = sql_anywhere.config_parser.levels('WARNING')
    assert len(written) == 1
    assert 'UPDATE' in written[0]
    assert 'trigger' in written[0]


def test_last_user_says_the_same(sql_anywhere):
    assert convert(sql_anywhere, 'last user', 'VARCHAR(30)') == 'CURRENT_USER'
    assert 'UPDATE' in sql_anywhere.config_parser.levels('WARNING')[0]


@pytest.mark.parametrize('special', ['current publisher', 'current remote user'])
def test_a_special_value_without_a_counterpart_is_reported(sql_anywhere, special):
    """
    CURRENT_USER is used so that a column which had a default keeps one, and the difference
    is reported: the publisher of a SQL Remote setup is not the user who inserted the row.
    """
    assert convert(sql_anywhere, special, 'VARCHAR(30)') == 'CURRENT_USER'
    written = sql_anywhere.config_parser.levels('WARNING')
    assert len(written) == 1
    assert 'not the same value' in written[0]


def test_autoincrement_is_still_the_business_of_the_identity_column(sql_anywhere):
    assert convert(sql_anywhere, 'autoincrement', 'INTEGER') is None
    assert convert(sql_anywhere, 'global autoincrement', 'INTEGER') is None


def test_the_ordinary_sql_anywhere_defaults_are_unharmed(sql_anywhere):
    assert convert(sql_anywhere, "'ACTIVE'", 'VARCHAR(10)') == "'ACTIVE'"
    assert convert(sql_anywhere, '0', 'INTEGER') == '0'
    assert convert(sql_anywhere, 'newid()', 'UUID') == 'gen_random_uuid()'


def test_nothing_is_dropped_at_info_any_more(sql_anywhere):
    """
    A default which disappears is a column full of NULLs where the source put a value, so it
    can never be a message the default log level does not show.
    """
    for value in ('"ACTIVE"', "'a' || \"b\"", 'current timestamp', "'x'"):
        sql_anywhere.config_parser.messages.clear()
        converted = convert(sql_anywhere, value, 'VARCHAR(20)')
        assert converted is not None, f'{value} was dropped'
        assert sql_anywhere.config_parser.levels('INFO') == []
