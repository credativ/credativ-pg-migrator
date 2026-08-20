# SPDX-License-Identifier: GPL-3.0-or-later
"""
migration.varchar_to_text_length and migration.char_to_text_length - which string columns
are migrated as TEXT instead of keeping their length.

The two settings are independent: a varchar column is decided by the first and a char
column by the second. That is easy to get wrong because 'CHAR' is a substring of 'VARCHAR',
and because both settings default to -1, which compares true against every length.

Run with:  python3 -m pytest tests/test_varchar_to_text.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.planner import Planner


class ConfigStub:
    def __init__(self, varchar_to_text_length=-1, char_to_text_length=-1):
        self.varchar_to_text_length = varchar_to_text_length
        self.char_to_text_length = char_to_text_length

    def get_varchar_to_text_length(self):
        return self.varchar_to_text_length

    def get_char_to_text_length(self):
        return self.char_to_text_length

    def print_log_message(self, level, message):
        pass


class SourceStub:
    STRING_TYPES = ('CHAR', 'VARCHAR', 'TEXT', 'CLOB', 'STRING')

    def is_string_type(self, coltype):
        return any(name in (coltype or '').upper() for name in self.STRING_TYPES)


def planner(varchar_to_text_length=-1, char_to_text_length=-1):
    instance = Planner.__new__(Planner)
    instance.config_parser = ConfigStub(varchar_to_text_length, char_to_text_length)
    instance.source_connection = SourceStub()
    return instance


# --------------------------------------------------------------------------------------
# nothing configured - the columns keep what they are


@pytest.mark.parametrize('coltype', ['VARCHAR', 'CHAR', 'NUMERIC'])
def test_without_the_settings_a_column_keeps_its_type(coltype):
    assert planner().promote_string_type_to_text(coltype, 4000) == coltype


# --------------------------------------------------------------------------------------
# one setting must not decide for the other family


def test_a_varchar_is_not_promoted_by_the_char_setting():
    """
    'CHAR' is a substring of 'VARCHAR'. With only char_to_text_length configured, every
    varchar column - univarchar and nvarchar of Sybase ASE among them, which are mapped to
    VARCHAR - fell through to the char branch and became TEXT.
    """
    assert planner(char_to_text_length=10).promote_string_type_to_text('VARCHAR', 255) == 'VARCHAR'


def test_a_char_is_not_promoted_by_the_varchar_setting():
    assert planner(varchar_to_text_length=10).promote_string_type_to_text('CHAR', 255) == 'CHAR'


# --------------------------------------------------------------------------------------
# each setting decides its own family


@pytest.mark.parametrize('coltype', ['VARCHAR', 'NVARCHAR', 'UNIVARCHAR'])
def test_a_long_varchar_is_promoted(coltype):
    assert planner(varchar_to_text_length=100).promote_string_type_to_text(coltype, 4000) == 'TEXT'


@pytest.mark.parametrize('coltype', ['VARCHAR', 'NVARCHAR', 'UNIVARCHAR'])
def test_a_short_varchar_keeps_its_length(coltype):
    assert planner(varchar_to_text_length=100).promote_string_type_to_text(coltype, 20) == coltype


def test_a_long_char_is_promoted():
    assert planner(char_to_text_length=100).promote_string_type_to_text('CHAR', 4000) == 'TEXT'


def test_a_short_char_keeps_its_length():
    assert planner(char_to_text_length=100).promote_string_type_to_text('CHAR', 20) == 'CHAR'


def test_the_limit_itself_is_promoted():
    """The setting is the length at which a column becomes TEXT, inclusive."""
    assert planner(varchar_to_text_length=100).promote_string_type_to_text('VARCHAR', 100) == 'TEXT'


def test_both_settings_together_decide_each_family_on_its_own():
    instance = planner(varchar_to_text_length=1000, char_to_text_length=10)
    assert instance.promote_string_type_to_text('VARCHAR', 255) == 'VARCHAR'
    assert instance.promote_string_type_to_text('CHAR', 255) == 'TEXT'


# --------------------------------------------------------------------------------------
# a column the source reports no length for


def test_a_column_without_a_length_is_text():
    assert planner().promote_string_type_to_text('VARCHAR', -1) == 'TEXT'
    assert planner().promote_string_type_to_text('CLOB', -1) == 'TEXT'


def test_a_type_which_is_not_a_string_is_never_promoted():
    assert planner(varchar_to_text_length=0).promote_string_type_to_text('NUMERIC', 10) == 'NUMERIC'
    assert planner().promote_string_type_to_text('NUMERIC', -1) == 'NUMERIC'
