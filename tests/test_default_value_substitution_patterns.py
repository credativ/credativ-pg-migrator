# SPDX-License-Identifier: GPL-3.0-or-later
"""
The patterns the planner writes into default_values_substitution for every entry of
sql_functions_mapping.

A row of that table replaces the default of a column entirely - the lookup finds the row by
a regular expression and returns the target value in place of the whole default. The pattern
therefore has to describe a default which IS that function; one which merely contains it
must not match, or the rest of the expression is thrown away.

The regular expressions are evaluated by PostgreSQL in the lookup query, not by Python. What
is asserted here is what they are written to mean, with the same constructs in both.

Run with:  python3 -m pytest tests/test_default_value_substitution_patterns.py -v
"""

import os
import re
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.planner import Planner


def pattern_for(function_text):
    return Planner.default_value_pattern_for_function(Planner.__new__(Planner), function_text)


def matches(function_text, default_value):
    """The way the lookup asks: the trimmed default against the pattern of the row."""
    return re.search(pattern_for(function_text), str(default_value).strip()) is not None


# --------------------------------------------------------------------------------------
# a default which is the function - this is what the substitution is for


@pytest.mark.parametrize('default_value', [
    'suser_name()',
    'SUSER_NAME()',
    '  suser_name()  ',
])
def test_a_default_which_is_the_function_is_substituted(default_value):
    assert matches('suser_name()', default_value)


def test_the_parentheses_a_source_writes_around_a_default_are_allowed():
    """MS SQL keeps the default of a column as '(getdate())' - the parentheses are its own."""
    assert matches('getdate()', '(getdate())')
    assert matches('getdate()', '((getdate()))')


def test_a_mapping_which_is_not_a_function_call_works_the_same_way():
    assert matches('sysdate', 'sysdate')
    assert matches('sysdate', 'SYSDATE')


# --------------------------------------------------------------------------------------
# a default which only contains the function - the whole value must not be replaced


def test_a_function_inside_a_larger_expression_is_not_substituted():
    """
    "'[' + suser_name() + '@' + host_name() + ']'" was collapsed to the bare replacement,
    losing the brackets, the '@' and host_name(). Such a default is translated token by
    token by the connector instead.
    """
    assert not matches('suser_name()', "'[' + suser_name() + '@' + host_name() + ']'")


def test_a_function_used_as_an_argument_is_not_substituted():
    assert not matches('getdate()', 'dateadd(day, -1, getdate())')


def test_a_longer_name_which_starts_with_the_mapped_one_is_not_substituted():
    assert not matches('user', 'user_name()')
    assert not matches('user', 'suser_name()')


def test_a_concatenation_of_the_function_with_a_literal_is_not_substituted():
    assert not matches('getdate()', "'run at ' + getdate()")
