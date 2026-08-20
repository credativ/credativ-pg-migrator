# SPDX-License-Identifier: GPL-3.0-or-later
"""
The bind parameters an application leaves in its SQL, through the conversion and back.

Two properties are worth more than the rest: a marker inside a string literal is text and
not a parameter, and the ORDER of the parameters has to be the same afterwards or the reader
of the output file has to be told in the plainest words available. A rewrite really moves
them - TOP (?) becomes LIMIT $1 at the other end of the statement - and an application which
binds its values positionally would then put every value in the wrong place.

Run with:  python3 -m pytest tests/test_query_parameters.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.query_conversion.parameters import detect_style, extract


def round_trip(text, style='auto'):
    """Take the markers out and put them back, with nothing in between."""
    parameters, _warnings = extract(text, style)
    restored, warnings = parameters.restore(parameters.statement)
    return restored, warnings


# --------------------------------------------------------------------------------------
# the styles


@pytest.mark.parametrize('text,style,count', [
    ("SELECT a FROM t WHERE id = ? AND name LIKE ?", 'qmark', 2),
    ("SELECT a FROM t WHERE id = :cust", 'named', 1),
    ("SELECT a FROM t WHERE id = @cust", 'at', 1),
    ("SELECT a FROM t WHERE id = %s", 'pyformat', 1),
    ("SELECT a FROM t WHERE id = %(cust)s", 'pyformat', 1),
    ("SELECT a FROM t WHERE id = $1", 'numeric', 1),
    ("SELECT a FROM t WHERE id = 1", 'none', 0),
])
def test_every_style_is_recognised(text, style, count):
    parameters, _warnings = extract(text)
    assert parameters.style == style
    assert parameters.count == count


def test_the_statement_handed_to_postgresql_uses_its_own_numbering():
    parameters, _warnings = extract("SELECT a FROM t WHERE id = ? AND b = ?")
    assert parameters.statement == "SELECT a FROM t WHERE id = $1 AND b = $2"


def test_a_named_parameter_used_twice_is_one_parameter():
    """As it is for the driver which binds it."""
    parameters, _warnings = extract("SELECT a FROM t WHERE id = :cust OR ref = :cust")
    assert parameters.count == 1
    assert parameters.statement == "SELECT a FROM t WHERE id = $1 OR ref = $1"


def test_a_style_which_is_configured_is_not_guessed_at():
    parameters, _warnings = extract("SELECT a FROM t WHERE id = @cust", 'none')
    assert parameters.count == 0


def test_an_unknown_style_is_refused():
    with pytest.raises(ValueError):
        extract("SELECT 1", 'hieroglyphs')


def test_a_file_which_mixes_two_styles_is_reported():
    _parameters, warnings = extract("SELECT a FROM t WHERE id = ? AND b = :cust")
    assert any('more than one kind' in warning for warning in warnings)


# --------------------------------------------------------------------------------------
# what is not a parameter


def test_a_marker_inside_a_string_literal_is_text():
    parameters, _warnings = extract("SELECT 'why? because' FROM t WHERE a = ?")
    assert parameters.count == 1
    assert "'why? because'" in parameters.statement


def test_a_cast_is_not_a_named_parameter():
    parameters, _warnings = extract("SELECT a::text FROM t WHERE id = :cust")
    assert parameters.count == 1
    assert 'a::text' in parameters.statement


def test_a_global_variable_of_the_source_is_not_a_parameter():
    """@@rowcount and @@nestlevel of Sybase ASE and MS SQL Server begin with two @."""
    parameters, _warnings = extract("SELECT @@rowcount AS n FROM t WHERE id = @cust")
    assert parameters.count == 1
    assert '@@rowcount' in parameters.statement


def test_a_database_link_of_oracle_is_not_a_parameter():
    """
    'FROM orders@remote_erp' addresses a table of another database in Oracle. Read as a
    parameter it took the name of the table with it: the statement came out of the
    conversion as 'FROM orderscpgm_bind_param_1'. A parameter of a driver never stands
    directly behind a name.
    """
    parameters, _warnings = extract("SELECT COUNT(*) FROM orders@remote_erp", 'at')
    assert parameters.count == 0
    assert parameters.statement == "SELECT COUNT(*) FROM orders@remote_erp"


def test_a_parameter_behind_an_operator_or_a_parenthesis_is_still_one():
    parameters, _warnings = extract("SELECT a FROM t WHERE b=@p AND c IN (@q)", 'at')
    assert parameters.count == 2


@pytest.mark.parametrize('text', [
    "SELECT '?' FROM t",
    "SELECT a FROM t -- and a ? in a comment",
    "SELECT a FROM t /* and ? here */",
])
def test_a_statement_without_parameters_is_left_alone(text):
    parameters, _warnings = extract(text)
    assert parameters.count == 0
    assert parameters.statement == text
    assert detect_style(text)[0] == 'none'


# --------------------------------------------------------------------------------------
# the round trip


@pytest.mark.parametrize('text', [
    "SELECT a FROM t WHERE id = ? AND name LIKE ?",
    "SELECT a FROM t WHERE id = :cust AND ref = :cust AND x = :other",
    "SELECT a FROM t WHERE id = @cust",
    "SELECT a FROM t WHERE id = %s AND n = %(name)s",
    "SELECT a FROM t WHERE id = $1",
    "SELECT 'a ? inside' FROM t WHERE id = ?",
])
def test_the_markers_come_back_exactly_as_they_were(text):
    restored, warnings = round_trip(text)
    assert restored == text
    assert warnings == []


def test_the_numbered_form_is_kept_when_it_is_asked_for():
    parameters, _warnings = extract("SELECT a FROM t WHERE id = ?")
    restored, _warnings = parameters.restore(parameters.statement, 'numeric')
    assert restored == "SELECT a FROM t WHERE id = $1"


# --------------------------------------------------------------------------------------
# the conversion token, and the order


def test_the_converter_sees_a_name_it_carries_through_unharmed():
    """
    Every converter of this migrator parses the statement, and a parser reads '$1' as a
    column named '$1' and writes it back quoted - "$1" - which is not a parameter any more.
    """
    parameters, _warnings = extract("SELECT a FROM t WHERE id = ?")
    assert '$1' not in parameters.conversion_statement
    assert 'cpgm_bind_param_1' in parameters.conversion_statement


@pytest.mark.parametrize('converted', [
    'SELECT "a" FROM "t" WHERE "id" = cpgm_bind_param_1',
    'SELECT "a" FROM "t" WHERE "id" = "cpgm_bind_param_1"',
    'SELECT "a" FROM "t" WHERE "id" = CPGM_BIND_PARAM_1',
])
def test_the_token_is_recognised_however_the_converter_wrote_it(converted):
    parameters, _warnings = extract("SELECT a FROM t WHERE id = ?")
    assert parameters.to_numbered(converted).endswith('$1')


def test_a_reordering_is_reported_as_blocking():
    """TOP (?) at the front becomes LIMIT $1 at the end - the values would be swapped."""
    parameters, _warnings = extract("SELECT TOP (?) a FROM t WHERE x = ?")
    restored, warnings = parameters.restore('SELECT "a" FROM "t" WHERE "x" = $2 LIMIT $1')
    assert restored == 'SELECT "a" FROM "t" WHERE "x" = ? LIMIT ?'
    assert any(warning.startswith('BLOCKING') and 'order' in warning for warning in warnings)


def test_a_lost_parameter_is_reported_as_blocking():
    parameters, _warnings = extract("SELECT a FROM t WHERE x = ? AND y = ?")
    _restored, warnings = parameters.restore('SELECT "a" FROM "t" WHERE "x" = $1')
    assert any(warning.startswith('BLOCKING') and 'lost' in warning for warning in warnings)


def test_an_unchanged_order_is_not_reported():
    parameters, _warnings = extract("SELECT a FROM t WHERE x = ? AND y = ?")
    _restored, warnings = parameters.restore('SELECT "a" FROM "t" WHERE "x" = $1 AND "y" = $2')
    assert warnings == []


def test_the_description_says_how_many_and_what_they_look_like():
    parameters, _warnings = extract("SELECT a FROM t WHERE x = ? AND y = ?")
    assert parameters.describe() == 'parameters: 2 (?, ?) -> $1..$2'
    parameters, _warnings = extract("SELECT a FROM t")
    assert parameters.describe() == 'parameters: none'
