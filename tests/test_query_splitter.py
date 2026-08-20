# SPDX-License-Identifier: GPL-3.0-or-later
"""
Cutting a file of application SQL into the statements it holds.

text.split(';') is wrong on the first real file: a semicolon inside a string literal, a
comment or a $$ quoted body is not the end of a statement, and files exported from a client
of Sybase ASE or MS SQL Server are separated by GO on a line of its own. Everything here is
about a separator which is NOT one, because that is what silently produces two broken
statements out of one good one.

Run with:  python3 -m pytest tests/test_query_splitter.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.query_conversion.splitter import (
    Statement, has_go_separator, split_statements)


def texts(*arguments, **keywords):
    return [statement.text for statement in split_statements(*arguments, **keywords)]


# --------------------------------------------------------------------------------------
# a separator which is not one


def test_a_semicolon_inside_a_string_literal_does_not_cut():
    statements = texts("SELECT 'a;b' AS x FROM t; SELECT 2")
    assert statements == ["SELECT 'a;b' AS x FROM t", "SELECT 2"]


def test_a_doubled_quote_inside_a_literal_does_not_end_it():
    statements = texts("SELECT 'it''s here; still' FROM t; SELECT 2")
    assert statements == ["SELECT 'it''s here; still' FROM t", "SELECT 2"]


def test_a_semicolon_inside_a_line_comment_does_not_cut():
    statements = texts("SELECT a FROM t -- and ; here\nWHERE b = 1; SELECT 2")
    assert statements[0] == "SELECT a FROM t -- and ; here\nWHERE b = 1"
    assert len(statements) == 2


def test_a_semicolon_inside_a_block_comment_does_not_cut():
    statements = texts("SELECT a /* ; not a separator ; */ FROM t; SELECT 2")
    assert len(statements) == 2


def test_a_semicolon_inside_a_dollar_quoted_body_does_not_cut():
    statements = texts("SELECT $$one; two$$ AS x; SELECT 2")
    assert statements == ["SELECT $$one; two$$ AS x", "SELECT 2"]


def test_a_semicolon_inside_a_quoted_identifier_does_not_cut():
    assert len(texts('SELECT "od;d" FROM t; SELECT 2')) == 2
    assert len(texts('SELECT [od;d] FROM t; SELECT 2')) == 2
    assert len(texts('SELECT `od;d` FROM t; SELECT 2')) == 2


def test_a_dollar_which_is_not_a_quote_is_left_alone():
    """$1 is a bind parameter, not the opening of a quoted body."""
    assert texts("SELECT a FROM t WHERE b = $1; SELECT 2") == [
        "SELECT a FROM t WHERE b = $1", "SELECT 2"]


# --------------------------------------------------------------------------------------
# GO, the separator of the Sybase ASE and MS SQL Server clients


def test_go_on_its_own_line_cuts():
    assert texts("select 1 from a\ngo\nselect 2 from b\n") == ["select 1 from a", "select 2 from b"]


def test_go_with_a_repeat_count_cuts():
    assert texts("select 1\nGO 3\nselect 2\n") == ["select 1", "select 2"]


def test_go_inside_a_statement_is_not_a_separator():
    """'go' is a perfectly good column or alias name."""
    assert texts("select go from t\n") == ["select go from t"]
    assert texts("select 'go' as x\n") == ["select 'go' as x"]


def test_the_file_is_recognised_as_using_go():
    assert has_go_separator("select 1\ngo\nselect 2\n")
    assert not has_go_separator("select 1;\nselect 2;\n")


def test_auto_takes_both_separators():
    assert texts("select 1; select 2\ngo\nselect 3") == ["select 1", "select 2", "select 3"]


def test_only_the_configured_separator_cuts():
    assert texts("select 1; select 2\ngo\nselect 3", 'semicolon') == [
        "select 1", "select 2\ngo\nselect 3"]
    assert texts("select 1; select 2\ngo\nselect 3", 'go') == ["select 1; select 2", "select 3"]


# --------------------------------------------------------------------------------------
# the other ways of cutting a file


def test_whole_file_is_one_statement():
    assert texts("select 1\n\nselect 2\n", 'whole_file') == ["select 1\n\nselect 2"]


def test_a_blank_line_cuts_when_it_is_configured_to():
    assert texts("select 1\n\nselect 2\n", 'blank_line') == ["select 1", "select 2"]


def test_a_blank_line_inside_a_literal_does_not_cut():
    assert len(texts("select 'a\n\nb' from t\n\nselect 2", 'blank_line')) == 2


def test_an_unknown_separator_is_refused():
    with pytest.raises(ValueError):
        split_statements("select 1", 'every_second_line')


# --------------------------------------------------------------------------------------
# what a statement carries with it


def test_the_lines_of_the_statement_are_recorded():
    statements = split_statements("select 1;\n\nselect\n  2\n;", input_file='q.sql')
    assert (statements[0].line_from, statements[0].line_to) == (1, 1)
    assert (statements[1].line_from, statements[1].line_to) == (3, 4)
    assert statements[1].location == 'q.sql:3-4'


def test_the_name_written_above_a_statement_is_taken():
    statements = split_statements("-- name: daily_sales\nselect 1;\nselect 2;")
    assert statements[0].name == 'daily_sales'
    assert statements[1].name is None


def test_a_comment_which_is_not_a_name_is_not_one():
    statements = split_statements("-- the daily sales\nselect 1;")
    assert statements[0].name is None


def test_the_hash_ignores_the_formatting_and_nothing_else():
    one = Statement("select  a\n  from t", 1, 1, 2)
    same = Statement("select a from t", 1, 1, 1)
    other = Statement("select b from t", 1, 1, 1)
    assert one.sha256 == same.sha256
    assert one.sha256 != other.sha256


# --------------------------------------------------------------------------------------
# what files really look like


def test_crlf_and_a_byte_order_mark_are_read_like_anything_else():
    statements = split_statements("﻿select 1;\r\nselect 2;\r\n")
    assert statements[0].text == 'select 1'
    assert statements[1].text == 'select 2'
    assert '\r' not in statements[1].text


def test_an_empty_file_holds_no_statement():
    assert texts('') == []
    assert texts('\n\n   \n') == []
    assert texts(';;;') == []


def test_a_trailing_separator_does_not_add_an_empty_statement():
    assert texts("select 1;\n") == ["select 1"]


def test_the_statements_are_numbered_in_the_order_they_stand_in():
    statements = split_statements("select 1; select 2; select 3")
    assert [statement.ordinal for statement in statements] == [1, 2, 3]
