# SPDX-License-Identifier: GPL-3.0-or-later
"""
The textual gates of the query conversion, and the parts of a statement they may read.

Gate 2 decides from the text, because a statement the parser could not read still has to be
refused when it writes. What it must not do is read a word which stands inside a string
literal or inside a comment: the content of a literal is data which never runs, and a comment
runs even less. Four of the five gates read the whole text and refused correct statements for
a word standing in one of them - "SELECT id FROM customer -- the report for update of the
pricing sheet" was answered with "the statement takes row locks", and the statement was never
converted and never tested.

Every case below stands in both directions: the word inside a literal or a comment must not
refuse, and the same word standing in the SQL must still refuse.

Run with:  python3 -m pytest tests/test_query_gates_literals.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.query_conversion import classifier


## (label, the statement, the word which used to refuse it)
INSIDE_A_LITERAL = [
    ('a sequence named in a literal',
     "SELECT 'next value for the counter' AS note, id FROM orders"),
    ('nextval named in a literal',
     "SELECT id, 'run nextval by hand' AS hint FROM orders"),
    ('a row lock named in a literal',
     "SELECT id, 'locked for update by the night job' AS note FROM orders"),
    ('a lock hint named in a literal',
     "SELECT id, 'holdlock is not used here' AS note FROM orders"),
    ('INTO TEMP named in a literal',
     "SELECT 'copied into temp storage' AS note FROM orders"),
    ('a host variable named in a literal',
     "SELECT 'writes into :hostvar' AS note FROM orders"),
    ('a data change table named in a literal',
     "SELECT 'reads from final table (insert)' AS note FROM orders"),
    ('NOLOCK named in a literal',
     "SELECT id, 'NOLOCK is not used here' AS note FROM orders"),
]

INSIDE_A_COMMENT = [
    ('a row lock named in a line comment',
     "SELECT id, name FROM customer  -- the report for update of the pricing sheet\n WHERE id = 1"),
    ('a lock hint named in a block comment',
     "/* holdlock was removed on 2026-01-02 */ SELECT id FROM orders"),
    ('a sequence named in a line comment',
     "-- the id comes from a nextval on the source\nSELECT id FROM orders"),
    ('INTO TEMP named in a block comment',
     "/* was: SELECT ... INTO TEMP tmp1 */ SELECT id FROM orders"),
    ('a data change table named in a comment',
     "-- rewritten from FROM FINAL TABLE (INSERT ...)\nSELECT id FROM orders"),
    ('a host variable named in a comment',
     "-- the caller reads it INTO :row_count\nSELECT count(*) FROM orders"),
]

STILL_REFUSED = [
    ('a real FOR UPDATE', "SELECT id FROM orders FOR UPDATE", 'row locks'),
    ('a real FOR SHARE', "SELECT id FROM orders FOR SHARE", 'row locks'),
    ('a real holdlock', "SELECT id FROM orders o holdlock", 'takes locks'),
    ('a real updlock', "SELECT id FROM orders WITH (UPDLOCK)", 'takes locks'),
    ('a real nextval', "SELECT seq_order.NEXTVAL FROM dual", 'sequence'),
    ('a real NEXT VALUE FOR', "SELECT NEXT VALUE FOR seq_order", 'sequence'),
    ('a real INTO TEMP', "SELECT a, b FROM orders INTO TEMP tmp1", 'creates and fills'),
    ('a real host variable', "SELECT count(*) INTO :row_count FROM orders", 'host variable'),
    ('a real FINAL TABLE',
     "SELECT order_id FROM FINAL TABLE (INSERT INTO orders (id) VALUES (1))", 'FINAL TABLE'),
]


@pytest.mark.parametrize('label, statement', INSIDE_A_LITERAL, ids=[case[0] for case in INSIDE_A_LITERAL])
def test_a_word_inside_a_string_literal_does_not_refuse(label, statement):
    classification = classifier.classify(statement, 'sybase_ase')
    assert classification.verdict == 'select', f"{label}: {classification.reason}"


@pytest.mark.parametrize('label, statement', INSIDE_A_COMMENT, ids=[case[0] for case in INSIDE_A_COMMENT])
def test_a_word_inside_a_comment_does_not_refuse(label, statement):
    classification = classifier.classify(statement, 'sybase_ase')
    assert classification.verdict == 'select', f"{label}: {classification.reason}"


@pytest.mark.parametrize('label, statement, expected', STILL_REFUSED,
                         ids=[case[0] for case in STILL_REFUSED])
def test_the_same_word_in_the_sql_still_refuses(label, statement, expected):
    classification = classifier.classify(statement, 'sybase_ase')
    assert classification.verdict == 'refused', f"{label} was not refused"
    assert expected in classification.reason


def test_a_literal_holding_nolock_raises_no_warning_either():
    """The NOLOCK warning is a textual test as well and reads the same parts of the statement."""
    classification = classifier.classify("SELECT id, 'NOLOCK' AS note FROM orders", 'mssql')
    assert classification.verdict == 'select'
    assert not any('NOLOCK' in warning for warning in classification.warnings)


def test_a_real_nolock_hint_still_warns():
    classification = classifier.classify("SELECT id FROM orders WITH (NOLOCK)", 'mssql')
    assert classification.verdict == 'select'
    assert any('NOLOCK' in warning for warning in classification.warnings)


def test_the_search_is_given_every_region_of_the_statement():
    """A word behind a literal is still SQL - the scan does not stop at the first literal."""
    statement = "SELECT 'a note' AS note, id FROM orders FOR UPDATE"
    classification = classifier.classify(statement, 'sybase_ase')
    assert classification.verdict == 'refused'
    assert 'row locks' in classification.reason


def test_a_word_boundary_is_decided_against_the_whole_text():
    """
    The search is given the boundaries of the region and not a slice of it, so a name which
    only ends in a refused word is not read as that word.
    """
    classification = classifier.classify("SELECT id FROM orders_for_update_log", 'sybase_ase')
    assert classification.verdict == 'select', classification.reason


## ---------------------------------------------------------------- the functions of unknown effect

def test_a_function_the_migrator_does_not_know_is_named():
    classification = classifier.classify("SELECT my_pricing_func(id) FROM orders", 'postgresql')
    assert classification.verdict == 'select'
    assert any('MY_PRICING_FUNC()' in warning for warning in classification.warnings)
    assert any('never executed' in warning for warning in classification.warnings)


def test_the_ordinary_functions_are_not_named():
    classification = classifier.classify(
        "SELECT upper(name), count(*), coalesce(total, 0) FROM orders GROUP BY 1", 'postgresql')
    assert classification.verdict == 'select'
    assert not classification.warnings


def test_a_function_which_writes_is_still_refused_rather_than_warned():
    classification = classifier.classify("SELECT nextval('s')", 'postgresql')
    assert classification.verdict == 'refused'


## ---------------------------------------------------------------- the names without a schema

def test_a_table_without_a_schema_is_reported():
    classification = classifier.classify_converted(
        'SELECT c.id FROM customer AS c JOIN orders AS o ON c.id = o.cid')
    assert classifier.unqualified_tables(classification.parsed) == ['customer', 'orders']


def test_a_qualified_table_is_not_reported():
    classification = classifier.classify_converted('SELECT id FROM "target_schema"."customer"')
    assert classifier.unqualified_tables(classification.parsed) == []


def test_a_common_table_expression_is_not_a_name_without_a_schema():
    classification = classifier.classify_converted(
        'WITH recent AS (SELECT 1 AS a) SELECT a FROM recent')
    assert classifier.unqualified_tables(classification.parsed) == []


def test_the_kind_of_the_statement_is_recorded():
    assert classifier.classify('SELECT 1', 'postgresql').kind == 'SELECT'
    assert classifier.classify('SELECT a FROM t UNION SELECT b FROM u', 'postgresql').kind == 'UNION'
    assert classifier.classify('VALUES (1)', 'postgresql').kind == 'VALUES'
