# SPDX-License-Identifier: GPL-3.0-or-later
"""
Oracle's '(+)' outer joins, and the one thing which makes them different from every other
dialect this migrator reads.

In Sybase ASE, MS SQL Server and SQL Anywhere the marker sits on the join operator and says
nothing about the other conditions, so which of them belongs to the join has to be inferred -
a restriction on the inner table belongs to it, and that is what
`move_inner_table_predicates()` does.

Oracle writes the marker on the column, condition by condition, and therefore says which of
the two readings it means:

    WHERE c.id = o.cid(+) AND o.status(+) = 'X'   the status belongs to the join
    WHERE c.id = o.cid(+) AND o.status = 'X'      the status is a filter - and it turns the
                                                  outer join into an inner one on Oracle
                                                  exactly as it would on PostgreSQL

So nothing is inferred for Oracle: only what carries a '(+)' moves, and a condition without one
stays in the WHERE clause. Both halves are asserted here, because getting either of them wrong
produces a statement which is valid, looks healthy and answers other rows.

The marker survives into the parsed statement as `join_mark` on the column - even inside a call
- which is what makes the attribution possible at all.

Nothing here connects to anything.

Run with:  python3 -m pytest tests/test_oracle_outer_joins.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.connectors.oracle_query_conversion import OracleQueryConversion
from credativ_pg_migrator.database_connector import DatabaseConnector


class RecordingLog:
    args = None

    def __init__(self):
        self.messages = []

    def print_log_message(self, level, message):
        self.messages.append((level, str(message)))

    def get_target_db_type(self):
        return 'postgresql'

    def get_connectivity(self, direction):
        return {'db_type': 'postgresql'}

    def get_remote_objects_substitution(self):
        return {}


class OracleConversion(OracleQueryConversion, DatabaseConnector):
    def __init__(self):
        self.config_parser = RecordingLog()


OracleConversion.__abstractmethods__ = frozenset()


@pytest.fixture
def oracle():
    return OracleConversion()


def convert(oracle, statement):
    return oracle.convert_query_code({
        'query_code': statement,
        'source_schema_name': 'SCOTT',
        'target_schema_name': 'tgt',
        'target_db_type': 'postgresql',
        'statement_id': 'test:1',
    })


def code_of(oracle, statement):
    answer = convert(oracle, statement)
    assert answer['converted'] is True, answer['error']
    return answer['code']


## ---------------------------------------------------------------- the join itself

def test_the_marker_on_the_right_makes_a_left_join(oracle):
    code = code_of(oracle, 'SELECT c.id, o.total FROM customer c, orders o WHERE c.id = o.cid(+)')
    assert 'LEFT JOIN' in code
    assert '(+)' not in code


def test_the_marker_on_the_left_makes_a_right_join(oracle):
    code = code_of(oracle, 'SELECT c.id FROM customer c, orders o WHERE c.id(+) = o.cid')
    assert 'RIGHT JOIN' in code


## ---------------------------------------------------------------- a marked condition moves

def test_a_marked_equality_against_a_literal_moves_into_the_on_clause(oracle):
    """It carries the marker, so Oracle says it belongs to the join."""
    code = code_of(oracle, "SELECT c.id FROM customer c, orders o "
                           "WHERE c.id = o.cid(+) AND o.status(+) = 'X'")
    assert "ON c.id = o.cid AND o.status = 'X'" in code
    assert 'WHERE' not in code


def test_a_marked_comparison_moves(oracle):
    code = code_of(oracle, 'SELECT c.id FROM customer c, orders o '
                           'WHERE c.id = o.cid(+) AND o.total(+) > 100')
    assert 'ON c.id = o.cid AND o.total > 100' in code


def test_a_marked_in_list_moves(oracle):
    code = code_of(oracle, "SELECT c.id FROM customer c, orders o "
                           "WHERE c.id = o.cid(+) AND o.status(+) IN ('A','B')")
    assert "o.status IN ('A', 'B')" in code.split('WHERE')[0]


def test_a_marker_inside_a_call_is_attributed(oracle):
    """
    'UPPER(o.cid(+))' is not a shape a textual marking can recognise, and it was counted as an
    outer join which could not be rewritten. The parsed statement keeps the marker on the
    column, so the condition can be attributed there.
    """
    code = code_of(oracle, 'SELECT c.id FROM customer c, orders o '
                           'WHERE UPPER(c.id) = UPPER(o.cid(+))')
    assert 'LEFT JOIN' in code
    assert 'ON UPPER(c.id) = UPPER(o.cid)' in code


def test_a_move_is_reported(oracle):
    answer = convert(oracle, "SELECT c.id FROM customer c, orders o "
                             "WHERE c.id = o.cid(+) AND o.status(+) = 'X'")
    assert any("outer join operator '(+)'" in warning for warning in answer['warnings'])


## ---------------------------------------------------------------- an unmarked condition does not

def test_a_condition_without_the_marker_stays_in_the_where_clause(oracle):
    """
    This is the half which must not be inferred. Oracle applies such a condition to the result
    of the join, exactly as PostgreSQL does - the outer join becomes an inner one on both
    sides, and that is what the statement says.
    """
    code = code_of(oracle, "SELECT c.id FROM customer c, orders o "
                           "WHERE c.id = o.cid(+) AND o.status = 'X'")
    assert "WHERE o.status = 'X'" in code
    assert "o.status = 'X'" not in code.split('WHERE')[0]


def test_a_test_for_null_without_the_marker_stays(oracle):
    code = code_of(oracle, 'SELECT c.id FROM customer c, orders o '
                           'WHERE c.id = o.cid(+) AND o.cid IS NULL')
    assert 'WHERE o.cid IS NULL' in code


def test_marked_and_unmarked_conditions_land_on_different_sides(oracle):
    code = code_of(oracle, "SELECT c.id FROM customer c, orders o "
                           "WHERE c.id = o.cid(+) AND o.status(+) = 'X' AND o.kind = 'K'")
    before, after = code.split('WHERE')
    assert "o.status = 'X'" in before
    assert "o.kind = 'K'" in after


def test_a_condition_on_the_outer_table_stays(oracle):
    code = code_of(oracle, 'SELECT c.id FROM customer c, orders o '
                           'WHERE c.id = o.cid(+) AND c.active = 1')
    assert 'WHERE c.active = 1' in code


def test_a_statement_with_no_outer_join_is_not_touched(oracle):
    code = code_of(oracle, "SELECT c.id FROM customer c, orders o "
                           "WHERE c.id = o.cid AND o.status = 'X'")
    assert 'JOIN' not in code
    assert "o.status = 'X'" in code.split('WHERE')[1]


## ---------------------------------------------------------------- what is refused

def test_a_marked_condition_under_an_or_is_refused(oracle):
    """
    Oracle refuses this itself with ORA-01719, so a statement holding it did not run on the
    source either. It is not converted rather than guessed at.
    """
    answer = convert(oracle, 'SELECT c.id FROM customer c, orders o '
                             'WHERE c.id = o.cid(+) OR c.id = 1')
    assert answer['converted'] is False
    assert 'outer join' in answer['error']


def test_a_marker_never_reaches_a_converted_statement(oracle):
    answer = convert(oracle, 'SELECT c.id FROM customer c, orders o '
                             'WHERE c.id = o.cid(+) OR c.id = 1')
    assert '(+)' not in (answer['code'] or '')


## ---------------------------------------------------------------- the view path

def test_the_view_path_converts_and_moves_the_same_way(oracle):
    ddl = oracle.convert_view_code({
        'view_code': "SELECT c.id FROM customer c, orders o "
                     "WHERE c.id = o.cid(+) AND o.status(+) = 'X'",
        'source_schema_name': 'SCOTT', 'target_schema_name': 'tgt',
        'target_view_name': 'v_test', 'target_db_type': 'postgresql', 'view_type': 'VIEW'})
    assert 'LEFT JOIN' in ddl
    assert "ON c.id = o.cid AND o.status = 'X'" in ddl
    assert '(+)' not in ddl


def test_the_view_path_leaves_an_unmarked_condition_where_it_stands(oracle):
    ddl = oracle.convert_view_code({
        'view_code': "SELECT c.id FROM customer c, orders o "
                     "WHERE c.id = o.cid(+) AND o.status = 'X'",
        'source_schema_name': 'SCOTT', 'target_schema_name': 'tgt',
        'target_view_name': 'v_test', 'target_db_type': 'postgresql', 'view_type': 'VIEW'})
    assert "WHERE o.status = 'X'" in ddl
