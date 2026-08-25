# SPDX-License-Identifier: GPL-3.0-or-later
"""
The '*=' and '=*' outer joins of the Transact-SQL family, for both connectors and both paths.

Sybase ASE and MS SQL Server are one dialect family and wrote the same operator, so they are
held to the same answers here: every case runs against both connectors, and against the view
path as well as the query path. The conversion is shared - the marking in
`prepare_query_for_parsing()` and the rewrite in `query_conversion/outer_joins.py` - and this
file is what keeps them from drifting apart again.

Three things decide whether such a conversion is right, and only the first is obvious:

  * the marked equality becomes a LEFT or a RIGHT JOIN. The asterisk stands next to the table
    whose rows are kept: 'a.x *= b.y' keeps every row of a.

  * a WHERE condition which restricts the *inner* table has to move into the ON clause. In
    this family such a condition belongs to the join; in PostgreSQL, standing in the WHERE
    clause, it is applied to the result of the join and throws away exactly the rows the outer
    join added - the LEFT JOIN is an inner join again, the statement is valid, and it answers
    fewer rows without looking wrong.

  * 'AND inner.col IS NULL' must NOT move. It is how this family writes "the rows which have
    no match", it is answered after the join, and inside an ON clause it is never true.

And what cannot be attributed is refused rather than converted, in both paths - the alternative
is the comma join it started from, which is an inner join that PostgreSQL accepts.

Nothing here connects to anything.

Run with:  python3 -m pytest tests/test_tsql_outer_joins.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.connectors.ms_sql_connector import MsSQLConnector
from credativ_pg_migrator.connectors.sql_anywhere_query_conversion import SqlAnywhereQueryConversion
from credativ_pg_migrator.connectors.sybase_ase_connector import SybaseASEConnector
from credativ_pg_migrator.database_connector import DatabaseConnector
from credativ_pg_migrator.query_conversion import classifier
from credativ_pg_migrator.query_conversion import outer_joins


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


def build_ms_sql():
    connector = MsSQLConnector.__new__(MsSQLConnector)
    connector.config_parser = RecordingLog()
    connector._udt_map_cache = {}
    return connector


def build_sybase():
    connector = SybaseASEConnector.__new__(SybaseASEConnector)
    connector.config_parser = RecordingLog()
    connector._udt_cache = {}
    return connector


class SqlAnywhereConversion(SqlAnywhereQueryConversion, DatabaseConnector):
    """The conversion with the base connector behind it and no driver in front of it."""

    def __init__(self):
        self.config_parser = RecordingLog()


SqlAnywhereConversion.__abstractmethods__ = frozenset()


def build_sql_anywhere():
    return SqlAnywhereConversion()


## SQL Anywhere writes '*=' as the Transact-SQL compatibility syntax of Sybase ASE and reads it
## the way ASE does, so it is held to the same answers as the other two
CONNECTORS = [('ms_sql', build_ms_sql), ('sybase_ase', build_sybase),
              ('sql_anywhere', build_sql_anywhere)]
BOTH = pytest.mark.parametrize('name, build', CONNECTORS, ids=[case[0] for case in CONNECTORS])


def plain(sql):
    """
    The statement without the quoting each connector chooses for itself.

    The three do not agree about quoting identifiers or about what to do with the schema of
    the source, and none of that is what this file is about: the question here is which
    condition ends up in the ON clause and which stays in the WHERE clause, and that is the
    same question in every one of them.
    """
    return ' '.join((sql or '').replace('"', '').split())


def convert_query(connector, statement):
    answer = connector.convert_query_code({
        'query_code': statement,
        'source_schema_name': 'dbo',
        'target_schema_name': 'tgt',
        'target_db_type': 'postgresql',
        'statement_id': 'test:1',
    })
    answer['plain'] = plain(answer.get('code'))
    return answer


def convert_view(connector, body, name):
    """
    The view path of each connector, given what that connector's view path takes.

    They are not uniform about it - §2.1 of the strategy measured it: ms_sql and sybase_ase are
    given the query alone, sql_anywhere the whole CREATE VIEW text - so the test gives each of
    them the shape it reads rather than pretending they agree.
    """
    view_code = f'CREATE VIEW v_test AS {body}' if name == 'sql_anywhere' else body
    return plain(connector.convert_view_code({
        'view_code': view_code,
        'source_schema_name': 'dbo',
        'target_schema_name': 'tgt',
        'target_view_name': 'v_test',
        'target_db_type': 'postgresql',
        'view_type': 'VIEW',
    }))


## ---------------------------------------------------------------- the join itself

@BOTH
def test_star_equals_becomes_a_left_join(name, build):
    answer = convert_query(build(), 'SELECT c.id, o.total FROM customer c, orders o '
                                    'WHERE c.id *= o.cid')
    assert answer['converted'] is True, answer['error']
    assert 'LEFT JOIN' in answer['plain']
    assert 'ON c.id = o.cid' in answer['plain']


@BOTH
def test_equals_star_keeps_the_rows_of_the_table_the_asterisk_names(name, build):
    """'c.id =* o.cid' keeps every row of o - so o is preserved and c is null-supplying."""
    answer = convert_query(build(), 'SELECT c.id FROM customer c, orders o '
                                    'WHERE c.id =* o.cid')
    assert answer['converted'] is True, answer['error']
    assert 'RIGHT JOIN' in answer['plain']
    assert 'orders' in answer['plain'].split('RIGHT JOIN')[1]


@BOTH
def test_the_operator_never_survives_the_conversion(name, build):
    answer = convert_query(build(), 'SELECT c.id FROM customer c, orders o '
                                    'WHERE c.id *= o.cid')
    assert '*=' not in answer['plain']
    assert 'left_outer' not in answer['plain']
    assert 'right_outer' not in answer['plain']


@BOTH
def test_two_outer_joins_in_one_statement(name, build):
    answer = convert_query(build(), 'SELECT c.id FROM customer c, orders o, items i '
                                    'WHERE c.id *= o.cid AND o.id *= i.oid')
    assert answer['converted'] is True, answer['error']
    assert answer['plain'].count('LEFT JOIN') == 2


@BOTH
def test_the_true_left_where_the_condition_stood_is_taken_out(name, build):
    answer = convert_query(build(), 'SELECT c.id FROM customer c, orders o '
                                    'WHERE c.id *= o.cid AND c.active = 1')
    assert 'TRUE' not in answer['plain'].upper()


## ---------------------------------------------------------------- which side the condition lands on

@BOTH
def test_a_condition_on_the_inner_table_moves_into_the_on_clause(name, build):
    """
    Left in the WHERE clause it would undo the outer join: PostgreSQL applies it to the result
    of the join, where the NULL-extended rows do not satisfy it.
    """
    answer = convert_query(build(), "SELECT c.id FROM customer c, orders o "
                                    "WHERE c.id *= o.cid AND o.status = 'X'")
    assert answer['converted'] is True, answer['error']
    assert "ON c.id = o.cid AND o.status = 'X'" in answer['plain']
    assert 'WHERE' not in answer['plain']


@BOTH
def test_a_condition_on_the_outer_table_stays_in_the_where_clause(name, build):
    answer = convert_query(build(), 'SELECT c.id FROM customer c, orders o '
                                    'WHERE c.id *= o.cid AND c.active = 1')
    assert answer['converted'] is True, answer['error']
    assert 'WHERE c.active = 1' in answer['plain']


@BOTH
def test_both_kinds_of_condition_at_once_land_where_they_belong(name, build):
    answer = convert_query(build(), "SELECT c.id FROM customer c, orders o "
                                    "WHERE c.id *= o.cid AND o.status = 'X' AND c.active = 1")
    code = answer['plain']
    assert "o.status = 'X'" in code.split('WHERE')[0]
    assert 'c.active = 1' in code.split('WHERE')[1]


@BOTH
def test_a_test_for_null_on_the_inner_table_does_not_move(name, build):
    """
    'AND o.cid IS NULL' after an outer join is how this family writes "customers with no
    order". Inside the ON clause it is never true and the statement would answer no rows.
    """
    answer = convert_query(build(), 'SELECT c.id FROM customer c, orders o '
                                    'WHERE c.id *= o.cid AND o.cid IS NULL')
    assert answer['converted'] is True, answer['error']
    assert 'WHERE o.cid IS NULL' in answer['plain']


@BOTH
def test_a_condition_reading_two_tables_does_not_move(name, build):
    """It is not a restriction on the inner table, so nothing may be decided about it."""
    answer = convert_query(build(), 'SELECT c.id FROM customer c, orders o '
                                    'WHERE c.id *= o.cid AND o.total > c.credit_limit')
    assert 'WHERE' in answer['plain']
    assert 'o.total > c.credit_limit' in answer['plain'].split('WHERE')[1]


@BOTH
def test_a_parenthesised_or_on_the_inner_table_keeps_its_parentheses(name, build):
    """Without them the ON clause reads "a = b AND x = 'X' OR x = 'Y'", which is another condition."""
    answer = convert_query(build(), "SELECT c.id FROM customer c, orders o "
                                    "WHERE c.id *= o.cid AND (o.status = 'X' OR o.status = 'Y')")
    assert answer['converted'] is True, answer['error']
    assert "AND (o.status = 'X' OR o.status = 'Y')" in answer['plain']


@BOTH
def test_an_or_left_in_the_where_clause_keeps_its_parentheses_too(name, build):
    answer = convert_query(build(), "SELECT c.id FROM customer c, orders o "
                                    "WHERE c.id *= o.cid AND (c.a = 1 OR c.b = 2) AND o.s = 'X'")
    assert answer['converted'] is True, answer['error']
    assert 'WHERE (c.a = 1 OR c.b = 2)' in answer['plain']


@BOTH
def test_a_join_written_as_ansi_in_the_source_is_left_alone(name, build):
    """
    A developer who wrote a LEFT JOIN and a WHERE clause meant both of them: the two dialects
    agree about that statement, and moving the condition would change what it answers.
    """
    answer = convert_query(build(), "SELECT c.id FROM customer c "
                                    "LEFT JOIN orders o ON c.id = o.cid WHERE o.status = 'X'")
    assert answer['converted'] is True, answer['error']
    assert "WHERE o.status = 'X'" in answer['plain']


@BOTH
def test_an_ansi_join_beside_a_legacy_one_keeps_its_own_where_clause(name, build):
    answer = convert_query(build(),
                           "SELECT c.id FROM customer c, orders o "
                           "LEFT JOIN items i ON o.id = i.oid "
                           "WHERE c.id *= o.cid AND o.status = 'X' AND i.kind = 'K'")
    assert answer['converted'] is True, answer['error']
    ## the legacy join took its condition into the ON clause, the ANSI one kept its WHERE
    assert "o.status = 'X'" in answer['plain'].split('WHERE')[0]
    assert "i.kind = 'K'" in answer['plain'].split('WHERE')[1]


@BOTH
def test_a_statement_without_an_outer_join_is_not_touched(name, build):
    answer = convert_query(build(), "SELECT c.id FROM customer c, orders o "
                                    "WHERE c.id = o.cid AND o.status = 'X'")
    assert answer['converted'] is True, answer['error']
    assert 'JOIN' not in answer['plain']
    assert "o.status = 'X'" in answer['plain'].split('WHERE')[1]


## ---------------------------------------------------------------- what is said about it

@BOTH
def test_a_moved_condition_is_reported(name, build):
    """The move changes which rows the statement answers, so it is never silent."""
    answer = convert_query(build(), "SELECT c.id FROM customer c, orders o "
                                    "WHERE c.id *= o.cid AND o.status = 'X'")
    assert answer['warnings']
    assert any('inner table of an outer join' in warning for warning in answer['warnings'])


@BOTH
def test_nothing_is_reported_when_nothing_moved(name, build):
    answer = convert_query(build(), 'SELECT c.id FROM customer c, orders o '
                                    'WHERE c.id *= o.cid AND c.active = 1')
    assert not answer['warnings']


## ---------------------------------------------------------------- what is refused

@BOTH
def test_a_marked_condition_under_an_or_is_refused(name, build):
    """
    Moving it into the ON clause makes it an AND of the join and leaves the other side of the
    OR behind - the statement would answer other rows.
    """
    answer = convert_query(build(), 'SELECT c.id FROM customer c, orders o '
                                    'WHERE c.id *= o.cid OR c.id = 1')
    assert answer['converted'] is False
    assert answer['code'] == ''
    assert 'LEFT JOIN' in answer['error']


@BOTH
def test_a_refused_outer_join_is_not_reported_as_an_unreadable_statement(name, build):
    """It parsed and it converted; the outer join alone could not be done."""
    answer = convert_query(build(), 'SELECT c.id FROM customer c, orders o '
                                    'WHERE c.id *= o.cid OR c.id = 1')
    assert 'could not be parsed' not in (answer['error'] or '')


@BOTH
def test_the_marker_never_reaches_a_converted_statement(name, build):
    answer = convert_query(build(), 'SELECT c.id FROM customer c, orders o '
                                    'WHERE c.id *= o.cid OR c.id = 1')
    assert 'left_outer' not in (answer['code'] or '')
    assert 'left_outer' not in (answer['error'] or '')


## ---------------------------------------------------------------- the view path

@BOTH
def test_the_view_path_converts_the_outer_join_too(name, build):
    ddl = convert_view(build(), 'SELECT c.id, o.total FROM customer c, orders o '
                                'WHERE c.id *= o.cid', name)
    assert 'LEFT JOIN' in ddl
    assert '*=' not in ddl


@BOTH
def test_the_view_path_moves_the_condition_of_the_inner_table_as_well(name, build):
    ddl = convert_view(build(), "SELECT c.id FROM customer c, orders o "
                                "WHERE c.id *= o.cid AND o.status = 'X'", name)
    assert "ON c.id = o.cid AND o.status = 'X'" in ddl


@BOTH
def test_a_view_never_carries_a_marker_into_the_target(name, build):
    """
    A marker which reached the target would stand in an ordinary comma join with an ordinary
    equality - an INNER join which PostgreSQL creates without complaint and which answers
    fewer rows than the view of the source did. What is handed back instead is the operator
    of the source, which PostgreSQL refuses out loud.
    """
    ddl = convert_view(build(), 'SELECT c.id FROM customer c, orders o '
                                'WHERE c.id *= o.cid OR c.id = 1', name)
    assert 'left_outer' not in ddl
    assert 'right_outer' not in ddl
    assert '*=' in ddl


## ---------------------------------------------------------------- the gates see it as a read

## the dialect the classifier reads each source in, for the gate below
CLASSIFIER_DIALECTS = {'ms_sql': 'mssql', 'sybase_ase': 'sybase_ase', 'sql_anywhere': 'sql_anywhere'}


@BOTH
def test_a_statement_with_an_outer_join_is_classified_as_a_read(name, build):
    """
    The gates read the statement of the application; the parser reads what
    prepare_query_for_parsing() made of it. Without the preparation such a statement is
    reported as one the migrator cannot read, although its own connector converts it.
    """
    connector = build()
    source_db_type = CLASSIFIER_DIALECTS[name]
    statement = 'SELECT c.id FROM customer c, orders o WHERE c.id *= o.cid'
    assert classifier.classify(statement, source_db_type).verdict == 'unparsed'
    prepared = connector.prepare_query_for_parsing(statement)
    assert classifier.classify(statement, source_db_type, parse_text=prepared).verdict == 'select'


## ---------------------------------------------------------------- the shared marking

def test_the_operator_inside_a_string_literal_is_text():
    masked = lambda code: ''.join(' ' if index in range(code.index("'"), code.rindex("'") + 1)
                                  else character for index, character in enumerate(code))
    code = "SELECT 'a *= b' AS note FROM t"
    assert outer_joins.mark_tsql_outer_joins(code, masked) == code


def test_a_statement_without_the_operator_is_answered_as_it_is():
    code = 'SELECT a FROM t WHERE a = b'
    assert outer_joins.mark_tsql_outer_joins(code) is code


def test_both_operators_are_marked_with_the_side_they_mean():
    marked = outer_joins.mark_tsql_outer_joins('a.x *= b.y AND c.x =* d.y')
    assert 'left_outer' in marked.split('AND')[0]
    assert 'right_outer' in marked.split('AND')[1]


def test_the_compound_assignment_of_ms_sql_is_not_an_outer_join():
    """
    'UPDATE t SET x *= 2' multiplies - MS SQL Server has read '*=' that way since 2008, and
    the conversion of a routine sends its UPDATE statements through the same converter as its
    SELECT statements. Marking it would turn an assignment into a comparison.
    """
    assert outer_joins.mark_tsql_outer_joins('UPDATE t SET x *= 2') == 'UPDATE t SET x *= 2'


def test_an_outer_join_behind_a_set_clause_is_still_an_outer_join():
    marked = outer_joins.mark_tsql_outer_joins('UPDATE t SET x *= 2 WHERE a.i *= b.i')
    assert 'SET x *= 2' in marked
    assert 'a.i = /* left_outer */ b.i' in marked


def test_an_outer_join_in_a_subquery_is_marked():
    marked = outer_joins.mark_tsql_outer_joins(
        'SELECT a FROM t WHERE x = (SELECT max(p) FROM q, r WHERE q.a *= r.b)')
    assert 'q.a = /* left_outer */ r.b' in marked


def test_a_bare_condition_is_read_as_a_condition():
    """A connector may hand in a fragment; there is no SET in front of it."""
    assert 'left_outer' in outer_joins.mark_tsql_outer_joins('x.i *= y.i')
