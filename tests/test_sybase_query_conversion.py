# SPDX-License-Identifier: GPL-3.0-or-later
"""
The query conversion of Sybase ASE.

It is the source the whole step was designed for and it had no tests of its own. It lives in
`sybase_ase_connector.py` rather than in a module of its own, but it needs no server: the
conversion is a transformation of text, and the connector is built here without connecting.

The heart of it is the outer join. Sybase writes it in the WHERE clause as '*=' and '=*', no
parser reads that, and prepare_query_for_parsing() rewrites it into an equality carrying a
comment marker which the rewrite then turns into a LEFT or RIGHT JOIN. That rewrite had been
written against a model of sqlglot in which the tables behind the comma of a FROM clause stood
in `From.expressions`; they are implicit joins on the SELECT now, so the table was never found
and every '*=' statement went through unconverted with the marker still in it. It goes through
the shared module of query_conversion/outer_joins.py now, which is the one Oracle and SQL
Anywhere use.

Run with:  python3 -m pytest tests/test_sybase_query_conversion.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.connectors.sybase_ase_connector import SybaseASEConnector
from credativ_pg_migrator.query_conversion import classifier


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


@pytest.fixture
def sybase():
    """The connector without a driver behind it - nothing here connects to anything."""
    connector = SybaseASEConnector.__new__(SybaseASEConnector)
    connector.config_parser = RecordingLog()
    ## the connector reads the user defined types of the source once and keeps them; an empty
    ## map is a source which has none
    connector._udt_cache = {}
    return connector


def convert(connector, statement, source_schema='dbo', target_schema='tgt'):
    return connector.convert_query_code({
        'query_code': statement,
        'source_schema_name': source_schema,
        'target_schema_name': target_schema,
        'target_db_type': 'postgresql',
        'statement_id': 'test:1',
    })


## ---------------------------------------------------------------- the contract

def test_the_connector_says_it_can_convert_a_statement(sybase):
    assert sybase.query_conversion_supported() is True


def test_the_answer_is_a_dictionary_with_the_four_keys(sybase):
    answer = convert(sybase, 'SELECT id FROM dbo.customer')
    assert set(answer) == {'code', 'converted', 'warnings', 'error'}
    assert answer['converted'] is True
    assert answer['error'] is None


def test_a_statement_which_cannot_be_parsed_is_not_offered_as_converted(sybase):
    answer = convert(sybase, 'SELECT id FROM dbo.customer WHERE 1 = ')
    assert answer['converted'] is False
    assert answer['code'] == ''
    assert 'could not be parsed' in answer['error']


def test_nothing_is_ever_answered_with_the_text_it_was_given(sybase):
    """The rule of FAKE_CONVERSIONS_AND_SILENT_SKIPS: a failure never looks like a conversion."""
    broken = 'SELECT id FROM dbo.customer WHERE 1 = '
    answer = convert(sybase, broken)
    assert answer['code'] != broken
    assert answer['code'] == ''


## ---------------------------------------------------------------- the outer joins

def test_the_outer_join_of_the_source_becomes_a_left_join(sybase):
    answer = convert(sybase,
                     'SELECT c.id, o.total FROM dbo.customer c, dbo.orders o WHERE c.id *= o.cid')
    assert answer['converted'] is True
    assert 'LEFT JOIN' in answer['code']
    assert '*=' not in answer['code']
    assert 'left_outer' not in answer['code']


def test_the_conditions_which_are_not_the_join_stay_in_the_where_clause(sybase):
    answer = convert(sybase, 'SELECT c.id, o.total FROM dbo.customer c, dbo.orders o '
                             'WHERE c.id *= o.cid AND c.active = 1')
    assert answer['converted'] is True
    assert 'LEFT JOIN' in answer['code']
    assert 'WHERE "c"."active" = 1' in answer['code']


def test_the_true_left_behind_by_the_rewrite_is_taken_out(sybase):
    """"WHERE TRUE AND x" is "WHERE x", and the shorter one is what a developer reads."""
    answer = convert(sybase, 'SELECT c.id FROM dbo.customer c, dbo.orders o '
                             'WHERE c.id *= o.cid AND c.active = 1')
    assert 'WHERE TRUE' not in answer['code'].upper()


def test_the_other_spelling_of_the_outer_join_is_read_too(sybase):
    answer = convert(sybase, 'SELECT c.id, o.total FROM dbo.customer c, dbo.orders o '
                             'WHERE o.cid =* c.id AND c.active = 1')
    assert answer['converted'] is True
    ## '=*' preserves the table the asterisk stands next to - every customer, NULLs from orders
    assert 'LEFT JOIN "tgt"."orders"' in answer['code']


def test_an_outer_join_under_an_or_is_refused_rather_than_answered_wrongly(sybase):
    """
    Moving such a condition into the ON clause makes it an AND of the join and leaves the
    other side of the OR behind. The statement would answer other rows and look healthy.
    """
    answer = convert(sybase, 'SELECT c.id FROM dbo.customer c, dbo.orders o '
                             'WHERE c.id *= o.cid OR c.id = 1')
    assert answer['converted'] is False
    assert 'LEFT JOIN' in answer['error']


def test_the_example_of_the_strategy_converts(sybase):
    """The statement §10.2 of the strategy shows as the output of this step."""
    answer = convert(sybase,
                     'SELECT TOP 100 c.cust_id, c.name, sum(o.total) FROM customer c, orders o '
                     'WHERE c.cust_id *= o.cust_id AND c.active_flag = 1 '
                     'GROUP BY c.cust_id, c.name')
    assert answer['converted'] is True
    assert 'LEFT JOIN' in answer['code']
    assert 'LIMIT 100' in answer['code']
    assert 'GROUP BY' in answer['code']


## ---------------------------------------------------------------- the rest of the dialect

def test_top_becomes_limit(sybase):
    answer = convert(sybase, 'SELECT TOP 10 id FROM dbo.customer')
    assert 'LIMIT 10' in answer['code']
    assert 'TOP' not in answer['code'].upper()


def test_string_concatenation_with_plus_becomes_the_operator_of_the_target(sybase):
    answer = convert(sybase, "SELECT c.name + ' ' + c.surname AS full_name FROM dbo.customer c")
    assert '||' in answer['code']


def test_the_functions_of_the_source_are_mapped(sybase):
    answer = convert(sybase, 'SELECT getdate(), isnull(note, 0), datalength(name) FROM dbo.customer')
    code = answer['code'].lower()
    assert 'current_timestamp' in code
    assert 'coalesce' in code
    assert 'getdate' not in code
    assert 'isnull' not in code


def test_the_source_schema_becomes_the_target_schema(sybase):
    answer = convert(sybase, 'SELECT id FROM dbo.customer')
    assert '"tgt"."customer"' in answer['code']
    assert 'dbo' not in answer['code']


def test_a_statement_without_a_schema_keeps_its_names(sybase):
    """Qualifying it needs the name map of §7.3, which is not implemented - see §17.2 D3."""
    answer = convert(sybase, 'SELECT id FROM customer')
    assert answer['converted'] is True
    assert '"customer"' in answer['code']


def test_a_bind_parameter_carried_as_an_identifier_survives_the_conversion(sybase):
    """
    The workflow replaces '?' by an identifier before the converter sees the statement,
    because every parser reads '$1' as a column and writes it back quoted.
    """
    answer = convert(sybase, 'SELECT id FROM dbo.customer WHERE name LIKE cpgm_bind_param_1')
    assert answer['converted'] is True
    assert 'cpgm_bind_param_1' in answer['code']


## ---------------------------------------------------------------- the gates, in this dialect

@pytest.mark.parametrize('statement, expected', [
    ('SELECT id INTO newtable FROM dbo.customer', 'creates and fills'),
    ('SELECT id FROM dbo.customer HOLDLOCK', 'takes locks'),
    ('UPDATE dbo.customer SET name = 1', 'not a read'),
    ('SELECT id FROM dbo.orders FOR UPDATE', 'row locks'),
])
def test_the_gates_refuse_what_they_have_to_in_this_dialect(statement, expected):
    classification = classifier.classify(statement, 'sybase_ase')
    assert classification.verdict == 'refused'
    assert expected in classification.reason


def test_a_statement_with_an_outer_join_reaches_the_converter(sybase):
    """
    The gates read the statement of the application; the parser reads what
    prepare_query_for_parsing() made of it. A '*=' statement must not be reported as one the
    migrator cannot read - its own connector converts it.
    """
    statement = 'SELECT c.id FROM dbo.customer c, dbo.orders o WHERE c.id *= o.cid'
    parse_text = sybase.prepare_query_for_parsing(statement)
    classification = classifier.classify(statement, 'sybase_ase', parse_text=parse_text)
    assert classification.verdict == 'select', classification.reason
