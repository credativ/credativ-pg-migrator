# SPDX-License-Identifier: GPL-3.0-or-later
"""
remote_objects_substitution - one contract for the value, checked at startup, and a record of
what it really replaced.

The option is deprecated. It is a plain search and replace over the whole statement: it rewrites
a name inside a string literal or a comment, it matches a substring rather than a name, and the
outcome depends on the order the entries are written in. None of that is repaired here - the
design which would is in development/REMOTE_OBJECTS_SUBSTITUTION.md. What is held in this file
is the two repairs which stand on their own:

  - **one contract.** The accessor answered whatever the YAML happened to hold: the default was a
    DICT while the schema documents a list of pairs, so a configuration written as a mapping
    worked in the query conversion and stopped the planner with "too many values to unpack".
    It answers a list of (source, target) pairs now, always, and a shape it cannot read is
    refused at startup with a message saying how to write it.

  - **a record of what fired.** Four of the five places the substitution is applied replaced
    silently, so a view was created reading a different table than its text names and nothing in
    the run said so - not the log, not the protocol, not the summary. Every replacement is now
    recorded with the object it was made in, written to the protocol table
    `remote_objects_applied`, and counted in the summary.

Nothing here connects to anything.

Run with:  python3 -m pytest tests/test_remote_objects_substitution.py -v
"""

import ast
import json
import os
import re
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.config_parser import ConfigParser

SCHEMA_PATH = os.path.join(REPO, 'credativ_pg_migrator', 'config.schema.json')


class Args:
    config = None
    log_level = 'INFO'


def parser(value):
    made = ConfigParser.__new__(ConfigParser)
    made.args = Args()
    made.object_filter_counters = {}
    made.remote_substitutions_applied = []
    made.messages = []
    made.print_log_message = lambda level, message: made.messages.append((level, str(message)))
    made.config = {} if value is None else {'remote_objects_substitution': value}
    return made


def warnings_of(made):
    return [message for level, message in made.messages if level == 'WARNING']


## ------------------------------------------------------------------ one contract


@pytest.mark.parametrize('written,expected', [
    ([['a.b', 'c.d']],              [('a.b', 'c.d')]),
    ([('a.b', 'c.d')],              [('a.b', 'c.d')]),
    ({'a.b': 'c.d'},                [('a.b', 'c.d')]),
    ([],                            []),
    (None,                          []),
])
def test_the_accessor_always_answers_a_list_of_pairs(written, expected):
    assert parser(written).get_remote_objects_substitution() == expected


def test_the_shape_which_used_to_stop_the_planner_is_readable():
    """
    `for source, target in get_remote_objects_substitution()` is what the planner does. Written
    as a mapping the accessor used to answer a dict, and unpacking its keys raised
    'too many values to unpack' - in the planner, on every run, while the same file worked in
    the query conversion.
    """
    pairs = parser({'otherdb..customers': 'legacy.customers'}).get_remote_objects_substitution()
    for source, target in pairs:
        assert (source, target) == ('otherdb..customers', 'legacy.customers')


## --------------------------------------------------------------- checked at startup


@pytest.mark.parametrize('written,expected_in_message', [
    ({'a': 'b'},        'has to be a list'),
    ('a,b',             'must be a list'),
    ([['a']],           'exactly two elements'),
    ([['a', 'b', 'c']], 'exactly two elements'),
    ([['', 'x']],       'empty source'),
    ([['  ', 'x']],     'empty source'),
    ([['x', '']],       'empty target'),
])
def test_a_shape_which_cannot_be_read_is_refused(written, expected_in_message):
    with pytest.raises(ValueError) as raised:
        parser(written).validate_remote_objects_substitution()
    assert expected_in_message in str(raised.value)


def test_the_refusal_of_a_mapping_shows_how_to_write_it():
    with pytest.raises(ValueError) as raised:
        parser({'a': 'b'}).validate_remote_objects_substitution()
    assert 'remote_objects_substitution:' in str(raised.value)
    assert '- [' in str(raised.value)


def test_an_empty_source_is_refused_because_it_would_match_everywhere():
    with pytest.raises(ValueError) as raised:
        parser([['', 'x']]).validate_remote_objects_substitution()
    assert 'every position' in str(raised.value)


def test_a_configuration_without_the_key_says_nothing():
    made = parser(None)
    assert made.validate_remote_objects_substitution() is True
    assert made.messages == []


## ------------------------------------------------------------------- deprecation


def test_a_configuration_which_uses_it_is_told_that_it_is_deprecated():
    made = parser([['otherdb..t', 'legacy.t']])
    made.validate_remote_objects_substitution()
    written = ' '.join(warnings_of(made))
    assert 'DEPRECATED' in written
    assert 'remote_objects_applied' in written, 'the warning has to say where to check what it did'


def test_the_deprecation_warning_says_the_migrated_database_needs_no_entry():
    made = parser([['otherdb..t', 'legacy.t']])
    made.validate_remote_objects_substitution()
    assert 'MIGRATED' in ' '.join(warnings_of(made))


def test_the_schema_marks_it_deprecated():
    schema = json.load(open(SCHEMA_PATH, encoding='utf-8'))
    node = schema['properties']['remote_objects_substitution']
    assert node.get('deprecated') is True
    assert 'string literal' in node['description'], 'the description has to say what it does to a literal'


def test_the_generated_reference_shows_the_deprecation():
    reference = open(os.path.join(REPO, 'docs', 'config_reference.md'), encoding='utf-8').read()
    row = [line for line in reference.splitlines()
           if line.startswith('| `remote_objects_substitution`')]
    assert row and '**deprecated**' in row[0]


## --------------------------------------- the shapes whose outcome depends on the order


def test_two_entries_with_the_same_source_are_reported():
    made = parser([['a.b', 'c'], ['A.B', 'd']])
    made.validate_remote_objects_substitution()
    written = ' '.join(warnings_of(made))
    assert 'same source' in written and 'never fires' in written


def test_two_entries_which_chain_are_reported():
    """
    The output of the first is matched by the second, so what comes out depends on the order -
    and the query of a view is given the list twice, which chains it again.
    """
    made = parser([['a..', 'b.'], ['b.', 'c.']])
    made.validate_remote_objects_substitution()
    assert any('chain' in warning for warning in warnings_of(made))


def test_a_source_contained_in_another_source_is_reported():
    made = parser([['arch', 'x'], ['archive', 'y']])
    made.validate_remote_objects_substitution()
    assert any('is contained in the source of' in warning for warning in warnings_of(made))


def test_two_unrelated_entries_are_not_reported():
    made = parser([['otherdb..a', 'legacy.a'], ['thirddb..b', 'archive.b']])
    made.validate_remote_objects_substitution()
    assert len(warnings_of(made)) == 1, 'only the deprecation itself'


## ------------------------------------------------------- what fired, in every path


class Connector:
    """The shared implementation, without a driver behind it."""

    def __init__(self, substitutions):
        from credativ_pg_migrator.database_connector import DatabaseConnector

        self.config_parser = parser(substitutions)
        self.apply_remote_objects_substitution = (
            DatabaseConnector.apply_remote_objects_substitution.__get__(self))
        self.record_remote_objects_applied = (
            DatabaseConnector.record_remote_objects_applied.__get__(self))


def test_a_replacement_is_recorded_with_the_object_it_was_made_in():
    connector = Connector([['otherdb..archive', 'legacy.archive']])
    code, applied = connector.apply_remote_objects_substitution(
        'select * from otherdb..archive', 'view', 'v_report')
    assert code == 'select * from legacy.archive'
    assert applied == [{'source_object_name': 'otherdb..archive',
                        'target_object_name': 'legacy.archive', 'occurrences': 1}]
    assert connector.config_parser.remote_substitutions_applied == [{
        'object_type': 'view', 'object_name': 'v_report',
        'source_object_name': 'otherdb..archive',
        'target_object_name': 'legacy.archive', 'occurrences': 1}]


def test_the_number_of_matches_is_counted():
    connector = Connector([['otherdb..a', 'legacy.a']])
    _, applied = connector.apply_remote_objects_substitution(
        'select * from otherdb..a union select * from otherdb..a', 'view', 'v')
    assert applied[0]['occurrences'] == 2


def test_a_replacement_is_a_warning_naming_the_object():
    connector = Connector([['otherdb..archive', 'legacy.archive']])
    connector.apply_remote_objects_substitution('select * from otherdb..archive', 'view', 'v_report')
    written = ' '.join(warnings_of(connector.config_parser))
    assert 'v_report' in written
    assert 'otherdb..archive -> legacy.archive' in written
    assert 'not the one the text of the source names' in written


def test_a_rule_which_does_not_fire_records_nothing():
    connector = Connector([['otherdb..archive', 'legacy.archive']])
    code, applied = connector.apply_remote_objects_substitution(
        'select * from dbo.orders', 'view', 'v')
    assert code == 'select * from dbo.orders'
    assert applied == []
    assert connector.config_parser.remote_substitutions_applied == []
    assert warnings_of(connector.config_parser) == []


def test_no_rules_means_no_work_and_no_record():
    connector = Connector(None)
    code, applied = connector.apply_remote_objects_substitution('select 1', 'view', 'v')
    assert (code, applied) == ('select 1', [])


## ------------------------------------------------- one mechanism instead of five copies


def test_no_call_site_rolls_its_own_loop_any_more():
    """
    The same list used to be applied by five hand-written loops - three inside connectors, two
    reading the protocol table - and four of them recorded nothing. They all go through
    apply_remote_objects_substitution() now, which is what makes the record complete.
    """
    for relative in ('planner.py', 'orchestrator.py',
                     'connectors/sybase_ase_connector.py',
                     'connectors/ibm_db2_luw_connector.py',
                     'connectors/ibm_db2_zos_connector.py'):
        source = open(os.path.join(REPO, 'credativ_pg_migrator', relative), encoding='utf-8').read()
        assert 'get_remote_objects_substitution()' not in source, (
            f"{relative} reads the list itself instead of going through "
            f"apply_remote_objects_substitution()")
        assert 'get_records_remote_objects_substitution()' not in source, (
            f"{relative} still reads the rules out of the protocol table")


@pytest.mark.parametrize('relative', [
    'planner.py', 'orchestrator.py',
    'connectors/sybase_ase_connector.py',
    'connectors/ibm_db2_luw_connector.py',
    'connectors/ibm_db2_zos_connector.py',
])
def test_every_call_site_names_the_object_it_is_converting(relative):
    """
    A record which cannot say which view was changed is not much of a record, so every call
    passes the kind of object and its name. Read with ast rather than with a regex - the calls
    span lines and carry calls of their own inside them.
    """
    tree = ast.parse(open(os.path.join(REPO, 'credativ_pg_migrator', relative), encoding='utf-8').read())
    calls = [node for node in ast.walk(tree)
             if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Attribute)
             and node.func.attr == 'apply_remote_objects_substitution']
    assert calls, f"{relative} no longer applies the substitution at all"
    for call in calls:
        assert len(call.args) + len(call.keywords) >= 3, (
            f"{relative}: a call without object_type and object_name: "
            f"{ast.unparse(call)[:100]}")


def test_the_protocol_table_is_created_with_the_others():
    source = open(os.path.join(REPO, 'credativ_pg_migrator', 'migrator_tables.py'),
                  encoding='utf-8').read()
    assert 'def create_table_for_remote_objects_applied' in source
    create_all = re.search(r'def create_all\(self\):(.*?)(?=\n    def )', source, re.S).group(1)
    assert 'create_table_for_remote_objects_applied()' in create_all


def test_the_summary_reports_the_substitutions():
    source = open(os.path.join(REPO, 'credativ_pg_migrator', 'migrator_tables.py'),
                  encoding='utf-8').read()
    summary = re.search(r'def print_migration_summary\(self\):(.*?)(?=\n    def )', source, re.S).group(1)
    assert 'REMOTE OBJECT SUBSTITUTIONS' in summary
    assert 'record_remote_objects_applied()' in summary, (
        'the summary has to flush the record before it reads it back')
