# SPDX-License-Identifier: GPL-3.0-or-later
"""
A functional index means in the target what it meant in the source.

P1-3 of development/OPEN_ISSUES.md, and what reading it turned up next to it. Three things
happened to the expression of a functional index without a word being written:

  * **the collation was deleted.** `clean_index_expression()` of the MySQL and MariaDB
    connectors cut `COLLATE <name>` out with a regular expression, and `get_create_index_sql()`
    of the PostgreSQL target does the same to the expression of every source which is not
    PostgreSQL. A collation decides which strings count as equal, so a case-insensitive index
    became a case-sensitive one: it answers `WHERE name = 'MÜLLER'` without the row which
    holds `müller`, and a UNIQUE one stops refusing two values which differ only in case -
    the target then accepts rows the source refused.
  * **a `sqlglot.transpile` which raised was answered with `except Exception: pass`**, which
    left the raw MySQL expression standing as the PostgreSQL one. Usually PostgreSQL refuses
    it and the index is recorded as failed; sometimes the same text is valid there with
    another meaning, and then the index is built on something nobody wrote.
  * **every identifier of the expression became a string literal.** Step 1 replaced the
    backticks of MySQL with double quotes before handing the text to sqlglot as MySQL - where
    a double quote starts a STRING. `lower(`email`)` was converted into `lower('email')`: an
    index on a constant, on every functional index of every MySQL and MariaDB source.

Nothing here connects to anything.

Run with:  python3 -m pytest tests/test_index_collations.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator import collations


class RecordingConfig:
    """Enough of a configuration for an expression to be converted, recording what is said."""

    def __init__(self, source_db_type='mysql', names_case='lower'):
        self.messages = []
        self.source_db_type = source_db_type
        self.names_case = names_case

    def print_log_message(self, level, message):
        self.messages.append((level, str(message)))

    def levels(self, level):
        return [message for written, message in self.messages if written == level]

    def written(self):
        return ' | '.join(message for _, message in self.messages)

    def get_connectivity(self, direction):
        return {'db_type': 'postgresql'}

    def get_source_db_type(self):
        return self.source_db_type

    def convert_names_case(self, name):
        return name.lower() if self.names_case == 'lower' else name

    def get_remote_objects_substitution(self):
        return {}


# --------------------------------------------------------------------------------------
# what a collation of another engine means here


@pytest.mark.parametrize('name', [
    'utf8mb4_bin', 'latin1_bin', 'binary', 'Latin1_General_BIN2', 'utf8_bin',
])
def test_a_byte_order_collation_is_carried_over_exactly(name):
    """MySQL compares the encoded bytes for a _bin collation, and so does the C collation."""
    decision = collations.decide(name)
    assert decision.outcome == collations.BYTE_ORDER
    assert decision.clause == ' COLLATE "C"'
    assert decision.is_faithful
    assert not decision.changes_which_rows_match


@pytest.mark.parametrize('name', [
    'utf8mb4_general_ci', 'utf8mb4_0900_ai_ci', 'utf8mb4_unicode_ci',
    'SQL_Latin1_General_CP1_CI_AS', 'Latin1_General_CI_AI',
])
def test_a_case_insensitive_collation_cannot_be_carried_over(name):
    decision = collations.decide(name)
    assert decision.outcome == collations.INSENSITIVE
    assert decision.clause == ''
    assert not decision.is_faithful
    assert decision.changes_which_rows_match


@pytest.mark.parametrize('name', ['utf8mb4_0900_as_cs', 'Latin1_General_CS_AS'])
def test_a_case_sensitive_collation_becomes_the_default_of_the_target(name):
    """
    The default collation of a PostgreSQL database compares with regard to case and accents
    as well, so the kind of comparison is kept - only the locale is not carried over.
    """
    decision = collations.decide(name)
    assert decision.outcome == collations.SENSITIVE
    assert decision.is_faithful
    assert not decision.changes_which_rows_match


def test_a_collation_the_target_really_has_is_used_as_it_stands():
    decision = collations.decide('de_DE.utf8', existing_names={'de_DE.utf8', 'C'})
    assert decision.outcome == collations.KEPT
    assert decision.clause == ' COLLATE "de_DE.utf8"'


def test_the_catalogue_of_the_target_is_asked_before_the_name_is_read():
    """
    `fr_CI.utf8` is Côte d'Ivoire and not a case-insensitive collation. Reading the name is
    the last resort, which is why what the target really has is asked first.
    """
    assert collations.decide('fr_CI.utf8').outcome == collations.INSENSITIVE
    assert collations.decide('fr_CI.utf8', existing_names={'fr_CI.utf8'}).outcome == collations.KEPT


def test_a_collation_nobody_here_knows_is_reported_and_not_assumed_harmless():
    decision = collations.decide('some_house_collation')
    assert decision.outcome == collations.UNKNOWN
    assert collations.report_level(decision) == 'WARNING'


@pytest.mark.parametrize('quoted', ['`utf8mb4_bin`', '"utf8mb4_bin"', "'utf8mb4_bin'", ' utf8mb4_bin '])
def test_the_quoting_of_the_dialect_is_not_part_of_the_name(quoted):
    assert collations.decide(quoted).outcome == collations.BYTE_ORDER


def test_only_the_two_which_change_the_comparison_are_written_loudly():
    for name in ('utf8mb4_bin', 'utf8mb4_0900_as_cs'):
        assert collations.report_level(collations.decide(name)) == 'DEBUG'
    for name in ('utf8mb4_general_ci', 'house_rules'):
        assert collations.report_level(collations.decide(name)) == 'WARNING'


def test_the_message_says_what_changes_and_how_to_get_it_back():
    decision = collations.decide('utf8mb4_general_ci')
    message = collations.explain(decision, "index 'ix_name'", 'INDEX')
    assert 'ix_name' in message
    assert 'utf8mb4_general_ci' in message
    assert 'DROPPED' in message
    assert 'deterministic = false' in message, 'the way out belongs in the message'
    assert 'UNIQUE' not in message


def test_a_unique_index_is_told_that_it_stops_refusing_rows():
    """The half of this which changes what data may exist in the target."""
    decision = collations.decide('utf8mb4_general_ci')
    message = collations.explain(decision, "index 'ix_name'", 'UNIQUE')
    assert 'UNIQUE' in message
    assert 'accepts rows the source' in message


# --------------------------------------------------------------------------------------
# taking a collation out of the way of the rewrites and putting it back


def test_a_collation_survives_a_rewrite_which_would_have_deleted_it():
    """
    apply_sql_functions_mapping() deletes a COLLATE clause, and it is shared with the view
    path, which is why the clause is taken out of its way instead of the rewrite being changed.
    """
    import re

    expression, found = collations.take_out('("name" COLLATE utf8mb4_bin)')
    assert found == ['utf8mb4_bin']
    assert 'COLLATE' not in expression
    ## what apply_sql_functions_mapping() does to a COLLATE clause
    mangled = re.sub(r'(?i)\bCOLLATE\s+[`\'"]?[a-zA-Z0-9_]+[`\'"]?', '', expression)
    assert mangled == expression, 'the token must not look like a collation any more'
    restored, decisions = collations.put_back(mangled, found)
    assert restored == '("name" COLLATE "C")'
    assert [decision.outcome for decision in decisions] == [collations.BYTE_ORDER]


def test_a_dropped_collation_leaves_no_gap_behind():
    expression, found = collations.take_out('("name" COLLATE utf8mb4_general_ci)')
    restored, _ = collations.put_back(expression, found)
    assert restored == '("name")'


def test_every_collation_of_an_expression_is_decided_separately():
    expression, found = collations.take_out(
        '(("a" COLLATE utf8mb4_bin) || ("b" COLLATE utf8mb4_general_ci))')
    assert found == ['utf8mb4_bin', 'utf8mb4_general_ci']
    restored, decisions = collations.put_back(expression, found)
    assert restored == '(("a" COLLATE "C") || ("b"))'
    assert [decision.outcome for decision in decisions] == [
        collations.BYTE_ORDER, collations.INSENSITIVE]


def test_an_expression_without_a_collation_is_untouched():
    expression, found = collations.take_out('(upper("name"))')
    assert found == []
    assert expression == '(upper("name"))'
    restored, decisions = collations.put_back(expression, found)
    assert restored == '(upper("name"))'
    assert decisions == []


# --------------------------------------------------------------------------------------
# the MySQL and MariaDB connectors


def connector_class(engine):
    module = f'credativ_pg_migrator.connectors.{engine}_connector'
    name = {'mysql': 'MySQLConnector', 'mariadb': 'MariaDBConnector'}[engine]
    try:
        import importlib

        return getattr(importlib.import_module(module), name)
    except Exception as error:
        pytest.skip(f'{engine} needs a driver which is not installed here ({error})')


@pytest.fixture(params=['mysql', 'mariadb'])
def connector(request):
    made = connector_class(request.param).__new__(connector_class(request.param))
    made.config_parser = RecordingConfig()
    return made


def clean(connector, expression, index_type='INDEX'):
    return connector.clean_index_expression(
        expression, index_name='ix_customers', index_type=index_type,
        source_table_name='customers')


def test_an_identifier_of_the_source_stays_an_identifier(connector):
    """
    The backticks used to be replaced with double quotes before the text was handed to
    sqlglot as MySQL, where a double quote starts a STRING - so every column of every
    functional index became a constant.
    """
    assert clean(connector, 'lower(`email`)') == 'lower("email")'
    assert clean(connector, '(concat(`first_name`,\' \',`last_name`))') == \
        '("first_name" || \' \' || "last_name")'
    assert "'email'" not in clean(connector, 'lower(`email`)')


def test_a_byte_order_collation_reaches_the_target(connector):
    assert clean(connector, '(`name` collate utf8mb4_bin)') == '("name" COLLATE "C")'
    assert connector.config_parser.levels('WARNING') == []


def test_a_case_insensitive_collation_is_dropped_and_said_out_loud(connector):
    expression = clean(connector, '(`name` collate utf8mb4_general_ci)', index_type='UNIQUE')
    assert expression == '("name")'
    written = connector.config_parser.levels('WARNING')
    assert len(written) == 1
    assert 'ix_customers' in written[0]
    assert 'customers' in written[0]
    assert 'utf8mb4_general_ci' in written[0]
    assert 'UNIQUE' in written[0]


def test_an_expression_which_cannot_be_read_is_refused_and_not_handed_over(connector):
    """
    `except Exception: pass` used to leave the raw MySQL expression standing. What comes back
    now is nothing, and the index is reported as one which was not migrated.
    """
    unreadable = '(SELECT FROM WHERE ((('
    assert clean(connector, unreadable) == ''
    written = connector.config_parser.levels('ERROR')
    assert len(written) == 1
    assert 'NOT migrated' in written[0]
    assert unreadable in written[0], 'the expression of the source belongs in the message'
    assert 'ix_customers' in written[0]


def test_the_or_of_mysql_is_not_read_as_the_concatenation_of_postgresql(connector):
    """
    `||` is an OR in MySQL and a concatenation in PostgreSQL. Handing the raw expression over
    was valid in both and meant two different things; the transpiler answers with the meaning
    the source had.
    """
    assert clean(connector, '(`a` || `b`)') == '("a" OR "b")'


def test_a_charset_introducer_is_still_removed(connector):
    expression = clean(
        connector,
        "(cast(json_unquote(json_extract(`data`,_utf8mb4'$.name')) as char(30)) collate utf8mb4_bin)")
    assert '_utf8mb4' not in expression
    assert 'COLLATE "C"' in expression
    assert '"data"' in expression


def test_an_empty_expression_is_answered_with_nothing(connector):
    assert clean(connector, '') == ''
    assert connector.config_parser.messages == []


def test_a_connector_without_a_configuration_does_not_fail_on_the_message(connector):
    """fetch_indexes() is tested with config_parser set to None - a message must not raise."""
    connector.config_parser = None
    assert clean(connector, '(`name` collate utf8mb4_general_ci)') == '("name")'


# --------------------------------------------------------------------------------------
# the PostgreSQL target, which strips the collation of every source which is not PostgreSQL


@pytest.fixture
def target():
    try:
        from credativ_pg_migrator.connectors.postgresql_connector import PostgreSQLConnector
    except Exception as error:
        pytest.skip(f'postgresql needs a driver which is not installed here ({error})')

    made = PostgreSQLConnector.__new__(PostgreSQLConnector)
    made.config_parser = RecordingConfig(source_db_type='mysql')
    ## the catalogue of the target is not asked - the connection is not open here
    made.existing_collation_names = {'C', 'POSIX'}
    return made


def create_index_sql(target, index_columns, index_type='INDEX'):
    return target.get_create_index_sql({
        'index_name': 'ix_customers',
        'index_type': index_type,
        'target_schema_name': 'migtest',
        'target_table_name': 'customers',
        'index_columns': index_columns,
        'is_function_based': 'YES',
        'target_columns': {},
    })


def test_the_target_carries_a_byte_order_collation_into_the_ddl(target):
    sql = create_index_sql(target, '(lower("name") COLLATE "C")')
    assert 'COLLATE "C"' in sql
    assert target.config_parser.levels('WARNING') == []


def test_the_name_of_a_collation_is_not_folded_with_the_identifiers(target):
    """
    A collation is not an object of this migration and its name is case sensitive, so
    names_case_handling must not touch it. `COLLATE "C"` used to be folded to `COLLATE "c"`
    together with the identifiers of the expression, and PostgreSQL has no collation "c":
    every functional index which carried a collation failed to be created.
    """
    assert target.config_parser.convert_names_case('C') == 'c', 'the folding is really on'
    sql = create_index_sql(target, '(lower("NAME") COLLATE "C")')
    assert 'COLLATE "C"' in sql
    assert 'COLLATE "c"' not in sql


def test_the_target_reports_a_collation_it_cannot_express(target):
    """
    The same defect as in the source connectors, and it is the one every other engine goes
    through: ms_sql, sybase_ase, oracle, informix and the Db2 family all come here.
    """
    sql = create_index_sql(target, '(lower("name") COLLATE SQL_Latin1_General_CP1_CI_AS)',
                           index_type='UNIQUE')
    assert 'COLLATE' not in sql
    written = target.config_parser.levels('WARNING')
    assert len(written) == 1
    assert 'SQL_Latin1_General_CP1_CI_AS' in written[0]
    assert 'ix_customers' in written[0]
    assert 'UNIQUE' in written[0]


def test_the_target_leaves_a_postgresql_source_to_the_collations_it_migrated(target):
    """
    A PostgreSQL source has its collations migrated with the schema, and that path resolves
    them through user_collations - it is not this decision and must not be taken over by it.
    """
    target.config_parser.source_db_type = 'postgresql'
    sql = create_index_sql(target, '(lower("name") COLLATE "C")')
    assert 'COLLATE "C"' in sql
