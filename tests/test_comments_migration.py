# SPDX-License-Identifier: GPL-3.0-or-later
"""
The comments phase: `COMMENT ON …` for everything a migration created.

It ended on its first refusal. `COMMENT ON TRIGGER "migtest"."c_customers_audit"` is a syntax
error at the dot — a trigger is not a schema object, it belongs to its table, and PostgreSQL
takes `COMMENT ON TRIGGER <name> ON <table>` — and because the whole phase stood in one
try/except, that one statement ended it. Every comment behind it, in a migration with any
commented trigger at all, was never even attempted: the views, and the user defined types,
whose branch had a `KeyError: 'type_name'` of its own which nobody had ever reached.

And the summary reported all of them as migrated anyway: it counted the comments the protocol
holds and printed that number as the number set, with a hard-coded `0` failed.

Three repairs, and this file holds the guards for all three:

  * the statements are the ones PostgreSQL takes;
  * every comment is its own statement and its own answer - one which is refused is a line in
    the log, not the end of the phase - and a comment whose object was never created is
    **skipped**, not counted as a failure, because the object is already recorded as one;
  * the summary reports what the phase really did, and says `?` where the phase did not run.

The last test is the one which would have caught both defects at once: every key the phase
reads out of a decoded protocol row has to exist in the decoder it came from. Read with `ast`,
so it stays true for a key added later.

Nothing here talks to a database.

Run with:  python3 -m pytest tests/test_comments_migration.py -v
"""

import ast
import os
import sys
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.orchestrator import Orchestrator


def orchestrator_with(tables=(), indexes=(), constraints=(), triggers=(), views=(), types=()):
    """
    The comments phase, with the protocol answering rows built here and the target recording
    what it was asked to run.
    """
    made = Orchestrator.__new__(Orchestrator)
    made.config_parser = MagicMock()
    made.config_parser.convert_names_case.side_effect = lambda name: (name or '').lower()
    made.config_parser.get_use_aliases_as_target_names.return_value = False
    made.config_parser.get_source_db_type.return_value = 'postgresql'
    made.config_parser.is_dry_run.return_value = False
    made.messages = []
    made.config_parser.print_log_message.side_effect = \
        lambda level, message: made.messages.append((level, str(message)))

    protocol = MagicMock()
    protocol.fetch_all_tables.return_value = list(tables)
    protocol.fetch_all_indexes.return_value = list(indexes)
    protocol.fetch_all_constraints.return_value = list(constraints)
    protocol.fetch_all_triggers.return_value = list(triggers)
    protocol.fetch_all_views.return_value = list(views)
    protocol.fetch_all_user_defined_types.return_value = list(types)
    for name in ('table', 'index', 'constraint', 'trigger', 'view', 'user_defined_type'):
        getattr(protocol, f'decode_{name}_row').side_effect = lambda row: row
    made.migrator_tables = protocol

    made.executed = []
    target = MagicMock()
    target.execute_query.side_effect = lambda query: made.executed.append(' '.join(query.split()))
    made.target_connection = target
    made.handle_error = MagicMock()
    return made


def trigger_row(name='c_customers_audit', comment='what it is for', success=True,
                target_name=None, table='customers'):
    return {'target_schema_name': 'migtest', 'target_table_name': table,
            'source_table_name': table, 'trigger_name': name,
            'target_trigger_name': target_name if target_name is not None else name,
            'trigger_comment': comment, 'success': success}


def view_row(name='v_customers', comment='what it shows', success=True, sql='CREATE VIEW x AS SELECT 1'):
    return {'target_schema_name': 'migtest', 'target_view_name': name, 'target_view_alias': '',
            'target_view_sql': sql, 'view_comment': comment, 'success': success}


def type_row(name='postal_address', comment='a composite type', success=True):
    return {'target_schema_name': 'migtest', 'source_type_name': name,
            'target_type_name': name, 'type_comment': comment, 'success': success}


def table_row(name='customers', comment='the customers', columns=None):
    return {'target_schema_name': 'migtest', 'target_table_name': name, 'target_alias_name': '',
            'table_comment': comment, 'target_columns': columns or {}}


# --------------------------------------------------------------------------------------
# the statements are the ones PostgreSQL takes


def test_a_trigger_comment_names_the_trigger_and_the_table_it_stands_on():
    """
    The defect this file is about. A trigger is not a schema object of its own - it belongs to
    its table, and two tables may carry triggers of one name - so PostgreSQL takes
    `COMMENT ON TRIGGER <name> ON <table>`. `COMMENT ON TRIGGER "migtest"."c_customers_audit"`
    is a syntax error at the dot.
    """
    made = orchestrator_with(triggers=[trigger_row()])
    made.stdwf_migrate_comments()

    assert made.executed == [
        'COMMENT ON TRIGGER "c_customers_audit" ON "migtest"."customers" '
        "IS 'what it is for'"]


def test_a_trigger_comment_uses_the_name_the_target_really_has():
    """names_case_handling may have renamed it, and the protocol records what it became."""
    made = orchestrator_with(triggers=[trigger_row(name='MyTrigger', target_name='mytrigger')])
    made.stdwf_migrate_comments()
    assert 'COMMENT ON TRIGGER "mytrigger" ON "migtest"."customers"' in made.executed[0]


def test_a_type_comment_reads_the_key_the_row_really_has():
    """
    The row of a user defined type has source_type_name and target_type_name, and this branch
    read `type_name` - a KeyError which ended the phase. Nobody had ever seen it, because the
    trigger comments above it ended the phase first.
    """
    made = orchestrator_with(types=[type_row()])
    made.stdwf_migrate_comments()
    assert made.executed == [
        'COMMENT ON TYPE "migtest"."postal_address" IS \'a composite type\'']


def test_a_materialized_view_is_not_a_view_for_comment_on():
    made = orchestrator_with(views=[view_row(sql='CREATE MATERIALIZED VIEW x AS SELECT 1')])
    made.stdwf_migrate_comments()
    assert made.executed[0].startswith('COMMENT ON MATERIALIZED VIEW')


def test_a_table_and_its_columns_are_commented():
    made = orchestrator_with(tables=[table_row(columns={
        'a': {'column_name': 'city', 'column_comment': 'where they are'}})])
    made.stdwf_migrate_comments()
    assert made.executed == [
        'COMMENT ON TABLE "migtest"."customers" IS \'the customers\'',
        'COMMENT ON COLUMN "migtest"."customers"."city" IS \'where they are\'']


# --------------------------------------------------------------------------------------
# one comment which fails is one comment which fails


def test_a_refused_comment_does_not_end_the_phase():
    """
    The repair which matters most: the phase used to stand in one try/except, so the first
    statement the target refused took every comment behind it with it - the views and the
    types of a migration with any commented trigger at all.
    """
    made = orchestrator_with(triggers=[trigger_row(name='first'), trigger_row(name='second')],
                             types=[type_row()])
    made.target_connection.execute_query.side_effect = \
        lambda query: (_ for _ in ()).throw(RuntimeError('syntax error')) \
        if 'first' in query else made.executed.append(' '.join(query.split()))

    made.stdwf_migrate_comments()

    assert made.comments_attempted == 3
    assert made.comments_succeeded == 2
    assert len(made.comments_failed) == 1
    assert any('COMMENT ON TYPE' in query for query in made.executed), \
        'the type behind the failure was never even attempted before this repair'


def test_a_refused_comment_is_named_in_the_log_and_in_the_phase_status():
    made = orchestrator_with(triggers=[trigger_row()])
    made.target_connection.execute_query.side_effect = RuntimeError('syntax error at or near "."')

    made.stdwf_migrate_comments()

    status = made.migrator_tables.update_main_status.call_args[0][0]
    assert status['success'] is False
    assert '1 of 1 comments FAILED' in status['message']
    assert any('c_customers_audit' in message for _level, message in made.messages)


def test_a_phase_with_nothing_refused_reports_success():
    made = orchestrator_with(triggers=[trigger_row()])
    made.stdwf_migrate_comments()
    status = made.migrator_tables.update_main_status.call_args[0][0]
    assert status['success'] is True and status['message'] == 'finished OK'


def test_the_transaction_is_rolled_back_after_a_refusal():
    """
    A statement PostgreSQL refuses leaves the session in an aborted transaction, and every
    statement behind it answers 'current transaction is aborted' - which would make one bad
    comment look like a whole phase of bad ones.
    """
    made = orchestrator_with(triggers=[trigger_row()])
    made.target_connection.execute_query.side_effect = RuntimeError('nope')
    made.stdwf_migrate_comments()
    assert made.target_connection.connection.rollback.called


# --------------------------------------------------------------------------------------
# a comment whose object was never created is not a failed comment


@pytest.mark.parametrize('rows, kind', [
    ({'triggers': [trigger_row(success=False)]}, 'trigger'),
    ({'views': [view_row(success=False)]}, 'view'),
    ({'types': [type_row(success=False)]}, 'type'),
])
def test_a_comment_on_an_object_which_was_not_created_is_skipped(rows, kind):
    """
    The view which could not be created is already recorded as a failed view. Attempting its
    comment and reporting the refusal counts one defect twice.
    """
    made = orchestrator_with(**rows)
    made.stdwf_migrate_comments()

    assert made.executed == []
    assert made.comments_attempted == 0
    assert made.comments_failed == []
    assert len(made.comments_skipped) == 1
    status = made.migrator_tables.update_main_status.call_args[0][0]
    assert status['success'] is True


def test_the_phase_records_what_it_did_for_the_summary():
    made = orchestrator_with(triggers=[trigger_row(name='ok'), trigger_row(name='gone', success=False)])
    made.stdwf_migrate_comments()
    assert made.migrator_tables.comments_result == {
        'attempted': 1, 'succeeded': 1, 'failed': 0, 'skipped': 1}


# --------------------------------------------------------------------------------------
# every key the phase reads is a key the row really has


def returned_keys(function_node):
    """The keys of the dictionary a decode_*_row() answers."""
    for node in ast.walk(function_node):
        if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict):
            return {key.value for key in node.value.keys
                    if isinstance(key, ast.Constant) and isinstance(key.value, str)}
    return set()


def read_keys(function_node, variable):
    """Every key the phase reads out of one `<x>_data` variable, by subscript or by .get()."""
    found = set()
    for node in ast.walk(function_node):
        if (isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name)
                and node.value.id == variable and isinstance(node.slice, ast.Constant)
                and isinstance(node.slice.value, str)):
            found.add(node.slice.value)
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == 'get' and isinstance(node.func.value, ast.Name)
                and node.func.value.id == variable and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)):
            found.add(node.args[0].value)
    return found


def function_named(path, name):
    with open(path, encoding='utf-8') as handle:
        tree = ast.parse(handle.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f'{name} is not in {path}')


def decoders_used_by_the_phase():
    """`<x>_data = self.migrator_tables.decode_<y>(...)` inside the phase, as {variable: decoder}."""
    phase = function_named(os.path.join(REPO, 'credativ_pg_migrator', 'orchestrator.py'),
                           'stdwf_migrate_comments')
    found = {}
    for node in ast.walk(phase):
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
            continue
        call = node.value.func
        if not (isinstance(call, ast.Attribute) and call.attr.startswith('decode_')):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                found[target.id] = call.attr
    return phase, found


@pytest.mark.parametrize('variable, decoder', sorted(decoders_used_by_the_phase()[1].items()))
def test_every_key_the_comments_phase_reads_exists_in_the_row_it_reads_it_from(variable, decoder):
    """
    The guard which would have caught `type_data['type_name']` - a KeyError, because the row
    carries source_type_name and target_type_name and not that. The key the phase reads through
    the helper below is asserted by the test after this one.
    """
    phase, _decoders = decoders_used_by_the_phase()
    keys = read_keys(phase, variable)
    available = returned_keys(function_named(
        os.path.join(REPO, 'credativ_pg_migrator', 'migrator_tables.py'), decoder))
    missing = sorted(keys - available)
    assert not missing, (
        f"stdwf_migrate_comments() reads {', '.join(missing)} out of {variable}, and "
        f"{decoder}() does not answer {'them' if len(missing) > 1 else 'it'}. A subscript is a "
        f"KeyError which ends the phase; a .get() is a None which reads as 'no' and is not one")


@pytest.mark.parametrize('variable, decoder', sorted(decoders_used_by_the_phase()[1].items()))
def test_every_row_the_phase_reads_can_say_whether_its_object_was_created(variable, decoder):
    """
    The other half of the same guard. The phase asks every decoded row whether the object it
    describes was really created - a comment on an object which was not is skipped, not
    attempted and blamed - and it asks with `.get('success')`, which answers None for a decoder
    which does not carry it. None reads as "not False", so the comment is attempted anyway and
    its refusal is counted as a failed comment: one defect counted twice.

    `decode_view_row()` stopped at the comment and answered no success at all, which is exactly
    how the two views of the migtest example were reported as failed comments rather than as
    comments whose view had failed.
    """
    if variable == 'table_data':
        ## a table which was not created has no columns in the protocol either, and the phase
        ## reads its comments out of the same row - there is nothing to skip
        pytest.skip('the table row is the one the whole phase is driven from')
    available = returned_keys(function_named(
        os.path.join(REPO, 'credativ_pg_migrator', 'migrator_tables.py'), decoder))
    assert 'success' in available, (
        f"{decoder}() answers no 'success', so the comments phase cannot tell whether the "
        f"object was created - and a comment on an object which was not created is reported as "
        f"a failed comment instead of being skipped")
