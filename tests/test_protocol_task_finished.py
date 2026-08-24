# SPDX-License-Identifier: GPL-3.0-or-later
"""
The method the orchestrator calls has to exist.

P2-1 of development/OPEN_ISSUES.md. `index_worker()` answered an index whose SQL came out
empty with

    self.migrator_tables.update_protocol_task_finished('indexes', index_data['id'], ...)

and nothing defined that method. The call raised `AttributeError: 'MigratorTables' object has
no attribute 'update_protocol_task_finished'`, the `except` around the worker caught it and
recorded the index as failed with the AttributeError as its message: the right outcome for the
wrong reason, and a line which tells the reader nothing about the index.

Reading it turned up the other half. The branch ended in `return True`, and the caller answers
a worker which returned anything but False with

    update_index_status({'row_id': ..., 'success': True, 'message': 'migrated OK'})

so as soon as the missing method existed, the row it had just written would have been
overwritten with **migrated OK for an index which was never created** - which is F-24, the
finding the tracker recorded as no longer happening *because the crash got there first*.
Repairing one without the other would have brought it back. The worker answers False now.

Nothing here connects to anything: the protocol connection is a stub which records the SQL it
was given.

Run with:  python3 -m pytest tests/test_protocol_task_finished.py -v
"""

import ast
import glob
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.migrator_tables import MigratorTables


# --------------------------------------------------------------------------------------
# no call into the protocol tables may name a method which is not there


def calls_into_the_protocol_tables():
    """Every `migrator_tables.<name>(...)` of the package, with where it stands."""
    files = [os.path.join(REPO, 'credativ_pg_migrator', name)
             for name in ('orchestrator.py', 'planner.py', 'validator.py')]
    files += sorted(glob.glob(os.path.join(REPO, 'credativ_pg_migrator', 'connectors', '*.py')))
    found = []
    for path in files:
        with open(path, encoding='utf-8') as handle:
            tree = ast.parse(handle.read(), filename=path)
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
                continue
            holder = node.func.value
            ## self.migrator_tables.x(...) and the migrator_tables handed to a connector
            name = (holder.attr if isinstance(holder, ast.Attribute)
                    else holder.id if isinstance(holder, ast.Name) else None)
            if name == 'migrator_tables':
                found.append((os.path.basename(path), node.lineno, node.func.attr))
    return found


def test_every_method_called_on_the_protocol_tables_exists():
    """
    The check which would have caught P2-1 the day it was written. A method which is only
    called inside an `except`-wrapped worker is never missed until a migration runs into the
    one branch which calls it.
    """
    calls = calls_into_the_protocol_tables()
    assert len(calls) > 300, 'the scan found almost nothing - it is not looking where it should'
    missing = sorted({(module, line, method) for module, line, method in calls
                      if not hasattr(MigratorTables, method)})
    assert not missing, (
        'these calls name a method MigratorTables does not have: ' +
        ', '.join(f'{module}:{line} {method}()' for module, line, method in missing))


def test_the_method_this_repair_adds_is_the_one_which_was_called():
    assert hasattr(MigratorTables, 'update_protocol_task_finished')
    assert hasattr(MigratorTables, 'update_protocol_task_started'), 'its counterpart'


# --------------------------------------------------------------------------------------
# what it writes


class Cursor:
    def __init__(self, row):
        self.row = row
        self.statements = []

    def execute(self, query, params=None):
        self.statements.append((' '.join(query.split()), params))

    def fetchone(self):
        return self.row

    def close(self):
        pass


class Connection:
    def __init__(self, row=(1,)):
        self.cursor_object = Cursor(row)
        self.commits = 0

    def cursor(self):
        return self.cursor_object

    def commit(self):
        self.commits += 1


class ProtocolConnection:
    def __init__(self, row=(1,)):
        self.connection = Connection(row)
        self.queries = []

    def execute_query(self, query, params=None):
        self.queries.append((' '.join(query.split()), params))


class Config:
    def __init__(self):
        self.messages = []

    def print_log_message(self, level, message):
        self.messages.append((level, str(message)))

    def levels(self, level):
        return [message for written, message in self.messages if written == level]

    def get_protocol_name(self):
        return 'protocol'

    def get_protocol_name_indexes(self):
        return 'indexes'

    def get_protocol_name_sequences(self):
        return 'sequences'

    def get_protocol_name_views(self):
        return 'views'


def tables(row=(1,)):
    made = MigratorTables.__new__(MigratorTables)
    made.protocol_schema = 'migration'
    made.config_parser = Config()
    made.protocol_connection = ProtocolConnection(row)
    return made


def test_it_writes_the_end_of_the_task_the_reason_and_the_verdict():
    made = tables()
    made.update_protocol_task_finished('indexes', 42, 'not created - no statement could be built')
    statement, params = made.protocol_connection.connection.cursor_object.statements[0]
    assert 'UPDATE "migration"."indexes"' in statement
    assert 'task_completed = clock_timestamp()' in statement
    assert 'success = %s' in statement
    assert 'message = %s' in statement
    assert 'WHERE id = %s' in statement
    assert params == ('FALSE', 'not created - no statement could be built', 42)


def test_an_object_which_was_not_created_is_not_a_success_by_default():
    """
    The only caller means exactly this: an object the migration did not create. An object
    which WAS created is recorded by the update_*_status() of its own kind.
    """
    made = tables()
    made.update_protocol_task_finished('indexes', 1, 'not created')
    assert made.protocol_connection.connection.cursor_object.statements[0][1][0] == 'FALSE'
    made = tables()
    made.update_protocol_task_finished('indexes', 1, 'done', success=True)
    assert made.protocol_connection.connection.cursor_object.statements[0][1][0] == 'TRUE'


def test_the_journal_of_the_run_is_finished_as_well():
    """
    Every object is written into the journal when it is planned, with execution_success still
    empty. One which is never finished leaves that row saying the work began and never saying
    what came of it.
    """
    made = tables()
    made.update_protocol_task_finished('indexes', 42, 'not created')
    statement, params = made.protocol_connection.queries[0]
    assert 'UPDATE "migration"."protocol"' in statement
    assert params == ('FALSE', 'not created', None, 42, 'index')


def test_the_sequences_are_keyed_by_their_own_column():
    made = tables()
    made.update_protocol_task_finished('sequences', 7, 'not created')
    statement, _ = made.protocol_connection.connection.cursor_object.statements[0]
    assert 'WHERE sequence_id = %s' in statement


def test_an_object_type_nobody_knows_is_reported_and_writes_nothing():
    made = tables()
    made.update_protocol_task_finished('penguins', 1, 'not created')
    assert made.protocol_connection.connection.cursor_object.statements == []
    assert 'Invalid object_type' in made.config_parser.levels('ERROR')[0]


def test_a_row_which_is_not_there_is_reported():
    made = tables(row=None)
    made.update_protocol_task_finished('indexes', 999, 'not created')
    assert 'No row 999' in made.config_parser.levels('ERROR')[0]


def test_the_two_names_of_one_object_are_written_down_and_not_guessed():
    """
    The protocol tables are selected by the plural name of the object and the journal of the
    run calls the same object by its singular one - 'indexes' against 'index', 'text_search'
    against 'text search'. The crossing between the two vocabularies is written down, and
    every name on either side has to be one which really exists: a table which can be
    updated, and an object type the journal was really filled with.
    """
    import re

    with open(os.path.join(REPO, 'credativ_pg_migrator', 'migrator_tables.py'), encoding='utf-8') as handle:
        source = handle.read()
    written_into_the_journal = set(
        re.findall(r"insert_protocol\(\{'object_type': '([^']+)'", source))
    assert 'index' in written_into_the_journal, 'the scan found nothing to compare against'

    for table, journal in MigratorTables.PROTOCOL_TABLE_TO_JOURNAL_OBJECT.items():
        assert hasattr(MigratorTables, f'create_table_for_{table}'), (
            f'{table} is not a protocol table which is created')
        assert journal in written_into_the_journal, (
            f'nothing ever writes {journal!r} into the journal, so finishing it there '
            f'would update no row')


def test_every_table_the_method_can_write_to_has_the_columns_it_sets():
    """
    It sets task_completed, success and message on whichever protocol table it is pointed at.
    A table without one of them would raise on the UPDATE, in a branch nobody runs often.
    """
    import re

    with open(os.path.join(REPO, 'credativ_pg_migrator', 'migrator_tables.py'), encoding='utf-8') as handle:
        source = handle.read()
    for table in MigratorTables.PROTOCOL_TABLE_TO_JOURNAL_OBJECT:
        body = re.search(rf'def create_table_for_{table}\(self\):(.*?)(?=\n    def )', source, re.S)
        assert body, table
        for column in ('task_completed', 'success', 'message'):
            assert re.search(rf'\n\s+{column}\s', body.group(1)), f'{table} has no {column}'


# --------------------------------------------------------------------------------------
# the worker which called it


class ProtocolTables:
    def __init__(self):
        self.finished = []
        self.started = []
        self.statuses = []

    def update_protocol_task_finished(self, object_type, row_id, message, success=False):
        self.finished.append((object_type, row_id, message, success))

    def update_protocol_task_started(self, object_type, row_id):
        self.started.append((object_type, row_id))

    def update_index_status(self, settings):
        self.statuses.append(dict(settings))


def index_worker_over(index_data):
    from credativ_pg_migrator.orchestrator import Orchestrator

    made = Orchestrator.__new__(Orchestrator)
    made.config_parser = Config()
    made.migrator_tables = ProtocolTables()
    made.on_error_action = 'continue'
    return made.index_worker(index_data, 'postgresql'), made


@pytest.mark.parametrize('empty', ['', '   ', None])
def test_an_index_with_no_statement_is_recorded_as_not_created(empty):
    result, made = index_worker_over({'index_name': 'ix_orders', 'index_sql': empty, 'id': 5,
                                      'target_table_name': 'orders'})
    assert made.migrator_tables.finished == [
        ('indexes', 5, made.migrator_tables.finished[0][2], False)]
    assert 'no CREATE INDEX statement could be built' in made.migrator_tables.finished[0][2]
    written = made.config_parser.levels('WARNING')[0]
    assert 'ix_orders' in written
    assert 'orders' in written


def test_such_an_index_is_not_answered_with_true():
    """
    The caller writes 'migrated OK' over the row of every worker which did not answer False.
    Answering True here would report an index which does not exist as migrated - F-24, which
    only stopped happening because the missing method crashed first.
    """
    result, made = index_worker_over({'index_name': 'ix', 'index_sql': '', 'id': 1})
    assert result is False
    assert made.migrator_tables.started == [], 'the task never started - there was nothing to run'


def test_the_caller_would_overwrite_the_record_of_a_worker_which_answers_true():
    """
    Asserted on the caller itself, so that the reason the worker answers False cannot be
    refactored away without this failing.
    """
    with open(os.path.join(REPO, 'credativ_pg_migrator', 'orchestrator.py'), encoding='utf-8') as handle:
        source = handle.read()
    assert "'success': True, 'message': 'migrated OK'" in source, (
        'the caller no longer writes migrated OK - check whether index_worker still has to '
        'answer False for an index which was not created')
