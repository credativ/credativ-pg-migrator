# SPDX-License-Identifier: GPL-3.0-or-later
"""
A phase of a migration says what it really did.

P2-7 of development/OPEN_ISSUES.md. Nineteen places wrote

    update_main_status({..., 'success': True, 'message': 'finished OK'})

on the way out of a phase, whatever had happened inside it — so the migration of the indexes
reported `finished OK` over a run in which indexes had failed to be created, and the failures
were in the indexes protocol table all along, one query away.

Reading it for this turned up two more things of the same kind:

  * **the timing table of the summary showed no status at all** — `Phase / Step | Duration |
    Start Time` — so a phase which failed looked exactly like one which succeeded, and the
    status the phases were writing would have been read by nobody even once it was true;
  * **the planner closed a row it had not opened.** Each of its three workflow branches opened
    `Planner / Standard workflow` (or Mapping, or Anonymization) and closed
    `Planner / ''` — the row of the planner as a whole. The phase row of the workflow was
    therefore never closed at all: no duration, no result, in every migration ever run.

Nothing here connects to anything: the protocol connection is a stub which answers the counting
query with whatever a test wants it to answer.

Run with:  python3 -m pytest tests/test_phase_status.py -v
"""

import ast
import os
import re
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.migrator_tables import MigratorTables


class Config:
    def __init__(self):
        self.messages = []

    def print_log_message(self, level, message):
        self.messages.append((level, str(message)))

    def get_protocol_name_indexes(self):
        return 'indexes'

    def get_protocol_name_tables(self):
        return 'tables'


class Cursor:
    def __init__(self, row):
        self.row = row
        self.statements = []

    def execute(self, query, params=None):
        self.statements.append(' '.join(query.split()))

    def fetchone(self):
        return self.row

    def close(self):
        pass


class Connection:
    def __init__(self, row):
        self.cursor_object = Cursor(row)

    def cursor(self):
        return self.cursor_object

    def rollback(self):
        pass


def tables(counts=(0, 0)):
    made = MigratorTables.__new__(MigratorTables)
    made.protocol_schema = 'migration'
    made.config_parser = Config()
    made.protocol_connection = type('P', (), {'connection': Connection(counts)})()
    return made


# --------------------------------------------------------------------------------------
# the status of a phase is what its objects say


def test_a_phase_whose_objects_all_arrived_finished_ok():
    made = tables(counts=(480, 0))
    success, message = made.phase_result('indexes migration', True, 'finished OK')
    assert success is True
    assert message == 'finished OK'


def test_a_phase_which_lost_objects_did_not_finish_ok():
    """The whole of P2-7: the failures were one query away the entire time."""
    made = tables(counts=(480, 2))
    success, message = made.phase_result('indexes migration', True, 'finished OK')
    assert success is False
    assert '2 of 480 indexes FAILED' in message
    assert 'indexes protocol table' in message


def test_a_phase_which_reports_its_own_failure_keeps_it():
    made = tables(counts=(480, 0))
    success, message = made.phase_result('indexes migration', False, 'ERROR: the target went away')
    assert success is False
    assert message == 'ERROR: the target went away'


def test_a_phase_which_creates_no_objects_is_left_alone():
    made = tables(counts=(480, 7))
    success, message = made.phase_result('comments migration', True, 'finished OK')
    assert success is True
    assert message == 'finished OK'


def test_an_object_which_was_never_attempted_is_not_a_failure():
    """
    `success IS FALSE` is a failure; `success IS NULL` is an object nobody got to - an index
    the configuration excluded, a run which stopped before it. Counting those would report a
    deliberate skip as a broken migration.
    """
    made = tables(counts=(480, 0))
    made.phase_result('indexes migration', True, 'finished OK')
    statement = made.protocol_connection.connection.cursor_object.statements[0]
    assert 'COUNT(CASE WHEN success IS FALSE THEN 1 END)' in statement
    assert 'IS NULL' not in statement


def test_a_protocol_table_which_is_not_there_answers_nothing():
    made = tables()

    def explode(query, params=None):
        raise RuntimeError('relation "indexes" does not exist')

    made.protocol_connection.connection.cursor_object.execute = explode
    assert made.count_failed_objects('indexes') == (None, None)
    ## and a phase over it is left as it was rather than called failed
    assert made.phase_result('indexes migration', True, 'finished OK') == (True, 'finished OK')


def test_an_object_type_nobody_knows_answers_nothing():
    assert tables().count_failed_objects('penguins') == (None, None)


def test_the_check_is_in_the_one_method_every_phase_closes_itself_through():
    """
    Not in the nineteen places which call it: a new phase cannot forget it, and no call site
    can go back to claiming success.
    """
    source = inspect_source('migrator_tables.py')
    body = source.split('def update_main_status')[1].split('\n    def ')[0]
    assert 'self.phase_result(' in body


def inspect_source(name):
    with open(os.path.join(REPO, 'credativ_pg_migrator', name), encoding='utf-8') as handle:
        return handle.read()


# --------------------------------------------------------------------------------------
# no phase may escape the decision


def phases_which_close_themselves():
    """Every subtask name a phase closes itself with, across the planner and the orchestrator."""
    found = set()
    for name in ('planner.py', 'orchestrator.py'):
        source = inspect_source(name)
        for match in re.finditer(r"update_main_status\(\{[^}]*'subtask_name':\s*'([^']*)'", source):
            found.add(match.group(1))
    return found


def test_every_phase_is_either_counted_or_written_off():
    """
    A phase is either one whose objects are counted (PHASE_OBJECT_TABLES) or one which creates
    none of its own (PHASES_WITHOUT_OBJECTS, with the reason). A phase which is neither would
    close itself with 'finished OK' and nothing would ever check it - which is what P2-7 was.
    """
    decided = set(MigratorTables.PHASE_OBJECT_TABLES) | set(MigratorTables.PHASES_WITHOUT_OBJECTS)
    undecided = sorted(phases_which_close_themselves() - decided)
    assert not undecided, (
        'these phases close themselves and are in neither table: ' + ', '.join(repr(p) for p in undecided))


def test_every_counted_phase_names_a_protocol_table_which_exists():
    for subtask, (object_type, label) in MigratorTables.PHASE_OBJECT_TABLES.items():
        assert hasattr(MigratorTables, f'create_table_for_{object_type}'), (subtask, object_type)
        assert label, subtask


def test_the_two_tables_do_not_overlap():
    assert not (set(MigratorTables.PHASE_OBJECT_TABLES) & set(MigratorTables.PHASES_WITHOUT_OBJECTS))


# --------------------------------------------------------------------------------------
# the planner closes the row it opened


def opened_and_closed(source):
    """
    The phase names a module opens and the ones it closes.

    One closing call names the phase through a variable rather than a literal - the
    orchestrator closes whichever row it opened for itself, `''` or `Resume after crash` - so
    the names that variable can hold are read out of its assignment and counted as closed.
    """
    opened = set(re.findall(r"insert_main\(\{[^}]*'subtask_name':\s*'([^']*)'", source))
    closed = set(re.findall(r"update_main_status\(\{[^}]*'subtask_name':\s*'([^']*)'", source))
    if re.search(r"'subtask_name':\s*self\.main_subtask", source):
        for match in re.finditer(r"self\.main_subtask\s*=\s*'([^']*)'\s+if\s+.*?\s+else\s+'([^']*)'", source):
            closed.update(match.groups())
    return opened, closed


def test_the_planner_closes_every_phase_row_it_opens():
    """
    Each branch opened `Planner / <workflow>` and closed `Planner / ''`, so the phase row of
    the workflow was never closed at all - no duration and no result, in every run.
    """
    opened, closed = opened_and_closed(inspect_source('planner.py'))
    assert 'Standard workflow' in opened
    assert not (opened - closed), f'opened and never closed: {sorted(opened - closed)}'


def test_the_orchestrator_closes_every_phase_row_it_opens():
    """
    A resumed run opened `Orchestrator / Resume after crash` and closed `Orchestrator / ''` -
    a row it had never opened - so the resume was never closed and the closing update matched
    no row at all. The orchestrator closes whichever row it opened for itself now.
    """
    opened, closed = opened_and_closed(inspect_source('orchestrator.py'))
    assert 'Resume after crash' in opened
    assert not (opened - closed), f'opened and never closed: {sorted(opened - closed)}'


def test_the_planner_as_a_whole_says_what_its_workflow_did():
    """It used to be closed with 'finished OK' by whichever branch had run, whatever it did."""
    source = inspect_source('planner.py')
    assert 'planning_failed = True' in source
    assert 'the planning of the workflow FAILED' in source


# --------------------------------------------------------------------------------------
# and the summary shows it


def test_the_timing_table_has_a_result_column():
    """
    It showed `Phase / Step | Duration | Start Time` and nothing else, so a phase which failed
    looked exactly like one which succeeded.
    """
    source = inspect_source('migrator_tables.py')
    assert "{'Phase / Step':<44} | {'Duration':<14} | {'Start':<10} | Result" in source
    assert "{'Phase / Step':<44} | {'Duration':<14} | Start Time" not in source


def test_a_phase_which_never_finished_is_shown_as_such():
    source = inspect_source('migrator_tables.py')
    timing = source.split('[ TIMING & EXECUTION PROFILES ]')[1].split('lines.append("")')[0]
    assert "'DID NOT FINISH'" in timing
    assert "task_data['success'] is True" in timing


def test_the_timing_table_no_longer_swallows_its_errors():
    """`except Exception: pass` around the whole table hid a broken summary as an empty one."""
    source = inspect_source('migrator_tables.py')
    timing = source.split('[ TIMING & EXECUTION PROFILES ]')[1].split('[ OBJECTS MIGRATION RESULTS ]')[0]
    assert 'except Exception:\n            pass' not in timing
