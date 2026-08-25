# SPDX-License-Identifier: GPL-3.0-or-later
"""
The order the constraints of a migration are created in.

A foreign key can only be created once the UNIQUE constraint it references exists, and the two
belong to **different tables** — so the order the protocol holds them in, which is the order the
planner read the tables in, decided whether the key could be created at all. `fk_children` sorts
in front of `fk_parent`, so the key referencing `fk_parent (alt_key_a, alt_key_b)` was created
before the constraint which makes those columns unique:

    ERROR: there is no unique constraint matching given keys for referenced table "fk_parent"

The workers made it worse rather than better. Eight constraints are in flight at once, so a key
which happened to stand *behind* its unique constraint in the protocol failed as well — in some
runs and not in others, which is the worst way for a defect to present itself. And the comment of
a constraint which was never created failed in its turn, one phase later, with
`constraint "..." for table "..." does not exist`.

The phase runs in two waves now: everything which is not a foreign key, then the foreign keys.
Each wave is still parallel; what is serialised is only the dependency which really exists.

The primary keys are not part of this — they are created with the indexes, in the phase before.

Nothing here talks to a database.

Run with:  python3 -m pytest tests/test_constraint_order.py -v
"""

import os
import sys
import threading
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.orchestrator import Orchestrator


def constraint(name, kind, table='t', identifier=None):
    return {'id': identifier if identifier is not None else name, 'constraint_name': name,
            'constraint_type': kind, 'source_table_name': table}


def orchestrator_with(constraints, workers=8, failing=()):
    """
    The constraints phase, with the protocol answering these constraints and the worker
    recording the order in which it was asked to create them.
    """
    made = Orchestrator.__new__(Orchestrator)
    made.config_parser = MagicMock()
    made.config_parser.get_parallel_workers_count.return_value = workers
    made.config_parser.get_target_db_type.return_value = 'postgresql'
    made.config_parser.should_migrate_constraints.return_value = True
    made.config_parser.print_log_message = MagicMock()
    made.on_error_action = 'continue'

    protocol = MagicMock()
    protocol.fetch_all_constraints.return_value = list(constraints)
    protocol.decode_constraint_row.side_effect = lambda row: row
    made.migrator_tables = protocol

    made.created = []
    lock = threading.Lock()

    def worker(constraint_data, target_db_type):
        with lock:
            made.created.append(constraint_data['constraint_name'])
        return constraint_data['constraint_name'] not in failing

    made.constraint_worker = worker
    return made


def kinds_in_order(made):
    """The kind of each constraint, in the order the worker was asked to create it."""
    by_name = {row['constraint_name']: row['constraint_type']
               for row in made.migrator_tables.fetch_all_constraints.return_value}
    return [by_name[name] for name in made.created]


# --------------------------------------------------------------------------------------


def test_a_foreign_key_is_created_after_the_unique_constraint_it_may_reference():
    """
    The defect itself, in the order the migtest example really holds them: the key of
    `fk_children` stands in front of the unique constraint of `fk_parent`, because the planner
    reads the tables by name.
    """
    made = orchestrator_with([
        constraint('fk_children_match_full', 'FOREIGN KEY', 'fk_children'),
        constraint('fk_parent_alt_key', 'UNIQUE', 'fk_parent'),
        constraint('fk_partial_setnull_fk', 'FOREIGN KEY', 'fk_partial_setnull'),
    ])
    made.stdwf_migrate_constraints()

    assert made.created.index('fk_parent_alt_key') < made.created.index('fk_children_match_full')
    assert made.created.index('fk_parent_alt_key') < made.created.index('fk_partial_setnull_fk')


def test_every_foreign_key_comes_after_every_other_constraint():
    made = orchestrator_with([
        constraint('fk_a', 'FOREIGN KEY'),
        constraint('chk_a', 'CHECK'),
        constraint('uq_a', 'UNIQUE'),
        constraint('fk_b', 'FOREIGN KEY'),
        constraint('exc_a', 'EXCLUSION'),
        constraint('uq_b', 'UNIQUE'),
    ])
    made.stdwf_migrate_constraints()

    kinds = kinds_in_order(made)
    first_foreign_key = kinds.index('FOREIGN KEY')
    assert 'FOREIGN KEY' not in kinds[:first_foreign_key]
    assert set(kinds[first_foreign_key:]) == {'FOREIGN KEY'}
    assert len(made.created) == 6


def test_the_first_wave_is_finished_before_the_second_one_starts():
    """
    Parallel inside a wave is what makes the phase fast; parallel across the two is what made
    the key fail in some runs and not in others. The pool of the first wave is closed before the
    second is opened.
    """
    made = orchestrator_with([constraint(f'uq_{index}', 'UNIQUE') for index in range(20)]
                             + [constraint(f'fk_{index}', 'FOREIGN KEY') for index in range(20)],
                             workers=8)
    made.stdwf_migrate_constraints()

    kinds = kinds_in_order(made)
    assert kinds == ['UNIQUE'] * 20 + ['FOREIGN KEY'] * 20


def test_the_case_of_the_type_is_not_what_decides():
    made = orchestrator_with([
        constraint('fk_a', 'foreign key'),
        constraint('uq_a', 'unique'),
    ])
    made.stdwf_migrate_constraints()
    assert made.created == ['uq_a', 'fk_a']


def test_a_constraint_the_configuration_excludes_is_not_created():
    made = orchestrator_with([constraint('uq_a', 'UNIQUE', 'kept'),
                              constraint('fk_a', 'FOREIGN KEY', 'skipped')])
    made.config_parser.should_migrate_constraints.side_effect = \
        lambda table_name: table_name != 'skipped'
    made.stdwf_migrate_constraints()
    assert made.created == ['uq_a']


def test_a_phase_with_no_constraints_at_all_does_nothing_and_says_so():
    made = orchestrator_with([])
    made.stdwf_migrate_constraints()
    assert made.created == []
    status = made.migrator_tables.update_main_status.call_args[0][0]
    assert status['success'] is True


def test_a_constraint_which_failed_does_not_stop_the_wave_behind_it():
    """`on_error: continue` - the whole phase is attempted and the failures are recorded."""
    made = orchestrator_with([constraint('uq_a', 'UNIQUE'), constraint('uq_b', 'UNIQUE'),
                              constraint('fk_a', 'FOREIGN KEY')],
                             workers=1, failing={'uq_a'})
    made.stdwf_migrate_constraints()

    assert made.created == ['uq_a', 'uq_b', 'fk_a']
    recorded = [call.args[0]['row_id']
                for call in made.migrator_tables.update_constraint_status.call_args_list]
    assert 'uq_a' not in recorded, 'a failed constraint is not recorded as migrated OK'
    assert set(recorded) == {'uq_b', 'fk_a'}


def test_the_phase_says_how_it_split_the_work():
    made = orchestrator_with([constraint('uq_a', 'UNIQUE'), constraint('fk_a', 'FOREIGN KEY')])
    made.stdwf_migrate_constraints()
    said = ' '.join(str(call.args[1]) for call in made.config_parser.print_log_message.call_args_list)
    assert '1 constraint(s) first, then 1 foreign key(s)' in said
