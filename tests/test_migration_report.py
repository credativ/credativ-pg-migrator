# SPDX-License-Identifier: GPL-3.0-or-later
"""
The two blocks of the closing summary which NAME rather than count.

The rest of the summary counts: `Indexes 77 | 75 | 2` tells a reader that two indexes are
missing and nothing at all about which two — and the answer is in the protocol tables, one query
away, which is not where somebody reads a migration report from.

  * `[ PARTITIONING ]` — what each table was partitioned by on the source, what it is partitioned
    by on the target, and how many partitions each side has. The line which matters most is the
    table which the source partitions and the target does not: something was dropped, and the
    summary is where a reader looks for what a run changed. §5.6 of
    `development/PARTITIONING_STRATEGY.md`.
  * `[ DETAILED MIGRATION REPORT ]` — every table with its row counts and its duration, every
    object which did not arrive with what the target said about it, and every object which was
    never attempted. The last of the three is the one which is easy to get wrong: an object
    which was not attempted is not an object which failed.

Both are written to the file `summary.report_filename` names, when it names one.

Nothing here talks to a database: the blocks are built from what the protocol answered, and the
protocol is a cursor.

Run with:  python3 -m pytest tests/test_migration_report.py -v
"""

import json
import os
import sys
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.migrator_tables import MigratorTables


class Cursor:
    """
    A protocol which answers by what it was asked. The blocks send several queries each, so
    matching on the text of the query keeps the fixtures readable and independent of the order.
    """

    def __init__(self, answers):
        self.answers = answers
        self.answer = []
        self.asked = []

    def execute(self, query, *args):
        self.asked.append(' '.join(query.split()))
        for needle, rows in self.answers.items():
            if needle in ' '.join(query.split()):
                self.answer = rows
                return
        self.answer = []

    def fetchall(self):
        return self.answer

    def fetchone(self):
        return self.answer[0] if self.answer else None


def report_for(answers):
    made = MigratorTables.__new__(MigratorTables)
    made.protocol_schema = 'migration'
    made.protocol_connection = MagicMock()
    config = MagicMock()
    for name in ('source_table_partitioning', 'target_table_partitioning', 'tables',
                 'data_migration', 'collations', 'text_search', 'user_defined_types', 'domains',
                 'sequences', 'indexes', 'constraints', 'funcprocs', 'triggers', 'views'):
        getattr(config, f'get_protocol_name_{name}').return_value = f'protocol_{name}'
    made.config_parser = config
    return made, Cursor(answers)


## one row per level of the source scheme: root, method, columns, ranges, level
SOURCE_ROWS = 'FROM "migration"."protocol_source_table_partitioning"'
TARGET_ROWS = 'FROM "migration"."protocol_target_table_partitioning"'
TABLE_ROWS = 'FROM "migration"."protocol_tables"'


# --------------------------------------------------------------------------------------
# [ PARTITIONING ]


def partitioning_lines(source=(), target=(), tables=()):
    made, cursor = report_for({SOURCE_ROWS: list(source), TARGET_ROWS: list(target),
                               TABLE_ROWS: list(tables)})
    return made.partitioning_summary_lines(cursor)


def test_a_migration_with_nothing_partitioned_prints_no_block_at_all():
    """An empty block is noise in a report which is read at the end of every run."""
    assert partitioning_lines(tables=[('customers', 'customers', False, None, None, None, True)]) == []


def test_a_preserved_scheme_is_shown_on_both_sides():
    lines = partitioning_lines(
        source=[('orders', 'RANGE', 'order_date', 'orders_2023: x; orders_2024: y', 1)],
        target=[('orders', 'orders_2023: x; orders_2024: y')],
        tables=[('orders', 'orders', True, 'RANGE', 'order_date', None, True)])
    body = '\n'.join(lines)
    assert 'RANGE (order_date)' in body
    assert 'scheme of the source preserved' in body


def test_a_scheme_of_more_than_one_level_says_so():
    """
    A reader who sees only the first level does not know the table is two levels deep - and the
    two `Parts` columns have to count the same thing, or the report invites a comparison which
    is not one.
    """
    lines = partitioning_lines(
        source=[('inventory_movements', 'RANGE', 'moved_at', 'a: x; b: y; c: z', 1),
                ('inventory_movements', 'HASH', 'product_id', 'p0: x; p1: y', 2),
                ('inventory_movements', 'HASH', 'product_id', 'q0: x; q1: y', 2)],
        target=[('inventory_movements', 'a: x; b: y; c: z; p0: x; p1: y; q0: x; q1: y')],
        tables=[('inventory_movements', 'inventory_movements', True, 'RANGE', 'moved_at', None, True)])
    body = '\n'.join(lines)
    assert 'RANGE (moved_at) / HASH (product_id)' in body
    ## seven partitions on each side - three at the top and two under each of two of them.
    ## Both columns say 7, which is the point: they count the same thing
    assert body.count('     7 |') == 2


def test_a_flattened_table_is_the_line_which_matters_most():
    lines = partitioning_lines(
        source=[('payments', 'LIST', 'method', 'a: x; b: y; c: z', 1)],
        tables=[('payments', 'payments', False, None, None, None, True)])
    body = '\n'.join(lines)
    assert 'LIST (method)' in body
    assert 'FLATTENED into one table' in body
    assert '1 table(s) partitioned on the source arrive as one ordinary table: payments.' in body


def test_a_table_the_source_does_not_partition_says_where_its_scheme_came_from():
    lines = partitioning_lines(
        tables=[('currency_rates', 'currency_rates', True, 'RANGE', 'rate_date',
                 json.dumps(['a', 'b', 'c']), True)])
    body = '\n'.join(lines)
    assert 'partitioned by target_partitioning' in body
    ## no row in the target partitioning protocol, so the count comes from the statements
    assert '|     3 |' in body


def test_a_partitioned_table_which_failed_says_so():
    lines = partitioning_lines(
        source=[('orders', 'RANGE', 'order_date', 'a: x', 1)],
        tables=[('orders', 'orders', True, 'RANGE', 'order_date', None, False)])
    assert 'the table FAILED' in '\n'.join(lines)


# --------------------------------------------------------------------------------------
# [ DETAILED MIGRATION REPORT ]


def detailed_lines(tables=(), failures=None):
    answers = {'FROM "migration"."protocol_tables" t': list(tables)}
    answers.update(failures or {})
    made, cursor = report_for(answers)
    return made.detailed_report_lines(cursor)


def table_row(name='orders', source_rows=100, limited=None, target_rows=100, success=True,
              message=None, started=None, completed=None, data_migrated=True):
    import datetime
    started = started or datetime.datetime(2026, 8, 25, 12, 0, 0)
    completed = completed or datetime.datetime(2026, 8, 25, 12, 0, 3)
    return (name, name, source_rows, limited, target_rows, success, message, started, completed,
            data_migrated)


def test_every_table_is_named_with_its_rows_and_its_duration():
    body = '\n'.join(detailed_lines(tables=[table_row('orders', 60001, None, 60001)]))
    assert 'orders' in body and '60,001' in body and '3.0 s' in body and 'OK' in body


def test_a_row_count_which_does_not_match_is_not_smoothed_over():
    body = '\n'.join(detailed_lines(tables=[table_row('orders', 100, None, 97)]))
    assert 'MISMATCH - 3 row(s) missing' in body


def test_a_table_whose_data_was_never_copied_is_not_a_table_which_lost_its_rows():
    """
    `tables.target_table_rows` is what the target held when the plan was made - zero for every
    table of a fresh migration. Reading the counts from there reported every table as having
    lost all of its rows, which is how this was found.
    """
    body = '\n'.join(detailed_lines(
        tables=[table_row('orders', 100, None, None, data_migrated=False)]))
    assert 'structure only - no data was migrated' in body
    assert 'MISMATCH' not in body


def test_a_row_limit_is_what_the_table_is_measured_against():
    """`data_migration_limitation` copies part of a table on purpose - that is not a shortfall."""
    body = '\n'.join(detailed_lines(tables=[table_row('orders', 1000, 40, 40)]))
    assert 'MISMATCH' not in body
    assert '40' in body


def test_an_object_which_did_not_arrive_is_named_with_what_the_target_said():
    body = '\n'.join(detailed_lines(
        tables=[table_row()],
        failures={'FROM "migration"."protocol_views"':
                  [('v_customer_activity', '', False, 'ERROR: function weighted_avg does not exist')]}))
    assert '### What did not arrive' in body
    assert 'v_customer_activity' in body
    assert 'function weighted_avg does not exist' in body
    assert 'view' in body


def test_a_run_in_which_everything_arrived_says_that_plainly():
    body = '\n'.join(detailed_lines(tables=[table_row()]))
    assert 'nothing - every object of the plan was created' in body


def test_an_object_which_was_never_attempted_is_not_an_object_which_failed():
    """
    Reporting the two alike is how a run is read as worse than it was - or, in the other
    direction, how an object nobody tried to create disappears from the report entirely.
    """
    body = '\n'.join(detailed_lines(
        tables=[table_row()],
        failures={'FROM "migration"."protocol_indexes"':
                  [('idx_a', 'orders', None, None)]}))
    assert '### What was not attempted' in body
    assert 'idx_a' in body
    assert 'What did not arrive: nothing' in body


def test_the_two_kinds_of_bad_news_are_told_apart_in_one_run():
    body = '\n'.join(detailed_lines(
        tables=[table_row()],
        failures={'FROM "migration"."protocol_indexes"': [('idx_a', 'orders', None, None)],
                  'FROM "migration"."protocol_views"': [('v_a', '', False, 'boom')]}))
    failed_at = body.index('### What did not arrive')
    not_attempted_at = body.index('### What was not attempted')
    assert body[failed_at:not_attempted_at].count('v_a') == 1
    assert 'idx_a' in body[not_attempted_at:]
    assert 'idx_a' not in body[failed_at:not_attempted_at]


def test_only_the_first_line_of_a_long_error_is_shown():
    body = '\n'.join(detailed_lines(
        tables=[table_row()],
        failures={'FROM "migration"."protocol_views"':
                  [('v_a', '', False, 'ERROR: boom\nLINE 1: select ...\n        ^')]}))
    assert 'ERROR: boom' in body
    assert 'LINE 1' not in body


def test_a_protocol_table_which_is_not_there_does_not_stop_the_report():
    """A migration which never ran a phase has no protocol table for it."""
    made, cursor = report_for({'FROM "migration"."protocol_tables" t': [table_row()]})
    cursor.execute = MagicMock(side_effect=RuntimeError('relation does not exist'))
    lines = made.detailed_report_lines(cursor)
    assert '[ DETAILED MIGRATION REPORT ]' in '\n'.join(lines)


@pytest.mark.parametrize('value, expected', [(None, '-'), (0, '0'), (1234567, '1,234,567')])
def test_a_row_count_is_written_so_it_can_be_read(value, expected):
    assert MigratorTables.thousands(value) == expected
