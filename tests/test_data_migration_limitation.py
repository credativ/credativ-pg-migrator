# SPDX-License-Identifier: GPL-3.0-or-later
"""
How an entry of data_migration_limitation decides which rows of a table are migrated.

Every reader of the restrictions - the planner while it counts the rows, the orchestrator
while it copies them and the validator while it compares the two sides - asks the same
resolver, so one entry cannot mean one thing in one place and something else in another.
The resolver is what these tests exercise; it needs no database, the rows of the protocol
table are handed to it directly.

Run with:  python3 -m pytest tests/test_data_migration_limitation.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.migrator_tables import MigratorTables


class ConfigStub:
    def __init__(self):
        self.messages = []

    def print_log_message(self, level, message):
        self.messages.append((level, message))


def resolver(rows):
    """A MigratorTables which answers with the given rows of data_migration_limitation."""
    tables = MigratorTables.__new__(MigratorTables)
    tables.config_parser = ConfigStub()
    tables.get_records_data_migration_limitation = lambda source_table_name: rows
    return tables


COLUMNS = {1: {'column_name': 'id'}, 2: {'column_name': 'created_at'}}


def resolve(rows, **settings):
    base = {'source_schema_name': 'sales', 'source_table_name': 'orders',
            'source_columns': COLUMNS, 'source_table_rows_all': 5000}
    base.update(settings)
    return resolver(rows).resolve_data_migration_limitation(base)


# --------------------------------------------------------------------------------------
# the column which has to be there


def test_the_condition_is_used_when_the_table_has_the_column():
    assert resolve([("created_at >= '2024-01-01'", 'created_at', None)]) == "created_at >= '2024-01-01'"


def test_the_condition_is_not_used_when_the_table_lacks_the_column():
    """
    One condition written for a group of tables reaches only those which really have the
    column - otherwise it would be sent to the source as SQL naming a column that is not
    there, and the count of every table would fail.
    """
    assert resolve([("archived_at IS NULL", 'archived_at', None)]) == ''


def test_the_column_may_be_named_by_a_pattern():
    assert resolve([("created_at >= '2024-01-01'", 'created.*', None)]) == "created_at >= '2024-01-01'"


def test_a_column_which_is_not_a_usable_pattern_is_compared_as_a_name():
    """An unusable regular expression is reported, and does not end the run."""
    tables = resolver([("id > 1", 'created_at[', None)])
    condition = tables.resolve_data_migration_limitation(
        {'source_schema_name': 'sales', 'source_table_name': 'orders',
         'source_columns': COLUMNS, 'source_table_rows_all': 5000})
    assert condition == ''
    assert any(level == 'WARNING' for level, _message in tables.config_parser.messages)


# --------------------------------------------------------------------------------------
# the row limit


def test_a_table_larger_than_the_limit_is_restricted():
    assert resolve([("created_at >= '2024-01-01'", 'created_at', 1000)],
                   source_table_rows_all=5000) == "created_at >= '2024-01-01'"


def test_a_table_not_larger_than_the_limit_is_migrated_whole():
    """The small reference tables stay complete while the same entry thins out the large ones."""
    assert resolve([("created_at >= '2024-01-01'", 'created_at', 1000)],
                   source_table_rows_all=1000) == ''
    assert resolve([("created_at >= '2024-01-01'", 'created_at', 1000)],
                   source_table_rows_all=17) == ''


def test_without_a_limit_the_size_of_the_table_does_not_matter():
    assert resolve([("created_at >= '2024-01-01'", 'created_at', None)],
                   source_table_rows_all=1) == "created_at >= '2024-01-01'"


def test_an_unknown_row_count_does_not_suppress_the_condition():
    """A caller which cannot say how many rows the table has gets the restriction applied."""
    assert resolve([("created_at >= '2024-01-01'", 'created_at', 1000)],
                   source_table_rows_all=None) == "created_at >= '2024-01-01'"


# --------------------------------------------------------------------------------------
# what the condition itself looks like


def test_the_placeholders_are_substituted():
    condition = resolve([("id IN (SELECT id FROM {source_schema_name}.{source_table_name}_archive)", 'id', None)])
    assert condition == "id IN (SELECT id FROM sales.orders_archive)"


def test_several_matching_entries_are_combined_with_and():
    condition = resolve([("created_at >= '2024-01-01'", 'created_at', None),
                         ("id > 100", 'id', None)])
    assert condition == "created_at >= '2024-01-01' AND id > 100"


def test_only_the_entries_which_apply_are_combined():
    condition = resolve([("created_at >= '2024-01-01'", 'created_at', None),
                         ("archived_at IS NULL", 'archived_at', None),
                         ("id > 100", 'id', 1000000)])
    assert condition == "created_at >= '2024-01-01'"


def test_a_caller_which_does_not_know_the_columns_is_warned():
    """Leaving the table restricted or whole without a word would be the worse answer."""
    tables = resolver([("created_at >= '2024-01-01'", 'created_at', None)])
    condition = tables.resolve_data_migration_limitation(
        {'source_schema_name': 'sales', 'source_table_name': 'orders',
         'source_columns': {}, 'source_table_rows_all': 5000})
    assert condition == ''
    assert any(level == 'WARNING' for level, _message in tables.config_parser.messages)


def test_no_rows_at_all_means_the_whole_table():
    assert resolve(None) == ''
    assert resolve([]) == ''


# --------------------------------------------------------------------------------------
# the shapes the columns of a table arrive in


@pytest.mark.parametrize('columns', [
    {1: {'column_name': 'created_at'}},                      # the protocol tables
    [{'name': 'created_at', 'column_name': 'created_at'}],   # the mapping workflow
    [{'column_name': 'created_at'}],                         # the validator
    ['created_at'],                                          # plain names
])
def test_the_columns_may_arrive_in_any_of_the_shapes_the_callers_hold(columns):
    assert resolve([("created_at >= '2024-01-01'", 'created_at', None)],
                   source_columns=columns) == "created_at >= '2024-01-01'"
