# SPDX-License-Identifier: GPL-3.0-or-later
"""
SQL Anywhere - the ORDER BY which the chunked read is paged by.

'SELECT TOP m START AT k' takes a window out of the result, and which rows are in that window is
only defined when the result is ordered. The connector built the ORDER BY into a local variable
and never added it to the statement, so every chunk paged an UNORDERED result: the server was
free to answer equal rows in a different order for each chunk, and rows were then read twice or
missed altogether. It was not in the log either - the statement was written there before the
clause was built - so nothing about the run showed it.

Three things are held here:

  - the clause reaches the statement, so a chunk is a window over an ordered result;
  - it is added only where the statement really PAGES. A table read in one pass from the first
    row needs no order, and sorting a whole table for nothing is what adding the clause
    unconditionally would have cost every migration, chunked or not - chunking is off by
    default. A resumed read pages too, even unchunked, because it starts past row 1;
  - a long column is left out of the order (SQL Anywhere cannot sort on one) but stays in the
    SELECT and in the INSERT - it is migrated like every other column.

The statement is asserted by reading the source of migrate_table rather than by running it: the
method needs a live SQL Anywhere connection, and what is under test is the text it builds.

Run with:  python3 -m pytest tests/test_sql_anywhere_chunking.py -v
"""

import ast
import os
import re
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

CONNECTOR = os.path.join(REPO, 'credativ_pg_migrator', 'connectors', 'sql_anywhere_connector.py')


@pytest.fixture(scope='module')
def source():
    return open(CONNECTOR, encoding='utf-8').read()


@pytest.fixture(scope='module')
def migrate_table_body(source):
    return re.search(r'def migrate_table\(self, migrate_target_connection, settings\):(.*?)(?=\n    def )',
                     source, re.S).group(1)


## ------------------------------------------------- the clause reaches the statement


def test_the_order_by_is_added_to_the_query(migrate_table_body):
    assert 'query += order_by_clause' in migrate_table_body, (
        'the ORDER BY is built and never added - the chunks page an unordered result')


def test_the_logged_statement_is_the_one_which_runs(migrate_table_body):
    """
    The statement was written to the log before the clause was built, so even a DEBUG run could
    not show that the order was missing.
    """
    added = migrate_table_body.index('query += order_by_clause')
    logged = migrate_table_body.index('Fetching data with cursor using query')
    assert added < logged


## ------------------------------------------------------- only where it really pages


def test_the_order_is_added_only_when_the_statement_pages(migrate_table_body):
    """
    chunk_start_row_number > 1 covers the resumed read, which pages even without chunking;
    total_chunks > 1 covers the chunked one.
    """
    assert 'is_paging = chunk_start_row_number > 1 or total_chunks > 1' in migrate_table_body
    assert 'if is_paging and orderby_columns:' in migrate_table_body


def paging_decision(chunk_start_row_number, total_chunks, orderby_columns):
    """The decision of the connector, as its source writes it."""
    is_paging = chunk_start_row_number > 1 or total_chunks > 1
    if is_paging and orderby_columns:
        return 'ORDER BY'
    if is_paging:
        return 'WARNING'
    return 'no ORDER BY'


@pytest.mark.parametrize('start,chunks,orderby,expected', [
    (1, 1, '"id"', 'no ORDER BY'),        # the default: one pass from the first row
    (1, 20, '"id"', 'ORDER BY'),          # chunked, first chunk
    (100001, 20, '"id"', 'ORDER BY'),     # chunked, a later chunk
    (40001, 1, '"id"', 'ORDER BY'),       # unchunked but resumed - it pages
    (1, 1, '', 'no ORDER BY'),            # nothing sortable, but nothing to page either
    (100001, 20, '', 'WARNING'),          # nothing sortable and it does page
])
def test_the_decision_for_every_shape_of_read(start, chunks, orderby, expected):
    assert paging_decision(start, chunks, orderby) == expected


def test_a_table_which_cannot_be_ordered_is_reported_rather_than_sorted(migrate_table_body):
    """
    A table of long columns only and no primary key cannot be paged reliably. Saying so beats
    both a silent wrong result and a sort the server refuses.
    """
    assert 'neither a primary key nor a column which can be sorted on' in migrate_table_body
    assert "'WARNING'" in migrate_table_body.split('is_paging = ')[1]


## --------------------------------------------------- a long column is only unsortable


def test_a_long_column_is_left_out_of_the_order(migrate_table_body):
    for data_type in ('long varchar', 'long binary', 'image', 'xml'):
        assert data_type in migrate_table_body, f"{data_type} is not excluded from the ORDER BY"


def test_a_long_column_is_still_selected_and_inserted(migrate_table_body):
    """
    The column is left out of the ORDER BY and of nothing else. Putting the `continue` before
    the select list would drop the value of every LOB column from the migration.
    """
    loop = migrate_table_body.split('for order_num, col in source_columns.items():')[1]
    loop = loop.split('select_columns = ')[0]
    assert loop.index('select_columns_list.append') < loop.index('cannot be sorted on'), (
        'a long column is skipped before it is added to the SELECT list - its data would be lost')
    assert loop.index('insert_columns_list.append') < loop.index('cannot be sorted on')


def test_the_module_still_parses(source):
    ast.parse(source)
