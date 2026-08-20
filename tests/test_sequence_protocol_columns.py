# SPDX-License-Identifier: GPL-3.0-or-later
"""
The sequences protocol table: that its columns, the row decoder and the INSERT agree, and
that the two values a sequence has - the one it is declared to start at and the one it
stands at - are kept apart.

decode_sequence_row() reads the row by position. A column added in the middle of the table
shifts every following one, and nothing says so: the migration keeps running and writes the
increment into the minimum value. These tests read the CREATE TABLE the migrator issues and
compare it with the decoder and with the INSERT, so the three cannot drift apart.

Run with:  python3 -m pytest tests/test_sequence_protocol_columns.py -v
"""

import os
import re
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.migrator_tables import MigratorTables

TABLE_NAME = 'protocol_sequences'


class ConnectionRecorder:
    def __init__(self):
        self.statements = []

    def execute_query(self, query, params=None):
        self.statements.append(query)


class ConfigStub:
    def get_protocol_name_sequences(self):
        return TABLE_NAME

    def print_log_message(self, level, message):
        pass


def build_tables():
    tables = MigratorTables.__new__(MigratorTables)
    tables.config_parser = ConfigStub()
    tables.protocol_schema = 'migration'
    tables.protocol_connection = ConnectionRecorder()
    tables.drop_table_sql = """DROP TABLE IF EXISTS "{protocol_schema}"."{table_name}";"""
    return tables


def table_columns():
    """The columns of the sequences protocol table, in the order the migrator creates them."""
    tables = build_tables()
    tables.create_table_for_sequences()
    create = next(s for s in tables.protocol_connection.statements if 'CREATE TABLE' in s)
    body = create[create.index('(', create.index(TABLE_NAME)) + 1:create.rindex(')')]
    columns = []
    depth = 0
    current = ''
    for character in body:
        if character == '(':
            depth += 1
        elif character == ')':
            depth -= 1
        if character == ',' and depth == 0:
            columns.append(current)
            current = ''
        else:
            current += character
    columns.append(current)
    names = []
    for column in columns:
        column = column.strip()
        if not column:
            continue
        names.append(column.split()[0])
    return names


def decoded_positions():
    """What decode_sequence_row() reads out of which position of a row: {name: index}."""
    row = list(range(200))
    return MigratorTables.decode_sequence_row(None, row)


# --------------------------------------------------------------------------------------
# the three descriptions of one row


def test_the_decoder_reads_every_value_out_of_the_position_it_stands_in():
    """
    Position by position, not only in the same order: a decoder which reads two names out of
    one position keeps the order and is still wrong.
    """
    columns = table_columns()
    decoded = decoded_positions()
    ## the decoder stops before the bookkeeping columns at the end of the table
    expected = {name: index for index, name in enumerate(columns[:len(decoded)])}
    assert decoded == expected


def test_the_decoder_covers_every_column_up_to_the_bookkeeping_ones():
    columns = table_columns()
    bookkeeping = ['task_created', 'task_started', 'task_completed', 'success', 'message']
    assert columns[-len(bookkeeping):] == bookkeeping
    assert len(decoded_positions()) == len(columns) - len(bookkeeping)


def test_the_insert_names_columns_which_exist():
    source = open(os.path.join(REPO, 'credativ_pg_migrator', 'migrator_tables.py'), encoding='utf-8').read()
    body = source[source.index('def insert_sequence(self, settings):'):]
    named = re.search(r'INSERT INTO[^(]*\(([^)]*)\)\s*\n\s*VALUES', body, re.S).group(1)
    inserted = [name.strip() for name in named.split(',')]
    columns = set(table_columns())
    assert not [name for name in inserted if name not in columns]


def test_the_insert_passes_one_value_per_column():
    source = open(os.path.join(REPO, 'credativ_pg_migrator', 'migrator_tables.py'), encoding='utf-8').read()
    body = source[source.index('def insert_sequence(self, settings):'):]
    match = re.search(r'INSERT INTO[^(]*\(([^)]*)\)\s*\n\s*VALUES \(([^)]*)\)', body, re.S)
    inserted = [name.strip() for name in match.group(1).split(',')]
    placeholders = [value.strip() for value in match.group(2).split(',')]
    assert len(inserted) == len(placeholders)


# --------------------------------------------------------------------------------------
# the two values of a sequence


def test_the_declared_start_and_the_position_are_separate_columns():
    """
    They used to be one: the connectors wrote the position of the sequence into
    source_start_value, because that is what the target has to start at - so the protocol
    said the sequence starts at 5000 when it was declared to start at 1 and had counted to
    5000, and the target was created with that as its START WITH, which is where a RESTART
    of it would go back to.
    """
    columns = table_columns()
    assert 'source_start_value' in columns
    assert 'source_last_value' in columns
    assert columns.index('source_last_value') == columns.index('source_start_value') + 1


@pytest.mark.parametrize('column', ['source_start_value', 'source_last_value'])
def test_both_values_are_clamped_to_what_a_bigint_column_holds(column):
    """An identity value of Sybase ASE can be a NUMERIC no BIGINT column can hold."""
    source = open(os.path.join(REPO, 'credativ_pg_migrator', 'migrator_tables.py'), encoding='utf-8').read()
    body = source[source.index('def insert_sequence(self, settings):'):]
    clamped = re.search(r'clamp_bigint_sequence_fields\(\s*settings,\s*\(([^)]*)\)', body, re.S).group(1)
    assert f"'{column}'" in clamped
