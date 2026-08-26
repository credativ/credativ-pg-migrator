# SPDX-License-Identifier: GPL-3.0-or-later
"""
The table a foreign key points at - how it is recorded and how the constraint is created.

The `constraints` protocol table held `referenced_table_schema` and `referenced_table_name`
without a prefix, both of them the spelling of the SOURCE, and `target_referenced_table_name`
next to them. There was no `target_referenced_table_schema` at all, and everything downstream
made its own guess about where the referenced table had landed:

  - `get_create_constraint_sql()` built `REFERENCES "<schema of the referencing table>"."<name>"`
    and read the source schema into a local variable it then never used, so a foreign key could
    only ever point inside the schema of the table carrying it;

  - `constraint_worker()` looked the referenced table up again by its SOURCE schema. Most
    connectors report a foreign key inside one schema with an EMPTY referenced schema, and the
    lookup `lower(source_schema_name) = lower('')` matches nothing - so every such foreign key
    was skipped with `referenced table .ORDERS not found`, a message which names a schema
    nobody configured;

  - `stdwf_sync_fk_column_types()` looked the parent table up in a dictionary keyed by the name
    of the TARGET, using the name of the source.

The columns are `source_referenced_table_schema` / `source_referenced_table_name` /
`target_referenced_table_schema` / `target_referenced_table_name` now, which is the convention
the rest of the protocol layer already follows (P4-2 of development/OPEN_ISSUES.md).

Nothing here connects to anything.

Run with:  python3 -m pytest tests/test_constraint_referenced_tables.py -v
"""

import ast
import os
import re
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

MIGRATOR_TABLES = os.path.join(REPO, 'credativ_pg_migrator', 'migrator_tables.py')


## --------------------------------------------------------------------- the protocol table


def create_table_columns():
    src = open(MIGRATOR_TABLES).read()
    body = re.search(r'def create_table_for_constraints\(self\):(.*?)(?=\n    def )', src, re.S).group(1)
    block = re.search(r'CREATE TABLE IF NOT EXISTS.*?\n(.*?)\n\s*\)\s*\n?\s*"""', body, re.S)
    return [line.strip().rstrip(',').split()[0].lstrip('(')
            for line in block.group(1).splitlines() if line.strip()]


@pytest.mark.parametrize('column', [
    'source_referenced_table_schema',
    'source_referenced_table_name',
    'target_referenced_table_schema',
    'target_referenced_table_name',
])
def test_the_referenced_table_is_recorded_for_both_sides(column):
    assert column in create_table_columns()


@pytest.mark.parametrize('column', ['referenced_table_schema', 'referenced_table_name'])
def test_the_bare_names_are_gone(column):
    """
    A bare name says nothing about which of the two databases it belongs to, and both of these
    held the source spelling while a `target_*` column stood right next to them.
    """
    assert column not in create_table_columns()


def test_the_insert_writes_every_column_it_names():
    """
    The statement is written by hand, so the column list, the placeholders and the parameters
    have to be counted against each other - a column added in the middle shifts the rest.
    """
    src = open(MIGRATOR_TABLES).read()
    body = re.search(r'def insert_constraint\(self, settings\):(.*?)(?=\n    def )', src, re.S).group(1)
    columns_and_values = re.search(r'INSERT INTO[^\n]*\n(.*?)\n\s*VALUES \(([^)]*)\)', body, re.S)
    columns = [c.strip() for c in columns_and_values.group(1).replace('\n', ' ').strip()
               .lstrip('(').rstrip(')').split(',') if c.strip()]
    placeholders = [p for p in columns_and_values.group(2).split(',') if p.strip()]

    tree = ast.parse(src)
    params = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == 'insert_constraint':
            for statement in ast.walk(node):
                if isinstance(statement, ast.Assign) and getattr(statement.targets[0], 'id', '') == 'params':
                    params = statement.value.elts

    assert params is not None, 'insert_constraint no longer builds a params tuple'
    assert len(columns) == len(placeholders) == len(params), (
        f"{len(columns)} columns, {len(placeholders)} placeholders, {len(params)} parameters")


## ------------------------------------------------------------------ the REFERENCES clause


class Config:
    def __init__(self, names_case='lower'):
        self.names_case = names_case
        self.messages = []

    def print_log_message(self, level, message):
        self.messages.append((level, str(message)))

    def levels(self, level):
        return [message for written, message in self.messages if written == level]

    def get_names_case_handling(self):
        return self.names_case

    def convert_names_case(self, name):
        if name is None:
            return None
        if self.names_case == 'lower':
            return name.lower()
        if self.names_case == 'upper':
            return name.upper()
        return name

    def get_use_aliases_as_target_names(self):
        return False


def constraint_sql(**overrides):
    from credativ_pg_migrator.connectors.postgresql_connector import PostgreSQLConnector

    connector = PostgreSQLConnector.__new__(PostgreSQLConnector)
    connector.config_parser = Config()
    settings = {
        'source_db_type': 'oracle',
        'source_schema_name': 'LEGACY',
        'source_table_name': 'ORDERS',
        'target_schema_name': 'app',
        'target_table_name': 'orders',
        'target_columns': {},
        'constraint_name': 'FK_ORDERS_CUSTOMER',
        'constraint_type': 'FOREIGN KEY',
        'constraint_owner': '',
        'constraint_columns': 'CUSTOMER_ID',
        'referenced_table_schema': 'LEGACY',
        'referenced_table_name': 'CUSTOMERS',
        'referenced_columns': 'ID',
        'constraint_sql': '',
        'constraint_comment': '',
        'delete_rule': 'NO ACTION',
        'update_rule': 'NO ACTION',
        'constraint_status': 'ENABLED',
    }
    settings.update(overrides)
    return connector.get_create_constraint_sql(settings)


def test_the_references_clause_names_the_recorded_target_schema():
    """
    It used to name the schema of the REFERENCING table, whatever the referenced table was
    recorded as - the source schema was read into a local variable and never used.
    """
    sql = constraint_sql(target_referenced_table_schema='shared')
    assert 'REFERENCES "shared"."customers"' in sql


def test_without_a_recorded_schema_the_clause_keeps_the_old_assumption():
    """
    One schema of the source becomes one schema of the target, so the schema of the referencing
    table is right for every migration this was written for - it must simply not be the only
    thing the clause can say.
    """
    sql = constraint_sql()
    assert 'REFERENCES "app"."customers"' in sql


def test_the_source_schema_never_reaches_the_statement():
    """'LEGACY' is a schema of the source; the target does not have it."""
    assert 'LEGACY' not in constraint_sql(target_referenced_table_schema='shared')


## -------------------------------------------------------------------- the constraint worker


class ProtocolTables:
    def __init__(self, referenced_table=None):
        self.referenced_table = referenced_table
        self.statuses = []
        self.started = []
        self.lookups = []

    def update_protocol_task_started(self, object_type, row_id):
        self.started.append((object_type, row_id))

    def update_constraint_status(self, settings):
        self.statuses.append(dict(settings))

    def select_table_by_source(self, settings):
        self.lookups.append(dict(settings))
        return self.referenced_table

    def insert_target_column_alteration(self, settings):
        pass


class TargetConnection:
    def __init__(self, existing=(('app', 'customers'), ('app', 'orders'))):
        self.existing = {(schema.lower(), table.lower()) for schema, table in existing}
        self.executed = []
        self.disconnected = 0

    def connect(self):
        pass

    def disconnect(self):
        self.disconnected += 1

    def target_table_exists(self, schema_name, table_name):
        return ((schema_name or '').lower(), (table_name or '').lower()) in self.existing

    def align_foreign_key_column_types(self, settings):
        return []

    def execute_query(self, query, params=None):
        self.executed.append(query)


def worker_over(constraint_data, referenced_table=None, target_connection=None):
    from credativ_pg_migrator.orchestrator import Orchestrator

    made = Orchestrator.__new__(Orchestrator)
    made.config_parser = Config()
    made.migrator_tables = ProtocolTables(referenced_table)
    made.on_error_action = 'continue'
    connection = target_connection or TargetConnection()
    made.load_connector = lambda which: connection
    return made.constraint_worker(constraint_data, 'postgresql'), made, connection


def foreign_key(**overrides):
    data = {
        'id': 7,
        'constraint_name': 'FK_ORDERS_CUSTOMER',
        'constraint_type': 'FOREIGN KEY',
        'constraint_columns': 'customer_id',
        'referenced_columns': 'id',
        'constraint_sql': 'ALTER TABLE "app"."orders" ADD CONSTRAINT "fk" FOREIGN KEY ("customer_id") REFERENCES "app"."customers" ("id")',
        'source_schema_name': 'LEGACY',
        'source_table_name': 'ORDERS',
        'target_schema_name': 'app',
        'target_table_name': 'orders',
        'target_alias_name': '',
        'source_referenced_table_schema': '',
        'source_referenced_table_name': 'CUSTOMERS',
        'target_referenced_table_schema': 'app',
        'target_referenced_table_name': 'customers',
    }
    data.update(overrides)
    return data


def test_a_referenced_schema_left_empty_means_the_schema_of_the_constraint():
    """
    Most connectors report a foreign key inside one schema with an empty referenced schema.
    The worker looked the table up under that empty string, matched nothing, and skipped EVERY
    such foreign key with 'referenced table .CUSTOMERS not found'.
    """
    result, made, _ = worker_over(foreign_key())
    assert result is True, made.migrator_tables.statuses
    assert made.migrator_tables.lookups == [
        {'source_schema_name': 'LEGACY', 'source_table_name': 'CUSTOMERS'}]


def test_the_recorded_target_names_are_used_when_the_table_was_not_migrated_here():
    """
    A referenced table which is not in the protocol tables of this run - reloaded separately,
    or created by a pre-migration script - is not a reason to skip the constraint. Whether the
    target really holds it is the question, and the target is asked.
    """
    result, made, _ = worker_over(foreign_key(), referenced_table=None)
    assert result is True, made.migrator_tables.statuses


def test_a_referenced_table_which_is_nowhere_is_reported_with_the_reason():
    result, made, connection = worker_over(
        foreign_key(target_referenced_table_schema='', target_referenced_table_name=''))
    assert result is False
    message = made.migrator_tables.statuses[0]['message']
    assert 'not part of this migration' in message
    assert 'LEGACY.CUSTOMERS' in message
    assert connection.disconnected == 1, 'the worker left its connection open'


def test_a_referenced_table_missing_from_the_target_is_reported_and_the_connection_closed():
    connection = TargetConnection(existing=(('app', 'orders'),))
    result, made, _ = worker_over(foreign_key(), target_connection=connection)
    assert result is False
    assert 'does not exist' in made.migrator_tables.statuses[0]['message']
    assert connection.disconnected == 1


def test_the_protocol_row_of_the_referenced_table_wins_over_the_recorded_names():
    """
    A table migrated in this run carries the alias and the target schema it really got, which
    is what the worker has to check - the recorded pair is what a table outside the run falls
    back to.
    """
    referenced = {'target_schema_name': 'other', 'target_table_name': 'CusTomers',
                  'target_alias_name': ''}
    connection = TargetConnection(existing=(('other', 'customers'), ('app', 'orders')))
    result, made, _ = worker_over(foreign_key(), referenced_table=referenced,
                                  target_connection=connection)
    assert result is True, made.migrator_tables.statuses


def test_a_referencing_table_missing_from_the_target_closes_its_connection():
    connection = TargetConnection(existing=(('app', 'customers'),))
    result, made, _ = worker_over(foreign_key(), target_connection=connection)
    assert result is False
    assert 'target table app.orders does not exist' in made.migrator_tables.statuses[0]['message']
    assert connection.disconnected == 1


def test_a_constraint_without_a_statement_is_recorded_and_does_not_crash():
    """
    This branch closed a connection which the branch above it opens, so it raised
    UnboundLocalError and the run reported an error of the migration instead of a constraint
    which had no statement.
    """
    result, made, _ = worker_over(foreign_key(constraint_sql=''))
    assert result is False
    assert made.migrator_tables.statuses[0]['message'] == 'ERROR: no statement could be built for this constraint'
    assert made.config_parser.levels('WARNING'), 'the skipped constraint has to be visible'


def test_a_check_constraint_needs_no_referenced_table():
    check = foreign_key(constraint_type='CHECK', source_referenced_table_name='',
                        target_referenced_table_schema='', target_referenced_table_name='',
                        constraint_sql='ALTER TABLE "app"."orders" ADD CONSTRAINT "c" CHECK (qty > 0)')
    result, made, _ = worker_over(check)
    assert result is True, made.migrator_tables.statuses
    assert made.migrator_tables.lookups == []
