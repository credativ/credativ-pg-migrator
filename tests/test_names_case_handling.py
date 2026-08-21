# SPDX-License-Identifier: GPL-3.0-or-later
"""
`names_case_handling`, and the rule the whole migrator follows about names.

The rule has two halves and both matter:

  * the target **schema** is used exactly as the configuration spells it - it is never
    case-converted, because the user wrote it and means it;

  * every object **name inside** that schema is spelled the way `names_case_handling` says,
    and the protocol tables record **both** spellings: `source_*` is what was read from the
    source, `target_*` is what was created in the target.

The source spelling is never converted, and that is not a detail. A source can hold CUSTOMER
and Customer as two different tables; if the record of what was read were folded, the migrator
would have no way of telling them apart afterwards - and the whole point of the protocol tables
is to be the record of what really happened.

The same fact has a consequence which the migrator has to answer for: case folding is not
injective, so `lower` and `upper` can make one target object out of two source objects. That is
refused before anything is created - see the collision tests at the end.

Nothing here connects to anything.

Run with:  python3 -m pytest tests/test_names_case_handling.py -v
"""

import os
import re
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

MIGRATOR_TABLES = os.path.join(REPO, 'credativ_pg_migrator', 'migrator_tables.py')


class Config:
    """Only what the pieces under test ask of a configuration."""

    def __init__(self, case='lower'):
        self.case = case
        self.messages = []

    def get_names_case_handling(self):
        return self.case

    def convert_names_case(self, name):
        if name is None:
            return None
        if self.case == 'lower':
            return name.lower()
        if self.case == 'upper':
            return name.upper()
        return name

    def print_log_message(self, level, message):
        self.messages.append((level, str(message)))


## --------------------------------------------------------------------------------------
## the boundary which records the target spelling


def normaliser(case='lower'):
    from credativ_pg_migrator.migrator_tables import MigratorTables
    tables = MigratorTables.__new__(MigratorTables)
    tables.config_parser = Config(case)
    return tables


@pytest.mark.parametrize('case,expected', [('lower', 'customers'), ('upper', 'CUSTOMERS'),
                                           ('keep', 'CusTomers')])
def test_a_target_name_is_recorded_as_the_target_spells_it(case, expected):
    row = normaliser(case).with_target_names({'target_table_name': 'CusTomers'})
    assert row['target_table_name'] == expected


def test_the_source_name_is_never_touched():
    row = normaliser('lower').with_target_names({
        'source_schema_name': 'MigTest', 'source_table_name': 'CusTomers',
        'source_column_name': 'CustID', 'index_name': 'IX_Cust', 'constraint_name': 'PK_Cust',
        'trigger_name': 'TR_Cust'})
    assert row['source_schema_name'] == 'MigTest'
    assert row['source_table_name'] == 'CusTomers'
    assert row['source_column_name'] == 'CustID'
    ## these three are the source spelling as well - they are the bare columns of the
    ## protocol tables, and their target counterpart is target_index_name and its siblings
    assert row['index_name'] == 'IX_Cust'
    assert row['constraint_name'] == 'PK_Cust'
    assert row['trigger_name'] == 'TR_Cust'


def test_the_target_schema_is_not_converted():
    """The user wrote it in the configuration and means it exactly as written."""
    row = normaliser('lower').with_target_names({'target_schema_name': 'MigTest',
                                                 'target_table_name': 'CusTomers'})
    assert row['target_schema_name'] == 'MigTest'
    assert row['target_table_name'] == 'customers'


def test_every_kind_of_target_name_is_covered():
    settings = {key: 'MixedCase' for key in normaliser().TARGET_NAME_KEYS}
    row = normaliser('lower').with_target_names(settings)
    assert set(row.values()) == {'mixedcase'}


def test_an_empty_name_is_left_alone():
    row = normaliser('lower').with_target_names({'target_table_name': '', 'target_alias_name': None})
    assert row['target_table_name'] == ''
    assert row['target_alias_name'] is None


def test_the_conversion_is_idempotent():
    """
    A caller which converts on its own may stay as it is - and several do. Applying it twice
    has to answer what applying it once answered.
    """
    once = normaliser('lower').with_target_names({'target_table_name': 'CusTomers'})
    twice = normaliser('lower').with_target_names(once)
    assert once == twice


def test_the_dictionary_of_the_caller_is_not_changed():
    settings = {'target_table_name': 'CusTomers'}
    normaliser('lower').with_target_names(settings)
    assert settings['target_table_name'] == 'CusTomers'


## --------------------------------------------------------------------------------------
## the protocol tables: every source name has a target counterpart


def create_table_columns(create_fn):
    src = open(MIGRATOR_TABLES).read()
    body = re.search(rf'def {create_fn}\(self\):(.*?)(?=\n    def )', src, re.S).group(1)
    block = re.search(r'CREATE TABLE IF NOT EXISTS.*?\n(.*?)\n\s*\)\s*\n?\s*"""', body, re.S)
    return [line.strip().rstrip(',').split()[0].lstrip('(')
            for line in block.group(1).splitlines() if line.strip()]


def decoded_positions(decode_fn):
    src = open(MIGRATOR_TABLES).read()
    body = re.search(rf'def {decode_fn}\(self, row\):(.*?)(?=\n    def )', src, re.S).group(1)
    return {int(index): key for key, index in re.findall(r"'(\w+)':\s*row\[(\d+)\]", body)}


PROTOCOL_TABLES = [
    ('create_table_for_indexes', 'decode_index_row', 'target_index_name'),
    ('create_table_for_constraints', 'decode_constraint_row', 'target_constraint_name'),
    ('create_table_for_constraints', 'decode_constraint_row', 'target_referenced_table_name'),
    ('create_table_for_triggers', 'decode_trigger_row', 'target_trigger_name'),
    ('create_table_for_default_values', 'decode_default_value_row', 'target_default_value_name'),
]


@pytest.mark.parametrize('create_fn,decode_fn,column', PROTOCOL_TABLES,
                         ids=[f"{c[2]}" for c in PROTOCOL_TABLES])
def test_the_protocol_table_records_the_target_spelling(create_fn, decode_fn, column):
    """
    These four tables used to hold one bare name column and nothing else, so there was nowhere
    to put the name the target really has.
    """
    assert column in create_table_columns(create_fn)
    assert column in decoded_positions(decode_fn).values()


ALIGNED = [
    ('create_table_for_indexes', 'decode_index_row'),
    ('create_table_for_constraints', 'decode_constraint_row'),
    ('create_table_for_triggers', 'decode_trigger_row'),
    ('create_table_for_default_values', 'decode_default_value_row'),
    ('create_table_for_tables', 'decode_table_row'),
    ('create_table_for_views', 'decode_view_row'),
    ('create_table_for_funcprocs', 'decode_funcproc_row'),
    ('create_table_for_sequences', 'decode_sequence_row'),
]


@pytest.mark.parametrize('create_fn,decode_fn', ALIGNED, ids=[c[1] for c in ALIGNED])
def test_the_decoder_matches_the_column_order_of_its_table(create_fn, decode_fn):
    """
    The decoders read a row by position, so a column added in the middle of a CREATE TABLE
    silently shifts every key behind it - the kind of defect which shows up as a name in a
    timestamp column, far from where it was made.
    """
    columns = create_table_columns(create_fn)
    for index, key in sorted(decoded_positions(decode_fn).items()):
        assert index < len(columns), f"{decode_fn} reads row[{index}], past the end of the table"
        assert columns[index] == key, (
            f"{decode_fn} calls row[{index}] {key!r} but the column there is {columns[index]!r}")


## --------------------------------------------------------------------------------------
## the source names are read unchanged


SOURCE_SIDE_METHODS = ('fetch_table_names', 'fetch_table_columns', 'fetch_indexes',
                       'fetch_constraints', 'fetch_sequences', 'fetch_triggers')


def connector_files():
    directory = os.path.join(REPO, 'credativ_pg_migrator', 'connectors')
    return sorted(name for name in os.listdir(directory) if name.endswith('_connector.py'))


@pytest.mark.parametrize('filename', connector_files())
def test_a_connector_reads_the_source_names_unchanged(filename):
    """
    What `fetch_*` answers is the record of what the source is called, and it has to be the
    source's own spelling - the target connector applies names_case_handling when it builds
    the DDL. ibm_db2_luw used to convert here, which is why the protocol held a source name
    the source does not have, and why the data of a table was read with a name Db2 - where a
    delimited identifier is case sensitive - does not know.
    """
    import ast
    path = os.path.join(REPO, 'credativ_pg_migrator', 'connectors', filename)
    source = open(path).read()
    offenders = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.FunctionDef) and node.name in SOURCE_SIDE_METHODS:
            segment = ast.get_source_segment(source, node) or ''
            if 'convert_names_case' in segment:
                offenders.append(node.name)
    assert not offenders, f"{filename} converts the case of a source name in {offenders}"


## --------------------------------------------------------------------------------------
## the collision: two objects of the source, one object of the target


class FakeCursor:
    def __init__(self, rows_by_table):
        self.rows_by_table = rows_by_table
        self.rows = []

    def execute(self, query):
        table = re.search(r'FROM "[^"]+"\."([^"]+)"', query).group(1)
        self.rows = self.rows_by_table.get(table, [])

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class FakeConnection:
    def __init__(self, rows_by_table):
        self.rows_by_table = rows_by_table

    def cursor(self):
        return FakeCursor(self.rows_by_table)


class FakeProtocolConnection:
    def __init__(self, rows_by_table):
        self.connection = FakeConnection(rows_by_table)


class FakeProtocol:
    """The shape the planner asks of migrator_tables: a schema and a connection."""

    def __init__(self, rows_by_table):
        self.protocol_schema = 'protocol'
        self.protocol_connection = FakeProtocolConnection(rows_by_table)


class CollisionConfig(Config):
    """The configuration a planner asks for the names of its protocol tables."""

    def __getattr__(self, name):
        if name.startswith('get_protocol_name_'):
            return lambda: name[len('get_protocol_name_'):]
        raise AttributeError(name)


def planner_with(rows_by_table, case='lower'):
    from credativ_pg_migrator.planner import Planner
    planner = Planner.__new__(Planner)
    planner.config_parser = CollisionConfig(case)
    planner.migrator_tables = FakeProtocol(rows_by_table)
    return planner


def test_two_tables_which_become_one_stop_the_run():
    planner = planner_with({'tables': [('migtest', 'customer', 'CUSTOMER, Customer')]})
    with pytest.raises(ValueError) as raised:
        planner.check_target_name_collisions()
    message = str(raised.value)
    assert 'CUSTOMER, Customer' in message
    assert 'customer' in message
    assert 'names_case_handling' in message


def test_the_message_says_nothing_was_created():
    planner = planner_with({'tables': [('migtest', 'customer', 'CUSTOMER, Customer')]})
    with pytest.raises(ValueError) as raised:
        planner.check_target_name_collisions()
    assert 'Nothing has been created or dropped' in str(raised.value)


def test_the_message_offers_the_way_out():
    planner = planner_with({'tables': [('migtest', 'customer', 'CUSTOMER, Customer')]})
    with pytest.raises(ValueError) as raised:
        planner.check_target_name_collisions()
    assert 'names_case_handling: keep' in str(raised.value)


def test_a_run_without_a_collision_goes_on():
    planner = planner_with({})
    planner.check_target_name_collisions()
    assert any('no two names of the source become one' in message
               for _level, message in planner.config_parser.messages)


def test_keep_can_never_collide_and_is_not_checked():
    """
    With 'keep' every name of the source stays what it is, so two of them cannot become one -
    and asking the protocol tables about it would be work with a known answer.
    """
    planner = planner_with({'tables': [('migtest', 'customer', 'CUSTOMER, Customer')]}, case='keep')
    planner.check_target_name_collisions()


def test_columns_are_checked_within_their_table():
    planner = planner_with({'columns': [('migtest', 'orders', 'total', 'TOTAL, Total')]})
    with pytest.raises(ValueError) as raised:
        planner.check_target_name_collisions()
    assert 'columns TOTAL, Total' in str(raised.value)
    assert '"migtest"."orders"."total"' in str(raised.value)


def test_every_kind_of_object_is_checked():
    planner = planner_with({})
    checked = {entry[4] for entry in planner.COLLISION_CHECKS}
    assert {'table', 'column', 'view', 'sequence', 'index', 'constraint', 'trigger',
            'user defined type', 'domain', 'collation', 'text search object'} <= checked


def test_all_of_the_collisions_are_reported_not_only_the_first():
    planner = planner_with({
        'tables': [('migtest', 'customer', 'CUSTOMER, Customer')],
        'views':  [('migtest', 'v_sales', 'V_SALES, V_Sales')]})
    with pytest.raises(ValueError) as raised:
        planner.check_target_name_collisions()
    message = str(raised.value)
    assert 'CUSTOMER, Customer' in message
    assert 'V_SALES, V_Sales' in message


def test_a_protocol_table_which_is_not_there_is_reported_and_not_fatal():
    """A kind of object which was not planned has no table - that is not a collision."""
    class MissingConnection:
        def cursor(self):
            raise RuntimeError('relation does not exist')

    from credativ_pg_migrator.planner import Planner
    planner = Planner.__new__(Planner)
    planner.config_parser = CollisionConfig('lower')
    planner.migrator_tables = FakeProtocol({})
    planner.migrator_tables.protocol_connection.connection = MissingConnection()
    planner.check_target_name_collisions()
    assert any('could not be read' in message for _level, message in planner.config_parser.messages)


## --------------------------------------------------------------------------------------
## the routines, which the orchestrator plans and therefore checks itself


def orchestrator_with(case='lower'):
    from credativ_pg_migrator.orchestrator import Orchestrator
    orchestrator = Orchestrator.__new__(Orchestrator)
    orchestrator.config_parser = Config(case)
    return orchestrator


def test_two_routines_which_become_one_stop_the_run():
    routines = {1: {'name': 'GET_TOTAL', 'arguments': 'integer'},
                2: {'name': 'Get_Total', 'arguments': 'integer'}}
    with pytest.raises(ValueError) as raised:
        orchestrator_with().check_funcproc_name_collisions(routines)
    assert 'GET_TOTAL, Get_Total' in str(raised.value)


def test_two_routines_of_one_name_and_different_arguments_are_not_a_collision():
    """PostgreSQL tells overloads apart by their arguments, exactly as the source does."""
    routines = {1: {'name': 'get_total', 'arguments': 'integer'},
                2: {'name': 'get_total', 'arguments': 'text'}}
    orchestrator_with().check_funcproc_name_collisions(routines)


def test_routines_are_not_checked_under_keep():
    routines = {1: {'name': 'GET_TOTAL', 'arguments': ''}, 2: {'name': 'Get_Total', 'arguments': ''}}
    orchestrator_with('keep').check_funcproc_name_collisions(routines)


def test_no_routines_at_all_is_not_an_error():
    orchestrator_with().check_funcproc_name_collisions({})
