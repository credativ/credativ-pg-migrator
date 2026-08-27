# SPDX-License-Identifier: GPL-3.0-or-later
"""
Partitioning, PostgreSQL to PostgreSQL - `development/PARTITIONING_STRATEGY.md` §2.4 and §4.

Three things a migration can do with a table, and one it must never do:

  * **preserve** the scheme of the source - the parent is created `PARTITION BY …` and every
    partition of the source becomes a partition of the target, sub-partitions and all;
  * **flatten** it into one ordinary table, which is a change the run says out loud;
  * **repartition** it by `target_partitioning`, a scheme the source never had, which wins over
    both of the above because somebody wrote it out by hand;
  * and never migrate a **partition** as a table of its own. That is what the connector used to
    do: `fetch_table_names()` answers the parent AND every partition, so the rows were written
    twice - the parent answers all of them - and the partition was attached to a parent nothing
    had partitioned, which PostgreSQL refuses.

Global in `migration.source_partitioning` and per table through `table_settings`, the same
mechanism the migrate_* switches use.

The feasibility half runs in the pre-migration analysis, where nothing has been created yet:
what the target version cannot build, a scheme whose key could not be read, and - for
`target_partitioning` - the rule which breaks migrations, that a unique constraint on a
partitioned table has to contain every partitioning column.

Nothing here talks to a database: the planning module is a pure function of the catalogue
answers, which is what makes that possible.

Run with:  python3 -m pytest tests/test_partitioning.py -v
"""

import os
import sys
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

import datetime

from credativ_pg_migrator import partitioning
from credativ_pg_migrator.connectors.postgresql_connector import PostgreSQLConnector


# --------------------------------------------------------------------------------------
# a source schema to decide about


def parent(key='RANGE (created_at)', method='RANGE', columns=('created_at',), partitions=()):
    return {
        'is_partitioned': True, 'is_partition': False, 'parent_table': '',
        'method': method, 'columns': list(columns), 'key_definition': key, 'level': 1,
        'partitions': list(partitions), 'partition_count': len(partitions),
        'engine_specific': {},
    }


def part(name, bound, is_partitioned=False, is_default=False):
    return {'name': name, 'bound': bound, 'is_partitioned': is_partitioned,
            'is_default': is_default, 'rows': None}


def child(parent_table, key='', method='', columns=(), partitions=()):
    return {
        'is_partitioned': bool(key), 'is_partition': True, 'parent_table': parent_table,
        'method': method, 'columns': list(columns), 'key_definition': key, 'level': 2,
        'partitions': list(partitions), 'partition_count': len(partitions),
        'engine_specific': {},
    }


ORDERS = {
    'orders': parent(partitions=[
        part('orders_2023', "FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')", is_partitioned=True),
        part('orders_2024', "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')"),
    ]),
    'orders_2023': child('orders', key='HASH (customer_id)', method='HASH', columns=['customer_id'],
                         partitions=[
                             part('orders_2023_h0', 'FOR VALUES WITH (modulus 2, remainder 0)'),
                             part('orders_2023_h1', 'FOR VALUES WITH (modulus 2, remainder 1)'),
                         ]),
    'orders_2024': child('orders'),
    'orders_2023_h0': child('orders_2023'),
    'orders_2023_h1': child('orders_2023'),
    'customers': {},
}

MODERN = 160000


def plan_of(schemes=None, mode='preserve', selected=None, repartitioned=(), version=MODERN):
    schemes = ORDERS if schemes is None else schemes
    return partitioning.build_plan(
        schemes, selected if selected is not None else list(schemes),
        mode_of=lambda table_name: mode,
        repartitioned_tables=repartitioned,
        target_version_num=version)


# --------------------------------------------------------------------------------------
# a partition is not a table of its own


@pytest.mark.parametrize('table_name', ['orders_2023', 'orders_2024', 'orders_2023_h0'])
def test_a_partition_is_not_migrated_as_a_table(table_name):
    """
    The defect this repairs: the parent of a partitioned table answers ALL of its rows, so a
    run which also migrated every partition wrote every row twice - and tried to attach a
    partition to a parent which nothing had partitioned.
    """
    decision = plan_of()[table_name]
    assert decision.action == partitioning.PART_OF_PARENT
    assert decision.migrated_as_table is False
    assert decision.root_table == 'orders'


def test_a_partition_of_a_partition_belongs_to_the_table_at_the_top():
    assert plan_of()['orders_2023_h0'].root_table == 'orders'


def test_a_plain_table_is_left_alone():
    decision = plan_of()['customers']
    assert decision.action == partitioning.NOT_PARTITIONED
    assert decision.migrated_as_table is True
    assert decision.warnings == [] and decision.issues == []


def test_a_partition_whose_parent_is_not_migrated_becomes_a_table_of_its_own():
    """
    Nothing else can be done with it - and it is not what the user is likely to have meant, so
    it is said rather than done quietly.
    """
    decision = plan_of(selected=['orders_2024', 'customers'])['orders_2024']
    assert decision.action == partitioning.ORPHAN_PARTITION
    assert decision.migrated_as_table is True
    assert any('is not migrated as a partitioned table' in warning
               for warning in decision.warnings)


# --------------------------------------------------------------------------------------
# preserve


def test_the_scheme_of_the_source_is_carried_over_with_every_level():
    decision = plan_of()['orders']
    assert decision.action == partitioning.PRESERVE
    assert decision.key_definition == 'RANGE (created_at)'
    assert [partition.name for partition in decision.partitions] == [
        'orders_2023', 'orders_2023_h0', 'orders_2023_h1', 'orders_2024']


def test_a_partition_is_created_after_the_partition_it_belongs_to():
    """
    The statements are executed in order, so a sub-partition may not stand in front of the
    partition it hangs from.
    """
    names = [partition.name for partition in plan_of()['orders'].partitions]
    assert names.index('orders_2023') < names.index('orders_2023_h0')


def test_a_partition_which_is_itself_partitioned_carries_its_own_key():
    partitions = {partition.name: partition for partition in plan_of()['orders'].partitions}
    assert partitions['orders_2023'].key_definition == 'HASH (customer_id)'
    assert partitions['orders_2024'].key_definition == ''


def test_a_default_partition_is_carried_over_and_what_it_costs_is_said():
    schemes = dict(ORDERS)
    schemes['orders'] = parent(partitions=[part('orders_def', 'DEFAULT', is_default=True)])
    schemes['orders_def'] = child('orders')
    decision = plan_of(schemes, selected=['orders', 'orders_def'])['orders']
    assert decision.partitions[0].is_default is True
    assert any('scan the default partition' in warning for warning in decision.warnings)


def test_a_partition_with_no_bound_cannot_be_created_and_says_so():
    schemes = {'orders': parent(partitions=[part('orders_x', '')]), 'orders_x': child('orders')}
    decision = plan_of(schemes, selected=['orders', 'orders_x'])['orders']
    assert any('has no bound' in issue for issue in decision.issues)


def test_a_scheme_whose_key_could_not_be_read_is_refused():
    schemes = {'orders': parent(key='', method='', columns=[])}
    decision = plan_of(schemes, selected=['orders'])['orders']
    assert any('partitioning key could not be read' in issue for issue in decision.issues)
    assert any('source_partitioning: flatten' in issue for issue in decision.issues)


def test_a_partition_the_filters_left_out_is_created_anyway_and_the_run_says_so():
    """
    The partitions of a preserved scheme belong to their parent; they are not selected one by
    one. A run which silently dropped one would leave a range of the data with nowhere to go.
    """
    decision = plan_of(selected=['orders', 'orders_2023', 'orders_2023_h0', 'orders_2023_h1'])['orders']
    assert any('orders_2024' in warning and 'created anyway' in warning
               for warning in decision.warnings)


# --------------------------------------------------------------------------------------
# the target has to be able to build it


def test_hash_partitioning_needs_postgresql_11():
    decision = plan_of(version=100000)['orders']
    assert any('HASH' in issue and '11' in issue for issue in decision.issues)


def test_a_default_partition_needs_postgresql_11():
    schemes = {'orders': parent(partitions=[part('orders_def', 'DEFAULT', is_default=True)]),
               'orders_def': child('orders')}
    decision = plan_of(schemes, selected=['orders', 'orders_def'], version=100000)['orders']
    assert any('DEFAULT partition' in issue and '11' in issue for issue in decision.issues)


def test_a_scheme_the_target_can_build_raises_nothing():
    assert plan_of()['orders'].issues == []


def test_a_target_whose_version_is_unknown_is_reported_as_unchecked():
    """P2-8: a check which was not made must not read like one which passed."""
    decision = plan_of(version=None)['orders']
    assert decision.issues == []
    assert any('was not checked' in warning for warning in decision.warnings)


# --------------------------------------------------------------------------------------
# flatten


def test_flatten_makes_one_table_and_says_what_was_dropped():
    decision = plan_of(mode='flatten')['orders']
    assert decision.action == partitioning.FLATTEN
    assert decision.partitions == []
    assert any('created as ONE ordinary table' in warning for warning in decision.warnings)
    assert 'FLATTENED' in decision.describe()


def test_the_partitions_of_a_flattened_table_are_still_not_tables_of_their_own():
    """Their rows arrive through the parent either way - migrating them again doubles them."""
    plan = plan_of(mode='flatten')
    assert plan['orders_2024'].action == partitioning.PART_OF_PARENT


def test_flattening_a_hash_scheme_is_the_case_where_nothing_is_lost():
    schemes = {'events': parent(key='HASH (cid)', method='HASH', columns=['cid'])}
    decision = plan_of(schemes, mode='flatten', selected=['events'])['events']
    assert any('hash scheme prunes' in warning for warning in decision.warnings)


def test_a_mode_can_be_decided_per_table():
    plan = partitioning.build_plan(
        {'a': parent(), 'b': parent()}, ['a', 'b'],
        mode_of=lambda table_name: 'flatten' if table_name == 'b' else 'preserve',
        target_version_num=MODERN)
    assert plan['a'].action == partitioning.PRESERVE
    assert plan['b'].action == partitioning.FLATTEN


# --------------------------------------------------------------------------------------
# target_partitioning wins, and says that it did


def test_an_explicit_entry_wins_over_the_scheme_of_the_source():
    decision = plan_of(repartitioned=['orders'])['orders']
    assert decision.action == partitioning.REPARTITION
    assert any('wins' in warning for warning in decision.warnings)


def test_the_partitions_of_a_repartitioned_table_become_tables_of_their_own():
    """
    The scheme of the source is not being kept, so nothing holds its partitions together any
    more. It is the orphan case, and it is reported as one.
    """
    plan = plan_of(repartitioned=['orders'])
    assert plan['orders_2024'].action == partitioning.ORPHAN_PARTITION


# --------------------------------------------------------------------------------------
# the feasibility of a target_partitioning entry - §4.4


ENTRY = {'table_name': 'events', 'partition_by': 'RANGE',
         'partitioning_columns': 'ts', 'date_range': 'month'}


def facts_of(columns=('id', 'ts'), unique_keys=(), types=None, not_null=('ts',),
             null_fraction=None, generated=(), exclusions=(), referenced_by=(),
             inheritance_parent=False, inheritance_child=False, rows=100000,
             btree=True, hash_opclass=True):
    """What fetch_partitioning_facts() answers, for a table built to order."""
    types = types or {}
    return {
        'columns': {name: {
            'type_name': types.get(name, 'timestamp with time zone' if name == 'ts' else 'bigint'),
            'not_null': name in not_null,
            'is_generated': name in generated,
            'has_btree_opclass': btree,
            'has_hash_opclass': hash_opclass,
            'null_fraction': null_fraction,
        } for name in columns},
        'unique_keys': None if unique_keys is None else list(unique_keys),
        'exclusion_constraints': list(exclusions),
        'referenced_by': list(referenced_by),
        'inherits_from_a_plain_table': inheritance_child,
        'is_a_plain_inheritance_parent': inheritance_parent,
        'row_estimate': rows,
        'date_range_types': ('date', 'timestamp without time zone', 'timestamp with time zone'),
    }


def check(entry=None, columns=('id', 'ts'), unique_keys=(), facts=None, **kwargs):
    """
    One target_partitioning entry through the whole check. The bounds are handed in as read,
    so that the entries which ask for a date_range really produce their partitions here.
    """
    if facts is None:
        facts = facts_of(columns=columns, unique_keys=unique_keys)
    bounds = kwargs.pop('bounds', (datetime.date(2025, 1, 1), datetime.date(2025, 3, 1)))
    verdict = partitioning.check_repartitioning(
        entry or ENTRY, list(columns), None,
        target_version_num=kwargs.pop('version', MODERN),
        facts=facts,
        first_value=bounds[0], last_value=bounds[1],
        bounds_were_read=kwargs.pop('bounds_were_read', True),
        **kwargs)
    return verdict.issues, verdict.warnings


def test_a_primary_key_which_does_not_contain_the_partitioning_column_is_refused():
    """
    The rule which breaks migrations, §3.1 of the design: PostgreSQL refuses a unique
    constraint on a partitioned table which does not contain every partitioning column, so the
    table is created, the data is loaded and the constraint fails at the very end.
    """
    issues, _warnings = check(facts=facts_of(unique_keys=[
        {'name': 'events_pkey', 'columns': ['id'], 'is_primary': True}]))
    assert any('PRIMARY KEY events_pkey' in issue and 'does not contain ts' in issue
               for issue in issues)


def test_a_unique_constraint_is_checked_as_well_as_the_primary_key():
    issues, _warnings = check(facts=facts_of(unique_keys=[
        {'name': 'events_pkey', 'columns': ['id', 'ts'], 'is_primary': True},
        {'name': 'events_uq', 'columns': ['code'], 'is_primary': False}]))
    assert any('UNIQUE events_uq' in issue for issue in issues)


def test_a_key_which_contains_the_partitioning_columns_raises_nothing():
    issues, warnings = check(facts=facts_of(unique_keys=[
        {'name': 'events_pkey', 'columns': ['id', 'ts'], 'is_primary': True}]))
    assert issues == [] and warnings == []


def test_a_preserved_scheme_is_checked_against_the_keys_too():
    """
    Not the smaller case, and the one a PostgreSQL source could never show: Oracle keeps a
    primary key which does not contain the partitioning column in a GLOBAL index, which is legal
    and ordinary there. PostgreSQL has no global index, so the table would be created, the data
    would be loaded, and ADD PRIMARY KEY would be refused at the very end of the run.
    """
    decision = plan_of()['orders']
    assert decision.action == partitioning.PRESERVE
    partitioning.check_preserved_keys(
        decision, [{'name': 'orders_pk', 'columns': ['order_id'], 'is_primary': True}])
    assert any('orders_pk' in issue and 'created_at' in issue for issue in decision.issues)
    ## and it says what can be done about a scheme nobody chose, which is not what it says
    ## about an entry somebody wrote
    assert any('source_partitioning: flatten' in issue for issue in decision.issues)


def test_a_preserved_key_which_contains_the_partitioning_column_raises_nothing():
    decision = plan_of()['orders']
    partitioning.check_preserved_keys(
        decision, [{'name': 'orders_pk', 'columns': ['order_id', 'created_at'],
                    'is_primary': True}])
    assert decision.issues == []


def test_a_preserved_table_whose_keys_cannot_be_read_says_the_check_was_not_made():
    decision = plan_of()['orders']
    before = len(decision.warnings)
    partitioning.check_preserved_keys(decision, None)
    assert decision.issues == []
    assert any('NOT checked' in warning for warning in decision.warnings[before:])


def test_only_a_preserved_table_is_checked_this_way():
    """A flattened table keeps its key as it is, and a re-partitioned one is checked by §4.4."""
    decision = plan_of(mode='flatten')['orders']
    partitioning.check_preserved_keys(
        decision, [{'name': 'orders_pk', 'columns': ['order_id'], 'is_primary': True}])
    assert decision.issues == []


def test_a_source_whose_keys_cannot_be_read_says_the_check_was_not_made():
    issues, warnings = check(facts=facts_of(unique_keys=None))
    assert issues == []
    assert any('NOT checked' in warning for warning in warnings)


def test_an_entry_naming_a_table_which_is_not_there_is_refused():
    issues, _warnings = check(table_exists=False)
    assert any('which the source schema does not hold' in issue for issue in issues)


def test_an_entry_naming_a_partition_is_refused():
    issues, _warnings = check(table_is_partition=True)
    assert any('is a PARTITION of another table' in issue for issue in issues)


def test_an_entry_naming_a_column_the_table_does_not_have_is_refused():
    issues, _warnings = check(columns=['id'])
    assert any('names the column(s) ts' in issue for issue in issues)


def test_a_method_postgresql_does_not_have_is_refused():
    entry = dict(ENTRY, partition_by='ROUND ROBIN', date_range=None)
    issues, _warnings = check(entry=entry)
    assert any('RANGE, LIST and HASH and nothing else' in issue for issue in issues)


def test_hash_in_an_entry_needs_postgresql_11():
    entry = {'table_name': 'events', 'partition_by': 'HASH', 'partitioning_columns': 'id'}
    issues, _warnings = check(entry=entry, version=100000)
    assert any('needs PostgreSQL 11' in issue for issue in issues)


def test_a_date_range_over_something_which_is_not_one_range_column_is_refused():
    entry = dict(ENTRY, partitioning_columns='ts, region')
    issues, _warnings = check(entry=entry, columns=['id', 'ts', 'region'])
    assert any('date_range' in issue and 'exactly one' in issue for issue in issues)


@pytest.mark.parametrize('written, expected', [
    ('ts', ['ts']),
    ('ts, region', ['ts', 'region']),
    ('"ts" , "region"', ['ts', 'region']),
    (['ts', 'region'], ['ts', 'region']),
    ('', []),
])
def test_the_partitioning_columns_are_read_however_they_were_written(written, expected):
    assert partitioning.partitioning_columns_of({'partitioning_columns': written}) == expected


# --------------------------------------------------------------------------------------
# what the PostgreSQL connector reads and writes


@pytest.fixture
def connector():
    made = PostgreSQLConnector.__new__(PostgreSQLConnector)
    made.config_parser = MagicMock()
    made.config_parser.convert_names_case = lambda name: (name or '').lower()
    made.connect = MagicMock()
    made.disconnect = MagicMock()
    return made


def with_catalogue(connector, table_row, partition_rows=()):
    cursor = MagicMock()
    cursor.fetchone.return_value = table_row
    cursor.fetchall.return_value = list(partition_rows)
    connector.connection = MagicMock()
    connector.connection.cursor.return_value = cursor
    return cursor


def test_the_connector_reads_the_scheme_of_a_partitioned_parent(connector):
    with_catalogue(connector, (16400, 'p', False, 'RANGE (created_at)', None, None, None),
                   [('orders_2024', "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')", False, 1200),
                    ('orders_def', 'DEFAULT', False, -1)])
    scheme = connector.fetch_table_partitioning(
        {'source_schema_name': 'app', 'source_table_name': 'orders'})

    assert scheme['is_partitioned'] is True and scheme['is_partition'] is False
    assert scheme['method'] == 'RANGE' and scheme['columns'] == ['created_at']
    assert scheme['partition_count'] == 2
    assert scheme['partitions'][1]['is_default'] is True
    ## reltuples is -1 on a relation which was never analysed - unknown, and not zero
    assert scheme['partitions'][0]['rows'] == 1200
    assert scheme['partitions'][1]['rows'] is None


def test_the_connector_reads_that_a_table_is_a_partition(connector):
    with_catalogue(connector, (16401, 'r', True, None, "FOR VALUES FROM (1) TO (2)", 'orders', 'app'))
    scheme = connector.fetch_table_partitioning(
        {'source_schema_name': 'app', 'source_table_name': 'orders_1'})
    assert scheme['is_partition'] is True and scheme['parent_table'] == 'orders'
    assert scheme['partition_bound'] == 'FOR VALUES FROM (1) TO (2)'


@pytest.mark.parametrize('row', [
    (16500, 'r', False, None, None, None, None),   # an ordinary table
    None,                                          # not there at all
])
def test_a_table_which_is_neither_answers_nothing(connector, row):
    with_catalogue(connector, row)
    assert connector.fetch_table_partitioning(
        {'source_schema_name': 'app', 'source_table_name': 'customers'}) == {}


@pytest.mark.parametrize('definition, expected', [
    ('RANGE (created_at)', ('RANGE', ['created_at'])),
    ('LIST (region)', ('LIST', ['region'])),
    ('HASH (customer_id)', ('HASH', ['customer_id'])),
    ('RANGE (created_at, region)', ('RANGE', ['created_at', 'region'])),
    ## an expression key, which PostgreSQL allows: the comma inside the call is not a
    ## separator, and reading it as one would answer two columns which do not exist
    ("RANGE (date_trunc('month'::text, created_at))",
     ('RANGE', ["date_trunc('month'::text, created_at)"])),
    ('', ('', [])),
    ('SOMETHING ELSE', ('', [])),
])
def test_the_partitioning_key_is_read_as_the_catalogue_writes_it(definition, expected):
    assert PostgreSQLConnector.parse_partition_key_definition(definition) == expected


def test_a_partition_is_created_with_the_bound_the_source_had(connector):
    sql = connector.get_create_partition_sql({
        'target_schema_name': 'migtest', 'target_table_name': 'ORDERS_2024',
        'parent_table_name': 'ORDERS',
        'partition_bound': "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')"})
    assert sql == ('CREATE TABLE "migtest"."orders_2024" PARTITION OF "migtest"."orders" '
                   "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')")


def test_a_partition_which_is_itself_partitioned_is_created_with_its_own_key(connector):
    sql = connector.get_create_partition_sql({
        'target_schema_name': 'migtest', 'target_table_name': 'orders_2023',
        'parent_table_name': 'orders', 'partition_bound': 'FOR VALUES FROM (1) TO (2)',
        'key_definition': 'HASH (customer_id)'})
    assert sql.endswith('PARTITION BY HASH (customer_id)')


def test_a_partition_without_a_bound_is_refused_rather_than_written(connector):
    with pytest.raises(ValueError, match='no bound'):
        connector.get_create_partition_sql({
            'target_schema_name': 'migtest', 'target_table_name': 'p',
            'parent_table_name': 't', 'partition_bound': ''})


def test_the_create_table_of_a_partitioned_parent_no_longer_carries_its_own_partition_by():
    """
    The connector used to append `PARTITION BY` out of a fetch_table_names() of the whole
    schema, run once per table, and to answer `CREATE TABLE … PARTITION OF` for a partition -
    against a parent nothing had partitioned. The decision is the planner's now, so a second
    clause from here would be appended to the one the planner writes.
    """
    import ast
    import inspect
    import textwrap
    ## the comments explain what used to be here, so the CODE is what is asserted - ast drops
    ## the comments and keeps everything which runs
    code = ast.unparse(ast.parse(textwrap.dedent(
        inspect.getsource(PostgreSQLConnector.get_create_table_sql))))
    assert 'PARTITION BY' not in code
    assert 'PARTITION OF' not in code


# --------------------------------------------------------------------------------------
# the setting, global and per table


import logging
import types

import yaml

from credativ_pg_migrator.config_parser import ConfigParser

MINIMAL = {
    'migrator': {'type': 'postgresql', 'host': 'h', 'port': 5432, 'username': 'u',
                 'password': 'p', 'database': 'd', 'schema': 'migration'},
    'source': {'type': 'postgresql', 'host': 'h', 'port': 5432, 'username': 'u',
               'password': 'p', 'database': 'd', 'schema': 'app'},
    'target': {'type': 'postgresql', 'host': 'h', 'port': 5432, 'username': 'u',
               'password': 'p', 'database': 'd', 'schema': 'migtest'},
}


def parser_for(tmp_path, fragment=None):
    config = dict(MINIMAL)
    config.update(fragment or {})
    path = tmp_path / 'config.yaml'
    path.write_text(yaml.safe_dump(config), encoding='utf-8')
    logging.disable(logging.CRITICAL)
    try:
        return ConfigParser(
            types.SimpleNamespace(config=str(path), log_file=None, log_level='INFO',
                                  ignore_config_schema_errors=False),
            logging.getLogger('test_partitioning'))
    finally:
        logging.disable(logging.NOTSET)


def test_the_scheme_of_the_source_is_kept_unless_the_configuration_says_otherwise(tmp_path):
    """
    A migration which was not asked to change the shape of a table should not change it, and a
    partitioned table arriving as one ordinary table is a change nobody asked for.
    """
    assert parser_for(tmp_path).get_source_partitioning() == 'preserve'


@pytest.mark.parametrize('written, read_as', [
    ('preserve', 'preserve'), ('PRESERVE', 'preserve'), ('as_is', 'preserve'),
    ('keep', 'preserve'), ('copy', 'preserve'),
    ('flatten', 'flatten'), ('monolith', 'flatten'), ('merge', 'flatten'),
    ('single_table', 'flatten'), ('none', 'flatten'),
])
def test_the_setting_reads_the_word_that_was_written(tmp_path, written, read_as):
    parser = parser_for(tmp_path, {'migration': {'source_partitioning': written}})
    assert parser.get_source_partitioning() == read_as


def test_a_table_can_be_decided_on_its_own(tmp_path):
    parser = parser_for(tmp_path, {
        'migration': {'source_partitioning': 'preserve'},
        'table_settings': [{'table_name': 'events', 'source_partitioning': 'flatten'}],
    })
    assert parser.get_source_partitioning('orders') == 'preserve'
    assert parser.get_source_partitioning('events') == 'flatten'


def test_a_table_listed_for_another_reason_keeps_the_global_setting(tmp_path):
    """
    The rule the migrate_* switches already follow: an entry overrides a setting only when it
    really carries it, so a table listed for its character set does not silently lose its
    partitioning as well.
    """
    parser = parser_for(tmp_path, {
        'migration': {'source_partitioning': 'flatten'},
        'table_settings': [{'table_name': 'events', 'migrate_data': False}],
    })
    assert parser.get_source_partitioning('events') == 'flatten'


def test_a_pattern_decides_for_every_table_it_matches(tmp_path):
    parser = parser_for(tmp_path, {
        'table_settings': [{'table_name': 'log_.*', 'source_partitioning': 'flatten'}],
    })
    assert parser.get_source_partitioning('log_2024') == 'flatten'
    assert parser.get_source_partitioning('orders') == 'preserve'


def test_a_value_nobody_can_carry_out_is_refused_by_the_schema(tmp_path):
    with pytest.raises(ValueError, match='source_partitioning'):
        parser_for(tmp_path, {'migration': {'source_partitioning': 'subpartition'}})


def test_target_partitioning_answers_a_list_even_when_it_is_written_as_a_mapping(tmp_path):
    """
    §0.3 of the design: the accessor defaulted a LIST setting to an empty dict. Harmless while
    it is empty, and a `TypeError: string indices must be integers` for a configuration which
    writes the block as a mapping - the planner iterates the entries.
    """
    parser = parser_for(tmp_path)
    assert parser.get_target_partitioning() == []
    parser.config['target_partitioning'] = {'table_name': 'orders'}
    assert parser.get_target_partitioning() == []
    parser.config['target_partitioning'] = [{'table_name': 'orders'}]
    assert parser.get_target_partitioning() == [{'table_name': 'orders'}]


# --------------------------------------------------------------------------------------
# the planner: what it reads, what it reports, and what it writes into the CREATE TABLE


def planner_with(schemes, mode='preserve', selected=None, repartitioned=(), version=MODERN,
                 candidates=None, absent=None, not_read=None):
    from credativ_pg_migrator.planner import Planner

    made = Planner.__new__(Planner)
    made.config_parser = MagicMock()
    made.source_schema_name = 'app'
    made.target_schema_name = 'migtest'
    made.partitioning_plan = None
    made.partitioning_table_ids = {}
    made.partitioning_note = ''
    made.migrator_tables = MagicMock()
    made.messages = []
    made.config_parser.print_log_message.side_effect = \
        lambda level, message: made.messages.append((level, str(message)))
    made.config_parser.is_object_selected.side_effect = lambda kind, name: (
        (True, None) if selected is None or name in selected else (False, 'excluded'))
    ## names_case_handling: the target scheme is recorded and written in the names the target
    ## has, which for a PostgreSQL source are the names the source had
    made.config_parser.convert_names_case = lambda name: (name or '').lower()
    made.config_parser.get_source_partitioning.side_effect = lambda table_name=None: mode
    made.config_parser.get_target_partitioning.return_value = [
        {'table_name': name, 'partition_by': 'RANGE', 'partitioning_columns': 'ts',
         'date_range': 'month'}
        for name in repartitioned]

    source = MagicMock()
    source.fetch_table_names.return_value = {
        index: {'id': 100 + index, 'table_name': name}
        for index, name in enumerate(sorted(schemes), start=1)}
    source.object_kind_is_absent.return_value = bool(absent)
    source.object_kind_not_read.return_value = not_read
    source.OBJECT_KINDS_ABSENT = {'table_partitioning': absent} if absent else {}
    source.fetch_partitioning_candidates.return_value = candidates
    source.fetch_table_partitioning.side_effect = \
        lambda settings: schemes.get(settings['source_table_name'], {})
    ## a connector which does not read the facts of a table: the planner then falls back to the
    ## unique keys it can get out of fetch_indexes(), which every connector answers
    source.fetch_partitioning_facts.return_value = None
    source.probe_column_bounds.return_value = (datetime.date(2025, 1, 1), datetime.date(2025, 3, 1))
    made.source_connection = source

    target = MagicMock()
    target.get_server_version_num.return_value = version
    target.get_create_partition_sql.side_effect = lambda settings: (
        f'CREATE TABLE "{settings["target_schema_name"]}"."{settings["target_table_name"]}" '
        f'PARTITION OF "{settings["target_schema_name"]}"."{settings["parent_table_name"]}" '
        f'{settings["partition_bound"]}'
        + (f' PARTITION BY {settings["key_definition"]}' if settings.get('key_definition') else ''))
    target.connection.cursor.return_value.fetchall.return_value = []
    made.target_connection = target
    return made


def test_the_planner_reads_the_scheme_once_and_answers_the_same_plan_twice():
    """
    The pre-migration analysis reports it and stdwf_prepare_tables builds from it. A second
    read could answer differently, and a report which does not match the run is worse than no
    report.
    """
    made = planner_with(ORDERS)
    first = made.get_partitioning_plan()
    second = made.get_partitioning_plan()
    assert first is second
    assert made.source_connection.fetch_table_names.call_count == 1


def test_the_planner_only_asks_about_the_tables_which_could_be_partitioned():
    """
    A schema of three hundred ordinary tables must not cost three hundred round trips - the
    connector answers in one query which tables are worth asking about.
    """
    made = planner_with(ORDERS, candidates={'orders', 'orders_2023', 'orders_2024',
                                            'orders_2023_h0', 'orders_2023_h1'})
    made.get_partitioning_plan()
    asked = {call.args[0]['source_table_name']
             for call in made.source_connection.fetch_table_partitioning.call_args_list}
    assert 'customers' not in asked


def test_a_partition_which_the_filters_left_out_is_still_read_for_its_own_scheme():
    """
    Otherwise a sub-partitioned partition comes out with no children, and the scheme is
    silently built one level short.
    """
    made = planner_with(ORDERS, selected=['orders'])
    plan = made.get_partitioning_plan()
    assert [partition.name for partition in plan['orders'].partitions] == [
        'orders_2023', 'orders_2023_h0', 'orders_2023_h1', 'orders_2024']


def test_a_source_which_does_not_read_partitioning_says_so_and_asks_nothing():
    made = planner_with(ORDERS, not_read='Oracle partitions by RANGE, LIST and HASH; this '
                                         'connector does not read it')
    assert made.get_partitioning_plan() == {}
    assert 'does not read it' in made.partitioning_note
    assert not made.source_connection.fetch_table_partitioning.called


def test_a_source_with_no_partitioning_at_all_is_not_the_same_thing():
    made = planner_with({}, absent='SQL Anywhere has no table partitioning at all')
    made.check_partitioning()
    assert any('no table partitioning at all' in message for _level, message in made.messages)


def test_the_analysis_reports_what_is_there_and_what_will_happen_to_it():
    made = planner_with(ORDERS)
    made.check_partitioning()
    report = '\n'.join(message for _level, message in made.messages)
    assert '1 of 6 table(s) are partitioned on the source, holding 4 partition(s)' in report
    ## §4.2's headline counts the schemes of more than one level on their own - §2.2 is about
    ## what reproducing one costs
    assert '1 of them are partitioned on more than one level' in report
    assert 'RANGE (created_at)' in report
    assert 'preserved' in report


def test_the_analysis_of_a_schema_with_nothing_partitioned_says_that(): 
    made = planner_with({'customers': {}, 'orders': {}})
    assert made.check_partitioning() == []
    assert any('no table of the source schema is partitioned' in message
               for _level, message in made.messages)


def test_the_analysis_hands_back_what_cannot_be_built_as_a_blocking_issue():
    made = planner_with(ORDERS, version=100000)
    issues = made.check_partitioning()
    assert any('HASH' in issue and '11' in issue for issue in issues)


def test_the_analysis_writes_the_scheme_into_the_protocol_tables():
    """The two tables which have existed - and been created empty at the start of every run."""
    made = planner_with(ORDERS)
    made.check_partitioning()
    source_rows = made.migrator_tables.insert_source_table_partitioning.call_args_list
    target_rows = made.migrator_tables.insert_target_table_partitioning.call_args_list
    assert [call.args[0]['source_table_name'] for call in source_rows] == ['orders', 'orders_2023']
    assert [call.args[0]['target_table_name'] for call in target_rows] == ['orders']
    assert 'orders_2024' in target_rows[0].args[0]['target_partition_ranges']


def test_a_flattened_table_writes_the_scheme_of_the_source_and_no_target_scheme():
    made = planner_with(ORDERS, mode='flatten')
    made.check_partitioning()
    assert made.migrator_tables.insert_source_table_partitioning.called
    assert not made.migrator_tables.insert_target_table_partitioning.called


def test_the_create_table_of_a_preserved_parent_gets_the_key_and_its_partitions():
    made = planner_with(ORDERS)
    decision = made.get_partitioning_plan()['orders']
    clause, statements = made.partitioning_clause_for(decision, 'orders')

    assert clause == ' PARTITION BY RANGE (created_at)'
    assert len(statements) == 4
    assert statements[0].startswith('CREATE TABLE "migtest"."orders_2023" PARTITION OF "migtest"."orders"')
    assert statements[0].endswith('PARTITION BY HASH (customer_id)')
    ## the sub-partitions hang from the partition they belong to, not from the top table
    assert 'PARTITION OF "migtest"."orders_2023"' in statements[1]


def test_the_parent_is_named_as_the_target_names_it():
    """
    The parent may have been renamed - by names_case_handling, or by an alias used as the
    target name - and the partitions have to point at the name the target really has.
    """
    made = planner_with(ORDERS)
    decision = made.get_partitioning_plan()['orders']
    _clause, statements = made.partitioning_clause_for(decision, 'sales_orders')
    assert 'PARTITION OF "migtest"."sales_orders"' in statements[0]
    ## a sub-partition still points at its own parent, which was not renamed
    assert 'PARTITION OF "migtest"."orders_2023"' in statements[1]


@pytest.mark.parametrize('mode', ['flatten'])
def test_a_flattened_table_gets_no_clause_and_no_partitions(mode):
    made = planner_with(ORDERS, mode=mode)
    decision = made.get_partitioning_plan()['orders']
    assert made.partitioning_clause_for(decision, 'orders') == ('', [])


def test_a_table_which_is_not_partitioned_gets_nothing():
    made = planner_with(ORDERS)
    assert made.partitioning_clause_for(made.get_partitioning_plan()['customers'], 'customers') == ('', [])
    assert made.partitioning_clause_for(None, 'customers') == ('', [])


def repartitioning_planner(pkey_columns):
    made = planner_with({'events': {}}, repartitioned=['events'])
    made.source_connection.fetch_table_columns.return_value = {
        1: {'column_name': 'id'}, 2: {'column_name': 'ts'}}
    made.source_connection.fetch_indexes.return_value = {
        1: {'index_name': 'events_pkey', 'index_type': 'PRIMARY KEY',
            'index_columns': pkey_columns}}
    return made


def test_the_analysis_checks_every_target_partitioning_entry_against_the_source():
    assert repartitioning_planner('"id", "ts"').check_partitioning() == []

    issues = repartitioning_planner('"other"').check_partitioning()
    assert any('does not contain ts' in issue for issue in issues)


def test_a_range_entry_which_creates_no_partition_is_refused():
    """
    The table would be created partitioned and EMPTY, and every row of the migration would be
    refused with `no partition of relation ... found for row` - one row at a time, in the
    middle of the data migration.
    """
    made = repartitioning_planner('"id", "ts"')
    made.config_parser.get_target_partitioning.return_value = [
        {'table_name': 'events', 'partition_by': 'RANGE', 'partitioning_columns': 'ts'}]
    issues = made.check_partitioning()
    assert any('says nothing about which partitions to create' in issue for issue in issues)


def test_the_report_says_what_it_checked_and_found_good():
    """
    An entry which passes says so as plainly as one which fails. A report which only speaks up
    when it is unhappy is one nobody trusts when it is silent.
    """
    made = repartitioning_planner('"id", "ts"')
    made.check_partitioning()
    report = '\n'.join(message for _level, message in made.messages)
    assert 'target_partitioning: events -> RANGE (ts), month' in report
    assert 'can be partitioned as asked' in report
    assert 'contains ts' in report


def test_an_entry_naming_a_table_the_migration_does_not_have_is_blocking():
    made = planner_with({'orders': {}}, repartitioned=['gone'])
    issues = made.check_partitioning()
    assert any('gone' in issue and 'does not hold' in issue for issue in issues)


# --------------------------------------------------------------------------------------
# the generator: a scheme the source never had - §5.3


import datetime


def bounds(date_range, first, last):
    return partitioning.range_partition_bounds(date_range, first, last)


def test_the_end_of_a_partition_is_the_start_of_the_next_one():
    """
    PostgreSQL range bounds are `FROM (a) TO (b)` with a inclusive and b exclusive. The
    generator this replaces wrote `start + 1 interval - 1 day` — an INCLUSIVE end — so the last
    day of every month fell through the gap between one partition and the next and its rows fit
    into no partition at all. §0.3 of the design.
    """
    made = bounds('month', datetime.date(2024, 1, 31), datetime.date(2024, 4, 30))
    assert all(made[index][1] == made[index + 1][0] for index in range(len(made) - 1))
    assert all(end > start for start, end in made)


@pytest.mark.parametrize('date_range, first_start, first_end', [
    ('year', datetime.date(2024, 1, 1), datetime.date(2025, 1, 1)),
    ('quarter', datetime.date(2024, 10, 1), datetime.date(2025, 1, 1)),
    ('month', datetime.date(2024, 12, 1), datetime.date(2025, 1, 1)),
    ('week', datetime.date(2024, 12, 23), datetime.date(2024, 12, 30)),
    ('day', datetime.date(2024, 12, 31), datetime.date(2025, 1, 1)),
])
def test_every_range_is_generated_including_day(date_range, first_start, first_end):
    """
    `day` was accepted by the configuration and produced no partitions at all — P3-3, the defect
    which prompted the whole design. `quarter` was never offered.
    """
    made = bounds(date_range, datetime.date(2025, 1, 1), datetime.date(2025, 1, 15))
    assert made
    assert made[0] == (first_start, first_end)


def test_a_range_covers_the_data_with_one_interval_of_headroom_at_each_end():
    """
    A bound written as a date is read in the TimeZone of the session and a timestamptz value is
    compared in UTC, so a row within a few hours of the first or the last boundary can fall
    outside a range which covers exactly min..max.
    """
    made = bounds('month', datetime.date(2025, 1, 10), datetime.date(2025, 3, 20))
    assert made[0][0] == datetime.date(2024, 12, 1)
    assert made[-1][1] == datetime.date(2025, 5, 1)


def test_an_unknown_range_is_refused_rather_than_producing_nothing():
    with pytest.raises(ValueError, match='date_range'):
        bounds('fortnight', datetime.date(2025, 1, 1), datetime.date(2025, 2, 1))


@pytest.mark.parametrize('value, expected', [
    (datetime.date(2025, 1, 2), datetime.date(2025, 1, 2)),
    (datetime.datetime(2025, 1, 2, 13, 30), datetime.date(2025, 1, 2)),
    ('2025-01-02', datetime.date(2025, 1, 2)),
    ('2025-01-02 13:30:00+00', datetime.date(2025, 1, 2)),
])
def test_the_bounds_are_read_from_a_date_a_timestamp_or_the_text_of_either(value, expected):
    assert partitioning.as_date(value) == expected


def test_a_table_with_no_rows_gets_no_partitions_rather_than_a_guess():
    assert bounds('month', None, None) == []


def test_the_partitions_are_named_and_bounded_the_way_postgresql_takes_them():
    entry = {'partition_by': 'RANGE', 'partitioning_columns': 'rate_date', 'date_range': 'month'}
    made = partitioning.generate_range_partitions(
        entry, 'currency_rates', datetime.date(2025, 1, 1), datetime.date(2025, 1, 20))
    assert [partition.name for partition in made] == [
        'currency_rates_month_20241201', 'currency_rates_month_20250101',
        'currency_rates_month_20250201']
    assert made[1].bound == "FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')"


def test_a_default_partition_is_added_when_it_is_asked_for():
    entry = {'date_range': 'year', 'default_partition': True}
    made = partitioning.generate_range_partitions(
        entry, 'orders', datetime.date(2025, 1, 1), datetime.date(2025, 6, 1))
    assert made[-1].name == 'orders_default'
    assert made[-1].bound == 'DEFAULT' and made[-1].is_default is True


def test_the_partition_names_can_be_written_by_the_entry():
    entry = {'date_range': 'month', 'partition_name': '{table}_p{start:%Y%m}'}
    made = partitioning.generate_range_partitions(
        entry, 'orders', datetime.date(2025, 1, 1), datetime.date(2025, 1, 5))
    assert [partition.name for partition in made] == ['orders_p202412', 'orders_p202501',
                                                      'orders_p202502']


def test_a_name_which_does_not_fit_into_an_identifier_is_refused():
    """PostgreSQL truncates at 63 characters silently, and two partitions end up with one name."""
    entry = {'date_range': 'day'}
    with pytest.raises(ValueError, match='63'):
        partitioning.generate_range_partitions(
            entry, 'a' * 60, datetime.date(2025, 1, 1), datetime.date(2025, 1, 3))


def test_two_partitions_which_would_share_a_name_are_refused():
    entry = {'date_range': 'day', 'partition_name': '{table}_{start:%Y%m}'}
    with pytest.raises(ValueError, match='same name'):
        partitioning.generate_range_partitions(
            entry, 'orders', datetime.date(2025, 1, 1), datetime.date(2025, 1, 5))


def test_a_name_template_which_cannot_be_written_says_which_names_it_may_use():
    entry = {'date_range': 'month', 'partition_name': '{table}_{nonsense}'}
    with pytest.raises(ValueError, match='{table}, {range}, {start} and {end}'):
        partitioning.generate_range_partitions(
            entry, 'orders', datetime.date(2025, 1, 1), datetime.date(2025, 2, 1))


# --------------------------------------------------------------------------------------
# the min/max of the column, asked in the quoting of the source


def test_the_bounds_query_is_built_in_the_quoting_of_the_source(connector):
    """
    §0.3: the planner assembled it with the double quotes of PostgreSQL and sent it to whatever
    the source was — `SELECT min("created_at") … FROM "SCOTT"."ORDERS"` is ORA-00904 against
    Oracle. It is the connector's now.
    """
    cursor = MagicMock()
    cursor.fetchone.return_value = (datetime.date(2025, 1, 1), datetime.date(2025, 3, 1))
    connector.connection = MagicMock()
    connector.connection.cursor.return_value = cursor

    assert connector.probe_column_bounds({
        'source_schema_name': 'app', 'source_table_name': 'currency_rates',
        'column_name': 'rate_date'}) == (datetime.date(2025, 1, 1), datetime.date(2025, 3, 1))
    sent = cursor.execute.call_args[0][0]
    assert sent == ('SELECT min("rate_date"), max("rate_date") FROM "app"."currency_rates"')


def test_more_than_one_column_is_refused_rather_than_written_into_one_min(connector):
    """
    The old code interpolated the whole column list into `min(...)`, so two partitioning
    columns produced `min("a", "b")` — not a function call in any dialect.
    """
    connector.connection = MagicMock()
    with pytest.raises(ValueError, match='ONE column'):
        connector.probe_column_bounds({
            'source_schema_name': 'app', 'source_table_name': 't', 'column_name': 'a, b'})


@pytest.mark.parametrize('module_name, class_name, opening, closing', [
    ('mysql_connector', 'MySQLConnector', '`', '`'),
    ('mariadb_connector', 'MariaDBConnector', '`', '`'),
    ('ms_sql_connector', 'MsSQLConnector', '[', ']'),
    ('sybase_ase_connector', 'SybaseASEConnector', '[', ']'),
    ('postgresql_connector', 'PostgreSQLConnector', '"', '"'),
])
def test_each_source_delimits_an_identifier_the_way_it_really_does(module_name, class_name,
                                                                   opening, closing):
    """
    A double quote is a string LITERAL in MySQL unless ANSI_QUOTES is set, so `min("col")` there
    would answer the constant 'col' rather than the smallest value of the column - a wrong
    answer, silently, which is worse than an error.
    """
    import importlib
    module = importlib.import_module(f'credativ_pg_migrator.connectors.{module_name}')
    connector_class = getattr(module, class_name)
    assert connector_class.IDENTIFIER_QUOTES == (opening, closing)


# --------------------------------------------------------------------------------------
# the deeper diagnosis: everything which cannot work, found before anything is created


def test_a_generated_column_cannot_be_a_partition_key():
    issues, _warnings = check(facts=facts_of(generated=('ts',)))
    assert any('GENERATED column' in issue for issue in issues)


def test_a_type_with_no_btree_operator_class_cannot_carry_a_range_key():
    """
    RANGE and LIST compare the bounds with < and =. A column of a type which has no default
    btree operator class — json, point, xml — cannot be a partition key, and the CREATE TABLE
    is what would say so, after the whole plan was made.
    """
    issues, _warnings = check(facts=facts_of(types={'ts': 'json'}, btree=False))
    assert any('no default btree operator class' in issue for issue in issues)


def test_a_type_with_no_hash_operator_class_cannot_carry_a_hash_key():
    entry = {'table_name': 'events', 'partition_by': 'HASH', 'partitioning_columns': 'ts'}
    issues, _warnings = check(entry=entry, facts=facts_of(hash_opclass=False))
    assert any('no default hash operator class' in issue for issue in issues)


@pytest.mark.parametrize('type_name', ['text', 'bigint', 'numeric(10,2)'])
def test_a_date_range_over_a_column_which_carries_no_date_is_refused(type_name):
    """
    `date_range: month` over a text column passed every check there was and fell over in the
    middle of the run, where the value read from it could not be made into a date.
    """
    issues, _warnings = check(facts=facts_of(types={'ts': type_name}))
    assert any('A range of dates can only be counted over' in issue for issue in issues)


def test_a_nullable_partitioning_column_with_nulls_and_no_default_partition_is_refused():
    """
    A NULL fits no RANGE partition except the DEFAULT one. Without it, those rows are refused
    one at a time in the middle of the data migration — and the statistics of the source say in
    advance that there are some.
    """
    issues, _warnings = check(facts=facts_of(not_null=(), null_fraction=0.12))
    assert any('12.0% of its rows are NULL' in issue for issue in issues)
    assert any('default_partition: true' in issue for issue in issues)


def test_a_default_partition_answers_the_nulls():
    entry = dict(ENTRY, default_partition=True)
    issues, warnings = check(entry=entry, facts=facts_of(not_null=(), null_fraction=0.12))
    assert issues == []


def test_a_nullable_column_nobody_has_analysed_is_reported_as_not_known():
    """P2-8 again: "not checked" and "checked and good" must not read alike."""
    issues, warnings = check(facts=facts_of(not_null=(), null_fraction=None))
    assert issues == []
    assert any('NOT known whether it holds a NULL' in warning for warning in warnings)


def test_a_not_null_partitioning_column_needs_no_default_partition():
    issues, warnings = check(facts=facts_of(not_null=('ts',)))
    assert issues == [] and warnings == []


def test_an_inheritance_parent_cannot_be_partitioned():
    issues, _warnings = check(facts=facts_of(inheritance_parent=True))
    assert any('INHERITANCE hierarchy' in issue for issue in issues)


def test_a_table_which_inherits_cannot_be_partitioned():
    issues, _warnings = check(facts=facts_of(inheritance_child=True))
    assert any('INHERITS from another table' in issue for issue in issues)


def test_an_exclusion_constraint_stops_the_table_from_being_partitioned():
    issues, _warnings = check(facts=facts_of(exclusions=('price_history_no_overlap',)))
    assert any('EXCLUSION constraint price_history_no_overlap' in issue for issue in issues)


def test_a_foreign_key_referencing_the_table_needs_postgresql_12():
    facts = facts_of(referenced_by=[{'name': 'items_order_fk', 'table': 'items'}])
    issues, _warnings = check(facts=facts, version=110000)
    assert any('referencing a PARTITIONED table needs PostgreSQL 12' in issue for issue in issues)


def test_the_same_foreign_key_is_only_a_note_on_a_modern_target():
    facts = facts_of(referenced_by=[{'name': 'items_order_fk', 'table': 'items'}])
    issues, warnings = check(facts=facts)
    assert issues == [] and warnings == []


def test_a_small_table_is_not_worth_partitioning_and_the_run_says_so():
    issues, warnings = check(facts=facts_of(rows=120))
    assert issues == []
    assert any('does not make a small table faster' in warning for warning in warnings)


def test_too_many_partitions_is_a_warning_and_far_too_many_is_a_refusal():
    entry = dict(ENTRY, date_range='day')
    issues, warnings = check(entry=entry,
                             bounds=(datetime.date(2023, 1, 1), datetime.date(2025, 1, 1)))
    assert issues == []
    assert any('creates 734 partitions' in warning for warning in warnings)

    issues, _warnings = check(entry=entry,
                              bounds=(datetime.date(1990, 1, 1), datetime.date(2025, 1, 1)))
    assert any('past what a scheme can carry' in issue for issue in issues)


def test_a_generated_partition_name_which_the_target_already_holds_is_refused():
    issues, _warnings = check(existing_target_names={'events_month_20250101'})
    assert any('a name the target schema already holds' in issue for issue in issues)


def test_a_name_the_generator_refuses_is_reported_as_the_entry_which_asked_for_it():
    entry = dict(ENTRY, partition_name='{table}_{start:%Y}')
    issues, _warnings = check(entry=entry)
    assert any('same name' in issue for issue in issues)


@pytest.mark.parametrize('method, expected', [
    ('HASH', 'the number of partitions to create it with is not part of the configuration'),
    ('LIST', 'the values of each partition are not part of the configuration'),
])
def test_a_method_whose_partitions_cannot_be_generated_yet_says_so_rather_than_creating_none(
        method, expected):
    """
    Both would build a partitioned table with nothing under it, which refuses every row of the
    migration. Saying "not built yet" is the honest answer; an empty partitioned table is not.
    """
    entry = {'table_name': 'events', 'partition_by': method, 'partitioning_columns': 'ts'}
    issues, _warnings = check(entry=entry)
    assert any(expected in issue for issue in issues)


def test_a_table_with_no_rows_in_the_column_is_a_warning_and_not_a_refusal():
    issues, warnings = check(bounds=(None, None))
    assert issues == []
    assert any('holds no row in' in warning for warning in warnings)


def test_a_source_which_reads_no_facts_reports_every_check_it_could_not_make():
    """P2-8: a check which was not made must not read like one which passed."""
    verdict = partitioning.check_repartitioning(
        ENTRY, ['id', 'ts'], None, target_version_num=MODERN, facts=None,
        first_value=datetime.date(2025, 1, 1), last_value=datetime.date(2025, 3, 1),
        bounds_were_read=True)
    assert verdict.can_be_built
    assert any('NOT checked that their types' in warning for warning in verdict.warnings)
    assert any('NOT checked that they contain the partitioning columns' in warning
               for warning in verdict.warnings)


def test_an_entry_which_passes_says_what_it_checked():
    verdict = partitioning.check_repartitioning(
        ENTRY, ['id', 'ts'], None, target_version_num=MODERN, facts=facts_of(unique_keys=[
            {'name': 'events_pkey', 'columns': ['id', 'ts'], 'is_primary': True}]),
        first_value=datetime.date(2025, 1, 1), last_value=datetime.date(2025, 3, 1),
        bounds_were_read=True)
    assert verdict.can_be_built
    assert any('contains ts' in note for note in verdict.notes)
    assert any('is NOT NULL' in note for note in verdict.notes)
    assert any('partition(s) by month' in note for note in verdict.notes)
