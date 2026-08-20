# SPDX-License-Identifier: GPL-3.0-or-later
"""
The two schema names the migrator cannot work without: the schema of its own metadata
(migrator -> schema) and the schema of the target (target -> schema).

The protocol schema is dropped and created again at the start of every run. Named 'public',
that DROP SCHEMA ... CASCADE takes the public schema of the database with everything anybody
else keeps in it - so the name is refused at startup, before a connection is opened, and
again in MigratorTables next to the connection which would carry the DROP out. The second
gate matters because the first one lives in the configuration check, and a check can be
skipped or reordered; the DROP cannot.

Run with:  python3 -m pytest tests/test_schema_names.py -v
"""

import copy
import os
import sys
import types

import pytest
import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.config_parser import ConfigParser

BASE_CONFIG_PATH = os.path.join(REPO, 'docs', 'configs', 'oracle_to_postgresql.yaml')


@pytest.fixture(scope='module')
def base_config():
    with open(BASE_CONFIG_PATH, encoding='utf-8') as handle:
        return yaml.safe_load(handle)


class SilentLogger:
    def __getattr__(self, name):
        return lambda *arguments, **keywords: None


def parse(tmp_path, config):
    path = tmp_path / 'config.yaml'
    path.write_text(yaml.safe_dump(config), encoding='utf-8')
    args = types.SimpleNamespace(config=str(path), log_file=None, log_level='INFO',
                                 ignore_config_schema_errors=True)
    return ConfigParser(args, SilentLogger())


# --------------------------------------------------------------------------------------
# the schema of the migrator metadata


@pytest.mark.parametrize('name', ['public', 'PUBLIC', '  Public  '])
def test_public_as_the_migrator_schema_stops_the_run(tmp_path, base_config, name):
    config = copy.deepcopy(base_config)
    config['migrator']['schema'] = name
    with pytest.raises(ValueError) as caught:
        parse(tmp_path, config)
    assert "cannot be 'public'" in str(caught.value)


@pytest.mark.parametrize('name', ['', '   '])
def test_an_empty_migrator_schema_stops_the_run(tmp_path, base_config, name):
    config = copy.deepcopy(base_config)
    config['migrator']['schema'] = name
    with pytest.raises(ValueError) as caught:
        parse(tmp_path, config)
    assert 'cannot be empty' in str(caught.value)


def test_a_schema_of_its_own_is_accepted(tmp_path, base_config):
    config = copy.deepcopy(base_config)
    config['migrator']['schema'] = 'migration'
    assert parse(tmp_path, config).get_migrator_schema() == 'migration'


def test_a_name_which_only_contains_public_is_accepted(tmp_path, base_config):
    """Only the schema called 'public' is dangerous, not every name containing the word."""
    config = copy.deepcopy(base_config)
    config['migrator']['schema'] = 'public_migration'
    assert parse(tmp_path, config).get_migrator_schema() == 'public_migration'


# --------------------------------------------------------------------------------------
# the schema of the target


@pytest.mark.parametrize('name', ['', '   '])
def test_an_empty_target_schema_stops_the_run(tmp_path, base_config, name):
    config = copy.deepcopy(base_config)
    config['target']['schema'] = name
    with pytest.raises(ValueError) as caught:
        parse(tmp_path, config)
    assert 'cannot be empty' in str(caught.value)


def test_public_as_the_target_schema_is_allowed(tmp_path, base_config):
    """
    Migrating into the public schema of the target is an ordinary thing to do - it is the
    schema of the migrator's own metadata which is dropped, not this one.
    """
    config = copy.deepcopy(base_config)
    config['target']['schema'] = 'public'
    assert parse(tmp_path, config).get_target_schema() == 'public'


# --------------------------------------------------------------------------------------
# the gate in front of the DROP


def build_migrator_tables(schema_name):
    """MigratorTables the way the migration builds it, with the connection kept shut."""
    from unittest.mock import MagicMock, patch
    ## migrator_tables imports psycopg2 at module level - the rest of this file needs no driver
    pytest.importorskip('psycopg2')
    from credativ_pg_migrator.migrator_tables import MigratorTables

    config = MagicMock()
    config.get_migrator_db_type.return_value = 'postgresql'
    config.get_migrator_schema.return_value = schema_name
    with patch('credativ_pg_migrator.migrator_tables.ProtocolPostgresConnection') as connection:
        tables = MigratorTables(MagicMock(), config)
        tables._connection_opened = connection.return_value.connect.called
    return tables


@pytest.mark.parametrize('name,expected', [
    ('public', "cannot be 'public'"),
    ('  PUBLIC ', "cannot be 'public'"),
    ('', 'cannot be empty'),
    ('   ', 'cannot be empty'),
    (None, 'cannot be empty'),
])
def test_the_protocol_tables_refuse_the_name_as_well(name, expected):
    """The same rule where the DROP is issued, and before the connection is opened."""
    with pytest.raises(ValueError) as caught:
        build_migrator_tables(name)
    assert expected in str(caught.value)


def test_a_usable_name_reaches_the_connection():
    tables = build_migrator_tables('migration')
    assert tables.protocol_schema == 'migration'
    assert tables._connection_opened
