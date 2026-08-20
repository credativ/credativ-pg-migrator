# SPDX-License-Identifier: GPL-3.0-or-later
"""
Startup validation of the configuration against credativ_pg_migrator/config.schema.json.

A setting the migrator cannot carry out stops the run at the start. An unknown key is
reported but does not, so that a configuration written for a later version stays usable
with an earlier one - while a misspelling, which would otherwise do nothing at all,
is still named.

Run with:  python3 -m pytest tests/test_config_schema_validation.py -v
"""

import copy
import json
import logging
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


class CollectingLogger:
    def __init__(self):
        self.records = []

    def _add(self, level):
        return lambda message: self.records.append((level, message))

    def __getattr__(self, name):
        if name in ('error', 'warning', 'info', 'debug'):
            return self._add(name)
        raise AttributeError(name)

    def messages(self, level):
        return [message for recorded, message in self.records if recorded == level]


def build(tmp_path, config, ignore=False):
    path = tmp_path / 'config.yaml'
    path.write_text(yaml.safe_dump(config), encoding='utf-8')
    logger = CollectingLogger()
    args = types.SimpleNamespace(config=str(path), log_file=None, log_level='INFO',
                                 ignore_config_schema_errors=ignore)
    return args, logger


def parse(tmp_path, config, ignore=False):
    args, logger = build(tmp_path, config, ignore)
    parser = ConfigParser(args, logger)
    return parser, logger


def parse_expecting_failure(tmp_path, config):
    args, logger = build(tmp_path, config)
    with pytest.raises(ValueError) as caught:
        ConfigParser(args, logger)
    return caught.value, logger


# --------------------------------------------------------------------------------------
# what must stop the run


@pytest.mark.parametrize('label,mutate', [
    ('value outside the allowed set',
     lambda c: c['migration'].update({'on_error': 'sometimes'})),
    ('a second value outside the allowed set',
     lambda c: c['migration'].update({'validate_objects': 'maybe'})),
    ('wrong type for a block',
     lambda c: c.update({'summary': 'yes'})),
    ('wrong type for a scalar',
     lambda c: c['migration'].update({'parallel_workers': 'many'})),
    ('missing required block',
     lambda c: c.pop('target')),
    ('a fixed-arity row of the wrong length',
     lambda c: c.update({'data_migration_limitation': [['orders', 'id > 1', 'id', 1000, 'more']]})),
])
def test_a_setting_the_migrator_cannot_carry_out_stops_the_run(tmp_path, base_config, label, mutate):
    config = copy.deepcopy(base_config)
    mutate(config)
    error, logger = parse_expecting_failure(tmp_path, config)
    assert 'configuration schema' in str(error)
    assert logger.messages('error'), f'{label}: the offending setting was not named at ERROR'


def test_the_offending_setting_is_named_by_its_path(tmp_path, base_config):
    config = copy.deepcopy(base_config)
    config['migration']['on_error'] = 'sometimes'
    _error, logger = parse_expecting_failure(tmp_path, config)
    assert any('migration.on_error' in message for message in logger.messages('error'))


def test_the_failure_says_how_to_run_anyway(tmp_path, base_config):
    config = copy.deepcopy(base_config)
    config['migration']['on_error'] = 'sometimes'
    error, _logger = parse_expecting_failure(tmp_path, config)
    assert '--ignore-config-schema-errors' in str(error)


# --------------------------------------------------------------------------------------
# what must NOT stop the run


@pytest.mark.parametrize('label,mutate,where', [
    ('unknown key at the top level', lambda c: c.update({'parallel_wrokers': 8}), 'parallel_wrokers'),
    ('unknown key inside migration', lambda c: c['migration'].update({'migrate_indexs': True}), 'migrate_indexs'),
    ('unknown key inside a nested block', lambda c: c['source'].update({'hostname': 'x'}), 'hostname'),
])
def test_an_unknown_key_is_reported_but_does_not_stop_the_run(tmp_path, base_config, label, mutate, where):
    """
    Forward compatibility: a config written for a later migrator must still run here. The
    misspelling is still named, so it does not just quietly do nothing.
    """
    config = copy.deepcopy(base_config)
    mutate(config)
    _parser, logger = parse(tmp_path, config)
    warnings = logger.messages('warning')
    assert any(where in message for message in warnings), f'{label}: not reported'
    assert not logger.messages('error')


@pytest.mark.parametrize('entry', [
    ['orders', "created_at >= '2024-01-01'", 'created_at'],
    ['orders', "created_at >= '2024-01-01'", 'created_at', 1000000],
])
def test_a_data_migration_limitation_is_accepted_with_and_without_the_row_limit(tmp_path, base_config, entry):
    """
    The row limit is the optional fourth element - the number of rows a table has to exceed
    before the condition is applied to it. An entry without it restricts the table whatever
    its size, which is what every entry did before the limit existed.
    """
    config = copy.deepcopy(base_config)
    config['data_migration_limitation'] = [entry]
    parser, logger = parse(tmp_path, config)
    assert not logger.messages('error')
    assert parser.get_data_migration_limitation() == [
        ['orders', "created_at >= '2024-01-01'", 'created_at', entry[3] if len(entry) == 4 else None]]


def test_a_row_limit_which_is_not_a_number_stops_the_run(tmp_path, base_config):
    config = copy.deepcopy(base_config)
    config['data_migration_limitation'] = [['orders', 'id > 1', 'id', 'a million']]
    with pytest.raises(ValueError):
        parse(tmp_path, config)


def test_a_valid_configuration_says_so_and_starts(tmp_path, base_config):
    _parser, logger = parse(tmp_path, copy.deepcopy(base_config))
    assert any('matches the schema' in message for message in logger.messages('info'))
    assert not logger.messages('error')


@pytest.mark.parametrize('name', sorted(
    os.path.basename(p) for p in __import__('glob').glob(os.path.join(REPO, 'docs', 'configs', '*.yaml'))))
def test_every_shipped_config_passes_the_blocking_validation(tmp_path, name):
    path = os.path.join(REPO, 'docs', 'configs', name)
    with open(path, encoding='utf-8') as handle:
        config = yaml.safe_load(handle)
    parser, logger = parse(tmp_path, config)
    assert parser is not None
    assert not logger.messages('error')


# --------------------------------------------------------------------------------------
# the escape hatch


def test_the_flag_turns_the_blocking_errors_into_warnings(tmp_path, base_config):
    config = copy.deepcopy(base_config)
    config['migration']['on_error'] = 'sometimes'
    _parser, logger = parse(tmp_path, config, ignore=True)
    assert not logger.messages('error')
    assert any('migration.on_error' in message for message in logger.messages('warning'))
    assert any('--ignore-config-schema-errors' in message for message in logger.messages('warning'))


def test_the_flag_is_absent_by_default_and_the_run_still_blocks(tmp_path, base_config):
    """A caller which does not know the flag must get the safe behaviour, not a crash."""
    config = copy.deepcopy(base_config)
    config['migration']['on_error'] = 'sometimes'
    path = tmp_path / 'config.yaml'
    path.write_text(yaml.safe_dump(config), encoding='utf-8')
    args = types.SimpleNamespace(config=str(path), log_file=None, log_level='INFO')
    with pytest.raises(ValueError):
        ConfigParser(args, CollectingLogger())


def test_the_command_line_registers_the_flag():
    from credativ_pg_migrator.command_line import CommandLine
    saved = sys.argv
    try:
        sys.argv = ['credativ-pg-migrator', '--config', 'x.yaml']
        assert CommandLine().parse_arguments().ignore_config_schema_errors is False
        sys.argv = ['credativ-pg-migrator', '--config', 'x.yaml', '--ignore-config-schema-errors']
        assert CommandLine().parse_arguments().ignore_config_schema_errors is True
    finally:
        sys.argv = saved


# --------------------------------------------------------------------------------------
# the schema itself must be able to see unknown keys


@pytest.mark.parametrize('block', ['(root)', 'migration'])
def test_unknown_keys_are_detectable_where_most_options_live(block):
    """
    additionalProperties must be closed, or a misspelt key produces no finding at all -
    which is how a typo used to do nothing, silently.
    """
    with open(os.path.join(REPO, 'credativ_pg_migrator', 'config.schema.json'),
              encoding='utf-8') as handle:
        schema = json.load(handle)
    node = schema if block == '(root)' else schema['properties'][block]
    assert node.get('additionalProperties') is False


# --------------------------------------------------------------------------------------
# the schema must never be stricter than the code


def load_schema():
    with open(os.path.join(REPO, 'credativ_pg_migrator', 'config.schema.json'),
              encoding='utf-8') as handle:
        return json.load(handle)


def schema_node(path):
    node = load_schema()
    for step in path:
        node = node['properties'][step]
    return node


def every_value_the_schema_accepts(node):
    return {value for branch in node['anyOf'] for value in branch['enum']}


# The three settings which have standard values plus aliases, and the tables in the code
# that define them. Everything below is checked against these, so a value added to the
# code without the schema - or the other way round - fails.
STANDARD_AND_ALIASES = [
    (('pattern_syntax',), ConfigParser.PATTERN_SYNTAX_STANDARD, ConfigParser.PATTERN_SYNTAX_ALIASES),
    (('migration', 'validate_objects'), ConfigParser.VALIDATE_OBJECTS_STANDARD, ConfigParser.VALIDATE_OBJECTS_ALIASES),
    (('migration', 'packages_as'), ConfigParser.PACKAGES_AS_STANDARD, ConfigParser.PACKAGES_AS_ALIASES),
]


@pytest.mark.parametrize('path,standard,aliases', STANDARD_AND_ALIASES,
                         ids=lambda v: '.'.join(v) if isinstance(v, tuple) and v and isinstance(v[0], str) else '')
def test_the_schema_accepts_every_value_the_code_accepts(path, standard, aliases):
    """A blocking validator must not refuse a value the migrator would happily use."""
    accepted_by_code = set(standard) | set(aliases)
    accepted_by_schema = every_value_the_schema_accepts(schema_node(path))
    missing = accepted_by_code - accepted_by_schema
    assert not missing, f'accepted by the code but refused by the schema: {sorted(missing)}'


@pytest.mark.parametrize('path,standard,aliases', STANDARD_AND_ALIASES,
                         ids=lambda v: '.'.join(v) if isinstance(v, tuple) and v and isinstance(v[0], str) else '')
def test_the_schema_names_the_same_standard_values_as_the_code(path, standard, aliases):
    """
    The standard values and the aliases must not become a single undifferentiated list -
    the reference reads the two apart, and a reader has to be able to tell which value to
    write.
    """
    node = schema_node(path)
    assert node['x-standard-values'] == list(standard)
    assert node['x-aliases'] == dict(aliases)
    assert node['anyOf'][0]['enum'] == list(standard), 'the first branch must be the standard values'
    assert not set(node['anyOf'][0]['enum']) & set(node['anyOf'][1]['enum']), \
        'a value must be either standard or an alias, never both'


@pytest.mark.parametrize('path,standard,aliases', STANDARD_AND_ALIASES,
                         ids=lambda v: '.'.join(v) if isinstance(v, tuple) and v and isinstance(v[0], str) else '')
def test_every_alias_points_at_a_standard_value(path, standard, aliases):
    for alias, target in aliases.items():
        assert target in standard, f'{alias} points at {target}, which is not a standard value'


@pytest.mark.parametrize('written,expected', [
    ('glob', 'glob'), ('WILDCARD', 'glob'), ('fnmatch', 'glob'),
    ('regexp', 'regex'), ('re', 'regex'), ('sql_like', 'like'),
])
def test_an_alias_is_read_as_its_standard_value(tmp_path, base_config, written, expected):
    config = copy.deepcopy(base_config)
    config['pattern_syntax'] = written
    parser, logger = parse(tmp_path, config)
    assert parser.get_pattern_syntax() == expected
    assert not logger.messages('error')


@pytest.mark.parametrize('setting,written,reader,expected', [
    ('validate_objects', 'verify', 'get_validate_objects_mode', 'check'),
    ('validate_objects', 'skip', 'get_validate_objects_mode', 'off'),
    ('validate_objects', 'YES', 'get_validate_objects_mode', 'retry'),
    ('packages_as', 'schema', 'get_packages_migration_style', 'schemas'),
    ('packages_as', 'prefix', 'get_packages_migration_style', 'functions'),
])
def test_migration_aliases_are_read_as_their_standard_value(tmp_path, base_config, setting,
                                                            written, reader, expected):
    config = copy.deepcopy(base_config)
    config['migration'][setting] = written
    parser, logger = parse(tmp_path, config)
    assert getattr(parser, reader)() == expected
    assert not logger.messages('error')


@pytest.mark.parametrize('written', ['GLOB', 'Regex', 'LIKE'])
def test_a_case_insensitive_setting_is_not_refused_on_its_capitalisation(tmp_path, base_config, written):
    """The code lower-cases these before reading them, so the schema must see the same."""
    config = copy.deepcopy(base_config)
    config['pattern_syntax'] = written
    parser, logger = parse(tmp_path, config)
    assert not logger.messages('error')
    assert parser.get_pattern_syntax() == written.lower()


@pytest.mark.parametrize('setting,written', [
    ('names_case_handling', 'LOWER'),
    ('packages_as', 'Functions'),
    ('validate_objects', 'RETRY'),
])
def test_case_insensitive_migration_settings_are_not_refused(tmp_path, base_config, setting, written):
    config = copy.deepcopy(base_config)
    config['migration'][setting] = written
    _parser, logger = parse(tmp_path, config)
    assert not logger.messages('error')


def test_validate_objects_still_accepts_a_boolean(tmp_path, base_config):
    for value in (True, False, None):
        config = copy.deepcopy(base_config)
        config['migration']['validate_objects'] = value
        _parser, logger = parse(tmp_path, config)
        assert not logger.messages('error'), f'validate_objects: {value!r} was refused'


@pytest.mark.parametrize('path,standard,aliases', STANDARD_AND_ALIASES,
                         ids=lambda v: '.'.join(v) if isinstance(v, tuple) and v and isinstance(v[0], str) else '')
def test_a_wrong_value_is_told_which_standard_values_exist(tmp_path, base_config, path, standard, aliases):
    """
    A setting with aliases fails as "not valid under any of the given schemas", which says
    nothing about what to write. The message must name the standard values instead.
    """
    config = copy.deepcopy(base_config)
    holder = config
    for step in path[:-1]:
        holder = holder.setdefault(step, {})
    holder[path[-1]] = 'definitely_not_a_valid_value'
    _error, logger = parse_expecting_failure(tmp_path, config)
    reported = ' '.join(logger.messages('error'))
    assert 'not valid under any of the given schemas' not in reported
    for value in standard:
        assert repr(value) in reported, f'{value} was not named in: {reported}'
