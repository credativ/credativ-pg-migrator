# SPDX-License-Identifier: GPL-3.0-or-later
"""
The settings whose values include the word 'off', written the way they read best.

The configuration is read with PyYAML, which follows YAML 1.1: an unquoted off, on, yes and
no are booleans there, not the words they were written as. Three settings take 'off' -
migration.validate_objects, query_conversion.target_test and query_conversion.output.sidecar
- so all three have to read False as 'off', or the documented value stops the run at the
start with "False is not one of 'off', 'parse', 'explain'".

The configuration is written as text here, not dumped from a dict: yaml.safe_dump quotes
'off' by itself, which is exactly the trap this file is about.

Run with:  python3 -m pytest tests/test_off_valued_settings.py -v
"""

import logging
import os
import sys
import types

import pytest
import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.config_parser import ConfigParser
from credativ_pg_migrator.query_conversion.workflow import probe_statements

BASE = """
migrator:
  type: "postgresql"
  host: "h"
  port: 5432
  username: "u"
  password: "p"
  database: "d"
  schema: "migration"
source:
  type: "sybase_ase"
  host: "h"
  port: 5000
  username: "u"
  password: "p"
  database: "d"
  schema: "dbo"
target:
  type: "postgresql"
  host: "h"
  port: 5432
  username: "u"
  password: "p"
  database: "d"
  schema: "public"
"""


def parser_for(tmp_path, text, ignore_schema_errors=False):
    """A ConfigParser over the written text, schema check included."""
    path = tmp_path / 'config.yaml'
    path.write_text(BASE + text, encoding='utf-8')
    logging.disable(logging.CRITICAL)
    try:
        return ConfigParser(
            types.SimpleNamespace(config=str(path), log_file=None, log_level='INFO',
                                  ignore_config_schema_errors=ignore_schema_errors),
            logging.getLogger('test_off_valued_settings'))
    finally:
        logging.disable(logging.NOTSET)


def test_yaml_really_does_read_an_unquoted_off_as_false():
    """The premise of this file. If this ever fails, the rest of it is pointless."""
    assert yaml.safe_load('a: off')['a'] is False
    assert yaml.safe_load('a: on')['a'] is True
    assert yaml.safe_load('a: "off"')['a'] == 'off'


# --------------------------------------------------------------------------------------
# query_conversion.target_test


@pytest.mark.parametrize('written, read_as', [
    ('off', 'off'),          # the boolean False after YAML 1.1
    ('"off"', 'off'),
    ('no', 'off'),
    ('false', 'off'),
    ('', 'off'),             # a key left empty asks for nothing
    ('parse', 'parse'),
    ('explain', 'explain'),
    ('EXPLAIN', 'explain'),
    ('on', 'explain'),       # the boolean True: the value it has when it is on
    ('true', 'explain'),
])
def test_target_test_reads_the_word_that_was_written(tmp_path, written, read_as):
    parser = parser_for(tmp_path, f"query_conversion:\n  enabled: true\n  target_test: {written}\n")
    assert parser.get_query_conversion_target_test() == read_as


def test_target_test_defaults_to_explain_when_it_is_not_written(tmp_path):
    parser = parser_for(tmp_path, "query_conversion:\n  enabled: true\n")
    assert parser.get_query_conversion_target_test() == 'explain'


def test_an_unknown_target_test_is_handed_on_unchanged(tmp_path):
    """A typo is refused by the schema check first. Started with
    --ignore-config-schema-errors, which is what that switch is for, the value reaches
    probe_statements() as it was written - the accessor must not quietly turn it into the
    default, or the run would silently test something other than what was asked for."""
    parser = parser_for(tmp_path, "query_conversion:\n  enabled: true\n  target_test: 'prepare'\n",
                        ignore_schema_errors=True)
    assert parser.get_query_conversion_target_test() == 'prepare'
    with pytest.raises(ValueError, match='prepare'):
        probe_statements('SELECT 1', {'target_test': 'prepare'})


# --------------------------------------------------------------------------------------
# query_conversion.output.sidecar


@pytest.mark.parametrize('written, read_as', [
    ('off', 'off'),
    ('"off"', 'off'),
    ('no', 'off'),
    ('false', 'off'),
    ('', 'off'),
    ('json', 'json'),
    ('csv', 'csv'),
    ('CSV', 'csv'),
    ('on', 'json'),
    ('true', 'json'),
])
def test_sidecar_reads_the_word_that_was_written(tmp_path, written, read_as):
    parser = parser_for(
        tmp_path,
        f"query_conversion:\n  enabled: true\n  output:\n    sidecar: {written}\n")
    assert parser.get_query_conversion_output_sidecar() == read_as


def test_sidecar_defaults_to_json_when_it_is_not_written(tmp_path):
    parser = parser_for(tmp_path, "query_conversion:\n  enabled: true\n  output:\n    suffix: '_pg'\n")
    assert parser.get_query_conversion_output_sidecar() == 'json'


# --------------------------------------------------------------------------------------
# migration.validate_objects, which has read the booleans since it was written - the model
# the two above now follow


@pytest.mark.parametrize('written, read_as', [
    ('off', 'off'),
    ('no', 'off'),
    ('on', 'retry'),
    ('yes', 'retry'),
    ('retry', 'retry'),
    ('check', 'check'),
])
def test_validate_objects_reads_the_word_that_was_written(tmp_path, written, read_as):
    parser = parser_for(tmp_path, f"migration:\n  validate_objects: {written}\n")
    assert parser.get_validate_objects_mode() == read_as


# --------------------------------------------------------------------------------------
# the schema has to accept what the code reads


def test_an_unquoted_off_does_not_stop_the_run(tmp_path):
    """The schema check raises for a value the migrator cannot carry out. Writing the
    documented value of a documented option must not be such a value - which is what this
    was before the aliases were added."""
    parser = parser_for(tmp_path, "query_conversion:\n"
                                  "  enabled: true\n"
                                  "  target_test: off\n"
                                  "  output:\n"
                                  "    sidecar: off\n")
    assert parser.get_query_conversion_target_test() == 'off'
    assert parser.get_query_conversion_output_sidecar() == 'off'


def test_a_value_the_migrator_cannot_carry_out_still_stops_the_run(tmp_path):
    with pytest.raises(ValueError, match='target_test'):
        parser_for(tmp_path, "query_conversion:\n  enabled: true\n  target_test: maybe\n")
