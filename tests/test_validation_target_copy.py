# SPDX-License-Identifier: GPL-3.0-or-later
"""
validation.target_copy - the third connection of a run, and the two accessors which disagreed
about whether it exists.

The validator of the `mapping` workflow needs an untouched copy of the target from *before* the
migration: it is the only way to tell a row the migration wrote from a row which was already
there, which is what makes the `skip` and the two `merge_*` conflict actions checkable at all.
It is configured under `validation.target_copy` rather than at the top level next to `source`
and `target`, which is how it came to be forgotten in one accessor while another knew it:

  - `get_db_config('target_copy')` answered, and
  - `get_db_session_settings('target_copy')` raised `Invalid source_or_target: target_copy`.

Every connector asks for its session settings while it is being built, so the validator could
not build that connection at all and **every table of every mapping validation was reported as
failed** - with the reason, which the validator says per table is a failure of the VALIDATION
and not a measurement of the table. The two now read one definition of what the connections of
a run are.

The other half: `validation.target_copy` forbade two keys the code reads from it. `owner` is
read by the validator (`schema` first, then `owner`, as it does for the target) and was refused
by the schema; `settings` had nowhere to be written even though the connection is a PostgreSQL
one like any other.

Nothing here connects to anything.

Run with:  python3 -m pytest tests/test_validation_target_copy.py -v
"""

import json
import os
import sys

import pytest
from jsonschema import Draft202012Validator

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.config_parser import ConfigParser

SCHEMA_PATH = os.path.join(REPO, 'credativ_pg_migrator', 'config.schema.json')


def parser(**validation):
    made = ConfigParser.__new__(ConfigParser)
    made.config = {
        'workflow': 'mapping',
        'source': {'type': 'oracle', 'database': 'ORCLPDB1', 'schema': 'LEGACY'},
        'target': {'type': 'postgresql', 'database': 'testdb', 'schema': 'public'},
        'validation': validation,
    }
    made.print_log_message = lambda level, message: None
    return made


COPY = {'type': 'postgresql', 'host': 'localhost', 'port': 5432,
        'username': 'postgres', 'password': 'postgres',
        'database': 'testdb_copy', 'schema': 'public'}


@pytest.fixture(scope='module')
def schema():
    return json.load(open(SCHEMA_PATH, encoding='utf-8'))


## ------------------------------------------------ the two accessors agree about the sides


def test_the_sides_of_a_run_are_defined_once():
    assert ConfigParser.DATABASE_SIDES == ('source', 'target', 'target_copy')


@pytest.mark.parametrize('side', ['source', 'target', 'target_copy'])
def test_both_accessors_answer_for_every_side(side):
    """
    They used to disagree: get_db_config() knew the copy and get_db_session_settings() did not,
    so building the connection to it raised before it was opened.
    """
    made = parser(target_copy=dict(COPY))
    assert made.get_db_config(side) is not None
    assert made.get_db_session_settings(side) == {}


@pytest.mark.parametrize('accessor', ['get_db_config', 'get_db_session_settings'])
def test_a_side_which_does_not_exist_is_still_refused(accessor):
    made = parser(target_copy=dict(COPY))
    with pytest.raises(ValueError) as raised:
        getattr(made, accessor)('nonsense')
    assert 'Invalid source_or_target' in str(raised.value)


def test_the_refusal_names_the_sides_there_are():
    """The message used to name only what was wrong, not what would have been right."""
    made = parser(target_copy=dict(COPY))
    with pytest.raises(ValueError) as raised:
        made.get_db_session_settings('targt_copy')
    for side in ConfigParser.DATABASE_SIDES:
        assert side in str(raised.value)


## ------------------------------------------------------- the session settings of the copy


def test_the_copy_carries_session_settings_of_its_own():
    copy = dict(COPY, settings={'search_path': 'public', 'work_mem': '32MB'})
    made = parser(target_copy=copy)
    assert made.get_db_session_settings('target_copy') == {'search_path': 'public', 'work_mem': '32MB'}


def test_the_settings_of_the_target_are_not_reused_for_the_copy():
    """It is a different database - a role or a search_path of the target may not exist there."""
    made = parser(target_copy=dict(COPY))
    made.config['target']['settings'] = {'role': 'migrator'}
    assert made.get_db_session_settings('target') == {'role': 'migrator'}
    assert made.get_db_session_settings('target_copy') == {}


def test_a_copy_without_settings_answers_an_empty_mapping():
    """The connector asks every connection for them; None would be a TypeError there."""
    assert parser(target_copy=dict(COPY)).get_db_session_settings('target_copy') == {}


def test_a_validation_block_without_a_copy_still_answers():
    made = parser(workers=8)
    assert made.get_db_config('target_copy') == {}
    assert made.get_db_session_settings('target_copy') == {}


## ------------------------------------------- the schema allows what the code reads from it


@pytest.mark.parametrize('key', ['owner', 'settings'])
def test_the_schema_allows_the_keys_the_code_reads(schema, key):
    """
    `validation.target_copy` has additionalProperties: false, so a key the code reads and the
    schema does not list is reported to the user as unknown - for `owner`, which the validator
    asks for right after `schema`, exactly as it does for the target.
    """
    assert key in schema['properties']['validation']['properties']['target_copy']['properties']


def test_the_copy_takes_the_same_keys_as_the_target(schema):
    """
    It is a connection to a PostgreSQL database like the target. A key the target has and the
    copy does not is a key somebody will write there and have refused.
    """
    target = set(schema['properties']['target']['properties'])
    copy = set(schema['properties']['validation']['properties']['target_copy']['properties'])
    assert not (target - copy), f"the target has keys the copy refuses: {sorted(target - copy)}"


@pytest.mark.parametrize('written', [
    dict(COPY),
    dict(COPY, settings={'search_path': 'public'}),
    {k: v for k, v in COPY.items() if k != 'schema'} | {'owner': 'public'},
])
def test_a_configured_copy_passes_the_schema_check(schema, written):
    """Against the subschema of the block itself, which is what refused these keys."""
    subschema = schema['properties']['validation']['properties']['target_copy']
    errors = list(Draft202012Validator(subschema).iter_errors(written))
    assert not errors, [f"{'.'.join(str(p) for p in e.path)}: {e.message}" for e in errors]


def test_the_schema_says_what_the_copy_is_for(schema):
    description = schema['properties']['validation']['properties']['target_copy']['description']
    assert 'mapping' in description
    assert 'before the migration' in description


## ----------------------------------------------------- the validator asks for the copy

def test_the_validator_builds_the_third_connection_for_the_mapping_workflow():
    """
    Asserted on the source, because the failure was that building it raised: the call has to
    stay, and it has to stay behind the workflow check - the standard workflow has no copy.
    """
    source = open(os.path.join(REPO, 'credativ_pg_migrator', 'validator.py'), encoding='utf-8').read()
    assert "self._get_connector('target_copy')" in source
    assert "if self.config_parser.get_workflow() == 'mapping':" in source
