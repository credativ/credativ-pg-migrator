# SPDX-License-Identifier: GPL-3.0-or-later
"""
Checks over the configuration language and the files that document it.

Nothing here needs a database. The point is to catch, in CI, the class of defect that
used to be found by hand: a sample config that no longer matches the code, a schema key
the code never reads, a code key no example shows, and the YAML traps - duplicate keys
and keys left empty - that made the old all-options reference unusable.

Run with:  python3 -m pytest tests/test_config_docs.py -v
"""

import glob
import json
import os
import re
import subprocess
import sys

import pytest
import yaml
from jsonschema import Draft202012Validator


from credativ_pg_migrator.config_parser import ConfigParser

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMA_PATH = os.path.join(REPO, 'credativ_pg_migrator', 'config.schema.json')
CONFIG_PARSER = os.path.join(REPO, 'credativ_pg_migrator', 'config_parser.py')
REFERENCE_MD = os.path.join(REPO, 'docs', 'config_reference.md')
GENERATOR = os.path.join(REPO, 'tools', 'generate_config_docs.py')
CONFIG_GLOB = os.path.join(REPO, 'docs', 'configs', '*.yaml')

# Keys whose examples are commented-out block sequences directly under the key. Leaving
# such a key bare is how the examples stay uncommentable, and the code reads all of them
# through `or []` / `or {}`, so null is a legitimate value. Every other key must carry a
# real value - a bare key is the trap that made include/exclude parse as null.
NULL_ALLOWED = {
    'table_settings',
    'tables',
    'data_types_substitution',
    'default_values_substitution',
    'remote_objects_substitution',
    'data_migration_limitation',
    'target_partitioning',
    'forced_table_mappings',
    'forced_column_mappings',
}

# Keys read from the config by something other than config_parser, or read dynamically,
# so scanning config_parser.py alone cannot see them.
READ_ELSEWHERE = {
    'env_variables',          # main.py
    'regex_mappings',         # anonymization/routing.py
    'on_value_too_long',      # anonymization/routing.py
    'find_fitting_value_attempts',
}


def config_files():
    return sorted(glob.glob(CONFIG_GLOB))


def load_schema():
    with open(SCHEMA_PATH, encoding='utf-8') as handle:
        return json.load(handle)


def schema_keys(node, out, skip_unimplemented=False):
    """
    Every property name the schema declares, at any depth. With skip_unimplemented, a
    subtree marked "x-implemented": false is left out - those options are documented
    precisely as having no effect yet, so no code reads them.
    """
    if isinstance(node, dict):
        if skip_unimplemented and node.get('x-implemented') is False:
            return out
        for name, child in (node.get('properties') or {}).items():
            if skip_unimplemented and isinstance(child, dict) and child.get('x-implemented') is False:
                continue
            out.add(name)
            schema_keys(child, out, skip_unimplemented)
        for branch in ('items', 'additionalProperties'):
            schema_keys(node.get(branch), out, skip_unimplemented)
        for branch in ('oneOf', 'anyOf', 'allOf'):
            for option in node.get(branch) or []:
                schema_keys(option, out, skip_unimplemented)
        for definition in (node.get('$defs') or {}).values():
            schema_keys(definition, out, skip_unimplemented)
    return out


def code_keys():
    """
    Every name the package looks up in a dictionary. Deliberately over-inclusive: it also
    catches lookups over the migrator's own structures, so it can only be used to prove
    that a schema key IS read somewhere, never that a name is a config key.
    """
    found = set()
    for path in glob.glob(os.path.join(REPO, 'credativ_pg_migrator', '**', '*.py'),
                          recursive=True):
        with open(path, encoding='utf-8') as handle:
            source = handle.read()
        found |= set(re.findall(r"\.get\(\s*['\"]([a-z_0-9]+)['\"]", source))
        found |= set(re.findall(r"\[\s*['\"]([a-z_0-9]+)['\"]\s*\]", source))
    # The object filters are read through a variable, so their names appear only in the
    # table which drives that lookup. Taken from the class itself, so the two cannot part.
    found |= {name for pair in ConfigParser.OBJECT_FILTER_KEYS.values() for name in pair}
    return found


def config_parser_keys():
    """The narrower set: names config_parser.py itself reads out of self.config."""
    with open(CONFIG_PARSER, encoding='utf-8') as handle:
        source = handle.read()
    found = set(re.findall(r"self\.config(?:\[[^\]]+\])*\.get\(\s*['\"]([a-z_0-9]+)['\"]", source))
    found |= set(re.findall(r"self\.config(?:\[[^\]]+\])*\[\s*['\"]([a-z_0-9]+)['\"]\s*\]", source))
    return found


# --------------------------------------------------------------------------------------
# the schema itself


def test_schema_is_a_valid_json_schema():
    Draft202012Validator.check_schema(load_schema())


def test_every_schema_property_has_a_description():
    """A key nobody described is a key nobody can look up."""
    missing = []

    def walk(node, path):
        if not isinstance(node, dict):
            return
        for name, child in (node.get('properties') or {}).items():
            here = f'{path}.{name}' if path else name
            if isinstance(child, dict) and '$ref' not in child and not child.get('description'):
                missing.append(here)
            walk(child, here)
        for branch in ('items', 'additionalProperties'):
            walk(node.get(branch), f'{path}[]')
        for definition_name, definition in (node.get('$defs') or {}).items():
            walk(definition, definition_name)

    walk(load_schema(), '')
    assert not missing, 'schema properties without a description: ' + ', '.join(missing)


# --------------------------------------------------------------------------------------
# the sample configs


@pytest.mark.parametrize('path', config_files(), ids=os.path.basename)
def test_config_has_no_duplicate_keys(path):
    """
    ruamel refuses a document with duplicate keys outright, and PyYAML silently keeps the
    last of each - which is how three export-format examples merged into one nonsensical
    block. Loading with ruamel is the check.
    """
    ruamel = pytest.importorskip('ruamel.yaml', reason='ruamel.yaml not installed')
    ruamel.YAML(typ='safe').load(open(path, encoding='utf-8'))


@pytest.mark.parametrize('path', config_files(), ids=os.path.basename)
def test_config_matches_the_schema(path):
    with open(path, encoding='utf-8') as handle:
        document = yaml.safe_load(handle)
    errors = sorted(Draft202012Validator(load_schema()).iter_errors(document),
                    key=lambda e: list(e.path))
    assert not errors, '\n'.join(
        f"  {'.'.join(str(p) for p in e.path) or '(root)'}: {e.message}" for e in errors)


@pytest.mark.parametrize('path', config_files(), ids=os.path.basename)
def test_config_has_no_unintended_null_keys(path):
    with open(path, encoding='utf-8') as handle:
        document = yaml.safe_load(handle) or {}
    nulls = [key for key, value in document.items()
             if value is None and key not in NULL_ALLOWED]
    assert not nulls, (
        f'keys left empty, which parse as null rather than as an empty list: {nulls}. '
        f'Write [] instead, or add the key to NULL_ALLOWED if a bare key is intended.')


@pytest.mark.parametrize('path', config_files(), ids=os.path.basename)
def test_config_uses_only_real_placeholders(path):
    """
    planner.py substitutes {{source_schema_name}}, {{source_table_name}} and
    {{source_alias_name}} and nothing else. Any other {{...}} stays in the path as literal
    text, so the data file is never found.
    """
    with open(path, encoding='utf-8') as handle:
        text = handle.read()
    used = set(re.findall(r'\{\{([a-z_]+)\}\}', text))
    unknown = used - {'source_schema_name', 'source_table_name', 'source_alias_name'}
    assert not unknown, f'placeholders that are never substituted: {sorted(unknown)}'


@pytest.mark.parametrize('path', config_files(), ids=os.path.basename)
def test_fixed_arity_lists_have_the_right_length(path):
    """
    These blocks are unpacked into a fixed number of names, so a row of the wrong length
    is a ValueError at startup rather than a warning.
    """
    arity = {
        'data_types_substitution': {5},
        'default_values_substitution': {4},
        'remote_objects_substitution': {2},
        ## three elements, or four with the optional row limit
        'data_migration_limitation': {3, 4},
    }
    with open(path, encoding='utf-8') as handle:
        document = yaml.safe_load(handle) or {}
    problems = []
    for key, expected in arity.items():
        for position, row in enumerate(document.get(key) or [], start=1):
            if isinstance(row, list) and len(row) not in expected:
                spelled = ' or '.join(str(length) for length in sorted(expected))
                problems.append(f'{key}[{position}] has {len(row)} elements, expected {spelled}')
    assert not problems, '; '.join(problems)


# --------------------------------------------------------------------------------------
# schema against code, in both directions


def test_schema_declares_no_key_the_code_never_reads():
    """
    Every option in the schema is either read somewhere in the package, or marked
    "x-implemented": false. An option that is neither is a documented setting that
    silently does nothing - which is the failure this repository treats as a bug.
    """
    declared = schema_keys(load_schema(), set(), skip_unimplemented=True)
    known = code_keys() | READ_ELSEWHERE
    # Free-form maps: the names under them are user data, not option names.
    free_form = {'work_mem', 'maintenance_work_mem', 'role', 'search_path'}
    unread = sorted(declared - known - free_form)
    assert not unread, (
        'schema declares keys that config_parser never reads: ' + ', '.join(unread) +
        ' - either the code lost them, or the schema documents something that does nothing.')


def test_code_reads_no_key_the_schema_omits():
    """
    A key config_parser reads out of self.config but the schema does not declare cannot be
    looked up in the reference and is not validated. This is the check that would have
    caught source.server and anonymization.regex_mappings being missing.
    """
    declared = schema_keys(load_schema(), set())
    # Read from a sub-mapping of the config whose keys are user data, not option names,
    # or read out of a block the schema types as free-form.
    not_option_names = {
        'source_schema_name', 'source_table_name', 'mapping_rules',
    }
    missing = sorted(config_parser_keys() - declared - not_option_names)
    assert not missing, (
        'config_parser reads keys the schema does not declare: ' + ', '.join(missing))


# --------------------------------------------------------------------------------------
# generated documentation


def test_reference_markdown_is_up_to_date():
    """The checked-in reference must be what the generator produces from the schema."""
    result = subprocess.run([sys.executable, GENERATOR, '--check'],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr


def test_reference_markdown_has_no_broken_internal_links():
    with open(REFERENCE_MD, encoding='utf-8') as handle:
        text = handle.read()
    headings = set()
    for line in text.splitlines():
        match = re.match(r'#{2,6}\s+(.*)', line)
        if match:
            slug = re.sub(r'[^\w\s-]', '', match.group(1).strip().lower())
            headings.add(re.sub(r'\s+', '-', slug))
    links = set(re.findall(r'\]\(#([^)]+)\)', text))
    assert not links - headings, f'broken anchors: {sorted(links - headings)}'
