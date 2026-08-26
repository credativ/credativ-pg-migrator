# SPDX-License-Identifier: GPL-3.0-or-later
"""
chunk_size - the default, and why no example config sets it.

Chunking replaces the one SELECT which reads a table with several paged ones. It is not a
performance setting: every chunk repeats the sort and re-reads the rows it skips, and the paging
needs an order which is unique or rows can be read twice or missed. Several sources do not
implement it at all. The one case it is for is a source whose driver or server materialises a
whole result set before the first row arrives.

So the default is -1 (no chunking) and the example configurations leave it out, which is what
this file holds - an example which sets it is a recommendation to use it, and it was recommended
in eight of them.

Section 8.2 of docs/user_guide.md is the description; development notes and the per-source table
live there too.

Run with:  python3 -m pytest tests/test_chunk_size_default.py -v
"""

import glob
import json
import os
import re
import sys

import pytest
import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.config_parser import ConfigParser

CONFIG_GLOB = os.path.join(REPO, 'docs', 'configs', '*.yaml')
SCHEMA_PATH = os.path.join(REPO, 'credativ_pg_migrator', 'config.schema.json')


class Args:
    log_level = 'INFO'


def parser(config):
    made = ConfigParser.__new__(ConfigParser)
    made.config = config
    made.args = Args()
    made.messages = []
    made.print_log_message = lambda level, message: made.messages.append((level, str(message)))
    return made


## ------------------------------------------------------------------ the default


def test_a_configuration_without_the_key_does_not_chunk():
    assert parser({'migration': {}}).get_chunk_size() == -1


def test_a_configuration_without_a_migration_block_does_not_chunk():
    assert parser({}).get_chunk_size() == -1


def test_a_table_without_an_override_does_not_chunk():
    made = parser({'migration': {}, 'table_settings': [{'table_name': 'orders'}]})
    assert made.get_table_chunk_size('orders') == -1


def test_the_schema_says_the_same():
    schema = json.load(open(SCHEMA_PATH, encoding='utf-8'))
    assert schema['properties']['migration']['properties']['chunk_size']['default'] == -1


def test_a_chunk_smaller_than_a_batch_is_refused_with_a_warning():
    """
    A chunk smaller than a batch cannot be read one batch at a time, so chunking is switched
    off rather than producing batches which straddle a chunk boundary.
    """
    made = parser({'migration': {'batch_size': 100000, 'chunk_size': 1000}})
    assert made.get_chunk_size() == -1
    assert any('smaller than batch size' in message for _, message in made.messages)


def test_a_chunk_larger_than_a_batch_is_kept():
    made = parser({'migration': {'batch_size': 10000, 'chunk_size': 250000}})
    assert made.get_chunk_size() == 250000


## ------------------------------------------------------- no example recommends it


def example_configs():
    return sorted(glob.glob(CONFIG_GLOB))


@pytest.mark.parametrize('path', example_configs(), ids=lambda p: os.path.basename(p))
def test_no_example_sets_the_row_chunk_size(path):
    """
    'chunk_size' also names the piece size of data_export.big_files_split, which is a size in
    bytes ('2GB') and a different setting entirely - only migration.chunk_size and the per-table
    override of it are meant here.
    """
    with open(path, encoding='utf-8') as handle:
        config = yaml.safe_load(handle) or {}

    assert 'chunk_size' not in (config.get('migration') or {}), (
        f"{os.path.basename(path)} sets migration.chunk_size - the default -1 is right for "
        f"almost every migration, see section 8.2 of docs/user_guide.md")

    for entry in (config.get('table_settings') or []):
        assert 'chunk_size' not in (entry or {}), (
            f"{os.path.basename(path)} sets chunk_size for table {entry.get('table_name')!r}")


@pytest.mark.parametrize('path', example_configs(), ids=lambda p: os.path.basename(p))
def test_no_example_recommends_it_in_a_comment_either(path):
    """
    A commented-out setting is a recommendation too - it is what a reader uncomments first. The
    byte size of big_files_split keeps its examples, so only a row count is looked for.
    """
    for number, line in enumerate(open(path, encoding='utf-8'), start=1):
        match = re.search(r'#\s*chunk_size\s*:\s*(\S+)', line)
        if match and not match.group(1).strip('\'"').upper().endswith(('B', "B'", 'B"')):
            pytest.fail(f"{os.path.basename(path)}:{number} suggests a row chunk_size: {line.strip()}")


## ------------------------------------------------------------- the documentation


def chunking_section():
    guide = open(os.path.join(REPO, 'docs', 'user_guide.md'), encoding='utf-8').read()
    assert '### 8.2 Chunked reading of large tables' in guide
    return guide.split('### 8.2')[1].split('### 8.3')[0]


@pytest.mark.parametrize('source', [
    'Oracle', 'PostgreSQL', 'MS SQL Server', 'MySQL', 'MariaDB', 'SQLite', 'Informix',
    'SQL Anywhere', 'Sybase ASE', 'IBM DB2 LUW', 'IBM DB2 for i', 'IBM DB2 for z/OS',
])
def test_the_guide_says_what_every_source_does_with_chunk_size(source):
    """All twelve, so that a reader can look their own up rather than infer it."""
    assert source in chunking_section(), (
        f"the guide does not say what {source} does with chunk_size")


def test_the_guide_separates_where_it_works_from_where_it_does_nothing():
    section = chunking_section()
    assert 'Where it can be used, and where it must not be' in section
    assert 'The setting does nothing' in section
    assert 'It depends on the server' in section


def test_the_guide_still_warns_about_the_two_costs():
    section = chunking_section()
    assert 'slower' in section.lower()
    assert 'read twice or missed' in section


def test_the_schema_description_warns_rather_than_recommends():
    schema = json.load(open(SCHEMA_PATH, encoding='utf-8'))
    description = schema['properties']['migration']['properties']['chunk_size']['description']
    assert 'NOT a performance setting' in description
    assert 'SLOWER' in description
    assert 'Good values are' not in description, (
        'the description used to recommend 10x-20x batch_size, which reads as an invitation')
