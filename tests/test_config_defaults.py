# SPDX-License-Identifier: GPL-3.0-or-later
"""
The default documented in the schema must be the default the code applies.

This is the last place where docs/config_reference.md could disagree with the migrator.
Names, types, allowed values and requiredness are already checked in both directions by
tests/test_config_docs.py and tests/test_config_schema_validation.py; the defaults are
checked here, by leaving the key out of a configuration and asking the accessor that
reads it what it produced.

The comparison is of the EFFECTIVE default, not of the literal in the source: several
accessors pass None into .get() as a sentinel and resolve the real default afterwards
(pattern_syntax, varchar_to_text_length, char_to_text_length), so reading the literal
out of the code would report differences which do not exist.

Run with:  python3 -m pytest tests/test_config_defaults.py -v
"""

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

SCHEMA_PATH = os.path.join(REPO, 'credativ_pg_migrator', 'config.schema.json')

# A configuration with nothing in it but what the schema requires, so that every optional
# key really is absent and every accessor really does fall back to its default.
MINIMAL_CONFIG = {
    'migrator': {'type': 'postgresql', 'host': 'h', 'port': 5432, 'username': 'u',
                 'password': 'p', 'database': 'd', 'schema': 'migration'},
    'source':   {'type': 'oracle', 'host': 'h', 'port': 1521, 'username': 'u',
                 'password': 'p', 'database': 'd', 'schema': 's'},
    'target':   {'type': 'postgresql', 'host': 'h', 'port': 5432, 'username': 'u',
                 'password': 'p', 'database': 'd', 'schema': 'public'},
}

# schema path -> the accessor which reads it. A pair (accessor, fragment) is used where
# the default only applies inside a block which has to be present for it to be reached.
DEFAULT_READERS = {
    'workflow': 'get_workflow',
    'pattern_syntax': 'get_pattern_syntax',

    'pre_migration_analysis.top_n_tables.by_rows': 'get_top_n_tables_by_rows',
    'pre_migration_analysis.top_n_tables.by_size': 'get_top_n_tables_by_size',
    'pre_migration_analysis.top_n_tables.by_columns': 'get_top_n_tables_by_columns',
    'pre_migration_analysis.top_n_tables.by_indexes': 'get_top_n_tables_by_indexes',
    'pre_migration_analysis.top_n_tables.by_constraints': 'get_top_n_tables_by_constraints',

    'source.system_catalog': 'get_system_catalog',
    'source.db_locale': 'get_source_db_locale',
    'source.client_locale': 'get_source_client_locale',
    'source.oracle_thick_mode': 'get_oracle_thick_mode',

    'migration.drop_schema': 'should_drop_schema',
    'migration.drop_tables': 'should_drop_tables',
    'migration.truncate_tables': 'should_truncate_tables',
    'migration.create_tables': 'should_create_tables',
    'migration.migrate_data': 'should_migrate_data',
    'migration.migrate_indexes': 'should_migrate_indexes',
    'migration.migrate_constraints': 'should_migrate_constraints',
    'migration.migrate_triggers': 'should_migrate_triggers',
    'migration.migrate_funcprocs': 'should_migrate_funcprocs',
    'migration.migrate_views': 'should_migrate_views',
    'migration.set_sequences': 'should_set_sequences',
    'migration.migrate_lob_values': 'should_migrate_lob_values',
    'migration.validate_objects': 'get_validate_objects_mode',
    'migration.on_error': 'get_on_error_action',
    'migration.on_undecodable_bytes': 'get_on_undecodable_bytes_action',
    'migration.parallel_workers': 'get_parallel_workers_count',
    'migration.batch_size': 'get_batch_size',
    'migration.chunk_size': 'get_chunk_size',
    'migration.names_case_handling': 'get_names_case_handling',
    'migration.use_aliases_as_target_names': 'get_use_aliases_as_target_names',
    'migration.varchar_to_text_length': 'get_varchar_to_text_length',
    'migration.char_to_text_length': 'get_char_to_text_length',
    'migration.zero_datetime_default': 'get_zero_datetime_default',
    'migration.zero_datetime_value': 'get_zero_datetime_data_value',
    'migration.relax_not_null_datetime': 'get_relax_not_null_datetime',
    'migration.uuid_default_function': 'get_uuid_default_function',
    'migration.required_extensions': 'get_required_extensions',
    'migration.packages_as': 'get_packages_migration_style',
    'migration.map_numeric_1_to_boolean': 'should_map_numeric_1_to_boolean',
    'migration.pre_migration_script': 'get_pre_migration_script',
    'migration.post_migration_script': 'get_post_migration_script',

    'tables': 'get_tables_config',

    'validation.workers': 'get_validation_workers',
    'validation.batch_size': 'get_validation_batch_size',
    'validation.check_row_counts': 'is_validation_row_counts_enabled',
    'validation.check_table_checksums': 'is_validation_table_checksums_enabled',
    'validation.check_random_sample': 'is_validation_random_sample_enabled',
    'validation.check_lob_sizes': 'is_validation_lob_sizes_enabled',
    'validation.random_sample_size': 'get_validation_sample_size',

    'query_conversion.enabled': 'is_query_conversion_enabled',
    'query_conversion.run_after_migration': 'should_run_query_conversion_after_migration',
    'query_conversion.encoding': 'get_query_conversion_encoding',
    'query_conversion.statement_separator': 'get_query_conversion_statement_separator',
    'query_conversion.parameter_style': 'get_query_conversion_parameter_style',
    'query_conversion.parameter_output': 'get_query_conversion_parameter_output',
    'query_conversion.source_test': 'get_query_conversion_source_test',
    'query_conversion.target_test': 'get_query_conversion_target_test',
    'query_conversion.timeout': 'get_query_conversion_timeout',
    'query_conversion.workers': 'get_query_conversion_workers',
    'query_conversion.on_error': 'get_query_conversion_on_error',
    'query_conversion.output.directory': 'get_query_conversion_output_directory',
    'query_conversion.output.prefix': 'get_query_conversion_output_prefix',
    'query_conversion.output.suffix': 'get_query_conversion_output_suffix',
    'query_conversion.output.overwrite': 'get_query_conversion_output_overwrite',
    'query_conversion.output.include_original': 'get_query_conversion_output_include_original',
    'query_conversion.output.sidecar': 'get_query_conversion_output_sidecar',

    'mapping.forced_table_mappings': 'get_forced_table_mappings',
    'mapping.forced_column_mappings': 'get_forced_column_mappings',

    'summary.top_migrated_tables': 'get_summary_top_migrated_tables',
    'summary.top_mismatched_tables': 'get_summary_top_mismatched_tables',
    'summary.top_longest_batches': 'get_summary_top_longest_batches',
    'summary.top_anonymized_tables': 'get_summary_top_anonymized_tables',
    'summary.top_anonymized_columns': 'get_summary_top_anonymized_columns',
    'summary.show_anonymization_examples': 'get_summary_show_anonymization_examples',

    # source.data_export - the block has to exist for its own options to be reached
    '$defs.dataExport.on_missing_data_file': (
        'get_source_data_export_on_missing_data_file', {'source': {'data_export': {}}}),
    '$defs.dataExport.delimiter': (
        'get_source_data_export_delimiter', {'source': {'data_export': {}}}),
    '$defs.dataExport.header': (
        'get_source_data_export_header', {'source': {'data_export': {}}}),
    '$defs.dataExport.clean': (
        'get_source_data_export_clean', {'source': {'data_export': {}}}),
    '$defs.dataExport.workers': (
        'get_source_data_export_workers', {'source': {'data_export': {}}}),
    '$defs.dataExport.big_files_split.enabled': (
        'get_source_data_export_big_files_split_enabled',
        {'source': {'data_export': {'big_files_split': {}}}}),
    # workers is only reached when the split is actually configured - an empty block is
    # read as "not configured" and answers -1, which is the block default, not this one.
    '$defs.dataExport.big_files_split.workers': (
        'get_source_data_export_big_files_split_workers',
        {'source': {'data_export': {'big_files_split': {'enabled': True}}}}),
}

# Documented defaults which no accessor of ConfigParser applies, with the reason. Listing
# them is deliberate: a default added to the schema has to be either checked above or
# consciously written down here, so it cannot slip in unnoticed.
UNMAPPED_DEFAULTS = {
    'migrator.type': 'the key is required, so the default is never applied',
    'migrator.port': 'the key is required, so the default is never applied',
    'migrator.schema': 'the key is required, so the default is never applied',
    'target.type': 'the key is required, so the default is never applied',
    'target.port': 'the key is required, so the default is never applied',
    'target.schema': 'the key is required, so the default is never applied',
    'source.schema': 'read together with source.owner inside get_source_schema, which the '
                     'minimal configuration has to set for the connectors',
    'migrator.sslmode': 'read inline while the connection string is built, not through an accessor',
    'source.sslmode': 'read inline while the connection string is built, not through an accessor',
    'target.sslmode': 'read inline while the connection string is built, not through an accessor',
    'validation.target_copy.type': 'read inline from the target_copy block',
    'validation.target_copy.port': 'read inline from the target_copy block',
    'validation.target_copy.sslmode': 'read inline from the target_copy block',
    'include_tables': 'the equivalence of "all", [] and an absent key is checked directly in '
                      'tests/test_object_filters.py',
    'exclude_tables': 'see include_tables',
    'include_views': 'see include_tables',
    'exclude_views': 'see include_tables',
    'include_funcprocs': 'see include_tables',
    'exclude_funcprocs': 'see include_tables',
    'migration.numeric_1_boolean_columns': 'the list is not returned; it is matched against '
                                           'inside should_map_numeric_1_to_boolean',
    'mapping.heuristics.table_normalization_rules': 'applied by connectors/match_schemas.py, '
                                                    'not by an accessor',
    'mapping.heuristics.column_normalization_rules': 'applied by connectors/match_schemas.py, '
                                                     'not by an accessor',
    'mapping.heuristics.column_prefixes_to_strip': 'applied by planner.py, not by an accessor',
    'anonymization.on_value_too_long': 'applied by anonymization/routing.py',
    'anonymization.find_fitting_value_attempts': 'applied by anonymization/routing.py',
    'anonymization.regex_mappings': 'applied by anonymization/routing.py',
    'anonymization.regex_mappings[].table_pattern': 'per entry, applied by anonymization/routing.py',
    'anonymization.regex_mappings[].column_pattern': 'per entry, applied by anonymization/routing.py',
    '$defs.dataExport.character_set': 'applied by planner.py where the data source is read',
    '$defs.tableDataExport.header': 'per table, applied where the table data source is read',
    '$defs.targetLobStorage.storage': 'the block is not implemented',
}


def load_schema():
    with open(SCHEMA_PATH, encoding='utf-8') as handle:
        return json.load(handle)


def documented_defaults():
    """Every schema property carrying a default, as path -> value."""
    found = {}

    def walk(node, path):
        if not isinstance(node, dict):
            return
        for name, child in (node.get('properties') or {}).items():
            here = f'{path}.{name}' if path else name
            if isinstance(child, dict) and 'default' in child:
                found[here] = child['default']
            walk(child, here)
        walk(node.get('items'), f'{path}[]')
        if isinstance(node.get('additionalProperties'), dict):
            walk(node['additionalProperties'], f'{path}.<name>')
        for name, definition in (node.get('$defs') or {}).items():
            walk(definition, f'$defs.{name}')

    walk(load_schema(), '')
    return found


def merge(base, fragment):
    merged = json.loads(json.dumps(base))
    for key, value in (fragment or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def parser_for(tmp_path, fragment=None):
    config = merge(MINIMAL_CONFIG, fragment)
    path = tmp_path / 'config.yaml'
    path.write_text(yaml.safe_dump(config), encoding='utf-8')
    logging.disable(logging.CRITICAL)
    try:
        return ConfigParser(
            types.SimpleNamespace(config=str(path), log_file=None, log_level='INFO',
                                  ignore_config_schema_errors=False),
            logging.getLogger('test_config_defaults'))
    finally:
        logging.disable(logging.NOTSET)


# --------------------------------------------------------------------------------------
# every documented default is accounted for


def test_every_documented_default_is_either_checked_or_explained():
    """
    A default added to the schema must be either verified against the code below or
    written into UNMAPPED_DEFAULTS with a reason. Neither is allowed to be forgotten.
    """
    documented = set(documented_defaults())
    accounted = set(DEFAULT_READERS) | set(UNMAPPED_DEFAULTS)
    missing = sorted(documented - accounted)
    assert not missing, (
        'these documented defaults are neither checked nor explained - add them to '
        'DEFAULT_READERS or to UNMAPPED_DEFAULTS: ' + ', '.join(missing))


def test_no_stale_entries_in_the_tables():
    """A path that no longer carries a default must not linger in the tables."""
    documented = set(documented_defaults())
    stale = sorted((set(DEFAULT_READERS) | set(UNMAPPED_DEFAULTS)) - documented)
    assert not stale, 'no default is documented for these any more: ' + ', '.join(stale)


def test_the_unmapped_list_stays_the_minority():
    """If most defaults cannot be checked, this test has stopped being worth anything."""
    assert len(DEFAULT_READERS) > len(UNMAPPED_DEFAULTS)


# --------------------------------------------------------------------------------------
# the documented default is the default the code applies


@pytest.mark.parametrize('path', sorted(DEFAULT_READERS))
def test_the_documented_default_is_what_the_code_applies(tmp_path, path):
    documented = documented_defaults()
    assert path in documented, f'{path} carries no default in the schema'
    expected = documented[path]

    entry = DEFAULT_READERS[path]
    accessor, fragment = entry if isinstance(entry, tuple) else (entry, None)

    parser = parser_for(tmp_path, fragment)
    assert hasattr(parser, accessor), f'{path}: ConfigParser has no {accessor}()'
    produced = getattr(parser, accessor)()

    # A value the code normalises to lower case is documented in that form.
    if isinstance(expected, str) and isinstance(produced, str):
        assert produced.lower() == expected.lower(), (
            f'{path}: the schema documents {expected!r}, {accessor}() produced {produced!r}')
    else:
        assert produced == expected, (
            f'{path}: the schema documents {expected!r}, {accessor}() produced {produced!r}')


def test_the_accessors_are_read_from_a_configuration_with_the_keys_absent(tmp_path):
    """The premise of the whole file: none of the checked keys is in MINIMAL_CONFIG."""
    parser = parser_for(tmp_path)
    for path in DEFAULT_READERS:
        top = path.split('.')[0]
        if top.startswith('$defs'):
            continue
        holder = parser.config
        steps = path.split('.')
        for step in steps[:-1]:
            holder = (holder or {}).get(step) if isinstance(holder, dict) else None
        if isinstance(holder, dict):
            assert steps[-1] not in holder, f'{path} is set in MINIMAL_CONFIG - its default is not being tested'
