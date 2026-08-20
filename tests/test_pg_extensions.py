import fnmatch
import re
import unittest
from unittest.mock import MagicMock, patch
from credativ_pg_migrator.planner import Planner


def build_planner(source_extensions=None, target_extensions=None, target_available=None,
                  dependencies=None, configured=None, source_tables=None,
                  include_tables=None, exclude_tables=None):
    mock_config = MagicMock()
    mock_config.get_log_file.return_value = 'migrator.log'
    mock_config.get_source_schema.return_value = 'public'
    mock_config.get_target_schema.return_value = 'migtest'
    mock_config.get_target_db_type.return_value = 'postgresql'
    mock_config.get_on_error_action.return_value = 'stop'
    mock_config.get_pre_migration_script.return_value = None
    mock_config.get_post_migration_script.return_value = None
    mock_config.get_required_extensions.return_value = list(configured or [])
    mock_config.get_include_tables.return_value = include_tables or ['.*']
    mock_config.get_exclude_tables.return_value = exclude_tables or []

    ## the planner asks is_object_selected(), which answers (selected, reason) - a bare
    ## MagicMock answers with something which cannot be unpacked into the two. The patterns
    ## are read the way the configuration reads them by default: as globs.
    def matches(pattern, object_name):
        return re.fullmatch(fnmatch.translate(str(pattern).strip()), str(object_name), re.IGNORECASE)

    def is_object_selected(object_kind, object_name):
        for pattern in (exclude_tables or []):
            if matches(pattern, object_name):
                return False, f'excluded by exclude_tables: {pattern}'
        for pattern in (include_tables or ['*']):
            if matches(pattern, object_name):
                return True, None
        return False, 'not matched by include_tables'
    mock_config.is_object_selected.side_effect = is_object_selected
    mock_config.should_migrate_indexes.return_value = True
    mock_config.should_migrate_constraints.return_value = True
    mock_config.should_migrate_triggers.return_value = True
    mock_config.should_migrate_views.return_value = True
    mock_config.should_migrate_funcprocs.return_value = True
    messages = []
    mock_config.print_log_message.side_effect = lambda level, message: messages.append((level, message))

    with patch('credativ_pg_migrator.planner.MigratorLogger'), \
         patch('credativ_pg_migrator.planner.MigratorTables'), \
         patch.object(Planner, 'load_connector', return_value=MagicMock()):
        planner = Planner(mock_config)

    planner.source_connection.fetch_installed_extensions.return_value = source_extensions or {}
    planner.target_connection.fetch_installed_extensions.return_value = target_extensions or {}
    planner.target_connection.fetch_available_extensions.return_value = target_available or {}
    planner.source_connection.fetch_extension_dependencies.return_value = dependencies or {}
    planner.source_connection.fetch_table_names.return_value = {
        index: {'table_name': name} for index, name in enumerate(source_tables or [], start=1)}
    planner.messages = messages
    return planner


SRC = {'pg_trgm': {'version': '1.6', 'schema': 'ext'},
       'pgcrypto': {'version': '1.4', 'schema': 'ext'}}
DEPS = {'pg_trgm': ['index customers_company_trgm_idx'],
        'pgcrypto': ['column documents.checksum (generated)']}


class TestExtensionChecks(unittest.TestCase):

    def test_dependency_already_installed_in_target_is_not_blocking(self):
        planner = build_planner(source_extensions=SRC, dependencies=DEPS,
                                target_extensions={'pg_trgm': {'version': '1.6', 'schema': 'public'},
                                                   'pgcrypto': {'version': '1.4', 'schema': 'public'}})
        self.assertEqual(planner.check_extensions(), [])

    def test_dependency_listed_in_configuration_is_not_blocking(self):
        planner = build_planner(source_extensions=SRC, dependencies=DEPS,
                                target_available={'pg_trgm': '1.6', 'pgcrypto': '1.4'},
                                configured=['pg_trgm', 'pgcrypto'])
        self.assertEqual(planner.check_extensions(), [])

    def test_missing_dependency_is_blocking(self):
        planner = build_planner(source_extensions=SRC, dependencies=DEPS,
                                target_available={'pg_trgm': '1.6', 'pgcrypto': '1.4'},
                                configured=['pg_trgm'])
        issues = planner.check_extensions()
        self.assertEqual(len(issues), 1)
        self.assertIn("'pgcrypto'", issues[0])
        self.assertIn('column documents.checksum (generated)', issues[0])
        self.assertIn('would be installed', issues[0])
        self.assertNotIn("'pg_trgm'", issues[0])

    def test_missing_and_unavailable_dependency_says_so(self):
        planner = build_planner(source_extensions=SRC, dependencies=DEPS, target_available={})
        issues = planner.check_extensions()
        self.assertEqual(len(issues), 2)
        self.assertTrue(all('NOT even available' in issue for issue in issues))

    def test_configuration_snippet_is_logged_for_missing_extensions(self):
        planner = build_planner(source_extensions=SRC, dependencies=DEPS,
                                target_available={'pg_trgm': '1.6', 'pgcrypto': '1.4'},
                                configured=['pg_trgm'])
        planner.check_extensions()
        logged = "\n".join(message for level, message in planner.messages if level == 'WARNING')
        self.assertIn('required_extensions:', logged)
        self.assertIn('- pgcrypto', logged)
        # extensions already configured stay in the suggested list
        self.assertIn('- pg_trgm', logged)

    def test_no_dependencies_means_no_issues(self):
        planner = build_planner(source_extensions=SRC, dependencies={})
        self.assertEqual(planner.check_extensions(), [])

    def test_source_without_extensions_is_reported_and_passes(self):
        planner = build_planner()
        self.assertEqual(planner.check_extensions(), [])
        logged = "\n".join(message for level, message in planner.messages)
        self.assertIn('reports no extensions', logged)

    def test_extension_case_in_configuration_is_ignored(self):
        planner = build_planner(source_extensions=SRC, dependencies=DEPS,
                                target_available={'pg_trgm': '1.6', 'pgcrypto': '1.4'},
                                configured=['PG_TRGM', 'PgCrypto'])
        self.assertEqual(planner.check_extensions(), [])

    def test_tables_selected_for_migration_honours_include_and_exclude(self):
        planner = build_planner(source_tables=['customers', 'orders_2023', 'orders_2024', 'audit_log'],
                                include_tables=['orders_*', 'customers'],
                                exclude_tables=['orders_2023'])
        self.assertEqual(planner.get_tables_selected_for_migration(), ['customers', 'orders_2024'])

    def test_tables_selected_for_migration_defaults_to_all(self):
        planner = build_planner(source_tables=['a', 'b'])
        self.assertEqual(planner.get_tables_selected_for_migration(), ['a', 'b'])

    def test_dependency_query_is_limited_to_selected_tables(self):
        planner = build_planner(source_extensions=SRC, dependencies=DEPS,
                                target_extensions={'pg_trgm': {'version': '1.6', 'schema': 'public'},
                                                   'pgcrypto': {'version': '1.4', 'schema': 'public'}},
                                source_tables=['customers', 'audit_log'],
                                exclude_tables=['audit_log'])
        planner.check_extensions()
        settings = planner.source_connection.fetch_extension_dependencies.call_args[0][0]
        self.assertEqual(settings['table_names'], ['customers'])
        self.assertEqual(settings['source_schema_name'], 'public')


if __name__ == '__main__':
    unittest.main()
