import unittest
from unittest.mock import MagicMock, patch
from credativ_pg_migrator.orchestrator import Orchestrator
from credativ_pg_migrator.planner import Planner
from credativ_pg_migrator.connectors.postgresql_connector import PostgreSQLConnector
import datetime
import json

class TestPgUdtOrdering(unittest.TestCase):

    def test_orchestrator_execution_order(self):
        mock_config = MagicMock()
        mock_config.get_log_file.return_value = 'migrator.log'
        mock_config.is_standard_workflow.return_value = True
        mock_config.is_resume_after_crash.return_value = False
        mock_config.is_dry_run.return_value = True
        mock_config.get_source_schema.return_value = 'public'
        mock_config.get_target_schema.return_value = 'public'
        mock_config.get_on_error_action.return_value = 'stop'

        with patch('credativ_pg_migrator.orchestrator.MigratorLogger'), \
             patch('credativ_pg_migrator.orchestrator.MigratorTables'), \
             patch.object(Orchestrator, 'load_connector', return_value=MagicMock()):
            orchestrator = Orchestrator(mock_config)

            call_order = []
            orchestrator.run_create_domains = MagicMock(side_effect=lambda: call_order.append('domains'))
            orchestrator.run_create_user_defined_types = MagicMock(side_effect=lambda: call_order.append('udts'))
            orchestrator.check_pausing_resuming = MagicMock()

            orchestrator.run()

            self.assertEqual(call_order, ['domains', 'udts'])

    def test_planner_preparation_order(self):
        mock_config = MagicMock()
        mock_config.get_log_file.return_value = 'migrator.log'
        mock_config.is_standard_workflow.return_value = True
        mock_config.is_resume_after_crash.return_value = False
        mock_config.get_connectivity_type.return_value = 'live'
        mock_config.get_source_schema.return_value = 'public'
        mock_config.get_target_schema.return_value = 'public'
        mock_config.get_on_error_action.return_value = 'stop'
        mock_config.get_pre_migration_script.return_value = None
        mock_config.get_post_migration_script.return_value = None

        with patch('credativ_pg_migrator.planner.MigratorLogger'), \
             patch('credativ_pg_migrator.planner.MigratorTables'), \
             patch.object(Planner, 'load_connector', return_value=MagicMock()):
            planner = Planner(mock_config)
            planner.source_db_config = {'connectivity': 'live'}
            planner.check_database_connection = MagicMock()
            planner.pre_planning = MagicMock()
            planner.run_premigration_analysis = MagicMock()

            call_order = []
            planner.stdwf_prepare_domains = MagicMock(side_effect=lambda: call_order.append('domains'))
            planner.stdwf_prepare_user_defined_types = MagicMock(side_effect=lambda: call_order.append('udts'))
            planner.stdwf_prepare_defaults = MagicMock()
            planner.stdwf_prepare_aliases = MagicMock()
            planner.stdwf_prepare_sequences = MagicMock()
            planner.stdwf_prepare_tables = MagicMock()
            planner.stdwf_prepare_data_sources = MagicMock()
            planner.stdwf_prepare_views = MagicMock()
            planner.check_pausing_resuming = MagicMock()

            planner.create_plan()

            self.assertEqual(call_order[0], 'domains')
            self.assertEqual(call_order[1], 'udts')

    def test_composite_topological_sort(self):
        mock_config = MagicMock()
        mock_config.get_log_file.return_value = 'migrator.log'
        with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
             patch.object(PostgreSQLConnector, 'prepare_session_settings', return_value=''):
            connector = PostgreSQLConnector(mock_config, 'source')
            connector.connect = MagicMock()
            connector.disconnect = MagicMock()
            mock_cursor = MagicMock()
            connector.connection = MagicMock()
            connector.connection.cursor.return_value = mock_cursor

            mock_cursor.fetchall.side_effect = [
                [], # ENUMs
                [
                    (2000, 'public', 'money_amount', None, '"amount" numeric, "currency" currency_type', [1000]),
                    (1000, 'public', 'currency_type', None, '"code" text', [25]),
                ], # Composite types
                []  # Range types
            ]

            udts = connector.fetch_user_defined_types('public')
            self.assertEqual(len(udts), 2)
            self.assertEqual(udts[1]['type_name'], 'currency_type')
            self.assertEqual(udts[2]['type_name'], 'money_amount')

    def test_range_type_subtype_diff_syntax(self):
        mock_config = MagicMock()
        mock_config.get_log_file.return_value = 'migrator.log'
        with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
             patch.object(PostgreSQLConnector, 'prepare_session_settings', return_value=''):
            connector = PostgreSQLConnector(mock_config, 'source')
            connector.connect = MagicMock()
            connector.disconnect = MagicMock()
            mock_cursor = MagicMock()
            connector.connection = MagicMock()
            connector.connection.cursor.return_value = mock_cursor

            mock_cursor.fetchall.side_effect = [
                [], # ENUMs
                [], # Composite types
                [
                    ('public', 'weight_range', None, 'double precision', None, None, 'float8mi')
                ]  # Range types
            ]

            udts = connector.fetch_user_defined_types('public')
            self.assertEqual(len(udts), 1)
            self.assertIn('SUBTYPE_DIFF = float8mi', udts[1]['sql'])
            self.assertNotIn('SUBDIFF =', udts[1]['sql'])

    def test_create_domain_sql_not_null_deduplication(self):
        mock_config = MagicMock()
        mock_config.get_log_file.return_value = 'migrator.log'
        with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
             patch.object(PostgreSQLConnector, 'prepare_session_settings', return_value=''):
            connector = PostgreSQLConnector(mock_config, 'source')
            settings = {
                'domain_name': 'non_empty_text',
                'target_schema_name': 'migtest',
                'domain_data_type': 'text',
                'domain_not_null': True,
                'source_domain_check_sql': 'NOT NULL CHECK ((length(btrim(VALUE)) > 0))',
                'migrated_as': 'DOMAIN'
            }
            domain_sql = connector.get_create_domain_sql(settings)
            self.assertNotIn('NOT NULL NOT NULL', domain_sql)
            self.assertIn('NOT NULL CHECK', domain_sql)

    @patch('psycopg2.extras.execute_batch')
    def test_insert_batch_jsonb_serialization(self, mock_execute_batch):
        mock_config = MagicMock()
        mock_config.get_log_file.return_value = 'migrator.log'
        with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
             patch.object(PostgreSQLConnector, 'prepare_session_settings', return_value=''):
            connector = PostgreSQLConnector(mock_config, 'target')
            connector.connection = MagicMock()
            mock_cursor = MagicMock()
            connector.connection.cursor.return_value.__enter__.return_value = mock_cursor

            settings = {
                'target_schema_name': 'migtest',
                'target_table_name': 'app_settings',
                'target_columns': {
                    'col1': {'column_name': 'key', 'column_order': 1, 'data_type': 'text', 'is_nullable': 'NO'},
                    'col2': {'column_name': 'value', 'column_order': 2, 'data_type': 'jsonb', 'is_nullable': 'NO'},
                    'col3': {'column_name': 'updated_at', 'column_order': 3, 'data_type': 'timestamp with time zone', 'is_nullable': 'NO'}
                },
                'insert_columns': '"key", "value", "updated_at"',
                'data': [
                    {'key': 'feature.dark_mode', 'value': False, 'updated_at': '2026-08-05T13:01:38'},
                    {'key': 'feature.theme', 'value': {'primary': 'blue'}, 'updated_at': '2026-08-05T13:01:38'},
                    {'key': 'ui.theme', 'value': 'corporate', 'updated_at': '2026-08-05T13:01:38'},
                    {'key': 'maintenance.window', 'value': None, 'updated_at': '2026-08-05T13:01:38'}
                ],
                'worker_id': 'worker-1'
            }

            inserted_rows = connector.insert_batch(settings)
            self.assertEqual(inserted_rows, 4)
            mock_execute_batch.assert_called_once()
            passed_data = mock_execute_batch.call_args[0][2]
            
            self.assertEqual(passed_data[0], ('feature.dark_mode', 'false', '2026-08-05T13:01:38'))
            self.assertEqual(passed_data[1], ('feature.theme', '{"primary": "blue"}', '2026-08-05T13:01:38'))
            self.assertEqual(passed_data[2], ('ui.theme', '"corporate"', '2026-08-05T13:01:38'))
            self.assertEqual(passed_data[3], ('maintenance.window', 'null', '2026-08-05T13:01:38'))

    def test_fetch_sequences(self):
        mock_config = MagicMock()
        mock_config.get_log_file.return_value = 'migrator.log'
        with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
             patch.object(PostgreSQLConnector, 'prepare_session_settings', return_value=''):
            connector = PostgreSQLConnector(mock_config, 'source')
            connector.connect = MagicMock()
            connector.disconnect = MagicMock()
            mock_cursor = MagicMock()
            connector.connection = MagicMock()
            connector.connection.cursor.return_value = mock_cursor

            mock_cursor.fetchall.return_value = [('customer_events_event_id_seq', 54321)]
            connector.get_sequence_details = MagicMock(return_value={
                'min_value': 1,
                'max_value': 9223372036854775807,
                'increment_by': 1,
                'cycle': False,
                'cache_size': 1,
                'start_value': 1
            })
            mock_cursor.fetchone.return_value = (100, True)

            sequences = connector.fetch_sequences('public')
            self.assertEqual(len(sequences), 1)
            self.assertEqual(sequences[1]['sequence_name'], 'customer_events_event_id_seq')
            # The declared start of the sequence and the value it stands at are two different
            # facts and are reported as two. source_start_value used to carry the position,
            # because that is what the target has to start at - which made the protocol say a
            # sequence starts at 100 when it was declared to start at 1, and gave the target
            # sequence a START WITH which a RESTART of it would go back to.
            self.assertEqual(sequences[1]['source_start_value'], 1)
            self.assertEqual(sequences[1]['source_last_value'], 100)
            self.assertIn('CREATE SEQUENCE "public"."customer_events_event_id_seq"', sequences[1]['source_sequence_sql'])

    def test_migrate_single_sequence(self):
        mock_config = MagicMock()
        mock_config.get_log_file.return_value = 'migrator.log'
        with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
             patch.object(PostgreSQLConnector, 'prepare_session_settings', return_value=''):
            source_connector = PostgreSQLConnector(mock_config, 'source')
            source_connector.connect = MagicMock()
            source_connector.disconnect = MagicMock()
            mock_cursor = MagicMock()
            source_connector.connection = MagicMock()
            source_connector.connection.cursor.return_value = mock_cursor
            mock_cursor.fetchone.return_value = (988, True)

            target_connector = MagicMock()

            settings = {
                'source_schema_name': 'public',
                'target_schema_name': 'migtest',
                'source_sequence_name': 'countdown_seq',
                'target_sequence_name': 'countdown_seq',
                'source_increment_by': 1,
                'source_minvalue': 1,
                'source_maxvalue': 9223372036854775807,
                'source_start_value': 988,
                'source_cache': 1,
                'source_is_cycled': 'NO'
            }

            res = source_connector.migrate_sequences(target_connector, settings)
            self.assertTrue(res)
            target_connector.execute_query.assert_any_call('CREATE SEQUENCE IF NOT EXISTS "migtest"."countdown_seq" INCREMENT BY 1 MINVALUE 1 START WITH 988 CACHE 1 NO CYCLE;')
            target_connector.execute_query.assert_any_call('SELECT setval(\'"migtest"."countdown_seq"\', 988, true);')

    def test_migrate_single_sequence_keeps_the_declared_start_and_the_position_apart(self):
        """
        The sequence of the target is declared to start where the sequence of the source is
        declared to start, and is then set to where the source stands. Both facts survive the
        migration: a RESTART of the target sequence goes back to 1, as it does in the source,
        while the next value it hands out is behind the migrated rows.
        """
        mock_config = MagicMock()
        mock_config.get_log_file.return_value = 'migrator.log'
        with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
             patch.object(PostgreSQLConnector, 'prepare_session_settings', return_value=''):
            source_connector = PostgreSQLConnector(mock_config, 'source')
            source_connector.connect = MagicMock()
            source_connector.disconnect = MagicMock()
            mock_cursor = MagicMock()
            source_connector.connection = MagicMock()
            source_connector.connection.cursor.return_value = mock_cursor
            mock_cursor.fetchone.return_value = (5000, True)

            target_connector = MagicMock()

            settings = {
                'source_schema_name': 'public',
                'target_schema_name': 'migtest',
                'source_sequence_name': 'orders_seq',
                'target_sequence_name': 'orders_seq',
                'source_increment_by': 1,
                'source_minvalue': 1,
                'source_maxvalue': 9223372036854775807,
                'source_start_value': 1,
                'source_last_value': 5000,
                'source_cache': 1,
                'source_is_cycled': 'NO'
            }

            self.assertTrue(source_connector.migrate_sequences(target_connector, settings))
            target_connector.execute_query.assert_any_call('CREATE SEQUENCE IF NOT EXISTS "migtest"."orders_seq" INCREMENT BY 1 MINVALUE 1 START WITH 1 CACHE 1 NO CYCLE;')
            target_connector.execute_query.assert_any_call('SELECT setval(\'"migtest"."orders_seq"\', 5000, true);')

    def test_migrate_single_sequence_starts_where_the_source_stands_without_a_declared_start(self):
        """
        Oracle keeps no declared start value - ALL_SEQUENCES forgets it once the sequence
        runs - so the position is all there is, and the target has to start there.
        """
        mock_config = MagicMock()
        mock_config.get_log_file.return_value = 'migrator.log'
        with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
             patch.object(PostgreSQLConnector, 'prepare_session_settings', return_value=''):
            source_connector = PostgreSQLConnector(mock_config, 'source')
            source_connector.connect = MagicMock(side_effect=Exception('no source connection'))
            source_connector.disconnect = MagicMock()
            target_connector = MagicMock()

            settings = {
                'source_schema_name': 'public',
                'target_schema_name': 'migtest',
                'source_sequence_name': 'orders_seq',
                'target_sequence_name': 'orders_seq',
                'source_increment_by': 1,
                'source_start_value': None,
                'source_last_value': 5000,
                'source_is_cycled': 'NO'
            }

            self.assertTrue(source_connector.migrate_sequences(target_connector, settings))
            target_connector.execute_query.assert_any_call('CREATE SEQUENCE IF NOT EXISTS "migtest"."orders_seq" INCREMENT BY 1 START WITH 5000 NO CYCLE;')

    def test_fetch_table_columns_array_type(self):
        mock_config = MagicMock()
        mock_config.get_log_file.return_value = 'migrator.log'
        with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
             patch.object(PostgreSQLConnector, 'prepare_session_settings', return_value=''):
            connector = PostgreSQLConnector(mock_config, 'source')
            connector.connect = MagicMock()
            connector.disconnect = MagicMock()
            mock_cursor = MagicMock()
            connector.connection = MagicMock()
            connector.connection.cursor.return_value = mock_cursor

            mock_cursor.fetchall.return_value = [
                (1, 'id', 'integer', None, 32, 0, 'NO', 'NO', None, 'pg_catalog', 'int4', None, 'NO', 'integer', None, None),
                (2, 'tags', 'ARRAY', None, None, None, 'NO', 'YES', None, 'pg_catalog', '_text', None, 'NO', 'text[]', None, None)
            ]

            columns = connector.fetch_table_columns({'table_schema': 'public', 'table_name': 'customers'})
            self.assertEqual(columns[2]['data_type'], 'text[]')
            self.assertEqual(columns[2]['column_type'], 'text[]')

    def test_create_table_sql_bit_default(self):
        mock_config = MagicMock()
        mock_config.get_log_file.return_value = 'migrator.log'
        mock_config.get_source_db_type.return_value = 'postgresql'
        mock_config.convert_names_case.side_effect = lambda x: x
        mock_migrator_tables = MagicMock()
        mock_migrator_tables.get_domain_details.return_value = None
        with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
             patch.object(PostgreSQLConnector, 'prepare_session_settings', return_value=''):
            connector = PostgreSQLConnector(mock_config, 'target')
            connector.fetch_table_names = MagicMock(return_value={})
            settings = {
                'source_table_id': 1,
                'source_schema_name': 'public',
                'target_schema_name': 'migtest',
                'source_table_name': 'network_devices',
                'target_table_name': 'network_devices',
                'migrator_tables': mock_migrator_tables,
                'target_columns': {
                    1: {
                        'column_name': 'flags',
                        'data_type': 'BIT',
                        'is_nullable': 'NO',
                        'is_identity': 'NO',
                        'is_hidden_column': 'NO',
                        'is_generated_virtual': 'NO',
                        'is_generated_stored': 'NO',
                        'udt_schema': '',
                        'udt_name': '',
                        'domain_name': '',
                        'column_type_substitution': '',
                        'character_maximum_length': 8,
                        'numeric_precision': '',
                        'numeric_scale': '',
                        'basic_character_maximum_length': '',
                        'basic_numeric_precision': '',
                        'column_comment': '',
                        'column_default_name': '',
                        'column_default_value': "'00000000'::\"bit\"",
                        'replaced_column_default_value': "'00000000'::\"bit\""
                    }
                }
            }
            create_sql = connector.get_create_table_sql(settings)
            self.assertIn('"flags" BIT(8)', create_sql)
            self.assertIn("DEFAULT '00000000'::\"bit\"", create_sql)
            self.assertNotIn("::BOOLEAN", create_sql)

    def test_create_table_sql_mysql_bit_default(self):
        mock_config = MagicMock()
        mock_config.get_log_file.return_value = 'migrator.log'
        mock_config.get_source_db_type.return_value = 'mysql'
        mock_migrator_tables = MagicMock()
        mock_migrator_tables.get_domain_details.return_value = None
        with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
             patch.object(PostgreSQLConnector, 'prepare_session_settings', return_value=''):
            connector = PostgreSQLConnector(mock_config, 'target')
            connector.fetch_table_names = MagicMock(return_value={})
            settings = {
                'source_table_id': 1,
                'source_schema_name': 'public',
                'target_schema_name': 'migtest',
                'source_table_name': 'user_flags',
                'target_table_name': 'user_flags',
                'migrator_tables': mock_migrator_tables,
                'target_columns': {
                    1: {
                        'column_name': 'is_active',
                        'data_type': 'BIT',
                        'is_nullable': 'NO',
                        'is_identity': 'NO',
                        'is_hidden_column': 'NO',
                        'is_generated_virtual': 'NO',
                        'is_generated_stored': 'NO',
                        'udt_schema': '',
                        'udt_name': '',
                        'domain_name': '',
                        'column_type_substitution': '',
                        'character_maximum_length': '',
                        'numeric_precision': '',
                        'numeric_scale': '',
                        'basic_character_maximum_length': '',
                        'basic_numeric_precision': '',
                        'column_comment': '',
                        'column_default_name': '',
                        'column_default_value': "b'1'",
                        'replaced_column_default_value': "b'1'"
                    }
                }
            }
            create_sql = connector.get_create_table_sql(settings)
            self.assertIn("DEFAULT TRUE", create_sql)

    @patch('psycopg2.extras.execute_batch')
    def test_insert_batch_not_null_fallback(self, mock_execute_batch):
        mock_config = MagicMock()
        mock_config.get_log_file.return_value = 'migrator.log'
        with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
             patch.object(PostgreSQLConnector, 'prepare_session_settings', return_value=''):
            connector = PostgreSQLConnector(mock_config, 'target')
            connector.connection = MagicMock()
            mock_cursor = MagicMock()
            connector.connection.cursor.return_value.__enter__.return_value = mock_cursor

            settings = {
                'target_schema_name': 'migtest',
                'target_table_name': 'partial_records',
                'target_columns': {
                    1: {'column_name': 'record_id', 'data_type': 'integer', 'is_nullable': 'NO'},
                    2: {'column_name': 'label', 'data_type': 'text', 'is_nullable': 'NO'},
                    3: {'column_name': 'reference', 'data_type': 'text', 'is_nullable': 'NO'},
                    4: {'column_name': 'imported_at', 'data_type': 'timestamp with time zone', 'is_nullable': 'NO'}
                },
                'insert_columns': '"record_id", "label", "reference", "imported_at"',
                'data': [
                    {'record_id': 4, 'label': 'record-4', 'reference': None, 'imported_at': '2026-08-05 13:01:38.129707+00'}
                ],
                'worker_id': 'worker-1'
            }

            inserted_rows = connector.insert_batch(settings)
            self.assertEqual(inserted_rows, 1)
            mock_execute_batch.assert_called_once()
            passed_data = mock_execute_batch.call_args[0][2]
            
            self.assertEqual(passed_data[0], (4, 'record-4', '', '2026-08-05 13:01:38.129707+00'))

    @patch('psycopg2.connect')
    def test_register_type_casters(self, mock_connect):
        mock_config = MagicMock()
        mock_config.get_log_file.return_value = 'migrator.log'
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
             patch.object(PostgreSQLConnector, 'prepare_session_settings', return_value=''), \
             patch('psycopg2.extensions.register_type') as mock_register_type:
            connector = PostgreSQLConnector(mock_config, 'source')
            connector.connect()
            mock_register_type.assert_called_once()
            call_args = mock_register_type.call_args
            self.assertEqual(call_args[0][1], mock_conn)

    def test_create_index_sql_with_collation(self):
        mock_config = MagicMock()
        mock_config.get_log_file.return_value = 'migrator.log'
        mock_config.convert_names_case.side_effect = lambda x: x
        with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
             patch.object(PostgreSQLConnector, 'prepare_session_settings', return_value=''):
            connector = PostgreSQLConnector(mock_config, 'target')
            settings = {
                'index_name': 'countries_name_natural_idx',
                'index_type': 'INDEX',
                'target_schema_name': 'migtest',
                'target_table_name': 'countries',
                'index_columns': 'name COLLATE natural_numeric',
                'target_columns': {
                    1: {'column_name': 'name', 'data_type': 'text'}
                }
            }
            create_sql = connector.get_create_index_sql(settings)
            self.assertIn('("name" COLLATE "natural_numeric")', create_sql)
            self.assertNotIn('"name COLLATE natural_numeric"', create_sql)

    def test_create_index_sql_with_opclass(self):
        mock_config = MagicMock()
        mock_config.get_log_file.return_value = 'migrator.log'
        mock_config.convert_names_case.side_effect = lambda x: x
        with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
             patch.object(PostgreSQLConnector, 'prepare_session_settings', return_value=''):
            connector = PostgreSQLConnector(mock_config, 'target')
            settings = {
                'index_name': 'customer_events_type_payload_idx',
                'index_type': 'INDEX',
                'target_schema_name': 'migtest',
                'target_table_name': 'customer_events',
                'index_columns': 'event_type gin_trgm_ops, payload',
                'target_columns': {
                    1: {'column_name': 'event_type', 'data_type': 'text'},
                    2: {'column_name': 'payload', 'data_type': 'jsonb'}
                }
            }
            create_sql = connector.get_create_index_sql(settings)
            self.assertIn('("event_type" "gin_trgm_ops", "payload")', create_sql)
            self.assertNotIn('"event_type gin_trgm_ops"', create_sql)

    def test_create_index_sql_with_using_gin(self):
        mock_config = MagicMock()
        mock_config.get_log_file.return_value = 'migrator.log'
        mock_config.get_source_db_type.return_value = 'postgresql'
        mock_config.convert_names_case.side_effect = lambda x: x
        with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
             patch.object(PostgreSQLConnector, 'prepare_session_settings', return_value=''):
            connector = PostgreSQLConnector(mock_config, 'target')
            settings = {
                'index_name': 'customer_notes_body_gin_idx',
                'index_type': 'INDEX',
                'target_schema_name': 'migtest',
                'target_table_name': 'customer_notes',
                'index_columns': 'body_tsv',
                'index_sql': 'CREATE INDEX customer_notes_body_gin_idx ON customer_notes USING gin (body_tsv)',
                'target_columns': {
                    1: {'column_name': 'body_tsv', 'data_type': 'tsvector'}
                }
            }
            create_sql = connector.get_create_index_sql(settings)
            self.assertIn('USING gin ("body_tsv")', create_sql)

if __name__ == '__main__':
    unittest.main()
