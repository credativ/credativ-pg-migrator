import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch
from credativ_pg_migrator.orchestrator import Orchestrator


TARGET_COLUMNS = {
    '1': {'column_name': 'doc_id', 'data_type': 'integer',
          'is_generated_virtual': 'NO', 'is_generated_stored': 'NO'},
    '2': {'column_name': 'name_upper', 'data_type': 'text',
          'is_generated_virtual': 'NO', 'is_generated_stored': 'YES'},
    '3': {'column_name': 'file_name', 'data_type': 'text',
          'is_generated_virtual': 'NO', 'is_generated_stored': 'NO'},
    '4': {'column_name': 'content', 'data_type': 'bytea',
          'is_generated_virtual': 'NO', 'is_generated_stored': 'NO'},
    '5': {'column_name': 'byte_size', 'data_type': 'integer',
          'is_generated_virtual': 'YES', 'is_generated_stored': 'NO'},
}


class TestLobWorkerGeneratedColumns(unittest.TestCase):

    def setUp(self):
        self.lob_dir = tempfile.mkdtemp()
        self.payload = b'BLOB-PAYLOAD'
        with open(os.path.join(self.lob_dir, 'lob1'), 'wb') as handle:
            handle.write(self.payload)
        self.pointer = f"0,{len(self.payload):x},lob1"

    def tearDown(self):
        shutil.rmtree(self.lob_dir)

    def _run(self, target_columns, lob_col_index):
        config = MagicMock()
        config.get_log_file.return_value = 'migrator.log'
        connections = []

        def make_connection(_which):
            connection = MagicMock()
            cursor = MagicMock()
            # the SELECT from the staging table returns only the selected columns
            cursor.fetchone.side_effect = [(1, 'a.bin', self.pointer), None]
            connection.connection.cursor.return_value = cursor
            connections.append(connection)
            return connection

        with patch('credativ_pg_migrator.orchestrator.MigratorLogger'):
            orchestrator = Orchestrator.__new__(Orchestrator)
            orchestrator.config_parser = config
            orchestrator.on_error_action = 'stop'
            orchestrator.load_connector = make_connection
            result = orchestrator.lob_worker({
                'target_schema_name': 'migtest', 'target_table_name': 'docs',
                'primary_key_columns': '', 'unl_import_table': 'docs_lob_import',
                'all_lob_columns': ['content'], 'lob_column': 'content',
                'lob_col_index': lob_col_index, 'lob_col_type': 'bytea',
                'lob_columns_count': 1, 'target_columns': target_columns,
                'datafile': 'lob1', 'datafiles_count': 1, 'current_datafile_num': 1,
                'occurrences': 1, 'lob_files_path': self.lob_dir,
            })
        calls = [call[0] for connection in connections
                 for call in connection.connection.cursor.return_value.execute.call_args_list]
        statements = [call[0] for call in calls]
        self.calls = calls
        return result, statements

    def test_generated_columns_are_excluded_from_select_and_insert(self):
        result, statements = self._run(TARGET_COLUMNS, lob_col_index=4)
        self.assertTrue(result)
        select_sql = next(sql for sql in statements if sql.strip().upper().startswith('SELECT'))
        insert_sql = next(sql for sql in statements if 'INSERT INTO' in sql)
        for generated in ('name_upper', 'byte_size'):
            self.assertNotIn(generated, select_sql)
            self.assertNotIn(generated, insert_sql)
        self.assertIn('"doc_id", "file_name", "content"', select_sql)
        self.assertIn('"doc_id", "file_name", "content"', insert_sql)

    def test_lob_value_is_read_although_the_index_shifted(self):
        # The caller passes position 4 - the ordinal in the full column list. After the
        # generated column in front of it is removed, the value sits at position 3, so the
        # index has to be re-derived or the LOB pointer would be read from the wrong column.
        result, _ = self._run(TARGET_COLUMNS, lob_col_index=4)
        self.assertTrue(result)
        insert_call = next(call for call in self.calls if 'INSERT INTO' in call[0])
        parameters = insert_call[1]
        # doc_id, file_name, the resolved LOB content, and the trailing value for the UPDATE
        self.assertEqual(parameters, [1, 'a.bin', self.payload, self.payload])

    def test_broken_lob_pointer_leaves_the_row_with_null(self):
        columns = TARGET_COLUMNS
        config = MagicMock()
        config.get_log_file.return_value = 'migrator.log'
        connections = []

        def make_connection(_which):
            connection = MagicMock()
            cursor = MagicMock()
            cursor.fetchone.side_effect = [(1, 'a.bin', '0,10,does_not_exist'), None]
            connection.connection.cursor.return_value = cursor
            connections.append(connection)
            return connection

        with patch('credativ_pg_migrator.orchestrator.MigratorLogger'):
            orchestrator = Orchestrator.__new__(Orchestrator)
            orchestrator.config_parser = config
            orchestrator.on_error_action = 'stop'
            orchestrator.load_connector = make_connection
            result = orchestrator.lob_worker({
                'target_schema_name': 'migtest', 'target_table_name': 'docs',
                'primary_key_columns': '', 'unl_import_table': 'docs_lob_import',
                'all_lob_columns': ['content'], 'lob_column': 'content',
                'lob_col_index': 4, 'lob_col_type': 'bytea',
                'lob_columns_count': 1, 'target_columns': columns,
                'datafile': 'lob1', 'datafiles_count': 1, 'current_datafile_num': 1,
                'occurrences': 1, 'lob_files_path': self.lob_dir,
            })
        self.assertTrue(result)
        calls = [call[0] for connection in connections
                 for call in connection.connection.cursor.return_value.execute.call_args_list]
        insert_call = next(call for call in calls if 'INSERT INTO' in call[0])
        self.assertEqual(insert_call[1], [1, 'a.bin', None, None])

    def test_table_without_generated_columns_keeps_all_columns(self):
        columns = {key: dict(value, is_generated_virtual='NO', is_generated_stored='NO')
                   for key, value in TARGET_COLUMNS.items()}
        result, statements = self._run(columns, lob_col_index=4)
        self.assertTrue(result)
        select_sql = next(sql for sql in statements if sql.strip().upper().startswith('SELECT'))
        self.assertIn('"name_upper"', select_sql)
        self.assertIn('"byte_size"', select_sql)


if __name__ == '__main__':
    unittest.main()
