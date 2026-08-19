import unittest
from unittest.mock import MagicMock
from credativ_pg_migrator.connectors.mysql_connector import MySQLConnector

class TestMySQLFetchIndexes(unittest.TestCase):
    def test_fetch_indexes_with_expression_and_none_column_name(self):
        connector = MySQLConnector.__new__(MySQLConnector)
        connector.config_parser = None
        connector.connect = MagicMock()
        connector.disconnect = MagicMock()
        
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)  # Simulate EXPRESSION column exists in INFORMATION_SCHEMA.STATISTICS
        # Simulate query response where COLUMN_NAME (row[1]) is None and EXPRESSION (row[6]) is present
        mock_cursor.fetchall.return_value = [
            ('idx_expr', None, 1, 0, 'INDEX', '', 'lower(`email`)'),
            ('idx_multi', 'col1', 1, 0, 'INDEX', '', None),
            ('idx_multi', None, 2, 0, 'INDEX', '', None),
            ('idx_multi', 'col2', 3, 0, 'INDEX', '', None),
        ]
        connector.connection = MagicMock()
        connector.connection.cursor.return_value = mock_cursor

        settings = {
            'source_table_id': 1,
            'source_table_schema': 'test_db',
            'source_table_name': 'test_tbl',
        }

        result = connector.fetch_indexes(settings)

        self.assertIn(1, result)
        self.assertEqual(result[1]['index_name'], 'idx_expr')
        self.assertIn(result[1]['index_columns'], ('lower("email")', "lower('email')"))
        self.assertEqual(result[1]['is_function_based'], 'YES')

        self.assertIn(2, result)
        self.assertEqual(result[2]['index_name'], 'idx_multi')
        self.assertEqual(result[2]['index_columns'], 'col1, col2')

    def test_clean_index_expression_utf8mb4_and_charset(self):
        connector = MySQLConnector.__new__(MySQLConnector)
        connector.config_parser = None

        expr1 = r"""cast(json_extract("attributes",_utf8mb4\'$.tags\') as char(32) array)"""
        cleaned1 = connector.clean_index_expression(expr1)
        self.assertNotIn('_utf8mb4', cleaned1)
        self.assertNotIn('\\', cleaned1)
        self.assertIn('CHAR(32)[]', cleaned1.upper())

        expr2 = r"""cast(json_unquote(json_extract("attributes",_utf8mb4\'$.color\')) as char(20) charset utf8mb4)"""
        cleaned2 = connector.clean_index_expression(expr2)
        self.assertNotIn('_utf8mb4', cleaned2)
        self.assertNotIn('charset', cleaned2.lower())
        self.assertNotIn('\\', cleaned2)

    def test_fetch_constraints_with_none_column_name(self):
        connector = MySQLConnector.__new__(MySQLConnector)
        connector.config_parser = None
        connector.connect = MagicMock()
        connector.disconnect = MagicMock()
        
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ('test_db', 'test_tbl', 'col1', 'fk_test', 'test_db', 'ref_tbl', 'ref_col', 1, 1),
            ('test_db', 'test_tbl', None, 'fk_test', 'test_db', 'ref_tbl', None, 2, 2),
        ]
        connector.connection = MagicMock()
        connector.connection.cursor.return_value = mock_cursor

        settings = {
            'source_table_id': 1,
            'source_table_schema': 'test_db',
            'source_table_name': 'test_tbl',
        }

        result = connector.fetch_constraints(settings)
        self.assertIn(1, result)
        self.assertEqual(result[1]['constraint_name'], 'fk_test')
        self.assertEqual(result[1]['constraint_columns'], 'col1')

if __name__ == '__main__':
    unittest.main()
