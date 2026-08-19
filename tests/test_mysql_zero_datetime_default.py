import unittest
from unittest.mock import MagicMock
from credativ_pg_migrator.config_parser import ConfigParser
from credativ_pg_migrator.connectors.mysql_connector import MySQLConnector
from credativ_pg_migrator.connectors.postgresql_connector import PostgreSQLConnector

class TestZeroDatetimeDefault(unittest.TestCase):
    def _create_mysql_connector(self, zero_datetime_config_value):
        config_parser = MagicMock(spec=ConfigParser)
        config_parser.get_zero_datetime_default.return_value = zero_datetime_config_value
        
        connector = MySQLConnector.__new__(MySQLConnector)
        connector.config_parser = config_parser
        return connector

    def test_default_remove_zero_datetime(self):
        connector = self._create_mysql_connector('remove')
        
        # Test '0000-00-00'
        res1 = connector.convert_default_value({'extracted_default_value': '0000-00-00', 'column_type': 'date'})
        self.assertEqual(res1, '')

        # Test '0000-00-00 00:00:00'
        res2 = connector.convert_default_value({'extracted_default_value': '0000-00-00 00:00:00', 'column_type': 'timestamp'})
        self.assertEqual(res2, '')

        # Test '0000-00-00 00:00:00.000000'
        res3 = connector.convert_default_value({'extracted_default_value': '0000-00-00 00:00:00.000000', 'column_type': 'datetime'})
        self.assertEqual(res3, '')

    def test_replace_zero_datetime_with_string_value(self):
        connector = self._create_mysql_connector('1970-01-01 00:00:00')
        
        res = connector.convert_default_value({'extracted_default_value': '0000-00-00 00:00:00', 'column_type': 'timestamp'})
        self.assertEqual(res, "'1970-01-01 00:00:00'")

    def test_replace_zero_datetime_with_sql_keyword(self):
        connector = self._create_mysql_connector('CURRENT_TIMESTAMP')
        
        res = connector.convert_default_value({'extracted_default_value': '0000-00-00 00:00:00', 'column_type': 'timestamp'})
        self.assertEqual(res, 'CURRENT_TIMESTAMP')

    def test_non_zero_datetime_preserved(self):
        connector = self._create_mysql_connector('remove')
        
        res = connector.convert_default_value({'extracted_default_value': '2026-01-01', 'column_type': 'date'})
        self.assertEqual(res, '2026-01-01')

    def test_postgresql_ddl_generation(self):
        config_parser = MagicMock(spec=ConfigParser)
        config_parser.get_zero_datetime_default.return_value = 'remove'
        config_parser.convert_names_case.side_effect = lambda x: x

        pg_connector = PostgreSQLConnector.__new__(PostgreSQLConnector)
        pg_connector.config_parser = config_parser
        pg_connector.split_top_level_commas = MagicMock(return_value=[])

        column_info = {
            'column_name': 'order_date',
            'data_type': 'DATE',
            'is_nullable': 'NO',
            'column_default_name': '',
            'column_default_value': '0000-00-00',
            'replaced_column_default_value': '',
            'is_identity': 'NO',
            'is_generated_virtual': 'NO',
            'is_generated_stored': 'NO',
            'is_hidden_column': 'NO',
            'basic_character_maximum_length': '',
            'basic_numeric_precision': '',
            'basic_numeric_scale': '',
            'character_maximum_length': '',
            'column_comment': '',
            'domain_name': '',
        }
        
        # Verify zero date default is stripped and DEFAULT clause is removed in Postgres connector
        create_sql = pg_connector.get_create_table_sql({
            'source_schema_name': 'public',
            'source_table_name': 'test_tbl',
            'source_table_id': 1,
            'target_schema_name': 'public',
            'target_table_name': 'test_tbl',
            'target_table_id': 1,
            'target_columns': {1: column_info},
            'columns': {1: column_info},
            'primary_keys': [],
            'table_comment': '',
            'migrator_tables': MagicMock(),
        })

        self.assertIn('"order_date" DATE NOT NULL', create_sql)
        self.assertNotIn('DEFAULT', create_sql)

if __name__ == '__main__':
    unittest.main()
