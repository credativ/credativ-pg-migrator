import unittest
from unittest.mock import MagicMock
from credativ_pg_migrator.config_parser import ConfigParser
from credativ_pg_migrator.connectors.postgresql_connector import PostgreSQLConnector
from credativ_pg_migrator.planner import Planner

class TestExtensionCheck(unittest.TestCase):
    def test_get_required_extensions_inferred(self):
        config_parser = ConfigParser.__new__(ConfigParser)
        config_parser.config = {'migration': {'uuid_default_function': 'uuid_generate_v4()'}}
        
        exts = config_parser.get_required_extensions()
        self.assertIn('uuid-ossp', exts)

    def test_get_required_extensions_explicit(self):
        config_parser = ConfigParser.__new__(ConfigParser)
        config_parser.config = {'migration': {'required_extensions': ['postgis', 'pgcrypto']}}
        
        exts = config_parser.get_required_extensions()
        self.assertIn('postgis', exts)
        self.assertIn('pgcrypto', exts)

    def test_check_and_create_extension_mock(self):
        connector = PostgreSQLConnector.__new__(PostgreSQLConnector)
        connector.config_parser = MagicMock()
        
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        connector.connection = mock_conn
        connector.connect = MagicMock()
        connector.disconnect = MagicMock()

        success, msg = connector.check_and_create_extension('uuid-ossp')
        self.assertTrue(success)
        self.assertIn("present", msg)

if __name__ == '__main__':
    unittest.main()
