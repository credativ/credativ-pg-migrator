import unittest
from unittest.mock import MagicMock
from credativ_pg_migrator.connectors.mysql_connector import MySQLConnector
from credativ_pg_migrator.config_parser import ConfigParser

class TestMySQLDefaultValues(unittest.TestCase):
    def test_convert_default_value_uuid(self):
        config_parser = ConfigParser.__new__(ConfigParser)
        config_parser.config = {'migration': {'uuid_default_function': 'gen_random_uuid()'}}

        connector = MySQLConnector.__new__(MySQLConnector)
        connector.config_parser = config_parser
        
        # Test uuid_to_bin(uuid(), 1) on TEXT column
        res1 = connector.convert_default_value({'extracted_default_value': 'uuid_to_bin(uuid(), 1)', 'column_type': 'TEXT'})
        self.assertEqual(res1, 'gen_random_uuid()::text')
        
        # Test uuid_to_bin(uuid(),1) on UUID column
        res2 = connector.convert_default_value({'extracted_default_value': 'uuid_to_bin(uuid(),1)', 'column_type': 'UUID'})
        self.assertEqual(res2, 'gen_random_uuid()')
        
        # Test uuid() on UUID column
        res3 = connector.convert_default_value({'extracted_default_value': 'uuid()', 'column_type': 'UUID'})
        self.assertEqual(res3, 'gen_random_uuid()')
        
        # Test sysdate()
        res4 = connector.convert_default_value({'extracted_default_value': 'sysdate()'})
        self.assertEqual(res4, 'current_timestamp')
        
        # Test curdate()
        res5 = connector.convert_default_value({'extracted_default_value': 'curdate()'})
        self.assertEqual(res5, 'current_date')

    def test_custom_uuidv7_default_function(self):
        config_parser = ConfigParser.__new__(ConfigParser)
        config_parser.config = {'migration': {'uuid_default_function': 'uuidv7()'}}

        connector = MySQLConnector.__new__(MySQLConnector)
        connector.config_parser = config_parser

        # Test uuidv7() on UUID target column
        res_uuid = connector.convert_default_value({'extracted_default_value': 'uuid()', 'column_type': 'UUID'})
        self.assertEqual(res_uuid, 'uuidv7()')

        # Test uuidv7() on VARCHAR(36) target column
        res_text = connector.convert_default_value({'extracted_default_value': 'uuid_to_bin(uuid(), 1)', 'column_type': 'VARCHAR(36)'})
        self.assertEqual(res_text, 'uuidv7()::text')

if __name__ == '__main__':
    unittest.main()
