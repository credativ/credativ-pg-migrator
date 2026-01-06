import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Adjust path to import the connector
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Mock dependencies BEFORE importing the connector
sys.modules['jaydebeapi'] = MagicMock()
sys.modules['pyodbc'] = MagicMock()
# sys.modules['credativ_pg_migrator.database_connector'] = MagicMock() # Removed to avoid StopIteration issues
sys.modules['tabulate'] = MagicMock()
# We need to mock DatabaseConnector class specifically because SybaseASEConnector inherits from it
# But since we import it, we might need to be careful.
# Actually, if we mock the module, the import might fail if we expect a class.
# Let's try to mock the class in the module.

from credativ_pg_migrator.connectors.sybase_ase_connector import SybaseASEConnector

class TestSybaseUDTSubstitution(unittest.TestCase):
    def setUp(self):
        self.mock_config = MagicMock()
        self.mock_config.get_on_error_action.return_value = 'stop'
        self.mock_config.get_log_file.return_value = 'test.log'
        self.mock_config.get_db_config.return_value = {'username': 'u', 'password': 'p', 'jdbc': {'driver': 'd', 'libraries': 'l'}}
        self.mock_config.get_connectivity.return_value = 'jdbc'
        self.mock_config.get_connect_string.return_value = 'conn_str'
        
        # Mock Config Substitutions
        # [schema, table, source, target, comment]
        # Let's say 'ConfigType' is mapped to 'INTEGER' in config
        self.mock_config.get_data_types_substitution.return_value = [
            ['', '', 'ConfigType', 'INTEGER', ''] 
        ]

        # Initialize connector
        self.connector = SybaseASEConnector(self.mock_config, 'source')
        
        # Mock DB Connection and Cursor
        self.connector.connection = MagicMock()
        self.mock_cursor = MagicMock()
        self.connector.connection.cursor.return_value = self.mock_cursor

    def test_get_udt_mapping(self):
        # Mock fetchall return for UDTs
        # row: type_name, length, prec, scale, base_type_name
        self.mock_cursor.fetchall.return_value = [
            ('MyBaseType', 10, None, None, 'varchar'),
            ('NumType', None, 10, 2, 'numeric'),
            ('ConfigType', 50, None, None, 'varchar'), # This one is also in config substitution!
        ]

        udt_map = self.connector._get_udt_codes_mapping()
        
        self.assertEqual(udt_map['MyBaseType'], 'VARCHAR(10)')
        self.assertEqual(udt_map['NumType'], 'NUMERIC(10,2)')
        self.assertEqual(udt_map['ConfigType'], 'VARCHAR(50)')

    def test_substitution_logic_base(self):
        # Setup UDT cache manually to avoid DB call in this test
        self.connector._udt_cache = {
            'MyBaseType': 'VARCHAR(10)',
            'NumType': 'NUMERIC(10,2)'
        }
        # Config substitution for this test is empty or irrelevant
        self.mock_config.get_data_types_substitution.return_value = []
        
        text = "DECLARE @x MyBaseType; DECLARE @y NumType;"
        result = self.connector._apply_udt_to_base_type_substitutions(text)
        
        self.assertIn("VARCHAR(10)", result)
        self.assertIn("NUMERIC(10,2)", result)
        self.assertNotIn("MyBaseType", result)

    def test_substitution_priority(self):
        # 'ConfigType' is in config -> INTEGER
        # 'ConfigType' is in UDT -> VARCHAR(50) (if we checked DB)
        
        # 1. Setup UDT cache
        self.connector._udt_cache = {
            'ConfigType': 'VARCHAR(50)',
            'OtherType': 'TEXT'
        }
        
        # 2. Setup Config
        self.mock_config.get_data_types_substitution.return_value = [
            ['', '', 'ConfigType', 'INTEGER', '']
        ]
        
        # 3. Test _apply_data_type_substitutions first (User calls this first)
        text = "DECLARE @a ConfigType; DECLARE @b OtherType;"
        
        # Standard flow in conversion:
        text = self.connector._apply_data_type_substitutions(text)
        # Should be: DECLARE @a INTEGER; DECLARE @b OtherType;
        self.assertIn("INTEGER", text)
        self.assertNotIn("ConfigType", text)
        
        # Now call UDT substitution
        text = self.connector._apply_udt_to_base_type_substitutions(text)
        # Should be: DECLARE @a INTEGER; DECLARE @b TEXT;
        self.assertIn("TEXT", text)
        self.assertNotIn("OtherType", text)
        
        # Ensure INTEGER wasn't touched (UDT logic shouldn't corrupt it)
        self.assertIn("INTEGER", text)

    def test_convert_funcproc_integration(self):
        # Integration test mocking the whole flow
        self.mock_cursor.fetchall.return_value = [
            ('MyBaseType', 20, None, None, 'varchar')
        ]
        self.connector._udt_cache = None # Force fetch
        self.mock_config.get_data_types_substitution.return_value = []
        
        code = """
        CREATE PROCEDURE test_proc
        @p1 MyBaseType
        AS
        DECLARE @v1 MyBaseType
        SELECT @v1 = @p1
        """
        
        settings = {
            'funcproc_code': code,
            'target_db_type': 'postgresql',
            'target_schema': 'public'
        }
        
        result = self.connector.convert_funcproc_code(settings)
        
        # param should be converted
        self.assertIn("p1 VARCHAR(20)", result)
        # declaration should be converted
        self.assertIn("v1 VARCHAR(20);", result)

if __name__ == '__main__':
    unittest.main()
