
import re
import sys
from unittest.mock import MagicMock

# Mock ConfigParser
class MockConfigParser:
    def get_log_file(self):
        return 'test.log'
    def get_on_error_action(self):
        return 'continue'
    def get_data_types_substitution(self):
        # We simulate that user has NOT provided specific substitution for 'MyType',
        # so it falls back to UDT logic.
        return []
    def print_log_message(self, level, message):
         if level in ('ERROR', 'WARNING'):
            print(f"[{level}] {message}") 
    def convert_names_case(self, name):
        return name.lower()
    def get_target_db_type(self):
        return 'postgresql'
    def get_source_schema(self):
        return 'dbo'
    def get_target_schema(self):
        return 'migraene'

# Mocking dependencies
sys.modules['jaydebeapi'] = MagicMock()
sys.modules['pyodbc'] = MagicMock()
sys.modules['jaydebeapi'].Error = Exception
sys.modules['pyodbc'].Error = Exception
sys.modules['tabulate'] = MagicMock()
sys.modules['sqlglot'] = MagicMock()

try:
    from credativ_pg_migrator.connectors.sybase_ase_connector import SybaseASEConnector
except ImportError:
    sys.path.append('/home/josef/github.com/credativ/credativ-pg-migrator')
    from credativ_pg_migrator.connectors.sybase_ase_connector import SybaseASEConnector

def reproduce_casing():
    config_parser = MockConfigParser()
    connector = SybaseASEConnector(config_parser, 'source')
    
    # We mock _get_udt_codes_mapping to return specific types
    # Case 1: UDT defined in UPPERCASE in DB, used in lowercase in code
    # Case 2: UDT defined directly in MixedCase, used in UPPERCASE
    
    # Mocking the UDT cache so we don't need real DB connection
    connector._udt_cache = {
        'MyType': 'VARCHAR(100)',
        'OTHER_TYPE': 'INTEGER'
    }
    
    # Test cases
    test_cases = [
        ("DECLARE @v MyType", "DECLARE v VARCHAR(100);"), 
        ("DECLARE @v mytype", "DECLARE v VARCHAR(100);"), # Lowercase usage
        ("DECLARE @v MYTYPE", "DECLARE v VARCHAR(100);"), # Uppercase usage
        ("DECLARE @v OTHER_TYPE", "DECLARE v INTEGER;"),
        ("DECLARE @v other_type", "DECLARE v INTEGER;"),
        ("DECLARE @v [MyType]", "DECLARE v VARCHAR(100);"), # Brackets
        ("DECLARE @v \"MyType\"", "DECLARE v VARCHAR(100);"), # Double quotes
    ]
    
    failure_count = 0
    
    print("Testing UDT substitution casing...")
    for sql_input, expected_part in test_cases:
        # We wrap input in a dummy proc structure so convert_funcproc_code can process it
        code = f"""
        CREATE PROC test_proc AS
        BEGIN
            {sql_input}
        END
        """
        
        settings = {
            'funcproc_code': code,
            'target_db_type': 'postgresql',
            'target_schema': 'migraene'
        }
        
        try:
            converted = connector.convert_funcproc_code(settings)
            
            # Check if expected part is in converted output
            # We need to be careful about whitespace/formatting
            # convert_funcproc_code does formatting like ";\n"
            
            # Normalize whitespace for comparison
            converted_norm = " ".join(converted.split())
            expected_norm = " ".join(expected_part.split())
            
            # We look for the declaration in the valid output
            # Note: convert_funcproc_code puts declarations in DECLARE block
            
            if expected_norm in converted_norm:
                print(f"[PASS] Input: {sql_input} -> Found: {expected_part}")
            else:
                print(f"[FAIL] Input: {sql_input}")
                print(f"       Expected to find: {expected_part}")
                print(f"       Actual Output (snippet): {converted}")
                failure_count += 1
                
        except Exception as e:
            print(f"[ERROR] Input: {sql_input} caused exception: {e}")
            failure_count += 1

    if failure_count > 0:
        print(f"\nFAILED: {failure_count} tests failed.")
        sys.exit(1)
    else:
        print("\nSUCCESS: All casing tests passed.")

if __name__ == "__main__":
    reproduce_casing()
