
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
        return []
    def print_log_message(self, level, message):
         pass
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

def reproduce_semicolon():
    config_parser = MockConfigParser()
    connector = SybaseASEConnector(config_parser, 'source')
    
    # Sybase code from the user issue
    sybase_code = """
    create proc
    config_checkFP
        @techauftrag TypID
    as
    BEGIN
        select
            cgi.fp_enabled
        from
            cgi,
            auftragmatrix am
        where
            am.techauftrag_id = @techauftrag
            and
            am.artikelgruppe_id = 7
            and
            am.artikel_id = cgi.cgi_id
    END
    """
    
    settings = {
        'funcproc_code': sybase_code,
        'target_db_type': 'postgresql',
        'target_schema': 'migraene'
    }
    
    print("Testing Procedure Semicolon Insertion...")
    try:
        converted = connector.convert_funcproc_code(settings)
        print("Converted Code:")
        print(converted)
        
        # We expect the SELECT block to be terminated by ; before END
        # The output snippet from user showed "am.artikel_id = cgi.cgi_id\nEND;"
        # Correct should be "am.artikel_id = cgi.cgi_id;\nEND;"
        
        # Check specifically for the end of the select
        if re.search(r'cgi\.cgi_id\s*;\s*END', converted, flags=re.IGNORECASE | re.DOTALL):
            print("\nSUCCESS: Found semicolon before END.")
        elif re.search(r'cgi\.cgi_id\s*END', converted, flags=re.IGNORECASE | re.DOTALL):
            print("\nFAIL: Missing semicolon before END.")
            sys.exit(1)
        else:
            print("\nFAIL: Could not verify pattern.")
            sys.exit(1)
            
    except Exception as e:
        print(f"Exception: {e}")
        sys.exit(1)

if __name__ == "__main__":
    reproduce_semicolon()
