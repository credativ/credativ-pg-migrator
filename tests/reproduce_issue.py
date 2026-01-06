
import re
import sys
import traceback

# Mock ConfigParser
class MockConfigParser:
    def get_log_file(self):
        return 'test.log'
    def get_on_error_action(self):
        return 'continue'
    def get_data_types_substitution(self):
        return []
    def print_log_message(self, level, message):
        pass # print(f"[{level}] {message}")
    def convert_names_case(self, name):
        return name.lower()
    def get_target_db_type(self):
        return 'postgresql'
    def get_source_schema(self):
        return 'dbo'
    def get_target_schema(self):
        return 'migraene' 

# Mocking dependencies
from unittest.mock import MagicMock
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

def reproduce():
    config_parser = MockConfigParser()
    connector = SybaseASEConnector(config_parser, 'source')
    
    # Sybase code from the user issue
    sybase_code = """
    create proc access_update
    as
    BEGIN

    declare @count int,
        @access_id numeric(10,0)

    declare access_cursor cursor
    for select access_id from access where erlaubte_einwahl & 8 != 0
    for update
    open access_cursor

    select @count = 0

    while (1=1)
    begin

    fetch access_cursor into @access_id
    if (@@sqlstatus!=0) break

    select @count = @count+1
    if (@count % 500) = 0 print '[%1!]',@count

    update access set erlaubte_einwahl = erlaubte_einwahl | 64
    where current of access_cursor

    end
    close access_cursor
    deallocate cursor access_cursor

    END
    """
    
    # Setting up inputs for convert_trigger (reusing it as it seems to hold the logic key)
    # The user said "Error migrating Procedure access_update", but let's see if we can use
    # convert_trigger's logic which seemed to contain the body processing we saw.
    # Actually, looking at the file content previously, convert_trigger calls internal logic or contains it.
    # Let's try to call convert_trigger and see if it processes the body.
    # Or maybe there is convert_procedure? I didn't see it in grep.
    # Wait, the user provided `create proc access_update`. 
    # If I search for `convert_trigger` in the file, I saw line 1301 `CREATE OR REPLACE FUNCTION ...`.
    # That means `convert_trigger` produces a FUNCTION? 
    # Yes, Sybase triggers/procs often become PG functions.
    
    settings = {
        'funcproc_code': sybase_code,
        'target_db_type': 'postgresql',
        'target_schema': 'migraene'
    }
    
    print("Converting code...")
    try:
        # SybaseASEConnector.convert_funcproc_code(self, settings)
        converted = connector.convert_funcproc_code(settings)
        print("Conversion Result:")
        print(converted)
        
        # Validation checks
        errors = []
        if "IF (@@sqlstatus!=0) THEN" in converted:
             errors.append("FAILURE: @@sqlstatus was not converted.")
        
        if "EXIT END IF;" in converted:
             # This is tricky. simpler check: "EXIT;" vs "EXIT"
             # The current output from user has "THEN EXIT END IF;"
             # We want "THEN EXIT; END IF;"
             if "EXIT END IF;" in converted and "EXIT;" not in converted:
                  errors.append("FAILURE: EXIT missing semicolon.")
        
        if errors:
            print("\nERRORS FOUND:")
            for e in errors:
                print(e)
            sys.exit(1)
        else:
            print("\nSUCCESS: Conversion looks correct.")
            
    except Exception as e:
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    reproduce()
