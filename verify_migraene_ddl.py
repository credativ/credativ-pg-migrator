
import sys
import os
import re
from unittest.mock import MagicMock

# Add repo root to path
repo_root = '/home/josef/github.com/credativ/credativ-pg-migrator'
sys.path.append(repo_root)

# Mock modules
sys.modules['jaydebeapi'] = MagicMock()
sys.modules['pyodbc'] = MagicMock()
sys.modules['tabulate'] = MagicMock()

from credativ_pg_migrator.connectors.sybase_ase_connector import SybaseASEConnector

# Mock config
class MockConfig:
    def get_on_error_action(self): return 'stop'
    def get_log_file(self): return '/dev/null'
    def convert_names_case(self, name): return name.lower()
    def print_log_message(self, level, msg): 
        pass # Silence logs for bulk processing
    def get_total_chunks(self, *args): return 1
    def get_connectivity(self, *args): return 'odbc'
    def get_db_config(self, *args): return {'username': 'test'}
    def get_data_types_substitution(self, *args): return {}
    def get_udt_to_base_type_substitution(self, *args): return {}

mock_config = MockConfig()
connector = SybaseASEConnector(mock_config, 'source')
connector.get_types_mapping = lambda x: {'numeric': 'numeric', 'int': 'integer', 'datetime': 'timestamp'}
connector.get_sql_functions_mapping = lambda x: {} 
connector.get_on_error_action = lambda: 'stop'

def process_ddl_file(filepath):
    print(f"Processing {filepath}...")
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()

    # Split by 'go'
    # ASE DDL uses "go" on a separate line
    batches = re.split(r'^\s*go\s*$', content, flags=re.MULTILINE | re.IGNORECASE)

    objects = []

    for batch in batches:
        batch = batch.strip()
        if not batch: continue

        # Identify type
        obj_type = None
        if re.search(r'create\s+proc', batch, re.IGNORECASE):
            obj_type = 'PROCEDURE'
        elif re.search(r'create\s+func', batch, re.IGNORECASE):
            obj_type = 'FUNCTION'
        elif re.search(r'create\s+trigger', batch, re.IGNORECASE):
            obj_type = 'TRIGGER'

        if obj_type:
             # Extract name
             name_match = re.search(r'create\s+(?:proc|procedure|func|function|trigger)\s+([a-zA-Z0-9_\.]+)', batch, re.IGNORECASE)
             name = name_match.group(1) if name_match else "unknown"
             objects.append({'type': obj_type, 'name': name, 'code': batch})

    print(f"Found {len(objects)} objects to convert.")
    
    success_count = 0
    fail_count = 0

    for obj in objects:
        print(f"Converting {obj['type']} {obj['name']}...")
        try:
            result = ""
            if obj['type'] in ['PROCEDURE', 'FUNCTION']:
                settings = {
                    'funcproc_code': obj['code'],
                    'funcproc_name': obj['name'],
                    'target_db_type': 'postgres',
                    'target_schema': 'public'
                }
                # Use the robust wrapper
                result = connector.convert_funcproc_code(settings)
                
            elif obj['type'] == 'TRIGGER':
                 # Mock trigger settings structure
                 # Trigger conversion needs extracted table name and trigger name
                 # We'll use a simplified call or mock extraction if needed.
                 # Actually convert_trigger_code expects 'trigger_code', 'trigger_name', 'table_name' in settings
                 
                 # Extract table name from "ON table_name"
                 tbl_match = re.search(r'\bON\s+([a-zA-Z0-9_\.]+)', obj['code'], re.IGNORECASE)
                 table_name = tbl_match.group(1) if tbl_match else "unknown_table"
                 
                 settings = {
                     'trigger_sql': obj['code'],
                     'trigger_name': obj['name'],
                     'table_name': table_name,
                     'target_db_type': 'postgres',
                     'target_schema': 'public'
                 }
                 result = connector.convert_trigger(settings)

            # Verification
            if not result or result.strip() == "":
                print(f"  [FAILED] Empty result for {obj['name']}")
                fail_count += 1
                print(f"--- FAILURE DETAIL ---\n{obj['code'][:500]}\n----------------------")
            elif "/* PARSING FAILED:" in result:
                print(f"  [FAILED] Parsing error in {obj['name']}")
                fail_count += 1
                print(f"--- FAILURE DETAIL ---\n{obj['code'][:500]}\n--- RESULT ---\n{result[:500]}\n----------------------")
            elif "/* CRITICAL FAILURE" in result:
                print(f"  [CRITICAL] Exception in {obj['name']}")
                fail_count += 1
                print(f"--- FAILURE DETAIL ---\n{obj['code'][:500]}\n--- RESULT ---\n{result[:500]}\n----------------------")
            else:
                # print(f"  [OK] Success")
                success_count += 1
                if obj['name'] == 'config_recycleDID':
                    pass # Silence debug print for full run
                    # print(f"--- INPUT ---\n{obj['code']}\n--- OUTPUT V2 ---\n{result}\n")

        except Exception as e:
            print(f"  [EXCEPTION] Error converting {obj['name']}: {e}")
            fail_count += 1
            
        if fail_count >= 5:
            print("Stopping after 5 failures for debug.")
            break

    print(f"\nSummary: {success_count} Succeeded, {fail_count} Failed.")

if __name__ == "__main__":
    process_ddl_file('data/ddlgen_MIGRAENE_upd.sql')
