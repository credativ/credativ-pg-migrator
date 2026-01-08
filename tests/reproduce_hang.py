
import re
import time
import sys
from unittest.mock import MagicMock

# Mock Logger
class MockConfig:
    def print_log_message(self, lvl, msg):
        print(f"{lvl}: {msg}")
    def get_data_types_substitution(self): return []
    def get_on_error_action(self): return 'continue'
    def get_log_file(self): return '/dev/null'

sys.modules['jaydebeapi'] = MagicMock()
sys.modules['tabulate'] = MagicMock()
sys.modules['pyodbc'] = MagicMock()

from credativ_pg_migrator.connectors.sybase_ase_connector import SybaseASEConnector

def test_perf():
    # Setup Connector
    # We patch _get_udt_codes_mapping directly
    c = SybaseASEConnector(MockConfig(), 'source')
    
    # Generate 1000 UDTs
    udts = {f'TypUser{i}': 'VARCHAR(100)' for i in range(1000)}
    c._get_udt_codes_mapping = MagicMock(return_value=udts)

    # Generate huge text (1MB)
    text = "DECLARE @v1 TypUser1; \n" * 10000
    
    print("Starting process...")
    start = time.time()
    c._apply_udt_to_base_type_substitutions(text, {})
    end = time.time()
    print(f"Finished in {end - start:.2f} seconds")

test_perf()
