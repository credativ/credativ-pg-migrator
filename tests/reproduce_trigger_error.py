import sys
import os
import re
import sqlglot
from unittest.mock import MagicMock

# Mock Modules
sys.modules['jaydebeapi'] = MagicMock()
sys.modules['tabulate'] = MagicMock()
sys.modules['credativ_pg_migrator.migrator_logging'] = MagicMock()

# Mock Parent Class via Module
db_conn_mod = MagicMock()
class MockDBConnector:
    def __init__(self, config_parser, connection_settings=None):
        self.config_parser = config_parser
        self.connection = None
    def connect(self): pass
    def get_types_mapping(self, settings): return {'int': 'integer', 'varchar': 'varchar', 'datetime': 'timestamp'}
    def _apply_data_type_substitutions(self, text): return text
    def _apply_udt_to_base_type_substitutions(self, text, settings): return text
    def get_sql_functions_mapping(self, settings): return {}
    def _rename_sybase_local_variables(self, code): return code

db_conn_mod.DatabaseConnector = MockDBConnector
sys.modules['credativ_pg_migrator.database_connector'] = db_conn_mod

sys.path.append('/home/josef/github.com/credativ/credativ-pg-migrator')

from credativ_pg_migrator.connectors.sybase_ase_connector import SybaseASEConnector

# Instantiate
connector = SybaseASEConnector(config_parser=MagicMock(), source_or_target='source')

# Test 4: Global Variables
trigger_sql_globals = """
UPDATE t1 SET c=1
IF @@error > 0 BEGIN
   PRINT 'Error occurred'
END
IF @@rowcount = 0 BEGIN
   PRINT 'No rows'
END
IF @@trancount > 0 BEGIN
   PRINT 'Inside txn'
END
"""
print("\nRunning Global Variables Conversion...")
settings = {
    'trigger_sql': trigger_sql_globals,
    'trigger_name': 'trg_glob',
    'target_schema': 'public',
    'target_table': 't1',
    'target_db_type': 'postgresql'
}

try:
    res = connector.convert_trigger_v2(settings)
    print("Globals Result:")
    print(res)
except Exception as e:
    print(f"FAILED Globals: {e}")
    import traceback
    traceback.print_exc()
