
import sys
import os

# Mock modules
class MockConfig:
    def print_log_message(self, *args):
        print(args)

import sys
from unittest.mock import MagicMock
sys.modules['jaydebeapi'] = MagicMock()
sys.modules['tabulate'] = MagicMock()

from credativ_pg_migrator.connectors.sybase_ase_connector import SybaseASEConnector

def test_crash():
    try:
        # Config needs get_on_error_action
        class MockConfigV2:
             def print_log_message(self, *args): print(args)
             def get_on_error_action(self): return 'continue'
             def get_parameter(self, *args): return {}
             def get_log_file(self): return '/dev/null'
        
        c = SybaseASEConnector(MockConfigV2(), 'source')
        # The crash happens when _convert_stmts defines CustomTSQL class
        c._convert_stmts("IF 1=1 BEGIN END", {})
        print("Success")
    except Exception as e:
        print(f"Crash: {e}")
        import traceback
        traceback.print_exc()

test_crash()
