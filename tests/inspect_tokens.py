import sys
import os
sys.path.append(os.getcwd())
from unittest.mock import MagicMock
sys.modules['jaydebeapi'] = MagicMock()
sys.modules['pyodbc'] = MagicMock()
sys.modules['tabulate'] = MagicMock()

from credativ_pg_migrator.connectors.sybase_ase_connector import CustomTSQL
from sqlglot import TokenType

from sqlglot.dialects import TSQL
print(f"TSQL COMMANDS: {TSQL.Tokenizer.COMMANDS}")
print(f"TokenType.UPDATE in COMMANDS? {TokenType.UPDATE in TSQL.Tokenizer.COMMANDS}")
import sqlglot
from sqlglot import exp

print("\n--- Parsing SELECT 1 UPDATE ---")
try:
    s = sqlglot.parse_one("SELECT 1 UPDATE", read=CustomTSQL)
    print(f"Parsed: {s.sql()}")
    print(f"Expressions: {s.expressions}")
    for e in s.expressions:
         print(f"Alias: {e.alias}")
except Exception as e:
    print(e)
