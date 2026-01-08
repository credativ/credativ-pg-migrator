import sys
import os
from unittest.mock import MagicMock
import sqlglot
from sqlglot import exp

# Mock modules
sys.modules['jaydebeapi'] = MagicMock()
sys.modules['tabulate'] = MagicMock()

# Adjust path
sys.path.append(os.getcwd())

from credativ_pg_migrator.connectors.sybase_ase_connector import SybaseASEConnector

# Mock dependencies
mock_config = MagicMock()
mock_config.get_on_error_action.return_value = 'stop'
mock_config.get_log_file.return_value = 'test.log'

connector = SybaseASEConnector(mock_config, 'source')

sql_body = """
IF 1=1
 BEGIN
  if ((select count(*) from deleted) > 0)
    begin
        select locvar_bz_id = 20001
        update cgi_php set lastchange=locvar_date
    end
  else
    begin
        if (select count(*) from inserted) > 0
          begin
            select locvar_bz_id = 10000
            update cgi_php set lastchange=locvar_date
          end
     end
 END
ELSE
 BEGIN
  if ((select count(*) from deleted) > 0)
    begin
        select locvar_bz_id = 20001
    end
 END
"""

print("\n--- CONVERSION TEST (Via _convert_stmts) ---")
try:
    results = connector._convert_stmts(sql_body, {}, is_trigger=True)
    for r in results:
        print(f"Output: {r}")
except Exception as e:
    import traceback
    traceback.print_exc()
    print(f"Conversion Error: {e}")

from sqlglot.dialects import TSQL
from sqlglot import TokenType

print(f"Has IF stmt parser? {TokenType.IF in TSQL.Parser.STATEMENT_PARSERS}")
print(f"Has BEGIN stmt parser? {TokenType.BEGIN in TSQL.Parser.STATEMENT_PARSERS}")

class CustomTSQL(TSQL):
    class Parser(TSQL.Parser):
        STATEMENT_PARSERS = TSQL.Parser.STATEMENT_PARSERS.copy()
        STATEMENT_PARSERS[TokenType.BEGIN] = lambda self: self._parse_block()
        STATEMENT_PARSERS[TokenType.IF] = lambda self: self._parse_if()

print("--- AST INSPECTION (CustomTSQL) ---")
parsed_list = sqlglot.parse(sql_body, read=CustomTSQL, error_level='ignore')
for i, parsed in enumerate(parsed_list):
    print(f"\nStatement {i}: {type(parsed)}")
    print(f"SQL: {parsed.sql()}")

    if isinstance(parsed, exp.If):
        print(f"  True Body Type: {type(parsed.args.get('true'))}")
        print("  True Body:", parsed.args.get('true'))

print("\n--- CONVERSION TEST (Simulated Custom) ---")
for i, expression in enumerate(parsed_list):
     print(f"Output: {expression.sql(dialect='postgres')}")
