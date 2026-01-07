import sys
import os
import re
from unittest.mock import MagicMock


# Add repo root to path
repo_root = '/home/josef/github.com/credativ/credativ-pg-migrator'
sys.path.append(repo_root)

# Mock modules that might be missing or problematic
sys.modules['jaydebeapi'] = MagicMock()
sys.modules['pyodbc'] = MagicMock()
sys.modules['tabulate'] = MagicMock()


from credativ_pg_migrator.connectors.sybase_ase_connector import SybaseASEConnector

# Mock config parser
class MockConfig:
    def get_on_error_action(self): return 'stop'
    def get_log_file(self): return '/dev/null'
    def convert_names_case(self, name): return name.lower()
    def print_log_message(self, level, msg): 
        if level in ['ERROR', 'WARNING']:
            print(f"[{level}] {msg}")
    def get_total_chunks(self, *args): return 1
    def get_connectivity(self, *args): return 'odbc'
    def get_db_config(self, *args): return {'username': 'test'}
    def get_data_types_substitution(self, *args): return {}
    def get_udt_to_base_type_substitution(self, *args): return {}


mock_config = MockConfig()
# Bypass __init__ if it does too much (like MigratorLogger which opens files)
# But avoiding __init__ is messy. Let's try to let it run.
# We mocked dependencies.

connector = SybaseASEConnector(mock_config, 'source')
# Mock methods used by V1
connector.get_types_mapping = lambda x: {'numeric': 'numeric', 'int': 'integer'}
connector.get_sql_functions_mapping = lambda x: {} 
connector.get_on_error_action = lambda: 'stop'

sybase_sql = """
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

settings = {
    'funcproc_code': sybase_sql,
    'funcproc_name': 'access_update',
    'target_db_type': 'postgres',
    'target_schema': 'public'
}

print("--- Sybase Source ---")
print(sybase_sql)

print("\n--- V1 Output (Regex) ---")
try:
    if hasattr(connector, 'convert_funcproc_code_v1'):
        v1_out = connector.convert_funcproc_code_v1(settings)
        print(v1_out)
    else:
        print("convert_funcproc_code_v1 not found!")
except Exception as e:
    print(f"V1 Error: {e}")

print("\n--- V2 Output (Parser) ---")
try:
    import sqlglot
    from sqlglot import exp
    print(f"Debug: Has Print? {hasattr(exp, 'Print')}")

    # Replicate V2 body extraction
    body_content = sybase_sql
    header_match = re.search(r'CREATE\s+(?:PROC|PROCEDURE)\s+([a-zA-Z0-9_\.]+)(.*?)(\bAS\b)', sybase_sql, flags=re.IGNORECASE | re.DOTALL)
    if header_match:
         body_content = sybase_sql[header_match.end(3):].strip()
    
    if re.match(r'^BEGIN\b', body_content, flags=re.IGNORECASE):
         body_content = re.sub(r'^BEGIN', '', body_content, count=1, flags=re.IGNORECASE).strip()
         body_content = re.sub(r'END\s*$', '', body_content, flags=re.IGNORECASE).strip() # simplified
    
    print("Debug: Parsed Body Content Preview:")
    print(body_content[:200])
    
    parsed_body = sqlglot.parse(body_content, read='tsql')
    print(f"Debug: Body Expressions count: {len(parsed_body)}")
    for i, e in enumerate(parsed_body):
        print(f"Expr {i}: Type={type(e)} SQL={e.sql()[:50]}...")

    # Call wrapper to test fallback
    v2_out = connector.convert_funcproc_code(settings)
    print(v2_out)
except Exception as e:
    print(f"V2 Error: {e}")

