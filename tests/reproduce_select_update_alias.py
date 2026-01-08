import sys
import os
sys.path.append(os.getcwd())
from unittest.mock import MagicMock
sys.modules['jaydebeapi'] = MagicMock()
sys.modules['pyodbc'] = MagicMock()
sys.modules['tabulate'] = MagicMock()

import sqlglot
from sqlglot import exp
from credativ_pg_migrator.connectors.sybase_ase_connector import CustomTSQL
from sqlglot import Parser, TokenType

# Custom _parse implementation to handle missing semicolons
def custom_parse(self, parse_method, raw_tokens, sql=None):
    self.reset()
    self.sql = sql or ""
    self._tokens = raw_tokens
    self._advance()

    expressions = []
    while self._curr:
        if self._match(TokenType.SEMICOLON):
             continue

        # If we are at END, we should let _parse_block (if used) handle it?
        # But this is top-level _parse.
        # Top-level TSQL shouldn't have unassociated END?
        # But if we are simulating _parse_block loop...

        stmt = parse_method(self)

        if not stmt:
             if self._curr:
                 # If we failed to parse statement but have tokens...
                 # For robust parsing, we might skip token?
                 # Or raise error better.
                 print(f"DEBUG: Failed to parse statement at {self._curr}")
                 self.raise_error("Unable to parse statement")
             break

        expressions.append(stmt)

    return expressions

CustomTSQL.Parser._parse = custom_parse

sql = """
select @date=getdate()
update job_event_filter_static_notify
set lastchange=@date
"""

print("--- Tokenizing ---")
tokens = CustomTSQL.Tokenizer().tokenize(sql)
for t in tokens:
    print(t)

print("\n--- Parsing ---")
parsed = sqlglot.parse(sql, read=CustomTSQL)
for p in parsed:
    print(f"Statement Type: {type(p)}")
    if type(p).__name__ == 'Block':
        print(f"Block expressions: {len(p.expressions)}")
        for e in p.expressions:
             print(f"  Expr: {e}")
             if isinstance(e, exp.Select):
                 for se in e.expressions:
                      print(f"    Select Expression Alias: {se.alias}")

print("\n--- Parsing with Semicolon ---")
sql_semi = """
declare @date datetime;
select @date=getdate();
update job_event_filter_static_notify
set lastchange=@date
"""
parsed_semi = sqlglot.parse(sql_semi, read=CustomTSQL)
for p in parsed_semi:
    print(f"Statement: {p.sql()}")
