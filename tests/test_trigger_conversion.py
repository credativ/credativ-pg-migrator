import logging
import sys
import sqlglot
from sqlglot import exp, TokenType
from sqlglot.dialects import TSQL
from sqlglot.dialects.postgres import Postgres

# Define Custom Block Expression
class Block(exp.Expression):
    arg_types = {"expressions": True}

# Register Block with Postgres Generator
def block_handler(self, expression):
    return "\n".join(self.sql(e) for e in expression.expressions)
Postgres.Generator.TRANSFORMS[Block] = block_handler

class CustomTSQL(TSQL):
    class Tokenizer(TSQL.Tokenizer):
        COMMANDS = TSQL.Tokenizer.COMMANDS - {TokenType.COMMAND, TokenType.SET}

    class Parser(TSQL.Parser):
        config_parser = None

        def _parse_alias(self, this, explicit=False):
             # FIX: Explicitly prevent UPDATE/INSERT/DELETE/MERGE/SET from being aliases
             if self._curr.token_type in (TokenType.UPDATE, TokenType.INSERT, TokenType.DELETE, TokenType.MERGE, TokenType.SET):
                  return this
             return super()._parse_alias(this, explicit)

        def _parse_command_custom(self):
            prev_is_print = self._prev.text.upper() == 'PRINT' if self._prev else False
            curr_is_print = self._curr.text.upper() == 'PRINT' if self._curr else False

            if curr_is_print:
                 self._advance()
            elif not prev_is_print:
                 if self._prev.text.upper() == 'SET':
                      expressions = []
                      balance = 0
                      while self._curr:
                           if self._curr.token_type in (TokenType.SEMICOLON, TokenType.END):
                                break
                           if balance == 0:
                                txt = self._curr.text.upper()
                                if txt in ('SELECT', 'UPDATE', 'INSERT', 'DELETE', 'BEGIN', 'IF', 'WHILE', 'RETURN', 'DECLARE', 'CREATE', 'TRUNCATE', 'GO'):
                                     break
                           if self._curr.token_type == TokenType.L_PAREN:
                                balance += 1
                           elif self._curr.token_type == TokenType.R_PAREN:
                                balance -= 1
                           expressions.append(self._curr.text)
                           self._advance()
                      return exp.Command(this='SET', expression=exp.Literal.string(" ".join(expressions)))
                 return self._parse_command()
            return exp.Command(this='PRINT', expression=self._parse_conjunction())
        
        STATEMENT_PARSERS = TSQL.Parser.STATEMENT_PARSERS.copy()
        STATEMENT_PARSERS[TokenType.COMMAND] = _parse_command_custom
        STATEMENT_PARSERS[TokenType.SET] = _parse_command_custom

        def _parse_block(self):
            if not self._match(TokenType.BEGIN): pass
            expressions = []
            while self._curr and self._curr.token_type != TokenType.END:
                 stmt = self._parse_statement()
                 if stmt: expressions.append(stmt)
                 self._match(TokenType.SEMICOLON)
            self._match(TokenType.END)
            return Block(expressions=expressions)

        def _parse_if(self):
            return self.expression(exp.If, this=self._parse_conjunction(), true=self._parse_statement(), false=self._parse_statement() if self._match(TokenType.ELSE) else None)

        STATEMENT_PARSERS[TokenType.BEGIN] = lambda self: self._parse_block()
        if hasattr(TokenType, 'IF'): STATEMENT_PARSERS[getattr(TokenType, 'IF')] = lambda self: self._parse_if()

        def _parse(self, parse_method, raw_tokens, sql=None):
            self.reset()
            self.sql = sql or ""
            self._tokens = raw_tokens
            self._index = -1
            self._advance()
            expressions = []
            while self._curr:
                if self._match(TokenType.SEMICOLON): continue
                stmt = self._parse_statement()
                if not stmt and self._curr.token_type == TokenType.EOF: break
                if not stmt and self._curr: self._advance(); continue
                if stmt: expressions.append(stmt)
            return expressions

print("--- Testing Trigger Conversion Parsing ---")

import re

sql = """
BEGIN
  DECLARE @date datetime
  DECLARE @changedby univarchar(40)
  SELECT @date=getdate()
  SELECT @changedby=changedby FROM inserted
  
  IF UPDATE(changedby) AND @changedby IS NOT NULL
  BEGIN
    UPDATE ums_account
      SET lastchange=@date,
          changedby=inserted.changedby+' '+deleted.changedby
      FROM ums_account, inserted, deleted
      WHERE ums_account.ums_id = inserted.ums_id AND inserted.ums_id *= deleted.ums_id
    RETURN
  END
  ELSE
  BEGIN
    UPDATE ums_account
      SET lastchange=@date,
          changedby='['+suser_name()+'@'+host_name()+'] '+inserted.changedby
      FROM ums_account, inserted
      WHERE ums_account.ums_id = inserted.ums_id
  END
END
"""


# Simulate Pre-processing from convert_trigger_v2
def if_update_replacer(match):
    col = match.group(1)
    return f"locvar_sybase_update_func({col})"
sql_preprocessed = re.sub(r'\bUPDATE\(([a-zA-Z0-9_]+)\)', if_update_replacer, sql, flags=re.IGNORECASE)

# Simulate Sybase Join Pre-processing (MISSING IN ORIGINAL convert_trigger_v2 but REQUIRED)
sql_preprocessed = re.sub(r'\*=', '= /* left_outer */', sql_preprocessed)
sql_preprocessed = re.sub(r'=\*', '= /* right_outer */', sql_preprocessed)

print(f"Pre-processed SQL:\n{sql_preprocessed}")

try:
    print("Attempting to parse...")
    parsed = sqlglot.parse(sql_preprocessed, read=CustomTSQL)
    print("Parsing successful.")
    
    print("\n--- Parsed Expressions & Generation ---")
    
    # helper for converting and checking for semicolon usage roughly
    def debug_gen(expr):
        # transform triggers logic usually applies here too, but we just check generation
        # NOTE: transform_sybase_joins logic is complex and part of connector, skipped here for basic parse check
        sql_out = expr.sql(dialect='postgres')
        print(f"[{type(expr).__name__}]: {sql_out!r}")
        return sql_out

    final_stmts = []
    for expr in parsed:
        final_stmts.append(debug_gen(expr))
    
    print("\n--- Final Output Post-Processing ---")
    final_body = "\n".join(final_stmts)
    
    def update_func_replacer(match):
         content = match.group(1)
         return f"NEW.{content} IS DISTINCT FROM OLD.{content}"

    final_body = re.sub(r'locvar_sybase_update_func\((.*?)\)', update_func_replacer, final_body, flags=re.IGNORECASE)
    print(final_body)

except Exception as e:
    import traceback
    traceback.print_exc()
    print(f"FAILURE: {e}")
