import sqlglot
from sqlglot import exp, TokenType
from sqlglot.dialects import TSQL
from sqlglot.dialects.postgres import Postgres
import logging
import sys

# Define Custom Block Expression
class Block(exp.Expression):
    arg_types = {"expressions": True}

# Register Block with Postgres Generator to allow fallback generation
def block_handler(self, expression):
    return "\n".join(self.sql(e) for e in expression.expressions)
Postgres.Generator.TRANSFORMS[Block] = block_handler

class CustomTSQL(TSQL):
    class Tokenizer(TSQL.Tokenizer):
        COMMANDS = TSQL.Tokenizer.COMMANDS - {TokenType.COMMAND, TokenType.SET}

    class Parser(TSQL.Parser):
        # SIMULATE BUG: Allow UPDATE to be alias
        ID_VAR_TOKENS = TSQL.Parser.ID_VAR_TOKENS | {TokenType.UPDATE}
        
        # FIX: Prevent UPDATE from being alias
        def _parse_alias(self, this):
            if self._curr.token_type in (TokenType.UPDATE, TokenType.INSERT, TokenType.DELETE, TokenType.MERGE, TokenType.SET):
                 return this
            return super()._parse_alias(this)

        config_parser = None

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

sql = """
SELECT @date=getdate()
UPDATE domainmatrix
SET lastchange=@date
"""

print("--- Parsing SQL ---")
print(sql)
print("-------------------")

try:
    parsed = sqlglot.parse(sql, read=CustomTSQL)
    for expression in parsed:
        print(f"Expression Type: {type(expression)}")
        print(f"SQL: {expression.sql()}")
except Exception as e:
    import traceback
    traceback.print_exc()
    print(f"Parse Failed: {e}")
