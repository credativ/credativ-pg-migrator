import sqlglot
from sqlglot import exp, TokenType
print(f"DEBUG: TokenType members: {[m for m in dir(TokenType) if not m.startswith('_')]}")
from sqlglot.dialects.tsql import TSQL

# Re-create CustomTSQL classes as in the connector
class Block(exp.Expression):
    arg_types = {"expressions": True}

class CustomTSQL(TSQL):
    class Tokenizer(TSQL.Tokenizer):
        QUOTES = ["'", '"']
        STRING_ESCAPES = ["'"]
        
        # Remove PRINT/COMMAND from COMMANDS to prevent greedy parsing
        COMMANDS = TSQL.Tokenizer.COMMANDS - {TokenType.COMMAND}

    class Parser(TSQL.Parser):
        STATEMENT_PARSERS = TSQL.Parser.STATEMENT_PARSERS.copy()
        config_parser = None

        def _parse_command_custom(self):
            # Intercept PRINT to parse expression
            # Note: _parse_statement has already consumed the COMMAND token
            print(f"DEBUG: _parse_command_custom called. Prev: {self._prev}, Curr: {self._curr}")
            if self._prev.text.upper() == 'PRINT':
                 print("DEBUG: Matched PRINT")
                 # Use _parse_conjunction to avoid consuming END (as alias)
                 expr = self._parse_conjunction()
                 print(f"DEBUG: Parsed expression: {expr}")
                 if not expr:
                      print("DEBUG: _parse_expression returned None!")
                 return exp.Command(this='PRINT', expression=expr)
            return self._parse_command()

        STATEMENT_PARSERS[TokenType.COMMAND] = _parse_command_custom

        def _parse_block(self):
            print(f"DEBUG: _parse_block called, curr={self._curr}")
            if not self._match(TokenType.BEGIN):
                 pass

            expressions = []
            print(f"DEBUG: Entering loop. Curr type: {self._curr.token_type}")
            try:
                while True:
                     # Check EOF (None curr)
                     if not self._curr:
                          print("DEBUG: Loop termination: EOF (None curr)")
                          break
                     
                     # Check termination manually
                     if self._curr.token_type == TokenType.END:
                          print("DEBUG: Loop termination: END")
                          break
                     
                     # Check if token text is empty (safe EOF check fallback)
                     if not self._curr.text and self._curr.token_type != TokenType.STRING:
                          print("DEBUG: Loop termination: Empty text (EOF check)")
                          break
                     
                     print(f"DEBUG: Loop. Curr: {self._curr} Type: {self._curr.token_type}")
                     if self._curr.token_type in self.STATEMENT_PARSERS:
                          print(f"DEBUG: Found handler for {self._curr.token_type}")
                     else:
                          print(f"DEBUG: NO handler for {self._curr.token_type}")

                     expr = self._parse_statement()
                     if not expr:
                          print(f"DEBUG: Block loop got None at {self._curr}")
                          # If None, and we are at unknown state, break to avoid infinite loop
                          print("DEBUG: Breaking loop due to None expression.")
                          break
                     expressions.append(expr)
                     self._match(TokenType.SEMICOLON)
            except Exception as e:
                print(f"DEBUG: Exception in loop: {e}")
                import traceback
                traceback.print_exc()
                raise e
            
            if not self._match(TokenType.END):
                 self.raise_error("Expected END")
            return Block(expressions=expressions)

        def _parse_if(self):
            return self.expression(
                 exp.If,
                 this=self._parse_conjunction(),
                 true=self._parse_statement(),
                 false=self._parse_statement() if self._match(TokenType.ELSE) else None,
            )

        if hasattr(TokenType, 'IF'):
             STATEMENT_PARSERS[getattr(TokenType, 'IF')] = lambda self: self._parse_if()
        
        STATEMENT_PARSERS[TokenType.BEGIN] = _parse_block

# Test Variants
variants = [
    ("Original", "PRINT 'Error'\nEND"),
    ("Space", "PRINT 'Error' \nEND"),
    ("Semicolon", "PRINT 'Error';\nEND"),
    ("NextLine", "PRINT 'Error'\n\nEND"),
    ("DoubleQuote", 'PRINT "Error"\nEND'),
    ("Reproduction", """UPDATE t1 SET c=1
IF x > 0 BEGIN
   PRINT 'Error occurred'
END"""),
    ("UpdateOnly", "UPDATE t1 SET c=1"),
    ("UpdateSemi", "UPDATE t1 SET c=1;"),
    ("ReproductionSemi", """UPDATE t1 SET c=1;
IF x > 0 BEGIN
   PRINT 'Error occurred'
END""")
]

print("Keys in STATEMENT_PARSERS: ", list(CustomTSQL.Parser.STATEMENT_PARSERS.keys()))
print(f"DEBUG: TokenType.COMMAND = {TokenType.COMMAND}")
print(f"DEBUG: TokenType.STRING = {TokenType.STRING}")

import inspect
print("Source of Tokenizer._add:")
print(inspect.getsource(CustomTSQL.Tokenizer._add))

for name, v_sql in variants:
    print(f"\n--- {name} ---")
    tokens = CustomTSQL.Tokenizer().tokenize(v_sql)
    for t in tokens:
        print(t)
    
    print("Parsing...")
    try:
        stmts = sqlglot.parse(v_sql, read=CustomTSQL)
        for s in stmts:
            print(s)
    except Exception as e:
        print(f"Parse Error: {e}")
