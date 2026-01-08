import sqlglot
from sqlglot import exp, TokenType
from sqlglot.dialects.tsql import TSQL

# Re-create CustomTSQL classes as in the connector
class Block(exp.Expression):
    arg_types = {"expressions": True}

class CustomTSQL(TSQL):
    class Tokenizer(TSQL.Tokenizer):
        QUOTES = ["'", '"']
        STRING_ESCAPES = ["'"]

    class Parser(TSQL.Parser):
        STATEMENT_PARSERS = TSQL.Parser.STATEMENT_PARSERS.copy()
        config_parser = None

        def _parse_block(self):
            if not self._match(TokenType.BEGIN):
                 pass # Should we raise? For now pass

            expressions = []
            while self._curr and self._curr.token_type != TokenType.END:
                 stmt = self._parse_statement()
                 if stmt:
                      expressions.append(stmt)
                 self._match(TokenType.SEMICOLON)

            self._match(TokenType.END)
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

sql = """
UPDATE t1 SET c=1
IF @@error > 0 BEGIN
   PRINT 'Error'
END
"""





print("TSQL Tokenizer QUOTES:", CustomTSQL.Tokenizer.QUOTES)
print("TSQL Tokenizer ESCAPES:", CustomTSQL.Tokenizer.STRING_ESCAPES) # Check STRING_ESCAPES instead of ESCAPES?
# Wait, Tokenizer might not have STRING_ESCAPES class attribute directly visible if inherited from Dialect or defined in __init__ logic?
# Typically it's STRING_ESCAPES.


variants = [
    ("Original", "PRINT 'Error'\nEND"),
    ("Space", "PRINT 'Error' \nEND"),
    ("Semicolon", "PRINT 'Error';\nEND"),
    ("NextLine", "PRINT 'Error'\n\nEND"),
    ("DoubleQuote", 'PRINT "Error"\nEND')
]

for name, v_sql in variants:
    print(f"\n--- {name} ---")
    tokens = CustomTSQL.Tokenizer().tokenize(v_sql)
    for t in tokens:
        print(t)
