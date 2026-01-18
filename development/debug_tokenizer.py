
import sqlglot
from sqlglot import exp, TokenType
from sqlglot.dialects import TSQL

class CustomTSQL(TSQL):
    class Tokenizer(TSQL.Tokenizer):
        COMMANDS = TSQL.Tokenizer.COMMANDS - {TokenType.COMMAND, TokenType.SET}

def test(text):
    tokens = CustomTSQL.Tokenizer().tokenize(text)
    print(f"Text: {repr(text)}")
    for t in tokens:
        print(f"  {t.token_type} : {t.text}")

print("--- Space ---")
test("SET x = null if(1=1)")

print("\n--- Newline ---")
test("SET x = null\nif(1=1)")
