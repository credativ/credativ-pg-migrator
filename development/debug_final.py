
import sys
sys.path.append('.')
from tsql_convertor_gemini import CustomTSQL
from sqlglot import TokenType
import sqlglot

sql = "SET locvar_OptIn = 1 SET locvar_OptOut = 2"

print(f"Testing SQL: {sql}")
tokens = CustomTSQL.Tokenizer().tokenize(sql)
print("Tokens:")
for t in tokens:
    print(f"  {t.token_type} : {t.text}")

print("\nParsing:")
try:
    parsed = sqlglot.parse(sql, read=CustomTSQL)
    for p in parsed:
        print(p.sql())
except Exception as e:
    print(f"Error: {e}")

print("\n--- Testing Custom Methods ---")
# Manually invoke parser
dialect = CustomTSQL()
parser = dialect.parser()
parser._tokens = tokens
parser._advance(0)

# Check parsing
print(f"Curr: {parser._curr.text} Type: {parser._curr.token_type}")
stmt = parser._parse_statement()
print(f"Stmt 1: {stmt}")
print(f"Curr after 1: {parser._curr.text if parser._curr else 'None'}")
stmt2 = parser._parse_statement()
print(f"Stmt 2: {stmt2}")
