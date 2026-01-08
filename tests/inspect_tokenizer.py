from sqlglot.tokens import Tokenizer
import inspect

t = Tokenizer()
print("Attributes of Tokenizer instance:")
print(dir(t))

print("\nMethod signature of _scan_string:")
if hasattr(t, '_scan_string'):
    print(inspect.signature(t._scan_string))
else:
    print("Method _scan_string not found")

# Initialize and see internal state
t.tokenize("SELECT 1")
print("\nState after tokenize:")
print(f"Current Index: {t._current}")
print(f"Size: {t.size}")
print(f"Text len: {len(t._text)}")
print(f"Attr sql: {t.sql}")
if hasattr(t, '_chars'):
     print(f"Attr _chars len: {len(t._chars)}")
