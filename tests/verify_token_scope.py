
try:
    from sqlglot import TokenType
    print(f"Global TokenType.IF: {TokenType.IF}")
except Exception as e:
    print(f"Global TokenType.IF Error: {e}")

def test_scope():
    from sqlglot import TokenType
    try:
        print(f"Local TokenType.IF: {TokenType.IF}")
    except Exception as e:
        print(f"Local TokenType.IF Error: {e}")

test_scope()
