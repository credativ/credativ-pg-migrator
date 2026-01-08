import sqlglot

sql = """
UPDATE t1
SET c = 1
"""
print("--- Test 1: UPDATE split by newline ---")
parsed = sqlglot.parse(sql, read='tsql')
for e in parsed:
    print(e.sql())

sql2 = """
SELECT 1
SET @x = 1
"""
print("\n--- Test 2: SELECT and SET separated by newline (no semicolon) ---")
parsed2 = sqlglot.parse(sql2, read='tsql')
for e in parsed2:
    print(e.sql())

sql3 = """
UPDATE t1 SET c=1
SET @x=2
"""
print("\n--- Test 3: UPDATE and SET separated by newline ---")
parsed3 = sqlglot.parse(sql3, read='tsql')
for e in parsed3:
    print(e.sql())
