
import re

text = """
 create proc access_update
as
BEGIN

declare @count int,
        @access_id numeric(10,0)

declare access_cursor cursor
for select access_id from access where erlaubte_einwahl & 8 != 0
for update
open access_cursor

select @count = 0

while (1=1)
"""

# Strip BEGIN as per my code
text = text.replace("BEGIN", "")

print(f"Text len: {len(text)}")
print(text)

regex = r'DECLARE\s+[a-zA-Z0-9_]+\s+CURSOR\s+FOR\s+.*?(?=\b(?:OPEN|FETCH|BEGIN|END|DECLARE|CLOSE|DEALLOCATE)\b)'

print(f"Regex: {regex}")

def cursor_replacer(match):
     print(f"Replacer matched: {match.group(0)}")
     return "REPLACED"

new_text = re.sub(regex, cursor_replacer, text, flags=re.IGNORECASE | re.DOTALL)

print("Original Text len:", len(text))
print("New Text len:", len(new_text))
if "REPLACED" in new_text:
    print("SUBSTITUTION SUCCESS")
    print(new_text)
else:
    print("SUBSTITUTION FAILED")
