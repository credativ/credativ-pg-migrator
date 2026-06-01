import re
print_args = '"The Replication Server command should not contain the keyword \'rs_rcl\'"'
match = re.match(r'^((?:\'[^\']*\')|(?:"[^"]*"))(.*)$', print_args, re.IGNORECASE)
print(match.group(1))
print(match.group(2))
