import re
text = "RAISE NOTICE 'The Replication Server command should not contain the keyword ''rs_rcl'''"
# Is there a pass that replaces '' with ', '?
# Let's check where the comma might be inserted.
