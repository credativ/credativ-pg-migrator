import sys
import os
sys.path.append(os.getcwd())
import inspect
from sqlglot.dialects import TSQL
from sqlglot import Parser

try:
    print("Source of Parser._parse_alias:")
    print(inspect.getsource(Parser._parse_alias))
except Exception as e:
    print(f"Could not get source: {e}")

try:
    print("Source of Parser._parse:")
    print(inspect.getsource(Parser._parse))
except Exception as e:
    print(f"Count not get source: {e}")
