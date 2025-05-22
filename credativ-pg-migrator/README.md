# credativ-pg-migrator

This directory contains the main codebase for the migrator tool. The tool is written in Python in multiple classes and modules. The main entry point is the `main.py` file.
For practical usage and examples refer to the [tests](../tests/) directory. There you can find working examples of the migration process for different databases.

## Run

```
. ../migrator_venv/bin/activate
echo ""> ../logs/test_database_20250306.log; python3 main.py --config=../config/test_database.yaml --log-file=../logs/test_database_20250306.log --log-level=DEBUG
```

For repeated tests recreate target PostgreSQL database and empty the log file.

## Build

../migrator_venv/bin/pyinstaller --onefile --name credativ-pg-migrator main.py

## Manual test of code conversion

../migrator_venv/bin/python3 test_code_conversion.py
