# credativ-pg-migrator

This directory contains the main codebase for the migrator tool. The tool is written in Python in multiple classes and modules. The main entry point is the `main.py` file.

## Run

cd migrator

. ../migrator_venv/bin/activate

echo ""> ../logs/informix_iwadb_20250306.log; python3 main.py --config=../config/informix_iwadb.yaml --log-file=../logs/informix_iwadb_20250306.log --log-level=DEBUG

echo "" > ../logs/informix_sinter_20250310.log; python3 main.py --config=../config/informix_sinter.yaml --log-file=../logs/informix_sinter_20250310.log --log-level=DEBUG

## OMDB

echo "" > ../logs/omdb_20250326.log; python3 main.py --config=../tests/omdb/omdb.yaml --log-level=DEBUG --log-file=../logs/omdb_20250326.log

## Data check

echo ""> ../logs/informix_iwadb_20250306_check.log; python3 data_check.py --config=../config/informix_iwadb.yaml --log-file=../logs/informix_iwadb_20250306_check.log --log-level=DEBUG

## Manual test of code conversion

../migrator_venv/bin/python3 test_code_conversion.py
