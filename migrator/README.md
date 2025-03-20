# credativ-pg-migrator

This directory contains the main codebase for the migrator tool. The tool is written in Python in multiple classes and modules. The main entry point is the `main.py` file.

## Run

cd migrator

. ../migrator_venv/bin/activate

echo ""> ../logs/informix_bfr_20250304.log; python3 main.py --config=../config/informix_bfr.yaml --log-file=../logs/informix_bfr_20250303.log --log-level=DEBUG

echo ""> ../logs/informix_iwadb_20250306.log; python3 main.py --config=../config/informix_iwadb.yaml --log-file=../logs/informix_iwadb_20250306.log --log-level=DEBUG

echo "" > ../logs/informix_sinter_20250310.log; python3 main.py --config=../config/informix_sinter.yaml --log-file=../logs/informix_sinter_20250310.log --log-level=DEBUG

## 1und1

echo "" > ../logs/sybase_1und1_ccd_20250320.log; python3 main.py --config=../config/sybase_1und1_ccd.yaml --log-file=../logs/sybase_1und1_ccd_20250320.log --log-level=DEBUG

## Data check

echo ""> ../logs/informix_iwadb_20250306_check.log; python3 data_check.py --config=../config/informix_iwadb.yaml --log-file=../logs/informix_iwadb_20250306_check.log --log-level=DEBUG
