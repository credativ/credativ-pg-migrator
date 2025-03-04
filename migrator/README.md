# credativ-pg-migrator

This directory contains the main codebase for the migrator tool. The tool is written in Python in multiple classes and modules. The main entry point is the `main.py` file.

## Run

cd migrator

. ../migrator_venv/bin/activate

echo ""> ../logs/informix_bfr_20250304.log; python3 main.py --config=../config/informix_bfr.yaml --log-file=../logs/informix_bfr_20250303.log --log-level=DEBUG
