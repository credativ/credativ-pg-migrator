# Test Instructions

## Test conversion of UNL to CSV

Some corner cases might require separate tests and manual inspection of the output CSV file. Extract problematic rows from the UNL file and create a small UNL file for testing.
Table-info file shall contain information about the table structure, you can find it in the log file in the row `[DEBUG] DEBUG3: update_table_status: Returned row:` for that particular table. This structure is python dictionary serialized using `str()`, you can copy-paste it to a file without any changes.

Example of usage:
python3 test_conversion.py --config ../data/test_config.yaml --unl-file ../data/table_data.unl --csv-file ../data/output.csv --table-info ../data/table_info.dict
