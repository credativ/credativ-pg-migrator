# credativ-pg-migrator: UNL to CSV conversion test script
# Copyright (C) 2025 credativ GmbH
#
# This script allows you to test the convert_unl_to_csv method from ConfigParser.
# Usage: python main_tests.py --unl-file <input.unl> --csv-file <output.csv> --dummy-config <dummy_config.yaml> --table-json <table_info.json>

# import sys
import argparse
import ast
import os
from credativ_pg_migrator.config_parser import ConfigParser
from credativ_pg_migrator.migrator_logging import MigratorLogger

def main():
	parser = argparse.ArgumentParser(description="Test convert_unl_to_csv from ConfigParser.")
	parser.add_argument('--unl-file', required=True, help='Path to the input UNL file')
	parser.add_argument('--csv-file', required=True, help='Path to the output CSV file')
	parser.add_argument('--config', required=True, help='Path to the dummy config file (YAML or other, for ConfigParser)')
	parser.add_argument('--table-info', required=True, help='Path to the file with table/columns info as Python dict string')
	args = parser.parse_args()

	# Read table and columns info from file containing Python dict as string
	with open(args.table_info, 'r', encoding='utf-8') as f:
		config_str = f.read()
		config_json = ast.literal_eval(config_str)

	source_table = config_json.get('source_table', 'test_table')
	source_columns_dict = config_json.get('source_columns', {})
	target_columns_dict = config_json.get('target_columns', {})

	# If columns are lists, convert them to dicts with string keys for compatibility
	if isinstance(source_columns_dict, list):
		source_columns_dict = {str(i): v for i, v in enumerate(source_columns_dict)}
	if isinstance(target_columns_dict, list):
		target_columns_dict = {str(i): v for i, v in enumerate(target_columns_dict)}

	# Convert columns dicts to lists of column names (ordered by key if possible)
	def columns_dict_to_list(cols):
		try:
			return [cols[k]['column_name'] for k in sorted(cols, key=lambda x: int(x))]
		except Exception:
			return [v['column_name'] for v in cols.values()]

	source_columns = columns_dict_to_list(source_columns_dict)
	target_columns = columns_dict_to_list(target_columns_dict)

	# Minimal logger for ConfigParser
	LOG_FILE = 'test_convert_unl_to_csv.log'
	if os.path.exists(LOG_FILE):
		os.remove(LOG_FILE)

	logger = MigratorLogger(LOG_FILE)
	# Dummy args for ConfigParser
	class DummyArgs:
		def __init__(self, config):
			self.config = config
			self.dry_run = False
			self.resume = False
			self.drop_unfinished_tables = False
			self.log_file = LOG_FILE
			self.log_level = 'DEBUG3'
	dummy_args = DummyArgs(args.config)

	config_parser = ConfigParser(dummy_args, logger.logger)

	data_source_settings = {
		'file_name': args.unl_file,
		'converted_file_name': args.csv_file,
		'source_table': source_table,
		'format': 'UNL',
		'format_options': {
			'null': '\\N',
			'delimiter': '|',
			'header': False
		}
	}

	print(f"Converting {args.unl_file} to {args.csv_file} ...")
	config_parser.convert_unl_to_csv(data_source_settings, source_columns_dict, target_columns_dict)
	print("Conversion done.")

if __name__ == "__main__":
	main()
