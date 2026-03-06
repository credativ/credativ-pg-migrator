# credativ-pg-migrator
# Copyright (C) 2025 credativ GmbH
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from .database_connector import DatabaseConnector
import psycopg2
import time
import datetime

class IbmDb2ZosConnector(DatabaseConnector):
    def __init__(self, config_parser, is_target=False):
        if is_target:
            raise ValueError("IBM DB2 z/OS is only supported as a source database")

        self.connection = None
        self.connectivity = self.config_parser.get_connectivity(self.source_or_target)
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.on_error_action = self.config_parser.get_on_error_action()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger
        self.source_db_config = self.config_parser.get_source_config()

        if self.connectivity == self.config_parser.const_connectivity_ddl():
            self.ddl_directory = self.source_db_config['ddl']['directory']
            if not os.path.exists(self.ddl_directory):
                raise ValueError(f"DDL directory not found: {self.ddl_directory}")
            else:
                if not os.listdir(self.ddl_directory):
                    raise ValueError(f"DDL directory is empty: {self.ddl_directory}")
                else:
                    self.config_parser.print_log_message('INFO', f"DDL directory found: {self.ddl_directory}")

                if not os.listdir(self.ddl_directory):
                    raise ValueError(f"DDL directory is empty: {self.ddl_directory}")
                else:

                    extension_counts = {}
                    for filename in os.listdir(self.ddl_directory):
                        if os.path.isfile(os.path.join(self.ddl_directory, filename)):
                            ext = os.path.splitext(filename)[1]
                            extension_counts[ext] = extension_counts.get(ext, 0) + 1
                    for ext, count in extension_counts.items():
                        self.config_parser.print_log_message('INFO', f"Found {count} files with extension '{ext}'")

                    self.config_parser.print_log_message('INFO', f"DDL directory found: {ddl_directory}")
        else:
            raise ValueError(f"Unsupported IBM DB2 z/OS connectivity: {self.connectivity}")

    def connect(self):
        self.config_parser.print_log_message('DEBUG', "IbmDb2ZosConnector: connect() called (dummy implementation).")
        pass

    def disconnect(self):
        self.config_parser.print_log_message('DEBUG', "IbmDb2ZosConnector: disconnect() called (dummy implementation).")
        pass

    def fetch_all_tables(self, schema_name: str) -> dict:
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            tables = {}
            order_num = 1
            for filename in os.listdir(self.ddl_directory):
                if os.path.isfile(os.path.join(self.ddl_directory, filename)):
                    ext = os.path.splitext(filename)[1]
                    if ext == '.sql':
                        with open(os.path.join(self.ddl_directory, filename), 'r') as f:
                            tables[order_num] = {
                                'id': order_num,
                                'schema_name': schema_name,
                                'table_name': filename,
                                'comment': f.read()
                            }
                        order_num += 1
            return tables
        return {}

    def fetch_table_columns(self, settings) -> dict:
        self.config_parser.print_log_message('DEBUG', "IbmDb2ZosConnector: fetch_table_columns() called.")
        return {}

    def get_types_mapping(self, settings):
        return self.types_mapping

if __name__ == "__main__":
    print("This script is not meant to be run directly")
