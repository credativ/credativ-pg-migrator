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
        self.config_parser = config_parser
        self.config_parser.print_log_message('DEBUG', "IbmDb2ZosConnector initialized.")
        self.types_mapping = {
            # Dummy mapping to be implemented
        }

    def connect(self):
        self.config_parser.print_log_message('DEBUG', "IbmDb2ZosConnector: connect() called (dummy implementation).")
        pass

    def disconnect(self):
        self.config_parser.print_log_message('DEBUG', "IbmDb2ZosConnector: disconnect() called (dummy implementation).")
        pass

    def fetch_all_tables(self, schema_name: str) -> dict:
        self.config_parser.print_log_message('DEBUG', f"IbmDb2ZosConnector: fetch_all_tables() called for schema {schema_name}.")
        return {}

    def fetch_table_columns(self, settings) -> dict:
        self.config_parser.print_log_message('DEBUG', "IbmDb2ZosConnector: fetch_table_columns() called.")
        return {}

    def get_types_mapping(self, settings):
        return self.types_mapping

if __name__ == "__main__":
    print("This script is not meant to be run directly")
