from database_connector import DatabaseConnector
from migrator_logging import MigratorLogger
import mysql.connector

class MySQLConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        if source_or_target not in ['source', 'target']:
            raise ValueError("MySQL/MariaDB must be either source or target database")

        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.on_error_action = self.config_parser.get_on_error_action()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger

    def connect(self):
        db_config = self.config_parser.get_db_config(self.source_or_target)
        self.connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['username'],
            password=db_config['password'],
            database=db_config['database']
        )

    def disconnect(self):
        if self.connection:
            self.connection.close()

    def fetch_table_names(self, table_schema: str):
        query = f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{table_schema}'
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            tables = {i + 1: {'id': None, 'schema_name': table_schema, 'table_name': row[0], 'comment': ''} for i, row in enumerate(cursor.fetchall())}
            cursor.close()
            self.disconnect()
            return tables
        except mysql.connector.Error as e:
            self.logger.error(f"Error fetching table names: {e}")
            raise

    def fetch_table_columns(self, table_schema: str, table_name: str, migrator_tables) -> dict:
        query = f"""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{table_schema}' AND TABLE_NAME = '{table_name}'
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            columns = {
                i + 1: {
                    'name': row[0],
                    'type': row[1],
                    'length': row[2],
                    'nullable': 'NOT NULL' if row[3] == 'NO' else '',
                    'default': row[4],
                    'comment': '',
                    'other': ''
                } for i, row in enumerate(cursor.fetchall())
            }
            cursor.close()
            self.disconnect()
            return columns
        except mysql.connector.Error as e:
            self.logger.error(f"Error fetching table columns: {e}")
            raise

    def convert_table_columns(self, target_db_type: str, table_schema: str, table_name: str, columns: dict):
        # Implement conversion logic based on target_db_type
        pass

    def migrate_table(self, migrate_target_connection, settings):
        # Implement table migration logic
        pass

    def fetch_indexes(self, source_table_id: int, target_schema, target_table_name):
        # Implement index fetching logic
        pass

    def fetch_constraints(self, source_table_id: int, target_schema, target_table_name):
        # Implement constraint fetching logic
        pass

    def fetch_triggers(self, table_id: int, table_schema: str, table_name: str):
        # Implement trigger fetching logic
        pass

    def convert_trigger(self, trig: str, settings: dict):
        # Implement trigger conversion logic
        pass

    def fetch_funcproc_names(self, schema: str):
        # Implement function/procedure name fetching logic
        pass

    def fetch_funcproc_code(self, funcproc_id: int):
        # Implement function/procedure code fetching logic
        pass

    def convert_funcproc_code(self, funcproc_code: str, target_db_type: str, source_schema: str, target_schema: str, table_list: list):
        # Implement function/procedure code conversion logic
        pass

    def fetch_sequences(self, table_schema: str, table_name: str):
        # Implement sequence fetching logic
        pass

    def fetch_views_names(self, source_schema: str):
        # Implement view name fetching logic
        pass

    def fetch_view_code(self, view_id: int):
        # Implement view code fetching logic
        pass

    def convert_view_code(self, view_code: str, settings: dict):
        # Implement view code conversion logic
        pass

    def get_sequence_current_value(self, sequence_id: int):
        # Implement sequence current value fetching logic
        pass

    def execute_query(self, query: str, params=None):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            cursor.close()
        except mysql.connector.Error as e:
            self.logger.error(f"Error executing query: {e}")
            raise

    def execute_sql_script(self, script_path: str):
        try:
            with open(script_path, 'r') as file:
                script = file.read()
            cursor = self.connection.cursor()
            for statement in script.split(';'):
                if statement.strip():
                    cursor.execute(statement)
            cursor.close()
        except mysql.connector.Error as e:
            self.logger.error(f"Error executing SQL script: {e}")
            raise

    def begin_transaction(self):
        self.connection.start_transaction()

    def commit_transaction(self):
        self.connection.commit()

    def rollback_transaction(self):
        self.connection.rollback()

    def get_rows_count(self, table_schema: str, table_name: str):
        query = f"SELECT COUNT(*) FROM {table_schema}.{table_name}"
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except mysql.connector.Error as e:
            self.logger.error(f"Error fetching row count: {e}")
            raise

    def get_table_size(self, table_schema: str, table_name: str):
        query = f"""
            SELECT DATA_LENGTH + INDEX_LENGTH
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{table_schema}' AND TABLE_NAME = '{table_name}'
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            size = cursor.fetchone()[0]
            cursor.close()
            return size
        except mysql.connector.Error as e:
            self.logger.error(f"Error fetching table size: {e}")
            raise

    def fetch_user_defined_types(self, schema: str):
        # Implement user-defined type fetching logic
        pass