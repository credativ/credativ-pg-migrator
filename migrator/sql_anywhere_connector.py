from database_connector import DatabaseConnector
from migrator_logging import MigratorLogger
import pyodbc

class SQLAnywhereConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        if source_or_target != 'source':
            raise ValueError("SQL Anywhere is only supported as a source database")

        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.on_error_action = self.config_parser.get_on_error_action()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger

    def connect(self):
        connection_string = self.config_parser.get_connect_string(self.source_or_target)
        self.connection = pyodbc.connect(connection_string)

    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
        except AttributeError:
            pass

    def fetch_table_names(self, table_schema: str):
        query = f"""
            SELECT table_id, table_name
            FROM sys.systable
            WHERE creator = '{table_schema}'
            AND table_type = 'BASE'
            ORDER BY table_name
        """
        try:
            tables = {}
            order_num = 1
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                tables[order_num] = {
                    'id': row[0],
                    'schema_name': table_schema,
                    'table_name': row[1],
                    'comment': ''
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return tables
        except pyodbc.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_table_columns(self, table_schema: str, table_name: str, migrator_tables) -> dict:
        query = f"""
            SELECT column_id, column_name, domain_name, width, nulls, default
            FROM sys.syscolumn
            WHERE creator = '{table_schema}'
            AND table_name = '{table_name}'
            ORDER BY column_id
        """
        try:
            result = {}
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                result[row[0]] = {
                    'name': row[1],
                    'type': row[2],
                    'length': row[3],
                    'nullable': 'NOT NULL' if row[4] == 'N' else '',
                    'default': row[5],
                    'comment': '',
                    'other': ''
                }
            cursor.close()
            self.disconnect()
            return result
        except pyodbc.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def convert_table_columns(self, settings):
        target_db_type = settings['target_db_type']
        target_schema = settings['target_schema']
        target_table_name = settings['target_table_name']
        source_columns = settings['source_columns']
        # Basic implementation for converting table columns
        type_mapping = {
            'INTEGER': 'INTEGER',
            'VARCHAR': 'TEXT',
            'CHAR': 'TEXT',
            'DATE': 'DATE',
            'TIMESTAMP': 'TIMESTAMP',
            'DECIMAL': 'DECIMAL'
        }
        converted = {}
        create_table_sql_parts = []

        for order_num, column_info in source_columns.items():
            coltype = type_mapping.get(column_info['type'].upper(), 'TEXT')
            length = column_info['length']
            converted[order_num] = {
                'name': column_info['name'],
                'type': coltype,
                'length': length,
                'nullable': column_info['nullable'],
                'default': column_info['default'],
                'comment': column_info['comment'],
                'other': column_info['other']
            }

            if coltype in ('CHAR', 'VARCHAR') and length:
                create_table_sql_parts.append(f"\"{column_info['name']}\" {coltype}({length}) {column_info['nullable']}")
            else:
                create_table_sql_parts.append(f"\"{column_info['name']}\" {coltype} {column_info['nullable']}")

        create_table_sql = f"CREATE TABLE \"{target_schema}\".\"{target_table_name}\" ({', '.join(create_table_sql_parts)})"
        return converted, create_table_sql

    def migrate_table(self, migrate_target_connection, settings):
        raise NotImplementedError("Table migration is not yet implemented for SQL Anywhere")

    def fetch_indexes(self, settings):
        source_table_id = settings['source_table_id']
        source_schema = settings['source_schema']
        source_table_name = settings['source_table_name']
        target_schema = settings['target_schema']
        target_table_name = settings['target_table_name']
        target_columns = settings['target_columns']
        raise NotImplementedError("Fetching indexes is not yet implemented for SQL Anywhere")

    def fetch_constraints(self, settings):
        source_table_id = settings['source_table_id']
        source_schema = settings['source_schema']
        source_table_name = settings['source_table_name']
        target_schema = settings['target_schema']
        target_table_name = settings['target_table_name']
        raise NotImplementedError("Fetching constraints is not yet implemented for SQL Anywhere")

    def fetch_triggers(self, table_id: int, table_schema: str, table_name: str):
        raise NotImplementedError("Fetching triggers is not yet implemented for SQL Anywhere")

    def convert_trigger(self, trig: str, settings: dict):
        raise NotImplementedError("Trigger conversion is not yet implemented for SQL Anywhere")

    def fetch_funcproc_names(self, schema: str):
        raise NotImplementedError("Fetching function/procedure names is not yet implemented for SQL Anywhere")

    def fetch_funcproc_code(self, funcproc_id: int):
        raise NotImplementedError("Fetching function/procedure code is not yet implemented for SQL Anywhere")

    def convert_funcproc_code(self, funcproc_code: str, target_db_type: str, source_schema: str, target_schema: str, table_list: list):
        raise NotImplementedError("Function/procedure conversion is not yet implemented for SQL Anywhere")

    def fetch_sequences(self, table_schema: str, table_name: str):
        raise NotImplementedError("Fetching sequences is not yet implemented for SQL Anywhere")

    def fetch_views_names(self, source_schema: str):
        raise NotImplementedError("Fetching view names is not yet implemented for SQL Anywhere")

    def fetch_view_code(self, settings):
        view_id = settings['view_id']
        source_schema = settings['source_schema']
        source_view_name = settings['source_view_name']
        target_schema = settings['target_schema']
        target_view_name = settings['target_view_name']
        raise NotImplementedError("Fetching view code is not yet implemented for SQL Anywhere")

    def convert_view_code(self, view_code: str, settings: dict):
        raise NotImplementedError("View conversion is not yet implemented for SQL Anywhere")

    def get_sequence_current_value(self, sequence_id: int):
        raise NotImplementedError("Fetching sequence current value is not yet implemented for SQL Anywhere")

    def execute_query(self, query: str, params=None):
        cursor = self.connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        cursor.close()

    def execute_sql_script(self, script_path: str):
        with open(script_path, 'r') as file:
            script = file.read()
        cursor = self.connection.cursor()
        cursor.execute(script)
        cursor.close()

    def begin_transaction(self):
        self.connection.autocommit = False

    def commit_transaction(self):
        self.connection.commit()
        self.connection.autocommit = True

    def rollback_transaction(self):
        self.connection.rollback()

    def get_rows_count(self, table_schema: str, table_name: str):
        query = f"SELECT COUNT(*) FROM \"{table_schema}\".\"{table_name}\""
        cursor = self.connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_table_size(self, table_schema: str, table_name: str):
        raise NotImplementedError("Fetching table size is not yet implemented for SQL Anywhere")

    def fetch_user_defined_types(self, schema: str):
        raise NotImplementedError("Fetching user-defined types is not yet implemented for SQL Anywhere")

    def testing_select(self):
        return "SELECT 1"
