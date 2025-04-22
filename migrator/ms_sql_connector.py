import jaydebeapi
from jaydebeapi import Error
import pyodbc
from pyodbc import Error
from database_connector import DatabaseConnector
from migrator_logging import MigratorLogger
import re
import traceback

class MsSQLConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        if source_or_target not in ['source']:
            raise ValueError(f"MS SQL Server is only supported as a source database. Current value: {source_or_target}")

        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.on_error_action = self.config_parser.get_on_error_action()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger

    def connect(self):
        if self.config_parser.get_connectivity(self.source_or_target) == 'odbc':
            connection_string = self.config_parser.get_connect_string(self.source_or_target)
            self.connection = pyodbc.connect(connection_string)
        elif self.config_parser.get_connectivity(self.source_or_target) == 'jdbc':
            connection_string = self.config_parser.get_connect_string(self.source_or_target)
            username = self.config_parser.get_db_config(self.source_or_target)['username']
            password = self.config_parser.get_db_config(self.source_or_target)['password']
            jdbc_driver = self.config_parser.get_db_config(self.source_or_target)['jdbc']['driver']
            jdbc_libraries = self.config_parser.get_db_config(self.source_or_target)['jdbc']['libraries']
            self.connection = jaydebeapi.connect(
                jdbc_driver,
                connection_string,
                [username, password],
                jdbc_libraries
            )
        else:
            raise ValueError(f"Unsupported connectivity type: {self.config_parser.get_connectivity(self.source_or_target)}")
        self.connection.autocommit = True

    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
        except AttributeError:
            pass

    def fetch_table_names(self, table_schema: str):
        query = f"""
            SELECT
                t.object_id AS table_id,
                s.name AS schema_name,
                t.name AS table_name
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = '{table_schema}'
            ORDER BY t.name
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
                    'schema_name': row[1],
                    'table_name': row[2]
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return tables
        except Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_table_columns(self, table_schema: str, table_name: str, migrator_tables) -> dict:
        result = {}
        query = f"""
            SELECT
                c.column_id AS ordinal_position,
                c.name AS column_name,
                t.name AS data_type,
                c.max_length AS length,
                c.is_nullable,
                c.is_identity,
                dc.definition AS default_value
            FROM sys.columns c
            JOIN sys.tables tb ON c.object_id = tb.object_id
            JOIN sys.schemas s ON tb.schema_id = s.schema_id
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
            WHERE s.name = '{table_schema}' AND tb.name = '{table_name}'
            ORDER BY c.column_id
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"MSSQL: Reading columns for {table_schema}.{table_name}")
            cursor.execute(query)
            for row in cursor.fetchall():
                result[row[0]] = {
                    'name': row[1],
                    'type': row[2],
                    'length': row[3],
                    'nullable': 'NOT NULL' if not row[4] else '',
                    'default': row[6].strip('()') if row[6] else '',
                    'other': 'IDENTITY' if row[5] else ''
                }

                # checking for default values substitution with the original data type
                if result[row[0]]['default'] != '':
                    result[row[0]]['default'] = migrator_tables.check_default_values_substitution(result[row[0]]['name'], result[row[0]]['type'], result[row[0]]['default'])

            cursor.close()
            self.disconnect()
            return result
        except Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def convert_table_columns(self, target_db_type: str, table_schema: str, table_name: str, source_columns: dict):
        # ...existing code from SybaseASEConnector.convert_table_columns...
        pass

    def fetch_indexes(self, settings):
        source_table_id = settings['source_table_id']
        source_schema = settings['source_schema']
        source_table_name = settings['source_table_name']
        target_schema = settings['target_schema']
        target_table_name = settings['target_table_name']
        target_columns = settings['target_columns']
        query = f"""
            SELECT
                i.name AS index_name,
                i.is_unique,
                i.is_primary_key,
                STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.index_column_id) AS column_list
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE i.object_id = {source_table_id}
            GROUP BY i.name, i.is_unique, i.is_primary_key
            ORDER BY i.name
        """
        # ...existing code from SybaseASEConnector.fetch_indexes...
        pass

    def fetch_constraints(self, source_table_id: int, target_schema, target_table_name):
        # ...existing code from SybaseASEConnector.fetch_constraints...
        pass

    def fetch_funcproc_names(self, schema: str):
        query = f"""
            SELECT
                p.object_id AS id,
                p.name AS name,
                CASE
                    WHEN p.type = 'P' THEN 'Procedure'
                    WHEN p.type = 'FN' THEN 'Function'
                END AS type
            FROM sys.objects p
            JOIN sys.schemas s ON p.schema_id = s.schema_id
            WHERE s.name = '{schema}' AND p.type IN ('P', 'FN')
            ORDER BY p.name
        """
        # ...existing code from SybaseASEConnector.fetch_funcproc_names...
        pass

    def fetch_funcproc_code(self, funcproc_id: int):
        query = f"""
            SELECT m.definition
            FROM sys.sql_modules m
            WHERE m.object_id = {funcproc_id}
        """
        # ...existing code from SybaseASEConnector.fetch_funcproc_code...
        pass

    def convert_funcproc_code(self, funcproc_code: str, target_db_type: str, source_schema: str, target_schema: str, table_list: list):
        # ...existing code from SybaseASEConnector.convert_funcproc_code...
        pass

    def fetch_views_names(self, owner_name):
        query = f"""
            SELECT
                v.object_id AS id,
                s.name AS schema_name,
                v.name AS view_name
            FROM sys.views v
            JOIN sys.schemas s ON v.schema_id = s.schema_id
            WHERE s.name = '{owner_name}'
            ORDER BY v.name
        """
        # ...existing code from SybaseASEConnector.fetch_views_names...
        pass

    def fetch_view_code(self, view_id):
        query = f"""
            SELECT m.definition
            FROM sys.sql_modules m
            WHERE m.object_id = {view_id}
        """
        # ...existing code from SybaseASEConnector.fetch_view_code...
        pass

    def convert_view_code(self, view_code: str, settings: dict):
        # ...existing code from SybaseASEConnector.convert_view_code...
        pass

    def migrate_table(self, migrate_target_connection, settings):
        # ...existing code from SybaseASEConnector.migrate_table...
        pass

    def fetch_triggers(self, schema_name, table_name):
        # ...existing code from SybaseASEConnector.fetch_triggers...
        pass

    def convert_trigger(self, trigger_name, trigger_code, source_schema, target_schema, table_list):
        # ...existing code from SybaseASEConnector.convert_trigger...
        pass

    def execute_query(self, query: str, params=None):
        # ...existing code from SybaseASEConnector.execute_query...
        pass

    def execute_sql_script(self, script_path: str):
        # ...existing code from SybaseASEConnector.execute_sql_script...
        pass

    def begin_transaction(self):
        # ...existing code from SybaseASEConnector.begin_transaction...
        pass

    def commit_transaction(self):
        # ...existing code from SybaseASEConnector.commit_transaction...
        pass

    def rollback_transaction(self):
        # ...existing code from SybaseASEConnector.rollback_transaction...
        pass

    def handle_error(self, e, description=None):
        # ...existing code from SybaseASEConnector.handle_error...
        pass

    def get_rows_count(self, table_schema: str, table_name: str):
        query = f"""SELECT COUNT(*) FROM [{table_schema}].[{table_name}]"""
        # ...existing code from SybaseASEConnector.get_rows_count...
        pass

    def get_table_size(self, table_schema: str, table_name: str):
        """
        Returns a size of the table in bytes
        """
        pass

    def fetch_sequences(self):
        query = """
            SELECT
                s.name AS sequence_name,
                s.object_id AS sequence_id,
                s.start_value AS start_value,
                s.increment AS increment_value,
                s.min_value AS min_value,
                s.max_value AS max_value,
                s.cycle_option AS cycle_option
            FROM sys.sequences s
        """
        # ...existing code from SybaseASEConnector.fetch_sequences...
        pass

    def fetch_user_defined_types(self):
        query = """
            SELECT
                s.name AS type_name,
                s.system_type_id AS system_type_id,
                s.user_type_id AS user_type_id,
                s.max_length AS max_length,
                s.is_nullable AS is_nullable
            FROM sys.types s
            WHERE s.is_user_defined = 1
        """
        # ...existing code from SybaseASEConnector.fetch_user_defined_types...
        pass

    def get_sequence_current_value(self, sequence_name: str):
        pass