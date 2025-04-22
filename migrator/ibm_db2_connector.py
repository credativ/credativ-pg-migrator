from database_connector import DatabaseConnector
from migrator_logging import MigratorLogger
import jaydebeapi

class IBMDB2Connector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        if source_or_target != 'source':
            raise ValueError("IBM DB2 is only supported as a source database")

        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.on_error_action = self.config_parser.get_on_error_action()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger

    def connect(self):
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
        self.connection.jconn.setAutoCommit(False)

    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
        except AttributeError:
            pass

    def fetch_table_names(self, table_schema: str):
        query = f"""
            SELECT TABNAME AS table_name
            FROM SYSCAT.TABLES
            WHERE TABSCHEMA = '{table_schema}'
            ORDER BY TABNAME
        """
        try:
            tables = {}
            order_num = 1
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                tables[order_num] = {
                    'id': None,
                    'schema_name': table_schema,
                    'table_name': row[0],
                    'comment': ''
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return tables
        except jaydebeapi.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_table_columns(self, table_schema: str, table_name: str, migrator_tables) -> dict:
        query = f"""
            SELECT COLNAME, TYPENAME, LENGTH, NULLS, DEFAULT
            FROM SYSCAT.COLUMNS
            WHERE TABSCHEMA = '{table_schema}' AND TABNAME = '{table_name}'
            ORDER BY COLNO
        """
        try:
            result = {}
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for idx, row in enumerate(cursor.fetchall(), start=1):
                result[idx] = {
                    'name': row[0],
                    'type': row[1],
                    'length': row[2],
                    'nullable': 'NOT NULL' if row[3] == 'N' else '',
                    'default': row[4],
                    'comment': '',
                    'other': ''
                }
            cursor.close()
            self.disconnect()
            return result
        except jaydebeapi.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def convert_table_columns(self, target_db_type: str, table_schema: str, table_name: str, source_columns: dict):
        # Placeholder for column conversion logic
        return source_columns, ""

    def migrate_table(self, migrate_target_connection, settings):
        # Placeholder for table migration logic
        pass

    def fetch_indexes(self, settings):
        source_table_id = settings['source_table_id']
        source_schema = settings['source_schema']
        source_table_name = settings['source_table_name']
        target_schema = settings['target_schema']
        target_table_name = settings['target_table_name']
        target_columns = settings['target_columns']
        # Placeholder for fetching indexes
        return {}

    def fetch_constraints(self, source_table_id: int, target_schema, target_table_name):
        # Placeholder for fetching constraints
        return {}

    def fetch_triggers(self, table_id: int, table_schema: str, table_name: str):
        # Placeholder for fetching triggers
        return {}

    def convert_trigger(self, trig: str, settings: dict):
        # Placeholder for trigger conversion
        pass

    def fetch_funcproc_names(self, schema: str):
        # Placeholder for fetching function/procedure names
        return {}

    def fetch_funcproc_code(self, funcproc_id: int):
        # Placeholder for fetching function/procedure code
        return ""

    def convert_funcproc_code(self, funcproc_code: str, target_db_type: str, source_schema: str, target_schema: str, table_list: list):
        # Placeholder for function/procedure conversion
        return None

    def fetch_sequences(self, table_schema: str, table_name: str):
        # Placeholder for fetching sequences
        return {}

    def fetch_views_names(self, source_schema: str):
        # Placeholder for fetching view names
        return {}

    def fetch_view_code(self, view_id: int):
        # Placeholder for fetching view code
        return ""

    def convert_view_code(self, view_code: str, settings: dict):
        # Placeholder for view conversion
        return view_code

    def get_sequence_current_value(self, sequence_id: int):
        # Placeholder for fetching sequence current value
        return None

    def execute_query(self, query: str, params=None):
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            cursor.close()
        except jaydebeapi.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def execute_sql_script(self, script_path: str):
        try:
            with open(script_path, 'r') as file:
                script = file.read()
            cursor = self.connection.cursor()
            cursor.execute(script)
            cursor.close()
        except jaydebeapi.Error as e:
            self.logger.error(f"Error executing script: {script_path}")
            self.logger.error(e)
            raise

    def begin_transaction(self):
        self.connection.jconn.setAutoCommit(False)

    def commit_transaction(self):
        self.connection.commit()
        self.connection.jconn.setAutoCommit(True)

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
        except jaydebeapi.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def get_table_size(self, table_schema: str, table_name: str):
        # Placeholder for fetching table size
        return 0

    def fetch_user_defined_types(self, schema: str):
        # Placeholder for fetching user-defined types
        return {}