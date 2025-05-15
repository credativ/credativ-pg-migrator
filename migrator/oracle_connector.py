from database_connector import DatabaseConnector
from migrator_logging import MigratorLogger
import cx_Oracle

class OracleConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        if source_or_target != 'source':
            raise ValueError("Oracle is only supported as a source database")

        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.on_error_action = self.config_parser.get_on_error_action()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger

    def connect(self):
        connection_string = self.config_parser.get_connect_string(self.source_or_target)
        username = self.config_parser.get_db_config(self.source_or_target)['username']
        try:
            if username == 'SYS':
                self.connection = cx_Oracle.connect(user=username,
                                                    password=self.config_parser.get_db_config(self.source_or_target)['password'],
                                                    dsn=connection_string,
                                                    encoding="UTF-8",
                                                    mode=cx_Oracle.SYSDBA)
            else:
                self.connection = cx_Oracle.connect(user=username,
                                                    password = self.config_parser.get_db_config(self.source_or_target)['password'],
                                                    dsn=connection_string,
                                                    encoding="UTF-8")

        except ImportError as e:
            self.logger.error("cx_Oracle module is not installed.")
            raise e
        except cx_Oracle.DatabaseError as e:
            self.logger.error(f"Error connecting to Oracle database: {e}")
            raise e

    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
        except AttributeError:
            pass

    def fetch_table_names(self, table_schema: str):
        query = f"""
            SELECT table_name
            FROM all_tables
            WHERE owner = '{table_schema.upper()}'
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
                    'id': None,
                    'schema_name': table_schema,
                    'table_name': row[0],
                    'comment': ''
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return tables
        except Exception as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_table_columns(self, table_schema: str, table_name: str, migrator_tables) -> dict:
        query = f"""
            SELECT column_id, column_name, data_type, data_length, nullable, data_default
            FROM all_tab_columns
            WHERE owner = '{table_schema.upper()}' AND table_name = '{table_name.upper()}'
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
        except Exception as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def convert_table_columns(self, settings):
        target_db_type = settings['target_db_type']
        target_schema = settings['target_schema']
        target_table_name = settings['target_table_name']
        source_columns = settings['source_columns']
        converted_columns = {}
        create_table_sql = ""

        if target_db_type == 'postgresql':
            type_mapping = {
                'VARCHAR': 'VARCHAR',
                'VARCHAR2': 'VARCHAR',
                'NUMBER': 'NUMERIC',
                'DATE': 'TIMESTAMP',
                'CLOB': 'TEXT',
                'BLOB': 'BYTEA'
            }

            for order_num, column in source_columns.items():
                column_name = column['name']
                column_type = column['type']
                column_length = column['length']
                column_nullable = column['nullable']
                column_default = column['default']
                column_comment = column['comment']

                if column_type in type_mapping:
                    converted_type = type_mapping[column_type]
                    if converted_type == 'VARCHAR' and column_length:
                        if column_length > 254:
                            converted_type = 'TEXT'
                        else:
                            converted_type += f"({column_length})"
                else:
                    column_type = 'TEXT'

                converted_columns[order_num] = {
                    'name': column_name,
                    'type': converted_type,
                    'length': column_length,
                    'nullable': column_nullable,
                    'default': column_default,
                    'comment': column_comment
                }

            create_table_sql_parts = []
            for _, info in converted_columns.items():
                create_table_sql_column = ""
                column_name = info['name']
                column_type = info['type']
                column_nullable = info['nullable']
                column_default = info['default']
                column_comment = info['comment']

                create_table_sql_column = f'''"{column_name}" {column_type} {column_nullable}'''

                if column_default:
                    create_table_sql_parts.append(f"DEFAULT {column_default}")

                if column_comment:
                    create_table_sql_parts.append(f"COMMENT '{column_comment}'")

                create_table_sql_parts.append(create_table_sql_column)

            create_table_sql = ", ".join(create_table_sql_parts)
            create_table_sql = f'''CREATE TABLE "{target_schema}"."{target_table_name}" ({create_table_sql});'''
        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

        return converted_columns, create_table_sql

    def migrate_table(self, migrate_target_connection, settings):
        return 0

    def fetch_indexes(self, settings):
        source_table_id = settings['source_table_id']
        source_schema = settings['source_schema']
        source_table_name = settings['source_table_name']
        target_schema = settings['target_schema']
        target_table_name = settings['target_table_name']
        target_columns = settings['target_columns']
        table_indexes = {}
        order_num = 1

        index_query = f"""
                        SELECT
                            ai.index_name,
                            c.constraint_type,
                            ai.index_type,
                            ai.uniqueness,
                            listagg('"'||aic.column_name||'"', ', ') WITHIN GROUP (ORDER BY aic.column_position) AS indexed_columns,
                            listagg('"'||aic.column_name||'" '|| aic.descend, ', ') WITHIN GROUP (ORDER BY aic.column_position) AS indexed_columns_orders
                        FROM all_indexes ai
                        JOIN all_ind_columns aic
                        ON ai.owner = aic.index_owner AND ai.index_name = aic.index_name
                        AND ai.table_owner = aic.table_owner AND ai.table_name = aic.table_name
                        LEFT JOIN dba_constraints c
                        ON c.owner = ai.owner AND c.table_name = ai.table_name AND c.constraint_name = ai.index_name
                        WHERE
                            ai.table_owner = '{source_schema.upper()}'
                        	AND ai.table_name = '{source_table_name.upper()}'
                        GROUP BY
                            ai.owner,
                            ai.index_name,
                            c.constraint_type,
                            ai.table_owner,
                            ai.table_name,
                            ai.index_type,
                            ai.uniqueness
                        ORDER BY
                            ai.index_name
            """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(index_query)
            for row in cursor.fetchall():
                index_name = row[0]
                constraint_type = row[1]
                index_type = row[2]
                uniqueness = row[3]
                columns_list = row[4]
                columns_list_orders = row[5]

                if index_name not in table_indexes:
                    table_indexes[order_num] = {
                        'name': index_name,
                        'type': 'PRIMARY KEY' if constraint_type == 'P' else 'UNIQUE' if uniqueness == 'UNIQUE' else 'INDEX',
                        'owner': source_schema,
                        'columns': columns_list,
                        'columns_count': len(columns_list.split(',')),
                        'columns_data_types': [],
                        'sql': '',
                        'comment': '',
                    }

                create_index_query = None
                if constraint_type == 'P':
                    create_index_query = f"""ALTER TABLE "{target_schema}"."{target_table_name}" ADD CONSTRAINT "{index_name}" PRIMARY KEY ({columns_list})"""
                elif uniqueness == 'UNIQUE':
                    create_index_query = f"""CREATE UNIQUE INDEX "{index_name}" on "{target_schema}"."{target_table_name}" ({columns_list_orders})"""
                else:
                    create_index_query = f"""CREATE INDEX "{index_name}" on "{target_schema}"."{target_table_name}" ({columns_list_orders})"""
                table_indexes[order_num]['sql'] = create_index_query
                order_num += 1

            cursor.close()
            self.disconnect()
            return table_indexes

        except Exception as e:
            self.logger.error(f"Error executing query: {index_query}")
            self.logger.error(e)
            raise

    def fetch_constraints(self, settings):
        source_table_id = settings['source_table_id']
        source_schema = settings['source_schema']
        source_table_name = settings['source_table_name']
        target_schema = settings['target_schema']
        target_table_name = settings['target_table_name']
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

    def fetch_view_code(self, settings):
        view_id = settings['view_id']
        source_schema = settings['source_schema']
        source_view_name = settings['source_view_name']
        target_schema = settings['target_schema']
        target_view_name = settings['target_view_name']
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
        except Exception as e:
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
        except Exception as e:
            self.logger.error(f"Error executing SQL script: {script_path}")
            self.logger.error(e)
            raise

    def begin_transaction(self):
        self.connection.begin()

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
        except Exception as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def get_table_size(self, table_schema: str, table_name: str):
        # Placeholder for fetching table size
        return None

    def fetch_user_defined_types(self, schema: str):
        # Placeholder for fetching user-defined types
        return {}

    def testing_select(self):
        return "SELECT 1 FROM DUAL"
