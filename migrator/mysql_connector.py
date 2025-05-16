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
            database=db_config['database'],
            port=db_config['port']
        )

    def disconnect(self):
        if self.connection:
            self.connection.close()

    def fetch_table_names(self, table_schema: str):
        query = f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{table_schema}'
            AND TABLE_TYPE not in ('VIEW', 'SYSTEM VIEW')
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
                    'character_maximum_length': row[2],
                    'is_nullable': 'NOT NULL' if row[3] == 'NO' else '',
                    'column_default': row[4] if row[4] is not None else '',
                    'comment': '',
                } for i, row in enumerate(cursor.fetchall())
            }
            cursor.close()
            self.disconnect()
            return columns
        except mysql.connector.Error as e:
            self.logger.error(f"Error fetching table columns: {e}")
            raise

    def get_types_mapping(self, settings):
        target_db_type = settings['target_db_type']
        types_mapping = {}
        if target_db_type == 'postgresql':
            types_mapping = {
                'INT': 'INTEGER',
                'VARCHAR': 'VARCHAR',
                'TEXT': 'TEXT',
                'CHAR': 'TEXT',
                'FLOAT': 'REAL',
                'DOUBLE': 'DOUBLE PRECISION',
                'DECIMAL': 'NUMERIC',
                'DATETIME': 'DATETIME',
                'TIMESTAMP': 'TIMESTAMP',
                'DATE': 'DATE',
                'TIME': 'TIME',
                'BOOLEAN': 'BOOLEAN',
                'BLOB': 'BYTEA',
                'JSON': 'JSON',
                'ENUM': 'VARCHAR',
                'SET': 'VARCHAR',
                'TINYINT': 'SMALLINT',
                'SMALLINT': 'SMALLINT',
                'MEDIUMINT': 'INTEGER',
                'BIGINT': 'BIGINT',
                'BIT': 'BOOLEAN',
                'YEAR': 'INTEGER',
                'POINT': 'POINT',
                # Add more type mappings as needed
            }
        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

        return types_mapping

    def get_create_table_sql(self, settings):
        return ""

    def is_string_type(self, column_type: str) -> bool:
        string_types = ['CHAR', 'VARCHAR', 'NCHAR', 'NVARCHAR', 'TEXT', 'LONG VARCHAR', 'LONG NVARCHAR', 'UNICHAR', 'UNIVARCHAR']
        return column_type.upper() in string_types

    def is_numeric_type(self, column_type: str) -> bool:
        numeric_types = ['BIGINT', 'INTEGER', 'INT', 'TINYINT', 'SMALLINT', 'FLOAT', 'DOUBLE PRECISION', 'DECIMAL', 'NUMERIC']
        return column_type.upper() in numeric_types

    def migrate_table(self, migrate_target_connection, settings):
        part_name = 'initialize'
        source_table_rows = 0
        try:
            worker_id = settings['worker_id']
            source_schema = settings['source_schema']
            source_table = settings['source_table']
            source_table_id = settings['source_table_id']
            source_columns = settings['source_columns']
            target_schema = settings['target_schema']
            target_table = settings['target_table']
            target_columns = settings['target_columns']
            primary_key_columns = settings['primary_key_columns']
            batch_size = settings['batch_size']
            migrator_tables = settings['migrator_tables']
            migration_limitation = settings['migration_limitation']

            source_table_rows = self.get_rows_count(source_schema, source_table)
            if source_table_rows == 0:
                self.logger.info(f"Worker {worker_id}: Table {source_schema}.{source_table} is empty, skipping migration.")
                return 0
            else:
                self.logger.info(f"Worker {worker_id}: Table {source_schema}.{source_table} has {source_table_rows} rows.")
                protocol_id = migrator_tables.insert_data_migration(source_schema, source_table, source_table_id, source_table_rows, worker_id, target_schema, target_table, 0)

                # Open a cursor and fetch rows in batches
                query = f'''SELECT * FROM {source_schema.upper()}."{source_table}"'''
                if migration_limitation:
                    query += f" WHERE {migration_limitation}"

                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Worker {worker_id}: Fetching data with cursor using query: {query}")

                # offset = 0
                total_inserted_rows = 0
                cursor = self.connection.cursor()
                while True:
                    # part_name = 'fetch_data: {source_table} - {offset}'
                    # if primary_key_columns:
                    #     query = f"""SELECT * FROM {source_schema}.{source_table} ORDER BY {primary_key_columns} LIMIT {batch_size} OFFSET {offset}"""
                    # else:
                    #     query = f"""SELECT * FROM {source_schema}.{source_table} LIMIT {batch_size} OFFSET {offset}"""
                    # cursor.execute(query)
                    # records = cursor.fetchall()
                    records = cursor.fetchmany(batch_size)
                    if not records:
                        break
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Worker {worker_id}: Fetched {len(records)} rows from source table {source_table}.")

                    records = [
                        {column['name']: value for column, value in zip(source_columns.values(), record)}
                        for record in records
                    ]

                    for record in records:
                        for order_num, column in source_columns.items():
                            column_name = column['name']
                            column_type = column['type']
                            target_column_type = target_columns[order_num]['type']
                            # if column_type.lower() in ['binary', 'bytea']:
                            if column_type.lower() in ['blob']:
                                record[column_name] = bytes(record[column_name].getBytes(1, int(record[column_name].length())))  # Convert 'com.informix.jdbc.IfxCblob' to bytes
                            elif column_type.lower() in ['clob']:
                                # elif isinstance(record[column_name], IfxCblob):
                                record[column_name] = record[column_name].getSubString(1, int(record[column_name].length()))  # Convert IfxCblob to string
                                # record[column_name] = bytes(record[column_name].getBytes(1, int(record[column_name].length())))  # Convert IfxBblob to bytes
                                # record[column_name] = record[column_name].read()  # Convert IfxBblob to bytes
                            elif column_type.lower() in ['integer', 'smallint', 'tinyint', 'bit', 'boolean'] and target_column_type.lower() in ['boolean']:
                                # Convert integer to boolean
                                record[column_name] = bool(record[column_name])

                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Worker {worker_id}: Starting insert of {len(records)} rows from source table {source_table}")
                    inserted_rows = migrate_target_connection.insert_batch(target_schema, target_table, target_columns, records)
                    total_inserted_rows += inserted_rows
                    self.logger.info(f"Worker {worker_id}: Inserted {inserted_rows} (total: {total_inserted_rows} from: {source_table_rows} ({round(total_inserted_rows/source_table_rows*100, 2)}%)) rows into target table {target_table}")

                    # offset += batch_size

                target_table_rows = migrate_target_connection.get_rows_count(target_schema, target_table)
                self.logger.info(f"Worker {worker_id}: Finished migrating data for table {source_table}.")
                migrator_tables.update_data_migration_status(protocol_id, True, 'OK', target_table_rows)
                cursor.close()
                return target_table_rows
        except mysql.connector.Error as e:
            self.logger.error(f"Worker {worker_id}: Error during {part_name} -> {e}")
            raise e

    def fetch_indexes(self, settings):
        source_table_id = settings['source_table_id']
        source_schema = settings['source_schema']
        source_table_name = settings['source_table_name']
        target_schema = settings['target_schema']
        target_table_name = settings['target_table_name']
        target_columns = settings['target_columns']
        table_indexes = {}
        order_num = 1
        query = f"""
            SELECT
                DISTINCT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX,
                NON_UNIQUE, coalesce(CONSTRAINT_TYPE,'INDEX') as CONSTRAINT_TYPE
            FROM INFORMATION_SCHEMA.STATISTICS S
            LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tC
            ON S.TABLE_SCHEMA = tC.TABLE_SCHEMA AND S.TABLE_NAME = tC.TABLE_NAME
            AND S.INDEX_NAME = tC.CONSTRAINT_NAME
            WHERE S.TABLE_SCHEMA = '{source_schema}' AND S.TABLE_NAME = '{source_table_name}'
            ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                index_name = row[0]
                column_name = row[1]
                seq_in_index = row[2]
                non_unique = row[3]
                constraint_type = row[4]

                if index_name not in table_indexes:
                    table_indexes[index_name] = {
                        'name': index_name,
                        'owner': target_schema,
                        'columns': [],
                        'type': constraint_type,
                        'sql': '',
                        'comment': '',
                        'columns_data_types': [],
                        'columns_count': 0
                    }

                table_indexes[index_name]['columns'].append(column_name)

                for col_num, column_info in target_columns.items():
                    if column_info['name'] == column_name:
                        table_indexes[index_name]['columns_data_types'].append(column_info['type'])

            cursor.close()
            self.disconnect()
            returned_indexes = {}
            for index_name, index_info in table_indexes.items():
                index_info['columns_count'] = len(index_info['columns'])
                index_info['columns'] = ', '.join(index_info['columns'])
                index_info['columns_data_types'] = ', '.join(index_info['columns_data_types'])

                if index_info['type'] == 'PRIMARY KEY':
                    index_info['sql'] = f"""ALTER TABLE "{target_schema}"."{target_table_name}" ADD CONSTRAINT "{target_table_name}_{index_info['name']}" PRIMARY KEY ({index_info['columns']})"""
                else:
                    index_info['sql'] = f"""CREATE {'UNIQUE' if index_info['type'] == 'UNIQUE' else ''} INDEX "{target_table_name}_{index_info['name']}" ON "{target_schema}"."{target_table_name}" ({index_info['columns']})"""

                returned_indexes[order_num] = {
                    'name': index_info['name'],
                    'owner': index_info['owner'],
                    'columns': index_info['columns'],
                    'type': index_info['type'],
                    'sql': index_info['sql'],
                    'comment': index_info['comment'],
                    'columns_data_types': index_info['columns_data_types'],
                    'columns_count': index_info['columns_count']
                }
                order_num += 1
            return returned_indexes
        except mysql.connector.Error as e:
            self.logger.error(f"Error fetching indexes: {e}")
            raise

    def fetch_constraints(self, settings):
        source_table_id = settings['source_table_id']
        source_schema = settings['source_schema']
        source_table_name = settings['source_table_name']
        target_schema = settings['target_schema']
        target_table_name = settings['target_table_name']
        order_num = 1
        table_constraints = {}
        create_constr_query = ""
        returned_constraints = {}
        query = f"""
            SELECT
                TABLE_SCHEMA AS schema_name,
                TABLE_NAME AS table_name,
                COLUMN_NAME AS column_name,
                CONSTRAINT_NAME AS foreign_key_name,
                REFERENCED_TABLE_SCHEMA AS referenced_schema_name,
                REFERENCED_TABLE_NAME AS referenced_table_name,
                REFERENCED_COLUMN_NAME AS referenced_column_name,
                ordinal_position,
                position_in_unique_constraint
            FROM
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE
                REFERENCED_TABLE_NAME IS NOT NULL
                AND TABLE_SCHEMA = '{source_schema}'
                AND TABLE_NAME = '{source_table_name}'
            ORDER BY foreign_key_name, ordinal_position
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                schema_name = row[0]
                table_name = row[1]
                column_name = row[2]
                foreign_key_name = row[3]
                referenced_schema_name = row[4]
                referenced_table_name = row[5]
                referenced_column_name = row[6]
                ordinal_position = row[7]
                position_in_unique_constraint = row[8]

                if foreign_key_name not in table_constraints:
                    table_constraints[foreign_key_name] = {
                        'name': foreign_key_name,
                        'columns': [],
                        'referenced_table': f"{target_schema}.{referenced_table_name}",
                        'referenced_columns': [],
                        'sql': '',
                        'comment': '',
                        'columns_count': 0
                    }

                table_constraints[foreign_key_name]['columns'].append(column_name)
                table_constraints[foreign_key_name]['referenced_columns'].append(referenced_column_name)

            cursor.close()
            self.disconnect()
            for constraint, constraint_info in table_constraints.items():
                constraint_info['columns_count'] = len(constraint_info['columns'])
                constraint_info['columns'] = ', '.join(constraint_info['columns'])
                constraint_info['referenced_columns'] = ', '.join(constraint_info['referenced_columns'])
                constraint_info['sql'] = f"""ALTER TABLE "{target_schema}"."{target_table_name}" ADD CONSTRAINT "{target_table_name}_{constraint_info['name']}" FOREIGN KEY ({constraint_info['columns']})
                                            REFERENCES {constraint_info['referenced_table']} ({constraint_info['referenced_columns']})"""

                returned_constraints[order_num] = {
                    'name': f"{target_table_name}_{constraint_info['name']}",
                    'type': 'FOREIGN KEY',
                    'sql': constraint_info['sql'],
                    'comment': constraint_info['comment'],
                }
                order_num += 1

            return returned_constraints

        except mysql.connector.Error as e:
            self.logger.error(f"Error fetching constraints: {e}")
            raise


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

    def get_sequence_details(self, sequence_owner, sequence_name):
        # Placeholder for fetching sequence details
        return {}

    def fetch_views_names(self, source_schema: str):
        views = {}
        order_num = 1
        query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{source_schema}'"""
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                view_name = row[0]
                views[order_num] = {
                    'id': None,
                    'schema_name': source_schema,
                    'view_name': view_name,
                    'comment': ''
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return views
        except mysql.connector.Error as e:
            self.logger.error(f"Error fetching view names: {e}")
            raise

    def fetch_view_code(self, settings):
        # view_id = settings['view_id']
        source_schema = settings['source_schema']
        source_view_name = settings['source_view_name']
        # target_schema = settings['target_schema']
        # target_view_name = settings['target_view_name']
        query = f"""
            SELECT VIEW_DEFINITION
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = '{source_schema}' AND TABLE_NAME = '{source_view_name}'
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            view_code = cursor.fetchone()[0]
            cursor.close()
            self.disconnect()
            return view_code
        except mysql.connector.Error as e:
            self.logger.error(f"Error fetching view {source_view_name} code: {e}")
            raise

    def convert_view_code(self, view_code: str, settings: dict):
        converted_view_code = view_code
        converted_view_code = converted_view_code.replace('`', '"')
        converted_view_code = converted_view_code.replace(f'''"{settings['source_schema']}".''', f'''"{settings['target_schema']}".''')
        return converted_view_code

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

    def testing_select(self):
        return "SELECT 1"
