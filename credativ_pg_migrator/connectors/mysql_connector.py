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

from credativ_pg_migrator.database_connector import DatabaseConnector
from credativ_pg_migrator.migrator_logging import MigratorLogger
import mysql.connector  ## install mysql-connector-python
import traceback
from tabulate import tabulate

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

    def get_sql_functions_mapping(self, settings):
        """ Returns a dictionary of SQL functions mapping for the target database """
        target_db_type = settings['target_db_type']
        if target_db_type == 'postgresql':
            return {}
        else:
            self.config_parser.print_log_message('ERROR', f"Unsupported target database type: {target_db_type}")

    def fetch_table_names(self, table_schema: str):
        tables = {}
        query = f"""
            SELECT
                TABLE_NAME,
                TABLE_COMMENT
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{table_schema}'
            AND TABLE_TYPE not in ('VIEW', 'SYSTEM VIEW')
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for i, row in enumerate(cursor.fetchall()):
                tables[i + 1] = {
                    'id': None,
                    'schema_name': table_schema,
                    'table_name': row[0],
                    'comment': row[1]
                }
            cursor.close()
            self.disconnect()
            return tables
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching table names: {e}")
            raise

    def fetch_table_columns(self, settings) -> dict:
        table_schema = settings['table_schema']
        table_name = settings['table_name']
        columns = {}
        query = f"""
            SELECT
                ORDINAL_POSITION,
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE,
                COLUMN_TYPE,
                COLUMN_DEFAULT,
                CASE WHEN upper(EXTRA) = 'AUTO_INCREMENT' THEN 'YES'
                ELSE 'NO' END AS IS_IDENTITY,
                CASE WHEN upper(EXTRA) = 'STORED GENERATED' THEN 'YES'
                ELSE 'NO' END AS IS_GENERATED_STORED,
                CASE WHEN upper(EXTRA) = 'VIRTUAL GENERATED' THEN 'YES'
                ELSE 'NO' END AS IS_GENERATED_VIRTUAL,
                GENERATION_EXPRESSION,
                COLUMN_COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{table_schema}' AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                ordinal_position = row[0]
                column_name = row[1]
                data_type = row[2]
                character_maximum_length = row[3]
                numeric_precision = row[4]
                numeric_scale = row[5]
                is_nullable = row[6]
                column_type = row[7]
                column_default = row[8]
                is_identity = row[9]
                is_generated_stored = row[10]
                is_generated_virtual = row[11]
                generation_expression = row[12]
                column_comment = row[13]
                columns[ordinal_position] = {
                    'column_name': column_name,
                    'data_type': data_type,
                    'column_type': column_type,
                    'character_maximum_length': character_maximum_length,
                    'numeric_precision': numeric_precision,
                    'numeric_scale': numeric_scale,
                    'is_nullable': is_nullable,
                    'column_default_value': column_default,
                    'is_identity': is_identity,
                    'is_generated_stored': is_generated_stored,
                    'is_generated_virtual': is_generated_virtual,
                    'generation_expression': generation_expression,
                    'column_comment': column_comment,
                }
            cursor.close()
            self.disconnect()
            return columns
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching table columns: {e}")
            self.config_parser.print_log_message('ERROR', "Full stack trace:")
            self.config_parser.print_log_message('ERROR', traceback.format_exc())
            raise

    def get_types_mapping(self, settings):
        target_db_type = settings['target_db_type']
        types_mapping = {}
        if target_db_type == 'postgresql':
            types_mapping = {
                'INT': 'INTEGER',
                'INTEGER': 'INTEGER',
                'FLOAT': 'REAL',
                'DOUBLE': 'DOUBLE PRECISION',
                'DECIMAL': 'NUMERIC',
                'TINYINT': 'SMALLINT',
                'SMALLINT': 'SMALLINT',
                'MEDIUMINT': 'INTEGER',
                'BIGINT': 'BIGINT',

                'VARCHAR': 'VARCHAR',
                'TEXT': 'TEXT',
                'CHAR': 'CHAR',
                'JSON': 'JSONB',
                'ENUM': 'VARCHAR',
                'SET': 'TEXT',  # PostgreSQL does not have a direct SET type, using TEXT array instead

                'DATETIME': 'TIMESTAMP',
                'TIMESTAMP': 'TIMESTAMP',
                'DATE': 'DATE',
                'TIME': 'TIME',

                'BOOLEAN': 'BOOLEAN',
                'BLOB': 'BYTEA',
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
        target_table_rows = 0
        try:
            worker_id = settings['worker_id']
            source_schema = settings['source_schema']
            source_table = settings['source_table']
            source_table_id = settings['source_table_id']
            source_columns = settings['source_columns']
            target_schema = settings['target_schema']
            target_table = settings['target_table']
            target_columns = settings['target_columns']
            # primary_key_columns = settings['primary_key_columns']
            batch_size = settings['batch_size']
            migrator_tables = settings['migrator_tables']
            migration_limitation = settings['migration_limitation']

            source_table_rows = self.get_rows_count(source_schema, source_table)
            target_table_rows = 0

            ## source_schema, source_table, source_table_id, source_table_rows, worker_id, target_schema, target_table, target_table_rows
            protocol_id = migrator_tables.insert_data_migration({
                'worker_id': worker_id,
                'source_table_id': source_table_id,
                'source_schema': source_schema,
                'source_table': source_table,
                'target_schema': target_schema,
                'target_table': target_table,
                'source_table_rows': source_table_rows,
                'target_table_rows': target_table_rows,
            })

            if source_table_rows == 0:
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Table {source_schema}.{source_table} is empty, skipping migration.")
                return 0
            else:
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Table {source_schema}.{source_table} has {source_table_rows} rows.")

                # Open a cursor and fetch rows in batches
                # Build comma-separated column names, encapsulated in double quotes

                select_columns_list = []
                for order_num, col in source_columns.items():
                    self.config_parser.print_log_message('DEBUG2',
                                                         f"Worker {worker_id}: Table {source_schema}.{source_table}: Processing column {col['column_name']} ({order_num}) with data type {col['data_type']}")
                    insert_columns = ', '.join([f'''"{col['column_name']}"''' for col in source_columns.values()])

                    if col['data_type'].lower() == 'geometry':
                        select_columns_list.append(f"ST_asText(`{col['column_name']}`) as `{col['column_name']}`")
                    elif col['data_type'].lower() == 'set':
                        select_columns_list.append(f"cast(`{col['column_name']}` as char(4000)) as `{col['column_name']}`")
                    else:
                        select_columns_list.append(f"`{col['column_name']}`")
                select_columns = ', '.join(select_columns_list)

                query = f'''SELECT {select_columns} FROM {source_schema}.{source_table}'''
                if migration_limitation:
                    query += f" WHERE {migration_limitation}"

                self.config_parser.print_log_message('DEBUG2',
                    f"Worker {worker_id}: Fetching data with cursor using query: {query}")

                # offset = 0
                total_inserted_rows = 0
                cursor = self.connection.cursor()
                cursor.execute(query)
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
                    self.config_parser.print_log_message( 'DEBUG',
                        f"Worker {worker_id}: Fetched {len(records)} rows from source table {source_table}.")

                    records = [
                        {column['column_name']: value for column, value in zip(source_columns.values(), record)}
                        for record in records
                    ]

                    for record in records:
                        for order_num, column in source_columns.items():
                            column_name = column['column_name']
                            column_type = column['data_type']
                            target_column_type = target_columns[order_num]['data_type']
                            # if column_type.lower() in ['binary', 'bytea']:
                            if column_type.lower() in ['blob']:
                                if record[column_name] is not None:
                                    record[column_name] = bytes(record[column_name])
                            elif column_type.lower() in ['clob']:
                                record[column_name] = record[column_name].getSubString(1, int(record[column_name].length()))  # Convert IfxCblob to string
                                # record[column_name] = bytes(record[column_name].getBytes(1, int(record[column_name].length())))  # Convert IfxBblob to bytes
                                # record[column_name] = record[column_name].read()  # Convert IfxBblob to bytes
                            elif column_type.lower() == 'set':
                                # Convert SET to plain comma separated string
                                if isinstance(record[column_name], list):
                                    record[column_name] = ','.join(str(item) for item in record[column_name])
                                elif record[column_name] is None:
                                    record[column_name] = ''
                                else:
                                    record[column_name] = str(record[column_name])
                            elif column_type.lower() == 'geometry':
                                record[column_name] = f"{record[column_name]}"

                                # # Convert geometry to string representation if possible
                                # if record[column_name] is not None:
                                #     try:
                                #         # Try to decode as UTF-8 string (may work for some geometry types)
                                #         record[column_name] = record[column_name].decode('utf-8', errors='replace')
                                #     except Exception as e:
                                #         # Fallback: represent as string of bytes
                                #         record[column_name] = str(record[column_name])
                                # else:
                                #     record[column_name] = None
                            elif column_type.lower() in ['integer', 'smallint', 'tinyint', 'bit', 'boolean'] and target_column_type.lower() in ['boolean']:
                                # Convert integer to boolean
                                record[column_name] = bool(record[column_name])

                    # Reorder columns in each record based on the order in source_columns
                    ordered_column_names = [col['column_name'] for col in source_columns.values()]
                    records = [
                        {col_name: record[col_name] for col_name in ordered_column_names}
                        for record in records
                    ]

                    self.config_parser.print_log_message('DEBUG',
                        f"Worker {worker_id}: Starting insert of {len(records)} rows from source table {source_table}")
                    inserted_rows = migrate_target_connection.insert_batch({
                        'target_schema': target_schema,
                        'target_table': target_table,
                        'target_columns': target_columns,
                        'data': records,
                        'worker_id': worker_id,
                        'migrator_tables': migrator_tables,
                        'insert_columns': insert_columns,
                    })
                    total_inserted_rows += inserted_rows
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Inserted {inserted_rows} (total: {total_inserted_rows} from: {source_table_rows} ({round(total_inserted_rows/source_table_rows*100, 2)}%)) rows into target table {target_table}")

                    # offset += batch_size

                target_table_rows = migrate_target_connection.get_rows_count(target_schema, target_table)
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Finished migrating data for table {target_table} - migrated {target_table_rows} rows.")
                migrator_tables.update_data_migration_status(protocol_id, True, 'OK', target_table_rows)
                cursor.close()
                return target_table_rows
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Error during {part_name} -> {e}")
            self.config_parser.print_log_message('ERROR', "Full stack trace:")
            self.config_parser.print_log_message('ERROR', traceback.format_exc())
            raise e

    def fetch_indexes(self, settings):
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']
        table_indexes = {}
        order_num = 1
        query = f"""
            SELECT
                DISTINCT
                INDEX_NAME,
                COLUMN_NAME,
                SEQ_IN_INDEX,
                NON_UNIQUE,
                coalesce(CONSTRAINT_TYPE,'INDEX') as CONSTRAINT_TYPE,
                INDEX_COMMENT
            FROM INFORMATION_SCHEMA.STATISTICS S
            LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tC
            ON S.TABLE_SCHEMA = tC.TABLE_SCHEMA AND S.TABLE_NAME = tC.TABLE_NAME
                AND S.INDEX_NAME = tC.CONSTRAINT_NAME
            WHERE S.TABLE_SCHEMA = '{source_table_schema}'
                AND S.TABLE_NAME = '{source_table_name}'
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
                index_comment = row[5]
                if index_name not in table_indexes:
                    table_indexes[index_name] = {
                        'index_name': index_name,
                        'index_owner': source_table_schema,
                        'index_columns': [],
                        'index_type': constraint_type,
                        'index_comment': index_comment,
                    }

                table_indexes[index_name]['index_columns'].append(column_name)

            cursor.close()
            self.disconnect()
            returned_indexes = {}
            for index_name, index_info in table_indexes.items():
                index_info['index_columns'] = ', '.join(index_info['index_columns'])

                returned_indexes[order_num] = {
                    'index_name': index_info['index_name'],
                    'index_owner': index_info['index_owner'],
                    'index_columns': index_info['index_columns'],
                    'index_type': index_info['index_type'],
                    'index_comment': index_info['index_comment'],
                }
                order_num += 1
            return returned_indexes
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching indexes: {e}")
            raise

    def get_create_index_sql(self, settings):
        return ""

    def fetch_constraints(self, settings):
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

        order_num = 1
        table_constraints = {}
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
                AND TABLE_SCHEMA = '{source_table_schema}'
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
                        'constraint_name': foreign_key_name,
                        'constraint_owner': schema_name,
                        'constraint_type': 'FOREIGN KEY',
                        'constraint_columns': [],
                        'referenced_table_name': referenced_table_name,
                        'referenced_table_schema': referenced_schema_name,
                        'referenced_columns': [],
                        'constraint_sql': '',
                        'constraint_comment': '',
                    }

                table_constraints[foreign_key_name]['constraint_columns'].append(column_name)
                table_constraints[foreign_key_name]['referenced_columns'].append(referenced_column_name)

            cursor.close()
            self.disconnect()
            for constraint_name, constraint_info in table_constraints.items():
                constraint_info['constraint_columns'] = ', '.join(constraint_info['constraint_columns'])
                constraint_info['referenced_columns'] = ', '.join(constraint_info['referenced_columns'])

                returned_constraints[order_num] = {
                    'constraint_name': constraint_info['constraint_name'],
                    'constraint_owner': constraint_info['constraint_owner'],
                    'constraint_columns': constraint_info['constraint_columns'],
                    'referenced_table_name': constraint_info['referenced_table_name'],
                    'referenced_table_schema': constraint_info['referenced_table_schema'],
                    'referenced_columns': constraint_info['referenced_columns'],
                    'constraint_type': constraint_info['constraint_type'],
                    'constraint_sql': constraint_info['constraint_sql'],
                    'constraint_comment': constraint_info['constraint_comment'],
                }
                order_num += 1

            return returned_constraints

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching constraints: {e}")
            raise

    def get_create_constraint_sql(self, settings):
        return ""

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

    def convert_funcproc_code(self, settings):
        funcproc_code = settings['funcproc_code']
        target_db_type = settings['target_db_type']
        source_schema = settings['source_schema']
        target_schema = settings['target_schema']
        table_list = settings['table_list']
        view_list = settings['view_list']
        converted_code = ''
        # placeholder for actual conversion logic
        return converted_code

    def fetch_sequences(self, table_schema: str, table_name: str):
        # Implement sequence fetching logic
        pass

    def get_sequence_details(self, sequence_owner, sequence_name):
        # Placeholder for fetching sequence details
        return {}

    def fetch_views_names(self, source_schema: str):
        views = {}
        order_num = 1
        query = f"""
            SELECT
                TABLE_NAME
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = '{source_schema}'"""
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
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching view names: {e}")
            raise

    def fetch_view_code(self, settings):
        # view_id = settings['view_id']
        source_schema = settings['source_schema']
        source_view_name = settings['source_view_name']
        # target_schema = settings['target_schema']
        # target_view_name = settings['target_view_name']
        query = f"""
            SELECT
                VIEW_DEFINITION
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = '{source_schema}'
            AND TABLE_NAME = '{source_view_name}'
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            view_code = cursor.fetchone()[0]
            cursor.close()
            self.disconnect()
            return view_code
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching view {source_view_name} code: {e}")
            raise

    def convert_view_code(self, settings: dict):
        view_code = settings['view_code']
        converted_view_code = view_code
        converted_view_code = converted_view_code.replace('`', '"')
        converted_view_code = converted_view_code.replace(f'''"{settings['source_schema']}".''', f'''"{settings['target_schema']}".''')
        converted_view_code = converted_view_code.replace(f'''{settings['source_schema']}.''', f'''"{settings['target_schema']}".''')
        converted_view_code = converted_view_code.replace('""', '"')
        return converted_view_code

    def get_sequence_current_value(self, sequence_id: int):
        # Implement sequence current value fetching logic
        pass

    def execute_query(self, query: str, params=None):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            cursor.close()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {e}")
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
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing SQL script: {e}")
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
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching row count: {e}")
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
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching table size: {e}")
            raise

    def fetch_user_defined_types(self, schema: str):
        # Implement user-defined type fetching logic
        pass

    def fetch_domains(self, schema: str):
        # Placeholder for fetching domains
        return {}

    def get_create_domain_sql(self, settings):
        # Placeholder for generating CREATE DOMAIN SQL
        return ""

    def fetch_default_values(self, settings) -> dict:
        # Placeholder for fetching default values
        return {}

    def get_table_description(self, settings) -> dict:
        table_schema = settings['table_schema']
        table_name = settings['table_name']
        output = ""
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(f"describe {table_schema}.{table_name}")

            set_num = 1
            while True:
                if cursor.description is not None:
                    rows = cursor.fetchall()
                    if rows:
                        output += f"Result set {set_num}:\n"
                        columns = [column[0] for column in cursor.description]
                        table = tabulate(rows, headers=columns, tablefmt="github")
                        output += table + "\n\n"
                        set_num += 1
                if not cursor.nextset():
                    break

            cursor.execute(f"show create table {table_schema}.{table_name}")

            set_num = 1
            while True:
                if cursor.description is not None:
                    rows = cursor.fetchall()
                    if rows:
                        output += f"Result set {set_num}:\n"
                        columns = [column[0] for column in cursor.description]
                        table = tabulate(rows, headers=columns, tablefmt="github")
                        output += table + "\n\n"
                        set_num += 1
                if not cursor.nextset():
                    break

            cursor.close()
            self.disconnect()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching table description for {table_schema}.{table_name}: {e}")
            raise

        return { 'table_description': output.strip() }

    def testing_select(self):
        return "SELECT 1"

    def get_database_version(self):
        query = "SELECT VERSION()"
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            version = cursor.fetchone()[0]
            cursor.close()
            self.disconnect()
            return version
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching database version: {e}")
            raise

    def get_database_size(self):
        query = "SELECT SUM(data_length + index_length) FROM information_schema.tables WHERE table_schema = DATABASE()"
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            size = cursor.fetchone()[0]
            cursor.close()
            self.disconnect()
            return size
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching database size: {e}")
            raise

    def get_top10_biggest_tables(self, settings):
        query = f"""
            SELECT
                TABLE_NAME,
                (DATA_LENGTH + INDEX_LENGTH) AS total_size
            FROM
                information_schema.tables
            WHERE
                TABLE_SCHEMA = '{settings['source_schema']}'
            ORDER BY
                total_size DESC
            LIMIT 10
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            tables = {}
            for i, row in enumerate(cursor.fetchall()):
                tables[i + 1] = {
                    'table_name': row[0],
                    'total_size': row[1]
                }
            cursor.close()
            self.disconnect()
            return tables
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching top 10 biggest tables: {e}")
            raise

if __name__ == "__main__":
    print("This script is not meant to be run directly")
