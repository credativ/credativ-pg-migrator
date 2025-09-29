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
import time
import datetime

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
        total_inserted_rows = 0
        migration_stats = {}
        batch_number = 0
        shortest_batch_seconds = 0
        longest_batch_seconds = 0
        average_batch_seconds = 0
        chunk_start_row_number = 0
        chunk_end_row_number = 0
        processing_start_time = time.time()
        order_by_clause = ''
        try:
            worker_id = settings['worker_id']
            source_schema = settings['source_schema']
            source_table = settings['source_table']
            source_table_id = settings['source_table_id']
            source_columns = settings['source_columns']
            target_schema = self.config_parser.convert_names_case(settings['target_schema'])
            target_table = self.config_parser.convert_names_case(settings['target_table'])
            target_columns = settings['target_columns']
            batch_size = settings['batch_size']
            migrator_tables = settings['migrator_tables']
            migration_limitation = settings['migration_limitation']
            chunk_size = settings['chunk_size']
            chunk_number = settings['chunk_number']
            resume_after_crash = settings['resume_after_crash']
            drop_unfinished_tables = settings['drop_unfinished_tables']

            source_table_rows = self.get_rows_count(source_schema, source_table, migration_limitation)
            target_table_rows = migrate_target_connection.get_rows_count(target_schema, target_table)

            total_chunks = self.config_parser.get_total_chunks(source_table_rows, chunk_size)
            if chunk_size == -1:
                chunk_size = source_table_rows + 1

            migration_stats = {
                'rows_migrated': target_table_rows,
                'chunk_number': chunk_number,
                'total_chunks': total_chunks,
                'source_table_rows': source_table_rows,
                'target_table_rows': target_table_rows,
                'finished': True if source_table_rows == 0 else False,
            }

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
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Table {source_table} is empty - skipping data migration.")
                migrator_tables.update_data_migration_status({
                        'row_id': protocol_id,
                        'success': True,
                        'message': 'Skipped',
                        'target_table_rows': 0,
                        'batch_count': 0,
                        'shortest_batch_seconds': 0,
                        'longest_batch_seconds': 0,
                        'average_batch_seconds': 0,
                    })

                return migration_stats

            else:

                if source_table_rows > target_table_rows:

                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Source table {source_table}: {source_table_rows} rows / Target table {target_table}: {target_table_rows} rows - starting data migration.")

                    select_columns_list = []
                    orderby_columns_list = []
                    insert_columns_list = []

                    for order_num, col in source_columns.items():
                        self.config_parser.print_log_message('DEBUG2',
                                                            f"Worker {worker_id}: Table {source_schema}.{source_table}: Processing column {col['column_name']} ({order_num}) with data type {col['data_type']}")

                        if col['data_type'].lower() == 'geometry':
                            select_columns_list.append(f"ST_asText(`{col['column_name']}`) as `{col['column_name']}`")
                        elif col['data_type'].lower() == 'set':
                            select_columns_list.append(f"cast(`{col['column_name']}` as char(4000)) as `{col['column_name']}`")
                        else:
                            select_columns_list.append(f"`{col['column_name']}`")

                        insert_columns_list.append(f'''"{self.config_parser.convert_names_case(col['column_name'])}"''')
                        orderby_columns_list.append(f'''`{col['column_name']}`''')

                    select_columns = ', '.join(select_columns_list)
                    orderby_columns = ', '.join(orderby_columns_list)
                    insert_columns = ', '.join(insert_columns_list)

                    if resume_after_crash and not drop_unfinished_tables:
                        chunk_number = self.config_parser.get_total_chunks(target_table_rows, chunk_size)
                        self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Resuming migration for table {source_schema}.{source_table} from chunk {chunk_number} with data chunk size {chunk_size}.")
                        chunk_offset = target_table_rows
                    else:
                        chunk_offset = (chunk_number - 1) * chunk_size

                    chunk_start_row_number = chunk_offset + 1
                    chunk_end_row_number = chunk_offset + chunk_size

                    self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Migrating table {source_schema}.{source_table}: chunk {chunk_number}, data chunk size {chunk_size}, batch size {batch_size}, chunk offset {chunk_offset}, chunk end row number {chunk_end_row_number}, source table rows {source_table_rows}")
                    order_by_clause = ''

                    query = f'''SELECT {select_columns} FROM `{source_schema}`.`{source_table}` '''
                    if migration_limitation:
                        query += f" WHERE {migration_limitation}"
                    primary_key_columns = migrator_tables.select_primary_key(source_schema, source_table)
                    self.config_parser.print_log_message('DEBUG2', f"Worker {worker_id}: Primary key columns for {source_schema}.{source_table}: {primary_key_columns}")
                    if primary_key_columns:
                        orderby_columns = primary_key_columns
                    order_by_clause = f""" ORDER BY {orderby_columns}"""
                    query += order_by_clause + f" LIMIT {chunk_size} OFFSET {chunk_offset}"

                    self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Fetching data with cursor using query: {query}")

                    part_name = 'execute query'
                    cursor = self.connection.cursor()
                    cursor.arraysize = batch_size

                    batch_start_time = time.time()
                    reading_start_time = batch_start_time
                    processing_start_time = batch_start_time
                    batch_end_time = None
                    batch_number = 0
                    batch_durations = []

                    cursor.execute(query)
                    total_inserted_rows = 0
                    while True:
                        records = cursor.fetchmany(batch_size)
                        if not records:
                            break
                        batch_number += 1
                        reading_end_time = time.time()
                        reading_duration = reading_end_time - reading_start_time
                        self.config_parser.print_log_message('DEBUG',f"Worker {worker_id}: Fetched {len(records)} rows (batch {batch_number}) from source table {source_table}.")

                        transforming_start_time = time.time()
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

                        # # Reorder columns in each record based on the order in source_columns
                        # ordered_column_names = [col['column_name'] for col in source_columns.values()]
                        # records = [
                        #     {col_name: record[col_name] for col_name in ordered_column_names}
                        #     for record in records
                        # ]

                        self.config_parser.print_log_message('DEBUG',
                            f"Worker {worker_id}: Starting insert of {len(records)} rows from source table {source_table}")
                        transforming_end_time = time.time()
                        transforming_duration = transforming_end_time - transforming_start_time
                        inserting_start_time = time.time()
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
                        inserting_end_time = time.time()
                        inserting_duration = inserting_end_time - inserting_start_time

                        batch_end_time = time.time()
                        batch_duration = batch_end_time - batch_start_time
                        batch_durations.append(batch_duration)
                        percent_done = round(total_inserted_rows / source_table_rows * 100, 2)

                        batch_start_dt = datetime.datetime.fromtimestamp(batch_start_time)
                        batch_end_dt = datetime.datetime.fromtimestamp(batch_end_time)
                        batch_start_str = batch_start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')
                        batch_end_str = batch_end_dt.strftime('%Y-%m-%d %H:%M:%S.%f')
                        migrator_tables.insert_batches_stats({
                            'source_schema': source_schema,
                            'source_table': source_table,
                            'source_table_id': source_table_id,
                            'chunk_number': chunk_number,
                            'batch_number': batch_number,
                            'batch_start': batch_start_str,
                            'batch_end': batch_end_str,
                            'batch_rows': inserted_rows,
                            'batch_seconds': batch_duration,
                            'worker_id': worker_id,
                            'reading_seconds': reading_duration,
                            'transforming_seconds': transforming_duration,
                            'writing_seconds': inserting_duration,
                        })

                        msg = (
                            f"Worker {worker_id}: Inserted {inserted_rows} "
                            f"(total: {total_inserted_rows} from: {source_table_rows} "
                            f"({percent_done}%)) rows into target table '{target_table}': "
                            f"Batch {batch_number} duration: {batch_duration:.2f} seconds "
                            f"(r: {reading_duration:.2f}, t: {transforming_duration:.2f}, w: {inserting_duration:.2f})"
                        )
                        self.config_parser.print_log_message('INFO', msg)

                        batch_start_time = time.time()
                        reading_start_time = batch_start_time

                    target_table_rows = migrate_target_connection.get_rows_count(target_schema, target_table)
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Target table {target_schema}.{target_table} has {target_table_rows} rows")

                    shortest_batch_seconds = min(batch_durations) if batch_durations else 0
                    longest_batch_seconds = max(batch_durations) if batch_durations else 0
                    average_batch_seconds = sum(batch_durations) / len(batch_durations) if batch_durations else 0
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Migrated {total_inserted_rows} rows from {source_table} to {target_schema}.{target_table} in {batch_number} batches: "
                                                            f"Shortest batch: {shortest_batch_seconds:.2f} seconds, "
                                                            f"Longest batch: {longest_batch_seconds:.2f} seconds, "
                                                            f"Average batch: {average_batch_seconds:.2f} seconds")

                    cursor.close()

                elif source_table_rows <= target_table_rows:
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Source table {source_table} has {source_table_rows} rows, which is less than or equal to target table {target_table} with {target_table_rows} rows. No data migration needed.")

                migration_stats = {
                    'rows_migrated': total_inserted_rows,
                    'chunk_number': chunk_number,
                    'total_chunks': total_chunks,
                    'source_table_rows': source_table_rows,
                    'target_table_rows': target_table_rows,
                    'finished': False,
                }

                self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Migration stats: {migration_stats}")
                if source_table_rows <= target_table_rows or chunk_number >= total_chunks:
                    self.config_parser.print_log_message('DEBUG3', f"Worker {worker_id}: Setting migration status to finished for table {source_table} (chunk {chunk_number}/{total_chunks})")
                    migration_stats['finished'] = True
                    migrator_tables.update_data_migration_status({
                        'row_id': protocol_id,
                        'success': True,
                        'message': 'OK',
                        'target_table_rows': target_table_rows,
                        'batch_count': batch_number,
                        'shortest_batch_seconds': shortest_batch_seconds,
                        'longest_batch_seconds': longest_batch_seconds,
                        'average_batch_seconds': average_batch_seconds,
                    })

                migrator_tables.insert_data_chunk({
                    'worker_id': worker_id,
                    'source_table_id': source_table_id,
                    'source_schema': source_schema,
                    'source_table': source_table,
                    'target_schema': target_schema,
                    'target_table': target_table,
                    'source_table_rows': source_table_rows,
                    'target_table_rows': target_table_rows,
                    'chunk_number': chunk_number,
                    'chunk_size': chunk_size,
                    'migration_limitation': migration_limitation,
                    'chunk_start': chunk_start_row_number,
                    'chunk_end': chunk_end_row_number,
                    'inserted_rows': total_inserted_rows,
                    'batch_size': batch_size,
                    'total_batches': batch_number,
                    'task_started': datetime.datetime.fromtimestamp(processing_start_time).strftime('%Y-%m-%d %H:%M:%S.%f'),
                    'task_completed': datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f'),
                    'order_by_clause': order_by_clause,
                })
                return migration_stats
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Error during {part_name} -> {e}")
            self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Full stack trace: {traceback.format_exc()}")
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

    def get_rows_count(self, table_schema: str, table_name: str, migration_limitation: str = None):
        query = f"SELECT COUNT(*) FROM {table_schema}.{table_name}"
        if migration_limitation:
            query += f" WHERE {migration_limitation}"
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

    def get_top_n_tables(self, settings):
        top_tables = {}
        top_tables['by_rows'] = {}
        top_tables['by_size'] = {}
        top_tables['by_columns'] = {}
        top_tables['by_indexes'] = {}
        top_tables['by_constraints'] = {}

        try:
            order_num = 1
            top_n = self.config_parser.get_top_n_tables_by_rows()
            if top_n > 0:
                query = f"""
                    SELECT
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    TABLE_ROWS,
                    (DATA_LENGTH + INDEX_LENGTH) AS table_size
                    FROM information_schema.tables
                    WHERE TABLE_SCHEMA = '{settings['source_schema']}'
                    ORDER BY TABLE_ROWS DESC
                    LIMIT {top_n}
                """
                self.connect()
                cursor = self.connection.cursor()
                cursor.execute(query)
                order_num = 1
                for row in cursor.fetchall():
                    top_tables['by_rows'][order_num] = {
                        'owner': row[0].strip() if row[0] else '',
                        'table_name': row[1].strip() if row[1] else '',
                        'row_count': row[2],
                        'table_size': row[3],
                    }
                    order_num += 1
                cursor.close()
                self.disconnect()
                self.config_parser.print_log_message('DEBUG2', f"Top {top_n} tables by rows: {top_tables['by_rows']}")
            else:
                self.config_parser.print_log_message('DEBUG', "Top N tables by rows is not configured or set to 0, skipping this part.")

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching top {top_n} tables by rows: {e}")

        return top_tables

    def get_top_fk_dependencies(self, settings):
        top_fk_dependencies = {}
        return top_fk_dependencies

    def target_table_exists(self, target_schema, target_table):
        query = f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{target_schema}' AND TABLE_NAME = '{target_table}'
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            exists = cursor.fetchone()[0] > 0
            cursor.close()
            return exists
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error checking if target table exists: {e}")
            raise

    def fetch_all_rows(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

if __name__ == "__main__":
    print("This script is not meant to be run directly")
