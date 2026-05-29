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
import ibm_db_dbi  ## install ibm_db package to use this connector
import traceback
import re
import sqlglot
import time
import datetime

class IbmDb2LuwConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        if source_or_target != 'source':
            raise ValueError("IBM DB2 is only supported as a source database")

        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.connectivity = self.config_parser.get_connectivity(self.source_or_target)
        self.on_error_action = self.config_parser.get_on_error_action()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger

    def connect(self):
        connection_string = self.config_parser.get_connect_string(self.source_or_target)
        try:
            self.connection = ibm_db_dbi.connect(connection_string, "", "")
            if not self.connection:
                raise Exception("Failed to connect to the database")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: connect: Unexpected error while connecting into the database: {e}")
            raise

    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
        except Exception as e:
            pass

    def get_sql_functions_mapping(self, settings):
        """ Returns a dictionary of SQL functions mapping for the target database """
        target_db_type = settings['target_db_type']
        if target_db_type == 'postgresql':
            return {
                # --- Special Registers (Session Variables) ---
                "CURRENT SQLID": "CURRENT_USER",
                "CURRENT USER": "CURRENT_USER",
                "USER": "SESSION_USER",          # SESSION_USER tracks the original login role
                "CURRENT DATE": "CURRENT_DATE",
                "CURRENT TIME": "CURRENT_TIME",
                "CURRENT TIMESTAMP": "CURRENT_TIMESTAMP",
                "CURRENT SCHEMA": "CURRENT_SCHEMA",
                "CURRENT SERVER": "current_database()",

                # --- Null Handling & Control Flow ---
                "VALUE(": "COALESCE(",
                "IFNULL(": "COALESCE(",
                "NVL(": "COALESCE(",
                ## "DECODE(expr, search, result, default)": "CASE expr WHEN search THEN result ELSE default END",

                # --- String Functions ---
                "SUBSTR(": "SUBSTRING(",
                "POSSTR(": "STRPOS(",       # DB2's POSSTR takes (source, search)
                "LOCATE(": "POSITION(", # DB2's LOCATE takes (search, source)
                "UCASE(": "UPPER(",
                "LCASE(": "LOWER(",
                "STRIP(": "TRIM(",
                "LENGTH(": "LENGTH(",
                "CONCAT(": "CONCAT(",                 # Or simply use the str1 || str2 operator

                # --- Date and Time Functions ---
                "YEAR(": "EXTRACT(YEAR FROM ",
                "MONTH(": "EXTRACT(MONTH FROM ",
                "DAY(": "EXTRACT(DAY FROM ",
                "HOUR(": "EXTRACT(HOUR FROM ",
                "MINUTE(": "EXTRACT(MINUTE FROM ",
                "SECOND(": "EXTRACT(SECOND FROM ",

                # Db2 DAYS() returns the integer number of days since Jan 1, 0001.
                # To replicate this exact integer in Postgres, you subtract that date from your column.
                ## "DAYS(date_col)": "(date_col::DATE - '0001-01-01'::DATE)",

                # "DATE(expr)": "expr::DATE",                                 # Or CAST(expr AS DATE)
                # "TIMESTAMP(expr)": "expr::TIMESTAMP",                       # Or CAST(expr AS TIMESTAMP)
                # "ADD_DAYS(date_col, n)": "date_col + (n || ' days')::INTERVAL",
                # "ADD_MONTHS(date_col, n)": "date_col + (n || ' months')::INTERVAL",

                # --- Math & Numeric Functions ---
                "CEILING(": "CEIL(",
                "TRUNCATE(": "TRUNC(",
                "RAND()": "RANDOM()",
                "DECFLOAT(": "num::NUMERIC",                            # PostgreSQL uses NUMERIC for arbitrary precision
            }
        else:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: get_sql_functions_mapping: Unsupported target database type: {target_db_type}")
            return {}

    def migrate_sequences(self, target_connector, settings):
        return True

    def fetch_table_names(self, table_schema: str):
        query = f"""
            SELECT
                TABLEID,
                TABNAME,
                REMARKS
            FROM SYSCAT.TABLES
            WHERE TABSCHEMA = upper('{table_schema}') AND TYPE = 'T'
            ORDER BY TABNAME"""
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
                    'comment': row[2]
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return tables
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: fetch_table_names: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_table_columns(self, settings) -> dict:
        table_schema = settings['table_schema']
        table_name = settings['table_name']
        result = {}
        try:
            if self.config_parser.get_system_catalog() in ('SYSCAT','NONE'):
                query = f"""
                    SELECT
                        COLNO,
                        COLNAME,
                        TYPENAME,
                        "LENGTH",
                        "LENGTH",
                        "SCALE",
                        "NULLS",
                        "DEFAULT",
                        "REMARKS",
                        IDENTITY
                    FROM SYSCAT.COLUMNS
                    WHERE TABSCHEMA = upper('{table_schema}') AND tabname = '{table_name}' ORDER BY COLNO
                """
            elif self.config_parser.get_system_catalog() in ('SYSIBM'):
                query = f"""
                    SELECT
                        C.ORDINAL_POSITION,
                        C.COLUMN_NAME,
                        C.DATA_TYPE,
                        C.CHARACTER_MAXIMUM_LENGTH,
                        C.NUMERIC_PRECISION,
                        C.NUMERIC_SCALE,
                        C.IS_NULLABLE,
                        C.COLUMN_DEFAULT,
                        '' AS REMARKS,
                        S.IDENTITY
                    FROM SYSIBM.COLUMNS C
                    LEFT JOIN SYSIBM.SYSCOLUMNS S ON C.TABLE_NAME = S.TBNAME AND C.TABLE_SCHEMA = S.TBCREATOR AND C.COLUMN_NAME = S.NAME
                    WHERE C.TABLE_NAME = '{table_name}' AND C.TABLE_SCHEMA = upper('{table_schema}') ORDER BY C.ORDINAL_POSITION
                """
            else:
                raise ValueError(f"Unsupported system catalog: {self.config_parser.get_system_catalog()}")
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
                if self.config_parser.get_system_catalog() == 'SYSCAT':
                    is_nullable = 'NO' if is_nullable == 'N' else 'YES'
                column_default = row[7]
                column_comment = row[8] if len(row) > 8 else ''
                is_identity = 'YES' if (len(row) > 9 and row[9] in ('Y', 'YES')) else 'NO'

                # when column is identity, code shall ignore default value if this is set
                if is_identity == 'YES' and column_default is not None:
                    column_default = None

                column_type = data_type
                if self.is_string_type(data_type) and character_maximum_length is not None:
                    column_type = f"{data_type}({character_maximum_length})"
                elif self.is_numeric_type(data_type) and numeric_precision is not None and numeric_scale is not None:
                    column_type = f"{data_type}({numeric_precision},{numeric_scale})"
                elif self.is_numeric_type(data_type) and numeric_precision is not None:
                    column_type = f"{data_type}({numeric_precision})"

                result[ordinal_position] = {
                    'column_name': column_name,
                    'data_type': data_type,
                    'column_type': column_type,
                    'character_maximum_length': character_maximum_length,
                    'numeric_precision': numeric_precision,
                    'numeric_scale': numeric_scale,
                    'is_nullable': is_nullable,
                    'column_default_value': column_default,
                    'column_comment': column_comment,
                    'is_identity': is_identity,
                }
            cursor.close()
            self.disconnect()
            return result
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: fetch_table_columns: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_types_mapping(self, settings):
        target_db_type = settings['target_db_type']
        types_mapping = {}
        if target_db_type == 'postgresql':
            types_mapping = {
                'BIGDATETIME': 'TIMESTAMP',
                'DATE': 'DATE',
                'DATETIME': 'TIMESTAMP',
                'SMALLDATETIME': 'TIMESTAMP',
                'TIME': 'TIME',
                'TIMESTAMP': 'TIMESTAMP',
                'BIGINT': 'BIGINT',
                'UNSIGNED BIGINT': 'BIGINT',
                'INTEGER': 'INTEGER',
                'INT': 'INTEGER',
                'INT8': 'BIGINT',
                'UNSIGNED INT': 'INTEGER',
                'UINT': 'INTEGER',
                'TINYINT': 'SMALLINT',
                'SMALLINT': 'SMALLINT',

                'BLOB': 'BYTEA',

                'BOOLEAN': 'BOOLEAN',
                'BIT': 'BOOLEAN',

                'BINARY': 'BYTEA',
                'VARBINARY': 'BYTEA',
                'IMAGE': 'BYTEA',
                'CHAR': 'CHAR',
                'NCHAR': 'CHAR',
                'UNICHAR': 'CHAR',
                'NVARCHAR': 'VARCHAR',
                'TEXT': 'TEXT',
                'SYSNAME': 'TEXT',
                'LONGSYSNAME': 'TEXT',
                'LONG VARCHAR': 'VARCHAR',
                'LONG NVARCHAR': 'VARCHAR',
                'UNICHAR': 'CHAR',
                'UNITEXT': 'TEXT',
                'UNIVARCHAR': 'VARCHAR',
                'VARCHAR': 'VARCHAR',

                'CLOB': 'TEXT',
                'DECIMAL': 'DECIMAL',
                'DOUBLE PRECISION': 'DOUBLE PRECISION',
                'FLOAT': 'FLOAT',
                'INTERVAL': 'INTERVAL',
                'MONEY': 'MONEY',
                'NUMERIC': 'NUMERIC',
                'REAL': 'REAL',
                'SERIAL8': 'BIGSERIAL',
                'SERIAL': 'SERIAL',
                'SMALLFLOAT': 'REAL',
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
            source_schema_name = settings['source_schema_name']
            source_table_name = settings['source_table_name']
            source_table_id = settings['source_table_id']
            source_columns = settings['source_columns']
            # target_schema_name = self.config_parser.convert_names_case(settings['target_schema_name'])
            target_schema_name = settings['target_schema_name'] ## target schema is used as it is defined in config, not converted to upper/lower case
            target_table_name = self.config_parser.convert_names_case(settings['target_table_name'])
            target_columns = settings['target_columns']
            batch_size = settings['batch_size']
            migrator_tables = settings['migrator_tables']
            migration_limitation = settings['migration_limitation']
            chunk_size = settings['chunk_size']
            chunk_number = settings['chunk_number']
            resume_after_crash = settings['resume_after_crash']
            drop_unfinished_tables = settings['drop_unfinished_tables']

            source_table_rows = self.get_rows_count(source_schema_name, source_table_name, migration_limitation)
            target_table_rows = migrate_target_connection.get_rows_count(target_schema_name, target_table_name)

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
            ## source_schema_name, source_table_name, source_table_id, source_table_rows, worker_id, target_schema_name, target_table_name, target_table_rows
            protocol_id = migrator_tables.insert_data_migration({
                'worker_id': worker_id,
                'source_table_id': source_table_id,
                'source_schema_name': source_schema_name,
                'source_table_name': source_table_name,
                'target_schema_name': target_schema_name,
                'target_table_name': target_table_name,
                'source_table_rows': source_table_rows,
                'target_table_rows': target_table_rows,
            })

            if source_table_rows == 0:
                self.config_parser.print_log_message('INFO', f"ibm_db2_luw_connector: migrate_table: Worker {worker_id}: Table {source_table_name} is empty - skipping data migration.")
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
                part_name = 'migrate_table in batches using cursor'
                self.config_parser.print_log_message('INFO', f"ibm_db2_luw_connector: migrate_table: Worker {worker_id}: Table {source_table_name} has {source_table_rows} rows - starting data migration.")

                data_conflict_action = settings.get('data_conflict_action')
                if source_table_rows > target_table_rows or data_conflict_action in ('merge_keep_target', 'merge_keep_source', 'replace'):
                    migrator_tables.update_data_migration_started(protocol_id)

                    part_name = 'migrate_table in batches using cursor'
                    self.config_parser.print_log_message('INFO', f"ibm_db2_luw_connector: migrate_table: Worker {worker_id}: Source table {source_table_name}: {source_table_rows} rows / Target table {target_table_name}: {target_table_rows} rows - starting data migration.")

                    select_columns_list = []
                    orderby_columns_list = []
                    insert_columns_list = []
                    for order_num, col in source_columns.items():
                        self.config_parser.print_log_message('DEBUG2',
                                                            f"Worker {worker_id}: Table {source_schema_name}.{source_table_name}: Processing column {col['column_name']} ({order_num}) with data type {col['data_type']}")

			            # if col['data_type'].lower() == 'datetime':
			            #     select_columns_list.append(f"TO_CHAR({col['column_name']}, '%Y-%m-%d %H:%M:%S') as {col['column_name']}")
			            #     select_columns_list.append(f"ST_asText(`{col['column_name']}`) as `{col['column_name']}`")
			            # elif col['data_type'].lower() == 'set':
			            #     select_columns_list.append(f"cast(`{col['column_name']}` as char(4000)) as `{col['column_name']}`")
			            # else:
                        select_columns_list.append(f'''"{col['column_name']}"''')

                        insert_columns_list.append(f'''"{self.config_parser.convert_names_case(col['column_name'])}"''')
                        orderby_columns_list.append(f'''"{col['column_name']}"''')

                    select_columns = ', '.join(select_columns_list)
                    orderby_columns = ', '.join(orderby_columns_list)
                    insert_columns = ', '.join(insert_columns_list)

                    if resume_after_crash and not drop_unfinished_tables:
                        chunk_number = self.config_parser.get_total_chunks(target_table_rows, chunk_size)
                        self.config_parser.print_log_message('DEBUG', f"ibm_db2_luw_connector: migrate_table: Worker {worker_id}: Resuming migration for table {source_schema_name}.{source_table_name} from chunk {chunk_number} with data chunk size {chunk_size}.")
                        chunk_offset = target_table_rows
                    else:
                        chunk_offset = (chunk_number - 1) * chunk_size

                    chunk_start_row_number = chunk_offset + 1
                    chunk_end_row_number = chunk_offset + chunk_size

                    self.config_parser.print_log_message('DEBUG', f"ibm_db2_luw_connector: migrate_table: Worker {worker_id}: Migrating table {source_schema_name}.{source_table_name}: chunk {chunk_number}, data chunk size {chunk_size}, batch size {batch_size}, chunk offset {chunk_offset}, chunk end row number {chunk_end_row_number}, source table rows {source_table_rows}")
                    order_by_clause = ''

                    # if table is small, skipping ordering does not make sense because it will not speed up the migration

                    query = f'''SELECT {select_columns} FROM {source_schema_name.upper()}."{source_table_name}"'''
                    if migration_limitation:
                        query += f" WHERE {migration_limitation}"
                    primary_key_columns = migrator_tables.select_primary_key({'source_schema_name': source_schema_name, 'source_table_name': source_table_name})
                    self.config_parser.print_log_message('DEBUG2', f"ibm_db2_luw_connector: migrate_table: Worker {worker_id}: Primary key columns for {source_schema_name}.{source_table_name}: {primary_key_columns}")
                    if primary_key_columns:
                        orderby_columns = primary_key_columns
                    order_by_clause = f""" ORDER BY {orderby_columns}"""
                    query += order_by_clause + f" LIMIT {chunk_size} OFFSET {chunk_offset}"

                    self.config_parser.print_log_message('DEBUG', f"ibm_db2_luw_connector: migrate_table: Worker {worker_id}: Fetching data with cursor using query: {query}")

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
                    while True:
                        records = cursor.fetchmany(batch_size)
                        if not records:
                            break
                        batch_number += 1
                        reading_end_time = time.time()
                        reading_duration = reading_end_time - reading_start_time
                        self.config_parser.print_log_message('DEBUG', f"ibm_db2_luw_connector: migrate_table: Worker {worker_id}: Fetched {len(records)} rows (batch {batch_number}) from source table {source_table_name}.")

                        transforming_start_time = time.time()
                        records = [
                            {column['column_name']: value for column, value in zip(source_columns.values(), record)}
                            for record in records
                        ]
                        for record in records:
                            for order_num, column in source_columns.items():
                                column_name = column['column_name']
                                column_type = column['data_type']
                                if column_type.lower() in ['binary', 'varbinary', 'image']:
                                    record[column_name] = bytes(record[column_name]) if record[column_name] is not None else None
                                elif column_type.lower() in ['datetime', 'smalldatetime', 'date', 'time', 'timestamp']:
                                    record[column_name] = str(record[column_name]) if record[column_name] is not None else None

                        # Insert batch into target table
                        self.config_parser.print_log_message('DEBUG', f"ibm_db2_luw_connector: migrate_table: Worker {worker_id}: Starting insert of {len(records)} rows from source table {source_table_name}")
                        transforming_end_time = time.time()
                        transforming_duration = transforming_end_time - transforming_start_time
                        inserting_start_time = time.time()
                        inserted_rows = migrate_target_connection.insert_batch({
                            'target_schema_name': target_schema_name,
                            'target_table_name': target_table_name,
                            'target_columns': target_columns,
                            'data': records,
                            'worker_id': worker_id,
                            'migrator_tables': migrator_tables,
                            'insert_columns': insert_columns,
                            'insert_values': settings.get('insert_values'),
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
                            'source_schema_name': source_schema_name,
                            'source_table_name': source_table_name,
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
                            f"({percent_done}%)) rows into target table '{target_table_name}': "
                            f"Batch {batch_number} duration: {batch_duration:.2f} seconds "
                            f"(r: {reading_duration:.2f}, t: {transforming_duration:.2f}, w: {inserting_duration:.2f})"
                        )
                        self.config_parser.print_log_message('INFO', msg)

                        batch_start_time = time.time()
                        reading_start_time = batch_start_time

                    target_table_rows = migrate_target_connection.get_rows_count(target_schema_name, target_table_name)
                    self.config_parser.print_log_message('INFO', f"ibm_db2_luw_connector: migrate_table: Worker {worker_id}: Target table {target_schema_name}.{target_table_name} has {target_table_rows} rows")

                    shortest_batch_seconds = min(batch_durations) if batch_durations else 0
                    longest_batch_seconds = max(batch_durations) if batch_durations else 0
                    average_batch_seconds = sum(batch_durations) / len(batch_durations) if batch_durations else 0
                    self.config_parser.print_log_message('INFO', f"ibm_db2_luw_connector: migrate_table: Worker {worker_id}: Migrated {total_inserted_rows} rows from {source_table_name} to {target_schema_name}.{target_table_name} in {batch_number} batches: "
                                                            f"Shortest batch: {shortest_batch_seconds:.2f} seconds, "
                                                            f"Longest batch: {longest_batch_seconds:.2f} seconds, "
                                                            f"Average batch: {average_batch_seconds:.2f} seconds")
                    cursor.close()

                elif source_table_rows <= target_table_rows and data_conflict_action not in ('merge_keep_target', 'merge_keep_source', 'replace'):
                    self.config_parser.print_log_message('INFO', f"ibm_db2_luw_connector: migrate_table: Worker {worker_id}: Source table {source_table_name} has {source_table_rows} rows, which is less than or equal to target table {target_table_name} with {target_table_rows} rows. No data migration needed.")

                migration_stats = {
                    'rows_migrated': total_inserted_rows,
                    'chunk_number': chunk_number,
                    'total_chunks': total_chunks,
                    'source_table_rows': source_table_rows,
                    'target_table_rows': target_table_rows,
                    'finished': False,
                }

                self.config_parser.print_log_message('DEBUG', f"ibm_db2_luw_connector: migrate_table: Worker {worker_id}: Migration stats: {migration_stats}")
                if source_table_rows == target_table_rows or chunk_number >= total_chunks:
                    self.config_parser.print_log_message('DEBUG3', f"ibm_db2_luw_connector: migrate_table: Worker {worker_id}: Setting migration status to finished for table {source_table_name} (chunk {chunk_number}/{total_chunks})")
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
                    'source_schema_name': source_schema_name,
                    'source_table_name': source_table_name,
                    'target_schema_name': target_schema_name,
                    'target_table_name': target_table_name,
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
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: migrate_table: Worker {worker_id}: Error during {part_name} -> {e}")
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: migrate_table: Worker {worker_id}: Full stack trace: {traceback.format_exc()}")
            raise e

    def fetch_indexes(self, settings):
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

        table_indexes = {}
        order_num = 1
        query = f"""
            SELECT
                INDNAME,
                COLNAMES,
                COLCOUNT,
                UNIQUERULE,
                REMARKS
            FROM SYSCAT.INDEXES I
            WHERE I.TABSCHEMA = upper('{source_table_schema}')
            AND I.TABNAME = '{source_table_name}'
            ORDER BY INDNAME
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                index_name = row[0]
                index_columns = ', '.join(f'"{col}"' for col in row[1].lstrip('+').split('+') if col)
                columns_count = row[2]
                index_type = row[3]
                index_comment = row[4]

                table_indexes[order_num] = {
                    'index_name': index_name,
                    'index_type': 'PRIMARY KEY' if index_type == 'P' else 'UNIQUE' if index_type == 'U' else 'INDEX',
                    'index_owner': source_table_schema,
                    'index_columns': index_columns,
                    'index_comment': index_comment,
                }
                order_num += 1

            cursor.close()
            self.disconnect()
            self.config_parser.print_log_message( 'DEBUG2', f"ibm_db2_luw_connector: fetch_indexes: Indexes for table {source_table_name} ({source_table_schema}): {table_indexes}")
            return table_indexes
        except Exception as e:
            self.config_parser.print_log_message( 'ERROR', f"ibm_db2_luw_connector: fetch_indexes: Error executing query: {query}")
            self.config_parser.print_log_message( 'ERROR', str(e))
            raise

    def get_create_index_sql(self, settings):
        return ""

    def fetch_constraints(self, settings):
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

        order_num = 1
        table_constraints = {}
        create_constraint_query = None
        query = f"""
            SELECT
                CONSTNAME,
                TYPE
            FROM SYSCAT.TABCONST
            WHERE TABSCHEMA = '{source_table_schema.upper()}'
            AND TABNAME = '{source_table_name}'
            AND TYPE NOT IN ('P')
            ORDER BY CONSTNAME;"""
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                constraint_name = row[0]
                constraint_type = row[1]

                if constraint_type == 'F':
                    constraint_type = 'FOREIGN KEY'
                    query_fk = f"""
                        SELECT
                            PK_COLNAMES,
                            REFTABNAME,
                            FK_COLNAMES,
                            REFTABSCHEMA
                        FROM SYSCAT.REFERENCES
                        WHERE TABSCHEMA = '{source_table_schema.upper()}'
                        AND TABNAME = '{source_table_name}'
                        AND CONSTNAME = '{constraint_name}'
                    """
                    cursor.execute(query_fk)
                    fk_row = cursor.fetchone()
                    if fk_row:
                        pk_columns = fk_row[0].strip().lstrip('+').split('+')
                        pk_columns = ', '.join(f'"{col}"' for col in pk_columns)
                        ref_table_name = fk_row[1]
                        fk_columns = fk_row[2].strip().lstrip('+').split('+')
                        fk_columns = ', '.join(f'"{col}"' for col in fk_columns)
                        ref_table_schema = fk_row[3].strip() if fk_row[3] else source_table_schema
                    else:
                        ref_table_schema = source_table_schema

                    table_constraints[order_num] = {
                        'constraint_name': constraint_name,
                        'constraint_type': constraint_type,
                        'constraint_owner': source_table_schema,
                        'constraint_columns': fk_columns,
                        'referenced_table_schema': ref_table_schema,
                        'referenced_table_name': ref_table_name,
                        'referenced_columns': pk_columns,
                        'constraint_sql': '',
                        'constraint_comment': '',
                    }
                    order_num += 1
                elif constraint_type == 'K':
                    constraint_type = 'CHECK'
                    query_chk = f"""
                        SELECT TEXT
                        FROM SYSCAT.CHECKS
                        WHERE TABSCHEMA = '{source_table_schema.upper()}'
                        AND TABNAME = '{source_table_name}'
                        AND CONSTNAME = '{constraint_name}'
                    """
                    cursor.execute(query_chk)
                    chk_row = cursor.fetchone()
                    constraint_sql = chk_row[0].strip() if chk_row else ''

                    table_constraints[order_num] = {
                        'constraint_name': constraint_name,
                        'constraint_type': constraint_type,
                        'constraint_owner': source_table_schema,
                        'constraint_columns': '',
                        'referenced_table_schema': '',
                        'referenced_table_name': '',
                        'referenced_columns': '',
                        'constraint_sql': constraint_sql,
                        'constraint_comment': '',
                    }
                    order_num += 1

            cursor.close()
            self.disconnect()
            return table_constraints
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: fetch_constraints: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_constraint_sql(self, settings):
        return ""

    def fetch_triggers(self, table_id: int, table_schema: str, table_name: str):
        triggers = {}
        order_num = 1
        try:
            query = f"""
                SELECT TRIGNAME, TRIGEVENT, TEXT, REMARKS
                FROM SYSCAT.TRIGGERS
                WHERE TABSCHEMA = upper('{table_schema}') AND TABNAME = '{table_name}'
            """
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                triggers[order_num] = {
                    'id': order_num,
                    'name': row[0].strip() if row[0] else row[0],
                    'event': 'UPDATE', # dummy, parsed in convert_trigger
                    'new': '',
                    'old': '',
                    'sql': row[2],
                    'comment': row[3].strip() if row[3] else None
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return triggers
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: fetch_triggers: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def convert_trigger(self, settings: dict):
        trigger_sql = settings.get('trigger_sql', '')
        trigger_name = settings.get('trigger_name', '')
        target_schema_name = settings.get('target_schema_name', '')
        target_table_name = settings.get('target_table_name', '')

        # Basic cleanup
        trigger_sql = re.sub(r'--([^\n]*)', r'/*\1*/', trigger_sql)

        # 1. Timing (BEFORE, AFTER, INSTEAD OF)
        timing_match = re.search(r'\b(BEFORE|AFTER|INSTEAD\s+OF)\b', trigger_sql, re.IGNORECASE)
        timing = timing_match.group(1).upper() if timing_match else 'BEFORE'

        # 2. Event
        event_match = re.search(r'\b(INSERT|UPDATE|DELETE)(?:\s+OF\s+([a-zA-Z0-9_,\s]+))?\b', trigger_sql[timing_match.end():] if timing_match else trigger_sql, re.IGNORECASE)
        event = event_match.group(1).upper() if event_match else 'UPDATE'
        of_cols = event_match.group(2) if event_match and event_match.group(2) else None

        pg_event = event
        if of_cols and event == 'UPDATE':
            cols = [c.strip() for c in of_cols.split(',')]
            # Discard any matches that leaked to 'ON'
            cols = [c for c in cols if c and c.upper() != 'ON']
            # Reconstruct list safely
            actual_cols = []
            for c in cols:
                if ' ON ' in c.upper():
                    c = c.upper().split(' ON ')[0].strip()
                if c.upper().endswith(' ON'):
                    c = c[:-3].strip()
                if c:
                    actual_cols.append(c)
            if actual_cols:
                pg_event += f" OF {', '.join(actual_cols)}"

        # 3. Referencing Aliases
        old_alias, new_alias = 'OLD', 'NEW'
        old_match = re.search(r'\bOLD\s+AS\s+([a-zA-Z0-9_]+)\b', trigger_sql, re.IGNORECASE)
        if old_match: old_alias = old_match.group(1)

        new_match = re.search(r'\bNEW\s+AS\s+([a-zA-Z0-9_]+)\b', trigger_sql, re.IGNORECASE)
        if new_match: new_alias = new_match.group(1)

        # 4. Extract WHEN and Body
        mode_match = re.search(r'\bMODE\s+DB2SQL\b', trigger_sql, re.IGNORECASE)
        for_each_match = re.search(r'\bFOR\s+EACH\s+(ROW|STATEMENT)\b', trigger_sql, re.IGNORECASE)

        start_pos = 0
        if mode_match:
            start_pos = mode_match.end()
        elif for_each_match:
            start_pos = for_each_match.end()

        remainder = trigger_sql[start_pos:].strip()
        remainder = re.sub(r'(?i)^(NOT\s+)?SECURED\s+', '', remainder).strip()

        when_clause = ""
        body = ""

        if remainder.upper().startswith('WHEN'):
            when_text = remainder[4:].lstrip()
            if when_text.startswith('('):
                depth = 0
                for i, char in enumerate(when_text):
                    if char == '(': depth += 1
                    elif char == ')': depth -= 1
                    if depth == 0:
                        when_clause = when_text[1:i].strip()
                        body = when_text[i+1:].strip()
                        break
        else:
            body = remainder

        # Strip BEGIN ATOMIC / BEGIN / END
        body = re.sub(r'(?i)^BEGIN\s+ATOMIC\s+', '', body).strip()
        body = re.sub(r'(?i)^BEGIN\s+', '', body).strip()
        body = re.sub(r'(?i)END;?\s*$', '', body).strip()

        # 5. Replacements
        def replace_aliases(text):
            if not text: return text
            if old_alias.upper() != 'OLD':
                text = re.sub(rf'\b{re.escape(old_alias)}\.', 'OLD.', text, flags=re.IGNORECASE)
            if new_alias.upper() != 'NEW':
                text = re.sub(rf'\b{re.escape(new_alias)}\.', 'NEW.', text, flags=re.IGNORECASE)
            return text

        when_clause = replace_aliases(when_clause)
        body = replace_aliases(body)

        # Replace CURRENT DATE / TIMESTAMP
        body = re.sub(r'\bCURRENT\s+DATE\b', 'CURRENT_DATE', body, flags=re.IGNORECASE)
        body = re.sub(r'\bCURRENT\s+TIMESTAMP\b', 'CURRENT_TIMESTAMP', body, flags=re.IGNORECASE)
        when_clause = re.sub(r'\bCURRENT\s+DATE\b', 'CURRENT_DATE', when_clause, flags=re.IGNORECASE)
        when_clause = re.sub(r'\bCURRENT\s+TIMESTAMP\b', 'CURRENT_TIMESTAMP', when_clause, flags=re.IGNORECASE)

        # Handle SIGNAL SQLSTATE and RAISE_ERROR
        body = re.sub(r"(?i)SIGNAL\s+SQLSTATE\s+'([^']+)'\s*\(\s*('[^']+')\s*\);?", r"RAISE EXCEPTION \2 USING ERRCODE = '\1';", body)
        body = re.sub(r"(?i)RAISE_ERROR\s*\(\s*'([^']+)'\s*,\s*('[^']+')\s*\)", r"RAISE EXCEPTION \2 USING ERRCODE = '\1';", body)

        # Handle assignments: SET a = b or SET (a,b) = (c,d)
        if body.upper().startswith('SET'):
            body = re.sub(r'(?i)^SET\s*', '', body)
            tuple_match = re.match(r'^\(\s*([^)]+)\s*\)\s*=\s*\(\s*(.+)\s*\);?$', body, re.IGNORECASE | re.DOTALL)
            if tuple_match:
                cols = [c.strip() for c in tuple_match.group(1).split(',')]
                vals = [c.strip() for c in tuple_match.group(2).split(',')]
                if len(cols) == 1:
                    body = f"{cols[0]} := {tuple_match.group(2)};"
                elif len(cols) == len(vals):
                    # Multi-assignment
                    body = "\n".join([f"{c} := {v};" for c, v in zip(cols, vals)])
            else:
                body = re.sub(r'(?i)^([A-Za-z0-9_.]+)\s*=', r'\1 := ', body)
            if not body.strip().endswith(';'):
                body += ';'

        # Handle plain updates
        if not body.strip().endswith(';'):
            body += ';'

        # Target Generation
        func_name = f"{trigger_name}_func"

        pg_func = f"""CREATE OR REPLACE FUNCTION "{target_schema_name}"."{func_name}"()
RETURNS TRIGGER AS $$
BEGIN
{body}
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""
        when_sql = f"\nWHEN ({when_clause})" if when_clause else ""
        pg_trigger = f"""CREATE TRIGGER "{trigger_name}"
{timing} {pg_event} ON "{target_schema_name}"."{target_table_name}"
FOR EACH ROW{when_sql}
EXECUTE FUNCTION "{target_schema_name}"."{func_name}"();
"""

        self.config_parser.print_log_message('DEBUG', f"ibm_db2_luw_connector: convert_trigger: Converted {trigger_name}")
        return pg_func + '\n' + pg_trigger

    def fetch_funcproc_names(self, schema: str):
        # Placeholder for fetching function/procedure names
        return {}

    def fetch_funcproc_code(self, funcproc_id: int):
        # Placeholder for fetching function/procedure code
        return ""

    def convert_funcproc_code(self, settings):
        funcproc_code = settings['funcproc_code']
        target_db_type = settings['target_db_type']
        source_schema_name = settings['source_schema_name']
        target_schema_name = settings['target_schema_name']
        table_list = settings['table_list']
        view_list = settings['view_list']
        converted_code = funcproc_code
        
        if target_db_type == 'postgresql':
            sql_functions_mapping = self.get_sql_functions_mapping({ 'target_db_type': target_db_type })
            if sql_functions_mapping:
                for src_func, tgt_func in sql_functions_mapping.items():
                    escaped_src_func = re.escape(src_func)
                    if escaped_src_func.endswith(r'\(') or escaped_src_func.endswith(r'\)'):
                        converted_code = re.sub(rf"(?i)\b{escaped_src_func}", tgt_func, converted_code, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
                    else:
                        converted_code = re.sub(rf"(?i)\b{escaped_src_func}\b", tgt_func, converted_code, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)

        return converted_code

    def fetch_sequences(self, schema_name: str):
        sequences = {}
        order_num = 1
        try:
            query_standalone = f"""
                SELECT
                    SEQNAME,
                    NULL AS TABNAME,
                    NULL AS COLNAME,
                    START,
                    INCREMENT,
                    MINVALUE,
                    MAXVALUE,
                    CACHE,
                    CYCLE
                FROM SYSCAT.SEQUENCES
                WHERE SEQSCHEMA = upper('{schema_name}') AND SEQTYPE = 'S'
            """
            query_identity = f"""
                SELECT
                    TABNAME || '_' || COLNAME || '_SEQ' AS SEQNAME,
                    TABNAME,
                    COLNAME,
                    START,
                    INCREMENT,
                    MINVALUE,
                    MAXVALUE,
                    CACHE,
                    CYCLE
                FROM SYSCAT.COLIDENTATTRIBUTES
                WHERE TABSCHEMA = upper('{schema_name}')
            """
            
            self.connect()
            cursor = self.connection.cursor()
            
            for query in [query_standalone, query_identity]:
                cursor.execute(query)
                for row in cursor.fetchall():
                    sequences[order_num] = {
                        'sequence_name': row[0].strip() if row[0] else row[0],
                        'table_name': row[1].strip() if row[1] else row[1],
                        'column_name': row[2].strip() if row[2] else row[2],
                        'source_start_value': int(row[3]) if row[3] is not None else None,
                        'source_increment_by': int(row[4]) if row[4] is not None else None,
                        'source_minvalue': int(row[5]) if row[5] is not None else None,
                        'source_maxvalue': int(row[6]) if row[6] is not None else None,
                        'source_cache': int(row[7]) if row[7] is not None else None,
                        'source_is_cycled': row[8].strip() if row[8] else row[8],
                        'source_sequence_sql': ''
                    }
                    order_num += 1
            cursor.close()
            self.disconnect()
            return sequences
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: fetch_sequences: Error executing query: {e}")
            raise

    def get_sequence_details(self, sequence_owner, sequence_name):
        return {}

    def migrate_sequences(self, target_connector, settings):
        target_schema_name = settings.get('target_schema_name', '')
        target_sequence_name = settings.get('target_sequence_name', '')
        source_table_name = settings.get('source_table_name', None)
        source_start_value = settings.get('source_start_value')
        source_increment_by = settings.get('source_increment_by')
        source_minvalue = settings.get('source_minvalue')
        source_maxvalue = settings.get('source_maxvalue')
        source_cache = settings.get('source_cache')
        source_is_cycled = settings.get('source_is_cycled')

        if not target_sequence_name:
            return True

        # Do not explicitly create sequences for identity columns
        if source_table_name:
            return True

        try:
            sql_parts = [f'CREATE SEQUENCE "{target_schema_name}"."{target_sequence_name}"']
            if source_increment_by is not None:
                sql_parts.append(f"INCREMENT BY {source_increment_by}")
            if source_minvalue is not None:
                sql_parts.append(f"MINVALUE {source_minvalue}")
            if source_maxvalue is not None:
                sql_parts.append(f"MAXVALUE {source_maxvalue}")
            if source_start_value is not None:
                sql_parts.append(f"START WITH {source_start_value}")
            if source_cache is not None and source_cache > 1:
                sql_parts.append(f"CACHE {source_cache}")
            if source_is_cycled == 'Y':
                sql_parts.append("CYCLE")
            
            sequence_sql = " ".join(sql_parts)
            self.config_parser.print_log_message('DEBUG', f"ibm_db2_luw_connector: migrate_sequences: SQL: {sequence_sql}")
            target_connector.execute_query(sequence_sql)
            return True
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: migrate_sequences: Error creating sequence {target_sequence_name}: {e}")
            return False

    def get_aliases(self, settings):
        source_schema_name = settings.get('source_schema_name')
        aliases = {}
        order_num = 1
        query = ""
        try:
            if self.config_parser.get_system_catalog() in ('SYSCAT', 'NONE'):
                query = f"""
                    SELECT
                        TABNAME,
                        BASE_TABSCHEMA,
                        BASE_TABNAME,
                        TABSCHEMA,
                        REMARKS
                    FROM SYSCAT.TABLES
                    WHERE TYPE = 'A' AND TABSCHEMA = upper('{source_schema_name}')
                    ORDER BY TABNAME
                """
            elif self.config_parser.get_system_catalog() == 'SYSIBM':
                query = f"""
                    SELECT
                        NAME,
                        CREATOR,
                        NAME,
                        CREATOR,
                        REMARKS
                    FROM SYSIBM.SYSTABLES
                    WHERE TYPE = 'A' AND CREATOR = upper('{source_schema_name}')
                    ORDER BY NAME
                """
            else:
                raise ValueError(f"Unsupported system catalog: {self.config_parser.get_system_catalog()}")

            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                alias_name = row[0]
                aliased_schema_name = row[1] if row[1] else ''
                aliased_table_name = row[2] if row[2] else ''
                alias_owner = row[3] if row[3] else source_schema_name
                alias_comment = row[4]
                aliases[order_num] = {
                    'id': order_num,
                    'alias_schema_name': source_schema_name,
                    'alias_name': alias_name,
                    'aliased_schema_name': aliased_schema_name,
                    'aliased_table_name': aliased_table_name,
                    'alias_owner': alias_owner,
                    'alias_sql': f"CREATE ALIAS {source_schema_name}.{alias_name} FOR {aliased_schema_name}.{aliased_table_name}",
                    'alias_comment': alias_comment
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return aliases
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: get_aliases: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_views_names(self, source_schema_name: str):
        views = {}
        order_num = 1
        query = f"""
            SELECT
                V.VIEWNAME,
                V.VIEWSCHEMA,
                T.REMARKS
            FROM SYSCAT.VIEWS V
            LEFT JOIN SYSCAT.TABLES T ON V.VIEWSCHEMA = T.TABSCHEMA AND V.VIEWNAME = T.TABNAME
            WHERE V.VIEWSCHEMA = upper('{source_schema_name}') AND V.VALID = 'Y'
            ORDER BY V.VIEWNAME
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                view_name = row[0].strip() if row[0] else row[0]
                view_schema = row[1].strip() if row[1] else row[1]
                comment = row[2]
                views[order_num] = {
                    'id': order_num,
                    'schema_name': view_schema,
                    'view_name': view_name,
                    'target_schema_name': '',
                    'target_view_name': '',
                    'comment': comment,
                    'is_alias': False
                }
                order_num += 1

            cursor.close()
            self.disconnect()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_luw_connector: fetch_views_names: ({source_schema_name}): {views}")
            return views
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: fetch_views_names: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_view_code(self, settings):
        source_schema_name = settings['source_schema_name']
        source_view_name = settings['source_view_name']
        query = f"""
            SELECT
                TEXT
            FROM SYSCAT.VIEWS
            WHERE VIEWSCHEMA = upper('{source_schema_name}') AND VIEWNAME = '{source_view_name}'
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            view_sql = row[0] if row else ""
            cursor.close()
            self.disconnect()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_luw_connector: fetch_view_code: ({source_schema_name}.{source_view_name}): {view_sql}")
            return view_sql
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: fetch_view_code: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def convert_view_code(self, settings: dict):

        def quote_column_names(node):
            if isinstance(node, sqlglot.exp.Column):
                if node.name:
                    base_name = node.name.upper() if not node.args.get("this").args.get("quoted") else node.name
                    converted_name = self.config_parser.convert_names_case(base_name)
                    node.set("this", sqlglot.exp.Identifier(this=converted_name, quoted=True))
                
                table_id = node.args.get("table")
                if isinstance(table_id, sqlglot.exp.Identifier):
                    base_table = table_id.name.upper() if not table_id.args.get("quoted") else table_id.name
                    converted_table = self.config_parser.convert_names_case(base_table)
                    table_id.set("this", converted_table)
                    if not table_id.args.get("quoted"):
                        table_id.set("quoted", True)
                        
                db_id = node.args.get("db")
                if isinstance(db_id, sqlglot.exp.Identifier):
                    base_db = db_id.name.upper() if not db_id.args.get("quoted") else db_id.name
                    converted_db = self.config_parser.convert_names_case(base_db)
                    db_id.set("this", converted_db)
                    if not db_id.args.get("quoted"):
                        db_id.set("quoted", True)
            if isinstance(node, sqlglot.exp.Alias) and isinstance(node.args.get("alias"), sqlglot.exp.Identifier):
                alias = node.args["alias"]
                base_name = alias.name.upper() if not alias.args.get("quoted") else alias.name
                converted_alias = self.config_parser.convert_names_case(base_name)
                alias.set("this", converted_alias)
                if not alias.args.get("quoted"):
                    alias.set("quoted", True)
            if isinstance(node, sqlglot.exp.Schema):
                for expr in node.expressions:
                    if isinstance(expr, sqlglot.exp.Identifier):
                        base_name = expr.name.upper() if not expr.args.get("quoted") else expr.name
                        converted_name = self.config_parser.convert_names_case(base_name)
                        expr.set("this", converted_name)
                        if not expr.args.get("quoted"):
                            expr.set("quoted", True)
            return node

        def replace_schema_names(node):
            if isinstance(node, sqlglot.exp.Table):
                schema = node.args.get("db")
                if schema and schema.name.upper() == settings['source_schema_name'].upper():
                    node.set("db", sqlglot.exp.Identifier(this=settings['target_schema_name'], quoted=False))
            return node

        def quote_schema_and_table_names(node):
            if isinstance(node, sqlglot.exp.TableAlias):
                alias_id = node.args.get("this")
                if isinstance(alias_id, sqlglot.exp.Identifier):
                    base_alias = alias_id.name.upper() if not alias_id.args.get("quoted") else alias_id.name
                    converted_alias = self.config_parser.convert_names_case(base_alias)
                    alias_id.set("this", converted_alias)
                    if not alias_id.args.get("quoted"):
                        alias_id.set("quoted", True)
            if isinstance(node, sqlglot.exp.Table):
                schema = node.args.get("db")
                schema_name_for_lookup = schema.name if schema else settings['source_schema_name']
                if schema:
                    base_schema = schema.name.upper() if not schema.args.get("quoted") else schema.name
                    converted_schema = self.config_parser.convert_names_case(base_schema)
                    schema.set("this", converted_schema)
                    if not schema.args.get("quoted"):
                        schema.set("quoted", True)
                table = node.args.get("this")
                if table:
                    # Lookup alias if enabled
                    table_name_to_use = table.name.upper() if not table.args.get("quoted") else table.name
                    if not isinstance(node.parent, sqlglot.exp.Create):
                        if self.config_parser.get_use_aliases_as_target_names() and settings.get('migrator_tables'):
                            alias_dict = settings['migrator_tables'].get_alias_for_table(schema_name_for_lookup, table_name_to_use)
                            if alias_dict and not settings.get('alias_view'):
                                alias_name = alias_dict.get('target_alias_name')
                                alias_target_type = alias_dict.get('alias_target_type', 'UNKNOWN')

                                if alias_target_type == 'TABLE':
                                    if alias_name.lower() == settings.get('target_view_name', '').lower() or alias_name.lower() == settings.get('source_view_name', '').lower():
                                        self.config_parser.print_log_message('INFO', f"ibm_db2_luw_connector: convert_view_code: Skipped replacing referenced table '{table_name_to_use}' with alias '{alias_name}' to avoid circular reference. Settings: {settings}")
                                    else:
                                        self.config_parser.print_log_message('INFO', f"ibm_db2_luw_connector: convert_view_code: Replaced referenced table '{table_name_to_use}' with alias '{alias_name}' inside view generation. Settings: {settings}")
                                        table_name_to_use = alias_name
                                else:
                                    self.config_parser.print_log_message('DEBUG', f"ibm_db2_luw_connector: convert_view_code: Skipped replacing '{table_name_to_use}' with alias '{alias_name}' because alias points to a {alias_target_type}, not a TABLE.")

                    converted_table = self.config_parser.convert_names_case(table_name_to_use)
                    table.set("this", converted_table)
                    if not table.args.get("quoted"):
                        table.set("quoted", True)
            return node

        def replace_functions(node):
            mapping = self.get_sql_functions_mapping({ 'target_db_type': settings['target_db_type'] })
            func_name_map = {}
            for k, v in mapping.items():
                if k.endswith('('):
                    func_name_map[k[:-1].lower()] = v[:-1] if v.endswith('(') else v
                elif k.endswith('()'):
                    func_name_map[k[:-2].lower()] = v
                else:
                    func_name_map[k.lower()] = v

            if isinstance(node, sqlglot.exp.Anonymous):
                func_name = node.name.lower()
                if func_name in func_name_map:
                    mapped = func_name_map[func_name]
                    if '(' not in mapped:
                        node.set("this", sqlglot.exp.Identifier(this=mapped, quoted=False))
                    else:
                        if mapped.startswith('extract('):
                            arg = node.args.get("expressions")
                            if arg and len(arg) == 1:
                                return sqlglot.exp.Extract(
                                    this=sqlglot.exp.Identifier(this=func_name, quoted=False),
                                    expression=arg[0]
                                )
                        else:
                            for orig, repl in mapping.items():
                                if orig.endswith('(') and func_name == orig[:-1].lower():
                                    if repl.endswith('('):
                                        node.set("this", sqlglot.exp.Identifier(this=repl[:-1], quoted=False))
                                    else:
                                        node.set("this", sqlglot.exp.Identifier(this=repl, quoted=False))
                                    break
                                elif orig.endswith('()') and func_name == orig[:-2].lower():
                                    node.set("this", sqlglot.exp.Identifier(this=repl, quoted=False))
                                    break
                elif func_name + "()" in func_name_map:
                    mapped = func_name_map[func_name + "()"]
                    return sqlglot.exp.Anonymous(this=mapped)
            return node

        def convert_string_concatenation(node):
            if isinstance(node, sqlglot.exp.Add):
                left = node.left
                right = node.right
                is_left_string = left.is_string or (isinstance(left, sqlglot.exp.Cast) and left.to.this.name.upper() in ('VARCHAR', 'CHAR', 'TEXT', 'NVARCHAR', 'NCHAR', 'UNIVARCHAR', 'UNICHAR'))
                is_right_string = right.is_string or (isinstance(right, sqlglot.exp.Cast) and right.to.this.name.upper() in ('VARCHAR', 'CHAR', 'TEXT', 'NVARCHAR', 'NCHAR', 'UNIVARCHAR', 'UNICHAR'))

                if is_left_string or is_right_string:
                    new_left = left
                    new_right = right
                    if not is_left_string:
                         new_left = sqlglot.exp.Cast(this=left, to=sqlglot.exp.DataType.build('text'))
                    if not is_right_string:
                         new_right = sqlglot.exp.Cast(this=right, to=sqlglot.exp.DataType.build('text'))
                    return sqlglot.exp.DPipe(this=new_left, expression=new_right)
            return node

        def convert_numeric_literals_to_strings(node):
            if isinstance(node, sqlglot.exp.Binary):
                left, right = node.left, node.right
                if isinstance(left, sqlglot.exp.Column) and isinstance(right, sqlglot.exp.Literal) and not right.args.get("is_string"):
                    right.args["is_string"] = True
                elif isinstance(right, sqlglot.exp.Column) and isinstance(left, sqlglot.exp.Literal) and not left.args.get("is_string"):
                    left.args["is_string"] = True
            elif isinstance(node, sqlglot.exp.DecodeCase):
                for i in range(1, len(node.expressions) - 1, 2):
                    search_val = node.expressions[i]
                    if isinstance(search_val, sqlglot.exp.Literal) and not search_val.args.get("is_string"):
                        search_val.args["is_string"] = True
            return node

        view_code = settings['view_code']
        converted_code = view_code

        remote_subs = self.config_parser.get_remote_objects_substitution()
        if remote_subs:
            iterator = remote_subs.items() if isinstance(remote_subs, dict) else remote_subs
            for source_obj, target_obj in iterator:
                if source_obj and target_obj:
                    converted_code = re.sub(re.escape(source_obj), target_obj, converted_code, flags=re.IGNORECASE)

        if settings['target_db_type'] == 'postgresql':
            sql_functions_mapping = self.get_sql_functions_mapping({ 'target_db_type': settings['target_db_type'] })
            if sql_functions_mapping:
                for src_func, tgt_func in sql_functions_mapping.items():
                    escaped_src_func = re.escape(src_func)
                    if escaped_src_func.endswith(r'\(') or escaped_src_func.endswith(r'\)'):
                        converted_code = re.sub(rf"(?i)\b{escaped_src_func}", tgt_func, converted_code, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
                    else:
                        converted_code = re.sub(rf"(?i)\b{escaped_src_func}\b", tgt_func, converted_code, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)

            try:
                # Use default sqlglot dialect because 'db2' dialect is not supported
                parsed_code = sqlglot.parse_one(converted_code)
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: convert_view_code: Error parsing View code: {e}")
                # Fallback to the unparsed converted_code instead of empty string to avoid crashes
                return converted_code

            parsed_code = parsed_code.transform(quote_column_names)
            parsed_code = parsed_code.transform(convert_string_concatenation)
            parsed_code = parsed_code.transform(quote_schema_and_table_names)
            parsed_code = parsed_code.transform(replace_schema_names)
            parsed_code = parsed_code.transform(replace_functions)
            parsed_code = parsed_code.transform(convert_numeric_literals_to_strings)

            converted_code = parsed_code.sql(dialect="postgres")
            converted_code = converted_code.replace("()()", "()")

            self.config_parser.print_log_message('DEBUG', f"ibm_db2_luw_connector: convert_view_code: Converted view: {converted_code}")
        else:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: convert_view_code: Unsupported target database type: {settings['target_db_type']}")

        return converted_code

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
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: execute_query: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def execute_sql_script(self, script_path: str):
        try:
            with open(script_path, 'r') as file:
                script = file.read()
            cursor = self.connection.cursor()
            cursor.execute(script)
            cursor.close()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: execute_sql_script: Error executing script: {script_path}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def begin_transaction(self):
        self.connection.jconn.setAutoCommit(False)

    def commit_transaction(self):
        self.connection.commit()
        self.connection.jconn.setAutoCommit(True)

    def rollback_transaction(self):
        self.connection.rollback()

    def get_rows_count(self, table_schema: str, table_name: str, migration_limitation: str = None):
        query = f"""SELECT COUNT(*) FROM {table_schema.upper()}."{table_name}" """
        if migration_limitation:
            query += f" WHERE {migration_limitation}"
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: get_rows_count: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_table_size(self, table_schema: str, table_name: str):
        # Placeholder for fetching table size
        return 0

    def fetch_user_defined_types(self, schema: str):
        # Placeholder for fetching user-defined types
        return {}

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
        # Placeholder for fetching table description
        self.config_parser.print_log_message('DEBUG3', f"ibm_db2_luw_connector: get_table_description: IBM DB2 connector: Getting table description for {settings['table_schema']}.{settings['table_name']}")
        return { 'table_description': '' }

    def testing_select(self):
        return "SELECT 1 FROM SYSIBM.SYSDUMMY1"

    def get_database_version(self):
        try:
            query = "SELECT SERVICE_LEVEL FROM SYSIBMADM.ENV_INST_INFO"
            cursor = self.connection.cursor()
            cursor.execute(query)
            version = cursor.fetchone()[0]
            cursor.close()
            return version
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: get_database_version: Error fetching database version: {e}")
            raise

    def get_database_size(self):
        query = "SELECT SUM(DATA_OBJECT_P_SIZE + INDEX_OBJECT_P_SIZE) FROM SYSIBMADM.ADMINTABINFO WHERE TABSCHEMA = 'SYSIBM'"
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            size = cursor.fetchone()[0]
            cursor.close()
            return size
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: get_database_size: Error fetching database size: {e}")
            raise

    def get_top_n_tables(self, settings):
        top_tables = {}
        top_tables['by_rows'] = {}
        top_tables['by_size'] = {}
        top_tables['by_columns'] = {}
        top_tables['by_indexes'] = {}
        top_tables['by_constraints'] = {}

        source_schema_name = settings['source_schema_name']
        try:
            order_num = 1
            top_n = self.config_parser.get_top_n_tables_by_rows()
            if top_n > 0:
                query = f"""
                    SELECT
                    TABSCHEMA,
                    TABNAME,
                    STATS_ROWS_MODIFIED,
                    SUM(DATA_OBJECT_P_SIZE + INDEX_OBJECT_P_SIZE) AS TOTAL_SIZE
                    FROM SYSIBMADM.ADMINTABINFO
                    WHERE TABSCHEMA = upper('{source_schema_name}')
                    GROUP BY TABSCHEMA, TABNAME, STATS_ROWS_MODIFIED
                    ORDER BY STATS_ROWS_MODIFIED DESC
                    FETCH FIRST {top_n} ROWS ONLY
                """
                self.config_parser.print_log_message('DEBUG', f"ibm_db2_luw_connector: get_top_n_tables: Fetching top {top_n} tables by row count for schema {source_schema_name} with query: {query}")
                cursor = self.connection.cursor()
                cursor.execute(query)
                tables = cursor.fetchall()
                cursor.close()
                order_num = 1
                for row in tables:
                    top_tables['by_rows'][order_num] = {
                        'owner': row[0].strip(),
                        'table_name': row[1].strip(),
                        'row_count': row[2],
                        'row_size': None,
                        'table_size': row[3],
                    }
                    order_num += 1
                self.config_parser.print_log_message('DEBUG2', f"ibm_db2_luw_connector: get_top_n_tables: Top {top_n} tables BY ROWS: {top_tables}")
            else:
                self.config_parser.print_log_message('DEBUG', f"ibm_db2_luw_connector: get_top_n_tables: Top N tables by rows is set to 0, skipping fetching top tables by row count.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: get_top_n_tables: Error fetching top {top_n} tables by row count: {e}")

        return top_tables

    def target_table_exists(self, target_schema_name, target_table_name):
        exists = False
        query = f"""
            SELECT COUNT(*)
            FROM SYSCAT.TABLES
            WHERE TABSCHEMA = upper('{target_schema_name}') AND TABNAME = '{target_table_name}' AND TYPE = 'T'
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            exists = cursor.fetchone()[0] > 0
            cursor.close()
            self.disconnect()
            return exists
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_luw_connector: target_table_exists: Error checking if target table exists: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_top_fk_dependencies(self, settings):
        top_fk_dependencies = {}
        return top_fk_dependencies

    def fetch_all_rows(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def convert_default_value(self, settings) -> dict:
        extracted_default_value = settings['extracted_default_value']
        return extracted_default_value

    def get_table_checksum(self, schema_name: str, table_name: str, columns: list):
        return None

    def get_random_pks(self, schema_name: str, table_name: str, pk_columns: list, sample_size: int):
        return []

    def get_row_checksums(self, schema_name: str, table_name: str, pk_columns: list, pk_values_list: list, columns: list):
        return {}

    def get_lob_sizes(self, schema_name: str, table_name: str, pk_columns: list, pk_values_list: list, lob_columns: list):
        return {}

if __name__ == "__main__":
    print("This script is not meant to be run directly")
