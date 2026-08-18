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
import sqlanydb
import pyodbc
import traceback
from tabulate import tabulate
import time
import datetime
import re

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
        if self.config_parser.get_connectivity(self.source_or_target) == 'native':
            config = self.config_parser.get_db_config(self.source_or_target)
            self.connection = sqlanydb.connect(
                    userid=config['username'],
                    pwd=config['password'],
                    host=f"{config['host']}:{config['port']}",
                    dbn=config['database'])
            # self.connection = sqlanydb.connect(connection_string)
        elif self.config_parser.get_connectivity(self.source_or_target) == 'odbc':
            connection_string = self.config_parser.get_connect_string(self.source_or_target)
            self.config_parser.print_log_message('DEBUG', f"sql_anywhere_connector: connect: SQL Anywhere ODBC connection string: {connection_string}")
            self.connection = pyodbc.connect(connection_string)

    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
        except Exception as e:
            pass

    def get_sql_functions_mapping(self, settings):
        """ Returns a dictionary of SQL functions mapping for the target database """
        target_db_type = settings.get('target_db_type', 'postgresql')
        if target_db_type == 'postgresql':
            return {
                'current timestamp': 'CURRENT_TIMESTAMP',
                'current_timestamp': 'CURRENT_TIMESTAMP',
                'timestamp': 'CURRENT_TIMESTAMP',
                'current date': 'CURRENT_DATE',
                'current_date': 'CURRENT_DATE',
                'current time': 'CURRENT_TIME',
                'current_time': 'CURRENT_TIME',
                'current user': 'CURRENT_USER',
                'current_user': 'CURRENT_USER',
                'last user': 'CURRENT_USER',
                'current publisher': 'CURRENT_USER',
                'getutcdate()': "timezone('UTC', now())",
                'getdate()': 'CURRENT_TIMESTAMP',
                'now()': 'CURRENT_TIMESTAMP',
                'today()': 'CURRENT_DATE',
                'user_name()': 'CURRENT_USER',
                'user_id()': 'CURRENT_USER',
                'user': 'CURRENT_USER',
                'year(': 'extract(year from ',
                'month(': 'extract(month from ',
                'day(': 'extract(day from ',
                'len(': 'length(',
                'length(': 'length(',
                'isnull(': 'coalesce(',
                'ifnull(': 'coalesce(',
                'string(': 'concat(',
                'charindex(': 'position(',
                'locate(': 'position(',
                'stuff(': 'overlay(',
                'dateformat(': 'to_char(',
                'datepart(yyyy,': "date_part('year',",
                'datepart(year,': "date_part('year',",
                'datepart(month,': "date_part('month',",
                'datepart(yy,': "date_part('year',",
                'datepart(qq,': "date_part('quarter',",
                'datepart(mm,': "date_part('month',",
                'datepart(dy,': "date_part('doy',",
                'datepart(dd,': "date_part('day',",
                'datepart(wk,': "date_part('week',",
                'datepart(hh,': "date_part('hour',",
                'datepart(mi,': "date_part('minute',",
                'datepart(ss,': "date_part('second',",
                'datepart(ms,': "date_part('milliseconds',",
            }
        else:
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: get_sql_functions_mapping: Unsupported target database type: {target_db_type}")
            return {}

    def migrate_sequences(self, target_connector, settings):
        """
        One sequence of the source created in the target.

        A sequence which stands for an identity column is not created here - the column carries
        it in the target and the table migration sets its value once the rows are there.
        """
        target_schema_name = settings.get('target_schema_name', '')
        target_sequence_name = self.config_parser.convert_names_case(settings.get('target_sequence_name', ''))
        if not target_sequence_name or settings.get('source_table_name'):
            return True

        statement = [f'CREATE SEQUENCE IF NOT EXISTS "{target_schema_name}"."{target_sequence_name}"']
        if settings.get('source_increment_by') is not None:
            statement.append(f"INCREMENT BY {settings['source_increment_by']}")
        if settings.get('source_minvalue') is not None:
            statement.append(f"MINVALUE {settings['source_minvalue']}")
        if settings.get('source_maxvalue') is not None:
            statement.append(f"MAXVALUE {settings['source_maxvalue']}")
        if settings.get('source_start_value') is not None:
            statement.append(f"START WITH {settings['source_start_value']}")
        if settings.get('source_cache') is not None:
            ## SQL Anywhere writes NO CACHE as a cache of 0, PostgreSQL counts from 1
            statement.append(f"CACHE {max(int(settings['source_cache']), 1)}")
        statement.append('CYCLE' if settings.get('source_is_cycled') else 'NO CYCLE')

        try:
            target_connector.execute_query(' '.join(statement))
            return True
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: migrate_sequences: Error creating sequence {target_sequence_name}: {e}")
            return False

    def fetch_table_names(self, table_schema: str):
        query = f"""
            SELECT table_id, table_name
            FROM sys.systable
            WHERE creator in (SELECT DISTINCT user_id
            FROM sys.SYSUSERPERM where user_name = '{table_schema}')
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
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: fetch_table_names: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_table_columns(self, settings) -> dict:
        table_schema = settings['table_schema']
        table_name = settings['table_name']
        query = f"""
            SELECT
                c.column_id,
                c.column_name,
                d.domain_name,
                c.width,
                c.scale,
                c."nulls",
                c."default"
            FROM sys.syscolumn c
            LEFT JOIN SYS.SYSDOMAIN d ON d.domain_id = c.domain_id
            WHERE c.table_id = (
                SELECT t.table_id FROM sys.systable t
                WHERE t.creator in (
                    SELECT DISTINCT user_id
                    FROM sys.SYSUSERPERM where user_name = '{table_schema}'
                    )
                AND table_name = '{table_name}'
                )
            ORDER BY column_id
        """
        try:
            result = {}
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                column_id = row[0]
                column_name = row[1]
                domain_name = row[2]
                width = row[3]
                scale = row[4]
                nulls = row[5]
                default_value = row[6]
                column_type = domain_name
                if self.is_string_type(column_type) and width is not None and width > 0:
                    column_type += f"({width})"
                elif self.is_numeric_type(column_type) and width is not None and scale is not None:
                    column_type += f"({width}, {scale})"
                elif self.is_numeric_type(column_type) and width is not None:
                    column_type += f"({width})"
                result[column_id] = {
                    'column_name': column_name,
                    'data_type': domain_name,
                    'column_type': column_type,
                    'character_maximum_length': width if self.is_string_type(row[2]) else None,
                    'numeric_precision': width if self.is_numeric_type(row[2]) else None,
                    'numeric_scale': scale,
                    'is_nullable': 'NO' if nulls == 'N' else 'YES',
                    'is_identity': 'YES' if default_value is not None and default_value.upper() == 'AUTOINCREMENT' else 'NO',
                    'column_default_value': default_value if default_value is not None and default_value.upper() != 'AUTOINCREMENT' else None,
                    'column_comment': '',
                }
            cursor.close()
            self.disconnect()
            return result
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: fetch_table_columns: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_types_mapping(self, settings):
        target_db_type = settings['target_db_type']
        types_mapping = {}
        if target_db_type == 'postgresql':
            types_mapping = {
                'INTEGER': 'INTEGER',
                'INT': 'INTEGER',
                'UNSIGNED INT': 'BIGINT',
                'UNSIGNED INTEGER': 'BIGINT',
                'UNSIGNED BIGINT': 'NUMERIC(20, 0)',
                'UNSIGNED SMALLINT': 'INTEGER',
                'UNSIGNED TINYINT': 'SMALLINT',
                'VARCHAR': 'VARCHAR',
                'NVARCHAR': 'VARCHAR',
                'CHAR': 'CHAR',
                'NCHAR': 'CHAR',
                'DATE': 'DATE',
                'TIME': 'TIME',
                'DATETIME': 'TIMESTAMP',
                'SMALLDATETIME': 'TIMESTAMP',
                'TIMESTAMP': 'TIMESTAMP',
                'DECIMAL': 'DECIMAL',
                'NUMERIC': 'NUMERIC',
                'MONEY': 'NUMERIC(19, 4)',
                'SMALLMONEY': 'NUMERIC(10, 4)',
                'BINARY': 'BYTEA',
                'VARBINARY': 'BYTEA',
                'LONG VARBINARY': 'BYTEA',
                'LONG BINARY': 'BYTEA',
                'IMAGE': 'BYTEA',
                'BOOLEAN': 'BOOLEAN',
                'BIT': 'BOOLEAN',
                'VARBIT': 'BIT VARYING',
                'FLOAT': 'REAL',
                'DOUBLE': 'DOUBLE PRECISION',
                'DOUBLE PRECISION': 'DOUBLE PRECISION',
                'REAL': 'REAL',
                'SMALLINT': 'SMALLINT',
                'BIGINT': 'BIGINT',
                'TINYINT': 'SMALLINT',
                'TEXT': 'TEXT',
                'LONG VARCHAR': 'TEXT',
                'LONG NVARCHAR': 'TEXT',
                'UNICHAR': 'CHAR',
                'UNIVARCHAR': 'VARCHAR',
                'CLOB': 'TEXT',
                'BLOB': 'BYTEA',
                'XML': 'XML',
                'JSON': 'JSON',
                'UUID': 'UUID',
                'UNIQUEIDENTIFIER': 'UUID',
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
        source_table_rows_limited = 0
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

            target_table_rows = 0
            migration_limitation = settings['migration_limitation']
            chunk_size = settings['chunk_size']
            chunk_number = settings['chunk_number']
            resume_after_crash = settings['resume_after_crash']
            drop_unfinished_tables = settings['drop_unfinished_tables']

            source_table_rows_all = settings.get('source_table_rows_all', 0)

            source_table_rows_limited = self.get_rows_count(source_schema_name, source_table_name, migration_limitation)
            target_table_rows = migrate_target_connection.get_rows_count(target_schema_name, target_table_name)

            total_chunks = self.config_parser.get_total_chunks(source_table_rows_limited, chunk_size)
            if chunk_size == -1:
                chunk_size = source_table_rows_limited + 1

            migration_stats = {
                'rows_migrated': target_table_rows,
                'chunk_number': chunk_number,
                'total_chunks': total_chunks,
                'source_table_rows_all': source_table_rows_all,

                'source_table_rows_limited': source_table_rows_limited,
                'target_table_rows': target_table_rows,
                'finished': True if source_table_rows_limited == 0 else False,
            }

            protocol_id = migrator_tables.insert_data_migration({
                'worker_id': worker_id,
                'source_table_id': source_table_id,
                'source_schema_name': source_schema_name,
                'source_table_name': source_table_name,
                'target_schema_name': target_schema_name,
                'target_table_name': target_table_name,
                'source_table_rows_all': source_table_rows_all,

                'source_table_rows_limited': source_table_rows_limited,
                'target_table_rows': target_table_rows,
            })

            if source_table_rows_limited == 0:
                self.config_parser.print_log_message('INFO', f"sql_anywhere_connector: migrate_table: Worker {worker_id}: Table {source_table_name} is empty - skipping data migration.")
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

                data_conflict_action = settings.get('data_conflict_action')
                if target_table_rows == 0 or data_conflict_action in ('merge_keep_target', 'merge_keep_source', 'replace'):
                    migrator_tables.update_data_migration_started(protocol_id)

                    self.config_parser.print_log_message('INFO', f"sql_anywhere_connector: migrate_table: Worker {worker_id}: Source table {source_table_name}: {source_table_rows_limited} rows / Target table {target_table_name}: {target_table_rows} rows - starting data migration.")

                    select_columns_list = []
                    orderby_columns_list = []
                    insert_columns_list = []
                    for order_num, col in source_columns.items():
                        self.config_parser.print_log_message('DEBUG2',
                                                            f"Worker {worker_id}: Table {source_schema_name}.{source_table_name}: Processing column {col['column_name']} ({order_num}) with data type {col['data_type']}")
                        insert_columns_list.append(f'''"{self.config_parser.convert_names_case(col['column_name'])}"''')
                        orderby_columns_list.append(f'''"{col['column_name']}"''')

                        # if col['data_type'].lower() == 'datetime':
                        #     select_columns_list.append(f"TO_CHAR({col['column_name']}, '%Y-%m-%d %H:%M:%S') as {col['column_name']}")
                        #     select_columns_list.append(f"ST_asText(`{col['column_name']}`) as `{col['column_name']}`")
                        # elif col['data_type'].lower() == 'set':
                        #     select_columns_list.append(f"cast(`{col['column_name']}` as char(4000)) as `{col['column_name']}`")
                        # else:
                        select_columns_list.append(f'''"{col['column_name']}"''')

                    select_columns = ', '.join(select_columns_list)
                    orderby_columns = ', '.join(orderby_columns_list)
                    insert_columns = ', '.join(insert_columns_list)

                    if resume_after_crash and not drop_unfinished_tables:
                        chunk_number = self.config_parser.get_total_chunks(target_table_rows, chunk_size)
                        self.config_parser.print_log_message('DEBUG', f"sql_anywhere_connector: migrate_table: Worker {worker_id}: Resuming migration for table {source_schema_name}.{source_table_name} from chunk {chunk_number} with data chunk size {chunk_size}.")
                        chunk_offset = target_table_rows
                    else:
                        chunk_offset = (chunk_number - 1) * chunk_size

                    chunk_start_row_number = chunk_offset + 1
                    chunk_end_row_number = chunk_offset + chunk_size

                    self.config_parser.print_log_message('DEBUG', f"sql_anywhere_connector: migrate_table: Worker {worker_id}: Migrating table {source_schema_name}.{source_table_name}: chunk {chunk_number}, data chunk size {chunk_size}, batch size {batch_size}, chunk offset {chunk_offset}, chunk end row number {chunk_end_row_number}, source table rows {source_table_rows_limited}")
                    order_by_clause = ''

                    part_name = 'fetch_data'
                    query = f"SELECT TOP {chunk_size} START AT {chunk_start_row_number} {select_columns} FROM {source_schema_name}.{source_table_name}"
                    if migration_limitation:
                        query += f" WHERE {migration_limitation}"
                    primary_key_columns = migrator_tables.select_primary_key({'source_schema_name': source_schema_name, 'source_table_name': source_table_name})
                    self.config_parser.print_log_message('DEBUG2', f"sql_anywhere_connector: migrate_table: Worker {worker_id}: Primary key columns for {source_schema_name}.{source_table_name}: {primary_key_columns}")
                    if primary_key_columns:
                        orderby_columns = primary_key_columns
                    order_by_clause = f""" ORDER BY {orderby_columns}"""

                    self.config_parser.print_log_message('DEBUG', f"sql_anywhere_connector: migrate_table: Worker {worker_id}: Fetching data with cursor using query: {query}")

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
                        part_name = 'fetch_data_batch'
                        records = cursor.fetchmany(batch_size)
                        if not records:
                            break
                        batch_number += 1
                        reading_end_time = time.time()
                        reading_duration = reading_end_time - reading_start_time
                        self.config_parser.print_log_message('DEBUG', f"sql_anywhere_connector: migrate_table: Worker {worker_id}: Fetched {len(records)} rows (batch {batch_number}) from source table '{source_table_name}' using cursor")

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
                                    record[column_name] = bytes(record[column_name].getBytes(1, int(record[column_name].length())))  # Convert 'com.informix.jdbc.IfxCblob' to bytes
                                elif column_type.lower() in ['clob']:
                                    # elif isinstance(record[column_name], IfxCblob):
                                    record[column_name] = record[column_name].getSubString(1, int(record[column_name].length()))  # Convert IfxCblob to string
                                    # record[column_name] = bytes(record[column_name].getBytes(1, int(record[column_name].length())))  # Convert IfxBblob to bytes
                                    # record[column_name] = record[column_name].read()  # Convert IfxBblob to bytes
                                elif column_type.lower() in ['integer', 'smallint', 'tinyint', 'bit', 'boolean'] and target_column_type.lower() in ['boolean']:
                                    # Convert integer to boolean
                                    record[column_name] = bool(record[column_name])

                        self.config_parser.print_log_message('DEBUG', f"sql_anywhere_connector: migrate_table: Worker {worker_id}: Starting insert of {len(records)} rows from source table {source_table_name}")
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
                            'data_conflict_action': data_conflict_action,
                            'primary_key_columns': primary_key_columns,
                        })
                        total_inserted_rows += inserted_rows
                        inserting_end_time = time.time()
                        inserting_duration = inserting_end_time - inserting_start_time

                        batch_end_time = time.time()
                        batch_duration = batch_end_time - batch_start_time
                        batch_durations.append(batch_duration)
                        percent_done = round(total_inserted_rows / source_table_rows_limited * 100, 2)

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
                            f"(total: {total_inserted_rows} from: {source_table_rows_limited} "
                            f"({percent_done}%)) rows into target table '{target_table_name}': "
                            f"Batch {batch_number} duration: {batch_duration:.2f} seconds "
                            f"(r: {reading_duration:.2f}, t: {transforming_duration:.2f}, w: {inserting_duration:.2f})"
                        )
                        self.config_parser.print_log_message('INFO', msg)

                        batch_start_time = time.time()
                        reading_start_time = batch_start_time

                    target_table_rows = migrate_target_connection.get_rows_count(target_schema_name, target_table_name)
                    self.config_parser.print_log_message('INFO', f"sql_anywhere_connector: migrate_table: Worker {worker_id}: Target table {target_schema_name}.{target_table_name} has {target_table_rows} rows")

                    shortest_batch_seconds = min(batch_durations) if batch_durations else 0
                    longest_batch_seconds = max(batch_durations) if batch_durations else 0
                    average_batch_seconds = sum(batch_durations) / len(batch_durations) if batch_durations else 0
                    self.config_parser.print_log_message('INFO', f"sql_anywhere_connector: migrate_table: Worker {worker_id}: Migrated {total_inserted_rows} rows from {source_table_name} to {target_schema_name}.{target_table_name} in {batch_number} batches: "
                                                            f"Shortest batch: {shortest_batch_seconds:.2f} seconds, "
                                                            f"Longest batch: {longest_batch_seconds:.2f} seconds, "
                                                            f"Average batch: {average_batch_seconds:.2f} seconds")

                    cursor.close()

                else:
                    self.config_parser.print_log_message('INFO', f"sql_anywhere_connector: migrate_table: Worker {worker_id}: Target table {target_table_name} has {target_table_rows} rows and data_conflict_action is '{data_conflict_action}'. Skipping data migration.")

                migration_stats = {
                    'rows_migrated': total_inserted_rows,
                    'chunk_number': chunk_number,
                    'total_chunks': total_chunks,
                    'source_table_rows_all': source_table_rows_all,

                    'source_table_rows_limited': source_table_rows_limited,
                    'target_table_rows': target_table_rows,
                    'finished': False,
                }

                self.config_parser.print_log_message('DEBUG', f"sql_anywhere_connector: migrate_table: Worker {worker_id}: Migration stats: {migration_stats}")
                if source_table_rows_limited <= target_table_rows or chunk_number >= total_chunks:
                    self.config_parser.print_log_message('DEBUG3', f"sql_anywhere_connector: migrate_table: Worker {worker_id}: Setting migration status to finished for table {source_table_name} (chunk {chunk_number}/{total_chunks})")
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
                    'source_table_rows_all': source_table_rows_all,

                    'source_table_rows_limited': source_table_rows_limited,
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
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: migrate_table: Worker {worker_id}: Error during {part_name} -> {e}")
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: migrate_table: Worker {worker_id}: Full stack trace: {traceback.format_exc()}")
            raise e


    def fetch_indexes(self, settings):
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

        table_indexes = {}
        order_num = 1
        query = f"""
            SELECT
                iname,
                indextype,
                colnames
            FROM SYS.SYSINDEXES
            WHERE creator = '{source_table_schema}'
            AND tname = '{source_table_name}'
            ORDER BY iname
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                index_name = row[0]
                index_type = row[1].upper()
                index_columns = row[2]

                if index_type == 'NON-UNIQUE':
                    index_type = 'INDEX'

                if index_type == 'PRIMARY KEY':
                    index_columns = index_columns.replace(" ASC", "").replace(" DESC", "")

                columns = []
                for col in index_columns.split(","):
                    col = col.strip()
                    if col.upper().endswith(" ASC"):
                        col_name = col[:-4].strip()
                        columns.append(f'"{col_name}" ASC')
                    elif col.upper().endswith(" DESC"):
                        col_name = col[:-5].strip()
                        columns.append(f'"{col_name}" DESC')
                    else:
                        columns.append(f'"{col}"')
                index_columns = ', '.join(columns)
                if index_type != 'FOREIGN KEY':
                    table_indexes[order_num] = {
                        'index_name': index_name,
                        'index_owner': source_table_schema,
                        'index_type': index_type,
                        'index_columns': index_columns,
                        'index_comment': '',
                    }
                    order_num += 1
            cursor.close()
            self.disconnect()

            return table_indexes
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: fetch_indexes: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_index_sql(self, settings):
        return ""

    ## SYS.SYSTRIGGER stores the referential integrity actions of a foreign key.
    ## event: 'C' = ON UPDATE, 'D' = ON DELETE
    ## referential_action: 'C' = CASCADE, 'N' = SET NULL, 'D' = SET DEFAULT, 'R' = RESTRICT
    SA_REFERENTIAL_ACTIONS = {
        'C': 'CASCADE',
        'N': 'SET NULL',
        'D': 'SET DEFAULT',
        'R': 'RESTRICT',
    }

    def fetch_constraints(self, settings):
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

        order_num = 1
        table_constraints = {}
        ## In SYS.SYSFOREIGNKEYS the "foreign" table is the child - the one carrying the
        ## foreign key columns and therefore the one the constraint has to be created on.
        ## The "primary" table is the parent being referenced. The constraints are fetched
        ## per table, so we select the rows where the current table is the foreign one.
        query = f"""
            SELECT
                "role" as fk_name,
                primary_creator,
                primary_tname,
                foreign_creator,
                foreign_tname,
                columns,
                count(*) over (partition by "role") fk_name_uniqueness,
                row_number() over (partition by "role" order by s.primary_tname) as fk_name_ordinal_number
            FROM SYS.SYSFOREIGNKEYS s
            WHERE foreign_creator = '{source_table_schema}'
            AND foreign_tname = '{source_table_name}'
            ORDER BY "role"
        """
        ## The role of a foreign key is the name of its index on the foreign table,
        ## which is how the referential actions are matched to the constraint.
        actions_query = f"""
            SELECT i.index_name, t.event, t.referential_action
            FROM SYS.SYSTRIGGER t
            JOIN SYS.SYSTAB tb ON tb.table_id = t.foreign_table_id
            JOIN SYS.SYSUSER u ON u.user_id = tb.creator
            JOIN SYS.SYSIDX i ON i.table_id = t.foreign_table_id AND i.index_id = t.foreign_key_id
            WHERE t.foreign_table_id IS NOT NULL
            AND u.user_name = '{source_table_schema}'
            AND tb.table_name = '{source_table_name}'
        """
        try:
            self.connect()
            cursor = self.connection.cursor()

            referential_actions = {}
            cursor.execute(actions_query)
            for row in cursor.fetchall():
                fk_index_name, event, referential_action = row[0], row[1], row[2]
                action = self.SA_REFERENTIAL_ACTIONS.get(referential_action, '')
                if not action:
                    continue
                rules = referential_actions.setdefault(fk_index_name, {})
                if event == 'D':
                    rules['delete_rule'] = action
                elif event == 'C':
                    rules['update_rule'] = action

            cursor.execute(query)
            for row in cursor.fetchall():
                fk_index_name = row[0]
                constraint_name = f"{fk_index_name}_fk"
                constraint_type = 'FOREIGN KEY'
                primary_table_schema = row[1]
                primary_table_name = row[2]
                sa_columns = row[5]

                fk_name_uniqueness = row[6]
                fk_name_ordinal_number = row[7]
                if fk_name_uniqueness > 1:
                    constraint_name = f"{constraint_name}{fk_name_ordinal_number}"

                ## "columns" holds the column pairs as "foreign_column IS primary_column",
                ## separated by commas for multi-column foreign keys. Both sides have to keep
                ## their order, so the pairs must not be deduplicated or sorted.
                fk_columns = []
                pk_columns = []
                for column_pair in sa_columns.split(","):
                    if " IS " not in column_pair:
                        continue
                    foreign_column, primary_column = column_pair.split(" IS ", 1)
                    for column, target_list in ((foreign_column, fk_columns), (primary_column, pk_columns)):
                        column = column.strip().replace(" ASC", "").replace(" DESC", "")
                        target_list.append('"'+column+'"')

                if not fk_columns:
                    self.config_parser.print_log_message('WARNING',
                        f"sql_anywhere_connector: fetch_constraints: Skipping foreign key {constraint_name} "
                        f"on table {source_table_schema}.{source_table_name} - unexpected column list '{sa_columns}'.")
                    continue

                rules = referential_actions.get(fk_index_name, {})
                table_constraints[order_num] = {
                    'constraint_name': constraint_name,
                    'constraint_type': constraint_type,
                    'constraint_owner': source_table_schema,
                    'constraint_columns': ','.join(fk_columns),
                    'referenced_table_schema': primary_table_schema,
                    'referenced_table_name': primary_table_name,
                    'referenced_columns': ','.join(pk_columns),
                    'delete_rule': rules.get('delete_rule', 'NO ACTION'),
                    'update_rule': rules.get('update_rule', 'NO ACTION'),
                    'constraint_sql': '',
                    'constraint_comment': '',
                }
                order_num += 1
            cursor.close()
            self.disconnect()

            return table_constraints
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: fetch_constraints: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_constraint_sql(self, settings):
        return ""

    def get_aliases(self, settings):
        return {}

    ## -----------------------------------------------------------------------------------
    ## Routines and triggers
    ##
    ## SQL Anywhere keeps the whole text of a procedure, a function and a trigger in the
    ## catalog - SYS.SYSPROCEDURE.proc_defn and SYS.SYSTRIGGER.trigger_defn - normalized by
    ## the server: every identifier is quoted, every keyword is lowercased and the comments
    ## of the script which created the object are kept where they stood, which for an object
    ## defined behind a comment block means between CREATE PROCEDURE and its name.
    ##
    ## The server accepts two procedural dialects and decides from the syntax which one it
    ## reads: Watcom-SQL (BEGIN/END blocks, DECLARE, SIGNAL, FOR-cursor loops) and
    ## Transact-SQL (@parameters, an AS body, the "inserted"/"deleted" pseudo tables). The
    ## conversion below reads Watcom-SQL, which is the dialect close enough to PL/pgSQL to be
    ## translated statement by statement, and refuses what it cannot express instead of
    ## producing a routine which does less than the one of the source did.
    ## -----------------------------------------------------------------------------------

    MANUAL_ADJUSTMENT_MARKER = 'MANUAL ADJUSTMENT REQUIRED'

    ## SYS.SYSPROCPARM.parm_type - what one row of the parameter list of a routine describes
    SA_PARM_PARAMETER = 0
    SA_PARM_RESULT_COLUMN = 1
    SA_PARM_RETURN_VALUE = 4

    ## The type names which keep the width SQL Anywhere stores beside them. Behind every
    ## other type that width is the size in bytes - an INTEGER carries width 4 - and putting
    ## it into the target would turn the type into INTEGER(4), which PostgreSQL rejects.
    SA_TYPES_WITH_LENGTH = ('CHAR', 'CHARACTER', 'VARCHAR', 'NCHAR', 'NVARCHAR',
                            'BINARY', 'VARBINARY')
    SA_TYPES_WITH_PRECISION = ('NUMERIC', 'DECIMAL')

    ## A string literal of the source is masked before the conversion runs over the code and
    ## restored afterwards, so that no pattern matches inside it. A routine building dynamic
    ## SQL holds whole statements in its literals - p_purge_events assembles a DELETE - and
    ## converting the text inside them would rewrite the statement the routine sends at run
    ## time. The character is one no SQL text contains.
    SA_LITERAL_PLACEHOLDER = '\x01lit{}\x01'

    ## What the conversion cannot express in PL/pgSQL. Each entry is a pattern searched in
    ## the body of a routine or a trigger and the reason written into the report - a routine
    ## which hits one of them is not created in the target.
    SA_UNSUPPORTED_CONSTRUCTS = (
        (r'(?is)\bfor\s+\S+\s+as\s+\S+\s+cursor\s+for\b',
         'a FOR ... AS ... CURSOR FOR loop - the columns of its query are read as bare '
         'variables, which PL/pgSQL reaches through the record of the loop'),
        (r'(?is)\bdeclare\s+[^;]*\bcursor\s+for\b',
         'a declared cursor together with OPEN / FETCH / @@sqlstatus'),
        (r'(?i)\bsavepoint\b',
         'a SAVEPOINT - a function of PostgreSQL runs inside the transaction of its caller '
         'and cannot control it'),
        (r'(?i)\brollback\b',
         'a ROLLBACK - a function of PostgreSQL cannot end the transaction of its caller'),
        (r'(?i)\bon\s+existing\s+update\b',
         'INSERT ... ON EXISTING UPDATE - the columns it matches on are the primary key of '
         'the table, which the conversion does not read'),
        (r'(?i)\bcreate\s+or\s+replace\s+variable\b',
         'a connection scope variable (CREATE OR REPLACE VARIABLE), which PostgreSQL does '
         'not have'),
        (r'(?i)"?varexists"?\s*\(',
         'VAREXISTS() - it reads a connection scope variable, which PostgreSQL does not have'),
        (r'(?i)\bfrom\s+"?(?:inserted|deleted)"?\b|\b(?:inserted|deleted)"?\.',
         'the Transact-SQL pseudo tables "inserted" / "deleted", which hold the changed rows '
         'as a set'),
        (r'(?i)@@(?:identity|sqlstatus|error)\b',
         'a Transact-SQL global variable (@@identity, @@sqlstatus or @@error)'),
        (r'(?i)\bset\s+self_recursion\b',
         'SET SELF_RECURSION, which switches the recursion of a trigger on'),
        (r'(?i)\bwhile\s*[("]',
         'a WHILE loop reading a cursor'),
    )

    def sa_variable_name(self, name):
        """
        The name of a parameter or a variable as PostgreSQL can carry it.

        A routine written in Transact-SQL names them @order_id, which is not an identifier of
        PostgreSQL. Dropping the sign alone is not enough: the name of a parameter of the
        source is often the name of a column as well, and @order_id and order_id are two
        different things there while a bare order_id in PL/pgSQL would be ambiguous. The
        prefix keeps them apart, the same way the MS SQL connector does it.
        """
        return re.sub(r'^@', 'locvar_', (name or '').strip())

    def sa_mask_literals(self, code):
        """ The code with every string literal replaced by a placeholder, and the literals. """
        literals = []
        masked = []
        position = 0
        length = len(code)
        while position < length:
            character = code[position]
            if character == "'":
                end = position + 1
                while end < length:
                    if code[end] == "'":
                        ## '' inside a literal is an escaped quote and does not end it
                        if end + 1 < length and code[end + 1] == "'":
                            end += 2
                            continue
                        break
                    end += 1
                literals.append(code[position:end + 1])
                masked.append(self.SA_LITERAL_PLACEHOLDER.format(len(literals) - 1))
                position = end + 1
            else:
                masked.append(character)
                position += 1
        return ''.join(masked), literals

    def sa_unmask_literals(self, code, literals):
        for index, literal in enumerate(literals):
            code = code.replace(self.SA_LITERAL_PLACEHOLDER.format(index), literal)
        return code

    def sa_convert_type(self, type_name, width, scale, target_db_type):
        """ One type of the SQL Anywhere catalog as the type of the target. """
        source_type = (type_name or '').strip().upper()
        types_mapping = self.get_types_mapping({'target_db_type': target_db_type})
        target_type = types_mapping.get(source_type, source_type)

        ## a mapping which already carries its own precision - MONEY becomes NUMERIC(19, 4) -
        ## is complete, the width of the source has no meaning for it any more
        if '(' in target_type:
            return target_type
        if source_type in self.SA_TYPES_WITH_LENGTH and width:
            return f"{target_type}({width})"
        if source_type in self.SA_TYPES_WITH_PRECISION and width:
            return f"{target_type}({width}, {scale or 0})" if scale else f"{target_type}({width})"
        return target_type

    def sa_fetch_routine_parameters(self, proc_id, target_db_type):
        """
        The parameters of one routine, read from SYS.SYSPROCPARM.

        parm_type says what a row describes: the return value of a function, a column of the
        RESULT clause of a procedure, or an ordinary parameter, whose direction is given by
        parm_mode_in / parm_mode_out.
        """
        query = f"""
            SELECT pp.parm_id, pp.parm_type, pp.parm_mode_in, pp.parm_mode_out,
                   pp.parm_name, pp.width, pp.scale, d.domain_name
            FROM SYS.SYSPROCPARM pp
            LEFT JOIN SYS.SYSDOMAIN d ON d.domain_id = pp.domain_id
            WHERE pp.proc_id = {proc_id}
            ORDER BY pp.parm_id
        """
        parameters = []
        cursor = self.connection.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            parm_type, mode_in, mode_out = row[1], row[2], row[3]
            if mode_in == 'Y' and mode_out == 'Y':
                direction = 'INOUT'
            elif mode_out == 'Y':
                direction = 'OUT'
            else:
                direction = 'IN'
            parameters.append({
                'parm_type': parm_type,
                'direction': direction,
                'name': self.sa_variable_name(row[4] or ''),
                'type': self.sa_convert_type(row[7], row[5], row[6], target_db_type),
            })
        cursor.close()
        return parameters

    def fetch_funcproc_names(self, schema: str):
        query = f"""
            SELECT p.proc_id, p.proc_name, p.remarks
            FROM SYS.SYSPROCEDURE p
            JOIN SYS.SYSUSER u ON u.user_id = p.creator
            WHERE u.user_name = '{schema}'
            ORDER BY p.proc_name
        """
        funcprocs = {}
        order_num = 1
        target_db_type = self.config_parser.get_target_db_type()
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            for row in rows:
                proc_id = row[0]
                parameters = self.sa_fetch_routine_parameters(proc_id, target_db_type)
                ## only a function has a return value in the parameter list - that row is
                ## what tells a function and a procedure apart, SYSPROCEDURE has no flag
                is_function = any(parameter['parm_type'] == self.SA_PARM_RETURN_VALUE
                                  for parameter in parameters)
                ## a procedure with a RESULT clause hands back a result set and becomes a
                ## function returning a table - PostgreSQL has no procedure which does that
                has_result_set = any(parameter['parm_type'] == self.SA_PARM_RESULT_COLUMN
                                     for parameter in parameters)
                arguments = ', '.join(parameter['type'] for parameter in parameters
                                      if parameter['parm_type'] == self.SA_PARM_PARAMETER)
                funcprocs[order_num] = {
                    'name': row[1],
                    'id': proc_id,
                    'type': 'FUNCTION' if (is_function or has_result_set) else 'PROCEDURE',
                    'comment': row[2] or '',
                    'arguments': arguments,
                }
                order_num += 1
            self.disconnect()
            return funcprocs
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: fetch_funcproc_names: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_funcproc_code(self, funcproc_id: int):
        """
        The definition of one routine together with its parameters.

        The parameters are read here and not in the conversion, which is given only the name
        of the routine: they carry the types and the directions the target signature is built
        from, and reading them from the catalog is more reliable than reading them out of the
        text, where a domain stands under the name it was declared with.
        """
        query = f"SELECT proc_defn FROM SYS.SYSPROCEDURE WHERE proc_id = {funcproc_id}"
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            definition = row[0] if row and row[0] else ''
            parameters = self.sa_fetch_routine_parameters(
                funcproc_id, self.config_parser.get_target_db_type())
            self.disconnect()
            return {'definition': definition, 'id': funcproc_id, 'parameters': parameters}
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: fetch_funcproc_code: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def sa_find_unsupported(self, code, object_name, object_kind):
        """ The reasons why this code cannot be turned into PL/pgSQL, as a list. """
        reasons = []
        for pattern, reason in self.SA_UNSUPPORTED_CONSTRUCTS:
            if re.search(pattern, code):
                reasons.append(reason)
                self.config_parser.print_log_message('WARNING',
                    f"sql_anywhere_connector: convert: {object_kind} {object_name} uses {reason}. "
                    f"It is not created in the target and has to be migrated by hand.")
        return reasons

    def sa_extract_declarations(self, body, context):
        """
        The DECLARE statements taken out of a Watcom block.

        Watcom-SQL declares a variable with a statement inside the block, PL/pgSQL declares it
        in a section in front of the block - the statements are collected here and the body is
        given back without them. A declared EXCEPTION is not a variable at all: it names a
        SQLSTATE, which SIGNAL raises later, so it is remembered and its declaration dropped.
        """
        remaining = []
        for line in body.split('\n'):
            stripped = line.strip().rstrip(';').strip()
            if not re.match(r'(?i)^declare\s+', stripped):
                remaining.append(line)
                continue

            declaration = re.sub(r'(?i)^declare\s+', '', stripped)
            exception_match = re.match(
                r'(?i)^"?([\w@]+)"?\s+exception\s+for\s+sqlstate\s+(?:value\s+)?(\S+)$', declaration)
            if exception_match:
                context['exceptions'][exception_match.group(1).lower()] = exception_match.group(2)
                continue

            variable_match = re.match(r'(?i)^"?([\w@]+)"?\s+(.+)$', declaration)
            if not variable_match:
                remaining.append(line)
                continue
            name = self.sa_variable_name(variable_match.group(1))
            type_text = variable_match.group(2).strip()
            type_match = re.match(r'(?i)^([a-z][a-z0-9_ ]*?)\s*(?:\(\s*([\d\s,]+)\s*\))?$', type_text)
            if type_match:
                arguments = [argument.strip() for argument in (type_match.group(2) or '').split(',') if argument.strip()]
                type_text = self.sa_convert_type(type_match.group(1).strip(),
                                                 arguments[0] if arguments else None,
                                                 arguments[1] if len(arguments) > 1 else None,
                                                 context['target_db_type'])
            context['declarations'].append(f"{name} {type_text}")
        return '\n'.join(remaining)

    def sa_convert_assignments(self, body, context):
        """
        SET of Watcom-SQL as the assignment of PL/pgSQL.

        "SET x = expression" assigns, "UPDATE t SET c = expression" writes a column, and both
        are written the same way - the SET of an UPDATE is found by walking the code from left
        to right and skipping the first SET behind every UPDATE. What every assignment writes
        into is remembered: a name which the object does not declare and does not take as a
        parameter is a connection scope variable, which PostgreSQL does not have.
        """
        result = []
        position = 0
        update_open = False
        for token in re.finditer(r'(?i)\b(update|set)\b', body):
            if token.group(1).lower() == 'update':
                update_open = True
                continue
            if update_open:
                update_open = False
                continue
            assignment = re.match(
                r'(?i)set\s+((?:NEW|OLD)\.)?("?[\w@]+"?)(\."?[\w]+"?)?\s*=\s*',
                body[token.start():])
            if not assignment:
                continue
            target = ''.join(part for part in assignment.groups() if part)
            target = re.sub(r'(?<![\w"])@(\w+)', r'locvar_\1', target)
            context.setdefault('assignment_targets', []).append(target)
            result.append(body[position:token.start()])
            result.append(f"{target} := ")
            position = token.start() + assignment.end()
        result.append(body[position:])
        return ''.join(result)

    def sa_convert_rowcount(self, body, context):
        """
        @@rowcount as GET DIAGNOSTICS.

        @@rowcount reports how many rows the statement in front of it changed. PL/pgSQL reads
        the same number with GET DIAGNOSTICS, which has to stand directly behind that
        statement - the diagnostics are therefore put in front of the statement which reads
        @@rowcount, where the statement whose rows are counted has just ended.
        """
        if not re.search(r'(?i)@@rowcount\b', body):
            return body
        context['declarations'].append('sa_row_count BIGINT')
        converted = []
        for statement in re.split(r'(?<=;)', body):
            if re.search(r'(?i)@@rowcount\b', statement):
                statement = re.sub(r'(?i)@@rowcount\b', 'sa_row_count', statement)
                leading_newlines = re.match(r'[\r\n]*', statement).group(0)
                statement = (f"{leading_newlines}    GET DIAGNOSTICS sa_row_count = ROW_COUNT;\n"
                             f"{statement[len(leading_newlines):]}")
            converted.append(statement)
        return ''.join(converted)

    def sa_convert_exception_section(self, body, context):
        """
        The EXCEPTION block of a Watcom routine as the one of PL/pgSQL.

        Both put their handlers at the end of the block and both name the condition behind
        WHEN - Watcom names the exception which was declared for a SQLSTATE, PL/pgSQL names
        the SQLSTATE itself.
        """
        section_match = re.search(r'(?im)^[ \t]*exception[ \t]*$', body)
        if not section_match:
            return body
        head, section = body[:section_match.start()], body[section_match.end():]
        ## the last statement of the block stands in front of EXCEPTION without a semicolon
        head = head.rstrip()
        if head and not head.endswith(';'):
            head += ';'

        def convert_handler(match):
            name = match.group(1).lower()
            if name == 'others':
                return 'WHEN OTHERS THEN'
            sqlstate = context['exceptions'].get(name)
            if not sqlstate:
                context['refusals'].append(
                    f"the handler WHEN {name} names an exception which was not declared in the "
                    f"routine, so the SQLSTATE it stands for is not known")
                return f"WHEN OTHERS THEN /* {self.MANUAL_ADJUSTMENT_MARKER}: was WHEN {name} */"
            return f"WHEN SQLSTATE {sqlstate} THEN"

        section = re.sub(r'(?is)\bwhen\s+"?([\w@]+)"?\s+then', convert_handler, section)
        ## the statement of a handler ends where the next handler begins
        section = re.sub(r'(?is)([^\s;])(\s*)(WHEN\s+(?:SQLSTATE|OTHERS))', r'\1;\2\3', section)
        return f"{head}\nEXCEPTION{section}"

    ## The block keywords of PL/pgSQL in front of which the statement before them has to be
    ## ended. The normalized text of the catalog writes each of them at the beginning of a
    ## line, which is what keeps ELSE of an IF apart from the ELSE of a CASE expression.
    SA_BLOCK_CLOSERS = ('end if', 'end loop', 'end case', 'elsif', 'else', 'end')

    def sa_terminate_statements(self, body):
        """
        The semicolons PL/pgSQL wants and Watcom-SQL leaves out.

        Watcom-SQL ends a statement with a semicolon only when another one follows it - the
        last statement of a block stands in front of END, END IF or ELSE without one, while
        PL/pgSQL ends every statement with one.
        """
        for keyword in self.SA_BLOCK_CLOSERS:
            pattern = keyword.replace(' ', r'\s+')
            body = re.sub(rf'(?is)([^\s;])(\s*[\r\n]\s*)({pattern}\b)', r'\1;\2\3', body)
        return body

    def sa_strip_comments(self, code):
        """
        The code without its comments.

        The text of the catalog keeps the comments of the script which created the object,
        and it keeps them where they stood - an object defined behind a comment block carries
        that block between CREATE PROCEDURE and its own name. They are removed before the
        conversion reads the code: a comment standing at the end of a block would take the
        semicolon which ends the statement in front of it, and the comments of the source stay
        readable in the protocol table, which keeps the definition as it was fetched.
        """
        code = re.sub(r'(?s)/\*.*?\*/', ' ', code)
        code = re.sub(r'(?m)--[^\n]*$', '', code)
        return code

    def sa_convert_top(self, body):
        """ SELECT TOP n of SQL Anywhere as the LIMIT clause of PostgreSQL. """
        converted = []
        for statement in re.split(r'(?<=;)', body):
            top_match = re.search(r'(?i)\bselect\s+top\s+("?[\w]+"?|\d+)\s', statement)
            if not top_match:
                converted.append(statement)
                continue
            limit = top_match.group(1).strip('"')
            statement = statement[:top_match.start()] + 'select ' + statement[top_match.end():]
            trailing = re.search(r'(\s*;?\s*)$', statement).group(1)
            statement = f"{statement[:len(statement) - len(trailing)]} LIMIT {limit}{trailing}"
            converted.append(statement)
        return ''.join(converted)

    def sa_strip_update_set_qualifiers(self, body):
        """
        The SET clause of an UPDATE without the table in front of its columns.

        SQL Anywhere writes the column of an UPDATE qualified - UPDATE "contact" SET
        "contact"."id" = ... - and PostgreSQL takes only the column there. The qualification
        of the WHERE clause is left alone, PostgreSQL reads it the same way as the source.
        """
        def strip_clause(match):
            clause = re.sub(r'(?i)(^|,)(\s*)"?[\w]+"?\s*\.\s*("?[\w]+"?\s*=)',
                            r'\1\2\3', match.group(2))
            return f"{match.group(1)}{clause}"

        return re.sub(r'(?is)(\bupdate\b\s+(?:"?[\w]+"?\s*\.\s*)?"?[\w]+"?'
                      r'(?:\s+as\s+"?[\w]+"?)?\s+set\s+)(.*?)(?=\bwhere\b|\bfrom\b|;|$)',
                      strip_clause, body)

    def sa_convert_case_statement(self, body):
        """
        The CASE statement of Watcom-SQL as the CASE statement of PL/pgSQL.

        Both branch on a value and end with END CASE, and the difference is the semicolon:
        Watcom-SQL ends the statements of a branch where the next WHEN begins, PL/pgSQL ends
        every one of them. A CASE expression is not touched - it ends with END, not END CASE.
        """
        def convert_statement(match):
            region = match.group(0)
            first_branch = re.search(r'(?is)\bwhen\b', region)
            if not first_branch:
                return region
            head, tail = region[:first_branch.start()], region[first_branch.start():]
            tail = re.sub(r'(?is)([^\s;])(\s*)(\bwhen\b|\belse\b|\bend\s+case\b)',
                          r'\1;\2\3', tail)
            return f"{head}{tail}"

        return re.sub(r'(?is)\bcase\b(?:(?!\bcase\b).)*?\bend\s+case\b',
                      convert_statement, body)

    def sa_check_assignment_targets(self, context, known_names, object_name, object_kind):
        """
        Whether every assignment of the converted code writes into something PostgreSQL knows.

        A name which the object neither declares nor takes as a parameter is a connection
        scope variable of SQL Anywhere - CREATE VARIABLE creates it and every routine of the
        connection sees it. PostgreSQL has nothing which lives that long, so the object is not
        created: it would otherwise be reported as migrated and fail when it runs.
        """
        known = {name.strip('"').lower() for name in known_names}
        for target in context.get('assignment_targets', []):
            name = target.split('.')[0].strip('"').lower()
            if name in ('new', 'old') or name in known:
                continue
            reason = (f"the assignment to {target}, a connection scope variable of the source - "
                      f"PostgreSQL has no variable which outlives the statement")
            if reason not in context['refusals']:
                context['refusals'].append(reason)
                self.config_parser.print_log_message('WARNING',
                    f"sql_anywhere_connector: convert: {object_kind} {object_name} writes into "
                    f"{target}, which it does not declare and does not take as a parameter. It is "
                    f"not created in the target and has to be migrated by hand.")

    ## YEAR(), MONTH() and their relatives give back a whole number in SQL Anywhere, and a
    ## routine divides that number: (MONTH(d) + 2) / 3 is the quarter of the year. EXTRACT of
    ## PostgreSQL gives back a numeric, where the same division is 2.333... instead of 2 - the
    ## result is cast so that the arithmetic behind it stays the arithmetic of the source.
    SA_DATE_PART_FUNCTIONS = {
        'year': 'YEAR', 'month': 'MONTH', 'day': 'DAY', 'dayofyear': 'DOY',
        'hour': 'HOUR', 'minute': 'MINUTE', 'second': 'SECOND',
        'quarter': 'QUARTER', 'week': 'WEEK',
    }

    def sa_read_parenthesized(self, code, start):
        """ What stands between the parenthesis at 'start' and its own closing one. """
        depth = 0
        for position in range(start, len(code)):
            if code[position] == '(':
                depth += 1
            elif code[position] == ')':
                depth -= 1
                if depth == 0:
                    return code[start + 1:position], position + 1
        return None, None

    def sa_convert_date_functions(self, code):
        """ YEAR(x) and its relatives as EXTRACT, giving back the whole number they gave. """
        names = '|'.join(self.SA_DATE_PART_FUNCTIONS)
        pattern = re.compile(rf'(?i)(?<![\w."]) ?({names})\s*\(')
        position = 0
        while True:
            match = pattern.search(code, position)
            if not match:
                return code
            inner, end = self.sa_read_parenthesized(code, match.end() - 1)
            if inner is None:
                position = match.end()
                continue
            field = self.SA_DATE_PART_FUNCTIONS[match.group(1).lower()]
            replacement = f"EXTRACT({field} FROM {inner})::integer"
            code = code[:match.start()] + replacement + code[end:]
            position = match.start() + len(replacement)

    def sa_convert_body(self, body, context):
        """
        The body of a Watcom-SQL block as the body of a PL/pgSQL block.

        The declarations the block carries are taken out of it - the caller puts them into the
        DECLARE section - and everything the two languages write differently is translated
        statement by statement. What the conversion cannot express was found before it ran and
        stands in context['refusals']; the code of such an object is converted as far as it
        goes and is not created in the target.
        """
        body, literals = self.sa_mask_literals(body)

        ## The catalog quotes the name of a function like every other identifier - "COUNT"(x)
        ## reaches PostgreSQL as a quoted name, which no function of the target carries. The
        ## name of a table stands in front of a parenthesis as well - INSERT INTO "t"( ... ) -
        ## and has to keep its quotes, or a table whose name is not lowercase is not found.
        def unquote_function_call(match):
            if re.search(r'(?i)\b(?:into|from|join|update|table)\s*$', body[:match.start()]):
                return match.group(0)
            return f"{match.group(1)}("

        body = re.sub(r'"([A-Za-z0-9_]+)"\s*\(', unquote_function_call, body)
        ## COUNT() of SQL Anywhere counts the rows, count(*) does the same in PostgreSQL
        body = re.sub(r'(?i)\bcount\s*\(\s*\)', 'count(*)', body)

        ## the row images of a trigger stand under the names the trigger gave them
        for alias, replacement in context.get('correlations', {}).items():
            body = re.sub(rf'(?i)(?<![\w".])"?{re.escape(alias)}"?\s*\.\s*"?([\w]+)"?',
                          rf'{replacement}."\1"', body)

        body = self.sa_extract_declarations(body, context)
        body = self.sa_strip_update_set_qualifiers(body)
        body = self.sa_convert_assignments(body, context)
        body = self.sa_convert_case_statement(body)

        ## a sequence is read as an object with a property, PostgreSQL reads it with a function
        body = re.sub(r'(?i)"?([\w]+)"?\s*\.\s*"?nextval"?', r"nextval('\1')", body)
        body = re.sub(r'(?i)"?([\w]+)"?\s*\.\s*"?currval"?', r"currval('\1')", body)

        body = re.sub(r'(?i)\bexecute\s+immediate\b', 'EXECUTE', body)
        body = re.sub(r'(?i)\belseif\b', 'ELSIF', body)

        ## SIGNAL raises the SQLSTATE the exception was declared for
        def convert_signal(match):
            name = match.group(1).lower()
            sqlstate = context['exceptions'].get(name)
            if not sqlstate:
                context['refusals'].append(
                    f"SIGNAL {name} names an exception which was not declared in the object, "
                    f"so the SQLSTATE it raises is not known")
                return f"RAISE EXCEPTION 'MANUAL ADJUSTMENT REQUIRED: SIGNAL {name}'"
            return f"RAISE EXCEPTION USING ERRCODE = {sqlstate}, MESSAGE = 'SIGNAL {name}'"

        body = re.sub(r'(?i)\bsignal\s+"?([\w]+)"?', convert_signal, body)
        body = re.sub(r'(?i)\bresignal\b', 'RAISE', body)

        ## RAISERROR raises an error of the application with a number of its own - PostgreSQL
        ## has no such number, the message keeps it so that it is not lost
        literal_pattern = re.escape(self.SA_LITERAL_PLACEHOLDER).replace(re.escape('{}'), r'\d+')

        def convert_raiserror(match):
            message = match.group(2)
            detail = f"SQL Anywhere RAISERROR {match.group(1)}"
            if message:
                return f"RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = {message}, DETAIL = '{detail}'"
            return f"RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = '{detail}'"

        body = re.sub(rf'(?i)\braiserror\s+(\d+)\s*({literal_pattern})?', convert_raiserror, body)
        body = re.sub(rf'(?i)\bmessage\s+({literal_pattern})\s+to\s+(?:client|console|log)\b',
                      r'RAISE NOTICE \1', body)

        body = self.sa_convert_top(body)
        body = self.sa_convert_date_functions(body)

        body = re.sub(r'(?<![\w"@])@(\w+)', r'locvar_\1', body)

        body = self.sa_terminate_statements(body)
        body = self.sa_convert_rowcount(body, context)
        body = self.sa_convert_exception_section(body, context)
        body = self.sa_unmask_literals(body, literals)
        body = self.apply_sql_functions_mapping(body, {'target_db_type': context['target_db_type']})
        body = body.strip()
        ## the last statement of the block stands in front of the END which was taken off with
        ## the header - there is no keyword left for sa_terminate_statements to find it by
        if body and not body.endswith(';'):
            body += ';'
        return body

    def sa_convert_result_set_query(self, body, result_columns, object_name):
        """
        The query of a RESULT procedure as the RETURN QUERY of a function.

        RETURN QUERY hands the rows back only when their types are exactly the ones the
        function declares, and the query of the source rarely delivers them: a concatenation
        gives back a text where the RESULT clause names a VARCHAR. The query is therefore
        wrapped and every column cast to the type the function was declared with.
        """
        statement = body.strip().rstrip(';').strip()
        ## more than one statement - the conversion cannot tell which of them is the result set
        if not re.match(r'(?is)^select\b', statement) or ';' in statement:
            self.config_parser.print_log_message('WARNING',
                f"sql_anywhere_connector: convert_funcproc_code: The body of {object_name} is not a "
                f"single query - its rows are returned as they are and their types have to match "
                f"the RESULT clause exactly.")
            return re.sub(r'(?im)^(\s*)(select\b)', r'\1RETURN QUERY \2', body, count=1)

        aliases = [f"sa_column_{index + 1}" for index in range(len(result_columns))]
        projection = ', '.join(f"sa_result.{alias}::{column['type']}"
                               for alias, column in zip(aliases, result_columns))
        ## the wrapping SELECT does nothing but cast, so the rows reach the caller in the order
        ## the query of the source put them in
        return (f"RETURN QUERY SELECT {projection}\n"
                f"FROM (\n{statement}\n) AS sa_result({', '.join(aliases)});")

    def sa_split_routine_header(self, definition, object_name):
        """
        The header of a routine and its body.

        Returns the keyword the routine was created with, whatever stands between the name and
        the body - the parameter list, RETURNS, RESULT - and the statements of the body.
        """
        definition = self.sa_strip_comments(definition).strip()
        header_match = re.match(
            r'(?is)create\s+(?:or\s+replace\s+)?(function|procedure)\s+'
            r'(?:"?[^".\s(]+"?\s*\.\s*)?"?[^".\s(]+"?\s*(.*)$', definition)
        if not header_match:
            return None, None, None
        keyword, tail = header_match.group(1).lower(), header_match.group(2)

        ## the body begins behind the last BEGIN of the header - a routine without one is a
        ## single statement, which becomes the only statement of the block
        body_match = re.search(r'(?is)\bbegin\b', tail)
        if not body_match:
            return keyword, tail.strip(), ''
        head = tail[:body_match.start()]
        body = tail[body_match.end():]
        end_match = None
        for end_match in re.finditer(r'(?is)\bend\b', body):
            pass
        if end_match:
            body = body[:end_match.start()]
        return keyword, head.strip(), body

    def convert_funcproc_code(self, settings):
        """
        One procedure or function of SQL Anywhere as a routine of PostgreSQL.

        The signature is built from the catalog and not from the text: SYS.SYSPROCPARM carries
        the type and the direction of every parameter, and a domain of the source stands there
        under the type it was declared with. A procedure with a RESULT clause becomes a
        function returning a table - it hands back a result set, which a procedure of
        PostgreSQL cannot do.
        """
        funcproc_code = settings['funcproc_code']
        funcproc_name = settings.get('funcproc_name', '')
        target_schema_name = settings['target_schema_name']
        target_db_type = settings.get('target_db_type', 'postgresql')

        definition = funcproc_code.get('definition', '') if isinstance(funcproc_code, dict) else str(funcproc_code or '')
        parameters = funcproc_code.get('parameters', []) if isinstance(funcproc_code, dict) else []
        if not definition.strip():
            return ''

        try:
            keyword, head, body = self.sa_split_routine_header(definition, funcproc_name)
            if keyword is None:
                self.config_parser.print_log_message('WARNING',
                    f"sql_anywhere_connector: convert_funcproc_code: The header of {funcproc_name} could "
                    f"not be read, the routine is not migrated.")
                return ''

            context = {
                'target_db_type': target_db_type,
                'declarations': [],
                'exceptions': {},
                'refusals': self.sa_find_unsupported(self.sa_strip_comments(definition),
                                                     funcproc_name, 'The routine'),
                'correlations': {},
            }
            converted_body = self.sa_convert_body(body, context)

            arguments = [parameter for parameter in parameters
                         if parameter['parm_type'] == self.SA_PARM_PARAMETER]
            result_columns = [parameter for parameter in parameters
                              if parameter['parm_type'] == self.SA_PARM_RESULT_COLUMN]
            return_values = [parameter for parameter in parameters
                             if parameter['parm_type'] == self.SA_PARM_RETURN_VALUE]

            target_name = self.config_parser.convert_names_case(funcproc_name)
            signature = ', '.join(f"{parameter['direction']} {parameter['name']} {parameter['type']}"
                                  for parameter in arguments)

            body_options = ''
            if return_values:
                object_keyword = 'FUNCTION'
                returns_clause = f"RETURNS {return_values[0]['type']}"
                ## a function of PostgreSQL has no OUT parameters beside its return value
                signature = ', '.join(f"{parameter['name']} {parameter['type']}"
                                      for parameter in arguments)
            elif result_columns:
                object_keyword = 'FUNCTION'
                columns = ', '.join(f"{column['name']} {column['type']}" for column in result_columns)
                returns_clause = f"RETURNS TABLE({columns})"
                signature = ', '.join(f"{parameter['name']} {parameter['type']}"
                                      for parameter in arguments)
                ## The columns of RETURNS TABLE are variables of the function, and they carry the
                ## names the RESULT clause gave them - which are the names of the columns the query
                ## reads. PL/pgSQL would answer such a query with "column reference is ambiguous";
                ## the option tells it to read a name which can be a column as the column, which is
                ## what the procedure of the source meant.
                body_options = '#variable_conflict use_column\n'
                outputs = [parameter['name'] for parameter in arguments
                           if parameter['direction'] != 'IN']
                if outputs:
                    self.config_parser.print_log_message('WARNING',
                        f"sql_anywhere_connector: convert_funcproc_code: The procedure {funcproc_name} "
                        f"hands back a result set and writes into its parameter(s) "
                        f"{', '.join(outputs)}. A function of PostgreSQL returning a table has no "
                        f"output parameters beside it - the value(s) written into them are lost and "
                        f"the caller has to be changed.")
                ## the procedure hands its rows back by running the query, the function has to
                ## return them
                converted_body = self.sa_convert_result_set_query(converted_body, result_columns,
                                                                  funcproc_name)
            else:
                object_keyword = 'PROCEDURE'
                returns_clause = ''

            self.sa_check_assignment_targets(
                context,
                [declaration.split()[0] for declaration in context['declarations']]
                + [parameter['name'] for parameter in parameters],
                funcproc_name, 'The routine')

            declare_section = ''
            if context['declarations']:
                declarations = '\n'.join(f"    {declaration};"
                                         for declaration in dict.fromkeys(context['declarations']))
                declare_section = f"DECLARE\n{declarations}\n"

            body_quote = '$$' if '$$' not in converted_body else '$sa_routine$'
            routine_sql = (f'CREATE OR REPLACE {object_keyword} "{target_schema_name}"."{target_name}"({signature})\n'
                           + (f'{returns_clause}\n' if returns_clause else '')
                           + f'LANGUAGE plpgsql\nAS {body_quote}\n'
                           f'{body_options}{declare_section}BEGIN\n{converted_body}\nEND;\n{body_quote};')

            if context['refusals']:
                ## The routine is not created: it would do less than the one of the source did,
                ## and the migration would report it as migrated. The converted code is kept -
                ## the protocol table stores it and it is what the migration by hand starts from -
                ## and the line in front of it makes PostgreSQL refuse the whole statement, so
                ## that the routine is reported as failed and cannot reach the target by accident.
                reasons = ''.join(f"     - {reason}\n" for reason in context['refusals'])
                self.config_parser.print_log_message('WARNING',
                    f"sql_anywhere_connector: convert_funcproc_code: The routine {funcproc_name} is NOT "
                    f"created in the target - it could not be converted completely and has to be "
                    f"migrated by hand: {'; '.join(context['refusals'])}")
                message = (f"{self.MANUAL_ADJUSTMENT_MARKER} - the routine {funcproc_name} was not "
                           f"converted completely: {'; '.join(context['refusals'])}").replace("'", "''")
                routine_sql = (f"DO $sa_refused$ BEGIN\n"
                               f"    RAISE EXCEPTION '{message}';\nEND $sa_refused$;\n\n"
                               f"/* The routine below is what the conversion reached and is where the "
                               f"migration by hand\n   starts. The statement in front of it fails, so "
                               f"that the routine is reported as failed\n   and cannot reach the target "
                               f"half converted:\n{reasons}*/\n{routine_sql}")

            return routine_sql
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: convert_funcproc_code: Error converting {funcproc_name}: {e}")
            self.config_parser.print_log_message('ERROR', traceback.format_exc())
            return ''

    def fetch_triggers(self, table_id: int, table_schema: str, table_name: str):
        """
        The triggers of one table.

        SYS.SYSTRIGGER holds the triggers of the user beside the ones the server maintains for
        the referential actions of a foreign key - those carry no name and are not migrated,
        the actions themselves travel with the foreign key.
        """
        query = f"""
            SELECT t.trigger_id, t.trigger_name, t.event, t.trigger_time,
                   t.trigger_order, t.trigger_defn, t.remarks
            FROM SYS.SYSTRIGGER t
            WHERE t.table_id = {table_id}
            AND t.trigger_name IS NOT NULL
            AND t.foreign_key_id IS NULL
            ORDER BY t.trigger_order, t.trigger_name
        """
        try:
            triggers = {}
            order_num = 1
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                definition = row[5] or ''
                correlations = re.findall(r'(?i)\b(old|new)\s+as\s+"?([\w]+)"?',
                                          self.sa_strip_comments(definition))
                triggers[order_num] = {
                    'id': row[0],
                    'name': row[1],
                    'event': (row[2] or '').strip(),
                    'old': next((alias for kind, alias in correlations if kind.lower() == 'old'), ''),
                    'new': next((alias for kind, alias in correlations if kind.lower() == 'new'), ''),
                    'sql': definition,
                    'comment': row[6] or '',
                }
                ## PostgreSQL fires the triggers of one event in the order of their names, it
                ## has nothing like the ORDER of SQL Anywhere - a trigger which relies on it
                ## keeps its place only as long as the names happen to sort the same way
                if row[4] and int(row[4]) > 1:
                    self.config_parser.print_log_message('WARNING',
                        f"sql_anywhere_connector: fetch_triggers: Trigger {row[1]} of table {table_name} "
                        f"is created with ORDER {row[4]}. PostgreSQL fires the triggers of an event in "
                        f"the order of their names - check that the names keep the order the source "
                        f"gave them.")
                order_num += 1
            cursor.close()
            self.disconnect()
            return triggers
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: fetch_triggers: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def convert_trigger(self, settings: dict):
        """
        One trigger of SQL Anywhere as a trigger function and a trigger of PostgreSQL.

        A trigger of PostgreSQL runs a function, it cannot carry the statements itself, so
        every trigger becomes two statements. What the two share is the model: the timing, the
        event, the level and the WHEN condition mean the same thing in both, the row images
        stand under the names the source gave them and are read as OLD and NEW, and the
        transition tables of a statement level trigger become the REFERENCING tables which
        PostgreSQL knows as well.
        """
        trigger_code = settings['trigger_sql']
        trigger_name = settings['trigger_name']
        target_schema_name = settings['target_schema_name']
        target_table_name = self.config_parser.convert_names_case(settings['target_table_name'])
        target_db_type = settings.get('target_db_type', 'postgresql')

        try:
            definition = self.sa_strip_comments(trigger_code or '').strip()
            if not definition:
                return ''

            header_match = re.match(
                r'(?is)create\s+trigger\s+(?:"?[^".\s]+"?\s*\.\s*)?"?[^".\s]+"?\s+(.*)$', definition)
            if not header_match:
                self.config_parser.print_log_message('WARNING',
                    f"sql_anywhere_connector: convert_trigger: The header of trigger {trigger_name} could "
                    f"not be read, it is not migrated.")
                return ''
            tail = header_match.group(1)

            ## a trigger written in Transact-SQL has no timing and no level - it runs once per
            ## statement and reads the changed rows from the pseudo tables
            event_match = re.match(
                r'(?is)(before|after|instead\s+of)\s+(insert|delete|update)\s*'
                r'(?:of\s+(.+?)\s+)?(?:order\s+\d+\s+)?on\b(.*)$', tail)
            if not event_match:
                self.config_parser.print_log_message('WARNING',
                    f"sql_anywhere_connector: convert_trigger: Trigger {trigger_name} is written in the "
                    f"Transact-SQL dialect - it runs once per statement and reads the rows it changed "
                    f"from 'inserted' / 'deleted'. It is NOT created in the target and has to be "
                    f"migrated by hand.")
                return (f"/* {self.MANUAL_ADJUSTMENT_MARKER} - trigger {trigger_name} is written in the\n"
                        f"   Transact-SQL dialect of SQL Anywhere: it runs once per statement and reads\n"
                        f"   the changed rows from the pseudo tables 'inserted' and 'deleted', which a\n"
                        f"   trigger of PostgreSQL does not have. The definition of the source follows,\n"
                        f"   it has to be rewritten by hand:\n\n{trigger_code}\n*/")

            timing = ' '.join(event_match.group(1).upper().split())
            operation = event_match.group(2).upper()
            update_columns = ''
            if event_match.group(3):
                update_columns = ' OF ' + ', '.join(f'"{column.strip().strip(chr(34))}"'
                                                    for column in event_match.group(3).split(','))
            rest = event_match.group(4)

            body_match = re.search(r'(?is)\bbegin\b', rest)
            if not body_match:
                self.config_parser.print_log_message('WARNING',
                    f"sql_anywhere_connector: convert_trigger: Trigger {trigger_name} has no block which "
                    f"could be read, it is not migrated.")
                return ''
            head, body = rest[:body_match.start()], rest[body_match.end():]
            end_match = None
            for end_match in re.finditer(r'(?is)\bend\b', body):
                pass
            if end_match:
                body = body[:end_match.start()]

            statement_level = bool(re.search(r'(?is)\bfor\s+each\s+statement\b', head))
            when_match = re.search(r'(?is)\bwhen\s*\((.*)\)\s*$', head.strip())
            when_condition = when_match.group(1) if when_match else ''

            old_alias = settings.get('trigger_old') or ''
            new_alias = settings.get('trigger_new') or ''
            for kind, alias in re.findall(r'(?i)\b(old|new)\s+as\s+"?([\w]+)"?', head):
                if kind.lower() == 'old':
                    old_alias = alias
                else:
                    new_alias = alias

            ## a statement level trigger reads the changed rows as tables, a row level trigger
            ## reads one row at a time under the names OLD and NEW
            correlations = {}
            referencing = ''
            if statement_level:
                tables = []
                if old_alias:
                    tables.append(f'OLD TABLE AS "{old_alias}"')
                if new_alias:
                    tables.append(f'NEW TABLE AS "{new_alias}"')
                if tables:
                    referencing = 'REFERENCING ' + ' '.join(tables) + '\n'
            else:
                if old_alias:
                    correlations[old_alias] = 'OLD'
                if new_alias:
                    correlations[new_alias] = 'NEW'

            if timing == 'INSTEAD OF' or (timing == 'BEFORE' and not statement_level):
                returned_row = 'OLD' if operation == 'DELETE' else 'NEW'
            else:
                ## the value an AFTER trigger and a statement level trigger return is ignored
                returned_row = 'NULL'

            context = {
                'target_db_type': target_db_type,
                'declarations': [],
                'exceptions': {},
                'refusals': self.sa_find_unsupported(definition, trigger_name, 'The trigger'),
                'correlations': correlations,
            }
            converted_body = self.sa_convert_body(body, context)
            ## RETURN of a Watcom trigger leaves the trigger, it names no value
            converted_body = re.sub(r'(?i)\breturn\b(?=\s*;)', f'RETURN {returned_row}', converted_body)

            if when_condition:
                masked_condition, literals = self.sa_mask_literals(when_condition)
                for alias, replacement in correlations.items():
                    masked_condition = re.sub(
                        rf'(?i)(?<![\w".])"?{re.escape(alias)}"?\s*\.\s*"?([\w]+)"?',
                        rf'{replacement}."\1"', masked_condition)
                when_condition = self.sa_unmask_literals(masked_condition, literals)

            self.sa_check_assignment_targets(
                context, [declaration.split()[0] for declaration in context['declarations']],
                trigger_name, 'The trigger')

            declare_section = ''
            if context['declarations']:
                declarations = '\n'.join(f"    {declaration};"
                                         for declaration in dict.fromkeys(context['declarations']))
                declare_section = f"DECLARE\n{declarations}\n"

            target_trigger_name = self.config_parser.convert_names_case(trigger_name)
            ## the name of the function is the name of the trigger, which is unique per schema
            ## in the source, and is cut to the length PostgreSQL stores
            function_name = f"{target_trigger_name}_trigfunc"[:63]
            body_quote = '$$' if '$$' not in converted_body else '$sa_trigger$'

            function_sql = (f'CREATE OR REPLACE FUNCTION "{target_schema_name}"."{function_name}"()\n'
                            f'RETURNS trigger\nLANGUAGE plpgsql\nAS {body_quote}\n'
                            f'{declare_section}BEGIN\n{converted_body}\n'
                            f'    RETURN {returned_row};\nEND;\n{body_quote};')

            trigger_sql = (f'CREATE TRIGGER "{target_trigger_name}"\n'
                           f'{timing} {operation}{update_columns} ON "{target_schema_name}"."{target_table_name}"\n'
                           f'{referencing}'
                           f"FOR EACH {'STATEMENT' if statement_level else 'ROW'}\n"
                           + (f'WHEN ({when_condition})\n' if when_condition else '')
                           + f'EXECUTE FUNCTION "{target_schema_name}"."{function_name}"();')

            converted_code = f"{function_sql}\n\n{trigger_sql}"

            if context['refusals']:
                reasons = ''.join(f"     - {reason}\n" for reason in context['refusals'])
                converted_code = (f"/* {self.MANUAL_ADJUSTMENT_MARKER} - this trigger is NOT usable as it "
                                  f"stands.\n   It could not be converted completely:\n{reasons}"
                                  f"   The code below is what the conversion reached and is where the "
                                  f"migration\n   by hand starts. */\n{converted_code}")
            return converted_code
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: convert_trigger: Error converting trigger {trigger_name}: {e}")
            self.config_parser.print_log_message('ERROR', traceback.format_exc())
            return ''

    def trigger_needs_manual_adjustment(self, converted_code):
        """
        Whether a converted trigger carries something which has to be written by hand.

        The marker travels in the code itself - the protocol table keeps the code, and a reader
        of the code alone sees what the migration report says.
        """
        return bool(converted_code) and self.MANUAL_ADJUSTMENT_MARKER in converted_code

    def trigger_manual_adjustment_details(self, converted_code):
        """ The reasons the conversion wrote into the head of the code, for the report. """
        if not self.trigger_needs_manual_adjustment(converted_code):
            return None
        reasons = re.findall(r'^\s+- (.*)$', converted_code, re.MULTILINE)
        return '; '.join(reason.strip() for reason in reasons) or 'see the code of the trigger'

    def fetch_sequences(self, schema_name: str):
        """
        The sequences of the schema.

        A sequence continues in the target where it stood in the source: resume_at is the value
        SYS.SYSSEQUENCE hands out next, and starting the target sequence there is what keeps it
        from giving a second time what the source already gave. The declared start_with says
        only where the sequence began and is used when the position is not known.
        """
        query = f"""
            SELECT s.sequence_name, s.min_value, s.max_value, s.increment_by,
                   s.start_with, s.cache, s.cycle, s.resume_at
            FROM SYS.SYSSEQUENCE s
            JOIN SYS.SYSUSER u ON u.user_id = s.owner
            WHERE u.user_name = '{schema_name}'
            ORDER BY s.sequence_name
        """
        sequences = {}
        order_num = 1
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                sequences[order_num] = {
                    'id': order_num,
                    'sequence_name': row[0],
                    'source_minvalue': int(row[1]) if row[1] is not None else None,
                    'source_maxvalue': int(row[2]) if row[2] is not None else None,
                    'source_increment_by': int(row[3]) if row[3] is not None else None,
                    'source_start_value': int(row[7] if row[7] is not None else row[4]),
                    'source_cache': int(row[5]) if row[5] is not None else None,
                    'source_is_cycled': bool(row[6]),
                    'used_in_identity': False,
                    'source_sequence_sql': '',
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return sequences
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: fetch_sequences: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_sequence_details(self, sequence_owner, sequence_name):
        # Placeholder for fetching sequence details
        return {}

    def fetch_views_names(self, source_schema_name: str):
        views = {}
        order_num = 1
        query = f"""SELECT viewname FROM sys.sysviews WHERE vcreator = '{source_schema_name}'"""
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                views[order_num] = {
                    'id': None,
                    'schema_name': source_schema_name,
                    'view_name': row[0],
                    'comment': ''
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return views
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: fetch_views_names: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_view_code(self, settings):
        view_id = settings['view_id']
        source_schema_name = settings['source_schema_name']
        source_view_name = settings['source_view_name']
        target_schema_name = settings['target_schema_name']
        target_view_name = settings['target_view_name']
        query = f"""
            SELECT viewtext
            FROM sys.sysviews
            WHERE vcreator = '{source_schema_name}'
            AND viewname = '{source_view_name}'
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
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: fetch_view_code: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def convert_view_code(self, settings: dict):
        view_code = settings.get('view_code')
        if not view_code:
            return view_code

        source_schema = settings.get('source_schema_name', '')
        target_schema = settings.get('target_schema_name', 'public')

        # 1. Strip source schema qualification: "DBA". -> ""
        if source_schema:
            view_code = re.sub(rf"(?i)\"{re.escape(source_schema)}\"\.", "", view_code)

        # 2. Strip double quotes from function calls e.g. "COUNT"( -> count(
        view_code = re.sub(r'"([A-Za-z0-9_]+)"\s*\(', r'\1(', view_code)

        # 3. Convert empty COUNT() -> count(*)
        view_code = re.sub(r"(?i)\bCOUNT\s*\(\s*\)", "count(*)", view_code)

        # 4. Convert IF cond THEN val1 ELSE val2 ENDIF -> CASE WHEN cond THEN val1 ELSE val2 END
        view_code = re.sub(r"(?i)\bIF\s+(.+?)\s+THEN\s+(.+?)\s+ELSE\s+(.+?)\s+ENDIF\b", r"CASE WHEN \1 THEN \2 ELSE \3 END", view_code)

        # 5. Convert LIST(expr, sep ...) -> string_agg(expr::text, sep ...)
        view_code = re.sub(r"(?i)\bLIST\s*\(\s*([^\s,]+)\s*,", r"string_agg(\1::text,", view_code)

        # 6. Convert SELECT TOP N -> SELECT ... LIMIT N
        def replace_top(match):
            top_n = match.group(1)
            rest = match.group(2)
            return f"SELECT {rest} LIMIT {top_n}"

        view_code = re.sub(r"(?i)\bSELECT\s+TOP\s+(\d+)\s+(.+?)(?=\)|\s*$)", replace_top, view_code, flags=re.DOTALL)

        # 7. Convert boolean comparisons: "is_active" = 1 -> "is_active" = true
        view_code = re.sub(r'is_active"\s*=\s*1\b', 'is_active" = true', view_code)
        view_code = re.sub(r'is_active"\s*=\s*0\b', 'is_active" = false', view_code)

        # 8. Fix recursive CTE string concatenation type matching
        view_code = re.sub(r"as\s+varchar\s*\(\s*500\s*\)", "as text", view_code, flags=re.IGNORECASE)

        # 9. Apply standard function mappings
        view_code = self.apply_sql_functions_mapping(view_code, settings)

        # 10. Ensure CREATE OR REPLACE VIEW
        if not view_code.lower().startswith("create"):
            view_code = "CREATE OR REPLACE VIEW " + view_code
        else:
            view_code = re.sub(r"(?i)^CREATE\s+(MATERIALIZED\s+)?VIEW", "CREATE OR REPLACE VIEW", view_code)

        return view_code

    def get_sequence_current_value(self, sequence_id: int):
        pass

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

    def get_rows_count(self, table_schema: str, table_name: str, migration_limitation: str = None):
        query = f"SELECT COUNT(*) FROM \"{table_schema}\".\"{table_name}\""
        if migration_limitation:
            query += f" WHERE {migration_limitation}"
        cursor = self.connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_table_size(self, table_schema: str, table_name: str):
        raise NotImplementedError("Fetching table size is not yet implemented for SQL Anywhere")

    def get_table_next_identity(self, table_schema: str, table_name: str):
        try:
            # SQL Anywhere does not expose sequence counters easily.
            # We first find the identity column (default = 'autoincrement')
            col_query = f"""
                SELECT c.column_name
                FROM sys.syscolumn c
                WHERE c.table_id = (
                    SELECT t.table_id FROM sys.systable t
                    WHERE t.creator in (
                        SELECT DISTINCT user_id
                        FROM sys.SYSUSERPERM where user_name = '{table_schema}'
                    )
                    AND table_name = '{table_name}'
                ) AND UPPER(c."default") = 'AUTOINCREMENT'
            """
            cursor = self.connection.cursor()
            cursor.execute(col_query)
            col_row = cursor.fetchone()
            if not col_row:
                cursor.close()
                return None
            identity_col = col_row[0]

            # Then query the max value
            max_query = f'SELECT MAX("{identity_col}") FROM "{table_schema}"."{table_name}"'
            cursor.execute(max_query)
            max_row = cursor.fetchone()
            cursor.close()

            if max_row and max_row[0] is not None:
                return int(max_row[0]) + 1
            return 1
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"sql_anywhere_connector: get_table_next_identity: Error fetching next identity for {table_schema}.{table_name}: {e}")
            return None

    def fetch_user_defined_types(self, schema: str):
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
        self.config_parser.print_log_message('DEBUG3', f"sql_anywhere_connector: get_table_description: SQL Anywhere connector: Getting table description for {settings['table_schema']}.{settings['table_name']}")
        table_schema = settings['table_schema']
        table_name = settings['table_name']
        output = ""
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT sa_get_table_definition('{table_schema}', '{table_name}')")

            set_num = 1
            if cursor.description is not None:
                rows = cursor.fetchall()
                if rows:
                    output += f"Result set {set_num}:\n"
                    columns = [column[0] for column in cursor.description]
                    table = tabulate(rows, headers=columns, tablefmt="github")
                    output += table + "\n\n"
                    set_num += 1

            cursor.close()
            self.disconnect()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: get_table_description: Error fetching table description for {table_schema}.{table_name}: {e}")
            raise

        return { 'table_description': output.strip() }

    def testing_select(self):
        return "SELECT 1"

    def get_database_version(self):
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute("select top 1 version, platform, first_time from SYSHISTORY order by first_time desc")
            version = cursor.fetchone()
            cursor.close()
            self.disconnect()
            return version
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: get_database_version: Error fetching database version: {e}")
            raise

    def get_database_size(self):
        query = "select round(db_property('FileSize') * db_property('PageSize') / 1024 / 1024,2) as db_size_mb"
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        size = cursor.fetchone()[0]
        cursor.close()
        self.disconnect()
        return size

    def get_top_n_tables(self, settings):
        top_tables = {}
        top_tables['by_rows'] = {}
        top_tables['by_size'] = {}
        top_tables['by_columns'] = {}
        top_tables['by_indexes'] = {}
        top_tables['by_constraints'] = {}

        source_schema_name = settings.get('source_schema_name', 'public')
        try:
            order_num = 1
            top_n = self.config_parser.get_top_n_tables_by_rows()
            if top_n > 0:
                query = f"""
                    SELECT TOP {top_n}
                        t.table_name,
                        table_page_count
                    FROM sys.systable t
                    WHERE creator in (SELECT DISTINCT user_id
                    FROM sys.SYSUSERPERM where user_name = '{source_schema_name}')
                    ORDER BY table_page_count DESC
                """
                self.config_parser.print_log_message('DEBUG3', f"sql_anywhere_connector: get_top_n_tables: Fetching top {top_n} tables by rows for schema {source_schema_name} with query: {query}")
                self.connect()
                cursor = self.connection.cursor()
                cursor.execute(query)
                order_num = 1
                for row in cursor.fetchall():
                    top_tables['by_rows'][order_num] = {
                        'owner': source_schema_name,
                        'table_name': row[0].strip(),
                        'table_size': row[1]
                    }
                    order_num += 1
                cursor.close()
                self.disconnect()
                self.config_parser.print_log_message('DEBUG3', f"sql_anywhere_connector: get_top_n_tables: Top {top_n} tables by rows fetched successfully {top_tables['by_rows']}")
            else:
                self.config_parser.print_log_message('DEBUG', "sql_anywhere_connector: get_top_n_tables: Top N tables by rows is not configured or set to 0")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: get_top_n_tables: Error fetching top tables by rows: {e}")

        return top_tables

    def get_top_fk_dependencies(self, settings):
        top_fk_dependencies = {}
        return top_fk_dependencies

    def target_table_exists(self, target_schema_name, target_table_name):
        query = f"""
            SELECT COUNT(*)
            FROM sys.systable
            WHERE creator in (SELECT DISTINCT user_id
            FROM sys.SYSUSERPERM where user_name = '{target_schema_name}')
            AND table_name = '{target_table_name}'
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
            self.config_parser.print_log_message('ERROR', f"sql_anywhere_connector: target_table_exists: Error checking if target table exists: {e}")
            raise

    def fetch_all_rows(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def convert_default_value(self, settings) -> dict:
        extracted_default_value = settings.get('extracted_default_value')
        if not extracted_default_value:
            return extracted_default_value

        val = str(extracted_default_value).strip()

        # Drop autoincrement / number(*) defaults handled by serial/identity in target database
        if re.match(r'^(global\s+)?autoincrement|number\s*\(\s*\*\s*\)$', val, re.IGNORECASE):
            return None

        # Strip outer enclosing parentheses if present
        if val.startswith('(') and val.endswith(')'):
            inner = val[1:-1].strip()
            if not (inner.startswith('(') and not inner.endswith(')')):
                val = inner

        # Clean double quotes around function names e.g. "LOWER"( -> LOWER(
        val = re.sub(r'"([A-Za-z0-9_]+)"\s*\(', r'\1(', val)

        # UUID generators (e.g. NEWID(), newid(), uuid_generate_v4(), gen_random_uuid())
        if re.search(r'(?i)\b(?:newid|newid\s*\(\s*\)|uuid_generate_v4|gen_random_uuid)\b', val):
            column_type = settings.get('column_type', '')
            return self.config_parser.get_uuid_default_function(column_type)

        # Convert simple double-quoted string literals e.g. "ACTIVE" -> 'ACTIVE'
        if re.fullmatch(r'"[^\"]*"', val):
            val = "'" + val[1:-1].replace("'", "''") + "'"

        val = self.apply_sql_functions_mapping(val, settings)

        # If val still contains double-quoted identifiers (column references), drop it as PostgreSQL DEFAULT cannot reference columns
        if re.search(r'"[A-Za-z0-9_]+"', val):
            self.config_parser.print_log_message('INFO', f"sql_anywhere_connector: convert_default_value: Default value '{extracted_default_value}' contains column reference - dropped for PostgreSQL target.")
            return None

        return val

    def get_table_checksum(self, schema_name: str, table_name: str, columns: list):
        if not columns:
            return None
            
        cols_list = []
        for col in columns:
            dtype = col.get('data_type', '').lower()
            if any(x in dtype for x in ['lob', 'bytea', 'xml', 'json', 'text', 'image', 'ntext', 'varbinary']):
                continue
            cols_list.append(f'"{col["column_name"]}"')
            
        if not cols_list:
            return None
            
        cols_str = ", ".join(cols_list)
        query = f'SELECT {cols_str} FROM "{schema_name}"."{table_name}"'
        return self._compute_python_table_checksum(query)

    def get_random_pks(self, schema_name: str, table_name: str, pk_columns: list, sample_size: int):
        return []

    def get_row_checksums(self, schema_name: str, table_name: str, pk_columns: list, pk_values_list: list, columns: list):
        if not columns or not pk_columns or not pk_values_list:
            return {}
            
        cols_list = []
        for col in columns:
            dtype = col.get('data_type', '').lower()
            if any(x in dtype for x in ['lob', 'bytea', 'xml', 'json', 'text', 'image', 'ntext', 'varbinary']):
                continue
            cols_list.append(f'"{col["column_name"]}"')
            
        if not cols_list:
            return {}
            
        cols_str = ", ".join(cols_list)
        pk_cols_str = ", ".join([f'"{c}"' for c in pk_columns])
        
        in_values = []
        for pk_dict in pk_values_list:
            vals = []
            for c in pk_columns:
                val = pk_dict[c]
                if val is None:
                    vals.append("NULL")
                elif isinstance(val, str):
                    escaped_val = val.replace("'", "''")
                    vals.append(f"'{escaped_val}'")
                else:
                    vals.append(str(val))
            in_values.append(f"({', '.join(vals)})")
        
        where_clause = f"({pk_cols_str}) IN ({', '.join(in_values)})"
        if len(pk_columns) == 1:
            where_clause = f"{pk_cols_str} IN ({', '.join([v.strip('()') for v in in_values])})"
            
        query = f'SELECT {pk_cols_str}, {cols_str} FROM "{schema_name}"."{table_name}" WHERE {where_clause}'
        return self._compute_python_row_checksums(query, len(pk_columns))

    def get_lob_sizes(self, schema_name: str, table_name: str, pk_columns: list, pk_values_list: list, lob_columns: list):
        return {}

if __name__ == "__main__":
    print("This script is not meant to be run directly")
