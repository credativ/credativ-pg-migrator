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
import oracledb  ## pip install python-oracledb
import traceback
from tabulate import tabulate
import time
import datetime
import re
import sqlglot

class OracleConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        if source_or_target != 'source':
            raise ValueError("Oracle is only supported as a source database")

        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.on_error_action = self.config_parser.get_on_error_action()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger

        if self.config_parser.get_oracle_thick_mode():
            try:
                oracledb.init_oracle_client()
                self.config_parser.print_log_message('INFO', "oracle_connector: Oracle thick mode enabled via configuration.")
            except Exception as e:
                self.config_parser.print_log_message('DEBUG', f"oracle_connector: thick mode already initialized or failed: {e}")

    def connect(self):
        # Idempotent: reuse an already-open connection instead of orphaning it.
        # Combined with disconnect() clearing self.connection, this lets every method
        # safely call self.connect() at its start without depending on caller bracketing.
        if self.connection is not None:
            return
        connection_string = self.config_parser.get_connect_string(self.source_or_target)
        username = self.config_parser.get_db_config(self.source_or_target)['username']
        try:
            if username == 'SYS':
                self.connection = oracledb.connect(user=username,
                                                    password=self.config_parser.get_db_config(self.source_or_target)['password'],
                                                    dsn=connection_string,
                                                    mode=oracledb.SYSDBA)
            else:
                self.connection = oracledb.connect(user=username,
                                                    password = self.config_parser.get_db_config(self.source_or_target)['password'],
                                                    dsn=connection_string)

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: connect: Error connecting to Oracle database: {e}")
            self.config_parser.print_log_message('ERROR', "oracle_connector: connect: Full stack trace:")
            self.config_parser.print_log_message('ERROR', traceback.format_exc())
            raise e

    def disconnect(self):
        if self.connection is not None:
            try:
                self.connection.close()
            except Exception as e:
                self.config_parser.print_log_message('DEBUG', f"oracle_connector: disconnect: Error while closing connection: {e}")
            finally:
                # Always clear the handle so connect() reopens on next use
                self.connection = None

    def get_sql_functions_mapping(self, settings):
        """ Returns a dictionary of SQL functions mapping for the target database """
        target_db_type = settings['target_db_type']
        if target_db_type == 'postgresql':
            return {}
        else:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: get_sql_functions_mapping: Unsupported target database type: {target_db_type}")

    def migrate_sequences(self, target_connector, settings):
        """
        Create a single standalone sequence in the target database.
        Called once per sequence by the orchestrator's sequence_worker, receiving the
        decoded protocol row (settings) with the source_* attributes captured by
        fetch_sequences() at planning time. Returns True on success, False on failure.
        """
        target_schema_name = settings['target_schema_name']
        target_sequence_name = settings['target_sequence_name']

        # PostgreSQL sequences are backed by bigint; Oracle's default bounds
        # (e.g. MAXVALUE 9999999999999999999999999999) exceed that range and must be clamped.
        PG_BIGINT_MAX = 9223372036854775807
        PG_BIGINT_MIN = -9223372036854775808

        def _to_int(val):
            try:
                return int(val)
            except (TypeError, ValueError):
                return None

        increment_by = _to_int(settings.get('source_increment_by')) or 1
        minvalue = _to_int(settings.get('source_minvalue'))
        maxvalue = _to_int(settings.get('source_maxvalue'))
        start_value = _to_int(settings.get('source_start_value'))
        cache = _to_int(settings.get('source_cache'))
        is_cycled = str(settings.get('source_is_cycled') or '').upper() in ('Y', 'YES', 'TRUE', '1')

        # Drop bounds that fall outside PostgreSQL's bigint range - let PostgreSQL use its defaults
        if maxvalue is not None and maxvalue >= PG_BIGINT_MAX:
            maxvalue = None
        if minvalue is not None and minvalue <= PG_BIGINT_MIN:
            minvalue = None
        if start_value is not None:
            start_value = max(min(start_value, PG_BIGINT_MAX), PG_BIGINT_MIN)

        try:
            target_connector.connect()

            parts = [f'CREATE SEQUENCE IF NOT EXISTS "{target_schema_name}"."{target_sequence_name}"']
            parts.append(f"INCREMENT BY {increment_by}")
            if minvalue is not None:
                parts.append(f"MINVALUE {minvalue}")
            if maxvalue is not None:
                parts.append(f"MAXVALUE {maxvalue}")
            if start_value is not None:
                # START WITH must respect the (possibly clamped) MINVALUE / MAXVALUE
                if minvalue is not None:
                    start_value = max(start_value, minvalue)
                if maxvalue is not None:
                    start_value = min(start_value, maxvalue)
                parts.append(f"START WITH {start_value}")
            # PostgreSQL requires CACHE >= 1; only emit an explicit cache when meaningful
            if cache is not None and cache > 1:
                parts.append(f"CACHE {cache}")
            parts.append("CYCLE" if is_cycled else "NO CYCLE")
            create_sql = " ".join(parts) + ";"

            self.config_parser.print_log_message('DEBUG', f"oracle_connector: migrate_sequences: Creating sequence with SQL: {create_sql}")
            target_connector.execute_query(create_sql)
            target_connector.disconnect()
            self.config_parser.print_log_message('INFO', f"oracle_connector: migrate_sequences: Sequence \"{target_schema_name}\".\"{target_sequence_name}\" created successfully.")
            return True
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: migrate_sequences: Error migrating sequence {target_sequence_name}: {e}")
            try:
                target_connector.disconnect()
            except Exception:
                pass
            return False

    def fetch_table_names(self, table_schema: str):
        # Exclude materialized view container tables (they share the mview name and appear in
        # ALL_TABLES) - they are migrated via the view path as CREATE MATERIALIZED VIEW, not as
        # base tables, to avoid duplicate/conflicting objects.
        query = """
            SELECT t.table_name, tc.comments
            FROM all_tables t
            LEFT JOIN all_tab_comments tc
                ON tc.owner = t.owner AND tc.table_name = t.table_name AND tc.table_type = 'TABLE'
            WHERE t.owner = :owner
                AND NOT EXISTS (
                    SELECT 1 FROM all_mviews m
                    WHERE m.owner = t.owner AND m.mview_name = t.table_name
                )
            ORDER BY t.table_name
        """
        try:
            tables = {}
            order_num = 1
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, {'owner': table_schema.upper()})
            for row in cursor.fetchall():
                tables[order_num] = {
                    'id': None,
                    'schema_name': table_schema,
                    'table_name': row[0],
                    'comment': row[1] or ''
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return tables
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: fetch_table_names: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_table_columns(self, settings) -> dict:
        table_schema = settings['table_schema']
        table_name = settings['table_name']
        query = """
            SELECT
                c.column_id,
                c.column_name,
                c.data_type,
                c.char_length,
                c.data_precision,
                c.data_scale,
                c.nullable,
                c.data_default,
                cc.comments
            FROM all_tab_columns c
            LEFT JOIN all_col_comments cc
                ON cc.owner = c.owner AND cc.table_name = c.table_name AND cc.column_name = c.column_name
            WHERE c.owner = :owner AND c.table_name = :table_name
            ORDER BY c.column_id
        """
        binds = {'owner': table_schema.upper(), 'table_name': table_name.upper()}
        try:
            result = {}
            self.connect()
            cursor = self.connection.cursor()

            # Oracle 12c+ identity columns (GENERATED ... AS IDENTITY) are not reflected in
            # data_default, so collect them separately from ALL_TAB_IDENTITY_COLS.
            identity_columns = set()
            try:
                cursor.execute("""
                    SELECT column_name
                    FROM all_tab_identity_cols
                    WHERE owner = :owner AND table_name = :table_name
                """, binds)
                identity_columns = {r[0] for r in cursor.fetchall()}
            except Exception as e_ident:
                self.config_parser.print_log_message('DEBUG', f"oracle_connector: fetch_table_columns: ALL_TAB_IDENTITY_COLS not available (Oracle < 12c?): {e_ident}")

            cursor.execute(query, binds)
            for row in cursor.fetchall():
                column_id = row[0]
                column_name = row[1]
                data_type = row[2]
                character_maximum_length = row[3]
                data_precision = row[4]
                data_scale = row[5]
                column_nullable = row[6]
                column_default = row[7]
                column_comment = row[8]

                # Normalize Oracle types that embed precision in data_type (e.g.
                # "TIMESTAMP(6) WITH TIME ZONE", "INTERVAL DAY(2) TO SECOND(6)") to canonical
                # forms so get_types_mapping can match them instead of falling back to TEXT.
                dt_upper = (data_type or '').upper()
                if 'WITH LOCAL TIME ZONE' in dt_upper:
                    data_type = 'TIMESTAMP WITH LOCAL TIME ZONE'
                elif 'WITH TIME ZONE' in dt_upper:
                    data_type = 'TIMESTAMP WITH TIME ZONE'
                elif dt_upper.startswith('INTERVAL YEAR'):
                    data_type = 'INTERVAL YEAR TO MONTH'
                elif dt_upper.startswith('INTERVAL DAY'):
                    data_type = 'INTERVAL DAY TO SECOND'

                column_type = data_type.upper()
                if self.is_string_type(column_type) and character_maximum_length is not None:
                    column_type += f"({character_maximum_length})"
                elif self.is_numeric_type(column_type) and data_precision is not None:
                    if data_scale is not None:
                        column_type += f"({data_precision}, {data_scale})"
                    else:
                        column_type += f"({data_precision})"

                result[column_id] = {
                    'column_name': column_name,
                    'data_type': data_type,
                    'column_type': column_type,
                    'character_maximum_length': character_maximum_length if self.is_string_type(data_type) else None,
                    'numeric_precision': data_precision if self.is_numeric_type(data_type) else None,
                    'numeric_scale': data_scale if self.is_numeric_type(data_type) else None,
                    'is_nullable': 'NO' if column_nullable == 'N' else 'YES',
                    'is_identity': 'YES' if column_name in identity_columns else 'NO',
                    'column_default_value': column_default,
                    'comment': column_comment or '',
                    'column_comment': column_comment or '',
                }

                # Identity columns carry a system-generated sequence default; clear it so it
                # is not emitted as a column default on the target.
                if column_name in identity_columns:
                    result[column_id]['column_default_value'] = ""

                self.config_parser.print_log_message('DEBUG3', f"oracle_connector: fetch_table_columns: Checking if default value is a sequence for column {column_name} ({column_default})...")
                if (isinstance(column_default, str)
                    and 'nextval' in column_default.lower()):
                    parts = column_default.replace('"', '').split(".")
                    if len(parts) == 3:
                        owner, seq_name, _ = parts
                        sequence_details = self.get_sequence_details(owner, seq_name)
                        if sequence_details:
                            self.config_parser.print_log_message('DEBUG3', f"oracle_connector: fetch_table_columns: Found sequence {sequence_details['name']} for column {column_name}.")
                            result[column_id]['column_default_value'] = ""
                            result[column_id]['is_identity'] = 'YES'
                            # if data_type in ('NUMBER'):
                            #     result[column_id]['data_type'] = 'BIGINT'
                    ## TODO: insert_internal_data_types_substitutions
                    ## internal subtitution of this type breaks foreign key constraints

            cursor.close()
            self.disconnect()

            return result
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: fetch_table_columns: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_types_mapping(self, settings):
        target_db_type = settings['target_db_type']
        types_mapping = {}
        if target_db_type == 'postgresql':
            types_mapping = {
                'VARCHAR': 'VARCHAR',
                'VARCHAR2': 'VARCHAR',
                'NVARCHAR': 'VARCHAR',
                'NVARCHAR2': 'VARCHAR',
                'CHARACTER VARYING': 'VARCHAR',
                'CHAR': 'CHAR',
                'LONG VARCHAR': 'TEXT',
                'LONG NVARCHAR': 'TEXT',
                'NCHAR': 'CHAR',
                'LONG': 'TEXT',
                'NCLOB': 'TEXT',

                'NUMBER': 'NUMERIC',
                'FLOAT': 'FLOAT',
                'DOUBLE PRECISION': 'DOUBLE PRECISION',
                'BINARY_FLOAT': 'REAL',
                'BINARY_DOUBLE': 'DOUBLE PRECISION',

                'DATE': 'DATE',
                'TIMESTAMP': 'TIMESTAMP',
                'TIMESTAMP(6)': 'TIMESTAMP',
                # data_type for these embeds the precision (e.g. TIMESTAMP(6) WITH TIME ZONE);
                # fetch_table_columns normalizes them to these canonical forms.
                'TIMESTAMP WITH TIME ZONE': 'TIMESTAMPTZ',
                'TIMESTAMP WITH LOCAL TIME ZONE': 'TIMESTAMPTZ',

                'CLOB': 'TEXT',
                'BLOB': 'BYTEA',
                'LONG RAW': 'BYTEA',
                'RAW': 'BYTEA',

                'BOOLEAN': 'BOOLEAN',
                'INTERVAL': 'INTERVAL',
                'INTERVAL YEAR TO MONTH': 'INTERVAL',
                'INTERVAL DAY TO SECOND': 'INTERVAL',

                'ROWID': 'TEXT',
                'UROWID': 'TEXT',
                'XMLTYPE': 'XML',
                'JSON': 'JSONB',

                'SERIAL': 'SERIAL',
                'BIGSERIAL': 'BIGSERIAL',
                'INT': 'INTEGER',
                'BIGINT': 'BIGINT',
                'INTEGER': 'INTEGER',
                'SMALLINT': 'SMALLINT',
                'REAL': 'REAL',
                'DECIMAL': 'DECIMAL',
            }
        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

        return types_mapping

    def get_create_table_sql(self, settings):
        return ""

    def is_string_type(self, column_type: str) -> bool:
        column_type_upper = column_type.upper()
        # RAW and LONG RAW are binary types despite 'LONG' appearing in 'LONG RAW'
        if 'RAW' in column_type_upper:
            return False
        return 'CHAR' in column_type_upper or 'VARCHAR' in column_type_upper or 'LONG' in column_type_upper or 'TEXT' in column_type_upper or 'CLOB' in column_type_upper

    def is_numeric_type(self, column_type: str) -> bool:
        numeric_types = ['BIGINT', 'INTEGER', 'INT', 'TINYINT', 'SMALLINT', 'FLOAT', 'DOUBLE PRECISION', 'DECIMAL', 'NUMERIC', 'REAL', 'NUMBER', 'SERIAL', 'BIGSERIAL']
        return column_type.upper() in numeric_types

    def get_sequence_details(self, sequence_owner, sequence_name):
        query = f"""
            SELECT
                sequence_name,
                min_value,
                max_value,
                increment_by,
                cycle_flag,
                order_flag,
                cache_size,
                last_number
            FROM all_sequences
            WHERE sequence_owner = :sequence_owner
            AND sequence_name = :sequence_name
        """
        try:
            self.connect()  # idempotent; must NOT disconnect (called within caller scopes)
            cursor = self.connection.cursor()
            cursor.execute(query, {'sequence_owner': sequence_owner.upper(), 'sequence_name': sequence_name.upper()})
            result = cursor.fetchone()
            cursor.close()
            if result:
                return {
                    'name': result[0],
                    'min_value': result[1],
                    'max_value': result[2],
                    'increment_by': result[3],
                    'cycle': result[4],
                    'order': result[5],
                    'cache_size': result[6],
                    'last_value': result[7],
                    'comment': ''
                }
            else:
                return None
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: get_sequence_details: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

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
            self.connect()  # ensure the source connection is open (idempotent)
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
            batch_size = settings['batch_size']
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
                self.config_parser.print_log_message('INFO', f"oracle_connector: migrate_table: Worker {worker_id}: Table {source_table_name} is empty - skipping data migration.")
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

                    self.config_parser.print_log_message('INFO', f"oracle_connector: migrate_table: Worker {worker_id}: Source table {source_table_name}: {source_table_rows_limited} rows / Target table {target_table_name}: {target_table_rows} rows - starting data migration.")

                    select_columns_list = []
                    orderby_columns_list = []
                    insert_columns_list = []
                    for order_num, col in source_columns.items():
                        self.config_parser.print_log_message('DEBUG2',
                                                            f"Worker {worker_id}: Table {source_schema_name}.{source_table_name}: Processing column {col['column_name']} ({order_num}) with data type {col['data_type']}")

                        # DATE / TIMESTAMP values are returned natively as Python datetime
                        # objects by python-oracledb and inserted directly into PostgreSQL,
                        # so no TO_CHAR string conversion is applied here.
                        select_columns_list.append(f'''"{col['column_name']}"''')

                        if col['data_type'].lower() not in ('clob', 'nclob', 'blob', 'bfile', 'long', 'long raw', 'xmltype'):
                            orderby_columns_list.append(f'''"{col['column_name']}"''')

                    for order_num, col in target_columns.items():
                        insert_columns_list.append(f'''"{self.config_parser.convert_names_case(col['column_name'])}"''')

                    if not orderby_columns_list:
                        first_valid_col = next(iter(source_columns.values()))['column_name']
                        orderby_columns_list.append(f'''"{first_valid_col}"''')

                    select_columns = ', '.join(select_columns_list)
                    orderby_columns = ', '.join(orderby_columns_list)
                    insert_columns = ', '.join(insert_columns_list)

                    if resume_after_crash and not drop_unfinished_tables:
                        chunk_number = self.config_parser.get_total_chunks(target_table_rows, chunk_size)
                        self.config_parser.print_log_message('DEBUG', f"oracle_connector: migrate_table: Worker {worker_id}: Resuming migration for table {source_schema_name}.{source_table_name} from chunk {chunk_number} with data chunk size {chunk_size}.")
                        chunk_offset = target_table_rows
                    else:
                        chunk_offset = (chunk_number - 1) * chunk_size

                    chunk_start_row_number = chunk_offset + 1
                    chunk_end_row_number = chunk_offset + chunk_size

                    self.config_parser.print_log_message('DEBUG', f"oracle_connector: migrate_table: Worker {worker_id}: Migrating table {source_schema_name}.{source_table_name}: chunk {chunk_number}, data chunk size {chunk_size}, batch size {batch_size}, chunk offset {chunk_offset}, chunk end row number {chunk_end_row_number}, source table rows {source_table_rows_limited}")
                    order_by_clause = ''

                    query = f'''SELECT {select_columns} FROM "{source_schema_name}"."{source_table_name}"'''
                    if migration_limitation:
                        query += f" WHERE {migration_limitation}"
                    primary_key_columns = migrator_tables.select_primary_key({'source_schema_name': source_schema_name, 'source_table_name': source_table_name})
                    self.config_parser.print_log_message('DEBUG2', f"oracle_connector: migrate_table: Worker {worker_id}: Primary key columns for {source_schema_name}.{source_table_name}: {primary_key_columns}")
                    if primary_key_columns:
                        orderby_columns = primary_key_columns
                    order_by_clause = f""" ORDER BY {orderby_columns}"""
                    query += order_by_clause + f" OFFSET {chunk_offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY"

                    self.config_parser.print_log_message('DEBUG', f"oracle_connector: migrate_table: Worker {worker_id}: Fetching data with cursor using query: {query}")

                    part_name = 'execute query'
                    cursor = self.connection.cursor()
                    if batch_size > 10000:
                        cursor.arraysize = 1000
                    else:
                        cursor.arraysize = 100

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
                        self.config_parser.print_log_message('DEBUG', f"oracle_connector: migrate_table: Worker {worker_id}: Fetched {len(records)} rows (batch {batch_number}) from source table '{source_table_name}' using cursor")

                        transforming_start_time = time.time()
                        records = [
                            {column['column_name']: value for column, value in zip(source_columns.values(), record)}
                            for record in records
                        ]
                        for record in records:
                            for order_num, column in source_columns.items():
                                column_name = column['column_name']
                                column_type = column['data_type']
                                # self.config_parser.print_log_message('DEBUG3', f"oracle_connector: migrate_table: Worker {worker_id}: Processing column {column_name} with data type {column_type} in record {record}")
                                # LOB values are returned as locator objects and must be read into
                                # memory before insertion. NCLOB is included here (previously only
                                # BLOB/CLOB were handled); the hasattr guard tolerates drivers/config
                                # that already return LOBs as str/bytes.
                                if column_type.lower() in ('blob', 'clob', 'nclob'):
                                    lob_value = record[column_name]
                                    if lob_value is not None and hasattr(lob_value, 'read'):
                                        record[column_name] = lob_value.read()

                        # Insert batch into target table
                        self.config_parser.print_log_message('DEBUG', f"oracle_connector: migrate_table: Worker {worker_id}: Starting insert of {len(records)} rows from source table {source_table_name}")
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
                    self.config_parser.print_log_message('INFO', f"oracle_connector: migrate_table: Worker {worker_id}: Target table {target_schema_name}.{target_table_name} has {target_table_rows} rows")

                    shortest_batch_seconds = min(batch_durations) if batch_durations else 0
                    longest_batch_seconds = max(batch_durations) if batch_durations else 0
                    average_batch_seconds = sum(batch_durations) / len(batch_durations) if batch_durations else 0
                    self.config_parser.print_log_message('INFO', f"oracle_connector: migrate_table: Worker {worker_id}: Migrated {total_inserted_rows} rows from {source_table_name} to {target_schema_name}.{target_table_name} in {batch_number} batches: "
                                                            f"Shortest batch: {shortest_batch_seconds:.2f} seconds, "
                                                            f"Longest batch: {longest_batch_seconds:.2f} seconds, "
                                                            f"Average batch: {average_batch_seconds:.2f} seconds")

                    cursor.close()

                else:
                    self.config_parser.print_log_message('INFO', f"oracle_connector: migrate_table: Worker {worker_id}: Target table {target_table_name} has {target_table_rows} rows and data_conflict_action is '{data_conflict_action}'. Skipping data migration.")

                migration_stats = {
                    'rows_migrated': total_inserted_rows,
                    'chunk_number': chunk_number,
                    'total_chunks': total_chunks,
                    'source_table_rows_all': source_table_rows_all,

                    'source_table_rows_limited': source_table_rows_limited,
                    'target_table_rows': target_table_rows,
                    'finished': False,
                }

                self.config_parser.print_log_message('DEBUG', f"oracle_connector: migrate_table: Worker {worker_id}: Migration stats: {migration_stats}")
                if source_table_rows_limited <= target_table_rows or chunk_number >= total_chunks:
                    self.config_parser.print_log_message('DEBUG3', f"oracle_connector: migrate_table: Worker {worker_id}: Setting migration status to finished for table {source_table_name} (chunk {chunk_number}/{total_chunks})")
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
            self.config_parser.print_log_message('ERROR', f"oracle_connector: migrate_table: Worker {worker_id}: Error during {part_name} -> {e}")
            self.config_parser.print_log_message('ERROR', f"oracle_connector: migrate_table: Worker {worker_id}: Full stack trace: {traceback.format_exc()}")
            raise e

    def fetch_indexes(self, settings):
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

        table_indexes = {}
        order_num = 1
        hidden_columns_count = 0

        ## for the future reference - oracle function to get DDL
        # SELECT DBMS_METADATA.GET_DDL('INDEX', index_name, table_owner) AS ddl
        # FROM   dba_indexes
        # WHERE  table_owner = 'C##CHINOOK'
        # AND  table_name = 'ALBUM';
        # 'TABLE', 'INDEX', 'VIEW', 'SEQUENCE', 'PACKAGE', 'FUNCTION', 'PROCEDURE', 'CONSTRAINT', 'TRIGGER', 'SYNONYM'

        binds = {'owner': source_table_schema.upper(), 'table_name': source_table_name.upper()}
        index_query = """
            SELECT
                ai.index_name,
                c.constraint_type,
                ai.index_type,
                ai.uniqueness,
                listagg(CASE WHEN coalesce(cols.HIDDEN_COLUMN, 'NO') = 'YES' THEN '('|| aic.column_name ||')' ELSE '"'|| aic.column_name ||'"' END, ', ')
                    WITHIN GROUP (ORDER BY aic.column_position) AS indexed_columns,
                listagg(CASE WHEN coalesce(cols.HIDDEN_COLUMN, 'NO') = 'YES' THEN '('|| aic.column_name ||') '|| aic.descend ELSE '"'|| aic.column_name ||'" '|| aic.descend END, ', ')
                    WITHIN GROUP (ORDER BY aic.column_position) AS indexed_columns_orders,
                sum(CASE WHEN coalesce(cols.HIDDEN_COLUMN, 'NO') = 'YES' THEN 1 ELSE 0 END) AS hidden_columns_count
            FROM all_indexes ai
            JOIN all_ind_columns aic
            ON ai.owner = aic.index_owner AND ai.index_name = aic.index_name
            LEFT JOIN all_tab_cols cols
            ON cols.owner = ai.table_owner AND cols.table_name = ai.table_name AND cols.column_name = aic.column_name
            AND ai.table_owner = aic.table_owner AND ai.table_name = aic.table_name
            LEFT JOIN all_constraints c
            ON c.owner = ai.owner AND c.table_name = ai.table_name AND c.constraint_name = ai.index_name
            WHERE
                ai.table_owner = :owner
                AND ai.table_name = :table_name
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
            cursor.execute(index_query, binds)
            for row in cursor.fetchall():
                index_name = row[0]
                constraint_type = row[1]
                index_type = row[2]
                uniqueness = row[3]
                columns_list = row[4]
                columns_list_orders = row[5]
                hidden_columns_count += int(row[6])

                if index_name not in table_indexes:
                    table_indexes[order_num] = {
                        'index_name': index_name,
                        'index_type': 'PRIMARY KEY' if constraint_type == 'P' else 'UNIQUE' if uniqueness == 'UNIQUE' else 'INDEX',
                        'index_owner': source_table_schema,
                        'index_columns': columns_list if constraint_type == 'P' else columns_list_orders,
                        'index_comment': '',
                        'index_sql': '',
                        'index_hidden_columns_count': int(row[6]),
                    }
                order_num += 1

            for order_num, index_info in table_indexes.items():
                # Fetch the DDL for each index
                try:
                    query = "SELECT DBMS_METADATA.GET_DDL('INDEX', :index_name, :owner) FROM dual"
                    cursor.execute(query, {'index_name': index_info['index_name'].upper(), 'owner': source_table_schema.upper()})
                    ddl = cursor.fetchone()[0]
                    if ddl:
                        ddl = ddl.decode('utf-8') if isinstance(ddl, bytes) else ddl
                        table_indexes[order_num]['index_sql'] = f"{ddl}"
                        self.config_parser.print_log_message('DEBUG', f"oracle_connector: fetch_indexes: Fetched DDL for index {index_info['index_name']}: {ddl}")
                except Exception as e:
                    self.config_parser.print_log_message('ERROR', f"oracle_connector: fetch_indexes: Error fetching DDL for index {index_info['index_name']}: {e}")
                    table_indexes[order_num]['index_sql'] = f"Error fetching DDL: {e}"

            if hidden_columns_count > 0:
                self.config_parser.print_log_message('INFO', f"oracle_connector: fetch_indexes: Table {source_table_schema}.{source_table_name} has {hidden_columns_count} hidden columns in indexes.")
                try:
                    query = "SELECT COLUMN_NAME, DATA_DEFAULT FROM all_tab_cols WHERE owner = :owner AND table_name = :table_name AND hidden_column = 'YES'"
                    cursor.execute(query, binds)
                    hidden_columns = cursor.fetchall()
                    for col in hidden_columns:
                        col_name = col[0]
                        col_default = col[1] if col[1] else 'NULL'
                        self.config_parser.print_log_message('DEBUG', f"oracle_connector: fetch_indexes: Hidden column: {col_name}, Default value: {col_default}")

                        for order_num, index_info in table_indexes.items():
                            if index_info['index_hidden_columns_count'] > 0:
                                if col_name in index_info['index_columns']:
                                    index_info['index_columns'] = index_info['index_columns'].replace(col_name, f"{col_default}")
                                    self.config_parser.print_log_message('DEBUG', f"oracle_connector: fetch_indexes: Updated index {index_info['index_name']} with hidden column {col_name} and default value {col_default}")
                except Exception as e:
                    self.config_parser.print_log_message('ERROR', f"oracle_connector: fetch_indexes: Error fetching hidden columns for table {source_table_schema}.{source_table_name}: {e}")
            cursor.close()
            return table_indexes

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: fetch_indexes: Error executing query: {index_query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_index_sql(self, settings):
        return ""

    def get_indexes_count(self, schema_name: str, table_name: str) -> int:
        query = """
            SELECT count(*)
            FROM all_indexes
            WHERE table_owner = :owner
            AND table_name = :table_name
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, {'owner': schema_name.upper(), 'table_name': table_name.upper()})
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: get_indexes_count: Error: {e}")
            return -1

    def get_schema_indexes_count(self, schema_name: str) -> int:
        query = "SELECT count(*) FROM all_indexes WHERE table_owner = :owner"
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, {'owner': schema_name.upper()})
            count = cursor.fetchone()[0]
            cursor.close()
            self.disconnect()
            return count
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: get_schema_indexes_count: Error: {e}")
            return -1

    def fetch_constraints(self, settings):
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

        order_num = 1
        table_constraints = {}
        binds = {'owner': source_table_schema.upper(), 'table_name': source_table_name.upper()}
        constraints_query = """
            SELECT
                fk_cons.constraint_name AS fk_constraint_name,
                fk_cons.delete_rule,
                fk_cons.status,
                    listagg('"'||fk_col.column_name||'"', ', ') WITHIN GROUP (ORDER BY fk_col.position) AS fk_columns,
                pk_cons.owner AS pk_owner,
                pk_cons.table_name AS pk_table_name,
                pk_cons.constraint_name AS pk_constraint_name,
                    listagg('"'||pk_col.column_name||'"', ', ') WITHIN GROUP (ORDER BY pk_col.position) AS pk_columns
            FROM
                all_constraints fk_cons
            JOIN
                all_cons_columns fk_col ON fk_cons.owner = fk_col.owner
                                        AND fk_cons.constraint_name = fk_col.constraint_name
                                        AND fk_cons.table_name = fk_col.table_name
            JOIN
                all_constraints pk_cons ON fk_cons.r_owner = pk_cons.owner
                                        AND fk_cons.r_constraint_name = pk_cons.constraint_name
            JOIN
                all_cons_columns pk_col ON pk_cons.owner = pk_col.owner
                                        AND pk_cons.constraint_name = pk_col.constraint_name
                                        AND pk_cons.table_name = pk_col.table_name
                                        AND fk_col.position = pk_col.position -- Ensures correct order for composite keys
            WHERE
                fk_cons.constraint_type = 'R'
                AND fk_cons.owner = :owner
                AND fk_cons.table_name = :table_name
            GROUP BY
                fk_cons.constraint_name,
                fk_cons.delete_rule,
                fk_cons.status,
                pk_cons.owner,
                pk_cons.table_name,
                pk_cons.constraint_name
            ORDER BY
                fk_cons.constraint_name
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(constraints_query, binds)
            for row in cursor.fetchall():
                constraint_name = row[0]
                delete_rule = row[1]
                status = row[2]  ## ENABLED
                fk_columns = row[3]
                pk_owner = row[4]
                pk_table_name = row[5]
                pk_constraint_name = row[6]  ## corresponds to the primary key constraint name
                pk_columns = row[7]
                constraint_type = 'FOREIGN KEY'

                if constraint_name not in table_constraints:
                    table_constraints[order_num] = {
                        'constraint_name': constraint_name,
                        'constraint_type': constraint_type,
                        'constraint_owner': source_table_schema,
                        'referenced_table_name': pk_table_name,
                        'referenced_table_schema': pk_owner,
                        'referenced_columns': pk_columns,
                        'constraint_columns': fk_columns,
                        'constraint_sql': '',
                        'constraint_comment': '',
                        'delete_rule': delete_rule,
                        'constraint_status': status,
                    }

                order_num += 1

            # Fetch CHECK constraints.
            # Oracle stores NOT NULL constraints as CHECK constraints (constraint_type = 'C')
            # with a search_condition of the form "COLUMN" IS NOT NULL. Those are part of the
            # column definition (already handled via is_nullable) and must NOT be migrated as
            # separate CHECK constraints.
            check_query = """
                SELECT
                    constraint_name,
                    search_condition,
                    status
                FROM all_constraints
                WHERE constraint_type = 'C'
                    AND owner = :owner
                    AND table_name = :table_name
                ORDER BY constraint_name
            """
            try:
                check_cursor = self.connection.cursor()
                check_cursor.execute(check_query, binds)
                for row in check_cursor.fetchall():
                    check_constraint_name = row[0]
                    # search_condition is a LONG column - python-oracledb returns it as str
                    search_condition = (row[1] or '').strip()
                    check_status = row[2]

                    if not search_condition:
                        continue

                    # Skip Oracle internal NOT NULL constraints - handled by column definition
                    if re.match(r'^\s*"?[\w$#]+"?\s+IS\s+NOT\s+NULL\s*$', search_condition, re.IGNORECASE):
                        self.config_parser.print_log_message('DEBUG3', f"oracle_connector: fetch_constraints: Skipping NOT NULL check constraint {check_constraint_name} ({search_condition})")
                        continue

                    # Oracle quotes and uppercases identifiers in the stored condition
                    # (e.g. "SALARY" > 0). The target connector re-quotes column names itself,
                    # so strip Oracle's identifier quoting to avoid double-quoted identifiers
                    # in the generated CHECK expression.
                    check_expression = search_condition.replace('"', '')

                    table_constraints[order_num] = {
                        'constraint_name': check_constraint_name,
                        'constraint_type': 'CHECK',
                        'constraint_owner': source_table_schema,
                        'referenced_table_name': '',
                        'referenced_table_schema': '',
                        'referenced_columns': '',
                        'constraint_columns': '',
                        'constraint_sql': check_expression,
                        'constraint_comment': '',
                        'delete_rule': '',
                        'constraint_status': check_status,
                    }
                    self.config_parser.print_log_message('DEBUG', f"oracle_connector: fetch_constraints: Found CHECK constraint {check_constraint_name}: {check_expression}")
                    order_num += 1
                check_cursor.close()
            except Exception as e:
                self.config_parser.print_log_message('WARNING', f"oracle_connector: fetch_constraints: Error fetching CHECK constraints for {source_table_schema}.{source_table_name}: {e}")

            cursor.close()
            return table_constraints
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: fetch_constraints: Error executing query: {constraints_query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_constraint_sql(self, settings):
        return ""

    def get_constraints_count(self, schema_name: str, table_name: str) -> int:
        query = """
            SELECT count(*)
            FROM all_constraints
            WHERE owner = :owner
            AND table_name = :table_name
            AND (constraint_type IN ('P', 'U', 'R') OR (constraint_type = 'C' AND generated = 'USER NAME'))
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, {'owner': schema_name.upper(), 'table_name': table_name.upper()})
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: get_constraints_count: Error: {e}")
            return -1

    def get_schema_constraints_count(self, schema_name: str) -> int:
        query = "SELECT count(*) FROM all_constraints WHERE owner = :owner"
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, {'owner': schema_name.upper()})
            count = cursor.fetchone()[0]
            cursor.close()
            self.disconnect()
            return count
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: get_schema_constraints_count: Error: {e}")
            return -1

    def get_aliases(self, settings):
        source_schema_name = settings.get('source_schema_name')
        aliases = {}
        order_num = 1
        query = f"""
            SELECT
                synonym_name AS alias_name,
                table_owner AS aliased_schema_name,
                table_name AS aliased_table_name,
                owner AS alias_owner
            FROM all_synonyms
            WHERE owner = :owner
            ORDER BY synonym_name
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, {'owner': source_schema_name})
            for row in cursor.fetchall():
                alias_name = row[0].strip() if row[0] else ''
                aliased_schema_name = row[1].strip() if row[1] else ''
                aliased_table_name = row[2].strip() if row[2] else ''
                alias_owner = row[3].strip() if row[3] else source_schema_name
                alias_sql = f"CREATE SYNONYM {alias_owner}.{alias_name} FOR {aliased_schema_name}.{aliased_table_name}"

                aliases[order_num] = {
                    'id': order_num,
                    'alias_schema_name': source_schema_name,
                    'alias_name': alias_name,
                    'aliased_schema_name': aliased_schema_name,
                    'aliased_table_name': aliased_table_name,
                    'alias_owner': alias_owner,
                    'alias_sql': alias_sql,
                    'alias_comment': ''
                }
                order_num += 1
            self.disconnect()
            return aliases
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: get_aliases: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_triggers(self, table_id: int, table_schema: str, table_name: str):
        triggers = {}
        order_num = 1
        query = """
            SELECT owner, trigger_name, trigger_type, triggering_event, referencing_names, status
            FROM all_triggers
            WHERE table_owner = :owner
                AND table_name = :table_name
            ORDER BY trigger_name
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, {'owner': table_schema.upper(), 'table_name': table_name.upper()})
            rows = cursor.fetchall()
            for row in rows:
                owner, trigger_name, trigger_type, triggering_event, referencing, status = row
                old_ref, new_ref = 'OLD', 'NEW'
                if referencing:
                    parts = referencing.upper().split()
                    if 'OLD' in parts and parts.index('OLD') + 2 < len(parts):
                        old_ref = parts[parts.index('OLD') + 2]
                    if 'NEW' in parts and parts.index('NEW') + 2 < len(parts):
                        new_ref = parts[parts.index('NEW') + 2]

                # Full CREATE TRIGGER DDL (contains timing/events/level/when + the PL/SQL body)
                trigger_ddl = ''
                try:
                    cursor.execute("SELECT DBMS_METADATA.GET_DDL('TRIGGER', :n, :o) FROM dual", {'n': trigger_name, 'o': owner})
                    r = cursor.fetchone()
                    if r and r[0] is not None:
                        trigger_ddl = r[0].read() if hasattr(r[0], 'read') else str(r[0])
                except Exception as e_md:
                    self.config_parser.print_log_message('DEBUG', f"oracle_connector: fetch_triggers: DBMS_METADATA failed for trigger {trigger_name}: {e_md}")

                triggers[order_num] = {
                    'id': None,
                    'name': trigger_name,
                    'event': triggering_event,
                    'row_statement': 'ROW' if 'EACH ROW' in (trigger_type or '').upper() else 'STATEMENT',
                    'old': old_ref,
                    'new': new_ref,
                    'sql': trigger_ddl,
                    'comment': '',
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return triggers
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"oracle_connector: fetch_triggers: Error fetching triggers for {table_schema}.{table_name}: {e}")
            try:
                self.disconnect()
            except Exception:
                pass
            return triggers

    def convert_trigger(self, settings):
        """
        Convert an Oracle CREATE TRIGGER (row/statement level) into a PostgreSQL trigger
        function plus a CREATE TRIGGER that calls it. Best-effort: the PL/SQL body is converted
        with the same heuristics as convert_funcproc_code, and constructs that cannot be
        translated are flagged for manual review.
        """
        trigger_sql = settings.get('trigger_sql', '') or ''
        target_db_type = settings.get('target_db_type', 'postgresql')
        source_schema_name = settings.get('source_schema_name', '')
        target_schema_name = settings['target_schema_name']
        target_table_name = settings.get('target_table_name', '')
        trigger_name = settings.get('trigger_name', '')

        if target_db_type != 'postgresql' or not trigger_sql.strip():
            return ''

        code = re.sub(r'(?i)\b(?:NON)?EDITIONABLE\b', '', trigger_sql).strip()
        code = re.sub(r'\s*/\s*$', '', code).strip()

        # Locate the PL/SQL block (DECLARE or BEGIN at top level) that follows the header.
        ds, de, _ = self._plsql_find_top_level_kw(code, {'DECLARE', 'BEGIN'})
        if ds == -1:
            self.config_parser.print_log_message('WARNING', f"oracle_connector: convert_trigger: could not locate the body of trigger {trigger_name} (compound/complex trigger?); source preserved, nothing created.")
            return ''
        header = code[:ds]
        body = code[ds:]
        header_u = header.upper()

        if 'COMPOUND TRIGGER' in header_u:
            self.config_parser.print_log_message('WARNING', f"oracle_connector: convert_trigger: {trigger_name} is a COMPOUND trigger, which has no direct PostgreSQL equivalent; source preserved, nothing created.")
            return ''
        if re.search(r'(?i)REFERENCING\b', header) and not re.search(r'(?i)REFERENCING\s+NEW\s+AS\s+NEW\s+OLD\s+AS\s+OLD', header):
            self.config_parser.print_log_message('WARNING', f"oracle_connector: convert_trigger: {trigger_name} uses custom REFERENCING names; the body's correlation names may need manual adjustment (PostgreSQL uses NEW/OLD).")

        # Timing
        if 'INSTEAD OF' in header_u:
            timing = 'INSTEAD OF'
        elif re.search(r'\bAFTER\b', header_u):
            timing = 'AFTER'
        else:
            timing = 'BEFORE'

        # Events (between the timing keyword and ON), e.g. "INSERT OR UPDATE OF col"
        ev = re.search(r'(?is)\b(?:BEFORE|AFTER|INSTEAD\s+OF)\b(.*?)\bON\b', header)
        events = re.sub(r'\s+', ' ', ev.group(1)).strip() if ev else 'INSERT'

        level = 'ROW' if re.search(r'(?is)\bFOR\s+EACH\s+ROW\b', header) else 'STATEMENT'

        # WHEN (condition) - only valid for row-level, non-INSTEAD-OF triggers in PostgreSQL
        when_clause = ''
        wm = re.search(r'(?is)\bWHEN\s*\((.*)\)\s*$', header.strip())
        if wm and level == 'ROW' and timing != 'INSTEAD OF':
            when_clause = re.sub(r'(?i)\bnew\s*\.', 'NEW.', wm.group(1).strip())
            when_clause = re.sub(r'(?i)\bold\s*\.', 'OLD.', when_clause)

        # Convert the body: :NEW/:OLD -> NEW/OLD, trigger predicates, PL/SQL constructs, types.
        body = re.sub(r'(?i):\s*NEW\b', 'NEW', body)
        body = re.sub(r'(?i):\s*OLD\b', 'OLD', body)
        # Oracle trigger predicates INSERTING / UPDATING / DELETING -> TG_OP checks.
        if re.search(r"(?i)\bUPDATING\s*\(", body):
            self.config_parser.print_log_message('WARNING', f"oracle_connector: convert_trigger: {trigger_name} uses column-level UPDATING('col'); mapped to TG_OP = 'UPDATE' (column specificity is lost) - manual review recommended.")
        body = re.sub(r"(?i)\bUPDATING\s*\(\s*'[^']*'\s*\)", "TG_OP = 'UPDATE'", body)
        body = re.sub(r"(?i)\bINSERTING\b", "TG_OP = 'INSERT'", body)
        body = re.sub(r"(?i)\bUPDATING\b", "TG_OP = 'UPDATE'", body)
        body = re.sub(r"(?i)\bDELETING\b", "TG_OP = 'DELETE'", body)
        body = self._map_plsql_datatypes(body)
        body = self._apply_plsql_substitutions(body)
        body = re.sub(r'(?is)\bEND\s+[\w$#]+\s*;\s*$', 'END;', body.strip())
        if not body.rstrip().endswith(';'):
            body += ';'

        # Insert a fall-through RETURN before the outermost END (Oracle trigger bodies never
        # RETURN, but PostgreSQL row-level triggers must return NEW/OLD).
        if timing != 'AFTER':
            ret_stmt = ("IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;"
                        if level == 'ROW' else "RETURN NULL;")
            body = re.sub(r'(?is)\bEND\s*;\s*$', f'{ret_stmt}\nEND;', body, count=1)
        else:
            body = re.sub(r'(?is)\bEND\s*;\s*$', 'RETURN NULL;\nEND;', body, count=1)

        target_trigger_name = self.config_parser.convert_names_case(trigger_name)
        target_table = self.config_parser.convert_names_case(target_table_name)
        func_name = f"{target_trigger_name}_tgfn"

        func_ddl = (f'CREATE OR REPLACE FUNCTION "{target_schema_name}"."{func_name}"() RETURNS TRIGGER AS $$\n'
                    f'{body}\n$$ LANGUAGE plpgsql;')
        when_sql = f' WHEN ({when_clause})' if when_clause else ''
        trigger_ddl = (f'CREATE TRIGGER "{target_trigger_name}" {timing} {events} ON "{target_schema_name}"."{target_table}"\n'
                       f'FOR EACH {level}{when_sql} EXECUTE FUNCTION "{target_schema_name}"."{func_name}"();')

        result = func_ddl + '\n\n' + trigger_ddl
        if source_schema_name:
            result = result.replace(f'"{source_schema_name.upper()}".', f'"{target_schema_name}".')

        self.config_parser.print_log_message('WARNING', f"oracle_connector: convert_trigger: trigger {trigger_name} was converted from PL/SQL to a PL/pgSQL trigger function on a best-effort basis - please review and test before relying on it.")
        return result

    def fetch_funcproc_names(self, schema: str):
        """Fetch standalone functions, procedures and packages from ALL_OBJECTS. The integer
        object_id is used as the id (the protocol table stores it as INTEGER); PACKAGE BODY is
        skipped because the package is fetched as a unit under its PACKAGE (spec) object."""
        funcprocs = {}
        order_num = 1
        query = """
            SELECT object_name, object_id, object_type
            FROM all_objects
            WHERE owner = :owner
                AND object_type IN ('FUNCTION', 'PROCEDURE', 'PACKAGE')
            ORDER BY object_type, object_name
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, {'owner': schema.upper()})
            for row in cursor.fetchall():
                funcprocs[order_num] = {
                    'name': row[0],
                    'id': row[1],
                    'type': row[2],
                    'comment': '',
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return funcprocs
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"oracle_connector: fetch_funcproc_names: Error fetching functions/procedures for schema {schema}: {e}")
            try:
                self.disconnect()
            except Exception:
                pass
            return funcprocs

    def fetch_funcproc_code(self, funcproc_id: int):
        """Fetch the full source of a function/procedure/package by its ALL_OBJECTS object_id,
        preferring DBMS_METADATA.GET_DDL (clean CREATE OR REPLACE output) and falling back to
        reconstructing it from ALL_SOURCE."""
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute("SELECT object_type, object_name, owner FROM all_objects WHERE object_id = :id", {'id': funcproc_id})
            row = cursor.fetchone()
            if not row:
                cursor.close()
                self.disconnect()
                return ''
            object_type, object_name, owner = row
            ddl_type = 'PACKAGE' if object_type in ('PACKAGE', 'PACKAGE BODY') else object_type
            code = ''
            try:
                cursor.execute("SELECT DBMS_METADATA.GET_DDL(:t, :n, :o) FROM dual", {'t': ddl_type, 'n': object_name, 'o': owner})
                r = cursor.fetchone()
                if r and r[0] is not None:
                    code = r[0].read() if hasattr(r[0], 'read') else str(r[0])
            except Exception as e_md:
                self.config_parser.print_log_message('DEBUG', f"oracle_connector: fetch_funcproc_code: DBMS_METADATA failed for {object_name} ({e_md}); falling back to ALL_SOURCE.")
                cursor.execute("""
                    SELECT text FROM all_source
                    WHERE owner = :o AND name = :n
                    ORDER BY (CASE type WHEN 'PACKAGE' THEN 1 WHEN 'PACKAGE BODY' THEN 2 ELSE 0 END), line
                """, {'o': owner, 'n': object_name})
                body = ''.join(t[0] for t in cursor.fetchall() if t[0] is not None)
                code = ('CREATE OR REPLACE ' + body) if body else ''
            cursor.close()
            self.disconnect()
            return code
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: fetch_funcproc_code: Error fetching code for object_id {funcproc_id}: {e}")
            try:
                self.disconnect()
            except Exception:
                pass
            return ''

    def _plsql_find_top_level_kw(self, code, keywords, start=0):
        """Return (start_idx, end_idx, word) of the first keyword found at parenthesis depth 0,
        skipping single-quoted strings and -- / /* */ comments. (-1, -1, None) if not found."""
        i, n, depth = start, len(code), 0
        while i < n:
            c = code[i]
            if c == "'":
                i += 1
                while i < n and code[i] != "'":
                    i += 1
                i += 1
                continue
            if c == '-' and i + 1 < n and code[i + 1] == '-':
                while i < n and code[i] != '\n':
                    i += 1
                continue
            if c == '/' and i + 1 < n and code[i + 1] == '*':
                i += 2
                while i + 1 < n and not (code[i] == '*' and code[i + 1] == '/'):
                    i += 1
                i += 2
                continue
            if c == '(':
                depth += 1
                i += 1
                continue
            if c == ')':
                depth -= 1
                i += 1
                continue
            if depth == 0 and (c.isalpha() or c == '_'):
                j = i
                while j < n and (code[j].isalnum() or code[j] in '_$#'):
                    j += 1
                if code[i:j].upper() in keywords:
                    return i, j, code[i:j].upper()
                i = j
                continue
            i += 1
        return -1, -1, None

    def _map_plsql_datatypes(self, code):
        """Map common Oracle scalar type names to PostgreSQL equivalents (word-boundaried)."""
        mapping = [
            (r'\bVARCHAR2\b', 'VARCHAR'), (r'\bNVARCHAR2\b', 'VARCHAR'), (r'\bNVARCHAR\b', 'VARCHAR'),
            (r'\bNCHAR\b', 'CHAR'), (r'\bNUMBER\b', 'NUMERIC'), (r'\bBINARY_FLOAT\b', 'REAL'),
            (r'\bBINARY_DOUBLE\b', 'DOUBLE PRECISION'), (r'\bPLS_INTEGER\b', 'INTEGER'),
            (r'\bBINARY_INTEGER\b', 'INTEGER'), (r'\bSIMPLE_INTEGER\b', 'INTEGER'),
            (r'\bLONG\s+RAW\b', 'BYTEA'), (r'\bRAW\b', 'BYTEA'), (r'\bCLOB\b', 'TEXT'),
            (r'\bNCLOB\b', 'TEXT'), (r'\bBLOB\b', 'BYTEA'), (r'\bLONG\b', 'TEXT'),
            (r'\bVARCHAR\s*\(\s*(\d+)\s+(?:BYTE|CHAR)\s*\)', r'VARCHAR(\1)'),
        ]
        for pat, repl in mapping:
            code = re.sub(rf'(?i){pat}', repl, code)
        return code

    def _convert_plsql_params(self, params_str):
        """Convert an Oracle parameter list to PostgreSQL form: reorder IN/OUT/IN OUT modes to
        precede the name, drop the default IN, and normalize DEFAULT."""
        params_str = params_str.strip()
        if not params_str:
            return ''
        # split on top-level commas
        parts, depth, cur = [], 0, ''
        for ch in params_str:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            if ch == ',' and depth == 0:
                parts.append(cur)
                cur = ''
            else:
                cur += ch
        if cur.strip():
            parts.append(cur)

        converted = []
        for p in parts:
            p = p.strip()
            m = re.match(r'(?is)^([\w$#]+)\s+(IN\s+OUT|IN|OUT)?\s*(.*)$', p)
            if not m:
                converted.append(p)
                continue
            name, mode, rest = m.group(1), (m.group(2) or '').upper(), m.group(3).strip()
            rest = re.sub(r'(?i)^\s*(?::=|DEFAULT)\s*', 'DEFAULT ', rest) if re.match(r'(?i)^\s*(:=|DEFAULT)', rest) else rest
            # separate DEFAULT clause from the type
            dm = re.match(r'(?is)^(.*?)\s*(?::=|DEFAULT)\s+(.*)$', rest)
            if dm:
                ptype, default = dm.group(1).strip(), dm.group(2).strip()
            else:
                ptype, default = rest, ''
            prefix = 'INOUT ' if mode == 'IN OUT' else ('OUT ' if mode == 'OUT' else '')
            piece = f"{prefix}{name} {ptype}".strip()
            if default:
                piece += f" DEFAULT {default}"
            converted.append(piece)
        return ', '.join(converted)

    def _apply_plsql_substitutions(self, code):
        """Best-effort replacement of common Oracle PL/SQL constructs with PL/pgSQL equivalents."""
        # DBMS_OUTPUT.PUT_LINE(x) -> RAISE NOTICE '%', (x)
        code = re.sub(r'(?i)\bDBMS_OUTPUT\.PUT_LINE\s*\(', "RAISE NOTICE '%', (", code)
        # RAISE_APPLICATION_ERROR(code, msg) -> RAISE EXCEPTION msg (drop the numeric code)
        code = re.sub(r'(?i)\bRAISE_APPLICATION_ERROR\s*\(\s*-?[\w]+\s*,\s*([^;]+?)\)\s*;', r'RAISE EXCEPTION \1;', code)
        # sequence.NEXTVAL / CURRVAL -> nextval('sequence') / currval('sequence')
        code = re.sub(r'(?i)\b([A-Za-z_][\w$#]*)\s*\.\s*NEXTVAL\b', r"nextval('\1')", code)
        code = re.sub(r'(?i)\b([A-Za-z_][\w$#]*)\s*\.\s*CURRVAL\b', r"currval('\1')", code)
        # date/time + null helpers
        code = re.sub(r'(?i)\bSYSTIMESTAMP\b', 'CURRENT_TIMESTAMP', code)
        code = re.sub(r'(?i)\bSYSDATE\b', 'CURRENT_TIMESTAMP', code)
        code = re.sub(r'(?i)\bNVL\s*\(', 'COALESCE(', code)
        # EXECUTE IMMEDIATE -> EXECUTE
        code = re.sub(r'(?i)\bEXECUTE\s+IMMEDIATE\b', 'EXECUTE', code)
        # Oracle's dummy DUAL table
        code = re.sub(r'(?i)\s+FROM\s+dual\b', '', code)
        return code

    def _warn_plsql_constructs(self, code, funcproc_name):
        """Warn about PL/SQL constructs that this best-effort converter does not translate."""
        upper = code.upper()
        checks = [
            ('BULK COLLECT', 'BULK COLLECT'),
            ('FORALL', 'FORALL bulk DML'),
            ('PRAGMA AUTONOMOUS_TRANSACTION', 'PRAGMA AUTONOMOUS_TRANSACTION (no PostgreSQL equivalent)'),
            ('CONNECT BY', 'CONNECT BY hierarchical query'),
            ('%ROWTYPE', '%ROWTYPE (verify - supported in PL/pgSQL but declaration order may differ)'),
        ]
        for needle, desc in checks:
            if needle in upper:
                self.config_parser.print_log_message('WARNING', f"oracle_connector: convert_funcproc_code: {funcproc_name} uses {desc}; manual review of the generated PL/pgSQL is required.")
        # DBMS_* packages other than the OUTPUT calls we rewrite
        for m in set(re.findall(r'(?i)\bDBMS_[A-Z_]+', code)):
            if m.upper() != 'DBMS_OUTPUT':
                self.config_parser.print_log_message('WARNING', f"oracle_connector: convert_funcproc_code: {funcproc_name} calls {m}, which has no automatic PostgreSQL equivalent; manual review required.")

    def convert_funcproc_code(self, settings):
        funcproc_code = settings['funcproc_code']
        if isinstance(funcproc_code, dict):
            funcproc_code = funcproc_code.get('definition', '') or ''
        funcproc_code = (funcproc_code or '').strip()
        target_db_type = settings['target_db_type']
        source_schema_name = settings.get('source_schema_name', '')
        target_schema_name = settings['target_schema_name']
        funcproc_name = settings.get('funcproc_name', '')

        if target_db_type != 'postgresql':
            raise ValueError(f"oracle_connector: convert_funcproc_code: unsupported target database type: {target_db_type}")
        if not funcproc_code:
            return ''

        # Strip DBMS_METADATA / Oracle DDL artifacts
        code = re.sub(r'(?i)\b(?:NON)?EDITIONABLE\b', '', funcproc_code).strip()
        code = re.sub(r'\s*/\s*$', '', code).strip()  # trailing SQL*Plus slash terminator

        self._warn_plsql_constructs(code, funcproc_name)

        # Packages have no direct PostgreSQL equivalent - preserve the source, do not create.
        if re.match(r'(?is)^\s*CREATE\s+(OR\s+REPLACE\s+)?(EDITIONABLE\s+|NONEDITIONABLE\s+)?PACKAGE\b', code):
            self.config_parser.print_log_message('WARNING', f"oracle_connector: convert_funcproc_code: {funcproc_name} is an Oracle PACKAGE, which has no direct PostgreSQL equivalent. It must be split manually into individual functions/procedures (and package state into session settings). Source preserved; nothing created.")
            return ''

        is_function = bool(re.match(r'(?is)^\s*CREATE\s+(OR\s+REPLACE\s+)?FUNCTION\b', code))
        is_procedure = bool(re.match(r'(?is)^\s*CREATE\s+(OR\s+REPLACE\s+)?PROCEDURE\b', code))
        if not (is_function or is_procedure):
            self.config_parser.print_log_message('WARNING', f"oracle_connector: convert_funcproc_code: could not recognize {funcproc_name} as a FUNCTION or PROCEDURE; source preserved, nothing created.")
            return ''

        kind = 'FUNCTION' if is_function else 'PROCEDURE'

        # Extract the object name (schema-qualified or not) right after FUNCTION/PROCEDURE
        header_re = re.match(r'(?is)^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|PROCEDURE)\s+("?[\w$#]+"?(?:\s*\.\s*"?[\w$#]+"?)?)\s*(.*)$', code)
        if not header_re:
            self.config_parser.print_log_message('WARNING', f"oracle_connector: convert_funcproc_code: could not parse header of {funcproc_name}; source preserved, nothing created.")
            return ''
        raw_name = header_re.group(1)
        remainder = header_re.group(2)
        # bare object name (drop any source schema qualifier and quotes); apply the configured
        # target name-case handling for consistency with tables/views/sequences.
        bare_name = raw_name.split('.')[-1].strip().strip('"')
        target_name = self.config_parser.convert_names_case(bare_name)

        # Parameter list (balanced-paren aware via the top-level scanner is overkill here; the
        # params are the first parenthesised group when present)
        params_pg = ''
        after_params = remainder
        if remainder.lstrip().startswith('('):
            depth, idx = 0, remainder.index('(')
            end = None
            k = idx
            in_str = False
            while k < len(remainder):
                ch = remainder[k]
                if ch == "'":
                    in_str = not in_str
                elif not in_str and ch == '(':
                    depth += 1
                elif not in_str and ch == ')':
                    depth -= 1
                    if depth == 0:
                        end = k
                        break
                k += 1
            if end is not None:
                params_pg = self._convert_plsql_params(remainder[idx + 1:end])
                after_params = remainder[end + 1:]

        # Return type (functions) then the IS/AS that starts the block
        return_type = ''
        rm = re.match(r'(?is)^\s*RETURN\s+(.+?)\s+(IS|AS)\b(.*)$', after_params)
        if is_function and rm:
            return_type = self._map_plsql_datatypes(rm.group(1).strip())
            block = rm.group(3)
        else:
            # find the block-introducing IS/AS at top level
            s, e, _ = self._plsql_find_top_level_kw(after_params, {'IS', 'AS'})
            if s == -1:
                self.config_parser.print_log_message('WARNING', f"oracle_connector: convert_funcproc_code: could not locate the IS/AS block of {funcproc_name}; source preserved, nothing created.")
                return ''
            block = after_params[e:]

        # Split declarations (before the first top-level BEGIN) from the executable body
        bs, be, _ = self._plsql_find_top_level_kw(block, {'BEGIN'})
        if bs == -1:
            declarations, body = '', block
        else:
            declarations, body = block[:bs].strip(), block[bs:]

        # Normalize the trailing "END <name>;" -> "END;"
        body = re.sub(r'(?is)\bEND\s+' + re.escape(bare_name) + r'\s*;\s*$', 'END;', body.strip())

        # Assemble PostgreSQL DDL
        header = f'CREATE OR REPLACE {kind} "{target_schema_name}"."{target_name}"({params_pg})'
        if kind == 'FUNCTION':
            header += f' RETURNS {return_type or "void"}'
        inner = (f"DECLARE\n{declarations}\n" if declarations else '') + body
        result = f"{header}\nAS $$\n{inner}\n$$ LANGUAGE plpgsql;"

        # Type mapping + Oracle->PG construct substitutions across the whole function
        result = self._map_plsql_datatypes(result)
        result = self._apply_plsql_substitutions(result)
        result = self.apply_sql_functions_mapping(result, settings)

        # Re-point source-schema-qualified references to the target schema
        if source_schema_name:
            result = result.replace(f'"{source_schema_name.upper()}".', f'"{target_schema_name}".')

        self.config_parser.print_log_message('WARNING', f"oracle_connector: convert_funcproc_code: {funcproc_name or bare_name} was converted from PL/SQL to PL/pgSQL on a best-effort basis - please review and test the generated function before relying on it.")
        return result

    def fetch_sequences(self, schema_name: str):
        """
        Fetch standalone sequences owned by the given schema from ALL_SEQUENCES.
        The current position (LAST_NUMBER) is used as the target START WITH so the
        migrated sequence continues from where the source left off. The actual
        CREATE SEQUENCE on the target is generated by migrate_sequences().
        """
        sequences = {}
        order_num = 1
        query = f"""
            SELECT
                sequence_name,
                min_value,
                max_value,
                increment_by,
                cycle_flag,
                cache_size,
                last_number
            FROM all_sequences
            WHERE sequence_owner = :owner
            ORDER BY sequence_name
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, {'owner': schema_name.upper()})
            for row in cursor.fetchall():
                sequence_name = row[0]
                min_value = row[1]
                max_value = row[2]
                increment_by = row[3]
                cycle_flag = row[4]
                cache_size = row[5]
                last_number = row[6]
                is_cycled = 'YES' if str(cycle_flag or '').upper() in ('Y', 'YES') else 'NO'

                source_sequence_sql = (
                    f'CREATE SEQUENCE "{schema_name}"."{sequence_name}" '
                    f'MINVALUE {min_value} MAXVALUE {max_value} '
                    f'INCREMENT BY {increment_by} START WITH {last_number} '
                    f'CACHE {cache_size} {"CYCLE" if is_cycled == "YES" else "NOCYCLE"}'
                )

                sequences[order_num] = {
                    'sequence_name': sequence_name,
                    'id': order_num,
                    'table_name': None,
                    'column_name': None,
                    'source_sequence_sql': source_sequence_sql,
                    'source_start_value': last_number,
                    'source_increment_by': increment_by,
                    'source_minvalue': min_value,
                    'source_maxvalue': max_value,
                    'source_cache': cache_size,
                    'source_is_cycled': is_cycled,
                }
                self.config_parser.print_log_message('DEBUG', f"oracle_connector: fetch_sequences: Found sequence {sequence_name} (start {last_number}, increment {increment_by}).")
                order_num += 1
            cursor.close()
            self.disconnect()
            return sequences
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"oracle_connector: fetch_sequences: Error fetching sequences for schema {schema_name}: {e}")
            try:
                self.disconnect()
            except Exception:
                pass
            return sequences

    def fetch_views_names(self, source_schema_name: str):
        # Regular views (ALL_VIEWS) and materialized views (ALL_MVIEWS) are returned together,
        # tagged with view_type so the target creates CREATE VIEW / CREATE MATERIALIZED VIEW.
        views = {}
        order_num = 1
        query = """
            SELECT view_name, 'VIEW' AS view_type
            FROM all_views
            WHERE owner = :owner
            UNION ALL
            SELECT mview_name AS view_name, 'MATERIALIZED VIEW' AS view_type
            FROM all_mviews
            WHERE owner = :owner
            ORDER BY 1
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, {'owner': source_schema_name.upper()})
            for row in cursor.fetchall():
                views[order_num] = {
                    'id': None,
                    'schema_name': source_schema_name,
                    'view_name': row[0],
                    'comment': '',
                    'view_type': row[1],
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return views
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: fetch_views_names: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_view_code(self, settings):
        source_schema_name = settings['source_schema_name']
        source_view_name = settings['source_view_name']
        binds = {'owner': source_schema_name.upper(), 'view_name': source_view_name.upper()}
        # ALL_VIEWS.TEXT / ALL_MVIEWS.QUERY both hold only the defining query (no CREATE prefix).
        view_query = "SELECT text FROM all_views WHERE owner = :owner AND view_name = :view_name"
        mview_query = "SELECT query FROM all_mviews WHERE owner = :owner AND mview_name = :view_name"
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(view_query, binds)
            row = cursor.fetchone()
            if row is None:
                # Not a plain view - fall back to a materialized view definition
                cursor.execute(mview_query, binds)
                row = cursor.fetchone()
            view_code = row[0] if row and row[0] is not None else ''
            cursor.close()
            self.disconnect()
            return view_code
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: fetch_view_code: Error fetching view/mview code for {source_schema_name}.{source_view_name}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def _warn_unconvertible_oracle_sql(self, sql, view_label):
        """Log warnings for Oracle constructs that cannot be reliably auto-converted, so they
        get manual review instead of silently producing wrong PostgreSQL.
        (Oracle (+) outer joins are handled by _convert_marked_outer_joins and warned about
        separately only when a specific condition could not be converted.)"""
        if not sql:
            return
        upper = sql.upper()
        issues = []
        if 'CONNECT BY' in upper or 'START WITH' in upper:
            issues.append("Oracle CONNECT BY / START WITH hierarchical query - needs a PostgreSQL recursive CTE")
        if re.search(r'\bROWNUM\b', upper):
            issues.append("Oracle ROWNUM - use LIMIT or a window function in PostgreSQL")
        if 'LISTAGG' in upper:
            issues.append("Oracle LISTAGG - use STRING_AGG in PostgreSQL")
        for issue in issues:
            self.config_parser.print_log_message('WARNING', f"oracle_connector: convert_view_code: view {view_label} contains {issue}. Manual review of the generated view is recommended.")

    def _preprocess_oracle_outer_joins(self, sql):
        """Turn Oracle (+) outer-join operators into inline comment markers on the '=' so the
        parser attaches them to the EQ node (reuses the Sybase ASE *=/=* marker technique).
        'col = col(+)' -> right side is null-supplying -> LEFT outer.
        'col(+) = col' -> left side is null-supplying  -> RIGHT outer."""
        if not sql or '(+)' not in sql:
            return sql
        sql = re.sub(r'([\w."]+)\s*=\s*([\w."]+)\s*\(\s*\+\s*\)', r'\1 = /* left_outer */ \2', sql)
        sql = re.sub(r'([\w."]+)\s*\(\s*\+\s*\)\s*=\s*([\w."]+)', r'\1 = /* right_outer */ \2', sql)
        return sql

    def _convert_marked_outer_joins(self, expression):
        """Rewrite comment-marked equality predicates in the WHERE clause into ANSI LEFT/RIGHT
        JOINs. In sqlglot's model the extra comma-separated tables are implicit joins on the
        SELECT, so the null-supplying table's implicit join becomes a LEFT JOIN; if that table is
        the FROM anchor, the preserved table's join becomes a RIGHT JOIN instead. Returns
        (expression, unconverted_count)."""
        unconverted = 0
        for select_node in expression.find_all(sqlglot.exp.Select):
            where = select_node.args.get('where')
            joins = select_node.args.get('joins') or []
            if not where or not joins:
                continue
            join_by_alias = {}
            for j in joins:
                t = j.this
                if t is not None and t.alias_or_name:
                    join_by_alias[t.alias_or_name] = j
            for eq in list(where.find_all(sqlglot.exp.EQ)):
                if not eq.comments:
                    continue
                if any('left_outer' in c for c in eq.comments):
                    null_col, preserved_col = eq.right, eq.left
                elif any('right_outer' in c for c in eq.comments):
                    null_col, preserved_col = eq.left, eq.right
                else:
                    continue
                if not isinstance(null_col, sqlglot.exp.Column) or not null_col.table:
                    unconverted += 1
                    continue
                join_kind = 'LEFT'
                target_join = join_by_alias.get(null_col.table)
                if target_join is None and isinstance(preserved_col, sqlglot.exp.Column) and preserved_col.table:
                    # null-supplying table is the FROM anchor - RIGHT JOIN the preserved table
                    target_join = join_by_alias.get(preserved_col.table)
                    join_kind = 'RIGHT'
                if target_join is None:
                    unconverted += 1
                    continue
                cond = eq.copy()
                cond.comments = None
                existing_on = target_join.args.get('on')
                if existing_on is not None:
                    cond = sqlglot.exp.And(this=existing_on, expression=cond)
                target_join.set('kind', target_join.args.get('kind') or join_kind)
                target_join.set('on', cond)
                eq.replace(sqlglot.exp.Boolean(this=True))
        return expression, unconverted

    def _postfix_oracle_to_pg_sql(self, sql):
        """Targeted fixes for Oracle constructs sqlglot leaves as-is or mis-handles."""
        if not sql:
            return sql
        # sqlglot renders SYSTIMESTAMP as SYSTIMESTAMP() which does not exist in PostgreSQL
        sql = re.sub(r'(?i)\bSYSTIMESTAMP\s*\(\s*\)', 'CURRENT_TIMESTAMP', sql)
        sql = re.sub(r'(?i)\bSYSTIMESTAMP\b', 'CURRENT_TIMESTAMP', sql)
        # sequence.NEXTVAL / sequence.CURRVAL -> nextval('sequence') / currval('sequence')
        sql = re.sub(r'(?i)\b([A-Za-z_][\w$#]*)\s*\.\s*NEXTVAL\b', r"nextval('\1')", sql)
        sql = re.sub(r'(?i)\b([A-Za-z_][\w$#]*)\s*\.\s*CURRVAL\b', r"currval('\1')", sql)
        # Oracle's dummy DUAL table - PostgreSQL allows SELECT without FROM
        sql = re.sub(r'(?i)\s+FROM\s+dual\b', '', sql)
        # LISTAGG(expr, 'delim') WITHIN GROUP (ORDER BY cols) -> STRING_AGG(expr, 'delim' ORDER BY cols).
        # Conservative: only the common form (simple expr/order-by without nested parens); anything
        # more complex is left for the manual review flagged by _warn_unconvertible_oracle_sql.
        sql = re.sub(
            r"(?i)\bLISTAGG\s*\(\s*([^,()]+?)\s*,\s*('[^']*')\s*\)\s*WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+([^()]+?)\s*\)",
            r"STRING_AGG(\1, \2 ORDER BY \3)",
            sql,
        )
        # Tidy the boolean placeholders left by outer-join extraction (all semantics-preserving:
        # "WHERE TRUE AND x" == "WHERE x", "x AND TRUE" == "x", "WHERE TRUE" == no filter).
        sql = re.sub(r'(?i)\bWHERE\s+TRUE\s+AND\s+', 'WHERE ', sql)
        sql = re.sub(r'(?i)\s+AND\s+TRUE\b', '', sql)
        sql = re.sub(r'(?i)\bWHERE\s+TRUE\b(?=\s*(?:;|\)|$|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|UNION|INTERSECT|EXCEPT))', '', sql)
        return sql

    def convert_view_code(self, settings: dict):
        view_code = settings['view_code'] or ''
        view_type = settings.get('view_type', 'VIEW')
        source_schema_name = settings.get('source_schema_name', '')
        target_schema_name = settings['target_schema_name']
        target_view_name = settings.get('target_view_name', '')
        view_label = f"{source_schema_name}.{target_view_name}" if source_schema_name else target_view_name

        # Surface constructs that cannot be reliably auto-converted before touching the SQL.
        self._warn_unconvertible_oracle_sql(view_code, view_label)

        # Parse the Oracle defining query and generate PostgreSQL (handles NVL/DECODE/SYSDATE/
        # SUBSTR/INSTR/MINUS/REGEXP/MOD/analytic functions/casts, etc.), rewriting Oracle (+)
        # outer joins into ANSI LEFT/RIGHT JOINs along the way. Fall back to the raw query on any
        # parse failure so the view is still stored (with a warning) for manual fixing.
        converted = view_code
        try:
            marked = self._preprocess_oracle_outer_joins(view_code)
            ast = sqlglot.parse_one(marked, read="oracle")
            ast, unconverted_joins = self._convert_marked_outer_joins(ast)
            converted = ast.sql(dialect="postgres")
            # Strip any outer-join markers that could not be converted, then warn about them
            converted = re.sub(r'\s*/\*\s*(?:left|right)_outer\s*\*/\s*', ' ', converted)
            if unconverted_joins:
                self.config_parser.print_log_message('WARNING', f"oracle_connector: convert_view_code: view {view_label} has {unconverted_joins} Oracle (+) outer-join condition(s) that could not be converted to ANSI joins (they remain as inner-join conditions). Manual review required.")
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"oracle_connector: convert_view_code: sqlglot conversion of view {view_label} failed ({e}); using the raw Oracle definition. Manual review required.")
            converted = view_code

        converted = self._postfix_oracle_to_pg_sql(converted)

        # Re-point any source-schema-qualified references to the target schema (both the Oracle
        # canonical quoted-upper form and an unquoted any-case form). Unqualified references are
        # resolved by the target search_path set by the orchestrator before view creation.
        if source_schema_name:
            converted = converted.replace(f'"{source_schema_name.upper()}".', f'"{target_schema_name}".')
            converted = re.sub(rf'(?i)\b{re.escape(source_schema_name)}\s*\.', f'"{target_schema_name}".', converted)
            converted = converted.replace('""', '"')

        # ALL_VIEWS.TEXT / ALL_MVIEWS.QUERY store only the defining query, so wrap it into a
        # full CREATE [MATERIALIZED] VIEW statement (view_type is 'VIEW' or 'MATERIALIZED VIEW').
        ddl = f'CREATE {view_type} "{target_schema_name}"."{target_view_name}" AS {converted.strip()}'
        if not ddl.rstrip().endswith(';'):
            ddl += ';'
        return ddl

    def get_sequence_current_value(self, sequence_id: int):
        # Placeholder for fetching sequence current value
        return None

    def execute_query(self, query: str, params=None):
        try:
            self.connect()
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            cursor.close()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: execute_query: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def execute_sql_script(self, script_path: str):
        try:
            self.connect()
            with open(script_path, 'r') as file:
                script = file.read()
            cursor = self.connection.cursor()
            cursor.execute(script)
            cursor.close()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: execute_sql_script: Error executing SQL script: {script_path}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def begin_transaction(self):
        self.connection.begin()

    def commit_transaction(self):
        self.connection.commit()

    def rollback_transaction(self):
        self.connection.rollback()

    def get_rows_count(self, table_schema: str, table_name: str, migration_limitation: str = None):
        # Table name is a dynamic identifier and the limitation is arbitrary SQL, so neither
        # can be bound; only the connection is ensured here.
        query = f"SELECT COUNT(*) FROM {table_schema}.{table_name}"
        if migration_limitation:
            query += f" WHERE {migration_limitation}"
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: get_rows_count: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_table_size(self, table_schema: str, table_name: str):
        # Best-effort on-disk size in bytes from DBA_SEGMENTS (requires DBA privileges).
        # Reporting-only and not called by the core migration, so degrade to None otherwise.
        query = """
            SELECT NVL(SUM(bytes), 0)
            FROM dba_segments
            WHERE owner = :owner AND segment_name = :table_name AND segment_type = 'TABLE'
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, {'owner': table_schema.upper(), 'table_name': table_name.upper()})
            row = cursor.fetchone()
            cursor.close()
            return row[0] if row else None
        except Exception as e:
            self.config_parser.print_log_message('DEBUG', f"oracle_connector: get_table_size: Could not determine size for {table_schema}.{table_name} (DBA_SEGMENTS may require DBA privileges): {e}")
            return None

    def get_table_next_identity(self, table_schema: str, table_name: str):
        try:
            self.connect()
            # Check for Oracle 12c+ identity columns
            query = """
                SELECT s.LAST_NUMBER
                FROM ALL_TAB_IDENTITY_COLS i
                JOIN ALL_SEQUENCES s ON i.sequence_name = s.sequence_name AND i.owner = s.sequence_owner
                WHERE i.owner = :owner AND i.table_name = :table_name
            """
            cursor = self.connection.cursor()
            cursor.execute(query, {'owner': table_schema.upper(), 'table_name': table_name.upper()})
            row = cursor.fetchone()
            cursor.close()
            if row and row[0] is not None:
                return int(row[0])
            return None
        except Exception as e:
            # Table or view doesn't exist (e.g. Oracle < 12c)
            return None

    def _oracle_type_to_pg(self, ora_type, length, precision, scale, types_mapping):
        """Map an Oracle scalar type (+ length/precision/scale) to a PostgreSQL type string."""
        ora_type_up = (ora_type or '').upper()
        pg_type = types_mapping.get(ora_type_up, ora_type_up)
        if self.is_string_type(ora_type_up) and length:
            pg_type += f"({length})"
        elif self.is_numeric_type(ora_type_up) and precision:
            if scale:
                pg_type += f"({precision}, {scale})"
            else:
                pg_type += f"({precision})"
        return pg_type

    def fetch_user_defined_types(self, schema: str):
        """
        Fetch Oracle user-defined types (object types and collection types).
        - OBJECT types are mapped to PostgreSQL composite types (CREATE TYPE ... AS (...)).
        - COLLECTION types (VARRAY / nested table) are mapped to a PostgreSQL array domain
          (CREATE DOMAIN ... AS <element_type>[]).
        Note: PL/SQL scalar SUBTYPEs are not schema objects and are not handled here.
        Oracle SQL-standard domains (23ai+) are handled by fetch_domains().
        """
        user_defined_types = {}
        order_num = 1
        # Only PostgreSQL is supported as target by this connector
        types_mapping = {k.upper(): v for k, v in self.get_types_mapping({'target_db_type': 'postgresql'}).items()}
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT type_name, typecode
                FROM all_types
                WHERE owner = :owner
                    AND typecode IN ('OBJECT', 'COLLECTION')
                ORDER BY type_name
            """, {'owner': schema.upper()})
            type_rows = cursor.fetchall()

            for type_name, typecode in type_rows:
                type_sql = ''
                if typecode == 'OBJECT':
                    cursor.execute("""
                        SELECT attr_name, attr_type_name, length, precision, scale
                        FROM all_type_attrs
                        WHERE owner = :owner AND type_name = :type_name
                        ORDER BY attr_no
                    """, {'owner': schema.upper(), 'type_name': type_name})
                    attr_defs = []
                    for attr_name, attr_type_name, length, precision, scale in cursor.fetchall():
                        pg_type = self._oracle_type_to_pg(attr_type_name, length, precision, scale, types_mapping)
                        attr_defs.append(f'"{attr_name}" {pg_type}')
                    if not attr_defs:
                        self.config_parser.print_log_message('DEBUG', f"oracle_connector: fetch_user_defined_types: Object type {type_name} has no scalar attributes - skipping.")
                        continue
                    type_sql = f'CREATE TYPE "{schema}"."{type_name}" AS (' + ', '.join(attr_defs) + ');'
                else:  # COLLECTION - VARRAY or nested table
                    cursor.execute("""
                        SELECT elem_type_name, length, precision, scale
                        FROM all_coll_types
                        WHERE owner = :owner AND type_name = :type_name
                    """, {'owner': schema.upper(), 'type_name': type_name})
                    coll = cursor.fetchone()
                    if not coll or not coll[0]:
                        self.config_parser.print_log_message('DEBUG', f"oracle_connector: fetch_user_defined_types: Collection type {type_name} element type could not be resolved - skipping.")
                        continue
                    pg_elem_type = self._oracle_type_to_pg(coll[0], coll[1], coll[2], coll[3], types_mapping)
                    type_sql = f'CREATE DOMAIN "{schema}"."{type_name}" AS {pg_elem_type}[];'

                user_defined_types[order_num] = {
                    'schema_name': schema,
                    'type_name': type_name,
                    'base_type': '',
                    'sql': type_sql,
                    'comment': '',
                }
                self.config_parser.print_log_message('DEBUG', f"oracle_connector: fetch_user_defined_types: {typecode} type {type_name} -> {type_sql}")
                order_num += 1

            cursor.close()
            self.disconnect()
            return user_defined_types
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"oracle_connector: fetch_user_defined_types: Error fetching user-defined types for schema {schema}: {e}")
            try:
                self.disconnect()
            except Exception:
                pass
            return user_defined_types

    def fetch_domains(self, schema: str):
        """
        Fetch Oracle SQL-standard domains.
        Oracle only introduced SQL domains in 23ai (ALL_DOMAINS view). Older releases
        (11g/12c/19c/21c) have no domain objects, so this returns {} on those versions
        (the ALL_DOMAINS query fails and is handled gracefully).
        Object types, VARRAYs and nested tables are handled by fetch_user_defined_types().
        """
        domains = {}
        order_num = 1
        types_mapping = {k.upper(): v for k, v in self.get_types_mapping({'target_db_type': 'postgresql'}).items()}
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT owner, name
                FROM all_domains
                WHERE owner = :owner
                ORDER BY name
            """, {'owner': schema.upper()})
            domain_rows = cursor.fetchall()

            for domain_owner, domain_name in domain_rows:
                # Resolve the base data type of a single-column domain
                domain_data_type = 'TEXT'
                try:
                    cursor.execute("""
                        SELECT data_type, data_length, data_precision, data_scale
                        FROM all_domain_cols
                        WHERE owner = :owner AND domain_name = :domain_name
                        ORDER BY column_id
                        FETCH FIRST 1 ROWS ONLY
                    """, {'owner': domain_owner, 'domain_name': domain_name})
                    dcol = cursor.fetchone()
                    if dcol and dcol[0]:
                        domain_data_type = self._oracle_type_to_pg(dcol[0], dcol[1], dcol[2], dcol[3], types_mapping)
                except Exception as e_col:
                    self.config_parser.print_log_message('DEBUG', f"oracle_connector: fetch_domains: Could not resolve base type for domain {domain_name}: {e_col}")

                source_domain_sql = ''
                try:
                    cursor.execute("SELECT DBMS_METADATA.GET_DDL('DOMAIN', :domain_name, :owner) FROM dual", {'domain_name': domain_name, 'owner': domain_owner})
                    ddl_row = cursor.fetchone()
                    if ddl_row and ddl_row[0] is not None:
                        source_domain_sql = ddl_row[0].read() if hasattr(ddl_row[0], 'read') else str(ddl_row[0])
                except Exception as e_ddl:
                    self.config_parser.print_log_message('DEBUG', f"oracle_connector: fetch_domains: Could not fetch DDL for domain {domain_name}: {e_ddl}")

                domains[order_num] = {
                    'domain_schema': domain_owner,
                    'domain_name': domain_name,
                    'source_domain_sql': source_domain_sql,
                    'domain_data_type': domain_data_type,
                    'source_domain_check_sql': '',
                    'domain_comment': '',
                }
                self.config_parser.print_log_message('DEBUG', f"oracle_connector: fetch_domains: Found domain {domain_name} (base type {domain_data_type}).")
                order_num += 1

            cursor.close()
            self.disconnect()
            return domains
        except Exception as e:
            # Expected on Oracle < 23ai (ALL_DOMAINS does not exist) - return {} silently
            self.config_parser.print_log_message('DEBUG', f"oracle_connector: fetch_domains: No domains fetched (Oracle < 23ai or unsupported): {e}")
            try:
                self.disconnect()
            except Exception:
                pass
            return domains

    def get_create_domain_sql(self, settings):
        # Placeholder for generating CREATE DOMAIN SQL
        return ""

    def fetch_default_values(self, settings) -> dict:
        # Placeholder for fetching default values
        return {}

    def get_table_description(self, settings) -> dict:
        self.config_parser.print_log_message('DEBUG3', f"oracle_connector: get_table_description: Oracle connector: Getting table description for {settings['table_schema']}.{settings['table_name']}")
        table_schema = settings['table_schema']
        table_name = settings['table_name']
        output = ""
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute("SELECT dbms_metadata.get_ddl('TABLE', :table_name, :owner) FROM dual", {'table_name': table_name, 'owner': table_schema})

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
            self.config_parser.print_log_message('ERROR', f"oracle_connector: get_table_description: Error fetching table description for {table_schema}.{table_name}: {e}")
            raise

        return { 'table_description': output.strip() }


    def testing_select(self):
        return "SELECT 1 FROM DUAL"

    def get_database_version(self):
        query = "SELECT * FROM v$version"
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            version_info = cursor.fetchall()
            cursor.close()
            self.disconnect()
            return version_info
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: get_database_version: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_database_size(self):
        # DBA_DATA_FILES requires DBA privileges and has no ALL_* equivalent; this is a
        # reporting-only metric, so degrade gracefully to None for non-DBA accounts.
        query = """
            SELECT SUM(bytes) / 1024 / 1024 AS size_mb
            FROM dba_data_files
            WHERE tablespace_name NOT IN ('SYSTEM', 'SYSAUX')
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            size_mb = cursor.fetchone()[0]
            cursor.close()
            self.disconnect()
            return size_mb
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"oracle_connector: get_database_size: Could not determine database size (DBA_DATA_FILES may require DBA privileges): {e}")
            try:
                self.disconnect()
            except Exception:
                pass
            return None

    def get_top_n_tables(self, settings):
        top_tables = {}
        top_tables['by_rows'] = {}
        top_tables['by_size'] = {}
        top_tables['by_columns'] = {}
        top_tables['by_indexes'] = {}
        top_tables['by_constraints'] = {}

        source_schema_name = settings.get('source_schema_name', None)
        owner_bind = source_schema_name.upper() if source_schema_name else None
        # NOTE: row counts come from ALL_TABLES.NUM_ROWS (Oracle optimizer statistics). They
        # are an estimate and may be stale or NULL if statistics were not recently gathered
        # (DBMS_STATS). This is intentional for a fast pre-migration overview - an exact
        # COUNT(*) per table would be prohibitively expensive on large schemas.
        try:
            top_n = self.config_parser.get_top_n_tables_by_rows()
            if top_n > 0:
                # Preferred query includes on-disk size from DBA_SEGMENTS (requires DBA privileges).
                size_query = f"""
                    SELECT
                    t.owner,
                    t.table_name,
                    nvl(t.num_rows, 0) AS row_count,
                    ROUND((s.bytes / 1024 / 1024), 2) AS row_size
                    FROM all_tables t
                    LEFT JOIN dba_segments s
                    ON t.owner = s.owner AND t.table_name = s.segment_name AND s.segment_type = 'TABLE'
                    WHERE (:owner IS NULL OR t.owner = :owner)
                    ORDER BY nvl(t.num_rows, 0) DESC
                    FETCH FIRST {top_n} ROWS ONLY
                """
                # Fallback without DBA_SEGMENTS so non-DBA accounts still get row counts.
                fallback_query = f"""
                    SELECT
                    t.owner,
                    t.table_name,
                    nvl(t.num_rows, 0) AS row_count,
                    NULL AS row_size
                    FROM all_tables t
                    WHERE (:owner IS NULL OR t.owner = :owner)
                    ORDER BY nvl(t.num_rows, 0) DESC
                    FETCH FIRST {top_n} ROWS ONLY
                """
                self.connect()
                cursor = self.connection.cursor()
                try:
                    cursor.execute(size_query, {'owner': owner_bind})
                    tables = cursor.fetchall()
                except Exception as e_seg:
                    self.config_parser.print_log_message('WARNING', f"oracle_connector: get_top_n_tables: DBA_SEGMENTS not accessible ({e_seg}); falling back to row counts without on-disk size.")
                    cursor.execute(fallback_query, {'owner': owner_bind})
                    tables = cursor.fetchall()
                cursor.close()
                self.disconnect()

                for order_num, row in enumerate(tables, start=1):
                    top_tables['by_rows'][order_num] = {
                        'owner': row[0].strip(),
                        'table_name': row[1].strip(),
                        'row_count': row[2],
                        'row_size': row[3],
                    }
                self.config_parser.print_log_message('DEBUG2', f"oracle_connector: get_top_n_tables: Top {top_n} tables by rows: {top_tables['by_rows']}")
            else:
                self.config_parser.print_log_message('DEBUG', "oracle_connector: get_top_n_tables: Top N tables by rows is not configured or set to 0, skipping this part.")

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: get_top_n_tables: Error fetching top N tables: {e}")

        return top_tables

    def get_top_fk_dependencies(self, settings):
        top_fk_dependencies = {}
        return top_fk_dependencies

    def target_table_exists(self, target_schema_name, target_table_name):
        query = """
            SELECT COUNT(*)
            FROM all_tables
            WHERE owner = :owner
            AND table_name = :table_name
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, {'owner': target_schema_name.upper(), 'table_name': target_table_name.upper()})
            exists = cursor.fetchone()[0] > 0
            cursor.close()
            self.disconnect()
            return exists
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: target_table_exists: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_all_rows(self, query):
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: fetch_all_rows: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def convert_default_value(self, settings) -> dict:
        extracted_default_value = settings['extracted_default_value']
        return extracted_default_value

    def get_table_checksum(self, schema_name: str, table_name: str, columns: list):
        if not columns:
            return None

        cols_list = []
        for col in columns:
            dtype = col.get('data_type', '').lower()
            if any(x in dtype for x in ['lob', 'bfile', 'xml', 'json', 'long']):
                continue
            if dtype == 'date':
                cols_list.append(f"CASE WHEN \"{col['column_name']}\" IS NOT NULL THEN TO_CHAR(\"{col['column_name']}\", 'YYYY-MM-DD HH24:MI:SS') || '.000000' ELSE NULL END")
            elif 'time' in dtype:
                cols_list.append(f"TO_CHAR(\"{col['column_name']}\", 'YYYY-MM-DD HH24:MI:SS.FF6')")
            elif col.get('_force_round_0'):
                cols_list.append(f"ROUND(\"{col['column_name']}\", 0)")
            else:
                cols_list.append(f'"{col["column_name"]}"')

        if not cols_list:
            return None

        cols_str = ", ".join(cols_list)
        query = f'SELECT {cols_str} FROM "{schema_name.upper()}"."{table_name.upper()}"'
        return self._compute_python_table_checksum(query)

    def get_random_pks(self, schema_name: str, table_name: str, pk_columns: list, sample_size: int):
        if not pk_columns:
            return []
        cols = ", ".join([f'"{c}"' for c in pk_columns])
        query = f'SELECT * FROM (SELECT {cols} FROM "{schema_name.upper()}"."{table_name.upper()}" ORDER BY DBMS_RANDOM.VALUE) WHERE ROWNUM <= {sample_size}'
        pks = []
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                pks.append(dict(zip(pk_columns, row)))
            cursor.close()
            self.disconnect()
            return pks
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: get_random_pks: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            return []

    def get_row_checksums(self, schema_name: str, table_name: str, pk_columns: list, pk_values_list: list, columns: list):
        if not columns or not pk_columns or not pk_values_list:
            return {}

        cols_list = []
        for col in columns:
            dtype = col.get('data_type', '').lower()
            if any(x in dtype for x in ['lob', 'bfile', 'xml', 'json', 'long']):
                continue
            if dtype == 'date':
                cols_list.append(f"CASE WHEN \"{col['column_name']}\" IS NOT NULL THEN TO_CHAR(\"{col['column_name']}\", 'YYYY-MM-DD HH24:MI:SS') || '.000000' ELSE NULL END")
            elif 'time' in dtype:
                cols_list.append(f"TO_CHAR(\"{col['column_name']}\", 'YYYY-MM-DD HH24:MI:SS.FF6')")
            elif col.get('_force_round_0'):
                cols_list.append(f"ROUND(\"{col['column_name']}\", 0)")
            else:
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

        query = f'SELECT {pk_cols_str}, {cols_str} FROM "{schema_name.upper()}"."{table_name.upper()}" WHERE {where_clause}'
        return self._compute_python_row_checksums(query, len(pk_columns))

    def get_lob_sizes(self, schema_name: str, table_name: str, pk_columns: list, pk_values_list: list, lob_columns: list):
        if not lob_columns or not pk_columns or not pk_values_list:
            return {}

        size_cols = [f"DBMS_LOB.GETLENGTH(\"{col['column_name']}\")" for col in lob_columns]
        size_selects = ", ".join(size_cols)
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

        query = f'SELECT {pk_cols_str}, {size_selects} FROM "{schema_name.upper()}"."{table_name.upper()}" WHERE {where_clause}'

        sizes = {}
        cursor = None
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                pk_tuple = tuple(row[:len(pk_columns)])
                pk_key = pk_tuple[0] if len(pk_tuple) == 1 else pk_tuple
                sizes[pk_key] = row[len(pk_columns):]
            return sizes
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: get_lob_sizes: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            return {}
        finally:
            if cursor is not None:
                cursor.close()
            self.disconnect()

if __name__ == "__main__":
    print("This script is not meant to be run directly")
