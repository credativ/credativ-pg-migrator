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
        try:
            self.connection = ibm_db_dbi.connect(connection_string, "", "")
            if not self.connection:
                raise Exception("Failed to connect to the database")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Unexpected error while conneting into the database: {e}")
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
            return {}
        else:
            self.config_parser.print_log_message('ERROR', f"Unsupported target database type: {target_db_type}")

    def fetch_table_names(self, table_schema: str):
        query = f"""
            SELECT
                TABLEID,
                TABNAME,
                REMARKS
            FROM SYSCAT.TABLES
            WHERE TABSCHEMA = upper('{table_schema}')
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
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
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
                        "REMARKS"
                    FROM SYSCAT.COLUMNS
                    WHERE TABSCHEMA = upper('{table_schema}') AND tabname = '{table_name}' ORDER BY COLNO
                """
            elif self.config_parser.get_system_catalog() in ('SYSIBM'):
                query = f"""
                    SELECT
                        ORDINAL_POSITION,
                        COLUMN_NAME,
                        DATA_TYPE,
                        CHARACTER_MAXIMUM_LENGTH,
                        NUMERIC_PRECISION,
                        NUMERIC_SCALE,
                        IS_NULLABLE,
                        COLUMN_DEFAULT
                    FROM SYSIBM.COLUMNS
                    WHERE TABLE_NAME = '{table_name}' AND TBCREATOR = upper('{table_schema}') ORDER BY COLNO
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
                    'is_identity': 'NO',
                }
            cursor.close()
            self.disconnect()
            return result
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
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
        part_name = 'migrate_table initialize'
        inserted_rows = 0
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
            # primary_key_columns_count = settings['primary_key_columns_count']
            # primary_key_columns_types = settings['primary_key_columns_types']
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
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Table {source_table} is empty - skipping data migration.")
                return 0
            else:
                part_name = 'migrate_table in batches using cursor'
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Table {source_table} has {source_table_rows} rows - starting data migration.")

                select_columns_list = []
                for order_num, col in source_columns.items():
                    self.config_parser.print_log_message('DEBUG2',
                                                         f"Worker {worker_id}: Table {source_schema}.{source_table}: Processing column {col['column_name']} ({order_num}) with data type {col['data_type']}")
                    insert_columns = ', '.join([f'''"{col['column_name']}"''' for col in source_columns.values()])

                    # if col['data_type'].lower() == 'datetime':
                    #     select_columns_list.append(f"TO_CHAR({col['column_name']}, '%Y-%m-%d %H:%M:%S') as {col['column_name']}")
                    #     select_columns_list.append(f"ST_asText(`{col['column_name']}`) as `{col['column_name']}`")
                    # elif col['data_type'].lower() == 'set':
                    #     select_columns_list.append(f"cast(`{col['column_name']}` as char(4000)) as `{col['column_name']}`")
                    # else:
                    select_columns_list.append(f'''"{col['column_name']}"''')
                select_columns = ', '.join(select_columns_list)

                # Open a cursor and fetch rows in batches
                query = f'''SELECT {select_columns} FROM {source_schema.upper()}."{source_table}"'''
                if migration_limitation:
                    query += f" WHERE {migration_limitation}"

                self.config_parser.print_log_message( 'DEBUG', f"Worker {worker_id}: Fetching data with cursor using query: {query}")

                # # polars library is not always available
                # for df in pl.read_database(query, self.connection, iter_batches=True, batch_size=batch_size):
                #     if df.is_empty():
                #         break

                #     self.config_parser.print_log_message( 'DEBUG', f"Worker {worker_id}: Fetched {len(df)} rows from source table {source_table} using cursor.")
                #
                #     # Convert Polars DataFrame to list of dictionaries for insertion
                #     records = df.to_dicts()

                cursor = self.connection.cursor()
                cursor.execute(query)
                total_inserted_rows = 0
                while True:
                    records = cursor.fetchmany(batch_size)
                    if not records:
                        break
                    self.config_parser.print_log_message( 'DEBUG', f"Worker {worker_id}: Fetched {len(records)} rows from source table '{source_table}' using cursor")

                    # Convert records to a list of dictionaries
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
                    self.config_parser.print_log_message( 'DEBUG', f"Worker {worker_id}: Inserting {len(records)} rows into target table '{target_table}'")
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
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Inserted {inserted_rows} (total: {total_inserted_rows} from: {source_table_rows} ({round(total_inserted_rows/source_table_rows*100, 2)}%)) rows into target table '{target_table}'")

                target_table_rows = migrate_target_connection.get_rows_count(target_schema, target_table)
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Target table {target_schema}.{target_table} has {target_table_rows} rows")
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
            self.config_parser.print_log_message( 'DEBUG2', f"Indexes for table {source_table_name} ({source_table_schema}): {index_columns}")
            return table_indexes
        except Exception as e:
            self.config_parser.print_log_message( 'ERROR', f"Error executing query: {query}")
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
                            FK_COLNAMES
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

                    table_constraints[order_num] = {
                        'constraint_name': constraint_name,
                        'constraint_type': constraint_type,
                        'constraint_owner': source_table_schema,
                        'constraint_columns': fk_columns,
                        'referenced_table_schema': '',
                        'referenced_table_name': ref_table_name,
                        'referenced_columns': pk_columns,
                        'constraint_sql': '',
                        'constraint_comment': '',
                    }
                    order_num += 1

            cursor.close()
            self.disconnect()
            return table_constraints
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_constraint_sql(self, settings):
        return ""

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
        # Placeholder for fetching sequences
        return {}

    def get_sequence_details(self, sequence_owner, sequence_name):
        # Placeholder for fetching sequence details
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

    def convert_view_code(self, settings: dict):
        view_code = settings['view_code']
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
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
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
            self.config_parser.print_log_message('ERROR', f"Error executing script: {script_path}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def begin_transaction(self):
        self.connection.jconn.setAutoCommit(False)

    def commit_transaction(self):
        self.connection.commit()
        self.connection.jconn.setAutoCommit(True)

    def rollback_transaction(self):
        self.connection.rollback()

    def get_rows_count(self, table_schema: str, table_name: str):
        query = f"""SELECT COUNT(*) FROM {table_schema.upper()}."{table_name}" """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
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
            self.config_parser.print_log_message('ERROR', f"Error fetching database version: {e}")
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
            self.config_parser.print_log_message('ERROR', f"Error fetching database size: {e}")
            raise

    def get_top10_biggest_tables(self, settings):
        source_schema = settings['source_schema']
        query = f"""
            SELECT
                TABSCHEMA,
                TABNAME,
                SUM(DATA_OBJECT_P_SIZE + INDEX_OBJECT_P_SIZE) AS TOTAL_SIZE
            FROM SYSIBMADM.ADMINTABINFO
            WHERE TABSCHEMA = upper('{source_schema}')
            GROUP BY TABSCHEMA, TABNAME
            ORDER BY TOTAL_SIZE DESC
            FETCH FIRST 10 ROWS ONLY
        """
        self.config_parser.print_log_message('DEBUG', f"Fetching top 10 biggest tables for schema {source_schema} with query: {query}")
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            tables = cursor.fetchall()
            cursor.close()
            return tables
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching top 10 biggest tables: {e}")
            raise

if __name__ == "__main__":
    print("This script is not meant to be run directly")
