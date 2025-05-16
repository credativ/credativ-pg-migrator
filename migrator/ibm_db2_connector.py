from database_connector import DatabaseConnector
from migrator_logging import MigratorLogger
import ibm_db_dbi
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
            self.logger.error(f"Unexpected error while conneting into the database: {e}")
            raise

    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
        except AttributeError:
            pass

    def fetch_table_names(self, table_schema: str):
        query = f"""SELECT TABLEID, TABNAME FROM SYSCAT.TABLES WHERE TABSCHEMA = upper('{table_schema}') ORDER BY TABNAME"""
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
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_table_columns(self, table_schema: str, table_name: str, migrator_tables) -> dict:
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
                        "DEFAULT"
                    FROM SYSCAT.COLUMNS
                    WHERE TABSCHEMA = upper('{table_schema}') AND tabname = '{table_name}' ORDER BY COLNO
                """
            elif self.config_parser.get_system_catalog() == 'SYSIBM':
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
                column_type = row[2]
                character_maximum_length = row[3]
                numeric_precision = row[4]
                numeric_scale = row[5]
                is_nullable = row[6]
                if self.config_parser.get_system_catalog() == 'SYSCAT':
                    is_nullable = 'NO' if is_nullable == 'N' else 'YES'
                column_default = row[7]
                result[ordinal_position] = {
                    'column_name': column_name,
                    'data_type': column_type,
                    'character_maximum_length': character_maximum_length,
                    'numeric_precision': numeric_precision,
                    'numeric_scale': numeric_scale,
                    'is_nullable': is_nullable,
                    'column_default': column_default,
                    'column_comment': '',
                    'is_identity': 'NO',
                }
            cursor.close()
            self.disconnect()
            return result
        except Exception as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
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
                'CHAR': 'TEXT',
                'NCHAR': 'TEXT',
                'UNICHAR': 'TEXT',
                'NVARCHAR': 'TEXT',
                'TEXT': 'TEXT',
                'SYSNAME': 'TEXT',
                'LONGSYSNAME': 'TEXT',
                'LONG VARCHAR': 'TEXT',
                'LONG NVARCHAR': 'TEXT',
                'UNICHAR': 'TEXT',
                'UNITEXT': 'TEXT',
                'UNIVARCHAR': 'TEXT',
                'VARCHAR': 'TEXT',

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
            primary_key_columns = settings['primary_key_columns']
            primary_key_columns_count = settings['primary_key_columns_count']
            primary_key_columns_types = settings['primary_key_columns_types']
            batch_size = settings['batch_size']
            migrator_tables = settings['migrator_tables']
            source_table_rows = self.get_rows_count(source_schema, source_table)
            migration_limitation = settings['migration_limitation']

            if source_table_rows == 0:
                self.logger.info(f"Worker {worker_id}: Table {source_table} is empty - skipping data migration.")
                migrator_tables.insert_data_migration(source_schema, source_table, source_table_id, source_table_rows, worker_id, target_schema, target_table, 0)
                return 0
            else:
                part_name = 'migrate_table in batches using cursor'
                self.logger.info(f"Worker {worker_id}: Table {source_table} has {source_table_rows} rows - starting data migration.")
                protocol_id = migrator_tables.insert_data_migration(source_schema, source_table, source_table_id, source_table_rows, worker_id, target_schema, target_table, 0)

                # Open a cursor and fetch rows in batches
                query = f'''SELECT * FROM {source_schema.upper()}."{source_table}"'''
                if migration_limitation:
                    query += f" WHERE {migration_limitation}"

                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Worker {worker_id}: Fetching data with cursor using query: {query}")

                # # polars library is not always available
                # for df in pl.read_database(query, self.connection, iter_batches=True, batch_size=batch_size):
                #     if df.is_empty():
                #         break

                #     if self.config_parser.get_log_level() == 'DEBUG':
                #         self.logger.debug(f"Worker {worker_id}: Fetched {len(df)} rows from source table {source_table} using cursor.")

                #     # Convert Polars DataFrame to list of dictionaries for insertion
                #     records = df.to_dicts()

                cursor = self.connection.cursor()
                cursor.execute(query)
                total_inserted_rows = 0
                while True:
                    records = cursor.fetchmany(batch_size)
                    if not records:
                        break
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Worker {worker_id}: Fetched {len(records)} rows from source table '{source_table}' using cursor")

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
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Worker {worker_id}: Starting insert of {len(records)} rows from source table {source_table}")
                    settings = {
                        'target_schema': target_schema,
                        'target_table': target_table,
                        'target_columns': target_columns,
                        'data': records,
                        'worker_id': worker_id,
                        'migrator_tables': migrator_tables,
                    }
                    inserted_rows = migrate_target_connection.insert_batch(settings)
                    total_inserted_rows += inserted_rows
                    self.logger.info(f"Worker {worker_id}: Inserted {inserted_rows} (total: {total_inserted_rows} from: {source_table_rows} ({round(total_inserted_rows/source_table_rows*100, 2)}%)) rows into target table '{target_table}'")

                target_table_rows = migrate_target_connection.get_rows_count(target_schema, target_table)
                self.logger.info(f"Worker {worker_id}: Target table {target_schema}.{target_table} has {target_table_rows} rows")
                migrator_tables.update_data_migration_status(protocol_id, True, 'OK', target_table_rows)
                cursor.close()
                return target_table_rows
        except Exception as e:
            self.logger.error(f"Worker {worker_id}: Error during {part_name} -> {e}")
            self.logger.error("Full stack trace:")
            self.logger.error(traceback.format_exc())
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
        index_columns_data_types_str = ''
        query = f"""
            SELECT INDNAME, COLNAMES, COLCOUNT, UNIQUERULE, MADE_UNIQUE
            FROM SYSCAT.INDEXES I
            WHERE I.TABSCHEMA = upper('{settings['source_schema']}')
            AND I.TABNAME = '{settings['source_table_name']}'
            ORDER BY INDNAME
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                index_name = row[0]
                index_columns = row[1].lstrip('+').split('+')
                index_columns = ', '.join(f'"{col}"' for col in index_columns)
                index_columns_count = row[2]
                index_unique = row[2]
                index_type = row[3]
                table_id = row[4]

                create_index_query = None

                if index_type == 'P':
                    create_index_query = f"""ALTER TABLE "{target_schema}"."{target_table_name}" ADD CONSTRAINT "{index_name}" PRIMARY KEY ({index_columns});"""
                else:
                    create_index_query = f"""CREATE {'UNIQUE' if index_type == 'U' else ''} INDEX "{index_name}" ON "{target_schema}"."{target_table_name}" ({index_columns});"""

                table_indexes[order_num] = {
                    'name': index_name,
                    'type': 'PRIMARY KEY' if index_type == 'P' else 'UNIQUE' if index_type == 'U' else 'INDEX',
                    'owner': settings['source_schema'],
                    'columns': index_columns,
                    'columns_count': index_columns_count,
                    'columns_data_types': [],
                    'sql': create_index_query,
                    'comment': '',
                }

                order_num += 1

            cursor.close()
            self.disconnect()
            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Indexes for table {settings['source_table_name']} ({settings['source_schema']}): {index_columns_data_types_str}")
            return table_indexes
        except Exception as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_constraints(self, settings):
        source_table_id = settings['source_table_id']
        source_schema = settings['source_schema']
        source_table_name = settings['source_table_name']
        target_schema = settings['target_schema']
        target_table_name = settings['target_table_name']
        order_num = 1
        table_constraints = {}
        create_constraint_query = None
        query = f"""
            SELECT CONSTNAME, TYPE
            FROM SYSCAT.TABCONST
            WHERE TABSCHEMA = '{source_schema.upper()}'
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
                        SELECT PK_COLNAMES, REFTABNAME, FK_COLNAMES
                        FROM SYSCAT.REFERENCES
                        WHERE TABSCHEMA = '{source_schema.upper()}'
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
                        create_constraint_query = f"""ALTER TABLE "{target_schema}"."{target_table_name}" ADD CONSTRAINT "{constraint_name}" FOREIGN KEY ({fk_columns}) REFERENCES "{target_schema}"."{ref_table_name}" ({pk_columns});"""
                else:
                    pass

                if create_constraint_query:
                    table_constraints[order_num] = {
                        'name': constraint_name,
                        'type': constraint_type,
                        'owner': source_schema,
                        'columns': [],
                        'sql': create_constraint_query,
                        'comment': '',
                    }
                    order_num += 1

            cursor.close()
            self.disconnect()
            return table_constraints
        except Exception as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

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
        query = f"""SELECT COUNT(*) FROM {table_schema.upper()}."{table_name}" """
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
        return 0

    def fetch_user_defined_types(self, schema: str):
        # Placeholder for fetching user-defined types
        return {}

    def testing_select(self):
        return "SELECT 1 FROM SYSIBM.SYSDUMMY1"