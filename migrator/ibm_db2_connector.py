from database_connector import DatabaseConnector
from migrator_logging import MigratorLogger
import ibm_db_dbi

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
        query = f"""
            SELECT COLNO, COLNAME, TYPENAME, "LENGTH", "SCALE", STRINGUNITSLENGTH, "NULLS", "DEFAULT"
            FROM SYSCAT.COLUMNS
            WHERE TABSCHEMA = upper('{table_schema}') AND tabname = '{table_name}' ORDER BY COLNO
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
                    'length': row[5] if row[5] else row[3],
                    'precision': row[4],
                    'nullable': 'NOT NULL' if row[6] == 'N' else '',
                    'default': row[7] if row[7] else '',
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

        type_mapping = {}
        create_table_sql = ""
        converted = {}
        if target_db_type == 'postgresql':
            type_mapping = {
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

            for order_num, column_info in source_columns.items():
                coltype = column_info['type'].upper()
                length = column_info['length']
                if type_mapping.get(coltype, 'UNKNOWN').startswith('UNKNOWN'):
                    self.logger.info(f"Column {column_info['name']} - unknown data type: {column_info['type']}")
                    # coltype = 'TEXT' ## default to TEXT may not be the best option -> let the table creation fail
                else:
                    coltype = type_mapping.get(coltype, 'TEXT')
                if coltype == 'VARCHAR' and int(column_info['length']) >= 254:
                    coltype = 'TEXT'
                    length = ''

                converted[order_num] = {
                    'name': column_info['name'],
                    'type': coltype,
                    'length': length,
                    'default': column_info['default'],
                    'nullable': column_info['nullable'],
                    'comment': column_info['comment'],
                    'other': column_info['other']
                }

            create_table_sql_parts = []
            for _, info in converted.items():
                if 'length' in info and info['type'] in ('CHAR', 'VARCHAR'):
                    create_table_sql_parts.append(f""""{info['name']}" {info['type']}({info['length']}) {info['nullable']}""")
                else:
                    create_table_sql_parts.append(f""""{info['name']}" {info['type']} {info['nullable']}""")
                if info['default']:
                    if info['type'] in ('CHAR', 'VARCHAR', 'TEXT') and ('||' in info['default'] or '(' in info['default'] or ')' in info['default']):
                        create_table_sql_parts[-1] += f""" DEFAULT {info['default']}""".replace("''", "'")
                    elif info['type'] in ('CHAR', 'VARCHAR', 'TEXT'):
                        create_table_sql_parts[-1] += f""" DEFAULT '{info['default']}'""".replace("''", "'")
                    elif info['type'] in ('BOOLEAN', 'BIT'):
                        create_table_sql_parts[-1] += f""" DEFAULT {info['default']}::BOOLEAN"""
                    else:
                        create_table_sql_parts[-1] += f" DEFAULT {info['default']}"
            create_table_sql = ", ".join(create_table_sql_parts)
            create_table_sql = f"""CREATE TABLE "{target_schema}"."{target_table_name}" ({create_table_sql})"""

        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

        return converted, create_table_sql

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
                        {column['name']: value for column, value in zip(source_columns.values(), record)}
                        for record in records
                    ]
                    for record in records:
                        for order_num, column in source_columns.items():
                            column_name = column['name']
                            column_type = column['type']
                            if column_type.lower() in ['binary', 'varbinary', 'image']:
                                record[column_name] = bytes(record[column_name]) if record[column_name] is not None else None
                            elif column_type.lower() in ['datetime', 'smalldatetime', 'date', 'time', 'timestamp']:
                                record[column_name] = str(record[column_name]) if record[column_name] is not None else None

                    # Insert batch into target table
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Worker {worker_id}: Starting insert of {len(records)} rows from source table {source_table}")
                    inserted_rows = migrate_target_connection.insert_batch(target_schema, target_table, target_columns, records)
                    total_inserted_rows += inserted_rows
                    self.logger.info(f"Worker {worker_id}: Inserted {inserted_rows} (total: {total_inserted_rows} from: {source_table_rows} ({round(total_inserted_rows/source_table_rows*100, 2)}%)) rows into target table '{target_table}'")

                target_table_rows = migrate_target_connection.get_rows_count(target_schema, target_table)
                self.logger.info(f"Worker {worker_id}: Target table {target_schema}.{target_table} has {target_table_rows} rows")
                migrator_tables.update_data_migration_status(protocol_id, True, 'OK', target_table_rows)
                cursor.close()
        except Exception as e:
            self.logger.error(f"Worker {worker_id}: Error during {part_name} -> {e}")
            raise e
        finally:
            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Worker {worker_id}: Finished processing table {source_table}.")
            return target_table_rows

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