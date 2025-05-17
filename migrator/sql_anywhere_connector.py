from database_connector import DatabaseConnector
from migrator_logging import MigratorLogger
import sqlanydb
import pyodbc
import traceback

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
            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"SQL Anywhere ODBC connection string: {connection_string}")
            self.connection = pyodbc.connect(connection_string)

    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
        except AttributeError:
            pass

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
        except pyodbc.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
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
                c.column_type,
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
                result[row[0]] = {
                    'column_name': row[1],
                    'data_type': row[2],
                    'character_maximum_length': row[3] if self.is_string_type(row[2]) else None,
                    'numeric_precision': row[3] if self.is_numeric_type(row[2]) else None,
                    'numeric_scale': row[4],
                    'is_nullable': 'NO' if row[6] == 'N' else 'YES',
                    'is_identity': 'YES' if row[7] is not None and row[7].upper() == 'AUTOINCREMENT' else 'NO',
                    'column_default': row[7] if row[7] is not None and row[7].upper() != 'AUTOINCREMENT' else None,
                    'column_comment': '',
                }
            cursor.close()
            self.disconnect()
            return result
        except pyodbc.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def get_types_mapping(self, settings):
        target_db_type = settings['target_db_type']
        types_mapping = {}
        if target_db_type == 'postgresql':
            types_mapping = {
                'INTEGER': 'INTEGER',
                'VARCHAR': 'VARCHAR',
                'CHAR': 'CHAR',
                'DATE': 'DATE',
                'TIMESTAMP': 'TIMESTAMP',
                'DECIMAL': 'DECIMAL',
                'BINARY': 'BYTEA',
                'LONG VARBINARY': 'BYTEA',
                'LONG BINARY': 'BYTEA',
                'BOOLEAN': 'BOOLEAN',
                'FLOAT': 'REAL',
                'DOUBLE PRECISION': 'DOUBLE PRECISION',
                'SMALLINT': 'SMALLINT',
                'BIGINT': 'BIGINT',
                'TINYINT': 'SMALLINT',
                'NUMERIC': 'NUMERIC',
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
            source_table_rows = self.get_rows_count(source_schema, source_table)
            migration_limitation = settings['migration_limitation']

            if source_table_rows == 0:
                self.logger.info(f"Worker {worker_id}: Table {source_table} is empty - skipping data migration.")
                migrator_tables.insert_data_migration(source_schema, source_table, source_table_id, source_table_rows, worker_id, target_schema, target_table, 0)
                return 0
            else:
                self.logger.info(f"Worker {worker_id}: Table {source_table} has {source_table_rows} rows - starting data migration.")
                protocol_id = migrator_tables.insert_data_migration(source_schema, source_table, source_table_id, source_table_rows, worker_id, target_schema, target_table, 0)
                offset = 0
                total_inserted_rows = 0
                cursor = self.connection.cursor()
                query = f"SELECT * FROM {source_schema}.{source_table}"
                if migration_limitation:
                    query += f" WHERE {migration_limitation}"

                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Worker {worker_id}: Fetching data with cursor using query: {query}")

                # Fetch the data in batches
                cursor.execute(query)
                total_inserted_rows = 0
                while True:
                    records = cursor.fetchmany(batch_size)
                    if not records:
                        break
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Worker {worker_id}: Fetched {len(records)} rows from source table '{source_table}' using cursor")

                    records = [
                        {column['column_name']: value for column, value in zip(source_columns.values(), record)}
                        for record in records
                    ]

                    # Adjust binary or bytea types
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
                    self.logger.info(f"Worker {worker_id}: Inserted {inserted_rows} (total: {total_inserted_rows} from: {source_table_rows} ({round(total_inserted_rows/source_table_rows*100, 2)}%)) rows into target table {target_table}")

                    # offset += batch_size

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
        except pyodbc.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def get_create_index_sql(self, settings):
        return ""

    def fetch_constraints(self, settings):
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

        order_num = 1
        table_constraints = {}
        query = f"""
            SELECT "role" as fk_name, primary_creator, primary_tname, foreign_creator, foreign_tname, columns
            FROM SYS.SYSFOREIGNKEYS s
            WHERE (primary_creator = '{source_table_schema}' or foreign_creator = '{source_table_schema}')
            AND primary_tname = '{source_table_name}'
            ORDER BY "role"
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                constraint_name = row[0]
                constraint_type = 'FOREIGN KEY'
                primary_table_name = row[2]
                foreign_table_name = row[4]
                sa_columns = row[5]
                ref_columns, pk_columns = sa_columns.split(" IS ")
                columns = []
                for col in ref_columns.split(","):
                    col = col.strip().replace(" ASC", "").replace(" DESC", "")
                    if col not in columns:
                        columns.append('"'+col+'"')
                ref_columns = ','.join(columns)

                columns = []
                for col in pk_columns.split(","):
                    col = col.strip().replace(" ASC", "").replace(" DESC", "")
                    if col not in columns:
                        columns.append('"'+col+'"')
                pk_columns = ','.join(columns)

                constraint_sql = f'''ALTER TABLE "{target_schema}"."{foreign_table_name}" ADD CONSTRAINT "{target_table_name}_{constraint_name}_fk{order_num}"
                FOREIGN KEY ({ref_columns}) REFERENCES "{target_schema}"."{primary_table_name}" ({pk_columns})'''

                table_constraints[order_num] = {
                    'id': None,
                    'name': constraint_name,
                    'type': constraint_type,
                    'sql': constraint_sql,
                    'comment': '',
                }
                order_num += 1
            cursor.close()
            self.disconnect()

            return table_constraints
        except pyodbc.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def get_create_constraint_sql(self, settings):
        return ""

    def fetch_triggers(self, table_id: int, table_schema: str, table_name: str):
        pass

    def convert_trigger(self, trig: str, settings: dict):
        pass

    def fetch_funcproc_names(self, schema: str):
        pass

    def fetch_funcproc_code(self, funcproc_id: int):
        pass

    def convert_funcproc_code(self, funcproc_code: str, target_db_type: str, source_schema: str, target_schema: str, table_list: list):
        pass

    def fetch_sequences(self, table_schema: str, table_name: str):
        pass

    def get_sequence_details(self, sequence_owner, sequence_name):
        # Placeholder for fetching sequence details
        return {}

    def fetch_views_names(self, source_schema: str):
        views = {}
        order_num = 1
        query = f"""SELECT viewname FROM sys.sysviews WHERE vcreator = '{source_schema}'"""
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                views[order_num] = {
                    'id': None,
                    'schema_name': source_schema,
                    'view_name': row[0],
                    'comment': ''
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return views
        except pyodbc.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_view_code(self, settings):
        view_id = settings['view_id']
        source_schema = settings['source_schema']
        source_view_name = settings['source_view_name']
        target_schema = settings['target_schema']
        target_view_name = settings['target_view_name']
        query = f"""
            SELECT viewtext
            FROM sys.sysviews
            WHERE vcreator = '{source_schema}'
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
        except pyodbc.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def convert_view_code(self, view_code: str, settings: dict):
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

    def get_rows_count(self, table_schema: str, table_name: str):
        query = f"SELECT COUNT(*) FROM \"{table_schema}\".\"{table_name}\""
        cursor = self.connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_table_size(self, table_schema: str, table_name: str):
        raise NotImplementedError("Fetching table size is not yet implemented for SQL Anywhere")

    def fetch_user_defined_types(self, schema: str):
        pass

    def testing_select(self):
        return "SELECT 1"
