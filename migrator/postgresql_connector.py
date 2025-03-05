import psycopg2
import psycopg2.extras
from psycopg2 import sql
from database_connector import DatabaseConnector
from migrator_logging import MigratorLogger
import traceback

class PostgreSQLConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger

    def connect(self):
        connection_string = self.config_parser.get_connect_string(self.source_or_target)
        self.connection = psycopg2.connect(connection_string)
        self.connection.autocommit = True

    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
        except AttributeError:
            pass

    def fetch_table_names(self, schema: str = 'public'):
        query = f"""
            SELECT relname, oid
            FROM pg_class
            WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{schema}')
            AND relkind in ('r', 'p')
            ORDER BY relname
        """
        if self.config_parser.get_log_level() == 'DEBUG':
            self.logger.debug(f"Reading table names for {schema}")
            self.logger.debug(f"Query: {query}")
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            table_names = {row[0]: row[1] for row in cursor.fetchall()}
            cursor.close()
            self.disconnect()
            return table_names
        except psycopg2.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_table_columns(self, table_schema: str, table_name: str) -> dict:
        try:
            query =f"""
                    SELECT
                        column_name,
                        data_type, /* udt_name, */
                        CASE WHEN character_maximum_length IS NOT NULL
                            THEN character_maximum_length
                        ELSE numeric_precision END as length,
                        CASE WHEN upper(is_nullable)='YES'
                            THEN ''
                        ELSE 'NOT NULL' END AS nullable,
                        column_default
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}' AND table_schema = '{table_schema}'
                """
            self.connect()
            cursor = self.connection.cursor()
            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Reading columns for {table_name}")
            cursor.execute(query)
            columns = {row[0]:
                        {'type': row[1], 'length': row[2], 'nullable': row[3], 'default': row[4]}
                        for row in cursor.fetchall()}
            cursor.close()
            self.disconnect()
            return columns
        except psycopg2.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def convert_table_columns(self, target_db_type: str, table_schema: str, table_name: str, columns: dict):
        # type_mapping = {}
        create_table_sql = ""
        converted_schema = {}

        if target_db_type == 'postgresql':
            converted_schema = columns

            create_table_sql = ", ".join([
                f""""{col}" {info['type']}({info['length']}) {info['nullable']}""".strip()
                if 'length' in info and info['length'] is not None and info['type'].upper() in ('CHAR', 'VARCHAR', 'CHARACTER VARYING')
                else f""""{col}" {info['type']} {info['nullable']}"""
                for col, info in converted_schema.items()
            ])
            create_table_sql = f"""CREATE TABLE "{table_schema}"."{table_name}" ({create_table_sql})"""
        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

        return converted_schema, create_table_sql

    def fetch_indexes(self, table_id: int, table_schema: str, table_name: str):
        table_indexes = {}
        query = f"""
            SELECT
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname = '{table_schema}' AND tablename = '{table_name}'
        """
        if self.config_parser.get_log_level() == 'DEBUG':
            self.logger.debug(f"Reading indexes for {table_name}")
            self.logger.debug(f"Query: {query}")
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                table_indexes[row[0]] = {
                    'index_type': "UNIQUE" if "UNIQUE INDEX" in row[1] else "INDEX",
                    'index_sql': row[1],
                    'index_columns': ', '.join([col.strip() for col in row[1].split('(')[1].split(')')[0].split(',')])
                }

            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Indexes: {table_indexes}")

            query = f"""
            SELECT tco.constraint_name,
                string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as key_columns
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu
                ON kcu.constraint_name = tco.constraint_name
                AND kcu.constraint_schema = tco.constraint_schema
                AND kcu.constraint_name = tco.constraint_name
            WHERE tco.constraint_type = 'PRIMARY KEY' AND
                kcu.table_schema = '{table_schema}' AND
                kcu.table_name = '{table_name}'
            GROUP BY tco.constraint_name
            """
            cursor.execute(query)
            primary_key = cursor.fetchone()
            if primary_key:
                table_indexes[primary_key[0]] = {
                    'index_type': "PRIMARY KEY",
                    'index_sql': table_indexes[primary_key[0]]['index_sql'],
                    'index_columns': primary_key[1]
                }
            cursor.close()
            self.disconnect()
            return table_indexes
        except psycopg2.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_constraints(self, table_id: int, table_schema: str, table_name: str):
        query = f"""
            SELECT
                conname,
                pg_get_constraintdef(oid) as condef
            FROM pg_constraint
            WHERE conrelid = '{table_id}'::regclass
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            constraints = {row[0]: {'constraint_type': row[1].split()[0], 'constraint_sql': row[1]} for row in cursor.fetchall()}
            cursor.close()
            self.disconnect()
            return constraints
        except psycopg2.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def execute_query(self, query: str, params=None):
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)

    def execute_sql_script(self, script_path: str):
        with open(script_path, 'r') as file:
            script = file.read()

        with self.connection.cursor() as cursor:
            try:
                cursor.execute(script)
                for notice in cursor.connection.notices:
                    self.logger.info(notice)
            except psycopg2.Error as e:
                for notice in cursor.connection.notices:
                    self.logger.info(notice)
                self.logger.error(f"Error executing script: {e}")
                raise

    def begin_transaction(self):
        self.connection.autocommit = False

    def commit_transaction(self):
        self.connection.commit()
        self.connection.autocommit = True

    def rollback_transaction(self):
        self.connection.rollback()

    def migrate_table(self, migrate_target_connection, settings):
        return 0

    def insert_batch(self, table_schema: str, table_name: str, columns: dict, data: list):
        try:
            # Ensure data is a list of tuples
            if isinstance(data, list) and all(isinstance(item, dict) for item in data):
                formatted_data = []
                for item in data:
                    row = []
                    for col in sorted(columns.keys()):
                        col_name = columns[col]['name']
                        row.append(item.get(col_name))
                    formatted_data.append(tuple(row))
                data = formatted_data

            with self.connection.cursor() as cursor:
                column_names = [f'"{columns[col]["name"]}"' for col in sorted(columns.keys())]
                insert_query = sql.SQL(f"""
                    INSERT INTO "{table_schema}"."{table_name}" ({', '.join(column_names)})
                    VALUES ({', '.join(['%s' for _ in column_names])})
                """)
                self.connection.autocommit = False
                psycopg2.extras.execute_batch(cursor, insert_query, data)
        except psycopg2.Error as e:
            self.logger.error(f"Error inserting batch data: {e}")
            raise
        finally:
            self.connection.commit()
            self.connection.autocommit = True

    def fetch_funcproc_names(self, schema: str):
        pass

    def fetch_funcproc_code(self, funcproc_id: int):
        pass

    def convert_funcproc_code(self, funcproc_id: int, target_db_type: str, target_schema: str):
        pass

    def handle_error(self, e, description=None):
        self.logger.error(f"An error in {self.__class__.__name__} ({description}): {e}")
        self.logger.error(traceback.format_exc())
        if self.on_error_action == 'stop':
            self.logger.error("Stopping due to error.")
            exit(1)

    def fetch_sequences(self, table_schema: str, table_name: str):
        sequence_data = {}
        order_num = 1
        try:
            query = f"""
                SELECT
                    c.relname::text AS sequence_name,
                    c.oid AS sequence_id,
                    a.attname AS column_name,
                    'SELECT SETVAL( (SELECT oid from pg_class where relname = ''' || c.relname ||
                    ''' and relkind = ''S''), (SELECT MAX(' || quote_ident(a.attname) || ') + 1 FROM ' ||
                    quote_ident(t.relnamespace::regnamespace::text)||'.'|| quote_ident(t.relname) || '));' as sequence_sql
                FROM
                    pg_depend d
                    JOIN pg_class c ON d.objid = c.oid
                    JOIN pg_attribute a ON d.refobjid = a.attrelid AND a.attnum = d.refobjsubid
                    JOIN pg_class t ON t.oid = d.refobjid
                WHERE
                    c.relkind = 'S'  /* sequence */
                    AND t.relname = '{table_name}'
                    AND t.relkind = 'r' /* regular local table */
                    AND d.refobjsubid > 0
                    AND c.relnamespace = '{table_schema}'::regnamespace
                ORDER BY 2,3
                """
            # self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                sequence_data[order_num] = {
                    'name': row[0],
                    'id': row[1],
                    'column_name': row[2],
                    'set_sequence_sql': row[3]
                }
            cursor.close()
            # self.disconnect()
            return sequence_data
        except psycopg2.Error as e:
            self.logger.error(f"Error executing sequence query: {query}")
            self.logger.error(e)

    def get_rows_count(self, table_schema: str, table_name: str):
        query = f"""
            SELECT count(*)
            FROM "{table_schema}"."{table_name}"
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count
