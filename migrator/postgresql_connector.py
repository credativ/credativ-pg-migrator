import psycopg2
import psycopg2.extras
from psycopg2 import sql
from database_connector import DatabaseConnector
from migrator_logging import MigratorLogger
import traceback
import re

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
            SELECT oid, relname
            FROM pg_class
            WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{schema}')
            AND relkind in ('r', 'p')
            ORDER BY relname
        """
        # if self.config_parser.get_log_level() == 'DEBUG':
        #     self.logger.debug(f"Reading table names for {schema}")
        #     self.logger.debug(f"Query: {query}")
        try:
            tables = {}
            order_num = 1
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                tables[order_num] = {
                    'id': row[0],
                    'schema_name': schema,
                    'table_name': row[1]
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return tables
        except psycopg2.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_table_columns(self, table_schema: str, table_name: str, migrator_tables=None) -> dict:
        result = {}
        try:
            query =f"""
                    SELECT
                        c.ordinal_position,
                        c.column_name,
                        c.data_type,
                        CASE WHEN c.character_maximum_length IS NOT NULL
                            THEN c.character_maximum_length
                        ELSE c.numeric_precision END as length,
                        CASE WHEN upper(c.is_nullable)='YES'
                            THEN ''
                        ELSE 'NOT NULL' END AS nullable,
                        c.column_default,
                        u.udt_schema||'.'||u.udt_name as full_udt_name
                    FROM information_schema.columns c
                    LEFT JOIN information_schema.column_udt_usage u ON c.table_schema = u.table_schema
                        AND c.table_name = u.table_name
                        AND c.column_name = u.column_name
                        AND c.udt_name = u.udt_name
                    WHERE c.table_name = '{table_name}' AND c.table_schema = '{table_schema}'
                """
            self.connect()
            cursor = self.connection.cursor()
            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Reading columns for {table_name}")
            cursor.execute(query)
            for row in cursor.fetchall():
                data_type = row[2].upper()
                other = ''
                if data_type == 'USER-DEFINED':
                    data_type = row[6].upper()
                    other = 'USER-DEFINED'
                result[row[0]] = {
                    'name': row[1],
                    'type': data_type,
                    'length': row[3],
                    'nullable': row[4],
                    'default': row[5],
                    'other': other
                }
            cursor.close()
            self.disconnect()
            return result

        except psycopg2.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def convert_table_columns(self, target_db_type: str, table_schema: str, table_name: str, columns: dict):
        # type_mapping = {}
        create_table_sql = ""
        converted_schema = {}

        if target_db_type == 'postgresql':
            for col, info in columns.items():
                converted_schema[col] = {
                    'name': info['name'],
                    'nullable': info['nullable']
                }
                if (info['length'] is not None
                    and info['type'].upper() in ('CHAR', 'VARCHAR', 'CHARACTER VARYING')):
                    converted_schema[col]['type'] = f"{info['type']}({info['length']})"
                else:
                    converted_schema[col]['type'] = info['type']

            create_table_sql = ', '.join([(f'"{info["name"]}" {info["type"]} {info["nullable"]}').strip()
                                          for _, info in converted_schema.items()])
            create_table_sql = f"""CREATE TABLE "{table_schema}"."{table_name}" ({create_table_sql})"""
        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

        return converted_schema, create_table_sql

    def fetch_indexes(self, source_table_id: int, target_schema, target_table_name):
        table_indexes = {}
        order_num = 1
        query = f"""
            SELECT
                i.indexname,
                i.indexdef,
                coalesce(c.constraint_type, 'INDEX') as type
            FROM pg_indexes i
            JOIN pg_class t
            ON t.relnamespace::regnamespace::text = i.schemaname
            AND t.relname = i.tablename
            LEFT JOIN information_schema.table_constraints c
            ON i.schemaname = c.table_schema
                and i.tablename = c.table_name
                and i.indexname = c.constraint_name
            WHERE t.oid = {source_table_id}
        """
        # if self.config_parser.get_log_level() == 'DEBUG':
        #     self.logger.debug(f"Reading indexes for {table_name}")
        #     self.logger.debug(f"Query: {query}")
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                columns_match = re.search(r'\((.*?)\)', row[1])
                columns = columns_match.group(1) if columns_match else ''
                index_name = row[0]
                index_type = row[2]
                index_sql = row[1]
                if index_type == 'PRIMARY KEY':
                    index_sql = f'ALTER TABLE "{target_schema}"."{target_table_name}" ADD CONSTRAINT "{index_name}" PRIMARY KEY ({columns});'
                table_indexes[order_num] = {
                    'name': index_name,
                    'type': index_type,
                    'columns': columns,
                    'sql': index_sql
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return table_indexes
        except psycopg2.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_constraints(self, source_table_id: int, target_schema, target_table_name):
        order_num = 1
        constraints = {}
        # c = check constraint, f = foreign key constraint, n = not-null constraint (domains only),
        # p = primary key constraint, u = unique constraint, t = constraint trigger,
        # x = exclusion constraint
        query = f"""
            SELECT
                oid,
                conname,
                CASE WHEN contype = 'c'
                    THEN 'CHECK'
                WHEN contype = 'f'
                    THEN 'FOREIGN KEY'
                WHEN upper(contype) = 'P'
                    THEN 'PRIMARY KEY'
                WHEN contype = 'u'
                    THEN 'UNIQUE'
                WHEN contype = 't'
                    THEN 'TRIGGER'
                WHEN contype = 'x'
                    THEN 'EXCLUSION'
                ELSE contype
                END as type,
                pg_get_constraintdef(oid) as condef
            FROM pg_constraint
            WHERE conrelid = '{source_table_id}'::regclass
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                constraint_name = row[1]
                constraint_type = row[2]
                constraint_sql = row[3]
                if constraint_type in ('PRIMARY KEY', 'p', 'P'):
                    continue # Primary key is handled in fetch_indexes
                constraints[order_num] = {
                    'id': row[0],
                    'name': constraint_name,
                    'type': constraint_type,
                    'sql': constraint_sql
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return constraints
        except psycopg2.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_triggers(self, table_id: int, table_schema: str, table_name: str):
        pass

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
        part_name = 'initialize'
        try:
            worker_id = settings['worker_id']
            source_schema = settings['source_schema']
            source_table = settings['source_table']
            source_columns = settings['source_columns']
            target_schema = settings['target_schema']
            target_table = settings['target_table']
            target_columns = settings['target_columns']
            primary_key_columns = settings['primary_key_columns']
            batch_size = settings['batch_size']

            source_table_rows = self.get_rows_count(source_schema, source_table)
            if source_table_rows == 0:
                self.logger.info(f"Worker {worker_id}: Table {source_schema}.{source_table} has no rows. Skipping migration.")
                return
            else:
                self.logger.info(f"Worker {worker_id}: Table {source_schema}.{source_table} has {source_table_rows} rows.")
                offset = 0
                while True:
                    part_name = f'prepare fetch data: {source_table} - {offset}'
                    if primary_key_columns:
                        query = f"""SELECT * FROM "{source_schema}"."{source_table}" ORDER BY {primary_key_columns} LIMIT {batch_size} OFFSET {offset}"""
                    else:
                        query = f"""SELECT * FROM "{source_schema}"."{source_table}" ORDER BY ctid LIMIT {batch_size} OFFSET {offset}"""
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Worker {worker_id}: Fetching data with query: {query}")

                    part_name = f'do fetch data: {source_table} - {offset}'

                    cursor = self.connection.cursor()
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    cursor.close()
                    if not rows:
                        break

                    records = []
                    for row in rows:
                        record = {}
                        for order_num, column in source_columns.items():
                            column_name = column['name']
                            column_type = column['type']
                            if column_type in ['bytea']:
                                record[column_name] = row[int(order_num) - 1].tobytes()
                            else:
                                record[column_name] = row[int(order_num) - 1]
                        records.append(record)

                    part_name = f'insert data: {source_table} - {offset}: {len(records)} rows'
                    migrate_target_connection.insert_batch(target_schema, target_table, target_columns, records)
                    self.logger.info(f"Worker {worker_id}: Inserted {len(records)} rows into {target_schema}.{target_table}.")
                    offset += batch_size

                self.logger.info(f"Worker {worker_id}: Finished migrating table {source_schema}.{source_table}.")
                return source_table_rows
        except Exception as e:
            self.logger.error(f"Woker {worker_id}: Error in {part_name}: {e}")
            raise e

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
                insert_query = sql.SQL(f"""INSERT INTO "{table_schema}"."{table_name}" ({', '.join(column_names)}) VALUES ({', '.join(['%s' for _ in column_names])})""")
                # if self.config_parser.get_log_level() == 'DEBUG':
                #     self.logger.debug(f"Insert query: {insert_query}")
                self.connection.autocommit = False
                try:
                    psycopg2.extras.execute_batch(cursor, insert_query, data)
                except psycopg2.Error as e:
                    self.logger.error(f"Error inserting batch data: {e}")
                    self.logger.error(f"Trying to insert row by row.")
                    self.connection.rollback()
                    for row in data:
                        try:
                            cursor.execute(insert_query, row)
                            self.connection.commit()
                        except psycopg2.Error as e:
                            self.connection.rollback()
                            self.logger.error(f"Error inserting row: {row}")

        except psycopg2.Error as e:
            self.logger.error(f"Error befor inserting batch data: {e}")
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
                    ''' and relkind = ''S''), (SELECT MAX(' || quote_ident(a.attname) || ') /*+ 1*/ FROM ' ||
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

    def get_sequence_current_value(self, sequence_id: int):
        try:
            query = f"""select '"'||relnamespace::regnamespace::text||'"."'||relname||'"' as seqname from pg_class where oid = {sequence_id}"""
            cursor = self.connection.cursor()
            cursor.execute(query)
            sequence_data = cursor.fetchone()
            sequence_name = f"{sequence_data[0]}"

            query = f"""
                SELECT last_value
                FROM {sequence_name}
            """
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            cur_value = cursor.fetchone()[0]
            cursor.close()
            self.disconnect()
            return cur_value
        except psycopg2.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

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

    def convert_trigger(self, trigger_id: int, target_db_type: str, target_schema: str):
        pass

    def fetch_views_names(self, source_schema: str):
        views = {}
        order_num = 1
        query = f"""
            SELECT
                oid,
                relname as viewname
            FROM pg_class
            WHERE relkind = 'v'
            AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{source_schema}')
            AND relname NOT LIKE 'pg_%'
            ORDER BY viewname
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                views[order_num] = {
                    'id': row[0],
                    'schema_name': source_schema,
                    'view_name': row[1]
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return views
        except psycopg2.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_view_code(self, view_id: int):
        query = f"""
            SELECT definition
            FROM pg_views
            WHERE oid = {view_id}
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            view_code = cursor.fetchone()[0]
            cursor.close()
            self.disconnect()
            return view_code
        except psycopg2.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def convert_view_code(self, view_code: str, settings: dict):
        return view_code

    def fetch_user_defined_types(self, schema: str):
        user_defined_types = {}
        order_num = 1
        query = f"""
            SELECT t.typnamespace::regnamespace::text as schemaname, typname as type_name,
                'CREATE TYPE "'||t.typnamespace::regnamespace||'"."'||typname||'" As ENUM ('||string_agg(''''||e.enumlabel||'''', ',' ORDER BY e.enumsortorder)::text||');' AS elements
            FROM pg_type AS t
            LEFT JOIN pg_enum AS e ON e.enumtypid = t.oid
            WHERE t.typnamespace::regnamespace::text NOT IN ('pg_catalog', 'information_schema')
            AND t.typtype = 'e'
            AND t.typcategory = 'E'
            GROUP BY t.oid ORDER BY t.typnamespace::regnamespace, typname;
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                user_defined_types[order_num] = {
                    'schema_name': row[0],
                    'type_name': row[1],
                    'sql': row[2]
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return user_defined_types
        except psycopg2.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise
