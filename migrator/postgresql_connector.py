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
        self.session_settings = self.prepare_session_settings()
        self.logger.info(f"Session settings prepared: {self.session_settings}")

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
            SELECT
                oid,
                relname,
                obj_description(oid, 'pg_class') as table_comment
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
                    'table_name': row[1],
                    'comment': row[2]
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
                        u.udt_schema||'.'||u.udt_name as full_udt_name,
                        col_description((c.table_schema||'.'||c.table_name)::regclass::oid, c.ordinal_position) as column_comment
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
                self.logger.debug(f"PostgreSQL: Reading columns for {table_schema}.{table_name}")
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
                    'comment': row[7],
                    'other': other
                }
            cursor.close()
            self.disconnect()
            return result

        except psycopg2.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def convert_table_columns(self, settings):
        target_db_type = settings['target_db_type']
        target_schema = settings['target_schema']
        target_table_name = settings['target_table_name']
        source_columns = settings['source_columns']
        # type_mapping = {}
        create_table_sql = ""
        converted_schema = {}

        if target_db_type == 'postgresql':
            for col, info in source_columns.items():
                converted_schema[col] = {
                    'name': info['name'],
                    'nullable': info['nullable'],
                    'default': info['default'],
                    'comment': info['comment'],
                    'other': info['other']
                }
                if (info['length'] is not None
                    and info['type'].upper() in ('CHAR', 'VARCHAR', 'CHARACTER VARYING')):
                    converted_schema[col]['type'] = f"{info['type']}({info['length']})"
                else:
                    converted_schema[col]['type'] = info['type']

            create_table_sql = ', '.join([(f'''"{info["name"]}" {info["type"]} {info["nullable"]} {'DEFAULT ' + info['default'] if info['default'] else ''}''').strip()
                                          for _, info in converted_schema.items()])
            create_table_sql = f"""CREATE TABLE "{target_schema}"."{target_table_name}" ({create_table_sql})"""
        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

        return converted_schema, create_table_sql

    def fetch_indexes(self, settings):
        source_table_id = settings['source_table_id']
        source_schema = settings['source_schema']
        source_table_name = settings['source_table_name']
        target_schema = settings['target_schema']
        target_table_name = settings['target_table_name']
        target_columns = settings['target_columns']
        table_indexes = {}
        order_num = 1
        query = f"""
            SELECT
                i.indexname,
                i.indexdef,
                coalesce(c.constraint_type, 'INDEX') as type,
                obj_description(('"'||i.schemaname||'"."'||i.indexname||'"')::regclass::oid, 'pg_class') as index_comment
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
                index_columns = columns_match.group(1) if columns_match else ''
                index_name = row[0]
                index_type = row[2]
                index_sql = row[1]

                index_columns_count = 0
                index_columns_data_types = []
                for column_name in index_columns.split(','):
                    column_name = column_name.strip().strip('"')
                    for order_num, column_info in target_columns.items():
                        if column_name == column_info['name']:
                            index_columns_count += 1
                            column_data_type = column_info['type']
                            if self.config_parser.get_log_level() == 'DEBUG':
                                self.logger.debug(f"Table: {target_schema}.{target_table_name}, index: {index_name}, column: {column_name} has data type {column_data_type}")
                            index_columns_data_types.append(column_data_type)
                            index_columns_data_types_str = ', '.join(index_columns_data_types)

                if index_type == 'PRIMARY KEY':
                    index_sql = f'ALTER TABLE "{target_schema}"."{target_table_name}" ADD CONSTRAINT "{index_name}" PRIMARY KEY ({index_columns});'
                table_indexes[order_num] = {
                    'name': index_name,
                    'type': index_type,
                    'owner': '',
                    'columns': index_columns,
                    'columns_count': index_columns_count,
                    'columns_data_types': index_columns_data_types_str,
                    'sql': index_sql,
                    'comment': row[3]
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return table_indexes
        except psycopg2.Error as e:
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
                WHEN contype = 'p'
                    THEN 'PRIMARY KEY'
                WHEN contype = 'u'
                    THEN 'UNIQUE'
                WHEN contype = 't'
                    THEN 'TRIGGER'
                WHEN contype = 'x'
                    THEN 'EXCLUSION'
                ELSE contype::text
                END as type,
                pg_get_constraintdef(oid) as condef,
                obj_description(oid, 'pg_constraint') as constraint_comment
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
                constraint_sql = f'ALTER TABLE "{target_schema}"."{target_table_name}" ADD CONSTRAINT "{constraint_name}" {row[3]}'
                if constraint_type in ('PRIMARY KEY', 'p', 'P'):
                    continue # Primary key is handled in fetch_indexes
                constraints[order_num] = {
                    'id': row[0],
                    'name': constraint_name,
                    'type': constraint_type,
                    'sql': constraint_sql,
                    'comment': row[4]
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
        source_table_rows = 0
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
            batch_size = settings['batch_size']

            source_table_rows = self.get_rows_count(source_schema, source_table)
            if source_table_rows == 0:
                self.logger.info(f"Worker {worker_id}: Table {source_schema}.{source_table} has no rows. Skipping migration.")
                return 0
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

                target_table_rows = migrate_target_connection.get_rows_count(target_schema, target_table)
                self.logger.info(f"Worker {worker_id}: Finished migrating data for table {source_table}.")
                migrator_tables.update_data_migration_status(protocol_id, True, 'OK', target_table_rows)
                return source_table_rows
        except Exception as e:
            self.logger.error(f"Woker {worker_id}: Error in {part_name}: {e}")
            raise e

    def insert_batch(self, table_schema: str, table_name: str, columns: dict, data: list):
        inserted_rows = 0
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
                    if self.session_settings:
                        if self.config_parser.get_log_level() == 'DEBUG':
                            self.logger.info(f"Insert into {table_name}: Executing session settings: {self.session_settings}")
                        cursor.execute(self.session_settings)
                    psycopg2.extras.execute_batch(cursor, insert_query, data)
                    inserted_rows = len(data)
                except psycopg2.Error as e:
                    self.logger.error(f"Error inserting batch data into {table_name}: {e}")
                    self.logger.error(f"Trying to insert row by row.")
                    self.connection.rollback()
                    for row in data:
                        try:
                            cursor.execute(insert_query, row)
                            inserted_rows += 1
                            self.connection.commit()
                        except psycopg2.Error as e:
                            self.connection.rollback()
                            self.logger.error(f"Error inserting row into {table_name}: {row}")
                            self.logger.error(e)

        except psycopg2.Error as e:
            self.logger.error(f"Error before inserting batch data: {e}")
            raise
        finally:
            self.connection.commit()
            self.connection.autocommit = True
            return inserted_rows

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
                    'SELECT SETVAL( (SELECT oid from pg_class s where s.relname = ''' || c.relname ||
                    ''' and s.relkind = ''S'' AND s.relnamespace::regnamespace::text = ''' ||
                    c.relnamespace::regnamespace::text || '''), (SELECT MAX(' || quote_ident(a.attname) || ') /*+ 1*/ FROM ' ||
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

    def get_table_size(self, table_schema: str, table_name: str):
        query = f"""
            SELECT pg_total_relation_size('{table_schema}.{table_name}')
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        size = cursor.fetchone()[0]
        cursor.close()
        return size

    def convert_trigger(self, trigger_id: int, target_db_type: str, target_schema: str):
        pass

    def fetch_views_names(self, source_schema: str):
        views = {}
        order_num = 1
        query = f"""
            SELECT
                oid,
                relname as viewname,
                obj_description(oid, 'pg_class') as view_comment
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
                    'view_name': row[1],
                    'comment': row[2]
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return views
        except psycopg2.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_view_code(self, settings):
        view_id = settings['view_id']
        # source_schema = settings['source_schema']
        # source_view_name = settings['source_view_name']
        # target_schema = settings['target_schema']
        # target_view_name = settings['target_view_name']
        query = f"""
            SELECT definition
            FROM pg_views
            WHERE (schemaname||'.'||viewname)::regclass::oid = {view_id}
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
                'CREATE TYPE "'||t.typnamespace::regnamespace||'"."'||typname||'" As ENUM ('||string_agg(''''||e.enumlabel||'''', ',' ORDER BY e.enumsortorder)::text||');' AS elements,
                obj_description(t.oid, 'pg_type') as type_comment
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
                    'sql': row[2],
                    'comment': row[3]
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return user_defined_types
        except psycopg2.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def prepare_session_settings(self):
        """
        Prepare session settings for the database connection.
        """
        filtered_settings = ""
        try:
            settings = self.config_parser.get_target_db_session_settings()
            if not settings:
                self.logger.warning("No session settings found in config file.")
                return filtered_settings
            # self.logger.info(f"Preparing session settings: {settings} / {settings.keys()} / {tuple(settings.keys())}")
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute("SELECT name FROM pg_settings WHERE name IN %s", (tuple(settings.keys()),))
            matching_settings = cursor.fetchall()
            cursor.close()
            self.disconnect()
            if not matching_settings:
                self.logger.warning("No settings found to prepare.")
                return filtered_settings

            for setting in matching_settings:
                setting_name = setting[0]
                if setting_name in ['search_path']:
                    filtered_settings += f"SET {setting_name} = {settings[setting_name]};"
                else:
                    filtered_settings += f"SET {setting_name} = '{settings[setting_name]}';"
            self.logger.info(f"Session settings: {filtered_settings}")
            return filtered_settings
        except psycopg2.Error as e:
            self.logger.error(f"Error preparing session settings: {e}")
            raise

    def testing_select(self):
        return "SELECT 1"
