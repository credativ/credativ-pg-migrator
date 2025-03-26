import json
from postgresql_connector import PostgreSQLConnector
class MigratorTables:
    def __init__(self, logger, config_parser):
        self.logger = logger
        self.config_parser = config_parser
        protocol_db_type = self.config_parser.get_migrator_db_type()
        if protocol_db_type == 'postgresql':
            self.protocol_connection = PostgreSQLConnector(self.config_parser, 'target')
        else:
            raise ValueError(f"Unsupported database type for protocol table: {protocol_db_type}")
        self.protocol_connection.connect()
        self.protocol_schema = self.config_parser.get_migrator_schema()
        self.drop_table_sql = """DROP TABLE IF EXISTS "{protocol_schema}"."{table_name}";"""

    def create_all(self):
        self.create_protocol()
        self.create_table_for_main()
        self.create_table_for_user_defined_types()
        self.create_table_for_tables()
        self.create_table_for_indexes()
        self.create_table_for_constraints()
        self.create_table_for_funcprocs()
        self.create_table_for_sequences()
        self.create_table_for_triggers()
        self.create_table_for_views()

    def prepare_data_types_substitution(self):
        # Drop table if exists
        self.protocol_connection.execute_query(f"""
        DROP TABLE IF EXISTS "{self.protocol_schema}".data_types_substitution;
        """)
        # Create table if not exists
        self.protocol_connection.execute_query(f"""
        CREATE TABLE IF NOT EXISTS "{self.protocol_schema}".data_types_substitution (
        source_type TEXT,
        target_type TEXT,
        target_type_length TEXT,
        inserted TIMESTAMP DEFAULT clock_timestamp()
        )
        """)

        # Insert data into the table
        for source_type, target_type, target_type_length in self.config_parser.get_data_types_substitution():
            self.protocol_connection.execute_query(f"""
            INSERT INTO "{self.protocol_schema}".data_types_substitution
            (source_type, target_type, target_type_length)
            VALUES (%s, %s, %s)
            """, (source_type, target_type, target_type_length))

    def check_data_types_substitution(self, check_type):
        query = f"""
        SELECT target_type, target_type_length
        FROM "{self.protocol_schema}".data_types_substitution
        WHERE trim('{check_type}') = trim(source_type)
        """
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        if result:
            return result[0], result[1]
        else:
            query = f"""
            SELECT target_type, target_type_length
            FROM "{self.protocol_schema}".data_types_substitution
            WHERE trim('{check_type}') LIKE trim(source_type)
            """
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            if result:
                return result[0], result[1]
            else:
                return None, None

    def prepare_default_values_substitution(self):
        # Drop table if exists
        self.protocol_connection.execute_query(f"""
        DROP TABLE IF EXISTS "{self.protocol_schema}".default_values_substitution;
        """)
        # Create table if not exists
        self.protocol_connection.execute_query(f"""
        CREATE TABLE IF NOT EXISTS "{self.protocol_schema}".default_values_substitution (
        column_name TEXT,
        source_column_data_type TEXT,
        source_default_value TEXT,
        target_default_value TEXT,
        inserted TIMESTAMP DEFAULT clock_timestamp()
        )
        """)

        # Insert data into the table
        for column_name, source_column_data_type, source_default_value, target_default_value in self.config_parser.get_default_values_substitution():
            self.protocol_connection.execute_query(f"""
            INSERT INTO "{self.protocol_schema}".default_values_substitution
            (column_name, source_column_data_type, source_default_value, target_default_value)
            VALUES (%s, %s, %s, %s)
            """, (column_name, source_column_data_type, source_default_value, target_default_value))

    def check_default_values_substitution(self, check_column_name, check_column_data_type, check_default_value):
        target_default_value = check_default_value
        query = f"""
            SELECT target_default_value
            FROM "{self.protocol_schema}".default_values_substitution
            WHERE upper(trim(%s)) LIKE upper(trim(column_name))
            AND upper(trim(%s)) LIKE upper(trim(source_column_data_type))
            AND upper(trim(%s::TEXT)) LIKE upper(trim(source_default_value::TEXT))
        """
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query, (check_column_name, check_column_data_type, check_default_value))
        result = cursor.fetchone()
        # if self.config_parser.get_log_level() == 'DEBUG':
        #     self.logger.debug(f"1 check_default_values_substitution {check_column_name}, {check_column_data_type}, {check_default_value} query: {query} - {result}")
        if result:
            target_default_value = result[0]
        else:
            query = f"""
                SELECT target_default_value
                FROM "{self.protocol_schema}".default_values_substitution
                WHERE upper(trim(column_name)) = ''
                AND upper(trim(%s)) LIKE upper(trim(source_column_data_type))
                AND upper(trim(%s::TEXT)) LIKE upper(trim(source_default_value::TEXT))
            """
            cursor.execute(query, (check_column_data_type, check_default_value))
            result = cursor.fetchone()
            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"2 check_default_values_substitution {check_column_name}, {check_column_data_type}, {check_default_value} query: {query} - {result}")
            if result:
                target_default_value = result[0]
            else:
                query = f"""
                    SELECT target_default_value
                    FROM "{self.protocol_schema}".default_values_substitution
                    WHERE upper(trim(column_name)) = ''
                    AND upper(trim(source_column_data_type)) = ''
                    AND upper(trim(%s::TEXT)) LIKE upper(trim(source_default_value::TEXT))
                """
                cursor.execute(query, (check_default_value,))
                result = cursor.fetchone()
                # if self.config_parser.get_log_level() == 'DEBUG':
                #     self.logger.debug(f"3 check_default_values_substitution {check_column_name}, {check_column_data_type}, {check_default_value} query: {query} - {result}")
                if result:
                    target_default_value = result[0]
        cursor.close()
        return target_default_value

    def create_protocol(self):
        table_name = self.config_parser.get_protocol_name()
        query = f"""CREATE SCHEMA IF NOT EXISTS "{self.protocol_schema}" """
        self.protocol_connection.execute_query(query)

        query = f"""
        CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}" (
            id SERIAL PRIMARY KEY,
            object_type TEXT,
            object_name TEXT,
            object_action TEXT,
            object_ddl TEXT,
            insertion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            execution_timestamp TIMESTAMP,
            execution_success BOOLEAN,
            execution_error_message TEXT,
            row_type TEXT default 'info',
            execution_results TEXT,
            object_protocol_id BIGINT
        );
        """
        self.protocol_connection.execute_query(query)
        # if self.config_parser.get_log_level() == 'DEBUG':
        #     self.logger.debug(f"Protocol table {table_name} created.")

    def create_table_for_main(self):
        table_name = self.config_parser.get_protocol_name_main()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            task_name TEXT,
            task_started TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        # if self.config_parser.get_log_level() == 'DEBUG':
        #     self.logger.debug(f"Tasks table {table_name} created.")

    def insert_main(self, task_name):
        table_name = self.config_parser.get_protocol_name_main()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (task_name) VALUES ('{task_name}')
            RETURNING *
        """
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Returned row: {row}")
            main_row = self.decode_main_row(row)
            self.insert_protocol('main', task_name, 'start', None, None, None, None, 'info', None, main_row['id'])
        except Exception as e:
            self.logger.error(f"Error inserting task {task_name} into {table_name}.")
            self.logger.error(e)
            raise

    def update_main_status(self, task_name, success, message):
        table_name = self.config_parser.get_protocol_name_main()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = CURRENT_TIMESTAMP,
            success = %s,
            message = %s
            WHERE task_name = %s
            RETURNING *
        """
        params = ('TRUE' if success else 'FALSE', message, task_name)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Returned row: {row}")
            if row:
                main_row = self.decode_main_row(row)
                self.update_protocol('main', main_row['id'], success, message, None)
            else:
                self.logger.error(f"Error updating status for task {task_name} in {table_name}.")
                self.logger.error(f"Error: No protocol row returned.")
        except Exception as e:
            self.logger.error(f"Error updating status for task {task_name} in {table_name}.")
            self.logger.error(f"Query: {query}")
            self.logger.error(e)
            raise

    def decode_main_row(self, row):
        return {
            'id': row[0],
            'task_name': row[1],
            'task_started': row[2],
            'task_completed': row[3],
            'success': row[4],
            'message': row[5]
        }

    def create_table_for_user_defined_types(self):
        table_name = self.config_parser.get_protocol_name_user_defined_types()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema_name TEXT,
            source_type_name TEXT,
            source_type_sql TEXT,
            target_schema_name TEXT,
            target_type_name TEXT,
            target_type_sql TEXT,
            task_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        # if self.config_parser.get_log_level() == 'DEBUG':
        #     self.logger.debug(f"User defined types table {table_name} created.")

    def insert_user_defined_type(self, source_schema_name, source_type_name, source_type_sql, target_schema_name, target_type_name, target_type_sql):
        table_name = self.config_parser.get_protocol_name_user_defined_types()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema_name, source_type_name, source_type_sql, target_schema_name, target_type_name, target_type_sql)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema_name, source_type_name, source_type_sql, target_schema_name, target_type_name, target_type_sql)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Returned row: {row}")
            user_defined_type_row = self.decode_user_defined_type_row(row)
            self.insert_protocol('user_defined_type', target_type_name, 'create', target_type_sql, None, None, None, 'info', None, user_defined_type_row['id'])
        except Exception as e:
            self.logger.error(f"Error inserting user defined type {target_type_name} into {table_name}.")
            self.logger.error(e)
            raise

    def update_user_defined_type_status(self, row_id, success, message):
        table_name = self.config_parser.get_protocol_name_user_defined_types()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = CURRENT_TIMESTAMP,
            success = %s,
            message = %s
            WHERE id = %s
            RETURNING *
        """
        params = ('TRUE' if success else 'FALSE', message, row_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Returned row: {row}")
            if row:
                user_defined_type_row = self.decode_user_defined_type_row(row)
                self.update_protocol('user_defined_type', user_defined_type_row['id'], success, message, None)
            else:
                self.logger.error(f"Error updating status for user defined type {row_id} in {table_name}.")
                self.logger.error(f"Error: No protocol row returned.")
        except Exception as e:
            self.logger.error(f"Error updating status for user defined type {row_id} in {table_name}.")
            self.logger.error(f"Query: {query}")
            self.logger.error(e)
            raise

    def decode_user_defined_type_row(self, row):
        return {
            'id': row[0],
            'source_schema_name': row[1],
            'source_type_name': row[2],
            'source_type_sql': row[3],
            'target_schema_name': row[4],
            'target_type_name': row[5],
            'target_type_sql': row[6]
        }

    def fetch_all_user_defined_types(self):
        table_name = self.config_parser.get_protocol_name_user_defined_types()
        query = f"""
            SELECT *
            FROM "{self.protocol_schema}"."{table_name}" ORDER BY id
        """
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def create_table_for_data_migration(self):
        table_name = self.config_parser.get_protocol_name_data_migration()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema TEXT,
            source_table TEXT,
            source_table_id INTEGER,
            source_table_rows BIGINT    ,
            target_schema TEXT,
            target_table TEXT,
            target_table_rows BIGINT,
            task_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        # if self.config_parser.get_log_level() == 'DEBUG':
        #     self.logger.debug(f"Data migration table {table_name} created.")

    def insert_data_migration(self, source_schema, source_table, source_table_id, source_table_rows, target_schema, target_table, target_table_rows):
        table_name = self.config_parser.get_protocol_name_data_migration()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema, source_table, source_table_id, source_table_rows, target_schema, target_table, target_table_rows)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema, source_table, source_table_id, source_table_rows, target_schema, target_table, target_table_rows)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Returned row: {row}")
            data_migration_row = self.decode_data_migration_row(row)
            self.insert_protocol('data_migration', target_table, 'create', None, None, None, None, 'info', None, data_migration_row['id'])
        except Exception as e:
            self.logger.error(f"Error inserting data migration {target_table} into {table_name}.")
            self.logger.error(e)
            raise

    def update_data_migration_status(self, row_id, success, message, target_table_rows):
        table_name = self.config_parser.get_protocol_name_data_migration()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = CURRENT_TIMESTAMP,
            success = %s,
            message = %s,
            target_table_rows = %s
            WHERE id = %s
            RETURNING *
        """
        params = ('TRUE' if success else 'FALSE', message, target_table_rows, row_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Returned row: {row}")
            if row:
                data_migration_row = self.decode_data_migration_row(row)
                self.update_protocol('data_migration', data_migration_row['id'], success, message, 'source rows: ' + str(data_migration_row['source_table_rows']) + ', target rows: ' + str(target_table_rows))
            else:
                self.logger.error(f"Error updating status for data migration {row_id} in {table_name}.")
                self.logger.error(f"Error: No protocol row returned.")
        except Exception as e:
            self.logger.error(f"Error updating status for data migration {row_id} in {table_name}.")
            self.logger.error(f"Query: {query}")
            self.logger.error(e)
            raise

    def create_table_for_tables(self):
        table_name = self.config_parser.get_protocol_name_tables()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema TEXT,
            source_table TEXT,
            source_table_id INTEGER,
            source_columns TEXT,
            target_schema TEXT,
            target_table TEXT,
            target_columns TEXT,
            target_table_sql TEXT,
            task_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        # if self.config_parser.get_log_level() == 'DEBUG':
        #     self.logger.debug(f"Tasks table {table_name} created.")

    def create_table_for_indexes(self):
        table_name = self.config_parser.get_protocol_name_indexes()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema TEXT,
            source_table TEXT,
            source_table_id INTEGER,
            index_name TEXT,
            index_type VARCHAR(30),
            target_schema TEXT,
            target_table TEXT,
            index_sql TEXT,
            index_columns TEXT,
            task_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        # if self.config_parser.get_log_level() == 'DEBUG':
        #     self.logger.debug(f"Indexes table {table_name} created.")

    def create_table_for_constraints(self):
        table_name = self.config_parser.get_protocol_name_constraints()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema TEXT,
            source_table TEXT,
            source_table_id INTEGER,
            constraint_name TEXT,
            constraint_type TEXT,
            target_schema TEXT,
            target_table TEXT,
            constraint_sql TEXT,
            task_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        # if self.config_parser.get_log_level() == 'DEBUG':
        #     self.logger.debug(f"Constraints table {table_name} created.")

    def create_table_for_funcprocs(self):
        table_name = self.config_parser.get_protocol_name_funcprocs()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema TEXT,
            source_funcproc_name TEXT,
            source_funcproc_id INTEGER,
            source_funcproc_sql TEXT,
            target_schema TEXT,
            target_funcproc_name TEXT,
            target_funcproc_sql TEXT,
            task_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        # if self.config_parser.get_log_level() == 'DEBUG':
        #     self.logger.debug(f"Funcprocs table {table_name} created.")

    def create_table_for_sequences(self):
        table_name = self.config_parser.get_protocol_name_sequences()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (sequence_id INTEGER,
            schema_name TEXT,
            table_name TEXT,
            column_name TEXT,
            sequence_name TEXT,
            set_sequence_sql TEXT,
            task_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)

    def create_table_for_triggers(self):
        table_name = self.config_parser.get_protocol_name_triggers()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema TEXT,
            source_table TEXT,
            source_table_id INTEGER,
            target_schema TEXT,
            target_table TEXT,
            trigger_id BIGINT,
            trigger_name TEXT,
            trigger_event TEXT,
            trigger_new TEXT,
            trigger_old TEXT,
            trigger_row_statement TEXT,
            trigger_source_sql TEXT,
            trigger_target_sql TEXT,
            task_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)

    def create_table_for_views(self):
        table_name = self.config_parser.get_protocol_name_views()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema TEXT,
            source_view_name TEXT,
            source_view_id INTEGER,
            source_view_sql TEXT,
            target_schema TEXT,
            target_view_name TEXT,
            target_view_sql TEXT,
            task_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)

    def decode_protocol_row(self, row):
        return {
            'id': row[0],
            'object_type': row[1],
            'object_name': row[2],
            'object_action': row[3],
            'object_ddl': row[4],
            'execution_timestamp': row[5],
            'execution_success': row[6],
            'execution_error_message': row[7],
            'row_type': row[8],
            'execution_results': row[9],
            'object_protocol_id': row[10]
        }

    def decode_table_row(self, row):
        return {
            'id': row[0],
            'source_schema': row[1],
            'source_table': row[2],
            'source_table_id': row[3],
            'source_columns': json.loads(row[4]),
            'target_schema': row[5],
            'target_table': row[6],
            'target_columns': json.loads(row[7]),
            'target_table_sql': row[8]
        }

    def decode_index_row(self, row):
        return {
            'id': row[0],
            'source_schema': row[1],
            'source_table': row[2],
            'source_table_id': row[3],
            'index_name': row[4],
            'index_type': row[5],
            'target_schema': row[6],
            'target_table': row[7],
            'index_sql': row[8],
            'index_columns': row[9]
        }

    def decode_constraint_row(self, row):
        return {
            'id': row[0],
            'source_schema': row[1],
            'source_table': row[2],
            'source_table_id': row[3],
            'constraint_name': row[4],
            'constraint_type': row[5],
            'target_schema': row[6],
            'target_table': row[7],
            'constraint_sql': row[8]
        }

    def decode_funcproc_row(self, row):
        return {
            'id': row[0],
            'source_schema': row[1],
            'source_funcproc_name': row[2],
            'source_funcproc_id': row[3],
            'source_funcproc_sql': row[4],
            'target_schema': row[5],
            'target_funcproc_name': row[6],
            'target_funcproc_sql': row[7]
        }

    def decode_sequence_row(self, row):
        return {
            'sequence_id': row[0],
            'schema_name': row[1],
            'table_name': row[2],
            'column_name': row[3],
            'sequence_name': row[4],
            'set_sequence_sql': row[5]
        }

    def decode_trigger_row(self, row):
        return {
            'id': row[0],
            'source_schema': row[1],
            'source_table': row[2],
            'source_table_id': row[3],
            'target_schema': row[4],
            'target_table': row[5],
            'trigger_id': row[6],
            'trigger_name': row[7],
            'trigger_event': row[8],
            'trigger_new': row[9],
            'trigger_old': row[10],
            'trigger_row_statement': row[11],
            'trigger_source_sql': row[12],
            'trigger_target_sql': row[13]
        }

    def decode_view_row(self, row):
        return {
            'id': row[0],
            'source_schema': row[1],
            'source_view_name': row[2],
            'source_view_id': row[3],
            'source_view_sql': row[4],
            'target_schema': row[5],
            'target_view_name': row[6],
            'target_view_sql': row[7]
        }

    def insert_protocol(self, object_type, object_name, object_action, object_ddl, execution_timestamp, execution_success, execution_error_message, row_type, execution_results, object_protocol_id):
        table_name = self.config_parser.get_protocol_name()
        query = f"""
        INSERT INTO "{self.protocol_schema}"."{table_name}"
        (object_type, object_name, object_action, object_ddl, execution_timestamp, execution_success, execution_error_message, row_type, execution_results, object_protocol_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (object_type, object_name, object_action, object_ddl, execution_timestamp, execution_success, execution_error_message, row_type, execution_results, object_protocol_id)
        try:
            self.protocol_connection.execute_query(query, params)
            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Info for {object_name} inserted into {table_name}.")
        except Exception as e:
            self.logger.error(f"Error inserting info {object_name} into {table_name}.")
            self.logger.error(e)
            raise

    def update_protocol(self, object_type, object_protocol_id, execution_success, execution_error_message, execution_results):
        table_name = self.config_parser.get_protocol_name()
        query = f"""
        UPDATE "{self.protocol_schema}"."{table_name}"
        SET execution_success = %s,
        execution_error_message = %s,
        execution_results = %s,
        execution_timestamp = CURRENT_TIMESTAMP
        WHERE object_protocol_id = %s
        AND object_type = %s
        """
        params = ('TRUE' if execution_success else 'FALSE', execution_error_message, execution_results, object_protocol_id, object_type)
        try:
            self.protocol_connection.execute_query(query, params)
            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Info for {object_protocol_id} updated in {table_name}.")
        except Exception as e:
            self.logger.error(f"Error updating info {object_protocol_id} in {table_name}.")
            self.logger.error(e)
            raise

    def insert_tables(self, source_schema, source_table, source_table_id, source_columns, target_schema, target_table, target_columns, target_table_sql):
        table_name = self.config_parser.get_protocol_name_tables()
        source_columns_str = json.dumps(source_columns)
        target_columns_str = json.dumps(target_columns)
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema, source_table, source_table_id, source_columns, target_schema, target_table, target_columns, target_table_sql)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema, source_table, source_table_id, source_columns_str, target_schema, target_table, target_columns_str, target_table_sql)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"(insert_tables) returned row: {row}")
            table_row = self.decode_table_row(row)
            self.insert_protocol('table', target_table, 'create', target_table_sql, None, None, None, 'info', None, table_row['id'])
        except Exception as e:
            self.logger.error(f"Error inserting table info {source_table} into {table_name}.")
            self.logger.error(f"Source columns: {source_columns}")
            self.logger.error(f"Target columns: {target_columns}")
            self.logger.error(e)
            raise

    def update_table_status(self, row_id, success, message):
        table_name = self.config_parser.get_protocol_name_tables()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = CURRENT_TIMESTAMP,
            success = %s,
            message = %s
            WHERE id = %s
            RETURNING *
        """
        params = ('TRUE' if success else 'FALSE', message, row_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"(update_table_status) returned row: {row}")
            if row:
                table_row = self.decode_table_row(row)
                self.update_protocol('table', table_row['id'], success, message, None)
            else:
                self.logger.error(f"Error updating status for table {row_id} in {table_name}.")
                self.logger.error(f"Error: No protocol row returned.")
        except Exception as e:
            self.logger.error(f"Error updating status for table {row_id} in {table_name}.")
            self.logger.error(f"Query: {query}")
            self.logger.error(e)
            raise

    def insert_indexes(self, source_schema, source_table, source_table_id, index_name, index_type, target_schema, target_table, index_sql, index_columns):
        table_name = self.config_parser.get_protocol_name_indexes()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema, source_table, source_table_id, index_name, index_type, target_schema, target_table, index_sql, index_columns)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema, source_table, source_table_id, index_name, index_type, target_schema, target_table, index_sql, index_columns)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Returned row: {row}")
            index_row = self.decode_index_row(row)
            self.insert_protocol('index', index_name, 'create', index_sql, None, None, None, 'info', None, index_row['id'])
        except Exception as e:
            self.logger.error(f"Error inserting index info {index_name} into {table_name}.")
            self.logger.error(e)
            raise

    def update_index_status(self, row_id, success, message):
        table_name = self.config_parser.get_protocol_name_indexes()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = CURRENT_TIMESTAMP,
            success = %s,
            message = %s
            WHERE id = %s
            RETURNING *
        """
        params = ('TRUE' if success else 'FALSE', message, row_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Returned row: {row}")
            if row:
                index_row = self.decode_index_row(row)
                self.update_protocol('index', index_row['id'], success, message, None)
            else:
                self.logger.error(f"Error updating status for index {row_id} in {table_name}.")
                self.logger.error(f"Error: No protocol row returned.")
        except Exception as e:
            self.logger.error(f"Error updating status for index {row_id} in {table_name}.")
            self.logger.error(f"Query: {query}")
            self.logger.error(e)
            raise

    def insert_constraints(self, source_schema, source_table, source_table_id, constraint_name, constraint_type, target_schema, target_table, constraint_sql):
        table_name = self.config_parser.get_protocol_name_constraints()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema, source_table, source_table_id, constraint_name, constraint_type, target_schema, target_table, constraint_sql)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema, source_table, source_table_id, constraint_name, constraint_type, target_schema, target_table, constraint_sql)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Returned row: {row}")
            constraint_row = self.decode_constraint_row(row)
            self.insert_protocol('constraint', constraint_name, 'create', constraint_sql, None, None, None, 'info', None, constraint_row['id'])
        except Exception as e:
            self.logger.error(f"Error inserting constraint info {constraint_name} into {table_name}.")
            self.logger.error(f"Query: {query}")
            self.logger.error(e)
            raise

    def update_constraint_status(self, row_id, success, message):
        table_name = self.config_parser.get_protocol_name_constraints()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = CURRENT_TIMESTAMP,
            success = %s,
            message = %s
            WHERE id = %s
            RETURNING *
        """
        params = ('TRUE' if success else 'FALSE', message, row_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Returned row: {row}")
            if row:
                constraint_row = self.decode_constraint_row(row)
                self.update_protocol('constraint', constraint_row['id'], success, message, None)
            else:
                self.logger.error(f"Error updating status for constraint {row_id} in {table_name}.")
                self.logger.error(f"Error: No protocol row returned.")
        except Exception as e:
            self.logger.error(f"Error updating status for constraint {row_id} in {table_name}.")
            self.logger.error(f"Query: {query}")
            self.logger.error(e)
            raise

    def insert_funcprocs(self, source_schema, source_funcproc_name, source_funcproc_id, source_funcproc_sql, target_schema, target_funcproc_name, target_funcproc_sql):
        table_name = self.config_parser.get_protocol_name_funcprocs()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema, source_funcproc_name, source_funcproc_id, source_funcproc_sql, target_schema, target_funcproc_name, target_funcproc_sql)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema, source_funcproc_name, source_funcproc_id, source_funcproc_sql, target_schema, target_funcproc_name, target_funcproc_sql)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Returned row: {row}")
            funcproc_row = self.decode_funcproc_row(row)
            self.insert_protocol('funcproc', source_funcproc_name, 'create', target_funcproc_sql, None, None, None, 'info', None, funcproc_row['id'])
        except Exception as e:
            self.logger.error(f"Error inserting funcproc info {source_funcproc_name} into {table_name}.")
            self.logger.error(e)
            raise

    def update_funcproc_status(self, source_funcproc_id, success, message):
        table_name = self.config_parser.get_protocol_name_funcprocs()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = CURRENT_TIMESTAMP,
            success = %s,
            message = %s
            WHERE source_funcproc_id = %s
            RETURNING *
        """
        params = ('TRUE' if success else 'FALSE', message, source_funcproc_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Parameters: {params}")
            #     self.logger.debug(f"Returned row: {row}")
            if row:
                funcproc_row = self.decode_funcproc_row(row)
                self.update_protocol('funcproc', funcproc_row['id'], success, message, None)
            else:
                self.logger.error(f"Error updating status for funcproc {source_funcproc_id} in {table_name}.")
                self.logger.error(f"Error: No protocol row returned.")
        except Exception as e:
            self.logger.error(f"Error updating status for funcproc {source_funcproc_id} in {table_name}.")
            self.logger.error(f"Query: {query}")
            self.logger.error(e)
            raise

    def insert_sequence(self, sequence_id, schema_name, table_name, column_name, sequence_name, set_sequence_sql):
        table_name = self.config_parser.get_protocol_name_sequences()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (sequence_id, schema_name, table_name, column_name, sequence_name, set_sequence_sql)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (sequence_id, schema_name, table_name, column_name, sequence_name, set_sequence_sql)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Returned row: {row}")
            sequence_row = self.decode_sequence_row(row)
            self.insert_protocol('sequence', sequence_name, 'create', set_sequence_sql, None, None, None, 'info', None, sequence_row['sequence_id'])
        except Exception as e:
            self.logger.error(f"Error inserting sequence info {sequence_name} into {table_name}.")
            self.logger.error(e)
            raise

    def update_sequence_status(self, sequence_id, success, message):
        table_name = self.config_parser.get_protocol_name_sequences()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = CURRENT_TIMESTAMP,
            success = %s,
            message = %s
            WHERE id = %s
            RETURNING *
        """
        params = ('TRUE' if success else 'FALSE', message, sequence_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Returned row: {row}")
            if row:
                sequence_row = self.decode_sequence_row(row)
                self.update_protocol('sequence', sequence_row['sequence_id'], success, message, None)
            else:
                self.logger.error(f"Error updating status for sequence {sequence_id} in {table_name}.")
                self.logger.error(f"Error: No protocol row returned.")
        except Exception as e:
            self.logger.error(f"Error updating status for sequence_if {sequence_id} in {table_name}.")
            self.logger.error(f"Query: {query}")
            self.logger.error(e)
            raise

    def insert_trigger(self, source_schema, source_table, source_table_id, target_schema, target_table, trigger_id, trigger_name, trigger_event, trigger_new, trigger_old, trigger_source_sql, trigger_target_sql):
        table_name = self.config_parser.get_protocol_name_triggers()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema, source_table, source_table_id, target_schema, target_table, trigger_id, trigger_name, trigger_event, trigger_new, trigger_old, trigger_source_sql, trigger_target_sql)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema, source_table, source_table_id, target_schema, target_table, trigger_id, trigger_name, trigger_event, trigger_new, trigger_old, trigger_source_sql, trigger_target_sql)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Returned row: {row}")
            trigger_row = self.decode_trigger_row(row)
            self.insert_protocol('trigger', trigger_name, 'create', trigger_target_sql, None, None, None, 'info', None, trigger_row['id'])
        except Exception as e:
            self.logger.error(f"Error inserting trigger info {trigger_name} into {table_name}.")
            self.logger.error(e)
            raise

    def update_trigger_status(self, row_id, success, message):
        table_name = self.config_parser.get_protocol_name_triggers()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = CURRENT_TIMESTAMP,
            success = %s,
            message = %s
            WHERE id = %s
            RETURNING *
        """
        params = ('TRUE' if success else 'FALSE', message, row_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Returned row: {row}")
            if row:
                trigger_row = self.decode_trigger_row(row)
                self.update_protocol('trigger', trigger_row['id'], success, message, None)
            else:
                self.logger.error(f"Error updating status for trigger {row_id} in {table_name}.")
                self.logger.error(f"Error: No protocol row returned.")
        except Exception as e:
            self.logger.error(f"Error updating status for trigger {row_id} in {table_name}.")
            self.logger.error(f"Query: {query}")
            self.logger.error(e)
            raise

    def select_triggers(self):
        table_name = self.config_parser.get_protocol_name_triggers()
        query = f"""
            SELECT * FROM "{self.protocol_schema}"."{table_name}" ORDER BY id
        """
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except Exception as e:
            self.logger.error(f"Error selecting triggers.")
            self.logger.error(e)
            return None

    def insert_view(self, source_schema, source_view_name, source_view_id, source_view_sql, target_schema, target_view_name, target_view_sql):
        table_name = self.config_parser.get_protocol_name_views()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema, source_view_name, source_view_id, source_view_sql, target_schema, target_view_name, target_view_sql)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema, source_view_name, source_view_id, source_view_sql, target_schema, target_view_name, target_view_sql)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Returned row: {row}")
            view_row = self.decode_view_row(row)
            self.insert_protocol('view', source_view_name, 'create', target_view_sql, None, None, None, 'info', None, view_row['id'])
        except Exception as e:
            self.logger.error(f"Error inserting view info {source_view_name} into {table_name}.")
            self.logger.error(e)
            raise

    def select_views(self):
        table_name = self.config_parser.get_protocol_name_views()
        query = f"""
            SELECT * FROM "{self.protocol_schema}"."{table_name}" ORDER BY id
        """
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except Exception as e:
            self.logger.error(f"Error selecting views.")
            self.logger.error(e)
            return None

    def update_view_status(self, row_id, success, message):
        table_name = self.config_parser.get_protocol_name_views()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = CURRENT_TIMESTAMP,
            success = %s,
            message = %s
            WHERE id = %s
            RETURNING *
        """
        params = ('TRUE' if success else 'FALSE', message, row_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Returned row: {row}")
            if row:
                view_row = self.decode_view_row(row)
                self.update_protocol('view', view_row['id'], success, message, None)
            else:
                self.logger.error(f"Error updating status for view {row_id} in {table_name}.")
                self.logger.error(f"Error: No protocol row returned.")
        except Exception as e:
            self.logger.error(f"Error updating status for view {row_id} in {table_name}.")
            self.logger.error(f"Query: {query}")
            self.logger.error(e)
            raise

    def select_primary_key(self, target_schema, target_table):
        tables_table = self.config_parser.get_protocol_name_tables()
        indexes_table = self.config_parser.get_protocol_name_indexes()
        query = f"""
            SELECT i.index_columns
            FROM "{self.protocol_schema}"."{tables_table}" t
            JOIN "{self.protocol_schema}"."{indexes_table}" i ON i.source_table_id = t.source_table_id
            WHERE t.target_schema = '{target_schema}' AND
                t.target_table = '{target_table}' AND
                index_type = 'PRIMARY KEY'
        """
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query)
            index_columns = cursor.fetchone()
            cursor.close()
            if index_columns:
                return index_columns[0]
            else:
                return None
        except Exception as e:
            self.logger.error(f"Error selecting primary key for {target_schema}.{target_table}.")
            self.logger.error(e)
            return None

    def print_summary(self, objects, migrator_table_name, additional_columns=None):
        try:
            self.logger.info(f"{objects} summary:")
            query = f"""SELECT COUNT(*) FROM "{self.protocol_schema}"."{migrator_table_name}" """
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query)
            summary = cursor.fetchone()[0]
            self.logger.info(f"    Found in source: {summary}")
            if additional_columns:
                columns_count = len(additional_columns.split(','))
                columns_numbers = ', '.join(str(i + 2) for i in range(columns_count))
                query = f"""SELECT COUNT(*), {additional_columns} FROM "{self.protocol_schema}"."{migrator_table_name}" GROUP BY {columns_numbers} ORDER BY {columns_numbers}"""
                cursor.execute(query)
                rows = cursor.fetchall()
                for row in rows:
                    self.logger.info(f"        {row[1:]}: {row[0]}")

            query = f"""SELECT success, COUNT(*) FROM "{self.protocol_schema}"."{migrator_table_name}" GROUP BY 1 ORDER BY 1"""
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                status = "successfully migrated" if row[0] else "error" if row[0] is False else "unknown status"
                self.logger.info(f"    {status}: {row[1]}")
            if additional_columns:
                query = f"""SELECT COUNT(*), {additional_columns} FROM "{self.protocol_schema}"."{migrator_table_name}" WHERE success = True GROUP BY {columns_numbers} ORDER BY {columns_numbers}"""
                cursor.execute(query)
                rows = cursor.fetchall()
                for row in rows:
                    status = "successfully migrated" if row[0] else "error" if row[0] is False else "unknown status"
                    self.logger.info(f"        {row[1:]}: {row[0]}")
            cursor.close()

        except Exception as e:
            self.logger.error(f"Error printing migration summary.")
            self.logger.error(e)
            raise

    def print_main(self, migrator_table_name):
        try:
            query = f"""SELECT * FROM "{self.protocol_schema}"."{migrator_table_name}" ORDER BY id"""
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            for row in rows:
                task_data = self.decode_main_row(row)
                if task_data['task_completed'] and task_data['task_started']:
                    length = task_data['task_completed'] - task_data['task_started']
                else:
                    length = "none"
                status = f"{task_data['task_name'][:40]:<40} -> start: {str(task_data['task_started'])[:19]:<19} | end: {str(task_data['task_completed'])[:19]:<19} | length: {str(length)[:19]}"
                self.logger.info(f"{status}")
        except Exception as e:
            self.logger.error(f"Error printing migration summary.")
            self.logger.error(e)
            raise

    def print_migration_summary(self):
        self.logger.info("Migration time stats:")
        self.print_main(self.config_parser.get_protocol_name_main())
        self.logger.info("Migration summary:")
        self.print_summary('User Defined Types', self.config_parser.get_protocol_name_user_defined_types())
        self.print_summary('Tables', self.config_parser.get_protocol_name_tables())
        self.print_summary('Indexes', self.config_parser.get_protocol_name_indexes(), 'index_type')
        self.print_summary('Constraints', self.config_parser.get_protocol_name_constraints(), 'constraint_type')
        self.print_summary('Functions / procedures', self.config_parser.get_protocol_name_funcprocs())
        self.print_summary('Sequences', self.config_parser.get_protocol_name_sequences())
        self.print_summary('Triggers', self.config_parser.get_protocol_name_triggers())
        self.print_summary('Views', self.config_parser.get_protocol_name_views())

    def fetch_all_tables(self):
        query = f"""SELECT * FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_tables()}" ORDER BY id"""
        # self.protocol_connection.connect()
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        tables = cursor.fetchall()
        return tables

    def fetch_all_target_table_names(self):
        tables = self.fetch_all_tables()
        table_names = []
        for table in tables:
            table_names.append(table[6])
        return table_names

    def fetch_all_indexes(self):
        query = f"""SELECT * FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_indexes()}" ORDER BY id"""
        # self.protocol_connection.connect()
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        indexes = cursor.fetchall()
        return indexes

    def fetch_all_constraints(self):
        query = f"""SELECT * FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_constraints()}" ORDER BY id"""
        # self.protocol_connection.connect()
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        constraints = cursor.fetchall()
        return constraints
