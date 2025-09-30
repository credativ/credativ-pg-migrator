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

import json
import psycopg2
from credativ_pg_migrator.constants import MigratorConstants

class ProtocolPostgresConnection:
    def __init__(self, config_parser):
        self.config_parser = config_parser
        self.connection = None

    def connect(self):
        cfg = self.config_parser.get_migrator_config()
        if not cfg:
            raise ValueError("Configuration for the migrator is not set.")
        if cfg['type'] != 'postgresql':
            raise ValueError(f"Unsupported database type for protocol connection: {cfg['type']}")
        if 'username' not in cfg or 'password' not in cfg or 'database' not in cfg:
            raise ValueError("Configuration for the migrator is incomplete. 'username', 'password', and 'database' are required.")
        self.connection = psycopg2.connect(
            dbname=cfg['database'],
            user=cfg['username'],
            password=cfg['password'],
            host=cfg.get('host', 'localhost'),
            port=cfg.get('port', 5432)
        )

    def execute_query(self, query, params=None):
        with self.connection.cursor() as cur:
            cur.execute(query, params) if params else cur.execute(query)
            self.connection.commit()

class MigratorTables:
    def __init__(self, logger, config_parser):
        self.logger = logger
        self.config_parser = config_parser
        protocol_db_type = self.config_parser.get_migrator_db_type()
        if protocol_db_type == 'postgresql':
            self.protocol_connection = ProtocolPostgresConnection(self.config_parser)
        else:
            raise ValueError(f"Unsupported database type for protocol table: {protocol_db_type}")
        self.protocol_connection.connect()
        self.protocol_schema = self.config_parser.get_migrator_schema()
        self.drop_table_sql = """DROP TABLE IF EXISTS "{protocol_schema}"."{table_name}";"""

    def create_all(self):
        self.create_protocol()
        self.create_table_for_main()
        self.create_table_for_user_defined_types()
        self.create_table_for_default_values()
        self.create_table_for_domains()
        self.create_table_for_new_objects()
        self.create_table_for_tables()
        self.create_table_for_data_sources()
        self.create_table_for_target_columns_alterations()
        self.create_table_for_data_migration()
        self.create_table_for_data_chunks()
        self.create_table_for_batches_stats()
        # self.create_table_for_pk_ranges()
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
            table_name TEXT,
            column_name TEXT,
            source_type TEXT,
            target_type TEXT,
            comment TEXT,
            inserted TIMESTAMP DEFAULT clock_timestamp()
        )
        """)
        self.config_parser.print_log_message('DEBUG3', f"Table data_types_substitution created in schema {self.protocol_schema}")

        # Insert data into the table
        for table_name, column_name, source_type, target_type, comment in self.config_parser.get_data_types_substitution():
            self.protocol_connection.execute_query(f"""
            INSERT INTO "{self.protocol_schema}".data_types_substitution
            (table_name, column_name, source_type, target_type, comment)
            VALUES (%s, %s, %s, %s, %s)
            """, (table_name, column_name, source_type, target_type, comment))
        self.config_parser.print_log_message('DEBUG3', f"Data inserted into table data_types_substitution in schema {self.protocol_schema}")

    def check_data_types_substitution(self, settings):
        """
        Check if replacement for the data type exists in the data_types_substitution table.
        Returns target_data_type or None.
        """
        table_name = settings.get('table_name', '')
        column_name = settings.get('column_name', '')
        check_type = settings['check_type']
        where_clauses = []
        params = []

        trimmed_table_name = table_name.strip()
        if trimmed_table_name == '':
            trimmed_table_name = None
        if trimmed_table_name is not None:
            where_clauses.append(f'''(
                lower(trim(%s)) = lower(trim(table_name))
                OR lower(trim(%s)) ~ lower(trim(table_name))
                OR lower(trim(%s)) ILIKE lower(trim(table_name))
                OR nullif(lower(trim(table_name)), '') IS NULL
            )''')
            params.extend([trimmed_table_name, trimmed_table_name, trimmed_table_name])

        trimmed_column_name = column_name.strip()
        if trimmed_column_name == '':
            trimmed_column_name = None
        if trimmed_column_name is not None:
            where_clauses.append(f'''(
                lower(trim(%s)) = lower(trim(column_name))
                OR lower(trim(%s)) ~ lower(trim(column_name))
                OR lower(trim(%s)) ILIKE lower(trim(column_name))
                OR nullif(lower(trim(column_name)), '') IS NULL
            )''')
            params.extend([trimmed_column_name, trimmed_column_name, trimmed_column_name])

        where_clauses.append("""(
            lower(trim(%s)) = lower(trim(source_type))
            OR lower(trim(%s)) ILIKE lower(trim(source_type))
            OR lower(trim(%s)) ~ lower(trim(source_type))
            )
        """)
        params.extend([check_type, check_type, check_type])

        where_sql = " AND ".join(where_clauses)
        query = f"""
        SELECT target_type
        FROM "{self.protocol_schema}".data_types_substitution
        WHERE {where_sql}
        LIMIT 1
        """
        self.config_parser.print_log_message('DEBUG2', f"check_data_types_substitution query: {query} - params: {params}")
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query, params)
        result = cursor.fetchone()
        cursor.close()
        self.config_parser.print_log_message('DEBUG2', f"check_data_types_substitution result: {result}")
        return result[0] if result else None

    def prepare_data_migration_limitation(self):
        # Drop table if exists
        self.protocol_connection.execute_query(f"""
        DROP TABLE IF EXISTS "{self.protocol_schema}".data_migration_limitation;
        """)
        # Create table if not exists
        self.protocol_connection.execute_query(f"""
        CREATE TABLE IF NOT EXISTS "{self.protocol_schema}".data_migration_limitation (
        source_table_name TEXT,
        where_limitation TEXT,
        use_when_column_present TEXT,
        inserted TIMESTAMP DEFAULT clock_timestamp()
        )
        """)
        self.config_parser.print_log_message('DEBUG3', f"Table data_migration_limitation created in schema {self.protocol_schema}")

        # Insert data into the table
        for source_table_name, where_limitation, use_when_column_present in self.config_parser.get_data_migration_limitation():
            self.protocol_connection.execute_query(f"""
            INSERT INTO "{self.protocol_schema}".data_migration_limitation
            (source_table_name, where_limitation, use_when_column_present)
            VALUES (%s, %s, %s)
            """, (source_table_name, where_limitation, use_when_column_present))
        self.config_parser.print_log_message('DEBUG3', f"Data inserted into table data_migration_limitation in schema {self.protocol_schema}")

    def get_records_data_migration_limitation(self, source_table_name):
        query = f"""
        SELECT where_limitation, use_when_column_present
        FROM "{self.protocol_schema}".data_migration_limitation
        WHERE trim('{source_table_name}') = trim(source_table_name)
        OR trim('{source_table_name}') ~ trim(source_table_name)
        """
        self.config_parser.print_log_message( 'DEBUG3', f"get_records_data_migration_limitation query: {query}")
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        if result:
            return result
        else:
            return None

    def prepare_remote_objects_substitution(self):
        # Drop table if exists
        self.protocol_connection.execute_query(f"""
        DROP TABLE IF EXISTS "{self.protocol_schema}".remote_objects_substitution;
        """)
        # Create table if not exists
        self.protocol_connection.execute_query(f"""
        CREATE TABLE IF NOT EXISTS "{self.protocol_schema}".remote_objects_substitution (
        source_object_name TEXT,
        target_object_name TEXT,
        inserted TIMESTAMP DEFAULT clock_timestamp()
        )
        """)
        self.config_parser.print_log_message('DEBUG3', f"Table remote_objects_substitution created in schema {self.protocol_schema}")

        # Insert data into the table
        for source_object_name, target_object_name in self.config_parser.get_remote_objects_substitution():
            self.protocol_connection.execute_query(f"""
            INSERT INTO "{self.protocol_schema}".remote_objects_substitution
            (source_object_name, target_object_name)
            VALUES (%s, %s)
            """, (source_object_name, target_object_name))
        self.config_parser.print_log_message('DEBUG3', f"Data inserted into table remote_objects_substitution in schema {self.protocol_schema}")

    def get_records_remote_objects_substitution(self):
        query = f"""
        SELECT source_object_name, target_object_name
        FROM "{self.protocol_schema}".remote_objects_substitution
        """
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result

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
        default_value_value TEXT,
        target_default_value TEXT,
        inserted TIMESTAMP DEFAULT clock_timestamp()
        )
        """)
        self.config_parser.print_log_message('DEBUG3', f"Table default_values_substitution created in schema {self.protocol_schema}")

        # Insert data into the table
        for column_name, source_column_data_type, default_value_value, target_default_value in self.config_parser.get_default_values_substitution():
            self.insert_default_values_substitution({
                'column_name': column_name,
                'source_column_data_type': source_column_data_type,
                'default_value_value': default_value_value,
                'target_default_value': target_default_value
            })
        self.config_parser.print_log_message('DEBUG3', f"Data inserted into table default_values_substitution in schema {self.protocol_schema}")

    def insert_default_values_substitution(self, settings):
        self.protocol_connection.execute_query(f"""
        INSERT INTO "{self.protocol_schema}".default_values_substitution
        (column_name, source_column_data_type, default_value_value, target_default_value)
        VALUES (%s, %s, %s, %s)
        """, (settings['column_name'], settings['source_column_data_type'], settings['default_value_value'], settings['target_default_value']))

    def check_default_values_substitution(self, settings):
        ## check_column_name, check_column_data_type, check_default_value
        check_column_name = settings['check_column_name']
        check_column_data_type = settings['check_column_data_type']
        check_default_value = settings['check_default_value']

        target_default_value = check_default_value

        try:
            query = f"""
                SELECT target_default_value
                FROM "{self.protocol_schema}".default_values_substitution
                WHERE trim(%s) ~ trim(column_name)
                AND trim(%s) ~ trim(source_column_data_type)
                AND trim(%s::TEXT) ~ trim(default_value_value::TEXT)
            """
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, (check_column_name, check_column_data_type, check_default_value))
            result = cursor.fetchone()

            if result:
                target_default_value = result[0]
            else:
                query = f"""
                    SELECT target_default_value
                    FROM "{self.protocol_schema}".default_values_substitution
                    WHERE upper(trim(%s)) LIKE upper(trim(column_name))
                    AND upper(trim(%s)) LIKE upper(trim(source_column_data_type))
                    AND upper(trim(%s::TEXT)) LIKE upper(trim(default_value_value::TEXT))
                """
                cursor = self.protocol_connection.connection.cursor()
                cursor.execute(query, (check_column_name, check_column_data_type, check_default_value))
                result = cursor.fetchone()
                self.config_parser.print_log_message( 'DEBUG3', f"1 check_default_values_substitution {check_column_name}, {check_column_data_type}, {check_default_value} query: {query} - {result}")
                if result:
                    target_default_value = result[0]
                else:
                    query = f"""
                        SELECT target_default_value
                        FROM "{self.protocol_schema}".default_values_substitution
                        WHERE upper(trim(column_name)) = ''
                        AND upper(trim(%s)) LIKE upper(trim(source_column_data_type))
                        AND upper(trim(%s::TEXT)) LIKE upper(trim(default_value_value::TEXT))
                    """
                    cursor.execute(query, (check_column_data_type, check_default_value))
                    result = cursor.fetchone()
                    self.config_parser.print_log_message( 'DEBUG3', f"2 check_default_values_substitution {check_column_name}, {check_column_data_type}, {check_default_value} query: {query} - {result}")
                    if result:
                        target_default_value = result[0]
                    else:
                        query = f"""
                            SELECT target_default_value
                            FROM "{self.protocol_schema}".default_values_substitution
                            WHERE upper(trim(column_name)) = ''
                            AND upper(trim(source_column_data_type)) = ''
                            AND upper(trim(%s::TEXT)) LIKE upper(trim(default_value_value::TEXT))
                        """
                        cursor.execute(query, (check_default_value,))
                        result = cursor.fetchone()
                        self.config_parser.print_log_message( 'DEBUG3', f"3 check_default_values_substitution {check_column_name}, {check_column_data_type}, {check_default_value} query: {query} - {result}")
                        if result:
                            target_default_value = result[0]
            cursor.close()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error checking default values substitution for {check_column_name}, {check_column_data_type}, {check_default_value}.")
            self.config_parser.print_log_message('ERROR', e)

        return target_default_value

    def create_protocol(self):
        query = f"""DROP SCHEMA IF EXISTS "{self.protocol_schema}" CASCADE"""
        self.protocol_connection.execute_query(query)

        query = f"""CREATE SCHEMA IF NOT EXISTS "{self.protocol_schema}" """
        self.protocol_connection.execute_query(query)

        table_name = self.config_parser.get_protocol_name()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        query = f"""
        CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}" (
            id SERIAL PRIMARY KEY,
            object_type TEXT,
            object_name TEXT,
            object_action TEXT,
            object_ddl TEXT,
            insertion_timestamp TIMESTAMP DEFAULT clock_timestamp(),
            execution_timestamp TIMESTAMP,
            execution_success BOOLEAN,
            execution_error_message TEXT,
            row_type TEXT default 'info',
            execution_results TEXT,
            object_protocol_id BIGINT
        );
        """
        self.protocol_connection.execute_query(query)
        self.config_parser.print_log_message('DEBUG3', f"Table {table_name} created in schema {self.protocol_schema}")

    def create_table_for_main(self):
        table_name = self.config_parser.get_protocol_name_main()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            task_name TEXT,
            subtask_name TEXT,
            task_started TIMESTAMP DEFAULT clock_timestamp(),
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG3', f"Table {table_name} created in schema {self.protocol_schema}")

    def insert_main(self, task_name, subtask_name):
        table_name = self.config_parser.get_protocol_name_main()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (task_name, subtask_name) VALUES ('{task_name}', '{subtask_name}')
            RETURNING *
        """
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            self.config_parser.print_log_message( 'DEBUG3', f"insert_main: returned row: {row}")
            main_row = self.decode_main_row(row)
            self.insert_protocol('main', task_name + ': ' + subtask_name, 'start', None, None, None, None, 'info', None, main_row['id'])
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"insert_main: Error inserting task {task_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def update_main_status(self, task_name, subtask_name, success, message):
        table_name = self.config_parser.get_protocol_name_main()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = clock_timestamp(),
            success = %s,
            message = %s
            WHERE task_name = %s
            AND subtask_name = %s
            RETURNING *
        """
        params = ('TRUE' if success else 'FALSE', message, task_name, subtask_name)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
            self.config_parser.print_log_message( 'DEBUG3', f"update_main_status: returned row: {row}")
            if row:
                main_row = self.decode_main_row(row)
                self.update_protocol('main', main_row['id'], success, message, None)
            else:
                self.config_parser.print_log_message('ERROR', f"update_main_status: Error updating status for task {task_name} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"update_main_status: Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"update_main_status: Error updating status for task {task_name} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"update_main_status: Query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def decode_main_row(self, row):
        return {
            'id': row[0],
            'task_name': row[1],
            'subtask_name': row[2],
            'task_started': row[3],
            'task_completed': row[4],
            'success': row[5],
            'message': row[6]
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
            type_comment TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG3', f"Table {table_name} created in schema {self.protocol_schema}")

    def insert_user_defined_type(self, settings):
        ## source_schema_name, source_type_name, source_type_sql, target_schema_name, target_type_name, target_type_sql, type_comment
        source_schema_name = settings['source_schema_name']
        source_type_name = settings['source_type_name']
        source_type_sql = settings['source_type_sql']
        target_schema_name = settings['target_schema_name']
        target_type_name = settings['target_type_name']
        target_type_sql = settings['target_type_sql']
        type_comment = settings['type_comment']

        table_name = self.config_parser.get_protocol_name_user_defined_types()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema_name, source_type_name, source_type_sql, target_schema_name, target_type_name, target_type_sql, type_comment)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema_name, source_type_name, source_type_sql, target_schema_name, target_type_name, target_type_sql, type_comment)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            self.config_parser.print_log_message( 'DEBUG3', f"insert_user_defined_type: returned row: {row}")
            user_defined_type_row = self.decode_user_defined_type_row(row)
            self.insert_protocol('user_defined_type', target_type_name, 'create', target_type_sql, None, None, None, 'info', None, user_defined_type_row['id'])
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"insert_user_defined_type: Error inserting user defined type {target_type_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def update_user_defined_type_status(self, row_id, success, message):
        table_name = self.config_parser.get_protocol_name_user_defined_types()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = clock_timestamp(),
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

            if row:
                user_defined_type_row = self.decode_user_defined_type_row(row)
                self.config_parser.print_log_message( 'DEBUG3', f"update_user_defined_type_status: returned row: {user_defined_type_row}")
                self.update_protocol('user_defined_type', user_defined_type_row['id'], success, message, None)
            else:
                self.config_parser.print_log_message('ERROR', f"update_user_defined_type_status: Error updating status for user defined type {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"update_user_defined_type_status: Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"update_user_defined_type_status: Error updating status for user defined type {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"update_user_defined_type_status: Query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_all_user_defined_types(self):
        table_name = self.config_parser.get_protocol_name_user_defined_types()
        query = f"""SELECT * FROM "{self.protocol_schema}"."{table_name}" ORDER BY id"""
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        self.config_parser.print_log_message('DEBUG3', f"fetch_all_user_defined_types: returned rows: {len(rows)}")
        return rows

    def decode_user_defined_type_row(self, row):
        return {
            'id': row[0],
            'source_schema_name': row[1],
            'source_type_name': row[2],
            'source_type_sql': row[3],
            'target_schema_name': row[4],
            'target_type_name': row[5],
            'target_type_sql': row[6],
            'type_comment': row[7]
        }

    def create_table_for_domains(self):
        table_name = self.config_parser.get_protocol_name_domains()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema_name TEXT,
            source_domain_name TEXT,
            source_domain_sql TEXT,
            source_domain_check_sql TEXT,
            target_schema_name TEXT,
            target_domain_name TEXT,
            target_domain_sql TEXT,
            migrated_as TEXT,
            domain_comment TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG3', f"create_table_for_domains: Table {table_name} created in schema {self.protocol_schema}")

    def insert_domain(self, settings):
        ## source_schema_name, source_domain_name, source_domain_sql, target_schema_name, target_domain_name, target_domain_sql, domain_comment
        source_schema_name = settings['source_schema_name']
        source_domain_name = settings['source_domain_name']
        source_domain_sql = settings['source_domain_sql']
        source_domain_check_sql = settings['source_domain_check_sql'] if 'source_domain_check_sql' in settings else ''
        target_schema_name = settings['target_schema_name']
        target_domain_name = settings['target_domain_name']
        target_domain_sql = settings['target_domain_sql']
        migrated_as = settings['migrated_as'] if 'migrated_as' in settings else ''
        domain_comment = settings['domain_comment']

        table_name = self.config_parser.get_protocol_name_domains()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema_name, source_domain_name, source_domain_sql, source_domain_check_sql,
            target_schema_name, target_domain_name, target_domain_sql, migrated_as, domain_comment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema_name, source_domain_name, source_domain_sql, source_domain_check_sql,
                  target_schema_name, target_domain_name, target_domain_sql, migrated_as, domain_comment)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            domain_row = self.decode_user_defined_type_row(row)
            self.config_parser.print_log_message( 'DEBUG3', f"insert_domain: returned row: {domain_row}")
            self.insert_protocol('domain', target_domain_name, 'create', target_domain_sql, None, None, None, 'info', None, domain_row['id'])
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"insert_domain: Error inserting domain {target_domain_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def update_domain_status(self, row_id, success, message):
        table_name = self.config_parser.get_protocol_name_domains()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = clock_timestamp(),
            success = %s,
            message = %s
            WHERE id = %s
            RETURNING *
        """
        self.config_parser.print_log_message( 'DEBUG3', f"Query: {query}")
        params = ('TRUE' if success else 'FALSE', message, row_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            if row:
                domain_row = self.decode_domain_row(row)
                self.config_parser.print_log_message( 'DEBUG3', f"update_domain_status: returned row: {domain_row}")
                self.update_protocol('domain', domain_row['id'], success, message, None)
            else:
                self.config_parser.print_log_message('ERROR', f"update_domain_status: Error updating status for domain {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"update_domain_status: Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"update_domain_status: Error updating status for domain {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"update_domain_status: Query: {query}")
            self.config_parser.print_log_message('ERROR', f"update_domain_status: {e}")
            raise

    def fetch_all_domains(self, domain_owner=None, domain_name=None):
        table_name = self.config_parser.get_protocol_name_domains()
        where_clause = ""
        if domain_owner:
            where_clause += f" WHERE source_schema_name = '{domain_owner}'"
        if domain_name:
            if where_clause:
                where_clause += f" AND source_domain_name = '{domain_name}'"
            else:
                where_clause += f" WHERE source_domain_name = '{domain_name}'"
        query = f"""SELECT * FROM "{self.protocol_schema}"."{table_name}" {where_clause} ORDER BY id"""
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def get_domain_details(self, domain_owner=None, domain_name=None):
        domain_row = self.fetch_all_domains(domain_owner, domain_name)
        result = self.decode_domain_row(domain_row[0]) if domain_row else {}
        return result

    def decode_domain_row(self, row):
        return {
            'id': row[0],
            'source_schema_name': row[1],
            'source_domain_name': row[2],
            'source_domain_sql': row[3],
            'source_domain_check_sql': row[4],
            'target_schema_name': row[5],
            'target_domain_name': row[6],
            'target_domain_sql': row[7],
            'migrated_as': row[8],
            'domain_comment': row[9]
        }

    def create_table_for_default_values(self):
        table_name = self.config_parser.get_protocol_name_default_values()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            default_value_schema TEXT,
            default_value_name TEXT,
            default_value_sql TEXT,
            extracted_default_value TEXT,
            default_value_data_type TEXT,
            default_value_comment TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG3', f"create_table_for_default_values: Table {table_name} created in schema {self.protocol_schema}")

    def insert_default_value(self, settings):
        default_value_schema = settings['default_value_schema']
        default_value_name = settings['default_value_name']
        default_value_sql = settings['default_value_sql']
        extracted_default_value = settings['extracted_default_value']
        default_value_data_type = settings['default_value_data_type']

        table_name = self.config_parser.get_protocol_name_default_values()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (default_value_schema, default_value_name, default_value_sql,
            extracted_default_value, default_value_data_type)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (default_value_schema, default_value_name, default_value_sql,
                  extracted_default_value, default_value_data_type)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            default_value_row = self.decode_user_defined_type_row(row)
            self.config_parser.print_log_message( 'DEBUG3', f"insert_default_value: returned row: {default_value_row}")
            self.insert_protocol('default_value', default_value_name, 'create', None, None, None, None, 'info', None, default_value_row['id'])
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"insert_default_value: Error inserting default value {default_value_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def update_default_value_status(self, row_id, success, message):
        table_name = self.config_parser.get_protocol_name_default_values()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = clock_timestamp(),
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

            if row:
                default_value_row = self.decode_default_value_row(row)
                self.config_parser.print_log_message('DEBUG3', f"update_default_value_status: returned row: {default_value_row}")
                self.update_protocol('default_value', default_value_row['id'], success, message, None)
            else:
                self.config_parser.print_log_message('ERROR', f"update_default_value_status: Error updating status for default value {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"update_default_value_status: Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"update_default_value_status: Error updating status for default value {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"update_default_value_status: Query: {query}")
            self.config_parser.print_log_message('ERROR', f"update_default_value_status: Exception: {e}")
            raise

    def decode_default_value_row(self, row):
        return {
            'id': row[0],
            'default_value_schema': row[1],
            'default_value_name': row[2],
            'default_value_sql': row[3],
            'extracted_default_value': row[4],
            'default_value_data_type': row[5],
        }

    def get_default_value_details(self, default_value_name):
        table_name = self.config_parser.get_protocol_name_default_values()
        query = f"""SELECT * FROM "{self.protocol_schema}"."{table_name}" WHERE default_value_name = '{default_value_name}'"""
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        cursor.close()
        return self.decode_default_value_row(row) if row else {}

    def create_table_for_target_columns_alterations(self):
        table_name = self.config_parser.get_protocol_name_target_columns_alterations()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            target_schema TEXT,
            target_table TEXT,
            target_column TEXT,
            reason TEXT,
            original_data_type TEXT,
            altered_data_type TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG3', f"create_table_for_target_columns_alterations: Table {table_name} created in schema {self.protocol_schema}")

    def insert_target_column_alteration(self, settings):
        ## target_schema, target_table, target_column, original_data_type, altered_data_type
        target_schema = settings['target_schema']
        target_table = settings['target_table']
        target_column = settings['target_column']
        reason = settings['reason'] if 'reason' in settings else ''
        original_data_type = settings['original_data_type']
        altered_data_type = settings['altered_data_type']

        table_name = self.config_parser.get_protocol_name_target_columns_alterations()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (target_schema, target_table, target_column, reason, original_data_type, altered_data_type)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (target_schema, target_table, target_column, reason, original_data_type, altered_data_type)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            target_column_alteration_row = self.decode_target_column_alteration_row(row)
            self.config_parser.print_log_message( 'DEBUG3', f"insert_target_column_alteration: returned row: {target_column_alteration_row}")
            self.insert_protocol('target_column_alteration', target_table + '.' + target_column, 'alter', None, None, None, None, 'info', None, target_column_alteration_row['id'])
            return target_column_alteration_row['id']
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"insert_target_column_alteration: Error inserting target column alteration {target_table}.{target_column} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"insert_target_column_alteration: Exception: {e}")
            raise

    def decode_target_column_alteration_row(self, row):
        return {
            'id': row[0],
            'target_schema': row[1],
            'target_table': row[2],
            'target_column': row[3],
            'original_data_type': row[4],
            'altered_data_type': row[5]
        }

    def fk_find_dependent_columns_to_alter(self, settings):
        """
        Find the dependent column to alter in the target table based on the foreign key constraints.
        Yields each matching row as a dict.
        """
        table_name_constraints = self.config_parser.get_protocol_name_constraints()
        table_name_target_columns_alterations = self.config_parser.get_protocol_name_target_columns_alterations()
        query = f"""SELECT
                        replace(c.constraint_columns,'"','') AS target_column,
                        a.reason,
                        a.original_data_type,
                        a.altered_data_type
                    FROM "{self.protocol_schema}".{table_name_constraints} c
                    JOIN "{self.protocol_schema}".{table_name_target_columns_alterations} a
                    ON c.referenced_table_name = a.target_table
                    AND replace(c.referenced_columns,'"','') = a.target_column
                    WHERE c.constraint_type = 'FOREIGN KEY'
                    AND c.target_schema = '{settings['target_schema']}'
                    AND c.target_table = '{settings['target_table']}'
                """
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        for row in cursor:
            yield {
                'target_column': row[0],
                'reason': row[1],
                'original_data_type': row[2],
                'altered_data_type': row[3]
            }
        cursor.close()

    def create_table_for_data_migration(self):
        table_name = self.config_parser.get_protocol_name_data_migration()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema TEXT,
            source_table TEXT,
            source_table_id INTEGER,
            source_table_rows INTEGER,
            worker_id TEXT,
            target_schema TEXT,
            target_table TEXT,
            target_table_rows INTEGER,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP DEFAULT clock_timestamp(),
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT,
            batch_count INTEGER DEFAULT 0,
            shortest_batch_seconds FLOAT DEFAULT 0,
            longest_batch_seconds FLOAT DEFAULT 0,
            average_batch_seconds FLOAT DEFAULT 0
            )
        """)
        self.config_parser.print_log_message('DEBUG3', f"create_table_for_data_migration: Table {table_name} created in schema {self.protocol_schema}")
        self.protocol_connection.execute_query(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_data_migration_unique1
            ON "{self.protocol_schema}"."{self.config_parser.get_protocol_name_data_migration()}"
            (source_schema, source_table, target_schema, target_table)
        """)
        self.config_parser.print_log_message('DEBUG3', f"create_table_for_data_migration: Unique index created for table {table_name}.")

    def create_table_for_batches_stats(self):
        table_name = self.config_parser.get_protocol_name_batches_stats()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema TEXT,
            source_table TEXT,
            source_table_id INTEGER,
            chunk_number INTEGER,
            batch_number INTEGER,
            batch_start TIMESTAMP,
            batch_end TIMESTAMP,
            batch_rows INTEGER,
            batch_seconds FLOAT,
            reading_seconds FLOAT,
            transforming_seconds FLOAT,
            writing_seconds FLOAT,
            inserted_at TIMESTAMP DEFAULT clock_timestamp(),
            worker_id TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG3', f"create_table_for_batches_stats: Table {table_name} created in schema {self.protocol_schema}.")

    def create_table_for_data_chunks(self):
        try:
            table_name = self.config_parser.get_protocol_name_data_chunks()
            self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
            self.protocol_connection.execute_query(f"""
                CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
                (
                    id SERIAL PRIMARY KEY,
                    worker_id TEXT,
                    source_table_id INTEGER,
                    source_schema TEXT,
                    source_table TEXT,
                    target_schema TEXT,
                    target_table TEXT,
                    source_table_rows BIGINT,
                    target_table_rows BIGINT,
                    chunk_number INTEGER,
                    chunk_size BIGINT,
                    migration_limitation TEXT,
                    chunk_start BIGINT,
                    chunk_end BIGINT,
                    order_by_clause TEXT,
                    inserted_rows BIGINT,
                    batch_size BIGINT,
                    total_batches INTEGER,
                    task_started TIMESTAMP,
                    task_completed TIMESTAMP
                )
            """)
            self.config_parser.print_log_message('DEBUG3', f"create_table_for_data_chunks: Table {table_name} created successfully.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"create_table_for_data_chunks: Error creating table {table_name}.")
            self.config_parser.print_log_message('ERROR', f"create_table_for_data_chunks: Exception: {e}")
            raise

    def insert_data_chunk(self, settings):
        ## worker_id, source_table_id, source_schema, source_table, target_schema, target_table,
        ## source_table_rows, target_table_rows, chunk_number, chunk_size, migration_limitation,
        ## chunk_start, chunk_end, inserted_rows, batch_size, total_batches
        worker_id = settings['worker_id']
        source_table_id = settings['source_table_id']
        source_schema = settings['source_schema']
        source_table = settings['source_table']
        target_schema = settings['target_schema']
        target_table = settings['target_table']
        source_table_rows = settings['source_table_rows']
        target_table_rows = settings['target_table_rows']
        chunk_number = settings['chunk_number']
        chunk_size = settings['chunk_size']
        migration_limitation = settings.get('migration_limitation', '')
        chunk_start = settings.get('chunk_start', 0)
        chunk_end = settings.get('chunk_end', 0)
        inserted_rows = settings.get('inserted_rows', 0)
        batch_size = settings.get('batch_size', 0)
        total_batches = settings.get('total_batches', 0)
        task_started = settings.get('task_started', None)
        task_completed = settings.get('task_completed', None)
        order_by_clause = settings.get('order_by_clause', '')

        table_name = self.config_parser.get_protocol_name_data_chunks()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (worker_id, source_table_id, source_schema, source_table,
            target_schema, target_table, source_table_rows, target_table_rows,
            chunk_number, chunk_size, migration_limitation,
            chunk_start, chunk_end, order_by_clause, inserted_rows, batch_size, total_batches,
            task_started, task_completed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (str(worker_id), source_table_id, source_schema,
                  source_table, target_schema,
                  target_table,
                  source_table_rows,
                  target_table_rows,
                  chunk_number,
                  chunk_size,
                  migration_limitation,
                  chunk_start,
                  chunk_end,
                  order_by_clause,
                  inserted_rows,
                  batch_size,
                  total_batches,
                  task_started,
                  task_completed)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            data_chunk_row = self.decode_data_chunk_row(row)
            self.config_parser.print_log_message('DEBUG3', f"insert_data_chunk: Returned row: {data_chunk_row}")
            self.insert_protocol('data_chunk', f"{target_table}.{chunk_number}", 'create', None, None, None, None, 'info', None, data_chunk_row['id'])
            return data_chunk_row['id']
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"insert_data_chunk: Error inserting data chunk for {target_table} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"insert_data_chunk: Exception: {e}")
            raise

    def decode_data_chunk_row(self, row):
        return {
            'id': row[0],
            'worker_id': row[1],
            'source_table_id': row[2],
            'source_schema': row[3],
            'source_table': row[4],
            'target_schema': row[5],
            'target_table': row[6],
            'source_table_rows': row[7],
            'target_table_rows': row[8],
            'chunk_number': row[9],
            'chunk_size': row[10],
            'migration_limitation': row[11],
            'chunk_start': row[12],
            'chunk_end': row[13],
            'order_by_clause': row[14],
            'inserted_rows': row[15],
            'batch_size': row[16],
            'total_batches': row[17],
            'task_started': row[18],
            'task_completed': row[19]
        }

    def insert_batches_stats(self, settings):
        ## source_schema, source_table, source_table_id, batch_number, batch_start, batch_end, batch_rows, batch_seconds
        source_schema = settings['source_schema']
        source_table = settings['source_table']
        source_table_id = settings['source_table_id']
        chunk_number = settings['chunk_number']
        batch_number = settings['batch_number']
        batch_start = settings['batch_start']
        batch_end = settings['batch_end']
        batch_rows = settings['batch_rows']
        batch_seconds = settings['batch_seconds']
        reading_seconds = settings.get('reading_seconds', 0)
        transforming_seconds = settings.get('transforming_seconds', 0)
        writing_seconds = settings.get('writing_seconds', 0)
        worker_id = settings['worker_id']

        table_name = self.config_parser.get_protocol_name_batches_stats()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema, source_table, source_table_id, chunk_number, batch_number,
            batch_start, batch_end, batch_rows, batch_seconds, worker_id,
            reading_seconds, transforming_seconds, writing_seconds)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema, source_table, source_table_id, chunk_number, batch_number,
                  batch_start, batch_end, batch_rows, batch_seconds, str(worker_id),
                  reading_seconds, transforming_seconds, writing_seconds)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            self.protocol_connection.connection.commit()  # Commit the transaction
            cursor.close()

            self.config_parser.print_log_message( 'DEBUG3', f"insert_batches_stats: Returned row: {row}")
            return row[0]  # Return the ID of the inserted row
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"insert_batches_stats: Error inserting batches stats for {source_table} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"insert_batches_stats: Exception: {e}")
            raise

    def insert_data_migration(self, settings):
        ## source_schema, source_table, source_table_id, source_table_rows, worker_id, target_schema, target_table, target_table_rows
        source_schema = settings['source_schema']
        source_table = settings['source_table']
        source_table_id = settings['source_table_id']
        source_table_rows = settings['source_table_rows']
        worker_id = settings['worker_id']
        target_schema = settings['target_schema']
        target_table = settings['target_table']
        target_table_rows = settings['target_table_rows']

        table_name = self.config_parser.get_protocol_name_data_migration()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema, source_table, source_table_id, source_table_rows, worker_id, target_schema, target_table, target_table_rows)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_schema, source_table, target_schema, target_table)
            DO UPDATE SET source_table_rows = EXCLUDED.source_table_rows,
            target_table_rows = EXCLUDED.target_table_rows,
            task_created = clock_timestamp(),
            worker_id = EXCLUDED.worker_id,
            success = NULL,
            message = NULL
            RETURNING *
        """
        params = (source_schema, source_table, source_table_id, source_table_rows, str(worker_id), target_schema, target_table, target_table_rows)
        self.config_parser.print_log_message('DEBUG3', f"Inserting data migration record for table {target_table} with params: {params}")
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            data_migration_row = self.decode_data_migration_row(row)
            self.config_parser.print_log_message('DEBUG3', f"insert_data_migration: Returned row: {data_migration_row}")
            self.insert_protocol('data_migration', target_table, 'create', None, None, None, None, 'info', None, data_migration_row['id'])
            return data_migration_row['id']
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"insert_data_migration: Error inserting data migration {target_table} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"insert_data_migration: Exception: {e}")
            raise

    def update_data_migration_started(self, row_id):
        table_name = self.config_parser.get_protocol_name_data_migration()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_started = clock_timestamp()
            WHERE id = %s
            RETURNING *
        """
        params = (row_id,)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            self.config_parser.print_log_message('DEBUG3', f"update_data_migration_started: Returned row: {row}")
            # if row:
            #     data_migration_row = self.decode_data_migration_row(row)
            #     self.update_protocol('data_migration', data_migration_row['id'], None, None, None)
            # else:
            #     self.config_parser.print_log_message('ERROR', f"Error updating started status for data migration {row_id} in {table_name}.")
            #     self.config_parser.print_log_message('ERROR', f"Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"update_data_migration_started: Error updating started status for data migration {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"update_data_migration_started: Query: {query}")
            self.config_parser.print_log_message('ERROR', f"update_data_migration_started: Exception: {e}")
            raise

    def update_data_migration_status(self, settings):
        row_id = settings['row_id']
        success = settings['success']
        message = settings['message']
        target_table_rows = settings['target_table_rows']
        batch_count = settings.get('batch_count', 0)
        shortest_batch_seconds = settings.get('shortest_batch_seconds', 0)
        longest_batch_seconds = settings.get('longest_batch_seconds', 0)
        average_batch_seconds = settings.get('average_batch_seconds', 0)
        table_name = self.config_parser.get_protocol_name_data_migration()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = clock_timestamp(),
            success = %s,
            message = %s,
            target_table_rows = %s,
            batch_count = %s,
            shortest_batch_seconds = %s,
            longest_batch_seconds = %s,
            average_batch_seconds = %s
            WHERE id = %s
            RETURNING *
        """
        params = ('TRUE' if success else 'FALSE',
                  message, target_table_rows,
                  batch_count, shortest_batch_seconds,
                  longest_batch_seconds, average_batch_seconds,
                  row_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            if row:
                data_migration_row = self.decode_data_migration_row(row)
                self.config_parser.print_log_message('DEBUG3', f"update_data_migration_status: Returned row: {data_migration_row}")
                self.update_protocol('data_migration', data_migration_row['id'], success, message, 'source rows: ' + str(data_migration_row['source_table_rows']) + ', target rows: ' + str(target_table_rows))
            else:
                self.config_parser.print_log_message('ERROR', f"update_data_migration_status: Error updating status for data migration {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"update_data_migration_status: Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"update_data_migration_status: Error updating status for data migration {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"update_data_migration_status: Query: {query}")
            self.config_parser.print_log_message('ERROR', f"update_data_migration_status: Exception: {e}")
            raise

    def update_data_migration_rows(self, settings):
        row_id = settings['row_id']
        source_table_rows = settings['source_table_rows']
        target_table_rows = settings['target_table_rows']
        table_name = self.config_parser.get_protocol_name_data_migration()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET source_table_rows = %s,
            target_table_rows = %s
            WHERE id = %s
            RETURNING *
        """
        params = (source_table_rows, target_table_rows, row_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            if row:
                data_migration_row = self.decode_data_migration_row(row)
                self.config_parser.print_log_message('DEBUG3', f"update_data_migration_rows: Returned row: {data_migration_row}")
                self.update_protocol('data_migration', data_migration_row['id'], None, None, None)
            else:
                self.config_parser.print_log_message('ERROR', f"update_data_migration_rows: Error updating rows for data migration {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"update_data_migration_rows: Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"update_data_migration_rows: Error updating rows for data migration {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"update_data_migration_rows: Query: {query}")
            self.config_parser.print_log_message('ERROR', f"update_data_migration_rows: Exception: {e}")
            raise

    def decode_data_migration_row(self, row):
        return {
            'id': row[0],
            'source_schema': row[1],
            'source_table': row[2],
            'source_table_id': row[3],
            'source_table_rows': row[4],
            'worker_id': row[5],
            'target_schema': row[6],
            'target_table': row[7],
            'target_table_rows': row[8],
            'task_created': row[9],
            'task_started': row[10],
            'task_completed': row[11],
            'success': row[12],
            'message': row[13],
            'batch_count': row[14],
            'shortest_batch_seconds': row[15],
            'longest_batch_seconds': row[16],
            'average_batch_seconds': row[17],
        }

    def create_table_for_pk_ranges(self):
        table_name = self.config_parser.get_protocol_name_pk_ranges()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema TEXT,
            source_table TEXT,
            source_table_id INTEGER,
            worker_id TEXT,
            pk_columns TEXT,
            batch_start TEXT,
            batch_end TEXT,
            row_count BIGINT
            )
        """)
        self.config_parser.print_log_message('DEBUG', f"Created table {table_name} for PK ranges.")

    def insert_pk_ranges(self, values):
        table_name = self.config_parser.get_protocol_name_pk_ranges()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema, source_table, source_table_id, worker_id, pk_columns, batch_start, batch_end, row_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (values['source_schema'], values['source_table'], values['source_table_id'],
                  str(values['worker_id']), values['pk_columns'],
                  values['batch_start'], values['batch_end'], values['row_count'])
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            data_migration_row = self.decode_pk_ranges_row(row)
            self.config_parser.print_log_message('DEBUG3', f"insert_pk_ranges: Returned row: {data_migration_row}")
            self.insert_protocol('data_migration', values['source_table'], 'pk_range',
                                 f'''PK range: {values['batch_start']} - {values['batch_end']} / {values['row_count']}''',
                                 None, True, None, 'info', None, data_migration_row['id'])
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"insert_pk_ranges: Error inserting PK ranges {values['source_table']} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"insert_pk_ranges: Exception: {e}")
            raise

    def fetch_all_pk_ranges(self, worker_id):
        table_name = self.config_parser.get_protocol_name_pk_ranges()
        query = f"""SELECT batch_start, batch_end, row_count FROM "{self.protocol_schema}"."{table_name}" WHERE worker_id = '{worker_id}' ORDER BY id"""
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def decode_pk_ranges_row(self, row):
        return {
            'id': row[0],
            'source_schema': row[1],
            'source_table': row[2],
            'source_table_id': row[3],
            'worker_id': row[4],
            'batch_start': row[5],
            'batch_end': row[6],
            'row_count': row[7]
        }

    def create_table_for_new_objects(self):
        table_name = self.config_parser.get_protocol_name_new_objects()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            object_comment TEXT,
            object_type TEXT,
            object_sql TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG', f"Created protocol table {table_name} for new objects.")

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
            source_table_description TEXT,
            target_schema TEXT,
            target_table TEXT,
            target_columns TEXT,
            target_table_sql TEXT,
            table_comment TEXT,
            partitioned BOOLEAN,
            partitioned_by TEXT,
            partitioning_columns TEXT,
            create_partitions_sql TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG', f"Created protocol table {table_name} for tables.")

    def create_table_for_data_sources(self):
        table_name = self.config_parser.get_protocol_name_data_sources()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema TEXT,
            source_table TEXT,
            source_table_id INTEGER,
            lob_columns TEXT,
            file_name TEXT,
            file_size BIGINT,
            file_lines INTEGER,
            file_found BOOLEAN,
            converted_file_name TEXT,
            format_options TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG', f"Created protocol table {table_name} for data sources.")

    def insert_data_source(self, settings):
        ## source_schema, source_table, source_table_id, clob_columns, blob_columns, file_name, format_options
        source_schema = settings['source_schema']
        source_table = settings['source_table']
        source_table_id = settings['source_table_id']
        lob_columns = settings.get('lob_columns', '')
        file_name = settings.get('file_name', '')
        file_size = settings.get('file_size', 0)
        file_lines = settings.get('file_lines', 0)
        file_found = settings.get('file_found', False)
        converted_file_name = settings.get('converted_file_name', '')
        format_options = json.dumps(settings.get('format_options', ''))

        table_name = self.config_parser.get_protocol_name_data_sources()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema, source_table, source_table_id, lob_columns,
            file_name, file_size, file_lines, file_found, converted_file_name, format_options)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema, source_table, source_table_id,
                  lob_columns, file_name, file_size, file_lines, file_found, converted_file_name, format_options)
        self.config_parser.print_log_message('DEBUG3', f"insert_data_source: Query: {query} / Params: {params}")

        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            data_source_row = self.decode_data_source_row(row)
            self.config_parser.print_log_message('DEBUG3', f"insert_data_source: Returned row: {data_source_row}")
            self.insert_protocol('data_source', f"{source_table} ({file_name})", 'create', None, None, None, None, 'info', None, data_source_row['id'])
            return data_source_row['id']
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"insert_data_source: Error inserting data source {source_table} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"insert_data_source: Exception: {e}")
            raise

    def get_data_sources(self, source_schema, source_table):
        table_name = self.config_parser.get_protocol_name_data_sources()
        query = f"""
            SELECT * FROM "{self.protocol_schema}"."{table_name}"
            WHERE source_schema = %s AND source_table = %s
        """
        params = (source_schema, source_table)
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query, params)
        row = cursor.fetchone()
        cursor.close()
        if row:
            return self.decode_data_source_row(row)
        return None

    def decode_data_source_row(self, row):
        return {
            'id': row[0],
            'source_schema': row[1],
            'source_table': row[2],
            'source_table_id': row[3],
            'lob_columns': row[4],
            'file_name': row[5],
            'file_size': row[6],
            'file_lines': row[7],
            'file_found': row[8],
            'converted_file_name': row[9],
            'format_options': json.loads(row[10]),
            'task_created': row[11],
            'task_started': row[12],
            'task_completed': row[13],
            'success': row[14],
            'message': row[15]
        }

    def update_status_data_source(self, row_id, success, message):
        table_name = self.config_parser.get_protocol_name_data_sources()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = clock_timestamp(),
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

            if row:
                data_source_row = self.decode_data_source_row(row)
                self.config_parser.print_log_message('DEBUG3', f"update_status_data_source: Returned row: {data_source_row}")
                self.update_protocol('data_source', data_source_row['id'], success, message, None)
            else:
                self.config_parser.print_log_message('ERROR', f"update_status_data_source: Error updating status for data source {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"update_status_data_source: Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"update_status_data_source: Error updating status for data source {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"update_status_data_source: Query: {query}")
            self.config_parser.print_log_message('ERROR', f"update_status_data_source: Exception: {e}")
            raise

    def create_table_for_indexes(self):
        table_name = self.config_parser.get_protocol_name_indexes()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema TEXT,
            source_table TEXT,
            source_table_id INTEGER,
            index_owner TEXT,
            index_name TEXT,
            index_type VARCHAR(30),
            target_schema TEXT,
            target_table TEXT,
            index_sql TEXT,
            index_columns TEXT,
            index_comment TEXT,
            is_function_based BOOLEAN DEFAULT FALSE,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG', f"Created protocol table {table_name} for indexes.")

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
            funcproc_comment TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG', f"Created protocol table {table_name} for functions/procedures.")

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
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG', f"Created protocol table {table_name} for sequences.")

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
            trigger_comment TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG', f"Created protocol table {table_name} for triggers.")

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
            view_comment TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG', f"Created protocol table {table_name} for views.")

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
            'source_table_description': row[5],
            'target_schema': row[6],
            'target_table': row[7],
            'target_columns': json.loads(row[8]),
            'target_table_sql': row[9],
            'table_comment': row[10],
            'partitioned': row[11],
            'partitioned_by': row[12],
            'partitioning_columns': row[13],
            'create_partitions_sql': row[14],
        }

    def decode_index_row(self, row):
        return {
            'id': row[0],
            'source_schema': row[1],
            'source_table': row[2],
            'source_table_id': row[3],
            'index_owner': row[4],
            'index_name': row[5],
            'index_type': row[6],
            'target_schema': row[7],
            'target_table': row[8],
            'index_sql': row[9],
            'index_columns': row[10],
            'index_comment': row[11],
            'is_function_based': row[12]
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
            'target_funcproc_sql': row[7],
            'funcproc_comment': row[8]
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
            'trigger_target_sql': row[13],
            'trigger_comment': row[14]
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
            'target_view_sql': row[7],
            'view_comment': row[8]
        }

    def insert_protocol(self, object_type, object_name, object_action, object_ddl, execution_timestamp, execution_success, execution_error_message, row_type, execution_results, object_protocol_id):
        table_name = self.config_parser.get_protocol_name()
        query = f"""
        INSERT INTO "{self.protocol_schema}"."{table_name}"
        (object_type, object_name, object_action, object_ddl, execution_timestamp, execution_success, execution_error_message, row_type, execution_results, object_protocol_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (object_type, object_name, object_action, object_ddl, execution_timestamp, execution_success, execution_error_message, row_type, execution_results, object_protocol_id)
        self.config_parser.print_log_message('DEBUG', f"insert_protocol: Executing query with params: {params}")
        try:
            self.protocol_connection.execute_query(query, params)
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"insert_protocol: Error inserting info {object_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"insert_protocol: Exception: {e}")
            raise

    def update_protocol(self, object_type, object_protocol_id, execution_success, execution_error_message, execution_results):
        table_name = self.config_parser.get_protocol_name()
        query = f"""
        UPDATE "{self.protocol_schema}"."{table_name}"
        SET execution_success = %s,
        execution_error_message = %s,
        execution_results = %s,
        execution_timestamp = clock_timestamp()
        WHERE object_protocol_id = %s
        AND object_type = %s
        """
        params = ('TRUE' if execution_success else 'FALSE', execution_error_message, execution_results, object_protocol_id, object_type)
        self.config_parser.print_log_message('DEBUG', f"update_protocol: Executing query with params: {params}")
        try:
            self.protocol_connection.execute_query(query, params)
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"update_protocol: Error updating info {object_protocol_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"update_protocol: Exception: {e}")
            raise

    def insert_tables(self, settings):
        source_schema = settings['source_schema']
        source_table = settings['source_table']
        source_table_id = settings['source_table_id']
        source_columns = settings['source_columns']
        source_table_description = settings['source_table_description']
        target_schema = settings['target_schema']
        target_table = settings['target_table']
        target_columns = settings['target_columns']
        target_table_sql = settings['target_table_sql']
        table_comment = settings['table_comment']
        partitioned = settings['partitioned']
        partitioned_by = settings['partitioned_by']
        partitioning_columns = settings['partitioning_columns']
        create_partitions_sql = settings['create_partitions_sql']

        table_name = self.config_parser.get_protocol_name_tables()
        source_columns_str = json.dumps(source_columns)
        target_columns_str = json.dumps(target_columns)
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema, source_table, source_table_id, source_columns, source_table_description,
            target_schema, target_table, target_columns, target_table_sql, table_comment,
            partitioned, partitioned_by, partitioning_columns, create_partitions_sql)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema, source_table, source_table_id, source_columns_str, source_table_description,
                  target_schema, target_table, target_columns_str, target_table_sql, table_comment,
                  partitioned, partitioned_by, partitioning_columns, create_partitions_sql)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            table_row = self.decode_table_row(row)
            self.config_parser.print_log_message('DEBUG3', f"insert_tables: Returned row: {table_row}")
            self.insert_protocol('table', target_table, 'create', target_table_sql, None, None, None, 'info', None, table_row['id'])
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"insert_tables: Error inserting table info {source_table} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"insert_tables: Settings: {settings}")
            self.config_parser.print_log_message('ERROR', f"insert_tables: Exception: {e}")
            raise

    def update_table_status(self, row_id, success, message):
        table_name = self.config_parser.get_protocol_name_tables()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = clock_timestamp(),
            success = %s,
            message = %s
            WHERE id = %s
            RETURNING *
        """
        params = ('TRUE' if success else 'FALSE', message, row_id)
        self.config_parser.print_log_message('DEBUG3', f"update_table_status: Executing query with params: {params}")
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            if row:
                table_row = self.decode_table_row(row)
                self.config_parser.print_log_message('DEBUG3', f"update_table_status: Returned row: {table_row}")
                self.update_protocol('table', table_row['id'], success, message, None)
            else:
                self.config_parser.print_log_message('ERROR', f"update_table_status: Error updating status for table {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"update_table_status: Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"update_table_status: Error updating status for table {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"update_table_status: Query: {query}")
            self.config_parser.print_log_message('ERROR', f"update_table_status: Exception: {e}")
            raise

    def insert_indexes(self, values):
        table_name = self.config_parser.get_protocol_name_indexes()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema, source_table, source_table_id, index_owner, index_name, index_type,
            target_schema, target_table, index_sql, index_columns, index_comment, is_function_based)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (values['source_schema'], values['source_table'], values['source_table_id'], values['index_owner'],
                  values['index_name'], values['index_type'], values['target_schema'],
                  values['target_table'], values['index_sql'], values['index_columns'],
                  values['index_comment'], True if values['is_function_based'] == 'YES' else False)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            index_row = self.decode_index_row(row)
            self.config_parser.print_log_message('DEBUG3', f"insert_indexes: Returned row: {index_row}")
            self.insert_protocol('index', values['index_name'], 'create', values['index_sql'], None, None, None, 'info', None, index_row['id'])
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"insert_indexes: Error inserting index info {values['index_name']} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"insert_indexes: Exception: {e}")
            raise

    def update_index_status(self, row_id, success, message):
        table_name = self.config_parser.get_protocol_name_indexes()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = clock_timestamp(),
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

            if row:
                index_row = self.decode_index_row(row)
                self.config_parser.print_log_message('DEBUG3', f"update_index_status: Returned row: {index_row}")
                self.update_protocol('index', index_row['id'], success, message, None)
            else:
                self.config_parser.print_log_message('ERROR', f"update_index_status: Error updating status for index {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"update_index_status: Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"update_index_status: Error updating status for index {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"update_index_status: Query: {query}")
            self.config_parser.print_log_message('ERROR', f"update_index_status: Exception: {e}")
            raise

    def create_table_for_constraints(self):
        table_name = self.config_parser.get_protocol_name_constraints()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_table_id INTEGER,
            source_schema TEXT,
            source_table TEXT,
            target_schema TEXT,
            target_table TEXT,
            constraint_name TEXT,
            constraint_type TEXT,
            constraint_owner TEXT,
            constraint_columns TEXT,
            referenced_table_schema TEXT,
            referenced_table_name TEXT,
            referenced_columns TEXT,
            constraint_sql TEXT,
            delete_rule TEXT,
            update_rule TEXT,
            constraint_comment TEXT,
            constraint_status TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG3', f"create_table_for_constraints: Created protocol table {table_name}.")

    def decode_constraint_row(self, row):
        return {
            'id': row[0],
            'source_table_id': row[1],
            'source_schema': row[2],
            'source_table': row[3],
            'target_schema': row[4],
            'target_table': row[5],
            'constraint_name': row[6],
            'constraint_type': row[7],
            'constraint_owner': row[8],
            'constraint_columns': row[9],
            'referenced_table_schema': row[10],
            'referenced_table_name': row[11],
            'referenced_columns': row[12],
            'constraint_sql': row[13],
            'delete_rule': row[14],
            'update_rule': row[15],
            'constraint_comment': row[16],
            'constraint_status': row[17],
            'task_created': row[18],
            'task_started': row[19],
            'task_completed': row[20],
            'success': row[21],
            'message': row[22]
        }

    def insert_constraint(self, settings):
        table_name = self.config_parser.get_protocol_name_constraints()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_table_id, source_schema, source_table,
            target_schema, target_table, constraint_name,
            constraint_type,
            constraint_owner, constraint_columns,
            referenced_table_schema, referenced_table_name,
            referenced_columns, constraint_sql,
            delete_rule, update_rule, constraint_comment,
            constraint_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (settings['source_table_id'], settings['source_schema'], settings['source_table'],
                    settings['target_schema'], settings['target_table'], settings['constraint_name'],
                    settings['constraint_type'] if 'constraint_type' in settings else '',
                    settings['constraint_owner'] if 'constraint_owner' in settings else '',
                    settings['constraint_columns'] if 'constraint_columns' in settings else '',
                    settings['referenced_table_schema'] if 'referenced_table_schema' in settings else '',
                    settings['referenced_table_name'] if 'referenced_table_name' in settings else '',
                    settings['referenced_columns'] if 'referenced_columns' in settings else '',
                    settings['constraint_sql'] if 'constraint_sql' in settings else '',
                    settings['delete_rule'] if 'delete_rule' in settings else '',
                    settings['update_rule'] if 'update_rule' in settings else '',
                    settings['constraint_comment'] if 'constraint_comment' in settings else '',
                    settings['constraint_status'] if 'constraint_status' in settings else ''
                    )

        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            constraint_row = self.decode_constraint_row(row)
            self.config_parser.print_log_message('DEBUG3', f"insert_constraint: Returned row: {constraint_row}")
            self.insert_protocol('constraint', settings['constraint_name'], 'create',
                                 settings['constraint_sql'], None, None, None, 'info', None, constraint_row['id'])
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"insert_constraint: Error inserting constraint info {settings['constraint_name']} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"insert_constraint: Query: {query}")
            self.config_parser.print_log_message('ERROR', f"insert_constraint: Exception: {e}")
            raise

    def update_constraint_status(self, row_id, success, message):
        table_name = self.config_parser.get_protocol_name_constraints()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = clock_timestamp(),
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

            if row:
                constraint_row = self.decode_constraint_row(row)
                self.config_parser.print_log_message('DEBUG3', f"update_constraint_status: Returned row: {constraint_row}")
                self.update_protocol('constraint', constraint_row['id'], success, message, None)
            else:
                self.config_parser.print_log_message('ERROR', f"update_constraint_status: Error updating status for constraint {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"update_constraint_status: Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"update_constraint_status: Error updating status for constraint {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"update_constraint_status: Query: {query}")
            self.config_parser.print_log_message('ERROR', f"update_constraint_status: Exception: {e}")
            raise

    def insert_funcprocs(self, source_schema, source_funcproc_name, source_funcproc_id, source_funcproc_sql, target_schema, target_funcproc_name, target_funcproc_sql, funcproc_comment):
        table_name = self.config_parser.get_protocol_name_funcprocs()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema, source_funcproc_name, source_funcproc_id, source_funcproc_sql, target_schema, target_funcproc_name, target_funcproc_sql, funcproc_comment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema, source_funcproc_name, source_funcproc_id, source_funcproc_sql, target_schema, target_funcproc_name, target_funcproc_sql, funcproc_comment)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            funcproc_row = self.decode_funcproc_row(row)
            self.config_parser.print_log_message('DEBUG3', f"insert_funcprocs: Returned row: {funcproc_row}")
            self.insert_protocol('funcproc', source_funcproc_name, 'create', target_funcproc_sql, None, None, None, 'info', None, funcproc_row['id'])
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"insert_funcprocs: Error inserting funcproc info {source_funcproc_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"insert_funcprocs: Exception: {e}")
            raise

    def update_funcproc_status(self, source_funcproc_id, success, message):
        table_name = self.config_parser.get_protocol_name_funcprocs()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = clock_timestamp(),
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

            if row:
                funcproc_row = self.decode_funcproc_row(row)
                self.config_parser.print_log_message('DEBUG3', f"update_funcproc_status: Returned row: {funcproc_row}")
                self.update_protocol('funcproc', funcproc_row['id'], success, message, None)
            else:
                self.config_parser.print_log_message('ERROR', f"update_funcproc_status: Error updating status for funcproc {source_funcproc_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"update_funcproc_status: Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"update_funcproc_status: Error updating status for funcproc {source_funcproc_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"update_funcproc_status: Query: {query}")
            self.config_parser.print_log_message('ERROR', f"update_funcproc_status: Exception: {e}")
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

            sequence_row = self.decode_sequence_row(row)
            self.config_parser.print_log_message('DEBUG3', f"insert_sequence: Returned row: {sequence_row}")
            self.insert_protocol('sequence', sequence_name, 'create', set_sequence_sql, None, None, None, 'info', None, sequence_row['sequence_id'])
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"insert_sequence: Error inserting sequence info {sequence_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"insert_sequence: Exception: {e}")
            raise

    def update_sequence_status(self, sequence_id, success, message):
        table_name = self.config_parser.get_protocol_name_sequences()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = clock_timestamp(),
            success = %s,
            message = %s
            WHERE sequence_id = %s
            RETURNING *
        """
        params = ('TRUE' if success else 'FALSE', message, sequence_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            if row:
                sequence_row = self.decode_sequence_row(row)
                self.config_parser.print_log_message('DEBUG3', f"update_sequence_status: Returned row: {sequence_row}")
                self.update_protocol('sequence', sequence_row['sequence_id'], success, message, None)
            else:
                self.config_parser.print_log_message('ERROR', f"update_sequence_status: Error updating status for sequence {sequence_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"update_sequence_status: Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"update_sequence_status: Error updating status for sequence {sequence_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"update_sequence_status: Query: {query}")
            self.config_parser.print_log_message('ERROR', f"update_sequence_status: Exception: {e}")
            raise

    def insert_trigger(self, source_schema, source_table, source_table_id, target_schema, target_table, trigger_id, trigger_name, trigger_event, trigger_new, trigger_old, trigger_source_sql, trigger_target_sql, trigger_comment):
        table_name = self.config_parser.get_protocol_name_triggers()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema, source_table, source_table_id, target_schema, target_table, trigger_id, trigger_name, trigger_event, trigger_new, trigger_old, trigger_source_sql, trigger_target_sql, trigger_comment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema, source_table, source_table_id, target_schema, target_table, trigger_id, trigger_name, trigger_event, trigger_new, trigger_old, trigger_source_sql, trigger_target_sql, trigger_comment)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            trigger_row = self.decode_trigger_row(row)
            self.config_parser.print_log_message('DEBUG3', f"insert_trigger: Returned row: {trigger_row}")
            self.insert_protocol('trigger', trigger_name, 'create', trigger_target_sql, None, None, None, 'info', None, trigger_row['id'])
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"insert_trigger: Error inserting trigger info {trigger_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"insert_trigger: Exception: {e}")
            raise

    def update_trigger_status(self, row_id, success, message):
        table_name = self.config_parser.get_protocol_name_triggers()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = clock_timestamp(),
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

            if row:
                trigger_row = self.decode_trigger_row(row)
                self.config_parser.print_log_message('DEBUG3', f"update_trigger_status: Returned row: {trigger_row}")
                self.update_protocol('trigger', trigger_row['id'], success, message, None)
            else:
                self.config_parser.print_log_message('ERROR', f"update_trigger_status: Error updating status for trigger {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"update_trigger_status: Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"update_trigger_status: Error updating status for trigger {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"update_trigger_status: Query: {query}")
            self.config_parser.print_log_message('ERROR', f"update_trigger_status: Exception: {e}")
            raise

    def fetch_all_triggers(self):
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
            self.config_parser.print_log_message('ERROR', f"Error selecting triggers.")
            self.config_parser.print_log_message('ERROR', e)
            return None

    def insert_view(self, source_schema, source_view_name, source_view_id, source_view_sql, target_schema, target_view_name, target_view_sql, view_comment):
        table_name = self.config_parser.get_protocol_name_views()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema, source_view_name, source_view_id, source_view_sql, target_schema, target_view_name, target_view_sql, view_comment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema, source_view_name, source_view_id, source_view_sql, target_schema, target_view_name, target_view_sql, view_comment)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            view_row = self.decode_view_row(row)
            self.config_parser.print_log_message('DEBUG3', f"insert_view: Returned row: {view_row}")
            self.insert_protocol('view', source_view_name, 'create', target_view_sql, None, None, None, 'info', None, view_row['id'])
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"insert_view: Error inserting view info {source_view_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"insert_view: Exception: {e}")
            raise

    def fetch_all_views(self):
        table_name = self.config_parser.get_protocol_name_views()
        query = f"""SELECT * FROM "{self.protocol_schema}"."{table_name}" ORDER BY id"""
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"fetch_all_views: Error selecting views.")
            self.config_parser.print_log_message('ERROR', f"fetch_all_views: Exception: {e}")
            return None

    def update_view_status(self, row_id, success, message):
        table_name = self.config_parser.get_protocol_name_views()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = clock_timestamp(),
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

            if row:
                view_row = self.decode_view_row(row)
                self.config_parser.print_log_message('DEBUG3', f"update_view_status: Returned row: {view_row}")
                self.update_protocol('view', view_row['id'], success, message, None)

            else:
                self.config_parser.print_log_message('ERROR', f"update_view_status: Error updating status for view {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"update_view_status: Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"update_view_status: Error updating status for view {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"update_view_status: Query: {query}")
            self.config_parser.print_log_message('ERROR', f"update_view_status: Exception: {e}")
            raise

    def select_primary_key(self, source_schema, source_table):
        query = f"""
            SELECT
                i.index_columns
            FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_indexes()}" i
            WHERE i.source_schema = '{source_schema}' AND
                i.source_table = '{source_table}' AND
                i.index_type in ('PRIMARY KEY', 'UNIQUE')
            ORDER BY CASE WHEN i.index_type = 'PRIMARY KEY' THEN 1 ELSE 2 END
            LIMIT 1
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
            self.config_parser.print_log_message('ERROR', f"Error selecting primary key for {source_schema}.{source_table}.")
            self.config_parser.print_log_message('ERROR', e)
            return None

    def print_summary(self, objects, migrator_table_name, additional_columns=None):
        try:
            self.config_parser.print_log_message('INFO', f"{objects} summary:")
            query = f"""SELECT COUNT(*) FROM "{self.protocol_schema}"."{migrator_table_name}" """
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query)
            summary = cursor.fetchone()[0]
            if objects.lower() not in ['sequences']:
                self.config_parser.print_log_message('INFO', f"    Found in source: {summary}")
            else:
                self.config_parser.print_log_message('INFO', f"    Found: {summary}")
            if additional_columns:
                columns_count = len(additional_columns.split(','))
                columns_numbers = ', '.join(str(i + 2) for i in range(columns_count))
                query = f"""SELECT COUNT(*), {additional_columns} FROM "{self.protocol_schema}"."{migrator_table_name}" GROUP BY {columns_numbers} ORDER BY {columns_numbers}"""
                cursor.execute(query)
                rows = cursor.fetchall()
                for row in rows:
                    self.config_parser.print_log_message('INFO', f"        {row[1:]}: {row[0]}")

            if not self.config_parser.is_dry_run():
                query = f"""SELECT success, COUNT(*) FROM "{self.protocol_schema}"."{migrator_table_name}" GROUP BY 1 ORDER BY 1"""
                cursor.execute(query)
                rows = cursor.fetchall()
                if objects.lower() not in ['sequences']:
                    success_description = "successfully migrated"
                else:
                    success_description = "successfully set"
                for row in rows:
                    status = success_description if row[0] else "error" if row[0] is False else "unknown status"
                    row_success = row[0] if row[0] is not None else 'NULL'
                    self.config_parser.print_log_message('INFO', f"    {status}: {row[1]}")
                    if additional_columns:
                        query = f"""SELECT COUNT(*), {additional_columns} FROM "{self.protocol_schema}"."{migrator_table_name}" WHERE success = {row_success} GROUP BY {columns_numbers} ORDER BY {columns_numbers}"""
                        cursor.execute(query)
                        rows = cursor.fetchall()
                        for row in rows:
                            # status = success_description if row[0] else "error" if row[0] is False else "unknown status"
                            self.config_parser.print_log_message('INFO', f"        {row[1:]}: {row[0]}")

            cursor.close()

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error printing migration summary.")
            self.config_parser.print_log_message('ERROR', e)
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
                intendation = ''
                if task_data['subtask_name'] != '':
                    intendation = '    '
                started_str = str(task_data['task_started'])[:19] if task_data['task_started'] else ''
                completed_str = str(task_data['task_completed'])[:19] if task_data['task_completed'] else ''
                length_str = str(length)[:19] if length != "none" else ''
                status = f"{intendation}{(task_data['task_name']+': '+task_data['subtask_name'])[:50]:<50} | {started_str:<19} -> {completed_str:<19} | length: {length_str}"
                self.config_parser.print_log_message('INFO', f"{status}")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error printing migration summary.")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def print_data_migration_summary(self):
        self.config_parser.print_log_message('INFO', "Table rows migration stats:")
        table_name = self.config_parser.get_protocol_name_data_migration()
        data_migration_table_name = self.config_parser.get_protocol_name_data_migration()
        batches_stats_table_name = self.config_parser.get_protocol_name_batches_stats()
        query = f"""SELECT min(task_created) as min_time, max(task_completed) as max_time FROM "{self.protocol_schema}"."{table_name}" WHERE task_completed IS NOT NULL"""
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        if row[0] and row[1]:
            min_time = row[0]
            max_time = row[1]
            length = max_time - min_time
            self.config_parser.print_log_message('INFO', f"    start: {str(min_time)[:19]} | end: {str(max_time)[:19]} | length: {str(length)[:19]}")
        else:
            self.config_parser.print_log_message('INFO', "    No data migration tasks completed.")

        query = f"""SELECT COUNT(*) FROM "{self.protocol_schema}"."{table_name}" """
        cursor.execute(query)
        summary = cursor.fetchone()[0]
        self.config_parser.print_log_message('INFO', f"    Tables in total: {summary}")

        if not self.config_parser.is_dry_run():
            query = f"""SELECT COUNT(*) FROM "{self.protocol_schema}"."{table_name}" WHERE source_table_rows = 0 OR source_table_rows IS NULL"""
            cursor.execute(query)
            summary = cursor.fetchone()[0]
            self.config_parser.print_log_message('INFO', f"    Empty tables (0 rows): {summary}")

        if not self.config_parser.is_dry_run():
            query = f"""SELECT COUNT(*) FROM "{self.protocol_schema}"."{table_name}" WHERE source_table_rows > 0"""
            cursor.execute(query)
            summary = cursor.fetchone()[0]
            self.config_parser.print_log_message('INFO', f"    Tables with data: {summary}")

        if not self.config_parser.is_dry_run():
            query = f"""SELECT COUNT(*) FROM "{self.protocol_schema}"."{table_name}" WHERE source_table_rows > 0 AND source_table_rows = target_table_rows"""
            cursor.execute(query)
            summary = cursor.fetchone()[0]
            self.config_parser.print_log_message('INFO', f"    Tables with data - fully migrated: {summary}")

            try:
                query = f"""SELECT target_schema, target_table, source_table_rows, target_table_rows, task_completed - task_created as migration_time,
                round(shortest_batch_seconds::numeric, 2) as shortest_batch_seconds,
                round(longest_batch_seconds::numeric, 2) as longest_batch_seconds,
                round(average_batch_seconds::numeric, 2) as average_batch_seconds,
                batch_count
                FROM "{self.protocol_schema}"."{data_migration_table_name}" WHERE source_table_rows > 0 AND source_table_rows = target_table_rows
                ORDER BY source_table_rows DESC LIMIT 10"""
                cursor.execute(query)
                rows = cursor.fetchall()
                if rows:
                    self.config_parser.print_log_message('INFO', "        Tables with data - fully migrated (top 10):")
                    max_table_name_length = max(len(row[1]) for row in rows) if rows else 10
                    for row in rows:
                        target_schema = row[0]
                        target_table = row[1]
                        source_table_rows = row[2]
                        target_table_rows = row[3]
                        migration_time = row[4]
                        formatted_source_rows = f"{source_table_rows:,}".rjust(15)
                        formatted_target_rows = f"{target_table_rows:,}".rjust(15)

                        message = f"        {target_schema}.{target_table[:max_table_name_length].ljust(max_table_name_length)} |"
                        message += f"{formatted_source_rows} = {formatted_target_rows} | length: {str(migration_time)[:19]:<19}"
                        if row[8] == 1:
                            message += " | 1 batch"
                        else:
                            message += f" | {row[8]:<6} batches | shortest: {row[5]:<6} | average: {row[7]:<6} | longest: {row[6]:<6}"
                        self.config_parser.print_log_message('INFO', message)
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"Error fetching fully migrated tables.")
                self.config_parser.print_log_message('ERROR', e)

        if not self.config_parser.is_dry_run():
            query = f"""SELECT COUNT(*) FROM "{self.protocol_schema}"."{table_name}" WHERE source_table_rows > 0 AND source_table_rows <> target_table_rows"""
            cursor.execute(query)
            summary = cursor.fetchone()[0]
            self.config_parser.print_log_message('INFO', f"    Tables with differences in row counts: {summary}")

            try:
                query = f"""SELECT target_schema, target_table, source_table_rows, target_table_rows, task_completed - task_created as migration_time
                FROM "{self.protocol_schema}"."{data_migration_table_name}" WHERE source_table_rows > 0 AND source_table_rows <> target_table_rows
                ORDER BY source_table_rows DESC LIMIT 10"""
                cursor.execute(query)
                rows = cursor.fetchall()
                if rows:
                    self.config_parser.print_log_message('INFO', "        Tables with different row counts (top 10):")
                    max_table_name_length = max(len(row[1]) for row in rows) if rows else 0
                    max_table_name_length += 1
                    for row in rows:
                        target_schema = row[0]
                        target_table = row[1]
                        source_table_rows = row[2]
                        target_table_rows = row[3]
                        migration_time = row[4]
                        formatted_source_rows = f"{source_table_rows:,}".rjust(12)
                        formatted_target_rows = f"{target_table_rows:,}".rjust(12)
                        self.config_parser.print_log_message('INFO', f"        {target_schema}.{target_table[:max_table_name_length].ljust(max_table_name_length)} | {formatted_source_rows} <> {formatted_target_rows} | length: {str(migration_time)[:19]:<19}")
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"Error fetching migrated tables with different row counts.")
                self.config_parser.print_log_message('ERROR', e)


        if not self.config_parser.is_dry_run():
            try:
                query = f"""SELECT source_schema, source_table, batch_number,
                round(batch_seconds::numeric, 2) as batch_seconds,
                round(reading_seconds::numeric, 2) as reading_seconds,
                round(transforming_seconds::numeric, 2) as transforming_seconds,
                round(writing_seconds::numeric, 2) as inserting_seconds
                FROM "{self.protocol_schema}"."{batches_stats_table_name}"
                ORDER BY batch_seconds DESC LIMIT 10"""
                cursor.execute(query)
                rows = cursor.fetchall()
                if rows:
                    self.config_parser.print_log_message('INFO', "    Longest migration batches (top 10):")
                    for row in rows:
                        target_schema = row[0]
                        target_table = row[1]
                        batch_number = row[2]
                        batch_seconds = row[3]
                        reading_seconds = row[4]
                        transforming_seconds = row[5]
                        inserting_seconds = row[6]
                        formatted_batch_seconds = f"{batch_seconds:,}".rjust(15)
                        formatted_reading_seconds = f"{reading_seconds:,}".rjust(15)
                        formatted_transforming_seconds = f"{transforming_seconds:,}".rjust(15)
                        formatted_inserting_seconds = f"{inserting_seconds:,}".rjust(15)
                        self.config_parser.print_log_message('INFO', f"        {target_schema}.{target_table[:max_table_name_length].ljust(max_table_name_length)} | "
                                                            f"batch: {batch_number} | {formatted_batch_seconds} sec | "
                                                            f"r: {formatted_reading_seconds} | t: {formatted_transforming_seconds} | w: {formatted_inserting_seconds}")
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"Error fetching longest migration batch.")
                self.config_parser.print_log_message('ERROR', e)

        cursor.close()

    def print_migration_summary(self):
        self.config_parser.print_log_message('INFO', "Migration stats:")
        self.config_parser.print_log_message('INFO', f"    Source database: {self.config_parser.get_source_db_name()}, schema: {self.config_parser.get_source_owner()} ({self.config_parser.get_source_db_type()})")
        self.config_parser.print_log_message('INFO', f"    Target database: {self.config_parser.get_target_db_name()}, schema: {self.config_parser.get_target_schema()} ({self.config_parser.get_target_db_type()})")
        self.print_main(self.config_parser.get_protocol_name_main())
        self.config_parser.print_log_message('INFO', "Migration summary:")
        if self.config_parser.is_dry_run():
            self.config_parser.print_log_message('INFO', "! Dry run mode enabled. No migration performed !")
        self.print_summary('User Defined Types', self.config_parser.get_protocol_name_user_defined_types())
        self.print_summary('Tables', self.config_parser.get_protocol_name_tables())
        self.print_data_migration_summary()
        self.print_summary('Altered columns', self.config_parser.get_protocol_name_target_columns_alterations(), 'reason')
        self.print_summary('Sequences', self.config_parser.get_protocol_name_sequences())
        self.print_summary('Indexes', self.config_parser.get_protocol_name_indexes(), 'index_type, index_owner')
        self.print_summary('Constraints', self.config_parser.get_protocol_name_constraints(), 'constraint_type')
        self.print_summary('Domains', self.config_parser.get_protocol_name_domains(), 'migrated_as')
        self.print_summary('Functions / procedures', self.config_parser.get_protocol_name_funcprocs())
        self.print_summary('Triggers', self.config_parser.get_protocol_name_triggers())
        self.print_summary('Views', self.config_parser.get_protocol_name_views())
        if self.config_parser.is_dry_run():
            self.config_parser.print_log_message('INFO', "! Dry run mode enabled. No migration performed !")

    def fetch_all_tables(self, only_unfinished=False):
        if only_unfinished:
            query = f"""SELECT * FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_tables()}" WHERE success IS NOT TRUE ORDER BY id"""
        else:
            query = f"""SELECT * FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_tables()}" ORDER BY id"""
        # self.protocol_connection.connect()
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        tables = cursor.fetchall()
        return tables

    def fetch_table(self, source_schema_name, source_table_name):
        query = f"""
                SELECT *
                FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_tables()}"
                WHERE source_schema = '{source_schema_name}'
                AND source_table = '{source_table_name}'
                """
        # self.protocol_connection.connect()
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        table = cursor.fetchone()
        cursor.close()
        if not table:
            return None
        return self.decode_table_row(table)

    def fetch_data_source(self, source_schema_name, source_table_name):
        query = f"""SELECT * FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_data_sources()}"
            WHERE source_schema = '{source_schema_name}' AND source_table = '{source_table_name}'"""
        self.config_parser.print_log_message('DEBUG3', f"fetch_data_source: Executing query: {query}")
        # self.protocol_connection.connect()
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        data_source = cursor.fetchone()
        if not data_source:
            return None
        return self.decode_data_source_row(data_source)

    def fetch_all_target_table_names(self):
        tables = self.fetch_all_tables()
        table_names = []
        for table in tables:
            values = self.decode_table_row(table)
            table_names.append(values['target_table'])
        return table_names

    def fetch_all_data_migrations(self, source_schema_name=None, source_table_name=None):
        if source_schema_name and source_table_name:
            query = f"""SELECT * FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_data_migration()}" WHERE source_schema = '{source_schema_name}' AND source_table = '{source_table_name}' ORDER BY id"""
        else:
            query = f"""SELECT * FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_data_migration()}" ORDER BY id"""
        # self.protocol_connection.connect()
        self.config_parser.print_log_message('DEBUG3', f"fetch_all_data_migrations: Executing query: {query}")
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        data_migrations = cursor.fetchall()
        self.config_parser.print_log_message('DEBUG3', f"fetch_all_data_migrations: Fetched {len(data_migrations)} rows.")
        return data_migrations

    def fetch_all_views(self):
        query = f"""SELECT * FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_views()}" ORDER BY id"""
        # self.protocol_connection.connect()
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        views = cursor.fetchall()
        return views

    def fetch_all_target_view_names(self):
        views = self.fetch_all_views()
        view_names = []
        for view in views:
            values = self.decode_view_row(view)
            view_names.append(values['target_view_name'])
        return view_names

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


if __name__ == "__main__":
    print("This script is not meant to be run directly")
