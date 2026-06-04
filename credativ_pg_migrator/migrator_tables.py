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
import uuid
import psycopg2
import traceback
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
            port=cfg.get('port', 5432),
            application_name=MigratorConstants.get_application_name()
        )
        self.connection.autocommit = True

    def execute_query(self, query, params=None):
        try:
            with self.connection.cursor() as cur:
                cur.execute(query, params) if params else cur.execute(query)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise e

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
        self.create_table_for_source_table_partitioning()
        self.create_table_for_target_table_partitioning()
        self.create_table_for_columns()
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
        self.create_table_for_aliases()
        self.create_table_for_triggers()
        self.create_table_for_views()
        self.create_ddl_tables()
        self.create_table_for_mapping()

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
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: prepare_data_types_substitution: Table data_types_substitution created in schema {self.protocol_schema}")

        # Insert data into the table
        for table_name, column_name, source_type, target_type, comment in self.config_parser.get_data_types_substitution():
            self.protocol_connection.execute_query(f"""
            INSERT INTO "{self.protocol_schema}".data_types_substitution
            (table_name, column_name, source_type, target_type, comment)
            VALUES (%s, %s, %s, %s, %s)
            """, (table_name, column_name, source_type, target_type, comment))
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: prepare_data_types_substitution: Data inserted into table data_types_substitution in schema {self.protocol_schema}")

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
        self.config_parser.print_log_message('DEBUG2', f"migrator_tables: check_data_types_substitution: query: {query} - params: {params}")
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query, params)
        result = cursor.fetchone()
        cursor.close()
        self.config_parser.print_log_message('DEBUG2', f"migrator_tables: check_data_types_substitution: result: {result}")
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
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: prepare_data_migration_limitation: Table data_migration_limitation created in schema {self.protocol_schema}")

        # Insert data into the table
        for source_table_name, where_limitation, use_when_column_present in self.config_parser.get_data_migration_limitation():
            self.protocol_connection.execute_query(f"""
            INSERT INTO "{self.protocol_schema}".data_migration_limitation
            (source_table_name, where_limitation, use_when_column_present)
            VALUES (%s, %s, %s)
            """, (source_table_name, where_limitation, use_when_column_present))
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: prepare_data_migration_limitation: Data inserted into table data_migration_limitation in schema {self.protocol_schema}")

    def get_records_data_migration_limitation(self, source_table_name):
        query = f"""
        SELECT where_limitation, use_when_column_present
        FROM "{self.protocol_schema}".data_migration_limitation
        WHERE trim('{source_table_name}') = trim(source_table_name)
        OR trim('{source_table_name}') ~ trim(source_table_name)
        """
        self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: get_records_data_migration_limitation: query: {query}")
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
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: prepare_remote_objects_substitution: Table remote_objects_substitution created in schema {self.protocol_schema}")

        # Insert data into the table
        for source_object_name, target_object_name in self.config_parser.get_remote_objects_substitution():
            self.protocol_connection.execute_query(f"""
            INSERT INTO "{self.protocol_schema}".remote_objects_substitution
            (source_object_name, target_object_name)
            VALUES (%s, %s)
            """, (source_object_name, target_object_name))
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: prepare_remote_objects_substitution: Data inserted into table remote_objects_substitution in schema {self.protocol_schema}")

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
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: prepare_default_values_substitution: Table default_values_substitution created in schema {self.protocol_schema}")

        # Insert data into the table
        for column_name, source_column_data_type, default_value_value, target_default_value in self.config_parser.get_default_values_substitution():
            self.insert_default_values_substitution({
                'column_name': column_name,
                'source_column_data_type': source_column_data_type,
                'default_value_value': default_value_value,
                'target_default_value': target_default_value
            })
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: prepare_default_values_substitution: Data inserted into table default_values_substitution in schema {self.protocol_schema}")

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
                WHERE (lower(trim(%s)) ~ lower(trim(column_name)) OR lower(trim(%s)) ILIKE lower(trim(column_name)) OR lower(trim(column_name)) = '')
                AND (lower(trim(%s)) ~ lower(trim(source_column_data_type)) OR lower(trim(%s)) ILIKE lower(trim(source_column_data_type)) OR lower(trim(source_column_data_type)) = '')
                AND (lower(trim(%s::TEXT)) ~ lower(trim(default_value_value::TEXT)) OR lower(trim(%s::TEXT)) ILIKE lower(trim(default_value_value::TEXT)) )
                ORDER BY CASE WHEN default_value_value LIKE '%%(?i)%%' THEN 1 ELSE 2 END
            """
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, (check_column_name, check_column_name, check_column_data_type, check_column_data_type,  check_default_value, check_default_value))
            result = cursor.fetchone()
            self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: check_default_values_substitution: 0 check_default_values_substitution {check_column_name}, {check_column_data_type}, {check_default_value} query: {query} - {result}")

            if result is not None:
                target_default_value = result[0]
                self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: check_default_values_substitution: 0 check_default_values_substitution found direct match: {target_default_value}")
            else:
                query = f"""
                    SELECT target_default_value
                    FROM "{self.protocol_schema}".default_values_substitution
                    WHERE lower(trim(%s)) ILIKE lower(trim(column_name))
                    AND lower(trim(%s)) ILIKE lower(trim(source_column_data_type))
                    AND lower(trim(%s::TEXT)) ILIKE lower(trim(default_value_value::TEXT))
                """
                cursor = self.protocol_connection.connection.cursor()
                cursor.execute(query, (check_column_name, check_column_data_type, check_default_value))
                result = cursor.fetchone()
                self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: check_default_values_substitution: 1 check_default_values_substitution {check_column_name}, {check_column_data_type}, {check_default_value} query: {query} - {result}")

                if result is not None:
                    target_default_value = result[0]
                    self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: check_default_values_substitution: 1 check_default_values_substitution found ILIKE match: {target_default_value}")
                else:
                    query = f"""
                        SELECT target_default_value
                        FROM "{self.protocol_schema}".default_values_substitution
                        WHERE lower(trim(column_name)) = ''
                        AND lower(trim(%s)) ILIKE lower(trim(source_column_data_type))
                        AND lower(trim(%s::TEXT)) ILIKE lower(trim(default_value_value::TEXT))
                    """
                    cursor.execute(query, (check_column_data_type, check_default_value))
                    result = cursor.fetchone()
                    self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: check_default_values_substitution: 2 check_default_values_substitution {check_column_name}, {check_column_data_type}, {check_default_value} query: {query} - {result}")

                    if result is not None:
                        target_default_value = result[0]
                        self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: check_default_values_substitution: 2 check_default_values_substitution found data type match: {target_default_value}")
                    else:
                        query = f"""
                            SELECT target_default_value
                            FROM "{self.protocol_schema}".default_values_substitution
                            WHERE lower(trim(column_name)) = ''
                            AND lower(trim(source_column_data_type)) = ''
                            AND lower(trim(%s::TEXT)) ILIKE lower(trim(default_value_value::TEXT))
                        """
                        cursor.execute(query, (check_default_value,))
                        result = cursor.fetchone()
                        self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: check_default_values_substitution: 3 check_default_values_substitution {check_column_name}, {check_column_data_type}, {check_default_value} query: {query} - {result}")

                        if result is not None:
                            target_default_value = result[0]
                            self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: check_default_values_substitution: 3 check_default_values_substitution found default value match: {target_default_value}")
            cursor.close()

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: check_default_values_substitution: Error checking default values substitution for {check_column_name}, {check_column_data_type}, {check_default_value}.")
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
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: create_protocol: Table {table_name} created in schema {self.protocol_schema}")

    def create_table_for_mapping(self):
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."mapping_tables" (
                id SERIAL PRIMARY KEY,
                source_schema_name TEXT,
                source_table_name TEXT,
                target_schema_name TEXT,
                target_table_name TEXT,
                match_type TEXT NOT NULL,
                similarity_score FLOAT,
                source_table_rows_all INTEGER,
                source_table_rows_limited INTEGER,
                target_table_rows INTEGER,
                info TEXT
            );
        """)
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."mapping_columns" (
                id SERIAL PRIMARY KEY,
                source_schema_name TEXT,
                source_table_name TEXT,
                source_column_name TEXT,
                target_schema_name TEXT,
                target_table_name TEXT,
                target_column_name TEXT,
                source_ordinal_number INTEGER,
                target_ordinal_number INTEGER,
                source_data_type TEXT,
                target_data_type TEXT,
                match_type TEXT NOT NULL
            );
        """)
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."mapping_target_indexes" (
                id SERIAL PRIMARY KEY,
                target_schema_name TEXT,
                target_table_name TEXT,
                index_name TEXT,
                index_def TEXT,
                is_primary_key BOOLEAN DEFAULT FALSE,
                index_type TEXT,
                dropped BOOLEAN,
                success BOOLEAN,
                message TEXT
            );
        """)
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."mapping_target_constraints" (
                id SERIAL PRIMARY KEY,
                target_schema_name TEXT,
                target_table_name TEXT,
                constraint_name TEXT,
                constraint_type TEXT,
                constraint_def TEXT,
                dropped BOOLEAN,
                success BOOLEAN,
                message TEXT
            );
        """)
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."mapping_target_sequences" (
                id SERIAL PRIMARY KEY,
                target_schema_name TEXT,
                target_table_name TEXT,
                sequence_schema_name TEXT,
                sequence_name TEXT,
                used_in_default BOOLEAN DEFAULT FALSE,
                used_in_identity BOOLEAN DEFAULT FALSE,
                used_in_trigger BOOLEAN DEFAULT FALSE,
                trigger_name TEXT,
                column_name TEXT
            );
        """)
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."mapping_unmatched_objects" (
                id SERIAL PRIMARY KEY,
                object_type TEXT,
                side TEXT,
                parent_object TEXT,
                object_name TEXT,
                row_count INTEGER
            );
        """)
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: create_table_for_mapping: Mapping tables created in schema {self.protocol_schema}")


    def insert_mapping_tables(self, settings):
        func_run_id = uuid.uuid4()
        source_schema_name = settings.get('source_schema_name')
        source_table_name = settings.get('source_table_name')
        target_schema_name = settings.get('target_schema_name')
        target_table_name = settings.get('target_table_name')
        match_type = settings.get('match_type')
        similarity_score = settings.get('similarity_score')
        source_table_rows_all = settings.get('source_table_rows_all')
        source_table_rows_limited = settings.get('source_table_rows_limited')
        target_table_rows = settings.get('target_table_rows')
        info = settings.get('info')

        query = f"""
            INSERT INTO "{self.protocol_schema}"."mapping_tables"
            (source_schema_name, source_table_name, target_schema_name, target_table_name, match_type, similarity_score, source_table_rows_all, source_table_rows_limited, target_table_rows, info)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        params = (source_schema_name, source_table_name, target_schema_name, target_table_name, match_type, similarity_score, source_table_rows_all, source_table_rows_limited, target_table_rows, info)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
            return row[0] if row else None
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_mapping_tables: ({func_run_id}): Error: {e}")
            raise

    def insert_mapping_columns(self, settings):
        func_run_id = uuid.uuid4()
        source_schema_name = settings.get('source_schema_name')
        source_table_name = settings.get('source_table_name')
        source_column_name = settings.get('source_column_name')
        target_schema_name = settings.get('target_schema_name')
        target_table_name = settings.get('target_table_name')
        target_column_name = settings.get('target_column_name')
        source_ordinal_number = settings.get('source_ordinal_number')
        target_ordinal_number = settings.get('target_ordinal_number')
        source_data_type = settings.get('source_data_type')
        target_data_type = settings.get('target_data_type')
        match_type = settings.get('match_type')

        query = f"""
            INSERT INTO "{self.protocol_schema}"."mapping_columns"
            (source_schema_name, source_table_name, source_column_name, target_schema_name, target_table_name, target_column_name, source_ordinal_number, target_ordinal_number, source_data_type, target_data_type, match_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        params = (source_schema_name, source_table_name, source_column_name, target_schema_name, target_table_name, target_column_name, source_ordinal_number, target_ordinal_number, source_data_type, target_data_type, match_type)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
            return row[0] if row else None
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_mapping_columns: ({func_run_id}): Error: {e}")
            raise

    def insert_mapping_unmatched_objects(self, unmatched_list):
        if not unmatched_list:
            return
            
        func_run_id = uuid.uuid4()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."mapping_unmatched_objects"
            (object_type, side, parent_object, object_name, row_count)
            VALUES %s
        """
        values = [(
            obj['object_type'],
            obj['side'],
            obj.get('parent_object', ''),
            obj['object_name'],
            obj.get('row_count', None)
        ) for obj in unmatched_list]
        
        try:
            cursor = self.protocol_connection.connection.cursor()
            psycopg2.extras.execute_values(cursor, query, values)
            cursor.close()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_mapping_unmatched_objects: ({func_run_id}): Error: {e}")
            raise

    def insert_mapping_target_indexes(self, settings):
        func_run_id = uuid.uuid4()
        target_schema_name = settings.get('target_schema_name')
        target_table_name = settings.get('target_table_name')
        index_name = settings.get('index_name')
        index_def = settings.get('index_def')
        is_primary_key = settings.get('is_primary_key', False)
        index_type = settings.get('index_type', 'UNKNOWN')

        query = f"""
            INSERT INTO "{self.protocol_schema}"."mapping_target_indexes"
            (target_schema_name, target_table_name, index_name, index_def, is_primary_key, index_type)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        params = (target_schema_name, target_table_name, index_name, index_def, is_primary_key, index_type)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
            return row[0] if row else None
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_mapping_target_indexes: ({func_run_id}): Error: {e}")
            raise

    def insert_mapping_target_constraints(self, settings):
        func_run_id = uuid.uuid4()
        target_schema_name = settings.get('target_schema_name')
        target_table_name = settings.get('target_table_name')
        constraint_name = settings.get('constraint_name')
        constraint_type = settings.get('constraint_type')
        constraint_def = settings.get('constraint_def')

        query = f"""
            INSERT INTO "{self.protocol_schema}"."mapping_target_constraints"
            (target_schema_name, target_table_name, constraint_name, constraint_type, constraint_def)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """
        params = (target_schema_name, target_table_name, constraint_name, constraint_type, constraint_def)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
            return row[0] if row else None
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_mapping_target_constraints: ({func_run_id}): Error: {e}")
            raise

    def insert_mapping_target_sequences(self, settings):
        func_run_id = uuid.uuid4()
        target_schema_name = settings.get('target_schema_name')
        target_table_name = settings.get('target_table_name')
        sequence_schema_name = settings.get('sequence_schema_name')
        sequence_name = settings.get('sequence_name')
        used_in_default = settings.get('used_in_default', False)
        used_in_identity = settings.get('used_in_identity', False)
        used_in_trigger = settings.get('used_in_trigger', False)
        trigger_name = settings.get('trigger_name')
        column_name = settings.get('column_name')

        query = f"""
            INSERT INTO "{self.protocol_schema}"."mapping_target_sequences"
            (target_schema_name, target_table_name, sequence_schema_name, sequence_name, used_in_default, used_in_identity, used_in_trigger, trigger_name, column_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        params = (target_schema_name, target_table_name, sequence_schema_name, sequence_name, used_in_default, used_in_identity, used_in_trigger, trigger_name, column_name)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
            return row[0] if row else None
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_mapping_target_sequences: ({func_run_id}): Error: {e}")
            raise

    def fetch_mapping_target_indexes(self, target_schema_name, target_table_name):
        query = f"""
            SELECT id, index_name, index_def, is_primary_key, index_type
            FROM "{self.protocol_schema}"."mapping_target_indexes"
            WHERE target_schema_name = %s AND target_table_name = %s
        """
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, (target_schema_name, target_table_name))
            rows = cursor.fetchall()
            cursor.close()
            return [{'id': row[0], 'index_name': row[1], 'index_def': row[2], 'is_primary_key': row[3], 'index_type': row[4]} for row in rows]
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: fetch_mapping_target_indexes: Error fetching for {target_schema_name}.{target_table_name}")
            self.config_parser.print_log_message('ERROR', e)
            return []

    def fetch_all_mapping_target_indexes(self):
        query = f"""
            SELECT id, target_schema_name, target_table_name, index_name, index_def, is_primary_key, index_type
            FROM "{self.protocol_schema}"."mapping_target_indexes"
        """
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            return [{'id': row[0], 'target_schema_name': row[1], 'target_table_name': row[2], 'index_name': row[3], 'index_def': row[4], 'is_primary_key': row[5], 'index_type': row[6]} for row in rows]
        except Exception as e:
            self.config_parser.print_log_message('ERROR', "migrator_tables: fetch_all_mapping_target_indexes: Error fetching all mapping target indexes")
            self.config_parser.print_log_message('ERROR', e)
            return []

    def fetch_mapping_target_sequences(self, target_schema_name, target_table_name):
        query = f"""
            SELECT sequence_schema_name, sequence_name, used_in_default, used_in_identity, used_in_trigger, trigger_name, column_name
            FROM "{self.protocol_schema}"."mapping_target_sequences"
            WHERE target_schema_name = %s AND target_table_name = %s
        """
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, (target_schema_name, target_table_name))
            rows = cursor.fetchall()
            cursor.close()
            return [{
                'sequence_schema_name': row[0],
                'sequence_name': row[1],
                'used_in_default': row[2],
                'used_in_identity': row[3],
                'used_in_trigger': row[4],
                'trigger_name': row[5],
                'column_name': row[6]
            } for row in rows]
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: fetch_mapping_target_sequences: Error fetching for {target_schema_name}.{target_table_name}")
            self.config_parser.print_log_message('ERROR', e)
            return []

    def fetch_mapping_target_constraints(self, target_schema_name, target_table_name):
        query = f"""
            SELECT id, constraint_name, constraint_type, constraint_def
            FROM "{self.protocol_schema}"."mapping_target_constraints"
            WHERE target_schema_name = %s AND target_table_name = %s
        """
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, (target_schema_name, target_table_name))
            rows = cursor.fetchall()
            cursor.close()
            return [{'id': row[0], 'constraint_name': row[1], 'constraint_type': row[2], 'constraint_def': row[3]} for row in rows]
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: fetch_mapping_target_constraints: Error fetching for {target_schema_name}.{target_table_name}")
            self.config_parser.print_log_message('ERROR', e)
            return []

    def fetch_all_mapping_target_constraints(self):
        query = f"""
            SELECT id, target_schema_name, target_table_name, constraint_name, constraint_type, constraint_def
            FROM "{self.protocol_schema}"."mapping_target_constraints"
        """
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            return [{'id': row[0], 'target_schema_name': row[1], 'target_table_name': row[2], 'constraint_name': row[3], 'constraint_type': row[4], 'constraint_def': row[5]} for row in rows]
        except Exception as e:
            self.config_parser.print_log_message('ERROR', "migrator_tables: fetch_all_mapping_target_constraints: Error fetching all mapping target constraints")
            self.config_parser.print_log_message('ERROR', e)
            return []

    def update_mapping_target_index_status(self, settings):
        func_run_id = uuid.uuid4()
        row_id = settings['row_id']
        success = settings['success']
        message = settings['message']

        query = f"""
            UPDATE "{self.protocol_schema}"."mapping_target_indexes"
            SET success = %s,
            message = %s
            WHERE id = %s
            RETURNING *
        """
        params = (success, message, row_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_mapping_target_index_status: ({func_run_id}): Error updating status for index row_id {row_id}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_mapping_target_index_status: ({func_run_id}): Exception: {e}")
            raise

    def update_mapping_target_constraint_status(self, settings):
        func_run_id = uuid.uuid4()
        row_id = settings['row_id']
        success = settings['success']
        message = settings['message']

        query = f"""
            UPDATE "{self.protocol_schema}"."mapping_target_constraints"
            SET success = %s,
            message = %s
            WHERE id = %s
            RETURNING *
        """
        params = (success, message, row_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_mapping_target_constraint_status: ({func_run_id}): Error updating status for constraint row_id {row_id}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_mapping_target_constraint_status: ({func_run_id}): Exception: {e}")
            raise

    def update_mapping_target_index_drop_status(self, settings):
        func_run_id = uuid.uuid4()
        row_id = settings['row_id']
        dropped = settings['dropped']
        message = settings['message']

        query = f"""
            UPDATE "{self.protocol_schema}"."mapping_target_indexes"
            SET dropped = %s,
            message = %s
            WHERE id = %s
            RETURNING *
        """
        params = (dropped, message, row_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_mapping_target_index_drop_status: ({func_run_id}): Error updating dropped status for index row_id {row_id}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_mapping_target_index_drop_status: ({func_run_id}): Exception: {e}")
            raise

    def update_mapping_target_constraint_drop_status(self, settings):
        func_run_id = uuid.uuid4()
        row_id = settings['row_id']
        dropped = settings['dropped']
        message = settings['message']

        query = f"""
            UPDATE "{self.protocol_schema}"."mapping_target_constraints"
            SET dropped = %s,
            message = %s
            WHERE id = %s
            RETURNING *
        """
        params = (dropped, message, row_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_mapping_target_constraint_drop_status: ({func_run_id}): Error updating dropped status for constraint row_id {row_id}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_mapping_target_constraint_drop_status: ({func_run_id}): Exception: {e}")
            raise

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
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: create_table_for_main: Table {table_name} created in schema {self.protocol_schema}")

    def insert_main(self, settings):
        task_name = settings.get('task_name')
        subtask_name = settings.get('subtask_name')
        func_run_id = uuid.uuid4()
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
            self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: insert_main: ({func_run_id}): returned row: {row}")
            main_row = self.decode_main_row(row)
            self.insert_protocol({'object_type': 'main', 'object_name': task_name + ': ' + subtask_name, 'object_action': 'start', 'object_ddl': None, 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': main_row['id']})
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_main: ({func_run_id}): Error inserting task {task_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_main: ({func_run_id}): Error: {e}")
            raise

    def update_protocol_task_started(self, object_type, row_id):
        func_run_id = uuid.uuid4()
        table_name = None
        method_name = f"get_protocol_name_{object_type}"
        if hasattr(self.config_parser, method_name):
            try:
                method = getattr(self.config_parser, method_name)
                table_name = method()
            except BaseException as e:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_protocol_task_started: ({func_run_id}): Error calling {method_name}: {e}")
                return
        else:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_protocol_task_started: ({func_run_id}): Invalid object_type '{object_type}'. Method {method_name} not found.")
            return

        id_column = "sequence_id" if object_type == "sequences" else "id"
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_started = clock_timestamp()
            WHERE {id_column} = %s
            RETURNING *
        """
        params = (row_id,)
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_protocol_task_started: ({func_run_id}): Updating record for table {table_name} with params: {params}, query: {query}")
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            self.protocol_connection.connection.commit()
            row = cursor.fetchone()
            cursor.close()
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_protocol_task_started: ({func_run_id}): Returned row: {row}")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_protocol_task_started: ({func_run_id}): Error updating started status for object type {object_type} {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_protocol_task_started: ({func_run_id}): Exception: {e}")
            raise

    def update_main_status(self, settings):
        task_name = settings.get('task_name')
        subtask_name = settings.get('subtask_name')
        success = settings.get('success')
        message = settings.get('message')
        func_run_id = uuid.uuid4()
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
        params = ('TRUE' if str(success).upper() == 'TRUE' else 'FALSE', message, task_name, subtask_name)
        self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: update_main_status: ({func_run_id}): params: {params}")
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
            self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: update_main_status: ({func_run_id}): returned row: {row}")
            if row:
                main_row = self.decode_main_row(row)
                self.update_protocol({'object_type': 'main', 'object_protocol_id': main_row['id'], 'execution_success': success, 'execution_error_message': message, 'execution_results': None})
            else:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_main_status: ({func_run_id}): Error updating status for task {task_name} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_main_status: ({func_run_id}): Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_main_status: ({func_run_id}): Error updating status for task {task_name} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_main_status: ({func_run_id}): Query: {query}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_main_status: ({func_run_id}): Error: {e}")
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
            target_basic_type TEXT,
            type_comment TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: create_table_for_user_defined_types: Table {table_name} created in schema {self.protocol_schema}")

    def insert_user_defined_type(self, settings):
        func_run_id = uuid.uuid4()
        ## source_schema_name, source_type_name, source_type_sql, target_schema_name, target_type_name, target_type_sql, target_basic_type, type_comment
        source_schema_name = settings['source_schema_name']
        source_type_name = settings['source_type_name']
        source_type_sql = settings['source_type_sql']
        target_schema_name = settings['target_schema_name']
        target_type_name = settings['target_type_name']
        target_type_sql = settings['target_type_sql']
        target_basic_type = settings.get('target_basic_type')
        type_comment = settings['type_comment']

        table_name = self.config_parser.get_protocol_name_user_defined_types()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema_name, source_type_name, source_type_sql, target_schema_name, target_type_name, target_type_sql, target_basic_type, type_comment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema_name, source_type_name, source_type_sql, target_schema_name, target_type_name, target_type_sql, target_basic_type, type_comment)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: insert_user_defined_type: ({func_run_id}): returned row: {row}")
            user_defined_type_row = self.decode_user_defined_type_row(row)
            self.insert_protocol({'object_type': 'user_defined_type', 'object_name': target_type_name, 'object_action': 'create', 'object_ddl': target_type_sql, 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': user_defined_type_row['id']})
            return user_defined_type_row['id']
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_user_defined_type: ({func_run_id}): Error inserting user defined type {target_type_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_user_defined_type: ({func_run_id}): Error: {e}")
            raise

    def update_user_defined_type_status(self, settings):
        row_id = settings.get('row_id')
        success = settings.get('success')
        message = settings.get('message')
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_user_defined_types()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = clock_timestamp(),
            success = %s,
            message = %s
            WHERE id = %s
            RETURNING *
        """
        params = ('TRUE' if str(success).upper() == 'TRUE' else 'FALSE', message.replace('"', ''), row_id)
        self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: update_user_defined_type_status: ({func_run_id}): query: {query}, params: {params}")
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            if row:
                user_defined_type_row = self.decode_user_defined_type_row(row)
                self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: update_user_defined_type_status: ({func_run_id}): returned row: {user_defined_type_row}")
                self.update_protocol({'object_type': 'user_defined_type', 'object_protocol_id': user_defined_type_row['id'], 'execution_success': success, 'execution_error_message': message, 'execution_results': None})
            else:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_user_defined_type_status: ({func_run_id}): Error updating status for user defined type {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_user_defined_type_status: ({func_run_id}): Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_user_defined_type_status: ({func_run_id}): Error updating status for user defined type {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_user_defined_type_status: ({func_run_id}): Query: {query}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_user_defined_type_status: ({func_run_id}): Error: {e}")
            raise

    def fetch_all_user_defined_types(self):
        table_name = self.config_parser.get_protocol_name_user_defined_types()
        query = f"""SELECT * FROM "{self.protocol_schema}"."{table_name}" ORDER BY id"""
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: fetch_all_user_defined_types: returned rows: {len(rows)}")
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
            'target_basic_type': row[7],
            'type_comment': row[8],
            'task_created': row[9],
            'task_started': row[10],
            'task_completed': row[11],
            'success': row[12],
            'message': row[13]
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
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: create_table_for_domains: Table {table_name} created in schema {self.protocol_schema}")

    def insert_domain(self, settings):
        func_run_id = uuid.uuid4()
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
            self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: insert_domain: ({func_run_id}): returned row: {domain_row}")
            self.insert_protocol({'object_type': 'domain', 'object_name': target_domain_name, 'object_action': 'create', 'object_ddl': target_domain_sql, 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': domain_row['id']})
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_domain: ({func_run_id}): Error inserting domain {target_domain_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_domain: ({func_run_id}): Error: {e}")
            raise

    def update_domain_status(self, settings):
        row_id = settings.get('row_id')
        success = settings.get('success')
        message = settings.get('message')
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_domains()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = clock_timestamp(),
            success = %s,
            message = %s
            WHERE id = %s
            RETURNING *
        """
        self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: update_domain_status: ({func_run_id}): Query: {query}")
        params = ('TRUE' if success else 'FALSE', message, row_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            if row:
                domain_row = self.decode_domain_row(row)
                self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: update_domain_status: ({func_run_id}): returned row: {domain_row}")
                self.update_protocol({'object_type': 'domain', 'object_protocol_id': domain_row['id'], 'execution_success': success, 'execution_error_message': message, 'execution_results': None})
            else:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_domain_status: ({func_run_id}): Error updating status for domain {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_domain_status: ({func_run_id}): Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_domain_status: ({func_run_id}): Error updating status for domain {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_domain_status: ({func_run_id}): Query: {query}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_domain_status: ({func_run_id}): Error: {e}")
            raise

    def fetch_all_domains(self, settings=None):
        if settings is None: settings = {}
        domain_owner = settings.get('domain_owner')
        domain_name = settings.get('domain_name')
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

    def get_domain_details(self, settings):
        source_schema_name = settings.get('source_schema_name')
        source_domain_name = settings.get('source_domain_name')
        domain_row = self.fetch_all_domains({'domain_owner': source_schema_name, 'domain_name': source_domain_name})
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
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: create_table_for_default_values: Table {table_name} created in schema {self.protocol_schema}")

    def insert_default_value(self, settings):
        func_run_id = uuid.uuid4()
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

            default_value_row = self.decode_default_value_row(row)
            self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: insert_default_value: ({func_run_id}): returned row: {default_value_row}")
            self.insert_protocol({'object_type': 'default_value', 'object_name': default_value_name, 'object_action': 'create', 'object_ddl': None, 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': default_value_row['id']})
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_default_value: ({func_run_id}): Error inserting default value {default_value_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_default_value: ({func_run_id}): Error: {e}")
            raise

    def update_default_value_status(self, settings):
        row_id = settings.get('row_id')
        success = settings.get('success')
        message = settings.get('message')
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_default_values()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_completed = clock_timestamp(),
            success = %s,
            message = %s
            WHERE id = %s
            RETURNING *
        """
        params = ('TRUE' if str(success).upper() == 'TRUE' else 'FALSE', message, row_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            if row:
                default_value_row = self.decode_default_value_row(row)
                self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_default_value_status: ({func_run_id}): returned row: {default_value_row}")
                self.update_protocol({'object_type': 'default_value', 'object_protocol_id': default_value_row['id'], 'execution_success': success, 'execution_error_message': message, 'execution_results': None})
            else:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_default_value_status: ({func_run_id}): Error updating status for default value {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_default_value_status: ({func_run_id}): Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_default_value_status: ({func_run_id}): Error updating status for default value {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_default_value_status: ({func_run_id}): Query: {query}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_default_value_status: ({func_run_id}): Exception: {e}")
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

    def get_default_value_details(self, settings):
        default_value_name = settings.get('default_value_name')
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
            target_schema_name TEXT,
            target_table_name TEXT,
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
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: create_table_for_target_columns_alterations: Table {table_name} created in schema {self.protocol_schema}")

    def insert_target_column_alteration(self, settings):
        func_run_id = uuid.uuid4()
        ## target_schema_name, target_table_name, target_column, original_data_type, altered_data_type
        target_schema_name = settings['target_schema_name']
        target_table_name = settings['target_table_name']
        target_column = settings['target_column']
        reason = settings['reason'] if 'reason' in settings else ''
        original_data_type = settings['original_data_type']
        altered_data_type = settings['altered_data_type']

        table_name = self.config_parser.get_protocol_name_target_columns_alterations()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (target_schema_name, target_table_name, target_column, reason, original_data_type, altered_data_type)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (target_schema_name, target_table_name, target_column, reason, original_data_type, altered_data_type)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            target_column_alteration_row = self.decode_target_column_alteration_row(row)
            self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: insert_target_column_alteration: ({func_run_id}): returned row: {target_column_alteration_row}")
            self.insert_protocol({'object_type': 'target_column_alteration', 'object_name': target_table_name + '.' + target_column, 'object_action': 'alter', 'object_ddl': None, 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': target_column_alteration_row['id']})
            return target_column_alteration_row['id']
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_target_column_alteration: ({func_run_id}): Error inserting target column alteration {target_table_name}.{target_column} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_target_column_alteration: ({func_run_id}): Exception: {e}")
            raise

    def decode_target_column_alteration_row(self, row):
        return {
            'id': row[0],
            'target_schema_name': row[1],
            'target_table_name': row[2],
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
                    ON c.referenced_table_name = a.target_table_name
                    AND replace(c.referenced_columns,'"','') = a.target_column
                    WHERE c.constraint_type = 'FOREIGN KEY'
                    AND c.target_schema_name = '{settings['target_schema_name']}'
                    AND c.target_table_name = '{settings['target_table_name']}'
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
            source_schema_name TEXT,
            source_table_name TEXT,
            source_table_id INTEGER,
            source_table_rows_all INTEGER,
            source_table_rows_limited INTEGER,
            worker_id TEXT,
            target_schema_name TEXT,
            target_table_name TEXT,
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
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: create_table_for_data_migration: Table {table_name} created in schema {self.protocol_schema}")
        self.protocol_connection.execute_query(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_data_migration_unique1
            ON "{self.protocol_schema}"."{self.config_parser.get_protocol_name_data_migration()}"
            (source_schema_name, source_table_name, target_schema_name, target_table_name)
        """)
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: create_table_for_data_migration: Unique index created for table {table_name}.")

    def create_table_for_batches_stats(self):
        table_name = self.config_parser.get_protocol_name_batches_stats()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema_name TEXT,
            source_table_name TEXT,
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
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: create_table_for_batches_stats: Table {table_name} created in schema {self.protocol_schema}.")

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
                    source_schema_name TEXT,
                    source_table_name TEXT,
                    target_schema_name TEXT,
                    target_table_name TEXT,
                    source_table_rows_all BIGINT,
                    source_table_rows_limited BIGINT,
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
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: create_table_for_data_chunks: Table {table_name} created successfully.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: create_table_for_data_chunks: Error creating table {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: create_table_for_data_chunks: Exception: {e}")
            raise

    def insert_data_chunk(self, settings):
        ## worker_id, source_table_id, source_schema_name, source_table_name, target_schema_name, target_table_name,
        ## source_table_rows, target_table_rows, chunk_number, chunk_size, migration_limitation,
        ## chunk_start, chunk_end, inserted_rows, batch_size, total_batches
        worker_id = settings['worker_id']
        source_table_id = settings['source_table_id']
        source_schema_name = settings['source_schema_name']
        source_table_name = settings['source_table_name']
        target_schema_name = settings['target_schema_name']
        target_table_name = settings['target_table_name']
        source_table_rows_all = settings['source_table_rows_all']
        source_table_rows_limited = settings['source_table_rows_limited']
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
            (worker_id, source_table_id, source_schema_name, source_table_name,
            target_schema_name, target_table_name, source_table_rows_all, source_table_rows_limited, target_table_rows,
            chunk_number, chunk_size, migration_limitation,
            chunk_start, chunk_end, order_by_clause, inserted_rows, batch_size, total_batches,
            task_started, task_completed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (str(worker_id), source_table_id, source_schema_name,
                  source_table_name, target_schema_name,
                  target_table_name,
                  source_table_rows_all,
                  source_table_rows_limited,
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
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_data_chunk: Returned row: {data_chunk_row}")
            self.insert_protocol({'object_type': 'data_chunk', 'object_name': f'{target_table_name}.{chunk_number}', 'object_action': 'create', 'object_ddl': None, 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': data_chunk_row['id']})
            return data_chunk_row['id']
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_data_chunk: Error inserting data chunk for {target_table_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_data_chunk: Exception: {e}")
            raise

    def decode_data_chunk_row(self, row):
        return {
            'id': row[0],
            'worker_id': row[1],
            'source_table_id': row[2],
            'source_schema_name': row[3],
            'source_table_name': row[4],
            'target_schema_name': row[5],
            'target_table_name': row[6],
            'source_table_rows_all': row[7],
            'source_table_rows_limited': row[8],
            'target_table_rows': row[9],
            'chunk_number': row[10],
            'chunk_size': row[10],
            'migration_limitation': row[11],
            'chunk_start': row[12],
            'chunk_end': row[13],
            'order_by_clause': row[14],
            'inserted_rows': row[15],
            'batch_size': row[17],
            'total_batches': row[18],
            'task_started': row[19],
            'task_completed': row[20]
        }

    def insert_batches_stats(self, settings):
        ## source_schema_name, source_table_name, source_table_id, batch_number, batch_start, batch_end, batch_rows, batch_seconds
        source_schema_name = settings['source_schema_name']
        source_table_name = settings['source_table_name']
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
            (source_schema_name, source_table_name, source_table_id, chunk_number, batch_number,
            batch_start, batch_end, batch_rows, batch_seconds, worker_id,
            reading_seconds, transforming_seconds, writing_seconds)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema_name, source_table_name, source_table_id, chunk_number, batch_number,
                  batch_start, batch_end, batch_rows, batch_seconds, str(worker_id),
                  reading_seconds, transforming_seconds, writing_seconds)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            self.protocol_connection.connection.commit()  # Commit the transaction
            cursor.close()

            self.config_parser.print_log_message( 'DEBUG3', f"migrator_tables: insert_batches_stats: Returned row: {row}")
            return row[0]  # Return the ID of the inserted row
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_batches_stats: Error inserting batches stats for {source_table_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_batches_stats: Exception: {e}")
            raise

    def insert_data_migration(self, settings):
        func_run_id = uuid.uuid4()
        ## source_schema_name, source_table_name, source_table_id, source_table_rows, worker_id, target_schema_name, target_table_name, target_table_rows
        source_schema_name = settings['source_schema_name']
        source_table_name = settings['source_table_name']
        source_table_id = settings['source_table_id']
        source_table_rows_all = settings['source_table_rows_all']
        source_table_rows_limited = settings['source_table_rows_limited']
        worker_id = settings['worker_id']
        target_schema_name = settings['target_schema_name']
        target_table_name = settings['target_table_name']
        target_table_rows = settings['target_table_rows']

        table_name = self.config_parser.get_protocol_name_data_migration()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema_name, source_table_name, source_table_id, source_table_rows_all, source_table_rows_limited, worker_id, target_schema_name, target_table_name, target_table_rows)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_schema_name, source_table_name, target_schema_name, target_table_name)
            DO UPDATE SET source_table_rows_all = EXCLUDED.source_table_rows_all,
            source_table_rows_limited = EXCLUDED.source_table_rows_limited,
            target_table_rows = EXCLUDED.target_table_rows,
            task_created = clock_timestamp(),
            worker_id = EXCLUDED.worker_id,
            success = NULL,
            message = NULL
            RETURNING *
        """
        params = (source_schema_name, source_table_name, source_table_id, source_table_rows_all, source_table_rows_limited, str(worker_id), target_schema_name, target_table_name, target_table_rows)
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_data_migration: ({func_run_id}): Inserting data migration record for table {target_table_name} with params: {params}")
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            data_migration_row = self.decode_data_migration_row(row)
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_data_migration: ({func_run_id}): Returned row: {data_migration_row}")
            self.insert_protocol({'object_type': 'data_migration', 'object_name': target_table_name, 'object_action': 'create', 'object_ddl': None, 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': data_migration_row['id']})
            return data_migration_row['id']
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_data_migration: ({func_run_id}): Error inserting data migration {target_table_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_data_migration: ({func_run_id}): Exception: {e}")
            raise

    def update_data_migration_started(self, row_id):
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_data_migration()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET task_started = clock_timestamp()
            WHERE id = %s
            RETURNING *
        """
        params = (row_id,)
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_data_migration_started: ({func_run_id}): Updating data migration record for table {table_name} with params: {params}, query: {query}")
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            self.protocol_connection.connection.commit()
            row = cursor.fetchone()
            cursor.close()

            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_data_migration_started: ({func_run_id}): Returned row: {row}")
            # if row:
            #     data_migration_row = self.decode_data_migration_row(row)
            #     self.update_protocol('data_migration', data_migration_row['id'], None, None, None)
            # else:
            #     self.config_parser.print_log_message('ERROR', f"migrator_tables: update_data_migration_started: Error updating started status for data migration {row_id} in {table_name}.")
            #     self.config_parser.print_log_message('ERROR', f"migrator_tables: update_data_migration_started: Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_data_migration_started: ({func_run_id}): Error updating started status for data migration {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_data_migration_started: ({func_run_id}): Query: {query}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_data_migration_started: ({func_run_id}): Exception: {e}")
            raise

    def update_data_migration_status(self, settings):
        func_run_id = uuid.uuid4()
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
                self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_data_migration_status: ({func_run_id}): Returned row: {data_migration_row}")
                self.update_protocol({'object_type': 'data_migration', 'object_protocol_id': data_migration_row['id'], 'execution_success': success, 'execution_error_message': message, 'execution_results': 'source rows: ' + str(data_migration_row['source_table_rows_limited']) + ', target rows: ' + str(target_table_rows)})
            else:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_data_migration_status: ({func_run_id}): Error updating status for data migration {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_data_migration_status: ({func_run_id}): Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_data_migration_status: ({func_run_id}): Error updating status for data migration {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_data_migration_status: ({func_run_id}): Query: {query}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_data_migration_status: ({func_run_id}): Exception: {e}")
            raise

    def update_data_migration_rows(self, settings):
        func_run_id = uuid.uuid4()
        row_id = settings['row_id']
        source_table_rows_all = settings.get('source_table_rows_all')
        source_table_rows_limited = settings['source_table_rows_limited']
        target_table_rows = settings['target_table_rows']
        table_name = self.config_parser.get_protocol_name_data_migration()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET source_table_rows_all = COALESCE(%s, source_table_rows_all),
            source_table_rows_limited = %s,
            target_table_rows = %s
            WHERE id = %s
            RETURNING *
        """
        params = (source_table_rows_all, source_table_rows_limited, target_table_rows, row_id)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            if row:
                data_migration_row = self.decode_data_migration_row(row)
                self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_data_migration_rows: ({func_run_id}): Returned row: {data_migration_row}")
                self.update_protocol({'object_type': 'data_migration', 'object_protocol_id': data_migration_row['id'], 'execution_success': None, 'execution_error_message': None, 'execution_results': None})
            else:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_data_migration_rows: ({func_run_id}): Error updating rows for data migration {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_data_migration_rows: ({func_run_id}): Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_data_migration_rows: ({func_run_id}): Error updating rows for data migration {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_data_migration_rows: ({func_run_id}): Query: {query}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_data_migration_rows: ({func_run_id}): Exception: {e}")
            raise

    def decode_data_migration_row(self, row):
        return {
            'id': row[0],
            'source_schema_name': row[1],
            'source_table_name': row[2],
            'source_table_id': row[3],
            'source_table_rows_all': row[4],
            'source_table_rows_limited': row[5],
            'worker_id': row[6],
            'target_schema_name': row[7],
            'target_table_name': row[8],
            'target_table_rows': row[9],
            'task_created': row[10],
            'task_started': row[11],
            'task_completed': row[12],
            'success': row[13],
            'message': row[14],
            'batch_count': row[15],
            'shortest_batch_seconds': row[16],
            'longest_batch_seconds': row[17],
            'average_batch_seconds': row[18],
        }

    def create_table_for_pk_ranges(self):
        table_name = self.config_parser.get_protocol_name_pk_ranges()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema_name TEXT,
            source_table_name TEXT,
            source_table_id INTEGER,
            worker_id TEXT,
            pk_columns TEXT,
            batch_start TEXT,
            batch_end TEXT,
            row_count BIGINT
            )
        """)
        self.config_parser.print_log_message('DEBUG', f"migrator_tables: create_table_for_pk_ranges: Created table {table_name} for PK ranges.")

    def insert_pk_ranges(self, settings):
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_pk_ranges()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema_name, source_table_name, source_table_id, worker_id, pk_columns, batch_start, batch_end, row_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (settings.get('source_schema_name'), settings.get('source_table_name'), settings.get('source_table_id'),
                  str(settings.get('worker_id')), settings.get('pk_columns'),
                  settings.get('batch_start'), settings.get('batch_end'), settings.get('row_count'))
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            data_migration_row = self.decode_pk_ranges_row(row)
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_pk_ranges: ({func_run_id}): Returned row: {data_migration_row}")
            self.insert_protocol({'object_type': 'data_migration', 'object_name': settings.get('source_table_name'), 'object_action': 'pk_range', 'object_ddl': f"PK range: {settings.get('batch_start')} - {settings.get('batch_end')} / {settings.get('row_count')}", 'execution_timestamp': None, 'execution_success': True, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': data_migration_row['id']})
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_pk_ranges: ({func_run_id}): Error inserting PK ranges {settings.get('source_table_name')} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_pk_ranges: ({func_run_id}): Exception: {e}")
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
            'source_schema_name': row[1],
            'source_table_name': row[2],
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
        self.config_parser.print_log_message('DEBUG', f"migrator_tables: create_table_for_new_objects: Created protocol table {table_name} for new objects.")

    def create_table_for_tables(self):
        table_name = self.config_parser.get_protocol_name_tables()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema_name TEXT,
            source_table_name TEXT,
            source_table_id INTEGER,
            source_columns TEXT,
            source_table_rows_all BIGINT,
            source_table_rows_limited BIGINT,
            source_table_description TEXT,
            source_table_sql TEXT,
            target_schema_name TEXT,
            target_table_name TEXT,
            target_alias_name TEXT,
            target_columns TEXT,
            target_table_rows BIGINT,
            target_table_sql TEXT,
            table_comment TEXT,
            create_partitions_sql TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG', f"migrator_tables: create_table_for_tables: Created protocol table {table_name} for tables.")

    def create_table_for_source_table_partitioning(self):
        table_name = self.config_parser.get_protocol_name_source_table_partitioning()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema_name TEXT,
            source_table_name TEXT,
            source_table_id INTEGER,
            source_table_partitioning_level INTEGER,
            source_partition_columns TEXT,
            source_partition_ranges TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG', f"migrator_tables: create_table_for_source_table_partitioning: Created protocol table {table_name} for source table partitioning.")

    def create_table_for_target_table_partitioning(self):
        table_name = self.config_parser.get_protocol_name_target_table_partitioning()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            target_schema_name TEXT,
            target_table_name TEXT,
            target_table_id INTEGER,
            target_table_partitioning_level INTEGER,
            target_partition_columns TEXT,
            target_partition_ranges TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG', f"migrator_tables: create_table_for_target_table_partitioning: Created protocol table {table_name} for target table partitioning.")

    def create_table_for_columns(self):
        table_name = self.config_parser.get_protocol_name_columns()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema_name TEXT,
            source_table_name TEXT,
            source_table_id INTEGER,
            source_column_name TEXT,
            source_column_id INTEGER,
            source_column_data_type TEXT,
            source_column_is_nullable TEXT,
            source_column_is_primary_key TEXT,
            source_column_is_identity TEXT,
            source_column_default_name TEXT,
            source_column_default_value TEXT,
            source_column_replaced_default_value TEXT,
            source_column_character_maximum_length TEXT,
            source_column_numeric_precision TEXT,
            source_column_numeric_scale TEXT,
            source_column_basic_data_type TEXT,
            source_column_basic_character_maximum_length TEXT,
            source_column_basic_numeric_precision TEXT,
            source_column_basic_numeric_scale TEXT,
            source_column_basic_column_type TEXT,
            source_column_is_generated_virtual TEXT,
            source_column_is_generated_stored TEXT,
            source_column_generation_expression TEXT,
            source_column_stripped_generation_expression TEXT,
            source_column_udt_schema TEXT,
            source_column_udt_name TEXT,
            source_column_domain_schema TEXT,
            source_column_domain_name TEXT,
            source_column_description TEXT,
            source_column_sql TEXT,
            target_schema_name TEXT,
            target_table_name TEXT,
            target_alias_name TEXT,
            target_table_id INTEGER,
            target_column_name TEXT,
            target_column_id INTEGER,
            target_column_data_type TEXT,
            target_column_description TEXT,
            target_column_sql TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG', f"migrator_tables: create_table_for_columns: Created protocol table {table_name} for columns.")


    def create_table_for_data_sources(self):
        table_name = self.config_parser.get_protocol_name_data_sources()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema_name TEXT,
            source_table_name TEXT,
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
        self.config_parser.print_log_message('DEBUG', f"migrator_tables: create_table_for_data_sources: Created protocol table {table_name} for data sources.")

    def insert_data_source(self, settings):
        func_run_id = uuid.uuid4()
        ## source_schema_name, source_table_name, source_table_id, clob_columns, blob_columns, file_name, format_options
        source_schema_name = settings['source_schema_name']
        source_table_name = settings['source_table_name']
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
            (source_schema_name, source_table_name, source_table_id, lob_columns,
            file_name, file_size, file_lines, file_found, converted_file_name, format_options)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema_name, source_table_name, source_table_id,
                  lob_columns, file_name, file_size, file_lines, file_found, converted_file_name, format_options)
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_data_source: ({func_run_id}): Query: {query} / Params: {params}")

        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            data_source_row = self.decode_data_source_row(row)
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_data_source: ({func_run_id}): Returned row: {data_source_row}")
            self.insert_protocol({'object_type': 'data_source', 'object_name': f'{source_table_name} ({file_name})', 'object_action': 'create', 'object_ddl': None, 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': data_source_row['id']})
            return data_source_row['id']
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_data_source: ({func_run_id}): Error inserting data source {source_table_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_data_source: ({func_run_id}): Exception: {e}")
            raise

    def get_data_sources(self, settings):
        source_schema_name = settings.get('source_schema_name')
        source_table_name = settings.get('source_table_name')
        table_name = self.config_parser.get_protocol_name_data_sources()
        query = f"""
            SELECT * FROM "{self.protocol_schema}"."{table_name}"
            WHERE source_schema_name = %s AND source_table_name = %s
        """
        params = (source_schema_name, source_table_name)
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
            'source_schema_name': row[1],
            'source_table_name': row[2],
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

    def update_status_data_source(self, settings):
        row_id = settings.get('row_id')
        success = settings.get('success')
        message = settings.get('message')
        func_run_id = uuid.uuid4()
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
                self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_status_data_source: ({func_run_id}): Returned row: {data_source_row}")
                self.update_protocol({'object_type': 'data_source', 'object_protocol_id': data_source_row['id'], 'execution_success': success, 'execution_error_message': message, 'execution_results': None})
            else:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_status_data_source: ({func_run_id}): Error updating status for data source {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_status_data_source: ({func_run_id}): Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_status_data_source: ({func_run_id}): Error updating status for data source {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_status_data_source: ({func_run_id}): Query: {query}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_status_data_source: ({func_run_id}): Exception: {e}")
            raise

    def create_table_for_indexes(self):
        table_name = self.config_parser.get_protocol_name_indexes()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema_name TEXT,
            source_table_name TEXT,
            source_table_id INTEGER,
            index_owner TEXT,
            index_name TEXT,
            index_type VARCHAR(30),
            target_schema_name TEXT,
            target_table_name TEXT,
            target_alias_name TEXT,
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
        self.config_parser.print_log_message('DEBUG', f"migrator_tables: create_table_for_indexes: Created protocol table {table_name} for indexes.")

    def create_table_for_funcprocs(self):
        table_name = self.config_parser.get_protocol_name_funcprocs()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema_name TEXT,
            source_funcproc_name TEXT,
            source_funcproc_id INTEGER,
            source_funcproc_sql TEXT,
            target_schema_name TEXT,
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
        self.config_parser.print_log_message('DEBUG', f"migrator_tables: create_table_for_funcprocs: Created protocol table {table_name} for functions/procedures.")

    def create_table_for_sequences(self):
        table_name = self.config_parser.get_protocol_name_sequences()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (sequence_id INTEGER,
            source_schema_name TEXT,
            source_table_name TEXT,
            source_column_name TEXT,
            source_sequence_name TEXT,
            source_sequence_sql TEXT,
            source_start_value BIGINT,
            source_increment_by BIGINT,
            source_minvalue BIGINT,
            source_maxvalue BIGINT,
            source_cache BIGINT,
            source_is_cycled BOOLEAN,
            source_sequence_comment TEXT,
            target_schema_name TEXT,
            target_table_name TEXT,
            target_column_name TEXT,
            target_sequence_name TEXT,
            target_sequence_sql TEXT,
            target_sequence_comment TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG', f"migrator_tables: create_table_for_sequences: Created protocol table {table_name} for sequences.")

    def create_table_for_aliases(self):
        table_name = self.config_parser.get_protocol_name_aliases()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema_name TEXT,
            source_alias_name TEXT,
            source_alias_id INTEGER,
            source_alias_sql TEXT,
            source_referenced_schema_name TEXT,
            source_referenced_table_name TEXT,
            source_referenced_column_name TEXT,
            source_alias_comment TEXT,
            target_schema_name TEXT,
            target_alias_name TEXT,
            alias_target_type TEXT,
            target_referenced_schema_name TEXT,
            target_referenced_table_name TEXT,
            target_referenced_column_name TEXT,
            target_alias_sql TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG', f"migrator_tables: create_table_for_aliases: Created protocol table {table_name} for aliases.")

    def create_table_for_triggers(self):
        table_name = self.config_parser.get_protocol_name_triggers()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema_name TEXT,
            source_table_name TEXT,
            source_table_id INTEGER,
            target_schema_name TEXT,
            target_table_name TEXT,
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
        self.config_parser.print_log_message('DEBUG', f"migrator_tables: create_table_for_triggers: Created protocol table {table_name} for triggers.")

    def create_table_for_views(self):
        table_name = self.config_parser.get_protocol_name_views()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_schema_name TEXT,
            source_view_name TEXT,
            source_view_id INTEGER,
            source_view_sql TEXT,
            target_schema_name TEXT,
            target_view_name TEXT,
            target_view_alias TEXT,
            target_view_sql TEXT,
            alias_view BOOLEAN default FALSE,
            view_comment TEXT,
            task_created TIMESTAMP DEFAULT clock_timestamp(),
            task_started TIMESTAMP,
            task_completed TIMESTAMP,
            success BOOLEAN,
            message TEXT
            )
        """)
        self.config_parser.print_log_message('DEBUG', f"migrator_tables: create_table_for_views: Created protocol table {table_name} for views.")

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
            'source_schema_name': row[1],
            'source_table_name': row[2],
            'source_table_id': row[3],
            'source_columns': json.loads(row[4]),
            'source_table_rows_all': row[5],
            'source_table_rows_limited': row[6],
            'source_table_description': row[7],
            'source_table_sql': row[8],
            'target_schema_name': row[9],
            'target_table_name': row[10],
            'target_alias_name': row[11],
            'target_columns': json.loads(row[12]),
            'target_table_rows': row[13],
            'target_table_sql': row[14],
            'table_comment': row[15],
            'create_partitions_sql': row[16],
        }

    def decode_index_row(self, row):
        return {
            'id': row[0],
            'source_schema_name': row[1],
            'source_table_name': row[2],
            'source_table_id': row[3],
            'index_owner': row[4],
            'index_name': row[5],
            'index_type': row[6],
            'target_schema_name': row[7],
            'target_table_name': row[8],
            'target_alias_name': row[9],
            'index_sql': row[10],
            'index_columns': row[11],
            'index_comment': row[12],
            'is_function_based': row[13]
        }

    def decode_funcproc_row(self, row):
        return {
            'id': row[0],
            'source_schema_name': row[1],
            'source_funcproc_name': row[2],
            'source_funcproc_id': row[3],
            'source_funcproc_sql': row[4],
            'target_schema_name': row[5],
            'target_funcproc_name': row[6],
            'target_funcproc_sql': row[7],
            'funcproc_comment': row[8]
        }

    def decode_sequence_row(self, row):
        return {
            'sequence_id': row[0],
            'source_schema_name': row[1],
            'source_table_name': row[2],
            'source_column_name': row[3],
            'source_sequence_name': row[4],
            'source_sequence_sql': row[5],
            'source_start_value': row[6],
            'source_increment_by': row[7],
            'source_minvalue': row[8],
            'source_maxvalue': row[9],
            'source_cache': row[10],
            'source_is_cycled': row[11],
            'source_sequence_comment': row[12],
            'target_schema_name': row[13],
            'target_table_name': row[14],
            'target_column_name': row[15],
            'target_sequence_name': row[16],
            'target_sequence_sql': row[17],
            'target_sequence_comment': row[18]
        }

    def decode_trigger_row(self, row):
        return {
            'id': row[0],
            'source_schema_name': row[1],
            'source_table_name': row[2],
            'source_table_id': row[3],
            'target_schema_name': row[4],
            'target_table_name': row[5],
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
            'source_schema_name': row[1],
            'source_view_name': row[2],
            'source_view_id': row[3],
            'source_view_sql': row[4],
            'target_schema_name': row[5],
            'target_view_name': row[6],
            'target_view_alias': row[7],
            'target_view_sql': row[8],
            'alias_view': row[9],
            'view_comment': row[10]
        }

    def insert_protocol(self, settings):
        object_type = settings.get('object_type')
        object_name = settings.get('object_name')
        object_action = settings.get('object_action')
        object_ddl = settings.get('object_ddl')
        execution_timestamp = settings.get('execution_timestamp')
        execution_success = settings.get('execution_success')
        execution_error_message = settings.get('execution_error_message')
        row_type = settings.get('row_type')
        execution_results = settings.get('execution_results')
        object_protocol_id = settings.get('object_protocol_id')
        table_name = self.config_parser.get_protocol_name()
        query = f"""
        INSERT INTO "{self.protocol_schema}"."{table_name}"
        (object_type, object_name, object_action, object_ddl, execution_timestamp, execution_success, execution_error_message, row_type, execution_results, object_protocol_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (object_type, object_name, object_action, object_ddl, execution_timestamp, execution_success, execution_error_message, row_type, execution_results, object_protocol_id)
        self.config_parser.print_log_message('DEBUG', f"migrator_tables: insert_protocol: Executing query with params: {params}")
        try:
            self.protocol_connection.execute_query(query, params)
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_protocol: Error inserting info {object_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_protocol: Exception: {e}")
            raise

    def update_protocol(self, settings):
        object_type = settings.get('object_type')
        object_protocol_id = settings.get('object_protocol_id')
        execution_success = settings.get('execution_success')
        execution_error_message = settings.get('execution_error_message')
        execution_results = settings.get('execution_results')
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
        params = ('TRUE' if str(execution_success).upper() == 'TRUE' else 'FALSE', execution_error_message, execution_results, object_protocol_id, object_type)
        self.config_parser.print_log_message('DEBUG', f"migrator_tables: update_protocol: Executing query with params: {params}")
        try:
            self.protocol_connection.execute_query(query, params)
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_protocol: Error updating info {object_protocol_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_protocol: Exception: {e}")
            raise

    def insert_tables(self, settings):
        func_run_id = uuid.uuid4()
        source_schema_name = settings['source_schema_name']
        source_table_name = settings['source_table_name']
        source_table_id = settings['source_table_id']
        source_columns = settings['source_columns']
        source_table_rows_all = settings['source_table_rows_all']
        source_table_rows_limited = settings['source_table_rows_limited']
        source_table_description = settings['source_table_description']
        source_table_sql = settings['source_table_sql']
        target_schema_name = settings['target_schema_name']
        target_table_name = settings['target_table_name']
        target_alias_name = settings.get('target_alias_name', '')
        target_columns = settings['target_columns']
        target_table_rows = settings['target_table_rows']
        target_table_sql = settings['target_table_sql']
        table_comment = settings['table_comment']
        create_partitions_sql = settings['create_partitions_sql']

        table_name = self.config_parser.get_protocol_name_tables()
        source_columns_str = json.dumps(source_columns)
        target_columns_str = json.dumps(target_columns)
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema_name, source_table_name, source_table_id, source_columns, source_table_rows_all, source_table_rows_limited, source_table_description, source_table_sql,
            target_schema_name, target_table_name, target_alias_name, target_columns, target_table_rows, target_table_sql, table_comment,
            create_partitions_sql)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (source_schema_name, source_table_name, source_table_id, source_columns_str, source_table_rows_all, source_table_rows_limited, source_table_description, source_table_sql,
                  target_schema_name, target_table_name, target_alias_name, target_columns_str, target_table_rows, target_table_sql, table_comment,
                  create_partitions_sql)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            table_row = self.decode_table_row(row)
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_tables: ({func_run_id}): Returned row: {table_row}")
            self.insert_protocol({'object_type': 'table', 'object_name': target_table_name, 'object_action': 'create', 'object_ddl': target_table_sql, 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': table_row['id']})
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_tables: ({func_run_id}): Error inserting table info {source_table_name} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_tables: ({func_run_id}): Settings: {settings}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_tables: ({func_run_id}): Exception: {e}")
            raise

    def update_table_status(self, settings):
        row_id = settings.get('row_id')
        success = settings.get('success')
        message = settings.get('message')
        func_run_id = uuid.uuid4()
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
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_table_status: ({func_run_id}): Executing query with params: {params}")
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            if row:
                table_row = self.decode_table_row(row)
                self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_table_status: ({func_run_id}): Returned row: {table_row}")
                self.update_protocol({'object_type': 'table', 'object_protocol_id': table_row['id'], 'execution_success': success, 'execution_error_message': message, 'execution_results': None})
            else:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_table_status: ({func_run_id}): Error updating status for table {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_table_status: ({func_run_id}): Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_table_status: ({func_run_id}): Error updating status for table {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_table_status: ({func_run_id}): Query: {query}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_table_status: ({func_run_id}): Exception: {e}")
            raise

    def update_table_rows_counts(self, settings):
        row_id = settings.get('row_id')
        source_table_rows_all = settings.get('source_table_rows_all')
        source_table_rows_limited = settings.get('source_table_rows_limited')
        target_table_rows = settings.get('target_table_rows')
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_tables()
        query = f"""
            UPDATE "{self.protocol_schema}"."{table_name}"
            SET source_table_rows_all = COALESCE(%s, source_table_rows_all),
            source_table_rows_limited = COALESCE(%s, source_table_rows_limited),
            target_table_rows = COALESCE(%s, target_table_rows)
            WHERE id = %s
            RETURNING *
        """
        params = (source_table_rows_all, source_table_rows_limited, target_table_rows, row_id)
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_table_rows_counts: ({func_run_id}): Executing query with params: {params}")
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            self.protocol_connection.connection.commit()
            cursor.close()

            if not row:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_table_rows_counts: ({func_run_id}): Error updating rows counts for table {row_id} in {table_name}.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_table_rows_counts: ({func_run_id}): Error updating rows counts for table {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_table_rows_counts: ({func_run_id}): Query: {query}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_table_rows_counts: ({func_run_id}): Exception: {e}")
            raise

    def select_table_by_source(self, settings):
        source_schema_name = settings.get('source_schema_name')
        source_table_name = settings.get('source_table_name')
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_tables()
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: select_table_by_source: ({func_run_id}): Selecting table for source_schema_name={source_schema_name}, source_table_name={source_table_name} in {table_name}.")
        query = f"""
            SELECT * FROM "{self.protocol_schema}"."{table_name}"
            WHERE lower(source_schema_name) = lower(%s) AND lower(source_table_name) = lower(%s)
        """
        params = (source_schema_name, source_table_name)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
            if row:
                return self.decode_table_row(row)
            return None
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: select_table_by_source: ({func_run_id}): Error selecting table for source_schema_name={source_schema_name}, source_table_name={source_table_name} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: select_table_by_source: ({func_run_id}): Query: {query}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: select_table_by_source: ({func_run_id}): Params: {params}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: select_table_by_source: ({func_run_id}): Exception: {e}")
            raise

    def insert_indexes(self, settings):
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_indexes()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema_name, source_table_name, source_table_id, index_owner, index_name, index_type,
            target_schema_name, target_table_name, target_alias_name, index_sql, index_columns, index_comment, is_function_based)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (settings.get('source_schema_name'), settings.get('source_table_name'), settings.get('source_table_id'), settings.get('index_owner'),
                  settings.get('index_name'), settings.get('index_type'), settings.get('target_schema_name'),
                  settings.get('target_table_name'), settings.get('target_alias_name', ''), settings.get('index_sql'), settings.get('index_columns'),
                  settings.get('index_comment'), True if settings.get('is_function_based') == 'YES' else False)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            index_row = self.decode_index_row(row)
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_indexes: ({func_run_id}): Returned row: {index_row}")
            self.insert_protocol({'object_type': 'index', 'object_name': settings.get('index_name'), 'object_action': 'create', 'object_ddl': settings.get('index_sql'), 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': index_row['id']})
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_indexes: ({func_run_id}): Error inserting index info {settings.get('index_name')} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_indexes: ({func_run_id}): Exception: {e}")
            raise

    def update_index_status(self, settings):
        row_id = settings.get('row_id')
        success = settings.get('success')
        message = settings.get('message')
        func_run_id = uuid.uuid4()
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
                self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_index_status: ({func_run_id}): Returned row: {index_row}")
                self.update_protocol({'object_type': 'index', 'object_protocol_id': index_row['id'], 'execution_success': success, 'execution_error_message': message, 'execution_results': None})
            else:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_index_status: ({func_run_id}): Error updating status for index {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_index_status: ({func_run_id}): Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_index_status: ({func_run_id}): Error updating status for index {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_index_status: ({func_run_id}): Query: {query}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_index_status: ({func_run_id}): Exception: {e}")
            raise

    def create_table_for_constraints(self):
        table_name = self.config_parser.get_protocol_name_constraints()
        self.protocol_connection.execute_query(self.drop_table_sql.format(protocol_schema=self.protocol_schema, table_name=table_name))
        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{table_name}"
            (id SERIAL PRIMARY KEY,
            source_table_id INTEGER,
            source_schema_name TEXT,
            source_table_name TEXT,
            target_schema_name TEXT,
            target_table_name TEXT,
            target_alias_name TEXT,
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
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: create_table_for_constraints: Created protocol table {table_name}.")

    def decode_constraint_row(self, row):
        return {
            'id': row[0],
            'source_table_id': row[1],
            'source_schema_name': row[2],
            'source_table_name': row[3],
            'target_schema_name': row[4],
            'target_table_name': row[5],
            'target_alias_name': row[6],
            'constraint_name': row[7],
            'constraint_type': row[8],
            'constraint_owner': row[9],
            'constraint_columns': row[10],
            'referenced_table_schema': row[11],
            'referenced_table_name': row[12],
            'referenced_columns': row[13],
            'constraint_sql': row[14],
            'delete_rule': row[15],
            'update_rule': row[16],
            'constraint_comment': row[17],
            'constraint_status': row[18],
            'task_created': row[19],
            'task_started': row[20],
            'task_completed': row[21],
            'success': row[22],
            'message': row[23]
        }

    def insert_constraint(self, settings):
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_constraints()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_table_id, source_schema_name, source_table_name,
            target_schema_name, target_table_name, target_alias_name, constraint_name,
            constraint_type,
            constraint_owner, constraint_columns,
            referenced_table_schema, referenced_table_name,
            referenced_columns, constraint_sql,
            delete_rule, update_rule, constraint_comment,
            constraint_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (settings['source_table_id'], settings['source_schema_name'], settings['source_table_name'],
                    settings['target_schema_name'], settings['target_table_name'], settings.get('target_alias_name', ''), settings['constraint_name'],
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
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_constraint: ({func_run_id}): Returned row: {constraint_row}")
            self.insert_protocol({'object_type': 'constraint', 'object_name': settings['constraint_name'], 'object_action': 'create', 'object_ddl': settings['constraint_sql'], 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': constraint_row['id']})
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_constraint: ({func_run_id}): Error inserting constraint info {settings['constraint_name']} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_constraint: ({func_run_id}): Query: {query}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_constraint: ({func_run_id}): Exception: {e}")
            raise

    def update_constraint_status(self, settings):
        row_id = settings.get('row_id')
        success = settings.get('success')
        message = settings.get('message')
        func_run_id = uuid.uuid4()
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
                self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_constraint_status: ({func_run_id}): Returned row: {constraint_row}")
                self.update_protocol({'object_type': 'constraint', 'object_protocol_id': constraint_row['id'], 'execution_success': success, 'execution_error_message': message, 'execution_results': None})
            else:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_constraint_status: ({func_run_id}): Error updating status for constraint {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_constraint_status: ({func_run_id}): Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_constraint_status: ({func_run_id}): Error updating status for constraint {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_constraint_status: ({func_run_id}): Query: {query}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_constraint_status: ({func_run_id}): Exception: {e}")
            raise

    def insert_funcprocs(self, settings):
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_funcprocs()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema_name, source_funcproc_name, source_funcproc_id, source_funcproc_sql, target_schema_name, target_funcproc_name, target_funcproc_sql, funcproc_comment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (settings.get('source_schema_name'), settings.get('source_funcproc_name'), settings.get('source_funcproc_id'), settings.get('source_funcproc_sql'), settings.get('target_schema_name'), settings.get('target_funcproc_name'), settings.get('target_funcproc_sql'), settings.get('funcproc_comment'))
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            funcproc_row = self.decode_funcproc_row(row)
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_funcprocs: ({func_run_id}): Returned row: {funcproc_row}")
            self.insert_protocol({'object_type': 'funcproc', 'object_name': settings.get('source_funcproc_name'), 'object_action': 'create', 'object_ddl': settings.get('target_funcproc_sql'), 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': funcproc_row['id']})
            return funcproc_row['id']
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_funcprocs: ({func_run_id}): Error inserting funcproc info {settings.get('source_funcproc_name')} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_funcprocs: ({func_run_id}): Exception: {e}")
            raise

    def update_funcproc_status(self, settings):
        source_funcproc_id = settings.get('source_funcproc_id')
        success = settings.get('success')
        message = settings.get('message')
        func_run_id = uuid.uuid4()
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
                self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_funcproc_status: ({func_run_id}): Returned row: {funcproc_row}")
                self.update_protocol({'object_type': 'funcproc', 'object_protocol_id': funcproc_row['id'], 'execution_success': success, 'execution_error_message': message, 'execution_results': None})
            else:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_funcproc_status: ({func_run_id}): Error updating status for funcproc {source_funcproc_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_funcproc_status: ({func_run_id}): Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_funcproc_status: ({func_run_id}): Error updating status for funcproc {source_funcproc_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_funcproc_status: ({func_run_id}): Query: {query}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_funcproc_status: ({func_run_id}): Exception: {e}")
            raise

    def insert_sequence(self, settings):
        func_run_id = uuid.uuid4()
        protocol_table_name = self.config_parser.get_protocol_name_sequences()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{protocol_table_name}"
            (sequence_id, source_schema_name, source_table_name, source_column_name, source_sequence_name, source_sequence_sql, source_start_value, source_increment_by, source_minvalue, source_maxvalue, source_cache, source_is_cycled, source_sequence_comment, target_schema_name, target_table_name, target_column_name, target_sequence_name, target_sequence_sql, target_sequence_comment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (settings.get('sequence_id'), settings.get('source_schema_name'), settings.get('source_table_name'), settings.get('source_column_name'), settings.get('source_sequence_name'), settings.get('source_sequence_sql'), settings.get('source_start_value'), settings.get('source_increment_by'), settings.get('source_minvalue'), settings.get('source_maxvalue'), settings.get('source_cache'), settings.get('source_is_cycled'), settings.get('source_sequence_comment'), settings.get('target_schema_name'), settings.get('target_table_name'), settings.get('target_column_name'), settings.get('target_sequence_name'), settings.get('target_sequence_sql'), settings.get('target_sequence_comment'))
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            sequence_row = self.decode_sequence_row(row)
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_sequence: ({func_run_id}): Returned row: {sequence_row}")
            self.insert_protocol({'object_type': 'sequence', 'object_name': settings.get('source_sequence_name'), 'object_action': 'create', 'object_ddl': settings.get('source_sequence_sql'), 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': sequence_row['sequence_id']})
            return sequence_row['sequence_id']
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_sequence: ({func_run_id}): Error inserting sequence info {settings.get('source_sequence_name')} into {protocol_table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_sequence: ({func_run_id}): Exception: {e}")
            raise

    def update_sequence_status(self, settings):
        sequence_id = settings.get('sequence_id')
        success = settings.get('success')
        message = settings.get('message')
        func_run_id = uuid.uuid4()
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
                self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_sequence_status: ({func_run_id}): Returned row: {sequence_row}")
                self.update_protocol({'object_type': 'sequence', 'object_protocol_id': sequence_row['sequence_id'], 'execution_success': success, 'execution_error_message': message, 'execution_results': None})
            else:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_sequence_status: ({func_run_id}): Error updating status for sequence {sequence_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_sequence_status: ({func_run_id}): Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_sequence_status: ({func_run_id}): Error updating status for sequence {sequence_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_sequence_status: ({func_run_id}): Query: {query}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_sequence_status: ({func_run_id}): Exception: {e}")
            raise

    def insert_trigger(self, settings):
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_triggers()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema_name, source_table_name, source_table_id, target_schema_name, target_table_name, trigger_id, trigger_name, trigger_event, trigger_new, trigger_old, trigger_source_sql, trigger_target_sql, trigger_comment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (settings.get('source_schema_name'), settings.get('source_table_name'), settings.get('source_table_id'), settings.get('target_schema_name'), settings.get('target_table_name'), settings.get('trigger_id'), settings.get('trigger_name'), settings.get('trigger_event'), settings.get('trigger_new'), settings.get('trigger_old'), settings.get('trigger_source_sql'), settings.get('trigger_target_sql'), settings.get('trigger_comment'))
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            trigger_row = self.decode_trigger_row(row)
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_trigger: ({func_run_id}): Returned row: {trigger_row}")
            self.insert_protocol({'object_type': 'trigger', 'object_name': settings.get('trigger_name'), 'object_action': 'create', 'object_ddl': settings.get('trigger_target_sql'), 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': trigger_row['id']})
            return trigger_row['id']
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_trigger: ({func_run_id}): Error inserting trigger info {settings.get('trigger_name')} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_trigger: ({func_run_id}): Exception: {e}")
            raise

    def update_trigger_status(self, settings):
        row_id = settings.get('row_id')
        success = settings.get('success')
        message = settings.get('message')
        func_run_id = uuid.uuid4()
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
                self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_trigger_status: ({func_run_id}): Returned row: {trigger_row}")
                self.update_protocol({'object_type': 'trigger', 'object_protocol_id': trigger_row['id'], 'execution_success': success, 'execution_error_message': message, 'execution_results': None})
            else:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_trigger_status: ({func_run_id}): Error updating status for trigger {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_trigger_status: ({func_run_id}): Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_trigger_status: ({func_run_id}): Error updating status for trigger {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_trigger_status: ({func_run_id}): Query: {query}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_trigger_status: ({func_run_id}): Exception: {e}")
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
            self.config_parser.print_log_message('ERROR', f"migrator_tables: fetch_all_triggers: Error selecting triggers.")
            self.config_parser.print_log_message('ERROR', e)
            return None

    def insert_view(self, settings):
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_views()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema_name, source_view_name, source_view_id, source_view_sql, target_schema_name, target_view_name, target_view_alias, target_view_sql, alias_view, view_comment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (settings.get('source_schema_name'), settings.get('source_view_name'), settings.get('source_view_id'), settings.get('source_view_sql'), settings.get('target_schema_name'), settings.get('target_view_name'), settings.get('target_view_alias', ''), settings.get('target_view_sql'), settings.get('alias_view', False), settings.get('view_comment'))
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()

            view_row = self.decode_view_row(row)
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_view: ({func_run_id}): Returned row: {view_row}")
            self.insert_protocol({'object_type': 'view', 'object_name': settings.get('source_view_name'), 'object_action': 'create', 'object_ddl': settings.get('target_view_sql'), 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': view_row['id']})
            return view_row['id']
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_view: ({func_run_id}): Error inserting view info {settings.get('source_view_name')} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_view: ({func_run_id}): Exception: {e}")
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
            self.config_parser.print_log_message('ERROR', f"migrator_tables: fetch_all_views: Error selecting views.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: fetch_all_views: Exception: {e}")
            return None

    def update_view_status(self, settings):
        row_id = settings.get('row_id')
        success = settings.get('success')
        message = settings.get('message')
        func_run_id = uuid.uuid4()
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
                self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_view_status: ({func_run_id}): Returned row: {view_row}")
                self.update_protocol({'object_type': 'view', 'object_protocol_id': view_row['id'], 'execution_success': success, 'execution_error_message': message, 'execution_results': None})
                if view_row.get('alias_view') and self.config_parser.get_source_db_type() == 'ibm_db2_zos':
                    source_alias_id = view_row.get('source_view_id') - 1000000
                    cursor = self.protocol_connection.connection.cursor()
                    cursor.execute(f'SELECT id FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_aliases()}" WHERE source_alias_id = %s', (source_alias_id,))
                    alias_rec = cursor.fetchone()
                    cursor.close()
                    if alias_rec:
                        self.update_aliases_status({'row_id': alias_rec[0], 'success': success, 'message': message})
                    else:
                        self.config_parser.print_log_message('ERROR', f"migrator_tables: update_view_status: ({func_run_id}): Could not find alias with source_alias_id {source_alias_id} to update its status.")

            else:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_view_status: ({func_run_id}): Error updating status for view {row_id} in {table_name}.")
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_view_status: ({func_run_id}): Error: No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_view_status: ({func_run_id}): Error updating status for view {row_id} in {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_view_status: ({func_run_id}): Query: {query}")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_view_status: ({func_run_id}): Exception: {e}")
            raise

    def select_primary_key(self, settings):
        source_schema_name = settings.get('source_schema_name')
        source_table_name = settings.get('source_table_name')
        query = f"""
            SELECT
                i.index_columns
            FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_indexes()}" i
            WHERE i.source_schema_name = '{source_schema_name}' AND
                i.source_table_name = '{source_table_name}' AND
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
            self.config_parser.print_log_message('ERROR', f"migrator_tables: select_primary_key: Error selecting primary key for {source_schema_name}.{source_table_name}.")
            self.config_parser.print_log_message('ERROR', e)
            return None

    def select_primary_key_all_columns(self, settings):
        source_schema_name = settings.get('source_schema_name')
        source_table_name = settings.get('source_table_name')
        query = f"""
            SELECT
                *
            FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_indexes()}" i
            WHERE i.source_schema_name = '{source_schema_name}' AND
                i.source_table_name = '{source_table_name}' AND
                i.index_type = 'PRIMARY KEY'
            LIMIT 1
        """
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query)
            index_row = cursor.fetchone()
            cursor.close()
            if index_row:
                return self.decode_index_row(index_row)
            else:
                return None
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: select_primary_key_all_columns: Error selecting primary key SQL for {source_schema_name}.{source_table_name}.")
            self.config_parser.print_log_message('ERROR', e)
            return None

    def print_summary(self, settings):
        objects = settings.get('objects')
        migrator_table_name = settings.get('migrator_table_name')
        additional_columns = settings.get('additional_columns')
        try:
            self.config_parser.print_log_message('INFO', f"migrator_tables: print_summary: {objects} summary:")
            query = f"""SELECT COUNT(*) FROM "{self.protocol_schema}"."{migrator_table_name}" """
            cursor = self.protocol_connection.connection.cursor()
            try:
                cursor.execute(query)
            except psycopg2.errors.UndefinedTable:
                self.protocol_connection.connection.rollback()
                self.config_parser.print_log_message('INFO', f"migrator_tables: print_summary: Table {migrator_table_name} does not exist, skipping.")
                return
            summary = cursor.fetchone()[0]
            if objects.lower() not in ['sequences']:
                self.config_parser.print_log_message('INFO', f"migrator_tables: print_summary:     Found in source: {summary}")
            else:
                self.config_parser.print_log_message('INFO', f"migrator_tables: print_summary:     Found: {summary}")
            if additional_columns:
                columns_count = len(additional_columns.split(','))
                columns_numbers = ', '.join(str(i + 2) for i in range(columns_count))
                query = f"""SELECT COUNT(*), {additional_columns} FROM "{self.protocol_schema}"."{migrator_table_name}" GROUP BY {columns_numbers} ORDER BY {columns_numbers}"""
                cursor.execute(query)
                rows = cursor.fetchall()
                for row in rows:
                    self.config_parser.print_log_message('INFO', f"migrator_tables: print_summary:         {row[1:]}: {row[0]}")

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
                    self.config_parser.print_log_message('INFO', f"migrator_tables: print_summary:     {status}: {row[1]}")
                    if additional_columns:
                        query = f"""SELECT COUNT(*), {additional_columns} FROM "{self.protocol_schema}"."{migrator_table_name}" WHERE success = {row_success} GROUP BY {columns_numbers} ORDER BY {columns_numbers}"""
                        cursor.execute(query)
                        rows = cursor.fetchall()
                        for row in rows:
                            # status = success_description if row[0] else "error" if row[0] is False else "unknown status"
                            self.config_parser.print_log_message('INFO', f"migrator_tables: print_summary:         {row[1:]}: {row[0]}")

            cursor.close()

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: print_summary: Error printing migration summary.")
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
                self.config_parser.print_log_message('INFO', f"migrator_tables: print_main: {status}")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: print_main: Error printing migration summary.")
            self.config_parser.print_log_message('ERROR', e)
            raise



    def generate_mapping_report(self, filename):
        import os
        
        # Ensure it saves as markdown if not explicitly specified as csv, or just output markdown to the given filename.
        # User requested markdown format despite `.csv` extension in sample config.
        # If it ends with .csv, we will write markdown inside it anyway to respect the filename config but fulfill the format requirement.
        
        lines = []
        lines.append("# Mapping Workflow Detailed Report")
        lines.append("")
        
        # Fetch mapped tables
        query_mapped = f"""
            SELECT source_table_name, target_table_name, source_table_rows_all, target_table_rows
            FROM "{self.protocol_schema}"."mapping_tables"
            ORDER BY source_table_name
        """
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query_mapped)
        mapped_tables = cursor.fetchall()
        
        # Fetch unmatched objects
        query_unmatched = f"""
            SELECT object_type, side, parent_object, object_name, row_count
            FROM "{self.protocol_schema}"."mapping_unmatched_objects"
        """
        cursor.execute(query_unmatched)
        unmatched_raw = cursor.fetchall()
        
        unmatched_source_tables = [row for row in unmatched_raw if row[0] == 'table' and row[1] == 'source']
        unmatched_target_tables = [row for row in unmatched_raw if row[0] == 'table' and row[1] == 'target']
        unmatched_columns = [row for row in unmatched_raw if row[0] == 'column']
        
        unmatched_cols_by_table = {}
        for row in unmatched_columns:
            side = row[1]
            parent = row[2]
            col_name = row[3]
            key = (side, parent)
            if key not in unmatched_cols_by_table:
                unmatched_cols_by_table[key] = []
            unmatched_cols_by_table[key].append(col_name)
            
        # Generate Table of Contents
        lines.append("## Table of Contents")
        lines.append("")
        lines.append("- [Mapped Tables](#mapped-tables)")
        for tbl in mapped_tables:
            anchor = f"{tbl[0]}-mapped-to-{tbl[1]}".replace('_', '-').lower()
            lines.append(f"  - [{tbl[0]} -> {tbl[1]}](#{anchor})")
        lines.append("- [Unmapped Source Tables](#unmapped-source-tables)")
        lines.append("- [Unmapped Target Tables](#unmapped-target-tables)")
        lines.append("")
        
        # Generate Mapped Tables Section
        lines.append("## Mapped Tables")
        lines.append("")
        for tbl in mapped_tables:
            src_tbl = tbl[0]
            tgt_tbl = tbl[1]
            src_rows = tbl[2] if tbl[2] is not None else 0
            tgt_rows = tbl[3] if tbl[3] is not None else 0
            
            anchor_text = f"{src_tbl} mapped to {tgt_tbl}"
            lines.append(f"### {anchor_text}")
            lines.append(f"**Source Rows:** {src_rows} | **Target Rows:** {tgt_rows}")
            lines.append("")
            
            # Fetch mapped columns
            query_cols = f"""
                SELECT source_column_name, target_column_name 
                FROM "{self.protocol_schema}"."mapping_columns"
                WHERE source_table_name = %s AND target_table_name = %s
                ORDER BY source_column_name
            """
            cursor.execute(query_cols, (src_tbl, tgt_tbl))
            mapped_cols = cursor.fetchall()
            
            if mapped_cols:
                lines.append("| Source Column | Target Column |")
                lines.append("|---|---|")
                for c in mapped_cols:
                    lines.append(f"| {c[0]} | {c[1]} |")
                lines.append("")
            else:
                lines.append("*No mapped columns found.*")
                lines.append("")
                
            # Unmapped columns for this table
            src_unmapped_cols = unmatched_cols_by_table.get(('source', src_tbl), [])
            if src_unmapped_cols:
                lines.append(f"**Unmapped Source Columns:** {', '.join(src_unmapped_cols)}")
                lines.append("")
                
            tgt_unmapped_cols = unmatched_cols_by_table.get(('target', tgt_tbl), [])
            if tgt_unmapped_cols:
                lines.append(f"**Unmapped Target Columns:** {', '.join(tgt_unmapped_cols)}")
                lines.append("")
        
        # Generate Unmapped Tables Section
        lines.append("## Unmapped Source Tables")
        lines.append("")
        if unmatched_source_tables:
            lines.append("| Source Table | Row Count |")
            lines.append("|---|---|")
            for ut in unmatched_source_tables:
                rc = ut[4] if ut[4] is not None else -1
                lines.append(f"| {ut[3]} | {rc} |")
        else:
            lines.append("*None*")
        lines.append("")
            
        lines.append("## Unmapped Target Tables")
        lines.append("")
        if unmatched_target_tables:
            lines.append("| Target Table | Row Count |")
            lines.append("|---|---|")
            for ut in unmatched_target_tables:
                rc = ut[4] if ut[4] is not None else -1
                lines.append(f"| {ut[3]} | {rc} |")
        else:
            lines.append("*None*")
        lines.append("")
        
        try:
            cursor.close()
            with open(filename, 'w') as f:
                f.write("\\n".join(lines))
            self.config_parser.print_log_message('INFO', f"migrator_tables: generate_mapping_report: Successfully wrote report to {filename}")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: generate_mapping_report: Failed to write report to {filename}: {e}")

    def print_migration_summary(self):
        lines = []
        lines.append("=" * 80)
        lines.append("                       CREDATIV PG-MIGRATOR SUMMARY                             ")
        lines.append("=" * 80)
        lines.append("")
        lines.append("[ DATABASE CONTEXT ]")
        lines.append(f"Source: {self.config_parser.get_source_db_name()}, schema: {self.config_parser.get_source_owner()} ({self.config_parser.get_source_db_type()})")
        lines.append(f"Target: {self.config_parser.get_target_db_name()}, schema: {self.config_parser.get_target_schema()} ({self.config_parser.get_target_db_type()})")
        lines.append(f"Workflow: {self.config_parser.get_workflow()}")
        lines.append("")
        
        if self.config_parser.is_dry_run():
            lines.append("! DRY RUN MODE ENABLED - NO MIGRATION PERFORMED !")
            lines.append("")

        lines.append("[ TIMING & EXECUTION PROFILES ]")
        lines.append("-" * 80)
        lines.append(f"{'Phase / Step':<44} | {'Duration':<14} | Start Time")
        lines.append("-" * 80)
        
        try:
            query = f"""SELECT * FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_main()}" ORDER BY id"""
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                task_data = self.decode_main_row(row)
                if task_data['task_completed'] and task_data['task_started']:
                    length = task_data['task_completed'] - task_data['task_started']
                    length_str = str(length)[:str(length).find('.')+3] if '.' in str(length) else str(length) 
                else:
                    length_str = "-"
                started_str = str(task_data['task_started']).split()[1][:8] if task_data['task_started'] else "-"
                
                name = task_data['task_name']
                sub = task_data['subtask_name']
                display_name = f"  {sub}" if sub else name
                display_name = display_name[:44]
                lines.append(f"{display_name:<44} | {length_str:<14} | {started_str}")
        except Exception:
            pass

        lines.append("")
        lines.append("[ OBJECTS MIGRATION RESULTS ]")
        lines.append("-" * 80)
        lines.append(f"{'Object Type':<24} | {'Source':>6} | {'Success':>7} | {'Failed':>6} | Details")
        lines.append("-" * 80)
        
        total_errors = 0
        objects_to_check = [
            ('User Defined Types', self.config_parser.get_protocol_name_user_defined_types(), None),
            ('Domains', self.config_parser.get_protocol_name_domains(), 'migrated_as'),
            ('Sequences', self.config_parser.get_protocol_name_sequences(), None),
            ('Tables', self.config_parser.get_protocol_name_tables(), None),
            ('Table Partitions', self.config_parser.get_protocol_name_source_table_partitioning(), None),
            ('Columns', self.config_parser.get_protocol_name_columns(), None),
            ('Altered Columns', self.config_parser.get_protocol_name_target_columns_alterations(), 'reason'),
            ('Indexes', self.config_parser.get_protocol_name_indexes(), 'index_type, index_owner'),
            ('Constraints', self.config_parser.get_protocol_name_constraints(), 'constraint_type'),
            ('Functions / Procedures', self.config_parser.get_protocol_name_funcprocs(), None),
            ('Triggers', self.config_parser.get_protocol_name_triggers(), None),
            ('Views', self.config_parser.get_protocol_name_views(), None),
            ('Aliases', self.config_parser.get_protocol_name_aliases(), None)
        ]

        for obj_name, table_name, add_cols in objects_to_check:
            try:
                cursor.execute(f"""SELECT COUNT(*) FROM "{self.protocol_schema}"."{table_name}" """)
                total = cursor.fetchone()[0]
                
                success_count = 0
                error_count = 0
                if not self.config_parser.is_dry_run():
                    cursor.execute(f"""SELECT success, COUNT(*) FROM "{self.protocol_schema}"."{table_name}" GROUP BY 1""")
                    for s, c in cursor.fetchall():
                        if s is True: success_count = c
                        elif s is False:
                            error_count = c
                            total_errors += c

                details = []
                if add_cols and not self.config_parser.is_dry_run():
                    cols_count = len(add_cols.split(','))
                    cols_enum = ", ".join(str(i+2) for i in range(cols_count))
                    cursor.execute(f"""SELECT COUNT(*), {add_cols} FROM "{self.protocol_schema}"."{table_name}" GROUP BY {cols_enum} ORDER BY {cols_enum}""")
                    for r in cursor.fetchall():
                        c = r[0]
                        val = " ".join(str(x) for x in r[1:] if x)
                        if val: details.append(f"{val}: {c}")

                if obj_name == 'Tables':
                    cursor.execute(f"""SELECT COUNT(*) FROM "{self.protocol_schema}"."{table_name}" WHERE source_table_rows_limited = 0 OR source_table_rows_limited IS NULL""")
                    empty_tables = cursor.fetchone()[0]
                    cursor.execute(f"""SELECT COUNT(*) FROM "{self.protocol_schema}"."{table_name}" WHERE source_table_rows_limited > 0""")
                    data_tables = cursor.fetchone()[0]
                    details.append(f"Empty: {empty_tables}, With Data: {data_tables}")

                details_str = ", ".join(details)
                if obj_name == 'Altered Columns':
                    lines.append(f"{obj_name:<24} | {total:>6} | {'-':>7} | {'-':>6} | {details_str}")
                else:
                    lines.append(f"{obj_name:<24} | {total:>6} | {success_count:>7} | {error_count:>6} | {details_str}")

            except psycopg2.errors.UndefinedTable:
                self.protocol_connection.connection.rollback()
            except Exception:
                self.protocol_connection.connection.rollback()

        # Add Comments summary
        try:
            comments_total = 0
            for tbl, col in [
                (self.config_parser.get_protocol_name_tables(), 'table_comment'),
                (self.config_parser.get_protocol_name_indexes(), 'index_comment'),
                (self.config_parser.get_protocol_name_views(), 'view_comment'),
                (self.config_parser.get_protocol_name_constraints(), 'constraint_comment'),
                (self.config_parser.get_protocol_name_funcprocs(), 'funcproc_comment'),
                (self.config_parser.get_protocol_name_triggers(), 'trigger_comment'),
                (self.config_parser.get_protocol_name_user_defined_types(), 'type_comment')
            ]:
                try:
                    cursor.execute(f"""SELECT COUNT(*) FROM "{self.protocol_schema}"."{tbl}" WHERE {col} IS NOT NULL AND {col} != ''""")
                    comments_total += cursor.fetchone()[0]
                except Exception:
                    self.protocol_connection.connection.rollback()

            try:
                cursor.execute(f"""
                    SELECT count(*)
                    FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_tables()}" t,
                         json_each(t.target_columns::json) c
                    WHERE c.value->>'column_comment' IS NOT NULL AND c.value->>'column_comment' != ''
                """)
                comments_total += cursor.fetchone()[0]
            except Exception:
                self.protocol_connection.connection.rollback()

            success_str = str(comments_total) if not self.config_parser.is_dry_run() else '-'
            failed_str = '0' if not self.config_parser.is_dry_run() else '-'
            lines.append(f"{'Comments':<24} | {comments_total:>6} | {success_str:>7} | {failed_str:>6} | ")
        except Exception:
            self.protocol_connection.connection.rollback()

        lines.append("")
        lines.append("[ DATA MIGRATION RESULTS ]")
        lines.append("-" * 80)
        dm_table = self.config_parser.get_protocol_name_data_migration()
        stats_table = self.config_parser.get_protocol_name_batches_stats()
        
        try:
            cursor.execute(f"""SELECT COUNT(*) FROM "{self.protocol_schema}"."{dm_table}" """)
            total_dm = cursor.fetchone()[0]
            if total_dm > 0:
                cursor.execute(f"""SELECT COUNT(*) FROM "{self.protocol_schema}"."{dm_table}" WHERE source_table_rows_limited = 0 OR source_table_rows_limited IS NULL""")
                empty_dm = cursor.fetchone()[0]
                cursor.execute(f"""SELECT COUNT(*) FROM "{self.protocol_schema}"."{dm_table}" WHERE source_table_rows_limited > 0""")
                data_dm = cursor.fetchone()[0]
                cursor.execute(f"""SELECT COUNT(*) FROM "{self.protocol_schema}"."{dm_table}" WHERE source_table_rows_limited > 0 AND source_table_rows_limited = target_table_rows""")
                full_dm = cursor.fetchone()[0]
                cursor.execute(f"""SELECT COUNT(*) FROM "{self.protocol_schema}"."{dm_table}" WHERE source_table_rows_limited > 0 AND source_table_rows_limited <> target_table_rows""")
                diff_dm = cursor.fetchone()[0]
                
                cursor.execute(f"""SELECT COUNT(*) FROM "{self.protocol_schema}"."{dm_table}" WHERE success = FALSE""")
                dm_errors = cursor.fetchone()[0]
                total_errors += dm_errors
                
                lines.append(f"Total Tables Processed   : {total_dm}")
                lines.append(f"Empty Tables (0 rows)    : {empty_dm}")
                lines.append(f"Tables with Data         : {data_dm}")
                lines.append(f"Fully Migrated (Matched) : {full_dm}")
                lines.append(f"Row Count Mismatches     : {diff_dm}")
                
                if full_dm > 0:
                    top_migrated_limit = self.config_parser.get_summary_top_migrated_tables()
                    cursor.execute(f"""SELECT target_schema_name, target_table_name, source_table_rows_all, source_table_rows_limited, target_table_rows, 
                                       EXTRACT(EPOCH FROM (task_completed - task_started))
                                       FROM "{self.protocol_schema}"."{dm_table}" 
                                       WHERE source_table_rows_limited > 0 AND source_table_rows_limited = target_table_rows AND success = TRUE
                                       ORDER BY source_table_rows_limited DESC LIMIT {top_migrated_limit}""")
                    rows = cursor.fetchall()
                    if rows:
                        max_name_len = max(len(sch + '.' + tbl) for sch, tbl, *_ in rows)
                        max_name_len = max(max_name_len, 28)
                        lines.append("")
                        lines.append(f"Biggest Successfully Migrated Tables (Top {top_migrated_limit}):")
                        lines.append(f"{'Table Name':<{max_name_len}} | {'Src All':>11} | {'Src Lim':>11} | {'Tgt Rows':>11} | {'Time(s)':>8}")
                        lines.append("-" * (max_name_len + 53))
                        for sch, tbl, s_all, s_lim, t_rows, duration in rows:
                            s_all_fmt = f"{s_all:,}" if s_all is not None else "-"
                            s_lim_fmt = f"{s_lim:,}"
                            t_fmt = f"{t_rows:,}"
                            d_fmt = f"{round(duration, 2) if duration else 0.0}"
                            lines.append(f"{sch + '.' + tbl:<{max_name_len}} | {s_all_fmt:>11} | {s_lim_fmt:>11} | {t_fmt:>11} | {d_fmt:>8}")

                if diff_dm > 0:
                    top_mismatched_limit = self.config_parser.get_summary_top_mismatched_tables()
                    cursor.execute(f"""SELECT target_schema_name, target_table_name, source_table_rows_all, source_table_rows_limited, target_table_rows,
                                       ABS(source_table_rows_limited - target_table_rows),
                                       EXTRACT(EPOCH FROM (task_completed - task_started))
                                       FROM "{self.protocol_schema}"."{dm_table}" 
                                       WHERE source_table_rows_limited > 0 AND source_table_rows_limited <> target_table_rows 
                                       ORDER BY source_table_rows_limited DESC LIMIT {top_mismatched_limit}""")
                    rows = cursor.fetchall()
                    if rows:
                        max_name_len = max(len(sch + '.' + tbl) for sch, tbl, *_ in rows)
                        max_name_len = max(max_name_len, 22)
                        lines.append("")
                        lines.append(f"Tables with row count mismatches (Top {top_mismatched_limit} by Source Rows):")
                        lines.append(f"{'Table Name':<{max_name_len}} | {'Src All':>10} | {'Src Lim':>10} | {'Tgt Rows':>10} | {'Diff':>7} | {'Time':>6}")
                        lines.append("-" * (max_name_len + 58))
                        for sch, tbl, s_all, s_lim, t_rows, diff, duration in rows:
                            s_all_fmt = f"{s_all:,}" if s_all is not None else "-"
                            s_lim_fmt = f"{s_lim:,}"
                            t_fmt = f"{t_rows:,}"
                            diff_fmt = f"{diff:,}"
                            d_fmt = f"{round(duration, 2) if duration else 0.0}"
                            lines.append(f"{sch + '.' + tbl:<{max_name_len}} | {s_all_fmt:>10} | {s_lim_fmt:>10} | {t_fmt:>10} | {diff_fmt:>7} | {d_fmt:>6}")

                top_longest_batches_limit = self.config_parser.get_summary_top_longest_batches()
                cursor.execute(f"""SELECT target_schema_name, target_table_name, batch_number, 
                                   round(batch_seconds::numeric, 2), round(reading_seconds::numeric, 2), 
                                   round(transforming_seconds::numeric, 2), round(writing_seconds::numeric, 2)
                                   FROM "{self.protocol_schema}"."{stats_table}"
                                   ORDER BY batch_seconds DESC LIMIT {top_longest_batches_limit}""")
                batches = cursor.fetchall()
                if batches:
                    max_name_len = max(len(sch + '.' + tbl) for sch, tbl, *_ in batches)
                    max_name_len = max(max_name_len, 30)
                    lines.append("")
                    lines.append(f"Longest Data Batches (Top {top_longest_batches_limit}):")
                    lines.append(f"{'Table Name':<{max_name_len}} | {'Batch':>5} | {'Total(s)':>8} | {'Read(s)':>8} | {'Trans(s)':>8} | {'Write(s)':>8}")
                    lines.append("-" * (max_name_len + 52))
                    for sch, tbl, b_num, b_sec, r_sec, t_sec, w_sec in batches:
                        b_fmt = f"{b_sec:,}"
                        r_fmt = f"{r_sec:,}"
                        t_fmt = f"{t_sec:,}"
                        w_fmt = f"{w_sec:,}"
                        lines.append(f"{sch + '.' + tbl:<{max_name_len}} | {b_num:>5} | {b_fmt:>8} | {r_fmt:>8} | {t_fmt:>8} | {w_fmt:>8}")
            else:
                lines.append("No data migration executed in this run.")

        except psycopg2.errors.UndefinedTable:
            self.protocol_connection.connection.rollback()
        except Exception:
            self.protocol_connection.connection.rollback()

        if self.config_parser.is_mapping_workflow():
            lines.append("")
            lines.append("[ MAPPING WORKFLOW RESULTS ]")
            lines.append("-" * 80)
            
            try:
                cursor = self.protocol_connection.connection.cursor()
                
                cursor.execute(f'SELECT count(*) FROM "{self.protocol_schema}"."mapping_tables"')
                tables_count = cursor.fetchone()[0]
                lines.append(f"Mapped Tables: {tables_count}")
                cursor.execute(f'SELECT match_type, count(*) FROM "{self.protocol_schema}"."mapping_tables" GROUP BY match_type ORDER BY count(*) DESC')
                for row in cursor.fetchall():
                    lines.append(f"    Found via {row[0]}: {row[1]}")
                    
                cursor.execute(f'SELECT count(*) FROM "{self.protocol_schema}"."mapping_columns"')
                columns_count = cursor.fetchone()[0]
                lines.append(f"Mapped Columns: {columns_count}")
                cursor.execute(f'SELECT match_type, count(*) FROM "{self.protocol_schema}"."mapping_columns" GROUP BY match_type ORDER BY count(*) DESC')
                for row in cursor.fetchall():
                    lines.append(f"    Found via {row[0]}: {row[1]}")
                    
                cursor.execute(f'SELECT count(*) FROM "{self.protocol_schema}"."mapping_target_sequences"')
                sequences_count = cursor.fetchone()[0]
                lines.append(f"Mapped Sequences: {sequences_count}")
                
                cursor.execute(f'SELECT side, count(*) FROM "{self.protocol_schema}"."mapping_unmatched_objects" WHERE object_type = \'table\' GROUP BY 1 ORDER BY 1')
                unmapped_tables = {row[0]: row[1] for row in cursor.fetchall()}
                cursor.execute(f'SELECT side, count(*) FROM "{self.protocol_schema}"."mapping_unmatched_objects" WHERE object_type = \'column\' GROUP BY 1 ORDER BY 1')
                unmapped_columns = {row[0]: row[1] for row in cursor.fetchall()}
                
                if unmapped_tables or unmapped_columns:
                    lines.append("")
                    lines.append("Unmapped Objects:")
                    if 'source' in unmapped_tables: lines.append(f"    Source Tables: {unmapped_tables['source']}")
                    if 'target' in unmapped_tables: lines.append(f"    Target Tables: {unmapped_tables['target']}")
                    if 'source' in unmapped_columns: lines.append(f"    Source Columns: {unmapped_columns['source']}")
                    if 'target' in unmapped_columns: lines.append(f"    Target Columns: {unmapped_columns['target']}")

                report_filename = self.config_parser.get_mapping_report_filename()
                if report_filename:
                    import os
                    lines.append("")
                    lines.append(f"Detailed Mapping Report generated at: {os.path.abspath(report_filename)}")
                lines.append("")
                
                for obj_name, tbl_name, is_index in [('Indexes', 'mapping_target_indexes', True), ('Constraints', 'mapping_target_constraints', False)]:
                    cursor.execute(f'SELECT count(*) FROM "{self.protocol_schema}"."{tbl_name}"')
                    total = cursor.fetchone()[0]
                    lines.append(f"Target {obj_name}: {total}")
                    
                    if is_index:
                        cursor.execute(f'SELECT index_type, count(*) FROM "{self.protocol_schema}"."{tbl_name}" WHERE is_primary_key = TRUE GROUP BY 1 ORDER BY 1')
                        for row in cursor.fetchall():
                            lines.append(f"    Primary Keys (kept) - {row[0]}: {row[1]}")
                    else:
                        cursor.execute(f'SELECT count(*) FROM "{self.protocol_schema}"."{tbl_name}" WHERE constraint_type = \'PRIMARY KEY\'')
                        pks = cursor.fetchone()[0]
                        if pks > 0:
                            lines.append(f"    ('PRIMARY KEY',): {pks}")

                    type_col = "index_type" if is_index else "constraint_type"
                    filter_sql = "not is_primary_key" if is_index else "constraint_type != 'PRIMARY KEY'"

                    cursor.execute(f'SELECT {type_col}, count(*) FROM "{self.protocol_schema}"."{tbl_name}" WHERE {filter_sql} AND dropped = TRUE GROUP BY 1 ORDER BY 1')
                    for row in cursor.fetchall():
                        lines.append(f"    successfully dropped ({row[0]}): {row[1]}")

                    cursor.execute(f'SELECT {type_col}, count(*) FROM "{self.protocol_schema}"."{tbl_name}" WHERE {filter_sql} AND dropped = FALSE GROUP BY 1 ORDER BY 1')
                    for row in cursor.fetchall():
                        lines.append(f"    error dropping ({row[0]}): {row[1]}")

                    cursor.execute(f'SELECT {type_col}, count(*) FROM "{self.protocol_schema}"."{tbl_name}" WHERE {filter_sql} AND dropped IS NULL GROUP BY 1 ORDER BY 1')
                    for row in cursor.fetchall():
                        lines.append(f"    drop unknown status ({row[0]}): {row[1]}")

                    cursor.execute(f'SELECT {type_col}, count(*) FROM "{self.protocol_schema}"."{tbl_name}" WHERE {filter_sql} AND success = TRUE GROUP BY 1 ORDER BY 1')
                    for row in cursor.fetchall():
                        lines.append(f"    successfully recreated ({row[0]}): {row[1]}")

                    cursor.execute(f'SELECT {type_col}, count(*) FROM "{self.protocol_schema}"."{tbl_name}" WHERE {filter_sql} AND success = FALSE GROUP BY 1 ORDER BY 1')
                    for row in cursor.fetchall():
                        lines.append(f"    error recreating ({row[0]}): {row[1]}")

                    cursor.execute(f'SELECT {type_col}, count(*) FROM "{self.protocol_schema}"."{tbl_name}" WHERE {filter_sql} AND success IS NULL GROUP BY 1 ORDER BY 1')
                    for row in cursor.fetchall():
                        msg = "recreate skipped (handled by constraint)" if is_index else "recreate unknown status"
                        lines.append(f"    {msg} ({row[0]}): {row[1]}")

                cursor.close()
            except Exception:
                self.protocol_connection.connection.rollback()
                lines.append("Error retrieving mapping summary details.")

        if self.config_parser.is_anonymization_workflow():
            lines.append("")
            lines.append("[ ANONYMIZATION WORKFLOW RESULTS ]")
            lines.append("-" * 80)
            anon_tables_data = []
            anon_tables = 0
            anon_columns = 0
            try:
                from credativ_pg_migrator.anonymization.routing import MigratorAnonymizer
                anonymizer = MigratorAnonymizer(self.config_parser.config)
                if anonymizer.is_active():
                    raw_tables = self.fetch_all_tables()
                    for t_row in raw_tables:
                        table_data = self.decode_table_row(t_row)
                        if table_data.get('source_table_rows_limited', 0) > 0:
                            target_schema_name = table_data.get('target_schema_name', '')
                            target_table_name = table_data['target_table_name']
                            full_table_name = f"{target_schema_name}.{target_table_name}" if target_schema_name else target_table_name
                            
                            target_columns = table_data.get('target_columns', {})
                            table_matched = False
                            table_anon_cols = []
                            for col_val in target_columns.values():
                                col_name = col_val.get('column_name') if isinstance(col_val, dict) else col_val
                                data_type = col_val.get('data_type', 'unknown') if isinstance(col_val, dict) else 'unknown'
                                method, params = anonymizer.get_method_for_column(target_table_name, col_name)
                                if method:
                                    anon_columns += 1
                                    table_matched = True
                                    table_anon_cols.append({'name': col_name, 'data_type': data_type, 'method': method, 'params': params})
                            if table_matched:
                                anon_tables += 1
                                anon_tables_data.append({
                                    'table_name': full_table_name,
                                    'columns': table_anon_cols
                                })
                
                lines.append(f"Anonymized {anon_columns} columns in {anon_tables} tables.")
                
                if anon_tables_data:
                    top_anonymized_tables_limit = self.config_parser.get_summary_top_anonymized_tables()
                    top_anonymized_columns_limit = self.config_parser.get_summary_top_anonymized_columns()
                    anon_tables_data.sort(key=lambda x: (-len(x['columns']), x['table_name']))
                    top_tables = anon_tables_data[:top_anonymized_tables_limit]
                    lines.append("")
                    lines.append("Top Tables with Most Anonymized Columns:")
                    for idx, t_data in enumerate(top_tables, 1):
                        if idx > 1:
                            lines.append("")
                        col_count = len(t_data['columns'])
                        lines.append(f"{idx}. {t_data['table_name']} ({col_count} columns anonymized)")
                        
                        sorted_cols = sorted(t_data['columns'], key=lambda x: x['name'])
                        display_cols = sorted_cols[:top_anonymized_columns_limit]
                        
                        if display_cols:
                            col_len = max([len(c['name']) for c in display_cols] + [11])
                            type_len = max([len(c['data_type']) for c in display_cols] + [9])
                            lines.append(f"   { 'Column Name'.ljust(col_len) } | { 'Data Type'.ljust(type_len) } | Method")
                            lines.append(f"   { '-' * col_len }-+-{ '-' * type_len }-+-{ '-' * 20 }")
                            
                            for col_info in display_cols:
                                method_str = col_info['method']
                                if col_info.get('params') and 'part' in col_info['params']:
                                    method_str += f" (part: {col_info['params']['part']})"
                                elif col_info.get('params'):
                                    param_str = ", ".join(f"{k}: {v}" for k, v in col_info['params'].items())
                                    method_str += f" ({param_str})"
                                lines.append(f"   { col_info['name'].ljust(col_len) } | { col_info['data_type'].ljust(type_len) } | { method_str }")
                            
                        if col_count > top_anonymized_columns_limit:
                            lines.append(f"   - ... and {col_count - top_anonymized_columns_limit} more")
                            
                        show_examples = self.config_parser.get_summary_show_anonymization_examples()
                        if show_examples > 0:
                            self._append_anonymization_examples(lines, t_data['table_name'], display_cols, show_examples)
            except Exception as e:
                lines.append(f"Error computing anonymization stats: {e}")

        lines.append("")
        lines.append(f"TOTAL ERRORS IN MIGRATION: {total_errors}")
        lines.append("=" * 80)
        final_summary = "\n" + "\n".join(lines)
        self.config_parser.print_log_message('INFO', final_summary)

    def _append_anonymization_examples(self, lines, table_fqn, display_cols, limit):
        if self.config_parser.get_source_db_type() != 'postgresql' or self.config_parser.get_target_db_type() != 'postgresql':
            return
            
        try:
            import psycopg2
            import psycopg2.extras
            
            src_cfg = self.config_parser.get_source_config()
            tgt_cfg = self.config_parser.get_target_config()
            
            if not src_cfg or not tgt_cfg:
                return

            schema_parts = table_fqn.split('.', 1)
            if len(schema_parts) != 2:
                return
            schema_name, table_name = schema_parts

            src_dbname = self.config_parser.get_source_db_name()
            tgt_dbname = self.config_parser.get_target_db_name()

            src_conn = psycopg2.connect(
                host=src_cfg.get('host', 'localhost'),
                port=src_cfg.get('port', 5432),
                dbname=src_dbname,
                user=src_cfg.get('username'),
                password=src_cfg.get('password'),
                application_name=MigratorConstants.get_application_name()
            )
            
            tgt_conn = psycopg2.connect(
                host=tgt_cfg.get('host', 'localhost'),
                port=tgt_cfg.get('port', 5432),
                dbname=tgt_dbname,
                user=tgt_cfg.get('username'),
                password=tgt_cfg.get('password'),
                application_name=MigratorConstants.get_application_name()
            )
            
            src_cur = src_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            tgt_cur = tgt_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            # Find primary keys
            src_cur.execute("""
                SELECT a.attname
                FROM   pg_index i
                JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                JOIN   pg_class c ON c.oid = i.indrelid
                JOIN   pg_namespace n ON n.oid = c.relnamespace
                WHERE  n.nspname = %s AND c.relname = %s
                AND    i.indisprimary
            """, (schema_name, table_name))
            
            pks = [row[0] for row in src_cur.fetchall()]
            if not pks:
                src_cur.close()
                tgt_cur.close()
                src_conn.close()
                tgt_conn.close()
                lines.append(f"   [!] Examples skipped: No primary key found for {table_fqn}.")
                return
                
            pk_cols_str = ", ".join([f'"{pk}"' for pk in pks])
            anon_cols = [c['name'] for c in display_cols]
            fetch_cols_str = ", ".join([f'"{col}"' for col in anon_cols])
            
            src_cur.execute(f'SELECT {pk_cols_str}, {fetch_cols_str} FROM "{schema_name}"."{table_name}" ORDER BY random() LIMIT {limit}')
            src_rows = src_cur.fetchall()
            
            if src_rows:
                lines.append("")
                lines.append(f"   Examples (Original => Anonymized):")
                for row_idx, row in enumerate(src_rows, 1):
                    pk_vals_str = ", ".join([f"{pk}={row[pk]}" for pk in pks])
                    lines.append(f"   Row {row_idx} (PK: {pk_vals_str}):")
                    where_clauses = " AND ".join([f'"{pk}" = %s' for pk in pks])
                    where_values = [row[pk] for pk in pks]
                    tgt_cur.execute(f'SELECT {fetch_cols_str} FROM "{schema_name}"."{table_name}" WHERE {where_clauses}', where_values)
                    tgt_row = tgt_cur.fetchone()
                    
                    if tgt_row:
                        for col in anon_cols:
                            orig_val = str(row[col])[:30] + ('...' if len(str(row[col])) > 30 else '')
                            anon_val = str(tgt_row[col])[:30] + ('...' if len(str(tgt_row[col])) > 30 else '')
                            lines.append(f"     - {col}: '{orig_val}' => '{anon_val}'")
                    else:
                        lines.append(f"     [!] Row missing in target database.")
            
            src_cur.close()
            tgt_cur.close()
            src_conn.close()
            tgt_conn.close()
            
        except Exception as e:
            lines.append(f"   [!] Could not fetch examples: {e}")




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

    def fetch_all_sequences(self, only_unfinished=False):
        if only_unfinished:
            query = f"""SELECT * FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_sequences()}" WHERE success IS NOT TRUE ORDER BY sequence_id"""
        else:
            query = f"""SELECT * FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_sequences()}" ORDER BY sequence_id"""
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        sequences = cursor.fetchall()
        return sequences

    def fetch_sequence(self, settings):
        source_schema_name = settings.get('source_schema_name')
        source_sequence_name = settings.get('source_sequence_name')
        query = f"""
                SELECT *
                FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_sequences()}"
                WHERE source_schema_name = '{source_schema_name}'
                AND source_sequence_name = '{source_sequence_name}'
                """
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        sequence = cursor.fetchone()
        cursor.close()
        if not sequence:
            return None
        return sequence

    def fetch_table(self, settings):
        source_schema_name = settings.get('source_schema_name')
        source_table_name = settings.get('source_table_name')
        query = f"""
                SELECT *
                FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_tables()}"
                WHERE source_schema_name = '{source_schema_name}'
                AND source_table_name = '{source_table_name}'
                """
        # self.protocol_connection.connect()
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        table = cursor.fetchone()
        cursor.close()
        if not table:
            return None
        return self.decode_table_row(table)

    def fetch_data_source(self, settings):
        source_schema_name = settings.get('source_schema_name')
        source_table_name = settings.get('source_table_name')
        query = f"""SELECT * FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_data_sources()}"
            WHERE source_schema_name = '{source_schema_name}' AND source_table_name = '{source_table_name}'"""
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: fetch_data_source: Executing query: {query}")
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
            table_names.append(values['target_table_name'])
        return table_names

    def fetch_all_data_migrations(self, settings=None):
        if settings is None: settings = {}
        source_schema_name = settings.get('source_schema_name')
        source_table_name = settings.get('source_table_name')
        if source_schema_name and source_table_name:
            query = f"""SELECT * FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_data_migration()}" WHERE source_schema_name = '{source_schema_name}' AND source_table_name = '{source_table_name}' ORDER BY id"""
        else:
            query = f"""SELECT * FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_data_migration()}" ORDER BY id"""
        # self.protocol_connection.connect()
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: fetch_all_data_migrations: Executing query: {query}")
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        data_migrations = cursor.fetchall()
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: fetch_all_data_migrations: Fetched {len(data_migrations)} rows.")
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
            # target_view_name = values['target_view_alias'] if self.config_parser.get_use_aliases_as_target_names() and values.get('target_view_alias') else values['target_view_name']
            target_view_name = values['target_view_name']
            if target_view_name:
                view_names.append(target_view_name)
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


    def decode_source_table_partitioning_row(self, row):
        return {
            'id': row[0],
            'source_schema_name': row[1],
            'source_table_name': row[2],
            'source_table_id': row[3],
            'source_table_partitioning_level': row[4],
            'source_partition_columns': row[5],
            'source_partition_ranges': row[6],
            'task_created': row[7],
            'task_started': row[8],
            'task_completed': row[9],
            'success': row[10],
            'message': row[11]
        }

    def insert_source_table_partitioning(self, settings):
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_source_table_partitioning()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema_name, source_table_name, source_table_id, source_table_partitioning_level, source_partition_columns, source_partition_ranges)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (settings.get('source_schema_name'), settings.get('source_table_name'), settings.get('source_table_id'), settings.get('source_table_partitioning_level'), settings.get('source_partition_columns'), settings.get('source_partition_ranges'))
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
            partitioning_row = self.decode_source_table_partitioning_row(row)
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_source_table_partitioning: ({func_run_id}): Returned row: {partitioning_row}")
            self.insert_protocol({'object_type': 'source_table_partitioning', 'object_name': settings.get('source_table_name'), 'object_action': 'create', 'object_ddl': None, 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': partitioning_row['id']})
            return partitioning_row['id']
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_source_table_partitioning: ({func_run_id}): Error inserting info for {settings.get('source_table_name')} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_source_table_partitioning: ({func_run_id}): Exception: {e}")
            raise

    def update_source_table_partitioning_status(self, settings):
        row_id = settings.get('row_id')
        success = settings.get('success')
        message = settings.get('message')
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_source_table_partitioning()
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
                partitioning_row = self.decode_source_table_partitioning_row(row)
                self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_source_table_partitioning_status: ({func_run_id}): Returned row: {partitioning_row}")
                self.update_protocol({'object_type': 'source_table_partitioning', 'object_protocol_id': partitioning_row['id'], 'execution_success': success, 'execution_error_message': message, 'execution_results': None})
            else:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_source_table_partitioning_status: ({func_run_id}): Error updating status for row {row_id} in {table_name}. No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_source_table_partitioning_status: ({func_run_id}): Error updating status for row {row_id} in {table_name}. Exception: {e}")
            raise

    def decode_target_table_partitioning_row(self, row):
        return {
            'id': row[0],
            'target_schema_name': row[1],
            'target_table_name': row[2],
            'target_table_id': row[3],
            'target_table_partitioning_level': row[4],
            'target_partition_columns': row[5],
            'target_partition_ranges': row[6],
            'task_created': row[7],
            'task_started': row[8],
            'task_completed': row[9],
            'success': row[10],
            'message': row[11]
        }

    def insert_target_table_partitioning(self, settings):
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_target_table_partitioning()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (target_schema_name, target_table_name, target_table_id, target_table_partitioning_level, target_partition_columns, target_partition_ranges)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (settings.get('target_schema_name'), settings.get('target_table_name'), settings.get('target_table_id'), settings.get('target_table_partitioning_level'), settings.get('target_partition_columns'), settings.get('target_partition_ranges'))
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
            partitioning_row = self.decode_target_table_partitioning_row(row)
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_target_table_partitioning: ({func_run_id}): Returned row: {partitioning_row}")
            self.insert_protocol({'object_type': 'target_table_partitioning', 'object_name': settings.get('target_table_name'), 'object_action': 'create', 'object_ddl': None, 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': partitioning_row['id']})
            return partitioning_row['id']
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_target_table_partitioning: ({func_run_id}): Error inserting info for {settings.get('target_table_name')} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_target_table_partitioning: ({func_run_id}): Exception: {e}")
            raise

    def update_target_table_partitioning_status(self, settings):
        row_id = settings.get('row_id')
        success = settings.get('success')
        message = settings.get('message')
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_target_table_partitioning()
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
                partitioning_row = self.decode_target_table_partitioning_row(row)
                self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_target_table_partitioning_status: ({func_run_id}): Returned row: {partitioning_row}")
                self.update_protocol({'object_type': 'target_table_partitioning', 'object_protocol_id': partitioning_row['id'], 'execution_success': success, 'execution_error_message': message, 'execution_results': None})
            else:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_target_table_partitioning_status: ({func_run_id}): Error updating status for row {row_id} in {table_name}. No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_target_table_partitioning_status: ({func_run_id}): Error updating status for row {row_id} in {table_name}. Exception: {e}")
            raise

    def decode_columns_row(self, row):
        return {
            'id': row[0],
            'source_schema_name': row[1],
            'source_table_name': row[2],
            'source_table_id': row[3],
            'source_column_name': row[4],
            'source_column_id': row[5],
            'source_column_data_type': row[6],
            'source_column_is_nullable': row[7],
            'source_column_is_primary_key': row[8],
            'source_column_is_identity': row[9],
            'source_column_default_name': row[10],
            'source_column_default_value': row[11],
            'source_column_replaced_default_value': row[12],
            'source_column_character_maximum_length': row[13],
            'source_column_numeric_precision': row[14],
            'source_column_numeric_scale': row[15],
            'source_column_basic_data_type': row[16],
            'source_column_basic_character_maximum_length': row[17],
            'source_column_basic_numeric_precision': row[18],
            'source_column_basic_numeric_scale': row[19],
            'source_column_basic_column_type': row[20],
            'source_column_is_generated_virtual': row[21],
            'source_column_is_generated_stored': row[22],
            'source_column_generation_expression': row[23],
            'source_column_stripped_generation_expression': row[24],
            'source_column_udt_schema': row[25],
            'source_column_udt_name': row[26],
            'source_column_domain_schema': row[27],
            'source_column_domain_name': row[28],
            'source_column_description': row[29],
            'source_column_sql': row[30],
            'target_schema_name': row[31],
            'target_table_name': row[32],
            'target_alias_name': row[33],
            'target_table_id': row[34],
            'target_column_name': row[35],
            'target_column_id': row[36],
            'target_column_data_type': row[37],
            'target_column_description': row[38],
            'target_column_sql': row[39],
            'task_created': row[40],
            'task_started': row[41],
            'task_completed': row[42],
            'success': row[43],
            'message': row[44]
        }

    def insert_columns(self, settings):
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_columns()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema_name, source_table_name, source_table_id, source_column_name, source_column_id, source_column_data_type, source_column_is_nullable, source_column_is_primary_key, source_column_is_identity, source_column_default_name, source_column_default_value, source_column_replaced_default_value, source_column_character_maximum_length, source_column_numeric_precision, source_column_numeric_scale, source_column_basic_data_type, source_column_basic_character_maximum_length, source_column_basic_numeric_precision, source_column_basic_numeric_scale, source_column_basic_column_type, source_column_is_generated_virtual, source_column_is_generated_stored, source_column_generation_expression, source_column_stripped_generation_expression, source_column_udt_schema, source_column_udt_name, source_column_domain_schema, source_column_domain_name, source_column_description, source_column_sql, target_schema_name, target_table_name, target_alias_name, target_table_id, target_column_name, target_column_id, target_column_data_type, target_column_description, target_column_sql)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (settings.get('source_schema_name'), settings.get('source_table_name'), settings.get('source_table_id'), settings.get('source_column_name'), settings.get('source_column_id'), settings.get('source_column_data_type'), settings.get('source_column_is_nullable'), settings.get('source_column_is_primary_key'), settings.get('source_column_is_identity'), settings.get('source_column_default_name'), settings.get('source_column_default_value'), settings.get('source_column_replaced_default_value'), settings.get('source_column_character_maximum_length'), settings.get('source_column_numeric_precision'), settings.get('source_column_numeric_scale'), settings.get('source_column_basic_data_type'), settings.get('source_column_basic_character_maximum_length'), settings.get('source_column_basic_numeric_precision'), settings.get('source_column_basic_numeric_scale'), settings.get('source_column_basic_column_type'), settings.get('source_column_is_generated_virtual'), settings.get('source_column_is_generated_stored'), settings.get('source_column_generation_expression'), settings.get('source_column_stripped_generation_expression'), settings.get('source_column_udt_schema'), settings.get('source_column_udt_name'), settings.get('source_column_domain_schema'), settings.get('source_column_domain_name'), settings.get('source_column_description'), settings.get('source_column_sql'), settings.get('target_schema_name'), settings.get('target_table_name'), settings.get('target_alias_name', ''), settings.get('target_table_id'), settings.get('target_column_name'), settings.get('target_column_id'), settings.get('target_column_data_type'), settings.get('target_column_description'), settings.get('target_column_sql'))
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
            columns_row = self.decode_columns_row(row)
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_columns: ({func_run_id}): Returned row: {columns_row}")
            self.insert_protocol({'object_type': 'column', 'object_name': settings.get('source_column_name'), 'object_action': 'create', 'object_ddl': None, 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': columns_row['id']})
            return columns_row['id']
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_columns: ({func_run_id}): Error inserting info for {settings.get('source_column_name')} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_columns: ({func_run_id}): Exception: {e}")
            raise

    def update_columns_status(self, settings):
        row_id = settings.get('row_id')
        success = settings.get('success')
        message = settings.get('message')
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_columns()
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
                columns_row = self.decode_columns_row(row)
                self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_columns_status: ({func_run_id}): Returned row: {columns_row}")
                self.update_protocol({'object_type': 'column', 'object_protocol_id': columns_row['id'], 'execution_success': success, 'execution_error_message': message, 'execution_results': None})
            else:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_columns_status: ({func_run_id}): Error updating status for row {row_id} in {table_name}. No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_columns_status: ({func_run_id}): Error updating status for row {row_id} in {table_name}. Exception: {e}")
            raise

    def decode_aliases_row(self, row):
        return {
            'id': row[0],
            'source_schema_name': row[1],
            'source_alias_name': row[2],
            'source_alias_id': row[3],
            'source_alias_sql': row[4],
            'source_referenced_schema_name': row[5],
            'source_referenced_table_name': row[6],
            'source_referenced_column_name': row[7],
            'source_alias_comment': row[8],
            'target_schema_name': row[9],
            'target_alias_name': row[10],
            'alias_target_type': row[11], # Added alias_target_type
            'target_referenced_schema_name': row[12],
            'target_referenced_table_name': row[13],
            'target_referenced_column_name': row[14],
            'target_alias_sql': row[15],
            'task_created': row[16],
            'task_started': row[17],
            'task_completed': row[18],
            'success': row[19],
            'message': row[20]
        }

    def get_alias_for_table(self, source_schema_name, source_table_name):
        table_name = self.config_parser.get_protocol_name_aliases()
        query = f"""
            SELECT target_alias_name, alias_target_type, id
            FROM "{self.protocol_schema}"."{table_name}"
            WHERE upper(source_referenced_schema_name) = upper(%s) AND upper(source_referenced_table_name) = upper(%s)
        """
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, (source_schema_name, source_table_name))
            row = cursor.fetchone()
            cursor.close()
            if row:
                return {'target_alias_name': row[0], 'alias_target_type': row[1], 'id': row[2]}
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: get_alias_for_table: Error querying alias for {source_schema_name}.{source_table_name}: {e}")
        return None

    def insert_aliases(self, settings):
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_aliases()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."{table_name}"
            (source_schema_name, source_alias_name, source_alias_id, source_alias_sql, source_referenced_schema_name, source_referenced_table_name, source_referenced_column_name, source_alias_comment, target_schema_name, target_alias_name, alias_target_type, target_referenced_schema_name, target_referenced_table_name, target_referenced_column_name, target_alias_sql)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """
        params = (settings.get('source_schema_name'), settings.get('source_alias_name'), settings.get('source_alias_id'), settings.get('source_alias_sql'), settings.get('source_referenced_schema_name'), settings.get('source_referenced_table_name'), settings.get('source_referenced_column_name'), settings.get('source_alias_comment'), settings.get('target_schema_name'), settings.get('target_alias_name'), settings.get('alias_target_type'), settings.get('target_referenced_schema_name'), settings.get('target_referenced_table_name'), settings.get('target_referenced_column_name'), settings.get('target_alias_sql'))
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
            aliases_row = self.decode_aliases_row(row)
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_aliases: ({func_run_id}): Returned row: {aliases_row}")
            self.insert_protocol({'object_type': 'alias', 'object_name': settings.get('source_alias_name'), 'object_action': 'create', 'object_ddl': None, 'execution_timestamp': None, 'execution_success': None, 'execution_error_message': None, 'row_type': 'info', 'execution_results': None, 'object_protocol_id': aliases_row['id']})
            return aliases_row['id']
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_aliases: ({func_run_id}): Error inserting info for {settings.get('source_alias_name')} into {table_name}.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_aliases: ({func_run_id}): Exception: {e}")
            raise

    def update_aliases_status(self, settings):
        row_id = settings.get('row_id')
        success = settings.get('success')
        message = settings.get('message')
        func_run_id = uuid.uuid4()
        table_name = self.config_parser.get_protocol_name_aliases()
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
                aliases_row = self.decode_aliases_row(row)
                self.config_parser.print_log_message('DEBUG3', f"migrator_tables: update_aliases_status: ({func_run_id}): Returned row: {aliases_row}")
                self.update_protocol({'object_type': 'alias', 'object_protocol_id': aliases_row['id'], 'execution_success': success, 'execution_error_message': message, 'execution_results': None})
            else:
                self.config_parser.print_log_message('ERROR', f"migrator_tables: update_aliases_status: ({func_run_id}): Error updating status for row {row_id} in {table_name}. No protocol row returned.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_aliases_status: ({func_run_id}): Error updating status for row {row_id} in {table_name}. Exception: {e}")
            raise


    def fetch_all_source_table_partitioning(self, settings):
        source_schema_name = settings.get('source_schema_name')
        table_name = self.config_parser.get_protocol_name_source_table_partitioning()
        query = f"""SELECT * FROM "{self.protocol_schema}"."{table_name}" WHERE source_schema_name = '{source_schema_name}' ORDER BY id"""
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def fetch_all_target_table_partitioning(self, settings):
        target_schema_name = settings.get('target_schema_name')
        table_name = self.config_parser.get_protocol_name_target_table_partitioning()
        query = f"""SELECT * FROM "{self.protocol_schema}"."{table_name}" WHERE target_schema_name = '{target_schema_name}' ORDER BY id"""
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def fetch_all_columns(self, settings):
        source_schema_name = settings.get('source_schema_name')
        table_name = self.config_parser.get_protocol_name_columns()
        query = f"""SELECT * FROM "{self.protocol_schema}"."{table_name}" WHERE source_schema_name = '{source_schema_name}' ORDER BY id"""
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def fetch_all_aliases(self, settings):
        source_schema_name = settings.get('source_schema_name')
        table_name = self.config_parser.get_protocol_name_aliases()
        query = f"""SELECT * FROM "{self.protocol_schema}"."{table_name}" WHERE source_schema_name = '{source_schema_name}' ORDER BY id"""
        # self.config_parser.print_log_message('DEBUG3', f"migrator_tables: fetch_all_aliases: ({source_schema_name}): {query}")
        cursor = self.protocol_connection.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        # self.config_parser.print_log_message('DEBUG3', f"migrator_tables: fetch_all_aliases: ({source_schema_name}): {rows}")
        cursor.close()
        return rows

    def create_ddl_tables(self):
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: create_ddl_tables: starting")
        self.protocol_connection.execute_query("DROP TABLE IF EXISTS ddl_tables, ddl_columns, ddl_indexes, ddl_foreign_keys, ddl_sequences, ddl_views, ddl_aliases, ddl_triggers CASCADE")

        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}".ddl_tables (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_schema_name VARCHAR,
                source_table_name VARCHAR,
                source_partition_columns VARCHAR,
                source_partition_ranges VARCHAR,
                source_table_sql TEXT,
                source_table_comment TEXT
            )
        """)

        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}".ddl_columns (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_schema_name VARCHAR,
                source_table_name VARCHAR,
                source_column_name VARCHAR,
                source_data_type VARCHAR,
                source_is_nullable BOOLEAN,
                source_default_value VARCHAR,
                source_pk_indicator BOOLEAN,
                source_is_identity BOOLEAN,
                source_column_sql TEXT,
                source_column_comment TEXT
            )
        """)

        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}".ddl_indexes (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_schema_name VARCHAR,
                source_table_name VARCHAR,
                source_index_name VARCHAR,
                source_is_unique BOOLEAN,
                source_columns_list VARCHAR,
                source_index_sql TEXT,
                source_index_comment TEXT
            )
        """)

        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}".ddl_foreign_keys (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_schema_name VARCHAR,
                source_table_name VARCHAR,
                source_fk_name VARCHAR,
                source_columns_list VARCHAR,
                source_ref_schema_name VARCHAR,
                source_ref_table_name VARCHAR,
                source_ref_columns_list VARCHAR,
                source_fk_sql TEXT,
                source_fk_comment TEXT
            )
        """)

        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}".ddl_sequences (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_schema_name VARCHAR,
                source_seq_name VARCHAR,
                source_table_name VARCHAR,
                source_column_name VARCHAR,
                source_start_value BIGINT,
                source_increment_by BIGINT,
                source_minvalue BIGINT,
                source_maxvalue BIGINT,
                source_cache BIGINT,
                source_is_cycled BOOLEAN,
                source_ddl_text VARCHAR,
                source_seq_comment TEXT
            )
        """)

        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}".ddl_views (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_schema_name VARCHAR,
                source_view_name VARCHAR,
                source_view_sql TEXT,
                source_view_comment TEXT
            )
        """)

        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}".ddl_aliases (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_schema_name VARCHAR,
                source_alias_name VARCHAR,
                source_target_schema VARCHAR,
                source_target_name VARCHAR,
                source_alias_sql TEXT,
                source_alias_comment TEXT,
                alias_target_type VARCHAR,
                target_schema_name VARCHAR,
                target_alias_name VARCHAR,
                target_referenced_schema_name VARCHAR,
                target_referenced_table_name VARCHAR,
                target_referenced_column_name VARCHAR
            )
        """)

        self.protocol_connection.execute_query(f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}".ddl_triggers (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_schema_name VARCHAR,
                source_trigger_name VARCHAR,
                source_ddl_text VARCHAR,
                source_trigger_sql TEXT,
                source_trigger_comment TEXT
            )
        """)

        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: create_ddl_tables: Tables created in schema {self.protocol_schema}")


    def insert_ddl_tables(self, settings):
        func_run_id = uuid.uuid4()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."ddl_tables"
            (source_schema_name, source_table_name, source_partition_columns, source_partition_ranges, source_table_sql, source_table_comment)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        params = (settings.get('source_schema_name'), settings.get('source_table_name'), settings.get('source_partition_columns'), settings.get('source_partition_ranges'), settings.get('source_table_sql'), settings.get('source_table_comment'))
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_ddl_tables: ({func_run_id}): inserting: {params}")
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row_id = cursor.fetchone()[0]
            cursor.close()
            return row_id
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_ddl_tables: ({func_run_id}): Exception: {e}")
            raise

    def insert_ddl_columns(self, settings):
        func_run_id = uuid.uuid4()
        source_schema_name = settings['source_schema_name']
        source_table_name = settings['source_table_name']
        source_column_name = settings['source_column_name']
        source_data_type = settings['source_data_type']
        source_is_nullable = settings.get('source_is_nullable', True)
        source_default_value = settings.get('source_default_value', None)
        source_pk_indicator = settings.get('source_pk_indicator', False)
        source_column_sql = settings.get('source_column_sql', None)
        source_column_comment = settings.get('source_column_comment', None)
        source_is_identity = settings.get('source_is_identity', False)

        query = f"""
            INSERT INTO "{self.protocol_schema}"."ddl_columns"
            (source_schema_name, source_table_name, source_column_name, source_data_type,
            source_is_nullable, source_default_value, source_pk_indicator, source_is_identity,
            source_column_sql, source_column_comment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        params = (source_schema_name, source_table_name, source_column_name, source_data_type,
                  source_is_nullable, source_default_value, source_pk_indicator,
                  source_is_identity, source_column_sql, source_column_comment)
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_ddl_columns: inserting: {params}")
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
            return row[0] if row else None
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_ddl_columns: Error inserting column {settings.get('source_column_name')} into ddl_columns.")
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_ddl_columns: Error: {e}")
            raise

    def insert_ddl_indexes(self, settings):
        func_run_id = uuid.uuid4()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."ddl_indexes"
            (source_schema_name, source_table_name, source_index_name, source_is_unique, source_columns_list, source_index_sql, source_index_comment)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        params = (settings.get('source_schema_name'), settings.get('source_table_name'), settings.get('source_index_name'), settings.get('source_is_unique'), settings.get('source_columns_list'), settings.get('source_index_sql'), settings.get('source_index_comment'))
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_ddl_indexes: inserting: {params}")
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row_id = cursor.fetchone()[0]
            cursor.close()
            return row_id
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_ddl_indexes: ({func_run_id}): Exception: {e}")
            raise

    def insert_ddl_foreign_keys(self, settings):
        func_run_id = uuid.uuid4()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."ddl_foreign_keys"
            (source_schema_name, source_table_name, source_fk_name, source_columns_list, source_ref_schema_name, source_ref_table_name, source_ref_columns_list, source_fk_sql, source_fk_comment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        params = (settings.get('source_schema_name'), settings.get('source_table_name'), settings.get('source_fk_name'), settings.get('source_columns_list'), settings.get('source_ref_schema_name'), settings.get('source_ref_table_name'), settings.get('source_ref_columns_list'), settings.get('source_fk_sql'), settings.get('source_fk_comment'))
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_ddl_foreign_keys: inserting: {params}")
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row_id = cursor.fetchone()[0]
            cursor.close()
            return row_id
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_ddl_foreign_keys: ({func_run_id}): Exception: {e}")
            raise

    def insert_ddl_sequences(self, settings):
        func_run_id = uuid.uuid4()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."ddl_sequences"
            (source_schema_name, source_seq_name, source_table_name, source_column_name, source_start_value, source_increment_by, source_minvalue, source_maxvalue, source_cache, source_is_cycled, source_ddl_text, source_seq_comment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        params = (settings.get('source_schema_name'), settings.get('source_seq_name'), settings.get('source_table_name'), settings.get('source_column_name'), settings.get('source_start_value'), settings.get('source_increment_by'), settings.get('source_minvalue'), settings.get('source_maxvalue'), settings.get('source_cache'), settings.get('source_is_cycled'), settings.get('source_ddl_text'), settings.get('source_seq_comment'))
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_ddl_sequences: inserting: {params}")
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row_id = cursor.fetchone()[0]
            cursor.close()
            return row_id
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_ddl_sequences: ({func_run_id}): Exception: {e}")
            raise

    def insert_ddl_views(self, settings):
        func_run_id = uuid.uuid4()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."ddl_views"
            (source_schema_name, source_view_name, source_view_sql, source_view_comment)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """
        params = (settings.get('source_schema_name'), settings.get('source_view_name'), settings.get('source_view_sql'), settings.get('source_view_comment'))
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_ddl_views: ({func_run_id}): inserting: {params}")
        try:
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_ddl_views: ({func_run_id}): open cursor")
            cursor = self.protocol_connection.connection.cursor()
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_ddl_views: ({func_run_id}): execute query")
            cursor.execute(query, params)
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_ddl_views: ({func_run_id}): fetchone")
            row_id = cursor.fetchone()[0]
            self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_ddl_views: ({func_run_id}): close cursor")
            cursor.close()
            return row_id
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_ddl_views: ({func_run_id}): Exception: {e}\n{traceback.format_exc()}")
            raise

    def insert_ddl_aliases(self, settings):
        func_run_id = uuid.uuid4()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."ddl_aliases"
            (source_schema_name, source_alias_name, source_target_schema, source_target_name, source_alias_sql, source_alias_comment, alias_target_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        params = (settings.get('source_schema_name'), settings.get('source_alias_name'), settings.get('source_target_schema'), settings.get('source_target_name'), settings.get('source_alias_sql'), settings.get('source_alias_comment'), settings.get('alias_target_type'))
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_ddl_aliases: inserting: {params}")
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row_id = cursor.fetchone()[0]
            cursor.close()
            return row_id
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_ddl_aliases: ({func_run_id}): Exception: {e}")
            raise

    def insert_ddl_triggers(self, settings):
        func_run_id = uuid.uuid4()
        query = f"""
            INSERT INTO "{self.protocol_schema}"."ddl_triggers"
            (source_schema_name, source_trigger_name, source_ddl_text, source_trigger_sql, source_trigger_comment)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """
        params = (settings.get('source_schema_name'), settings.get('source_trigger_name'), settings.get('source_ddl_text'), settings.get('source_trigger_sql'), settings.get('source_trigger_comment'))
        self.config_parser.print_log_message('DEBUG3', f"migrator_tables: insert_ddl_triggers: inserting: {params}")
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            row_id = cursor.fetchone()[0]
            cursor.close()
            return row_id
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_ddl_triggers: ({func_run_id}): Exception: {e}")
            raise

    def update_ddl_comment(self, settings):
        obj_type = settings.get('object_type')
        schema = settings.get('source_schema_name')
        name = settings.get('source_name')
        col_name = settings.get('source_column_name')
        comment = settings.get('comment')

        try:
            cursor = self.protocol_connection.connection.cursor()
            if obj_type == 'TABLE':
                query = f"""UPDATE "{self.protocol_schema}"."ddl_tables" SET source_table_comment = %s WHERE source_schema_name = %s AND source_table_name = %s"""
                cursor.execute(query, (comment, schema, name))
            elif obj_type == 'COLUMN':
                query = f"""UPDATE "{self.protocol_schema}"."ddl_columns" SET source_column_comment = %s WHERE source_schema_name = %s AND source_table_name = %s AND source_column_name = %s"""
                cursor.execute(query, (comment, schema, name, col_name))
            elif obj_type == 'INDEX':
                query = f"""UPDATE "{self.protocol_schema}"."ddl_indexes" SET source_index_comment = %s WHERE source_schema_name = %s AND source_index_name = %s"""
                cursor.execute(query, (comment, schema, name))
            elif obj_type == 'VIEW':
                query = f"""UPDATE "{self.protocol_schema}"."ddl_views" SET source_view_comment = %s WHERE source_schema_name = %s AND source_view_name = %s"""
                cursor.execute(query, (comment, schema, name))
            elif obj_type == 'ALIAS':
                query = f"""UPDATE "{self.protocol_schema}"."ddl_aliases" SET source_alias_comment = %s WHERE source_schema_name = %s AND source_alias_name = %s"""
                cursor.execute(query, (comment, schema, name))
            elif obj_type == 'TRIGGER':
                query = f"""UPDATE "{self.protocol_schema}"."ddl_triggers" SET source_trigger_comment = %s WHERE source_schema_name = %s AND source_trigger_name = %s"""
                cursor.execute(query, (comment, schema, name))
            elif obj_type == 'SEQUENCE':
                query = f"""UPDATE "{self.protocol_schema}"."ddl_sequences" SET source_seq_comment = %s WHERE source_schema_name = %s AND source_seq_name = %s"""
                cursor.execute(query, (comment, schema, name))
            cursor.close()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: update_ddl_comment: Exception: {e}")
            raise
    def create_table_for_validation(self):
        query = f"""
            CREATE TABLE IF NOT EXISTS "{self.protocol_schema}"."{self.config_parser.get_protocol_name_validation()}" (
                id SERIAL PRIMARY KEY,
                target_schema_name text,
                target_table_name text,
                row_logic boolean,
                row_msg text,
                table_hash_logic boolean,
                table_msg text,
                row_hash_logic boolean,
                row_hash_msg text,
                lob_size_logic boolean,
                lob_size_msg text,
                passed boolean,
                validated_at timestamp default current_timestamp
            )
        """
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query)
            cursor.close()
            self.protocol_connection.connection.commit()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: create_table_for_validation: Error: {e}")
            raise

    def insert_validation_result(self, settings):
        target_schema_name = settings.get('target_schema_name')
        target_table_name = settings.get('target_table_name')
        row_logic = settings.get('row_logic')
        row_msg = settings.get('row_msg')
        table_hash_logic = settings.get('table_hash_logic')
        table_msg = settings.get('table_msg')
        row_hash_logic = settings.get('row_hash_logic')
        row_hash_msg = settings.get('row_hash_msg')
        lob_size_logic = settings.get('lob_size_logic')
        lob_size_msg = settings.get('lob_size_msg')
        passed = settings.get('passed')

        query = f"""
            INSERT INTO "{self.protocol_schema}"."{self.config_parser.get_protocol_name_validation()}"
            (target_schema_name, target_table_name, row_logic, row_msg, table_hash_logic, table_msg, row_hash_logic, row_hash_msg, lob_size_logic, lob_size_msg, passed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (target_schema_name, target_table_name, row_logic, row_msg, table_hash_logic, table_msg, row_hash_logic, row_hash_msg, lob_size_logic, lob_size_msg, passed)
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query, params)
            cursor.close()
            self.protocol_connection.connection.commit()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: insert_validation_result: Error: {e}")
            raise

    def print_validation_summary(self, val_logger=None):
        def log_info(msg):
            if val_logger:
                val_logger.info(msg)
            else:
                self.config_parser.print_log_message('INFO', msg)

        query = f"""
            SELECT target_schema_name, target_table_name, row_logic, row_msg, table_hash_logic, table_msg, row_hash_logic, row_hash_msg, lob_size_logic, lob_size_msg, passed
            FROM "{self.protocol_schema}"."{self.config_parser.get_protocol_name_validation()}"
            ORDER BY target_schema_name, target_table_name
        """
        try:
            cursor = self.protocol_connection.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()

            log_info("=========================================")
            log_info("       Data Validation Summary           ")
            log_info("=========================================")

            total = len(results)
            passed_count = sum(1 for r in results if r[10])
            failed_count = total - passed_count

            row_count_tests = sum(1 for r in results if r[2] is not None)
            row_count_pass = sum(1 for r in results if r[2] is True)
            row_count_fail = sum(1 for r in results if r[2] is False)

            table_hash_tests = sum(1 for r in results if r[4] is not None)
            table_hash_pass = sum(1 for r in results if r[4] is True)
            table_hash_fail = sum(1 for r in results if r[4] is False)

            row_hash_tests = sum(1 for r in results if r[6] is not None)
            row_hash_pass = sum(1 for r in results if r[6] is True)
            row_hash_fail = sum(1 for r in results if r[6] is False)

            lob_size_tests = sum(1 for r in results if r[8] is not None)
            lob_size_pass = sum(1 for r in results if r[8] is True)
            lob_size_fail = sum(1 for r in results if r[8] is False)

            log_info(f"Total Tables Validated: {total}")
            log_info(f"Tables Passed: {passed_count}")
            log_info(f"Tables Failed: {failed_count}")

            log_info("--- Test Category Totals ---")
            if row_count_tests > 0:
                log_info(f"Row Counts   : {row_count_pass} Passed, {row_count_fail} Failed (Total: {row_count_tests})")
            if table_hash_tests > 0:
                log_info(f"Table Hashes : {table_hash_pass} Passed, {table_hash_fail} Failed (Total: {table_hash_tests})")
            if row_hash_tests > 0:
                log_info(f"Row Hashes   : {row_hash_pass} Passed, {row_hash_fail} Failed (Total: {row_hash_tests})")
            if lob_size_tests > 0:
                log_info(f"LOB Sizes    : {lob_size_pass} Passed, {lob_size_fail} Failed (Total: {lob_size_tests})")

            if total > 0:
                log_info("--- Validation Details ---")
                for r in results:
                    status = "PASS" if r[10] else "FAIL"
                    log_info(f"[{status}] Table: {r[0]}.{r[1]}")
                    if r[2] is not None:
                        log_info(f"  Row Counts: {'[PASS]' if r[2] else '[FAIL]'} {r[3].strip()}")
                    if r[4] is not None:
                        log_info(f"  Table Checksum: {'[PASS]' if r[4] else '[FAIL]'} {r[5].strip()}")
                    if r[6] is not None:
                        log_info(f"  Row Checksums: {'[PASS]' if r[6] else '[FAIL]'} {r[7].strip()}")
                    if r[8] is not None:
                        log_info(f"  LOB Sizes: {'[PASS]' if r[8] else '[FAIL]'} {r[9].strip()}")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"migrator_tables: print_validation_summary: Error: {e}")

if __name__ == "__main__":
    print("This script is not meant to be run directly")
