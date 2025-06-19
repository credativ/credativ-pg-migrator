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

import jaydebeapi
from jaydebeapi import Error
import pyodbc
from pyodbc import Error
from credativ_pg_migrator.database_connector import DatabaseConnector
from credativ_pg_migrator.migrator_logging import MigratorLogger
import re
import traceback

class MsSQLConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        if source_or_target not in ['source']:
            raise ValueError(f"MS SQL Server is only supported as a source database. Current value: {source_or_target}")

        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.on_error_action = self.config_parser.get_on_error_action()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger

    def connect(self):
        if self.config_parser.get_connectivity(self.source_or_target) == 'odbc':
            connection_string = self.config_parser.get_connect_string(self.source_or_target)
            self.connection = pyodbc.connect(connection_string)
        elif self.config_parser.get_connectivity(self.source_or_target) == 'jdbc':
            connection_string = self.config_parser.get_connect_string(self.source_or_target)
            username = self.config_parser.get_db_config(self.source_or_target)['username']
            password = self.config_parser.get_db_config(self.source_or_target)['password']
            jdbc_driver = self.config_parser.get_db_config(self.source_or_target)['jdbc']['driver']
            jdbc_libraries = self.config_parser.get_db_config(self.source_or_target)['jdbc']['libraries']
            self.connection = jaydebeapi.connect(
                jdbc_driver,
                connection_string,
                [username, password],
                jdbc_libraries
            )
        else:
            raise ValueError(f"Unsupported connectivity type: {self.config_parser.get_connectivity(self.source_or_target)}")
        self.connection.autocommit = True

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
                t.object_id AS table_id,
                s.name AS schema_name,
                t.name AS table_name
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = '{table_schema}'
            ORDER BY t.name
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
                    'schema_name': row[1],
                    'table_name': row[2],
                    'comment': ''
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
        if self.config_parser.get_system_catalog() == 'INFORMATION_SCHEMA':
            query = f"""
                SELECT
                    c.ordinal_position,
                    c.column_name,
                    c.data_type,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    c.is_nullable,
                    'NO' AS is_identity,
                    c.column_default
                FROM information_schema.columns c
                WHERE c.table_schema = '{table_schema}' AND c.table_name = '{table_name}'
                ORDER BY c.ordinal_position
            """
        elif self.config_parser.get_system_catalog() in ('SYS', 'NONE'):
            query = f"""
                SELECT
                    c.column_id AS ordinal_position,
                    c.name AS column_name,
                    t.name AS data_type,
                    c.max_length AS length,
                    c.precision AS numeric_precision,
                    c.scale AS numeric_scale,
                    c.is_nullable,
                    c.is_identity,
                    dc.definition AS default_value
                FROM sys.columns c
                JOIN sys.tables tb ON c.object_id = tb.object_id
                JOIN sys.schemas s ON tb.schema_id = s.schema_id
                JOIN sys.types t ON c.user_type_id = t.user_type_id
                LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
                WHERE s.name = '{table_schema}' AND tb.name = '{table_name}'
                ORDER BY c.column_id
            """
        else:
            raise ValueError(f"Unsupported system catalog: {self.config_parser.get_system_catalog()}")
        try:
            self.connect()
            cursor = self.connection.cursor()
            self.config_parser.print_log_message('DEBUG2', f"MSSQL: Reading columns for {table_schema}.{table_name}")
            cursor.execute(query)
            for row in cursor.fetchall():
                ordinal_position = row[0]
                column_name = row[1]
                data_type = row[2]
                character_maximum_length = row[3]
                numeric_precision = row[4]
                numeric_scale = row[5]
                is_nullable = row[6]
                is_identity = row[7]
                column_default = row[8]

                column_type = data_type.upper()
                if self.is_string_type(column_type) and character_maximum_length is not None:
                    column_type += f"({character_maximum_length})"
                elif self.is_numeric_type(column_type) and numeric_precision is not None and numeric_scale is not None:
                    column_type += f"({numeric_precision}, {numeric_scale})"
                elif self.is_numeric_type(column_type) and numeric_precision is not None:
                    column_type += f"({numeric_precision})"

                result[ordinal_position] = {
                    'column_name': column_name,
                    'data_type': data_type,
                    'column_type': column_type,
                    'character_maximum_length': character_maximum_length,
                    'numeric_precision': numeric_precision,
                    'numeric_scale': numeric_scale,
                    'is_nullable': is_nullable,
                    'is_identity': is_identity,
                    'column_default_value': column_default,
                    'comment': ''
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
                'UNIQUEIDENTIFIER': 'UUID',
                'ROWVERSION': 'BYTEA',
                'SQL_VARIANT': 'BYTEA',

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

    def fetch_indexes(self, settings):
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']
        table_indexes = {}
        order_num = 1
        query = f"""
            SELECT
                i.name AS index_name,
                i.is_unique,
                i.is_primary_key,
                STRING_AGG('"' + c.name + '"', ', ') WITHIN GROUP (ORDER BY ic.index_column_id) AS column_list
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE i.object_id = {source_table_id}
            GROUP BY i.name, i.is_unique, i.is_primary_key
            ORDER BY i.name
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)

            indexes = cursor.fetchall()

            for index in indexes:
                self.config_parser.print_log_message('DEBUG', f"Processing index: {index}")
                index_name = index[0].strip()
                index_unique = index[1]  ## integer 0 or 1
                index_primary_key = index[2]  ## integer 0 or 1
                index_columns = index[3].strip()
                index_owner = ''

                table_indexes[order_num] = {
                    'index_name': index_name,
                    'index_type': "PRIMARY KEY" if index_primary_key == 1 else "UNIQUE" if index_unique == 1 and index_primary_key == 0 else "INDEX",
                    'index_owner': index_owner,
                    'index_columns': index_columns,
                    'index_comment': ''
                }
                order_num += 1

            cursor.close()
            self.disconnect()
            return table_indexes

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_index_sql(self, settings):
        return ""

    def fetch_constraints(self, settings):
        """
        Fetches table constraints from the source database and prepares them for migration.
        MS SQL Server has several sys objects which show constraints:
        sys.key_constraints - primary key and unique constraints
        sys.check_constraints - check constraints
        sys.foreign_keys - foreign key constraints
        sys.default_constraints - default constraints
        """
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

        order_num = 1
        table_constraints = {}
        query = f"""
            WITH ConstraintColumns AS (
            SELECT
                fk.name AS constraint_name,
                STRING_AGG('"' + cc.name + '"', ', ') WITHIN GROUP (ORDER BY cc.column_id) AS constraint_columns
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            JOIN sys.columns cc ON fkc.parent_object_id = cc.object_id AND fkc.parent_column_id = cc.column_id
            WHERE fk.parent_object_id = {source_table_id}
            GROUP BY fk.name
            ),
            ReferencedColumns AS (
            SELECT
                fk.name AS constraint_name,
                STRING_AGG('"' + rc.name + '"', ', ') WITHIN GROUP (ORDER BY rc.column_id) AS referenced_columns
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
            WHERE fk.parent_object_id = {source_table_id}
            GROUP BY fk.name
            )
            SELECT
            fk.name AS constraint_name,
            'FOREIGN KEY' AS constraint_type,
            cc.constraint_columns,
            rt.name AS referenced_table,
            rc.referenced_columns,
            pt.name AS constraint_table
            FROM sys.foreign_keys fk
            JOIN ConstraintColumns cc ON fk.name = cc.constraint_name
            JOIN ReferencedColumns rc ON fk.name = rc.constraint_name
            JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
            JOIN sys.tables pt ON fk.parent_object_id = pt.object_id
            WHERE fk.parent_object_id = {source_table_id}
            ORDER BY fk.name
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)

            constraints = cursor.fetchall()

            for constraint in constraints:
                self.config_parser.print_log_message('DEBUG', f"Processing constraint: {constraint}")
                constraint_name = constraint[0].strip()
                constraint_type = constraint[1].strip()
                constraint_columns = constraint[2].strip()
                referenced_table = constraint[3].strip()
                referenced_columns = constraint[4].strip()
                constraint_owner = ''

                table_constraints[order_num] = {
                    'constraint_name': constraint_name,
                    'constraint_type': constraint_type,
                    'constraint_owner': constraint_owner,
                    'constraint_columns': constraint_columns,
                    'referenced_table_schema': '',
                    'referenced_table_name': referenced_table,
                    'referenced_columns': referenced_columns,
                    'constraint_sql': '',
                    'constraint_comment': ''
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

    def fetch_funcproc_names(self, schema: str):
        query = f"""
            SELECT
                p.object_id AS id,
                p.name AS name,
                CASE
                    WHEN p.type = 'P' THEN 'Procedure'
                    WHEN p.type = 'FN' THEN 'Function'
                END AS type
            FROM sys.objects p
            JOIN sys.schemas s ON p.schema_id = s.schema_id
            WHERE s.name = '{schema}' AND p.type IN ('P', 'FN')
            ORDER BY p.name
        """
        # ...existing code from SybaseASEConnector.fetch_funcproc_names...
        pass

    def fetch_funcproc_code(self, funcproc_id: int):
        query = f"""
            SELECT m.definition
            FROM sys.sql_modules m
            WHERE m.object_id = {funcproc_id}
        """
        # ...existing code from SybaseASEConnector.fetch_funcproc_code...
        pass

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

    def fetch_views_names(self, owner_name):
        views = {}
        order_num = 1
        query = f"""
            SELECT
                v.object_id AS id,
                s.name AS schema_name,
                v.name AS view_name
            FROM sys.views v
            JOIN sys.schemas s ON v.schema_id = s.schema_id
            WHERE s.name = '{owner_name}'
            ORDER BY v.name
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                views[order_num] = {
                    'id': row[0],
                    'schema_name': row[1],
                    'view_name': row[2],
                    'comment': ''
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return views
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_view_code(self, settings):
        view_id = settings['view_id']
        source_schema = settings['source_schema']
        source_view_name = settings['source_view_name']
        target_schema = settings['target_schema']
        target_view_name = settings['target_view_name']
        view_code = ''
        query = f"""
            SELECT m.definition
            FROM sys.sql_modules m
            WHERE m.object_id = {view_id}
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                view_code = row[0]
                self.config_parser.print_log_message('DEBUG', f"View code for {source_schema}.{source_view_name}: {view_code}")
                return view_code
            cursor.close()
            self.disconnect()
            return view_code
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def convert_view_code(self, settings: dict):
        view_code = settings['view_code']
        # ...existing code from SybaseASEConnector.convert_view_code...
        pass

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
                self.config_parser.print_log_message( 'INFO', f"Worker {worker_id}: Table {source_table} is empty - skipping data migration.")
                return 0
            else:
                part_name = 'migrate_table in batches using cursor'
                self.config_parser.print_log_message( 'INFO', f"Worker {worker_id}: Table {source_table} has {source_table_rows} rows - starting data migration.")

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
                    select_columns_list.append(f'''{col['column_name']}''')
                select_columns = ', '.join(select_columns_list)

                # Open a cursor and fetch rows in batches
                query = f"SELECT {select_columns} FROM {source_schema}.{source_table}"
                if migration_limitation:
                    query += f" WHERE {migration_limitation}"

                self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Fetching data with cursor using query: {query}")

                cursor = self.connection.cursor()
                cursor.execute(query)
                total_inserted_rows = 0
                while True:
                    records = cursor.fetchmany(batch_size)
                    if not records:
                        break
                    self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Fetched {len(records)} rows from source table '{source_table}' using cursor")

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
                    self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Inserting {len(records)} rows into target table '{target_table}'")
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
                    self.config_parser.print_log_message( 'INFO', f"Worker {worker_id}: Inserted {inserted_rows} (total: {total_inserted_rows} from: {source_table_rows} ({round(total_inserted_rows/source_table_rows*100, 2)}%)) rows into target table '{target_table}'")

                target_table_rows = migrate_target_connection.get_rows_count(target_schema, target_table)
                self.config_parser.print_log_message( 'INFO', f"Worker {worker_id}: Target table {target_schema}.{target_table} has {target_table_rows} rows")
                migrator_tables.update_data_migration_status(protocol_id, True, 'OK', target_table_rows)
                cursor.close()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Error during {part_name} -> {e}")
            self.config_parser.print_log_message('ERROR', "Full stack trace:")
            self.config_parser.print_log_message('ERROR', traceback.format_exc())
            raise e
        finally:
            self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Finished processing table {source_table}. Inserted {inserted_rows} rows into target table '{target_table}'.")
            return target_table_rows

    def fetch_triggers(self, schema_name, table_name):
        # ...existing code from SybaseASEConnector.fetch_triggers...
        pass

    def convert_trigger(self, trigger_name, trigger_code, source_schema, target_schema, table_list):
        # ...existing code from SybaseASEConnector.convert_trigger...
        pass

    def execute_query(self, query: str, params=None):
        # ...existing code from SybaseASEConnector.execute_query...
        pass

    def execute_sql_script(self, script_path: str):
        # ...existing code from SybaseASEConnector.execute_sql_script...
        pass

    def begin_transaction(self):
        # ...existing code from SybaseASEConnector.begin_transaction...
        pass

    def commit_transaction(self):
        # ...existing code from SybaseASEConnector.commit_transaction...
        pass

    def rollback_transaction(self):
        # ...existing code from SybaseASEConnector.rollback_transaction...
        pass

    def handle_error(self, e, description=None):
        # ...existing code from SybaseASEConnector.handle_error...
        pass

    def get_rows_count(self, table_schema: str, table_name: str):
        query = f"""SELECT COUNT(*) FROM [{table_schema}].[{table_name}]"""
        # query = f"""SELECT COUNT(*) FROM {table_schema}.{table_name} """
        self.config_parser.print_log_message('DEBUG', f"get_rows_count query: {query}")
        cursor = self.connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_table_size(self, table_schema: str, table_name: str):
        """
        Returns a size of the table in bytes
        """
        pass

    def fetch_sequences(self):
        query = """
            SELECT
                s.name AS sequence_name,
                s.object_id AS sequence_id,
                s.start_value AS start_value,
                s.increment AS increment_value,
                s.min_value AS min_value,
                s.max_value AS max_value,
                s.cycle_option AS cycle_option
            FROM sys.sequences s
        """
        # ...existing code from SybaseASEConnector.fetch_sequences...
        pass

    def get_sequence_details(self, sequence_owner, sequence_name):
        # Placeholder for fetching sequence details
        return {}

    def fetch_user_defined_types(self, schema: str):
        query = """
            SELECT
                s.name AS type_name,
                s.system_type_id AS system_type_id,
                s.user_type_id AS user_type_id,
                s.max_length AS max_length,
                s.is_nullable AS is_nullable
            FROM sys.types s
            WHERE s.is_user_defined = 1
        """
        # ...existing code from SybaseASEConnector.fetch_user_defined_types...
        pass

    def get_sequence_current_value(self, sequence_name: str):
        pass

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
        return "SELECT 1"

    def get_database_version(self):
        query = "SELECT @@VERSION"
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        version = cursor.fetchone()[0]
        cursor.close()
        self.disconnect()
        return version

    def get_database_size(self):
        query = "SELECT SUM(size * 8 * 1024) FROM sys.master_files WHERE database_id = DB_ID()"
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        size = cursor.fetchone()[0]
        cursor.close()
        self.disconnect()
        return size

    def get_top10_biggest_tables(self, settings):
        source_schema = settings['source_schema']
        query = """
            SELECT TOP 10
                s.name AS schema_name,
                t.name AS table_name,
                SUM(p.rows) AS row_count,
                SUM(a.total_pages) * 8 AS total_size_kb
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
            JOIN sys.allocation_units a ON p.partition_id = a.container_id
            WHERE s.name = '{source_schema}'
            GROUP BY s.name, t.name
            ORDER BY total_size_kb DESC
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query.format(source_schema=source_schema))
            rows = cursor.fetchall()
            biggest_tables = {}
            order_num = 1
            for row in rows:
                biggest_tables[order_num] = {
                    'schema_name': row[0],
                    'table_name': row[1],
                    'row_count': row[2],
                    'total_size_kb': row[3]
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return biggest_tables
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise e

if __name__ == "__main__":
    print("This script is not meant to be run directly")
