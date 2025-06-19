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

from credativ_pg_migrator.database_connector import DatabaseConnector
from credativ_pg_migrator.migrator_logging import MigratorLogger
import cx_Oracle
import traceback
from tabulate import tabulate

class OracleConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        if source_or_target != 'source':
            raise ValueError("Oracle is only supported as a source database")

        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.on_error_action = self.config_parser.get_on_error_action()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger

    def connect(self):
        connection_string = self.config_parser.get_connect_string(self.source_or_target)
        username = self.config_parser.get_db_config(self.source_or_target)['username']
        try:
            if username == 'SYS':
                self.connection = cx_Oracle.connect(user=username,
                                                    password=self.config_parser.get_db_config(self.source_or_target)['password'],
                                                    dsn=connection_string,
                                                    encoding="UTF-8",
                                                    mode=cx_Oracle.SYSDBA)
            else:
                self.connection = cx_Oracle.connect(user=username,
                                                    password = self.config_parser.get_db_config(self.source_or_target)['password'],
                                                    dsn=connection_string,
                                                    encoding="UTF-8")

        except Exception as e:
            self.config_parser.print_log_message('ERROR', "cx_Oracle module is not installed.")
            raise e
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error connecting to Oracle database: {e}")
            self.config_parser.print_log_message('ERROR', "Full stack trace:")
            self.config_parser.print_log_message('ERROR', traceback.format_exc())
            raise e

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
            SELECT table_name
            FROM all_tables
            WHERE owner = '{table_schema.upper()}'
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
                    'id': None,
                    'schema_name': table_schema,
                    'table_name': row[0],
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
        query = f"""
            SELECT
                column_id,
                column_name,
                data_type,
                char_length,
                data_precision,
                data_scale,
                nullable,
                data_default
            FROM all_tab_columns
            WHERE owner = '{table_schema.upper()}' AND table_name = '{table_name.upper()}'
            ORDER BY column_id
        """
        try:
            result = {}
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                column_id = row[0]
                column_name = row[1]
                data_type = row[2]
                character_maximum_length = row[3]
                data_precision = row[4]
                data_scale = row[5]
                column_nullable = row[6]
                column_default = row[7]
                column_type = data_type.upper()
                if self.is_string_type(column_type) and character_maximum_length is not None:
                    column_type += f"({character_maximum_length})"
                elif self.is_numeric_type(column_type) and data_precision is not None:
                    if data_scale is not None:
                        column_type += f"({data_precision}, {data_scale})"
                    else:
                        column_type += f"({data_precision})"

                result[column_id] = {
                    'column_name': column_name,
                    'data_type': data_type,
                    'column_type': column_type,
                    'character_maximum_length': character_maximum_length if self.is_string_type(data_type) else None,
                    'numeric_precision': data_precision if self.is_numeric_type(data_type) else None,
                    'numeric_scale': data_scale if self.is_numeric_type(data_type) else None,
                    'is_nullable': 'NO' if column_nullable == 'N' else 'YES',
                    'is_identity': 'NO',
                    'column_default_value': column_default,
                    'comment': '',
                }

                self.config_parser.print_log_message('DEBUG', f"Checking if default value is a sequence for column {column_name} ({column_default})...")
                if (isinstance(column_default, str)
                    and 'nextval' in column_default.lower()):
                    parts = column_default.replace('"', '').split(".")
                    if len(parts) == 3:
                        owner, seq_name, _ = parts
                        sequence_details = self.get_sequence_details(owner, seq_name)
                        if sequence_details:
                            self.config_parser.print_log_message('DEBUG', f"Found sequence {sequence_details['name']} for column {column_name}.")
                            result[column_id]['column_default_value'] = ""
                            result[column_id]['is_identity'] = 'YES'
                            # if data_type in ('NUMBER'):
                            #     result[column_id]['data_type'] = 'BIGINT'
                    ## TODO: insert_internal_data_types_substitutions
                    ## internal subtitution of this type breaks foreign key constraints

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
                'VARCHAR': 'VARCHAR',
                'VARCHAR2': 'VARCHAR',
                'NVARCHAR': 'VARCHAR',
                'NVARCHAR2': 'VARCHAR',
                'CHARACTER VARYING': 'VARCHAR',
                'CHAR': 'CHAR',
                'LONG VARCHAR': 'TEXT',
                'LONG NVARCHAR': 'TEXT',
                'NCHAR': 'CHAR',
                'LONG': 'TEXT',

                'NUMBER': 'NUMERIC',
                'FLOAT': 'FLOAT',
                'DOUBLE PRECISION': 'DOUBLE PRECISION',

                'DATE': 'DATE',
                'TIMESTAMP': 'TIMESTAMP',
                'TIMESTAMP(6)': 'TIMESTAMP',

                'CLOB': 'TEXT',
                'BLOB': 'BYTEA',
                'LONG RAW': 'BYTEA',

                'BOOLEAN': 'BOOLEAN',
                'INTERVAL': 'INTERVAL',

                'SERIAL': 'SERIAL',
                'BIGSERIAL': 'BIGSERIAL',
                'INT': 'INTEGER',
                'BIGINT': 'BIGINT',
                'INTEGER': 'INTEGER',
                'SMALLINT': 'SMALLINT',
                'REAL': 'REAL',
                'DECIMAL': 'DECIMAL',
                'NUMBER': 'NUMERIC',
            }
        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

        return types_mapping

    def get_create_table_sql(self, settings):
        return ""

    def is_string_type(self, column_type: str) -> bool:
        column_type_upper = column_type.upper()
        return 'CHAR' in column_type_upper or 'VARCHAR' in column_type_upper or 'LONG' in column_type_upper or 'TEXT' in column_type_upper or 'CLOB' in column_type_upper

    def is_numeric_type(self, column_type: str) -> bool:
        numeric_types = ['BIGINT', 'INTEGER', 'INT', 'TINYINT', 'SMALLINT', 'FLOAT', 'DOUBLE PRECISION', 'DECIMAL', 'NUMERIC', 'REAL', 'NUMBER', 'SERIAL', 'BIGSERIAL']
        return column_type.upper() in numeric_types

    def get_sequence_details(self, sequence_owner, sequence_name):
        query = f"""
            SELECT
                sequence_name,
                min_value,
                max_value,
                increment_by,
                cycle_flag,
                order_flag,
                cache_size,
                last_number
            FROM all_sequences
            WHERE sequence_owner = '{sequence_owner.upper()}'
            AND sequence_name = '{sequence_name.upper()}'
        """
        try:
            # self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            # self.disconnect()
            if result:
                return {
                    'name': result[0],
                    'min_value': result[1],
                    'max_value': result[2],
                    'increment_by': result[3],
                    'cycle': result[4],
                    'order': result[5],
                    'cache_size': result[6],
                    'last_value': result[7],
                    'comment': ''
                }
            else:
                return None
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def migrate_table(self, migrate_target_connection, settings):
        return 0

    def fetch_indexes(self, settings):
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

        table_indexes = {}
        order_num = 1
        hidden_columns_count = 0

        ## for the future reference - oracle function to get DDL
        # SELECT DBMS_METADATA.GET_DDL('INDEX', index_name, table_owner) AS ddl
        # FROM   dba_indexes
        # WHERE  table_owner = 'C##CHINOOK'
        # AND  table_name = 'ALBUM';
        # 'TABLE', 'INDEX', 'VIEW', 'SEQUENCE', 'PACKAGE', 'FUNCTION', 'PROCEDURE', 'CONSTRAINT', 'TRIGGER', 'SYNONYM'

        index_query = f"""
            SELECT
                ai.index_name,
                c.constraint_type,
                ai.index_type,
                ai.uniqueness,
                listagg(CASE WHEN coalesce(cols.HIDDEN_COLUMN, 'NO') = 'YES' THEN '('|| aic.column_name ||')' ELSE '"'|| aic.column_name ||'"' END, ', ')
                    WITHIN GROUP (ORDER BY aic.column_position) AS indexed_columns,
                listagg(CASE WHEN coalesce(cols.HIDDEN_COLUMN, 'NO') = 'YES' THEN '('|| aic.column_name ||') '|| aic.descend ELSE '"'|| aic.column_name ||'" '|| aic.descend END, ', ')
                    WITHIN GROUP (ORDER BY aic.column_position) AS indexed_columns_orders,
                sum(CASE WHEN coalesce(cols.HIDDEN_COLUMN, 'NO') = 'YES' THEN 1 ELSE 0 END) AS hidden_columns_count
            FROM all_indexes ai
            JOIN all_ind_columns aic
            ON ai.owner = aic.index_owner AND ai.index_name = aic.index_name
            LEFT JOIN all_tab_cols cols
            ON cols.owner = ai.table_owner AND cols.table_name = ai.table_name AND cols.column_name = aic.column_name
            AND ai.table_owner = aic.table_owner AND ai.table_name = aic.table_name
            LEFT JOIN dba_constraints c
            ON c.owner = ai.owner AND c.table_name = ai.table_name AND c.constraint_name = ai.index_name
            WHERE
                ai.table_owner = '{source_table_schema.upper()}'
                AND ai.table_name = '{source_table_name.upper()}'
            GROUP BY
                ai.owner,
                ai.index_name,
                c.constraint_type,
                ai.table_owner,
                ai.table_name,
                ai.index_type,
                ai.uniqueness
            ORDER BY
                ai.index_name
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(index_query)
            for row in cursor.fetchall():
                index_name = row[0]
                constraint_type = row[1]
                index_type = row[2]
                uniqueness = row[3]
                columns_list = row[4]
                columns_list_orders = row[5]
                hidden_columns_count += int(row[6])

                if index_name not in table_indexes:
                    table_indexes[order_num] = {
                        'index_name': index_name,
                        'index_type': 'PRIMARY KEY' if constraint_type == 'P' else 'UNIQUE' if uniqueness == 'UNIQUE' else 'INDEX',
                        'index_owner': source_table_schema,
                        'index_columns': columns_list if constraint_type == 'P' else columns_list_orders,
                        'index_comment': '',
                        'index_sql': '',
                        'index_hidden_columns_count': int(row[6]),
                    }
                order_num += 1

            for order_num, index_info in table_indexes.items():
                # Fetch the DDL for each index
                try:
                    query = f"""SELECT DBMS_METADATA.GET_DDL('INDEX', '{index_info['index_name'].upper()}', '{source_table_schema.upper()}') FROM dual"""
                    cursor.execute(query)
                    ddl = cursor.fetchone()[0]
                    if ddl:
                        ddl = ddl.decode('utf-8') if isinstance(ddl, bytes) else ddl
                        table_indexes[order_num]['index_sql'] = f"{ddl}"
                        self.config_parser.print_log_message('DEBUG', f"Fetched DDL for index {index_info['index_name']}: {ddl}")
                except Exception as e:
                    self.config_parser.print_log_message('ERROR', f"Error fetching DDL for index {index_info['index_name']}: {e}")
                    table_indexes[order_num]['index_sql'] = f"Error fetching DDL: {e}"

            if hidden_columns_count > 0:
                self.config_parser.print_log_message('INFO', f"Table {source_table_schema}.{source_table_name} has {hidden_columns_count} hidden columns in indexes.")
                try:
                    query = f"""SELECT COLUMN_NAME, DATA_DEFAULT FROM all_tab_cols WHERE owner = '{source_table_schema.upper()}' AND table_name = '{source_table_name.upper()}' AND hidden_column = 'YES'"""
                    cursor.execute(query)
                    hidden_columns = cursor.fetchall()
                    for col in hidden_columns:
                        col_name = col[0]
                        col_default = col[1] if col[1] else 'NULL'
                        self.config_parser.print_log_message('DEBUG', f"Hidden column: {col_name}, Default value: {col_default}")

                        for order_num, index_info in table_indexes.items():
                            if index_info['index_hidden_columns_count'] > 0:
                                if col_name in index_info['index_columns']:
                                    index_info['index_columns'] = index_info['index_columns'].replace(col_name, f"{col_default}")
                                    self.config_parser.print_log_message('DEBUG', f"Updated index {index_info['index_name']} with hidden column {col_name} and default value {col_default}")
                except Exception as e:
                    self.config_parser.print_log_message('ERROR', f"Error fetching hidden columns for table {source_table_schema}.{source_table_name}: {e}")

            cursor.close()
            self.disconnect()
            return table_indexes

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {index_query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_index_sql(self, settings):
        return ""

    def fetch_constraints(self, settings):
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

        order_num = 1
        table_constraints = {}
        constraints_query = f"""
            SELECT
                fk_cons.constraint_name AS fk_constraint_name,
                fk_cons.delete_rule,
                fk_cons.status,
                    listagg('"'||fk_col.column_name||'"', ', ') WITHIN GROUP (ORDER BY fk_col.position) AS fk_columns,
                pk_cons.owner AS pk_owner,
                pk_cons.table_name AS pk_table_name,
                pk_cons.constraint_name AS pk_constraint_name,
                    listagg('"'||pk_col.column_name||'"', ', ') WITHIN GROUP (ORDER BY pk_col.position) AS pk_columns
            FROM
                all_constraints fk_cons
            JOIN
                all_cons_columns fk_col ON fk_cons.owner = fk_col.owner
                                        AND fk_cons.constraint_name = fk_col.constraint_name
                                        AND fk_cons.table_name = fk_col.table_name
            JOIN
                all_constraints pk_cons ON fk_cons.r_owner = pk_cons.owner
                                        AND fk_cons.r_constraint_name = pk_cons.constraint_name
            JOIN
                all_cons_columns pk_col ON pk_cons.owner = pk_col.owner
                                        AND pk_cons.constraint_name = pk_col.constraint_name
                                        AND pk_cons.table_name = pk_col.table_name
                                        AND fk_col.position = pk_col.position -- Ensures correct order for composite keys
            WHERE
                fk_cons.constraint_type = 'R'
                AND fk_cons.owner = '{source_table_schema.upper()}'
                AND fk_cons.table_name = '{source_table_name.upper()}'
            GROUP BY
                fk_cons.constraint_name,
                fk_cons.delete_rule,
                fk_cons.status,
                pk_cons.owner,
                pk_cons.table_name,
                pk_cons.constraint_name
            ORDER BY
                fk_cons.constraint_name
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(constraints_query)
            for row in cursor.fetchall():
                constraint_name = row[0]
                delete_rule = row[1]
                status = row[2]  ## ENABLED
                fk_columns = row[3]
                pk_owner = row[4]
                pk_table_name = row[5]
                pk_constraint_name = row[6]  ## corresponds to the primary key constraint name
                pk_columns = row[7]
                constraint_type = 'FOREIGN KEY'

                if constraint_name not in table_constraints:
                    table_constraints[order_num] = {
                        'constraint_name': constraint_name,
                        'constraint_type': constraint_type,
                        'constraint_owner': source_table_schema,
                        'referenced_table_name': pk_table_name,
                        'referenced_table_schema': pk_owner,
                        'referenced_columns': pk_columns,
                        'constraint_columns': fk_columns,
                        'constraint_sql': '',
                        'constraint_comment': '',
                        'delete_rule': delete_rule,
                        'constraint_status': status,
                    }

                order_num += 1

            cursor.close()
            self.disconnect()
            return table_constraints
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {constraints_query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_constraint_sql(self, settings):
        return ""

    def fetch_triggers(self, table_id: int, table_schema: str, table_name: str):
        try:
            triggers = {}
            order_num = 1
            query = f"""
                SELECT
                    trigger_name,
                    trigger_type,
                    triggering_event,
                    status,
                    referencing_names
                FROM all_triggers
                WHERE table_owner = '{table_schema.upper()}'
                AND table_name = '{table_name.upper()}'
                ORDER BY trigger_name
            """
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                referencing = row[4]
                old_ref = ""
                new_ref = ""
                if referencing:
                    parts = referencing.split()
                    if "OLD" in parts:
                        old_ref = parts[parts.index("OLD") + 2]
                    if "NEW" in parts:
                        new_ref = parts[parts.index("NEW") + 2]

                triggers[order_num] = {
                    'id': None,
                    'name': row[0],
                    'event': row[2],
                    'row_statement': '',
                    'old': old_ref,
                    'new': new_ref,
                    'sql': '',
                    'comment': ''
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return triggers
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def convert_trigger(self, trig: str, settings: dict):
        # Placeholder for trigger conversion
        pass

    def fetch_funcproc_names(self, schema: str):
        # Placeholder for fetching function/procedure names
        return {}

    def fetch_funcproc_code(self, funcproc_id: int):
        # Placeholder for fetching function/procedure code
        return ""

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

    def fetch_sequences(self, table_schema: str, table_name: str):
        # Placeholder for fetching sequences
        return {}

    def fetch_views_names(self, source_schema: str):
        views = {}
        order_num = 1
        query = f"""
            SELECT view_name
            FROM all_views
            WHERE owner = '{source_schema.upper()}'
            ORDER BY view_name
        """
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
        query = f"""
            SELECT text
            FROM all_views
            WHERE owner = '{source_schema.upper()}'
            AND view_name = '{source_view_name.upper()}'
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            view_code = cursor.fetchone()[0]
            cursor.close()
            self.disconnect()
            return view_code
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def convert_view_code(self, settings: dict):
        view_code = settings['view_code']
        converted_view_code = view_code
        converted_view_code = converted_view_code.replace(f'''"{settings['source_schema'].upper()}".''', f'''"{settings['target_schema']}".''')
        return converted_view_code

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
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def execute_sql_script(self, script_path: str):
        try:
            with open(script_path, 'r') as file:
                script = file.read()
            cursor = self.connection.cursor()
            cursor.execute(script)
            cursor.close()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing SQL script: {script_path}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def begin_transaction(self):
        self.connection.begin()

    def commit_transaction(self):
        self.connection.commit()

    def rollback_transaction(self):
        self.connection.rollback()

    def get_rows_count(self, table_schema: str, table_name: str):
        query = f"SELECT COUNT(*) FROM {table_schema}.{table_name}"
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_table_size(self, table_schema: str, table_name: str):
        # Placeholder for fetching table size
        return None

    def fetch_user_defined_types(self, schema: str):
        # Placeholder for fetching user-defined types
        return {}

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
        table_schema = settings['table_schema']
        table_name = settings['table_name']
        output = ""
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT dbms_metadata.get_ddl('TABLE', '{table_name}', '{table_schema}') FROM dual")

            set_num = 1
            if cursor.description is not None:
                rows = cursor.fetchall()
                if rows:
                    output += f"Result set {set_num}:\n"
                    columns = [column[0] for column in cursor.description]
                    table = tabulate(rows, headers=columns, tablefmt="github")
                    output += table + "\n\n"
                    set_num += 1

            cursor.close()
            self.disconnect()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching table description for {table_schema}.{table_name}: {e}")
            raise

        return { 'table_description': output.strip() }


    def testing_select(self):
        return "SELECT 1 FROM DUAL"

    def get_database_version(self):
        query = "SELECT * FROM v$version"
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            version_info = cursor.fetchall()
            cursor.close()
            self.disconnect()
            return version_info
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_database_size(self):
        query = """
            SELECT SUM(bytes) / 1024 / 1024 AS size_mb
            FROM dba_data_files
            WHERE tablespace_name NOT IN ('SYSTEM', 'SYSAUX')
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            size_mb = cursor.fetchone()[0]
            cursor.close()
            self.disconnect()
            return size_mb
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_top10_biggest_tables(self, settings):
        query = f"""
            SELECT
                owner,
                segment_name,
                ROUND(SUM(bytes) / 1024 / 1024, 2) AS size_mb
            FROM dba_segments
            WHERE owner = '{settings['source_schema'].upper()}'
            AND segment_type = 'TABLE'
            GROUP BY owner, segment_name
            ORDER BY size_mb DESC
            FETCH FIRST 10 ROWS ONLY
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            tables = cursor.fetchall()
            cursor.close()
            self.disconnect()

            result = {}
            for order_num, row in enumerate(tables, start=1):
                result[order_num] = {
                    'owner': row[0],
                    'table_name': row[1],
                    'size_mb': row[2]
                }
            return result
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

if __name__ == "__main__":
    print("This script is not meant to be run directly")
