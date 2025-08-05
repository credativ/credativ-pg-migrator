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

import time
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from credativ_pg_migrator.database_connector import DatabaseConnector
from credativ_pg_migrator.migrator_logging import MigratorLogger
import traceback
import re
import datetime

class PostgreSQLConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger
        self.session_settings = self.prepare_session_settings()

    def connect(self):
        connection_string = self.config_parser.get_connect_string(self.source_or_target)
        self.connection = psycopg2.connect(connection_string)
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
        self.config_parser.print_log_message('DEBUG3', f"Reading table names for {schema}")
        self.config_parser.print_log_message('DEBUG3', f"Query: {query}")
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
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_table_columns(self, settings) -> dict:
        table_schema = settings['table_schema']
        table_name = settings['table_name']
        result = {}
        try:
            query =f"""
                    SELECT
                        c.ordinal_position,
                        c.column_name,
                        c.data_type,
                        c.character_maximum_length,
                        c.numeric_precision,
                        c.numeric_scale,
                        c.is_identity,
                        c.is_nullable,
                        c.column_default,
                        u.udt_schema,
                        u.udt_name,
                        col_description((c.table_schema||'.'||c.table_name)::regclass::oid, c.ordinal_position) as column_comment,
                        is_generated
                    FROM information_schema.columns c
                    LEFT JOIN information_schema.column_udt_usage u ON c.table_schema = u.table_schema
                        AND c.table_name = u.table_name
                        AND c.column_name = u.column_name
                        AND c.udt_name = u.udt_name
                    WHERE c.table_name = '{table_name}' AND c.table_schema = '{table_schema}'
                """
            self.connect()
            cursor = self.connection.cursor()
            self.config_parser.print_log_message('DEBUG2', f"PostgreSQL: Reading columns for {table_schema}.{table_name}")
            cursor.execute(query)
            for row in cursor.fetchall():
                ordinal_position = row[0]
                column_name = row[1]
                data_type = row[2]
                character_maximum_length = row[3]
                numeric_precision = row[4]
                numeric_scale = row[5]
                is_identity = row[6]
                is_nullable = row[7]
                column_default = row[8]
                udt_schema = row[9]
                udt_name = row[10]
                column_comment = row[11]
                is_generated = row[12]
                column_type = data_type
                if self.is_string_type(data_type) and character_maximum_length:
                    column_type = f"{data_type}({character_maximum_length})"
                elif self.is_numeric_type(data_type) and numeric_precision and numeric_scale:
                    column_type = f"{data_type}({numeric_precision},{numeric_scale})"
                elif self.is_numeric_type(data_type) and numeric_precision and not numeric_scale:
                    column_type = f"{data_type}({numeric_precision})"
                result[ordinal_position] = {
                    'column_name': column_name,
                    'is_nullable': is_nullable,
                    'column_default_name': '',
                    'column_default_value': column_default,
                    'data_type': data_type,
                    'column_type': column_type,
                    'basic_data_type': data_type if data_type not in ('USER-DEFINED', 'DOMAIN') else '',
                    'is_identity': is_identity,
                    'character_maximum_length': character_maximum_length,
                    'numeric_precision': numeric_precision,
                    'numeric_scale': numeric_scale,
                    'udt_schema': udt_schema,
                    'udt_name': udt_name,
                    'column_comment': column_comment,
                    'is_generated_virtual': 'NO',
                    'is_generated_stored': is_generated,
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
        if target_db_type != 'postgresql':
            raise ValueError(f"Unsupported target database type: {target_db_type}")
        return types_mapping

    def get_create_table_sql(self, settings):
        source_schema = settings['source_schema']
        source_table = settings['source_table']
        source_table_id = settings['source_table_id']
        target_schema = self.config_parser.convert_names_case(settings['target_schema'])
        target_table_name = self.config_parser.convert_names_case(settings['target_table'])
        # source_columns = settings['source_columns']
        converted = settings['target_columns']
        migrator_tables = settings['migrator_tables']
        create_table_sql = ""
        create_table_sql_parts = []

        self.config_parser.print_log_message('DEBUG', f"Creating DDL for table {target_schema}.{target_table_name}, case handling: {self.config_parser.get_names_case_handling()}")

        for _, column_info in converted.items():

            column_name = self.config_parser.convert_names_case(column_info['column_name'])

            self.config_parser.print_log_message('DEBUG3', f"Creating DDL for table {target_schema}.{target_table_name}, column_info: {column_info}")

            if column_info['is_hidden_column'] == 'YES':
                self.config_parser.print_log_message('DEBUG', f"Skipping hidden column {column_name}: {column_info}")
                continue

            create_column_sql = ""
            column_data_type = column_info['data_type'].upper()
            is_identity = column_info['is_identity']
            # if column_info['column_type_substitution'] != '':
            #     column_data_type = column_info['column_type_substitution'].upper()
            if column_info['data_type'] == 'USER-DEFINED' and column_info['udt_schema'] != '' and column_info['udt_name'] != '':
                column_data_type = f'''"{column_info['udt_schema']}"."{column_info['udt_name']}"'''
            # elif column_info['basic_data_type'] != '':
            #     column_data_type = column_info['basic_data_type'].upper()

            character_maximum_length = ''
            if 'character_maximum_length' in column_info and column_info['character_maximum_length'] != '':
                character_maximum_length = column_info['character_maximum_length']
            if column_info['basic_character_maximum_length'] != '':
                character_maximum_length = column_info['basic_character_maximum_length']

            domain_name = column_info['domain_name']

            column_comment = column_info['column_comment']
            nullable_string = ''
            if column_info['is_nullable'] == 'NO':
                nullable_string = 'NOT NULL'

            numeric_precision = column_info.get('numeric_precision')
            numeric_scale = column_info.get('numeric_scale')
            basic_numeric_precision = column_info.get('basic_numeric_precision')
            basic_numeric_scale = column_info.get('basic_numeric_scale')
            altered_data_type = ''

            if is_identity == 'YES' and column_data_type not in ('BIGINT', 'INTEGER', 'SMALLINT'):
                altered_data_type = 'BIGINT'
                migrator_tables.insert_target_column_alteration({
                    'target_schema': settings['target_schema'],
                    'target_table': settings['target_table'],
                    'target_column': column_info['column_name'],
                    'reason': 'IDENTITY',
                    'original_data_type': column_data_type,
                    'altered_data_type': altered_data_type,
                })
                create_column_sql = f""""{column_name}" {altered_data_type}"""
                self.config_parser.print_log_message('DEBUG', f"Column {column_name} is identity, altered data type to {altered_data_type}")
            elif column_data_type in ('NUMBER', 'NUMERIC') and (numeric_precision is None or numeric_precision == 10) and numeric_scale == 0:
                altered_data_type = 'INTEGER'
                migrator_tables.insert_target_column_alteration({
                    'target_schema': settings['target_schema'],
                    'target_table': settings['target_table'],
                    'target_column': column_info['column_name'],
                    'reason': 'NUMBER without precision, scale ' + str(numeric_scale),
                    'original_data_type': column_data_type,
                    'altered_data_type': altered_data_type,
                })
                create_column_sql = f""""{column_name}" {altered_data_type}"""
                self.config_parser.print_log_message('DEBUG', f"Column {column_name} is NUMBER without precision, scale {numeric_scale}, altered data type to {altered_data_type}")
            elif column_data_type in ('NUMBER', 'NUMERIC') and numeric_precision is None and numeric_scale == 10:
                altered_data_type = 'DOUBLE PRECISION'
                migrator_tables.insert_target_column_alteration({
                    'target_schema': settings['target_schema'],
                    'target_table': settings['target_table'],
                    'target_column': column_info['column_name'],
                    'reason': 'NUMBER without precision, scale ' + str(numeric_scale),
                    'original_data_type': column_data_type,
                    'altered_data_type': altered_data_type,
                })
                create_column_sql = f""""{column_name}" {altered_data_type}"""
                self.config_parser.print_log_message('DEBUG', f"Column {column_name} is NUMBER without precision, scale {numeric_scale}, altered data type to {altered_data_type}")
            elif column_data_type in ('NUMBER', 'NUMERIC') and numeric_precision == 1 and numeric_scale == 0:
                altered_data_type = 'BOOLEAN'
                migrator_tables.insert_target_column_alteration({
                    'target_schema': settings['target_schema'],
                    'target_table': settings['target_table'],
                    'target_column': column_info['column_name'],
                    'reason': 'NUMBER with precision 1, scale 0',
                    'original_data_type': column_data_type,
                    'altered_data_type': altered_data_type,
                })
                create_column_sql = f""""{column_name}" {altered_data_type}"""
                self.config_parser.print_log_message('DEBUG', f"Column {column_name} is NUMBER with precision 1, scale 0, altered data type to {altered_data_type}")
            elif column_data_type in ('NUMBER', 'NUMERIC') and numeric_precision == 19 and numeric_scale == 0:
                altered_data_type = 'BIGINT'
                migrator_tables.insert_target_column_alteration({
                    'target_schema': settings['target_schema'],
                    'target_table': settings['target_table'],
                    'target_column': column_info['column_name'],
                    'reason': 'NUMBER with precision 19, scale 0',
                    'original_data_type': column_data_type,
                    'altered_data_type': altered_data_type,
                })
                create_column_sql = f""""{column_name}" {altered_data_type}"""
                self.config_parser.print_log_message('DEBUG', f"Column {column_name} is NUMBER with precision 19, scale 0, altered data type to {altered_data_type}")
            else:
                if (character_maximum_length != '' and 'CHAR' in column_data_type):
                    create_column_sql = f""""{column_name}" {column_data_type}({character_maximum_length})"""
                elif self.is_numeric_type(column_data_type) and column_data_type in ('DECIMAL', 'NUMERIC'):
                    if numeric_precision not in (None, '') and numeric_scale not in (None, ''):
                        create_column_sql = f""""{column_name}" {column_data_type}({numeric_precision},{numeric_scale})"""
                    elif numeric_precision not in (None, ''):
                        create_column_sql = f""""{column_name}" {column_data_type}({numeric_precision})"""
                    elif basic_numeric_precision not in (None, '') and basic_numeric_scale not in (None, ''):
                        create_column_sql = f""""{column_name}" {column_data_type}({basic_numeric_precision},{basic_numeric_scale})"""
                    elif basic_numeric_precision not in (None, ''):
                        create_column_sql = f""""{column_name}" {column_data_type}({basic_numeric_precision})"""
                    else:
                        create_column_sql = f""""{column_name}" {column_data_type}"""
                else:
                    create_column_sql = f""""{column_name}" {column_data_type}"""

            if altered_data_type != '':
                column_data_type = altered_data_type

            if nullable_string != '':
                create_column_sql += f""" {nullable_string}"""

            if is_identity == 'YES':
                create_column_sql += " GENERATED BY DEFAULT AS IDENTITY"

            if column_info['is_generated_virtual'] == 'YES' or column_info['is_generated_stored'] == 'YES':
                generated_column_expression = column_info['stripped_generation_expression']
                # Quote column names in generated_column_expression if they match any column name in converted
                for other_col_info in converted.values():
                    other_col_name = other_col_info['column_name']
                    # Use word boundary for precise match, preserve case
                    pattern = r'\b{}\b'.format(re.escape(other_col_name))
                    # Only replace if not already quoted
                    generated_column_expression = re.sub(
                        pattern,
                        lambda m: f'"{m.group(0)}"' if not m.group(0).startswith('"') and not m.group(0).endswith('"') else m.group(0),
                        generated_column_expression
                    )
                if self.is_string_type(column_data_type):
                    generated_column_expression = generated_column_expression.replace("+", "||")
                create_column_sql += f" GENERATED ALWAYS AS {generated_column_expression} STORED"

            column_default = ''
            if column_info['column_default_name'] != '' and column_info['column_default_value'] == '' and column_info['replaced_column_default_value'] == '':
                default_value_info = migrator_tables.get_default_value_details(default_value_name=column_info['column_default_name'])
                if default_value_info:
                    column_default = default_value_info['extracted_default_value']

            elif column_info['column_default_value'] or column_info['replaced_column_default_value']:
                column_default = column_info['column_default_value']
                if column_info['replaced_column_default_value']:
                    column_default = column_info['replaced_column_default_value'].strip()

            if column_default != '':
                if (('CHAR' in column_data_type or column_data_type in ('TEXT'))
                    and ('||' in column_default or '(' in column_default or ')' in column_default)):
                    # default value is here NOT quoted
                    create_column_sql += f""" DEFAULT {column_default}""".replace("''", "'")
                elif 'CHAR' in column_data_type or column_data_type in ('TEXT'):
                    # here we must quote the default value
                    create_column_sql += f""" DEFAULT '{column_default}'""".replace("''", "'")
                elif column_data_type in ('BOOLEAN', 'BIT'):
                    if column_default.lower() in ('0', '(0)', 'false'):
                        create_column_sql += """ DEFAULT FALSE"""
                    elif column_default.lower() in ('1', '(1)', 'true'):
                        create_column_sql += """ DEFAULT TRUE"""
                    else:
                        create_column_sql += f""" DEFAULT {column_default}::BOOLEAN"""
                elif column_data_type in ('BYTEA'):
                    create_column_sql += f""" DEFAULT '{column_default}'::BYTEA"""
                else:
                    create_column_sql += f" DEFAULT {column_default}::{column_data_type}"

            if domain_name:
                domain_details = migrator_tables.get_domain_details(domain_name=domain_name)
                if domain_details:
                    domain_row_id = domain_details['id']
                    domain_name = domain_details['source_domain_name']
                    migrated_as = domain_details['migrated_as']
                    source_domain_check_sql = domain_details['source_domain_check_sql']
                    if source_domain_check_sql:
                        # Replace exact word VALUE with the column name, case-sensitive, word boundary
                        pattern = r'\bVALUE\b'
                        source_domain_check_sql = re.sub(pattern, f'"{column_info["column_name"]}"', source_domain_check_sql)
                    if migrated_as == 'CHECK CONSTRAINT':
                        constraint_name = f"{domain_name}_tab_{target_table_name}"
                        create_constraint_sql = f"""ALTER TABLE "{target_schema}"."{target_table_name}" ADD CONSTRAINT "{constraint_name}" CHECK({source_domain_check_sql})"""
                        migrator_tables.insert_constraint({
                            'source_schema': source_schema,
                            'source_table': source_table,
                            'source_table_id': source_table_id,
                            'target_schema': target_schema,
                            'target_table': target_table_name,
                            'constraint_name': constraint_name,
                            'constraint_type': 'CHECK (from domain)',
                            'constraint_sql': create_constraint_sql,
                            'constraint_comment': ('added from domains ' + column_comment).strip(),
                        })

            self.config_parser.print_log_message('DEBUG3', f"Creating DDL for table {target_schema}.{target_table_name}, create_column_sql: {create_column_sql}")
            create_table_sql_parts.append(create_column_sql)

        create_table_sql = ", ".join(create_table_sql_parts)
        create_table_sql = f"""CREATE TABLE "{target_schema}"."{target_table_name}" ({create_table_sql})"""
        return create_table_sql

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

                table_indexes[order_num] = {
                    'index_name': index_name,
                    'index_type': index_type,
                    'index_owner': source_table_schema,
                    'index_columns': index_columns,
                    'index_sql': index_sql,
                    'index_comment': row[3]
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

        # source_schema = settings['source_schema']
        # source_table = settings['source_table']
        # source_table_id = settings['source_table_id']
        # index_owner = settings['index_owner']
        index_name = self.config_parser.convert_names_case(settings['index_name'])
        index_type = settings['index_type']
        target_schema = self.config_parser.convert_names_case(settings['target_schema'])
        target_table = self.config_parser.convert_names_case(settings['target_table'])
        index_columns = self.config_parser.convert_names_case(settings['index_columns'])
        # index_comment = settings['index_comment']

        # index_columns = ', '.join(f'"{col}"' for col in index_columns)
        # index_columns_count = row[2]
        create_index_query = ''
        if index_type == 'PRIMARY KEY':
            create_index_query = f"""ALTER TABLE "{target_schema}"."{target_table}" ADD CONSTRAINT "{index_name}_tab_{target_table}" PRIMARY KEY ({index_columns});"""
        else:
            create_index_query = f"""CREATE {'UNIQUE' if index_type == 'UNIQUE' else ''} INDEX "{index_name}_tab_{target_table}" ON "{target_schema}"."{target_table}" ({index_columns});"""

        return create_index_query

        # index_columns_count = 0
        # index_columns_data_types = []
        # for column_name in index_columns.split(','):
        #     column_name = column_name.strip().strip('"')
        #     for col_order_num, column_info in target_columns.items():
        #         if column_name == column_info['column_name']:
        #             index_columns_count += 1
        #             column_data_type = column_info['data_type']
        #             self.config_parser.print_log_message('DEBUG', f"Table: {target_schema}.{target_table_name}, index: {index_name}, column: {column_name} has data type {column_data_type}")
        #             index_columns_data_types.append(column_data_type)
        #             index_columns_data_types_str = ', '.join(index_columns_data_types)

        # columns = []
        # for col in index_columns.split(","):
        #     col = col.strip().replace(" ASC", "").replace(" DESC", "")
        #     if col not in columns:
        #         columns.append('"'+col+'"')
        # index_columns = ','.join(columns)

    def fetch_constraints(self, settings):
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

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
                constraint_sql = row[3]
                constraint_comment = row[4]

                if constraint_type in ('PRIMARY KEY', 'p', 'P'):
                    continue # Primary key is handled in fetch_indexes

                constraints[order_num] = {
                    'constraint_name': constraint_name,
                    'constraint_type': constraint_type,
                    'constraint_sql': constraint_sql,
                    'constraint_comment': constraint_comment
                }
                order_num += 1

            cursor.close()
            self.disconnect()
            return constraints
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_constraint_sql(self, settings):
        create_constraint_query = ''
        source_db_type = settings['source_db_type']
        target_schema = self.config_parser.convert_names_case(settings['target_schema'])
        target_table_name = self.config_parser.convert_names_case(settings['target_table'])
        target_columns = settings['target_columns']
        constraint_name = self.config_parser.convert_names_case(settings['constraint_name'])
        constraint_type = settings['constraint_type']
        constraint_owner = self.config_parser.convert_names_case(settings['constraint_owner'])
        constraint_columns = self.config_parser.convert_names_case(settings['constraint_columns'])
        referenced_table_schema = self.config_parser.convert_names_case(settings['referenced_table_schema'])
        referenced_table_name = self.config_parser.convert_names_case(settings['referenced_table_name'])
        referenced_columns = self.config_parser.convert_names_case(settings['referenced_columns'])
        delete_rule = settings['delete_rule'] if 'delete_rule' in settings else 'NO ACTION'
        update_rule = settings['update_rule'] if 'update_rule' in settings else 'NO ACTION'
        constraint_comment = settings['constraint_comment']
        constraint_sql = self.config_parser.convert_names_case(settings['constraint_sql']) if 'constraint_sql' in settings else ''
        constraint_status = settings['constraint_status'] if 'constraint_status' in settings else 'ENABLED'

        if source_db_type != 'postgresql':
            if constraint_type == 'FOREIGN KEY':
                create_constraint_query = f"""ALTER TABLE "{target_schema}"."{target_table_name}" ADD CONSTRAINT "{constraint_name}_tab_{target_table_name}" FOREIGN KEY ({constraint_columns}) REFERENCES "{target_schema}"."{referenced_table_name}" ({referenced_columns})"""
                if delete_rule == 'CASCADE':
                    create_constraint_query += " ON DELETE CASCADE"
                if update_rule == 'CASCADE':
                    create_constraint_query += " ON UPDATE CASCADE"
                if constraint_comment:
                    create_constraint_query += f" COMMENT '{constraint_comment}'"
            elif constraint_type == 'CHECK':
                # Replace column names in constraint_sql with double-quoted names using precise match

                if constraint_sql and target_columns:
                    for col_info in target_columns.values():
                        col_name = col_info['column_name']
                        # Use word boundary for precise match, preserve case
                        pattern = r'\b{}\b'.format(re.escape(col_name))
                        constraint_sql = re.sub(pattern, f'"{col_name}"', constraint_sql)
                create_constraint_query = f"""ALTER TABLE "{target_schema}"."{target_table_name}" ADD CONSTRAINT "{constraint_name}_tab_{target_table_name}" CHECK ({constraint_sql})"""
        else:
            create_constraint_query = f"""ALTER TABLE "{target_schema}"."{target_table_name}" ADD CONSTRAINT "{constraint_name}" {constraint_sql}"""
        return create_constraint_query

    def fetch_triggers(self, table_id: int, table_schema: str, table_name: str):
        pass

    def execute_query(self, query: str, params=None):
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)

    def copy_from_file(self, sql: str, file_path: str):
        with open(file_path, 'r') as file:
            with self.connection.cursor() as cursor:
                try:
                    cursor.copy_expert(sql, file)
                    for notice in cursor.connection.notices:
                        self.config_parser.print_log_message('INFO', notice)
                    cursor.connection.commit()
                except Exception as e:
                    for notice in cursor.connection.notices:
                        self.config_parser.print_log_message('INFO', notice)
                    self.config_parser.print_log_message('ERROR', f"Error executing copy_expert: {e}")
                    raise

    def execute_sql_script(self, script_path: str):
        with open(script_path, 'r') as file:
            script = file.read()

        with self.connection.cursor() as cursor:
            try:
                cursor.execute(script)
                for notice in cursor.connection.notices:
                    self.config_parser.print_log_message('INFO', notice)
            except Exception as e:
                for notice in cursor.connection.notices:
                    self.config_parser.print_log_message('INFO', notice)
                self.config_parser.print_log_message('ERROR', f"Error executing script: {e}")
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
        target_table_rows = 0
        total_inserted_rows = 0
        migration_stats = {}
        batch_number = 0
        shortest_batch_seconds = 0
        longest_batch_seconds = 0
        average_batch_seconds = 0
        chunk_start_row_number = 0
        chunk_end_row_number = 0
        processing_start_time = time.time()
        order_by_clause = ''
        try:
            worker_id = settings['worker_id']
            source_schema = settings['source_schema']
            source_table = settings['source_table']
            source_table_id = settings['source_table_id']
            source_columns = settings['source_columns']
            target_schema = self.config_parser.convert_names_case(settings['target_schema'])
            target_table = self.config_parser.convert_names_case(settings['target_table'])
            target_columns = settings['target_columns']
            # primary_key_columns = settings['primary_key_columns']
            batch_size = settings['batch_size']
            migrator_tables = settings['migrator_tables']
            migration_limitation = settings['migration_limitation']
            chunk_size = settings['chunk_size']
            chunk_number = settings['chunk_number']
            resume_after_crash = settings['resume_after_crash']
            drop_unfinished_tables = settings['drop_unfinished_tables']

            source_table_rows = self.get_rows_count(source_schema, source_table, migration_limitation)
            target_table_rows = migrate_target_connection.get_rows_count(target_schema, target_table)

            total_chunks = self.config_parser.get_total_chunks(source_table_rows, chunk_size)
            if chunk_size == -1:
                chunk_size = source_table_rows + 1

            migration_stats = {
                'rows_migrated': target_table_rows,
                'chunk_number': chunk_number,
                'total_chunks': total_chunks,
                'source_table_rows': source_table_rows,
                'target_table_rows': target_table_rows,
                'finished': True if source_table_rows == 0 else False,
            }
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
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Table {source_table} is empty - skipping data migration.")
                migrator_tables.update_data_migration_status({
                        'row_id': protocol_id,
                        'success': True,
                        'message': 'Skipped',
                        'target_table_rows': 0,
                        'batch_count': 0,
                        'shortest_batch_seconds': 0,
                        'longest_batch_seconds': 0,
                        'average_batch_seconds': 0,
                    })

                return migration_stats

            else:

                if source_table_rows > target_table_rows:

                    part_name = 'migrate_table in batches using cursor'
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Source table {source_table}: {source_table_rows} rows / Target table {target_table}: {target_table_rows} rows - starting data migration.")

                    select_columns_list = []
                    orderby_columns_list = []
                    insert_columns_list = []
                    for order_num, col in source_columns.items():
                        self.config_parser.print_log_message('DEBUG2',
                                                            f"Worker {worker_id}: Table {source_schema}.{source_table}: Processing column {col['column_name']} ({order_num}) with data type {col['data_type']}")

                        if col['data_type'].lower() == 'datetime':
                            select_columns_list.append(f"TO_CHAR({col['column_name']}, '%Y-%m-%d %H:%M:%S') as {col['column_name']}")
                        #     select_columns_list.append(f"ST_asText(`{col['column_name']}`) as `{col['column_name']}`")
                        # elif col['data_type'].lower() == 'set':
                        #     select_columns_list.append(f"cast(`{col['column_name']}` as char(4000)) as `{col['column_name']}`")
                        else:
                            select_columns_list.append(f'''"{col['column_name']}"''')

                        insert_columns_list.append(f'''"{self.config_parser.convert_names_case(col['column_name'])}"''')
                        orderby_columns_list.append(f'''"{col['column_name']}"''')

                    select_columns = ', '.join(select_columns_list)
                    orderby_columns = ', '.join(orderby_columns_list)
                    insert_columns = ', '.join(insert_columns_list)

                    if resume_after_crash and not drop_unfinished_tables:
                        chunk_number = self.config_parser.get_total_chunks(target_table_rows, chunk_size)
                        self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Resuming migration for table {source_schema}.{source_table} from chunk {chunk_number} with data chunk size {chunk_size}.")
                        chunk_offset = target_table_rows
                    else:
                        chunk_offset = (chunk_number - 1) * chunk_size

                    chunk_start_row_number = chunk_offset + 1
                    chunk_end_row_number = chunk_offset + chunk_size

                    self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Migrating table {source_schema}.{source_table}: chunk {chunk_number}, data chunk size {chunk_size}, batch size {batch_size}, chunk offset {chunk_offset}, chunk end row number {chunk_end_row_number}, source table rows {source_table_rows}")
                    order_by_clause = ''

                    query = f'''SELECT {select_columns} FROM "{source_schema}"."{source_table}" '''
                    if migration_limitation:
                        query += f" WHERE {migration_limitation}"
                    primary_key_columns = migrator_tables.select_primary_key(source_schema, source_table)
                    self.config_parser.print_log_message('DEBUG2', f"Worker {worker_id}: Primary key columns for {source_schema}.{source_table}: {primary_key_columns}")
                    if primary_key_columns:
                        orderby_columns = primary_key_columns
                    order_by_clause = f""" ORDER BY {orderby_columns}"""
                    query += order_by_clause + f" LIMIT {chunk_size} OFFSET {chunk_offset}"

                    self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Fetching data with cursor using query: {query}")

                    part_name = 'execute query'
                    cursor = self.connection.cursor()
                    cursor.arraysize = batch_size

                    batch_start_time = time.time()
                    reading_start_time = batch_start_time
                    processing_start_time = batch_start_time
                    batch_end_time = None
                    batch_number = 0
                    batch_durations = []

                    cursor.execute(query)
                    total_inserted_rows = 0
                    while True:
                        records = cursor.fetchmany(batch_size)
                        if not records:
                            break
                        batch_number += 1
                        reading_end_time = time.time()
                        reading_duration = reading_end_time - reading_start_time
                        self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Fetched {len(records)} rows (batch {batch_number}) from source table {source_table}.")

                        transforming_start_time = time.time()
                        records = [
                            {column['column_name']: value for column, value in zip(source_columns.values(), record)}
                            for record in records
                        ]
                        for record in records:
                            for order_num, column in source_columns.items():
                                column_name = column['column_name']
                                column_type = column['data_type']
                                if column_type in ['bytea']:
                                    record[column_name] = record[column_name].tobytes()

                        # Insert batch into target table
                        self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Starting insert of {len(records)} rows from source table {source_table}")
                        transforming_end_time = time.time()
                        transforming_duration = transforming_end_time - transforming_start_time
                        inserting_start_time = time.time()
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
                        inserting_end_time = time.time()
                        inserting_duration = inserting_end_time - inserting_start_time

                        batch_end_time = time.time()
                        batch_duration = batch_end_time - batch_start_time
                        batch_durations.append(batch_duration)
                        percent_done = round(total_inserted_rows / source_table_rows * 100, 2)

                        batch_start_dt = datetime.datetime.fromtimestamp(batch_start_time)
                        batch_end_dt = datetime.datetime.fromtimestamp(batch_end_time)
                        batch_start_str = batch_start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')
                        batch_end_str = batch_end_dt.strftime('%Y-%m-%d %H:%M:%S.%f')
                        migrator_tables.insert_batches_stats({
                            'source_schema': source_schema,
                            'source_table': source_table,
                            'source_table_id': source_table_id,
                            'chunk_number': chunk_number,
                            'batch_number': batch_number,
                            'batch_start': batch_start_str,
                            'batch_end': batch_end_str,
                            'batch_rows': inserted_rows,
                            'batch_seconds': batch_duration,
                            'worker_id': worker_id,
                            'reading_seconds': reading_duration,
                            'transforming_seconds': transforming_duration,
                            'writing_seconds': inserting_duration,
                        })

                        msg = (
                            f"Worker {worker_id}: Inserted {inserted_rows} "
                            f"(total: {total_inserted_rows} from: {source_table_rows} "
                            f"({percent_done}%)) rows into target table '{target_table}': "
                            f"Batch {batch_number} duration: {batch_duration:.2f} seconds "
                            f"(r: {reading_duration:.2f}, t: {transforming_duration:.2f}, w: {inserting_duration:.2f})"
                        )
                        self.config_parser.print_log_message('INFO', msg)

                        batch_start_time = time.time()
                        reading_start_time = batch_start_time

                    target_table_rows = migrate_target_connection.get_rows_count(target_schema, target_table)
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Target table {target_schema}.{target_table} has {target_table_rows} rows")

                    shortest_batch_seconds = min(batch_durations) if batch_durations else 0
                    longest_batch_seconds = max(batch_durations) if batch_durations else 0
                    average_batch_seconds = sum(batch_durations) / len(batch_durations) if batch_durations else 0
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Migrated {total_inserted_rows} rows from {source_table} to {target_schema}.{target_table} in {batch_number} batches: "
                                                            f"Shortest batch: {shortest_batch_seconds:.2f} seconds, "
                                                            f"Longest batch: {longest_batch_seconds:.2f} seconds, "
                                                            f"Average batch: {average_batch_seconds:.2f} seconds")

                    cursor.close()

                elif source_table_rows <= target_table_rows:
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Source table {source_table} has {source_table_rows} rows, which is less than or equal to target table {target_table} with {target_table_rows} rows. No data migration needed.")

                migration_stats = {
                    'rows_migrated': total_inserted_rows,
                    'chunk_number': chunk_number,
                    'total_chunks': total_chunks,
                    'source_table_rows': source_table_rows,
                    'target_table_rows': target_table_rows,
                    'finished': False,
                }

                self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Migration stats: {migration_stats}")
                if source_table_rows <= target_table_rows or chunk_number >= total_chunks:
                    self.config_parser.print_log_message('DEBUG3', f"Worker {worker_id}: Setting migration status to finished for table {source_table} (chunk {chunk_number}/{total_chunks})")
                    migration_stats['finished'] = True
                    migrator_tables.update_data_migration_status({
                        'row_id': protocol_id,
                        'success': True,
                        'message': 'OK',
                        'target_table_rows': target_table_rows,
                        'batch_count': batch_number,
                        'shortest_batch_seconds': shortest_batch_seconds,
                        'longest_batch_seconds': longest_batch_seconds,
                        'average_batch_seconds': average_batch_seconds,
                    })

                migrator_tables.insert_data_chunk({
                    'worker_id': worker_id,
                    'source_table_id': source_table_id,
                    'source_schema': source_schema,
                    'source_table': source_table,
                    'target_schema': target_schema,
                    'target_table': target_table,
                    'source_table_rows': source_table_rows,
                    'target_table_rows': target_table_rows,
                    'chunk_number': chunk_number,
                    'chunk_size': chunk_size,
                    'migration_limitation': migration_limitation,
                    'chunk_start': chunk_start_row_number,
                    'chunk_end': chunk_end_row_number,
                    'inserted_rows': total_inserted_rows,
                    'batch_size': batch_size,
                    'total_batches': batch_number,
                    'task_started': datetime.datetime.fromtimestamp(processing_start_time).strftime('%Y-%m-%d %H:%M:%S.%f'),
                    'task_completed': datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f'),
                    'order_by_clause': order_by_clause,
                })

                return migration_stats

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Error during {part_name} -> {e}")
            self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Full stack trace: {traceback.format_exc()}")
            raise e

    def insert_batch(self, settings):
        target_schema = settings['target_schema']
        target_table = settings['target_table']
        columns = settings['target_columns']
        data = settings['data']
        worker_id = settings['worker_id']
        insert_columns = settings.get('insert_columns', None)

        if not insert_columns:
            insert_columns = [f'"{columns[col]["column_name"]}"' for col in sorted(columns.keys())]

        if isinstance(insert_columns, list):
            insert_columns = ', '.join(insert_columns)

        inserted_rows = 0
        self.config_parser.print_log_message('DEBUG2', f"Worker {worker_id}: insert_batch into {target_schema}.{target_table} with {len(data)} rows, columns: {insert_columns}, data type: {type(data)}")
        try:
            # Ensure data is a list of tuples
            self.config_parser.print_log_message('DEBUG2', f"Worker {worker_id}: Started insert batch into {target_schema}.{target_table} with {len(data)} rows")
            if isinstance(data, list) and all(isinstance(item, dict) for item in data):
                formatted_data = []
                for item in data:
                    row = []
                    for col in columns.keys():
                        column_name = columns[col]['column_name']
                        # column_type = columns[col]['data_type'].lower()
                        # if column_type in ['bytea', 'blob']:
                        #     if item.get(column_name) is not None:
                        #         row.append(psycopg2.Binary(item.get(column_name)))
                        #     else:
                        #         row.append(None)
                        # else:
                        row.append(item.get(column_name))
                    formatted_data.append(tuple(row))
                data = formatted_data
            else:
                self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Data for insert_batch must be a list of dictionaries, got {type(data)}")
                return 0

            self.config_parser.print_log_message('DEBUG2', f"Worker {worker_id}: insert_batch [2] into {target_schema}.{target_table} with {len(data)} rows, columns: |{insert_columns}| data type: {type(data)}")

            with self.connection.cursor() as cursor:
                insert_query = sql.SQL(f"""INSERT INTO "{target_schema}"."{target_table}" ({insert_columns}) VALUES ({', '.join(['%s' for _ in columns.keys()])})""")
                self.config_parser.print_log_message('DEBUG3', f"Worker {worker_id}: Insert query: {insert_query}")
                self.connection.autocommit = False
                try:
                    if self.session_settings:
                        cursor.execute(self.session_settings)
                    self.config_parser.print_log_message('DEBUG3', f"Worker {worker_id}: Starting psycopg2.extras.execute_batch into {target_table} with {len(data)} rows")

                    psycopg2.extras.execute_batch(cursor, insert_query, data)
                    inserted_rows = len(data)
                except Exception as e:
                    self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Error inserting batch data into {target_table}: {e}")
                    self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Trying to insert row by row.")
                    self.connection.rollback()
                    for row in data:
                        try:
                            cursor.execute(insert_query, row)
                            inserted_rows += 1
                            self.connection.commit()
                        except Exception as e:
                            self.connection.rollback()
                            self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Error inserting row into {target_table}: {row}")
                            self.config_parser.print_log_message('ERROR', e)

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Error before inserting batch data: {e}")
            raise
        finally:
            self.connection.commit()
            self.connection.autocommit = True
            return inserted_rows

    def fetch_funcproc_names(self, schema: str):
        pass

    def fetch_funcproc_code(self, funcproc_id: int):
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

    def handle_error(self, e, description=None):
        self.config_parser.print_log_message('ERROR', f"An error in {self.__class__.__name__} ({description}): {e}")
        self.config_parser.print_log_message('ERROR', traceback.format_exc())
        if self.on_error_action == 'stop':
            self.config_parser.print_log_message('ERROR', "Stopping due to error.")
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
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing sequence query: {query}")
            self.config_parser.print_log_message('ERROR', e)

    def get_sequence_details(self, sequence_owner, sequence_name):
        # Placeholder for fetching sequence details
        return {}

    def get_sequence_current_value(self, sequence_id: int):
        try:
            query = f"""select '"'||relnamespace::regnamespace::text||'"."'||relname||'"' as seqname from pg_class where oid = {sequence_id}"""
            cursor = self.connection.cursor()
            cursor.execute(query)
            sequence_data = cursor.fetchone()
            sequence_name = f"{sequence_data[0]}"

            query = f"""SELECT last_value FROM {sequence_name}"""
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            cur_value = cursor.fetchone()[0]
            cursor.close()
            self.disconnect()
            return cur_value
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_rows_count(self, table_schema: str, table_name: str, migration_limitation: str = None):
        query = f"""SELECT count(*) FROM "{table_schema}"."{table_name}" """
        if migration_limitation:
            query += f" WHERE {migration_limitation}"
        self.config_parser.print_log_message('DEBUG3', f"postgresql: get_rows_count query: {query}")
        cursor = self.connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_table_size(self, table_schema: str, table_name: str):
        query = f"""SELECT pg_total_relation_size('{table_schema}.{table_name}')"""
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
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
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
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def convert_view_code(self, settings: dict):
        view_code = settings['view_code']
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
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def prepare_session_settings(self):
        """
        Prepare session settings for the database connection.
        """
        filtered_settings = ""
        try:
            settings = self.config_parser.get_target_db_session_settings()
            if not settings:
                self.config_parser.print_log_message('INFO', "No session settings found in config file.")
                return filtered_settings
            # self.config_parser.print_log_message('INFO', f"Preparing session settings: {settings} / {settings.keys()} / {tuple(settings.keys())}")
            self.connect()
            cursor = self.connection.cursor()
            lower_keys = tuple(k.lower() for k in settings.keys())
            cursor.execute("SELECT name FROM (SELECT name FROM pg_settings UNION ALL SELECT name FROM (VALUES('role')) as t(name) ) a WHERE lower(a.name) IN %s", (lower_keys,))
            matching_settings = cursor.fetchall()
            cursor.close()
            self.disconnect()
            if not matching_settings:
                self.config_parser.print_log_message('INFO', "No settings found to prepare.")
                return filtered_settings

            for setting in matching_settings:
                setting_name = setting[0]
                if setting_name in ['search_path']:
                    filtered_settings += f"SET {setting_name} = {settings[setting_name]};"
                else:
                    filtered_settings += f"SET {setting_name} = '{settings[setting_name]}';"
            self.config_parser.print_log_message('INFO', f"Session settings: {filtered_settings}")
            return filtered_settings
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error preparing session settings: {e}")
            raise

    def fetch_domains(self, schema: str):
        # Placeholder for fetching domains
        return {}

    def get_create_domain_sql(self, settings):
        create_domain_sql = ""
        domain_name = settings['domain_name']
        target_schema = settings['target_schema']
        domain_check_sql = settings['source_domain_check_sql']
        domain_data_type = settings['domain_data_type']
        domain_comment = settings['domain_comment']
        migrated_as = settings['migrated_as'] if 'migrated_as' in settings else 'CHECK CONSTRAINT'

        if migrated_as == 'CHECK CONSTRAINT':
            create_domain_sql = f"""CHECK({domain_check_sql})"""
        else:
            create_domain_sql = f"""CREATE DOMAIN "{target_schema}"."{domain_name}" AS {domain_data_type} CHECK({domain_check_sql})"""

        # if domain_comment:
        #     create_domain_sql += f" COMMENT '{domain_comment}'"
        return create_domain_sql

    def fetch_default_values(self, settings) -> dict:
        # Placeholder for fetching default values
        return {}

    def get_table_description(self, settings) -> dict:
        # Placeholder for fetching table description
        return { 'table_description': '' }

    def testing_select(self):
        return "SELECT 1"

    def get_database_version(self):
        query = "SELECT version()"
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        version = cursor.fetchone()[0]
        cursor.close()
        self.disconnect()
        return version

    def get_database_size(self):
        query = "SELECT pg_database_size(current_database())"
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        size = cursor.fetchone()[0]
        cursor.close()
        self.disconnect()
        return size

    def get_top_n_tables(self, settings):
        top_tables = {}
        top_tables['by_rows'] = {}
        top_tables['by_size'] = {}
        top_tables['by_columns'] = {}
        top_tables['by_indexes'] = {}
        top_tables['by_constraints'] = {}
        # return top_tables

        source_schema = settings.get('source_schema', 'public')
        try:
            order_num = 1
            top_n = self.config_parser.get_top_n_tables_by_rows()
            if top_n > 0:
                query = f"""
                    SELECT
                    n.nspname AS owner,
                    c.relname AS table_name,
                    c.reltuples::bigint AS row_count,
                    pg_total_relation_size(c.oid) AS row_size
                    FROM
                    pg_class c
                    JOIN
                    pg_namespace n ON n.oid = c.relnamespace
                    WHERE
                    c.relkind = 'r' AND n.nspname = '{source_schema}'
                    ORDER BY
                    c.reltuples DESC
                    LIMIT {top_n};
                """
                self.connect()
                cursor = self.connection.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                cursor.close()
                self.disconnect()

                order_num = 1
                for row in results:
                    top_tables['by_rows'][order_num] = {
                    'owner': row[0].strip(),
                    'table_name': row[1].strip(),
                    'row_count': row[2],
                    'table_size': row[3],
                    }
                    order_num += 1

                self.config_parser.print_log_message('DEBUG2', f"Top {top_n} tables by rows: {top_tables['by_rows']}")
            else:
                self.config_parser.print_log_message('DEBUG', "Top N tables by rows is not configured or set to 0, skipping this part.")

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching top tables by rows: {e}")

        return top_tables

    def get_top_fk_dependencies(self, settings):
        top_fk_dependencies = {}
        return top_fk_dependencies

    def target_table_exists(self, target_schema, target_table):
        query = f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = '{target_schema}'
                AND table_name = '{target_table}'
            )
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists

    def fetch_all_rows(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

if __name__ == "__main__":
    print("This script is not meant to be run directly")
