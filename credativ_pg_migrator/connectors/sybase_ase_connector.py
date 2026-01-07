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
import sys
from tabulate import tabulate
import sqlglot
from sqlglot import exp
import time
import datetime

class SybaseASEConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        if source_or_target != 'source':
            raise ValueError(f"Sybase ASE is only supported as a source database")

        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.on_error_action = self.config_parser.get_on_error_action()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger
        self._udt_cache = None

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
            return {
                'getdate()': 'current_timestamp',
                'getutcdate()': "timezone('UTC', now())",
                'datetime': 'current_timestamp',
                'year(': 'extract(year from ',
                'month(': 'extract(month from ',
                'day(': 'extract(day from ',

                'db_name()': 'current_database()',
                'dbo.suser_name()': 'current_user',
                'dbo.user_sname()': 'current_user',
                'suser_name()': 'current_user',
                'user_name()': 'current_user',
                'len(': 'length(',
                'isnull(': 'coalesce(',

                'str_replace(': 'replace(',
                'convert(': 'cast(',
                'stuff(': 'overlay(',
                'replicate(': 'repeat(',
                'charindex(': 'position(',
                '@@nestlevel': '-1', ## no equivalent in PostgreSQL
            }
        else:
            self.config_parser.print_log_message('ERROR', f"Unsupported target database type: {target_db_type}")

    def fetch_table_names(self, table_schema: str):
        # 2048 = proxy table referencing remote table
        query = f"""
            SELECT
            o.id as table_id,
            o.name as table_name
            FROM sysobjects o
            WHERE user_name(o.uid) = '{table_schema}'
            AND o.type = 'U'
            AND (o.sysstat & 2048 <> 2048)
            ORDER BY o.name
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
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_table_columns(self, settings) -> dict:
        table_schema = settings['table_schema']
        table_name = settings['table_name']
        result = {}
        try:
            self.connect()
            cursor = self.connection.cursor()
            self.config_parser.print_log_message('DEBUG', f"Sybase ASE: Reading columns for {table_schema}.{table_name}")
            cursor.execute("SELECT @@unicharsize, @@ncharsize")
            unichar_size, nchar_size = cursor.fetchone()
            self.config_parser.print_log_message('DEBUG', f"Sybase ASE: unichar size: {unichar_size}, nchar size: {nchar_size}")
            query = f"""
                SELECT
                    c.colid as ordinal_position,
                    c.name as column_name,
                    t.name as data_type,
                    '' as data_type_length,
                    c.length,
                    CASE
                        WHEN c.status&8=8 and t.name <> 'bit' THEN 1
                    ELSE 0 END AS column_nullable,
                    CASE
                        WHEN c.status&128=128 and t.name <> 'bit' THEN 1
                    ELSE 0 END AS identity_column,
                    '' as full_data_type_length,
                    object_name(c.domain) as column_domain,
                    object_name(c.cdefault) as column_default_name,
                    ltrim(rtrim(str_replace(co.text, char(10),''))) as column_default_value,
                    c.status,
                    t.variable as variable_length,
                    c.prec as data_type_precision,
                    c.scale as data_type_scale,
                    t.allownulls as type_nullable,
                    t.ident as type_has_identity_property,
                    object_name(c.domain) as domain_name,
                    case when c.status2 & 16 = 16 then 1 else 0 end is_generated_virtual,
                    case when c.status2 & 32 = 32 then 1 else 0 end is_genreated_stored,
                    com.text as computed_column_expression,
                    case when c.status3 & 1 = 1 then 1 else 0 end as is_hidden_column
                FROM syscolumns c
                JOIN sysobjects tab ON c.id = tab.id
                JOIN systypes t ON c.usertype = t.usertype
                LEFT JOIN syscomments co ON c.cdefault = co.id
                LEFT JOIN syscomments com ON c.computedcol = com.id
                WHERE user_name(tab.uid) = '{table_schema}'
                    AND tab.name = '{table_name}'
                    AND tab.type = 'U'
                ORDER BY c.colid
            """
            cursor.execute(query)
            for row in cursor.fetchall():
                self.config_parser.print_log_message('DEBUG', f"Processing column: {row}")
                ordinal_position = row[0]
                column_name = row[1].strip()
                data_type = row[2].strip()
                # data_type_length = row[3].strip()
                length = row[4]
                column_nullable = row[5]
                identity_column = row[6]
                # full_data_type_length = row[7].strip()
                column_domain = row[8]
                column_default_name = row[9]
                column_default_value = row[10].replace('DEFAULT ', '').strip().strip('"') if row[10] and row[10].replace('DEFAULT ', '').strip().startswith('"') and row[10].replace('DEFAULT ', '').strip().endswith('"') else (row[10].replace('DEFAULT ', '').strip() if row[10] else '')
                status = row[11]
                variable_length = row[12]
                data_type_precision = row[13]
                data_type_scale = row[14]
                type_nullable = row[15]
                type_has_identity_property = row[16]
                domain_name = row[17]
                is_generated_virtual = row[18]
                is_generated_stored = row[19]
                generation_expression = row[20]
                is_hidden_column = row[21]
                stripped_generation_expression = generation_expression.replace('AS ', '').replace('MATERIALIZED', '').strip() if generation_expression else ''

                if data_type.lower() in ('univarchar', 'unichar'):
                    data_type_length = str(int(length / unichar_size))
                    character_maximum_length = int(length / unichar_size)
                elif data_type.lower() in ('nvarchar', 'nchar'):
                    data_type_length = str(int(length / nchar_size))
                    character_maximum_length = int(length / nchar_size)
                elif data_type.lower() in ('numeric', 'double precision', 'decimal'):
                    data_type_length = f"{data_type_precision},{data_type_scale}"
                    character_maximum_length = None
                else:
                    data_type_length = length
                    character_maximum_length = length if self.is_string_type(data_type) else None

                full_data_type_length = f"{data_type}({data_type_length})" if data_type_length else data_type

                result[ordinal_position] = {
                    'column_name': column_name,
                    'data_type': data_type,
                    'column_type': full_data_type_length,
                    'character_maximum_length': character_maximum_length,
                    'numeric_precision': data_type_precision if self.is_numeric_type(data_type) else None,
                    'numeric_scale': data_type_scale if self.is_numeric_type(data_type) else None,
                    'is_nullable': 'NO' if column_nullable == 0 else 'YES',
                    'column_default_name': column_default_name,
                    'column_default_value': column_default_value,
                    'column_comment': '',
                    'is_identity': 'YES' if identity_column == 1 else 'NO',
                    'domain_name': domain_name,
                    'is_generated_virtual': 'YES' if is_generated_virtual == 1 else 'NO',
                    'is_generated_stored': 'YES' if is_generated_stored == 1 else 'NO',
                    'generation_expression': generation_expression,
                    'stripped_generation_expression': stripped_generation_expression,
                    'is_hidden_column': 'YES' if is_hidden_column == 1 else 'NO',
                }

                # Check for config substitutions first (Higher Priority)
                config_substitutions = self.config_parser.get_data_types_substitution()
                substitution_found = False

                # Iterate through substitutions to find a match for the current data_type
                # Substitution format: [schema, table, source_type, target_type, target_length]
                # We prioritize exact matches on type name.
                for sub in config_substitutions:
                    # sub[2] is source_type, sub[3] is target_type
                    if sub[2].lower() == data_type.lower():
                         # Found a substitution
                         target_type = sub[3]
                         # If target_length is provided, use it (e.g. VARCHAR(255))
                         # But typically target_type might be just 'TEXT' or 'BIGINT'
                         # We need to populate basic_ fields.

                         # Determine if target type is substituted
                         result[ordinal_position]['basic_data_type'] = target_type
                         # We might not know precision/scale easily from just the name unless we parse it or it's provided.
                         # For now, we assume the config substitution handles the mapping sufficiently for the migration mapping phase.
                         # The key result is basic_column_type.
                         result[ordinal_position]['basic_column_type'] = target_type
                         substitution_found = True
                         break

                if substitution_found:
                     continue

                query_custom_types = f"""
                    SELECT
                        bt.name AS source_data_type,
                        ut.ident as type_has_identity_property,
                        ut.allownulls as type_nullable,
                        ut.length as length,
                        ut.prec as data_type_precision,
                        ut.scale as data_type_scale
                    FROM systypes ut
                    JOIN (SELECT * FROM systypes t JOIN (SELECT type, min(usertype) as usertype FROM systypes GROUP BY type) bt0
                        ON t.type = bt0.type AND t.usertype = bt0.usertype) bt
                        ON ut.type = bt.type AND ut.hierarchy = bt.hierarchy
                    WHERE ut.name <> bt.name AND LOWER(ut.name) not in ('timestamp')
                    AND ut.name = '{data_type}'
                    ORDER BY ut.name
                """
                cursor.execute(query_custom_types)
                custom_type = cursor.fetchone()
                if custom_type:
                    source_data_type = custom_type[0]
                    type_has_identity_property = custom_type[1]
                    type_nullable = custom_type[2]
                    length = custom_type[3]
                    data_type_precision = custom_type[4]
                    data_type_scale = custom_type[5]

                    basic_character_maximum_length = None
                    if source_data_type in ('univarchar', 'unichar'):
                        source_length = str(int(length / unichar_size))
                        basic_character_maximum_length = int(length / unichar_size)
                    elif source_data_type in ('nvarchar', 'nchar'):
                        source_length = str(int(length / nchar_size))
                        basic_character_maximum_length = int(length / nchar_size)
                    elif source_data_type in ('numeric', 'double precision', 'decimal'):
                        source_length = f"{data_type_precision},{data_type_scale}"
                    else:
                        source_length = str(length)
                        basic_character_maximum_length = length

                    source_data_type_length = f"{source_data_type}({source_length})" if source_length else source_data_type

                    # Convert base type to PostgreSQL equivalent
                    # We need types mapping here
                    types_mapping = self.get_types_mapping(settings)

                    # We need to handle mapped base type
                    # source_data_type is e.g. 'varchar', 'numeric'
                    # types_mapping keys are usually uppercase
                    mapped_type = types_mapping.get(source_data_type.upper(), source_data_type)

                    # If mapped type matches source type (case insensitive), we might still want to uppercase it?
                    # But types_mapping values are usually what we want (e.g. 'VARCHAR', 'NUMERIC')

                    mapped_type_length = f"{mapped_type}({source_length})" if source_length and self.is_string_type(source_data_type) else (f"{mapped_type}({source_length})" if source_length and self.is_numeric_type(source_data_type) else mapped_type)

                    # Actually, logic for length might differ per type (PG TEXT has no length usually, but VARCHAR does)
                    # But here we are producing 'basic_column_type' which serves as fallback.
                    # The original code used source_data_type.

                    # Let's align with how `types_mapping` works.
                    # If mapped_type is TEXT, we might drop length if source was varchar?
                    # The request says: "base data types must be converted into proper PostgreSQL types using conversions returned by get_types_mapping function"

                    # Let's use the same logic as _get_udt_codes_mapping will use.

                    mapped_full_type = mapped_type.upper()
                    if source_length:
                        if self.is_string_type(source_data_type) and mapped_type.upper() not in ('TEXT', 'BYTEA', 'BOOLEAN', 'INTEGER', 'BIGINT', 'SMALLINT', 'DATE', 'TIMESTAMP', 'TIME'):
                             mapped_full_type += f"({source_length})"
                        elif self.is_numeric_type(source_data_type) and mapped_type.upper() in ('NUMERIC', 'DECIMAL'):
                             mapped_full_type += f"({source_length})"

                    result[ordinal_position]['basic_data_type'] = mapped_type # Was source_data_type
                    result[ordinal_position]['basic_character_maximum_length'] = basic_character_maximum_length
                    result[ordinal_position]['basic_numeric_precision'] = data_type_precision if self.is_numeric_type(source_data_type) else None
                    result[ordinal_position]['basic_numeric_scale'] = data_type_scale if self.is_numeric_type(source_data_type) else None
                    result[ordinal_position]['basic_column_type'] = mapped_full_type # Was source_data_type_length

            cursor.close()
            self.disconnect()
            return result
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_default_values(self, settings) -> dict:
        source_schema = settings['source_schema']
        query = f"""
            SELECT
                USER_NAME(def_obj.uid) AS DefaultOwner,
                def_obj.name AS DefaultObjectName,
                sc.colid AS DefinitionLineNumber,
                sc.text AS DefaultDefinitionPart
            FROM
                sysobjects def_obj
            JOIN
                syscomments sc ON def_obj.id = sc.id
            WHERE
                def_obj.type = 'D'  -- 'D' signifies a Default object created with CREATE DEFAULT
            ORDER BY
                DefaultObjectName, DefinitionLineNumber
        """
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        default_values = {}
        for row in cursor.fetchall():
            default_owner = row[0]
            default_object_name = row[1]
            definition_line_number = row[2]
            default_definition_part = row[3].strip()
            if default_object_name not in default_values:
                default_values[default_object_name] = {
                    'default_value_schema': default_owner,
                    'default_value_name': default_object_name,
                    'default_value_sql': default_definition_part,
                    'extracted_default_value': '',
                    'default_value_comment': '',
                }
            else:
                default_values[default_object_name]['default_value_sql'] += f" {default_definition_part}"
        cursor.close()
        self.disconnect()

        for default_object_name, default_value in default_values.items():
            default_value['default_value_sql'] = re.sub(r'\s+', ' ', default_value['default_value_sql']).strip()
            default_value['default_value_sql'] = re.sub(r'\n', '', default_value['default_value_sql'])
            # default_value['default_value_sql'] = re.sub(r'\"', '', default_value['default_value_sql'])
            # default_value['default_value_sql'] = re.sub(r'`', '', default_value['default_value_sql'])
            extracted_default_value = default_value['default_value_sql']
            extracted_default_value = re.sub(rf'create\s+default\s+{re.escape(default_value["default_value_name"])}\s+as', '', extracted_default_value, flags=re.IGNORECASE).strip()
            extracted_default_value = re.sub(rf'default\s+', '', extracted_default_value, flags=re.IGNORECASE).strip()
            extracted_default_value = extracted_default_value.replace('"', '')
            extracted_default_value = extracted_default_value.replace("'", '')
            default_value['extracted_default_value'] = extracted_default_value.strip()
        return default_values


    def get_types_mapping(self, settings):
        # Guard against None settings or missing key
        if settings is None:
            settings = {}
        target_db_type = settings.get('target_db_type', 'postgresql')

        types_mapping = {}
        if target_db_type == 'postgresql':
            types_mapping = {
                'BIGDATETIME': 'TIMESTAMP',
                'DATE': 'DATE',
                'DATETIME': 'TIMESTAMP',
                'BIGTIME': 'TIMESTAMP',
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
                'UNIVARCHAR': 'VARCHAR',
                'TEXT': 'TEXT',
                'SYSNAME': 'TEXT',
                'LONGSYSNAME': 'TEXT',
                'LONG VARCHAR': 'TEXT',
                'LONG NVARCHAR': 'TEXT',
                'UNITEXT': 'TEXT',
                'VARCHAR': 'VARCHAR',

                'CLOB': 'TEXT',
                'DECIMAL': 'DECIMAL',
                'DOUBLE PRECISION': 'DOUBLE PRECISION',
                'FLOAT': 'FLOAT',
                'INTERVAL': 'INTERVAL',
                # 'MONEY': 'MONEY',
                # 'SMALLMONEY': 'MONEY',
                'MONEY': 'INTEGER',
                'SMALLMONEY': 'INTEGER',
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

    def _get_udt_codes_mapping(self, settings=None):
        """
        Returns a dictionary mapping UDT names to their base SQL definition.
        Example: {'MY_TYPE': 'VARCHAR(10)', 'NUM_TYPE': 'NUMERIC(10,2)'}
        """
        if self._udt_cache is not None:
            return self._udt_cache

        udt_map = {}
        udt_rows = []

        # Priority: Check migrator_tables (Protocol Table)
        migrator_tables = settings.get('migrator_tables') if settings else None

        if migrator_tables:
            try:
                # Assuming migrator_tables has fetch_all_user_defined_types
                udt_rows = migrator_tables.fetch_all_user_defined_types()
                # Format of udt_rows from fetch_all_user_defined_types needs to be adapted or used
                # fetch_all_user_defined_types returns raw rows from migrator protocol table
                # We need to decode or map them.
                # Assuming row format: [id, row_data, status, comment, ...] or similar.
                # Use decode_user_defined_type_row

                # We build udt_map from this list
                for row_data in udt_rows:
                    decoded = migrator_tables.decode_user_defined_type_row(row_data)
                    # format: dict(type_name, base_type, length, prec, scale, ...)
                    # base_type needs to be mapped to PG type

                    type_name = decoded['type_name']
                    base_type = decoded['base_type_name']
                    length = decoded['length'] if decoded['length'] else 0
                    prec = decoded['prec'] if decoded['prec'] else 0
                    scale = decoded['scale'] if decoded['scale'] else 0

                    if not base_type: base_type = "UNKNOWN"

                    # Convert base_type to PG type
                    pg_base_type = base_type.upper()
                    if settings:
                         types_mapping = self.get_types_mapping(settings)
                         pg_base_type = types_mapping.get(base_type.upper(), base_type.upper())

                    type_sql = pg_base_type

                    # Apply length/precision logic (similar to query based loop below)
                    if base_type.lower() in ('varchar', 'char', 'nvarchar', 'nchar', 'varbinary', 'binary', 'univarchar', 'unichar'):
                         if pg_base_type not in ('TEXT', 'BYTEA', 'DATE', 'TIMESTAMP', 'TIME', 'BOOLEAN', 'INTEGER', 'BIGINT', 'SMALLINT'):
                              type_sql += f"({length})"
                    elif base_type.lower() in ('numeric', 'decimal'):
                         if pg_base_type in ('NUMERIC', 'DECIMAL'):
                              type_sql += f"({prec},{scale})"

                    udt_map[type_name] = type_sql

                self._udt_cache = udt_map
                return udt_map

            except Exception as e:
                self.config_parser.print_log_message('WARNING', f"Failed to fetch UDTs from protocol table: {e}. Fallback to live query.")


        query = """
            SELECT
                t.name as type_name,
                t.length,
                t.prec,
                t.scale,
                bt.name as base_type_name
            FROM dbo.systypes t
            JOIN dbo.sysusers u ON t.uid = u.uid
            LEFT JOIN dbo.systypes bt ON t.type = bt.type AND bt.usertype < 100
            WHERE t.usertype > 100
            ORDER BY t.name
        """

        try:
            should_disconnect = False
            if not self.connection:
                self.connect()
                should_disconnect = True

            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

            for row in rows:
                type_name = row[0]
                length = row[1]
                prec = row[2]
                scale = row[3]
                base_type = row[4]

                if not base_type:
                    base_type = "UNKNOWN"

                # Convert base_type to PG type
                # Check mapping
                pg_base_type = base_type.upper()
                if settings: ## we must have settings to get types mapping
                     types_mapping = self.get_types_mapping(settings)
                     # types_mapping keys are usually uppercase
                     pg_base_type = types_mapping.get(base_type.upper(), base_type.upper())

                type_sql = pg_base_type

                if base_type.lower() in ('varchar', 'char', 'nvarchar', 'nchar', 'varbinary', 'binary', 'univarchar', 'unichar'):
                    # Check if PG type supports length?
                    # If PG type is TEXT, we drop length.
                    if pg_base_type not in ('TEXT', 'BYTEA', 'DATE', 'TIMESTAMP', 'TIME', 'BOOLEAN', 'INTEGER', 'BIGINT', 'SMALLINT'):
                        type_sql += f"({length})"
                elif base_type.lower() in ('numeric', 'decimal'):
                     if pg_base_type in ('NUMERIC', 'DECIMAL'):
                         type_sql += f"({prec},{scale})"

                udt_map[type_name] = type_sql

            cursor.close()
            if should_disconnect:
                self.disconnect()

        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"Failed to fetch UDTs for substitution: {e}")
            # If we fail, we just return empty map to not break flow
            pass

        self._udt_cache = udt_map
        return udt_map

    def _apply_udt_to_base_type_substitutions(self, text, settings):
        """
        Apply UDT -> Base Type substitutions, BUT respect config substitutions.
        If a UDT is defined in config data_types_substitution, we SKIP it here.
        """
        udt_map = self._get_udt_codes_mapping(settings)
        if not udt_map:
            return text

        # Get config substitutions
        config_substitutions = self.config_parser.get_data_types_substitution()
        # config_substitutions is list of [schema, table, source_type, target_type, comment]
        # We collect source types to ignore

        ignored_types = set()
        if config_substitutions:
            for entry in config_substitutions:
                if len(entry) >= 3 and entry[2]:
                    ignored_types.add(entry[2].upper())

        # Get type mappings for recursive substitution
        types_mapping = self.get_types_mapping({'target_db_type': settings.get('target_db_type', 'postgresql')})

        for udt_name, base_def in udt_map.items():
            if udt_name.upper() in ignored_types:
                continue

            self.config_parser.print_log_message('DEBUG', f"DEBUGGING UDT: Checking {udt_name} -> {base_def}")

            # Recursive step: Convert the base definition if it matches a Sybase type
            # e.g. base_def="univarchar(20)" -> "VARCHAR(20)"
            final_def = base_def
            for sybase_type, pg_type in types_mapping.items():
                 # Use word boundary to match type name, ignoring parens for regex
                 # Escape sybase type just in case
                 final_def = re.sub(rf'\b{re.escape(sybase_type)}\b', pg_type, final_def, flags=re.IGNORECASE)

            # Perform substitution
            # Use word boundaries
            try:
                pattern = re.compile(rf'(?:\[|")?\b{re.escape(udt_name)}\b(?:\]|")?', flags=re.IGNORECASE)
                text = pattern.sub(final_def, text)
            except re.error:
                pass

        return text


    def _apply_data_type_substitutions(self, text):
        """
        Apply data type substitutions defined in the configuration.
        Substitutions are applied based on table name (optional), column name (optional),
        and source data type (regex).
        In the context of functions/procedures/triggers, we mainly care about the source data type matching.
        """
        substitutions = self.config_parser.get_data_types_substitution()
        if not substitutions:
            return text

        # Sort substitutions by length of source type (descending) to match specific types first?
        # Or just rely on config order. Config order is probably best.

        for entry in substitutions:
            # entry: [table_name, column_name, source_type, target_type, comment]
            if len(entry) != 5:
                continue

            # For general code substitution, we ignore table_name and column_name usually,
            # or treat them as wildcards if they are empty.
            # However, for function params/vars, we assume no table/column context matches unless explicitly handled.
            # But the requirement is likely to map generic types like 'TypID' -> 'BIGINT'.
            # So we look for entries where source_type is defined.

            # We are not passed table/column context here easily for params/vars unless we parse them deeply.
            # So we focus on source_type match.

            source_type = entry[2]
            target_type = entry[3]

            if source_type:
                # Use regex or simple replace? Config says regex.
                # Use word boundaries to avoid partial replacement.
                try:
                    pattern = re.compile(rf'\b{source_type}\b', flags=re.IGNORECASE)
                    text = pattern.sub(target_type, text)
                except re.error:
                    self.config_parser.print_log_message('WARNING', f"Invalid regex in data_types_substitution: {source_type}")

        return text


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
        SELECT * FROM (
            SELECT
                i.name + '_' + convert(varchar, i.id) + '_' + convert(varchar, i.indid) as index_name,  /* sybase allows duplicate names of indexes */
                case when i.status & 2 = 2 then 1 else 0 end as index_unique,
                case when index_col(o.name, i.indid, 1) is not null then '"' + index_col(o.name, i.indid, 1) + '"' end +
                case when index_col(o.name, i.indid, 2) is not null then ', "'+index_col(o.name, i.indid, 2) + '"' else '' end +
                case when index_col(o.name, i.indid, 3) is not null then ', "'+index_col(o.name, i.indid, 3) + '"' else '' end +
                case when index_col(o.name, i.indid, 4) is not null then ', "'+index_col(o.name, i.indid, 4) + '"' else '' end +
                case when index_col(o.name, i.indid, 5) is not null then ', "'+index_col(o.name, i.indid, 5) + '"' else '' end +
                case when index_col(o.name, i.indid, 6) is not null then ', "'+index_col(o.name, i.indid, 6) + '"' else '' end +
                case when index_col(o.name, i.indid, 7) is not null then ', "'+index_col(o.name, i.indid, 7) + '"' else '' end +
                case when index_col(o.name, i.indid, 8) is not null then ', "'+index_col(o.name, i.indid, 8) + '"' else '' end +
                case when index_col(o.name, i.indid, 9) is not null then ', "'+index_col(o.name, i.indid, 9) + '"' else '' end +
                case when index_col(o.name, i.indid, 10) is not null then ', "'+index_col(o.name, i.indid, 10) + '"' else '' end +
                case when index_col(o.name, i.indid, 11) is not null then ', "'+index_col(o.name, i.indid, 11) + '"' else '' end +
                case when index_col(o.name, i.indid, 12) is not null then ', "'+index_col(o.name, i.indid, 12) + '"' else '' end
                as column_list,
                case when i.status & 2048 = 2048 then 1 else 0 end as primary_key_index
                FROM sysobjects o, sysindexes i
                WHERE i.id = o.id
                    AND o.id = {source_table_id}
                    AND o.type = 'U'
                    AND indid > 0
        ) a WHERE nullif(column_list, '') IS NOT NULL  /* omit system indexes without column list */
        ORDER BY index_name
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
                index_columns = index[2].strip()
                index_primary_key = index[3]
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
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

        # Get all indexes for the table
        order_num = 1
        table_constraints = {}
        index_query = f"""
        SELECT
            object_name(c.constrid, db_id()) as constraint_name,
            case when col_name(c.tableid, r.fokey1, db_id()) is not null then '"' + col_name(c.tableid, r.fokey1, db_id()) + '"' end +
            case when col_name(c.tableid, r.fokey2, db_id()) is not null then ',"' + col_name(c.tableid, r.fokey2, db_id()) + '"' else '' end +
            case when col_name(c.tableid, r.fokey3, db_id()) is not null then ',"' + col_name(c.tableid, r.fokey3, db_id()) + '"' else '' end +
            case when col_name(c.tableid, r.fokey4, db_id()) is not null then ',"' + col_name(c.tableid, r.fokey4, db_id()) + '"' else '' end +
            case when col_name(c.tableid, r.fokey5, db_id()) is not null then ',"' + col_name(c.tableid, r.fokey5, db_id()) + '"' else '' end
            as foreign_keys_columns,
            user_name(oc.uid) as ref_table_schema,
            oc.name as ref_table_name,
            case when col_name(r.reftabid, r.refkey1, r.pmrydbid) is not null then '"' + col_name(r.reftabid, r.refkey1, r.pmrydbid) + '"' end +
            case when col_name(r.reftabid, r.refkey2, r.pmrydbid) is not null then ',"' + col_name(r.reftabid, r.refkey2, r.pmrydbid) + '"' else '' end +
            case when col_name(r.reftabid, r.refkey3, r.pmrydbid) is not null then ',"' + col_name(r.reftabid, r.refkey3, r.pmrydbid) + '"' else '' end +
            case when col_name(r.reftabid, r.refkey4, r.pmrydbid) is not null then ',"' + col_name(r.reftabid, r.refkey4, r.pmrydbid) + '"' else '' end +
            case when col_name(r.reftabid, r.refkey5, r.pmrydbid) is not null then ',"' + col_name(r.reftabid, r.refkey5, r.pmrydbid) + '"' else '' end
            as ref_key_columns
        FROM sysconstraints c
        JOIN dbo.sysreferences r on c.constrid = r.constrid
        JOIN dbo.sysobjects ot on c.tableid = ot.id
        JOIN dbo.sysobjects oc on r.reftabid = oc.id
        WHERE c.tableid = {source_table_id}
        AND c.status & 64 = 64
        ORDER BY constraint_name
        """
        ## status & 64 = 64 - foreign key constraint (0x0040)
        self.connect()
        cursor = self.connection.cursor()
        self.config_parser.print_log_message('DEBUG', f"Reading constraints for {source_table_name}")
        cursor.execute(index_query)
        constraints = cursor.fetchall()

        for constraint in constraints:
            fk_name = constraint[0]
            fk_column = constraint[1].strip()
            ref_table_schema = constraint[2]
            ref_table_name = constraint[3]
            ref_column = constraint[4].strip()

            table_constraints[order_num] = {
                'constraint_name': fk_name,
                'constraint_owner': source_table_schema,
                'constraint_type': 'FOREIGN KEY',
                'constraint_columns': fk_column,
                'referenced_table_schema': ref_table_schema,
                'referenced_table_name': ref_table_name,
                'referenced_columns': ref_column,
                'constraint_sql': '',
                'constraint_comment': ''
            }
            order_num += 1

        # get check constraints
        check_query = f"""
            SELECT
                o.name AS ConstraintName,
                s_check.text AS CheckConstraintDefinition -- For check constraints
            FROM
                sysconstraints c
            JOIN
                sysobjects o ON c.constrid = o.id
            LEFT JOIN
                syscomments s_check ON o.id = s_check.id
            WHERE c.status & 128 = 128
            AND c.tableid = {source_table_id}
        """
        ## status & 128 = 128 - check constraint (0x0080)
        cursor.execute(check_query)
        check_constraints = cursor.fetchall()
        for check_constraint in check_constraints:
            check_name = check_constraint[0]
            check_expression = check_constraint[1].strip()
            check_expression = check_expression.replace('CONSTRAINT', '').replace(check_name, '').replace('CHECK','').strip()
            table_constraints[order_num] = {
                'constraint_name': check_name,
                'constraint_type': 'CHECK',
                'constraint_sql': check_expression,
                'constraint_comment': ''
            }
            order_num += 1

        cursor.close()
        self.disconnect()
        return table_constraints

    def get_create_constraint_sql(self, settings):
        return ""

    def fetch_funcproc_names(self, schema: str):
        funcproc_data = {}
        order_num = 1
        query = f"""
            SELECT
                DISTINCT
                o.name,
                o.id,
                CASE
                    WHEN o.type = 'P' THEN 'Procedure'
                    WHEN o.type = 'F' THEN 'Function'
                    WHEN o.type = 'XP' THEN 'Extended Procedure'
                END AS type,
                o.sysstat
            FROM syscomments c, sysobjects o
            WHERE o.id=c.id
                AND user_name(o.uid) = '{schema}'
                AND type in ('F', 'P', 'XP')
                AND (o.sysstat & 4 = 4 or o.sysstat & 10 = 10 or o.sysstat & 12 = 12)
            ORDER BY o.name
        """
        self.config_parser.print_log_message('DEBUG3', f"Fetching function/procedure names for schema {schema}")
        self.config_parser.print_log_message('DEBUG3', f"Query: {query}")
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            funcproc_data[order_num] = {
                'name': row[0],
                'id': row[1],
                'type': row[2],
                'sysstat': row[3],
                'comment': ''
            }
            order_num += 1
        cursor.close()
        self.disconnect()
        return funcproc_data

    def fetch_funcproc_code(self, funcproc_id: int):
        """
        Fetches the code of a function or procedure by its ID. General query:

            SELECT u.name as owner, o.name as proc_name, c.colid as line_num, c.text as source_code
            FROM sysusers u, syscomments c, sysobjects o
            WHERE o.type = 'P' AND o.id = c.id AND o.uid = u.uid
            ORDER BY o.id, c.colid
        """
        query = f"""
            SELECT c.text
            FROM syscomments c, sysobjects o
            WHERE o.id=c.id and o.id = {funcproc_id}
            ORDER BY c.colid
        """
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        procbody = cursor.fetchall()
        cursor.close()
        self.disconnect()
        procbody_str = ''.join([body[0] for body in procbody])
        return procbody_str

    def convert_funcproc_code_v2(self, settings):
        print(f"DEBUG: Entered convert_funcproc_code_v2 for {settings.get('funcproc_name')}")
        """
        Parser-based conversion using sqlglot.
        Breaks code into logical statements (expressions) and transpiles to Postgres.
        Includes ported V1 logic for rowcount, exec, assignments, cursors, etc.
        """
        funcproc_code = settings['funcproc_code']
        if not funcproc_code:
             return "-- [WARNING] Empty input code provided"
        # target_db_type = settings['target_db_type'] # Typically 'postgres'
        # print(f"DEBUG: V2 Input Code FULL:\n{repr(funcproc_code)}")
        # print(f"DEBUG Input Len: {len(funcproc_code)}")

        # --- Pre-processing (Ported from V1 & Enhancements) ---

        # 0. Encapsulate comments
        funcproc_code = re.sub(r'--([^\n]*)', r'/*\1*/', funcproc_code)

        # 1. Remove GO
        funcproc_code = re.sub(r'\bGO\b', '', funcproc_code, flags=re.IGNORECASE)

        # 2. Temp Table Replacement (# -> tt_)
        funcproc_code = re.sub(r'#([a-zA-Z0-9_]+)', r'tt_\1', funcproc_code)

        # 3. Rename @return to @v_return
        funcproc_code = re.sub(r'@return\b', '@v_return', funcproc_code, flags=re.IGNORECASE)

        # 4. Global @@rowcount detection
        has_rowcount = '@@rowcount' in funcproc_code.lower()

        # 5. Fix common typos
        funcproc_code = re.sub(r'\bfetc\s+h\b', 'FETCH', funcproc_code, flags=re.IGNORECASE)
        funcproc_code = re.sub(r'\bc\s+ursor\b', 'CURSOR', funcproc_code, flags=re.IGNORECASE)

        # 6. @@sqlstatus Replacement
        funcproc_code = re.sub(r'@@sqlstatus\s*!=\s*0', 'NOT FOUND', funcproc_code, flags=re.IGNORECASE)
        funcproc_code = re.sub(r'@@sqlstatus\s*=\s*0', 'FOUND', funcproc_code, flags=re.IGNORECASE)
        funcproc_code = re.sub(r'@@sqlstatus\s*=\s*2', 'NOT FOUND', funcproc_code, flags=re.IGNORECASE)

        # 7. Extract Header (Params) and Body
        header_match = re.search(r'CREATE\s+(?:PROC|PROCEDURE)\s+([a-zA-Z0-9_\.]+)(.*?)(\bAS\b)', funcproc_code, flags=re.IGNORECASE | re.DOTALL)

        body_content = funcproc_code
        params_str = ""
        proc_name = settings['funcproc_name']

        if header_match:
             proc_name_extracted = header_match.group(1)
             params_str = header_match.group(2).strip()
             body_content = funcproc_code[header_match.end(3):].strip()

        # Strip outer BEGIN and END if present
        if re.match(r'^BEGIN\b', body_content, flags=re.IGNORECASE):
             body_content = re.sub(r'^BEGIN', '', body_content, count=1, flags=re.IGNORECASE).strip()
             body_content = re.sub(r'END\s*$', '', body_content, flags=re.IGNORECASE).strip()

        declarations = []

        # --- Pre-process Cursors (Run First!) ---
        # Match DECLARE name CURSOR FOR ...
        cursor_declarations = []
        def cursor_replacer(match):
             full = match.group(0)
             clean = re.sub(r'^\s*DECLARE\s+', '', full, flags=re.IGNORECASE).strip()
             cursor_declarations.append(clean + ';')
             return '' # remove from body

        # Explicit pattern compile for debugging
        cursor_pattern = re.compile(r'DECLARE\s+[a-zA-Z0-9_]+\s+CURSOR\s+FOR\s+.*?(?=\bOPEN\b)', re.IGNORECASE | re.DOTALL)
        body_content = cursor_pattern.sub(cursor_replacer, body_content)
        declarations.extend(cursor_declarations)

        # --- Mask Cursor Commands (OPEN, FETCH, CLOSE, DEALLOCATE) ---
        # Mask as SELECT 'CURSOR_CMD:...' which uses standard SQL literal string.

        def mask_fetch(match):
            cur = match.group(1)
            var = match.group(2)
            return f"SELECT * FROM (SELECT 'CURSOR_CMD:FETCH {cur} INTO {var}') AS _cursor_mask"

        def mask_open(match):
            return f"SELECT * FROM (SELECT 'CURSOR_CMD:OPEN {match.group(1)}') AS _cursor_mask"

        def mask_close(match):
            return f"SELECT * FROM (SELECT 'CURSOR_CMD:CLOSE {match.group(1)}') AS _cursor_mask"

        def mask_deallocate(match):
            return f"SELECT * FROM (SELECT 'CURSOR_CMD:NULL; -- DEALLOCATE {match.group(1)}') AS _cursor_mask"

        body_content = re.sub(r'\bFETCH\s+([a-zA-Z0-9_]+)\s+INTO\s+@([a-zA-Z0-9_]+)', mask_fetch, body_content, flags=re.IGNORECASE)
        body_content = re.sub(r'\bOPEN\s+([a-zA-Z0-9_]+)', mask_open, body_content, flags=re.IGNORECASE)
        body_content = re.sub(r'\bCLOSE\s+([a-zA-Z0-9_]+)', mask_close, body_content, flags=re.IGNORECASE)
        body_content = re.sub(r'\bDEALLOCATE\s+(?:CURSOR\s+)?([a-zA-Z0-9_]+)', mask_deallocate, body_content, flags=re.IGNORECASE)
        # print(f"DEBUG: Body after masking:\n{body_content[:1000]}")

        # --- Pre-process Declarations (Variables) ---
        types_mapping = self.get_types_mapping({'target_db_type': 'postgresql'})

        def split_respecting_parens(text):
            parts = []
            current = []
            depth = 0
            for char in text:
                if char == '(':
                    depth += 1
                elif char == ')':
                    depth -= 1
                if char == ',' and depth == 0:
                    parts.append("".join(current).strip())
                    current = []
                else:
                    current.append(char)
            if current:
                parts.append("".join(current).strip())
            return parts

        def declaration_replacer(match):
            full_decl = match.group(0)
            content = full_decl[7:].strip() # len('DECLARE') = 7
            content_clean = content.replace('@', '')
            content_clean = self._apply_data_type_substitutions(content_clean)
            content_clean = self._apply_udt_to_base_type_substitutions(content_clean, settings)
            for sybase_type, pg_type in types_mapping.items():
                content_clean = re.sub(rf'\b{re.escape(sybase_type)}\b', pg_type, content_clean, flags=re.IGNORECASE)

            parts = split_respecting_parens(content_clean)
            for part in parts:
                declarations.append(part.strip() + ';')
            return ''

        # Extract Variable Declarations matches generic DECLARE
        body_content = re.sub(r'DECLARE\s+@.*?(?=\bBEGIN\b|\bIF\b|\bWHILE\b|\bSELECT\b|\bINSERT\b|\bUPDATE\b|\bDELETE\b|\bRETURN\b|\bSET\b|\bFETCH\b|\bOPEN\b|\bCLOSE\b|\bDEALLOCATE\b|\bDECLARE\b|$)', declaration_replacer, body_content, flags=re.IGNORECASE | re.DOTALL)

        converted_statements = []

        # --- Use sqlglot to parse the cleaned body ---
        try:
            # Using error_level='ignore' to allow partial parsing and avoid hard crashes
            parsed = sqlglot.parse(body_content.strip(), read='tsql', error_level='ignore')
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Global parsing failed for code: {funcproc_code[:100]}... Error: {e}")
            return f"/* PARSING FAILED: {e} */\n" + funcproc_code

        for expression in parsed:
            # Transpile to Postgres
            try:
                pg_sql = expression.sql(dialect='postgres')
                skip_semicolon = False

                # 1. Handle masked commands (SELECT 'CURSOR_CMD:...')
                is_cursor_cmd = False
                if isinstance(expression, exp.Select):
                      # Check generated SQL for CURSOR_CMD
                      if "'CURSOR_CMD:" in pg_sql:
                           # Regex extract
                           match_cmd = re.search(r"'CURSOR_CMD:(.*?)'", pg_sql)
                           if match_cmd:
                                pg_sql = match_cmd.group(1)
                                is_cursor_cmd = True

                if not is_cursor_cmd:
                    # 2. Normal PRINT -> RAISE NOTICE
                    if isinstance(expression, exp.Command) and expression.this.upper().startswith('PRINT'):
                        msg_match = re.search(r'PRINT\s+(.*)', expression.this, re.IGNORECASE)
                        if msg_match:
                            msg_val = msg_match.group(1).strip()
                            pg_sql = f"RAISE NOTICE {msg_val}"
                        else:
                            pg_sql = f"RAISE NOTICE 'PRINT found'"

                    # 3. SELECT Assignments & SELECT INTO
                    elif isinstance(expression, exp.Select):
                        generated = expression.sql(dialect='postgres')
                        match_assign = re.match(r'SELECT\s+([a-zA-Z0-9_]+)\s*=\s*(.*)', generated, re.IGNORECASE)
                        if match_assign:
                            var_name = match_assign.group(1).replace('@', '')
                            val_expr = match_assign.group(2)
                            pg_sql = f"{var_name} := {val_expr}"
                        elif 'INTO tt_' in generated:
                            match_into = re.search(r'\bINTO\s+(tt_[a-zA-Z0-9_]+)', generated)
                            if match_into:
                                table = match_into.group(1)
                                select_part = re.sub(r'\bINTO\s+tt_[a-zA-Z0-9_]+\s*', '', generated)
                                pg_sql = f"DROP TABLE IF EXISTS {table}; CREATE TEMP TABLE {table} AS {select_part}"
                            else:
                                pg_sql = generated
                        else:
                            pg_sql = generated

                    # 4. EXEC -> PERFORM or Assignment
                    elif isinstance(expression, exp.Command) and (expression.this.upper().startswith('EXEC') or expression.this.upper().startswith('EXECUTE')):
                        gen_sql = expression.sql(dialect='postgres')

                        match_exec_assign = re.match(r'(?:EXEC|EXECUTE)\s+([a-zA-Z0-9_]+)\s*=\s*([a-zA-Z0-9_\.]+)(.*)', gen_sql, re.IGNORECASE)
                        match_exec = re.match(r'(?:EXEC|EXECUTE)\s+([a-zA-Z0-9_\.]+)(.*)', gen_sql, re.IGNORECASE)

                        if match_exec_assign:
                            var = match_exec_assign.group(1).replace('@', '')
                            proc = match_exec_assign.group(2)
                            args = match_exec_assign.group(3).strip()
                            args = args.replace('@', '')
                            if args and not args.startswith('('): args = f"({args})"
                            elif not args: args = "()"
                            pg_sql = f"{var} := {proc}{args}"
                        elif match_exec:
                            proc = match_exec.group(1)
                            args = match_exec.group(2).strip()
                            args = args.replace('@', '')
                            if args and not args.startswith('('): args = f"({args})"
                            elif not args: args = "()"
                            pg_sql = f"PERFORM {proc}{args}"
                        else:
                            pg_sql = gen_sql

                    # 5. RETURN
                    elif isinstance(expression, exp.Return):
                        pg_sql = "RETURN"

                    # 6. IF / WHILE (Control Flow)
                    # Safe check for existence of While/If before using instance check if module is old
                    elif isinstance(expression, exp.If):
                        pg_sql = expression.sql(dialect='postgres')
                        if 'END IF' not in pg_sql.upper():
                            pg_sql += " END IF"
                        skip_semicolon = True
                    elif hasattr(exp, 'While') and isinstance(expression, exp.While):
                        pg_sql = expression.sql(dialect='postgres')
                        skip_semicolon = True

                    else:
                        pg_sql = expression.sql(dialect='postgres')


                    # Fix DEALLOCATE CURSOR -> DEALLOCATE
                    if pg_sql.upper().startswith('DEALLOCATE CURSOR '):
                        pg_sql = pg_sql.replace('DEALLOCATE CURSOR ', 'DEALLOCATE ')
                    if pg_sql.upper().startswith('CLOSE CURSOR '): # Sybase CLOSE name, PG CLOSE name.
                        pg_sql = pg_sql.replace('CLOSE CURSOR ', 'CLOSE ') # just in case

                    # --- Post-processing per statement ---

                    # Fix Variables: Remove '@' (sqlglot might have left some)
                    pg_sql = re.sub(r'(?<!\w)@([a-zA-Z0-9_]+)', r'\1', pg_sql)

                    # @@rowcount -> _rowcount replacement (if used as var)
                    pg_sql = re.sub(r'@@rowcount', '_rowcount', pg_sql, flags=re.IGNORECASE)

                    # Semicolon
                    if not skip_semicolon and not pg_sql.strip().endswith(';'):
                        pg_sql += ';'

                    # Fix sqlglot 'INTERVAL variable unit' generation (e.g. INTERVAL -days day)
                    # Maps to: (variable * INTERVAL '1 unit')
                    pg_sql = re.sub(
                        r"(?i)\bINTERVAL\s+([-@\w]+)\s+(YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)S?\b",
                        r"(\1 * INTERVAL '1 \2')",
                        pg_sql
                    )

                    converted_statements.append(pg_sql)

                    # --- Rowcount Injection ---
                    if has_rowcount:
                        is_dml = isinstance(expression, (exp.Insert, exp.Update, exp.Delete))
                        if 'SELECT' in pg_sql.upper() and 'INTO' in pg_sql.upper(): is_dml = True
                        if is_dml:
                            converted_statements.append("GET DIAGNOSTICS _rowcount = ROW_COUNT;")

            except Exception as e:
                self.config_parser.print_log_message('WARNING', f"Failed to transpile statement: {expression}. Error: {e}")
                converted_statements.append(f"-- FAILED CONVERSION: {expression.sql()}\n-- Error: {e}")

        # --- Hoist Declarations ---
        final_stmts_clean = []
        for stmt in converted_statements:
             stmt_stripped = stmt.strip()
             # Check for Variable declaration
             if stmt_stripped.upper().startswith('DECLARE '):
                  declarations.append(stmt_stripped)
             else:
                  final_stmts_clean.append(stmt)

        # Inject _rowcount declaration
        if has_rowcount:
             declarations.insert(0, "_rowcount INTEGER;")

        final_body = "\n".join(final_stmts_clean)

        # --- Clean Parameters (Ported Logic) ---
        pg_params_str = params_str
        if pg_params_str:
             pg_params_str = re.sub(r'@', '', pg_params_str)
             pg_params_str = re.sub(r'\bnumeric\b', 'numeric', pg_params_str, flags=re.IGNORECASE)
             pg_params_str = re.sub(r'\bint\b', 'integer', pg_params_str, flags=re.IGNORECASE)

        # Construct DDL
        ddl = f"CREATE OR REPLACE FUNCTION {settings['target_schema']}.{proc_name}({pg_params_str})\n"
        ddl += "RETURNS void AS $$\n"
        ddl += "DECLARE\n"
        if declarations:
             ddl += "\n".join(declarations) + "\n"
        ddl += "BEGIN\n"
        ddl += final_body + "\n"
        ddl += "END;\n"
        ddl += "$$ LANGUAGE plpgsql;"

        return ddl

    def convert_funcproc_code(self, settings):
        try:
            v2_result = self.convert_funcproc_code_v2(settings)
            # Check for failure markers, empty result, or silent empty body
            is_valid = v2_result and "/* PARSING FAILED:" not in v2_result
            if is_valid and "-- [WARNING] Empty input" not in v2_result:
                 if not re.search(r'BEGIN\s+END;', v2_result.strip()):
                      return v2_result

            # Fallback to V1
            self.config_parser.print_log_message('WARNING', "V2 Conversion failed or incomplete, falling back to V1.")
            return self.convert_funcproc_code_v1(settings)
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"V2 Conversion Critical Failure: {e}. Falling back to V1.")
            try:
                return self.convert_funcproc_code_v1(settings)
            except Exception as v1_e:
                return f"/* CRITICAL FAILURE IN V1/V2: {e} / {v1_e} */\n" + settings.get('funcproc_code', '')
    def convert_funcproc_code_v1(self, settings):
        funcproc_code = settings['funcproc_code']
        target_db_type = settings['target_db_type']

        # 0. Encapsulate comments
        # Convert -- comment to /* comment */ to prevent breaking code
        funcproc_code = re.sub(r'--([^\n]*)', r'/*\1*/', funcproc_code)

        # 1. Clean up potential GO statements and strict comments handling if needed (minimal here)
        converted_code = re.sub(r'\bGO\b', '', funcproc_code, flags=re.IGNORECASE)

        # PROACTIVE FIX: Common OCR/Typo errors in source code
        converted_code = re.sub(r'\bfetc\s+h\b', 'FETCH', converted_code, flags=re.IGNORECASE)
        converted_code = re.sub(r'\bc\s+ursor\b', 'CURSOR', converted_code, flags=re.IGNORECASE)

        # 1.5 Transaction Control - Not supported in PG functions
        # We comment them out BEFORE extracting body so 'BEGIN TRAN' doesn't confuse BEGIN/END
        # Handle SAVE TRAN, and named transactions.
        # Use NULL; /* command */ to preserve statement validity for IF/ELSE

        # Regex for BEGIN/COMMIT/ROLLBACK/SAVE TRAN[SACTION] [name]
        # We capture the full command to comment it out
        # We replace with NULL; /* [COMMAND] */
        # Note: IF ... THEN NULL; is valid. IF ... THEN -- comment \n ... might be risky if newline changes logic.
        # NULL; is safest relative to flow.

        def tran_replacer(match):
             cmd = match.group(1)
             # Avoid 'BEGIN' word in comment to not confuse IF block detection which looks for BEGIN
             cmd_safe = re.sub(r'\bBEGIN\b', 'START', cmd, flags=re.IGNORECASE)
             return f"NULL; /* {cmd_safe} */"

        # Use [ \t] instead of \s before the optional name to prevent matching across newlines
        # Also support TRAN, TRANS, TRANSACTION via TRAN\w*
        # And ensure the 'name' is NOT a keyword (declare, select, etc) using negative lookahead
        # to prevent consuming the next statement if whitespace matching is loose.
        converted_code = re.sub(r'(\b(?:BEGIN|COMMIT|ROLLBACK|SAVE)\s+(?:TRAN\w*)(?:[ \t]+(?!(?:DECLARE|SELECT|INSERT|UPDATE|DELETE|EXECUTE|EXEC|RETURN|IF|WHILE|BEGIN|COMMIT|ROLLBACK|SAVE)\b)[a-zA-Z0-9_]+)?\b)', tran_replacer, converted_code, flags=re.IGNORECASE)

        # 1.6 EXECUTE -> PERFORM (Function Call)
        # Sybase: EXEC[UTE] proc_name [args]
        # PG: PERFORM proc_name(args);
        # Helper to transform EXEC calls
        def exec_transformer(match):
            proc_name = match.group(1)
            args_str = match.group(2)
            if args_str:
                args = args_str.strip()
                # If args are not in parens, wrap them
                if not args.startswith('('):
                   # Sybase args are comma separated, sometimes with @
                   # We clean @
                   args = args.replace('@', '')
                   # We assume simple comma separation works for generated SQL
                   return f"PERFORM {proc_name}({args});"
                else:
                    # Already has parens (e.g. EXEC (@sql)) - could be dynamic SQL
                    # If it's dynamic SQL, it should be text.
                    return f"PERFORM {proc_name}{args};"
            else:
                 # No args
                 return f"PERFORM {proc_name}();"

        # Regex to catch EXEC/EXECUTE followed by name and optional args
        # We match until newline or semicolon or end of string
        # Regex to catch EXEC/EXECUTE variants
        # 1. Assignment: EXECUTE @var = proc ...
        # 2. Simple: EXECUTE proc ...

        def exec_assignment_transformer(match):
             variable = match.group(1).replace('@', '')
             proc_name = match.group(2)
             args = match.group(3)
             if args:
                  args = args.replace('@', '').strip()
                  if args.startswith(','): args = args[1:].strip()
                  # args should be comma separated.
                  # wrap in parens
                  return f"{variable} := {proc_name}({args});"
             else:
                  return f"{variable} := {proc_name}();"

        converted_code = re.sub(r'\b(?:EXEC|EXECUTE)\s+(@[a-zA-Z0-9_]+)\s*=\s*([a-zA-Z0-9_\.]+)([^\n;]*)(?=;|\n|$)', exec_assignment_transformer, converted_code, flags=re.IGNORECASE)

        # 2. Simple EXEC (existing logic, mostly)
        converted_code = re.sub(r'\b(?:EXEC|EXECUTE)\s+([a-zA-Z0-9_\.]+)([^\n;]*)(?=;|\n|$)', exec_transformer, converted_code, flags=re.IGNORECASE)

        # 2. Extract Header and Body
        # Pattern: CREATE PROC[EDURE] name [params] AS [BEGIN] body [END]
        header_match = re.search(r'CREATE\s+(?:PROC|PROCEDURE)\s+([a-zA-Z0-9_\.]+)(.*?)(\bAS\b)', converted_code, flags=re.IGNORECASE | re.DOTALL)

        func_schema = ""
        func_name = ""
        params_str = ""
        body_start_idx = 0

        if header_match:
            print(f"DEBUG_MATCH: SUCCESS params='{header_match.group(2)}'")
            full_name = header_match.group(1)
            params_str = header_match.group(2).strip()
            # body technically starts after AS.
            body_start_idx = header_match.end(3)

            if '.' in full_name:
                parts = full_name.split('.')
                func_schema = parts[0]
                func_name = parts[1]
            else:
                func_name = full_name

            if not func_schema:
                func_schema = settings.get('target_schema', 'public')
        else:
            # Fallback if regex fails - return original or minor mod
            print(f"DEBUG_FAIL: Header regex failed. Code start: {converted_code[:100]}")
            return converted_code

        # Helper to split by comma respecting parens
        def split_respecting_parens(text):
            parts = []
            current = []
            depth = 0
            for char in text:
                if char == '(':
                    depth += 1
                elif char == ')':
                    depth -= 1

                if char == ',' and depth == 0:
                    parts.append("".join(current).strip())
                    current = []
                else:
                    current.append(char)
            if current:
                parts.append("".join(current).strip())
            return parts

        # 3. Process Parameters
        types_mapping = self.get_types_mapping({'target_db_type': target_db_type})

        pg_params = []
        if params_str:
            # Fix 1: Robustly strip outer parens
            clean_params = params_str.strip()

            # Remove comments to allow clean parenthesis check
            # Note: convert_funcproc_code_v1/v2 already converts -- to /* */
            clean_params = re.sub(r'/\*.*?\*/', '', clean_params, flags=re.DOTALL).strip()

            while clean_params.startswith('(') and clean_params.endswith(')'):
                clean_params = clean_params[1:-1].strip()
                # Re-clean comments in case parens wrapped comments? Unlikely but safe.
                clean_params = re.sub(r'/\*.*?\*/', '', clean_params, flags=re.DOTALL).strip()

            print(f"DEBUG_PARA: '{func_name}' ORG='{params_str}' CLEAN='{clean_params}'")

            clean_params = clean_params.replace('@', '')

            # Custom type substitutions first
            clean_params = self._apply_data_type_substitutions(clean_params)
            clean_params = self._apply_udt_to_base_type_substitutions(clean_params, settings)

            # Fix 2: Handle OUTPUT -> INOUT
            # Strategy: Split by comma, process each param
            param_parts = split_respecting_parens(clean_params)
            processed_params = []

            for p in param_parts:
                p_clean = p.strip()
                # Check for OUTPUT (case insensitive)
                # Matches: param_name type OUTPUT
                output_match = re.search(r'\bOUTPUT\b', p_clean, flags=re.IGNORECASE)
                if output_match:
                    # Remove OUTPUT
                    p_clean = re.sub(r'\bOUTPUT\b', '', p_clean, flags=re.IGNORECASE).strip()
                    # Add INOUT prefix if not already present (unlikely in Sybase but good for safety)
                    # Sybase: @p type OUTPUT
                    # PG: INOUT p type
                    # We assume format is "name type" or "name type default val"
                    # Just prepend INOUT
                    p_clean = "INOUT " + p_clean

                # Standard type mapping
                for sybase_type, pg_type in types_mapping.items():
                    p_clean = re.sub(rf'\b{re.escape(sybase_type)}\b', pg_type, p_clean, flags=re.IGNORECASE)

                processed_params.append(p_clean)

            pg_params_str = ", ".join(processed_params)
        else:
            print(f"DEBUG_PARA_EMPTY: '{func_name}'")
            pg_params_str = ""

        # 4. Process Body
        body_content = converted_code[body_start_idx:].strip()

        # Remove leading BEGIN and trailing END if they wrap the entire body
        if re.match(r'^BEGIN\b', body_content, flags=re.IGNORECASE):
            body_content = re.sub(r'^BEGIN', '', body_content, count=1, flags=re.IGNORECASE).strip()
            body_content = re.sub(r'END\s*$', '', body_content, flags=re.IGNORECASE).strip()

        # 5. Variable Declarations
        declarations = []

        def declaration_replacer(match):
            full_decl = match.group(0)
            content = full_decl[7:].strip() # len('DECLARE') = 7

            content_clean = content.replace('@', '')
            # Custom type substitutions first
            content_clean = self._apply_data_type_substitutions(content_clean)
            content_clean = self._apply_udt_to_base_type_substitutions(content_clean, settings)
            for sybase_type, pg_type in types_mapping.items():
                content_clean = re.sub(rf'\b{re.escape(sybase_type)}\b', pg_type, content_clean, flags=re.IGNORECASE)

            parts = split_respecting_parens(content_clean)
            for part in parts:
                declarations.append(part.strip() + ';')

            return '' # Remove from body

        body_content = re.sub(r'DECLARE\s+@.*?(?=\bBEGIN\b|\bIF\b|\bWHILE\b|\bSELECT\b|\bINSERT\b|\bUPDATE\b|\bDELETE\b|\bRETURN\b|\bSET\b|\bFETCH\b|\bOPEN\b|\bCLOSE\b|\bDEALLOCATE\b|\bDECLARE\b|$)', declaration_replacer, body_content, flags=re.IGNORECASE | re.DOTALL)

        # 6. Global Variable Replacement in Body
        # Replace @var with var, BUT skip @@system_vars

        # FIX: Rename @return to @v_return to avoid collision with keyword RETURN
        body_content = re.sub(r'@return\b', '@v_return', body_content, flags=re.IGNORECASE)

        body_content = re.sub(r'(?<!@)@([a-zA-Z0-9_]+)', r'\1', body_content)

        # Handle @@rowcount
        has_rowcount = '@@rowcount' in body_content.lower()
        if has_rowcount:
             declarations.append('_rowcount INTEGER;')
             # Replace usage
             body_content = re.sub(r'@@rowcount', '_rowcount', body_content, flags=re.IGNORECASE)

        # RAISERROR conversion
        # Sybase: RAISERROR num "msg" or RAISERROR num 'msg'
        # PG: RAISE EXCEPTION 'msg' USING ERRCODE = 'num'
        def raiserror_replacer(match):
            code = match.group(1)
            msg = match.group(2)
            # Fix quotes if double
            if msg.startswith('"'):
                msg = "'" + msg[1:-1].replace("'", "''") + "'"
            return f"RAISE EXCEPTION {msg} USING ERRCODE = '{code}'"

        body_content = re.sub(r'RAISERROR\s+(\d+)\s+(["\'].*?["\'])', raiserror_replacer, body_content, flags=re.IGNORECASE)

        # 7. Conversions (Line by line / block based)

        # Cursor Declarations
        # Fix: Capture optional FOR UPDATE
        cursor_matches = re.finditer(r'DECLARE\s+([a-zA-Z0-9_]+)\s+CURSOR\s+FOR\s+(.*?)(\s+FOR\s+UPDATE)?\s+(?=\bOPEN\b|\bDECLARE\b|$)', body_content, flags=re.IGNORECASE | re.DOTALL)
        # Simplify regex to just capture everything until OPEN or next DECLARE or end, handling FOR UPDATE inside
        # Actually, simpler: DECLARE ... CURSOR FOR ... [FOR UPDATE]
        # We can try to capture line-based or until OPEN.
        # Let's retain the original logic but extend the matching group to consume "FOR UPDATE" if present at the end of declaration.

        # New strategy: Find DECLARE ... CURSOR FOR ...
        # Then consume content until 'OPEN cursor_name' or 'DEALLOCATE' or 'DECLARE'
        # But for now, let's just make the simple regex robust for trailing FOR UPDATE

        # Re-reading user issue: "FOR UPDATE" is on a new line.
        # The original regex: r'DECLARE\s+([a-zA-Z0-9_]+)\s+CURSOR\s+FOR\s+(.*)'
        # This matches until end of line? ".+" matches except newline. re.DOTALL not used in original snippet?
        # Original: flags=re.IGNORECASE (no DOTALL). So it stops at newline.
        # User Code:
        # declare access_cursor cursor
        # for select ...
        # for update
        # open access_cursor

        # The body_content passed here has newlines.
        cursor_matches = re.finditer(r'DECLARE\s+([a-zA-Z0-9_]+)\s+CURSOR\s+FOR\s+(.+?)(\s+FOR\s+UPDATE)?(?=\s+OPEN|\s+DECLARE|\s+SELECT|\s+SET|\s+FETCH|\s+BEGIN|$)', body_content, flags=re.IGNORECASE | re.DOTALL)

        for cm in cursor_matches:
            c_name = cm.group(1)
            c_def = cm.group(2).strip()
            for_upd = cm.group(3)
            if for_upd:
                 c_def += " " + for_upd.strip()
            declarations.append(f"{c_name} CURSOR FOR {c_def};")

        # Cleanup: Remove the matched text from body
        # We need to construct a regex that matches exactly what we found to remove it
        body_content = re.sub(r'DECLARE\s+[a-zA-Z0-9_]+\s+CURSOR\s+FOR\s+.+?(\s+FOR\s+UPDATE)?(?=\s+OPEN|\s+DECLARE|\s+SELECT|\s+SET|\s+FETCH|\s+BEGIN|$)', '', body_content, flags=re.IGNORECASE | re.DOTALL)

        # 8. Control Flow & Assignments

        # Misc Conversions (Before flow control to ensure valid statements)
        body_content = re.sub(r'fetch\s+([a-zA-Z0-9_]+)\s+into\s+(.*)', r'FETCH \1 INTO \2;', body_content, flags=re.IGNORECASE)

        def print_replacer(match):
             content = match.group(1).strip()
             # Split by comma respecting parens
             args = split_respecting_parens(content)

             if not args:
                  return "RAISE NOTICE '';"

             first_arg = args[0]
             rest_args = args[1:]

             # Check if first arg is a string literal
             if first_arg.startswith("'") and first_arg.endswith("'"):
                  # It's a format string
                  msg = first_arg
                  if rest_args:
                       return f"RAISE NOTICE {msg}, {', '.join(rest_args)};"
                  else:
                       return f"RAISE NOTICE {msg};"
             else:
                  # First arg is a variable or expression
                  # treat as RAISE NOTICE '%', arg
                  # Use %s for generic? PG RAISE NOTICE uses %
                  # If multiple args, we construct specific format string?
                  # Sybase PRINT "val" prints val.
                  # PG RAISE NOTICE '%', val.

                  # If multiple args: PRINT @a, @b -> RAISE NOTICE '%, %', @a, @b
                  format_str = ", ".join(["%"] * len(args))
                  return f"RAISE NOTICE '{format_str}', {', '.join(args)};"

        body_content = re.sub(r'print\s+(.+?)(?=;|\n|$)', print_replacer, body_content, flags=re.IGNORECASE)
        body_content = re.sub(r'open\s+([a-zA-Z0-9_]+)', r'OPEN \1;', body_content, flags=re.IGNORECASE)
        body_content = re.sub(r'close\s+([a-zA-Z0-9_]+)', r'CLOSE \1;', body_content, flags=re.IGNORECASE)
        body_content = re.sub(r'deallocate\s+cursor\s+([a-zA-Z0-9_]+)', r'DEALLOCATE \1;', body_content, flags=re.IGNORECASE)

        # BREAK -> EXIT;
        body_content = re.sub(r'\bBREAK\b', 'EXIT;', body_content, flags=re.IGNORECASE)

        # Handle @@sqlstatus (Cursor status)
        # @@sqlstatus = 0 -> FOUND
        # @@sqlstatus != 0 -> NOT FOUND (Commonly used for loop break)
        # 1. Replace "@@sqlstatus != 0" (and variants)
        # We look for IF (@@sqlstatus!=0) ...
        # Regex to capture optional whitespace: @@sqlstatus\s*!=\s*0
        # Replace with NOT FOUND
        body_content = re.sub(r'@@sqlstatus\s*!=\s*0', 'NOT FOUND', body_content, flags=re.IGNORECASE)
        # 2. Replace "@@sqlstatus = 0"
        body_content = re.sub(r'@@sqlstatus\s*=\s*0', 'FOUND', body_content, flags=re.IGNORECASE)
        # 3. Handle simple "IF (@@sqlstatus)" which implies non-zero? No, usually compared.
        # But some code might use > 0.
        body_content = re.sub(r'@@sqlstatus\s*>\s*0', 'NOT FOUND', body_content, flags=re.IGNORECASE)

        # 8.0 Pre-process END ELSE to avoid breaking IF-ELSE chains
        # T-SQL: IF ... BEGIN ... END ELSE ...
        # PG: IF ... THEN ... ELSE ... END IF;
        # We replace "END ELSE" with "ELSE" so the first block merges into the structure,
        # and the final END closes the whole IF.

        # FIX: Ensure semicolon before ELSE/ELSIF if missing
        # Sybase often allows: stmt \n ELSE
        # PG needs: stmt; \n ELSE
        # Regex updated to handle comments: stmt -- comment \n ELSE -> stmt; -- comment \n ELSE
        body_content = re.sub(r'([^;\s])([ \t]*(?:--[^\n]*|/\*.*?\*/[ \t]*)?)\n\s*(ELSE|ELSIF)\b', r'\1;\2\n\3', body_content, flags=re.IGNORECASE)

        body_content = re.sub(r'END\s*;?\s+ELSE', r'ELSE', body_content, flags=re.IGNORECASE | re.MULTILINE)

        # 8.05 Temp Table Handling (# -> tt_)
        # Global replacements of #name with tt_name
        # Note: Do this BEFORE select into transformer so we can detect tt_name
        body_content = re.sub(r'#([a-zA-Z0-9_]+)', r'tt_\1', body_content)

        # 8.1 Select Into (Handle BEFORE simple assignment to catch FROM clauses even multiline)
        # SELECT var = col FROM ... -> SELECT col INTO var FROM ...
        def select_into_transformer(match):
            content = match.group(1) # content between SELECT and FROM
            rest = match.group(2) # FROM ...

            # Check for generic SELECT ... INTO table syntax (converted from #table -> tt_table)
            # We convert to DROP TABLE IF EXISTS ...; CREATE TEMP TABLE ... AS SELECT ...
            # look for `INTO tt_([a-zA-Z0-9_]+)`

            into_match = re.search(r'\bINTO\s+(tt_[a-zA-Z0-9_]+)', content, flags=re.IGNORECASE)
            if into_match:
                table_name = into_match.group(1)
                # Remove `INTO table_name` from content
                # Be careful to remove exactly the match
                # Use split/join or sub
                new_content = re.sub(r'\bINTO\s+tt_[a-zA-Z0-9_]+\s*', '', content, flags=re.IGNORECASE)

                # Check if new_content ends with comma (failed parse?) or handled correctly?
                # Usually standard SQL: SELECT a, b INTO #t ...

                return f"DROP TABLE IF EXISTS {table_name}; CREATE TEMP TABLE {table_name} AS SELECT {new_content} {rest}"

            if '=' in content:
                parts = split_respecting_parens(content)
                vars_list = []
                cols_list = []
                for asm in parts:
                    if '=' in asm:
                        side_l, side_r = asm.split('=', 1)
                        vars_list.append(side_l.strip())
                        cols_list.append(side_r.strip())
                    else:
                        cols_list.append(asm)

                if vars_list:
                    return f"SELECT {', '.join(cols_list)} INTO {', '.join(vars_list)} {rest}"

            return match.group(0)

        # Use DOTALL to match newlines in the SELECT ... FROM
        body_content = re.sub(r'SELECT\s+(.+?)\s+(FROM\s+.*)', select_into_transformer, body_content, flags=re.IGNORECASE | re.DOTALL)

        # 8.2 Simple Assignment: SELECT var = val (no FROM)
        def simple_assignment(match):
            full_match = match.group(0)
            if 'FROM' in full_match.upper():
                return full_match

            content = match.group(1).strip() # content after SELECT

            if '=' not in content:
                return full_match

            parts = split_respecting_parens(content)
            assignments = []
            is_assignment = True
            for part in parts:
                if '=' in part:
                     side_l, side_r = part.split('=', 1)
                     assignments.append(f"{side_l.strip()} := {side_r.strip()}")
                else:
                     is_assignment = False

            if is_assignment and assignments:
                return "; ".join(assignments) + ";"
            return full_match

        # Match SELECT until newline or semicolon
        body_content = re.sub(r'SELECT\s+([^;\n]+)', simple_assignment, body_content, flags=re.IGNORECASE)


        # IF / WHILE
        # Mark BLOCK IFs to distinguish from Single Line IFs
        # Use DOTALL to match newlines in condition
        # Ensure we don't match too greedily if nested
        # But (.*?) is non-greedy.
        body_content = re.sub(r'IF\s+(.*?)\s+BEGIN', r'IF \1 THEN --BLOCK', body_content, flags=re.IGNORECASE | re.DOTALL)
        body_content = re.sub(r'WHILE\s+(.*?)\s+BEGIN', r'WHILE \1 LOOP', body_content, flags=re.IGNORECASE | re.DOTALL)
        body_content = re.sub(r'ELSE\s+BEGIN', r'ELSE --BLOCK', body_content, flags=re.IGNORECASE)

        # Single Line IF (no BEGIN)
        def single_line_if(match):
            cond = match.group(1)
            if 'THEN' in cond.upper():
                return match.group(0)
            return f"IF {cond} THEN --SINGLE" # Mark as single

        # Expanded lookahead to include EXIT(BREAK), CONTINUE, PRINT, etc.
        # Original: (?=\s+(?:UPDATE|INSERT|DELETE|SELECT|RETURN|RAISE|PERFORM|FETCH|OPEN|CLOSE|DEALLOCATE))
        # New tokens: EXIT, CONTINUE, PRINT (handled later as markers)
        # Also need to handle 'BREAK' which is now 'EXIT'
        # Added BEGIN to lookahead to prevent single line match if BEGIN follows (though IF..BEGIN regex above should catch it first)

        # Note: We already replaced BREAK -> EXIT above.

        body_content = re.sub(r'(?<!\bTABLE\s)IF\s+(.+?)(?=\s+(?:UPDATE|INSERT|DELETE|SELECT|RETURN|RAISE|PERFORM|FETCH|OPEN|CLOSE|DEALLOCATE|EXIT|CONTINUE|PRINT|BEGIN))', single_line_if, body_content, flags=re.IGNORECASE)
        body_content = re.sub(r'(?<!\bTABLE\s)IF\s+(.+?)\s*$', single_line_if, body_content, flags=re.IGNORECASE | re.MULTILINE)

        # Line processing to close blocks and single IFs
        lines = body_content.split('\n')
        new_lines = []
        stack = [] # 'IF_BLOCK', 'LOOP'
        pending_single_if = False
        in_dml = False # Track if we are inside a DML statement


        new_lines = []
        stack = []
        pending_single_if = False
        in_dml = False

        i = 0
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()

            # Remove our markers
            is_block_if = '--BLOCK' in line
            is_single_if = '--SINGLE' in line

            clean_line = line.replace('--BLOCK', '').replace('--SINGLE', '')

            # Detect rowcount usage
            has_rowcount = '@@rowcount' in clean_line.lower() or 'sql%rowcount' in clean_line.lower()

            # Stopper Logic (Close previous DML)
            stopper_match = re.search(r'^\b(IF|WHILE|RETURN|BEGIN|END|RAISE|PRINT|FETCH|OPEN|CLOSE|DEALLOCATE|INSERT|UPDATE|DELETE)\b', stripped, flags=re.IGNORECASE)
            if in_dml and stopper_match:
                 if new_lines:
                      prev = new_lines[-1]
                      if not prev.strip().endswith(';'):
                           new_lines[-1] = prev + ";"
                 if has_rowcount:
                      new_lines.append("GET DIAGNOSTICS _rowcount = ROW_COUNT;")
                 in_dml = False

            # Starter Logic (Open new DML)
            dml_start_match = re.search(r'^\b(UPDATE|INSERT|DELETE|SELECT)\b', stripped, flags=re.IGNORECASE)
            if dml_start_match:
                 in_dml = True

            # Simple Closure Logic
            if in_dml and stripped.endswith(';'):
                 if has_rowcount:
                      clean_line += " GET DIAGNOSTICS _rowcount = ROW_COUNT;"
                 in_dml = False

            if is_block_if:
                stack.append('IF_BLOCK')
            elif re.search(r'WHILE\s+.*\s+LOOP', line, flags=re.IGNORECASE):
                stack.append('LOOP')

            if is_single_if:
                if clean_line.rstrip().endswith('THEN'):
                    pending_single_if = True
                    new_lines.append(clean_line)
                else:
                    # Single line IF with statement on same line: IF ... THEN stmt
                    # Check next line for ELSE
                    next_is_else = False
                    if i + 1 < len(lines):
                         next_line = lines[i+1].strip()
                         # Check for ELSE or ELSE --BLOCK
                         if re.match(r'^ELSE\b', next_line, flags=re.IGNORECASE):
                              next_is_else = True

                    if next_is_else:
                         # Treat as if we are entering an ELSE block for a single IF
                         # We do NOT append END IF;
                         # Push a state to stack indicating we are in an IF that needs closing after ELSE
                         stack.append('IF_SINGLE_ELSE')
                         new_lines.append(clean_line)
                    else:
                         new_lines.append(clean_line + " END IF;")
                i += 1
                continue

            if pending_single_if and stripped:
                # We just processed the statement for a pending single IF
                # Check next line for ELSE
                next_is_else = False
                if i + 1 < len(lines):
                        next_line = lines[i+1].strip()
                        if re.match(r'^ELSE\b', next_line, flags=re.IGNORECASE):
                            next_is_else = True

                if next_is_else:
                     stack.append('IF_SINGLE_ELSE')
                     new_lines.append(clean_line)
                else:
                     new_lines.append(clean_line + " END IF;")

                pending_single_if = False
                i += 1
                continue

            # Standard END handling
            if re.match(r'^END\s*;?$', stripped, flags=re.IGNORECASE):
                if stack:
                    block = stack.pop()
                    if block == 'IF_BLOCK':
                        new_lines.append("END IF;")
                    elif block == 'LOOP':
                        new_lines.append("END LOOP;")
                    elif block == 'IF_SINGLE_ELSE':
                         # If we hit END while in IF_SINGLE_ELSE, it means the ELSE block finished?
                         # Usually ELSE for single IF doesn't have END unless it was a block ELSE.
                         # But wait, if it was 'ELSE --BLOCK', it would have pushed IF_BLOCK?
                         # No, 'ELSE --BLOCK' is just a line.
                         new_lines.append("END IF;") # Close the ELSE's IF
                    else:
                        new_lines.append(clean_line)
                else:
                    new_lines.append("END;")
            else:
                 # Check if this line is ELSE.
                 # If we match ELSE, and we are in IF_SINGLE_ELSE, we just print ELSE.
                 # The 'IF_SINGLE_ELSE' state remains until the statement AFTER else is done?
                 # Wait.
                 # If we are in IF_SINGLE_ELSE state, it means we had `IF ... stmt` (no END IF) `ELSE`.
                 # So we are now processing `ELSE`.
                 # If `ELSE` is `ELSE --BLOCK` (converted from `ELSE BEGIN`), then `is_block_if` would be true?
                 # No, `is_block_if` checks `--BLOCK`. My regex replaced `ELSE BEGIN` with `ELSE`.
                 # I should probably replace `ELSE BEGIN` with `ELSE --BLOCK` to track it.

                 new_lines.append(clean_line)

                 # If we are in 'IF_SINGLE_ELSE' state, and we just emitted a statement that is NOT 'ELSE',
                 # that implies the single-line ELSE body is finished.
                 # UNLESS the statement was 'ELSE' itself.

                 is_else_line = re.match(r'^ELSE\b', stripped, flags=re.IGNORECASE)
                 if stack and stack[-1] == 'IF_SINGLE_ELSE' and not is_else_line:
                      # We just processed the body of the else. Close it.
                      stack.pop()
                      new_lines[-1] += " END IF;"

            i += 1

        # Fallback closure
        if pending_single_if:
             new_lines.append("END IF;")


        # If DML is still open at end of body (e.g. because END was stripped), close it
        if in_dml:
             if new_lines and not new_lines[-1].strip().endswith(';'):
                  new_lines[-1] += ";"
             if has_rowcount:
                  new_lines.append("GET DIAGNOSTICS _rowcount = ROW_COUNT;")

        body_content = '\n'.join(new_lines)

        # Fix ENDs
        lines = body_content.split('\n')
        new_lines = []
        stack = []

        for line in lines:
            stripped = line.strip()
            if re.search(r'IF\s+.*\s+THEN', line, flags=re.IGNORECASE):
                stack.append('IF')
            elif re.search(r'WHILE\s+.*\s+LOOP', line, flags=re.IGNORECASE):
                stack.append('LOOP')

            # Check for END
            if re.match(r'^END\s*;?$', stripped, flags=re.IGNORECASE):
                if stack:
                    block = stack.pop()
                    if block == 'IF':
                        new_lines.append(line.upper().replace('END', 'END IF;'))
                    elif block == 'LOOP':
                        new_lines.append(line.upper().replace('END', 'END LOOP;'))
                    else:
                        new_lines.append('END;')
                else:
                    new_lines.append('END;')
            else:
                 new_lines.append(line)

        body_content = '\n'.join(new_lines)

        body_content = re.sub(r'RETURN\s+\d+', 'RETURN', body_content, flags=re.IGNORECASE)

        # Check for Output parameters to adjust RETURNs and RETURNS clause
        output_params = []
        if header_match:
             # Re-parse params_str to find output params
             # This is a bit rough, assuming comma separation on top level
             # Better to trust our params parsing if we had it structured.
             # We can regex search for 'OUTPUT' or 'OUT' in params_str
             # params_str: "@id int output, @val varchar(10)"
             # We need to count them.

             # Split by comma respecting parens (not strictly needed for just counting OUTPUT keywords, mostly)
             # But let's look at the generated pg_params_str.
             # It has INOUT for output params.
             # "INOUT geraet_id BIGINT"

             output_params = re.findall(r'\b(INOUT|OUT)\b', pg_params_str, flags=re.IGNORECASE)

        def cleaner_return(match):
             expr = match.group(1).strip()
             terminator = match.group(2)
             if not expr:
                  return f"RETURN;{terminator}"
             return f"RETURN; /* {expr} */{terminator}"

        # Match RETURN <expr> until semicolon or newline or end of string OR 'END' keyword
        # Apply to ALL returns because Sybase status codes are not used in PG functions
        body_content = re.sub(r'RETURN\b\s*(.*?)(\;|\n|\bEND\b|$)', cleaner_return, body_content, flags=re.IGNORECASE)

        if output_params:
             # If we have output parameters, replacement is already done above.
             if len(output_params) > 1:
                  returns_clause = "RETURNS RECORD"
             else:
                  # If we have 1 OUT param, PG usually implies RETURNS <type> of that param,
                  # OR if defined as INOUT in signature, we can use RETURNS void or matches?
                  # Actually, if parameters are DEFINED as INOUT, function returns VOID? No.
                  # In PG, if you use OUT/INOUT params, you usually do RETURNS RECORD (if multiple)
                  # or RETURNS <type> (if single OUT), OR RETURNS SETOF ...
                  # But wait, PG docs: "If there are OUT or INOUT parameters, the RETURNS clause
                  # can be omitted (equivalent to RETURNS record) or can specify the result type."
                  # "If RETURNS void is used, output parameters are not allowed." -> ERROR user saw!

                  # So we must NOT return VOID.
                  # We can return "RECORD" generally, or rely on PG inferring it if we omit it?
                  # We must specify something if we used 'RETURNS void' before.

                  # Safest: "RETURNS RECORD" if multiple.
                  # If single, we should ideally match the type, but "RETURNS RECORD" works too?
                  # Actually, if we use INOUT in declaration, "RETURNS <type>" is expected if single?
                  # Or "RETURNS SETOF <type>"?

                  # User error: "function result type must be bigint because of OUT parameters"
                  # This implies PG expects the type of the single OUT param.
                  pass
                  # We need to extract the type of the single OUT param.
                  # pg_params_str: "techauftrag_id BIGINT, INOUT geraet_id BIGINT =0"
                  # We can try to extract the type.

        # Determine RETURNS clause
        returns_clause = "RETURNS void"
        if output_params:
             if len(output_params) > 1:
                  returns_clause = "RETURNS RECORD"
             else:
                  # Extract type of the single INOUT param
                  # Look for "INOUT param_name TYPE"
                  single_out = re.search(r'\b(?:INOUT|OUT)\s+[a-zA-Z0-9_]+\s+([a-zA-Z0-9_]+(?:\(.*\))?)', pg_params_str, flags=re.IGNORECASE)
                  if single_out:
                       returns_clause = f"RETURNS {single_out.group(1)}"
                  else:
                       returns_clause = "RETURNS RECORD" # Fallback

        # Other types mapping in body
        # Custom type substitutions first
        body_content = self._apply_data_type_substitutions(body_content)
        body_content = self._apply_udt_to_base_type_substitutions(body_content, settings)
        for sybase_type, pg_type in types_mapping.items():
            body_content = re.sub(rf'\b{re.escape(sybase_type)}\b', pg_type, body_content, flags=re.IGNORECASE)



        # 9. Assembly
        final_code = f"CREATE OR REPLACE FUNCTION {func_schema}.{func_name}({pg_params_str})\n"
        final_code += f"{returns_clause} AS $$\n"

        if declarations:
            final_code += "DECLARE\n"
            for decl in declarations:
                final_code += f"    {decl}\n"

        final_code += "BEGIN\n"
        final_code += body_content
        final_code += "\nEND;\n$$ LANGUAGE plpgsql;"

        return final_code

    def fetch_sequences(self, table_schema: str, table_name: str):
        pass

    def get_sequence_details(self, sequence_owner, sequence_name):
        # Placeholder for fetching sequence details
        return {}

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
        self.connection.jconn.setAutoCommit(False)

    def commit_transaction(self):
        self.connection.commit()
        self.connection.jconn.setAutoCommit(True)

    def rollback_transaction(self):
        self.connection.rollback()

    def handle_error(self, e, description=None):
        self.config_parser.print_log_message('ERROR', f"An error in {self.__class__.__name__} ({description}): {e}")
        self.config_parser.print_log_message('ERROR', traceback.format_exc())
        if self.on_error_action == 'stop':
            self.config_parser.print_log_message('ERROR', "Stopping due to error.")
            exit(1)
        else:
            pass

    def get_rows_count(self, table_schema: str, table_name: str, migration_limitation: str = None):
        query = f"""SELECT COUNT(*) FROM {table_schema}.{table_name} """
        if migration_limitation:
            query += f" WHERE {migration_limitation} "
        self.config_parser.print_log_message('DEBUG3',f"get_rows_count query: {query}")
        cursor = self.connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    ## function to analyze primary key distribution
    ## looks like python handels cursors differently than PostgreSQL from FDW
    ## so currently this function is not used
    ##
    # def analyze_pk_distribution_batches(self, values):
    #     migrator_tables = values['migrator_tables']
    #     schema_name = values['source_schema']
    #     table_name = values['source_table']
    #     primary_key_columns = values['primary_key_columns']
    #     primary_key_columns_count = values['primary_key_columns_count']
    #     primary_key_columns_types = values['primary_key_columns_types']
    #     worker_id = values['worker_id']
    #     analyze_batch_size = self.config_parser.get_batch_size()

    #     if primary_key_columns_count == 1 and primary_key_columns_types in ('BIGINT', 'INTEGER', 'NUMERIC', 'REAL', 'FLOAT', 'DOUBLE PRECISION', 'DECIMAL', 'SMALLINT', 'TINYINT'):
    #         # primary key is one column of numeric type - analysis with min/max values is much quicker
    #         self.config_parser.print_log_message('DEBUG', f"Worker: {worker_id}: PK analysis: {primary_key_columns} ({primary_key_columns_types}): min/max analysis")

    #         current_batch_percent = 20

    #         sybase_cursor = self.connection.cursor()
    #         temp_table = f"temp_id_ranges_{str(worker_id).replace('-', '_')}"
    #         migrator_tables.protocol_connection.execute_query(f"""DROP TABLE IF EXISTS "{temp_table}" """)
    #         migrator_tables.protocol_connection.execute_query(f"""CREATE TEMP TABLE IF NOT EXISTS "{temp_table}" (batch_start BIGINT, batch_end BIGINT, row_count BIGINT)""")

    #         pk_range_table = self.config_parser.get_protocol_name_pk_ranges()
    #         sybase_cursor.execute(f"SELECT MIN({primary_key_columns}) FROM {schema_name}.{table_name}")
    #         min_id = sybase_cursor.fetchone()[0]

    #         sybase_cursor.execute(f"SELECT MAX({primary_key_columns}) FROM {schema_name}.{table_name}")
    #         max_id = sybase_cursor.fetchone()[0]

    #         self.config_parser.print_log_message('DEBUG', f"Worker: {worker_id}: PK analysis: {primary_key_columns}: min_id: {min_id}, max_id: {max_id}")

    #         total_range = int(max_id) - int(min_id)
    #         current_start = min_id
    #         loop_counter = 0
    #         previous_row_count = 0
    #         same_previous_row_count = 0
    #         current_decrease_ratio = 2

    #         while current_start <= max_id:
    #             current_batch_size = int(total_range / 100 * current_batch_percent)
    #             if current_batch_size < analyze_batch_size:
    #                 current_batch_size = analyze_batch_size
    #                 current_decrease_ratio = 2
    #                 self.config_parser.print_log_message('DEBUG', f"Worker: {worker_id}: PK analysis: {loop_counter}: resetting current_decrease_ratio to {current_decrease_ratio}")

    #             current_end = current_start + current_batch_size

    #             self.config_parser.print_log_message('DEBUG', f"Worker: {worker_id}: PK analysis: {loop_counter}: Loop counter: {loop_counter}, current_batch_percent: {round(current_batch_percent, 8)}, current_batch_size: {current_batch_size}, current_start: {current_start} (min: {min_id}), current_end: {current_end} (max: {max_id}), perc: {round(current_start / max_id * 100, 4)}")

    #             if current_end > max_id:
    #                 current_end = max_id

    #             loop_counter += 1
    #             sybase_cursor.execute(f"""SELECT COUNT(*) FROM {schema_name}.{table_name} WHERE {primary_key_columns} BETWEEN %s AND %s""", (current_start, current_end))
    #             testing_row_count = sybase_cursor.fetchone()[0]

    #             self.config_parser.print_log_message('DEBUG', f"Worker: {worker_id}: PK analysis: {loop_counter}: Testing row count: {testing_row_count}")

    #             if testing_row_count == previous_row_count:
    #                 same_previous_row_count += 1
    #                 if same_previous_row_count >= 2:
    #                     current_decrease_ratio *= 2
    #                     self.config_parser.print_log_message('DEBUG', f"Worker: {worker_id}: PK analysis: {loop_counter}: changing current_decrease_ratio to {current_decrease_ratio}")
    #                     same_previous_row_count = 0
    #             else:
    #                 same_previous_row_count = 0

    #             previous_row_count = testing_row_count

    #             if testing_row_count > analyze_batch_size:
    #                 current_batch_percent /= current_decrease_ratio
    #                 self.config_parser.print_log_message('DEBUG', f"Worker: {worker_id}: PK analysis: {loop_counter}: Decreasing analyze_batch_percent to {round(current_batch_percent, 8)}")
    #                 continue

    #             if testing_row_count == 0:
    #                 current_batch_percent *= 1.5
    #                 self.config_parser.print_log_message('DEBUG', f"Worker: {worker_id}: PK analysis: {loop_counter}: Increasing analyze_batch_percent to {round(current_batch_percent, 8)} without restarting loop")

    #             sybase_cursor.execute(f"""SELECT
    #                         %s::bigint AS batch_start,
    #                         %s::bigint AS batch_end,
    #                         COUNT(*) AS row_count
    #                         FROM {schema_name}.{table_name}
    #                         WHERE {primary_key_columns  } BETWEEN %s AND %s""",
    #                         (current_start, current_end, current_start, current_end))

    #             result = sybase_cursor.fetchone()
    #             if result:
    #                 insert_batch_start = result[0]
    #                 insert_batch_end = result[1]
    #                 insert_row_count = result[2]
    #                 self.config_parser.print_log_message('DEBUG', f"Worker: {worker_id}: PK analysis: {loop_counter}: Insert batch into temp table: start: {insert_batch_start}, end: {insert_batch_end}, row count: {insert_row_count}")
    #                 migrator_tables.protocol_connection.execute_query(f"""INSERT INTO "{temp_table}" (batch_start, batch_end, row_count) VALUES (%s, %s, %s)""", (insert_batch_start, insert_batch_end, insert_row_count))

    #             current_start = current_end + 1
    #             self.config_parser.print_log_message('DEBUG', f"Worker: {worker_id}: PK analysis: {loop_counter}: loop end - new current_start: {current_start}")

    #         self.config_parser.print_log_message('DEBUG', f"Worker: {worker_id}: PK analysis: {loop_counter}: second loop")

    #         current_start = min_id
    #         while current_start <= max_id:
    #             migrator_tables.protocol_connection.execute_query("""
    #                 SELECT
    #                     min(batch_start) as batch_start,
    #                     max(batch_end) as batch_end,
    #                     max(cumulative_row_count) as row_count
    #                 FROM (
    #                     SELECT
    #                         batch_start,
    #                         batch_end,
    #                         sum(row_count) over (order by batch_start) as cumulative_row_count
    #                     FROM "{temp_table}"
    #                     WHERE batch_start >= %s::bigint
    #                     ORDER BY batch_start
    #                 ) subquery
    #                 WHERE cumulative_row_count <= %s::bigint
    #             """, (current_start, analyze_batch_size))
    #             result = migrator_tables.fetchone()
    #             if result:
    #                 insert_batch_start = result[0]
    #                 insert_batch_end = result[1]
    #                 insert_row_count = result[2]
    #                 self.config_parser.print_log_message('DEBUG', (f"Worker: {worker_id}: PK analysis: {loop_counter}: Insert batch into protocol table: start: {insert_batch_start}, end: {insert_batch_end}, row count: {insert_row_count}")

    #             values = {}
    #             values['source_schema'] = schema_name
    #             values['source_table'] = table_name
    #             values['source_table_id'] = 0
    #             values['worker_id'] = worker_id
    #             values['pk_columns'] = primary_key_columns
    #             values['batch_start'] = insert_batch_start
    #             values['batch_end'] = insert_batch_end
    #             values['row_count'] = insert_row_count
    #             migrator_tables.insert_pk_ranges(values)
    #             current_start = insert_batch_end

    #         migrator_tables.protocol_connection.execute_query(f"""DROP TABLE IF EXISTS "{temp_table}" """)
    #         self.connection.commit()
    #         self.config_parser.print_log_message('INFO', f"Worker: {worker_id}: PK analysis: {loop_counter}: Finished analyzing PK distribution for table {table_name}.")
    #         ## end of function


        # unfortunately, the following code is not working as expected - Sybase does not support BETWEEN for multiple columns as PostgreSQL does
        # this solution worked for foreign data wrapper but not for native connection
        # if PK has more than one column, we shall use cursor
        # else:

            # # we need to do slower analysis with selecting all values of primary key
            # # necessary for composite keys or non-numeric keys
            # self.config_parser.print_log_message('DEBUG', f"Worker: {worker_id}: PK analysis: {primary_key_columns} ({primary_key_columns_types}): analyzing all PK values")

            # primary_key_columns_list = primary_key_columns.split(',')
            # primary_key_columns_types_list = primary_key_columns_types.split(',')
            # temp_table_structure = ', '.join([f"{column.strip()} {column_type.strip()}" for column, column_type in zip(primary_key_columns_list, primary_key_columns_types_list)])
            # self.config_parser.print_log_message('DEBUG', f"Worker: {worker_id}: PK analysis: {primary_key_columns}: temp table structure: {temp_table_structure}")

            # # step 1: create temp table with all PK values
            # sybase_cursor = self.connection.cursor()
            # temp_table = f"temp_id_ranges_{str(worker_id).replace('-', '_')}"
            # migrator_tables.protocol_connection.execute_query(f"""DROP TABLE IF EXISTS "{temp_table}" """)
            # migrator_tables.protocol_connection.execute_query(f"""CREATE TEMP TABLE {temp_table} ({temp_table_structure}) ON COMMIT PRESERVE ROWS""")

            # sybase_cursor = self.connection.cursor()
            # sybase_cursor.execute(f"""SELECT {primary_key_columns.replace("'","").replace('"','')} FROM {schema_name}.{table_name} ORDER BY {primary_key_columns.replace("'","").replace('"','')}""")
            # rows = sybase_cursor.fetchall()
            # pk_temp_table_row_count = len(rows)
            # for row in rows:
            #     # self.config_parser.print_log_message('DEBUG', f"Worker: {worker_id}: PK analysis: {primary_key_columns}: row: {row}")
            #     insert_values = ', '.join([f"'{value}'" if isinstance(value, str) else str(value) for value in row])
            #     migrator_tables.protocol_connection.execute_query(f"""INSERT INTO "{temp_table}" ({primary_key_columns}) VALUES ({insert_values})""")
            # self.config_parser.print_log_message('DEBUG', f"Worker: {worker_id}: PK analysis: {primary_key_columns}: Inserted {pk_temp_table_row_count} rows into temp table {temp_table}")

            # # step 2: analyze distribution of PK values
            # pk_temp_table_offset = 0
            # batch_loop = 1
            # count_inserted_total = 0

            # migrator_tables_cursor = migrator_tables.protocol_connection.connection.cursor()
            # while True:
            #     # Read min values
            #     migrator_tables_cursor.execute(f"""SELECT {primary_key_columns.replace("'","").replace('"','')} FROM {temp_table}
            #         ORDER BY {primary_key_columns} LIMIT 1 OFFSET {pk_temp_table_offset}""")
            #     rec_min_values = migrator_tables_cursor.fetchone()
            #     if not rec_min_values:
            #         break

            #     # Read max values
            #     pk_temp_table_offset_max = pk_temp_table_offset + analyze_batch_size - 1
            #     if pk_temp_table_offset_max > pk_temp_table_row_count:
            #         pk_temp_table_offset_max = pk_temp_table_row_count - 1

            #     migrator_tables_cursor.execute(f"""SELECT {primary_key_columns} FROM {temp_table}
            #         ORDER BY {primary_key_columns} LIMIT 1 OFFSET {pk_temp_table_offset_max}""")
            #     rec_max_values = migrator_tables_cursor.fetchone()
            #     if not rec_max_values:
            #         break

            #     self.config_parser.print_log_message('DEBUG', f"Worker: {worker_id}: PK analysis: {batch_loop}: Loop counter: {batch_loop}, PK values: {rec_min_values} / {rec_max_values}")

            #     values = {}
            #     values['source_schema'] = schema_name
            #     values['source_table'] = table_name
            #     values['source_table_id'] = 0
            #     values['worker_id'] = worker_id
            #     values['pk_columns'] = primary_key_columns
            #     values['batch_start'] = str(rec_min_values)
            #     values['batch_end'] = str(rec_max_values)
            #     values['row_count'] = analyze_batch_size
            #     migrator_tables.insert_pk_ranges(values)

            #     pk_temp_table_offset += analyze_batch_size
            #     batch_loop += 1



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
            # target_schema = self.config_parser.convert_names_case(settings['target_schema'])
            target_schema = settings['target_schema']  ## target schema is used as it is defined in config, not converted to upper/lower case
            target_table = self.config_parser.convert_names_case(settings['target_table'])
            target_columns = settings['target_columns']
            batch_size = settings['batch_size']
            migrator_tables = settings['migrator_tables']
            source_table_rows = self.get_rows_count(source_schema, source_table)
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

                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Source table {source_table}: {source_table_rows} rows / Target table {target_table}: {target_table_rows} rows - starting data migration.")

                    select_columns_list = []
                    orderby_columns_list = []
                    insert_columns_list = []
                    for order_num, col in source_columns.items():
                        self.config_parser.print_log_message('DEBUG2',
                                                            f"Worker {worker_id}: Table {source_schema}.{source_table}: Processing column {col['column_name']} ({order_num}) with data type {col['data_type']}")

                        # if col['data_type'].lower() == 'datetime':
                        #     select_columns_list.append(f"TO_CHAR({col['column_name']}, '%Y-%m-%d %H:%M:%S') as {col['column_name']}")
                        #     select_columns_list.append(f"ST_asText(`{col['column_name']}`) as `{col['column_name']}`")
                        # elif col['data_type'].lower() == 'set':
                        #     select_columns_list.append(f"cast(`{col['column_name']}` as char(4000)) as `{col['column_name']}`")
                        # else:
                        select_columns_list.append(f"{col['column_name']}")

                        insert_columns_list.append(f'''"{self.config_parser.convert_names_case(col['column_name'])}"''')

                        # fixing error - [42000] [FreeTDS][SQL Server]The TEXT, IMAGE and UNITEXT datatypes cannot be used in an ORDER BY clause or in the select list of a query in a UNION statement.\n (420) (SQLExecDirectW)
                        if col['data_type'].lower() in ['text', 'image', 'unitext']:
                            self.config_parser.print_log_message('DEBUG2', f"Worker {worker_id}: Table {source_schema}.{source_table}: Column {col['column_name']} ({order_num}) with data type {col['data_type']} cannot be used in ORDER BY clause or in the select list of a query in a UNION statement.")
                            continue
                        orderby_columns_list.append(f'''{col['column_name']}''')

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

                    ## Sybase ASE does not support LIMIT with OFFSET, in older versions,
                    # therefore we cannot use chunks and cannot continue after a crash
                    # Partially migrated tables must be dropped and restarted
                    query = f"SELECT {select_columns} FROM {source_schema}.{source_table}"
                    if migration_limitation:
                        query += f" WHERE {migration_limitation}"
                    primary_key_columns = migrator_tables.select_primary_key(source_schema, source_table)
                    self.config_parser.print_log_message('DEBUG2', f"Worker {worker_id}: Primary key columns for {source_schema}.{source_table}: {primary_key_columns}")
                    if primary_key_columns:
                        orderby_columns = primary_key_columns
                    order_by_clause = f""" ORDER BY {orderby_columns}"""
                    query += order_by_clause
                    # query += order_by_clause + f" LIMIT {chunk_size}"

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
                        self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Fetched {len(records)} rows (batch {batch_number}) from source table '{source_table}' using cursor")

                        # Convert records to a list of dictionaries
                        transforming_start_time = time.time()
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
                # we currently do not implement chunking for Sybase ASE
                # if source_table_rows <= target_table_rows or chunk_number >= total_chunks:
                if source_table_rows <= target_table_rows:
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

    def convert_trigger_v2(self, settings):
        """
        Parser-based conversion for triggers.
        Returns the BODY of the Postgres trigger function.
        Includes ported V1 logic: IF UPDATE, inserted/deleted -> NEW/OLD, etc.
        """
        trigger_code = settings['trigger_sql']

        # --- Pre-processing (Ported from V1) ---

        # 0. Encapsulate comments
        trigger_code = re.sub(r'--([^\n]*)', r'/*\1*/', trigger_code)

        # 1. Remove GO
        trigger_code = re.sub(r'\bGO\b', '', trigger_code, flags=re.IGNORECASE)

        # 2. Extract Body (After AS) - Critical for sqlglot to just parse statements, not DDL
        # Pattern: CREATE TRIGGER ... AS [BEGIN] ... [END]
        # or ... FOR INSERT AS ...
        # match case insensitive AS on word boundary
        as_match = re.search(r'\bAS\b', trigger_code, flags=re.IGNORECASE)
        body_content = trigger_code
        if as_match:
             body_content = trigger_code[as_match.end():].strip()

        # 3. Global Replacements specific to Triggers

        # inserted/deleted -> NEW/OLD (Naive but often sufficient for T-SQL -> PG Row Triggers)
        # Note: In T-SQL, inserted/deleted are tables. In PG Row Triggers, NEW/OLD are records.
        # If the code does `SELECT ... FROM inserted`, that fails in PG (unless specific workaround).
        # But commonly: `IF (SELECT count(*) FROM inserted) > 0` -> logic checks.
        # OR `SELECT @v = col FROM inserted`.
        # V1 logic replaced them globally. We will do same but watch out for `FROM NEW`.

        body_content = re.sub(r'\binserted\b', 'NEW', body_content, flags=re.IGNORECASE)
        body_content = re.sub(r'\bdeleted\b', 'OLD', body_content, flags=re.IGNORECASE)

        # Cleanup: Remove `FROM NEW`/`FROM OLD` if they appear in `SELECT ... FROM NEW` pattern?
        # Because `SELECT ... FROM NEW` is valid T-SQL (table) but invalid PG (record).
        # PG: `SELECT NEW.col` works directly.
        # We'll try to handle this during expression processing or strict regex.
        # Regex from V1: remove FROM clause if it only contains NEW/OLD
        # We do this PRE-parsing so sqlglot parses `SELECT NEW.col` (valid-ish) instead of `SELECT ... FROM NEW` (invalidish for row)

        # Regex to strip `FROM NEW` / `FROM OLD`
        # Simple heuristic: `FROM\s+(?:NEW|OLD)\b` -> empty string
        body_content = re.sub(r'\bFROM\s+(?:NEW|OLD)\b', '', body_content, flags=re.IGNORECASE)

        # 4. IF UPDATE(col) -> IF NEW.col IS DISTINCT FROM OLD.col
        # This is strictly a T-SQL function. sqlglot might parse it as Func 'UPDATE'.
        # We can transform it in AST, but regex is reliable for this specific pattern per V1.
        def if_update_replacer(match):
            col = match.group(1)
            return f"NEW.{col} IS DISTINCT FROM OLD.{col}" # PG idiomatic check

        body_content = re.sub(r'\bUPDATE\(([a-zA-Z0-9_]+)\)', if_update_replacer, body_content, flags=re.IGNORECASE)


        converted_statements = []
        try:
            expressions = sqlglot.parse(body_content, read='tsql')

            for expression in expressions:
                try:
                    pg_sql = ""
                    skip_semicolon = False

                    # 1. ROLLBACK TRIGGER / TRANSACTION -> RAISE EXCEPTION
                    if isinstance(expression, exp.Command) and 'ROLLBACK' in expression.this.upper():
                         # Check if it resembles extraction logic from V1
                         # match string
                         cmd_str = expression.this.upper()
                         if 'TRIGGER' in cmd_str or 'TRAN' in cmd_str:
                              # Extract message if possible? sqlglot Command content is just the string usually.
                              # V1 extracted message.
                              # Let's just output generic exception or try to preserve string.
                              # Original: ROLLBACK TRIGGER WITH RAISERROR 12345 'Message'
                              # sqlglot might parse 'WITH RAISERROR...' as options?
                              # Let's rely on regex on the raw expression SQL if we want message.
                              raw = expression.sql()
                              msg_match = re.search(r"'([^']+)'", raw)
                              msg = msg_match.group(1) if msg_match else "Trigger Rollback"
                              pg_sql = f"RAISE EXCEPTION '{msg}'"

                    elif isinstance(expression, exp.Rollback):
                         pg_sql = "RAISE EXCEPTION 'Transaction Rollback Not Supported in Trigger'"

                    # 2. PRINT -> RAISE NOTICE (Standard)
                    elif isinstance(expression, exp.Print):
                        msg_val = expression.this.sql(dialect='postgres')
                        pg_sql = f"RAISE NOTICE {msg_val}"

                    # 3. SELECT Assignments (reuse logic)
                    elif isinstance(expression, exp.Select):
                        generated = expression.sql(dialect='postgres')

                        # Check "SELECT @v = 1" pattern (sqlglot might emit `SELECT v = 1` or `v := 1`)
                        match_assign = re.match(r'SELECT\s+([a-zA-Z0-9_]+)\s*=\s*(.*)', generated, re.IGNORECASE)
                        if match_assign:
                            var_name = match_assign.group(1).replace('@', '')
                            val_expr = match_assign.group(2)
                            pg_sql = f"{var_name} := {val_expr}"
                        else:
                            pg_sql = generated

                    # 4. IF / WHILE (Control Flow)
                    elif isinstance(expression, (exp.If, exp.While)):
                         pg_sql = expression.sql(dialect='postgres')
                         if 'END IF' not in pg_sql.upper() and isinstance(expression, exp.If):
                              pg_sql += " END IF"

                    else:
                        pg_sql = expression.sql(dialect='postgres')

                    # --- Post-processing per statement ---

                    # Fix Variables: Remove '@' (sqlglot might have left some)
                    pg_sql = re.sub(r'(?<!\w)@([a-zA-Z0-9_]+)', r'\1', pg_sql)

                    # Fix `NEW`/`OLD` usage confusion if `sqlglot` didn't like them?
                    # `NEW.col` should work fine.

                    # Semicolon
                    if not skip_semicolon and not pg_sql.strip().endswith(';'):
                        pg_sql += ';'

                    converted_statements.append(pg_sql)

                except Exception as e:
                    self.config_parser.print_log_message('WARNING', f"Failed to transpile trigger statement: {expression}. Error: {e}")
                    converted_statements.append(f"-- FAILED CONVERSION: {expression.sql()}\n-- Error: {e}")

        except Exception as e:
             self.config_parser.print_log_message('ERROR', f"Trigger parsing failed: {e}")
             return f"/* TRIGGER PARSING FAILED: {e} */\n" + body_content

        # Join statements
        final_body = "\n".join(converted_statements)

        return final_body

    def convert_trigger(self, settings):
        # return self.convert_trigger_v1(settings)
        return self.convert_trigger_v2(settings)

    def convert_trigger_v1(self, settings):
        trigger_name = self.config_parser.convert_names_case(settings['trigger_name'])
        trigger_code = settings['trigger_sql']

        # 0. Encapsulate comments
        # Convert -- comment to /* comment */ to prevent breaking code
        trigger_code = re.sub(r'--([^\n]*)', r'/*\1*/', trigger_code)

        target_schema = settings['target_schema']
        target_table = self.config_parser.convert_names_case(settings['target_table'])
        target_db_type = settings['target_db_type']

        # Helper
        def split_respecting_parens(text):
            parts = []
            current = []
            depth = 0
            for char in text:
                if char == '(':
                    depth += 1
                elif char == ')':
                    depth -= 1

                if char == ',' and depth == 0:
                    parts.append("".join(current).strip())
                    current = []
                else:
                    current.append(char)
            if current:
                parts.append("".join(current).strip())
            return parts

        # 1. Basic Cleanup
        converted_code = re.sub(r'\bGO\b', '', trigger_code, flags=re.IGNORECASE)

        # 2. Extract Body (After AS)
        # Pattern: CREATE TRIGGER ... AS [BEGIN] ... [END]
        # or ... FOR INSERT AS ...
        as_match = re.search(r'\bAS\b', converted_code, flags=re.IGNORECASE)
        if as_match:
            body_content = converted_code[as_match.end():].strip()
        else:
            body_content = converted_code # Fallback?

        # Remove outer BEGIN/END if present
        if re.match(r'^BEGIN\b', body_content, flags=re.IGNORECASE):
            body_content = re.sub(r'^BEGIN', '', body_content, count=1, flags=re.IGNORECASE).strip()
            body_content = re.sub(r'END\s*$', '', body_content, flags=re.IGNORECASE).strip()

        # 3. Variable Declarations
        types_mapping = self.get_types_mapping({'target_db_type': target_db_type})
        declarations = []

        # Pre-process: Rename specific conflicting variables globally before stripping @
        # e.g. @date -> @v_date (Keep @ so DECLARE regex matches it)
        body_content = re.sub(r'@date\b', '@v_date', body_content, flags=re.IGNORECASE)

        def declaration_replacer(match):
            full_decl = match.group(0)
            content = full_decl[7:].strip() # len('DECLARE') = 7

            content_clean = content.replace('@', '')
            # Custom type substitutions first
            content_clean = self._apply_data_type_substitutions(content_clean)
            content_clean = self._apply_udt_to_base_type_substitutions(content_clean, settings)
            for sybase_type, pg_type in types_mapping.items():
                content_clean = re.sub(rf'\b{re.escape(sybase_type)}\b', pg_type, content_clean, flags=re.IGNORECASE)

            parts = split_respecting_parens(content_clean)
            for part in parts:
                declarations.append(part.strip() + ';')

            return '' # Remove from body

        body_content = re.sub(r'DECLARE\s+@.*?(?=\bBEGIN\b|\bIF\b|\bWHILE\b|\bSELECT\b|\bINSERT\b|\bUPDATE\b|\bDELETE\b|\bRETURN\b|\bSET\b|\bFETCH\b|\bOPEN\b|\bCLOSE\b|\bDEALLOCATE\b|\bDECLARE\b|$)', declaration_replacer, body_content, flags=re.IGNORECASE | re.DOTALL)

        # 4. Global Replacements
        # Functions
        function_map = self.get_sql_functions_mapping({ 'target_db_type': target_db_type })
        for sybase_func, pg_equiv in function_map.items():
            escaped_src_func = re.escape(sybase_func)
            body_content = re.sub(escaped_src_func, pg_equiv, body_content, flags=re.IGNORECASE)

        # Type substitutions in body
        body_content = self._apply_data_type_substitutions(body_content)
        body_content = self._apply_udt_to_base_type_substitutions(body_content, settings)
        body_content = self._apply_udt_to_base_type_substitutions(body_content, settings)
        for sybase_type, pg_type in types_mapping.items():
            body_content = re.sub(rf'\b{re.escape(sybase_type)}\b', pg_type, body_content, flags=re.IGNORECASE)

        # Remove @
        body_content = re.sub(r'(?<!@)@([a-zA-Z0-9_]+)', r'\1', body_content)

        # INSERTED/DELETED -> NEW/OLD
        body_content = re.sub(r'\binserted\b', 'NEW', body_content, flags=re.IGNORECASE)
        body_content = re.sub(r'\bdeleted\b', 'OLD', body_content, flags=re.IGNORECASE)

        # Sybase Specific Cleanups
        # @@trancount -> 1 (Assume transaction active)
        body_content = re.sub(r'@@trancount', '1', body_content, flags=re.IGNORECASE)
        # Remove SET chained/transaction commands
        body_content = re.sub(r'SET\s+chained\s+\w+', '', body_content, flags=re.IGNORECASE)
        body_content = re.sub(r'SET\s+transaction\s+isolation\s+level\s+\d+', '', body_content, flags=re.IGNORECASE)

        # PRINT -> RAISE NOTICE
        def print_replacer(match):
             content = match.group(1).strip()
             args = split_respecting_parens(content)
             if not args:
                  return "RAISE NOTICE '';"
             first_arg = args[0]
             rest_args = args[1:]
             if first_arg.startswith("'") and first_arg.endswith("'"):
                  msg = first_arg
                  if rest_args:
                       return f"RAISE NOTICE {msg}, {', '.join(rest_args)};"
                  else:
                       return f"RAISE NOTICE {msg};"
             else:
                  format_str = ", ".join(["%"] * len(args))
                  return f"RAISE NOTICE '{format_str}', {', '.join(args)};"

        body_content = re.sub(r'print\s+(.+?)(?=;|\n|$)', print_replacer, body_content, flags=re.IGNORECASE)

        # 5. Assignments and Selects

        # Select Into
        def select_into_transformer(match):
            content = match.group(1)
            rest = match.group(2)

            # Clean up FROM NEW/OLD in rest
            # If rest contains "FROM NEW" or "FROM OLD", we strip it if it's the only thing or logic suggests.
            from_match = re.search(r'FROM\s+(.*?)(?:\bWHERE\b|\bGROUP\b|\bORDER\b|$)', rest, re.IGNORECASE)
            if from_match:
                table_list = from_match.group(1)
                # Remove comments
                table_list = re.sub(r'--.*', '', table_list)
                table_list = re.sub(r'/\*.*?\*/', '', table_list, flags=re.DOTALL)

                tables = split_respecting_parens(table_list)
                clean_tables = []
                for t in tables:
                    t_clean = t.strip()
                    if not t_clean:
                        continue
                    # Check first word (table name) against keywords
                    # NEW alias -> NEW. NEW -> NEW.
                    first_word = t_clean.split()[0].upper()
                    if first_word not in ('NEW', 'OLD', 'INSERTED', 'DELETED'):
                        clean_tables.append(t)

                if not clean_tables:
                   # No tables left, remove FROM clause entirely
                   start, end = from_match.span()
                   # Simply remove the match range from rest
                   rest = rest[:start] + rest[end:]

            if '=' in content:
                parts = split_respecting_parens(content)
                vars_list = []
                cols_list = []
                for asm in parts:
                    if '=' in asm:
                        side_l, side_r = asm.split('=', 1)
                        vars_list.append(side_l.strip())
                        cols_list.append(side_r.strip())
                    else:
                        cols_list.append(asm)
                if vars_list:
                    return f"SELECT {', '.join(cols_list)} INTO {', '.join(vars_list)} {rest}"
            return match.group(0)

        # Regex must ensure we don't cross statement boundaries (UPDATE, INSERT, etc.)
        # Added SELECT to lookahead to prevent merging multiple SELECTs
        # Also constrained matches after FROM to statement boundaries
        body_content = re.sub(r'SELECT\s+((?:(?!\b(?:UPDATE|INSERT|DELETE|IF|WHILE|RETURN|BEGIN|END|SELECT)\b).)+?)\s+(FROM\s+(?:(?!\b(?:UPDATE|INSERT|DELETE|IF|WHILE|RETURN|BEGIN|END|SELECT)\b).)+)', select_into_transformer, body_content, flags=re.IGNORECASE | re.DOTALL)

        # Cleanup: Remove FROM NEW/OLD from SELECT statements if they persist
        # e.g. "SELECT ... INTO ... FROM NEW" -> "SELECT ... INTO ..."
        body_content = re.sub(r'(SELECT\s+[^;]+?)\s+FROM\s+(?:NEW|OLD|INSERTED|DELETED)\b', r'\1', body_content, flags=re.IGNORECASE | re.DOTALL)

        # Simple Assignments
        def simple_assignment(match):
            full_match = match.group(0)
            if 'FROM' in full_match.upper():
                return full_match
            content = match.group(1).strip()
            if '=' not in content:
                return full_match

            parts = split_respecting_parens(content)
            assignments = []
            is_assignment = True
            for part in parts:
                if '=' in part:
                    side_l, side_r = part.split('=', 1)
                    assignments.append(f"{side_l.strip()} := {side_r.strip()}")
                else:
                    is_assignment = False

            if is_assignment and assignments:
                return "; ".join(assignments) + ";"
            return full_match

        body_content = re.sub(r'SELECT\s+([^;\n]+)', simple_assignment, body_content, flags=re.IGNORECASE)

        # 6. UPDATE ... FROM fixes
        # If UPDATE target ... FROM target, table2 -> FROM table2
        # Need to parse UPDATE target

        def update_from_fix(match):
            target = match.group(1)
            set_clause = match.group(2)
            from_clause = match.group(3)
            rest = match.group(4)

            # Parse FROM tables
            # Sybase FROM t1, t2
            # PG FROM t2 (if t1 is target)
            tables = split_respecting_parens(from_clause)
            new_tables = []
            for t in tables:
                t_clean = t.strip()
                # Check alias? "table alias" or "table AS alias"
                # If target matches table name or alias
                # Simplifying: check if target string is contained
                # Remove target table
                if target.lower() == t_clean.lower():
                    continue # Skip target
                # Check for "target alias"
                if t_clean.lower().startswith(target.lower() + ' ') or t_clean.lower().startswith(target.lower() + '\t'):
                     continue

                # Also remove NEW and OLD from FROM clause in triggers
                if t_clean.upper() in ('NEW', 'OLD', 'INSERTED', 'DELETED'):
                    continue

                new_tables.append(t)

            if new_tables:
                return f"UPDATE {target} {set_clause} FROM {', '.join(new_tables)} {rest}"
            else:
                # If no tables left (self update only), remove FROM
                return f"UPDATE {target} {set_clause} {rest}"

        # Regex: UPDATE target SET ... FROM ... [WHERE...]
        # Be careful matching SET ... FROM
        body_content = re.sub(r'UPDATE\s+([a-zA-Z0-9_]+)\s+(SET\s+.*?)\s+FROM\s+(.*?)(\bWHERE\b|\bGROUP\b|\bORDER\b|$)', update_from_fix, body_content, flags=re.IGNORECASE | re.DOTALL)

        # 7. String Concatenation Fix (+ -> ||)
        # Heuristic: '...' + ... or ... + '...'
        body_content = re.sub(r"('\s*)\+\s*", r"\1 || ", body_content)
        body_content = re.sub(r"\s*\+\s*(')", r" || \1", body_content)

        # 8. Control Flow (IF/WHILE) - Minimal support as per trigger usage

        # IF UPDATE(column) -> IF NEW.column IS DISTINCT FROM OLD.column
        # Needs to happen before IF regex
        def if_update_replacer(match):
            col = match.group(1)
            # Sybase: IF UPDATE(col)
            # PG: IF NEW.col IS DISTINCT FROM OLD.col
            return f"IF NEW.{col} IS DISTINCT FROM OLD.{col}"

        body_content = re.sub(r'IF\s+UPDATE\(([\w]+)\)', if_update_replacer, body_content, flags=re.IGNORECASE)

        # Rollback Trigger
        # rollback trigger [with raiserror number 'message']
        # rollback transaction ...
        # Replace with RAISE EXCEPTION
        def rollback_replacer(match):
            # Try to capture message if present
            # Pattern: rollback trigger with raiserror 99999 'Message'
            rest = match.group(1) if match.lastindex >= 1 else ''
            message = "Trigger Rollback"

            # Extract message string '...'
            msg_match = re.search(r"'([^']+)'", rest)
            if msg_match:
                message = msg_match.group(1)

            return f"RAISE EXCEPTION '{message}';"

        body_content = re.sub(r'rollback\s+(?:trigger|transaction)\s*(.*)', rollback_replacer, body_content, flags=re.IGNORECASE)

        # FIX: Ensure semicolon before ELSE/ELSIF if missing
        # Regex updated to handle comments
        body_content = re.sub(r'([^;\s])([ \t]*(?:--[^\n]*|/\*.*?\*/[ \t]*)?)\n\s*(ELSE|ELSIF)\b', r'\1;\2\n\3', body_content, flags=re.IGNORECASE)

        # ELSE IF -> ELSIF
        body_content = re.sub(r'ELSE\s+IF', 'ELSIF', body_content, flags=re.IGNORECASE)

        # IF replacement with DOTALL support for multiline conditions
        body_content = re.sub(r'IF\s+(.*?)\s+BEGIN', r'IF \1 THEN', body_content, flags=re.IGNORECASE | re.DOTALL)

        # Standardize other keywords
        body_content = re.sub(r'WHILE\s+(.*?)\s+BEGIN', r'WHILE \1 LOOP', body_content, flags=re.IGNORECASE)
        body_content = re.sub(r'ELSE\s+BEGIN', r'ELSE', body_content, flags=re.IGNORECASE)
        body_content = re.sub(r'END\s*;?\s+ELSE', r'ELSE', body_content, flags=re.IGNORECASE)


        # END replacement (simple approach for now, triggers usually simple)
        # But we stripped outer END. Inner ENDs need closure.
        # If we replaced BEGIN with THEN/LOOP, we need END IF/LOOP.
        # Let's use the stack logic if we really want to be safe, or just END IF for triggers?
        # User trigger has IF?
        # "IF UPDATE(column)" was handled in old code.
        body_content = re.sub(r'if\s+UPDATE\([a-zA-Z_]+\)', '-- IF UPDATE(column) not supported', body_content, flags=re.IGNORECASE)

        # Fix inner ENDs
        # Reuse logic from convert_funcproc_code manually or simplified
        lines = body_content.split('\n')
        new_lines = []
        stack = []
        for line in lines:
            stripped = line.strip()
            if re.search(r'IF\s+.*\s+THEN', line, flags=re.IGNORECASE):
                stack.append('IF')
            elif re.search(r'WHILE\s+.*\s+LOOP', line, flags=re.IGNORECASE):
                stack.append('LOOP')

            if re.match(r'^END\s*;?$', stripped, flags=re.IGNORECASE):
                if stack:
                    block = stack.pop()
                    if block == 'IF':
                        new_lines.append("END IF;")
                    elif block == 'LOOP':
                        new_lines.append("END LOOP;")
                    else:
                        new_lines.append('END;')
                else:
                    new_lines.append('END;') # Should not happen if outer stripped
            else:
                new_lines.append(line)
        body_content = '\n'.join(new_lines)

        # 9. Semicolon Heuristic (Ensure statements end with ;)
        lines = body_content.split('\n')
        final_lines = []
        statement_buffer = []

        def flush_buffer(buf):
            if not buf: return

            # Find last non-empty line index
            last_idx = -1
            for i in range(len(buf) - 1, -1, -1):
                line_stripped = buf[i].strip()
                if line_stripped:
                    # Skip comments
                    if line_stripped.startswith('/*') or line_stripped.startswith('--'):
                        continue
                    last_idx = i
                    break

            if last_idx != -1:
                s = buf[last_idx].rstrip()
                # Don't add if ends with ; or block openers/closers that don't need it
                ignore_ends = ('BEGIN', 'THEN', 'LOOP', 'ELSE', ';')
                if not s.upper().endswith(ignore_ends):
                    # Check if it started with a command that needs ;
                    combined = " ".join([b.strip() for b in buf]) # Join all lines to check full start
                    # Remove comments from check
                    combined_code = re.sub(r'--.*', '', combined).strip()

                    first_word = combined_code.split()[0].upper()

                    # Also check assignments "var := val"
                    # Remove strings to avoid false positives like IF x = ':='
                    combined_no_strings = re.sub(r"'.*?'", '', combined_code)
                    is_assignment = ':=' in combined_no_strings

                    # Keywords requiring semicolon
                    needs_semi = ('UPDATE', 'INSERT', 'DELETE', 'SELECT', 'PERFORM', 'CALL', 'WITH', 'MERGE', 'RAISE')

                    if first_word in needs_semi or is_assignment:
                         buf[last_idx] = s + ';'

            final_lines.extend(buf)

        for line in lines:
            stripped = line.strip()
            # Start of new statement?
            is_start = False
            # Check forkeywords that start statements
            if re.match(r'^(UPDATE|INSERT|DELETE|SELECT|IF|WHILE|RETURN|END|DECLARE|BEGIN|RAISE)\b', stripped, re.IGNORECASE):
                is_start = True
            elif ':=' in line: # Assignment line?
                 is_start = True

            if is_start:
                 flush_buffer(statement_buffer)
                 statement_buffer = [line]
            else:
                 statement_buffer.append(line)

        flush_buffer(statement_buffer)
        body_content = '\n'.join(final_lines)

        # 10. Return
        # Only replace RETURN word boundary.
        # Handle RETURN result? Sybase triggers don't typically return values like functions, but RETURN without args exits.
        # If RETURN 1 or RETURN @var, we might need to be careful.
        # For now, converting standalone RETURN to RETURN NEW;
        body_content = re.sub(r'\bRETURN\b', 'RETURN NEW;', body_content, flags=re.IGNORECASE)
        # If RETURN NEW; NEW; (double) -> fix
        body_content = body_content.replace('RETURN NEW; NEW;', 'RETURN NEW;')

        # Event parsing
        events = re.findall(r'for\s+([a-z, ]+?)(?:\s+as\b|$)', trigger_code, re.IGNORECASE)
        events = events[0].replace(' ', '').upper().split(',') if events else []
        pg_events = ' OR '.join(events)

        # Assemble
        pg_func = f"""CREATE OR REPLACE FUNCTION {trigger_name}_func()
            RETURNS trigger AS $$
            DECLARE
            {chr(10).join(declarations)}
            BEGIN
            {body_content.strip()}
            RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """

        pg_trigger = f"""CREATE TRIGGER {trigger_name}
            AFTER {pg_events} ON "{target_schema}"."{target_table}"
            FOR EACH ROW
            EXECUTE FUNCTION {trigger_name}_func();
            """

        return pg_func + '\n' + pg_trigger

    def fetch_triggers(self, table_id, schema_name, table_name):
        trigger_data = {}
        order_num = 1
        query = f"""
            SELECT
                DISTINCT
                o.name,
                o.id,
                o.sysstat,
                c.text,
                c.colid
            FROM syscomments c, sysobjects o
            WHERE o.id=c.id
                AND user_name(o.uid) = '{schema_name}'
                AND type in ('TR')
                AND (o.sysstat & 8 = 8)
            ORDER BY o.name, c.colid
        """
        self.config_parser.print_log_message('DEBUG3', f"Fetching function/procedure names for schema {schema_name}")
        self.config_parser.print_log_message('DEBUG3', f"Query: {query}")
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        triggers_text = {}
        for row in cursor.fetchall():
            trigger_name = row[0]
            trigger_id = row[1]
            sysstat = row[2]
            text_part = row[3]
            colid = row[4]

            if trigger_name not in triggers_text:
                triggers_text[trigger_name] = {
                    'id': trigger_id,
                    'sysstat': sysstat,
                    'text_parts': []
                }

            triggers_text[trigger_name]['text_parts'].append((colid, text_part))

        # Sort text parts by colid and concatenate
        for trigger_name, trigger_info in triggers_text.items():
            trigger_info['text_parts'].sort(key=lambda x: x[0])
            concatenated_sql = ''.join([part[1] for part in trigger_info['text_parts']])

            trigger_data[order_num] = {
            'name': trigger_name,
            'id': trigger_info['id'],
            'sysstat': trigger_info['sysstat'],
            'event': '',
            'new': '',
            'old': '',
            'sql': concatenated_sql,
            'comment': ''
            }
            order_num += 1
        cursor.close()
        self.disconnect()
        return trigger_data

    def fetch_views_names(self, owner_name):
        views = {}
        order_num = 1
        query = f"""
            SELECT * FROM (
                SELECT
                id,
                user_name(uid) as view_owner,
                name as view_name
                FROM sysobjects WHERE type = 'V') a
            WHERE a.view_owner = '{owner_name}'
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
        # source_schema = settings['source_schema']
        # source_view_name = settings['source_view_name']
        # target_schema = settings['target_schema']
        # target_view_name = settings['target_view_name']
        query = f"""
            SELECT c.text
            FROM syscomments c
            JOIN sysobjects o
            ON o.id=c.id
            WHERE o.id = {view_id}
            ORDER BY c.colid
        """
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        view_code = cursor.fetchall()
        cursor.close()
        self.disconnect()
        view_code_str = ''.join([code[0] for code in view_code])
        return view_code_str

    def convert_view_code(self, settings: dict):

        def quote_column_names(node):
            if isinstance(node, sqlglot.exp.Column) and node.name:
                node.set("this", sqlglot.exp.Identifier(this=node.name, quoted=True))
            if isinstance(node, sqlglot.exp.Alias) and isinstance(node.args.get("alias"), sqlglot.exp.Identifier):
                alias = node.args["alias"]
                if not alias.args.get("quoted"):
                    alias.set("quoted", True)
            # for child in node.iter_expressions():
            #     quote_column_names(child)
            return node

        def replace_schema_names(node):
            if isinstance(node, sqlglot.exp.Table):
                schema = node.args.get("db")
                if schema and schema.name == settings['source_schema']:
                    node.set("db", sqlglot.exp.Identifier(this=settings['target_schema'], quoted=False))
            return node

        def quote_schema_and_table_names(node):
            if isinstance(node, sqlglot.exp.Table):
                # Quote schema name if present
                schema = node.args.get("db")
                if schema and not schema.args.get("quoted"):
                    schema.set("quoted", True)
                # Quote table name
                table = node.args.get("this")
                if table and not table.args.get("quoted"):
                    table.set("quoted", True)
            return node

        def replace_functions(node):
            mapping = self.get_sql_functions_mapping({ 'target_db_type': settings['target_db_type'] })
            # Prepare mapping for function names (without parentheses)
            func_name_map = {}
            for k, v in mapping.items():
                if k.endswith('('):
                    func_name_map[k[:-1].lower()] = v[:-1] if v.endswith('(') else v
                elif k.endswith('()'):
                    func_name_map[k[:-2].lower()] = v
                else:
                    func_name_map[k.lower()] = v

            if isinstance(node, sqlglot.exp.Anonymous):
                func_name = node.name.lower()
                if func_name in func_name_map:
                    mapped = func_name_map[func_name]
                    # If mapped is a function name, replace the function name
                    if '(' not in mapped:
                        node.set("this", sqlglot.exp.Identifier(this=mapped, quoted=False))
                    else:
                        # For mappings like 'year(' -> 'extract(year from '
                        # We need to rewrite the function call
                        if mapped.startswith('extract('):
                            # e.g. year(t1.b) -> extract(year from t1.b)
                            arg = node.args.get("expressions")
                            if arg and len(arg) == 1:
                                part = func_name
                                return sqlglot.exp.Extract(
                                    this=sqlglot.exp.Identifier(this=part, quoted=False),
                                    expression=arg[0]
                                )
                        else:
                            # Iterate over the mapping to handle function name replacements
                            for orig, repl in mapping.items():
                                # Handle mappings ending with '(' (function calls)
                                if orig.endswith('(') and func_name == orig[:-1].lower():
                                    if repl.endswith('('):
                                        node.set("this", sqlglot.exp.Identifier(this=repl[:-1], quoted=False))
                                    else:
                                        node.set("this", sqlglot.exp.Identifier(this=repl, quoted=False))
                                    break
                                # Handle mappings ending with '()' (function calls with no args)
                                elif orig.endswith('()') and func_name == orig[:-2].lower():
                                    node.set("this", sqlglot.exp.Identifier(this=repl, quoted=False))
                                    break
                    # For direct function name replacements, handled above
                # For functions like getdate(), getutcdate(), etc.
                elif func_name + "()" in func_name_map:
                    mapped = func_name_map[func_name + "()"]
                    return sqlglot.exp.Anonymous(this=mapped)
            return node


        def transform_sybase_joins(expression):
            # Check for EQ nodes with outer join comments
            outer_joins = []
            for node in expression.find_all(sqlglot.exp.EQ):
                if node.comments and any('left_outer' in c for c in node.comments):
                    outer_joins.append((node, 'LEFT'))
                elif node.comments and any('right_outer' in c for c in node.comments):
                    outer_joins.append((node, 'RIGHT'))

            for node, join_type in outer_joins:
                # LEFT JOIN: A *= B -> left=A, right=B (preserves A, B is null supplying)
                # RIGHT JOIN: A =* B -> left=A, right=B (preserves B, A is null supplying)

                null_supplying_col = node.right if join_type == 'LEFT' else node.left

                # Identify target table from null supplying column
                target_alias = ""
                if isinstance(null_supplying_col, sqlglot.exp.Column):
                     target_alias = null_supplying_col.table

                if not target_alias:
                    continue

                select_node = node.find_ancestor(sqlglot.exp.Select)
                if not select_node:
                    continue

                # Find target table in FROM clause
                target_table_node = None
                from_clause = select_node.args.get('from')
                if from_clause:
                     for child in from_clause.expressions:
                         # 1. Direct match (Table or Aliased Subquery)
                         if child.alias_or_name == target_alias:
                             target_table_node = child
                             break

                         # 2. Wrapper match (Subquery/Paren without explicit alias, containing the table)
                         # e.g. FROM (table) -> child is Subquery/Paren
                         child_tables = list(child.find_all(sqlglot.exp.Table))
                         if len(child_tables) == 1 and child_tables[0].alias_or_name == target_alias:
                             target_table_node = child
                             break

                if target_table_node:
                    # Remove from FROM clause
                    from_clause.expressions.remove(target_table_node)

                    # Create JOIN
                    join_condition = node.copy()
                    join_condition.comments = None

                    join = sqlglot.exp.Join(
                        this=target_table_node,
                        kind="LEFT",
                        on=join_condition
                    )

                    # Add to Select joins
                    if "joins" not in select_node.args:
                        select_node.args["joins"] = []
                    select_node.args["joins"].append(join)

                    # Replace condition in WHERE with TRUE
                    node.replace(sqlglot.exp.Boolean(this=True))

            return expression

        def convert_string_concatenation(node):
            if isinstance(node, sqlglot.exp.Add):
                left = node.left
                right = node.right
                is_left_string = left.is_string or (isinstance(left, sqlglot.exp.Cast) and left.to.this.name.upper() in ('VARCHAR', 'CHAR', 'TEXT', 'NVARCHAR', 'NCHAR', 'UNIVARCHAR', 'UNICHAR'))
                is_right_string = right.is_string or (isinstance(right, sqlglot.exp.Cast) and right.to.this.name.upper() in ('VARCHAR', 'CHAR', 'TEXT', 'NVARCHAR', 'NCHAR', 'UNIVARCHAR', 'UNICHAR'))

                if is_left_string or is_right_string:
                    # Conversion needed
                    new_left = left
                    new_right = right

                    # Cast non-string operands to text to avoid type errors in PostgreSQL
                    if not is_left_string:
                         new_left = sqlglot.exp.Cast(this=left, to=sqlglot.exp.DataType.build('text'))
                    if not is_right_string:
                         new_right = sqlglot.exp.Cast(this=right, to=sqlglot.exp.DataType.build('text'))

                    return sqlglot.exp.DPipe(this=new_left, expression=new_right)
            return node

        self.config_parser.print_log_message('DEBUG3', f"settings in convert_view_code: {settings}")
        converted_code = settings['view_code']

        # Apply remote_objects_substitution
        remote_subs = self.config_parser.get_remote_objects_substitution()
        if remote_subs:
            iterator = remote_subs.items() if isinstance(remote_subs, dict) else remote_subs
            for source_obj, target_obj in iterator:
                if source_obj and target_obj:
                    # Case-insensitive replacement
                    converted_code = re.sub(re.escape(source_obj), target_obj, converted_code, flags=re.IGNORECASE)
                    self.config_parser.print_log_message('DEBUG', f"Applied remote object substitution: {source_obj} -> {target_obj}")

        # Pre-process Sybase specific join syntax
        # *= -> = /* left_outer */
        # =* -> = /* right_outer */
        # Pre-process Sybase specific join syntax
        # *= -> = /* left_outer */
        # =* -> = /* right_outer */
        converted_code = re.sub(r'\*=', '= /* left_outer */', converted_code)
        converted_code = re.sub(r'=\*', '= /* right_outer */', converted_code)

        # Remove 'noholdlock' hints (often interpreted as aliases)
        converted_code = re.sub(r'\bnoholdlock\b', '', converted_code, flags=re.IGNORECASE)

        converted_code = self._apply_udt_to_base_type_substitutions(converted_code, settings)

        if settings['target_db_type'] == 'postgresql':

            try:
                parsed_code = sqlglot.parse_one(converted_code)
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"Error parsing View code: {e}")
                return ''

            # double quote column names
            parsed_code = parsed_code.transform(quote_column_names)

            # Transform Sybase Joins
            parsed_code = transform_sybase_joins(parsed_code)

            # Convert string concatenation + to ||
            parsed_code = parsed_code.transform(convert_string_concatenation)

            self.config_parser.print_log_message('DEBUG3', f"Double quoted columns: {parsed_code.sql()}")

            # replace source schema with target schema
            parsed_code = parsed_code.transform(replace_schema_names)
            self.config_parser.print_log_message('DEBUG3', f"Replaced schema names: {parsed_code.sql()}")

            # double quote schema and table names
            parsed_code = parsed_code.transform(quote_schema_and_table_names)
            self.config_parser.print_log_message('DEBUG3', f"Double quoted schema and table names: {parsed_code.sql()}")

            # replace functions
            parsed_code = parsed_code.transform(replace_functions)
            self.config_parser.print_log_message('DEBUG3', f"Replaced functions: {parsed_code.sql()}")

            converted_code = parsed_code.sql()
            converted_code = converted_code.replace("()()", "()")

            sql_functions_mapping = self.get_sql_functions_mapping({ 'target_db_type': settings['target_db_type'] })

            if sql_functions_mapping:
                for src_func, tgt_func in sql_functions_mapping.items():
                    escaped_src_func = re.escape(src_func)
                    converted_code = re.sub(rf"(?i){escaped_src_func}", tgt_func, converted_code, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
                    self.config_parser.print_log_message('DEBUG', f"Checking convertion of function {src_func} to {tgt_func} in view code")

            # converted_code = converted_code.replace(f"{settings['source_database']}..", f"{settings['target_schema']}.")
            # converted_code = converted_code.replace(f"{settings['source_database']}.{settings['source_schema']}.", f"{settings['target_schema']}.")
            # converted_code = converted_code.replace(f"{settings['source_schema']}.", f"{settings['target_schema']}.")
            self.config_parser.print_log_message('DEBUG', f"Converted view: {converted_code}")
        else:
            self.config_parser.print_log_message('ERROR', f"Unsupported target database type: {settings['target_db_type']}")
        return converted_code

    def get_sequence_current_value(self, sequence_name):
        pass

    def fetch_user_defined_types(self, schema: str):
        # Fetch user defined types
        # We look for entries in systypes where usertype > 100 (user defined)
        # We join with a second instance of systypes to get the base physical type name.

        # Note: In ASE, types define length/prec/scale.
        # Variable length types: varchar, char, nvarchar, nchar, varbinary, binary -> use length
        # Numeric types: numeric, decimal -> use prec, scale

        query = """
            SELECT
                u.name as schema_name,
                t.name as type_name,
                t.length,
                t.prec,
                t.scale,
                bt.name as base_type_name
            FROM dbo.systypes t
            JOIN dbo.sysusers u ON t.uid = u.uid
            LEFT JOIN dbo.systypes bt ON t.type = bt.type AND bt.usertype < 100
            WHERE t.usertype > 100
            ORDER BY t.name
        """

        self.connect()
        cursor = self.connection.cursor()
        self.config_parser.print_log_message('DEBUG', "Fetching user defined types")
        cursor.execute(query)
        rows = cursor.fetchall()

        udts = {}
        order_num = 1

        for row in rows:
            schema_name = row[0]
            type_name = row[1]
            length = row[2]
            prec = row[3]
            scale = row[4]
            base_type = row[5]

            # Construct SQL definition
            if not base_type:
                # Should not happen for valid UDTs referencing standard types
                base_type = "UNKNOWN"

            # Create source type SQL for reference (Sybase DDL)
            type_sql = base_type.upper()
            base_lower = base_type.lower()

            if base_lower in ('varchar', 'char', 'nvarchar', 'nchar', 'varbinary', 'binary', 'univarchar', 'unichar'):
                type_sql += f"({length})"
            elif base_lower in ('numeric', 'decimal'):
                type_sql += f"({prec},{scale})"

            udts[order_num] = {
                'schema_name': schema_name,
                'type_name': type_name,
                'sql': type_sql,
                'base_type': base_type,
                'length': length,
                'prec': prec,
                'scale': scale,
                'comment': ''
            }
            order_num += 1

        cursor.close()
        self.disconnect()
        return udts

    def get_table_size(self, table_schema: str, table_name: str):
        query = f"""
            SELECT
                data_pages(db_id(), o.id, 0)*b.blocksize*1024 as size_bytes
            FROM {table_schema}.sysobjects o,
                (SELECT low/1024 as blocksize
                FROM master.{table_schema}.spt_values d
                WHERE d.number = 1 AND d.type = 'E') b
            WHERE type='U' and o.name = '{table_name}'
            """
        # self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        cursor.close()
        # self.disconnect()
        return row[0]

    def fetch_domains(self, schema: str):
        order_num = 1
        domains = {}
        schema_condition = f"AND r.uid = USER_ID('{schema}')" if schema else ""
        query = f"""
            SELECT
                r.name AS RuleName,
                USER_NAME(r.uid) AS RuleOwner,
                sc.colid AS DefinitionLineNumber,
                sc.text AS RuleDefinitionPart
            FROM
                sysobjects r
            JOIN
                syscomments sc ON r.id = sc.id
            WHERE
                r.type = 'R' {schema_condition}
            ORDER BY
                RuleName, DefinitionLineNumber
        """
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        domains = {}
        for row in rows:
            rule_name = row[0]
            rule_owner = row[1]
            rule_definition_part = row[3].strip()
            if rule_name not in domains:
                domains[rule_name] = {
                    'domain_schema': schema,
                    'domain_name': rule_name,
                    'domain_owner': rule_owner,
                    'source_domain_sql': rule_definition_part,
                    'domain_comment': '',
                }
            else:
                domains[rule_name]['source_domain_sql'] += '' + rule_definition_part

        for rule_name, domain_info in domains.items():
            query = f"""
                SELECT DISTINCT
                    bt.name as basic_data_type
                FROM sysobjects r
                LEFT JOIN syscolumns c ON c.domain = r.id
                LEFT JOIN sysobjects o ON c.id = o.id
                LEFT JOIN systypes ut ON c.usertype = ut.usertype
                LEFT JOIN (
                    SELECT * FROM systypes t
                    JOIN (SELECT type, min(usertype) as usertype FROM systypes GROUP BY type) bt0
                    ON t.type = bt0.type AND t.usertype = bt0.usertype) bt
                ON ut.type = bt.type AND ut.hierarchy = bt.hierarchy
                WHERE r.type = 'R' AND r.name = '{domain_info['domain_name']}'
            """
            cursor.execute(query)
            row = cursor.fetchone()
            if row:
                basic_data_type = row[0]
                domains[rule_name]['domain_data_type'] = basic_data_type
            else:
                domains[rule_name]['domain_data_type'] = None

            domains[rule_name]['source_domain_sql'] = domains[rule_name]['source_domain_sql'].replace('\n', ' ')

            domain_check_sql = domains[rule_name]['source_domain_sql']
            domain_check_sql = re.sub(r'@\w+', 'VALUE', domain_check_sql)
            domain_check_sql = re.sub(r'create rule', '', domain_check_sql, flags=re.IGNORECASE)
            domain_check_sql = re.sub(rf"{re.escape(domains[rule_name]['domain_name'])}\s+AS", '', domain_check_sql, flags=re.IGNORECASE)
            domain_check_sql = domain_check_sql.replace('"', "'")
            # Remove all comments starting with /* and ending with */
            domain_check_sql = re.sub(r'/\*.*?\*/', '', domain_check_sql, flags=re.DOTALL)
            domains[rule_name]['source_domain_check_sql'] = domain_check_sql.strip()

        cursor.close()
        self.disconnect()
        self.config_parser.print_log_message('DEBUG', f"Found domains: {domains}")
        return domains

    def get_create_domain_sql(self, settings):
        # Placeholder for generating CREATE DOMAIN SQL
        return ""

    def get_table_description(self, settings) -> dict:
        table_schema = settings['table_schema']
        table_name = settings['table_name']
        output = ""
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(f"exec sp_help '{table_schema}.{table_name}'")

            set_num = 1
            while True:
                if cursor.description is not None:
                    rows = cursor.fetchall()
                    if rows:
                        output += f"Result set {set_num}:\n"
                        columns = [column[0] for column in cursor.description]
                        table = tabulate(rows, headers=columns, tablefmt="github")
                        output += table + "\n\n"
                        set_num += 1
                if not cursor.nextset():
                    break

            cursor.close()
            self.disconnect()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching table description for {table_schema}.{table_name}: {e}")
            raise

        return { 'table_description': output.strip() }


    def testing_select(self):
        return 'SELECT 1'

    def get_database_version(self):
        query = "SELECT @@version"
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        version = cursor.fetchone()[0]
        cursor.close()
        self.disconnect()
        return version

    def get_database_size(self):
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute("exec sp_spaceused")
        row = cursor.fetchone()
        self.logger.info(f"\n* Total size of Sybase database: {row}")
        size = row[1]
        if cursor.nextset():
            row = cursor.fetchone()
            self.logger.info(
            f"  Reserved: {row[0]}\n"
            f"  Data: {row[1]}\n"
            f"  Indexes: {row[2]}\n"
            f"  Unused: {row[3]}"
            )
        cursor.close()
        self.disconnect()
        return size

    def get_top_n_tables(self, settings):
        """
        //TODO
        what about this query?:

        select top 10 convert(varchar(30),o.name) AS table_name,
        row_count(db_id(), o.id) AS row_count,
        data_pages(db_id(), o.id, 0) AS pages,
        data_pages(db_id(), o.id, 0) * (@@maxpagesize/1024) AS kbs
        from sysobjects o
        where type = 'U'
        order by kbs DESC, table_name ASC
        """
        top_tables = {}
        top_tables['by_rows'] = {}
        top_tables['by_size'] = {}
        top_tables['by_columns'] = {}
        top_tables['by_indexes'] = {}
        top_tables['by_constraints'] = {}
        # return top_tables

        source_schema = settings['source_schema']
        try:
            order_num = 1
            top_n = self.config_parser.get_top_n_tables_by_rows()
            if top_n > 0:
                self.connect()
                cursor = self.connection.cursor()
                top_n = 10
                query = f"""
                SELECT TOP {top_n}
                user_name(o.uid) as owner,
                o.name as table_name,
                row_count(db_id(), o.id) as row_count,
                data_pages(db_id(), o.id, 0)*b.blocksize as row_size
                FROM {source_schema}.sysobjects o,
                (SELECT low/1024 as blocksize
                FROM master.{source_schema}.spt_values d
                WHERE d.number = 1 AND d.type = 'E') b
                WHERE type='U'
                ORDER BY row_count DESC
                """
                self.config_parser.print_log_message('DEBUG', f"Executing query to get top {top_n} tables by rows: {query}")
                cursor.execute(query)
                order_num = 1
                rows = cursor.fetchall()
                cursor.close()
                self.disconnect()
                for row in rows:
                    top_tables['by_rows'][order_num] = {
                        'owner': row[0].strip(),
                        'table_name': row[1].strip(),
                        'row_count': row[2],
                        'row_size': row[3],
                    }
                    order_num += 1
                self.config_parser.print_log_message('DEBUG', f"Top tables by rows: {top_tables['by_rows']}")
            else:
                self.config_parser.print_log_message('DEBUG', f"Skipping top tables by rows check, top_n is set to 0")

        except Exception as error:
            self.config_parser.print_log_message('ERROR', f"Warning: cannot check top tables by rows - error: {error}")

        return top_tables

    def get_top_fk_dependencies(self, settings):
        top_fk_dependencies = {}
        return top_fk_dependencies

    def target_table_exists(self, target_schema, target_table):
        """
        Check if the target table exists in the target schema.
        """
        query = f"""
            SELECT COUNT(*)
            FROM sysobjects o
            WHERE user_name(o.uid) = '{target_schema}'
              AND o.name = '{target_table}'
              AND o.type = 'U'
              AND (o.sysstat & 2048 <> 2048)
        """
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        exists = cursor.fetchone()[0] > 0
        cursor.close()
        self.disconnect()
        return exists

    def fetch_all_rows(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

if __name__ == "__main__":
    print("This script is not meant to be run directly")
