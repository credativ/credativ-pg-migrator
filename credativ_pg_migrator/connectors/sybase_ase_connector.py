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
from tabulate import tabulate
import sqlglot

class SybaseASEConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        if source_or_target != 'source':
            raise ValueError(f"Sybase ASE is only supported as a source database")

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

                    result[ordinal_position]['basic_data_type'] = source_data_type
                    result[ordinal_position]['basic_character_maximum_length'] = basic_character_maximum_length
                    result[ordinal_position]['basic_numeric_precision'] = data_type_precision if self.is_numeric_type(source_data_type) else None
                    result[ordinal_position]['basic_numeric_scale'] = data_type_scale if self.is_numeric_type(source_data_type) else None
                    result[ordinal_position]['basic_column_type'] = source_data_type_length

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
        target_db_type = settings['target_db_type']
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
                'TEXT': 'TEXT',
                'SYSNAME': 'TEXT',
                'LONGSYSNAME': 'TEXT',
                'LONG VARCHAR': 'TEXT',
                'LONG NVARCHAR': 'TEXT',
                'UNITEXT': 'TEXT',
                'UNIVARCHAR': 'VARCHAR',
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
            ref_table_name = constraint[2]
            ref_column = constraint[3].strip()

            table_constraints[order_num] = {
                'constraint_name': fk_name,
                'constraint_owner': source_table_schema,
                'constraint_type': 'FOREIGN KEY',
                'constraint_columns': fk_column,
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
        procbody_str = ' '.join([body[0] for body in procbody])
        return procbody_str

    def convert_funcproc_code(self, settings):
        funcproc_code = settings['funcproc_code']
        target_db_type = settings['target_db_type']
        source_schema = settings['source_schema']
        target_schema = settings['target_schema']
        table_list = settings['table_list']
        view_list = settings['view_list']

        function_immutable = ''

        ### this functionality will be published later, not in this version
        return ""

        if target_db_type == 'postgresql':
            postgresql_code = funcproc_code

            # Replace empty lines with ";"
            postgresql_code = re.sub(r'^\s*$', ';\n', postgresql_code, flags=re.MULTILINE)
            # Split the code based on "\n
            commands = [command.strip() for command in postgresql_code.split('\n') if command.strip()]
            postgresql_code = ''
            line_number = 0

            for command in commands:
                command = command.strip().upper()
                self.config_parser.print_log_message('DEBUG3', f"Processing command: '{command}'")

                if command.startswith('--'):
                    command = command.replace(command, f"\n/* {command.strip()} */;")

                if command.startswith('IF'):
                    command = command.replace(command, f";{command.strip()}")

                if command == 'AS':
                    command = command.replace(command, "AS $$\nBEGIN\n")

                # Add ";" before specific keywords (case insensitive)
                keywords = ["LET", "END FOREACH", "EXIT FOREACH", "RETURN", "DEFINE", "ON EXCEPTION", "END EXCEPTION",
                            "ELSE", "ELIF", "END IF", "END LOOP", "END WHILE", "END FOR", "END FUNCTION", "END PROCEDURE",
                            "UPDATE", "INSERT", "DELETE FROM"]
                for keyword in keywords:
                    command = re.sub(r'(?i)\b' + re.escape(keyword) + r'\b', ";" + keyword, command, flags=re.IGNORECASE)

                    # Comment out lines starting with FOR followed by a single word within the first 5 lines
                if re.match(r'^\s*FOR\s+\w+\s*$', command, flags=re.IGNORECASE) and line_number <= 5:
                    command = f"/* {command} */"

                # Add ";" after specific keywords (case insensitive)
                keywords = ["ELSE", "END IF", "END LOOP", "END WHILE", "END FOR", "END FUNCTION", "END PROCEDURE", "THEN", "END EXCEPTION",
                            "EXIT FOREACH", "END FOREACH", "CONTINUE FOREACH", "EXIT WHILE", "EXIT FOR", "EXIT LOOP"]
                for keyword in keywords:
                    command = re.sub(r'(?i)\b' + re.escape(keyword) + r'\b', keyword + ";", command, flags=re.IGNORECASE)

                postgresql_code += ' ' + command + ' '
                line_number += 1

            commands = postgresql_code.split(';')
            postgresql_code = ''
            for command in commands:
                command = command.strip().replace('\n', ' ')
                command = re.sub(r'\s+', ' ', command)
                # command = command.strip()
                if command:
                    command = command + ';\n'
                    command = re.sub(r'THEN;', 'THEN', command, flags=re.IGNORECASE)
                    command = re.sub(r' \*/;', ' */', command, flags=re.IGNORECASE)
                    command = re.sub(r'--;\n', '--', command, flags=re.IGNORECASE)

                postgresql_code += command

            postgresql_code = re.sub(r'(\S)\s*(/\*)', r'\1\n\2', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r'\n\*/;', ' */', postgresql_code, flags=re.IGNORECASE)

        return postgresql_code

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

    def get_rows_count(self, table_schema: str, table_name: str):
        query = f"""SELECT COUNT(*) FROM {table_schema}.{table_name} """
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
            source_table_rows = self.get_rows_count(source_schema, source_table)
            migration_limitation = settings['migration_limitation']
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
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Table {source_table} is empty - skipping data migration.")
                return 0
            else:
                part_name = 'migrate_table in batches using cursor'
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Table {source_table} has {source_table_rows} rows - starting data migration.")

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

                    select_columns_list.append(f"{col['column_name']}")
                select_columns = ', '.join(select_columns_list)

                # Open a cursor and fetch rows in batches
                query = f"SELECT {select_columns} FROM {source_schema}.{source_table}"
                if migration_limitation:
                    query += f" WHERE {migration_limitation}"

                self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Fetching data with cursor using query: {query}")

                sybase_cursor = self.connection.cursor()
                sybase_cursor.execute(query)
                total_inserted_rows = 0
                while True:
                    records = sybase_cursor.fetchmany(batch_size)
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
                    self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Starting insert of {len(records)} rows from source table {source_table}")
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
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Inserted {inserted_rows} (total: {total_inserted_rows} from: {source_table_rows} ({round(total_inserted_rows/source_table_rows*100, 2)}%)) rows into target table '{target_table}'")

                target_table_rows = migrate_target_connection.get_rows_count(target_schema, target_table)
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Target table {target_schema}.{target_table} has {target_table_rows} rows")
                migrator_tables.update_data_migration_status(protocol_id, True, 'OK', target_table_rows)
                sybase_cursor.close()

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Error during {part_name} -> {e}")
            self.config_parser.print_log_message('ERROR', "Full stack trace:")
            self.config_parser.print_log_message('ERROR', traceback.format_exc())
            raise e
        finally:
            self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Finished processing table {source_table}.")
            return target_table_rows

    def convert_trigger(self, settings):
        trigger_name = settings['trigger_name']
        trigger_code = settings['trigger_sql']
        source_schema = settings['source_schema']
        target_schema = settings['target_schema']
        table_list = settings['table_list']
        return ''

    def fetch_triggers(self, table_id, schema_name, table_name):
        trigger_data = {}
        order_num = 1
        query = f"""
            SELECT
                DISTINCT
                o.name,
                o.id,
                o.sysstat
            FROM syscomments c, sysobjects o
            WHERE o.id=c.id
                AND user_name(o.uid) = '{schema_name}'
                AND type in ('TR')
                AND (o.sysstat & 8 = 8)
            ORDER BY o.name
        """
        self.config_parser.print_log_message('DEBUG3', f"Fetching function/procedure names for schema {schema_name}")
        self.config_parser.print_log_message('DEBUG3', f"Query: {query}")
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            trigger_data[order_num] = {
                'name': row[0],
                'id': row[1],
                'sysstat': row[2],
                'event': '',
                'new': '',
                'old': '',
                'sql': '',
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

        self.config_parser.print_log_message('DEBUG3', f"settings in convert_view_code: {settings}")
        converted_code = settings['view_code']
        if settings['target_db_type'] == 'postgresql':

            try:
                parsed_code = sqlglot.parse_one(converted_code)
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"Error parsing View code: {e}")
                return ''

            # double quote column names
            parsed_code = parsed_code.transform(quote_column_names)
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
        pass

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

    def get_top10_biggest_tables(self, settings):
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
        source_schema = settings['source_schema']
        try:
            self.connect()
            cursor = self.connection.cursor()
            top_n = 10
            query = f"""
            SELECT TOP {top_n}
                o.name,
                row_count(db_id(), o.id) as total_rows,
                data_pages(db_id(), o.id, 0)*b.blocksize as size_kb
            FROM {source_schema}.sysobjects o,
                (SELECT low/1024 as blocksize
                 FROM master.{source_schema}.spt_values d
                 WHERE d.number = 1 AND d.type = 'E') b
            WHERE type='U'
            ORDER BY size_kb DESC
            """
            self.config_parser.print_log_message('DEBUG', f"Executing query to get top {top_n} biggest tables: {query}")
            cursor.execute(query)
            result = {}
            order_num = 1
            rows = cursor.fetchall()
            for row in rows:
                result[order_num] = {
                    'table_name': row[0],
                    'total_rows': row[1],
                    'size_bytes': int(row[2]) * 1024
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return result
        except Exception as error:
            self.config_parser.print_log_message('ERROR', f"Warning: cannot check top biggest tables - error: {error}")
            return []

if __name__ == "__main__":
    print("This script is not meant to be run directly")
