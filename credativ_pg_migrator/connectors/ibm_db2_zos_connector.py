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
from credativ_pg_migrator.migrator_tables import MigratorTables
import psycopg2
import time
import datetime
import os
import glob
import re
import sqlglot

class IbmDb2ZosConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        if source_or_target != 'source':
            raise ValueError("IBM DB2 z/OS is only supported as a source database")

        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: __init__: Starting INIT")
        self.connectivity = self.config_parser.get_connectivity(self.source_or_target)
        self.on_error_action = self.config_parser.get_on_error_action()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger
        self.source_db_config = self.config_parser.get_source_config()

        if self.connectivity == self.config_parser.const_connectivity_ddl():
            self.ddl_path = self.source_db_config['ddl']['path']
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: __init__: Source_db_config: {self.source_db_config} - ddl_path: {self.ddl_path}")

            self.ddl_files = []
            if os.path.exists(self.ddl_path) and os.path.isdir(self.ddl_path):
                self.ddl_files = glob.glob(os.path.join(self.ddl_path, '*.*'))
            else:
                self.ddl_files = glob.glob(self.ddl_path)

            if not self.ddl_files:
                raise ValueError(f"No DDL files found for path or mask: '{self.ddl_path}'")

            self.config_parser.print_log_message('INFO', f"ibm_db2_zos_connector: __init__: DDL path valid: '{self.ddl_path}', found {len(self.ddl_files)} files")

            extension_counts = {}
            for filepath in self.ddl_files:
                if os.path.isfile(filepath):
                    ext = os.path.splitext(filepath)[1]
                    extension_counts[ext] = extension_counts.get(ext, 0) + 1
            for ext, count in extension_counts.items():
                self.config_parser.print_log_message('INFO', f"ibm_db2_zos_connector: __init__: Found {count} files with extension '{ext}'")
        else:
            raise ValueError(f"Unsupported IBM DB2 z/OS connectivity: {self.connectivity}")

        self.migrator_tables = MigratorTables(self.logger, self.config_parser)
        self.protocol_schema = self.migrator_tables.protocol_schema

        self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: __init__: INIT done")

    def connect(self):
        self.config_parser.print_log_message('DEBUG', "ibm_db2_zos_connector: connect: connect() called.")
        pass

    def disconnect(self):
        self.config_parser.print_log_message('DEBUG', "ibm_db2_zos_connector: disconnect: disconnect() called.")
        pass

    def fetch_all_tables(self, schema_name: str) -> dict:
        tables = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT source_schema_name, source_table_name, source_partition_columns, source_partition_ranges
                        FROM "{self.protocol_schema}"."ddl_tables"
                        WHERE upper(trim(source_schema_name)) = upper(trim('{schema_name}'))
                        ORDER BY id"""
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: fetch_all_tables: ({schema_name}): starting: schema_name: {schema_name} - self.connectivity: {self.connectivity} - query: {query}")
            try:
                cursor = self.migrator_tables.protocol_connection.connection.cursor()
                cursor.execute(query)
                rows = cursor.fetchall()
                self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: fetch_all_tables: ({schema_name}): {rows}")
                for i, row in enumerate(rows, 1):
                    tables[i] = {
                        'id': i,
                        'schema_name': row[0],
                        'table_name': row[1],
                        'comment': f"Partition: {row[2]}, Ranges: {row[3]}" if row[2] else None
                    }
                cursor.close()
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"ibm_db2_zos_connector: fetch_all_tables: ({schema_name}): {e}")
                raise
        return tables

    def fetch_table_columns(self, settings) -> dict:
        self.config_parser.print_log_message('DEBUG', "ibm_db2_zos_connector: fetch_table_columns: fetch_table_columns() called.")
        table_schema = settings.get('table_schema')
        table_name = settings.get('table_name')
        columns = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT source_column_name, source_data_type, source_is_nullable, source_default_value, source_pk_indicator, source_is_identity
                        FROM "{self.protocol_schema}"."ddl_columns"
                        WHERE trim(source_schema_name) = trim(%s) AND trim(source_table_name) = trim(%s) ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (table_schema, table_name))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: fetch_table_columns: ({table_schema}.{table_name}): {rows}")
            for i, row in enumerate(rows, 1):
                col_name = row[0]
                col_type = row[1]
                is_nullable = 'YES' if row[2] else 'NO'
                default_val = row[3]
                is_pk = row[4]
                is_identity = 'YES' if row[5] else 'NO'

                # when column is identity, code shall ignore default value if this is set
                if is_identity == 'YES' and default_val is not None:
                    default_val = None

                base_type = col_type.split('(')[0].strip().upper()
                char_length = None
                numeric_prec = None
                numeric_scale = None

                if '(' in col_type:
                    params_str = col_type[col_type.find('(')+1:col_type.find(')')]
                    params = [p.strip() for p in params_str.split(',')]
                    if base_type in ['CHAR', 'VARCHAR', 'CLOB', 'GRAPHIC', 'VARGRAPHIC', 'DBCLOB', 'BINARY', 'VARBINARY', 'BLOB']:
                        char_length = params[0]
                    elif base_type in ['DECIMAL', 'NUMERIC']:
                        numeric_prec = params[0]
                        if len(params) > 1:
                            numeric_scale = params[1]

                columns[i] = {
                    'column_name': col_name,
                    'is_nullable': is_nullable,
                    'column_default_name': None,
                    'column_default_value': default_val,
                    'replaced_column_default_value': None,
                    'data_type': base_type,
                    'column_type': col_type,
                    'column_type_substitution': None,
                    'character_maximum_length': char_length,
                    'numeric_precision': numeric_prec,
                    'numeric_scale': numeric_scale,
                    'basic_data_type': None,
                    'basic_character_maximum_length': None,
                    'basic_numeric_precision': None,
                    'basic_numeric_scale': None,
                    'basic_column_type': None,
                    'is_identity': is_identity,
                    'column_comment': 'Primary Key' if is_pk else None,
                    'is_generated_virtual': 'NO',
                    'is_generated_stored': 'NO',
                    'generation_expression': None,
                    'stripped_generation_expression': None,
                    'udt_schema': None,
                    'udt_name': None,
                    'domain_schema': None,
                    'domain_name': None,
                    'is_hidden_column': 'NO'
                }
            cursor.close()
        return columns

    def get_types_mapping(self, settings):
        target_db_type = settings['target_db_type']
        types_mapping = {}
        if target_db_type == 'postgresql':
            types_mapping = {
                'SMALLINT': 'SMALLINT',
                'INTEGER': 'INTEGER',
                'INT': 'INTEGER',
                'BIGINT': 'BIGINT',
                'DECIMAL': 'DECIMAL',
                'NUMERIC': 'NUMERIC',
                'REAL': 'REAL',
                'DOUBLE': 'DOUBLE PRECISION',
                'FLOAT': 'DOUBLE PRECISION',
                'DECFLOAT': 'NUMERIC',
                'CHAR': 'CHAR',
                'VARCHAR': 'VARCHAR',
                'CLOB': 'TEXT',
                'GRAPHIC': 'CHAR',
                'VARGRAPHIC': 'VARCHAR',
                'DBCLOB': 'TEXT',
                'BINARY': 'BYTEA',
                'VARBINARY': 'BYTEA',
                'BLOB': 'BYTEA',
                'DATE': 'DATE',
                'TIME': 'TIME',
                'TIMESTAMP': 'TIMESTAMP',
                'XML': 'XML',
                'ROWID': 'BYTEA'
            }
        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

        return types_mapping


    def parse_ddl_files(self, settings):
        self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: parse_ddl_files: Starting DDL parser - self.ddl_files: {self.ddl_files}")
        migrator_tables = settings['migrator_tables']
        if not migrator_tables:
            self.config_parser.print_log_message('ERROR', "ibm_db2_zos_connector: parse_ddl_files: migrator_tables not found in settings.")
            return

        for filepath in self.ddl_files:
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: parse_ddl_files: Processing file: {filepath}")
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()

            # Extract triggers first to avoid splitting by semicolons inside their bodies
            # A trigger ends at the '@' statement terminator (DDL files with compound statement
            # bodies switch the terminator to '@' because ';' separates the statements of the
            # body), otherwise at the next object or at the end of the file.
            trigger_pattern = re.compile(
                r"(CREATE\s+TRIGGER\s+\"?([A-Za-z0-9_$#@]+)\"?\.\"?([A-Za-z0-9_$#@]+)\"?"
                r"[\s\S]*?)"
                r"(?:@[^\S\n]*(?=\n|$)"
                r"|(?=(?:CREATE\s+(?:TABLE|VIEW|INDEX|UNIQUE\s+INDEX|ALIAS|SEQUENCE|TRIGGER))"
                r"|(?:ALTER\s+TABLE)|(?:SET\s+CURRENT\s+SCHEMA)|$))", re.IGNORECASE)
            for match in trigger_pattern.finditer(content):
                self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: parse_ddl_files: Found trigger: {match.group(1)}")
                schema_name = match.group(2).upper()
                trigger_name = match.group(3).upper()
                ddl_text = match.group(1).strip()
                # a trigger not closed by '@' reaches up to the next object - the comment lines
                # in front of that object are not part of the trigger
                ddl_lines = ddl_text.split('\n')
                while ddl_lines and (not ddl_lines[-1].strip() or ddl_lines[-1].strip().startswith('--')):
                    ddl_lines.pop()
                ddl_text = '\n'.join(ddl_lines).strip()
                migrator_tables.insert_ddl_triggers({
                    'source_schema_name': schema_name,
                    'source_trigger_name': trigger_name,
                    'source_ddl_text': ddl_text,
                    'source_trigger_sql': ddl_text,
                    'source_trigger_comment': None
                })

            # Remove the extracted triggers from content so they aren't parsed again
            content = trigger_pattern.sub("", content)

            # Functions and procedures are extracted the same way - a compound body contains
            # semicolons, so such a DDL file switches the statement terminator to '@'
            funcproc_pattern = re.compile(
                r"(CREATE\s+(?:OR\s+REPLACE\s+)?(FUNCTION|PROCEDURE)\s+\"?([A-Za-z0-9_$#@]+)\"?\.\"?([A-Za-z0-9_$#@]+)\"?"
                r"[\s\S]*?)"
                r"(?:@[^\S\n]*(?=\n|$)"
                r"|(?=(?:CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW|INDEX|ALIAS|SEQUENCE|TRIGGER|FUNCTION|PROCEDURE))"
                r"|(?:ALTER\s+(?:TABLE|PROCEDURE|FUNCTION))|(?:SET\s+CURRENT\s+SQLID)|$))", re.IGNORECASE)
            for match in funcproc_pattern.finditer(content):
                funcproc_type = match.group(2).upper()
                funcproc_schema = match.group(3).upper()
                funcproc_name = match.group(4).upper()
                ddl_text = match.group(1).strip()
                ddl_lines = ddl_text.split('\n')
                while ddl_lines and (not ddl_lines[-1].strip() or ddl_lines[-1].strip().startswith('--')):
                    ddl_lines.pop()
                ddl_text = '\n'.join(ddl_lines).strip()
                self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: parse_ddl_files: Found {funcproc_type}: {funcproc_name}")
                migrator_tables.insert_ddl_funcprocs({
                    'source_schema_name': funcproc_schema,
                    'source_funcproc_name': funcproc_name,
                    'source_funcproc_type': funcproc_type,
                    'source_ddl_text': ddl_text,
                    'source_funcproc_comment': None
                })

            content = funcproc_pattern.sub("", content)

            # A routine can carry several versions, the dump contains all of them but only the
            # active one is in effect - an additional version cannot be migrated as its own object
            for match in re.finditer(r"(?i)ALTER\s+(FUNCTION|PROCEDURE)\s+\"?([A-Za-z0-9_$#@]+)\"?\.\"?([A-Za-z0-9_$#@]+)\"?\s+ADD\s+VERSION\s+([A-Za-z0-9_$#@]+)", content):
                self.config_parser.print_log_message('WARNING', f"ibm_db2_zos_connector: parse_ddl_files: {match.group(1)} {match.group(2)}.{match.group(3)} has an additional version {match.group(4)} which is not migrated - only the version of the CREATE statement is used.")

            # Split statements by ';' or by '@' at the end of a line, but not inside comments
            # and string literals
            statements = self.split_sql_statements(content)
            for stmt in statements:
                stmt = stmt.strip()
                if not stmt:
                    continue

                # Extract inline comments and clean statement for regex matching
                comment_lines = []
                clean_stmt_lines = []
                for line in stmt.split('\n'):
                    stripped_line = line.strip()
                    if stripped_line.startswith('--'):
                        comment_lines.append(stripped_line[2:].strip())
                    else:
                        clean_stmt_lines.append(line)

                clean_stmt = '\n'.join(clean_stmt_lines).strip()
                comment_text = '\n'.join(comment_lines).strip() if comment_lines else None

                if not clean_stmt:
                    continue

                # Parse Comments (COMMENT ON statements)
                match_comment = re.search(r"^COMMENT\s+ON\s+(TABLE|COLUMN|INDEX|VIEW|ALIAS|TRIGGER|SEQUENCE)\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?(?:\.\"?([A-Za-z0-9_]+)\"?)?\s+IS\s+'(.*)'", clean_stmt, re.IGNORECASE | re.DOTALL)
                if match_comment:
                    self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: parse_ddl_files: Found comment: {match_comment.group(1)}")
                    obj_type = match_comment.group(1).upper()
                    schema_name = match_comment.group(2).upper()
                    obj_name = match_comment.group(3).upper()
                    col_name = match_comment.group(4).upper() if match_comment.group(4) else None
                    comment_val = match_comment.group(5)

                    migrator_tables.update_ddl_comment({
                        'object_type': obj_type,
                        'source_schema_name': schema_name,
                        'source_name': obj_name,
                        'source_column_name': col_name,
                        'comment': comment_val
                    })
                    continue

                # Parse Indexes
                match_index = re.search(r"^CREATE\s+(UNIQUE\s+)?INDEX\s+([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\s+ON\s+([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)", clean_stmt, re.IGNORECASE)
                if match_index:
                    self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: parse_ddl_files: Found index: {match_index.group(1)}")
                    is_unique = bool(match_index.group(1))
                    idx_schema = match_index.group(2).upper()
                    idx_name = match_index.group(3).upper()
                    tbl_schema = match_index.group(4).upper()
                    tbl_name = match_index.group(5).upper()

                    # Fetch columns list in parenthesis
                    start_idx = stmt.find('(', match_index.end())
                    if start_idx != -1:
                        depth = 0
                        end_idx = -1
                        for i in range(start_idx, len(stmt)):
                            if stmt[i] == '(':
                                depth += 1
                            elif stmt[i] == ')':
                                depth -= 1
                                if depth == 0:
                                    end_idx = i
                                    break

                        if end_idx != -1:
                            cols_list = []
                            is_function_based = False
                            for col_entry in self.split_top_level_commas(stmt[start_idx+1:end_idx]):
                                # an ordering keyword is dropped, PostgreSQL defaults to ASC and
                                # DB2 sorts NULL values as the largest ones just like PostgreSQL
                                col_entry = re.sub(r"(?i)\s+(ASC|DESC)\s*$", "", col_entry).strip()
                                if not col_entry:
                                    continue
                                if '(' in col_entry:
                                    # an expression (e.g. UPPER(EMAIL)) has to be handed over as
                                    # such, otherwise the target quotes it as a column name
                                    is_function_based = True
                                    cols_list.append(col_entry)
                                else:
                                    cols_list.append(col_entry.strip('"').upper())

                            migrator_tables.insert_ddl_indexes({
                                'source_schema_name': tbl_schema,
                                'source_table_name': tbl_name,
                                'source_index_name': idx_name,
                                'source_is_unique': is_unique,
                                'source_columns_list': ', '.join(cols_list),
                                'source_index_sql': stmt,
                                'source_index_comment': comment_text,
                                'source_is_function_based': is_function_based
                            })
                    continue

                # Parse Global Variables (CREATE VARIABLE <schema>.<name> <type> DEFAULT <value>)
                match_variable = re.search(
                    r"^CREATE\s+(?:OR\s+REPLACE\s+)?VARIABLE\s+\"?([A-Za-z0-9_$#@]+)\"?\.\"?([A-Za-z0-9_$#@]+)\"?"
                    r"\s+([A-Za-z0-9_]+(?:\s*\([^)]*\))?)"
                    r"(?:\s+DEFAULT\s+(.+?))?\s*$", clean_stmt, re.IGNORECASE | re.DOTALL)
                if match_variable:
                    self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: parse_ddl_files: Found global variable: {match_variable.group(2)}")
                    migrator_tables.insert_ddl_variables({
                        'source_schema_name': match_variable.group(1).upper(),
                        'source_variable_name': match_variable.group(2).upper(),
                        'source_data_type': match_variable.group(3).strip().upper(),
                        'source_default_value': match_variable.group(4).strip() if match_variable.group(4) else None,
                        'source_variable_sql': stmt,
                        'source_variable_comment': comment_text
                    })
                    continue

                # Parse Sequences
                match_seq = re.search(r"^CREATE\s+SEQUENCE\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?", clean_stmt, re.IGNORECASE)
                if match_seq:
                    self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: parse_ddl_files: Found sequence: {match_seq.group(1)}")
                    schema_name = match_seq.group(1).upper()
                    seq_name = match_seq.group(2).upper()

                    # Rebuild Sequence SQL Statement based on DB2 properties for PostgreSQL compatibility
                    seq_sql = f'CREATE SEQUENCE "{schema_name.lower()}"."{seq_name.lower()}"'
                    seq_params_str = clean_stmt.upper()

                    # Individual parameter tracking
                    parsed_start = None
                    parsed_increment = None
                    parsed_minvalue = None
                    parsed_maxvalue = None
                    parsed_cache = None
                    parsed_cycle = False

                    start_with_match = re.search(r"START\s+WITH\s+(-?\d+)", seq_params_str)
                    if start_with_match:
                        parsed_start = int(start_with_match.group(1))
                        seq_sql += f" START WITH {parsed_start}"

                    increment_by_match = re.search(r"INCREMENT\s+BY\s+(-?\d+)", seq_params_str)
                    if increment_by_match:
                        parsed_increment = int(increment_by_match.group(1))
                        seq_sql += f" INCREMENT BY {parsed_increment}"

                    minvalue_match = re.search(r"MINVALUE\s+(-?\d+)", seq_params_str)
                    if minvalue_match:
                        parsed_minvalue = int(minvalue_match.group(1))
                        seq_sql += f" MINVALUE {parsed_minvalue}"
                    elif "NO MINVALUE" in seq_params_str:
                        seq_sql += " NO MINVALUE"

                    maxvalue_match = re.search(r"MAXVALUE\s+(-?\d+)", seq_params_str)
                    if maxvalue_match:
                        parsed_maxvalue = int(maxvalue_match.group(1))
                        seq_sql += f" MAXVALUE {parsed_maxvalue}"
                    elif "NO MAXVALUE" in seq_params_str:
                        seq_sql += " NO MAXVALUE"

                    if "CACHE" in seq_params_str and "NO CACHE" in seq_params_str:
                        seq_sql += " CACHE 1" # Disable caching
                    else:
                        cache_match = re.search(r"CACHE\s+(\d+)", seq_params_str)
                        if cache_match:
                            parsed_cache = int(cache_match.group(1))
                            seq_sql += f" CACHE {parsed_cache}"

                    if "CYCLE" in seq_params_str and "NO CYCLE" not in seq_params_str:
                        parsed_cycle = True
                        seq_sql += " CYCLE"

                    migrator_tables.insert_ddl_sequences({
                        'source_schema_name': schema_name,
                        'source_seq_name': seq_name,
                        'source_table_name': None,
                        'source_column_name': None,
                        'source_start_value': parsed_start,
                        'source_increment_by': parsed_increment,
                        'source_minvalue': parsed_minvalue,
                        'source_maxvalue': parsed_maxvalue,
                        'source_cache': parsed_cache,
                        'source_is_cycled': parsed_cycle,
                        'source_ddl_text': seq_sql,
                        'source_seq_comment': comment_text
                    })
                    continue

                # Parse Views
                match_view = re.search(r"^CREATE\s+VIEW\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?", clean_stmt, re.IGNORECASE)
                if match_view:
                    self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: parse_ddl_files: Found view: {match_view.group(1)}")
                    schema_name = match_view.group(1).upper()
                    view_name = match_view.group(2).upper()
                    migrator_tables.insert_ddl_views({
                        'source_schema_name': schema_name,
                        'source_view_name': view_name,
                        'source_view_sql': stmt,
                        'source_view_comment': comment_text
                    })
                    continue

                # Parse Aliases
                match_alias = re.search(r"^CREATE\s+ALIAS\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?\s+FOR\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?", clean_stmt, re.IGNORECASE)
                if match_alias:
                    self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: parse_ddl_files: Found alias: {match_alias.group(1)}")
                    schema_name = match_alias.group(1).upper()
                    alias_name = match_alias.group(2).upper()
                    target_schema = match_alias.group(3).upper()
                    target_name = match_alias.group(4).upper()
                    migrator_tables.insert_ddl_aliases({
                        'source_schema_name': schema_name,
                        'source_alias_name': alias_name,
                        'source_target_schema': target_schema,
                        'source_target_name': target_name,
                        'source_alias_sql': stmt,
                        'source_alias_comment': comment_text
                    })
                    continue

                # Parse Foreign Keys
                match_fk = re.search(r"^ALTER\s+TABLE\s+([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\s+ADD\s+CONSTRAINT\s+([A-Za-z0-9_]+)\s+FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\s*\(([^)]+)\)", clean_stmt, re.IGNORECASE)
                if match_fk:
                    self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: parse_ddl_files: Found foreign key: {match_fk.group(1)}")
                    tbl_schema = match_fk.group(1).upper()
                    tbl_name = match_fk.group(2).upper()
                    fk_name = match_fk.group(3).upper()
                    cols_str = match_fk.group(4)
                    ref_schema = match_fk.group(5).upper()
                    ref_name = match_fk.group(6).upper()
                    ref_cols_str = match_fk.group(7)

                    cols_list = [c.strip().upper() for c in cols_str.split(',')]
                    ref_cols_list = [c.strip().upper() for c in ref_cols_str.split(',')]

                    migrator_tables.insert_ddl_foreign_keys({
                        'source_schema_name': tbl_schema,
                        'source_table_name': tbl_name,
                        'source_fk_name': fk_name,
                        'source_columns_list': ', '.join(cols_list),
                        'source_ref_schema_name': ref_schema,
                        'source_ref_table_name': ref_name,
                        'source_ref_columns_list': ', '.join(ref_cols_list),
                        'source_fk_sql': stmt,
                        'source_fk_comment': comment_text
                    })
                    continue

                # Find CREATE TABLE
                match_table = re.search(r"^CREATE\s+TABLE\s+([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)", clean_stmt, re.IGNORECASE)
                if not match_table:
                    continue

                schema_name = match_table.group(1).upper()
                table_name = match_table.group(2).upper()

                # Materialized query tables (CREATE TABLE ... AS (<query>) DATA INITIALLY DEFERRED ...)
                # are stored as views - they are migrated as PostgreSQL materialized views,
                # their body is a query and not a list of column definitions.
                if re.search(r"(?i)\bDATA\s+INITIALLY\s+DEFERRED\b", clean_stmt):
                    self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: parse_ddl_files: Found materialized query table: {table_name}")
                    migrator_tables.insert_ddl_views({
                        'source_schema_name': schema_name,
                        'source_view_name': table_name,
                        'source_view_sql': clean_stmt,
                        'source_view_comment': comment_text,
                        'source_view_type': 'MATERIALIZED VIEW'
                    })
                    continue

                # Extract block inside parenthesis
                start_idx = clean_stmt.find('(', match_table.end())
                if start_idx == -1:
                    continue

                depth = 0
                end_idx = -1
                for i in range(start_idx, len(clean_stmt)):
                    if clean_stmt[i] == '(':
                        depth += 1
                    elif clean_stmt[i] == ')':
                        depth -= 1
                        if depth == 0:
                            end_idx = i
                            break

                if end_idx == -1:
                    continue

                columns_str = clean_stmt[start_idx+1:end_idx]
                # Split on the commas outside of parentheses and string literals, so that a
                # constraint like CHECK (STATUS IN ('A','D')) stays in one piece
                col_defs = self.split_top_level_commas(columns_str)

                # Extract Partitioning parameters from the trailing text
                trailing_str = clean_stmt[end_idx+1:]
                partition_col = None
                partition_ranges = None

                match_part = re.search(r"PARTITION\s+BY\s*\(\s*([^)]+)\s*\)\s*\(([\s\S]*?)\)\s*(?:IN|;|$)", trailing_str, re.IGNORECASE)
                if match_part:
                    partition_col = match_part.group(1).replace(" ASC", "").replace(" DESC", "").strip()
                    partition_ranges = match_part.group(2).strip()

                # Register Table
                self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: parse_ddl_files: Found table: {match_table.group(1)}")
                migrator_tables.insert_ddl_tables({
                    'source_schema_name': schema_name,
                    'source_table_name': table_name,
                    'source_partition_columns': partition_col,
                    'source_partition_ranges': partition_ranges,
                    'source_table_sql': stmt,
                    'source_table_comment': comment_text
                })

                # Table level constraints - DB2 declares primary keys, unique constraints,
                # foreign keys and check constraints inside the CREATE TABLE, usually named
                # ("CONSTRAINT PK_REGIONS PRIMARY KEY (REGION_ID)"). comment_text describes the
                # table and is not the comment of the individual constraints.
                pk_columns = set()
                for col_def in col_defs:
                    if not re.match(r"(?i)^(CONSTRAINT\s|PRIMARY\s+KEY\b|FOREIGN\s+KEY\b|UNIQUE\b|CHECK\b)", col_def):
                        continue
                    pk_columns.update(self.parse_table_constraint(col_def, {
                        'migrator_tables': migrator_tables,
                        'schema_name': schema_name,
                        'table_name': table_name,
                        'comment_text': None
                    }))

                # Second pass for extracting column metrics
                for col_def in col_defs:
                    col_def_u = col_def.upper()
                    if col_def_u.startswith("PRIMARY KEY") or col_def_u.startswith("CONSTRAINT") or col_def_u.startswith("FOREIGN KEY") or col_def_u.startswith("UNIQUE") or col_def_u.startswith("CHECK") or col_def_u.startswith("PERIOD"):
                        continue

                    parts = col_def.split(maxsplit=1)
                    col_name = parts[0].upper()
                    rest = parts[1] if len(parts) > 1 else ""

                    # Exclude any other definition that is not a column
                    if len(parts) < 2:
                        continue

                    type_match = re.match(r"([A-Za-z0-9_]+(?:\s*\([^)]+\))?)", rest, re.IGNORECASE)
                    if not type_match:
                        print(f"Failed to parse type for column {col_name} on {table_name}: {rest}")
                        continue

                    data_type = type_match.group(1).upper()
                    after_type = rest[len(data_type):].strip()

                    is_nullable = True
                    if "NOT NULL" in after_type.upper():
                        is_nullable = False

                    is_identity = False
                    default_value = None

                    # Check for Identity Column definition
                    identity_match = re.search(r"GOVERNING\s+AS\s+IDENTITY|AS\s+IDENTITY\s*\(([^)]+)\)", after_type, re.IGNORECASE)
                    if not identity_match:
                        identity_match = re.search(r"GENERATED\s+(?:ALWAYS|BY\s+DEFAULT)\s+AS\s+IDENTITY(?:\s*\(([^)]+)\))?", after_type, re.IGNORECASE)

                    if identity_match:
                        self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: parse_ddl_files: Found identity: {identity_match.group(1)}")
                        is_identity = True
                        seq_params_str = identity_match.group(1) if identity_match.lastindex and identity_match.group(identity_match.lastindex) else ""

                        # Set default PostgreSQL Sequence generator mapping
                        seq_name = f"{table_name}_{col_name}_seq".lower()
                        default_value = f"nextval('\"{schema_name.lower()}\".\"{seq_name}\"')"

                        # Rebuild Sequence SQL Statement based on DB2 properties
                        seq_sql = f'CREATE SEQUENCE "{schema_name.lower()}"."{seq_name}"'

                        # Individual parameter tracking
                        parsed_start = None
                        parsed_increment = None
                        parsed_minvalue = None
                        parsed_maxvalue = None
                        parsed_cache = None
                        parsed_cycle = False

                        if seq_params_str:
                            seq_params_str = seq_params_str.upper()

                            start_with_match = re.search(r"START\s+WITH\s+(-?\d+)", seq_params_str)
                            if start_with_match:
                                parsed_start = int(start_with_match.group(1))
                                seq_sql += f" START WITH {parsed_start}"

                            increment_by_match = re.search(r"INCREMENT\s+BY\s+(-?\d+)", seq_params_str)
                            if increment_by_match:
                                parsed_increment = int(increment_by_match.group(1))
                                seq_sql += f" INCREMENT BY {parsed_increment}"

                            minvalue_match = re.search(r"MINVALUE\s+(-?\d+)", seq_params_str)
                            if minvalue_match:
                                parsed_minvalue = int(minvalue_match.group(1))
                                seq_sql += f" MINVALUE {parsed_minvalue}"
                            elif "NO MINVALUE" in seq_params_str:
                                seq_sql += " NO MINVALUE"

                            maxvalue_match = re.search(r"MAXVALUE\s+(-?\d+)", seq_params_str)
                            if maxvalue_match:
                                parsed_maxvalue = int(maxvalue_match.group(1))
                                seq_sql += f" MAXVALUE {parsed_maxvalue}"
                            elif "NO MAXVALUE" in seq_params_str:
                                seq_sql += " NO MAXVALUE"

                            if "CACHE" in seq_params_str and "NO CACHE" in seq_params_str:
                                seq_sql += " CACHE 1" # Disable caching
                            else:
                                cache_match = re.search(r"CACHE\s+(\d+)", seq_params_str)
                                if cache_match:
                                    parsed_cache = int(cache_match.group(1))
                                    seq_sql += f" CACHE {parsed_cache}"

                            if "CYCLE" in seq_params_str and "NO CYCLE" not in seq_params_str:
                                parsed_cycle = True
                                seq_sql += " CYCLE"

                        self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: parse_ddl_files: Found sequence: {seq_name.upper()}")
                        migrator_tables.insert_ddl_sequences({
                            'source_schema_name': schema_name,
                            'source_seq_name': seq_name.upper(),
                            'source_table_name': table_name,
                            'source_column_name': col_name,
                            'source_start_value': parsed_start,
                            'source_increment_by': parsed_increment,
                            'source_minvalue': parsed_minvalue,
                            'source_maxvalue': parsed_maxvalue,
                            'source_cache': parsed_cache,
                            'source_is_cycled': parsed_cycle,
                            'source_ddl_text': seq_sql,
                            'source_seq_comment': f"Auto-generated sequence for identity column {table_name}.{col_name}"
                        })
                    else:
                        default_match = re.search(r"WITH\s+DEFAULT(?:\s+('[^']*'|-?[0-9\.]+|[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?))?", after_type, re.IGNORECASE)
                        if default_match:
                            val = default_match.group(1)
                            if val is None or val.upper() in ('NOT NULL', 'GENERATED', 'CONSTRAINT'):
                                default_value = "SYSTEM DEFAULT"
                            else:
                                default_value = val

                    is_pk = col_name in pk_columns

                    # Column sql is the column definition
                    migrator_tables.insert_ddl_columns({
                        'source_schema_name': schema_name,
                        'source_table_name': table_name,
                        'source_column_name': col_name,
                        'source_data_type': data_type,
                        'source_is_nullable': is_nullable,
                        'source_default_value': default_value,
                        'source_pk_indicator': is_pk,
                        'source_column_sql': col_def,
                        'source_column_comment': None,
                        'source_is_identity': is_identity
                    })

        cursor = migrator_tables.protocol_connection.connection.cursor()
        cursor.execute(f'SELECT source_schema_name FROM "{migrator_tables.protocol_schema}"."ddl_tables" WHERE source_schema_name IS NOT NULL')
        schemas = [row[0] for row in cursor.fetchall()]
        cursor.close()
        self.config_parser.print_log_message('DEBUG3', f'ibm_db2_zos_connector: parse_ddl_files: found schemas: {schemas}')

        if schemas:
            most_frequent_schema = max(set(schemas), key=schemas.count)
            self.config_parser.print_log_message('DEBUG3', f'ibm_db2_zos_connector: parse_ddl_files: setting schema: {most_frequent_schema}')
            self.config_parser.set_source_schema(most_frequent_schema)

        self.config_parser.print_log_message('INFO', "ibm_db2_zos_connector: parse_ddl_files: DDL parsing completed and unified protocol tables populated with DB2 source metadata.")


    def get_sql_functions_mapping(self, settings):
        target_db_type = settings['target_db_type']
        if target_db_type == 'postgresql':
            return {
                # --- Special Registers (Session Variables) ---
                "CURRENT SQLID": "CURRENT_USER",
                "CURRENT USER": "CURRENT_USER",
                "USER": "SESSION_USER",          # SESSION_USER tracks the original login role
                "CURRENT DATE": "CURRENT_DATE",
                "CURRENT TIME": "CURRENT_TIME",
                "CURRENT TIMESTAMP": "CURRENT_TIMESTAMP",
                "CURRENT SCHEMA": "CURRENT_SCHEMA",
                "CURRENT SERVER": "current_database()",

                # --- Null Handling & Control Flow ---
                "VALUE(": "COALESCE(",
                "IFNULL(": "COALESCE(",
                "NVL(": "COALESCE(",
                ## "DECODE(expr, search, result, default)": "CASE expr WHEN search THEN result ELSE default END",

                # --- String Functions ---
                "SUBSTR(": "SUBSTRING(",
                "POSSTR(": "STRPOS(",       # DB2's POSSTR takes (source, search)
                "LOCATE(": "POSITION(", # DB2's LOCATE takes (search, source)
                "UCASE(": "UPPER(",
                "LCASE(": "LOWER(",
                "STRIP(": "TRIM(",
                "LENGTH(": "LENGTH(",
                "CONCAT(": "CONCAT(",                 # Or simply use the str1 || str2 operator

                # --- Date and Time Functions ---
                "YEAR(": "EXTRACT(YEAR FROM ",
                "MONTH(": "EXTRACT(MONTH FROM ",
                "DAY(": "EXTRACT(DAY FROM ",
                "HOUR(": "EXTRACT(HOUR FROM ",
                "MINUTE(": "EXTRACT(MINUTE FROM ",
                "SECOND(": "EXTRACT(SECOND FROM ",

                # Db2 DAYS() returns the integer number of days since Jan 1, 0001.
                # To replicate this exact integer in Postgres, you subtract that date from your column.
                ## "DAYS(date_col)": "(date_col::DATE - '0001-01-01'::DATE)",

                # "DATE(expr)": "expr::DATE",                                 # Or CAST(expr AS DATE)
                # "TIMESTAMP(expr)": "expr::TIMESTAMP",                       # Or CAST(expr AS TIMESTAMP)
                # "ADD_DAYS(date_col, n)": "date_col + (n || ' days')::INTERVAL",
                # "ADD_MONTHS(date_col, n)": "date_col + (n || ' months')::INTERVAL",

                # --- Math & Numeric Functions ---
                "CEILING(": "CEIL(",
                "TRUNCATE(": "TRUNC(",
                "RAND()": "RANDOM()",
                "DECFLOAT(": "num::NUMERIC",                            # PostgreSQL uses NUMERIC for arbitrary precision
            }
        else:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_zos_connector: get_sql_functions_mapping: Unsupported target database type: {target_db_type}")
            return {}

    def fetch_table_names(self, table_schema: str):
        return self.fetch_all_tables(table_schema)

    def get_table_description(self, settings) -> dict:
        return {}

    def fetch_default_values(self, settings) -> dict:
        return {}

    def is_string_type(self, column_type: str) -> bool:
        string_types = ['CHAR', 'VARCHAR', 'NCHAR', 'NVARCHAR', 'TEXT', 'LONG VARCHAR', 'LONG NVARCHAR', 'UNICHAR', 'UNIVARCHAR']
        return column_type.upper() in string_types

    def is_numeric_type(self, column_type: str) -> bool:
        numeric_types = ['BIGINT', 'INTEGER', 'INT', 'TINYINT', 'SMALLINT', 'FLOAT', 'DOUBLE PRECISION', 'DECIMAL', 'NUMERIC']
        return column_type.upper() in numeric_types

    def get_create_table_sql(self, settings):
        pass

    def migrate_table(self, migrate_target_connection, settings):
        return {'finished': True, 'rows_migrated': 0, 'source_table_rows_limited': 0, 'target_table_rows': 0, 'chunk_number': 1, 'total_chunks': 1}

    def fetch_indexes(self, settings):
        table_schema = settings.get('source_table_schema')
        table_name = settings.get('source_table_name')
        indexes = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            # Find Primary Key columns
            pk_query = f"""SELECT source_column_name FROM "{self.protocol_schema}"."ddl_columns"
                           WHERE source_schema_name = %s AND source_table_name = %s AND source_pk_indicator = TRUE ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(pk_query, (table_schema, table_name))
            pk_cols = [row[0] for row in cursor.fetchall()]

            order_num = 1
            pk_cols_set = set()
            if pk_cols:
                pk_cols_str = ', '.join(pk_cols)
                pk_name = f"{table_name}_PK" # Synthetic name for the primary key
                indexes[order_num] = {
                    'index_name': pk_name,
                    'index_type': 'PRIMARY KEY',
                    'index_owner': table_schema,
                    'index_columns': pk_cols_str,
                    'index_comment': None,
                    'index_sql': None,
                    'is_function_based': 'NO'
                }
                order_num += 1
                pk_cols_set = set(c.upper() for c in pk_cols)

            query = f"""SELECT source_index_name, source_is_unique, source_columns_list, source_is_function_based
                        FROM "{self.protocol_schema}"."ddl_indexes"
                        WHERE source_schema_name = %s AND source_table_name = %s ORDER BY id"""
            cursor.execute(query, (table_schema, table_name))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: fetch_indexes: ({table_schema}.{table_name}): {rows}")
            for row in rows:
                idx_name = row[0]
                is_unique = row[1]
                cols = row[2]
                is_function_based = row[3]

                # Check if this unique index is effectively the primary key backing index
                idx_cols_set = set(c.strip().upper() for c in cols.split(',')) if cols else set()
                if pk_cols_set and is_unique and not is_function_based and pk_cols_set == idx_cols_set:
                    self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: fetch_indexes: Skipping index {idx_name} as it matches primary key columns.")
                    continue

                indexes[order_num] = {
                    'index_name': idx_name,
                    'index_type': 'UNIQUE' if is_unique else 'INDEX',
                    'index_owner': table_schema,
                    'index_columns': cols,
                    'index_comment': None,
                    'index_sql': None,
                    'is_function_based': 'YES' if is_function_based else 'NO'
                }
                order_num += 1
            cursor.close()
        return indexes

    def get_create_index_sql(self, settings):
        pass

    def fetch_constraints(self, settings):
        table_schema = settings.get('source_table_schema')
        table_name = settings.get('source_table_name')
        constraints = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT source_fk_name, source_columns_list, source_ref_schema_name, source_ref_table_name, source_ref_columns_list,
                               source_constraint_type, source_check_clause, source_delete_rule, source_update_rule
                        FROM "{self.protocol_schema}"."ddl_foreign_keys"
                        WHERE source_schema_name = %s AND source_table_name = %s ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (table_schema, table_name))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: fetch_constraints: ({table_schema}.{table_name}): {rows}")
            for i, row in enumerate(rows, 1):
                raw_ref_cols = row[4]
                if raw_ref_cols:
                    ref_cols_list = [c.strip() for c in raw_ref_cols.split(',')]
                    # Deduplicate preserving order
                    seen = set()
                    deduped_ref_cols = [x for x in ref_cols_list if not (x in seen or seen.add(x))]
                    referenced_columns = ', '.join(deduped_ref_cols)
                else:
                    referenced_columns = raw_ref_cols

                raw_constraint_cols = row[1]
                if raw_constraint_cols:
                    constraint_cols_list = [c.strip() for c in raw_constraint_cols.split(',')]
                    # Deduplicate preserving order
                    seen = set()
                    deduped_constraint_cols = [x for x in constraint_cols_list if not (x in seen or seen.add(x))]
                    constraint_columns = ', '.join(deduped_constraint_cols)
                else:
                    constraint_columns = raw_constraint_cols

                constraint_type = row[5] or 'FOREIGN KEY'
                constraints[i] = {
                    'constraint_name': row[0],
                    'constraint_type': constraint_type,
                    'constraint_owner': table_schema,
                    'constraint_columns': constraint_columns,
                    'referenced_table_schema': row[2],
                    'referenced_table_name': row[3],
                    'referenced_columns': referenced_columns,
                    # the target connector wraps the clause of a check constraint in CHECK (...)
                    'constraint_sql': row[6] if constraint_type == 'CHECK' else None,
                    'delete_rule': row[7] or 'NO ACTION',
                    'update_rule': row[8] or 'NO ACTION',
                    'constraint_comment': None,
                    'constraint_status': 'ENABLED'
                }
            cursor.close()
        return constraints

    def get_create_constraint_sql(self, settings):
        pass

    def fetch_triggers(self, table_id: int, table_schema: str, table_name: str):
        triggers = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT id, source_trigger_name, source_ddl_text
                        FROM "{self.protocol_schema}"."ddl_triggers"
                        WHERE source_schema_name = %s ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (table_schema,))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: fetch_triggers: ({table_schema}): {rows}")
            order_num = 1
            for row in rows:
                # Triggers are stored per schema - keep only those defined ON the requested
                # table. The table is taken from the trigger definition, a plain substring
                # search would also match every other table named in the trigger body.
                if table_name and row[2]:
                    match_on = re.search(r"(?i)\bON\s+(?:\"?[A-Za-z0-9_$#@]+\"?\.)?\"?([A-Za-z0-9_$#@]+)\"?", row[2])
                    if not match_on or match_on.group(1).upper() != table_name.upper():
                        continue
                triggers[order_num] = {
                    'id': row[0],
                    'name': row[1],
                    'event': 'UNKNOWN',
                    'new': None,
                    'old': None,
                    'sql': row[2],
                    'comment': None
                }
                order_num += 1
            cursor.close()
        return triggers

    def split_sql_statements(self, content: str) -> list:
        """
        Splits the content of a DDL file into single statements. Statement terminators inside
        string literals, line comments and block comments are ignored - DDL exports regularly
        contain prose comments with a semicolon in them ("... unloaded HEX-converted; NOTES"),
        which would otherwise cut the following statement in half. Comments are kept in the
        returned statements, the caller extracts the object comments from them.
        """
        statements = []
        current = []
        in_literal = False
        in_line_comment = False
        in_block_comment = False
        length = len(content)
        i = 0
        while i < length:
            char = content[i]
            next_char = content[i+1] if i + 1 < length else ''

            if in_line_comment:
                current.append(char)
                if char == '\n':
                    in_line_comment = False
            elif in_block_comment:
                current.append(char)
                if char == '*' and next_char == '/':
                    current.append(next_char)
                    i += 1
                    in_block_comment = False
            elif in_literal:
                current.append(char)
                if char == "'":
                    # '' inside a literal is an escaped quote, not its end
                    if next_char == "'":
                        current.append(next_char)
                        i += 1
                    else:
                        in_literal = False
            elif char == '-' and next_char == '-':
                current.append(char)
                current.append(next_char)
                i += 1
                in_line_comment = True
            elif char == '/' and next_char == '*':
                current.append(char)
                current.append(next_char)
                i += 1
                in_block_comment = True
            elif char == "'":
                in_literal = True
                current.append(char)
            elif char == ';':
                statements.append("".join(current))
                current = []
            elif char == '@':
                # '@' terminates a statement only when it stands alone at the end of a line
                j = i + 1
                while j < length and content[j] in ' \t\r':
                    j += 1
                if j >= length or content[j] == '\n':
                    statements.append("".join(current))
                    current = []
                else:
                    current.append(char)
            else:
                current.append(char)
            i += 1

        if current:
            statements.append("".join(current))
        return statements

    def parse_table_constraint(self, constraint_def: str, settings: dict) -> list:
        """
        Parses one table level constraint of a CREATE TABLE statement. DB2 allows all of them
        to be named, so the plain and the named form are handled alike:
            [CONSTRAINT <name>] PRIMARY KEY (<columns>)
            [CONSTRAINT <name>] UNIQUE (<columns>)
            [CONSTRAINT <name>] FOREIGN KEY (<columns>) REFERENCES <schema>.<table> (<columns>)
                                [ON DELETE <rule>] [ON UPDATE <rule>]
            [CONSTRAINT <name>] CHECK (<expression>)
        Unique constraints are registered as unique indexes and primary keys are returned as a
        list of column names, because the target connector builds both of them from the indexes
        (`index_type` 'PRIMARY KEY' / 'UNIQUE'). Foreign key and check constraints are stored in
        the constraints protocol table. Returns the list of primary key columns (empty otherwise).
        """
        migrator_tables = settings['migrator_tables']
        schema_name = settings['schema_name']
        table_name = settings['table_name']
        comment_text = settings.get('comment_text')

        definition = constraint_def.strip()
        constraint_name = None
        match_name = re.match(r"(?i)^CONSTRAINT\s+\"?([A-Za-z0-9_$#@]+)\"?\s+(.*)$", definition, re.DOTALL)
        if match_name:
            constraint_name = match_name.group(1).upper()
            definition = match_name.group(2).strip()

        definition_upper = definition.upper()

        if definition_upper.startswith("PRIMARY KEY"):
            match_cols = re.match(r"(?i)^PRIMARY\s+KEY\s*\((.*?)\)\s*$", definition, re.DOTALL)
            if not match_cols:
                return []
            return [c.strip().strip('"').upper() for c in match_cols.group(1).split(',') if c.strip()]

        if definition_upper.startswith("UNIQUE"):
            match_cols = re.match(r"(?i)^UNIQUE\s*\((.*?)\)\s*$", definition, re.DOTALL)
            if not match_cols:
                return []
            columns_list = [c.strip().strip('"').upper() for c in match_cols.group(1).split(',') if c.strip()]
            index_name = constraint_name or f"{table_name}_{'_'.join(columns_list)}_KEY"
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: parse_table_constraint: Found unique constraint: {index_name}")
            migrator_tables.insert_ddl_indexes({
                'source_schema_name': schema_name,
                'source_table_name': table_name,
                'source_index_name': index_name,
                'source_is_unique': True,
                'source_columns_list': ', '.join(columns_list),
                'source_index_sql': constraint_def,
                'source_index_comment': comment_text
            })
            return []

        if definition_upper.startswith("FOREIGN KEY"):
            match_fk = re.match(
                r"(?i)^FOREIGN\s+KEY\s*\(([^)]*)\)\s*REFERENCES\s+(?:\"?([A-Za-z0-9_$#@]+)\"?\.)?\"?([A-Za-z0-9_$#@]+)\"?\s*(?:\(([^)]*)\))?(.*)$",
                definition, re.DOTALL)
            if not match_fk:
                return []
            columns_list = [c.strip().strip('"').upper() for c in match_fk.group(1).split(',') if c.strip()]
            ref_schema = match_fk.group(2).upper() if match_fk.group(2) else schema_name
            ref_table = match_fk.group(3).upper()
            ref_columns_list = [c.strip().strip('"').upper() for c in match_fk.group(4).split(',')] if match_fk.group(4) else []
            rules = match_fk.group(5) or ''

            delete_rule = 'NO ACTION'
            update_rule = 'NO ACTION'
            match_delete = re.search(r"(?i)\bON\s+DELETE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|NO\s+ACTION|RESTRICT)", rules)
            if match_delete:
                delete_rule = ' '.join(match_delete.group(1).upper().split())
            match_update = re.search(r"(?i)\bON\s+UPDATE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|NO\s+ACTION|RESTRICT)", rules)
            if match_update:
                update_rule = ' '.join(match_update.group(1).upper().split())

            fk_name = constraint_name or f"{table_name}_{'_'.join(columns_list)}_FKEY"
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: parse_table_constraint: Found foreign key: {fk_name}")
            migrator_tables.insert_ddl_foreign_keys({
                'source_schema_name': schema_name,
                'source_table_name': table_name,
                'source_fk_name': fk_name,
                'source_columns_list': ', '.join(columns_list),
                'source_ref_schema_name': ref_schema,
                'source_ref_table_name': ref_table,
                'source_ref_columns_list': ', '.join(ref_columns_list),
                'source_fk_sql': constraint_def,
                'source_fk_comment': comment_text,
                'source_constraint_type': 'FOREIGN KEY',
                'source_check_clause': None,
                'source_delete_rule': delete_rule,
                'source_update_rule': update_rule
            })
            return []

        if definition_upper.startswith("CHECK"):
            start_idx = definition.find('(')
            if start_idx == -1:
                return []
            depth = 0
            end_idx = -1
            for i in range(start_idx, len(definition)):
                if definition[i] == '(':
                    depth += 1
                elif definition[i] == ')':
                    depth -= 1
                    if depth == 0:
                        end_idx = i
                        break
            if end_idx == -1:
                return []
            check_name = constraint_name or f"{table_name}_CHECK"
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: parse_table_constraint: Found check constraint: {check_name}")
            migrator_tables.insert_ddl_foreign_keys({
                'source_schema_name': schema_name,
                'source_table_name': table_name,
                'source_fk_name': check_name,
                'source_columns_list': None,
                'source_ref_schema_name': None,
                'source_ref_table_name': None,
                'source_ref_columns_list': None,
                'source_fk_sql': constraint_def,
                'source_fk_comment': comment_text,
                'source_constraint_type': 'CHECK',
                'source_check_clause': definition[start_idx+1:end_idx].strip(),
                'source_delete_rule': None,
                'source_update_rule': None
            })
            return []

        return []

    def split_top_level_commas(self, text: str) -> list:
        """
        Splits on commas which are neither nested in parentheses nor placed inside a string
        literal, so that a parameter like P_NET DECIMAL(12,2) stays in one piece.
        """
        parts = []
        current = []
        depth = 0
        in_literal = False
        i = 0
        while i < len(text):
            char = text[i]
            if in_literal:
                current.append(char)
                if char == "'":
                    if i + 1 < len(text) and text[i+1] == "'":
                        current.append(text[i+1])
                        i += 1
                    else:
                        in_literal = False
            elif char == "'":
                in_literal = True
                current.append(char)
            elif char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                parts.append("".join(current).strip())
                current = []
            else:
                current.append(char)
            i += 1
        if current:
            parts.append("".join(current).strip())
        return [p for p in parts if p]

    def replace_outside_string_literals(self, code: str, pattern: str, replacement: str) -> str:
        """
        Applies re.sub only to the parts of the code which are not inside a string literal,
        so that identifiers/keywords are rewritten but the content of literals stays untouched.
        """
        if not code:
            return code
        # re.split with a capturing group returns [code, literal, code, literal, ..., code],
        # so all even indexes are the parts outside of string literals
        parts = re.split(r"('(?:[^']|'')*')", code)
        for i in range(0, len(parts), 2):
            parts[i] = re.sub(pattern, replacement, parts[i])
        return ''.join(parts)

    def convert_db2_operators(self, code: str) -> str:
        """
        Converts DB2 constructs which the SQL parser does not understand: the correlated table
        function "TABLE (SELECT ...)" becomes "LATERAL (SELECT ...)".
        """
        return self.replace_outside_string_literals(code, r"(?i)\bTABLE\s*\(\s*(SELECT\b|WITH\b)", r"LATERAL (\1")

    def convert_mqt_to_materialized_view(self, code: str) -> str:
        """
        A DB2 materialized query table (MQT) is declared as
        "CREATE TABLE <name> AS (<query>) DATA INITIALLY DEFERRED REFRESH DEFERRED ..."
        and is migrated as a PostgreSQL materialized view. The MQT specific clauses have no
        PostgreSQL counterpart. Code of other objects is returned unchanged.
        """
        if not code or not re.search(r"(?i)\bDATA\s+INITIALLY\s+DEFERRED\b", code):
            return code

        code = re.sub(
            r"(?i)\s*\b(?:DATA\s+INITIALLY\s+DEFERRED"
            r"|REFRESH\s+(?:DEFERRED|IMMEDIATE)"
            r"|(?:ENABLE|DISABLE)\s+QUERY\s+OPTIMIZATION"
            r"|MAINTAINED\s+BY\s+(?:SYSTEM|USER|FEDERATED_TOOL))\b",
            "",
            code,
        )
        # the tablespace the MQT lives in
        code = re.sub(r"(?i)\s*\bIN\s+[A-Za-z0-9_$#@]+(?:\.[A-Za-z0-9_$#@]+)?\s*;?\s*$", "", code.rstrip())
        code = re.sub(r"(?i)\bCREATE\s+TABLE\b", "CREATE MATERIALIZED VIEW", code, count=1)
        return code

    def fetch_global_variables(self, schema_name: str) -> dict:
        """
        Returns the global variables of the schema as {name: {data_type, default_value}}.
        The result is cached, it is read once per connector instance.
        """
        if getattr(self, 'global_variables_cache', None) is None:
            self.global_variables_cache = {}
        cache_key = (schema_name or '').upper()
        if cache_key in self.global_variables_cache:
            return self.global_variables_cache[cache_key]

        variables = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT source_variable_name, source_data_type, source_default_value
                        FROM "{self.protocol_schema}"."ddl_variables"
                        WHERE trim(source_schema_name) = trim(%s) ORDER BY id"""
            try:
                cursor = self.migrator_tables.protocol_connection.connection.cursor()
                cursor.execute(query, (schema_name,))
                for row in cursor.fetchall():
                    variables[row[0].upper()] = {'data_type': row[1], 'default_value': row[2]}
                cursor.close()
            except Exception as e:
                self.config_parser.print_log_message('WARNING', f"ibm_db2_zos_connector: fetch_global_variables: ({schema_name}): {e}")

        self.global_variables_cache[cache_key] = variables
        return variables

    def convert_global_variables(self, code: str, schema_name: str) -> str:
        """
        Converts references to DB2 global variables into PostgreSQL session settings, which have
        the same session scope: an assignment becomes set_config(), a read becomes
        current_setting() falling back to the default declared with the variable.
            SET MIGTEST.G_CASCADE = 1   ->  PERFORM set_config('migtest.g_cascade', (1)::text, false);
            MIGTEST.G_CASCADE           ->  COALESCE(NULLIF(current_setting('migtest.g_cascade', true), ''), '0')::INTEGER
        """
        if not code or not schema_name:
            return code

        variables = self.fetch_global_variables(schema_name)
        if not variables:
            return code

        types_mapping = self.get_types_mapping({'target_db_type': 'postgresql'})
        for variable_name, variable_info in variables.items():
            setting_name = f"{schema_name.lower()}.{variable_name.lower()}"

            source_type = (variable_info.get('data_type') or '').upper()
            base_type = source_type.split('(')[0].strip()
            target_type = types_mapping.get(base_type, 'TEXT')
            if '(' in source_type and target_type not in ('TEXT', 'BYTEA'):
                target_type += source_type[source_type.find('('):]

            default_value = variable_info.get('default_value')
            default_literal = None
            if default_value is not None:
                default_value = str(default_value).strip()
                if default_value.upper() != 'NULL':
                    if default_value.startswith("'") and default_value.endswith("'"):
                        default_literal = default_value
                    else:
                        default_literal = "'" + default_value.replace("'", "''") + "'"

            current_value = f"current_setting('{setting_name}', true)"
            if default_literal:
                read_expression = f"COALESCE(NULLIF({current_value}, ''), {default_literal})::{target_type}"
            else:
                read_expression = f"NULLIF({current_value}, '')::{target_type}"

            qualified_name = rf"{re.escape(schema_name)}\s*\.\s*{re.escape(variable_name)}"

            # an assignment first - it consumes the whole SET statement
            code = self.replace_outside_string_literals(
                code,
                rf"(?im)^([^\S\n]*)SET\s+{qualified_name}\s*=\s*([^;\n]+?)\s*;?[^\S\n]*$",
                rf"\1PERFORM set_config('{setting_name}', (\2)::text, false);")
            # everything left is a read
            code = self.replace_outside_string_literals(
                code,
                rf"(?i)(?<![A-Za-z0-9_.\"]){qualified_name}\b",
                read_expression.replace('\\', '\\\\'))

        return code

    def convert_trigger(self, settings: dict):
        trigger_sql = settings.get('trigger_sql', '')
        trigger_name = settings.get('trigger_name', '')
        source_schema_name = settings.get('source_schema_name', '')
        target_schema_name = settings.get('target_schema_name', '')
        target_table_name = settings.get('target_table_name', '')

        # Basic cleanup
        trigger_sql = re.sub(r'--([^\n]*)', r'/*\1*/', trigger_sql)

        # '@' statement terminator of the DDL file
        trigger_sql = re.sub(r"@[^\S\n]*$", "", trigger_sql.rstrip())

        # DB2 for z/OS only clauses without a PostgreSQL counterpart: the VERSION identifier of
        # an advanced (V12+) trigger, the mandatory MODE DB2SQL of a basic trigger and the
        # NO CASCADE of a BEFORE trigger, which PostgreSQL enforces on its own.
        trigger_sql = re.sub(r"(?im)^[^\S\n]*VERSION\s+[A-Za-z0-9_$#@]+[^\S\n]*$", "", trigger_sql)
        trigger_sql = re.sub(r"(?i)\s*\bMODE\s+DB2SQL\b", "", trigger_sql)
        trigger_sql = re.sub(r"(?i)\bNO\s+CASCADE\s+(?=BEFORE\b)", "", trigger_sql)

        # 1. Timing (BEFORE, AFTER, INSTEAD OF)
        timing_match = re.search(r'\b(BEFORE|AFTER|INSTEAD\s+OF)\b', trigger_sql, re.IGNORECASE)
        timing = timing_match.group(1).upper() if timing_match else 'BEFORE'

        # 2. Event - the column list of UPDATE OF ends at the ON keyword of the trigger,
        # it regularly continues on the next line
        event_text = trigger_sql[timing_match.end():] if timing_match else trigger_sql
        event_match = re.search(r'\b(INSERT|UPDATE|DELETE)(?:\s+OF\s+([\s\S]*?))?\s*\bON\b', event_text, re.IGNORECASE)
        if not event_match:
            event_match = re.search(r'\b(INSERT|UPDATE|DELETE)(?:\s+OF\s+([a-zA-Z0-9_,\s"]+))?\b', event_text, re.IGNORECASE)
        event = event_match.group(1).upper() if event_match else 'UPDATE'
        of_cols = event_match.group(2) if event_match and event_match.group(2) else None

        pg_event = event
        if of_cols and event == 'UPDATE':
            actual_cols = []
            for c in of_cols.split(','):
                c = c.strip()
                if c:
                    is_quoted = c.startswith('"') and c.endswith('"')
                    base_c = c.strip('"') if is_quoted else c.upper()
                    actual_cols.append(f'"{self.config_parser.convert_names_case(base_c)}"')
            if actual_cols:
                pg_event += f" OF {', '.join(actual_cols)}"

        # 3. Referencing Aliases
        old_alias, new_alias = 'OLD', 'NEW'
        old_table_alias, new_table_alias = None, None

        old_match = re.search(r'\bOLD\s+AS\s+([a-zA-Z0-9_]+)\b', trigger_sql, re.IGNORECASE)
        if old_match: old_alias = old_match.group(1)

        new_match = re.search(r'\bNEW\s+AS\s+([a-zA-Z0-9_]+)\b', trigger_sql, re.IGNORECASE)
        if new_match: new_alias = new_match.group(1)

        # transition tables of a statement level trigger (REFERENCING OLD/NEW TABLE AS ...)
        old_table_match = re.search(r'\bOLD\s+TABLE\s+AS\s+([a-zA-Z0-9_]+)\b', trigger_sql, re.IGNORECASE)
        if old_table_match: old_table_alias = old_table_match.group(1)

        new_table_match = re.search(r'\bNEW\s+TABLE\s+AS\s+([a-zA-Z0-9_]+)\b', trigger_sql, re.IGNORECASE)
        if new_table_match: new_table_alias = new_table_match.group(1)

        # 4. Extract WHEN and Body - everything behind FOR EACH ROW / FOR EACH STATEMENT
        for_each_match = re.search(r'\bFOR\s+EACH\s+(ROW|STATEMENT)\b', trigger_sql, re.IGNORECASE)
        for_each_scope = for_each_match.group(1).upper() if for_each_match else 'ROW'
        remainder = trigger_sql[for_each_match.end():].strip() if for_each_match else trigger_sql

        when_clause = ""
        body = ""
        if remainder.upper().startswith('WHEN'):
            when_text = remainder[4:].lstrip()
            if when_text.startswith('('):
                depth = 0
                for i, char in enumerate(when_text):
                    if char == '(': depth += 1
                    elif char == ')': depth -= 1
                    if depth == 0:
                        when_clause = when_text[1:i].strip()
                        body = when_text[i+1:].strip()
                        break
        else:
            body = remainder

        # Strip BEGIN ATOMIC / BEGIN ... END
        body = re.sub(r'(?i)^BEGIN\s+ATOMIC\s+', '', body).strip()
        body = re.sub(r'(?i)^BEGIN\s+', '', body).strip()
        body = re.sub(r'(?i)END;?\s*$', '', body).strip()

        # 5. Replacements
        def replace_aliases(text):
            if not text: return text
            if old_alias.upper() != 'OLD':
                text = re.sub(rf'\b{re.escape(old_alias)}\.', 'OLD.', text, flags=re.IGNORECASE)
            if new_alias.upper() != 'NEW':
                text = re.sub(rf'\b{re.escape(new_alias)}\.', 'NEW.', text, flags=re.IGNORECASE)

            def replace_record_field(match):
                prefix = match.group(1).upper()
                field_name = match.group(2)
                is_quoted = field_name.startswith('"') and field_name.endswith('"')
                base_field = field_name.strip('"') if is_quoted else field_name.upper()
                return f'{prefix}."{self.config_parser.convert_names_case(base_field)}"'

            text = re.sub(r'\b(OLD|NEW)\.([a-zA-Z0-9_"]+)\b', replace_record_field, text, flags=re.IGNORECASE)
            return text

        when_clause = replace_aliases(when_clause)
        body = replace_aliases(body)

        # global variables have to be resolved before the schema names are rewritten,
        # they are addressed by the source schema name as well
        body = self.convert_global_variables(body, source_schema_name)
        when_clause = self.convert_global_variables(when_clause, source_schema_name)

        # objects addressed by the source schema name have to be addressed by the target one
        if source_schema_name and target_schema_name:
            schema_pattern = rf'(?i)(?<![A-Za-z0-9_."]){re.escape(source_schema_name)}\s*\.'
            body = self.replace_outside_string_literals(body, schema_pattern, f'"{target_schema_name}".')
            when_clause = self.replace_outside_string_literals(when_clause, schema_pattern, f'"{target_schema_name}".')

        # VARCHAR(<expression>) is a conversion function in DB2 and a data type in PostgreSQL -
        # the identifiers of the expression are already quoted at this point (OLD."order_id")
        body = re.sub(r'(?i)\bVARCHAR\s*\(\s*(COUNT\s*\([^()]*\)|[a-zA-Z0-9_."]+\s*[+*/|-]\s*[^()]+|[a-zA-Z0-9_."]+\.[a-zA-Z0-9_."]+|[a-zA-Z_][a-zA-Z0-9_]*\([^()]*\))\s*\)', r'CAST(\1 AS VARCHAR)', body)

        # Replace CURRENT DATE / TIMESTAMP
        body = re.sub(r'\bCURRENT\s+DATE\b', 'CURRENT_DATE', body, flags=re.IGNORECASE)
        body = re.sub(r'\bCURRENT\s+TIMESTAMP\b', 'CURRENT_TIMESTAMP', body, flags=re.IGNORECASE)
        when_clause = re.sub(r'\bCURRENT\s+DATE\b', 'CURRENT_DATE', when_clause, flags=re.IGNORECASE)
        when_clause = re.sub(r'\bCURRENT\s+TIMESTAMP\b', 'CURRENT_TIMESTAMP', when_clause, flags=re.IGNORECASE)

        # Handle SIGNAL SQLSTATE (both the z/OS parenthesised form and the SET MESSAGE_TEXT form)
        body = re.sub(r"(?i)\bSIGNAL\s+SQLSTATE\s+(?:VALUE\s+)?'([^']+)'\s+SET\s+MESSAGE_TEXT\s*=\s*('[^']+'|[a-zA-Z0-9_.\"]+);?", r"RAISE EXCEPTION \2 USING ERRCODE = '\1';", body)
        body = re.sub(r"(?i)\bSIGNAL\s+SQLSTATE\s+'([^']+)'\s*\(\s*('[^']+'|[a-zA-Z0-9_.\"]+)\s*\);?", r"RAISE EXCEPTION \2 USING ERRCODE = '\1';", body)
        body = re.sub(r"(?i)RAISE_ERROR\s*\(\s*'([^']+)'\s*,\s*('[^']+')\s*\)", r"RAISE EXCEPTION \2 USING ERRCODE = '\1';", body)

        # DECLARE of a local variable belongs into the declaration section of a PL/pgSQL block
        declarations = []
        declared_variables = []
        def collect_declaration(match):
            declared_variables.append(match.group(1))
            declarations.append(f"{match.group(1)} {match.group(2).strip()};")
            return ''
        body = re.sub(r'(?im)^[^\S\n]*DECLARE\s+([A-Za-z0-9_]+)\s+([^;\n]+);[^\S\n]*$', collect_declaration, body).strip()

        # Handle assignments: SET a = b or SET (a,b) = (c,d)
        if body.upper().startswith('SET'):
            body = re.sub(r'(?i)^SET\s*', '', body)
            tuple_match = re.match(r'^\(\s*([^)]+)\s*\)\s*=\s*\(\s*(.+)\s*\);?$', body, re.IGNORECASE | re.DOTALL)
            if tuple_match:
                cols = [c.strip() for c in tuple_match.group(1).split(',')]
                vals = [c.strip() for c in tuple_match.group(2).split(',')]
                if len(cols) == 1:
                    body = f"{cols[0]} := {tuple_match.group(2)};"
                elif len(cols) == len(vals):
                    # Multi-assignment
                    body = "\n".join([f"{c} := {v};" for c, v in zip(cols, vals)])
            else:
                body = re.sub(r'(?i)^([A-Za-z0-9_."]+)\s*=', r'\1 := ', body)
            if not body.strip().endswith(';'):
                body += ';'
        # assignments to a declared local variable inside a compound body - only those, the SET
        # of an UPDATE statement in the body must not be touched
        for declared_variable in declared_variables:
            body = re.sub(rf'(?im)^([^\S\n]*)SET\s+({re.escape(declared_variable)})\s*=', r'\1\2 :=', body)

        # Handle plain updates
        if not body.strip().endswith(';'):
            body += ';'

        # Target Generation
        target_table_name = self.config_parser.convert_names_case(target_table_name)
        converted_trigger_name = self.config_parser.convert_names_case(trigger_name)
        func_name = f"{converted_trigger_name}_func"

        if for_each_scope == 'STATEMENT' or timing == 'AFTER':
            return_stmt = "RETURN NULL;"
        elif timing == 'BEFORE' and event == 'DELETE':
            return_stmt = "RETURN OLD;"
        else:
            return_stmt = "RETURN NEW;"

        declare_sql = ("DECLARE\n" + "\n".join(declarations) + "\n") if declarations else ""
        pg_func = f"""CREATE OR REPLACE FUNCTION "{target_schema_name}"."{func_name}"()
RETURNS TRIGGER AS $$
{declare_sql}BEGIN
{body}
{return_stmt}
END;
$$ LANGUAGE plpgsql;
"""
        ref_parts = []
        if old_table_alias:
            ref_parts.append(f"OLD TABLE AS {old_table_alias}")
        if new_table_alias:
            ref_parts.append(f"NEW TABLE AS {new_table_alias}")
        referencing_sql = f"\nREFERENCING {' '.join(ref_parts)}" if ref_parts else ""

        when_sql = f"\nWHEN ({when_clause})" if when_clause else ""
        pg_trigger = f"""CREATE TRIGGER "{converted_trigger_name}"
{timing} {pg_event} ON "{target_schema_name}"."{target_table_name}"{referencing_sql}
FOR EACH {for_each_scope}{when_sql}
EXECUTE FUNCTION "{target_schema_name}"."{func_name}"();
"""

        self.config_parser.print_log_message('DEBUG', f"ibm_db2_zos_connector: convert_trigger: Converted {trigger_name}")
        return pg_func + '\n' + pg_trigger

    def fetch_funcproc_names(self, schema: str):
        funcprocs = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT id, source_funcproc_name, source_funcproc_type, source_funcproc_comment
                        FROM "{self.protocol_schema}"."ddl_funcprocs"
                        WHERE trim(source_schema_name) = trim(%s) ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (schema,))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: fetch_funcproc_names: ({schema}): {rows}")
            for i, row in enumerate(rows, 1):
                funcprocs[i] = {
                    'id': row[0],
                    'name': row[1],
                    'type': row[2] or 'FUNCTION',
                    'comment': row[3],
                }
            cursor.close()
        return funcprocs

    def fetch_funcproc_code(self, funcproc_id: int):
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT source_ddl_text FROM "{self.protocol_schema}"."ddl_funcprocs" WHERE id = %s"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (funcproc_id,))
            row = cursor.fetchone()
            cursor.close()
            if row:
                return row[0]
        return ""

    def replace_function_call(self, code: str, function_name: str, build_replacement) -> str:
        """
        Replaces every call of the function by what build_replacement(argument) returns. The
        argument is read with balanced parentheses, so a nested expression is handled as well.
        build_replacement may return None to leave that occurrence untouched.
        """
        if not code:
            return code
        result = code
        search_from = 0
        pattern = re.compile(rf"(?i)\b{re.escape(function_name)}\s*\(")
        while True:
            match_call = pattern.search(result, search_from)
            if not match_call:
                break
            open_paren = match_call.end() - 1
            depth = 0
            end = -1
            for i in range(open_paren, len(result)):
                if result[i] == '(':
                    depth += 1
                elif result[i] == ')':
                    depth -= 1
                    if depth == 0:
                        end = i
                        break
            if end == -1:
                break
            replacement = build_replacement(result[open_paren+1:end].strip())
            if replacement is None:
                search_from = end + 1
                continue
            result = result[:match_call.start()] + replacement + result[end+1:]
            search_from = match_call.start() + len(replacement)
        return result

    def convert_varchar_function(self, code: str) -> str:
        """
        VARCHAR(<expression>) is a conversion function in DB2 and a data type in PostgreSQL.
        A plain number is a length specification of the data type and stays untouched.
        """
        def build(argument):
            if re.match(r"^\d+(\s*,\s*\d+)?$", argument):
                return None
            return f"CAST({argument} AS VARCHAR)"
        return self.replace_function_call(code, 'VARCHAR', build)

    def convert_date_part_functions(self, code: str) -> str:
        """
        YEAR(), MONTH() and DAY() return an INTEGER in DB2, while EXTRACT() returns a numeric
        in PostgreSQL - without the cast an integer division like (MONTH(D) + 2) / 3 silently
        becomes a division with a fraction.
        """
        for function_name, part in (('YEAR', 'YEAR'), ('MONTH', 'MONTH'), ('DAY', 'DAY')):
            code = self.replace_function_call(
                code, function_name,
                lambda argument, _part=part: f"EXTRACT({_part} FROM {argument})::integer")
        return code

    def convert_data_type(self, source_type: str) -> str:
        """Maps a DB2 data type of a routine parameter or return value to the target type,
        the length / precision specification is kept."""
        source_type = (source_type or '').strip()
        if not source_type:
            return 'TEXT'
        base_type = source_type.split('(')[0].strip().upper()
        types_mapping = self.get_types_mapping({'target_db_type': 'postgresql'})
        target_type = types_mapping.get(base_type, base_type)
        if '(' in source_type and target_type.upper() not in ('TEXT', 'BYTEA', 'XML', 'DATE'):
            target_type += source_type[source_type.find('('):source_type.find(')')+1]
        return target_type

    def convert_funcproc_parameters(self, parameters: str) -> str:
        """
        Converts the parameter list of a routine. DB2 writes the mode in front of the name
        (IN P_QTY INTEGER), PostgreSQL in front of the parameter (IN p_qty integer), and an
        OUT parameter of a PostgreSQL procedure has to be INOUT to be callable the same way.
        The name of a parameter is a variable of the routine and not a database object, so it is
        neither quoted nor case converted - it has to keep matching the references in the body,
        which PostgreSQL folds the same way.
        """
        converted = []
        for parameter in self.split_top_level_commas(parameters or ''):
            match_parameter = re.match(r"(?i)^\s*(IN|OUT|INOUT)?\s*([A-Za-z0-9_$#@]+)\s+(.+?)\s*$", parameter, re.DOTALL)
            if not match_parameter:
                continue
            mode = (match_parameter.group(1) or 'IN').upper()
            if mode == 'OUT':
                mode = 'INOUT'
            converted.append(f'{mode} {match_parameter.group(2)} {self.convert_data_type(match_parameter.group(3))}')
        return ', '.join(converted)

    def convert_funcproc_body(self, body: str, settings: dict) -> tuple:
        """
        Converts the SQL PL statements of a routine body into PL/pgSQL. Returns the converted
        body, the declarations of its local variables and the exception handlers.
        """
        source_schema_name = settings.get('source_schema_name', '')
        target_schema_name = settings.get('target_schema_name', '')

        declarations = []
        declared_variables = []
        handlers = []

        # DECLARE <condition> HANDLER FOR <condition> <statement> - taken out before the plain
        # variable declarations, it is a handler and not a variable
        def collect_handler(match):
            handlers.append((match.group(2).upper(), match.group(3).strip()))
            return ''
        body = re.sub(r'(?is)\bDECLARE\s+(EXIT|CONTINUE|UNDO)\s+HANDLER\s+FOR\s+(NOT\s+FOUND|SQLEXCEPTION|SQLWARNING)\s+(.*?);(?=\s*(?:DECLARE|SELECT|SET|INSERT|UPDATE|DELETE|IF|VALUES|CALL|GET|EXECUTE|BEGIN|END|$))',
                      collect_handler, body)

        def collect_declaration(match):
            declared_variables.append(match.group(1))
            declarations.append(f"{match.group(1)} {self.convert_data_type(match.group(2))};")
            return ''
        body = re.sub(r'(?im)^[^\S\n]*DECLARE\s+([A-Za-z0-9_$#@]+)\s+([^;\n]+);[^\S\n]*$', collect_declaration, body)

        # global variables and the schema of the referenced objects
        body = self.convert_global_variables(body, source_schema_name)
        if source_schema_name and target_schema_name:
            schema_pattern = rf'(?i)(?<![A-Za-z0-9_."]){re.escape(source_schema_name)}\s*\.'
            body = self.replace_outside_string_literals(body, schema_pattern, f'"{target_schema_name}".')

            # A string literal is normally left alone, but a routine assembles its dynamic SQL
            # in one - the objects addressed there have to be renamed as well.
            def convert_dynamic_sql(match):
                literal = match.group(0)
                if not re.search(r"(?i)\b(SELECT|INSERT\s+INTO|UPDATE|DELETE\s+FROM|FROM|JOIN|CALL)\b", literal):
                    return literal
                converted_literal = re.sub(schema_pattern, f'{target_schema_name}.', literal)
                if converted_literal != literal:
                    self.config_parser.print_log_message('DEBUG', f"ibm_db2_zos_connector: convert_funcproc_body: Renamed the schema in the dynamic SQL statement {literal.strip()}")
                return converted_literal
            body = re.sub(r"'(?:[^']|'')*'", convert_dynamic_sql, body)

        # VALUES NEXT VALUE FOR <sequence> INTO <variable> is the DB2 way to draw a sequence value
        body = re.sub(r'(?is)\bVALUES\s+NEXT\s+VALUE\s+FOR\s+("?[A-Za-z0-9_$#@.]+"?(?:\."?[A-Za-z0-9_$#@]+"?)?)\s+INTO\s+([A-Za-z0-9_$#@]+)\s*;',
                      lambda m: f"{m.group(2)} := nextval('{m.group(1).replace(chr(34), '')}');", body)
        body = re.sub(r'(?i)\bNEXT\s+VALUE\s+FOR\s+("?[A-Za-z0-9_$#@.]+"?)', lambda m: f"nextval('{m.group(1).replace(chr(34), '')}')", body)

        # SIGNAL SQLSTATE in both the SET MESSAGE_TEXT and the parenthesised z/OS form
        body = re.sub(r"(?is)\bSIGNAL\s+SQLSTATE\s+(?:VALUE\s+)?'([^']+)'\s+SET\s+MESSAGE_TEXT\s*=\s*('[^']*'|[A-Za-z0-9_.\"]+)\s*;?",
                      r"RAISE EXCEPTION \2 USING ERRCODE = '\1';", body)
        body = re.sub(r"(?is)\bSIGNAL\s+SQLSTATE\s+'([^']+)'\s*\(\s*('[^']*'|[A-Za-z0-9_.\"]+)\s*\)\s*;?",
                      r"RAISE EXCEPTION \2 USING ERRCODE = '\1';", body)

        # EXECUTE IMMEDIATE <variable> is EXECUTE <variable> in PL/pgSQL
        body = re.sub(r'(?i)\bEXECUTE\s+IMMEDIATE\b', 'EXECUTE', body)

        # VARCHAR(<expression>) is a conversion function in DB2 and a data type in PostgreSQL
        body = self.convert_varchar_function(body)
        # YEAR() / MONTH() / DAY() keep their integer result
        body = self.convert_date_part_functions(body)

        # the DB2 functions without a PostgreSQL counterpart (NVL(), DECODE(), ...)
        sql_functions_mapping = self.get_sql_functions_mapping({'target_db_type': 'postgresql'})
        for source_function, target_function in (sql_functions_mapping or {}).items():
            escaped_function = re.escape(source_function)
            if escaped_function.endswith(r'\(') or escaped_function.endswith(r'\)'):
                body = re.sub(rf"(?i)\b{escaped_function}", target_function, body)
            else:
                body = re.sub(rf"(?i)\b{escaped_function}\b", target_function, body)

        body = re.sub(r'\bCURRENT\s+DATE\b', 'CURRENT_DATE', body, flags=re.IGNORECASE)
        body = re.sub(r'\bCURRENT\s+TIMESTAMP\b', 'CURRENT_TIMESTAMP', body, flags=re.IGNORECASE)

        # assignments to a declared local variable or to an output parameter
        for declared_variable in declared_variables + [p for p in settings.get('out_parameters', [])]:
            body = re.sub(rf'(?im)^([^\S\n]*)SET\s+({re.escape(declared_variable)})\s*=', r'\1\2 :=', body)

        # a NOT FOUND handler corresponds to NO_DATA_FOUND, which PostgreSQL only raises for
        # SELECT INTO STRICT - without STRICT a missing row leaves the variables NULL
        exception_sql = ''
        if handlers:
            handler_map = {'NOT FOUND': 'NO_DATA_FOUND', 'SQLEXCEPTION': 'OTHERS', 'SQLWARNING': 'OTHERS'}
            handler_parts = []
            for condition, statement in handlers:
                statement = re.sub(r"(?is)\bSIGNAL\s+SQLSTATE\s+(?:VALUE\s+)?'([^']+)'\s+SET\s+MESSAGE_TEXT\s*=\s*('[^']*'|[A-Za-z0-9_.\"]+)\s*;?",
                                   r"RAISE EXCEPTION \2 USING ERRCODE = '\1';", statement)
                if not statement.strip().endswith(';'):
                    statement += ';'
                handler_parts.append(f"WHEN {handler_map.get(condition, 'OTHERS')} THEN\n{statement}")
                if condition == 'NOT FOUND':
                    body = re.sub(r'(?i)\bSELECT\b(?![\s\S]*?\bINTO\s+STRICT\b)((?:(?!\bSELECT\b)[\s\S])*?)\bINTO\b', r'SELECT\1INTO STRICT', body)
            exception_sql = "EXCEPTION\n" + "\n".join(handler_parts) + "\n"

        return body.strip(), declarations, exception_sql

    def convert_funcproc_code(self, settings):
        funcproc_code = settings.get('funcproc_code', '')
        if isinstance(funcproc_code, dict):
            funcproc_code = funcproc_code.get('definition', '') or ''
        funcproc_name = settings.get('funcproc_name', '')
        target_db_type = settings.get('target_db_type', 'postgresql')
        source_schema_name = settings.get('source_schema_name', '')
        target_schema_name = settings.get('target_schema_name', '')

        if not funcproc_code or target_db_type != 'postgresql':
            if target_db_type != 'postgresql':
                self.config_parser.print_log_message('ERROR', f"ibm_db2_zos_connector: convert_funcproc_code: Unsupported target database type: {target_db_type}")
            return ''

        code = re.sub(r'--([^\n]*)', r'/*\1*/', funcproc_code)
        code = re.sub(r"@[^\S\n]*$", "", code.rstrip())

        match_header = re.match(
            r"(?is)^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(FUNCTION|PROCEDURE)\s+"
            r"\"?([A-Za-z0-9_$#@]+)\"?\.\"?([A-Za-z0-9_$#@]+)\"?\s*\(", code)
        if not match_header:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_zos_connector: convert_funcproc_code: Cannot parse the header of {funcproc_name} - not migrated.")
            return ''

        routine_type = match_header.group(1).upper()
        routine_name = match_header.group(3).upper()

        # parameter list
        start_idx = code.find('(', match_header.end() - 1)
        depth = 0
        end_idx = -1
        for i in range(start_idx, len(code)):
            if code[i] == '(':
                depth += 1
            elif code[i] == ')':
                depth -= 1
                if depth == 0:
                    end_idx = i
                    break
        if end_idx == -1:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_zos_connector: convert_funcproc_code: Cannot parse the parameters of {funcproc_name} - not migrated.")
            return ''
        parameters = code[start_idx+1:end_idx]
        remainder = code[end_idx+1:]

        # An external routine is a load module written in another language, its code is not
        # part of the DDL and there is nothing which could be translated.
        match_language = re.search(r"(?i)\bLANGUAGE\s+([A-Za-z0-9_]+)", remainder)
        language = match_language.group(1).upper() if match_language else 'SQL'
        if language != 'SQL' or re.search(r"(?i)\bEXTERNAL\s+NAME\b", remainder):
            self.config_parser.print_log_message('WARNING', f"ibm_db2_zos_connector: convert_funcproc_code: {routine_type} {routine_name} is an external routine (LANGUAGE {language}) - its load module is not part of the DDL and it is not migrated.")
            return ''

        # return type of a function - a scalar type or a table
        returns_sql = ''
        match_returns_table = None
        table_columns = []
        if routine_type == 'FUNCTION':
            match_returns_table = re.search(r"(?is)\bRETURNS\s+TABLE\s*\((.*?)\)\s*(?=\b(?:LANGUAGE|SPECIFIC|DETERMINISTIC|NOT\s+DETERMINISTIC|READS|MODIFIES|CONTAINS|NO\s+SQL|EXTERNAL|PARAMETER|RETURN|BEGIN)\b)", remainder)
            if match_returns_table:
                for column in self.split_top_level_commas(match_returns_table.group(1)):
                    match_column = re.match(r"(?i)^\s*([A-Za-z0-9_$#@]+)\s+(.+?)\s*$", column, re.DOTALL)
                    if match_column:
                        table_columns.append((self.config_parser.convert_names_case(match_column.group(1).upper()),
                                              self.convert_data_type(match_column.group(2))))
                returns_sql = f"RETURNS TABLE({', '.join(f'{chr(34)}{n}{chr(34)} {t}' for n, t in table_columns)})"
            else:
                match_returns = re.search(r"(?is)\bRETURNS\s+([A-Za-z0-9_]+(?:\s*\([^)]*\))?)", remainder)
                returns_sql = f"RETURNS {self.convert_data_type(match_returns.group(1))}" if match_returns else "RETURNS TEXT"

        # the body starts at BEGIN or at the RETURN of a simple SQL function
        match_body = re.search(r"(?is)\bBEGIN(?:\s+ATOMIC)?\b", remainder)
        is_compound = bool(match_body)
        if is_compound:
            body = remainder[match_body.end():]
            body = re.sub(r'(?is)\bEND\s*;?\s*$', '', body).strip()
        else:
            match_return = re.search(r"(?is)\bRETURN\b", remainder)
            if not match_return:
                self.config_parser.print_log_message('ERROR', f"ibm_db2_zos_connector: convert_funcproc_code: Cannot find the body of {routine_type} {routine_name} - not migrated.")
                return ''
            body = remainder[match_return.end():].strip().rstrip(';')
            if match_returns_table:
                # A table function returns a result set. PostgreSQL demands that the types of
                # RETURN QUERY match the declared ones exactly (DB2 converts them silently), so
                # the query is wrapped and its columns are cast by position.
                if table_columns:
                    inner_names = [f'"mig_col_{i}"' for i in range(1, len(table_columns) + 1)]
                    cast_list = ', '.join(f'CAST({inner} AS {column_type})'
                                          for inner, (_, column_type) in zip(inner_names, table_columns))
                    body = f'RETURN QUERY SELECT {cast_list} FROM (\n{body}\n) AS "mig_result"({", ".join(inner_names)});'
                else:
                    body = f"RETURN QUERY {body};"
            elif re.match(r"(?is)^\s*(SELECT|WITH)\b", body):
                # RETURN <subselect> has to be parenthesised in PL/pgSQL
                body = f"RETURN ({body});"
            else:
                body = f"RETURN {body};"

        out_parameters = [m.group(1) for m in re.finditer(r"(?i)\b(?:OUT|INOUT)\s+([A-Za-z0-9_$#@]+)\s+", parameters)]
        body, declarations, exception_sql = self.convert_funcproc_body(body, {
            'source_schema_name': source_schema_name,
            'target_schema_name': target_schema_name,
            'out_parameters': out_parameters,
        })

        converted_parameters = self.convert_funcproc_parameters(parameters)
        converted_name = self.config_parser.convert_names_case(routine_name)
        declare_sql = ("DECLARE\n" + "\n".join(declarations) + "\n") if declarations else ""
        if not body.strip().endswith(';'):
            body += ';'

        # The output columns of a table function are PL/pgSQL variables of the same name as the
        # columns its query selects - without this directive every such reference is ambiguous.
        variable_conflict_sql = "#variable_conflict use_column\n" if match_returns_table else ""

        converted_code = f'CREATE OR REPLACE {routine_type} "{target_schema_name}"."{converted_name}"({converted_parameters})\n'
        if returns_sql:
            converted_code += f'{returns_sql}\n'
        converted_code += f'LANGUAGE plpgsql\nAS $$\n{variable_conflict_sql}{declare_sql}BEGIN\n{body}\n{exception_sql}END;\n$$;\n'

        self.config_parser.print_log_message('DEBUG', f"ibm_db2_zos_connector: convert_funcproc_code: Converted {routine_type} {routine_name}")
        return converted_code

    def fetch_sequences(self, schema_name: str):
        seqs = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            ## we migrate only sequences not attached to tables
            query = f"""SELECT id, source_seq_name, source_ddl_text, source_start_value, source_increment_by, source_minvalue, source_maxvalue, source_cache, source_is_cycled
                        FROM "{self.protocol_schema}"."ddl_sequences"
                        WHERE source_schema_name = %s
                        AND source_table_name IS NULL AND source_column_name IS NULL
                        ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (schema_name,))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: fetch_sequences: ({schema_name}): {rows}")
            for i, row in enumerate(rows, 1):
                seqs[i] = {
                    'id': row[0],
                    'sequence_name': row[1],
                    'column_name': None,
                    'source_sequence_sql': row[2],
                    'source_start_value': row[3],
                    'source_increment_by': row[4],
                    'source_minvalue': row[5],
                    'source_maxvalue': row[6],
                    'source_cache': row[7],
                    'source_is_cycled': row[8]
                }
            cursor.close()
        return seqs

    def get_sequence_details(self, sequence_owner, sequence_name):
        return {}

    def migrate_sequences(self, target_connector, settings):
        target_schema_name = settings.get('target_schema_name', '')
        target_sequence_name = settings.get('target_sequence_name', '')
        source_start_value = settings.get('source_start_value')
        source_increment_by = settings.get('source_increment_by')
        source_minvalue = settings.get('source_minvalue')
        source_maxvalue = settings.get('source_maxvalue')
        source_cache = settings.get('source_cache')
        source_is_cycled = settings.get('source_is_cycled')

        if not target_sequence_name:
            return True

        if self.connectivity == self.config_parser.const_connectivity_ddl():
            try:
                sql_parts = [f'CREATE SEQUENCE "{target_schema_name}"."{target_sequence_name}"']
                if source_increment_by is not None:
                    sql_parts.append(f"INCREMENT BY {source_increment_by}")
                if source_minvalue is not None:
                    sql_parts.append(f"MINVALUE {source_minvalue}")
                if source_maxvalue is not None:
                    sql_parts.append(f"MAXVALUE {source_maxvalue}")
                if source_start_value is not None:
                    sql_parts.append(f"START WITH {source_start_value}")
                if source_cache is not None:
                    sql_parts.append(f"CACHE {source_cache}")
                if source_is_cycled:
                    sql_parts.append("CYCLE")

                target_sequence_sql = " ".join(sql_parts)

                self.config_parser.print_log_message('INFO', f"ibm_db2_zos_connector: migrate_sequences: Creating sequence {target_sequence_name} ...")
                target_connector.execute_query(target_sequence_sql)
                return True
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"ibm_db2_zos_connector: migrate_sequences: Error creating sequence {target_sequence_name}: {e}")
                return False

        return True

    def fetch_views_names(self, source_schema_name: str):
        views = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT id, source_schema_name, source_view_name, source_view_type
                        FROM "{self.protocol_schema}"."ddl_views"
                        WHERE source_schema_name = %s ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (source_schema_name,))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: fetch_views_names: ({source_schema_name}): {rows}")
            for i, row in enumerate(rows, 1):
                views[i] = {
                    'id': row[0],
                    'schema_name': row[1],
                    'view_name': row[2],
                    'target_schema_name': '',
                    'target_view_name': '',
                    'comment': None,
                    'view_type': row[3] or 'VIEW',
                    'is_alias': False
                }

            # Now fetch aliases that point to views unconditionally
            # This ensures that even if use_aliases_as_target_names is active for tables,
            # we always create additional views "select * from <original view>" for view aliases
            alias_query = f"""
                SELECT a.id, a.source_schema_name, a.source_alias_name,
                       a.source_target_schema, a.source_target_name
                FROM "{self.protocol_schema}"."ddl_aliases" a
                INNER JOIN "{self.protocol_schema}"."ddl_views" v
                    ON a.source_target_schema = v.source_schema_name
                    AND a.source_target_name = v.source_view_name
                WHERE a.source_schema_name = %s
                ORDER BY a.id
            """
            cursor.execute(alias_query, (source_schema_name,))
            alias_rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: fetch_views_names (aliases): ({source_schema_name}): {alias_rows}")

            # Start appending aliases, preserving unique IDs (shift by 1,000,000 to avoid clash with view IDs)
            offset = len(views)
            for j, row in enumerate(alias_rows, 1):
                views[offset + j] = {
                    'id': row[0] + 1000000, # Shift ID to avoid collision with actual view IDs
                    'schema_name': row[1],
                    'view_name': row[2],
                    'target_schema_name': row[3],
                    'target_view_name': row[4],
                    'comment': None,
                    'is_alias': True
                }

            cursor.close()
        return views

    def get_aliases(self, settings):
        source_schema_name = settings.get('source_schema_name')
        aliases = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT a.id, a.source_schema_name, a.source_alias_name, a.source_target_schema, a.source_target_name, a.source_alias_sql, a.source_alias_comment,
                            CASE
                                WHEN t.source_table_name IS NOT NULL THEN 'TABLE'
                                WHEN v.source_view_name IS NOT NULL THEN 'VIEW'
                                ELSE 'UNKNOWN'
                            END as alias_target_type
                        FROM "{self.protocol_schema}"."ddl_aliases" a
                        LEFT JOIN "{self.protocol_schema}"."ddl_tables" t
                            ON a.source_target_schema = t.source_schema_name AND a.source_target_name = t.source_table_name
                        LEFT JOIN "{self.protocol_schema}"."ddl_views" v
                            ON a.source_target_schema = v.source_schema_name AND a.source_target_name = v.source_view_name
                        WHERE a.source_schema_name = %s ORDER BY a.id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (source_schema_name,))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: get_aliases: ({source_schema_name}): {rows}")
            for i, row in enumerate(rows, 1):
                aliases[i] = {
                    'id': row[0],
                    'alias_schema_name': row[1],
                    'alias_name': row[2],
                    'aliased_schema_name': row[3],
                    'aliased_table_name': row[4],
                    'alias_owner': row[1],
                    'alias_sql': row[5],
                    'alias_comment': row[6],
                    'alias_target_type': row[7]
                }
            cursor.close()
        return aliases

    def fetch_view_code(self, settings):
        source_schema_name = settings.get('source_schema_name')
        source_view_name = settings.get('source_view_name')
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT source_view_sql
                        FROM "{self.protocol_schema}"."ddl_views"
                        WHERE source_schema_name = %s AND source_view_name = %s"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (source_schema_name, source_view_name))
            row = cursor.fetchone()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: fetch_view_code: ({source_schema_name}.{source_view_name}): {row}")
            if row:
                cursor.close()
                return row[0]

            # If not found, try looking up as an alias mapped to a view
            alias_query = f"""
                SELECT a.source_schema_name, a.source_alias_name, a.source_target_schema, a.source_target_name
                FROM "{self.protocol_schema}"."ddl_aliases" a
                INNER JOIN "{self.protocol_schema}"."ddl_views" v
                    ON a.source_target_schema = v.source_schema_name
                    AND a.source_target_name = v.source_view_name
                WHERE a.source_schema_name = %s AND a.source_alias_name = %s
            """
            cursor.execute(alias_query, (source_schema_name, source_view_name))
            alias_row = cursor.fetchone()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: fetch_view_code (from alias): ({source_schema_name}.{source_view_name}): {alias_row}")
            cursor.close()

            if alias_row:
                # schema_name = alias_row[0], alias_name = alias_row[1]
                # target_schema = alias_row[2], target_name = alias_row[3]
                return f'CREATE VIEW "{alias_row[0]}"."{alias_row[1]}" AS SELECT * FROM "{alias_row[2]}"."{alias_row[3]}"'

        return ""

    def convert_default_value(self, settings) -> dict:
        extracted_default_value = settings['extracted_default_value']
        self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: convert_default_value: ({extracted_default_value})")
        if extracted_default_value != None and extracted_default_value.upper() == 'SYSTEM DEFAULT':
            column_type = settings['column_type']
            if self.is_string_type(column_type):
                return "''"
            elif self.is_numeric_type(column_type):
                return '0'
            return 'NULL'
        return extracted_default_value

    def convert_view_code(self, settings: dict):

        def convert_identifier_case(name, quoted):
            """
            An identifier which is not delimited by double quotes is folded to upper case by
            DB2, so it has to be normalized before migration.names_case_handling is applied -
            otherwise 'keep' would preserve the case in which the DDL happens to be written
            instead of the case in which the object really exists.
            """
            return self.config_parser.convert_names_case(name if quoted else name.upper())

        def quote_column_names(node):
            if isinstance(node, sqlglot.exp.Column) and node.name:
                identifier = node.args.get("this")
                converted_name = convert_identifier_case(node.name, bool(identifier.args.get("quoted")) if identifier else False)
                node.set("this", sqlglot.exp.Identifier(this=converted_name, quoted=True))
                # the table qualifier of the column (O.STATUS) is an alias or a table name
                table_id = node.args.get("table")
                if isinstance(table_id, sqlglot.exp.Identifier):
                    table_id.set("this", convert_identifier_case(table_id.name, bool(table_id.args.get("quoted"))))
                    table_id.set("quoted", True)
            if isinstance(node, sqlglot.exp.Alias) and isinstance(node.args.get("alias"), sqlglot.exp.Identifier):
                alias = node.args["alias"]
                alias.set("this", convert_identifier_case(alias.name, bool(alias.args.get("quoted"))))
                alias.set("quoted", True)
            if isinstance(node, sqlglot.exp.Schema):
                for expr in node.expressions:
                    if isinstance(expr, sqlglot.exp.Identifier):
                        expr.set("this", convert_identifier_case(expr.name, bool(expr.args.get("quoted"))))
                        expr.set("quoted", True)
            if isinstance(node, sqlglot.exp.CTE):
                alias = node.args.get("alias")
                if isinstance(alias, sqlglot.exp.TableAlias):
                    alias_this = alias.args.get("this")
                    if isinstance(alias_this, sqlglot.exp.Identifier):
                        alias_this.set("this", convert_identifier_case(alias_this.name, bool(alias_this.args.get("quoted"))))
                        alias_this.set("quoted", True)
                    # the column list of the CTE header (WITH TREE (CATEGORY_ID, DEPTH) AS ...)
                    for col_id in alias.args.get("columns") or []:
                        if isinstance(col_id, sqlglot.exp.Identifier):
                            col_id.set("this", convert_identifier_case(col_id.name, bool(col_id.args.get("quoted"))))
                            col_id.set("quoted", True)
            return node

        def replace_schema_names(node):
            if isinstance(node, sqlglot.exp.Table):
                schema = node.args.get("db")
                if schema and schema.name.upper() == settings['source_schema_name'].upper():
                    node.set("db", sqlglot.exp.Identifier(this=settings['target_schema_name'], quoted=False))
            return node

        def quote_schema_and_table_names(node):
            if isinstance(node, sqlglot.exp.TableAlias):
                # alias of a table in the FROM clause and its optional column list
                alias_id = node.args.get("this")
                if isinstance(alias_id, sqlglot.exp.Identifier):
                    alias_id.set("this", convert_identifier_case(alias_id.name, bool(alias_id.args.get("quoted"))))
                    alias_id.set("quoted", True)
                for col_id in node.args.get("columns") or []:
                    if isinstance(col_id, sqlglot.exp.Identifier):
                        col_id.set("this", convert_identifier_case(col_id.name, bool(col_id.args.get("quoted"))))
                        col_id.set("quoted", True)
            if isinstance(node, sqlglot.exp.Table):
                schema = node.args.get("db")
                schema_name_for_lookup = schema.name if schema else settings['source_schema_name']
                if schema:
                    converted_schema = self.config_parser.convert_names_case(schema.name)
                    schema.set("this", converted_schema)
                    if not schema.args.get("quoted"):
                        schema.set("quoted", True)
                table = node.args.get("this")
                if table:
                    # Lookup alias if enabled
                    table_name_to_use = table.name
                    if not isinstance(node.parent, sqlglot.exp.Create):
                        if self.config_parser.get_use_aliases_as_target_names() and settings.get('migrator_tables'):
                            alias_dict = settings['migrator_tables'].get_alias_for_table(schema_name_for_lookup, table.name)
                            if alias_dict and not settings.get('alias_view'):
                                alias_name = alias_dict.get('target_alias_name')
                                alias_target_type = alias_dict.get('alias_target_type', 'UNKNOWN')

                                if alias_target_type == 'TABLE':
                                    if alias_name.lower() == settings.get('target_view_name', '').lower() or alias_name.lower() == settings.get('source_view_name', '').lower():
                                        self.config_parser.print_log_message('INFO', f"ibm_db2_zos_connector: convert_view_code: Skipped replacing referenced table '{table.name}' with alias '{alias_name}' to avoid circular reference. Settings: {settings}")
                                    else:
                                        self.config_parser.print_log_message('INFO', f"ibm_db2_zos_connector: convert_view_code: Replaced referenced table '{table.name}' with alias '{alias_name}' inside view generation. Settings: {settings}")
                                        table_name_to_use = alias_name
                                else:
                                    self.config_parser.print_log_message('DEBUG', f"ibm_db2_zos_connector: convert_view_code: Skipped replacing '{table.name}' with alias '{alias_name}' because alias points to a {alias_target_type}, not a TABLE.")

                    converted_table = convert_identifier_case(table_name_to_use, bool(table.args.get("quoted")))
                    table.set("this", converted_table)
                    table.set("quoted", True)
            return node

        def replace_functions(node):
            mapping = self.get_sql_functions_mapping({ 'target_db_type': settings['target_db_type'] })
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
                    if '(' not in mapped:
                        node.set("this", sqlglot.exp.Identifier(this=mapped, quoted=False))
                    else:
                        if mapped.startswith('extract('):
                            arg = node.args.get("expressions")
                            if arg and len(arg) == 1:
                                return sqlglot.exp.Extract(
                                    this=sqlglot.exp.Identifier(this=func_name, quoted=False),
                                    expression=arg[0]
                                )
                        else:
                            for orig, repl in mapping.items():
                                if orig.endswith('(') and func_name == orig[:-1].lower():
                                    if repl.endswith('('):
                                        node.set("this", sqlglot.exp.Identifier(this=repl[:-1], quoted=False))
                                    else:
                                        node.set("this", sqlglot.exp.Identifier(this=repl, quoted=False))
                                    break
                                elif orig.endswith('()') and func_name == orig[:-2].lower():
                                    node.set("this", sqlglot.exp.Identifier(this=repl, quoted=False))
                                    break
                elif func_name + "()" in func_name_map:
                    mapped = func_name_map[func_name + "()"]
                    return sqlglot.exp.Anonymous(this=mapped)
            return node

        def convert_string_concatenation(node):
            if isinstance(node, sqlglot.exp.Add):
                left = node.left
                right = node.right
                is_left_string = left.is_string or (isinstance(left, sqlglot.exp.Cast) and left.to.this.name.upper() in ('VARCHAR', 'CHAR', 'TEXT', 'NVARCHAR', 'NCHAR', 'UNIVARCHAR', 'UNICHAR'))
                is_right_string = right.is_string or (isinstance(right, sqlglot.exp.Cast) and right.to.this.name.upper() in ('VARCHAR', 'CHAR', 'TEXT', 'NVARCHAR', 'NCHAR', 'UNIVARCHAR', 'UNICHAR'))

                if is_left_string or is_right_string:
                    new_left = left
                    new_right = right
                    if not is_left_string:
                         new_left = sqlglot.exp.Cast(this=left, to=sqlglot.exp.DataType.build('text'))
                    if not is_right_string:
                         new_right = sqlglot.exp.Cast(this=right, to=sqlglot.exp.DataType.build('text'))
                    return sqlglot.exp.DPipe(this=new_left, expression=new_right)
            return node

        def align_union_types(union_node):
            """
            PostgreSQL requires the column types of the non-recursive and the recursive term
            of a recursive CTE to be identical, DB2 resolves them on its own. Where one arm
            casts a column explicitly, the same cast is applied to the other arm.
            """
            if isinstance(union_node, sqlglot.exp.Union):
                select1 = union_node.this
                select2 = union_node.expression
                if isinstance(select1, sqlglot.exp.Select) and isinstance(select2, sqlglot.exp.Select):
                    exprs1 = select1.expressions
                    exprs2 = select2.expressions
                    for i in range(min(len(exprs1), len(exprs2))):
                        e1 = exprs1[i]
                        e2 = exprs2[i]
                        if isinstance(e1, sqlglot.exp.Cast) and not isinstance(e2, sqlglot.exp.Cast):
                            select2.expressions[i] = sqlglot.exp.Cast(this=e2, to=e1.to.copy())
                        elif isinstance(e2, sqlglot.exp.Cast) and not isinstance(e1, sqlglot.exp.Cast):
                            select1.expressions[i] = sqlglot.exp.Cast(this=e1, to=e2.to.copy())

        def convert_recursive_with(node):
            """
            DB2 derives the recursion from the CTE referencing itself, PostgreSQL requires the
            RECURSIVE keyword to be stated explicitly - without it the self-reference fails
            with 'relation ... does not exist'.
            """
            if isinstance(node, sqlglot.exp.With):
                is_recursive = False
                for cte in node.expressions:
                    if isinstance(cte, sqlglot.exp.CTE):
                        cte_name = cte.alias_or_name.upper()
                        for table_ref in cte.this.find_all(sqlglot.exp.Table):
                            if table_ref.name and table_ref.name.upper() == cte_name:
                                is_recursive = True
                                break
                        for union_node in cte.this.find_all(sqlglot.exp.Union):
                            align_union_types(union_node)
                    if is_recursive:
                        break
                if is_recursive:
                    node.set("recursive", True)
            return node

        view_code = settings['view_code']
        converted_code = self.convert_mqt_to_materialized_view(view_code)

        remote_subs = self.config_parser.get_remote_objects_substitution()
        if remote_subs:
            iterator = remote_subs.items() if isinstance(remote_subs, dict) else remote_subs
            for source_obj, target_obj in iterator:
                if source_obj and target_obj:
                    converted_code = re.sub(re.escape(source_obj), target_obj, converted_code, flags=re.IGNORECASE)

        # WITH [CASCADED|LOCAL] CHECK OPTION is valid in PostgreSQL as well, but the SQL parser
        # does not understand it - it is cut off here and appended back to the converted code.
        check_option = ''
        check_option_match = re.search(r"(?i)\s*\bWITH\s+(CASCADED\s+|LOCAL\s+)?CHECK\s+OPTION\s*;?\s*$", converted_code)
        if check_option_match:
            scope = check_option_match.group(1).strip().upper() + ' ' if check_option_match.group(1) else ''
            check_option = f" WITH {scope}CHECK OPTION"
            converted_code = converted_code[:check_option_match.start()].rstrip()

        if settings['target_db_type'] == 'postgresql':
            # DB2 LISTAGG(...) [WITHIN GROUP (ORDER BY ...)] has no PostgreSQL counterpart
            converted_code = re.sub(
                r"(?i)\bLISTAGG\s*\(\s*([^,()]+?)\s*,\s*('[^']*'|[^()]+?)\s*\)\s*WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+([^()]+?)\s*\)",
                r"STRING_AGG(\1::text, \2 ORDER BY \3)",
                converted_code,
            )
            converted_code = re.sub(
                r"(?i)\bLISTAGG\s*\(\s*([^,()]+?)\s*,\s*('[^']*'|[^()]+?)\s*\)",
                r"STRING_AGG(\1::text, \2)",
                converted_code,
            )
            converted_code = self.convert_db2_operators(converted_code)

            sql_functions_mapping = self.get_sql_functions_mapping({ 'target_db_type': settings['target_db_type'] })
            if sql_functions_mapping:
                for src_func, tgt_func in sql_functions_mapping.items():
                    escaped_src_func = re.escape(src_func)
                    if escaped_src_func.endswith(r'\(') or escaped_src_func.endswith(r'\)'):
                        converted_code = re.sub(rf"(?i)\b{escaped_src_func}", tgt_func, converted_code, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
                    else:
                        converted_code = re.sub(rf"(?i)\b{escaped_src_func}\b", tgt_func, converted_code, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)

            try:
                # The 'db2' dialect is not supported by sqlglot, the code is read as 'postgres':
                # DB2 sorts NULL values as the largest ones, exactly like PostgreSQL, while the
                # default sqlglot dialect assumes the opposite and would compensate for it by
                # adding explicit NULLS FIRST / NULLS LAST which inverts the original ordering.
                parsed_code = sqlglot.parse_one(converted_code, read="postgres")
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"ibm_db2_zos_connector: convert_view_code: Error parsing View code: {e}")
                # Fallback to the unparsed converted_code instead of empty string to avoid crashes
                return converted_code + check_option

            # sqlglot does not raise on unknown syntax - it silently falls back to a plain
            # Command node. All transformations below would then be no-ops and the untranslated
            # DB2 code would be handed over to the target database, so this is reported here.
            if isinstance(parsed_code, sqlglot.exp.Command):
                self.config_parser.print_log_message('ERROR', f"ibm_db2_zos_connector: convert_view_code: View code contains syntax unsupported by the SQL parser, it is left unconverted: {converted_code}")
                return converted_code + check_option

            parsed_code = parsed_code.transform(quote_column_names)
            parsed_code = parsed_code.transform(convert_string_concatenation)
            parsed_code = parsed_code.transform(quote_schema_and_table_names)
            parsed_code = parsed_code.transform(replace_schema_names)
            parsed_code = parsed_code.transform(replace_functions)
            parsed_code = parsed_code.transform(convert_recursive_with)

            converted_code = parsed_code.sql(dialect="postgres")
            converted_code = converted_code.replace("()()", "()") + check_option

            self.config_parser.print_log_message('DEBUG', f"ibm_db2_zos_connector: convert_view_code: Converted view: {converted_code}")
        else:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_zos_connector: convert_view_code: Unsupported target database type: {settings['target_db_type']}")

        return converted_code

    def get_sequence_current_value(self, sequence_id: int):
        return 0

    def execute_query(self, query: str, params=None):
        pass

    def execute_sql_script(self, script_path: str):
        pass

    def begin_transaction(self):
        pass

    def commit_transaction(self):
        pass

    def rollback_transaction(self):
        pass

    def get_rows_count(self, table_schema: str, table_name: str, migration_limitation: str = None):
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            # there is no source database to count in - the structure comes from the DDL files
            # and the data from the unload files, the number of rows is the one loaded into the target
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: get_rows_count: ({table_schema}.{table_name}): not counted, source is a set of DDL and data files.")
            return 0

        query = f"""SELECT COUNT(*) FROM {table_schema.upper()}."{table_name}" """
        if migration_limitation:
            query += f" WHERE {migration_limitation}"
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_zos_connector: get_rows_count: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_table_size(self, table_schema: str, table_name: str):
        return 0

    def get_table_next_identity(self, table_schema: str, table_name: str):
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            # the catalog of the source database is not available with DDL connectivity
            return None
        try:
            query = f"""
                SELECT MAXASSIGNEDVAL + 1
                FROM SYSIBM.SYSSEQUENCES
                WHERE SEQTYPE = 'I' AND SCHEMA = '{table_schema}' AND TBNAME = '{table_name}'
            """
            cursor = self.connection.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            if row and row[0] is not None:
                return int(row[0])
            return None
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"ibm_db2_zos_connector: get_table_next_identity: Error fetching next identity for {table_schema}.{table_name}: {e}")
            return None

    def fetch_user_defined_types(self, schema: str):
        return {}

    def fetch_domains(self, schema: str):
        return {}

    def get_create_domain_sql(self, settings):
        pass

    def testing_select(self):
        pass

    def get_database_version(self):
        return "Dummy zOS"

    def get_database_size(self):
        return 0

    def get_top_n_tables(self, settings):
        return {}

    def get_top_fk_dependencies(self, settings):
        return {}

    def target_table_exists(self, target_schema_name, target_table_name):
        return False

    def fetch_all_rows(self, query):
        return []

    def get_table_checksum(self, schema_name: str, table_name: str, columns: list):
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            # a checksum of the source cannot be computed without a source database
            return None
        if not columns:
            return None

        cols_list = []
        for col in columns:
            dtype = col.get('data_type', '').lower()
            if any(x in dtype for x in ['lob', 'bytea', 'xml', 'json', 'text', 'dbclob', 'vargraphic']):
                continue
            cols_list.append(f'"{col["column_name"]}"')
            
        if not cols_list:
            return None
            
        cols_str = ", ".join(cols_list)
        query = f'SELECT {cols_str} FROM "{schema_name}"."{table_name}"'
        return self._compute_python_table_checksum(query)

    def get_random_pks(self, schema_name: str, table_name: str, pk_columns: list, sample_size: int):
        return []

    def get_row_checksums(self, schema_name: str, table_name: str, pk_columns: list, pk_values_list: list, columns: list):
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            # row checksums of the source cannot be computed without a source database
            return {}
        if not columns or not pk_columns or not pk_values_list:
            return {}
            
        cols_list = []
        for col in columns:
            dtype = col.get('data_type', '').lower()
            if any(x in dtype for x in ['lob', 'bytea', 'xml', 'json', 'text', 'dbclob', 'vargraphic']):
                continue
            cols_list.append(f'"{col["column_name"]}"')
            
        if not cols_list:
            return {}
            
        cols_str = ", ".join(cols_list)
        pk_cols_str = ", ".join([f'"{c}"' for c in pk_columns])
        
        in_values = []
        for pk_dict in pk_values_list:
            vals = []
            for c in pk_columns:
                val = pk_dict[c]
                if val is None:
                    vals.append("NULL")
                elif isinstance(val, str):
                    escaped_val = val.replace("'", "''")
                    vals.append(f"'{escaped_val}'")
                else:
                    vals.append(str(val))
            in_values.append(f"({', '.join(vals)})")
        
        where_clause = f"({pk_cols_str}) IN ({', '.join(in_values)})"
        if len(pk_columns) == 1:
            where_clause = f"{pk_cols_str} IN ({', '.join([v.strip('()') for v in in_values])})"
            
        query = f'SELECT {pk_cols_str}, {cols_str} FROM "{schema_name}"."{table_name}" WHERE {where_clause}'
        return self._compute_python_row_checksums(query, len(pk_columns))

    def get_lob_sizes(self, schema_name: str, table_name: str, pk_columns: list, pk_values_list: list, lob_columns: list):
        return {}

if __name__ == "__main__":
    print("This script is not meant to be run directly")
