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
            trigger_pattern = re.compile(r"(CREATE\s+TRIGGER\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?[\s\S]*?(?=(?:CREATE\s+(?:TABLE|VIEW|INDEX|UNIQUE\s+INDEX|ALIAS|SEQUENCE|TRIGGER))|(?:ALTER\s+TABLE)|(?:SET\s+CURRENT\s+SCHEMA)|$))", re.IGNORECASE)
            for match in trigger_pattern.finditer(content):
                schema_name = match.group(2).upper()
                trigger_name = match.group(3).upper()
                ddl_text = match.group(1).strip()
                migrator_tables.insert_ddl_triggers({
                    'source_schema_name': schema_name,
                    'source_trigger_name': trigger_name,
                    'source_ddl_text': ddl_text,
                    'source_trigger_sql': ddl_text,
                    'source_trigger_comment': None
                })

            # Remove the extracted triggers from content so they aren't parsed again
            content = trigger_pattern.sub("", content)

            statements = content.split(';')
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
                            cols_str = stmt[start_idx+1:end_idx]
                            cols_list = []
                            for c in cols_str.split(','):
                                col_stmt = c.strip().split()
                                if col_stmt:
                                    cols_list.append(col_stmt[0].upper())

                            migrator_tables.insert_ddl_indexes({
                                'source_schema_name': tbl_schema,
                                'source_table_name': tbl_name,
                                'source_index_name': idx_name,
                                'source_is_unique': is_unique,
                                'source_columns_list': ', '.join(cols_list),
                                'source_index_sql': stmt,
                                'source_index_comment': comment_text
                            })
                    continue

                # Parse Sequences
                match_seq = re.search(r"^CREATE\s+SEQUENCE\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?", clean_stmt, re.IGNORECASE)
                if match_seq:
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

                # Split columns correctly ignoring commas inside parenthesis
                cols = []
                current = []
                depth = 0
                for char in columns_str:
                    if char == '(':
                        depth += 1
                        current.append(char)
                    elif char == ')':
                        depth -= 1
                        current.append(char)
                    elif char == ',' and depth == 0:
                        cols.append("".join(current).strip())
                        current = []
                    else:
                        current.append(char)
                if current:
                    cols.append("".join(current).strip())

                col_defs = [c for c in cols if c]

                pk_columns = set()
                # First pass for Primary Key constraints within the table block
                for col_def in col_defs:
                    if col_def.upper().startswith("PRIMARY KEY"):
                        match_pk = re.search(r"\((.*)\)", col_def)
                        if match_pk:
                            pks = [p.strip().upper() for p in match_pk.group(1).split(',')]
                            pk_columns.update(pks)

                # Extract Partitioning parameters from the trailing text
                trailing_str = clean_stmt[end_idx+1:]
                partition_col = None
                partition_ranges = None

                match_part = re.search(r"PARTITION\s+BY\s*\(\s*([^)]+)\s*\)\s*\(([\s\S]*?)\)\s*(?:IN|;|$)", trailing_str, re.IGNORECASE)
                if match_part:
                    partition_col = match_part.group(1).replace(" ASC", "").replace(" DESC", "").strip()
                    partition_ranges = match_part.group(2).strip()

                # Register Table
                migrator_tables.insert_ddl_tables({
                    'source_schema_name': schema_name,
                    'source_table_name': table_name,
                    'source_partition_columns': partition_col,
                    'source_partition_ranges': partition_ranges,
                    'source_table_sql': stmt,
                    'source_table_comment': comment_text
                })

                # Second pass for extracting column metrics
                for col_def in col_defs:
                    col_def_u = col_def.upper()
                    if col_def_u.startswith("PRIMARY KEY") or col_def_u.startswith("CONSTRAINT") or col_def_u.startswith("FOREIGN KEY") or col_def_u.startswith("UNIQUE"):
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
        return {'finished': True, 'rows_migrated': 0, 'source_table_rows': 0, 'target_table_rows': 0, 'chunk_number': 1, 'total_chunks': 1}

    def fetch_indexes(self, settings):
        table_schema = settings.get('source_table_schema')
        table_name = settings.get('source_table_name')
        indexes = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT source_index_name, source_is_unique, source_columns_list
                        FROM "{self.protocol_schema}"."ddl_indexes"
                        WHERE source_schema_name = %s AND source_table_name = %s ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (table_schema, table_name))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_zos_connector: fetch_indexes: ({table_schema}.{table_name}): {rows}")
            for i, row in enumerate(rows, 1):
                idx_name = row[0]
                is_unique = row[1]
                cols = row[2]
                indexes[i] = {
                    'index_name': idx_name,
                    'index_type': 'UNIQUE' if is_unique else 'INDEX',
                    'index_owner': table_schema,
                    'index_columns': cols,
                    'index_comment': None,
                    'index_sql': None,
                    'is_function_based': 'NO'
                }
            cursor.close()
        return indexes

    def get_create_index_sql(self, settings):
        pass

    def fetch_constraints(self, settings):
        table_schema = settings.get('source_table_schema')
        table_name = settings.get('source_table_name')
        constraints = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT source_fk_name, source_columns_list, source_ref_schema_name, source_ref_table_name, source_ref_columns_list
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

                constraints[i] = {
                    'constraint_name': row[0],
                    'constraint_type': 'FOREIGN KEY',
                    'constraint_owner': table_schema,
                    'constraint_columns': constraint_columns,
                    'referenced_table_schema': row[2],
                    'referenced_table_name': row[3],
                    'referenced_columns': referenced_columns,
                    'constraint_sql': None,
                    'delete_rule': 'NO ACTION',
                    'update_rule': 'NO ACTION',
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
                if table_name and table_name.upper() not in row[2].upper():
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

    def convert_trigger(self, settings: dict):
        trigger_sql = settings.get('trigger_sql', '')
        trigger_name = settings.get('trigger_name', '')
        target_schema_name = settings.get('target_schema_name', '')
        target_table_name = settings.get('target_table_name', '')

        # Basic cleanup
        trigger_sql = re.sub(r'--([^\n]*)', r'/*\1*/', trigger_sql)

        # 1. Timing (BEFORE, AFTER, INSTEAD OF)
        timing_match = re.search(r'\b(BEFORE|AFTER|INSTEAD\s+OF)\b', trigger_sql, re.IGNORECASE)
        timing = timing_match.group(1).upper() if timing_match else 'BEFORE'

        # 2. Event
        event_match = re.search(r'\b(INSERT|UPDATE|DELETE)(?:\s+OF\s+([a-zA-Z0-9_,\s]+))?\b', trigger_sql[timing_match.end():] if timing_match else trigger_sql, re.IGNORECASE)
        event = event_match.group(1).upper() if event_match else 'UPDATE'
        of_cols = event_match.group(2) if event_match and event_match.group(2) else None

        pg_event = event
        if of_cols and event == 'UPDATE':
            cols = [c.strip() for c in of_cols.split(',')]
            # Discard any matches that leaked to 'ON'
            cols = [c for c in cols if c and c.upper() != 'ON']
            # Reconstruct list safely
            actual_cols = []
            for c in cols:
                if ' ON ' in c.upper():
                    c = c.upper().split(' ON ')[0].strip()
                if c.upper().endswith(' ON'):
                    c = c[:-3].strip()
                if c:
                    actual_cols.append(c)
            if actual_cols:
                pg_event += f" OF {', '.join(actual_cols)}"

        # 3. Referencing Aliases
        old_alias, new_alias = 'OLD', 'NEW'
        old_match = re.search(r'\bOLD\s+AS\s+([a-zA-Z0-9_]+)\b', trigger_sql, re.IGNORECASE)
        if old_match: old_alias = old_match.group(1)

        new_match = re.search(r'\bNEW\s+AS\s+([a-zA-Z0-9_]+)\b', trigger_sql, re.IGNORECASE)
        if new_match: new_alias = new_match.group(1)

        # 4. Extract WHEN and Body
        mode_match = re.search(r'\bMODE\s+DB2SQL\b', trigger_sql, re.IGNORECASE)
        when_clause = ""
        body = ""
        remainder = trigger_sql[mode_match.end():].strip() if mode_match else trigger_sql

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

        # Strip BEGIN ATOMIC / END
        body = re.sub(r'(?i)^BEGIN\s+ATOMIC', '', body).strip()
        body = re.sub(r'(?i)END;?\s*$', '', body).strip()

        # 5. Replacements
        def replace_aliases(text):
            if not text: return text
            if old_alias.upper() != 'OLD':
                text = re.sub(rf'\b{re.escape(old_alias)}\.', 'OLD.', text, flags=re.IGNORECASE)
            if new_alias.upper() != 'NEW':
                text = re.sub(rf'\b{re.escape(new_alias)}\.', 'NEW.', text, flags=re.IGNORECASE)
            return text

        when_clause = replace_aliases(when_clause)
        body = replace_aliases(body)

        # Replace CURRENT DATE / TIMESTAMP
        body = re.sub(r'\bCURRENT\s+DATE\b', 'CURRENT_DATE', body, flags=re.IGNORECASE)
        body = re.sub(r'\bCURRENT\s+TIMESTAMP\b', 'CURRENT_TIMESTAMP', body, flags=re.IGNORECASE)
        when_clause = re.sub(r'\bCURRENT\s+DATE\b', 'CURRENT_DATE', when_clause, flags=re.IGNORECASE)
        when_clause = re.sub(r'\bCURRENT\s+TIMESTAMP\b', 'CURRENT_TIMESTAMP', when_clause, flags=re.IGNORECASE)

        # Handle SIGNAL SQLSTATE and RAISE_ERROR
        body = re.sub(r"(?i)SIGNAL\s+SQLSTATE\s+'([^']+)'\s*\(\s*('[^']+')\s*\);?", r"RAISE EXCEPTION \2 USING ERRCODE = '\1';", body)
        body = re.sub(r"(?i)RAISE_ERROR\s*\(\s*'([^']+)'\s*,\s*('[^']+')\s*\)", r"RAISE EXCEPTION \2 USING ERRCODE = '\1';", body)

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
                body = re.sub(r'(?i)^([A-Za-z0-9_.]+)\s*=', r'\1 := ', body)
            if not body.strip().endswith(';'):
                body += ';'

        # Handle plain updates
        if not body.strip().endswith(';'):
            body += ';'

        # Target Generation
        func_name = f"{trigger_name}_func"

        pg_func = f"""CREATE OR REPLACE FUNCTION "{target_schema_name}"."{func_name}"()
RETURNS TRIGGER AS $$
BEGIN
{body}
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""
        when_sql = f"\nWHEN ({when_clause})" if when_clause else ""
        pg_trigger = f"""CREATE TRIGGER "{trigger_name}"
{timing} {pg_event} ON "{target_schema_name}"."{target_table_name}"
FOR EACH ROW{when_sql}
EXECUTE FUNCTION "{target_schema_name}"."{func_name}"();
"""

        self.config_parser.print_log_message('DEBUG', f"ibm_db2_zos_connector: convert_trigger: Converted {trigger_name}")
        return pg_func + '\n' + pg_trigger

    def fetch_funcproc_names(self, schema: str):
        return {}

    def fetch_funcproc_code(self, funcproc_id: int):
        return ""

    def convert_funcproc_code(self, settings):
        pass

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
            query = f"""SELECT id, source_schema_name, source_view_name
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
                    'comment': None
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
                    'comment': None
                }

            cursor.close()
        return views

    def get_aliases(self, settings):
        source_schema_name = settings.get('source_schema_name')
        aliases = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT id, source_schema_name, source_alias_name, source_target_schema, source_target_name, source_alias_sql, source_alias_comment
                        FROM "{self.protocol_schema}"."ddl_aliases"
                        WHERE source_schema_name = %s ORDER BY id"""
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
                    'alias_comment': row[6]
                }
            cursor.close()
        return aliases

    def fetch_view_code(self, settings):
        source_schema_name = settings.get('source_schema_name')
        source_view_name = settings.get('source_view_name')
        
        # Check if this view itself is an alias acting as a view
        alias_target_schema = settings.get('target_schema_name')
        alias_target_view = settings.get('target_view_name')
        
        if alias_target_schema and alias_target_view:
             return f'CREATE VIEW "{source_schema_name}"."{source_view_name}" AS SELECT * FROM "{alias_target_schema}"."{alias_target_view}"'

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

        def quote_column_names(node):
            if isinstance(node, sqlglot.exp.Column) and node.name:
                converted_name = self.config_parser.convert_names_case(node.name)
                node.set("this", sqlglot.exp.Identifier(this=converted_name, quoted=True))
            if isinstance(node, sqlglot.exp.Alias) and isinstance(node.args.get("alias"), sqlglot.exp.Identifier):
                alias = node.args["alias"]
                converted_alias = self.config_parser.convert_names_case(alias.name)
                alias.set("this", converted_alias)
                if not alias.args.get("quoted"):
                    alias.set("quoted", True)
            if isinstance(node, sqlglot.exp.Schema):
                for expr in node.expressions:
                    if isinstance(expr, sqlglot.exp.Identifier):
                        converted_name = self.config_parser.convert_names_case(expr.name)
                        expr.set("this", converted_name)
                        if not expr.args.get("quoted"):
                            expr.set("quoted", True)
            return node

        def replace_schema_names(node):
            if isinstance(node, sqlglot.exp.Table):
                schema = node.args.get("db")
                if schema and schema.name.upper() == settings['source_schema_name'].upper():
                    node.set("db", sqlglot.exp.Identifier(this=settings['target_schema_name'], quoted=False))
            return node

        def quote_schema_and_table_names(node):
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
                    if self.config_parser.get_use_aliases_as_target_names() and settings.get('migrator_tables'):
                        alias_name = settings['migrator_tables'].get_alias_for_table(schema_name_for_lookup, table.name)
                        if alias_name:
                            self.config_parser.print_log_message('INFO', f"ibm_db2_zos_connector: convert_view_code: Replaced referenced table '{table.name}' with alias '{alias_name}' inside view generation.")
                            table_name_to_use = alias_name

                    converted_table = self.config_parser.convert_names_case(table_name_to_use)
                    table.set("this", converted_table)
                    if not table.args.get("quoted"):
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

        view_code = settings['view_code']
        converted_code = view_code

        remote_subs = self.config_parser.get_remote_objects_substitution()
        if remote_subs:
            iterator = remote_subs.items() if isinstance(remote_subs, dict) else remote_subs
            for source_obj, target_obj in iterator:
                if source_obj and target_obj:
                    converted_code = re.sub(re.escape(source_obj), target_obj, converted_code, flags=re.IGNORECASE)

        if settings['target_db_type'] == 'postgresql':
            sql_functions_mapping = self.get_sql_functions_mapping({ 'target_db_type': settings['target_db_type'] })
            if sql_functions_mapping:
                for src_func, tgt_func in sql_functions_mapping.items():
                    escaped_src_func = re.escape(src_func)
                    if escaped_src_func.endswith(r'\(') or escaped_src_func.endswith(r'\)'):
                        converted_code = re.sub(rf"(?i)\b{escaped_src_func}", tgt_func, converted_code, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
                    else:
                        converted_code = re.sub(rf"(?i)\b{escaped_src_func}\b", tgt_func, converted_code, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)

            try:
                # Use default sqlglot dialect because 'db2' dialect is not supported
                parsed_code = sqlglot.parse_one(converted_code)
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"ibm_db2_zos_connector: convert_view_code: Error parsing View code: {e}")
                # Fallback to the unparsed converted_code instead of empty string to avoid crashes
                return converted_code

            parsed_code = parsed_code.transform(quote_column_names)
            parsed_code = parsed_code.transform(convert_string_concatenation)
            parsed_code = parsed_code.transform(quote_schema_and_table_names)
            parsed_code = parsed_code.transform(replace_schema_names)
            parsed_code = parsed_code.transform(replace_functions)

            converted_code = parsed_code.sql(dialect="postgres")
            converted_code = converted_code.replace("()()", "()")

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

    def get_rows_count(self, table_schema: str, table_name: str):
        return 0

    def get_table_size(self, table_schema: str, table_name: str):
        return 0

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

if __name__ == "__main__":
    print("This script is not meant to be run directly")
