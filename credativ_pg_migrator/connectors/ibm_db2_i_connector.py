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

class IbmDb2IConnector(DatabaseConnector):
    """
    IBM DB2 for i (IBM i / AS/400) connector supporting DDL connectivity
    parsing DDL SQL files (with DB2 for i specific syntax like FOR SYSTEM NAME,
    FOR COLUMN, CCSID, RECORD FORMAT, LABEL ON) and migrating data via CSV files.
    """

    def __init__(self, config_parser, source_or_target):
        if source_or_target != 'source':
            raise ValueError("IBM DB2 for i is only supported as a source database")

        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.config_parser.print_log_message('DEBUG3', "ibm_db2_i_connector: __init__: Starting INIT")
        self.connectivity = self.config_parser.get_connectivity(self.source_or_target)
        self.on_error_action = self.config_parser.get_on_error_action()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger
        self.source_db_config = self.config_parser.get_source_config()

        if self.connectivity == self.config_parser.const_connectivity_ddl():
            self.ddl_path = self.source_db_config['ddl']['path']
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: __init__: Source_db_config: {self.source_db_config} - ddl_path: {self.ddl_path}")

            self.ddl_files = []
            if os.path.exists(self.ddl_path) and os.path.isdir(self.ddl_path):
                self.ddl_files = glob.glob(os.path.join(self.ddl_path, '*.*'))
            else:
                self.ddl_files = glob.glob(self.ddl_path)

            if not self.ddl_files:
                raise ValueError(f"No DDL files found for path or mask: '{self.ddl_path}'")

            self.config_parser.print_log_message('INFO', f"ibm_db2_i_connector: __init__: DDL path valid: '{self.ddl_path}', found {len(self.ddl_files)} files")

            extension_counts = {}
            for filepath in self.ddl_files:
                if os.path.isfile(filepath):
                    ext = os.path.splitext(filepath)[1]
                    extension_counts[ext] = extension_counts.get(ext, 0) + 1
            for ext, count in extension_counts.items():
                self.config_parser.print_log_message('INFO', f"ibm_db2_i_connector: __init__: Found {count} files with extension '{ext}'")
        else:
            raise ValueError(f"Unsupported IBM DB2 for i connectivity: {self.connectivity}")

        self.migrator_tables = MigratorTables(self.logger, self.config_parser)
        self.protocol_schema = self.migrator_tables.protocol_schema
        self.config_parser.print_log_message('DEBUG3', "ibm_db2_i_connector: __init__: INIT done")

    def connect(self):
        self.config_parser.print_log_message('DEBUG', "ibm_db2_i_connector: connect: connect() called.")
        pass

    def disconnect(self):
        self.config_parser.print_log_message('DEBUG', "ibm_db2_i_connector: disconnect: disconnect() called.")
        pass

    def fetch_all_tables(self, schema_name: str) -> dict:
        tables = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT source_schema_name, source_table_name, source_partition_columns, source_partition_ranges
                        FROM "{self.protocol_schema}"."ddl_tables"
                        WHERE upper(trim(source_schema_name)) = upper(trim('{schema_name}'))
                        ORDER BY id"""
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: fetch_all_tables: ({schema_name}): starting: schema_name: {schema_name} - self.connectivity: {self.connectivity} - query: {query}")
            try:
                cursor = self.migrator_tables.protocol_connection.connection.cursor()
                cursor.execute(query)
                rows = cursor.fetchall()
                self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: fetch_all_tables: ({schema_name}): {rows}")
                for i, row in enumerate(rows, 1):
                    tables[i] = {
                        'id': i,
                        'schema_name': self.config_parser.convert_names_case(row[0]),
                        'table_name': self.config_parser.convert_names_case(row[1]),
                        'comment': f"Partition: {row[2]}, Ranges: {row[3]}" if row[2] else None
                    }
                cursor.close()
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"ibm_db2_i_connector: fetch_all_tables: ({schema_name}): {e}")
                raise
        return tables

    def fetch_table_columns(self, settings) -> dict:
        self.config_parser.print_log_message('DEBUG', "ibm_db2_i_connector: fetch_table_columns: fetch_table_columns() called.")
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
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: fetch_table_columns: ({table_schema}.{table_name}): {rows}")
            for i, row in enumerate(rows, 1):
                col_name = self.config_parser.convert_names_case(row[0])
                col_type = row[1]
                is_nullable = 'YES' if row[2] else 'NO'
                default_val = row[3]
                is_pk = row[4]
                is_identity = 'YES' if row[5] else 'NO'

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
                    elif base_type in ['DECIMAL', 'NUMERIC', 'PACKED', 'ZONED']:
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
                'PACKED': 'DECIMAL',
                'NUMERIC': 'NUMERIC',
                'ZONED': 'NUMERIC',
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
                'TIMESTMP': 'TIMESTAMP',
                'DATETIME': 'TIMESTAMP',
                'XML': 'XML',
                'ROWID': 'BYTEA'
            }
        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")
        return types_mapping

    def parse_ddl_files(self, settings):
        self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: parse_ddl_files: Starting DDL parser - self.ddl_files: {self.ddl_files}")
        migrator_tables = settings['migrator_tables']
        if not migrator_tables:
            self.config_parser.print_log_message('ERROR', "ibm_db2_i_connector: parse_ddl_files: migrator_tables not found in settings.")
            return

        for filepath in self.ddl_files:
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: parse_ddl_files: Processing file: {filepath}")
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()

            # Extract triggers first to avoid splitting by semicolons inside trigger bodies
            trigger_pattern = re.compile(r"(CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?[\s\S]*?(?=(?:CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW|INDEX|UNIQUE\s+INDEX|ALIAS|SEQUENCE|TRIGGER))|(?:ALTER\s+TABLE)|(?:SET\s+CURRENT\s+SCHEMA)|$))", re.IGNORECASE)
            for match in trigger_pattern.finditer(content):
                self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: parse_ddl_files: Found trigger: {match.group(1)}")
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

            content = trigger_pattern.sub("", content)

            statements = re.split(r';|@(?=\s*(?:\n|$))', content)
            for stmt in statements:
                stmt = stmt.strip()
                if not stmt:
                    continue

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

                # Strip DB2 for i RENAME TABLE / INDEX / COLUMN statements if standalone
                if re.search(r"^\s*RENAME\s+(?:TABLE|INDEX)\b", clean_stmt, re.IGNORECASE):
                    continue

                # Parse Comments (COMMENT ON and DB2 for i LABEL ON statements)
                match_comment = re.search(r"^(?:COMMENT|LABEL)\s+ON\s+(TABLE|COLUMN|INDEX|VIEW|ALIAS|TRIGGER|SEQUENCE)\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?(?:\.\"?([A-Za-z0-9_]+)\"?)?\s+(?:IS|TEXT\s+IS)\s+'(.*)'", clean_stmt, re.IGNORECASE | re.DOTALL)
                if match_comment:
                    self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: parse_ddl_files: Found comment: {match_comment.group(1)}")
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

                # Parse DB2 for i multi-column LABEL ON COLUMN (schema.table (col1 IS 'text1', col2 TEXT IS 'text2'))
                match_multi_label = re.search(r"^LABEL\s+ON\s+COLUMN\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?\s*\(([\s\S]+)\)", clean_stmt, re.IGNORECASE)
                if match_multi_label:
                    schema_name = match_multi_label.group(1).upper()
                    tbl_name = match_multi_label.group(2).upper()
                    labels_block = match_multi_label.group(3)
                    for item in re.split(r',\s*(?=[A-Za-z0-9_]+\s+(?:TEXT\s+)?IS)', labels_block):
                        item_match = re.search(r"\"?([A-Za-z0-9_]+)\"?\s+(?:TEXT\s+)?IS\s+'(.*)'", item.strip(), re.IGNORECASE | re.DOTALL)
                        if item_match:
                            col_n = item_match.group(1).upper()
                            comm_v = item_match.group(2)
                            migrator_tables.update_ddl_comment({
                                'object_type': 'COLUMN',
                                'source_schema_name': schema_name,
                                'source_name': tbl_name,
                                'source_column_name': col_n,
                                'comment': comm_v
                            })
                    continue

                # Parse Indexes (CREATE INDEX ... FOR SYSTEM NAME ...)
                match_index = re.search(r"^CREATE\s+(UNIQUE\s+)?INDEX\s+(?:\"?([A-Za-z0-9_]+)\"?\.)?\"?([A-Za-z0-9_]+)\"?(?:\s+FOR\s+SYSTEM\s+NAME\s+[A-Za-z0-9_]+)?\s+ON\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?", clean_stmt, re.IGNORECASE)
                if match_index:
                    self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: parse_ddl_files: Found index: {match_index.group(3)}")
                    is_unique = bool(match_index.group(1))
                    idx_schema = match_index.group(2).upper() if match_index.group(2) else match_index.group(4).upper()
                    idx_name = match_index.group(3).upper()
                    tbl_schema = match_index.group(4).upper()
                    tbl_name = match_index.group(5).upper()

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
                                    cols_list.append(col_stmt[0].strip('"').upper())

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
                    self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: parse_ddl_files: Found sequence: {match_seq.group(2)}")
                    schema_name = match_seq.group(1).upper()
                    seq_name = match_seq.group(2).upper()

                    seq_sql = f'CREATE SEQUENCE "{schema_name.lower()}"."{seq_name.lower()}"'
                    seq_params_str = clean_stmt.upper()

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
                        seq_sql += " CACHE 1"
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

                # Parse Views (CREATE OR REPLACE VIEW ... FOR SYSTEM NAME ...)
                match_view = re.search(r"^CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?(?:\s+FOR\s+SYSTEM\s+NAME\s+[A-Za-z0-9_]+)?", clean_stmt, re.IGNORECASE)
                if match_view:
                    self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: parse_ddl_files: Found view: {match_view.group(2)}")
                    schema_name = match_view.group(1).upper()
                    view_name = match_view.group(2).upper()
                    migrator_tables.insert_ddl_views({
                        'source_schema_name': schema_name,
                        'source_view_name': view_name,
                        'source_view_sql': stmt,
                        'source_view_comment': comment_text
                    })
                    continue

                # Parse Aliases (CREATE ALIAS ... FOR SYSTEM NAME ...)
                match_alias = re.search(r"^CREATE\s+ALIAS\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?(?:\s+FOR\s+SYSTEM\s+NAME\s+[A-Za-z0-9_]+)?\s+FOR\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?", clean_stmt, re.IGNORECASE)
                if match_alias:
                    self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: parse_ddl_files: Found alias: {match_alias.group(2)}")
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
                match_fk = re.search(r"^ALTER\s+TABLE\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?\s+ADD\s+CONSTRAINT\s+\"?([A-Za-z0-9_]+)\"?\s+FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?\s*\(([^)]+)\)", clean_stmt, re.IGNORECASE)
                if match_fk:
                    self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: parse_ddl_files: Found foreign key: {match_fk.group(3)}")
                    tbl_schema = match_fk.group(1).upper()
                    tbl_name = match_fk.group(2).upper()
                    fk_name = match_fk.group(3).upper()
                    cols_str = match_fk.group(4)
                    ref_schema = match_fk.group(5).upper()
                    ref_name = match_fk.group(6).upper()
                    ref_cols_str = match_fk.group(7)

                    cols_list = [c.strip().strip('"').upper() for c in cols_str.split(',')]
                    ref_cols_list = [c.strip().strip('"').upper() for c in ref_cols_str.split(',')]

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

                # Parse CREATE TABLE / CREATE OR REPLACE TABLE (with FOR SYSTEM NAME, RECORD FORMAT, CCSID)
                match_table = re.search(r"^CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?(?:\s+FOR\s+SYSTEM\s+NAME\s+[A-Za-z0-9_]+)?", clean_stmt, re.IGNORECASE)
                if not match_table:
                    continue

                schema_name = match_table.group(1).upper()
                table_name = match_table.group(2).upper()

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
                for col_def in col_defs:
                    if col_def.upper().startswith("PRIMARY KEY"):
                        match_pk = re.search(r"\((.*)\)", col_def)
                        if match_pk:
                            pks = [p.strip().strip('"').upper() for p in match_pk.group(1).split(',')]
                            pk_columns.update(pks)

                trailing_str = clean_stmt[end_idx+1:]
                partition_col = None
                partition_ranges = None

                match_part = re.search(r"PARTITION\s+BY\s*\(\s*([^)]+)\s*\)\s*\(([\s\S]*?)\)\s*(?:IN|;|$)", trailing_str, re.IGNORECASE)
                if match_part:
                    partition_col = match_part.group(1).replace(" ASC", "").replace(" DESC", "").strip()
                    partition_ranges = match_part.group(2).strip()

                self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: parse_ddl_files: Found table: {match_table.group(2)}")
                migrator_tables.insert_ddl_tables({
                    'source_schema_name': schema_name,
                    'source_table_name': table_name,
                    'source_partition_columns': partition_col,
                    'source_partition_ranges': partition_ranges,
                    'source_table_sql': stmt,
                    'source_table_comment': comment_text
                })

                for col_def in col_defs:
                    col_def_u = col_def.upper()
                    if col_def_u.startswith("PRIMARY KEY") or col_def_u.startswith("CONSTRAINT") or col_def_u.startswith("FOREIGN KEY") or col_def_u.startswith("UNIQUE"):
                        continue

                    # Handle DB2 for i system column name syntax: <long_col_name> FOR COLUMN <sys_col_name> <data_type> ...
                    col_def_clean = re.sub(r"(?i)\bFOR\s+COLUMN\s+[A-Za-z0-9_]+\b", "", col_def).strip()

                    parts = col_def_clean.split(maxsplit=1)
                    if len(parts) < 2:
                        continue

                    col_name = parts[0].strip('"').upper()
                    rest = parts[1]

                    type_match = re.match(r"([A-Za-z0-9_]+(?:\s*\([^)]+\))?)", rest, re.IGNORECASE)
                    if not type_match:
                        continue

                    data_type = type_match.group(1).upper()
                    after_type = rest[len(data_type):].strip()

                    # Strip DB2 for i CCSID specifications
                    after_type = re.sub(r"(?i)\bCCSID\s+\d+\b", "", after_type).strip()

                    is_nullable = True
                    if "NOT NULL" in after_type.upper():
                        is_nullable = False

                    is_identity = False
                    default_value = None

                    identity_match = re.search(r"GENERATED\s+(?:ALWAYS|BY\s+DEFAULT)\s+AS\s+IDENTITY(?:\s*\(([^)]+)\))?", after_type, re.IGNORECASE)

                    if identity_match:
                        self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: parse_ddl_files: Found identity: {identity_match.group(0)}")
                        is_identity = True
                        seq_params_str = identity_match.group(1) if identity_match.lastindex and identity_match.group(identity_match.lastindex) else ""

                        seq_name = f"{table_name}_{col_name}_seq".lower()
                        default_value = f"nextval('\"{schema_name.lower()}\".\"{seq_name}\"')"

                        seq_sql = f'CREATE SEQUENCE "{schema_name.lower()}"."{seq_name}"'

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
                                seq_sql += " CACHE 1"
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
                        default_match = re.search(r"(?:WITH\s+DEFAULT|DEFAULT)\s+('[^']*'|-?[0-9\.]+|[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?)?", after_type, re.IGNORECASE)
                        if default_match:
                            val = default_match.group(1)
                            if val is None or val.upper() in ('NOT NULL', 'GENERATED', 'CONSTRAINT'):
                                default_value = "SYSTEM DEFAULT"
                            else:
                                default_value = val

                    is_pk = col_name in pk_columns

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

        if schemas:
            most_frequent_schema = max(set(schemas), key=schemas.count)
            self.config_parser.set_source_schema(most_frequent_schema)

        self.config_parser.print_log_message('INFO', "ibm_db2_i_connector: parse_ddl_files: DDL parsing completed and unified protocol tables populated with DB2 for i source metadata.")

    def get_sql_functions_mapping(self, settings):
        target_db_type = settings['target_db_type']
        if target_db_type == 'postgresql':
            return {
                "CURRENT SQLID": "CURRENT_USER",
                "CURRENT USER": "CURRENT_USER",
                "USER": "SESSION_USER",
                "CURRENT DATE": "CURRENT_DATE",
                "CURRENT TIME": "CURRENT_TIME",
                "CURRENT TIMESTAMP": "CURRENT_TIMESTAMP",
                "CURRENT SCHEMA": "CURRENT_SCHEMA",
                "CURRENT SERVER": "current_database()",
                "VARCHAR_FORMAT(": "to_char(",
                "POSSTR(": "position(",
                "LISTAGG(": "string_agg(",
                "SUBSTR(": "substring(",
                "VALUE(": "coalesce(",
                "NVL(": "coalesce(",
            }
        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

    def fetch_indexes(self, settings) -> dict:
        table_schema = settings.get('table_schema')
        table_name = settings.get('table_name')
        pk_columns_list = settings.get('pk_columns_list', [])
        indexes = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT source_index_name, source_is_unique, source_columns_list, source_index_comment
                        FROM "{self.protocol_schema}"."ddl_indexes"
                        WHERE trim(source_schema_name) = trim(%s) AND trim(source_table_name) = trim(%s) ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (table_schema, table_name))
            rows = cursor.fetchall()
            for i, row in enumerate(rows, 1):
                idx_name = self.config_parser.convert_names_case(row[0])
                is_unique = row[1]
                col_expr = ', '.join([self.config_parser.convert_names_case(c.strip()) for c in row[2].split(',')]) if row[2] else ''
                comment = row[3]

                if pk_columns_list and is_unique:
                    idx_cols = [c.strip().upper() for c in col_expr.split(',')]
                    pk_cols = [c.strip().upper() for c in pk_columns_list]
                    if idx_cols == pk_cols:
                        continue

                indexes[i] = {
                    'index_name': idx_name,
                    'is_unique': is_unique,
                    'is_primary': False,
                    'is_clustered': False,
                    'column_expression': col_expr,
                    'index_comment': comment
                }
            cursor.close()
        return indexes

    def fetch_constraints(self, settings) -> dict:
        table_schema = settings.get('table_schema')
        table_name = settings.get('table_name')
        constraints = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT source_fk_name, source_columns_list, source_ref_schema_name, source_ref_table_name, source_ref_columns_list
                        FROM "{self.protocol_schema}"."ddl_foreign_keys"
                        WHERE trim(source_schema_name) = trim(%s) AND trim(source_table_name) = trim(%s) ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (table_schema, table_name))
            rows = cursor.fetchall()
            for i, row in enumerate(rows, 1):
                fk_name = self.config_parser.convert_names_case(row[0])
                pk_cols = ', '.join([self.config_parser.convert_names_case(c.strip()) for c in row[1].split(',')]) if row[1] else ''
                ref_schema = row[2]
                ref_table = self.config_parser.convert_names_case(row[3])
                fk_cols = ', '.join([self.config_parser.convert_names_case(c.strip()) for c in row[4].split(',')]) if row[4] else ''

                constraints[i] = {
                    'constraint_name': fk_name,
                    'constraint_type': 'FOREIGN KEY',
                    'check_clause': None,
                    'referenced_schema_name': ref_schema,
                    'referenced_table_name': ref_table,
                    'pk_columns': pk_cols,
                    'fk_columns': fk_cols,
                    'fk_delete_rule': 'NO ACTION',
                    'fk_update_rule': 'NO ACTION',
                    'constraint_comment': None
                }
            cursor.close()
        return constraints

    def fetch_triggers(self, settings) -> dict:
        table_schema = settings.get('table_schema')
        triggers = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT id, source_trigger_name, source_ddl_text, source_trigger_comment
                        FROM "{self.protocol_schema}"."ddl_triggers"
                        WHERE trim(source_schema_name) = trim(%s) ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (table_schema,))
            rows = cursor.fetchall()
            for i, row in enumerate(rows, 1):
                triggers[i] = {
                    'id': row[0],
                    'name': self.config_parser.convert_names_case(row[1]),
                    'sql': row[2],
                    'comment': row[3]
                }
            cursor.close()
        return triggers

    def convert_trigger(self, settings: dict):
        trigger_sql = settings.get('trigger_sql', '')
        trigger_name = settings.get('trigger_name', '')
        target_schema_name = settings.get('target_schema_name', '')
        target_table_name = settings.get('target_table_name', '')

        trigger_sql = re.sub(r'--([^\n]*)', r'/*\1*/', trigger_sql)

        timing_match = re.search(r'\b(BEFORE|AFTER|INSTEAD\s+OF)\b', trigger_sql, re.IGNORECASE)
        timing = timing_match.group(1).upper() if timing_match else 'BEFORE'

        event_match = re.search(r'\b(INSERT|UPDATE|DELETE)(?:\s+OF\s+([a-zA-Z0-9_,\s"]+))?\b', trigger_sql[timing_match.end():] if timing_match else trigger_sql, re.IGNORECASE)
        event = event_match.group(1).upper() if event_match else 'UPDATE'
        of_cols = event_match.group(2) if event_match and event_match.group(2) else None

        pg_event = event
        if of_cols and event == 'UPDATE':
            cols = [c.strip() for c in of_cols.split(',')]
            cols = [c for c in cols if c and c.upper() != 'ON']
            actual_cols = []
            for c in cols:
                if ' ON ' in c.upper():
                    c = c.upper().split(' ON ')[0].strip()
                if c.upper().endswith(' ON'):
                    c = c[:-3].strip()
                if c:
                    is_quoted = c.startswith('"') and c.endswith('"')
                    base_c = c.strip('"') if is_quoted else c.upper()
                    converted_c = self.config_parser.convert_names_case(base_c)
                    actual_cols.append(f'"{converted_c}"')
            if actual_cols:
                pg_event += f" OF {', '.join(actual_cols)}"

        old_alias, new_alias = 'OLD', 'NEW'
        old_table_alias, new_table_alias = None, None

        old_match = re.search(r'\bOLD\s+AS\s+([a-zA-Z0-9_]+)\b', trigger_sql, re.IGNORECASE)
        if old_match: old_alias = old_match.group(1)

        new_match = re.search(r'\bNEW\s+AS\s+([a-zA-Z0-9_]+)\b', trigger_sql, re.IGNORECASE)
        if new_match: new_alias = new_match.group(1)

        for_each_match = re.search(r'\bFOR\s+EACH\s+(ROW|STATEMENT)\b', trigger_sql, re.IGNORECASE)
        for_each_scope = for_each_match.group(1).upper() if for_each_match else 'ROW'

        start_pos = 0
        if for_each_match:
            start_pos = for_each_match.end()

        remainder = trigger_sql[start_pos:].strip()

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

        body = re.sub(r'(?i)^BEGIN\s+ATOMIC\s+', '', body).strip()
        body = re.sub(r'(?i)^BEGIN\s+', '', body).strip()
        body = re.sub(r'(?i)END;?\s*$', '', body).strip()

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
                converted_field = self.config_parser.convert_names_case(base_field)
                return f'{prefix}."{converted_field}"'

            text = re.sub(r'\b(OLD|NEW)\.([a-zA-Z0-9_"]+)\b', replace_record_field, text, flags=re.IGNORECASE)
            return text

        when_clause = replace_aliases(when_clause)
        body = replace_aliases(body)

        body = re.sub(r'(?i)\bVARCHAR\s*\(\s*(COUNT\s*\([^()]*\)|[a-zA-Z0-9_.]+\s*[+*/|-]\s*[^()]+|[a-zA-Z0-9_.]+\.[a-zA-Z0-9_.]+|[a-zA-Z_][a-zA-Z0-9_]*\([^()]*\))\s*\)', r'CAST(\1 AS VARCHAR)', body)
        body = re.sub(r'\bCURRENT\s+DATE\b', 'CURRENT_DATE', body, flags=re.IGNORECASE)
        body = re.sub(r'\bCURRENT\s+TIMESTAMP\b', 'CURRENT_TIMESTAMP', body, flags=re.IGNORECASE)
        when_clause = re.sub(r'\bCURRENT\s+DATE\b', 'CURRENT_DATE', when_clause, flags=re.IGNORECASE)
        when_clause = re.sub(r'\bCURRENT\s+TIMESTAMP\b', 'CURRENT_TIMESTAMP', when_clause, flags=re.IGNORECASE)

        body = re.sub(
            r"(?i)\bSIGNAL\s+SQLSTATE\s+(?:VALUE\s+)?'([^']+)'\s+SET\s+MESSAGE_TEXT\s*=\s*('[^']+'|[a-zA-Z0-9_.]+);?",
            r"RAISE EXCEPTION \2 USING ERRCODE = '\1';",
            body,
            flags=re.MULTILINE | re.DOTALL
        )
        body = re.sub(
            r"(?i)\bSIGNAL\s+SQLSTATE\s+'([^']+)'\s*\(\s*('[^']+'|[a-zA-Z0-9_.]+)\s*\);?",
            r"RAISE EXCEPTION \2 USING ERRCODE = '\1';",
            body
        )

        if body.upper().startswith('SET'):
            body = re.sub(r'(?i)^SET\s*', '', body)
            tuple_match = re.match(r'^\(\s*([^)]+)\s*\)\s*=\s*\(\s*(.+)\s*\);?$', body, re.IGNORECASE | re.DOTALL)
            if tuple_match:
                cols = [c.strip() for c in tuple_match.group(1).split(',')]
                vals = [c.strip() for c in tuple_match.group(2).split(',')]
                if len(cols) == 1:
                    body = f"{cols[0]} := {tuple_match.group(2)};"
                elif len(cols) == len(vals):
                    body = "\n".join([f"{c} := {v};" for c, v in zip(cols, vals)])
            else:
                body = re.sub(r'(?i)^([A-Za-z0-9_."]+)\s*=', r'\1 := ', body)
            if not body.strip().endswith(';'):
                body += ';'

        if not body.strip().endswith(';'):
            body += ';'

        target_table_name = self.config_parser.convert_names_case(target_table_name)
        converted_trigger_name = self.config_parser.convert_names_case(trigger_name)
        func_name = f"{converted_trigger_name}_func"

        if for_each_scope == 'STATEMENT' or timing == 'AFTER':
            return_stmt = "RETURN NULL;"
        elif timing == 'BEFORE' and event == 'DELETE':
            return_stmt = "RETURN OLD;"
        else:
            return_stmt = "RETURN NEW;"

        pg_func = f"""CREATE OR REPLACE FUNCTION "{target_schema_name}"."{func_name}"()
RETURNS TRIGGER AS $$
BEGIN
{body}
{return_stmt}
END;
$$ LANGUAGE plpgsql;
"""
        when_sql = f"\nWHEN ({when_clause})" if when_clause else ""
        pg_trigger = f"""CREATE TRIGGER "{converted_trigger_name}"
{timing} {pg_event} ON "{target_schema_name}"."{target_table_name}"
FOR EACH {for_each_scope}{when_sql}
EXECUTE FUNCTION "{target_schema_name}"."{func_name}"();
"""
        return pg_func + '\n' + pg_trigger

    def fetch_sequences(self, settings) -> dict:
        table_schema = settings.get('table_schema')
        sequences = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT id, source_seq_name, source_table_name, source_column_name, source_start_value, source_increment_by, source_minvalue, source_maxvalue, source_cache, source_is_cycled, source_ddl_text
                        FROM "{self.protocol_schema}"."ddl_sequences"
                        WHERE trim(source_schema_name) = trim(%s) ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (table_schema,))
            rows = cursor.fetchall()
            for i, row in enumerate(rows, 1):
                sequences[i] = {
                    'id': row[0],
                    'sequence_name': self.config_parser.convert_names_case(row[1]),
                    'table_name': self.config_parser.convert_names_case(row[2]) if row[2] else None,
                    'column_name': self.config_parser.convert_names_case(row[3]) if row[3] else None,
                    'start_value': row[4],
                    'increment_by': row[5],
                    'min_value': row[6],
                    'max_value': row[7],
                    'cache_size': row[8],
                    'is_cycled': row[9],
                    'ddl_text': row[10]
                }
            cursor.close()
        return sequences

    def migrate_sequences(self, settings):
        sequence_info = settings.get('sequence_info', {})
        target_schema_name = settings.get('target_schema_name', '')
        source_seq_name = sequence_info.get('sequence_name')
        target_sequence_name = self.config_parser.convert_names_case(source_seq_name)

        target_connection = settings.get('target_connection')
        ddl_text = sequence_info.get('ddl_text')
        if not ddl_text:
            ddl_text = f'CREATE SEQUENCE "{target_schema_name}"."{target_sequence_name}"'

        ddl_text = re.sub(r'CREATE\s+SEQUENCE\s+"[^"]+"\."[^"]+"', f'CREATE SEQUENCE "{target_schema_name}"."{target_sequence_name}"', ddl_text, flags=re.IGNORECASE)

        try:
            target_connection.execute_query(ddl_text)
            self.config_parser.print_log_message('INFO', f"ibm_db2_i_connector: migrate_sequences: Created sequence {target_sequence_name}")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ibm_db2_i_connector: migrate_sequences: Error creating sequence {target_sequence_name}: {e}")
            raise

    def fetch_views_names(self, settings) -> dict:
        source_schema_name = settings.get('source_schema_name')
        views = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT source_view_name, source_view_comment
                        FROM "{self.protocol_schema}"."ddl_views"
                        WHERE trim(source_schema_name) = trim(%s) ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (source_schema_name,))
            rows = cursor.fetchall()
            for i, row in enumerate(rows, 1):
                views[i] = {
                    'view_name': self.config_parser.convert_names_case(row[0]),
                    'view_comment': row[1]
                }
            cursor.close()

            query_aliases = f"""SELECT source_alias_name, source_alias_comment, alias_target_type
                                FROM "{self.protocol_schema}"."ddl_aliases"
                                WHERE trim(source_schema_name) = trim(%s) ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query_aliases, (source_schema_name,))
            alias_rows = cursor.fetchall()
            cursor.close()

            idx = len(views) + 1
            for row in alias_rows:
                alias_name = self.config_parser.convert_names_case(row[0])
                alias_comment = row[1]
                alias_target_type = row[2]

                if alias_target_type == 'TABLE':
                    self.config_parser.print_log_message('INFO', f"ibm_db2_i_connector: fetch_views_names: Database Alias '{alias_name}' points to a TABLE. Creating SQL VIEW to expose target table pointers.")
                    views[idx] = {
                        'view_name': alias_name,
                        'view_comment': alias_comment,
                        'alias_view': True
                    }
                    idx += 1
                else:
                    self.config_parser.print_log_message('DEBUG', f"ibm_db2_i_connector: fetch_views_names: Database Alias '{alias_name}' points to a VIEW. Skipping alias view wrapper.")
        return views

    def get_aliases(self, settings) -> dict:
        source_schema_name = settings.get('source_schema_name')
        aliases = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT source_alias_name, source_target_schema, source_target_name, alias_target_type
                        FROM "{self.protocol_schema}"."ddl_aliases"
                        WHERE trim(source_schema_name) = trim(%s) ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (source_schema_name,))
            rows = cursor.fetchall()
            for i, row in enumerate(rows, 1):
                aliases[i] = {
                    'alias_name': self.config_parser.convert_names_case(row[0]),
                    'aliased_table_schema': row[1],
                    'aliased_table_name': self.config_parser.convert_names_case(row[2]),
                    'alias_target_type': row[3] if len(row) > 3 and row[3] else 'UNKNOWN'
                }
            cursor.close()
        return aliases

    def fetch_view_code(self, settings) -> str:
        source_schema_name = settings.get('source_schema_name')
        source_view_name = settings.get('source_view_name')
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT source_view_sql
                        FROM "{self.protocol_schema}"."ddl_views"
                        WHERE trim(source_schema_name) = trim(%s) AND upper(trim(source_view_name)) = upper(trim(%s))"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (source_schema_name, source_view_name))
            row = cursor.fetchone()
            cursor.close()
            if row:
                return row[0]

            query_alias = f"""SELECT source_target_schema, source_target_name
                              FROM "{self.protocol_schema}"."ddl_aliases"
                              WHERE trim(source_schema_name) = trim(%s) AND upper(trim(source_alias_name)) = upper(trim(%s))"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query_alias, (source_schema_name, source_view_name))
            alias_row = cursor.fetchone()
            cursor.close()
            if alias_row:
                target_schema = alias_row[0]
                target_name = self.config_parser.convert_names_case(alias_row[1])
                target_schema_name = settings.get('target_schema_name', target_schema)
                return f'CREATE VIEW "{target_schema_name}"."{self.config_parser.convert_names_case(source_view_name)}" AS SELECT * FROM "{target_schema_name}"."{target_name}"'
        return ""

    def convert_view_code(self, settings: dict):
        cte_names = set()

        def quote_column_names(node):
            if isinstance(node, sqlglot.exp.Column):
                if node.name:
                    base_name = node.name.upper() if not node.args.get("this").args.get("quoted") else node.name
                    converted_name = self.config_parser.convert_names_case(base_name)
                    node.set("this", sqlglot.exp.Identifier(this=converted_name, quoted=True))
                table_id = node.args.get("table")
                if isinstance(table_id, sqlglot.exp.Identifier):
                    base_table = table_id.name.upper() if not table_id.args.get("quoted") else table_id.name
                    converted_table = self.config_parser.convert_names_case(base_table)
                    table_id.set("this", converted_table)
                    table_id.set("quoted", True)
                db_id = node.args.get("db")
                if isinstance(db_id, sqlglot.exp.Identifier):
                    target_schema = settings.get('target_schema_name') or 'public'
                    db_id.set("this", target_schema)
                    db_id.set("quoted", True)
            if isinstance(node, sqlglot.exp.Alias) and isinstance(node.args.get("alias"), sqlglot.exp.Identifier):
                alias = node.args["alias"]
                base_name = alias.name.upper() if not alias.args.get("quoted") else alias.name
                converted_alias = self.config_parser.convert_names_case(base_name)
                alias.set("this", converted_alias)
                alias.set("quoted", True)
            if isinstance(node, sqlglot.exp.Schema):
                for expr in node.expressions:
                    if isinstance(expr, sqlglot.exp.Identifier):
                        base_name = expr.name.upper() if not expr.args.get("quoted") else expr.name
                        converted_name = self.config_parser.convert_names_case(base_name)
                        expr.set("this", converted_name)
                        expr.set("quoted", True)
            if isinstance(node, sqlglot.exp.CTE):
                alias = node.args.get("alias")
                if isinstance(alias, sqlglot.exp.TableAlias):
                    alias_this = alias.args.get("this")
                    if isinstance(alias_this, sqlglot.exp.Identifier):
                        base_cte = alias_this.name.upper() if not alias_this.args.get("quoted") else alias_this.name
                        converted_cte = self.config_parser.convert_names_case(base_cte)
                        alias_this.set("this", converted_cte)
                        alias_this.set("quoted", True)
                    columns = alias.args.get("columns")
                    if columns:
                        for col_id in columns:
                            if isinstance(col_id, sqlglot.exp.Identifier):
                                base_col = col_id.name.upper() if not col_id.args.get("quoted") else col_id.name
                                converted_col = self.config_parser.convert_names_case(base_col)
                                col_id.set("this", converted_col)
                                col_id.set("quoted", True)
            return node

        def quote_schema_and_table_names(node):
            if isinstance(node, sqlglot.exp.TableAlias):
                alias_id = node.args.get("this")
                if isinstance(alias_id, sqlglot.exp.Identifier):
                    base_alias = alias_id.name.upper() if not alias_id.args.get("quoted") else alias_id.name
                    converted_alias = self.config_parser.convert_names_case(base_alias)
                    alias_id.set("this", converted_alias)
                    alias_id.set("quoted", True)
                columns = node.args.get("columns")
                if columns:
                    for col_id in columns:
                        if isinstance(col_id, sqlglot.exp.Identifier):
                            base_col = col_id.name.upper() if not col_id.args.get("quoted") else col_id.name
                            converted_col = self.config_parser.convert_names_case(base_col)
                            col_id.set("this", converted_col)
                            col_id.set("quoted", True)
            if isinstance(node, sqlglot.exp.Table):
                table = node.args.get("this")
                schema = node.args.get("db")
                is_create_view_table = isinstance(node.parent, sqlglot.exp.Create) or isinstance(node.parent, sqlglot.exp.Schema)

                if table:
                    if isinstance(table, sqlglot.exp.Identifier):
                        table_name_to_use = table.name.upper() if not table.args.get("quoted") else table.name
                    else:
                        table_name_to_use = str(table).upper()

                    target_schema = settings.get('target_schema_name') or 'public'

                    if is_create_view_table:
                        view_target = settings.get('target_view_name') or table_name_to_use
                        converted_table = self.config_parser.convert_names_case(view_target)
                        node.set("db", sqlglot.exp.Identifier(this=target_schema, quoted=True))
                    elif table_name_to_use in cte_names:
                        converted_table = self.config_parser.convert_names_case(table_name_to_use)
                        node.set("db", None)
                    else:
                        converted_table = self.config_parser.convert_names_case(table_name_to_use)
                        node.set("db", sqlglot.exp.Identifier(this=target_schema, quoted=True))

                    node.set("this", sqlglot.exp.Identifier(this=converted_table, quoted=True))
            return node

        def replace_functions(node):
            mapping = self.get_sql_functions_mapping({'target_db_type': settings['target_db_type']})
            func_name_map = {k.lower(): v for k, v in mapping.items()}

            if isinstance(node, sqlglot.exp.Anonymous):
                func_name = node.name.lower()
                if func_name in func_name_map:
                    mapped = func_name_map[func_name]
                    node.set("this", sqlglot.exp.Identifier(this=mapped.strip('('), quoted=False))
            return node

        view_code = settings['view_code']
        converted_code = view_code

        converted_code = re.sub(r"(?i)\bFOR\s+SYSTEM\s+NAME\s+[A-Za-z0-9_]+\b", "", converted_code)

        if settings['target_db_type'] == 'postgresql':
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

            try:
                parsed_code = sqlglot.parse_one(converted_code)
                for cte_node in parsed_code.find_all(sqlglot.exp.CTE):
                    if cte_node.alias_or_name:
                        cte_names.add(cte_node.alias_or_name.upper())
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"ibm_db2_i_connector: convert_view_code: Error parsing View code: {e}")
                return converted_code

            parsed_code = parsed_code.transform(quote_column_names)
            parsed_code = parsed_code.transform(quote_schema_and_table_names)
            parsed_code = parsed_code.transform(replace_functions)

            converted_code = parsed_code.sql(dialect="postgres")
            converted_code = converted_code.replace("()()", "()")
            return converted_code
        return view_code

    def convert_funcproc_code(self, settings: dict):
        code = settings.get('code', '')
        code = re.sub(
            r"(?i)\bSIGNAL\s+SQLSTATE\s+(?:VALUE\s+)?'([^']+)'\s+SET\s+MESSAGE_TEXT\s*=\s*('[^']+'|[a-zA-Z0-9_.]+);?",
            r"RAISE EXCEPTION \2 USING ERRCODE = '\1';",
            code,
            flags=re.MULTILINE | re.DOTALL
        )
        return code

    def convert_default_value(self, settings: dict):
        extracted_default_value = settings.get('extracted_default_value', '')
        if not extracted_default_value:
            return None

        val_upper = str(extracted_default_value).strip().upper()

        if val_upper in ('SYSTEM DEFAULT', 'NULL', 'NONE'):
            return None

        if val_upper == 'CURRENT DATE':
            return 'CURRENT_DATE'
        elif val_upper == 'CURRENT TIME':
            return 'CURRENT_TIME'
        elif val_upper in ('CURRENT TIMESTAMP', 'CURRENT_TIMESTAMP'):
            return 'CURRENT_TIMESTAMP'
        elif val_upper in ('CURRENT USER', 'USER'):
            return 'CURRENT_USER'

        return extracted_default_value

    def get_rows_count(self, schema_name: str, table_name: str, settings: dict = None):
        return 0

    def get_table_size(self, table_schema: str, table_name: str):
        return 0

    def target_table_exists(self, target_schema_name, target_table_name):
        return False

    def get_database_version(self):
        return "IBM DB2 for i (DDL/CSV)"

    def get_database_size(self):
        return 0

if __name__ == "__main__":
    print("This script is not meant to be run directly")
