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

                # Materialized query tables (CREATE TABLE ... AS (<query>) DATA INITIALLY DEFERRED ...)
                # are stored as views - they are migrated as PostgreSQL materialized views,
                # their body is a query and not a list of column definitions.
                if re.search(r"(?i)\bDATA\s+INITIALLY\s+DEFERRED\b", clean_stmt):
                    self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: parse_ddl_files: Found materialized query table: {match_table.group(2)}")
                    migrator_tables.insert_ddl_views({
                        'source_schema_name': match_table.group(1).upper(),
                        'source_view_name': match_table.group(2).upper(),
                        'source_view_sql': clean_stmt,
                        'source_view_comment': comment_text,
                        'source_view_type': 'MATERIALIZED VIEW'
                    })
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

    def fetch_table_names(self, table_schema: str):
        return self.fetch_all_tables(table_schema)

    def get_table_description(self, settings) -> dict:
        return {}

    def fetch_default_values(self, settings) -> dict:
        return {}

    def is_string_type(self, column_type: str) -> bool:
        string_types = ['CHAR', 'VARCHAR', 'CLOB', 'GRAPHIC', 'VARGRAPHIC', 'DBCLOB',
                        'NCHAR', 'NVARCHAR', 'TEXT', 'LONG VARCHAR', 'LONG VARGRAPHIC']
        return column_type.upper() in string_types

    def is_numeric_type(self, column_type: str) -> bool:
        numeric_types = ['SMALLINT', 'INTEGER', 'INT', 'BIGINT', 'DECIMAL', 'NUMERIC',
                         'PACKED', 'ZONED', 'DECFLOAT', 'REAL', 'FLOAT', 'DOUBLE', 'DOUBLE PRECISION']
        return column_type.upper() in numeric_types

    def get_create_table_sql(self, settings):
        pass

    def migrate_table(self, migrate_target_connection, settings):
        return {'finished': True, 'rows_migrated': 0, 'source_table_rows_limited': 0, 'target_table_rows': 0, 'chunk_number': 1, 'total_chunks': 1}

    def fetch_indexes(self, settings) -> dict:
        table_schema = settings.get('source_table_schema')
        table_name = settings.get('source_table_name')
        indexes = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            # Primary key columns are not stored as a separate index in the DDL protocol tables,
            # they are reconstructed here from the PK indicator of the columns
            pk_query = f"""SELECT source_column_name
                           FROM "{self.protocol_schema}"."ddl_columns"
                           WHERE trim(source_schema_name) = trim(%s) AND trim(source_table_name) = trim(%s)
                             AND source_pk_indicator = TRUE ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(pk_query, (table_schema, table_name))
            pk_cols = [row[0] for row in cursor.fetchall()]

            order_num = 1
            pk_cols_set = set()
            if pk_cols:
                indexes[order_num] = {
                    'index_name': f"{table_name}_PK",
                    'index_type': 'PRIMARY KEY',
                    'index_owner': table_schema,
                    'index_columns': ', '.join(pk_cols),
                    'index_comment': None,
                    'index_sql': None,
                    'is_function_based': 'NO'
                }
                order_num += 1
                pk_cols_set = set(c.strip().upper() for c in pk_cols)

            query = f"""SELECT source_index_name, source_is_unique, source_columns_list, source_index_comment
                        FROM "{self.protocol_schema}"."ddl_indexes"
                        WHERE trim(source_schema_name) = trim(%s) AND trim(source_table_name) = trim(%s) ORDER BY id"""
            cursor.execute(query, (table_schema, table_name))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: fetch_indexes: ({table_schema}.{table_name}): {rows}")
            for row in rows:
                idx_name = row[0]
                is_unique = row[1]
                cols = row[2]
                comment = row[3]

                # Skip the unique index backing the primary key - it is already covered above
                idx_cols_set = set(c.strip().upper() for c in cols.split(',')) if cols else set()
                if pk_cols_set and is_unique and pk_cols_set == idx_cols_set:
                    self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: fetch_indexes: Skipping index {idx_name} as it matches primary key columns.")
                    continue

                indexes[order_num] = {
                    'index_name': idx_name,
                    'index_type': 'UNIQUE' if is_unique else 'INDEX',
                    'index_owner': table_schema,
                    'index_columns': cols,
                    'index_comment': comment,
                    'index_sql': None,
                    'is_function_based': 'NO'
                }
                order_num += 1
            cursor.close()
        return indexes

    def get_create_index_sql(self, settings):
        pass

    def fetch_constraints(self, settings) -> dict:
        table_schema = settings.get('source_table_schema')
        table_name = settings.get('source_table_name')
        constraints = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT source_fk_name, source_columns_list, source_ref_schema_name, source_ref_table_name, source_ref_columns_list, source_fk_comment
                        FROM "{self.protocol_schema}"."ddl_foreign_keys"
                        WHERE trim(source_schema_name) = trim(%s) AND trim(source_table_name) = trim(%s) ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (table_schema, table_name))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: fetch_constraints: ({table_schema}.{table_name}): {rows}")
            for i, row in enumerate(rows, 1):
                constraints[i] = {
                    'constraint_name': row[0],
                    'constraint_type': 'FOREIGN KEY',
                    'constraint_owner': table_schema,
                    'constraint_columns': self.deduplicate_columns_list(row[1]),
                    'referenced_table_schema': row[2],
                    'referenced_table_name': row[3],
                    'referenced_columns': self.deduplicate_columns_list(row[4]),
                    'constraint_sql': None,
                    'delete_rule': 'NO ACTION',
                    'update_rule': 'NO ACTION',
                    'constraint_comment': row[5],
                    'constraint_status': 'ENABLED'
                }
            cursor.close()
        return constraints

    def deduplicate_columns_list(self, columns_list: str):
        """Normalizes a comma separated columns list and removes duplicates, preserving order."""
        if not columns_list:
            return columns_list
        seen = set()
        deduped = []
        for col in columns_list.split(','):
            col = col.strip()
            if col and col not in seen:
                seen.add(col)
                deduped.append(col)
        return ', '.join(deduped)

    def get_create_constraint_sql(self, settings):
        pass

    def fetch_triggers(self, table_id: int, table_schema: str, table_name: str) -> dict:
        triggers = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT id, source_trigger_name, source_ddl_text, source_trigger_comment
                        FROM "{self.protocol_schema}"."ddl_triggers"
                        WHERE trim(source_schema_name) = trim(%s) ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (table_schema,))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: fetch_triggers: ({table_schema}): {rows}")
            order_num = 1
            for row in rows:
                # triggers are stored per schema - keep only those referencing the requested table
                if table_name and row[2] and table_name.upper() not in row[2].upper():
                    continue
                triggers[order_num] = {
                    'id': row[0],
                    'name': row[1],
                    'event': 'UNKNOWN',
                    'new': None,
                    'old': None,
                    'sql': row[2],
                    'comment': row[3]
                }
                order_num += 1
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

    def fetch_funcproc_names(self, schema: str):
        return {}

    def fetch_funcproc_code(self, funcproc_id: int):
        return ""

    def fetch_sequences(self, schema_name: str) -> dict:
        sequences = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            ## sequences attached to a table are migrated as identity columns, not as standalone sequences
            query = f"""SELECT id, source_seq_name, source_table_name, source_column_name, source_start_value, source_increment_by, source_minvalue, source_maxvalue, source_cache, source_is_cycled, source_ddl_text
                        FROM "{self.protocol_schema}"."ddl_sequences"
                        WHERE trim(source_schema_name) = trim(%s)
                          AND source_table_name IS NULL AND source_column_name IS NULL
                        ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (schema_name,))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: fetch_sequences: ({schema_name}): {rows}")
            for i, row in enumerate(rows, 1):
                sequences[i] = {
                    'id': row[0],
                    'sequence_name': row[1],
                    'table_name': row[2],
                    'column_name': row[3],
                    'source_start_value': row[4],
                    'source_increment_by': row[5],
                    'source_minvalue': row[6],
                    'source_maxvalue': row[7],
                    'source_cache': row[8],
                    'source_is_cycled': row[9],
                    'source_sequence_sql': row[10]
                }
            cursor.close()
        return sequences

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

                self.config_parser.print_log_message('INFO', f"ibm_db2_i_connector: migrate_sequences: Creating sequence {target_sequence_name} ...")
                target_connector.execute_query(target_sequence_sql)
                return True
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"ibm_db2_i_connector: migrate_sequences: Error creating sequence {target_sequence_name}: {e}")
                return False

        return True

    def fetch_views_names(self, source_schema_name: str) -> dict:
        views = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT id, source_schema_name, source_view_name, source_view_comment, source_view_type
                        FROM "{self.protocol_schema}"."ddl_views"
                        WHERE trim(source_schema_name) = trim(%s) ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (source_schema_name,))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: fetch_views_names: ({source_schema_name}): {rows}")
            for i, row in enumerate(rows, 1):
                # target_schema_name / target_view_name are deliberately not set here -
                # the planner then derives the target name from the source view name
                views[i] = {
                    'id': row[0],
                    'schema_name': row[1],
                    'view_name': row[2],
                    'comment': row[3],
                    'view_type': row[4] or 'VIEW',
                    'is_alias': False
                }

            # Aliases pointing to a TABLE are exposed as views "SELECT * FROM <target table>",
            # aliases pointing to a VIEW are skipped - the view itself is migrated anyway.
            query_aliases = f"""SELECT a.id, a.source_schema_name, a.source_alias_name, a.source_target_schema, a.source_target_name, a.source_alias_comment
                                FROM "{self.protocol_schema}"."ddl_aliases" a
                                INNER JOIN "{self.protocol_schema}"."ddl_tables" t
                                    ON trim(a.source_target_schema) = trim(t.source_schema_name)
                                    AND trim(a.source_target_name) = trim(t.source_table_name)
                                WHERE trim(a.source_schema_name) = trim(%s) ORDER BY a.id"""
            cursor.execute(query_aliases, (source_schema_name,))
            alias_rows = cursor.fetchall()
            cursor.close()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: fetch_views_names (aliases): ({source_schema_name}): {alias_rows}")

            offset = len(views)
            for j, row in enumerate(alias_rows, 1):
                self.config_parser.print_log_message('INFO', f"ibm_db2_i_connector: fetch_views_names: Database Alias '{row[2]}' points to a TABLE. Creating SQL VIEW to expose target table pointers.")
                views[offset + j] = {
                    'id': row[0] + 1000000,  # shifted to avoid collision with actual view IDs
                    'schema_name': row[1],
                    'view_name': row[2],
                    'aliased_schema_name': row[3],
                    'aliased_table_name': row[4],
                    'comment': row[5],
                    'view_type': 'VIEW',
                    'is_alias': True
                }
        return views

    def get_aliases(self, settings) -> dict:
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
                            ON trim(a.source_target_schema) = trim(t.source_schema_name) AND trim(a.source_target_name) = trim(t.source_table_name)
                        LEFT JOIN "{self.protocol_schema}"."ddl_views" v
                            ON trim(a.source_target_schema) = trim(v.source_schema_name) AND trim(a.source_target_name) = trim(v.source_view_name)
                        WHERE trim(a.source_schema_name) = trim(%s) ORDER BY a.id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (source_schema_name,))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"ibm_db2_i_connector: get_aliases: ({source_schema_name}): {rows}")
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
                # source object names are used here on purpose - schema, view and table names
                # are translated to their target counterparts later in convert_view_code
                return f'CREATE VIEW "{source_schema_name}"."{source_view_name}" AS SELECT * FROM "{alias_row[0]}"."{alias_row[1]}"'
        return ""

    def strip_db2i_specific_clauses(self, code: str) -> str:
        """
        Removes DB2 for i specific clauses which have no PostgreSQL counterpart and which
        the SQL parser does not understand:
          - FOR SYSTEM NAME <system-name>  - the short system name of the object
          - RCDFMT <format-name>           - the record format name, always the last clause
          - CCSID <n>                      - the coded character set of a string column
        """
        if not code:
            return code

        # FOR SYSTEM NAME <system-name> - system names may contain $, # and @
        code = re.sub(r"(?i)\s*\bFOR\s+SYSTEM\s+NAME\s+\"?[A-Za-z0-9_$#@]+\"?", "", code)

        # RCDFMT <format-name> - closes the statement, optionally followed by
        # WITH [CASCADED|LOCAL] CHECK OPTION and/or the statement terminator
        code = re.sub(
            r"(?i)\s*\b(?:RCDFMT|RECORD\s+FORMAT)\s+\"?[A-Za-z0-9_$#@]+\"?"
            r"(?=(?:\s*WITH\s+(?:CASCADED\s+|LOCAL\s+)?CHECK\s+OPTION)?\s*;?\s*$)",
            "",
            code,
        )

        # CCSID <n> of individual columns
        code = re.sub(r"(?i)\s*\bCCSID\s+\d+\b", "", code)

        return code.strip()

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

    def convert_db2i_operators(self, code: str) -> str:
        """
        Converts DB2 for i specific operators which the SQL parser does not understand.
        DB2 for i allows CONCAT to be used as an infix operator ("A CONCAT B"), which is
        equivalent to the standard "A || B". The function form "CONCAT(A, B)" is left as it is.
        The correlated table function "TABLE (SELECT ...)" becomes "LATERAL (SELECT ...)".
        """
        code = self.replace_outside_string_literals(code, r"(?i)\bCONCAT\b(?!\s*\()", "||")
        code = self.replace_outside_string_literals(code, r"(?i)\bTABLE\s*\(\s*(SELECT\b|WITH\b)", r"LATERAL (\1")
        return code

    def convert_mqt_to_materialized_view(self, code: str) -> str:
        """
        A DB2 for i materialized query table (MQT) is declared as
        "CREATE TABLE <name> AS (<query>) DATA INITIALLY DEFERRED REFRESH DEFERRED ..."
        and is migrated as a PostgreSQL materialized view. The MQT specific clauses have no
        PostgreSQL counterpart and are removed. Code of other objects is returned unchanged.
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
        code = re.sub(r"(?i)\bCREATE\s+(?:OR\s+REPLACE\s+)?TABLE\b", "CREATE MATERIALIZED VIEW", code, count=1)
        return code

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

        def align_union_types(union_node):
            """
            PostgreSQL requires the column types of the non-recursive and the recursive term
            of a recursive CTE to be identical, DB2 for i resolves them on its own. Where one
            arm casts a column explicitly, the same cast is applied to the other arm.
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
            DB2 for i derives the recursion from the CTE referencing itself, PostgreSQL requires
            the RECURSIVE keyword to be stated explicitly - without it the self-reference fails
            with 'relation ... does not exist'.
            """
            if isinstance(node, sqlglot.exp.With):
                is_recursive = False
                for cte in node.expressions:
                    if isinstance(cte, sqlglot.exp.CTE):
                        cte_name = cte.alias_or_name.upper()
                        # the CTE is recursive if it references itself in its own query definition
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
        converted_code = self.strip_db2i_specific_clauses(converted_code)

        # WITH [CASCADED|LOCAL] CHECK OPTION is valid in PostgreSQL as well, but the SQL parser
        # does not understand it - it is cut off here and appended back to the converted code.
        check_option = ''
        check_option_match = re.search(r"(?i)\s*\bWITH\s+(CASCADED\s+|LOCAL\s+)?CHECK\s+OPTION\s*;?\s*$", converted_code)
        if check_option_match:
            scope = check_option_match.group(1).strip().upper() + ' ' if check_option_match.group(1) else ''
            check_option = f" WITH {scope}CHECK OPTION"
            converted_code = converted_code[:check_option_match.start()].rstrip()

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

            converted_code = self.convert_db2i_operators(converted_code)

            try:
                # The 'db2' dialect is not supported by sqlglot, the code is read as 'postgres':
                # DB2 for i sorts NULL values as the largest ones, exactly like PostgreSQL, while
                # the default sqlglot dialect assumes the opposite and would compensate for it by
                # adding explicit NULLS FIRST / NULLS LAST which inverts the original ordering.
                parsed_code = sqlglot.parse_one(converted_code, read="postgres")
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"ibm_db2_i_connector: convert_view_code: Error parsing View code: {e}")
                return converted_code + check_option

            # sqlglot does not raise on unknown syntax - it silently falls back to a plain
            # Command node. All transformations below would then be no-ops and the untranslated
            # DB2 for i code would be handed over to the target database, so this is reported here.
            if isinstance(parsed_code, sqlglot.exp.Command):
                self.config_parser.print_log_message('ERROR', f"ibm_db2_i_connector: convert_view_code: View code contains syntax unsupported by the SQL parser, it is left unconverted: {converted_code}")
                return converted_code + check_option

            for cte_node in parsed_code.find_all(sqlglot.exp.CTE):
                if cte_node.alias_or_name:
                    cte_names.add(cte_node.alias_or_name.upper())

            parsed_code = parsed_code.transform(quote_column_names)
            parsed_code = parsed_code.transform(quote_schema_and_table_names)
            parsed_code = parsed_code.transform(replace_functions)
            parsed_code = parsed_code.transform(convert_recursive_with)

            converted_code = parsed_code.sql(dialect="postgres")
            converted_code = converted_code.replace("()()", "()")
            return converted_code + check_option
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
        return 0

    def get_table_size(self, table_schema: str, table_name: str):
        return 0

    def get_table_next_identity(self, table_schema: str, table_name: str):
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
        return "IBM DB2 for i (DDL/CSV)"

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
        return None

    def get_random_pks(self, schema_name: str, table_name: str, pk_columns: list, sample_size: int):
        return []

    def get_row_checksums(self, schema_name: str, table_name: str, pk_columns: list, pk_values_list: list, columns: list):
        return {}

    def get_lob_sizes(self, schema_name: str, table_name: str, pk_columns: list, pk_values_list: list, lob_columns: list):
        return {}

if __name__ == "__main__":
    print("This script is not meant to be run directly")
