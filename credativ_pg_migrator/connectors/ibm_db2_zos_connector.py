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
import psycopg2
import time
import datetime
import os
import glob
import re

class IbmDb2ZosConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        if source_or_target != 'source':
            raise ValueError("IBM DB2 z/OS is only supported as a source database")

        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.config_parser.print_log_message('DEBUG3', f"IbmDb2ZosConnector: Starting INIT")
        self.connectivity = self.config_parser.get_connectivity(self.source_or_target)
        self.on_error_action = self.config_parser.get_on_error_action()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger
        self.source_db_config = self.config_parser.get_source_config()

        if self.connectivity == self.config_parser.const_connectivity_ddl():
            self.ddl_directory = self.source_db_config['ddl']['directory']
            self.config_parser.print_log_message('DEBUG3', f"Source_db_config: {self.source_db_config} - ddl_directory: {self.ddl_directory}")
            if not os.path.exists(self.ddl_directory):
                raise ValueError(f"DDL directory not found: '{self.ddl_directory}'")
            else:
                if not os.listdir(self.ddl_directory):
                    raise ValueError(f"DDL directory is empty: '{self.ddl_directory}'")
                else:
                    self.config_parser.print_log_message('INFO', f"DDL directory found: '{self.ddl_directory}'")

                if not os.listdir(self.ddl_directory):
                    raise ValueError(f"DDL directory is empty: '{self.ddl_directory}'")
                else:

                    extension_counts = {}
                    for filename in os.listdir(self.ddl_directory):
                        if os.path.isfile(os.path.join(self.ddl_directory, filename)):
                            ext = os.path.splitext(filename)[1]
                            extension_counts[ext] = extension_counts.get(ext, 0) + 1
                    for ext, count in extension_counts.items():
                        self.config_parser.print_log_message('INFO', f"Found {count} files with extension '{ext}'")

                    self.config_parser.print_log_message('INFO', f"DDL directory found: {self.ddl_directory}")
        else:
            raise ValueError(f"Unsupported IBM DB2 z/OS connectivity: {self.connectivity}")

        self.config_parser.print_log_message('DEBUG3', f"IbmDb2ZosConnector: INIT done")

    def connect(self):
        self.config_parser.print_log_message('DEBUG', "IbmDb2ZosConnector: connect() called.")
        from credativ_pg_migrator.migrator_tables import MigratorTables
        self.migrator_tables = MigratorTables(self.logger, self.config_parser)
        self.protocol_schema = self.migrator_tables.protocol_schema

    def disconnect(self):
        self.config_parser.print_log_message('DEBUG', "IbmDb2ZosConnector: disconnect() called.")
        if hasattr(self, 'migrator_tables') and self.migrator_tables and self.migrator_tables.protocol_connection:
            self.migrator_tables.protocol_connection.connection.close()

    def fetch_all_tables(self, schema_name: str) -> dict:
        tables = {}
        self.config_parser.print_log_message('DEBUG3', f"fetch_all_tables ({schema_name}): starting - self.connectivity: {self.connectivity}")
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT source_schema_name, source_table_name, source_partition_columns, source_partition_ranges
                        FROM "{self.protocol_schema}"."ddl_tables"
                        WHERE source_schema_name = %s ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (schema_name,))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"fetch_all_tables ({schema_name}): {rows}")
            for i, row in enumerate(rows, 1):
                tables[i] = {
                    'id': i,
                    'schema_name': row[0],
                    'table_name': row[1],
                    'comment': f"Partition: {row[2]}, Ranges: {row[3]}" if row[2] else None
                }
            cursor.close()
        return tables

    def fetch_table_columns(self, settings) -> dict:
        self.config_parser.print_log_message('DEBUG', "IbmDb2ZosConnector: fetch_table_columns() called.")
        table_schema = settings.get('table_schema')
        table_name = settings.get('table_name')
        columns = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT source_column_name, source_data_type, source_is_nullable, source_default_value, source_pk_indicator
                        FROM "{self.protocol_schema}"."ddl_columns"
                        WHERE source_schema_name = %s AND source_table_name = %s ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (table_schema, table_name))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"fetch_table_columns ({table_schema}.{table_name}): {rows}")
            for i, row in enumerate(rows, 1):
                col_name = row[0]
                col_type = row[1]
                is_nullable = 'YES' if row[2] else 'NO'
                default_val = row[3]
                is_pk = row[4]

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
                    'is_identity': 'NO',
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
        if not hasattr(self, 'types_mapping'):
            self.types_mapping = {
                'postgresql': {
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
            }
        return self.types_mapping


    def parse_ddl_files(self, settings):
        self.config_parser.print_log_message('DEBUG3', f"IbmDb2ZosConnector: Starting DDL parser")
        migrator_tables = settings['migrator_tables']
        if not migrator_tables:
            self.config_parser.print_log_message('ERROR', "parse_ddl_files: migrator_tables not found in settings.")
            return

        for filepath in glob.glob(os.path.join(self.ddl_directory, '*.*')):
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
                    'source_ddl_text': ddl_text
                })

            # Remove the extracted triggers from content so they aren't parsed again
            content = trigger_pattern.sub("", content)

            statements = content.split(';')
            for stmt in statements:
                stmt = stmt.strip()
                if not stmt:
                    continue

                # Parse Indexes
                match_index = re.search(r"^CREATE\s+(UNIQUE\s+)?INDEX\s+([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\s+ON\s+([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)", stmt, re.IGNORECASE)
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
                                'source_columns_list': ', '.join(cols_list)
                            })
                    continue

                # Parse Sequences
                match_seq = re.search(r"^CREATE\s+SEQUENCE\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?", stmt, re.IGNORECASE)
                if match_seq:
                    schema_name = match_seq.group(1).upper()
                    seq_name = match_seq.group(2).upper()
                    migrator_tables.insert_ddl_sequences({
                        'source_schema_name': schema_name,
                        'source_seq_name': seq_name,
                        'source_ddl_text': stmt
                    })
                    continue

                # Parse Views
                match_view = re.search(r"^CREATE\s+VIEW\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?", stmt, re.IGNORECASE)
                if match_view:
                    schema_name = match_view.group(1).upper()
                    view_name = match_view.group(2).upper()
                    migrator_tables.insert_ddl_views({
                        'source_schema_name': schema_name,
                        'source_view_name': view_name,
                        'source_ddl_text': stmt
                    })
                    continue

                # Parse Aliases
                match_alias = re.search(r"^CREATE\s+ALIAS\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?\s+FOR\s+\"?([A-Za-z0-9_]+)\"?\.\"?([A-Za-z0-9_]+)\"?", stmt, re.IGNORECASE)
                if match_alias:
                    schema_name = match_alias.group(1).upper()
                    alias_name = match_alias.group(2).upper()
                    target_schema = match_alias.group(3).upper()
                    target_name = match_alias.group(4).upper()
                    migrator_tables.insert_ddl_aliases({
                        'source_schema_name': schema_name,
                        'source_alias_name': alias_name,
                        'source_target_schema': target_schema,
                        'source_target_name': target_name
                    })
                    continue

                # Parse Foreign Keys
                match_fk = re.search(r"^ALTER\s+TABLE\s+([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\s+ADD\s+CONSTRAINT\s+([A-Za-z0-9_]+)\s+FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\s*\(([^)]+)\)", stmt, re.IGNORECASE)
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
                        'source_ref_columns_list': ', '.join(ref_cols_list)
                    })
                    continue

                # Find CREATE TABLE
                match_table = re.search(r"^CREATE\s+TABLE\s+([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)", stmt, re.IGNORECASE)
                if not match_table:
                    continue

                schema_name = match_table.group(1).upper()
                table_name = match_table.group(2).upper()

                # Extract block inside parenthesis
                start_idx = stmt.find('(', match_table.end())
                if start_idx == -1:
                    continue

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

                if end_idx == -1:
                    continue

                columns_str = stmt[start_idx+1:end_idx]

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
                trailing_str = stmt[end_idx+1:]
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
                    'source_partition_ranges': partition_ranges
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

                    default_value = None
                    default_match = re.search(r"WITH\s+DEFAULT\s+('[^']*'|[0-9\.]+|[A-Za-z0-9_]+)?", after_type, re.IGNORECASE)
                    if default_match:
                        if default_match.group(1):
                            default_value = default_match.group(1)
                        else:
                            default_value = "SYSTEM DEFAULT"

                    is_pk = col_name in pk_columns

                    migrator_tables.insert_ddl_columns({
                        'source_schema_name': schema_name,
                        'source_table_name': table_name,
                        'source_column_name': col_name,
                        'source_data_type': data_type,
                        'source_is_nullable': is_nullable,
                        'source_default_value': default_value,
                        'source_pk_indicator': is_pk
                    })

        self.config_parser.print_log_message('INFO', "DDL parsing completed and unified protocol tables populated with DB2 source metadata.")


    def get_sql_functions_mapping(self, settings):
        return {}

    def fetch_table_names(self, table_schema: str):
        return self.fetch_all_tables(table_schema)

    def get_table_description(self, settings) -> dict:
        return {}

    def fetch_default_values(self, settings) -> dict:
        return {}

    def is_string_type(self, column_type: str) -> bool:
        return False

    def is_numeric_type(self, column_type: str) -> bool:
        return False

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
            self.config_parser.print_log_message('DEBUG3', f"fetch_indexes ({table_schema}.{table_name}): {rows}")
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
            self.config_parser.print_log_message('DEBUG3', f"fetch_constraints ({table_schema}.{table_name}): {rows}")
            for i, row in enumerate(rows, 1):
                constraints[i] = {
                    'constraint_name': row[0],
                    'constraint_type': 'FOREIGN KEY',
                    'constraint_owner': table_schema,
                    'constraint_columns': row[1],
                    'referenced_table_schema': row[2],
                    'referenced_table_name': row[3],
                    'referenced_columns': row[4],
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
            self.config_parser.print_log_message('DEBUG3', f"fetch_triggers ({table_schema}): {rows}")
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

    def convert_trigger(self, trig: str, settings: dict):
        pass

    def fetch_funcproc_names(self, schema: str):
        return {}

    def fetch_funcproc_code(self, funcproc_id: int):
        return ""

    def convert_funcproc_code(self, settings):
        pass

    def fetch_sequences(self, table_schema: str, table_name: str):
        seqs = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT id, source_seq_name, source_ddl_text
                        FROM "{self.protocol_schema}"."ddl_sequences"
                        WHERE source_schema_name = %s ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (table_schema,))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"fetch_sequences ({table_schema}): {rows}")
            for i, row in enumerate(rows, 1):
                seqs[i] = {
                    'id': row[0],
                    'name': row[1],
                    'column_name': None,
                    'set_sequence_sql': row[2]
                }
            cursor.close()
        return seqs

    def get_sequence_details(self, sequence_owner, sequence_name):
        return {}

    def fetch_views_names(self, source_schema_name: str):
        views = {}
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT id, source_schema_name, source_view_name
                        FROM "{self.protocol_schema}"."ddl_views"
                        WHERE source_schema_name = %s ORDER BY id"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (source_schema_name,))
            rows = cursor.fetchall()
            self.config_parser.print_log_message('DEBUG3', f"fetch_views_names ({source_schema_name}): {rows}")
            for i, row in enumerate(rows, 1):
                views[i] = {
                    'id': row[0],
                    'schema_name': row[1],
                    'view_name': row[2],
                    'comment': None
                }
            cursor.close()
        return views

    def fetch_view_code(self, settings):
        source_schema_name = settings.get('source_schema_name')
        source_view_name = settings.get('source_view_name')
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            query = f"""SELECT source_ddl_text
                        FROM "{self.protocol_schema}"."ddl_views"
                        WHERE source_schema_name = %s AND source_view_name = %s"""
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(query, (source_schema_name, source_view_name))
            row = cursor.fetchone()
            self.config_parser.print_log_message('DEBUG3', f"fetch_view_code ({source_schema_name}.{source_view_name}): {row}")
            cursor.close()
            if row:
                return row[0]
        return ""

    def convert_view_code(self, view_code: str, settings: dict):
        pass

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
