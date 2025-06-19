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
from credativ_pg_migrator.database_connector import DatabaseConnector
from credativ_pg_migrator.migrator_logging import MigratorLogger
import re
import traceback
import pyodbc
# import polars as pl

class InformixConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        if source_or_target != 'source':
            raise ValueError(f"Informix is supported only as source database")

        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.on_error_action = self.config_parser.get_on_error_action()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger

    def connect(self):
        if self.config_parser.get_connectivity(self.source_or_target) == 'odbc':
            connection_string = self.config_parser.get_connect_string(self.source_or_target)
            username = self.config_parser.get_db_config(self.source_or_target)['username']
            password = self.config_parser.get_db_config(self.source_or_target)['password']
            self.connection = pyodbc.connect(
                f"DRIVER={{IBM INFORMIX ODBC DRIVER}};SERVER={connection_string};UID={username};PWD={password}"
            )
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
            raise ValueError(f"Unsupported connectivity: {self.config_parser.get_connectivity(self.source_or_target)}")

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
                'year(': 'extract(year from ',
                'month(': 'extract(month from ',
                'day(': 'extract(day from ',
            }
        else:
            self.config_parser.print_log_message('ERROR', f"Unsupported target database type: {target_db_type}")

    def fetch_table_names(self, table_schema: str):
        query = f"""
            SELECT tabid, tabname
            FROM systables
            WHERE owner = '{table_schema}' AND tabtype = 'T'
            ORDER BY tabname
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
        query = f"""
            SELECT
                c.colno,
                c.colname,
                case
                    WHEN c.extended_id = 0 THEN
                        CASE (CASE WHEN c.coltype >= 256 THEN c.coltype - 256 ELSE c.coltype END)
                            WHEN 0 THEN 'CHAR'
                            WHEN 1 THEN 'SMALLINT'
                            WHEN 2 THEN 'INTEGER'
                            WHEN 3 THEN 'FLOAT'
                            WHEN 4 THEN 'SMALLFLOAT'
                            WHEN 5 THEN 'DECIMAL'
                            WHEN 6 THEN 'SERIAL'
                            WHEN 7 THEN 'DATE'
                            WHEN 8 THEN 'MONEY'
                            WHEN 9 THEN 'NULL'
                            WHEN 10 THEN 'DATETIME'
                            WHEN 11 THEN 'BYTE'
                            WHEN 12 THEN 'TEXT'
                            WHEN 13 THEN 'VARCHAR'
                            WHEN 14 THEN 'INTERVAL'
                            WHEN 15 THEN 'NCHAR'
                            WHEN 16 THEN 'NVARCHAR'
                            WHEN 17 THEN 'INT8'
                            WHEN 18 THEN 'SERIAL8'
                            WHEN 19 THEN 'SET'
                            WHEN 20 THEN 'MULTISET'
                            WHEN 21 THEN 'LIST'
                            WHEN 22 THEN 'ROW'
                            WHEN 23 THEN 'COLLECTION'
                            WHEN 24 THEN 'ROWREF'
                            WHEN 25 THEN 'LVARCHAR'
                            WHEN 26 THEN 'BOOLEAN'

                            when 53 THEN 'BIGSERIAL'
                            ELSE 'UNKNOWN-'||cast(c.coltype as varchar(10))
                        END
                    ELSE
                    	CASE WHEN x.name IS NOT NULL THEN upper(x.name)
                    	ELSE 'UNKNOWN-'||cast(c.coltype as varchar(10))||'-'||cast(x.extended_id as varchar(10)) END
                END AS coltype,
                c.collength,
                CASE WHEN c.coltype >= 256 THEN 'NO' ELSE 'YES' END AS nullable,
                CASE WHEN d.type = 'L' THEN
                    CASE
                        WHEN c.coltype IN (0, 13, 15, 16, 40, 41, 45) THEN d.default
                        ELSE SUBSTR(d.default, INSTR(d.default, ' ') + 1)
                    END
                ELSE NULL
                END AS default_value,
                ifx_bit_rightshift(c.collength, 8) as numeric_precision,
                bitand(c.collength, "0xff") as numeric_scale
            FROM syscolumns c LEFT join sysxtdtypes x ON c.extended_id = x.extended_id
            LEFT JOIN sysdefaults d ON c.tabid = d.tabid AND c.colno = d.colno and d.class = 'T'
            WHERE c.tabid = (SELECT t.tabid
                            FROM systables t
                            WHERE t.tabname = '{table_name}'
                            AND t.owner = '{table_schema}')
            ORDER BY colno
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            self.config_parser.print_log_message('DEBUG', f"Informix: Reading columns for {table_schema}.{table_name}")
            cursor.execute(query)
            for row in cursor.fetchall():
                column_number = row[0]
                column_name = row[1]
                data_type = row[2].strip().upper()
                maximum_length = row[3]
                is_nullable = row[4].strip().upper()
                column_default_value = row[5]
                numeric_precision = row[6]
                numeric_scale = row[7]

                column_type = data_type
                if self.is_string_type(data_type):
                    column_type = f"{data_type}({maximum_length})"
                elif self.is_numeric_type(data_type):
                    column_type = f"{data_type}({maximum_length},{numeric_scale})"
                result[column_number] = {
                    'column_name': column_name,
                    'data_type': data_type,
                    'column_type': '',
                    'character_maximum_length': maximum_length if self.is_string_type(data_type) else None,
                    'numeric_precision': numeric_precision if self.is_numeric_type(data_type) else None,
                    'numeric_scale': numeric_scale if self.is_numeric_type(data_type) and numeric_scale < 255 else None,
                    'is_nullable': is_nullable,
                    'is_identity': 'YES' if data_type in ('SERIAL', 'SERIAL8', 'BIGSERIAL') else 'NO',
                    'column_default_value': column_default_value,
                    'column_comment': ''
                }

            cursor.close()
            self.disconnect()
            return result
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_views_names(self, source_schema: str):
        views = {}
        order_num = 1
        query = f"""
            SELECT DISTINCT v.tabid, t.tabname
            FROM sysviews v
            JOIN systables t on v.tabid = t.tabid
            WHERE t.owner = '{source_schema}'
            ORDER BY t.tabname
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
        SELECT v.viewtext
        FROM sysviews v
        WHERE v.tabid = {view_id}
        ORDER BY v.seqno
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
        view_code = settings['view_code']
        converted_view_code = view_code
        converted_view_code = converted_view_code.replace(f'''"{settings['source_schema']}".''', f'''"{settings['target_schema']}".''')
        return converted_view_code

    def get_types_mapping(self, settings):
        target_db_type = settings['target_db_type']
        types_mapping = {}
        if target_db_type == 'postgresql':
            types_mapping = {
                'BLOB': 'BYTEA',
                'BOOLEAN': 'BOOLEAN',
                'BYTE': 'BYTEA',
                'CHAR': 'CHAR',
                'CLOB': 'TEXT',
                'DECIMAL': 'DECIMAL',
                'DATE': 'DATE',
                'DATETIME': 'TIMESTAMP',
                'FLOAT': 'FLOAT',
                'INTEGER': 'INTEGER',
                'INTERVAL': 'INTERVAL',
                'INT8': 'BIGINT',
                'LVARCHAR': 'VARCHAR',
                'MONEY': 'MONEY',
                'NCHAR': 'CHAR',
                'NVARCHAR': 'VARCHAR',
                # 'SERIAL8': 'BIGSERIAL',
                # 'SERIAL': 'SERIAL',
                # SERIAL & SERIAL8 are replaced in PostgreSQL with IDENTITY columns
                'SERIAL8': 'BIGINT',
                'SERIAL': 'INTEGER',
                'SMALLFLOAT': 'REAL',
                'SMALLINT': 'SMALLINT',
                'TEXT': 'TEXT',
                'TIME': 'TIME',
                'TIMESTAMP': 'TIMESTAMP',
                'VARCHAR': 'VARCHAR',
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
                coalesce(c.constrname, i.idxname) as index_name,
                coalesce(c.constrtype, i.idxtype) as index_type,
                i.clustered,
                i.owner,
                cast(i2.indexkeys  AS lvarchar) as index_keys,
                part1, part2, part3, part4, part5, part6, part7, part8, part9, part10, part11, part12, part13, part14, part15, part16
            FROM sysindexes i
            LEFT JOIN sysconstraints c
            ON i.tabid = c.tabid and i.idxname = c.idxname
            LEFT JOIN sysindices i2
            ON i.tabid = i2.tabid and i.idxname = i2.idxname
            WHERE i.tabid = '{source_table_id}'
            ORDER BY index_name
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)

            indexes = cursor.fetchall()

            for index in indexes:
                self.config_parser.print_log_message('DEBUG', f"Processing index: {index}")
                procedure_id = 0
                procedure_colnos = []
                procedure_owner = ''
                procedure_name = ''
                procedure_columns = ''
                function_based_index = False

                index_name = index[0].strip()
                index_type = index[1].strip()
                index_owner = index[3].strip()
                index_keys = index[4]
                colnos = [colno for colno in index[5:] if colno]

                # Check if index_keys matches the pattern like '<561>(4) [1]'
                match = re.match(r'<(\d+)>\(([\d,]+)\)', str(index_keys))
                if match:
                    procedure_id = int(match.group(1))
                    procedure_colnos = [int(x) for x in match.group(2).split(',')]
                    self.config_parser.print_log_message('DEBUG', f"Index {index_name}: index_keys: procedure_id={procedure_id}, procedure_colnos={procedure_colnos}")
                # Get column names for each colno

                columns = []
                if colnos:
                    self.config_parser.print_log_message('DEBUG3', f"Index {index_name}: Extracted colnos: {colnos}")
                    for colno in colnos:
                        cursor.execute(f"SELECT colname FROM syscolumns WHERE colno = {colno} AND tabid = {source_table_id}")
                        colname = cursor.fetchone()[0]
                        columns.append(colname)

                if procedure_id > 0:
                    cursor.execute(f"""
                    SELECT owner, procname
                    FROM sysprocedures
                    WHERE procid = {procedure_id}
                    """)
                    procedure_info = cursor.fetchone()
                    if procedure_info:
                        procedure_owner = procedure_info[0].strip()
                        procedure_name = procedure_info[1].strip()
                        self.config_parser.print_log_message('DEBUG', f"Index {index_name}: Function-based index found: {index_name} on procedure {procedure_name}")
                        function_based_index = True

                if procedure_colnos:
                    # Get the column names for the function-based index
                    proc_columns = []
                    for colno in procedure_colnos:
                        cursor.execute(f"SELECT colname FROM syscolumns WHERE colno = {colno} AND tabid = {source_table_id}")
                        colname = cursor.fetchone()[0]
                        proc_columns.append(colname)
                    procedure_columns = ', '.join(proc_columns)
                    self.config_parser.print_log_message('DEBUG', f"Index {index_name}: Function-based index columns: {procedure_columns}")

                index_columns = ', '.join([f'"{col}"' for col in columns])
                self.config_parser.print_log_message('DEBUG', f"Index {index_name}: Columns list: {index_columns}, index type: {index_type}, clustered: {index[2]}")

                table_indexes[order_num] = {
                    'index_name': index_name,
                    'index_type': "PRIMARY KEY" if index_type == 'P' else "UNIQUE" if index_type == 'U' else "INDEX",
                    'index_owner': index_owner,
                    'index_columns': index_columns if not function_based_index else f'''{procedure_owner}.{procedure_name}({procedure_columns})''',
                    'index_keys': index_keys,
                    'index_comment': '',
                    'is_function_based': 'YES' if function_based_index else 'NO',
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

        order_num = 1
        table_constraints = {}

        # index_query = f"""
        # SELECT idxname, idxtype, clustered
        # FROM sysindexes WHERE tabid = {source_table_id}
        # """

        self.connect()
        cursor = self.connection.cursor()

        # self.config_parser.print_log_message('DEBUG', f"Reading constraints for {target_table_name}")
        # cursor.execute(index_query)
        # indexes = cursor.fetchall()

        # for index in indexes:
            # index_name = index[0]
            # Check if the index is a primary key by looking at sysconstraints

        cursor.execute(f"""
        SELECT
            constrtype,
            constrname,
            idxname
        FROM sysconstraints
        WHERE tabid = {source_table_id}
        """)
        constraints = cursor.fetchall()
        for constraint in constraints:
            constraint_type = ''
            constraint_name = ''
            index_name = ''
            constraint_columns = ''
            referenced_table_schema = ''
            referenced_table_name = ''
            referenced_columns = ''
            create_constraint_query = ''

            if constraint[0] in ('C', 'R'):
                constraint_type = constraint[0]
                constraint_name = constraint[1]
                index_name = constraint[2]

                if constraint_type == 'R':
                    self.config_parser.print_log_message('DEBUG', f"Processing table: {source_table_name} ({source_table_id}) - foreign key: {constraint_name}")

                    # Get foreign key details
                    find_fk_query = f"""
                    SELECT
                        trim(t.owner),
                        t.tabname AS table_name,
                        c.constrname AS constraint_name,
                        col.colname,
                        trim(rt.owner),
                        rt.tabname AS referenced_table_name,
                        r.delrule,
                        pc.constrname as primary_key_name,
                        rcol.colname as referenced_column,
                        c.constrid
                    FROM sysconstraints c
                    JOIN systables t ON c.tabid = t.tabid
                    JOIN sysindexes i ON c.idxname = i.idxname
                    JOIN syscolumns col ON t.tabid = col.tabid AND col.colno IN (i.part1, i.part2, i.part3, i.part4, i.part5, i.part6, i.part7, i.part8, i.part9, i.part10, i.part11, i.part12, i.part13, i.part14, i.part15, i.part16)
                    JOIN sysreferences r ON c.constrid = r.constrid
                    JOIN systables rt ON r.ptabid = rt.tabid
                    JOIN sysconstraints pc ON r.primary = pc.constrid
                    JOIN sysindexes pi ON pc.idxname = pi.idxname
                    JOIN syscolumns rcol ON rt.tabid = rcol.tabid AND rcol.colno IN (pi.part1, pi.part2, pi.part3, pi.part4, pi.part5, pi.part6, pi.part7, pi.part8, pi.part9, pi.part10, pi.part11, pi.part12, pi.part13, pi.part14, pi.part15, pi.part16)
                    WHERE c.constrtype = 'R' AND c.tabid = {source_table_id} AND c.constrname = '{constraint_name}'
                    """

                    cursor.execute(find_fk_query)

                    if cursor.rowcount > 1:
                        raise ValueError(f"ERROR: Multiple foreign key details found for table {source_table_name} and index {index_name}/{constraint_name}")

                    fk_details = cursor.fetchone()
                    self.config_parser.print_log_message('DEBUG', f"Source table {source_table_name}: FOREIGN KEY details: {fk_details}")

                    # main_schema = fk_details[0]
                    # main_table_name = fk_details[1]
                    constraint_name = fk_details[2]
                    constraint_columns = fk_details[3]
                    referenced_table_schema = fk_details[4]
                    referenced_table_name = fk_details[5]
                    referenced_columns = fk_details[8]

                elif constraint_type == 'C':
                    find_ck_query = f"""
                        SELECT ck.checktext
                        FROM sysconstraints c
                        JOIN syschecks ck ON c.constrid = ck.constrid
                        WHERE c.tabid = {source_table_id}
                        AND c.constrname = {constraint_name}
                        AND c.constrtype = 'C'
                        AND ck.type in ('T', 's')
                        ORDER BY seqno
                    """
                    cursor.execute(find_ck_query)
                    ck_details = cursor.fetchone()
                    self.config_parser.print_log_message('DEBUG', f"Source table {source_table_name}: CHECK constraint details: {ck_details}")

                    create_constraint_query = ''.join([f"{ck[0].strip()}" for ck in ck_details])

                table_constraints[order_num] = {
                    'constraint_name': constraint_name,
                    'constraint_type': 'FOREIGN KEY' if constraint_type == 'R' else 'CHECK' if constraint_type == 'C' else constraint_type,
                    'constraint_owner': source_table_schema,
                    'constraint_columns': constraint_columns,
                    'referenced_table_schema': referenced_table_schema,
                    'referenced_table_name': referenced_table_name,
                    'referenced_columns': referenced_columns,
                    'constraint_sql': create_constraint_query,
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
                procname,
                procid,
                CASE WHEN isproc = 't' THEN 'Procedure' ELSE 'Function' END AS type
            FROM sysprocedures
            WHERE owner = '{schema}'
            ORDER BY procname
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
                'comment': ''
            }
            order_num += 1
        cursor.close()
        self.disconnect()
        return funcproc_data

    def fetch_funcproc_code(self, funcproc_id: int):
        query = f"""
        SELECT data
        FROM sysprocbody
        WHERE procid = {funcproc_id} AND datakey = 'T'
        ORDER BY seqno
        """
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        procbody = cursor.fetchall()
        cursor.close()
        self.disconnect()
        procbody_str = ''.join([body[0] for body in procbody])
        return procbody_str

    def convert_funcproc_code(self, settings):
        funcproc_code = settings['funcproc_code']
        target_db_type = settings['target_db_type']
        source_schema = settings['source_schema']
        target_schema = settings['target_schema']
        table_list = settings['table_list']
        view_list = settings['view_list']

        function_immutable = ''

        if target_db_type == 'postgresql':
            postgresql_code = funcproc_code

            # Replace empty lines with ";"
            postgresql_code = re.sub(r'^\s*$', ';\n', postgresql_code, flags=re.MULTILINE)
            # Split the code based on "\n
            commands = [command.strip() for command in postgresql_code.split('\n') if command.strip()]
            postgresql_code = ''
            line_number = 0
            # self.config_parser.print_log_message('DEBUG', 'Processing step 1: Splitting code into commands and replacing keywords')

            for command in commands:
                if command.startswith('--'):
                    command = command.replace(command, f"\n/* {command.strip()} */;")
                elif command.startswith('IF'):
                    command = command.replace(command, f";{command.strip()}")

                # Add ";" before specific keywords (case insensitive)
                keywords = ["LET", "END FOREACH", "EXIT FOREACH", "RETURN", "DEFINE", "ON EXCEPTION", "END EXCEPTION",
                            "ELSE", "ELIF", "END IF", "END LOOP", "END WHILE", "END FOR", "END FUNCTION", "END PROCEDURE",
                            "UPDATE", "INSERT", "DELETE FROM"]
                for keyword in keywords:
                    command = re.sub(r'(?i)\b' + re.escape(keyword) + r'\b', ";" + keyword, command, flags=re.IGNORECASE)

                if command.startswith('REFERENCING'):
                    command = f"RETURNS TRIGGER AS $$\n/* {command} */"

                    # Comment out lines starting with FOR followed by a single word within the first 5 lines
                if re.match(r'^\s*FOR\s+\w+\s*$', command, flags=re.IGNORECASE) and line_number <= 5:
                    command = f"/* {command} */"

                # Add ";" after specific keywords (case insensitive)
                keywords = ["ELSE", "END IF", "END LOOP", "END WHILE", "END FOR", "END FUNCTION", "END PROCEDURE", "THEN", "END EXCEPTION",
                            "EXIT FOREACH", "END FOREACH", "CONTINUE FOREACH", "EXIT WHILE", "EXIT FOR", "EXIT LOOP"]
                for keyword in keywords:
                    command = re.sub(r'(?i)\b' + re.escape(keyword) + r'\b', keyword + ";", command, flags=re.IGNORECASE)

                if re.search(r'\bOUTER\b', command, flags=re.IGNORECASE) and 'OUTER JOIN' not in command.upper():
                    command = re.sub(r',\s*\bOUTER\b', ' LEFT OUTER JOIN ', command, flags=re.IGNORECASE)

                command = re.sub(r'\bDATETIME YEAR TO DAY', 'TIMESTAMP', command, flags=re.IGNORECASE)
                command = re.sub(r'\bdatetime year to fraction\(5\)', 'TIMESTAMP', command, flags=re.IGNORECASE)
                command = re.sub(r'\bdatetime year to fraction', 'TIMESTAMP', command, flags=re.IGNORECASE)
                command = re.sub(r'\bDATETIME YEAR TO SECOND', 'TIMESTAMP', command, flags=re.IGNORECASE)

                # Check if the code contains "WITH (NOT VARIANT);"
                if re.search(r"\s*WITH\s*\(\s*NOT\s+VARIANT\s*\)\s*;?\s*", command, flags=re.MULTILINE | re.IGNORECASE | re.DOTALL):
                    function_immutable = "IMMUTABLE"
                    command = re.sub(r"\s*WITH\s*\(\s*NOT\s+VARIANT\s*\)\s*;?\s*", "", command, flags=re.MULTILINE | re.IGNORECASE | re.DOTALL)

                postgresql_code += ' ' + command + ' '
                line_number += 1

            # Split the code based on ";"
            # self.config_parser.print_log_message('DEBUG', 'Processing step 2: Splitting code into commands based on ";", reformating code and removing unnecessary spaces')

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
            postgresql_code = re.sub(r'(FOREACH\s+\w+\s+FOR);', r'\1', postgresql_code, flags=re.MULTILINE | re.IGNORECASE)

            self.config_parser.print_log_message('DEBUG3', f'[1] postgresql_code: {postgresql_code}')

            # Replace CREATE PROCEDURE ... RETURNS TRIGGER AS with CREATE FUNCTION
            # postgresql_code = re.sub(
            #     r'CREATE\s+PROCEDURE\s+(\w+\.\w+)\s*\(.*?\)\s+RETURNS\s+TRIGGER\s+AS',
            #     r'CREATE FUNCTION \1 RETURNS TRIGGER AS',
            #     postgresql_code,
            #     flags=re.MULTILINE | re.IGNORECASE
            # )
            postgresql_code = re.sub(
                r'CREATE\s+PROCEDURE\s+("?\w+"?\."?\w+"?\s*\(\))\s+RETURNS\s+TRIGGER\s+AS\b(.*)',
                r'CREATE FUNCTION \1 RETURNS TRIGGER AS \2',
                postgresql_code,
                flags=re.MULTILINE | re.IGNORECASE
            )

            # Replace CREATE PROCEDURE ... RETURNING with CREATE FUNCTION
            postgresql_code = re.sub(
                r'CREATE\s+PROCEDURE\s+(.*?)\s+RETURNING',
                r'CREATE FUNCTION \1 RETURNING',
                postgresql_code,
                flags=re.MULTILINE | re.IGNORECASE
            )

            # Move RETURNING to a new line if there are multiple words before it
            postgresql_code = re.sub(
                r'(\b\w+\b\s+\b\w+\b.*?\bRETURNING\b)',
                lambda match: re.sub(r'\bRETURNING\b', r'\nRETURNING', match.group(0)),
                postgresql_code,
                flags=re.IGNORECASE
            )

            # Replace source_schema in the function/procedure name with target_schema
            postgresql_code = re.sub(
                rf'CREATE\s+(FUNCTION|PROCEDURE)\s+"{source_schema}"\.',
                rf'CREATE \1 "{target_schema}".',
                postgresql_code,
                flags=re.IGNORECASE
            )

            # Convert DEFINE lines to DECLARE and BEGIN block
            def_lines = re.findall(r'^\s*DEFINE\s+.*$', postgresql_code, flags=re.MULTILINE | re.IGNORECASE)

            if def_lines:
                last_def_line = def_lines[-1].strip()
                # print(f'last_def_line: {last_def_line}')
                postgresql_code = postgresql_code.replace(last_def_line, last_def_line + '\nBEGIN;', 1)

                # Replace lvarchar definitions with text data type
                postgresql_code = re.sub(r'\blvarchar\(\d+\)', 'text', postgresql_code, flags=re.IGNORECASE)
                postgresql_code = re.sub(r'\blvarchar', 'text', postgresql_code, flags=re.IGNORECASE)
                postgresql_code = re.sub(r'\bvarchar\(\d+\)', 'text', postgresql_code, flags=re.IGNORECASE)
                postgresql_code = re.sub(r'\bDATETIME YEAR TO DAY', 'TIMESTAMP', postgresql_code, flags=re.IGNORECASE)
                postgresql_code = re.sub(r'\bDATETIME YEAR TO SECOND', 'TIMESTAMP', postgresql_code, flags=re.IGNORECASE)
                postgresql_code = re.sub(r'\bDATETIME YEAR TO FRACTION\(5\)', 'TIMESTAMP', postgresql_code, flags=re.IGNORECASE)
                postgresql_code = re.sub(r'\bDATETIME YEAR TO FRACTION', 'TIMESTAMP', postgresql_code, flags=re.IGNORECASE)
                # print(f'postgresql_code: {postgresql_code}')

                postgresql_code = re.sub(r'^\s*DEFINE\s+', '\nDECLARE\n', postgresql_code, count=1, flags=re.MULTILINE | re.IGNORECASE)
                postgresql_code = re.sub(r'^\s*DEFINE\s+', '', postgresql_code, flags=re.MULTILINE | re.IGNORECASE)

            # Replace variable declarations with %TYPE where LIKE is used
            # declarations with LIKE can be also in the header
            # postgresql_code = re.sub(r'\s+(\w+)\s+LIKE\s+([\w\d_]+)\.(\w+);', r'\n\1 \2.\3%TYPE;', postgresql_code, flags=re.IGNORECASE)
            # Replace variable declarations with %TYPE where LIKE is used
            postgresql_code = re.sub(r'\s+(\w+)\s+LIKE\s+([\w\d_]+)\.(\w+);', r'\n\1 \2.\3%TYPE;', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r'\(([^)]+)\)', lambda match: re.sub(r'(\w+)\s+LIKE\s+([\w\d_]+)\.(\w+)', r'\1 \2.\3%TYPE', match.group(0)), postgresql_code, flags=re.IGNORECASE)

            # Replace SELECT INTO TEMP with CREATE TEMP TABLE
            postgresql_code = re.sub(
                r'Select\s+([\w\d_,\s]+)\s+from\s+([\w\d_,=><\s]+)\s+INTO TEMP\s+([\w\d_]+);',
                lambda match: f"CREATE TEMP TABLE {match.group(3)} AS SELECT {match.group(1)} FROM {match.group(2)};",
                postgresql_code,
                flags=re.IGNORECASE
            )

            # Remove WITH HOLD if there is no COMMIT or ROLLBACK
            if re.search(r'\bWITH HOLD\b', postgresql_code, re.IGNORECASE) and not re.search(r'\b(COMMIT|ROLLBACK)\b', postgresql_code, re.IGNORECASE):
                postgresql_code = re.sub(r'\bWITH HOLD\b', '', postgresql_code, flags=re.IGNORECASE)
                self.config_parser.print_log_message('DEBUG', f'code contains WITH HOLD but no COMMIT or ROLLBACK')

            self.config_parser.print_log_message('DEBUG3', f'Processing step 4: Converting FOREACH cursor FOR loop to FOR loop')
            # convert FOREACH cursor FOR loop to FOR loop
            foreach_cursor_matches = re.finditer(
                # r'FOREACH\s+\w+\s+FOR\s+SELECT\s+(.*?)\s+INTO\s+(.*?)\s+FROM\s+(.*?)\s+WHERE\s+(.*?)(?=;\s*FOREACH|;\s*END|;\s*IF|;\s*UPDATE|;\s*LET|;\s*SELECT|;|$)',
                r'^FOREACH\s+\w+\s+FOR\s+SELECT\s+(.*?)\s+INTO\s+(.*?)\s+FROM\s+(.*?);?$',
                postgresql_code,
                flags=re.MULTILINE | re.IGNORECASE
            )
            for match in foreach_cursor_matches:
                foreach_cursor_sql = match.group(0)
                for_sql = f'FOR {match.group(2).strip()} IN (SELECT {match.group(1).strip()} FROM {match.group(3).strip()} \n) \nLOOP'
                postgresql_code = postgresql_code.replace(foreach_cursor_sql, for_sql)

            foreach_cursor_matches = re.finditer(
                r'^FOREACH\s+SELECT\s+(.*?)\s+INTO\s+(.*?)\s+FROM\s+(.*?);?$',
                postgresql_code,
                flags=re.MULTILINE | re.IGNORECASE
            )
            for match in foreach_cursor_matches:
                foreach_cursor_sql = match.group(0)
                for_sql = f'FOR {match.group(2).strip()} IN (SELECT {match.group(1).strip()} FROM {match.group(3).strip()} \n)\nLOOP'
                postgresql_code = postgresql_code.replace(foreach_cursor_sql, for_sql)

            self.config_parser.print_log_message('DEBUG3', f'Processing step 5: Header for Procedures, Adding AS $$ and BEGIN to the code')
            # header for procedures
            header_match = re.search(r'CREATE PROCEDURE.*?\);', postgresql_code, flags=re.DOTALL | re.IGNORECASE)
            if header_match:
                header_end = header_match.end()-1
                postgresql_code = postgresql_code[:header_end] + ' AS $$\n' + postgresql_code[header_end:]
            else:
                header_match = re.search(r'CREATE PROCEDURE.*?\(\)\s*', postgresql_code, flags=re.DOTALL | re.IGNORECASE)
                if header_match:
                    header_end = header_match.end()
                    postgresql_code = postgresql_code[:header_end] + '\n AS $$\n' + postgresql_code[header_end:]

            # header for functions
            header_match = re.search(r'CREATE FUNCTION.*?RETURNING\s+\w+\s+;?', postgresql_code, flags=re.DOTALL | re.IGNORECASE)
            if header_match:
                header_end = header_match.end()
                postgresql_code_part = re.sub(r'RETURNING', 'RETURNS', postgresql_code[:header_end], flags=re.DOTALL | re.IGNORECASE)
                if ';' in postgresql_code_part:
                    postgresql_code_part = re.sub(r';', ' AS $$\n', postgresql_code_part, flags=re.DOTALL | re.IGNORECASE)
                else:
                    postgresql_code_part += ' AS $$\n'
                postgresql_code = postgresql_code_part + postgresql_code[header_end:]

            header_match = re.search(r'\s*RETURNING\s+\w+\s+;?', postgresql_code, flags=re.DOTALL | re.IGNORECASE)
            if header_match:
                header_end = header_match.end()
                postgresql_code_part = re.sub(r'RETURNING', 'RETURNS', postgresql_code[:header_end], flags=re.DOTALL | re.IGNORECASE)
                if ';' in postgresql_code_part:
                    postgresql_code_part = re.sub(r';', ' AS $$\n', postgresql_code_part, flags=re.DOTALL | re.IGNORECASE)
                else:
                    postgresql_code_part += ' AS $$\n'
                postgresql_code = postgresql_code_part + postgresql_code[header_end:]

            # Simplify LET commands
            postgresql_code = re.sub(r'(?i)^\s*LET\s+', '', postgresql_code, flags=re.MULTILINE)

            # Add BEGIN after "AS $$" if there is no DECLARE command
            if "DECLARE" not in postgresql_code:
                postgresql_code = re.sub(r'AS\s+\$\$', 'AS $$\nBEGIN', postgresql_code, flags=re.IGNORECASE)

            # Replace Informix specific syntax with PostgreSQL syntax
            returning_matches = re.finditer(r'RETURNING\s+(\w+)\s*;', postgresql_code, flags=re.DOTALL | re.IGNORECASE | re.MULTILINE)
            for match in returning_matches:
                return_type = match.group(1)
                postgresql_code = postgresql_code.replace(match.group(0), f'RETURNS {return_type} AS $$\n')

            postgresql_code = re.sub(r'^\s*WITH RESUME;', '', postgresql_code, flags=re.MULTILINE | re.IGNORECASE)
            postgresql_code = re.sub(r'EXIT\s+WHILE\s*;', 'EXIT;', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r'EXIT\s+FOREACH\s*;', 'EXIT;', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r'EXIT\s+FOR\s*;?', 'EXIT;', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r'CONTINUE\s+FOREACH\s*;', 'CONTINUE;', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r'END\s+PROCEDURE\s*;', 'END;', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r'END\s+FUNCTION\s*;', 'END;', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r'END\s+WHILE', 'END LOOP;', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r'END\s+FOREACH\s*;', 'END LOOP;', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r'END\s+FOR\s*;?', 'END LOOP;', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r'ELIF\s*', 'ELSIF ', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r'END\s+IF\s*', 'END IF', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r'current', 'CURRENT_TIMESTAMP', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r'""', "''", postgresql_code, flags=re.IGNORECASE)

            postgresql_code = re.sub(r'set\s+debug\s+file\s+to\s+.*;$', r'/* \g<0> */', postgresql_code, flags=re.MULTILINE | re.IGNORECASE)
            postgresql_code = re.sub(r'TRACE\s+ON\s*;\s*$', r'/* \g<0> */', postgresql_code, flags=re.MULTILINE | re.IGNORECASE)

            postgresql_code = re.sub(r'(?i)^\s*WHILE\s+.*$', lambda match: match.group(0) + ' LOOP\n', postgresql_code, flags=re.MULTILINE)
            postgresql_code = re.sub(r';\s*LOOP', '\nLOOP', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r'BEGIN;', 'BEGIN', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r'^LOOP;', 'LOOP', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r'ELSE;', 'ELSE', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r';;', ';', postgresql_code, flags=re.IGNORECASE )
            postgresql_code = re.sub(r'\*/;', '*/', postgresql_code, flags=re.IGNORECASE)

            self.config_parser.print_log_message('DEBUG3', f'Processing step 7: Replacing source schema and table names with target schema and table names ({len(table_list)} tables)')

            for table in table_list:
                self.config_parser.print_log_message('DEBUG3', f'Replacing table {table} from schema {source_schema} to {target_schema}')

                source_table_pattern = re.compile(rf'("{source_schema}"\.)?"{table}"')
                target_table = f'"{target_schema}"."{table}"'
                postgresql_code = source_table_pattern.sub(target_table, postgresql_code)

                source_table_pattern = re.compile(rf'\b{table}\b')
                postgresql_code = source_table_pattern.sub(target_table, postgresql_code)

            for view in view_list:
                self.config_parser.print_log_message('DEBUG3', f'Replacing view {view} from schema {source_schema} to {target_schema}')

                source_view_pattern = re.compile(rf'("{source_schema}"\.)?"{view}"')
                target_view = f'"{target_schema}"."{view}"'
                postgresql_code = source_view_pattern.sub(target_view, postgresql_code)

                source_view_pattern = re.compile(rf'\b{view}\b')
                postgresql_code = source_view_pattern.sub(target_view, postgresql_code)

            # Remove second occurrence of "target_schema" in %TYPE declarations
            postgresql_code = re.sub(
                                    rf'("{target_schema}"\."\w+"\.)"{target_schema}"\.("\w+"%TYPE)',
                                    rf'\1\2', postgresql_code,
                                    flags=re.MULTILINE | re.IGNORECASE)

            # Add function return type and language
            postgresql_code += f'\n$$ LANGUAGE plpgsql {function_immutable};'

            # Remove lines which contain only ";"
            postgresql_code = "\n".join([line for line in postgresql_code.split('\n') if line.strip() != ";"])
            # Remove empty lines from the converted code
            postgresql_code = "\n".join([line for line in postgresql_code.splitlines() if line.strip()])

            # Repair function header
            # returning_matches = re.finditer(r'^\s*CREATE\s+FUNCTION\s+[\w\s]+\(\)\s+RETURNS\s+(\w+)\s*;', postgresql_code, flags=re.DOTALL | re.IGNORECASE | re.MULTILINE)
            # returning_matches = re.finditer(r'^\s*CREATE\s+FUNCTION\s+[\w\s".]+\([\w\s".]+\)\s+RETURNS\s+(\w+)\s*;', postgresql_code, flags=re.DOTALL | re.IGNORECASE | re.MULTILINE)
            returning_matches = re.finditer(r'^\s*(CREATE\s+FUNCTION\s+.*?\))\s+RETURNS\s+(\w+)\s*;', postgresql_code, flags=re.DOTALL | re.IGNORECASE | re.MULTILINE)
            for match in returning_matches:
                header_part = match.group(1)
                return_type = match.group(2)
                postgresql_code = postgresql_code.replace(match.group(0), f'{header_part} RETURNS {return_type} AS $$\n')

            self.config_parser.print_log_message('DEBUG3', 'Processing step 8: Handling ON EXCEPTION blocks')
            # some procs /funcs have ON EXCEPTION block, some of them several times
            if "ON EXCEPTION" in postgresql_code:
                exception_lines = [line for line in postgresql_code.split('\n') if 'ON EXCEPTION' in line]
                commentedout_exception_occurences = 0
                for line in exception_lines:
                    line = line.strip()
                    if line.startswith("/*"):
                        commentedout_exception_occurences += 1

                live_exception_occurences = len(exception_lines) - commentedout_exception_occurences
                self.config_parser.print_log_message('DEBUG3', f'Found {len(exception_lines)} ON EXCEPTION occurences, {commentedout_exception_occurences} commented out, {live_exception_occurences} live')
                if live_exception_occurences > 0:

                    for i in range(live_exception_occurences):
                        #### handle ON EXCEPTION block in scope of the main BEGIN - END block
                        # Split the postgresql_code by lines
                        lines = postgresql_code.split('\n')

                        # Find the first occurrence of BEGIN
                        begin_index = next((i for i, line in enumerate(lines) if 'BEGIN' in line), None)
                        self.config_parser.print_log_message('DEBUG3', f'ON EXCEPTION - begin_index: {begin_index}')

                        if begin_index is not None:
                            # Find the ON EXCEPTION - END EXCEPTION block that follows the first BEGIN
                            exception_start_index = next((i for i, line in enumerate(lines[begin_index:], start=begin_index) if 'ON EXCEPTION' in line), None)
                            exception_end_index = next((i for i, line in enumerate(lines[begin_index:], start=begin_index) if 'END EXCEPTION;' in line), None)

                            # Ensure that exception_start_index is immediately after begin_index
                            if exception_start_index is not None and exception_start_index != begin_index + 1:
                                self.config_parser.print_log_message('DEBUG3', 'ON EXCEPTION does not immediately follow BEGIN, trying LOOP occurence')

                                ## try LOOP - END LOOP occurence
                                # loop_begin_index = next((i for i, line in enumerate(lines) if 'LOOP' in line), None)
                                loop_begin_index = next((i for i, line in enumerate(lines) if 'LOOP' in line and i + 1 < len(lines) and 'ON EXCEPTION' in lines[i + 1]), None)

                                self.config_parser.print_log_message('DEBUG3', f'loop_begin_index: {loop_begin_index}')

                                if loop_begin_index is not None:
                                    # Find the ON EXCEPTION - END EXCEPTION block that follows the first BEGIN
                                    exception_start_index = next((i for i, line in enumerate(lines[loop_begin_index:], start=loop_begin_index) if 'ON EXCEPTION' in line), None)
                                    exception_end_index = next((i for i, line in enumerate(lines[loop_begin_index:], start=loop_begin_index) if 'END EXCEPTION' in line), None)

                                    # Ensure that exception_start_index is immediately after loop_begin_index
                                    if exception_start_index is not None and exception_start_index != loop_begin_index + 1:
                                        self.config_parser.print_log_message('DEBUG3', 'ON EXCEPTION does not immediately follow LOOP command')

                                    if exception_start_index is not None and exception_end_index is not None:
                                        # Extract the exception block
                                        exception_block = lines[exception_start_index:exception_end_index + 1]

                                        # Replace the line with index exception_start_index with a new line containing "BEGIN"
                                        lines[exception_start_index] = "BEGIN"
                                        # Remove the ON EXCEPTION - END EXCEPTION block from its current position
                                        del lines[exception_start_index+1:exception_end_index + 1]

                                        # Find the ON EXCEPTION line
                                        on_exception_line = next((line for line in exception_block if 'ON EXCEPTION SET' in line), None)

                                        set_variable_line = ''
                                        variable_name = ''
                                        if on_exception_line:
                                            # Extract the variable name from the ON EXCEPTION line
                                            match = re.search(r'ON EXCEPTION\s+SET\s+([\w\s,]+);', on_exception_line)
                                            if match:
                                                variable_names = [var.strip() for var in match.group(1).split(',')]
                                                if len(variable_names) == 1:
                                                    set_variable_line = f"""{variable_names[0]} = SQLSTATE||'-'||SQLERRM;"""
                                                elif len(variable_names) == 2:
                                                    set_variable_line = f"""{variable_names[0]} = SQLSTATE;\n{variable_names[1]} = SQLERRM;"""
                                                elif len(variable_names) == 3:
                                                    set_variable_line = f"""{variable_names[0]} = SQLSTATE;\n{variable_names[1]} = SQLERRM;\n{variable_names[2]} = '';"""
                                                # match = re.search(r'ON EXCEPTION SET (.*?);', on_exception_line)
                                                # variable_names = match.group(1).split(',') if match else ['unknown_variable']
                                                # if len(variable_names) == 1:
                                                #     set_variable_line = f"""{variable_names[0]} = SQLSTATE||'-'||SQLERRM;"""
                                                # elif len(variable_names) == 3:
                                                #     set_variable_line = f"""{variable_names[0]} = SQLSTATE;\n{variable_name[1]} = SQLSTATE;\n {variable_name[2]} = SQLERRM;"""
                                                # print(f'set_variable_line: {set_variable_line}')
                                            # else:
                                            #     raise ValueError(f"Failed to find a match for 'ON EXCEPTION SET' in line: {on_exception_line}")

                                        # Modify the exception block
                                        modified_exception_block = [re.sub(r'ON EXCEPTION SET \w+', f'EXCEPTION WHEN OTHERS THEN\n{set_variable_line}', line) for line in exception_block]
                                        modified_exception_block = [re.sub(r'ON EXCEPTION;?', f'EXCEPTION WHEN OTHERS THEN', line) for line in modified_exception_block]
                                        modified_exception_block = [line for line in modified_exception_block if 'END EXCEPTION' not in line]
                                        modified_exception_block.append('END;')

                                        # Insert the modified exception block before the last END;
                                        end_index = next((i for i, line in enumerate(lines) if 'END LOOP;' in line), None)
                                        if end_index is not None:
                                            lines = lines[:end_index] + modified_exception_block + lines[end_index:]

                                    postgresql_code = '\n'.join(lines)

                            elif exception_start_index is not None and exception_end_index is not None:
                                # Extract the exception block
                                exception_block = lines[exception_start_index:exception_end_index + 1]

                                # Remove the ON EXCEPTION - END EXCEPTION block from its current position
                                del lines[exception_start_index:exception_end_index + 1]

                                # Find the ON EXCEPTION line
                                on_exception_line = next((line for line in exception_block if 'ON EXCEPTION SET' in line), None)

                                set_variable_line = ''
                                if on_exception_line:
                                    # Extract the variable name from the ON EXCEPTION line
                                    # variable_name = re.search(r'ON EXCEPTION SET (\w+);', on_exception_line).group(1)
                                    variable_name = ''
                                    match = re.search(r'ON EXCEPTION SET (\w+);', on_exception_line)
                                    if match:
                                        variable_names = [var.strip() for var in match.group(1).split(',')]
                                        if len(variable_names) == 1:
                                            set_variable_line = f"""{variable_names[0]} = SQLSTATE||'-'||SQLERRM;"""
                                        elif len(variable_names) == 2:
                                            set_variable_line = f"""{variable_names[0]} = SQLSTATE;\n{variable_names[1]} = SQLERRM;"""
                                        elif len(variable_names) == 3:
                                            set_variable_line = f"""{variable_names[0]} = SQLSTATE;\n{variable_names[1]} = SQLERRM;\n{variable_names[2]} = '';"""
                                        # variable_names = match.group(1).split(',') if match else ['unknown_variable']
                                        # if len(variable_names) == 1:
                                        #     set_variable_line = f"""{variable_names[0]} = SQLSTATE||'-'||SQLERRM;"""
                                        # elif len(variable_names) == 3:
                                        #     set_variable_line = f"""{variable_names[0]} = SQLSTATE;\n{variable_name[1]} = SQLSTATE;\n {variable_name[2]} = SQLERRM;"""
                                        # print(f'set_variable_line: {set_variable_line}')
                                    # else:
                                    #     raise ValueError(f"Failed to find a match for 'ON EXCEPTION SET' in line: {on_exception_line}")

                                # Modify the exception block
                                modified_exception_block = [re.sub(r'ON EXCEPTION SET \w+', f'EXCEPTION WHEN OTHERS THEN\n{set_variable_line}', line) for line in exception_block]
                                modified_exception_block = [re.sub(r'ON EXCEPTION;?', f'EXCEPTION WHEN OTHERS THEN', line) for line in modified_exception_block]
                                modified_exception_block = [line for line in modified_exception_block if 'END EXCEPTION' not in line]

                                end_index = next((i for i, line in enumerate(lines) if 'END;' in line and '$$ LANGUAGE plpgsql {function_immutable};' in lines[i + 1]), None)
                                if end_index is not None:
                                    lines = lines[:end_index] + modified_exception_block + lines[end_index:]

                                # Join the lines back into a single string
                                postgresql_code = '\n'.join(lines)

            postgresql_code = re.sub(r';;', ';', postgresql_code, flags=re.IGNORECASE)
            # Indent the code
            postgresql_code = self.config_parser.indent_code(postgresql_code)
            # Remove empty lines from the converted code
            postgresql_code = "\n".join([line for line in postgresql_code.splitlines() if line.strip()])

            # Check if the first or second line ends with AS $$ and the next line starts with RETURN
            lines = postgresql_code.splitlines()
            for i in range(len(lines) - 1):
                if lines[i].strip().endswith("AS $$") and lines[i + 1].strip().startswith("RETURN"):
                    lines.insert(i + 1, "BEGIN")
                    break
            postgresql_code = "\n".join(lines)

            return postgresql_code

        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

    def fetch_sequences(self, table_schema: str, table_name: str):
        pass

    def get_sequence_details(self, sequence_owner, sequence_name):
        # Placeholder for fetching sequence details
        return {}

    def migrate_table(self, migrate_target_connection, settings):
        part_name = 'initialize'
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
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Table {source_table} is empty - skipping data migration.")
                return 0
            else:
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Table {source_table} has {source_table_rows} rows - starting data migration.")
                # Fetch the data in batches
                # Open a cursor and fetch rows in batches

                select_columns_list = []
                for order_num, col in source_columns.items():
                    self.config_parser.print_log_message('DEBUG2',
                                                         f"Worker {worker_id}: Table {source_schema}.{source_table}: Processing column {col['column_name']} ({order_num}) with data type {col['data_type']}")
                    insert_columns = ', '.join([f'''"{col['column_name']}"''' for col in source_columns.values()])

                    if col['data_type'].lower() == 'datetime':
                        select_columns_list.append(f"TO_CHAR({col['column_name']}, '%Y-%m-%d %H:%M:%S') as {col['column_name']}")
                    #     select_columns_list.append(f"ST_asText(`{col['column_name']}`) as `{col['column_name']}`")
                    # elif col['data_type'].lower() == 'set':
                    #     select_columns_list.append(f"cast(`{col['column_name']}` as char(4000)) as `{col['column_name']}`")
                    else:
                        select_columns_list.append(f"{col['column_name']}")
                select_columns = ', '.join(select_columns_list)

                query = f'''SELECT {select_columns} FROM "{source_schema}".{source_table}'''
                if migration_limitation:
                    query += f" WHERE {migration_limitation}"

                self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Fetching data with cursor using query: {query}")

                part_name = 'execute query'
                cursor = self.connection.cursor()
                cursor.execute(query)
                # offset = 0
                total_inserted_rows = 0
                while True:
                    # part_name = f'prepare fetch data: {source_table} - {offset}'
                    # if primary_key_columns:
                    #     query = f"SELECT SKIP {offset} * FROM {source_schema}.{source_table} ORDER BY {primary_key_columns} LIMIT {batch_size}"
                    # else:
                    #     query = f"SELECT SKIP {offset} * FROM {source_schema}.{source_table} LIMIT {batch_size}"


                    # part_name = f'do fetch data: {source_table} - {offset}'
                    # # polars library is not always available
                    # df = pl.read_database(query, self.connection)
                    # if df.is_empty():
                    #     break
                    # self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Fetched {len(df)} rows from source table {source_table}.")
                    # # self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Migrating batch starting at offset {offset} for table {table_name}.")
                    # # Convert Polars DataFrame to list of tuples for insertion
                    # records = df.to_dicts()

                    # cursor.execute(query)
                    # records = cursor.fetchall()
                    records = cursor.fetchmany(batch_size)
                    if not records:
                        break
                    self.config_parser.print_log_message('DEBUG',f"Worker {worker_id}: Fetched {len(records)} rows from source table {source_table}.")

                    records = [
                        {column['column_name']: value for column, value in zip(source_columns.values(), record)}
                        for record in records
                    ]

                    # Adjust binary or bytea types
                    for record in records:
                        for order_num, column in source_columns.items():
                            column_name = column['column_name']
                            column_type = column['data_type']
                            target_column_type = target_columns[order_num]['data_type']
                            # if column_type.lower() in ['binary', 'bytea']:
                            if column_type.lower() in ['blob']:
                                record[column_name] = bytes(record[column_name].getBytes(1, int(record[column_name].length())))  # Convert 'com.informix.jdbc.IfxCblob' to bytes
                            elif column_type.lower() in ['clob']:
                                # elif isinstance(record[column_name], IfxCblob):
                                record[column_name] = record[column_name].getSubString(1, int(record[column_name].length()))  # Convert IfxCblob to string
                                # record[column_name] = bytes(record[column_name].getBytes(1, int(record[column_name].length())))  # Convert IfxBblob to bytes
                                # record[column_name] = record[column_name].read()  # Convert IfxBblob to bytes
                            elif column_type.lower() in ['integer', 'smallint', 'tinyint', 'bit', 'boolean'] and target_column_type.lower() in ['boolean']:
                                # Convert integer to boolean
                                record[column_name] = bool(record[column_name])

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
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Inserted {inserted_rows} (total: {total_inserted_rows} from: {source_table_rows} ({round(total_inserted_rows/source_table_rows*100, 2)}%)) rows into target table {target_table}")

                    # offset += batch_size

                target_table_rows = migrate_target_connection.get_rows_count(target_schema, target_table)
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Target table {target_schema}.{target_table} has {target_table_rows} rows")
                migrator_tables.update_data_migration_status(protocol_id, True, 'OK', target_table_rows)
                cursor.close()
                return target_table_rows
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Error during {part_name} -> {e}")
            self.config_parser.print_log_message('ERROR', "Full stack trace:")
            self.config_parser.print_log_message('ERROR', traceback.format_exc())
            raise e


    def fetch_triggers(self, table_id: int, table_schema: str, table_name: str):
        try:
            query = f"""
            select tr.trigid, tr.trigname,
            case when tr.event = 'D' then 'ON DELETE'
            when tr.event = 'I' then 'INSERT'
            when tr.event = 'U' then 'UPDATE'
            when tr.event = 'S' then 'SELECT'
            when tr.event = 'd' then 'INSTEAD OF DELETE'
            when tr.event = 'i' then 'INSTEAD OF INSERT'
            when tr.event = 'u' then 'INSTEAD OF UPDATE'
            else tr.event end as trigger_event,
            tr.old, tr.new
            from systriggers tr
            where tr.owner = '{table_schema}' and tr.tabid = {table_id}
            """
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            triggers = {}
            order_num = 1
            for row in cursor.fetchall():
                self.config_parser.print_log_message('DEBUG', f"fetch_triggers row: {row}")
                triggers[order_num] = {
                    'id': row[0],
                    'name': row[1].strip(),
                    'event': row[2].strip(),
                    'row_statement': '',
                    'old': row[3].strip() if row[3] else '',
                    'new': row[4].strip() if row[4] else '',
                    'sql': '',
                    'comment': ''
                }

                query = f"""
                SELECT data
                FROM systrigbody
                WHERE datakey IN ('A', 'D')
                AND trigid = {row[0]}
                ORDER BY trigid, datakey DESC, seqno
                """
                cursor.execute(query)
                trigger_code = cursor.fetchall()
                trigger_code_str = '\n'.join([body[0].strip() for body in trigger_code])

                trigger_code_lines = trigger_code_str.split('\n')

                for i, line in enumerate(trigger_code_lines):
                    line = line.strip()  # Remove trailing spaces
                    if line.startswith("--"):
                        trigger_code_lines[i] = f"/* {line.strip()} */"

                trigger_code_str = '\n'.join(trigger_code_lines)

                self.config_parser.print_log_message('DEBUG', f"trigger SQL: {trigger_code_str}")

                triggers[order_num]['sql'] = trigger_code_str
                triggers[order_num]['row_statement'] = 'FOR EACH ROW' if 'FOR EACH ROW' in trigger_code_str.upper() else ''
                order_num += 1
            cursor.close()
            self.disconnect()
            return triggers
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error when fetching triggers for the table {table_name}/{table_id}: {e}")
            raise


    def convert_trigger(self, informix_code: str, settings: dict):
        pgsql_trigger_code = ''
        pgsql_triggers = []
        trigger_code = ''
        trigger_name = ''
        func_code = ''

        try:
            # Split the input into individual trigger definitions
            triggers = re.split(r'(?i)create trigger', informix_code, re.IGNORECASE | re.DOTALL | re.MULTILINE)
            for trig in triggers:
                trig = trig.strip()
                if not trig:
                    continue

                trig_lines = trig.split('\n')
                trig_lines = [line.strip() for line in trig_lines]
                trig_lines = [line for line in trig_lines if line != '--']
                trig_lines = [f"/* {line.strip()} */" if line.startswith('--') else line for line in trig_lines]
                trig = '\n'.join(trig_lines)
                self.config_parser.print_log_message('DEBUG', f"Trigger code: {trig}")

                # Replace groups of multiple spaces with just one space
                trig = re.sub(r'\s+', ' ', trig)
                # Add new line character before each word WHEN (case insensitive)
                trig = re.sub(r'(?i)\s*when', '\nWHEN', trig, flags=re.IGNORECASE)

                # Extract how NEW and OLD are referenced in Informix code
                new_ref = ""
                old_ref = ""
                ref_match = re.search(r'referencing\s+(new\s+as\s+(\S+))?\s*(old\s+as\s+(\S+))?', trig, re.IGNORECASE)
                if ref_match:
                    new_ref = ref_match.group(2) if ref_match.group(2) else ""
                    old_ref = ref_match.group(4) if ref_match.group(4) else ""

                self.config_parser.print_log_message('DEBUG', f"new_ref: {new_ref}, old_ref: {old_ref}")

                # Extract schema, trigger name, and operation (insert/update)
                header_match = re.match(r'"([^"]+)"\.(\S+)\s+(insert|update|delete)', trig, re.IGNORECASE)
                if not header_match:
                    continue
                schema = header_match.group(1)
                trigger_name = header_match.group(2)
                operation = header_match.group(3).lower()

                self.config_parser.print_log_message('DEBUG', f"Trigger name: {trigger_name}, Operation: {operation}")

                # Extract the table name (assumes: on "schemaname".table)
                table_match = re.search(r'\s+on\s+"([^"]+)"\.(\S+)', trig, re.IGNORECASE)
                if table_match:
                    table_schema = table_match.group(1)
                    table_name = table_match.group(2)
                else:
                    table_schema = schema
                    table_name = "unknown_table"

                self.config_parser.print_log_message('DEBUG', f"Table name: {table_name}, Schema: {table_schema}")

                func_body_lines = []

                order_num = 1
                when_conditions = {}
                proc_calls = {}

                # when_pattern = re.compile(r'when\s*\((.*?)\)\s*\((.*?)\)', re.DOTALL | re.IGNORECASE)
                # after_pattern = re.compile(r'after\s*\((.*)\)', re.DOTALL | re.IGNORECASE)

                # when_matches = re.findall(r'(?:when\s*\((.*?)\)\s*)?\(\s*(execute procedure.*?;?)\s*\)', trig, re.IGNORECASE | re.DOTALL | re.MULTILINE)
                # when_matches = re.findall(r'(?:when\s*\((.*?)\)\s*)?\(\s*(execute procedure.*?\(.*?\));?\s*\)', trig, re.IGNORECASE | re.DOTALL | re.MULTILINE)
                # when_matches = re.findall(r'(?:when\s*\((?:\((?:\((.*?)\))?\))?\)\s*)?\(\s*(execute procedure.*?\(.*?\));?\s*\)', trig, re.IGNORECASE | re.DOTALL | re.MULTILINE)
                when_matches = re.findall(r'when\s*\((.*?)\)\s*\((.*?\)\s*\))', trig, re.IGNORECASE | re.DOTALL | re.MULTILINE)
                # when_matches = re.findall(r'when\s*\((.*?)\)\s*\((.*?\n*?)\)', trig, re.IGNORECASE | re.DOTALL | re.MULTILINE)

                self.config_parser.print_log_message('DEBUG', f"when_matches: {when_matches}")

                for match in when_matches:
                    when_condition = match[0]
                    proc_call = match[1]
                    proc_call = re.sub(r'\*/', '*/\n', proc_call, flags=re.IGNORECASE)
                    when_conditions[order_num] = when_condition
                    proc_calls[order_num] = proc_call
                    order_num += 1
                    self.config_parser.print_log_message('DEBUG', f"when_condition: {when_condition}")
                    self.config_parser.print_log_message('DEBUG', f"proc_call: {proc_call}")

                after_pattern = re.compile(r'after\s*\((.*)\)', re.DOTALL | re.IGNORECASE | re.MULTILINE)
                # Extract AFTER clause
                after_match = after_pattern.search(trig)
                after_all_commands = []
                after_current_command = []
                if after_match:
                    after_content = after_match.group(1).strip()
                    # Split the content into individual SQL commands by commas, considering nested structures
                    open_parentheses = 0

                    for part in re.split(r'(,)', after_content):  # Keep commas as separate tokens
                        if part == ',' and open_parentheses == 0:
                        # Check if the current command starts with a valid SQL keyword
                            if after_current_command and any(after_current_command[0].strip().lower().startswith(kw) for kw in ['insert', 'update', 'delete']):
                                # End of a command
                                after_all_commands.append(''.join(after_current_command).strip())
                                after_current_command = []
                            else:
                                # Concatenate with the previous part
                                if after_all_commands:
                                    after_all_commands[-1] += ',' + ''.join(after_current_command).strip()
                                    after_current_command = []
                        else:
                            after_current_command.append(part)
                            # Track parentheses to handle nested structures
                            open_parentheses += part.count('(') - part.count(')')

                    # Add the last command if any
                    if after_current_command:
                        if any(after_current_command[0].strip().lower().startswith(kw) for kw in ['insert', 'update', 'delete']):
                            after_all_commands.append(''.join(after_current_command).strip())
                        else:
                            if after_all_commands:
                                after_all_commands[-1] += ',' + ''.join(after_current_command).strip()

                    self.config_parser.print_log_message('DEBUG', f"AFTER part after_all_commands: {after_all_commands}")

                if not when_conditions and not proc_calls:
                    action_all_commands = []
                    action_current_command = []
                    actions_match = re.search(r'for each row\s*\((.*)\)', trig, re.IGNORECASE | re.DOTALL)
                    if actions_match:
                        action_content = actions_match.group(1).strip()
                        # Split the content into individual SQL commands by commas, considering nested structures
                        open_parentheses = 0

                        for part in re.split(r'(,)', action_content):  # Keep commas as separate tokens
                            if part == ',' and open_parentheses == 0:
                            # Check if the current command starts with a valid SQL keyword
                                if action_current_command and any(action_current_command[0].strip().lower().startswith(kw) for kw in ['insert', 'update', 'delete']):
                                    # End of a command
                                    action_all_commands.append(''.join(action_current_command).strip())
                                    action_current_command = []
                                else:
                                    # Concatenate with the previous part
                                    if action_all_commands:
                                        action_all_commands[-1] += ',' + ''.join(action_current_command).strip()
                                        action_current_command = []
                            else:
                                action_current_command.append(part)
                                # Track parentheses to handle nested structures
                                open_parentheses += part.count('(') - part.count(')')

                        # Add the last command if any
                        if action_current_command:
                            if any(action_current_command[0].strip().lower().startswith(kw) for kw in ['insert', 'update', 'delete']):
                                action_all_commands.append(''.join(action_current_command).strip())
                            else:
                                if action_all_commands:
                                    action_all_commands[-1] += ',' + ''.join(action_current_command).strip()

                        self.config_parser.print_log_message('DEBUG', f"ACTION part action_all_commands: {action_all_commands}")

                        actions = actions_match.group(1).split(',')
                        for action in actions:
                            print(f"action: {action.strip()}")
                            if "execute procedure" in action:
                                # action = re.sub("execute procedure", "", action, flags=re.IGNORECASE) ## keep it for further processing
                                action = re.sub("with trigger references", "", action, flags=re.IGNORECASE)
                                action = action.replace(settings['source_schema'], settings['target_schema'])
                            proc_calls[order_num] = action.strip()
                            order_num += 1

                self.config_parser.print_log_message('DEBUG', f"when_conditions: {when_conditions}")
                self.config_parser.print_log_message('DEBUG', f"proc_calls: {proc_calls}")

                function_name = trigger_name + "_trigfunc"
                counter = 0
                if ((when_conditions and proc_calls) or after_all_commands):

                    for i in when_conditions.keys():
                        proc_call = proc_calls[i].replace("execute procedure", "PERFORM")

                        if when_conditions[i]:
                            func_body_lines.append(f"    IF {when_conditions[i]} THEN")
                            func_body_lines.append(f"        {proc_call.replace(settings['source_schema'], settings['target_schema'])};")
                            func_body_lines.append("    END IF;")

                    if re.search(r'for each row', trig, re.IGNORECASE) and after_all_commands:
                        func_body_lines.append(f"""    /* AFTER part */""")
                        for after_command in after_all_commands:
                            if after_command:
                                func_body_lines.append(f"""    {after_command.replace(f'''"{settings['source_schema']}"''', f'''"{settings['target_schema']}"''')};""")

                    if ((not re.search(r'for each row', trig, re.IGNORECASE) or re.search(r'before', trig, re.IGNORECASE))
                        and after_all_commands):
                        self.config_parser.print_log_message('ERROR', f"Trigger {trigger_name} has AFTER clause but is not FOR EACH ROW. This is not supported!!!")
                        func_body_lines.append("/* AFTER clause not migrated */")
                        for after_command in after_all_commands:
                            if after_command:
                                func_body_lines.append(f"/*    {after_command.replace(settings['source_schema'], settings['target_schema'])}; */")

                    self.config_parser.print_log_message('DEBUG3', f"func_body_lines: {func_body_lines}")

                    func_code = f"""CREATE OR REPLACE FUNCTION "{settings['target_schema']}"."{function_name + str(counter)}"()
                        RETURNS trigger AS $$
                        BEGIN
                        {chr(10).join(func_body_lines)}
                            RETURN NEW;
                        END;
                        $$ LANGUAGE plpgsql;"""

                    trigger_code = f"""CREATE TRIGGER "{trigger_name + str(counter)}" """

                    if re.search(r'for each row', trig, re.IGNORECASE):
                        trigger_code += f"""\nAFTER {operation.upper()} ON "{table_schema.replace(settings['source_schema'], settings['target_schema'])}"."{table_name}" """

                    if new_ref:
                        trigger_code += f"\nREFERENCING NEW TABLE AS {new_ref}"
                    if old_ref:
                        trigger_code += f"\nREFERENCING OLD TABLE AS {old_ref}"

                    if re.search(r'for each row', trig, re.IGNORECASE):
                        trigger_code += f"\nFOR EACH ROW"

                    trigger_code += f"\nEXECUTE FUNCTION {schema.replace(settings['source_schema'], settings['target_schema'])}.{function_name + str(counter)}();"
                    counter += 1

                    pgsql_triggers.append(func_code + "\n\n" + trigger_code)

                elif not when_conditions and proc_calls:
                    for i in proc_calls.keys():
                        trigger_code = ''
                        func_code = ''
                        proc_call = proc_calls[i]
                        self.config_parser.print_log_message('DEBUG3', f"proc_call: {proc_call}")

                        trigger_code = f"""CREATE TRIGGER "{trigger_name + str(counter)}" """

                        if re.search(r'for each row', trig, re.IGNORECASE):
                            trigger_code += f"""\nAFTER {operation.upper()} ON "{table_schema.replace(settings['source_schema'], settings['target_schema'])}"."{table_name}" """

                        if new_ref:
                            trigger_code += f"\nREFERENCING NEW TABLE AS {new_ref}"
                        if old_ref:
                            trigger_code += f"\nREFERENCING OLD TABLE AS {old_ref}"

                        if re.search(r'for each row', trig, re.IGNORECASE):
                            trigger_code += f"\nFOR EACH ROW"

                        if proc_call.startswith("execute procedure"):
                            proc_call = proc_call.replace("execute procedure", "")
                            trigger_code += f"\nEXECUTE FUNCTION {proc_call};"
                        else:
                            func_code = f"""CREATE OR REPLACE FUNCTION "{settings['target_schema']}"."{function_name + str(counter)}"()
                                RETURNS trigger AS $$
                                BEGIN
                                    {proc_call.replace(f'''"{settings['source_schema']}"''', f'''"{settings['target_schema']}"''')};
                                    RETURN NEW;
                                END;
                                $$ LANGUAGE plpgsql;"""
                            trigger_code += f"\nEXECUTE FUNCTION {settings['target_schema']}.{function_name + str(counter)}();"
                            counter += 1

                        pgsql_triggers.append(func_code + "\n\n" + trigger_code)

            pgsql_trigger_code = "\n\n".join(pgsql_triggers)
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error converting trigger {trigger_name}: {e}")
            self.config_parser.print_log_message('ERROR', traceback.format_exc())

        return pgsql_trigger_code



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

    def get_sequence_maxvalue(self, sequence_id: int):
        query = f"SELECT maxval FROM syssqlsequences WHERE seqid = {sequence_id}"
        cursor = self.connection.cursor()
        cursor.execute(query)
        maxval = cursor.fetchone()[0]
        cursor.close()
        return maxval

    def handle_error(self, e, description=None):
        self.config_parser.print_log_message('ERROR', f"An error in {self.__class__.__name__} ({description}): {e}")
        self.config_parser.print_log_message('ERROR', traceback.format_exc())
        if self.on_error_action == 'stop':
            self.config_parser.print_log_message('ERROR', "Stopping due to error.")
            exit(1)
        else:
            pass

    def get_rows_count(self, table_schema: str, table_name: str):
        query = f"""SELECT COUNT(*) FROM "{table_schema}".{table_name} """
        self.config_parser.print_log_message('DEBUG3', f"get_rows_count query: {query}")
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

    def get_sequence_current_value(self, sequence_id: int):
        pass

    def fetch_user_defined_types(self, schema: str):
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
        query = """SELECT DBINFO('version','full') FROM systables WHERE tabid = 1;"""
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        version = cursor.fetchone()[0]
        cursor.close()
        self.disconnect()
        return version

    def get_database_size(self):
        return None

    def get_top10_biggest_tables(self, settings):
        query = f"""
            select
                owner, tabname, rowsize, nrows, rowsize*nrows as size
            from systables where owner = '{settings['source_schema']}'
            order by size desc limit 10
        """
        self.config_parser.print_log_message('DEBUG', f"Fetching top 10 biggest tables for schema {settings['source_schema']} with query: {query}")
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        tables = cursor.fetchall()
        cursor.close()
        self.disconnect()
        top_tables = {}
        order_num = 1
        for row in tables:
            top_tables[order_num] = {
                'owner': row[0].strip(),
                'table_name': row[1].strip(),
                'row_size': row[2],
                'row_count': row[3],
                'size': row[4]
            }
            order_num += 1
        self.config_parser.print_log_message('DEBUG', f"Top 10 biggest tables: {top_tables}")
        return top_tables

if __name__ == "__main__":
    print("This script is not meant to be run directly")
