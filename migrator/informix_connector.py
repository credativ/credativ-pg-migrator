import jaydebeapi
from database_connector import DatabaseConnector
from migrator_logging import MigratorLogger
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
        except AttributeError:
            pass

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
        except jaydebeapi.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_table_columns(self, table_schema: str, table_name: str, migrator_tables) -> dict:
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
                            ELSE 'UNKNOWN-'||cast(c.coltype as varchar(10))
                        END
                    ELSE
                    	CASE WHEN x.name IS NOT NULL THEN upper(x.name)
                    	ELSE 'UNKNOWN-'||cast(c.coltype as varchar(10))||'-'||cast(x.extended_id as varchar(10)) END
                END AS coltype,
                c.collength,
                CASE WHEN c.coltype >= 256 THEN 'NOT NULL' ELSE '' END AS nullable,
                CASE WHEN d.type = 'L' THEN
                    CASE WHEN d.default LIKE 'AAAAAA%' THEN replace(d.default, 'AAAAAA ', '')
                    WHEN d.default LIKE 'AAAD6A%' THEN replace(d.default, 'AAAD6A ', '')
                    ELSE d.default END
                ELSE NULL
                END AS default_value
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
            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Reading columns for {table_name}")
            cursor.execute(query)
            for row in cursor.fetchall():
                result[row[0]] = {
                    'name': row[1],
                    'type': row[2],
                    'length': row[3],
                    'nullable': row[4],
                    'default': re.sub(r'[^\x20-\x7E]', ' ', row[5]).strip() if row[5] else '',
                    'other': '',
                    'comment': ''
                }

                # if self.config_parser.get_log_level() == 'DEBUG':
                #     self.logger.debug(f"0 default: {result[row[0]]['default']}")
                # checking for default values substitution with the origingal data type
                if migrator_tables:
                    if result[row[0]]['default'] != '':
                        result[row[0]]['default'] = migrator_tables.check_default_values_substitution(result[row[0]]['name'], result[row[0]]['type'], result[row[0]]['default'])

            cursor.close()
            self.disconnect()
            return result
        except jaydebeapi.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
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
        except jaydebeapi.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_view_code(self, view_id: int):
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

    def convert_view_code(self, view_code: str, settings: dict):
        converted_view_code = view_code
        converted_view_code = converted_view_code.replace(f'''"{settings['source_schema']}".''', f'''"{settings['target_schema']}".''')
        return converted_view_code

    def convert_table_columns(self, target_db_type: str, table_schema: str, table_name: str, source_columns: dict):
        type_mapping = {}
        create_table_sql = ""
        converted = {}
        if target_db_type == 'postgresql':
            type_mapping = {
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
                'SERIAL8': 'BIGSERIAL',
                'SERIAL': 'SERIAL',
                'SMALLFLOAT': 'REAL',
                'SMALLINT': 'SMALLINT',
                'TEXT': 'TEXT',
                'TIME': 'TIME',
                'TIMESTAMP': 'TIMESTAMP',
                'VARCHAR': 'VARCHAR',
            }

            for order_num, column_info in source_columns.items():
                coltype = column_info['type'].upper()
                length = column_info['length']
                if type_mapping.get(coltype, 'UNKNOWN').startswith('UNKNOWN'):
                    self.logger.info(f"Column {column_info['name']} - unknown data type: {column_info['type']}")
                    # coltype = 'TEXT' ## default to TEXT may not be the best option -> let the table creation fail
                else:
                    coltype = type_mapping.get(coltype, 'TEXT')
                if coltype == 'VARCHAR' and column_info['length'] >= 254:
                    coltype = 'TEXT'
                    length = ''

                converted[order_num] = {
                    'name': column_info['name'],
                    'type': coltype,
                    'length': length,
                    'nullable': column_info['nullable'],
                    'default': column_info['default'],
                    'other': column_info['other'],
                    'comment': column_info['comment']
                }

            create_table_sql_parts = []
            for _, info in converted.items():
                create_table_sql_column = ''
                if 'length' in info and info['type'] in ('CHAR', 'VARCHAR'):
                    create_table_sql_column = f""""{info['name']}" {info['type']}({info['length']}) {info['nullable']}"""
                else:
                    create_table_sql_column = f""""{info['name']}" {info['type']} {info['nullable']}"""
                if info['default'] != '':
                    if info['type'] in ('CHAR', 'VARCHAR', 'TEXT'):
                        create_table_sql_column += f" DEFAULT '{info['default']}'".replace("''", "'")
                    else:
                        create_table_sql_column += f" DEFAULT {info['default']}"
                create_table_sql_parts.append(create_table_sql_column)
            create_table_sql = ", ".join(create_table_sql_parts)
            create_table_sql = f"""CREATE TABLE "{table_schema}"."{table_name}" ({create_table_sql})"""
        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

        return converted, create_table_sql

    def fetch_indexes(self, source_table_id: int, target_schema, target_table_name, target_columns):
        table_indexes = {}
        order_num = 1
        query = f"""
            SELECT idxname, idxtype, clustered, part1, part2, part3, part4, part5, part6, part7, part8, part9, part10, part11, part12, part13, part14, part15, part16
            FROM sysindexes
            WHERE tabid = '{source_table_id}'
            ORDER BY idxname
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)

            indexes = cursor.fetchall()

            for index in indexes:
                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Processing index: {index}")
                index_name = index[0]
                index_type = index[1]
                colnos = [colno for colno in index[3:] if colno]

                # Get column names for each colno
                columns = []
                for colno in colnos:
                    cursor.execute(f"SELECT colname FROM syscolumns WHERE colno = {colno} AND tabid = {source_table_id}")
                    colname = cursor.fetchone()[0]
                    columns.append(colname)

                # Check if the index is a primary key by looking at sysconstraints
                cursor.execute(f"""
                SELECT constrtype, constrname FROM sysconstraints
                WHERE tabid = {source_table_id} AND idxname = '{index_name}'
                """)
                constraint = cursor.fetchone()
                if constraint and constraint[0] in ('P', 'R'):
                    index_type = constraint[0]
                    index_name = constraint[1]

                index_columns = ', '.join([f'"{col}"' for col in columns])
                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Columns list: {index_columns}, index type: {index_type}, clustered: {index[2]}")

                index_columns_count = 0
                index_columns_data_types = []
                for column_name in index_columns.split(','):
                    column_name = column_name.strip().strip('"')
                    for order_num, column_info in target_columns.items():
                        if column_name == column_info['name']:
                            index_columns_count += 1
                            column_data_type = column_info['type']
                            if self.config_parser.get_log_level() == 'DEBUG':
                                self.logger.debug(f"Table: {target_schema}.{target_table_name}, index: {index_name}, column: {column_name} has data type {column_data_type}")
                            index_columns_data_types.append(column_data_type)
                            index_columns_data_types_str = ', '.join(index_columns_data_types)

                create_index_query = None
                if index_type == 'U':
                    create_index_query = f"""CREATE UNIQUE INDEX "{index_name}" ON "{target_schema}"."{target_table_name}" ({index_columns});"""
                elif index_type == 'P':
                    create_index_query = f"""ALTER TABLE "{target_schema}"."{target_table_name}" ADD CONSTRAINT "{index_name}" PRIMARY KEY ({index_columns});"""
                else:
                    if index_type != 'R':
                        create_index_query = f"""CREATE INDEX "{index_name}" ON "{target_schema}"."{target_table_name}" ({index_columns});"""
                    else:
                        pass
                        # Skipping Foreign key

                if create_index_query:
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"SQL: {create_index_query}")
                    table_indexes[order_num] = {
                        'name': index_name,
                        'type': "PRIMARY KEY" if index_type == 'P' else "UNIQUE" if index_type == 'U' else "INDEX",
                        'columns': index_columns,
                        'columns_count': index_columns_count,
                        'columns_data_types': index_columns_data_types_str,
                        'sql': create_index_query,
                        'comment': ''
                    }
                    order_num += 1

            cursor.close()
            self.disconnect()
            return table_indexes

        except jaydebeapi.Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_constraints(self, source_table_id: int, target_schema, target_table_name):
        # Get all indexes for the table
        order_num = 1
        table_constraints = {}
        create_constr_query = ""
        constr_id = 0

        # index_query = f"""
        # SELECT idxname, idxtype, clustered
        # FROM sysindexes WHERE tabid = {source_table_id}
        # """

        self.connect()
        cursor = self.connection.cursor()

        # if self.config_parser.get_log_level() == 'DEBUG':
        #     self.logger.debug(f"Reading constraints for {target_table_name}")
        # cursor.execute(index_query)
        # indexes = cursor.fetchall()

        # for index in indexes:
            # index_name = index[0]
            # Check if the index is a primary key by looking at sysconstraints

        cursor.execute(f"""
        SELECT constrtype, constrname, idxname FROM sysconstraints
        WHERE tabid = {source_table_id}
        """)
        constraints = cursor.fetchall()
        for constraint in constraints:
            if constraint[0] in ('C', 'N', 'R'):
                constr_type = constraint[0]
                constr_name = constraint[1]
                index_name = constraint[2]

                if constr_type == 'R':
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.info(f"Processing table: {target_table_name} ({source_table_id}) - foreign key: {constr_name}")
                    # Get foreign key details
                    find_fk_query = f"""
                    SELECT
                        trim(t.owner), t.tabname AS table_name, c.constrname AS constraint_name, col.colname,
                        trim(rt.owner), rt.tabname AS referenced_table_name, r.delrule, pc.constrname as primary_key_name, rcol.colname as referenced_column,
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
                    WHERE c.constrtype = 'R' AND c.tabid = {source_table_id} AND c.constrname = '{constr_name}'
                    """
                    # print(f"find_fk_query: {find_fk_query}")
                    cursor.execute(find_fk_query)
                    fk_details = cursor.fetchone()
                    if self.config_parser.get_log_level() == 'DEBUG':
                            self.logger.debug(f"fk_details: {fk_details}")

                    if cursor.rowcount > 1:
                        raise ValueError(f"ERROR: Multiple foreign key details found for table {target_table_name} and index {index_name}/{constr_name}")

                    # main_schema = fk_details[0]
                    # main_table_name = fk_details[1]
                    fk_name = fk_details[2]
                    fk_column = fk_details[3]
                    # ref_schema = fk_details[4]
                    ref_table_name = fk_details[5]
                    ref_column = fk_details[8]
                    constr_id = fk_details[9]

                    create_constr_query = f"""ALTER TABLE "{target_schema}"."{target_table_name}" ADD CONSTRAINT "{fk_name}" FOREIGN KEY ("{fk_column}") REFERENCES "{target_schema}"."{ref_table_name}" ("{ref_column}");"""

                elif constr_type == 'C':
                    find_ck_ids_query = f"""
                    SELECT distinct c.constrid
                    FROM sysconstraints c
                    JOIN syschecks ck ON c.constrid = ck.constrid
                    WHERE c.tabid = {source_table_id}
                    AND c.constrtype = 'C'
                    AND ck.type in ('T', 's')
                    ORDER BY c.constrid
                    """
                    cursor.execute(find_ck_ids_query)
                    ck_ids = cursor.fetchall()
                    for ck_id_row in ck_ids:
                        constr_id = ck_id_row[0]

                        find_ck_query = f"""
                            SELECT ck.checktext
                            FROM sysconstraints c
                            JOIN syschecks ck ON c.constrid = ck.constrid
                            WHERE c.tabid = {source_table_id}
                            AND c.constrid = {constr_id}
                            AND c.constrtype = 'C'
                            AND ck.type in ('T', 's')
                            ORDER BY seqno
                        """
                        cursor.execute(find_ck_query)
                        ck_details = cursor.fetchall()
                        ck_sql = ''.join([f"{ck[0].strip()}" for ck in ck_details])
                        ck_name = f"ck_{target_table_name}_{constr_name}"
                        create_constr_query = f"""ALTER TABLE "{target_schema}"."{target_table_name}" ADD CONSTRAINT "{ck_name}" CHECK ({ck_sql});"""


                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"constraint: ({constr_id}) {constr_name}, type: {constr_type}, sql: {create_constr_query}")

                if create_constr_query:
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"SQL: {create_constr_query}")
                    table_constraints[order_num] = {
                        'id': constr_id,
                        'name': constr_name,
                        'type': 'FOREIGN KEY' if constr_type == 'R' else 'CHECK' if constr_type == 'C' else constr_type,
                        'sql': create_constr_query,
                        'comment': ''
                    }
                    order_num += 1

        cursor.close()
        self.disconnect()
        return table_constraints

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
        # if self.config_parser.get_log_level() == 'DEBUG':
        #     self.logger.debug(f"Fetching function/procedure names for schema {schema}")
        #     self.logger.debug(f"Query: {query}")
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

    def convert_funcproc_code(self, funcproc_code: str, target_db_type: str, source_schema: str, target_schema: str, table_list: list):

        def indent_code(code):
            lines = code.split('\n')
            indent_level = 0
            indented_lines = []
            for line in lines:
                stripped_line = line.strip()
                if (stripped_line.upper().startswith('END')
                    or stripped_line.upper().startswith('ELSE')
                    or stripped_line.upper().startswith('ELSIF')
                    or stripped_line.upper().startswith('EXCEPTION')
                    or stripped_line.upper().startswith('BEGIN')):
                    indent_level -= 1
                    if indent_level < 0:
                        indent_level = 0
                indented_lines.append(f"{self.config_parser.get_indent() * indent_level}{stripped_line}")
                if (stripped_line.upper().endswith('LOOP')
                    or stripped_line.upper().startswith('BEGIN')
                    or stripped_line.upper().startswith('IF')
                    or stripped_line.upper().startswith('ELSIF')
                    or stripped_line.upper().startswith('EXCEPTION')
                    or stripped_line.upper().startswith('DECLARE')):
                    indent_level += 1
            return '\n'.join(indented_lines)

        if target_db_type == 'postgresql':
            postgresql_code = funcproc_code

            # Replace empty lines with ";"
            postgresql_code = re.sub(r'^\s*$', ';\n', postgresql_code, flags=re.MULTILINE)
            # Split the code based on "\n
            commands = [command.strip() for command in postgresql_code.split('\n') if command.strip()]
            postgresql_code = ''
            line_number = 0
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

                postgresql_code += ' ' + command + ' '
                line_number += 1

            # Split the code based on ";"
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

            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f'[1] postgresql_code: {postgresql_code}')

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
            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f'[00] def_lines: {def_lines}')

            if def_lines:
                last_def_line = def_lines[-1].strip()
                # print(f'last_def_line: {last_def_line}')
                postgresql_code = postgresql_code.replace(last_def_line, last_def_line + '\nBEGIN;', 1)
                # if self.config_parser.get_log_level() == 'DEBUG':
                #     self.logger.debug(f'[000] postgresql_code: {postgresql_code}')

                # Replace lvarchar definitions with text data type
                postgresql_code = re.sub(r'\blvarchar\(\d+\)', 'text', postgresql_code, flags=re.IGNORECASE)
                postgresql_code = re.sub(r'\blvarchar', 'text', postgresql_code, flags=re.IGNORECASE)
                postgresql_code = re.sub(r'\bvarchar\(\d+\)', 'text', postgresql_code, flags=re.IGNORECASE)
                postgresql_code = re.sub(r'\bDATETIME YEAR TO DAY', 'TIMESTAMP', postgresql_code, flags=re.IGNORECASE)
                # print(f'postgresql_code: {postgresql_code}')

                postgresql_code = re.sub(r'^\s*DEFINE\s+', '\nDECLARE\n', postgresql_code, count=1, flags=re.MULTILINE | re.IGNORECASE)
                postgresql_code = re.sub(r'^\s*DEFINE\s+', '', postgresql_code, flags=re.MULTILINE | re.IGNORECASE)

            # Replace variable declarations with %TYPE where LIKE is used
            # declarations with LIKE can be also in the header
            # postgresql_code = re.sub(r'\s+(\w+)\s+LIKE\s+([\w\d_]+)\.(\w+);', r'\n\1 \2.\3%TYPE;', postgresql_code, flags=re.IGNORECASE)
            # Replace variable declarations with %TYPE where LIKE is used
            postgresql_code = re.sub(r'\s+(\w+)\s+LIKE\s+([\w\d_]+)\.(\w+);', r'\n\1 \2.\3%TYPE;', postgresql_code, flags=re.IGNORECASE)
            postgresql_code = re.sub(r'\(([^)]+)\)', lambda match: re.sub(r'(\w+)\s+LIKE\s+([\w\d_]+)\.(\w+)', r'\1 \2.\3%TYPE', match.group(0)), postgresql_code, flags=re.IGNORECASE)
            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f'[0000] postgresql_code: {postgresql_code}')

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
                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f'code contains WITH HOLD but no COMMIT or ROLLBACK')

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

            # header for procedures
            header_match = re.search(r'CREATE PROCEDURE.*?\);', postgresql_code, flags=re.DOTALL | re.IGNORECASE)
            if header_match:
                header_end = header_match.end()-1
                # if self.config_parser.get_log_level() == 'DEBUG':
                #     self.logger.debug(f"[1] header_end: {header_end}, postgresql_code: {postgresql_code[:header_end]}")
                postgresql_code = postgresql_code[:header_end] + ' AS $$\n' + postgresql_code[header_end:]
                # if self.config_parser.get_log_level() == 'DEBUG':
                #     self.logger.debug(f"[1] postgresql_code: {postgresql_code[:header_end+10]}")
            else:
                header_match = re.search(r'CREATE PROCEDURE.*?\(\)\s*', postgresql_code, flags=re.DOTALL | re.IGNORECASE)
                if header_match:
                    header_end = header_match.end()
                    # if self.config_parser.get_log_level() == 'DEBUG':
                    #     self.logger.debug(f"[1b] header_end: {header_end}, postgresql_code: {postgresql_code[:header_end]}")
                    postgresql_code = postgresql_code[:header_end] + '\n AS $$\n' + postgresql_code[header_end:]
                    # if self.config_parser.get_log_level() == 'DEBUG':
                    #     self.logger.debug(f"[1b] postgresql_code: {postgresql_code[:header_end+10]}")

            # header for functions
            header_match = re.search(r'CREATE FUNCTION.*?RETURNING\s+\w+\s+;?', postgresql_code, flags=re.DOTALL | re.IGNORECASE)
            if header_match:
                header_end = header_match.end()
                # if self.config_parser.get_log_level() == 'DEBUG':
                #     self.logger.debug(f"[2] header_end: {header_end}, postgresql_code: {postgresql_code[:header_end]}")
                postgresql_code_part = re.sub(r'RETURNING', 'RETURNS', postgresql_code[:header_end], flags=re.DOTALL | re.IGNORECASE)
                if ';' in postgresql_code_part:
                    postgresql_code_part = re.sub(r';', ' AS $$\n', postgresql_code_part, flags=re.DOTALL | re.IGNORECASE)
                else:
                    postgresql_code_part += ' AS $$\n'
                postgresql_code = postgresql_code_part + postgresql_code[header_end:]
                # if self.config_parser.get_log_level() == 'DEBUG':
                #     self.logger.debug(f"[2] postgresql_code: {postgresql_code[:header_end+10]}")

            header_match = re.search(r'\s*RETURNING\s+\w+\s+;?', postgresql_code, flags=re.DOTALL | re.IGNORECASE)
            if header_match:
                header_end = header_match.end()
                # if self.config_parser.get_log_level() == 'DEBUG':
                #     self.logger.debug(f"[3] header_end: {header_end}, postgresql_code: {postgresql_code[:header_end]}")
                postgresql_code_part = re.sub(r'RETURNING', 'RETURNS', postgresql_code[:header_end], flags=re.DOTALL | re.IGNORECASE)
                if ';' in postgresql_code_part:
                    postgresql_code_part = re.sub(r';', ' AS $$\n', postgresql_code_part, flags=re.DOTALL | re.IGNORECASE)
                else:
                    postgresql_code_part += ' AS $$\n'
                postgresql_code = postgresql_code_part + postgresql_code[header_end:]
                # if self.config_parser.get_log_level() == 'DEBUG':
                #     self.logger.debug(f"[3] postgresql_code: {postgresql_code[:header_end+10]}")

            # Simplify LET commands
            postgresql_code = re.sub(r'(?i)^\s*LET\s+', '', postgresql_code, flags=re.MULTILINE)

            # Add BEGIN after "AS $$" if there is no DECLARE command
            if "DECLARE" not in postgresql_code:
                postgresql_code = re.sub(r'AS\s+\$\$', 'AS $$\nBEGIN', postgresql_code, flags=re.IGNORECASE)

            # Replace Informix specific syntax with PostgreSQL syntax
            returning_matches = re.finditer(r'RETURNING\s+(\w+)\s*;', postgresql_code, flags=re.DOTALL | re.IGNORECASE)
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
            postgresql_code = re.sub(r';;', ';', postgresql_code, flags=re.IGNORECASE)

            for table in table_list:
                source_table_pattern = re.compile(rf'("{source_schema}"\.)?"{table}"')
                target_table = f'"{target_schema}"."{table}"'
                postgresql_code = source_table_pattern.sub(target_table, postgresql_code)

                source_table_pattern = re.compile(rf'\b{table}\b')
                postgresql_code = source_table_pattern.sub(target_table, postgresql_code)

            # Remove second occurrence of "target_schema" in %TYPE declarations
            postgresql_code = re.sub(
                                    rf'("{target_schema}"\."\w+"\.)"{target_schema}"\.("\w+"%TYPE)',
                                    rf'\1\2', postgresql_code,
                                    flags=re.MULTILINE | re.IGNORECASE)

            # Add function return type and language
            postgresql_code += '\n$$ LANGUAGE plpgsql;'

            # Remove lines which contain only ";"
            postgresql_code = "\n".join([line for line in postgresql_code.split('\n') if line.strip() != ";"])
            # Remove empty lines from the converted code
            postgresql_code = "\n".join([line for line in postgresql_code.splitlines() if line.strip()])

            # some procs /funcs have ON EXCEPTION block, some of them several times
            if "ON EXCEPTION" in postgresql_code:
                exception_lines = [line for line in postgresql_code.split('\n') if 'ON EXCEPTION' in line]
                commentedout_exception_occurences = 0
                for line in exception_lines:
                    line = line.strip()
                    if line.startswith("/*"):
                        commentedout_exception_occurences += 1

                live_exception_occurences = len(exception_lines) - commentedout_exception_occurences
                if live_exception_occurences > 0:

                    for i in range(live_exception_occurences):
                        #### handle ON EXCEPTION block in scope of the main BEGIN - END block
                        # Split the postgresql_code by lines
                        lines = postgresql_code.split('\n')

                        # Find the first occurrence of BEGIN
                        begin_index = next((i for i, line in enumerate(lines) if 'BEGIN' in line), None)
                        if self.config_parser.get_log_level() == 'DEBUG':
                            self.logger.debug(f'ON EXCEPTION - begin_index: {begin_index}')

                        if begin_index is not None:
                            # Find the ON EXCEPTION - END EXCEPTION block that follows the first BEGIN
                            exception_start_index = next((i for i, line in enumerate(lines[begin_index:], start=begin_index) if 'ON EXCEPTION' in line), None)
                            exception_end_index = next((i for i, line in enumerate(lines[begin_index:], start=begin_index) if 'END EXCEPTION;' in line), None)
                            if self.config_parser.get_log_level() == 'DEBUG':
                                self.logger.debug(f'ON EXCEPTION - exception_start_index: {exception_start_index}')
                            if self.config_parser.get_log_level() == 'DEBUG':
                                self.logger.debug(f'ON EXCEPTION - exception_end_index: {exception_end_index}')

                            # Ensure that exception_start_index is immediately after begin_index
                            if exception_start_index is not None and exception_start_index != begin_index + 1:
                                if self.config_parser.get_log_level() == 'DEBUG':
                                    self.logger.debug('ON EXCEPTION does not immediately follow BEGIN, trying LOOP occurence')

                                ## try LOOP - END LOOP occurence
                                # loop_begin_index = next((i for i, line in enumerate(lines) if 'LOOP' in line), None)
                                loop_begin_index = next((i for i, line in enumerate(lines) if 'LOOP' in line and i + 1 < len(lines) and 'ON EXCEPTION' in lines[i + 1]), None)

                                if self.config_parser.get_log_level() == 'DEBUG':
                                    self.logger.debug(f'loop_begin_index: {loop_begin_index}')

                                if loop_begin_index is not None:
                                    # Find the ON EXCEPTION - END EXCEPTION block that follows the first BEGIN
                                    exception_start_index = next((i for i, line in enumerate(lines[loop_begin_index:], start=loop_begin_index) if 'ON EXCEPTION' in line), None)
                                    exception_end_index = next((i for i, line in enumerate(lines[loop_begin_index:], start=loop_begin_index) if 'END EXCEPTION' in line), None)
                                    if self.config_parser.get_log_level() == 'DEBUG':
                                        self.logger.debug(f'loop: exception_start_index: {exception_start_index}')
                                    if self.config_parser.get_log_level() == 'DEBUG':
                                        self.logger.debug(f'loop: exception_end_index: {exception_end_index}')

                                    # Ensure that exception_start_index is immediately after loop_begin_index
                                    if exception_start_index is not None and exception_start_index != loop_begin_index + 1:
                                        if self.config_parser.get_log_level() == 'DEBUG':
                                            self.logger.debug('ON EXCEPTION does not immediately follow LOOP command')

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
                                        if on_exception_line:
                                            # Extract the variable name from the ON EXCEPTION line
                                            variable_name = re.search(r'ON EXCEPTION SET (\w+);', on_exception_line).group(1)
                                            set_variable_line = f"""{variable_name} = SQLSTATE||'-'||SQLERRM;"""
                                            # match = re.search(r'ON EXCEPTION SET (.*?);', on_exception_line)
                                            # variable_names = match.group(1).split(',') if match else ['unknown_variable']
                                            # if len(variable_names) == 1:
                                            #     set_variable_line = f"""{variable_names[0]} = SQLSTATE||'-'||SQLERRM;"""
                                            # elif len(variable_names) == 3:
                                            #     set_variable_line = f"""{variable_names[0]} = SQLSTATE;\n{variable_name[1]} = SQLSTATE;\n {variable_name[2]} = SQLERRM;"""
                                            print(f'set_variable_line: {set_variable_line}')

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
                                    variable_name = re.search(r'ON EXCEPTION SET (\w+);', on_exception_line).group(1)
                                    set_variable_line = f"""{variable_name} = SQLSTATE||'-'||SQLERRM;"""
                                    # variable_names = match.group(1).split(',') if match else ['unknown_variable']
                                    # if len(variable_names) == 1:
                                    #     set_variable_line = f"""{variable_names[0]} = SQLSTATE||'-'||SQLERRM;"""
                                    # elif len(variable_names) == 3:
                                    #     set_variable_line = f"""{variable_names[0]} = SQLSTATE;\n{variable_name[1]} = SQLSTATE;\n {variable_name[2]} = SQLERRM;"""
                                    print(f'set_variable_line: {set_variable_line}')

                                # Modify the exception block
                                modified_exception_block = [re.sub(r'ON EXCEPTION SET \w+', f'EXCEPTION WHEN OTHERS THEN\n{set_variable_line}', line) for line in exception_block]
                                modified_exception_block = [re.sub(r'ON EXCEPTION;?', f'EXCEPTION WHEN OTHERS THEN', line) for line in modified_exception_block]
                                modified_exception_block = [line for line in modified_exception_block if 'END EXCEPTION' not in line]
                                # if self.config_parser.get_log_level() == 'DEBUG':
                                #     self.logger.debug(f'modified_exception_block: {modified_exception_block}')
                                #     self.logger.debug(f'lines: {lines}')

                                end_index = next((i for i, line in enumerate(lines) if 'END;' in line and '$$ LANGUAGE plpgsql;' in lines[i + 1]), None)
                                if self.config_parser.get_log_level() == 'DEBUG':
                                    self.logger.debug(f'end_index: {end_index}')
                                if end_index is not None:
                                    lines = lines[:end_index] + modified_exception_block + lines[end_index:]

                                # Join the lines back into a single string
                                postgresql_code = '\n'.join(lines)

            postgresql_code = re.sub(r';;', ';', postgresql_code, flags=re.IGNORECASE)
            # Indent the code
            postgresql_code = indent_code(postgresql_code)
            # Remove empty lines from the converted code
            postgresql_code = "\n".join([line for line in postgresql_code.splitlines() if line.strip()])

            return postgresql_code

        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

    def fetch_sequences(self, table_schema: str, table_name: str):
        pass

    def migrate_table(self, migrate_target_connection, settings):
        part_name = 'initialize'
        try:
            worker_id = settings['worker_id']
            source_schema = settings['source_schema']
            source_table = settings['source_table']
            source_table_id = settings['source_table_id']
            source_columns = settings['source_columns']
            target_schema = settings['target_schema']
            target_table = settings['target_table']
            target_columns = settings['target_columns']
            primary_key_columns = settings['primary_key_columns']
            batch_size = settings['batch_size']
            migrator_tables = settings['migrator_tables']
            source_table_rows = self.get_rows_count(source_schema, source_table)

            if source_table_rows == 0:
                self.logger.info(f"Worker {worker_id}: Table {source_table} is empty - skipping data migration.")
                migrator_tables.insert_data_migration(source_schema, source_table, source_table_id, source_table_rows, worker_id, target_schema, target_table, 0)
                return 0
            else:
                self.logger.info(f"Worker {worker_id}: Table {source_table} has {source_table_rows} rows - starting data migration.")
                protocol_id = migrator_tables.insert_data_migration(source_schema, source_table, source_table_id, source_table_rows, worker_id, target_schema, target_table, 0)
                offset = 0
                total_inserted_rows = 0
                informix_cursor = self.connection.cursor()
                # Fetch the data in batches
                while True:
                    part_name = f'prepare fetch data: {source_table} - {offset}'
                    if primary_key_columns:
                        query = f"SELECT SKIP {offset} * FROM {source_schema}.{source_table} ORDER BY {primary_key_columns} LIMIT {batch_size}"
                    else:
                        query = f"SELECT SKIP {offset} * FROM {source_schema}.{source_table} LIMIT {batch_size}"

                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Worker {worker_id}: Fetching data with query: {query}")

                    part_name = f'do fetch data: {source_table} - {offset}'
                    # # polars library is not always available
                    # df = pl.read_database(query, self.connection)
                    # if df.is_empty():
                    #     break
                    # self.logger.info(f"Worker {worker_id}: Fetched {len(df)} rows from source table {source_table}.")
                    # # self.logger.info(f"Worker {worker_id}: Migrating batch starting at offset {offset} for table {table_name}.")
                    # # Convert Polars DataFrame to list of tuples for insertion
                    # records = df.to_dicts()

                    informix_cursor.execute(query)
                    records = informix_cursor.fetchall()
                    if not records:
                        break
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Worker {worker_id}: Fetched {len(records)} rows from source table {source_table}.")

                    records = [
                        {column['name']: value for column, value in zip(source_columns.values(), record)}
                        for record in records
                    ]

                    # Adjust binary or bytea types
                    for record in records:
                        for order_num, column in source_columns.items():
                            column_name = column['name']
                            column_type = column['type']
                            target_column_type = target_columns[order_num]['type']
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

                    part_name = f'insert data: {target_table} - {offset}'
                    inserted_rows = migrate_target_connection.insert_batch(target_schema, target_table, target_columns, records)
                    total_inserted_rows += inserted_rows
                    self.logger.info(f"Worker {worker_id}: Inserted {inserted_rows} (total: {total_inserted_rows} from: {source_table_rows} ({round(total_inserted_rows/source_table_rows*100, 2)}%)) rows into target table {target_table}")

                    offset += batch_size

                target_table_rows = migrate_target_connection.get_rows_count(target_schema, target_table)
                self.logger.info(f"Worker {worker_id}: Finished migrating data for table {source_table}.")
                migrator_tables.update_data_migration_status(protocol_id, True, 'OK', target_table_rows)
                return source_table_rows
        except Exception as e:
            self.logger.error(f"Worker {worker_id}: Error during {part_name} -> {e}")
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
            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"fetch_triggers query: {query}")
            for row in cursor.fetchall():
                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"fetch_triggers row: {row}")
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

                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"trigger SQL: {trigger_code_str}")

                triggers[order_num]['sql'] = trigger_code_str
                triggers[order_num]['row_statement'] = 'FOR EACH ROW' if 'FOR EACH ROW' in trigger_code_str.upper() else ''
                order_num += 1
            cursor.close()
            self.disconnect()
            return triggers
        except Exception as e:
            self.logger.error(f"Error when fetching triggers for the table {table_name}/{table_id}: {e}")
            raise

    def convert_trigger(self, trig: str, settings: dict):
        pgsql_trigger_code = ''
        pgsql_triggers = []
        trigger_code = ''
        func_code = ''

        # Split the input into individual trigger definitions
        triggers = re.split(r'(?i)create trigger', trig)
        for trig in triggers:
            trig = trig.strip()
            if not trig:
                continue

            # Remove all comments (lines starting with /* and ending with */)
            trig = re.sub(r'/\*.*?\*/', '', trig, flags=re.DOTALL)
            # Remove all new line characters from the original trigger code
            trig = trig.replace('\n', ' ')
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

            # Extract schema, trigger name, and operation (insert/update)
            header_match = re.match(r'"([^"]+)"\.(\S+)\s+(insert|update|delete)', trig, re.IGNORECASE)
            if not header_match:
                continue
            schema = header_match.group(1)
            trigger_name = header_match.group(2)
            operation = header_match.group(3).lower()

            # Extract the table name (assumes: on "schemaname".table)
            table_match = re.search(r'\s+on\s+"([^"]+)"\.(\S+)', trig, re.IGNORECASE)
            if table_match:
                table_schema = table_match.group(1)
                table_name = table_match.group(2)
            else:
                table_schema = schema
                table_name = "unknown_table"

            func_body_lines = []

            when_conditions = []
            proc_calls = []
            # when_matches = re.findall(r'(?:when\s*\((.*?)\)\s*)?\(\s*(execute procedure.*?;?)\s*\)', trig, re.IGNORECASE | re.DOTALL | re.MULTILINE)
            when_matches = re.findall(r'(?:when\s*\((.*?)\)\s*)?\(\s*(execute procedure.*?\(.*?\));?\s*\)', trig, re.IGNORECASE | re.DOTALL | re.MULTILINE)
            # when_matches = re.findall(r'(?:when\s*\((?:\((?:\((.*?)\))?\))?\)\s*)?\(\s*(execute procedure.*?\(.*?\));?\s*\)', trig, re.IGNORECASE | re.DOTALL | re.MULTILINE)

            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"when_matches: {when_matches}")

            for match in when_matches:
                when_condition = match[0]
                proc_call = match[1]
                when_conditions.append(when_condition)
                proc_calls.append(proc_call)

            if not proc_calls:
                # Find everything after the words FOR EACH ROW and take it as actions
                actions_match = re.search(r'for each row\s*\((.*)\)', trig, re.IGNORECASE | re.DOTALL)
                if actions_match:
                    actions = actions_match.group(1).split(',')
                    for action in actions:
                        print(f"action: {action.strip()}")
                        if "execute procedure" in action:
                            # action = re.sub("execute procedure", "", action, flags=re.IGNORECASE) ## keep it for further processing
                            action = re.sub("with trigger references", "", action, flags=re.IGNORECASE)
                            action = action.replace(settings['source_schema'], settings['target_schema'])
                        proc_calls.append(action.strip())

            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"when_conditions: {when_conditions}")
                self.logger.debug(f"proc_calls: {proc_calls}")

            function_name = trigger_name + "_trigfunc"
            counter = 0
            if when_conditions and proc_calls:

                for i in range(len(when_conditions)):
                    proc_call = proc_calls[i].replace("execute procedure", "PERFORM")

                    if when_conditions[i]:
                        func_body_lines.append(f"    IF {when_conditions[i]} THEN")
                        func_body_lines.append(f"        {proc_call.replace(settings['source_schema'], settings['target_schema'])};")
                        func_body_lines.append("    END IF;")

            # Build the trigger function code
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
                for i in range(len(proc_calls)):
                    trigger_code = ''
                    func_code = ''
                    proc_call = proc_calls[i]
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"proc_call: {proc_call}")

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
                                {proc_call.replace(settings['source_schema'], settings['target_schema'])};
                                RETURN NEW;
                            END;
                            $$ LANGUAGE plpgsql;"""
                        trigger_code += f"\nEXECUTE FUNCTION {settings['target_schema']}.{function_name + str(counter)}();"
                        counter += 1

                    pgsql_triggers.append(func_code + "\n\n" + trigger_code)

        pgsql_trigger_code = "\n\n".join(pgsql_triggers)

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
        self.logger.error(f"An error in {self.__class__.__name__} ({description}): {e}")
        self.logger.error(traceback.format_exc())
        if self.on_error_action == 'stop':
            self.logger.error("Stopping due to error.")
            exit(1)
        else:
            pass

    def get_rows_count(self, table_schema: str, table_name: str):
        query = f"""SELECT COUNT(*) FROM "{table_schema}".{table_name} """
        if self.config_parser.get_log_level() == 'DEBUG':
            self.logger.debug(f"get_rows_count query: {query}")
        cursor = self.connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_sequence_current_value(self, sequence_id: int):
        pass

    def fetch_user_defined_types(self, schema: str):
        pass