import jaydebeapi
from jaydebeapi import Error
import pyodbc
from pyodbc import Error
from database_connector import DatabaseConnector
from migrator_logging import MigratorLogger
import re
import traceback

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
        except AttributeError:
            pass

    def fetch_table_names(self, table_schema: str):
        query = f"""
            SELECT
            o.id as table_id,
            o.name as table_name
            FROM sysobjects o
            WHERE user_name(o.uid) = '{table_schema}' AND o.type = 'U'
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
                    'table_name': row[1]
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return tables
        except Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_table_columns(self, table_schema: str, table_name: str, migrator_tables) -> dict:
        result = {}
        query = f"""
            SELECT
                c.colid as ordinal_position,
                c.name as column_name,
                t.name as data_type,
                CASE
                    WHEN t.name in ('univarchar', 'unichar', 'varchar', 'char')
                        THEN convert('varchar', c.length )
                    WHEN t.name in ('numeric', 'double precision', 'decimal')
                        THEN convert('varchar', c.prec) + ',' + convert('varchar', c.scale)
                    WHEN t.name in ('float', 'binary') THEN convert('varchar', c.length)
                    ELSE ''
                END as data_type_length,
                c.length as length,
                CASE
                    WHEN c.status&8=8 and t.name <> 'bit' THEN 1
                ELSE 0 END AS column_nullable,
                CASE
                    WHEN c.status&128=128 and t.name <> 'bit' THEN 1
                ELSE 0 END AS identity_column,
                t.name +
                CASE
                    WHEN t.name in ('univarchar', 'unichar', 'varchar', 'char')
                        THEN '(' + convert('varchar', c.length ) + ')'
                    WHEN t.name in ('numeric', 'double precision', 'decimal')
                        THEN '(' + convert('varchar', c.prec) + ',' + convert('varchar', c.scale) + ')'
                    WHEN t.name in ('float', 'binary')
                        THEN '(' + convert('varchar', c.length) + ')'
                    ELSE ''
                END as full_data_type_length,
                object_name(c.domain) as column_domain,
                object_name(c.cdefault) as column_default_name,
                ltrim(rtrim(str_replace(co.text, char(10),''))) as column_default_value,
                c.status,
                t.variable as variable_length,
                c.prec as data_type_precision,
                c.scale as data_type_scale,
                t.allownulls as type_nullable,
                t.ident as type_has_identity_property
            FROM syscolumns c
            JOIN sysobjects tab ON c.id = tab.id
            JOIN systypes t ON c.usertype = t.usertype
            LEFT JOIN syscomments co ON c.cdefault = co.id
            WHERE user_name(tab.uid) = '{table_schema}'
                AND tab.name = '{table_name}'
                AND tab.type = 'U'
            ORDER BY c.colid
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Reading columns for {table_name}")
            cursor.execute(query)
            for row in cursor.fetchall():
                result[row[0]] = {
                    'name': row[1].strip(),
                    'type': row[2].strip(),
                    'length': row[3].strip(),
                    'nullable': 'NOT NULL' if row[5] == 0 else '',
                    'default': row[10].replace('DEFAULT', '').strip().strip('"')
                        if row[10] and row[10].replace('DEFAULT', '').strip().startswith('"') and row[10].replace('DEFAULT', '').strip().endswith('"')
                        else (row[10].replace('DEFAULT', '').strip() if row[10] else ''),
                    'other': 'IDENTITY' if row[6] == 1 else ''
                }

                # if self.config_parser.get_log_level() == 'DEBUG':
                #     self.logger.debug(f"0 default: {result[row[0]]['default']}")
                # checking for default values substitution with the origingal data type
                if result[row[0]]['default'] != '':
                    result[row[0]]['default'] = migrator_tables.check_default_values_substitution(result[row[0]]['name'], result[row[0]]['type'], result[row[0]]['default'])

                query_custom_types = f"""
                    SELECT
                        ut.name AS user_data_type_name,
                        bt.name AS source_data_type,
                        CASE
                            WHEN bt.name in ('univarchar', 'unichar', 'varchar', 'char')
                                THEN convert('varchar', ut.length )
                            WHEN bt.name in ('numeric', 'double precision', 'decimal')
                                THEN convert('varchar', ut.prec) + ',' + convert('varchar', ut.scale)
                            WHEN bt.name in ('float', 'binary')
                                THEN convert('varchar', ut.length)
                            ELSE ''
                        END as length_precision,
                        ut.ident as type_has_identity_property,
                        ut.allownulls as type_nullable,
                        bt.name + CASE
                        WHEN bt.name in ('univarchar', 'unichar', 'varchar', 'char')
                            THEN '(' + convert('varchar', ut.length ) + ')'
                        WHEN bt.name in ('numeric', 'double precision', 'decimal')
                            THEN '(' + convert('varchar', ut.prec) + ',' + convert('varchar', ut.scale) + ')'
                        WHEN bt.name in ('float', 'binary') THEN '(' + convert('varchar', ut.length) + ')'
                        ELSE ''
                        END as source_data_type_length
                    FROM systypes ut
                    JOIN (SELECT * FROM systypes t JOIN (SELECT type, min(usertype) as usertype FROM systypes GROUP BY type) bt0
                        ON t.type = bt0.type AND t.usertype = bt0.usertype) bt
                        ON ut.type = bt.type AND ut.hierarchy = bt.hierarchy
                    WHERE ut.name <> bt.name AND LOWER(ut.name) not in ('timestamp')
                    AND ut.name = '{row[2]}'
                    ORDER BY ut.name
                """
                cursor.execute(query_custom_types)
                custom_types = cursor.fetchall()
                if custom_types:
                    custom_type = custom_types[0]
                    result[row[0]]['type'] = custom_type[1]
                    result[row[0]]['length'] = custom_type[2]
                    if custom_type[3] == 1:
                        result[row[0]]['other'] = 'IDENTITY'

                # if self.config_parser.get_log_level() == 'DEBUG':
                #     self.logger.debug(f"Substituting data type: {row[7]}, {migrator_tables.check_data_types_substitution(row[7])}")
                #     if custom_types:
                #         self.logger.debug(f"Substituting data type: {custom_type[5]}, {migrator_tables.check_data_types_substitution(custom_type[5])}")
                substitution = migrator_tables.check_data_types_substitution(row[7])
                if substitution and substitution != (None, None):
                    result[row[0]]['type'], result[row[0]]['length'] = migrator_tables.check_data_types_substitution(row[7])
                if custom_types and migrator_tables.check_data_types_substitution(custom_type[5]) != (None, None):
                    result[row[0]]['type'], result[row[0]]['length'] = migrator_tables.check_data_types_substitution(custom_type[5])

                # checking for default values substitution with the new data type
                if result[row[0]]['default'] != '':
                    result[row[0]]['default'] = migrator_tables.check_default_values_substitution(result[row[0]]['name'], result[row[0]]['type'], result[row[0]]['default'])

                # if self.config_parser.get_log_level() == 'DEBUG':
                #     self.logger.debug(f"1 default: {result[row[0]]['default']}")

            cursor.close()
            self.disconnect()
            return result
        except Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def convert_table_columns(self, target_db_type: str, table_schema: str, table_name: str, source_columns: dict):
        type_mapping = {}
        create_table_sql = ""
        converted = {}
        if target_db_type == 'postgresql':
            type_mapping = {
                'BIGDATETIME': 'TIMESTAMP',
                'DATE': 'DATE',
                'DATETIME': 'TIMESTAMP',
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
                'NVARCHAR': 'TEXT',
                'TEXT': 'TEXT',
                'SYSNAME': 'TEXT',
                'LONGSYSNAME': 'TEXT',
                'LONG VARCHAR': 'TEXT',
                'LONG NVARCHAR': 'TEXT',
                'UNICHAR': 'CHAR',
                'UNITEXT': 'TEXT',
                'UNIVARCHAR': 'TEXT',
                'VARCHAR': 'TEXT',

                'CLOB': 'TEXT',
                'DECIMAL': 'DECIMAL',
                'DOUBLE PRECISION': 'DOUBLE PRECISION',
                'FLOAT': 'FLOAT',
                'INTERVAL': 'INTERVAL',
                'MONEY': 'MONEY',
                'NUMERIC': 'NUMERIC',
                'REAL': 'REAL',
                'SERIAL8': 'BIGSERIAL',
                'SERIAL': 'SERIAL',
                'SMALLFLOAT': 'REAL',
            }

            for order_num, column_info in source_columns.items():
                coltype = column_info['type'].upper()
                length = column_info['length']
                if type_mapping.get(coltype, 'UNKNOWN').startswith('UNKNOWN'):
                    self.logger.info(f"Column {column_info['name']} - unknown data type: {column_info['type']}")
                    # coltype = 'TEXT' ## default to TEXT may not be the best option -> let the table creation fail
                else:
                    coltype = type_mapping.get(coltype, 'TEXT')
                if coltype == 'VARCHAR' and int(column_info['length']) >= 254:
                    coltype = 'TEXT'
                    length = ''

                converted[order_num] = {
                    'name': column_info['name'],
                    'type': coltype,
                    'length': length,
                    'default': column_info['default'],
                    'nullable': column_info['nullable'],
                    'other': column_info['other']
                }

            create_table_sql_parts = []
            for _, info in converted.items():
                if 'length' in info and info['type'] in ('CHAR', 'VARCHAR'):
                    create_table_sql_parts.append(f""""{info['name']}" {info['type']}({info['length']}) {info['nullable']}""")
                else:
                    create_table_sql_parts.append(f""""{info['name']}" {info['type']} {info['nullable']}""")
                if info['default']:
                    if info['type'] in ('CHAR', 'VARCHAR', 'TEXT'):
                        create_table_sql_parts[-1] += f""" DEFAULT '{info['default']}'""".replace("''", "'")
                    else:
                        create_table_sql_parts[-1] += f" DEFAULT {info['default']}"
            create_table_sql = ", ".join(create_table_sql_parts)
            create_table_sql = f"""CREATE TABLE "{table_schema}"."{table_name}" ({create_table_sql})"""

        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

        return converted, create_table_sql

    def fetch_indexes(self, source_table_id: int, target_schema, target_table_name):
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
                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Processing index: {index}")
                index_name = index[0]
                index_unique = index[1]
                index_columns = index[2].strip()
                index_primary_key = index[3]

                create_index_query = None
                if index_primary_key == 1:
                    create_index_query = f"""ALTER TABLE "{target_schema}"."{target_table_name}" ADD CONSTRAINT "{index_name}" PRIMARY KEY ({index_columns});"""
                elif index_unique == 1 and index_primary_key == 0:
                    create_index_query = f"""CREATE UNIQUE INDEX "{index_name}" ON "{target_schema}"."{target_table_name}" ({index_columns});"""
                else:
                    create_index_query = f"""CREATE INDEX "{index_name}" ON "{target_schema}"."{target_table_name}" ({index_columns});"""

                if create_index_query:
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"SQL: {create_index_query}")
                    table_indexes[order_num] = {
                        'name': index_name,
                        'type': "PRIMARY KEY" if index_primary_key == 1 else "UNIQUE" if index_unique == 1 and index_primary_key == 0 else "INDEX",
                        'columns': index_columns,
                        'sql': create_index_query
                    }
                    order_num += 1

            cursor.close()
            self.disconnect()
            return table_indexes

        except Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_constraints(self, source_table_id: int, target_schema, target_table_name):
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
        ORDER BY constraint_name
        """

        self.connect()
        cursor = self.connection.cursor()
        if self.config_parser.get_log_level() == 'DEBUG':
            self.logger.debug(f"Reading constraints for {target_table_name}")
        cursor.execute(index_query)
        constraints = cursor.fetchall()

        for constraint in constraints:
            fk_name = constraint[0]
            fk_column = constraint[1].strip()
            ref_table_name = constraint[2]
            ref_column = constraint[3].strip()

            create_fk_query = f"""ALTER TABLE "{target_schema}"."{target_table_name}" ADD CONSTRAINT "{fk_name}" FOREIGN KEY ({fk_column}) REFERENCES "{target_schema}"."{ref_table_name}" ({ref_column});"""

            if create_fk_query:
                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"SQL: {create_fk_query}")
                table_constraints[order_num] = {
                    'name': fk_name,
                    'type': 'FOREIGN KEY',
                    'sql': create_fk_query
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
                DISTINCT
                o.name,
                o.id,
                CASE
                    WHEN o.type = 'P' THEN 'Procedure'
                    WHEN o.type = 'F' THEN 'Function'
                    WHEN o.type = 'TR' THEN 'Trigger'
                    WHEN o.type = 'XP' THEN 'Extended Procedure'
                END AS type,
                o.sysstat, o.sysstat2
            FROM syscomments c, sysobjects o
            WHERE o.id=c.id
                AND user_name(o.uid) = '{schema}'
                AND type in ('F', 'P', 'TR', 'XP')
                AND (o.sysstat & 4 = 4 or o.sysstat & 8 = 8 or o.sysstat & 12 = 12)
            ORDER BY o.name
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
                'type': row[2]
            }
            order_num += 1
        cursor.close()
        self.disconnect()
        return funcproc_data

    def fetch_funcproc_code(self, funcproc_id: int):
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

    def convert_funcproc_code(self, funcproc_code: str, target_db_type: str, source_schema: str, target_schema: str, table_list: list):
        return None

    def fetch_sequences(self, table_schema: str, table_name: str):
        pass

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
        self.logger.error(f"An error in {self.__class__.__name__} ({description}): {e}")
        self.logger.error(traceback.format_exc())
        if self.on_error_action == 'stop':
            self.logger.error("Stopping due to error.")
            exit(1)
        else:
            pass

    def get_rows_count(self, table_schema: str, table_name: str):
        query = f"""SELECT COUNT(*) FROM {table_schema}.{table_name} """
        if self.config_parser.get_log_level() == 'DEBUG':
            self.logger.debug(f"get_rows_count query: {query}")
        cursor = self.connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def analyze_id_distribution_batch(self, schema_name, table_name, id_column, analyze_batch_percent, analyze_batch_size, debug=False):
        cur = self.connection.cursor()

        cur.execute(sql.SQL("CREATE TEMP TABLE IF NOT EXISTS temp_id_ranges (batch_start BIGINT, batch_end BIGINT, row_count BIGINT)"))

        cur.execute(sql.SQL("SELECT MIN({}) FROM {}.{}").format(
            sql.Identifier(id_column),
            sql.Identifier(schema_name),
            sql.Identifier(table_name)
        ))
        min_id = cur.fetchone()[0]

        cur.execute(sql.SQL("SELECT MAX({}) FROM {}.{}").format(
            sql.Identifier(id_column),
            sql.Identifier(schema_name),
            sql.Identifier(table_name)
        ))
        max_id = cur.fetchone()[0]

        min_pk = min_id
        max_pk = max_id
        total_range = max_id - min_id
        current_batch_percent = analyze_batch_percent
        current_start = min_id
        loop_counter = 0
        previous_row_count = 0
        same_previous_row_count = 0
        current_decrease_ratio = 2

        while current_start <= max_id:
            current_batch_size = int(total_range / 100 * current_batch_percent)
            if current_batch_size < analyze_batch_size:
                current_batch_size = analyze_batch_size
                current_decrease_ratio = 2
                if debug:
                    print(f"{loop_counter}: resetting current_decrease_ratio to {current_decrease_ratio}")

            current_end = current_start + current_batch_size

            if debug:
                print(f"{loop_counter}: Loop counter: {loop_counter}, current_batch_percent: {round(current_batch_percent, 8)}, current_batch_size: {current_batch_size}, current_start: {current_start} (min: {min_id}), current_end: {current_end} (max: {max_id}), perc: {round(current_start / max_id * 100, 4)}")

            if current_end > max_id:
                current_end = max_id

            loop_counter += 1
            cur.execute(sql.SQL("SELECT COUNT(*) FROM {}.{} WHERE {} BETWEEN %s AND %s").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
                sql.Identifier(id_column)
            ), (current_start, current_end))
            testing_row_count = cur.fetchone()[0]

            if debug:
                print(f"{loop_counter}: Testing row count: {testing_row_count}")

            if testing_row_count == previous_row_count:
                same_previous_row_count += 1
                if same_previous_row_count >= 2:
                    current_decrease_ratio *= 2
                    if debug:
                        print(f"{loop_counter}: changing current_decrease_ratio to {current_decrease_ratio}")
                    same_previous_row_count = 0
            else:
                same_previous_row_count = 0

            previous_row_count = testing_row_count

            if testing_row_count > analyze_batch_size:
                current_batch_percent /= current_decrease_ratio
                if debug:
                    print(f"{loop_counter}: Decreasing analyze_batch_percent to {round(current_batch_percent, 8)}")
                continue

            if testing_row_count == 0:
                current_batch_percent *= 1.5
                if debug:
                    print(f"{loop_counter}: Increasing analyze_batch_percent to {round(current_batch_percent, 8)} without restarting loop")

            cur.execute(sql.SQL("INSERT INTO temp_id_ranges SELECT %s::bigint AS batch_start, %s::bigint AS batch_end, COUNT(*) AS row_count FROM {}.{} WHERE {} BETWEEN %s AND %s").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
                sql.Identifier(id_column)
            ), (current_start, current_end, current_start, current_end))

            current_start = current_end + 1
            if debug:
                print(f"{loop_counter}: loop end - new current_start: {current_start}")

        if debug:
            print(f"{loop_counter}: second loop")

        current_start = min_id
        while current_start <= max_id:
            cur.execute(sql.SQL("""
                SELECT min(batch_start) as batch_start, max(batch_end) as batch_end, max(cumulative_row_count) as row_count
                FROM (
                    SELECT batch_start, batch_end, sum(row_count) over (order by batch_start) as cumulative_row_count
                    FROM temp_id_ranges
                    WHERE batch_start >= %s::bigint
                    ORDER BY batch_start
                ) subquery
                WHERE cumulative_row_count <= %s::bigint
            """), (current_start, analyze_batch_size))
            result = cur.fetchone()
            if result:
                batch_start, batch_end, row_count = result
                yield min_pk, max_pk, batch_start, batch_end, row_count
            current_start = batch_end

        cur.execute(sql.SQL("SELECT * FROM temp_id_ranges WHERE row_count > %s::bigint"), (analyze_batch_size,))
        for record in cur.fetchall():
            batch_start, batch_end, row_count = record
            yield min_pk, max_pk, batch_start, batch_end, row_count

        cur.execute("DROP TABLE temp_id_ranges")
        if debug:
            print(f"{loop_counter}: Done")

    def migrate_table(self, migrate_target_connection, settings):
        part_name = 'initialize'
        try:
            worker_id = settings['worker_id']
            source_schema = settings['source_schema']
            source_table = settings['source_table']
            source_columns = settings['source_columns']
            target_schema = settings['target_schema']
            target_table = settings['target_table']
            target_columns = settings['target_columns']
            primary_key_columns = settings['primary_key_columns']
            batch_size = settings['batch_size']
            source_table_rows = self.get_rows_count(source_schema, source_table)

            if source_table_rows == 0:
                self.logger.info(f"Worker {worker_id}: Table {source_table} is empty - skipping data migration.")
                return 0
            else:
                self.logger.info(f"Worker {worker_id}: Table {source_table} has {source_table_rows} rows - starting data migration.")
                offset = 0
                while True:
                    part_name = f'prepare fetch data: {source_table} - {offset}'
                    if primary_key_columns:
                        query = f"SELECT TOP {batch_size} * FROM {source_schema}.{source_table} WHERE {primary_key_columns} > (SELECT MAX({primary_key_columns}) FROM (SELECT TOP {offset} {primary_key_columns} FROM {source_schema}.{source_table} ORDER BY {primary_key_columns}) AS temp) ORDER BY {primary_key_columns}"
                    else:
                        query = f"SELECT TOP {batch_size} * FROM {source_schema}.{source_table} WHERE id > {offset} ORDER BY id"

                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Worker {worker_id}: Fetching data with query: {query}")

                    part_name = f'do fetch data: {source_table} - {offset}'
                    df = pl.read_database(query, self.connection)
                    if df.is_empty():
                        break

                    self.logger.info(f"Worker {worker_id}: Fetched {len(df)} rows from source table {source_table}.")
                    # self.logger.info(f"Worker {worker_id}: Migrating batch starting at offset {offset} for table {table_name}.")

                    # Convert Polars DataFrame to list of tuples for insertion
                    records = df.to_dicts()

                    # Adjust binary or bytea types
                    for record in records:
                        for order_num, column in source_columns.items():
                            column_name = column['name']
                            column_type = column['type']
                            if column_type.lower() in ['binary', 'varbinary']:
                                record[column_name] = bytes(record[column_name])

                    part_name = f'insert data: {target_table} - {offset}'
                    migrate_target_connection.insert_batch(target_schema, target_table, target_columns, records)
                    self.logger.info(f"Worker {worker_id}: inserted {len(df)} rows into target table {target_table}.")

                    offset += batch_size

                self.logger.info(f"Worker {worker_id}: Finished migrating data for table {source_table}.")
                return source_table_rows
        except Exception as e:
            self.logger.error(f"Worker {worker_id}: Error during {part_name} -> {e}")
            raise e
