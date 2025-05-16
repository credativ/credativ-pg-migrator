import jaydebeapi
from jaydebeapi import Error
import pyodbc
from pyodbc import Error
from database_connector import DatabaseConnector
from migrator_logging import MigratorLogger
import re
import traceback
#import polars as pl

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
                self.logger.debug(f"Sybase ASE: Reading columns for {table_schema}.{table_name}")
            cursor.execute(query)
            for row in cursor.fetchall():
                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Processing column: {row}")
                ordinal_position = row[0]
                column_name = row[1].strip()
                data_type = row[2].strip()
                data_type_length = row[3].strip()
                length = row[4]
                column_nullable = row[5]
                identity_column = row[6]
                full_data_type_length = row[7].strip()
                column_domain = row[8]
                column_default_name = row[9]
                column_default_value = row[10].replace('column_default', '').strip().strip('"') if row[10] and row[10].replace('column_default', '').strip().startswith('"') and row[10].replace('column_default', '').strip().endswith('"') else (row[10].replace('column_default', '').strip() if row[10] else '')
                status = row[11]
                variable_length = row[12]
                data_type_precision = row[13]
                data_type_scale = row[14]
                type_nullable = row[15]
                type_has_identity_property = row[16]
                result[ordinal_position] = {
                    'column_name': column_name,
                    'data_type': data_type,
                    'column_type': full_data_type_length,
                    'character_maximum_length': length if self.is_string_type(data_type) else None,
                    'is_nullable': 'NO' if column_nullable == 0 else 'YES',
                    'column_default': column_default_value,
                    'column_comment': '',
                    'is_identity': 'YES' if identity_column == 1 else 'NO',
                }

                # # if self.config_parser.get_log_level() == 'DEBUG':
                # #     self.logger.debug(f"0 default: {result[row[0]]['column_default']}")
                # # checking for default values substitution with the origingal data type
                # if migrator_tables is not None:
                #     if result[row[0]]['column_default'] != '':
                #         result[row[0]]['column_default'] = migrator_tables.check_default_values_substitution(result[row[0]]['name'], result[row[0]]['type'], result[row[0]]['column_default'])

                query_custom_types = f"""
                    SELECT
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
                        END as source_data_type_length,
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
                    length_precision = custom_type[1]
                    type_has_identity_property = custom_type[2]
                    type_nullable = custom_type[3]
                    source_data_type_length = custom_type[4]
                    length = custom_type[5]
                    data_type_precision = custom_type[6]
                    data_type_scale = custom_type[7]
                    result[ordinal_position]['basic_data_type'] = source_data_type
                    result[ordinal_position]['basic_character_maximum_length'] = length if self.is_string_type(source_data_type) else None
                    result[ordinal_position]['basic_numeric_precision'] = data_type_precision if self.is_numeric_type(source_data_type) else None
                    result[ordinal_position]['basic_numeric_scale'] = data_type_scale if self.is_numeric_type(source_data_type) else None
                    result[ordinal_position]['basic_column_type'] = source_data_type_length
                    if custom_type[3] == 1:
                        result[ordinal_position]['is_identity'] = 'YES'

            cursor.close()
            self.disconnect()
            return result
        except Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def get_types_mapping(self, settings):
        target_db_type = settings['target_db_type']
        types_mapping = {}
        if target_db_type == 'postgresql':
            types_mapping = {
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
                'MONEY': 'MONEY',
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
        source_schema = settings['source_schema']
        source_table_name = settings['source_table_name']
        target_schema = settings['target_schema']
        target_table_name = settings['target_table_name']
        target_columns = settings['target_columns']
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
                index_name = index[0].strip()
                index_unique = index[1]  ## integer 0 or 1
                index_columns = index[2].strip()
                index_primary_key = index[3]
                index_owner = ''

                index_columns_count = 0
                index_columns_data_types = []
                for column_name in index_columns.split(','):
                    column_name = column_name.strip().strip('"')
                    for col_order_num, column_info in target_columns.items():
                        if column_name == column_info['column_name']:
                            index_columns_count += 1
                            column_data_type = column_info['data_type']
                            if self.config_parser.get_log_level() == 'DEBUG':
                                self.logger.debug(f"Table: {target_schema}.{target_table_name}, index: {index_name}, column: {column_name} has data type {column_data_type}")
                            index_columns_data_types.append(column_data_type)
                            index_columns_data_types_str = ', '.join(index_columns_data_types)

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
                        'owner': index_owner,
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

        except Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
            raise

    def fetch_constraints(self, settings):
        source_table_id = settings['source_table_id']
        source_schema = settings['source_schema']
        source_table_name = settings['source_table_name']
        target_schema = settings['target_schema']
        target_table_name = settings['target_table_name']
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
                    'sql': create_fk_query,
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
                'type': row[2],
                'comment': ''
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

    def analyze_pk_distribution_batches(self, values):
        migrator_tables = values['migrator_tables']
        schema_name = values['source_schema']
        table_name = values['source_table']
        primary_key_columns = values['primary_key_columns']
        primary_key_columns_count = values['primary_key_columns_count']
        primary_key_columns_types = values['primary_key_columns_types']
        worker_id = values['worker_id']
        analyze_batch_size = self.config_parser.get_batch_size()

        if primary_key_columns_count == 1 and primary_key_columns_types in ('BIGINT', 'INTEGER', 'NUMERIC', 'REAL', 'FLOAT', 'DOUBLE PRECISION', 'DECIMAL', 'SMALLINT', 'TINYINT'):
            # primary key is one column of numeric type - analysis with min/max values is much quicker
            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Worker: {worker_id}: PK analysis: {primary_key_columns} ({primary_key_columns_types}): min/max analysis")

            current_batch_percent = 20

            sybase_cursor = self.connection.cursor()
            temp_table = f"temp_id_ranges_{str(worker_id).replace('-', '_')}"
            migrator_tables.protocol_connection.execute_query(f"""DROP TABLE IF EXISTS "{temp_table}" """)
            migrator_tables.protocol_connection.execute_query(f"""CREATE TEMP TABLE IF NOT EXISTS "{temp_table}" (batch_start BIGINT, batch_end BIGINT, row_count BIGINT)""")

            pk_range_table = self.config_parser.get_protocol_name_pk_ranges()
            sybase_cursor.execute(f"SELECT MIN({primary_key_columns}) FROM {schema_name}.{table_name}")
            min_id = sybase_cursor.fetchone()[0]

            sybase_cursor.execute(f"SELECT MAX({primary_key_columns}) FROM {schema_name}.{table_name}")
            max_id = sybase_cursor.fetchone()[0]

            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Worker: {worker_id}: PK analysis: {primary_key_columns}: min_id: {min_id}, max_id: {max_id}")

            total_range = int(max_id) - int(min_id)
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
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Worker: {worker_id}: PK analysis: {loop_counter}: resetting current_decrease_ratio to {current_decrease_ratio}")

                current_end = current_start + current_batch_size

                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Worker: {worker_id}: PK analysis: {loop_counter}: Loop counter: {loop_counter}, current_batch_percent: {round(current_batch_percent, 8)}, current_batch_size: {current_batch_size}, current_start: {current_start} (min: {min_id}), current_end: {current_end} (max: {max_id}), perc: {round(current_start / max_id * 100, 4)}")

                if current_end > max_id:
                    current_end = max_id

                loop_counter += 1
                sybase_cursor.execute(f"""SELECT COUNT(*) FROM {schema_name}.{table_name} WHERE {primary_key_columns} BETWEEN %s AND %s""", (current_start, current_end))
                testing_row_count = sybase_cursor.fetchone()[0]

                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Worker: {worker_id}: PK analysis: {loop_counter}: Testing row count: {testing_row_count}")

                if testing_row_count == previous_row_count:
                    same_previous_row_count += 1
                    if same_previous_row_count >= 2:
                        current_decrease_ratio *= 2
                        if self.config_parser.get_log_level() == 'DEBUG':
                            self.logger.debug(f"Worker: {worker_id}: PK analysis: {loop_counter}: changing current_decrease_ratio to {current_decrease_ratio}")
                        same_previous_row_count = 0
                else:
                    same_previous_row_count = 0

                previous_row_count = testing_row_count

                if testing_row_count > analyze_batch_size:
                    current_batch_percent /= current_decrease_ratio
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Worker: {worker_id}: PK analysis: {loop_counter}: Decreasing analyze_batch_percent to {round(current_batch_percent, 8)}")
                    continue

                if testing_row_count == 0:
                    current_batch_percent *= 1.5
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Worker: {worker_id}: PK analysis: {loop_counter}: Increasing analyze_batch_percent to {round(current_batch_percent, 8)} without restarting loop")

                sybase_cursor.execute(f"""SELECT
                            %s::bigint AS batch_start,
                            %s::bigint AS batch_end,
                            COUNT(*) AS row_count
                            FROM {schema_name}.{table_name}
                            WHERE {primary_key_columns  } BETWEEN %s AND %s""",
                            (current_start, current_end, current_start, current_end))

                result = sybase_cursor.fetchone()
                if result:
                    insert_batch_start = result[0]
                    insert_batch_end = result[1]
                    insert_row_count = result[2]
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Worker: {worker_id}: PK analysis: {loop_counter}: Insert batch into temp table: start: {insert_batch_start}, end: {insert_batch_end}, row count: {insert_row_count}")
                    migrator_tables.protocol_connection.execute_query(f"""INSERT INTO "{temp_table}" (batch_start, batch_end, row_count) VALUES (%s, %s, %s)""", (insert_batch_start, insert_batch_end, insert_row_count))

                current_start = current_end + 1
                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Worker: {worker_id}: PK analysis: {loop_counter}: loop end - new current_start: {current_start}")

            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Worker: {worker_id}: PK analysis: {loop_counter}: second loop")

            current_start = min_id
            while current_start <= max_id:
                migrator_tables.protocol_connection.execute_query("""
                    SELECT
                        min(batch_start) as batch_start,
                        max(batch_end) as batch_end,
                        max(cumulative_row_count) as row_count
                    FROM (
                        SELECT
                            batch_start,
                            batch_end,
                            sum(row_count) over (order by batch_start) as cumulative_row_count
                        FROM "{temp_table}"
                        WHERE batch_start >= %s::bigint
                        ORDER BY batch_start
                    ) subquery
                    WHERE cumulative_row_count <= %s::bigint
                """, (current_start, analyze_batch_size))
                result = migrator_tables.fetchone()
                if result:
                    insert_batch_start = result[0]
                    insert_batch_end = result[1]
                    insert_row_count = result[2]
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Worker: {worker_id}: PK analysis: {loop_counter}: Insert batch into protocol table: start: {insert_batch_start}, end: {insert_batch_end}, row count: {insert_row_count}")

                values = {}
                values['source_schema'] = schema_name
                values['source_table'] = table_name
                values['source_table_id'] = 0
                values['worker_id'] = worker_id
                values['pk_columns'] = primary_key_columns
                values['batch_start'] = insert_batch_start
                values['batch_end'] = insert_batch_end
                values['row_count'] = insert_row_count
                migrator_tables.insert_pk_ranges(values)
                current_start = insert_batch_end

            migrator_tables.protocol_connection.execute_query(f"""DROP TABLE IF EXISTS "{temp_table}" """)
            self.connection.commit()
            self.logger.info(f"Worker: {worker_id}: PK analysis: {loop_counter}: Finished analyzing PK distribution for table {table_name}.")

        # unfortunately, the following code is not working as expected - Sybase does not support BETWEEN for multiple columns as PostgreSQL does
        # this solution worked for foreign data wrapper but not for native connection
        # if PK has more than one column, we shall use cursor
        # else:

            # # we need to do slower analysis with selecting all values of primary key
            # # necessary for composite keys or non-numeric keys
            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Worker: {worker_id}: PK analysis: {primary_key_columns} ({primary_key_columns_types}): analyzing all PK values")

            # primary_key_columns_list = primary_key_columns.split(',')
            # primary_key_columns_types_list = primary_key_columns_types.split(',')
            # temp_table_structure = ', '.join([f"{column.strip()} {column_type.strip()}" for column, column_type in zip(primary_key_columns_list, primary_key_columns_types_list)])
            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Worker: {worker_id}: PK analysis: {primary_key_columns}: temp table structure: {temp_table_structure}")

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
            #     # if self.config_parser.get_log_level() == 'DEBUG':
            #     #     self.logger.debug(f"Worker: {worker_id}: PK analysis: {primary_key_columns}: row: {row}")
            #     insert_values = ', '.join([f"'{value}'" if isinstance(value, str) else str(value) for value in row])
            #     migrator_tables.protocol_connection.execute_query(f"""INSERT INTO "{temp_table}" ({primary_key_columns}) VALUES ({insert_values})""")
            # if self.config_parser.get_log_level() == 'DEBUG':
            #     self.logger.debug(f"Worker: {worker_id}: PK analysis: {primary_key_columns}: Inserted {pk_temp_table_row_count} rows into temp table {temp_table}")

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

            #     if self.config_parser.get_log_level() == 'DEBUG':
            #         self.logger.debug(f"Worker: {worker_id}: PK analysis: {batch_loop}: Loop counter: {batch_loop}, PK values: {rec_min_values} / {rec_max_values}")

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
            primary_key_columns = settings['primary_key_columns']
            primary_key_columns_count = settings['primary_key_columns_count']
            primary_key_columns_types = settings['primary_key_columns_types']
            batch_size = settings['batch_size']
            migrator_tables = settings['migrator_tables']
            source_table_rows = self.get_rows_count(source_schema, source_table)
            migration_limitation = settings['migration_limitation']

            if source_table_rows == 0:
                self.logger.info(f"Worker {worker_id}: Table {source_table} is empty - skipping data migration.")
                migrator_tables.insert_data_migration(source_schema, source_table, source_table_id, source_table_rows, worker_id, target_schema, target_table, 0)
                return 0
            else:
                part_name = 'migrate_table in batches using cursor'
                self.logger.info(f"Worker {worker_id}: Table {source_table} has {source_table_rows} rows - starting data migration.")
                protocol_id = migrator_tables.insert_data_migration(source_schema, source_table, source_table_id, source_table_rows, worker_id, target_schema, target_table, 0)

                # Open a cursor and fetch rows in batches
                query = f"SELECT * FROM {source_schema}.{source_table}"

                if migration_limitation:
                    query += f" WHERE {migration_limitation}"

                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Worker {worker_id}: Fetching data with cursor using query: {query}")

                sybase_cursor = self.connection.cursor()
                sybase_cursor.execute(query)
                total_inserted_rows = 0
                while True:
                    records = sybase_cursor.fetchmany(batch_size)
                    if not records:
                        break
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Worker {worker_id}: Fetched {len(records)} rows from source table '{source_table}' using cursor")

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
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Worker {worker_id}: Starting insert of {len(records)} rows from source table {source_table}")
                    settings = {
                        'target_schema': target_schema,
                        'target_table': target_table,
                        'target_columns': target_columns,
                        'data': records,
                        'worker_id': worker_id,
                        'migrator_tables': migrator_tables,
                    }
                    inserted_rows = migrate_target_connection.insert_batch(settings)
                    total_inserted_rows += inserted_rows
                    self.logger.info(f"Worker {worker_id}: Inserted {inserted_rows} (total: {total_inserted_rows} from: {source_table_rows} ({round(total_inserted_rows/source_table_rows*100, 2)}%)) rows into target table '{target_table}'")

                target_table_rows = migrate_target_connection.get_rows_count(target_schema, target_table)
                self.logger.info(f"Worker {worker_id}: Target table {target_schema}.{target_table} has {target_table_rows} rows")
                migrator_tables.update_data_migration_status(protocol_id, True, 'OK', target_table_rows)
                sybase_cursor.close()

        except Exception as e:
            self.logger.error(f"Worker {worker_id}: Error during {part_name} -> {e}")
            self.logger.error("Full stack trace:")
            self.logger.error(traceback.format_exc())
            raise e
        finally:
            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Worker {worker_id}: Finished processing table {source_table}.")
            return target_table_rows

    def convert_trigger(self, trigger_name, trigger_code, source_schema, target_schema, table_list):
        return None

    def fetch_triggers(self, schema_name, table_name):
        pass

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
        except Error as e:
            self.logger.error(f"Error executing query: {query}")
            self.logger.error(e)
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

    def convert_view_code(self, view_code: str, settings: dict):
        if self.config_parser.get_log_level() == 'DEBUG':
            self.logger.debug(f"settings in convert_view_code: {settings}")
        converted_code = view_code
        converted_code = converted_code.replace(f"{settings['source_database']}..", f"{settings['target_schema']}.")
        converted_code = converted_code.replace(f"{settings['source_database']}.{settings['source_schema']}.", f"{settings['target_schema']}.")
        converted_code = converted_code.replace(f"{settings['source_schema']}.", f"{settings['target_schema']}.")
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

    def testing_select(self):
        return 'SELECT 1'
