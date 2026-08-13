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
# import jpype
from credativ_pg_migrator.database_connector import DatabaseConnector
from credativ_pg_migrator.migrator_logging import MigratorLogger
from credativ_pg_migrator.jvm_helper import detach_thread_from_jvm
import re
import traceback
import pyodbc
import time
import datetime

class InformixConnector(DatabaseConnector):

    ## Informix refuses a query whose output row is wider than 32767 bytes. BSON and JSON
    ## columns are read as LVARCHAR, so such a cast cannot simply claim the whole LVARCHAR
    ## maximum - it has to share the row with all other columns of the SELECT list.
    MAX_OUTPUT_ROWSIZE = 32767
    MAX_LVARCHAR_LENGTH = 32739
    ## reserve for the per column overhead of the row descriptor and for columns whose
    ## width we can only estimate
    OUTPUT_ROWSIZE_RESERVE = 512
    ## below this a cast makes no sense anymore - such a table has to be migrated with
    ## the document column excluded
    MIN_DOCUMENT_CAST_LENGTH = 256
    ## estimated width of a column in the output row of a SELECT
    DEFAULT_OUTPUT_WIDTH = 256
    OUTPUT_WIDTH_BY_DATA_TYPE = {
        'boolean': 1,
        'smallint': 2,
        'integer': 4, 'int': 4, 'serial': 4, 'date': 4, 'smallfloat': 4, 'real': 4,
        'int8': 8, 'bigint': 8, 'serial8': 8, 'bigserial': 8, 'float': 8, 'double precision': 8,
        ## read through a cast to LVARCHAR(40), see migrate_table()
        'interval': 48,
        'decimal': 17, 'numeric': 17, 'money': 17,
        ## only the descriptor of a large object travels in the row, not its content
        'text': 72, 'byte': 72, 'clob': 72, 'blob': 72,
        ## these are wrapped into TO_CHAR() in the SELECT list
        'datetime': 256, 'time': 256, 'timestamp': 256,
    }
    DOCUMENT_DATA_TYPES = ('bson', 'json')
    ## Collection and row types reach the driver as a Java object (HashSet, ArrayList,
    ## IfxStruct) which the target cannot store - Informix casts them to their literal
    ## text representation instead: SET{'a','b'}, LIST{'x'}, ROW('a','b')
    COLLECTION_DATA_TYPES = ('set', 'multiset', 'list', 'row', 'collection')
    ## those of them which become an array in the target - a ROW is a record, not a
    ## collection, and keeps the text of its literal
    ARRAY_DATA_TYPES = ('set', 'multiset', 'list', 'collection')

    ## Informix reserves the tabid values 1 to 99 for the tables and views of the system
    ## catalog - user objects start at 100. Filtering by owner alone is not enough because
    ## a database created by the informix user has its own objects owned by 'informix' too.
    FIRST_USER_TABID = 100
    ## sysprocedures.mode marks the routines supplied by the database server with a
    ## lowercase letter, routines created by a user carry the uppercase one:
    ## D = DBA, O = owner, P = protected, R = restricted, T = trigger
    USER_ROUTINE_MODES = ('D', 'O', 'P', 'R', 'T')

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
            self.connection = pyodbc.connect(connection_string)
        elif self.config_parser.get_connectivity(self.source_or_target) == 'jdbc':
            connection_string = self.config_parser.get_connect_string(self.source_or_target)
            username = self.config_parser.get_db_config(self.source_or_target)['username']
            password = self.config_parser.get_db_config(self.source_or_target)['password']
            jdbc_driver = self.config_parser.get_db_config(self.source_or_target)['jdbc']['driver']
            jdbc_libraries = self.config_parser.get_db_config(self.source_or_target)['jdbc']['libraries']

            # if not jpype.isJVMStarted():
            #     jpype.startJVM(jpype.getDefaultJVMPath(), f"-Djava.class.path={jdbc_libraries}")
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
        finally:
            detach_thread_from_jvm()

    def estimate_column_output_width(self, col):
        """ Estimated number of bytes a column occupies in the output row of a SELECT """
        data_type = (col.get('data_type') or '').lower()
        if data_type in ('char', 'nchar', 'varchar', 'nvarchar', 'lvarchar'):
            ## character_maximum_length is only filled for the types is_string_type()
            ## knows, which leaves out LVARCHAR - the length of the catalog is the
            ## fallback, and only a column without any length at all is estimated
            length = col.get('character_maximum_length') or col.get('source_column_length') or self.DEFAULT_OUTPUT_WIDTH
            ## trim() turns a CHAR into a VARCHAR, both carry one extra byte for the length
            return length + 1
        return self.OUTPUT_WIDTH_BY_DATA_TYPE.get(data_type, self.DEFAULT_OUTPUT_WIDTH)

    def calculate_document_cast_length(self, source_columns):
        """
        Length of the LVARCHAR cast used to read BSON / JSON and collection columns.

        Informix rejects a query whose output row exceeds 32767 bytes, so the casts may
        only claim what the remaining columns of the SELECT list leave over, shared
        between them. Returns 0 when the table has no such column at all.
        """
        cast_types = self.DOCUMENT_DATA_TYPES + self.COLLECTION_DATA_TYPES
        document_columns = 0
        used_width = 0
        for col in source_columns.values():
            if (col.get('data_type') or '').lower() in cast_types:
                document_columns += 1
            else:
                used_width += self.estimate_column_output_width(col)

        if document_columns == 0:
            return 0

        available_width = self.MAX_OUTPUT_ROWSIZE - used_width - self.OUTPUT_ROWSIZE_RESERVE
        cast_length = available_width // document_columns
        return max(self.MIN_DOCUMENT_CAST_LENGTH, min(cast_length, self.MAX_LVARCHAR_LENGTH))

    def map_trigger_correlation_names(self, text, old_ref, new_ref):
        """
        Replace the correlation names of an Informix trigger with OLD and NEW.

        Informix names the two row images itself ('referencing old as o new as n') and the
        body then reads 'o.list_price'. A PL/pgSQL trigger function addresses them as OLD
        and NEW, so the names of the source are of no use in the target.
        """
        if old_ref:
            text = re.sub(rf'\b{re.escape(old_ref)}\.', 'OLD.', text)
        if new_ref:
            text = re.sub(rf'\b{re.escape(new_ref)}\.', 'NEW.', text)
        return text

    ## the keywords which end the FROM clause of a query
    CLAUSES_AFTER_FROM = (r'(?i)\b(WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|UNION|INTERSECT|EXCEPT'
                          r'|LIMIT|OFFSET|FOR\s+UPDATE|WITH\s+(?:NO\s+)?LOCKS|INTO\s+TEMP)\b')

    def scan_sql_text(self, text, mask_identifiers=True):
        """
        The text with its string literals and comments blanked out, and the depth of parentheses
        of every one of its characters.

        A position found in the blanked text addresses the same character of the original, so a
        keyword can be looked for where SQL means it - not inside a literal, and not in a
        subquery when the clause of the query around it is the one being read.

        A quoted identifier is blanked as well, so that a keyword spelled inside one is not read
        as a keyword. It is kept when `mask_identifiers` is off, for a caller which looks for a
        name and not for a keyword - the qualifier of `"informix".equal(...)` is one.
        """
        masked = list(text)
        depths = [0] * len(text)
        depth = 0
        index = 0
        while index < len(text):
            character = text[index]
            if character == '"':
                ## the parentheses of a quoted identifier are part of the name, they are not read
                end = index + 1
                while end < len(text) and text[end] != '"':
                    end += 1
                end = min(end + 1, len(text))
                for position in range(index, end):
                    depths[position] = depth
                    if mask_identifiers:
                        masked[position] = ' '
                index = end
                continue
            if character == "'":
                masked[index] = ' '
                depths[index] = depth
                index += 1
                while index < len(text):
                    depths[index] = depth
                    closing = text[index] == "'"
                    masked[index] = ' '
                    index += 1
                    if closing:
                        break
                continue
            if text.startswith('{', index):
                ## Informix writes a comment in braces as well as with '--'
                end = text.find('}', index)
                end = len(text) if end == -1 else end + 1
                for position in range(index, end):
                    masked[position] = ' '
                    depths[position] = depth
                index = end
                continue
            if text.startswith('--', index):
                end = text.find('\n', index)
                end = len(text) if end == -1 else end
                for position in range(index, end):
                    masked[position] = ' '
                    depths[position] = depth
                index = end
                continue
            if character == '(':
                depth += 1
            depths[index] = depth
            if character == ')':
                depth -= 1
            index += 1
        return ''.join(masked), depths

    def split_top_level_commas(self, text):
        """
        Split on the commas which are not inside parentheses or a string literal.

        Needed wherever a list of items may contain a routine call of its own, where the
        commas belong to its argument list and must not end the item.
        """
        items = []
        current = []
        depth = 0
        quote = ''
        for character in text:
            if quote:
                if character == quote:
                    quote = ''
            elif character in ("'", '"'):
                quote = character
            elif character == '(':
                depth += 1
            elif character == ')':
                depth -= 1
            elif character == ',' and depth == 0:
                items.append(''.join(current).strip())
                current = []
                continue
            current.append(character)
        if ''.join(current).strip():
            items.append(''.join(current).strip())
        return items

    def is_year_month_interval(self, col):
        """
        True for an INTERVAL of the year-month class, False for the day-time class.

        Informix encodes the qualifier of an INTERVAL in syscolumns.collength: the low
        byte holds the largest qualifier in its upper and the smallest in its lower four
        bits, with YEAR = 0, MONTH = 2, DAY = 4, HOUR = 6, MINUTE = 8, SECOND = 10.
        """
        largest_qualifier = ((col.get('source_column_length') or 0) % 256) // 16
        return largest_qualifier <= 2

    def convert_interval_value(self, value, year_month):
        """
        Turn the text of an Informix interval into a literal PostgreSQL reads correctly.

        The value is selected normalized to the widest qualifier of its class, so it
        always arrives as '[-]DDD HH:MM:SS' or as '[-]YYY-MM'. It cannot be handed over
        unchanged: for Informix the leading sign negates the whole interval, while
        PostgreSQL reads '-2 06:30:00' as '-2 days +06:30:00' - it applies the sign to
        the day alone. The sign is therefore repeated in front of every field.
        """
        text = str(value).strip()
        if not text:
            return None
        sign = '-' if text.startswith('-') else ''
        text = text.lstrip('+-').strip()

        if year_month:
            years, _, months = text.partition('-')
            return f"{sign}{int(years or 0)} years {sign}{int(months or 0)} months"

        days, _, clock = text.partition(' ')
        hours, minutes, seconds = (clock.split(':') + ['0', '0', '0'])[:3]
        return (f"{sign}{int(days or 0)} days {sign}{int(hours or 0)} hours "
                f"{sign}{int(minutes or 0)} minutes {sign}{float(seconds or 0)} seconds")

    def execute_query_with_rowsize_retry(self, cursor, query, cast_length, worker_id, table_reference):
        """
        Run the data query, making the LVARCHAR casts smaller if Informix rejects the row.

        How many bytes a column occupies in the output row of a SELECT can only be
        estimated - see calculate_document_cast_length() - and an estimate which is too
        optimistic costs the whole table with 'Maximum output rowsize (32767) exceeded'.
        Instead of failing, the casts of the document and collection columns are halved
        until the statement is accepted: a shortened value is still better than no table,
        and it is reported. Returns the query which was finally executed.
        """
        while True:
            try:
                cursor.execute(query)
                return query
            except Exception as e:
                if 'Maximum output rowsize' not in str(e) or cast_length <= self.MIN_DOCUMENT_CAST_LENGTH:
                    raise
                cast_length = max(self.MIN_DOCUMENT_CAST_LENGTH, cast_length // 2)
                ## every cast of the SELECT list carries the same length, and LVARCHAR
                ## appears nowhere else in the generated query
                query = re.sub(r'(?i)AS\s+LVARCHAR\(\d+\)', f'AS LVARCHAR({cast_length})', query)
                self.config_parser.print_log_message('WARNING',
                    f"informix_connector: migrate_table: Worker {worker_id}: Table {table_reference}: The output row of the data query exceeds the maximum of Informix - the document and collection columns are reduced to {cast_length} characters and the query is repeated. A longer value is truncated.")

    def get_sql_functions_mapping(self, settings):
        """ Returns a dictionary of SQL functions mapping for the target database """
        target_db_type = settings['target_db_type']
        if target_db_type == 'postgresql':
            return {
                'year(': 'extract(year from ',
                'month(': 'extract(month from ',
                'day(': 'extract(day from ',
                ## NVL is the Informix spelling of COALESCE, and a routine using it is
                ## created without a word by PostgreSQL - the body of a PL/pgSQL routine is
                ## only checked for its syntax - and fails on the first call with
                ## 'function nvl(numeric, integer) does not exist'
                'nvl(': 'coalesce(',
            }
        else:
            self.config_parser.print_log_message('ERROR', f"informix_connector: get_sql_functions_mapping: Unsupported target database type: {target_db_type}")

    def migrate_sequences(self, target_connector, settings):
        return True

    def fetch_table_names(self, table_schema: str):
        query = f"""
            SELECT tabid, tabname
            FROM systables
            WHERE owner = '{table_schema}' AND tabtype = 'T'
            AND tabid >= {self.FIRST_USER_TABID}
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
            self.config_parser.print_log_message('ERROR', f"informix_connector: fetch_table_names: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_table_columns(self, settings) -> dict:
        """
        Column list of a table.

        A column of an extended data type carries the name of that type in sysxtdtypes.
        The constructed types - SET(...), MULTISET(...), LIST(...) and an unnamed
        ROW(...) - have no name of their own (mode 'C'), they are identified by the base
        type code in sysxtdtypes.type. A named row type (mode 'R') is reported as ROW as
        well, because PostgreSQL has no counterpart for either and both are migrated as
        their text representation.
        """
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
                    WHEN trim(x.mode) = 'C' THEN
                        CASE x.type
                            WHEN 19 THEN 'SET'
                            WHEN 20 THEN 'MULTISET'
                            WHEN 21 THEN 'LIST'
                            WHEN 22 THEN 'ROW'
                            WHEN 23 THEN 'COLLECTION'
                            ELSE 'UNKNOWN-'||cast(c.coltype as varchar(10))||'-'||cast(x.extended_id as varchar(10))
                        END
                    WHEN trim(x.mode) = 'R' THEN 'ROW'
                    WHEN x.name IS NOT NULL THEN upper(trim(x.name))
                    ELSE 'UNKNOWN-'||cast(c.coltype as varchar(10))||'-'||cast(x.extended_id as varchar(10))
                END AS coltype,
                c.collength,
                CASE WHEN c.coltype >= 256 THEN 'NO' ELSE 'YES' END AS nullable,
                CASE WHEN d.type = 'L' THEN
                    CASE
                        WHEN (CASE WHEN c.coltype >= 256 THEN c.coltype - 256 ELSE c.coltype END)
                            IN (0, 13, 15, 16, 40, 41, 45) THEN d.default
                        ELSE SUBSTR(d.default, INSTR(d.default, ' ') + 1)
                    END
                ELSE NULL
                END AS default_value,
                ifx_bit_rightshift(c.collength, 8) as numeric_precision,
                bitand(c.collength, "0xff") as numeric_scale,
                d.type AS default_type
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
            self.config_parser.print_log_message('DEBUG', f"informix_connector: fetch_table_columns: Informix: Reading columns for {table_schema}.{table_name}")
            cursor.execute(query)
            for row in cursor.fetchall():
                column_number = row[0]
                column_name = row[1]
                data_type = row[2].strip().upper()
                maximum_length = row[3]
                is_nullable = row[4].strip().upper()
                numeric_precision = row[6]
                numeric_scale = row[7]
                column_default_value = self.convert_informix_default({
                    'column_name': column_name,
                    'data_type': data_type,
                    'default_type': row[8],
                    'default_value': row[5],
                })

                column_type = data_type
                if self.is_string_type(data_type):
                    column_type = f"{data_type}({maximum_length})"
                elif self.is_numeric_type(data_type):
                    column_type = f"{data_type}({maximum_length},{numeric_scale})"
                result[column_number] = {
                    'column_name': column_name,
                    'data_type': data_type,
                    'column_type': '',
                    ## syscolumns.collength as it is - the width calculation of the SELECT
                    ## list needs it also for the types is_string_type() does not cover,
                    ## LVARCHAR above all
                    'source_column_length': maximum_length,
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
            self.config_parser.print_log_message('ERROR', f"informix_connector: fetch_table_columns: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise
    def get_aliases(self, settings):
        source_schema_name = settings.get('source_schema_name')
        aliases = {}
        order_num = 1
        query = f"""
            SELECT
                t.tabname as alias_name,
                s.owner as aliased_schema_name,
                s.tabname as aliased_table_name,
                t.owner as alias_owner
            FROM systables t
            JOIN syssyntable s ON t.tabid = s.tabid
            WHERE t.owner = '{source_schema_name}' AND t.tabtype IN ('S', 'P')
            AND t.tabid >= {self.FIRST_USER_TABID}
            ORDER BY t.tabname
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                alias_name = row[0].strip() if row[0] else ''
                aliased_schema_name = row[1].strip() if row[1] else ''
                aliased_table_name = row[2].strip() if row[2] else ''
                alias_owner = row[3].strip() if row[3] else source_schema_name
                alias_sql = f"CREATE SYNONYM {alias_owner}.{alias_name} FOR {aliased_schema_name}.{aliased_table_name}"
                
                aliases[order_num] = {
                    'id': order_num,
                    'alias_schema_name': source_schema_name,
                    'alias_name': alias_name,
                    'aliased_schema_name': aliased_schema_name,
                    'aliased_table_name': aliased_table_name,
                    'alias_owner': alias_owner,
                    'alias_sql': alias_sql,
                    'alias_comment': ''
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return aliases
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"informix_connector: get_aliases: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise
    def fetch_views_names(self, source_schema_name: str):
        views = {}
        order_num = 1
        query = f"""
            SELECT DISTINCT v.tabid, t.tabname
            FROM sysviews v
            JOIN systables t on v.tabid = t.tabid
            WHERE t.owner = '{source_schema_name}'
            AND t.tabid >= {self.FIRST_USER_TABID}
            ORDER BY t.tabname
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                views[order_num] = {
                    'id': row[0],
                    'schema_name': source_schema_name,
                    'view_name': row[1],
                    'comment': ''
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return views
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"informix_connector: fetch_views_names: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_view_code(self, settings):
        view_id = settings['view_id']
        # source_schema_name = settings['source_schema_name']
        # source_view_name = settings['source_view_name']
        # target_schema_name = settings['target_schema_name']
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

    def convert_collection_value(self, value):
        """
        Turn the literal of an Informix collection into a list for the array of the target.

        The value is read from the source as its literal representation, which names the
        constructor and holds the elements in braces:

            SET{'+49 30 000002','+49 172 000006'}   ->  ['+49 30 000002', '+49 172 000006']
            LIST{'office'}                          ->  ['office']
            SET{}                                   ->  []

        A value which does not look like one of them is handed over unchanged, so nothing
        is lost when the source delivers something unexpected.
        """
        text = str(value).strip()
        collection_match = re.match(r'(?is)^(?:SET|MULTISET|LIST|COLLECTION)?\s*\{(?P<elements>.*)\}$', text)
        if not collection_match:
            return text

        elements = []
        for element in self.split_top_level_commas(collection_match.group('elements')):
            element = element.strip()
            if len(element) >= 2 and element.startswith("'") and element.endswith("'"):
                ## a quote inside an element is doubled in the literal
                element = element[1:-1].replace("''", "'")
            elements.append(element)
        return elements

    def convert_matches_operator(self, code):
        """
        Convert the MATCHES operator of Informix, which PostgreSQL does not know.

        MATCHES is a pattern operator of its own, it is not LIKE with another name:

            *      any number of characters      -> % of LIKE
            ?      exactly one character         -> _ of LIKE
            [abc]  one character out of a set    -> no counterpart in LIKE
            %, _   ordinary characters           -> have to be escaped for LIKE

        A pattern without a character class becomes LIKE, which the target can answer from
        an index. A pattern with one becomes SIMILAR TO, whose bracket expression means the
        same thing. Only a pattern written as a literal can be translated - MATCHES against
        an expression is reported and left as it is.
        """
        def convert_pattern(match):
            pattern = match.group('pattern')[1:-1]
            converted = []
            character_class = False
            index = 0
            while index < len(pattern):
                character = pattern[index]
                if character == '\\' and index + 1 < len(pattern):
                    ## an escaped character of MATCHES stands for itself
                    following = pattern[index + 1]
                    converted.append(f"\\{following}" if following in '%_[]\\' else following)
                    index += 2
                    continue
                if character == '[':
                    character_class = True
                    closing = pattern.find(']', index + 1)
                    if closing == -1:
                        converted.append('\\[')
                        index += 1
                        continue
                    converted.append(pattern[index:closing + 1])
                    index = closing + 1
                    continue
                converted.append({'*': '%', '?': '_', '%': '\\%', '_': '\\_'}.get(character, character))
                index += 1

            operator = 'SIMILAR TO' if character_class else 'LIKE'
            self.config_parser.print_log_message('DEBUG',
                f"informix_connector: convert_matches_operator: MATCHES {match.group('pattern')} converted to {operator} '{''.join(converted)}'")
            return f"{operator} '{''.join(converted)}'"

        code = re.sub(r"(?i)\bMATCHES\s+(?P<pattern>'(?:[^']|'')*')", convert_pattern, code)

        if re.search(r'(?i)\bMATCHES\b', code):
            self.config_parser.print_log_message('WARNING',
                "informix_connector: convert_matches_operator: A MATCHES operator whose pattern is not a literal was left in the code - PostgreSQL does not know the operator, it has to be rewritten manually.")
        return code

    ## The operators which Informix writes as a call to a function of its own system schema when it
    ## stores the text of a view. `is_active = 't'` on a BOOLEAN column is kept as
    ## `"informix".equal(is_active, 't')`, and there is no such function in PostgreSQL.
    OPERATOR_FUNCTIONS = {
        'equal': '=', 'notequal': '<>',
        'lessthan': '<', 'lessthanorequal': '<=',
        'greaterthan': '>', 'greaterthanorequal': '>=',
        'plus': '+', 'minus': '-', 'times': '*', 'divide': '/',
        'concat': '||', 'like': 'LIKE', 'notlike': 'NOT LIKE', 'matches': 'MATCHES',
    }
    UNARY_OPERATOR_FUNCTIONS = {'negate': '-'}

    def convert_operator_functions(self, code):
        """
        The operators which Informix stores as a call to a function of its system schema, written as
        the operators they are.

        The text of a view in `sysviews` is not the text the author wrote - the server writes the
        query back from its parsed form, and an operator whose operands need one of its own
        implementations is written as a call: `is_active = 't'` becomes
        `"informix".equal(is_active, 't')`, `a || b` becomes `"informix".concat(a, b)`. The
        qualifier is the system schema of Informix, so the schema replacement of the migration
        turned it into a function of the target schema and PostgreSQL answered
        `function public.equal(boolean, unknown) does not exist`.

        The conversion runs before the schema is replaced, so the qualifier is still the one of
        Informix and a function of the user which happens to carry one of these names is not
        touched.
        """
        pattern = re.compile(r'(?i)(?<![\w."])"?informix"?\s*\.\s*(?P<name>[a-zA-Z_]\w*)\s*\(')
        while True:
            masked, depths = self.scan_sql_text(code, mask_identifiers=False)
            match = None
            for candidate in pattern.finditer(masked):
                name = candidate.group('name').lower()
                if name in self.OPERATOR_FUNCTIONS or name in self.UNARY_OPERATOR_FUNCTIONS:
                    match = candidate
                    break
            if match is None:
                return code

            ## the arguments reach up to the parenthesis which closes the call
            opening = match.end() - 1
            closing = next((position for position in range(opening + 1, len(code))
                            if masked[position] == ')' and depths[position] == depths[opening]), None)
            if closing is None:
                self.config_parser.print_log_message('WARNING',
                    f"informix_connector: convert_operator_functions: the call of {match.group('name')} is not closed - "
                    "it is left as it is")
                return code

            name = match.group('name').lower()
            ## an argument may be a call of its own, so it is converted before it is used
            arguments = [self.convert_operator_functions(argument)
                         for argument in self.split_top_level_commas(code[opening + 1:closing])]

            if name in self.UNARY_OPERATOR_FUNCTIONS and len(arguments) == 1:
                replacement = f"({self.UNARY_OPERATOR_FUNCTIONS[name]}{arguments[0]})"
            elif name in self.OPERATOR_FUNCTIONS and len(arguments) == 2:
                replacement = f"({arguments[0]} {self.OPERATOR_FUNCTIONS[name]} {arguments[1]})"
            else:
                self.config_parser.print_log_message('WARNING',
                    f"informix_connector: convert_operator_functions: {match.group('name')} of Informix was called with "
                    f"{len(arguments)} argument(s), which is not the operator it stands for - the call is left as it is "
                    "and has to be rewritten manually")
                return code

            self.config_parser.print_log_message('DEBUG',
                f"informix_connector: convert_operator_functions: {code[match.start():closing + 1].strip()} "
                f"converted to {replacement}")
            code = code[:match.start()] + replacement + code[closing + 1:]

    def clause_end(self, masked, depths, start, depth):
        """
        Where the clause of a query beginning at the given position ends.

        A clause ends at the keyword which begins the next one, at the semicolon which ends the
        statement, or at the parenthesis which closes the query it belongs to - all of them read
        on the depth of the query itself, so that a subquery inside the clause ends nothing. The
        semicolon was not read before, and the ` ;` which Informix writes at the end of the text
        of a view was taken for a part of the last condition: it ended up inside the ON clause the
        conversion built, as `ON ((x1.country_code = x0.country_code ) ;)`.
        """
        for keyword in re.finditer(self.CLAUSES_AFTER_FROM, masked[start:]):
            position = start + keyword.start()
            if depths[position] == depth:
                return position
        for position in range(start, len(masked)):
            if depths[position] < depth or (masked[position] == ';' and depths[position] == depth):
                return position
        return len(masked)

    def outer_join_table_reference(self, item):
        """
        The name a column of a table reference is addressed by - its alias, or the table itself
        when it was given none.
        """
        reference = re.sub(r'(?is)\s+AS\s+', ' ', item.strip())
        parts = reference.split()
        if len(parts) > 1 and not re.match(r'(?i)^(ONLY|OUTER)$', parts[-2]):
            return parts[-1].strip('"')
        return parts[0].split('.')[-1].strip('"')

    def parse_outer_join_items(self, from_list):
        """
        The entries of a FROM clause of Informix as a list of nodes.

        An entry marked with OUTER is the subordinate table of an outer join, and the tables it
        encloses form a join of their own: `OUTER(b, c)` is the join of b and c outer-joined as a
        whole, `OUTER(b, OUTER(c))` has c outer-joined to b inside it. Every node carries the
        names its columns are addressed by, so that a condition of the WHERE clause can be
        attributed to the join it belongs to.
        """
        nodes = []
        for item in self.split_top_level_commas(from_list):
            outer_match = re.match(r'(?is)^OUTER\s*(\((?P<parenthesized>.*)\)|(?P<plain>.+))$', item.strip())
            if not outer_match:
                nodes.append({'outer': False, 'text': item.strip(),
                              'references': [self.outer_join_table_reference(item)], 'children': []})
                continue
            content = outer_match.group('parenthesized')
            if content is None:
                content = outer_match.group('plain')
            children = self.parse_outer_join_items(content)
            nodes.append({'outer': True, 'text': None, 'children': children,
                          'references': [name for child in children for name in child['references']]})
        return nodes

    def build_outer_join_tree(self, nodes, predicates):
        """
        The FROM clause of the nodes written with the explicit joins of PostgreSQL, and the
        conditions which were used for them.

        A condition of the WHERE clause belongs to the join of the last node it reads: that is the
        join which may leave the row of that node empty, and Informix applies such a condition
        while it joins - a row of the dominant table is kept even when the condition fails, which
        is what makes it a condition of the join and not a filter of the query.
        """
        used = set()

        def claim(node, index_of, is_outer):
            """
            The conditions of the WHERE clause which belong to the join of this node.

            A condition belongs to it when it reads the node and reads no node which is joined
            after it. For an outer join a condition on the subordinate table alone belongs to the
            join as well - that is how Informix reads it, and leaving it in the WHERE clause of
            PostgreSQL would undo the outer join. For an inner join it does not: `x2.id = 100` is
            a filter of the query, and the query reads better with it in its WHERE clause.
            """
            claimed = []
            for position, predicate in enumerate(predicates):
                if position in used:
                    continue
                names = [name for name in predicate['references'] if name in index_of]
                if not names or not any(name in node['references'] for name in names):
                    continue
                if max(index_of[name] for name in names) != index_of[node['references'][-1]]:
                    ## the condition reads a table which is joined later - it belongs to that join
                    continue
                if not is_outer and all(name in node['references'] for name in names):
                    continue
                claimed.append(position)
            used.update(claimed)
            return ' AND '.join(predicates[position]['text'] for position in claimed)

        def build(nodes):
            ## the order the nodes are joined in decides which join a condition belongs to
            index_of = {}
            for order, node in enumerate(nodes):
                for name in node['references']:
                    index_of[name] = order

            ## The joins inside a group are built first, so that a condition between two tables of
            ## the group is claimed by the join between them and not by the join of the whole
            ## group: `outer(b, c)` is the join of b and c, outer-joined as one.
            parts = []
            for node in nodes:
                if node['children']:
                    text, count = build(node['children'])
                    parts.append(f"({text})" if count > 1 else text)
                else:
                    parts.append(node['text'])

            joined = parts[0]
            for node, part in zip(nodes[1:], parts[1:]):
                condition = claim(node, index_of, node['outer'])
                if node['outer']:
                    joined += f" LEFT OUTER JOIN {part} ON ({condition or 'TRUE'})"
                    if not condition:
                        self.config_parser.print_log_message('WARNING',
                            "informix_connector: convert_outer_joins: An outer join of the source has no condition in the "
                            "WHERE clause - it is written as 'ON (TRUE)', which keeps every row of both tables.")
                elif condition:
                    joined += f" INNER JOIN {part} ON ({condition})"
                else:
                    ## the comma of the source joined the two without a condition. It cannot stay
                    ## a comma: a join binds tighter than a comma, so the ON clause of a join
                    ## written behind one could not read the tables in front of it.
                    joined += f" CROSS JOIN {part}"
            return joined, len(nodes)

        from_clause, _ = build(nodes)
        remaining = [predicate['text'] for position, predicate in enumerate(predicates) if position not in used]
        return from_clause, remaining

    def convert_outer_joins(self, code):
        """
        The outer joins of Informix, written as OUTER in the FROM clause, as the explicit joins of
        PostgreSQL.

        Informix marks the subordinate table of an outer join in the FROM clause and writes the
        condition of the join into the WHERE clause: `from orders x0, outer(order_items x1) where
        x1.order_id = x0.order_id`. PostgreSQL knows neither the marker nor that reading of the
        WHERE clause and answered the view with `syntax error at or near "x1"`, so such a view was
        never created. The marked table becomes the right side of a LEFT OUTER JOIN and the
        conditions which read it become the ON clause of that join - they have to move, because a
        condition on the subordinate table in the WHERE clause of PostgreSQL would undo the outer
        join and turn it back into an inner one.

        A WHERE clause whose conditions cannot be attributed - an OR spanning the subordinate
        table - is left as it is and reported, so that the view fails to be created instead of
        being created with another meaning.
        """
        for _ in range(20):
            masked, depths = self.scan_sql_text(code)

            from_clause = None
            for match in re.finditer(r'(?i)\bFROM\b', masked):
                depth = depths[match.start()]
                end = self.clause_end(masked, depths, match.end(), depth)
                items = self.split_top_level_commas(code[match.end():end])
                if any(re.match(r'(?i)^OUTER\b', item) for item in items):
                    from_clause = {'start': match.end(), 'end': end, 'depth': depth}
                    break

            if from_clause is None:
                return code

            ## the WHERE clause of the same query carries the conditions of the joins
            where_start = where_end = None
            where_match = re.compile(r'(?i)\s*\bWHERE\b').match(masked, from_clause['end'])
            if where_match:
                where_start = where_match.end()
                where_end = self.clause_end(masked, depths, where_start, from_clause['depth'])

            predicates = []
            if where_start is not None:
                condition = code[where_start:where_end]
                condition_masked = masked[where_start:where_end]
                condition_depths = [depth - from_clause['depth'] for depth in depths[where_start:where_end]]
                if re.search(r'(?i)\bOR\b', ''.join(character if condition_depths[position] == 0 else ' '
                                                    for position, character in enumerate(condition_masked))):
                    self.config_parser.print_log_message('WARNING',
                        "informix_connector: convert_outer_joins: The WHERE clause of a query with an outer join of "
                        "Informix contains an OR which is not parenthesized - which of its conditions belong to the join "
                        "cannot be told, so the query is left as it is and has to be rewritten manually: "
                        f"{' '.join(code[from_clause['start']:where_end].split())}")
                    return code
                start = 0
                for keyword in re.finditer(r'(?i)\bAND\b', condition_masked):
                    if condition_depths[keyword.start()] != 0:
                        continue
                    predicates.append(condition[start:keyword.start()])
                    start = keyword.end()
                predicates.append(condition[start:])
                predicates = [{'text': text.strip(),
                               'references': re.findall(r'(?i)\b([a-zA-Z_][\w$]*)\s*\.', text)}
                              for text in predicates if text.strip()]

            nodes = self.parse_outer_join_items(code[from_clause['start']:from_clause['end']])
            joined, remaining = self.build_outer_join_tree(nodes, predicates)

            rebuilt = f" {joined} "
            if remaining:
                rebuilt += f"WHERE {' AND '.join(remaining)} "
            replaced_until = where_end if where_start is not None else from_clause['end']
            code = code[:from_clause['start']] + rebuilt + code[replaced_until:]
            self.config_parser.print_log_message('DEBUG',
                f"informix_connector: convert_outer_joins: outer join converted to {' '.join(joined.split())}")

        return code

    def convert_view_code(self, settings: dict):
        view_code = settings['view_code']
        converted_view_code = view_code
        ## before the schema is replaced - the qualifier of these calls is the system schema of
        ## Informix, and it is what tells them apart from a function of the user
        converted_view_code = self.convert_operator_functions(converted_view_code)
        converted_view_code = converted_view_code.replace(f'''"{settings['source_schema_name']}".''', f'''"{settings['target_schema_name']}".''')
        converted_view_code = self.convert_outer_joins(converted_view_code)
        converted_view_code = self.convert_matches_operator(converted_view_code)
        converted_view_code = self.apply_sql_functions_mapping(converted_view_code, settings)
        return converted_view_code

    def get_types_mapping(self, settings):
        target_db_type = settings['target_db_type']
        types_mapping = {}
        if target_db_type == 'postgresql':
            types_mapping = {
                'BLOB': 'BYTEA',
                'BOOLEAN': 'BOOLEAN',
                # BSON is the binary and JSON the textual representation of a document -
                # both become JSONB, which is the binary document type of PostgreSQL
                'BSON': 'JSONB',
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
                'JSON': 'JSONB',
                'LVARCHAR': 'VARCHAR',
                # MONEY of Informix is a DECIMAL with a fixed scale, and that is what it
                # becomes here. The MONEY type of PostgreSQL is not the same thing: it has
                # almost no operators (`operator does not exist: money >= numeric` for a
                # CHECK constraint as ordinary as `credit_limit >= 0.00`, and no `money +
                # integer` either), and it keeps the number of decimal places of the
                # lc_monetary setting instead of the declared one, so a MONEY(12,4) would
                # lose the last two digits of every value.
                'MONEY': 'NUMERIC',
                'NCHAR': 'CHAR',
                'NVARCHAR': 'VARCHAR',
                # 'SERIAL8': 'BIGSERIAL',
                # 'SERIAL': 'SERIAL',
                # SERIAL & SERIAL8 are replaced in PostgreSQL with IDENTITY columns
                'SERIAL8': 'BIGINT',
                'SERIAL': 'INTEGER',
                'BIGSERIAL': 'BIGINT',
                'SMALLFLOAT': 'REAL',
                'SMALLINT': 'SMALLINT',
                'TEXT': 'TEXT',
                'TIME': 'TIME',
                'TIMESTAMP': 'TIMESTAMP',
                'VARCHAR': 'VARCHAR',
                # A collection of Informix is an array in PostgreSQL, which keeps the
                # elements addressable instead of storing them as one string: CARDINALITY(),
                # the element access and the containment operators go on working. The
                # element type is not carried over - a collection may hold a row type or
                # another collection, and TEXT holds all of them.
                'COLLECTION': 'TEXT[]',
                'LIST': 'TEXT[]',
                'MULTISET': 'TEXT[]',
                'SET': 'TEXT[]',
                # a row type is a record and not a collection - it stays the text of its
                # literal, PostgreSQL has no anonymous record type for a column
                'ROW': 'TEXT',
                'IDSSECURITYLABEL': 'TEXT',
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
        ## MONEY belongs here as well - it is a DECIMAL of Informix and its precision and
        ## scale have to reach the target, otherwise the column ends up as an unconstrained
        ## NUMERIC instead of NUMERIC(12,2)
        numeric_types = ['BIGINT', 'INTEGER', 'INT', 'TINYINT', 'SMALLINT', 'FLOAT', 'DOUBLE PRECISION', 'DECIMAL', 'NUMERIC', 'MONEY']
        return column_type.upper() in numeric_types

    def fetch_indexes(self, settings):
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']
        ## a function based index calls a routine which the migration puts into the target
        ## schema - the owner reported by the source catalog does not exist in the target
        target_table_schema = settings.get('target_table_schema') or source_table_schema
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
                self.config_parser.print_log_message('DEBUG', f"informix_connector: fetch_indexes: Processing index: {index}")
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
                    self.config_parser.print_log_message('DEBUG', f"informix_connector: fetch_indexes: Index {index_name}: index_keys: procedure_id={procedure_id}, procedure_colnos={procedure_colnos}")
                # Get column names for each colno

                columns = []
                if colnos:
                    self.config_parser.print_log_message('DEBUG3', f"informix_connector: fetch_indexes: Index {index_name}: Extracted colnos: {colnos}")
                    for colno in colnos:
                        ## Informix stores the column number of a descending index column
                        ## negated - the column itself is found under its absolute value
                        descending = colno < 0
                        cursor.execute(f"SELECT colname FROM syscolumns WHERE colno = {abs(colno)} AND tabid = {source_table_id}")
                        colname = cursor.fetchone()
                        if colname is None:
                            self.config_parser.print_log_message('WARNING', f"informix_connector: fetch_indexes: Index {index_name}: No column with colno {abs(colno)} found in table {source_table_id} - the key is left out of the index.")
                            continue
                        columns.append({'column_name': colname[0].strip(), 'descending': descending})

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
                        self.config_parser.print_log_message('DEBUG', f"informix_connector: fetch_indexes: Index {index_name}: Function-based index found: {index_name} on procedure {procedure_name}")
                        function_based_index = True

                if procedure_colnos:
                    # Get the column names for the function-based index
                    proc_columns = []
                    for colno in procedure_colnos:
                        cursor.execute(f"SELECT colname FROM syscolumns WHERE colno = {abs(colno)} AND tabid = {source_table_id}")
                        colname = cursor.fetchone()
                        if colname is None:
                            self.config_parser.print_log_message('WARNING', f"informix_connector: fetch_indexes: Index {index_name}: No column with colno {abs(colno)} found in table {source_table_id} - the key is left out of the function based index.")
                            continue
                        proc_columns.append(colname[0].strip())
                    procedure_columns = ', '.join(proc_columns)
                    self.config_parser.print_log_message('DEBUG', f"informix_connector: fetch_indexes: Index {index_name}: Function-based index columns: {procedure_columns}")

                target_index_type = "PRIMARY KEY" if index_type == 'P' else "UNIQUE" if index_type == 'U' else "INDEX"
                ## PostgreSQL accepts an ordering keyword in CREATE INDEX but not in the
                ## column list of a PRIMARY KEY constraint
                keep_ordering = target_index_type != "PRIMARY KEY"
                index_columns = ', '.join([
                    f'"{col["column_name"]}" DESC' if col['descending'] and keep_ordering else f'"{col["column_name"]}"'
                    for col in columns
                ])
                self.config_parser.print_log_message('DEBUG', f"informix_connector: fetch_indexes: Index {index_name}: Columns list: {index_columns}, index type: {index_type}, clustered: {index[2]}")

                table_indexes[order_num] = {
                    'index_name': index_name,
                    'index_type': target_index_type,
                    'index_owner': index_owner,
                    'index_columns': index_columns if not function_based_index else f'''"{target_table_schema}".{procedure_name}({procedure_columns})''',
                    'index_keys': index_keys,
                    'index_comment': '',
                    'is_function_based': 'YES' if function_based_index else 'NO',
                }
                order_num += 1

            cursor.close()
            self.disconnect()
            return table_indexes

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"informix_connector: fetch_indexes: Error executing query: {query}")
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

        # self.config_parser.print_log_message('DEBUG', f"informix_connector: fetch_constraints: Reading constraints for {target_table_name}")
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
                    self.config_parser.print_log_message('DEBUG', f"informix_connector: fetch_constraints: Processing table: {source_table_name} ({source_table_id}) - foreign key: {constraint_name}")

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
                    self.config_parser.print_log_message('DEBUG', f"informix_connector: fetch_constraints: Source table {source_table_name}: FOREIGN KEY details: {fk_details}")

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
                        AND c.constrname = '{constraint_name}'
                        AND c.constrtype = 'C'
                        AND ck.type in ('T', 's')
                        ORDER BY ck.seqno
                    """
                    cursor.execute(find_ck_query)
                    ## Informix stores the text of a check constraint split into 32 byte
                    ## pieces - all rows have to be read and put together in seqno order
                    ck_details = cursor.fetchall()
                    self.config_parser.print_log_message('DEBUG', f"informix_connector: fetch_constraints: Source table {source_table_name}: CHECK constraint details: {ck_details}")

                    create_constraint_query = ''.join([ck[0] for ck in ck_details if ck[0] is not None]).strip()

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
            AND mode IN ({', '.join(f"'{mode}'" for mode in self.USER_ROUTINE_MODES)})
            ORDER BY procname
        """
        self.config_parser.print_log_message('DEBUG3', f"informix_connector: fetch_funcproc_names: Fetching function/procedure names for schema {schema}")
        self.config_parser.print_log_message('DEBUG3', f"informix_connector: fetch_funcproc_names: Query: {query}")
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
        procbody_str = ''.join([str(body[0]) for body in procbody])
        return procbody_str

    ## PostgreSQL types which accept a length or precision - behind every other type the
    ## modifier taken over from Informix has to go, the target rejects the whole routine
    ## with "type modifier is not allowed for type ..." otherwise
    TYPES_WITH_MODIFIER = ('char', 'character', 'varchar', 'character varying', 'nchar',
                           'numeric', 'decimal', 'time', 'timestamp', 'interval',
                           'bit', 'varbit', 'bit varying')
    ## these type names are ordinary SQL keywords as well - replacing them in the code of a
    ## routine would destroy statements like "UPDATE ... SET x = 1" or a ROW(...) constructor
    TYPES_NOT_REPLACED_IN_CODE = ('SET', 'ROW', 'LIST', 'MULTISET', 'COLLECTION')

    ## the return type of a routine, with the length or precision it may carry -
    ## 'VARCHAR(120)' and 'MONEY(14,2)' as well as a bare 'INTEGER'. The nesting of one
    ## level is needed for 'TABLE (item_count INTEGER, total MONEY(12,2))', which
    ## convert_returning_clause() builds for a function returning several values.
    RETURN_TYPE_PATTERN = r'\w+(?:\s*\((?:[^()]|\([^()]*\))*\))?'

    ## the error number Informix uses for an exception raised by a routine itself - its
    ## counterpart is the SQLSTATE P0001, which RAISE EXCEPTION of PL/pgSQL already sets
    INFORMIX_USER_EXCEPTION_ERROR = '-746'

    MERGE_PLACEHOLDER = '__CREDATIV_PG_MIGRATOR_MERGE_{}__'
    GLOBAL_PLACEHOLDER = '__CREDATIV_PG_MIGRATOR_GLOBAL_{}__'
    ## prefix of the customized option a global variable of Informix is migrated to - a
    ## customized option must carry one, and a single namespace for the whole migration
    ## matches the source, where a global variable is shared by the entire session
    GLOBAL_VARIABLE_NAMESPACE = 'credativ_pg_migrator'

    def convert_returning_clause(self, code):
        """
        Convert the RETURNING clause of an Informix function.

        A function of Informix returns several values at once and may give each of them a
        name:

            RETURNING INTEGER AS item_count, MONEY(12,2) AS total, DATE AS ord_date;
            ...
            RETURN v_cnt, v_tot, v_dt;

        PostgreSQL expresses that as a function returning a table, so the clause becomes
        'RETURNING TABLE (item_count INTEGER, total MONEY(12,2), ord_date DATE)' - the
        header conversion turns the keyword into RETURNS afterwards - and every RETURN of
        the routine becomes 'RETURN QUERY SELECT ...'. An unnamed value gets the name
        'column<n>', the position is what identifies it in Informix as well.

        'RETURN ... WITH RESUME' of an iterator function hands one row to the caller and
        continues where it left off. RETURN QUERY appends its rows in the same way, so the
        loop of the source keeps working - what the caller receives is the whole set
        instead of one row per call.

        A function returning a single value keeps its plain type; the name Informix allows
        there has no counterpart and would only turn a scalar function into a table one.
        """
        returning_match = re.search(r'(?is)\bRETURNING\b(?P<clause>[^;]+);', code)
        if not returning_match:
            return code

        items = []
        for position, item in enumerate(self.split_top_level_commas(returning_match.group('clause')), start=1):
            item_match = re.match(r'(?is)^(?P<type>.+?)(?:\s+AS\s+(?P<name>\w+))?$', item.strip())
            if not item_match:
                continue
            items.append((item_match.group('name') or f'column{position}', item_match.group('type').strip()))

        if not items:
            return code

        if len(items) == 1:
            replacement = f"RETURNING {items[0][1]};"
        else:
            columns = ', '.join(f"{name} {data_type}" for name, data_type in items)
            replacement = f"RETURNING TABLE ({columns});"
            self.config_parser.print_log_message('INFO',
                f"informix_connector: convert_funcproc_code: The function returns {len(items)} values and is migrated as a function returning a table ({columns}). Its callers have to read it as a table, 'SELECT * FROM the_function(...)'.")

        code = code[:returning_match.start()] + replacement + code[returning_match.end():]

        if len(items) > 1:
            code = re.sub(r'(?is)\bRETURN\s+(?P<values>[^;]+?)(?:\s+WITH\s+RESUME)?\s*;',
                          lambda match: f"RETURN QUERY SELECT {match.group('values').strip()};",
                          code)
        return code

    def extract_global_variables(self, code, target_db_type):
        """
        Replace the global variables of a routine with customized options of the session.

        'DEFINE GLOBAL <name> <type> DEFAULT <value>' declares a variable which lives for
        the whole session and is shared by every routine declaring it - one routine sets
        it, another one reads it. PL/pgSQL has nothing of that kind, its variables belong
        to a single call, and the declaration is not even valid inside a DECLARE section,
        so the routine failed with 'syntax error at or near "BOOLEAN"'.

        A customized option ('<namespace>.<name>') has exactly the wanted lifetime and
        visibility, so every read of the variable becomes current_setting() with the
        declared default as its fallback, and every assignment becomes set_config().
        Both are returned as snippets behind a placeholder, because the conversion of the
        routine replaces every occurrence of 'current' with 'CURRENT_TIMESTAMP' and would
        otherwise turn current_setting() into CURRENT_TIMESTAMP_setting().
        """
        snippets = []
        declarations = []

        def take_declaration(match):
            declarations.append({
                'name': match.group('name'),
                'type': self.convert_data_types_in_code(match.group('type').strip(), target_db_type),
                'default': (match.group('default') or '').strip(),
            })
            return ''

        code = re.sub(
            r'(?im)^[ \t]*DEFINE\s+GLOBAL\s+(?P<name>\w+)\s+(?P<type>.+?)(?:\s+DEFAULT\s+(?P<default>.+?))?\s*;[ \t]*\n?',
            take_declaration,
            code)

        if re.search(r'(?i)\bDEFINE\s+GLOBAL\b', code):
            self.config_parser.print_log_message('WARNING',
                "informix_connector: extract_global_variables: A 'DEFINE GLOBAL' declaration could not be read - only one variable per declaration is supported. It is left in the code and has to be migrated manually.")

        def placeholder(text):
            snippets.append(text)
            return self.GLOBAL_PLACEHOLDER.format(len(snippets) - 1)

        for declaration in declarations:
            name = declaration['name']
            option = f"{self.GLOBAL_VARIABLE_NAMESPACE}.{name.lower()}"
            stored = f"nullif(current_setting('{option}', true), '')"
            read = f"coalesce({stored}, {declaration['default']})" if declaration['default'] else stored
            read = f"{read}::{declaration['type']}"

            self.config_parser.print_log_message('WARNING',
                f"informix_connector: extract_global_variables: The global variable {name} is migrated as the customized option '{option}' - PostgreSQL has no global variable of a routine. It keeps the lifetime and the visibility of the Informix original, but it is read and written as text, so the behaviour has to be verified.")

            ## the assignments first, otherwise the variable on the left hand side would be
            ## replaced by the expression reading it - a read on the right hand side is
            ## substituted here as well, it is no longer visible afterwards
            code = re.sub(
                rf"(?im)^[ \t]*(?:LET\s+)?{re.escape(name)}\s*:?=\s*(?P<value>.+?)\s*;[ \t]*$",
                lambda match: ';' + placeholder(
                    f"PERFORM set_config('{option}', ({re.sub(rf'(?i)\b{re.escape(name)}\b', read, match.group('value').strip())})::text, false)") + ';',
                code)

            ## whatever is left is a read of the variable
            code = re.sub(rf'(?i)\b{re.escape(name)}\b', lambda match: placeholder(read), code)

        return code, snippets

    def restore_global_variables(self, code, snippets):
        """ Put the converted global variable accesses back in place of their placeholders """
        for index, snippet in enumerate(snippets):
            code = code.replace(self.GLOBAL_PLACEHOLDER.format(index), snippet)
        return code

    def convert_data_types_in_code(self, code, target_db_type):
        """
        Translate the Informix data types used inside the code of a routine.

        The parameter list, the DECLARE section and a cast name data types just like a
        table definition does, but the conversion of a routine never looked at them - only
        a few of them were covered by a rule of their own. So a type PostgreSQL does not
        know under that name reached the target unchanged, and so did a length behind a
        type which does not accept one ("type modifier is not allowed for type money" for
        a parameter declared as MONEY(12,2)).

        The same mapping as for a table column is used, minus the type names which are
        also SQL keywords - see TYPES_NOT_REPLACED_IN_CODE.
        """
        types_mapping = self.get_types_mapping({'target_db_type': target_db_type})

        ## the qualifiers of an Informix DATETIME / INTERVAL are not a length and have no
        ## counterpart in PostgreSQL - the type is used without them
        code = re.sub(r'(?i)\bDATETIME\s+\w+(?:\s*\(\s*\d+\s*\))?\s+TO\s+\w+(?:\s*\(\s*\d+\s*\))?', 'TIMESTAMP', code)
        code = re.sub(r'(?i)\bINTERVAL\s+\w+(?:\s*\(\s*\d+\s*\))?\s+TO\s+\w+(?:\s*\(\s*\d+\s*\))?', 'INTERVAL', code)

        def replace_type(match):
            target_type = types_mapping[match.group('type').upper()]
            modifier = match.group('modifier') or ''
            if modifier and target_type.lower() not in self.TYPES_WITH_MODIFIER:
                self.config_parser.print_log_message('DEBUG',
                    f"informix_connector: convert_data_types_in_code: Removed the length {modifier.strip()} behind {match.group('type')} - PostgreSQL accepts none for {target_type}.")
                modifier = ''
            return target_type + modifier

        ## longest name first, so that SERIAL8 is not matched as SERIAL
        for source_type in sorted(types_mapping, key=len, reverse=True):
            if source_type in self.TYPES_NOT_REPLACED_IN_CODE:
                continue
            code = re.sub(
                rf"(?i)\b(?P<type>{re.escape(source_type)})\b(?P<modifier>\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\))?",
                replace_type,
                code)
        return code

    def convert_merge_statement(self, merge_sql):
        """
        Convert one Informix MERGE statement into the PostgreSQL form.

        PostgreSQL knows MERGE since version 15 and the syntax is almost the same. Two
        things differ: the target of the SET clause may not be qualified with the alias
        of the merged table ("SET target columns cannot be qualified with the relation
        name"), and Informix reads a single row from the pseudo table sysmaster:sysdual,
        for which PostgreSQL needs no FROM clause at all.
        """
        merge_sql = merge_sql.strip().rstrip(';').strip()
        merge_sql = re.sub(r'(?i)\s+FROM\s+(?:\w+:)?sysdual\b', '', merge_sql)

        def strip_set_alias(match):
            ## only the assignment targets - a column at the start of the SET list or
            ## behind a comma - lose the alias, the assigned values keep theirs
            return match.group(1) + re.sub(r'(^|,)(\s*)[A-Za-z_]\w*\.(?=[A-Za-z_]\w*\s*=)', r'\1\2', match.group(2))

        merge_sql = re.sub(
            r'(?is)(\bWHEN\s+(?:NOT\s+)?MATCHED\b.*?\bUPDATE\s+SET\b)(.*?)(?=\bWHEN\b|$)',
            strip_set_alias,
            merge_sql)
        return merge_sql + ';'

    def extract_merge_statements(self, code):
        """
        Take every MERGE statement out of the routine and leave a placeholder behind.

        The conversion below works line by line and inserts a statement separator in
        front of keywords such as UPDATE and INSERT. Inside a MERGE those keywords belong
        to the WHEN MATCHED / WHEN NOT MATCHED branches, so the statement would be torn
        into pieces. The header detection would break as well: it ends the header at the
        first ');', and an Informix routine header is not closed by a semicolon, so the
        whole MERGE ended up in front of 'AS $$' instead of in the body.

        The placeholder is a single statement of its own, which is what makes the header
        end where it should. It is put back in restore_merge_statements().
        """
        merge_statements = []

        def replace(match):
            merge_statements.append(self.convert_merge_statement(match.group(0)))
            return f';{self.MERGE_PLACEHOLDER.format(len(merge_statements) - 1)};'

        code = re.sub(r'(?is)\bMERGE\s+INTO\b.*?;', replace, code)
        return code, merge_statements

    def restore_merge_statements(self, code, merge_statements):
        """ Put the converted MERGE statements back in place of their placeholders """
        for index, statement in enumerate(merge_statements):
            code = code.replace(self.MERGE_PLACEHOLDER.format(index), statement)
        return code

    def convert_funcproc_code(self, settings):
        funcproc_code = settings['funcproc_code']
        target_db_type = settings['target_db_type']
        source_schema_name = settings['source_schema_name']
        target_schema_name = settings['target_schema_name']
        table_list = settings['table_list']
        view_list = settings['view_list']

        function_immutable = ''

        if target_db_type == 'postgresql':
            postgresql_code = funcproc_code

            # Normalize the CREATE clause before anything else looks at it. Informix knows
            # the variants CREATE DBA PROCEDURE / CREATE DBA FUNCTION (executable only by a
            # user holding the DBA privilege) and CREATE ... IF NOT EXISTS, and every rule
            # converting the header expects a plain "CREATE PROCEDURE" / "CREATE FUNCTION".
            # Without this the whole header stays in the code as it was in Informix
            # ("create dba procedure informix.systdist(...) returning int, date, ...").
            # PostgreSQL has no DBA-only routine - restrict the EXECUTE privilege of such a
            # function in the target instead, it is granted to PUBLIC by default.
            normalized_code, dba_replacements = re.subn(
                r'\bCREATE\s+DBA\s+(PROCEDURE|FUNCTION)\b',
                r'CREATE \1',
                postgresql_code,
                flags=re.IGNORECASE)
            if dba_replacements:
                postgresql_code = normalized_code
                self.config_parser.print_log_message('WARNING',
                    "informix_connector: convert_funcproc_code: Routine is declared as a DBA routine - PostgreSQL has no equivalent, the keyword DBA is removed. Check the EXECUTE privilege of the created function, PostgreSQL grants it to PUBLIC.")

            normalized_code, ine_replacements = re.subn(
                r'\bCREATE\s+(PROCEDURE|FUNCTION)\s+IF\s+NOT\s+EXISTS\b',
                r'CREATE \1',
                postgresql_code,
                flags=re.IGNORECASE)
            if ine_replacements:
                postgresql_code = normalized_code
                self.config_parser.print_log_message('DEBUG',
                    "informix_connector: convert_funcproc_code: Removed 'IF NOT EXISTS' from the CREATE clause - PostgreSQL does not support it for routines.")

            # A function returning several values becomes one returning a table
            postgresql_code = self.convert_returning_clause(postgresql_code)

            # Global variables have no PL/pgSQL counterpart and become customized options
            postgresql_code, global_variables = self.extract_global_variables(postgresql_code, target_db_type)

            # A MERGE statement must not be touched by the line based conversion below
            postgresql_code, merge_statements = self.extract_merge_statements(postgresql_code)
            if merge_statements:
                self.config_parser.print_log_message('DEBUG',
                    f"informix_connector: convert_funcproc_code: {len(merge_statements)} MERGE statement(s) converted separately from the rest of the routine.")

            # Replace empty lines with ";"
            postgresql_code = re.sub(r'^\s*$', ';\n', postgresql_code, flags=re.MULTILINE)
            # Split the code based on "\n
            commands = [command.strip() for command in postgresql_code.split('\n') if command.strip()]
            postgresql_code = ''
            line_number = 0
            # self.config_parser.print_log_message('DEBUG', 'informix_connector: convert_funcproc_code: Processing step 1: Splitting code into commands and replacing keywords')

            for command in commands:
                ## A comment behind a statement has to be bracketed here, before the code is
                ## split on the semicolon: 'RETURN;   -- being deleted; do not touch it' would
                ## otherwise be torn apart at the semicolon inside the comment and its second
                ## half would become a statement of its own.
                comment_match = re.search(r'--', command)
                if comment_match and not command.startswith('--') and command[:comment_match.start()].count("'") % 2 == 0:
                    comment_text = command[comment_match.start() + 2:].strip().rstrip(';')
                    command = f"{command[:comment_match.start()].rstrip()} /* {comment_text} */"

                if command.startswith('--'):
                    command = command.replace(command, f"\n/* {command.strip().rstrip(';')} */;")
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

                ## The outer join of Informix is converted the same way as in a view - see
                ## convert_outer_joins(). Replacing the comma and the keyword by 'LEFT OUTER JOIN'
                ## alone, as this did, left the parentheses of 'OUTER(t x)' standing and the join
                ## without the ON clause it needs, and it left the condition of the join in the
                ## WHERE clause, where PostgreSQL would undo the outer join again.
                if re.search(r'(?i)(,|\bFROM\b)\s*\bOUTER\b', command):
                    command = self.convert_outer_joins(command)

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
            # self.config_parser.print_log_message('DEBUG', 'informix_connector: convert_funcproc_code: Processing step 2: Splitting code into commands based on ";", reformating code and removing unnecessary spaces')

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

            self.config_parser.print_log_message('DEBUG3', f'informix_connector: convert_funcproc_code: [1] postgresql_code: {postgresql_code}')

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

            # Replace source_schema_name in the function/procedure name with target_schema_name.
            # Informix keeps the CREATE statement as it was written, so the owner in front of the
            # routine name is regularly unquoted - the quotes have to be optional here, otherwise
            # the routine keeps the schema of the source and the target reports
            # 'schema "..." does not exist'.
            postgresql_code = re.sub(
                rf'CREATE\s+(FUNCTION|PROCEDURE)\s+"?{re.escape(source_schema_name)}"?\.',
                rf'CREATE \1 "{target_schema_name}".',
                postgresql_code,
                flags=re.IGNORECASE
            )

            # A routine qualified with any other schema than the migrated one is left as it is -
            # only reported, because that schema is not part of this migration.
            foreign_schema_match = re.search(
                r'CREATE\s+(?:FUNCTION|PROCEDURE)\s+"?(\w+)"?\.',
                postgresql_code,
                flags=re.IGNORECASE)
            if foreign_schema_match and foreign_schema_match.group(1).lower() != target_schema_name.lower():
                self.config_parser.print_log_message('WARNING',
                    f"informix_connector: convert_funcproc_code: Routine is created in the schema \"{foreign_schema_match.group(1)}\" - neither the migrated schema \"{source_schema_name}\" nor the target schema \"{target_schema_name}\". It is kept as it is and will fail unless that schema exists in the target.")

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
                self.config_parser.print_log_message('DEBUG', f'informix_connector: convert_funcproc_code: code contains WITH HOLD but no COMMIT or ROLLBACK')

            self.config_parser.print_log_message('DEBUG3', f'informix_connector: convert_funcproc_code: Processing step 4: Converting FOREACH cursor FOR loop to FOR loop')
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

            self.config_parser.print_log_message('DEBUG3', f'informix_connector: convert_funcproc_code: Processing step 5: Header for Procedures, Adding AS $$ and BEGIN to the code')
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
            # The return type may carry a length or a precision - 'RETURNING VARCHAR(120);'
            # and 'RETURNING MONEY(14,2);' are as usual as a bare 'RETURNING INTEGER;'
            header_match = re.search(rf'CREATE FUNCTION.*?RETURNING\s+{self.RETURN_TYPE_PATTERN}\s*;?', postgresql_code, flags=re.DOTALL | re.IGNORECASE)
            if header_match:
                header_end = header_match.end()
                postgresql_code_part = re.sub(r'RETURNING', 'RETURNS', postgresql_code[:header_end], flags=re.DOTALL | re.IGNORECASE)
                if ';' in postgresql_code_part:
                    postgresql_code_part = re.sub(r';', ' AS $$\n', postgresql_code_part, flags=re.DOTALL | re.IGNORECASE)
                else:
                    postgresql_code_part += ' AS $$\n'
                postgresql_code = postgresql_code_part + postgresql_code[header_end:]

            header_match = re.search(rf'\s*RETURNING\s+{self.RETURN_TYPE_PATTERN}\s*;?', postgresql_code, flags=re.DOTALL | re.IGNORECASE)
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
            returning_matches = re.finditer(rf'RETURNING\s+({self.RETURN_TYPE_PATTERN})\s*;', postgresql_code, flags=re.DOTALL | re.IGNORECASE | re.MULTILINE)
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

            # Back in place before step 7, so that the tables of a MERGE are qualified
            # with the target schema like the tables of every other statement
            postgresql_code = self.restore_merge_statements(postgresql_code, merge_statements)
            postgresql_code = self.restore_global_variables(postgresql_code, global_variables)

            self.config_parser.print_log_message('DEBUG3', f'informix_connector: convert_funcproc_code: Processing step 7: Replacing source schema and table names with target schema and table names ({len(table_list)} tables)')

            for table in table_list:
                # self.config_parser.print_log_message('DEBUG3', f'informix_connector: convert_funcproc_code: Replacing table {table} from schema {source_schema_name} to {target_schema_name}')

                source_table_pattern = re.compile(rf'("{source_schema_name}"\.)?"{table}"')
                target_table_name = f'"{target_schema_name}"."{table}"'
                postgresql_code = source_table_pattern.sub(target_table_name, postgresql_code)

                source_table_pattern = re.compile(rf'\b{table}\b')
                postgresql_code = source_table_pattern.sub(target_table_name, postgresql_code)

            for view in view_list:
                # self.config_parser.print_log_message('DEBUG3', f'informix_connector: convert_funcproc_code: Replacing view {view} from schema {source_schema_name} to {target_schema_name}')

                source_view_pattern = re.compile(rf'("{source_schema_name}"\.)?"{view}"')
                target_view = f'"{target_schema_name}"."{view}"'
                postgresql_code = source_view_pattern.sub(target_view, postgresql_code)

                source_view_pattern = re.compile(rf'\b{view}\b')
                postgresql_code = source_view_pattern.sub(target_view, postgresql_code)

            # Remove second occurrence of "target_schema_name" in %TYPE declarations
            postgresql_code = re.sub(
                                    rf'("{target_schema_name}"\."\w+"\.)"{target_schema_name}"\.("\w+"%TYPE)',
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

            self.config_parser.print_log_message('DEBUG3', 'informix_connector: convert_funcproc_code: Processing step 8: Handling ON EXCEPTION blocks')
            # some procs /funcs have ON EXCEPTION block, some of them several times
            if "ON EXCEPTION" in postgresql_code:
                exception_lines = [line for line in postgresql_code.split('\n') if 'ON EXCEPTION' in line]
                commentedout_exception_occurences = 0
                for line in exception_lines:
                    line = line.strip()
                    if line.startswith("/*"):
                        commentedout_exception_occurences += 1

                live_exception_occurences = len(exception_lines) - commentedout_exception_occurences
                self.config_parser.print_log_message('DEBUG3', f'informix_connector: convert_funcproc_code: Found {len(exception_lines)} ON EXCEPTION occurences, {commentedout_exception_occurences} commented out, {live_exception_occurences} live')
                if live_exception_occurences > 0:

                    for i in range(live_exception_occurences):
                        #### handle ON EXCEPTION block in scope of the main BEGIN - END block
                        # Split the postgresql_code by lines
                        lines = postgresql_code.split('\n')

                        # Find the first occurrence of BEGIN
                        begin_index = next((i for i, line in enumerate(lines) if 'BEGIN' in line), None)
                        self.config_parser.print_log_message('DEBUG3', f'informix_connector: convert_funcproc_code: ON EXCEPTION - begin_index: {begin_index}')

                        if begin_index is not None:
                            # Find the ON EXCEPTION - END EXCEPTION block that follows the first BEGIN
                            exception_start_index = next((i for i, line in enumerate(lines[begin_index:], start=begin_index) if 'ON EXCEPTION' in line), None)
                            exception_end_index = next((i for i, line in enumerate(lines[begin_index:], start=begin_index) if 'END EXCEPTION;' in line), None)

                            # Ensure that exception_start_index is immediately after begin_index
                            if exception_start_index is not None and exception_start_index != begin_index + 1:
                                self.config_parser.print_log_message('DEBUG3', 'informix_connector: convert_funcproc_code: ON EXCEPTION does not immediately follow BEGIN, trying LOOP occurence')

                                ## try LOOP - END LOOP occurence
                                # loop_begin_index = next((i for i, line in enumerate(lines) if 'LOOP' in line), None)
                                loop_begin_index = next((i for i, line in enumerate(lines) if 'LOOP' in line and i + 1 < len(lines) and 'ON EXCEPTION' in lines[i + 1]), None)

                                self.config_parser.print_log_message('DEBUG3', f'informix_connector: convert_funcproc_code: loop_begin_index: {loop_begin_index}')

                                if loop_begin_index is not None:
                                    # Find the ON EXCEPTION - END EXCEPTION block that follows the first BEGIN
                                    exception_start_index = next((i for i, line in enumerate(lines[loop_begin_index:], start=loop_begin_index) if 'ON EXCEPTION' in line), None)
                                    exception_end_index = next((i for i, line in enumerate(lines[loop_begin_index:], start=loop_begin_index) if 'END EXCEPTION' in line), None)

                                    # Ensure that exception_start_index is immediately after loop_begin_index
                                    if exception_start_index is not None and exception_start_index != loop_begin_index + 1:
                                        self.config_parser.print_log_message('DEBUG3', 'informix_connector: convert_funcproc_code: ON EXCEPTION does not immediately follow LOOP command')

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

                                ## The exception handler belongs in front of the END of the
                                ## routine, which is the line followed by the language clause.
                                ## This condition used to be written as a plain string
                                ## containing '{function_immutable}' instead of an f-string,
                                ## so it never matched anything - and because the block had
                                ## already been cut out above, the whole ON EXCEPTION handler
                                ## was silently dropped from every routine which had one.
                                end_index = next((i for i, line in enumerate(lines)
                                                  if line.strip().startswith('END;')
                                                  and i + 1 < len(lines)
                                                  and lines[i + 1].lstrip().startswith('$$ LANGUAGE')), None)
                                if end_index is not None:
                                    lines = lines[:end_index] + modified_exception_block + lines[end_index:]
                                else:
                                    self.config_parser.print_log_message('WARNING',
                                        "informix_connector: convert_funcproc_code: The ON EXCEPTION block of the routine could not be placed into the converted code - the error handling has to be added manually.")
                                    lines = lines[:exception_start_index] + [f"/* {line} */" for line in exception_block] + lines[exception_start_index:]

                                # Join the lines back into a single string
                                postgresql_code = '\n'.join(lines)

            postgresql_code = re.sub(r';;', ';', postgresql_code, flags=re.IGNORECASE)

            # RAISE EXCEPTION of Informix names the error number and the ISAM error code in
            # front of the message ('RAISE EXCEPTION -746, 0, 'text''), while PL/pgSQL expects
            # the message first and takes everything else through a USING clause.
            def convert_raise_exception(match):
                error_number = match.group('error')
                isam_error = match.group('isam')
                message = (match.group('message') or '').strip()

                if not message:
                    ## an exception raised without a message of its own would arrive in the
                    ## target as an empty error - the error number is reported instead
                    message = f"'Informix error {error_number}'"
                    self.config_parser.print_log_message('DEBUG',
                        f"informix_connector: convert_funcproc_code: RAISE EXCEPTION {error_number} has no message of its own - the error number is reported as the message.")

                statement = f"RAISE EXCEPTION {message}"

                ## -746 is the number Informix itself uses for an exception raised by a
                ## routine, and PL/pgSQL raises its own exceptions with the equivalent
                ## SQLSTATE P0001 - there is nothing to carry over. Any other number is
                ## chosen by the routine and part of what its callers check for, so it is
                ## kept in the DETAIL of the message instead of being dropped silently.
                if error_number != self.INFORMIX_USER_EXCEPTION_ERROR:
                    detail = f"Informix error {error_number}"
                    if isam_error and isam_error.strip('-0') != '':
                        detail += f", ISAM error {isam_error}"
                    statement += f" USING DETAIL = '{detail}'"
                    self.config_parser.print_log_message('DEBUG',
                        f"informix_connector: convert_funcproc_code: RAISE EXCEPTION {error_number} is raised with the standard SQLSTATE of PL/pgSQL, the error number is kept in the DETAIL of the message.")
                return statement

            postgresql_code = re.sub(
                r"(?i)\bRAISE\s+EXCEPTION\s+(?P<error>-?\d+)\s*(?:,\s*(?P<isam>-?\d+)\s*)?(?:,\s*(?P<message>'(?:[^']|'')*'))?",
                convert_raise_exception,
                postgresql_code)

            # Transaction control inside a routine. A PL/pgSQL block runs in the transaction
            # of its caller, so 'BEGIN WORK' has no counterpart at all, and PL/pgSQL knows no
            # SAVEPOINT either - its subtransaction is the BEGIN ... EXCEPTION ... END block.
            # The statements are commented out instead of being dropped, so that the reader
            # of the converted routine sees what the source did.
            ## One pass over all of them, longest first: replacing them one kind after the
            ## other would match SAVEPOINT again inside the comment just written around
            ## 'ROLLBACK WORK TO SAVEPOINT x'. Not anchored to the start of a line either -
            ## the statement also turns up behind 'EXCEPTION WHEN OTHERS THEN' once the
            ## exception block has been moved.
            commented_out_statements = []

            def comment_out_transaction_statement(match):
                commented_out_statements.append(re.sub(r'\s+', ' ', match.group(0).strip()))
                return f"/* {match.group(0).strip()} */"

            postgresql_code = re.sub(
                r'(?i)\b(?:ROLLBACK\s+WORK\s+TO\s+SAVEPOINT\s+\w+|BEGIN\s+WORK|COMMIT\s+WORK|ROLLBACK\s+WORK|SAVEPOINT\s+\w+)\s*;',
                comment_out_transaction_statement,
                postgresql_code)
            if commented_out_statements:
                self.config_parser.print_log_message('WARNING',
                    f"informix_connector: convert_funcproc_code: The transaction control of the routine was commented out ({', '.join(commented_out_statements)}) - a PL/pgSQL routine runs in the transaction of its caller and has no savepoints of its own. Verify that the caller commits, and use a BEGIN ... EXCEPTION ... END block where the routine relied on a savepoint.")

            # Informix calls a routine with EXECUTE PROCEDURE / EXECUTE FUNCTION, and takes
            # the result of the call through an INTO clause behind it
            ## both parts stop at the semicolon - a call without an INTO clause of its own
            ## would otherwise reach across the statements which follow it and take the INTO
            ## of a later SELECT
            postgresql_code = re.sub(
                r'(?is)\bEXECUTE\s+(?:PROCEDURE|FUNCTION)\s+(?P<call>[^;]+?)\s+INTO\s+(?P<target>[^;]+);',
                lambda match: f"SELECT {match.group('call').strip()} INTO {match.group('target').strip()};",
                postgresql_code)
            ## a procedure is called with CALL, a function has to be selected - PERFORM is
            ## the way to call one whose result is not used
            postgresql_code = re.sub(r'(?i)\bEXECUTE\s+PROCEDURE\s+', 'CALL ', postgresql_code)
            postgresql_code = re.sub(r'(?i)\bEXECUTE\s+FUNCTION\s+', 'PERFORM ', postgresql_code)

            # Sequences are read through the pseudo columns of the sequence in Informix
            postgresql_code = re.sub(r'(?i)\b(\w+)\.NEXTVAL\b', r"nextval('\1')", postgresql_code)
            postgresql_code = re.sub(r'(?i)\b(\w+)\.CURRVAL\b', r"currval('\1')", postgresql_code)
            # sysdual is the one row pseudo table of Informix, PostgreSQL selects without FROM
            postgresql_code = re.sub(r'(?i)\s+FROM\s+(?:\w+:)?sysdual\b', '', postgresql_code)
            # TODAY is the current date, CURRENT is already handled with the other keywords
            postgresql_code = re.sub(r'(?i)\bTODAY\b', 'CURRENT_DATE', postgresql_code)

            # The pattern operator of Informix, which PostgreSQL does not know
            postgresql_code = self.convert_matches_operator(postgresql_code)

            # The SQL functions of Informix which the target does not know under that name.
            # Without this a routine using them is created without a complaint - PostgreSQL
            # only checks the syntax of a PL/pgSQL body - and fails on its first call.
            postgresql_code = self.apply_sql_functions_mapping(postgresql_code, settings)

            # The data types of the parameter list, of the DECLARE section and of a cast
            postgresql_code = self.convert_data_types_in_code(postgresql_code, target_db_type)

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

            # Report what could not be converted, instead of leaving it to be discovered
            # in the error message of the target database.
            if re.search(r'\bRETURNING\b', postgresql_code, flags=re.IGNORECASE):
                self.config_parser.print_log_message('WARNING',
                    "informix_connector: convert_funcproc_code: The RETURNING clause of the routine could not be converted - only a single return type is handled. A routine returning several values needs 'RETURNS TABLE (...)' or OUT parameters in PostgreSQL, the converted code has to be completed manually.")
            if re.search(r'\bWITH\s+RESUME\b', postgresql_code, flags=re.IGNORECASE):
                self.config_parser.print_log_message('WARNING',
                    "informix_connector: convert_funcproc_code: The routine returns a set of rows with 'RETURN ... WITH RESUME'. PostgreSQL needs a set returning function ('RETURNS SETOF ...' / 'RETURNS TABLE (...)' with 'RETURN NEXT'), the converted code has to be completed manually.")

            return postgresql_code

        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

    def fetch_sequences(self, schema_name: str):
        # Placeholder for fetching sequences
        return {}

    def get_sequence_details(self, sequence_owner, sequence_name):
        # Placeholder for fetching sequence details
        return {}

    def migrate_table(self, migrate_target_connection, settings):
        part_name = 'initialize'
        source_table_rows_limited = 0
        target_table_rows = 0
        total_inserted_rows = 0
        migration_stats = {}
        batch_number = 0
        shortest_batch_seconds = 0
        longest_batch_seconds = 0
        average_batch_seconds = 0
        chunk_start_row_number = 0
        chunk_end_row_number = 0
        processing_start_time = time.time()
        order_by_clause = ''
        try:
            worker_id = settings['worker_id']
            source_schema_name = settings['source_schema_name']
            source_table_name = settings['source_table_name']
            source_table_id = settings['source_table_id']
            source_columns = settings['source_columns']
            # target_schema_name = self.config_parser.convert_names_case(settings['target_schema_name'])
            target_schema_name = settings['target_schema_name']  ## target schema is used as it is defined in the config file, no conversion to upper/lower case
            target_table_name = self.config_parser.convert_names_case(settings['target_table_name'])
            target_columns = settings['target_columns']
            batch_size = settings['batch_size']
            migrator_tables = settings['migrator_tables']
            migration_limitation = settings['migration_limitation']
            chunk_size = settings['chunk_size']
            chunk_number = settings['chunk_number']
            resume_after_crash = settings['resume_after_crash']
            drop_unfinished_tables = settings['drop_unfinished_tables']

            source_table_rows_all = settings.get('source_table_rows_all', 0)

            source_table_rows_limited = self.get_rows_count(source_schema_name, source_table_name, migration_limitation)
            target_table_rows = migrate_target_connection.get_rows_count(target_schema_name, target_table_name)

            total_chunks = self.config_parser.get_total_chunks(source_table_rows_limited, chunk_size)
            if chunk_size == -1:
                chunk_size = source_table_rows_limited + 1

            migration_stats = {
                'rows_migrated': target_table_rows,
                'chunk_number': chunk_number,
                'total_chunks': total_chunks,
                'source_table_rows_all': source_table_rows_all,

                'source_table_rows_limited': source_table_rows_limited,
                'target_table_rows': target_table_rows,
                'finished': True if source_table_rows_limited == 0 else False,
            }

            protocol_id = migrator_tables.insert_data_migration({
                'worker_id': worker_id,
                'source_table_id': source_table_id,
                'source_schema_name': source_schema_name,
                'source_table_name': source_table_name,
                'target_schema_name': target_schema_name,
                'target_table_name': target_table_name,
                'source_table_rows_all': source_table_rows_all,

                'source_table_rows_limited': source_table_rows_limited,
                'target_table_rows': target_table_rows,
            })

            if source_table_rows_limited == 0:
                self.config_parser.print_log_message('INFO', f"informix_connector: migrate_table: Worker {worker_id}: Table {source_table_name} is empty - skipping data migration.")
                migrator_tables.update_data_migration_status({
                        'row_id': protocol_id,
                        'success': True,
                        'message': 'Skipped',
                        'target_table_rows': 0,
                        'batch_count': 0,
                        'shortest_batch_seconds': 0,
                        'longest_batch_seconds': 0,
                        'average_batch_seconds': 0,
                    })

                return migration_stats

            else:

                data_conflict_action = settings.get('data_conflict_action')
                if target_table_rows == 0 or data_conflict_action in ('merge_keep_target', 'merge_keep_source', 'replace'):
                    migrator_tables.update_data_migration_started(protocol_id)

                    self.config_parser.print_log_message('INFO', f"informix_connector: migrate_table: Worker {worker_id}: Source table {source_table_name}: {source_table_rows_limited} rows / Target table {target_table_name}: {target_table_rows} rows - starting data migration.")

                    select_columns_list = []
                    orderby_columns_list = []
                    insert_columns_list = []
                    document_cast_length = self.calculate_document_cast_length(source_columns)
                    for order_num, col in source_columns.items():
                        self.config_parser.print_log_message('DEBUG2',
                                                            f"Worker {worker_id}: Table {source_schema_name}.{source_table_name}: Processing column {col['column_name']} ({order_num}) with data type {col['data_type']}")

                        if col['data_type'].lower() == 'datetime':
                            select_columns_list.append(f"TO_CHAR({col['column_name']}, '%Y-%m-%d %H:%M:%S') as {col['column_name']}")
                        elif col['data_type'].lower() in ['clob', 'blob'] and not self.config_parser.should_migrate_lob_values():
                            select_columns_list.append(f"CAST(NULL as {col['data_type']}) as {col['column_name']}")
                        elif col['data_type'].lower() in ['char', 'nchar']:
                            ## compensate for Informix's fixed-length char columns
                            select_columns_list.append(f"trim({col['column_name']}) as {col['column_name']}")
                        elif col['data_type'].lower() == 'interval':
                            ## the driver hands an interval over as a com.informix.lang.Interval
                            ## object, which the target cannot store - reading it normalized to
                            ## the widest qualifier of its class gives a text of a known shape,
                            ## which convert_interval_value() turns into a PostgreSQL literal
                            qualifier = 'YEAR(9) TO MONTH' if self.is_year_month_interval(col) else 'DAY(9) TO SECOND'
                            select_columns_list.append(f"CAST(CAST({col['column_name']} AS INTERVAL {qualifier}) AS LVARCHAR(40)) as {col['column_name']}")
                        elif col['data_type'].lower() in ['bson', 'json']:
                            ## Both are opaque types of Informix and would arrive as an object of the
                            ## driver, which the target JSONB column cannot accept - a BSON document is
                            ## converted to its JSON representation first, JSON is already text
                            self.config_parser.print_log_message('WARNING',
                                f"informix_connector: migrate_table: Worker {worker_id}: Table {source_schema_name}.{source_table_name}: Column {col['column_name']} ({col['data_type']}) is transferred as its JSON text representation, limited to {document_cast_length} characters by the maximum output rowsize of Informix - larger documents have to be migrated separately.")
                            if col['data_type'].lower() == 'bson':
                                select_columns_list.append(f"CAST(CAST({col['column_name']} AS JSON) AS LVARCHAR({document_cast_length})) as {col['column_name']}")
                            else:
                                select_columns_list.append(f"CAST({col['column_name']} AS LVARCHAR({document_cast_length})) as {col['column_name']}")
                        elif col['data_type'].lower() in self.COLLECTION_DATA_TYPES:
                            ## A collection or row column arrives as an object of the driver -
                            ## Informix renders it as its literal text representation instead,
                            ## which is what the TEXT column of the target expects
                            self.config_parser.print_log_message('WARNING',
                                f"informix_connector: migrate_table: Worker {worker_id}: Table {source_schema_name}.{source_table_name}: Column {col['column_name']} ({col['data_type']}) is transferred as its literal text representation, limited to {document_cast_length} characters by the maximum output rowsize of Informix.")
                            select_columns_list.append(f"CAST({col['column_name']} AS LVARCHAR({document_cast_length})) as {col['column_name']}")
                        #     select_columns_list.append(f"ST_asText(`{col['column_name']}`) as `{col['column_name']}`")
                        # elif col['data_type'].lower() == 'set':
                        #     select_columns_list.append(f"cast(`{col['column_name']}` as char(4000)) as `{col['column_name']}`")
                        else:
                            select_columns_list.append(f"{col['column_name']}")

                        insert_columns_list.append(f'''"{self.config_parser.convert_names_case(col['column_name'])}"''')
                        orderby_columns_list.append(f'''"{col['column_name']}"''')

                    select_columns = ', '.join(select_columns_list)
                    orderby_columns = ', '.join(orderby_columns_list)
                    insert_columns = ', '.join(insert_columns_list)

                    if resume_after_crash and not drop_unfinished_tables:
                        chunk_number = self.config_parser.get_total_chunks(target_table_rows, chunk_size)
                        self.config_parser.print_log_message('DEBUG', f"informix_connector: migrate_table: Worker {worker_id}: Resuming migration for table {source_schema_name}.{source_table_name} from chunk {chunk_number} with data chunk size {chunk_size}.")
                        chunk_offset = target_table_rows
                    else:
                        chunk_offset = (chunk_number - 1) * chunk_size

                    chunk_start_row_number = chunk_offset + 1
                    chunk_end_row_number = chunk_offset + chunk_size

                    self.config_parser.print_log_message('DEBUG', f"informix_connector: migrate_table: Worker {worker_id}: Migrating table {source_schema_name}.{source_table_name}: chunk {chunk_number}, data chunk size {chunk_size}, batch size {batch_size}, chunk offset {chunk_offset}, chunk end row number {chunk_end_row_number}, source table rows {source_table_rows_limited}")
                    order_by_clause = ''

                    query = f'''SELECT SKIP {chunk_offset} {select_columns} FROM "{source_schema_name}".{source_table_name}'''
                    if migration_limitation:
                        query += f" WHERE {migration_limitation}"
                    primary_key_columns = migrator_tables.select_primary_key({'source_schema_name': source_schema_name, 'source_table_name': source_table_name})
                    self.config_parser.print_log_message('DEBUG2', f"informix_connector: migrate_table: Worker {worker_id}: Primary key columns for {source_schema_name}.{source_table_name}: {primary_key_columns}")
                    if primary_key_columns:
                        orderby_columns = primary_key_columns
                    order_by_clause = f""" ORDER BY {orderby_columns}"""
                    query += order_by_clause + f" LIMIT {chunk_size}"

                    self.config_parser.print_log_message('DEBUG', f"informix_connector: migrate_table: Worker {worker_id}: Fetching data with cursor using query: {query}")

                    part_name = 'execute query'
                    cursor = self.connection.cursor()
                    cursor.arraysize = batch_size

                    batch_start_time = time.time()
                    reading_start_time = batch_start_time
                    processing_start_time = batch_start_time
                    batch_end_time = None
                    batch_number = 0
                    batch_durations = []

                    query = self.execute_query_with_rowsize_retry(
                        cursor, query, document_cast_length, worker_id, f"{source_schema_name}.{source_table_name}")
                    total_inserted_rows = 0
                    while True:
                        records = cursor.fetchmany(batch_size)
                        if not records:
                            break
                        batch_number += 1
                        reading_end_time = time.time()
                        reading_duration = reading_end_time - reading_start_time
                        self.config_parser.print_log_message('DEBUG',f"informix_connector: migrate_table: Worker {worker_id}: Fetched {len(records)} rows (batch {batch_number}) from source table {source_table_name}.")

                        transforming_start_time = time.time()
                        records = [
                            {column['column_name']: value for column, value in zip(source_columns.values(), record)}
                            for record in records
                        ]
                        for record in records:
                            for order_num, column in source_columns.items():
                                column_name = column['column_name']
                                column_type = column['data_type']
                                target_column_type = target_columns[order_num]['data_type']
                                # if column_type.lower() in ['binary', 'bytea']:
                                if column_type.lower() in self.ARRAY_DATA_TYPES and record[column_name] is not None:
                                    record[column_name] = self.convert_collection_value(record[column_name])
                                elif column_type.lower() == 'interval' and record[column_name] is not None:
                                    record[column_name] = self.convert_interval_value(record[column_name], self.is_year_month_interval(column))
                                elif column_type.lower() in ['blob'] and record[column_name] is not None:
                                    record[column_name] = bytes(record[column_name].getBytes(1, int(record[column_name].length())))  # Convert 'com.informix.jdbc.IfxCblob' to bytes
                                elif column_type.lower() in ['clob'] and record[column_name] is not None:
                                    # elif isinstance(record[column_name], IfxCblob):
                                    record[column_name] = record[column_name].getSubString(1, int(record[column_name].length()))  # Convert IfxCblob to string
                                    # record[column_name] = bytes(record[column_name].getBytes(1, int(record[column_name].length())))  # Convert IfxBblob to bytes
                                    # record[column_name] = record[column_name].read()  # Convert IfxBblob to bytes
                                elif column_type.lower() in ['integer', 'smallint', 'tinyint', 'bit', 'boolean'] and target_column_type.lower() in ['boolean']:
                                    # Convert integer to boolean
                                    record[column_name] = bool(record[column_name])

                        # Insert batch into target table
                        self.config_parser.print_log_message('DEBUG', f"informix_connector: migrate_table: Worker {worker_id}: Starting insert of {len(records)} rows from source table {source_table_name}")
                        transforming_end_time = time.time()
                        transforming_duration = transforming_end_time - transforming_start_time
                        inserting_start_time = time.time()
                        inserted_rows = migrate_target_connection.insert_batch({
                            'target_schema_name': target_schema_name,
                            'target_table_name': target_table_name,
                            'target_columns': target_columns,
                            'data': records,
                            'worker_id': worker_id,
                            'migrator_tables': migrator_tables,
                            'insert_columns': insert_columns,
                            'insert_values': settings.get('insert_values'),
                            'data_conflict_action': data_conflict_action,
                            'primary_key_columns': primary_key_columns,
                        })
                        total_inserted_rows += inserted_rows
                        inserting_end_time = time.time()
                        inserting_duration = inserting_end_time - inserting_start_time

                        batch_end_time = time.time()
                        batch_duration = batch_end_time - batch_start_time
                        batch_durations.append(batch_duration)
                        percent_done = round(total_inserted_rows / source_table_rows_limited * 100, 2)

                        batch_start_dt = datetime.datetime.fromtimestamp(batch_start_time)
                        batch_end_dt = datetime.datetime.fromtimestamp(batch_end_time)
                        batch_start_str = batch_start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')
                        batch_end_str = batch_end_dt.strftime('%Y-%m-%d %H:%M:%S.%f')
                        migrator_tables.insert_batches_stats({
                            'source_schema_name': source_schema_name,
                            'source_table_name': source_table_name,
                            'source_table_id': source_table_id,
                            'chunk_number': chunk_number,
                            'batch_number': batch_number,
                            'batch_start': batch_start_str,
                            'batch_end': batch_end_str,
                            'batch_rows': inserted_rows,
                            'batch_seconds': batch_duration,
                            'worker_id': worker_id,
                            'reading_seconds': reading_duration,
                            'transforming_seconds': transforming_duration,
                            'writing_seconds': inserting_duration,
                        })

                        msg = (
                            f"Worker {worker_id}: Inserted {inserted_rows} "
                            f"(total: {total_inserted_rows} from: {source_table_rows_limited} "
                            f"({percent_done}%)) rows into target table '{target_table_name}': "
                            f"Batch {batch_number} duration: {batch_duration:.2f} seconds "
                            f"(r: {reading_duration:.2f}, t: {transforming_duration:.2f}, w: {inserting_duration:.2f})"
                        )
                        self.config_parser.print_log_message('INFO', msg)

                        batch_start_time = time.time()
                        reading_start_time = batch_start_time

                    target_table_rows = migrate_target_connection.get_rows_count(target_schema_name, target_table_name)
                    self.config_parser.print_log_message('INFO', f"informix_connector: migrate_table: Worker {worker_id}: Target table {target_schema_name}.{target_table_name} has {target_table_rows} rows")

                    shortest_batch_seconds = min(batch_durations) if batch_durations else 0
                    longest_batch_seconds = max(batch_durations) if batch_durations else 0
                    average_batch_seconds = sum(batch_durations) / len(batch_durations) if batch_durations else 0
                    self.config_parser.print_log_message('INFO', f"informix_connector: migrate_table: Worker {worker_id}: Migrated {total_inserted_rows} rows from {source_table_name} to {target_schema_name}.{target_table_name} in {batch_number} batches: "
                                                            f"Shortest batch: {shortest_batch_seconds:.2f} seconds, "
                                                            f"Longest batch: {longest_batch_seconds:.2f} seconds, "
                                                            f"Average batch: {average_batch_seconds:.2f} seconds")

                    cursor.close()

                else:
                    self.config_parser.print_log_message('INFO', f"informix_connector: migrate_table: Worker {worker_id}: Target table {target_table_name} has {target_table_rows} rows and data_conflict_action is '{data_conflict_action}'. Skipping data migration.")

                migration_stats = {
                    'rows_migrated': total_inserted_rows,
                    'chunk_number': chunk_number,
                    'total_chunks': total_chunks,
                    'source_table_rows_all': source_table_rows_all,

                    'source_table_rows_limited': source_table_rows_limited,
                    'target_table_rows': target_table_rows,
                    'finished': False,
                }

                self.config_parser.print_log_message('DEBUG', f"informix_connector: migrate_table: Worker {worker_id}: Migration stats: {migration_stats}")
                if source_table_rows_limited <= target_table_rows or chunk_number >= total_chunks:
                    self.config_parser.print_log_message('DEBUG3', f"informix_connector: migrate_table: Worker {worker_id}: Setting migration status to finished for table {source_table_name} (chunk {chunk_number}/{total_chunks})")
                    migration_stats['finished'] = True
                    migrator_tables.update_data_migration_status({
                        'row_id': protocol_id,
                        'success': True,
                        'message': 'OK',
                        'target_table_rows': target_table_rows,
                        'batch_count': batch_number,
                        'shortest_batch_seconds': shortest_batch_seconds,
                        'longest_batch_seconds': longest_batch_seconds,
                        'average_batch_seconds': average_batch_seconds,
                    })

                migrator_tables.insert_data_chunk({
                    'worker_id': worker_id,
                    'source_table_id': source_table_id,
                    'source_schema_name': source_schema_name,
                    'source_table_name': source_table_name,
                    'target_schema_name': target_schema_name,
                    'target_table_name': target_table_name,
                    'source_table_rows_all': source_table_rows_all,

                    'source_table_rows_limited': source_table_rows_limited,
                    'target_table_rows': target_table_rows,
                    'chunk_number': chunk_number,
                    'chunk_size': chunk_size,
                    'migration_limitation': migration_limitation,
                    'chunk_start': chunk_start_row_number,
                    'chunk_end': chunk_end_row_number,
                    'inserted_rows': total_inserted_rows,
                    'batch_size': batch_size,
                    'total_batches': batch_number,
                    'task_started': datetime.datetime.fromtimestamp(processing_start_time).strftime('%Y-%m-%d %H:%M:%S.%f'),
                    'task_completed': datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f'),
                    'order_by_clause': order_by_clause,
                })

                return migration_stats

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"informix_connector: migrate_table: Worker {worker_id}: Error during {part_name} -> {e}")
            self.config_parser.print_log_message('ERROR', f"informix_connector: migrate_table: Worker {worker_id}: Full stack trace: {traceback.format_exc()}")
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
                self.config_parser.print_log_message('DEBUG', f"informix_connector: fetch_triggers: row: {row}")
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
                ## Informix stores the text of a trigger split into fixed size pieces, one
                ## row per seqno - joining them with a newline and stripping each piece cuts
                ## words in half at every boundary ("n.total_amo" + "unt"), the pieces have
                ## to be concatenated exactly as they are stored
                trigger_code_str = ''.join([body[0] for body in trigger_code if body[0] is not None]).strip()

                trigger_code_lines = trigger_code_str.split('\n')

                for i, line in enumerate(trigger_code_lines):
                    line = line.strip()  # Remove trailing spaces
                    if line.startswith("--"):
                        trigger_code_lines[i] = f"/* {line.strip()} */"

                trigger_code_str = '\n'.join(trigger_code_lines)

                self.config_parser.print_log_message('DEBUG', f"informix_connector: fetch_triggers: trigger SQL: {trigger_code_str}")

                triggers[order_num]['sql'] = trigger_code_str
                triggers[order_num]['row_statement'] = 'FOR EACH ROW' if 'FOR EACH ROW' in trigger_code_str.upper() else ''
                order_num += 1
            cursor.close()
            self.disconnect()
            return triggers
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"informix_connector: fetch_triggers: Error when fetching triggers for the table {table_name}/{table_id}: {e}")
            raise


    def read_parenthesized_group(self, text, start):
        """
        Content of the parenthesized group beginning at or behind 'start'.

        Returns the text between the parentheses and the position behind the closing one,
        counting the nesting and skipping string literals.
        """
        open_position = text.find('(', start)
        if open_position == -1:
            return '', len(text)
        depth = 0
        quote = ''
        for index in range(open_position, len(text)):
            character = text[index]
            if quote:
                if character == quote:
                    quote = ''
            elif character in ("'", '"'):
                quote = character
            elif character == '(':
                depth += 1
            elif character == ')':
                depth -= 1
                if depth == 0:
                    return text[open_position + 1:index], index + 1
        return text[open_position + 1:], len(text)

    def extract_trigger_action_blocks(self, trig):
        """
        Split the action part of an Informix trigger into its blocks.

        One trigger may carry all three of them:

            BEFORE (actions) FOR EACH ROW [WHEN (condition)] (actions) AFTER (actions)

        BEFORE and AFTER run once per statement, FOR EACH ROW once per row. Reading them
        with a greedy regular expression let the FOR EACH ROW block swallow everything up
        to the last closing parenthesis, so the statements of the AFTER block ended up
        inside the row trigger and the parentheses no longer matched. The blocks are read
        by counting parentheses instead.

        Returns a list of (kind, when_condition, actions), kind being 'before', 'after'
        or 'for each row'.
        """
        blocks = []
        position = 0
        keyword = re.compile(r'(?i)\b(before|after|for\s+each\s+row)\b')
        while True:
            match = keyword.search(trig, position)
            if not match:
                break
            kind = re.sub(r'\s+', ' ', match.group(1)).lower()
            position = match.end()

            ## a WHEN condition belongs to the action list which follows it
            when_condition = ''
            when_match = re.compile(r'(?i)\s*\bwhen\b').match(trig, position)
            if when_match:
                when_condition, position = self.read_parenthesized_group(trig, when_match.end())

            if trig.find('(', position) == -1:
                break
            actions, position = self.read_parenthesized_group(trig, position)
            blocks.append((kind, when_condition.strip(), self.split_top_level_commas(actions)))
        return blocks

    def convert_trigger_action(self, action, settings, old_ref, new_ref):
        """ One action of a trigger block as a PL/pgSQL statement """
        action = action.replace(f'''"{settings['source_schema_name']}"''', f'''"{settings['target_schema_name']}"''')
        ## PostgreSQL calls a procedure with CALL - 'EXECUTE FUNCTION' of CREATE TRIGGER is
        ## something else entirely, it names the trigger function and takes only literals
        action = re.sub(r'(?i)^\s*execute\s+procedure\s*', 'CALL ', action)
        return self.map_trigger_correlation_names(action.strip(), old_ref, new_ref).rstrip(';')

    def convert_trigger(self, settings: dict):
        """
        Convert the triggers of one Informix table to PostgreSQL.

        Every action block of the source becomes a trigger of its own, because PostgreSQL
        separates what Informix writes in a single statement: the BEFORE and AFTER blocks
        are statement level triggers, the FOR EACH ROW block is a row level one. Each of
        them gets the trigger function PostgreSQL requires - a trigger cannot execute the
        statements directly.
        """
        informix_code = settings['trigger_sql']
        source_schema_name = settings['source_schema_name']
        target_schema_name = settings['target_schema_name']
        pgsql_triggers = []
        trigger_name = ''

        try:
            for trig in re.split(r'(?i)create\s+trigger', informix_code):
                trig = trig.strip()
                if not trig:
                    continue

                ## the code is read as one line below, so a comment would swallow whatever
                ## follows it - they are turned into the bracketed form first
                trig = '\n'.join(f"/* {line.strip()} */" if line.strip().startswith('--') else line
                                 for line in trig.split('\n') if line.strip() != '--')
                trig = re.sub(r'\s+', ' ', trig).strip()
                self.config_parser.print_log_message('DEBUG', f"informix_connector: convert_trigger: Trigger code: {trig}")

                ## 'INSTEAD OF' stands between the name and the event of a trigger on a view
                header_match = re.match(r'(?i)"?([^".\s]+)"?\.(\S+?)"?\s+(?:(instead\s+of)\s+)?(insert|update|delete)\b', trig)
                if not header_match:
                    self.config_parser.print_log_message('WARNING',
                        f"informix_connector: convert_trigger: The header of a trigger could not be read, it is not migrated: {trig[:120]}")
                    continue
                schema = header_match.group(1)
                trigger_name = header_match.group(2).strip('"')
                instead_of = bool(header_match.group(3))
                operation = header_match.group(4).upper()

                ## 'UPDATE OF a, b' fires only when one of those columns is written, and
                ## PostgreSQL knows the same restriction
                update_columns = ''
                if operation == 'UPDATE':
                    columns_match = re.match(r'(?i)\s*of\s+(.+?)\s+on\b', trig[header_match.end():])
                    if columns_match:
                        update_columns = ' OF ' + ', '.join(column.strip().strip('"') for column in columns_match.group(1).split(','))

                table_match = re.search(r'(?i)\son\s+"?([^".\s]+)"?\.\s*"?([^"\s(]+)"?', trig)
                table_schema = table_match.group(1) if table_match else schema
                table_name = table_match.group(2) if table_match else 'unknown_table'
                if table_schema == source_schema_name:
                    table_schema = target_schema_name

                ## Informix names the two row images itself, in either order
                new_ref = ''
                old_ref = ''
                ref_match = re.search(r'(?i)referencing\s+((?:(?:new|old)\s+as\s+\w+\s*)+)', trig)
                if ref_match:
                    for correlation, alias in re.findall(r'(?i)(new|old)\s+as\s+(\w+)', ref_match.group(1)):
                        if correlation.lower() == 'new':
                            new_ref = alias
                        else:
                            old_ref = alias

                self.config_parser.print_log_message('DEBUG',
                    f"informix_connector: convert_trigger: {trigger_name}: {'INSTEAD OF ' if instead_of else ''}{operation}{update_columns} on {table_schema}.{table_name}, new: {new_ref or '-'}, old: {old_ref or '-'}")

                blocks = self.extract_trigger_action_blocks(trig)
                if not blocks:
                    self.config_parser.print_log_message('WARNING',
                        f"informix_connector: convert_trigger: Trigger {trigger_name} has no action which could be read - it has to be migrated manually.")
                    continue

                counter = 0
                for kind, when_condition, actions in blocks:
                    actions = [self.convert_trigger_action(action, settings, old_ref, new_ref)
                               for action in actions if action.strip()]
                    if not actions:
                        continue

                    row_level = kind == 'for each row'
                    if row_level:
                        timing = 'INSTEAD OF' if instead_of else 'AFTER'
                        scope = 'FOR EACH ROW'
                        ## the row PostgreSQL expects back - for a DELETE only OLD is filled,
                        ## and an INSTEAD OF trigger reports the row as handled by returning it
                        returned_row = 'OLD' if operation == 'DELETE' else 'NEW'
                    else:
                        timing = 'BEFORE' if kind == 'before' else 'AFTER'
                        scope = 'FOR EACH STATEMENT'
                        ## a statement level trigger has neither OLD nor NEW
                        returned_row = 'NULL'
                        for action in actions:
                            if re.search(r'(?i)\b(OLD|NEW)\.', action):
                                self.config_parser.print_log_message('WARNING',
                                    f"informix_connector: convert_trigger: Trigger {trigger_name}: the {kind.upper()} block uses a column of the changed row, which a statement level trigger of PostgreSQL cannot read. The statement has to be migrated manually: {action}")

                    body_lines = [f"    {action};" for action in actions]
                    if when_condition:
                        condition = self.map_trigger_correlation_names(when_condition, old_ref, new_ref)
                        body_lines = [f"    IF {condition} THEN"] + [f"    {line}" for line in body_lines] + ["    END IF;"]

                    function_name = f"{trigger_name}_trigfunc{counter}"
                    func_code = (f'CREATE OR REPLACE FUNCTION "{target_schema_name}"."{function_name}"()\n'
                                 f'RETURNS trigger AS $$\n'
                                 f'BEGIN\n'
                                 + '\n'.join(body_lines) + '\n'
                                 f'    RETURN {returned_row};\n'
                                 f'END;\n'
                                 f'$$ LANGUAGE plpgsql;')

                    trigger_code = (f'CREATE TRIGGER "{trigger_name}{counter}"\n'
                                    f'{timing} {operation}{update_columns} ON "{table_schema}"."{table_name}"\n'
                                    f'{scope}\n'
                                    f'EXECUTE FUNCTION "{target_schema_name}"."{function_name}"();')

                    pgsql_triggers.append(func_code + "\n\n" + trigger_code)
                    counter += 1

            pgsql_trigger_code = "\n\n".join(pgsql_triggers)
            pgsql_trigger_code = self.convert_matches_operator(pgsql_trigger_code)
            pgsql_trigger_code = self.apply_sql_functions_mapping(pgsql_trigger_code, settings)
            # The body names Informix data types in its casts, e.g. '::lvarchar(2000)'
            pgsql_trigger_code = self.convert_data_types_in_code(pgsql_trigger_code, settings['target_db_type'])
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"informix_connector: convert_trigger: Error converting trigger {trigger_name}: {e}")
            self.config_parser.print_log_message('ERROR', traceback.format_exc())
            return ''

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
        self.config_parser.print_log_message('ERROR', f"informix_connector: handle_error: An error in {self.__class__.__name__} ({description}): {e}")
        self.config_parser.print_log_message('ERROR', traceback.format_exc())
        if self.on_error_action == 'stop':
            self.config_parser.print_log_message('ERROR', "informix_connector: handle_error: Stopping due to error.")
            exit(1)
        else:
            self.config_parser.print_log_message('WARNING', f"informix_connector: handle_error: Error caught, but continuing as requested by configuration (on_error_action='{self.on_error_action}').")

    def get_rows_count(self, table_schema: str, table_name: str, migration_limitation: str = None):
        query = f"""SELECT COUNT(*) FROM "{table_schema}".{table_name} """
        if migration_limitation:
            query += f" WHERE {migration_limitation}"
        self.config_parser.print_log_message('DEBUG3', f"informix_connector: get_rows_count: informix: get_rows_count query: {query}")
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

    def get_table_next_identity(self, table_schema: str, table_name: str):
        try:
            # Informix does not expose sequence counters easily.
            # We first find the identity column (SERIAL, SERIAL8, BIGSERIAL)
            col_query = f"""
                SELECT c.colname
                FROM syscolumns c
                JOIN systables t ON c.tabid = t.tabid
                WHERE t.tabname = '{table_name}' AND t.owner = '{table_schema}'
                  AND (MOD(c.coltype, 256) IN (6, 18, 53))
            """
            cursor = self.connection.cursor()
            cursor.execute(col_query)
            col_row = cursor.fetchone()
            if not col_row:
                cursor.close()
                return None
            identity_col = col_row[0]

            # Then query the max value
            max_query = f'SELECT MAX("{identity_col}") FROM "{table_schema}"."{table_name}"'
            cursor.execute(max_query)
            max_row = cursor.fetchone()
            cursor.close()

            if max_row and max_row[0] is not None:
                return int(max_row[0]) + 1
            return 1
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"informix_connector: get_table_next_identity: Error fetching next identity for {table_schema}.{table_name}: {e}")
            return None

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
        self.config_parser.print_log_message('DEBUG3', f"informix_connector: get_table_description: Informix connector: Getting table description for {settings['table_schema']}.{settings['table_name']}")
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

    def get_date_time_columns(self, cursor, table_schema: str, table_name: str):
        query = f"""
            SELECT
                c.colno,
                c.colname,
                CASE
                    WHEN c.extended_id = 0 THEN
                        CASE (CASE WHEN c.coltype >= 256 THEN c.coltype - 256 ELSE c.coltype END)
                            WHEN 7 THEN 'DATE'
                            WHEN 10 THEN 'DATETIME'
                            -- Add other time-related types if needed
                            ELSE NULL
                        END
                    ELSE
                        CASE WHEN x.name IS NOT NULL THEN upper(x.name)
                        ELSE NULL END
                END AS coltype,
                c.collength
            FROM syscolumns c
            LEFT JOIN sysxtdtypes x ON c.extended_id = x.extended_id
            WHERE c.tabid = (
                SELECT t.tabid
                FROM systables t
                WHERE t.tabname = '{table_name.strip()}'
                AND t.owner = '{table_schema.strip()}'
            )
            AND (
                (c.extended_id = 0 AND (c.coltype IN (7, 10) OR (c.coltype - 256) IN (7, 10)))
                OR (c.extended_id <> 0 AND (UPPER(x.name) LIKE '%DATE%' OR UPPER(x.name) LIKE '%TIME%'))
            )
            ORDER BY c.colno
            """
        self.config_parser.print_log_message('DEBUG3', f"informix_connector: get_date_time_columns: Fetching date/time columns for table {table_name.strip()} with query: {query}")
        cursor.execute(query)
        date_time_columns = cursor.fetchall()
        return ', '.join([f"{col[1]} ({col[2]})" for col in date_time_columns]) if date_time_columns else None

    def get_pk_columns(self, cursor, table_schema: str, table_name: str):
        query = f"""
            SELECT
                coalesce(c.constrname, i.idxname) as index_name,
                (SELECT colname FROM syscolumns ic WHERE ic.colno = i.part1 AND ic.tabid = i.tabid) as col1,
                (SELECT colname FROM syscolumns ic WHERE ic.colno = i.part2 AND ic.tabid = i.tabid) as col2,
                (SELECT colname FROM syscolumns ic WHERE ic.colno = i.part3 AND ic.tabid = i.tabid) as col3,
                (SELECT colname FROM syscolumns ic WHERE ic.colno = i.part4 AND ic.tabid = i.tabid) as col4
            FROM sysindexes i
            LEFT JOIN sysconstraints c
            ON i.tabid = c.tabid and i.idxname = c.idxname
            LEFT JOIN sysindices i2
            ON i.tabid = i2.tabid and i.idxname = i2.idxname
            WHERE coalesce(c.constrtype, i.idxtype) = 'P'
            AND i.tabid = (SELECT tabid FROM systables
                WHERE tabname = '{table_name.strip()}'
                AND owner = '{table_schema.strip()}')
        """
        self.config_parser.print_log_message('DEBUG3', f"informix_connector: get_pk_columns: Fetching PK columns for table {table_name.strip()} with query: {query}")
        cursor.execute(query)
        pk_columns = cursor.fetchall()
        pk_column_names = []
        for row in pk_columns:
            for col in row[1:]:
                if col:
                    pk_column_names.append(col.strip())
        return ', '.join(pk_column_names)

    def get_top_n_tables(self, settings):
        top_tables = {}
        top_tables['by_rows'] = {}
        top_tables['by_size'] = {}
        top_tables['by_columns'] = {}
        top_tables['by_indexes'] = {}
        top_tables['by_constraints'] = {}

        # exclude_tables can be a list of table names or regex patterns
        exclude_tables = self.config_parser.get_exclude_tables()
        exclude_clause = ""
        if exclude_tables:
            clauses = []
            for value in exclude_tables:
                if value.startswith('^') or any(c in value for c in ['*', '.', '$', '[', ']', '?', '+', '|', '(', ')']):
                    # Treat as regex pattern
                    clauses.append(f"tabname NOT MATCHES '{value}'")
                else:
                    # Treat as exact table name
                    clauses.append(f"tabname <> '{value}'")
                if clauses:
                    exclude_clause = " AND " + " AND ".join(clauses)
        try:
            order_num = 1
            top_n = self.config_parser.get_top_n_tables_by_rows()
            if top_n > 0:
                query = f"""
                    select
                        owner, tabname, nrows, rowsize, rowsize*nrows as size,
                        (select count(*) from sysconstraints c where t.tabid = c.tabid and constrtype = 'R') as fk_count,
                        CASE WHEN bitand(flags, 1) = 1 THEN 'YES' ELSE 'NO' END AS has_rowid,
                        (select count(*) FROM sysconstraints ic JOIN systables it ON ic.tabid = it.tabid JOIN sysreferences ir ON ic.constrid = ir.constrid
                        JOIN systables irt ON ir.ptabid = irt.tabid JOIN sysconstraints ipc ON ir."primary" = ipc.constrid WHERE ic.constrtype = 'R' and irt.owner = t.owner and irt.tabname = t.tabname) as ref_fk_count
                    from systables t where owner = '{settings['source_schema_name']}' {exclude_clause}
                    order by nrows desc limit {top_n}
                """
                self.config_parser.print_log_message('DEBUG2', f"informix_connector: get_top_n_tables: Fetching top {top_n} tables BY ROWS for schema {settings['source_schema_name']} with query: {query}")
                self.connect()
                cursor = self.connection.cursor()
                cursor.execute(query)
                tables = cursor.fetchall()
                for row in tables:

                    top_tables['by_rows'][order_num] = {
                        'owner': row[0].strip(),
                        'table_name': row[1].strip(),
                        'row_count': row[2],
                        'row_size': row[3],
                        'table_size': row[4],
                        'fk_count': row[5],
                        'date_time_columns': self.get_date_time_columns(cursor, row[0].strip(), row[1].strip()),
                        'pk_columns': self.get_pk_columns(cursor, row[0].strip(), row[1].strip()),
                        'has_rowid': row[6],
                        'ref_fk_count': row[7],
                    }
                    order_num += 1

                cursor.close()
                self.disconnect()
                self.config_parser.print_log_message('DEBUG2', f"informix_connector: get_top_n_tables: Top {top_n} tables BY ROWS: {top_tables}")
            else:
                self.config_parser.print_log_message('INFO', "informix_connector: get_top_n_tables: Skipping fetching top tables by rows as the setting is not defined or set to 0")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"informix_connector: get_top_n_tables: Error fetching top tables by rows: {e}")

        try:
            order_num = 1
            top_n = self.config_parser.get_top_n_tables_by_size()
            if top_n > 0:
                query = f"""
                    select
                        owner, tabname, rowsize, nrows, rowsize*nrows as size,
                        (select count(*) from sysconstraints c where t.tabid = c.tabid and constrtype = 'R') as fk_count,
                        CASE WHEN bitand(flags, 1) = 1 THEN 'YES' ELSE 'NO' END AS has_rowid,
                        (select count(*) FROM sysconstraints ic JOIN systables it ON ic.tabid = it.tabid JOIN sysreferences ir ON ic.constrid = ir.constrid
                        JOIN systables irt ON ir.ptabid = irt.tabid JOIN sysconstraints ipc ON ir."primary" = ipc.constrid WHERE ic.constrtype = 'R' and irt.owner = t.owner and irt.tabname = t.tabname) as ref_fk_count
                    from systables t where owner = '{settings['source_schema_name']}' {exclude_clause}
                    order by size desc limit {top_n}
                """
                self.config_parser.print_log_message('DEBUG2', f"informix_connector: get_top_n_tables: Fetching top {top_n} tables BY SIZE for schema {settings['source_schema_name']} with query: {query}")
                self.connect()
                cursor = self.connection.cursor()
                cursor.execute(query)
                tables = cursor.fetchall()
                for row in tables:
                    top_tables['by_size'][order_num] = {
                        'owner': row[0].strip(),
                        'table_name': row[1].strip(),
                        'table_size': row[4],
                        'row_count': row[3],
                        'row_size': row[2],
                        'fk_count': row[5],
                        'date_time_columns': self.get_date_time_columns(cursor, row[0].strip(), row[1].strip()),
                        'pk_columns': self.get_pk_columns(cursor, row[0].strip(), row[1].strip()),
                        'has_rowid': row[6],
                        'ref_fk_count': row[7],
                    }
                    order_num += 1
                cursor.close()
                self.disconnect()
                self.config_parser.print_log_message('DEBUG2', f"informix_connector: get_top_n_tables: Top {top_n} tables BY SIZE: {top_tables}")
            else:
                self.config_parser.print_log_message('INFO', "informix_connector: get_top_n_tables: Skipping fetching top tables by size as the setting is not defined or set to 0")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"informix_connector: get_top_n_tables: Error fetching top tables by size: {e}")

        try:
            order_num = 1
            top_n = self.config_parser.get_top_n_tables_by_columns()
            if top_n > 0:
                query = f"""
                    select
                        t.owner, tabname, count(*) as column_count, rowsize, nrows, rowsize*nrows as size,
                        (select count(*) from sysconstraints c where t.tabid = c.tabid and constrtype = 'R') as fk_count,
                        CASE WHEN bitand(flags, 1) = 1 THEN 'YES' ELSE 'NO' END AS has_rowid,
                        (select count(*) FROM sysconstraints ic JOIN systables it ON ic.tabid = it.tabid JOIN sysreferences ir ON ic.constrid = ir.constrid
                        JOIN systables irt ON ir.ptabid = irt.tabid JOIN sysconstraints ipc ON ir."primary" = ipc.constrid WHERE ic.constrtype = 'R' and irt.owner = t.owner and irt.tabname = t.tabname) as ref_fk_count
                    from systables t
                    join syscolumns c on t.tabid = c.tabid
                    where t.owner = '{settings['source_schema_name']}' {exclude_clause}
                    and c.colno > 0
                    group by t.owner, tabname, rowsize, nrows, size, fk_count, has_rowid
                    order by column_count desc limit {top_n}
                """
                self.config_parser.print_log_message('DEBUG2', f"informix_connector: get_top_n_tables: Fetching top {top_n} tables BY COLUMNS for schema {settings['source_schema_name']} with query: {query}")
                self.connect()
                cursor = self.connection.cursor()
                cursor.execute(query)
                tables = cursor.fetchall()
                for row in tables:
                    top_tables['by_columns'][order_num] = {
                        'owner': row[0].strip(),
                        'table_name': row[1].strip(),
                        'column_count': row[2],
                        'row_size': row[3],
                        'row_count': row[4],
                        'table_size': row[5],
                        'fk_count': row[6],
                        'date_time_columns': self.get_date_time_columns(cursor, row[0].strip(), row[1].strip()),
                        'pk_columns': self.get_pk_columns(cursor, row[0].strip(), row[1].strip()),
                        'has_rowid': row[7],
                        'ref_fk_count': row[8],
                    }
                    order_num += 1
                cursor.close()
                self.disconnect()
                self.config_parser.print_log_message('DEBUG2', f"informix_connector: get_top_n_tables: Top {top_n} tables BY COLUMNS: {top_tables}")
            else:
                self.config_parser.print_log_message('INFO', "informix_connector: get_top_n_tables: Skipping fetching top tables by columns as the setting is not defined or set to 0")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"informix_connector: get_top_n_tables: Error fetching top tables by columns: {e}")

        try:
            order_num = 1
            top_n = self.config_parser.get_top_n_tables_by_indexes()
            if top_n > 0:
                query = f"""
                    select
                        t.owner, tabname, count(*) as index_count, rowsize, nrows, rowsize*nrows as size,
                        (select count(*) from sysconstraints c where t.tabid = c.tabid and constrtype = 'R') as fk_count,
                        CASE WHEN bitand(flags, 1) = 1 THEN 'YES' ELSE 'NO' END AS has_rowid,
                        (select count(*) FROM sysconstraints ic JOIN systables it ON ic.tabid = it.tabid JOIN sysreferences ir ON ic.constrid = ir.constrid
                        JOIN systables irt ON ir.ptabid = irt.tabid JOIN sysconstraints ipc ON ir."primary" = ipc.constrid WHERE ic.constrtype = 'R' and irt.owner = t.owner and irt.tabname = t.tabname) as ref_fk_count
                    from systables t
                    join sysindexes i on t.tabid = i.tabid
                    where t.owner = '{settings['source_schema_name']}' {exclude_clause}
                    group by t.owner, tabname, rowsize, nrows, size, fk_count, has_rowid
                    order by index_count desc limit {top_n}
                """
                self.config_parser.print_log_message('DEBUG2', f"informix_connector: get_top_n_tables: Fetching top {top_n} tables BY INDEXES for schema {settings['source_schema_name']} with query: {query}")
                self.connect()
                cursor = self.connection.cursor()
                cursor.execute(query)
                tables = cursor.fetchall()
                for row in tables:
                    top_tables['by_indexes'][order_num] = {
                        'owner': row[0].strip(),
                        'table_name': row[1].strip(),
                        'index_count': row[2],
                        'row_size': row[3],
                        'row_count': row[4],
                        'table_size': row[5],
                        'fk_count': row[6],
                        'date_time_columns': self.get_date_time_columns(cursor, row[0].strip(), row[1].strip()),
                        'pk_columns': self.get_pk_columns(cursor, row[0].strip(), row[1].strip()),
                        'has_rowid': row[7],
                        'ref_fk_count': row[8],
                    }
                    order_num += 1
                cursor.close()
                self.disconnect()
                self.config_parser.print_log_message('DEBUG2', f"informix_connector: get_top_n_tables: Top {top_n} tables BY INDEXES: {top_tables}")
            else:
                self.config_parser.print_log_message('INFO', "informix_connector: get_top_n_tables: Skipping fetching top tables by indexes as the setting is not defined or set to 0")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"informix_connector: get_top_n_tables: Error fetching top tables by indexes: {e}")

        try:
            order_num = 1
            top_n = self.config_parser.get_top_n_tables_by_constraints()
            if top_n > 0:
                query = f"""
                    select
                        t.owner, tabname, count(*) as constraint_count, rowsize, nrows, rowsize*nrows as size, constrtype,
                        CASE WHEN bitand(flags, 1) = 1 THEN 'YES' ELSE 'NO' END AS has_rowid,
                        (select count(*) FROM sysconstraints ic JOIN systables it ON ic.tabid = it.tabid JOIN sysreferences ir ON ic.constrid = ir.constrid
                        JOIN systables irt ON ir.ptabid = irt.tabid JOIN sysconstraints ipc ON ir."primary" = ipc.constrid WHERE ic.constrtype = 'R' and irt.owner = t.owner and irt.tabname = t.tabname) as ref_fk_count
                    from systables t
                    join sysconstraints c on t.tabid = c.tabid
                    where t.owner = '{settings['source_schema_name']}' {exclude_clause}
                    AND constrtype IN ('R', 'C')
                    group by t.owner, tabname, rowsize, nrows, size, constrtype, has_rowid
                    order by constraint_count desc limit {top_n}
                """
                self.config_parser.print_log_message('DEBUG2', f"informix_connector: get_top_n_tables: Fetching top {top_n} tables BY CONSTRAINTS for schema {settings['source_schema_name']} with query: {query}")
                self.connect()
                cursor = self.connection.cursor()
                cursor.execute(query)
                tables = cursor.fetchall()
                for row in tables:
                    top_tables['by_constraints'][order_num] = {
                        'owner': row[0].strip(),
                        'table_name': row[1].strip(),
                        'constraint_type': 'FOREIGN KEY' if row[6].strip() == 'R' else 'CHECK',
                        'constraint_count': row[2],
                        'row_size': row[3],
                        'row_count': row[4],
                        'table_size': row[5],
                        'date_time_columns': self.get_date_time_columns(cursor, row[0].strip(), row[1].strip()),
                        'pk_columns': self.get_pk_columns(cursor, row[0].strip(), row[1].strip()),
                        'has_rowid': row[7],
                        'ref_fk_count': row[8],
                    }
                    order_num += 1
                cursor.close()
                self.disconnect()
                self.config_parser.print_log_message('DEBUG2', f"informix_connector: get_top_n_tables: Top {top_n} tables BY CONSTRAINTS: {top_tables}")
            else:
                self.config_parser.print_log_message('INFO', "informix_connector: get_top_n_tables: Skipping fetching top tables by constraints as the setting is not defined or set to 0")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"informix_connector: get_top_n_tables: Error fetching top tables by constraints: {e}")

        return top_tables

    def get_top_fk_dependencies(self, settings):
        top_fk_dependencies = {}
        source_schema_name = settings['source_schema_name']

        # exclude_tables can be a list of table names or regex patterns
        exclude_tables = self.config_parser.get_exclude_tables()
        exclude_clause = ""
        if exclude_tables:
            clauses = []
            for value in exclude_tables:
                if value.startswith('^') or any(c in value for c in ['*', '.', '$', '[', ']', '?', '+', '|', '(', ')']):
                    # Treat as regex pattern
                    clauses.append(f"tabname NOT MATCHES '{value}'")
                else:
                    # Treat as exact table name
                    clauses.append(f"tabname <> '{value}'")
                if clauses:
                    exclude_clause = " AND " + " AND ".join(clauses)

        try:
            order_num = 1
            top_n = 10 # self.config_parser.get_top_n_fk_dependencies_by_tables()
            if top_n > 0:
                query = f"""
                    SELECT
                        t.owner, t.tabname, COUNT(*) AS fk_count
                    FROM systables t
                    JOIN sysconstraints c ON t.tabid = c.tabid
                    WHERE c.constrtype = 'R' AND t.owner = '{source_schema_name}' {exclude_clause}
                    GROUP BY t.owner, t.tabname
                    ORDER BY fk_count DESC LIMIT {top_n}
                """
                self.config_parser.print_log_message('DEBUG2', f"informix_connector: get_top_fk_dependencies: Fetching top {top_n} foreign key dependencies BY TABLES for schema {settings['source_schema_name']} with query: {query}")
                self.connect()
                cursor = self.connection.cursor()
                cursor.execute(query)
                tables = cursor.fetchall()
                for row in tables:
                    query = f"""
                    SELECT
                        t.tabname || '.' || col.colname || ' -> ' || rt.tabname || '.' || rcol.colname AS dependency_columns
                    FROM sysconstraints c
                    JOIN systables t ON c.tabid = t.tabid
                    JOIN sysindexes i ON c.idxname = i.idxname
                    JOIN syscolumns col ON t.tabid = col.tabid AND col.colno IN (i.part1, i.part2, i.part3, i.part4, i.part5, i.part6, i.part7, i.part8, i.part9, i.part10, i.part11, i.part12, i.part13, i.part14, i.part15, i.part16)
                    JOIN sysreferences r ON c.constrid = r.constrid
                    JOIN systables rt ON r.ptabid = rt.tabid
                    JOIN sysconstraints pc ON r."primary" = pc.constrid
                    JOIN sysindexes pi ON pc.idxname = pi.idxname
                    JOIN syscolumns rcol ON rt.tabid = rcol.tabid AND rcol.colno IN (pi.part1, pi.part2, pi.part3, pi.part4, pi.part5, pi.part6, pi.part7, pi.part8, pi.part9, pi.part10, pi.part11, pi.part12, pi.part13, pi.part14, pi.part15, pi.part16)
                    WHERE c.constrtype = 'R' and t.owner = '{row[0].strip()}' and t.tabname = '{row[1].strip()}'
                    """
                    cursor.execute(query)
                    dependencies = cursor.fetchall()
                    dependency_columns = ', '.join([dep[0] for dep in dependencies])

                    top_fk_dependencies[order_num] = {
                        'owner': row[0].strip(),
                        'table_name': row[1].strip(),
                        'fk_count': row[2],
                        'dependencies': dependency_columns,
                    }

                    order_num += 1

                cursor.close()
                self.disconnect()
                self.config_parser.print_log_message('DEBUG2', f"informix_connector: get_top_fk_dependencies: Top {top_n} foreign key dependencies BY TABLES: {top_fk_dependencies}")
            else:
                self.config_parser.print_log_message('INFO', "informix_connector: get_top_fk_dependencies: Skipping fetching top foreign key dependencies by tables as the setting is not defined or set to 0")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"informix_connector: get_top_fk_dependencies: Error fetching top foreign key dependencies by tables: {e}")

        return top_fk_dependencies

    def target_table_exists(self, target_schema_name, target_table_name):
        try:
            query = f"""
                SELECT COUNT(*)
                FROM systables
                WHERE owner = '{target_schema_name}' AND tabname = '{target_table_name}' AND tabtype = 'T'
            """
            self.config_parser.print_log_message('DEBUG3', f"informix_connector: target_table_exists: Checking if target table exists with query: {query}")
            cursor = self.connection.cursor()
            cursor.execute(query)
            exists = cursor.fetchone()[0]
            cursor.close()
            return exists
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"informix_connector: target_table_exists: Error checking if target table exists: {e}")
            return False

    def fetch_all_rows(self, query):
        try:
            self.config_parser.print_log_message('DEBUG3', f"informix_connector: fetch_all_rows: Executing query to fetch all rows: {query}")
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"informix_connector: fetch_all_rows: Error fetching all rows: {e}")
            return []

    def convert_informix_default(self, settings) -> str:
        """
        Translate one sysdefaults record into a default value usable in PostgreSQL.

        sysdefaults.type tells which kind of default was declared - only type 'L'
        stores a literal in sysdefaults.default, all other types are keywords
        (TODAY, CURRENT, USER, SITENAME/DBSERVERNAME, NULL) which are stored
        nowhere else and would otherwise be lost.
        sysdefaults.default is CHAR(256), so literals come back padded - Informix
        pads them with NUL bytes, which must be removed before the value is used,
        otherwise psycopg refuses to pass it to PostgreSQL
        ("A string literal cannot contain NUL (0x00) characters").
        """
        column_name = settings.get('column_name', '')
        data_type = (settings.get('data_type') or '').strip().upper()
        default_type = (settings.get('default_type') or '').strip().upper()
        default_value = settings.get('default_value')

        if default_type == '' and default_value is None:
            return ''

        if default_type == 'L':
            if default_value is None:
                return ''
            # remove NUL padding and other control characters used as padding
            cleaned_default = self.clean_default_value(default_value)
            if cleaned_default == '':
                self.config_parser.print_log_message('WARNING',
                    f"informix_connector: convert_informix_default: Column {column_name} ({data_type}) has a literal default which contains no printable characters - default value is ignored.")
                return ''
            if data_type == 'BOOLEAN':
                # Informix stores boolean literals as 't' / 'f'
                if cleaned_default.strip("'").lower() in ('t', 'true', '1'):
                    return 'TRUE'
                if cleaned_default.strip("'").lower() in ('f', 'false', '0'):
                    return 'FALSE'
                self.config_parser.print_log_message('WARNING',
                    f"informix_connector: convert_informix_default: Column {column_name} - unexpected BOOLEAN default value '{cleaned_default}' - kept as it is.")
            return cleaned_default
        elif default_type == 'T':
            # TODAY
            return 'CURRENT_DATE'
        elif default_type == 'C':
            # CURRENT [ ... ] - Informix keeps no information about the precision here
            return 'CURRENT_TIMESTAMP'
        elif default_type == 'U':
            # USER
            return 'CURRENT_USER'
        elif default_type == 'S':
            # SITENAME / DBSERVERNAME - PostgreSQL has no equivalent,
            # so the name of the source Informix server is used as a literal
            source_server_name = (self.config_parser.get_db_config(self.source_or_target).get('server') or '')
            self.config_parser.print_log_message('WARNING',
                f"informix_connector: convert_informix_default: Column {column_name} - default SITENAME/DBSERVERNAME has no equivalent in PostgreSQL - replaced by literal '{source_server_name}'.")
            return f"'{source_server_name}'"
        elif default_type == 'N':
            # explicit DEFAULT NULL - same as no default in PostgreSQL
            return ''
        else:
            self.config_parser.print_log_message('WARNING',
                f"informix_connector: convert_informix_default: Column {column_name} - unknown default type '{default_type}' (value: '{default_value}') - default value is ignored.")
            return ''

    def clean_default_value(self, default_value) -> str:
        """ Removes NUL bytes / control characters used by Informix as padding of CHAR(256) values. """
        if default_value is None:
            return ''
        return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', str(default_value)).strip()

    def convert_default_value(self, settings) -> dict:
        extracted_default_value = settings['extracted_default_value']
        return self.clean_default_value(extracted_default_value)

    def get_table_checksum(self, schema_name: str, table_name: str, columns: list):
        if not columns:
            return None
            
        cols_list = []
        for col in columns:
            dtype = col.get('data_type', '').lower()
            if any(x in dtype for x in ['lob', 'bytea', 'xml', 'json', 'bson', 'text']):
                continue
            cols_list.append(f'"{col["column_name"]}"')
            
        if not cols_list:
            return None
            
        cols_str = ", ".join(cols_list)
        query = f'SELECT {cols_str} FROM "{schema_name}".{table_name}'
        return self._compute_python_table_checksum(query)

    def get_random_pks(self, schema_name: str, table_name: str, pk_columns: list, sample_size: int):
        return []

    def get_row_checksums(self, schema_name: str, table_name: str, pk_columns: list, pk_values_list: list, columns: list):
        if not columns or not pk_columns or not pk_values_list:
            return {}
            
        cols_list = []
        for col in columns:
            dtype = col.get('data_type', '').lower()
            if any(x in dtype for x in ['lob', 'bytea', 'xml', 'json', 'bson', 'text']):
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
            
        query = f'SELECT {pk_cols_str}, {cols_str} FROM "{schema_name}".{table_name} WHERE {where_clause}'
        return self._compute_python_row_checksums(query, len(pk_columns))

    def get_lob_sizes(self, schema_name: str, table_name: str, pk_columns: list, pk_values_list: list, lob_columns: list):
        return {}

if __name__ == "__main__":
    print("This script is not meant to be run directly")
