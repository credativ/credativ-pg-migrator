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
from jaydebeapi import Error
import pyodbc
from pyodbc import Error
from credativ_pg_migrator.database_connector import DatabaseConnector, first_line
from credativ_pg_migrator.migrator_logging import MigratorLogger
from credativ_pg_migrator.connectors.tsql_parser import TsqlParser
from credativ_pg_migrator.query_conversion import outer_joins as query_outer_joins
from credativ_pg_migrator.query_conversion import money_literals as query_money_literals
from credativ_pg_migrator.query_conversion.outer_joins import outer_join_warnings
from credativ_pg_migrator.jvm_helper import detach_thread_from_jvm
from credativ_pg_migrator.text_decoding import TextDecoder
import re
import struct
import threading
import traceback
import time
import datetime
import sqlglot
from sqlglot import exp, TokenType
from sqlglot.dialects import TSQL
import logging

# Define Custom Block Expression
class Block(exp.Expression):
    arg_types = {"expressions": True}

# Register Block with Postgres Generator to allow fallback generation
from sqlglot.dialects.postgres import Postgres

def block_handler(self, expression):
    # Ensure all statements in a block end with a semicolon
    statements = []
    for e in expression.expressions:
        stmt = self.sql(e).strip()
        if stmt and not stmt.endswith(';'):
            stmt += ';'
        statements.append(stmt)
    return "\n".join(statements)

Postgres.Generator.TRANSFORMS[Block] = block_handler

class CustomTSQL(TSQL):
    class Tokenizer(TSQL.Tokenizer):
        COMMANDS = TSQL.Tokenizer.COMMANDS - {TokenType.COMMAND, TokenType.SET}

    class Parser(TSQL.Parser):
        config_parser = None

        def _parse_alias(self, this, explicit=False):
             # FIX: Explicitly prevent UPDATE/INSERT/DELETE/MERGE/SET from being aliases
             # usage of keywords as aliases without AS is weird in implicit statement boundary contexts
             if self._curr:
                  # Check standard TokenTypes
                  if self._curr.token_type in (TokenType.UPDATE, TokenType.INSERT, TokenType.DELETE, TokenType.MERGE, TokenType.SET, TokenType.SELECT):
                       return this

                  # Check text for others (PRINT, RAISERROR, etc might be Commands or Vars)
                  txt = self._curr.text.upper()
                  if txt in ('PRINT', 'RAISERROR', 'EXEC', 'EXECUTE', 'IF', 'WHILE', 'BEGIN', 'DECLARE', 'CREATE', 'GO', 'ELSE'):
                       return this

             return super()._parse_alias(this, explicit)

        def _parse_command_custom(self):
            # Intercept PRINT to parse expression
            # Also helper for SET non-greedy parsing

            prev_is_print = self._prev.text.upper() == 'PRINT' if self._prev else False
            curr_is_print = self._curr.text.upper() == 'PRINT' if self._curr else False

            if curr_is_print:
                 self._advance()
            elif not prev_is_print:
                 if self._prev.text.upper() == 'SET':
                      # SET already consumed by dispatcher mechanism?
                      # Handle SET non-greedily: Stop at new statement keywords or semicolon
                      expressions = []
                      balance = 0
                      while self._curr:
                           if self._curr.token_type in (TokenType.SEMICOLON, TokenType.END):
                                break

                           if balance == 0:
                                txt = self._curr.text.upper()
                                if txt in ('SELECT', 'UPDATE', 'INSERT', 'DELETE', 'BEGIN', 'IF', 'WHILE', 'RETURN', 'DECLARE', 'CREATE', 'TRUNCATE', 'GO', 'ELSE', 'SET', 'PRINT', 'RAISERROR', 'EXEC', 'EXECUTE'):
                                     break

                           if self._curr.token_type == TokenType.L_PAREN:
                                balance += 1
                           elif self._curr.token_type == TokenType.R_PAREN:
                                balance -= 1

                           expressions.append(self._curr.text)
                           self._advance()

                      return exp.Command(this='SET', expression=exp.Literal.string(" ".join(expressions)))

                 # Not a PRINT or SET command
                 return self._parse_command()

            # Use _parse_conjunction to avoid consuming END (as alias)
            return exp.Command(this='PRINT', expression=self._parse_conjunction())

        def _parse_rollback_custom(self):
             # Custom parsing for ROLLBACK to avoid greedy consumption of END or other keywords
             # T-SQL: ROLLBACK [ { TRAN | TRANSACTION } [ savepoint_name | @savepoint_variable ] ]
             # PG: ROLLBACK [ WORK | TRANSACTION ] [ AND [ NO ] CHAIN ]

             # Consume ROLLBACK (already consumed by dispatcher? No, matched by key)
             # But this is called via STATEMENT_PARSERS
             # If we are here, we are at the start.

             # Actually, STATEMENT_PARSERS are calleed after token matching?
             # No, TSQL.Parser.STATEMENT_PARSERS maps TokenType -> function.
             # The loop is: if token in STATEMENT_PARSERS, call it.
             # The function assumes it's at that token (or just after?)
             # Standard _parse_rollback consumes tokens.
             # We should consume ROLLBACK first if not already?
             # Wait, generic parser usually consumes the triggering token?
             # No, standard functions often consume. e.g. _parse_if calls self._match(TokenType.ELSE).
             # Let's verify existing parsers. e.g. _parse_select starts by consuming SELECT.

             if self._match(TokenType.ROLLBACK) or self._match(TokenType.COMMAND):
                  pass

             # Handle optional TRANSACTION / WORK
             # Use text match if token attribute missing or just to be safe across versions
             if self._curr:
                  txt = self._curr.text.upper()
                  if txt in ('TRANSACTION', 'TRAN', 'WORK'):
                       self._advance()

             # Ignore savepoints for migration simplicity/safety for now, or match ID only
             # Ensuring we don't eat 'END'
             if self._curr and self._curr.token_type not in (TokenType.END, TokenType.SEMICOLON, TokenType.ELSE):
                 pass # Could match identifier here if needed, but risky.

             return exp.Rollback()

        STATEMENT_PARSERS = TSQL.Parser.STATEMENT_PARSERS.copy()
        STATEMENT_PARSERS[TokenType.COMMAND] = _parse_command_custom
        STATEMENT_PARSERS[TokenType.SET] = _parse_command_custom
        # Override ROLLBACK if it exists as a token
        if hasattr(TokenType, 'ROLLBACK'):
             STATEMENT_PARSERS[TokenType.ROLLBACK] = _parse_rollback_custom

        def _parse_block(self):
            if not self._match(TokenType.BEGIN):
                 pass

            expressions = []
            loop_counter = 0
            last_token_idx = -1

            while self._curr and self._curr.token_type != TokenType.END:
                 loop_counter += 1
                 if loop_counter > 100000:
                      raise Exception(f"Potential Infinite Loop in _parse_block at token {self._curr}")

                 # Check progress
                 current_idx = self._index
                 if current_idx == last_token_idx:
                      # Stuck?

                      # Detect orphaned ELSE -> Incorrectly parsed block boundary or lost ELSE IF
                      if self._curr.token_type == TokenType.ELSE:
                           if self.config_parser:
                                self.config_parser.print_log_message('DEBUG', "ms_sql_connector: _parse_block: Encountered ELSE in Block. Treating as implicit block end.")
                           break

                      msg = f"DEBUG: Processed token {self._curr} but did not advance. Force advance."
                      if self.config_parser:
                           self.config_parser.print_log_message('DEBUG', msg)
                      else:
                           logging.debug(msg)
                      self._advance()
                 last_token_idx = current_idx

                 stmt = self._parse_statement()
                 if stmt:
                      expressions.append(stmt)
                 self._match(TokenType.SEMICOLON)

            self._match(TokenType.END)
            return Block(expressions=expressions)

        def _parse_if(self):
            res = self.expression(
                 exp.If,
                 this=self._parse_conjunction(),
                 true=self._parse_statement(),
                 false=self._parse_statement() if self._match(TokenType.ELSE) else None,
            )
            return res

        STATEMENT_PARSERS[TokenType.BEGIN] = lambda self: self._parse_block()
        if hasattr(TokenType, 'IF'):
             STATEMENT_PARSERS[getattr(TokenType, 'IF')] = lambda self: self._parse_if()



    class Generator(TSQL.Generator):
        TRANSFORMS = TSQL.Generator.TRANSFORMS.copy()

        def _block_handler(self, expression):
            # Block handler needs to process children
            # Since sqlglot generator expects strings, we need to generate sql for children
            stmts = []
            if hasattr(expression, 'expressions'):
                for e in expression.expressions:
                    stmts.append(self.sql(e))
            return "\\n".join(stmts)

        TRANSFORMS[Block] = _block_handler

## Read once per connector and shared by the workers of the query conversion - see
## _get_udt_map(). A lock created on the instance would itself have to be created under a
## lock; there is one fetch per run behind this one, so a module level lock costs nothing.
UDT_MAP_LOCK = threading.Lock()


class MsSQLConnector(DatabaseConnector):

    ## What this connector does not read out of SQL Server - see
    ## DatabaseConnector.OBJECT_KINDS_NOT_READ. The user defined types ARE read, above.
    OBJECT_KINDS_NOT_READ = {
        'domains': ('SQL Server has rules (CREATE RULE) bound to a type or a column, which are '
                    'the closest thing it has to a domain constraint - the Sybase ASE connector '
                    'of this same migrator reads exactly those as domains. This connector does '
                    'not.'),
    }

    ## The ODBC type codes whose values pyodbc hands over as bytes, with the name each of them
    ## has in SQL Server. A message about a value which could not be decoded says which type it
    ## came from - the converter is registered per type code and knows nothing else about where
    ## the value stood.
    ODBC_TYPE_NAMES = {
        -155: 'datetimeoffset',
        -154: 'time',
        -152: 'xml',
        -151: 'udt',
        -150: 'sql_variant',
    }

    def __init__(self, config_parser, source_or_target):
        if source_or_target not in ['source']:
            raise ValueError(f"MS SQL Server is only supported as a source database. Current value: {source_or_target}")

        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.on_error_action = self.config_parser.get_on_error_action()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger

    ## ------------------------------------------------------------------------------------
    ## Bytes to text.
    ##
    ## The ODBC driver hands the wide and the extended types over as bytes and does not say
    ## which encoding they are in - which of utf-8 and utf-16 arrives depends on how the
    ## driver was built and configured, so both are tried and neither is a guess. What is a
    ## guess is what happens when neither reads the value, and until 0.16.0 the answer was
    ## errors='ignore' three times over: the byte was deleted from the value, the row reached
    ## the target shorter than it left the source, and nothing said so. The decision is
    ## migration.on_undecodable_bytes now and it is applied in text_decoding.py, which counts
    ## every value it had to touch and reports the total when the connection is closed.

    def text_decoder(self):
        """The decoder of this connection, created on first use so that it is never missing."""
        decoder = getattr(self, '_text_decoder', None)
        if decoder is None:
            decoder = TextDecoder(self.config_parser, 'ms_sql_connector')
            self._text_decoder = decoder
        return decoder

    def decode_odbc_value(self, value, type_code):
        """
        One value of one of the byte-valued ODBC types, as text.

        A value which carries a byte order mark is utf-16 whatever the driver was built for,
        so that encoding is tried first for it; everything else follows the order the
        connector has always used, utf-8 before utf-16.
        """
        if isinstance(value, (bytes, bytearray)) and bytes(value[:2]) in (b'\xff\xfe', b'\xfe\xff'):
            encodings = ('utf-16', 'utf-8')
        else:
            encodings = None
        place = f"SQL type {type_code} ({self.ODBC_TYPE_NAMES.get(type_code, 'unknown')})"
        return self.text_decoder().decode(value, place=place, encodings=encodings)

    def connect(self):
        if self.config_parser.get_connectivity(self.source_or_target) == 'odbc':
            connection_string = self.config_parser.get_connect_string(self.source_or_target)
            self.connection = pyodbc.connect(connection_string, autocommit=True)

            def handle_datetimeoffset(value, type_code=-155):
                if value is None:
                    return None
                if isinstance(value, bytes) and len(value) == 20:
                    year, month, day, hour, minute, second, fraction, tz_hour, tz_minute = struct.unpack("<hhhhhhIhh", value)
                    sec_frac = fraction // 1000
                    tz_total_min = tz_hour * 60 + (tz_minute if tz_hour >= 0 else -tz_minute)
                    tz_sign = "+" if tz_total_min >= 0 else "-"
                    abs_tz_min = abs(tz_total_min)
                    abs_tz_h = abs_tz_min // 60
                    abs_tz_m = abs_tz_min % 60
                    return f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}.{sec_frac:06d}{tz_sign}{abs_tz_h:02d}:{abs_tz_m:02d}"
                ## Not the 20 byte structure the type has: it is read as text rather than as
                ## str(value), which used to write the repr of the bytes - b'...' - into the
                ## target as if it were the value.
                return self.decode_odbc_value(value, type_code)

            def handle_ss_udt(value, type_code=-151):
                if value is None:
                    return None
                if isinstance(value, bytes):
                    return value
                return str(value).encode('utf-8')

            def handle_string_converter(value, type_code=None):
                if value is None:
                    return None
                return self.decode_odbc_value(value, type_code)

            for type_code, converter in [
                (-155, handle_datetimeoffset),
                (-151, handle_ss_udt),
                (-152, handle_string_converter),
                (-150, handle_string_converter),
                (-154, handle_string_converter),
            ]:
                try:
                    ## pyodbc calls a converter with the value alone, so the type code the
                    ## converter is registered for is bound here - it is the only thing a
                    ## message about an undecodable value can say about where it stood.
                    self.connection.add_output_converter(
                        type_code, lambda value, converter=converter, type_code=type_code:
                            converter(value, type_code))
                except Exception as e:
                    self.config_parser.print_log_message('DEBUG', f"ms_sql_connector: connect: Warning registering output converter for {type_code}: {e}")
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
        try:
            self.connection.autocommit = True
        except Exception:
            pass

    def disconnect(self):
        try:
            ## How many values did not fit any of the encodings expected for them, before the
            ## connection which read them is gone. Nothing is written when there were none.
            decoder = getattr(self, '_text_decoder', None)
            if decoder is not None:
                decoder.log_summary()
        except Exception:
            pass
        try:
            if self.connection:
                self.connection.close()
        except Exception as e:
            pass
        finally:
            detach_thread_from_jvm()

    def get_sql_functions_mapping(self, settings):
        """ Returns a dictionary of SQL functions mapping for the target database """
        target_db_type = settings['target_db_type']
        if target_db_type == 'postgresql':
            return {
                'getdate()': 'current_timestamp',
                'getutcdate()': "timezone('UTC', now())",
                'sysdatetime()': 'current_timestamp',
                'year(': 'extract(year from ',
                'month(': 'extract(month from ',
                'day(': 'extract(day from ',
                'db_name()': 'current_database()',
                'original_db_name()': 'current_database()',
                'suser_name()': 'current_user',
                'suser_sname()': 'current_user',
                'user_name()': 'current_user',
                'len(': 'length(',
                'datalength(': 'octet_length(',
                'isnull(': 'coalesce(',
                'substring(': 'substring(',
                'charindex(': 'position(',
                'replace(': 'replace(',
                'stuff(': 'overlay(',
                'lower(': 'lower(',
                'upper(': 'upper(',
                'ltrim(': 'ltrim(',
                'rtrim(': 'rtrim(',
                'space(': "repeat(' ', ",
                'replicate(': 'repeat(',
                # 'dateadd(': "mapped via transpiler custom logic often or requires complex rewriting",
                # 'datediff(': "requires age() logic",
            }
        else:
            self.config_parser.print_log_message('ERROR', f"ms_sql_connector: get_sql_functions_mapping: Unsupported target database type: {target_db_type}")
            return {}

    def migrate_sequences(self, target_connector, settings):
        return True

    def fetch_table_names(self, table_schema: str):
        query = f"""
            SELECT
                t.object_id AS table_id,
                s.name AS schema_name,
                t.name AS table_name
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = '{table_schema}'
            ORDER BY t.name
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
                    'schema_name': row[1],
                    'table_name': row[2],
                    'comment': ''
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return tables
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ms_sql_connector: fetch_table_names: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_table_columns(self, settings) -> dict:
        table_schema = settings['table_schema']
        table_name = settings['table_name']
        result = {}
        if self.config_parser.get_system_catalog() == 'INFORMATION_SCHEMA':
            query = f"""
                SELECT
                    c.ordinal_position,
                    c.column_name,
                    c.data_type,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    c.is_nullable,
                    'NO' AS is_identity,
                    c.column_default
                FROM information_schema.columns c
                WHERE c.table_schema = '{table_schema}' AND c.table_name = '{table_name}'
                ORDER BY c.ordinal_position
            """
        elif self.config_parser.get_system_catalog() in ('SYS', 'NONE'):
            query = f"""
                SELECT
                    c.column_id AS ordinal_position,
                    c.name AS column_name,
                    CASE
                        WHEN t.is_user_defined = 1 THEN st.name
                        ELSE t.name
                    END AS data_type,
                    c.max_length AS length,
                    c.precision AS numeric_precision,
                    c.scale AS numeric_scale,
                    c.is_nullable,
                    c.is_identity,
                    dc.definition AS default_value
                FROM sys.columns c
                JOIN sys.tables tb ON c.object_id = tb.object_id
                JOIN sys.schemas s ON tb.schema_id = s.schema_id
                JOIN sys.types t ON c.user_type_id = t.user_type_id
                LEFT JOIN sys.types st ON t.system_type_id = st.user_type_id AND st.is_user_defined = 0
                LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
                WHERE s.name = '{table_schema}' AND tb.name = '{table_name}'
                ORDER BY c.column_id
            """
        else:
            raise ValueError(f"Unsupported system catalog: {self.config_parser.get_system_catalog()}")
        try:
            self.connect()
            cursor = self.connection.cursor()
            self.config_parser.print_log_message('DEBUG2', f"ms_sql_connector: fetch_table_columns: MSSQL: Reading columns for {table_schema}.{table_name}")
            cursor.execute(query)
            for row in cursor.fetchall():
                ordinal_position = row[0]
                column_name = row[1]
                data_type = row[2]
                character_maximum_length = row[3]
                numeric_precision = row[4]
                numeric_scale = row[5]
                is_nullable = row[6]
                is_identity = row[7]
                column_default = row[8]

                column_type = data_type.upper()
                target_db_type = settings.get('target_db_type', self.config_parser.get_target_db_type())
                types_mapping = self.get_types_mapping({'target_db_type': target_db_type})
                mapped_type = types_mapping.get(column_type, column_type)

                if self.is_string_type(column_type) and character_maximum_length is not None:
                    # In SQL Server, NVARCHAR(MAX) and VARCHAR(MAX) return -1 for max_length
                    if character_maximum_length == -1:
                        mapped_type = 'TEXT'
                    else:
                        mapped_type += f"({character_maximum_length})"
                elif self.is_numeric_type(column_type) and numeric_precision is not None and numeric_scale is not None:
                    mapped_type += f"({numeric_precision}, {numeric_scale})"
                elif self.is_numeric_type(column_type) and numeric_precision is not None:
                    mapped_type += f"({numeric_precision})"
                
                column_type = mapped_type

                if self.config_parser.get_source_db_type() == 'sybase_ase':
                    is_identity_bool = bool(is_identity is not None and (int(is_identity) & 128) == 128)
                else:
                    if str(is_identity).strip().upper() in ('YES', 'TRUE', '1'):
                        is_identity_bool = True
                    else:
                        is_identity_bool = False

                result[ordinal_position] = {
                    'column_name': column_name,
                    'data_type': data_type,
                    'column_type': column_type,
                    'character_maximum_length': character_maximum_length,
                    'numeric_precision': numeric_precision,
                    'numeric_scale': numeric_scale,
                    'is_nullable': 'YES' if is_nullable else 'NO',
                    'is_identity': 'YES' if is_identity_bool else 'NO',
                    'column_default_value': column_default if not is_identity_bool else None,
                    'comment': ''
                }

            cursor.close()
            self.disconnect()
            return result
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ms_sql_connector: fetch_table_columns: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_types_mapping(self, settings):
        target_db_type = settings['target_db_type']
        types_mapping = {}
        if target_db_type == 'postgresql':
            types_mapping = {
                'UNIQUEIDENTIFIER': 'UUID',
                'ROWVERSION': 'BYTEA',
                'SQL_VARIANT': 'BYTEA',

                'BIGDATETIME': 'TIMESTAMP',
                'DATE': 'DATE',
                'DATETIME': 'TIMESTAMP',
                'DATETIME2': 'TIMESTAMP',
                'DATETIMEOFFSET': 'TIMESTAMPTZ',
                'BIGTIME': 'TIMESTAMP',
                'SMALLDATETIME': 'TIMESTAMP',
                'TIME': 'TIME',
                'TIMESTAMP': 'BYTEA',
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
                'GEOMETRY': 'BYTEA',
                'GEOGRAPHY': 'BYTEA',
                'HIERARCHYID': 'BYTEA',
                'CHAR': 'CHAR',
                'NCHAR': 'CHAR',
                'UNICHAR': 'CHAR',
                'NVARCHAR': 'VARCHAR',
                'UNIVARCHAR': 'VARCHAR',
                'TEXT': 'TEXT',
                'NTEXT': 'TEXT',
                'SYSNAME': 'TEXT',
                'LONGSYSNAME': 'TEXT',
                'LONG VARCHAR': 'TEXT',
                'LONG NVARCHAR': 'TEXT',
                'UNITEXT': 'TEXT',
                'VARCHAR': 'VARCHAR',
                'XML': 'XML',

                'CLOB': 'TEXT',
                'DECIMAL': 'DECIMAL',
                'DOUBLE PRECISION': 'DOUBLE PRECISION',
                'FLOAT': 'FLOAT',
                'INTERVAL': 'INTERVAL',
                # 'MONEY': 'MONEY',
                # 'SMALLMONEY': 'MONEY',
                'MONEY': 'NUMERIC(19,4)',
                'SMALLMONEY': 'NUMERIC(10,4)',
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
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']
        table_indexes = {}
        order_num = 1
        query = f"""
            SELECT
                i.name AS index_name,
                i.is_unique,
                i.is_primary_key,
                STUFF(
                    (SELECT ', "' + c.name + '"'
                     FROM sys.index_columns ic2
                     JOIN sys.columns c ON ic2.object_id = c.object_id AND ic2.column_id = c.column_id
                     WHERE ic2.object_id = {source_table_id} AND ic2.index_id = i.index_id
                     ORDER BY ic2.index_column_id
                     FOR XML PATH('')),
                    1, 2, ''
                ) AS column_list,
                (SELECT COUNT(*)
                 FROM sys.index_columns ic3
                 JOIN sys.columns c3 ON ic3.object_id = c3.object_id AND ic3.column_id = c3.column_id
                 JOIN sys.types ty ON c3.user_type_id = ty.user_type_id
                 WHERE ic3.object_id = {source_table_id} AND ic3.index_id = i.index_id
                   AND ty.name IN ('xml', 'image', 'text', 'ntext', 'hierarchyid', 'geometry', 'geography')
                ) AS has_unsupported_col
            FROM sys.indexes i
            WHERE i.object_id = {source_table_id} AND i.type IN (1, 2)
            ORDER BY i.name
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)

            indexes = cursor.fetchall()

            for index in indexes:
                self.config_parser.print_log_message('DEBUG', f"ms_sql_connector: fetch_indexes: Processing index: {index}")
                if index[0] is None:
                    continue
                index_name = index[0].strip()
                index_unique = index[1]  ## integer 0 or 1
                index_primary_key = index[2]  ## integer 0 or 1
                index_columns = index[3].strip() if index[3] else ''
                has_unsupported_col = index[4] or 0
                index_owner = ''

                if has_unsupported_col > 0 or not index_columns:
                    self.config_parser.print_log_message('WARNING', f"ms_sql_connector: fetch_indexes: Skipping index {index_name} on table {source_table_name} as it contains unsupported column data types.")
                    continue

                table_indexes[order_num] = {
                    'index_name': index_name,
                    'index_type': "PRIMARY KEY" if index_primary_key == 1 else "UNIQUE" if index_unique == 1 and index_primary_key == 0 else "INDEX",
                    'index_owner': index_owner,
                    'index_columns': index_columns,
                    'index_comment': ''
                }
                order_num += 1

            cursor.close()
            self.disconnect()
            return table_indexes

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ms_sql_connector: fetch_indexes: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_index_sql(self, settings):
        return ""

    def fetch_constraints(self, settings):
        """
        Fetches table constraints from the source database and prepares them for migration.
        MS SQL Server has several sys objects which show constraints:
        sys.key_constraints - primary key and unique constraints
        sys.check_constraints - check constraints
        sys.foreign_keys - foreign key constraints
        sys.default_constraints - default constraints
        """
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

        order_num = 1
        table_constraints = {}
        """
        The following was not tested so far but as STRING_ARG is not available in MSSQL-Service 2016 I just replaced it with this
        """
        query = f"""
            SELECT
                fk.name AS constraint_name,
                'FOREIGN KEY' AS constraint_type,
                STUFF(
                    (SELECT ', "' + cc.name + '"'
                     FROM sys.foreign_key_columns fkc2
                     JOIN sys.columns cc ON fkc2.parent_object_id = cc.object_id AND fkc2.parent_column_id = cc.column_id
                     WHERE fkc2.constraint_object_id = fk.object_id
                     ORDER BY cc.column_id
                     FOR XML PATH('')),
                    1, 2, ''
                ) AS constraint_columns,
                rt.name AS referenced_table,
                STUFF(
                    (SELECT ', "' + rc.name + '"'
                     FROM sys.foreign_key_columns fkc3
                     JOIN sys.columns rc ON fkc3.referenced_object_id = rc.object_id AND fkc3.referenced_column_id = rc.column_id
                     WHERE fkc3.constraint_object_id = fk.object_id
                     ORDER BY rc.column_id
                     FOR XML PATH('')),
                    1, 2, ''
                ) AS referenced_columns,
                pt.name AS constraint_table,
                rs.name AS referenced_schema
            FROM sys.foreign_keys fk
            JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
            JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
            JOIN sys.tables pt ON fk.parent_object_id = pt.object_id
            WHERE fk.parent_object_id = {source_table_id}
            ORDER BY fk.name
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)

            constraints = cursor.fetchall()

            for constraint in constraints:
                self.config_parser.print_log_message('DEBUG', f"ms_sql_connector: fetch_constraints: Processing constraint: {constraint}")
                constraint_name = constraint[0].strip()
                constraint_type = constraint[1].strip()
                constraint_columns = constraint[2].strip()
                referenced_table = constraint[3].strip()
                referenced_columns = constraint[4].strip()
                referenced_schema = constraint[6].strip()
                constraint_owner = ''

                table_constraints[order_num] = {
                    'constraint_name': constraint_name,
                    'constraint_type': constraint_type,
                    'constraint_owner': constraint_owner,
                    'constraint_columns': constraint_columns,
                    'referenced_table_schema': referenced_schema,
                    'referenced_table_name': referenced_table,
                    'referenced_columns': referenced_columns,
                    'constraint_sql': '',
                    'constraint_comment': ''
                }
                order_num += 1

            cursor.close()
            self.disconnect()
            return table_constraints
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ms_sql_connector: fetch_constraints: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_constraint_sql(self, settings):
        return ""

    def get_aliases(self, settings):
        source_schema_name = settings.get('source_schema_name')
        aliases = {}
        order_num = 1
        query = f"""
            SELECT
                s.name AS alias_name,
                PARSENAME(s.base_object_name, 2) AS aliased_schema_name,
                PARSENAME(s.base_object_name, 1) AS aliased_table_name,
                SCHEMA_NAME(s.schema_id) AS alias_owner,
                s.base_object_name
            FROM sys.synonyms s
            WHERE SCHEMA_NAME(s.schema_id) = '{source_schema_name}'
            ORDER BY s.name
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
                alias_sql = f"CREATE SYNONYM [{alias_owner}].[{alias_name}] FOR [{aliased_schema_name}].[{aliased_table_name}]"

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
            self.config_parser.print_log_message('ERROR', f"ms_sql_connector: get_aliases: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_funcproc_names(self, schema: str):
        query = f"""
            SELECT
                p.object_id AS id,
                p.name AS name,
                CASE
                    WHEN p.type = 'P' THEN 'Procedure'
                    WHEN p.type IN ('FN', 'TF', 'IF') THEN 'Function'
                    ELSE 'Unknown'
                END AS type
            FROM sys.objects p
            JOIN sys.schemas s ON p.schema_id = s.schema_id
            WHERE s.name = '{schema}'
              AND p.type IN ('P', 'FN', 'TF', 'IF')
              AND p.is_ms_shipped = 0
            ORDER BY p.name
        """
        self.config_parser.print_log_message('DEBUG3', f"ms_sql_connector: fetch_funcproc_names: query: {query}")
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()

            funcprocs = {}
            order_num = 1
            for row in rows:
                funcprocs[order_num] = {
                    'id': row[0],
                    'name': row[1],
                    'type': row[2],
                    'comment': ''
                }
                order_num += 1
            self.disconnect()
            return funcprocs
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ms_sql_connector: fetch_funcproc_names: Error fetching function/procedure names: {e}")
            return []

    def fetch_funcproc_code(self, funcproc_id: int):
        # 1. Fetch Definition
        query_def = f"""
            SELECT m.definition
            FROM sys.sql_modules m
            WHERE m.object_id = {funcproc_id}
        """

        # 2. Fetch Return Schema (for implicit result sets in Procedures)
        # Using sys.dm_exec_describe_first_result_set (SQL Server 2012+)
        query_schema = f"""
            SELECT
                name,
                system_type_name,
                max_length,
                precision,
                scale,
                is_nullable
            FROM sys.dm_exec_describe_first_result_set_for_object({funcproc_id}, 0)
            WHERE name IS NOT NULL
        """

        try:
            self.connect()
            cursor = self.connection.cursor()

            # Fetch Code
            cursor.execute(query_def)
            row = cursor.fetchone()
            definition = row[0] if row else None

            schema = []
            if definition:
                try:
                    cursor.execute(query_schema)
                    schema_rows = cursor.fetchall()
                    for s in schema_rows:
                        # Col Name, Type, Len, Prec, Scale, Nullable
                        schema.append({
                            'name': s[0],
                            'type': s[1],
                            'length': s[2],
                            'precision': s[3],
                            'scale': s[4],
                            'nullable': s[5]
                        })
                except Exception as ex_schema:
                    # DMV might not exist or parsing error
                    self.config_parser.print_log_message('DEBUG', f"ms_sql_connector: fetch_funcproc_code: Schema discovery failed (ignoring): {ex_schema}")

            cursor.close()
            self.disconnect()

            if definition:
                return {
                    'definition': definition,
                    'return_schema': schema
                }
            return None
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ms_sql_connector: fetch_funcproc_code: Error fetching function/procedure code for id {funcproc_id}: {e}")
            return None

    def convert_funcproc_code(self, settings):
        funcproc_code_input = settings['funcproc_code']
        # Handle dict input (with schema) vs string input
        if isinstance(funcproc_code_input, dict):
             funcproc_code = funcproc_code_input.get('definition', '')
             implicit_return_schema = funcproc_code_input.get('return_schema', [])
        else:
             funcproc_code = str(funcproc_code_input)
             implicit_return_schema = []

        target_schema_name = settings['target_schema_name']

        # 1. Cleanup
        funcproc_code = funcproc_code.strip()
        # Remove usage of GO
        funcproc_code = re.sub(r'\bGO\b', '', funcproc_code, flags=re.IGNORECASE)

        # Initialize TsqlParser
        # Standardize MS SQL bracket identifiers to PostgreSQL double quotes
        funcproc_code = self._rewrite_outside_string_literals(
            funcproc_code, lambda fragment: re.sub(r'\[([^\]]+)\]', r'"\1"', fragment)
        )

        ## The statements of the routine are converted for PostgreSQL like the query of a
        ## view - without the converter the parser only rearranged them, so `getdate()`,
        ## the string concatenation with '+' and the schema of the source reached the
        ## target unchanged.
        parser = TsqlParser(funcproc_code, self.config_parser,
                            view_converter=self.convert_statement_code, settings=settings,
                            functions_mapping_converter=self.apply_sql_functions_mapping)
        final_output = parser.run()

        # Reconstruct header string to parse parameters
        header_str = "\n".join(l.content for l in parser.header_lines)

        # 2. Identify Type and Name
        type_match = re.search(r'CREATE\s+(?:OR\s+ALTER\s+)?(PROC|PROCEDURE|FUNCTION)\s+(?:(\[.*?\]|".*?"|[\w]+)\.)?(\[.*?\]|".*?"|[\w]+)', header_str, re.IGNORECASE)

        if not type_match:
             return f"/* FAILED TO PARSE DEFINITION */\n{funcproc_code}"

        obj_type_raw = type_match.group(1).upper()
        obj_name = type_match.group(3).strip('[]"')

        is_proc = 'PROC' in obj_type_raw
        pg_type = 'PROCEDURE' if is_proc else 'FUNCTION'

        is_implicit_return = False
        if is_proc and implicit_return_schema:
             pg_type = 'FUNCTION'
             is_implicit_return = True

        self.config_parser.print_log_message('DEBUG', f"ms_sql_connector: convert_funcproc_code: DEBUG Parsing: obj_type_raw={obj_type_raw}, is_proc={is_proc}, pg_type={pg_type}, implicit_return={is_implicit_return}")

        types_mapping = self._get_variable_types_mapping(settings)

        # 4. Construct Header (Parameters)
        header_clean = header_str[type_match.end():].strip()

        returns_clause = "RETURNS VOID"
        if is_implicit_return:
             col_defs = []
             for col in implicit_return_schema:
                  c_name = col['name']
                  c_type = col.get('system_type_name', 'text')
                  t_mapped = self._apply_data_type_substitutions(c_type)
                  t_mapped = self._apply_udt_to_base_type_substitutions(t_mapped, settings)
                  for ms, pg_tgt in types_mapping.items():
                       t_mapped = re.sub(rf'\b{re.escape(ms)}\b', pg_tgt, t_mapped, flags=re.IGNORECASE)
                  col_defs.append(f'"{c_name}" {t_mapped}')
             if col_defs:
                  returns_clause = f"RETURNS TABLE ({', '.join(col_defs)})"
        elif not is_proc:
             ret_match = re.search(r'\bRETURNS\s+(.*)', header_clean, re.IGNORECASE)
             if ret_match:
                  ret_type_raw = ret_match.group(1).strip()
                  if 'TABLE' in ret_type_raw.upper():
                       returns_clause = f"RETURNS TABLE ({ret_type_raw.split('TABLE', 1)[-1].strip()})"
                       returns_clause = self._apply_data_type_substitutions(returns_clause)
                  else:
                       ret_mapped = self._apply_data_type_substitutions(ret_type_raw)
                       ret_mapped = self._apply_udt_to_base_type_substitutions(ret_mapped, settings)
                       for ms_type, pg_target_type in types_mapping.items():
                           ret_mapped = re.sub(rf'\b{re.escape(ms_type)}\b', pg_target_type, ret_mapped, flags=re.IGNORECASE)
                       returns_clause = f"RETURNS {ret_mapped}"
                  header_clean = header_clean[:ret_match.start()].strip()

        pg_params = []
        if header_clean:
            as_match = re.search(r'\bAS\b', header_clean, re.IGNORECASE)
            if as_match:
                header_clean = header_clean[:as_match.start()].strip()

            if header_clean.startswith('(') and header_clean.endswith(')'):
                header_clean = header_clean[1:-1]

            params_list = re.split(r',(?![^(]*\))', header_clean)
            for p in params_list:
                p = p.strip()
                if not p: continue
                p_match = re.search(r'^\s*(?:@|locvar_)?([\w]+)\s+([A-Za-z_][\w\s\(\),]*?)(?:\s+(OUTPUT|OUT|=.*)|$)', p, flags=re.IGNORECASE)
                if p_match:
                    p_name = p_match.group(1)
                    p_type = p_match.group(2).strip()
                    p_rest = p_match.group(3) or ""
                    p_type = self._apply_data_type_substitutions(p_type)
                    p_type = self._apply_udt_to_base_type_substitutions(p_type, settings)
                    for ms_type, pg_target_type in types_mapping.items():
                        p_type = re.sub(rf'\b{re.escape(ms_type)}\b', pg_target_type, p_type, flags=re.IGNORECASE)
                    mode = "INOUT " if "OUTPUT" in p_rest.upper() else ""
                    default_val = ""
                    def_match = re.search(r'=\s*([^ ]+)', p_rest)
                    if def_match:
                        default_val = f" DEFAULT {def_match.group(1)}"
                    pg_params.append(f"{mode}locvar_{p_name} {p_type}{default_val}")

        pg_params_str = ", ".join(pg_params)
        pg_name = f'"{obj_name}"'

        pg_header_str = f"CREATE OR REPLACE {pg_type} \"{target_schema_name}\".{pg_name}({pg_params_str})\n"
        if pg_type == 'FUNCTION':
            pg_header_str += f"{returns_clause} AS"
        else:
            pg_header_str += "AS"

        final_output = parser.pass_11_assemble_output(pg_header_str)
        parser.pass_12_add_if_levels(final_output)

        ddl = ""
        def get_indent(level):
            return "    " * max(0, level)

        indent_level = 0
        in_body = False
        first_begin_found = False

        for index, line_obj in enumerate(final_output):
            stripped = line_obj.content.strip()
            current_indent = indent_level

            is_begin = bool(re.match(r'^BEGIN\b', stripped, re.IGNORECASE))
            is_end = bool(re.match(r'^END;', stripped, re.IGNORECASE))

            if stripped.upper() == "DECLARE":
                indent_level = 0
                in_body = True
                ddl += get_indent(0) + line_obj.content + "\n"
                indent_level = 1
                continue

            if stripped == "$$":
                indent_level = 0
                ddl += get_indent(0) + line_obj.content + "\n"
                in_body = True
                continue

            if stripped.upper() == "$$ LANGUAGE PLPGSQL;":
                indent_level = 0
                ddl += get_indent(0) + line_obj.content + "\n"
                continue

            if not in_body:
                current_indent = 0
                ddl += get_indent(0) + line_obj.content + "\n"
                continue

            if is_begin:
                if not first_begin_found:
                    first_begin_found = True
                    current_indent = 0
                    indent_level = 1
                else:
                    current_indent = indent_level
                    indent_level += 1
            elif is_end:
                indent_level -= 1
                current_indent = indent_level
                if indent_level < 0:
                    indent_level = 0
                    current_indent = 0

            ddl += get_indent(current_indent) + line_obj.content + "\n"

        return ddl

    def fetch_views_names(self, owner_name):
        views = {}
        order_num = 1
        query = f"""
            SELECT
                v.object_id AS id,
                s.name AS schema_name,
                v.name AS view_name
            FROM sys.views v
            JOIN sys.schemas s ON v.schema_id = s.schema_id
            WHERE s.name = '{owner_name}'
            ORDER BY v.name
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
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ms_sql_connector: fetch_views_names: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_view_code(self, settings):
        view_id = settings['view_id']
        source_schema_name = settings['source_schema_name']
        source_view_name = settings['source_view_name']
        target_schema_name = settings['target_schema_name']
        target_view_name = settings['target_view_name']
        view_code = ''
        query = f"""
            SELECT m.definition
            FROM sys.sql_modules m
            WHERE m.object_id = {view_id}
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                view_code = row[0]
                self.config_parser.print_log_message('DEBUG', f"ms_sql_connector: fetch_view_code: View code for {source_schema_name}.{source_view_name}: {view_code}")
                return view_code
            cursor.close()
            self.disconnect()
            return view_code
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ms_sql_connector: fetch_view_code: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def convert_statement_code(self, settings: dict):
        """
        A statement of the source converted for PostgreSQL, without any wrapper around it.

        Used for the query of a view and for every statement of a routine - a routine used not
        to be converted at all, so `getdate()` and the string concatenation with '+' reached
        the target as they were written for MS SQL Server. `settings['view_code']` carries the
        statement, `source_schema_name` and `target_schema_name` the schemas. Raises ValueError
        when the statement cannot be parsed, so a caller can keep the original text.
        """
        # Fetch UDT map for substitution
        udt_lookup_map = self._get_udt_map()

        def quote_column_names(node):
            if isinstance(node, sqlglot.exp.Column) and node.name and node.name != '*':
                node.set("this", sqlglot.exp.Identifier(this=node.name, quoted=True))
            if isinstance(node, sqlglot.exp.Alias) and isinstance(node.args.get("alias"), sqlglot.exp.Identifier):
                alias = node.args["alias"]
                if not alias.args.get("quoted"):
                    alias.set("quoted", True)
            return node

        def replace_schema_names(node):
            if isinstance(node, (sqlglot.exp.Table, sqlglot.exp.Column)):
                schema = node.args.get("db")
                if schema and schema.name == settings['source_schema_name']:
                    node.set("db", sqlglot.exp.Identifier(this=settings['target_schema_name'], quoted=False))
            return node

        def quote_schema_and_table_names(node):
            if isinstance(node, (sqlglot.exp.Table, sqlglot.exp.Column)):
                # Quote schema name if present
                schema = node.args.get("db")
                if schema and getattr(schema, "set", None) and not schema.args.get("quoted"):
                    schema.set("quoted", True)

                # Quote table name
                if isinstance(node, sqlglot.exp.Table):
                    table = node.args.get("this")
                    alias = node.args.get("alias")
                    if alias:
                        alias_id = alias.args.get("this")
                        if alias_id and getattr(alias_id, "set", None) and not alias_id.args.get("quoted"):
                            alias_id.set("quoted", True)
                else:
                    table = node.args.get("table")

                if table and getattr(table, "set", None) and not table.args.get("quoted"):
                    table.set("quoted", True)
            return node

        def replace_functions(node):
            mapping = self.get_sql_functions_mapping({ 'target_db_type': settings['target_db_type'] })
            # Prepare mapping for function names (without parentheses)
            func_name_map = {}
            ## A mapping written as a complete call ('user_name()') or as a plain name replaces
            ## the whole expression, while one written as a prefix ('len(') only renames the
            ## function and keeps its arguments.
            whole_expression_replacements = set()
            for k, v in mapping.items():
                if k.endswith('('):
                    func_name_map[k[:-1].lower()] = v[:-1] if v.endswith('(') else v
                elif k.endswith('()'):
                    func_name_map[k[:-2].lower()] = v
                    whole_expression_replacements.add(k[:-2].lower())
                else:
                    func_name_map[k.lower()] = v
                    whole_expression_replacements.add(k.lower())

            if isinstance(node, sqlglot.exp.Anonymous):
                func_name = node.name.lower()
                if func_name in func_name_map:
                    mapped = func_name_map[func_name]
                    ## The function was called without arguments and its replacement stands for
                    ## the whole call, so the replacement is taken as the expression it is. Only
                    ## the name was replaced before, which left the parentheses of the call
                    ## around it: 'user_name()' became 'CURRENT_USER()', which PostgreSQL refuses
                    ## with 'syntax error at or near "("'.
                    if not node.expressions and func_name in whole_expression_replacements:
                        replacement = self.mapped_function_expression(mapped)
                        if replacement is not None:
                            return replacement
                    # If mapped is a function name, replace the function name
                    if '(' not in mapped:
                        node.set("this", sqlglot.exp.Identifier(this=mapped, quoted=False))
                    else:
                        # For mappings like 'year(' -> 'extract(year from '
                        # We need to rewrite the function call
                        if mapped.startswith('extract('):
                            # e.g. year(t1.b) -> extract(year from t1.b)
                            arg = node.args.get("expressions")
                            if arg and len(arg) == 1:
                                part = func_name
                                return sqlglot.exp.Extract(
                                    this=sqlglot.exp.Identifier(this=part, quoted=False),
                                    expression=arg[0]
                                )
                        else:
                            # Iterate over the mapping to handle function name replacements
                            for orig, repl in mapping.items():
                                # Handle mappings ending with '(' (function calls)
                                if orig.endswith('(') and func_name == orig[:-1].lower():
                                    if repl.endswith('('):
                                        node.set("this", sqlglot.exp.Identifier(this=repl[:-1], quoted=False))
                                    else:
                                        node.set("this", sqlglot.exp.Identifier(this=repl, quoted=False))
                                    break
                                # Handle mappings ending with '()' (function calls with no args)
                                elif orig.endswith('()') and func_name == orig[:-2].lower():
                                    node.set("this", sqlglot.exp.Identifier(this=repl, quoted=False))
                                    break
                    # For direct function name replacements, handled above
                # For functions like getdate(), getutcdate(), etc.
                elif func_name + "()" in func_name_map:
                    mapped = func_name_map[func_name + "()"]
                    return self.mapped_function_expression(mapped) or sqlglot.exp.Anonymous(this=mapped)
            return node

        def replace_udts(node):
            if isinstance(node, sqlglot.exp.DataType):
                # Check if the type is a UDT
                type_name = node.this.name if hasattr(node.this, 'name') else str(node.this)

                if type_name in udt_lookup_map:
                     udt_info = udt_lookup_map[type_name]
                     return sqlglot.exp.DataType.build(udt_info['sql']) # Use 'sql' or 'definition'
            return node

        def cast_arithmetic_operands(node):
            def peel_parentheses_and_cast(n):
                if isinstance(n, sqlglot.exp.Column):
                    return sqlglot.exp.Cast(this=n, to=sqlglot.exp.DataType.build('numeric'))
                elif isinstance(n, sqlglot.exp.Paren):
                    inner = n.args.get("this")
                    if inner:
                        n.set("this", peel_parentheses_and_cast(inner))
                return n

            if isinstance(node, (sqlglot.exp.Mul, sqlglot.exp.Div, sqlglot.exp.Add, sqlglot.exp.Sub)):
                for arg in ["this", "expression"]:
                    child = node.args.get(arg)
                    if child:
                        node.set(arg, peel_parentheses_and_cast(child))
            return node

        def convert_legacy_outer_joins(expression):
            """
            The '*=' and '=*' outer joins of the old T-SQL, as the joins of PostgreSQL.

            MS SQL Server read these until 2005 and the application files of a database old
            enough to be migrated are full of them. This connector used to leave them alone -
            the comment which stood here said "we assume ANSI joins for now" - so a statement
            no parser can read was reported as unreadable, while sybase_ase, which is the same
            family and writes the same operator, converted it. The work is shared with Sybase
            ASE and with Oracle's '(+)' and stands in query_conversion/outer_joins.py; only
            the marking is done in prepare_query_for_parsing() above.
            """
            converted_joins = set()
            expression, unconverted = query_outer_joins.convert_marked_outer_joins(
                expression, converted_joins)
            if unconverted:
                self.config_parser.print_log_message(
                    'WARNING', f"ms_sql_connector: convert_statement_code: {unconverted} outer "
                               f"join(s) written '*=' or '=*' could not be attributed to a table "
                               f"of the FROM clause and were not rewritten.")
            ## A restriction on the inner table belongs to the join in this dialect and to the
            ## result of the join in PostgreSQL, where it throws away the rows the outer join
            ## added - the LEFT JOIN would be an inner join again. It moves into the ON clause,
            ## and what moved is reported, because it decides which rows the statement answers.
            expression, moved = query_outer_joins.move_inner_table_predicates(
                expression, converted_joins)
            if moved:
                report = settings.setdefault('conversion_report', {})
                report['moved_predicates'] = report.get('moved_predicates', []) + moved
                self.config_parser.print_log_message(
                    'WARNING', f"ms_sql_connector: convert_statement_code: {', '.join(moved)} "
                               f"restrict the inner table of an outer join and were moved into "
                               f"its ON clause - in the WHERE clause of PostgreSQL they would "
                               f"undo the outer join.")
            return expression

        view_code = self.prepare_query_for_parsing(settings['view_code'])
        CustomTSQL.Parser.config_parser = self.config_parser
        try:
            expressions = sqlglot.parse(view_code, read=CustomTSQL)
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ms_sql_connector: transform_sybase_joins: Failed to parse view code: {e}")
            raise ValueError(f"-- ERROR parsing view: {e}\n/*\n"
                             f"{query_outer_joins.unmark_tsql_outer_joins(view_code)}\n*/") from e

        transformed_sqls = []
        for expression in expressions:
            try:
                # Unwrap CREATE VIEW to get the inner SELECT/query
                if isinstance(expression, sqlglot.exp.Create):
                    expression = expression.expression

                # Apply transformations
                expression = expression.transform(quote_column_names)
                expression = expression.transform(replace_functions)
                expression = expression.transform(replace_schema_names)
                expression = expression.transform(quote_schema_and_table_names)
                expression = expression.transform(replace_udts)
                ## '+' concatenates in MS SQL Server wherever one of its operands is text; it
                ## has to become '||' before the operands of the remaining arithmetic are cast,
                ## which would otherwise cast the parts of a concatenation to a number
                expression = expression.transform(self.convert_string_concatenation)
                expression = expression.transform(cast_arithmetic_operands)
                expression = convert_legacy_outer_joins(expression)

                ## the variables of the source keep their own spelling, the PostgreSQL generator
                ## would write '@v' as '$v' and the conversion of a routine renames '@v' later
                expression = expression.transform(self.keep_source_variables)

                pg_sql = expression.sql(dialect='postgres')
                ## the 'TRUE' the outer join rewrite leaves where the marked condition stood -
                ## "WHERE TRUE AND x" is "WHERE x", and the shorter one is what a developer reads
                pg_sql = query_outer_joins.tidy_boolean_placeholders(pg_sql)
                transformed_sqls.append(pg_sql)
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"ms_sql_connector: convert_statement_code: Failed to transform expression: {e}")
                transformed_sqls.append(f"-- ERROR transforming: {e}")

        converted_code = "\n".join(transformed_sqls)
        ## An outer join whose condition could not be attributed leaves its marker behind, and
        ## what stands around it is the comma join it started from - an INNER join. The view
        ## would be created and would answer fewer rows. Refused here, for the view path and
        ## the query path alike.
        marker_message = query_outer_joins.unconverted_marker_message(converted_code)
        if marker_message:
            error = ValueError(marker_message)
            ## it parsed and it converted - it is the outer join alone which could not be
            ## done, and a caller must not report it as a statement it could not read
            error.outer_join_failure = True
            raise error
        return converted_code

    def prepare_query_for_parsing(self, query_code):
        """
        The statement rewritten into something a T-SQL parser can read, without converting
        anything.

          '*=' and '=*' - the outer join MS SQL Server read until 2005. No parser of any
          dialect knows them, so they become an equality carrying a marker which says which
          side was outer, and convert_legacy_outer_joins() in convert_statement_code() turns
          the marker into a LEFT / RIGHT JOIN. sybase_ase does the same with the same shared
          code; this connector had nothing, so the identical statement converted from one
          source of the family and was reported as unreadable from the other.

          '$0' - a money literal. No parser of another dialect reads one: sqlglot takes it
          for an identifier, the conversion quotes it, and the target answers 'column "$0"
          does not exist' - or, for a number large enough to look like a placeholder,
          'there is no parameter $1000'. MONEY is migrated as NUMERIC(19,4), so the number
          alone is the whole value. The same shared code as in sybase_ase.

        It is used by convert_statement_code() - so the view path and the query path are given
        one preparation - and by the query conversion, which has to classify a statement
        before it converts it: a statement which cannot be parsed would be reported as one the
        migrator does not understand, and that answer must not be given to a statement its own
        connector converts.
        """
        if not query_code:
            return query_code
        prepared = query_outer_joins.mark_tsql_outer_joins(
            query_code, self.sql_without_literals_and_comments)
        return query_money_literals.convert_money_literals(
            prepared, self.sql_without_literals_and_comments)

    ## The source test of §8.1. SET NOEXEC ON makes the server compile every statement behind
    ## it - the names are resolved and the plan is made - and run none of them. It is a
    ## setting of the SESSION, so it is taken back in the cleanup whatever the statement did;
    ## a connection which cannot be put back is closed instead of being used again, because
    ## every statement of the migrator behind it would silently answer nothing.
    SOURCE_TEST_PARAMETER_STYLE = None

    def source_test_native_mechanism(self):
        return 'SET NOEXEC ON'

    def source_test_probe(self, sql, parameter_count=0):
        body = (sql or '').rstrip().rstrip(';')
        if not body:
            return [], []
        if parameter_count:
            ## a bind marker has no place in a batch which is submitted as text, and putting
            ## a literal in its place would compile another statement than the application
            ## runs. Not tested, and the block of the statement says so.
            return [], []
        return ['SET NOEXEC ON', body], ['SET NOEXEC OFF']

    def query_conversion_supported(self):
        return True

    def convert_query_code(self, settings: dict):
        """
        One statement of an application, converted for PostgreSQL - the same conversion the
        query of a view is given, without the CREATE VIEW around it. See the contract in
        DatabaseConnector.convert_query_code().
        """
        statement_id = settings.get('statement_id', '')
        ## the converter writes what it had to decide into this dictionary - it is made per
        ## call, so nothing is carried from one statement to the next or between threads
        statement_settings = {
            'view_code': settings['query_code'],
            'source_schema_name': settings['source_schema_name'],
            'target_schema_name': settings['target_schema_name'],
            'target_db_type': settings.get('target_db_type', 'postgresql'),
            'conversion_report': {},
        }
        try:
            converted = self.convert_statement_code(statement_settings)
        except ValueError as e:
            if getattr(e, 'outer_join_failure', False):
                return {'code': '', 'converted': False, 'warnings': [], 'error': first_line(e)}
            return {'code': '', 'converted': False, 'warnings': [],
                    'error': f"the statement could not be parsed as T-SQL: {first_line(e)}"}
        except Exception as e:
            return {'code': '', 'converted': False, 'warnings': [],
                    'error': f"the conversion ended with an error: {first_line(e)}"}

        warnings = outer_join_warnings(statement_settings.get('conversion_report') or {})

        ## convert_statement_code() writes the transformation it could not do into the text it
        ## returns. Such a result is not a conversion and is not offered as one.
        failed = [line for line in (converted or '').splitlines() if line.strip().startswith('-- ERROR')]
        if failed:
            return {'code': '', 'converted': False, 'warnings': warnings,
                    'error': f"the statement could not be transformed: {failed[0].strip()}"}
        if not (converted or '').strip():
            return {'code': '', 'converted': False, 'warnings': warnings,
                    'error': 'the conversion produced no statement at all'}

        ## an outer join whose condition could not be attributed keeps its marker; such a
        ## statement is reported, never offered as converted with a comment in the middle of it
        if '/* left_outer */' in converted or '/* right_outer */' in converted:
            return {'code': '', 'converted': False, 'warnings': warnings,
                    'error': "the outer join written '*=' or '=*' could not be rewritten as a "
                             "LEFT JOIN / RIGHT JOIN - the statement needs to be rewritten by hand"}

        self.config_parser.print_log_message('DEBUG', f"ms_sql_connector: convert_query_code: {statement_id}: {converted}")
        return {'code': converted, 'converted': True, 'warnings': warnings, 'error': None}

    def convert_view_code(self, settings: dict):
        """
        The complete `CREATE VIEW` statement of the target, built around the converted query.
        """
        try:
            final_select_sql = self.convert_statement_code(settings)
        except ValueError as e:
            return str(e)

        target_schema_name = settings['target_schema_name']
        target_view_name = settings['target_view_name']

        final_view_sql = f"CREATE OR REPLACE VIEW \"{target_schema_name}\".\"{target_view_name}\" AS\n{final_select_sql};"
        return final_view_sql

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
            target_schema_name = settings['target_schema_name'] ## target schema is used as it is defined in config, not converted to upper/lower case
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
            ## source_schema_name, source_table_name, source_table_id, source_table_rows_limited, worker_id, target_schema_name, target_table_name, target_table_rows
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
                self.config_parser.print_log_message('INFO', f"ms_sql_connector: migrate_table: Worker {worker_id}: Table {source_table_name} is empty - skipping data migration.")
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

                    self.config_parser.print_log_message('INFO', f"ms_sql_connector: migrate_table: Worker {worker_id}: Source table {source_table_name}: {source_table_rows_limited} rows / Target table {target_table_name}: {target_table_rows} rows - starting data migration.")

                    select_columns_list = []
                    orderby_columns_list = []
                    insert_columns_list = []
                    for order_num, col in source_columns.items():
                        self.config_parser.print_log_message('DEBUG2',
                                                            f"Worker {worker_id}: Table {source_schema_name}.{source_table_name}: Processing column {col['column_name']} ({order_num}) with data type {col['data_type']}")

                        target_type_check = migrator_tables.check_data_types_substitution({
                            'table_name': source_table_name,
                            'column_name': col['column_name'],
                            'check_type': col['data_type']
                        })

                        if target_type_check and target_type_check.lower() in ('bool', 'boolean'):
                            select_columns_list.append(f'''CASE WHEN [{col['column_name']}] = 1 THEN 'true' WHEN [{col['column_name']}] = 0 THEN 'false' ELSE NULL END AS [{col['column_name']}]''')
                        else:
                            select_columns_list.append(f'''[{col['column_name']}]''')

                        insert_columns_list.append(f'''"{self.config_parser.convert_names_case(col['column_name'])}"''')
                        orderby_columns_list.append(f'''[{col['column_name']}]''')

                    select_columns = ', '.join(select_columns_list)
                    insert_columns = ', '.join(insert_columns_list)
                    orderby_columns = ', '.join(orderby_columns_list)

                    if resume_after_crash and not drop_unfinished_tables:
                        chunk_number = self.config_parser.get_total_chunks(target_table_rows, chunk_size)
                        self.config_parser.print_log_message('DEBUG', f"ms_sql_connector: migrate_table: Worker {worker_id}: Resuming migration for table {source_schema_name}.{source_table_name} from chunk {chunk_number} with data chunk size {chunk_size}.")
                        chunk_offset = target_table_rows
                    else:
                        chunk_offset = (chunk_number - 1) * chunk_size

                    chunk_start_row_number = chunk_offset + 1
                    chunk_end_row_number = chunk_offset + chunk_size

                    self.config_parser.print_log_message('DEBUG', f"ms_sql_connector: migrate_table: Worker {worker_id}: Migrating table {source_schema_name}.{source_table_name}: chunk {chunk_number}, data chunk size {chunk_size}, batch size {batch_size}, chunk offset {chunk_offset}, chunk end row number {chunk_end_row_number}, source table rows {source_table_rows_limited}")
                    order_by_clause = ''

                    # if table is small, skipping ordering does not make sense because it will not speed up the migration
                    # if chunk_size > source_table_rows_limited:
                    #     query = f'''SELECT {select_columns} FROM "{source_schema_name}".{source_table_name}'''
                    #     if migration_limitation:
                    #         query += f" WHERE {migration_limitation}"
                    # else:

                    query = f"SELECT {select_columns} FROM [{source_schema_name}].[{source_table_name}]"
                    if migration_limitation:
                        query += f" WHERE {migration_limitation}"
                    primary_key_columns = migrator_tables.select_primary_key({'source_schema_name': source_schema_name, 'source_table_name': source_table_name})
                    self.config_parser.print_log_message('DEBUG2', f"ms_sql_connector: migrate_table: Worker {worker_id}: Primary key columns for {source_schema_name}.{source_table_name}: {primary_key_columns}")
                    if primary_key_columns:
                        orderby_columns = primary_key_columns
                    order_by_clause = f""" ORDER BY {orderby_columns}"""
                    query += order_by_clause + f" OFFSET {chunk_offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY;"

                    self.config_parser.print_log_message('DEBUG', f"ms_sql_connector: migrate_table: Worker {worker_id}: Fetching data with cursor using query: {query}")

                    part_name = 'execute query'
                    cursor = self.connection.cursor()
                    cursor.arraysize = batch_size

                    batch_start_time = time.time()
                    reading_start_time = batch_start_time
                    processing_start_time = batch_start_time
                    batch_end_time = None
                    batch_number = 0
                    batch_durations = []

                    cursor.execute(query)
                    total_inserted_rows = 0
                    ## The values are decoded while they are fetched - an undecodable one is
                    ## reported from here and not from the query which asked for them.
                    part_name = 'reading data'
                    while True:
                        records = cursor.fetchmany(batch_size)
                        if not records:
                            break
                        batch_number += 1
                        reading_end_time = time.time()
                        reading_duration = reading_end_time - reading_start_time
                        self.config_parser.print_log_message('DEBUG',f"ms_sql_connector: migrate_table: Worker {worker_id}: Fetched {len(records)} rows (batch {batch_number}) from source table {source_table_name}.")

                        transforming_start_time = time.time()
                        records = [
                            {column['column_name']: value for column, value in zip(source_columns.values(), record)}
                            for record in records
                        ]
                        for record in records:
                            for order_num, column in source_columns.items():
                                column_name = column['column_name']
                                column_type = column['data_type']
                                if column_type.lower() in ['binary', 'varbinary', 'image', 'hierarchyid', 'geometry', 'geography', 'udt', 'rowversion', 'timestamp']:
                                    record[column_name] = bytes(record[column_name]) if record[column_name] is not None else None
                                elif column_type.lower() in ['datetime', 'smalldatetime', 'date', 'time', 'datetime2', 'datetimeoffset']:
                                    record[column_name] = str(record[column_name]) if record[column_name] is not None else None

                        # Insert batch into target table
                        self.config_parser.print_log_message('DEBUG', f"ms_sql_connector: migrate_table: Worker {worker_id}: Starting insert of {len(records)} rows from source table {source_table_name}")
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
                    self.config_parser.print_log_message('INFO', f"ms_sql_connector: migrate_table: Worker {worker_id}: Target table {target_schema_name}.{target_table_name} has {target_table_rows} rows")

                    shortest_batch_seconds = min(batch_durations) if batch_durations else 0
                    longest_batch_seconds = max(batch_durations) if batch_durations else 0
                    average_batch_seconds = sum(batch_durations) / len(batch_durations) if batch_durations else 0
                    self.config_parser.print_log_message('INFO', f"ms_sql_connector: migrate_table: Worker {worker_id}: Migrated {total_inserted_rows} rows from {source_table_name} to {target_schema_name}.{target_table_name} in {batch_number} batches: "
                                                            f"Shortest batch: {shortest_batch_seconds:.2f} seconds, "
                                                            f"Longest batch: {longest_batch_seconds:.2f} seconds, "
                                                            f"Average batch: {average_batch_seconds:.2f} seconds")

                    cursor.close()

                else:
                    self.config_parser.print_log_message('INFO', f"ms_sql_connector: migrate_table: Worker {worker_id}: Target table {target_table_name} has {target_table_rows} rows and data_conflict_action is '{data_conflict_action}'. Skipping data migration.")

                migration_stats = {
                    'rows_migrated': total_inserted_rows,
                    'chunk_number': chunk_number,
                    'total_chunks': total_chunks,
                    'source_table_rows_all': source_table_rows_all,

                    'source_table_rows_limited': source_table_rows_limited,
                    'target_table_rows': target_table_rows,
                    'finished': False,
                }

                self.config_parser.print_log_message('DEBUG', f"ms_sql_connector: migrate_table: Worker {worker_id}: Migration stats: {migration_stats}")
                if source_table_rows_limited <= target_table_rows or chunk_number >= total_chunks:
                    self.config_parser.print_log_message('DEBUG3', f"ms_sql_connector: migrate_table: Worker {worker_id}: Setting migration status to finished for table {source_table_name} (chunk {chunk_number}/{total_chunks})")
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
            self.config_parser.print_log_message('ERROR', f"ms_sql_connector: migrate_table: Worker {worker_id}: Error during {part_name} -> {e}")
            self.config_parser.print_log_message('ERROR', f"ms_sql_connector: migrate_table: Worker {worker_id}: Full stack trace: {traceback.format_exc()}")
            raise e

    def fetch_triggers(self, table_id, schema_name, table_name):
        triggers = {}
        order_num = 1
        query = f"""
            SELECT
                t.name AS trigger_name,
                s.name AS schema_name,
                m.definition AS trigger_definition,
                te.type_desc AS event_type,
                t.is_disabled,
                t.object_id
            FROM sys.triggers t
            JOIN sys.tables tb ON t.parent_id = tb.object_id
            JOIN sys.schemas s ON tb.schema_id = s.schema_id
            JOIN sys.sql_modules m ON t.object_id = m.object_id
            JOIN sys.trigger_events te ON t.object_id = te.object_id
            WHERE s.name = '{schema_name}' AND tb.name = '{table_name}'
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

            # Group events by trigger
            trigger_data = {}
            for row in rows:
                trigger_name = row[0]
                schema = row[1]
                definition = row[2]
                event = row[3] # e.g. INSERT, UPDATE, DELETE
                is_disabled = row[4]
                object_id = row[5]

                if trigger_name not in trigger_data:
                    trigger_data[trigger_name] = {
                        'schema_name': schema,
                        'trigger_name': trigger_name,
                        'trigger_code': definition,
                        'events': {event},
                        'is_disabled': is_disabled,
                        'object_id': object_id
                    }
                else:
                    trigger_data[trigger_name]['events'].add(event)

            for name, data in trigger_data.items():
                events_list = list(data['events'])
                triggers[order_num] = {
                    'name': data['trigger_name'],
                    'trigger_owner': data['schema_name'],
                    'sql': data['trigger_code'],
                    'event': ' OR '.join(events_list), # Planner expects 'event' string
                    'new': 'inserted' if 'INSERT' in events_list or 'UPDATE' in events_list else None,
                    'old': 'deleted' if 'DELETE' in events_list or 'UPDATE' in events_list else None,
                    'status': 'DISABLED' if data['is_disabled'] else 'ENABLED',
                    'comment': '',
                    'id': data['object_id']
                }
                order_num += 1

            cursor.close()
            self.disconnect()
            return triggers
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ms_sql_connector: fetch_triggers: Error fetching triggers: {e}")
            return {}

    def convert_trigger(self, settings):
        trigger_name = settings['trigger_name']
        trigger_code = settings['trigger_sql']
        source_schema_name = settings['source_schema_name']
        target_schema_name = settings['target_schema_name']
        table_list = settings['table_list']

        trigger_code = trigger_code.strip()

        table_match = re.search(r'ON\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?', trigger_code, re.IGNORECASE)
        ## the table the trigger is on, named out of the text of the source - so the case
        ## handling of the migration has to be applied to it, the same way the planner applies
        ## it to the names it hands over
        table_name = self.config_parser.convert_names_case(
            table_match.group(2) if table_match else "UNKNOWN_TABLE")

        events = []
        if re.search(r'\bINSERT\b', trigger_code, re.IGNORECASE): events.append('INSERT')
        if re.search(r'\bUPDATE\b', trigger_code, re.IGNORECASE): events.append('UPDATE')
        if re.search(r'\bDELETE\b', trigger_code, re.IGNORECASE): events.append('DELETE')

        timing = 'AFTER'
        if re.search(r'\bINSTEAD\s+OF\b', trigger_code, re.IGNORECASE):
            timing = 'INSTEAD OF'

        body_match = re.search(r'CREATE\s+TRIGGER\s+.*?\s+AS\s+(.*)', trigger_code, re.IGNORECASE | re.DOTALL)
        body_content = body_match.group(1) if body_match else ""

        if not body_content:
             return f"/* COULD NOT ISOLATE BODY FOR {trigger_name} */ {trigger_code}"

        fake_code = f"CREATE PROCEDURE dummy AS\n{body_content}"
        parser = TsqlParser(fake_code, self.config_parser,
                            view_converter=self.convert_statement_code, settings=settings,
                            functions_mapping_converter=self.apply_sql_functions_mapping)
        final_output = parser.run(pg_header_str=" ")

        final_stmts_clean = []
        in_body = False
        first_begin_found = False
        indent_level = 0

        def get_indent(level):
            return "    " * max(0, level)

        has_rowcount = '@@rowcount' in body_content.lower()
        declarations = []

        for line_obj in final_output:
            stripped = line_obj.content.strip()
            if not stripped: continue
            if stripped in ('$$', '$$ LANGUAGE PLPGSQL;', '$$ LANGUAGE plpgsql;'): continue
            if line_obj.source_array == "header": continue

            if stripped.upper() == "DECLARE":
                in_body = True
                continue

            if stripped.upper().startswith("DECLARE "):
                declarations.append(stripped)
                continue

            if re.match(r'^BEGIN\b', stripped, re.IGNORECASE):
                if not first_begin_found:
                    first_begin_found = True
                    indent_level = 1
                else:
                    indent_level += 1
            elif re.match(r'^END;', stripped, re.IGNORECASE):
                indent_level -= 1
                if indent_level < 0: indent_level = 0

            final_stmts_clean.append(get_indent(indent_level) + line_obj.content)

        if has_rowcount:
             declarations.insert(0, "locvar_rowcount INTEGER;")

        pg_body = "\n".join(final_stmts_clean)

        ## the whole generated name follows the case handling, not only the part which came
        ## from the trigger - "tf_TR_AUDITSALES" is consistent but reads like a defect
        func_name = self.config_parser.convert_names_case(f"tf_{trigger_name}")
        func_schema = target_schema_name
        decl_section = "DECLARE\n" + "\n".join(declarations) if declarations else ""

        func_ddl = f"""
CREATE OR REPLACE FUNCTION "{func_schema}"."{func_name}"()
RETURNS TRIGGER AS $$
{decl_section}
BEGIN
{pg_body}
RETURN NULL;
END;
$$ LANGUAGE plpgsql;
"""

        events_str = " OR ".join(events)

        trigger_ddl = f"""
CREATE TRIGGER "{trigger_name}"
{timing} {events_str} ON "{target_schema_name}"."{table_name}"
FOR EACH ROW
EXECUTE FUNCTION "{func_schema}"."{func_name}"();
"""

        return f"{func_ddl}\n{trigger_ddl}"

    def execute_query(self, query: str, params=None):
        pass # Placeholder

    def _transform_trigger_tables(self, expression):

        # Transform MSSQL 'inserted'/'deleted' table usage to PG 'NEW'/'OLD' record usage
        # This requires AST modification:
        # 1. Remove 'inserted'/'deleted' from FROM/JOINs.
        # 2. Rename columns referencing them to use NEW/OLD as table alias.

        table_map = {'inserted': 'NEW', 'deleted': 'OLD'}
        aliases = {}

        # 1. Identify aliases first (Scan)
        from_clause = expression.args.get('from')
        joins = expression.args.get('joins') or []



        if from_clause:
            for item in from_clause.expressions:
                if isinstance(item, exp.Table) and item.name.lower() in table_map:
                    aliases[item.alias_or_name] = table_map[item.name.lower()]

        for j in joins:
            if isinstance(j.this, exp.Table) and j.this.name.lower() in table_map:
                aliases[j.this.alias_or_name] = table_map[j.this.name.lower()]

        # 2. Transform Logic
        def transformer(node):
            if isinstance(node, exp.Column):
                tbl = node.table
                if tbl and tbl in aliases:
                    # Replace with Aliased Column (NEW/OLD)
                    return exp.Column(
                        this=node.this,
                        table=exp.Identifier(this=aliases[tbl], quoted=False)
                    )
            return node

        expression = expression.transform(transformer)

        # 3. Handle Table Removal and Join Promotion
        # Re-access FROM after transform
        from_clause = expression.args.get('from')
        joins = expression.args.get('joins') or []

        if from_clause:
            new_froms = []
            for item in from_clause.expressions:
                if isinstance(item, exp.Table) and item.name.lower() in table_map:
                    continue
                new_froms.append(item)

            from_clause.set('expressions', new_froms)

            # If new_froms is empty, we MUST promote a JOIN if available, or empty FROM
            if not new_froms:
                if joins:
                    first_join = joins.pop(0)
                    new_table = first_join.this
                    condition = first_join.args.get('on')

                    # Set FROM to new_table
                    from_clause.set('expressions', [new_table])

                    # Move conditions to WHERE
                    if condition:
                        expression.where(condition, copy=False)

                    expression.set('joins', joins)
                else:
                    # No JOINs, so just empty FROM (SELECT NEW.col)
                    expression.set('from', None)

        # 4. Filter JOINs that are strictly transition tables (if not promoted)
        joins = expression.args.get('joins') or []
        new_joins = []
        for j in joins:
            if isinstance(j.this, exp.Table) and j.this.name.lower() in table_map:
                # Merge logic: if explicit JOIN condition exists, move it to WHERE
                # e.g. JOIN inserted ON i.id = t.id -> WHERE NEW.id = t.id
                condition = j.args.get('on')
                if condition:
                     expression.where(condition, copy=False)
                continue
            new_joins.append(j)
        expression.set('joins', new_joins)

        return expression
        # ...existing code from SybaseASEConnector.execute_query...
        pass

    def execute_sql_script(self, script_path: str):
        # ...existing code from SybaseASEConnector.execute_sql_script...
        pass

    def begin_transaction(self):
        # ...existing code from SybaseASEConnector.begin_transaction...
        pass

    def commit_transaction(self):
        # ...existing code from SybaseASEConnector.commit_transaction...
        pass

    def rollback_transaction(self):
        # ...existing code from SybaseASEConnector.rollback_transaction...
        pass

    def handle_error(self, e, description=None):
        self.config_parser.print_log_message('ERROR', f"ms_sql_connector: handle_error: An error in {self.__class__.__name__} ({description}): {e}")
        self.config_parser.print_log_message('ERROR', traceback.format_exc())
        if self.on_error_action == 'stop':
            self.config_parser.print_log_message('ERROR', "ms_sql_connector: handle_error: Stopping due to error.")
            exit(1)
        else:
            self.config_parser.print_log_message('WARNING', f"ms_sql_connector: handle_error: Error caught, but continuing as requested by configuration (on_error_action='{self.on_error_action}').")

    def get_rows_count(self, table_schema: str, table_name: str, migration_limitation: str = None):
        query = f"""SELECT COUNT(*) FROM [{table_schema}].[{table_name}]"""
        if migration_limitation:
            query += f" WHERE {migration_limitation}"
        self.config_parser.print_log_message('DEBUG', f"ms_sql_connector: get_rows_count: query: {query}")
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
            query = f"""
                SELECT ISNULL(CAST(last_value AS BIGINT) + CAST(increment_value AS BIGINT), CAST(seed_value AS BIGINT))
                FROM sys.identity_columns
                WHERE object_id = OBJECT_ID('[{table_schema}].[{table_name}]')
            """
            cursor = self.connection.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            if row and row[0] is not None:
                return int(row[0])
            return None
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"ms_sql_connector: get_table_next_identity: Error fetching next identity for {table_schema}.{table_name}: {e}")
            return None

    def fetch_sequences(self, schema_name: str):
        sequences = {}
        order_num = 1
        query = f"""
            SELECT
                s.name AS sequence_name,
                s.object_id AS sequence_id,
                CAST(s.start_value AS VARCHAR(50)) AS start_value,
                CAST(s.increment AS VARCHAR(50)) AS increment_value,
                CAST(s.minimum_value AS VARCHAR(50)) AS min_value,
                CAST(s.maximum_value AS VARCHAR(50)) AS max_value,
                s.is_cycling AS cycle_option,
                sch.name AS schema_name
            FROM sys.sequences s
            JOIN sys.schemas sch ON s.schema_id = sch.schema_id
            WHERE sch.name = '{schema_name}'
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                sequence_name = row[0]
                sequence_id = row[1]
                start_value = row[2]
                increment_value = row[3]
                min_value = row[4]
                max_value = row[5]
                cycle_option = "CYCLE" if row[6] else "NO CYCLE"
                sch_name = row[7]

                source_sequence_sql = f"CREATE SEQUENCE [{sch_name}].[{sequence_name}] START WITH {start_value} INCREMENT BY {increment_value} MINVALUE {min_value} MAXVALUE {max_value} {cycle_option};"

                sequences[order_num] = {
                    'sequence_name': sequence_name,
                    'id': sequence_id,
                    'source_sequence_sql': source_sequence_sql
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return sequences
        except Exception as e:
            self.handle_error(e, f"fetching sequences for schema {schema_name}")

    def get_sequence_details(self, sequence_owner, sequence_name):
        # Placeholder for fetching sequence details
        return {}

    def fetch_user_defined_types(self, schema: str):
        query = """
            SELECT
                t.name AS type_name,
                st.name AS system_type_name,
                t.max_length,
                t.precision,
                t.scale,
                t.is_nullable
            FROM sys.types t
            JOIN sys.types st ON t.system_type_id = st.user_type_id
            WHERE t.is_user_defined = 1 AND st.is_user_defined = 0
        """
        try:
            udts = {}
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

            order_num = 1
            for row in rows:
                type_name = row[0]
                system_type = row[1].upper()
                max_length = row[2]
                precision = row[3]
                scale = row[4]

                # Construct definition
                definition = system_type
                if self.is_string_type(system_type) and max_length != -1:
                    length = max_length // 2 if system_type in ('NCHAR', 'NVARCHAR') else max_length
                    definition = f"{system_type}({length})"
                elif self.is_numeric_type(system_type):
                    if system_type in ('DECIMAL', 'NUMERIC'):
                        definition = f"{system_type}({precision}, {scale})"

                # Structure expected by Planner: keyed by integer order_num
                udts[order_num] = {
                    'type_name': type_name,
                    'base_type': system_type,
                    'length': max_length,
                    'prec': precision,
                    'scale': scale,
                    'sql': definition,
                    'schema_name': schema,
                    'comment': ''
                }
                order_num += 1

            cursor.close()
            self.disconnect()
            return udts
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ms_sql_connector: fetch_user_defined_types: Error fetching UDTs: {e}")
            return {}

    def _get_udt_map(self):
        """
        A map of UDT name -> definition for the conversion, read from the source once.

        It used to be read on every call, and convert_statement_code() calls it for every
        statement it converts. The query conversion converts a whole file of them, with a
        pool of workers over one connector - so this was a round trip to the source database
        per statement, and fetch_user_defined_types() connects and disconnects around its
        query, which one worker did while another was using the same connection. The answer
        does not change during a run: it is read once, under a lock, and kept.
        """
        cached = getattr(self, '_udt_map_cache', None)
        if cached is not None:
            return cached
        with UDT_MAP_LOCK:
            cached = getattr(self, '_udt_map_cache', None)
            if cached is not None:
                return cached
            udts_full = self.fetch_user_defined_types('dbo')
            udt_map = {}
            for k, v in udts_full.items():
                udt_map[v['type_name']] = v
            self._udt_map_cache = udt_map
            return udt_map

    def get_sequence_current_value(self, sequence_name: str):
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
        self.config_parser.print_log_message('DEBUG3', f"ms_sql_connector: get_table_description: MS SQL connector: Getting table description for {settings['table_schema']}.{settings['table_name']}")
        return { 'table_description': '' }

    def testing_select(self):
        return "SELECT 1"

    def get_database_version(self):
        query = "SELECT @@VERSION"
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        version = cursor.fetchone()[0]
        cursor.close()
        self.disconnect()
        return version

    def get_database_size(self):
        query = "SELECT SUM(size * 8 * 1024) FROM sys.master_files WHERE database_id = DB_ID()"
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        size = cursor.fetchone()[0]
        cursor.close()
        self.disconnect()
        return size

    def get_top_n_tables(self, settings):
        top_tables = {}
        top_tables['by_rows'] = {}
        top_tables['by_size'] = {}
        top_tables['by_columns'] = {}
        top_tables['by_indexes'] = {}
        top_tables['by_constraints'] = {}

        source_schema_name = settings['source_schema_name']
        try:
            order_num = 1
            top_n = self.config_parser.get_top_n_tables_by_rows()
            if top_n > 0:
                query = f"""
                    SELECT TOP {top_n}
                    s.name AS schema_name,
                    t.name AS table_name,
                    SUM(p.rows) AS row_count,
                    SUM(a.total_pages) * 8 * 1024 AS total_size
                    FROM sys.tables t
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
                    JOIN sys.allocation_units a ON p.partition_id = a.container_id
                    WHERE s.name = '{source_schema_name}'
                    GROUP BY s.name, t.name
                    ORDER BY total_size DESC
                """
                self.connect()
                cursor = self.connection.cursor()
                cursor.execute(query.format(source_schema_name=source_schema_name))
                rows = cursor.fetchall()
                cursor.close()
                self.disconnect()
                order_num = 1
                for row in rows:
                    top_tables['by_rows'][order_num] = {
                        'owner': row[0].strip(),
                        'table_name': row[1].strip(),
                        'row_count': row[2],
                        'table_size': row[3],
                    }
                    order_num += 1
                self.config_parser.print_log_message('DEBUG', f"ms_sql_connector: get_top_n_tables: Top {top_n} tables by rows: {top_tables['by_rows']}")
            else:
                self.config_parser.print_log_message('DEBUG', "ms_sql_connector: get_top_n_tables: Top N tables by rows is not configured or set to 0, skipping this part.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"ms_sql_connector: get_top_n_tables: Error fetching top tables by rows: {e}")

        return top_tables

    def get_top_fk_dependencies(self, settings):
        top_fk_dependencies = {}
        return top_fk_dependencies

    def target_table_exists(self, target_schema_name, target_table_name):
        query = f"""
            SELECT COUNT(*)
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = '{target_schema_name}' AND t.name = '{target_table_name}'
        """
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        exists = cursor.fetchone()[0] > 0
        cursor.close()
        self.disconnect()
        return exists

    def fetch_all_rows(self, query):
        """
        Fetch all rows from the database using the provided query.
        This method is used to fetch data in a way that is compatible with the MS SQL Server connector.
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows



    def _convert_stmts(self, body_content, settings, is_nested=False, has_rowcount=False, is_trigger=False, implicit_return=False):
        import base64
        processed_body = body_content

         # --- Handle Sybase/MSSQL Outer Joins (*= and =*) before parsing ---
        processed_body = re.sub(
            r'((?:\[[^\]]+\]|"[^"]+"|[\w]+)(?:\.(?:\[[^\]]+\]|"[^"]+"|[\w]+))*)\s*\*\=\s*((?:\[[^\]]+\]|"[^"]+"|[\w]+)(?:\.(?:\[[^\]]+\]|"[^"]+"|[\w]+))*)',
            r"locvar_sybase_outer_join(\1, \2)",
            processed_body,
        )
        processed_body = re.sub(
            r'((?:\[[^\]]+\]|"[^"]+"|[\w]+)(?:\.(?:\[[^\]]+\]|"[^"]+"|[\w]+))*)\s*\=\*\s*((?:\[[^\]]+\]|"[^"]+"|[\w]+)(?:\.(?:\[[^\]]+\]|"[^"]+"|[\w]+))*)',
            r"locvar_sybase_right_join(\1, \2)",
            processed_body,
        )

        CustomTSQL.Parser.config_parser = self.config_parser

        try:
             parsed = sqlglot.parse(processed_body.strip(), read=CustomTSQL)
        except Exception as e:
             self.config_parser.print_log_message('ERROR', f"ms_sql_connector: _convert_stmts: Global parsing failed: {e}")
             return [f"/* PARSING FAILED: {e} */\n" + body_content]

        converted_statements = []
        active_rowcount_limit = 0
        def clean_pg_sql(pg_sql):
             # @@FETCH_STATUS
             pg_sql = re.sub(r'@@FETCH_STATUS\s*=\s*0', 'FOUND', pg_sql, flags=re.IGNORECASE)
             pg_sql = re.sub(r'@@FETCH_STATUS\s*(?:!=|<>)\s*0', 'NOT FOUND', pg_sql, flags=re.IGNORECASE)

             # @@ROWCOUNT (Specific handling before generic global replace)
             pg_sql = re.sub(r'@@rowcount', '_rowcount', pg_sql, flags=re.IGNORECASE)

             # Generic Replacement Rules (Fallback for non-AST reachable code)
             pg_sql = re.sub(r'(?<!\w)@@([a-zA-Z0-9_]+)', r'global_\1', pg_sql)
             pg_sql = re.sub(r'(?<!\w)@([a-zA-Z0-9_]+)', r'locvar_\1', pg_sql)
             # Also handle $var for parameters that skipped generic AST transform or string literals
             pg_sql = re.sub(r'(?<!\w)\$([a-zA-Z][a-zA-Z0-9_]*)', r'locvar_\1', pg_sql)
             return pg_sql

        # AST Transformer for variables
        def transform_variables_ast(node):
            if isinstance(node, (exp.Parameter, exp.SessionParameter)):
                 # Convert Parameter(@var) -> Identifier(locvar_var)
                 # Check if 'this' is Identifier or string
                 val = node.this.this if isinstance(node.this, exp.Identifier) else str(node.this)

                 # Session Params usually @@
                 if '@@' in val:
                      return exp.Identifier(this=val.replace('@@', 'global_'), quoted=False)

                 # Normal Params @ or just name
                 if val.startswith('@'):
                      new_name = val.replace('@', 'locvar_')
                 else:
                      new_name = f"locvar_{val}"

                 return exp.Identifier(this=new_name, quoted=False)

            if isinstance(node, exp.Identifier):
                 val = node.this
                 if val.startswith('@@'):
                      return exp.Identifier(this=val.replace('@@', 'global_'), quoted=False)
                 elif val.startswith('@'):
                      return exp.Identifier(this=val.replace('@', 'locvar_'), quoted=False)
            return node

        def process_node(expression):
             nonlocal active_rowcount_limit
             if not expression: return None

             # Check for SET ROWCOUNT
             if isinstance(expression, exp.Command) and expression.this.upper() == 'SET':
                  m = re.match(r'ROWCOUNT\s+(\d+)', expression.expression or '', re.IGNORECASE)
                  if m:
                       active_rowcount_limit = int(m.group(1))
                       return f"/* SET ROWCOUNT {active_rowcount_limit} converted to LIMIT */"

             is_block = isinstance(expression, Block) or type(expression).__name__ == 'Block'
             if is_block:
                  stmts = []
                  if hasattr(expression, 'expressions'):
                       for e in expression.expressions:
                            s = process_node(e)
                            if s: stmts.append(s)
                  return "\n".join(stmts)

             is_if = isinstance(expression, exp.If) or expression.key == 'if' or type(expression).__name__ == 'If'
             if is_if:
                  cond_sql = expression.this.sql(dialect='postgres')
                  cond_sql = clean_pg_sql(cond_sql)
                  true_node = expression.args.get('true')
                  false_node = expression.args.get('false')
                  true_sql = process_node(true_node) if true_node else ""
                  pg_sql = f"IF {cond_sql} THEN\n{true_sql}"
                  if false_node:
                       false_sql = process_node(false_node)
                       pg_sql += f"\nELSE\n{false_sql}"
                  pg_sql += "\nEND IF;"
                  return pg_sql

             # ... Outer Join AST Transformation (Same as Sybase) ...
             try:
                 where = expression.find(exp.Where)
                 joins_to_add = []
                 if where:
                      for func in where.find_all(exp.Anonymous):
                           fname = func.this
                           if fname.upper() in ('LOCVAR_SYBASE_OUTER_JOIN', 'LOCVAR_SYBASE_RIGHT_JOIN'):
                                kind = 'LEFT' if fname.upper() == 'LOCVAR_SYBASE_OUTER_JOIN' else 'RIGHT'
                                left = func.expressions[0]
                                right = func.expressions[1]
                                table_name = right.table if isinstance(right, exp.Column) else None
                                if table_name:
                                    joins_to_add.append({
                                        'table': table_name,
                                        'condition': exp.EQ(this=left, expression=right),
                                        'node': func,
                                        'kind': kind
                                    })
                      for j in joins_to_add:
                           j['node'].replace(exp.TRUE)

                 from_clause = expression.args.get('from')
                 if from_clause and joins_to_add:
                      new_froms = []
                      tables_to_remove = [j['table'] for j in joins_to_add]
                      for f in from_clause.expressions:
                           if isinstance(f, exp.Table) and f.alias_or_name in tables_to_remove:
                               continue
                           new_froms.append(f)
                      for j in joins_to_add:
                           join_expr = exp.Join(
                               this=exp.Table(this=exp.Identifier(this=j['table'], quoted=False)),
                               kind=j['kind'],
                               on=j['condition']
                           )
                           new_froms.append(join_expr)
                      from_clause.set('expressions', new_froms)
             except:
                  pass

             pg_sql = ""
             skip_semicolon = False

             if is_trigger:
                 if isinstance(expression, exp.Return):
                     return "RETURN NULL;"

             # Check for RAISERROR (Anonymous function call in AST)
             # RAISERROR ('msg', 16, 1, arg1, arg2...)
             is_raiserror = isinstance(expression, exp.Anonymous) and expression.this.upper() == 'RAISERROR'
             if is_raiserror:
                  # expression.expressions contains args
                  args = expression.expressions
                  if args:
                       # First arg is message
                       msg_node = args[0]
                       # Next 2 are severity, state (skipped in PG RAISE usually, or mapped)
                       # Rest are arguments

                       # PG Syntax: RAISE [LEVEL] 'format', arg1, arg2
                       # We need to construct this string

                       msg_sql = msg_node.sql(dialect='postgres')

                       other_args = []
                       if len(args) > 3:
                            for a in args[3:]:
                                 other_args.append(a.sql(dialect='postgres'))

                       # Handling params replacement in message?
                       # PG uses % to replace arguments positional? Yes.
                       # MSSQL uses %d, %s etc. PG raise format uses %

                       arg_str = ", ".join(other_args)
                       if arg_str:
                            return f"RAISE EXCEPTION {msg_sql}, {arg_str};"
                       else:
                            return f"RAISE EXCEPTION {msg_sql};"

             # Handle SELECT @var = value (Assignment without FROM)
             if isinstance(expression, exp.Select) and not expression.args.get('from'):
                 is_assignment = True
                 assignments = []
                 for e in expression.expressions:
                     # Unwrap Alias (e.g. SELECT @v=1 END -> parsed as Alias)
                     if isinstance(e, exp.Alias):
                          e = e.this

                     # Check for EQ node (v = x)
                     if isinstance(e, exp.EQ):
                         left = e.this
                         right = e.expression
                         # Check if left is variable
                         if isinstance(left, exp.Identifier) and (left.this.startswith('locvar_') or left.this.startswith('global_')):
                              assignments.append(f"{left.this} := {right.sql(dialect='postgres')};")
                         else:
                              is_assignment = False
                              break
                     else:
                         is_assignment = False
                         break

                 if is_assignment and assignments:
                      return "\n".join(assignments)

             pg_sql = expression.sql(dialect='postgres')

             # Apply ROWCOUNT limit if detected (and no explicit LIMIT exists)
             if active_rowcount_limit > 0 and isinstance(expression, exp.Select) and not expression.args.get('limit'):
                  pg_sql += f" LIMIT {active_rowcount_limit}"

             # Handle Implicit Return (SELECT -> RETURN QUERY SELECT)
             # Must not be an assignment (handled above) or INTO (handled by SQLGlot usually, or check args)
             if implicit_return and isinstance(expression, exp.Select) and not expression.args.get('into'):
                  pg_sql = f"RETURN QUERY {pg_sql}"

             if pg_sql.strip().upper() == 'BEGIN': return None

             pg_sql = pg_sql.replace('locvar_error_placeholder', 'SQLSTATE')

             if has_rowcount and isinstance(expression, (exp.Insert, exp.Update, exp.Delete, exp.Select)):
                   pg_sql += ";\nGET DIAGNOSTICS _rowcount = ROW_COUNT"

             # PRINT (Command or Anonymous)
             # If parsed as usage of PRINT command
             if isinstance(expression, exp.Command) and expression.this.upper() == 'PRINT':
                  # expression.expression is the text
                   p_arg = expression.expression
                   # Clean it
                   return f"RAISE NOTICE {p_arg};"

             # Fallback regex for PRINT if not caught above (e.g. if parsed as func)
             if "PRINT" in pg_sql.upper():
                  match_p = re.search(r"PRINT\s+(.*)", pg_sql, re.IGNORECASE)
                  if match_p:
                       pg_sql = f"RAISE NOTICE {match_p.group(1)}"

             # Assignments (MSSQL variable assignment usually via SET or SELECT)
             # sqlglot might output: _rowcount = ROW_COUNT (valid PG)
             # But T-SQL SET @v = 1 -> PG v := 1
             # sqlglot sometimes outputs SET v = 1, which works in PG for session vars, but plpgsql needs :=
             if pg_sql.strip().upper().startswith("SET "):
                 # Try to convert to assignment
                 # Update regex to support @ and @@ in variable names
                 match_set = re.match(r"SET\s+([@a-zA-Z0-9_]+)\s*=\s*(.*)", pg_sql, re.IGNORECASE)
                 if match_set:
                     var_raw = match_set.group(1)
                     val = match_set.group(2)

                     if '@@' in var_raw:
                          var = var_raw.replace('@@', 'global_')
                     elif '@' in var_raw:
                          var = var_raw.replace('@', 'locvar_')
                     else:
                          var = var_raw

                     pg_sql = f"{var} := {val}"

             # Clean result
             pg_sql = clean_pg_sql(pg_sql)

             if not skip_semicolon and not pg_sql.strip().endswith(';'):
                 pg_sql += ';'
             return pg_sql

        for expression in parsed:
             if is_trigger:
                 expression = self._transform_trigger_tables(expression)

             # Apply Variable Rename Transform
             expression = expression.transform(transform_variables_ast)

             res = process_node(expression)
             if res:
                  converted_statements.append(res)

        return converted_statements

    def _split_respecting_parens(self, text):
        parts = []
        current = ""
        depth = 0
        in_quote = False
        quote_char = ''

        for char in text:
            if in_quote:
                current += char
                if char == quote_char:
                    in_quote = False
            else:
                if char == "'" or char == '"':
                    in_quote = True
                    quote_char = char
                    current += char
                elif char == '(':
                    depth += 1
                    current += char
                elif char == ')':
                    depth -= 1
                    current += char
                elif char == ',' and depth == 0:
                    parts.append(current.strip())
                    current = ""
                else:
                    current += char
        if current:
            parts.append(current.strip())
        return parts

    def _get_variable_types_mapping(self, settings):
        # Basic mapping for variable declarations
        return {
            'nvarchar': 'varchar',
            'nchar': 'char',
            'datetime': 'timestamp',
            'datetime2': 'timestamp',
            'money': 'numeric(19,4)',
            'smallmoney': 'numeric(10,4)',
            'tinyint': 'smallint',
            'bit': 'boolean',
            'image': 'bytea',
            'uniqueidentifier': 'uuid',
            'varbinary': 'bytea',
            'binary': 'bytea',
            'rowversion': 'bytea',
            'timestamp': 'bytea',
            'xml': 'xml',
            'sql_variant': 'text'
        }

    def _declaration_replacer(self, match, settings, types_mapping, declarations):
        content = match.group(0).strip()
        # Remove DECLARE
        content = re.sub(r'^DECLARE\s+', '', content, flags=re.IGNORECASE).strip()

        defs = self._split_respecting_parens(content)

        for d in defs:
            d = d.strip()
            # Replace type
            for sybase_type, pg_type in types_mapping.items():
                d = re.sub(rf'\b{re.escape(sybase_type)}\b', pg_type, d, flags=re.IGNORECASE)

            # Variable Rename Rules
            if '@@' in d:
                 d = d.replace('@@', 'global_')
            elif '@' in d:
                 d = d.replace('@', 'locvar_')

            # Initialization
            d = d.replace('=', ':=')

            declarations.append(d + ';')

        return ""

    def _apply_data_type_substitutions(self, text):
        """
        Apply data type substitutions defined in the configuration.
        """
        substitutions = self.config_parser.get_data_types_substitution()
        if not substitutions:
            return text

        for entry in substitutions:
            if len(entry) != 5:
                continue

            source_type = entry[2]
            target_type = entry[3]

            if source_type:
                try:
                    pattern = re.compile(rf'\b{source_type}\b', flags=re.IGNORECASE)
                    text = pattern.sub(target_type, text)
                except re.error:
                    self.config_parser.print_log_message('WARNING', f"ms_sql_connector: _apply_data_type_substitutions: Invalid regex in data_types_substitution: {source_type}")

        return text

    def _apply_udt_to_base_type_substitutions(self, text, settings):
        udt_map = self._get_udt_map()
        for udt, info in udt_map.items():
             text = re.sub(rf'\b{re.escape(udt)}\b', info['base_type'], text, flags=re.IGNORECASE)
        return text

    # T-SQL niladic functions usable in DEFAULT constraints and their PostgreSQL counterparts.
    # Keys are lower case function names without parentheses.
    MSSQL_DEFAULT_FUNCTIONS_TO_POSTGRESQL = {
        'getdate': 'current_timestamp',
        'sysdatetime': 'current_timestamp',
        'sysdatetimeoffset': 'current_timestamp',
        'getutcdate': "timezone('UTC', now())",
        'sysutcdatetime': "timezone('UTC', now())",
        'current_timestamp': 'current_timestamp',
        'suser_name': 'current_user',
        'suser_sname': 'current_user',
        'user_name': 'current_user',
        'current_user': 'current_user',
        'system_user': 'current_user',
        'session_user': 'session_user',
        'db_name': 'current_database()',
        'original_db_name': 'current_database()',
        'app_name': "current_setting('application_name')",
    }

    def _split_outside_string_literals(self, text: str) -> list:
        """
        Splits text so that even indexes are code fragments and odd indexes are
        single quoted string literals (with doubled quotes handled).
        Allows rewriting of SQL code without touching the content of literals.
        """
        return re.split(r"('(?:[^']|'')*')", text)

    def _rewrite_outside_string_literals(self, text: str, rewriter) -> str:
        parts = self._split_outside_string_literals(text)
        for index in range(0, len(parts), 2):
            parts[index] = rewriter(parts[index])
        return ''.join(parts)

    def _convert_money_literals(self, text: str) -> str:
        """
        MONEY / SMALLMONEY defaults are stored as currency literals - $1000.0000,
        -$1,000.00, $-1000. PostgreSQL has no such literal and $1000 would even be
        parsed as a positional parameter, so the value is reduced to a plain number.
        """
        def convert_fragment(fragment: str) -> str:
            def replace(match):
                negative = (match.group(1) == '-') != (match.group(2) == '-')
                number = match.group(3).replace(',', '')
                return f"{'-' if negative else ''}{number}"
            return re.sub(r"(-?)\s*\$\s*(-?)\s*(\d[\d,]*(?:\.\d+)?)", replace, fragment)

        return self._rewrite_outside_string_literals(text, convert_fragment)

    def _split_top_level_arguments(self, text: str) -> list:
        """ Splits a function argument list on commas which are not nested in parentheses or literals. """
        arguments = []
        current = ''
        depth = 0
        in_literal = False
        for character in text:
            if in_literal:
                current += character
                if character == "'":
                    in_literal = False
                continue
            if character == "'":
                in_literal = True
                current += character
            elif character == '(':
                depth += 1
                current += character
            elif character == ')':
                depth -= 1
                current += character
            elif character == ',' and depth == 0:
                arguments.append(current.strip())
                current = ''
            else:
                current += character
        if current.strip() != '':
            arguments.append(current.strip())
        return arguments

    ## The date and time styles of CONVERT(), as the format PostgreSQL writes with to_char()
    ## and reads with to_date() / to_timestamp(). The style is what the value LOOKS like, so
    ## dropping it does not drop a decoration: CONVERT(varchar(10), getdate(), 103) is
    ## 24/08/2026 and the CAST which used to stand in its place is 2026-08-24. Every new row
    ## got the other one.
    CONVERT_STYLE_FORMATS = {
        1: 'MM/DD/YY',                  101: 'MM/DD/YYYY',
        2: 'YY.MM.DD',                  102: 'YYYY.MM.DD',
        3: 'DD/MM/YY',                  103: 'DD/MM/YYYY',
        4: 'DD.MM.YY',                  104: 'DD.MM.YYYY',
        5: 'DD-MM-YY',                  105: 'DD-MM-YYYY',
        6: 'DD Mon YY',                 106: 'DD Mon YYYY',
        7: 'Mon DD, YY',                107: 'Mon DD, YYYY',
        8: 'HH24:MI:SS',                108: 'HH24:MI:SS',
        10: 'MM-DD-YY',                 110: 'MM-DD-YYYY',
        11: 'YY/MM/DD',                 111: 'YYYY/MM/DD',
        12: 'YYMMDD',                   112: 'YYYYMMDD',
        13: 'DD Mon YYYY HH24:MI:SS:MS', 113: 'DD Mon YYYY HH24:MI:SS:MS',
        14: 'HH24:MI:SS:MS',            114: 'HH24:MI:SS:MS',
        20: 'YYYY-MM-DD HH24:MI:SS',    120: 'YYYY-MM-DD HH24:MI:SS',
        21: 'YYYY-MM-DD HH24:MI:SS.MS', 121: 'YYYY-MM-DD HH24:MI:SS.MS',
        23: 'YYYY-MM-DD',
        24: 'HH24:MI:SS',
        25: 'YYYY-MM-DD HH24:MI:SS.MS',
        126: 'YYYY-MM-DD"T"HH24:MI:SS.MS',
        127: 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"',
    }

    ## The styles which are NOT in the table above, and why. Transact-SQL pads the hour of
    ## these with a space - `Aug 24 2026  9:30AM` - and PostgreSQL either pads it with a zero
    ## (HH12) or removes the padding altogether (FMHH12), so there is no one format which
    ## writes the same string. They are reported with what they mean instead of being
    ## converted into something which is nearly right.
    CONVERT_STYLES_WITHOUT_A_FORMAT = {
        0: 'mon dd yyyy hh:miAM, the hour padded with a space',
        100: 'mon dd yyyy hh:miAM, the hour padded with a space',
        9: 'mon dd yyyy hh:mi:ss:mmmAM, the hour padded with a space',
        109: 'mon dd yyyy hh:mi:ss:mmmAM, the hour padded with a space',
        22: 'mm/dd/yy hh:mi:ss AM, the hour padded with a space',
        130: 'day mon yyyy hh:mi:ss:mmmAM in the Hijri calendar',
        131: 'dd/mm/yyyy hh:mi:ss:mmmAM in the Hijri calendar',
    }

    def _convert_style_argument(self, expression, target_type, style_argument):
        """
        `CONVERT(type, expression, style)` as PostgreSQL writes or reads that style.

        The style decides what the value looks like, so it decides what is stored in every
        row which takes the default. It used to be dropped with a warning and the call became
        a plain CAST, which writes the ISO notation whatever the source asked for.

        Answers None when the style cannot be carried over - an unknown one, one which no
        single to_char() format can write, or a target type where the number does not mean a
        date format at all (the styles of BINARY and MONEY are a different table) - and says
        so. The caller then falls back to the CAST, which is what happened before.
        """
        style_text = self.strip_enclosing_parentheses(str(style_argument).strip()).strip()
        try:
            style = int(style_text)
        except (TypeError, ValueError):
            self.config_parser.print_log_message('WARNING', f"ms_sql_connector: convert_default_value: CONVERT style '{style_argument}' is not a number - it is dropped and the value is CAST instead, which writes the ISO notation.")
            return None

        upper_type = str(target_type).upper()
        is_text = 'CHAR' in upper_type or 'TEXT' in upper_type
        is_date = upper_type.startswith('DATE') and 'TIME' not in upper_type
        is_timestamp = 'TIMESTAMP' in upper_type
        style_format = self.CONVERT_STYLE_FORMATS.get(style)

        if style_format and (is_text or is_date or is_timestamp):
            if is_text:
                ## the CAST is kept around it: Transact-SQL truncates the styled value to the
                ## length of the target type, and so does a cast to varchar(n)
                return f"CAST(to_char({expression}, '{style_format}') AS {target_type})"
            if is_date:
                return f"to_date({expression}, '{style_format}')"
            return f"to_timestamp({expression}, '{style_format}')::{target_type}"

        if style in self.CONVERT_STYLES_WITHOUT_A_FORMAT:
            self.config_parser.print_log_message('WARNING', f"ms_sql_connector: convert_default_value: CONVERT style {style} writes {self.CONVERT_STYLES_WITHOUT_A_FORMAT[style]}, which no single PostgreSQL to_char() format writes - the value is CAST instead, so it comes out in the ISO notation and NOT as the source wrote it. Write the default by hand if the notation matters.")
            return None

        if style_format:
            self.config_parser.print_log_message('WARNING', f"ms_sql_connector: convert_default_value: CONVERT style {style} is a date format, but the target type {target_type} is not a text, date or timestamp type - the style is dropped.")
            return None

        self.config_parser.print_log_message('WARNING', f"ms_sql_connector: convert_default_value: CONVERT style {style} is not one this migrator knows - it is dropped and the value is CAST instead, which may not write what the source wrote. Check what style {style} produces on the source.")
        return None

    def _convert_convert_calls(self, text: str, settings) -> str:
        """
        Rewrites T-SQL CONVERT(data_type, expression [, style]) into PostgreSQL
        CAST(expression AS data_type), or into the to_char() / to_date() / to_timestamp()
        which writes and reads what the style argument asked for - see
        _convert_style_argument().
        """
        target_db_type = settings.get('target_db_type', self.config_parser.get_target_db_type())
        types_mapping = self.get_types_mapping({'target_db_type': target_db_type})

        while True:
            match = re.search(r'(?i)\bCONVERT\s*\(', text)
            if not match:
                return text
            start = match.start()
            arguments_start = match.end()
            depth = 1
            in_literal = False
            position = arguments_start
            while position < len(text) and depth > 0:
                character = text[position]
                if in_literal:
                    if character == "'":
                        in_literal = False
                elif character == "'":
                    in_literal = True
                elif character == '(':
                    depth += 1
                elif character == ')':
                    depth -= 1
                position += 1
            if depth != 0:
                # unbalanced expression - leave it as it is
                return text
            arguments = self._split_top_level_arguments(text[arguments_start:position - 1])
            if len(arguments) < 2:
                return text
            data_type = self.strip_enclosing_parentheses(arguments[0]).strip()
            type_length = ''
            length_match = re.search(r'(\(\s*[^()]*\s*\))\s*$', data_type)
            if length_match:
                type_length = length_match.group(1)
                data_type = data_type[:length_match.start()].strip()
            data_type = data_type.strip('[]"').upper()
            data_type = types_mapping.get(data_type, data_type)
            if type_length and '(' not in data_type:
                data_type += type_length

            replacement = None
            if len(arguments) > 2:
                replacement = self._convert_style_argument(arguments[1], data_type, arguments[2])
            if replacement is None:
                replacement = f"CAST({arguments[1]} AS {data_type})"
            text = text[:start] + replacement + text[position:]

    def convert_default_value(self, settings) -> dict:
        extracted_default_value = settings['extracted_default_value']
        if extracted_default_value is None:
            return ''
        column_type = str(settings.get('column_type', '') or '').upper()
        default_value = str(extracted_default_value).strip()
        if default_value == '':
            return ''

        default_value = self.strip_enclosing_parentheses(default_value)
        create_def_match = re.search(r'(?i)^\s*CREATE\s+DEFAULT\s+.*?\s+AS\s+(.*?);?\s*$', default_value)
        if create_def_match:
            default_value = create_def_match.group(1).strip()
            default_value = self.strip_enclosing_parentheses(default_value)
        if default_value == '' or default_value.upper() == 'NULL':
            # NULL is the PostgreSQL default anyway
            return ''

        # unicode literals N'text' are plain literals in PostgreSQL
        default_value = re.sub(r"(?i)(?<![\w])N('(?:[^']|'')*')", r"\1", default_value)

        # MONEY / SMALLMONEY currency literals
        default_value = self._convert_money_literals(default_value)

        # quoted identifiers like [dbo].[my_function] or [varchar]
        default_value = self._rewrite_outside_string_literals(
            default_value, lambda fragment: re.sub(r'\[([^\[\]]+)\]', r'"\1"', fragment))

        # binary literals - 0x1F2E is written as bytea escape literal, the target
        # connector adds the quotes and the ::BYTEA cast
        binary_match = re.fullmatch(r'(?i)0x([0-9a-f]*)', default_value)
        if binary_match:
            if column_type.startswith('BYTEA'):
                return f"\\x{binary_match.group(1)}"
            return str(int(binary_match.group(1) or '0', 16))

        # UUID generators
        if re.search(r'(?i)\b(?:newid|newsequentialid)\s*\(\s*\)', default_value):
            return self.config_parser.get_uuid_default_function(column_type)

        default_value = self._convert_convert_calls(default_value, settings)

        # niladic functions - either as the whole default or nested in an expression
        def convert_functions(fragment: str) -> str:
            def replace(match):
                function_name = match.group(1).lower()
                mapped = self.MSSQL_DEFAULT_FUNCTIONS_TO_POSTGRESQL.get(function_name)
                return mapped if mapped else match.group(0)
            return re.sub(r'(?i)\b([a-z_][a-z0-9_]*)\s*\(\s*\)', replace, fragment)

        default_value = self._rewrite_outside_string_literals(default_value, convert_functions)

        # CURRENT_TIMESTAMP / SYSTEM_USER and friends are used without parentheses too
        bare_value = default_value.strip()
        if bare_value.lower() in self.MSSQL_DEFAULT_FUNCTIONS_TO_POSTGRESQL:
            return self.MSSQL_DEFAULT_FUNCTIONS_TO_POSTGRESQL[bare_value.lower()]

        return default_value.strip()

    def get_table_checksum(self, schema_name: str, table_name: str, columns: list):
        if not columns:
            return None
            
        cols_list = []
        for col in columns:
            dtype = col.get('data_type', '').lower()
            if any(x in dtype for x in ['lob', 'bytea', 'xml', 'json', 'text', 'image', 'ntext', 'varbinary']):
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
        if not columns or not pk_columns or not pk_values_list:
            return {}
            
        cols_list = []
        for col in columns:
            dtype = col.get('data_type', '').lower()
            if any(x in dtype for x in ['lob', 'bytea', 'xml', 'json', 'text', 'image', 'ntext', 'varbinary']):
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
