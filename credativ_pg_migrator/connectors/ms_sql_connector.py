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
from credativ_pg_migrator.database_connector import DatabaseConnector
from credativ_pg_migrator.migrator_logging import MigratorLogger
import re
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
                                self.config_parser.print_log_message('DEBUG', "Encoutered ELSE in Block. Treating as implicit block end.")
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

        def _parse(self, parse_method, raw_tokens, sql=None):
            self.reset()
            self.sql = sql or ""
            self._tokens = raw_tokens
            self._index = -1
            self._advance()

            expressions = []
            while self._curr:
                if self._match(TokenType.SEMICOLON):
                     continue

                stmt = parse_method(self)
                if not stmt:
                     if self._curr:
                          self.raise_error("Invalid expression / Unexpected token")
                     break
                expressions.append(stmt)
            return expressions

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

class MsSQLConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        if source_or_target not in ['source']:
            raise ValueError(f"MS SQL Server is only supported as a source database. Current value: {source_or_target}")

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
        except Exception as e:
            pass

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
            self.config_parser.print_log_message('ERROR', f"Unsupported target database type: {target_db_type}")
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
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
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
            self.config_parser.print_log_message('DEBUG2', f"MSSQL: Reading columns for {table_schema}.{table_name}")
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
                if self.is_string_type(column_type) and character_maximum_length is not None:
                    column_type += f"({character_maximum_length})"
                elif self.is_numeric_type(column_type) and numeric_precision is not None and numeric_scale is not None:
                    column_type += f"({numeric_precision}, {numeric_scale})"
                elif self.is_numeric_type(column_type) and numeric_precision is not None:
                    column_type += f"({numeric_precision})"

                result[ordinal_position] = {
                    'column_name': column_name,
                    'data_type': data_type,
                    'column_type': column_type,
                    'character_maximum_length': character_maximum_length,
                    'numeric_precision': numeric_precision,
                    'numeric_scale': numeric_scale,
                    'is_nullable': is_nullable,
                    'is_identity': is_identity,
                    'column_default_value': column_default,
                    'comment': ''
                }

            cursor.close()
            self.disconnect()
            return result
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
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
                'MONEY': 'INTEGER',
                'SMALLMONEY': 'INTEGER',
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
                STRING_AGG('"' + c.name + '"', ', ') WITHIN GROUP (ORDER BY ic.index_column_id) AS column_list
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE i.object_id = {source_table_id}
            GROUP BY i.name, i.is_unique, i.is_primary_key
            ORDER BY i.name
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)

            indexes = cursor.fetchall()

            for index in indexes:
                self.config_parser.print_log_message('DEBUG', f"Processing index: {index}")
                index_name = index[0].strip()
                index_unique = index[1]  ## integer 0 or 1
                index_primary_key = index[2]  ## integer 0 or 1
                index_columns = index[3].strip()
                index_owner = ''

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
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
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
        query = f"""
            WITH ConstraintColumns AS (
            SELECT
                fk.name AS constraint_name,
                STRING_AGG('"' + cc.name + '"', ', ') WITHIN GROUP (ORDER BY cc.column_id) AS constraint_columns
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            JOIN sys.columns cc ON fkc.parent_object_id = cc.object_id AND fkc.parent_column_id = cc.column_id
            WHERE fk.parent_object_id = {source_table_id}
            GROUP BY fk.name
            ),
            ReferencedColumns AS (
            SELECT
                fk.name AS constraint_name,
                STRING_AGG('"' + rc.name + '"', ', ') WITHIN GROUP (ORDER BY rc.column_id) AS referenced_columns
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
            WHERE fk.parent_object_id = {source_table_id}
            GROUP BY fk.name
            )
            SELECT
            fk.name AS constraint_name,
            'FOREIGN KEY' AS constraint_type,
            cc.constraint_columns,
            rt.name AS referenced_table,
            rc.referenced_columns,
            pt.name AS constraint_table,
            rs.name AS referenced_schema
            FROM sys.foreign_keys fk
            JOIN ConstraintColumns cc ON fk.name = cc.constraint_name
            JOIN ReferencedColumns rc ON fk.name = rc.constraint_name
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
                self.config_parser.print_log_message('DEBUG', f"Processing constraint: {constraint}")
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
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_constraint_sql(self, settings):
        return ""

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
        self.config_parser.print_log_message('DEBUG3', f"fetch_funcproc_names: query: {query}")
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
            self.config_parser.print_log_message('ERROR', f"Error fetching function/procedure names: {e}")
            return []

    def fetch_funcproc_code(self, funcproc_id: int):
        query = f"""
            SELECT m.definition
            FROM sys.sql_modules m
            WHERE m.object_id = {funcproc_id}
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            self.disconnect()

            if row:
                return row[0]
            return None
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching function/procedure code for id {funcproc_id}: {e}")
            return None

    def convert_funcproc_code(self, settings):
        funcproc_code = settings['funcproc_code']
        # target_db_type = settings['target_db_type'] # Unused
        # source_schema = settings['source_schema'] # Unused
        target_schema = settings['target_schema']
        # table_list = settings['table_list']
        # view_list = settings['view_list']

        # 1. Cleanup
        funcproc_code = funcproc_code.strip()
        # Remove usage of GO
        funcproc_code = re.sub(r'\bGO\b', '', funcproc_code, flags=re.IGNORECASE)

        # 2. Identify Type and Name
        # Support names with spaces [Name with Space] or "Name" or Name
        # Regex to match CREATE [OR ALTER] {PROC|PROCEDURE|FUNCTION} [schema.]name ...

        type_match = re.search(r'CREATE\s+(?:OR\s+ALTER\s+)?(PROC|PROCEDURE|FUNCTION)\s+(?:\[?(\w+)\]?\.?)?\[?([\w\s]+)\]?', funcproc_code, re.IGNORECASE)

        if not type_match:
             return f"/* FAILED TO PARSE DEFINITION */\n{funcproc_code}"

        obj_type_raw = type_match.group(1).upper()
        # schema_name = type_match.group(2) # Ignore source schema, use target_schema
        obj_name = type_match.group(3)

        is_proc = 'PROC' in obj_type_raw
        pg_type = 'PROCEDURE' if is_proc else 'FUNCTION'

        # 3. separate Body from Header
        # Usually AS begins the body. But parameters can be complex.
        # Simple heuristic: Split on first AS not inside parens?
        # Or just use the regex to extract finding the FIRST AS after the CREATE line.

        # Better: Convert the whole thing using declaration processing first

        # Extract Variables (Parameters + Body Locals)
        # We need to distinguish parameters because they go into the signature.

        # Regex to find parameters section
        # Logic: From Object Name end to 'AS'

        header_end_match = re.search(r'\bAS\b', funcproc_code, re.IGNORECASE)
        if not header_end_match:
             return f"/* COULD NOT FIND 'AS' KEYWORD */\n{funcproc_code}"

        header_part = funcproc_code[:header_end_match.start()]
        body_part = funcproc_code[header_end_match.end():]

        # Process Body declarations
        declarations = []
        types_mapping = self.get_types_mapping(settings)

        declaration_replacer = lambda m: self._declaration_replacer(m, settings, types_mapping, declarations)

        # Replace DECLAREs in body
        processed_body = re.sub(r'DECLARE\s+@.*?(?=\bBEGIN\b|\bIF\b|\bWHILE\b|\bSELECT\b|\bINSERT\b|\bUPDATE\b|\bDELETE\b|\bRETURN\b|\bSET\b|\bFETCH\b|\bOPEN\b|\bCLOSE\b|\bDEALLOCATE\b|\bDECLARE\b|$)', declaration_replacer, body_part, flags=re.IGNORECASE | re.DOTALL)

        # Apply substitutions
        processed_body = self._apply_data_type_substitutions(processed_body)
        processed_body = self._apply_udt_to_base_type_substitutions(processed_body, settings)

        # Convert Body Statements
        has_rowcount = '@@rowcount' in processed_body.lower()
        if has_rowcount:
             declarations.append('_rowcount INTEGER;')

        converted_stmts = self._convert_stmts(processed_body, settings, has_rowcount=has_rowcount)
        pg_body_content = "\n".join(converted_stmts)

        # 4. Construct Header (Parameters)
        # We need to parse parameters from header_part
        # This is tricky without a full parser.
        # Basic approach: Extract everything after name until AS.
        # But wait, header_part INCLUDES the Name.

        # Let's clean header part: remove CREATE ... Name
        header_clean = header_part[type_match.end():].strip()

        # Look for RETURNS clause (for functions)
        returns_clause = "RETURNS VOID" # Default for proc? No, Proc has no return
        if not is_proc:
             ret_match = re.search(r'\bRETURNS\s+(.*)', header_clean, re.IGNORECASE)
             if ret_match:
                  ret_type_raw = ret_match.group(1).strip()
                  # Clean wrapping parens if table variable
                  # If returns table -> RETURNS TABLE (...)

                  # Apply Type mapping
                  # For now: simple string replacement or simple parsing
                  # Check if it has @var TABLE
                  if 'TABLE' in ret_type_raw.upper():
                       returns_clause = f"RETURNS TABLE ({ret_type_raw.split('TABLE')[-1].strip()})"
                       # Need to fix types inside TABLE(...) definition too? Yes.
                       returns_clause = self._apply_data_type_substitutions(returns_clause)
                  else:
                       # scalar type
                       ret_mapped = self._apply_data_type_substitutions(ret_type_raw)
                       ret_mapped = self._apply_udt_to_base_type_substitutions(ret_mapped, settings)
                       for ms_type, pg_type in types_mapping.items():
                           ret_mapped = re.sub(rf'\b{re.escape(ms_type)}\b', pg_type, ret_mapped, flags=re.IGNORECASE)
                       returns_clause = f"RETURNS {ret_mapped}"

                  # Remove RETURNS from params string
                  header_clean = header_clean[:ret_match.start()].strip()
             else:
                  returns_clause = "RETURNS VOID" # Scalar func must have return?

        # Parse Parameters string `header_clean`
        # @p1 int, @p2 varchar(10) ...
        # Need to map types and replace @ with nothing or _

        pg_params = []
        if header_clean:
            # Clean outer parens if they exist (optional in TSQL)
            if header_clean.startswith('(') and header_clean.endswith(')'):
                header_clean = header_clean[1:-1]

            # Split by comma respecting parens (for decimal(10,2))
            # Reuse _split_respecting_parens but it doesn't handle comma.
            # Lazy split:
            param_parts = self._split_respecting_parens(header_clean)
            # Wait, _split_respecting_parens doesn't split by comma.
            # It tokenizes? No, viewing it: it looks like it iterates char by char but doesn't return list...
            # Ah, `view_file` cut off `_split_respecting_parens` implementation.

            # Assume naive split for now or regex
            params_list = re.split(r',(?![^(]*\))', header_clean)

            for p in params_list:
                p = p.strip()
                if not p: continue

                # Format: @name type [= default] [OUTPUT] [READONLY]
                # Regex search
                p_match = re.search(r'@([\w]+)\s+([\w\(\)]+)(.*)', p)
                if p_match:
                    p_name = p_match.group(1)
                    p_type = p_match.group(2)
                    p_rest = p_match.group(3) or ""

                    # Map type
                    p_type = self._apply_data_type_substitutions(p_type)
                    p_type = self._apply_udt_to_base_type_substitutions(p_type, settings)

                    # Apply built-in mapping
                    for ms_type, pg_type in types_mapping.items():
                        p_type = re.sub(rf'\b{re.escape(ms_type)}\b', pg_type, p_type, flags=re.IGNORECASE)

                    # Handle OUTPUT
                    mode = "INOUT " if "OUTPUT" in p_rest.upper() else ""

                    # Handle Default
                    default_val = ""
                    def_match = re.search(r'=\s*([^ ]+)', p_rest)
                    if def_match:
                        default_val = f" DEFAULT {def_match.group(1)}"

                    pg_params.append(f"{mode}locvar_{p_name} {p_type}{default_val}")

        pg_params_str = ", ".join(pg_params)

        # 5. Assembly
        # Quote Object Name for PG
        pg_name = f'"{obj_name}"'

        decl_section = "DECLARE\n" + "\n".join(declarations) if declarations else ""

        ddl = f"""
CREATE OR REPLACE {pg_type} "{target_schema}".{pg_name}({pg_params_str})
{returns_clause if not is_proc else ''}
AS $$
{decl_section}
BEGIN
{pg_body_content}
END;
$$ LANGUAGE plpgsql;
"""
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
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_view_code(self, settings):
        view_id = settings['view_id']
        source_schema = settings['source_schema']
        source_view_name = settings['source_view_name']
        target_schema = settings['target_schema']
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
                self.config_parser.print_log_message('DEBUG', f"View code for {source_schema}.{source_view_name}: {view_code}")
                return view_code
            cursor.close()
            self.disconnect()
            return view_code
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def convert_view_code(self, settings: dict):
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
                if schema and schema.name == settings['source_schema']:
                    node.set("db", sqlglot.exp.Identifier(this=settings['target_schema'], quoted=False))
            return node

        def quote_schema_and_table_names(node):
            if isinstance(node, (sqlglot.exp.Table, sqlglot.exp.Column)):
                # Quote schema name if present
                schema = node.args.get("db")
                if schema and not schema.args.get("quoted"):
                    schema.set("quoted", True)

                # Quote table name
                if isinstance(node, sqlglot.exp.Table):
                    table = node.args.get("this")
                else:
                    table = node.args.get("table")

                if table and not table.args.get("quoted"):
                    table.set("quoted", True)
            return node

        def replace_functions(node):
            mapping = self.get_sql_functions_mapping({ 'target_db_type': settings['target_db_type'] })
            # Prepare mapping for function names (without parentheses)
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
                    return sqlglot.exp.Anonymous(this=mapped)
            return node

        def replace_udts(node):
            if isinstance(node, sqlglot.exp.DataType):
                # Check if the type is a UDT
                type_name = node.this.name if hasattr(node.this, 'name') else str(node.this)

                if type_name in udt_lookup_map:
                     udt_info = udt_lookup_map[type_name]
                     return sqlglot.exp.DataType.build(udt_info['sql']) # Use 'sql' or 'definition'
            return node

        def transform_sybase_joins(expression):
            # Check for EQ nodes with outer join comments (sqlglot default parsing of *=)
            # OR check for 'outer_join' property if parsing handles it.
            # sqlglot T-SQL might parse *= as normal EQ, or custom.
            # Since we didn't inject the scanner pre-processor for *= yet (it's in Sybase v2 conversion),
            # we rely on sqlglot.
            # MSSQL views likely use standard ANSI JOINs, but legacy syntax exists.
            # We assume ANSI joins for now, or sqlglot handles standard T-SQL.
            return expression

        view_code = settings['view_code']
        CustomTSQL.Parser.config_parser = self.config_parser
        try:
            expressions = sqlglot.parse(view_code, read=CustomTSQL)
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Failed to parse view code: {e}")
            return f"-- ERROR parsing view: {e}\n/*\n{view_code}\n*/"

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
                # expression = transform_sybase_joins(expression) # Not needed if standard SQL

                pg_sql = expression.sql(dialect='postgres')
                transformed_sqls.append(pg_sql)
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"Failed to transform expression: {e}")
                transformed_sqls.append(f"-- ERROR transforming: {e}")

        final_select_sql = "\n".join(transformed_sqls)

        target_schema = settings['target_schema']
        target_view_name = settings['target_view_name']

        final_view_sql = f"CREATE OR REPLACE VIEW \"{target_schema}\".\"{target_view_name}\" AS\n{final_select_sql};"
        return final_view_sql

    def migrate_table(self, migrate_target_connection, settings):
        part_name = 'initialize'
        source_table_rows = 0
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
            source_schema = settings['source_schema']
            source_table = settings['source_table']
            source_table_id = settings['source_table_id']
            source_columns = settings['source_columns']
            # target_schema = self.config_parser.convert_names_case(settings['target_schema'])
            target_schema = settings['target_schema'] ## target schema is used as it is defined in config, not converted to upper/lower case
            target_table = self.config_parser.convert_names_case(settings['target_table'])
            target_columns = settings['target_columns']
            batch_size = settings['batch_size']
            migrator_tables = settings['migrator_tables']
            migration_limitation = settings['migration_limitation']
            chunk_size = settings['chunk_size']
            chunk_number = settings['chunk_number']
            resume_after_crash = settings['resume_after_crash']
            drop_unfinished_tables = settings['drop_unfinished_tables']

            source_table_rows = self.get_rows_count(source_schema, source_table, migration_limitation)
            target_table_rows = migrate_target_connection.get_rows_count(target_schema, target_table)

            total_chunks = self.config_parser.get_total_chunks(source_table_rows, chunk_size)
            if chunk_size == -1:
                chunk_size = source_table_rows + 1

            migration_stats = {
                'rows_migrated': target_table_rows,
                'chunk_number': chunk_number,
                'total_chunks': total_chunks,
                'source_table_rows': source_table_rows,
                'target_table_rows': target_table_rows,
                'finished': True if source_table_rows == 0 else False,
            }
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

                if source_table_rows > target_table_rows:

                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Source table {source_table}: {source_table_rows} rows / Target table {target_table}: {target_table_rows} rows - starting data migration.")

                    select_columns_list = []
                    orderby_columns_list = []
                    insert_columns_list = []
                    for order_num, col in source_columns.items():
                        self.config_parser.print_log_message('DEBUG2',
                                                            f"Worker {worker_id}: Table {source_schema}.{source_table}: Processing column {col['column_name']} ({order_num}) with data type {col['data_type']}")

                        # if col['data_type'].lower() == 'datetime':
                        #     select_columns_list.append(f"TO_CHAR({col['column_name']}, '%Y-%m-%d %H:%M:%S') as {col['column_name']}")
                        #     select_columns_list.append(f"ST_asText(`{col['column_name']}`) as `{col['column_name']}`")
                        # elif col['data_type'].lower() == 'set':
                        #     select_columns_list.append(f"cast(`{col['column_name']}` as char(4000)) as `{col['column_name']}`")
                        # else:
                        select_columns_list.append(f'''[{col['column_name']}]''')

                        insert_columns_list.append(f'''"{self.config_parser.convert_names_case(col['column_name'])}"''')
                        orderby_columns_list.append(f'''[{col['column_name']}]''')

                    select_columns = ', '.join(select_columns_list)
                    insert_columns = ', '.join(insert_columns_list)
                    orderby_columns = ', '.join(orderby_columns_list)

                    if resume_after_crash and not drop_unfinished_tables:
                        chunk_number = self.config_parser.get_total_chunks(target_table_rows, chunk_size)
                        self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Resuming migration for table {source_schema}.{source_table} from chunk {chunk_number} with data chunk size {chunk_size}.")
                        chunk_offset = target_table_rows
                    else:
                        chunk_offset = (chunk_number - 1) * chunk_size

                    chunk_start_row_number = chunk_offset + 1
                    chunk_end_row_number = chunk_offset + chunk_size

                    self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Migrating table {source_schema}.{source_table}: chunk {chunk_number}, data chunk size {chunk_size}, batch size {batch_size}, chunk offset {chunk_offset}, chunk end row number {chunk_end_row_number}, source table rows {source_table_rows}")
                    order_by_clause = ''

                    # if table is small, skipping ordering does not make sense because it will not speed up the migration
                    # if chunk_size > source_table_rows:
                    #     query = f'''SELECT {select_columns} FROM "{source_schema}".{source_table}'''
                    #     if migration_limitation:
                    #         query += f" WHERE {migration_limitation}"
                    # else:

                    query = f"SELECT {select_columns} FROM [{source_schema}].[{source_table}]"
                    if migration_limitation:
                        query += f" WHERE {migration_limitation}"
                    primary_key_columns = migrator_tables.select_primary_key(source_schema, source_table)
                    self.config_parser.print_log_message('DEBUG2', f"Worker {worker_id}: Primary key columns for {source_schema}.{source_table}: {primary_key_columns}")
                    if primary_key_columns:
                        orderby_columns = primary_key_columns
                    order_by_clause = f""" ORDER BY {orderby_columns}"""
                    query += order_by_clause + f" OFFSET {chunk_offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY;"

                    self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Fetching data with cursor using query: {query}")

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
                    while True:
                        records = cursor.fetchmany(batch_size)
                        if not records:
                            break
                        batch_number += 1
                        reading_end_time = time.time()
                        reading_duration = reading_end_time - reading_start_time
                        self.config_parser.print_log_message('DEBUG',f"Worker {worker_id}: Fetched {len(records)} rows (batch {batch_number}) from source table {source_table}.")

                        transforming_start_time = time.time()
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
                        self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Starting insert of {len(records)} rows from source table {source_table}")
                        transforming_end_time = time.time()
                        transforming_duration = transforming_end_time - transforming_start_time
                        inserting_start_time = time.time()
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
                        inserting_end_time = time.time()
                        inserting_duration = inserting_end_time - inserting_start_time

                        batch_end_time = time.time()
                        batch_duration = batch_end_time - batch_start_time
                        batch_durations.append(batch_duration)
                        percent_done = round(total_inserted_rows / source_table_rows * 100, 2)

                        batch_start_dt = datetime.datetime.fromtimestamp(batch_start_time)
                        batch_end_dt = datetime.datetime.fromtimestamp(batch_end_time)
                        batch_start_str = batch_start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')
                        batch_end_str = batch_end_dt.strftime('%Y-%m-%d %H:%M:%S.%f')
                        migrator_tables.insert_batches_stats({
                            'source_schema': source_schema,
                            'source_table': source_table,
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
                            f"(total: {total_inserted_rows} from: {source_table_rows} "
                            f"({percent_done}%)) rows into target table '{target_table}': "
                            f"Batch {batch_number} duration: {batch_duration:.2f} seconds "
                            f"(r: {reading_duration:.2f}, t: {transforming_duration:.2f}, w: {inserting_duration:.2f})"
                        )
                        self.config_parser.print_log_message('INFO', msg)

                        batch_start_time = time.time()
                        reading_start_time = batch_start_time

                    target_table_rows = migrate_target_connection.get_rows_count(target_schema, target_table)
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Target table {target_schema}.{target_table} has {target_table_rows} rows")

                    shortest_batch_seconds = min(batch_durations) if batch_durations else 0
                    longest_batch_seconds = max(batch_durations) if batch_durations else 0
                    average_batch_seconds = sum(batch_durations) / len(batch_durations) if batch_durations else 0
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Migrated {total_inserted_rows} rows from {source_table} to {target_schema}.{target_table} in {batch_number} batches: "
                                                            f"Shortest batch: {shortest_batch_seconds:.2f} seconds, "
                                                            f"Longest batch: {longest_batch_seconds:.2f} seconds, "
                                                            f"Average batch: {average_batch_seconds:.2f} seconds")

                    cursor.close()

                elif source_table_rows <= target_table_rows:
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Source table {source_table} has {source_table_rows} rows, which is less than or equal to target table {target_table} with {target_table_rows} rows. No data migration needed.")

                migration_stats = {
                    'rows_migrated': total_inserted_rows,
                    'chunk_number': chunk_number,
                    'total_chunks': total_chunks,
                    'source_table_rows': source_table_rows,
                    'target_table_rows': target_table_rows,
                    'finished': False,
                }

                self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Migration stats: {migration_stats}")
                if source_table_rows <= target_table_rows or chunk_number >= total_chunks:
                    self.config_parser.print_log_message('DEBUG3', f"Worker {worker_id}: Setting migration status to finished for table {source_table} (chunk {chunk_number}/{total_chunks})")
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
                    'source_schema': source_schema,
                    'source_table': source_table,
                    'target_schema': target_schema,
                    'target_table': target_table,
                    'source_table_rows': source_table_rows,
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
            self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Error during {part_name} -> {e}")
            self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Full stack trace: {traceback.format_exc()}")
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
            self.config_parser.print_log_message('ERROR', f"Error fetching triggers: {e}")
            return {}

    def convert_trigger(self, settings):
        trigger_name = settings['trigger_name']
        trigger_code = settings['trigger_sql']
        source_schema = settings['source_schema']
        target_schema = settings['target_schema']
        table_list = settings['table_list']

        # Clean up code
        trigger_code = trigger_code.strip()

        # Regular Expressions to extract info
        # ON [Schema].[Table]
        table_match = re.search(r'ON\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?', trigger_code, re.IGNORECASE)
        table_name = table_match.group(2) if table_match else "UNKNOWN_TABLE"

        # Events
        events = []
        if re.search(r'\bINSERT\b', trigger_code, re.IGNORECASE): events.append('INSERT')
        if re.search(r'\bUPDATE\b', trigger_code, re.IGNORECASE): events.append('UPDATE')
        if re.search(r'\bDELETE\b', trigger_code, re.IGNORECASE): events.append('DELETE')

        # Timing
        timing = 'AFTER'
        if re.search(r'\bINSTEAD\s+OF\b', trigger_code, re.IGNORECASE):
            timing = 'INSTEAD OF'

        # Body Isolation
        body_match = re.search(r'CREATE\s+TRIGGER\s+.*?\s+AS\s+(.*)', trigger_code, re.IGNORECASE | re.DOTALL)
        body_start = body_match.group(1) if body_match else ""

        if not body_start:
             return f"/* COULD NOT ISOLATE BODY FOR {trigger_name} */ {trigger_code}"

        # --- Conversion Logic adhering to Sybase pattern ---

        # 1. Variable Declarations Extraction
        declarations = []
        types_mapping = self.get_types_mapping(settings)

        # Helper wrapper for replacer
        declaration_replacer = lambda m: self._declaration_replacer(m, settings, types_mapping, declarations)

        # Extract declarations
        body_content = re.sub(r'DECLARE\s+@.*?(?=\bBEGIN\b|\bIF\b|\bWHILE\b|\bSELECT\b|\bINSERT\b|\bUPDATE\b|\bDELETE\b|\bRETURN\b|\bSET\b|\bFETCH\b|\bOPEN\b|\bCLOSE\b|\bDEALLOCATE\b|\bDECLARE\b|$)', declaration_replacer, body_start, flags=re.IGNORECASE | re.DOTALL)

        # 2. UDT & Type Substitutions
        body_content = self._apply_data_type_substitutions(body_content)
        body_content = self._apply_udt_to_base_type_substitutions(body_content, settings)

        # 3. Convert Statements
        has_rowcount = '@@rowcount' in body_content.lower()
        if has_rowcount:
             declarations.append('_rowcount INTEGER;')

        converted_stmts = self._convert_stmts(body_content, settings, has_rowcount=has_rowcount, is_trigger=True)
        pg_body = "\n".join(converted_stmts)

        # Construct Function
        func_name = f"tf_{trigger_name}"
        func_schema = target_schema

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

        # Construct Trigger (Changed to Row Level to support NEW/OLD replacement)
        # Transition tables (inserted/deleted) are replaced by NEW/OLD record access by AST transformer

        events_str = " OR ".join(events)

        # NOTE: MSSQL Triggers are statement-level by default but access 'inserted' set.
        # User requested mapping 'inserted' -> NEW and 'deleted' -> OLD.
        # This implies running FOR EACH ROW.
        # Warning: This changes semantics if the logic was doing set-based aggregation.
        # But for row-processing logic (select @var = col from inserted), this conversion is correct.

        trigger_ddl = f"""
CREATE TRIGGER "{trigger_name}"
{timing} {events_str} ON "{target_schema}"."{table_name}"
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
        # ...existing code from SybaseASEConnector.handle_error...
        pass

    def get_rows_count(self, table_schema: str, table_name: str, migration_limitation: str = None):
        query = f"""SELECT COUNT(*) FROM [{table_schema}].[{table_name}]"""
        if migration_limitation:
            query += f" WHERE {migration_limitation}"
        self.config_parser.print_log_message('DEBUG', f"get_rows_count query: {query}")
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

    def fetch_sequences(self):
        query = """
            SELECT
                s.name AS sequence_name,
                s.object_id AS sequence_id,
                s.start_value AS start_value,
                s.increment AS increment_value,
                s.min_value AS min_value,
                s.max_value AS max_value,
                s.cycle_option AS cycle_option
            FROM sys.sequences s
        """
        # ...existing code from SybaseASEConnector.fetch_sequences...
        pass

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
            self.config_parser.print_log_message('ERROR', f"Error fetching UDTs: {e}")
            return {}

    def _get_udt_map(self):
        """ Helper to get a map of UDT name -> definition for conversion logic """
        udts_full = self.fetch_user_defined_types('dbo')
        udt_map = {}
        for k, v in udts_full.items():
            udt_map[v['type_name']] = v
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
        self.config_parser.print_log_message('DEBUG3', f"MS SQL connector: Getting table description for {settings['table_schema']}.{settings['table_name']}")
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

        source_schema = settings['source_schema']
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
                    WHERE s.name = '{source_schema}'
                    GROUP BY s.name, t.name
                    ORDER BY total_size DESC
                """
                self.connect()
                cursor = self.connection.cursor()
                cursor.execute(query.format(source_schema=source_schema))
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
                self.config_parser.print_log_message('DEBUG', f"Top {top_n} tables by rows: {top_tables['by_rows']}")
            else:
                self.config_parser.print_log_message('DEBUG', "Top N tables by rows is not configured or set to 0, skipping this part.")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Error fetching top tables by rows: {e}")

        return top_tables

    def get_top_fk_dependencies(self, settings):
        top_fk_dependencies = {}
        return top_fk_dependencies

    def target_table_exists(self, target_schema, target_table):
        query = f"""
            SELECT COUNT(*)
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = '{target_schema}' AND t.name = '{target_table}'
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



    def _convert_stmts(self, body_content, settings, is_nested=False, has_rowcount=False, is_trigger=False):
        import base64
        processed_body = body_content

         # --- Handle Sybase/MSSQL Outer Joins (*= and =*) before parsing ---
        processed_body = re.sub(r"([\w\.]+)\s*\*\=\s*([\w\.]+)", r"locvar_sybase_outer_join(\1, \2)", processed_body)
        processed_body = re.sub(r"([\w\.]+)\s*\=\*\s*([\w\.]+)", r"locvar_sybase_right_join(\1, \2)", processed_body)

        CustomTSQL.Parser.config_parser = self.config_parser

        try:
             parsed = sqlglot.parse(processed_body.strip(), read=CustomTSQL)
        except Exception as e:
             self.config_parser.print_log_message('ERROR', f"Global parsing failed: {e}")
             return [f"/* PARSING FAILED: {e} */\n" + body_content]

        converted_statements = []
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
             if not expression: return None

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

             pg_sql = expression.sql(dialect='postgres')

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

    def get_types_mapping(self, settings):
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
                    self.config_parser.print_log_message('WARNING', f"Invalid regex in data_types_substitution: {source_type}")

        return text

    def _apply_udt_to_base_type_substitutions(self, text, settings):
        udt_map = self._get_udt_map()
        for udt, info in udt_map.items():
             text = re.sub(rf'\b{re.escape(udt)}\b', info['base_type'], text, flags=re.IGNORECASE)
        return text



if __name__ == "__main__":
    print("This script is not meant to be run directly")
