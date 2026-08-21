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
from credativ_pg_migrator.jvm_helper import detach_thread_from_jvm
import re
import traceback
import sys
from tabulate import tabulate
import sqlglot
from credativ_pg_migrator.connectors.tsql_parser import TsqlParser
from credativ_pg_migrator.query_conversion import outer_joins as query_outer_joins
from sqlglot import exp, TokenType
from sqlglot.dialects import TSQL
import time
import datetime
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
             if self._curr.token_type in (TokenType.UPDATE, TokenType.INSERT, TokenType.DELETE, TokenType.MERGE, TokenType.SET):
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
                                if txt in ('SELECT', 'UPDATE', 'INSERT', 'DELETE', 'BEGIN', 'IF', 'WHILE', 'RETURN', 'DECLARE', 'CREATE', 'TRUNCATE', 'GO'):
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
        STATEMENT_PARSERS = TSQL.Parser.STATEMENT_PARSERS.copy()
        STATEMENT_PARSERS[TokenType.COMMAND] = _parse_command_custom
        STATEMENT_PARSERS[TokenType.SET] = _parse_command_custom

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
            return "\n".join(stmts)

        TRANSFORMS[Block] = _block_handler

class SybaseASEConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        if source_or_target != 'source':
            raise ValueError(f"Sybase ASE is only supported as a source database")

        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.on_error_action = self.config_parser.get_on_error_action()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger
        self._udt_cache = None
        ## the messages of sysusermessages, read once when a RAISERROR needs one
        self._user_messages = None

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
        finally:
            self.connection = None
            detach_thread_from_jvm()

    def get_sql_functions_mapping(self, settings):
        """ Returns a dictionary of SQL functions mapping for the target database """
        target_db_type = settings['target_db_type']
        if target_db_type == 'postgresql':
            return {
                'getdate()': 'current_timestamp',
                'getutcdate()': "timezone('UTC', now())",
                'datetime': 'current_timestamp',
                'current_timestamp()': 'CURRENT_TIMESTAMP',
                'year(': 'extract(year from ',
                'month(': 'extract(month from ',
                'day(': 'extract(day from ',

                'db_name()': 'current_database()',
                'dbo.suser_name()': 'current_user',
                'dbo.user_sname()': 'current_user',
                'suser_name()': 'current_user',
                'user_name()': 'current_user',
                'len(': 'length(',
                'isnull(': 'coalesce(',
                'datalength(': 'length(',
                'substring(': 'substring(',
                'charindex(': 'position(',
                'str_replace(': 'replace(',
                'stuff(': 'overlay(',
                'dateadd(': "now() + interval '",  # requires more complex logic
                'datediff(': "age(",  # requires more logic
                'datepart(yy,': "date_part('year',",
                'datepart(yyyy,': "date_part('year',",
                'datepart(year,': "date_part('year',",
                'datepart(qq,': "date_part('quarter',",
                'datepart(mm,': "date_part('month',",
                'datepart(month,': "date_part('month',",
                'datepart(dy,': "date_part('doy',",
                'datepart(dd,': "date_part('day',",
                'datepart(wk,': "date_part('week',",
                'datepart(hh,': "date_part('hour',",
                'datepart(mi,': "date_part('minute',",
                'datepart(ss,': "date_part('second',",
                'datepart(ms,': "date_part('milliseconds',",
                'try_cast(': 'CAST(',
                '@@nestlevel': '0',
            }
        else:
            self.config_parser.print_log_message('ERROR', f"sybase_ase_connector: get_sql_functions_mapping: Unsupported target database type: {target_db_type}")
            return {}

    def _split_respecting_parens(self, text):
        parts = []
        current = []
        depth = 0
        for char in text:
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
            if char == ',' and depth == 0:
                parts.append("".join(current).strip())
                current = []
            else:
                current.append(char)
        if current:
            parts.append("".join(current).strip())
        return parts

    def _declaration_replacer(self, match, settings, types_mapping, declarations):
        full_decl = match.group(0)
        content = full_decl[7:].strip() # len('DECLARE') = 7
        content_clean = content.replace('@', '')
        content_clean = self._apply_data_type_substitutions(content_clean)
        content_clean = self._apply_udt_to_base_type_substitutions(content_clean, settings)
        ## the UDTs were just replaced by the types of the target - they must not be mapped again
        content_clean = self._apply_types_mapping(content_clean, self._types_mapping_for_mapped_text(types_mapping))

        parts = self._split_respecting_parens(content_clean)
        for part in parts:
            declarations.append(part.strip() + ';')
        return ''

    def migrate_sequences(self, target_connector, settings):
        return True

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
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sybase_ase_connector: fetch_table_names: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    # A money literal of Sybase - $1000, $19.99, $-5 - is a number written with a currency
    # sign in front of it. PostgreSQL has no such literal and reads '$1000' in a statement
    # as the positional parameter number 1000: 'there is no parameter $1000'.
    MONEY_LITERAL_PATTERN = re.compile(r"""(?<![A-Za-z0-9_$@#'"])\$\s*([-+]?)\s*(\d+(?:\.\d*)?|\.\d+)""")

    @classmethod
    def _convert_money_literals(cls, sql_text):
        """
        Rewrite the money literals of an expression into plain numbers. The MONEY of the
        source is migrated as NUMERIC(19,4), so the number alone is the whole value. What
        stands inside a string literal is data ('costs $5') and is left untouched.
        """
        if not sql_text or '$' not in str(sql_text):
            return sql_text
        parts = re.split(r"('(?:[^']|'')*')", str(sql_text))
        for position in range(0, len(parts), 2):
            parts[position] = cls.MONEY_LITERAL_PATTERN.sub(
                lambda match: f"{match.group(1) if match.group(1) == '-' else ''}{match.group(2)}", parts[position])
        return ''.join(parts)

    @staticmethod
    def _joined_text_pieces(pieces):
        """
        syscomments stores the text of an object in pieces of 255 bytes, numbered by colid.
        They are a byte stream and must be joined in that order and without a separator -
        a piece can end in the middle of a word.
        """
        if not pieces:
            return ''
        return ''.join(text for _, text in sorted(pieces.items()) if text is not None)

    def _extract_default_expression(self, default_text, default_object_name=None):
        """
        The expression of a column default, taken out of the text syscomments keeps for it.
        Two shapes arrive here:
          - the default written in the CREATE TABLE, which Sybase stores as
            "DEFAULT  getdate()";
          - a default object bound to the column with sp_bindefault, whose text is the
            complete batch which created it - "create default d_zero as 0", including
            every comment written in that batch. The comments have to be removed while
            the line breaks are still there, otherwise a '--' comment swallows the whole
            statement behind it and the "default" of the column becomes a comment.
        """
        if not default_text:
            return ''
        text = self.strip_sql_comments(str(default_text))
        text = re.sub(r'\s+', ' ', text).strip()
        if not text:
            return ''

        create_default = re.search(
            r'(?is)\bCREATE\s+DEFAULT\s+(?:(?:[A-Za-z0-9_$#]+|"[^"]+"|\[[^\]]+\])\s*\.\s*)*'
            r'(?:[A-Za-z0-9_$#]+|"[^"]+"|\[[^\]]+\])\s+AS\s*(.*)$', text)
        if create_default:
            text = create_default.group(1).strip()
        else:
            text = re.sub(r'(?i)^DEFAULT\s+', '', text).strip()

        text = text.rstrip(';').strip()
        # The batch separator is not part of the value
        text = re.sub(r'(?is)\s*\bGO\s*$', '', text).strip()
        # A name Sybase wrote in double quotes is not a string literal - the quotes would
        # reach the target as part of the value.
        if len(text) > 1 and text.startswith('"') and text.endswith('"'):
            text = text[1:-1].strip()
        if not text and default_object_name:
            self.config_parser.print_log_message('WARNING', f"sybase_ase_connector: _extract_default_expression: The default object {default_object_name} carries no value which could be migrated - its text is: {str(default_text).strip()}")
        return text

    def fetch_table_columns(self, settings) -> dict:
        table_schema = settings['table_schema']
        table_name = settings['table_name']
        result = {}
        try:
            self.connect()
            cursor = self.connection.cursor()
            self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: fetch_table_columns: Sybase ASE: Reading columns for {table_schema}.{table_name}")
            cursor.execute("SELECT @@unicharsize, @@ncharsize")
            unichar_size, nchar_size = cursor.fetchone()
            self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: fetch_table_columns: Sybase ASE: unichar size: {unichar_size}, nchar size: {nchar_size}")
            query = f"""
                SELECT
                    c.colid as ordinal_position,
                    c.name as column_name,
                    t.name as data_type,
                    '' as data_type_length,
                    c.length,
                    CASE
                        WHEN c.status&8=8 and t.name <> 'bit' THEN 1
                    ELSE 0 END AS column_nullable,
                    CASE
                        WHEN c.status&128=128 and t.name <> 'bit' THEN 1
                    ELSE 0 END AS identity_column,
                    '' as full_data_type_length,
                    object_name(c.domain) as column_domain,
                    object_name(c.cdefault) as column_default_name,
                    co.text as column_default_value,
                    c.status,
                    t.variable as variable_length,
                    c.prec as data_type_precision,
                    c.scale as data_type_scale,
                    t.allownulls as type_nullable,
                    t.ident as type_has_identity_property,
                    object_name(c.domain) as domain_name,
                    case when c.status2 & 16 = 16 then 1 else 0 end is_generated_virtual,
                    case when c.status2 & 32 = 32 then 1 else 0 end is_genreated_stored,
                    com.text as computed_column_expression,
                    case when c.status3 & 1 = 1 then 1 else 0 end as is_hidden_column,
                    co.colid as default_text_piece,
                    com.colid as computed_text_piece
                FROM syscolumns c
                JOIN sysobjects tab ON c.id = tab.id
                JOIN systypes t ON c.usertype = t.usertype
                LEFT JOIN syscomments co ON c.cdefault = co.id
                LEFT JOIN syscomments com ON c.computedcol = com.id
                WHERE user_name(tab.uid) = '{table_schema}'
                    AND tab.name = '{table_name}'
                    AND tab.type = 'U'
                ORDER BY c.colid, co.colid, com.colid
            """
            cursor.execute(query)
            rows = cursor.fetchall()

            # syscomments keeps the text of a default or of a computed column in pieces of
            # 255 bytes - a long one arrives as several rows, and the two joins multiply
            # them. The pieces are collected per column first, so that every column is
            # built once and from its complete text.
            default_text_pieces = {}
            computed_text_pieces = {}
            for row in rows:
                if row[10] is not None and row[22] is not None:
                    default_text_pieces.setdefault(row[0], {})[row[22]] = row[10]
                if row[20] is not None and row[23] is not None:
                    computed_text_pieces.setdefault(row[0], {})[row[23]] = row[20]

            processed_positions = set()
            for row in rows:
                ordinal_position = row[0]
                if ordinal_position in processed_positions:
                    continue
                processed_positions.add(ordinal_position)
                self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: fetch_table_columns: Processing column: {row}")
                column_name = row[1].strip()
                data_type = row[2].strip()
                # data_type_length = row[3].strip()
                length = row[4]
                column_nullable = row[5]
                identity_column = row[6]
                # full_data_type_length = row[7].strip()
                column_domain = row[8]
                column_default_name = row[9]
                column_default_value = self._extract_default_expression(
                    self._joined_text_pieces(default_text_pieces.get(ordinal_position)), column_default_name)
                status = row[11]
                variable_length = row[12]
                data_type_precision = row[13]
                data_type_scale = row[14]
                type_nullable = row[15]
                type_has_identity_property = row[16]
                domain_name = row[17]
                is_generated_virtual = row[18]
                is_generated_stored = row[19]
                generation_expression = self._joined_text_pieces(computed_text_pieces.get(ordinal_position)) or None
                is_hidden_column = row[21]
                stripped_generation_expression = self._convert_computed_column_expression(
                    generation_expression, f"computed column {table_schema}.{table_name}.{column_name}")

                if data_type.lower() in ('univarchar', 'unichar'):
                    data_type_length = str(int(length / unichar_size))
                    character_maximum_length = int(length / unichar_size)
                elif data_type.lower() in ('nvarchar', 'nchar'):
                    data_type_length = str(int(length / nchar_size))
                    character_maximum_length = int(length / nchar_size)
                elif data_type.lower() in ('numeric', 'double precision', 'decimal'):
                    data_type_length = f"{data_type_precision},{data_type_scale}"
                    character_maximum_length = None
                else:
                    data_type_length = length
                    character_maximum_length = length if self.is_string_type(data_type) else None

                full_data_type_length = f"{data_type}({data_type_length})" if data_type_length else data_type

                result[ordinal_position] = {
                    'column_name': column_name,
                    'data_type': data_type,
                    'column_type': full_data_type_length,
                    'character_maximum_length': character_maximum_length,
                    'numeric_precision': data_type_precision if self.is_numeric_type(data_type) else None,
                    'numeric_scale': data_type_scale if self.is_numeric_type(data_type) else None,
                    'is_nullable': 'NO' if column_nullable == 0 else 'YES',
                    'column_default_name': column_default_name,
                    'column_default_value': column_default_value,
                    'column_comment': '',
                    'is_identity': 'YES' if identity_column == 1 else 'NO',
                    'domain_name': domain_name,
                    'is_generated_virtual': 'YES' if is_generated_virtual == 1 else 'NO',
                    'is_generated_stored': 'YES' if is_generated_stored == 1 else 'NO',
                    'generation_expression': generation_expression,
                    'stripped_generation_expression': stripped_generation_expression,
                    'is_hidden_column': 'YES' if is_hidden_column == 1 else 'NO',
                }

                # Check for config substitutions first (Higher Priority)
                config_substitutions = self.config_parser.get_data_types_substitution()
                substitution_found = False

                # Iterate through substitutions to find a match for the current data_type
                # Substitution format: [schema, table, source_type, target_type, target_length]
                # We prioritize exact matches on type name.
                for sub in config_substitutions:
                    # sub[2] is source_type, sub[3] is target_type
                    if sub[2].lower() == data_type.lower():
                         # Found a substitution
                         target_type = sub[3]
                         # If target_length is provided, use it (e.g. VARCHAR(255))
                         # But typically target_type might be just 'TEXT' or 'BIGINT'
                         # We need to populate basic_ fields.

                         # Determine if target type is substituted
                         result[ordinal_position]['basic_data_type'] = target_type
                         # We might not know precision/scale easily from just the name unless we parse it or it's provided.
                         # For now, we assume the config substitution handles the mapping sufficiently for the migration mapping phase.
                         # The key result is basic_column_type.
                         result[ordinal_position]['basic_column_type'] = target_type
                         substitution_found = True
                         break

                if substitution_found:
                     continue

                query_custom_types = f"""
                    SELECT
                        bt.name AS source_data_type,
                        ut.ident as type_has_identity_property,
                        ut.allownulls as type_nullable,
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
                    type_has_identity_property = custom_type[1]
                    type_nullable = custom_type[2]
                    length = custom_type[3]
                    data_type_precision = custom_type[4]
                    data_type_scale = custom_type[5]

                    basic_character_maximum_length = None
                    if source_data_type in ('univarchar', 'unichar'):
                        source_length = str(int(length / unichar_size))
                        basic_character_maximum_length = int(length / unichar_size)
                    elif source_data_type in ('nvarchar', 'nchar'):
                        source_length = str(int(length / nchar_size))
                        basic_character_maximum_length = int(length / nchar_size)
                    elif source_data_type in ('numeric', 'double precision', 'decimal'):
                        source_length = f"{data_type_precision},{data_type_scale}"
                    else:
                        source_length = str(length)
                        basic_character_maximum_length = length

                    source_data_type_length = f"{source_data_type}({source_length})" if source_length else source_data_type

                    # Convert base type to PostgreSQL equivalent
                    # We need types mapping here
                    types_mapping = self.get_types_mapping(settings)

                    # We need to handle mapped base type
                    # source_data_type is e.g. 'varchar', 'numeric'
                    # types_mapping keys are usually uppercase
                    mapped_type = types_mapping.get(source_data_type.upper(), source_data_type)

                    # If mapped type matches source type (case insensitive), we might still want to uppercase it?
                    # But types_mapping values are usually what we want (e.g. 'VARCHAR', 'NUMERIC')

                    mapped_type_length = f"{mapped_type}({source_length})" if source_length and self.is_string_type(source_data_type) else (f"{mapped_type}({source_length})" if source_length and self.is_numeric_type(source_data_type) else mapped_type)

                    # Actually, logic for length might differ per type (PG TEXT has no length usually, but VARCHAR does)
                    # But here we are producing 'basic_column_type' which serves as fallback.
                    # The original code used source_data_type.

                    # Let's align with how `types_mapping` works.
                    # If mapped_type is TEXT, we might drop length if source was varchar?
                    # The request says: "base data types must be converted into proper PostgreSQL types using conversions returned by get_types_mapping function"

                    # Let's use the same logic as _get_udt_codes_mapping will use.

                    mapped_full_type = mapped_type.upper()
                    if source_length:
                        if self.is_string_type(source_data_type) and mapped_type.upper() not in ('TEXT', 'BYTEA', 'BOOLEAN', 'INTEGER', 'BIGINT', 'SMALLINT', 'DATE', 'TIMESTAMP', 'TIME'):
                             mapped_full_type += f"({source_length})"
                        elif self.is_numeric_type(source_data_type) and mapped_type.upper() in ('NUMERIC', 'DECIMAL'):
                             mapped_full_type += f"({source_length})"

                    ## The basic type of a column is reported as the type of the SOURCE, exactly
                    ## like its data_type and its column_type, because the planner maps it to the
                    ## type of the target itself and a user substitution is written against the
                    ## name of the source as well. Reported as the mapped type it was mapped a
                    ## second time: a UDT over 'datetime' arrived as TIMESTAMP, and TIMESTAMP is
                    ## the name of the row version type of Sybase, so the column of
                    ## 'T_TypLastchange' was created as BYTEA.
                    result[ordinal_position]['basic_data_type'] = source_data_type
                    result[ordinal_position]['basic_character_maximum_length'] = basic_character_maximum_length
                    result[ordinal_position]['basic_numeric_precision'] = data_type_precision if self.is_numeric_type(source_data_type) else None
                    result[ordinal_position]['basic_numeric_scale'] = data_type_scale if self.is_numeric_type(source_data_type) else None
                    result[ordinal_position]['basic_column_type'] = source_data_type_length

            cursor.close()
            self.disconnect()
            return result
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sybase_ase_connector: fetch_table_columns: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_default_values(self, settings) -> dict:
        source_schema_name = settings['source_schema_name']
        query = f"""
            SELECT
                USER_NAME(def_obj.uid) AS DefaultOwner,
                def_obj.name AS DefaultObjectName,
                sc.colid AS DefinitionLineNumber,
                sc.text AS DefaultDefinitionPart
            FROM
                sysobjects def_obj
            JOIN
                syscomments sc ON def_obj.id = sc.id
            WHERE
                def_obj.type = 'D'  -- 'D' signifies a Default object created with CREATE DEFAULT
            ORDER BY
                DefaultObjectName, DefinitionLineNumber
        """
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        default_values = {}
        for row in cursor.fetchall():
            default_owner = row[0]
            default_object_name = row[1]
            definition_line_number = row[2]
            default_definition_part = row[3] or ''
            if default_object_name not in default_values:
                default_values[default_object_name] = {
                    'default_value_schema': default_owner,
                    'default_value_name': default_object_name,
                    'default_value_sql': default_definition_part,
                    'extracted_default_value': '',
                    'default_value_comment': '',
                }
            else:
                # The pieces of syscomments are a byte stream - joining them with a space
                # inserted would break a word which was split between two of them.
                default_values[default_object_name]['default_value_sql'] += default_definition_part
        cursor.close()
        self.disconnect()

        for default_object_name, default_value in default_values.items():
            # The comments of the batch have to go before the text is joined into one line,
            # otherwise a '--' comment swallows the statement behind it.
            # The value of a default object is put into the DDL of every column bound to it,
            # so it has to be translated exactly like the default written in a CREATE TABLE -
            # 'getdate()' becomes 'current_timestamp', the money literal '$1000' becomes 1000.
            default_value['extracted_default_value'] = self.convert_default_value({
                'extracted_default_value': self._extract_default_expression(
                    default_value['default_value_sql'], default_object_name),
                'column_type': '',
            })
            default_value['default_value_sql'] = re.sub(r'\s+', ' ', default_value['default_value_sql']).strip()
        return default_values


    def get_types_mapping(self, settings):
        # Guard against None settings or missing key
        if settings is None:
            settings = {}
        target_db_type = settings.get('target_db_type', 'postgresql')

        types_mapping = {}
        if target_db_type == 'postgresql':
            types_mapping = {
                'BIGDATETIME': 'TIMESTAMP',
                'DATE': 'DATE',
                'DATETIME': 'TIMESTAMP',
                # BIGTIME holds a time of the day with microseconds, not a point in time -
                # migrated as TIMESTAMP it received the date of the migration run.
                'BIGTIME': 'TIME',
                'SMALLDATETIME(4)': 'TIMESTAMP',
                'SMALLDATETIME': 'TIMESTAMP',
                'TIME': 'TIME',
                # The TIMESTAMP of Sybase ASE is not a point in time at all: it is the row
                # version of the table, a VARBINARY(8) the server writes on every change
                # (the ROWVERSION of MS SQL). Its value is binary - migrating it as a
                # PostgreSQL TIMESTAMP ended as
                # 'invalid input syntax for type timestamp: "b'\x00\x00...<\x8e'"'.
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
                'CHAR': 'CHAR',
                'NCHAR': 'CHAR',
                'UNICHAR': 'CHAR',
                'NVARCHAR': 'VARCHAR',
                'UNIVARCHAR': 'VARCHAR',
                'TEXT': 'TEXT',
                'SYSNAME': 'TEXT',
                'LONGSYSNAME': 'TEXT',
                'LONG VARCHAR': 'TEXT',
                'LONG NVARCHAR': 'TEXT',
                'UNITEXT': 'TEXT',
                'VARCHAR': 'VARCHAR',

                'CLOB': 'TEXT',
                'DECIMAL': 'DECIMAL',
                'DOUBLE PRECISION': 'DOUBLE PRECISION',
                'FLOAT': 'FLOAT',
                'INTERVAL': 'INTERVAL',
                # MONEY of Sybase ASE is a fixed point number with four decimal places -
                # MONEY holds up to 922337203685477.5807, SMALLMONEY up to 214748.3647.
                # The MONEY type of PostgreSQL is not the same thing: it has almost no
                # operators (`operator does not exist: money > integer` for a CHECK as
                # ordinary as `VALUE > 0`) and keeps the decimal places of the lc_monetary
                # setting instead of the four of the source. INTEGER, which stood here
                # before, silently dropped the decimal places of every amount.
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

    def _types_mapping_for_mapped_text(self, types_mapping):
        """
        The type mapping without the entries which would map a name the mapping itself
        produces. Applying it to a text which went through the mapping once already leaves
        that text alone, which matters wherever the two dialects use the same word for
        different things: 'datetime' of Sybase becomes TIMESTAMP, and TIMESTAMP is at the
        same time the name of the row version type of Sybase, which becomes BYTEA - mapping
        twice turned every datetime into a BYTEA.
        """
        produced_type_names = {str(value).upper() for value in types_mapping.values()}
        return {name: value for name, value in types_mapping.items()
                if name.upper() not in produced_type_names or name.upper() == str(value).upper()}

    def _apply_types_mapping(self, text, types_mapping):
        """
        Replace the type names of the source with the ones of the target, in ONE pass over
        the text. With one substitution per entry of the mapping, the text written by an
        entry was read again by the next one: 'bigdatetime' became TIMESTAMP and TIMESTAMP
        became BYTEA, because the TIMESTAMP of Sybase is the binary row version and is
        mapped to BYTEA - a parameter declared 'bigdatetime' reached the target as BYTEA.
        """
        if not text or not types_mapping:
            return text

        types_without_length = ('BYTEA', 'TEXT', 'BOOLEAN', 'INTEGER', 'BIGINT', 'SMALLINT', 'DATE', 'TIMESTAMP', 'TIME')
        ## the longest name first, so that 'UNSIGNED BIG INT' is not read as 'INT'
        names = sorted((name for name in types_mapping if '(' not in name), key=len, reverse=True)
        if not names:
            return text
        ## a name written with more than one space between its words is the same name
        alternatives = '|'.join(re.escape(name).replace('\\ ', ' ').replace(' ', r'\s+') for name in names)
        pattern = re.compile(rf'\b({alternatives})\b(\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\))?', re.IGNORECASE)

        def replace(match):
            source_type = re.sub(r'\s+', ' ', match.group(1)).strip().upper()
            pg_type = types_mapping.get(source_type)
            if pg_type is None:
                return match.group(0)
            length = match.group(2)
            if length and '(' not in pg_type and pg_type.upper() not in types_without_length:
                return f"{pg_type}{length}"
            return pg_type

        return pattern.sub(replace, text)

    def _quote_udts_in_declaration(self, decl_content, settings):
        migrator_tables = settings.get('migrator_tables') if settings else None
        if not migrator_tables and hasattr(self, 'migrator_tables'):
            migrator_tables = self.migrator_tables
        
        if not migrator_tables:
            return decl_content

        try:
            udt_rows = migrator_tables.fetch_all_user_defined_types()
            for row in udt_rows:
                decoded = migrator_tables.decode_user_defined_type_row(row)
                src_type = decoded['source_type_name']
                tgt_type = decoded['target_type_name']
                tgt_schema = decoded.get('target_schema_name', '')
                if src_type and tgt_type:
                    replacement = f'"{tgt_schema}"."{tgt_type}"' if tgt_schema else f'"{tgt_type}"'
                    # Target type needs double quotes in the declaration
                    decl_content = re.sub(rf'\b{re.escape(src_type)}\b', replacement, decl_content, flags=re.IGNORECASE)
        except Exception as e:
            if hasattr(self, 'config_parser') and self.config_parser:
                self.config_parser.print_log_message('WARNING', f"sybase_ase_connector: _quote_udts_in_declaration: Failed to apply UDT quotes: {e}")
        
        return decl_content

    def get_create_table_sql(self, settings):
        return ""

    def _get_udt_codes_mapping(self, settings=None):
        """
        Returns a dictionary mapping UDT names to their base SQL definition.
        Example: {'MY_TYPE': 'VARCHAR(10)', 'NUM_TYPE': 'NUMERIC(10,2)'}
        """
        if self._udt_cache is not None:
            return self._udt_cache

        udt_map = {}
        udt_rows = []

        # Priority: Check migrator_tables (Protocol Table)
        migrator_tables = settings.get('migrator_tables') if settings else None

        if migrator_tables:
            try:
                # Assuming migrator_tables has fetch_all_user_defined_types
                udt_rows = migrator_tables.fetch_all_user_defined_types()
                # Format of udt_rows from fetch_all_user_defined_types needs to be adapted or used
                # fetch_all_user_defined_types returns raw rows from migrator protocol table
                # We need to decode or map them.
                # Assuming row format: [id, row_data, status, comment, ...] or similar.
                # Use decode_user_defined_type_row

                # We build udt_map from this list
                for row_data in udt_rows:
                    decoded = migrator_tables.decode_user_defined_type_row(row_data)
                    # format: dict(type_name, base_type, length, prec, scale, ...)
                    # base_type needs to be mapped to PG type

                    type_name = decoded['type_name']
                    base_type = decoded['base_type_name']
                    target_basic_type = decoded.get('target_basic_type')
                    length = decoded['length'] if decoded['length'] else 0
                    prec = decoded['prec'] if decoded['prec'] else 0
                    scale = decoded['scale'] if decoded['scale'] else 0

                    if not base_type: base_type = "UNKNOWN"

                    # Convert base_type to PG type
                    # Priority: target_basic_type (from protocol) > types_mapping(base_type)
                    if target_basic_type:
                         pg_base_type = target_basic_type.upper()
                    else:
                         pg_base_type = base_type.upper()
                         if settings:
                              types_mapping = self.get_types_mapping(settings)
                              pg_base_type = types_mapping.get(base_type.upper(), base_type.upper())

                    type_sql = pg_base_type

                    # Apply length/precision logic (similar to query based loop below)
                    if base_type.lower() in ('varchar', 'char', 'nvarchar', 'nchar', 'varbinary', 'binary', 'univarchar', 'unichar'):
                         if pg_base_type not in ('TEXT', 'BYTEA', 'DATE', 'TIMESTAMP', 'TIME', 'BOOLEAN', 'INTEGER', 'BIGINT', 'SMALLINT'):
                              type_sql += f"({length})"
                    elif base_type.lower() in ('numeric', 'decimal'):
                         if pg_base_type in ('NUMERIC', 'DECIMAL'):
                              type_sql += f"({prec},{scale})"

                    udt_map[type_name] = type_sql

                self._udt_cache = udt_map
                return udt_map

            except Exception as e:
                self.config_parser.print_log_message('WARNING', f"sybase_ase_connector: _get_udt_codes_mapping: Failed to fetch UDTs from protocol table: {e}. Fallback to live query.")


        query = """
            SELECT
                t.name as type_name,
                t.length,
                t.prec,
                t.scale,
                bt.name as base_type_name
            FROM dbo.systypes t
            JOIN dbo.sysusers u ON t.uid = u.uid
            LEFT JOIN dbo.systypes bt ON bt.usertype = (
                /* The base type of a user defined type is found by its type code, and several
                   system types share one: 'varchar' and 'sysname' are both type 39, 'char' and
                   'nchar' are both 47, 'varbinary' and 'timestamp' are both 37. The join took
                   whichever of them the server returned last, so a type over varbinary could be
                   resolved to 'timestamp' - which is the row version type of Sybase and becomes
                   BYTEA - and one over char to 'sysname', which the mapping does not know at
                   all. The lowest usertype of a type code is its canonical type, and it is the
                   one taken here; it also makes the row per user defined type unambiguous. */
                SELECT MIN(b2.usertype) FROM dbo.systypes b2
                WHERE b2.type = t.type AND b2.usertype < 100
            )
            WHERE t.usertype > 100
            ORDER BY t.name
        """

        try:
            should_disconnect = False
            if not self.connection:
                self.connect()
                should_disconnect = True

            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

            for row in rows:
                type_name = row[0]
                length = row[1]
                prec = row[2]
                scale = row[3]
                base_type = row[4]

                if not base_type:
                    base_type = "UNKNOWN"

                # Convert base_type to PG type
                # Check mapping
                pg_base_type = base_type.upper()
                if settings: ## we must have settings to get types mapping
                     types_mapping = self.get_types_mapping(settings)
                     # types_mapping keys are usually uppercase
                     pg_base_type = types_mapping.get(base_type.upper(), base_type.upper())

                type_sql = pg_base_type

                if base_type.lower() in ('varchar', 'char', 'nvarchar', 'nchar', 'varbinary', 'binary', 'univarchar', 'unichar'):
                    # Check if PG type supports length?
                    # If PG type is TEXT, we drop length.
                    if pg_base_type not in ('TEXT', 'BYTEA', 'DATE', 'TIMESTAMP', 'TIME', 'BOOLEAN', 'INTEGER', 'BIGINT', 'SMALLINT'):
                        type_sql += f"({length})"
                elif base_type.lower() in ('numeric', 'decimal'):
                     if pg_base_type in ('NUMERIC', 'DECIMAL'):
                         type_sql += f"({prec},{scale})"

                udt_map[type_name] = type_sql

            cursor.close()
            if should_disconnect:
                self.disconnect()

        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"sybase_ase_connector: _get_udt_codes_mapping: Failed to fetch UDTs for substitution: {e}")
            # If we fail, we just return empty map to not break flow

        self._udt_cache = udt_map
        return udt_map

    def _apply_udt_to_base_type_substitutions(self, text, settings):
        """
        Apply UDT -> Base Type substitutions, BUT respect config substitutions.
        If a UDT is defined in config data_types_substitution, we SKIP it here.
        """
        udt_map = self._get_udt_codes_mapping(settings)
        if not udt_map:
            return text

        # Get config substitutions
        config_substitutions = self.config_parser.get_data_types_substitution()
        # config_substitutions is list of [schema, table, source_type, target_type, comment]
        # We collect source types to ignore

        ignored_types = set()
        if config_substitutions:
            for entry in config_substitutions:
                if len(entry) >= 3 and entry[2]:
                    ignored_types.add(entry[2].upper())

        # Get type mappings for recursive substitution
        types_mapping = self.get_types_mapping(settings)

        # Optimize: Pre-calculate all final definitions and use single regex pass
        self.config_parser.print_log_message('DEBUG', "sybase_ase_connector: _apply_udt_to_base_type_substitutions: Optimizing UDT substitution: preparing map...")
        udt_lookup = {}
        keys_to_match = []

        for udt_name, base_def in udt_map.items():
            if udt_name.upper() in ignored_types:
                continue

            ## _get_udt_codes_mapping already answers with the type of the TARGET
            ## ('T_TypLastchange' -> 'TIMESTAMP'), so only a name which the mapping does not
            ## produce itself may be mapped here - otherwise the TIMESTAMP of a datetime UDT
            ## is read as the row version type of Sybase and becomes BYTEA.
            final_def = self._apply_types_mapping(base_def, self._types_mapping_for_mapped_text(types_mapping))

            # Store in lookup (Upper case key for case-insensitive matching)
            udt_lookup[udt_name.upper()] = final_def
            keys_to_match.append(udt_name)

        if not keys_to_match:
            self.config_parser.print_log_message('DEBUG', "sybase_ase_connector: _apply_udt_to_base_type_substitutions: No UDTs to substitute.")
            return text

        self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: _apply_udt_to_base_type_substitutions: Compiling regex for {len(keys_to_match)} UDTs...")
        # Sort by length desc to handle prefixes/overlaps
        keys_to_match.sort(key=len, reverse=True)

        # Pattern: (?:\[|")?\b(UDT1|UDT2...)\b(?:\]|")?
        # Capturing group 1 contains the UDT name
        escaped_keys = [re.escape(k) for k in keys_to_match]
        pattern_str = r'(?:\[|")?\b(' + '|'.join(escaped_keys) + r')\b(?:\]|")?'

        try:
             regex = re.compile(pattern_str, flags=re.IGNORECASE)
        except re.error as e:
             self.config_parser.print_log_message('WARNING', f"sybase_ase_connector: _apply_udt_to_base_type_substitutions: Failed to compile optimized UDT regex: {e}. Fallback to slow loop.")
             # Fallback logic could be here, but simpler to just return or raise.
             return text

        def replacer(match):
             # match.group(1) is the inner UDT name
             core_name = match.group(1)
             if core_name:
                 return udt_lookup.get(core_name.upper(), match.group(0))
             return match.group(0)

        self.config_parser.print_log_message('DEBUG', "sybase_ase_connector: replacer: Executing UDT substitution...")
        text = regex.sub(replacer, text)
        self.config_parser.print_log_message('DEBUG', "sybase_ase_connector: replacer: UDT substitution complete.")

        return text


    def _apply_data_type_substitutions(self, text):
        """
        Apply data type substitutions defined in the configuration.
        Substitutions are applied based on table name (optional), column name (optional),
        and source data type (regex).
        In the context of functions/procedures/triggers, we mainly care about the source data type matching.
        """
        substitutions = self.config_parser.get_data_types_substitution()
        if not substitutions:
            return text

        # Sort substitutions by length of source type (descending) to match specific types first?
        # Or just rely on config order. Config order is probably best.

        for entry in substitutions:
            # entry: [table_name, column_name, source_type, target_type, comment]
            if len(entry) != 5:
                continue

            # For general code substitution, we ignore table_name and column_name usually,
            # or treat them as wildcards if they are empty.
            # However, for function params/vars, we assume no table/column context matches unless explicitly handled.
            # But the requirement is likely to map generic types like 'TypID' -> 'BIGINT'.
            # So we look for entries where source_type is defined.

            # We are not passed table/column context here easily for params/vars unless we parse them deeply.
            # So we focus on source_type match.

            source_type = entry[2]
            target_type = entry[3]

            if source_type:
                # Use regex or simple replace? Config says regex.
                # Use word boundaries to avoid partial replacement.
                try:
                    pattern = re.compile(rf'\b{source_type}\b', flags=re.IGNORECASE)
                    text = pattern.sub(target_type, text)
                except re.error:
                    self.config_parser.print_log_message('WARNING', f"sybase_ase_connector: _apply_data_type_substitutions: Invalid regex in data_types_substitution: {source_type}")

        return text


    def is_string_type(self, column_type: str) -> bool:
        string_types = ['CHAR', 'VARCHAR', 'NCHAR', 'NVARCHAR', 'TEXT', 'LONG VARCHAR', 'LONG NVARCHAR', 'UNICHAR', 'UNIVARCHAR']
        return column_type.upper() in string_types

    def is_numeric_type(self, column_type: str) -> bool:
        numeric_types = ['BIGINT', 'INTEGER', 'INT', 'TINYINT', 'SMALLINT', 'FLOAT', 'DOUBLE PRECISION', 'DECIMAL', 'NUMERIC']
        return column_type.upper() in numeric_types

    ## ---------------------------------------------------------------- computed columns

    def _convert_convert_calls(self, sql_text, description=''):
        """
        CONVERT(<type>, <expression>) of Sybase is the CAST of PostgreSQL. The three
        argument form CONVERT(<type>, <expression>, <style>) formats a date or a number
        according to the style number; a CAST does not do that, so such a call is left as
        it stands and reported - a value formatted differently is worse than a statement
        which fails and says so.
        """
        if not sql_text or 'convert' not in str(sql_text).lower():
            return sql_text

        types_mapping = self.get_types_mapping({'target_db_type': 'postgresql'})
        text = str(sql_text)
        result = []
        position = 0
        pattern = re.compile(r'(?i)(?<![A-Za-z0-9_$@#])CONVERT\s*\(')
        while True:
            match = pattern.search(text, position)
            if not match:
                result.append(text[position:])
                break
            open_index = match.end() - 1
            close_index = self._matching_parenthesis(text, open_index)
            if close_index is None:
                result.append(text[position:])
                break

            arguments = self._split_respecting_parens(text[open_index + 1:close_index])
            replacement = None
            if len(arguments) == 2:
                target_type = self._map_convert_target_type(arguments[0], types_mapping)
                if target_type:
                    # the expression can hold a CONVERT of its own
                    replacement = f"CAST({self._convert_convert_calls(arguments[1], description)} AS {target_type})"
            if replacement is None:
                self.config_parser.print_log_message('WARNING',
                    f"sybase_ase_connector: _convert_convert_calls: CONVERT({', '.join(arguments)}) of {description or 'the expression'} is not migrated - "
                    f"{'the style argument of the three argument form has no equivalent in a CAST' if len(arguments) > 2 else 'its target type is not known'}. "
                    f"The statement using it has to be completed by hand.")
                replacement = text[match.start():close_index + 1]

            result.append(text[position:match.start()])
            result.append(replacement)
            position = close_index + 1
        return ''.join(result)

    @staticmethod
    def _matching_parenthesis(text, open_index):
        """ Index of the ')' closing the '(' at open_index, string literals excluded. """
        depth = 0
        in_literal = False
        index = open_index
        while index < len(text):
            character = text[index]
            if in_literal:
                if character == "'":
                    in_literal = False
            elif character == "'":
                in_literal = True
            elif character == '(':
                depth += 1
            elif character == ')':
                depth -= 1
                if depth == 0:
                    return index
            index += 1
        return None

    def _map_convert_target_type(self, type_text, types_mapping):
        """ The first argument of CONVERT written as a PostgreSQL type, or None. """
        text = str(type_text).strip().strip('"').strip()
        match = re.fullmatch(r'(?is)([A-Za-z][A-Za-z0-9_ ]*?)\s*(\(\s*[\dA-Za-z, ]*\s*\))?', text)
        if not match:
            return None
        base_type = re.sub(r'\s+', ' ', match.group(1)).strip().upper()
        length = (match.group(2) or '').strip()
        mapped_type = types_mapping.get(base_type)
        if not mapped_type:
            return None
        # MONEY becomes NUMERIC(19,4) - a length of the source must not be appended to it
        if length and '(' not in mapped_type:
            return f"{mapped_type}{length}"
        return mapped_type

    def _convert_computed_column_expression(self, generation_expression, description=''):
        """
        The definition of a computed column as syscomments keeps it - 'AS <expression>
        MATERIALIZED' - reduced to the expression itself and translated to PostgreSQL.
        The keywords have to be removed as whole words: 'AS' also stands inside the
        expression (CAST(x AS int)) and MATERIALIZED is written in either case.
        """
        if not generation_expression:
            return ''
        text = self.strip_sql_comments(str(generation_expression))
        text = re.sub(r'(?is)^\s*AS\s+', '', text)
        text = re.sub(r'(?is)\s+(?:NOT\s+)?MATERIALIZED\s*$', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        if not text:
            return ''
        text = self._convert_convert_calls(text, description)
        text = self.apply_sql_functions_mapping(text, {'target_db_type': 'postgresql'})
        text = self._convert_money_literals(text)
        return text.strip()

    def _computed_column_expressions(self, cursor, source_table_id):
        """
        The computed columns of a table with their expression and whether they are hidden.
        A hidden one is the key of a functional index (sybfi5_1): it is not a column of
        the source table, only the value the index is built on, so it is not migrated as a
        column - the index which uses it becomes an index over the expression instead.
        """
        computed_columns = {}
        query = f"""
            SELECT c.name, case when c.status3 & 1 = 1 then 1 else 0 end as is_hidden, co.colid, co.text
            FROM syscolumns c
            LEFT JOIN syscomments co ON c.computedcol = co.id
            WHERE c.id = {source_table_id} AND c.computedcol IS NOT NULL AND c.computedcol > 0
            ORDER BY c.colid, co.colid
        """
        try:
            cursor.execute(query)
            text_pieces = {}
            for row in cursor.fetchall():
                column_name = str(row[0]).strip()
                computed_columns.setdefault(column_name.lower(), {
                    'column_name': column_name,
                    'is_hidden': row[1] == 1,
                    'expression': '',
                })
                if row[3] is not None and row[2] is not None:
                    text_pieces.setdefault(column_name.lower(), {})[row[2]] = row[3]
            for name, pieces in text_pieces.items():
                computed_columns[name]['expression'] = self._convert_computed_column_expression(
                    self._joined_text_pieces(pieces), f"computed column {computed_columns[name]['column_name']}")
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"sybase_ase_connector: _computed_column_expressions: Could not read the computed columns of the table ({e}) - an index over one of them cannot be migrated.")
        return computed_columns

    def _index_columns_with_expressions(self, index_columns, computed_columns, index_name):
        """
        Replace every key of the index which is a hidden computed column with the
        expression that column holds. Returns (column list, function based, name of a
        hidden column whose expression is missing).
        """
        converted_keys = []
        is_function_based = False
        for key in self._split_respecting_parens(index_columns):
            key = key.strip()
            column = computed_columns.get(key.strip('"').strip().lower())
            if column and column['is_hidden']:
                if not column['expression']:
                    return index_columns, False, column['column_name']
                converted_keys.append(f"({column['expression']})")
                is_function_based = True
                self.config_parser.print_log_message('DEBUG',
                    f"sybase_ase_connector: fetch_indexes: Index {index_name} is built on the hidden computed column {column['column_name']} "
                    f"- migrated as an index over its expression {column['expression']}.")
            else:
                converted_keys.append(key)
        return ', '.join(converted_keys), is_function_based, None

    def fetch_indexes(self, settings):
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

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

            # A functional index of Sybase is an index over a hidden computed column
            # (sybfi5_1) which holds the value of its expression. That column is not
            # migrated - it is not a column of the table - so the index has to be created
            # over the expression itself.
            computed_columns = self._computed_column_expressions(cursor, source_table_id)

            for index in indexes:
                self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: fetch_indexes: Processing index: {index}")
                index_name = index[0].strip()
                index_unique = index[1]  ## integer 0 or 1
                index_columns = index[2].strip()
                index_primary_key = index[3]
                index_owner = ''

                index_columns, is_function_based, missing_expression = self._index_columns_with_expressions(
                    index_columns, computed_columns, index_name)
                if missing_expression:
                    self.config_parser.print_log_message('WARNING',
                        f"sybase_ase_connector: fetch_indexes: Index {index_name} of {source_table_schema}.{source_table_name} is built on the hidden computed column "
                        f"{missing_expression}, whose expression could not be read - the index is not migrated and has to be recreated by hand.")
                    continue

                table_indexes[order_num] = {
                    'index_name': index_name,
                    'index_type': "PRIMARY KEY" if index_primary_key == 1 else "UNIQUE" if index_unique == 1 and index_primary_key == 0 else "INDEX",
                    'index_owner': index_owner,
                    'index_columns': index_columns,
                    'index_comment': '',
                    'is_function_based': 'YES' if is_function_based else 'NO',
                }
                order_num += 1

            cursor.close()
            self.disconnect()
            return table_indexes

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sybase_ase_connector: fetch_indexes: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_index_sql(self, settings):
        return ""

    def fetch_constraints(self, settings):
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

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
            user_name(oc.uid) as ref_table_schema,
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
        AND c.status & 64 = 64
        ORDER BY constraint_name
        """
        ## status & 64 = 64 - foreign key constraint (0x0040)
        self.connect()
        cursor = self.connection.cursor()
        self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: fetch_constraints: Reading constraints for {source_table_name}")
        cursor.execute(index_query)
        constraints = cursor.fetchall()

        for constraint in constraints:
            fk_name = constraint[0]
            fk_column = constraint[1].strip()
            ref_table_schema = constraint[2]
            ref_table_name = constraint[3]
            ref_column = constraint[4].strip()

            table_constraints[order_num] = {
                'constraint_name': fk_name,
                'constraint_owner': source_table_schema,
                'constraint_type': 'FOREIGN KEY',
                'constraint_columns': fk_column,
                'referenced_table_schema': ref_table_schema,
                'referenced_table_name': ref_table_name,
                'referenced_columns': ref_column,
                'constraint_sql': '',
                'constraint_comment': ''
            }
            order_num += 1

        # get check constraints
        check_query = f"""
            SELECT
                o.name AS ConstraintName,
                s_check.text AS CheckConstraintDefinition -- For check constraints
            FROM
                sysconstraints c
            JOIN
                sysobjects o ON c.constrid = o.id
            LEFT JOIN
                syscomments s_check ON o.id = s_check.id
            WHERE c.status & 128 = 128
            AND c.tableid = {source_table_id}
        """
        ## status & 128 = 128 - check constraint (0x0080)
        cursor.execute(check_query)
        check_constraints = cursor.fetchall()
        for check_constraint in check_constraints:
            check_name = check_constraint[0]
            check_expression = check_constraint[1].strip()
            check_expression = check_expression.replace('CONSTRAINT', '').replace(check_name, '').replace('CHECK','').strip()
            check_expression = self._convert_money_literals(check_expression)
            table_constraints[order_num] = {
                'constraint_name': check_name,
                'constraint_type': 'CHECK',
                'constraint_sql': check_expression,
                'constraint_comment': ''
            }
            order_num += 1

        cursor.close()
        self.disconnect()
        return table_constraints

    def get_create_constraint_sql(self, settings):
        return ""

    def get_aliases(self, settings):
        return {}

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
                    WHEN o.type = 'XP' THEN 'Extended Procedure'
                END AS type,
                o.sysstat
            FROM syscomments c, sysobjects o
            WHERE o.id=c.id
                AND user_name(o.uid) = '{schema}'
                AND type in ('F', 'P', 'XP')
                AND (o.sysstat & 4 = 4 or o.sysstat & 10 = 10 or o.sysstat & 12 = 12)
            ORDER BY o.name
        """
        self.config_parser.print_log_message('DEBUG3', f"sybase_ase_connector: fetch_funcproc_names: Fetching function/procedure names for schema {schema}")
        self.config_parser.print_log_message('DEBUG3', f"sybase_ase_connector: fetch_funcproc_names: Query: {query}")
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            funcproc_data[order_num] = {
                'name': row[0],
                'id': row[1],
                'type': row[2],
                'sysstat': row[3],
                'comment': ''
            }
            order_num += 1
        cursor.close()
        self.disconnect()
        return funcproc_data

    def fetch_funcproc_code(self, funcproc_id: int):
        """
        Fetches the code of a function or procedure by its ID. General query:

            SELECT u.name as owner, o.name as proc_name, c.colid as line_num, c.text as source_code
            FROM sysusers u, syscomments c, sysobjects o
            WHERE o.type = 'P' AND o.id = c.id AND o.uid = u.uid
            ORDER BY o.id, c.colid
        """
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
        procbody_str = ''.join([body[0] for body in procbody])
        return procbody_str


    def fetch_user_messages(self):
        """
        The messages of the source, keyed by their number as text. RAISERROR of Sybase
        names a message by its number - 'raiserror 20002, @sku' - and its text lives in
        sysusermessages, put there with sp_addmessage. Without it the converted routine
        can only report the number, so the table is read once and kept.
        """
        if self._user_messages is not None:
            return self._user_messages

        messages = {}
        opened_here = self.connection is None
        try:
            if opened_here:
                self.connect()
            cursor = self.connection.cursor()
            cursor.execute("SELECT error, description FROM sysusermessages ORDER BY error, langid")
            for row in cursor.fetchall():
                number = str(row[0]).strip()
                text = row[1]
                ## the first language of a message is the one it was created with
                if text and number not in messages:
                    messages[number] = str(text).strip()
            cursor.close()
            self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: fetch_user_messages: {len(messages)} messages of sysusermessages read.")
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"sybase_ase_connector: fetch_user_messages: The messages of the source could not be read ({e}) - a RAISERROR naming a message by its number reports the number instead of its text.")
        finally:
            if opened_here:
                self.disconnect()

        self._user_messages = messages
        return messages

    ## ---------------------------------------------------------------- procedure groups

    ## 'create procedure p_report;2' - the header of a member of a procedure group
    PROCEDURE_HEADER_PATTERN = re.compile(
        r'(?im)^[ \t]*CREATE\s+(?:OR\s+REPLACE\s+)?PROC(?:EDURE)?\s+'
        r'((?:[A-Za-z0-9_#$]+|"[^"]+"|\[[^\]]+\])(?:\s*\.\s*(?:[A-Za-z0-9_#$]+|"[^"]+"|\[[^\]]+\]))*)'
        r'[ \t]*(;[ \t]*(\d+))?')

    @staticmethod
    def _mask_comments(text):
        """ The text with the content of its comments replaced by spaces - same length, so
        that a position found in it is a position of the original. """
        masked = list(text)
        index = 0
        length = len(text)
        quote = None
        while index < length:
            character = text[index]
            if quote:
                if character == quote:
                    quote = None
                index += 1
                continue
            if character in ("'", '"'):
                quote = character
                index += 1
                continue
            if text.startswith('--', index):
                while index < length and text[index] != '\n':
                    masked[index] = ' '
                    index += 1
                continue
            if text.startswith('/*', index):
                end_of_comment = text.find('*/', index + 2)
                end_of_comment = length if end_of_comment == -1 else end_of_comment + 2
                while index < end_of_comment:
                    if text[index] != '\n':
                        masked[index] = ' '
                    index += 1
                continue
            index += 1
        return ''.join(masked)

    def _split_procedure_group(self, funcproc_code, funcproc_name):
        """
        A procedure group of Sybase ASE is a set of procedures sharing one name, told apart
        by a number - 'create procedure p_report;1', 'create procedure p_report;2'. The
        catalog knows one object for the whole group and keeps the text of every member in
        it, which is what the migrator reads: one routine whose code holds two CREATE
        statements, and a name the header of PostgreSQL cannot carry ('p_report"(;2').

        PostgreSQL has no such thing, so every member becomes a routine of its own: the
        member 1, which 'exec p_report' calls, keeps the name of the group, every other one
        gets the number appended ('p_report_2').

        Returns the list of (name, code) of the members, or an empty list when the code is
        not a group at all.
        """
        if not funcproc_code:
            return []

        masked_code = self._mask_comments(funcproc_code)
        headers = list(self.PROCEDURE_HEADER_PATTERN.finditer(masked_code))
        if not headers:
            return []
        if len(headers) == 1 and not headers[0].group(2):
            return []

        base_name = funcproc_name or headers[0].group(1)
        base_name = base_name.split('.')[-1].strip().strip('"').strip('[]')

        members = []
        for position, header in enumerate(headers):
            start = 0 if position == 0 else header.start()
            end = headers[position + 1].start() if position + 1 < len(headers) else len(funcproc_code)
            member_code = funcproc_code[start:end]
            number = header.group(3)
            member_name = base_name if number in (None, '1') else f"{base_name}_{number}"

            ## the number belongs to the name of the source and cannot stand in the header of
            ## the target - it is written into the name instead
            if header.group(2):
                member_code = (funcproc_code[start:header.start(1)]
                               + member_name
                               + funcproc_code[header.end(2):end])
            members.append((member_name, member_code))

        return members

    @staticmethod
    def _split_off_routine_options(text):
        """
        The options of a routine stand between its parameters and the AS of its body -
        'create procedure p @a int, @b int output with recompile as ...'. Split them off:
        they are no parameters, and none of them has a counterpart in PostgreSQL.
        Returns (parameters, options).
        """
        quote = None
        depth = 0
        index = 0
        while index < len(text):
            character = text[index]
            if quote:
                if character == quote:
                    quote = None
            elif character in ("'", '"'):
                quote = character
            elif character == '(':
                depth += 1
            elif character == ')':
                depth -= 1
            elif depth == 0 and character in ('w', 'W') and re.match(r'(?i)WITH\b', text[index:]):
                if index == 0 or not (text[index - 1].isalnum() or text[index - 1] == '_'):
                    return text[:index].strip(), text[index:].strip()
            index += 1
        return text.strip(), ''

    def convert_funcproc_code(self, settings, is_group_member=False):
        try:
            funcproc_code_input = settings['funcproc_code']
            
            if isinstance(funcproc_code_input, dict):
                funcproc_code = funcproc_code_input.get('definition', '')
                implicit_return_schema = funcproc_code_input.get('return_schema', [])
            else:
                funcproc_code = funcproc_code_input
                implicit_return_schema = []

            ## A procedure group of Sybase is one object of the catalog holding several
            ## CREATE statements. Every member becomes a routine of its own - see
            ## _split_procedure_group - and the statements are returned together, the way
            ## the source keeps them together.
            if not is_group_member:
                group_members = self._split_procedure_group(funcproc_code, settings.get('funcproc_name'))
                if group_members:
                    self.config_parser.print_log_message('WARNING',
                        f"sybase_ase_connector: convert_funcproc_code: {settings.get('funcproc_name')} is a procedure group of "
                        f"{len(group_members)} members, which PostgreSQL does not have - every member is migrated as a routine of "
                        f"its own: {', '.join(name for name, _ in group_members)}. A caller writing 'exec {settings.get('funcproc_name')};<n>' "
                        f"has to name the routine of that member.")
                    converted_members = []
                    for member_name, member_code in group_members:
                        member_settings = dict(settings)
                        member_settings['funcproc_code'] = member_code
                        member_settings['funcproc_name'] = member_name
                        converted_member = self.convert_funcproc_code(member_settings, is_group_member=True)
                        if converted_member and converted_member.strip():
                            converted_members.append(converted_member.strip())
                        else:
                            self.config_parser.print_log_message('WARNING',
                                f"sybase_ase_connector: convert_funcproc_code: The member {member_name} of the procedure group "
                                f"{settings.get('funcproc_name')} could not be converted - it is missing in the target.")
                    return "\n\n".join(converted_members) + "\n" if converted_members else ''

            # Convert double-quoted string literals to single-quoted strings
            # Sybase often allows "string" where PostgreSQL expects 'string' (which would otherwise parse as an identifier)
            def replacer_dq(m):
                inner = m.group(1)
                inner = inner.replace("'", "''")
                return f"'{inner}'"

            funcproc_code = re.sub(r'"([^"]*)"', replacer_dq, funcproc_code)

            ## The global variables of Sybase ASE have to go before the code is parsed - one of
            ## them left in place is a syntax error of the target and the routine cannot be
            ## created at all.
            funcproc_code, global_variable_declarations = self.convert_sybase_global_variables(
                funcproc_code, settings.get('funcproc_name', ''))

            target_db_type = settings.get('target_db_type', 'postgresql')
            local_settings = settings.copy() if settings else {}
            local_settings['target_db_type'] = target_db_type
            types_mapping = self.get_types_mapping(local_settings)

            funcproc_code = self._apply_types_mapping(funcproc_code, types_mapping)

            ## The whole code - the header with it - was just mapped, so everything read out of
            ## it afterwards carries the type names of the TARGET already. Mapping such a name a
            ## second time changes it again when the two dialects use the same word for
            ## different things: 'bigdatetime' became TIMESTAMP here, and TIMESTAMP is the name
            ## of the row version type of Sybase, which becomes BYTEA - the parameter reached
            ## the target as BYTEA. The names the mapping itself produces are therefore left
            ## alone in every later pass over that text.
            header_types_mapping = self._types_mapping_for_mapped_text(types_mapping)

            ## A routine which declares an output parameter answers through that parameter, and
            ## PostgreSQL refuses a function which has both a RETURNS TABLE and one of them, so
            ## a result set is not looked for in such a routine at all - looking for one turned
            ## the SELECT of an 'INSERT ... SELECT' into a RETURN QUERY of a set the routine
            ## never returned.
            header_parameters = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:PROC|PROCEDURE|FUNCTION)\s+[a-zA-Z0-9_\.]+(.*?)\bAS\b',
                                          funcproc_code, flags=re.IGNORECASE | re.DOTALL)
            declares_output_parameter = bool(header_parameters
                                             and re.search(r'(?i)\b(?:OUT|OUTPUT)\b', header_parameters.group(1)))

            if not implicit_return_schema and not declares_output_parameter:
                temp_parser = TsqlParser(funcproc_code, self.config_parser, view_converter=self.convert_view_code, settings=settings, functions_mapping_converter=self.apply_sql_functions_mapping)
                extracted_schema = temp_parser.extract_implicit_return_schema()
                if extracted_schema:
                    implicit_return_schema = extracted_schema
                    self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: convert_funcproc_code: Dynamically inferred return schema: {implicit_return_schema}")
            elif declares_output_parameter:
                self.config_parser.print_log_message('DEBUG',
                    f"sybase_ase_connector: convert_funcproc_code: {settings.get('funcproc_name')} declares an output "
                    "parameter - its result is that parameter, so no result set is inferred for it.")

            is_implicit_return = bool(implicit_return_schema)
            ## the text of a message a RAISERROR names by its number
            settings['user_messages'] = self.fetch_user_messages()
            parser = TsqlParser(funcproc_code, self.config_parser, implicit_return=is_implicit_return, view_converter=self.convert_view_code, settings=settings, functions_mapping_converter=self.apply_sql_functions_mapping)
            self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: convert_funcproc_code: Running 12-pass parser for {settings.get('funcproc_name')}")

            final_output = parser.run()

            # Reconstruct header string to parse parameters
            header_str = "\n".join(l.content for l in parser.header_lines)

            ## Sybase ASE 16 knows 'CREATE OR REPLACE PROCEDURE' - without the clause the header
            ## did not match at all, so the routine was created without its parameters while its
            ## body used them ('sp_changelog_delete()' with locvar_row_id inside)
            header_match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:PROC|PROCEDURE|FUNCTION)\s+([a-zA-Z0-9_\.]+)(.*?)(\bAS\b)', header_str, flags=re.IGNORECASE | re.DOTALL)

            func_schema = ""
            proc_name = settings.get('funcproc_name', '')
            params_str = ""

            if header_match:
                 full_name = header_match.group(1)
                 params_str = header_match.group(2).strip()
                 if '.' in full_name:
                     parts = full_name.split('.')
                     func_schema = parts[0]
                     if not proc_name: proc_name = parts[1]
                 else:
                     if not proc_name: proc_name = full_name

            if not func_schema:
                 func_schema = settings.get('target_schema_name', 'public')

            pg_params_str = ""
            output_params = []
            explicit_func_return = None
            if params_str:
                 clean_params = params_str.strip()
                 clean_params = re.sub(r'/\*.*?\*/', '', clean_params, flags=re.DOTALL).strip()

                 explicit_func_return_match = re.search(r'\bRETURNS\s+([a-zA-Z0-9_]+(?:\s*\([^)]*\))?)', clean_params, flags=re.IGNORECASE)
                 if explicit_func_return_match:
                     explicit_func_return = explicit_func_return_match.group(1).strip()
                     explicit_func_return = self._apply_data_type_substitutions(explicit_func_return)
                     explicit_func_return = self._apply_udt_to_base_type_substitutions(explicit_func_return, settings)
                     explicit_func_return = self._apply_types_mapping(explicit_func_return, header_types_mapping)
                     
                     clean_params = clean_params[:explicit_func_return_match.start()] + clean_params[explicit_func_return_match.end():]
                     clean_params = clean_params.strip()

                 while clean_params.startswith('(') and clean_params.endswith(')'):
                     clean_params = clean_params[1:-1].strip()
                     clean_params = re.sub(r'/\*.*?\*/', '', clean_params, flags=re.DOTALL).strip()

                 ## 'with recompile' and the other options of the routine are written behind
                 ## its parameters, and they were read as a part of the last one: the
                 ## parameter list ended as 'locvar_deleted INTEGER output with recompile',
                 ## where the OUTPUT was no longer at its end and stayed in the DDL, which
                 ## PostgreSQL answered with 'syntax error at or near "output"'.
                 clean_params, routine_options = self._split_off_routine_options(clean_params)
                 if routine_options:
                     if re.fullmatch(r'(?is)WITH\s+RECOMPILE\s*', routine_options):
                         ## how a plan is cached is not part of the routine in PostgreSQL
                         self.config_parser.print_log_message('DEBUG',
                             f"sybase_ase_connector: convert_funcproc_code: {proc_name}: '{routine_options}' is not migrated - PostgreSQL decides by itself when it replans a statement.")
                     else:
                         self.config_parser.print_log_message('WARNING',
                             f"sybase_ase_connector: convert_funcproc_code: {proc_name}: the options '{routine_options}' of the routine have no counterpart in PostgreSQL and are not migrated. Check whether the routine needs them - 'execute as' in particular decides with whose rights it runs (SECURITY DEFINER).")

                 clean_params = clean_params.replace('@', '')
                 clean_params = self._apply_data_type_substitutions(clean_params)
                 clean_params = self._apply_udt_to_base_type_substitutions(clean_params, settings)

                 param_parts = self._split_respecting_parens(clean_params)
                 processed_params = []

                 for p in param_parts:
                     p_clean = p.strip()

                     ## Sybase writes the mode of a parameter behind its type and accepts both
                     ## words for it ('@id int output', '@id numeric(19,0) out'), while
                     ## PostgreSQL writes the mode in front of the name. Only OUTPUT was read,
                     ## so the OUT of '@event_queue_id numeric(19, 0) out' stayed where it stood
                     ## and PostgreSQL answered 'syntax error at or near "out"'. A parameter of
                     ## Sybase declared this way is passed by reference and carries its incoming
                     ## value into the routine, which is INOUT here.
                     mode_match = re.search(r'(?i)\b(?:OUTPUT|OUT)\b\s*$', p_clean)
                     if mode_match is None:
                         ## some routines write the mode in front of the default value
                         mode_match = re.search(r'(?i)\b(?:OUTPUT|OUT)\b(?=\s*=)', p_clean)
                     if mode_match:
                         p_clean = (p_clean[:mode_match.start()] + p_clean[mode_match.end():]).strip()
                         p_clean = "INOUT " + p_clean

                     p_clean = self._apply_types_mapping(p_clean, header_types_mapping)

                     ## The default of a bit parameter is written as 0 or 1 by Sybase, and the
                     ## type became boolean here - PostgreSQL answers 'argument of DEFAULT must
                     ## be type boolean, not type integer'.
                     p_clean = re.sub(r'(?i)\b(BOOLEAN)(\s*(?:=|DEFAULT)\s*)0\b', r'\1\2false', p_clean)
                     p_clean = re.sub(r'(?i)\b(BOOLEAN)(\s*(?:=|DEFAULT)\s*)1\b', r'\1\2true', p_clean)

                     processed_params.append(p_clean)

                 ## PostgreSQL takes every parameter behind one carrying a default for an input
                 ## parameter and refuses the list with 'input parameters after one with a
                 ## default value must also have defaults'. Sybase has no such rule and writes
                 ## its output parameters where it likes, so an output parameter behind a default
                 ## is given one of its own - the callers pass it, and NULL is what it holds
                 ## before the routine writes it.
                 default_seen = False
                 for index, p_clean in enumerate(processed_params):
                     if re.search(r'(?i)(=|\bDEFAULT\b)', p_clean):
                         default_seen = True
                     elif default_seen:
                         processed_params[index] = p_clean + " DEFAULT NULL"

                 pg_params_str = ", ".join(processed_params)
                 output_params = re.findall(r'\b(?:INOUT|OUT)\b', pg_params_str, flags=re.IGNORECASE)

            # Detect explicit RETURN <value> in the function body
            has_explicit_return_value = False
            for line_obj in final_output:
                stripped = line_obj.content.strip()
                if re.match(r'^RETURN\s+[^;]+|^RETURN\s*\(', stripped, flags=re.IGNORECASE):
                    if not re.match(r'^RETURN\s*;?$', stripped, flags=re.IGNORECASE):
                        has_explicit_return_value = True
                        break

            ## A procedure of Sybase answers with a status code and with its output parameters at
            ## the same time - 'return 0' for the run which worked and 'return -1' for the one
            ## which did not, next to the '@id output' it wrote. A function of PostgreSQL answers
            ## with one thing, and it refuses the two next to each other outright: 'RETURN cannot
            ## have a parameter in function with OUT parameters'. The status becomes an output
            ## parameter of its own, so the routine answers with the record of both and no caller
            ## loses the value it read.
            status_parameter = None
            if output_params and has_explicit_return_value:
                 status_parameter = 'locvar_sybase_status'
                 pg_params_str += f", OUT {status_parameter} INTEGER"
                 output_params.append('OUT')
                 self.config_parser.print_log_message('DEBUG',
                     f"sybase_ase_connector: convert_funcproc_code: {proc_name} returns a status code next to its output "
                     f"parameter(s) - the status is returned as the additional output parameter {status_parameter}.")

            returns_clause = "RETURNS void"
            convert_to_scalar_return = False
            returns_dataset = False

            if explicit_func_return:
                 returns_clause = f"RETURNS {explicit_func_return}"
            elif output_params:
                 ## The result of a routine with output parameters is the row of those
                 ## parameters, which PostgreSQL builds itself. A RETURNS TABLE next to them is
                 ## refused outright - 'OUT and INOUT arguments aren't allowed in TABLE
                 ## functions' - so the output parameters are what the routine returns.
                 if len(output_params) > 1:
                      returns_clause = "RETURNS record"
                 else:
                      single_out = re.search(r'(?i)\b(?:INOUT|OUT)\s+[a-zA-Z0-9_]+\s+([a-zA-Z0-9_]+(?:\s*\([^)]*\))?)', pg_params_str)
                      returns_clause = f"RETURNS {single_out.group(1)}" if single_out else "RETURNS record"
            elif is_implicit_return:
                 if has_explicit_return_value and len(implicit_return_schema) == 1:
                      # If a function mixes RETURN and SELECT, and returns 1 column, force it to be a scalar return
                      col = implicit_return_schema[0]
                      c_type = col.get('system_type_name', 'text')
                      t_mapped = self._apply_data_type_substitutions(c_type)
                      t_mapped = self._apply_udt_to_base_type_substitutions(t_mapped, settings)
                      t_mapped = self._apply_types_mapping(t_mapped, types_mapping)
                      returns_clause = f"RETURNS {t_mapped}"
                      convert_to_scalar_return = True
                 else:
                      col_defs = []
                      for col in implicit_return_schema:
                           c_name = col['name']
                           c_type = col.get('system_type_name', 'text')
                           t_mapped = self._apply_data_type_substitutions(c_type)
                           t_mapped = self._apply_udt_to_base_type_substitutions(t_mapped, settings)
                           t_mapped = self._apply_types_mapping(t_mapped, types_mapping)
                           col_defs.append(f'"{c_name}" {t_mapped}')
                      if col_defs:
                           returns_clause = f"RETURNS TABLE ({', '.join(col_defs)})"
                           returns_dataset = True
            elif has_explicit_return_value:
                 returns_clause = "RETURNS integer"

            # Now we generate the PostgreSQL DDL string using the parsed output array
            # and append it with appropriate indentations

            # The pg header string formatted just like TsqlParser outputs:
            pg_header_str = f'CREATE OR REPLACE FUNCTION "{func_schema}"."{proc_name}"({pg_params_str})\n{returns_clause} AS'

            ## the variables which stand for the global variables of Sybase belong to the routine
            ## just as its own do, and are declared in front of them
            for declaration in reversed(global_variable_declarations):
                if not any(declaration.split()[0] in variable['content'] for variable in parser.variables):
                    parser.variables.insert(0, {"line": 0, "content": declaration})

            # Re-run pass_11 with the customized header to let the parser cleanly merge it
            final_output = parser.pass_11_assemble_output(pg_header_str)
            
            if status_parameter:
                 ## the status the routine returned is written to the parameter which carries it
                 for line_obj in final_output:
                      match = re.match(r'(?i)^(\s*)RETURN\s+(?!QUERY\b|NEXT\b)([^;]+?)\s*;?\s*$', line_obj.content)
                      if match:
                           line_obj.content = f"{match.group(1)}{status_parameter} := {match.group(2)}; RETURN;"

            if returns_dataset:
                 ## The routine answers with the rows of its SELECT, and a 'return @@rowcount'
                 ## next to that is the status code of Sybase - which PostgreSQL refuses outright
                 ## in a function returning a set: 'RETURN cannot have a parameter in function
                 ## returning set'. The statement is kept as a comment instead of being dropped,
                 ## so whoever reads the routine sees what the source returned there, and the
                 ## rows are what the function answers with.
                 for line_obj in final_output:
                      match = re.match(r'(?i)^(\s*)RETURN\s+(?!QUERY\b|NEXT\b)(.+?)\s*;?\s*$', line_obj.content)
                      if match:
                           line_obj.content = f"{match.group(1)}/* RETURN {match.group(2)}; -- Sybase ASE construct which cannot be used in PostgreSQL */"
                           self.config_parser.print_log_message('WARNING',
                               f"sybase_ase_connector: convert_funcproc_code: {proc_name} returns rows and a status code "
                               f"at the same time - the status code 'RETURN {match.group(2)}' cannot be returned next to "
                               f"the rows and is commented out in the generated routine.")

            if convert_to_scalar_return:
                 for line_obj in final_output:
                      content = line_obj.content
                      # If it's a simple variable or column
                      if re.match(r'(?i)^(\s*)RETURN\s+QUERY\s+SELECT\s+([a-zA-Z0-9_@]+)\s*;?\s*$', content):
                           line_obj.content = re.sub(r'(?i)^(\s*)RETURN\s+QUERY\s+SELECT\s+([a-zA-Z0-9_@]+)\s*;?', r'\1RETURN \2;', content)
                      # Otherwise wrap in parentheses for evaluation
                      elif re.match(r'(?i)^(\s*)RETURN\s+QUERY\s+SELECT\b', content):
                           line_obj.content = re.sub(r'(?i)^(\s*)RETURN\s+QUERY\s+(SELECT\s+[^;]+);?', r'\1RETURN (\2);', content)

            parser.pass_12_add_if_levels(final_output)

            ## The output of the parser is assembled a second time here, with the header this
            ## connector builds - and that threw away the SQL functions mapping, which run()
            ## applies to the output of its own pass 11. Every function of the source stood in
            ## the generated routine as it was written ('getdate()', 'isnull(', 'len(') and
            ## PostgreSQL does not have any of them.
            for line_obj in final_output:
                line_obj.content = self.apply_sql_functions_mapping(line_obj.content, {'target_db_type': target_db_type})

            # Build DDL with indentation (Logic ported from TsqlParser.print_with_indentation)
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

                if line_obj.source_array == "variable_declaration" or stripped.upper().startswith("DECLARE "):
                    line_obj.content = self._quote_udts_in_declaration(line_obj.content, settings)
                    stripped = line_obj.content.strip()

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

            ddl = self.unquote_local_variables(ddl)

            # Double quote user defined types that remained in the DDL
            udt_map = self._get_udt_codes_mapping(settings)
            if udt_map:
                for udt_name in udt_map.keys():
                    ddl = re.sub(rf'(?<!")\b{re.escape(udt_name)}\b(?!")', f'"{udt_name}"', ddl, flags=re.IGNORECASE)

            return ddl
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sybase_ase_connector: convert_funcproc_code: Critical Failure: {e}")
            import traceback
            self.config_parser.print_log_message('ERROR', f"sybase_ase_connector: convert_funcproc_code: Traceback: {traceback.format_exc()}")
            return f"/* CRITICAL FAILURE IN PARSER: {e} */\n" + settings.get('funcproc_code', '')

    def fetch_sequences(self, schema_name: str):
        # Placeholder for fetching sequences
        return {}

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
        self.config_parser.print_log_message('ERROR', f"sybase_ase_connector: handle_error: An error in {self.__class__.__name__} ({description}): {e}")
        self.config_parser.print_log_message('ERROR', traceback.format_exc())
        if self.on_error_action == 'stop':
            self.config_parser.print_log_message('ERROR', "sybase_ase_connector: handle_error: Stopping due to error.")
            exit(1)
        else:
            self.config_parser.print_log_message('WARNING', f"sybase_ase_connector: handle_error: Error caught, but continuing as requested by configuration (on_error_action='{self.on_error_action}').")

    def get_rows_count(self, table_schema: str, table_name: str, migration_limitation: str = None):
        if migration_limitation:
            query = f"""SELECT COUNT(*) FROM {table_schema}.{table_name} WHERE {migration_limitation} """
        else:
            query = f"""SELECT ROW_COUNT(db_id(), object_id('{table_name}')) """
        self.config_parser.print_log_message('DEBUG3',f"sybase_ase_connector: get_rows_count: query: {query}")
        cursor = self.connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    ## function to analyze primary key distribution
    ## looks like python handels cursors differently than PostgreSQL from FDW
    ## so currently this function is not used
    ##
    # def analyze_pk_distribution_batches(self, values):
    #     migrator_tables = values['migrator_tables']
    #     schema_name = values['source_schema_name']
    #     table_name = values['source_table_name']
    #     primary_key_columns = values['primary_key_columns']
    #     primary_key_columns_count = values['primary_key_columns_count']
    #     primary_key_columns_types = values['primary_key_columns_types']
    #     worker_id = values['worker_id']
    #     analyze_batch_size = self.config_parser.get_batch_size()

    #     if primary_key_columns_count == 1 and primary_key_columns_types in ('BIGINT', 'INTEGER', 'NUMERIC', 'REAL', 'FLOAT', 'DOUBLE PRECISION', 'DECIMAL', 'SMALLINT', 'TINYINT'):
    #         # primary key is one column of numeric type - analysis with min/max values is much quicker
    #         self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_rows_count: Worker: {worker_id}: PK analysis: {primary_key_columns} ({primary_key_columns_types}): min/max analysis")

    #         current_batch_percent = 20

    #         sybase_cursor = self.connection.cursor()
    #         temp_table = f"temp_id_ranges_{str(worker_id).replace('-', '_')}"
    #         migrator_tables.protocol_connection.execute_query(f"""DROP TABLE IF EXISTS "{temp_table}" """)
    #         migrator_tables.protocol_connection.execute_query(f"""CREATE TEMP TABLE IF NOT EXISTS "{temp_table}" (batch_start BIGINT, batch_end BIGINT, row_count BIGINT)""")

    #         pk_range_table = self.config_parser.get_protocol_name_pk_ranges()
    #         sybase_cursor.execute(f"SELECT MIN({primary_key_columns}) FROM {schema_name}.{table_name}")
    #         min_id = sybase_cursor.fetchone()[0]

    #         sybase_cursor.execute(f"SELECT MAX({primary_key_columns}) FROM {schema_name}.{table_name}")
    #         max_id = sybase_cursor.fetchone()[0]

    #         self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_rows_count: Worker: {worker_id}: PK analysis: {primary_key_columns}: min_id: {min_id}, max_id: {max_id}")

    #         total_range = int(max_id) - int(min_id)
    #         current_start = min_id
    #         loop_counter = 0
    #         previous_row_count = 0
    #         same_previous_row_count = 0
    #         current_decrease_ratio = 2

    #         while current_start <= max_id:
    #             current_batch_size = int(total_range / 100 * current_batch_percent)
    #             if current_batch_size < analyze_batch_size:
    #                 current_batch_size = analyze_batch_size
    #                 current_decrease_ratio = 2
    #                 self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_rows_count: Worker: {worker_id}: PK analysis: {loop_counter}: resetting current_decrease_ratio to {current_decrease_ratio}")

    #             current_end = current_start + current_batch_size

    #             self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_rows_count: Worker: {worker_id}: PK analysis: {loop_counter}: Loop counter: {loop_counter}, current_batch_percent: {round(current_batch_percent, 8)}, current_batch_size: {current_batch_size}, current_start: {current_start} (min: {min_id}), current_end: {current_end} (max: {max_id}), perc: {round(current_start / max_id * 100, 4)}")

    #             if current_end > max_id:
    #                 current_end = max_id

    #             loop_counter += 1
    #             sybase_cursor.execute(f"""SELECT COUNT(*) FROM {schema_name}.{table_name} WHERE {primary_key_columns} BETWEEN %s AND %s""", (current_start, current_end))
    #             testing_row_count = sybase_cursor.fetchone()[0]

    #             self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_rows_count: Worker: {worker_id}: PK analysis: {loop_counter}: Testing row count: {testing_row_count}")

    #             if testing_row_count == previous_row_count:
    #                 same_previous_row_count += 1
    #                 if same_previous_row_count >= 2:
    #                     current_decrease_ratio *= 2
    #                     self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_rows_count: Worker: {worker_id}: PK analysis: {loop_counter}: changing current_decrease_ratio to {current_decrease_ratio}")
    #                     same_previous_row_count = 0
    #             else:
    #                 same_previous_row_count = 0

    #             previous_row_count = testing_row_count

    #             if testing_row_count > analyze_batch_size:
    #                 current_batch_percent /= current_decrease_ratio
    #                 self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_rows_count: Worker: {worker_id}: PK analysis: {loop_counter}: Decreasing analyze_batch_percent to {round(current_batch_percent, 8)}")
    #                 continue

    #             if testing_row_count == 0:
    #                 current_batch_percent *= 1.5
    #                 self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_rows_count: Worker: {worker_id}: PK analysis: {loop_counter}: Increasing analyze_batch_percent to {round(current_batch_percent, 8)} without restarting loop")

    #             sybase_cursor.execute(f"""SELECT
    #                         %s::bigint AS batch_start,
    #                         %s::bigint AS batch_end,
    #                         COUNT(*) AS row_count
    #                         FROM {schema_name}.{table_name}
    #                         WHERE {primary_key_columns  } BETWEEN %s AND %s""",
    #                         (current_start, current_end, current_start, current_end))

    #             result = sybase_cursor.fetchone()
    #             if result:
    #                 insert_batch_start = result[0]
    #                 insert_batch_end = result[1]
    #                 insert_row_count = result[2]
    #                 self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_rows_count: Worker: {worker_id}: PK analysis: {loop_counter}: Insert batch into temp table: start: {insert_batch_start}, end: {insert_batch_end}, row count: {insert_row_count}")
    #                 migrator_tables.protocol_connection.execute_query(f"""INSERT INTO "{temp_table}" (batch_start, batch_end, row_count) VALUES (%s, %s, %s)""", (insert_batch_start, insert_batch_end, insert_row_count))

    #             current_start = current_end + 1
    #             self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_rows_count: Worker: {worker_id}: PK analysis: {loop_counter}: loop end - new current_start: {current_start}")

    #         self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_rows_count: Worker: {worker_id}: PK analysis: {loop_counter}: second loop")

    #         current_start = min_id
    #         while current_start <= max_id:
    #             migrator_tables.protocol_connection.execute_query("""
    #                 SELECT
    #                     min(batch_start) as batch_start,
    #                     max(batch_end) as batch_end,
    #                     max(cumulative_row_count) as row_count
    #                 FROM (
    #                     SELECT
    #                         batch_start,
    #                         batch_end,
    #                         sum(row_count) over (order by batch_start) as cumulative_row_count
    #                     FROM "{temp_table}"
    #                     WHERE batch_start >= %s::bigint
    #                     ORDER BY batch_start
    #                 ) subquery
    #                 WHERE cumulative_row_count <= %s::bigint
    #             """, (current_start, analyze_batch_size))
    #             result = migrator_tables.fetchone()
    #             if result:
    #                 insert_batch_start = result[0]
    #                 insert_batch_end = result[1]
    #                 insert_row_count = result[2]
    #                 self.config_parser.print_log_message('DEBUG', (f"Worker: {worker_id}: PK analysis: {loop_counter}: Insert batch into protocol table: start: {insert_batch_start}, end: {insert_batch_end}, row count: {insert_row_count}")

    #             values = {}
    #             values['source_schema_name'] = schema_name
    #             values['source_table_name'] = table_name
    #             values['source_table_id'] = 0
    #             values['worker_id'] = worker_id
    #             values['pk_columns'] = primary_key_columns
    #             values['batch_start'] = insert_batch_start
    #             values['batch_end'] = insert_batch_end
    #             values['row_count'] = insert_row_count
    #             migrator_tables.insert_pk_ranges(values)
    #             current_start = insert_batch_end

    #         migrator_tables.protocol_connection.execute_query(f"""DROP TABLE IF EXISTS "{temp_table}" """)
    #         self.connection.commit()
    #         self.config_parser.print_log_message('INFO', f"sybase_ase_connector: get_rows_count: Worker: {worker_id}: PK analysis: {loop_counter}: Finished analyzing PK distribution for table {table_name}.")
    #         ## end of function


        # unfortunately, the following code is not working as expected - Sybase does not support BETWEEN for multiple columns as PostgreSQL does
        # this solution worked for foreign data wrapper but not for native connection
        # if PK has more than one column, we shall use cursor
        # else:

            # # we need to do slower analysis with selecting all values of primary key
            # # necessary for composite keys or non-numeric keys
            # self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_rows_count: Worker: {worker_id}: PK analysis: {primary_key_columns} ({primary_key_columns_types}): analyzing all PK values")

            # primary_key_columns_list = primary_key_columns.split(',')
            # primary_key_columns_types_list = primary_key_columns_types.split(',')
            # temp_table_structure = ', '.join([f"{column.strip()} {column_type.strip()}" for column, column_type in zip(primary_key_columns_list, primary_key_columns_types_list)])
            # self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_rows_count: Worker: {worker_id}: PK analysis: {primary_key_columns}: temp table structure: {temp_table_structure}")

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
            #     # self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_rows_count: Worker: {worker_id}: PK analysis: {primary_key_columns}: row: {row}")
            #     insert_values = ', '.join([f"'{value}'" if isinstance(value, str) else str(value) for value in row])
            #     migrator_tables.protocol_connection.execute_query(f"""INSERT INTO "{temp_table}" ({primary_key_columns}) VALUES ({insert_values})""")
            # self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_rows_count: Worker: {worker_id}: PK analysis: {primary_key_columns}: Inserted {pk_temp_table_row_count} rows into temp table {temp_table}")

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

            #     self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_rows_count: Worker: {worker_id}: PK analysis: {batch_loop}: Loop counter: {batch_loop}, PK values: {rec_min_values} / {rec_max_values}")

            #     values = {}
            #     values['source_schema_name'] = schema_name
            #     values['source_table_name'] = table_name
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
            target_schema_name = settings['target_schema_name']  ## target schema is used as it is defined in config, not converted to upper/lower case
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
                self.config_parser.print_log_message('INFO', f"sybase_ase_connector: migrate_table: Worker {worker_id}: Table {source_table_name} is empty - skipping data migration.")
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

                    self.config_parser.print_log_message('INFO', f"sybase_ase_connector: migrate_table: Worker {worker_id}: Source table {source_table_name}: {source_table_rows_limited} rows / Target table {target_table_name}: {target_table_rows} rows - starting data migration.")

                    select_columns_list = []
                    orderby_columns_list = []
                    insert_columns_list = []

                    def is_generated_column(column):
                        return column.get('is_generated_virtual') == 'YES' or column.get('is_generated_stored') == 'YES'

                    # A computed column of the source is a generated column of the target,
                    # which PostgreSQL computes itself and refuses a value for ('cannot
                    # insert a non-DEFAULT value into column'). A hidden column - the key
                    # of a functional index - does not exist in the target at all.
                    migrated_source_columns = {
                        order_num: column for order_num, column in source_columns.items()
                        if not is_generated_column(column)
                        and column.get('is_hidden_column') != 'YES'
                        and not is_generated_column(target_columns.get(order_num, {}) if target_columns else {})
                    }
                    skipped_columns = [column['column_name'] for order_num, column in source_columns.items()
                                       if order_num not in migrated_source_columns]
                    if skipped_columns:
                        self.config_parser.print_log_message('INFO', f"sybase_ase_connector: migrate_table: Worker {worker_id}: Table {source_schema_name}.{source_table_name}: Columns computed by the target or not migrated at all are left out of the data migration: {', '.join(skipped_columns)}.")

                    for order_num, col in migrated_source_columns.items():
                        self.config_parser.print_log_message('DEBUG2',
                                                            f"Worker {worker_id}: Table {source_schema_name}.{source_table_name}: Processing column {col['column_name']} ({order_num}) with data type {col['data_type']}")

                        # if col['data_type'].lower() == 'datetime':
                        #     select_columns_list.append(f"TO_CHAR({col['column_name']}, '%Y-%m-%d %H:%M:%S') as {col['column_name']}")
                        #     select_columns_list.append(f"ST_asText(`{col['column_name']}`) as `{col['column_name']}`")
                        # elif col['data_type'].lower() == 'set':
                        #     select_columns_list.append(f"cast(`{col['column_name']}` as char(4000)) as `{col['column_name']}`")
                        # else:
                        select_columns_list.append(f"{col['column_name']}")

                        insert_columns_list.append(f'''"{self.config_parser.convert_names_case(col['column_name'])}"''')

                        # fixing error - [42000] [FreeTDS][SQL Server]The TEXT, IMAGE and UNITEXT datatypes cannot be used in an ORDER BY clause or in the select list of a query in a UNION statement.\n (420) (SQLExecDirectW)
                        if col['data_type'].lower() in ['text', 'image', 'unitext']:
                            self.config_parser.print_log_message('DEBUG2', f"sybase_ase_connector: migrate_table: Worker {worker_id}: Table {source_schema_name}.{source_table_name}: Column {col['column_name']} ({order_num}) with data type {col['data_type']} cannot be used in ORDER BY clause or in the select list of a query in a UNION statement.")
                            continue
                        orderby_columns_list.append(f'''{col['column_name']}''')

                    select_columns = ', '.join(select_columns_list)
                    orderby_columns = ', '.join(orderby_columns_list)
                    insert_columns = ', '.join(insert_columns_list)

                    if resume_after_crash and not drop_unfinished_tables:
                        chunk_number = self.config_parser.get_total_chunks(target_table_rows, chunk_size)
                        self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: migrate_table: Worker {worker_id}: Resuming migration for table {source_schema_name}.{source_table_name} from chunk {chunk_number} with data chunk size {chunk_size}.")
                        chunk_offset = target_table_rows
                    else:
                        chunk_offset = (chunk_number - 1) * chunk_size

                    chunk_start_row_number = chunk_offset + 1
                    chunk_end_row_number = chunk_offset + chunk_size

                    self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: migrate_table: Worker {worker_id}: Migrating table {source_schema_name}.{source_table_name}: chunk {chunk_number}, data chunk size {chunk_size}, batch size {batch_size}, chunk offset {chunk_offset}, chunk end row number {chunk_end_row_number}, source table rows {source_table_rows_limited}")
                    order_by_clause = ''

                    ## Sybase ASE does not support LIMIT with OFFSET, in older versions,
                    # therefore we cannot use chunks and cannot continue after a crash
                    # Partially migrated tables must be dropped and restarted
                    query = f"SELECT {select_columns} FROM {source_schema_name}.{source_table_name}"
                    if migration_limitation:
                        query += f" WHERE {migration_limitation}"
                    primary_key_columns = migrator_tables.select_primary_key({'source_schema_name': source_schema_name, 'source_table_name': source_table_name})
                    self.config_parser.print_log_message('DEBUG2', f"sybase_ase_connector: migrate_table: Worker {worker_id}: Primary key columns for {source_schema_name}.{source_table_name}: {primary_key_columns}")
                    if primary_key_columns:
                        orderby_columns = primary_key_columns
                    order_by_clause = f""" ORDER BY {orderby_columns}"""
                    query += order_by_clause
                    # query += order_by_clause + f" LIMIT {chunk_size}"

                    self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: migrate_table: Worker {worker_id}: Fetching data with cursor using query: {query}")

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
                        self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: migrate_table: Worker {worker_id}: Fetched {len(records)} rows (batch {batch_number}) from source table '{source_table_name}' using cursor")

                        # Convert records to a list of dictionaries
                        transforming_start_time = time.time()
                        records = [
                            {column['column_name']: value for column, value in zip(migrated_source_columns.values(), record)}
                            for record in records
                        ]
                        for record in records:
                            for order_num, column in migrated_source_columns.items():
                                column_name = column['column_name']
                                column_type = column['data_type']
                                # The TIMESTAMP of Sybase is the binary row version of the
                                # row, not a point in time (see get_types_mapping)
                                if column_type.lower() in ['binary', 'varbinary', 'image', 'timestamp']:
                                    record[column_name] = bytes(record[column_name]) if record[column_name] is not None else None
                                elif column_type.lower() in ['datetime', 'smalldatetime', 'date', 'time']:
                                    record[column_name] = str(record[column_name]) if record[column_name] is not None else None

                        # Insert batch into target table
                        self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: migrate_table: Worker {worker_id}: Starting insert of {len(records)} rows from source table {source_table_name}")
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
                    self.config_parser.print_log_message('INFO', f"sybase_ase_connector: migrate_table: Worker {worker_id}: Target table {target_schema_name}.{target_table_name} has {target_table_rows} rows")

                    shortest_batch_seconds = min(batch_durations) if batch_durations else 0
                    longest_batch_seconds = max(batch_durations) if batch_durations else 0
                    average_batch_seconds = sum(batch_durations) / len(batch_durations) if batch_durations else 0
                    self.config_parser.print_log_message('INFO', f"sybase_ase_connector: migrate_table: Worker {worker_id}: Migrated {total_inserted_rows} rows from {source_table_name} to {target_schema_name}.{target_table_name} in {batch_number} batches: "
                                                            f"Shortest batch: {shortest_batch_seconds:.2f} seconds, "
                                                            f"Longest batch: {longest_batch_seconds:.2f} seconds, "
                                                            f"Average batch: {average_batch_seconds:.2f} seconds")


                    cursor.close()

                else:
                    self.config_parser.print_log_message('INFO', f"sybase_ase_connector: migrate_table: Worker {worker_id}: Target table {target_table_name} has {target_table_rows} rows and data_conflict_action is '{data_conflict_action}'. Skipping data migration.")

                migration_stats = {
                    'rows_migrated': total_inserted_rows,
                    'chunk_number': chunk_number,
                    'total_chunks': total_chunks,
                    'source_table_rows_all': source_table_rows_all,

                    'source_table_rows_limited': source_table_rows_limited,
                    'target_table_rows': target_table_rows,
                    'finished': False,
                }

                self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: migrate_table: Worker {worker_id}: Migration stats: {migration_stats}")
                # Sybase ASE does not support query chunking (LIMIT/OFFSET).
                # Therefore, the query fetches all matching rows in a single pass.
                # We must unconditionally mark the migration as finished to prevent the 
                # orchestrator from looping and duplicating the entire dataset.
                if True:
                    self.config_parser.print_log_message('DEBUG3', f"sybase_ase_connector: migrate_table: Worker {worker_id}: Setting migration status to finished for table {source_table_name} (chunk {chunk_number}/{total_chunks})")
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
            self.config_parser.print_log_message('ERROR', f"sybase_ase_connector: migrate_table: Worker {worker_id}: Error during {part_name} -> {e}")
            self.config_parser.print_log_message('ERROR', f"sybase_ase_connector: migrate_table: Worker {worker_id}: Full stack trace: {traceback.format_exc()}")
            raise e

    PSEUDO_TABLE_RECORDS = {'inserted': 'NEW', 'deleted': 'OLD'}

    ## the words an unquoted name of a query is not a column of the pseudo table for
    NON_COLUMN_WORDS = frozenset("""
        select distinct top all from where group by having order asc desc union intersect except
        and or not null is in like between exists case when then else end as on join inner left
        right full outer cross apply into values set update insert delete truncate returning
        limit offset fetch first next rows only with recursive true false unknown current_date
        current_time current_timestamp current_user session_user localtime localtimestamp
        interval collate escape similar to any some new old
        int integer smallint bigint tinyint numeric decimal dec float real double precision
        money smallmoney char character varchar text nchar nvarchar unichar univarchar
        date time datetime smalldatetime timestamp bigdatetime bigtime bit boolean bool bytea
        binary varbinary image uuid json jsonb xml serial bigserial name
    """.split())

    def convert_sybase_global_variables(self, code, routine_name):
        """
        The global variables of Sybase ASE as what PostgreSQL offers in their place, together with
        the declarations the replacements need.

        They were left in the code as they stood, and PostgreSQL answered every one of them with
        'syntax error at or near "@"' - it has no such variables, so a routine reading one could
        not even be created. What has a counterpart is replaced by it; what has none becomes a
        variable holding the value the routine would read here, so that the code is accepted and
        the place is named in the log. '@@rowcount' is not touched: the parser reads it out of
        the row count of the last statement, which is what it means.

        Which of them keep the behaviour of the source and which have to be rewritten by hand is
        written down in the user guide, under 'Sybase ASE - cases which need manual adjustment'.
        """
        declarations = []

        ## Sybase reads the name of the running routine out of its own id
        code = re.sub(r'(?i)\bobject_name\s*\(\s*@@procid\s*\)', f"'{routine_name}'", code)

        ## the value the IDENTITY column of the last INSERT was given. lastval() answers with the
        ## value the last sequence of the session produced, which is that value as long as the
        ## column is an identity column of PostgreSQL and nothing else drew from a sequence in
        ## between.
        code = re.sub(r'(?i)@@identity\b', 'lastval()', code)

        code = re.sub(r'(?i)@@spid\b', 'pg_backend_pid()', code)
        code = re.sub(r'(?i)@@servername\b', "current_setting('cluster_name', true)", code)
        code = re.sub(r'(?i)@@version\b', 'version()', code)
        ## a bare @@procid - the id of the running routine - is left to the report below: PostgreSQL
        ## has no such value, and only the name of the routine, which is what 'object_name(@@procid)'
        ## asks for, can be answered

        ## A routine of PostgreSQL runs inside the transaction of its caller and cannot open one
        ## of its own, so the number of open transactions is 1 wherever the routine reads it.
        if re.search(r'(?i)@@trancount\b', code):
            code = re.sub(r'(?i)@@trancount\b', 'global_trancount', code)
            declarations.append("global_trancount INTEGER DEFAULT 1; "
                                "/* @@trancount - a routine of PostgreSQL runs inside the transaction of its caller */")

        ## @@error is the status the last statement left behind. PostgreSQL raises an exception
        ## instead of setting a status, so the statement which failed never reaches the test: the
        ## variable keeps the 0 it starts with, the routine is accepted, and the error handling
        ## written around it is dead code until it is rewritten with an EXCEPTION block.
        if re.search(r'(?i)@@error\b', code):
            code = re.sub(r'(?i)@@error\b', 'locvar_sybase_error', code)
            declarations.append("locvar_sybase_error INTEGER DEFAULT 0; "
                                "/* @@error - PostgreSQL raises an exception instead of setting a status */")
            self.config_parser.print_log_message('WARNING',
                f"sybase_ase_connector: {routine_name} tests @@error - PostgreSQL raises an exception where Sybase ASE "
                "sets a status, so the test never becomes true and the error handling behind it does nothing. It has to "
                "be rewritten as an EXCEPTION block by hand.")

        ## the parser reads these out of the statement they belong to
        handled_by_parser = ('@@rowcount', '@@sqlstatus', '@@nestlevel')
        remaining = sorted({found.lower() for found in re.findall(r'@@[a-zA-Z_][a-zA-Z0-9_]*', code)}
                           - set(handled_by_parser))
        if remaining:
            self.config_parser.print_log_message('WARNING',
                f"sybase_ase_connector: {routine_name} reads the global variable(s) {', '.join(remaining)} of Sybase ASE, "
                "which PostgreSQL does not have and will refuse with 'syntax error at or near \"@\"'. They have to be "
                "replaced by hand.")

        return code, declarations

    def unquote_local_variables(self, code):
        """
        The variables of the converted routine written without quotes.

        The statement converter reads a bare name of a query as a column and quotes it to keep
        its case, which is right for a column and wrong for a variable: 'select @a = @@trancount'
        became 'locvar_a := "global_trancount"'. PL/pgSQL does resolve the quoted name as long as
        it is lower case, but the quotes say column to every reader of the code. A name carrying
        one of the prefixes the conversion gives its own variables is never a column.
        """
        return re.sub(r'"((?:locvar|global)_[a-zA-Z0-9_]+)"', r'\1', code)

    def encapsulate_line_comments(self, code):
        """
        The comments of the code written with '--' as block comments.

        The parser joins the lines of a statement into one, and a comment which runs to the end
        of its line would comment out everything joined behind it. Only a '--' which really
        begins a comment is converted: one inside a string literal belongs to the text, and one
        inside a block comment belongs to that comment - the row of dashes which frames the
        header a generator wrote ('/*\\n----\\n  trigger generated by ...\\n----\\n*/') turned
        into '/*----*/' there, which closed the comment around it and left the rest of it
        standing in the code as statements nobody could read.

        A '/*' or a '*/' inside the text of the comment is taken apart as well - the text is not
        SQL, and either of them would open or close a comment which is not meant.
        """
        result = []
        index = 0
        length = len(code)
        comment_depth = 0
        while index < length:
            if comment_depth > 0:
                if code.startswith('/*', index):
                    comment_depth += 1
                elif code.startswith('*/', index):
                    comment_depth -= 1
                else:
                    result.append(code[index])
                    index += 1
                    continue
                result.append(code[index:index + 2])
                index += 2
                continue

            if code[index] == "'":
                end = index + 1
                while end < length:
                    if code[end] == "'":
                        if end + 1 < length and code[end + 1] == "'":
                            end += 2
                            continue
                        end += 1
                        break
                    end += 1
                result.append(code[index:end])
                index = end
                continue

            if code.startswith('/*', index):
                comment_depth += 1
                result.append(code[index:index + 2])
                index += 2
                continue

            if code.startswith('--', index):
                end = code.find('\n', index)
                end = length if end == -1 else end
                text = code[index:end].replace('/*', '/ *').replace('*/', '* /')
                result.append(f"/*{text}*/")
                index = end
                continue

            result.append(code[index])
            index += 1

        return ''.join(result)

    def scan_sql_text(self, text):
        """
        The text with its string literals and comments blanked out, and the depth of parentheses
        of every one of its characters.

        A position found in the blanked text addresses the same character of the original, so a
        keyword or a name can be looked for where SQL means it and be taken from the text as it
        was written. A parenthesis counts towards the depth of what it encloses, which makes the
        span of a subquery the run of characters of its own depth.
        """
        masked = list(text)
        depths = [0] * len(text)
        depth = 0
        index = 0
        while index < len(text):
            char = text[index]
            if char == "'":
                masked[index] = ' '
                depths[index] = depth
                index += 1
                while index < len(text):
                    depths[index] = depth
                    is_quote = text[index] == "'"
                    masked[index] = ' '
                    index += 1
                    if is_quote:
                        ## a doubled quote is a quote inside the literal, not its end
                        if index < len(text) and text[index] == "'":
                            continue
                        break
                continue
            if text.startswith('/*', index):
                end = text.find('*/', index)
                end = len(text) if end == -1 else end + 2
                for position in range(index, end):
                    masked[position] = ' '
                    depths[position] = depth
                index = end
                continue
            if char == '(':
                depth += 1
            depths[index] = depth
            if char == ')':
                depth -= 1
            index += 1
        return ''.join(masked), depths

    def pseudo_table_pattern(self, pseudo_table=None):
        """
        The expression which finds a pseudo table of a trigger, written with or without quotes.

        A name behind a dot is not one of them: Sybase ASE offers 'inserted' and 'deleted' under
        those names alone, so a qualified 'dbo.inserted' is a table of the schema and stays as it
        is written.
        """
        return rf'(?i)(?<![\w."])"?\b({pseudo_table or "inserted|deleted"})\b"?'

    def reads_pseudo_table(self, text, pseudo_table=None):
        """
        Whether the code reads a pseudo table of a trigger, looking past its literals and
        comments - the conversion names them in the comments it leaves behind.
        """
        return bool(re.search(self.pseudo_table_pattern(pseudo_table), self.scan_sql_text(text)[0]))

    def substitute_outside_literals(self, text, pattern, replacement):
        """
        The text with every occurrence of the pattern outside its literals and comments replaced.
        """
        masked, _ = self.scan_sql_text(text)
        result = []
        position = 0
        for match in re.finditer(pattern, masked):
            result.append(text[position:match.start()])
            result.append(replacement)
            position = match.end()
        result.append(text[position:])
        return ''.join(result)

    def find_pseudo_table_reference(self, statement):
        """
        The first reference to a pseudo table of a trigger in the statement, as its position, the
        pseudo table named and the depth of parentheses it stands on.
        """
        masked, depths = self.scan_sql_text(statement)
        match = re.search(self.pseudo_table_pattern(), masked)
        if not match:
            return None
        return {'start': match.start(), 'end': match.end(),
                'name': match.group(1).lower(),
                'record': self.PSEUDO_TABLE_RECORDS[match.group(1).lower()],
                'depth': depths[match.start()],
                'masked': masked, 'depths': depths}

    def enclosing_query_span(self, masked, depths, position, depth):
        """
        The span of the query a position belongs to: the parentheses of its depth which enclose
        it, or the whole statement when it stands on the depth of the statement itself.
        """
        if depth == 0:
            return 0, len(masked)
        start = position
        while start > 0 and not (depths[start] == depth and masked[start] == '('):
            start -= 1
        end = position
        while end < len(masked) and not (depths[end] == depth and masked[end] == ')'):
            end += 1
        return start + 1, end

    def clause_positions(self, masked, depths, span, depth):
        """
        The clauses of a query written between the given positions and on the given depth, as a
        list of the keyword and where it begins.
        """
        pattern = (r'(?i)\b(FROM|WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|UNION|INTERSECT|EXCEPT'
                   r'|LIMIT|OFFSET|RETURNING|FOR\s+UPDATE|JOIN|VALUES)\b')
        clauses = []
        for match in re.finditer(pattern, masked[span[0]:span[1]]):
            start = span[0] + match.start()
            if depths[start] != depth:
                continue
            clauses.append({'keyword': re.sub(r'\s+', ' ', match.group(1)).upper(),
                            'start': start, 'end': span[0] + match.end()})
        return clauses

    def split_pseudo_table_from_list(self, from_list, pseudo_table):
        """
        The FROM list without the pseudo table, and the names its columns were addressed by - the
        pseudo table itself and the alias it was given.
        """
        masked, depths = self.scan_sql_text(from_list)
        items = []
        start = 0
        for position, char in enumerate(masked):
            if char == ',' and depths[position] == 0:
                items.append(from_list[start:position])
                start = position + 1
        items.append(from_list[start:])

        remaining = []
        names = []
        for item in items:
            match = re.match(rf'(?i)^\s*"?{pseudo_table}"?\s*(?:AS\s+)?("?[\w$#]+"?)?\s*$', item)
            if match:
                names.append(pseudo_table)
                if match.group(1):
                    names.append(match.group(1).strip('"'))
            else:
                remaining.append(item.strip())
        return remaining, names

    def subquery_ranges(self, text):
        """
        The spans of the subqueries of an expression - every parenthesis whose content is a query
        of its own.

        A name written inside one of them belongs to the tables that query reads, not to the
        pseudo table of the query around it: `select @v = (select max(x) from other) from
        inserted` reads x of 'other', and both the column and the aggregate over it stay as they
        are. A subquery which reads the pseudo table itself is rewritten as the query it is, in
        its own turn of the conversion.
        """
        masked, _ = self.scan_sql_text(text)
        ranges = []
        opened = []
        for position, char in enumerate(masked):
            if char == '(':
                opened.append(position)
            elif char == ')' and opened:
                start = opened.pop()
                if re.match(r'(?i)^\s*SELECT\b', masked[start + 1:position]):
                    ranges.append((start, position + 1))
        return ranges

    def qualify_pseudo_table_columns(self, expression, record, names, unqualified_are_columns):
        """
        The columns of an expression as fields of the record which replaces the pseudo table.

        A column addressed through the pseudo table or its alias is always renamed. A column
        written without a qualifier is one of the pseudo table only when that was the only table
        the query read - with another table left in the FROM clause the column may as well be
        one of that table, and it is left as it stands. The statement converter has quoted the
        columns it read, and an unquoted name is taken for one when it is not a keyword, a type,
        a function call or a variable.
        """
        for name in names:
            expression = self.substitute_outside_literals(
                expression, rf'(?i)(?<![\w."])"?\b{re.escape(name)}\b"?\s*\.\s*', f'{record}.')

        if not unqualified_are_columns:
            return expression

        masked, _ = self.scan_sql_text(expression)
        subqueries = self.subquery_ranges(expression)
        result = []
        position = 0
        for match in re.finditer(r'"[^"]*"|[a-zA-Z_][\w$#]*', masked):
            name = match.group(0)
            before = masked[:match.start()].rstrip()
            after = masked[match.end():].lstrip()

            is_column = True
            if any(start <= match.start() < end for start, end in subqueries):
                ## a name of a subquery belongs to the tables that subquery reads
                is_column = False
            elif before.endswith('.') or before.endswith('@') or after.startswith('.') or after.startswith('('):
                ## a qualified name, a variable or the name of a function
                is_column = False
            elif not name.startswith('"'):
                if name.lower() in self.NON_COLUMN_WORDS or re.match(r'(?i)^(locvar|global)_', name):
                    is_column = False

            if is_column:
                result.append(expression[position:match.start()])
                result.append(f'{record}.{expression[match.start():match.end()]}')
                position = match.end()
        result.append(expression[position:])
        return ''.join(result)

    def unwrap_single_row_aggregates(self, expression):
        """
        An aggregate over the one row of the pseudo table as the value it aggregates.

        `min(sales_object_id)` over the rows of the statement is the column itself once the rows
        are seen one at a time, and `count(*)` is 1. Keeping the aggregate would work as well -
        PostgreSQL aggregates over the one row of a query without a FROM clause - but it reads
        as a query over a set which the trigger no longer performs.
        """
        def unwrapper(subqueries):
            def unwrap(match):
                if any(start <= match.start() < end for start, end in subqueries):
                    ## the aggregate of a subquery is one over the rows that subquery reads
                    return match.group(0)
                function, argument = match.group(1).lower(), match.group(2).strip()
                return '1' if function == 'count' else argument
            return unwrap

        previous = None
        while previous != expression:
            previous = expression
            expression = re.sub(r'(?i)\b(MIN|MAX|SUM|AVG|COUNT)\s*\(\s*((?:[^()]|\([^()]*\))*?)\s*\)',
                                unwrapper(self.subquery_ranges(expression)), expression)
        return expression

    def rewrite_assignment_select(self, select_list, condition, counted):
        """
        The list of an assignment SELECT with the condition which selected the row of the pseudo
        table folded into the value of every assignment.

        `select @old = created from deleted where id = @id` has no FROM clause left to carry the
        condition, and PL/pgSQL assigns an expression alone, so the condition becomes a CASE. A
        row which the condition does not select leaves the variable empty, as the SELECT of
        Sybase over no row leaves it at what it held. A COUNT counted that row, so it becomes 0.
        """
        masked, depths = self.scan_sql_text(select_list)
        pairs = []
        start = 0
        for position, char in enumerate(masked):
            if char == ',' and depths[position] == 0:
                pairs.append(select_list[start:position])
                start = position + 1
        pairs.append(select_list[start:])

        rewritten = []
        for pair in pairs:
            match = re.match(r'^\s*(@[\w@]+)\s*=\s*(.+)$', pair, re.DOTALL)
            if not match:
                rewritten.append(pair.strip())
                continue
            variable, value = match.group(1), match.group(2).strip()
            if condition:
                otherwise = ' ELSE 0' if counted else ''
                value = f"CASE WHEN {condition} THEN {value}{otherwise} END"
            rewritten.append(f"{variable} = {value}")
        return ', '.join(rewritten)

    ## ------------------------------------------------------------------------------------
    ## The pseudo tables of a trigger, read as a parsed statement
    ##
    ## The conversion below reads the statement the way convert_view_code() reads a view - it
    ## is parsed, the tree is rewritten and the result is generated again - instead of looking
    ## for the clause keywords in the text. A FROM list which names the pseudo table beside a
    ## real table, a JOIN against it, both pseudo tables in one statement and the FROM clause
    ## of the DELETE and UPDATE of Transact-SQL are all the same thing in a parsed statement:
    ## an entry of the sources of a query. The clause positions in the text they are written
    ## at are not, which is why every one of those shapes had to be refused before.
    ## ------------------------------------------------------------------------------------

    ## The name a local variable of Sybase ASE is masked under while the statement is parsed.
    ## '@' is the absolute value operator of PostgreSQL, so '@old = created' parses as a
    ## comparison of the column 'old' against 'created' and the variable is lost. The name is
    ## written in lower case and carries no character which would make the generator quote it.
    SYBASE_VARIABLE_MASK = 'sybvar_mask_'

    def mask_sybase_variables(self, statement):
        """
        The statement with its local variables replaced by plain names, and what they stood for.

        Pass 9 of the parser renames '@var' to 'locvar_var' and runs after this conversion, so
        the variables have to be given back exactly as they were written.
        """
        variables = {}

        def mask(match):
            name = f"{self.SYBASE_VARIABLE_MASK}{len(variables)}"
            variables[name] = match.group(0)
            return name

        return re.sub(r'@@?[\w#$]+', mask, statement), variables

    def unmask_sybase_variables(self, statement, variables):
        """ The statement with the masked names written as the variables they stand for. """
        for name, variable in variables.items():
            statement = re.sub(rf'(?i)"?\b{re.escape(name)}\b"?', variable.replace('\\', '\\\\'), statement)
        return statement

    def is_masked_sybase_variable(self, name):
        return bool(name) and name.lower().startswith(self.SYBASE_VARIABLE_MASK)

    def normalise_tsql_dml(self, statement):
        """
        The DELETE of Transact-SQL written so that a SQL parser reads it.

        'delete from t from t, deleted where ...' names the table twice - once as the target of
        the DELETE and once as the first entry of a FROM clause which lists the tables the
        condition reads. The second FROM is the one carrying the pseudo table. No parser reads
        two FROM clauses in one DELETE, and dropping the first keyword leaves 'delete t from
        t, deleted where ...', which is the other spelling of the same statement and parses.
        """
        match = re.match(r'(?is)^(\s*DELETE\s+)FROM(\s+.+)$', statement)
        if not match:
            return statement
        rest = match.group(2)
        ## only when a second FROM follows on the level of the statement itself
        masked, depths = self.scan_sql_text(rest)
        if not any(depths[found.start()] == 0 for found in re.finditer(r'(?i)\bFROM\b', masked)):
            return statement
        return match.group(1) + rest.lstrip()

    def is_pseudo_table_node(self, node):
        """ The record a table node of a parsed statement stands for, or None. """
        if not isinstance(node, sqlglot.exp.Table):
            return None
        if node.args.get('db') or node.args.get('catalog'):
            ## 'dbo.inserted' is a table of the schema, not the pseudo table
            return None
        return self.PSEUDO_TABLE_RECORDS.get((node.name or '').lower())

    def scope_sources(self, scope):
        """
        The tables a query reads, as a list of (table, join) - the join being the node which
        attaches the table to the query, or None for the first one.

        A query of PostgreSQL keeps the entries behind the first one on the query itself, the
        DELETE and the UPDATE of Transact-SQL keep them on the table which heads their FROM
        clause. Both are a list of joins whose 'this' is a table, so they are read the same way.
        """
        if isinstance(scope, sqlglot.exp.Select):
            from_clause = scope.args.get('from_') or scope.args.get('from')
            head = from_clause.this if from_clause else None
            joins = list(scope.args.get('joins') or [])
        elif isinstance(scope, sqlglot.exp.Update):
            from_clause = scope.args.get('from_') or scope.args.get('from')
            head = from_clause.this if from_clause else None
            joins = list(head.args.get('joins') or []) if head is not None else []
        elif isinstance(scope, sqlglot.exp.Delete):
            head, joins = self.delete_source_head(scope)
        else:
            return []

        if head is None:
            return []
        sources = [(head, None)]
        sources.extend((join.this, join) for join in joins)
        return [(table, join) for table, join in sources if isinstance(table, sqlglot.exp.Table)]

    @staticmethod
    def delete_target(scope):
        """
        The table a DELETE removes rows from, in either of the two spellings it arrives in.

        Transact-SQL names the target in front of its FROM clause and repeats it inside that
        clause, which sqlglot keeps as 'tables' (the target) and 'this' (the head of the FROM
        list). PostgreSQL names it once behind FROM and lists the tables the condition reads in
        a USING clause, which is 'this' (the target) and 'using'.
        """
        tables = scope.args.get('tables')
        if tables:
            return tables[0]
        return scope.args.get('this')

    def delete_source_head(self, scope):
        """ The first table a DELETE reads besides its target, and the joins behind it. """
        using = scope.args.get('using')
        if using and isinstance(using[0], sqlglot.exp.Table):
            head = using[0]
        elif scope.args.get('tables'):
            ## the FROM list of Transact-SQL, which repeats the target as its first entry
            head = scope.args.get('this')
        else:
            return None, []
        if not isinstance(head, sqlglot.exp.Table):
            return None, []
        return head, list(head.args.get('joins') or [])

    @staticmethod
    def join_is_outer(join):
        """ Whether a join keeps the rows of one side which the other side does not match. """
        if join is None:
            return False
        side = (join.args.get('side') or '').upper()
        kind = (join.args.get('kind') or '').upper()
        return side in ('LEFT', 'RIGHT', 'FULL') or kind == 'OUTER'

    def set_scope_sources(self, scope, sources):
        """
        The query rewritten to read the given tables, in the places its kind keeps them.

        A query left without a table reads no table at all - PostgreSQL allows a SELECT with a
        WHERE clause and no FROM clause, which is what a query over the one row of a pseudo
        table becomes. The target of a DELETE or an UPDATE is not one of these entries: it is
        named by the statement itself and is removed from the FROM clause it was repeated in.

        Every source but the first keeps the join it was attached by, so that the condition of a
        JOIN and the side an outer join keeps are not lost. The first one cannot: it heads the
        FROM clause and there is nothing in front of it to join it to, so its condition is given
        to the caller to put into the WHERE clause instead.
        """
        head, head_join = sources[0] if sources else (None, None)
        joins = []
        for table, join in sources[1:]:
            joins.append(join.copy() if join is not None else sqlglot.exp.Join(this=table))
            joins[-1].set('this', table)

        if isinstance(scope, sqlglot.exp.Select):
            key = 'from_' if 'from_' in scope.args or scope.args.get('from') is None else 'from'
            if head is None:
                scope.args.pop('from_', None)
                scope.args.pop('from', None)
            else:
                head.set('joins', None)
                scope.set(key, sqlglot.exp.From(this=head))
            scope.set('joins', joins or None)
            return

        if isinstance(scope, sqlglot.exp.Update):
            key = 'from_' if 'from_' in scope.args or scope.args.get('from') is None else 'from'
            if head is None:
                scope.args.pop('from_', None)
                scope.args.pop('from', None)
            else:
                head.set('joins', joins or None)
                scope.set(key, sqlglot.exp.From(this=head))
            return

        if isinstance(scope, sqlglot.exp.Delete):
            ## PostgreSQL names the target once, behind its own FROM keyword, so the target
            ## moves into 'this' and the FROM list Transact-SQL repeated it in disappears.
            target = self.delete_target(scope).copy()
            target.set('joins', None)
            scope.set('tables', None)
            scope.set('this', target)
            ## a table left beside the target is one the condition reads - PostgreSQL lists
            ## those in a USING clause, where Transact-SQL wrote them in its second FROM clause
            if head is None:
                scope.set('using', None)
            else:
                head.set('joins', None)
                scope.set('using', [head] + [join.this for join in joins])

    ## The condition under which the row a pseudo table stands for exists at all. A trigger
    ## fired by an INSERT has no OLD row and one fired by a DELETE has no NEW row, which is
    ## what 'if not exists (select 1 from deleted)' asks about - the Transact-SQL way of
    ## telling an INSERT from an UPDATE inside a trigger written for both.
    PSEUDO_TABLE_PRESENCE = {'NEW': "TG_OP <> 'DELETE'", 'OLD': "TG_OP <> 'INSERT'"}

    def rewrite_scope_pseudo_tables(self, scope, trigger_name, scope_records):
        """
        One query of a statement with the pseudo tables it reads replaced by the records.

        The table leaves the sources of the query, the condition which attached it to the other
        tables moves into the WHERE clause and the columns addressed through it become fields of
        the record. Nothing else about the query changes: a real table it read stays where it
        was, and so do the condition and the aggregate over that table.
        """
        sources = self.scope_sources(scope)
        pseudo = [(table, join, self.is_pseudo_table_node(table)) for table, join in sources
                  if self.is_pseudo_table_node(table)]
        if not pseudo:
            return False

        ## A group over the pseudo table asks for one row per group of the rows the statement
        ## changed, which is a question about the whole set. One row is one group, so the
        ## conversion would answer a different question and has to refuse instead.
        if scope.args.get('group') or scope.args.get('having') or scope.args.get('distinct'):
            return None

        conditions = []
        for table, join, record in pseudo:
            if self.join_is_outer(join):
                ## the rows of the other table which the statement did not touch - a question
                ## about the whole set, which one row cannot answer
                return None

            names = {(table.name or '').lower()}
            if table.alias:
                names.add(table.alias.lower())

            ## A column addressed through the pseudo table or its alias is a field of the record
            ## wherever it stands - the subquery of an EXISTS reads the row of the query around
            ## it, so the whole tree of the query is rewritten, not its own level alone.
            for column in scope.find_all(sqlglot.exp.Column):
                if (column.table or '').lower() in names:
                    column.set('table', sqlglot.exp.Identifier(this=record, quoted=False))

            if join is not None and join.args.get('on') is not None:
                conditions.append(join.args['on'])

        remaining = [(table, join) for table, join in sources if not self.is_pseudo_table_node(table)]

        ## The target of a DELETE or an UPDATE of Transact-SQL is repeated in its FROM clause.
        ## PostgreSQL names it once, and leaving it in the FROM clause would read it a second
        ## time - 'UPDATE t SET ... FROM t' joins the table with itself and updates every row.
        if isinstance(scope, (sqlglot.exp.Delete, sqlglot.exp.Update)):
            target = (self.delete_target(scope) if isinstance(scope, sqlglot.exp.Delete)
                      else scope.args.get('this'))
            target_name = (target.name or '').lower() if isinstance(target, sqlglot.exp.Table) else None
            for position, (table, join) in enumerate(list(remaining)):
                if (table.name or '').lower() == target_name and not table.alias:
                    remaining.pop(position)
                    break

        ## Only a SELECT reads the pseudo table alone. The target of a DELETE or an UPDATE is a
        ## table of its own, so a column written there without a qualifier - the column an
        ## UPDATE assigns above all - is one of that table and has to be left as it stands.
        sole_source = (isinstance(scope, sqlglot.exp.Select)
                       and not remaining and len(pseudo) == 1)

        if sole_source:
            ## the query reads the one row and nothing else, so a name written without a
            ## qualifier is one of its columns and an aggregate over it is the value itself
            self.qualify_bare_columns(scope, pseudo[0][2])
            self.unwrap_aggregates_over_one_row(scope)

        ## The first source heads the FROM clause and cannot keep the join it was attached by.
        ## An outer join cannot be promoted that way - the side it keeps would be lost - and the
        ## condition of an inner one becomes a condition of the query.
        if remaining and remaining[0][1] is not None:
            if self.join_is_outer(remaining[0][1]):
                return None
            if remaining[0][1].args.get('on') is not None:
                conditions.append(remaining[0][1].args['on'])
            remaining[0] = (remaining[0][0], None)

        self.set_scope_sources(scope, remaining)

        for condition in conditions:
            self.add_scope_condition(scope, condition)

        ## which records this query stood for, for the EXISTS over it - keyed by identity
        ## because a node of sqlglot is not hashable by value
        scope_records[id(scope)] = [record for _, _, record in pseudo]
        return True

    def own_level_expressions(self, scope, node_type):
        """
        The nodes of a query which belong to the query itself and not to a subquery of it.
        """
        for node in scope.find_all(node_type):
            parent = node.parent
            while parent is not None and parent is not scope:
                if isinstance(parent, (sqlglot.exp.Select, sqlglot.exp.Delete, sqlglot.exp.Update)):
                    break
                parent = parent.parent
            if parent is scope:
                yield node

    def qualify_bare_columns(self, scope, record):
        """
        The columns of a query written without a qualifier as fields of the record.

        A masked variable is a name of the same shape and is not a column of the pseudo table,
        and neither is a name which belongs to a subquery reading a table of its own.
        """
        for column in self.own_level_expressions(scope, sqlglot.exp.Column):
            if column.table:
                continue
            if self.is_masked_sybase_variable(column.name):
                continue
            column.set('table', sqlglot.exp.Identifier(this=record, quoted=False))

    def unwrap_aggregates_over_one_row(self, scope):
        """
        An aggregate over the one row of a pseudo table as the value it aggregates.

        'min(id)' over the rows the statement changed is the column itself once the rows are
        seen one at a time, and 'count(*)' is 1.
        """
        for node in list(self.own_level_expressions(scope, sqlglot.exp.AggFunc)):
            name = type(node).__name__.upper()
            if name == 'COUNT':
                node.replace(sqlglot.exp.Literal.number(1))
            elif name in ('MIN', 'MAX', 'SUM', 'AVG') and node.this is not None:
                node.replace(node.this.copy())

    def add_scope_condition(self, scope, condition):
        """ The condition of a removed join added to the WHERE clause of the query. """
        where = scope.args.get('where')
        if where is None:
            scope.set('where', sqlglot.exp.Where(this=condition.copy()))
        else:
            where.set('this', sqlglot.exp.And(this=where.this.copy(), expression=condition.copy()))

    def collapse_one_row_existence_tests(self, parsed, scope_records):
        """
        An EXISTS over a query which reads a pseudo table and nothing else, as a plain condition.

        'exists (select * from inserted where c)' asks whether the row of the trigger is a row
        the condition selects, which is the condition itself once the pseudo table is gone.

        Without a condition the question is only whether the row exists at all, and that is the
        Transact-SQL way of telling the events of a trigger written for several of them apart:
        'if not exists (select 1 from deleted)' means 'if this is an INSERT'. Reading it as a
        constant would turn that test into one which never fires, so the presence of the record
        is asked about instead - and it is kept in front of the condition in the other case as
        well, where it also guards the condition against a record the event does not provide.

        Returns the tree, which is not always the one it was given: replace() of sqlglot puts a
        node in the place its parent holds it in and does nothing at all to a node without a
        parent, and the whole statement of an IF is exactly such a node.
        """
        for exists in list(parsed.find_all(sqlglot.exp.Exists)):
            query = exists.this
            if not isinstance(query, sqlglot.exp.Select):
                continue
            records = scope_records.get(id(query))
            if not records:
                continue
            if query.args.get('from_') or query.args.get('from') or query.args.get('joins'):
                continue
            if query.args.get('group') or query.args.get('having'):
                continue

            conditions = [sqlglot.condition(self.PSEUDO_TABLE_PRESENCE[record])
                          for record in dict.fromkeys(records)]
            where = query.args.get('where')
            if where is not None:
                conditions.append(sqlglot.exp.Paren(this=where.this.copy()))

            replacement = conditions[0]
            for condition in conditions[1:]:
                replacement = sqlglot.exp.And(this=replacement, expression=condition)
            replacement = sqlglot.exp.Paren(this=replacement)

            if exists is parsed:
                parsed = replacement
            else:
                exists.replace(replacement)
        return parsed

    def fold_assignment_conditions(self, parsed):
        """
        The condition of an assignment SELECT folded into the value it assigns.

        'select @old = created from deleted where id = @id' has no FROM clause left to carry the
        condition and PL/pgSQL assigns an expression alone, so the condition becomes a CASE. A
        row the condition does not select leaves the variable empty, which is what a SELECT of
        Sybase ASE over no row does, and what makes the WHILE loop over the rows of a statement
        end after the one row this trigger sees.
        """
        for select in list(parsed.find_all(sqlglot.exp.Select)):
            where = select.args.get('where')
            if where is None:
                continue
            if select.args.get('from_') or select.args.get('from') or select.args.get('joins'):
                continue
            assignments = [node for node in select.expressions if isinstance(node, sqlglot.exp.EQ)
                           and self.is_masked_sybase_variable(
                               node.this.name if isinstance(node.this, sqlglot.exp.Column) else '')]
            if not assignments or len(assignments) != len(select.expressions):
                continue
            for assignment in assignments:
                value = assignment.expression
                ## a count of the rows the condition did not select is 0, not an empty value
                otherwise = (sqlglot.exp.Literal.number(0)
                             if isinstance(value, sqlglot.exp.Literal) and value.this == '1'
                             and not value.args.get('is_string') else None)
                case = sqlglot.exp.Case(
                    ifs=[sqlglot.exp.If(this=where.this.copy(), true=value.copy())],
                    default=otherwise)
                assignment.set('expression', case)
            select.set('where', None)

    def convert_trigger_pseudo_tables_parsed(self, statement, trigger_name):
        """
        One statement of a trigger reading the pseudo tables, converted through the parser.

        Returns the converted statement, or None when the statement could not be read or names
        a pseudo table in a place which needs the whole set of rows the statement changed.
        """
        masked, variables = self.mask_sybase_variables(statement)
        masked = self.normalise_tsql_dml(masked)

        try:
            parsed = sqlglot.parse_one(masked, read='postgres')
        except Exception as e:
            self.config_parser.print_log_message('DEBUG',
                f"sybase_ase_connector: convert_trigger: Trigger {trigger_name}: the statement could not "
                f"be parsed for the conversion of its pseudo tables ({e}): {' '.join(statement.split())}")
            return None

        ## sqlglot does not raise on syntax it does not know - it returns a plain Command node,
        ## and every rewriting below would silently do nothing to it
        if parsed is None or isinstance(parsed, sqlglot.exp.Command):
            return None

        scope_records = {}
        changed = False
        ## the innermost query first, so that a subquery is converted before the query which
        ## reads it decides whether anything of the pseudo table is left in it
        for scope in reversed(list(parsed.find_all(
                sqlglot.exp.Select, sqlglot.exp.Delete, sqlglot.exp.Update))):
            outcome = self.rewrite_scope_pseudo_tables(scope, trigger_name, scope_records)
            if outcome is None:
                return None
            changed = changed or outcome

        if not changed:
            return None

        parsed = self.collapse_one_row_existence_tests(parsed, scope_records)
        self.fold_assignment_conditions(parsed)

        try:
            generated = parsed.sql(dialect='postgres')
        except Exception as e:
            self.config_parser.print_log_message('DEBUG',
                f"sybase_ase_connector: convert_trigger: Trigger {trigger_name}: the converted statement "
                f"could not be written ({e}): {' '.join(statement.split())}")
            return None

        generated = self.unmask_sybase_variables(generated, variables)
        if self.reads_pseudo_table(generated):
            return None
        return generated

    def convert_trigger_pseudo_tables(self, statement, command_kind, trigger_name, refusals):
        """
        One statement of a trigger of Sybase ASE reading the pseudo tables 'inserted' and
        'deleted' as the same statement reading the records NEW and OLD of a trigger of
        PostgreSQL.

        They are not tables - they are the names under which Sybase ASE offers the rows of the
        statement which fired the trigger. Removing the name and leaving the rest of the query
        as it stood is not enough: `select @old = created from deleted where id = @id` became
        `old := created where id = @id`, which PostgreSQL refuses, and the columns of the pseudo
        table were left addressing nothing. The pseudo table stands for exactly one row here, so
        the FROM clause it was listed in loses it, its columns become fields of the record, an
        aggregate over it is the value itself and the condition which selected its row is kept
        as a condition over the record.

        A trigger of Sybase ASE fires once per statement and can read all of its rows at once,
        while this is a trigger per row. A statement which needs the whole set - a group over the
        pseudo table, an outer join whose missing side is the pseudo table - cannot be expressed
        this way. It is kept as it was written, marked for the reader as needing to be completed
        by hand, and reported as a refusal which makes the whole trigger fail.

        This is the fallback of convert_trigger_pseudo_tables_parsed() and reads the statement by
        the positions its clause keywords are written at. It handles the pseudo table listed as
        the only entry of a FROM clause; every other shape is left to the parsed conversion.
        """
        if not self.reads_pseudo_table(statement):
            return statement

        original = statement
        refused = []

        def refuse(reason):
            refused.append(reason)
            self.config_parser.print_log_message('DEBUG',
                f"sybase_ase_connector: convert_trigger: Trigger {trigger_name}: {reason} - "
                f"the statement is not converted by the textual conversion: {' '.join(original.split())}")

        ## A column addressed through the pseudo table itself is a field of the record wherever
        ## it stands, and renaming those first leaves only the entries of a FROM clause to deal
        ## with - `select inserted.a from inserted` names the table twice, and the reference in
        ## the select list is not the one which has to leave a FROM clause.
        for pseudo_table, record in self.PSEUDO_TABLE_RECORDS.items():
            statement = self.substitute_outside_literals(
                statement, self.pseudo_table_pattern(pseudo_table) + r'\s*\.\s*', f'{record}.')

        ## every FROM clause listing a pseudo table, the innermost first - a statement can read
        ## both of them and can read one in a subquery of its own
        for _ in range(10):
            reference = self.find_pseudo_table_reference(statement)
            if reference is None:
                return statement

            masked, depths = reference['masked'], reference['depths']
            span = self.enclosing_query_span(masked, depths, reference['start'], reference['depth'])
            clauses = self.clause_positions(masked, depths, span, reference['depth'])

            from_clause = next((clause for clause in clauses if clause['keyword'] == 'FROM'), None)
            if from_clause is None or not from_clause['end'] <= reference['start'] < span[1]:
                refuse(f"the table {reference['name']} is read outside a FROM clause")
                break

            if any(clause['keyword'] in ('JOIN', 'GROUP BY', 'HAVING', 'UNION', 'INTERSECT', 'EXCEPT')
                   for clause in clauses):
                refuse(f"the query over {reference['name']} joins or groups its rows")
                break

            following = [clause for clause in clauses if clause['start'] > from_clause['end']]
            from_end = following[0]['start'] if following else span[1]
            from_list = statement[from_clause['end']:from_end]
            remaining, names = self.split_pseudo_table_from_list(from_list, reference['name'])
            if not names:
                refuse(f"the table {reference['name']} is not a plain entry of the FROM clause")
                break
            if len(names) > 2 or self.reads_pseudo_table(', '.join(remaining)):
                ## both pseudo tables listed together are a join of the two sets, and a column
                ## written without a qualifier cannot even be told apart
                refuse("the FROM clause lists more than one pseudo table")
                break

            record = reference['record']

            ## the parts of the query: what stands in front of its FROM clause, the condition of
            ## its WHERE clause and whatever follows that condition
            head = statement[span[0]:from_clause['start']]
            where_clause = next((clause for clause in following if clause['keyword'] == 'WHERE'), None)
            if where_clause is None:
                condition, trailing = '', statement[from_end:span[1]].strip()
            else:
                after_where = [clause for clause in following if clause['start'] > where_clause['end']]
                where_end = after_where[0]['start'] if after_where else span[1]
                condition = statement[where_clause['end']:where_end].strip()
                trailing = statement[where_end:span[1]].strip() if after_where else ''

            ## Only a SELECT reads the pseudo table alone. The target of an UPDATE or a DELETE is
            ## a table of its own, so a column written there without a qualifier is one of that
            ## table and has to be left as it stands - and so has the target of an INSERT, which
            ## stands in front of the SELECT reading the pseudo table.
            select_starts = [match.start() for match in re.finditer(r'(?i)\bSELECT\b', masked[span[0]:from_clause['start']])
                             if depths[span[0] + match.start()] == reference['depth']]
            head_split = select_starts[-1] if select_starts else len(head)
            sole_source = not remaining and bool(select_starts)

            head_prefix = self.qualify_pseudo_table_columns(head[:head_split], record, names, False)
            head_query = head[head_split:]
            if sole_source:
                head_query = self.unwrap_single_row_aggregates(head_query)
                condition = self.unwrap_single_row_aggregates(condition)
                if re.search(r'(?i)\bCOUNT\s*\(', head[head_split:] + condition):
                    self.config_parser.print_log_message('WARNING',
                        f"sybase_ase_connector: convert_trigger: Trigger {trigger_name} counts the rows of "
                        f"{reference['name']} - a trigger per row of PostgreSQL sees one row at a time, so the "
                        f"count is 1: {' '.join(original.split())}")
            head_query = self.qualify_pseudo_table_columns(head_query, record, names, sole_source)
            condition = self.qualify_pseudo_table_columns(condition, record, names, sole_source)

            if sole_source and re.match(r'(?i)^ORDER\s+BY\b', trailing):
                ## one row is in no order
                self.config_parser.print_log_message('DEBUG',
                    f"sybase_ase_connector: convert_trigger: Trigger {trigger_name}: the ORDER BY over "
                    f"{reference['name']} was dropped - the trigger reads one row: {' '.join(original.split())}")
                trailing = ''

            ## An EXISTS over the pseudo table asks whether the row of the trigger is the row it
            ## looks for, which is the condition of its subquery alone.
            ##
            ## Without a condition it asks whether the row exists at all, and that is not a
            ## constant: 'if not exists (select 1 from deleted)' is how Transact-SQL tells an
            ## INSERT from an UPDATE inside a trigger written for both. Reading it as TRUE or
            ## FALSE made the test decide the same way every time - the branch auditing an
            ## INSERT never ran and every row was audited as an UPDATE - so the presence of the
            ## record is asked about instead.
            exists = re.search(r'(?i)(\bNOT\s+)?\bEXISTS\s*$', statement[:max(span[0] - 1, 0)])
            if exists and sole_source and not trailing:
                negation = 'NOT ' if exists.group(1) else ''
                presence = self.PSEUDO_TABLE_PRESENCE[record]
                replacement = (f"{negation}({presence} AND ({condition}))" if condition
                               else f"{negation}({presence})")
                statement = statement[:exists.start()] + replacement + statement[span[1] + 1:]
                continue

            assignment = re.match(r'(?i)^\s*SELECT\s+(?:ALL\s+|DISTINCT\s+)?(@[\w@]+\s*=.*)$', head_query, re.DOTALL)
            if assignment and sole_source:
                counted = bool(re.search(r'(?i)\bCOUNT\s*\(', head[head_split:]))
                parts = [head_prefix.strip(),
                         'SELECT ' + self.rewrite_assignment_select(assignment.group(1), condition, counted),
                         trailing]
            else:
                parts = [head_prefix.strip(), head_query.strip(),
                         'FROM ' + ', '.join(remaining) if remaining else '',
                         f"WHERE {condition}" if condition else '',
                         trailing]

            query = ' '.join(' '.join(part.split()) for part in parts if part.strip())
            statement = statement[:span[0]] + query + statement[span[1]:]
        else:
            refuse("the statement reads the pseudo tables of a trigger in too many places")

        if self.reads_pseudo_table(statement):
            return None

        return statement

    ## The line which marks a statement the conversion could not express as a trigger per row.
    ## The orchestrator looks for it to keep the trigger out of the target - see
    ## trigger_needs_manual_adjustment().
    MANUAL_ADJUSTMENT_MARKER = 'MANUAL ADJUSTMENT REQUIRED'

    def validate_generated_body(self, body):
        """
        What is structurally wrong with a generated routine body, as a list of reasons.

        The conversion runs over the statements of a routine one at a time and cannot see that
        the statements it was given do not add up - a condition cut in half by the parser leaves
        a body whose parentheses or blocks do not close, and PostgreSQL refuses the whole
        routine with a syntax error which names a line and not a cause. Counting them here turns
        that into a reason the migration report can carry, and keeps the object from being
        created and counted as migrated.
        """
        ## Only the parentheses are counted. Counting BEGIN against END looks like the same
        ## kind of check and is not one: the END of a CASE expression, of an IF and of a loop
        ## close a statement rather than a block, the outermost BEGIN and END of the routine
        ## belong to the template around this body, and a body full of CASE expressions - the
        ## shape every converted assignment SELECT has - then reads as badly unbalanced while
        ## being perfectly correct. PostgreSQL itself is the check for everything else: the
        ## orchestrator creates the routine and reports what it says.
        reasons = []
        masked, _ = self.scan_sql_text(body)

        depth = 0
        for char in masked:
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
        if depth != 0:
            reasons.append("the parentheses of the converted body do not close - it has "
                           f"{abs(depth)} {'unclosed opening' if depth > 0 else 'closing'} "
                           "parenthesis(es) too many, which means a statement was cut in half "
                           "before it was converted")

        return reasons

    def trigger_needs_manual_adjustment(self, converted_code):
        """
        Whether a converted trigger carries a statement which has to be completed by hand.

        The marker is written into the code itself so that it travels with it - the protocol
        table keeps the code, and a reader of the code alone sees the same thing the migration
        report says.
        """
        return bool(converted_code) and self.MANUAL_ADJUSTMENT_MARKER in converted_code

    def trigger_manual_adjustment_details(self, converted_code):
        """ The reasons the conversion left in the head of the code, for the migration report. """
        if not self.trigger_needs_manual_adjustment(converted_code):
            return None
        reasons = re.findall(r'^\s+- (.*)$', converted_code, re.MULTILINE)
        return '; '.join(reason.strip() for reason in reasons) or 'see the code of the trigger'

    def convert_trigger_pseudo_tables_statement(self, statement, command_kind, trigger_name, refusals):
        """
        One statement of a trigger reading the pseudo tables, converted or marked for the reader.

        The parsed conversion is the one which handles the shapes a trigger really uses - the
        pseudo table beside a real table, both of them in one statement, the FROM clause of a
        DELETE or an UPDATE. The textual conversion is kept behind it for a statement no parser
        reads. A statement neither of them converts is kept exactly as it was written: it is
        never replaced by one which does nothing, because the trigger would then be created and
        counted as migrated while silently doing less than the trigger of the source did.
        """
        if not self.reads_pseudo_table(statement):
            return statement

        converted = self.convert_trigger_pseudo_tables_parsed(statement, trigger_name)
        if converted is not None:
            self.config_parser.print_log_message('DEBUG',
                f"sybase_ase_connector: convert_trigger: Trigger {trigger_name}: pseudo tables converted: "
                f"{' '.join(statement.split())} -> {' '.join(converted.split())}")
            return converted

        converted = self.convert_trigger_pseudo_tables(statement, command_kind, trigger_name, refusals)
        if converted is not None:
            self.config_parser.print_log_message('DEBUG',
                f"sybase_ase_connector: convert_trigger: Trigger {trigger_name}: pseudo tables converted "
                f"by the textual conversion: {' '.join(statement.split())} -> {' '.join(converted.split())}")
            return converted

        single_line = ' '.join(statement.split())
        refusals.append(single_line)
        self.config_parser.print_log_message('WARNING',
            f"sybase_ase_connector: convert_trigger: Trigger {trigger_name}: the statement reads "
            f"'inserted' or 'deleted' in a way a trigger per row of PostgreSQL cannot express. It is "
            f"kept in the converted code as it was written and has to be completed by hand - the "
            f"trigger is reported as failed and is NOT created in the target: {single_line}")

        ## The statement stays exactly as the source wrote it, behind a comment naming what has
        ## to be done. It does not become a statement which does nothing: the reader of the code
        ## has to find the place, and PostgreSQL has to refuse it if anybody tries to create it.
        return (f"/* {self.MANUAL_ADJUSTMENT_MARKER} - the statement below reads 'inserted' or "
                f"'deleted' as a set of rows, which a trigger FOR EACH ROW cannot do. "
                f"Rewrite it by hand before creating this trigger. */\n{statement}")

    def convert_trigger(self, settings):
        """
        Parser-based conversion for triggers (V2).
        Returns full DDL (CREATE FUNCTION + CREATE TRIGGER).
        """
        trigger_code = settings['trigger_sql']
        trigger_name = self.config_parser.convert_names_case(settings['trigger_name'])
        target_schema_name = settings['target_schema_name']
        target_table_name = self.config_parser.convert_names_case(settings['target_table_name'])
        target_db_type = settings['target_db_type']

        # --- Pre-processing ---

        # 0. Encapsulate comments
        trigger_code = self.encapsulate_line_comments(trigger_code)

        # 1. Remove GO
        trigger_code = re.sub(r'\bGO\b', '', trigger_code, flags=re.IGNORECASE)

        # Pre-process Sybase specific join syntax (Missing in original V2)
        # *= -> = /* left_outer */
        # =* -> = /* right_outer */
        trigger_code = re.sub(r'\*=', '= /* left_outer */', trigger_code)
        trigger_code = re.sub(r'=\*', '= /* right_outer */', trigger_code)

        # 1.5 Rename Local Variables (@var -> locvar_var)
        # Handle natively in Parser Pass 9
        # trigger_code = self._rename_sybase_local_variables(trigger_code)

        # 1.6 Handle Global Variables
        has_rowcount = '@@rowcount' in trigger_code.lower()

        ## @@rowcount of a trigger is the number of rows the statement which fired it changed, and
        ## this trigger fires once per row - the parser cannot read that out of a statement, so it
        ## is replaced here and declared below. Everything else Sybase offers as a global variable
        ## is converted like it is for a routine, which also declares what it replaced them with:
        ## '@@error' became a variable nothing declared and the trigger function was refused with
        ## 'column "locvar_error_placeholder" does not exist'.
        trigger_code = re.sub(r'@@rowcount\b', 'locvar_rowcount', trigger_code, flags=re.IGNORECASE)
        trigger_code, global_variable_declarations = self.convert_sybase_global_variables(trigger_code, trigger_name)

        # 2. Extract Body (After AS)
        as_match = re.search(r'\bAS\b', trigger_code, flags=re.IGNORECASE)
        body_content = trigger_code
        if as_match:
             body_content = trigger_code[as_match.end():].strip()

        # 3. Global Replacements specific to Triggers
        ## The pseudo tables 'inserted' and 'deleted' are rewritten by the parser, one statement
        ## at a time - see convert_trigger_pseudo_tables - because a FROM clause, the columns
        ## belonging to it and the condition selecting its rows have to be seen together.
        pseudo_table_refusals = []
        pseudo_tables_read = [pseudo_table for pseudo_table in self.PSEUDO_TABLE_RECORDS
                              if self.reads_pseudo_table(body_content, pseudo_table)]
        if pseudo_tables_read:
            self.config_parser.print_log_message('DEBUG',
                f"sybase_ase_connector: convert_trigger: Trigger {trigger_name} reads "
                f"{', '.join(pseudo_tables_read)} - converted to the records "
                f"{', '.join(self.PSEUDO_TABLE_RECORDS[pseudo_table] for pseudo_table in pseudo_tables_read)} "
                "of the trigger function.")

        # IF UPDATE(col) -> IF locvar_sybase_update_func(col) (To avoid parser confusion with UPDATE keyword)
        def if_update_replacer(match):
            col = match.group(1)
            # return f"NEW.{col} IS DISTINCT FROM OLD.{col}"
            return f"locvar_sybase_update_func({col})"
        body_content = re.sub(r'\bUPDATE\(([a-zA-Z0-9_]+)\)', if_update_replacer, body_content, flags=re.IGNORECASE)

        # 4. Extract Declarations (Ported from convert_funcproc_code_v2)
        declarations = []
        local_settings = settings.copy() if settings else {}
        local_settings['target_db_type'] = target_db_type
        types_mapping = self.get_types_mapping(local_settings)

        declaration_replacer = lambda m: self._declaration_replacer(m, settings, types_mapping, declarations)

        # Expanded lookahead for declaration end
        ## The declaration of a cursor carries the query of the cursor, and this reads only up to
        ## the SELECT of it: 'declare c1 cursor for select id from t' was taken as the declaration
        ## 'c1 cursor for' and left its query standing in the body as a SELECT of its own, which
        ## has nowhere to put its rows. The parser reads a cursor as a whole - see its Pass 3c -
        ## so a line declaring one is left to it.
        body_content = re.sub(r'DECLARE\s+(?![@#])(?![^\n]*\bCURSOR\b)[a-zA-Z0-9_].*?(?=\bBEGIN\b|\bEND\b|\bIF\b|\bWHILE\b|\bSELECT\b|\bINSERT\b|\bUPDATE\b|\bDELETE\b|\bRETURN\b|\bSET\b|\bFETCH\b|\bOPEN\b|\bCLOSE\b|\bDEALLOCATE\b|\bDECLARE\b|\bEXEC\b|\bEXECUTE\b|\bPRINT\b|\bRAISERROR\b|\bWAITFOR\b|\bCOMMIT\b|\bROLLBACK\b|\bSAVE\b|$)', declaration_replacer, body_content, flags=re.IGNORECASE | re.DOTALL)

        # 5. Convert Statements (using _convert_stmts logic)
        # 5. Convert Statements using new 12-pass Parser
        # We wrap the body_content in a dummy header so the parser triggers properly
        fake_code = f"CREATE PROCEDURE dummy AS\n{body_content}"
        
        fake_code = self._apply_types_mapping(fake_code, types_mapping)
            
        pseudo_table_converter = lambda statement, command_kind: self.convert_trigger_pseudo_tables_statement(
            statement, command_kind, trigger_name, pseudo_table_refusals)

        ## the text of a message a RAISERROR names by its number
        settings['user_messages'] = self.fetch_user_messages()
        parser = TsqlParser(fake_code, self.config_parser, view_converter=self.convert_view_code, settings=settings, functions_mapping_converter=self.apply_sql_functions_mapping, pseudo_table_converter=pseudo_table_converter)
        final_output = parser.run(pg_header_str=" ") # space prevents default header

        final_stmts_clean = []
        in_body = False
        first_begin_found = False
        indent_level = 0

        def get_indent(level):
            return "    " * max(0, level)

        for line_obj in final_output:
            stripped = line_obj.content.strip()
            if not stripped: continue
            if stripped in ('$$', '$$ LANGUAGE PLPGSQL;', '$$ LANGUAGE plpgsql;'): continue
            if line_obj.source_array == "header": continue

            if line_obj.source_array == "declare_section" or stripped.upper() == "DECLARE":
                in_body = True
                continue

            ## A cursor is declared in the DECLARE section of PL/pgSQL just as a variable is -
            ## 'c1 CURSOR FOR SELECT ...' was left in the body, where PostgreSQL has no statement
            ## it could be. Its query is not a declaration of a type, so it keeps its own names.
            if line_obj.source_array == "cursor_declaration":
                declarations.append(stripped)
                continue

            if line_obj.source_array == "variable_declaration" or stripped.upper().startswith("DECLARE "):
                decl_str = self._quote_udts_in_declaration(stripped, settings)
                declarations.append(decl_str)
                continue

            if re.match(r'^BEGIN\b', stripped, re.IGNORECASE):
                if not first_begin_found:
                    first_begin_found = True
                    # Skip the outermost BEGIN, as pg_func template provides it
                    continue
                else:
                    indent_level += 1
            elif re.match(r'^END;', stripped, re.IGNORECASE):
                if indent_level == 0:
                    # Skip the outermost END;, as pg_func template provides it
                    continue
                indent_level -= 1

            final_stmts_clean.append(get_indent(indent_level) + line_obj.content)

        if has_rowcount:
             ## @@rowcount of a trigger of Sybase ASE is the number of rows the statement which
             ## fired it changed, and the regular use of it is the `if @@rowcount = 0 return` at
             ## the head of the trigger. This trigger fires per row, so that number is 1 - the
             ## variable was declared without a value, which made every test on it read NULL.
             declarations.insert(0, "locvar_rowcount INTEGER DEFAULT 1; /* @@rowcount - this trigger fires once per row */")

        for declaration in reversed(global_variable_declarations):
             if not any(declaration.split()[0] in existing for existing in declarations):
                  declarations.insert(0, declaration)

        final_body = "\n".join(final_stmts_clean)

        # Post-processing: Replace locvar_sybase_update_func(col) with NEW.col IS DISTINCT FROM OLD.col
        # This handles the result of if_update_replacer after parsing/generation
        def update_func_replacer(match):
             # match group 1: optional quote
             # match group 2: column name
             # match group 3: optional quote (should match group 1)
             # Use simplified regex since we know how sqlglot generates output (likely quoted)

             # Regex to capture: locvar_sybase_update_func( "col" ) or ( col )
             content = match.group(1)
             return f"NEW.{content} IS DISTINCT FROM OLD.{content}"

        final_body = re.sub(r'locvar_sybase_update_func\((.*?)\)', update_func_replacer, final_body, flags=re.IGNORECASE)

        ## A pseudo table left in a place the statement by statement conversion never reads - the
        ## query of a cursor, an EXECUTE - would reach the target and fail there with
        ## 'relation "inserted" does not exist'. The literals and comments are masked out first,
        ## because the refusals above name the pseudo tables in the comments they leave behind.
        converted_code = "\n".join(declarations) + "\n" + final_body
        surviving = [pseudo_table for pseudo_table in self.PSEUDO_TABLE_RECORDS
                     if self.reads_pseudo_table(converted_code, pseudo_table)]
        if surviving:
            pseudo_table_refusals.append(
                f"the table(s) {', '.join(surviving)} are still read by the converted code")
            self.config_parser.print_log_message('WARNING',
                f"sybase_ase_connector: convert_trigger: Trigger {trigger_name} still reads "
                f"{', '.join(surviving)} after the conversion - PostgreSQL has no such table and will "
                "refuse the trigger function. The statement reading it has to be written by hand.")

        ## A body which does not hold together is not a conversion of anything - it is the
        ## result of a statement the parser could not keep in one piece. PostgreSQL would refuse
        ## it, so the trigger is reported the same way a refused statement is.
        for reason in self.validate_generated_body(final_body):
            pseudo_table_refusals.append(reason)
            self.config_parser.print_log_message('WARNING',
                f"sybase_ase_connector: convert_trigger: Trigger {trigger_name}: {reason}. The trigger "
                "is NOT created in the target and is reported as failed.")

        ## A statement which needs the whole set of rows of a pseudo table was kept as the source
        ## wrote it and has to be rewritten by hand. The trigger is not the trigger of the source
        ## until that is done, so it is not created in the target and is reported as failed - see
        ## the marker at its head, which the orchestrator reads.
        if pseudo_table_refusals:
            self.config_parser.print_log_message('WARNING',
                f"sybase_ase_connector: convert_trigger: Trigger {trigger_name} has "
                f"{len(pseudo_table_refusals)} statement(s) which could not be converted. The trigger "
                "is NOT created in the target and is reported as failed - the statements are kept in "
                "the stored code as they were written, for the migration by hand.")
            final_body = (f"/* {self.MANUAL_ADJUSTMENT_MARKER} - this trigger is NOT usable as it stands.\n"
                          "   The following statement(s) read the rows of 'inserted' or 'deleted' as a set,\n"
                          "   which a trigger FOR EACH ROW of PostgreSQL cannot do. They were left in the code\n"
                          "   below exactly as Sybase ASE wrote them and have to be rewritten by hand:\n"
                          + "".join(f"     - {refusal}\n" for refusal in pseudo_table_refusals)
                          + "*/\n" + final_body)

        # 6. Event Extraction
        events = re.findall(r'for\s+([a-z, ]+?)(?:\s+as\b|$)', trigger_code, re.IGNORECASE)
        pg_events = "INSERT OR UPDATE OR DELETE"
        event_list = ['INSERT', 'UPDATE', 'DELETE']
        if events:
             event_list = events[0].replace(' ', '').upper().split(',')
             pg_events = ' OR '.join(event_list)

        ## A trigger fired by a DELETE has no NEW row - the row which was deleted is OLD, and
        ## that is the one such a trigger returns. The value is used by a BEFORE trigger and
        ## ignored by an AFTER one, but 'RETURN NEW' in a DELETE trigger is wrong either way.
        returned_record = 'OLD' if event_list == ['DELETE'] else 'NEW'

        ## The records of the trigger are written by the conversion in upper case, and the
        ## PostgreSQL generator of sqlglot quotes an identifier to preserve that case. A quoted
        ## "OLD" is not the record OLD of PL/pgSQL, which resolves the name in lower case, so
        ## the quotes are removed again.
        final_body = re.sub(r'"(OLD|NEW)"(\s*\.)', r'\1\2', final_body)
        final_body = re.sub(r'"(OLD|NEW)"', r'\1', final_body)
        final_body = self.unquote_local_variables(final_body)

        # A plain RETURN of Transact-SQL leaves the trigger and is written without a value.
        # A trigger function of PL/pgSQL has to return a row, and PostgreSQL refuses a RETURN
        # without an expression with 'missing expression at or near ";"'. It returns the same
        # row the function returns at its end, which leaves the trigger in the same way.
        final_body, plain_returns = re.subn(r'(?i)\bRETURN\s*;', f'RETURN {returned_record};', final_body)
        if plain_returns:
            self.config_parser.print_log_message('DEBUG',
                f"sybase_ase_connector: convert_trigger: Trigger {trigger_name}: {plain_returns} plain RETURN statement(s) converted to 'RETURN {returned_record}' - a trigger function of PostgreSQL cannot return without a value.")

        # 7. Assemble DDL
        pg_func = f"""CREATE OR REPLACE FUNCTION "{target_schema_name}"."{trigger_name}_func"()
RETURNS trigger AS $$
DECLARE
{chr(10).join(declarations)}
BEGIN
{final_body}
RETURN {returned_record};
END;
$$ LANGUAGE plpgsql;
"""

        pg_trigger = f"""CREATE TRIGGER "{trigger_name}"
AFTER {pg_events} ON "{target_schema_name}"."{target_table_name}"
FOR EACH ROW
EXECUTE FUNCTION "{target_schema_name}"."{trigger_name}_func"();
"""
        return pg_func + '\n' + pg_trigger

    # def convert_trigger_v1(self, settings):
    #     trigger_name = self.config_parser.convert_names_case(settings['trigger_name'])
    #     trigger_code = settings['trigger_sql']

    #     # 0. Encapsulate comments
    #     # Convert -- comment to /* comment */ to prevent breaking code
    #     trigger_code = re.sub(r'--([^\n]*)', r'/*\1*/', trigger_code)

    #     target_schema_name = settings['target_schema_name']
    #     target_table_name = self.config_parser.convert_names_case(settings['target_table_name'])
    #     target_db_type = settings['target_db_type']

    #     # 1. Basic Cleanup
    #     converted_code = re.sub(r'\bGO\b', '', trigger_code, flags=re.IGNORECASE)

    #     # 2. Extract Body (After AS)
    #     # Pattern: CREATE TRIGGER ... AS [BEGIN] ... [END]
    #     # or ... FOR INSERT AS ...
    #     as_match = re.search(r'\bAS\b', converted_code, flags=re.IGNORECASE)
    #     if as_match:
    #         body_content = converted_code[as_match.end():].strip()
    #     else:
    #         body_content = converted_code # Fallback?

    #     # Remove outer BEGIN/END if present
    #     if re.match(r'^BEGIN\b', body_content, flags=re.IGNORECASE):
    #         body_content = re.sub(r'^BEGIN', '', body_content, count=1, flags=re.IGNORECASE).strip()
    #         body_content = re.sub(r'END\s*$', '', body_content, flags=re.IGNORECASE).strip()

    #     # 3. Variable Declarations
    #     types_mapping = self.get_types_mapping({'target_db_type': target_db_type})
    #     declarations = []

    #     # Pre-process: Rename specific conflicting variables globally before stripping @
    #     # e.g. @date -> @v_date (Keep @ so DECLARE regex matches it)
    #     body_content = re.sub(r'@date\b', '@v_date', body_content, flags=re.IGNORECASE)

    #     def declaration_replacer(match):
    #         full_decl = match.group(0)
    #         content = full_decl[7:].strip() # len('DECLARE') = 7

    #         content_clean = content.replace('@', '')
    #         # Custom type substitutions first
    #         content_clean = self._apply_data_type_substitutions(content_clean)
    #         content_clean = self._apply_udt_to_base_type_substitutions(content_clean, settings)
    #         for sybase_type, pg_type in types_mapping.items():
    #             content_clean = re.sub(rf'\b{re.escape(sybase_type)}\b', pg_type, content_clean, flags=re.IGNORECASE)

    #         parts = self._split_respecting_parens(content_clean)
    #         for part in parts:
    #             declarations.append(part.strip() + ';')

    #         return '' # Remove from body

    #     self.config_parser.print_log_message('DEBUG', "sybase_ase_connector: update_func_replacer: Starting variable declaration extraction...")
    #     body_content = re.sub(r'DECLARE\s+@.*?(?=\bBEGIN\b|\bIF\b|\bWHILE\b|\bSELECT\b|\bINSERT\b|\bUPDATE\b|\bDELETE\b|\bRETURN\b|\bSET\b|\bFETCH\b|\bOPEN\b|\bCLOSE\b|\bDEALLOCATE\b|\bDECLARE\b|$)', declaration_replacer, body_content, flags=re.IGNORECASE | re.DOTALL)
    #     self.config_parser.print_log_message('DEBUG', "sybase_ase_connector: update_func_replacer: Variable declaration extraction complete.")

    #     # 4. Global Replacements
    #     # Functions
    #     function_map = self.get_sql_functions_mapping({ 'target_db_type': target_db_type })
    #     for sybase_func, pg_equiv in function_map.items():
    #         escaped_src_func = re.escape(sybase_func)
    #         body_content = re.sub(escaped_src_func, pg_equiv, body_content, flags=re.IGNORECASE)

    #     # Type substitutions in body
    #     self.config_parser.print_log_message('DEBUG', "sybase_ase_connector: update_func_replacer: Starting global type substitutions...")
    #     body_content = self._apply_data_type_substitutions(body_content)
    #     body_content = self._apply_udt_to_base_type_substitutions(body_content, settings)
    #     for sybase_type, pg_type in types_mapping.items():
    #         body_content = re.sub(rf'\b{re.escape(sybase_type)}\b', pg_type, body_content, flags=re.IGNORECASE)

    #     # Remove @
    #     body_content = re.sub(r'(?<!@)@([a-zA-Z0-9_]+)', r'\1', body_content)

    #     # INSERTED/DELETED -> NEW/OLD
    #     body_content = re.sub(r'\binserted\b', 'NEW', body_content, flags=re.IGNORECASE)
    #     body_content = re.sub(r'\bdeleted\b', 'OLD', body_content, flags=re.IGNORECASE)

    #     # Sybase Specific Cleanups
    #     # @@trancount -> 1 (Assume transaction active)
    #     body_content = re.sub(r'@@trancount', '1', body_content, flags=re.IGNORECASE)
    #     # Remove SET chained/transaction commands
    #     body_content = re.sub(r'SET\s+chained\s+\w+', '', body_content, flags=re.IGNORECASE)
    #     body_content = re.sub(r'SET\s+transaction\s+isolation\s+level\s+\d+', '', body_content, flags=re.IGNORECASE)

    #     # PRINT -> RAISE NOTICE
    #     def print_replacer(match):
    #          content = match.group(1).strip()
    #          args = self._split_respecting_parens(content)
    #          if not args:
    #               return "RAISE NOTICE '';"
    #          first_arg = args[0]
    #          rest_args = args[1:]
    #          if first_arg.startswith("'") and first_arg.endswith("'"):
    #               msg = first_arg
    #               if rest_args:
    #                    return f"RAISE NOTICE {msg}, {', '.join(rest_args)};"
    #               else:
    #                    return f"RAISE NOTICE {msg};"
    #          else:
    #               format_str = ", ".join(["%"] * len(args))
    #               return f"RAISE NOTICE '{format_str}', {', '.join(args)};"

    #     body_content = re.sub(r'print\s+(.+?)(?=;|\n|$)', print_replacer, body_content, flags=re.IGNORECASE)

    #     # 5. Assignments and Selects

    #     # Select Into
    #     def select_into_transformer(match):
    #         content = match.group(1)
    #         rest = match.group(2)

    #         # Clean up FROM NEW/OLD in rest
    #         # If rest contains "FROM NEW" or "FROM OLD", we strip it if it's the only thing or logic suggests.
    #         from_match = re.search(r'FROM\s+(.*?)(?:\bWHERE\b|\bGROUP\b|\bORDER\b|$)', rest, re.IGNORECASE)
    #         if from_match:
    #             table_list = from_match.group(1)
    #             # Remove comments
    #             table_list = re.sub(r'--.*', '', table_list)
    #             table_list = re.sub(r'/\*.*?\*/', '', table_list, flags=re.DOTALL)

    #             tables = self._split_respecting_parens(table_list)
    #             clean_tables = []
    #             for t in tables:
    #                 t_clean = t.strip()
    #                 if not t_clean:
    #                     continue
    #                 # Check first word (table name) against keywords
    #                 # NEW alias -> NEW. NEW -> NEW.
    #                 first_word = t_clean.split()[0].upper()
    #                 if first_word not in ('NEW', 'OLD', 'INSERTED', 'DELETED'):
    #                     clean_tables.append(t)

    #             if not clean_tables:
    #                # No tables left, remove FROM clause entirely
    #                start, end = from_match.span()
    #                # Simply remove the match range from rest
    #                rest = rest[:start] + rest[end:]

    #         if '=' in content:
    #             parts = self._split_respecting_parens(content)
    #             vars_list = []
    #             cols_list = []
    #             for asm in parts:
    #                 if '=' in asm:
    #                     side_l, side_r = asm.split('=', 1)
    #                     vars_list.append(side_l.strip())
    #                     cols_list.append(side_r.strip())
    #                 else:
    #                     cols_list.append(asm)
    #             if vars_list:
    #                 return f"SELECT {', '.join(cols_list)} INTO {', '.join(vars_list)} {rest}"
    #         return match.group(0)

    #     # Regex must ensure we don't cross statement boundaries (UPDATE, INSERT, etc.)
    #     # Added SELECT to lookahead to prevent merging multiple SELECTs
    #     # Also constrained matches after FROM to statement boundaries
    #     body_content = re.sub(r'SELECT\s+((?:(?!\b(?:UPDATE|INSERT|DELETE|IF|WHILE|RETURN|BEGIN|END|SELECT)\b).)+?)\s+(FROM\s+(?:(?!\b(?:UPDATE|INSERT|DELETE|IF|WHILE|RETURN|BEGIN|END|SELECT)\b).)+)', select_into_transformer, body_content, flags=re.IGNORECASE | re.DOTALL)

    #     # Cleanup: Remove FROM NEW/OLD from SELECT statements if they persist
    #     # e.g. "SELECT ... INTO ... FROM NEW" -> "SELECT ... INTO ..."
    #     body_content = re.sub(r'(SELECT\s+[^;]+?)\s+FROM\s+(?:NEW|OLD|INSERTED|DELETED)\b', r'\1', body_content, flags=re.IGNORECASE | re.DOTALL)

    #     # Simple Assignments
    #     def simple_assignment(match):
    #         full_match = match.group(0)
    #         if 'FROM' in full_match.upper():
    #             return full_match
    #         content = match.group(1).strip()
    #         if '=' not in content:
    #             return full_match

    #         parts = self._split_respecting_parens(content)
    #         assignments = []
    #         is_assignment = True
    #         for part in parts:
    #             if '=' in part:
    #                 side_l, side_r = part.split('=', 1)
    #                 assignments.append(f"{side_l.strip()} := {side_r.strip()}")
    #             else:
    #                 is_assignment = False

    #         if is_assignment and assignments:
    #             return "; ".join(assignments) + ";"
    #         return full_match

    #     body_content = re.sub(r'SELECT\s+([^;\n]+)', simple_assignment, body_content, flags=re.IGNORECASE)

    #     # 6. UPDATE ... FROM fixes
    #     # If UPDATE target ... FROM target, table2 -> FROM table2
    #     # Need to parse UPDATE target

    #     def update_from_fix(match):
    #         target = match.group(1)
    #         set_clause = match.group(2)
    #         from_clause = match.group(3)
    #         rest = match.group(4)

    #         # Parse FROM tables
    #         # Sybase FROM t1, t2
    #         # PG FROM t2 (if t1 is target)
    #         tables = self._split_respecting_parens(from_clause)
    #         new_tables = []
    #         for t in tables:
    #             t_clean = t.strip()
    #             # Check alias? "table alias" or "table AS alias"
    #             # If target matches table name or alias
    #             # Simplifying: check if target string is contained
    #             # Remove target table
    #             if target.lower() == t_clean.lower():
    #                 continue # Skip target
    #             # Check for "target alias"
    #             if t_clean.lower().startswith(target.lower() + ' ') or t_clean.lower().startswith(target.lower() + '\t'):
    #                  continue

    #             # Also remove NEW and OLD from FROM clause in triggers
    #             if t_clean.upper() in ('NEW', 'OLD', 'INSERTED', 'DELETED'):
    #                 continue

    #             new_tables.append(t)

    #         if new_tables:
    #             return f"UPDATE {target} {set_clause} FROM {', '.join(new_tables)} {rest}"
    #         else:
    #             # If no tables left (self update only), remove FROM
    #             return f"UPDATE {target} {set_clause} {rest}"

    #     # Regex: UPDATE target SET ... FROM ... [WHERE...]
    #     # Be careful matching SET ... FROM
    #     body_content = re.sub(r'UPDATE\s+([a-zA-Z0-9_]+)\s+(SET\s+.*?)\s+FROM\s+(.*?)(\bWHERE\b|\bGROUP\b|\bORDER\b|$)', update_from_fix, body_content, flags=re.IGNORECASE | re.DOTALL)

    #     # 7. String Concatenation Fix (+ -> ||)
    #     # Heuristic: '...' + ... or ... + '...'
    #     body_content = re.sub(r"('\s*)\+\s*", r"\1 || ", body_content)
    #     body_content = re.sub(r"\s*\+\s*(')", r" || \1", body_content)

    #     # 8. Control Flow (IF/WHILE) - Minimal support as per trigger usage

    #     # IF UPDATE(column) -> IF NEW.column IS DISTINCT FROM OLD.column
    #     # Needs to happen before IF regex
    #     def if_update_replacer(match):
    #         col = match.group(1)
    #         # Sybase: IF UPDATE(col)
    #         # PG: IF NEW.col IS DISTINCT FROM OLD.col
    #         return f"IF NEW.{col} IS DISTINCT FROM OLD.{col}"

    #     body_content = re.sub(r'IF\s+UPDATE\(([\w]+)\)', if_update_replacer, body_content, flags=re.IGNORECASE)

    #     # Rollback Trigger
    #     # rollback trigger [with raiserror number 'message']
    #     # rollback transaction ...
    #     # Replace with RAISE EXCEPTION
    #     def rollback_replacer(match):
    #         # Try to capture message if present
    #         # Pattern: rollback trigger with raiserror 99999 'Message'
    #         rest = match.group(1) if match.lastindex >= 1 else ''
    #         message = "Trigger Rollback"

    #         # Extract message string '...'
    #         msg_match = re.search(r"'([^']+)'", rest)
    #         if msg_match:
    #             message = msg_match.group(1)

    #         return f"RAISE EXCEPTION '{message}';"

    #     body_content = re.sub(r'rollback\s+(?:trigger|transaction)\s*(.*)', rollback_replacer, body_content, flags=re.IGNORECASE)

    #     # FIX: Ensure semicolon before ELSE/ELSIF if missing
    #     # Regex updated to handle comments
    #     body_content = re.sub(r'([^;\s])([ \t]*(?:--[^\n]*|/\*.*?\*/[ \t]*)?)\n\s*(ELSE|ELSIF)\b', r'\1;\2\n\3', body_content, flags=re.IGNORECASE)

    #     # ELSE IF -> ELSIF
    #     body_content = re.sub(r'ELSE\s+IF', 'ELSIF', body_content, flags=re.IGNORECASE)

    #     # IF replacement with DOTALL support for multiline conditions
    #     body_content = re.sub(r'IF\s+(.*?)\s+BEGIN', r'IF \1 THEN', body_content, flags=re.IGNORECASE | re.DOTALL)

    #     # Standardize other keywords
    #     body_content = re.sub(r'WHILE\s+(.*?)\s+BEGIN', r'WHILE \1 LOOP', body_content, flags=re.IGNORECASE)
    #     body_content = re.sub(r'ELSE\s+BEGIN', r'ELSE', body_content, flags=re.IGNORECASE)
    #     body_content = re.sub(r'END\s*;?\s+ELSE', r'ELSE', body_content, flags=re.IGNORECASE)


    #     # END replacement (simple approach for now, triggers usually simple)
    #     # But we stripped outer END. Inner ENDs need closure.
    #     # If we replaced BEGIN with THEN/LOOP, we need END IF/LOOP.
    #     # Let's use the stack logic if we really want to be safe, or just END IF for triggers?
    #     # User trigger has IF?
    #     # "IF UPDATE(column)" was handled in old code.
    #     body_content = re.sub(r'if\s+UPDATE\([a-zA-Z_]+\)', '-- IF UPDATE(column) not supported', body_content, flags=re.IGNORECASE)

    #     # Fix inner ENDs
    #     # Reuse logic from convert_funcproc_code manually or simplified
    #     lines = body_content.split('\n')
    #     new_lines = []
    #     stack = []
    #     for line in lines:
    #         stripped = line.strip()
    #         if re.search(r'IF\s+.*\s+THEN', line, flags=re.IGNORECASE):
    #             stack.append('IF')
    #         elif re.search(r'WHILE\s+.*\s+LOOP', line, flags=re.IGNORECASE):
    #             stack.append('LOOP')

    #         if re.match(r'^END\s*;?$', stripped, flags=re.IGNORECASE):
    #             if stack:
    #                 block = stack.pop()
    #                 if block == 'IF':
    #                     new_lines.append("END IF;")
    #                 elif block == 'LOOP':
    #                     new_lines.append("END LOOP;")
    #                 else:
    #                     new_lines.append('END;')
    #             else:
    #                 new_lines.append('END;') # Should not happen if outer stripped
    #         else:
    #             new_lines.append(line)
    #     body_content = '\n'.join(new_lines)

    #     # 9. Semicolon Heuristic (Ensure statements end with ;)
    #     lines = body_content.split('\n')
    #     final_lines = []
    #     statement_buffer = []

    #     def flush_buffer(buf):
    #         if not buf: return

    #         # Find last non-empty line index
    #         last_idx = -1
    #         for i in range(len(buf) - 1, -1, -1):
    #             line_stripped = buf[i].strip()
    #             if line_stripped:
    #                 # Skip comments
    #                 if line_stripped.startswith('/*') or line_stripped.startswith('--'):
    #                     continue
    #                 last_idx = i
    #                 break

    #         if last_idx != -1:
    #             s = buf[last_idx].rstrip()

    #             # Remove trailing comments for check (to avoid adding ; to BEGIN -- comment)
    #             s_code = re.sub(r'--.*', '', s)
    #             s_code = re.sub(r'/\*.*?\*/', '', s_code, flags=re.DOTALL)
    #             s_code = s_code.strip()

    #             # Don't add if ends with ; or block openers/closers that don't need it
    #             ignore_ends = ('BEGIN', 'THEN', 'LOOP', 'ELSE', ';')
    #             if not s_code.upper().endswith(ignore_ends):
    #                 # Check if it started with a command that needs ;
    #                 combined = " ".join([b.strip() for b in buf]) # Join all lines to check full start
    #                 # Remove comments from check
    #                 combined_code = re.sub(r'--.*', '', combined).strip()

    #                 first_word = combined_code.split()[0].upper()

    #                 # Also check assignments "var := val"
    #                 # Remove strings to avoid false positives like IF x = ':='
    #                 combined_no_strings = re.sub(r"'.*?'", '', combined_code)
    #                 is_assignment = ':=' in combined_no_strings

    #                 # Keywords requiring semicolon
    #                 needs_semi = ('UPDATE', 'INSERT', 'DELETE', 'SELECT', 'PERFORM', 'CALL', 'WITH', 'MERGE', 'RAISE')

    #                 if first_word in needs_semi or is_assignment:
    #                      buf[last_idx] = s + ';'

    #         final_lines.extend(buf)

    #     for line in lines:
    #         stripped = line.strip()
    #         # Start of new statement?
    #         is_start = False
    #         # Check forkeywords that start statements
    #         if re.match(r'^(UPDATE|INSERT|DELETE|SELECT|IF|WHILE|RETURN|END|DECLARE|BEGIN|RAISE)\b', stripped, re.IGNORECASE):
    #             is_start = True
    #         elif ':=' in line: # Assignment line?
    #              is_start = True

    #         if is_start:
    #              flush_buffer(statement_buffer)
    #              statement_buffer = [line]
    #         else:
    #              statement_buffer.append(line)

    #     flush_buffer(statement_buffer)
    #     body_content = '\n'.join(final_lines)

    #     # 10. Return
    #     # Only replace RETURN word boundary.
    #     # Handle RETURN result? Sybase triggers don't typically return values like functions, but RETURN without args exits.
    #     # If RETURN 1 or RETURN @var, we might need to be careful.
    #     # For now, converting standalone RETURN to RETURN NEW;
    #     body_content = re.sub(r'\bRETURN\b', 'RETURN NEW;', body_content, flags=re.IGNORECASE)
    #     # If RETURN NEW; NEW; (double) -> fix
    #     body_content = body_content.replace('RETURN NEW; NEW;', 'RETURN NEW;')

    #     # Event parsing
    #     events = re.findall(r'for\s+([a-z, ]+?)(?:\s+as\b|$)', trigger_code, re.IGNORECASE)
    #     events = events[0].replace(' ', '').upper().split(',') if events else []
    #     pg_events = ' OR '.join(events)

    #     # Assemble
    #     pg_func = f"""CREATE OR REPLACE FUNCTION {trigger_name}_func()
    #         RETURNS trigger AS $$
    #         DECLARE
    #         {chr(10).join(declarations)}
    #         BEGIN
    #         {body_content.strip()}
    #         RETURN NEW;
    #         END;
    #         $$ LANGUAGE plpgsql;
    #         """

    #     pg_trigger = f"""CREATE TRIGGER {trigger_name}
    #         AFTER {pg_events} ON "{target_schema_name}"."{target_table_name}"
    #         FOR EACH ROW
    #         EXECUTE FUNCTION {trigger_name}_func();
    #         """

    #     return pg_func + '\n' + pg_trigger

    def fetch_triggers(self, table_id, schema_name, table_name):
        trigger_data = {}
        order_num = 1
        ## A trigger belongs to the table it is declared ON, and Sybase ASE records that in the
        ## table itself: sysobjects.instrig, updtrig and deltrig of the table carry the id of
        ## the trigger for each statement (ASE allows one per statement and table).
        ## sysdepends, which was used here, lists every object a trigger reads or writes, so a
        ## trigger was reported for each table it touches: 'tr_datadeletiontaskqueue_d', which is
        ## declared on data_deletion_task_queue and deletes from another table, was migrated as a
        ## trigger of that other table - it would have fired on the wrong statements, and it was
        ## created a second time for its own table as well.
        query = f"""
            SELECT DISTINCT
                tr.name AS trigger_name,
                tr.id AS trigger_id,
                tr.sysstat,
                c.text,
                c.colid
            FROM sysobjects tbl
            JOIN sysobjects tr ON tr.id IN (tbl.instrig, tbl.updtrig, tbl.deltrig)
            JOIN syscomments c ON tr.id = c.id
            WHERE
                tbl.id = {table_id}
                AND tr.type = 'TR'
                AND tbl.type = 'U'
            ORDER BY tr.id, c.colid
        """
        self.config_parser.print_log_message('DEBUG3', f"sybase_ase_connector: fetch_triggers: Fetching triggers for table {table_name}")
        self.config_parser.print_log_message('DEBUG3', f"sybase_ase_connector: fetch_triggers: Query: {query}")
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        triggers_text = {}
        for row in cursor.fetchall():
            trigger_name = row[0]
            trigger_id = row[1]
            sysstat = row[2]
            text_part = row[3]
            colid = row[4]

            if trigger_name not in triggers_text:
                triggers_text[trigger_name] = {
                    'id': trigger_id,
                    'sysstat': sysstat,
                    'text_parts': []
                }

            triggers_text[trigger_name]['text_parts'].append((colid, text_part))

        # Sort text parts by colid and concatenate
        for trigger_name, trigger_info in triggers_text.items():
            trigger_info['text_parts'].sort(key=lambda x: x[0])
            concatenated_sql = ''.join([part[1] for part in trigger_info['text_parts']])

            trigger_data[order_num] = {
            'name': trigger_name,
            'id': trigger_info['id'],
            'sysstat': trigger_info['sysstat'],
            'event': '',
            'new': '',
            'old': '',
            'sql': concatenated_sql,
            'comment': ''
            }
            order_num += 1
        cursor.close()
        self.disconnect()
        return trigger_data

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
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sybase_ase_connector: fetch_views_names: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_view_code(self, settings):
        view_id = settings['view_id']
        # source_schema_name = settings['source_schema_name']
        # source_view_name = settings['source_view_name']
        # target_schema_name = settings['target_schema_name']
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

    def convert_statement_code(self, settings: dict):
        """
        One statement of the source converted for PostgreSQL, without a wrapper around it.

        This is the conversion the query of a view is given - the outer joins written '*=',
        the string concatenation with '+', the double quoted string literals, the user
        defined types, the functions of the source and the schema of the target. It is used
        for the query of a view and for a statement of an application; 'view_code' carries
        the statement.

        Raises ValueError when the statement cannot be parsed. The error carries the text as
        far as the conversion got in its 'partial_code' attribute, for a caller which prefers
        that to nothing.
        """

        def quote_column_names(node):
            if isinstance(node, sqlglot.exp.Column):
                if node.name:
                    node.set("this", sqlglot.exp.Identifier(this=node.name, quoted=True))
                # Quote the table qualifier (alias)
                if "table" in node.args and isinstance(node.args["table"], sqlglot.exp.Identifier):
                    table = node.args["table"]
                    if not table.args.get("quoted"):
                        table.set("quoted", True)
                # Quote the db qualifier if present
                if "db" in node.args and isinstance(node.args["db"], sqlglot.exp.Identifier):
                    db = node.args["db"]
                    if not db.args.get("quoted"):
                        db.set("quoted", True)

            if isinstance(node, sqlglot.exp.Alias) and isinstance(node.args.get("alias"), sqlglot.exp.Identifier):
                alias = node.args["alias"]
                if not alias.args.get("quoted"):
                    alias.set("quoted", True)
            # for child in node.iter_expressions():
            #     quote_column_names(child)
            return node

        def replace_schema_names(node):
            if isinstance(node, sqlglot.exp.Table):
                schema = node.args.get("db")
                if schema and schema.name == settings['source_schema_name']:
                    node.set("db", sqlglot.exp.Identifier(this=settings['target_schema_name'], quoted=False))
            return node

        def quote_schema_and_table_names(node):
            if isinstance(node, sqlglot.exp.Table):
                # Quote schema name if present
                schema = node.args.get("db")
                if schema and not schema.args.get("quoted"):
                    schema.set("quoted", True)
                # Quote table name
                table = node.args.get("this")
                if table and not table.args.get("quoted"):
                    table.set("quoted", True)
                # Quote table alias if present
                alias = node.args.get("alias")
                if alias and isinstance(alias, sqlglot.exp.TableAlias):
                    alias_id = alias.args.get("this")
                    if alias_id and isinstance(alias_id, sqlglot.exp.Identifier) and not alias_id.args.get("quoted"):
                        alias_id.set("quoted", True)
            return node

        def replace_functions(node):
            mapping = self.get_sql_functions_mapping({ 'target_db_type': settings['target_db_type'] })
            # Prepare mapping for function names (without parentheses)
            func_name_map = {}
            ## A mapping written as a complete call ('suser_name()') or as a plain name
            ## ('@@nestlevel') replaces the whole expression, while one written as a prefix
            ## ('len(') only renames the function and keeps its arguments.
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
                    ## the name of the function was replaced before, which left the parentheses
                    ## of the call around it: 'suser_name()' became 'CURRENT_USER()', which
                    ## PostgreSQL refuses with 'syntax error at or near "("' - its niladic
                    ## keyword functions are written without them - and 'getutcdate()' would have
                    ## become "TIMEZONE('UTC', NOW())()".
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


        def transform_sybase_joins(expression):
            """
            The '*=' and '=*' outer joins, rewritten by the shared module.

            This used to be an implementation of its own, written against a model of sqlglot
            in which the tables behind the comma of a FROM clause stood in
            `From.expressions`. They do not any more - the extra tables are implicit joins on
            the SELECT - so the table the marked condition named was never found, the rewrite
            gave up on every statement and the marker went through to the end. A view kept
            the '/* left_outer */' in its text and a statement of an application was reported
            as one whose outer join could not be rewritten. Every Sybase ASE statement written
            with '*=' was affected, which is the shape the strategy names as the example of
            what this step is for.

            The shared module is the one Oracle's '(+)' and SQL Anywhere's '*=' already go
            through, and it is written for the model sqlglot has. Returns the expression; a
            condition it could not attribute keeps its marker, and the caller refuses such a
            statement rather than offering it as converted.
            """
            expression, unconverted = query_outer_joins.convert_marked_outer_joins(expression)
            if unconverted:
                self.config_parser.print_log_message(
                    'WARNING', f"sybase_ase_connector: convert_statement_code: {unconverted} outer "
                               f"join(s) written '*=' or '=*' could not be attributed to a table of "
                               f"the FROM clause and were not rewritten.")
            return expression

        def replace_cast_types(node):
            if isinstance(node, (sqlglot.exp.Cast, sqlglot.exp.TryCast, sqlglot.exp.Convert)):
                # Convert and Cast have different properties for the target DataType
                type_node = node.to if isinstance(node, (sqlglot.exp.Cast, sqlglot.exp.TryCast)) else node.args.get('this')
                expr_node = node.this if isinstance(node, (sqlglot.exp.Cast, sqlglot.exp.TryCast)) else node.args.get('expression')

                if isinstance(type_node, sqlglot.exp.DataType):
                    type_name = type_node.this.name.upper() if getattr(type_node.this, 'name', None) else str(type_node.this).upper()
                    if type_name == 'USERDEFINED' and 'kind' in type_node.args:
                        type_name = type_node.args['kind'].upper()

                    mapping = self.get_types_mapping({ 'target_db_type': settings['target_db_type'] })
                    if type_name in mapping:
                        mapped = mapping[type_name]
                        # Construct a new mapped Postgres DataType securely accepting nested constraints like VARCHAR(10)
                        new_type_node = sqlglot.exp.DataType.build(mapped, expressions=type_node.expressions)
                    else:
                        new_type_node = type_node

                    # Safely convert everything uniformly into a standard safe CAST wrapper
                    # for strictly Postgres compatible execution
                    return sqlglot.exp.Cast(this=expr_node, to=new_type_node)
            return node

        ## the '+' of Sybase ASE which concatenates and the conversion of its operands are
        ## shared with the other T-SQL sources - see convert_string_concatenation()
        ## and is_string_expression() of the base connector

        self.config_parser.print_log_message('DEBUG3', f"sybase_ase_connector: convert_statement_code: settings in convert_view_code: {settings}")
        converted_code = settings['view_code']

        # Apply remote_objects_substitution
        remote_subs = self.config_parser.get_remote_objects_substitution()
        if remote_subs:
            iterator = remote_subs.items() if isinstance(remote_subs, dict) else remote_subs
            for source_obj, target_obj in iterator:
                if source_obj and target_obj:
                    # Case-insensitive replacement
                    converted_code = re.sub(re.escape(source_obj), target_obj, converted_code, flags=re.IGNORECASE)
                    self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: convert_statement_code: Applied remote object substitution: {source_obj} -> {target_obj}")

        converted_code = self.prepare_query_for_parsing(converted_code)

        converted_code = self._apply_udt_to_base_type_substitutions(converted_code, settings)

        if settings['target_db_type'] == 'postgresql':

            try:
                parsed_code = sqlglot.parse_one(converted_code, read='tsql')
            except Exception as e:
                ## The statement could not be read, so there is no conversion of it. What the
                ## caller does with that is the caller's decision - a view keeps its source
                ## text and is reported as failed, a query of an application is reported as
                ## NOT CONVERTED - but nothing here answers with a text which was not
                ## converted as if it had been.
                error = ValueError(f"the statement could not be parsed as T-SQL: {first_line(e)}")
                error.partial_code = converted_code
                raise error

            # double quote column names
            parsed_code = parsed_code.transform(quote_column_names)

            # Transform Sybase Joins
            parsed_code = transform_sybase_joins(parsed_code)

            # Convert string concatenation + to ||
            parsed_code = parsed_code.transform(self.convert_string_concatenation)

            # Map Sybase native cast datatypes to Postgres native equivalents
            parsed_code = parsed_code.transform(replace_cast_types)

            self.config_parser.print_log_message('DEBUG3', f"sybase_ase_connector: convert_statement_code: Double quoted columns: {parsed_code.sql(dialect='postgres')}")

            # replace source schema with target schema
            parsed_code = parsed_code.transform(replace_schema_names)
            self.config_parser.print_log_message('DEBUG3', f"sybase_ase_connector: convert_statement_code: Replaced schema names: {parsed_code.sql(dialect='postgres')}")

            # double quote schema and table names
            parsed_code = parsed_code.transform(quote_schema_and_table_names)
            self.config_parser.print_log_message('DEBUG3', f"sybase_ase_connector: convert_statement_code: Double quoted schema and table names: {parsed_code.sql(dialect='postgres')}")

            # replace functions
            parsed_code = parsed_code.transform(replace_functions)
            self.config_parser.print_log_message('DEBUG3', f"sybase_ase_connector: convert_statement_code: Replaced functions: {parsed_code.sql(dialect='postgres')}")

            ## The statement is generated for PostgreSQL. With the default dialect of sqlglot,
            ## which was used here, the niladic keyword functions are written as calls -
            ## 'suser_name()' came out as 'CURRENT_USER()' and 'getdate()' as
            ## 'CURRENT_TIMESTAMP()', and PostgreSQL refuses both with
            ## 'syntax error at or near "("'. The variables of the source are kept in their own
            ## spelling first, the PostgreSQL generator would write them as '$v'.
            parsed_code = parsed_code.transform(self.keep_source_variables)
            converted_code = parsed_code.sql(dialect='postgres')
            converted_code = converted_code.replace("()()", "()")
            ## the 'TRUE' the outer join rewrite leaves where a condition moved into an ON
            ## clause - "WHERE TRUE AND x" is "WHERE x", and the shorter one is what a
            ## developer has to read
            converted_code = query_outer_joins.tidy_boolean_placeholders(converted_code)

            converted_code = self.apply_sql_functions_mapping(converted_code, settings)

            # converted_code = converted_code.replace(f"{settings['source_database']}..", f"{settings['target_schema_name']}.")
            # converted_code = converted_code.replace(f"{settings['source_database']}.{settings['source_schema_name']}.", f"{settings['target_schema_name']}.")
            # converted_code = converted_code.replace(f"{settings['source_schema_name']}.", f"{settings['target_schema_name']}.")
            self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: convert_statement_code: Converted view: {converted_code}")
        else:
            self.config_parser.print_log_message('ERROR', f"sybase_ase_connector: convert_statement_code: Unsupported target database type: {settings['target_db_type']}")
        return converted_code


    def convert_view_code(self, settings: dict):
        """
        The query of a view, converted for the target.

        A statement which cannot be parsed keeps the text of the source, exactly as before:
        the view is reported as failed by the migration and its source code stays readable in
        the protocol.
        """
        try:
            return self.convert_statement_code(settings)
        except ValueError as e:
            self.config_parser.print_log_message('ERROR', f"sybase_ase_connector: convert_view_code: {e}")
            return getattr(e, 'partial_code', settings['view_code'])

    def prepare_query_for_parsing(self, query_code):
        """
        The statement rewritten into something a T-SQL parser can read, without converting
        anything: this is what makes the constructs of Sybase ASE parseable at all.

          '*=' and '=*' - the outer join of ASE written in the WHERE clause. No parser of any
          other dialect knows them, so they become an equality carrying a marker which says
          which side was outer; the conversion turns the marker into a LEFT / RIGHT JOIN.

          'noholdlock' - a read hint which a parser reads as the alias of the table.

          "text" - with quoted_identifier off, a double quoted literal is a STRING in ASE and
          an identifier everywhere else.

        It is used by convert_statement_code() and by the query conversion, which has to
        classify the statement before it converts it - a statement which cannot be parsed
        would be reported as unreadable although the conversion can do it.
        """
        if not query_code:
            return query_code

        prepared = re.sub(r'\*=', '= /* left_outer */', query_code)
        prepared = re.sub(r'=\*', '= /* right_outer */', prepared)
        prepared = re.sub(r'\bnoholdlock\b', '', prepared, flags=re.IGNORECASE)

        def single_quote(match):
            return "'" + match.group(1).replace("'", "''") + "'"

        return re.sub(r'"([^"]*)"', single_quote, prepared)

    def query_conversion_supported(self):
        return True

    def convert_query_code(self, settings: dict):
        """
        One statement of an application, converted for PostgreSQL - the same conversion the
        query of a view is given. See the contract in DatabaseConnector.convert_query_code().
        """
        statement_id = settings.get('statement_id', '')
        try:
            converted = self.convert_statement_code({
                'view_code': settings['query_code'],
                'source_schema_name': settings['source_schema_name'],
                'target_schema_name': settings['target_schema_name'],
                'target_db_type': settings.get('target_db_type', 'postgresql'),
            })
        except ValueError as e:
            return {'code': '', 'converted': False, 'warnings': [], 'error': first_line(e)}
        except Exception as e:
            return {'code': '', 'converted': False, 'warnings': [],
                    'error': f"the conversion ended with an error: {first_line(e)}"}

        if not (converted or '').strip():
            return {'code': '', 'converted': False, 'warnings': [],
                    'error': 'the conversion produced no statement at all'}

        warnings = []
        ## the outer joins of the source are rewritten by the conversion; a marker left in the
        ## text says it did not finish, and such a statement is not offered as converted
        if '/* left_outer */' in converted or '/* right_outer */' in converted:
            return {'code': '', 'converted': False, 'warnings': [],
                    'error': "the outer join written '*=' or '=*' could not be rewritten as a "
                             "LEFT JOIN / RIGHT JOIN - the statement needs to be rewritten by hand"}

        self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: convert_query_code: {statement_id}: {converted}")
        return {'code': converted, 'converted': True, 'warnings': warnings, 'error': None}

    def get_sequence_current_value(self, sequence_name):
        pass

    def fetch_user_defined_types(self, schema: str):
        # Fetch user defined types
        # We look for entries in systypes where usertype > 100 (user defined)
        # We join with a second instance of systypes to get the base physical type name.

        # Note: In ASE, types define length/prec/scale.
        # Variable length types: varchar, char, nvarchar, nchar, varbinary, binary -> use length
        # Numeric types: numeric, decimal -> use prec, scale

        query = """
            SELECT
                u.name as schema_name,
                t.name as type_name,
                t.length,
                t.prec,
                t.scale,
                bt.name as base_type_name
            FROM dbo.systypes t
            JOIN dbo.sysusers u ON t.uid = u.uid
            LEFT JOIN dbo.systypes bt ON bt.usertype = (
                /* The base type of a user defined type is found by its type code, and several
                   system types share one: 'varchar' and 'sysname' are both type 39, 'char' and
                   'nchar' are both 47, 'varbinary' and 'timestamp' are both 37. The join took
                   whichever of them the server returned last, so a type over varbinary could be
                   resolved to 'timestamp' - which is the row version type of Sybase and becomes
                   BYTEA - and one over char to 'sysname', which the mapping does not know at
                   all. The lowest usertype of a type code is its canonical type, and it is the
                   one taken here; it also makes the row per user defined type unambiguous. */
                SELECT MIN(b2.usertype) FROM dbo.systypes b2
                WHERE b2.type = t.type AND b2.usertype < 100
            )
            WHERE t.usertype > 100
            ORDER BY t.name
        """

        self.connect()
        cursor = self.connection.cursor()
        self.config_parser.print_log_message('DEBUG', "sybase_ase_connector: fetch_user_defined_types: Fetching user defined types")
        cursor.execute(query)
        rows = cursor.fetchall()

        udts = {}
        order_num = 1

        for row in rows:
            schema_name = row[0]
            type_name = row[1]
            length = row[2]
            prec = row[3]
            scale = row[4]
            base_type = row[5]

            # Construct SQL definition
            if not base_type:
                # Should not happen for valid UDTs referencing standard types
                base_type = "UNKNOWN"

            # Create source type SQL for reference (Sybase DDL)
            type_sql = base_type.upper()
            base_lower = base_type.lower()

            if base_lower in ('varchar', 'char', 'nvarchar', 'nchar', 'varbinary', 'binary', 'univarchar', 'unichar'):
                type_sql += f"({length})"
            elif base_lower in ('numeric', 'decimal'):
                type_sql += f"({prec},{scale})"

            udts[order_num] = {
                'schema_name': schema_name,
                'type_name': type_name,
                'sql': type_sql,
                'base_type': base_type,
                'length': length,
                'prec': prec,
                'scale': scale,
                'comment': ''
            }
            order_num += 1

        cursor.close()
        self.disconnect()
        return udts

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

    def get_table_next_identity(self, table_schema: str, table_name: str):
        try:
            # According to Sybase ASE documentation, next_identity returns the next value.
            # Using just table_name, but may use owner.table_name if necessary.
            full_table_name = f"{table_schema}.{table_name}" if table_schema else table_name
            query = f"SELECT next_identity('{full_table_name}')"
            cursor = self.connection.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            if row and row[0] is not None:
                return int(row[0])
            return None
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"sybase_ase_connector: get_table_next_identity: Error fetching next identity for {full_table_name}: {e}")
            return None

    def fetch_domains(self, schema: str):
        order_num = 1
        domains = {}
        ## the data type a rule is bound to is a type of the source and has to be migrated
        ## like the type of a column
        types_mapping = self.get_types_mapping({'target_db_type': self.config_parser.get_target_db_type()})
        schema_condition = f"AND r.uid = USER_ID('{schema}')" if schema else ""
        ## A table CHECK constraint is stored in sysobjects as an object of type 'R', the
        ## same type as a rule created with CREATE RULE, and only a row in sysconstraints
        ## tells the two apart. Such a constraint belongs to one table and is already
        ## migrated with that table by fetch_constraints, so a domain must not be created
        ## for it - that produced a second, redundant object whose base type was only
        ## guessed from the checked column ('CHECK (VALUE > 0)' of a MONEY column then
        ## failed with 'operator does not exist: money > integer').
        no_check_constraint_condition = "AND NOT EXISTS (SELECT 1 FROM sysconstraints k WHERE k.constrid = r.id)"
        query = f"""
            SELECT
                r.name AS RuleName,
                USER_NAME(r.uid) AS RuleOwner,
                sc.colid AS DefinitionLineNumber,
                sc.text AS RuleDefinitionPart
            FROM
                sysobjects r
            JOIN
                syscomments sc ON r.id = sc.id
            WHERE
                r.type = 'R' {schema_condition}
                {no_check_constraint_condition}
            ORDER BY
                RuleName, DefinitionLineNumber
        """
        self.connect()
        cursor = self.connection.cursor()

        cursor.execute(f"""
            SELECT r.name AS ConstraintName
            FROM sysobjects r
            WHERE r.type = 'R' {schema_condition}
                AND EXISTS (SELECT 1 FROM sysconstraints k WHERE k.constrid = r.id)
            ORDER BY r.name
        """)
        skipped_check_constraints = [skipped_row[0].strip() for skipped_row in cursor.fetchall()]
        if skipped_check_constraints:
            self.config_parser.print_log_message('INFO',
                f"sybase_ase_connector: fetch_domains: Table CHECK constraints {', '.join(skipped_check_constraints)} "
                "are not rules - they are migrated together with their tables, not as domains.")

        cursor.execute(query)
        rows = cursor.fetchall()
        domains = {}
        for row in rows:
            rule_name = row[0]
            rule_owner = row[1]
            ## syscomments keeps the text of an object cut into pieces of a fixed length,
            ## which regularly falls inside a word - the pieces must be joined exactly as
            ## they are, stripping each of them would glue the words at the boundary
            ## together ('@val' + 'in (0, 1)' as '@valin (0, 1)').
            rule_definition_part = row[3]
            if rule_name not in domains:
                domains[rule_name] = {
                    'domain_schema': schema,
                    'domain_name': rule_name,
                    'domain_owner': rule_owner,
                    'source_domain_sql': rule_definition_part,
                    'domain_comment': '',
                }
            else:
                domains[rule_name]['source_domain_sql'] += rule_definition_part

        for rule_name, domain_info in domains.items():
            ## The comments of the whole batch are stored with the rule, so they have to be
            ## removed while the line ends are still in place - the newlines were replaced
            ## by spaces first, which put the rest of the statement behind a '--' comment
            ## and so commented the CHECK of the rule away without any message.
            rule_sql = domains[rule_name]['source_domain_sql']
            rule_sql_without_comments = self.strip_sql_comments(rule_sql)

            query = f"""
                SELECT DISTINCT
                    bt.name as basic_data_type
                FROM sysobjects r
                LEFT JOIN syscolumns c ON c.domain = r.id
                LEFT JOIN sysobjects o ON c.id = o.id
                LEFT JOIN systypes ut ON c.usertype = ut.usertype
                LEFT JOIN (
                    SELECT * FROM systypes t
                    JOIN (SELECT type, min(usertype) as usertype FROM systypes GROUP BY type) bt0
                    ON t.type = bt0.type AND t.usertype = bt0.usertype) bt
                ON ut.type = bt.type AND ut.hierarchy = bt.hierarchy
                WHERE r.type = 'R' AND r.name = '{domain_info['domain_name']}'
            """
            cursor.execute(query)
            row = cursor.fetchone()
            if row and row[0]:
                ## the catalog reports the data type of the source, and it was used for the
                ## CREATE DOMAIN unchanged - a domain over 'money' was created as a
                ## PostgreSQL MONEY, whose CHECK then failed with
                ## 'operator does not exist: money > integer'
                basic_data_type = row[0].strip()
                domains[rule_name]['domain_data_type'] = types_mapping.get(basic_data_type.upper(), basic_data_type)
                if domains[rule_name]['domain_data_type'].upper() != basic_data_type.upper():
                    self.config_parser.print_log_message('DEBUG',
                        f"sybase_ase_connector: fetch_domains: Domain {rule_name}: data type {basic_data_type} of the source migrated as {domains[rule_name]['domain_data_type']}.")
            else:
                ## the rule is bound to nothing the catalog can report a type for, so the
                ## condition itself has to tell - a comment must not decide that, a word
                ## like 'don't' in it is not a string literal of the rule
                domain_sql_lower = rule_sql_without_comments.lower()
                if re.search(r'\blike\b', domain_sql_lower) or "'" in rule_sql_without_comments:
                    domains[rule_name]['domain_data_type'] = 'TEXT'
                else:
                    domains[rule_name]['domain_data_type'] = 'NUMERIC'

            ## kept for the protocol as it stands in the source, only folded into one line
            domains[rule_name]['source_domain_sql'] = re.sub(r'\s+', ' ', rule_sql).strip()

            domain_check_sql = rule_sql_without_comments
            ## Everything up to 'CREATE RULE [owner.]name AS' introduces the rule, the
            ## expression behind it is what the CHECK of the domain needs. The parameter of
            ## the rule (@val) stands for the checked value and becomes VALUE.
            header_pattern = r'(?is)^.*?\bcreate\s+rule\s+(?:[\w"\[\]]+\s*\.\s*)*[\w"\[\]]+\s+as\s+'
            if re.search(header_pattern, domain_check_sql):
                domain_check_sql = re.sub(header_pattern, '', domain_check_sql, count=1)
            else:
                self.config_parser.print_log_message('WARNING',
                    f"sybase_ase_connector: fetch_domains: Rule {rule_name} does not begin with 'CREATE RULE {rule_name} AS' "
                    f"- its condition is taken over as it stands and may need to be completed by hand: {domain_check_sql.strip()}")
            domain_check_sql = domain_check_sql.replace('"', "'")
            ## a '@' inside a string literal ('%@example.com') belongs to the data, only the
            ## parameter of the rule is the checked value
            literal_parts = re.split(r"('(?:[^']|'')*')", domain_check_sql)
            domain_check_sql = ''.join(part if position % 2 else re.sub(r'@\w+', 'VALUE', part)
                                       for position, part in enumerate(literal_parts))
            domain_check_sql = re.sub(r'\s+', ' ', domain_check_sql).strip()
            # Ensure PostgreSQL standalone constraints rely on VALUE
            domain_check_sql = re.sub(r'(?i)(CHECK\s*\(\s*)([a-zA-Z_]\w*)(\s+|[<>=!])', r'\g<1>VALUE\g<3>', domain_check_sql)
            domain_check_sql = self._convert_money_literals(domain_check_sql)
            domains[rule_name]['source_domain_check_sql'] = domain_check_sql.strip()

        cursor.close()
        self.disconnect()
        self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: fetch_domains: Found domains: {domains}")
        return domains

    def get_create_domain_sql(self, settings):
        # Placeholder for generating CREATE DOMAIN SQL
        return ""

    def get_table_description(self, settings) -> dict:
        self.config_parser.print_log_message('DEBUG3', f"sybase_ase_connector: get_table_description: Sybase ASE connector: Getting table description for {settings['table_schema']}.{settings['table_name']}")
        table_schema = settings['table_schema']
        table_name = settings['table_name']
        output = ""
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(f"exec sp_help '{table_schema}.{table_name}'")

            set_num = 1
            while True:
                if cursor.description is not None:
                    rows = cursor.fetchall()
                    if rows:
                        output += f"Result set {set_num}:\n"
                        columns = [column[0] for column in cursor.description]
                        table = tabulate(rows, headers=columns, tablefmt="github")
                        output += table + "\n\n"
                        set_num += 1
                if not cursor.nextset():
                    break

            cursor.close()
            self.disconnect()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sybase_ase_connector: get_table_description: Error fetching table description for {table_schema}.{table_name}: {e}")
            raise

        return { 'table_description': output.strip() }


    def testing_select(self):
        return 'SELECT 1'

    def get_database_version(self):
        query = "SELECT @@version"
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        version = cursor.fetchone()[0]
        cursor.close()
        self.disconnect()
        return version

    def get_database_size(self):
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute("exec sp_spaceused")
        row = cursor.fetchone()
        self.logger.info(f"\n* Total size of Sybase database: {row}")
        size = row[1]
        if cursor.nextset():
            row = cursor.fetchone()
            self.logger.info(
            f"  Reserved: {row[0]}\n"
            f"  Data: {row[1]}\n"
            f"  Indexes: {row[2]}\n"
            f"  Unused: {row[3]}"
            )
        cursor.close()
        self.disconnect()
        return size

    def get_top_n_tables(self, settings):
        """
        //TODO
        what about this query?:

        select top 10 convert(varchar(30),o.name) AS table_name,
        row_count(db_id(), o.id) AS row_count,
        data_pages(db_id(), o.id, 0) AS pages,
        data_pages(db_id(), o.id, 0) * (@@maxpagesize/1024) AS kbs
        from sysobjects o
        where type = 'U'
        order by kbs DESC, table_name ASC
        """
        top_tables = {}
        top_tables['by_rows'] = {}
        top_tables['by_size'] = {}
        top_tables['by_columns'] = {}
        top_tables['by_indexes'] = {}
        top_tables['by_constraints'] = {}
        # return top_tables

        source_schema_name = settings['source_schema_name']
        try:
            order_num = 1
            top_n = self.config_parser.get_top_n_tables_by_rows()
            if top_n > 0:
                self.connect()
                cursor = self.connection.cursor()
                top_n = 10
                query = f"""
                SELECT TOP {top_n}
                user_name(o.uid) as owner,
                o.name as table_name,
                row_count(db_id(), o.id) as row_count,
                data_pages(db_id(), o.id, 0)*b.blocksize as row_size
                FROM {source_schema_name}.sysobjects o,
                (SELECT low/1024 as blocksize
                FROM master.{source_schema_name}.spt_values d
                WHERE d.number = 1 AND d.type = 'E') b
                WHERE type='U'
                ORDER BY row_count DESC
                """
                self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_top_n_tables: Executing query to get top {top_n} tables by rows: {query}")
                cursor.execute(query)
                order_num = 1
                rows = cursor.fetchall()
                cursor.close()
                self.disconnect()
                for row in rows:
                    top_tables['by_rows'][order_num] = {
                        'owner': row[0].strip(),
                        'table_name': row[1].strip(),
                        'row_count': row[2],
                        'row_size': row[3],
                    }
                    order_num += 1
                self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_top_n_tables: Top tables by rows: {top_tables['by_rows']}")
            else:
                self.config_parser.print_log_message('DEBUG', f"sybase_ase_connector: get_top_n_tables: Skipping top tables by rows check, top_n is set to 0")

        except Exception as error:
            self.config_parser.print_log_message('ERROR', f"sybase_ase_connector: get_top_n_tables: Warning: cannot check top tables by rows - error: {error}")

        return top_tables

    def get_top_fk_dependencies(self, settings):
        top_fk_dependencies = {}
        return top_fk_dependencies

    def target_table_exists(self, target_schema_name, target_table_name):
        """
        Check if the target table exists in the target schema.
        """
        query = f"""
            SELECT COUNT(*)
            FROM sysobjects o
            WHERE user_name(o.uid) = '{target_schema_name}'
              AND o.name = '{target_table_name}'
              AND o.type = 'U'
              AND (o.sysstat & 2048 <> 2048)
        """
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        exists = cursor.fetchone()[0] > 0
        cursor.close()
        self.disconnect()
        return exists

    def fetch_all_rows(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def convert_default_value(self, settings) -> dict:
        extracted_default_value = settings['extracted_default_value']
        extracted_default_value = self.apply_sql_functions_mapping(extracted_default_value, settings)
        extracted_default_value = self._convert_money_literals(extracted_default_value)
        return extracted_default_value

    def get_table_checksum(self, schema_name: str, table_name: str, columns: list):
        if not columns:
            return None
            
        cols_list = []
        for col in columns:
            dtype = col.get('data_type', '').lower()
            if any(x in dtype for x in ['lob', 'bytea', 'xml', 'json', 'text', 'image', 'unitext']):
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
            if any(x in dtype for x in ['lob', 'bytea', 'xml', 'json', 'text', 'image', 'unitext']):
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
