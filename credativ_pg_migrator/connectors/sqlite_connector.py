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

"""
SQLite source connector.

SQLite is a file based, dynamically typed database. Two properties shape this connector:

1. There is no data dictionary in the usual sense - almost everything beyond the plain
   column list has to be read from the original CREATE statements stored in sqlite_master.
   The connector therefore carries a small DDL parser which extracts CHECK constraints,
   generated column expressions and AUTOINCREMENT markers out of the stored DDL.

2. Column types are only "declared types" with a type affinity - a column declared TEXT
   may well contain integers, and one declared INTEGER may contain the text 'N/A'. The
   PostgreSQL type is therefore chosen from the declared type together with the storage
   classes the column really holds (see _widened_types_by_stored_values), and the values
   themselves are coerced during the data migration to whatever the target column expects.

SQLite has no schemas. The 'main' database is used as the schema; a name configured in
the config file is honoured only when it matches an attached database.
"""

import os
import re
import glob
import time
import hashlib
import sqlite3
import datetime
import tempfile
import traceback
import urllib.parse
from decimal import Decimal

import sqlglot
from tabulate import tabulate

from credativ_pg_migrator.database_connector import DatabaseConnector
from credativ_pg_migrator.migrator_logging import MigratorLogger
from credativ_pg_migrator.text_decoding import TextDecoder


class SQLiteConnector(DatabaseConnector):

    # Identifier as it can appear in SQLite DDL - double quoted, backtick quoted,
    # bracket quoted (MS Access style, accepted by SQLite) or a bare name.
    # The alternation is wrapped in a non-capturing group so that the pattern stays a
    # single unit when it is concatenated into a larger regular expression.
    IDENTIFIER_PATTERN = r'(?:"(?:[^"]|"")*"|`(?:[^`]|``)*`|\[[^\]]*\]|[A-Za-z_][A-Za-z0-9_$]*)'

    # Keywords which start a table level constraint inside CREATE TABLE (...)
    TABLE_CONSTRAINT_KEYWORDS = ('CONSTRAINT', 'PRIMARY', 'UNIQUE', 'CHECK', 'FOREIGN')

    def __init__(self, config_parser, source_or_target):
        if source_or_target not in ['source', 'target']:
            raise ValueError("SQLite must be either source or target database")
        if source_or_target == 'target':
            raise ValueError("SQLite is supported only as a source database - the target database must be PostgreSQL")

        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.on_error_action = self.config_parser.get_on_error_action()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger
        self.source_db_config = self.config_parser.get_source_config()
        # Cache of parsed CREATE TABLE statements, keyed by (schema, table)
        self._ddl_cache = {}
        # Cache of the type conflicts found in the stored values, keyed by (schema, table)
        self._stored_values_cache = {}
        # The connectivity has to be resolved here and not only in connect(): with
        # connectivity 'ddl' the planner skips the connection check and the pre-migration
        # analysis entirely and goes straight to parse_ddl_files(), so an unusable value
        # would only surface much later as an unrelated looking error.
        self.connectivity = self._check_connectivity()

        self.ddl_files = []
        self._ddl_database_path = None
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            self._prepare_ddl_files()

    ## ---------------------------------------------------------------- bytes to text
    ##
    ## SQLite does not enforce the encoding of a TEXT value and it does not enforce the type
    ## of a column either, so a value which is declared TEXT can arrive as bytes which are
    ## not valid UTF-8 - a legacy database in a single byte encoding is exactly that. Four
    ## places decoded those with errors='replace', which writes U+FFFD into the target as if
    ## it were the data: it cannot be told apart from a U+FFFD which was really there and
    ## cannot be turned back into the byte it stood for. What happens instead is
    ## migration.on_undecodable_bytes, applied in text_decoding.py.

    def text_decoder(self):
        """The decoder of the values of this connection. UTF-8 is the only encoding SQLite
        hands text over in, so there is nothing else to try before the setting applies."""
        decoder = getattr(self, '_text_decoder', None)
        if decoder is None:
            decoder = TextDecoder(self.config_parser, 'sqlite_connector',
                                  encodings=('utf-8',))
            self._text_decoder = decoder
        return decoder

    def script_decoder(self):
        """The decoder of the DDL scripts, which are files and not values of a connection."""
        decoder = getattr(self, '_script_decoder', None)
        if decoder is None:
            ## utf-8-sig is utf-8 and also removes a byte order mark, which plain utf-8
            ## leaves in the text as \ufeff - SQLite then refuses the first statement of the
            ## file as an unrecognised token.
            decoder = TextDecoder(self.config_parser, 'sqlite_connector',
                                  encodings=('utf-8-sig',), last_resort='latin-1')
            self._script_decoder = decoder
        return decoder

    ## ---------------------------------------------------------------- connection

    def _check_connectivity(self):
        """
        SQLite supports two connectivity modes:
          native - the source is a SQLite database file, read through the sqlite3 module
          ddl    - the source objects are read from SQL script files, and the data usually
                   comes from CSV files configured under data_export
        """
        connectivity = self.config_parser.get_connectivity(self.source_or_target)
        if connectivity is None or str(connectivity).strip() == '':
            return 'native'
        connectivity = str(connectivity).strip().lower()
        if connectivity not in ('native', self.config_parser.const_connectivity_ddl()):
            raise ValueError(
                f"sqlite_connector: unsupported connectivity '{connectivity}' for the SQLite "
                f"{self.source_or_target} database. Supported values are \"native\" (read the "
                f"objects and the data from a SQLite database file given by 'database') and "
                f"\"ddl\" (read the objects from the SQL script files given by 'ddl: path:', "
                f"with the data usually coming from CSV files configured under 'data_export'). "
                f"The setting may also be left out, which means \"native\".")
        return connectivity

    ## ---------------------------------------------------------------- DDL connectivity

    def _prepare_ddl_files(self):
        """
        Resolve 'ddl: path:' into the list of SQL script files, exactly like the other
        DDL based connectors: the path can be a directory, a file mask or a single file.
        """
        try:
            self.ddl_path = self.source_db_config['ddl']['path']
        except (KeyError, TypeError):
            raise ValueError(
                "sqlite_connector: connectivity is \"ddl\", so the source section must contain "
                "a 'ddl:' block with a 'path:' pointing to the SQL script file(s) holding the "
                "CREATE statements - a directory, a file mask or one specific file.")

        ddl_path = os.path.expanduser(str(self.ddl_path))
        if not os.path.isabs(ddl_path):
            ddl_path = os.path.join(os.path.dirname(os.path.abspath(self.config_parser.args.config)), ddl_path)
        self.ddl_path = os.path.normpath(ddl_path)

        if os.path.isdir(self.ddl_path):
            self.ddl_files = sorted(glob.glob(os.path.join(self.ddl_path, '*.*')))
        else:
            self.ddl_files = sorted(glob.glob(self.ddl_path))
        self.ddl_files = [path for path in self.ddl_files if os.path.isfile(path)]

        if not self.ddl_files:
            raise ValueError(f"sqlite_connector: No DDL files found for path or mask: '{self.ddl_path}'")

        self.config_parser.print_log_message('INFO', f"sqlite_connector: DDL path valid: '{self.ddl_path}', found {len(self.ddl_files)} file(s)")
        for filepath in self.ddl_files:
            self.config_parser.print_log_message('DEBUG', f"sqlite_connector: DDL file: {filepath} ({os.path.getsize(filepath)} bytes)")

        # The scripts are replayed into a real SQLite database, which is then introspected
        # exactly like a database given with native connectivity. SQLite is the only parser
        # that understands its own DDL completely, so nothing is lost in a hand-written one.
        # The path is derived from the script list so that every connector instance - the
        # planner and each orchestrator worker - resolves it to the same file.
        digest = hashlib.sha1('|'.join(self.ddl_files).encode('utf-8')).hexdigest()[:12]
        base_dir = None
        try:
            if self.config_parser.get_source_data_export():
                base_dir = self.config_parser.get_source_data_export_conversion_path()
        except Exception:
            base_dir = None
        if not base_dir or not os.path.isdir(base_dir):
            base_dir = tempfile.gettempdir()
        self._ddl_database_path = os.path.join(base_dir, f"credativ_pg_migrator_sqlite_ddl_{digest}.sqlite")

    def _database_path(self):
        """ The SQLite file this connector reads from, for both connectivity modes. """
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            return self._ddl_database_path
        return self.config_parser.get_connect_string(self.source_or_target)

    def _read_script(self, filepath):
        """
        Read a SQL script; a legacy dump is not necessarily valid UTF-8.

        A script which is not UTF-8 is read as latin-1, which keeps every byte, and that is
        reported: what is in the file are the names of the objects the migration is about to
        create, so a script read in the wrong encoding creates them misspelled.
        """
        with open(filepath, 'rb') as script_file:
            raw = script_file.read()
        return self.script_decoder().decode(raw, place=os.path.basename(filepath))

    def _execute_script(self, connection, script, filepath):
        """
        Replay one SQL script into the staging database.

        The fast path is executescript(), which hands the whole file to SQLite at once.
        It is all-or-nothing though, so when it fails the script is replayed statement by
        statement - that isolates the offending statement, reports it, and lets the rest of
        the file through instead of losing every object in it.
        """
        try:
            connection.executescript(script)
            return 0
        except sqlite3.Error as e:
            self.config_parser.print_log_message('WARNING', f"sqlite_connector: _execute_script: {os.path.basename(filepath)}: {e} - replaying the file statement by statement to isolate the problem.")

        try:
            connection.rollback()
        except sqlite3.Error:
            pass

        failed = 0
        statement = ''
        cursor = connection.cursor()
        for line in script.splitlines(keepends=True):
            statement += line
            # sqlite3_complete() knows about string literals and about the BEGIN ... END
            # body of a trigger, so a trigger is not cut apart at its inner semicolons.
            if not sqlite3.complete_statement(statement):
                continue
            text = statement.strip()
            statement = ''
            if not text or text.startswith('--'):
                continue
            try:
                cursor.execute(text)
            except sqlite3.Error as e:
                failed += 1
                preview = re.sub(r'\s+', ' ', text)[:200]
                self.config_parser.print_log_message('WARNING', f"sqlite_connector: _execute_script: {os.path.basename(filepath)}: skipped statement ({e}): {preview}")
        if statement.strip():
            try:
                cursor.execute(statement)
            except sqlite3.Error as e:
                failed += 1
                self.config_parser.print_log_message('WARNING', f"sqlite_connector: _execute_script: {os.path.basename(filepath)}: skipped trailing statement ({e}).")
        cursor.close()
        connection.commit()
        return failed

    def _build_ddl_database(self, force=False):
        """
        Create the staging SQLite database from the configured SQL scripts. The database is
        built under a temporary name and moved into place atomically, so that orchestrator
        workers running in parallel either see the previous complete file or the new one.
        """
        if not force and self._ddl_database_path and os.path.isfile(self._ddl_database_path):
            return self._ddl_database_path

        build_path = f"{self._ddl_database_path}.{os.getpid()}.tmp"
        for leftover in (build_path,):
            if os.path.exists(leftover):
                os.remove(leftover)

        self.config_parser.print_log_message('INFO', f"sqlite_connector: _build_ddl_database: Replaying {len(self.ddl_files)} DDL script(s) into the staging database {self._ddl_database_path}")
        failed_statements = 0
        build_connection = sqlite3.connect(build_path)
        build_connection.text_factory = self._decode_text
        try:
            # A dump normally switches these off itself; doing it here as well keeps a
            # schema-only script from failing on a forward reference between tables.
            build_connection.execute("PRAGMA foreign_keys=OFF")
            build_connection.execute("PRAGMA legacy_alter_table=ON")
            for filepath in self.ddl_files:
                script = self._read_script(filepath)
                if not script.strip():
                    self.config_parser.print_log_message('WARNING', f"sqlite_connector: _build_ddl_database: {filepath} is empty - skipped.")
                    continue
                failed_statements += self._execute_script(build_connection, script, filepath)
            ## which of the scripts were not in the encoding they were expected in
            self.script_decoder().log_summary()
            build_connection.commit()
            objects = build_connection.execute(
                "SELECT type, count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' GROUP BY type ORDER BY type").fetchall()
        except Exception:
            build_connection.close()
            if os.path.exists(build_path):
                os.remove(build_path)
            raise
        build_connection.close()

        plurals = {'table': 'tables', 'index': 'indexes', 'view': 'views', 'trigger': 'triggers'}
        summary = ', '.join(f"{count} {plurals.get(kind, kind + 's') if count != 1 else kind}" for kind, count in objects) or 'no objects'
        if not objects:
            os.remove(build_path)
            raise ValueError(
                f"sqlite_connector: the DDL script(s) under '{self.ddl_path}' produced no database "
                f"objects. Check that the files contain SQLite CREATE statements"
                + (f" - {failed_statements} statement(s) could not be executed; each one is logged at WARNING." if failed_statements else "."))

        os.replace(build_path, self._ddl_database_path)
        self.config_parser.print_log_message('INFO', f"sqlite_connector: _build_ddl_database: Staging database created from DDL: {summary}")
        if failed_statements:
            # A skipped CREATE statement means a missing object, so this is a warning -
            # shown by the default log level, with each skipped statement logged beside it.
            self.config_parser.print_log_message('WARNING', f"sqlite_connector: _build_ddl_database: ATTENTION: {failed_statements} statement(s) of the DDL script(s) could not be executed and were SKIPPED - the objects they create are missing from the migration. Each skipped statement is logged separately.")
        return self._ddl_database_path

    def parse_ddl_files(self, settings):
        """
        Entry point called by the planner for DDL connectivity. For SQLite "parsing" means
        replaying the SQL scripts into a staging SQLite database - after that every object
        (tables, columns, indexes, constraints, views, triggers) is read with exactly the
        same code as for a native connection.
        """
        self._ddl_cache = {}
        self._build_ddl_database(force=True)
        # SQLite has no schemas, so whatever the config named the schema, the objects of the
        # staging database live in 'main'.
        self.config_parser.set_source_schema('main')
        self.config_parser.print_log_message('INFO', "sqlite_connector: parse_ddl_files: DDL scripts loaded - source schema set to 'main'.")

    def _decode_text(self, value):
        """
        Text factory for sqlite3. SQLite does not enforce the encoding of TEXT values, so a
        legacy database can easily contain bytes which are not valid UTF-8. The default
        factory raises on those, which ends the migration on a single bad row; what happens
        instead is migration.on_undecodable_bytes, and its default keeps every byte.
        """
        return self.text_decoder().decode(value, place='TEXT value')

    def connect(self):
        if self.connectivity == self.config_parser.const_connectivity_ddl():
            # Every orchestrator worker builds its own connector, and only the planner runs
            # parse_ddl_files(), so the staging database is (re)built here when it is missing.
            self._build_ddl_database()

        database_path = self._database_path()
        if not os.path.isfile(database_path):
            raise FileNotFoundError(f"sqlite_connector: connect: SQLite database file not found: {database_path}")

        self.connection = None
        # A migration only reads from the source, so the database is opened read-only.
        # That fails when the file carries a hot journal / WAL needing recovery, in which
        # case we fall back to a normal read-write connection.
        try:
            uri = 'file:' + urllib.parse.quote(os.path.abspath(database_path)) + '?mode=ro'
            self.connection = sqlite3.connect(uri, uri=True, timeout=30)
            self.connection.execute("SELECT count(*) FROM sqlite_master")
        except sqlite3.Error as e:
            if self.connection is not None:
                try:
                    self.connection.close()
                except sqlite3.Error:
                    pass
                self.connection = None
            self.config_parser.print_log_message('WARNING', f"sqlite_connector: connect: Could not open {database_path} read-only ({e}) - opening read-write.")

        if self.connection is None:
            self.connection = sqlite3.connect(database_path, timeout=30)

        self.connection.text_factory = self._decode_text

    def disconnect(self):
        try:
            ## How many values did not fit UTF-8, before the connection which read them is
            ## gone. Nothing is written when there were none.
            decoder = getattr(self, '_text_decoder', None)
            if decoder is not None:
                decoder.log_summary()
        except Exception:
            pass
        try:
            if self.connection:
                self.connection.close()
        except Exception:
            pass
        finally:
            self.connection = None

    def handle_error(self, e, description=None):
        self.config_parser.print_log_message('ERROR', f"sqlite_connector: handle_error: An error in {self.__class__.__name__} ({description}): {e}")
        self.config_parser.print_log_message('ERROR', traceback.format_exc())
        if self.on_error_action == 'stop':
            self.config_parser.print_log_message('ERROR', "sqlite_connector: handle_error: Stopping due to error.")
            exit(1)
        else:
            self.config_parser.print_log_message('WARNING', f"sqlite_connector: handle_error: Error caught, but continuing as requested by configuration (on_error_action='{self.on_error_action}').")

    ## ---------------------------------------------------------------- identifiers / schema

    @staticmethod
    def _quote_ident(name):
        """ Quote an identifier for use in SQLite SQL. """
        return '"' + str(name).replace('"', '""') + '"'

    @classmethod
    def _unquote_ident(cls, name):
        """ Strip the quoting SQLite accepts around identifiers. """
        if name is None:
            return ''
        name = str(name).strip()
        if len(name) >= 2:
            if name[0] == '"' and name[-1] == '"':
                return name[1:-1].replace('""', '"')
            if name[0] == '`' and name[-1] == '`':
                return name[1:-1].replace('``', '`')
            if name[0] == '[' and name[-1] == ']':
                return name[1:-1]
            if name[0] == "'" and name[-1] == "'":
                return name[1:-1].replace("''", "'")
        return name

    def _resolve_schema(self, schema_name):
        """
        SQLite has no schemas. Everything lives in the 'main' database, unless further
        databases were attached. A configured schema name is used only when it really is
        an attached database; any other value (including the migrator default 'public')
        resolves to 'main'.
        """
        name = self._unquote_ident(schema_name) if schema_name else ''
        if name.lower() in ('main', 'temp'):
            return name.lower()
        if name and self.connection is not None:
            try:
                cursor = self.connection.cursor()
                cursor.execute("PRAGMA database_list")
                attached = {str(row[1]).lower() for row in cursor.fetchall()}
                cursor.close()
                if name.lower() in attached:
                    return name.lower()
            except sqlite3.Error:
                pass
        return 'main'

    def _qualified_name(self, schema_name, object_name):
        return f"{self._quote_ident(self._resolve_schema(schema_name))}.{self._quote_ident(object_name)}"

    def _pragma_rows(self, schema_name, pragma_name, argument=None):
        """
        Run a PRAGMA and return its rows. PRAGMA does not accept bound parameters, so the
        argument is quoted as an identifier.
        """
        schema = self._resolve_schema(schema_name)
        if argument is None:
            statement = f'PRAGMA {self._quote_ident(schema)}.{pragma_name}'
        else:
            statement = f'PRAGMA {self._quote_ident(schema)}.{pragma_name}({self._quote_ident(argument)})'
        cursor = self.connection.cursor()
        cursor.execute(statement)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def _fetch_master(self, schema_name, object_type=None, object_name=None):
        """ Read rows of sqlite_master (rowid, name, tbl_name, sql) for the given schema. """
        schema = self._resolve_schema(schema_name)
        query = f'SELECT rowid, type, name, tbl_name, sql FROM {self._quote_ident(schema)}.sqlite_master WHERE 1 = 1'
        params = []
        if object_type:
            query += ' AND type = ?'
            params.append(object_type)
        if object_name:
            query += ' AND name = ?'
            params.append(object_name)
        query += ' ORDER BY name'
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def _object_sql(self, schema_name, object_type, object_name):
        rows = self._fetch_master(schema_name, object_type, object_name)
        return rows[0][4] if rows else ''

    ## ---------------------------------------------------------------- DDL parsing helpers

    @staticmethod
    def _strip_sql_comments(sql_text):
        """ Remove -- and /* */ comments, leaving string literals and quoted names alone. """
        if not sql_text:
            return ''
        result = []
        index = 0
        length = len(sql_text)
        quote = None
        while index < length:
            char = sql_text[index]
            if quote:
                result.append(char)
                if char == quote:
                    quote = None
                index += 1
                continue
            if char in ('"', "'", '`'):
                quote = char
                result.append(char)
                index += 1
                continue
            if char == '[':
                quote = ']'
                result.append(char)
                index += 1
                continue
            if char == '-' and index + 1 < length and sql_text[index + 1] == '-':
                while index < length and sql_text[index] != '\n':
                    index += 1
                result.append(' ')
                continue
            if char == '/' and index + 1 < length and sql_text[index + 1] == '*':
                index += 2
                while index + 1 < length and not (sql_text[index] == '*' and sql_text[index + 1] == '/'):
                    index += 1
                index += 2
                result.append(' ')
                continue
            result.append(char)
            index += 1
        return ''.join(result)

    @staticmethod
    def _split_top_level(text, separator=','):
        """ Split on a separator that is outside parentheses, string literals and quoted names. """
        if not text:
            return []
        parts = []
        current = []
        depth = 0
        quote = None
        index = 0
        length = len(text)
        while index < length:
            char = text[index]
            if quote:
                current.append(char)
                if char == quote:
                    if quote in ('"', "'", '`') and index + 1 < length and text[index + 1] == quote:
                        current.append(text[index + 1])
                        index += 2
                        continue
                    quote = None
                index += 1
                continue
            if char in ('"', "'", '`'):
                quote = char
                current.append(char)
                index += 1
                continue
            if char == '[':
                quote = ']'
                current.append(char)
                index += 1
                continue
            if char == '(':
                depth += 1
            elif char == ')':
                depth = max(depth - 1, 0)
            elif char == separator and depth == 0:
                parts.append(''.join(current))
                current = []
                index += 1
                continue
            current.append(char)
            index += 1
        parts.append(''.join(current))
        return [part.strip() for part in parts if part.strip()]

    @staticmethod
    def _find_paren_group(text, start_index=0):
        """
        Find the first parenthesized group at or after start_index.
        Returns (content, index_after_closing_paren) or (None, -1).
        """
        depth = 0
        content_start = None
        quote = None
        index = start_index
        length = len(text)
        while index < length:
            char = text[index]
            if quote:
                if char == quote:
                    quote = None
                index += 1
                continue
            if char in ('"', "'", '`'):
                quote = char
                index += 1
                continue
            if char == '[':
                quote = ']'
                index += 1
                continue
            if char == '(':
                depth += 1
                if depth == 1:
                    content_start = index + 1
            elif char == ')':
                depth -= 1
                if depth == 0 and content_start is not None:
                    return text[content_start:index], index + 1
            index += 1
        return None, -1

    @classmethod
    def _extract_table_body(cls, ddl):
        """ Return the text between the outermost parentheses of a CREATE TABLE statement. """
        content, _ = cls._find_paren_group(ddl)
        return content or ''

    @staticmethod
    def _safe_object_name(*parts):
        """
        Build the name of an object the source database does not name itself (a foreign key,
        a column CHECK constraint). SQLite object names may contain anything, while
        PostgreSQL silently truncates identifiers at 63 bytes - two generated names derived
        from long table names could therefore collide. Reduce the name to plain characters
        and keep it short enough for the suffix the target connector appends.
        """
        cleaned = []
        for part in parts:
            if part is None or str(part) == '':
                continue
            cleaned.append(re.sub(r'[^A-Za-z0-9_]+', '_', str(part)).strip('_'))
        name = '_'.join(piece for piece in cleaned if piece)
        return name[:40] if len(name) > 40 else name

    @classmethod
    def _leading_identifier(cls, text):
        """ Return the first identifier of a column definition and the rest of the definition. """
        match = re.match(r'\s*(' + cls.IDENTIFIER_PATTERN + r')', text)
        if not match:
            return '', text
        return cls._unquote_ident(match.group(1)), text[match.end():]

    def _parse_table_ddl(self, schema_name, table_name):
        """
        Parse the CREATE TABLE statement of a table and return everything the SQLite
        catalog does not expose through PRAGMA:

        {
            'sql': original DDL,
            'columns': { lower(column_name): {
                            'generation_expression', 'generated_kind',
                            'autoincrement', 'check_sql', 'collation', 'unique' } },
            'checks': [ {'name', 'sql', 'column'} ],
            'without_rowid': bool,
            'is_virtual': bool,
        }
        """
        schema = self._resolve_schema(schema_name)
        cache_key = (schema, table_name)
        if cache_key in self._ddl_cache:
            return self._ddl_cache[cache_key]

        parsed = {
            'sql': '',
            'columns': {},
            'checks': [],
            'without_rowid': False,
            'is_virtual': False,
        }
        try:
            ddl = self._object_sql(schema_name, 'table', table_name) or ''
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"sqlite_connector: _parse_table_ddl: Could not read the DDL of {schema}.{table_name}: {e}")
            ddl = ''

        parsed['sql'] = ddl
        if not ddl:
            self._ddl_cache[cache_key] = parsed
            return parsed

        clean_ddl = self._strip_sql_comments(ddl)
        parsed['is_virtual'] = re.search(r'(?is)^\s*CREATE\s+VIRTUAL\s+TABLE\b', clean_ddl) is not None
        parsed['without_rowid'] = re.search(r'(?is)\)\s*(?:[A-Za-z, ]*\b)?WITHOUT\s+ROWID\b', clean_ddl) is not None

        if parsed['is_virtual']:
            self._ddl_cache[cache_key] = parsed
            return parsed

        body = self._extract_table_body(clean_ddl)
        unnamed_check_number = 0
        for definition in self._split_top_level(body):
            first_word = definition.split(None, 1)[0].upper().strip('(')
            if first_word in self.TABLE_CONSTRAINT_KEYWORDS:
                # Table level constraint - only CHECK carries information the PRAGMAs miss
                constraint_name = ''
                remainder = definition
                named = re.match(r'(?is)^\s*CONSTRAINT\s+(' + self.IDENTIFIER_PATTERN + r')\s+(.*)$', definition)
                if named:
                    constraint_name = self._unquote_ident(named.group(1))
                    remainder = named.group(2)
                if re.match(r'(?is)^\s*CHECK\b', remainder):
                    check_expression, _ = self._find_paren_group(remainder)
                    if check_expression:
                        unnamed_check_number += 1
                        parsed['checks'].append({
                            'name': constraint_name or self._safe_object_name(table_name, 'check', unnamed_check_number),
                            'sql': check_expression.strip(),
                            'column': '',
                        })
                continue

            # Column definition
            column_name, rest = self._leading_identifier(definition)
            if not column_name:
                continue
            column_details = {
                'generation_expression': '',
                'generated_kind': '',
                'autoincrement': False,
                'check_sql': '',
                'collation': '',
                'unique': False,
            }

            if re.search(r'(?is)\bAUTOINCREMENT\b', rest):
                column_details['autoincrement'] = True
            if re.search(r'(?is)\bUNIQUE\b', rest):
                column_details['unique'] = True
            collation = re.search(r'(?is)\bCOLLATE\s+(' + self.IDENTIFIER_PATTERN + r')', rest)
            if collation:
                column_details['collation'] = self._unquote_ident(collation.group(1))

            # GENERATED ALWAYS AS (expr) [STORED|VIRTUAL] - the AS keyword is mandatory,
            # GENERATED ALWAYS is optional.
            generated = re.search(r'(?is)(?:\bGENERATED\s+ALWAYS\s+)?\bAS\s*(?=\()', rest)
            if generated:
                expression, expression_end = self._find_paren_group(rest, generated.start())
                if expression:
                    column_details['generation_expression'] = expression.strip()
                    trailing = rest[expression_end:]
                    column_details['generated_kind'] = 'VIRTUAL' if re.match(r'(?is)\s*VIRTUAL\b', trailing) else 'STORED'

            check = re.search(r'(?is)\bCHECK\s*(?=\()', rest)
            if check:
                check_expression, _ = self._find_paren_group(rest, check.start())
                if check_expression:
                    unnamed_check_number += 1
                    column_details['check_sql'] = check_expression.strip()
                    parsed['checks'].append({
                        'name': self._safe_object_name(table_name, column_name, 'check'),
                        'sql': check_expression.strip(),
                        'column': column_name,
                    })

            parsed['columns'][column_name.lower()] = column_details

        self._ddl_cache[cache_key] = parsed
        return parsed

    @staticmethod
    def _parse_declared_type(declared_type):
        """
        Split a SQLite declared type into base type, length and precision/scale.
        SQLite accepts any declared type, so this has to cope with everything from
        '' (no type at all) over 'VARCHAR(100)' to 'DOUBLE PRECISION' and 'DECIMAL(10, 2)'.

        Returns (base_type, character_maximum_length, numeric_precision, numeric_scale).
        """
        if declared_type is None:
            return '', None, None, None
        text = str(declared_type).strip()
        if not text:
            # A column without a declared type has BLOB affinity in SQLite, but in
            # practice such columns hold text - TEXT is the safer, lossless target.
            return '', None, None, None

        # PRAGMA table_info reports the declared type without the column constraints, but
        # a type read from the DDL can still carry a trailing COLLATE clause.
        text = re.sub(r'(?is)\s+COLLATE\s+\S+\s*$', '', text).strip()

        arguments = None
        match = re.match(r'^\s*([^()]+?)\s*\(([^()]*)\)\s*$', text)
        if match:
            base_type = match.group(1).strip()
            arguments = match.group(2).strip()
        else:
            base_type = text

        base_type = re.sub(r'\s+', ' ', base_type).strip().upper()

        character_maximum_length = None
        numeric_precision = None
        numeric_scale = None
        if arguments:
            numbers = [part.strip() for part in arguments.split(',')]
            try:
                if len(numbers) == 1 and numbers[0]:
                    value = int(numbers[0])
                    if any(token in base_type for token in ('CHAR', 'TEXT', 'CLOB', 'BINARY', 'BLOB', 'STRING')):
                        character_maximum_length = value
                    else:
                        numeric_precision = value
                elif len(numbers) >= 2:
                    numeric_precision = int(numbers[0])
                    numeric_scale = int(numbers[1])
            except (TypeError, ValueError):
                # Not a numeric argument - e.g. an enum-like declaration; ignore it
                character_maximum_length = None
                numeric_precision = None
                numeric_scale = None

        return base_type, character_maximum_length, numeric_precision, numeric_scale

    ## ---------------------------------------------------------------- SQL conversion

    def get_sql_functions_mapping(self, settings):
        """ Returns a dictionary of SQL functions mapping for the target database """
        target_db_type = settings['target_db_type']
        if target_db_type == 'postgresql':
            return {
                'ifnull(': 'coalesce(',
                # SQLite instr(X, Y) searches Y inside X - the same argument order as strpos
                'instr(': 'strpos(',
                'substr(': 'substring(',
                'total(': 'sum(',
                'last_insert_rowid()': 'lastval()',
                'sqlite_version()': 'version()',
                "date('now')": 'current_date',
                "datetime('now')": 'current_timestamp',
                "time('now')": 'current_time',
                'changes()': '0',
            }
        else:
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: get_sql_functions_mapping: Unsupported target database type: {target_db_type}")
            return {}

    @staticmethod
    def _normalize_quoting(text):
        """
        Rewrite the identifier quoting SQLite accepts (backticks, square brackets) into
        the PostgreSQL double quotes. String literals are left untouched - a bracket or a
        backtick inside a literal is data, not quoting.
        """
        if not text:
            return text
        result = []
        index = 0
        length = len(text)
        while index < length:
            char = text[index]
            if char == "'":
                start = index
                index += 1
                while index < length:
                    if text[index] == "'":
                        if index + 1 < length and text[index + 1] == "'":
                            index += 2
                            continue
                        index += 1
                        break
                    index += 1
                result.append(text[start:index])
                continue
            if char == '"':
                start = index
                index += 1
                while index < length and text[index] != '"':
                    index += 1
                index += 1
                result.append(text[start:min(index, length)])
                continue
            if char in ('`', '['):
                closing = '`' if char == '`' else ']'
                start = index + 1
                index += 1
                while index < length and text[index] != closing:
                    index += 1
                result.append('"' + text[start:index].replace('"', '""') + '"')
                index += 1
                continue
            result.append(char)
            index += 1
        return ''.join(result)

    def _transpile(self, sql_text, description=''):
        """ Best effort translation of a SQLite expression / statement into PostgreSQL. """
        if not sql_text or not str(sql_text).strip():
            return sql_text
        converted = str(sql_text)
        try:
            transpiled = sqlglot.transpile(converted, read='sqlite', write='postgres')
            if transpiled and transpiled[0]:
                converted = transpiled[0]
        except Exception as e:
            self.config_parser.print_log_message('DEBUG', f"sqlite_connector: _transpile: sqlglot could not translate {description}: {e}")
        # When sqlglot cannot parse the input, the SQLite quoting survives untouched
        return self._finalize_sql(converted)

    def _finalize_sql(self, converted):
        """ The last steps every translated fragment goes through. """
        converted = self._normalize_quoting(converted)
        return self.apply_sql_functions_mapping(converted, {'target_db_type': 'postgresql'})

    def _qualify_object_names(self, sql_text, target_schema_name, object_names):
        """
        SQLite statements reference tables without a schema, so a view or trigger body
        copied verbatim would only resolve through the target search_path. Prefix the
        names of migrated objects with the target schema instead.
        """
        if not sql_text or not object_names:
            return sql_text
        converted = sql_text
        for object_name in sorted(object_names, key=len, reverse=True):
            target_name = self.config_parser.convert_names_case(object_name)
            replacement = f'\\1"{target_schema_name}"."{target_name}"'
            # main.tbl / "main"."tbl" - the explicit SQLite database qualifier
            converted = re.sub(
                r'(?i)(\b(?:FROM|JOIN|INTO|UPDATE)\s+)["`\[]?main["`\]]?\s*\.\s*["`\[]?' + re.escape(object_name) + r'["`\]]?(?![\w$."`\]])',
                replacement, converted)
            # bare table name
            converted = re.sub(
                r'(?i)(\b(?:FROM|JOIN|INTO|UPDATE)\s+)["`\[]?' + re.escape(object_name) + r'["`\]]?(?![\w$."`\]])',
                replacement, converted)
        return converted

    def _migrated_object_names(self, schema_name):
        """ Names of all tables and views in the source database - used to qualify references. """
        names = set()
        try:
            for row in self._fetch_master(schema_name):
                if row[1] in ('table', 'view') and not str(row[2]).startswith('sqlite_'):
                    names.add(row[2])
        except Exception as e:
            self.config_parser.print_log_message('DEBUG', f"sqlite_connector: _migrated_object_names: Could not list objects: {e}")
        return names

    ## ---------------------------------------------------------------- tables and columns

    def _shadow_table_prefixes(self, schema_name):
        """
        Virtual tables (FTS, RTREE, ...) keep their data in shadow tables named
        <virtual table>_<suffix>. Neither the virtual table nor its shadow tables can be
        migrated meaningfully, so both are skipped.
        """
        prefixes = []
        try:
            for row in self._fetch_master(schema_name, 'table'):
                sql_text = row[4] or ''
                if re.search(r'(?is)^\s*CREATE\s+VIRTUAL\s+TABLE\b', sql_text):
                    prefixes.append(str(row[2]))
        except Exception:
            pass
        return prefixes

    def fetch_table_names(self, table_schema: str):
        tables = {}
        order_num = 1
        try:
            self.connect()
            schema = self._resolve_schema(table_schema)
            virtual_tables = self._shadow_table_prefixes(table_schema)
            for row in self._fetch_master(table_schema, 'table'):
                table_name = str(row[2])
                if table_name.startswith('sqlite_'):
                    continue
                if table_name in virtual_tables:
                    self.config_parser.print_log_message('WARNING', f"sqlite_connector: fetch_table_names: Skipping virtual table {table_name} - virtual tables have no PostgreSQL equivalent.")
                    continue
                if any(table_name.startswith(prefix + '_') for prefix in virtual_tables):
                    self.config_parser.print_log_message('DEBUG', f"sqlite_connector: fetch_table_names: Skipping shadow table {table_name} of a virtual table.")
                    continue
                tables[order_num] = {
                    'id': row[0],
                    'schema_name': schema,
                    'table_name': table_name,
                    'comment': '',
                    'source_table_sql': row[4] or '',
                }
                order_num += 1
            self.disconnect()
            return tables
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: fetch_table_names: Error fetching table names: {e}")
            self.disconnect()
            raise

    def get_table_description(self, settings) -> dict:
        table_schema = settings['table_schema']
        table_name = settings['table_name']
        output = []
        try:
            self.connect()
            schema = self._resolve_schema(table_schema)
            output.append(f'Table "{schema}"."{table_name}"')

            parsed_ddl = self._parse_table_ddl(table_schema, table_name)
            rows, has_hidden = self._table_columns_pragma(table_schema, table_name)
            if rows:
                headers = ['Column', 'Declared type', 'Not null', 'Default', 'PK', 'Generated']
                table_rows = []
                for row in rows:
                    hidden = row[6] if has_hidden and len(row) > 6 else 0
                    column_details = parsed_ddl['columns'].get(str(row[1]).lower(), {})
                    generated = column_details.get('generated_kind') or ({2: 'VIRTUAL', 3: 'STORED'}.get(hidden, ''))
                    table_rows.append([
                        str(row[1]), str(row[2] or ''), 'YES' if row[3] else 'NO',
                        str(row[4]) if row[4] is not None else '', str(row[5]), generated,
                    ])
                output.append(tabulate(table_rows, headers=headers, tablefmt='github'))
                output.append('')

            index_rows = self._pragma_rows(table_schema, 'index_list', table_name)
            if index_rows:
                output.append('Indexes:')
                for index_row in index_rows:
                    index_name = str(index_row[1])
                    index_columns = [str(column[2]) for column in self._pragma_rows(table_schema, 'index_info', index_name) if column[2] is not None]
                    if not index_columns:
                        # An index over expressions - PRAGMA index_info has no names for it
                        index_columns, _ = self._index_expression_columns(table_schema, index_name)
                    unique_marker = 'UNIQUE ' if index_row[2] else ''
                    output.append(f"    {unique_marker}{index_name} ({', '.join(index_columns)}) [origin: {index_row[3]}]")

            foreign_keys = self._pragma_rows(table_schema, 'foreign_key_list', table_name)
            if foreign_keys:
                output.append('Foreign-key constraints:')
                for foreign_key in foreign_keys:
                    output.append(f"    {foreign_key[3]} -> {foreign_key[2]}({foreign_key[4]}) ON DELETE {foreign_key[6]} ON UPDATE {foreign_key[5]}")

            table_ddl = self._object_sql(table_schema, 'table', table_name)
            if table_ddl:
                output.append('')
                output.append('Source DDL:')
                output.append(f"    {table_ddl}")

            self.disconnect()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: get_table_description: Error fetching table description for {table_schema}.{table_name}: {e}")
            self.disconnect()
            return {'table_description': f"Error: {str(e)}"}

        return {'table_description': "\n".join(output)}

    def _table_columns_pragma(self, table_schema, table_name):
        """
        Read the column list. table_xinfo also reports hidden and generated columns;
        older SQLite versions only know table_info.
        """
        try:
            rows = self._pragma_rows(table_schema, 'table_xinfo', table_name)
            return rows, True
        except sqlite3.Error:
            rows = self._pragma_rows(table_schema, 'table_info', table_name)
            return rows, False

    def fetch_table_columns(self, settings) -> dict:
        table_schema = settings['table_schema']
        table_name = settings['table_name']
        try:
            self.connect()
            columns = self._table_columns(table_schema, table_name, settings.get('target_db_type'))
            self.disconnect()
            return columns
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: fetch_table_columns: Error fetching columns of {table_schema}.{table_name}: {e}")
            self.config_parser.print_log_message('ERROR', traceback.format_exc())
            self.disconnect()
            raise

    def _table_columns(self, table_schema, table_name, target_db_type=None) -> dict:
        """
        The column list of one table, on an already open connection. Used by
        fetch_table_columns() and by everything which needs the target type of a column
        while it is doing something else on the same connection (CHECK constraints).
        """
        columns = {}
        boolean_fallback_columns = set()
        parsed_ddl = self._parse_table_ddl(table_schema, table_name)
        rows, has_hidden = self._table_columns_pragma(table_schema, table_name)

        primary_key_columns = [row for row in rows if row[5]]
        single_column_pk = len(primary_key_columns) == 1

        for row in rows:
            column_id = row[0]
            column_name = str(row[1])
            declared_type = row[2] or ''
            not_null = bool(row[3])
            default_value = row[4]
            primary_key_position = row[5]
            hidden = row[6] if has_hidden and len(row) > 6 else 0

            base_type, character_maximum_length, numeric_precision, numeric_scale = self._parse_declared_type(declared_type)
            column_details = parsed_ddl['columns'].get(column_name.lower(), {})

            # SQLite has no boolean type. A column declared BOOLEAN, BOOL or BIT holds the
            # very same 0/1 integers as the NUMERIC(1,0) column of any other database, and
            # such a column can just as well carry a small code - which of the two it is
            # cannot be told from the declaration. The decision is therefore left to the
            # same configuration which decides it for Oracle NUMBER(1,0):
            # migration.map_numeric_1_to_boolean, or the allow-list
            # migration.numeric_1_boolean_columns. Without them the column becomes SMALLINT,
            # which is lossless and keeps the CHECK constraints, defaults and trigger
            # conditions the source wrote against 0 and 1 working.
            if base_type in ('BOOLEAN', 'BOOL', 'BIT') and not self.config_parser.should_map_numeric_1_to_boolean(
                    self.config_parser.get_target_schema(), table_name, column_name):
                self.config_parser.print_log_message('DEBUG', f"sqlite_connector: _table_columns: Column {table_schema}.{table_name}.{column_name} is declared {declared_type} - migrated as SMALLINT, because SQLite has no boolean type. Set 'map_numeric_1_to_boolean' or list the column in 'numeric_1_boolean_columns' to migrate it as BOOLEAN.")
                base_type = 'SMALLINT'
                character_maximum_length = None
                numeric_precision = None
                numeric_scale = None
                # A column of this kind holding 'true' / 'false' instead of 1 / 0 cannot be
                # a SMALLINT at all - it becomes BOOLEAN, which is the only type able to
                # hold those values without turning them into text (see the probe below).
                boolean_fallback_columns.add(column_id + 1)

            # An INTEGER PRIMARY KEY is an alias of the rowid: SQLite fills it in
            # automatically, which is exactly what a PostgreSQL identity column does.
            # The alias only exists for the declared type INTEGER on a rowid table.
            is_rowid_alias = (
                primary_key_position == 1
                and single_column_pk
                and base_type == 'INTEGER'
                and not parsed_ddl['without_rowid']
            )
            is_identity = 'YES' if (is_rowid_alias or column_details.get('autoincrement')) else 'NO'

            generated_kind = column_details.get('generated_kind', '')
            if not generated_kind and hidden in (2, 3):
                generated_kind = 'VIRTUAL' if hidden == 2 else 'STORED'
            generation_expression = column_details.get('generation_expression', '')

            columns[column_id + 1] = {
                'column_name': column_name,
                'data_type': base_type,
                'column_type': declared_type,
                'basic_data_type': '',
                'basic_character_maximum_length': '',
                'basic_numeric_precision': '',
                'basic_numeric_scale': '',
                'basic_column_type': '',
                'character_maximum_length': character_maximum_length,
                'numeric_precision': numeric_precision,
                'numeric_scale': numeric_scale,
                'is_nullable': 'NO' if (not_null or is_rowid_alias) else 'YES',
                'column_default_name': '',
                'column_default_value': '' if default_value is None else str(default_value),
                'is_identity': is_identity,
                'is_generated_stored': 'YES' if generated_kind == 'STORED' else 'NO',
                'is_generated_virtual': 'YES' if generated_kind == 'VIRTUAL' else 'NO',
                'generation_expression': generation_expression,
                'stripped_generation_expression': self._transpile(generation_expression, f'generated column {table_name}.{column_name}') if generation_expression else '',
                'column_comment': '',
                'udt_schema': '',
                'udt_name': '',
                'domain_schema': '',
                'domain_name': '',
                # hidden = 1 marks a hidden column of a virtual table - it carries no
                # data of its own and must not become a column of the target table.
                'is_hidden_column': 'YES' if hidden == 1 else 'NO',
            }

        # The declared type is only a hint in SQLite - the values really stored decide
        # whether the type it was mapped to can hold them (see _widened_types_by_stored_values).
        target_db_type = target_db_type or self.config_parser.get_target_db_type()
        widened_types = self._widened_types_by_stored_values(table_schema, table_name, columns, target_db_type, boolean_fallback_columns)
        types_mapping = self.get_types_mapping({'target_db_type': target_db_type}) if widened_types else {}
        for order_num, (widened_type, reasons) in widened_types.items():
            column = columns[order_num]
            declared_type = column['column_type'] or '(no type)'
            category = self._target_type_category(types_mapping.get(str(column['data_type']).upper(), ''))
            if widened_type == 'BOOLEAN':
                self.config_parser.print_log_message('WARNING',
                    f"sqlite_connector: _table_columns: Column {table_schema}.{table_name}.{column['column_name']} is declared {declared_type} and holds its values as text ('true' / 'false') "
                    f"- it is migrated as BOOLEAN and not as SMALLINT, which cannot hold them. Set 'map_numeric_1_to_boolean' or "
                    f"'numeric_1_boolean_columns' to migrate every such column as BOOLEAN.")
            else:
                found = ', '.join(self._describe_stored_value_conflict(reason, category, declared_type) for reason in reasons)
                self.config_parser.print_log_message('WARNING',
                    f"sqlite_connector: _table_columns: Column {table_schema}.{table_name}.{column['column_name']} is declared {declared_type}, "
                    f"but holds {found} - it is migrated as {widened_type}, otherwise the data of the whole table would be rejected by the target. "
                    f"Use 'data_types_substitution' to force another type.")
            column['data_type'] = widened_type
            column['character_maximum_length'] = None
            column['numeric_precision'] = None
            column['numeric_scale'] = None

        return columns

    def get_types_mapping(self, settings):
        target_db_type = settings['target_db_type']
        if target_db_type != 'postgresql':
            raise ValueError(f"Unsupported target database type: {target_db_type}")

        # SQLite accepts any declared type, so the mapping covers the type names of the
        # dialects SQLite databases are typically created from, not just its own five
        # storage classes.
        return {
            # integers - a SQLite INTEGER holds up to 8 bytes, so BIGINT is the lossless target
            'INT': 'INTEGER',
            'INTEGER': 'BIGINT',
            'TINYINT': 'SMALLINT',
            'SMALLINT': 'SMALLINT',
            'MEDIUMINT': 'INTEGER',
            'BIGINT': 'BIGINT',
            'UNSIGNED BIG INT': 'NUMERIC',
            'INT2': 'SMALLINT',
            'INT4': 'INTEGER',
            'INT8': 'BIGINT',
            'SERIAL': 'BIGINT',
            'BIGSERIAL': 'BIGINT',

            # text
            'CHARACTER': 'CHAR',
            'CHAR': 'CHAR',
            'NCHAR': 'CHAR',
            'NATIVE CHARACTER': 'CHAR',
            'VARCHAR': 'VARCHAR',
            'VARYING CHARACTER': 'VARCHAR',
            'NVARCHAR': 'VARCHAR',
            'NVARCHAR2': 'VARCHAR',
            'VARCHAR2': 'VARCHAR',
            'TEXT': 'TEXT',
            'NTEXT': 'TEXT',
            'CLOB': 'TEXT',
            'STRING': 'TEXT',
            'LONGTEXT': 'TEXT',
            'MEDIUMTEXT': 'TEXT',
            'TINYTEXT': 'TEXT',

            # binary
            'BLOB': 'BYTEA',
            'LONGBLOB': 'BYTEA',
            'MEDIUMBLOB': 'BYTEA',
            'TINYBLOB': 'BYTEA',
            'BINARY': 'BYTEA',
            'VARBINARY': 'BYTEA',
            'BYTEA': 'BYTEA',
            'IMAGE': 'BYTEA',

            # approximate numeric
            'REAL': 'DOUBLE PRECISION',
            'DOUBLE': 'DOUBLE PRECISION',
            'DOUBLE PRECISION': 'DOUBLE PRECISION',
            'FLOAT': 'DOUBLE PRECISION',

            # exact numeric
            'NUMERIC': 'NUMERIC',
            'DECIMAL': 'NUMERIC',
            'NUMBER': 'NUMERIC',
            'MONEY': 'NUMERIC',
            'SMALLMONEY': 'NUMERIC',

            # date / time - SQLite stores these as text, integer or real
            'DATE': 'DATE',
            'DATETIME': 'TIMESTAMP',
            'DATETIME2': 'TIMESTAMP',
            'SMALLDATETIME': 'TIMESTAMP',
            'TIMESTAMP': 'TIMESTAMP',
            'TIMESTAMPTZ': 'TIMESTAMP WITH TIME ZONE',
            'TIME': 'TIME',
            'YEAR': 'INTEGER',

            # misc
            'BOOLEAN': 'BOOLEAN',
            'BOOL': 'BOOLEAN',
            'BIT': 'BOOLEAN',
            'JSON': 'JSONB',
            'JSONB': 'JSONB',
            'UUID': 'UUID',
            'GUID': 'UUID',
            'UNIQUEIDENTIFIER': 'UUID',
            'XML': 'XML',
            # a column declared without any type at all
            '': 'TEXT',
        }

    def is_string_type(self, column_type: str) -> bool:
        string_types = ['CHAR', 'VARCHAR', 'NCHAR', 'NVARCHAR', 'NVARCHAR2', 'VARCHAR2', 'TEXT', 'NTEXT',
                        'CLOB', 'STRING', 'CHARACTER', 'VARYING CHARACTER', 'NATIVE CHARACTER',
                        'LONGTEXT', 'MEDIUMTEXT', 'TINYTEXT']
        return str(column_type).upper() in string_types

    def is_numeric_type(self, column_type: str) -> bool:
        numeric_types = ['BIGINT', 'INTEGER', 'INT', 'INT2', 'INT4', 'INT8', 'TINYINT', 'SMALLINT',
                         'MEDIUMINT', 'FLOAT', 'REAL', 'DOUBLE', 'DOUBLE PRECISION', 'DECIMAL',
                         'NUMERIC', 'NUMBER', 'MONEY', 'SMALLMONEY']
        return str(column_type).upper() in numeric_types

    def _is_lob_type(self, declared_type) -> bool:
        upper_type = str(declared_type or '').upper()
        return any(token in upper_type for token in ('BLOB', 'CLOB', 'BINARY', 'IMAGE'))

    ## ---------------------------------------------------------------- type affinity

    # A SQLite column has a declared type and a type affinity, but neither of them is
    # enforced. The affinity only says how a value is converted when it CAN be converted
    # without loss - a well formed number written into an INTEGER column becomes an
    # integer - while everything else is stored exactly as it was given. A column declared
    # INTEGER therefore really holds the text 'N/A', and handing that to a PostgreSQL
    # BIGINT column ends the whole batch with
    # 'invalid input syntax for type bigint: "N/A"'. The declared type alone cannot decide
    # the target type; the storage classes the column really contains have to decide with it.

    # The text forms _coerce_boolean() understands. A BOOLEAN column holding one of them
    # stays BOOLEAN, any other text makes it TEXT.
    _BOOLEAN_TEXT_VALUES = ('1', 't', 'true', 'y', 'yes', 'on', '0', 'f', 'false', 'n', 'no', 'off', '')

    # Value range of the PostgreSQL integer types - a SQLite INTEGER is always 8 bytes,
    # so a column declared TINYINT can hold a number the target SMALLINT rejects.
    _INTEGER_RANGES = {
        'SMALLINT': (-32768, 32767),
        'INTEGER': (-2147483648, 2147483647),
    }

    # How many aggregate expressions are sent in one probe query - SQLITE_MAX_COLUMN
    # defaults to 2000, and a very wide table would otherwise exceed it.
    _PROBE_CHUNK_SIZE = 400

    @staticmethod
    def _target_type_category(target_type):
        """ The kind of value the mapped PostgreSQL type accepts. """
        upper = str(target_type or '').upper()
        if upper in ('SMALLINT', 'INTEGER', 'BIGINT'):
            return 'integer'
        if upper.startswith('NUMERIC') or upper.startswith('DECIMAL'):
            return 'numeric'
        if upper in ('REAL', 'DOUBLE PRECISION', 'FLOAT'):
            return 'real'
        if upper in ('BOOLEAN', 'BOOL'):
            return 'boolean'
        if upper.startswith('TIMESTAMP'):
            return 'timestamp'
        if upper == 'DATE':
            return 'date'
        if upper.startswith('TIME'):
            return 'time'
        # TEXT, VARCHAR, BYTEA, JSONB, UUID, XML - every storage class can be migrated
        # into these, the value conversion of the data migration takes care of it.
        return 'other'

    def _stored_value_checks(self, quoted_column, category, target_type):
        """
        The SQL expressions which report - per storage class - whether a column holds a
        value the target type cannot accept. Each returns 1 when at least one such value
        exists. They are all evaluated in a single pass over the table.
        """
        checks = []
        # A BLOB never fits a numeric, boolean or date/time column
        checks.append(('blob', f"typeof({quoted_column}) = 'blob'"))

        if category == 'integer':
            # Text left in a column with integer affinity is text SQLite could not read as
            # a number - a well formed integer literal would have been converted on INSERT.
            checks.append(('text', f"typeof({quoted_column}) = 'text' AND "
                                   f"(trim({quoted_column}) GLOB '*[^0-9+-]*' OR NOT trim({quoted_column}) GLOB '*[0-9]*')"))
            # 1.5 in a column declared INTEGER is stored as REAL and rejected by an integer target
            checks.append(('real', f"typeof({quoted_column}) = 'real' AND {quoted_column} <> cast({quoted_column} AS INTEGER)"))
            value_range = self._INTEGER_RANGES.get(str(target_type).upper())
            if value_range:
                checks.append(('range', f"typeof({quoted_column}) = 'integer' AND "
                                        f"({quoted_column} < {value_range[0]} OR {quoted_column} > {value_range[1]})"))
        elif category in ('numeric', 'real'):
            checks.append(('text', f"typeof({quoted_column}) = 'text' AND "
                                   f"(trim({quoted_column}) GLOB '*[^0-9eE+.-]*' OR NOT trim({quoted_column}) GLOB '*[0-9]*')"))
        elif category == 'boolean':
            checks.append(('text', self._boolean_text_check(quoted_column)))
        elif category in ('date', 'time', 'timestamp'):
            # date() / time() / datetime() return NULL for anything they cannot read as a
            # point in time - exactly the values PostgreSQL would refuse as well.
            function = {'date': 'date', 'time': 'time', 'timestamp': 'datetime'}[category]
            checks.append(('text', f"typeof({quoted_column}) = 'text' AND {function}({quoted_column}) IS NULL"))
        return checks

    def _boolean_text_check(self, quoted_column):
        """ Reports a text value which _coerce_boolean() would not recognize as a boolean. """
        values = ', '.join(f"'{value}'" for value in self._BOOLEAN_TEXT_VALUES)
        return f"typeof({quoted_column}) = 'text' AND lower(trim({quoted_column})) NOT IN ({values})"

    def _widened_types_by_stored_values(self, table_schema, table_name, columns, target_db_type, boolean_fallback_columns=None):
        """
        Find the columns whose values do not fit the type their declaration was mapped to
        and return {order_num: (widened type, reasons)}. The whole table is read once, with
        one aggregate expression per check, so this costs a single sequential scan.
        """
        cache_key = (str(table_schema).lower(), str(table_name).lower())
        if cache_key in self._stored_values_cache:
            return self._stored_values_cache[cache_key]

        widened = {}
        boolean_fallback_columns = boolean_fallback_columns or set()
        try:
            types_mapping = self.get_types_mapping({'target_db_type': target_db_type})
        except Exception:
            # Only PostgreSQL is supported as a target, but the mapping must never be the
            # reason the column list cannot be read.
            self._stored_values_cache[cache_key] = widened
            return widened

        probes = []
        categories = {}
        for order_num, column in columns.items():
            if column.get('is_hidden_column') == 'YES':
                continue
            # An INTEGER PRIMARY KEY / AUTOINCREMENT column is the rowid, SQLite itself
            # guarantees it holds nothing but integers.
            if column.get('is_identity') == 'YES':
                continue
            declared_type = str(column.get('data_type') or '').upper()
            target_type = types_mapping.get(declared_type, '')
            category = self._target_type_category(target_type)
            if category == 'other':
                continue
            categories[order_num] = category
            quoted_column = self._quote_ident(column['column_name'])
            checks = self._stored_value_checks(quoted_column, category, target_type)
            if order_num in boolean_fallback_columns:
                # A column declared BOOLEAN which the configuration turns into SMALLINT is
                # additionally tested for boolean words, to tell 'true' from 'N/A'.
                checks.append(('boolean_text', self._boolean_text_check(quoted_column)))
            for reason, expression in checks:
                probes.append((order_num, reason, expression))

        if not probes:
            self._stored_values_cache[cache_key] = widened
            return widened

        conflicts = {}
        qualified_name = self._qualified_name(table_schema, table_name)
        try:
            cursor = self.connection.cursor()
            for offset in range(0, len(probes), self._PROBE_CHUNK_SIZE):
                chunk = probes[offset:offset + self._PROBE_CHUNK_SIZE]
                expressions = ', '.join(f"max({expression})" for _, _, expression in chunk)
                cursor.execute(f"SELECT {expressions} FROM {qualified_name}")
                row = cursor.fetchone()
                for (order_num, reason, _), found in zip(chunk, row or []):
                    if found:
                        conflicts.setdefault(order_num, []).append(reason)
            cursor.close()
        except sqlite3.Error as e:
            # Without the probe the declared types are used as before - the migration is
            # not stopped by it, but the reason a later INSERT may fail has to be visible.
            self.config_parser.print_log_message('WARNING', f"sqlite_connector: _table_columns: Could not check the stored values of {table_schema}.{table_name} against the declared column types ({e}) - the declared types are used unchanged.")
            self._stored_values_cache[cache_key] = widened
            return widened

        for order_num, reasons in conflicts.items():
            if (order_num in boolean_fallback_columns and 'text' in reasons
                    and 'boolean_text' not in reasons and 'blob' not in reasons):
                # The text in the column is 'true' / 'false' / 'yes' / 'no' - the column
                # really is a flag, it is only not written as 0 and 1.
                widened[order_num] = ('BOOLEAN', reasons)
                continue
            if 'boolean_text' in reasons:
                reasons = [reason for reason in reasons if reason != 'text']
            if 'blob' in reasons or 'text' in reasons or 'boolean_text' in reasons:
                # TEXT holds every storage class SQLite knows
                new_type = 'TEXT'
            elif 'real' in reasons:
                # NUMERIC keeps both the integers and the fractional values exactly
                new_type = 'NUMERIC'
            elif 'range' in reasons:
                new_type = 'BIGINT'
            else:
                continue
            widened[order_num] = (new_type, reasons)

        self._stored_values_cache[cache_key] = widened
        return widened

    def _describe_stored_value_conflict(self, reason, category, declared_type):
        if reason == 'blob':
            return 'values stored as BLOB'
        if reason == 'real':
            return 'values stored as REAL, which an integer column cannot hold'
        if reason == 'range':
            return f'integer values outside the range of the target type of {declared_type}'
        if reason == 'boolean_text':
            return 'text values which are neither a number nor a boolean'
        if category == 'boolean':
            return 'text values which are not a boolean'
        if category in ('date', 'time', 'timestamp'):
            return 'text values which are not a valid date or time'
        return 'text values which are not a number'


    ## ---------------------------------------------------------------- CHECK constraints

    def _target_column_types(self, table_schema, table_name):
        """
        The PostgreSQL type every column of the table is created with, keyed by the lower
        cased column name. That is the mapping of the declared type, corrected by the
        values really stored (see _widened_types_by_stored_values) and by the narrow
        numeric rule of the target DDL builder, which turns a NUMERIC(1,0) column into
        BOOLEAN or SMALLINT depending on the configuration - the same decision is repeated
        here, with the same configuration helper, because a CHECK constraint has to know
        the type of the column it is written against.
        """
        target_types = {}
        try:
            columns = self._table_columns(table_schema, table_name)
            types_mapping = self.get_types_mapping({'target_db_type': self.config_parser.get_target_db_type()})
        except Exception as e:
            self.config_parser.print_log_message('DEBUG', f"sqlite_connector: _target_column_types: Could not resolve the target types of {table_schema}.{table_name}: {e}")
            return target_types

        for column in columns.values():
            data_type = str(column.get('data_type') or '').upper()
            target_type = str(types_mapping.get(data_type, data_type)).upper()
            if target_type in ('NUMBER', 'NUMERIC') and column.get('numeric_precision') == 1 and column.get('numeric_scale') == 0:
                target_type = 'BOOLEAN' if self.config_parser.should_map_numeric_1_to_boolean(
                    self.config_parser.get_target_schema(), table_name, column['column_name']) else 'SMALLINT'
            target_types[str(column['column_name']).lower()] = target_type
        return target_types

    def _adapt_condition_to_target_types(self, tree, target_types):
        """
        Rewrite the literals of a condition to the types the target columns really have.
        SQLite writes a flag as 0 and 1 even when the column is declared BOOLEAN, so
        'eu_member IN (0, 1)' reaches a PostgreSQL BOOLEAN column as
        'operator does not exist: boolean = integer'.

        Returns the list of the reasons the condition cannot be used at all - it is empty
        when the condition was either adapted or needed no adaptation.
        """
        problems = []
        comparisons = (sqlglot.exp.EQ, sqlglot.exp.NEQ, sqlglot.exp.GT, sqlglot.exp.GTE,
                       sqlglot.exp.LT, sqlglot.exp.LTE)

        def adapt(column_node, literal_nodes):
            target_type = target_types.get(str(column_node.name).lower())
            if not target_type or not literal_nodes:
                return
            if target_type in ('BOOLEAN', 'BOOL'):
                for literal in literal_nodes:
                    value = str(literal.name).strip().lower()
                    if value in ('0', 'false', 'f', 'n', 'no', 'off'):
                        literal.replace(sqlglot.exp.false())
                    elif value in ('1', 'true', 't', 'y', 'yes', 'on'):
                        literal.replace(sqlglot.exp.true())
                    else:
                        problems.append(f"column {column_node.name} is migrated as BOOLEAN and cannot be compared with {literal.sql()}")
            elif target_type in ('TEXT', 'VARCHAR', 'CHAR', 'BYTEA'):
                # A column which had to be widened to TEXT because of the values it holds
                # cannot be compared with a number any more - PostgreSQL has no operator
                # for it, and quoting the number would compare it as a string.
                for literal in literal_nodes:
                    if not literal.is_string:
                        problems.append(f"column {column_node.name} is migrated as {target_type} and cannot be compared with the number {literal.sql()}")

        for node in list(tree.find_all(*comparisons)):
            left, right = node.this, node.expression
            if isinstance(left, sqlglot.exp.Column) and isinstance(right, sqlglot.exp.Literal):
                adapt(left, [right])
            elif isinstance(right, sqlglot.exp.Column) and isinstance(left, sqlglot.exp.Literal):
                adapt(right, [left])

        for node in list(tree.find_all(sqlglot.exp.In)):
            if isinstance(node.this, sqlglot.exp.Column):
                literals = [item for item in (node.args.get('expressions') or []) if isinstance(item, sqlglot.exp.Literal)]
                adapt(node.this, literals)

        for node in list(tree.find_all(sqlglot.exp.Between)):
            if isinstance(node.this, sqlglot.exp.Column):
                bounds = [node.args.get('low'), node.args.get('high')]
                adapt(node.this, [bound for bound in bounds if isinstance(bound, sqlglot.exp.Literal)])

        return problems

    def _convert_check_expression(self, check_sql, target_types, description):
        """
        Translate a CHECK expression to PostgreSQL and adapt its literals to the types the
        target columns really have. Returns None when the expression cannot be used on the
        target at all - the constraint is then skipped instead of failing the migration.
        """
        try:
            tree = sqlglot.parse_one(check_sql, read='sqlite')
        except Exception as e:
            # Without a parse tree the expression can only be handed over as it is,
            # exactly as before - a type mismatch in it surfaces when it is created.
            self.config_parser.print_log_message('DEBUG', f"sqlite_connector: _convert_check_expression: sqlglot could not parse {description}: {e}")
            return self._transpile(check_sql, description)

        problems = self._adapt_condition_to_target_types(tree, target_types)
        if problems:
            self.config_parser.print_log_message('WARNING', f"sqlite_connector: fetch_constraints: The {description} ({check_sql}) is not migrated: {'; '.join(problems)}. The values of the source stay as they are - recreate the constraint by hand if the target has to enforce it.")
            return None
        return self._finalize_sql(tree.sql(dialect='postgres'))

    ## ---------------------------------------------------------------- indexes

    def _index_expression_columns(self, schema_name, index_name):
        """
        PRAGMA index_info reports NULL for an expression of a functional index, so the
        expressions have to be read from the CREATE INDEX statement.
        """
        index_sql = self._object_sql(schema_name, 'index', index_name)
        if not index_sql:
            return [], ''
        clean_sql = self._strip_sql_comments(index_sql)
        content, end_index = self._find_paren_group(clean_sql)
        if not content:
            return [], ''
        where_clause = ''
        where_match = re.search(r'(?is)\bWHERE\b\s*(.*)$', clean_sql[end_index:])
        if where_match:
            where_clause = where_match.group(1).strip().rstrip(';').strip()
        return self._split_top_level(content), where_clause

    def fetch_indexes(self, settings):
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

        table_indexes = {}
        order_num = 1
        try:
            self.connect()
            parsed_ddl = self._parse_table_ddl(source_table_schema, source_table_name)
            column_rows, _ = self._table_columns_pragma(source_table_schema, source_table_name)

            # PRIMARY KEY - on a rowid table SQLite does not create an index for an
            # INTEGER PRIMARY KEY, so the primary key is always taken from table_info.
            primary_key_columns = [str(row[1]) for row in sorted((row for row in column_rows if row[5]), key=lambda row: row[5])]
            if primary_key_columns:
                table_indexes[order_num] = {
                    'index_name': 'pk',
                    'index_owner': self._resolve_schema(source_table_schema),
                    'index_type': 'PRIMARY KEY',
                    'index_columns': ', '.join(primary_key_columns),
                    'index_comment': '',
                    'is_function_based': 'NO',
                }
                order_num += 1

            unique_constraint_number = 0
            for index_row in self._pragma_rows(source_table_schema, 'index_list', source_table_name):
                index_name = str(index_row[1])
                is_unique = bool(index_row[2])
                origin = str(index_row[3])
                is_partial = bool(index_row[4]) if len(index_row) > 4 else False

                if origin == 'pk':
                    # Already covered by the primary key entry above
                    continue

                index_columns = []
                is_function_based = 'NO'
                has_expression = False
                for column_row in self._pragma_rows(source_table_schema, 'index_info', index_name):
                    if column_row[2] is None:
                        has_expression = True
                    else:
                        index_columns.append(str(column_row[2]))

                where_clause = ''
                if has_expression or is_partial:
                    definition_columns, where_clause = self._index_expression_columns(source_table_schema, index_name)
                    if has_expression and definition_columns:
                        index_columns = []
                        for definition in definition_columns:
                            definition = re.sub(r'(?is)\s+(ASC|DESC)\s*$', '', definition).strip()
                            definition = re.sub(r'(?is)\s+COLLATE\s+' + self.IDENTIFIER_PATTERN, '', definition).strip()
                            if re.fullmatch(self.IDENTIFIER_PATTERN, definition):
                                index_columns.append(self._unquote_ident(definition))
                            else:
                                is_function_based = 'YES'
                                index_columns.append(f"({self._transpile(definition, f'index {index_name}')})")

                if not index_columns:
                    self.config_parser.print_log_message('WARNING', f"sqlite_connector: fetch_indexes: Index {index_name} on {source_table_name} has no usable columns - skipping.")
                    continue

                index_comment = ''
                if is_partial:
                    # PostgreSQL supports partial indexes, but the migrator's index model
                    # only carries the column list. A partial index is therefore created
                    # over the whole table, and a partial UNIQUE index is degraded to a
                    # plain index - keeping it unique would reject rows SQLite accepted.
                    index_comment = (f"[PARTIAL INDEX] Original SQLite definition was restricted by: WHERE {where_clause}. "
                                     f"Recreate it manually if the restriction matters.")
                    self.config_parser.print_log_message('WARNING', f"sqlite_connector: fetch_indexes: Index {index_name} on {source_table_name} is partial (WHERE {where_clause}) - migrated as a full{' non-unique' if is_unique else ''} index.")
                    is_unique = False

                if origin == 'u' and index_name.startswith('sqlite_autoindex_'):
                    # Auto-generated name of a UNIQUE table constraint - a readable name
                    # is nicer in the target database than sqlite_autoindex_<table>_<n>
                    unique_constraint_number += 1
                    index_name = f"uq_{unique_constraint_number}"

                table_indexes[order_num] = {
                    'index_name': index_name,
                    'index_owner': self._resolve_schema(source_table_schema),
                    'index_type': 'UNIQUE' if is_unique else 'INDEX',
                    'index_columns': ', '.join(index_columns),
                    'index_comment': index_comment,
                    'is_function_based': is_function_based,
                }
                order_num += 1

            self.disconnect()
            return table_indexes
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: fetch_indexes: Error fetching indexes of {source_table_schema}.{source_table_name}: {e}")
            self.disconnect()
            raise

    ## ---------------------------------------------------------------- constraints

    def _primary_key_columns(self, schema_name, table_name):
        rows, _ = self._table_columns_pragma(schema_name, table_name)
        return [str(row[1]) for row in sorted((row for row in rows if row[5]), key=lambda row: row[5])]

    def fetch_constraints(self, settings):
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

        constraints = {}
        order_num = 1
        try:
            self.connect()
            schema = self._resolve_schema(source_table_schema)

            # Foreign keys. PRAGMA foreign_key_list returns one row per column, grouped by id.
            foreign_keys = {}
            for row in self._pragma_rows(source_table_schema, 'foreign_key_list', source_table_name):
                foreign_key_id = row[0]
                referenced_table = str(row[2])
                from_column = str(row[3]) if row[3] is not None else None
                to_column = str(row[4]) if row[4] is not None else None
                on_update = str(row[5] or 'NO ACTION').upper()
                on_delete = str(row[6] or 'NO ACTION').upper()

                if foreign_key_id not in foreign_keys:
                    foreign_keys[foreign_key_id] = {
                        'referenced_table_name': referenced_table,
                        'columns': [],
                        'referenced_columns': [],
                        'update_rule': on_update,
                        'delete_rule': on_delete,
                        'implicit_reference': False,
                    }
                if from_column:
                    foreign_keys[foreign_key_id]['columns'].append(from_column)
                if to_column:
                    foreign_keys[foreign_key_id]['referenced_columns'].append(to_column)
                else:
                    # "REFERENCES parent" without a column list points at the parent's primary key
                    foreign_keys[foreign_key_id]['implicit_reference'] = True

            for foreign_key_id, foreign_key in sorted(foreign_keys.items()):
                referenced_columns = foreign_key['referenced_columns']
                if foreign_key['implicit_reference'] or not referenced_columns:
                    referenced_columns = self._primary_key_columns(source_table_schema, foreign_key['referenced_table_name'])
                    if not referenced_columns:
                        self.config_parser.print_log_message('WARNING', f"sqlite_connector: fetch_constraints: Foreign key {foreign_key_id} of {source_table_name} references {foreign_key['referenced_table_name']} which has no primary key - skipping.")
                        continue

                constraints[order_num] = {
                    'constraint_name': self._safe_object_name('fk', source_table_name, foreign_key['referenced_table_name'], foreign_key_id),
                    'constraint_owner': schema,
                    'constraint_type': 'FOREIGN KEY',
                    'constraint_columns': ', '.join(foreign_key['columns']),
                    'referenced_table_schema': schema,
                    'referenced_table_name': foreign_key['referenced_table_name'],
                    'referenced_columns': ', '.join(referenced_columns),
                    'constraint_sql': '',
                    'constraint_comment': '',
                    'delete_rule': foreign_key['delete_rule'],
                    'update_rule': foreign_key['update_rule'],
                    'constraint_status': 'ENABLED',
                }
                order_num += 1

            # CHECK constraints are not exposed by any PRAGMA - they come from the DDL
            parsed_ddl = self._parse_table_ddl(source_table_schema, source_table_name)
            # The literals of a CHECK have to match the types the target columns really
            # get - SQLite writes a flag as 0 / 1 even in a column declared BOOLEAN.
            target_types = self._target_column_types(source_table_schema, source_table_name) if parsed_ddl['checks'] else {}
            for check in parsed_ddl['checks']:
                converted_check = self._convert_check_expression(
                    check['sql'], target_types, f"check constraint {check['name']} of {source_table_name}")
                if not converted_check or not converted_check.strip():
                    continue
                constraints[order_num] = {
                    'constraint_name': check['name'],
                    'constraint_owner': schema,
                    'constraint_type': 'CHECK',
                    'constraint_columns': check['column'],
                    'referenced_table_schema': '',
                    'referenced_table_name': '',
                    'referenced_columns': '',
                    'constraint_sql': converted_check,
                    'constraint_comment': '',
                    'delete_rule': '',
                    'update_rule': '',
                    'constraint_status': 'ENABLED',
                }
                order_num += 1

            self.disconnect()
            return constraints
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: fetch_constraints: Error fetching constraints of {source_table_schema}.{source_table_name}: {e}")
            self.disconnect()
            raise

    ## ---------------------------------------------------------------- triggers

    TRIGGER_PATTERN = re.compile(
        r'(?is)^\s*CREATE\s+(?:TEMP(?:ORARY)?\s+)?TRIGGER\s+(?:IF\s+NOT\s+EXISTS\s+)?'
        r'(?P<name>' + IDENTIFIER_PATTERN + r'(?:\s*\.\s*(?:' + IDENTIFIER_PATTERN + r'))?)\s+'
        r'(?:(?P<timing>BEFORE|AFTER|INSTEAD\s+OF)\s+)?'
        r'(?P<event>DELETE|INSERT|UPDATE)(?P<update_of>\s+OF\s+.*?)?\s+'
        r'ON\s+(?P<table>' + IDENTIFIER_PATTERN + r'(?:\s*\.\s*(?:' + IDENTIFIER_PATTERN + r'))?)\s*'
        r'(?:FOR\s+EACH\s+ROW\s*)?'
        r'(?:WHEN\s+(?P<when>.*?)\s*)?'
        r'\bBEGIN\b\s*(?P<body>.*?)\s*\bEND\s*;?\s*$'
    )

    def fetch_triggers(self, table_id: int, table_schema: str, table_name: str):
        triggers = {}
        order_num = 1
        try:
            self.connect()
            for row in self._fetch_master(table_schema, 'trigger'):
                if str(row[3]) != table_name:
                    continue
                trigger_sql = row[4] or ''
                match = self.TRIGGER_PATTERN.match(self._strip_sql_comments(trigger_sql))
                event = ''
                if match:
                    timing = re.sub(r'\s+', ' ', (match.group('timing') or 'BEFORE')).upper()
                    event = f"{timing} {match.group('event').upper()}"
                triggers[order_num] = {
                    'id': row[0],
                    'name': str(row[2]),
                    'sql': trigger_sql,
                    'event': event,
                    'new': '',
                    'old': '',
                    'comment': '',
                }
                order_num += 1
            self.disconnect()
            return triggers
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: fetch_triggers: Error fetching triggers of {table_schema}.{table_name}: {e}")
            self.disconnect()
            raise

    def _convert_trigger_body(self, body, target_schema_name, object_names):
        """
        Translate the statement list of a SQLite trigger body into PL/pgSQL statements.
        SQLite trigger bodies contain plain INSERT / UPDATE / DELETE / SELECT statements
        plus the RAISE() function, all of which have a direct PL/pgSQL counterpart.
        """
        converted_statements = []
        for statement in self._split_top_level(body, ';'):
            statement = statement.strip()
            if not statement:
                continue

            # SELECT RAISE(ABORT, 'text') is SQLite's way of rejecting a row
            raise_match = re.match(
                r"(?is)^(?:SELECT\s+)?RAISE\s*\(\s*(ABORT|ROLLBACK|FAIL)\s*,\s*(.+?)\s*\)\s*$", statement)
            if raise_match:
                converted_statements.append(f"RAISE EXCEPTION {raise_match.group(2).strip()};")
                continue
            if re.match(r"(?is)^(?:SELECT\s+)?RAISE\s*\(\s*IGNORE\s*\)\s*$", statement):
                converted_statements.append("RETURN NULL;")
                continue

            converted = self._transpile(statement, 'trigger body statement')
            converted = self._qualify_object_names(converted, target_schema_name, object_names)
            converted_statements.append(converted.rstrip(';') + ';')
        return converted_statements

    def convert_trigger(self, settings: dict):
        """
        Build a PL/pgSQL trigger function plus the CREATE TRIGGER statement for it.
        Both statements are returned as one script - the orchestrator executes the whole
        converted code in a single call.
        """
        trigger_sql = settings.get('trigger_sql') or ''
        trigger_name = settings.get('trigger_name') or ''
        target_schema_name = settings['target_schema_name']
        target_table_name = self.config_parser.convert_names_case(settings['target_table_name'])

        match = self.TRIGGER_PATTERN.match(self._strip_sql_comments(trigger_sql))
        if not match:
            self.config_parser.print_log_message('WARNING', f"sqlite_connector: convert_trigger: Could not parse trigger {trigger_name} - it must be migrated manually. Source: {trigger_sql}")
            return ''

        timing = re.sub(r'\s+', ' ', (match.group('timing') or 'BEFORE')).upper()
        event = match.group('event').upper()
        update_of = ''
        if match.group('update_of'):
            update_columns = re.sub(r'(?is)^\s*OF\s+', '', match.group('update_of').strip())
            quoted_columns = [f'"{self.config_parser.convert_names_case(self._unquote_ident(column))}"'
                              for column in self._split_top_level(update_columns)]
            if quoted_columns:
                update_of = ' OF ' + ', '.join(quoted_columns)

        try:
            self.connect()
            object_names = self._migrated_object_names(settings.get('source_schema_name'))
            self.disconnect()
        except Exception:
            object_names = set()

        statements = self._convert_trigger_body(match.group('body') or '', target_schema_name, object_names)
        if not statements:
            self.config_parser.print_log_message('WARNING', f"sqlite_connector: convert_trigger: Trigger {trigger_name} has an empty body after conversion - skipping.")
            return ''

        if timing == 'INSTEAD OF':
            return_statement = 'RETURN OLD;' if event == 'DELETE' else 'RETURN NEW;'
        elif timing == 'BEFORE':
            return_statement = 'RETURN OLD;' if event == 'DELETE' else 'RETURN NEW;'
        else:
            return_statement = 'RETURN NULL;'

        converted_trigger_name = self.config_parser.convert_names_case(trigger_name)
        function_name = self.config_parser.convert_names_case(f"{trigger_name}_fn")

        when_clause = ''
        if match.group('when'):
            converted_when = self._transpile(match.group('when').strip(), f'trigger {trigger_name} WHEN clause')
            if converted_when:
                when_clause = f"\nWHEN ({converted_when})"

        indented_body = "\n".join(f"    {statement}" for statement in statements)
        # search_path is fixed on the function so that the unqualified names a SQLite
        # trigger body uses resolve in the migrated schema.
        function_sql = (
            f'CREATE OR REPLACE FUNCTION "{target_schema_name}"."{function_name}"() RETURNS trigger\n'
            f'LANGUAGE plpgsql\n'
            f'SET search_path = "{target_schema_name}", pg_catalog\n'
            f'AS $trigger$\n'
            f'BEGIN\n'
            f'{indented_body}\n'
            f'    {return_statement}\n'
            f'END;\n'
            f'$trigger$;'
        )
        trigger_ddl = (
            f'CREATE TRIGGER "{converted_trigger_name}"\n'
            f'{timing} {event}{update_of} ON "{target_schema_name}"."{target_table_name}"\n'
            f'FOR EACH ROW{when_clause}\n'
            f'EXECUTE FUNCTION "{target_schema_name}"."{function_name}"();'
        )
        return f"{function_sql}\n{trigger_ddl}"

    ## ---------------------------------------------------------------- views

    VIEW_HEADER_PATTERN = re.compile(
        r'(?is)^\s*CREATE\s+(?:TEMP(?:ORARY)?\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?'
        r'(?:' + IDENTIFIER_PATTERN + r')(?:\s*\.\s*(?:' + IDENTIFIER_PATTERN + r'))?\s*'
        r'(?:\([^)]*\)\s*)?AS\s+'
    )

    def fetch_views_names(self, source_schema_name: str):
        views = {}
        order_num = 1
        try:
            self.connect()
            schema = self._resolve_schema(source_schema_name)
            for row in self._fetch_master(source_schema_name, 'view'):
                view_name = str(row[2])
                if view_name.startswith('sqlite_'):
                    continue
                views[order_num] = {
                    'id': row[0],
                    'schema_name': schema,
                    'view_name': view_name,
                    'comment': '',
                    'view_type': 'VIEW',
                }
                order_num += 1
            self.disconnect()
            return views
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: fetch_views_names: Error fetching view names: {e}")
            self.disconnect()
            raise

    def fetch_view_code(self, settings):
        source_schema_name = settings['source_schema_name']
        source_view_name = settings['source_view_name']
        try:
            self.connect()
            view_sql = self._object_sql(source_schema_name, 'view', source_view_name)
            self.disconnect()
            if not view_sql:
                return ''
            clean_sql = self._strip_sql_comments(view_sql).strip().rstrip(';')
            match = self.VIEW_HEADER_PATTERN.match(clean_sql)
            if match:
                return clean_sql[match.end():].strip()
            # Without a recognizable header the whole statement is handed over - the
            # conversion step logs it, so the user can see what has to be fixed.
            self.config_parser.print_log_message('WARNING', f"sqlite_connector: fetch_view_code: Could not isolate the SELECT of view {source_view_name} - using the full statement.")
            return clean_sql
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: fetch_view_code: Error fetching code of view {source_view_name}: {e}")
            self.disconnect()
            raise

    def convert_view_code(self, settings: dict):
        view_code = settings.get('view_code') or ''
        if not view_code.strip():
            return ''
        target_schema_name = settings['target_schema_name']
        target_view_name = settings['target_view_name']
        source_schema_name = settings.get('source_schema_name')
        view_type = settings.get('view_type', 'VIEW')

        converted_code = self._transpile(view_code, f'view {target_view_name}')

        try:
            self.connect()
            object_names = self._migrated_object_names(source_schema_name)
            self.disconnect()
        except Exception:
            object_names = set()
        converted_code = self._qualify_object_names(converted_code, target_schema_name, object_names)

        ddl = f'CREATE {view_type} "{target_schema_name}"."{target_view_name}" AS {converted_code}'
        if not ddl.rstrip().endswith(';'):
            ddl += ';'
        return ddl

    ## ---------------------------------------------------------------- default values

    def convert_default_value(self, settings) -> dict:
        extracted_default_value = settings.get('extracted_default_value')
        if extracted_default_value is None or str(extracted_default_value).strip() == '':
            return ''

        default_value = str(extracted_default_value).strip()
        if default_value.upper() == 'NULL':
            return ''

        # SQLite wraps non-trivial defaults in parentheses: DEFAULT (datetime('now'))
        if default_value.startswith('(') and default_value.endswith(')'):
            inner, end_index = self._find_paren_group(default_value)
            if inner is not None and end_index == len(default_value):
                default_value = inner.strip()

        upper_value = default_value.upper()
        if upper_value in ('CURRENT_TIMESTAMP', 'CURRENT_DATE', 'CURRENT_TIME'):
            return upper_value

        # A blob literal X'AABB' becomes a bytea literal
        blob_literal = re.fullmatch(r"(?i)X'([0-9a-fA-F]*)'", default_value)
        if blob_literal:
            return f"'\\x{blob_literal.group(1).lower()}'::bytea"

        # A plain literal (number or quoted string) is taken over unchanged
        if re.fullmatch(r"'(?:[^']|'')*'", default_value):
            return default_value
        if re.fullmatch(r'[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?', default_value):
            return default_value

        return self._transpile(default_value, 'default value')

    def fetch_default_values(self, settings) -> dict:
        # SQLite has no independently created named default values
        return {}

    ## ---------------------------------------------------------------- objects SQLite does not have

    def fetch_funcproc_names(self, schema: str):
        # SQLite has no stored functions or procedures
        return {}

    def fetch_funcproc_code(self, funcproc_id: int):
        return ''

    def convert_funcproc_code(self, settings):
        return ''

    def fetch_sequences(self, schema_name: str):
        # SQLite has no sequences - AUTOINCREMENT columns are migrated as identity columns
        return {}

    def get_sequence_details(self, sequence_owner, sequence_name):
        return {}

    def get_sequence_current_value(self, sequence_id: int):
        return None

    def migrate_sequences(self, target_connector, settings):
        return True

    def fetch_user_defined_types(self, schema: str):
        return {}

    def fetch_domains(self, schema: str):
        return {}

    def get_aliases(self, settings):
        return {}

    def get_top_fk_dependencies(self, settings):
        return {}

    ## ---------------------------------------------------------------- target-only methods

    def get_create_table_sql(self, settings):
        """ Relevant only for target database """
        return ""

    def get_create_index_sql(self, settings):
        """ Relevant only for target database """
        return ""

    def get_create_constraint_sql(self, settings):
        """ Relevant only for target database """
        return ""

    def get_create_domain_sql(self, settings):
        """ Relevant only for target database """
        return ""

    ## ---------------------------------------------------------------- data migration

    def get_table_next_identity(self, table_schema: str, table_name: str):
        """
        The next value an AUTOINCREMENT column would use. SQLite keeps it in the
        sqlite_sequence table; without AUTOINCREMENT it is derived from the data.
        """
        schema = self._resolve_schema(table_schema)
        try:
            cursor = self.connection.cursor()
            cursor.execute(f'SELECT count(*) FROM {self._quote_ident(schema)}.sqlite_master WHERE type = ? AND name = ?', ('table', 'sqlite_sequence'))
            if cursor.fetchone()[0]:
                cursor.execute(f'SELECT seq FROM {self._quote_ident(schema)}.sqlite_sequence WHERE name = ?', (table_name,))
                row = cursor.fetchone()
                if row and row[0] is not None:
                    cursor.close()
                    return int(row[0]) + 1

            primary_key_columns = self._primary_key_columns(table_schema, table_name)
            if len(primary_key_columns) == 1:
                cursor.execute(f'SELECT max({self._quote_ident(primary_key_columns[0])}) FROM {self._quote_ident(schema)}.{self._quote_ident(table_name)}')
                row = cursor.fetchone()
                cursor.close()
                if row and isinstance(row[0], int):
                    return row[0] + 1
                return None
            cursor.close()
            return None
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"sqlite_connector: get_table_next_identity: Error fetching next identity for {table_schema}.{table_name}: {e}")
            return None

    def _coerce_boolean(self, value):
        if value is None or isinstance(value, bool):
            return value
        if isinstance(value, (int, float, Decimal)):
            return value != 0
        if isinstance(value, (bytes, bytearray, memoryview)):
            value = self.text_decoder().decode(bytes(value), place='BOOLEAN value')
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in ('1', 't', 'true', 'y', 'yes', 'on'):
                return True
            if normalized in ('0', 'f', 'false', 'n', 'no', 'off', ''):
                return False
            return True
        return bool(value)

    def _coerce_datetime(self, value):
        """
        SQLite stores date and time values as ISO text, as a Unix timestamp (INTEGER) or
        as a Julian day number (REAL). Text is handed over unchanged - PostgreSQL parses
        it - while the numeric forms are converted here.
        """
        if value is None or isinstance(value, (str, datetime.date, datetime.datetime, datetime.time)):
            return value
        if isinstance(value, (bytes, bytearray, memoryview)):
            return self.text_decoder().decode(bytes(value), place='date or time value')
        if isinstance(value, int):
            try:
                return datetime.datetime.fromtimestamp(value, datetime.timezone.utc).replace(tzinfo=None)
            except (OverflowError, OSError, ValueError):
                return str(value)
        if isinstance(value, float):
            try:
                # Julian day 2440587.5 is the Unix epoch
                return datetime.datetime.fromtimestamp((value - 2440587.5) * 86400.0, datetime.timezone.utc).replace(tzinfo=None)
            except (OverflowError, OSError, ValueError):
                return str(value)
        return value

    def _transform_value(self, value, source_column, target_column):
        """
        Adapt one SQLite value to the PostgreSQL type of the target column. SQLite does
        not enforce the declared type of a column, so the value can be of any storage
        class regardless of what the column was declared as.
        """
        if value is None:
            return None

        target_type = str((target_column or {}).get('data_type', '')).upper()
        declared_type = str((source_column or {}).get('column_type', '')).upper()

        if isinstance(value, memoryview):
            value = value.tobytes()
        if isinstance(value, bytearray):
            value = bytes(value)

        if target_type in ('BOOLEAN', 'BOOL'):
            return self._coerce_boolean(value)

        if target_type == 'BYTEA':
            if not self.config_parser.should_migrate_lob_values():
                return None
            if isinstance(value, bytes):
                return value
            return str(value).encode('utf-8')

        if any(token in target_type for token in ('TIMESTAMP', 'DATE', 'TIME')):
            return self._coerce_datetime(value)

        if isinstance(value, bytes):
            # A BLOB value in a non-binary target column. Text which was merely stored as a
            # BLOB is decoded; real binary content is written as the SQLite blob literal
            # X'..' instead, because decoding it with replacement characters destroys it
            # without anybody noticing.
            try:
                return value.decode('utf-8')
            except UnicodeDecodeError:
                return f"X'{value.hex().upper()}'"

        if 'CHAR' in target_type or target_type in ('TEXT', 'JSONB', 'JSON', 'XML', 'UUID'):
            if not self.config_parser.should_migrate_lob_values() and self._is_lob_type(declared_type):
                return None
            if isinstance(value, str):
                return value
            return str(value)

        return value

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
        worker_id = settings.get('worker_id')
        try:
            worker_id = settings['worker_id']
            source_schema_name = settings['source_schema_name']
            source_table_name = settings['source_table_name']
            source_table_id = settings['source_table_id']
            source_columns = settings['source_columns']
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
                self.config_parser.print_log_message('INFO', f"sqlite_connector: migrate_table: Worker {worker_id}: Table {source_table_name} is empty - skipping data migration.")
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

            data_conflict_action = settings.get('data_conflict_action')
            if target_table_rows == 0 or data_conflict_action in ('merge_keep_target', 'merge_keep_source', 'replace'):
                migrator_tables.update_data_migration_started(protocol_id)

                self.config_parser.print_log_message('INFO', f"sqlite_connector: migrate_table: Worker {worker_id}: Source table {source_table_name}: {source_table_rows_limited} rows / Target table {target_table_name}: {target_table_rows} rows - starting data migration.")

                def is_generated_column(column):
                    return column.get('is_generated_virtual') == 'YES' or column.get('is_generated_stored') == 'YES'

                # Generated and hidden columns are not read from the source: PostgreSQL
                # computes generated columns itself and rejects values for them.
                migrated_source_columns = {
                    order_num: column for order_num, column in source_columns.items()
                    if not is_generated_column(column)
                    and column.get('is_hidden_column') != 'YES'
                    and not is_generated_column(target_columns.get(order_num, {}))
                }

                select_columns_list = []
                orderby_columns_list = []
                insert_columns_list = []
                for order_num, column in migrated_source_columns.items():
                    self.config_parser.print_log_message('DEBUG2', f"sqlite_connector: migrate_table: Worker {worker_id}: Table {source_schema_name}.{source_table_name}: Processing column {column['column_name']} ({order_num}) with declared type {column['column_type']}")
                    select_columns_list.append(self._quote_ident(column['column_name']))
                    insert_columns_list.append(f'''"{self.config_parser.convert_names_case(column['column_name'])}"''')
                    orderby_columns_list.append(self._quote_ident(column['column_name']))

                select_columns = ', '.join(select_columns_list)
                orderby_columns = ', '.join(orderby_columns_list)
                insert_columns = ', '.join(insert_columns_list)

                if resume_after_crash and not drop_unfinished_tables:
                    chunk_number = self.config_parser.get_total_chunks(target_table_rows, chunk_size)
                    self.config_parser.print_log_message('DEBUG', f"sqlite_connector: migrate_table: Worker {worker_id}: Resuming migration for table {source_schema_name}.{source_table_name} from chunk {chunk_number} with data chunk size {chunk_size}.")
                    chunk_offset = target_table_rows
                else:
                    chunk_offset = (chunk_number - 1) * chunk_size

                chunk_start_row_number = chunk_offset + 1
                chunk_end_row_number = chunk_offset + chunk_size

                self.config_parser.print_log_message('DEBUG', f"sqlite_connector: migrate_table: Worker {worker_id}: Migrating table {source_schema_name}.{source_table_name}: chunk {chunk_number}, data chunk size {chunk_size}, batch size {batch_size}, chunk offset {chunk_offset}, chunk end row number {chunk_end_row_number}, source table rows {source_table_rows_limited}")

                query = f'''SELECT {select_columns} FROM {self._qualified_name(source_schema_name, source_table_name)} '''
                if migration_limitation:
                    query += f" WHERE {migration_limitation}"

                primary_key_columns = migrator_tables.select_primary_key({'source_schema_name': source_schema_name, 'source_table_name': source_table_name})
                self.config_parser.print_log_message('DEBUG2', f"sqlite_connector: migrate_table: Worker {worker_id}: Primary key columns for {source_schema_name}.{source_table_name}: {primary_key_columns}")
                if primary_key_columns:
                    orderby_columns = ', '.join(
                        self._quote_ident(self._unquote_ident(column)) for column in primary_key_columns.split(',') if column.strip())
                order_by_clause = f""" ORDER BY {orderby_columns}"""
                query += order_by_clause + f" LIMIT {chunk_size} OFFSET {chunk_offset}"

                self.config_parser.print_log_message('DEBUG', f"sqlite_connector: migrate_table: Worker {worker_id}: Fetching data with cursor using query: {query}")

                part_name = 'execute query'
                cursor = self.connection.cursor()
                cursor.arraysize = batch_size

                batch_start_time = time.time()
                reading_start_time = batch_start_time
                processing_start_time = batch_start_time
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
                    self.config_parser.print_log_message('DEBUG', f"sqlite_connector: migrate_table: Worker {worker_id}: Fetched {len(records)} rows (batch {batch_number}) from source table {source_table_name}.")

                    part_name = 'transform batch'
                    transforming_start_time = time.time()
                    transformed_records = []
                    for record in records:
                        row = {}
                        for position, (order_num, column) in enumerate(migrated_source_columns.items()):
                            column_name = column['column_name']
                            row[column_name] = self._transform_value(
                                record[position], column, target_columns.get(order_num, {}))
                        transformed_records.append(row)
                    transforming_end_time = time.time()
                    transforming_duration = transforming_end_time - transforming_start_time

                    part_name = 'insert batch'
                    self.config_parser.print_log_message('DEBUG', f"sqlite_connector: migrate_table: Worker {worker_id}: Starting insert of {len(transformed_records)} rows from source table {source_table_name}")
                    inserting_start_time = time.time()
                    inserted_rows = migrate_target_connection.insert_batch({
                        'target_schema_name': target_schema_name,
                        'target_table_name': target_table_name,
                        'target_columns': target_columns,
                        'data': transformed_records,
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

                    batch_start_str = datetime.datetime.fromtimestamp(batch_start_time).strftime('%Y-%m-%d %H:%M:%S.%f')
                    batch_end_str = datetime.datetime.fromtimestamp(batch_end_time).strftime('%Y-%m-%d %H:%M:%S.%f')
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
                self.config_parser.print_log_message('INFO', f"sqlite_connector: migrate_table: Worker {worker_id}: Target table {target_schema_name}.{target_table_name} has {target_table_rows} rows")

                shortest_batch_seconds = min(batch_durations) if batch_durations else 0
                longest_batch_seconds = max(batch_durations) if batch_durations else 0
                average_batch_seconds = sum(batch_durations) / len(batch_durations) if batch_durations else 0
                self.config_parser.print_log_message('INFO', f"sqlite_connector: migrate_table: Worker {worker_id}: Migrated {total_inserted_rows} rows from {source_table_name} to {target_schema_name}.{target_table_name} in {batch_number} batches: "
                                                             f"Shortest batch: {shortest_batch_seconds:.2f} seconds, "
                                                             f"Longest batch: {longest_batch_seconds:.2f} seconds, "
                                                             f"Average batch: {average_batch_seconds:.2f} seconds")

                cursor.close()

            else:
                self.config_parser.print_log_message('INFO', f"sqlite_connector: migrate_table: Worker {worker_id}: Target table {target_table_name} has {target_table_rows} rows and data_conflict_action is '{data_conflict_action}'. Skipping data migration.")

            migration_stats = {
                'rows_migrated': total_inserted_rows,
                'chunk_number': chunk_number,
                'total_chunks': total_chunks,
                'source_table_rows_all': source_table_rows_all,

                'source_table_rows_limited': source_table_rows_limited,
                'target_table_rows': target_table_rows,
                'finished': False,
            }

            self.config_parser.print_log_message('DEBUG', f"sqlite_connector: migrate_table: Worker {worker_id}: Migration stats: {migration_stats}")
            if source_table_rows_limited <= target_table_rows or chunk_number >= total_chunks:
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
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: migrate_table: Worker {worker_id}: Error during {part_name} -> {e}")
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: migrate_table: Worker {worker_id}: Full stack trace: {traceback.format_exc()}")
            raise e

    ## ---------------------------------------------------------------- generic SQL execution

    def execute_query(self, query: str, params=None):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or [])
            cursor.close()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: execute_query: Error executing query: {e}")
            raise

    def execute_sql_script(self, script_path: str):
        try:
            with open(script_path, 'r') as script_file:
                script = script_file.read()
            self.connection.executescript(script)
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: execute_sql_script: Error executing SQL script {script_path}: {e}")
            raise

    def begin_transaction(self):
        self.connection.isolation_level = ''
        self.connection.execute('BEGIN')

    def commit_transaction(self):
        self.connection.commit()

    def rollback_transaction(self):
        self.connection.rollback()

    def fetch_all_rows(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def testing_select(self):
        return "SELECT 1"

    ## ---------------------------------------------------------------- counts, sizes, versions

    def get_rows_count(self, table_schema: str, table_name: str, migration_limitation: str = None):
        query = f'SELECT count(*) FROM {self._qualified_name(table_schema, table_name)}'
        if migration_limitation:
            query += f" WHERE {migration_limitation}"
        self.config_parser.print_log_message('DEBUG3', f"sqlite_connector: get_rows_count: query: {query}")
        cursor = self.connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_table_size(self, table_schema: str, table_name: str):
        """
        SQLite has no per table size in its catalog. The dbstat virtual table provides it,
        but only when SQLite was compiled with SQLITE_ENABLE_DBSTAT_VTAB - otherwise the
        size is reported as unknown (0).
        """
        try:
            schema = self._resolve_schema(table_schema)
            cursor = self.connection.cursor()
            cursor.execute(f'SELECT coalesce(sum(pgsize), 0) FROM {self._quote_ident(schema)}.dbstat WHERE name = ?', (table_name,))
            size = cursor.fetchone()[0]
            cursor.close()
            return size or 0
        except Exception as e:
            self.config_parser.print_log_message('DEBUG', f"sqlite_connector: get_table_size: Size of {table_schema}.{table_name} is not available (dbstat missing): {e}")
            return 0

    def get_indexes_count(self, schema_name: str, table_name: str):
        try:
            return len(self._pragma_rows(schema_name, 'index_list', table_name))
        except Exception:
            return None

    def get_constraints_count(self, schema_name: str, table_name: str):
        try:
            foreign_keys = {row[0] for row in self._pragma_rows(schema_name, 'foreign_key_list', table_name)}
            parsed_ddl = self._parse_table_ddl(schema_name, table_name)
            return len(foreign_keys) + len(parsed_ddl['checks'])
        except Exception:
            return None

    def get_generated_columns_count(self, table_schema: str) -> int:
        count = 0
        try:
            self.connect()
            for row in self._fetch_master(table_schema, 'table'):
                table_name = str(row[2])
                if table_name.startswith('sqlite_'):
                    continue
                parsed_ddl = self._parse_table_ddl(table_schema, table_name)
                count += sum(1 for column in parsed_ddl['columns'].values() if column.get('generation_expression'))
            self.disconnect()
            return count
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"sqlite_connector: get_generated_columns_count: Could not count generated columns: {e}")
            self.disconnect()
            return 0

    def get_database_version(self):
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute("SELECT sqlite_version()")
            version = cursor.fetchone()[0]
            cursor.close()
            self.disconnect()
            return f"SQLite {version}"
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: get_database_version: Error fetching database version: {e}")
            self.disconnect()
            raise

    def get_database_size(self):
        """ Size of the database file including its WAL / journal side files. """
        try:
            database_path = self._database_path()
            size = 0
            for suffix in ('', '-wal', '-shm', '-journal'):
                candidate = database_path + suffix
                if os.path.isfile(candidate):
                    size += os.path.getsize(candidate)
            return size
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: get_database_size: Error fetching database size: {e}")
            return 0

    def get_top_n_tables(self, settings):
        top_tables = {
            'by_rows': {},
            'by_size': {},
            'by_columns': {},
            'by_indexes': {},
            'by_constraints': {},
        }
        source_schema_name = settings.get('source_schema_name', 'main')
        requested = {
            'by_rows': self.config_parser.get_top_n_tables_by_rows(),
            'by_size': self.config_parser.get_top_n_tables_by_size(),
            'by_columns': self.config_parser.get_top_n_tables_by_columns(),
            'by_indexes': self.config_parser.get_top_n_tables_by_indexes(),
            'by_constraints': self.config_parser.get_top_n_tables_by_constraints(),
        }
        if not any(top_n > 0 for top_n in requested.values()):
            self.config_parser.print_log_message('DEBUG', "sqlite_connector: get_top_n_tables: No top N analysis is configured - skipping.")
            return top_tables

        try:
            self.connect()
            schema = self._resolve_schema(source_schema_name)
            virtual_tables = self._shadow_table_prefixes(source_schema_name)

            table_names = []
            for row in self._fetch_master(source_schema_name, 'table'):
                table_name = str(row[2])
                if table_name.startswith('sqlite_') or table_name in virtual_tables:
                    continue
                if any(table_name.startswith(prefix + '_') for prefix in virtual_tables):
                    continue
                table_names.append(table_name)

            # How often each table is the parent of a foreign key of another table
            referenced_counts = {name: 0 for name in table_names}
            for table_name in table_names:
                try:
                    for foreign_key in self._pragma_rows(source_schema_name, 'foreign_key_list', table_name):
                        parent = str(foreign_key[2])
                        if parent in referenced_counts:
                            referenced_counts[parent] += 1
                except Exception:
                    pass

            statistics = []
            for table_name in table_names:
                try:
                    column_rows = self._pragma_rows(source_schema_name, 'table_info', table_name)
                    date_time_columns = [
                        str(column[1]) for column in column_rows
                        if any(token in str(column[2] or '').upper() for token in ('DATE', 'TIME'))
                    ]
                    primary_key_columns = [str(column[1]) for column in sorted((c for c in column_rows if c[5]), key=lambda c: c[5])]
                    foreign_key_count = len({row[0] for row in self._pragma_rows(source_schema_name, 'foreign_key_list', table_name)})
                    parsed_ddl = self._parse_table_ddl(source_schema_name, table_name)
                    # SQLite keeps no row estimates in its catalog - the rows have to be counted
                    row_count = self.get_rows_count(source_schema_name, table_name)
                    table_size = self.get_table_size(source_schema_name, table_name)
                    statistics.append({
                        'owner': schema,
                        'table_name': table_name,
                        'row_count': row_count,
                        'row_size': int(table_size / row_count) if row_count and table_size else 0,
                        'table_size': table_size,
                        'fk_count': foreign_key_count,
                        'date_time_columns': ', '.join(date_time_columns),
                        'pk_columns': ', '.join(primary_key_columns),
                        'has_rowid': 'NO' if parsed_ddl['without_rowid'] else 'YES',
                        'ref_fk_count': referenced_counts.get(table_name, 0),
                        'column_count': len(column_rows),
                        'index_count': len(self._pragma_rows(source_schema_name, 'index_list', table_name)),
                        'constraint_count': foreign_key_count + len(parsed_ddl['checks']),
                        'constraint_type': 'FOREIGN KEY / CHECK',
                    })
                except Exception as e:
                    self.config_parser.print_log_message('WARNING', f"sqlite_connector: get_top_n_tables: Could not collect statistics for table {table_name}: {e}")

            def fill(metric, sort_key):
                top_n = requested[metric]
                if top_n <= 0:
                    self.config_parser.print_log_message('DEBUG', f"sqlite_connector: get_top_n_tables: Top N tables {metric} is not configured or set to 0, skipping this part.")
                    return
                order_num = 1
                for entry in sorted(statistics, key=lambda item: item[sort_key] or 0, reverse=True)[:top_n]:
                    top_tables[metric][order_num] = dict(entry)
                    order_num += 1

            fill('by_rows', 'row_count')
            fill('by_size', 'table_size')
            fill('by_columns', 'column_count')
            fill('by_indexes', 'index_count')
            fill('by_constraints', 'constraint_count')

            self.disconnect()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: get_top_n_tables: Error collecting top N tables: {e}")
            self.disconnect()

        return top_tables

    def target_table_exists(self, target_schema_name, target_table_name):
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                f'SELECT count(*) FROM {self._quote_ident(self._resolve_schema(target_schema_name))}.sqlite_master WHERE type = ? AND lower(name) = lower(?)',
                ('table', target_table_name))
            exists = cursor.fetchone()[0] > 0
            cursor.close()
            return exists
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: target_table_exists: Error checking if table {target_table_name} exists: {e}")
            raise

    ## ---------------------------------------------------------------- validation support

    def _checksum_columns(self, columns):
        """
        Column list for a checksum query. LOB-like columns are left out - the target
        connectors skip them too, so including them here would make every table mismatch.
        """
        selected = []
        for column in columns:
            data_type = str(column.get('data_type', '')).lower()
            if any(token in data_type for token in ('lob', 'blob', 'bytea', 'binary', 'image', 'xml', 'json', 'text')):
                continue
            selected.append(self._quote_ident(column['column_name']))
        return selected

    def get_table_checksum(self, schema_name: str, table_name: str, columns: list):
        if not columns:
            return None
        selected = self._checksum_columns(columns)
        if not selected:
            return None
        query = f'SELECT {", ".join(selected)} FROM {self._qualified_name(schema_name, table_name)}'
        return self._compute_python_table_checksum(query)

    def get_random_pks(self, schema_name: str, table_name: str, pk_columns: list, sample_size: int):
        if not pk_columns:
            return []
        selected = ', '.join(self._quote_ident(column) for column in pk_columns)
        query = f'SELECT {selected} FROM {self._qualified_name(schema_name, table_name)} ORDER BY random() LIMIT {int(sample_size)}'
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            primary_keys = [dict(zip(pk_columns, row)) for row in cursor.fetchall()]
            cursor.close()
            self.disconnect()
            return primary_keys
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: get_random_pks: Error executing query: {query}: {e}")
            self.disconnect()
            return []

    def _pk_where_clause(self, pk_columns, pk_values_list):
        quoted_pk_columns = ', '.join(self._quote_ident(column) for column in pk_columns)
        value_groups = []
        for pk_values in pk_values_list:
            values = []
            for column in pk_columns:
                value = pk_values[column]
                if value is None:
                    values.append('NULL')
                elif isinstance(value, str):
                    values.append("'" + value.replace("'", "''") + "'")
                else:
                    values.append(str(value))
            value_groups.append(f"({', '.join(values)})")
        if len(pk_columns) == 1:
            inner = ', '.join(group.strip('()') for group in value_groups)
            return quoted_pk_columns, f"{quoted_pk_columns} IN ({inner})"
        return quoted_pk_columns, f"({quoted_pk_columns}) IN ({', '.join(value_groups)})"

    def get_row_checksums(self, schema_name: str, table_name: str, pk_columns: list, pk_values_list: list, columns: list):
        if not columns or not pk_columns or not pk_values_list:
            return {}
        selected = self._checksum_columns(columns)
        if not selected:
            return {}
        quoted_pk_columns, where_clause = self._pk_where_clause(pk_columns, pk_values_list)
        query = (f'SELECT {quoted_pk_columns}, {", ".join(selected)} '
                 f'FROM {self._qualified_name(schema_name, table_name)} WHERE {where_clause}')
        return self._compute_python_row_checksums(query, len(pk_columns))

    def get_lob_sizes(self, schema_name: str, table_name: str, pk_columns: list, pk_values_list: list, lob_columns: list):
        if not lob_columns or not pk_columns or not pk_values_list:
            return {}
        # SQLite length() returns the number of bytes for a BLOB and the number of
        # characters for TEXT - the same semantics as octet_length / length in PostgreSQL.
        size_selects = ', '.join(f'length({self._quote_ident(column["column_name"])})' for column in lob_columns)
        quoted_pk_columns, where_clause = self._pk_where_clause(pk_columns, pk_values_list)
        query = (f'SELECT {quoted_pk_columns}, {size_selects} '
                 f'FROM {self._qualified_name(schema_name, table_name)} WHERE {where_clause}')
        sizes = {}
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                pk_tuple = tuple(row[:len(pk_columns)])
                pk_key = pk_tuple[0] if len(pk_tuple) == 1 else pk_tuple
                sizes[pk_key] = row[len(pk_columns):]
            cursor.close()
            return sizes
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"sqlite_connector: get_lob_sizes: Error executing query: {query}: {e}")
            return {}

    def get_column_statistics(self, schema_name: str, table_name: str, column_name: str, data_type: str, force_round_0: bool = False):
        """
        SQLite quotes identifiers the same way PostgreSQL does, so the generic
        implementation works - only the schema qualification has to be adjusted, because
        the migrator passes the configured schema name and not the SQLite database name.
        """
        return super().get_column_statistics(self._resolve_schema(schema_name), table_name, column_name, data_type, force_round_0)


if __name__ == "__main__":
    print("This script is not meant to be run directly")
