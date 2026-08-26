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

import time
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from credativ_pg_migrator.database_connector import DatabaseConnector, first_line
from credativ_pg_migrator.migrator_logging import MigratorLogger
from credativ_pg_migrator.constants import MigratorConstants
from credativ_pg_migrator import collations
import traceback
import re
import json
import datetime
from decimal import Decimal
import sqlglot


## --------------------------------------------------------------------------------------
## The conversion of a bare statement, PostgreSQL to PostgreSQL. §7 of
## development/archive/APPLICATION_QUERIES_CONVERSION_STRATEGY.md, P3-5 of
## development/OPEN_ISSUES.md.
##
## There is no dialect to convert here - the statement is already PostgreSQL, and the cost
## table of §7.1 calls this connector trivial for that reason. What it is not is empty: a
## migration moves the rows into a new physical order in a new database, and the constructs
## below are the ones which read that physical order, or the state of the session, or the
## catalogue - each of them valid PostgreSQL on the target, each of them answering something
## else there, and none of them visible to any level of the target test.
## --------------------------------------------------------------------------------------

## The system columns. PostgreSQL refuses to create a user column of any of these names, so
## a name found here is always the system column and never something the application declared.
POSTGRESQL_SYSTEM_COLUMNS = re.compile(
    r'(?i)(?<![\w])(?:ctid|xmin|xmax|cmin|cmax|tableoid)(?![\w(])')

## The sequence functions which answer what the SESSION did. nextval() is refused before this
## by gate 3 of the query conversion - it writes.
POSTGRESQL_SESSION_SEQUENCE = re.compile(r'(?i)(?<![\w.])(?:currval|lastval)\s*\(')

## A name resolved out of a string literal at run time, through the search_path of whoever
## runs the statement.
POSTGRESQL_REG_CAST = re.compile(
    r'(?i)::\s*(regclass|regconfig|regdictionary|regproc|regprocedure|regoper|regoperator'
    r'|regtype|regnamespace|regrole)\b')

## The catalogue of the server, and the functions which report on it.
POSTGRESQL_CATALOGUE = re.compile(
    r'(?i)(?<![\w.])(?:pg_catalog\.|information_schema\.|pg_class\b|pg_attribute\b'
    r'|pg_namespace\b|pg_index\b|pg_constraint\b|pg_proc\b|pg_type\b|pg_stat_[a-z_]+\b'
    r'|pg_settings\b|pg_roles\b|pg_user\b|pg_tables\b|pg_views\b|pg_indexes\b)')

## What answers a property of the connection rather than of the data.
POSTGRESQL_SESSION_FUNCTIONS = re.compile(
    r'(?i)(?<![\w.])(?:current_database|current_schema|current_catalog|current_user'
    r'|session_user|current_role|inet_client_addr|inet_server_addr|pg_backend_pid'
    r'|version)\s*(?:\(|\b)')

## An explicit collation, which has to exist on the target cluster and mean the same there.
POSTGRESQL_COLLATE = re.compile(r'(?i)(?<![\w.])COLLATE\s+')

class PostgreSQLConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger
        # Names of collations usable in this database - filled on first use, see
        # get_existing_collation_names()
        self.existing_collation_names = None
        ## must be there before prepare_session_settings() runs - it opens a connection of its
        ## own, and connect() applies whatever is prepared already
        self.session_settings = ""
        self.session_settings = self.prepare_session_settings()

    def connect(self):
        connection_string = self.config_parser.get_connect_string(self.source_or_target)
        self.connection = psycopg2.connect(connection_string, application_name=MigratorConstants.get_application_name())
        self.connection.autocommit = True
        self._register_type_casters()
        self.apply_session_settings()

    def apply_session_settings(self):
        """
        Apply the session settings of the configuration to the connection just opened.

        This is done here, and not by the callers, because every connection has to run with
        them - the short lived ones which create a single object as much as the workers which
        copy the data. The role is the owner of everything a connection creates, so a
        connection which is opened without it leaves an object behind owned by the login role
        of the connection instead. Before this, the settings were executed in the handful of
        places which had been patched to do it, and user defined types, domains, sequences,
        triggers, comments, default values, the pre- and post-migration scripts and the whole
        mapping workflow created their objects without them.
        """
        if not self.session_settings:
            return
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(self.session_settings)
            self.config_parser.print_log_message('DEBUG3', f"postgresql_connector: apply_session_settings: Applied on the {self.source_or_target} connection: {self.session_settings}")
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: apply_session_settings: Error applying the session settings '{self.session_settings}' on the {self.source_or_target} connection: {e}")
            raise

    def _register_type_casters(self):
        try:
            def cast_str(val, cur):
                return val
            oids = (1082, 1114, 1184, 1083, 1266, 1186)
            caster = psycopg2.extensions.new_type(oids, "DATE_STR_CASTER", cast_str)
            psycopg2.extensions.register_type(caster, self.connection)
        except Exception:
            pass

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
            return {}
        else:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: get_sql_functions_mapping: Unsupported target database type: {target_db_type}")

    def check_and_create_extension(self, extension_name: str) -> tuple:
        """
        Check if a PostgreSQL extension exists, attempt CREATE EXTENSION IF NOT EXISTS if missing.
        Returns (success: bool, message: str).
        """
        ext_clean = extension_name.strip().lower()
        if not ext_clean:
            return True, ""

        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1 FROM pg_extension WHERE extname = %s", (ext_clean,))
            row = cursor.fetchone()
            if row:
                cursor.close()
                self.disconnect()
                return True, f"Extension '{ext_clean}' is present on target PostgreSQL database."

            self.config_parser.print_log_message('INFO', f"postgresql_connector: check_and_create_extension: Extension '{ext_clean}' missing on target database - attempting CREATE EXTENSION IF NOT EXISTS \"{ext_clean}\".")
            cursor.execute(f'CREATE EXTENSION IF NOT EXISTS "{ext_clean}"')
            self.connection.commit()
            cursor.close()
            self.disconnect()
            self.config_parser.print_log_message('INFO', f"postgresql_connector: check_and_create_extension: Successfully created extension '{ext_clean}' on target database.")
            return True, f"Extension '{ext_clean}' created successfully on target database."
        except Exception as e:
            if hasattr(self, 'connection') and self.connection:
                try:
                    self.connection.rollback()
                except Exception:
                    pass
                self.disconnect()
            err_msg = str(e).strip()
            self.config_parser.print_log_message('WARNING', f"postgresql_connector: check_and_create_extension: Failed to create extension '{ext_clean}': {err_msg}")
            return False, f"Required PostgreSQL extension '{ext_clean}' is missing and CREATE EXTENSION failed: {err_msg}"

    def fetch_installed_extensions(self):
        """
        Extensions installed in this database. Returns dict: name -> {'version', 'schema'}.
        """
        extensions = {}
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT e.extname, e.extversion, n.nspname
                FROM pg_catalog.pg_extension e
                JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
                ORDER BY e.extname
            """)
            for name, version, schema in cursor.fetchall():
                extensions[name] = {'version': version, 'schema': schema}
            cursor.close()
            self.disconnect()
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"postgresql_connector: fetch_installed_extensions: Cannot read installed extensions: {e}")
        return extensions

    def fetch_available_extensions(self):
        """
        Extensions which can be installed in this database - the control files present on the
        server. Returns dict: name -> default version.
        """
        extensions = {}
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute("SELECT name, default_version FROM pg_catalog.pg_available_extensions ORDER BY name")
            for name, default_version in cursor.fetchall():
                extensions[name] = default_version
            cursor.close()
            self.disconnect()
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"postgresql_connector: fetch_available_extensions: Cannot read available extensions: {e}")
        return extensions

    def fetch_extension_dependencies(self, settings):
        """
        Which extensions the objects selected for migration actually depend on.

        The dependencies are not guessed from the SQL text - they are read from pg_depend,
        which records every type, function, operator, operator class and text search
        dictionary an object refers to. An object is attributed to an extension when it is a
        member of one (a pg_depend entry with deptype = 'e').

        settings:
            source_schema_name - schema being migrated
            table_names        - list of table names selected for migration
            migrate_indexes / migrate_constraints / migrate_triggers /
            migrate_views / migrate_funcprocs - booleans from the configuration

        Returns dict: extension name -> sorted list of objects requiring it.
        """
        schema = settings['source_schema_name']
        table_names = list(settings.get('table_names') or [])
        query = ''
        dependencies = {}
        try:
            self.connect()
            cursor = self.connection.cursor()
            query = """
                WITH tabs AS (
                    SELECT c.oid
                    FROM pg_catalog.pg_class c
                    WHERE c.relnamespace = to_regnamespace(%(schema)s)
                      AND c.relkind IN ('r', 'p')
                      AND c.relname = ANY(%(tables)s)
                ),
                objects AS (
                    SELECT 'pg_class'::regclass AS classid, t.oid AS objid,
                           'table ' || t.oid::regclass::text AS required_by
                    FROM tabs t
                    UNION ALL
                    SELECT 'pg_attrdef'::regclass, ad.oid,
                           'column ' || ad.adrelid::regclass::text || '.' || a.attname
                             || CASE WHEN a.attgenerated <> '' THEN ' (generated)' ELSE ' (default)' END
                    FROM pg_catalog.pg_attrdef ad
                    JOIN tabs t ON t.oid = ad.adrelid
                    JOIN pg_catalog.pg_attribute a ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
                    UNION ALL
                    SELECT 'pg_class'::regclass, i.indexrelid,
                           'index ' || i.indexrelid::regclass::text
                    FROM pg_catalog.pg_index i JOIN tabs t ON t.oid = i.indrelid
                    WHERE %(migrate_indexes)s
                    UNION ALL
                    SELECT 'pg_constraint'::regclass, con.oid,
                           'constraint ' || con.conname || ' on ' || con.conrelid::regclass::text
                    FROM pg_catalog.pg_constraint con JOIN tabs t ON t.oid = con.conrelid
                    WHERE %(migrate_constraints)s
                    UNION ALL
                    SELECT 'pg_trigger'::regclass, tg.oid,
                           'trigger ' || tg.tgname || ' on ' || tg.tgrelid::regclass::text
                    FROM pg_catalog.pg_trigger tg JOIN tabs t ON t.oid = tg.tgrelid
                    WHERE NOT tg.tgisinternal AND %(migrate_triggers)s
                    UNION ALL
                    SELECT 'pg_rewrite'::regclass, r.oid,
                           'view ' || r.ev_class::regclass::text
                    FROM pg_catalog.pg_rewrite r
                    JOIN pg_catalog.pg_class v ON v.oid = r.ev_class
                    WHERE v.relnamespace = to_regnamespace(%(schema)s)
                      AND v.relkind IN ('v', 'm') AND %(migrate_views)s
                    UNION ALL
                    SELECT 'pg_class'::regclass, v.oid,
                           'view ' || v.oid::regclass::text
                    FROM pg_catalog.pg_class v
                    WHERE v.relnamespace = to_regnamespace(%(schema)s)
                      AND v.relkind IN ('v', 'm') AND %(migrate_views)s
                    UNION ALL
                    SELECT 'pg_proc'::regclass, p.oid,
                           'function ' || p.oid::regprocedure::text
                    FROM pg_catalog.pg_proc p
                    WHERE p.pronamespace = to_regnamespace(%(schema)s) AND %(migrate_funcprocs)s
                    UNION ALL
                    SELECT 'pg_ts_config'::regclass, cfg.oid,
                           'text search configuration ' || cfg.cfgname
                    FROM pg_catalog.pg_ts_config cfg
                    WHERE cfg.cfgnamespace = to_regnamespace(%(schema)s)
                    UNION ALL
                    SELECT 'pg_ts_dict'::regclass, dict.oid,
                           'text search dictionary ' || dict.dictname
                    FROM pg_catalog.pg_ts_dict dict
                    WHERE dict.dictnamespace = to_regnamespace(%(schema)s)
                )
                SELECT e.extname, o.required_by
                FROM objects o
                JOIN pg_catalog.pg_depend d ON d.classid = o.classid AND d.objid = o.objid
                JOIN pg_catalog.pg_depend ed ON ed.classid = d.refclassid AND ed.objid = d.refobjid
                    AND ed.refclassid = 'pg_extension'::regclass AND ed.deptype = 'e'
                JOIN pg_catalog.pg_extension e ON e.oid = ed.refobjid
                WHERE e.extname <> 'plpgsql'
                GROUP BY 1, 2
                ORDER BY 1, 2
            """
            cursor.execute(query, {
                'schema': schema,
                'tables': table_names,
                'migrate_indexes': bool(settings.get('migrate_indexes', True)),
                'migrate_constraints': bool(settings.get('migrate_constraints', True)),
                'migrate_triggers': bool(settings.get('migrate_triggers', True)),
                'migrate_views': bool(settings.get('migrate_views', True)),
                'migrate_funcprocs': bool(settings.get('migrate_funcprocs', True)),
            })
            for extension_name, required_by in cursor.fetchall():
                dependencies.setdefault(extension_name, []).append(required_by)
            cursor.close()
            self.disconnect()
            return dependencies
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_extension_dependencies: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_table_names(self, schema: str = 'public'):
        query = f"""
            SELECT
                c.oid,
                c.relname,
                obj_description(c.oid, 'pg_class') as table_comment,
                c.relkind,
                c.relispartition,
                pg_get_partkeydef(c.oid) as partition_key_def,
                pg_get_expr(c.relpartbound, c.oid) as partition_bound,
                (SELECT relname FROM pg_class WHERE oid = i.inhparent) as parent_table
            FROM pg_class c
            LEFT JOIN pg_inherits i ON c.oid = i.inhrelid
            WHERE c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{schema}')
            AND c.relkind in ('r', 'p')
            ORDER BY c.relname
        """
        self.config_parser.print_log_message('DEBUG3', f"postgresql_connector: fetch_table_names: Reading table names for {schema}")
        self.config_parser.print_log_message('DEBUG3', f"postgresql_connector: fetch_table_names: Query: {query}")
        try:
            tables = {}
            order_num = 1
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                tables[order_num] = {
                    'id': row[0],
                    'schema_name': schema,
                    'table_name': row[1],
                    'comment': row[2],
                    'relkind': row[3],
                    'relispartition': row[4],
                    'partition_key_def': row[5],
                    'partition_bound': row[6],
                    'parent_table': row[7]
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return tables
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_table_names: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_table_description(self, settings) -> dict:
        table_schema = settings['table_schema']
        table_name = settings['table_name']
        output = []
        output.append(f'Table "{table_schema}"."{table_name}"')
        self.config_parser.print_log_message('DEBUG3', f"postgresql_connector: get_table_description: PostgreSQL connector: Getting table description for {table_schema}.{table_name}")

        try:
            self.connect()
            cursor = self.connection.cursor()

            # 1. Attributes (Columns)
            # Fetch: Column, Type, Nullable, Default
            query_columns = f"""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = '{table_schema}' AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """
            cursor.execute(query_columns)
            columns = cursor.fetchall()

            if columns:
                headers = ['Column', 'Type', 'Nullable', 'Default']
                rows = []
                for col in columns:
                    name = str(col[0]) if col[0] is not None else ''
                    dtype = str(col[1]) if col[1] is not None else ''
                    nullable = str(col[2]) if col[2] is not None else ''
                    default = str(col[3]) if col[3] is not None else ''
                    rows.append([name, dtype, nullable, default])

                # Calculate column widths
                widths = [len(h) for h in headers]
                for row in rows:
                    for i, val in enumerate(row):
                        if len(val) > widths[i]:
                            widths[i] = len(val)

                # Format Table
                header_line = " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
                output.append(header_line)
                divider_line = "-+-".join("-" * widths[i] for i in range(len(widths)))
                output.append(divider_line)

                for row in rows:
                    line = " | ".join(row[i].ljust(widths[i]) for i in range(len(row)))
                    output.append(line)

            output.append("")

            # 2. Indexes
            # Use pg_indexes as information_schema standard does not fully cover indexes in PG
            query_indexes = f"""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = '{table_schema}' AND tablename = '{table_name}'
            """
            cursor.execute(query_indexes)
            indexes = cursor.fetchall()
            if indexes:
                output.append("Indexes:")
                for idx in indexes:
                    output.append(f"    {idx[1]}")

            # 3. Constraints
            # Use pg_constraint for robust definition
            query_constraints = f"""
                SELECT conname, pg_get_constraintdef(oid), contype
                FROM pg_constraint
                WHERE conrelid = '"{table_schema}"."{table_name}"'::regclass
            """
            cursor.execute(query_constraints)
            pg_cons = cursor.fetchall()

            check_constraints = []
            fk_constraints = []

            for con in pg_cons:
                name = con[0]
                definition = con[1]
                contype = con[2] # c=check, f=foreign key, p=primary key, u=unique

                # Primary keys and Unique constraints are typically listed in Indexes section (as matching indexes)
                # So we focus on Checks and Foreign Keys here similar to psql output
                if contype == 'c':
                    check_constraints.append(f"    \"{name}\" CHECK {definition}")
                elif contype == 'f':
                    fk_constraints.append(f"    \"{name}\" {definition}")

            if check_constraints:
                output.append("Check constraints:")
                output.extend(check_constraints)

            if fk_constraints:
                output.append("Foreign-key constraints:")
                output.extend(fk_constraints)

            # 4. Triggers (Optional but good for \d+)
            # Use simple query from information_schema
            query_triggers = f"""
                SELECT trigger_name, action_timing, event_manipulation
                FROM information_schema.triggers
                WHERE event_object_schema = '{table_schema}' AND event_object_table = '{table_name}'
            """
            cursor.execute(query_triggers)
            triggers = cursor.fetchall()
            if triggers:
                output.append("Triggers:")
                for trig in triggers:
                    output.append(f"    {trig[0]} {trig[1]} {trig[2]}")

            cursor.close()
            self.disconnect()

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: get_table_description: Error getting table description for {table_schema}.{table_name}: {e}")
            return {'table_description': f"Error: {str(e)}"}

        self.config_parser.print_log_message('DEBUG3', f"postgresql_connector: get_table_description: Table description for {table_schema}.{table_name}: {output}")
        return {'table_description': "\\n".join(output)}

    def fetch_table_columns(self, settings) -> dict:
        table_schema = settings['table_schema']
        table_name = settings['table_name']
        result = {}
        try:
            query = f"""
                    SELECT
                        c.ordinal_position,
                        c.column_name,
                        c.data_type,
                        c.character_maximum_length,
                        c.numeric_precision,
                        c.numeric_scale,
                        c.is_identity,
                        c.is_nullable,
                        c.column_default,
                        u.udt_schema,
                        u.udt_name,
                        col_description((c.table_schema||'.'||c.table_name)::regclass::oid, c.ordinal_position) as column_comment,
                        is_generated,
                        pg_catalog.format_type(a.atttypid, a.atttypmod) as format_type,
                        c.collation_schema,
                        c.collation_name,
                        a.attgenerated,
                        CASE WHEN a.attgenerated <> ''
                             THEN pg_catalog.pg_get_expr(ad.adbin, ad.adrelid)
                        END as generation_expression
                    FROM information_schema.columns c
                    JOIN pg_namespace n ON c.table_schema = n.nspname
                    JOIN pg_class cl ON cl.relname = c.table_name AND cl.relnamespace = n.oid
                    JOIN pg_attribute a ON a.attrelid = cl.oid AND a.attname = c.column_name AND NOT a.attisdropped
                    LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
                    LEFT JOIN information_schema.column_udt_usage u ON c.table_schema = u.table_schema
                        AND c.table_name = u.table_name
                        AND c.column_name = u.column_name
                        AND c.udt_name = u.udt_name
                    WHERE c.table_name = '{table_name}' AND c.table_schema = '{table_schema}'
                    ORDER BY c.ordinal_position
                """
            self.connect()
            cursor = self.connection.cursor()
            self.config_parser.print_log_message('DEBUG2', f"postgresql_connector: fetch_table_columns: PostgreSQL: Reading columns for {table_schema}.{table_name}")
            cursor.execute(query)
            for row in cursor.fetchall():
                ordinal_position = row[0]
                column_name = row[1]
                data_type = row[2]
                character_maximum_length = row[3]
                numeric_precision = row[4]
                numeric_scale = row[5]
                is_identity = row[6]
                is_nullable = row[7]
                column_default = row[8]
                udt_schema = row[9]
                udt_name = row[10]
                column_comment = row[11]
                is_generated = row[12]
                format_type = row[13] if len(row) > 13 and row[13] else ''
                collation_schema = row[14] if len(row) > 14 else None
                collation_name = row[15] if len(row) > 15 else None
                # 's' = stored, 'v' = virtual (PostgreSQL 18+), '' = ordinary column.
                # information_schema.columns.is_generated reports only ALWAYS / NEVER and
                # carries no expression, so the catalog is used instead.
                attgenerated = row[16] if len(row) > 16 else ''
                generation_expression = row[17] if len(row) > 17 else None

                if data_type.upper() == 'ARRAY' or (format_type and '[]' in format_type):
                    data_type = format_type if format_type else data_type
                    column_type = data_type
                else:
                    column_type = data_type
                    if (self.is_string_type(data_type) or data_type.upper() in ('BIT', 'VARBIT', 'BIT VARYING')) and character_maximum_length:
                        column_type = f"{data_type}({character_maximum_length})"
                    elif self.is_numeric_type(data_type) and numeric_precision and numeric_scale:
                        column_type = f"{data_type}({numeric_precision},{numeric_scale})"
                    elif self.is_numeric_type(data_type) and numeric_precision and not numeric_scale:
                        column_type = f"{data_type}({numeric_precision})"
                result[ordinal_position] = {
                    'column_name': column_name,
                    'is_nullable': is_nullable,
                    'column_default_name': '',
                    'column_default_value': column_default,
                    'data_type': data_type,
                    'column_type': column_type,
                    'basic_data_type': data_type if data_type not in ('USER-DEFINED', 'DOMAIN') else '',
                    'is_identity': is_identity,
                    'character_maximum_length': character_maximum_length,
                    'numeric_precision': numeric_precision,
                    'numeric_scale': numeric_scale,
                    'udt_schema': udt_schema,
                    'udt_name': udt_name,
                    'column_comment': column_comment,
                    'is_generated_virtual': 'YES' if attgenerated == 'v' else 'NO',
                    'is_generated_stored': 'YES' if attgenerated == 's' else 'NO',
                    'generation_expression': generation_expression or '',
                    'stripped_generation_expression': generation_expression or '',
                    'collation_schema': collation_schema,
                    'collation_name': collation_name,
                }
            cursor.close()
            self.disconnect()
            return result

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_table_columns: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_types_mapping(self, settings):
        target_db_type = settings['target_db_type']
        types_mapping = {}
        if target_db_type != 'postgresql':
            raise ValueError(f"Unsupported target database type: {target_db_type}")
        return types_mapping

    def convert_expression_identifiers(self, expression, converted_columns=None, convert_plain_text=None):
        """
        Rewrite the identifiers of a source SQL expression for PostgreSQL.

        Source engines write the expression with their own identifier quoting and case
        (Oracle: "CREDIT_LIMIT", MySQL/MariaDB: `credit_limit`, others: bare names). Names
        that match a column of the table are re-emitted with the configured name-case
        handling and PostgreSQL quoting; when no column list is available, any quoted
        identifier is treated as a column name (only identifiers are quoted in an
        expression). String literals, function names and keywords are left untouched.

        convert_plain_text, if given, is applied to everything between the tokens - use it
        for operator translation that must not touch the inside of string literals.
        """
        if not expression:
            return expression

        column_names = {}
        for column_info in (converted_columns or {}).values():
            column_names[column_info['column_name'].upper()] = column_info['column_name']

        # String literal | double-quoted identifier | backtick-quoted identifier | bare identifier
        token_pattern = re.compile(r"""('(?:''|[^'])*')|("(?:""|[^"])*")|(`[^`]*`)|([A-Za-z_][A-Za-z0-9_$#]*)""")

        def convert_plain(text):
            return convert_plain_text(text) if convert_plain_text else text

        converted_parts = []
        last_end = 0
        for match in token_pattern.finditer(expression):
            converted_parts.append(convert_plain(expression[last_end:match.start()]))
            last_end = match.end()
            literal, quoted_identifier, backtick_identifier, identifier = match.groups()
            if literal:
                converted_parts.append(literal)
                continue
            if quoted_identifier:
                name = quoted_identifier[1:-1]
            elif backtick_identifier:
                name = backtick_identifier[1:-1]
            else:
                name = identifier
            if name.upper() in column_names:
                converted_parts.append(f'"{self.config_parser.convert_names_case(column_names[name.upper()])}"')
            elif (quoted_identifier or backtick_identifier) and not column_names:
                # No column list to match against - a quoted name in an expression is a column
                converted_parts.append(f'"{self.config_parser.convert_names_case(name)}"')
            elif backtick_identifier:
                # Unknown backtick-quoted name - keep the name, drop the foreign quoting
                converted_parts.append(f'"{name}"')
            else:
                converted_parts.append(match.group(0))
        converted_parts.append(convert_plain(expression[last_end:]))

        return ''.join(converted_parts).strip()

    def convert_generation_expression(self, expression, converted_columns, column_data_type):
        """
        Rewrite a source generated-column expression for PostgreSQL. The result is wrapped in
        parentheses, which GENERATED ALWAYS AS (...) STORED requires.
        """
        if not expression:
            return expression

        def convert_operators(text):
            # Concatenation is '+' in some source engines, '||' in PostgreSQL. Applied only
            # outside string literals, and only for string results (the same guard as before).
            # A PostgreSQL expression already uses '||' and its '+' really is an addition,
            # which must not be rewritten.
            if self.config_parser.get_source_db_type() == 'postgresql':
                return text
            return text.replace('+', '||') if self.is_string_type(column_data_type) else text

        converted_expression = self.convert_expression_identifiers(
            expression, converted_columns, convert_plain_text=convert_operators)
        if not self.expression_is_parenthesized(converted_expression):
            converted_expression = f"({converted_expression})"
        return converted_expression

    def split_top_level_commas(self, text):
        """ Split on commas that are outside parentheses, string literals and quoted names. """
        parts = []
        current = []
        depth = 0
        in_literal = False
        in_quoted_name = False
        for char in text:
            if in_literal:
                current.append(char)
                if char == "'":
                    in_literal = False
                continue
            if in_quoted_name:
                current.append(char)
                if char == '"':
                    in_quoted_name = False
                continue
            if char == "'":
                in_literal = True
            elif char == '"':
                in_quoted_name = True
            elif char == '(':
                depth += 1
            elif char == ')':
                depth = max(depth - 1, 0)
            elif char == ',' and depth == 0:
                parts.append(''.join(current))
                current = []
                continue
            current.append(char)
        parts.append(''.join(current))
        return [part for part in parts if part.strip()]

    def split_leading_identifier(self, text):
        """
        Split the leading - possibly schema qualified and quoted - identifier off the text.
        Returns tuple (identifier, rest). Quoted parts are kept as they are, so names
        containing dots (e.g. "en_US.utf8") survive untouched.
        """
        position = 0
        length = len(text)
        token = ''
        while position < length:
            char = text[position]
            if char == '"':
                end = position + 1
                while end < length:
                    if text[end] == '"':
                        if end + 1 < length and text[end + 1] == '"':
                            end += 2
                            continue
                        break
                    end += 1
                token += text[position:min(end + 1, length)]
                position = end + 1
            elif char.isspace():
                break
            else:
                start = position
                while position < length and not text[position].isspace() and text[position] != '.':
                    position += 1
                token += text[start:position]
            if position < length and text[position] == '.':
                token += '.'
                position += 1
                continue
            break
        return token, text[position:].strip()

    def parse_identifier_parts(self, identifier):
        """ Split a qualified identifier into its parts and remove the quoting. """
        parts = []
        current = ''
        in_quotes = False
        position = 0
        while position < len(identifier):
            char = identifier[position]
            if char == '"':
                if in_quotes and position + 1 < len(identifier) and identifier[position + 1] == '"':
                    current += '"'
                    position += 2
                    continue
                in_quotes = not in_quotes
                position += 1
                continue
            if char == '.' and not in_quotes:
                parts.append(current)
                current = ''
                position += 1
                continue
            current += char
            position += 1
        parts.append(current)
        return [part.strip() for part in parts]

    def qualify_text_search_references(self, sql, text_search_objects):
        """
        Point '<name>'::regconfig / '<name>'::regdictionary literals at the objects
        recreated in the target schema.

        These references live inside a string literal, so they cannot be schema qualified
        by rewriting identifiers - and the source does not even hand them over qualified:
        pg_get_viewdef() and pg_get_expr() print the bare name whenever the object is
        visible in the search_path of the source. Left as they are, they would be resolved
        against the search_path of the target session, which normally does not contain the
        target schema of the migration.
        Names which were not migrated (built-in configurations like pg_catalog.english)
        are left untouched.
        """
        if not sql or not text_search_objects:
            return sql

        def replace_reference(match):
            raw_name = match.group('name')
            cast = match.group('cast')
            parts = self.parse_identifier_parts(raw_name)
            object_name = parts[-1]
            mapped_object = text_search_objects.get(object_name)
            if not mapped_object:
                return match.group(0)
            target_schema = mapped_object['target_schema_name']
            target_name = mapped_object['target_object_name']
            return f"'{self.quote_identifier_for_literal(target_schema)}.{self.quote_identifier_for_literal(target_name)}'::{cast}"

        return re.sub(
            r"'(?P<name>(?:[^']|'')+)'::(?P<cast>regconfig|regdictionary)\b",
            replace_reference, sql, flags=re.IGNORECASE)

    def quote_identifier_for_literal(self, name):
        """
        Quote an identifier which is part of an object name inside a string literal
        (e.g. '"My Schema"."My Config"'::regconfig). Simple lower case names need no quoting.
        """
        if re.fullmatch(r'[a-z_][a-z0-9_$]*', name or '') and name not in self.RESERVED_IDENTIFIERS:
            return name
        return '"' + (name or '').replace('"', '""') + '"'

    # Keywords which must stay quoted when they are used as an object name
    RESERVED_IDENTIFIERS = frozenset({'default', 'simple', 'all', 'analyse', 'analyze', 'and',
                                      'any', 'array', 'as', 'asc', 'both', 'case', 'cast',
                                      'check', 'collate', 'column', 'constraint', 'create',
                                      'current_date', 'current_time', 'current_timestamp',
                                      'current_user', 'desc', 'distinct', 'do', 'else', 'end',
                                      'except', 'false', 'for', 'foreign', 'from', 'grant',
                                      'group', 'having', 'in', 'initially', 'intersect', 'into',
                                      'leading', 'limit', 'localtime', 'localtimestamp', 'not',
                                      'null', 'offset', 'on', 'only', 'or', 'order', 'placing',
                                      'primary', 'references', 'returning', 'select', 'session_user',
                                      'some', 'symmetric', 'table', 'then', 'to', 'trailing',
                                      'true', 'union', 'unique', 'user', 'using', 'variadic',
                                      'when', 'where', 'window', 'with'})

    def get_existing_collation_names(self):
        """
        Names of collations which already exist in this database and are usable with its
        encoding - built-in ones (C, POSIX) and those provided by the operating system / ICU.
        """
        if self.existing_collation_names is None:
            self.existing_collation_names = set()
            # This runs in the middle of the DDL generation - a connection opened by the
            # caller must stay open, only a connection opened here is closed again.
            opened_here = self.connection is None or self.connection.closed
            try:
                if opened_here:
                    self.connect()
                cursor = self.connection.cursor()
                cursor.execute("""
                    SELECT collname FROM pg_catalog.pg_collation
                    WHERE collencoding IN (-1, pg_char_to_encoding(pg_catalog.current_setting('server_encoding')))
                """)
                self.existing_collation_names = {row[0] for row in cursor.fetchall()}
                cursor.close()
                if opened_here:
                    self.disconnect()
            except Exception as e:
                self.config_parser.print_log_message('WARNING', f"postgresql_connector: get_existing_collation_names: Cannot read collations of the database: {e}")
        return self.existing_collation_names

    def get_collate_clause(self, raw_collation, user_collations):
        """
        Build the COLLATE clause for the target database.
        Collations migrated together with the schema are referenced with the target schema -
        in the source they were often unqualified and resolved through the search_path,
        which does not contain the target schema during the migration.
        Built-in collations (C, POSIX, "en_US.utf8", ...) are kept as they are.
        """
        collation_parts = self.parse_identifier_parts(raw_collation)
        collation_name = collation_parts[-1]
        mapped_collation = (user_collations or {}).get(collation_name)
        if mapped_collation:
            return f''' COLLATE "{mapped_collation['target_schema_name']}"."{mapped_collation['target_collation_name']}"'''
        existing_collations = self.get_existing_collation_names()
        if existing_collations and collation_name not in existing_collations:
            # Operating system / ICU collations are not migrated, and collations of other
            # engines (utf8mb4_general_ci, Latin1_General_CI_AS, ...) have no counterpart
            # here at all. When the target cannot provide the collation, the default
            # collation is used instead of failing the whole object.
            self.config_parser.print_log_message('WARNING', f"postgresql_connector: get_collate_clause: Collation {collation_name} does not exist in the target database - the reference is dropped and the default collation is used.")
            return ''
        converted_parts = []
        for part in collation_parts:
            # Names of built-in collations (C, POSIX, en_US.utf8, ...) are case sensitive
            # and must not be touched by the configured name case handling.
            converted = part if self.config_parser.get_source_db_type() == 'postgresql' else self.config_parser.convert_names_case(part)
            converted_parts.append(f'"{converted}"' if converted else f'"{part}"')
        return f' COLLATE {".".join(converted_parts)}'

    def expression_is_parenthesized(self, expression):
        """ True when the whole expression is already wrapped in one pair of parentheses. """
        if not expression.startswith('(') or not expression.endswith(')'):
            return False
        depth = 0
        in_literal = False
        for position, char in enumerate(expression):
            if in_literal:
                if char == "'":
                    in_literal = False
                continue
            if char == "'":
                in_literal = True
            elif char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
                if depth == 0:
                    return position == len(expression) - 1
        return False

    def get_create_table_sql(self, settings):
        source_schema_name = settings['source_schema_name']
        source_table_name = settings['source_table_name']
        source_table_id = settings['source_table_id']
        # target_schema_name = self.config_parser.convert_names_case(settings['target_schema_name'])
        target_schema_name = settings['target_schema_name'] ## target schema is used as it is defined in config, not converted to upper/lower case
        target_table_name = self.config_parser.convert_names_case(settings['target_table_name'])
        # source_columns = settings['source_columns']
        converted = settings['target_columns']
        migrator_tables = settings['migrator_tables']
        user_collations = settings.get('user_collations') or {}
        text_search_objects = settings.get('text_search_objects') or {}
        create_table_sql = ""
        create_table_sql_parts = []

        ## A partition of the source used to be answered here with
        ## `CREATE TABLE … PARTITION OF <parent>`, which produced a statement the target
        ## refused: the parent was created WITHOUT a PARTITION BY clause, because nothing
        ## added one, and its rows were migrated twice - once through the parent, which
        ## answers all of them, and once through the partition. The whole decision is the
        ## planner's now (`partitioning.build_plan()`): a partition of a table which is being
        ## migrated is not a table of its own, and the parent carries both its PARTITION BY
        ## clause and the statements which create its partitions.

        self.config_parser.print_log_message('DEBUG', f"postgresql_connector: get_create_table_sql: Creating DDL for table {target_schema_name}.{target_table_name}, case handling: {self.config_parser.get_names_case_handling()}")

        for _, column_info in converted.items():

            column_name = self.config_parser.convert_names_case(column_info['column_name'])

            self.config_parser.print_log_message('DEBUG3', f"postgresql_connector: get_create_table_sql: Creating DDL for table {target_schema_name}.{target_table_name}, column_info: {column_info}")

            if column_info['is_hidden_column'] == 'YES':
                self.config_parser.print_log_message('DEBUG', f"postgresql_connector: get_create_table_sql: Skipping hidden column {column_name}: {column_info}")
                continue

            create_column_sql = ""
            column_data_type = column_info['data_type'].upper()
            is_identity = column_info['is_identity']
            # if column_info['column_type_substitution'] != '':
            #     column_data_type = column_info['column_type_substitution'].upper()
            if column_info['data_type'] == 'USER-DEFINED' and column_info['udt_schema'] != '' and column_info['udt_name'] != '':
                mapped_schema = target_schema_name if column_info['udt_schema'].upper() == source_schema_name.upper() else column_info['udt_schema']
                # Apply the configured name-case handling to the type name so it matches the
                # user-defined type actually created in the target (e.g. lower-cased).
                column_data_type = f'''"{mapped_schema}"."{self.config_parser.convert_names_case(column_info['udt_name'])}"'''
            # elif column_info['basic_data_type'] != '':
            #     column_data_type = column_info['basic_data_type'].upper()

            character_maximum_length = ''
            if 'character_maximum_length' in column_info and column_info['character_maximum_length'] not in ('', None):
                character_maximum_length = column_info['character_maximum_length']
            if column_info['basic_character_maximum_length'] not in ('', None):
                character_maximum_length = column_info['basic_character_maximum_length']
            if str(character_maximum_length).startswith('-') or (isinstance(character_maximum_length, (int, float)) and character_maximum_length < 0):
                character_maximum_length = ''
                if 'CHAR' in column_data_type:
                    column_data_type = 'TEXT'

            domain_name = column_info['domain_name']

            column_comment = column_info['column_comment']
            nullable_string = ''
            if column_info['is_nullable'] == 'NO':
                is_mysql_db = self.config_parser.get_source_db_type() in ('mysql', 'mariadb')
                is_datetime_col = any(t in column_data_type for t in ('DATE', 'TIME', 'TIMESTAMP'))
                if is_mysql_db and is_datetime_col and self.config_parser.get_relax_not_null_datetime() and not self.config_parser.get_zero_datetime_data_value():
                    nullable_string = ''
                else:
                    nullable_string = 'NOT NULL'

            numeric_precision = column_info.get('numeric_precision')
            numeric_scale = column_info.get('numeric_scale')
            basic_numeric_precision = column_info.get('basic_numeric_precision')
            basic_numeric_scale = column_info.get('basic_numeric_scale')
            altered_data_type = ''

            if is_identity == 'YES' and column_data_type not in ('BIGINT', 'INTEGER', 'SMALLINT'):
                altered_data_type = 'BIGINT'
                migrator_tables.insert_target_column_alteration({
                    'target_schema_name': settings['target_schema_name'],
                    'target_table_name': settings['target_table_name'],
                    'target_column': column_info['column_name'],
                    'reason': 'IDENTITY',
                    'original_data_type': column_data_type,
                    'altered_data_type': altered_data_type,
                })
                create_column_sql = f""""{column_name}" {altered_data_type}"""
                self.config_parser.print_log_message('DEBUG', f"postgresql_connector: get_create_table_sql: Column {column_name} is identity, altered data type to {altered_data_type}")
            elif column_data_type in ('NUMBER', 'NUMERIC') and (numeric_precision is None or numeric_precision == 10) and numeric_scale == 0:
                altered_data_type = 'INTEGER'
                migrator_tables.insert_target_column_alteration({
                    'target_schema_name': settings['target_schema_name'],
                    'target_table_name': settings['target_table_name'],
                    'target_column': column_info['column_name'],
                    'reason': 'NUMBER without precision, scale ' + str(numeric_scale),
                    'original_data_type': column_data_type,
                    'altered_data_type': altered_data_type,
                })
                create_column_sql = f""""{column_name}" {altered_data_type}"""
                self.config_parser.print_log_message('DEBUG', f"postgresql_connector: get_create_table_sql: Column {column_name} is NUMBER without precision, scale {numeric_scale}, altered data type to {altered_data_type}")
            elif column_data_type in ('NUMBER', 'NUMERIC') and numeric_precision is None and numeric_scale == 10:
                altered_data_type = 'DOUBLE PRECISION'
                migrator_tables.insert_target_column_alteration({
                    'target_schema_name': settings['target_schema_name'],
                    'target_table_name': settings['target_table_name'],
                    'target_column': column_info['column_name'],
                    'reason': 'NUMBER without precision, scale ' + str(numeric_scale),
                    'original_data_type': column_data_type,
                    'altered_data_type': altered_data_type,
                })
                create_column_sql = f""""{column_name}" {altered_data_type}"""
                self.config_parser.print_log_message('DEBUG', f"postgresql_connector: get_create_table_sql: Column {column_name} is NUMBER without precision, scale {numeric_scale}, altered data type to {altered_data_type}")
            elif column_data_type in ('NUMBER', 'NUMERIC') and numeric_precision == 1 and numeric_scale == 0:
                # Narrow numeric (precision 1, scale 0). Such a column may be a 0/1
                # boolean flag OR a small integer code (e.g. channel_id, day-of-week),
                # which are indistinguishable from the metadata. Default to SMALLINT
                # (lossless); opt specific columns in to BOOLEAN via
                # migration.numeric_1_boolean_columns (or the global map_numeric_1_to_boolean).
                if self.config_parser.should_map_numeric_1_to_boolean(
                        target_schema_name, target_table_name, column_info['column_name']):
                    altered_data_type = 'BOOLEAN'
                    alteration_reason = 'NUMBER with precision 1, scale 0 (mapped to boolean by config)'
                else:
                    altered_data_type = 'SMALLINT'
                    alteration_reason = 'NUMBER with precision 1, scale 0'
                migrator_tables.insert_target_column_alteration({
                    'target_schema_name': settings['target_schema_name'],
                    'target_table_name': settings['target_table_name'],
                    'target_column': column_info['column_name'],
                    'reason': alteration_reason,
                    'original_data_type': column_data_type,
                    'altered_data_type': altered_data_type,
                })
                create_column_sql = f""""{column_name}" {altered_data_type}"""
                self.config_parser.print_log_message('DEBUG', f"postgresql_connector: get_create_table_sql: Column {column_name} is NUMBER with precision 1, scale 0, altered data type to {altered_data_type}")
            elif column_data_type in ('NUMBER', 'NUMERIC') and numeric_precision == 19 and numeric_scale == 0:
                altered_data_type = 'BIGINT'
                migrator_tables.insert_target_column_alteration({
                    'target_schema_name': settings['target_schema_name'],
                    'target_table_name': settings['target_table_name'],
                    'target_column': column_info['column_name'],
                    'reason': 'NUMBER with precision 19, scale 0',
                    'original_data_type': column_data_type,
                    'altered_data_type': altered_data_type,
                })
                create_column_sql = f""""{column_name}" {altered_data_type}"""
                self.config_parser.print_log_message('DEBUG', f"postgresql_connector: get_create_table_sql: Column {column_name} is NUMBER with precision 19, scale 0, altered data type to {altered_data_type}")
            else:
                if (character_maximum_length != '' and ('CHAR' in column_data_type or 'BIT' in column_data_type)):
                    create_column_sql = f""""{column_name}" {column_data_type}({character_maximum_length})"""
                elif self.is_numeric_type(column_data_type) and column_data_type in ('DECIMAL', 'NUMERIC'):
                    if numeric_precision not in (None, '') and numeric_scale not in (None, ''):
                        create_column_sql = f""""{column_name}" {column_data_type}({numeric_precision},{numeric_scale})"""
                    elif numeric_precision not in (None, ''):
                        create_column_sql = f""""{column_name}" {column_data_type}({numeric_precision})"""
                    elif basic_numeric_precision not in (None, '') and basic_numeric_scale not in (None, ''):
                        create_column_sql = f""""{column_name}" {column_data_type}({basic_numeric_precision},{basic_numeric_scale})"""
                    elif basic_numeric_precision not in (None, ''):
                        create_column_sql = f""""{column_name}" {column_data_type}({basic_numeric_precision})"""
                    else:
                        create_column_sql = f""""{column_name}" {column_data_type}"""
                else:
                    create_column_sql = f""""{column_name}" {column_data_type}"""

            if altered_data_type != '':
                column_data_type = altered_data_type

            # Column collation - only collatable (string) columns can carry one, and only
            # when the column does not use the database default collation.
            column_collation = column_info.get('collation_name')
            if column_collation and altered_data_type == '':
                create_column_sql += self.get_collate_clause(f'"{column_collation}"', user_collations)

            if nullable_string != '':
                create_column_sql += f""" {nullable_string}"""

            if is_identity == 'YES':
                create_column_sql += " GENERATED BY DEFAULT AS IDENTITY"

            if column_info['is_generated_virtual'] == 'YES' or column_info['is_generated_stored'] == 'YES':
                generated_column_expression = self.convert_generation_expression(
                    column_info['stripped_generation_expression'], converted, column_data_type)
                generated_column_expression = self.qualify_text_search_references(
                    generated_column_expression, text_search_objects)
                if generated_column_expression:
                    # VIRTUAL generated columns exist since PostgreSQL 18. On an older target
                    # the column is created as STORED - the values are the same, it only costs
                    # storage instead of computing time.
                    storage_kind = 'STORED'
                    if column_info['is_generated_virtual'] == 'YES' and column_info['is_generated_stored'] != 'YES':
                        target_version = self.get_server_version_num()
                        if target_version is None or target_version >= 180000:
                            storage_kind = 'VIRTUAL'
                        else:
                            self.config_parser.print_log_message('WARNING', f"postgresql_connector: get_create_table_sql: Column {column_name} is a VIRTUAL generated column, which the target database does not support - it is created as STORED.")
                    create_column_sql += f" GENERATED ALWAYS AS {generated_column_expression} {storage_kind}"
                else:
                    # Without an expression the column cannot be generated - creating it as an
                    # ordinary column is better than emitting "GENERATED ALWAYS AS  STORED".
                    self.config_parser.print_log_message('WARNING', f"postgresql_connector: get_create_table_sql: Column {column_name} is marked as generated but carries no generation expression - created as an ordinary column.")

            column_default = ''
            if column_info['column_default_name'] != '' and column_info['column_default_value'] == '' and column_info['replaced_column_default_value'] == '':
                default_value_info = migrator_tables.get_default_value_details({'default_value_name': column_info['column_default_name']})
                if default_value_info:
                    column_default = default_value_info['extracted_default_value']

            elif column_info['column_default_value'] or column_info['replaced_column_default_value']:
                column_default = column_info['column_default_value']
                if column_info['replaced_column_default_value']:
                    column_default = column_info['replaced_column_default_value'].strip()

            if column_default != '':
                # Remove SQL Server syntax artifacts like () around default values
                column_default = column_default.strip()

                # Remove outer parentheses if present (SQL Server specific) - SQL Server
                # can wrap defaults in several layers, e.g. ((1000.0000))
                column_default = self.strip_enclosing_parentheses(column_default)

                val_clean = column_default.strip().strip("'\"")
                if val_clean.startswith('0000-00-00') or val_clean in ('0000-00-00', '0000-00-00 00:00:00', '0000-00-00 00:00:00.000000', '00:00:00'):
                    action = self.config_parser.get_zero_datetime_default()
                    if action is None or str(action).strip().lower() in ('', 'none', 'null', 'remove', 'delete', 'disable'):
                        column_default = ''
                    else:
                        action_str = str(action).strip()
                        keywords = ('CURRENT_TIMESTAMP', 'NOW()', 'CURRENT_DATE', 'CLOCK_TIMESTAMP()', 'LOCALTIMESTAMP')
                        if action_str.upper() in keywords:
                            column_default = action_str
                        elif (action_str.startswith("'") and action_str.endswith("'")) or (action_str.startswith('"') and action_str.endswith('"')):
                            column_default = action_str
                        else:
                            column_default = f"'{action_str}'"

                if column_default != '':
                    # Niladic SQL keyword-functions have no parentheses (e.g. Oracle USER or
                    # Sybase suser_name() mapped to current_user), so they must NOT be quoted
                    # as string literals.
                    sql_keyword_defaults = (
                        'current_user', 'current_role', 'session_user', 'user',
                        'current_catalog', 'current_schema',
                        'current_timestamp', 'current_date', 'current_time',
                        'localtime', 'localtimestamp',
                    )
                    default_is_expression = (
                        '||' in column_default or '(' in column_default
                        or ')' in column_default or '::' in column_default
                        or column_default.strip().lower() in sql_keyword_defaults
                    )
                    # value which already is a complete string literal ('ACTIVE', 'it''s')
                    # must be used as it is - quoting it again would break embedded quotes
                    default_is_string_literal = re.fullmatch(r"'(?:[^']|'')*'", column_default) is not None
                    source_db_type = self.config_parser.get_source_db_type()
                    # The default has to match the type the column is really created with:
                    # a column altered above (NUMERIC(1,0) mapped to BOOLEAN by
                    # migration.map_numeric_1_to_boolean, an identity widened to BIGINT)
                    # would otherwise get the default of the type it no longer has -
                    # 'column ... is of type boolean but default expression is of type integer'.
                    effective_data_type = altered_data_type.upper() if altered_data_type else column_data_type
                    if default_is_expression or default_is_string_literal:
                        create_column_sql += f""" DEFAULT {column_default}"""
                    elif 'CHAR' in effective_data_type or effective_data_type in ('TEXT'):
                        # here we must quote the default value
                        escaped_default = column_default.replace("'", "''")
                        create_column_sql += f""" DEFAULT '{escaped_default}'"""
                    elif effective_data_type in ('BOOLEAN', 'BOOL') or (source_db_type != 'postgresql' and effective_data_type == 'BIT'):
                        clean_default = column_default.lower().strip()
                        if clean_default in ('0', '(0)', 'false', "b'0'"):
                            create_column_sql += """ DEFAULT FALSE"""
                        elif clean_default in ('1', '(1)', 'true', "b'1'"):
                            create_column_sql += """ DEFAULT TRUE"""
                        else:
                            if source_db_type != 'postgresql':
                                create_column_sql += f""" DEFAULT {column_default}::BOOLEAN"""
                            else:
                                create_column_sql += f""" DEFAULT {column_default}"""
                    elif effective_data_type in ('BYTEA'):
                        create_column_sql += f""" DEFAULT '{column_default}'::BYTEA"""
                    else:
                        create_column_sql += f" DEFAULT {column_default}"

            if domain_name:
                domain_details = migrator_tables.get_domain_details({'source_domain_name': domain_name})
                if domain_details:
                    domain_row_id = domain_details['id']
                    domain_name = domain_details['source_domain_name']
                    migrated_as = domain_details['migrated_as']
                    source_domain_check_sql = domain_details['source_domain_check_sql']
                    if source_domain_check_sql:
                        # Replace exact word VALUE with the column name, case-sensitive, word boundary
                        pattern = r'\bVALUE\b'
                        source_domain_check_sql = re.sub(pattern, f'"{column_info["column_name"]}"', source_domain_check_sql)
                    if migrated_as == 'CHECK CONSTRAINT':
                        constraint_name = f"{domain_name}_tab_{target_table_name}"
                        create_constraint_sql = f"""ALTER TABLE "{target_schema_name}"."{target_table_name}" ADD CONSTRAINT "{constraint_name}" CHECK({source_domain_check_sql})"""
                        migrator_tables.insert_constraint({
                            'source_schema_name': source_schema_name,
                            'source_table_name': source_table_name,
                            'source_table_id': source_table_id,
                            'target_schema_name': target_schema_name,
                            'target_table_name': target_table_name,
                            'constraint_name': constraint_name,
                            'constraint_type': 'CHECK (from domain)',
                            'constraint_sql': create_constraint_sql,
                            'constraint_comment': ('added from domains ' + column_comment).strip(),
                        })

            self.config_parser.print_log_message('DEBUG3', f"postgresql_connector: get_create_table_sql: Creating DDL for table {target_schema_name}.{target_table_name}, create_column_sql: {create_column_sql}")
            create_table_sql_parts.append(create_column_sql)

        create_table_sql = ", ".join(create_table_sql_parts)

        ## The PARTITION BY clause of a partitioned parent used to be appended here, out of a
        ## fetch_table_names() of the whole schema run once per table. It is the planner's now,
        ## from the decision `partitioning.build_plan()` took - which is the only place where
        ## preserving the scheme of the source, flattening it and re-partitioning by
        ## target_partitioning can be told apart, and the only place which also knows what to do
        ## with the partitions themselves.
        create_table_sql = f"""CREATE TABLE "{target_schema_name}"."{target_table_name}" ({create_table_sql})"""
        return create_table_sql

    def is_string_type(self, column_type: str) -> bool:
        string_types = ['CHAR', 'VARCHAR', 'NCHAR', 'NVARCHAR', 'TEXT', 'LONG VARCHAR', 'LONG NVARCHAR', 'UNICHAR', 'UNIVARCHAR']
        return column_type.upper() in string_types

    def is_numeric_type(self, column_type: str) -> bool:
        numeric_types = ['BIGINT', 'INTEGER', 'INT', 'TINYINT', 'SMALLINT', 'FLOAT', 'DOUBLE PRECISION', 'DECIMAL', 'NUMERIC']
        return column_type.upper() in numeric_types

    def extract_index_key_list(self, index_sql):
        """
        Return the key list of a CREATE INDEX statement - everything between the parentheses
        which follow the access method.

        The parentheses have to be counted, they cannot be matched with a regular expression:
        the key of a functional index is an expression of its own (`lower(company_name)`,
        `lower((email)::text)`), and stopping at the first closing parenthesis truncates it.
        What follows the key list (`INCLUDE (...)`, `WITH (...)`, `WHERE ...`) is not part of
        the result.
        """
        if not index_sql:
            return ''

        start = None
        using_match = re.search(r'\b(?i:USING)\s+[a-zA-Z0-9_]+\s*\(', index_sql)
        if using_match:
            start = using_match.end() - 1
        else:
            # An index definition without the USING clause - take the first parenthesis
            # behind the table name.
            on_match = re.search(r'\b(?i:ON)\s+', index_sql)
            position = index_sql.find('(', on_match.end() if on_match else 0)
            if position >= 0:
                start = position
        if start is None:
            return ''
        end = self.find_matching_parenthesis(index_sql, start)
        if end is None:
            # Unbalanced definition - return what is behind the opening parenthesis
            return index_sql[start + 1:].strip()
        return index_sql[start + 1:end].strip()

    def find_matching_parenthesis(self, text, start):
        """
        Position of the parenthesis closing the one at `start`, ignoring parentheses inside
        string literals and quoted identifiers. None when the text is unbalanced.
        """
        depth = 0
        in_literal = False
        in_quoted_name = False
        for position in range(start, len(text)):
            char = text[position]
            if in_literal:
                if char == "'":
                    in_literal = False
                continue
            if in_quoted_name:
                if char == '"':
                    in_quoted_name = False
                continue
            if char == "'":
                in_literal = True
            elif char == '"':
                in_quoted_name = True
            elif char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
                if depth == 0:
                    return position
        return None

    def strip_generated_column_clauses(self, create_table_sql):
        """
        Remove `GENERATED ALWAYS AS (<expression>) STORED / VIRTUAL` from a CREATE TABLE
        statement, leaving ordinary columns behind.

        Used for the intermediate table of the LOB import: it is a staging copy which has to
        accept the values coming from the data file, and a generated column rejects them.
        `GENERATED BY DEFAULT AS IDENTITY` and `GENERATED ALWAYS AS IDENTITY` are not touched -
        they are not followed by a parenthesized expression.
        """
        if not create_table_sql:
            return create_table_sql

        pattern = re.compile(r'\s*\b(?i:GENERATED)\s+(?i:ALWAYS)\s+(?i:AS)\s*\(')
        result = create_table_sql
        while True:
            match = pattern.search(result)
            if not match:
                return result
            expression_end = self.find_matching_parenthesis(result, match.end() - 1)
            if expression_end is None:
                return result
            remainder = result[expression_end + 1:]
            keyword = re.match(r'\s*\b(?i:STORED|VIRTUAL)\b', remainder)
            if keyword:
                remainder = remainder[keyword.end():]
            result = result[:match.start()] + remainder

    def extract_index_tail(self, index_sql):
        """
        Return everything the index definition carries behind its key list - `INCLUDE (...)`,
        `NULLS NOT DISTINCT`, `WITH (...)` storage parameters and the `WHERE` predicate of a
        partial index. It already is valid PostgreSQL and is appended to the generated
        statement unchanged, apart from the identifier case handling and the text search
        references, which the caller applies.

        A `TABLESPACE` clause is deliberately dropped - the tablespace of the source does
        not have to exist in the target, and where it does not, it would fail the index.
        """
        if not index_sql:
            return ''
        using_match = re.search(r'\b(?i:USING)\s+[a-zA-Z0-9_]+\s*\(', index_sql)
        if using_match:
            start = using_match.end() - 1
        else:
            on_match = re.search(r'\b(?i:ON)\s+', index_sql)
            start = index_sql.find('(', on_match.end() if on_match else 0)
            if start < 0:
                return ''
        end = self.find_matching_parenthesis(index_sql, start)
        if end is None:
            return ''
        tail = index_sql[end + 1:].strip().rstrip(';').strip()
        if not tail:
            return ''
        tail = re.sub(r'\s*\b(?i:TABLESPACE)\s+(?:"(?:[^"]|"")+"|[a-zA-Z0-9_]+)', '', tail).strip()
        return tail

    def fetch_indexes(self, settings):
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

        table_indexes = {}
        order_num = 1
        query = f"""
            SELECT
                i.indexname,
                i.indexdef,
                CASE con.contype
                    WHEN 'p' THEN 'PRIMARY KEY'
                    WHEN 'u' THEN 'UNIQUE'
                    WHEN 'x' THEN 'EXCLUSION'
                    ELSE 'INDEX'
                END as type,
                obj_description(('"'||i.schemaname||'"."'||i.indexname||'"')::regclass::oid, 'pg_class') as index_comment,
                (x.indexprs IS NOT NULL) as is_expression_index,
                x.indisunique as is_unique,
                con.contype as constraint_kind,
                CASE WHEN con.oid IS NOT NULL THEN pg_get_constraintdef(con.oid) END as constraint_def
            FROM pg_indexes i
            JOIN pg_class t
            ON t.relnamespace::regnamespace::text = i.schemaname
            AND t.relname = i.tablename
            LEFT JOIN pg_class ic
            ON ic.relname = i.indexname
                AND ic.relnamespace = t.relnamespace
            LEFT JOIN pg_index x ON x.indexrelid = ic.oid
            LEFT JOIN pg_constraint con
            ON con.conindid = ic.oid
                AND con.conrelid = t.oid
                AND con.contype IN ('p', 'u', 'x')
            WHERE t.oid = {source_table_id}
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                index_name = row[0]
                index_type = row[2]
                index_sql = row[1]
                is_unique = row[5]
                constraint_kind = row[6]
                constraint_def = row[7]
                using_match = re.search(r'\b(?i:USING)\s+([a-zA-Z0-9_]+)', index_sql)
                using_method = using_match.group(1) if using_match else ''
                index_columns = self.extract_index_key_list(index_sql)
                index_tail = self.extract_index_tail(index_sql)
                # pg_index.indexprs tells us reliably that at least one key is an expression -
                # there is no need to recognize function names in the DDL text.
                is_function_based = 'YES' if row[4] else 'NO'

                # UNIQUE and EXCLUDE constraints are implemented by an index, but they are
                # objects of their own and are migrated by the constraints migration, which
                # uses the constraint definition. Recreating them here as a bare index is
                # both a duplicate and lossy - a temporal UNIQUE (... WITHOUT OVERLAPS) is
                # backed by a gist index, and "CREATE UNIQUE INDEX ... USING gist" is not
                # even a legal statement.
                if constraint_kind in ('u', 'x'):
                    self.config_parser.print_log_message('DEBUG', f"postgresql_connector: fetch_indexes: Index {index_name} implements constraint '{constraint_def}' - skipped here, it is created by the constraints migration.")
                    continue

                # A plain CREATE UNIQUE INDEX is not a constraint, so it is not listed in
                # information_schema.table_constraints - without pg_index.indisunique its
                # uniqueness would be lost.
                if is_unique and index_type == 'INDEX':
                    index_type = 'UNIQUE'

                table_indexes[order_num] = {
                    'index_name': index_name,
                    'index_type': index_type,
                    'index_owner': source_table_schema,
                    'index_columns': index_columns,
                    'index_sql': index_sql,
                    'index_comment': row[3],
                    'using_method': using_method,
                    'is_function_based': is_function_based,
                    # INCLUDE columns, NULLS NOT DISTINCT, WITH storage parameters and the
                    # WHERE predicate of a partial index
                    'index_tail': index_tail,
                    # Definition of the constraint the index implements (primary keys are
                    # created here, not by the constraints migration)
                    'constraint_def': constraint_def if constraint_kind == 'p' else '',
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return table_indexes
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_indexes: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_index_sql(self, settings):

        index_name = self.config_parser.convert_names_case(settings['index_name'])
        index_type = settings['index_type']
        target_schema_name = settings['target_schema_name']
        target_table_name = self.config_parser.convert_names_case(settings['target_table_name'])
        index_columns = settings['index_columns']
        target_columns = settings.get('target_columns')
        is_function_based = str(settings.get('is_function_based', 'NO')).upper() == 'YES'
        index_sql = settings.get('source_index_sql') or settings.get('index_sql', '')
        using_method = settings.get('using_method', '')
        user_collations = settings.get('user_collations') or {}
        text_search_objects = settings.get('text_search_objects') or {}
        constraint_def = settings.get('constraint_def') or ''

        # Last resort when the connector did not report the access method separately.
        # Only for a PostgreSQL source - the USING keyword of the other engines does not
        # name a PostgreSQL access method (MySQL USING BTREE / USING HASH).
        if not using_method and index_sql and self.config_parser.get_source_db_type() == 'postgresql':
            using_match = re.search(r'\b(?i:USING)\s+([a-zA-Z0-9_]+)', index_sql)
            if using_match:
                using_method = using_match.group(1)

        if not index_columns or not index_columns.strip():
            self.config_parser.print_log_message('WARNING', f"postgresql_connector: get_create_index_sql: Index '{index_name}' on table '{target_schema_name}.{target_table_name}' has empty columns list - skipping index creation.")
            return ''

        column_names = []
        for col in self.split_top_level_commas(index_columns):
            col = col.strip()

            # Split off trailing NULLS FIRST / NULLS LAST ordering keyword
            nulls_direction = ''
            parts_nulls = col.rsplit(None, 2)
            if len(parts_nulls) == 3 and parts_nulls[1].upper() == 'NULLS' and parts_nulls[2].upper() in ('FIRST', 'LAST'):
                col = parts_nulls[0].strip()
                nulls_direction = f' {parts_nulls[1].upper()} {parts_nulls[2].upper()}'

            # Split off trailing ASC/DESC ordering keyword
            order_direction = ''
            parts_order = col.rsplit(None, 1)
            if len(parts_order) == 2 and parts_order[1].upper() in ('ASC', 'DESC'):
                col = parts_order[0].strip()
                order_direction = f' {parts_order[1].upper()}'

            # An element of a function-based index is an expression
            if self.expression_is_parenthesized(col) or (is_function_based and '(' in col):
                ## The collations are taken out of the expression before anything reads it:
                ## the name of a collation is not an identifier of this migration, and the
                ## conversion below folds it with the rest of them - COLLATE "C" became
                ## COLLATE "c" under names_case_handling: lower, which PostgreSQL does not
                ## have. They are put back at the end of this branch.
                col, found_collations = collations.take_out(col)
                expression = self.convert_expression_identifiers(col, target_columns)
                expression = expression.replace(r"\'", "'").replace(r'\"', '"')
                if self.config_parser.get_source_db_type() != 'postgresql':
                    # Remove the charset introducer of MySQL / MariaDB (_utf8'text'). It
                    # introduces a string literal, so only a single quote can follow it,
                    # and it never follows an identifier character or a double quote:
                    # the tail of a quoted name ("unit_price", "natural_numeric") is not
                    # an introducer and used to be cut away with the rest of the name.
                    expression = re.sub(r'(?i)(?<![A-Za-z0-9_"])_[a-zA-Z0-9]+(\')', r'\1', expression)
                expression = re.sub(r'(?i)\b(?:CHARACTER\s+SET|CHARSET)\s+[a-zA-Z0-9_]+', '', expression)
                if self.config_parser.get_source_db_type() == 'postgresql':
                    # PostgreSQL collations are migrated with the schema - keep them in the
                    # expression, only point them to the collation in the target schema.
                    expression, _ = collations.put_back(
                        expression, found_collations,
                        resolve=lambda name: self.get_collate_clause(name, user_collations))
                else:
                    ## A collation of another engine used to be deleted here without a word,
                    ## for every source which is not PostgreSQL. It decides which strings
                    ## count as equal, so a case-insensitive index became a case-sensitive
                    ## one - it answers a query with fewer rows than the source did, and a
                    ## unique one stops refusing two values which differ only in case. What
                    ## PostgreSQL can say is carried over now (`_bin` compares byte by byte,
                    ## which is COLLATE "C") and what it cannot is reported. P1-3.
                    expression, _ = collations.put_back(
                        expression, found_collations,
                        existing_names=self.get_existing_collation_names(),
                        report=self.config_parser.print_log_message,
                        where=(f"postgresql_connector: get_create_index_sql: index "
                               f"'{index_name}' on table "
                               f"'{target_schema_name}.{target_table_name}'"),
                        index_type=index_type)
                expression = self.qualify_text_search_references(expression, text_search_objects)
                if not self.expression_is_parenthesized(expression):
                    expression = f"({expression})"
                column_names.append(f'{expression}{order_direction}{nulls_direction}')
                continue

            # Split off COLLATE clause if present
            collation_clause = ''
            collate_match = re.search(r'\s+(?i:COLLATE)\s+(.+)$', col)
            if collate_match:
                raw_collation, collate_remainder = self.split_leading_identifier(collate_match.group(1).strip())
                col = col[:collate_match.start()].strip()
                collation_clause = self.get_collate_clause(raw_collation, user_collations)
                if collate_remainder:
                    # Anything behind the collation name is the operator class - it is
                    # processed below together with the column name.
                    col = f"{col} {collate_remainder}"

            # Split off operator class if present (e.g. 'event_type gin_trgm_ops')
            opclass_clause = ''
            col_parts = col.split()
            if len(col_parts) == 2:
                col = col_parts[0].strip()
                opclass = col_parts[1].strip()
                opc_parts = opclass.split('.')
                conv_opc = []
                for opc in opc_parts:
                    opc_clean = opc.strip('`').strip("'").strip('"')
                    opc_conv = self.config_parser.convert_names_case(opc_clean)
                    conv_opc.append(f'"{opc_conv}"' if opc_conv else opc)
                opclass_clause = f' {".".join(conv_opc)}'

            col = col.strip('`').strip("'").strip('"')
            col = self.config_parser.convert_names_case(col)
            column_names.append(f'"{col}"{collation_clause}{opclass_clause}{order_direction}{nulls_direction}')
        # Join back with comma
        index_columns = ', '.join(column_names)

        spatial_types = ('POINT', 'GEOMETRY', 'BOX', 'POLYGON', 'PATH', 'CIRCLE', 'LINESTRING', 'MULTIPOINT', 'MULTILINESTRING', 'MULTIPOLYGON', 'GEOMETRYCOLLECTION')
        is_spatial = 'SPATIAL' in str(index_type).upper()
        if not is_spatial and target_columns:
            indexed_col_set = {c.strip('`').strip("'").strip('"').lower() for c in column_names}
            cols_list = target_columns.values() if isinstance(target_columns, dict) else target_columns
            for col_info in cols_list:
                if isinstance(col_info, dict):
                    c_name = str(col_info.get('column_name', '')).strip().lower()
                    c_type = str(col_info.get('column_data_type', col_info.get('data_type', col_info.get('type', '')))).upper()
                    if c_name in indexed_col_set and any(st in c_type for st in spatial_types):
                        is_spatial = True
                        break

        using_clause = ''
        if using_method and using_method.lower() != 'btree':
            using_clause = f" USING {using_method.lower()}"
        elif is_spatial:
            using_clause = " USING gist"

        # INCLUDE columns, NULLS NOT DISTINCT, storage parameters and the WHERE predicate of
        # a partial index. The predicate can reference columns and text search objects.
        index_tail = settings.get('index_tail') or ''
        if index_tail:
            index_tail = self.qualify_text_search_references(index_tail, text_search_objects)
            index_tail = ' ' + self.convert_constraint_sql_case(index_tail).strip()

        create_index_query = ''
        if constraint_def:
            # The index implements a table constraint - the constraint definition from the
            # source catalog is authoritative and carries what a key list cannot express
            # (WITHOUT OVERLAPS of a temporal key, NULLS NOT DISTINCT, INCLUDE columns,
            # DEFERRABLE).
            create_index_query = f"""ALTER TABLE "{target_schema_name}"."{target_table_name}" ADD CONSTRAINT "{index_name}_tab_{target_table_name}" {self.convert_constraint_sql_case(constraint_def)};"""
        elif index_type == 'PRIMARY KEY':
            create_index_query = f"""ALTER TABLE "{target_schema_name}"."{target_table_name}" ADD CONSTRAINT "{index_name}_tab_{target_table_name}" PRIMARY KEY ({index_columns});"""
        else:
            unique_clause = 'UNIQUE ' if index_type == 'UNIQUE' else ''
            create_index_query = f"""CREATE {unique_clause}INDEX "{index_name}_tab_{target_table_name}" ON "{target_schema_name}"."{target_table_name}"{using_clause} ({index_columns}){index_tail};"""

        return create_index_query

        # index_columns_count = 0
        # index_columns_data_types = []
        # for column_name in index_columns.split(','):
        #     column_name = column_name.strip().strip('"')
        #     for col_order_num, column_info in target_columns.items():
        #         if column_name == column_info['column_name']:
        #             index_columns_count += 1
        #             column_data_type = column_info['data_type']
        #             self.config_parser.print_log_message('DEBUG', f"postgresql_connector: get_create_index_sql: Table: {target_schema_name}.{target_table_name}, index: {index_name}, column: {column_name} has data type {column_data_type}")
        #             index_columns_data_types.append(column_data_type)
        #             index_columns_data_types_str = ', '.join(index_columns_data_types)

        # columns = []
        # for col in index_columns.split(","):
        #     col = col.strip().replace(" ASC", "").replace(" DESC", "")
        #     if col not in columns:
        #         columns.append('"'+col+'"')
        # index_columns = ','.join(columns)

    def fetch_constraints(self, settings):
        source_table_id = settings['source_table_id']
        source_table_schema = settings['source_table_schema']
        source_table_name = settings['source_table_name']

        order_num = 1
        constraints = {}
        # c = check constraint, f = foreign key constraint, n = not-null constraint (domains only),
        # p = primary key constraint, u = unique constraint, t = constraint trigger,
        # x = exclusion constraint
        query = f"""
            SELECT
                oid,
                conname,
                CASE WHEN contype = 'c'
                    THEN 'CHECK'
                WHEN contype = 'f'
                    THEN 'FOREIGN KEY'
                WHEN contype = 'p'
                    THEN 'PRIMARY KEY'
                WHEN contype = 'u'
                    THEN 'UNIQUE'
                WHEN contype = 't'
                    THEN 'TRIGGER'
                WHEN contype = 'x'
                    THEN 'EXCLUSION'
                ELSE contype::text
                END as type,
                pg_get_constraintdef(oid) as condef,
                CASE WHEN contype = 'f'
                    THEN (SELECT n.nspname FROM pg_namespace n, pg_class c WHERE c.oid = confrelid AND n.oid = c.relnamespace)
                ELSE NULL
                END AS ref_schema,
                CASE WHEN contype = 'f'
                    THEN (SELECT c.relname FROM pg_class c WHERE oid = confrelid)
                ELSE ''
                END AS ref_table,
                obj_description(oid, 'pg_constraint') as constraint_comment
            FROM pg_constraint
            WHERE conrelid = '{source_table_id}'::regclass
            AND contype NOT IN ('n')
            -- A constraint with a parent is one PostgreSQL maintains ITSELF for a partition
            -- tree: a foreign key referencing a partitioned table gets one child per partition
            -- of the referenced side, and a constraint on a partitioned parent gets one child
            -- per partition. Migrating them is wrong twice over - the target creates them
            -- again by itself from the parent constraint, under the same generated names, and
            -- each of them names a partition which is not a table of this migration at all.
            AND conparentid = 0
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                constraint_name = row[1]
                constraint_type = row[2]
                constraint_sql = row[3]
                constraint_ref_schema = row[4]
                constraint_ref_table = row[5]
                constraint_comment = row[6]

                if constraint_type in ('PRIMARY KEY', 'p', 'P'):
                    continue # Primary key is handled in fetch_indexes

                if constraint_type in ('TRIGGER', 't', 'T'):
                    # A CONSTRAINT TRIGGER is registered in pg_constraint, but it is a trigger -
                    # there is no ALTER TABLE ... ADD CONSTRAINT syntax for it, and
                    # pg_get_constraintdef returns only the deferrability
                    # ('TRIGGER DEFERRABLE INITIALLY DEFERRED'), not a usable definition.
                    # It is migrated by the triggers migration from pg_get_triggerdef.
                    self.config_parser.print_log_message('DEBUG', f"postgresql_connector: fetch_constraints: Constraint {constraint_name} is a CONSTRAINT TRIGGER - skipped here, it is created by the triggers migration.")
                    continue

                if constraint_type in ('FOREIGN KEY', 'f', 'F'):
                     constraints[order_num] = {
                         'constraint_name': constraint_name,
                         'constraint_type': constraint_type,
                         'constraint_sql': constraint_sql,
                         'constraint_comment': constraint_comment,
                         'referenced_table_schema': constraint_ref_schema,
                         'referenced_table_name': constraint_ref_table,
                     }
                     order_num += 1
                else:
                     constraints[order_num] = {
                         'constraint_name': constraint_name,
                         'constraint_type': constraint_type,
                         'constraint_sql': constraint_sql,
                         'constraint_comment': constraint_comment,
                     }
                     order_num += 1

            cursor.close()
            self.disconnect()
            return constraints
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_constraints: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_mapping_target_indexes(self, schema_name: str, table_name: str):
        query = f"""
            SELECT
                i.relname as indexname,
                pg_get_indexdef(i.oid) as indexdef,
                ix.indisprimary as is_primary_key,
                am.amname as indextype
            FROM pg_class t
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_namespace n ON t.relnamespace = n.oid
            JOIN pg_am am ON i.relam = am.oid
            WHERE n.nspname = '{schema_name}' AND t.relname = '{table_name}'
        """
        indexes = []
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                indexes.append({
                    'index_name': row[0],
                    'index_def': row[1],
                    'is_primary_key': row[2],
                    'index_type': row[3].upper() if row[3] else 'UNKNOWN'
                })
            cursor.close()
            return indexes
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_mapping_target_indexes: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            return []

    def fetch_mapping_target_constraints(self, schema_name: str, table_name: str):
        query = f"""
            SELECT
                c.conname,
                pg_get_constraintdef(c.oid) as condef,
                CASE WHEN contype = 'c' THEN 'CHECK'
                     WHEN contype = 'f' THEN 'FOREIGN KEY'
                     WHEN contype = 'p' THEN 'PRIMARY KEY'
                     WHEN contype = 'u' THEN 'UNIQUE'
                     WHEN contype = 't' THEN 'TRIGGER'
                     WHEN contype = 'x' THEN 'EXCLUSION'
                     ELSE contype::text END as type
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_namespace n ON t.relnamespace = n.oid
            WHERE n.nspname = '{schema_name}' AND t.relname = '{table_name}'
            AND c.contype NOT IN ('n')
        """
        constraints = []
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                constraints.append({
                    'constraint_name': row[0],
                    'constraint_def': row[1],
                    'constraint_type': row[2]
                })
            cursor.close()
            return constraints
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_mapping_target_constraints: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            return []

    def get_indexes_count(self, schema_name: str, table_name: str) -> int:
        query = f"""
            SELECT count(*)
            FROM pg_index i
            JOIN pg_class t ON t.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = '{schema_name}' AND t.relname = '{table_name}'
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: get_indexes_count: Error: {e}")
            return -1

    def get_constraints_count(self, schema_name: str, table_name: str) -> int:
        query = f"""
            SELECT count(*)
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_namespace n ON t.relnamespace = n.oid
            WHERE n.nspname = '{schema_name}' AND t.relname = '{table_name}'
            AND c.contype NOT IN ('n')
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: get_constraints_count: Error: {e}")
            return -1

    def get_schema_indexes_count(self, schema_name: str) -> int:
        query = f"""
            SELECT count(*)
            FROM pg_index i
            JOIN pg_class t ON t.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = '{schema_name}'
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            cursor.close()
            self.disconnect()
            return count
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: get_schema_indexes_count: Error: {e}")
            return -1

    def get_schema_constraints_count(self, schema_name: str) -> int:
        query = f"""
            SELECT count(*)
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_namespace n ON t.relnamespace = n.oid
            WHERE n.nspname = '{schema_name}'
            AND c.contype NOT IN ('n')
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            cursor.close()
            self.disconnect()
            return count
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: get_schema_constraints_count: Error: {e}")
            return -1

    def fetch_mapping_target_sequences(self, schema_name: str, table_name: str):
        self.config_parser.print_log_message('DEBUG', f"postgresql_connector: fetch_mapping_target_sequences: Fetching sequences for {schema_name}.{table_name}")
        query_general = f"""
            SELECT 
                a.attname as column_name,
                s.relname as sequence_name,
                n.nspname as sequence_schema_name,
                a.attidentity != '' as is_identity,
                pg_get_expr(ad.adbin, ad.adrelid) as default_expr
            FROM pg_class t
            JOIN pg_namespace tn ON t.relnamespace = tn.oid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum > 0 AND NOT a.attisdropped
            LEFT JOIN pg_attrdef ad ON ad.adrelid = t.oid AND ad.adnum = a.attnum
            LEFT JOIN pg_depend d ON d.refobjid = t.oid AND d.refobjsubid = a.attnum AND d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass
            LEFT JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S'
            LEFT JOIN pg_namespace n ON s.relnamespace = n.oid
            WHERE tn.nspname = '{schema_name}' AND t.relname = '{table_name}'
            AND (
                s.oid IS NOT NULL 
                OR (ad.adbin IS NOT NULL AND pg_get_expr(ad.adbin, ad.adrelid) LIKE '%nextval(%')
            )
        """

        sequences = []
        try:
            cursor = self.connection.cursor()
            
            # Fetch default and identity sequences
            cursor.execute(query_general)
            for row in cursor.fetchall():
                col_name = row[0]
                seq_name = row[1]
                seq_schema = row[2]
                is_identity = row[3]
                default_expr = row[4]
                
                used_in_default = False
                if default_expr and 'nextval(' in default_expr:
                    used_in_default = True
                    if not seq_name:
                        import re
                        match = re.search(r"nextval\('([^']+)'", default_expr)
                        if match:
                            full_name = match.group(1).replace('"', '').replace("'::regclass", "")
                            if '.' in full_name:
                                seq_schema, seq_name = full_name.split('.', 1)
                            else:
                                seq_schema = schema_name
                                seq_name = full_name
                
                if seq_name:
                    sequences.append({
                        'sequence_schema_name': seq_schema,
                        'sequence_name': seq_name,
                        'used_in_default': used_in_default,
                        'used_in_identity': is_identity,
                        'used_in_trigger': False,
                        'trigger_name': None,
                        'column_name': col_name
                    })

            # Fetch triggered sequences
            query_triggers = f"""
                SELECT tg.tgname, pg_get_triggerdef(tg.oid) as trigger_def
                FROM pg_trigger tg
                JOIN pg_class t ON tg.tgrelid = t.oid
                JOIN pg_namespace n ON t.relnamespace = n.oid
                WHERE n.nspname = '{schema_name}' AND t.relname = '{table_name}' AND tg.tgname NOT LIKE 'RI_ConstraintTrigger%'
            """
            cursor.execute(query_triggers)
            for row in cursor.fetchall():
                tgname = row[0]
                tgdef = row[1]
                if tgdef and 'nextval(' in tgdef:
                    import re
                    matches = re.findall(r"nextval\('([^']+)'", tgdef)
                    for match in matches:
                        full_name = match.replace('"', '').replace("'::regclass", "")
                        if '.' in full_name:
                            seq_schema, seq_name = full_name.split('.', 1)
                        else:
                            seq_schema = schema_name
                            seq_name = full_name
                        sequences.append({
                            'sequence_schema_name': seq_schema,
                            'sequence_name': seq_name,
                            'used_in_default': False,
                            'used_in_identity': False,
                            'used_in_trigger': True,
                            'trigger_name': tgname,
                            'column_name': None
                        })

            cursor.close()
            
            # Deduplicate
            unique_seqs = {}
            for s in sequences:
                key = (s['sequence_schema_name'], s['sequence_name'], s['column_name'], s['trigger_name'])
                unique_seqs[key] = s
            return list(unique_seqs.values())
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_mapping_target_sequences: Error fetching sequences for {schema_name}.{table_name}")
            self.config_parser.print_log_message('ERROR', e)
            return []
    def convert_constraint_sql_case(self, sql_expr):
        if not sql_expr:
            return ''
        # Pattern to match single-quoted string literals, allowing for doubled single quotes as escapes
        pattern = r"('[^']*(?:''[^']*)*')"
        parts = re.split(pattern, sql_expr)
        for i in range(len(parts)):
            if i % 2 == 0:  # Outside single quotes
                parts[i] = self.config_parser.convert_names_case(parts[i])
        return ''.join(parts)

    def get_create_constraint_sql(self, settings):
        create_constraint_query = ''
        source_db_type = settings['source_db_type']
        # target_schema_name = self.config_parser.convert_names_case(settings['target_schema_name'])
        target_schema_name = settings['target_schema_name'] ## target schema is used as it is defined in config, not converted to upper/lower case
        target_table_name = self.config_parser.convert_names_case(settings['target_table_name'])
        target_columns = settings['target_columns']
        constraint_name = self.config_parser.convert_names_case(settings['constraint_name'])
        constraint_type = settings['constraint_type']
        constraint_owner = self.config_parser.convert_names_case(settings['constraint_owner'])
        constraint_columns = self.config_parser.convert_names_case(settings['constraint_columns'])
        ## The schema the REFERENCES clause names. 'referenced_table_schema' is the schema of
        ## the SOURCE and must never be written into a statement for the target - it was read
        ## into a local variable here and then not used at all, so every foreign key was built
        ## as if the referenced table lay in the schema of the referencing one. The planner
        ## decides where the referenced table lands and passes it; without it the old
        ## assumption is kept, which is right for the one-schema migrations it was written for.
        target_referenced_table_schema = settings.get('target_referenced_table_schema') or target_schema_name
        referenced_table_name = self.config_parser.convert_names_case(settings['referenced_table_name'])
        referenced_columns = self.config_parser.convert_names_case(settings['referenced_columns'])
        delete_rule = settings['delete_rule'] if 'delete_rule' in settings else 'NO ACTION'
        update_rule = settings['update_rule'] if 'update_rule' in settings else 'NO ACTION'
        constraint_comment = settings['constraint_comment']
        constraint_sql = self.convert_constraint_sql_case(settings['constraint_sql']) if settings.get('constraint_sql') else ''
        constraint_status = settings['constraint_status'] if 'constraint_status' in settings else 'ENABLED'

        # Split constraint_columns by comma, clean up quotes, convert case, and re-quote
        if constraint_columns:
            column_names = []
            for col in constraint_columns.split(','):
                col = col.strip()
                # Remove backticks, single quotes, and double quotes
                col = col.strip('`').strip("'").strip('"')
                # Convert case using config parser function
                col = self.config_parser.convert_names_case(col)
                # Add to list with double quotes
                column_names.append(f'"{col}"')
            # Join back with comma
            constraint_columns = ', '.join(column_names)

        # Split referenced_columns by comma, clean up quotes, convert case, and re-quote
        if referenced_columns:
            ref_column_names = []
            for col in referenced_columns.split(','):
                col = col.strip()
                # Remove backticks, single quotes, and double quotes
                col = col.strip('`').strip("'").strip('"')
                # Convert case using config parser function
                col = self.config_parser.convert_names_case(col)
                # Add to list with double quotes
                ref_column_names.append(f'"{col}"')
            # Join back with comma
            referenced_columns = ', '.join(ref_column_names)

        if source_db_type != 'postgresql':
            if constraint_type == 'FOREIGN KEY':
                create_constraint_query = (
                    f'ALTER TABLE "{target_schema_name}"."{target_table_name}" ADD CONSTRAINT "{constraint_name}_tab_{target_table_name}" '
                    f'FOREIGN KEY ({constraint_columns}) REFERENCES "{target_referenced_table_schema}"."{referenced_table_name}" ({referenced_columns})'
                )
                delete_action = self.normalize_referential_action(delete_rule)
                if delete_action:
                    create_constraint_query += f" ON DELETE {delete_action}"
                update_action = self.normalize_referential_action(update_rule)
                if update_action:
                    create_constraint_query += f" ON UPDATE {update_action}"
                # A comment of the constraint is not part of ALTER TABLE ... ADD CONSTRAINT in
                # PostgreSQL, it is set by the comments migration with COMMENT ON CONSTRAINT.
            elif constraint_type == 'CHECK':
                # Replace column names in constraint_sql with double-quoted names using precise match
                if constraint_sql and target_columns:
                    for col_info in target_columns.values():
                        col_name = col_info['column_name']
                        # Use word boundary and negative lookbehind/lookahead for precise match without double-quoting already-quoted identifiers
                        pattern = r'(?<!["`\'])\b{}\b(?!["`\'])'.format(re.escape(col_name))
                        constraint_sql = re.sub(pattern, f'"{col_name}"', constraint_sql)
                    # Clean up any accidental consecutive double quotes
                    constraint_sql = re.sub(r'""+', '"', constraint_sql)
                create_constraint_query = f"""ALTER TABLE "{target_schema_name}"."{target_table_name}" ADD CONSTRAINT "{constraint_name}_tab_{target_table_name}" CHECK ({constraint_sql})"""
        else:
            create_constraint_query = f"""ALTER TABLE "{target_schema_name}"."{target_table_name}" ADD CONSTRAINT "{constraint_name}" {constraint_sql}"""
        return create_constraint_query

    ## NO ACTION is the PostgreSQL default and is left out of the generated SQL.
    ## Anything not recognized is ignored the same way, so a source connector
    ## reporting a rule in its own notation cannot produce invalid SQL.
    SUPPORTED_REFERENTIAL_ACTIONS = ('CASCADE', 'SET NULL', 'SET DEFAULT', 'RESTRICT')

    def normalize_referential_action(self, rule):
        if not rule:
            return ''
        action = ' '.join(str(rule).upper().replace('_', ' ').split())
        return action if action in self.SUPPORTED_REFERENTIAL_ACTIONS else ''

    def get_aliases(self, settings):
        return {}

    def fetch_triggers(self, table_id: int, table_schema: str, table_name: str):
        triggers = {}
        order_num = 1
        query = f"""
            SELECT
                t.oid,
                t.tgname,
                pg_get_triggerdef(t.oid) as definition,
                t.tgtype::text,
                t.tgisinternal,
                t.tgenabled,
                obj_description(t.oid, 'pg_trigger') as comment,
                t.tgtype
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.oid = {table_id}
            AND NOT t.tgisinternal
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                # Detect event and timing from tgtype (bitmask) or just rely on definition
                # tgtype definition:
                # 2 = BEFORE
                # 0 = AFTER (default?) -> actually checking bits
                # It is easier to rely on pg_get_triggerdef for the full SQL.
                # But planner expects decomposed values: event, new, old, etc?
                # looking at planner.py:
                # trigger_details['event']
                # trigger_details['new']
                # trigger_details['old']
                # trigger_details['sql']

                # For PG, we simply put the full definition in 'sql'.
                # The other fields might be purely informational for logging or other connectors.

                triggers[order_num] = {
                    'id': row[0],
                    'name': row[1],
                    'sql': row[2],
                    'event': 'See SQL', # simplified
                    'new': '',
                    'old': '',
                    'comment': row[6]
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return triggers
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_triggers: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def execute_query(self, query: str, params=None):
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)

    def copy_from_file(self, sql: str, file_path: str):
        self.config_parser.print_log_message('DEBUG3', f"postgresql_connector: copy_from_file: sql: {sql}, file_path: {file_path}")
        with open(file_path, 'r') as file:
            with self.connection.cursor() as cursor:
                try:
                    cursor.copy_expert(sql, file)
                    for notice in cursor.connection.notices:
                        self.config_parser.print_log_message('INFO', notice)
                    cursor.connection.commit()
                except Exception as e:
                    for notice in cursor.connection.notices:
                        self.config_parser.print_log_message('INFO', notice)
                    self.config_parser.print_log_message('ERROR', f"postgresql_connector: copy_from_file: file_path: {file_path}: Error executing copy_expert: {e}")
                    raise

    def execute_sql_script(self, script_path: str):
        with open(script_path, 'r') as file:
            script = file.read()

        with self.connection.cursor() as cursor:
            try:
                cursor.execute(script)
                for notice in cursor.connection.notices:
                    self.config_parser.print_log_message('INFO', notice)
            except Exception as e:
                for notice in cursor.connection.notices:
                    self.config_parser.print_log_message('INFO', notice)
                self.config_parser.print_log_message('ERROR', f"postgresql_connector: execute_sql_script: Error executing script: {e}")
                raise

    def begin_transaction(self):
        self.connection.autocommit = False

    def commit_transaction(self):
        self.connection.commit()
        self.connection.autocommit = True

    def rollback_transaction(self):
        self.connection.rollback()

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
            # primary_key_columns = settings['primary_key_columns']
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
                self.config_parser.print_log_message('INFO', f"postgresql_connector: migrate_table: Worker {worker_id}: Table {source_table_name} is empty - skipping data migration.")
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

                    part_name = 'migrate_table in batches using cursor'
                    self.config_parser.print_log_message('INFO', f"postgresql_connector: migrate_table: Worker {worker_id}: Source table {source_table_name}: {source_table_rows_limited} rows / Target table {target_table_name}: {target_table_rows} rows - starting data migration.")

                    select_columns_list = []
                    orderby_columns_list = []
                    insert_columns_list = []

                    def is_generated_column(col):
                        return col.get('is_generated_virtual') == 'YES' or col.get('is_generated_stored') == 'YES'

                    # A generated column is computed by the target and rejects an inserted
                    # value ("cannot insert a non-DEFAULT value into column ..."), so it is
                    # part of neither the SELECT nor the INSERT.
                    migrated_source_columns = {
                        order_num: col for order_num, col in source_columns.items()
                        if not is_generated_column(col) and not is_generated_column(target_columns.get(order_num, {}))
                    }
                    skipped_columns = [col['column_name'] for order_num, col in source_columns.items()
                                       if order_num not in migrated_source_columns]
                    if skipped_columns:
                        self.config_parser.print_log_message('DEBUG', f"postgresql_connector: migrate_table: Worker {worker_id}: Table {source_schema_name}.{source_table_name}: generated columns are computed by the target and excluded from the data migration: {', '.join(skipped_columns)}")

                    for order_num, col in migrated_source_columns.items():
                        self.config_parser.print_log_message('DEBUG2',
                                                            f"Worker {worker_id}: Table {source_schema_name}.{source_table_name}: Processing column {col['column_name']} ({order_num}) with data type {col['data_type']}")

                        if col['data_type'].lower() == 'datetime':
                            select_columns_list.append(f"TO_CHAR({col['column_name']}, '%Y-%m-%d %H:%M:%S') as {col['column_name']}")
                        #     select_columns_list.append(f"ST_asText(`{col['column_name']}`) as `{col['column_name']}`")
                        # elif col['data_type'].lower() == 'set':
                        #     select_columns_list.append(f"cast(`{col['column_name']}` as char(4000)) as `{col['column_name']}`")
                        else:
                            select_columns_list.append(f'''"{col['column_name']}"''')

                        insert_columns_list.append(f'''"{self.config_parser.convert_names_case(col['column_name'])}"''')
                        orderby_columns_list.append(f'''"{col['column_name']}"''')

                    select_columns = ', '.join(select_columns_list)
                    orderby_columns = ', '.join(orderby_columns_list)
                    insert_columns = ', '.join(insert_columns_list)

                    if resume_after_crash and not drop_unfinished_tables:
                        chunk_number = self.config_parser.get_total_chunks(target_table_rows, chunk_size)
                        self.config_parser.print_log_message('DEBUG', f"postgresql_connector: migrate_table: Worker {worker_id}: Resuming migration for table {source_schema_name}.{source_table_name} from chunk {chunk_number} with data chunk size {chunk_size}.")
                        chunk_offset = target_table_rows
                    else:
                        chunk_offset = (chunk_number - 1) * chunk_size

                    chunk_start_row_number = chunk_offset + 1
                    chunk_end_row_number = chunk_offset + chunk_size

                    self.config_parser.print_log_message('DEBUG', f"postgresql_connector: migrate_table: Worker {worker_id}: Migrating table {source_schema_name}.{source_table_name}: chunk {chunk_number}, data chunk size {chunk_size}, batch size {batch_size}, chunk offset {chunk_offset}, chunk end row number {chunk_end_row_number}, source table rows {source_table_rows_limited}")
                    order_by_clause = ''

                    query = f'''SELECT {select_columns} FROM "{source_schema_name}"."{source_table_name}" '''
                    if migration_limitation:
                        query += f" WHERE {migration_limitation}"
                    primary_key_columns = migrator_tables.select_primary_key({'source_schema_name': source_schema_name, 'source_table_name': source_table_name})
                    self.config_parser.print_log_message('DEBUG2', f"postgresql_connector: migrate_table: Worker {worker_id}: Primary key columns for {source_schema_name}.{source_table_name}: {primary_key_columns}")
                    if primary_key_columns:
                        orderby_columns = primary_key_columns
                    order_by_clause = f""" ORDER BY {orderby_columns}"""
                    query += order_by_clause + f" LIMIT {chunk_size} OFFSET {chunk_offset}"

                    self.config_parser.print_log_message('DEBUG', f"postgresql_connector: migrate_table: Worker {worker_id}: Fetching data with cursor using query: {query}")

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
                        self.config_parser.print_log_message('DEBUG', f"postgresql_connector: migrate_table: Worker {worker_id}: Fetched {len(records)} rows (batch {batch_number}) from source table {source_table_name}.")

                        transforming_start_time = time.time()
                        records = [
                            {column['column_name']: value for column, value in zip(migrated_source_columns.values(), record)}
                            for record in records
                        ]
                        for record in records:
                            for order_num, column in migrated_source_columns.items():
                                column_name = column['column_name']
                                column_type = column['data_type']
                                if column_type in ['bytea'] and record[column_name] is not None:
                                    record[column_name] = record[column_name].tobytes()

                        # Insert batch into target table
                        self.config_parser.print_log_message('DEBUG', f"postgresql_connector: migrate_table: Worker {worker_id}: Starting insert of {len(records)} rows from source table {source_table_name}")
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
                    self.config_parser.print_log_message('INFO', f"postgresql_connector: migrate_table: Worker {worker_id}: Target table {target_schema_name}.{target_table_name} has {target_table_rows} rows")

                    shortest_batch_seconds = min(batch_durations) if batch_durations else 0
                    longest_batch_seconds = max(batch_durations) if batch_durations else 0
                    average_batch_seconds = sum(batch_durations) / len(batch_durations) if batch_durations else 0
                    self.config_parser.print_log_message('INFO', f"postgresql_connector: migrate_table: Worker {worker_id}: Migrated {total_inserted_rows} rows from {source_table_name} to {target_schema_name}.{target_table_name} in {batch_number} batches: "
                                                            f"Shortest batch: {shortest_batch_seconds:.2f} seconds, "
                                                            f"Longest batch: {longest_batch_seconds:.2f} seconds, "
                                                            f"Average batch: {average_batch_seconds:.2f} seconds")

                    cursor.close()

                else:
                    self.config_parser.print_log_message('INFO', f"postgresql_connector: migrate_table: Worker {worker_id}: Target table {target_table_name} has {target_table_rows} rows and data_conflict_action is '{data_conflict_action}'. Skipping data migration.")

                migration_stats = {
                    'rows_migrated': total_inserted_rows,
                    'chunk_number': chunk_number,
                    'total_chunks': total_chunks,
                    'source_table_rows_all': source_table_rows_all,

                    'source_table_rows_limited': source_table_rows_limited,
                    'target_table_rows': target_table_rows,
                    'finished': False,
                }

                self.config_parser.print_log_message('DEBUG', f"postgresql_connector: migrate_table: Worker {worker_id}: Migration stats: {migration_stats}")
                if source_table_rows_limited <= target_table_rows or chunk_number >= total_chunks:
                    self.config_parser.print_log_message('DEBUG3', f"postgresql_connector: migrate_table: Worker {worker_id}: Setting migration status to finished for table {source_table_name} (chunk {chunk_number}/{total_chunks})")
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
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: migrate_table: Worker {worker_id}: Error during {part_name} -> {e}")
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: migrate_table: Worker {worker_id}: Full stack trace: {traceback.format_exc()}")
            raise e

    @staticmethod
    def _coerce_boolean_value(value):
        """
        Coerce a source value into a Python bool for insertion into a PostgreSQL
        BOOLEAN target column.

        Several source connectors map narrow numeric types to BOOLEAN (e.g. Oracle
        NUMBER(1,0)). The raw source value is then a number/string that PostgreSQL
        will not implicitly assign to a boolean column, so we normalize it here.
        Any non-zero number (or common truthy text) becomes True, zero/empty/common
        falsy text becomes False, and None is preserved as SQL NULL.
        """
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float, Decimal)):
            return value != 0
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in ('1', 't', 'true', 'y', 'yes'):
                return True
            if normalized in ('0', 'f', 'false', 'n', 'no', ''):
                return False
            # Fallback: any other non-empty string is treated as truthy.
            return True
        return bool(value)

    def sorted_column_keys(self, columns):
        """
        Column dictionaries are keyed by the source column position, but the protocol tables
        store them as JSON, so a position the source could not provide (e.g. an Oracle
        invisible column without COLUMN_ID) arrives as the string "null". Order numerically
        where possible and keep such keys last instead of raising ValueError.
        """
        def sort_key(key):
            try:
                return (0, int(key), '')
            except (TypeError, ValueError):
                return (1, 0, str(key))

        return sorted(columns.keys(), key=sort_key)

    def insert_batch(self, settings):
        target_schema_name = settings['target_schema_name']
        target_table_name = settings['target_table_name']
        columns = settings['target_columns']
        data = settings['data']
        worker_id = settings['worker_id']
        insert_columns = settings.get('insert_columns', None)

        insert_columns_provided = settings.get('insert_columns', None)
        insert_values = settings.get('insert_values', None)

        # Generated columns are computed by PostgreSQL and reject inserted values, so they are
        # not part of the INSERT - the source connectors omit them from the payload as well.
        insertable_column_keys = [
            col for col in self.sorted_column_keys(columns)
            if columns[col].get('is_generated_virtual') != 'YES' and columns[col].get('is_generated_stored') != 'YES'
        ]

        if not insert_columns:
            insert_columns = [f'"{columns[col]["column_name"]}"' for col in insertable_column_keys]

        if not insert_values:
            if isinstance(insert_columns, str) and insert_columns.strip():
                num_cols = len([c for c in insert_columns.split(',') if c.strip()])
                insert_values = ', '.join(['%s'] * num_cols)
            elif isinstance(insert_columns, list) and insert_columns:
                insert_values = ', '.join(['%s'] * len(insert_columns))
            else:
                insert_values = ', '.join(['%s'] * len(insertable_column_keys))

        if isinstance(insert_columns, list):
            insert_columns = ', '.join(insert_columns)

        inserted_rows = 0
        self.config_parser.print_log_message('DEBUG2', f"postgresql_connector: insert_batch: Worker {worker_id}: insert_batch into {target_schema_name}.{target_table_name} with {len(data)} rows, columns: {insert_columns}, data type: {type(data)}")
        try:
            # Ensure data is a list of tuples
            self.config_parser.print_log_message('DEBUG2', f"postgresql_connector: insert_batch: Worker {worker_id}: Started insert batch into {target_schema_name}.{target_table_name} with {len(data)} rows")
            if isinstance(data, list) and all(isinstance(item, dict) for item in data):

                # Match extract_keys order exactly to insert_columns
                if isinstance(insert_columns, str) and insert_columns.strip():
                    extract_keys = [c.strip().strip('"') for c in insert_columns.split(',')]
                elif insert_columns_provided and len(data) > 0:
                    extract_keys = list(data[0].keys())
                else:
                    extract_keys = [columns[col]['column_name'] for col in insertable_column_keys]

                # Target columns mapped to BOOLEAN (e.g. Oracle NUMBER(1,0)) need their
                # source numeric/text values coerced to Python bool so PostgreSQL accepts
                # them - a bare integer (e.g. 2) cannot be implicitly assigned to boolean.
                boolean_columns = {
                    col_info['column_name']
                    for col_info in columns.values()
                    if str(col_info.get('data_type', '')).strip().lower() in ('boolean', 'bool')
                }

                # Target columns of type JSON or JSONB need Python objects (dict, list, bool, etc.)
                # serialized to JSON strings so psycopg2 and PostgreSQL accept them properly.
                json_columns = {
                    col_info['column_name']
                    for col_info in columns.values()
                    if str(col_info.get('data_type', '')).strip().lower() in ('json', 'jsonb')
                }

                columns_by_name = {
                    col_info['column_name']: col_info
                    for col_info in columns.values()
                    if 'column_name' in col_info
                }

                formatted_data = []
                for item in data:
                    row = []
                    for col_name in extract_keys:
                        value = item.get(col_name)
                        if value is None and col_name not in item:
                            for k, v in item.items():
                                if str(k).lower() == str(col_name).lower():
                                    value = v
                                    break

                        col_info = columns_by_name.get(col_name)
                        if not col_info:
                            for c_k, c_v in columns_by_name.items():
                                if str(c_k).lower() == str(col_name).lower():
                                    col_info = c_v
                                    break

                        if value is None and col_info and col_info.get('is_nullable') == 'NO':
                            default_val = col_info.get('column_default_value')
                            if default_val is not None and str(default_val).strip() != '' and not str(default_val).lower().startswith('nextval'):
                                value = default_val
                            else:
                                data_type_lower = str(col_info.get('data_type', '')).strip().lower()
                                if data_type_lower in ('json', 'jsonb'):
                                    value = 'null'
                                elif self.is_string_type(data_type_lower) or any(t in data_type_lower for t in ('text', 'char', 'varchar', 'string')):
                                    value = ''
                                elif self.is_numeric_type(data_type_lower) or any(t in data_type_lower for t in ('int', 'numeric', 'decimal', 'float', 'double', 'real')):
                                    value = 0
                                elif data_type_lower in ('boolean', 'bool'):
                                    value = False

                        if col_name in boolean_columns:
                            value = self._coerce_boolean_value(value)
                        elif col_name in json_columns:
                            if value is None:
                                if col_info and col_info.get('is_nullable') == 'NO':
                                    value = 'null'
                            elif isinstance(value, str):
                                try:
                                    json.loads(value)
                                except Exception:
                                    value = json.dumps(value)
                            else:
                                value = json.dumps(value)
                        elif value is not None and (type(value).__name__ == 'array' or hasattr(value, 'tolist')):
                            value = json.dumps(value.tolist()) if hasattr(value, 'tolist') else str(value)
                        row.append(value)
                    formatted_data.append(tuple(row))
                self.config_parser.print_log_message('DEBUG3', f"postgresql_connector: insert_batch: INSERT COLUMNS: {insert_columns}")
                self.config_parser.print_log_message('DEBUG3', f"postgresql_connector: insert_batch: EXTRACT KEYS: {extract_keys}")
                
                formatted_data_str = str(formatted_data[0])
                if len(formatted_data_str) > 500:
                    formatted_data_str = formatted_data_str[:500] + "... (truncated)"
                self.config_parser.print_log_message('DEBUG3', f"postgresql_connector: insert_batch: FORMATTED DATA[0]: {formatted_data_str}")
                
                data = formatted_data
            else:
                self.config_parser.print_log_message('ERROR', f"postgresql_connector: insert_batch: Worker {worker_id}: Data for insert_batch must be a list of dictionaries, got {type(data)}")
                return 0

            self.config_parser.print_log_message('DEBUG2', f"postgresql_connector: insert_batch: Worker {worker_id}: insert_batch [2] into {target_schema_name}.{target_table_name} with {len(data)} rows, columns: |{insert_columns}| data type: {type(data)}")

            conflict_action = settings.get('data_conflict_action')
            conflict_clause = ""
            if conflict_action == 'merge_keep_target':
                conflict_clause = " ON CONFLICT DO NOTHING"
            elif conflict_action == 'merge_keep_source':
                primary_key_columns = settings.get('primary_key_columns')
                if primary_key_columns:
                    col_names = [c.strip().strip('"') for c in insert_columns.split(',')]
                    pk_names = [c.strip().strip('"') for c in primary_key_columns.split(',')]
                    update_parts = []
                    for col in col_names:
                        if col not in pk_names:
                            update_parts.append(f'"{col}" = EXCLUDED."{col}"')
                    
                    if update_parts:
                        where_parts = []
                        for col in col_names:
                            if col not in pk_names:
                                where_parts.append(f'"{target_table_name}"."{col}" IS DISTINCT FROM EXCLUDED."{col}"')
                        
                        where_clause = ""
                        if where_parts:
                            where_clause = " WHERE " + " OR ".join(where_parts)
                        conflict_clause = f" ON CONFLICT ({primary_key_columns}) DO UPDATE SET " + ", ".join(update_parts) + where_clause
                    else:
                        conflict_clause = f" ON CONFLICT ({primary_key_columns}) DO NOTHING"
                else:
                    self.config_parser.print_log_message('ERROR', f"postgresql_connector: insert_batch: Worker {worker_id}: Cannot use 'merge_keep_source' for table {target_table_name} without a primary key.")
                    raise ValueError(f"Table {target_table_name} lacks a primary key required for 'merge_keep_source'.")

            insert_query_str = f"""INSERT INTO "{target_schema_name}"."{target_table_name}" ({insert_columns}) VALUES ({insert_values}){conflict_clause}"""
            
            with self.connection.cursor() as cursor:
                insert_query = sql.SQL(insert_query_str)
                self.config_parser.print_log_message('DEBUG3', f"postgresql_connector: insert_batch: Worker {worker_id}: Insert query: {insert_query_str}")
                self.connection.autocommit = False
                try:
                    ## the session settings were applied when the connection was opened and
                    ## hold for the whole session - repeating them for every batch only cost
                    ## a round trip
                    self.config_parser.print_log_message('DEBUG3', f"postgresql_connector: insert_batch: Worker {worker_id}: Starting psycopg2.extras.execute_batch into {target_table_name} with {len(data)} rows")

                    psycopg2.extras.execute_batch(cursor, insert_query, data)
                    inserted_rows = len(data)
                    self.connection.commit()
                except Exception as e:
                    e_str = str(e)
                    if len(e_str) > 1000:
                        e_str = e_str[:1000] + "... (error message truncated)"
                    self.config_parser.print_log_message('ERROR', f"postgresql_connector: insert_batch: Worker {worker_id}: Error inserting batch data into {target_table_name}: {e_str}")
                    self.config_parser.print_log_message('ERROR', f"postgresql_connector: insert_batch: Worker {worker_id}: Trying to insert row by row.")
                    self.connection.rollback()
                    for row in data:
                        try:
                            cursor.execute(insert_query, row)
                            inserted_rows += 1
                            self.connection.commit()
                        except Exception as e:
                            self.connection.rollback()
                            row_str = str(row)
                            if len(row_str) > 500:
                                row_str = row_str[:500] + "... (truncated)"
                            self.config_parser.print_log_message('ERROR', f"postgresql_connector: insert_batch: Worker {worker_id}: Error inserting row into {target_table_name}: {row_str}")
                            
                            e_str = str(e)
                            if len(e_str) > 1000:
                                e_str = e_str[:1000] + "... (error message truncated)"
                            self.config_parser.print_log_message('ERROR', e_str)

                self.connection.autocommit = True

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: insert_batch: Worker {worker_id}: Error before inserting batch data: {e}")
            raise

        return inserted_rows

    def fetch_funcproc_names(self, schema: str):
        funcprocs = {}
        order_num = 1
        query = f"""
            SELECT
                p.oid,
                p.proname,
                pg_get_function_identity_arguments(p.oid) as arguments,
                obj_description(p.oid, 'pg_proc') as comment,
                p.prokind
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = '{schema}'
              AND p.prokind IN ('f', 'p', 'a')
              AND NOT EXISTS (SELECT 1 FROM pg_depend dep
                              WHERE dep.objid = p.oid AND dep.deptype = 'e')
            ORDER BY (p.prokind = 'a'), p.proname
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                # Aggregates are ordered last - they reference their state transition and
                # final functions, which have to exist first.
                func_type = {'p': 'PROCEDURE', 'a': 'AGGREGATE'}.get(row[4], 'FUNCTION')
                funcprocs[order_num] = {
                    'id': row[0],
                    'name': row[1],
                    'header': f"{row[1]}({row[2]})",
                    # The identity arguments are needed to address the object in
                    # COMMENT ON FUNCTION / PROCEDURE / AGGREGATE - the name alone is
                    # ambiguous for overloaded routines.
                    'arguments': row[2],
                    'comment': row[3],
                    'type': func_type
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return funcprocs
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_funcproc_names: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_funcproc_code(self, funcproc_id: int):
        query = f"SELECT pg_get_functiondef({funcproc_id})"
        try:
            self.connect()
            cursor = self.connection.cursor()
            # pg_get_functiondef() does not work for an aggregate ("... is an aggregate
            # function") - an aggregate is described by pg_aggregate, not by a body.
            cursor.execute("SELECT prokind FROM pg_catalog.pg_proc WHERE oid = %s", (funcproc_id,))
            row = cursor.fetchone()
            if row and row[0] == 'a':
                cursor.close()
                self.disconnect()
                return self.fetch_aggregate_code(funcproc_id)

            cursor.execute(query)
            code = cursor.fetchone()[0]
            cursor.close()
            self.disconnect()
            return code
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_funcproc_code: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def fetch_aggregate_code(self, aggregate_id: int):
        """
        Build the CREATE AGGREGATE statement of a user defined aggregate from pg_aggregate.

        The search_path of this short lived connection is set to pg_catalog, so that
        format_type() and pg_get_function_identity_arguments() qualify every user defined
        type and function with its schema while leaving the built-in ones bare. The schema
        replacement of convert_funcproc_code() then maps them to the target schema.
        """
        query = ''
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute("SET search_path TO pg_catalog")

            cursor.execute("""
                SELECT attname FROM pg_catalog.pg_attribute
                WHERE attrelid = 'pg_catalog.pg_aggregate'::regclass AND attnum > 0 AND NOT attisdropped
            """)
            available_columns = {row[0] for row in cursor.fetchall()}
            # aggfinalmodify / aggmfinalmodify exist since PostgreSQL 11
            final_modify = 'a.aggfinalmodify' if 'aggfinalmodify' in available_columns else "NULL::\"char\""
            mfinal_modify = 'a.aggmfinalmodify' if 'aggmfinalmodify' in available_columns else "NULL::\"char\""

            def function_name(column):
                return f"""(SELECT quote_ident(fn.nspname)||'.'||quote_ident(fp.proname)
                            FROM pg_catalog.pg_proc fp
                            JOIN pg_catalog.pg_namespace fn ON fn.oid = fp.pronamespace
                            WHERE fp.oid = nullif({column}, 0))"""

            query = f"""
                SELECT
                    quote_ident(n.nspname) as aggregate_schema,
                    quote_ident(p.proname) as aggregate_name,
                    pg_get_function_identity_arguments(p.oid) as arguments,
                    a.aggkind,
                    {function_name('a.aggtransfn')} as sfunc,
                    format_type(a.aggtranstype, NULL) as stype,
                    nullif(a.aggtransspace, 0) as sspace,
                    {function_name('a.aggfinalfn')} as finalfunc,
                    a.aggfinalextra,
                    {final_modify} as finalmodify,
                    {function_name('a.aggcombinefn')} as combinefunc,
                    {function_name('a.aggserialfn')} as serialfunc,
                    {function_name('a.aggdeserialfn')} as deserialfunc,
                    a.agginitval,
                    {function_name('a.aggmtransfn')} as msfunc,
                    {function_name('a.aggminvtransfn')} as minvfunc,
                    CASE WHEN a.aggmtranstype <> 0 THEN format_type(a.aggmtranstype, NULL) END as mstype,
                    nullif(a.aggmtransspace, 0) as msspace,
                    {function_name('a.aggmfinalfn')} as mfinalfunc,
                    a.aggmfinalextra,
                    {mfinal_modify} as mfinalmodify,
                    a.aggminitval,
                    (SELECT oprname FROM pg_catalog.pg_operator o WHERE o.oid = nullif(a.aggsortop, 0)) as sortop,
                    p.proparallel
                FROM pg_catalog.pg_aggregate a
                JOIN pg_catalog.pg_proc p ON p.oid = a.aggfnoid
                JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                WHERE p.oid = %s
            """
            cursor.execute(query, (aggregate_id,))
            row = cursor.fetchone()
            cursor.close()
            self.disconnect()
            if not row:
                self.config_parser.print_log_message('WARNING', f"postgresql_connector: fetch_aggregate_code: Aggregate with oid {aggregate_id} not found in pg_aggregate.")
                return ''
            return self.get_create_aggregate_sql(row)
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_aggregate_code: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_aggregate_sql(self, aggregate_row):
        """ Assemble CREATE AGGREGATE from a row delivered by fetch_aggregate_code(). """
        (aggregate_schema, aggregate_name, arguments, aggkind, sfunc, stype, sspace,
         finalfunc, finalextra, finalmodify, combinefunc, serialfunc, deserialfunc,
         initcond, msfunc, minvfunc, mstype, msspace, mfinalfunc, mfinalextra,
         mfinalmodify, minitcond, sortop, proparallel) = aggregate_row

        modify_keywords = {'r': 'READ_ONLY', 's': 'SHAREABLE', 'w': 'READ_WRITE'}
        parallel_keywords = {'s': 'SAFE', 'r': 'RESTRICTED', 'u': 'UNSAFE'}

        def quoted(value):
            return "'" + str(value).replace("'", "''") + "'"

        options = [f"SFUNC = {sfunc}", f"STYPE = {stype}"]
        if sspace:
            options.append(f"SSPACE = {sspace}")
        if finalfunc:
            options.append(f"FINALFUNC = {finalfunc}")
            if finalextra:
                options.append("FINALFUNC_EXTRA")
            if finalmodify in modify_keywords:
                options.append(f"FINALFUNC_MODIFY = {modify_keywords[finalmodify]}")
        if combinefunc:
            options.append(f"COMBINEFUNC = {combinefunc}")
        if serialfunc:
            options.append(f"SERIALFUNC = {serialfunc}")
        if deserialfunc:
            options.append(f"DESERIALFUNC = {deserialfunc}")
        if initcond is not None:
            options.append(f"INITCOND = {quoted(initcond)}")
        # Moving aggregate implementation - used for window functions with a moving frame
        if msfunc:
            options.append(f"MSFUNC = {msfunc}")
        if minvfunc:
            options.append(f"MINVFUNC = {minvfunc}")
        if mstype:
            options.append(f"MSTYPE = {mstype}")
        if msspace:
            options.append(f"MSSPACE = {msspace}")
        if mfinalfunc:
            options.append(f"MFINALFUNC = {mfinalfunc}")
            if mfinalextra:
                options.append("MFINALFUNC_EXTRA")
            if mfinalmodify in modify_keywords:
                options.append(f"MFINALFUNC_MODIFY = {modify_keywords[mfinalmodify]}")
        if minitcond is not None:
            options.append(f"MINITCOND = {quoted(minitcond)}")
        if sortop:
            options.append(f"SORTOP = OPERATOR({sortop})")
        if aggkind == 'h':
            options.append("HYPOTHETICAL")
        if proparallel in ('s', 'r'):
            options.append(f"PARALLEL = {parallel_keywords[proparallel]}")

        if aggkind in ('o', 'h'):
            # An ordered-set aggregate - pg_get_function_identity_arguments already formats
            # the direct arguments, the ORDER BY separator and the aggregated arguments.
            self.config_parser.print_log_message('DEBUG', f"postgresql_connector: get_create_aggregate_sql: {aggregate_name} is an ordered-set aggregate ({arguments}).")

        return (f"CREATE AGGREGATE {aggregate_schema}.{aggregate_name}({arguments}) (\n    "
                + ",\n    ".join(options) + "\n);")

    def convert_funcproc_code(self, settings):
        funcproc_code = settings['funcproc_code']
        # target_db_type = settings['target_db_type']
        source_schema_name = settings['source_schema_name']
        target_schema_name = settings['target_schema_name']

        # Simple schema replacement if they differ
        converted_code = self.qualify_text_search_references(
            funcproc_code, settings.get('text_search_objects'))
        if source_schema_name != target_schema_name:
            # Replace schema references
            # Use loose matching or generic replace for source_schema_name
            # This is risky if schema name is common word, but standard practice in this simple migration
            # Better: Replace "source_schema_name". with "target_schema_name".
            converted_code = converted_code.replace(f'"{source_schema_name}".', f'"{target_schema_name}".')
            # Also without quotes?
            converted_code = converted_code.replace(f'{source_schema_name}.', f'{target_schema_name}.')

        return converted_code

    def handle_error(self, e, description=None):
        self.config_parser.print_log_message('ERROR', f"postgresql_connector: handle_error: An error in {self.__class__.__name__} ({description}): {e}")
        self.config_parser.print_log_message('ERROR', traceback.format_exc())
        if self.on_error_action == 'stop':
            self.config_parser.print_log_message('ERROR', "postgresql_connector: handle_error: Stopping due to error.")
            exit(1)
        else:
            self.config_parser.print_log_message('WARNING', f"postgresql_connector: handle_error: Error caught, but continuing as requested by configuration (on_error_action='{self.on_error_action}').")

    def fetch_sequences(self, schema_name: str):
        sequences = {}
        order_num = 1
        query = f"""
            SELECT
                c.relname::text AS sequence_name,
                c.oid AS sequence_id
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind = 'S'
              AND n.nspname = '{schema_name}'
            ORDER BY c.relname
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()

            for row in rows:
                sequence_name = row[0]
                sequence_id = row[1]
                details = self.get_sequence_details(schema_name, sequence_name)
                min_value = details['min_value'] if details and details.get('min_value') is not None else 1
                max_value = details['max_value'] if details and details.get('max_value') is not None else 9223372036854775807
                increment_by = details['increment_by'] if details and details.get('increment_by') is not None else 1
                cycle = details['cycle'] if details and details.get('cycle') is not None else False
                cache_size = details['cache_size'] if details and details.get('cache_size') is not None else 1
                start_value = details['start_value'] if details and details.get('start_value') is not None else 1
                is_cycled = 'YES' if cycle else 'NO'

                ## where the sequence stands now - it is not in pg_sequence, it is in the
                ## sequence itself. It is reported next to the declared start value and not in
                ## its place: the two are different facts and the target needs both, the start
                ## for its own START WITH and the position for the setval which follows it.
                last_value = None
                try:
                    last_val_query = f'SELECT last_value FROM "{schema_name}"."{sequence_name}"'
                    cursor = self.connection.cursor()
                    cursor.execute(last_val_query)
                    curr_val_row = cursor.fetchone()
                    cursor.close()
                    if curr_val_row and curr_val_row[0] is not None:
                        last_value = curr_val_row[0]
                except Exception as e:
                    self.config_parser.print_log_message('WARNING', f"postgresql_connector: fetch_sequences: The position of the sequence {schema_name}.{sequence_name} could not be read ({e}) - the target sequence starts at its declared start value {start_value}.")

                source_sequence_sql = (
                    f'CREATE SEQUENCE "{schema_name}"."{sequence_name}" '
                    f'INCREMENT BY {increment_by} MINVALUE {min_value} MAXVALUE {max_value} '
                    f'START WITH {start_value} CACHE {cache_size} {"CYCLE" if is_cycled == "YES" else "NO CYCLE"};'
                )

                sequences[order_num] = {
                    'sequence_name': sequence_name,
                    'id': sequence_id,
                    'table_name': None,
                    'column_name': None,
                    'source_sequence_sql': source_sequence_sql,
                    'source_start_value': start_value,
                    'source_last_value': last_value,
                    'source_increment_by': increment_by,
                    'source_minvalue': min_value,
                    'source_maxvalue': max_value,
                    'source_cache': cache_size,
                    'source_is_cycled': is_cycled,
                }
                self.config_parser.print_log_message('DEBUG', f"postgresql_connector: fetch_sequences: Found sequence {sequence_name} (declared start {start_value}, standing at {last_value}, increment {increment_by}).")
                order_num += 1

            self.disconnect()
            return sequences
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_sequences: Error fetching sequences for schema {schema_name}: {e}")
            return sequences

    def fetch_table_sequences(self, table_schema: str, table_name: str):
        sequence_data = {}
        order_num = 1
        try:
            query = f"""
                SELECT
                    c.relname::text AS sequence_name,
                    c.oid AS sequence_id,
                    a.attname AS column_name,
                    '' as sequence_sql
                FROM
                    pg_depend d
                    JOIN pg_class c ON d.objid = c.oid
                    JOIN pg_attribute a ON d.refobjid = a.attrelid AND a.attnum = d.refobjsubid
                    JOIN pg_class t ON t.oid = d.refobjid
                WHERE
                    c.relkind = 'S'  /* sequence */
                    AND t.relname = '{table_name}'
                    AND t.relkind = 'r' /* regular local table */
                    AND d.refobjsubid > 0
                    AND c.relnamespace = '{table_schema}'::regnamespace
                ORDER BY 2,3
                """
            # self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                sequence_details = self.get_sequence_details(table_schema, row[0])

                # The sequence has to continue behind the data which was just loaded. Without a
                # value from the source - with DDL connectivity there is no source database at
                # all - it is derived from the migrated rows alone.
                set_sequence_sql = row[3] or self.get_set_sequence_sql({
                    'target_schema_name': table_schema,
                    'target_table_name': table_name,
                    'target_column_name': row[2],
                    'target_sequence_name': row[0],
                })

                sequence_data[order_num] = {
                    'name': row[0],
                    'id': row[1],
                    'column_name': row[2],
                    'set_sequence_sql': set_sequence_sql,
                    'details': sequence_details # Embed details
                }
            cursor.close()
            # self.disconnect()
            return sequence_data
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_table_sequences: Error executing sequence query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            return {}

    def get_set_sequence_sql(self, settings):
        """
        The statement which sets the sequence of a table so that it continues behind its data.

        Two values can say where the sequence has to continue and they do not always agree:
        the next value the source reported for its identity column, and the data which is
        really in the target. The source was preferred, which set the sequence below the data
        whenever the two came from different points in time - the source was reloaded, the
        identity block of Sybase ASE was burnt back after a crash, the data arrived from an
        older snapshot or from files - and the first INSERT of the application then failed with
        a duplicate key. The greater of the two is used, so the sequence can never fall behind
        the rows which are there; the gaps a source reserves above its data are kept as well.

        Without a value from the source the statement is the one derived from the data alone.
        """
        target_schema_name = settings['target_schema_name']
        target_table_name = settings['target_table_name']
        target_column_name = settings['target_column_name']
        target_sequence_name = settings['target_sequence_name']
        try:
            next_identity = int(settings['source_next_identity']) if settings.get('source_next_identity') is not None else None
        except (TypeError, ValueError):
            self.config_parser.print_log_message('WARNING',
                f"postgresql_connector: get_set_sequence_sql: The next identity value reported for "
                f'"{target_table_name}"."{target_column_name}" is not a number ({settings.get("source_next_identity")!r}) '
                "- the sequence is set from the data in the target.")
            next_identity = None

        behind_the_data = (f'COALESCE((SELECT MAX("{target_column_name}") '
                           f'FROM "{target_schema_name}"."{target_table_name}"), 0) + 1')
        new_value = behind_the_data if next_identity is None else f'GREATEST({next_identity}, {behind_the_data})'
        ## setval() takes a bigint, and the column of a sequence is not always an integer type -
        ## a NUMERIC one would end as 'function setval(unknown, numeric, boolean) does not exist'
        return f"""SELECT setval('"{target_schema_name}"."{target_sequence_name}"', ({new_value})::bigint, false);"""

    def get_table_next_identity(self, table_schema: str, table_name: str):
        try:
            # First find the sequence for the table. Since get_table_next_identity doesn't receive column_name,
            # we query pg_class and pg_depend to find an attached sequence, similar to fetch_table_sequences.
            query = f"""
                SELECT c.relname::text AS sequence_name
                FROM pg_depend d
                JOIN pg_class c ON d.objid = c.oid
                JOIN pg_class t ON t.oid = d.refobjid
                WHERE c.relkind = 'S'
                AND t.relname = '{table_name}'
                AND t.relkind = 'r'
                AND d.refobjsubid > 0
                AND c.relnamespace = '{table_schema}'::regnamespace
                LIMIT 1
            """
            cursor = self.connection.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            if not row:
                cursor.close()
                return None
            
            sequence_name = row[0]
            cursor.execute(f'SELECT last_value FROM "{table_schema}"."{sequence_name}"')
            val_row = cursor.fetchone()
            cursor.close()
            
            if val_row and val_row[0] is not None:
                return int(val_row[0])
            return None
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"postgresql_connector: get_table_next_identity: Error fetching next identity for {table_schema}.{table_name}: {e}")
            return None

    def get_sequence_details(self, sequence_owner, sequence_name):
        query = f"""
            SELECT
                s.seqmin,
                s.seqmax,
                s.seqincrement,
                s.seqcycle,
                s.seqcache,
                s.seqstart
            FROM pg_sequence s
            JOIN pg_class c ON s.seqrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = '{sequence_owner}'
              AND c.relname = '{sequence_name}'
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()

            if result:
                return {
                    'name': sequence_name,
                    'min_value': result[0],
                    'max_value': result[1],
                    'increment_by': result[2],
                    'cycle': result[3],
                    'cache_size': result[4],
                    ## pg_sequence.seqstart is where the sequence is declared to start. Where it
                    ## stands is not in the catalog at all - it is read from the sequence itself,
                    ## by get_sequence_current_value() or by the SELECT in fetch_sequences().
                    'start_value': result[5],
                    'comment': ''
                }
            else:
                return None

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: get_sequence_details: Error executing sequence query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_sequence_current_value(self, sequence_id: int):
        try:
            query = f"""select '"'||relnamespace::regnamespace::text||'"."'||relname||'"' as seqname from pg_class where oid = {sequence_id}"""
            cursor = self.connection.cursor()
            cursor.execute(query)
            sequence_data = cursor.fetchone()
            sequence_name = f"{sequence_data[0]}"

            query = f"""SELECT last_value FROM {sequence_name}"""
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            cur_value = cursor.fetchone()[0]
            cursor.close()
            self.disconnect()
            return cur_value
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: get_sequence_current_value: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_rows_count(self, table_schema: str, table_name: str, migration_limitation: str = None):
        query = f"""SELECT count(*) FROM "{table_schema}"."{table_name}" """
        if migration_limitation:
            query += f" WHERE {migration_limitation}"
        self.config_parser.print_log_message('DEBUG3', f"postgresql_connector: get_rows_count: postgresql: get_rows_count query: {query}")
        cursor = self.connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_table_size(self, table_schema: str, table_name: str):
        query = f"""SELECT pg_total_relation_size('{table_schema}.{table_name}')"""
        cursor = self.connection.cursor()
        cursor.execute(query)
        size = cursor.fetchone()[0]
        cursor.close()
        return size

    def convert_trigger(self, settings: dict):
        trigger_sql = settings['trigger_sql']
        source_schema_name = settings['source_schema_name']
        target_schema_name = settings['target_schema_name']

        # Simple schema replacement
        converted_code = trigger_sql
        if source_schema_name != target_schema_name:
             converted_code = converted_code.replace(f'"{source_schema_name}".', f'"{target_schema_name}".')
             converted_code = converted_code.replace(f'{source_schema_name}.', f'{target_schema_name}.')

        return converted_code

    def fetch_views_names(self, source_schema_name: str):
        views = {}
        order_num = 1
        query = f"""
            SELECT
                oid,
                relname as viewname,
                obj_description(oid, 'pg_class') as view_comment,
                relkind
            FROM pg_class
            WHERE relkind IN ('v', 'm')
            AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{source_schema_name}')
            AND relname NOT LIKE 'pg_%'
            ORDER BY viewname
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                view_type = 'MATERIALIZED VIEW' if row[3] == 'm' else 'VIEW'
                views[order_num] = {
                    'id': row[0],
                    'schema_name': source_schema_name,
                    'view_name': row[1],
                    'comment': row[2],
                    'view_type': view_type
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return views
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_views_names: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def migrate_sequences(self, target_connector, settings):
        """
        Create a single standalone sequence in the target database.
        Called once per sequence by the orchestrator's sequence_worker.
        """
        source_schema_name = settings.get('source_schema_name')
        target_schema_name = settings.get('target_schema_name')
        source_sequence_name = settings.get('source_sequence_name') or settings.get('sequence_name')
        target_sequence_name = settings.get('target_sequence_name') or source_sequence_name

        if not source_sequence_name:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: migrate_sequences: Missing sequence_name in settings: {settings}")
            return False

        self.config_parser.print_log_message('INFO', f"postgresql_connector: migrate_sequences: Migrating sequence {source_sequence_name} to {target_schema_name}.{target_sequence_name}...")

        PG_BIGINT_MAX = 9223372036854775807
        PG_BIGINT_MIN = -9223372036854775808

        def _to_int(val):
            try:
                return int(val)
            except (TypeError, ValueError):
                return None

        increment_by = _to_int(settings.get('source_increment_by')) or 1
        minvalue = _to_int(settings.get('source_minvalue'))
        maxvalue = _to_int(settings.get('source_maxvalue'))
        cache = _to_int(settings.get('source_cache'))
        is_cycled = str(settings.get('source_is_cycled') or '').upper() in ('Y', 'YES', 'TRUE', '1')

        ## Two different values, and the sequence of the target needs both: the declared start,
        ## which is what a RESTART of it will use, and the position, which the setval below
        ## sets. A source which keeps no declared start value hands the position over here, so
        ## that the target still does not begin behind its own data.
        start_value = _to_int(settings.get('source_start_value'))
        if start_value is None:
            start_value = _to_int(settings.get('source_last_value'))

        if maxvalue is not None and maxvalue >= PG_BIGINT_MAX:
            maxvalue = None
        if minvalue is not None and minvalue <= PG_BIGINT_MIN:
            minvalue = None

        last_value = _to_int(settings.get('source_last_value'))
        if last_value is None:
            last_value = start_value
        is_called = True
        try:
            self.connect()
            curr_val_query = f'SELECT last_value, is_called FROM "{source_schema_name}"."{source_sequence_name}"'
            cursor = self.connection.cursor()
            cursor.execute(curr_val_query)
            curr_val_row = cursor.fetchone()
            cursor.close()
            if curr_val_row:
                last_value = curr_val_row[0]
                is_called = curr_val_row[1]
        except Exception as e:
            self.config_parser.print_log_message('DEBUG', f"postgresql_connector: migrate_sequences: Could not fetch last_value directly from {source_sequence_name}: {e}")

        try:
            target_connector.connect()

            cycle_str = "CYCLE" if is_cycled else "NO CYCLE"
            parts = [f'CREATE SEQUENCE IF NOT EXISTS "{target_schema_name}"."{target_sequence_name}"']
            parts.append(f"INCREMENT BY {increment_by}")
            if minvalue is not None:
                parts.append(f"MINVALUE {minvalue}")
            if maxvalue is not None:
                parts.append(f"MAXVALUE {maxvalue}")
            if start_value is not None:
                parts.append(f"START WITH {start_value}")
            if cache is not None:
                parts.append(f"CACHE {cache}")
            parts.append(cycle_str)

            create_sql = " ".join(parts) + ";"
            self.config_parser.print_log_message('DEBUG', f"postgresql_connector: migrate_sequences: Sequence {target_sequence_name} SQL: {create_sql}")

            target_connector.execute_query(create_sql)

            if last_value is not None:
                setval_sql = f"SELECT setval('\"{target_schema_name}\".\"{target_sequence_name}\"', {last_value}, {'true' if is_called else 'false'});"
                self.config_parser.print_log_message('DEBUG', f"postgresql_connector: migrate_sequences: Sequence {target_sequence_name} SETVAL: {setval_sql}")
                target_connector.execute_query(setval_sql)

            return True
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: migrate_sequences: Failed to migrate sequence {source_sequence_name}: {e}")
            self.disconnect()
            try:
                target_connector.disconnect()
            except:
                pass
            raise

    def fetch_view_code(self, settings):
        view_id = settings['view_id']
        query = f"SELECT pg_get_viewdef({view_id}, true)"
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            view_code = cursor.fetchone()[0]
            cursor.close()
            self.disconnect()
            return view_code
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_view_code: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def convert_view_code(self, settings: dict):
        view_code = settings['view_code']
        view_name = settings['target_view_name']
        target_schema_name = settings['target_schema_name']
        view_type = settings.get('view_type', 'VIEW')

        ## the shared body converter, so that the query of a view and the statement of an
        ## application cannot be given different treatment (§15 phase 2 of the strategy). It
        ## is not asked to parse anything: the text comes from pg_get_viewdef() of the source,
        ## which PostgreSQL itself wrote, and a parse gate here could only refuse a view which
        ## migrates today.
        view_code = self.convert_statement_code(settings)

        ddl = f'CREATE {view_type} "{target_schema_name}"."{view_name}" AS {view_code}'
        if not ddl.strip().endswith(';'):
             ddl += ';'

        return ddl

    ## The source test of §8.1. PREPARE resolves every name, every function and every type
    ## and produces a plan, and it runs nothing. It is sent inside the same read only
    ## transaction the target test uses, and rolled back - the source of a migration is a
    ## production database and this step may not leave a thing behind in it.
    SOURCE_TEST_PARAMETER_STYLE = 'numbered'

    SOURCE_TEST_PROBE_NAME = 'credativ_pg_migrator_source_probe'

    def source_test_native_mechanism(self):
        return 'PREPARE'

    def source_test_probe(self, sql, parameter_count=0):
        body = (sql or '').rstrip().rstrip(';')
        if not body:
            return [], []
        timeout = self.config_parser.get_query_conversion_timeout()
        name = self.SOURCE_TEST_PROBE_NAME
        return ([
            'BEGIN;',
            'SET LOCAL transaction_read_only = on;',
            f"SET LOCAL statement_timeout = '{timeout}';",
            f'PREPARE {name} AS {body};',
            f'DEALLOCATE {name};',
        ], ['ROLLBACK;'])

    ## ---------------------------------------------------------------- partitioning

    ## The methods PostgreSQL writes in front of the key of a partitioned table, and the
    ## server version each of them needs. Hash partitioning and the DEFAULT partition arrived
    ## together in 11; declarative partitioning itself in 10.
    PARTITIONING_METHOD_VERSIONS = {'RANGE': 100000, 'LIST': 100000, 'HASH': 110000}

    ## The key as pg_get_partkeydef() writes it: 'RANGE (created_at)', 'LIST (region)',
    ## 'HASH (customer_id)', 'RANGE (created_at, region)', and expressions as well -
    ## 'RANGE (date_trunc(''month''::text, created_at))'.
    PARTITION_KEY_DEFINITION = re.compile(r'(?is)^\s*(RANGE|LIST|HASH)\s*\((.*)\)\s*$')

    def fetch_partitioning_candidates(self, schema):
        """
        The tables of one schema which are partitioned or are partitions, in one query - so
        that a schema of three hundred ordinary tables costs one round trip rather than three
        hundred.
        """
        query = """
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relkind IN ('r', 'p')
              AND (c.relkind = 'p' OR c.relispartition)
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, (schema,))
            names = {row[0] for row in cursor.fetchall()}
            cursor.close()
            return names
        except Exception as e:
            self.config_parser.print_log_message(
                'WARNING', f"postgresql_connector: fetch_partitioning_candidates: the partitioned "
                           f"tables of {schema} could not be listed ({first_line(e)}) - every table "
                           f"is asked instead.")
            return None

    def fetch_table_partitioning(self, settings):
        """
        The partitioning of one table, read out of pg_class / pg_partitioned_table /
        pg_inherits. See the contract in DatabaseConnector.fetch_table_partitioning().

        Only the partitions of the level below this table are listed. A partition which is
        itself partitioned says so with `is_partitioned`, and the caller asks it in turn -
        which is how a scheme of more than one level is read without this method recursing
        into a table the configuration may not even have selected.
        """
        source_schema_name = settings['source_schema_name']
        source_table_name = settings['source_table_name']

        query = """
            SELECT c.oid,
                   c.relkind,
                   c.relispartition,
                   pg_get_partkeydef(c.oid)                       AS key_definition,
                   pg_get_expr(c.relpartbound, c.oid)             AS partition_bound,
                   parent.relname                                 AS parent_table,
                   parent_ns.nspname                              AS parent_schema
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_inherits i ON i.inhrelid = c.oid
            LEFT JOIN pg_class parent ON parent.oid = i.inhparent
            LEFT JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
            WHERE n.nspname = %s AND c.relname = %s AND c.relkind IN ('r', 'p')
        """
        partitions_query = """
            SELECT child.relname,
                   pg_get_expr(child.relpartbound, child.oid)     AS partition_bound,
                   child.relkind = 'p'                            AS is_partitioned,
                   child.reltuples::bigint                        AS estimated_rows
            FROM pg_inherits i
            JOIN pg_class child ON child.oid = i.inhrelid
            JOIN pg_class parent ON parent.oid = i.inhparent
            JOIN pg_namespace n ON n.oid = parent.relnamespace
            WHERE n.nspname = %s AND parent.relname = %s
            ORDER BY child.relname
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, (source_schema_name, source_table_name))
            row = cursor.fetchone()
            if row is None:
                cursor.close()
                return {}

            is_partitioned = row[1] == 'p'
            is_partition = bool(row[2])
            if not is_partitioned and not is_partition:
                cursor.close()
                return {}

            method, columns = self.parse_partition_key_definition(row[3])
            partitions = []
            if is_partitioned:
                cursor.execute(partitions_query, (source_schema_name, source_table_name))
                for partition in cursor.fetchall():
                    bound = partition[1] or ''
                    partitions.append({
                        'name': partition[0],
                        'bound': bound,
                        ## a DEFAULT partition takes every row no other partition takes, and
                        ## it is what makes ATTACH of a new partition expensive - the reader of
                        ## the report has to be told which one it is
                        'is_default': bound.strip().upper() == 'DEFAULT',
                        'is_partitioned': bool(partition[2]),
                        ## reltuples is the planner's estimate and is -1 on a relation which
                        ## was never analysed; it is reported as unknown rather than as 0
                        'rows': int(partition[3]) if partition[3] is not None and partition[3] >= 0 else None,
                    })
            cursor.close()
        except Exception as e:
            self.config_parser.print_log_message(
                'ERROR', f"postgresql_connector: fetch_table_partitioning: the partitioning of "
                         f"{source_schema_name}.{source_table_name} could not be read: {first_line(e)}")
            return {}

        return {
            'is_partitioned': is_partitioned,
            'is_partition': is_partition,
            'parent_table': row[5] or '',
            'parent_schema': row[6] or '',
            'partition_bound': row[4] or '',
            'method': method,
            'columns': columns,
            'key_definition': row[3] or '',
            ## the level is what the caller knows and this method does not: a partition of a
            ## partition is level 3, and only the walk which found it can say so
            'level': 1,
            'partitions': partitions,
            'partition_count': len(partitions),
            'engine_specific': {},
        }

    @classmethod
    def parse_partition_key_definition(cls, key_definition):
        """
        The method and the columns out of what pg_get_partkeydef() wrote.

        The columns are answered as they stand, so an expression key - which PostgreSQL allows
        and this migrator's target_partitioning does not offer - comes back as the expression
        rather than as a name which does not exist.
        """
        if not key_definition:
            return '', []
        match = cls.PARTITION_KEY_DEFINITION.match(key_definition)
        if not match:
            return '', []
        method = match.group(1).upper()
        columns = [part.strip() for part in cls.split_top_level_commas(match.group(2)) if part.strip()]
        return method, columns

    @staticmethod
    def split_top_level_commas(text):
        """Split on the commas which are not inside brackets or a string literal."""
        parts = []
        depth = 0
        in_literal = False
        current = []
        index = 0
        while index < len(text):
            char = text[index]
            if in_literal:
                current.append(char)
                if char == "'":
                    if index + 1 < len(text) and text[index + 1] == "'":
                        current.append(text[index + 1])
                        index += 2
                        continue
                    in_literal = False
                index += 1
                continue
            if char == "'":
                in_literal = True
                current.append(char)
            elif char in '([':
                depth += 1
                current.append(char)
            elif char in ')]':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                parts.append(''.join(current))
                current = []
            else:
                current.append(char)
            index += 1
        parts.append(''.join(current))
        return parts

    ## The types a `date_range` can be counted in. Everything else - a text column holding
    ## something which looks like a date, a number - has no calendar, and asking for monthly
    ## partitions over it is a configuration which cannot be carried out.
    DATE_RANGE_TYPES = ('date', 'timestamp without time zone', 'timestamp with time zone')

    def fetch_partitioning_facts(self, settings):
        """
        Everything about one table which decides whether it can be partitioned - read before
        anything is created, which is the moment the answer is still free.

        See DatabaseConnector.fetch_partitioning_facts() for the shape. Every number here
        comes from the catalogue or from the statistics the server has already gathered:
        nothing in this method reads a row of the table.
        """
        source_schema_name = settings['source_schema_name']
        source_table_name = settings['source_table_name']

        columns_query = """
            SELECT a.attname,
                   format_type(a.atttypid, a.atttypmod)                     AS type_name,
                   a.attnotnull,
                   a.attgenerated <> ''                                     AS is_generated,
                   EXISTS (SELECT 1 FROM pg_opclass o JOIN pg_am m ON m.oid = o.opcmethod
                            WHERE o.opcdefault AND m.amname = 'btree'
                              AND o.opcintype = base.oid)                   AS has_btree,
                   EXISTS (SELECT 1 FROM pg_opclass o JOIN pg_am m ON m.oid = o.opcmethod
                            WHERE o.opcdefault AND m.amname = 'hash'
                              AND o.opcintype = base.oid)                   AS has_hash,
                   s.null_frac
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            -- a domain is partitioned by the operator classes of the type it is built on
            JOIN LATERAL (
                WITH RECURSIVE resolved(oid, typbasetype) AS (
                    SELECT t.oid, t.typbasetype FROM pg_type t WHERE t.oid = a.atttypid
                    UNION ALL
                    SELECT t.oid, t.typbasetype FROM pg_type t JOIN resolved r ON t.oid = r.typbasetype
                )
                SELECT oid FROM resolved WHERE typbasetype = 0 LIMIT 1
            ) base ON true
            LEFT JOIN pg_stats s ON s.schemaname = n.nspname AND s.tablename = c.relname
                                AND s.attname = a.attname
            WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
        """
        keys_query = """
            SELECT con.conname, con.contype,
                   ARRAY(SELECT a.attname FROM unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord)
                          JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
                         ORDER BY k.ord)                                    AS columns
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
              AND con.contype IN ('p', 'u', 'x') AND con.conparentid = 0
            ORDER BY con.conname
        """
        ## a unique INDEX which is not a constraint follows the same rule and is easy to forget
        unique_indexes_query = """
            SELECT i.relname,
                   ARRAY(SELECT pg_get_indexdef(x.indexrelid, k.ord::integer, true)
                           FROM generate_subscripts(x.indkey, 1) AS k(ord))  AS columns
            FROM pg_index x
            JOIN pg_class i ON i.oid = x.indexrelid
            JOIN pg_class c ON c.oid = x.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s AND x.indisunique
              AND NOT EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = i.oid)
            ORDER BY i.relname
        """
        referencing_query = """
            SELECT con.conname, child.relname
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.confrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_class child ON child.oid = con.conrelid
            WHERE n.nspname = %s AND c.relname = %s AND con.contype = 'f' AND con.conparentid = 0
            ORDER BY con.conname
        """
        table_query = """
            SELECT c.reltuples::bigint,
                   EXISTS (SELECT 1 FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent
                            WHERE i.inhrelid = c.oid AND p.relkind <> 'p')  AS inherits_from,
                   EXISTS (SELECT 1 FROM pg_inherits i JOIN pg_class ch ON ch.oid = i.inhrelid
                            WHERE i.inhparent = c.oid AND c.relkind <> 'p') AS inherited_by
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
        """
        binds = (source_schema_name, source_table_name)
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(columns_query, binds)
            columns = {}
            for row in cursor.fetchall():
                columns[row[0]] = {
                    'type_name': row[1], 'not_null': bool(row[2]), 'is_generated': bool(row[3]),
                    'has_btree_opclass': bool(row[4]), 'has_hash_opclass': bool(row[5]),
                    ## None where nobody has run ANALYZE - reported as not checked, never as 0
                    'null_fraction': row[6],
                }

            cursor.execute(keys_query, binds)
            unique_keys = []
            exclusion_constraints = []
            for row in cursor.fetchall():
                if row[1] == 'x':
                    exclusion_constraints.append(row[0])
                    continue
                unique_keys.append({'name': row[0], 'columns': list(row[2] or []),
                                    'is_primary': row[1] == 'p'})

            cursor.execute(unique_indexes_query, binds)
            for row in cursor.fetchall():
                unique_keys.append({'name': row[0], 'columns': list(row[1] or []),
                                    'is_primary': False})

            cursor.execute(referencing_query, binds)
            referenced_by = [{'name': row[0], 'table': row[1]} for row in cursor.fetchall()]

            cursor.execute(table_query, binds)
            row = cursor.fetchone()
            cursor.close()
        except Exception as e:
            self.config_parser.print_log_message(
                'WARNING', f"postgresql_connector: fetch_partitioning_facts: the facts of "
                           f"{source_schema_name}.{source_table_name} could not be read "
                           f"({first_line(e)}) - the checks which need them are reported as NOT "
                           f"made rather than as passed.")
            return None

        return {
            'columns': columns,
            'unique_keys': unique_keys,
            'exclusion_constraints': exclusion_constraints,
            'referenced_by': referenced_by,
            'inherits_from_a_plain_table': bool(row[1]) if row else False,
            'is_a_plain_inheritance_parent': bool(row[2]) if row else False,
            'row_estimate': int(row[0]) if row and row[0] is not None and row[0] >= 0 else None,
            'date_range_types': self.DATE_RANGE_TYPES,
        }

    def get_create_partition_sql(self, settings):
        """
        `CREATE TABLE … PARTITION OF …` for one partition of a scheme which is carried over
        from the source as it stands.

        The bound is the text `pg_get_expr(relpartbound)` answered on the source - it is
        already PostgreSQL and is not rewritten. Only the names are: the schema is the target
        schema, and the table names go through names_case_handling like every other name.
        """
        target_schema_name = settings['target_schema_name']
        target_table_name = self.config_parser.convert_names_case(settings['target_table_name'])
        parent_table_name = self.config_parser.convert_names_case(settings['parent_table_name'])
        bound = (settings.get('partition_bound') or '').strip()
        if not bound:
            raise ValueError(f"the partition {target_table_name} of {parent_table_name} has no "
                             f"bound - it cannot be created without one")
        sql = (f'CREATE TABLE "{target_schema_name}"."{target_table_name}" '
               f'PARTITION OF "{target_schema_name}"."{parent_table_name}" {bound}')
        key_definition = (settings.get('key_definition') or '').strip()
        if key_definition:
            ## a partition which is itself partitioned - the scheme has more than one level
            sql += f' PARTITION BY {key_definition}'
        return sql

    ## ---------------------------------------------------------------- application queries

    def query_conversion_supported(self):
        return True

    def convert_statement_code(self, settings: dict):
        """
        One statement of PostgreSQL, converted for PostgreSQL - the body of a view and the
        statement of an application are given the same one.

        There is no dialect conversion: what the two share is the text search references,
        which name an object of the migration from inside a string literal and can therefore
        not be rewritten as identifiers. Everything else the statement needs - the schema, the
        names the migration gave the tables and the columns - is the work of the name map of
        §7.3, which runs over the parsed statement outside the connector.
        """
        code = settings['view_code']
        target_db_type = settings.get('target_db_type') or 'postgresql'
        if target_db_type != 'postgresql':
            raise ValueError(f"the target database type {target_db_type} is not supported - "
                             f"this connector migrates PostgreSQL to PostgreSQL")
        return self.qualify_text_search_references(code, settings.get('text_search_objects'))

    def postgresql_conversion_blockers(self, source_code):
        """
        The reasons the statement may not be offered as a conversion. Each of them is valid
        PostgreSQL on the target and answers something else there, so none of them is caught
        by the target test - which is the whole reason they are listed.
        """
        statement = self.sql_without_literals_and_comments(source_code or '')
        reasons = []
        if POSTGRESQL_SYSTEM_COLUMNS.search(statement):
            reasons.append(
                "the statement reads a system column (ctid, xmin, xmax, cmin, cmax or "
                "tableoid). These carry the physical identity of a row and the transaction "
                "which wrote it: the migration inserted the rows again, in an order of its "
                "own, so every one of those values is another one on the target - and the "
                "statement is accepted there, which is why it is stopped here. Rewrite it "
                "around the primary key")
        if POSTGRESQL_SESSION_SEQUENCE.search(statement):
            reasons.append(
                "currval() and lastval() answer what the PREVIOUS statement of the same "
                "session took out of a sequence. A statement converted on its own has no such "
                "predecessor, and the sequences of the target were set to the values the "
                "migration found - PostgreSQL answers the RETURNING clause of the statement "
                "which wrote instead")
        return reasons

    def postgresql_conversion_warnings(self, source_code):
        """
        What is carried over unchanged and still answers something else on the target,
        because the target is another server with another catalogue and another session.
        """
        warnings = []
        statement = self.sql_without_literals_and_comments(source_code or '')

        if POSTGRESQL_REG_CAST.search(source_code or ''):
            warnings.append(
                "the statement names an object inside a string literal and casts it "
                "('name'::regclass, 'name'::regconfig). Such a name is resolved through the "
                "search_path when the statement runs and cannot be rewritten as an "
                "identifier, so the map of the migration does not reach it: where the object "
                "was migrated into another schema or under another name, the literal has to "
                "be written out with the schema of the target")
        if POSTGRESQL_CATALOGUE.search(statement):
            warnings.append(
                "the statement reads the catalogue of the server (pg_catalog / "
                "information_schema). It answers about the TARGET database - its tables, its "
                "settings, its statistics - and not about the database the application was "
                "written against; nothing of the catalogue is migrated")
        if POSTGRESQL_SESSION_FUNCTIONS.search(statement):
            warnings.append(
                "the statement asks about the connection - the database, the schema, the role "
                "or the server version. Every one of those answers the target after the "
                "migration, so a comparison against a name the application has written down "
                "stops being true")
        if POSTGRESQL_COLLATE.search(statement):
            warnings.append(
                "the statement names a collation explicitly. It has to exist on the target "
                "cluster and be built from the same locale data - a collation of another "
                "version sorts other rows first, and neither the migration nor the target "
                "test can see that")
        return warnings

    def convert_query_code(self, settings: dict):
        """
        One statement of an application, converted for PostgreSQL - the same conversion the
        query of a view is given. See the contract in DatabaseConnector.convert_query_code().

        The statement is already the dialect of the target, so what this answers is mostly the
        statement it was given. It is still read as PostgreSQL first: a statement the parser
        cannot read is reported as one which was not converted, and not handed on as one which
        was - the name map of §7.3 and gate 4 both parse it further down, and a statement which
        arrives there unreadable is reported with a message about a stage it never reached.
        """
        statement_id = settings.get('statement_id', '')
        source_code = settings['query_code']
        warnings = self.postgresql_conversion_warnings(source_code)

        try:
            parsed = sqlglot.parse_one(source_code, read='postgres')
        except Exception as e:
            return {'code': '', 'converted': False, 'warnings': warnings,
                    'error': f"the statement could not be read as PostgreSQL: {first_line(e)}"}
        if parsed is None:
            return {'code': '', 'converted': False, 'warnings': warnings,
                    'error': 'the parser read no statement at all'}

        blockers = self.postgresql_conversion_blockers(source_code)
        if blockers:
            return {'code': '', 'converted': False, 'warnings': warnings,
                    'error': '; '.join(blockers)}

        try:
            converted = self.convert_statement_code({
                'view_code': source_code,
                'source_schema_name': settings.get('source_schema_name'),
                'target_schema_name': settings.get('target_schema_name'),
                'target_db_type': settings.get('target_db_type', 'postgresql'),
            })
        except ValueError as e:
            return {'code': '', 'converted': False, 'warnings': warnings, 'error': first_line(e)}
        except Exception as e:
            return {'code': '', 'converted': False, 'warnings': warnings,
                    'error': f"the conversion ended with an error: {first_line(e)}"}

        if not (converted or '').strip():
            return {'code': '', 'converted': False, 'warnings': warnings,
                    'error': 'the conversion produced no statement at all'}

        self.config_parser.print_log_message(
            'DEBUG', f"postgresql_connector: convert_query_code: {statement_id}: {converted}")
        return {'code': converted, 'converted': True, 'warnings': warnings, 'error': None}


    def fetch_user_defined_types(self, schema: str):
        user_defined_types = {}
        order_num = 1
        self.connect()
        cursor = self.connection.cursor()

        try:
            # 1. ENUMS
            query_enum = f"""
                SELECT t.typnamespace::regnamespace::text as schemaname, typname as type_name,
                    'CREATE TYPE "'||t.typnamespace::regnamespace||'"."'||typname||'" AS ENUM ('||string_agg(''''||e.enumlabel||'''', ',' ORDER BY e.enumsortorder)::text||');' AS elements,
                    obj_description(t.oid, 'pg_type') as type_comment
                FROM pg_type AS t
                JOIN pg_enum AS e ON e.enumtypid = t.oid
                JOIN pg_namespace n ON t.typnamespace = n.oid
                WHERE n.nspname = '{schema}'
                AND t.typtype = 'e'
                GROUP BY t.oid, t.typnamespace, t.typname
                ORDER BY t.typname;
            """
            cursor.execute(query_enum)
            for row in cursor.fetchall():
                user_defined_types[order_num] = {
                    'schema_name': row[0],
                    'type_name': row[1],
                    'sql': row[2],
                    'comment': row[3]
                }
                order_num += 1

            # 2. Composite Types
            query_composite = f"""
                SELECT
                    t.oid,
                    n.nspname,
                    t.typname,
                    pg_catalog.obj_description(t.oid, 'pg_type'),
                    (
                        SELECT string_agg('"'||a.attname||'" '||pg_catalog.format_type(a.atttypid, a.atttypmod), ', ' ORDER BY a.attnum)
                        FROM pg_attribute a
                        WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped
                    ) as attributes,
                    (
                        SELECT ARRAY_AGG(DISTINCT COALESCE(NULLIF(elem.typelem, 0), a.atttypid))
                        FROM pg_attribute a
                        JOIN pg_type elem ON elem.oid = a.atttypid
                        WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped
                    ) as attr_type_oids
                FROM pg_type t
                JOIN pg_namespace n ON t.typnamespace = n.oid
                JOIN pg_class c ON t.typrelid = c.oid
                WHERE t.typtype = 'c'
                  AND c.relkind = 'c'
                  AND n.nspname = '{schema}'
                ORDER BY t.typname
            """
            cursor.execute(query_composite)
            composite_rows = cursor.fetchall()
            composite_items = []
            for row in composite_rows:
                oid = row[0]
                schema_name = row[1]
                type_name = row[2]
                comment = row[3]
                attributes = row[4]
                attr_type_oids = row[5] or []
                sql = f'CREATE TYPE "{schema_name}"."{type_name}" AS ({attributes});'
                composite_items.append({
                    'oid': oid,
                    'schema_name': schema_name,
                    'type_name': type_name,
                    'comment': comment,
                    'sql': sql,
                    'attr_type_oids': attr_type_oids
                })

            known_oids = {item['oid'] for item in composite_items}
            item_by_oid = {item['oid']: item for item in composite_items}
            sorted_composites = []
            visited = set()
            visiting = set()

            def visit_composite(item):
                if item['oid'] in visiting:
                    return
                if item['oid'] not in visited:
                    visiting.add(item['oid'])
                    dep_oids = set(item['attr_type_oids'] or []) & known_oids - {item['oid']}
                    for dep_oid in dep_oids:
                        if dep_oid in item_by_oid:
                            visit_composite(item_by_oid[dep_oid])
                    visiting.remove(item['oid'])
                    visited.add(item['oid'])
                    sorted_composites.append(item)

            for item in composite_items:
                if item['oid'] not in visited:
                    visit_composite(item)

            for item in sorted_composites:
                user_defined_types[order_num] = {
                    'schema_name': item['schema_name'],
                    'type_name': item['type_name'],
                    'sql': item['sql'],
                    'comment': item['comment']
                }
                order_num += 1

            # 3. Range Types
            query_range = f"""
                SELECT
                    n.nspname,
                    t.typname,
                    pg_catalog.obj_description(t.oid, 'pg_type'),
                    r.rngsubtype::regtype::text,
                    (SELECT c.collname FROM pg_collation c WHERE c.oid = r.rngcollation AND c.collname != 'default') as collation,
                    r.rngcanonical::regproc::text,
                    r.rngsubdiff::regproc::text
                FROM pg_type t
                JOIN pg_range r ON r.rngtypid = t.oid
                JOIN pg_namespace n ON t.typnamespace = n.oid
                WHERE n.nspname = '{schema}'
                ORDER BY t.typname
            """
            cursor.execute(query_range)
            for row in cursor.fetchall():
                schema_name = row[0]
                type_name = row[1]
                comment = row[2]
                subtype = row[3]
                collation = row[4]
                canonical = row[5]
                subdiff = row[6]

                parts = [f"SUBTYPE = {subtype}"]
                if collation:
                    parts.append(f"COLLATION = {collation}")
                if canonical and canonical != '-':
                    parts.append(f"CANONICAL = {canonical}")
                if subdiff and subdiff != '-':
                    parts.append(f"SUBTYPE_DIFF = {subdiff}")

                sql = f'CREATE TYPE "{schema_name}"."{type_name}" AS RANGE ({", ".join(parts)});'

                user_defined_types[order_num] = {
                    'schema_name': schema_name,
                    'type_name': type_name,
                    'sql': sql,
                    'comment': comment
                }
                order_num += 1

            cursor.close()
            self.disconnect()
            return user_defined_types
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_user_defined_types: Error fetching UDTs: {e}")
            raise

    def prepare_session_settings(self):
        """
        The SET statements of the session settings of this connection, as one string.

        The settings are read from the section of the configuration this connection belongs
        to - the settings of the target are not applied on the connection to the source, and a
        PostgreSQL source can carry its own.

        A name is compared against pg_settings without regard to case, so 'Role' or 'WORK_MEM'
        are the settings they name and not a KeyError. A name PostgreSQL does not know is
        reported instead of being dropped in silence. 'role' is applied last: a setting which
        needs more rights than the role has must be set before the switch to it.
        """
        filtered_settings = ""
        try:
            settings = self.config_parser.get_db_session_settings(self.source_or_target)
            if not settings:
                self.config_parser.print_log_message('INFO', f"postgresql_connector: prepare_session_settings: No session settings in the configuration for the {self.source_or_target} database.")
                return filtered_settings
            self.connect()
            cursor = self.connection.cursor()
            lower_keys = tuple(k.lower() for k in settings.keys())
            cursor.execute("SELECT name FROM (SELECT name FROM pg_settings UNION ALL SELECT name FROM (VALUES('role')) as t(name) ) a WHERE lower(a.name) IN %s", (lower_keys,))
            known_names = [row[0].lower() for row in cursor.fetchall()]
            cursor.close()
            self.disconnect()

            unknown_names = [name for name in settings.keys() if str(name).lower() not in known_names]
            if unknown_names:
                self.config_parser.print_log_message('WARNING', f"postgresql_connector: prepare_session_settings: The {self.source_or_target} database does not know the setting(s) {unknown_names} - they are not applied. Check the spelling against pg_settings.")

            accepted_names = [name for name in settings.keys() if str(name).lower() in known_names]
            ## the order of the configuration is kept, with 'role' moved to the end
            accepted_names.sort(key=lambda name: str(name).lower() == 'role')
            if not accepted_names:
                self.config_parser.print_log_message('INFO', "postgresql_connector: prepare_session_settings: No settings found to prepare.")
                return filtered_settings

            for setting_name in accepted_names:
                if str(setting_name).lower() == 'search_path':
                    filtered_settings += f"SET {setting_name} = {settings[setting_name]};"
                else:
                    filtered_settings += f"SET {setting_name} = '{settings[setting_name]}';"
            self.config_parser.print_log_message('INFO', f"postgresql_connector: prepare_session_settings: Session settings of the {self.source_or_target} database: {filtered_settings}")
            return filtered_settings
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: prepare_session_settings: Error preparing session settings: {e}")
            raise

    def fetch_domains(self, schema: str):
        domains = {}
        order_num = 1
        # Fetch domains (Base type, Default, Not Null, Constraints)
        query = f"""
            SELECT
                n.nspname as domain_schema,
                t.typname as domain_name,
                pg_catalog.format_type(t.typbasetype, t.typtypmod) as domain_data_type,
                t.typnotnull,
                t.typdefault,
                pg_catalog.obj_description(t.oid, 'pg_type') as comment,
                (SELECT string_agg(pg_get_constraintdef(r.oid), ' ') FROM pg_constraint r WHERE r.contypid = t.oid) as constraints
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            LEFT JOIN pg_class c ON c.oid = t.typrelid
            WHERE t.typtype = 'd'
              AND n.nspname = '{schema}'
            ORDER BY t.typname
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                domains[order_num] = {
                    'domain_schema': row[0],
                    'domain_name': row[1],
                    'domain_data_type': row[2],
                    'domain_not_null': row[3],
                    'domain_default': row[4],
                    'domain_comment': row[5],
                    'source_domain_check_sql': row[6], # Can be None
                    'source_domain_sql': f"CREATE DOMAIN \"{row[0]}\".\"{row[1]}\" AS {row[2]}", # Simplified for now, real construction happen in get_create_domain_sql
                }
                order_num += 1
            cursor.close()
            self.disconnect()
            return domains
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_domains: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_domain_sql(self, settings):
        create_domain_sql = ""
        domain_name = settings['domain_name']
        target_schema_name = settings['target_schema_name']
        domain_check_sql = settings.get('source_domain_check_sql')
        domain_data_type = settings['domain_data_type']
        domain_default = settings.get('domain_default')
        domain_not_null = settings.get('domain_not_null')

        migrated_as = settings.get('migrated_as', 'CHECK CONSTRAINT')

        if migrated_as == 'CHECK CONSTRAINT':
             # Fallback logic if needed, but primarily used for sybase patterns
             if domain_check_sql:
                create_domain_sql = f"""CHECK({domain_check_sql})"""
             else:
                create_domain_sql = ""
        else:
            # Construct standard CREATE DOMAIN
            sql_parts = [f'CREATE DOMAIN "{target_schema_name}"."{domain_name}" AS {domain_data_type}']

            if domain_default is not None:
                sql_parts.append(f"DEFAULT {domain_default}")

            if domain_not_null and not (domain_check_sql and 'NOT NULL' in domain_check_sql.upper()):
                sql_parts.append("NOT NULL")

            if domain_check_sql:
                # Ensure PostgreSQL standalone domain constraints rely on VALUE instead of column names
                domain_check_sql = re.sub(r'(?i)(CHECK\s*\(\s*)([a-zA-Z_]\w*)(\s+|[<>=!])', r'\g<1>VALUE\g<3>', domain_check_sql)

                # A source which delivers the bare condition of its domain - a Sybase ASE rule
                # is stored as 'create rule r_flag as @val in (0, 1)' - needs the CHECK clause
                # around it, otherwise the statement ends as 'AS SMALLINT VALUE in (0, 1)'.
                # The clause has to be recognized at the beginning of the condition and not
                # by the word CHECK anywhere in it - in "VALUE in ('CHECK', 'CASH')" that word
                # is data, and the condition would have been left without its clause.
                already_a_check_clause = re.match(r'(?is)\s*(CONSTRAINT\s+("[^"]+"|\w+)\s+)?CHECK\s*\(', domain_check_sql)
                if not already_a_check_clause:
                    domain_check_sql = f"CHECK ({domain_check_sql})"

                # pg_get_constraintdef already allows CHECK (...).
                # If multiple constraints were aggregated, they might look like CHECK (...) CHECK (...)
                # We just append them.
                sql_parts.append(domain_check_sql)

            create_domain_sql = " ".join(sql_parts) + ";"

        return create_domain_sql

    def fetch_collations(self, schema: str):
        """
        Returns user defined collations of the source database.

        Collations are database wide objects - a table in the migrated schema can use a
        collation created in another schema (typically public), therefore all non system
        schemas are searched. Collations sharing the same name in several schemas are
        reported only once - the one from the migrated schema wins.
        """
        collations = {}
        order_num = 1
        provider_names = {'c': 'libc', 'i': 'icu', 'd': 'default', 'b': 'builtin'}
        query = ''
        try:
            self.connect()
            cursor = self.connection.cursor()

            # Catalog columns of pg_collation differ between PostgreSQL versions:
            #   collisdeterministic - PostgreSQL 12+
            #   colliculocale       - PostgreSQL 15/16, renamed to colllocale in PostgreSQL 17
            #   collicurules        - PostgreSQL 16+
            cursor.execute("""
                SELECT attname FROM pg_catalog.pg_attribute
                WHERE attrelid = 'pg_catalog.pg_collation'::regclass
                  AND attnum > 0 AND NOT attisdropped
            """)
            available_columns = {row[0] for row in cursor.fetchall()}
            if 'colllocale' in available_columns:
                locale_column = 'c.colllocale'
            elif 'colliculocale' in available_columns:
                locale_column = 'c.colliculocale'
            else:
                locale_column = 'NULL::text'
            deterministic_column = 'c.collisdeterministic' if 'collisdeterministic' in available_columns else 'TRUE'
            rules_column = 'c.collicurules' if 'collicurules' in available_columns else 'NULL::text'

            query = f"""
                SELECT
                    n.nspname as collation_schema,
                    c.collname as collation_name,
                    c.collprovider as collation_provider,
                    {locale_column} as collation_locale,
                    c.collcollate as collation_lc_collate,
                    c.collctype as collation_lc_ctype,
                    {deterministic_column} as collation_deterministic,
                    {rules_column} as collation_rules,
                    c.collversion as collation_version,
                    pg_catalog.obj_description(c.oid, 'pg_collation') as collation_comment
                FROM pg_catalog.pg_collation c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.collnamespace
                WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                ORDER BY (n.nspname = '{schema}') DESC, n.nspname, c.collname
            """
            cursor.execute(query)
            for row in cursor.fetchall():
                collation_name = row[1]
                if collation_name in collations:
                    # Same name in another schema - references in the DDL are resolved by name,
                    # so only the first occurrence (the migrated schema) can be recreated.
                    self.config_parser.print_log_message('WARNING', f"postgresql_connector: fetch_collations: Collation {row[0]}.{collation_name} is skipped - collation with the same name is already migrated from another schema.")
                    continue
                provider = provider_names.get(row[2], row[2])
                collation_info = {
                    'collation_schema': row[0],
                    'collation_name': collation_name,
                    'collation_provider': provider,
                    'collation_locale': row[3],
                    'collation_lc_collate': row[4],
                    'collation_lc_ctype': row[5],
                    'collation_deterministic': row[6],
                    'collation_rules': row[7],
                    'collation_version': row[8],
                    'collation_comment': row[9],
                }
                collation_info['source_collation_sql'] = self.get_create_collation_sql(
                    {**collation_info, 'target_schema_name': row[0]})
                collations[collation_name] = collation_info

            cursor.close()
            self.disconnect()

            # Keys of the returned dict have to be ordinal numbers - see the base connector
            return {order_num + index: value for index, value in enumerate(collations.values())}
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_collations: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_collation_sql(self, settings):
        collation_name = settings['collation_name']
        target_schema_name = settings['target_schema_name']
        target_collation_name = settings.get('target_collation_name') or collation_name
        provider = (settings.get('collation_provider') or '').lower()
        locale = settings.get('collation_locale')
        lc_collate = settings.get('collation_lc_collate')
        lc_ctype = settings.get('collation_lc_ctype')
        deterministic = settings.get('collation_deterministic')
        rules = settings.get('collation_rules')

        def quoted(value):
            return "'" + str(value).replace("'", "''") + "'"

        options = []
        if provider in ('icu', 'libc', 'builtin'):
            options.append(f"provider = {provider}")

        if locale:
            options.append(f"locale = {quoted(locale)}")
        elif lc_collate and lc_ctype and lc_collate == lc_ctype:
            options.append(f"locale = {quoted(lc_collate)}")
        else:
            if lc_collate:
                options.append(f"lc_collate = {quoted(lc_collate)}")
            if lc_ctype:
                options.append(f"lc_ctype = {quoted(lc_ctype)}")

        if rules:
            options.append(f"rules = {quoted(rules)}")
        if deterministic is False:
            options.append("deterministic = false")

        if not options:
            self.config_parser.print_log_message('WARNING', f"postgresql_connector: get_create_collation_sql: Collation {collation_name} carries no locale settings - skipping.")
            return ''

        # The collation version is deliberately not copied - the target can be built with
        # another ICU / libc version and a wrong recorded version produces false warnings.
        return f'''CREATE COLLATION IF NOT EXISTS "{target_schema_name}"."{target_collation_name}" ({', '.join(options)});'''

    def fetch_text_search_objects(self, schema: str):
        """
        Returns user defined full text search dictionaries and configurations.

        Like collations these are database wide objects, so all non system schemas are
        searched - a generated tsvector column of the migrated schema regularly uses a
        configuration created in public. Objects belonging to an extension are left out,
        they come with the extension itself (e.g. the unaccent dictionary).
        Dictionaries come first, configurations reference them.
        """
        objects = {}
        query = ''
        try:
            self.connect()
            cursor = self.connection.cursor()

            query = f"""
                SELECT
                    n.nspname as object_schema,
                    d.dictname as object_name,
                    (SELECT quote_ident(tn.nspname)||'.'||quote_ident(t.tmplname)
                     FROM pg_catalog.pg_ts_template t
                     JOIN pg_catalog.pg_namespace tn ON tn.oid = t.tmplnamespace
                     WHERE t.oid = d.dicttemplate) as template_name,
                    d.dictinitoption as init_options,
                    pg_catalog.obj_description(d.oid, 'pg_ts_dict') as object_comment
                FROM pg_catalog.pg_ts_dict d
                JOIN pg_catalog.pg_namespace n ON n.oid = d.dictnamespace
                WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend dep
                                  WHERE dep.objid = d.oid AND dep.deptype = 'e')
                ORDER BY (n.nspname = '{schema}') DESC, n.nspname, d.dictname
            """
            cursor.execute(query)
            for row in cursor.fetchall():
                object_name = row[1]
                if object_name in objects:
                    self.config_parser.print_log_message('WARNING', f"postgresql_connector: fetch_text_search_objects: Text search dictionary {row[0]}.{object_name} is skipped - a dictionary with the same name is already migrated from another schema.")
                    continue
                objects[object_name] = {
                    'object_schema': row[0],
                    'object_name': object_name,
                    'object_type': 'DICTIONARY',
                    'template_name': row[2],
                    'init_options': row[3],
                    'parser_name': '',
                    'mappings': [],
                    'object_comment': row[4],
                }

            query = f"""
                SELECT
                    n.nspname as object_schema,
                    c.cfgname as object_name,
                    (SELECT quote_ident(pn.nspname)||'.'||quote_ident(p.prsname)
                     FROM pg_catalog.pg_ts_parser p
                     JOIN pg_catalog.pg_namespace pn ON pn.oid = p.prsnamespace
                     WHERE p.oid = c.cfgparser) as parser_name,
                    pg_catalog.obj_description(c.oid, 'pg_ts_config') as object_comment,
                    c.oid as config_oid,
                    c.cfgparser as parser_oid
                FROM pg_catalog.pg_ts_config c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.cfgnamespace
                WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend dep
                                  WHERE dep.objid = c.oid AND dep.deptype = 'e')
                ORDER BY (n.nspname = '{schema}') DESC, n.nspname, c.cfgname
            """
            cursor.execute(query)
            configurations = cursor.fetchall()

            for row in configurations:
                object_name = row[1]
                if object_name in objects:
                    self.config_parser.print_log_message('WARNING', f"postgresql_connector: fetch_text_search_objects: Text search configuration {row[0]}.{object_name} is skipped - an object with the same name is already migrated from another schema.")
                    continue
                # Token type -> ordered list of dictionaries. The mapping is what makes the
                # configuration different from the one it was copied from, so it has to be
                # transferred token type by token type.
                cursor.execute(f"""
                    SELECT tt.alias as token_type,
                           quote_ident(dn.nspname)||'.'||quote_ident(d.dictname) as dictionary
                    FROM pg_catalog.pg_ts_config_map m
                    JOIN pg_catalog.pg_ts_dict d ON d.oid = m.mapdict
                    JOIN pg_catalog.pg_namespace dn ON dn.oid = d.dictnamespace
                    JOIN pg_catalog.ts_token_type({row[5]}::oid) tt ON tt.tokid = m.maptokentype
                    WHERE m.mapcfg = {row[4]}
                    ORDER BY tt.alias, m.mapseqno
                """)
                mappings = {}
                for token_type, dictionary in cursor.fetchall():
                    mappings.setdefault(token_type, []).append(dictionary)

                objects[object_name] = {
                    'object_schema': row[0],
                    'object_name': object_name,
                    'object_type': 'CONFIGURATION',
                    'template_name': '',
                    'init_options': '',
                    'parser_name': row[2],
                    'mappings': sorted(mappings.items()),
                    'object_comment': row[3],
                }

            for object_info in objects.values():
                object_info['source_object_sql'] = self.get_create_text_search_sql(
                    {**object_info, 'target_schema_name': object_info['object_schema']})

            cursor.close()
            self.disconnect()

            # Dictionaries first - configurations reference them
            ordered = sorted(objects.values(), key=lambda item: 0 if item['object_type'] == 'DICTIONARY' else 1)
            return {index + 1: value for index, value in enumerate(ordered)}
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: fetch_text_search_objects: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            raise

    def get_create_text_search_sql(self, settings):
        object_name = settings['object_name']
        object_type = settings.get('object_type', '')
        target_schema_name = settings['target_schema_name']
        target_object_name = settings.get('target_object_name') or object_name
        qualified_name = f'"{target_schema_name}"."{target_object_name}"'

        if object_type == 'DICTIONARY':
            template_name = settings.get('template_name')
            if not template_name:
                self.config_parser.print_log_message('WARNING', f"postgresql_connector: get_create_text_search_sql: Text search dictionary {object_name} has no template - skipping.")
                return ''
            options = [f"TEMPLATE = {template_name}"]
            init_options = settings.get('init_options')
            if init_options:
                # dictinitoption is already a comma separated option list ("stopwords = 'english'")
                options.append(init_options)
            return f"""CREATE TEXT SEARCH DICTIONARY {qualified_name} ({', '.join(options)});"""

        if object_type == 'CONFIGURATION':
            parser_name = settings.get('parser_name')
            if not parser_name:
                self.config_parser.print_log_message('WARNING', f"postgresql_connector: get_create_text_search_sql: Text search configuration {object_name} has no parser - skipping.")
                return ''
            # The parser plus the explicit mappings are used instead of "COPY = <config>":
            # the configuration copied from does not have to exist in the target, and the
            # mappings were altered afterwards anyway.
            statements = [f"""CREATE TEXT SEARCH CONFIGURATION {qualified_name} (PARSER = {parser_name});"""]
            for token_type, dictionaries in settings.get('mappings') or []:
                if not dictionaries:
                    continue
                statements.append(
                    f"""ALTER TEXT SEARCH CONFIGURATION {qualified_name} ADD MAPPING FOR {token_type} WITH {', '.join(dictionaries)};""")
            return '\n'.join(statements)

        self.config_parser.print_log_message('WARNING', f"postgresql_connector: get_create_text_search_sql: Unknown text search object type '{object_type}' of {object_name} - skipping.")
        return ''

    def fetch_default_values(self, settings) -> dict:
        # Placeholder for fetching default values
        # Relevant only for database that support independently created named default values
        return {}

    def testing_select(self):
        return "SELECT 1"

    def get_database_version(self):
        query = "SELECT version()"
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        version = cursor.fetchone()[0]
        cursor.close()
        self.disconnect()
        return version

    def get_server_version_num(self):
        """ PostgreSQL server version as an integer, e.g. 120008 for 12.8, 170002 for 17.2. """
        query = "SHOW server_version_num"
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            version_num = int(cursor.fetchone()[0])
            cursor.close()
            self.disconnect()
            return version_num
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"postgresql_connector: get_server_version_num: Could not read the server version: {e}")
            return None

    def get_database_size(self):
        query = "SELECT pg_database_size(current_database())"
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
        # return top_tables

        source_schema_name = settings.get('source_schema_name', 'public')
        try:
            order_num = 1
            top_n = self.config_parser.get_top_n_tables_by_rows()
            if top_n > 0:
                query = f"""
                    SELECT
                    n.nspname AS owner,
                    c.relname AS table_name,
                    c.reltuples::bigint AS row_count,
                    pg_total_relation_size(c.oid) AS row_size
                    FROM
                    pg_class c
                    JOIN
                    pg_namespace n ON n.oid = c.relnamespace
                    WHERE
                    c.relkind = 'r' AND n.nspname = '{source_schema_name}'
                    ORDER BY
                    c.reltuples DESC
                    LIMIT {top_n};
                """
                self.connect()
                cursor = self.connection.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                cursor.close()
                self.disconnect()

                order_num = 1
                for row in results:
                    top_tables['by_rows'][order_num] = {
                    'owner': row[0].strip(),
                    'table_name': row[1].strip(),
                    'row_count': row[2],
                    'table_size': row[3],
                    }
                    order_num += 1

                self.config_parser.print_log_message('DEBUG2', f"postgresql_connector: get_top_n_tables: Top {top_n} tables by rows: {top_tables['by_rows']}")
            else:
                self.config_parser.print_log_message('DEBUG', "postgresql_connector: get_top_n_tables: Top N tables by rows is not configured or set to 0, skipping this part.")

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: get_top_n_tables: Error fetching top tables by rows: {e}")

        return top_tables
    
    def get_table_checksum(self, schema_name: str, table_name: str, columns: list):
        if not columns:
            return None
        
        # Use Python-based hashing for cross-database consistency
        cols_list = []
        for col in columns:
            dtype = col.get('data_type', '').lower()
            if any(x in dtype for x in ['lob', 'bytea', 'xml', 'json', 'text', 'oid']):
                # Skip LOBs / massive text blocks for table checksums to save bandwidth
                pass
            elif 'time' in dtype or 'date' in dtype:
                cols_list.append(f"TO_CHAR(\"{col['column_name']}\", 'YYYY-MM-DD HH24:MI:SS.US')")
            elif 'bool' in dtype or 'boolean' in dtype:
                cols_list.append(f"CAST(\"{col['column_name']}\" AS INT)")
            elif col.get('_force_round_0'):
                cols_list.append(f"ROUND(\"{col['column_name']}\", 0)")
            else:
                cols_list.append(f'"{col["column_name"]}"')
            
        if not cols_list:
            return None
            
        cols_str = ", ".join(cols_list)
        query = f'SELECT {cols_str} FROM "{schema_name}"."{table_name}"'
        return self._compute_python_table_checksum(query)

    def get_random_pks(self, schema_name: str, table_name: str, pk_columns: list, sample_size: int):
        if not pk_columns:
            return []
        cols = ", ".join([f'"{c}"' for c in pk_columns])
        query = f'SELECT {cols} FROM "{schema_name}"."{table_name}" ORDER BY random() LIMIT {sample_size}'
        pks = []
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                pks.append(dict(zip(pk_columns, row)))
            cursor.close()
            self.disconnect()
            return pks
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: get_random_pks: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            return []

    def get_row_checksums(self, schema_name: str, table_name: str, pk_columns: list, pk_values_list: list, columns: list):
        if not columns or not pk_columns or not pk_values_list:
            return {}
            
        cols_list = []
        for col in columns:
            dtype = col.get('data_type', '').lower()
            if any(x in dtype for x in ['lob', 'bytea', 'xml', 'json', 'text', 'oid']):
                pass
            elif 'time' in dtype or 'date' in dtype:
                cols_list.append(f"TO_CHAR(\"{col['column_name']}\", 'YYYY-MM-DD HH24:MI:SS.US')")
            elif 'bool' in dtype or 'boolean' in dtype:
                cols_list.append(f"CAST(\"{col['column_name']}\" AS INT)")
            elif col.get('_force_round_0'):
                cols_list.append(f"ROUND(\"{col['column_name']}\", 0)")
            else:
                cols_list.append(f'"{col["column_name"]}"')
            
        if not cols_list:
            return {}
            
        cols_str = ", ".join(cols_list)
        pk_cols_str = ", ".join([f'"{c}"' for c in pk_columns])
        
        # Build IN clause properly
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
        if not lob_columns or not pk_columns or not pk_values_list:
            return {}
            
        size_cols = []
        for col in lob_columns:
            if 'bytea' in col['data_type'].lower():
                size_cols.append(f"octet_length(\"{col['column_name']}\")")
            else:
                size_cols.append(f"length(\"{col['column_name']}\")")
                
        size_selects = ", ".join(size_cols)
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
            
        query = f'SELECT {pk_cols_str}, {size_selects} FROM "{schema_name}"."{table_name}" WHERE {where_clause}'
        
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
            self.config_parser.print_log_message('ERROR', f"postgresql_connector: get_lob_sizes: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            return {}

    def get_top_fk_dependencies(self, settings):
        top_fk_dependencies = {}
        return top_fk_dependencies

    def target_table_exists(self, target_schema_name, target_table_name):
        query = f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE lower(table_schema) = lower('{target_schema_name}')
                AND lower(table_name) = lower('{target_table_name}')
            )
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists

    ## The integer types share an operator family, so a foreign key between SMALLINT,
    ## INTEGER and BIGINT is accepted; NUMERIC belongs to another one, and a key mixing it
    ## with an integer type cannot be implemented at all.
    FK_COMPARABLE_INTEGER_TYPES = ('int2', 'int4', 'int8')
    FK_ALIGNABLE_NUMERIC_TYPES = FK_COMPARABLE_INTEGER_TYPES + ('numeric',)

    def align_foreign_key_column_types(self, settings):
        """
        Make the columns of a foreign key comparable before it is created.

        A column of the referenced table can end up with another data type than the column
        referencing it - a source identity column is created as BIGINT whatever its own type
        was, while the column referencing it keeps the type of the source, and PostgreSQL
        then refuses the key with 'Key columns "x" and "x" are of incompatible types:
        numeric and bigint'. The referencing column is altered to the type of the referenced
        one, which is the column of the primary key and must not be touched.

        Reads the types from the catalog of the target, so it repairs the key whatever the
        reason for the difference is. Returns the list of the columns it altered.
        """
        referencing_columns = settings['constraint_columns']
        referenced_columns = settings['referenced_columns']
        if not referencing_columns or not referenced_columns:
            return []

        referencing_types = self.fetch_column_types(settings['target_schema_name'], settings['target_table_name'])
        referenced_types = self.fetch_column_types(settings['referenced_schema_name'], settings['referenced_table_name'])

        altered_columns = []
        for referencing_column, referenced_column in zip(referencing_columns, referenced_columns):
            referencing_type = referencing_types.get(referencing_column.lower())
            referenced_type = referenced_types.get(referenced_column.lower())
            if not referencing_type or not referenced_type:
                continue
            if referencing_type['type_name'] == referenced_type['type_name']:
                continue
            if (referencing_type['type_name'] in self.FK_COMPARABLE_INTEGER_TYPES
                    and referenced_type['type_name'] in self.FK_COMPARABLE_INTEGER_TYPES):
                continue
            if not (referencing_type['type_name'] in self.FK_ALIGNABLE_NUMERIC_TYPES
                    and referenced_type['type_name'] in self.FK_ALIGNABLE_NUMERIC_TYPES):
                self.config_parser.print_log_message('WARNING',
                    f"postgresql_connector: align_foreign_key_column_types: Column "
                    f'"{settings["target_table_name"]}"."{referencing_column}" ({referencing_type["full_type"]}) and the '
                    f'column "{settings["referenced_table_name"]}"."{referenced_column}" ({referenced_type["full_type"]}) '
                    "it references have data types which cannot be aligned automatically - the foreign key is created as it is.")
                continue

            alter_column_sql = (f'ALTER TABLE "{settings["target_schema_name"]}"."{settings["target_table_name"]}" '
                                f'ALTER COLUMN "{referencing_column}" TYPE {referenced_type["full_type"]}')
            self.config_parser.print_log_message('INFO',
                f'postgresql_connector: align_foreign_key_column_types: Column "{settings["target_table_name"]}"."{referencing_column}" '
                f'({referencing_type["full_type"]}) references "{settings["referenced_table_name"]}"."{referenced_column}" '
                f'({referenced_type["full_type"]}) - altering it to the type of the referenced column, '
                "otherwise the foreign key cannot be implemented.")
            self.execute_query(alter_column_sql)
            altered_columns.append({
                'target_column': referencing_column,
                'original_data_type': referencing_type['full_type'],
                'altered_data_type': referenced_type['full_type'],
                'reason': (f'FOREIGN KEY to {settings["referenced_table_name"]}.{referenced_column} '
                           f'({referenced_type["full_type"]})'),
            })
        return altered_columns

    def fetch_column_types(self, target_schema_name, target_table_name):
        """
        The data types of the columns of a table of the target, keyed by the column name in
        lower case: {'category_id': {'type_name': 'int8', 'full_type': 'bigint'}}.
        """
        query = """
            SELECT lower(a.attname), t.typname, pg_catalog.format_type(a.atttypid, a.atttypmod)
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
            WHERE lower(n.nspname) = lower(%s)
              AND lower(c.relname) = lower(%s)
              AND a.attnum > 0
              AND NOT a.attisdropped
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (target_schema_name, target_table_name))
            return {row[0]: {'type_name': row[1], 'full_type': row[2]} for row in cursor.fetchall()}

    def target_view_exists(self, target_schema_name, target_view_name):
        """True if a view or materialized view with this name exists in the target schema.
        In PostgreSQL a view can only exist if its defining query is valid, so existence is a
        sufficient validity signal."""
        query = """
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('v', 'm')
                  AND lower(n.nspname) = lower(%s)
                  AND lower(c.relname) = lower(%s)
            )
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (target_schema_name, target_view_name))
            return cursor.fetchone()[0]

    def target_funcproc_exists(self, target_schema_name, target_funcproc_name):
        """True if a function or procedure with this name exists in the target schema
        (any overload / argument signature)."""
        query = """
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_proc p
                JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                WHERE lower(n.nspname) = lower(%s)
                  AND lower(p.proname) = lower(%s)
            )
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (target_schema_name, target_funcproc_name))
            return cursor.fetchone()[0]

    def target_funcprocs_with_prefix_exist(self, target_schema_name, name_prefix):
        """True if at least one function or procedure whose name starts with the given prefix
        exists in the target schema - used for objects migrated as a set of functions, such as
        an Oracle package split into <package>_<routine> functions."""
        query = """
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_proc p
                JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                WHERE lower(n.nspname) = lower(%s)
                  AND lower(p.proname) LIKE lower(%s)
            )
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (target_schema_name, name_prefix.replace('%', '') + '%'))
            return cursor.fetchone()[0]

    def target_trigger_exists(self, target_schema_name, target_table_name, trigger_name):
        """True if a trigger with this name exists on the given table in the target schema.
        The table name is optional; if not provided, any trigger with this name in the schema
        counts."""
        query = """
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_trigger t
                JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE NOT t.tgisinternal
                  AND lower(n.nspname) = lower(%s)
                  AND lower(t.tgname) = lower(%s)
                  AND (%s IS NULL OR lower(c.relname) = lower(%s))
            )
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (target_schema_name, trigger_name, target_table_name, target_table_name))
            return cursor.fetchone()[0]

    def fetch_all_rows(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def convert_default_value(self, settings) -> dict:
        extracted_default_value = settings['extracted_default_value']
        return extracted_default_value

if __name__ == "__main__":
    print("This script is not meant to be run directly")
