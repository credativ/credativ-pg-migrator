# credativ-pg-migrator
# Copyright (C) 2025 credativ GmbH

import jaydebeapi
from jaydebeapi import Error
import pyodbc
from pyodbc import Error
from credativ_pg_migrator.database_connector import DatabaseConnector
from credativ_pg_migrator.migrator_logging import MigratorLogger
import re
import traceback
import sys
import sqlglot
from sqlglot import exp, TokenType
from sqlglot.dialects import TSQL

# --- Custom Dialect Configuration ---
class CustomTSQL(TSQL):
    """
    Extended TSQL dialect configuration for sqlglot.
    Removes SET from COMMANDS to allow for more granular parsing of assignments if needed.
    """
    class Tokenizer(TSQL.Tokenizer):
        COMMANDS = TSQL.Tokenizer.COMMANDS - {TokenType.SET, TokenType.COMMAND}

class SybaseASEConnector(DatabaseConnector):
    def __init__(self, config_parser, source_or_target):
        if source_or_target != 'source':
            raise ValueError(f"Sybase ASE is only supported as a source database")

        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger
        self._udt_cache = None

    def connect(self):
        conn_type = self.config_parser.get_connectivity(self.source_or_target)
        if conn_type == 'odbc':
            connection_string = self.config_parser.get_connect_string(self.source_or_target)
            self.connection = pyodbc.connect(connection_string, autocommit=True)
        elif conn_type == 'jdbc':
            connection_string = self.config_parser.get_connect_string(self.source_or_target)
            conf = self.config_parser.get_db_config(self.source_or_target)
            self.connection = jaydebeapi.connect(
                conf['jdbc']['driver'],
                connection_string,
                [conf['username'], conf['password']],
                conf['jdbc']['libraries']
            )
        else:
            raise ValueError(f"Unsupported connectivity type: {conn_type}")

    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
        except Exception:
            pass
        finally:
            self.connection = None

    # --- Mapping Functions ---

    def get_sql_functions_mapping(self, settings):
        """Mapping of Sybase functions to PostgreSQL equivalents."""
        return {
            'getdate()': 'current_timestamp',
            'getutcdate()': "timezone('UTC', now())",
            'db_name()': 'current_database()',
            'suser_name()': 'current_user',
            'user_name()': 'current_user',
            'len(': 'length(',
            'isnull(': 'coalesce(',
            'datalength(': 'length(',
            'substring(': 'substring(',
            'charindex(': 'position(',
            'str_replace(': 'replace(',
            'stuff(': 'overlay(',
        }

    def get_types_mapping(self, settings):
        """Mapping of Sybase data types to PostgreSQL types."""
        return {
            'univarchar': 'VARCHAR', 'nvarchar': 'VARCHAR', 'unichar': 'CHAR', 'nchar': 'CHAR',
            'numeric': 'NUMERIC', 'decimal': 'NUMERIC', 'money': 'NUMERIC', 'smallmoney': 'NUMERIC',
            'int': 'INTEGER', 'integer': 'INTEGER', 'smallint': 'SMALLINT', 'tinyint': 'SMALLINT',
            'float': 'DOUBLE PRECISION', 'real': 'REAL', 'double precision': 'DOUBLE PRECISION',
            'datetime': 'TIMESTAMP', 'smalldatetime': 'TIMESTAMP',
            'bit': 'BOOLEAN',
            'image': 'BYTEA', 'binary': 'BYTEA', 'varbinary': 'BYTEA',
            'text': 'TEXT', 'unitext': 'TEXT'
        }

    # --- Conversion Entry Points ---

    def convert_funcproc_code(self, settings):
        """
        Main entry point. Converts Functions, Procedures, and Triggers.
        """
        code = settings.get('funcproc_code', '')
        if not code: return ""

        # Heuristic detection for Triggers
        upper_code = code.upper()
        is_trigger = 'CREATE TRIGGER' in upper_code or 'CREATE OR REPLACE TRIGGER' in upper_code or settings.get('object_type') == 'TRIGGER'

        try:
            return self._pipeline_convert(code, is_trigger, settings)
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Conversion failed for {settings.get('funcproc_name', 'Unknown')}: {e}")
            self.config_parser.print_log_message('ERROR', traceback.format_exc())
            # Return original code commented out on fatal error to prevent empty files
            return f"/* CRITICAL CONVERSION ERROR: {e} */\n/*\n{code}\n*/"

    def convert_trigger_code(self, settings):
        """Explicit entry point for triggers."""
        settings['funcproc_code'] = settings.get('trigger_code', settings.get('funcproc_code'))
        settings['object_type'] = 'TRIGGER'
        return self.convert_funcproc_code(settings)

    def _pipeline_convert(self, code, is_trigger, settings):
        """
        Executes the conversion pipeline:
        1. Pre-process (Regex) - Fix syntax, semicolons, assignments.
        2. Header Extraction - Get signature and body.
        3. Declarations - Extract and hoist vars.
        4. AST Parsing - Transform logic structure.
        5. Assembly - Generate PL/pgSQL.
        """
        # 1. Pre-processing
        normalized_code = self._preprocess_tsql(code)

        # 2. Variable Renaming (@var -> locvar_var)
        normalized_code = self._rename_variables(normalized_code)

        # 3. Header Extraction
        header_info = self._extract_header(normalized_code, is_trigger)
        body_code = header_info['body']

        # 4. Declarations Extraction
        body_code, declarations = self._extract_declarations(body_code, settings)

        # Ensure block wrapper for safe AST parsing
        if not re.search(r'^\s*BEGIN\b', body_code, re.IGNORECASE):
            body_code = f"BEGIN\n{body_code}\nEND"

        # 5. AST Transformation
        try:
            final_body = self._process_ast(body_code, is_trigger, declarations)
        except Exception as e:
            self.config_parser.print_log_message('WARNING', f"AST Parsing failed: {e}. Falling back to Regex processing.")
            final_body = self._fallback_regex_process(body_code, is_trigger)

        # 6. Final Assembly
        if is_trigger:
            return self._build_plpgsql_trigger(header_info, declarations, final_body, settings)
        else:
            return self._build_plpgsql_proc(header_info, declarations, final_body, settings)

    # --- Phase 1: Pre-processing & Normalization ---

    def _preprocess_tsql(self, code):
        # 1. Encapsulate comments
        code = re.sub(r'--([^\n]*)', r'/*\1*/', code)

        # 2. Remove GO / SET NOCOUNT
        code = re.sub(r'\bGO\b', '', code, flags=re.IGNORECASE)
        code = re.sub(r'\bSET\s+NOCOUNT\s+(ON|OFF)\b', r'/* SET NOCOUNT \1 */', code, flags=re.IGNORECASE)

        # 3. Handle PRINT -> Comment out (Strict Rule)
        code = re.sub(r'(?i)\bPRINT\s+(.*)', r'/* PRINT \1 */', code)

        # 4. ROLLBACK -> RAISE EXCEPTION
        code = re.sub(r'(?i)\bROLLBACK\s+(?:TRAN(?:SACTION)?\s*)?',
                      r"RAISE EXCEPTION 'Transaction Rollback Not Supported' USING ERRCODE = '99999'", code)
        code = re.sub(r'(?i)\bROLLBACK\s+TRIGGER\s+WITH\s+RAISERROR\s+(\d+)\s+(.*)',
                      r"RAISE EXCEPTION \2 USING ERRCODE = '\1'", code)

        # 5. Outer Joins (*=, =*) - Replace with placeholder function to avoid parse errors
        code = re.sub(r"([\w\.]+)\s*\*\=\s*([\w\.]+)", r"locvar_sybase_outer_join(\1, \2)", code)
        code = re.sub(r"([\w\.]+)\s*\=\*\s*([\w\.]+)", r"locvar_sybase_right_join(\1, \2)", code)

        # 6. Assignments: SELECT @v = 1 -> SET @v = 1
        # This normalizes T-SQL specific assignment SELECTs into standard SET commands.
        def select_to_set(match):
            content = match.group(1)
            # If FROM exists, it's a query or trigger select. Handle in AST phase.
            if re.search(r'\bFROM\b', content, re.IGNORECASE):
                return match.group(0)

            assignments = self._split_respecting_parens(content)
            sets = []
            for a in assignments:
                if '=' in a:
                    sets.append(f"SET {a.strip()}")
                else:
                    return match.group(0) # Fallback if structure is unclear
            return "; ".join(sets) + ";" if sets else match.group(0)

        code = re.sub(r'(?i)\bSELECT\s+(@[\w]+[\s]*=.*?)(?=\b(?:SELECT|INSERT|UPDATE|DELETE|SET|IF|WHILE|BEGIN|END|RETURN)\b|$)',
                      select_to_set, code, flags=re.DOTALL)

        # 7. Semicolon Injection
        code = self._inject_semicolons(code)

        return code

    def _inject_semicolons(self, code):
        """
        Analyzes code and injects semicolons before keywords that start new statements,
        unless the previous token implies a continuation (like IF/WHILE/ELSE).
        """
        keywords = {'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'SET', 'DECLARE', 'IF',
                    'WHILE', 'BEGIN', 'RETURN', 'EXEC', 'EXECUTE', 'RAISERROR', 'END'}

        tokens = re.split(r'(\s+|[();])', code)
        new_tokens = []
        last_was_stmt_end = True

        for t in tokens:
            if not t.strip():
                new_tokens.append(t)
                continue

            upper_t = t.upper()
            if upper_t in keywords and not last_was_stmt_end:
                 new_tokens.append(';')
                 new_tokens.append(' ')
                 last_was_stmt_end = True

            new_tokens.append(t)

            if t == ';':
                last_was_stmt_end = True
            elif upper_t in ('BEGIN', 'THEN', 'ELSE', 'IF', 'WHILE'):
                # Reset flag: these keywords expect a statement to follow, so don't inject ; before the next one
                last_was_stmt_end = True
            else:
                last_was_stmt_end = False

        return "".join(new_tokens)

    def _rename_variables(self, code):
        # Globals
        code = re.sub(r'@@rowcount', 'locvar_rowcount', code, flags=re.IGNORECASE)
        code = re.sub(r'@@error', 'SQLSTATE', code, flags=re.IGNORECASE)
        code = re.sub(r'\$\$identity', 'lastval()', code, flags=re.IGNORECASE)
        code = re.sub(r'\$\$procid', '0', code, flags=re.IGNORECASE)

        # Locals @var -> locvar_var
        vars_found = set(re.findall(r'(?<!@)@(?![@])([a-zA-Z0-9_]+)', code))
        sorted_vars = sorted(list(vars_found), key=len, reverse=True)

        for v in sorted_vars:
            pattern = re.compile(rf'(?<!@)@(?![@]){re.escape(v)}\b')
            code = pattern.sub(f"locvar_{v}", code)

        return code

    # --- Phase 2: Extraction ---

    def _extract_header(self, code, is_trigger):
        info = {'name': 'unknown', 'params': '', 'body': code, 'table': '', 'events': 'INSERT'}

        if is_trigger:
            match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\s+(?:[\w\.]+\.)?([\w_]+)\s+ON\s+(?:[\w\.]+\.)?([\w_]+)\s+FOR\s+(.*?)\s+AS', code, re.IGNORECASE | re.DOTALL)
            if match:
                info['name'] = match.group(1)
                info['table'] = match.group(2)
                info['events'] = match.group(3).upper()
                info['body'] = code[match.end():]
        else:
            match = re.search(r'CREATE\s+(?:PROC|PROCEDURE|FUNCTION)\s+(?:[\w\.]+\.)?([\w_]+)(.*?)(\bAS\b)', code, re.IGNORECASE | re.DOTALL)
            if match:
                info['name'] = match.group(1)
                info['params'] = match.group(2).strip()
                info['body'] = code[match.end(3):]

        return info

    def _extract_declarations(self, code, settings):
        declarations = []

        def replacer(match):
            block = match.group(1)
            parts = self._split_respecting_parens(block)
            for p in parts:
                p = self._map_datatype(p.strip(), settings)
                declarations.append(f"{p};")
            return ""

        # Match DECLARE until next keyword/semicolon
        pattern = r'DECLARE\s+(.*?)(?=\s+(?:SELECT|INSERT|UPDATE|DELETE|SET|IF|WHILE|BEGIN|RETURN|EXEC|/\*|$|;))'
        code = re.sub(pattern, replacer, code, flags=re.IGNORECASE | re.DOTALL)

        # Add default rowcount if used
        if 'locvar_rowcount' in code:
            declarations.insert(0, "locvar_rowcount INTEGER;")

        return code, declarations

    # --- Phase 3: AST Transformation ---

    def _process_ast(self, code, is_trigger, declarations):
        parsed = sqlglot.parse(code, read=CustomTSQL)
        final_stmts = []

        for node in parsed:
            if not node: continue

            # 1. Transform Node Structure (Tables, Assignments)
            transformed = self._transform_node(node, is_trigger)
            if not transformed: continue

            # 2. Generate SQL
            if isinstance(transformed, exp.Raw):
                sql = transformed.this
            else:
                sql = transformed.sql(dialect='postgres')

            # 3. Post-Fixes
            # Fix Assignment syntax if sqlglot output 'SET x = 1'
            if sql.startswith('SET ') and ':=' in sql:
                sql = sql.replace('SET ', '', 1)
            elif sql.startswith('SET ') and '=' in sql:
                sql = sql.replace('SET ', '', 1).replace('=', ':=', 1)

            # Remove bracket encapsulation
            sql = sql.replace('[', '"').replace(']', '"')

            # Ensure Semicolons
            sql = sql.strip()
            if not sql.endswith(';') and not sql.endswith('END IF'):
                sql += ';'

            # Inject GET DIAGNOSTICS after DML
            if isinstance(node, (exp.Insert, exp.Update, exp.Delete)):
                 sql += "\nGET DIAGNOSTICS locvar_rowcount = ROW_COUNT;"

            final_stmts.append(sql)

        return "\n".join(final_stmts)

    def _transform_node(self, node, is_trigger):
        # Trigger Specifics
        if is_trigger:
            # Map Tables (inserted -> NEW, deleted -> OLD)
            if isinstance(node, exp.Table):
                if node.name.lower() == 'inserted': return exp.Table(this=exp.Identifier(this='NEW', quoted=False))
                if node.name.lower() == 'deleted': return exp.Table(this=exp.Identifier(this='OLD', quoted=False))

            # Map Assignments from Transition Tables
            # SELECT @v = col FROM inserted  -->  v := NEW.col
            if isinstance(node, exp.Select):
                froms = node.find_all(exp.Table)
                prefix = None
                for f in froms:
                    if f.name.lower() == 'inserted': prefix = "NEW"
                    elif f.name.lower() == 'deleted': prefix = "OLD"

                if prefix:
                    assigns = []
                    # sqlglot can parse assignments in SELECT as Aliases or EQ
                    for e in node.expressions:
                        lhs, rhs = None, None
                        if isinstance(e, exp.EQ):
                            lhs, rhs = e.this, e.expression
                        elif isinstance(e, exp.Alias) and isinstance(e.this, exp.EQ):
                            lhs, rhs = e.this.this, e.this.expression

                        if lhs and rhs:
                            lhs_str = lhs.sql(dialect='postgres')
                            rhs_str = rhs.sql(dialect='postgres')
                            assigns.append(f"{lhs_str} := {prefix}.{rhs_str}")

                    if assigns:
                        return exp.Raw(this="; ".join(assigns))

        # EXEC -> PERFORM
        if isinstance(node, exp.Command) and node.this.upper() in ('EXEC', 'EXECUTE'):
            raw = node.expression.sql()
            if '=' in raw: # Assignment EXEC @v = proc
                 return exp.Raw(this=raw.replace('=', ':='))
            return exp.Raw(this=f"PERFORM {raw}")

        # Handle assignment sets: SET x = 1.
        if isinstance(node, exp.Command) and node.this.upper() == 'SET':
            expr = node.expression.sql()
            if '=' in expr:
                 return exp.Raw(this=expr.replace('=', ':=', 1))

        # RAISERROR -> RAISE EXCEPTION
        if isinstance(node, exp.Command) and node.this.upper() == 'RAISERROR':
             return exp.Raw(this=f"RAISE EXCEPTION 'Sybase Error'")

        return node

    def _fallback_regex_process(self, code, is_trigger):
        """ Minimal regex-based conversion if AST fails """
        code = re.sub(r'SELECT\s+(locvar_\w+)\s*=\s*(.*)', r'\1 := \2;', code, flags=re.IGNORECASE)
        code = re.sub(r'IF\s*\((.*)\)', r'IF \1 THEN', code, flags=re.IGNORECASE)
        if is_trigger:
             code = re.sub(r'\binserted\.', 'NEW.', code, flags=re.IGNORECASE)
             code = re.sub(r'\bdeleted\.', 'OLD.', code, flags=re.IGNORECASE)
             code = re.sub(r'\bFROM\s+inserted\b', 'FROM NEW', code, flags=re.IGNORECASE)
        return code

    # --- Step 6: Assembly ---

    def _build_plpgsql_proc(self, header, declarations, body, settings):
        name = header['name']
        schema = settings.get('target_schema', 'public')

        # Process Params
        params_str, out_count = self._convert_params(header['params'], settings)

        sql = f"CREATE OR REPLACE PROCEDURE {schema}.{name}({params_str})\n"
        sql += "LANGUAGE plpgsql\nAS $$\nDECLARE\n"
        sql += "\n".join(declarations) + "\nBEGIN\n"
        sql += body + "\nEND;\n$$;"
        return sql

    def _build_plpgsql_trigger(self, header, declarations, body, settings):
        trig_name = header['name']
        table = header['table']
        schema = settings.get('target_schema', 'public')

        events = []
        if 'INSERT' in header['events']: events.append('INSERT')
        if 'UPDATE' in header['events']: events.append('UPDATE')
        if 'DELETE' in header['events']: events.append('DELETE')
        event_str = " OR ".join(events)

        func_name = f"{trig_name}_func"

        # Function
        tf = f"CREATE OR REPLACE FUNCTION {schema}.{func_name}()\nRETURNS TRIGGER AS $$\nDECLARE\n"
        tf += "\n".join(declarations) + "\nBEGIN\n"
        tf += body
        if "RETURN" not in body.upper():
            tf += "\nRETURN NEW;"
        tf += "\nEND;\n$$ LANGUAGE plpgsql;\n\n"

        # Trigger
        tr = f"DROP TRIGGER IF EXISTS {trig_name} ON {schema}.{table};\n"
        tr += f"CREATE TRIGGER {trig_name}\nAFTER {event_str} ON {schema}.{table}\n"
        tr += f"FOR EACH ROW EXECUTE FUNCTION {schema}.{func_name}();"

        return tf + tr

    # --- Utilities ---

    def _convert_params(self, params_raw, settings):
        if not params_raw: return "", 0
        parts = self._split_respecting_parens(params_raw)
        pg_params = []
        out_count = 0

        for p in parts:
            p = p.strip().replace('@', 'p_')
            mode = "IN"
            if re.search(r'\bOUT(PUT)?\b', p, re.IGNORECASE):
                mode = "INOUT"
                out_count += 1
                p = re.sub(r'\bOUT(PUT)?\b', '', p, flags=re.IGNORECASE)

            # Type mapping
            p = self._map_datatype(p, settings)
            pg_params.append(f"{mode} {p}")

        return ", ".join(pg_params), out_count

    def _map_datatype(self, text, settings):
        m = self.get_types_mapping(settings)
        tokens = text.split()
        new_tokens = []
        for t in tokens:
            cleaned = t.lower().split('(')[0]
            if cleaned in m:
                # keep length if present unless mapping forces basic type
                if '(' in t and m[cleaned] not in ('TEXT', 'INTEGER', 'TIMESTAMP', 'BOOLEAN'):
                     new_tokens.append(t.lower().replace(cleaned, m[cleaned]))
                else:
                     new_tokens.append(m[cleaned])
            else:
                new_tokens.append(t)
        return " ".join(new_tokens)

    def _split_respecting_parens(self, text):
        parts, current, depth = [], [], 0
        in_quote = False
        quote_char = ''
        for char in text:
            if in_quote:
                current.append(char)
                if char == quote_char:
                    in_quote = False
            else:
                if char == "'" or char == '"':
                    in_quote = True
                    quote_char = char
                    current.append(char)
                elif char == '(':
                    depth += 1
                    current.append(char)
                elif char == ')':
                    depth -= 1
                    current.append(char)
                elif char == ',' and depth == 0:
                    parts.append("".join(current).strip())
                    current = []
                else:
                    current.append(char)
        if current: parts.append("".join(current).strip())
        return parts

    # Required Abstract Method Stubs
    def fetch_table_names(self, schema): return {}
    def fetch_table_columns(self, settings): return {}
    def fetch_indexes(self, settings): return {}
    def fetch_constraints(self, settings): return {}
    def fetch_funcproc_names(self, schema): return {}
    def fetch_funcproc_code(self, id): return ""
    def fetch_default_values(self, settings): return {}
    def get_create_table_sql(self, settings): return ""
    def get_create_index_sql(self, settings): return ""
    def get_create_constraint_sql(self, settings): return ""
    def migrate_sequences(self, target, settings): return True
