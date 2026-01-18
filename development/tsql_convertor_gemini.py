
import re
import sys
import os
import sqlglot
from sqlglot import exp, TokenType
from sqlglot.dialects import TSQL
from sqlglot.dialects.postgres import Postgres

class Block(exp.Expression):
    arg_types = {"expressions": True}

def block_handler(self, expression):
    statements = []
    if hasattr(expression, 'expressions'):
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
        def _parse_alias(self, this, explicit=False):
             if self._curr.token_type in (TokenType.UPDATE, TokenType.INSERT, TokenType.DELETE, TokenType.MERGE, TokenType.SET, TokenType.ELSE):
                  return this
             return super()._parse_alias(this, explicit)

        def _parse_command_custom(self):
            should_handle = False
            if self._curr and self._curr.token_type == TokenType.SET:
                 self._advance()
                 should_handle = True
            elif self._prev and self._prev.text.upper() == 'SET':
                 should_handle = True

            if should_handle:
                 expressions = []
                 balance = 0
                 while self._curr:
                      if self._curr.token_type in (TokenType.SEMICOLON, TokenType.END):
                           break
                      if balance == 0:
                           txt = self._curr.text.upper()
                           if txt in ('SELECT', 'UPDATE', 'INSERT', 'DELETE', 'BEGIN', 'IF', 'WHILE', 'RETURN', 'DECLARE', 'CREATE', 'TRUNCATE', 'GO', 'ELSE', 'SET'):
                                break
                      if self._curr.token_type == TokenType.L_PAREN:
                           balance += 1
                      elif self._curr.token_type == TokenType.R_PAREN:
                           balance -= 1
                      expressions.append(self._curr.text)
                      self._advance()
                 res = " ".join(expressions)
                 return exp.Command(this='SET', expression=exp.Literal.string(res))

            if not self._curr: return None

            # Handle PRINT and others
            if self._curr.text.upper() == 'PRINT':
                 return exp.Command(this='PRINT', expression=self._parse_conjunction())

            return self._parse_command()

        STATEMENT_PARSERS = TSQL.Parser.STATEMENT_PARSERS.copy()
        STATEMENT_PARSERS[TokenType.COMMAND] = _parse_command_custom
        STATEMENT_PARSERS[TokenType.SET] = _parse_command_custom

        def _parse_statement(self):
            if self._curr.token_type == TokenType.SET:
                 return self._parse_command_custom()
            return super()._parse_statement()

        def _parse(self, parse_method, raw_tokens, sql=None):
            self.reset()
            self.sql = sql or ""
            self._tokens = raw_tokens
            self._advance()

            expressions = []
            while self._curr:
                 stmt = parse_method(self)
                 if not stmt:
                      self.raise_error("Invalid expression / Unexpected token")
                      break
                 expressions.append(stmt)
                 self._match(TokenType.SEMICOLON)

            return expressions

        def _parse_block(self):
            if not self._match(TokenType.BEGIN): pass
            expressions = []
            loop_counter = 0
            while self._curr and self._curr.token_type != TokenType.END:
                 loop_counter += 1
                 if loop_counter > 50000: raise Exception(f"Potential Infinite Loop in _parse_block at token {self._curr}")
                 stmt = self._parse_statement()
                 if stmt: expressions.append(stmt)
                 self._match(TokenType.SEMICOLON)
            self._match(TokenType.END)
            return Block(expressions=expressions)

        def _parse_if(self):
            condition = self._parse_conjunction()
            true_stmt = self._parse_statement()
            while self._match(TokenType.SEMICOLON): pass

            matched_else = False
            if self._match(TokenType.ELSE):
                 matched_else = True
            elif self._curr and self._curr.text.upper() == 'ELSE':
                 self._advance()
                 matched_else = True

            false_stmt = self._parse_statement() if matched_else else None
            return self.expression(exp.If, this=condition, true=true_stmt, false=false_stmt)

        STATEMENT_PARSERS[TokenType.BEGIN] = lambda self: self._parse_block()
        if hasattr(TokenType, 'IF'): STATEMENT_PARSERS[getattr(TokenType, 'IF')] = lambda self: self._parse_if()

    class Generator(TSQL.Generator):
        TRANSFORMS = TSQL.Generator.TRANSFORMS.copy()
        def _block_handler(self, expression):
            stmts = []
            if hasattr(expression, 'expressions'):
                for e in expression.expressions: stmts.append(self.sql(e))
            return "\n".join(stmts)
        TRANSFORMS[Block] = _block_handler

class TSQLConverter:
    def __init__(self):
        self.types_mapping = self._get_types_mapping()

    def _get_types_mapping(self):
        return {
            'BIGDATETIME': 'TIMESTAMP', 'DATE': 'DATE', 'DATETIME': 'TIMESTAMP', 'BIGTIME': 'TIMESTAMP',
            'SMALLDATETIME': 'TIMESTAMP', 'TIME': 'TIME', 'TIMESTAMP': 'TIMESTAMP', 'BIGINT': 'BIGINT',
            'UNSIGNED BIGINT': 'BIGINT', 'INTEGER': 'INTEGER', 'INT': 'INTEGER', 'INT8': 'BIGINT',
            'UNSIGNED INT': 'INTEGER', 'UINT': 'INTEGER', 'TINYINT': 'SMALLINT', 'SMALLINT': 'SMALLINT',
            'BLOB': 'BYTEA', 'BOOLEAN': 'BOOLEAN', 'BIT': 'BOOLEAN', 'BINARY': 'BYTEA', 'VARBINARY': 'BYTEA',
            'IMAGE': 'BYTEA', 'CHAR': 'CHAR', 'NCHAR': 'CHAR', 'UNICHAR': 'CHAR', 'NVARCHAR': 'VARCHAR',
            'UNIVARCHAR': 'VARCHAR', 'TEXT': 'TEXT', 'SYSNAME': 'TEXT', 'LONGSYSNAME': 'TEXT', 'LONG VARCHAR': 'TEXT',
            'LONG NVARCHAR': 'TEXT', 'UNITEXT': 'TEXT', 'VARCHAR': 'VARCHAR', 'CLOB': 'TEXT',
            'DECIMAL': 'DECIMAL', 'DOUBLE PRECISION': 'DOUBLE PRECISION', 'FLOAT': 'FLOAT', 'INTERVAL': 'INTERVAL',
            'MONEY': 'NUMERIC(19,4)', 'SMALLMONEY': 'NUMERIC(10,4)', 'NUMERIC': 'NUMERIC', 'REAL': 'REAL'
        }

    def convert_file(self, input_path, output_path):
        try:
            with open(input_path, 'r', encoding='utf-8') as f: code = f.read()
            converted_code = self.convert_code(code)
            with open(output_path, 'w', encoding='utf-8') as f: f.write(converted_code)
            print(f"Successfully converted {input_path} to {output_path}")
        except Exception as e:
            # print(f"Error converting file: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

    def convert_code(self, code):
        is_trigger = bool(re.search(r'CREATE\s+TRIGGER', code, re.IGNORECASE))
        is_proc = bool(re.search(r'CREATE\s+(?:PROC|PROCEDURE)', code, re.IGNORECASE))
        is_func = bool(re.search(r'CREATE\s+FUNCTION', code, re.IGNORECASE))

        if is_trigger: return self._convert_trigger(code)
        elif is_proc or is_func: return self._convert_funcproc(code)
        else: return "-- [ERROR] Could not identify object type (PROCEDURE/FUNCTION/TRIGGER)"

    def _convert_funcproc(self, code):
        code = self._preprocess_code(code)
        code = self._rename_local_variables(code)

        header_match = re.search(r'CREATE\s+(?:PROC|PROCEDURE|FUNCTION)\s+([a-zA-Z0-9_\.]+)(.*?)(\bAS\b)', code, flags=re.IGNORECASE | re.DOTALL)
        if not header_match: return "-- [ERROR] Failed to parse CREATE header"

        full_name, params_str = header_match.group(1), header_match.group(2).strip()
        body_content = code[header_match.end(3):].strip()
        schema, name = "public", full_name
        if '.' in full_name: schema, name = full_name.split('.', 1)

        pg_params, output_params = self._process_params(params_str)
        pg_body, declarations = self._process_body(body_content)

        returns_clause = "RETURNS void"
        if output_params:
             if len(output_params) > 1: returns_clause = "RETURNS RECORD"
             else:
                  single_out = re.search(r'\b(?:INOUT|OUT)\s+[a-zA-Z0-9_]+\s+([a-zA-Z0-9_]+(?:\(.*\))?)', pg_params, flags=re.IGNORECASE)
                  if single_out: returns_clause = f"RETURNS {single_out.group(1)}"
                  else: returns_clause = "RETURNS RECORD"

        ddl = f"CREATE OR REPLACE FUNCTION {schema}.{name}({pg_params})\n{returns_clause} AS $$\nDECLARE\n"
        if declarations: ddl += "\n".join(declarations) + "\n"
        ddl += "BEGIN\n" + pg_body + "\nEND;\n$$ LANGUAGE plpgsql;"
        return self._post_process(ddl)

    def _convert_trigger(self, code):
        code = self._preprocess_code(code)
        code = self._rename_local_variables(code)

        header_pattern = r'CREATE\s+TRIGGER\s+([a-zA-Z0-9_\.]+)\s+ON\s+([a-zA-Z0-9_\.]+)\s+(?:FOR|AFTER)\s+(.*?)\s+AS'
        header_match = re.search(header_pattern, code, flags=re.IGNORECASE | re.DOTALL)
        if not header_match: return "-- [ERROR] Failed to parse TRIGGER header"

        trigger_name, table_name, events = header_match.group(1), header_match.group(2), header_match.group(3).strip()
        body_content = code[header_match.end(0):].strip()
        pg_events = " OR ".join([e.strip() for e in events.split(',')]).upper()

        pg_body, declarations = self._process_body(body_content, is_trigger=True)
        func_name = f"tf_{trigger_name.replace('.', '_').split('.')[-1]}"

        ddl = f"CREATE OR REPLACE FUNCTION {func_name}() RETURNS TRIGGER AS $$\nDECLARE\n"
        if declarations: ddl += "\n".join(declarations) + "\n"
        ddl += "BEGIN\n" + pg_body + "\nRETURN NEW;\nEND;\n$$ LANGUAGE plpgsql;\n\n"
        ddl += f"CREATE TRIGGER {trigger_name} AFTER {pg_events} ON {table_name}\nFOR EACH ROW EXECUTE FUNCTION {func_name}();"
        return self._post_process(ddl)

    def _preprocess_code(self, code):
        code = re.sub(r'--([^\n]*)', r'/*\1*/', code)
        code = re.sub(r'\bGO\b', '', code, flags=re.IGNORECASE)
        code = re.sub(r'#([a-zA-Z0-9_]+)', r'tt_\1', code)
        code = re.sub(r'@@rowcount\b', 'locvar_rowcount', code, flags=re.IGNORECASE)
        code = re.sub(r'@@error\b', 'locvar_error_placeholder', code, flags=re.IGNORECASE)
        code = re.sub(r'\bBREAK\b', 'EXIT', code, flags=re.IGNORECASE)
        return code

    def _rename_local_variables(self, code):
        all_words = set(re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', code))
        vars_found = set(re.findall(r'(?<!@)@(?![@])([a-zA-Z0-9_]+)', code))
        mapping = {}
        for v in vars_found:
             base_name, counter = "locvar_" + v, 1
             new_name = base_name
             while new_name in all_words:
                  new_name = f"{base_name}_{counter}"; counter += 1
             mapping[v] = new_name; all_words.add(new_name)
        for v in sorted(mapping.keys(), key=len, reverse=True):
             pattern = re.compile(rf'(?<!@)@(?![@]){re.escape(v)}\b')
             code = pattern.sub(mapping[v], code)
        return code

    def _process_params(self, params_str):
        if not params_str: return "", []
        clean_params = params_str.strip()
        while clean_params.startswith('(') and clean_params.endswith(')'): clean_params = clean_params[1:-1].strip()
        parts = self._split_respecting_parens(clean_params, ',')
        processed, output_params = [], []
        for p in parts:
             p = p.strip().replace('@', '')
             if re.search(r'\bOUTPUT\b', p, re.IGNORECASE):
                  p = re.sub(r'\bOUTPUT\b', '', p, flags=re.IGNORECASE).strip()
                  p = "INOUT " + p
                  output_params.append(p)
             for sybase, pg in self.types_mapping.items():
                  p = re.sub(rf'\b{re.escape(sybase)}\b', pg, p, flags=re.IGNORECASE)
             processed.append(p)
        return ", ".join(processed), output_params

    def _process_body(self, body_content, is_trigger=False):
        if re.match(r'^BEGIN\b', body_content, flags=re.IGNORECASE):
             body_content = re.sub(r'^BEGIN', '', body_content, count=1, flags=re.IGNORECASE).strip()
             body_content = re.sub(r'END\s*$', '', body_content, flags=re.IGNORECASE).strip()

        def simple_assignment(match):
            full_match = match.group(0)
            if 'FROM' in full_match.upper(): return full_match
            content = match.group(1).strip()
            if '=' not in content: return full_match

            parts = self._split_respecting_parens(content, ',')
            assignments, is_assignment = [], True
            for part in parts:
                part = part.strip()
                # Remove trailing semicolon even if followed by comments
                part = re.sub(r';\s*(?=$|--|/\*)', ' ', part)
                if '=' in part:
                     l, r = part.split('=', 1)
                     assignments.append(f"SET {l.strip()} = {r.strip()}")
                else: is_assignment = False

            if is_assignment and assignments: return "; ".join(assignments) + " "
            return full_match

        body_content = re.sub(r'SELECT\s+(.*?)(?=\bSELECT\b|\bINSERT\b|\bUPDATE\b|\bDELETE\b|\bIF\b|\bWHILE\b|\bBEGIN\b|\bRETURN\b|\bELSE\b|\bEND\b|$)', simple_assignment, body_content, flags=re.IGNORECASE | re.DOTALL)

        declarations = []
        def decl_replacer(match):
             content = match.group(0)[7:].strip()
             parts = self._split_respecting_parens(content, ',')
             for part in parts:
                  part = part.strip()
                  for sybase, pg in self.types_mapping.items():
                       part = re.sub(rf'\b{re.escape(sybase)}\b', pg, part, flags=re.IGNORECASE)
                  declarations.append(part + ';')
             return ''
        body_content = re.sub(r'DECLARE\s+(?![@#])[a-zA-Z0-9_].*?(?=\bBEGIN|END|IF|WHILE|SELECT|INSERT|UPDATE|DELETE|RETURN|SET|$)', decl_replacer, body_content, flags=re.IGNORECASE | re.DOTALL)

        try:
             pass
        except Exception as e:
             pass

        converted_stmts = []
        parsed = sqlglot.parse(body_content, read=CustomTSQL)
        for expression in parsed:
              if expression:
                   expression = self._transform_set(expression)
                   if is_trigger: expression = self._transform_trigger(expression)
                   sql = expression.sql(dialect='postgres')
                   if not sql.strip().endswith(';'): sql += ';'
                   converted_stmts.append(sql)

        return "\n".join(converted_stmts), declarations

    def _transform_set(self, expression):
        def transform(node):
            if isinstance(node, exp.Command) and node.this.upper() == 'SET':
                 content = node.expression.this
                 if '=' in content:
                      l, r = content.split('=', 1)
                      # Use Identifier with quoted=False to output raw x := y
                      # Strip result to clean up spaces
                      return exp.Identifier(this=f"{l.strip()} := {r.strip()}", quoted=False)
            return node
        return expression.transform(transform)

    def _transform_trigger(self, expression):
        def transform(node):
            if isinstance(node, exp.Column):
                if node.table.lower() == 'inserted': node.set('table', exp.Identifier(this='NEW', quoted=False))
                elif node.table.lower() == 'deleted': node.set('table', exp.Identifier(this='OLD', quoted=False))
                return node
            if isinstance(node, (exp.Select, exp.Update, exp.Delete)):
                 from_clause = node.args.get('from')
                 if from_clause:
                      new_froms = [f for f in from_clause.expressions if not (isinstance(f, exp.Table) and f.name.lower() in ('inserted', 'deleted'))]
                      if not new_froms: node.set('from', None)
                      else: from_clause.set('expressions', new_froms)
            return node
        return expression.transform(transform)

    def _post_process(self, ddl):
        ddl = re.sub(r';\s*;', ';', ddl)
        ddl = re.sub(r'END(\s*)$', r'END;\1', ddl)
        ddl = re.sub(r'END\s+IF(?![;])', 'END IF;', ddl)
        ddl = re.sub(r'END\s+LOOP(?![;])', 'END LOOP;', ddl)
        ddl = ddl.replace('locvar_error_placeholder', 'SQLSTATE')
        return ddl

    def _split_respecting_parens(self, text, delimiter):
        parts, current, depth = [], [], 0
        for char in text:
             if char == '(': depth += 1
             elif char == ')': depth -= 1
             if char == delimiter and depth == 0:
                  parts.append("".join(current).strip())
                  current = []
             else: current.append(char)
        if current: parts.append("".join(current).strip())
        return parts

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tsql_convertor_gemini.py <input_file> [output_file]")
        sys.exit(1)
    input_file, output_file = sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else input_file + ".converted.sql"
    TSQLConverter().convert_file(input_file, output_file)
