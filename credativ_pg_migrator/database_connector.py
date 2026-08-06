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

from abc import ABC, abstractmethod

class DatabaseConnector(ABC):
    """
    Abstract base class for database connectors.
    Each specific DB implementation must implement these methods.
    """

    def __init__(self, config_parser, source_or_target):
        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target

    @abstractmethod
    def connect(self):
        """Establishes a connection to the database."""
        pass

    def check_and_create_extension(self, extension_name: str) -> tuple:
        """
        Check if an extension exists on the target database, and attempt to create it if missing.
        Returns (success: bool, message: str).
        """
        return True, f"Extension check for '{extension_name}' is not applicable for this database type."

    @abstractmethod
    def disconnect(self):
        """Closes the connection to the database."""
        pass

    @abstractmethod
    def get_sql_functions_mapping(self, settings):
        """
        settings - dictionary with the following keys
            - target_db_type: str - target database type
        Maps SQL functions from the source database to the corresponding SQL functions in the target database.
        Example:
        { 'suser_name': 'current_user',
          'getdate': 'current_timestamp',
          '@@nestlevel': None,
          ...
        }
        If the function is not supported in the target database, it is mapped to None.
        If some function is not included in the mapping, it is understood as "function is the same in both databases"
        """
        pass

    def convert_find_in_set(self, sql_str: str) -> str:
        if not sql_str or 'find_in_set' not in sql_str.lower():
            return sql_str
        import re
        pos = 0
        while True:
            match = re.search(r'(?i)\bFIND_IN_SET\s*\(', sql_str[pos:])
            if not match:
                break
            
            start_idx = pos + match.start()
            open_paren_idx = pos + match.end() - 1

            depth = 1
            i = open_paren_idx + 1
            in_single_quote = False
            in_double_quote = False
            
            while i < len(sql_str) and depth > 0:
                char = sql_str[i]
                if char == "'" and not in_double_quote:
                    in_single_quote = not in_single_quote
                elif char == '"' and not in_single_quote:
                    in_double_quote = not in_double_quote
                elif not in_single_quote and not in_double_quote:
                    if char == '(':
                        depth += 1
                    elif char == ')':
                        depth -= 1
                i += 1

            if depth == 0:
                close_paren_idx = i - 1
                args_str = sql_str[open_paren_idx + 1:close_paren_idx]

                parts = []
                current = []
                depth = 0
                in_s_quote = False
                in_d_quote = False
                for c in args_str:
                    if c == "'" and not in_d_quote:
                        in_s_quote = not in_s_quote
                    elif c == '"' and not in_s_quote:
                        in_d_quote = not in_d_quote
                    elif not in_s_quote and not in_d_quote:
                        if c == '(':
                            depth += 1
                        elif c == ')':
                            depth -= 1
                        elif c == ',' and depth == 0:
                            parts.append(''.join(current).strip())
                            current = []
                            continue
                    current.append(c)
                if current:
                    parts.append(''.join(current).strip())

                if len(parts) == 2:
                    arg1, arg2 = parts[0], parts[1]
                    replacement = f"coalesce(array_position(string_to_array({arg2}, ','), {arg1}), 0)"
                    sql_str = sql_str[:start_idx] + replacement + sql_str[close_paren_idx + 1:]
                    pos = start_idx + len(replacement)
                else:
                    pos = close_paren_idx + 1
            else:
                pos = open_paren_idx + 1
        return sql_str

    def convert_date_extract_functions(self, sql_str: str) -> str:
        if not sql_str:
            return sql_str
        import re
        funcs = [
            ('YEAR', 'YEAR'),
            ('MONTH', 'MONTH'),
            ('DAY', 'DAY'),
            ('DAYOFMONTH', 'DAY'),
            ('HOUR', 'HOUR'),
            ('MINUTE', 'MINUTE'),
            ('SECOND', 'SECOND'),
            ('QUARTER', 'QUARTER'),
            ('WEEK', 'WEEK'),
        ]
        for func_name, extract_field in funcs:
            if func_name.lower() not in sql_str.lower():
                continue
            pos = 0
            pattern = rf'(?i)\b{func_name}\s*\('
            while True:
                match = re.search(pattern, sql_str[pos:])
                if not match:
                    break
                
                start_idx = pos + match.start()
                open_paren_idx = pos + match.end() - 1

                depth = 1
                i = open_paren_idx + 1
                in_single_quote = False
                in_double_quote = False
                
                while i < len(sql_str) and depth > 0:
                    char = sql_str[i]
                    if char == "'" and not in_double_quote:
                        in_single_quote = not in_single_quote
                    elif char == '"' and not in_single_quote:
                        in_double_quote = not in_double_quote
                    elif not in_single_quote and not in_double_quote:
                        if char == '(':
                            depth += 1
                        elif char == ')':
                            depth -= 1
                    i += 1

                if depth == 0:
                    close_paren_idx = i - 1
                    arg_str = sql_str[open_paren_idx + 1:close_paren_idx].strip()
                    parts = []
                    current = []
                    arg_depth = 0
                    in_s = False
                    in_d = False
                    for c in arg_str:
                        if c == "'" and not in_d:
                            in_s = not in_s
                        elif c == '"' and not in_s:
                            in_d = not in_d
                        elif not in_s and not in_d:
                            if c == '(':
                                arg_depth += 1
                            elif c == ')':
                                arg_depth -= 1
                            elif c == ',' and arg_depth == 0:
                                parts.append(''.join(current))
                                current = []
                                continue
                        current.append(c)
                    if current:
                        parts.append(''.join(current))

                    if len(parts) == 1 and arg_str:
                        replacement = f"EXTRACT({extract_field} FROM {arg_str})"
                        sql_str = sql_str[:start_idx] + replacement + sql_str[close_paren_idx + 1:]
                        pos = start_idx + len(replacement)
                    else:
                        pos = close_paren_idx + 1
                else:
                    pos = open_paren_idx + 1
        return sql_str

    def convert_mysql_internal_rollup_functions(self, sql_str: str) -> str:
        if not sql_str:
            return sql_str
        import re
        # 1. Convert rollup_group_item(arg1, arg2) -> arg1
        pos = 0
        while True:
            match = re.search(r'(?i)\brollup_group_item\s*\(', sql_str[pos:])
            if not match:
                break
            start_idx = pos + match.start()
            open_paren_idx = pos + match.end() - 1

            depth = 1
            i = open_paren_idx + 1
            in_single_quote = False
            in_double_quote = False
            while i < len(sql_str) and depth > 0:
                char = sql_str[i]
                if char == "'" and not in_double_quote:
                    in_single_quote = not in_single_quote
                elif char == '"' and not in_single_quote:
                    in_double_quote = not in_double_quote
                elif not in_single_quote and not in_double_quote:
                    if char == '(':
                        depth += 1
                    elif char == ')':
                        depth -= 1
                i += 1

            if depth == 0:
                close_paren_idx = i - 1
                args_str = sql_str[open_paren_idx + 1:close_paren_idx]
                parts = []
                current = []
                arg_depth = 0
                in_s = False
                in_d = False
                for c in args_str:
                    if c == "'" and not in_d:
                        in_s = not in_s
                    elif c == '"' and not in_s:
                        in_d = not in_d
                    elif not in_s and not in_d:
                        if c == '(':
                            arg_depth += 1
                        elif c == ')':
                            arg_depth -= 1
                        elif c == ',' and arg_depth == 0:
                            parts.append(''.join(current).strip())
                            current = []
                            continue
                    current.append(c)
                if current:
                    parts.append(''.join(current).strip())

                if len(parts) >= 1:
                    arg1 = parts[0]
                    replacement = arg1
                    sql_str = sql_str[:start_idx] + replacement + sql_str[close_paren_idx + 1:]
                    pos = start_idx + len(replacement)
                else:
                    pos = close_paren_idx + 1
            else:
                pos = open_paren_idx + 1

        # 2. Convert rollup_sum_switcher(arg) -> arg
        pos = 0
        while True:
            match = re.search(r'(?i)\brollup_sum_switcher\s*\(', sql_str[pos:])
            if not match:
                break
            start_idx = pos + match.start()
            open_paren_idx = pos + match.end() - 1

            depth = 1
            i = open_paren_idx + 1
            in_single_quote = False
            in_double_quote = False
            while i < len(sql_str) and depth > 0:
                char = sql_str[i]
                if char == "'" and not in_double_quote:
                    in_single_quote = not in_single_quote
                elif char == '"' and not in_single_quote:
                    in_double_quote = not in_double_quote
                elif not in_single_quote and not in_double_quote:
                    if char == '(':
                        depth += 1
                    elif char == ')':
                        depth -= 1
                i += 1

            if depth == 0:
                close_paren_idx = i - 1
                arg_str = sql_str[open_paren_idx + 1:close_paren_idx].strip()
                replacement = arg_str
                sql_str = sql_str[:start_idx] + replacement + sql_str[close_paren_idx + 1:]
                pos = start_idx + len(replacement)
            else:
                pos = open_paren_idx + 1

        return sql_str

    def convert_char_cast_to_varchar(self, sql_str: str) -> str:
        if not sql_str:
            return sql_str
        import re
        sql_str = re.sub(r'(?i)\bCAST\s*\((.*?)\s+AS\s+CHAR(?:ACTER)?\s*\(\s*(\d+)\s*\)\s*\)', r'CAST(\1 AS VARCHAR(\2))', sql_str)
        sql_str = re.sub(r'(?i)\bCAST\s*\((.*?)\s+AS\s+CHAR(?:ACTER)?\s*\)', r'CAST(\1 AS VARCHAR)', sql_str)
        return sql_str

    def convert_grouping_boolean_in_case(self, sql_str: str) -> str:
        if not sql_str:
            return sql_str
        import re
        pos = 0
        while True:
            match = re.search(r'(?i)\bWHEN\s+GROUPING\s*\(', sql_str[pos:])
            if not match:
                break
            when_start = pos + match.start()
            open_paren_idx = pos + match.end() - 1

            depth = 1
            i = open_paren_idx + 1
            in_s = False
            in_d = False
            while i < len(sql_str) and depth > 0:
                char = sql_str[i]
                if char == "'" and not in_d:
                    in_s = not in_s
                elif char == '"' and not in_s:
                    in_d = not in_d
                elif not in_s and not in_d:
                    if char == '(':
                        depth += 1
                    elif char == ')':
                        depth -= 1
                i += 1

            if depth == 0:
                close_paren_idx = i - 1
                grouping_expr = sql_str[when_start + 5:close_paren_idx + 1]
                after_grouping = sql_str[close_paren_idx + 1:]
                then_match = re.match(r'^\s+THEN\b', after_grouping, re.IGNORECASE)
                if then_match:
                    then_end = close_paren_idx + 1 + then_match.end()
                    replacement = f"WHEN {grouping_expr} = 1 THEN"
                    sql_str = sql_str[:when_start] + replacement + sql_str[then_end:]
                    pos = when_start + len(replacement)
                else:
                    pos = close_paren_idx + 1
            else:
                pos = open_paren_idx + 1
        return sql_str

    def apply_sql_functions_mapping(self, code: str, settings: dict) -> str:
        """
        Applies the SQL functions mapping to the provided code string using regular expressions.
        Uses case-insensitive replacement.
        """
        import re
        if 'target_db_type' not in settings:
            target_conn = self.config_parser.get_connectivity('target')
            settings['target_db_type'] = target_conn.get('db_type', 'postgresql') if target_conn else 'postgresql'
        if settings.get('target_db_type') == 'postgresql' and code:
            code = re.sub(r'(?i)\b(?:CHARACTER\s+SET|CHARSET)\s+[a-zA-Z0-9_]+', '', code)
            code = re.sub(r'(?i)\bCOLLATE\s+[`\'"]?[a-zA-Z0-9_]+[`\'"]?', '', code)
            code = re.sub(r'(?i)\bGROUP\s+BY\s+(.*?)\s+WITH\s+ROLLUP\b', r'GROUP BY ROLLUP (\1)', code, flags=re.DOTALL)
            code = self.convert_find_in_set(code)
            code = self.convert_date_extract_functions(code)
            code = self.convert_mysql_internal_rollup_functions(code)
            code = self.convert_char_cast_to_varchar(code)
            code = self.convert_grouping_boolean_in_case(code)
            code = self.convert_case_mixed_types(code)

        sql_functions_mapping = self.get_sql_functions_mapping(settings)
        if sql_functions_mapping and code:
            for src_func, tgt_func in sql_functions_mapping.items():
                escaped_src_func = re.escape(src_func)
                if src_func and (src_func[0].isalnum() or src_func[0] == '_') and (src_func[-1].isalnum() or src_func[-1] == '_'):
                    pattern = rf"(?i)\b{escaped_src_func}\b"
                elif src_func and (src_func[0].isalnum() or src_func[0] == '_'):
                    pattern = rf"(?i)\b{escaped_src_func}"
                else:
                    pattern = rf"(?i){escaped_src_func}"
                code = re.sub(pattern, tgt_func, code, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
        return code

    def convert_case_mixed_types(self, sql_str: str) -> str:
        if not sql_str:
            return sql_str
        import re

        def is_str_literal(val: str) -> bool:
            v = val.strip()
            return (v.startswith("'") and v.endswith("'")) or (v.startswith("N'") and v.endswith("'"))

        def is_cast_or_str(val: str) -> bool:
            v = val.strip().lower()
            return (
                is_str_literal(v)
                or v.startswith("cast(")
                or v.endswith("::text")
                or v.endswith("::varchar")
                or v.startswith("concat(")
                or v.startswith("to_char(")
                or v.startswith("coalesce(")
            )

        def replace_case(match):
            cond = match.group(1)
            then_val = match.group(2).strip()
            else_val = match.group(3).strip()

            then_str = is_str_literal(then_val)
            else_str = is_str_literal(else_val)

            if then_str and not is_cast_or_str(else_val):
                else_val = f"CAST({else_val} AS VARCHAR)"
            elif else_str and not is_cast_or_str(then_val):
                then_val = f"CAST({then_val} AS VARCHAR)"

            return f"CASE WHEN {cond} THEN {then_val} ELSE {else_val} END"

        pattern = re.compile(
            r"""(?i)\bCASE\s+WHEN\s+(.+?)\s+THEN\s+(.+?)\s+ELSE\s+(.+?)\s+END""",
            re.DOTALL
        )
        return pattern.sub(replace_case, sql_str)

    @abstractmethod
    def fetch_table_names(self, table_schema: str):
        """
        Fetch a list of table names in the specified schema.
        Returns:
        { ordinary_number: {
            'id': table_id,
            'schema_name': schema_name,
            'table_name': table_name,
            'comment': table_comment
            }
        }
        """
        pass

    @abstractmethod
    def get_table_description(self, settings) -> dict:
        """
        settings - dictionary with the following keys
            - table_schema: str,
            - table_name: str,
        Fetch a description of the table returned by the source database.
        Content depends on the database type.
        Added for better observability of the migration process.
        Returns a simple dictionary:
            - 'table_description': description of the table from the source database
        """
        pass

    @abstractmethod
    def fetch_table_columns(self, settings) -> dict:
        """
        settings - dictionary with the following keys
            - table_schema: str,
            - table_name: str,
        Returns a dictionary describing the schema of the specific table
        Items names and values correspond with INFORMATION_SCHEMA.COLUMNS table
        In case of legacy databases, content is suplied from system tables
        Columns starting with 'replaced_*' store substituted values
        Some connectors might add specific columns but these are not recognized by other connectors
        Not all columns are used in all connectors

        { column_ordinary_number: {
            'column_name':
                - full column name, in the format taken from system tables
                - can contain mix of upper and lower case letters as they are stored in system tables
            'is_nullable':
                - 'YES' / 'NO' -> 'NO' = constraint NOT NULL
            'column_default_name':
                - name of the default value from the system tables
                - relevant only for some databases, like Sybase ASE
            'column_default_value':
                - original default value from the system tables
            'replaced_column_default_value':
                - custom replacement for default value
            'data_type':
                - data type without size/length/precision/scale,
            'column_type':
                - full description of data type from table definition with all parameters,
                - like VARCHAR(255) / CHAR(11) / NUMBER(11,2)
                - this value is checked for custom replacements of data types
            'column_type_substitution':
                - custom replacement for column_type - based on the configuration file
                - contains JSON object with key-value pairs based on the configuration file
            'character_maximum_length': length of the column,
            'numeric_precision': numeric precision of the column,
            'numeric_scale': numeric scale of the column,
            'basic_data_type': basic data type for user defined types,
            'basic_character_maximum_length': basic length for user defined types,
            'basic_numeric_precision': basic precision for user defined types,
            'basic_numeric_scale': basic scale for user defined types,
            'basic_column_type': basic column type for user defined types with all parameters,
            'is_identity': 'YES' / 'NO' - automatically generated column from sequence
            'column_comment': comment for the column,
            'is_generated_virtual': 'YES' / 'NO',
            'is_generated_stored': 'YES' / 'NO',
            'generation_expression': expression for generated column,
            'stripped_generation_expression':
                - expression for generated column stripped of all the specific syntax of the source database
            'udt_schema': schema name of the user defined type,
            'udt_name': name of the user defined type,
            'domain_schema':
                - schema name of the domain
                - domains are additional checks on columns
            'domain_name':
                - name of the domain
            'is_hidden_column':
                - 'YES' / 'NO' - hidden column
                - for example hidden calculated stored column in Sybase ASE used for functional indexes
                - it is up to the target database to decide if it is relevant for migration or not
            }
        }

        ## Special notes for some databases:
        # Informix default values: https://www.ibm.com/docs/en/informix-servers/12.10?topic=tables-sysdefaults
        """
        pass

    @abstractmethod
    def fetch_default_values(self, settings) -> dict:
        """
        Relevant only for database that support independently created named default values
        settings - dictionary with the following keys
            - table_schema: str,
        Returns a dictionary describing the default values
        { ordinary_value: {
            - 'default_value_schema':
                - schema name / owner name of the default value
            - 'default_value_name'
            - 'default_value_sql'
                - original source SQL statement to create the default value in the source database
            - 'extracted_default_value':
                - plain default value extracted from the SQL statement
            - 'default_value_data_type':
                - data type of the default value - if possible to easily extract
            - 'default_value_comment'
            }
        }
        """
        pass

    @abstractmethod
    def convert_default_value(self, settings) -> dict:
        """
        settings - dictionary with the following keys
            - default_value_schema: str,
            - default_value_name: str,
            - default_value_sql: str,
            - extracted_default_value: str,
            - default_value_data_type: str,
            - default_value_comment: str,
        Returns converted default value
        """
        pass

    def strip_enclosing_parentheses(self, text: str) -> str:
        """
        Removes parentheses which enclose the whole expression - some databases,
        namely SQL Server, store defaults wrapped in several layers, e.g. ((1000.0000)).
        Pairs which do not enclose everything are kept, so (a)+(b) stays untouched.
        """
        if not text:
            return text
        value = text.strip()
        while len(value) >= 2 and value.startswith('(') and value.endswith(')'):
            depth = 0
            encloses_all = True
            in_literal = False
            for position, character in enumerate(value):
                if in_literal:
                    if character == "'":
                        in_literal = False
                    continue
                if character == "'":
                    in_literal = True
                elif character == '(':
                    depth += 1
                elif character == ')':
                    depth -= 1
                    if depth == 0 and position < len(value) - 1:
                        encloses_all = False
                        break
            if not encloses_all or depth != 0:
                break
            value = value[1:-1].strip()
        return value

    @abstractmethod
    def is_string_type(self, column_type: str) -> bool:
        """
        Check if the column type is a string type.
        Returns True if it is a string type, False otherwise.
        Legacy databases had very different types of string types, therefore this function
        """
        pass

    @abstractmethod
    def is_numeric_type(self, column_type: str) -> bool:
        """
        Check if the column type is a numeric type.
        Returns True if it is a numeric type, False otherwise.
        Legacy databases had very different types of numeric types, therefore this function
        """
        pass

    @abstractmethod
    def get_types_mapping(self, settings):
        """
        settings - dictionary with the following keys
            - target_db_type: str - target database type
        Converts the columns of one source table to the target database type and SQL syntax.
        Returns dictionary of types mapping between source and target database.
        Example:
        { 'INT': 'INTEGER',
          'VARCHAR2': 'VARCHAR',
          'DATETIME': 'TIMESTAMP',
          'CLOB': 'TEXT',
          'BLOB': 'BYTEA',
          ...
        }
        """
        pass

    @abstractmethod
    def get_create_table_sql(self, settings):
        """
        This function is currently relevant only for target database
        Centralizes creation of SQL DDL statement
        settings - dictionary with the following keys
            - target_db_type: str - target database type
            - target_schema_name: str - schema name of the table in the target database
            - target_table_name: str - table name in the source database
            - source_columns: dict - dictionary of columns to be converted
            - converted_columns: dict - dictionary of converted columns
        Returns:
          - SQL statement to create the table in the database - used for table creation
        """
        pass

    @abstractmethod
    def migrate_table(self, migrate_target_connection, settings):
        """
        Migrate a table from source to target database.
        Procedure is used inside a worker thread.
        Returns dictionary migration_stats:
        {
            'finished': bool - True if whole migration of this table is fully finished, False if not
            'rows_migrated': int - number of rows migrated in this chunk
            'source_table_rows': int - total number of rows in the source table
            'target_table_rows': int - total number of rows in the target table after this chunk
            'chunk_number': int - current chunk number
            'total_chunks': int - total number of chunks for this table
        }
        """
        pass

    @abstractmethod
    def fetch_indexes(self, settings):
        """
        Fetch indexes for a table.
        Information_schema on some databases does not contain specific table/view for indexes.
        Therefore columns names in returned dictionary are arbitrary
        settings - dictionary with the following keys
            - source_table_id:
                - internal ID of the table in the source database - if it is available
                - public internal ID does not exist for example in MySQL
            - source_table_schema:
                - schema name of the table in the source database
            - source_table_name:
                - table name in the source database
            - source_db_type:
                - type of the source database
            - source_db_version:
                - version of the source database
        Some databases use table_id for finding indexes, some need table_name and schema_name.

        Returned dictionary contains all indexes for the table - both primary and secondary indexes.
        PRIMARY KEYs are usually listed both in the indexes and constraints.
        For our purposes, we include them into indexes, because they should be created before
        references to them are used.

        Returns a dictionary:
            { ordinary_number: {
                'index_name': index_name,
                'index_type': index_type,   # INDEX, UNIQUE, PRIMARY KEY
                'index_owner': index_owner,  ## might be useful for some source databases
                'index_columns':
                    - comma separated, ordered list of columns "column_name1, column_name2, ..."
                    - from some databases like Oracle it might contain also ASC / DESC information for each column
                'index_comment': index_comment
                'index_sql':
                    - Some databases offer directly SQL statement to create the index
                    - if available, it is returned for debugging purposes
                'is_function_based':
                    - 'YES' / 'NO' - if the index is function based
                }
            }

        Notes:
        - 'index_owner':
            some source databases like Informix have a concept of system indexes,
            which are automatically created by the database engine. For example missing primary key index
            on a table if Foreign Key constraint is defined on that column.
            In this case, the owner of the index is set to 'informix' and these indexes might be confusing
            for the user because they are not defined in his data model.
        """
        pass

    @abstractmethod
    def get_create_index_sql(self, settings):
        """
        This function is currently relevant only for target database
        Centralizes creation of SQL DDL statement for indexes
        settings:
            -
        """

    @abstractmethod
    def fetch_constraints(self, settings):
        """
        settings - dictionary with the following keys
            - source_table_id: id of the table in the source database (does not exist in MySQL)
            - source_table_schema: schema name of the table in the source database
            - source_table_name: table name in the source database

        Fetch constraints for a table.
        Returns a dictionary:
            { ordinary_number: {
                'constraint_name': constraint_name:
                'constraint_type': constraint_type,
                'constraint_owner': constraint_owner,
                'constraint_columns':
                    - comma separated, ordered list of columns "column_name1, column_name2, ..."
                'referenced_table_schema':
                    - referenced_table_schema,
                    - might be empty for some databases
                'referenced_table_name': referenced_table_name,
                'referenced_columns':
                    - comma separated, ordered list of columns "column_name1, column_name2, ..."
                'constraint_sql':
                    - in case of foreigh key it might containg full DDL for the constrain from the source database
                    - if available for FK, it is returned for debugging purposes
                    - in case of check constraint in contains check expression
                'delete_rule':
                    - delete rule for foreign key - CASCADE / SET NULL / NO ACTION
                    - available only for some databases
                'update_rule':
                    - update rule for foreign key - CASCADE / SET NULL / NO ACTION
                    - available only for some databases
                'constraint_comment': constraint_comment
                'constraint_status':
                    - status of the constraint - ENABLED / DISABLED
                    - available only for some databases
                }
            }
        """
        pass

    @abstractmethod
    def get_aliases(self, settings):
        """
        Fetch all aliases from source database. PostgreSQL does not have direct equivalent for aliases.
        But we need to know them for consistency.
        settings:
            - source_schema_name
        Returns a dictionary:
            { ordinary_number: {
                'id': alias_id,
                'alias_schema_name': schema_name,
                'alias_name': alias_name,
                'aliased_schema_name': aliased_schema_name,
                'aliased_table_name': aliased_table_name,
                'alias_owner': alias_owner,
                'alias_sql': alias_sql,
                'alias_comment': alias_comment
                }
            }
        """
        pass

    @abstractmethod
    def get_create_constraint_sql(self, settings):
        """
        This function is currently relevant only for target database
        Centralizes creation of SQL DDL statement for constraints
        settings:
            -
        """
        pass

    @abstractmethod
    def fetch_triggers(self, table_id: int, table_schema: str, table_name: str):
        """
        Fetch triggers for a table.
        Returns a dictionary:
            { ordinary_number: {
                'id': trigger_id,
                'name': trigger_name:
                'event': trigger_event,
                'new': referencing_new,
                'old': referencing_old,
                'sql': create_trigger_sql,
                'comment': trigger_comment
                }
            }
        """
        pass

    @abstractmethod
    def convert_trigger(self, trig: str, settings: dict):
        pass

    @abstractmethod
    def fetch_funcproc_names(self, schema: str):
        """
        Fetch function and procedure names in the specified schema.
        Returns: dict
        { ordinary_number: {
            'name': funcproc_name:
            'id': funcproc_id,
            'type': 'FUNCTION' or 'PROCEDURE',
            'comment': funcproc_comment
            }
        }
        """
        pass

    @abstractmethod
    def fetch_funcproc_code(self, funcproc_id: int):
        """
        Fetch the code of a function or procedure.
        Returns a string with the code.
        """
        pass

    @abstractmethod
    def convert_funcproc_code(self, settings):
        """
        settings - dictionary with the following keys:
            - funcproc_code: str - code of the function or procedure in the source database
            - target_db_type: str - target database type
            - source_schema_name: str - schema name of the function or procedure in the source database
            - target_schema_name: str - schema name of the function or procedure in the target database
            - table_list: list - list of all tables in the migrated schema
            - view_list: list - list of all views in the migrated schema

        Convert function or procedure to the target database type.
        table_list - contains the list of all tables in the target schema - used for adding target_schema_name prefix to table names in the function code.
        """
        pass

    @abstractmethod
    def fetch_sequences(self, schema_name: str):
        """
        Fetch sequences for the specified schema.
        This function is only relevant for target databases that uses sequences.
        Returns: dict
        { ordinary_number: {
            'sequence_name': sequence_name:
            'id': sequence_id,
            'source_sequence_sql': source_sequence_sql
            }
        }
        """
        return {}

    def fetch_table_sequences(self, table_schema: str, table_name: str):
        """
        Fetches sequences exclusively attached to a specific table column, typically for post-data migration sequence RESETS.
        Target connection specific.
        Returns: dict
        """
        return {}


    @abstractmethod
    def get_sequence_details(self, sequence_owner, sequence_name):
        """
        Returns the details of a sequence.
        Returns: dict
        { ordinary_number: {
            'name': sequence_name:
            'min_value': min_value,
            'max_value': max_value,
            'increment_by': increment_by,
            'cycle': cycle,
            'order': order,
            'cache_size': cache_size,
            'last_value': last_value,
            'comment': sequence_comment
            }
        }
        """
        pass

    @abstractmethod
    def fetch_views_names(self, source_schema_name: str):
        """
        Fetch view names in the specified schema.
        Returns: dict
        { ordinary_number: {
            'id': view_id,
            'schema_name': schema_name,
            'view_name': view_name,
            'comment': view_comment
            }
        }
        """
        pass

    @abstractmethod
    def fetch_view_code(self, settings):
        """
        settings - dictionary with the following keys
            - view_id: id of the view in the source database (does not exist in MySQL)
            - source_schema_name: schema name of the view in the source database
            - source_view_name: view name in the source database
            - target_schema_name: target schema name
            - target_view_name: target view name
        Fetch the code of a view.
        Returns a string with the code.
        """
        pass

    @abstractmethod
    def convert_view_code(self, settings: dict):
        """
        settings - dictionary with the following keys
            - view_code: id of the view in the source database (does not exist in MySQL)
            - view_name: view name
            - schema_name: schema name
            - view_type: type of the view
        Convert view to the target database type.
        table_list - contains the list of all tables in the target schema - used for adding target_schema_name prefix to table names in the view code.
        """
        pass

    @abstractmethod
    def get_sequence_current_value(self, sequence_id: int):
        """
        Returns the current value of the sequence.
        """
        pass

    @abstractmethod
    def execute_query(self, query: str, params=None):
        """
        Executes a generic query in the connected database.
        """
        pass

    @abstractmethod
    def execute_sql_script(self, script_path: str):
        """Execute SQL script."""
        pass

    @abstractmethod
    def begin_transaction(self):
        """Begins a transaction."""
        pass

    @abstractmethod
    def commit_transaction(self):
        """Commits the current transaction."""
        pass

    @abstractmethod
    def rollback_transaction(self):
        """Rolls back the current transaction."""
        pass

    @abstractmethod
    def get_rows_count(self, table_schema: str, table_name: str, migration_limitation: str = None):
        """
        Returns a number of rows in a table
        """
        pass

    @abstractmethod
    def get_table_size(self, table_schema: str, table_name: str):
        """
        Returns a size of the table in bytes
        """
        pass

    @abstractmethod
    def get_table_next_identity(self, table_schema: str, table_name: str):
        """
        Returns the next sequence value for the table's IDENTITY column, if applicable.
        Used primarily for databases like Sybase ASE that don't expose sequences directly.
        Returns an integer if an identity is found, or None otherwise.
        """
        return None

    @abstractmethod
    def fetch_user_defined_types(self, schema: str):
        """
        Returns user defined types in the specified schema / all schemas - depending on the database.
        Returns: dict
        { ordinary_number: {
            'schema_name': schema_name,
            'type_name': type_name,
            'sql': type_sql,
            'comment': type_comment
            }
        }
        """
        pass

    @abstractmethod
    def fetch_domains(self, schema: str):
        """
        Returns domains in the specified schema / all schemas - depending on the database.
        If schema is empty, all schemas are searched.

        Returns: dict
        { ordinal_identifier: {
            'domain_schema': schema_name,
            'domain_name': domain_name,
            'source_domain_sql':
                - Original SQL statement to create the domain from the source database
                - Contains all the specific syntax of the source database
            'domain_data_type':
                - data type of the column /data type of the domain
            'source_domain_check_sql':
                - SQL statement to create the domain stript of all the specific syntax of the source database
                - Should contains only the check expression
                - This is used for creating corresponding object in the target database (in PostgreSQL it is additional CHECK constraint)
            'domain_comment': domain_comment
            }
        }
        """
        pass

    @abstractmethod
    def get_create_domain_sql(self, settings):
        """
        This function is currently relevant only for target database
        Centralizes creation of SQL DDL statement for domains
        settings:
            -
        """
        pass

    def fetch_collations(self, schema: str):
        """
        Returns user defined collations relevant for the migration.
        Most source databases do not know collations as standalone objects, therefore
        this method is optional and returns an empty dict by default.

        Returns: dict
        { ordinal_identifier: {
            'collation_schema': schema_name,
            'collation_name': collation_name,
            'collation_provider': 'icu' / 'libc' / 'builtin',
            'collation_locale': locale string (or None when lc_collate / lc_ctype are used),
            'collation_lc_collate': LC_COLLATE (or None),
            'collation_lc_ctype': LC_CTYPE (or None),
            'collation_deterministic': True / False,
            'collation_rules': ICU tailoring rules (or None),
            'collation_version': version string (or None),
            'source_collation_sql': original CREATE COLLATION statement,
            'collation_comment': comment
            }
        }
        """
        return {}

    def get_create_collation_sql(self, settings):
        """
        This function is relevant only for the target database.
        Centralizes creation of the SQL DDL statement for collations.
        Returns an empty string when the target cannot create the collation.
        """
        return ''

    def fetch_installed_extensions(self):
        """
        Extensions installed in this database. Only PostgreSQL has extensions, therefore this
        method is optional and returns an empty dict by default.

        Returns dict: extension name -> {'version': version, 'schema': schema_name}
        """
        return {}

    def fetch_available_extensions(self):
        """
        Extensions which could be installed in this database - relevant for the target.
        Returns dict: extension name -> default version.
        """
        return {}

    def fetch_extension_dependencies(self, settings):
        """
        Which extensions the objects selected for migration depend on - relevant for a
        PostgreSQL source, where a column type, an index operator class, a function or a text
        search dictionary can be provided by an extension.

        Returns dict: extension name -> list of objects requiring it.
        """
        return {}

    def fetch_text_search_objects(self, schema: str):
        """
        Returns user defined full text search objects - dictionaries and configurations.
        Only PostgreSQL knows these as standalone objects, therefore this method is
        optional and returns an empty dict by default.

        Returns: dict
        { ordinal_identifier: {
            'object_schema': schema_name,
            'object_name': object_name,
            'object_type': 'DICTIONARY' / 'CONFIGURATION',
            'template_name': schema qualified template of a dictionary,
            'init_options': option string of a dictionary,
            'parser_name': schema qualified parser of a configuration,
            'mappings': [ (token_type, [dictionary, ...]), ... ] of a configuration,
            'source_object_sql': original CREATE statement,
            'object_comment': comment
            }
        }
        """
        return {}

    def get_create_text_search_sql(self, settings):
        """
        This function is relevant only for the target database.
        Centralizes creation of the SQL DDL statements for full text search objects.
        Returns an empty string when the target cannot create the object.
        """
        return ''

    @abstractmethod
    def testing_select(self):
        """
        Simple select statement to test the connection - like "SELECT 1"
        Some databases require special form of statement
        """
        pass

    @abstractmethod
    def get_database_version(self):
        """
        Returns the version of the database.
        This is used for debugging purposes and for checking compatibility with the migrator.
        """
        pass

    @abstractmethod
    def get_database_size(self):
        """
        Returns the size of the database in bytes.
        This is used for debugging purposes and for checking compatibility with the migrator.
        """
        pass

    def get_server_version_num(self):
        """
        Returns the version of the database as a comparable integer, or None when the
        connector cannot report it. Used by the pre-migration analysis to check whether the
        target database supports what the source schema requires.
        """
        return None

    def get_generated_columns_count(self, table_schema: str) -> int:
        """
        Returns the number of generated / computed (virtual) columns in the given schema.
        Used by the pre-migration analysis to decide whether the target database must support
        generated columns. Connectors of engines that have such columns override this;
        the default 0 means "this source has none / cannot report them".
        """
        return 0

    @abstractmethod
    def get_top_n_tables(self, settings):
        """
        Settings - dictionary with the following keys
            - source_schema_name: str - schema name of the tables to be checked
        Returns a dictionary with the top N tables in the specified schema.
        The dictionary contains the following keys:
            - 'by_rows': dict - top tables by number of rows
            - 'by_size': dict - top tables by size in bytes
            - 'by_columns': dict - top tables by number of columns
            - 'by_indexes': dict - top tables by number of indexes
            - 'by_constraints': dict - top tables by number of constraints
        Each of these keys contains a dictionary with structure like this (not all keys are used in all cases):
        { ordinary_number: {
            'table_name': table_name,
            'table_schema': table_schema,
            'table_size': table_size,  # in bytes
            'table_rows': table_rows,  # number of rows
            'table_columns': table_columns,  # number of columns
            'table_indexes': table_indexes,  # number of indexes
            'table_constraints': table_constraints,  # number of constraints
            }
        """
        pass

    @abstractmethod
    def get_top_fk_dependencies(self, settings):
        """
        Fetch top foreign key dependencies in the specified schema.
        settings - dictionary with the following keys
            - source_schema_name: str - schema name of the tables to be checked
        Returns a dictionary with the top foreign key dependencies.
        Each of these keys contains a dictionary with structure like this:
        { ordinary_number: {
            'owner': owner_name,
            'table_name': table_name,
            'fk_count': foreign_key_count,
            'dependencies: list of source tables that have foreign key references to this table
            }
        }
        """
        pass

    @abstractmethod
    def target_table_exists(self, target_schema_name, target_table_name):
        """
        Check if the target table exists in the target database.
        Returns True if the table exists, False otherwise.
        """
        pass

    @abstractmethod
    def fetch_all_rows(self, query):
        """
        Fetch all rows from the database using the provided query.
        """
        pass

    def migrate_sequences(self, target_connector, settings):
        """
        Migrate sequences from source to target database.
        Returns True if successful, False otherwise.
        """
        return True

    @abstractmethod
    def get_table_checksum(self, schema_name: str, table_name: str, columns: list):
        """
        Calculates a deterministic table-level checksum hashing string aggregated values.
        cross-database compatible types usually required (e.g. TO_CHAR casts).
        Returns string or numeric hash.
        """
        pass

    @abstractmethod
    def get_random_pks(self, schema_name: str, table_name: str, pk_columns: list, sample_size: int):
        """
        Returns a random sample of Primary Key values for validation targets.
        """
        pass

    @abstractmethod
    def get_row_checksums(self, schema_name: str, table_name: str, pk_columns: list, pk_values_list: list, columns: list):
        """
        Returns corresponding row-level hashes matched directly against the provided PK filter sets.
        Returns a dictionary mapping PKs to Row Checksums.
        """
        pass

    def get_schema_indexes_count(self, schema_name: str) -> int:
        """
        Returns the total number of indexes in a given schema.
        Returns -1 if not supported or implemented.
        """
        return -1

    def get_schema_constraints_count(self, schema_name: str) -> int:
        """
        Returns the total number of constraints in a given schema.
        Returns -1 if not supported or implemented.
        """
        return -1

    def _compute_python_table_checksum(self, query: str):
        """
        Helper method to compute a deterministic, order-independent table checksum
        by fetching rows in chunks and summing their CRC32 integer hashes.
        """
        import zlib
        total_hash = 0
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            batch_size = self.config_parser.get_validation_batch_size()
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    row_str = "|".join([str(val) if val is not None else "" for val in row])
                    row_hash = zlib.crc32(row_str.encode('utf-8'))
                    total_hash += row_hash
            cursor.close()
            return total_hash
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"database_connector: _compute_python_table_checksum: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            return None

    def get_indexes_count(self, schema_name: str, table_name: str):
        """
        Returns the number of indexes on the given table.
        Default implementation returns None if unsupported.
        """
        return None

    def get_constraints_count(self, schema_name: str, table_name: str):
        """
        Returns the number of constraints on the given table.
        Default implementation returns None if unsupported.
        """
        return None

    def _compute_python_row_checksums(self, query: str, num_pk_cols: int):
        """
        Helper method to compute row-level checksums for validation sampling.
        Returns a dictionary of PKs to their CRC32 integer hashes.
        """
        import zlib
        checksums = {}
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                pk_tuple = tuple(row[:num_pk_cols])
                pk_key = pk_tuple[0] if num_pk_cols == 1 else pk_tuple
                
                # Compute hash on the non-PK columns (the rest of the row)
                data_row = row[num_pk_cols:]
                row_str = "|".join([str(val) if val is not None else "" for val in data_row])
                row_hash = zlib.crc32(row_str.encode('utf-8'))
                
                checksums[pk_key] = row_hash
            cursor.close()
            return checksums
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"database_connector: _compute_python_row_checksums: Error executing query: {query}")
            self.config_parser.print_log_message('ERROR', e)
            return {}

    @abstractmethod
    def get_lob_sizes(self, schema_name: str, table_name: str, pk_columns: list, pk_values_list: list, lob_columns: list):
        """
        Calculates internal length attributes explicitly for large objects mapping PKs against lengths.
        Returns dictionary mapping PKs to arrays of integer bounds.
        """
        pass

    def get_column_statistics(self, schema_name: str, table_name: str, column_name: str, data_type: str, force_round_0: bool = False):
        """
        Retrieves advanced column statistics for mismatched columns during validation.
        Determines the stats to retrieve based on the data_type category.
        Returns a dict:
        {
            'null_count': int,
            'empty_string_count': int,
            'min_value': str,
            'max_value': str,
            'avg_value': str
        }
        """
        dt_lower = data_type.lower()
        
        is_string = any(t in dt_lower for t in ['char', 'text', 'clob', 'string'])
        is_numeric = any(t in dt_lower for t in ['int', 'number', 'numeric', 'decimal', 'float', 'double', 'real', 'serial'])
        is_date = any(t in dt_lower for t in ['date', 'time'])
        is_lob = any(t in dt_lower for t in ['lob', 'bytea', 'image', 'xml', 'json', 'raw', 'oid', 'long'])
        is_boolean = any(t in dt_lower for t in ['bool', 'boolean'])
        
        null_sql = f"COUNT(CASE WHEN \"{column_name}\" IS NULL THEN 1 END)"
        
        empty_sql = "NULL"
        if is_string and not is_lob:
            empty_sql = f"COUNT(CASE WHEN \"{column_name}\" = '' THEN 1 END)"
            
        min_sql = "NULL"
        max_sql = "NULL"
        avg_sql = "NULL"
        
        if not is_lob:
            if is_boolean:
                min_sql = f"MIN(CAST(\"{column_name}\" AS INT))"
                max_sql = f"MAX(CAST(\"{column_name}\" AS INT))"
                avg_sql = f"AVG(CAST(\"{column_name}\" AS INT))"
            else:
                if is_numeric and force_round_0:
                    min_sql = f"ROUND(MIN(\"{column_name}\"), 0)"
                    max_sql = f"ROUND(MAX(\"{column_name}\"), 0)"
                else:
                    min_sql = f"MIN(\"{column_name}\")"
                    max_sql = f"MAX(\"{column_name}\")"
            
        if is_numeric and not is_boolean:
            avg_sql = f"AVG(CAST(\"{column_name}\" AS FLOAT))"
            
        query = f"SELECT {null_sql}, {empty_sql}, {min_sql}, {max_sql}, {avg_sql} FROM \"{schema_name}\".\"{table_name}\""
        
        stats = {
            'null_count': None,
            'empty_string_count': None,
            'min_value': None,
            'max_value': None,
            'avg_value': None
        }
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            if row:
                stats['null_count'] = int(row[0]) if row[0] is not None else None
                stats['empty_string_count'] = int(row[1]) if row[1] is not None else None
                stats['min_value'] = str(row[2]) if row[2] is not None else None
                stats['max_value'] = str(row[3]) if row[3] is not None else None
                stats['avg_value'] = str(row[4]) if row[4] is not None else None
            cursor.close()
        except Exception as e:
            self.config_parser.print_log_message('DEBUG3', f"database_connector: get_column_statistics: Failed to gather stats for {schema_name}.{table_name}.{column_name}: {e}")
            
        return stats

if __name__ == "__main__":
    print("This script is not meant to be run directly")
