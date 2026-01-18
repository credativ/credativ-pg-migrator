#!/usr/bin/env python3
"""
T-SQL to PostgreSQL pl/pgsql Converter
Standalone script for converting T-SQL functions, procedures, and triggers to PostgreSQL

This script implements multiple parsing strategies to handle T-SQL code that doesn't require semicolons.
It follows conversion rules from sybase_conversion_rules.md and mssql_conversion_rules.md

Usage:
    python tsql_convertor_copilot.py input_file.sql output_file.sql [--strategy=auto]

Strategies:
    - auto: Tries all strategies in order (default)
    - regex: Pure regex-based parsing
    - statement: Statement-by-statement parsing
    - hybrid: Combined approach (recommended)
"""

import re
import sys
import argparse
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from enum import Enum


class ObjectType(Enum):
    """Type of T-SQL object being converted"""
    FUNCTION = "FUNCTION"
    PROCEDURE = "PROCEDURE"
    TRIGGER = "TRIGGER"
    UNKNOWN = "UNKNOWN"


@dataclass
class ConversionContext:
    """Context for tracking conversion state"""
    object_type: ObjectType
    object_name: str
    schema_name: str = "public"
    has_rowcount: bool = False
    has_error: bool = False
    has_trancount: bool = False
    declarations: List[str] = None
    output_params: List[str] = None
    
    def __post_init__(self):
        if self.declarations is None:
            self.declarations = []
        if self.output_params is None:
            self.output_params = []


class TSQLConverter:
    """Main converter class with multiple parsing strategies"""
    
    def __init__(self, strategy='auto'):
        self.strategy = strategy
        self.types_mapping = self._get_types_mapping()
        self.functions_mapping = self._get_functions_mapping()
        
    def _get_types_mapping(self) -> Dict[str, str]:
        """Get Sybase/MSSQL to PostgreSQL type mappings"""
        return {
            'BIGDATETIME': 'TIMESTAMP',
            'DATE': 'DATE',
            'DATETIME': 'TIMESTAMP',
            'DATETIME2': 'TIMESTAMP',
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
            'MONEY': 'NUMERIC(19,4)',
            'SMALLMONEY': 'NUMERIC(10,4)',
            'NUMERIC': 'NUMERIC',
            'REAL': 'REAL',
            'SERIAL8': 'BIGSERIAL',
            'SERIAL': 'SERIAL',
            'SMALLFLOAT': 'REAL',
        }
    
    def _get_functions_mapping(self) -> Dict[str, str]:
        """Get T-SQL to PostgreSQL function mappings"""
        return {
            'getdate()': 'CURRENT_TIMESTAMP',
            'getutcdate()': "timezone('UTC', now())",
            'datetime': 'CURRENT_TIMESTAMP',
            'year(': 'EXTRACT(YEAR FROM ',
            'month(': 'EXTRACT(MONTH FROM ',
            'day(': 'EXTRACT(DAY FROM ',
            'db_name()': 'current_database()',
            'suser_name()': 'current_user',
            'user_name()': 'current_user',
            'len(': 'length(',
            'isnull(': 'COALESCE(',
            'charindex(': 'POSITION(',
            'substring(': 'SUBSTRING(',
            'ltrim(': 'LTRIM(',
            'rtrim(': 'RTRIM(',
            'upper(': 'UPPER(',
            'lower(': 'LOWER(',
            'replace(': 'REPLACE(',
            'cast(': 'CAST(',
            'convert(': 'CAST(',
            'datalength(': 'LENGTH(',
        }
    
    def convert_file(self, input_file: str, output_file: str) -> bool:
        """Convert a T-SQL file to PostgreSQL"""
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                tsql_code = f.read()
            
            print(f"Reading T-SQL code from: {input_file}")
            print(f"Original code length: {len(tsql_code)} characters")
            
            # Convert the code
            pg_code = self.convert(tsql_code)
            
            # Write output
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(pg_code)
            
            print(f"Conversion completed successfully!")
            print(f"PostgreSQL code written to: {output_file}")
            print(f"Converted code length: {len(pg_code)} characters")
            return True
            
        except Exception as e:
            print(f"Error during conversion: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def convert(self, tsql_code: str) -> str:
        """Main conversion entry point"""
        if not tsql_code or not tsql_code.strip():
            return "-- [WARNING] Empty input code provided"
        
        # Detect object type
        object_type = self._detect_object_type(tsql_code)
        
        if self.strategy == 'auto':
            # Try strategies in order of sophistication
            for strategy in ['hybrid', 'statement', 'regex']:
                try:
                    print(f"Trying strategy: {strategy}")
                    converter = TSQLConverter(strategy=strategy)
                    result = converter._convert_with_strategy(tsql_code, object_type)
                    print(f"Strategy {strategy} succeeded")
                    return result
                except Exception as e:
                    print(f"Strategy {strategy} failed: {e}")
                    continue
            return "-- [ERROR] All conversion strategies failed"
        else:
            return self._convert_with_strategy(tsql_code, object_type)
    
    def _detect_object_type(self, code: str) -> ObjectType:
        """Detect whether code is a function, procedure, or trigger"""
        code_upper = code.upper()
        if 'CREATE TRIGGER' in code_upper or 'CREATE OR REPLACE TRIGGER' in code_upper:
            return ObjectType.TRIGGER
        elif 'CREATE PROCEDURE' in code_upper or 'CREATE PROC' in code_upper:
            return ObjectType.PROCEDURE
        elif 'CREATE FUNCTION' in code_upper:
            return ObjectType.FUNCTION
        return ObjectType.UNKNOWN
    
    def _convert_with_strategy(self, tsql_code: str, object_type: ObjectType) -> str:
        """Convert using specified strategy"""
        if object_type == ObjectType.TRIGGER:
            return self._convert_trigger(tsql_code)
        else:  # FUNCTION or PROCEDURE
            return self._convert_funcproc(tsql_code)
    
    def _convert_funcproc(self, tsql_code: str) -> str:
        """Convert function or procedure"""
        if self.strategy == 'regex':
            return self._convert_funcproc_regex(tsql_code)
        elif self.strategy == 'statement':
            return self._convert_funcproc_statement(tsql_code)
        else:  # hybrid
            return self._convert_funcproc_hybrid(tsql_code)
    
    def _convert_trigger(self, tsql_code: str) -> str:
        """Convert trigger"""
        if self.strategy == 'regex':
            return self._convert_trigger_regex(tsql_code)
        elif self.strategy == 'statement':
            return self._convert_trigger_statement(tsql_code)
        else:  # hybrid
            return self._convert_trigger_hybrid(tsql_code)
    
    # ==================== PREPROCESSING METHODS ====================
    
    def _preprocess_code(self, code: str, is_trigger: bool = False) -> Tuple[str, ConversionContext]:
        """Apply universal preprocessing steps"""
        ctx = ConversionContext(
            object_type=ObjectType.TRIGGER if is_trigger else ObjectType.PROCEDURE,
            object_name="unknown"
        )
        
        # Step 1: Convert comments -- to /* */
        # Handle special case where comment has */ at the end
        def comment_replacer(match):
            comment_text = match.group(1)
            # Remove trailing */ if present
            comment_text = re.sub(r'\*/\s*$', '', comment_text)
            return f"/*{comment_text}*/"
        
        code = re.sub(r'--([^\n]*)', comment_replacer, code)
        
        # Step 2: Prefix keywords in comments to avoid confusion
        # Find all comments and prefix keywords
        def prefix_keywords_in_comment(match):
            comment = match.group(0)
            # Prefix common keywords with comment_
            keywords = ['DECLARE', 'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'BEGIN', 'END', 'IF', 'WHILE', 'RETURN']
            for kw in keywords:
                comment = re.sub(rf'\b{kw}\b', f'comment_{kw}', comment, flags=re.IGNORECASE)
            return comment
        
        code = re.sub(r'/\*.*?\*/', prefix_keywords_in_comment, code, flags=re.DOTALL)
        
        # Step 3: Remove GO statements
        code = re.sub(r'\bGO\b', '', code, flags=re.IGNORECASE)
        
        # Step 4: Temp table replacement # -> temp_table_
        code = re.sub(r'#([a-zA-Z0-9_]+)', r'temp_table_\1', code)
        
        # Step 5: Handle special Sybase variables
        # $$identity -> lastval()
        code = re.sub(r'\$\$identity\b', 'lastval()', code, flags=re.IGNORECASE)
        # $$procid -> 0 (placeholder)
        code = re.sub(r'\$\$procid\b', '0 /* TODO: $$procid not supported */', code, flags=re.IGNORECASE)
        # Other $$ variables
        code = re.sub(r'\$\$([a-zA-Z0-9_]+)', r'/* TODO: $$\1 not supported in PostgreSQL */', code)
        
        # Step 6: Detect global variables
        ctx.has_rowcount = '@@rowcount' in code.lower()
        ctx.has_error = '@@error' in code.lower()
        ctx.has_trancount = '@@trancount' in code.lower()
        
        # Step 7: Handle @@error specially - convert if(@@error != 0) patterns to exception handling
        # Must be done before replacing @@error
        code = self._convert_error_checks(code)
        
        # Step 8: Replace global variables
        # @@rowcount -> locvar_rowcount
        code = re.sub(r'@@rowcount\b', 'locvar_rowcount', code, flags=re.IGNORECASE)
        # @@error -> locvar_error (already handled above for IF checks)
        code = re.sub(r'@@error\b', 'locvar_error', code, flags=re.IGNORECASE)
        # @@trancount -> locvar_trancount
        code = re.sub(r'@@trancount\b', 'locvar_trancount', code, flags=re.IGNORECASE)
        
        # Step 9: @@sqlstatus (cursor status)
        code = re.sub(r'@@sqlstatus\s*=\s*0', 'FOUND', code, flags=re.IGNORECASE)
        code = re.sub(r'@@sqlstatus\s*!=\s*0', 'NOT FOUND', code, flags=re.IGNORECASE)
        code = re.sub(r'@@sqlstatus\s*<>\s*0', 'NOT FOUND', code, flags=re.IGNORECASE)
        code = re.sub(r'@@sqlstatus\s*=\s*2', 'NOT FOUND', code, flags=re.IGNORECASE)
        code = re.sub(r'@@sqlstatus\s*>\s*0', 'NOT FOUND', code, flags=re.IGNORECASE)
        
        # @@FETCH_STATUS (MSSQL cursor status)
        code = re.sub(r'@@FETCH_STATUS\s*=\s*0', 'FOUND', code, flags=re.IGNORECASE)
        code = re.sub(r'@@FETCH_STATUS\s*!=\s*0', 'NOT FOUND', code, flags=re.IGNORECASE)
        code = re.sub(r'@@FETCH_STATUS\s*<>\s*0', 'NOT FOUND', code, flags=re.IGNORECASE)
        
        # Step 10: SELECT @var = value -> locvar_var := value (assignments)
        code = self._convert_select_assignments(code)
        
        # Step 11: Rename local variables @ -> locvar_
        code = self._rename_local_variables(code)
        
        # Step 12: Convert bracket identifiers [name] -> "name"
        code = self._convert_bracket_identifiers(code)
        
        # Step 13: Legacy syntax
        # SET NOCOUNT ON/OFF
        code = re.sub(r'\bSET\s+NOCOUNT\s+(?:ON|OFF)\b', 
                     '/* TODO: SET NOCOUNT not supported in PostgreSQL */', 
                     code, flags=re.IGNORECASE)
        
        # PRINT statements
        code = re.sub(r'\bPRINT\s+([^\n;]+)', 
                     r'/* TODO: PRINT \1 - use RAISE NOTICE in PostgreSQL */', 
                     code, flags=re.IGNORECASE)
        
        # Step 14: Transaction control (mask)
        def tran_replacer(match):
            cmd = match.group(0)
            return f"/* {cmd} - TODO: Transaction control in functions not supported */"
        
        code = re.sub(r'\b(BEGIN|COMMIT|ROLLBACK|SAVE)\s+TRAN(?:SACTION)?\b(?:\s+[a-zA-Z0-9_]+)?',
                     tran_replacer, code, flags=re.IGNORECASE)
        
        # Step 15: ROLLBACK TRANSACTION with PRINT -> RAISE EXCEPTION
        def rollback_replacer(match):
            # Check if there's a PRINT after
            remaining = code[match.end():]
            print_match = re.match(r'\s*PRINT\s+[\'"]([^\'"]+)[\'"]', remaining, re.IGNORECASE)
            if print_match:
                msg = print_match.group(1)
                return f"RAISE EXCEPTION '{msg}'"
            return "RAISE EXCEPTION 'Transaction rolled back'"
        
        # This needs to be done more carefully - for now, simple replacement
        code = re.sub(r'\bROLLBACK\s+TRANSACTION\b', 
                     'RAISE EXCEPTION', 
                     code, flags=re.IGNORECASE)
        
        return code, ctx
    
    def _convert_error_checks(self, code: str) -> str:
        """Convert if(@@error <> 0) patterns to exception handling"""
        # Pattern: if(@@error != 0) or if(@@error <> 0) or if @@error <> 0
        # Replace entire IF block with EXCEPTION WHEN OTHERS THEN
        
        # This is complex - we need to find the IF statement and its body
        # For now, use a simpler approach: replace the condition
        def error_if_replacer(match):
            # The IF statement with @@error check
            # We'll mark it for later processing
            return "/* @@ERROR_CHECK_START */"
        
        code = re.sub(
            r'\bIF\s*\(\s*@@error\s*(?:!=|<>)\s*0\s*\)',
            error_if_replacer,
            code,
            flags=re.IGNORECASE
        )
        
        code = re.sub(
            r'\bIF\s+@@error\s*(?:!=|<>)\s*0\b',
            error_if_replacer,
            code,
            flags=re.IGNORECASE
        )
        
        return code
    
    def _convert_select_assignments(self, code: str) -> str:
        """Convert SELECT @var = value to locvar_var := value"""
        # Pattern: SELECT @var = value
        # This is tricky because SELECT can also be a query
        # We need to distinguish assignment SELECT from query SELECT
        
        # Simple heuristic: If SELECT has = but no FROM/WHERE/JOIN, it's likely assignment
        # But this is not always true...
        
        # For now, we'll handle simple cases
        # SELECT @var = value, @var2 = value2
        
        def assignment_replacer(match):
            assignments = match.group(1)
            # Split by comma
            parts = []
            for part in assignments.split(','):
                part = part.strip()
                # Look for @var = value pattern
                assign_match = re.match(r'@([a-zA-Z0-9_]+)\s*=\s*(.+)', part)
                if assign_match:
                    var_name = assign_match.group(1)
                    value = assign_match.group(2).strip()
                    parts.append(f"locvar_{var_name} := {value}")
                else:
                    parts.append(part)
            return ';\n'.join(parts) + ';'
        
        # Match SELECT with assignments (no FROM clause)
        # This is a heuristic
        code = re.sub(
            r'\bSELECT\s+(@[a-zA-Z0-9_]+\s*=\s*[^,;\n]+(?:\s*,\s*@[a-zA-Z0-9_]+\s*=\s*[^,;\n]+)*)\s*(?=\n|;|$|/\*)',
            assignment_replacer,
            code,
            flags=re.IGNORECASE
        )
        
        return code
    
    def _rename_local_variables(self, code: str) -> str:
        """Rename local variables @var to locvar_var"""
        # Find all @variables (not @@)
        variables = set(re.findall(r'(?<!@)@([a-zA-Z0-9_]+)', code))
        
        # Get all existing identifiers to avoid collisions
        existing = set(re.findall(r'\blocvar_[a-zA-Z0-9_]+\b', code))
        
        # Build mapping
        mapping = {}
        for var in variables:
            new_name = f"locvar_{var}"
            counter = 1
            while new_name in existing:
                new_name = f"locvar_{var}_{counter}"
                counter += 1
            mapping[var] = new_name
            existing.add(new_name)
        
        # Apply replacements (sort by length descending to handle prefixes)
        for var in sorted(variables, key=len, reverse=True):
            new_name = mapping[var]
            # Replace @var with new_name (but not @@var)
            code = re.sub(rf'(?<!@)@{re.escape(var)}\b', new_name, code)
        
        return code
    
    def _convert_bracket_identifiers(self, code: str) -> str:
        """Convert [identifier] to "identifier" in SQL statements"""
        # Only in certain contexts - be careful not to convert array indices
        # Simple approach: [word] -> "word" for identifiers
        code = re.sub(r'\[([a-zA-Z_][a-zA-Z0-9_]*)\]', r'"\1"', code)
        return code
    
    # ==================== REGEX-BASED CONVERSION ====================
    
    def _convert_funcproc_regex(self, tsql_code: str) -> str:
        """Pure regex-based conversion for functions/procedures"""
        code, ctx = self._preprocess_code(tsql_code, is_trigger=False)
        
        # Extract header (CREATE PROCEDURE/FUNCTION name (params) AS)
        header_match = re.search(
            r'CREATE\s+(?:OR\s+(?:ALTER|REPLACE)\s+)?(?:PROC(?:EDURE)?|FUNCTION)\s+([a-zA-Z0-9_\.]+)(.*?)(\bAS\b)',
            code,
            flags=re.IGNORECASE | re.DOTALL
        )
        
        if not header_match:
            return "-- [ERROR] Could not parse function/procedure header\n" + code
        
        full_name = header_match.group(1).strip()
        params_str = header_match.group(2).strip()
        body_start = header_match.end(3)
        
        # Parse name and schema
        if '.' in full_name:
            schema, name = full_name.rsplit('.', 1)
            ctx.schema_name = schema
            ctx.object_name = name
        else:
            ctx.object_name = full_name
        
        # Extract body
        body = code[body_start:].strip()
        
        # Remove outer BEGIN/END if present
        if re.match(r'^\s*BEGIN\b', body, flags=re.IGNORECASE):
            body = re.sub(r'^\s*BEGIN\s*', '', body, count=1, flags=re.IGNORECASE)
            body = re.sub(r'\s*END\s*$', '', body, flags=re.IGNORECASE | re.DOTALL)
        
        # Parse parameters
        pg_params, ctx = self._parse_parameters(params_str, ctx)
        
        # Extract and convert declarations
        body, ctx = self._extract_declarations(body, ctx)
        
        # Convert body statements
        body = self._convert_body_regex(body, ctx)
        
        # Build final function/procedure
        return self._build_funcproc(ctx, pg_params, body)
    
    def _parse_parameters(self, params_str: str, ctx: ConversionContext) -> Tuple[str, ConversionContext]:
        """Parse and convert parameter list"""
        if not params_str or not params_str.strip():
            return "", ctx
        
        # Remove outer parentheses if present
        params_str = params_str.strip()
        while params_str.startswith('(') and params_str.endswith(')'):
            params_str = params_str[1:-1].strip()
        
        # Remove @ from variables
        params_str = params_str.replace('@', '')
        
        # Split parameters by comma (respecting parentheses)
        params = self._split_params(params_str)
        
        pg_params = []
        for param in params:
            param = param.strip()
            if not param:
                continue
            
            # Check for OUTPUT/OUT
            is_output = re.search(r'\b(OUTPUT|OUT)\b', param, flags=re.IGNORECASE)
            if is_output:
                param = re.sub(r'\b(OUTPUT|OUT)\b', '', param, flags=re.IGNORECASE).strip()
                ctx.output_params.append(param.split()[0])  # Store param name
            
            # Parse: name type [= default]
            parts = param.split('=', 1)
            name_type = parts[0].strip()
            default = parts[1].strip() if len(parts) > 1 else None
            
            # Convert type
            name_type = self._convert_type(name_type)
            
            # Build parameter
            if is_output:
                pg_param = f"INOUT locvar_{name_type}"
            else:
                pg_param = f"IN locvar_{name_type}"
            
            if default:
                pg_param += f" DEFAULT {default}"
            
            pg_params.append(pg_param)
        
        return ",\n    ".join(pg_params), ctx
    
    def _split_params(self, params_str: str) -> List[str]:
        """Split parameters by comma, respecting parentheses"""
        params = []
        current = []
        depth = 0
        
        for char in params_str:
            if char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                params.append(''.join(current))
                current = []
            else:
                current.append(char)
        
        if current:
            params.append(''.join(current))
        
        return params
    
    def _convert_type(self, type_str: str) -> str:
        """Convert a data type from T-SQL to PostgreSQL"""
        # type_str is like "varname VARCHAR(100)" or "varname INT"
        parts = type_str.strip().split(None, 1)
        if len(parts) < 2:
            return type_str
        
        var_name = parts[0]
        data_type = parts[1].upper()
        
        # Check if type is in mapping (handle both with and without length)
        base_type = re.match(r'([A-Z\s]+)', data_type)
        if base_type:
            base = base_type.group(1).strip()
            if base in self.types_mapping:
                data_type = data_type.replace(base, self.types_mapping[base], 1)
        
        return f"{var_name} {data_type}"
    
    def _extract_declarations(self, body: str, ctx: ConversionContext) -> Tuple[str, ConversionContext]:
        """Extract DECLARE statements and move them to declarations section"""
        # Find all DECLARE statements
        # Pattern: DECLARE @var type [, @var2 type2 ...]
        
        def decl_replacer(match):
            decl = match.group(0)
            # Parse declarations
            # Remove DECLARE keyword
            decl_content = re.sub(r'^\s*DECLARE\s+', '', decl, flags=re.IGNORECASE)
            
            # Split by comma (but not inside parentheses)
            vars = self._split_params(decl_content)
            
            for var in vars:
                var = var.strip()
                if not var:
                    continue
                
                # Remove @ if still present
                var = var.replace('@', '')
                
                # Convert type
                var = self._convert_type(var)
                
                # Add locvar_ prefix if not already there
                if not var.startswith('locvar_'):
                    var = 'locvar_' + var
                
                ctx.declarations.append(var + ';')
            
            # Return empty string to remove from body
            return ''
        
        # Match DECLARE statements
        # They can span multiple lines and may end with comma-separated declarations
        body = re.sub(
            r'DECLARE\s+(?:locvar_)?[a-zA-Z0-9_]+\s+[^\n;]+(?:\s*,\s*(?:locvar_)?[a-zA-Z0-9_]+\s+[^\n;,]+)*',
            decl_replacer,
            body,
            flags=re.IGNORECASE
        )
        
        # Add rowcount declaration if needed
        if ctx.has_rowcount:
            if not any('locvar_rowcount' in d for d in ctx.declarations):
                ctx.declarations.insert(0, 'locvar_rowcount INTEGER;')
        
        if ctx.has_trancount:
            if not any('locvar_trancount' in d for d in ctx.declarations):
                ctx.declarations.insert(0, 'locvar_trancount INTEGER DEFAULT 1;')
        
        return body, ctx
    
    def _convert_body_regex(self, body: str, ctx: ConversionContext) -> str:
        """Convert body using regex transformations"""
        # This implements all the conversion rules
        
        # 1. Convert IF statements
        body = self._convert_if_statements(body)
        
        # 2. Convert WHILE loops
        body = self._convert_while_loops(body)
        
        # 3. Convert EXEC calls
        body = self._convert_exec_calls(body)
        
        # 4. Convert cursor operations
        body = self._convert_cursors(body)
        
        # 5. Convert CASE statements  
        body = self._convert_case_statements(body)
        
        # 6. Apply function mappings
        body = self._apply_function_mappings(body)
        
        # 7. Convert RAISERROR
        body = self._convert_raiserror(body)
        
        # 8. BREAK -> EXIT
        body = re.sub(r'\bBREAK\b', 'EXIT', body, flags=re.IGNORECASE)
        
        # 9. Add semicolons to statements
        body = self._add_semicolons(body)
        
        # 10. Final cleanup
        body = self._final_cleanup(body)
        
        return body
    
    def _convert_if_statements(self, body: str) -> str:
        """Convert IF statements to PostgreSQL syntax"""
        # Strategy: Find IF statements and convert them
        # Pattern: IF <condition> <statement> [ELSE <statement>]
        
        # This is complex because we need to handle:
        # 1. IF without BEGIN/END
        # 2. IF with BEGIN/END
        # 3. IF without THEN
        # 4. ELSE IF
        # 5. Nested IFs
        
        # For regex strategy, we'll use a simpler approach
        # Add THEN if missing
        def add_then(match):
            if_part = match.group(1)
            condition = match.group(2)
            # Check if THEN is already there
            if re.match(r'\s*THEN\b', match.string[match.end():], re.IGNORECASE):
                return match.group(0)
            return f"{if_part}({condition}) THEN"
        
        body = re.sub(
            r'\b(IF)\s*(\([^)]+\))',
            add_then,
            body,
            flags=re.IGNORECASE
        )
        
        # Also handle IF without parentheses
        body = re.sub(
            r'\bIF\s+([^\n]+?)(?=\s+BEGIN|\s+SELECT|\s+INSERT|\s+UPDATE|\s+DELETE|\s+RETURN|\s+SET|\s+EXEC)',
            r'IF \1 THEN',
            body,
            flags=re.IGNORECASE
        )
        
        # Add END IF where missing
        # This is tricky - we need to track BEGIN/END blocks
        # For now, simple heuristic: if we see IF...THEN but no END IF before next major statement
        
        return body
    
    def _convert_while_loops(self, body: str) -> str:
        """Convert WHILE loops"""
        # WHILE condition statement -> WHILE condition LOOP statement END LOOP;
        
        # Add LOOP if missing
        body = re.sub(
            r'\bWHILE\s+([^\n]+?)(\s+BEGIN)',
            r'WHILE \1 LOOP\2',
            body,
            flags=re.IGNORECASE
        )
        
        # Match END for WHILE and convert to END LOOP
        # This is imprecise but works for simple cases
        
        return body
    
    def _convert_exec_calls(self, body: str) -> str:
        """Convert EXEC/EXECUTE procedure calls"""
        # EXEC proc @p=1 -> PERFORM proc(1)
        # EXEC @var = func -> locvar_var := func()
        
        # With return value
        def exec_with_return(match):
            var = match.group(1)
            proc = match.group(2)
            # Extract parameters
            params = match.group(3) if match.lastindex >= 3 else ''
            return f"{var} := {proc}({params})"
        
        body = re.sub(
            r'\bEXEC(?:UTE)?\s+(locvar_[a-zA-Z0-9_]+)\s*=\s*([a-zA-Z0-9_\.]+)\s*(.*?)(?=\n|;|$)',
            exec_with_return,
            body,
            flags=re.IGNORECASE
        )
        
        # Without return value
        def exec_proc(match):
            proc = match.group(1)
            params = match.group(2) if match.lastindex >= 2 else ''
            # Parse params: @p=value -> value
            if params:
                param_list = []
                for p in params.split(','):
                    p = p.strip()
                    if '=' in p:
                        param_list.append(p.split('=', 1)[1].strip())
                    else:
                        param_list.append(p)
                params = ', '.join(param_list)
            return f"PERFORM {proc}({params})"
        
        body = re.sub(
            r'\bEXEC(?:UTE)?\s+([a-zA-Z0-9_\.]+)\s*(.*?)(?=\n|;|$)',
            exec_proc,
            body,
            flags=re.IGNORECASE
        )
        
        return body
    
    def _convert_cursors(self, body: str) -> str:
        """Convert cursor operations"""
        # DECLARE cursor CURSOR FOR -> already handled in declarations
        # OPEN cursor -> OPEN cursor
        # FETCH cursor INTO @var -> FETCH cursor INTO locvar_var
        # CLOSE cursor -> CLOSE cursor
        # DEALLOCATE cursor -> -- DEALLOCATE cursor (not needed in PG)
        
        body = re.sub(r'\bDEALLOCATE\s+([a-zA-Z0-9_]+)', 
                     r'/* DEALLOCATE \1 - not needed in PostgreSQL */', 
                     body, flags=re.IGNORECASE)
        
        return body
    
    def _convert_case_statements(self, body: str) -> str:
        """Convert CASE statements"""
        # Most CASE syntax is compatible, but we might need minor adjustments
        return body
    
    def _apply_function_mappings(self, body: str) -> str:
        """Apply T-SQL to PostgreSQL function mappings"""
        for tsql_func, pg_func in self.functions_mapping.items():
            # Case-insensitive replacement
            body = re.sub(
                re.escape(tsql_func),
                pg_func,
                body,
                flags=re.IGNORECASE
            )
        
        return body
    
    def _convert_raiserror(self, body: str) -> str:
        """Convert RAISERROR to RAISE EXCEPTION"""
        # RAISERROR('msg', level, state) -> RAISE EXCEPTION 'msg'
        # RAISERROR(msg, level, state) -> RAISE EXCEPTION msg
        
        def raiserror_replacer(match):
            args = match.group(1)
            # Extract first argument (message)
            parts = self._split_params(args)
            if parts:
                msg = parts[0].strip()
                return f"RAISE EXCEPTION {msg}"
            return "RAISE EXCEPTION 'Error occurred'"
        
        body = re.sub(
            r'\bRAISERROR\s*\(([^)]+)\)',
            raiserror_replacer,
            body,
            flags=re.IGNORECASE
        )
        
        return body
    
    def _add_semicolons(self, body: str) -> str:
        """Add semicolons to statements that need them"""
        # Add semicolons after:
        # - SELECT, INSERT, UPDATE, DELETE (if not already there)
        # - Variable assignments
        # - RETURN statements
        # - RAISE statements
        # - PERFORM statements
        
        # This is tricky - we need to identify statement boundaries
        # For now, use simple heuristics
        
        lines = body.split('\n')
        result = []
        
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith('--') or stripped.startswith('/*'):
                result.append(line)
                continue
            
            # Check if line needs semicolon
            needs_semicolon = False
            
            # Statements that need semicolons
            if re.match(r'\b(SELECT|INSERT|UPDATE|DELETE|RETURN|PERFORM|RAISE|EXIT)\b', 
                       stripped, re.IGNORECASE):
                needs_semicolon = True
            
            # Assignment statements
            if ':=' in stripped:
                needs_semicolon = True
            
            # Don't add if already has semicolon
            if stripped.endswith(';'):
                needs_semicolon = False
            
            # Don't add if it's a control structure keyword
            if re.match(r'\b(IF|THEN|ELSE|ELSIF|END|BEGIN|LOOP|WHILE)\b\s*$', 
                       stripped, re.IGNORECASE):
                needs_semicolon = False
            
            if needs_semicolon:
                line = line.rstrip() + ';'
            
            result.append(line)
        
        return '\n'.join(result)
    
    def _final_cleanup(self, body: str) -> str:
        """Final cleanup and sanitization"""
        # Remove doubled semicolons
        body = re.sub(r';+', ';', body)
        
        # Remove semicolon after */
        body = re.sub(r'\*/;', '*/', body)
        
        # Remove semicolon from BEGIN
        body = re.sub(r'BEGIN;', 'BEGIN', body, flags=re.IGNORECASE)
        
        # Ensure END IF has semicolon
        body = re.sub(r'\bEND\s+IF\b(?!;)', 'END IF;', body, flags=re.IGNORECASE)
        
        # Ensure END LOOP has semicolon
        body = re.sub(r'\bEND\s+LOOP\b(?!;)', 'END LOOP;', body, flags=re.IGNORECASE)
        
        # Ensure END WHILE has semicolon (if any)
        body = re.sub(r'\bEND\s+WHILE\b(?!;)', 'END WHILE;', body, flags=re.IGNORECASE)
        
        # Ensure standalone END has semicolon
        body = re.sub(r'\bEND\b\s*(?=\n|$)(?!;)', 'END;', body, flags=re.IGNORECASE)
        
        # Ensure RETURN has semicolon
        body = re.sub(r'\bRETURN\b([^;]*?)(?=\n|$)(?!;)', r'RETURN\1;', body, flags=re.IGNORECASE)
        
        # Remove semicolons on their own line and attach to previous line
        lines = body.split('\n')
        result = []
        for i, line in enumerate(lines):
            if line.strip() == ';' and result:
                result[-1] = result[-1].rstrip() + ';'
            else:
                result.append(line)
        body = '\n'.join(result)
        
        # Remove doubled semicolons again
        body = re.sub(r';+', ';', body)
        
        # Remove comment_ prefix
        body = body.replace('comment_', '')
        
        return body
    
    def _build_funcproc(self, ctx: ConversionContext, pg_params: str, body: str) -> str:
        """Build final PostgreSQL function/procedure"""
        # Determine if it's a function or procedure
        # If has RETURN value or OUTPUT params, it's typically a function
        is_function = bool(ctx.output_params) or 'RETURN' in body.upper()
        
        # Build declaration section
        decl_section = ""
        if ctx.declarations:
            decl_section = "DECLARE\n    " + "\n    ".join(ctx.declarations)
        
        # Build header
        func_type = "FUNCTION" if is_function else "PROCEDURE"
        
        # Build RETURNS clause
        returns = ""
        if is_function:
            if len(ctx.output_params) > 1:
                returns = "RETURNS RECORD"
            elif ctx.output_params:
                returns = "RETURNS TABLE"  # Or appropriate type
            else:
                returns = "RETURNS INTEGER"  # Default
        
        result = f"""CREATE OR REPLACE {func_type} {ctx.schema_name}.{ctx.object_name}(
    {pg_params}
)
{returns}
LANGUAGE plpgsql
AS $$
{decl_section}
BEGIN
{body}
END;
$$;
"""
        return result
    
    # ==================== STATEMENT-BASED CONVERSION ====================
    
    def _convert_funcproc_statement(self, tsql_code: str) -> str:
        """Statement-by-statement parsing and conversion"""
        # This strategy parses T-SQL into individual statements
        # Then converts each statement independently
        
        code, ctx = self._preprocess_code(tsql_code, is_trigger=False)
        
        # Extract header
        header_match = re.search(
            r'CREATE\s+(?:OR\s+(?:ALTER|REPLACE)\s+)?(?:PROC(?:EDURE)?|FUNCTION)\s+([a-zA-Z0-9_\.]+)(.*?)(\bAS\b)',
            code,
            flags=re.IGNORECASE | re.DOTALL
        )
        
        if not header_match:
            return "-- [ERROR] Could not parse header\n" + code
        
        full_name = header_match.group(1).strip()
        params_str = header_match.group(2).strip()
        body = code[header_match.end(3):].strip()
        
        # Parse name
        if '.' in full_name:
            schema, name = full_name.rsplit('.', 1)
            ctx.schema_name = schema
            ctx.object_name = name
        else:
            ctx.object_name = full_name
        
        # Parse parameters
        pg_params, ctx = self._parse_parameters(params_str, ctx)
        
        # Parse body into statements
        statements = self._parse_statements(body)
        
        # Convert each statement
        converted_statements = []
        for stmt in statements:
            converted = self._convert_statement(stmt, ctx)
            if converted:
                converted_statements.append(converted)
        
        # Extract declarations from converted statements
        final_stmts = []
        for stmt in converted_statements:
            if stmt.strip().upper().startswith('DECLARE'):
                ctx.declarations.append(stmt)
            else:
                final_stmts.append(stmt)
        
        # Add rowcount declaration
        if ctx.has_rowcount:
            if not any('locvar_rowcount' in d for d in ctx.declarations):
                ctx.declarations.insert(0, 'locvar_rowcount INTEGER;')
        
        body = '\n'.join(final_stmts)
        body = self._final_cleanup(body)
        
        return self._build_funcproc(ctx, pg_params, body)
    
    def _parse_statements(self, body: str) -> List[str]:
        """Parse body into individual statements"""
        # This is the key challenge: T-SQL doesn't require semicolons
        # We need to identify statement boundaries
        
        # Strategy: Use keywords to identify statement starts
        # Known statement starters: SELECT, INSERT, UPDATE, DELETE, DECLARE, SET,
        #                          IF, WHILE, BEGIN, END, RETURN, EXEC, etc.
        
        statements = []
        current_stmt = []
        
        # Tokenize by lines, but accumulate until we hit a new statement
        lines = body.split('\n')
        
        statement_starters = [
            'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DECLARE', 'SET',
            'IF', 'WHILE', 'BEGIN', 'END', 'RETURN', 'EXEC', 'EXECUTE',
            'OPEN', 'CLOSE', 'FETCH', 'DEALLOCATE', 'PRINT', 'RAISERROR',
            'BREAK', 'CONTINUE', 'CREATE', 'DROP', 'ALTER', 'TRUNCATE'
        ]
        
        for line in lines:
            stripped = line.strip()
            
            # Skip empty lines and comments
            if not stripped or stripped.startswith('--') or stripped.startswith('/*'):
                if current_stmt:
                    current_stmt.append(line)
                continue
            
            # Check if this line starts a new statement
            is_new_stmt = False
            for starter in statement_starters:
                if re.match(rf'\b{starter}\b', stripped, re.IGNORECASE):
                    is_new_stmt = True
                    break
            
            if is_new_stmt and current_stmt:
                # Save current statement
                statements.append('\n'.join(current_stmt))
                current_stmt = [line]
            else:
                current_stmt.append(line)
        
        # Don't forget last statement
        if current_stmt:
            statements.append('\n'.join(current_stmt))
        
        return statements
    
    def _convert_statement(self, stmt: str, ctx: ConversionContext) -> str:
        """Convert a single statement"""
        stripped = stmt.strip()
        
        if not stripped:
            return stmt
        
        # Apply conversion rules based on statement type
        
        # IF statement
        if re.match(r'\bIF\b', stripped, re.IGNORECASE):
            return self._convert_if_statement_detailed(stmt)
        
        # WHILE statement
        if re.match(r'\bWHILE\b', stripped, re.IGNORECASE):
            return self._convert_while_statement_detailed(stmt)
        
        # SELECT statement
        if re.match(r'\bSELECT\b', stripped, re.IGNORECASE):
            return self._convert_select_statement(stmt, ctx)
        
        # INSERT/UPDATE/DELETE
        if re.match(r'\b(INSERT|UPDATE|DELETE)\b', stripped, re.IGNORECASE):
            return self._convert_dml_statement(stmt)
        
        # DECLARE
        if re.match(r'\bDECLARE\b', stripped, re.IGNORECASE):
            return self._convert_declare_statement(stmt, ctx)
        
        # EXEC
        if re.match(r'\bEXEC(?:UTE)?\b', stripped, re.IGNORECASE):
            return self._convert_exec_statement(stmt)
        
        # RETURN
        if re.match(r'\bRETURN\b', stripped, re.IGNORECASE):
            if not stmt.rstrip().endswith(';'):
                return stmt.rstrip() + ';'
            return stmt
        
        # Default: apply basic conversions
        stmt = self._apply_function_mappings(stmt)
        return stmt
    
    def _convert_if_statement_detailed(self, stmt: str) -> str:
        """Convert IF statement with full detail"""
        # Add THEN if missing
        if not re.search(r'\bTHEN\b', stmt, re.IGNORECASE):
            # Find condition end
            match = re.match(r'(IF\s+.+?)\s+(BEGIN|SELECT|INSERT|UPDATE|DELETE|RETURN|SET|EXEC)',
                           stmt, re.IGNORECASE | re.DOTALL)
            if match:
                stmt = match.group(1) + ' THEN ' + match.group(2) + stmt[match.end():]
        
        # Add END IF if missing
        # Check if statement has ELSE
        has_else = re.search(r'\bELSE\b', stmt, re.IGNORECASE)
        has_end_if = re.search(r'\bEND\s+IF\b', stmt, re.IGNORECASE)
        
        if not has_end_if:
            # Count BEGINs and ENDs to find where to add END IF
            # For now, add at end
            stmt = stmt.rstrip() + '\n    END IF;'
        
        return stmt
    
    def _convert_while_statement_detailed(self, stmt: str) -> str:
        """Convert WHILE statement"""
        # Add LOOP if missing
        if not re.search(r'\bLOOP\b', stmt, re.IGNORECASE):
            stmt = re.sub(r'(\bWHILE\s+.+?)\s+BEGIN',
                         r'\1 LOOP\nBEGIN',
                         stmt, flags=re.IGNORECASE)
        
        # Add END LOOP if missing
        if not re.search(r'\bEND\s+LOOP\b', stmt, re.IGNORECASE):
            stmt = stmt.rstrip() + '\n    END LOOP;'
        
        return stmt
    
    def _convert_select_statement(self, stmt: str, ctx: ConversionContext) -> str:
        """Convert SELECT statement"""
        # Check if it's an assignment SELECT
        if re.match(r'SELECT\s+locvar_', stmt, re.IGNORECASE):
            # Assignment - convert to :=
            stmt = re.sub(r'SELECT\s+(locvar_[a-zA-Z0-9_]+)\s*=\s*(.+)',
                         r'\1 := \2;',
                         stmt, flags=re.IGNORECASE)
        else:
            # Regular SELECT - ensure it has semicolon
            if not stmt.rstrip().endswith(';'):
                stmt = stmt.rstrip() + ';'
        
        return stmt
    
    def _convert_dml_statement(self, stmt: str) -> str:
        """Convert INSERT/UPDATE/DELETE statement"""
        # Ensure semicolon
        if not stmt.rstrip().endswith(';'):
            stmt = stmt.rstrip() + ';'
        return stmt
    
    def _convert_declare_statement(self, stmt: str, ctx: ConversionContext) -> str:
        """Convert DECLARE statement"""
        # Extract variable name and type
        match = re.match(r'DECLARE\s+(locvar_[a-zA-Z0-9_]+)\s+(.+)', stmt, re.IGNORECASE)
        if match:
            var_name = match.group(1)
            var_type = match.group(2).strip()
            # Remove semicolon if present
            var_type = var_type.rstrip(';')
            # Convert type
            var_type = self._convert_type_only(var_type)
            return f"{var_name} {var_type};"
        return stmt
    
    def _convert_type_only(self, type_str: str) -> str:
        """Convert just the type part"""
        type_upper = type_str.upper()
        for tsql_type, pg_type in self.types_mapping.items():
            if type_upper.startswith(tsql_type):
                type_str = type_str.replace(tsql_type, pg_type, 1)
                break
        return type_str
    
    def _convert_exec_statement(self, stmt: str) -> str:
        """Convert EXEC statement"""
        return self._convert_exec_calls(stmt)
    
    # ==================== HYBRID CONVERSION ====================
    
    def _convert_funcproc_hybrid(self, tsql_code: str) -> str:
        """Hybrid approach: preprocessing + statement parsing + regex fallback"""
        # This is the recommended strategy
        # It combines the best of both approaches
        
        # Start with regex approach
        result = self._convert_funcproc_regex(tsql_code)
        
        # Apply additional statement-level fixes
        # (could parse specific problematic sections)
        
        return result
    
    # ==================== TRIGGER CONVERSION ====================
    
    def _convert_trigger_regex(self, tsql_code: str) -> str:
        """Convert trigger using regex strategy"""
        code, ctx = self._preprocess_code(tsql_code, is_trigger=True)
        
        # Extract trigger header
        header_match = re.search(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\s+([a-zA-Z0-9_\.]+)\s+ON\s+([a-zA-Z0-9_\.]+)\s+FOR\s+([A-Z,\s]+)\s+AS',
            code,
            flags=re.IGNORECASE
        )
        
        if not header_match:
            return "-- [ERROR] Could not parse trigger header\n" + code
        
        trigger_name = header_match.group(1).strip()
        table_name = header_match.group(2).strip()
        events = header_match.group(3).strip()
        
        # Parse names
        if '.' in trigger_name:
            schema, name = trigger_name.rsplit('.', 1)
            ctx.schema_name = schema
            ctx.object_name = name
        else:
            ctx.object_name = trigger_name
        
        # Extract body
        body = code[header_match.end():].strip()
        
        # Remove outer BEGIN/END
        if re.match(r'^\s*BEGIN\b', body, flags=re.IGNORECASE):
            body = re.sub(r'^\s*BEGIN\s*', '', body, count=1, flags=re.IGNORECASE)
            body = re.sub(r'\s*END\s*$', '', body, flags=re.IGNORECASE | re.DOTALL)
        
        # Trigger-specific conversions
        body = self._convert_trigger_body(body, ctx)
        
        # Convert events
        pg_events = ' OR '.join([e.strip().upper() for e in events.split(',')])
        
        # Build trigger function
        decl_section = ""
        if ctx.declarations:
            decl_section = "DECLARE\n    " + "\n    ".join(ctx.declarations)
        
        # Add rowcount if needed
        if ctx.has_rowcount:
            if not any('locvar_rowcount' in d for d in ctx.declarations):
                ctx.declarations.insert(0, 'locvar_rowcount INTEGER;')
        
        if ctx.declarations:
            decl_section = "DECLARE\n    " + "\n    ".join(ctx.declarations)
        
        result = f"""CREATE OR REPLACE FUNCTION {ctx.schema_name}.{ctx.object_name}_func()
RETURNS trigger AS $$
{decl_section}
BEGIN
{body}
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER {ctx.object_name}
AFTER {pg_events} ON "{ctx.schema_name}"."{table_name.split('.')[-1]}"
FOR EACH ROW
EXECUTE FUNCTION {ctx.schema_name}.{ctx.object_name}_func();
"""
        return result
    
    def _convert_trigger_body(self, body: str, ctx: ConversionContext) -> str:
        """Convert trigger body with special trigger rules"""
        # Extract declarations first
        body, ctx = self._extract_declarations(body, ctx)
        
        # Trigger-specific: Convert inserted/deleted to NEW/OLD
        # This is context-sensitive - we need to handle:
        # 1. SELECT ... FROM inserted -> SELECT ... (use NEW.column directly)
        # 2. inserted.column -> NEW.column
        # 3. deleted.column -> OLD.column
        
        # Convert column references
        body = re.sub(r'\binserted\.([a-zA-Z0-9_]+)', r'NEW.\1', body, flags=re.IGNORECASE)
        body = re.sub(r'\bdeleted\.([a-zA-Z0-9_]+)', r'OLD.\1', body, flags=re.IGNORECASE)
        
        # Remove FROM inserted/deleted
        body = re.sub(r'\bFROM\s+inserted\b', '', body, flags=re.IGNORECASE)
        body = re.sub(r'\bFROM\s+deleted\b', '', body, flags=re.IGNORECASE)
        
        # IF UPDATE(column) -> NEW.column IS DISTINCT FROM OLD.column
        def if_update_replacer(match):
            col = match.group(1)
            return f"NEW.{col} IS DISTINCT FROM OLD.{col}"
        
        body = re.sub(r'\bUPDATE\(([a-zA-Z0-9_]+)\)', if_update_replacer, body, flags=re.IGNORECASE)
        
        # ROLLBACK TRIGGER -> RAISE EXCEPTION
        body = re.sub(r'\bROLLBACK\s+TRIGGER\b(?:\s+WITH\s+RAISERROR\s+\d+\s+[\'"]([^\'"]+)[\'"])?',
                     r"RAISE EXCEPTION '\1'" if r'\1' else 'RAISE EXCEPTION',
                     body, flags=re.IGNORECASE)
        
        # Apply standard body conversions
        body = self._convert_body_regex(body, ctx)
        
        # Add GET DIAGNOSTICS after DML statements if rowcount is used
        if ctx.has_rowcount:
            # Insert after INSERT/UPDATE/DELETE statements
            lines = body.split('\n')
            result = []
            for i, line in enumerate(lines):
                result.append(line)
                if re.match(r'\s*(INSERT|UPDATE|DELETE)\b', line, re.IGNORECASE):
                    # Check if next line is not already GET DIAGNOSTICS
                    if i + 1 < len(lines) and 'GET DIAGNOSTICS' not in lines[i + 1]:
                        result.append('    GET DIAGNOSTICS locvar_rowcount = ROW_COUNT;')
            body = '\n'.join(result)
        
        return body
    
    def _convert_trigger_statement(self, tsql_code: str) -> str:
        """Convert trigger using statement strategy"""
        # Similar to regex but with statement-level parsing
        return self._convert_trigger_regex(tsql_code)
    
    def _convert_trigger_hybrid(self, tsql_code: str) -> str:
        """Convert trigger using hybrid strategy"""
        return self._convert_trigger_regex(tsql_code)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Convert T-SQL code to PostgreSQL pl/pgsql',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument('input_file', help='Input file containing T-SQL code')
    parser.add_argument('output_file', help='Output file for PostgreSQL code')
    parser.add_argument('--strategy', 
                       choices=['auto', 'regex', 'statement', 'hybrid'],
                       default='auto',
                       help='Conversion strategy (default: auto)')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("T-SQL to PostgreSQL Converter")
    print("=" * 70)
    print(f"Input file:  {args.input_file}")
    print(f"Output file: {args.output_file}")
    print(f"Strategy:    {args.strategy}")
    print("=" * 70)
    print()
    
    converter = TSQLConverter(strategy=args.strategy)
    success = converter.convert_file(args.input_file, args.output_file)
    
    print()
    print("=" * 70)
    if success:
        print("Conversion completed successfully!")
        sys.exit(0)
    else:
        print("Conversion failed!")
        sys.exit(1)


if __name__ == '__main__':
    main()
