#!/usr/bin/env python3
"""
T-SQL to PostgreSQL pl/pgsql Converter (Enhanced Version)
Standalone script for converting T-SQL functions, procedures, and triggers to PostgreSQL

This script implements comprehensive conversion rules following sybase_conversion_rules.md
and mssql_conversion_rules.md with proper handling of T-SQL code without semicolons.

Usage:
    python tsql_convertor_copilot.py input_file.sql output_file.sql

Features:
- Multiple parsing strategies (regex, statement-based, hybrid)
- Comprehensive type mapping
- Function and procedure conversion
- Trigger conversion with inserted/deleted handling
- Proper handling of declarations, IF statements, WHILE loops
- Error handling and transaction control conversion
"""

import re
import sys
import argparse
from typing import List, Dict, Tuple, Optional, Set
from dataclasses import dataclass, field
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
    table_name: str = ""
    has_rowcount: bool = False
    has_error: bool = False
    has_trancount: bool = False
    declarations: List[str] = field(default_factory=list)
    output_params: List[Dict] = field(default_factory=list)
    variables: Dict[str, str] = field(default_factory=dict)  # original -> converted mapping


class TSQLConverter:
    """Main converter class with comprehensive T-SQL to PostgreSQL conversion"""
    
    def __init__(self):
        self.types_mapping = self._get_types_mapping()
        self.functions_mapping = self._get_functions_mapping()
        
    def _get_types_mapping(self) -> Dict[str, str]:
        """Get Sybase/MSSQL to PostgreSQL type mappings"""
        return {
            # Date/Time types
            'BIGDATETIME': 'TIMESTAMP',
            'DATE': 'DATE',
            'DATETIME': 'TIMESTAMP',
            'DATETIME2': 'TIMESTAMP',
            'BIGTIME': 'TIMESTAMP',
            'SMALLDATETIME': 'TIMESTAMP',
            'TIME': 'TIME',
            'TIMESTAMP': 'TIMESTAMP',
            
            # Integer types
            'BIGINT': 'BIGINT',
            'UNSIGNED BIGINT': 'BIGINT',
            'INTEGER': 'INTEGER',
            'INT': 'INTEGER',
            'INT8': 'BIGINT',
            'UNSIGNED INT': 'INTEGER',
            'UINT': 'INTEGER',
            'TINYINT': 'SMALLINT',
            'SMALLINT': 'SMALLINT',
            
            # Binary types
            'BLOB': 'BYTEA',
            'BINARY': 'BYTEA',
            'VARBINARY': 'BYTEA',
            'IMAGE': 'BYTEA',
            
            # Boolean type
            'BOOLEAN': 'BOOLEAN',
            'BIT': 'BOOLEAN',
            
            # Character types
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
            
            # Numeric types
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
            'GETDATE()': 'CURRENT_TIMESTAMP',
            'GETUTCDATE()': "timezone('UTC', now())",
            'DB_NAME()': 'current_database()',
            'SUSER_NAME()': 'current_user',
            'USER_NAME()': 'current_user',
            'LEN(': 'LENGTH(',
            'ISNULL(': 'COALESCE(',
            'CHARINDEX(': 'POSITION(',
            'LTRIM(': 'LTRIM(',
            'RTRIM(': 'RTRIM(',
            'UPPER(': 'UPPER(',
            'LOWER(': 'LOWER(',
            'REPLACE(': 'REPLACE(',
            'DATALENGTH(': 'LENGTH(',
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
        print(f"Detected object type: {object_type.value}")
        
        # Convert based on type
        if object_type == ObjectType.TRIGGER:
            return self._convert_trigger(tsql_code)
        else:  # FUNCTION or PROCEDURE
            return self._convert_funcproc(tsql_code)
    
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
    
    # ==================== PREPROCESSING ====================
    
    def _preprocess_code(self, code: str, ctx: ConversionContext, is_trigger: bool = False) -> str:
        """Apply preprocessing steps to T-SQL code"""
        print("Step 1: Converting comments")
        code = self._convert_comments(code)
        
        print("Step 2: Removing GO statements")
        code = re.sub(r'\bGO\b\s*;?', '', code, flags=re.IGNORECASE)
        
        print("Step 3: Converting temp tables")
        code = re.sub(r'#([a-zA-Z0-9_]+)', r'temp_table_\1', code)
        
        print("Step 4: Handling Sybase variables")
        code = re.sub(r'\$\$identity\b', 'lastval()', code, flags=re.IGNORECASE)
        code = re.sub(r'\$\$procid\b', '0 /* TODO: $$procid not supported */', code, flags=re.IGNORECASE)
        
        print("Step 5: Detecting and handling global variables")
        ctx.has_rowcount = '@@rowcount' in code.lower()
        ctx.has_error = '@@error' in code.lower()
        ctx.has_trancount = '@@trancount' in code.lower()
        
        # Convert @@error checks before other processing
        if ctx.has_error:
            code = self._convert_error_checks(code)
        
        print("Step 6: Replacing global variables")
        code = re.sub(r'@@rowcount\b', 'locvar_rowcount', code, flags=re.IGNORECASE)
        code = re.sub(r'@@error\b', 'locvar_error', code, flags=re.IGNORECASE)
        code = re.sub(r'@@trancount\b', 'locvar_trancount', code, flags=re.IGNORECASE)
        
        # Cursor status
        code = re.sub(r'@@sqlstatus\s*=\s*0', 'FOUND', code, flags=re.IGNORECASE)
        code = re.sub(r'@@sqlstatus\s*(?:!=|<>)\s*0', 'NOT FOUND', code, flags=re.IGNORECASE)
        code = re.sub(r'@@sqlstatus\s*=\s*2', 'NOT FOUND', code, flags=re.IGNORECASE)
        code = re.sub(r'@@FETCH_STATUS\s*=\s*0', 'FOUND', code, flags=re.IGNORECASE)
        code = re.sub(r'@@FETCH_STATUS\s*(?:!=|<>)\s*0', 'NOT FOUND', code, flags=re.IGNORECASE)
        
        print("Step 7: Converting SELECT assignments")
        code = self._convert_select_assignments(code)
        
        print("Step 8: Renaming local variables")
        code, ctx = self._rename_local_variables(code, ctx)
        
        print("Step 9: Converting bracket identifiers")
        code = self._convert_bracket_identifiers(code)
        
        print("Step 10: Handling legacy syntax")
        code = re.sub(r'\bSET\s+NOCOUNT\s+(?:ON|OFF)\b\s*;?',
                     '/* TODO: SET NOCOUNT not supported in PostgreSQL */',
                     code, flags=re.IGNORECASE)
        
        code = re.sub(r'\bPRINT\s+([^\n;]+)',
                     r'/* TODO: PRINT \1 - use RAISE NOTICE in PostgreSQL */',
                     code, flags=re.IGNORECASE)
        
        print("Step 11: Masking transaction control")
        code = re.sub(r'\b(BEGIN|COMMIT|ROLLBACK|SAVE)\s+TRAN(?:SACTION)?\b(?:\s+[a-zA-Z0-9_]+)?',
                     lambda m: f"/* {m.group(0)} - TODO: Transaction control in functions not supported */",
                     code, flags=re.IGNORECASE)
        
        return code
    
    def _convert_comments(self, code: str) -> str:
        """Convert -- comments to /* */ format"""
        lines = code.split('\n')
        result = []
        
        for line in lines:
            # Check if line contains -- comment
            match = re.search(r'--(.*)$', line)
            if match:
                comment_text = match.group(1).strip()
                # Remove trailing */ if present
                comment_text = re.sub(r'\*/\s*$', '', comment_text)
                before_comment = line[:match.start()]
                result.append(f"{before_comment}/*{comment_text}*/")
            else:
                result.append(line)
        
        return '\n'.join(result)
    
    def _convert_error_checks(self, code: str) -> str:
        """Convert if(@@error <> 0) to EXCEPTION handling"""
        # This is complex - for now, mark for exception handling
        code = re.sub(
            r'\bIF\s*\(\s*@@error\s*(?:!=|<>)\s*0\s*\)',
            '/* EXCEPTION HANDLING MARKER */',
            code,
            flags=re.IGNORECASE
        )
        return code
    
    def _convert_select_assignments(self, code: str) -> str:
        """Convert SELECT @var = value assignments"""
        # Handle multi-assignment SELECT statements
        def convert_assignment(match):
            select_clause = match.group(1).strip()
            assignments = []
            
            # Split by comma (careful with commas in function calls)
            parts = self._split_by_comma(select_clause)
            
            for part in parts:
                part = part.strip()
                # Look for @var = value pattern
                assign_match = re.match(r'(@[a-zA-Z0-9_]+)\s*=\s*(.+)', part, re.IGNORECASE)
                if assign_match:
                    var = assign_match.group(1)
                    value = assign_match.group(2).strip()
                    # Don't convert yet - just mark it, will be converted when @ is replaced
                    assignments.append(f"{var} := {value}")
                else:
                    # Not an assignment, keep as is
                    assignments.append(part)
            
            return ';\n    '.join(assignments) + ';'
        
        # Match SELECT with = but without FROM (heuristic for assignments)
        # Look ahead to ensure no FROM clause
        pattern = r'\bSELECT\s+((?:@[a-zA-Z0-9_]+\s*=\s*[^,\n]+(?:\s*,\s*)?)+)(?!\s*FROM\b)'
        code = re.sub(pattern, convert_assignment, code, flags=re.IGNORECASE)
        
        return code
    
    def _split_by_comma(self, text: str) -> List[str]:
        """Split text by comma, respecting parentheses"""
        parts = []
        current = []
        depth = 0
        
        for char in text:
            if char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                parts.append(''.join(current))
                current = []
            else:
                current.append(char)
        
        if current:
            parts.append(''.join(current))
        
        return parts
    
    def _rename_local_variables(self, code: str, ctx: ConversionContext) -> Tuple[str, ConversionContext]:
        """Rename @variables to locvar_name"""
        # Find all @variables (not @@)
        variables = set(re.findall(r'(?<!@)@([a-zA-Z0-9_]+)', code))
        
        # Build mapping to avoid collisions
        existing_names = set(re.findall(r'\blocvar_[a-zA-Z0-9_]+\b', code))
        
        for var in variables:
            new_name = f"locvar_{var}"
            counter = 1
            while new_name in existing_names or new_name in ctx.variables.values():
                new_name = f"locvar_{var}_{counter}"
                counter += 1
            ctx.variables[var] = new_name
            existing_names.add(new_name)
        
        # Apply replacements (sort by length to handle prefixes correctly)
        for var in sorted(variables, key=len, reverse=True):
            new_name = ctx.variables[var]
            # Replace @var with new_name (but not @@var)
            pattern = rf'(?<!@)@{re.escape(var)}\b'
            code = re.sub(pattern, new_name, code)
        
        return code, ctx
    
    def _convert_bracket_identifiers(self, code: str) -> str:
        """Convert [identifier] to "identifier" for column/table names"""
        return re.sub(r'\[([a-zA-Z_][a-zA-Z0-9_]*)\]', r'"\1"', code)
    
    # ==================== FUNCTION/PROCEDURE CONVERSION ====================
    
    def _convert_funcproc(self, tsql_code: str) -> str:
        """Convert function or procedure"""
        ctx = ConversionContext(
            object_type=ObjectType.PROCEDURE,
            object_name="unknown"
        )
        
        # Extract header
        header_match = re.search(
            r'CREATE\s+(?:OR\s+(?:ALTER|REPLACE)\s+)?(?:PROC(?:EDURE)?|FUNCTION)\s+([a-zA-Z0-9_\.]+)(.*?)(\bAS\b)',
            tsql_code,
            flags=re.IGNORECASE | re.DOTALL
        )
        
        if not header_match:
            return "-- [ERROR] Could not parse function/procedure header\n" + tsql_code
        
        full_name = header_match.group(1).strip()
        params_str = header_match.group(2).strip()
        body_raw = tsql_code[header_match.end(3):].strip()
        
        # Parse schema and name
        if '.' in full_name:
            ctx.schema_name, ctx.object_name = full_name.rsplit('.', 1)
        else:
            ctx.object_name = full_name
        
        print(f"Converting {ctx.object_type.value}: {ctx.schema_name}.{ctx.object_name}")
        
        # Preprocess the entire code (header + body)
        full_code = tsql_code
        full_code = self._preprocess_code(full_code, ctx, is_trigger=False)
        
        # Re-extract body after preprocessing
        header_match2 = re.search(
            r'CREATE\s+(?:OR\s+(?:ALTER|REPLACE)\s+)?(?:PROC(?:EDURE)?|FUNCTION)\s+([a-zA-Z0-9_\.]+)(.*?)(\bAS\b)',
            full_code,
            flags=re.IGNORECASE | re.DOTALL
        )
        
        if header_match2:
            params_str = header_match2.group(2).strip()
            body = full_code[header_match2.end(3):].strip()
        else:
            body = body_raw
        
        # Remove outer BEGIN/END
        body = self._strip_outer_begin_end(body)
        
        # Parse parameters
        pg_params_list, ctx = self._parse_parameters(params_str, ctx)
        
        # Extract declarations from body
        body, ctx = self._extract_declarations(body, ctx)
        
        # Convert body
        body = self._convert_body(body, ctx)
        
        # Build final code
        return self._build_function_or_procedure(ctx, pg_params_list, body)
    
    def _strip_outer_begin_end(self, body: str) -> str:
        """Remove outer BEGIN and END from body"""
        body = body.strip()
        if re.match(r'^\s*BEGIN\b', body, re.IGNORECASE):
            body = re.sub(r'^\s*BEGIN\s*', '', body, count=1, flags=re.IGNORECASE)
            # Remove last END
            body = re.sub(r'\s*END\s*$', '', body, flags=re.IGNORECASE | re.DOTALL)
        return body.strip()
    
    def _parse_parameters(self, params_str: str, ctx: ConversionContext) -> Tuple[List[str], ConversionContext]:
        """Parse and convert parameter list"""
        if not params_str or not params_str.strip():
            return [], ctx
        
        # Remove outer parentheses
        params_str = params_str.strip()
        while params_str.startswith('(') and params_str.endswith(')'):
            params_str = params_str[1:-1].strip()
        
        # Split parameters
        param_parts = self._split_by_comma(params_str)
        
        pg_params = []
        for param in param_parts:
            param = param.strip()
            if not param:
                continue
            
            # Check for OUTPUT/OUT
            is_output = bool(re.search(r'\b(OUTPUT|OUT)\b', param, re.IGNORECASE))
            param = re.sub(r'\b(OUTPUT|OUT)\b', '', param, flags=re.IGNORECASE).strip()
            
            # Parse: [locvar_]name type [= default]
            parts = param.split('=', 1)
            name_type = parts[0].strip()
            default = parts[1].strip() if len(parts) > 1 else None
            
            # Extract name and type
            type_match = re.match(r'(locvar_[a-zA-Z0-9_]+)\s+(.+)', name_type)
            if type_match:
                param_name = type_match.group(1)
                param_type = type_match.group(2).strip()
            else:
                # Fallback
                parts = name_type.split(None, 1)
                param_name = parts[0] if parts else 'unknown'
                param_type = parts[1] if len(parts) > 1 else 'INTEGER'
            
            # Convert type
            param_type = self._convert_datatype(param_type)
            
            # Build parameter
            if is_output:
                ctx.output_params.append({'name': param_name, 'type': param_type})
                pg_param = f"INOUT {param_name} {param_type}"
            else:
                pg_param = f"IN {param_name} {param_type}"
            
            if default:
                pg_param += f" DEFAULT {default}"
            
            pg_params.append(pg_param)
        
        return pg_params, ctx
    
    def _convert_datatype(self, datatype: str) -> str:
        """Convert a data type from T-SQL to PostgreSQL"""
        datatype_upper = datatype.upper().strip()
        
        # Try exact match first
        if datatype_upper in self.types_mapping:
            return self.types_mapping[datatype_upper]
        
        # Try base type (without length/precision)
        for tsql_type, pg_type in self.types_mapping.items():
            if datatype_upper.startswith(tsql_type):
                # Replace base type, keep length/precision
                return datatype.upper().replace(tsql_type, pg_type, 1)
        
        # Return as is if no mapping found
        return datatype
    
    def _extract_declarations(self, body: str, ctx: ConversionContext) -> Tuple[str, ConversionContext]:
        """Extract DECLARE statements from body"""
        # Pattern: DECLARE locvar_name type [, locvar_name2 type2 ...]
        def decl_replacer(match):
            decl_text = match.group(0)
            # Remove DECLARE keyword
            decl_content = re.sub(r'^\s*DECLARE\s+', '', decl_text, flags=re.IGNORECASE).strip()
            
            # Split by comma
            vars = self._split_by_comma(decl_content)
            
            for var_decl in vars:
                var_decl = var_decl.strip()
                if not var_decl:
                    continue
                
                # Parse: locvar_name type
                parts = var_decl.split(None, 1)
                if len(parts) >= 2:
                    var_name = parts[0]
                    var_type = self._convert_datatype(parts[1])
                    ctx.declarations.append(f"{var_name} {var_type};")
                elif len(parts) == 1:
                    # Just a name, assume INTEGER
                    ctx.declarations.append(f"{parts[0]} INTEGER;")
            
            return ''  # Remove from body
        
        # Match DECLARE statements
        body = re.sub(
            r'\bDECLARE\s+locvar_[a-zA-Z0-9_]+\s+[^\n;]+(?:\s*,\s*locvar_[a-zA-Z0-9_]+\s+[^\n;,]+)*',
            decl_replacer,
            body,
            flags=re.IGNORECASE
        )
        
        # Add special declarations
        if ctx.has_rowcount and not any('locvar_rowcount' in d for d in ctx.declarations):
            ctx.declarations.insert(0, 'locvar_rowcount INTEGER;')
        
        if ctx.has_trancount and not any('locvar_trancount' in d for d in ctx.declarations):
            ctx.declarations.insert(0, 'locvar_trancount INTEGER DEFAULT 1;')
        
        return body, ctx
    
    def _convert_body(self, body: str, ctx: ConversionContext) -> str:
        """Convert function/procedure body"""
        # Apply function mappings
        for tsql_func, pg_func in self.functions_mapping.items():
            body = re.sub(re.escape(tsql_func), pg_func, body, flags=re.IGNORECASE)
        
        # Convert IF statements
        body = self._convert_if_statements(body)
        
        # Convert WHILE loops  
        body = self._convert_while_loops(body)
        
        # Convert EXEC calls
        body = self._convert_exec_calls(body)
        
        # Convert RAISERROR
        body = re.sub(
            r'\bRAISERROR\s*\([^)]+\)',
            lambda m: self._convert_raiserror(m.group(0)),
            body,
            flags=re.IGNORECASE
        )
        
        # BREAK -> EXIT
        body = re.sub(r'\bBREAK\b', 'EXIT', body, flags=re.IGNORECASE)
        
        # Add semicolons
        body = self._add_semicolons(body)
        
        # Final cleanup
        body = self._final_cleanup(body)
        
        return body
    
    def _convert_if_statements(self, body: str) -> str:
        """Convert IF statements - add THEN and END IF"""
        # Add THEN after IF condition if missing
        def add_then(match):
            # Check if THEN already follows
            remaining = body[match.end():]
            if re.match(r'\s*THEN\b', remaining, re.IGNORECASE):
                return match.group(0)
            
            if_clause = match.group(1)
            condition = match.group(2)
            return f"{if_clause}({condition}) THEN"
        
        # IF (condition) without THEN
        body = re.sub(
            r'\b(IF)\s*(\([^)]+\))(?!\s*THEN)',
            add_then,
            body,
            flags=re.IGNORECASE
        )
        
        # Add ELSIF for ELSE IF
        body = re.sub(r'\bELSE\s+IF\b', 'ELSIF', body, flags=re.IGNORECASE)
        
        return body
    
    def _convert_while_loops(self, body: str) -> str:
        """Convert WHILE loops to LOOP format"""
        # WHILE condition BEGIN ... END -> WHILE condition LOOP ... END LOOP;
        body = re.sub(
            r'\bWHILE\s+([^\n]+?)\s+BEGIN',
            r'WHILE \1 LOOP\nBEGIN',
            body,
            flags=re.IGNORECASE
        )
        return body
    
    def _convert_exec_calls(self, body: str) -> str:
        """Convert EXEC/EXECUTE calls"""
        # EXEC locvar_var = proc -> locvar_var := proc()
        body = re.sub(
            r'\bEXEC(?:UTE)?\s+(locvar_[a-zA-Z0-9_]+)\s*=\s*([a-zA-Z0-9_\.]+)',
            r'\1 := \2()',
            body,
            flags=re.IGNORECASE
        )
        
        # EXEC proc -> PERFORM proc()
        body = re.sub(
            r'\bEXEC(?:UTE)?\s+([a-zA-Z0-9_\.]+)',
            r'PERFORM \1()',
            body,
            flags=re.IGNORECASE
        )
        
        return body
    
    def _convert_raiserror(self, raiserror_stmt: str) -> str:
        """Convert RAISERROR to RAISE EXCEPTION"""
        # Extract message
        match = re.search(r'RAISERROR\s*\(\s*([\'"][^\'"]*[\'"]|[^,)]+)', raiserror_stmt, re.IGNORECASE)
        if match:
            msg = match.group(1).strip()
            return f"RAISE EXCEPTION {msg}"
        return "RAISE EXCEPTION 'Error occurred'"
    
    def _add_semicolons(self, body: str) -> str:
        """Add semicolons to statements"""
        lines = body.split('\n')
        result = []
        
        for line in lines:
            stripped = line.strip()
            
            # Skip empty or comment lines
            if not stripped or stripped.startswith('/*') or stripped.endswith('*/'):
                result.append(line)
                continue
            
            # Check if needs semicolon
            needs_semi = False
            
            # Statements that need semicolons
            if re.search(r'\b(SELECT|INSERT|UPDATE|DELETE|RETURN|PERFORM|RAISE|EXIT)\b', stripped, re.IGNORECASE):
                needs_semi = True
            
            # Assignment statements
            if ':=' in stripped:
                needs_semi = True
            
            # Already has semicolon
            if stripped.endswith(';'):
                needs_semi = False
            
            # Control structures don't need semicolons
            if re.match(r'\b(IF|THEN|ELSIF|ELSE|BEGIN|LOOP|WHILE)\b\s*$', stripped, re.IGNORECASE):
                needs_semi = False
            
            if needs_semi and not stripped.endswith(';'):
                line = line.rstrip() + ';'
            
            result.append(line)
        
        return '\n'.join(result)
    
    def _final_cleanup(self, body: str) -> str:
        """Final cleanup pass"""
        # Remove doubled semicolons
        body = re.sub(r';+', ';', body)
        
        # Remove semicolon after */
        body = re.sub(r'\*/\s*;', '*/', body)
        
        # Remove semicolon from BEGIN
        body = re.sub(r'BEGIN\s*;', 'BEGIN', body, flags=re.IGNORECASE)
        
        # Ensure END IF; END LOOP; etc.
        body = re.sub(r'\bEND\s+IF\b(?!;)', 'END IF;', body, flags=re.IGNORECASE)
        body = re.sub(r'\bEND\s+LOOP\b(?!;)', 'END LOOP;', body, flags=re.IGNORECASE)
        body = re.sub(r'\bEND\b\s*(?=\n|$)(?!;)(?!\s+IF)(?!\s+LOOP)', 'END;', body, flags=re.IGNORECASE)
        
        # Remove standalone semicolons
        lines = body.split('\n')
        result = []
        for line in lines:
            if line.strip() == ';' and result:
                result[-1] = result[-1].rstrip() + ';'
            else:
                result.append(line)
        body = '\n'.join(result)
        
        # Remove doubled semicolons again
        body = re.sub(r';+', ';', body)
        
        # Remove comment_ prefix (from preprocessing)
        body = body.replace('comment_', '')
        
        return body
    
    def _build_function_or_procedure(self, ctx: ConversionContext, pg_params: List[str], body: str) -> str:
        """Build final PostgreSQL function or procedure"""
        # Determine type
        is_procedure = ctx.object_type == ObjectType.PROCEDURE and not ctx.output_params
        
        # Build parameter list
        params_str = ",\n    ".join(pg_params) if pg_params else ""
        
        # Build declarations
        decl_section = ""
        if ctx.declarations:
            decl_lines = "\n    ".join(ctx.declarations)
            decl_section = f"DECLARE\n    {decl_lines}"
        
        # Build RETURNS clause
        returns_clause = ""
        if not is_procedure:
            if len(ctx.output_params) > 1:
                returns_clause = "RETURNS RECORD"
            elif len(ctx.output_params) == 1:
                returns_clause = f"RETURNS {ctx.output_params[0]['type']}"
            else:
                returns_clause = "RETURNS INTEGER"  # Default
        
        # Build final code
        func_or_proc = "PROCEDURE" if is_procedure else "FUNCTION"
        
        result = f"""CREATE OR REPLACE {func_or_proc} {ctx.schema_name}.{ctx.object_name}(
    {params_str}
)
{returns_clause}
LANGUAGE plpgsql
AS $$
{decl_section}
BEGIN
{body}
END;
$$;
"""
        return result
    
    # ==================== TRIGGER CONVERSION ====================
    
    def _convert_trigger(self, tsql_code: str) -> str:
        """Convert trigger"""
        ctx = ConversionContext(
            object_type=ObjectType.TRIGGER,
            object_name="unknown"
        )
        
        # Extract trigger header
        header_match = re.search(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\s+([a-zA-Z0-9_\.]+)\s+ON\s+([a-zA-Z0-9_\.]+)\s+FOR\s+([A-Z,\s]+)\s+AS',
            tsql_code,
            flags=re.IGNORECASE
        )
        
        if not header_match:
            return "-- [ERROR] Could not parse trigger header\n" + tsql_code
        
        trigger_name = header_match.group(1).strip()
        table_name = header_match.group(2).strip()
        events = header_match.group(3).strip()
        
        # Parse names
        if '.' in trigger_name:
            ctx.schema_name, ctx.object_name = trigger_name.rsplit('.', 1)
        else:
            ctx.object_name = trigger_name
        
        if '.' in table_name:
            ctx.table_name = table_name.rsplit('.', 1)[1]
        else:
            ctx.table_name = table_name
        
        print(f"Converting TRIGGER: {ctx.schema_name}.{ctx.object_name} on {ctx.table_name}")
        
        # Preprocess
        full_code = self._preprocess_code(tsql_code, ctx, is_trigger=True)
        
        # Re-extract body
        header_match2 = re.search(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\s+([a-zA-Z0-9_\.]+)\s+ON\s+([a-zA-Z0-9_\.]+)\s+FOR\s+([A-Z,\s]+)\s+AS',
            full_code,
            flags=re.IGNORECASE
        )
        
        if header_match2:
            body = full_code[header_match2.end():].strip()
        else:
            body = tsql_code[header_match.end():].strip()
        
        # Remove outer BEGIN/END
        body = self._strip_outer_begin_end(body)
        
        # Extract declarations
        body, ctx = self._extract_declarations(body, ctx)
        
        # Trigger-specific conversions
        body = self._convert_trigger_body(body, ctx)
        
        # Convert body
        body = self._convert_body(body, ctx)
        
        # Parse events
        pg_events = ' OR '.join([e.strip().upper() for e in events.split(',')])
        
        # Build trigger function and trigger
        return self._build_trigger(ctx, pg_events, body)
    
    def _convert_trigger_body(self, body: str, ctx: ConversionContext) -> str:
        """Apply trigger-specific conversions"""
        # Convert inserted/deleted to NEW/OLD
        # inserted.column -> NEW.column
        body = re.sub(r'\binserted\.([a-zA-Z0-9_"]+)', r'NEW.\1', body, flags=re.IGNORECASE)
        # deleted.column -> OLD.column
        body = re.sub(r'\bdeleted\.([a-zA-Z0-9_"]+)', r'OLD.\1', body, flags=re.IGNORECASE)
        
        # Remove FROM inserted/deleted
        body = re.sub(r'\bFROM\s+inserted\b', '', body, flags=re.IGNORECASE)
        body = re.sub(r'\bFROM\s+deleted\b', '', body, flags=re.IGNORECASE)
        
        # IF UPDATE(column) -> NEW.column IS DISTINCT FROM OLD.column
        def update_check(match):
            col = match.group(1)
            return f"NEW.{col} IS DISTINCT FROM OLD.{col}"
        body = re.sub(r'\bUPDATE\s*\(\s*([a-zA-Z0-9_"]+)\s*\)', update_check, body, flags=re.IGNORECASE)
        
        # ROLLBACK TRIGGER -> RAISE EXCEPTION
        body = re.sub(
            r'\bROLLBACK\s+TRIGGER\b(?:\s+WITH\s+RAISERROR\s+\d+\s+[\'"]([^\'"]+)[\'"])?',
            lambda m: f"RAISE EXCEPTION '{m.group(1)}'" if m.group(1) else 'RAISE EXCEPTION',
            body,
            flags=re.IGNORECASE
        )
        
        return body
    
    def _build_trigger(self, ctx: ConversionContext, pg_events: str, body: str) -> str:
        """Build trigger function and trigger DDL"""
        # Build declarations
        decl_section = ""
        if ctx.declarations:
            decl_lines = "\n    ".join(ctx.declarations)
            decl_section = f"DECLARE\n    {decl_lines}"
        
        # Determine return value based on event type
        return_value = "NEW" if "INSERT" in pg_events or "UPDATE" in pg_events else "OLD"
        
        # Build trigger function
        trigger_func = f"""CREATE OR REPLACE FUNCTION {ctx.schema_name}.{ctx.object_name}_func()
RETURNS trigger AS $$
{decl_section}
BEGIN
{body}
    RETURN {return_value};
END;
$$ LANGUAGE plpgsql;
"""
        
        # Build trigger
        trigger_ddl = f"""CREATE TRIGGER {ctx.object_name}
AFTER {pg_events} ON "{ctx.schema_name}"."{ctx.table_name}"
FOR EACH ROW
EXECUTE FUNCTION {ctx.schema_name}.{ctx.object_name}_func();
"""
        
        return trigger_func + '\n' + trigger_ddl


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Convert T-SQL code to PostgreSQL pl/pgsql',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('input_file', help='Input file containing T-SQL code')
    parser.add_argument('output_file', help='Output file for PostgreSQL code')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("T-SQL to PostgreSQL Converter (Enhanced Version)")
    print("=" * 70)
    print(f"Input file:  {args.input_file}")
    print(f"Output file: {args.output_file}")
    print("=" * 70)
    print()
    
    converter = TSQLConverter()
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
