"""
tsql_plpgsql_converter.py
================================

This module provides a handful of functions to convert Sybase or Microsoft
T‑SQL stored procedures, functions and triggers into approximate PostgreSQL
PL/pgSQL equivalents.  The primary goal of this module is to offer a
light‑weight, self‑contained alternative to the very complex, AST based
converters found inside the existing migrator sources.  It follows many of
the high level rules described in the supplied `sybase_conversion_rules.md`
and `mssql_conversion_rules.md` documents but purposefully avoids deep
dependency chains or heavy parser requirements.  A particular pain point
when converting T‑SQL is the fact that statements are not terminated by
semicolons; this module experiments with several strategies for inserting
statement terminators prior to performing more structural rewrites.

Three conversion strategies are implemented:

 * ``convert_simple`` — an extremely conservative approach which assumes that
   each non‑empty line of the input contains at most one complete statement.
   It blindly appends a semicolon to lines that do not already end with one
   before performing minimal rewrites.  This strategy is often sufficient
   for trivial procedures but will fail on multi‑line ``SELECT`` or ``IF``
   blocks.

 * ``convert_heuristic`` — a more sophisticated algorithm which attempts to
   detect logical statement boundaries by looking at keywords at the start of
   each line.  If a line starts with a T‑SQL keyword which normally begins
   a new statement (e.g. ``SELECT``, ``INSERT``, ``UPDATE``, ``DELETE``,
   ``IF``, ``ELSE``, ``WHILE``, ``BEGIN``, ``END``, ``DECLARE``, ``SET``,
   ``PRINT``, ``RAISERROR``) then the preceding line is terminated with a
   semicolon if it is currently unterminated.  This helps to preserve
   multi‑line constructs and reduces the likelihood of splitting expressions
   incorrectly.  After inserting semicolons the converter performs a
   series of regular expression driven rewrites to map variable names,
   parameter declarations, control flow and error handling into PL/pgSQL.

 * ``convert_sqlglot_fallback`` — a very lightweight wrapper around the
   ``sqlglot`` library.  It first uses the heuristic approach to insert
   semicolons and then feeds the resulting T‑SQL into ``sqlglot``'s
   ``transpile`` function with the ``TSQL`` dialect and ``postgres`` output.
   This rarely produces perfect results (because ``sqlglot`` itself
   struggles with missing semicolons and Sybase idiosyncrasies) but it can
   produce a syntactically valid baseline which the caller can manually
   improve.

Each converter returns a tuple ``(header_sql, body_sql)`` where
``header_sql`` contains the CREATE statement and parameter list rewritten for
PL/pgSQL and ``body_sql`` contains the function/procedure body including
the ``DECLARE`` and ``BEGIN…END`` sections.  Triggers are similarly
converted into a two part representation.

The module exposes a single public function ``convert_tsql`` which accepts
the source T‑SQL code and a ``strategy`` keyword argument indicating which
algorithm to employ.  If no strategy is provided then ``heuristic`` will
be used by default.  Callers can supply their own strategy function as
well.

This module is not intended to be a drop‑in replacement for the official
migrator.  Its purpose is to demonstrate a range of parsing and rewriting
techniques that handle missing semicolons reasonably well while remaining
easy to understand and extend.  Advanced Sybase features such as cursors,
outer join syntax (``*=``, ``=*``) or proprietary error handling are not
fully supported — unrecognised fragments are preserved verbatim inside
``/* TODO: ... */`` comments for subsequent manual review.
"""

from __future__ import annotations

import re
from typing import List, Tuple, Callable, Dict, Optional

try:
    # sqlglot is an optional dependency.  Importing it here allows the
    # fallback converter to work if present but does not crash otherwise.
    import sqlglot
    from sqlglot import exp, Dialect, transpile
    _SQLGLOT_AVAILABLE = True
except Exception:
    _SQLGLOT_AVAILABLE = False


# -----------------------------------------------------------------------------
# Internal helper functions
#

_STATEMENT_START_KEYWORDS = {
    'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'IF', 'ELSE', 'WHILE',
    'BEGIN', 'END', 'DECLARE', 'SET', 'RETURN', 'PRINT', 'RAISERROR', 'EXEC',
    'EXECUTE', 'LOOP', 'CASE', 'WITH', 'FETCH', 'OPEN', 'CLOSE'
}

def _strip_strings_and_comments(sql: str) -> str:
    """Remove string literals and comments from the provided SQL.

    This utility is used internally when looking for keywords or inserting
    semicolons to avoid accidentally splitting inside a string literal or
    comment.  Single‑quoted strings, double‑quoted identifiers and
    bracketed identifiers are all replaced with placeholder markers so that
    scanning for keywords can proceed safely.
    """
    # Replace single quoted strings
    sql = re.sub(r"'(?:''|[^'])*'", "''", sql)
    # Replace double quoted identifiers
    sql = re.sub(r'"(?:""|[^"])*"', '""', sql)
    # Replace bracket identifiers [foo]
    sql = re.sub(r'\[(?:[^\]]|\]\])\]', '""', sql)
    # Replace /* ... */ comments
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    # Replace -- comments
    sql = re.sub(r'--[^\n]*', '', sql)
    return sql


def _insert_semicolons_conservative(lines: List[str]) -> List[str]:
    """A very conservative strategy that terminates every non‑empty line.

    This function returns a new list where each non‑empty line has a semicolon
    appended if it does not already end with one.  Empty or whitespace only
    lines are preserved unchanged.  This approach is naive but ensures that
    ``sqlglot`` will see complete statements on every line.
    """
    result: List[str] = []
    for line in lines:
        stripped = line.rstrip()
        if stripped and not stripped.endswith(';'):
            result.append(stripped + ';')
        else:
            result.append(stripped)
    return result


def _insert_semicolons_heuristic(lines: List[str]) -> List[str]:
    """Insert semicolons between statements using heuristic rules.

    When scanning the list of lines this function looks at each line and
    determines whether the following non‑empty, non‑comment line begins with a
    T‑SQL keyword that typically denotes the start of a new statement.  If
    so, and the current line does not already end with a semicolon or BEGIN
    then a semicolon is appended.  Lines inside ``BEGIN…END`` blocks are
    handled naturally because ``BEGIN`` and ``END`` themselves are start
    keywords.  Inline comments starting with ``--`` are ignored when looking
    for terminating semicolons.
    """
    result: List[str] = []
    cleaned_lines = [l.rstrip() for l in lines]
    # Precompute stripped versions for keyword scanning
    stripped_no_str: List[str] = [
        _strip_strings_and_comments(l).lstrip().upper() for l in cleaned_lines
    ]
    n = len(cleaned_lines)
    for i in range(n):
        current = cleaned_lines[i]
        if not current.strip():
            result.append(current)
            continue
        # Determine if the next meaningful line starts a new statement
        need_term = False
        # Skip to next line that has content
        j = i + 1
        while j < n and not cleaned_lines[j].strip():
            j += 1
        if j < n:
            next_strip = stripped_no_str[j]
            # If next line begins with a keyword then we should terminate
            for kw in _STATEMENT_START_KEYWORDS:
                if next_strip.startswith(kw):
                    need_term = True
                    break
        else:
            # Last line of file: always ensure termination
            need_term = True
        # Only append semicolon if it is missing and termination is required
        line_terminated = current.rstrip()
        if need_term and not line_terminated.endswith((';', 'BEGIN', 'END', 'THEN', 'ELSE')):
            result.append(line_terminated + ';')
        else:
            result.append(line_terminated)
    return result


def _normalize_whitespace(sql: str) -> str:
    """Collapse multiple blank lines and normalise indentation.

    This helper simply reduces sequences of more than one blank line to a
    single blank line.  It does not otherwise alter indentation because T‑SQL
    and PL/pgSQL rely heavily on indentation for human readability.
    """
    # Collapse multiple blank lines
    sql = re.sub(r'\n{3,}', '\n\n', sql)
    return sql


def _convert_header(header: str) -> Tuple[str, List[str], List[str], Dict[str, str]]:
    """Parse the header of a T‑SQL CREATE statement and produce a PL/pgSQL header.

    This function looks for ``CREATE PROCEDURE``, ``CREATE FUNCTION`` or
    ``CREATE TRIGGER`` and extracts the object name and parameter list.  It
    returns a tuple containing the rewritten ``CREATE`` statement for
    PL/pgSQL, a list of renamed parameter declarations and a list of output
    parameter names.  If no parameters are present then the parameter list
    will be empty.  All variables are converted from ``@foo`` to ``p_foo``.
    """
    create_re = re.compile(
        r'\bCREATE\s+(?:OR\s+ALTER\s+)?(PROCEDURE|PROC|FUNCTION|TRIGGER)\s+'  # object type
        r'([a-zA-Z0-9_\.\[\]]+)',
        flags=re.IGNORECASE
    )
    m = create_re.search(header)
    if not m:
        raise ValueError('Unable to parse CREATE statement header')
    obj_type = m.group(1).upper()
    obj_name_raw = m.group(2).strip()
    # Remove brackets around identifiers and quote dots
    def unquote_ident(name: str) -> str:
        name = name.strip()
        # Remove [ and ]
        if name.startswith('[') and name.endswith(']'):
            name = name[1:-1]
        return name
    obj_parts = [unquote_ident(part) for part in obj_name_raw.split('.')]
    if len(obj_parts) == 1:
        schema, obj_name = 'public', obj_parts[0]
    elif len(obj_parts) == 2:
        schema, obj_name = obj_parts
    else:
        schema, obj_name = obj_parts[-2], obj_parts[-1]
    # Extract parameter list inside parentheses following the name
    params_section = ''
    # Find parentheses after the name
    paren = re.compile(r'\((.*)\)', flags=re.DOTALL)
    paren_match = paren.search(header, m.end())
    if paren_match:
        params_section = paren_match.group(1)
    params: List[str] = []
    out_params: List[str] = []
    if params_section:
        # Split parameters by commas outside parentheses
        depth = 0
        start = 0
        parts: List[str] = []
        for i, ch in enumerate(params_section):
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif ch == ',' and depth == 0:
                parts.append(params_section[start:i])
                start = i + 1
        parts.append(params_section[start:])
        for p in parts:
            p = p.strip()
            if not p:
                continue
            # Parameter pattern: @name type [= default] [output|out]
            match = re.match(
                r'@([a-zA-Z0-9_]+)\s+([^=\s]+(?:\s*\([^\)]+\))?)(?:\s*=\s*([^\s]+))?(?:\s+(OUTPUT|OUT))?',
                p,
                flags=re.IGNORECASE
            )
            if match:
                name = match.group(1)
                ptype = match.group(2)
                default = match.group(3)
                out_kw = match.group(4)
                pg_name = f"p_{name.lower()}"
                # Map common Sybase types to PostgreSQL types
                # Basic mapping; more can be added as needed
                type_map = {
                    'numeric': 'numeric', 'decimal': 'numeric', 'int': 'integer',
                    'integer': 'integer', 'tinyint': 'smallint', 'smallint': 'smallint',
                    'bigint': 'bigint', 'univarchar': 'varchar', 'nvarchar': 'varchar',
                    'varchar': 'varchar', 'char': 'char', 'nchar': 'char', 'datetime': 'timestamp',
                    'bit': 'boolean'
                }
                base_type = ptype.lower().split('(')[0]
                pg_type = type_map.get(base_type, ptype)
                if default:
                    default = default.strip().strip("'")
                    decl = f"{pg_name} {pg_type} DEFAULT '{default}'"
                else:
                    decl = f"{pg_name} {pg_type}"
                if out_kw:
                    # OUT or OUTPUT parameter
                    params.append(f"OUT {decl}")
                    out_params.append(pg_name)
                else:
                    params.append(f"IN {decl}")
            else:
                # Fallback: treat as raw and pass through
                params.append(p)
    # Build the CREATE line
    if obj_type in ('PROCEDURE', 'PROC'):
        create_line = f"CREATE OR REPLACE PROCEDURE {schema}.{obj_name}"
    elif obj_type == 'FUNCTION':
        create_line = f"CREATE OR REPLACE FUNCTION {schema}.{obj_name}"
    elif obj_type == 'TRIGGER':
        # Triggers are handled separately; header will be filled by trigger converter
        create_line = f"CREATE TRIGGER {obj_name}"
    else:
        create_line = f"CREATE OR REPLACE {obj_type} {schema}.{obj_name}"
    # Build a mapping from original parameter names (with leading @) to their
    # PostgreSQL parameter names (p_<name>).  This is used by the callers to
    # substitute occurrences of parameters in the body.  Keys are case
    # preserved as found in the header for more reliable matching.
    param_map: Dict[str, str] = {}
    # Reparse the original header section to locate parameter declarations.  We
    # cannot rely on ``params`` alone because that list has already been
    # normalised.  Instead search for @name tokens in the provided header
    # string.  For each occurrence map it to the corresponding p_ name by
    # matching names ignoring case.
    header_no_strings = _strip_strings_and_comments(header)
    # Find all @param occurrences preceding a type definition (roughly)
    for m in re.finditer(r'@([A-Za-z0-9_]+)', header_no_strings):
        raw_name = m.group(1)
        pg_name = f"p_{raw_name.lower()}"
        # Use the exact substring including @ for matching later
        param_map[f"@{raw_name}"] = pg_name
    return create_line, params, out_params, param_map


def _rename_local_variables(sql: str) -> Tuple[str, Dict[str, str]]:
    """Rename local T‑SQL variables (``@foo``) to ``locvar_foo``.

    This returns the modified SQL and a dictionary mapping original names to
    their new names.  Global variables (``@@foo``) are preserved for other
    passes to handle.  The renaming avoids replacing occurrences where the
    ``@`` symbol appears inside strings or quoted identifiers by first
    replacing strings and identifiers with placeholders, then performing the
    substitution on the remainder, and finally re‑inserting the placeholders.
    """
    placeholders: List[str] = []
    def _replace_match(match: re.Match) -> str:
        placeholders.append(match.group(0))
        return f"__STR_PLACEHOLDER_{len(placeholders) - 1}__"
    # Temporarily remove strings and quoted idents
    tmp = re.sub(r"'(?:''|[^'])*'", _replace_match, sql)
    tmp = re.sub(r'"(?:""|[^"])*"', _replace_match, tmp)
    tmp = re.sub(r'\[(?:[^\]]|\]\])\]', _replace_match, tmp)
    mapping: Dict[str, str] = {}
    def repl_var(m: re.Match) -> str:
        name = m.group(1)
        new_name = mapping.get(name)
        if not new_name:
            new_name = f"locvar_{name.lower()}"
            mapping[name] = new_name
        return new_name
    # Only match single @ variables, not @@ global variables
    tmp = re.sub(r'(?<!@)@([A-Za-z0-9_]+)', repl_var, tmp)
    # Put strings/idents back
    def reinstate(match: re.Match) -> str:
        idx = int(match.group(1))
        return placeholders[idx]
    tmp = re.sub(r'__STR_PLACEHOLDER_(\d+)__', reinstate, tmp)
    return tmp, mapping


def _replace_parameters(sql: str, param_map: Dict[str, str]) -> str:
    """Replace occurrences of procedure/function parameters in the SQL body.

    Parameters are specified in the ``param_map`` as mappings from the
    original T‑SQL variable name (including the leading ``@``) to the
    PostgreSQL name (e.g. ``p_foo``).  This helper temporarily removes
    strings and quoted identifiers so that parameter names inside them are
    not replaced.  It then performs a case‑insensitive replacement of
    parameter tokens using word boundaries to avoid partial matches.  After
    substitution the original strings and identifiers are restored.
    """
    if not param_map:
        return sql
    placeholders: List[str] = []
    def _replace_match(match: re.Match) -> str:
        placeholders.append(match.group(0))
        return f"__STR_PLACEHOLDER_{len(placeholders) - 1}__"
    # Remove string literals and quoted identifiers
    tmp = re.sub(r"'(?:''|[^'])*'", _replace_match, sql)
    tmp = re.sub(r'"(?:""|[^"])*"', _replace_match, tmp)
    tmp = re.sub(r'\[(?:[^\]]|\]\])\]', _replace_match, tmp)
    # Perform replacement on the remainder
    for orig, pg_name in param_map.items():
        # Escape @ and ensure case‑insensitive match with word boundary
        # Pattern matches the parameter name with leading @, ignoring case.  We
        # avoid using a word boundary at the beginning because @ is not a
        # word character.  We use a word boundary at the end to ensure we
        # only replace full identifiers (e.g. @param) and not longer
        # sequences (e.g. @param123).
        pattern = re.compile(rf'{re.escape(orig)}\b', flags=re.IGNORECASE)
        tmp = pattern.sub(pg_name, tmp)
    # Restore removed strings/idents
    def reinstate(match: re.Match) -> str:
        idx = int(match.group(1))
        return placeholders[idx]
    tmp = re.sub(r'__STR_PLACEHOLDER_(\d+)__', reinstate, tmp)
    return tmp


def _convert_assignments(sql: str, var_mapping: Dict[str, str]) -> str:
    """Convert T‑SQL assignment syntax into PL/pgSQL syntax.

    This function handles both ``SET @var = expr`` and ``SELECT @var = expr``
    assignments.  In PL/pgSQL the assignment operator is ``:=`` and local
    variables do not carry the ``@`` prefix.  Multiple assignments on a
    single line (``SELECT @a = 1, @b = 2``) are split into separate
    statements.  Only basic patterns are handled here; complex SELECT
    statements that produce a row set are left untouched for further
    processing.
    """
    """
    Convert both explicit ``SET`` assignments and ``SELECT`` assignment lists into
    PL/pgSQL ``:=`` syntax.  Variables referenced are substituted according to
    ``var_mapping``.  Assignments within ``SELECT`` statements are only
    rewritten when the ``SELECT`` does not contain a ``FROM`` clause; if a
    ``FROM`` clause is present we assume the statement returns a result set
    and leave it untouched for later processing.
    """
    # Replace variable names now so that assignment patterns match mapped names
    for orig, new in sorted(var_mapping.items(), key=lambda x: -len(x[0])):
        # Use word boundary to avoid partial replacements
        sql = re.sub(rf'\b{re.escape(orig)}\b', new, sql)

    # Handle SELECT @var = column FROM ... -> SELECT column INTO var FROM ...
    def select_into_replacer(match: re.Match) -> str:
        var = match.group(1)
        expr = match.group(2)
        return f"SELECT {expr} INTO {var} FROM"
    sql = re.sub(r'(?i)select\s+([A-Za-z0-9_]+)\s*=\s*([A-Za-z0-9_\.]+)\s+from', select_into_replacer, sql)

    # Handle SET var = expr -> var := expr;
    def set_replacer(match: re.Match) -> str:
        left, right = match.group(1), match.group(2)
        return f"{left} := {right}"
    sql = re.sub(r'\bSET\s+([A-Za-z0-9_]+)\s*=\s*([^;\n]+)', set_replacer, sql, flags=re.IGNORECASE)

    # Handle multi‑line and single‑line SELECT assignments without FROM clause.
    def select_assign_replacer(match: re.Match) -> str:
        assignments = match.group(1)
        # Flatten whitespace and split by commas
        assignments_clean = assignments.replace('\n', ' ').replace('\r', ' ')
        parts = []
        depth = 0
        start = 0
        for idx, ch in enumerate(assignments_clean):
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif ch == ',' and depth == 0:
                parts.append(assignments_clean[start:idx])
                start = idx + 1
        parts.append(assignments_clean[start:])
        stmts = []
        for item in parts:
            item = item.strip()
            if not item:
                continue
            m = re.match(r'([A-Za-z0-9_]+)\s*=\s*(.+)', item)
            if m:
                var, expr = m.group(1), m.group(2)
                stmts.append(f"{var} := {expr.strip()};")
            else:
                stmts.append(item.rstrip(',') + ';')
        return '\n'.join(stmts)

    # Regex to match SELECT assignment blocks that do not include a FROM clause.
    # This captures across multiple lines until a blank line or another SELECT/IF/BEGIN.
    # Match SELECT assignments that are not followed by a FROM/WHERE/GROUP/HAVING/BIGIN/RAISERROR/PRINT/semicolon or newline.
    # We intentionally do not consume the lookahead token; instead we use a positive lookahead so that the
    # following keyword remains in the string.  This prevents loss of control flow keywords such as IF/ELSE.
    select_assign_pattern = re.compile(
        r'(?is)\bSELECT\s+((?:\s*[A-Za-z0-9_]+\s*=\s*[^,\n]+\s*,)*\s*[A-Za-z0-9_]+\s*=\s*[^,\n]+)\s*(?=\bFROM\b|\bWHERE\b|\bGROUP\b|\bHAVING\b|\bBEGIN\b|\bRAISERROR\b|\bPRINT\b|;|\n)',
    )
    sql = select_assign_pattern.sub(lambda m: select_assign_replacer(m), sql)

    return sql


def _convert_if_statements(sql: str) -> str:
    """Convert T‑SQL IF/ELSE constructs into PL/pgSQL syntax.

    This function normalises ``IF`` statements by ensuring that each
    condition is followed by ``THEN`` and converts ``ELSE IF`` chains into
    ``ELSIF``.  It operates line by line so as to avoid complex nested
    parsing.  Standalone ``BEGIN``/``END`` markers associated with IF
    blocks should be removed separately by the caller if desired.  The
    transformation is heuristic and may not correctly handle deeply nested
    or unconventional constructs.
    """
    lines = sql.splitlines()
    new_lines: List[str] = []
    for line in lines:
        stripped = line.lstrip()
        indent = line[: len(line) - len(stripped)]
        lower = stripped.lower()
        # Handle ELSE IF / ELSEIF -> ELSIF
        if lower.startswith('else if') or lower.startswith('elseif'):
            # Replace the 'else if' prefix with 'ELSIF ' (note trailing space)
            rest = re.sub(r'^else\s+if', 'ELSIF ', stripped, flags=re.IGNORECASE)
            if ' then' not in rest.lower():
                rest = rest.rstrip(';') + ' THEN'
            new_lines.append(indent + rest)
            continue
        # Handle ELSE alone (but not ELSEIF)
        if lower.startswith('else') and not lower.startswith('elseif'):
            new_lines.append(indent + 'ELSE')
            continue
        # Handle IF or ELSIF
        if lower.startswith('if') or lower.startswith('elsif'):
            if lower.startswith('if'):
                keyword = 'IF'
                rest = stripped[2:].lstrip()
            else:
                keyword = 'ELSIF'
                rest = stripped[5:].lstrip()
            if ' then' not in lower:
                rest = rest.rstrip(';')
                new_lines.append(f"{indent}{keyword} {rest} THEN")
            else:
                # Preserve existing THEN (e.g. from prior passes)
                new_lines.append(f"{indent}{keyword} {rest}")
            continue
        # Otherwise preserve line
        new_lines.append(line)
    return '\n'.join(new_lines)


def _convert_print_and_raiserror(sql: str) -> str:
    """Translate PRINT and RAISERROR statements to PostgreSQL equivalents.

    ``PRINT 'message'`` in T‑SQL becomes ``RAISE NOTICE 'message';`` in
    PL/pgSQL.  ``RAISERROR`` is mapped to ``RAISE EXCEPTION`` preserving
    the error message.  Severity levels and state codes are ignored.
    """
    # PRINT 'message' -> RAISE NOTICE 'message';
    sql = re.sub(
        r'\bPRINT\s+(N?\'[^\']*\')',
        lambda m: f"RAISE NOTICE {m.group(1)};",
        sql,
        flags=re.IGNORECASE
    )
    # RAISERROR (<code>, 'msg', ..) -> RAISE EXCEPTION 'msg';
    def rais_replacer(match: re.Match) -> str:
        msg = match.group(2)
        return f"RAISE EXCEPTION {msg};"
    sql = re.sub(
        r'\bRAISERROR\s*\(\s*([0-9]+)\s*,\s*(N?\'[^\']*\')\s*(?:,.*)?\)',
        rais_replacer,
        sql,
        flags=re.IGNORECASE
    )
    return sql


def _convert_trigger_tables(sql: str) -> str:
    """Replace references to ``inserted`` and ``deleted`` tables with NEW/OLD.

    Sybase triggers use pseudo tables ``inserted`` and ``deleted`` to access
    the rows affected by the triggering statement.  In PostgreSQL row level
    triggers ``NEW`` and ``OLD`` are records rather than tables.  This simple
    transformation replaces ``inserted.col`` with ``NEW.col`` and
    ``deleted.col`` with ``OLD.col``.  Joins involving these tables are
    reduced to just the record references.  More complex set based logic is
    outside the scope of this converter and should be checked manually.
    """
    sql = re.sub(r'\binserted\.([A-Za-z0-9_]+)', r'NEW.\1', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bdeleted\.([A-Za-z0-9_]+)', r'OLD.\1', sql, flags=re.IGNORECASE)
    # Remove FROM clauses referencing inserted/deleted
    sql = re.sub(r'\bFROM\s+inserted\b', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bFROM\s+deleted\b', '', sql, flags=re.IGNORECASE)
    return sql


def _sanitize_final(sql: str) -> str:
    """Apply final clean up passes on the converted PL/pgSQL code.

    This includes removing redundant semicolons, ensuring terminators on
    control structures and collapsing whitespace.  The rules here are based
    on the sanitisation section of the Sybase conversion rules.
    """
    # Remove duplicate semicolons
    sql = re.sub(r';+\s*;', ';', sql)
    # Remove stray semicolons after comments
    sql = re.sub(r'/\*([^*]|\*(?!/))*\*/\s*;', r'/*\1*/', sql)
    # Ensure END terminators
    sql = re.sub(r'\bEND IF\b(?!;)', 'END IF;', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bEND LOOP\b(?!;)', 'END LOOP;', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bEND WHILE\b(?!;)', 'END WHILE;', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bEND\b(?!;)', 'END;', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bRETURN NEW\b(?!;)', 'RETURN NEW;', sql, flags=re.IGNORECASE)
    # Attach semicolons that are on their own line to the previous line
    sql = re.sub(r'\n\s*;\s*\n', ';\n', sql)
    # Remove comment_ prefixes leftover from comment preservation
    sql = re.sub(r'\bcomment_', '', sql)
    # Normalise whitespace
    sql = _normalize_whitespace(sql)
    return sql


# -----------------------------------------------------------------------------
# Conversion strategies
#

def convert_simple(tsql: str) -> Tuple[str, str]:
    """Simplistic conversion using conservative semicolon insertion.

    This function merely terminates every non‑blank line with a semicolon,
    rewrites variables and assignments and wraps the body into a PL/pgSQL
    ``BEGIN … END`` block.  Use this when the T‑SQL is very simple or as a
    baseline for more advanced strategies.
    """
    lines = tsql.strip().splitlines()
    # Split header and body by the first occurrence of AS or BEGIN
    header_lines: List[str] = []
    body_lines: List[str] = []
    in_body = False
    for line in lines:
        if not in_body and re.search(r'\bAS\b', line, flags=re.IGNORECASE):
            in_body = True
            continue
        if not in_body and re.search(r'\bBEGIN\b', line, flags=re.IGNORECASE):
            in_body = True
            continue
        if in_body:
            body_lines.append(line)
        else:
            header_lines.append(line)
    create_line, params, out_params, param_map = _convert_header('\n'.join(header_lines))
    # Insert semicolons on every non‑empty line
    body_term = '\n'.join(_insert_semicolons_conservative(body_lines))
    # Replace parameter occurrences with p_ names
    body_term = _replace_parameters(body_term, param_map)
    # Rename remaining local variables
    body_term, mapping = _rename_local_variables(body_term)
    # Convert assignments using the mapping for renamed variables
    body_term = _convert_assignments(body_term, mapping)
    # Convert IF statements and prints
    body_term = _convert_if_statements(body_term)
    body_term = _convert_print_and_raiserror(body_term)
    # Build parameter list
    param_list = f"({', '.join(params)})" if params else '()'
    # Determine RETURNS clause for procedures/functions
    returns_clause = ''
    if out_params:
        if len(out_params) == 1:
            # Single OUT param -> RETURNS type via OUT parameter
            returns_clause = ''
        else:
            returns_clause = ' RETURNS RECORD'
    # Build final body with DECLARE section
    decls = []
    for orig, new in mapping.items():
        decls.append(f"{new} varchar;")  # default type; in real converter we would infer
    declare_section = ''
    if decls:
        declare_section = 'DECLARE\n    ' + '\n    '.join(decls) + '\n'
    body_sql = f"{declare_section}BEGIN\n{body_term}\nEND;"
    header_sql = f"{create_line}{param_list}{returns_clause} LANGUAGE plpgsql"
    return header_sql, body_sql


def convert_heuristic(tsql: str) -> Tuple[str, str]:
    """More robust converter that uses heuristics to insert semicolons.

    This strategy attempts to detect statement boundaries by examining the
    following line for keywords.  After semicolons are inserted the code is
    transformed via a series of regex based rewrites into PL/pgSQL.  The
    resulting code tends to be easier to read and requires fewer manual
    corrections than the output of ``convert_simple``.
    """
    lines = tsql.strip().splitlines()
    header_lines: List[str] = []
    body_lines: List[str] = []
    in_body = False
    for line in lines:
        if not in_body and re.search(r'\bAS\b', line, flags=re.IGNORECASE):
            in_body = True
            continue
        if not in_body and re.search(r'\bBEGIN\b', line, flags=re.IGNORECASE):
            in_body = True
            continue
        if in_body:
            body_lines.append(line)
        else:
            header_lines.append(line)
    create_line, params, out_params, param_map = _convert_header('\n'.join(header_lines))
    # Replace parameter occurrences with p_ names before renaming locals
    body_code = '\n'.join(body_lines)
    body_code = _replace_parameters(body_code, param_map)
    # Rename local variables throughout the entire body so that we can
    # reliably parse declarations using the renamed identifiers.  This
    # function returns both the renamed SQL and a mapping dictionary.
    body_code, mapping = _rename_local_variables(body_code)
    # Perform assignment conversions on the renamed body before any further parsing.
    body_code = _convert_assignments(body_code, {})
    # Split renamed (and assignment converted) body into lines for further processing
    body_lines_renamed = body_code.splitlines()
    # Extract explicit DECLARE blocks from the renamed body
    decl_lines: List[str] = []
    filtered_body_lines: List[str] = []
    type_map = {
        'univarchar': 'varchar', 'nvarchar': 'varchar', 'varchar': 'varchar',
        'char': 'char', 'nchar': 'char', 'numeric': 'numeric', 'decimal': 'numeric',
        'int': 'integer', 'integer': 'integer', 'tinyint': 'smallint',
        'smallint': 'smallint', 'bigint': 'bigint', 'bit': 'boolean',
    }
    i = 0
    while i < len(body_lines_renamed):
        line = body_lines_renamed[i]
        if re.match(r'\s*DECLARE\b', line, flags=re.IGNORECASE):
            # Collect lines belonging to this DECLARE block
            decl_block_lines = [line]
            i += 1
            # Continuation lines: start with whitespace and contain comma
            while i < len(body_lines_renamed) and (body_lines_renamed[i].strip().startswith('locvar_') or (',' in body_lines_renamed[i] and body_lines_renamed[i].strip() and not re.match(r'\bBEGIN\b|\bSELECT\b|\bIF\b|\bSET\b', body_lines_renamed[i].strip(), flags=re.IGNORECASE))):
                decl_block_lines.append(body_lines_renamed[i])
                i += 1
            # Parse variables from decl_block_lines
            decl_content = []
            first_line = decl_block_lines[0]
            after_declare = re.split(r'\bDECLARE\b', first_line, flags=re.IGNORECASE, maxsplit=1)[1]
            decl_content.append(after_declare)
            decl_content.extend(decl_block_lines[1:])
            decl_joined = ' '.join([l.strip().rstrip(';') for l in decl_content])
            depth = 0
            start_idx = 0
            parts = []
            for idx, ch in enumerate(decl_joined):
                if ch == '(': depth += 1
                elif ch == ')': depth -= 1
                elif ch == ',' and depth == 0:
                    parts.append(decl_joined[start_idx:idx])
                    start_idx = idx + 1
            parts.append(decl_joined[start_idx:])
            for p in parts:
                p = p.strip()
                if not p:
                    continue
                mvar = re.match(r'([A-Za-z0-9_]+)\s+([^,]+)', p)
                if mvar:
                    var, dtype = mvar.group(1), mvar.group(2).strip()
                    base = dtype.lower().split('(')[0]
                    pg_type = type_map.get(base, dtype)
                    # Add declaration line using renamed variable and mapped type
                    decl_lines.append(f"{var} {pg_type};")
                else:
                    decl_lines.append(f"/* TODO: unparsed declaration: {p} */")
        else:
            filtered_body_lines.append(line)
            i += 1
    # Insert semicolons heuristically on the filtered body lines
    body_term_lines = _insert_semicolons_heuristic(filtered_body_lines)
    body_term = '\n'.join(body_term_lines)
    # Convert assignments (variables have already been renamed, so mapping is unused here)
    body_term = _convert_assignments(body_term, {})
    # Convert IF/ELSE constructs
    body_term = _convert_if_statements(body_term)
    # Convert PRINT/RAISERROR
    body_term = _convert_print_and_raiserror(body_term)
    # Normalise BEGIN/END casing and ensure standalone END statements end with a semicolon.
    # We avoid altering END IF/LOOP/WHILE constructs here.  Also replace lower‑case
    # 'begin' with uppercase BEGIN for consistency.
    body_term = re.sub(r'\bbegin\b', 'BEGIN', body_term, flags=re.IGNORECASE)
    body_term = re.sub(r'\bEND\b(?!\s+(?:IF|LOOP|WHILE))', 'END;', body_term, flags=re.IGNORECASE)
    # Remove standalone BEGIN/END lines that are remnants of T‑SQL block delimiters.
    # PL/pgSQL does not require explicit BEGIN/END inside IF/ELSE blocks.
    bt_lines = []
    for line in body_term.splitlines():
        stripped = line.strip().upper().rstrip(';')
        if stripped in ('BEGIN', 'END'):
            continue
        bt_lines.append(line)
    body_term = '\n'.join(bt_lines)
    # Do not automatically insert END IF here.  PL/pgSQL requires explicit END IF
    # terminators, but inserting them automatically can lead to incorrect
    # placement.  Users should verify and add END IF; statements where
    # appropriate after conversion.
    # Convert trigger pseudo tables if needed
    if re.search(r'\btrigger\b', create_line, flags=re.IGNORECASE):
        body_term = _convert_trigger_tables(body_term)
    # Determine parameter list
    param_list = f"({', '.join(params)})" if params else '()'
    returns_clause = ''
    if out_params:
        if len(out_params) == 1:
            returns_clause = ''
        else:
            returns_clause = ' RETURNS RECORD'
    # Build DECLARE section combining extracted declarations and mapping
    # Ensure we include declarations for renamed variables that were not explicitly declared
    for orig, new in mapping.items():
        # Only add if not already declared explicitly
        if not any(new == d.split()[0] for d in decl_lines):
            decl_lines.append(f"{new} varchar;")
    declare_section = ''
    if decl_lines:
        declare_section = 'DECLARE\n    ' + '\n    '.join(decl_lines) + '\n'
    body_sql = f"{declare_section}BEGIN\n{body_term}\nEND;"
    header_sql = f"{create_line}{param_list}{returns_clause} LANGUAGE plpgsql"
    final_sql = _sanitize_final(body_sql)
    # Insert a newline after every semicolon so that statements do not run into
    # the next one on the same line.  The replacement string contains a
    # literal newline character (escaped here as \n).
    final_sql = re.sub(r';\s*', ';\n', final_sql)
    return header_sql, final_sql


def convert_sqlglot_fallback(tsql: str) -> Tuple[str, str]:
    """Use sqlglot to perform the heavy lifting when available.

    The heuristic converter is first used to insert semicolons and perform
    minimal rewrites.  The result is then passed into ``sqlglot.transpile``
    using the TSQL dialect with PostgreSQL as the target.  This strategy
    requires ``sqlglot`` to be installed; if it is not available a
    ``RuntimeError`` is raised.
    """
    if not _SQLGLOT_AVAILABLE:
        raise RuntimeError('sqlglot is not available in this environment')
    header, body = convert_heuristic(tsql)
    try:
        # ``sqlglot`` expects full statements; join header and body for
        # transpilation then split them afterwards.
        full = f"{header}\n{body}"
        transpiled_list = sqlglot.transpile(full, read='tsql', write='postgres')
        if not transpiled_list:
            raise RuntimeError('sqlglot failed to transpile code')
        transpiled = transpiled_list[0]
    except Exception:
        # Fallback: return heuristic result
        return header, body
    # Split back into header and body at the first BEGIN
    parts = transpiled.split('BEGIN', 1)
    if len(parts) == 2:
        header_sql = parts[0].strip()
        body_sql = 'BEGIN' + parts[1]
    else:
        header_sql = transpiled
        body_sql = ''
    return header_sql.strip(), body_sql.strip()


def convert_tsql(tsql: str, strategy: Optional[Callable[[str], Tuple[str, str]]] = None) -> Tuple[str, str]:
    """Primary entry point for converting T‑SQL into PL/pgSQL.

    Parameters
    ----------
    tsql : str
        The source T‑SQL code for a single function, procedure or trigger.
    strategy : Callable[[str], Tuple[str, str]], optional
        A conversion strategy function.  If ``None`` (the default) the
        heuristic strategy is used.  Users may pass ``convert_simple`` or
        ``convert_sqlglot_fallback`` to experiment with alternative
        behaviours, or provide their own callable.  The function must take
        the T‑SQL string and return a tuple ``(header_sql, body_sql)``.

    Returns
    -------
    Tuple[str, str]
        The rewritten header and body suitable for execution in PostgreSQL.
    """
    if strategy is None:
        strategy = convert_heuristic
    return strategy(tsql)


__all__ = [
    'convert_tsql', 'convert_simple', 'convert_heuristic', 'convert_sqlglot_fallback'
]