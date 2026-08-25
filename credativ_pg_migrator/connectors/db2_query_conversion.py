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
The query conversion of the three Db2 flavours - LUW, for i and for z/OS.

They are three connectors and one SQL dialect: the same special registers written without
parentheses, the same labelled durations, the same isolation clause at the end of a
statement, the same SYSIBM.SYSDUMMY1. What differs between them is what each one adds on
top - the system names and the CONCAT operator of Db2 for i, the optimizer hints of z/OS -
and that is the only thing each connector implements for itself.

The part which matters most here is `prepare_query_for_parsing()`. No parser of this
migrator models Db2, so a statement of it is read as PostgreSQL, and PostgreSQL cannot read
'CURRENT DATE - 12 MONTHS' or 'WITH UR'. Without the preparation every second statement of
a Db2 application would be reported as one the migrator cannot read - which would be an
answer about the parser and not about the statement.
"""

import re

import sqlglot
from sqlglot import exp

from credativ_pg_migrator.database_connector import first_line


## The special registers of Db2, which are written without parentheses. PostgreSQL has the
## same values under a name with an underscore; a register it does not have is answered with
## the closest thing it does have, and the difference is reported as a warning.
SPECIAL_REGISTERS = (
    (r'CURRENT\s+TIMESTAMP\s+WITH\s+TIME\s+ZONE', 'CURRENT_TIMESTAMP', None),
    (r'CURRENT\s+TIMESTAMP', 'CURRENT_TIMESTAMP', None),
    (r'CURRENT\s+DATE', 'CURRENT_DATE', None),
    (r'CURRENT\s+TIME', 'CURRENT_TIME', None),
    (r'CURRENT\s+SCHEMA', 'CURRENT_SCHEMA', None),
    (r'CURRENT\s+USER', 'CURRENT_USER', None),
    (r'SESSION_USER', 'SESSION_USER', None),
    (r'CURRENT\s+SQLID', 'CURRENT_USER',
     "CURRENT SQLID is the authorization id a statement runs under and decides which schema "
     "an unqualified name is resolved in; it is converted to CURRENT_USER, which is the "
     "closest PostgreSQL has - the schema is resolved through the search_path there"),
    (r'CURRENT\s+SERVER', 'current_database()',
     "CURRENT SERVER is the name of the Db2 subsystem or database server; it is converted to "
     "current_database(), which names the database and not the server"),
    (r'CURRENT\s+TIMEZONE', 'CURRENT_SETTING(\'TimeZone\')',
     "CURRENT TIMEZONE is an interval in Db2 and the name of the zone in PostgreSQL - the "
     "converted statement gives back a different kind of value"),
)

## '<expression> + 12 MONTHS' - the labelled duration of Db2. PostgreSQL needs an interval.
LABELLED_DURATION = re.compile(
    r'(?i)([+-])\s*(\d+)\s+(YEARS?|MONTHS?|DAYS?|HOURS?|MINUTES?|SECONDS?|MICROSECONDS?)\b')

## the same with something which is not a number - an interval cannot be built from it
LABELLED_DURATION_EXPRESSION = re.compile(
    r'(?i)([+-])\s*([A-Za-z_][A-Za-z_0-9.]*|\([^()]*\))\s+'
    r'(YEARS?|MONTHS?|DAYS?|HOURS?|MINUTES?|SECONDS?|MICROSECONDS?)\b')

## the isolation clause at the end of a statement: WITH UR / CS / RS / RR
ISOLATION_CLAUSE = re.compile(r'(?i)\s+WITH\s+(UR|CS|RS|RR)\b'
                              r'(\s+USE\s+AND\s+KEEP\s+\w+\s+LOCKS\b)?\s*(?=;?\s*$)')

## the optimizer hints, which have no counterpart and change nothing about the result
OPTIMIZE_FOR = re.compile(r'(?i)\s+OPTIMIZE\s+FOR\s+\d+\s+ROWS?\b')
FETCH_ONLY = re.compile(r'(?i)\s+FOR\s+(READ|FETCH)\s+ONLY\b')
QUERYNO = re.compile(r'(?i)\s+QUERYNO\s+\d+\b')
SKIP_LOCKED = re.compile(r'(?i)\s+SKIP\s+LOCKED\s+DATA\b')

## the one row table of Db2 - PostgreSQL needs no table for a select without one
SYSDUMMY = re.compile(r'(?i)\s+FROM\s+SYSIBM[./]SYSDUMMY1\b')

## 'DAYS(a) - DAYS(b)' is how a Db2 application counts the days between two dates. PostgreSQL
## subtracts two dates directly and answers with the number of days, so the pair becomes one
## subtraction; the cast keeps the answer an integer when the columns are timestamps.
DAYS_BETWEEN = re.compile(r'(?i)\bDAYS\s*\(\s*([^()]+?)\s*\)\s*-\s*DAYS\s*\(\s*([^()]+?)\s*\)')
DAYS_ALONE = re.compile(r'(?i)\bDAYS\s*\(')


## The functions of Db2 and what they are called in PostgreSQL. One dialect, one mapping -
## it used to stand three times, and the copy of Db2 for i had drifted: it mapped POSSTR to
## POSITION, whose arguments are written the other way round and with IN between them, so the
## converted statement was not valid PostgreSQL at all.
DB2_FUNCTION_MAPPING = {
    # --- Special Registers (Session Variables) ---
    "CURRENT SQLID": "CURRENT_USER",
    "CURRENT USER": "CURRENT_USER",
    "USER": "SESSION_USER",          # SESSION_USER tracks the original login role
    "CURRENT DATE": "CURRENT_DATE",
    "CURRENT TIME": "CURRENT_TIME",
    "CURRENT TIMESTAMP": "CURRENT_TIMESTAMP",
    "CURRENT SCHEMA": "CURRENT_SCHEMA",
    "CURRENT SERVER": "current_database()",

    # --- Null Handling & Control Flow ---
    "VALUE(": "COALESCE(",
    "IFNULL(": "COALESCE(",
    "NVL(": "COALESCE(",

    # --- String Functions ---
    "SUBSTR(": "SUBSTRING(",
    "POSSTR(": "STRPOS(",       # POSSTR of Db2 takes (source, search), as STRPOS does
    "LOCATE(": "POSITION(",     # LOCATE of Db2 takes (search, source)
    "UCASE(": "UPPER(",
    "LCASE(": "LOWER(",
    "STRIP(": "TRIM(",
    "LENGTH(": "LENGTH(",
    "CONCAT(": "CONCAT(",
    "VARCHAR_FORMAT(": "TO_CHAR(",
    "TIMESTAMP_FORMAT(": "TO_TIMESTAMP(",
    "LISTAGG(": "STRING_AGG(",

    # --- Date and Time Functions ---
    "YEAR(": "EXTRACT(YEAR FROM ",
    "MONTH(": "EXTRACT(MONTH FROM ",
    "DAY(": "EXTRACT(DAY FROM ",
    "HOUR(": "EXTRACT(HOUR FROM ",
    "MINUTE(": "EXTRACT(MINUTE FROM ",
    "SECOND(": "EXTRACT(SECOND FROM ",

    # --- Math & Numeric Functions ---
    "CEILING(": "CEIL(",
    "TRUNCATE(": "TRUNC(",
    "RAND()": "RANDOM()",
    "DECFLOAT(": "NUMERIC(",
}


## YEAR(x), MONTH(x) and their siblings are modelled by the SQL parser as nodes of their
## own rather than as an unknown function call, so a mapping by name never reaches them - and
## the PostgreSQL generator writes them back as YEAR(x), which PostgreSQL does not have. They
## are rewritten as EXTRACT(part FROM x), which is what they mean.
DATE_PART_NODES = {getattr(exp, name): part for name, part in (
    ('Year', 'year'), ('Month', 'month'), ('Day', 'day'),
    ('Hour', 'hour'), ('Minute', 'minute'), ('Second', 'second'),
    ('Quarter', 'quarter'), ('Week', 'week'),
) if hasattr(exp, name)}


class Db2QueryConversion:
    """
    Mixed into the three Db2 connectors. It expects the connector to provide
    `convert_statement_code(settings)` - the conversion the query of a view is given - and
    `replace_outside_string_literals(code, pattern, replacement)`.
    """

    def query_conversion_supported(self):
        return True

    def get_sql_functions_mapping(self, settings):
        """
        The functions of Db2 and their PostgreSQL counterparts, the same for the three
        flavours. A connector which needs more adds them to a copy of this.
        """
        target_db_type = settings['target_db_type']
        if target_db_type != 'postgresql':
            self.config_parser.print_log_message(
                'ERROR', f"{type(self).__name__}: get_sql_functions_mapping: "
                         f"Unsupported target database type: {target_db_type}")
            return {}
        return dict(DB2_FUNCTION_MAPPING)

    def replace_outside_string_literals(self, code, pattern, replacement):
        """
        re.sub over the parts of the statement which are not inside a string literal, so
        that a keyword is rewritten and the content of a literal is not. Two of the three
        connectors carry their own copy of this; it stands here for the third and for
        everything in this module.
        """
        if not code:
            return code
        ## re.split with a capturing group answers [code, literal, code, literal, ..., code],
        ## so every even index is a part outside of a string literal
        parts = re.split(r"('(?:[^']|'')*')", code)
        for index in range(0, len(parts), 2):
            parts[index] = re.sub(pattern, replacement, parts[index])
        return ''.join(parts)

    def db2_date_part_to_extract(self, node):
        """
        YEAR(x) of Db2 as EXTRACT(YEAR FROM x). Called from the function transformation of
        each connector, before the mapping by name - a node of its own never carries a name
        the mapping could look up.
        """
        part = DATE_PART_NODES.get(type(node))
        if part is None:
            return node
        argument = node.args.get('this')
        if argument is None:
            return node
        return exp.Extract(this=exp.Identifier(this=part, quoted=False), expression=argument)

    ## ------------------------------------------------------------------ preparation

    def db2_dialect_preparation(self, code):
        """
        What this flavour of Db2 adds on top of the shared preparation. Db2 for i strips its
        system names and rewrites its CONCAT operator here, z/OS its own clauses. The
        default is to add nothing.
        """
        return code

    def prepare_query_for_parsing(self, query_code):
        """
        The statement rewritten into something a PostgreSQL parser can read, without
        converting anything it would not have to convert anyway.

        Every rewrite here is one the conversion has to do in any case: the special
        registers of Db2 are written without parentheses, its labelled durations are
        intervals in PostgreSQL, its isolation clause and its optimizer hints have no
        counterpart at all, and SYSIBM.SYSDUMMY1 does not exist. Doing them before the parse
        is what lets the statement be classified and converted rather than reported as one
        the migrator cannot read.
        """
        if not query_code:
            return query_code

        code = self.db2_dialect_preparation(query_code)

        for pattern, replacement, _warning in SPECIAL_REGISTERS:
            code = self.replace_outside_string_literals(
                code, rf'(?i)\b{pattern}\b', replacement)

        code = self.replace_outside_string_literals(
            code, LABELLED_DURATION.pattern, r"\1 INTERVAL '\2 \3'")

        for pattern in (ISOLATION_CLAUSE, OPTIMIZE_FOR, FETCH_ONLY, QUERYNO, SKIP_LOCKED):
            code = self.replace_outside_string_literals(code, pattern.pattern, '')

        ## 'SELECT CURRENT_DATE FROM SYSIBM.SYSDUMMY1' is 'SELECT CURRENT_DATE' here
        code = self.replace_outside_string_literals(code, SYSDUMMY.pattern, '')

        code = self.replace_outside_string_literals(
            code, DAYS_BETWEEN.pattern, r'(CAST(\1 AS DATE) - CAST(\2 AS DATE))')

        return code

    def db2_conversion_warnings(self, query_code):
        """
        What the reader of the converted statement has to be told: which clause was removed,
        and which register was answered with something close rather than equal.
        """
        warnings = []
        if not query_code:
            return warnings

        isolation = ISOLATION_CLAUSE.search(query_code)
        if isolation:
            level = isolation.group(1).upper()
            warnings.append(
                f"the isolation clause 'WITH {level}' has no counterpart in PostgreSQL and is "
                f"removed - the converted statement reads committed rows, which is what "
                f"PostgreSQL gives a reader by default")
        if OPTIMIZE_FOR.search(query_code):
            warnings.append("'OPTIMIZE FOR n ROWS' is an optimizer hint of Db2 and is removed - "
                            "it changes nothing about the rows the statement gives back")
        if QUERYNO.search(query_code):
            warnings.append("'QUERYNO' names the statement for the Db2 catalog and is removed")
        if SKIP_LOCKED.search(query_code):
            warnings.append("'SKIP LOCKED DATA' is removed - PostgreSQL has 'SKIP LOCKED' only "
                            "together with a row lock, which this step never takes")
        if FETCH_ONLY.search(query_code):
            warnings.append("'FOR READ ONLY' / 'FOR FETCH ONLY' is removed - the statement is a "
                            "read either way")
        if SYSDUMMY.search(query_code):
            warnings.append("SYSIBM.SYSDUMMY1 does not exist in PostgreSQL - the FROM clause is "
                            "removed, a SELECT needs no table there")

        for pattern, _replacement, warning in SPECIAL_REGISTERS:
            if warning and re.search(rf'(?i)\b{pattern}\b', query_code):
                warnings.append(warning)

        prepared = self.prepare_query_for_parsing(query_code)
        if DAYS_ALONE.search(prepared):
            warnings.append("DAYS() has no counterpart in PostgreSQL - the pair 'DAYS(a) - DAYS(b)' "
                            "is converted into the subtraction of the two dates, a single DAYS() "
                            "is not and has to be rewritten by hand")

        expression_duration = LABELLED_DURATION_EXPRESSION.search(query_code)
        if expression_duration and not LABELLED_DURATION.search(expression_duration.group(0)):
            warnings.append(
                f"the labelled duration '{expression_duration.group(0).strip()}' counts a number "
                f"which is not a literal, and an interval of PostgreSQL cannot be built from a "
                f"column - write it as \"x * INTERVAL '1 {expression_duration.group(3).lower()}'\"")

        return warnings

    ## ------------------------------------------------------------------ the entry point

    def convert_query_code(self, settings: dict):
        """
        One statement of an application, converted for PostgreSQL - the same conversion the
        query of a view is given. See the contract in DatabaseConnector.convert_query_code().
        """
        statement_id = settings.get('statement_id', '')
        warnings = self.db2_conversion_warnings(settings['query_code'])
        try:
            converted = self.convert_statement_code({
                'view_code': settings['query_code'],
                'source_schema_name': settings['source_schema_name'],
                'target_schema_name': settings['target_schema_name'],
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
            'DEBUG', f"{type(self).__name__}: convert_query_code: {statement_id}: {converted}")
        return {'code': converted, 'converted': True, 'warnings': warnings, 'error': None}
