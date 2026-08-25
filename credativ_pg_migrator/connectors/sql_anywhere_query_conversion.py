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
The SQL conversion of SAP SQL Anywhere: the query of a view and the statement of an
application, which are given the same conversion.

It stands in a module of its own and not in the connector because the connector imports
`sqlanydb`, which is not a dependency of this migrator - it is installed by whoever migrates a
SQL Anywhere database. The conversion is a transformation of text and needs no driver, so it
is mixed into the connector from here and can be tested on a machine which has no SQL Anywhere
client at all. It is the same reason the Db2 and the Oracle modules exist.

SQL Anywhere is a relative of T-SQL and is read as T-SQL, which reads most of a statement
correctly. What it does not read at all is rewritten in `prepare_query_for_parsing()`: the
'*=' outer join of the Watcom family, the 'TOP n START AT m' paging, the STRING() which
concatenates and the 'IF ... ENDIF' which is an expression there and a statement everywhere
else.

What the parser reads and answers with something else is rewritten in the parsed statement.
The one which matters most is LOCATE: `LOCATE(email, '@')` searches the second argument in the
first, and a T-SQL parser reads it the other way round - the converted statement is valid,
answers 0 for every row, and nothing about it looks wrong.
"""

import re

import sqlglot
from sqlglot import exp

from credativ_pg_migrator.database_connector import first_line
from credativ_pg_migrator.query_conversion import outer_joins
from credativ_pg_migrator.query_conversion.outer_joins import outer_join_warnings


## The format codes of DATEFORMAT() and what to_char() of PostgreSQL calls them. 'HH' is the
## hour of a 24 hour clock in SQL Anywhere and the hour of a 12 hour clock in PostgreSQL, and
## the minute is 'NN' there and 'MI' here - a format handed over unchanged answers another
## string, so a code which is not here stops the conversion instead.
DATE_FORMAT_CODES = (
    ('YYYY', 'YYYY'), ('YY', 'YY'),
    ('MMM', 'Mon'), ('MM', 'MM'),
    ('DDD', 'DDD'), ('DD', 'DD'),
    ('HH', 'HH24'), ('NN', 'MI'), ('SS', 'SS'),
)
DATE_FORMAT_LITERAL = re.compile(r'(?i)[A-Z]+')

## What SQL Anywhere writes and PostgreSQL has nothing for. A statement which still holds one
## of them after the conversion is reported as NOT CONVERTED with the reason.
WITHOUT_COUNTERPART = (
    (r'(?i)(?<![\w.])NUMBER\s*\(\s*\*\s*\)',
     "NUMBER(*) numbers the rows of a result while they are being read, and PostgreSQL has "
     "nothing which does that - row_number() OVER () numbers them in an order the query has "
     "to name, and which order NUMBER(*) used is not written anywhere"),
    (r'(?i)(?<![\w.])(?:UUIDTOSTR|STRTOUUID|BASE64_ENCODE|BASE64_DECODE|COMPRESS|DECOMPRESS'
     r'|CSCONVERT|HASH|SIMILAR|SOUNDEX_ANSI|WATCOMSQL|XMLAGG|XMLCONCAT|XMLELEMENT_SA'
     r'|SA_SPLIT_LIST|CONNECTION_PROPERTY|DB_PROPERTY|PROPERTY|NEXT_CONNECTION|EVENT_PARAMETER'
     r'|VAREXISTS|SQLDIALECT)\s*\(',
     "the function has no counterpart in PostgreSQL - the property and connection functions "
     "answer what the SQL Anywhere server knows about itself, and the encodings and hashes "
     "answer another string than anything of PostgreSQL does"),
    (r'(?i)(?<![\w.])(?:DATEFORMAT|LOCATE|LIST|NOW|TODAY|STRING)\s*\(',
     "the conversion could not write this call for PostgreSQL - DATEFORMAT() only where every "
     "code of its format has a counterpart in to_char(), LIST() only where its arguments are "
     "an expression and a separator, and LOCATE() only with two arguments"),
    (r'/\*\s*(?:left|right)_outer\s*\*/',
     "the outer join written '*=' or '=*' could not be rewritten as a LEFT JOIN / RIGHT JOIN "
     "- a condition which stands under an OR does not say which of its parts belong to the "
     "join, and left in the WHERE clause it is an inner join, which answers fewer rows and "
     "looks healthy while doing it"),
)

WITHOUT_COUNTERPART = tuple((re.compile(pattern), reason)
                            for pattern, reason in WITHOUT_COUNTERPART)

## What is converted and still means something else afterwards. The '+' which concatenates
## is not looked for here but in the parsed statement: the word '+' stands in the comment
## above many a statement, and a warning which fires on a comment is a warning nobody reads.
XML_TYPE = re.compile(r'(?i)(?<![\w.])XML')


## The functions of SQL Anywhere which are renamed one for one. Six entries of the mapping the
## connector carried are not here, and each of them was a defect:
##
##   'timestamp' and 'user' were matched as whole words anywhere in the statement, so
##   `CAST(a AS timestamp)` became `CAST(a AS CURRENT_TIMESTAMP)` and a table named "user"
##   became "CURRENT_USER". PostgreSQL reads USER and CURRENT_TIMESTAMP itself; the spellings
##   which really need converting are the two word ones, which are still here.
##
##   'locate(' and 'charindex(' were renamed to 'position(', whose arguments are written with
##   IN between them - `position(a, '@')` is not valid PostgreSQL at all. Both are read by the
##   parser instead, and LOCATE is rewritten further down because its arguments are the other
##   way round.
##
##   'dateformat(' was renamed to 'to_char(' and its format was handed over unchanged, so
##   `DATEFORMAT(d, 'HH:NN')` answered the hour of a twelve hour clock and the letters 'NN'.
##   The format is converted code by code further down.
##
##   'string(' is done before the statement is parsed - a T-SQL parser reads STRING as a cast.
SQL_ANYWHERE_FUNCTION_MAPPING = {
    'current timestamp': 'CURRENT_TIMESTAMP',
    'current_timestamp': 'CURRENT_TIMESTAMP',
    'current date': 'CURRENT_DATE',
    'current_date': 'CURRENT_DATE',
    'current time': 'CURRENT_TIME',
    'current_time': 'CURRENT_TIME',
    'current user': 'CURRENT_USER',
    'current_user': 'CURRENT_USER',
    'last user': 'CURRENT_USER',
    'current publisher': 'CURRENT_USER',
    'getutcdate()': "timezone('UTC', now())",
    'getdate()': 'CURRENT_TIMESTAMP',
    'now()': 'CURRENT_TIMESTAMP',
    'today()': 'CURRENT_DATE',
    'user_name()': 'CURRENT_USER',
    'user_id()': 'CURRENT_USER',
    'year(': 'extract(year from ',
    'month(': 'extract(month from ',
    'day(': 'extract(day from ',
    'len(': 'length(',
    'length(': 'length(',
    'byte_length(': 'octet_length(',
    'isnull(': 'coalesce(',
    'ifnull(': 'coalesce(',
    'stuff(': 'overlay(',
    'datepart(yyyy,': "date_part('year',",
    'datepart(year,': "date_part('year',",
    'datepart(month,': "date_part('month',",
    'datepart(yy,': "date_part('year',",
    'datepart(qq,': "date_part('quarter',",
    'datepart(mm,': "date_part('month',",
    'datepart(dy,': "date_part('doy',",
    'datepart(dd,': "date_part('day',",
    'datepart(wk,': "date_part('week',",
    'datepart(hh,': "date_part('hour',",
    'datepart(mi,': "date_part('minute',",
    'datepart(ss,': "date_part('second',",
    'datepart(ms,': "date_part('milliseconds',",
}


class SqlAnywhereQueryConversion:
    """
    Mixed into the SQL Anywhere connector. It expects the connector to bring what every
    connector has - `config_parser` and `apply_sql_functions_mapping()` of the base class.
    """

    def query_conversion_supported(self):
        return True

    def get_sql_functions_mapping(self, settings):
        """
        The functions of SQL Anywhere which are renamed one for one. What needs more than a
        new name is rewritten in the parsed statement further down.
        """
        target_db_type = settings.get('target_db_type', 'postgresql')
        if target_db_type != 'postgresql':
            self.config_parser.print_log_message(
                'ERROR', f"sql_anywhere_connector: get_sql_functions_mapping: "
                         f"Unsupported target database type: {target_db_type}")
            return {}
        return dict(SQL_ANYWHERE_FUNCTION_MAPPING)

    ## ------------------------------------------------------------------ the preparation

    def prepare_query_for_parsing(self, query_code):
        """
        The statement rewritten into something a T-SQL parser can read, without converting
        anything it would not have to convert anyway.

        SQL Anywhere is read as T-SQL and most of a statement needs nothing. Four things are
        not T-SQL at all: the '*=' outer join, which becomes an equality carrying a marker
        saying which side was outer - the conversion turns the marker into a LEFT or RIGHT
        JOIN; the 'START AT m' of its paging, which is written at the front of the statement
        and at the end in PostgreSQL; the STRING() which concatenates, and which a T-SQL
        parser reads as the cast of a single value; and the 'IF ... ENDIF', which is an
        expression in SQL Anywhere and a statement in every other dialect.

        It is used by the conversion itself and by the query conversion, which has to
        classify a statement before it converts it.
        """
        if not query_code:
            return query_code

        prepared = re.sub(r'\*=', '= /* left_outer */', query_code)
        prepared = re.sub(r'=\*', '= /* right_outer */', prepared)
        prepared = self.rewrite_start_at(prepared)
        ## STRING(a, b, c) concatenates and skips a NULL argument, which is what concat() of
        ## PostgreSQL does; a T-SQL parser reads STRING as a cast and stops at the second
        ## argument
        prepared = self.replace_outside_string_literals(
            prepared, r'(?i)(?<![\w.])STRING\s*\(', 'CONCAT(')
        ## COUNT() of SQL Anywhere is COUNT(*)
        prepared = self.replace_outside_string_literals(
            prepared, r'(?i)(?<![\w.])COUNT\s*\(\s*\)', 'COUNT(*)')
        ## the pseudo functions written with a star
        prepared = self.replace_outside_string_literals(
            prepared, r'(?i)(?<![\w.])NOW\s*\(\s*\*\s*\)', 'CURRENT_TIMESTAMP')
        prepared = self.replace_outside_string_literals(
            prepared, r'(?i)(?<![\w.])TODAY\s*\(\s*\*\s*\)', 'CURRENT_DATE')
        prepared = self.rewrite_if_endif(prepared)
        ## TIMESTAMP is a date and a time in SQL Anywhere and the row version of a table in
        ## T-SQL, which is read as a string of bytes - a cast to it came out as CAST(x AS BYTEA)
        prepared = self.replace_outside_string_literals(
            prepared, r'(?is)\bCAST\s*\(\s*(.+?)\s+AS\s+TIMESTAMP\s*\)',
            r'CAST(\1 AS DATETIME)')
        prepared = self.replace_outside_string_literals(
            prepared, r'(?i)\bCONVERT\s*\(\s*TIMESTAMP\s*,', 'CONVERT(DATETIME,')
        return prepared

    def replace_outside_string_literals(self, code, pattern, replacement):
        """re.sub over the parts of the statement which are not inside a string literal."""
        if not code:
            return code
        ## re.split with a capturing group answers [code, literal, code, ..., code], so every
        ## even index is a part outside of a string literal
        parts = re.split(r"('(?:[^']|'')*')", code)
        for index in range(0, len(parts), 2):
            parts[index] = re.sub(pattern, replacement, parts[index])
        return ''.join(parts)

    def rewrite_start_at(self, code):
        """
        `SELECT TOP n START AT m` of SQL Anywhere as the OFFSET of the target.

        The two numbers stand in front of the select list there and at the end of the
        statement here. TOP is read by the T-SQL parser and becomes a LIMIT on its own, so
        only the START AT has to be moved - to the end of the query it belongs to, behind the
        ORDER BY which decides which rows the two mean. SQL Anywhere counts its first row as
        1, PostgreSQL counts the rows it skips, so 'START AT 101' skips a hundred.
        """
        pattern = re.compile(r'(?i)(?<![\w.])START\s+AT\s+(\d+)')
        for _ in range(20):
            match = pattern.search(self.sql_without_literals_and_comments(code))
            if match is None:
                return code
            skipped = int(match.group(1)) - 1
            end = self.select_block_end(code, match.end())
            code = (code[:match.start()] + code[match.end():end]
                    + (f" OFFSET {skipped}" if skipped > 0 else '') + code[end:])
        return code

    def select_block_end(self, code, start):
        """
        Where the query which holds the given position ends: at the parenthesis which closes
        it, at the semicolon which ends the statement, or at the end of the text.
        """
        masked = self.sql_without_literals_and_comments(code)
        depth = 0
        for position in range(start, len(masked)):
            character = masked[position]
            if character == '(':
                depth += 1
            elif character == ')':
                if depth == 0:
                    return position
                depth -= 1
            elif character == ';' and depth == 0:
                return position
        return len(code)

    def rewrite_if_endif(self, code):
        """
        `IF condition THEN a ELSE b ENDIF` of SQL Anywhere as the CASE expression it is. It
        is an expression there and the beginning of a statement in every other dialect, so a
        parser stops at it.

        The parts are read in the text as it stands and not in the blanked one - the branches
        of such an expression are usually literals, and a blanked literal says nothing about
        where it ends. Whether the IF itself is inside a literal is what the blanked text is
        asked.

        An IF inside another IF is not rewritten: the first ENDIF ends the outer one by this
        reading, and the statement is then reported as one which could not be parsed - which
        is the answer it deserves either way.
        """
        pattern = re.compile(r'(?is)(?<![\w.])IF\s+(?P<condition>.+?)\s+THEN\s+(?P<then>.+?)'
                             r'(?:\s+ELSE\s+(?P<otherwise>.+?))?\s+ENDIF(?![\w.])')
        position = 0
        for _ in range(20):
            match = pattern.search(code, position)
            if match is None:
                return code
            masked = self.sql_without_literals_and_comments(code)
            if masked[match.start():match.start() + 2].upper() != 'IF':
                ## the IF stands inside a literal or a comment, where it is text
                position = match.start() + 2
                continue
            otherwise = match.group('otherwise')
            written = (f"CASE WHEN {match.group('condition')} THEN {match.group('then')}"
                       + (f" ELSE {otherwise}" if otherwise else '') + " END")
            code = code[:match.start()] + written + code[match.end():]
            position = match.start() + len(written)
        return code

    ## ------------------------------------------------------------------ the rewrites

    def postgres_expression(self, text):
        """A fragment of PostgreSQL SQL as a node of the statement being converted."""
        return sqlglot.parse_one(text, read='postgres')

    def written_as_postgres(self, node):
        """One node of the parsed statement as the PostgreSQL text it becomes."""
        return node.sql(dialect='postgres')

    def rewrite_sql_anywhere_expression(self, node, report):
        """
        One node of the parsed statement, rewritten where a T-SQL parser answers something
        SQL Anywhere does not mean. What cannot be written is left standing, where
        sql_anywhere_conversion_blockers() finds it.
        """
        if isinstance(node, exp.Add) and any(
                isinstance(side, exp.Literal) and side.is_string
                for side in (node.this, node.expression)):
            ## '+' concatenates strings in SQL Anywhere and adds numbers in PostgreSQL
            report['warnings'].append(
                f"\"{self.written_as_postgres(node)}\" adds with '+', which concatenates "
                f"strings in SQL Anywhere and adds numbers in PostgreSQL - the target answers "
                f"'operator does not exist: text + text' or adds the two as numbers. Write "
                f"'||' where the '+' was a concatenation")
            return node

        if isinstance(node, exp.StrPosition):
            return self.rewrite_locate(node, report)
        if isinstance(node, exp.List):
            return self.rewrite_list(node, report)
        if isinstance(node, exp.Anonymous):
            name = (node.this or '').upper() if isinstance(node.this, str) else ''
            if name == 'DATEFORMAT':
                return self.rewrite_dateformat(node, report)
        return node

    def rewrite_locate(self, node, report):
        """
        `LOCATE(string, pattern)` of SQL Anywhere searches the *second* argument in the
        first; POSITION of the standard is written the other way round, and a T-SQL parser
        reads the two as if they were the same. The converted statement is valid, answers 0
        for every row and looks like a statement which simply finds nothing.
        """
        haystack = node.args.get('this')
        needle = node.args.get('substr')
        if haystack is None or needle is None or node.args.get('position') is not None:
            report['notes'].append(
                "LOCATE() was called with something else than a string and a pattern - the "
                "third argument of SQL Anywhere, which says where to start and may count from "
                "the end of the string, has no counterpart in PostgreSQL")
            return node
        ## the parser filled 'this' with the pattern and 'substr' with the string, because
        ## that is what LOCATE means in T-SQL - here they are the other way round
        return self.postgres_expression(
            f"POSITION({self.written_as_postgres(haystack)} IN "
            f"{self.written_as_postgres(needle)})")

    def rewrite_dateformat(self, node, report):
        """
        `DATEFORMAT(d, 'YYYY-MM-DD')` as the to_char() of PostgreSQL. The codes are not the
        same: the hour of a 24 hour clock is 'HH' there and 'HH24' here, and the minute is
        'NN' there and 'MI' here. A format holding a code with no counterpart is left as it
        is - to_char() would answer it as literal text.
        """
        arguments = node.expressions or []
        if len(arguments) != 2 or not isinstance(arguments[1], exp.Literal) \
                or not arguments[1].is_string:
            report['notes'].append(
                "DATEFORMAT() was called with something else than a value and a format "
                "written out - which codes the format holds is only known when it is read")
            return node

        written = arguments[1].this
        converted = []
        position = 0
        while position < len(written):
            for code, counterpart in DATE_FORMAT_CODES:
                if written[position:position + len(code)].upper() == code:
                    converted.append(counterpart)
                    position += len(code)
                    break
            else:
                if DATE_FORMAT_LITERAL.match(written[position]):
                    report['notes'].append(
                        f"the format '{written}' of DATEFORMAT() holds the code "
                        f"'{written[position]}', which to_char() of PostgreSQL does not know - "
                        f"it would be answered as the letter it is")
                    return node
                converted.append(written[position])
                position += 1

        return self.postgres_expression(
            f"TO_CHAR({self.written_as_postgres(arguments[0])}, '{''.join(converted)}')")

    def rewrite_list(self, node, report):
        """
        `LIST(expr [, separator])` of SQL Anywhere as the string_agg() of PostgreSQL. The
        separator of LIST is a comma when it is not given; string_agg() has no default and
        needs one written out.
        """
        arguments = node.expressions or []
        if not 1 <= len(arguments) <= 2:
            report['notes'].append(
                "LIST() was called with something else than an expression and a separator")
            return node
        separator = (self.written_as_postgres(arguments[1]) if len(arguments) == 2 else "','")
        return self.postgres_expression(
            f"STRING_AGG(CAST({self.written_as_postgres(arguments[0])} AS TEXT), {separator})")

    ## ------------------------------------------------------------------ the conversion

    def convert_statement_with_report(self, settings):
        """
        The conversion itself: the statement of SQL Anywhere as the statement of the target,
        together with what happened to it.

        Answers `(code, report)`, where the report holds `unconverted_joins` and `notes`.
        Raises ValueError when the statement could not be parsed; the exception carries
        `partial_code`, which is what the conversion of a view falls back to.
        """
        code = settings.get('view_code') or ''
        report = {'unconverted_joins': 0, 'notes': [], 'warnings': []}
        prepared = self.prepare_query_for_parsing(code)

        try:
            ast = sqlglot.parse_one(prepared, read='tsql')
        except Exception as e:
            error = ValueError(f"the statement could not be parsed as T-SQL: {first_line(e)}")
            error.partial_code = self.sql_anywhere_textual_conversion(code, settings)
            raise error

        if ast is None:
            error = ValueError('the parser read no statement at all')
            error.partial_code = self.sql_anywhere_textual_conversion(code, settings)
            raise error

        converted_joins = set()
        ast, report['unconverted_joins'] = outer_joins.convert_marked_outer_joins(
            ast, converted_joins)
        ## SQL Anywhere writes '*=' as the Transact-SQL compatibility syntax of Sybase ASE, and
        ## reads it the way ASE does: a restriction on the inner table belongs to the join. In
        ## the WHERE clause of PostgreSQL it is applied to the result of the join, where it
        ## throws away exactly the rows the outer join added - the LEFT JOIN would be an inner
        ## join again, valid and answering fewer rows. Only the joins this conversion made out
        ## of a '*=' are touched: a join written as ANSI in the source means the same on both
        ## sides, and its WHERE clause is left alone.
        ast, moved = outer_joins.move_inner_table_predicates(ast, converted_joins)
        if moved:
            report['moved_predicates'] = moved
        ast = ast.transform(lambda node: self.rewrite_sql_anywhere_expression(node, report))
        converted = ast.sql(dialect='postgres')
        ## A marker which is still standing says the outer join was not rewritten - and what
        ## stands around it is the comma join it started from, which PostgreSQL creates as an
        ## INNER join without complaint. The statement is refused rather than converted into
        ## that, for the view path and the query path alike.
        finished = self.finish_statement_code(converted, settings)
        marker_message = outer_joins.unconverted_marker_message(finished)
        if marker_message:
            error = ValueError(marker_message)
            error.outer_join_failure = True
            error.partial_code = self.sql_anywhere_textual_conversion(code, settings)
            raise error
        return finished, report

    def finish_statement_code(self, code, settings):
        """
        What is done to the text whether or not it could be parsed: the function mapping of
        the connector and the schema of the source, which SQL Anywhere writes in front of
        every name of its catalog and the target does not need.
        """
        code = outer_joins.tidy_boolean_placeholders(code)
        source_schema = settings.get('source_schema_name', '')
        if source_schema:
            code = re.sub(rf'(?i)"{re.escape(source_schema)}"\.', '', code)
            code = re.sub(rf'(?i)(?<![\w."]){re.escape(source_schema)}\.', '', code)
        return self.apply_sql_functions_mapping(code, settings)

    def sql_anywhere_textual_conversion(self, code, settings):
        """
        What the conversion can do to a statement no parser could read: the rewrites which
        need no parse. It is what a view falls back to, and it is what the whole conversion
        of a view was before this module existed.
        """
        if not code:
            return code
        converted = self.prepare_query_for_parsing(code)
        ## the double quotes of a function call - "COUNT"( - which SQL Anywhere writes into
        ## the text of a view
        converted = re.sub(r'"([A-Za-z0-9_]+)"\s*\(', r'\1(', converted)
        converted = self.replace_outside_string_literals(
            converted, r'(?i)(?<![\w.])LIST\s*\(\s*([^\s,]+)\s*,', r'string_agg(\1::text,')
        ## This path has no parse and therefore no way to rewrite an outer join, so the marking
        ## the preparation did is undone again. Left in, PostgreSQL would read the marker as a
        ## comment and create the comma join around it - an INNER join, without complaint,
        ## answering fewer rows. The operator of the source fails loudly instead, which is what
        ## a statement nothing could convert has to do.
        converted = outer_joins.unmark_tsql_outer_joins(converted)
        return self.finish_statement_code(converted, settings)

    def convert_statement_code(self, settings: dict):
        """
        One statement of SQL Anywhere, converted for the target - the query of a view and the
        statement of an application are given the same conversion. Raises ValueError when the
        statement could not be parsed.
        """
        return self.convert_statement_with_report(settings)[0]

    def convert_view_code(self, settings: dict):
        """
        The query of a view, converted for the target and written as the statement which
        creates it.

        A statement which cannot be parsed is answered with what the conversion can do
        without a parser, exactly as before this module existed - the view is then created
        with whatever that is worth, or reported as failed by the migration.
        """
        code = settings.get('view_code')
        if not code:
            return code

        try:
            converted, report = self.convert_statement_with_report(settings)
            for note in report['notes'] + report['warnings'] + outer_join_warnings(report):
                self.config_parser.print_log_message(
                    'WARNING', f"sql_anywhere_connector: convert_view_code: {note}")
            if report['unconverted_joins']:
                self.config_parser.print_log_message(
                    'WARNING', f"sql_anywhere_connector: convert_view_code: "
                               f"{report['unconverted_joins']} outer join condition(s) written "
                               f"'*=' could not be converted to a LEFT JOIN and stay inner "
                               f"join conditions. Manual review required.")
        except ValueError as e:
            self.config_parser.print_log_message(
                'WARNING', f"sql_anywhere_connector: convert_view_code: {e}; "
                           f"the statement is converted as far as it can be without a parser. "
                           f"Manual review required.")
            converted = getattr(e, 'partial_code', code)

        ## These two are not the dialect - they are what this migration does to the types of
        ## the source, and a view of the source compares against the value the source had.
        converted = re.sub(r'is_active"\s*=\s*1\b', 'is_active" = true', converted)
        converted = re.sub(r'is_active"\s*=\s*0\b', 'is_active" = false', converted)
        converted = re.sub(r'as\s+varchar\s*\(\s*500\s*\)', 'as text', converted,
                           flags=re.IGNORECASE)

        if not converted.lower().startswith('create'):
            converted = 'CREATE OR REPLACE VIEW ' + converted
        else:
            converted = re.sub(r'(?i)^CREATE\s+(MATERIALIZED\s+)?VIEW',
                               'CREATE OR REPLACE VIEW', converted)
        return converted

    ## ------------------------------------------------------------------ the entry point

    def sql_anywhere_conversion_warnings(self, query_code):
        """
        What the reader of the converted statement has to be told: what was converted and
        still means something else on the target.
        """
        warnings = []
        if not query_code:
            return warnings
        statement = self.sql_without_literals_and_comments(query_code)

        if XML_TYPE.search(statement):
            warnings.append(
                "the XML type and the XML functions of SQL Anywhere are not the XML of "
                "PostgreSQL - what the migration made of the column decides whether the "
                "statement still reads it")
        return warnings

    def sql_anywhere_conversion_blockers(self, converted_code, report=None):
        """
        The reasons the converted statement may not be offered as a conversion: a construct of
        SQL Anywhere which is still standing in it, and what the conversion itself reported it
        could not write.
        """
        statement = self.sql_without_literals_and_comments(converted_code or '')
        reasons = [reason for pattern, reason in WITHOUT_COUNTERPART
                   ## the marker of an outer join is a comment and is blanked with the others,
                   ## so that one is looked for in the text as it stands
                   if pattern.search(converted_code or '' if 'outer' in pattern.pattern
                                     else statement)]
        if report:
            reasons.extend(report.get('notes') or [])
            if report.get('unconverted_joins'):
                reasons.append(
                    f"{report['unconverted_joins']} outer join condition(s) written '*=' or "
                    f"'=*' could not be rewritten as a LEFT JOIN / RIGHT JOIN - a condition "
                    f"which stands under an OR does not say which of its parts belong to the "
                    f"join, and left in the WHERE clause it is an inner join, which answers "
                    f"fewer rows and looks healthy while doing it")
        return reasons

    def convert_query_code(self, settings: dict):
        """
        One statement of an application, converted for PostgreSQL - the same conversion the
        query of a view is given. See the contract in DatabaseConnector.convert_query_code().
        """
        statement_id = settings.get('statement_id', '')
        warnings = self.sql_anywhere_conversion_warnings(settings['query_code'])
        report = {}
        try:
            converted, report = self.convert_statement_with_report({
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

        warnings.extend(report.get('warnings') or [])
        warnings.extend(outer_join_warnings(report))
        if not (converted or '').strip():
            return {'code': '', 'converted': False, 'warnings': warnings,
                    'error': 'the conversion produced no statement at all'}

        blockers = self.sql_anywhere_conversion_blockers(converted, report)
        if blockers:
            return {'code': '', 'converted': False, 'warnings': warnings,
                    'error': '; '.join(blockers)}

        self.config_parser.print_log_message(
            'DEBUG', f"sql_anywhere_connector: convert_query_code: {statement_id}: {converted}")
        return {'code': converted, 'converted': True, 'warnings': warnings, 'error': None}
