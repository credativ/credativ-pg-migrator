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
The SQL conversion of Oracle: the defining query of a view and the statement of an
application, which are given the same conversion.

It stands in a module of its own and not in the connector because the connector imports
`oracledb`, which is not a dependency of this migrator - it is installed by whoever migrates
an Oracle database. The conversion is a transformation of text and needs no driver, so it is
mixed into the connector from here and can be tested, and asked by the test suite of the
migration repository, on a machine which has no Oracle client at all. It is the same reason
`db2_query_conversion.py` exists.

sqlglot reads Oracle and writes PostgreSQL, so there is nothing to prepare before a statement
can be parsed and no `prepare_query_for_parsing()` here. What the module is about is what the
transpiler leaves standing, and Oracle leaves a great deal standing:

* the hierarchical query (`CONNECT BY`), `ROWNUM`, `ROWID`, a name over a database link, and
  the functions PostgreSQL has nothing for - `MONTHS_BETWEEN`, `SYS_CONTEXT`, `RATIO_TO_REPORT`,
  the `DBMS_` packages. A statement holding one of them is reported as NOT CONVERTED;
* what it writes as a name PostgreSQL does not have. `TRUNC(d, 'MM')` becomes
  `DATE_TRUNC('MM', d)`, and PostgreSQL knows the field as 'month' - the format models of
  Oracle are not the field names of PostgreSQL. `ADD_MONTHS` is not renamed at all. Both are
  rewritten here;
* and what is converted and still means something else afterwards: the empty string, which is
  NULL in Oracle and a value in PostgreSQL; the arithmetic of SYSDATE, which counts days in
  Oracle and answers an interval in PostgreSQL; an identifier quoted in upper case, which
  keeps its case on both sides and no longer finds a name the migration wrote in lower case.
  Each of those is reported as a warning above the statement.
"""

import re

import sqlglot
from sqlglot import exp

from credativ_pg_migrator.database_connector import first_line
from credativ_pg_migrator.query_conversion import outer_joins


## The format models of Oracle TRUNC(date, fmt) and the field PostgreSQL knows them by. A
## model which is not here is not truncated: 'DAY' begins the week on Sunday in Oracle and
## date_trunc('week') begins it on Monday, and a statement which answers another day is worse
## than one which is reported as not converted.
ORACLE_DATE_FORMATS = {
    'CC': 'century', 'SCC': 'century',
    'SYYYY': 'year', 'YYYY': 'year', 'YEAR': 'year', 'SYEAR': 'year',
    'YYY': 'year', 'YY': 'year', 'Y': 'year',
    'Q': 'quarter',
    'MONTH': 'month', 'MON': 'month', 'MM': 'month', 'RM': 'month',
    'IW': 'week',
    'DDD': 'day', 'DD': 'day', 'J': 'day',
    'HH': 'hour', 'HH12': 'hour', 'HH24': 'hour',
    'MI': 'minute',
}
## The fields PostgreSQL really knows, which is what a converted DATE_TRUNC has to name
POSTGRES_DATE_FIELDS = ('microseconds', 'milliseconds', 'second', 'minute', 'hour', 'day',
                        'week', 'month', 'quarter', 'year', 'decade', 'century', 'millennium')
UNKNOWN_DATE_TRUNC = re.compile(
    rf"(?i)\bDATE_TRUNC\s*\(\s*'(?!(?:{'|'.join(POSTGRES_DATE_FIELDS)})\s*')")

## What Oracle writes and PostgreSQL has nothing for. A statement which still holds one of
## them after the conversion is reported as NOT CONVERTED with the reason.
WITHOUT_COUNTERPART = (
    (r'(?i)(?<![\w.$#])(?:CONNECT\s+BY|START\s+WITH|SYS_CONNECT_BY_PATH|CONNECT_BY_ROOT'
     r'|CONNECT_BY_ISLEAF)(?![\w$#])',
     "the hierarchical query of Oracle (CONNECT BY / START WITH) is written in PostgreSQL as "
     "a recursive common table expression, which is another statement and not another "
     "spelling of this one"),
    (r'(?i)(?<![\w.$#])ROWNUM(?![\w$#])',
     "ROWNUM is given to a row before the ORDER BY is applied and LIMIT is applied after it, "
     "so the two answer the same rows only when the statement has no ORDER BY. Which of the "
     "two the statement means is not written anywhere, and a conversion which guesses answers "
     "another set of rows"),
    (r'(?i)(?<![\w.$#])ROWID(?![\w$#])',
     "ROWID is the physical address of a row in Oracle. The ctid of PostgreSQL is not the "
     "same thing - it changes when the row is updated - so the statement has to be rewritten "
     "to read the key of the table"),
    (r'[\w$#)]@[A-Za-z_][\w$#]*',
     "the statement reads a table over a database link, which PostgreSQL has no counterpart "
     "for - the objects of another database are reached through a foreign data wrapper there, "
     "and which table the link stands for is in remote_objects_substitution"),
    (r'(?i)(?<![\w.$#])(?:MONTHS_BETWEEN|RATIO_TO_REPORT|SYS_CONTEXT|USERENV|ORA_HASH|NLSSORT'
     r'|TZ_OFFSET|NUMTODSINTERVAL|NUMTOYMINTERVAL|TO_DSINTERVAL|TO_YMINTERVAL|WM_CONCAT'
     r'|SESSIONTIMEZONE|DBTIMEZONE|VSIZE|DUMP|BFILENAME|POWERMULTISET)\s*\(',
     "the function has no counterpart in PostgreSQL - MONTHS_BETWEEN() answers a fraction "
     "counted in months of 31 days, SYS_CONTEXT() and USERENV() answer what the Oracle "
     "session knows about itself, and neither can be written as an expression of the target"),
    (r'(?i)(?<![\w.$#])(?:DBMS_|UTL_|SYS\.)[A-Za-z_][\w$#]*',
     "the statement calls a package of Oracle (DBMS_, UTL_, SYS.), which is a program of the "
     "server and has no counterpart in the target"),
    (r'(?i)(?<![\w.$#])(?:MULTISET|TABLESAMPLE|KEEP\s*\(\s*DENSE_RANK)(?![\w$#])',
     "MULTISET, the SAMPLE clause and the KEEP (DENSE_RANK ...) aggregate of Oracle have no "
     "counterpart which answers the same rows - the sample of PostgreSQL is written "
     "TABLESAMPLE SYSTEM (n) and draws another one"),
    ## what the conversion tried to write and could not
    (r'(?i)(?<![\w.$#])(?:LISTAGG|ADD_MONTHS)\s*\(',
     "the conversion could not write this call for PostgreSQL: LISTAGG is only converted in "
     "the form 'LISTAGG(expr, delimiter) WITHIN GROUP (ORDER BY ...)', and ADD_MONTHS only "
     "where its arguments are an expression and a number"),
)

WITHOUT_COUNTERPART = tuple((re.compile(pattern), reason)
                            for pattern, reason in WITHOUT_COUNTERPART)

## What is converted and still means something else afterwards
EMPTY_STRING_COMPARISON = re.compile(r"(?:=|<>|!=|\^=)\s*''(?!')")
SYSDATE_ARITHMETIC = re.compile(r'(?i)(?<![\w.$#])(?:SYSDATE|SYSTIMESTAMP|CURRENT_DATE)'
                                r'(?![\w$#])\s*[-+]|[-+]\s*(?<![\w.$#])'
                                r'(?:SYSDATE|SYSTIMESTAMP|CURRENT_DATE)(?![\w$#])')
UPPER_CASE_IDENTIFIER = re.compile(r'"[A-Z][A-Z0-9_$#]*"')
OPTIMIZER_HINT = re.compile(r'/\*\+')
TRUNC_OF_ONE_ARGUMENT = re.compile(r'(?i)(?<![\w.$#])TRUNC\s*\(\s*[^,()]+\s*\)')


class OracleQueryConversion:
    """
    Mixed into the Oracle connector. It expects the connector to bring what every connector
    has - `config_parser` and `apply_sql_functions_mapping()` of the base class.
    """

    ## The source test of §8.1. python-oracledb answers Cursor.parse(), which sends the
    ## statement to the server to be parsed and does not execute it: the names are resolved
    ## and the syntax is checked. EXPLAIN PLAN is deliberately not used - it WRITES a row
    ## into PLAN_TABLE, and this step does not write to the source.
    SOURCE_TEST_PARAMETER_STYLE = 'oracle'

    def source_test_native_mechanism(self):
        return 'Cursor.parse()'

    def test_query_on_source(self, settings):
        body = (settings.get('query_code') or '').rstrip().rstrip(';')
        if not body:
            return 'not run', 'Cursor.parse() was given nothing to send'
        cursor = None
        try:
            cursor = self.source_test_connection().cursor()
        except Exception as e:
            self.close_source_test_connection()
            return 'ERROR', f"the source could not be asked: {first_line(e)}"
        try:
            cursor.parse(body)
            return 'OK', 'Cursor.parse() on the source'
        except Exception as e:
            return 'FAILED', first_line(e)
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def query_conversion_supported(self):
        return True

    def get_sql_functions_mapping(self, settings):
        """ Returns a dictionary of SQL functions mapping for the target database.

        This mapping is applied (via the base apply_sql_functions_mapping, a
        case-insensitive regex substitution) when converting views, functions and
        procedures. It therefore only lists Oracle functions that need an explicit
        rename to a PostgreSQL function with the *same argument order and semantics*
        - anything requiring argument reordering or restructuring (DECODE, NVL2,
        INSTR with 3+ args, MONTHS_BETWEEN, ...) is intentionally left out and is
        either handled by sqlglot during view conversion or flagged for manual review.

        Note: several common Oracle constructs (NVL, SYSDATE, SYSTIMESTAMP, DUAL,
        NEXTVAL/CURRVAL) are already handled by sqlglot (views) and by
        _apply_plsql_substitutions (functions/procedures); they are repeated here as
        harmless no-op-if-already-converted fallbacks for the raw/fallback code path.
        """
        target_db_type = settings['target_db_type']
        if target_db_type == 'postgresql':
            return {
                # Oracle GROUPING_ID(a, b, ...) == PostgreSQL GROUPING(a, b, ...):
                # both return the bitmask of the GROUP BY expressions not present in
                # the current grouping set. sqlglot does not translate GROUPING_ID.
                'grouping_id(': 'grouping(',
                # Null handling / misc functions with identical PostgreSQL equivalents
                'nvl(': 'coalesce(',
                'lengthb(': 'octet_length(',
                'sys_guid()': 'gen_random_uuid()',
                # Date/time pseudo-columns (no parentheses in Oracle)
                'systimestamp': 'current_timestamp',
                'sysdate': 'current_timestamp',
            }
        else:
            self.config_parser.print_log_message('ERROR', f"oracle_connector: get_sql_functions_mapping: Unsupported target database type: {target_db_type}")
            return {}

    ## ------------------------------------------------- what the transpiler cannot read
    def _warn_unconvertible_oracle_sql(self, sql, view_label):
        """Log warnings for Oracle constructs that cannot be reliably auto-converted, so they
        get manual review instead of silently producing wrong PostgreSQL.
        (Oracle (+) outer joins are handled by _convert_marked_outer_joins and warned about
        separately only when a specific condition could not be converted.)"""
        if not sql:
            return
        upper = sql.upper()
        issues = []
        if 'CONNECT BY' in upper or 'START WITH' in upper:
            issues.append("Oracle CONNECT BY / START WITH hierarchical query - needs a PostgreSQL recursive CTE")
        if re.search(r'\bROWNUM\b', upper):
            issues.append("Oracle ROWNUM - use LIMIT or a window function in PostgreSQL")
        if 'LISTAGG' in upper:
            issues.append("Oracle LISTAGG - use STRING_AGG in PostgreSQL")
        for issue in issues:
            self.config_parser.print_log_message('WARNING', f"oracle_connector: convert_view_code: view {view_label} contains {issue}. Manual review of the generated view is recommended.")

    def _preprocess_oracle_outer_joins(self, sql):
        """Turn Oracle (+) outer-join operators into inline comment markers on the '=' so the
        parser attaches them to the EQ node (reuses the Sybase ASE *=/=* marker technique).
        'col = col(+)' -> right side is null-supplying -> LEFT outer.
        'col(+) = col' -> left side is null-supplying  -> RIGHT outer."""
        if not sql or '(+)' not in sql:
            return sql
        sql = re.sub(r'([\w."]+)\s*=\s*([\w."]+)\s*\(\s*\+\s*\)', r'\1 = /* left_outer */ \2', sql)
        sql = re.sub(r'([\w."]+)\s*\(\s*\+\s*\)\s*=\s*([\w."]+)', r'\1 = /* right_outer */ \2', sql)
        return sql

    def _convert_marked_outer_joins(self, expression, converted_joins=None):
        """
        The marked conditions of the WHERE clause as the joins of PostgreSQL. The work is the
        same for every dialect which writes its outer joins that way and stands in
        query_conversion/outer_joins.py; only the marking of '(+)' above is Oracle.
        """
        return outer_joins.convert_marked_outer_joins(expression, converted_joins)

    def _strip_listagg_on_overflow(self, sql):
        """Remove Oracle LISTAGG's ON OVERFLOW clause, which has no PostgreSQL equivalent
        (and which sqlglot cannot even parse). Forms: ON OVERFLOW ERROR |
        ON OVERFLOW TRUNCATE ['indicator'] [WITH COUNT | WITHOUT COUNT]. Stripping it lets
        the aggregate parse/convert to STRING_AGG; the overflow behaviour itself is dropped."""
        if not sql:
            return sql
        return re.sub(
            r"(?i)\s+ON\s+OVERFLOW\s+(?:ERROR|TRUNCATE(?:\s+'[^']*')?(?:\s+WITH(?:OUT)?\s+COUNT)?)",
            '',
            sql,
        )

    def _strip_translate_using(self, sql):
        """Rewrite Oracle's charset-conversion form TRANSLATE(expr USING CHAR_CS|NCHAR_CS)
        to just (expr). This is the two-argument USING variant that converts a value to the
        database / national character set - distinct from the three-argument
        TRANSLATE(str, from, to). PostgreSQL has no equivalent (and sqlglot cannot parse the
        USING form), and with a single database encoding the conversion is a no-op, so the
        inner expression is kept as-is."""
        if not sql:
            return sql
        return re.sub(
            r'(?is)\bTRANSLATE\s*\(\s*(.+?)\s+USING\s+N?CHAR_CS\s*\)',
            r'(\1)',
            sql,
        )

    def _postfix_oracle_to_pg_sql(self, sql):
        """Targeted fixes for Oracle constructs sqlglot leaves as-is or mis-handles."""
        if not sql:
            return sql
        # Defensive: normally stripped before sqlglot, but the raw-fallback path reaches here
        # with TRANSLATE(... USING [N]CHAR_CS) still present.
        sql = self._strip_translate_using(sql)
        # sqlglot renders SYSTIMESTAMP as SYSTIMESTAMP() which does not exist in PostgreSQL
        sql = re.sub(r'(?i)\bSYSTIMESTAMP\s*\(\s*\)', 'CURRENT_TIMESTAMP', sql)
        sql = re.sub(r'(?i)\bSYSTIMESTAMP\b', 'CURRENT_TIMESTAMP', sql)
        # sequence.NEXTVAL / sequence.CURRVAL -> nextval('sequence') / currval('sequence')
        sql = re.sub(r'(?i)\b([A-Za-z_][\w$#]*)\s*\.\s*NEXTVAL\b', r"nextval('\1')", sql)
        sql = re.sub(r'(?i)\b([A-Za-z_][\w$#]*)\s*\.\s*CURRVAL\b', r"currval('\1')", sql)
        # Oracle's dummy DUAL table - PostgreSQL allows SELECT without FROM
        sql = re.sub(r'(?i)\s+FROM\s+dual\b', '', sql)
        # Drop the ON OVERFLOW clause (defensive: normally already stripped before sqlglot,
        # but the raw-fallback path reaches here with it still present).
        sql = self._strip_listagg_on_overflow(sql)
        # LISTAGG(expr, 'delim') WITHIN GROUP (ORDER BY cols) -> STRING_AGG(expr, 'delim' ORDER BY cols).
        # Conservative: only the common form (simple expr/order-by without nested parens); anything
        # more complex is left for the manual review flagged by _warn_unconvertible_oracle_sql.
        sql = re.sub(
            r"(?i)\bLISTAGG\s*\(\s*([^,()]+?)\s*,\s*('[^']*')\s*\)\s*WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+([^()]+?)\s*\)",
            r"STRING_AGG(\1, \2 ORDER BY \3)",
            sql,
        )
        # Tidy the boolean placeholders left by outer-join extraction - the same ones the
        # conversion of the marked joins leaves behind for every dialect which writes them
        sql = outer_joins.tidy_boolean_placeholders(sql)
        return sql

    ## ------------------------------------------------------------------ the rewrites

    def postgres_expression(self, text):
        """A fragment of PostgreSQL SQL as a node of the statement being converted."""
        return sqlglot.parse_one(text, read='postgres')

    def written_as_postgres(self, node):
        """One node of the parsed Oracle statement as the PostgreSQL text it becomes."""
        return node.sql(dialect='postgres')

    def rewrite_oracle_expression(self, node, report):
        """
        One node of the parsed statement, rewritten where the transpiler would write a name
        PostgreSQL does not have. It is applied to every node from the leaves upwards, so a
        call inside the arguments of another one is already rewritten when the outer one is
        looked at.

        What cannot be written is left standing and said in `report`, where
        convert_query_code() finds it - the statement is then reported as not converted
        rather than answered with an expression which means something else.
        """
        if isinstance(node, exp.AddMonths):
            ## ADD_MONTHS is not renamed by the transpiler at all. Adding an interval of
            ## months is the same thing in PostgreSQL, down to the last day of a month:
            ## '31-JAN' plus one month is the 28th or the 29th of February in both.
            return self.postgres_expression(
                f"({self.written_as_postgres(node.this)} + "
                f"({self.written_as_postgres(node.expression)}) * INTERVAL '1 month')")

        if isinstance(node, exp.Select):
            return self.rewrite_rownum_limit(node, report)

        if isinstance(node, exp.DateTrunc):
            return self.rewrite_date_trunc(node, report)

        if isinstance(node, exp.Round):
            decimals = node.args.get('decimals')
            if isinstance(decimals, exp.Literal) and decimals.is_string:
                ## ROUND(date, 'MM') rounds a date to the nearest month - it can move it
                ## forwards, which no date_trunc() does
                report['notes'].append(
                    f"ROUND({self.written_as_postgres(node.this)}, {decimals.sql()}) rounds a "
                    f"date to the nearest {decimals.this} and PostgreSQL has nothing which "
                    f"rounds a date - date_trunc() only ever moves it backwards")
            return node

        return node

    def rewrite_rownum_limit(self, node, report):
        """
        `WHERE ROWNUM <= n` of a query which has no ORDER BY of its own, as the LIMIT n of
        PostgreSQL.

        ROWNUM is given to a row while the rows are being read and LIMIT is applied after
        they have been sorted, so the two are the same thing exactly when the query block
        holding the ROWNUM does not sort. That is the shape of the paging Oracle applications
        are written in - `SELECT * FROM (SELECT ... ORDER BY x) WHERE ROWNUM <= 20` - where
        the ORDER BY stands in the subquery and the ROWNUM in the query around it.

        A ROWNUM in a block which does sort is left as it is, and the conversion of the
        statement stops with it: which of the two the author meant is not written anywhere.
        """
        where = node.args.get('where')
        if where is None or node.args.get('order') or node.args.get('limit'):
            return node

        def conjuncts(condition):
            if isinstance(condition, exp.And):
                return conjuncts(condition.this) + conjuncts(condition.expression)
            return [condition]

        for condition in conjuncts(where.this):
            wanted = self.rownum_bound(condition)
            if wanted is None:
                continue
            ## the same way the outer joins are taken out of the WHERE clause: the condition
            ## becomes TRUE and _postfix_oracle_to_pg_sql() tidies the clause afterwards
            condition.replace(exp.Boolean(this=True))
            node.set('limit', exp.Limit(expression=exp.Literal.number(wanted)))
            return node
        return node

    def rownum_bound(self, condition):
        """
        How many rows a condition on ROWNUM allows, or None when it is not one. Oracle
        counts from 1, so 'ROWNUM < 5' is four rows and 'ROWNUM = 1' is one.
        """
        if not isinstance(condition, (exp.LTE, exp.LT, exp.EQ)):
            return None
        left, right = condition.this, condition.expression
        if not (isinstance(left, exp.Column) and left.name.upper() == 'ROWNUM'
                and not left.table):
            return None
        if not (isinstance(right, exp.Literal) and right.is_int):
            return None
        wanted = int(right.this)
        if isinstance(condition, exp.LT):
            wanted -= 1
        elif isinstance(condition, exp.EQ) and wanted != 1:
            ## 'ROWNUM = 2' answers no row at all - the second row only exists once the first
            ## one was given a number - and that is not a limit
            return None
        return wanted if wanted >= 0 else None

    def rewrite_date_trunc(self, node, report):
        """
        TRUNC(date, 'MM') of Oracle as date_trunc('month', date). The transpiler writes the
        format model of Oracle where PostgreSQL expects the name of a field, and
        `DATE_TRUNC('MM', x)` is refused with "timestamp units MM not recognized".
        """
        unit = node.args.get('unit')
        written = (unit.name if isinstance(unit, exp.Expression) else unit) or ''
        field = ORACLE_DATE_FORMATS.get(str(written).upper())
        if field is None:
            report['notes'].append(
                f"the format model '{written}' of TRUNC() has no field of PostgreSQL which "
                f"truncates the same way - 'DAY' and 'W' begin the week on another day there, "
                f"and a statement which answers another day is not a converted statement")
            return node
        return self.postgres_expression(
            f"DATE_TRUNC('{field}', {self.written_as_postgres(node.this)})")

    ## ------------------------------------------------------------------ the conversion

    def convert_statement_with_report(self, settings):
        """
        The conversion itself: the statement of Oracle as the statement of the target,
        together with what happened to it.

        Answers `(code, report)`, where the report holds `unconverted_joins` - the number of
        `(+)` conditions which could not be made into an ANSI join - and `notes`, the
        constructs the conversion decided it could not write. Raises ValueError when the
        statement could not be parsed at all; the exception carries `partial_code`, which is
        what the conversion of a view falls back to.
        """
        code = settings['view_code'] or ''
        report = {'unconverted_joins': 0, 'unmarked_joins': 0, 'notes': []}

        ## Strip Oracle LISTAGG's ON OVERFLOW clause first - sqlglot cannot parse it, and
        ## leaving it in would force the whole statement onto the raw-Oracle fallback path.
        preprocessed = self._strip_listagg_on_overflow(code)
        ## Rewrite TRANSLATE(expr USING [N]CHAR_CS) - sqlglot cannot parse the USING form.
        preprocessed = self._strip_translate_using(preprocessed)
        marked = self._preprocess_oracle_outer_joins(preprocessed)

        try:
            ast = sqlglot.parse_one(marked, read="oracle")
        except Exception as e:
            error = ValueError(f"the statement could not be parsed as Oracle: {first_line(e)}")
            error.partial_code = self.finish_statement_code(code, settings)
            raise error

        if ast is None:
            error = ValueError('the parser read no statement at all')
            error.partial_code = self.finish_statement_code(code, settings)
            raise error

        converted_joins = set()
        ast, report['unconverted_joins'] = self._convert_marked_outer_joins(ast, converted_joins)
        ## A '(+)' which the textual marking above does not reach - one written on a condition
        ## which is not the join itself ('o.status(+) = ''X''') or one written inside a call
        ## ('UPPER(o.cid(+))'). sqlglot keeps it on the column, so it can be attributed from
        ## the parsed statement; Oracle writes the marker itself, so nothing is inferred here.
        ## Anything still carrying one afterwards is counted: the generator of PostgreSQL drops
        ## it without a word, and the outer join would become an inner one.
        ast, moved, report['unmarked_joins'] = outer_joins.convert_join_marked_predicates(
            ast, converted_joins)
        if moved:
            ## a warning and not a blocker - 'notes' is the list of reasons a statement may
            ## not be offered as converted, and this one was converted
            report['moved_predicates'] = moved
        ast = ast.transform(lambda node: self.rewrite_oracle_expression(node, report))
        converted = ast.sql(dialect="postgres")
        ## Strip any outer-join markers that could not be converted - they are counted in the
        ## report, and the caller decides what that means
        converted = re.sub(r'\s*/\*\s*(?:left|right)_outer\s*\*/\s*', ' ', converted)
        return self.finish_statement_code(converted, settings), report

    def finish_statement_code(self, code, settings):
        """
        What is done to the text whether or not it could be parsed: the constructs the
        transpiler leaves as they are, the function mapping of the connector, and the schema
        of the source.
        """
        source_schema_name = settings.get('source_schema_name', '')
        target_schema_name = settings['target_schema_name']

        code = self._postfix_oracle_to_pg_sql(code)
        ## Translate Oracle SQL functions that sqlglot leaves as-is (e.g. GROUPING_ID ->
        ## GROUPING) to their PostgreSQL equivalents. Same mapping used for functions and
        ## procedures.
        code = self.apply_sql_functions_mapping(code, settings)

        ## Re-point any source-schema-qualified references to the target schema (both the
        ## Oracle canonical quoted-upper form and an unquoted any-case form). Unqualified
        ## references are resolved by the target search_path set by the orchestrator before
        ## view creation.
        if source_schema_name:
            code = code.replace(f'"{source_schema_name.upper()}".', f'"{target_schema_name}".')
            code = re.sub(rf'(?i)\b{re.escape(source_schema_name)}\s*\.',
                          f'"{target_schema_name}".', code)
            code = code.replace('""', '"')
        return code

    def convert_statement_code(self, settings: dict):
        """
        One statement of Oracle, converted for the target - the defining query of a view and
        the statement of an application are given the same conversion. Raises ValueError when
        the statement could not be parsed.
        """
        return self.convert_statement_with_report(settings)[0]

    def convert_view_code(self, settings: dict):
        """
        The defining query of a view, converted for the target and wrapped into the statement
        which creates it.

        A statement which cannot be parsed keeps the text of the source, exactly as before:
        the view is reported as failed by the migration and its source code stays readable in
        the protocol.
        """
        view_type = settings.get('view_type', 'VIEW')
        source_schema_name = settings.get('source_schema_name', '')
        target_schema_name = settings['target_schema_name']
        target_view_name = settings.get('target_view_name', '')
        view_label = (f"{source_schema_name}.{target_view_name}" if source_schema_name
                      else target_view_name)

        ## Surface constructs that cannot be reliably auto-converted before touching the SQL.
        self._warn_unconvertible_oracle_sql(settings['view_code'], view_label)

        try:
            converted, report = self.convert_statement_with_report(settings)
            for note in report['notes']:
                self.config_parser.print_log_message(
                    'WARNING', f"oracle_connector: convert_view_code: view {view_label}: {note}")
            for moved in report.get('moved_predicates') or []:
                self.config_parser.print_log_message(
                    'WARNING', f"oracle_connector: convert_view_code: view {view_label}: {moved} "
                               f"carries the outer join operator '(+)' and was moved into the ON "
                               f"clause of the join - in the WHERE clause it would throw away the "
                               f"rows the outer join added.")
            if report['unconverted_joins'] or report['unmarked_joins']:
                self.config_parser.print_log_message('WARNING', f"oracle_connector: convert_view_code: view {view_label} has {report['unconverted_joins'] + report['unmarked_joins']} Oracle (+) outer-join condition(s) that could not be converted to ANSI joins (they remain as inner-join conditions). Manual review required.")
        except ValueError as e:
            self.config_parser.print_log_message('WARNING', f"oracle_connector: convert_view_code: sqlglot conversion of view {view_label} failed ({e}); using the raw Oracle definition. Manual review required.")
            converted = getattr(e, 'partial_code', settings['view_code'] or '')

        ## ALL_VIEWS.TEXT / ALL_MVIEWS.QUERY store only the defining query, so wrap it into a
        ## full CREATE [MATERIALIZED] VIEW statement (view_type is 'VIEW' or 'MATERIALIZED VIEW').
        ddl = f'CREATE {view_type} "{target_schema_name}"."{target_view_name}" AS {converted.strip()}'
        if not ddl.rstrip().endswith(';'):
            ddl += ';'
        return ddl

    ## ------------------------------------------------------------------ the entry point

    def oracle_conversion_warnings(self, query_code):
        """
        What the reader of the converted statement has to be told: what was converted and
        still means something else on the target.
        """
        warnings = []
        if not query_code:
            return warnings
        statement = self.sql_without_literals_and_comments(query_code)

        if EMPTY_STRING_COMPARISON.search(query_code):
            warnings.append(
                "the empty string is NULL in Oracle and a value of its own in PostgreSQL, so a "
                "comparison against '' is never true in the source and matches the empty "
                "strings in the target - what the column holds after the migration decides "
                "which of the two the statement should ask, and IS NULL is usually it")
        if SYSDATE_ARITHMETIC.search(statement):
            warnings.append(
                "the arithmetic of SYSDATE counts days in Oracle - 'SYSDATE - order_date' is a "
                "number there and an interval in PostgreSQL, and 'order_date + 7' is seven "
                "days in Oracle and a syntax error for a timestamp in PostgreSQL. Write the "
                "interval where the value is compared with a number")
        if UPPER_CASE_IDENTIFIER.search(query_code):
            warnings.append(
                "the statement quotes an identifier in upper case. Oracle stores an unquoted "
                "name in upper case and the quoted spelling finds it; PostgreSQL keeps the "
                "case of a quoted name as well, so it only finds the object if the migration "
                "wrote it in upper case too - see names_case_handling")
        if OPTIMIZER_HINT.search(query_code):
            warnings.append(
                "the optimizer hint of Oracle ('/*+ ... */') is removed - it changes nothing "
                "about the rows the statement gives back, and PostgreSQL has no counterpart "
                "for it")
        if TRUNC_OF_ONE_ARGUMENT.search(statement):
            warnings.append(
                "TRUNC() with one argument truncates a date to the day in Oracle and a number "
                "to a whole number in PostgreSQL. It is left as it is: which of the two the "
                "statement means is decided by the type of the column, which is not written "
                "in the statement - write date_trunc('day', x) where it is a date")
        return warnings

    def oracle_conversion_blockers(self, converted_code, report=None):
        """
        The reasons the converted statement may not be offered as a conversion: a construct of
        Oracle which is still standing in it, and what the conversion itself reported it could
        not write.
        """
        statement = self.sql_without_literals_and_comments(converted_code or '')
        reasons = [reason for pattern, reason in WITHOUT_COUNTERPART if pattern.search(statement)]
        if UNKNOWN_DATE_TRUNC.search(statement):
            reasons.append("a TRUNC() of a date was written with a format model of Oracle "
                           "which PostgreSQL does not know as a field")
        if report:
            reasons.extend(report.get('notes') or [])
            if report.get('unconverted_joins') or report.get('unmarked_joins'):
                reasons.append(
                    f"{report.get('unconverted_joins', 0) + report.get('unmarked_joins', 0)} "
                    f"outer join condition(s) written '(+)' "
                    f"could not be made into a LEFT JOIN. Left in the WHERE clause they are "
                    f"an inner join, which answers fewer rows and looks healthy while doing "
                    f"it - the statement has to be rewritten by hand")
        return reasons

    def convert_query_code(self, settings: dict):
        """
        One statement of an application, converted for PostgreSQL - the same conversion the
        query of a view is given. See the contract in DatabaseConnector.convert_query_code().
        """
        statement_id = settings.get('statement_id', '')
        warnings = self.oracle_conversion_warnings(settings['query_code'])
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

        if not (converted or '').strip():
            return {'code': '', 'converted': False, 'warnings': warnings,
                    'error': 'the conversion produced no statement at all'}

        moved = report.get('moved_predicates') or []
        if moved:
            warnings.append(
                f"{', '.join(moved)} carr{'y' if len(moved) > 1 else 'ies'} the outer join "
                f"operator '(+)', which says the condition belongs to the join and not to the "
                f"rows the join answers. It was moved into the ON clause, which is where "
                f"PostgreSQL asks the same question - left in the WHERE clause it would throw "
                f"away the rows the outer join added.")

        blockers = self.oracle_conversion_blockers(converted, report)
        if blockers:
            return {'code': '', 'converted': False, 'warnings': warnings,
                    'error': '; '.join(blockers)}

        self.config_parser.print_log_message(
            'DEBUG', f"oracle_connector: convert_query_code: {statement_id}: {converted}")
        return {'code': converted, 'converted': True, 'warnings': warnings, 'error': None}
