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
The query conversion of MySQL and MariaDB.

They are two connectors and one SQL dialect - MariaDB is a fork of MySQL, and everything an
application writes in the one it writes in the other - so the conversion stands once and is
mixed into both. What MariaDB has of its own and MySQL does not are the sequence objects and
`DELETE ... RETURNING`, and neither of them is a read: they are refused by the gates of the
query conversion, not converted here.

Unlike Db2 and Informix, this dialect *is* modelled by the parser of the migrator: sqlglot
reads MySQL and writes PostgreSQL, so there is nothing to prepare before a statement can be
parsed and no `prepare_query_for_parsing()` in this module. What there is instead is the
line between the three kinds of answer a transpiler can give:

* what it writes correctly - the great majority, and it is left alone;
* what it writes as something PostgreSQL does not have or does not mean. `CONCAT_WS` of
  MySQL skips a NULL argument and the transpiler wraps it in a CASE which answers NULL
  instead; `DATEDIFF` becomes a cast of an interval to a number, which PostgreSQL refuses;
  `DAYOFWEEK` becomes `DAY_OF_WEEK`, which does not exist. Those are rewritten here, in the
  parsed statement, before it is written back as PostgreSQL;
* what has no counterpart at all - `INET_NTOA`, `FIELD`, `LAST_INSERT_ID`, the hashes which
  answer a hex string in MySQL and a bytea in PostgreSQL. A statement holding one of them is
  reported as NOT CONVERTED with the reason. It is never handed back with the call still in
  it as if it had been converted.
"""

import re

import sqlglot
from sqlglot import exp

from credativ_pg_migrator.database_connector import first_line


## The whole seconds of one field of TIMESTAMPDIFF. The fields above the day are counted in
## the calendar and not in seconds - a month is not 30 days - and stand further down.
TIMESTAMP_DIFF_SECONDS = {
    'SECOND': 1, 'MINUTE': 60, 'HOUR': 3600, 'DAY': 86400, 'WEEK': 604800,
}
## The same for the fields which are counted in months, and by how much the month count is
## divided to answer the field.
TIMESTAMP_DIFF_MONTHS = {'MONTH': 1, 'QUARTER': 3, 'YEAR': 12}

## The unsigned integer types of MySQL, as sqlglot names them. PostgreSQL has no unsigned
## integer at all, so a cast to one becomes a cast to the signed type which holds it.
UNSIGNED_TYPES = {
    exp.DataType.Type.UTINYINT: 'SMALLINT',
    exp.DataType.Type.USMALLINT: 'INTEGER',
    exp.DataType.Type.UMEDIUMINT: 'INTEGER',
    exp.DataType.Type.UINT: 'BIGINT',
    exp.DataType.Type.UBIGINT: 'BIGINT',
    exp.DataType.Type.UINT128: 'NUMERIC',
    exp.DataType.Type.UINT256: 'NUMERIC',
    exp.DataType.Type.UDECIMAL: 'NUMERIC',
    exp.DataType.Type.UDOUBLE: 'DOUBLE PRECISION',
}

## What MySQL writes and PostgreSQL has nothing for - and what the transpiler answers with a
## name PostgreSQL does not have, which is the same thing seen from the other end. A
## statement which still holds one of them after the conversion is reported as NOT CONVERTED
## with the reason; the reason names what to write instead wherever there is something.
WITHOUT_COUNTERPART = (
    (r'INET_ATON|INET_NTOA|INET6_ATON|INET6_NTOA',
     "the address functions of MySQL have no counterpart in PostgreSQL - it has the INET and "
     "CIDR types instead, and a column migrated to one of them needs no conversion function "
     "at all"),
    (r'BIN|HEX|UNHEX|CONV|CRC32|OCT',
     "BIN(), HEX(), UNHEX(), CONV() and CRC32() answer another string than anything of "
     "PostgreSQL does - HEX() of MySQL writes '2A' and to_hex() writes '2a', and which of "
     "the two an application compares against cannot be guessed"),
    (r'SHA|SHA1|SHA2|SHA224|SHA256|SHA384|SHA512|AES_ENCRYPT|AES_DECRYPT|PASSWORD',
     "the hash functions of MySQL answer a hex string and the ones of PostgreSQL answer a "
     "bytea - write \"encode(sha256(convert_to(x, 'UTF8')), 'hex')\" where the hex string is "
     "what is compared, and note that the encoding of the source decides what is hashed"),
    (r'VERSION|CONNECTION_ID|LAST_INSERT_ID|FOUND_ROWS|ROW_COUNT|SLEEP|BENCHMARK|GET_LOCK'
     r'|RELEASE_LOCK|IS_FREE_LOCK|UUID_SHORT|CURRENT_ROLE',
     "the session and server functions of MySQL have no counterpart - LAST_INSERT_ID() and "
     "ROW_COUNT() answer what the *previous* statement of the same session did, which a "
     "converted statement cannot carry over"),
    (r'USER|SYSTEM_USER',
     "USER() of MySQL answers 'user@host' and CURRENT_USER of PostgreSQL answers the role "
     "name alone, so the two do not compare equal - write the role name if that is what is "
     "meant"),
    (r'FIELD|ELT|EXPORT_SET|MAKE_SET|FIND_IN_SET_ORDINAL',
     "FIELD(), ELT() and MAKE_SET() address a value by its position in a list of arguments; "
     "PostgreSQL writes that as a CASE or as an array, and which of the two fits depends on "
     "what the list is"),
    (r'TIME_TO_SEC|SEC_TO_TIME|MAKEDATE|MAKETIME|PERIOD_ADD|PERIOD_DIFF|WEEK|YEARWEEK'
     r'|WEEK_OF_YEAR|TO_DAYS|FROM_DAYS|TIME_FORMAT',
     "the date arithmetic of MySQL which counts in its own units - WEEK() alone has eight "
     "modes and the default one is not the ISO week PostgreSQL counts - has no counterpart "
     "which answers the same number for every input"),
    (r'JSON_CONTAINS|JSON_CONTAINS_PATH|JSON_LENGTH|JSON_ARRAYAGG|JSON_OBJECT_AGG'
     r'|JSON_OBJECTAGG|JSON_UNQUOTE|JSON_VALID|JSON_KEYS|JSON_SEARCH|JSON_TYPE|JSON_MERGE'
     r'|JSON_MERGE_PATCH|JSON_MERGE_PRESERVE',
     "PostgreSQL writes the JSON operations with its own operators (`->`, `->>`, `@>`, "
     "`jsonb_array_length`) and against jsonb rather than json, so the call cannot be "
     "renamed - the statement has to be rewritten around the operator"),
    ## the second half: what this conversion tried to write and could not
    (r'SUBSTRING_INDEX|TIMESTAMPDIFF|DAY_OF_WEEK|DAY_OF_YEAR|WEEKDAY|QUARTER|UNIX_TIMESTAMP'
     r'|MID|DAYNAME|NUMBER_TO_STR|BITWISE_COUNT|DATE_FORMAT|STR_TO_DATE|TIMESTAMPADD'
     r'|TIME_TO_STR|STR_TO_TIME',
     "there is no expression in PostgreSQL which this conversion could write for this call - "
     "either it was given arguments no expression covers, or the format it carries holds a "
     "code with no counterpart. It is left in the statement as it was written rather than "
     "being replaced by something which answers other values"),
)

WITHOUT_COUNTERPART = tuple(
    (re.compile(rf'(?i)(?<![\w.])(?:{names})\s*\('), reason)
    for names, reason in WITHOUT_COUNTERPART)

## The unsigned types survive as a name of their own rather than as a call
UNSIGNED_LEFTOVER = re.compile(r'(?i)(?<![\w.])(?:UTINYINT|USMALLINT|UMEDIUMINT|UINT|UBIGINT'
                               r'|UINT128|UINT256|UDECIMAL|UDOUBLE)\b')

## Which node of a parsed statement is rewritten by which method of the conversion. A node
## class an older sqlglot does not know simply is not in the table, and a statement holding
## that construct is then reported as one the conversion could not write - never as one which
## was converted.
NODE_REWRITES = {getattr(exp, node): method for node, method in (
    ('ConcatWs', 'rewrite_concat_ws'),
    ('SubstringIndex', 'rewrite_substring_index'),
    ('DateDiff', 'rewrite_date_diff'),
    ('TimestampDiff', 'rewrite_timestamp_diff'),
    ('DayOfWeek', 'rewrite_day_of_week'),
    ('DayOfYear', 'rewrite_day_of_year'),
    ('Quarter', 'rewrite_quarter'),
    ('Dayname', 'rewrite_dayname'),
    ('WeekOfYear', 'rewrite_week_of_year'),
    ('TimeToStr', 'rewrite_time_format'),
    ('StrToDate', 'rewrite_time_format'),
    ('StrToTime', 'rewrite_time_format'),
    ('UnixToTime', 'rewrite_time_format'),
    ('Cast', 'rewrite_unsigned_cast'),
    ('Anonymous', 'rewrite_anonymous_call'),
) if hasattr(exp, node)}

## What is converted and still means something else afterwards
ZERO_DATE = re.compile(r"'0000-00-00(?: 00:00:00)?'")
REGEXP_MATCH = re.compile(r'(?i)(?<![\w.])(?:REGEXP|RLIKE)(?![\w(])')
JSON_EXTRACT = re.compile(r'(?i)(?<![\w.])JSON_EXTRACT\s*\(')
WEEK_FUNCTION = re.compile(r'(?i)(?<![\w.])WEEK\s*\(')
MONTH_NAME = re.compile(r'(?i)(?<![\w.])MONTHNAME\s*\(')


## The functions of MySQL which are renamed one for one. It stands here once: both
## connectors carried a copy of it, and a second copy of a mapping is a copy which drifts.
MYSQL_FUNCTION_MAPPING = {
    'uuid_to_bin(uuid(), 1)': 'gen_random_uuid()::text',
    'uuid_to_bin(uuid(),1)': 'gen_random_uuid()::text',
    'uuid_to_bin(uuid(), 0)': 'gen_random_uuid()::text',
    'uuid_to_bin(uuid(),0)': 'gen_random_uuid()::text',
    'uuid_to_bin(uuid())': 'gen_random_uuid()::text',
    'uuid()': 'gen_random_uuid()',
    'sysdate()': 'current_timestamp',
    'now()': 'current_timestamp',
    'current_timestamp()': 'current_timestamp',
    'current_date()': 'current_date',
    'current_time()': 'current_time',
    'curdate()': 'current_date',
    'curtime()': 'current_time',
    'utc_timestamp()': "(now() at time zone 'utc')",
    'utc_date()': "(current_date at time zone 'utc')",
    'utc_time()': "(current_time at time zone 'utc')",
    'unix_timestamp()': 'extract(epoch from now())::bigint',
    'rand()': 'random()',
    'ifnull(': 'coalesce(',
    'isnull(': 'coalesce(',
    'char_length(': 'length(',
    'character_length(': 'length(',
    'length(': 'length(',
    'concat(': 'concat(',
    'substring(': 'substring(',
    'substr(': 'substring(',
    'instr(': 'strpos(',
    'replace(': 'replace(',
    'upper(': 'upper(',
    'lower(': 'lower(',
    'ltrim(': 'ltrim(',
    'rtrim(': 'rtrim(',
    'space(': "repeat(' ', ",
}


class MySqlQueryConversion:
    """
    Mixed into the MySQL and the MariaDB connector. It expects the connector to provide
    nothing beyond `config_parser` - the conversion itself stands here, so that the two
    connectors cannot answer the same statement differently.
    """

    def query_conversion_supported(self):
        return True

    def get_sql_functions_mapping(self, settings):
        """
        The functions of MySQL which are renamed one for one, the same for both connectors.
        What needs more than a new name is rewritten in the parsed statement further down.
        """
        target_db_type = settings['target_db_type']
        if target_db_type != 'postgresql':
            self.config_parser.print_log_message(
                'ERROR', f"{type(self).__name__}: get_sql_functions_mapping: "
                         f"Unsupported target database type: {target_db_type}")
            return {}
        return dict(MYSQL_FUNCTION_MAPPING)

    ## ------------------------------------------------------------------ the rewrites

    def postgres_expression(self, text):
        """A fragment of PostgreSQL SQL as a node of the statement being converted."""
        return sqlglot.parse_one(text, read='postgres')

    def written_as_postgres(self, node):
        """One node of the parsed MySQL statement as the PostgreSQL text it becomes."""
        return node.sql(dialect='postgres')

    def rewrite_mysql_expression(self, node):
        """
        One node of the parsed statement, rewritten where the transpiler would write
        something PostgreSQL does not have or does not mean. It is applied to every node of
        the statement from the leaves upwards, so a call inside the arguments of another one
        is already rewritten when the outer one is looked at.

        A node this does not know is answered as it is, and a call it knows but cannot write
        - TIMESTAMPDIFF in a unit PostgreSQL cannot count - is left standing as well, where
        `mysql_conversion_blockers()` finds it and stops the conversion.
        """
        rewrite = NODE_REWRITES.get(type(node))
        if rewrite:
            return getattr(self, rewrite)(node)
        return node

    def rewrite_concat_ws(self, node):
        """
        CONCAT_WS of MySQL skips a NULL argument, exactly as concat_ws() of PostgreSQL does.
        Without this the transpiler wraps the call in a CASE which answers NULL as soon as
        one of the arguments is NULL, which is another result - and the kind of difference
        nobody sees until the rows come back.
        """
        node.set('coalesce', True)
        return node

    def rewrite_date_diff(self, node):
        """
        DATEDIFF(a, b) of MySQL counts the whole days between two dates. The transpiler
        answers it with a cast of an interval to a number, which PostgreSQL refuses outright.
        """
        return self.postgres_expression(
            f"(CAST({self.written_as_postgres(node.this)} AS DATE)"
            f" - CAST({self.written_as_postgres(node.expression)} AS DATE))")

    def rewrite_day_of_week(self, node):
        """MySQL counts the days of the week from Sunday as 1, PostgreSQL from Sunday as 0."""
        return self.postgres_expression(
            f"(EXTRACT(DOW FROM {self.written_as_postgres(node.this)}) + 1)")

    def rewrite_day_of_year(self, node):
        return self.postgres_expression(
            f"EXTRACT(DOY FROM {self.written_as_postgres(node.this)})")

    def rewrite_quarter(self, node):
        return self.postgres_expression(
            f"EXTRACT(QUARTER FROM {self.written_as_postgres(node.this)})")

    def rewrite_week_of_year(self, node):
        """
        WEEKOFYEAR() of MySQL is its WEEK() in mode 3, which is the ISO week - the one
        PostgreSQL counts. The transpiler answers it with WEEK_OF_YEAR(), which PostgreSQL
        does not have.
        """
        return self.postgres_expression(
            f"EXTRACT(WEEK FROM {self.written_as_postgres(node.this)})")

    def rewrite_time_format(self, node):
        """
        DATE_FORMAT() and its siblings become to_char() and to_date(), whose format is
        written another way. A format code the transpiler has no counterpart for is carried
        over unchanged - to_char() then writes it out as text and the statement answers
        another string without anybody noticing - so the call is put back the way MySQL
        writes it, where mysql_conversion_blockers() finds it and stops the conversion.
        """
        if '%' not in self.written_as_postgres(node):
            return node
        arguments = [argument for argument in (node.this, node.args.get('format'))
                     if argument is not None]
        return exp.Anonymous(this='DATE_FORMAT', expressions=arguments)

    def rewrite_dayname(self, node):
        """to_char() pads the name of a day to nine characters and MySQL does not."""
        return self.postgres_expression(
            f"TRIM(TO_CHAR({self.written_as_postgres(node.this)}, 'TMDay'))")

    def rewrite_substring_index(self, node):
        """
        SUBSTRING_INDEX(s, d, n) of MySQL - everything in front of the n-th delimiter, or
        behind it when n is negative - as the fields of the string PostgreSQL splits it into.
        Only a literal count can be written: which end is counted from decides the whole
        expression, and that is not known before the value is.
        """
        count = node.args.get('count')
        sign = 1
        if isinstance(count, exp.Neg):
            ## '-1' is read as the negation of 1 and not as a literal of its own
            count, sign = count.this, -1
        if not isinstance(count, exp.Literal) or not count.is_int:
            return node
        wanted = sign * int(count.this)
        if wanted == 0:
            return self.postgres_expression("''")
        string = self.written_as_postgres(node.this)
        delimiter = self.written_as_postgres(node.args['delimiter'])
        if wanted == 1:
            return self.postgres_expression(f"SPLIT_PART({string}, {delimiter}, 1)")
        fields = f"STRING_TO_ARRAY({string}, {delimiter})"
        if wanted > 0:
            return self.postgres_expression(
                f"ARRAY_TO_STRING(({fields})[1:{wanted}], {delimiter})")
        first = (f"CARDINALITY({fields})" if wanted == -1
                 else f"CARDINALITY({fields}) - {abs(wanted) - 1}")
        return self.postgres_expression(
            f"ARRAY_TO_STRING(({fields})[{first}:], {delimiter})")

    def rewrite_timestamp_diff(self, node):
        """
        TIMESTAMPDIFF(unit, a, b) of MySQL - the whole units from a to b - written the way
        PostgreSQL counts them. The units up to the week are whole seconds and are counted
        as seconds; month, quarter and year are counted in the calendar, which is what age()
        answers. MySQL counts towards zero, hence the truncation and not a rounding.
        """
        unit = (node.args.get('unit').name if node.args.get('unit') else '').upper()
        end = self.written_as_postgres(node.this)
        start = self.written_as_postgres(node.expression)
        if unit in TIMESTAMP_DIFF_SECONDS:
            seconds = TIMESTAMP_DIFF_SECONDS[unit]
            difference = (f"EXTRACT(EPOCH FROM (CAST({end} AS TIMESTAMP) "
                          f"- CAST({start} AS TIMESTAMP)))")
            return self.postgres_expression(
                f"CAST(TRUNC({difference} / {seconds}) AS BIGINT)"
                if seconds != 1 else f"CAST(TRUNC({difference}) AS BIGINT)")
        if unit in TIMESTAMP_DIFF_MONTHS:
            months = (f"(EXTRACT(YEAR FROM AGE(CAST({end} AS TIMESTAMP), "
                      f"CAST({start} AS TIMESTAMP))) * 12 "
                      f"+ EXTRACT(MONTH FROM AGE(CAST({end} AS TIMESTAMP), "
                      f"CAST({start} AS TIMESTAMP))))")
            divisor = TIMESTAMP_DIFF_MONTHS[unit]
            return self.postgres_expression(
                f"CAST({months} AS BIGINT)" if divisor == 1
                else f"CAST(TRUNC({months} / {divisor}) AS BIGINT)")
        return node

    def rewrite_unsigned_cast(self, node):
        """
        A cast to an unsigned integer of MySQL as a cast to the signed type of PostgreSQL
        which holds every value of it. PostgreSQL has no unsigned integer, and the
        transpiler answers with the name of the MySQL type, which it does not have either.
        """
        target = node.to.this if node.to else None
        if target not in UNSIGNED_TYPES:
            return node
        return self.postgres_expression(
            f"CAST({self.written_as_postgres(node.this)} AS {UNSIGNED_TYPES[target]})")

    def rewrite_anonymous_call(self, node):
        """A call the parser does not model, and which PostgreSQL writes another way."""
        name = (node.this or '').upper() if isinstance(node.this, str) else ''
        arguments = node.expressions or []
        written = [self.written_as_postgres(argument) for argument in arguments]

        if name == 'WEEKDAY' and len(written) == 1:
            ## MySQL counts from Monday as 0, the ISO day of the week from Monday as 1
            return self.postgres_expression(f"(EXTRACT(ISODOW FROM {written[0]}) - 1)")
        if name == 'UNIX_TIMESTAMP' and len(written) == 1:
            return self.postgres_expression(f"EXTRACT(EPOCH FROM {written[0]})")
        if name == 'UNIX_TIMESTAMP' and not written:
            return self.postgres_expression("EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)")
        if name == 'MID' and len(written) == 3:
            return self.postgres_expression(
                f"SUBSTRING({written[0]} FROM {written[1]} FOR {written[2]})")
        if name in ('SYSDATE', 'LOCALTIME', 'LOCALTIMESTAMP') and not written:
            return self.postgres_expression('CURRENT_TIMESTAMP')
        return node

    ## ------------------------------------------------------------------ the conversion

    def convert_statement_code(self, settings: dict):
        """
        One statement of MySQL or MariaDB, converted for the target - the query of a view and
        the statement of an application are given the same conversion.

        Raises ValueError when the statement could not be parsed. The exception carries
        `partial_code`: the statement with everything the conversion can do without a parser
        already applied, which is what the migration writes into its protocol for a view it
        could not convert.
        """
        code = settings['view_code']
        ## the caller names the target; the migration always does, and a caller which does
        ## not is answered from the configuration, as this conversion always did
        target_db_type = settings.get('target_db_type') or (
            self.config_parser.get_target_db_type() if getattr(self, 'config_parser', None)
            else 'postgresql')

        if target_db_type != 'postgresql':
            self.config_parser.print_log_message(
                'ERROR', f"{type(self).__name__}: convert_statement_code: "
                         f"Unsupported target database type: {target_db_type}")
            return code

        ## the character set and the collation of a MySQL expression have no counterpart, and
        ## WITH ROLLUP is written in front of the columns in PostgreSQL
        if code:
            code = re.sub(r'(?i)\b(?:CHARACTER\s+SET|CHARSET)\s+[a-zA-Z0-9_]+', '', code)
            code = re.sub(r'(?i)\bCOLLATE\s+[`\'"]?[a-zA-Z0-9_]+[`\'"]?', '', code)
            code = re.sub(r'(?i)\bGROUP\s+BY\s+(.*?)\s+WITH\s+ROLLUP\b',
                          r'GROUP BY ROLLUP (\1)', code, flags=re.DOTALL)

        try:
            parsed = sqlglot.parse_one(code, read='mysql')
        except Exception as e:
            error = ValueError(f"the statement could not be parsed as MySQL: {first_line(e)}")
            error.partial_code = self.finish_statement_code(code, settings)
            raise error

        if parsed is None:
            error = ValueError('the parser read no statement at all')
            error.partial_code = self.finish_statement_code(code, settings)
            raise error

        parsed = parsed.transform(lambda node: self.rewrite_mysql_expression(node))
        return self.finish_statement_code(parsed.sql(dialect='postgres'), settings)

    def finish_statement_code(self, code, settings):
        """
        What is done to the text whether or not it could be parsed: the identifier quote of
        MySQL, the schema of the source and the function mapping of the connector.
        """
        code = code.replace('`', '"')
        code = code.replace(f'''"{settings['source_schema_name']}".''',
                            f'''"{settings['target_schema_name']}".''')
        code = code.replace(f'''{settings['source_schema_name']}.''',
                            f'''"{settings['target_schema_name']}".''')
        code = code.replace('""', '"')
        return self.apply_sql_functions_mapping(code, settings)

    def convert_view_code(self, settings: dict):
        """
        The query of a view, converted for the target.

        A statement which cannot be parsed keeps the text of the source, exactly as before:
        the view is reported as failed by the migration and its source code stays readable in
        the protocol.
        """
        try:
            return self.convert_statement_code(settings)
        except ValueError as e:
            self.config_parser.print_log_message(
                'WARNING', f"{type(self).__name__}: convert_view_code: {e}")
            return getattr(e, 'partial_code', settings['view_code'])

    ## ------------------------------------------------------------------ the entry point

    def mysql_conversion_warnings(self, query_code):
        """
        What the reader of the converted statement has to be told: what was converted and
        still means something else on the target.
        """
        warnings = []
        if not query_code:
            return warnings
        statement = self.sql_without_literals_and_comments(query_code)

        if REGEXP_MATCH.search(statement):
            warnings.append(
                "REGEXP / RLIKE is converted to the '~' of PostgreSQL, which compares case "
                "sensitively. The usual collation of MySQL does not, so the converted "
                "statement answers fewer rows - write '~*' where the comparison has to stay "
                "case insensitive")
        if ZERO_DATE.search(query_code):
            warnings.append(
                "'0000-00-00' is a date in MySQL and is no date anywhere else - PostgreSQL "
                "refuses the literal outright. What the column holds after the migration is "
                "NULL, so the condition is written with IS NULL there")
        if JSON_EXTRACT.search(statement):
            warnings.append(
                "JSON_EXTRACT answers a JSON value in both, but MySQL compares that value "
                "with a string and PostgreSQL does not - write the '->>' operator, which "
                "answers text, wherever the value is compared with a parameter or a literal")
        if WEEK_FUNCTION.search(statement):
            warnings.append(
                "WEEK() counts with mode 0 unless it is given another one: the week begins on "
                "Sunday and the first week of the year is the first one holding a Sunday. "
                "EXTRACT(WEEK ...) of PostgreSQL counts the ISO week, which begins on Monday "
                "and belongs to the year holding its Thursday - the two answer another number "
                "for the days around new year")
        if MONTH_NAME.search(statement):
            warnings.append(
                "MONTHNAME() is converted to to_char(), which pads the name of the month to "
                "nine characters - write it inside TRIM() where the name is compared")
        return warnings

    def mysql_conversion_blockers(self, converted_code):
        """
        The reasons the converted statement may not be offered as a conversion: a construct
        of MySQL which is still standing in it, because PostgreSQL has nothing for it or
        because the rewrite could not be done.
        """
        statement = self.sql_without_literals_and_comments(converted_code or '')
        reasons = [reason for pattern, reason in WITHOUT_COUNTERPART
                   if pattern.search(statement)]
        if UNSIGNED_LEFTOVER.search(statement):
            reasons.append("a cast to an unsigned integer of MySQL could not be written - "
                           "PostgreSQL has no unsigned integer type")
        return reasons

    def convert_query_code(self, settings: dict):
        """
        One statement of an application, converted for PostgreSQL - the same conversion the
        query of a view is given. See the contract in DatabaseConnector.convert_query_code().
        """
        statement_id = settings.get('statement_id', '')
        warnings = self.mysql_conversion_warnings(settings['query_code'])
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

        blockers = self.mysql_conversion_blockers(converted)
        if blockers:
            return {'code': '', 'converted': False, 'warnings': warnings,
                    'error': '; '.join(blockers)}

        self.config_parser.print_log_message(
            'DEBUG', f"{type(self).__name__}: convert_query_code: {statement_id}: {converted}")
        return {'code': converted, 'converted': True, 'warnings': warnings, 'error': None}
