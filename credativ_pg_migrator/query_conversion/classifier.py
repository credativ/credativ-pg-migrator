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
Deciding whether a statement is a SELECT, and nothing else.

This is a safety property of the query conversion, not a convenience: a statement which is
not a read is never converted and never sent to either database. It is decided more than
once, because every single way of deciding it has a known way of being fooled:

  gate 1 - what the parser of the source dialect says the statement is,
  gate 2 - what the text says, for a statement the parser could not read,
  gate 3 - the constructs which begin with SELECT and still write or lock,
  gate 4 - the same questions asked again of the converted statement, in the dialect of the
           target, because a conversion is a text transformation and its result has to be
           what it claims to be.

Nothing here talks to a database.
"""

import re

import sqlglot
from sqlglot import exp

## sqlglot marks the place of a parse error with terminal escape sequences, which have no
## business in a log file or in the comment block of an output file
ANSI_ESCAPE = re.compile(r'\x1b\[[0-9;]*m')

from credativ_pg_migrator.query_conversion.splitter import safe_regions

## The dialect of sqlglot which reads each source the migrator supports. A source which is
## not named here is parsed with the default dialect of sqlglot.
SQLGLOT_DIALECTS = {
    'postgresql': 'postgres',
    'mssql': 'tsql',
    'sybase_ase': 'tsql',
    'sql_anywhere': 'tsql',
    'oracle': 'oracle',
    'mysql': 'mysql',
    'mariadb': 'mysql',
    'sqlite': 'sqlite',
    'informix': None,
    'ibm_db2_luw': None,
    'ibm_db2_i': None,
    'ibm_db2_zos': None,
}

## Words which begin a statement that is not a read. Checked against the text itself, so
## that a statement the parser could not read is still refused rather than guessed at.
WRITING_KEYWORDS = (
    ## the writes every dialect has
    'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'TRUNCATE', 'REPLACE', 'UPSERT',
    ## the data definition, which stands in the same files as the queries
    'CREATE', 'ALTER', 'DROP', 'RENAME', 'COMMENT', 'GRANT', 'REVOKE',
    ## running something, which can do anything
    'EXEC', 'EXECUTE', 'CALL', 'PREPARE', 'DEALLOCATE',
    ## the session and the transaction, which change what everything behind them does
    'SET', 'USE', 'BEGIN', 'START', 'END', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'LOCK',
    ## moving data in and out, and the maintenance of a database
    'LOAD', 'UNLOAD', 'IMPORT', 'EXPORT', 'BULK', 'COPY', 'BACKUP', 'RESTORE', 'DUMP',
    'FLUSH', 'ANALYZE', 'VACUUM', 'REINDEX', 'CLUSTER', 'REFRESH', 'REORG', 'RUNSTATS',
    'PURGE', 'KILL', 'SHUTDOWN',
    ## the cursor of an embedded program, and the LOB writes of Sybase ASE
    'DECLARE', 'OPEN', 'FETCH', 'CLOSE', 'WRITETEXT', 'UPDATETEXT',
)

## Functions which move a sequence on and therefore write, whatever the statement around
## them looks like.
WRITING_FUNCTIONS = ('NEXTVAL', 'SETVAL', 'NEXTVAL_FOR', 'NEXT_VALUE_FOR')

## The same, as the dialects write it when it is not a function call at all: Oracle and
## Db2 read a sequence as the pseudo column 'sequence.NEXTVAL', MS SQL Server and Db2 write
## 'NEXT VALUE FOR sequence'. Neither is a function, so neither is found in the parsed
## statement - and both of them move the sequence on.
SEQUENCE_ADVANCE = re.compile(r'(?i)\bNEXTVAL\b|\bNEXT\s+VALUE\s+FOR\b|\bPREVVAL\b')

## Functions which cannot write, so that a call of anything else can be named as a call
## whose effects this step does not know. Not a complete list of what PostgreSQL has - it is
## the set which turns up in the statements of an application, and a name outside it is
## reported rather than refused.
KNOWN_READING_FUNCTIONS = frozenset((
    'ABS', 'AVG', 'CAST', 'CEIL', 'CEILING', 'CHAR_LENGTH', 'COALESCE', 'CONCAT', 'CONCAT_WS',
    'COUNT', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_USER', 'DATE',
    'DATE_PART', 'DATE_TRUNC', 'EXTRACT', 'FLOOR', 'GREATEST', 'INITCAP', 'LEAST', 'LEFT',
    'LENGTH', 'LOWER', 'LPAD', 'LTRIM', 'MAX', 'MIN', 'MOD', 'NOW', 'NULLIF', 'POSITION',
    'POWER', 'RANDOM', 'REPLACE', 'RIGHT', 'ROUND', 'RPAD', 'RTRIM', 'SIGN', 'SPLIT_PART',
    'SQRT', 'STRING_AGG', 'SUBSTR', 'SUBSTRING', 'SUM', 'TO_CHAR', 'TO_DATE', 'TO_NUMBER',
    'TO_TIMESTAMP', 'TRIM', 'TRUNC', 'UPPER',
))

LOCKING_HINTS = ('HOLDLOCK', 'UPDLOCK', 'XLOCK', 'TABLOCKX', 'PAGLOCK', 'ROWLOCK')

## the same, as one pattern, so that the search can be given the stretches of the statement
## which are SQL - see outside_literals()
LOCKING_HINT_PATTERN = re.compile(r'(?i)\b(' + '|'.join(LOCKING_HINTS) + r')\b')

## The locking reads, as the dialects write them. The parsed statement says it too where a
## parser models the clause - but 'FOR UPDATE' is not modelled in every dialect, and a
## statement which takes row locks has to be refused whether or not it could be read.
LOCKING_PHRASES = re.compile(r'(?i)\bFOR\s+UPDATE\b|\bFOR\s+SHARE\b'
                             r'|\bFOR\s+NO\s+KEY\s+UPDATE\b'
                             r'|\bLOCK\s+IN\s+SHARE\s+MODE\b|\bWITH\s+LOCK\b'
                             r'|\bKEEP\s+(?:UPDATE|EXCLUSIVE|SHARE)\s+LOCKS\b')

## The data change table reference of Db2: 'SELECT ... FROM FINAL TABLE (INSERT ...)'. The
## statement begins with SELECT, gives back rows, and carries out the write inside it. No
## parser of the migrator models it, so it is decided from the text.
DATA_CHANGE_TABLE = re.compile(r'(?i)\bFROM\s+(FINAL|OLD|NEW)\s+TABLE\s*\(')

## SELECT ... INTO, in the spellings which a parser cannot always read. 'INTO TEMP' and
## 'INTO SCRATCH' of Informix create and fill a table; 'INTO :name' and 'INTO @name' read
## into the host variable of an embedded program, which is not a statement of its own.
SELECT_INTO_TARGET = re.compile(r'(?i)\bINTO\s+(TEMP|SCRATCH)\b|\bINTO\s+[:@][A-Za-z_]')

NOLOCK_HINT = re.compile(r'(?i)\bWITH\s*\(\s*NOLOCK\s*\)|\bNOLOCK\b')

## the host variable an embedded SQL program reads into, as it stands in the text of the
## application - by the time the statement is parsed the marker has been replaced by a bind
## parameter, so this is read from the text and not from the parsed statement
HOST_VARIABLE_TARGET = re.compile(r'(?i)\bINTO\s+([:@]\S+)')


class Classification:
    """What the gates decided about one statement."""

    def __init__(self, verdict, gate=None, reason='', warnings=None, parsed=None, kind=''):
        ## 'select'   - a read, and it may be converted
        ## 'refused'  - a gate refused it; it is never sent anywhere
        ## 'unparsed' - the parser of the dialect could not read it, and nothing in the text
        ##              says it writes. It is not converted either, but for another reason:
        ##              the migrator does not understand it, rather than knowing it writes.
        self.verdict = verdict
        self.gate = gate
        self.reason = reason
        self.warnings = warnings or []
        self.parsed = parsed
        ## what the statement is, for the protocol table of §11 - 'SELECT', 'UNION', 'VALUES'
        ## for a read, and the kind the parser saw for the statement a gate refused
        self.kind = kind

    @property
    def is_select(self):
        return self.verdict == 'select'

    def __repr__(self):
        return f"Classification({self.verdict}, gate={self.gate}, reason={self.reason!r})"


def readable_parse_error(error):
    """The message of a parse error on one line, without the terminal escapes sqlglot adds."""
    text = ANSI_ESCAPE.sub('', str(error))
    return re.sub(r'\s+', ' ', text).strip()


def dialect_for(source_db_type):
    """The sqlglot dialect which reads the given source, or None for the default one."""
    return SQLGLOT_DIALECTS.get((source_db_type or '').lower())


def leading_keyword(text):
    """The first word of the statement, with the comments and the opening brackets left out."""
    index = 0
    length = len(text)
    while index < length:
        if text[index].isspace() or text[index] == '(':
            index += 1
            continue
        if text.startswith('--', index):
            end_of_line = text.find('\n', index)
            index = length if end_of_line == -1 else end_of_line
            continue
        if text.startswith('/*', index):
            end_of_comment = text.find('*/', index + 2)
            index = length if end_of_comment == -1 else end_of_comment + 2
            continue
        break
    match = re.compile(r'[A-Za-z_][A-Za-z_0-9]*').match(text, index)
    return match.group(0).upper() if match else ''


def statement_starts(text):
    """
    The text of the statement itself and of everything behind a top level semicolon in it.
    A file which was split wrongly, or one written to smuggle a second statement in, is
    caught here: every one of these has to begin with a read.
    """
    parts = [text]
    for start, end in safe_regions(text):
        for match in re.finditer(r';', text[start:end]):
            position = start + match.end()
            if text[position:].strip():
                parts.append(text[position:])
    return parts


def outside_literals(pattern, text):
    """
    The first match of the pattern in the stretches of the statement which are SQL.

    Every textual gate goes through here. A gate which read the whole text refused a
    statement for a word standing inside a string literal or inside a comment: "SELECT id
    FROM customer -- the report for update of the pricing sheet" was answered with "the
    statement takes row locks", and "SELECT 'next value for the counter'" with "the statement
    reads a sequence". Neither is true. The content of a literal is data which never runs and
    a comment runs even less, so only what stands outside of them can refuse a statement.

    The search is given the boundaries of the region rather than a slice of the text, so that
    a word boundary is decided against the characters which really stand next to it.
    """
    for start, end in safe_regions(text):
        match = pattern.search(text, start, end)
        if match:
            return match
    return None


def check_written_words(text):
    """Gate 2 - the deny list, applied to the text as it stands."""
    for part in statement_starts(text):
        keyword = leading_keyword(part)
        if keyword in WRITING_KEYWORDS:
            return f"gate 2: the statement is a {keyword}, not a read"
    return None


def check_sequence_advance(text):
    """
    Gate 2 as well - a statement which reads a sequence, decided from the text.

    Reading a sequence moves it on, so such a statement writes although it is a SELECT with
    nothing else in it. Oracle and Db2 write it as a pseudo column ('seq.NEXTVAL'), which is
    not a function call and cannot be found in the parsed statement at all.
    """
    match = outside_literals(SEQUENCE_ADVANCE, text)
    if match:
        return (f"gate 2: the statement reads a sequence ({match.group(0).strip()}), "
                f"which moves it on - that is a write")
    return None


def check_select_into(text):
    """
    Gate 2 as well - the SELECT ... INTO spellings, decided from the text.

    The parsed statement says it too, for the dialects a parser models. These two do not
    reach the parse at all: 'INTO TEMP' of Informix is not modelled anywhere, and a host
    variable is not SQL. Both of them have to be refused whether or not the statement parses -
    the first creates and fills a table, the second is a fragment of an embedded program.
    """
    match = outside_literals(SELECT_INTO_TARGET, text)
    if match:
        target = re.sub(r'\s+', ' ', match.group(0)).strip()
        if match.group(1):
            return (f"gate 2: {target} creates and fills a table - it writes, although the "
                    f"statement begins with SELECT")
        return (f"gate 2: {target} reads into a host variable of an embedded SQL program - "
                f"it is not a statement which can be run on its own")
    return None


def check_data_change_table(text):
    """
    Gate 2 as well - the data change table reference of Db2, decided from the text.

    'SELECT order_id FROM FINAL TABLE (INSERT INTO orders ...)' begins with SELECT, gives
    back rows and carries out the INSERT. It is the construct which looks the most like a
    read of everything in this file.
    """
    match = outside_literals(DATA_CHANGE_TABLE, text)
    if match:
        return (f"gate 2: the statement reads from a {match.group(1).upper()} TABLE, so the "
                f"INSERT, UPDATE or DELETE inside it is carried out - it writes, although it "
                f"begins with SELECT")
    return None


def check_locking_hints(text):
    """
    Gate 2 as well - a statement which takes locks, decided from the text.

    This one cannot wait for the parse: 'FROM orders o holdlock' is a hint written where a
    parser expects a second alias, so the statement does not parse at all - and a statement
    which locks has to be refused whether or not anything could read it. Refusing is the
    safe direction for a word which really stands in the SQL: the worst a wrong hit costs is
    a statement reported as needing a look. It is not the safe direction for a word inside a
    literal or a comment, which is why the search goes through outside_literals().
    """
    hint = outside_literals(LOCKING_HINT_PATTERN, text)
    if hint:
        return f"gate 2: the statement takes locks ({hint.group(0).upper()})"
    match = outside_literals(LOCKING_PHRASES, text)
    if match:
        return f"gate 2: the statement takes row locks ({re.sub(r'\s+', ' ', match.group(0)).upper()})"
    return None


def cte_is_select(cte):
    inner = cte.this
    return isinstance(inner, (exp.Select, exp.Union, exp.Except, exp.Intersect, exp.Subquery))


def parsed_is_select(expression):
    """Gate 1 - what the parser says the statement is."""
    if isinstance(expression, exp.Subquery):
        return parsed_is_select(expression.this)
    if isinstance(expression, (exp.Union, exp.Except, exp.Intersect)):
        return True
    if isinstance(expression, exp.Select):
        return True
    ## a bare VALUES is a statement of its own in Db2 and in PostgreSQL. It gives back the
    ## rows written into it and touches nothing - the VALUES of an INSERT is part of that
    ## INSERT and is refused with it, one node higher up.
    if isinstance(expression, exp.Values):
        return True
    return False


def describe_parsed(expression):
    """The kind of statement the parser saw, for the message which refuses it."""
    if isinstance(expression, exp.Command):
        return f"a statement the parser of the dialect does not model ({expression.name or 'unknown'})"
    return type(expression).__name__.upper()


def argument(expression, *names):
    """
    One argument of a parsed expression, whichever spelling the installed sqlglot uses for
    its name - version 30 renamed 'with' to 'with_' and 'from' to 'from_'.
    """
    args = getattr(expression, 'args', None) or {}
    for name in names:
        if args.get(name) is not None:
            return args[name]
    return None


def function_name(function):
    """The name a parsed function call carries, whether sqlglot models it or not."""
    if isinstance(function, exp.Anonymous):
        return (function.name or '').upper()
    name = function.sql_name() if hasattr(function, 'sql_name') else ''
    return (name or function.name or '').upper()


def check_traps(expression, text, source_db_type=''):
    """
    Gate 3 - the constructs which begin with SELECT and still write, lock or cannot be run
    on their own. Returns (reason, warnings): a reason refuses the statement, the warnings
    belong to a statement which is converted.
    """
    warnings = []

    ## SELECT ... INTO. Two different constructs share the syntax and both are refused:
    ## Sybase ASE and MS SQL Server create and fill a table with it, and the embedded SQL of
    ## Informix and Db2 reads into a host variable, which is not a statement of its own.
    into = argument(expression, 'into')
    if into is not None:
        target = into.this.sql() if getattr(into, 'this', None) is not None else into.sql()
        target = target.strip()
        ## the host variable is read in the text of the application and not in the parsed
        ## statement: a ':name' of an embedded program looks like a bind parameter and has
        ## been replaced by one before the parser saw it
        host_variable = outside_literals(HOST_VARIABLE_TARGET, text)
        if host_variable:
            target = host_variable.group(1)
        if ':' in target or '@' in target:
            return (f"gate 3: SELECT ... INTO {target} reads into a host variable of an "
                    f"embedded SQL program - it is not a statement which can be run on its own"), warnings
        return (f"gate 3: SELECT ... INTO {target} creates and fills a table - "
                f"it writes, although it begins with SELECT"), warnings

    ## a data modifying CTE - WITH x AS (DELETE ... RETURNING *) SELECT * FROM x
    with_clause = argument(expression, 'with_', 'with')
    if with_clause is not None:
        for cte in with_clause.expressions:
            if not cte_is_select(cte):
                return (f"gate 3: the common table expression {cte.alias or ''} is a "
                        f"{type(cte.this).__name__.upper()}, so the statement writes"), warnings

    ## row locks - meaningless in a test of the statement and a write of the lock table
    if argument(expression, 'locks'):
        return "gate 3: the statement takes row locks (FOR UPDATE / FOR SHARE)", warnings

    ## a function which moves a sequence on writes, wherever it stands
    for function in expression.find_all(exp.Func):
        name = function_name(function)
        if name in WRITING_FUNCTIONS:
            return f"gate 3: {name}() moves a sequence on, which is a write", warnings

    ## A call of a function the migrator does not know cannot be told apart from a call of one
    ## which writes: the trap table of §5 says such a statement is warned about, tested with
    ## EXPLAIN and never executed. The first of the three was the one which was missing.
    unknown = []
    for function in expression.find_all(exp.Anonymous):
        name = function_name(function)
        if name and name not in KNOWN_READING_FUNCTIONS and name not in unknown:
            unknown.append(name)
    if unknown:
        warnings.append(
            f"the statement calls {', '.join(f'{name}()' for name in unknown[:4])}"
            f"{', ...' if len(unknown) > 4 else ''} - the migrator does not know what "
            f"{'these functions do' if len(unknown) > 1 else 'this function does'}, so it cannot "
            f"say whether the call has effects of its own. It is tested with PREPARE or EXPLAIN "
            f"and is never executed by this step.")

    ## not a write, but it must not reach the target as it stands
    if outside_literals(NOLOCK_HINT, text):
        warnings.append("the table hint NOLOCK has no counterpart in PostgreSQL and is removed "
                        "from the converted statement - the query reads committed rows there")

    return None, warnings


def classify(text, source_db_type='', dialect=None, parse_text=None):
    """
    The gates 1 to 3, applied to a statement of the source in the dialect of the source.

    'text' is the statement as it stands in the file of the application and is what the
    textual gates read. 'parse_text' is what the parser is given, and is the same statement
    with its bind parameters replaced by a plain identifier: '%s' and '%(name)s' are not SQL
    in any dialect, so a statement holding them cannot be parsed at all and would be reported
    as unreadable although there is nothing wrong with it.

    A statement which the parser cannot read is not refused for that alone - a SELECT of a
    dialect sqlglot does not model completely is still a SELECT - it is answered with the
    verdict 'unparsed', and the caller reports it as not converted rather than as skipped.
    Nothing is ever passed through as if it had been understood.
    """
    if not text or not text.strip():
        return Classification('refused', 2, 'gate 2: the statement is empty')

    ## gate 2 first: it is the only one which works without a parse
    written = check_written_words(text)
    if written:
        return Classification('refused', 2, written)

    locking = check_locking_hints(text)
    if locking:
        return Classification('refused', 2, locking)

    sequence = check_sequence_advance(text)
    if sequence:
        return Classification('refused', 2, sequence)

    into = check_select_into(text)
    if into:
        return Classification('refused', 2, into)

    data_change = check_data_change_table(text)
    if data_change:
        return Classification('refused', 2, data_change)

    read_dialect = dialect if dialect is not None else dialect_for(source_db_type)
    parsed_text = parse_text if parse_text is not None else text
    try:
        expressions = [expression for expression in sqlglot.parse(parsed_text, read=read_dialect) if expression is not None]
    except Exception as e:
        return Classification('unparsed', None, f"the SQL parser could not read the statement: {readable_parse_error(e)}")

    if not expressions:
        return Classification('unparsed', None, 'the parser of the source dialect read no statement at all')

    if len(expressions) > 1:
        return Classification('refused', 3,
                              f"gate 3: the entry holds {len(expressions)} statements, not one - "
                              f"the file was split wrongly, or the entry is not a single query")

    expression = expressions[0]
    if isinstance(expression, exp.Command) or not parsed_is_select(expression):
        if isinstance(expression, exp.Command):
            return Classification('unparsed', None,
                                  f"the SQL parser does not model this statement of the source dialect "
                                  f"({(expression.name or expression.sql())[:60].strip()})")
        return Classification('refused', 1, f"gate 1: the statement is a {describe_parsed(expression)}, not a read",
                              kind=type(expression).__name__.upper())

    kind = type(expression).__name__.upper()
    reason, warnings = check_traps(expression, text, source_db_type)
    if reason:
        return Classification('refused', 3, reason, warnings, kind=kind)

    return Classification('select', None, '', warnings, expression, kind=kind)


def unqualified_tables(expression):
    """
    The tables the converted statement names without a schema in front of them.

    The target test runs with `SET LOCAL search_path TO "<target schema>"`, so such a name
    resolves there whatever the application's own session would do with it - the block says
    `target test: OK` for a statement which needs a search_path the application may not have.
    Naming them is what can be done before the name map of §7.3 exists to qualify them.

    A name defined by the statement itself - a common table expression - is not one of these:
    it never went through a search_path in the first place.
    """
    if expression is None:
        return []
    defined = set()
    for cte in expression.find_all(exp.CTE):
        if cte.alias:
            defined.add(cte.alias.lower())
    names = []
    seen = set()
    for table in expression.find_all(exp.Table):
        if argument(table, 'db', 'catalog'):
            continue
        name = table.name
        if not name or name.lower() in defined or name.lower() in seen:
            continue
        seen.add(name.lower())
        names.append(name)
    return names


def classify_converted(text):
    """
    Gate 4 - the converted statement, classified again in the dialect of the target.

    The conversion is a transformation of text and the thing which is about to be sent to
    PostgreSQL has to be what it claims to be. This closes the hole of a transformation
    which produced something other than a read.
    """
    classification = classify(text, source_db_type='postgresql', dialect='postgres')
    if classification.is_select:
        return classification
    reason = classification.reason.replace('gate 1', 'gate 4').replace('gate 2', 'gate 4').replace('gate 3', 'gate 4')
    if classification.verdict == 'unparsed':
        return Classification('refused', 4,
                              f"gate 4: the converted statement cannot be read as PostgreSQL - {reason}",
                              classification.warnings)
    return Classification('refused', 4, reason, classification.warnings)
