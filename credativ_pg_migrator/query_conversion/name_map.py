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
The names a statement of the application has to use after the migration.

A statement names the objects of the **source**. The target holds them under a schema of
another name, under the case `names_case_handling` gave them, and - where
`use_aliases_as_target_names` is set - under the alias the mapping chose. The migrator is the
only thing which knows both halves, and until now it did not use them: `--convert-queries`
wrote *"name mapping: off"* into every output file and converted `SELECT * FROM SCOTT.ORDERS`
into a statement which still says `SCOTT.ORDERS`. §7.3 of
`development/archive/APPLICATION_QUERIES_CONVERSION_STRATEGY.md`, P3-1 of
`development/OPEN_ISSUES.md`.

Three things come out of the map, and the second and the third are the reason it is worth more
than a search and replace:

  * **the rewrite** — the schema, the tables and the columns of the statement, spelled the way
    the target has them. Through the parsed statement and never by text, so a column named
    `count` is not rewritten inside a string literal and a table named `order` is not rewritten
    inside `ORDER BY`.
  * **the unresolved-reference report** — every table the statement names which the migration
    does not know. That is the query which reads `AUDIT_LOG` in a run whose `exclude_tables`
    left `AUDIT_LOG` behind, and it is said *before* the target test answers with a bare
    `relation "audit_log" does not exist`: the target cannot know what was left behind on
    purpose, and this is the one place which can.
  * **W1 of the warning catalogue** (§9) — a column whose **type class** changed under the
    migration. `NUMBER(1,0)` becomes `SMALLINT` and not `BOOLEAN` unless the configuration asks
    for it, so `WHERE FLAG = 1` still works; where it *was* asked for, the same comparison
    stops working and no test level anywhere can see it. Only this tool has both halves of that
    decision, which is why §9 singles W1 out.

What the map does **not** do: it does not qualify a name which the statement wrote without a
schema. Which schema such a name resolves to is the `search_path` of the application, and
inventing one here would answer a question the application has not asked - the conversion warns
about it instead, as it already did.
"""

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import traverse_scope


class Column:
    """One column of one table, as the source had it and as the target has it."""

    __slots__ = ('source_name', 'target_name', 'source_type', 'target_type')

    def __init__(self, source_name, target_name, source_type='', target_type=''):
        self.source_name = source_name
        self.target_name = target_name
        self.source_type = source_type or ''
        self.target_type = target_type or ''

    def __repr__(self):
        return f'Column({self.source_name!r} -> {self.target_name!r})'


class Table:
    """One table of the migration, with the columns the protocol recorded for it."""

    __slots__ = ('source_schema', 'source_name', 'target_schema', 'target_name', 'columns', 'kind')

    def __init__(self, source_schema, source_name, target_schema, target_name, columns=None,
                 kind='table'):
        self.source_schema = source_schema or ''
        self.source_name = source_name
        self.target_schema = target_schema or ''
        self.target_name = target_name
        ## keyed by the folded source name - a source which folds its undelimited names writes
        ## them in the catalogue in one case and in the application in another
        self.columns = columns or {}
        self.kind = kind

    def column(self, name):
        return self.columns.get(fold(name))

    def __repr__(self):
        return f'Table({self.source_name!r} -> {self.target_schema}.{self.target_name!r})'


def fold(name):
    """The key a name is looked up by: without quotes and without case."""
    if name is None:
        return ''
    return str(name).strip().strip('"').strip('`').strip('[').strip(']').lower()


class NameMap:
    """
    What the migration called the objects of the source, ready to be asked about a statement.

    `reason` says why the map is empty when it is - the protocol tables of a migration are not
    always there, and a run which converts statements without them has to say that the names
    were not mapped rather than let the reader assume they were.
    """

    def __init__(self, source_schema='', target_schema='', tables=None, reason=''):
        self.source_schema = source_schema or ''
        self.target_schema = target_schema or ''
        self.tables = tables or {}
        self.reason = reason

    @property
    def is_available(self):
        return bool(self.tables)

    def table(self, name):
        return self.tables.get(fold(name))

    def describe(self):
        if not self.is_available:
            return f"name mapping: off - {self.reason}"
        return (f"name mapping: on - {len(self.tables)} object(s) of the migration, "
                f"schema {self.source_schema or '(none)'} -> {self.target_schema}")


def build(migrator_tables, config_parser, report=None):
    """
    The map, read out of the protocol tables of the migration which has already run.

    Answers a map which is not available, with the reason, rather than raising: the conversion
    of the statements is worth running without it and the output says which of the two it was.
    """
    def say(level, message):
        if report:
            report(level, message)

    source_schema = config_parser.get_source_schema()
    target_schema = config_parser.get_target_schema()

    if migrator_tables is None:
        return NameMap(source_schema, target_schema, reason=(
            'the protocol tables of the migration were not reachable, so the names of the '
            'source are used as they are'))

    use_aliases = False
    try:
        use_aliases = bool(config_parser.get_use_aliases_as_target_names())
    except Exception:
        use_aliases = False

    tables = {}
    try:
        rows = migrator_tables.fetch_all_tables(only_unfinished=False) or []
    except Exception as e:
        return NameMap(source_schema, target_schema, reason=(
            f'the tables of the migration could not be read from the protocol ({e}), so the '
            f'names of the source are used as they are'))

    for row in rows:
        try:
            data = migrator_tables.decode_table_row(row)
        except Exception as e:
            say('WARNING', f"query_conversion: name map: a row of the tables protocol could not be read ({e}) - the objects it names are not mapped.")
            continue
        source_name = data.get('source_table_name')
        if not source_name:
            continue
        target_name = data.get('target_table_name') or source_name
        if use_aliases and data.get('target_alias_name'):
            target_name = data['target_alias_name']
        tables[fold(source_name)] = Table(
            data.get('source_schema_name'), source_name,
            data.get('target_schema_name') or target_schema, target_name,
            columns_of(data), 'table')

    ## the views of the migration are objects a statement may read exactly like a table
    try:
        for row in migrator_tables.fetch_all_views() or []:
            data = migrator_tables.decode_view_row(row)
            source_name = data.get('source_view_name')
            if not source_name or fold(source_name) in tables:
                continue
            target_name = data.get('target_view_name') or source_name
            if use_aliases and data.get('target_view_alias'):
                target_name = data['target_view_alias']
            tables[fold(source_name)] = Table(
                data.get('source_schema_name'), source_name,
                data.get('target_schema_name') or target_schema, target_name, {}, 'view')
    except Exception as e:
        say('WARNING', f"query_conversion: name map: the views of the migration could not be read from the protocol ({e}) - a statement which reads one is reported as unresolved.")

    if not tables:
        return NameMap(source_schema, target_schema, reason=(
            'the protocol tables of the migration hold no object - a migration has to have run '
            'before the names of a statement can be mapped'))
    return NameMap(source_schema, target_schema, tables)


def columns_of(table_data):
    """
    The columns of one table, paired source to target.

    The two dictionaries of the protocol row are keyed by the same ordinal, which is what pairs
    a column with itself: the name of a column is not changed by the migration (only its case
    is, and that is applied where the DDL is written), and the **types** are what W1 is about.
    """
    source_columns = table_data.get('source_columns') or {}
    target_columns = table_data.get('target_columns') or {}
    if not isinstance(source_columns, dict):
        return {}
    columns = {}
    for key, source_column in source_columns.items():
        if not isinstance(source_column, dict):
            continue
        name = source_column.get('column_name')
        if not name:
            continue
        target_column = target_columns.get(key) if isinstance(target_columns, dict) else None
        target_column = target_column if isinstance(target_column, dict) else {}
        columns[fold(name)] = Column(
            name,
            target_column.get('column_name') or name,
            source_column.get('data_type') or source_column.get('column_type') or '',
            target_column.get('data_type') or '')
    return columns


## ------------------------------------------------------------------------------------------
## What a type is, coarsely enough to say that it stopped being what it was.
##
## The comparison is by CLASS and not by name: a migration is expected to change VARCHAR2(30)
## into VARCHAR(30) and NUMBER into NUMERIC, and reporting those would bury the one which
## matters. A class which changed is a statement written for the source which may not work on
## the target - and the numeric/boolean pair is the one the migrator decides itself.
TYPE_CLASSES = (
    ('boolean', ('bool',)),
    ('date/time', ('timestamp', 'datetime', 'date', 'time', 'interval', 'year')),
    ('binary', ('bytea', 'blob', 'binary', 'raw', 'image', 'varbinary')),
    ('json/xml', ('json', 'xml')),
    ('uuid', ('uuid', 'uniqueidentifier')),
    ('text', ('char', 'text', 'clob', 'string', 'nvarchar', 'varchar', 'memo', 'lvarchar')),
    ('number', ('int', 'serial', 'number', 'numeric', 'decimal', 'float', 'double', 'real',
                'money', 'bit', 'smallmoney', 'dec')),
)


def type_class(type_name):
    """The class of a type name, or '' when it is not one this knows."""
    lowered = str(type_name or '').strip().lower()
    if not lowered:
        return ''
    for class_name, tokens in TYPE_CLASSES:
        for token in tokens:
            if token in lowered:
                return class_name
    return ''


class Rewrite:
    """What the map made of one statement."""

    def __init__(self, sql, mapped=False):
        self.sql = sql
        self.mapped = mapped
        self.renamed = []
        self.unresolved = []
        self.warnings = []


def apply(sql, name_map, dialect='postgres'):
    """
    The statement with the names of the target, what it names which the migration does not know,
    and the warnings of §9 which come out of the same map.

    A statement which cannot be parsed is answered exactly as it came in, with `mapped` False:
    a name changed by a search and replace inside a text nobody could read is not a conversion.
    """
    rewrite = Rewrite(sql)
    if not sql or not sql.strip() or not name_map or not name_map.is_available:
        return rewrite
    try:
        parsed = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return rewrite
    if parsed is None or isinstance(parsed, exp.Command):
        return rewrite

    try:
        scopes = traverse_scope(parsed)
    except Exception:
        scopes = []

    seen_unresolved = set()
    seen_warnings = set()

    ## The name every table node carried before anything was rewritten. The columns are
    ## resolved against the tables, and by the time they are the tables already carry the name
    ## of the target - so what they were called is written down first, keyed by the node
    ## itself, which is what the scopes hand back.
    source_names = {id(node): node.name for node in parsed.find_all(exp.Table)}

    for table_node in parsed.find_all(exp.Table):
        rewrite_table(table_node, name_map, rewrite, seen_unresolved)

    for scope in scopes:
        for column in scope.columns:
            rewrite_column(column, scope, name_map, rewrite, seen_unresolved, seen_warnings,
                           source_names)

    rewrite.sql = parsed.sql(dialect=dialect)
    rewrite.mapped = True
    ## the same rename is reported once however often the statement writes the name
    seen = set()
    rewrite.renamed = [rename for rename in rewrite.renamed
                       if not (rename in seen or seen.add(rename))]
    return rewrite


def rewrite_table(node, name_map, rewrite, seen_unresolved):
    """One table of the statement, given the name the target has for it."""
    name = node.name
    if not name:
        return
    schema = node.text('db')
    known = name_map.table(name)
    if known is None:
        ## a name the migration does not know: not migrated, excluded, or a table of another
        ## database entirely. It is reported rather than renamed.
        key = fold(name)
        if key not in seen_unresolved:
            seen_unresolved.add(key)
            rewrite.unresolved.append(name)
        return

    ## the schema: rewritten only where the statement really names the schema which was
    ## migrated. Another schema is another database's business and is left alone.
    if schema and fold(schema) in (fold(known.source_schema), fold(name_map.source_schema)):
        node.set('db', exp.to_identifier(known.target_schema, quoted=True))
    elif schema:
        if fold(schema) not in seen_unresolved:
            seen_unresolved.add(fold(schema))
            rewrite.unresolved.append(f"{schema} (schema)")

    if fold(known.target_name) != fold(name) or known.target_name != name:
        rewrite.renamed.append(f"{name} -> {known.target_name}")
    node.set('this', exp.to_identifier(known.target_name, quoted=True))


def table_of_column(column, scope, name_map, source_names):
    """
    Which table of the migration a column belongs to.

    The qualifier is looked up in the scope it stands in and then in the scopes around it - a
    column of an outer query inside an EXISTS is qualified by an alias the inner scope does not
    hold. A column without a qualifier belongs to the only table in scope, and where there is
    more than one it is left alone: guessing which of them it came from is how a rewrite breaks
    a statement which worked.
    """
    qualifier = column.table
    scopes = []
    walk = scope
    while walk is not None and len(scopes) < 16:
        scopes.append(walk)
        walk = getattr(walk, 'parent', None)

    if qualifier:
        for candidate in scopes:
            source = candidate.sources.get(qualifier)
            if source is None:
                continue
            if not isinstance(source, exp.Table):
                ## a subquery or a CTE - its columns are of its own making
                return None, True
            return name_map.table(source_names.get(id(source), source.name)), True
        return None, False

    tables = [source for source in scope.sources.values() if isinstance(source, exp.Table)]
    if len(scope.sources) == 1 and len(tables) == 1:
        return name_map.table(source_names.get(id(tables[0]), tables[0].name)), True
    return None, False


def rewrite_column(column, scope, name_map, rewrite, seen_unresolved, seen_warnings,
                   source_names):
    """One column: the name the target has for it, and W1 where its type class changed."""
    name = column.name
    if not name or name == '*':
        return
    table, resolved = table_of_column(column, scope, name_map, source_names)
    if table is None:
        return
    known = table.column(name)
    if known is None:
        key = f"{fold(table.source_name)}.{fold(name)}"
        if key not in seen_unresolved:
            seen_unresolved.add(key)
            rewrite.unresolved.append(f"{table.source_name}.{name} (column)")
        return

    if known.target_name != name:
        rewrite.renamed.append(f"{table.source_name}.{name} -> {known.target_name}")
        column.set('this', exp.to_identifier(known.target_name, quoted=True))

    warn_about_type(table, known, rewrite, seen_warnings)


def warn_about_type(table, column, rewrite, seen_warnings):
    """
    W1 of §9: the type class of a column changed under the migration.

    The class and not the name, because a migration is expected to write VARCHAR2(30) as
    VARCHAR(30) and NUMBER as NUMERIC - reporting those would bury the one which matters. The
    numeric/boolean pair is called out by name: it is the migrator's own decision
    (`map_numeric_1_to_boolean`, `numeric_1_boolean_columns`), it is invisible to every test
    level, and `WHERE FLAG = 1` against a boolean column is refused by PostgreSQL while
    `WHERE FLAG = 'Y'` against a smallint is refused just as flatly.
    """
    source_class = type_class(column.source_type)
    target_class = type_class(column.target_type)
    if not source_class or not target_class or source_class == target_class:
        return
    key = f"{fold(table.source_name)}.{fold(column.source_name)}"
    if key in seen_warnings:
        return
    seen_warnings.add(key)

    message = (f"W1: column {table.source_name}.{column.source_name} was "
               f"{column.source_type} on the source and is {column.target_type} in the target "
               f"- a {source_class} is not a {target_class}")
    if {source_class, target_class} == {'number', 'boolean'}:
        if target_class == 'boolean':
            message += (". A comparison written for the source - FLAG = 1 - is refused by "
                        "PostgreSQL against a boolean column; write it as FLAG IS TRUE or "
                        "FLAG = true")
        else:
            message += (". A comparison written for the source - FLAG = true - is refused by "
                        "PostgreSQL against a numeric column; write it as FLAG = 1")
    else:
        message += (", so a comparison, a function or an ORDER BY written for the source may "
                    "mean something else here or be refused outright")
    rewrite.warnings.append(message)
