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
The database qualifier in front of a table name, resolved where it can be resolved.

Transact-SQL names a table with up to four parts - `server.database.owner.table` - and lets any
of the middle ones be left out, which is where `database..table` comes from. PostgreSQL has two
parts, `schema.table`, so a qualifier which survives the conversion reaches the target as
something it cannot read: `FROM ccd.."batch_task"` is a syntax error, and `FROM ccd."ccd"."t"`
is the name of the database written in front of the schema which replaced it.

The important distinction is that most of those qualifiers are not remote at all. A statement
written inside the database `ccd` may name its own tables `ccd..t` out of habit, and that is the
SAME table the migration is copying - the migrator knows the name of the database it reads from,
so it can drop the qualifier and put the schema of the target in its place without being told
anything. That is what this module does.

What it cannot do is anything about a reference to ANOTHER database or another server: PostgreSQL
reaches those through a foreign data wrapper, which is a decision about the target, not a
conversion of the source. Such a reference is left exactly as the source wrote it and reported,
so that the object is not created with a name which quietly reads something else, and
`remote_objects_substitution` remains the way to rewrite it.

The whole thing works on the PARSED statement, so a database name inside a string literal, a
comment or another word is not touched - which a search and replace over the text cannot promise.
"""

import re

from sqlglot import exp


## What sqlglot 30 answers for each shape of a Transact-SQL name, which is what the reader
## below is written against:
##
##   t                     catalog None          db None            this Identifier(t)
##   dbo.t                 catalog None          db Identifier(dbo) this Identifier(t)
##   ccd..t                catalog Identifier    db '' (a STRING)   this Identifier(t)
##   ccd.dbo.t             catalog Identifier    db Identifier      this Identifier(t)
##   SRV1.otherdb.dbo.t    catalog Identifier    db Identifier      this Dot(dbo.t)
##
## The empty db of the three part form is a plain '' rather than an identifier, which is why the
## transforms of the connectors - which ask `if schema and ...` - never saw it.


## 'database..object' - the owner left out. sqlglot reads it in a FROM clause, and in an
## expression it does not read it at all ("Required keyword: 'this' missing for Column"), so a
## view with 'ccd..fn_x(a)' in its select list could not be parsed and reached the target with
## every one of its qualifiers untouched. Writing the omitted owner back makes the whole family
## the ordinary three part name, which parses everywhere and which resolve_tsql_table_references()
## then handles in one way.
##
## The first part may be quoted; the second must start a name, which is what tells '..' apart
## from the '...' of an ellipsis in a comment and from a number.
EMPTY_OWNER = re.compile(r'(?<![\w.$#"\]])((?:"[^"]+")|(?:\[[^\]]+\])|(?:[A-Za-z_][\w$#]*))'
                         r'\s*\.\s*\.\s*(?=[A-Za-z_"\[])')


def write_omitted_owner(code, owner, mask_literals=None, only_for_database=None):
    """
    'ccd..t' written as 'ccd.<owner>.t', so that every shape of a qualified name parses.

    'mask_literals' blanks out the string literals and the comments; where it is given the
    rewrite is applied only where the text is SQL, so a '..' inside a literal stays as it is.

    'only_for_database' restricts the rewrite to references to that one database, which is how
    it is used: writing an owner into 'otherdb..archive' would make it 'otherdb.dbo.archive',
    the schema mapping would then turn the owner into the schema of the target, and a reference
    to a database this migration does not read would come out as 'otherdb.target.archive' -
    changed, still unusable, and no longer the text the source wrote. A reference to another
    database is left exactly as it stands; resolve_tsql_table_references() reports it.
    """
    if not code or '..' not in code or not owner:
        return code

    wanted = (only_for_database or '').strip().strip('"[]').lower()
    searched = mask_literals(code) if mask_literals is not None else code
    pieces = []
    position = 0
    for match in EMPTY_OWNER.finditer(searched):
        database = code[match.start(1):match.end(1)]
        if wanted and database.strip().strip('"[]').lower() != wanted:
            continue
        pieces.append(code[position:match.start()])
        pieces.append(f"{database}.{owner}.")
        position = match.end()
    pieces.append(code[position:])
    return ''.join(pieces)


def _identifier_name(part):
    """The name of a name part, for the three shapes sqlglot uses for one."""
    if part is None:
        return ''
    if isinstance(part, str):
        return part
    return part.name or ''


def read_tsql_table_parts(table):
    """
    The four parts of a Transact-SQL table reference, as strings.

    Returns (server, database, schema, table_name). A part the statement did not write is the
    empty string. The four part form keeps its schema inside the name - sqlglot parses
    'SRV1.otherdb.dbo.t' with 'dbo.t' as a Dot expression - and it is split out here.
    """
    catalog = _identifier_name(table.args.get('catalog'))
    db = _identifier_name(table.args.get('db'))
    name_node = table.args.get('this')

    if isinstance(name_node, exp.Dot):
        ## four parts: catalog is the server, db is the database, and the schema and the table
        ## are still together in the name
        schema_and_table = name_node.sql(dialect='tsql').split('.')
        schema = '.'.join(schema_and_table[:-1]).strip('"[]')
        table_name = schema_and_table[-1].strip('"[]')
        return catalog, db, schema, table_name

    table_name = _identifier_name(name_node)
    if catalog:
        ## three parts: the catalog is the database and the db is the owner
        return '', catalog, db, table_name
    ## one or two parts: nothing in front of the schema
    return '', '', db, table_name


def read_qualified_call(dot):
    """
    The qualifier parts of a function call written with a database in front of it, or None.

    'ccd.dbo.fn_calc(a)' and 'ccd..fn_t(x)' are a Dot chain whose last expression is the call:

        Dot(this=Dot(this=Identifier(ccd), expression=Identifier(dbo)), expression=Anonymous)
        Dot(this=Dot(this=Identifier(ccd)),                            expression=Anonymous)

    - the second one has no expression on the inner Dot, which is the owner left out. Returns
    (qualifiers, call) with the qualifiers outermost first, or None when the node is not a call
    qualified by at least two parts.
    """
    if not isinstance(dot, exp.Dot) or not isinstance(dot.args.get('expression'), exp.Func):
        return None

    qualifiers = []
    node = dot.args.get('this')
    while isinstance(node, exp.Dot):
        ## an inner Dot with no expression is the owner the statement left out
        qualifiers.insert(0, _identifier_name(node.args.get('expression')))
        node = node.args.get('this')
    if not isinstance(node, exp.Identifier):
        return None
    qualifiers.insert(0, node.name)

    ## 'dbo.fn(x)' is one qualifier and is a schema, not a database - not this function's business
    if len(qualifiers) < 2:
        return None
    return qualifiers, dot.args['expression']


def resolve_tsql_table_references(expression, source_database, source_schema,
                                  target_schema=None):
    """
    Drop the qualifier of every reference which names the database being migrated, and report
    the ones which name another database or another server.

    'expression' is the parsed statement and is changed in place. Two kinds of reference carry a
    database qualifier and both are handled, because handling only the first of them is what made
    a view come back half converted - the table of the FROM clause resolved and the function in
    the select list still written 'ccd..fn_x(a)':

      TABLES, including the target of an INSERT / UPDATE / DELETE, are rewritten to
      'owner.table' - the owner the statement wrote, or the schema of the source where it left
      the owner out ('ccd..t'). The schema mapping of the connector runs after this and gives
      them the schema of the target like every other table.

      FUNCTION CALLS - 'ccd.dbo.fn_calc(a)', 'ccd..fn_t(x)', and the same inside a CROSS APPLY -
      are rewritten to the schema of the TARGET directly, when it is given. Nothing downstream
      maps the qualifier of a function the way it maps the schema of a table, so leaving the
      owner of the source in front of one would only exchange a name the target cannot read for
      a schema it does not have.

    Returns the list of references which could NOT be resolved, each a dictionary with 'server',
    'database', 'schema', 'table' and 'reference' - the last being the name as the source wrote
    it, for the message the caller writes.
    """
    unresolved = []
    if expression is None:
        return unresolved

    migrated_database = (source_database or '').strip().lower()

    def is_migrated(database, server):
        return bool(migrated_database) and not server and database.lower() == migrated_database

    ## Collected before anything is changed. Rewriting a node while the walk which produced it is
    ## still running is how a pass silently stops after the first hit.
    tables = list(expression.find_all(exp.Table))
    dots = list(expression.find_all(exp.Dot))

    for table in tables:
        server, database, schema, table_name = read_tsql_table_parts(table)

        if not server and not database:
            ## 'table' or 'owner.table' - what every other statement writes, and what the
            ## conversion already handles
            continue

        if not is_migrated(database, server):
            ## Another server, or another database. PostgreSQL reaches neither without a
            ## foreign data wrapper, so the reference is left exactly as the source wrote it -
            ## a name which is refused is better than one which silently reads a different
            ## table - and the caller is told.
            unresolved.append({
                'server': server,
                'database': database,
                'schema': schema,
                'table': table_name,
                'reference': '.'.join(part for part in (server, database, schema, table_name) if part),
            })
            continue

        ## The database of the reference IS the database being migrated: the qualifier says
        ## nothing the migration does not already know, and the table is one of its own.
        table.args.pop('catalog', None)
        table.set('db', exp.Identifier(this=schema or source_schema, quoted=False))

    for dot in dots:
        qualified = read_qualified_call(dot)
        if not qualified:
            continue
        qualifiers, call = qualified

        ## 'server.database.owner.fn()' is four parts, 'database.owner.fn()' three, and
        ## 'database..fn()' three with the owner empty
        server = qualifiers[0] if len(qualifiers) > 2 else ''
        database = qualifiers[1] if len(qualifiers) > 2 else qualifiers[0]
        schema = qualifiers[-1]
        ## the name as the SOURCE wrote it - rendering the call back to SQL upper cases an
        ## unquoted function name, and the report is about what the statement says
        name = call.this if isinstance(call.this, str) else _identifier_name(call.this)

        if not is_migrated(database, server):
            unresolved.append({
                'server': server,
                'database': database,
                'schema': schema,
                'table': name,
                'reference': '.'.join(part for part in (server, database, schema, name) if part),
            })
            continue

        if target_schema:
            dot.replace(exp.Dot(this=exp.Identifier(this=target_schema, quoted=True),
                                expression=call.copy()))
        else:
            ## no target schema to point at - the database is dropped and the owner kept, which
            ## is at least a name of two parts instead of one the target cannot read at all
            dot.replace(exp.Dot(this=exp.Identifier(this=schema or source_schema, quoted=False),
                                expression=call.copy()))

    return unresolved

    migrated_database = (source_database or '').strip().lower()

    for table in expression.find_all(exp.Table):
        server, database, schema, table_name = read_tsql_table_parts(table)

        if not server and not database:
            ## 'table' or 'owner.table' - what every other statement writes, and what the
            ## conversion already handles
            continue

        if server or database.lower() != migrated_database or not migrated_database:
            ## Another server, or another database. PostgreSQL reaches neither without a
            ## foreign data wrapper, so the reference is left exactly as the source wrote it -
            ## a name which is refused is better than one which silently reads a different
            ## table - and the caller is told.
            unresolved.append({
                'server': server,
                'database': database,
                'schema': schema,
                'table': table_name,
                'reference': '.'.join(part for part in (server, database, schema, table_name) if part),
            })
            continue

        ## The database of the reference IS the database being migrated: the qualifier says
        ## nothing the migration does not already know, and the table is one of its own.
        table.args.pop('catalog', None)
        table.set('db', exp.Identifier(this=schema or source_schema, quoted=False))

    return unresolved


def unresolved_reference_message(caller, object_name, unresolved):
    """
    One message naming every reference which was left as the source wrote it.

    It is one line per object rather than one per reference, because a view which reads four
    tables of an archive database is one decision and not four.
    """
    if not unresolved:
        return ''
    references = ', '.join(sorted({item['reference'] for item in unresolved}))
    return (f"{caller}: {object_name} reads {references} - a table of another database, which "
            f"PostgreSQL reaches only through a foreign data wrapper. The reference is left as "
            f"the source wrote it, so the object will be refused by the target rather than "
            f"silently read a different table. Rewrite it with remote_objects_substitution, or "
            f"create the object by hand.")
