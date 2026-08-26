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


def resolve_tsql_table_references(expression, source_database, source_schema):
    """
    Drop the qualifier of every table which names the database being migrated, and report the
    ones which name another database or another server.

    'expression' is the parsed statement and is changed in place. A reference to the migrated
    database is rewritten to 'owner.table' - the owner it was written with, or the schema of the
    source where the statement left it out ('ccd..t') - so that the schema mapping of the
    connector, which runs after this, gives it the schema of the target like every other table.

    Returns the list of references which could NOT be resolved, each a dictionary with 'server',
    'database', 'schema', 'table' and 'reference' - the last being the name as the source wrote
    it, for the message the caller writes.
    """
    unresolved = []
    if expression is None:
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
