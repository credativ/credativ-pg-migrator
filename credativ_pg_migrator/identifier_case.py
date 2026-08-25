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
The names inside a converted statement, spelled the way the target has them.

`names_case_handling` decides how the objects of the target are named, and the tables, the
columns and the indexes are created that way. A view is the other half of it: its defining
query names those objects too, and the name in the query has to be the name the object got.

Three of the twelve connectors did this and nine did not, each in its own way:

  * ms_sql and sybase_ase wrote the identifiers of the source **in double quotes** - so a view
    of a migration with `lower` said `FROM "CUSTOMERS"` while the table is `customers`, and
    PostgreSQL answered `relation "CUSTOMERS" does not exist`. A hard failure, and the one
    which is easiest to see.

  * informix, mariadb, mysql, oracle, postgresql, sql_anywhere and sqlite wrote them bare.
    That works with `lower` and only with `lower`, because PostgreSQL folds an undelimited
    name to lower case - the view happened to ask for the right thing. With `upper` the table
    is `"CUSTOMERS"` and the bare `CUSTOMERS` of the view folds to `customers`, which is not
    there; with `keep` and a source which spells its names in mixed case the same applies.

The repair is one transformation for all of them, applied where the converted statement is
stored, so no connector has to remember it and the twelve cannot drift apart again.

What is converted: the tables, the columns, the aliases these name each other by, and the
names a common table expression introduces. What is not: the **schema**, which is used exactly
as the configuration spells it; the functions, whose names belong to PostgreSQL and not to this
migration; the data types; and everything inside a string literal, which is data.
"""

import sqlglot
from sqlglot import exp

## How each source folds an identifier which was written without delimiters. It has to be
## applied before names_case_handling is: `keep` means "the case in which the object really
## exists", and for a source which folds, that is not the case in which the DDL was typed.
UNDELIMITED_FOLDING = {
    'oracle': 'upper',
    'ibm_db2_luw': 'upper',
    'ibm_db2_i': 'upper',
    'ibm_db2_zos': 'upper',
    'informix': 'lower',
    'postgresql': 'lower',
    ## MySQL and MariaDB store a name as it was written - whether they compare it case
    ## sensitively depends on the file system, not on the dialect - and the Transact-SQL
    ## family and SQLite store it as written as well.
    'mysql': 'keep',
    'mariadb': 'keep',
    'mssql': 'keep',
    'sybase_ase': 'keep',
    'sql_anywhere': 'keep',
    'sqlite': 'keep',
}


## The names inside a routine which are not objects of this migration and must never be
## rewritten: the records a trigger is given, and the variables PostgreSQL sets in one. A
## PL/pgSQL record is a variable, so it is folded to lower case whatever names_case_handling
## says - writing "NEW" for it under `upper` would look for a variable which is not there.
PLPGSQL_RESERVED = frozenset(('new', 'old', 'tg_op', 'tg_name', 'tg_when', 'tg_level',
                              'tg_relid', 'tg_relname', 'tg_table_name', 'tg_table_schema',
                              'tg_nargs', 'tg_argv', 'found', 'sqlstate', 'sqlerrm'))


def fold_undelimited(name, quoted, source_db_type):
    """
    The name an object really has in the source.

    A delimited name is what it says. An undelimited one is folded by the source before it is
    stored, so `SELECT * FROM customers` against Db2 reads the table `CUSTOMERS` - and a
    migration with `keep` has to keep *that*, not the spelling someone typed.
    """
    if quoted or not name:
        return name
    folding = UNDELIMITED_FOLDING.get((source_db_type or '').lower(), 'keep')
    if folding == 'upper':
        return name.upper()
    if folding == 'lower':
        return name.lower()
    return name


def rename_identifier(identifier, convert, source_db_type, keep=frozenset()):
    """
    One identifier, spelled as the target has it and delimited so it stays that way.

    A name in `keep` is left exactly as it is: it names something which is not an object of
    this migration - the record of a trigger, a variable of PostgreSQL - and giving it the
    case of a table would only stop it from being found.
    """
    if not isinstance(identifier, exp.Identifier) or not identifier.name:
        return
    if identifier.name.lower() in keep:
        return
    converted = convert(fold_undelimited(identifier.name, bool(identifier.args.get('quoted')),
                                         source_db_type))
    identifier.set('this', converted)
    ## Delimited from here on: an undelimited name is folded to lower case by PostgreSQL, which
    ## is the right answer only for `lower`. With `upper` the table is "CUSTOMERS" and a bare
    ## CUSTOMERS in the view would look for `customers`.
    identifier.set('quoted', True)


def convert_identifiers(sql, convert, source_db_type='', dialect='postgres', keep=frozenset()):
    """
    Every table, column and alias of a statement, spelled the way the target has them.

    `convert` is `config_parser.convert_names_case`. Returns (sql, converted) - `converted` is
    False when the statement could not be read, and then the text is answered exactly as it
    came in: a statement no parser understands is left alone and reported, never guessed at.
    """
    if not sql or not sql.strip():
        return sql, True
    try:
        statements = [statement for statement in sqlglot.parse(sql, read=dialect)
                      if statement is not None]
    except Exception:
        return sql, False
    if not statements:
        return sql, False

    for statement in statements:
        if isinstance(statement, exp.Command):
            ## sqlglot's bucket for what it does not model - it would be generated back as the
            ## text it came from, and nothing in it can be found reliably
            return sql, False
        for node in statement.walk():
            node = node[0] if isinstance(node, tuple) else node
            apply_to_node(node, convert, source_db_type, keep)

    return '\n'.join(statement.sql(dialect=dialect) for statement in statements), True


def apply_to_node(node, convert, source_db_type, keep=frozenset()):
    """The rules, one node at a time. Everything not named here is left as it is."""
    if isinstance(node, exp.Table):
        ## the table itself; 'db' and 'catalog' are the schema and are never converted
        rename_identifier(node.args.get('this'), convert, source_db_type, keep)

    elif isinstance(node, exp.Column):
        ## The field of a record - NEW.total inside a trigger - is the column of the table the
        ## trigger is on, so it follows the case handling like any other column. The record
        ## itself does not: it is a variable of PL/pgSQL and is always folded to lower case,
        ## so writing "NEW" for it would look for a variable which is not there. rename_
        ## identifier() leaves a name of the keep set alone, which is exactly that.
        rename_identifier(node.args.get('this'), convert, source_db_type, keep)
        ## the qualifier of the column is an alias of the statement or the name of a table -
        ## either way it has to be spelled the way the thing it names now is
        rename_identifier(node.args.get('table'), convert, source_db_type, keep)

    elif isinstance(node, exp.Alias):
        rename_identifier(node.args.get('alias'), convert, source_db_type, keep)

    elif isinstance(node, exp.TableAlias):
        rename_identifier(node.args.get('this'), convert, source_db_type, keep)
        for column in node.args.get('columns') or []:
            rename_identifier(column, convert, source_db_type, keep)

    elif isinstance(node, exp.Schema):
        ## the column list of a CTE header or of a CREATE statement
        for expression in node.expressions:
            rename_identifier(expression, convert, source_db_type, keep)
