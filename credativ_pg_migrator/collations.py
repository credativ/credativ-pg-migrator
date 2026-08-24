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
What a collation of another engine means, as far as PostgreSQL can say it.

A collation decides which strings count as equal and in which order they sort, so it decides
**which rows a query finds** and, on a unique index, **which rows may exist at all**. Dropping
one is therefore not a cosmetic simplification, and three places dropped it without a word:

  * `clean_index_expression()` of the MySQL and the MariaDB connectors - `re.sub` of
    `COLLATE <name>`, before the expression of a functional index was even transpiled;
  * `get_create_index_sql()` of the PostgreSQL target, which does the same to the expression
    of a functional index of **every** source which is not PostgreSQL.

`CREATE INDEX ix ON t ((name COLLATE utf8mb4_general_ci))` is a case-insensitive index: with
it, MySQL answers `WHERE name = 'MÜLLER'` with the row which holds `müller`, and a *unique*
one of that shape refuses to hold both. The migrated index compares case sensitively, finds
neither the row nor the duplicate, and nothing in the run said that this had changed. Was
F-15, P1-3 of `development/OPEN_ISSUES.md`.

What is decided here, per collation of the source:

  * **byte order** - `utf8mb4_bin`, `latin1_bin`, `binary`, `Latin1_General_BIN2`. PostgreSQL
    says exactly this with `COLLATE "C"`, which compares the encoded bytes. It is carried
    over, and nothing is reported: nothing changed.
  * **case or accent insensitive** - anything whose name holds `ci` or `ai`
    (`utf8mb4_general_ci`, `utf8mb4_0900_ai_ci`, `SQL_Latin1_General_CP1_CI_AS`). PostgreSQL
    cannot express this with a collation of the kind an index can use out of the box: it needs
    a **non-deterministic ICU collation**, which is an object somebody has to create. The
    clause is dropped and the difference is reported, in full, per index.
  * **case and accent sensitive** - `..._cs`, `..._CS_AS`. The default collation of a
    PostgreSQL database is case and accent sensitive as well, so dropping the clause keeps the
    kind of comparison; only the locale of the source is not carried over. Reported at DEBUG.
  * **anything this module does not know** - dropped and reported, because a collation nobody
    recognised is not a collation nobody needs.

Nothing here invents a collation. Creating a non-deterministic ICU collation so that a
case-insensitive index of MySQL can be recreated as one is a feature and not a repair - it is
an object of the target which the planner would have to create before the indexes, and it is
written down in `development/OPEN_ISSUES.md` rather than half-built here.
"""

import re

## The whole of `COLLATE <name>`, with the name in any of the quotings the four dialects use.
COLLATE_PATTERN = re.compile(r'(?i)\bCOLLATE\s+(`[^`]+`|"[^"]+"|\'[^\']+\'|[A-Za-z0-9_]+)')

## Collations of PostgreSQL itself, which are carried over as they stand. A target may hold
## many more - the caller passes the ones its catalogue really has.
POSTGRESQL_BUILTIN = frozenset(('c', 'posix', 'ucs_basic', 'default'))

## What the parts of a collation name say about how it compares. Read from the name because
## that is all there is: the source hands over a name, not a definition.
BYTE_ORDER_PARTS = frozenset(('bin', 'bin2', 'binary'))
INSENSITIVE_PARTS = frozenset(('ci', 'ai'))
SENSITIVE_PARTS = frozenset(('cs', 'as'))

## The outcomes, in the order of how much they cost the migration.
KEPT = 'kept'                     ## a collation of the target, used as it is
BYTE_ORDER = 'byte order'         ## carried over as COLLATE "C" - the same comparison
SENSITIVE = 'sensitive'           ## dropped; the default of the target compares the same way
INSENSITIVE = 'insensitive'       ## dropped; the target compares differently - reported
UNKNOWN = 'unknown'               ## dropped; nobody here knows what it did - reported


class Decision:
    """What one collation of the source becomes, and what has to be said about it."""

    __slots__ = ('name', 'outcome', 'clause')

    def __init__(self, name, outcome, clause):
        self.name = name
        self.outcome = outcome
        self.clause = clause

    @property
    def is_faithful(self):
        """Whether the target compares the way the source did."""
        return self.outcome in (KEPT, BYTE_ORDER, SENSITIVE)

    @property
    def changes_which_rows_match(self):
        """Whether rows the source called equal are not equal in the target any more."""
        return self.outcome == INSENSITIVE

    def __repr__(self):
        return f'Decision({self.name!r}, {self.outcome!r}, {self.clause!r})'


def bare_name(collation):
    """The name of a collation without the quoting of whichever dialect wrote it."""
    name = (collation or '').strip()
    for quote in ('`', '"', "'"):
        if len(name) > 1 and name.startswith(quote) and name.endswith(quote):
            name = name[1:-1]
            break
    return name.strip()


def name_parts(collation):
    return [part for part in re.split(r'[_\s.]+', bare_name(collation).lower()) if part]


def decide(collation, existing_names=()):
    """
    What `collation` becomes in PostgreSQL.

    `existing_names` are the collations the target database really has - a collation of the
    target, or one the migration created there, is used as it stands and nothing is decided.
    """
    name = bare_name(collation)
    if not name:
        return Decision(name, UNKNOWN, '')

    lowered = name.lower()
    if lowered in POSTGRESQL_BUILTIN or name in (existing_names or ()) or lowered in {
            str(existing).lower() for existing in (existing_names or ())}:
        return Decision(name, KEPT, f' COLLATE "{name}"' if lowered != 'default' else '')

    ## Reading the name is the last resort and only reached for a collation the target does
    ## not have: `fr_CI.utf8` is Cote d'Ivoire and not a case-insensitive collation, and the
    ## catalogue above is what tells the two apart.
    parts = set(name_parts(name))
    if parts & BYTE_ORDER_PARTS:
        ## MySQL compares the encoded bytes of the string for a _bin collation, and so does
        ## the C collation of PostgreSQL. This is the one which carries over exactly.
        return Decision(name, BYTE_ORDER, ' COLLATE "C"')
    if parts & INSENSITIVE_PARTS:
        return Decision(name, INSENSITIVE, '')
    if parts & SENSITIVE_PARTS:
        return Decision(name, SENSITIVE, '')
    return Decision(name, UNKNOWN, '')


def explain(decision, where, index_type=''):
    """
    The line to write about one collation which could not be carried over.

    `where` names the object it stood in; `index_type` is used to say what a unique index of
    the source stops enforcing, which is the half of this which changes what data may exist.
    """
    is_unique = 'UNIQUE' in str(index_type).upper() or 'PRIMARY' in str(index_type).upper()
    if decision.outcome == INSENSITIVE:
        message = (
            f"{where}: the collation {decision.name} of the source compares text WITHOUT "
            f"regard to case or accents, and PostgreSQL cannot say that with a collation an "
            f"index uses out of the box - it needs a non-deterministic ICU collation, which "
            f"is an object somebody has to create. The clause was DROPPED, so the migrated "
            f"index compares case sensitively and answers a query with fewer rows than the "
            f"source did.")
        if is_unique:
            message += (
                " The index is UNIQUE: in the target it no longer refuses two values which "
                "differ only in case or accents, so the target accepts rows the source "
                "refused.")
        message += (
            f" To get it back: CREATE COLLATION ... (provider = icu, deterministic = false, "
            f"locale = 'und-u-ks-level2') in the target and recreate the index with it.")
        return message
    if decision.outcome == UNKNOWN:
        return (
            f"{where}: the collation {decision.name} of the source is not one this migrator "
            f"knows, and it has no counterpart in PostgreSQL. The clause was DROPPED and the "
            f"object uses the default collation of the target database - check what "
            f"{decision.name} compares and whether the difference matters here.")
    if decision.outcome == SENSITIVE:
        return (
            f"{where}: the collation {decision.name} of the source compares with regard to "
            f"case and accents, which the default collation of the target does as well, so "
            f"the clause was dropped. Only the locale of the source is not carried over.")
    if decision.outcome == BYTE_ORDER:
        return (
            f"{where}: the collation {decision.name} of the source compares byte by byte and "
            f"was carried over as COLLATE \"C\", which compares the same way.")
    return f'{where}: the collation {decision.name} is used as the target has it.'


def report_level(decision):
    """How loudly a decision has to be said."""
    if decision.outcome in (INSENSITIVE, UNKNOWN):
        return 'WARNING'
    return 'DEBUG'


## The token a collation is replaced by while an expression goes through the transpiler and
## the generic rewrites of apply_sql_functions_mapping(), both of which delete a COLLATE
## clause. It is put back afterwards - see take_out() and put_back().
PLACEHOLDER = '__pgm_collate_{0}__'
## The whitespace in front of the token belongs to it: a clause which is carried over brings
## its own space, and one which is dropped must not leave `("name" )` behind.
PLACEHOLDER_PATTERN = re.compile(r'\s*__pgm_collate_(\d+)__')


def take_out(expression):
    """
    Replace every `COLLATE <name>` of an expression with a token, keeping its position.

    Returns `(expression, [collation name, ...])`. The token is a bare word which none of the
    rewrites in between recognises, so what comes back is the expression with the collations
    exactly where they stood.
    """
    found = []

    def replace(match):
        found.append(match.group(1))
        return PLACEHOLDER.format(len(found) - 1)

    return COLLATE_PATTERN.sub(replace, expression or ''), found


def put_back(expression, collations, existing_names=(), report=None, where='', index_type='',
             resolve=None):
    """
    Put the collations back, as what PostgreSQL can say - and say what it cannot.

    `report` is called as `report(level, message)` once per collation whose meaning is not
    carried over. `resolve` is for the one caller which has a mapping of its own - a
    PostgreSQL source has its collations migrated with the schema, and they are resolved
    against those rather than read from their names; it is called as `resolve(name)` and
    answers the clause to write. Returns the expression and the decisions, so the caller can
    act on them.
    """
    decisions = []

    def replace(match):
        position = int(match.group(1))
        if position >= len(collations):
            return ''
        if resolve is not None:
            return resolve(collations[position])
        decision = decide(collations[position], existing_names)
        decisions.append(decision)
        if report is not None and (decision.outcome != KEPT or where):
            report(report_level(decision), explain(decision, where, index_type))
        return decision.clause

    return PLACEHOLDER_PATTERN.sub(replace, expression or '').strip(), decisions
