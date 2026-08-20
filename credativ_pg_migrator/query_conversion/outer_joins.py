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
The outer joins which the old dialects write in the WHERE clause, as the joins of PostgreSQL.

Oracle writes them '(+)', Sybase ASE and SQL Anywhere write them '*=' and '=*', and Informix
marks the table in the FROM clause instead. The spellings differ and what has to happen to
them does not: the marked table becomes the right side of a LEFT or RIGHT JOIN and the
conditions which read it move into the ON clause of that join - they have to move, because a
condition on the subordinate table in the WHERE clause of PostgreSQL undoes the outer join and
turns it back into an inner one.

Each connector rewrites its own spelling into a comment marker on the '=' - the technique
Sybase ASE was given first - and hands the parsed statement here. What cannot be attributed is
counted and not converted: a statement which answers fewer rows and looks healthy while doing
it is worse than one which is reported as not converted.
"""

import re

from sqlglot import exp


def convert_marked_outer_joins(expression):
    """Rewrite comment-marked equality predicates in the WHERE clause into ANSI LEFT/RIGHT
    JOINs. In sqlglot's model the extra comma-separated tables are implicit joins on the
    SELECT, so the null-supplying table's implicit join becomes a LEFT JOIN; if that table is
    the FROM anchor, the preserved table's join becomes a RIGHT JOIN instead. Returns
    (expression, unconverted_count)."""
    unconverted = 0
    for select_node in expression.find_all(exp.Select):
        where = select_node.args.get('where')
        joins = select_node.args.get('joins') or []
        if not where or not joins:
            continue
        join_by_alias = {}
        for j in joins:
            t = j.this
            if t is not None and t.alias_or_name:
                join_by_alias[t.alias_or_name] = j
        for eq in list(where.find_all(exp.EQ)):
            if not eq.comments:
                continue
            if stands_under_an_or(eq, where):
                ## A condition under an OR does not belong to the join alone: moving it
                ## into the ON clause makes it an AND of the join and leaves the other
                ## side of the OR behind, which answers other rows. Informix refuses the
                ## same shape for the same reason.
                unconverted += 1
                continue
            if any('left_outer' in c for c in eq.comments):
                null_col, preserved_col = eq.right, eq.left
            elif any('right_outer' in c for c in eq.comments):
                null_col, preserved_col = eq.left, eq.right
            else:
                continue
            if not isinstance(null_col, exp.Column) or not null_col.table:
                unconverted += 1
                continue
            join_kind = 'LEFT'
            target_join = join_by_alias.get(null_col.table)
            if target_join is None and isinstance(preserved_col, exp.Column) and preserved_col.table:
                # null-supplying table is the FROM anchor - RIGHT JOIN the preserved table
                target_join = join_by_alias.get(preserved_col.table)
                join_kind = 'RIGHT'
            if target_join is None:
                unconverted += 1
                continue
            cond = eq.copy()
            cond.comments = None
            existing_on = target_join.args.get('on')
            if existing_on is not None:
                cond = exp.And(this=existing_on, expression=cond)
            target_join.set('kind', target_join.args.get('kind') or join_kind)
            target_join.set('on', cond)
            eq.replace(exp.Boolean(this=True))
    return expression, unconverted

def stands_under_an_or(node, boundary):
    """Whether the node is one side of an OR somewhere below the given clause."""
    parent = node.parent
    while parent is not None and parent is not boundary:
        if isinstance(parent, exp.Or):
            return True
        parent = parent.parent
    return False


def tidy_boolean_placeholders(sql):
    """
    The 'TRUE' left in the WHERE clause where a condition was moved into an ON clause, taken
    out again. All of it is semantics-preserving: "WHERE TRUE AND x" is "WHERE x", "x AND
    TRUE" is "x", and "WHERE TRUE" is no filter at all.
    """
    if not sql:
        return sql
    sql = re.sub(r'(?i)\bWHERE\s+TRUE\s+AND\s+', 'WHERE ', sql)
    sql = re.sub(r'(?i)\s+AND\s+TRUE\b', '', sql)
    sql = re.sub(r'(?i)\s*\bWHERE\s+TRUE\b(?=\s*(?:;|\)|$|GROUP\s+BY|ORDER\s+BY|HAVING'
                 r'|LIMIT|OFFSET|UNION|INTERSECT|EXCEPT))', '', sql)
    return sql
