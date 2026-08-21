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


## '*=' and '=*' of the Transact-SQL family. A literal holding one of them is text, so the
## caller may hand in a way of blanking the literals out before the rewrite is applied.
TSQL_OUTER_OPERATOR = re.compile(r'\*=|=\*')

## The keywords which say what the text around an operator is. An outer join stands in a
## search condition and is therefore preceded by one of WHERE, ON or HAVING; '*=' preceded by
## SET is the compound assignment MS SQL Server has had since 2008 - 'UPDATE t SET x *= 2'
## multiplies, and the routine conversion sends exactly such statements through here.
CLAUSE_KEYWORD = re.compile(r'(?i)\b(WHERE|ON|HAVING|SET)\b')


def from_anchor(select_node):
    """
    The alias of the table the FROM clause starts with.

    The name of the argument differs between the versions of sqlglot this migrator is used
    with - version 30 renamed 'from' to 'from_' - and asking for the wrong one answers None,
    which reads exactly like a SELECT without a FROM clause. That is what made a RIGHT JOIN
    lose the restrictions on its inner table: the anchor is the table the join leaves NULL.
    """
    for name in ('from_', 'from'):
        clause = select_node.args.get(name)
        if clause is not None and clause.this is not None:
            return clause.this.alias_or_name
    return None


def protect(condition):
    """
    A condition wrapped in parentheses where it needs them.

    An OR moved next to an AND changes what it means without them: the ON clause
    "a = b AND x = 'X' OR x = 'Y'" is read as "(a = b AND x = 'X') OR x = 'Y'", which is not
    the condition that was moved.
    """
    if isinstance(condition, exp.Or):
        return exp.Paren(this=condition)
    return condition


MARKERS = ('/* left_outer */', '/* right_outer */')


def unconverted_marker_message(code):
    """
    The message for a statement which still carries an outer join marker, or None.

    A marker which reaches the end means the rewrite could not attribute the condition to a
    table of the FROM clause - and what stands in the generated statement is then the comma
    join it started from with an ordinary equality in the WHERE clause, which is an INNER
    join. PostgreSQL accepts it, the view is created, the query runs, and it answers fewer
    rows than the source did. That is the one outcome this conversion must never produce, so
    the statement is refused instead: `FAKE_CONVERSIONS_AND_SILENT_SKIPS.md`.
    """
    if not code or not any(marker in code for marker in MARKERS):
        return None
    return ("the outer join written '*=' or '=*' could not be rewritten as a LEFT JOIN / RIGHT "
            "JOIN - its condition could not be attributed to a table of the FROM clause. The "
            "statement is not converted rather than converted into the inner join it would "
            "otherwise become, which would answer fewer rows without looking wrong. It has to "
            "be rewritten by hand.")


def outer_join_warnings(report):
    """
    The report of the conversion as the warnings which go into the block of the statement.

    A predicate moved into an ON clause changes which rows the statement answers, so the
    developer is told which one moved and why - a conversion of an outer join which is silent
    about this is the one a reader would trust and should not.
    """
    warnings = []
    moved = (report or {}).get('moved_predicates') or []
    if moved:
        warnings.append(
            f"{', '.join(moved)} restrict{'' if len(moved) > 1 else 's'} the inner table of an "
            f"outer join. In the source such a condition belongs to the join and the rows of the "
            f"outer table are kept either way; in the WHERE clause of PostgreSQL it would be "
            f"applied to the result of the join and would throw away exactly the rows the outer "
            f"join added. It was moved into the ON clause of the join, which is the same "
            f"question - check that this is what the query means.")
    return warnings


def mark_tsql_outer_joins(code, mask_literals=None):
    """
    The '*=' and '=*' of Sybase ASE and of MS SQL Server as a marker on the '='.

    No parser of any dialect reads them, so such a statement cannot be classified, let alone
    converted, while they stand in the text. Each becomes an ordinary equality carrying an
    inline comment which says which side was outer, and convert_marked_outer_joins() below
    turns the marker into a LEFT or a RIGHT JOIN. The asterisk stands next to the table whose
    rows are kept: 'a.x *= b.y' keeps every row of a, 'a.x =* b.y' keeps every row of b.

    Both connectors of the family use this. It stood in sybase_ase alone before, which is why
    the identical statement converted from Sybase ASE and was reported as unreadable from MS
    SQL Server.

    'mask_literals' is a callable which blanks out the string literals of the statement; where
    it is given, the rewrite is applied only where it says the text is SQL.
    """
    if not code or ('*=' not in code and '=*' not in code):
        return code
    searched = mask_literals(code) if mask_literals is not None else code
    clauses = [(match.start(), match.group(1).upper()) for match in CLAUSE_KEYWORD.finditer(searched)]
    pieces = []
    position = 0
    for match in TSQL_OUTER_OPERATOR.finditer(searched):
        if in_a_set_clause(clauses, match.start()):
            continue
        pieces.append(code[position:match.start()])
        pieces.append('= /* left_outer */' if match.group(0) == '*=' else '= /* right_outer */')
        position = match.end()
    pieces.append(code[position:])
    return ''.join(pieces)


def in_a_set_clause(clauses, position):
    """
    Whether the operator at this position is being assigned to rather than compared.

    'UPDATE t SET x *= 2' multiplies x by 2 - MS SQL Server has read '*=' that way since 2008,
    and the conversion of a routine sends its UPDATE statements through the same converter as
    its SELECT statements. Rewriting it as an outer join marker would turn an assignment into
    a comparison. An outer join always stands in a search condition, so the question is which
    keyword the operator stands behind: SET means it is an assignment, WHERE, ON and HAVING
    mean it is a condition. Nothing in front of it at all is a fragment, and a fragment of a
    condition is what the connectors hand in - so that is read as a condition too.
    """
    nearest = None
    for start, keyword in clauses:
        if start < position:
            nearest = keyword
        else:
            break
    return nearest == 'SET'


MARKER_PATTERN = re.compile(r'=\s*/\*\s*(left|right)_outer\s*\*/')


def unmark_tsql_outer_joins(code):
    """
    The markers turned back into the '*=' and '=*' they were made from.

    Every path which hands a statement back unconverted goes through here. A marker left in
    such a text is the worst of the three possible outcomes: PostgreSQL reads it as a comment,
    so what remains is an ordinary equality in a comma join - an INNER join, created without
    complaint, answering fewer rows. The operator of the source is neither valid PostgreSQL nor
    silent: it fails loudly, and it is what the developer wrote.
    """
    if not code or '_outer */' not in code:
        return code
    return MARKER_PATTERN.sub(lambda match: '*=' if match.group(1) == 'left' else '=*', code)


def null_supplying_aliases(select_node, only_joins=None):
    """
    The tables of one SELECT whose rows an outer join may leave as NULL.

    For 'a LEFT JOIN b' it is b; for 'a RIGHT JOIN b' it is a, the anchor of the FROM clause.
    A FULL JOIN makes both sides null-supplying.

    'only_joins' holds the ids of the joins to look at. move_inner_table_predicates() gives it
    the joins the conversion itself made out of a '*=', because those are the only ones whose
    WHERE conditions mean something else in the source than they do on the target. A join
    written as ANSI in the source means on both sides what it says, and nothing of it may be
    moved.
    """
    aliases = set()
    anchor = from_anchor(select_node)
    for join in select_node.args.get('joins') or []:
        if only_joins is not None and id(join) not in only_joins:
            continue
        ## either slot: a join this module made carries 'side', and a join which was written
        ## as ANSI in the source may carry the word in 'kind' depending on how it was parsed
        side = (join.side or join.kind or '').upper()
        table = join.this
        name = table.alias_or_name if table is not None else None
        if side == 'LEFT' and name:
            aliases.add(name)
        elif side == 'RIGHT' and anchor:
            aliases.add(anchor)
        elif side == 'FULL':
            if name:
                aliases.add(name)
            if anchor:
                aliases.add(anchor)
    return aliases


def conjuncts(condition):
    """The top level AND parts of a condition. A part under an OR is not one of them."""
    if isinstance(condition, exp.And):
        return conjuncts(condition.this) + conjuncts(condition.expression)
    if isinstance(condition, exp.Paren):
        return conjuncts(condition.this)
    return [condition]


def tables_named_by(condition):
    """The table qualifiers the condition reads. A column without one answers the empty name."""
    return {column.table or '' for column in condition.find_all(exp.Column)}


def is_a_null_test(condition):
    """
    Whether the condition asks whether something is NULL.

    'WHERE c.id *= o.cid AND o.cid IS NULL' is how this family writes "the customers which
    have no order", and it is answered after the join. Moving it into the ON clause would make
    it a condition of the join, where it is never true - the statement would answer no rows at
    all. It stays in the WHERE clause, whatever table it reads.
    """
    if isinstance(condition, exp.Not):
        condition = condition.this
    return isinstance(condition, exp.Is)


def move_inner_table_predicates(expression, only_joins=None):
    """
    The WHERE conditions which read only a null-supplying table, moved into the ON clause of
    its join. TRANSACT-SQL ONLY - see the note about Oracle at the end.

    This is the half of the outer join which is not the join itself, and the half a converter
    is most likely to get wrong - because getting it wrong produces a statement which is
    valid, looks healthy and answers fewer rows. In Sybase ASE and in MS SQL Server a
    restriction on the inner table of an old style outer join belongs to the join: it decides
    which rows of the inner table take part, and the rows of the outer table are kept either
    way. In PostgreSQL the same condition standing in the WHERE clause is applied to the
    result of the join, where it throws away exactly the rows the outer join added - and the
    LEFT JOIN is an inner join again.

        WHERE c.id *= o.cid AND o.status = 'X'

    is every customer, with the order of status X where there is one. Left where it stands, it
    becomes only the customers which have such an order.

    Three kinds of condition are left alone on purpose:

      * one which reads more than one table, or a table which is not null-supplying - it is
        not a restriction on the inner table,
      * a test for NULL - see is_a_null_test(),
      * anything under an OR, which conjuncts() never answers.

    Returns (expression, moved) - 'moved' holds the conditions as text, for the warning the
    caller writes into the block of the statement. The move changes which rows the statement
    answers, so it is never silent.

    NOT FOR ORACLE. There the marker is written per condition, so Oracle itself says which of
    the two readings it means and nothing may be moved on its behalf.
    """
    moved = []
    for select_node in expression.find_all(exp.Select):
        where = select_node.args.get('where')
        joins = select_node.args.get('joins') or []
        if where is None or where.this is None or not joins:
            continue
        outer_aliases = null_supplying_aliases(select_node, only_joins)
        if not outer_aliases:
            continue
        anchor = from_anchor(select_node)
        join_by_alias = {}
        for join in joins:
            table = join.this
            if table is not None and table.alias_or_name:
                join_by_alias[table.alias_or_name] = join

        kept = []
        moved_here = False
        ## the join which made the anchor of the FROM clause null-supplying, if one did
        for condition in conjuncts(where.this):
            named = tables_named_by(condition)
            alias = next(iter(named)) if len(named) == 1 else None
            if (alias is None or alias not in outer_aliases
                    or is_a_null_test(condition) or isinstance(condition, exp.Boolean)):
                kept.append(condition)
                continue
            ## the join which may leave this table NULL - its own join, or, when the table is
            ## the anchor of the FROM clause, the RIGHT or FULL join which made it so
            target_join = join_by_alias.get(alias)
            if target_join is not None and only_joins is not None and id(target_join) not in only_joins:
                target_join = None
            if target_join is None and alias == anchor:
                target_join = next(
                    (join for join in joins
                     if (join.side or join.kind or '').upper() in ('RIGHT', 'FULL')
                     and (only_joins is None or id(join) in only_joins)), None)
            if target_join is None:
                kept.append(condition)
                continue
            moved.append(condition.sql())
            moved_here = True
            piece = protect(condition.copy())
            existing_on = target_join.args.get('on')
            target_join.set('on', exp.And(this=existing_on, expression=piece)
                            if existing_on is not None else piece)

        if not moved_here:
            continue
        if kept:
            rebuilt = protect(kept[0].copy())
            for condition in kept[1:]:
                rebuilt = exp.And(this=rebuilt, expression=protect(condition.copy()))
            where.set('this', rebuilt)
        else:
            select_node.set('where', None)
    return expression, moved


def marked_columns(node):
    """The columns of a condition which carry Oracle's '(+)', as sqlglot models it."""
    return [column for column in node.find_all(exp.Column) if column.args.get('join_mark')]


def convert_join_marked_predicates(expression, converted_joins=None):
    """
    Oracle's '(+)' written on a condition which the equality rewrite above does not reach.

    ORACLE ONLY, and the opposite mechanism to move_inner_table_predicates(): there the
    dialect gives no marker and the reading has to be inferred from which table a condition
    restricts, here Oracle writes the marker on the column itself and says which of the two
    readings it means. So nothing is inferred - only what carries a '(+)' is moved, and a
    condition without one stays in the WHERE clause, where Oracle applies it too.

        WHERE c.id = o.cid(+) AND o.status(+) = 'X'   -> the status is part of the join
        WHERE c.id = o.cid(+) AND o.status = 'X'      -> the status is a filter, and it turns
                                                        the outer join into an inner one on
                                                        Oracle exactly as it does on PostgreSQL

    sqlglot parses '(+)' into `join_mark` on the column, which survives even inside a call -
    so 'UPPER(o.cid(+)) = c.id', which the textual marking of the connector cannot reach and
    which was reported as an outer join it could not rewrite, is handled here as well.

    What is refused rather than guessed: a condition whose marks name more than one table, a
    condition whose marked table is in no join of the statement, and a mark under an OR, which
    Oracle itself refuses with ORA-01719.

    Returns (expression, moved, unconverted). A mark left anywhere afterwards is counted in
    'unconverted': the generator of PostgreSQL drops it without a word, which would turn the
    outer join into an inner one, so the caller refuses such a statement.
    """
    moved = []
    unconverted = 0
    for select_node in expression.find_all(exp.Select):
        where = select_node.args.get('where')
        if where is None or where.this is None:
            continue
        joins = select_node.args.get('joins') or []
        anchor = from_anchor(select_node)
        join_by_alias = {}
        for join in joins:
            table = join.this
            if table is not None and table.alias_or_name:
                join_by_alias[table.alias_or_name] = join

        kept = []
        moved_here = False
        for condition in conjuncts(where.this):
            marked = marked_columns(condition)
            if not marked:
                kept.append(condition)
                continue
            if isinstance(condition, exp.Or) or any(
                    isinstance(node, exp.Or) for node in condition.find_all(exp.Or)):
                ## Oracle refuses this itself - ORA-01719 - so a statement which holds it did
                ## not run on the source either. It is not converted rather than guessed at.
                unconverted += 1
                kept.append(condition)
                continue
            aliases = {column.table for column in marked if column.table}
            if len(aliases) != 1:
                unconverted += 1
                kept.append(condition)
                continue
            alias = next(iter(aliases))

            join_kind = 'LEFT'
            target_join = join_by_alias.get(alias)
            if target_join is None and alias == anchor:
                ## the marked table is the anchor of the FROM clause, so the join of the
                ## table it is compared with is the one which has to keep its rows
                other = {column.table for column in condition.find_all(exp.Column)
                         if column.table and column.table != alias}
                target_join = next((join_by_alias[name] for name in other if name in join_by_alias), None)
                join_kind = 'RIGHT'
            if target_join is None:
                unconverted += 1
                kept.append(condition)
                continue

            piece = protect(condition.copy())
            for column in marked_columns(piece):
                column.set('join_mark', False)
            existing_on = target_join.args.get('on')
            target_join.set('side', target_join.args.get('side') or join_kind)
            target_join.set('on', exp.And(this=existing_on, expression=piece)
                            if existing_on is not None else piece)
            if converted_joins is not None:
                converted_joins.add(id(target_join))
            moved.append(condition.sql(dialect='oracle'))
            moved_here = True

        if not moved_here:
            continue
        if kept:
            rebuilt = protect(kept[0].copy())
            for condition in kept[1:]:
                rebuilt = exp.And(this=rebuilt, expression=protect(condition.copy()))
            where.set('this', rebuilt)
        else:
            select_node.set('where', None)

    ## anything still carrying a mark could not be attributed - the generator of PostgreSQL
    ## would drop it and answer an inner join
    unconverted += len(marked_columns(expression))
    return expression, moved, unconverted


def convert_marked_outer_joins(expression, converted_joins=None):
    """Rewrite comment-marked equality predicates in the WHERE clause into ANSI LEFT/RIGHT
    JOINs. In sqlglot's model the extra comma-separated tables are implicit joins on the
    SELECT, so the null-supplying table's implicit join becomes a LEFT JOIN; if that table is
    the FROM anchor, the preserved table's join becomes a RIGHT JOIN instead. Returns
    (expression, unconverted_count).

    'converted_joins' is an optional set which is filled with the id() of every join this made
    outer. move_inner_table_predicates() needs it to tell those joins from the ones which were
    written as ANSI in the source: only the former carry conditions which mean something else
    on the target than they did in the statement."""
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
            ## 'side' is the slot sqlglot models LEFT/RIGHT/FULL in; 'kind' is INNER/OUTER/
            ## CROSS. This set 'kind' before, which generates the same SQL and leaves the
            ## parsed statement saying the join has no side at all - so every later pass which
            ## asks whether a join is outer, move_inner_table_predicates() above among them,
            ## was answered no.
            target_join.set('side', target_join.args.get('side') or join_kind)
            target_join.set('on', cond)
            if converted_joins is not None:
                converted_joins.add(id(target_join))
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
