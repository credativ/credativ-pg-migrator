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
The money literals of the Transact-SQL family, as the plain numbers PostgreSQL reads.

Sybase ASE and MS SQL Server write a MONEY value with the currency sign in front of it -
'$0', '$19.99', '$-5'. PostgreSQL has no such literal, and no parser of another dialect
reads one either: sqlglot takes '$0' for an identifier and the conversion then quotes it,
so a view whose text said

    isnull(c.credit_limit, $0)

was created as

    COALESCE("c"."credit_limit", "$0")

and the target answered 'column "$0" does not exist'. Where the number is large enough to
look like a placeholder - '$1000' - PostgreSQL reads it as the positional parameter number
1000 instead, and answers 'there is no parameter $1000'. Both are the same defect: the
currency sign has to go before anything tries to parse the statement.

MONEY is migrated as NUMERIC(19,4), so the number alone is the whole value.

This is the rewrite for a whole STATEMENT - the query of a view, a statement of an
application - and it is deliberately stricter than the one the connectors apply to the text
of a column default: it does not read a thousands separator. '$1,000' inside a statement
cannot be told apart from a money literal followed by the next item of a select list, and
turning 'select $1,000' into 'select 1000' would silently drop an expression. A default
value, which is one expression and never a list, is converted by the connector itself.
"""

import re


## The currency sign directly in front of a number, with an optional sign of its own between
## the two. It is a literal only where nothing that could be part of a name stands in front
## of it: Sybase allows '$' inside an identifier, so 'a$5' is a name and not 'a' followed by
## a money literal. A '$' inside a string literal or a comment is text and is protected by
## the caller's mask, not by this pattern.
MONEY_LITERAL = re.compile(r"""(?<![A-Za-z0-9_$@#'"])\$\s*([-+]?)\s*(\d+(?:\.\d*)?|\.\d+)""")


def _as_number(match):
    """The literal without its currency sign. Only a leading '-' survives."""
    sign = match.group(1) if match.group(1) == '-' else ''
    return f"{sign}{match.group(2)}"


def convert_money_literals(code, mask_literals=None):
    """
    Every money literal of the statement rewritten into a plain number.

    'mask_literals' is a callable which blanks out everything that is not SQL - the string
    literals, the quoted identifiers and the comments. Where it is given, the rewrite is
    applied only where it says the text is SQL, so '$5' in the text of a condition or in the
    comment above a statement stays as it was written. Without it the whole text is rewritten,
    which is what a caller handing in a single expression wants.

    The positions of the mask and of the code are the same - the mask blanks characters out,
    it never changes their number - so a match found in the mask is cut out of the code.
    """
    if not code or '$' not in str(code):
        return code

    code = str(code)
    if mask_literals is None:
        return MONEY_LITERAL.sub(_as_number, code)

    searched = mask_literals(code)
    pieces = []
    position = 0
    for match in MONEY_LITERAL.finditer(searched):
        pieces.append(code[position:match.start()])
        pieces.append(_as_number(match))
        position = match.end()
    pieces.append(code[position:])
    return ''.join(pieces)
