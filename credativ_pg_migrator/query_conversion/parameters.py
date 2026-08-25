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
The bind parameters an application leaves in its SQL.

'... WHERE cust_id = ?' is not SQL. Neither the parser nor the target database accepts the
marker, so it is taken out before either sees the statement, replaced by the $1..$n
PostgreSQL offers for exactly this, and put back afterwards - the file the developer
receives has to be a drop-in replacement for what their code holds today.

The order of the parameters is the reason this is a module of its own: a rewrite can move a
marker to another place in the statement - TOP n becomes LIMIT n at the other end of it -
and a silently reordered parameter list is the worst defect this feature could ship. The
order is therefore compared before and after, and a change is reported as a blocking
warning rather than being repaired quietly.
"""

import re

from credativ_pg_migrator.query_conversion.splitter import safe_regions

STYLES = ('auto', 'qmark', 'named', 'at', 'pyformat', 'numeric', 'none')

## Each style, as it is written in the statement. 'named' excludes '::' so that a cast of
## PostgreSQL is not read as a parameter, 'at' excludes '@@' so that a global variable of
## Sybase ASE and MS SQL Server is not one either - and it excludes an '@' written directly
## behind a name, which is how Oracle addresses a table over a database link:
## 'FROM orders@remote_erp' held a parameter by that reading, and the table was renamed to
## 'orderscpgm_bind_param_1'. A parameter of a driver never stands directly behind a name.
PATTERNS = {
    'qmark': re.compile(r'\?'),
    'named': re.compile(r'(?<![:\w]):([A-Za-z_][A-Za-z_0-9]*)'),
    'at': re.compile(r'(?<![@\w$#)])@([A-Za-z_][A-Za-z_0-9]*)'),
    'pyformat': re.compile(r'%(?:\(([A-Za-z_][A-Za-z_0-9]*)\))?s'),
    'numeric': re.compile(r'\$(\d+)'),
}

## the styles which name their parameters - the same name used twice is one parameter
NAMED_STYLES = ('named', 'at', 'pyformat')

NUMBERED = re.compile(r'\$(\d+)')

## The name which stands in the place of a parameter while the statement is converted.
##
## $1 cannot be used for that: every converter of this migrator parses the statement, and
## the parsers read '$1' as a column named '$1' and write it back quoted - "$1" - which is
## not a parameter any more. A plain identifier survives that unharmed: it may be quoted,
## it may change its case, and it is still recognisable afterwards. The numbered form is
## what the target test and the output file get.
CONVERSION_TOKEN = 'cpgm_bind_param_{number}'
CONVERSION_TOKEN_PATTERN = re.compile(r'(?i)"?\bcpgm_bind_param_(\d+)\b"?')


class Parameters:
    """
    The parameters of one statement: what stood in the text, what was put in their place
    and what has to go back.
    """

    def __init__(self, style, markers, statement, conversion_statement=None):
        self.style = style
        ## markers[i] is the text which stood at the place of $(i+1)
        self.markers = markers
        ## the statement with every marker replaced by $1..$n - what PostgreSQL is asked about
        self.statement = statement
        ## the same statement with the markers replaced by an identifier the converters of the
        ## connectors carry through unharmed
        self.conversion_statement = conversion_statement if conversion_statement is not None else statement

    def to_numbered(self, converted):
        """
        The converted statement with the conversion tokens turned back into $1..$n - the form
        PostgreSQL understands and the form everything behind the conversion works with.
        """
        return CONVERSION_TOKEN_PATTERN.sub(lambda match: f"${int(match.group(1))}", converted)

    @property
    def count(self):
        return len(self.markers)

    def restore(self, converted, parameter_output='original'):
        """
        The converted statement with the markers put back, and the warnings the round trip
        produced. With parameter_output='numeric' the $1..$n are kept, which is what an
        application being ported to a PostgreSQL driver at the same time wants.
        """
        warnings = []
        order = numbers_in_order(converted)

        if len(order) != self.count or sorted(order) != list(range(1, self.count + 1)):
            missing = sorted(set(range(1, self.count + 1)) - set(order))
            if missing:
                warnings.append(
                    f"BLOCKING: the conversion lost the bind parameter(s) "
                    f"{', '.join(self.spell(number) for number in missing)} - the converted statement takes "
                    f"{len(order)} parameter(s) where the original takes {self.count}. Do not use it as it stands.")
            else:
                warnings.append(
                    f"BLOCKING: the converted statement takes {len(order)} bind parameters where the original "
                    f"takes {self.count}. Do not use it as it stands.")
        elif order != sorted(order):
            warnings.append(
                "BLOCKING: the conversion moved the bind parameters into another order "
                f"({', '.join(self.spell(number) for number in order)}). The values your application binds "
                "would land in the wrong places - reorder them, or use parameter_output: numeric.")

        if parameter_output == 'numeric' or self.style == 'none':
            return converted, warnings

        def put_back(match):
            number = int(match.group(1))
            if 1 <= number <= self.count:
                return self.markers[number - 1]
            return match.group(0)

        return NUMBERED.sub(put_back, converted), warnings

    def spell(self, number):
        """How a parameter is named in a message: as it stands in the original statement."""
        if 1 <= number <= self.count:
            return f"{self.markers[number - 1]} (${number})"
        return f"${number}"

    def describe(self):
        """One line for the output file: how many parameters and what they look like."""
        if not self.markers:
            return 'parameters: none'
        shown = ', '.join(self.markers[:6]) + (', ...' if self.count > 6 else '')
        return f"parameters: {self.count} ({shown}) -> $1..${self.count}"


def outside_literals(text):
    """The stretches of the statement in which a marker is a marker and not text."""
    return safe_regions(text)


def detect_style(text):
    """
    The style of the placeholders in the statement, and the styles which were also found.

    A file which mixes two styles is reported rather than guessed at: the second style is
    almost always something else - a cast, a variable of the source - and reading it as a
    parameter would take a piece of the statement away.
    """
    found = {}
    regions = outside_literals(text)
    for style, pattern in PATTERNS.items():
        count = 0
        for start, end in regions:
            count += len(pattern.findall(text[start:end]))
        if count:
            found[style] = count
    if not found:
        return 'none', []
    ordered = sorted(found.items(), key=lambda item: (-item[1], item[0]))
    return ordered[0][0], [style for style, _count in ordered[1:]]


def extract(text, parameter_style='auto'):
    """
    The statement with its bind parameters replaced by $1..$n, and what has to go back.

    A named parameter used twice is one parameter, as it is for the driver which binds it:
    ':cust' written twice becomes $1 twice.
    """
    if parameter_style not in STYLES:
        raise ValueError(f"Unknown parameter_style '{parameter_style}' - one of {', '.join(STYLES)}.")

    also_found = []
    if parameter_style == 'auto':
        style, also_found = detect_style(text)
    else:
        style = parameter_style

    if style == 'none':
        return Parameters('none', [], text, text), []

    warnings = []
    if parameter_style == 'auto' and also_found:
        warnings.append(
            f"the statement holds more than one kind of placeholder ({style} and "
            f"{', '.join(also_found)}) - only {style} was taken as a bind parameter. "
            f"Set parameter_style in the configuration if that is wrong.")

    pattern = PATTERNS[style]
    markers = []
    numbers_by_name = {}
    numbered_pieces = []
    token_pieces = []
    position = 0

    for start, end in outside_literals(text):
        ## everything up to this stretch is a literal or a comment and is kept as it is
        numbered_pieces.append(text[position:start])
        token_pieces.append(text[position:start])
        segment = text[start:end]
        last = 0
        for match in pattern.finditer(segment):
            name = match.group(1) if pattern.groups else None
            if style in NAMED_STYLES and name and name in numbers_by_name:
                number = numbers_by_name[name]
            else:
                markers.append(match.group(0))
                number = len(markers)
                if style in NAMED_STYLES and name:
                    numbers_by_name[name] = number
            numbered_pieces.append(segment[last:match.start()])
            token_pieces.append(segment[last:match.start()])
            numbered_pieces.append(f'${number}')
            token_pieces.append(CONVERSION_TOKEN.format(number=number))
            last = match.end()
        numbered_pieces.append(segment[last:])
        token_pieces.append(segment[last:])
        position = end
    numbered_pieces.append(text[position:])
    token_pieces.append(text[position:])

    return Parameters(style, markers, ''.join(numbered_pieces), ''.join(token_pieces)), warnings


## The markers the mechanisms of the source test accept in the place of a bind parameter.
## 'numbered' is $1..$n, which is what the statement already carries; 'qmark' is the '?' of
## the SQL standard, which every driver of a prepared statement takes; 'oracle' is the :1..:n
## python-oracledb binds by position. A mechanism which submits the statement as a batch -
## SET NOEXEC ON, EXPLAIN - has no place for a marker at all and declares None.
SOURCE_TEST_STYLES = ('numbered', 'qmark', 'oracle')


def to_source_test_style(text, style):
    """
    The statement written with the markers one mechanism of the source test accepts.

    It is given the statement as $1..$n and answers it in the style asked for. A $1 inside a
    string literal or a comment is text and is left alone, which is what safe_regions() is
    for - the same rule every other pass over a statement in this module follows.
    """
    if style == 'numbered':
        return text
    if style not in SOURCE_TEST_STYLES:
        raise ValueError(
            f"Unknown source test parameter style '{style}' - one of {', '.join(SOURCE_TEST_STYLES)}.")

    def marker(match):
        number = int(match.group(1))
        return '?' if style == 'qmark' else f':{number}'

    pieces = []
    position = 0
    for start, end in safe_regions(text):
        pieces.append(text[position:start])
        pieces.append(NUMBERED.sub(marker, text[start:end]))
        position = end
    pieces.append(text[position:])
    return ''.join(pieces)


def numbers_in_order(text):
    """The parameter numbers of a statement, in the order they stand in it, each once."""
    seen = []
    for start, end in safe_regions(text):
        for match in NUMBERED.finditer(text[start:end]):
            number = int(match.group(1))
            if number not in seen:
                seen.append(number)
    return seen
