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
Cutting a file of SQL into the statements it holds.

text.split(';') is wrong on the first file it meets: a semicolon inside a string literal,
inside a comment or inside a $$ quoted body is not the end of a statement, and the files
exported from a Sybase ASE or MS SQL client are separated by GO on a line of its own rather
than by a semicolon at all. The scanner here knows the literals and the comments of the
dialects this migrator reads from, so a separator inside one of them is text and nothing
else.
"""

import hashlib
import re

## GO, alone on its line, optionally with the repeat count the clients of Sybase ASE and
## MS SQL Server accept behind it
GO_LINE = re.compile(r'^[ \t]*GO(?:[ \t]+\d+)?[ \t]*(?:--.*)?$', re.IGNORECASE)

## the yesql / sqlc convention, written above the statement it names
NAME_COMMENT = re.compile(r'^\s*--\s*name\s*:\s*(\S+)', re.IGNORECASE)

SEPARATORS = ('auto', 'semicolon', 'go', 'blank_line', 'whole_file')


class Statement:
    """One statement of one file, with everything needed to point back at where it stands."""

    def __init__(self, text, ordinal, line_from, line_to, input_file='', name=None):
        self.text = text
        self.ordinal = ordinal
        self.line_from = line_from
        self.line_to = line_to
        self.input_file = input_file
        self.name = name

    @property
    def sha256(self):
        """
        The hash of the statement with its whitespace normalised - the same statement written
        with another indentation is the same statement. It is what lets a repetition be
        converted once and a later run see that nothing changed.
        """
        normalised = re.sub(r'\s+', ' ', self.text).strip()
        return hashlib.sha256(normalised.encode('utf-8')).hexdigest()

    @property
    def location(self):
        return f"{self.input_file}:{self.line_from}-{self.line_to}"

    def __repr__(self):
        return f"Statement({self.location}, ordinal={self.ordinal}, name={self.name!r})"


def strip_bom(text):
    return text[1:] if text.startswith('﻿') else text


def scan_boundaries(text):
    """
    The positions in the text at which a statement can end, and what ended it.

    Returns a list of (start, end, separator) - end is the index behind the last character
    of the statement, separator is ';', 'GO' or None for the end of the text. Everything
    inside a string literal, a quoted identifier or a comment is passed over.
    """
    boundaries = []
    length = len(text)
    index = 0
    statement_start = 0
    line_start = True

    while index < length:
        character = text[index]

        ## a comment reaches to the end of its line, and a separator inside it is text
        if character == '-' and text.startswith('--', index):
            end_of_line = text.find('\n', index)
            index = length if end_of_line == -1 else end_of_line
            continue

        if character == '/' and text.startswith('/*', index):
            end_of_comment = text.find('*/', index + 2)
            index = length if end_of_comment == -1 else end_of_comment + 2
            line_start = False
            continue

        ## string literals and quoted identifiers of the dialects this migrator reads
        if character in ("'", '"', '`'):
            index = skip_quoted(text, index, character)
            line_start = False
            continue

        if character == '[':
            ## the bracketed identifier of T-SQL; ']]' is an escaped ']' inside it
            index = skip_bracketed(text, index)
            line_start = False
            continue

        if character == '$':
            end_of_dollar = skip_dollar_quoted(text, index)
            if end_of_dollar is not None:
                index = end_of_dollar
                line_start = False
                continue

        if character == ';':
            boundaries.append((statement_start, index, ';'))
            index += 1
            statement_start = index
            line_start = True
            continue

        if line_start and character in ('G', 'g'):
            end_of_line = text.find('\n', index)
            line = text[index:length if end_of_line == -1 else end_of_line]
            if GO_LINE.match(line):
                boundaries.append((statement_start, index, 'GO'))
                index = length if end_of_line == -1 else end_of_line + 1
                statement_start = index
                line_start = True
                continue

        if character == '\n':
            line_start = True
        elif not character.isspace():
            line_start = False
        index += 1

    if statement_start < length:
        boundaries.append((statement_start, length, None))
    return boundaries


def skip_quoted(text, index, quote):
    """The index behind the closing quote. A doubled quote inside the literal is a quote."""
    length = len(text)
    index += 1
    while index < length:
        if text[index] == quote:
            if index + 1 < length and text[index + 1] == quote:
                index += 2
                continue
            return index + 1
        ## a backslash escape - MySQL and MariaDB write '\'' inside a literal
        if text[index] == '\\' and quote == "'" and index + 1 < length:
            index += 2
            continue
        index += 1
    ## a literal which is never closed: the rest of the file belongs to it
    return length


def skip_bracketed(text, index):
    length = len(text)
    index += 1
    while index < length:
        if text[index] == ']':
            if index + 1 < length and text[index + 1] == ']':
                index += 2
                continue
            return index + 1
        index += 1
    return length


def skip_dollar_quoted(text, index):
    """
    The index behind the closing $tag$ of a dollar quoted body, or None when what stands
    here is not one - a bare '$' of a parameter placeholder, for instance.
    """
    match = re.compile(r'\$([A-Za-z_][A-Za-z_0-9]*)?\$').match(text, index)
    if not match:
        return None
    closing = text.find(match.group(0), match.end())
    return len(text) if closing == -1 else closing + len(match.group(0))


def blank_line_boundaries(text):
    """The same as scan_boundaries, with a run of blank lines as the separator."""
    boundaries = []
    ## the positions of the blank lines which are not inside a literal or a comment - the
    ## scanner is reused for that, its boundaries only mark where it is safe to look
    safe = safe_regions(text)
    statement_start = 0
    for match in re.finditer(r'\n[ \t]*\n', text):
        if not any(start <= match.start() < end for start, end in safe):
            continue
        boundaries.append((statement_start, match.start(), 'blank line'))
        statement_start = match.end()
    if statement_start < len(text):
        boundaries.append((statement_start, len(text), None))
    return boundaries


def safe_regions(text):
    """
    The stretches of the text which are neither a literal, a quoted identifier nor a
    comment - the only places where a separator is a separator.
    """
    regions = []
    length = len(text)
    index = 0
    region_start = 0
    while index < length:
        character = text[index]
        skipped = None
        if character == '-' and text.startswith('--', index):
            end_of_line = text.find('\n', index)
            skipped = length if end_of_line == -1 else end_of_line
        elif character == '/' and text.startswith('/*', index):
            end_of_comment = text.find('*/', index + 2)
            skipped = length if end_of_comment == -1 else end_of_comment + 2
        elif character in ("'", '"', '`'):
            skipped = skip_quoted(text, index, character)
        elif character == '[':
            skipped = skip_bracketed(text, index)
        elif character == '$':
            skipped = skip_dollar_quoted(text, index)
        if skipped is not None:
            regions.append((region_start, index))
            index = skipped
            region_start = index
            continue
        index += 1
    regions.append((region_start, length))
    return regions


def has_go_separator(text):
    """Whether the file uses GO on a line of its own - which decides what 'auto' does."""
    return any(separator == 'GO' for _start, _end, separator in scan_boundaries(text))


def split_statements(text, separator='auto', input_file=''):
    """
    The statements of one file, in the order they stand in it.

    separator: auto | semicolon | go | blank_line | whole_file. 'auto' takes both the
    semicolon and a GO line, which is what a file exported from a client of Sybase ASE or
    MS SQL Server holds; neither can appear as a separator inside the other.
    """
    if separator not in SEPARATORS:
        raise ValueError(f"Unknown statement_separator '{separator}' - one of {', '.join(SEPARATORS)}.")

    text = strip_bom(text).replace('\r\n', '\n').replace('\r', '\n')

    if separator == 'whole_file':
        boundaries = [(0, len(text), None)]
    elif separator == 'blank_line':
        boundaries = blank_line_boundaries(text)
    else:
        boundaries = scan_boundaries(text)
        if separator == 'semicolon':
            boundaries = keep_separator(text, boundaries, ';')
        elif separator == 'go':
            boundaries = keep_separator(text, boundaries, 'GO')

    statements = []
    ordinal = 0
    for start, end, _separator in boundaries:
        fragment = text[start:end]
        if not fragment.strip():
            continue
        ## the whitespace in front of the statement belongs to the file, not to it - and the
        ## line the statement begins at is counted from where it really begins
        leading = len(fragment) - len(fragment.lstrip())
        fragment_start = start + leading
        statement_text = fragment[leading:].rstrip()
        ## an indented statement keeps its indentation on the lines behind the first
        if not statement_text.strip() or is_only_comments(statement_text):
            continue
        ordinal += 1
        line_from = text.count('\n', 0, fragment_start) + 1
        line_to = line_from + statement_text.count('\n')
        statements.append(Statement(
            text=statement_text,
            ordinal=ordinal,
            line_from=line_from,
            line_to=line_to,
            input_file=input_file,
            name=statement_name(statement_text)))
    return statements


def keep_separator(text, boundaries, separator):
    """
    Only one of the two separators cuts, the other one is part of the statement. The
    boundaries are merged, so that a statement ending at an ignored separator continues.
    """
    kept = []
    start = 0
    for boundary_start, boundary_end, boundary_separator in boundaries:
        if boundary_separator == separator or boundary_separator is None:
            kept.append((start, boundary_end, boundary_separator))
            ## behind the separator itself
            start = boundary_end + (1 if boundary_separator == ';' else 0)
            if boundary_separator == 'GO':
                end_of_line = text.find('\n', boundary_end)
                start = len(text) if end_of_line == -1 else end_of_line + 1
    if start < len(text) and (not kept or kept[-1][1] < len(text)):
        kept.append((start, len(text), None))
    return kept


def is_only_comments(text):
    """
    Whether the fragment holds nothing but comments and whitespace.

    A file of application statements begins with a header describing it, and the header is
    followed by the separator like everything else. That is not a statement: reported as one
    it would be counted, converted and answered with "the parser read no statement at all",
    which says nothing to anybody.
    """
    without_block = re.sub(r'/\*.*?\*/', ' ', text, flags=re.DOTALL)
    without_line = re.sub(r'(?m)--.*$', ' ', without_block)
    return not without_line.strip()


def statement_name(statement_text):
    """The name of the statement, from an annotation comment written above it."""
    for line in statement_text.splitlines():
        if not line.strip():
            continue
        match = NAME_COMMENT.match(line)
        if match:
            return match.group(1)
        if line.lstrip().startswith('--'):
            continue
        return None
    return None
