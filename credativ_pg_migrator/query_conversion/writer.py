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
The files the query conversion hands over.

The output file is the deliverable: what a developer reads and what they paste into the
application. It therefore says of every statement what happened to it, what was tested and
what the test answered - and a statement which was not converted or was refused stands in
it as a comment, so that the file as a whole can still be run.

The file is written to be the same for the same input. The duration of a test is not in it
for that reason; it stands in the sidecar and in the protocol table, where a number which
changes from run to run does no harm.
"""

import csv
import datetime
import io
import json
import os

## the five words which say what happened to a statement, and nothing else
CONVERTED = 'CONVERTED'
UNCHANGED = 'UNCHANGED'
CONVERTED_FAILING = 'CONVERTED_FAILING'
NOT_CONVERTED = 'NOT CONVERTED'
SKIPPED = 'SKIPPED'

RULE = '-- ' + '=' * 74
THIN_RULE = '-- ' + '-' * 74


class StatementResult:
    """What became of one statement, from the file it stood in to the test of the result."""

    def __init__(self, statement, total):
        self.statement = statement
        self.total = total
        self.status = NOT_CONVERTED
        self.reason = ''
        self.converted_sql = ''
        self.output_sql = ''
        self.warnings = []
        self.source_test = ('not run', '')
        self.target_test = ('not run', '')
        self.target_test_ms = None
        self.parameters_line = ''
        self.identical_to = None
        ## for the protocol table of §11: what the statement is, which gate refused it and
        ## when it was worked on
        self.statement_kind = ''
        self.gate_refused = None
        self.unresolved_objects = []
        self.task_started = None
        self.task_completed = None

    @property
    def ordinal(self):
        return self.statement.ordinal

    @property
    def name(self):
        return self.statement.name

    @property
    def is_failure(self):
        """Whether the run has to answer with a non-zero exit code because of this one."""
        return self.status in (CONVERTED_FAILING, NOT_CONVERTED)

    def as_dict(self):
        return {
            'input_file': self.statement.input_file,
            'ordinal': self.statement.ordinal,
            'line_from': self.statement.line_from,
            'line_to': self.statement.line_to,
            'name': self.statement.name,
            'sha256': self.statement.sha256,
            'statement_kind': self.statement_kind or None,
            'gate_refused': self.gate_refused,
            'status': self.status,
            'reason': self.reason,
            'source_sql': self.statement.text,
            'target_sql': self.output_sql,
            'warnings': list(self.warnings),
            'unresolved_objects': list(self.unresolved_objects),
            'task_started': self.task_started,
            'task_completed': self.task_completed,
            'source_test': {'result': self.source_test[0], 'message': self.source_test[1]},
            'target_test': {'result': self.target_test[0], 'message': self.target_test[1],
                            'duration_ms': self.target_test_ms},
            'parameters': self.parameters_line,
            'identical_to': self.identical_to,
        }


def json_ready(value):
    """
    The values a result holds which JSON does not know, rendered as text.

    The timestamps of a statement are datetimes, because that is what the protocol table takes
    and as_dict() serves both. Without this the sidecar stopped the whole run with "Object of
    type datetime is not JSON serializable" - after the output file had been written, so the
    deliverable stood there and the run failed behind it, with no protocol rows and no summary.

    Anything else is still an error rather than being stringified: the sidecar is what a CI job
    reads, and a type nobody thought about must not be written into it without a word.
    """
    if isinstance(value, datetime.datetime):
        return value.isoformat(sep=' ', timespec='milliseconds')
    if isinstance(value, datetime.date):
        return value.isoformat()
    raise TypeError(f"the sidecar cannot hold a value of type {type(value).__name__}")


def comment_block(text, indent='--   '):
    return '\n'.join(f"{indent}{line}".rstrip() for line in text.splitlines())


def commented_out(text):
    return '\n'.join(f"-- {line}".rstrip() for line in text.splitlines())


def counts_of(results):
    counts = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    return counts


class OutputWriter:
    """
    Writes one output file per input file, and the sidecar next to it.

    An output path which is the same as an input path is refused: the files of the user are
    the source of truth of this step and are never written to.
    """

    def __init__(self, settings, log):
        self.directory = settings.get('directory') or ''
        self.prefix = settings.get('prefix') or ''
        self.suffix = settings.get('suffix', '_pg')
        self.overwrite = bool(settings.get('overwrite', False))
        self.include_original = settings.get('include_original', True)
        self.sidecar = settings.get('sidecar', 'json')
        self.log = log

    def output_path(self, input_file):
        directory = self.directory or os.path.dirname(os.path.abspath(input_file))
        stem, extension = os.path.splitext(os.path.basename(input_file))
        return os.path.join(directory, f"{self.prefix}{stem}{self.suffix}{extension}")

    def check_path(self, input_file, output_file):
        if os.path.abspath(output_file) == os.path.abspath(input_file):
            raise ValueError(
                f"The converted statements of {input_file} would be written over the file they "
                f"were read from. Set query_conversion.output.suffix or output.directory to "
                f"something which does not name the input file.")
        if os.path.exists(output_file) and not self.overwrite:
            raise ValueError(
                f"{output_file} exists already. Set query_conversion.output.overwrite to true "
                f"to replace it, or write to another directory.")

    def check_all_paths(self, input_files):
        """
        Every output path of the run, checked before the first file is read.

        None of these answers needs a conversion: an output which would be written over its
        own input, an output which exists already and may not be replaced, and two input
        files of different directories whose outputs would land on the same path are all
        known from the names alone. Checking them here costs nothing and refuses in time;
        checking them at write time threw away a file which had already been converted and
        tested, and stopped the run before the files behind it were read.
        """
        planned = {}
        for input_file in input_files:
            output_file = self.output_path(input_file)
            self.check_path(input_file, output_file)
            key = os.path.abspath(output_file)
            if key in planned:
                raise ValueError(
                    f"{input_file} and {planned[key]} would both be written to {output_file}. "
                    f"Give query_conversion.output a directory of its own, or a prefix which "
                    f"tells the two apart - one of the two answers would otherwise be lost.")
            planned[key] = input_file
        return sorted(planned)

    def write(self, input_file, results, header):
        output_file = self.output_path(input_file)
        self.check_path(input_file, output_file)
        directory = os.path.dirname(os.path.abspath(output_file))
        if directory and not os.path.isdir(directory):
            os.makedirs(directory, exist_ok=True)

        text = self.render(input_file, results, header)
        with open(output_file, 'w', encoding='utf-8') as handle:
            handle.write(text)
        self.log('INFO', f"query_conversion: writer: {len(results)} statement(s) of {input_file} written to {output_file}.")

        sidecar_file = self.write_sidecar(output_file, results)
        return output_file, sidecar_file

    def render(self, input_file, results, header):
        lines = [RULE,
                 f"-- Converted application statements of {os.path.basename(input_file)}",
                 f"-- {header['tool']}",
                 f"-- source: {header['source_db_type']}   target: {header['target_db_type']} "
                 f"(schema \"{header['target_schema']}\")"]
        for line in header.get('notes', []):
            lines.append(f"-- {line}")

        counts = counts_of(results)
        lines.append(f"-- statements: {len(results)}   " + '   '.join(
            f"{status.lower().replace(' ', '_')}: {counts[status]}" for status in sorted(counts)))
        lines.append(RULE)
        lines.append('')

        for result in results:
            lines.append(self.render_block(result))
            lines.append('')
        return '\n'.join(lines).rstrip() + '\n'

    def render_block(self, result):
        heading = f"-- [{result.ordinal}/{result.total}]"
        if result.name:
            heading += f"  name: {result.name}"
        lines = [RULE, heading,
                 f"-- source: {result.statement.location}",
                 f"-- status: {result.status}"]
        if result.reason:
            lines.append(f"-- reason: {result.reason}")
        if result.identical_to:
            lines.append(f"-- identical to [{result.identical_to}] - converted and tested once")
        lines.append(f"-- source test: {result.source_test[0]}"
                     + (f" - {result.source_test[1]}" if result.source_test[1] else ''))
        lines.append(f"-- target test: {result.target_test[0]}"
                     + (f" - {result.target_test[1]}" if result.target_test[1] else ''))
        if result.parameters_line:
            lines.append(f"-- {result.parameters_line}")
        for warning in result.warnings:
            lines.append(f"-- WARNING: {warning}")

        if self.include_original:
            lines.append(THIN_RULE)
            lines.append('-- ORIGINAL:')
            lines.append(comment_block(result.statement.text))
        lines.append(RULE)

        if result.status in (CONVERTED, UNCHANGED, CONVERTED_FAILING) and result.output_sql:
            body = result.output_sql.rstrip().rstrip(';')
            ## a statement whose test failed is written as a comment as well - the file has to
            ## stay runnable as a whole, and this one would stop it
            lines.append(f"{body};" if result.status != CONVERTED_FAILING else commented_out(f"{body};"))
        else:
            ## nothing was produced, or nothing may be run: the original stands here as a
            ## comment so that the file is complete and still runnable
            lines.append(commented_out(result.statement.text))
        return '\n'.join(lines)

    def write_sidecar(self, output_file, results):
        if self.sidecar in (None, '', 'off'):
            return None
        stem, _extension = os.path.splitext(output_file)
        if self.sidecar == 'json':
            path = f"{stem}.json"
            with open(path, 'w', encoding='utf-8') as handle:
                json.dump([result.as_dict() for result in results], handle, indent=2,
                          ensure_ascii=False, default=json_ready)
                handle.write('\n')
            return path
        if self.sidecar == 'csv':
            path = f"{stem}.csv"
            with open(path, 'w', encoding='utf-8', newline='') as handle:
                writer = csv.writer(handle)
                writer.writerow(['input_file', 'ordinal', 'line_from', 'line_to', 'name', 'sha256',
                                 'status', 'reason', 'source_test', 'target_test', 'target_test_ms',
                                 'warnings', 'target_sql'])
                for result in results:
                    writer.writerow([
                        result.statement.input_file, result.statement.ordinal,
                        result.statement.line_from, result.statement.line_to,
                        result.statement.name or '', result.statement.sha256,
                        result.status, result.reason,
                        result.source_test[0], result.target_test[0], result.target_test_ms or '',
                        ' | '.join(result.warnings), result.output_sql])
            return path
        raise ValueError(f"Unknown query_conversion.output.sidecar '{self.sidecar}' - json, csv or off.")


WIDTH = 80

## the order the statuses are counted and reported in - from the best outcome to the worst
STATUS_ORDER = (CONVERTED, UNCHANGED, CONVERTED_FAILING, NOT_CONVERTED, SKIPPED)

## the heading of each status column, and the width it is printed in
SHORT_STATUS = {
    CONVERTED: 'conv',
    UNCHANGED: 'unch',
    CONVERTED_FAILING: 'fail',
    NOT_CONVERTED: 'n/conv',
    SKIPPED: 'skip',
}
STATUS_COLUMN = 6


def shorten(text, length):
    text = ' '.join((text or '').split())
    return text if len(text) <= length else text[:length - 3] + '...'


def render_summary(all_results, context=None):
    """
    The closing summary of the run, in the shape the summary of a migration has: what was
    read, what became of it per file, what has to be looked at and where the answer was
    written.

    It is the part of the run a user reads. Everything in it is countable - a statement is
    in exactly one of the five statuses - and everything which is not simply converted is
    named with the place it stands in, so that it can be found without opening the file.
    """
    context = context or {}
    counts = counts_of(all_results)
    lines = []

    lines.append('=' * WIDTH)
    lines.append('QUERY CONVERSION SUMMARY'.center(WIDTH))
    lines.append('=' * WIDTH)
    lines.append('')

    lines.append('[ CONTEXT ]')
    if context.get('source_db_type'):
        lines.append(f"Source: {context.get('source_database', '-')}, "
                     f"schema: {context.get('source_schema', '-')} ({context['source_db_type']})")
    if context.get('target_db_type'):
        lines.append(f"Target: {context.get('target_database', '-')}, "
                     f"schema: {context.get('target_schema', '-')} ({context['target_db_type']})")
    for note in context.get('notes', []):
        lines.append(note)
    lines.append('')

    ## ------------------------------------------------------------------ per file
    files = []
    for result in all_results:
        if result.statement.input_file not in files:
            files.append(result.statement.input_file)

    lines.append('[ QUERY CONVERSION ]')
    lines.append('-' * WIDTH)
    lines.append(f"{'File':<34} | {'stmts':>5} | "
                 + ' | '.join(f"{SHORT_STATUS[status]:>{STATUS_COLUMN}}" for status in STATUS_ORDER))
    lines.append('-' * WIDTH)
    for input_file in files:
        of_file = [result for result in all_results if result.statement.input_file == input_file]
        file_counts = counts_of(of_file)
        lines.append(
            f"{shorten(os.path.basename(input_file), 34):<34} | {len(of_file):>5} | "
            + ' | '.join(f"{file_counts.get(status, 0):>{STATUS_COLUMN}}" for status in STATUS_ORDER))
    lines.append('-' * WIDTH)
    lines.append(f"{'TOTAL':<34} | {len(all_results):>5} | "
                 + ' | '.join(f"{counts.get(status, 0):>{STATUS_COLUMN}}" for status in STATUS_ORDER))
    lines.append('-' * WIDTH)
    lines.append('conv = converted   unch = already valid PostgreSQL   fail = converted, the '
                 'target refused it')
    lines.append('n/conv = the converter could not do it   skip = a gate refused it (not a read)')
    lines.append('')

    ## ------------------------------------------------------------------ what to look at
    attention = [result for result in all_results if result.is_failure]
    if attention:
        lines.append('[ STATEMENTS WHICH NEED ATTENTION ]')
        lines.append('-' * WIDTH)
        for result in attention[:20]:
            name = f" {result.name}" if result.name else ''
            ## the file by its name and the lines it stands at - the whole path is in the
            ## output file and in the protocol table, and would take the line up here
            where = (f"{os.path.basename(result.statement.input_file)}:"
                     f"{result.statement.line_from}-{result.statement.line_to}")
            lines.append(f"{result.status:<18} {shorten(where, 44)}{name}")
            if result.reason:
                lines.append(f"{'':<18} {shorten(result.reason, WIDTH - 20)}")
        if len(attention) > 20:
            lines.append(f"... and {len(attention) - 20} more - the whole list is in the output "
                         f"files and in the protocol table")
        lines.append('')

    ## ------------------------------------------------------------------ the refusals
    refused = [result for result in all_results if result.status == SKIPPED]
    if refused:
        lines.append('[ REFUSED - NOT A READ, NEVER SENT TO A DATABASE ]')
        lines.append('-' * WIDTH)
        reasons = {}
        for result in refused:
            reasons[shorten(result.reason, 66)] = reasons.get(shorten(result.reason, 66), 0) + 1
        for reason, count in sorted(reasons.items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"{count:>5} x {reason}")
        lines.append('')

    ## ------------------------------------------------------------------ the warnings
    with_warnings = [result for result in all_results if result.warnings]
    blocking = [result for result in with_warnings
                if any(warning.startswith('BLOCKING') for warning in result.warnings)]
    if with_warnings:
        lines.append('[ WARNINGS ]')
        lines.append('-' * WIDTH)
        lines.append(f"{len(with_warnings)} statement(s) carry a warning"
                     + (f", {len(blocking)} of them BLOCKING - those must not be used as they stand"
                        if blocking else ''))
        texts = {}
        for result in with_warnings:
            for warning in result.warnings:
                key = shorten(warning, 66)
                texts[key] = texts.get(key, 0) + 1
        for warning, count in sorted(texts.items(), key=lambda item: (-item[1], item[0]))[:8]:
            lines.append(f"{count:>5} x {warning}")
        lines.append('')

    ## ------------------------------------------------------------------ the source test
    ## §8.1 - the statements the SOURCE refused are the ones which were broken before the
    ## migrator read them, and they are counted apart from everything else here so that a
    ## reader who sees "12 not converted" can see at once how many of the twelve are the
    ## conversion's doing.
    asked = [result for result in all_results if result.source_test[0] != 'not run']
    refused_by_source = [result for result in all_results if result.source_test[0] == 'FAILED']
    if asked:
        outcomes = {}
        for result in asked:
            outcomes[result.source_test[0]] = outcomes.get(result.source_test[0], 0) + 1
        lines.append('[ SOURCE TEST ]')
        lines.append('-' * WIDTH)
        lines.append(f"{len(asked)} statement(s) compiled against the source: "
                     + ', '.join(f"{outcome} {count}" for outcome, count in sorted(outcomes.items())))
        if refused_by_source:
            lines.append(f"{len(refused_by_source)} of them the SOURCE itself refuses - those were "
                         f"broken, or read an object the application makes at run time, before "
                         f"this step saw them:")
            for result in refused_by_source[:5]:
                lines.append(f"  [{result.ordinal}] {shorten(result.source_test[1], WIDTH - 12)}")
            if len(refused_by_source) > 5:
                lines.append(f"  ... and {len(refused_by_source) - 5} more - the blocks name them all")
        lines.append('')

    ## ------------------------------------------------------------------ the target test
    tested = [result for result in all_results if result.target_test_ms is not None]
    if tested:
        outcomes = {}
        for result in tested:
            outcomes[result.target_test[0]] = outcomes.get(result.target_test[0], 0) + 1
        total_ms = sum(result.target_test_ms for result in tested)
        slowest = max(tested, key=lambda result: result.target_test_ms)
        lines.append('[ TARGET TEST ]')
        lines.append('-' * WIDTH)
        lines.append(f"{len(tested)} statement(s) tested: "
                     + ', '.join(f"{outcome} {count}" for outcome, count in sorted(outcomes.items())))
        lines.append(f"total {total_ms:.1f} ms, slowest {slowest.target_test_ms:.1f} ms "
                     f"({shorten(slowest.name or slowest.statement.location, 40)})")
        lines.append('')

    ## ------------------------------------------------------------------ what was written
    written = context.get('written') or []
    if written:
        lines.append('[ FILES WRITTEN ]')
        lines.append('-' * WIDTH)
        for path in written:
            lines.append(f"  {path}")
        lines.append('')

    failures = len(attention)
    lines.append(f"STATEMENTS NEEDING ATTENTION: {failures}"
                 + ('' if failures else ' - every statement was converted or refused as it should be'))
    lines.append('=' * WIDTH)
    return '\n'.join(lines)
