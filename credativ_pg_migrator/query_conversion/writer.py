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
            'status': self.status,
            'reason': self.reason,
            'source_sql': self.statement.text,
            'target_sql': self.output_sql,
            'warnings': list(self.warnings),
            'source_test': {'result': self.source_test[0], 'message': self.source_test[1]},
            'target_test': {'result': self.target_test[0], 'message': self.target_test[1],
                            'duration_ms': self.target_test_ms},
            'parameters': self.parameters_line,
            'identical_to': self.identical_to,
        }


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
                json.dump([result.as_dict() for result in results], handle, indent=2, ensure_ascii=False)
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


def render_summary(all_results):
    """The closing count of the whole run, for the log and for the console."""
    counts = counts_of(all_results)
    output = io.StringIO()
    output.write('[ QUERY CONVERSION ]\n')
    output.write(f"  statements: {len(all_results)}\n")
    for status in (CONVERTED, UNCHANGED, CONVERTED_FAILING, NOT_CONVERTED, SKIPPED):
        output.write(f"  {status.lower().replace(' ', '_'):18}: {counts.get(status, 0)}\n")
    reasons = {}
    for result in all_results:
        if result.status in (CONVERTED_FAILING, NOT_CONVERTED, SKIPPED) and result.reason:
            key = result.reason.split(' - ')[0][:80]
            reasons[key] = reasons.get(key, 0) + 1
    if reasons:
        output.write('  most frequent reasons:\n')
        for reason, count in sorted(reasons.items(), key=lambda item: -item[1])[:5]:
            output.write(f"    {count:4} x {reason}\n")
    return output.getvalue()
