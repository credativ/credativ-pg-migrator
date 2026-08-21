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
The step which converts the SELECT statements of an application.

It runs after a migration, over the migrated target: it creates nothing, it migrates no data
and it never writes to either database. What it does is read files of statements, refuse
everything which is not a read, convert every read with the same connector code which
converts the views of the migration, ask the target whether the result is valid there, and
write files which say of every statement what happened to it.

Started by --convert-queries, or as the closing step of a migration when
query_conversion.run_after_migration is true.
"""

import concurrent.futures
import datetime
import glob
import importlib
import os
import threading
import time

from credativ_pg_migrator.constants import MigratorConstants
from credativ_pg_migrator.database_connector import first_line
from credativ_pg_migrator.migrator_logging import MigratorLogger
from credativ_pg_migrator.query_conversion import classifier
from credativ_pg_migrator.query_conversion import parameters as parameters_module
from credativ_pg_migrator.query_conversion.splitter import split_statements
from credativ_pg_migrator.query_conversion.writer import (
    CONVERTED, CONVERTED_FAILING, NOT_CONVERTED, SKIPPED, UNCHANGED,
    OutputWriter, StatementResult, render_summary)


def probe_statements(sql, settings):
    """
    The statements the target test sends, in order.

    Everything runs inside a transaction which is read only and which is rolled back
    whatever happens. That is the last of the four layers which keep this step from writing:
    even if every gate in front of it were defeated, the transaction cannot write.

    A statement which takes bind parameters is tested with PREPARE whatever the configured
    level is - EXPLAIN of a statement with parameters is refused by PostgreSQL for the same
    reason, so PREPARE is the deepest test such a statement can be given.
    """
    level = settings.get('target_test', 'explain')
    timeout = settings.get('timeout', '30s')
    schema = settings.get('target_schema', '')
    has_parameters = settings.get('has_parameters', False)
    name = settings.get('probe_name', 'credativ_pg_migrator_query_probe')

    statements = ['BEGIN;',
                  'SET LOCAL transaction_read_only = on;',
                  f"SET LOCAL statement_timeout = '{timeout}';"]
    if schema:
        statements.append(f'SET LOCAL search_path TO "{schema}";')

    body = sql.rstrip().rstrip(';')
    if level == 'parse' or has_parameters:
        statements.append(f'PREPARE {name} AS {body};')
        statements.append(f'DEALLOCATE {name};')
    elif level == 'explain':
        statements.append(f'EXPLAIN {body};')
    else:
        raise ValueError(f"Unknown query_conversion.target_test '{level}' - off, parse or explain.")

    statements.append('ROLLBACK;')
    return statements


class QueryConverter:
    """
    Reads the query files of the configuration, converts what may be converted and writes
    the answer. Built the way the Validator is: it takes the parsed configuration, makes its
    own connectors and touches nothing the migration left behind.
    """

    def __init__(self, config_parser, migrator_tables=None):
        self.config_parser = config_parser
        self.logger = MigratorLogger(config_parser.get_log_file()).logger
        self.settings = config_parser.get_query_conversion_config()
        self.source_db_type = config_parser.get_source_db_type()
        self.target_db_type = config_parser.get_target_db_type()
        self.source_schema = config_parser.get_source_schema()
        self.target_schema = config_parser.get_target_schema()
        self.migrator_tables = migrator_tables
        self.local = threading.local()
        ## every connection opened by a worker, so that the run can close them itself
        self.worker_connections = []
        self.worker_sources = []
        self.connections_lock = threading.Lock()
        ## the protocol table keeps the rows of the runs before this one, so a run has to say
        ## which one it is
        self.run_started = None
        self.results = []

    ## ------------------------------------------------------------------ connectors

    def load_connector(self, direction):
        database_type = self.config_parser.get_db_type(direction)
        module_path = MigratorConstants.get_modules().get(database_type)
        if not module_path:
            raise ValueError(f"Unsupported database type: {database_type}")
        module_name, class_name = module_path.split(':')
        connector_class = getattr(importlib.import_module(module_name), class_name)
        return connector_class(self.config_parser, direction)

    def source_connection(self):
        """
        One connector for the source per worker.

        Most sources convert a statement without asking anything: the conversion is a
        transformation of text and the connector is only the place it lives. MS SQL Server is
        the exception - its conversion reads the user defined types of the source - and a
        connector is not written to be used by several threads at once. One per worker is what
        the target already gets and costs nothing where nothing is asked.
        """
        connection = getattr(self.local, 'source', None)
        if connection is None:
            connection = self.load_connector('source')
            self.local.source = connection
            with self.connections_lock:
                self.worker_sources.append(connection)
        return connection

    def target_connection(self):
        """One connection to the target per worker - a connection is not shared by threads."""
        connection = getattr(self.local, 'target', None)
        if connection is None:
            connection = self.load_connector('target')
            connection.connect()
            self.local.target = connection
            ## the thread which opened it is the only one which can close it - self.local
            ## answers the caller's thread and nobody else's - so the connection is registered
            ## here and closed by that thread when the pool is done with it. Without the
            ## register, close_target_connection() ran in the main thread at the end of a file
            ## and closed the connection of the main thread alone: a run over many files opened
            ## files x workers connections, each of them released only when the garbage
            ## collector reached the dead thread rather than by disconnect().
            with self.connections_lock:
                self.worker_connections.append(connection)
        return connection

    def close_target_connection(self):
        """The target connection of the calling thread, closed and forgotten."""
        connection = getattr(self.local, 'target', None)
        if connection is not None:
            self.disconnect_target(connection)
            self.local.target = None

    def disconnect_target(self, connection):
        try:
            connection.disconnect()
        except Exception:
            pass
        with self.connections_lock:
            if connection in self.worker_connections:
                self.worker_connections.remove(connection)

    def close_worker_connections(self):
        """
        Every target connection this run opened, closed - the one of the calling thread and
        the ones of the workers of the pool which has just been shut down.

        A pool is built per input file and its threads are gone when it is; their thread local
        storage goes with them and takes the last reference to the connection with it, so
        nothing but the garbage collector would ever have closed them.
        """
        self.close_target_connection()
        with self.connections_lock:
            remaining = self.worker_connections + self.worker_sources
            self.worker_connections = []
            self.worker_sources = []
        self.local.source = None
        for connection in remaining:
            try:
                connection.disconnect()
            except Exception:
                pass

    ## ------------------------------------------------------------------ prerequisites

    def check_prerequisites(self, source_connection):
        """
        What has to be there before a single file is read, reported plainly.

        The step is meaningful only over a migrated target: every level of the target test
        asks the objects which are there. And a source whose connector cannot convert a bare
        statement stops the run here rather than having its statements passed through as if
        they had been converted.
        """
        if not hasattr(source_connection, 'query_conversion_supported') or not source_connection.query_conversion_supported():
            raise ValueError(
                f"Query conversion is not implemented for source type '{self.source_db_type}'. "
                f"The statements are not passed through unconverted - that would look like a "
                f"conversion without being one.")

        target = self.load_connector('target')
        try:
            target.connect()
            objects = self.count_target_objects(target)
        finally:
            try:
                target.disconnect()
            except Exception:
                pass

        if objects == 0:
            raise ValueError(
                f"The target schema \"{self.target_schema}\" holds no tables or views. The query "
                f"conversion tests every converted statement against the migrated objects, so it "
                f"is run after a migration, not before one.")
        self.print_log_message('INFO', f"Target schema \"{self.target_schema}\" holds {objects} table(s) and view(s).")
        self.check_protocol_matches_config()

    def check_protocol_matches_config(self):
        """
        §3.1(d)2 - the step has to be given the configuration the migration was run with.

        Reading the map of one migration while testing against the schema of another produces
        confident, wrong output, and it is cheap to notice: the protocol table of the tables
        records the schemas the migration really wrote. The strategy suggested the main table
        for this, which turned out to record only the tasks and their timings, so the check is
        made against the tables instead.

        It stops on a mismatch and says nothing when there is no protocol schema to ask - that
        is the reduced mode of §3.1(c), which the header of every output file already states.
        """
        if self.migrator_tables is None:
            self.print_log_message(
                'INFO', 'query_conversion: no connection to the migrator database - the '
                        'configuration is not compared against the protocol tables of a migration.')
            return
        table_name = self.config_parser.get_protocol_name_tables()
        protocol_schema = self.migrator_tables.protocol_schema
        try:
            cursor = self.migrator_tables.protocol_connection.connection.cursor()
            cursor.execute(f'''
                SELECT DISTINCT source_schema_name, target_schema_name
                FROM "{protocol_schema}"."{table_name}"
                WHERE target_schema_name IS NOT NULL
            ''')
            rows = cursor.fetchall()
            cursor.close()
        except Exception as e:
            self.print_log_message(
                'INFO', f"query_conversion: the protocol tables of a migration could not be read "
                        f"({first_line(e)}) - the configuration is not compared against them, and "
                        f"the names of the source are used as they are.")
            return

        if not rows:
            self.print_log_message(
                'INFO', 'query_conversion: the protocol tables hold no migrated table - the '
                        'configuration is not compared against them.')
            return

        targets = sorted({(row[1] or '').strip() for row in rows if row[1]})
        if not any(target.lower() == (self.target_schema or '').lower() for target in targets):
            raise ValueError(
                f"The protocol tables in \"{protocol_schema}\" record a migration into "
                f"{', '.join(chr(34) + target + chr(34) for target in targets)}, and this "
                f"configuration names \"{self.target_schema}\" as the target schema. Reading the "
                f"map of one migration while testing against the schema of another produces "
                f"output which looks right and is not. Run the step with the configuration the "
                f"migration was run with, or point migrator.protocol_schema at the run which "
                f"produced \"{self.target_schema}\".")

        sources = sorted({(row[0] or '').strip() for row in rows if row[0]})
        if sources and not any(source.lower() == (self.source_schema or '').lower() for source in sources):
            self.print_log_message(
                'WARNING', f"query_conversion: the protocol tables record a migration out of "
                           f"{', '.join(sources)}, and this configuration names "
                           f"'{self.source_schema}' as the source schema. The target matches, so "
                           f"the run goes on - but a statement which names an object of another "
                           f"schema is converted with the schema of this configuration.")
        self.print_log_message(
            'INFO', f"query_conversion: the protocol tables in \"{protocol_schema}\" record the "
                    f"migration into \"{self.target_schema}\" which this run is asked about.")

    def count_target_objects(self, target):
        cursor = target.connection.cursor()
        cursor.execute("""
            SELECT count(*) FROM information_schema.tables WHERE table_schema = %s
        """, (self.target_schema,))
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    ## ------------------------------------------------------------------ input files

    def input_files(self):
        """
        The files named by query_conversion.input, resolved against the directory of the
        configuration file, in the order they were written. A pattern which names nothing is
        reported - a silently empty run would look like a run with nothing to do.
        """
        patterns = self.config_parser.get_query_conversion_input()
        base = self.config_parser.get_config_directory()
        files = []
        for pattern in patterns:
            expanded = pattern if os.path.isabs(pattern) else os.path.join(base, pattern)
            matched = sorted(glob.glob(expanded, recursive=True))
            if not matched:
                self.print_log_message('WARNING', f"query_conversion: input: '{pattern}' names no file.")
            for path in matched:
                if os.path.isfile(path) and path not in files:
                    files.append(path)
        return files

    def resolve_output_directory(self):
        directory = self.config_parser.get_query_conversion_output_directory()
        if not directory:
            return ''
        if os.path.isabs(directory):
            return directory
        return os.path.join(self.config_parser.get_config_directory(), directory)

    def read_file(self, path):
        encoding = self.config_parser.get_query_conversion_encoding()
        try:
            with open(path, 'r', encoding=encoding) as handle:
                return handle.read()
        except UnicodeDecodeError as e:
            raise ValueError(
                f"{path} is not readable as {encoding}: {e}. Set query_conversion.encoding to the "
                f"encoding the file really has - reading it wrongly would convert damaged text.") from e

    ## ------------------------------------------------------------------ one statement

    def convert_statement(self, statement, total):
        """
        One statement from the file to the answer: the gates, the parameters, the conversion
        of the connector, the gate on the result and the test against the target.
        """
        result = StatementResult(statement, total)
        result.task_started = datetime.datetime.now()

        ## The bind parameters are taken out before anything parses the statement: '%s' and
        ## '%(name)s' are not SQL in any dialect, so a statement holding them cannot be read
        ## at all while the markers are still in it. The gates which read the text read the
        ## text of the application, the gate which parses reads the statement without them.
        bind_parameters, parameter_warnings = parameters_module.extract(
            statement.text, self.config_parser.get_query_conversion_parameter_style())

        ## the connector rewrites what no parser of its dialect can read - the '*=' outer
        ## join of Sybase ASE - so that a statement its own conversion handles is not
        ## reported as one the migrator cannot read
        source_connection = self.source_connection()
        parse_text = source_connection.prepare_query_for_parsing(bind_parameters.conversion_statement)
        classification = classifier.classify(statement.text, self.source_db_type,
                                             parse_text=parse_text)
        result.statement_kind = classification.kind
        result.gate_refused = f"gate {classification.gate}" if classification.gate else None
        if classification.verdict == 'refused':
            result.status = SKIPPED
            result.reason = classification.reason
            self.print_log_message('INFO', f"query_conversion: [{statement.ordinal}] {statement.location}: skipped - {classification.reason}")
            return result
        if classification.verdict == 'unparsed':
            result.status = NOT_CONVERTED
            result.reason = classification.reason
            self.print_log_message('WARNING', f"query_conversion: [{statement.ordinal}] {statement.location}: not converted - {classification.reason}")
            return result
        result.warnings.extend(classification.warnings)
        result.warnings.extend(parameter_warnings)
        result.parameters_line = bind_parameters.describe()

        ## §7.4 - the same substitution list the view path is given, from the same key of the
        ## configuration. It was applied by three connectors of the ten inside their own body
        ## converter and by nobody else, so a name over a database link went into the output
        ## of the other seven unchanged. It runs here, on the text of the source dialect and
        ## before any parse, which is where those three run it too - their pass then finds
        ## nothing left to replace instead of replacing twice.
        query_code, substituted = source_connection.apply_remote_objects_substitution(
            bind_parameters.conversion_statement)
        for substitution in substituted:
            result.warnings.append(
                f"remote_objects_substitution replaced {substitution} - the converted statement "
                f"reads the object of the target, not the one the text of the application names")

        answer = source_connection.convert_query_code({
            ## the converter sees the statement with the parameters replaced by an identifier
            ## it carries through unharmed; PostgreSQL sees $1..$n further down
            'query_code': query_code,
            'source_schema_name': self.source_schema,
            'target_schema_name': self.target_schema,
            'target_db_type': self.target_db_type,
            'statement_id': f"{statement.input_file}:{statement.ordinal}",
        })

        if not answer.get('converted'):
            result.status = NOT_CONVERTED
            result.reason = answer.get('error') or 'the connector of the source could not convert the statement'
            result.warnings.extend(answer.get('warnings') or [])
            self.print_log_message('WARNING', f"query_conversion: [{statement.ordinal}] {statement.location}: not converted - {result.reason}")
            return result

        result.warnings.extend(answer.get('warnings') or [])
        converted = bind_parameters.to_numbered(answer['code'])

        ## gate 4 - what is about to be sent has to be a read as well
        after = classifier.classify_converted(converted)
        if not after.is_select:
            result.status = SKIPPED
            result.reason = after.reason
            result.gate_refused = 'gate 4'
            self.print_log_message('WARNING', f"query_conversion: [{statement.ordinal}] {statement.location}: refused after conversion - {after.reason}")
            return result

        ## §7.3 is not implemented yet, so a name the statement carries without a schema is
        ## written out as it stands - and the target test resolves it through the search_path
        ## it sets itself, which the application does not necessarily have. Saying so is what
        ## can be done before the name map exists: an OK which rests on a search_path must not
        ## be read as an OK which does not.
        unqualified = classifier.unqualified_tables(after.parsed)
        if unqualified:
            result.warnings.append(
                f"the converted statement names {', '.join(unqualified[:5])}"
                f"{', ...' if len(unqualified) > 5 else ''} without a schema. The target test "
                f"resolved {'them' if len(unqualified) > 1 else 'it'} with "
                f"search_path = \"{self.target_schema}\"; the application has to set the same "
                f"search_path, or the name has to be written out as "
                f"\"{self.target_schema}\".\"{unqualified[0].lower()}\".")

        result.converted_sql = converted
        restored, restore_warnings = bind_parameters.restore(
            converted, self.config_parser.get_query_conversion_parameter_output())
        result.output_sql = restored
        result.warnings.extend(restore_warnings)

        ## the statement of the target is tested as it will be run - with the numbered
        ## parameters PostgreSQL understands, not with the markers of the application
        outcome, message, duration = self.test_on_target(converted, bind_parameters.count > 0)
        result.target_test = (outcome, message)
        result.target_test_ms = duration

        if outcome == 'FAILED':
            result.status = CONVERTED_FAILING
            result.reason = message
        elif same_statement(statement.text, restored):
            result.status = UNCHANGED
        else:
            result.status = CONVERTED
        return result

    def test_on_target(self, sql, has_parameters):
        """
        Ask the target whether the converted statement is valid there. Returns
        (outcome, message, duration in ms). Nothing is executed and nothing is committed.
        """
        level = self.config_parser.get_query_conversion_target_test()
        if level == 'off':
            return 'not run', 'query_conversion.target_test is off', None

        statements = probe_statements(sql, {
            'target_test': level,
            'timeout': self.config_parser.get_query_conversion_timeout(),
            'target_schema': self.target_schema,
            'has_parameters': has_parameters,
        })
        connection = self.target_connection()
        started = time.time()
        cursor = None
        try:
            cursor = connection.connection.cursor()
            for statement in statements:
                cursor.execute(statement)
            duration = round((time.time() - started) * 1000, 1)
            level_used = 'prepare' if (level == 'parse' or has_parameters) else level
            return 'OK', f"{level_used} on {self.target_db_type}", duration
        except Exception as e:
            duration = round((time.time() - started) * 1000, 1)
            message = str(e).strip().splitlines()[0] if str(e).strip() else repr(e)
            ## the transaction is ended by the finally below, whatever happened here
            if 'could not determine data type of parameter' in message.lower():
                return ('INCONCLUSIVE',
                        f"{message} - PostgreSQL cannot infer the type of a bind parameter here. "
                        f"The statement itself was accepted up to that point; give the parameter a "
                        f"cast in the application, or test it with values.", duration)
            return 'FAILED', message, duration
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            self.rollback(connection)

    def rollback(self, connection):
        try:
            cursor = connection.connection.cursor()
            cursor.execute('ROLLBACK;')
            cursor.close()
        except Exception:
            ## the connection is unusable - the next statement opens a new one
            self.close_target_connection()

    ## ------------------------------------------------------------------ the run

    def run(self):
        self.run_started = datetime.datetime.now()
        self.print_log_message('INFO', '=========================================')
        self.print_log_message('INFO', '      Starting Query Conversion          ')
        self.print_log_message('INFO', '=========================================')

        ## the connector of the main thread, which is what the capability gate is asked; every
        ## worker makes one of its own further down
        self.check_prerequisites(self.source_connection())

        files = self.input_files()
        if not files:
            raise ValueError(
                "query_conversion.input names no file. Nothing was converted - which is reported "
                "rather than passed over, so that a wrong path is not read as an empty task.")

        writer = OutputWriter({
            ## a relative directory is resolved against the configuration file, exactly as the
            ## input patterns are - a run started from anywhere writes to the same place
            'directory': self.resolve_output_directory(),
            'prefix': self.config_parser.get_query_conversion_output_prefix(),
            'suffix': self.config_parser.get_query_conversion_output_suffix(),
            'overwrite': self.config_parser.get_query_conversion_output_overwrite(),
            'include_original': self.config_parser.get_query_conversion_output_include_original(),
            'sidecar': self.config_parser.get_query_conversion_output_sidecar(),
        }, self.print_log_message)
        header = {
            'tool': f"{MigratorConstants.get_full_name()} {MigratorConstants.get_version()}",
            'source_db_type': self.source_db_type,
            'target_db_type': self.target_db_type,
            'target_schema': self.target_schema,
            'notes': [
                'name mapping: off - the names of the source are used as they are '
                '(the map of the migration is read from the protocol tables in a later version)',
                'source test: not run - the statements are never sent to the source database',
                f"target test: {self.config_parser.get_query_conversion_target_test()} - inside a read only "
                f"transaction which is rolled back",
            ],
        }

        ## Every output path is decided before the first file is read. It used to be checked
        ## by the writer, which runs after a file has been converted and tested - so an output
        ## file which existed already, on the first of twenty input files, threw the work of
        ## that file away and stopped the run before the other nineteen. Nothing here needs a
        ## conversion to be answered, so it is answered while it still costs nothing.
        writer.check_all_paths(files)

        stop_on_error = (self.config_parser.get_query_conversion_on_error() == 'stop')
        written = []
        stopped = None
        try:
            for path in files:
                results = self.convert_file(path)
                self.results.extend(results)
                output_file, sidecar = writer.write(path, results, header)
                written.append(output_file)
                if sidecar:
                    written.append(sidecar)
                ## on_error: stop ends the run - but not before the file which holds the answer
                ## has been written. The statements were all converted by the time the first
                ## failure is known, and a pipeline which gates on this needs the file which
                ## names what failed more than any other caller does.
                if stop_on_error:
                    stopped = next((result for result in results if result.is_failure), None)
                    if stopped is not None:
                        break
        finally:
            ## whatever ended the loop, the connections this run opened are its own to close
            self.close_worker_connections()

        self.record_protocol()
        self.print_summary(written, header)

        if stopped is not None:
            raise ValueError(
                f"{stopped.statement.location}: {stopped.status} - {stopped.reason}. "
                f"query_conversion.on_error is 'stop'. {os.path.basename(written[0]) if written else 'The output'} "
                f"and the protocol table hold everything which was converted up to here.")

        failures = [result for result in self.results if result.is_failure]
        return failures

    def print_summary(self, written, header):
        """
        The closing summary of the run - what was read, what became of it per file, what has
        to be looked at and where the answer was written.

        It is written as one message, the way the summary of a migration is, so that it
        stands in the log file and on the console in one piece rather than interleaved with
        whatever a worker logs at the same moment.
        """
        summary = render_summary(self.results, {
            'source_db_type': self.source_db_type,
            'source_database': self.config_parser.get_source_db_name(),
            'source_schema': self.source_schema,
            'target_db_type': self.target_db_type,
            'target_database': self.config_parser.get_target_db_name(),
            'target_schema': self.target_schema,
            'notes': header.get('notes', []),
            'written': written,
        })
        self.print_log_message('INFO', '\n' + summary)

    def convert_file(self, path):
        text = self.read_file(path)
        statements = split_statements(
            text, self.config_parser.get_query_conversion_statement_separator(), input_file=path)
        self.print_log_message('INFO', f"query_conversion: {path}: {len(statements)} statement(s).")

        total = len(statements)
        results = [None] * total
        by_hash = {}
        workers = max(1, int(self.config_parser.get_query_conversion_workers()))

        def work(index_statement):
            index, statement = index_statement
            started = datetime.datetime.now()
            try:
                result = self.convert_statement(statement, total)
            except Exception as e:
                result = StatementResult(statement, total)
                result.status = NOT_CONVERTED
                result.reason = f"the conversion of this statement ended with an error: {e}"
                result.task_started = started
                self.print_log_message('ERROR', f"query_conversion: {statement.location}: {e}")
            result.task_completed = datetime.datetime.now()
            return index, result

        ## a statement which stands in the file more than once is converted and tested once
        unique = []
        for index, statement in enumerate(statements):
            first = by_hash.get(statement.sha256)
            if first is None:
                by_hash[statement.sha256] = index
                unique.append((index, statement))

        if workers > 1 and len(unique) > 1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                for index, result in pool.map(work, unique):
                    results[index] = result
        else:
            for index_statement in unique:
                index, result = work(index_statement)
                results[index] = result

        for index, statement in enumerate(statements):
            if results[index] is not None:
                continue
            first_index = by_hash[statement.sha256]
            first = results[first_index]
            repeated = StatementResult(statement, total)
            repeated.status = first.status
            repeated.reason = first.reason
            repeated.converted_sql = first.converted_sql
            repeated.output_sql = first.output_sql
            repeated.warnings = list(first.warnings)
            repeated.source_test = first.source_test
            repeated.target_test = first.target_test
            repeated.target_test_ms = first.target_test_ms
            repeated.parameters_line = first.parameters_line
            repeated.identical_to = first.ordinal
            results[index] = repeated

        self.close_worker_connections()
        return results

    ## ------------------------------------------------------------------ protocol

    def record_protocol(self):
        if self.migrator_tables is None:
            self.print_log_message('INFO', 'query_conversion: no connection to the migrator database - '
                                           'the run is not recorded in a protocol table.')
            return
        try:
            self.migrator_tables.create_table_for_queries()
            for result in self.results:
                row = result.as_dict()
                row['run_started'] = self.run_started
                self.migrator_tables.insert_query(row)
        except Exception as e:
            self.print_log_message('WARNING', f"query_conversion: the run could not be recorded in the "
                                              f"protocol table: {e}")

    def print_log_message(self, level, message):
        self.config_parser.print_log_message(level, message)


def same_statement(source_text, converted_text):
    """
    Whether the conversion changed anything at all. A statement which was already valid
    PostgreSQL is reported as UNCHANGED, so that it is not read as a failure.
    """
    def normalise(text):
        return ' '.join(text.replace(';', ' ').split()).lower()
    return normalise(source_text) == normalise(converted_text)
