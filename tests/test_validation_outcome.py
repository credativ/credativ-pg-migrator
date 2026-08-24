# SPDX-License-Identifier: GPL-3.0-or-later
"""
"We could not tell" is not "it is correct".

P2-2 of development/OPEN_ISSUES.md. The validator started every table at `passed = True` and
the branches which found a mismatch set it to False, so a table where **no branch ran at all**
ended the run indistinguishable from one which passed every check — and the line in the log
said *"passed all active validations"*, which was true and told the reader the opposite of what
had happened. A table has no primary key, so the row sample and the LOB check are skipped; its
source has no checksum support, so the hash is skipped; row counts are switched off — and the
report is green.

There are three outcomes now, derived from the checks which really ran and never accumulated:
FAILED, PASSED, and NOT VALIDATED. The third one is neither of the other two and is counted,
recorded and printed as itself.

The structural counts joined it two days later (P2-3): the number of columns, of indexes and
of constraints was recorded from the first day and **compared by nothing**, so a table which
arrived with half its indexes was reported as validated. They can fail a table now — and,
deliberately, they cannot pass one: the number of columns matching says nothing about whether
the rows arrived, so it must not turn a table nobody looked into a table which passed.

**P2-5** is the fourth: a table was submitted for validation only when the row counts the
*migration* had recorded said it held rows — and the decoded protocol row has no
`source_table_rows` key at all (it has `source_table_rows_all` and `source_table_rows_limited`),
so that half of the condition was always 0 and a standard migration validated a table only when
the migration had already recorded rows in the **target**. A table whose data migration failed
before the first row landed therefore had a target count of 0 and was never looked at: exactly
the table most in need of it.

**P2-4** is the third of them: a table the validator could not measure at all — the connectors
could not be built, the databases could not be reached — was answered with `None`, and `run()`
dropped a falsy result. The table was missing from the protocol table, from the report built
out of it, and from the count at the bottom of that report: what the reader saw was a report of
the tables which happened to work, all green, over a total which did not say how many tables
the validation had really been asked about.

Reading the validator for this turned up two more holes of the same shape, repaired with it:

  * the **row sample** and the **LOB size** checks were written into no column of the protocol
    table, and the summary built its own verdict out of the two columns which were there — so a
    table which failed one of them was shown as `PASS` in the summary while the log said it had
    failed. The verdict of the validator is recorded now and the summary reads it.
  * a table whose validation **crashed** was marked failed through the same `passed` flag; it
    is recorded as failed with the exception in the message, and never as "nothing ran".

Nothing here connects to anything: the connectors, the protocol tables and the log are stubs.

Run with:  python3 -m pytest tests/test_validation_outcome.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.constants import MigratorConstants
from credativ_pg_migrator.validator import Validator, outcome_of, checks_which_ran, why_nothing_ran

PASSED = MigratorConstants.VALIDATION_PASSED
FAILED = MigratorConstants.VALIDATION_FAILED
NOT_VALIDATED = MigratorConstants.VALIDATION_NOT_VALIDATED


# --------------------------------------------------------------------------------------
# the rule itself


def result(**verdicts):
    base = {'row_logic': None, 'table_hash_logic': None, 'row_hash_logic': None,
            'lob_size_logic': None, 'error': ''}
    base.update(verdicts)
    return base


def test_a_check_which_said_no_fails_the_table():
    assert outcome_of(result(row_logic=False)) == FAILED
    assert outcome_of(result(row_logic=True, table_hash_logic=False)) == FAILED
    assert outcome_of(result(row_hash_logic=False)) == FAILED
    assert outcome_of(result(lob_size_logic=False)) == FAILED


def test_a_table_passes_when_every_check_which_ran_said_yes():
    assert outcome_of(result(row_logic=True)) == PASSED
    assert outcome_of(result(row_logic=True, table_hash_logic=True)) == PASSED
    ## one check which could not run does not spoil the others
    assert outcome_of(result(row_logic=True, row_hash_logic=None)) == PASSED


def test_a_table_no_check_could_run_against_is_not_a_table_which_passed():
    """The whole of P2-2 in one line."""
    assert outcome_of(result()) == NOT_VALIDATED
    assert outcome_of(result()) != PASSED


def test_a_crash_is_a_failure_and_not_an_absence():
    assert outcome_of(result(error='connection reset')) == FAILED
    ## even when a check had already passed before it
    assert outcome_of(result(row_logic=True, error='boom')) == FAILED


def test_the_outcome_is_derived_and_never_accumulated():
    """
    The defect was the starting value: `passed = True` before anything was measured. Whatever
    order the checks are written in, a result with no verdict in it cannot come out as passed.
    """
    for key in ('row_logic', 'table_hash_logic', 'row_hash_logic', 'lob_size_logic'):
        assert outcome_of(result(**{key: None})) == NOT_VALIDATED


def test_the_checks_which_ran_are_named_in_the_order_they_run():
    res = result(row_logic=True, row_hash_logic=False)
    assert checks_which_ran(res) == ['row counts', 'row sample']
    assert checks_which_ran(result()) == []


def test_a_table_which_could_not_be_measured_says_why_per_check():
    res = result()
    res['row_msg'] = ''
    res['table_msg'] = 'Skip: Table checksum unavailable (Src=None, Tgt=None)'
    res['row_hash_msg'] = 'Skip: No PKs available'
    res['lob_size_msg'] = 'Skip: No PKs available'
    reasons = why_nothing_ran(res, {'row counts': False, 'table checksum': True,
                                    'row sample': True, 'LOB sizes': True})
    assert reasons[0] == 'row counts: switched off in the configuration'
    assert 'Table checksum unavailable' in reasons[1]
    assert reasons[2] == 'row sample: Skip: No PKs available'
    assert len(reasons) == 4


# --------------------------------------------------------------------------------------
# the validator over stubbed connectors


class Log:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(('INFO', str(message)))

    def warning(self, message):
        self.messages.append(('WARNING', str(message)))

    def error(self, message):
        self.messages.append(('ERROR', str(message)))

    def levels(self, level):
        return [message for written, message in self.messages if written == level]

    def written(self):
        return ' | '.join(message for _, message in self.messages)


class Connector:
    """A source or a target which answers exactly what a test wants it to answer."""

    def __init__(self, rows=0, checksum=None, samples=(), row_hashes=None, lob_sizes=None,
                 indexes=None, constraints=None):
        self.indexes = indexes
        self.constraints = constraints
        self.rows = rows
        self.checksum = checksum
        self.samples = list(samples)
        self.row_hashes = row_hashes or {}
        self.lob_sizes = lob_sizes or {}

    def get_rows_count(self, schema, table, limitation=None):
        return self.rows

    def get_table_checksum(self, schema, table, columns):
        return self.checksum

    def get_random_pks(self, schema, table, pk_columns, sample_size):
        return self.samples

    def get_row_checksums(self, schema, table, pk_columns, pk_values, columns):
        return dict(self.row_hashes)

    def get_lob_sizes(self, schema, table, pk_columns, pk_values, lob_columns):
        return dict(self.lob_sizes)

    def get_indexes_count(self, schema, table):
        return self.indexes

    def get_constraints_count(self, schema, table):
        return self.constraints

    def get_column_statistics(self, schema, table, column, data_type, force_round_0=False):
        return {}

    def fetch_indexes(self, settings):
        return {}

    def fetch_constraints(self, settings):
        return {}


class ProtocolTables:
    """The protocol tables, as far as one table's validation touches them."""

    def __init__(self, primary_key=''):
        self.primary_key = primary_key
        self.table_results = []

    def select_primary_key(self, settings):
        return self.primary_key

    def resolve_data_migration_limitation(self, settings):
        return None

    def insert_validation_table_result(self, settings):
        self.table_results.append(dict(settings))

    def insert_validation_column_result(self, settings):
        pass

    def insert_validation_index_result(self, settings):
        pass

    def insert_validation_constraint_result(self, settings):
        pass


class Config:
    def __init__(self, migrate_indexes=True, migrate_constraints=True, migrate_data=True):
        self.migrate_indexes = migrate_indexes
        self.migrate_constraints = migrate_constraints
        self.migrate_data = migrate_data

    def get_workflow(self):
        return 'migration'

    def get_validation_target_copy_config(self):
        return {}

    def get_mapping_data_resolution(self, table):
        return None

    def should_migrate_data(self, table_name=None):
        return self.migrate_data

    def should_migrate_indexes(self, table_name=None):
        return self.migrate_indexes

    def should_migrate_constraints(self, table_name=None):
        return self.migrate_constraints


def validate(source, target, primary_key='', checks=(True, True, True, True), columns=1,
             target_columns=None, config=None):
    """One table through the real _validate_table_inner(), with everything else stubbed."""
    made = Validator.__new__(Validator)
    made.config_parser = config or Config()
    made.val_logger = type('L', (), {'logger': Log()})()
    made.migrator_tables = ProtocolTables(primary_key)

    table_info = {
        'source_schema_name': 'src', 'source_table_name': 'customers',
        'target_schema_name': 'tgt', 'target_table_name': 'customers',
        'source_columns': [{'column_name': f'c{i}', 'data_type': 'varchar'} for i in range(columns)],
        'target_columns': [{'column_name': f'c{i}', 'data_type': 'varchar'}
                           for i in range(columns if target_columns is None else target_columns)],
    }
    res = made._validate_table_inner(source, target, None, table_info, *checks, 10)
    return res, made.val_logger.logger, made.migrator_tables


def test_a_table_which_agrees_on_every_check_passes():
    source = Connector(rows=10, checksum='abc')
    target = Connector(rows=10, checksum='abc')
    res, log, tables = validate(source, target)
    assert res['outcome'] == PASSED
    assert res['checks_run'] == ['row counts', 'table checksum', 'column counts']
    assert log.levels('INFO')[-1].startswith('PASSED:')


def test_a_row_count_mismatch_fails_the_table():
    res, log, tables = validate(Connector(rows=10, checksum='a'), Connector(rows=9, checksum='a'))
    assert res['outcome'] == FAILED
    assert 'FAILED:' in log.written()


def test_a_table_with_no_check_available_is_not_validated():
    """
    No primary key, so no row sample and no LOB check; no checksum on this source, so no hash;
    row counts switched off. Every one of these happens in a real migration, and together they
    used to produce `OK: ... passed all active validations`.
    """
    source = Connector(rows=10, checksum=None)
    target = Connector(rows=10, checksum=None)
    res, log, tables = validate(source, target, primary_key='',
                                checks=(False, True, True, True))
    assert res['outcome'] == NOT_VALIDATED
    ## the column count was compared - and a structural check cannot pass a table, because it
    ## says nothing about whether the rows arrived
    assert res['checks_run'] == ['column counts']
    written = log.levels('WARNING')[-1]
    assert written.startswith('NOT VALIDATED:')
    assert 'says NOTHING' in written
    assert 'not a table which passed' in written
    assert 'switched off in the configuration' in written
    assert 'No PKs available' in written


def test_every_check_switched_off_is_not_a_pass_either():
    res, log, tables = validate(Connector(rows=5), Connector(rows=5),
                                primary_key='id', checks=(False, False, False, False))
    assert res['outcome'] == NOT_VALIDATED
    assert 'NOT VALIDATED' in log.written()


def test_a_table_whose_checksum_is_unavailable_still_passes_on_its_row_count():
    """One check is enough to have measured something - the outcome says which ones ran."""
    res, log, tables = validate(Connector(rows=7, checksum=None), Connector(rows=7, checksum=None),
                                checks=(True, True, False, False))
    assert res['outcome'] == PASSED
    assert res['checks_run'] == ['row counts', 'column counts']
    assert 'Skip: Table checksum unavailable' in res['table_msg']


def test_a_failed_row_sample_fails_the_table_and_is_recorded():
    """
    It could fail a table in the log and was written into no column of the protocol table, so
    the summary - which built its verdict out of the columns - showed the table as PASS.
    """
    source = Connector(rows=2, checksum='a', row_hashes={1: 'x', 2: 'y'})
    target = Connector(rows=2, checksum='a', samples=[{'id': 1}, {'id': 2}],
                       row_hashes={1: 'x', 2: 'CHANGED'})
    res, log, tables = validate(source, target, primary_key='id', checks=(True, True, True, False))
    assert res['outcome'] == FAILED
    assert res['row_hash_logic'] is False
    recorded = tables.table_results[0]
    assert recorded['outcome'] == FAILED
    assert recorded['row_hash_logic'] is False


def test_the_outcome_and_the_reason_are_written_into_the_protocol_row():
    res, log, tables = validate(Connector(rows=1, checksum=None), Connector(rows=1, checksum=None),
                                checks=(False, True, True, True))
    recorded = tables.table_results[0]
    assert recorded['outcome'] == NOT_VALIDATED
    assert 'switched off' in recorded['validation_message']


def test_a_crash_inside_the_validation_is_recorded_as_a_failure():
    class Exploding(Connector):
        def get_rows_count(self, schema, table, limitation=None):
            raise RuntimeError('connection reset by peer')

    res, log, tables = validate(Exploding(), Connector(rows=1))
    assert res['outcome'] == FAILED
    assert 'connection reset by peer' in res['error']
    assert 'validation crashed' in res['validation_message']


def test_the_sentence_this_repair_is_about_is_not_reachable_any_more():
    """
    `OK: ... passed all active validations` was written for a table against which not one
    validation had been run. No line of a run which measured nothing may claim a pass.
    """
    res, log, tables = validate(Connector(rows=3, checksum=None), Connector(rows=3, checksum=None),
                                checks=(False, False, False, False))
    assert res['outcome'] == NOT_VALIDATED
    assert 'passed all active validations' not in log.written()
    assert all(not message.startswith('PASSED:') for message in log.levels('INFO'))
    ## it says the opposite, in as many words
    assert 'It is not a table which passed' in log.levels('WARNING')[-1]


def test_the_source_of_the_validator_no_longer_starts_a_table_at_passed():
    """
    `'passed': True` in the result dictionary is the defect itself. A verdict which is
    accumulated from a hopeful starting value is one which nothing has to confirm.
    """
    import ast

    path = os.path.join(REPO, 'credativ_pg_migrator', 'validator.py')
    with open(path, encoding='utf-8') as handle:
        tree = ast.parse(handle.read(), filename=path)

    offenders = []
    for node in ast.walk(tree):
        ## the result dictionary of a table - the column, index and constraint results have a
        ## 'passed' of their own, and theirs is a real verdict about one object
        if not isinstance(node, ast.Dict):
            continue
        keys = [key.value for key in node.keys if isinstance(key, ast.Constant)]
        if 'passed' in keys and 'row_logic' in keys:
            offenders.append(node.lineno)
    assert not offenders, (
        f'the table result carries a "passed" flag again, at line(s) {offenders} - the outcome '
        f'is derived by outcome_of() and must not be accumulated')


# --------------------------------------------------------------------------------------
# every table in scope is validated - P2-5


def test_the_key_the_old_filter_read_does_not_exist():
    """
    The filter was `t.get('target_table_rows', 0) > 0 or t.get('source_table_rows', 0) > 0`,
    and the decoded protocol row has no `source_table_rows`. Half of the condition was always
    0, so a standard migration validated a table only when the migration had already recorded
    rows in the target for it.
    """
    import inspect
    import re

    from credativ_pg_migrator.migrator_tables import MigratorTables

    keys = set(re.findall(r"'(\w*table_rows\w*)'", inspect.getsource(MigratorTables.decode_table_row)))
    assert 'source_table_rows_all' in keys
    assert 'source_table_rows_limited' in keys
    assert 'source_table_rows' not in keys


def test_the_run_no_longer_asks_the_recorded_counts_which_tables_to_look_at():
    """
    The row counts of the migration decide nothing about which tables are validated. A table
    whose data migration failed before the first row landed has a target count of 0, and that
    is the table which most needs looking at.
    """
    import ast

    path = os.path.join(REPO, 'credativ_pg_migrator', 'validator.py')
    with open(path, encoding='utf-8') as handle:
        tree = ast.parse(handle.read(), filename=path)
    run = next(node for node in ast.walk(tree)
               if isinstance(node, ast.FunctionDef) and node.name == 'run')
    submits = [node for node in ast.walk(run)
               if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
               and node.func.attr == 'submit']
    assert submits, 'the workers are not submitted here any more - check this test'
    for submit in submits:
        for parent in ast.walk(run):
            if isinstance(parent, ast.If) and submit in list(ast.walk(parent)):
                condition = ast.dump(parent.test)
                assert 'table_rows' not in condition, (
                    'a table is submitted for validation only under a condition on the '
                    'recorded row counts again')


def test_an_empty_table_is_validated_and_passes():
    """
    An empty source and an empty target is a legitimate PASS - reached by looking, not by
    skipping. It used to be one of the tables which never reached the report at all.
    """
    res, log, tables = validate(Connector(rows=0, checksum='e'), Connector(rows=0, checksum='e'))
    assert res['outcome'] == PASSED
    assert res['source_row_count'] == 0
    assert res['target_row_count'] == 0
    assert 'row counts' in res['checks_run']


def test_a_table_which_should_have_had_rows_and_got_none_fails():
    """The other half of the same rule, and the reason the filter was worth removing."""
    res, log, tables = validate(Connector(rows=5000, checksum='a'), Connector(rows=0, checksum='b'))
    assert res['outcome'] == FAILED
    assert res['row_logic'] is False
    assert 'Src=5000, Tgt=0' in res['row_msg']


def test_a_row_count_which_cannot_be_read_is_not_a_count_of_zero():
    res, log, tables = validate(Connector(rows=None), Connector(rows=0),
                                checks=(True, False, False, False))
    assert res['row_logic'] is None
    assert 'not available on both sides' in res['row_msg']
    assert res['outcome'] == NOT_VALIDATED, 'nothing was measured, and nothing is not a pass'


def test_a_table_whose_data_was_not_migrated_is_not_reported_as_a_mismatch():
    """
    With migrate_data off the target holds none of the rows on purpose. Comparing them anyway
    reports every such table as a mismatch - which is what removing the filter would otherwise
    have produced for a whole run.
    """
    res, log, tables = validate(Connector(rows=5000, checksum='a'), Connector(rows=0, checksum='b'),
                                config=Config(migrate_data=False))
    assert res['row_logic'] is None
    assert res['table_hash_logic'] is None
    assert res['outcome'] == NOT_VALIDATED
    assert 'migrate_data is off for it' in res['row_msg']


def test_such_a_table_says_the_data_was_not_migrated_and_not_that_the_check_was_switched_off():
    """
    Two different reasons which must not be told as one: the check is on and there is nothing
    to compare it against.
    """
    res, log, tables = validate(Connector(rows=5000), Connector(rows=0),
                                config=Config(migrate_data=False))
    written = log.levels('WARNING')[-1]
    assert 'migrate_data is off for it' in written
    assert 'switched off in the configuration' not in written


def test_the_structure_of_such_a_table_is_still_compared():
    """It was created, and it should look like the source even when it holds none of its rows."""
    source = Connector(rows=5000, indexes=4)
    target = Connector(rows=0, indexes=1)
    res, log, tables = validate(source, target, config=Config(migrate_data=False))
    assert res['indexes_logic'] is False
    assert res['outcome'] == FAILED


# --------------------------------------------------------------------------------------
# a table which could not be validated at all - P2-4


class VLogger:
    """The logger holder the validator keeps, with the stop_logging() run() calls at the end."""

    def __init__(self):
        self.logger = Log()
        self.stopped = 0

    def stop_logging(self):
        self.stopped += 1


class RunConfig(Config):
    """The configuration run() reads before it starts the workers."""

    def get_log_file(self):
        return None

    def get_validation_report_filename(self):
        return 'report.md'

    def get_validation_workers(self):
        return 1

    def is_validation_row_counts_enabled(self):
        return True

    def is_validation_table_checksums_enabled(self):
        return False

    def is_validation_random_sample_enabled(self):
        return False

    def is_validation_lob_sizes_enabled(self):
        return False

    def get_validation_sample_size(self):
        return 10


class RunProtocolTables(ProtocolTables):
    def __init__(self, tables):
        super().__init__()
        self.tables = tables
        self.summaries = 0
        self.created = 0

    def create_table_for_validation(self):
        self.created += 1

    def fetch_all_tables(self, only_unfinished=False):
        return list(self.tables)

    def decode_table_row(self, row):
        return row

    def print_validation_summary(self, val_logger=None):
        self.summaries += 1


def table_info(name='customers', rows=10):
    return {'source_schema_name': 'src', 'source_table_name': name,
            'target_schema_name': 'tgt', 'target_table_name': name,
            'source_table_rows': rows, 'target_table_rows': rows,
            'source_columns': [], 'target_columns': []}


def validator_for(tables, connector_factory):
    made = Validator.__new__(Validator)
    made.config_parser = RunConfig()
    made.val_logger = VLogger()
    made.migrator_tables = RunProtocolTables(tables)
    made._get_connector = connector_factory
    return made


def test_a_table_whose_connectors_cannot_be_built_still_gets_a_row():
    """
    `_get_connector()` was called outside every try of validate_table(), so the exception
    travelled up into run(), which logged it without naming the table and wrote nothing
    anywhere: the table was simply not in the report.
    """
    def explode(direction):
        raise ValueError('Unsupported database type: nosuchdb')

    made = validator_for([table_info()], explode)
    res = made.validate_table(table_info(), True, False, False, False, 10)
    assert res is not None
    assert res['outcome'] == FAILED
    assert made.migrator_tables.table_results[0]['target_table_name'] == 'customers'
    assert 'connectors for the validation could not be built' in res['validation_message']


def test_a_table_whose_databases_cannot_be_reached_still_gets_a_row():
    class Unreachable(Connector):
        def connect(self):
            raise OSError('could not connect to server: Connection refused')

    made = validator_for([table_info()], lambda direction: Unreachable())
    res = made.validate_table(table_info(), True, False, False, False, 10)
    assert res['outcome'] == FAILED
    assert 'Connection refused' in res['validation_message']
    assert made.migrator_tables.table_results[0]['outcome'] == FAILED


def test_the_row_says_that_the_validation_failed_and_not_the_table():
    """
    A red row which reads as "this table is broken" when what broke was the validation of it
    is the other way to mislead the reader.
    """
    made = validator_for([table_info()], lambda direction: (_ for _ in ()).throw(OSError('down')))
    res = made.validate_table(table_info(), True, False, False, False, 10)
    assert 'failure of the VALIDATION and not a measurement of the table' in res['validation_message']
    assert 'nothing about it was compared' in res['validation_message']


def test_such_a_table_is_failed_and_not_merely_not_validated():
    """
    NOT VALIDATED means the checks do not apply - no primary key, no checksum on that source -
    which is an ordinary state of an ordinary migration. An exception is not an ordinary state
    and must not be filed with them.
    """
    made = validator_for([table_info()], lambda direction: (_ for _ in ()).throw(OSError('down')))
    res = made.validate_table(table_info(), True, False, False, False, 10)
    assert res['outcome'] == FAILED
    assert res['outcome'] != NOT_VALIDATED


def test_every_table_the_run_was_asked_about_is_in_the_report():
    """
    The whole of P2-4: one table which cannot be reached must not take itself out of the
    report, and must not take the count of the others with it.
    """
    class Reachable(Connector):
        def connect(self):
            pass

        def disconnect(self):
            pass

    made = validator_for([table_info('customers'), table_info('orders')],
                         lambda direction: Reachable(rows=10))

    real_validate = made.validate_table

    def validate_or_explode(info, *arguments):
        if info['source_table_name'] == 'orders':
            raise RuntimeError('the worker died')
        return real_validate(info, *arguments)

    made.validate_table = validate_or_explode
    made.run()

    recorded = {row['target_table_name']: row['outcome']
                for row in made.migrator_tables.table_results}
    assert set(recorded) == {'customers', 'orders'}, 'both tables belong in the report'
    assert recorded['orders'] == FAILED
    assert 'orders' in made.val_logger.logger.written(), 'the failing table is named'
    assert made.migrator_tables.summaries == 1


def test_a_worker_which_answers_nothing_at_all_is_still_recorded():
    made = validator_for([table_info()], lambda direction: Connector())
    made.validate_table = lambda info, *arguments: None
    made.run()
    assert len(made.migrator_tables.table_results) == 1
    assert made.migrator_tables.table_results[0]['outcome'] == FAILED
    assert 'ended without a result' in made.migrator_tables.table_results[0]['validation_message']


def test_a_table_which_could_not_even_be_recorded_says_so():
    """The last place which could have recorded it - the log has to say the report is short."""
    made = validator_for([table_info()], lambda direction: Connector())

    def refuse(settings):
        raise RuntimeError('protocol database is gone')

    made.migrator_tables.insert_validation_table_result = refuse
    made.could_not_be_validated(table_info(), OSError('down'), 'the databases could not be reached')
    assert 'MISSING from the validation report' in made.val_logger.logger.written()


# --------------------------------------------------------------------------------------
# the structural checks - P2-3


from credativ_pg_migrator.validator import compare_counts, count_is_available


@pytest.mark.parametrize('count,available', [
    (0, True), (1, True), (99, True),
    (None, False),   ## the connector does not count that
    (-1, False),     ## it tried and the query failed
    (True, False),   ## a bool is not a count
])
def test_only_a_real_number_is_a_count(count, available):
    assert count_is_available(count) is available


def test_the_columns_are_compared_exactly():
    """A migrated table holds the columns of the source: one fewer is data which did not arrive."""
    assert compare_counts(5, 5, 'columns', exact=True)[0] is True
    assert compare_counts(5, 4, 'columns', exact=True)[0] is False
    assert compare_counts(5, 6, 'columns', exact=True)[0] is False


def test_the_indexes_and_constraints_are_compared_for_a_shortfall():
    """
    The two sides do not count the same things and were never going to: PostgreSQL creates an
    index for every primary key and every unique constraint, the migration adds one to the
    parent of a foreign key which has none, and the SQLite connector counts neither the
    primary key nor a unique constraint as a constraint at all. Comparing those for equality
    reports a table which arrived complete as broken.
    """
    assert compare_counts(6, 7, 'indexes', exact=False)[0] is True, 'more is normal'
    assert compare_counts(6, 6, 'indexes', exact=False)[0] is True
    assert compare_counts(6, 3, 'indexes', exact=False)[0] is False, 'fewer is a loss'


def test_a_shortfall_says_how_many_and_where_to_read_which():
    verdict, message = compare_counts(6, 3, 'indexes', exact=False)
    assert verdict is False
    assert '3 of the indexes' in message
    assert 'protocol table' in message


def test_a_count_which_is_not_available_on_both_sides_is_not_a_check():
    for source, target in ((None, 3), (3, None), (-1, 3), (3, -1), (None, None)):
        verdict, message = compare_counts(source, target, 'indexes', exact=False)
        assert verdict is None
        assert message.startswith('Skip:')


def test_a_table_which_lost_indexes_is_not_reported_as_validated():
    """
    P2-3 in one line: the counts were recorded and compared by nothing, so `passed` was set to
    False only by the row count, the checksum and the samples - and a table which arrived with
    half its indexes came out validated.
    """
    source = Connector(rows=10, checksum='a', indexes=6, constraints=2)
    target = Connector(rows=10, checksum='a', indexes=3, constraints=2)
    res, log, tables = validate(source, target)
    assert res['indexes_logic'] is False
    assert res['outcome'] == FAILED, 'every data check agreed - the structure did not'
    assert 'index counts' in log.written()


def test_a_target_with_more_indexes_than_the_source_still_passes():
    source = Connector(rows=10, checksum='a', indexes=6, constraints=2)
    target = Connector(rows=10, checksum='a', indexes=8, constraints=5)
    res, log, tables = validate(source, target)
    assert res['outcome'] == PASSED
    assert res['indexes_logic'] is True
    assert res['constraints_logic'] is True


def test_a_missing_column_fails_the_table():
    source = Connector(rows=10, checksum='a')
    target = Connector(rows=10, checksum='a')
    res, log, tables = validate(source, target, columns=5, target_columns=4)
    assert res['columns_logic'] is False
    assert res['outcome'] == FAILED


def test_a_structural_check_alone_cannot_pass_a_table():
    """
    The asymmetry which keeps P2-3 from undoing P2-2. The columns match, and that says nothing
    about whether the rows arrived - so the table is still NOT VALIDATED and the line says
    which of the two happened.
    """
    source = Connector(rows=1, checksum=None, indexes=3)
    target = Connector(rows=1, checksum=None, indexes=3)
    res, log, tables = validate(source, target, checks=(False, False, False, False))
    assert res['columns_logic'] is True
    assert res['indexes_logic'] is True
    assert res['outcome'] == NOT_VALIDATED
    written = log.levels('WARNING')[-1]
    assert 'not one check of the DATA' in written
    assert 'the structure was compared' in written
    assert 'nothing looked at the data' in written


def test_a_structural_failure_fails_a_table_nothing_else_could_measure():
    """The other half of the asymmetry: it cannot pass a table and it can certainly fail one."""
    source = Connector(rows=1, checksum=None, indexes=9)
    target = Connector(rows=1, checksum=None, indexes=1)
    res, log, tables = validate(source, target, checks=(False, False, False, False))
    assert res['outcome'] == FAILED


def test_objects_the_configuration_did_not_ask_for_are_not_missed():
    """
    With migrate_indexes off the target has none of them on purpose, and a check which fails a
    table for doing what it was told is a check nobody will keep.
    """
    source = Connector(rows=10, checksum='a', indexes=6, constraints=4)
    target = Connector(rows=10, checksum='a', indexes=0, constraints=0)
    res, log, tables = validate(source, target,
                                config=Config(migrate_indexes=False, migrate_constraints=False))
    assert res['indexes_logic'] is None
    assert res['constraints_logic'] is None
    assert res['outcome'] == PASSED
    assert 'not migrated for this table' in res['indexes_msg']


def test_the_structural_verdicts_are_written_into_the_protocol_row():
    source = Connector(rows=10, checksum='a', indexes=6, constraints=2)
    target = Connector(rows=10, checksum='a', indexes=3, constraints=2)
    res, log, tables = validate(source, target)
    recorded = tables.table_results[0]
    assert recorded['indexes_logic'] is False
    assert recorded['columns_logic'] is True
    assert recorded['source_indexes_count'] == 6
    assert recorded['target_indexes_count'] == 3


def test_oracle_does_not_count_the_index_of_a_lob_column():
    """
    Oracle keeps one index per LOB column in all_indexes and PostgreSQL keeps the value out of
    line without an index of any kind, so counting it made every table with a CLOB look as
    though an index had been lost.
    """
    path = os.path.join(REPO, 'credativ_pg_migrator', 'connectors', 'oracle_connector.py')
    with open(path, encoding='utf-8') as handle:
        source = handle.read()
    body = source.split('def get_indexes_count')[1].split('def ')[0]
    assert "index_type <> 'LOB'" in body


# --------------------------------------------------------------------------------------
# the summary, which is where the verdict is read


class Cursor:
    """A cursor which answers each query of the summary with a prepared result set."""

    def __init__(self, result_sets):
        self.result_sets = list(result_sets)
        self.queries = []

    def execute(self, query, params=None):
        self.queries.append(query)

    def fetchall(self):
        return self.result_sets.pop(0) if self.result_sets else []

    def close(self):
        pass


class SummaryConfig:
    def __init__(self, report_filename):
        self.report_filename = report_filename

    def get_protocol_name_tables(self):
        return 'tables'

    def get_validation_tables_name(self):
        return 'validation_tables'

    def get_validation_columns_name(self):
        return 'validation_columns'

    def get_validation_indexes_name(self):
        return 'validation_indexes'

    def get_validation_constraints_name(self):
        return 'validation_constraints'

    def get_source_db_name(self):
        return 'srcdb'

    def get_source_owner(self):
        return 'src'

    def get_source_db_type(self):
        return 'oracle'

    def get_target_db_name(self):
        return 'tgtdb'

    def get_target_schema(self):
        return 'tgt'

    def get_target_db_type(self):
        return 'postgresql'

    def get_workflow(self):
        return 'migration'

    def get_validation_report_filename(self):
        return self.report_filename

    def get_mapping_data_resolution(self, table):
        return None

    def print_log_message(self, level, message):
        pass


def validation_row(table, row_cnt='PASS', tbl_hash='PASS', row_hash='-', lob='-', outcome=PASSED,
                   cols='PASS', idxs='-', cons='-'):
    ## the columns of the summary query, in its order: the counts, then the mark of every
    ## check, then the outcome, then the three structural marks
    return ('tgt', table, 'src', table, 10, 10, 'h', 'h', 3, 3, None, None, None, None,
            row_cnt, tbl_hash, row_hash, lob, outcome, cols, idxs, cons)


def summary_of(rows, tmp_path):
    from credativ_pg_migrator.migrator_tables import MigratorTables

    report = str(tmp_path / 'validation_report.md')
    made = MigratorTables.__new__(MigratorTables)
    made.protocol_schema = 'migration'
    made.config_parser = SummaryConfig(report)
    cursor = Cursor([rows, [], [], []])
    made.protocol_connection = type('C', (), {
        'connection': type('X', (), {'cursor': staticmethod(lambda: cursor)})()})()

    log = Log()
    made.print_validation_summary(val_logger=log)
    with open(report, encoding='utf-8') as handle:
        details = handle.read()
    return log.written(), details


def test_the_summary_marks_a_table_nobody_could_measure_with_a_question_mark(tmp_path):
    """
    It used to show `PASS`: the summary worked the verdict out again, out of the two columns
    which happened to be recorded, and a table with `-` in both counted as a pass.
    """
    rows = [validation_row('customers'),
            validation_row('orders', row_cnt='-', tbl_hash='-', outcome=NOT_VALIDATED)]
    summary, details = summary_of(rows, tmp_path)
    lines = [line for line in details.splitlines() if 'tgt.orders' in line]
    assert lines and '?' in lines[0]
    assert 'PASS' not in lines[0].split('|')[4]


def test_the_summary_counts_the_not_validated_tables_on_their_own(tmp_path):
    rows = [validation_row('a'),
            validation_row('b', outcome=FAILED, row_cnt='X'),
            validation_row('c', row_cnt='-', tbl_hash='-', outcome=NOT_VALIDATED)]
    summary, details = summary_of(rows, tmp_path)
    totals = [line for line in summary.splitlines() if 'All Evaluated Tables' in line][0]
    numbers = [part.strip() for part in totals.split('|') if part.strip()]
    assert numbers[1:] == ['3', '1', '1', '1'], totals
    assert 'Not measured' in summary
    assert '1 of 3 table(s) could not be measured at all' in summary


def test_a_skipped_check_is_not_counted_as_a_check_which_passed(tmp_path):
    """
    The same conflation one level down: a SKIP - the check ran and could not decide - used to
    be added to the Passed column of its own tally, so "Table Hashes: 4 of 4 passed" counted a
    table whose hash could not be computed at all.
    """
    rows = [validation_row('a'),
            validation_row('b', tbl_hash='SKIP'),
            validation_row('c', tbl_hash='X', outcome=FAILED)]
    summary, details = summary_of(rows, tmp_path)
    hashes = [line for line in summary.splitlines() if 'Table Hashes' in line][0]
    numbers = [part.strip() for part in hashes.split('|') if part.strip()]
    assert numbers[1:] == ['2', '1', '1', '1'], hashes


def test_the_summary_says_nothing_extra_when_every_table_was_measured(tmp_path):
    summary, details = summary_of([validation_row('a'), validation_row('b')], tmp_path)
    assert 'could not be measured at all' not in summary


def test_the_summary_no_longer_compares_the_counts_for_equality(tmp_path):
    """
    It worked the structural verdict out again from the two numbers, by comparing them for
    equality - so a table whose target holds more indexes than the source, which is what
    PostgreSQL and the migration between them produce for almost every table with a primary
    key, was marked X. The verdict the validator recorded is read now.
    """
    row = list(validation_row('customers', idxs='PASS'))
    row[10], row[11] = 6, 8          ## the target holds two indexes more
    summary, details = summary_of([tuple(row)], tmp_path)
    line = [line for line in details.splitlines() if 'tgt.customers' in line][0]
    assert '6/8 PASS' in line
    assert line.split('|')[4].strip() == 'PASS'


def test_the_summary_shows_what_the_comparison_of_the_counts_said(tmp_path):
    """`5/6` alone does not say whether that is a shortfall or the target holding more."""
    row = list(validation_row('orders', idxs='X', outcome=FAILED))
    row[10], row[11] = 6, 5
    summary, details = summary_of([tuple(row)], tmp_path)
    line = [line for line in details.splitlines() if 'tgt.orders' in line][0]
    assert '6/5 X' in line


def test_a_table_which_failed_the_row_sample_is_no_longer_shown_as_passed(tmp_path):
    """
    The row sample and the LOB check were recorded in no column, so the summary could not see
    them: it read `PASS` in the two columns it had and showed the table as passed while the
    log said it had failed.
    """
    rows = [validation_row('orders', row_hash='X', outcome=FAILED)]
    summary, details = summary_of(rows, tmp_path)
    line = [line for line in details.splitlines() if 'tgt.orders' in line][0]
    assert 'X' in line.split('|')[4]
    assert 'RowHash' in details


def test_a_row_written_by_an_older_run_is_still_read(tmp_path):
    """A protocol table filled before the outcome existed has None there - and no traceback."""
    rows = [validation_row('a', outcome=None)]
    summary, details = summary_of(rows, tmp_path)
    assert 'tgt.a' in details
