# SPDX-License-Identifier: GPL-3.0-or-later
"""
The two ends of the query conversion: what is sent to the target when a converted statement
is tested, and what is written into the file the developer receives.

The first of these is the last of the four layers which keep this step from writing: even if
every gate were defeated, the transaction the probe runs in is read only and is rolled back.
That is asserted here statement by statement.

Nothing in this file talks to a database.

Run with:  python3 -m pytest tests/test_query_conversion_workflow.py -v
"""

import json
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.query_conversion.splitter import Statement
from credativ_pg_migrator.query_conversion.workflow import probe_statements, same_statement
from credativ_pg_migrator.query_conversion.writer import (
    CONVERTED, CONVERTED_FAILING, NOT_CONVERTED, SKIPPED, UNCHANGED,
    OutputWriter, StatementResult, render_summary)


def probe(sql='SELECT 1', **settings):
    base = {'target_test': 'explain', 'timeout': '30s', 'target_schema': 'migtest',
            'has_parameters': False}
    base.update(settings)
    return probe_statements(sql, base)


# --------------------------------------------------------------------------------------
# what the target is asked, and under which promises


def test_the_probe_runs_in_a_transaction_and_rolls_it_back():
    statements = probe()
    assert statements[0] == 'BEGIN;'
    assert statements[-1] == 'ROLLBACK;'


def test_the_probe_transaction_is_read_only():
    """
    The fourth layer. The gates decide what may be sent; this decides what could happen if
    they were all wrong at once.
    """
    assert 'SET LOCAL transaction_read_only = on;' in probe()


def test_the_probe_is_bounded_in_time():
    assert "SET LOCAL statement_timeout = '30s';" in probe()
    assert "SET LOCAL statement_timeout = '2min';" in probe(timeout='2min')


def test_the_probe_looks_at_the_schema_of_the_migration():
    assert 'SET LOCAL search_path TO "migtest";' in probe()


@pytest.mark.parametrize('level,sent', [
    ('parse', 'PREPARE'),
    ('explain', 'EXPLAIN'),
])
def test_each_level_sends_what_it_promises(level, sent):
    assert any(statement.startswith(sent) for statement in probe(target_test=level))


def test_a_prepared_statement_is_deallocated_again():
    statements = probe(target_test='parse')
    prepared = [statement for statement in statements if statement.startswith('PREPARE')]
    deallocated = [statement for statement in statements if statement.startswith('DEALLOCATE')]
    assert len(prepared) == len(deallocated) == 1


def test_a_statement_with_parameters_is_always_prepared():
    """EXPLAIN of a statement with bind parameters is refused by PostgreSQL as well."""
    statements = probe('SELECT a FROM t WHERE b = $1', target_test='explain', has_parameters=True)
    assert any(statement.startswith('PREPARE') for statement in statements)
    assert not any(statement.startswith('EXPLAIN') for statement in statements)


def test_nothing_but_the_probe_is_sent():
    """No statement of the probe writes, creates or reads data."""
    statements = probe(target_test='explain')
    forbidden = ('INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'COMMIT', 'TRUNCATE')
    for statement in statements:
        assert not any(statement.upper().startswith(word) for word in forbidden), statement


def test_the_trailing_semicolon_of_the_statement_does_not_end_the_probe():
    statements = probe('SELECT 1;', target_test='explain')
    assert 'EXPLAIN SELECT 1;' in statements


def test_an_unknown_level_is_refused():
    with pytest.raises(ValueError):
        probe(target_test='benchmark')


# --------------------------------------------------------------------------------------
# a statement which did not have to change


@pytest.mark.parametrize('source,converted,expected', [
    ('SELECT a FROM t', 'SELECT a FROM t', True),
    ('SELECT  a\n FROM t', 'select a from t;', True),
    ('SELECT a FROM t', 'SELECT "a" FROM "s"."t"', False),
])
def test_a_statement_which_did_not_change_is_recognised(source, converted, expected):
    assert same_statement(source, converted) is expected


# --------------------------------------------------------------------------------------
# the file which is handed over


def make_result(status, ordinal=1, text='SELECT a FROM t', converted='SELECT "a" FROM "t"',
                warnings=(), file_name='queries.sql'):
    statement = Statement(text, ordinal, 1, 1, input_file=file_name, name=None)
    result = StatementResult(statement, 1)
    result.status = status
    result.output_sql = converted if status in (CONVERTED, UNCHANGED, CONVERTED_FAILING) else ''
    result.warnings = list(warnings)
    result.target_test = ('OK', 'explain on postgresql')
    result.parameters_line = 'parameters: none'
    return result


def writer(tmp_path, **settings):
    base = {'directory': str(tmp_path), 'prefix': '', 'suffix': '_pg', 'overwrite': False,
            'include_original': True, 'sidecar': 'json'}
    base.update(settings)
    return OutputWriter(base, lambda level, message: None)


HEADER = {'tool': 'credativ-pg-migrator test', 'source_db_type': 'mssql',
          'target_db_type': 'postgresql', 'target_schema': 'migtest', 'notes': ['name mapping: off']}


def test_the_converted_statement_is_written_as_sql(tmp_path):
    output = writer(tmp_path).render('queries.sql', [make_result(CONVERTED)], HEADER)
    assert 'SELECT "a" FROM "t";' in output
    assert '-- status: CONVERTED' in output


@pytest.mark.parametrize('status', [SKIPPED, NOT_CONVERTED])
def test_a_statement_which_may_not_be_used_is_commented_out(tmp_path, status):
    """The file has to stay runnable as a whole."""
    result = make_result(status)
    result.reason = 'gate 2: the statement is a UPDATE, not a read'
    output = writer(tmp_path).render('queries.sql', [result], HEADER)
    assert '-- SELECT a FROM t' in output
    assert '\nSELECT a FROM t' not in output.replace('-- SELECT a FROM t', '')
    assert 'gate 2' in output


def test_a_statement_whose_test_failed_is_commented_out_as_well(tmp_path):
    result = make_result(CONVERTED_FAILING)
    result.target_test = ('FAILED', 'relation "t" does not exist')
    output = writer(tmp_path).render('queries.sql', [result], HEADER)
    assert '-- SELECT "a" FROM "t";' in output
    assert 'relation "t" does not exist' in output


def test_every_block_says_what_both_tests_answered(tmp_path):
    output = writer(tmp_path).render('queries.sql', [make_result(CONVERTED)], HEADER)
    assert '-- source test: not run' in output
    assert '-- target test: OK' in output


def test_a_warning_is_written_where_it_cannot_be_missed(tmp_path):
    result = make_result(CONVERTED, warnings=['BLOCKING: the conversion moved the bind parameters'])
    output = writer(tmp_path).render('queries.sql', [result], HEADER)
    assert '-- WARNING: BLOCKING: the conversion moved the bind parameters' in output


def test_the_original_statement_can_be_left_out(tmp_path):
    with_original = writer(tmp_path).render('queries.sql', [make_result(CONVERTED)], HEADER)
    without = writer(tmp_path, include_original=False).render('queries.sql', [make_result(CONVERTED)], HEADER)
    assert '-- ORIGINAL:' in with_original
    assert '-- ORIGINAL:' not in without


def test_the_head_of_the_file_counts_the_statements(tmp_path):
    results = [make_result(CONVERTED, 1), make_result(SKIPPED, 2), make_result(NOT_CONVERTED, 3)]
    output = writer(tmp_path).render('queries.sql', results, HEADER)
    assert 'statements: 3' in output
    assert 'converted: 1' in output
    assert 'skipped: 1' in output


def test_the_same_input_produces_the_same_file(tmp_path):
    """No timestamp and no duration in it - the file is comparable from run to run."""
    first = writer(tmp_path).render('queries.sql', [make_result(CONVERTED)], HEADER)
    second = writer(tmp_path).render('queries.sql', [make_result(CONVERTED)], HEADER)
    assert first == second


# --------------------------------------------------------------------------------------
# where the file is written


def test_the_output_file_is_named_after_the_input_file(tmp_path):
    assert writer(tmp_path).output_path('/queries/reports.sql') == str(tmp_path / 'reports_pg.sql')
    assert writer(tmp_path, prefix='pg_', suffix='').output_path('/q/r.sql') == str(tmp_path / 'pg_r.sql')


def test_the_output_is_written_next_to_the_input_when_no_directory_is_given(tmp_path):
    input_file = tmp_path / 'reports.sql'
    assert writer(tmp_path, directory='').output_path(str(input_file)) == str(tmp_path / 'reports_pg.sql')


def test_an_output_which_would_overwrite_the_input_is_refused(tmp_path):
    input_file = tmp_path / 'reports.sql'
    input_file.write_text('SELECT 1', encoding='utf-8')
    instance = writer(tmp_path, directory='', suffix='')
    with pytest.raises(ValueError) as caught:
        instance.check_path(str(input_file), instance.output_path(str(input_file)))
    assert 'written over the file' in str(caught.value)


def test_an_existing_output_file_is_not_replaced_silently(tmp_path):
    (tmp_path / 'reports_pg.sql').write_text('older run', encoding='utf-8')
    instance = writer(tmp_path)
    with pytest.raises(ValueError):
        instance.check_path('/elsewhere/reports.sql', instance.output_path('/elsewhere/reports.sql'))
    writer(tmp_path, overwrite=True).check_path('/elsewhere/reports.sql', str(tmp_path / 'reports_pg.sql'))


def test_the_sidecar_holds_one_record_per_statement(tmp_path):
    input_file = tmp_path / 'src' / 'queries.sql'
    os.makedirs(input_file.parent, exist_ok=True)
    input_file.write_text('SELECT a FROM t', encoding='utf-8')
    results = [make_result(CONVERTED, 1), make_result(SKIPPED, 2)]
    output_file, sidecar = writer(tmp_path).write(str(input_file), results, HEADER)
    assert os.path.exists(output_file)
    records = json.loads(open(sidecar, encoding='utf-8').read())
    assert [record['status'] for record in records] == [CONVERTED, SKIPPED]
    assert records[0]['sha256']


def test_the_sidecar_can_be_switched_off(tmp_path):
    input_file = tmp_path / 'src' / 'queries.sql'
    os.makedirs(input_file.parent, exist_ok=True)
    input_file.write_text('SELECT a FROM t', encoding='utf-8')
    _output_file, sidecar = writer(tmp_path, sidecar='off').write(str(input_file), [make_result(CONVERTED)], HEADER)
    assert sidecar is None


# --------------------------------------------------------------------------------------
# the closing count


def test_the_summary_counts_every_status():
    summary = render_summary([make_result(CONVERTED, 1), make_result(SKIPPED, 2), make_result(SKIPPED, 3)])
    lines = [line for line in summary.splitlines() if line.startswith('TOTAL')]
    assert len(lines) == 1, summary
    ## stmts | conv | unch | fail | n/conv | skip
    assert [cell.strip() for cell in lines[0].split('|')] == ['TOTAL', '3', '1', '0', '0', '0', '2']


def test_the_summary_counts_each_file_of_its_own():
    summary = render_summary([make_result(CONVERTED, 1, file_name='a.sql'),
                              make_result(SKIPPED, 1, file_name='b.sql'),
                              make_result(SKIPPED, 2, file_name='b.sql')])
    rows = {line.split('|')[0].strip(): [cell.strip() for cell in line.split('|')[1:]]
            for line in summary.splitlines() if '|' in line}
    assert rows['a.sql'][:2] == ['1', '1']
    assert rows['b.sql'][0] == '2'
    assert rows['b.sql'][-1] == '2'


def test_the_summary_names_what_has_to_be_looked_at():
    """A statement which needs a person is named with the place it stands in."""
    failing = make_result(CONVERTED_FAILING, 7, file_name='queries/01_reports.sql')
    failing.reason = 'relation "migtest.orders" does not exist'
    summary = render_summary([make_result(CONVERTED, 1), failing])
    assert 'STATEMENTS WHICH NEED ATTENTION' in summary
    assert '01_reports.sql:1-1' in summary
    assert 'relation "migtest.orders" does not exist' in summary
    assert 'STATEMENTS NEEDING ATTENTION: 1' in summary


def test_the_summary_says_so_when_there_is_nothing_to_look_at():
    summary = render_summary([make_result(CONVERTED, 1), make_result(SKIPPED, 2)])
    assert 'STATEMENTS NEEDING ATTENTION: 0' in summary
    assert 'STATEMENTS WHICH NEED ATTENTION' not in summary


def test_the_summary_groups_the_refusals_by_their_reason():
    first = make_result(SKIPPED, 1)
    first.reason = 'gate 2: the statement is a UPDATE, not a read'
    second = make_result(SKIPPED, 2)
    second.reason = 'gate 2: the statement is a UPDATE, not a read'
    summary = render_summary([first, second])
    assert 'REFUSED - NOT A READ, NEVER SENT TO A DATABASE' in summary
    assert '2 x gate 2: the statement is a UPDATE, not a read' in summary


def test_the_summary_counts_the_blocking_warnings_separately():
    result = make_result(CONVERTED, 1, warnings=['BLOCKING: the conversion moved the bind parameters'])
    summary = render_summary([result, make_result(CONVERTED, 2, warnings=['the table hint NOLOCK is removed'])])
    assert '2 statement(s) carry a warning, 1 of them BLOCKING' in summary


def test_the_summary_reports_the_target_test():
    tested = make_result(CONVERTED, 1)
    tested.target_test_ms = 12.5
    summary = render_summary([tested, make_result(SKIPPED, 2)])
    assert 'TARGET TEST' in summary
    assert '1 statement(s) tested: OK 1' in summary


def test_the_summary_names_the_files_which_were_written():
    summary = render_summary([make_result(CONVERTED, 1)],
                             {'written': ['converted/01_reports_pg.sql', 'converted/01_reports_pg.json']})
    assert 'FILES WRITTEN' in summary
    assert 'converted/01_reports_pg.json' in summary


def test_the_summary_names_both_databases():
    summary = render_summary([make_result(CONVERTED, 1)], {
        'source_db_type': 'mssql', 'source_database': 'migtest', 'source_schema': 'dbo',
        'target_db_type': 'postgresql', 'target_database': 'mssql', 'target_schema': 'migtest',
        'notes': ['name mapping: off']})
    assert 'Source: migtest, schema: dbo (mssql)' in summary
    assert 'Target: mssql, schema: migtest (postgresql)' in summary
    assert 'name mapping: off' in summary


def test_a_statement_which_needs_attention_is_a_failure_of_the_run():
    assert make_result(CONVERTED_FAILING).is_failure
    assert make_result(NOT_CONVERTED).is_failure
    assert not make_result(CONVERTED).is_failure
    assert not make_result(UNCHANGED).is_failure
    ## a statement which was refused is not a failure - it is the step doing its work
    assert not make_result(SKIPPED).is_failure


# --------------------------------------------------------------------------------------
# the sidecar and the types a result holds


def result_with_timestamps(ordinal=1, total=1):
    import datetime
    statement = Statement('SELECT 1', ordinal, 1, 1, 'queries/reports.sql')
    result = StatementResult(statement, total)
    result.status = CONVERTED
    result.output_sql = 'SELECT 1'
    result.statement_kind = 'SELECT'
    result.task_started = datetime.datetime(2026, 8, 21, 11, 42, 21, 628000)
    result.task_completed = datetime.datetime(2026, 8, 21, 11, 42, 21, 661000)
    result.target_test = ('OK', 'explain on postgresql')
    result.target_test_ms = 12.4
    return result


def test_the_sidecar_holds_the_timestamps_of_a_statement(tmp_path):
    """
    The timestamps are datetimes, because that is what the protocol table takes and as_dict()
    serves both. json.dump stopped the whole run with "Object of type datetime is not JSON
    serializable" - after the output file had been written, so the deliverable stood there and
    the run failed behind it.
    """
    writer = OutputWriter({'directory': str(tmp_path), 'sidecar': 'json'}, lambda level, message: None)
    _output, sidecar = writer.write(str(tmp_path / 'reports.sql'), [result_with_timestamps()],
                                    {'tool': 't', 'source_db_type': 'ibm_db2_luw',
                                     'target_db_type': 'postgresql', 'target_schema': 'public'})
    record = json.loads(open(sidecar, encoding='utf-8').read())[0]
    assert record['task_started'] == '2026-08-21 11:42:21.628'
    assert record['task_completed'] == '2026-08-21 11:42:21.661'


def test_the_sidecar_is_valid_json_for_every_field_a_result_carries(tmp_path):
    writer = OutputWriter({'directory': str(tmp_path), 'sidecar': 'json'}, lambda level, message: None)
    result = result_with_timestamps()
    result.warnings = ['a warning']
    result.unresolved_objects = ['AUDIT_LOG']
    result.gate_refused = None
    _output, sidecar = writer.write(str(tmp_path / 'reports.sql'), [result],
                                    {'tool': 't', 'source_db_type': 'oracle',
                                     'target_db_type': 'postgresql', 'target_schema': 'public'})
    record = json.loads(open(sidecar, encoding='utf-8').read())[0]
    assert record['statement_kind'] == 'SELECT'
    assert record['warnings'] == ['a warning']
    assert record['unresolved_objects'] == ['AUDIT_LOG']
    assert record['target_test']['duration_ms'] == 12.4


def test_a_type_the_sidecar_does_not_know_is_an_error_and_not_a_string():
    """
    Stringifying whatever turns up would put something nobody designed into the file a CI job
    reads. It is refused by name instead.
    """
    from credativ_pg_migrator.query_conversion.writer import json_ready
    with pytest.raises(TypeError) as raised:
        json_ready(object())
    assert 'cannot hold a value of type' in str(raised.value)


def test_a_date_is_rendered_as_a_date():
    import datetime
    from credativ_pg_migrator.query_conversion.writer import json_ready
    assert json_ready(datetime.date(2026, 8, 21)) == '2026-08-21'
