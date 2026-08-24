# SPDX-License-Identifier: GPL-3.0-or-later
"""
The closing pass over the objects establishes presence, and says so.

P2-6 of development/OPEN_ISSUES.md. At the end of a standard migration the orchestrator asks
the catalogue of the target whether each migrated view, routine and trigger is there — an
object whose creation failed only because a dependency did not exist yet can become creatable
once the whole schema is present, so the pass is worth having. What it recorded was
`final_valid = true` and the word **"valid"**, and being in a catalogue is not doing what the
object of the source did.

It means three different things for the three kinds, which is the reason one word could not
carry it:

  * a **view** which is there has had its query resolved by PostgreSQL, and PostgreSQL keeps
    the objects it reads from being dropped underneath it — so it can be read. Whether it
    answers the same rows as the view of the source is not established by this and cannot be
    established by a catalogue at all.
  * a **PL/pgSQL routine** which is there has had the *syntax* of its body parsed and nothing
    else: a body which reads a table or a column which is not there is created without
    complaint and fails at the first call. The check matches the name alone as well, so one
    overload of a name is enough for all of them.
  * a **trigger** which is there is attached to its table, so the wiring arrived. What its
    function does is the function's own business.

Reading it for this turned up a defect of the kind P2-2 was about, one step down: the message
was **carried over** from a step which the next one overtook. A retry which raised no exception
set `message = 'valid after retry'`, and if the existence check right after it answered no the
row was written as `final_valid = false` with `valid after retry` as its message — a record
which says an object is valid after a retry which had not put it there. The message is composed
at the end now, from what really happened.

Nothing here connects to anything: the target connector and the protocol tables are stubs.

Run with:  python3 -m pytest tests/test_object_presence_check.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)


class Config:
    def __init__(self):
        self.messages = []

    def print_log_message(self, level, message):
        self.messages.append((level, str(message)))

    def written(self):
        return ' | '.join(message for _, message in self.messages)

    def convert_names_case(self, name):
        return (name or '').lower()

    def get_packages_migration_style(self):
        return 'prefix'


class Target:
    """A target which answers what a test wants it to answer, and records what it was run."""

    def __init__(self, exists=False, exists_after_retry=None, retry_raises=None,
                 exists_raises=None):
        self.answers = [exists, exists if exists_after_retry is None else exists_after_retry]
        self.retry_raises = retry_raises
        self.exists_raises = exists_raises
        self.statements = []
        self.session_settings = ''

    def _answer(self):
        if self.exists_raises:
            raise self.exists_raises
        return self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]

    def target_view_exists(self, schema, name):
        return self._answer()

    def target_funcproc_exists(self, schema, name):
        return self._answer()

    def target_funcprocs_with_prefix_exist(self, schema, prefix):
        return self._answer()

    def target_trigger_exists(self, schema, table, name):
        return self._answer()

    def execute_query(self, query):
        self.statements.append(query)
        if self.retry_raises and 'CREATE' in str(query).upper():
            raise self.retry_raises


class ProtocolTables:
    def __init__(self):
        self.recorded = []

    def update_object_final_valid(self, object_type, object_id, final_valid, message):
        self.recorded.append((object_type, object_id, final_valid, message))


def orchestrator():
    from credativ_pg_migrator.orchestrator import Orchestrator

    made = Orchestrator.__new__(Orchestrator)
    made.config_parser = Config()
    made.migrator_tables = ProtocolTables()
    return made


def check(object_type='view', mode='check', ddl='CREATE VIEW v AS SELECT 1', **target):
    made = orchestrator()
    connector = Target(**target)
    obj = {'id': 7, 'schema': 'tgt', 'name': 'v_customers', 'table': 'customers',
           'ddl': ddl, 'label': 'v_customers'}
    final_valid, was_retried = made._validate_one_object(connector, mode, object_type, obj)
    return final_valid, was_retried, made.migrator_tables.recorded[0][3], made, connector


# --------------------------------------------------------------------------------------
# what the pass says it established


def test_an_object_which_is_there_is_recorded_as_being_there_and_not_as_valid():
    final_valid, was_retried, message, made, connector = check(exists=True)
    assert final_valid is True
    assert message.startswith('in the target')
    assert 'presence and not equivalence' in message


@pytest.mark.parametrize('object_type,expected', [
    ('view', 'resolves the query of a view'),
    ('funcproc', 'checks the SYNTAX of a PL/pgSQL body'),
    ('trigger', 'attached to its table'),
])
def test_the_message_says_what_presence_means_for_this_kind_of_object(object_type, expected):
    """
    One word could not carry it: what being in the catalogue establishes differs a great deal
    between a view, a routine and a trigger.
    """
    final_valid, was_retried, message, made, connector = check(object_type=object_type, exists=True)
    assert expected in message


def test_the_message_of_a_routine_says_that_its_body_was_only_parsed():
    """
    The weakest of the three, and the one most likely to be read as more than it is: a body
    which reads a table which is not there is created without complaint.
    """
    final_valid, was_retried, message, made, connector = check(object_type='funcproc', exists=True)
    assert 'fails at the first call' in message
    assert 'name alone' in message, 'one overload of a name is enough for all of them'


def test_the_word_valid_is_not_used_for_a_name_lookup():
    for object_type in ('view', 'funcproc', 'trigger'):
        final_valid, was_retried, message, made, connector = check(object_type=object_type, exists=True)
        assert not message.startswith('valid')
        assert 'valid after retry' not in message


def test_an_object_which_is_not_there_says_so_plainly():
    final_valid, was_retried, message, made, connector = check(exists=False, mode='check')
    assert final_valid is False
    assert message == 'NOT in the target'


def test_an_object_nothing_was_ever_created_for_is_not_counted_as_missing():
    final_valid, was_retried, message, made, connector = check(ddl='', exists=False)
    assert final_valid is None
    assert message == 'not migrated (no target DDL)'


# --------------------------------------------------------------------------------------
# the message is composed from what happened - the defect found while reading it


def test_a_retry_which_raised_nothing_and_put_nothing_there_is_not_called_valid():
    """
    The message used to be set to 'valid after retry' the moment the DDL ran without an
    exception, and the existence check right after it could still answer no - so the row was
    written as final_valid=false with 'valid after retry' as its message.
    """
    final_valid, was_retried, message, made, connector = check(
        mode='retry', exists=False, exists_after_retry=False)
    assert final_valid is False
    assert was_retried is True
    assert 'valid' not in message
    assert 'raised nothing and the object is still not there' in message


def test_a_retry_which_worked_says_that_it_was_a_retry():
    final_valid, was_retried, message, made, connector = check(
        mode='retry', exists=False, exists_after_retry=True)
    assert final_valid is True
    assert was_retried is True
    assert message.startswith('in the target after a retry')


def test_a_retry_which_failed_carries_the_reason():
    final_valid, was_retried, message, made, connector = check(
        mode='retry', exists=False, retry_raises=RuntimeError('relation "orders" does not exist'))
    assert final_valid is False
    assert 'the retry of its DDL failed' in message
    assert 'relation "orders" does not exist' in message


def test_a_check_which_could_not_be_run_is_not_read_as_an_absent_object():
    final_valid, was_retried, message, made, connector = check(
        mode='check', exists_raises=RuntimeError('permission denied for schema tgt'))
    assert final_valid is False
    assert 'the check itself could not be run' in message
    assert 'permission denied' in message


def test_an_error_of_the_first_check_is_not_repeated_after_a_successful_retry():
    """
    The first check errored, the retry put the object there - the row must not still carry the
    error of a check which was overtaken.
    """
    made = orchestrator()

    class FirstCheckFails(Target):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def target_view_exists(self, schema, name):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError('the connection was not ready')
            return True

    connector = FirstCheckFails()
    obj = {'id': 1, 'schema': 'tgt', 'name': 'v', 'table': None,
           'ddl': 'CREATE VIEW v AS SELECT 1', 'label': 'v'}
    final_valid, was_retried = made._validate_one_object(connector, 'retry', 'view', obj)
    assert final_valid is True
    message = made.migrator_tables.recorded[0][3]
    assert 'the connection was not ready' not in message


def test_a_trigger_which_needs_a_hand_is_recorded_as_not_created():
    made = orchestrator()
    obj = {'id': 3, 'schema': 'tgt', 'name': 'tr', 'table': 't', 'ddl': None,
           'manual': True, 'label': 'tr'}
    final_valid, was_retried = made._validate_one_object(Target(), 'retry', 'trigger', obj)
    assert final_valid is False
    assert made.migrator_tables.recorded[0][3] == 'not created - manual adjustment required'


def test_the_stored_ddl_of_such_a_trigger_is_never_executed():
    """It is there to be completed by hand - running it is what it was kept out of the retry for."""
    made = orchestrator()
    connector = Target()
    obj = {'id': 3, 'schema': 'tgt', 'name': 'tr', 'table': 't', 'ddl': None,
           'manual': True, 'label': 'tr'}
    made._validate_one_object(connector, 'retry', 'trigger', obj)
    assert connector.statements == []


# --------------------------------------------------------------------------------------
# what the run and the summary call it


def test_the_summary_no_longer_calls_a_name_lookup_valid():
    path = os.path.join(REPO, 'credativ_pg_migrator', 'migrator_tables.py')
    with open(path, encoding='utf-8') as handle:
        source = handle.read()
    assert 'f"In target: {valid_count}, Missing: {invalid_count}"' in source
    assert 'f"Valid: {valid_count}, Invalid: {invalid_count}"' not in source


def test_the_run_says_the_limit_of_the_pass_once():
    """
    The reader is told what the pass can and cannot say, rather than having to know it. The
    line is written where the pass starts.
    """
    path = os.path.join(REPO, 'credativ_pg_migrator', 'orchestrator.py')
    with open(path, encoding='utf-8') as handle:
        source = handle.read()
    start = source.split('def stdwf_validate_objects')[1].split('def ')[0]
    assert 'not the same as doing what the object of the source did' in start
    assert 'in the target: {valid}, missing: {invalid}' in start


def test_every_kind_of_object_the_pass_looks_at_has_its_wording():
    from credativ_pg_migrator.orchestrator import Orchestrator

    assert set(Orchestrator.WHAT_PRESENCE_MEANS) == {'view', 'funcproc', 'trigger'}
    for wording in Orchestrator.WHAT_PRESENCE_MEANS.values():
        assert 'NOT' in wording or 'not' in wording, (
            'the wording has to say what it does not establish, not only what it does')
