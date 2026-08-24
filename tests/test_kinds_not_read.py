# SPDX-License-Identifier: GPL-3.0-or-later
"""
"Not implemented" is not "the source has none".

P2-8 of development/OPEN_ISSUES.md. `fetch_user_defined_types()` and `fetch_domains()` answered
`{}` in connectors whose sources certainly do hold such objects — Db2 distinct types, the
DISTINCT and named ROW types of Informix, the user-defined data types of SQL Anywhere, the
rules of SQL Server which the **Sybase ASE connector of this same migrator** reads as domains.
An empty answer says *the source holds none of these*, so the planner wrote "No user defined
types found" and the summary showed `0` — and a reader who takes the summary at its word
migrates a schema which is missing the objects nobody said were missing.

The two are separated by declaration now, per connector and per kind:

  * `OBJECT_KINDS_NOT_READ` — the source has them and this connector does not read them, with
    what is really there and where it is kept;
  * `OBJECT_KINDS_ABSENT` — the source does not have them at all, so `{}` is the truth.

The test which matters most is the last one: a fetch which is a **stub** and whose kind is in
neither table fails the suite. The question has to be answered rather than left to an empty
dictionary, and it cannot be left unanswered for a connector added later.

Nothing here connects to anything.

Run with:  python3 -m pytest tests/test_kinds_not_read.py -v
"""

import ast
import importlib
import inspect
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.constants import MigratorConstants

KINDS = ('user_defined_types', 'domains')


def connector_classes():
    """Every source connector, skipping the ones whose driver is not installed here."""
    found = {}
    for db_type, module_path in sorted(MigratorConstants.get_modules().items()):
        module_name, class_name = module_path.split(':')
        try:
            found[db_type] = getattr(importlib.import_module(module_name), class_name)
        except Exception:
            continue
    return found


CONNECTORS = connector_classes()


def is_a_stub(connector_class, method_name):
    """
    Whether a fetch answers without asking the source: `pass`, `return {}` or `return None`.

    Read from the source of the method rather than from a list, so a connector which grows a
    real implementation stops being a stub without anybody having to remember this file.
    """
    method = getattr(connector_class, method_name, None)
    if method is None:
        return True
    try:
        source = inspect.getsource(method)
    except (OSError, TypeError):
        return False
    tree = ast.parse(source.lstrip() if source.startswith(' ') else source)
    body = [node for node in tree.body[0].body
            if not (isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
                    and isinstance(node.value.value, str))]
    if len(body) != 1:
        return False
    only = body[0]
    if isinstance(only, ast.Pass):
        return True
    if isinstance(only, ast.Return):
        if only.value is None:
            return True
        if isinstance(only.value, ast.Dict) and not only.value.keys:
            return True
        if isinstance(only.value, ast.Constant) and only.value.value is None:
            return True
    return False


# --------------------------------------------------------------------------------------
# the declaration itself


def test_the_base_connector_declares_nothing_and_offers_the_two_questions():
    from credativ_pg_migrator.database_connector import DatabaseConnector

    assert DatabaseConnector.OBJECT_KINDS_NOT_READ == {}
    assert DatabaseConnector.OBJECT_KINDS_ABSENT == {}
    assert hasattr(DatabaseConnector, 'object_kind_not_read')
    assert hasattr(DatabaseConnector, 'object_kind_is_absent')


def test_a_connector_answers_what_it_cannot_read():
    informix = CONNECTORS.get('informix')
    if informix is None:
        pytest.skip('informix needs a driver which is not installed here')
    made = informix.__new__(informix)
    assert 'DISTINCT' in made.object_kind_not_read('user_defined_types')
    assert made.object_kind_not_read('domains') is None
    assert made.object_kind_is_absent('domains') is True


@pytest.mark.parametrize('db_type', sorted(CONNECTORS))
def test_no_kind_is_called_both_missing_and_absent(db_type):
    connector_class = CONNECTORS[db_type]
    both = set(connector_class.OBJECT_KINDS_NOT_READ) & set(connector_class.OBJECT_KINDS_ABSENT)
    assert not both, f'{db_type} says {sorted(both)} is both not read and not there'


@pytest.mark.parametrize('db_type', sorted(CONNECTORS))
def test_every_declaration_says_something(db_type):
    """A reason of two words helps nobody: it has to say what is really there, or why none is."""
    connector_class = CONNECTORS[db_type]
    for kind, reason in list(connector_class.OBJECT_KINDS_NOT_READ.items()) + \
            list(connector_class.OBJECT_KINDS_ABSENT.items()):
        assert kind in KINDS or kind, db_type
        assert len(str(reason)) > 30, f'{db_type}: {kind} is dismissed with "{reason}"'


# --------------------------------------------------------------------------------------
# the guard: no stub may answer "the source has none" by accident


@pytest.mark.parametrize('db_type', sorted(CONNECTORS))
@pytest.mark.parametrize('kind', KINDS)
def test_a_fetch_which_asks_nothing_has_to_say_which_of_the_two_it_is(db_type, kind):
    """
    The test this repair is really made of. A stub which is in neither table answers `{}` -
    *the source holds none of these* - and nothing anywhere says whether that is true.
    """
    connector_class = CONNECTORS[db_type]
    method_name = f'fetch_{kind}'
    if not is_a_stub(connector_class, method_name):
        return
    declared = (kind in connector_class.OBJECT_KINDS_NOT_READ
                or kind in connector_class.OBJECT_KINDS_ABSENT)
    assert declared, (
        f'{db_type}.{method_name}() answers without asking the source, and {kind} is in '
        f'neither OBJECT_KINDS_NOT_READ nor OBJECT_KINDS_ABSENT - so the run says the source '
        f'has none of them and nobody has decided whether that is true')


@pytest.mark.parametrize('db_type', sorted(CONNECTORS))
@pytest.mark.parametrize('kind', KINDS)
def test_a_fetch_which_really_reads_the_source_is_not_declared_as_unread(db_type, kind):
    """The other direction: a declaration which outlived the stub it described."""
    connector_class = CONNECTORS[db_type]
    if is_a_stub(connector_class, f'fetch_{kind}'):
        return
    assert kind not in connector_class.OBJECT_KINDS_NOT_READ, (
        f'{db_type}.fetch_{kind}() reads the source and is still declared as not read')
    assert kind not in connector_class.OBJECT_KINDS_ABSENT, (
        f'{db_type}.fetch_{kind}() reads the source and is still declared as having none')


def test_the_connectors_the_issue_names_are_the_ones_which_declare_it():
    """
    P2-8 names ibm_db2_zos, ibm_db2_i, ibm_db2_luw, informix and sql_anywhere for the user
    defined types. Reading the rest turned up two more of the same shape, which are declared
    with them: the domains of SQL Anywhere, and the rules of SQL Server which the Sybase ASE
    connector of this same migrator reads as domains.
    """
    not_read = {db_type: sorted(cls.OBJECT_KINDS_NOT_READ)
                for db_type, cls in CONNECTORS.items() if cls.OBJECT_KINDS_NOT_READ}
    for db_type in ('ibm_db2_zos', 'ibm_db2_i', 'ibm_db2_luw', 'informix', 'sql_anywhere'):
        if db_type in CONNECTORS:
            assert 'user_defined_types' in not_read.get(db_type, []), db_type
    if 'mssql' in CONNECTORS:
        assert 'domains' in not_read.get('mssql', [])
    if 'sql_anywhere' in CONNECTORS:
        assert 'domains' in not_read.get('sql_anywhere', [])


def test_an_engine_which_really_has_none_says_so_rather_than_being_left_out():
    """
    MySQL, MariaDB and SQLite have neither kind. `{}` is the truth for them - and it is written
    down, so that "nobody looked" and "there is nothing to look at" stay apart.
    """
    for db_type in ('mysql', 'mariadb', 'sqlite'):
        if db_type not in CONNECTORS:
            continue
        absent = CONNECTORS[db_type].OBJECT_KINDS_ABSENT
        assert set(KINDS) <= set(absent), db_type


# --------------------------------------------------------------------------------------
# what the run says about it


class Config:
    def __init__(self):
        self.messages = []

    def print_log_message(self, level, message):
        self.messages.append((level, str(message)))

    def levels(self, level):
        return [message for written, message in self.messages if written == level]


class ProtocolTables:
    def __init__(self):
        self.journal = []

    def insert_protocol(self, settings):
        self.journal.append(dict(settings))


class SourceWhichCannotRead:
    OBJECT_KINDS_NOT_READ = {'user_defined_types': 'Db2 has distinct types and this connector does not read them.'}

    def object_kind_not_read(self, kind):
        return self.OBJECT_KINDS_NOT_READ.get(kind)


class SourceWhichReadsEverything:
    def object_kind_not_read(self, kind):
        return None


def planner_with(source):
    from credativ_pg_migrator.planner import Planner

    made = Planner.__new__(Planner)
    made.config_parser = Config()
    made.migrator_tables = ProtocolTables()
    made.source_connection = source
    return made


def test_a_kind_which_was_not_read_is_reported_as_that_and_not_as_none_found():
    made = planner_with(SourceWhichCannotRead())
    assert made.report_kind_not_read('user_defined_types', 'user_defined_type', 'phase') is True
    written = made.config_parser.levels('WARNING')[0]
    assert 'NOT READ from this source' in written
    assert 'not the same as the source having none' in written
    assert 'distinct types' in written
    assert 'by hand' in written


def test_it_is_written_into_the_journal_where_the_summary_reads_it():
    made = planner_with(SourceWhichCannotRead())
    made.report_kind_not_read('user_defined_types', 'user_defined_type', 'phase')
    row = made.migrator_tables.journal[0]
    assert row['row_type'] == 'not read'
    assert row['object_type'] == 'user_defined_type'
    assert 'distinct types' in row['execution_error_message']


def test_a_source_which_really_has_none_is_left_to_say_so():
    made = planner_with(SourceWhichReadsEverything())
    assert made.report_kind_not_read('user_defined_types', 'user_defined_type', 'phase') is False
    assert made.config_parser.messages == []
    assert made.migrator_tables.journal == []


def test_a_journal_which_cannot_be_written_does_not_take_the_warning_with_it():
    made = planner_with(SourceWhichCannotRead())

    def refuse(settings):
        raise RuntimeError('the protocol database is gone')

    made.migrator_tables.insert_protocol = refuse
    assert made.report_kind_not_read('user_defined_types', 'user_defined_type', 'phase') is True
    assert made.config_parser.levels('WARNING'), 'the reader is told either way'
    assert made.config_parser.levels('ERROR')


def test_the_summary_shows_a_question_mark_rather_than_a_zero():
    path = os.path.join(REPO, 'credativ_pg_migrator', 'migrator_tables.py')
    with open(path, encoding='utf-8') as handle:
        source = handle.read()
    assert "WHERE row_type = 'not read'" in source
    assert 'NOT READ from this source - ' in source
    assert "lines.append(f\"{obj_name:<24} | {'?':>6}" in source
