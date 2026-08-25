# SPDX-License-Identifier: GPL-3.0-or-later
"""
The names inside the body of a migrated routine — measured, and then repaired.

P3-2 of development/OPEN_ISSUES.md said **measure before building anything**: the
`names_case_handling` repair reaches the statements inside a routine for ms_sql and sybase_ase,
because those go through the connector's statement converter one statement at a time, and the
other sources were never looked at. One routine per source was converted with a mixed-case
source, under `lower` and under `upper`, and this is what it showed:

| source | the routine | the names in the body |
|---|---|---|
| ms_sql, sybase_ase | converted statement by statement | **converted** |
| oracle | converted (PL/SQL to PL/pgSQL, best effort) | the names of the source; only the schema in front of them is re-pointed |
| ibm_db2_zos | converted | the same |
| informix | converted | it wrote `"migtest"."Orders"` — the source spelling, **in quotes**, which freezes it: with `lower` the migration created `orders` and the body named `Orders` |
| sql_anywhere | header converted, body carried over as text | the names of the source |
| ibm_db2_luw | **not converted** — a function mapping and SIGNAL, no header, no language | — |
| ibm_db2_i | **not converted**, and it read `settings['code']` while the one caller passes `funcproc_code`, so it answered the empty string for every routine of every Db2 for i migration | — |
| mysql, mariadb | **not converted** — an explicit placeholder returning `''` | — |
| sqlite | has no routines at all | — |

So the entry's guess — *"Db2 converts identifiers itself and is probably fine"* — was half right:
Db2 for z/OS converts the routine and leaves the names, and Db2 for LUW and for i do not convert
a routine at all.

What the measurement made repairable is asserted here: Informix no longer freezes the source
spelling, Db2 for i no longer answers the empty string by accident, and every source whose body
keeps its own names says so — loudly, where `names_case_handling` is not `lower`, because an
undelimited name folds to lower case and that is the one setting under which such a body finds
the objects it names.

Nothing here connects to anything.

Run with:  python3 -m pytest tests/test_routine_body_names.py -v
"""

import importlib
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.constants import MigratorConstants

ROUTINE = '''CREATE PROCEDURE MigTest.Add_Order(IN p_cid INTEGER)
BEGIN
  DECLARE v_total DECIMAL;
  SELECT SUM(o.Total) INTO v_total FROM MigTest.Orders o WHERE o.Cust_Id = p_cid;
  UPDATE MigTest.Customers SET Last_Total = v_total WHERE Cust_Id = p_cid;
END'''


class Config:
    def __init__(self, names_case='lower'):
        self.names_case = names_case
        self.messages = []

    def print_log_message(self, level, message):
        self.messages.append((level, str(message)))

    def levels(self, level):
        return [message for written, message in self.messages if written == level]

    def get_names_case_handling(self):
        return self.names_case

    def indent_code(self, code, *arguments, **keywords):
        return code

    def convert_names_case(self, name):
        if name is None:
            return None
        return name.lower() if self.names_case == 'lower' else (
            name.upper() if self.names_case == 'upper' else name)


def connector(db_type, names_case='lower'):
    module_path = MigratorConstants.get_modules().get(db_type)
    module_name, class_name = module_path.split(':')
    try:
        connector_class = getattr(importlib.import_module(module_name), class_name)
    except Exception as error:
        pytest.skip(f'{db_type} needs a driver which is not installed here ({error})')
    made = connector_class.__new__(connector_class)
    made.config_parser = Config(names_case)
    made.connectivity = 'odbc'
    made.source_schema_name = 'MigTest'
    made.source_package_names = set()
    return made


def convert(made, code=ROUTINE, **extra):
    settings = {
        'funcproc_code': code, 'funcproc_name': 'Add_Order', 'target_db_type': 'postgresql',
        'source_schema_name': 'MigTest', 'target_schema_name': 'migtest',
        'table_list': ['Orders', 'Customers'], 'view_list': [],
    }
    settings.update(extra)
    return made.convert_funcproc_code(settings) or ''


# --------------------------------------------------------------------------------------
# Informix: the source spelling was frozen in quotes


@pytest.mark.parametrize('names_case,expected', [('lower', 'orders'), ('upper', 'ORDERS')])
def test_informix_names_the_tables_of_the_body_as_the_target_has_them(names_case, expected):
    """
    It wrote `"migtest"."Orders"` - the source spelling, in quotes, which freezes it. With
    `lower` the migration created `orders` and the body of every routine named `Orders`: valid
    PL/pgSQL which fails the moment the routine runs.
    """
    made = connector('informix', names_case)
    converted = convert(made, ROUTINE.replace('END', 'END PROCEDURE;'))
    assert f'"migtest"."{expected}"' in converted
    assert '"migtest"."Orders"' not in converted


def test_informix_says_that_the_columns_of_the_body_are_still_its_own():
    made = connector('informix')
    assert 'COLUMNS' in made.routine_body_names_not_converted()
    assert 'tables and the views are given the names of the target' in made.routine_body_names_not_converted()


# --------------------------------------------------------------------------------------
# Db2 for i: the converter never received the code


def test_db2_for_i_reads_the_key_the_caller_really_passes():
    """
    It read `settings['code']` and the orchestrator - the only caller - passes `funcproc_code`,
    so it answered '' for every routine of every Db2 for i migration. The key is right now, and
    what the connector does with it is said out loud rather than reached by accident.
    """
    import inspect

    from credativ_pg_migrator.connectors.ibm_db2_i_connector import IbmDb2IConnector

    source = inspect.getsource(IbmDb2IConnector.convert_funcproc_code)
    assert "settings.get('funcproc_code'" in source
    assert "code = settings.get('code', '')" not in source


def test_db2_for_i_says_that_it_does_not_convert_a_routine():
    made = connector('ibm_db2_i')
    converted = convert(made)
    assert converted == '', 'Db2 SQL PL handed to PostgreSQL is a syntax error which says nothing'
    written = made.config_parser.levels('WARNING')[0]
    assert 'NOT converted' in written
    assert 'by hand' in written


def test_a_routine_with_no_code_says_nothing():
    made = connector('ibm_db2_i')
    assert convert(made, '   ') == ''
    assert made.config_parser.messages == []


# --------------------------------------------------------------------------------------
# what every source says about the names in its bodies


CONVERTED = ('mssql', 'sybase_ase')
KEEPS_ITS_OWN = ('oracle', 'ibm_db2_zos', 'informix', 'sql_anywhere')


@pytest.mark.parametrize('db_type', CONVERTED)
def test_a_source_which_converts_the_body_declares_nothing(db_type):
    """Their statements go through the statement converter one at a time - measured, not assumed."""
    assert connector(db_type).routine_body_names_not_converted() == ''


@pytest.mark.parametrize('db_type', KEEPS_ITS_OWN)
def test_a_source_whose_body_keeps_its_own_names_says_which_names(db_type):
    note = connector(db_type).routine_body_names_not_converted()
    assert note, db_type
    assert 'folds them to lower case' in note, (
        'the note has to say why lower is the one setting under which the body works')


def test_the_base_connector_declares_nothing_and_offers_the_question():
    from credativ_pg_migrator.database_connector import DatabaseConnector

    assert DatabaseConnector.ROUTINE_BODY_NAMES_NOT_CONVERTED == ''
    assert hasattr(DatabaseConnector, 'routine_body_names_not_converted')


# --------------------------------------------------------------------------------------
# and the run says it where it matters


class ProtocolTables:
    def insert_funcprocs(self, settings):
        pass

    def update_funcproc_status(self, settings):
        pass

    def get_records_remote_objects_substitution(self):
        return []


def test_the_run_warns_only_where_the_setting_makes_the_body_wrong():
    """
    With `lower` an undelimited name folds to what the migration created, so the body works and
    a warning would be noise. With `upper` or `keep` it does not, and the routine is created
    without complaint and fails the first time it is called.
    """
    path = os.path.join(REPO, 'credativ_pg_migrator', 'orchestrator.py')
    with open(path, encoding='utf-8') as handle:
        source = handle.read()
    block = source.split('routine_body_names_not_converted()')[1].split('Checking for remote objects')[0]
    assert "case_handling != 'lower'" in block
    assert 'fails the first time it is called' in block
    assert "print_log_message('DEBUG'" in block, 'under lower it is said at DEBUG and not as a warning'


def test_the_warning_is_not_raised_for_a_routine_which_was_not_converted():
    """A routine which came out empty has no body to have names in."""
    path = os.path.join(REPO, 'credativ_pg_migrator', 'orchestrator.py')
    with open(path, encoding='utf-8') as handle:
        source = handle.read()
    block = source.split('routine_body_names_not_converted()')[1].split('Checking for remote objects')[0]
    assert 'converted_code and str(converted_code).strip()' in block
