# SPDX-License-Identifier: GPL-3.0-or-later
"""
The names inside a converted statement, spelled the way the target has them.

`names_case_handling` decides how the objects of the target are named, and the tables and
columns are created that way. A view is the other half: its defining query names those objects
too, and the name in the query has to be the name the object got. Three of the twelve
connectors did that and nine did not - ms_sql and sybase_ase wrote the identifiers of the
source **in double quotes**, so a view of a migration with `lower` said `FROM "CUSTOMERS"`
while the table is `customers`; the other seven wrote them bare, which is right for `lower`
only because PostgreSQL folds an undelimited name to lower case, and wrong for `upper` and for
`keep` over a source which spells its names in mixed case.

It is one transformation for all of them now, applied where the converted statement is stored.
This file holds it to what it must and must not touch - the schema, the functions, the data
types and the content of a literal are not names this migration gave anything.

Nothing here connects to anything.

Run with:  python3 -m pytest tests/test_identifier_case.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator import identifier_case


def convert_for(case):
    if case == 'lower':
        return lambda name: name.lower() if name else name
    if case == 'upper':
        return lambda name: name.upper() if name else name
    return lambda name: name


def converted(sql, case='lower', source_db_type='sybase_ase'):
    out, ok = identifier_case.convert_identifiers(sql, convert_for(case), source_db_type)
    assert ok, f"the statement could not be read: {sql}"
    return out


## ---------------------------------------------------------------- what is converted

def test_a_table_is_spelled_as_the_target_has_it():
    assert '"customers"' in converted('SELECT id FROM CUSTOMERS')


def test_a_column_is_spelled_as_the_target_has_it():
    assert '"customer_id"' in converted('SELECT CUSTOMER_ID FROM CUSTOMERS')


def test_the_qualifier_of_a_column_follows_its_alias():
    out = converted('SELECT C.CUSTOMER_ID FROM CUSTOMERS C')
    assert '"c"."customer_id"' in out
    assert 'AS "c"' in out


def test_an_output_alias_is_converted():
    """The alias is the name of a column of the view, so it follows the same rule."""
    assert 'AS "full_name"' in converted("SELECT A || B AS Full_Name FROM T")


def test_a_common_table_expression_and_its_use_stay_consistent():
    out = converted('WITH Recent AS (SELECT ID FROM ORDERS) SELECT ID FROM Recent')
    assert '"recent"' in out
    assert 'Recent' not in out


def test_the_column_list_of_a_cte_header_is_converted():
    out = converted('WITH T (Alpha, Beta) AS (SELECT 1, 2) SELECT Alpha FROM T')
    assert '"alpha"' in out and '"beta"' in out


def test_names_are_delimited_afterwards():
    """
    Undelimited is only right for `lower`: with `upper` the table is "CUSTOMERS" and a bare
    CUSTOMERS in the view would be folded to `customers` by PostgreSQL and not be found.
    """
    assert '"CUSTOMERS"' in converted('SELECT id FROM customers', case='upper')


@pytest.mark.parametrize('case,expected', [('lower', '"customers"'), ('upper', '"CUSTOMERS"'),
                                           ('keep', '"CusTomers"')])
def test_every_setting_is_answered(case, expected):
    assert expected in converted('SELECT id FROM "CusTomers"', case=case)


## ---------------------------------------------------------------- what is not converted

def test_the_schema_is_left_exactly_as_it_is():
    """It comes from the configuration, where the user wrote it and means it."""
    out = converted('SELECT id FROM "MigTest"."CUSTOMERS"')
    assert '"MigTest"' in out
    assert '"customers"' in out


def test_the_name_of_a_function_is_not_touched():
    out = converted('SELECT COUNT(*), SUM(o.TOTAL) FROM ORDERS o')
    assert 'COUNT(' in out and 'SUM(' in out


def test_a_string_literal_is_data_and_stays_as_it_is():
    out = converted("SELECT id FROM T WHERE NOTE = 'CUSTOMERS Are Here'")
    assert "'CUSTOMERS Are Here'" in out


def test_a_data_type_is_not_a_name_of_this_migration():
    out = converted('SELECT CAST(X AS VARCHAR(10)) FROM T')
    assert 'VARCHAR' in out.upper()
    assert '"varchar"' not in out


def test_a_keyword_is_not_an_identifier():
    out = converted('SELECT DISTINCT A FROM T ORDER BY A')
    assert 'DISTINCT' in out and 'ORDER BY' in out


## ---------------------------------------------------------------- how the source folds

@pytest.mark.parametrize('source_db_type,expected', [
    ## Db2 and Oracle fold an undelimited name to upper case before they store it, so `keep`
    ## has to keep the name the object really has and not the case the DDL was typed in
    ('oracle', '"CUSTOMERS"'),
    ('ibm_db2_luw', '"CUSTOMERS"'),
    ## Informix and PostgreSQL fold to lower case
    ('informix', '"customers"'),
    ('postgresql', '"customers"'),
    ## the Transact-SQL family, MySQL and SQLite store the name as it was written
    ('sybase_ase', '"Customers"'),
    ('mysql', '"Customers"'),
])
def test_keep_keeps_the_name_the_source_really_has(source_db_type, expected):
    out = converted('SELECT id FROM Customers', case='keep', source_db_type=source_db_type)
    assert expected in out


def test_a_delimited_name_is_never_folded_by_the_source():
    """It was written in quotes, so it is what it says whatever the source does."""
    out = converted('SELECT id FROM "Customers"', case='keep', source_db_type='oracle')
    assert '"Customers"' in out


## ---------------------------------------------------------------- what cannot be read

def test_a_statement_which_cannot_be_read_is_answered_as_it_came_in():
    """
    A name changed by a search and replace inside a text nobody could parse is the kind of
    quiet damage this migrator treats as a bug.
    """
    broken = 'SELECT id FROM CUSTOMERS WHERE 1 = '
    out, ok = identifier_case.convert_identifiers(broken, convert_for('lower'), 'sybase_ase')
    assert ok is False
    assert out == broken


def test_an_empty_statement_is_not_an_error():
    out, ok = identifier_case.convert_identifiers('', convert_for('lower'), 'sybase_ase')
    assert ok is True and out == ''


def test_a_statement_the_parser_does_not_model_is_left_alone():
    text = 'EXEC some_thing @a = 1'
    out, ok = identifier_case.convert_identifiers(text, convert_for('lower'), 'sybase_ase')
    assert ok is False
    assert out == text


## ---------------------------------------------------------------- the whole DDL of a view

def test_a_create_view_keeps_its_schema_and_converts_its_body():
    out = converted('CREATE VIEW "migtest"."v_active" AS '
                    'SELECT C.CUSTOMER_ID FROM CUSTOMERS C WHERE C.IS_ACTIVE = 1')
    assert '"migtest"."v_active"' in out
    assert '"customers"' in out
    assert '"c"."is_active"' in out


def test_the_transformation_is_idempotent():
    once = converted('SELECT C.CUSTOMER_ID FROM CUSTOMERS C')
    assert converted(once) == once


## ---------------------------------------------------------------- every connector, both settings

"""
The matrix which the repair was measured against.

Every connector converts the query of a view its own way - some hand back the query alone,
some the whole CREATE VIEW text, some quote the identifiers and some do not (§2.1 of
development/APPLICATION_QUERIES_CONVERSION_STRATEGY.md measured that). What has to be the same
for all of them is the answer: after the conversion, a table of the target is named the way
names_case_handling named it.

A connector whose driver is not installed is skipped by name rather than passed over silently -
on a machine which has the driver the same test runs against it.
"""

import importlib

from credativ_pg_migrator.constants import MigratorConstants

## the connectors which are handed the whole CREATE VIEW text rather than the query alone
WHOLE_VIEW_TEXT = ('informix', 'sql_anywhere')

VIEW_BODY = "SELECT C.CUSTOMER_ID, C.LAST_NAME FROM CUSTOMERS C WHERE C.IS_ACTIVE = 'Y'"


class ConnectorConfig:
    def __init__(self, case, source_db_type):
        self.case = case
        self.source_db_type = source_db_type

    def print_log_message(self, level, message):
        pass

    def get_names_case_handling(self):
        return self.case

    def get_source_db_type(self):
        return self.source_db_type

    def convert_names_case(self, name):
        if name is None:
            return None
        return name.lower() if self.case == 'lower' else (
            name.upper() if self.case == 'upper' else name)

    def get_target_db_type(self):
        return 'postgresql'

    def get_connectivity(self, direction):
        return {'db_type': 'postgresql'}

    def get_remote_objects_substitution(self):
        return {}

    def get_on_error_action(self):
        return 'stop'

    def get_use_aliases_as_target_names(self):
        return False

    def __getattr__(self, name):
        raise AttributeError(name)


CONNECTORS = sorted(MigratorConstants.get_modules().items())


@pytest.mark.parametrize('case,expected', [('lower', '"customers"'), ('upper', '"CUSTOMERS"')])
@pytest.mark.parametrize('db_type,module_path', CONNECTORS, ids=[c[0] for c in CONNECTORS])
def test_a_view_names_the_table_as_the_target_has_it(db_type, module_path, case, expected):
    module_name, class_name = module_path.split(':')
    try:
        connector_class = getattr(importlib.import_module(module_name), class_name)
    except Exception as e:
        pytest.skip(f"{db_type} needs a driver which is not installed here ({e})")

    config = ConnectorConfig(case, db_type)
    connector = connector_class.__new__(connector_class)
    connector.config_parser = config
    ## the two caches a connector of the Transact-SQL family reads its types from - empty is a
    ## source which has no user defined type
    connector._udt_cache = {}
    connector._udt_map_cache = {}
    connector.source_or_target = 'source'

    view_code = (f"CREATE VIEW MIGTEST.V_ACTIVE_CUSTOMERS AS {VIEW_BODY}"
                 if db_type in WHOLE_VIEW_TEXT else VIEW_BODY)
    converted_view = connector.convert_view_code({
        'view_code': view_code,
        'source_schema_name': 'MIGTEST',
        'target_schema_name': 'migtest',
        'target_view_name': config.convert_names_case('V_ACTIVE_CUSTOMERS'),
        'target_db_type': 'postgresql',
        'view_type': 'VIEW',
    })

    final, ok = identifier_case.convert_identifiers(
        str(converted_view), config.convert_names_case, db_type)
    assert ok, f"{db_type}: the converted view could not be read as PostgreSQL: {converted_view}"
    assert expected in final, f"{db_type} with {case}: {final}"
    ## the schema of the target is never converted - it is used as the configuration writes it
    assert '"MIGTEST"' not in final


## ---------------------------------------------------------------- the statements of an application

"""
The query conversion has the same half of the problem the views had: a statement it converts
names the tables and columns of the target, so they have to be spelled the way the migration
spelled them. A statement of a Sybase ASE or MS SQL Server application came out as
`SELECT "C"."ID" FROM "CUSTOMERS"` - the identifiers of the source, in quotes - while the
migration created `customers`, and the target test answered `relation "CUSTOMERS" does not
exist`. Same repair, same shared transformation.
"""


def test_the_bind_parameters_are_not_identifiers():
    """
    $1..$n is what PostgreSQL is asked with, and the round trip puts the marker of the
    application back afterwards. Neither may be touched by the case of a name.
    """
    out = converted('SELECT "C"."ID" FROM "CUSTOMERS" AS "C" WHERE "C"."NAME" LIKE $1')
    assert 'LIKE $1' in out
    assert '"$1"' not in out


def test_the_whole_round_trip_of_a_statement_keeps_its_parameters():
    from credativ_pg_migrator.query_conversion import parameters as parameters_module
    binds, _warnings = parameters_module.extract(
        'SELECT C.ID FROM CUSTOMERS C WHERE C.NAME LIKE ?', 'auto')
    ## what the connector gives back, with the marker carried through as an identifier
    connector_answer = ('SELECT "C"."ID" FROM "CUSTOMERS" AS "C" '
                        'WHERE "C"."NAME" LIKE "cpgm_bind_param_1"')
    numbered = binds.to_numbered(connector_answer)
    final, ok = identifier_case.convert_identifiers(numbered, convert_for('lower'), 'sybase_ase')
    assert ok
    assert final == ('SELECT "c"."id" FROM "customers" AS "c" WHERE "c"."name" LIKE $1')
    restored, warnings = binds.restore(final, 'original')
    assert restored.endswith('LIKE ?')
    assert not warnings
