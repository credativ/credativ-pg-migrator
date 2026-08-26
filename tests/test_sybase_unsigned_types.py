# SPDX-License-Identifier: GPL-3.0-or-later
"""
The unsigned integers of Sybase ASE, and the range each of them has to keep.

PostgreSQL has no unsigned integer, so every one of them is migrated to the NEXT type up.
Three defects were held in nine lines of the type mapping:

  - `uint` (0 .. 4294967295) was mapped to INTEGER, which holds barely half of it. Everything
    above 2147483647 was refused by the target, and a refused row is dropped by the row-by-row
    retry of the batch - so the table ended up short and only --validate could see it.
  - `unsigned bigint` was mapped to BIGINT, the same defect one size up.
  - `usmallint` and `ubigint` - the names systypes really returns - were not in the mapping at
    all, and an unmapped type falls through to TEXT without a word.

The ranges are the point of this file, so they are asserted as numbers rather than as the type
names they happen to produce today: a mapping which does not hold the maximum value of its
source type fails here, whatever it is changed to.

The analysis behind it is development/SYBASE_NUMERIC_TYPE_MAPPING.md. What this does NOT do is
carry the lower bound over - the target accepts a negative value which the source refuses; that
needs a domain or a CHECK per column and is the option which was not taken.

Nothing here connects to anything.

Run with:  python3 -m pytest tests/test_sybase_unsigned_types.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.connectors.sybase_ase_connector import SybaseASEConnector


## What each type of the source holds, and what each type of the target holds. Both from the
## documentation of the two engines, not from the code under test.
SOURCE_MAX = {
    'tinyint': 255,
    'smallint': 32767,
    'usmallint': 65535,
    'int': 2147483647,
    'uint': 4294967295,
    'bigint': 9223372036854775807,
    'ubigint': 18446744073709551615,
}

TARGET_MAX = {
    'SMALLINT': 32767,
    'INTEGER': 2147483647,
    'BIGINT': 9223372036854775807,
    'NUMERIC(20,0)': 10 ** 20 - 1,
}


@pytest.fixture
def mapping():
    connector = SybaseASEConnector.__new__(SybaseASEConnector)
    return connector.get_types_mapping({'target_db_type': 'postgresql'})


def resolve(mapping, data_type, column_type):
    """
    The fallback chain of planner.convert_table_columns(): data_type, then column_type, then
    basic_data_type, then TEXT. The Sybase connector sets basic_data_type only for a user
    defined type, so a plain system type has none - which is how usmallint reached TEXT.
    """
    coltype = data_type.upper()
    if mapping.get(coltype, 'UNKNOWN').startswith('UNKNOWN'):
        if column_type:
            coltype = column_type.upper()
            coltype = mapping.get(coltype, 'TEXT').upper()
    else:
        coltype = mapping.get(coltype, coltype).upper()
    return coltype


## ---------------------------------------------------------------- the ranges

@pytest.mark.parametrize('data_type', sorted(SOURCE_MAX))
def test_the_target_type_holds_the_whole_range_of_the_source(mapping, data_type):
    """
    'column_type' is 'name(byte length)' as the connector builds it - the shape the report
    named ('uint(4)', 'money(8)').
    """
    byte_length = {'tinyint': 1, 'smallint': 2, 'usmallint': 2, 'int': 4,
                   'uint': 4, 'bigint': 8, 'ubigint': 8}[data_type]
    target = resolve(mapping, data_type, f'{data_type}({byte_length})')

    assert target in TARGET_MAX, f"{data_type} is migrated as {target}, which is not an integer type"
    assert TARGET_MAX[target] >= SOURCE_MAX[data_type], (
        f"{data_type} holds up to {SOURCE_MAX[data_type]} and is migrated as {target}, "
        f"which stops at {TARGET_MAX[target]}")


@pytest.mark.parametrize('data_type', ['usmallint', 'uint', 'ubigint'])
def test_an_unsigned_type_is_never_migrated_as_text(mapping, data_type):
    """
    A type the mapping does not know falls through to TEXT silently. An unsigned smallint
    column reached the target as a text column and every comparison in the application broke
    afterwards.
    """
    assert resolve(mapping, data_type, f'{data_type}(2)') != 'TEXT'


def test_the_reported_case(mapping):
    """uint(4) -> int4 was the report. int4 stops less than halfway."""
    assert resolve(mapping, 'uint', 'uint(4)') == 'BIGINT'


def test_no_integer_type_of_postgresql_holds_an_unsigned_bigint(mapping):
    """
    2**64-1 has twenty digits. BIGINT stops at 9223372036854775807, so NUMERIC is the honest
    answer rather than the convenient one.
    """
    assert mapping['UBIGINT'] == 'NUMERIC(20,0)'
    assert TARGET_MAX['NUMERIC(20,0)'] >= SOURCE_MAX['ubigint']


## ------------------------------------------------------------- both spellings

@pytest.mark.parametrize('catalog_name,sql_name', [
    ('USMALLINT', 'UNSIGNED SMALLINT'),
    ('UINT', 'UNSIGNED INT'),
    ('UBIGINT', 'UNSIGNED BIGINT'),
])
def test_the_catalog_name_and_the_sql_name_agree(mapping, catalog_name, sql_name):
    """
    systypes.name spells them 'uint' / 'ubigint', which is what a column brings; a routine or
    a view writes 'unsigned int'. Both have to reach the same target type - the mapping used
    to carry only the SQL spellings, so the names the columns really use were missing.
    """
    assert mapping[catalog_name] == mapping[sql_name]


## --------------------------------------------------- the routine and view bodies


@pytest.fixture
def connector():
    made = SybaseASEConnector.__new__(SybaseASEConnector)
    return made


@pytest.mark.parametrize('declaration,expected', [
    ('declare @a uint', 'declare @a BIGINT'),
    ('declare @a usmallint', 'declare @a INTEGER'),
    ('declare @a ubigint', 'declare @a NUMERIC(20,0)'),
    ('declare @a unsigned int', 'declare @a BIGINT'),
    ('declare @a unsigned smallint', 'declare @a INTEGER'),
    ('declare @a unsigned bigint', 'declare @a NUMERIC(20,0)'),
    ('declare @a uint(4)', 'declare @a BIGINT'),
    ('declare @a tinyint', 'declare @a SMALLINT'),
])
def test_a_variable_of_a_routine_gets_the_same_type_as_a_column(connector, mapping, declaration, expected):
    assert connector._apply_types_mapping(declaration, mapping) == expected


def test_the_second_pass_over_a_mapped_body_leaves_it_alone(connector, mapping):
    """
    The routine conversion maps the text once and passes over it again with the reduced
    mapping. NUMERIC(20,0) must not be read as a source type on the second pass.
    """
    once = connector._apply_types_mapping('declare @a uint, @b ubigint', mapping)
    twice = connector._apply_types_mapping(once, connector._types_mapping_for_mapped_text(mapping))
    assert once == 'declare @a BIGINT, @b NUMERIC(20,0)'
    assert twice == once


## ------------------------------------------------------------------- the DDL


class DDLConfig:
    def convert_names_case(self, name):
        return (name or '').lower()

    def print_log_message(self, level, message):
        pass

    def get_names_case_handling(self):
        return 'lower'

    def get_source_db_type(self):
        return 'sybase_ase'

    def get_relax_not_null_datetime(self):
        return False

    def get_zero_datetime_data_value(self):
        return None

    def should_map_numeric_1_to_boolean(self, *arguments):
        return False


class DDLProtocolTables:
    def __init__(self):
        self.alterations = []

    def insert_target_column_alteration(self, settings):
        self.alterations.append(dict(settings))

    def get_default_value_details(self, settings):
        return None


def create_table_sql(columns):
    from credativ_pg_migrator.connectors.postgresql_connector import PostgreSQLConnector

    connector = PostgreSQLConnector.__new__(PostgreSQLConnector)
    connector.config_parser = DDLConfig()
    converted = {}
    for index, (name, data_type) in enumerate(columns, start=1):
        converted[index] = {
            'column_name': name, 'data_type': data_type, 'is_nullable': 'YES',
            'is_identity': 'NO', 'character_maximum_length': None,
            'numeric_precision': None, 'numeric_scale': None,
            'basic_character_maximum_length': None, 'basic_numeric_precision': None,
            'basic_numeric_scale': None, 'domain_name': '', 'column_comment': '',
            'column_default_name': '', 'column_default_value': '',
            'replaced_column_default_value': '', 'is_generated_virtual': 'NO',
            'is_generated_stored': 'NO', 'stripped_generation_expression': '',
            'is_hidden_column': 'NO', 'udt_schema': '', 'udt_name': '', 'collation_name': None,
        }
    return connector.get_create_table_sql({
        'target_schema_name': 'app', 'target_table_name': 't', 'target_columns': converted,
        'source_schema_name': 'dbo', 'source_table_name': 'T', 'source_table_id': 1,
        'migrator_tables': DDLProtocolTables(), 'user_collations': {}, 'text_search_objects': {},
    })


def test_the_precision_of_an_unsigned_bigint_reaches_the_create_table(mapping):
    """
    NUMERIC(20,0) travels as one string through the mapping and the DDL builder - the builder
    appends a precision of its own only to a bare NUMERIC, so it must not double it here.
    """
    sql = create_table_sql([('c_ubigint', mapping['UBIGINT'])])
    assert '"c_ubigint" NUMERIC(20,0)' in sql
    assert 'NUMERIC(20,0)(' not in sql


def test_the_widened_integers_reach_the_create_table_as_plain_types(mapping):
    sql = create_table_sql([('c_usmallint', mapping['USMALLINT']),
                            ('c_uint', mapping['UINT']),
                            ('c_tinyint', mapping['TINYINT'])])
    assert '"c_usmallint" INTEGER' in sql
    assert '"c_uint" BIGINT' in sql
    assert '"c_tinyint" SMALLINT' in sql


## ------------------------------------------------ what was NOT changed, on purpose


def test_the_signed_integers_are_untouched(mapping):
    assert mapping['SMALLINT'] == 'SMALLINT'
    assert mapping['INT'] == 'INTEGER'
    assert mapping['INTEGER'] == 'INTEGER'
    assert mapping['BIGINT'] == 'BIGINT'
    assert mapping['INT8'] == 'BIGINT'


def test_money_keeps_its_four_decimal_places(mapping):
    """
    The other half of the same report. MONEY reaches 922337203685477.5807 - fifteen integer
    digits and four decimals - and SMALLMONEY 214748.3647.
    """
    assert mapping['MONEY'] == 'NUMERIC(19,4)'
    assert mapping['SMALLMONEY'] == 'NUMERIC(10,4)'
