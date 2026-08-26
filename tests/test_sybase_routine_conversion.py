# SPDX-License-Identifier: GPL-3.0-or-later
"""
The routine conversion of Sybase ASE - the header it builds and the statements it converts.

A routine is converted by `TsqlParser`, not by the statement path of sqlglot alone: the parser
takes the body apart and hands each statement of it to the connector's statement converter.
Three defects of that pipeline are held here, all of them found in one procedure of a
migration:

  - the routine was created in the schema the SOURCE wrote in its own header
    ('CREATE PROCEDURE dbo.p') instead of in the schema of the target, so PostgreSQL answered
    'schema "dbo" does not exist' and the routine was not created at all;

  - '@variables' were handed to the statement converter, which reads them as PARAMETERS of the
    statement and writes them as the '$name' of PostgreSQL - 'WHERE "customer_id" = $cid',
    'SELECT $OptIn = 1' - which is not PL/pgSQL, and which the rename of Pass 9 could no longer
    repair because the '@' it looks for was gone;

  - the rename of Pass 9 ran over the whole line, so an '@' inside a string literal was renamed
    as if it were a variable and 'admin@example.com' was written into the target as
    'adminlocvar_example.com'.

Nothing here connects to anything. Note the stub below: it carries every accessor the routine
conversion really calls, and `convert()` asserts that no statement fell back to its original
text - an incomplete stub makes the statement converter raise, the parser keeps the source text
and a test then asserts something about a conversion which never ran.

Run with:  python3 -m pytest tests/test_sybase_routine_conversion.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.connectors.sybase_ase_connector import SybaseASEConnector


class Config:
    """Every accessor the routine conversion of Sybase ASE reads - see the note above."""

    args = None

    def __init__(self, names_case='lower', target_schema='ccd'):
        self.messages = []
        self.names_case = names_case
        self.target_schema = target_schema

    def print_log_message(self, level, message):
        self.messages.append((level, str(message)))

    def levels(self, level):
        return [message for written, message in self.messages if written == level]

    def get_source_db_type(self):
        return 'sybase_ase'

    def get_target_db_type(self):
        return 'postgresql'

    def get_connectivity(self, direction):
        return {'db_type': 'postgresql'}

    def get_source_schema(self):
        return 'dbo'

    def get_target_schema(self):
        return self.target_schema

    def get_names_case_handling(self):
        return self.names_case

    def convert_names_case(self, name):
        if name is None:
            return None
        if self.names_case == 'lower':
            return name.lower()
        if self.names_case == 'upper':
            return name.upper()
        return name

    def get_remote_objects_substitution(self):
        return {}

    def get_data_types_substitution(self):
        return []

    def get_default_values_substitution(self):
        return []

    def indent_code(self, code, *arguments, **keywords):
        return code


def convert(code, name='p_test', target_schema='ccd', tables=('customers', 'audit')):
    """The routine converted, with the guard that every statement really went through."""
    connector = SybaseASEConnector.__new__(SybaseASEConnector)
    connector.config_parser = Config(target_schema=target_schema)
    connector._udt_cache = {}
    connector._user_messages = {}

    converted = connector.convert_funcproc_code({
        'funcproc_code': code,
        'funcproc_name': name,
        'source_schema_name': 'dbo',
        'target_schema_name': target_schema,
        'target_db_type': 'postgresql',
        'table_list': list(tables),
        'view_list': [],
    }) or ''

    fell_back = [message for _, message in connector.config_parser.messages
                 if 'Failed to convert' in message or 'has no attribute' in message]
    assert not fell_back, (
        f"a statement did not reach the statement converter, so this test would assert "
        f"something about a conversion which never ran: {fell_back}")
    return converted


## ---------------------------------------------------- the schema the routine is created in

SCHEMA_QUALIFIED = """create procedure dbo.p_test
as
begin
    declare @x int
    select @x = 1
    return
end
"""


def test_a_schema_qualified_routine_is_created_in_the_schema_of_the_target():
    """
    'CREATE PROCEDURE dbo.p_test' used to be created as '"dbo"."p_test"' - the qualifier of the
    source, which the target does not have. Everything else about the routine already used the
    target schema: the protocol records it there and the COMMENT ON of the orchestrator is
    written for it.
    """
    converted = convert(SCHEMA_QUALIFIED)
    assert 'CREATE OR REPLACE FUNCTION "ccd"."p_test"' in converted
    assert '"dbo"' not in converted


def test_a_routine_without_a_qualifier_is_created_in_the_schema_of_the_target():
    converted = convert(SCHEMA_QUALIFIED.replace('dbo.p_test', 'p_test'))
    assert 'CREATE OR REPLACE FUNCTION "ccd"."p_test"' in converted


def test_a_routine_written_with_its_database_in_front_of_it_is_created_in_the_target():
    """'database.owner.routine' - three parts, and the name is the last of them."""
    converted = convert(SCHEMA_QUALIFIED.replace('dbo.p_test', 'legacydb.dbo.p_test'))
    assert 'CREATE OR REPLACE FUNCTION "ccd"."p_test"' in converted


def test_the_name_of_a_three_part_routine_is_not_its_owner():
    """
    The name used to be read as parts[1] of the header, so 'legacydb.dbo.p_test' made a routine
    called 'dbo' whenever the caller passed no name of its own.
    """
    converted = convert(SCHEMA_QUALIFIED.replace('dbo.p_test', 'legacydb.dbo.p_test'), name='')
    assert 'CREATE OR REPLACE FUNCTION "ccd"."p_test"' in converted


def test_the_dropped_qualifier_of_the_source_is_written_into_the_log():
    connector = SybaseASEConnector.__new__(SybaseASEConnector)
    connector.config_parser = Config()
    connector._udt_cache = {}
    connector._user_messages = {}
    connector.convert_funcproc_code({
        'funcproc_code': SCHEMA_QUALIFIED, 'funcproc_name': 'p_test',
        'source_schema_name': 'dbo', 'target_schema_name': 'ccd',
        'target_db_type': 'postgresql', 'table_list': [], 'view_list': [],
    })
    written = ' '.join(connector.config_parser.levels('DEBUG'))
    assert "is written 'dbo.p_test' in the source" in written


## ---------------------------------------------------------------- the variables of the body

WITH_VARIABLES = """create procedure dbo.p_test
(
    @cid int
)
as
begin
    declare @n int
    update dbo.customers set last_seen = getdate() where customer_id = @cid
    delete from dbo.audit where id = @cid
    insert into dbo.audit (id, note) values (@cid, 'x')
    select @n = count(*) from dbo.customers where customer_id = @cid
    return @n
end
"""


def test_a_variable_of_the_body_is_never_written_as_a_positional_parameter():
    """
    Every parser of the family reads '@cid' as a parameter of the statement and writes it as
    '$cid' for PostgreSQL. A routine full of '$cid' is not PL/pgSQL, and Pass 9 could not
    rename what no longer carried an '@'.
    """
    converted = convert(WITH_VARIABLES)
    assert '$cid' not in converted
    assert '$n' not in converted
    assert 'locvar_cid' in converted


@pytest.mark.parametrize('statement', [
    'UPDATE "ccd"."customers" SET "last_seen" = CURRENT_TIMESTAMP WHERE "customer_id" = locvar_cid;',
    'DELETE FROM "ccd"."audit" WHERE "id" = locvar_cid;',
    'INSERT INTO "ccd"."audit" ("id", "note") VALUES (locvar_cid, \'x\');',
])
def test_every_kind_of_statement_keeps_its_variable_and_gets_the_target_schema(statement):
    assert statement in convert(WITH_VARIABLES)


def test_a_variable_is_written_unquoted_so_that_it_is_the_declared_one():
    """
    The conversion quotes the names it does not know, and a quoted name is not folded:
    '"locvar_Cid"' is a different name from the 'locvar_Cid' of the DECLARE block, which
    PostgreSQL folds to lower case.
    """
    converted = convert(WITH_VARIABLES)
    assert '"locvar_cid"' not in converted.lower()


def test_a_cursor_query_keeps_its_variable_too():
    converted = convert("""create procedure dbo.p_test
(
    @cid int
)
as
begin
    declare c_x cursor for select last_name from dbo.customers where customer_id = @cid
    open c_x
    close c_x
    return
end
""")
    assert 'FROM "ccd"."customers" WHERE "customer_id" = locvar_cid' in converted
    assert '$cid' not in converted


## ------------------------------------------------------------ the SELECT which assigns

MULTIPLE_ASSIGNMENTS = """create procedure dbo.p_test
as
begin
    declare
        @OptIn numeric(10,0),
        @OptOut numeric(10,0),
        @Marker univarchar(10)

    select
        @OptIn = 1,
        @OptOut = 2,
        @Marker = '10'

    return
end
"""


def test_a_select_which_assigns_becomes_the_assignments_of_plpgsql():
    """
    'SELECT @a = 1, @b = 2' is not a query - it is how Transact-SQL assigns. It used to be sent
    to the statement converter, which made 'SELECT $OptIn = 1, $OptOut = 2' of it, and Pass 8
    then no longer recognised the assignment it was there to convert.
    """
    converted = convert(MULTIPLE_ASSIGNMENTS, tables=())
    assert 'locvar_OptIn := 1;' in converted
    assert 'locvar_OptOut := 2;' in converted
    assert "locvar_Marker := '10';" in converted
    assert 'SELECT $' not in converted


def test_an_assignment_which_reads_a_table_becomes_a_select_into():
    """
    An assignment with a FROM clause is a query and is the 'SELECT ... INTO' of PL/pgSQL - and
    its query part still needs the conversion, the schema of the target included.
    """
    converted = convert("""create procedure dbo.p_test
as
begin
    declare @n int
    select @n = count(*) from dbo.customers
    return @n
end
""")
    assert 'SELECT COUNT(*) INTO locvar_n FROM "ccd"."customers";' in converted


def test_the_values_of_an_assignment_are_converted_as_well():
    """
    The functions of the source stand in the values of an assignment like anywhere else, and
    the statement converter is what knows them.
    """
    converted = convert("""create procedure dbo.p_test
as
begin
    declare @now datetime, @who varchar(50)
    select @now = getdate(), @who = user_name()
    return
end
""", tables=())
    assert 'getdate()' not in converted.lower()
    assert 'locvar_now := CURRENT_TIMESTAMP;' in converted


## ------------------------------------------------------------------- what is not a variable


def test_an_at_sign_inside_a_string_literal_is_data():
    """
    The rename of Pass 9 ran over the whole line, so 'mail to admin@example.com' was written
    into the target as 'mail to adminlocvar_example.com'. A routine which rewrites the text it
    inserts is worse than one which does not compile.
    """
    converted = convert("""create procedure dbo.p_test
(
    @who varchar(50)
)
as
begin
    insert into dbo.audit (note, who) values ('mail to admin@example.com', @who)
    return
end
""")
    assert "'mail to admin@example.com'" in converted
    assert 'adminlocvar_example.com' not in converted
    assert 'locvar_who' in converted
