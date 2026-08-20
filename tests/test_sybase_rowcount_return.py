import sys
import os

# Add the repository root to PYTHONPATH
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from credativ_pg_migrator.connectors.sybase_ase_connector import SybaseASEConnector

class DummyConfig:
    def print_log_message(self, level, msg):
        pass
    def get_data_types_substitution(self):
        return []
    def get_variable_types_mapping(self):
        return []
    def convert_names_case(self, name):
        return name
    def get_target_db_type(self):
        return 'postgresql'
    def get_on_error_action(self):
        return 'stop'
    def get_log_file(self):
        return 'test.log'
    def get_connectivity(self, direction):
        return 'odbc'
    def get_connect_string(self, direction):
        return 'dummy'
    def get_remote_objects_substitution(self):
        return []

connector = SybaseASEConnector(DummyConfig(), 'source')

def get_types_mapping(args):
    return {'char': 'char', 'varchar': 'varchar'}
connector.get_types_mapping = get_types_mapping
def get_udt_map(args):
    return {}
connector._get_udt_codes_mapping = get_udt_map

sql = """create proc storeid_proc
@stor_id	char(4)
as
select stor_name,
        stor_id,
        stor_address,
        city,
        state,
        postalcode,
        country
from stores
where stor_id = @stor_id
return @@rowcount
"""

settings = {
    'funcproc_code': sql,
    'funcproc_name': 'storeid_proc',
    'target_db_type': 'postgresql',
    'target_schema_name': 'public',
}
ddl = connector.convert_funcproc_code(settings)
print(ddl)

# Assertions to verify correct commenting out
#
# The routine selects rows and returns a status code at the same time. The rows are what
# the function answers with (RETURNS TABLE / RETURN QUERY); the status code cannot be
# returned next to them - PostgreSQL refuses "RETURN cannot have a parameter in function
# returning set" - so it is commented out instead of being emitted as invalid code.
#
# '@@rowcount' itself is translated before this: it becomes a declared variable filled by
# GET DIAGNOSTICS, so the commented out statement names that variable and not the global
# variable of Sybase.
commented_return = "/* RETURN locvar_rowcount; -- Sybase ASE construct which cannot be used in PostgreSQL */"
assert "RETURNS TABLE" in ddl, "Expected RETURNS TABLE clause"
assert "RETURN QUERY" in ddl, "Expected RETURN QUERY statement"
assert "GET DIAGNOSTICS locvar_rowcount = ROW_COUNT;" in ddl, "Expected @@rowcount to be translated to GET DIAGNOSTICS"
assert commented_return in ddl, "Expected commented out return statement"
assert "@@rowcount" not in ddl, "Expected no untranslated Sybase global variable"
rest = ddl.replace(commented_return, "")
assert "return locvar_rowcount;" not in rest.lower(), "Expected plain return statement to be commented out"
print("Unit test passed successfully!")
