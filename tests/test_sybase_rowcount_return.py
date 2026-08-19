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
assert "RETURNS TABLE" in ddl, "Expected RETURNS TABLE clause"
assert "RETURN QUERY" in ddl, "Expected RETURN QUERY statement"
assert "/* RETURN @@rowcount; -- Sybase ASE construct which cannot be used in PostgreSQL */" in ddl, "Expected commented out return statement"
assert "return @@rowcount;" not in ddl.replace("/* RETURN @@rowcount; -- Sybase ASE construct which cannot be used in PostgreSQL */", ""), "Expected plain return statement to be commented out"
print("Unit test passed successfully!")
