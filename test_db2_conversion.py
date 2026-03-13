from credativ_pg_migrator.connectors.ibm_db2_zos_connector import IBMDB2zOSConnector
class MockConfigParser:
    def print_log_message(self, level, msg): pass
    def convert_names_case(self, name): return name.lower()
    def get_remote_objects_substitution(self): return {}

config = MockConfigParser()
connector = IBMDB2zOSConnector(config, 'source')
connector.target_db_type = 'postgresql'

test_view = """CREATE VIEW "ENTW102"."VZPANSCHRIFT" (COL1, COL2) AS
SELECT
  *
FROM
  "ENTW102"."TWW6001N"
WHERE
  CURRENT SQLID IN('TRUBOZ1', 'PRUBOZ1', 'PRUBOZ2', '$USRTZP', 'PDVZP')
  OR KZZP = 'N'"""

settings = {
    'view_code': test_view,
    'source_schema_name': 'ENTW102',
    'target_schema_name': 'entw102',
    'target_db_type': 'postgresql',
    'target_view_name': 'vzpanschrift'
}

print(connector.convert_view_code(settings))
