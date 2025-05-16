MIGRATOR_VERSION = '0.7.1'
MIGRATOR_DEFAULT_NAME = 'migrator'
MIGRATOR_DEFAULT_LOG = f'./{MIGRATOR_DEFAULT_NAME}.log'
MIGRATOR_DEFAULT_SCHEMA = f'{MIGRATOR_DEFAULT_NAME}'
MIGRATOR_TASKS_TABLE = 'protocol'
MIGRATOR_DEFAULT_INDENT='    '
MIGRATOR_MODULES = {
					'postgresql': 'postgresql_connector:PostgreSQLConnector',
					'ibm_db2': 'ibm_db2_connector:IBMDB2Connector',
					'informix': 'informix_connector:InformixConnector',
					'mssql': 'ms_sql_connector:MsSQLConnector',
					'mysql': 'mysql_connector:MySQLConnector',
					'oracle': 'oracle_connector:OracleConnector',
					'sql_anywhere': 'sql_anywhere_connector:SQLAnywhereConnector',
					'sybase_ase': 'sybase_ase_connector:SybaseASEConnector'
					}
