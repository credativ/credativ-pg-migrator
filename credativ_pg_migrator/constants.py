# credativ-pg-migrator
# Copyright (C) 2025 credativ GmbH
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

MIGRATOR_VERSION = '0.7.6'
MIGRATOR_FULL_NAME = 'Database Migration Tool credativ-pg-migrator'

## Internal defaults used by the migrator
## Do not change these values unless !
MIGRATOR_DEFAULT_NAME = 'migrator'
MIGRATOR_DEFAULT_LOG = f'./{MIGRATOR_DEFAULT_NAME}.log'
MIGRATOR_DEFAULT_SCHEMA = f'{MIGRATOR_DEFAULT_NAME}'
MIGRATOR_TASKS_TABLE = 'protocol'
MIGRATOR_DEFAULT_INDENT='    '
MIGRATOR_INTERNAL_CONFIGURATION = {
	'migrate_domains_as': 'CHECK CONSTRAINT',
}

## Modules of database connectors
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

if __name__ == "__main__":
    print("This script is not meant to be run directly")
