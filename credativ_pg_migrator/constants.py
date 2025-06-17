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

class MigratorConstants:
    @staticmethod
    def get_version():
        return '0.8.3'

    @staticmethod
    def get_full_name():
        return 'Database Migration Tool credativ-pg-migrator'

    @staticmethod
    def get_message_levels():
        return ['INFO', 'DEBUG', 'DEBUG2', 'DEBUG3']

    @staticmethod
    def get_default_name():
        return 'migrator'

    @staticmethod
    def get_default_log():
        return f'./{MigratorConstants.get_default_name()}.log'

    @staticmethod
    def get_default_schema():
        return f'{MigratorConstants.get_default_name()}'

    @staticmethod
    def get_tasks_table():
        return 'protocol'

    @staticmethod
    def get_default_indent():
        return '    '

    @staticmethod
    def get_internal_configuration():
        return {
            'migrate_domains_as': 'CHECK CONSTRAINT',
        }

    @staticmethod
    def get_modules():
        return {
            'postgresql': 'connectors.postgresql_connector:PostgreSQLConnector',
            'ibm_db2': 'connectors.ibm_db2_connector:IBMDB2Connector',
            'informix': 'connectors.informix_connector:InformixConnector',
            'mssql': 'connectors.ms_sql_connector:MsSQLConnector',
            'mysql': 'connectors.mysql_connector:MySQLConnector',
            'oracle': 'connectors.oracle_connector:OracleConnector',
            'sql_anywhere': 'connectors.sql_anywhere_connector:SQLAnywhereConnector',
            'sybase_ase': 'connectors.sybase_ase_connector:SybaseASEConnector'
        }

if __name__ == "__main__":
    print("This script is not meant to be run directly")
