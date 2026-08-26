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
        return '0.16.0rc2'

    @staticmethod
    def get_full_name():
        return 'Database Migration Tool credativ-pg-migrator'

    @staticmethod
    def get_application_name():
        return 'credativ-pg-migrator'

    ## Message levels, from the quietest threshold to the noisiest, and their severity.
    ## A message is written when its severity is at least the severity of the level the
    ## run was started with, which is the way --log-level behaves everywhere else:
    ## the default INFO shows ERROR, WARNING and INFO, and each DEBUG step adds more.
    MESSAGE_LEVEL_SEVERITIES = {
        'ERROR': 50,
        'WARNING': 40,
        'INFO': 30,
        'DEBUG': 20,
        'DEBUG2': 10,
        'DEBUG3': 0,
    }

    @staticmethod
    def get_message_levels():
        """The accepted --log-level values, quietest first."""
        return list(MigratorConstants.MESSAGE_LEVEL_SEVERITIES.keys())

    @staticmethod
    def get_message_level_severity(level):
        """
        The severity of a message level, or None when the name is not a level.
        """
        if level is None:
            return None
        return MigratorConstants.MESSAGE_LEVEL_SEVERITIES.get(str(level).strip().upper())

    ## What the validation of one table can end in. Three outcomes and not two: a table for
    ## which no check could run - no primary key, no checksum on that source, every check
    ## switched off - is not a table which passed. "We could not tell" is not "it is correct",
    ## and a report which cannot say the difference cannot be used as evidence. P2-2.
    VALIDATION_PASSED = 'PASSED'
    VALIDATION_FAILED = 'FAILED'
    VALIDATION_NOT_VALIDATED = 'NOT VALIDATED'

    ## The mark each outcome gets in the Status column of the validation summary.
    VALIDATION_OUTCOME_MARKS = {
        VALIDATION_PASSED: 'PASS',
        VALIDATION_FAILED: 'X',
        VALIDATION_NOT_VALIDATED: '?',
    }

    @staticmethod
    def get_validation_outcomes():
        """The three outcomes, from the best to the one which says nothing."""
        return (MigratorConstants.VALIDATION_PASSED,
                MigratorConstants.VALIDATION_FAILED,
                MigratorConstants.VALIDATION_NOT_VALIDATED)

    @staticmethod
    def get_validation_outcome_mark(outcome):
        """The mark of an outcome in the summary; '-' for a row which has none recorded."""
        return MigratorConstants.VALIDATION_OUTCOME_MARKS.get(outcome, '-')

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
    def get_default_data_source():
        return 'SOURCE TABLE'

    @staticmethod
    def get_internal_configuration():
        return {
            'migrate_domains_as': 'CHECK CONSTRAINT',
        }

    @staticmethod
    def get_modules():
        return {
            'postgresql': 'credativ_pg_migrator.connectors.postgresql_connector:PostgreSQLConnector',
            'ibm_db2_luw': 'credativ_pg_migrator.connectors.ibm_db2_luw_connector:IbmDb2LuwConnector',
            'ibm_db2_zos': 'credativ_pg_migrator.connectors.ibm_db2_zos_connector:IbmDb2ZosConnector',
            'ibm_db2_i': 'credativ_pg_migrator.connectors.ibm_db2_i_connector:IbmDb2IConnector',
            'informix': 'credativ_pg_migrator.connectors.informix_connector:InformixConnector',
            'mssql': 'credativ_pg_migrator.connectors.ms_sql_connector:MsSQLConnector',
            'mysql': 'credativ_pg_migrator.connectors.mysql_connector:MySQLConnector',
            'mariadb': 'credativ_pg_migrator.connectors.mariadb_connector:MariaDBConnector',
            'oracle': 'credativ_pg_migrator.connectors.oracle_connector:OracleConnector',
            'sql_anywhere': 'credativ_pg_migrator.connectors.sql_anywhere_connector:SQLAnywhereConnector',
            'sqlite': 'credativ_pg_migrator.connectors.sqlite_connector:SQLiteConnector',
            'sybase_ase': 'credativ_pg_migrator.connectors.sybase_ase_connector:SybaseASEConnector'
        }

if __name__ == "__main__":
    print("This script is not meant to be run directly")
