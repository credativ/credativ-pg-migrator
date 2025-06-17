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

import yaml
from credativ_pg_migrator.constants import MigratorConstants

class ConfigParser:
    def __init__(self, args, logger):
        self.args = args
        self.config = self.load_config(args.config)
        self.logger = logger
        self.validate_config()

    def load_config(self, config_file):
        """Load the configuration file."""
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)

    def validate_config(self):

        ## //TODO source.schema or source.owner is required - not both

        names_case_handling = self.get_names_case_handling().lower()
        if names_case_handling not in ['lower', 'upper', 'keep']:
            raise ValueError(f"Invalid names_case_handling in the config file: {names_case_handling}. Must be one of 'lower', 'upper', or 'keep'.")

        include_tables = self.config['include_tables']
        if (include_tables is not None and type(include_tables) is str and include_tables.lower() != 'all'):
            # and type(include_tables) is not list):
            raise ValueError("When include_tables is used, it must be a list of names or regex patterns")

        data_types_substitution = self.get_data_types_substitution()
        if isinstance(data_types_substitution, list):
            for entry in data_types_substitution:
                if not isinstance(entry, (list, tuple)) or len(entry) != 5:
                    raise ValueError("Please update your config file. Each entry in data_types_substitution must have 5 elements - [table_name, column_name, source_type, target_type, comment].")

        return True

    ## Databases
    def get_db_config(self, source_or_target):
        return self.config[source_or_target]

    def get_db_type(self, source_or_target):
        if source_or_target not in ['source', 'target']:
            raise ValueError(f"Invalid source_or_target: {source_or_target}")
        return self.config[source_or_target]['type']

    def get_source_config(self):
        return self.config['source']

    def get_source_db_name(self):
        return self.get_source_config()['database']

    def get_source_schema(self):
        source_config = self.get_source_config()
        return source_config.get('schema', source_config.get('owner', 'public'))

    def get_source_owner(self):
        return self.get_source_schema()

    def get_source_db_type(self):
        return self.config['source']['type']

    def get_connectivity(self, source_or_target):
        return self.config[source_or_target].get('connectivity', None)

    def get_source_connectivity(self):
        return self.get_connectivity('source').lower()

    def get_target_config(self):
        return self.config['target']

    def get_target_db_type(self):
        return self.config['target']['type']

    def get_target_db_name(self):
        return self.get_target_config()['database']

    def get_target_schema(self):
        target_config = self.get_target_config()
        return target_config.get('schema', target_config.get('owner', 'public'))

    def get_connect_string(self, source_or_target):
        if source_or_target not in ['source', 'target']:
            raise ValueError(f"Invalid source_or_target: {source_or_target}")
        connectivity = self.get_connectivity(source_or_target)
        db_config = self.config[source_or_target]
        if db_config['type'] == 'postgresql':
            if connectivity == 'native' or connectivity is None:
                return f"""postgres://{db_config['username']}:{db_config['password']}@{db_config.get('host', 'localhost')}:{db_config['port']}/{db_config['database']}?sslmode={db_config.get('sslmode', 'prefer')}"""
                # return f"""dbname="{db_config['database']}" user="{db_config['username']}" password="{db_config['password']}" host="{db_config.get('host', 'localhost')}" port="{db_config['port']}" sslmode={db_config.get('sslmode', 'prefer')}"""
            else:
                raise ValueError(f"Unsupported Postgres connectivity: {connectivity}")
        elif db_config['type'] == 'informix':
            if connectivity == 'odbc':
                return f"DRIVER={db_config['odbc']['driver']};SERVER={db_config['server']};UID={db_config['username']};PWD={db_config['password']}"
            elif connectivity == 'jdbc':
                # ;user={db_config['username']};password={db_config['password']}
                return f"jdbc:informix-sqli://{db_config['host']}:{db_config['port']}/{db_config['database']}:INFORMIXSERVER={db_config['server']}"
            else:
                raise ValueError(f"Unsupported Informix connectivity: {connectivity}")
        elif db_config['type'] == 'sybase_ase':
            if connectivity == 'odbc':
                return f"DRIVER={db_config['odbc']['driver']};SERVER={db_config['host']};PORT={db_config['port']};DATABASE={db_config['database']};UID={db_config['username']};PWD={db_config['password']};TDS_Version=8.0"
            elif connectivity == 'jdbc':
                return f"jdbc:sybase:Tds:{db_config['host']}:{db_config['port']}/{db_config['database']}"
            else:
                raise ValueError(f"Unsupported Sybase ASE connectivity: {connectivity}")
        elif db_config['type'] == 'mssql':
            if connectivity == 'odbc':
                return f"DRIVER={db_config['odbc']['driver']};SERVER={db_config['host']};PORT={db_config['port']};DATABASE={db_config['database']};UID={db_config['username']};PWD={db_config['password']}"
            elif connectivity == 'jdbc':
                return f"jdbc:sqlserver://{db_config['host']}:{db_config['port']};databaseName={db_config['database']};user={db_config['username']};password={db_config['password']}"
            else:
                raise ValueError(f"Unsupported MSSQL connectivity: {connectivity}")
        elif db_config['type'] == 'mysql':
            if connectivity == 'odbc':
                return f"DRIVER={db_config['odbc']['driver']};SERVER={db_config['host']};PORT={db_config['port']};DATABASE={db_config['database']};UID={db_config['username']};PWD={db_config['password']}"
            elif connectivity == 'jdbc':
                return f"jdbc:mysql://{db_config['host']}:{db_config['port']}/{db_config['database']}?user={db_config['username']}&password={db_config['password']}"
            elif connectivity == 'native':
                return f"mysql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            else:
                raise ValueError(f"Unsupported MySQL connectivity: {connectivity}")
        elif db_config['type'] == 'ibm_db2':
            if connectivity == 'native':
                return f"DATABASE={db_config['database']};HOSTNAME={db_config['host']};PORT={db_config['port']};PROTOCOL=TCPIP;UID={db_config['username']};PWD={db_config['password']}"
            else:
                raise ValueError(f"Unsupported IBM DB2 connectivity: {connectivity}")
        elif db_config['type'] == 'sql_anywhere':
            if connectivity == 'native':
                # return f"DSN={db_config['dsn']};UID={db_config['username']};PWD={db_config['password']}"
                # return f"host={db_config['host']};port={db_config['port']};database={db_config['database']};uid={db_config['username']};pwd={db_config['password']}"
                return f"HOST={db_config['host']};PORT={db_config['port']};UID={db_config['username']};PWD={db_config['password']};DBN={db_config['database']}"
            # connection_string = "host=localhost:2639;uid=dba;pwd=sql;dbn=isovision"
            elif connectivity == 'odbc':
                return f"DRIVER={'{'+db_config['odbc']['driver']+'}'};SERVER={db_config['host']};PORT={db_config['port']};UID={db_config['username']};PWD={db_config['password']};DBN={db_config['database']}"
            # elif connectivity == 'jdbc':
            #     return f"jdbc:sqlanywhere://{db_config['host']}:{db_config['port']}/{db_config['database']};UID={db_config['username']};PWD={db_config['password']}"
            else:
                raise ValueError(f"Unsupported SQL Anywhere connectivity: {connectivity}")
        elif db_config['type'] == 'oracle':
            # if connectivity == 'native':
            #     return f"oracle://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            if connectivity == 'native':
                return f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
            elif connectivity == 'jdbc':
                return f"jdbc:oracle:thin:@{db_config['host']}:{db_config['port']}:{db_config['database']}"
            else:
                raise ValueError(f"Unsupported Oracle connectivity: {connectivity}")
        else:
            raise ValueError(f"Unsupported database type: {db_config['type']}")

    def get_source_connect_string(self):
        return self.get_connect_string('source')

    def get_target_connect_string(self):
        return self.get_connect_string('target')

    def get_system_catalog(self):
        return self.config.get('system_catalog', 'NONE').upper()

    ## Migrator
    def get_migrator_config(self):
        return self.config.get('migrator', {})

    def get_migrator_db_type(self):
        return self.get_migrator_config().get('type', None)

    def get_migrator_schema(self):
        return self.get_migrator_config().get('schema', MigratorConstants.get_default_schema())

    def get_migration_settings(self):
        return self.config['migration']

    def get_tables_config(self):
        return self.config.get('tables', []) # Default to empty list if not specified

    def get_protocol_name(self):
        return MigratorConstants.get_tasks_table()

    def get_protocol_name_main(self):
        return f"{self.get_protocol_name()}_main"

    def get_protocol_name_user_defined_types(self):
        return f"{self.get_protocol_name()}_user_defined_types"

    def get_protocol_name_domains(self):
        return f"{self.get_protocol_name()}_domains"

    def get_protocol_name_default_values(self):
        return f"{self.get_protocol_name()}_defaults"

    def get_protocol_name_target_columns_alterations(self):
        return f"{self.get_protocol_name()}_target_cols_alt"

    def get_protocol_name_new_objects(self):
        return f"{self.get_protocol_name()}_new_objects"

    def get_protocol_name_tables(self):
        return f"{self.get_protocol_name()}_tables"

    def get_protocol_name_pk_ranges(self):
        return f"{self.get_protocol_name()}_pk_ranges"

    def get_protocol_name_data_migration(self):
        return f"{self.get_protocol_name()}_data_migration"

    def get_protocol_name_indexes(self):
        return f"{self.get_protocol_name()}_indexes"

    def get_protocol_name_constraints(self):
        return f"{self.get_protocol_name()}_constraints"

    def get_protocol_name_funcprocs(self):
        return f"{self.get_protocol_name()}_funcprocs"

    def get_protocol_name_sequences(self):
        return f"{self.get_protocol_name()}_sequences"

    def get_protocol_name_triggers(self):
        return f"{self.get_protocol_name()}_triggers"

    def get_protocol_name_views(self):
        return f"{self.get_protocol_name()}_views"

    def get_data_types_substitution(self):
        return self.config.get('data_types_substitution', {})

    def get_default_values_substitution(self):
        implicit_substitutions = []
        from_config_file = self.config.get('default_values_substitution', {})
        if self.get_source_db_type() == 'sybase_ase':
            implicit_substitutions = [
                # Use regex patterns for matching default values
                # ["", "", r'(?i)(?:"getdate"|getdate)\s*\(\s*\)', "statement_timestamp()"],
                # ["", "", r'(?i)(?:"db_name"|db_name)\s*\(\s*\)', "current_database()"],
                # ["", "", r'(?i)(?:"user_name"|user_name)\s*\(\s*\)', "session_user"],
                ["", "BIT", r"^0$", "false"],
                ["", "BIT", r"^1$", "true"],
                # ["", r"(?i).*datetime.*", r"^0$", "current_timestamp"],
            ]
        # Merge substitutions as a list of lists
        merged_substitutions = []
        if isinstance(from_config_file, list):
            merged_substitutions.extend(from_config_file)
        elif isinstance(from_config_file, dict):
            # If from_config_file is a dict, convert its items to list of lists
            merged_substitutions.extend([list(item) for item in from_config_file.items()])
        merged_substitutions.extend(implicit_substitutions)
        return merged_substitutions

    def get_data_migration_limitation(self):
        return self.config.get('data_migration_limitation', {})

    def get_remote_objects_substitution(self):
        return self.config.get('remote_objects_substitution', {})

    ## Migration settings
    def should_drop_schema(self):
        return self.config.get('migration', {}).get('drop_schema', False)

    def should_drop_tables(self):
        return self.config.get('migration', {}).get('drop_tables', False) # Default to False

    def should_truncate_tables(self):
        return self.config.get('migration', {}).get('truncate_tables', False)

    def should_create_tables(self):
        return self.config.get('migration', {}).get('create_tables', False)

    def should_migrate_data(self):
        return self.config.get('migration', {}).get('migrate_data', False)

    def should_migrate_indexes(self):
        return self.config.get('migration', {}).get('migrate_indexes', False) # Default to False

    def should_migrate_constraints(self):
        return self.config.get('migration', {}).get('migrate_constraints', False) # Default to False

    def should_migrate_funcprocs(self):
        return self.config.get('migration', {}).get('migrate_funcprocs', False)

    def should_set_sequences(self):
        return self.config.get('migration', {}).get('set_sequences', False)

    def should_migrate_triggers(self):
        return self.config.get('migration', {}).get('migrate_triggers', False)

    def should_migrate_views(self):
        return self.config.get('migration', {}).get('migrate_views', False)

    def get_batch_size(self):
        return int(self.config.get('migration', {}).get('batch_size', 100000))

    def get_parallel_workers_count(self):
        return int(self.config.get('migration', {}).get('parallel_workers', 1)) # Default to 1

    def get_on_error_action(self):
        return self.config.get('migration', {}).get('on_error', 'stop')

    def get_pre_migration_script(self):
        return self.config.get('migration', {}).get('pre_migration_script', None)

    def get_post_migration_script(self):
        return self.config.get('migration', {}).get('post_migration_script', None)

    def get_names_case_handling(self):
        return self.config.get('migration', {}).get('names_case_handling', 'keep').lower()

    def convert_names_case(self, name):
        case_handling = self.get_names_case_handling().lower()
        if case_handling == 'lower':
            return name.lower()
        elif case_handling == 'upper':
            return name.upper()
        elif case_handling == 'keep':
            return name
        else:
            raise ValueError(f"Invalid names_case_handling: {case_handling}")

    def get_varchar_to_text_length(self):
        varchar_to_text_length = self.config.get('migration', {}).get('varchar_to_text_length', None)
        if varchar_to_text_length is not None:
            return int(varchar_to_text_length)
        else:
            return -1 # migrate varchars as they are

    def get_include_tables(self):
        include_tables = self.config.get('include_tables', None)
        if (include_tables is None or (type(include_tables) is str and include_tables.lower() == 'all')):
            return ['.*']  # Pattern matching all table names
        elif type(include_tables) is list:
            return include_tables
        else:
            return []

    def get_exclude_tables(self):
        return self.config['exclude_tables']

    def get_include_views(self):
        include_views = self.config.get('include_views', None)
        if include_views is None or (type(include_views) is str and include_views.lower() == 'all'):
            # Pattern matching all view names
            return ['.*']
        elif type(include_views) is list:
            return include_views
        else:
            return []

    def get_exclude_views(self):
        return self.config.get('exclude_views', [])

    def get_include_funcprocs(self):
        include_funcprocs = self.config.get('include_funcprocs', None)
        if include_funcprocs is None or (type(include_funcprocs) is str and include_funcprocs.lower() == 'all'):
            # Pattern matching all function/procedure names
            return ['.*']
        elif type(include_funcprocs) is list:
            return include_funcprocs
        else:
            return []

    def get_exclude_funcprocs(self):
        return self.config.get('exclude_funcprocs', [])

    def get_log_file(self):
        return self.args.log_file or MigratorConstants.get_default_log()

    def get_log_level(self):
        if self.args.log_level:
            return self.args.log_level
        return 'INFO'

    def print_log_message(self, message_level, message):
        if message_level.upper() == 'ERROR':
            self.logger.error(message)
            return
        current_log_level = self.get_log_level()
        if message_level.upper() not in MigratorConstants.get_message_levels():
            raise ValueError(f"Invalid message_level: {message_level}. Must be one of {MigratorConstants.get_message_levels()}")
        if MigratorConstants.get_message_levels().index(message_level.upper()) <= MigratorConstants.get_message_levels().index(current_log_level.upper()):
            pass
            if message_level == 'DEBUG':
                self.logger.debug(message)
            elif message_level == 'DEBUG2':
                self.logger.debug('DEBUG2: ' + message)
            elif message_level == 'DEBUG3':
                self.logger.debug('DEBUG3: ' + message)
            else:
                self.logger.info(message)


    def get_indent(self):
        return self.config.get('migrator', {}).get('indent', MigratorConstants.get_default_indent())

    def get_target_db_session_settings(self):
        return self.config['target'].get('settings', {})

    def get_target_partitioning(self):
        return self.config.get('target_partitioning', {})

    # another service functions

    def indent_code(self, code):
        lines = code.split('\n')
        indent_level = 0
        indented_lines = []
        for line in lines:
            stripped_line = line.strip()
            if (stripped_line.upper().startswith('END')
                or stripped_line.upper().startswith('ELSE')
                or stripped_line.upper().startswith('ELSIF')
                or stripped_line.upper().startswith('EXCEPTION')
                or stripped_line.upper().startswith('BEGIN')):
                indent_level -= 1
                if indent_level < 0:
                    indent_level = 0
            indented_lines.append(f"{self.get_indent() * indent_level}{stripped_line}")
            if (stripped_line.upper().endswith('LOOP')
                or stripped_line.upper().startswith('BEGIN')
                or stripped_line.upper().startswith('IF')
                or stripped_line.upper().startswith('ELSIF')
                or stripped_line.upper().startswith('EXCEPTION')
                or stripped_line.upper().startswith('DECLARE')):
                indent_level += 1
        return '\n'.join(indented_lines)


if __name__ == "__main__":
    print("This script is not meant to be run directly")
