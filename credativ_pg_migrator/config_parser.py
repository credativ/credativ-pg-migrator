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
import re
from datetime import datetime
import os
import time

class ConfigParser:
    def __init__(self, args, logger):
        self.args = args
        self.logger = logger
        self.config = self.load_config(args.config)
        self.validate_config()

    def load_config(self, config_file):
        """Load the configuration file."""
        self.print_log_message('INFO', f"Working directory: {os.path.dirname(os.path.abspath(self.args.config))}")
        self.print_log_message('INFO', f"Loading configuration from {config_file}")
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


    ## General config
    def is_dry_run(self):
        return bool(self.args.dry_run)

    def is_resume_after_crash(self):
        return bool(self.args.resume)

    def should_drop_unfinished_tables(self):
        if self.get_source_db_type() == 'sybase_ase':
            # Sybase ASE does not support LIMIT with OFFSET in older versions, so we cannot resume after crash
            # and must drop unfinished tables
            self.print_log_message('INFO', "##### Sybase ASE does not support LIMIT with OFFSET in older versions, dropping unfinished tables. #####")
            return True
        return bool(self.args.drop_unfinished_tables)


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

    def get_source_db_locale(self):
        """
        Get the locale for the source database, used for date and time formatting.
        Relevant only for some databases like Informix.
        If not specified, defaults to 'en_US.utf8'.
        """
        return self.config['source'].get('db_locale', 'en_US.utf8')

    def get_source_client_locale(self):
        """
        In this moment method is only prepared for future use.
        Get the client locale for the source database, used for date and time formatting.
        Relevant only for some databases like Informix.
        If not specified, defaults to 'en_US.utf8'.
        """
        return self.config['source'].get('client_locale', 'en_US.utf8')

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
        db_locale = self.get_source_db_locale() if source_or_target == 'source' else None
        # client_locale = self.get_source_client_locale() if source_or_target == 'source' else None
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
                return f"jdbc:informix-sqli://{db_config['host']}:{db_config['port']}/{db_config['database']}:INFORMIXSERVER={db_config['server']};DB_LOCALE={db_locale}"
                # ;CLIENT_LOCALE={client_locale}
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

    def get_protocol_name_batches_stats(self):
        return f"{self.get_protocol_name()}_batches_stats"

    def get_protocol_name_data_chunks(self):
        return f"{self.get_protocol_name()}_data_chunks"

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

    def should_migrate_data(self, table_name=None):
        if table_name:
            table_settings = self.config.get('table_settings', {})
            # table_settings is expected to be a list of dicts with 'table_name' and settings
            if isinstance(table_settings, list):
                for entry in table_settings:
                    pattern = entry.get('table_name')
                    # self.print_log_message('DEBUG3', f"should_migrate_data: checking table {table_name} with pattern {pattern}, setting is {entry.get('migrate_data', False)}")
                    if pattern and re.fullmatch(pattern, table_name, re.IGNORECASE):
                        # self.print_log_message('DEBUG3', f"should_migrate_data: table {table_name} matched pattern {pattern}, setting is {entry.get('migrate_data', False)}")
                        return entry.get('migrate_data', False)
        # self.print_log_message('DEBUG3', f"should_migrate_data: table {table_name} returned default setting {self.config.get('migration', {}).get('migrate_data', False)}")
        return self.config.get('migration', {}).get('migrate_data', False)

    def should_migrate_indexes(self, table_name=None):
        if table_name:
            table_settings = self.config.get('table_settings', {})
            if isinstance(table_settings, list):
                for entry in table_settings:
                    pattern = entry.get('table_name')
                    if pattern and re.fullmatch(pattern, table_name, re.IGNORECASE):
                        return entry.get('migrate_indexes', False)
        return self.config.get('migration', {}).get('migrate_indexes', False)

    def should_migrate_constraints(self, table_name=None):
        if table_name:
            table_settings = self.config.get('table_settings', {})
            # table_settings is expected to be a list of dicts with 'table_name' and settings
            if isinstance(table_settings, list):
                for entry in table_settings:
                    pattern = entry.get('table_name')
                    if pattern and re.fullmatch(pattern, table_name, re.IGNORECASE):
                        return entry.get('migrate_constraints', False)
        return self.config.get('migration', {}).get('migrate_constraints', False)

    def should_migrate_funcprocs(self):
        return self.config.get('migration', {}).get('migrate_funcprocs', False)

    def should_set_sequences(self):
        return self.config.get('migration', {}).get('set_sequences', False)

    def should_migrate_triggers(self, table_name=None):
        if table_name:
            table_settings = self.config.get('table_settings', {})
            # table_settings is expected to be a list of dicts with 'table_name' and settings
            if isinstance(table_settings, list):
                for entry in table_settings:
                    pattern = entry.get('table_name')
                    if pattern and re.fullmatch(pattern, table_name, re.IGNORECASE):
                        return entry.get('migrate_triggers', False)
        return self.config.get('migration', {}).get('migrate_triggers', False)

    def should_migrate_views(self):
        return self.config.get('migration', {}).get('migrate_views', False)

    def get_batch_size(self):
        return int(self.config.get('migration', {}).get('batch_size', 100000))

    def get_chunk_size(self):
        chunk_size = self.config.get('migration', {}).get('chunk_size', -1)
        if chunk_size == -1:
            self.print_log_message('DEBUG', "Chunk size is set to -1, which means no chunking will be done.")
            return -1
        if chunk_size < self.get_batch_size():
            self.print_log_message('WARNING', f"Chunk size {chunk_size} is smaller than batch size {self.get_batch_size()}. Disabling chunking.")
            return -1 ##self.get_batch_size() * 10
        return int(chunk_size)

    def get_total_chunks(self, source_table_rows, chunk_size):
        if chunk_size == -1:
            return 1
        total_chunks = int(source_table_rows / chunk_size)
        if (source_table_rows / chunk_size) > total_chunks:
            total_chunks += 1
        return total_chunks

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

    def get_char_to_text_length(self):
        char_to_text_length = self.config.get('migration', {}).get('char_to_text_length', None)
        if char_to_text_length is not None:
            return int(char_to_text_length)
        else:
            return -1

    def should_migrate_lob_values(self):
        """
        Check if LOB values (BLOB, CLOB) should be migrated.
        If not specified, defaults to False.
        """
        return self.config.get('migration', {}).get('migrate_lob_values', True)

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
        # self.logger.debug(f"Log level: {current_log_level}, Message level: {message_level.upper()}, Message level index: {MigratorConstants.get_message_levels().index(message_level.upper())}, Current log level index: {MigratorConstants.get_message_levels().index(current_log_level.upper())}")
        if MigratorConstants.get_message_levels().index(message_level.upper()) <= MigratorConstants.get_message_levels().index(current_log_level.upper()):
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

    def get_source_data_export(self):
        source_config = self.get_source_config()
        return source_config.get('data_export', {})

    def get_source_data_export_format(self):
        return self.get_source_data_export().get('format', None)

    def get_source_data_export_delimiter(self):
        return self.get_source_data_export().get('delimiter', None)

    def get_source_data_export_path(self):
        return self.get_source_data_export().get('path', None)

    def get_source_data_export_name(self):
        return self.get_source_data_export().get('name', None)

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

    def get_table_batch_size(self, table_name=None):
        if table_name:
            table_settings = self.config.get('table_settings', [])
            if isinstance(table_settings, list):
                for entry in table_settings:
                    pattern = entry.get('table_name')
                    if pattern and re.fullmatch(pattern, table_name, re.IGNORECASE):
                        return entry.get('batch_size', self.get_batch_size())
        return self.get_batch_size()

    def get_table_chunk_size(self, table_name=None):
        chunk_size = self.get_chunk_size()
        if table_name:
            table_settings = self.config.get('table_settings', [])
            if isinstance(table_settings, list):
                for entry in table_settings:
                    pattern = entry.get('table_name')
                    if pattern and re.fullmatch(pattern, table_name, re.IGNORECASE):
                        chunk_size = entry.get('chunk_size', self.get_chunk_size())
                        if chunk_size == -1:
                            self.print_log_message('DEBUG', f"Chunk size for table {table_name} is set to -1, which means no chunking will be done.")
                        if chunk_size < self.get_table_batch_size(table_name):
                            self.print_log_message('WARNING', f"Chunk size {chunk_size} for table {table_name} is smaller than batch size {self.get_table_batch_size(table_name)}. Disabling chunking.")
                            chunk_size = -1
        return chunk_size

    def get_table_data_export(self, table_name=None):
        data_export = self.get_source_data_export()
        if table_name:
            table_settings = self.config.get('table_settings', [])
            if isinstance(table_settings, list):
                for entry in table_settings:
                    pattern = entry.get('table_name')
                    if pattern and re.fullmatch(pattern, table_name, re.IGNORECASE):
                        return entry.get('data_export', data_export)
        return data_export

    def get_table_data_export_format(self, table_name=None):
        return self.get_table_data_export(table_name).get('format', None)

    def get_table_data_export_delimiter(self, table_name=None):
        return self.get_table_data_export(table_name).get('delimiter', None)

    def get_table_data_export_path(self, table_name=None):
        return self.get_table_data_export(table_name).get('path', None)

    def get_table_data_export_name(self, table_name=None):
        return self.get_table_data_export(table_name).get('name', None)


    ## pre-migration analysis
    def get_pre_migration_analysis(self):
        """
        Get the pre-migration analysis settings.
        If not specified, returns an empty dictionary.
        """
        return self.config.get('pre_migration_analysis', {})

    def get_top_n_tables(self):
        """
        Get the TOP N tables settings.
        If not specified, returns an empty dictionary.
        """
        return self.config.get('top_n_tables', {})

    def get_top_n_tables_by_rows(self):
        """
        Get the TOP N tables by rows setting from pre_migration_analysis.
        If not specified, returns None.
        """
        return self.config.get('pre_migration_analysis', {}).get('top_n_tables', {}).get('by_rows', 0)

    def get_top_n_tables_by_size(self):
        """
        Get the TOP N tables by total size setting from pre_migration_analysis.
        If not specified, returns None.
        """
        return self.config.get('pre_migration_analysis', {}).get('top_n_tables', {}).get('by_size', 0)

    def get_top_n_tables_by_columns(self):
        """
        Get the TOP N tables by column count setting from pre_migration_analysis.
        If not specified, returns None.
        """
        return self.config.get('pre_migration_analysis', {}).get('top_n_tables', {}).get('by_columns', 0)

    def get_top_n_tables_by_indexes(self):
        """
        Get the TOP N tables by index count setting from pre_migration_analysis.
        If not specified, returns None.
        """
        return self.config.get('pre_migration_analysis', {}).get('top_n_tables', {}).get('by_indexes', 0)

    def get_top_n_tables_by_constraints(self):
        """
        Get the TOP N tables by constraint count setting from pre_migration_analysis.
        If not specified, returns None.
        """
        return self.config.get('pre_migration_analysis', {}).get('top_n_tables', {}).get('by_constraints', 0)


    ## scheduled actions

    def pause_migration_fired(self):
        config_dir = os.path.dirname(os.path.abspath(self.args.config))

        scheduled_actions = self.config.get('migration', {}).get('scheduled_actions', [])
        self.print_log_message('DEBUG3', f"pause_migration_fired: Checking for scheduled actions: {scheduled_actions}")
        resume_file = os.path.join(config_dir, "resume_migration")

        now = datetime.now()
        for action in scheduled_actions:
            self.print_log_message('DEBUG3', f"pause_migration_fired: Checking action: {action}")
            if action.get('action') == 'pause' and 'datetime' in action:
                action_datetime_str = action['datetime']
                try:
                    # Expected format: "YYYY.MM.DD HH:MM"
                    action_datetime = datetime.strptime(action_datetime_str, "%Y.%m.%d %H:%M")
                    self.print_log_message('DEBUG3', f"pause_migration_fired: Parsed action datetime: {action_datetime}, current datetime: {now}")
                except ValueError:
                    self.logger.error(f"pause_migration_fired: Invalid datetime format in scheduled action: {action_datetime_str}. Expected format is YYYY.MM.DD HH:MM.")
                    continue  # skip invalid datetime format
                if now >= action_datetime and not action.get('fired', False):
                    self.print_log_message('INFO', f"""**** Pausing migration with scheduled action "{action.get('name')}" as current datetime {now} is past scheduled action datetime {action_datetime}. ****""")
                    self.print_log_message('INFO', f"**** To resume migration, create a file '{resume_file}' in the working directory. ****")
                    action['fired'] = True
                    return True

        pause_file = os.path.join(config_dir, "pause_migration")
        self.print_log_message('DEBUG', f"Checking for pause file '{pause_file}' to pause migration...")
        if os.path.exists(pause_file):
            os.remove(pause_file)
            self.print_log_message('INFO', f"**** Pause file '{pause_file}' found. Pausing migration. ****")
            self.print_log_message('INFO', f"**** To resume migration, create a file '{resume_file}' in the working directory. ****")
            return True

        cancel_file = os.path.join(config_dir, "cancel_migration")
        self.print_log_message('DEBUG', f"Checking for cancel file '{cancel_file}' to cancel migration...")
        if os.path.exists(cancel_file):
            self.print_log_message('INFO', f"Cancel file '{cancel_file}' found. Exiting migration.")
            os.remove(cancel_file)
            self.print_log_message('INFO', "**** Migration canceled on user request ****")
            exit(1)

        return False

    def wait_for_resume(self):
        config_dir = os.path.dirname(os.path.abspath(self.args.config))
        resume_file = os.path.join(config_dir, "resume_migration")
        self.print_log_message('INFO', f"Migration paused. Waiting for '{resume_file}' to exist to resume...")
        while not os.path.exists(resume_file):
            time.sleep(5)
        self.print_log_message('INFO', f"Resuming migration as '{resume_file}' was found.")
        os.remove(resume_file)

### Main entry point

if __name__ == "__main__":
    print("This script is not meant to be run directly")
