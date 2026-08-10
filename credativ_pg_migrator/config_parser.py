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
import csv
from datetime import datetime
import os
import time
from collections import Counter
import urllib.parse

class ConfigParser:
    def __init__(self, args, logger):
        self.args = args
        self.logger = logger
        self.config = self.load_config(args.config)
        self.print_log_message('DEBUG3', f"config_parser: __init__: Configuration loaded: {self.config}")
        self.validate_config()

    def load_config(self, config_file):
        """Load the configuration file."""
        self.print_log_message('INFO', f"config_parser: load_config: Working directory: {os.path.dirname(os.path.abspath(self.args.config))}")
        self.print_log_message('INFO', f"config_parser: load_config: Loading configuration from {config_file}")
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
            self.print_log_message('INFO', "config_parser: should_drop_unfinished_tables: ##### Sybase ASE does not support LIMIT with OFFSET in older versions, dropping unfinished tables. #####")
            return True
        return bool(self.args.drop_unfinished_tables)


    ## Databases

    def get_db_config(self, source_or_target):
        if source_or_target == 'target_copy':
            return self.get_validation_target_copy_config()
        if source_or_target not in ['source', 'target']:
            raise ValueError(f"Invalid source_or_target: {source_or_target}")
        return self.config[source_or_target]

    def get_db_type(self, source_or_target):
        return self.get_db_config(source_or_target).get('type')

    def get_source_config(self):
        return self.config['source']

    def get_source_db_name(self):
        source_config = self.get_source_config()
        if source_config.get('connectivity') == 'ddl':
            ddl_dir = source_config.get('ddl', {}).get('path', 'unknown')
            return f"DDL Files ({ddl_dir})"
        return source_config.get('database', 'unknown')

    def get_source_schema(self):
        source_config = self.get_source_config()
        return source_config.get('schema', source_config.get('owner', 'public'))

    def get_source_owner(self):
        return self.get_source_schema()

    def set_source_schema(self, schema):
        """ Set the schema for the source database. For special cases, like IBM DB2 z/OS with DDL connectivity. """
        self.config['source']['schema'] = schema

    def get_source_db_type(self):
        return self.config['source']['type']

    def get_source_db_version(self):
        return self.config['source'].get('version', None)

    def set_source_db_version(self, version):
        self.config['source']['version'] = version

    def get_connectivity(self, source_or_target):
        return self.get_db_config(source_or_target).get('connectivity', None)

    def get_source_connectivity(self):
        return self.get_connectivity('source').lower()

    def get_source_db_locale(self):
        """
        Get the locale for the source database, used for date and time formatting.
        Relevant only for some databases like Informix.
        If not specified, defaults to 'en_US.utf8'.
        """
        return self.config['source'].get('db_locale', 'en_US.utf8')

    def get_oracle_thick_mode(self):
        """
        Get the thick mode setting for the Oracle source connector.
        If true, the Oracle Client libraries will be required.
        Defaults to False (Thin mode).
        """
        return self.config['source'].get('oracle_thick_mode', False)

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
        connectivity = self.get_connectivity(source_or_target)
        db_config = self.get_db_config(source_or_target)
        db_locale = self.get_source_db_locale() if source_or_target == 'source' else None
        # client_locale = self.get_source_client_locale() if source_or_target == 'source' else None
        if db_config['type'] == 'postgresql':
            if connectivity == 'native' or connectivity is None:
                host = db_config.get('host', 'localhost')
                encoded_host = urllib.parse.quote(host, safe='')
                return f"""postgresql://{db_config['username']}:{db_config['password']}@{encoded_host}:{db_config['port']}/{db_config['database']}?sslmode={db_config.get('sslmode', 'prefer')}"""
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
                conn_str = f"jdbc:sqlserver://{db_config['host']}:{db_config['port']};databaseName={db_config['database']};user={db_config['username']};password={db_config['password']};{db_config.get('connection_string_options', '')}"
                return re.sub(r';+', ';', conn_str).rstrip(';')
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
        elif db_config['type'] == 'ibm_db2_luw':
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
        elif db_config['type'] == 'sqlite':
            # SQLite is read from a file with the sqlite3 module of the standard library.
            # "native" (the default when the value is left out) reads the database file given
            # by 'database'; "ddl" reads the objects from the SQL scripts under 'ddl: path:',
            # which the connector replays into a staging database of its own.
            normalized_connectivity = str(connectivity).strip().lower() if connectivity else 'native'
            if normalized_connectivity == 'ddl':
                ddl_path = (db_config.get('ddl') or {}).get('path')
                if not ddl_path:
                    raise ValueError("SQLite connection with connectivity 'ddl' requires 'ddl: path:' with the SQL script file(s)")
                ddl_path = os.path.expanduser(str(ddl_path))
                if not os.path.isabs(ddl_path):
                    ddl_path = os.path.join(os.path.dirname(os.path.abspath(self.args.config)), ddl_path)
                return os.path.normpath(ddl_path)
            if normalized_connectivity == 'native':
                # SQLite is a single file - "database" holds the path to it, and there is
                # no host, port, user or password. A relative path is resolved against the
                # directory of the config file, so that a config file can be moved together
                # with the database it describes.
                database_path = db_config.get('database') or db_config.get('file') or db_config.get('path')
                if not database_path:
                    raise ValueError("SQLite connection requires 'database' with the path to the SQLite database file")
                database_path = os.path.expanduser(str(database_path))
                if not os.path.isabs(database_path):
                    database_path = os.path.join(os.path.dirname(os.path.abspath(self.args.config)), database_path)
                return os.path.normpath(database_path)
            else:
                raise ValueError(
                    f"Unsupported SQLite connectivity: '{connectivity}'. SQLite supports "
                    f"\"native\" (or no value at all) - read the database file given by "
                    f"'database' - and \"ddl\" - read the objects from the SQL scripts given "
                    f"by 'ddl: path:'.")
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
        return self.config['source'].get('system_catalog', 'NONE').upper()

    ## Summary
    def get_summary_config(self):
        return (self.config.get('summary') or {})

    def get_summary_top_migrated_tables(self):
        return self.get_summary_config().get('top_migrated_tables', 5)

    def get_summary_top_mismatched_tables(self):
        return self.get_summary_config().get('top_mismatched_tables', 5)

    def get_summary_top_longest_batches(self):
        return self.get_summary_config().get('top_longest_batches', 10)

    def get_summary_top_anonymized_tables(self):
        return self.get_summary_config().get('top_anonymized_tables', 5)

    def get_mapping_report_filename(self):
        mapping_config = (self.config.get('mapping') or {})
        if 'report_filename' in mapping_config:
            return mapping_config.get('report_filename')
        return (self.config.get('migration') or {}).get('mapping_report_filename')

    def get_summary_top_anonymized_columns(self):
        return self.get_summary_config().get('top_anonymized_columns', 5)

    def get_summary_show_anonymization_examples(self):
        return self.get_summary_config().get('show_anonymization_examples', 0)

    ## Migrator
    def get_migrator_config(self):
        return (self.config.get('migrator') or {})

    def get_migrator_db_type(self):
        return self.get_migrator_config().get('type', None)

    def get_migrator_schema(self):
        return self.get_migrator_config().get('schema', MigratorConstants.get_default_schema())

    def get_anonymization_config(self):
        return (self.config.get('anonymization') or {})

    def get_migration_settings(self):
        return self.config['migration']

    def get_workflow(self):
        return self.config.get('workflow', 'standard')

    def is_standard_workflow(self):
        return self.get_workflow() == 'standard'

    def is_mapping_workflow(self):
        return self.get_workflow() == 'mapping'

    def is_anonymization_workflow(self):
        return self.get_workflow() == 'anonymization'

    def get_suspend_indexes_constraints(self):
        mapping_config = (self.config.get('mapping') or {})
        if 'suspend_indexes_constraints' in mapping_config:
            return mapping_config.get('suspend_indexes_constraints')
        settings = self.get_migration_settings()
        return settings.get('suspend_indexes_constraints', True)

    def get_mapping_workflow_heuristics(self):
        return (self.config.get('mapping') or {}).get('heuristics', {})

    def get_forced_table_mappings(self):
        return (self.config.get('mapping') or {}).get('forced_table_mappings', [])

    def get_forced_column_mappings(self):
        return (self.config.get('mapping') or {}).get('forced_column_mappings', [])

    def get_use_aliases_as_target_names(self):
        settings = self.get_migration_settings()
        return settings.get('use_aliases_as_target_names', False)

    def get_zero_datetime_default(self):
        settings = self.get_migration_settings() if 'migration' in self.config else {}
        if 'zero_datetime_default' in settings:
            return settings.get('zero_datetime_default')
        if 'mysql_zero_datetime_default' in settings:
            return settings.get('mysql_zero_datetime_default')
        source_config = self.get_source_config() if 'source' in self.config else {}
        if 'zero_datetime_default' in source_config:
            return source_config.get('zero_datetime_default')
        return 'remove'

    def get_zero_datetime_data_value(self):
        settings = self.get_migration_settings() if 'migration' in self.config else {}
        if 'zero_datetime_value' in settings:
            return settings.get('zero_datetime_value')
        if 'zero_datetime_data_value' in settings:
            return settings.get('zero_datetime_data_value')
        return None

    def get_relax_not_null_datetime(self):
        settings = self.get_migration_settings() if 'migration' in self.config else {}
        if 'relax_not_null_datetime' in settings:
            return settings.get('relax_not_null_datetime')
        return True

    def get_uuid_default_function(self, target_column_type=None):
        settings = self.get_migration_settings() if 'migration' in self.config else {}
        func = settings.get('uuid_default_function')
        if not func:
            func = settings.get('uuid_function', 'gen_random_uuid()')

        func_str = str(func).strip()
        if not func_str:
            func_str = 'gen_random_uuid()'

        if not func_str.endswith(')') and not func_str.endswith('::text'):
            func_str += '()'

        col_type_str = str(target_column_type).upper() if target_column_type else ''
        is_string_type = any(t in col_type_str for t in ('TEXT', 'CHAR', 'VARCHAR', 'STRING'))

        if is_string_type:
            if not func_str.endswith('::text'):
                return f"{func_str}::text"
            return func_str
        else:
            if func_str.endswith('::text'):
                return func_str[:-6].strip()
            return func_str

    def get_required_extensions(self) -> list:
        settings = self.get_migration_settings() if 'migration' in self.config else {}
        exts = settings.get('required_extensions', [])
        if not exts:
            exts = settings.get('extensions', [])
        if isinstance(exts, str):
            exts = [e.strip() for e in exts.split(',') if e.strip()]
        elif not isinstance(exts, list):
            exts = []

        ext_list = [str(e).strip().lower() for e in exts if e]

        # Auto-infer required extensions based on configuration
        uuid_func = self.get_uuid_default_function()
        if 'uuid_generate' in uuid_func.lower():
            if 'uuid-ossp' not in ext_list:
                ext_list.append('uuid-ossp')
        elif 'pgcrypto' in uuid_func.lower():
            if 'pgcrypto' not in ext_list:
                ext_list.append('pgcrypto')

        return list(dict.fromkeys(ext_list))

    def get_table_mapping(self, source_schema, source_table):
        """Returns the mapping rule for a specific source table if it exists within its data_export settings."""
        table_data_export = self.get_table_data_export(source_schema, source_table)
        if table_data_export and 'mapping_rules' in table_data_export:
            mapping_rules = table_data_export.get('mapping_rules', [])
            if mapping_rules and len(mapping_rules) > 0:
                return mapping_rules[0]
        return None

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

    def get_protocol_name_collations(self):
        return f"{self.get_protocol_name()}_collations"

    def get_protocol_name_text_search(self):
        return f"{self.get_protocol_name()}_text_search"

    def get_protocol_name_default_values(self):
        return f"{self.get_protocol_name()}_defaults"

    def get_protocol_name_target_columns_alterations(self):
        return f"{self.get_protocol_name()}_target_cols_alt"

    def get_protocol_name_new_objects(self):
        return f"{self.get_protocol_name()}_new_objects"

    def get_protocol_name_tables(self):
        return f"{self.get_protocol_name()}_tables"

    def get_protocol_name_source_table_partitioning(self):
        return f"{self.get_protocol_name()}_source_table_partitioning"

    def get_protocol_name_target_table_partitioning(self):
        return f"{self.get_protocol_name()}_target_table_partitioning"

    def get_protocol_name_columns(self):
        return f"{self.get_protocol_name()}_columns"

    def get_protocol_name_data_sources(self):
        return f"{self.get_protocol_name()}_data_sources"

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

    def get_protocol_name_aliases(self):
        return f"{self.get_protocol_name()}_aliases"

    def get_data_types_substitution(self):
        return (self.config.get('data_types_substitution') or {})

    def get_default_values_substitution(self):
        implicit_substitutions = []
        from_config_file = (self.config.get('default_values_substitution') or {})
        self.print_log_message('DEBUG3', f"config_parser: get_default_values_substitution: from_config_file: {from_config_file}")
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
        return (self.config.get('data_migration_limitation') or {})

    def get_remote_objects_substitution(self):
        return (self.config.get('remote_objects_substitution') or {})

    ## Migration settings
    def _match_table_name(self, table_name, pattern):
        if not pattern:
            return False
        if isinstance(pattern, list):
            for p in pattern:
                if re.fullmatch(p, table_name, re.IGNORECASE):
                    return True
            return False
        return bool(re.fullmatch(pattern, table_name, re.IGNORECASE))

    def should_drop_schema(self):
        return (self.config.get('migration') or {}).get('drop_schema', False)

    def should_drop_tables(self):
        return (self.config.get('migration') or {}).get('drop_tables', False) # Default to False

    def should_truncate_tables(self):
        return (self.config.get('migration') or {}).get('truncate_tables', False)

    def should_create_tables(self):
        return (self.config.get('migration') or {}).get('create_tables', False)

    def get_table_migration_switch(self, switch_name: str, table_name=None):
        """
        Returns the value of one of the migrate_* switches for a single table. A table_settings
        entry overrides the global migration setting only when it really contains the switch -
        a table listed in table_settings for a completely different reason (character set,
        delimiter, header, ...) keeps the global setting instead of silently losing its data,
        indexes, constraints or triggers.
        """
        global_value = (self.config.get('migration') or {}).get(switch_name, False)
        if table_name:
            table_settings = (self.config.get('table_settings') or {})
            # table_settings is expected to be a list of dicts with 'table_name' and settings
            if isinstance(table_settings, list):
                for entry in table_settings:
                    pattern = entry.get('table_name')
                    if switch_name in entry and self._match_table_name(table_name, pattern):
                        self.print_log_message('DEBUG3', f"config_parser: get_table_migration_switch: table {table_name} matched pattern {pattern}, {switch_name} is {entry.get(switch_name)}")
                        return entry.get(switch_name)
        return global_value

    def should_migrate_data(self, table_name=None):
        return self.get_table_migration_switch('migrate_data', table_name)

    def should_migrate_indexes(self, table_name=None):
        return self.get_table_migration_switch('migrate_indexes', table_name)

    def should_migrate_constraints(self, table_name=None):
        return self.get_table_migration_switch('migrate_constraints', table_name)

    def should_migrate_funcprocs(self):
        return (self.config.get('migration') or {}).get('migrate_funcprocs', False)

    def should_set_sequences(self):
        return (self.config.get('migration') or {}).get('set_sequences', False)

    def should_migrate_triggers(self, table_name=None):
        return self.get_table_migration_switch('migrate_triggers', table_name)

    def should_migrate_views(self):
        return (self.config.get('migration') or {}).get('migrate_views', False)

    def get_packages_migration_style(self):
        # How source packages (Oracle) are represented on the target, which has no packages.
        #   'functions' (default): one function per package routine in the target schema,
        #                          named <package>_<routine>
        #   'schemas'            : a schema named after the package, holding one function
        #                          per package routine under its own name
        val = (self.config.get('migration') or {}).get('packages_as', 'functions')
        normalized = str(val).strip().lower() if val is not None else 'functions'
        if normalized in ('schemas', 'schema', 'package_schema', 'package_schemas'):
            return 'schemas'
        if normalized in ('functions', 'function', 'prefixed_functions', 'prefix'):
            return 'functions'
        self.print_log_message('WARNING', f"config_parser: get_packages_migration_style: Unknown value '{val}' for migration.packages_as - using 'functions'.")
        return 'functions'

    def get_validate_objects_mode(self):
        # Final object-validity pass at the end of standard migration (views, functions/
        # procedures, triggers). Objects that failed to create because a dependency did not
        # yet exist can become creatable once the whole schema is migrated.
        #   'retry' (default): re-attempt the stored DDL of objects that are not yet present,
        #                      then verify existence in the target catalog and mark validity.
        #   'check'          : verify existence only (no DDL is re-executed).
        #   'off'            : skip the pass entirely.
        val = (self.config.get('migration') or {}).get('validate_objects', 'retry')
        if val is True:
            return 'retry'
        if val is False or val is None:
            return 'off'
        normalized = str(val).strip().lower()
        if normalized in ('retry', 'true', 'yes', 'on'):
            return 'retry'
        if normalized in ('check', 'verify', 'check_only'):
            return 'check'
        if normalized in ('off', 'false', 'no', 'none', 'skip'):
            return 'off'
        return 'retry'

    def should_map_numeric_1_to_boolean(self, schema_name=None, table_name=None, column_name=None):
        # Decides whether a narrow numeric source column (precision 1, scale 0 -
        # e.g. Oracle NUMBER(1,0), or NUMERIC(1,0) from other engines) is mapped to
        # PostgreSQL BOOLEAN. Such a column can legitimately be a 0/1 flag OR a small
        # integer code (e.g. channel_id, day-of-week 1-7), and the two are
        # indistinguishable from the type metadata alone. By default these columns are
        # therefore mapped to SMALLINT (lossless); individual columns are opted in to
        # BOOLEAN via migration.numeric_1_boolean_columns.
        migration = self.config.get('migration') or {}

        # Global escape hatch: map ALL such columns to BOOLEAN (restores the historical
        # 0.16.0 behavior). Default False.
        if migration.get('map_numeric_1_to_boolean', False) is True:
            return True

        if not column_name:
            return False

        patterns = migration.get('numeric_1_boolean_columns') or []
        for entry in patterns:
            # Structured entry: match any provided schema/table/column regex (all
            # supplied keys must match). Case-insensitive, full match.
            if isinstance(entry, dict):
                checks = [
                    (entry.get('schema'), schema_name),
                    (entry.get('table'), table_name),
                    (entry.get('column'), column_name),
                ]
                matched_any = False
                ok = True
                for pat, value in checks:
                    if not pat:
                        continue
                    matched_any = True
                    if value is None or not re.fullmatch(str(pat), str(value), re.IGNORECASE):
                        ok = False
                        break
                if matched_any and ok:
                    return True
            # Plain string entry: a column-name regex (matches in any table).
            elif entry and re.fullmatch(str(entry), str(column_name), re.IGNORECASE):
                return True

        return False

    def get_batch_size(self):
        return int((self.config.get('migration') or {}).get('batch_size', 100000))

    def get_chunk_size(self):
        chunk_size = (self.config.get('migration') or {}).get('chunk_size', -1)
        if chunk_size == -1:
            self.print_log_message('DEBUG', "config_parser: get_chunk_size: Chunk size is set to -1, which means no chunking will be done.")
            return -1
        if chunk_size < self.get_batch_size():
            self.print_log_message('WARNING', f"config_parser: get_chunk_size: Chunk size {chunk_size} is smaller than batch size {self.get_batch_size()}. Disabling chunking.")
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
        return int((self.config.get('migration') or {}).get('parallel_workers', 1)) # Default to 1

    def get_on_error_action(self):
        return (self.config.get('migration') or {}).get('on_error', 'stop')

    def get_pre_migration_script(self):
        return (self.config.get('migration') or {}).get('pre_migration_script', None)

    def get_post_migration_script(self):
        return (self.config.get('migration') or {}).get('post_migration_script', None)

    def get_names_case_handling(self):
        return (self.config.get('migration') or {}).get('names_case_handling', 'keep').lower()

    def convert_names_case(self, name):
        if name is None:
            return None
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
        varchar_to_text_length = (self.config.get('migration') or {}).get('varchar_to_text_length', None)
        if varchar_to_text_length is not None:
            return int(varchar_to_text_length)
        else:
            return -1 # migrate varchars as they are

    def get_char_to_text_length(self):
        char_to_text_length = (self.config.get('migration') or {}).get('char_to_text_length', None)
        if char_to_text_length is not None:
            return int(char_to_text_length)
        else:
            return -1

    def should_migrate_lob_values(self):
        """
        Check if LOB values (BLOB, CLOB) should be migrated.
        If not specified, defaults to False.
        """
        return (self.config.get('migration') or {}).get('migrate_lob_values', True)

    def get_include_tables(self):
        include_tables = self.config.get('include_tables', None)
        if (include_tables is None or (type(include_tables) is str and include_tables.lower() == 'all')):
            return ['.*']  # Pattern matching all table names
        elif type(include_tables) is list:
            return include_tables
        else:
            return []

    ## Validator
    def get_validation_tables_name(self):
        return "validation_tables"

    def get_validation_columns_name(self):
        return "validation_columns"

    def get_validation_indexes_name(self):
        return "validation_indexes"

    def get_validation_constraints_name(self):
        return "validation_constraints"

    def get_validation_config(self):
        return (self.config.get('validation') or {})

    def get_validation_target_copy_config(self):
        return self.get_validation_config().get('target_copy', {})

    def get_validation_workers(self):
        return int(self.get_validation_config().get('workers', 4))

    def get_validation_batch_size(self):
        return int(self.get_validation_config().get('batch_size', 10000))

    def get_validation_report_filename(self):
        return self.get_validation_config().get('report_filename', None)

    def is_validation_row_counts_enabled(self):
        return self.get_validation_config().get('check_row_counts', True)

    def is_validation_table_checksums_enabled(self):
        return self.get_validation_config().get('check_table_checksums', False)

    def is_validation_random_sample_enabled(self):
        return self.get_validation_config().get('check_random_sample', False)

    def is_validation_lob_sizes_enabled(self):
        return self.get_validation_config().get('check_lob_sizes', False)

    def get_validation_sample_size(self):
        return int(self.get_validation_config().get('random_sample_size', 1000))

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
                self.logger.info(message_level.upper() + ': ' + message)

    def get_indent(self):
        return (self.config.get('migrator') or {}).get('indent', MigratorConstants.get_default_indent())

    def get_target_db_session_settings(self):
        return self.config['target'].get('settings', {})

    def get_target_partitioning(self):
        return (self.config.get('target_partitioning') or {})

    def get_source_data_export(self):
        source_config = self.get_source_config()
        return source_config.get('data_export', {})

    def get_source_data_export_on_missing_data_file(self):
        return self.get_source_data_export().get('on_missing_data_file', 'source_table_name')

    def get_source_data_export_format(self):
        return self.get_source_data_export().get('format', None)

    def get_source_data_export_delimiter(self):
        return self.get_source_data_export().get('delimiter', "|")

    def get_source_data_export_file(self):
        return self.get_source_data_export().get('file', None)

    def get_source_data_export_file_path(self):
        export_file = self.get_source_data_export_file()
        if export_file is None:
            return None
        # Remove the file name from the export_file and leave just the path
        if os.path.basename(export_file):
            export_file = os.path.dirname(export_file)
        return os.path.abspath(export_file)

    def get_source_data_export_header(self):
        return self.get_source_data_export().get('header', False)

    def get_source_data_export_workers(self):
        return self.get_source_data_export().get('workers', 4)

    def get_source_data_export_conversion_path(self):
        conversion_path = self.get_source_data_export().get('conversion_path', None)
        if conversion_path is None:
            # If conversion_path is not set, try to extract the directory from the export file path
            export_file = self.get_source_data_export_file()
            if export_file:
                return os.path.dirname(os.path.abspath(export_file))
            return None
        return conversion_path

    def get_source_data_export_clean(self):
        return self.get_source_data_export().get('clean', False)

    def get_source_data_export_big_files_split(self):
        return self.get_source_data_export().get('big_files_split', None)

    def get_source_data_export_big_files_split_enabled(self):
        big_files_split = self.get_source_data_export_big_files_split()
        if big_files_split and isinstance(big_files_split, dict):
            return big_files_split.get('enabled', False)
        return False

    def get_source_data_export_big_files_split_threshold_bytes(self):
        big_files_split = self.get_source_data_export_big_files_split()
        if big_files_split and isinstance(big_files_split, dict):
            return self.convert_size_to_bytes(big_files_split.get('threshold', None))
        return None

    def get_source_data_export_big_files_split_chunk_size_bytes(self):
        big_files_split = self.get_source_data_export_big_files_split()
        if big_files_split and isinstance(big_files_split, dict):
            return self.convert_size_to_bytes(big_files_split.get('chunk_size', None))
        return None

    def get_source_data_export_big_files_split_workers(self):
        big_files_split = self.get_source_data_export_big_files_split()
        if big_files_split and isinstance(big_files_split, dict):
            return big_files_split.get('workers', 4)
        return -1  ## by default do not use parallel workers if splitting or workers are not specified

    def get_source_data_export_lob_columns(self):
        """
        Get LOB columns configuration from source database export.
        Returns a list of [table_name, column_name] pairs.
        """
        return self.get_source_data_export().get('lob_columns', [])

    def has_configured_lob_columns(self, source_table_name, lob_columns):
        """
        True when one of the LOB columns of the table was declared in data_export.lob_columns.

        Such a column is declared precisely because its data type does not say it: it holds
        a reference to the file with the value, not the value itself.
        """
        column_names = [column.strip() for column in (lob_columns or '').split(',') if column.strip()]
        for lob_config in self.get_source_data_export_lob_columns():
            if len(lob_config) >= 2:
                config_table_name = lob_config[0]
                config_column_name = lob_config[1]
                if (not config_table_name or config_table_name == source_table_name) and config_column_name in column_names:
                    return True
        return False

    def get_table_name_for_lob_import(self, table_name):
        return f"{table_name}_unllobimport"


    # another service functions

    def convert_size_to_bytes(self, size_str):
        if size_str is None:
            return None
        size_str = size_str.strip().upper()
        if size_str.endswith('TB'):
            return int(size_str[:-2]) * 1024 * 1024 * 1024 * 1024
        elif size_str.endswith('T'):
            return int(size_str[:-1]) * 1024 * 1024 * 1024 * 1024
        elif size_str.endswith('GB'):
            return int(size_str[:-2]) * 1024 * 1024 * 1024
        elif size_str.endswith('G'):
            return int(size_str[:-1]) * 1024 * 1024 * 1024
        elif size_str.endswith('MB'):
            return int(size_str[:-2]) * 1024 * 1024
        elif size_str.endswith('M'):
            return int(size_str[:-1]) * 1024 * 1024
        elif size_str.endswith('KB'):
            return int(size_str[:-2]) * 1024
        elif size_str.endswith('K'):
            return int(size_str[:-1]) * 1024
        elif size_str.endswith('B'):
            return int(size_str[:-1])
        else:
            raise ValueError(f"Invalid size format: {size_str}")

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
                    if self._match_table_name(table_name, pattern):
                        return entry.get('batch_size', self.get_batch_size())
        return self.get_batch_size()

    def get_table_chunk_size(self, table_name=None):
        chunk_size = self.get_chunk_size()
        if table_name:
            table_settings = self.config.get('table_settings', [])
            if isinstance(table_settings, list):
                for entry in table_settings:
                    pattern = entry.get('table_name')
                    if self._match_table_name(table_name, pattern):
                        chunk_size = entry.get('chunk_size', self.get_chunk_size())
                        if chunk_size == -1:
                            self.print_log_message('DEBUG', f"config_parser: get_table_chunk_size: Chunk size for table {table_name} is set to -1, which means no chunking will be done.")
                        if chunk_size < self.get_table_batch_size(table_name):
                            self.print_log_message('WARNING', f"config_parser: get_table_chunk_size: Chunk size {chunk_size} for table {table_name} is smaller than batch size {self.get_table_batch_size(table_name)}. Disabling chunking.")
                            chunk_size = -1
        return chunk_size

    def get_table_data_export(self, schema_name, table_name):
        if table_name:
            table_settings = self.config.get('table_settings', [])
            if isinstance(table_settings, list):
                for entry in table_settings:
                    pattern = entry.get('table_name')
                    table_schema = entry.get('table_schema', schema_name)
                    if self._match_table_name(table_name, pattern) and table_schema.lower() == schema_name.lower():
                        return entry.get('data_export', None)
        return None

    def get_global_data_conflict_action(self):
        mapping_action = (self.config.get('mapping') or {}).get('data_conflict_action')
        if mapping_action:
            return mapping_action
        return self.get_migration_settings().get('data_conflict_action', 'skip')

    def get_mapping_data_resolution(self, table_name):
        # First check specific table overrides
        if table_name:
            table_settings = self.config.get('table_settings', [])
            if isinstance(table_settings, list):
                for entry in table_settings:
                    pattern = entry.get('table_name')
                    if self._match_table_name(table_name, pattern):
                        action = entry.get('data_conflict_action')
                        if action:
                            return action
        # Fallback to global setting or 'skip'
        return self.get_global_data_conflict_action()

    def get_table_data_export_format(self, schema_name, table_name):
        return self.get_table_data_export(schema_name, table_name).get('format', None)

    def get_table_data_export_delimiter(self, schema_name, table_name):
        return self.get_table_data_export(schema_name, table_name).get('delimiter', None)

    def get_table_data_export_file(self, schema_name, table_name):
        return self.get_table_data_export(schema_name, table_name).get('file', None)

    def get_table_data_export_header(self, schema_name, table_name):
        return self.get_table_data_export(schema_name, table_name).get('header', False)

    def get_table_data_export_conversion_path(self, schema_name, table_name):
        conversion_path = self.get_table_data_export(schema_name, table_name).get('conversion_path', None)
        if conversion_path is None:
            # If conversion_path is not set, try to extract the directory from the export file path
            export_file = self.get_table_data_export_file(schema_name, table_name)
            if export_file:
                return os.path.dirname(os.path.abspath(export_file))
            return None
        return conversion_path

    ## pre-migration analysis
    def get_pre_migration_analysis(self):
        """
        Get the pre-migration analysis settings.
        If not specified, returns an empty dictionary.
        """
        return (self.config.get('pre_migration_analysis') or {})

    def get_top_n_tables(self):
        """
        Get the TOP N tables settings.
        If not specified, returns an empty dictionary.
        """
        return (self.config.get('top_n_tables') or {})

    def get_top_n_tables_by_rows(self):
        """
        Get the TOP N tables by rows setting from pre_migration_analysis.
        If not specified, returns None.
        """
        return (self.config.get('pre_migration_analysis') or {}).get('top_n_tables', {}).get('by_rows', 0)

    def get_top_n_tables_by_size(self):
        """
        Get the TOP N tables by total size setting from pre_migration_analysis.
        If not specified, returns None.
        """
        return (self.config.get('pre_migration_analysis') or {}).get('top_n_tables', {}).get('by_size', 0)

    def get_top_n_tables_by_columns(self):
        """
        Get the TOP N tables by column count setting from pre_migration_analysis.
        If not specified, returns None.
        """
        return (self.config.get('pre_migration_analysis') or {}).get('top_n_tables', {}).get('by_columns', 0)

    def get_top_n_tables_by_indexes(self):
        """
        Get the TOP N tables by index count setting from pre_migration_analysis.
        If not specified, returns None.
        """
        return (self.config.get('pre_migration_analysis') or {}).get('top_n_tables', {}).get('by_indexes', 0)

    def get_top_n_tables_by_constraints(self):
        """
        Get the TOP N tables by constraint count setting from pre_migration_analysis.
        If not specified, returns None.
        """
        return (self.config.get('pre_migration_analysis') or {}).get('top_n_tables', {}).get('by_constraints', 0)


    ## scheduled actions

    def pause_migration_fired(self):
        config_dir = os.path.dirname(os.path.abspath(self.args.config))

        scheduled_actions = (self.config.get('migration') or {}).get('scheduled_actions', [])
        self.print_log_message('DEBUG3', f"config_parser: pause_migration_fired: Checking for scheduled actions: {scheduled_actions}")
        resume_file = os.path.join(config_dir, "resume_migration")

        now = datetime.now()
        for action in scheduled_actions:
            self.print_log_message('DEBUG3', f"config_parser: pause_migration_fired: Checking action: {action}")
            if action.get('action') == 'pause' and 'datetime' in action:
                action_datetime_str = action['datetime']
                try:
                    # Expected format: "YYYY.MM.DD HH:MM"
                    action_datetime = datetime.strptime(action_datetime_str, "%Y.%m.%d %H:%M")
                    self.print_log_message('DEBUG3', f"config_parser: pause_migration_fired: Parsed action datetime: {action_datetime}, current datetime: {now}")
                except ValueError:
                    self.logger.error(f"pause_migration_fired: Invalid datetime format in scheduled action: {action_datetime_str}. Expected format is YYYY.MM.DD HH:MM.")
                    continue  # skip invalid datetime format
                if now >= action_datetime and not action.get('fired', False):
                    self.print_log_message('INFO', f"config_parser: pause_migration_fired: **** Pausing migration with scheduled action '{action.get('name')}' as current datetime {now} is past scheduled action datetime {action_datetime}. ****")
                    self.print_log_message('INFO', f"config_parser: pause_migration_fired: **** To resume migration, create a file '{resume_file}' in the working directory. ****")
                    action['fired'] = True
                    return True

        pause_file = os.path.join(config_dir, "pause_migration")
        self.print_log_message('DEBUG', f"config_parser: pause_migration_fired: Checking for pause file '{pause_file}' to pause migration...")
        if os.path.exists(pause_file):
            os.remove(pause_file)
            self.print_log_message('INFO', f"config_parser: pause_migration_fired: **** Pause file '{pause_file}' found. Pausing migration. ****")
            self.print_log_message('INFO', f"config_parser: pause_migration_fired: **** To resume migration, create a file '{resume_file}' in the working directory. ****")
            return True

        cancel_file = os.path.join(config_dir, "cancel_migration")
        self.print_log_message('DEBUG', f"config_parser: pause_migration_fired: Checking for cancel file '{cancel_file}' to cancel migration...")
        if os.path.exists(cancel_file):
            self.print_log_message('INFO', f"config_parser: pause_migration_fired: Cancel file '{cancel_file}' found. Exiting migration.")
            os.remove(cancel_file)
            self.print_log_message('INFO', "config_parser: pause_migration_fired: **** Migration canceled on user request ****")
            exit(1)

        return False

    def wait_for_resume(self):
        config_dir = os.path.dirname(os.path.abspath(self.args.config))
        resume_file = os.path.join(config_dir, "resume_migration")
        self.print_log_message('INFO', f"config_parser: wait_for_resume: Migration paused. Waiting for '{resume_file}' to exist to resume...")
        while not os.path.exists(resume_file):
            time.sleep(5)
        self.print_log_message('INFO', f"config_parser: wait_for_resume: Resuming migration as '{resume_file}' was found.")
        os.remove(resume_file)


    ### Other utility methods

    def get_table_lob_columns(self, source_schema_name, source_table_name, source_columns):
        lob_columns_list = []
        for _, column_info in source_columns.items():
            if column_info.get('data_type', '').upper() in ['BLOB', 'CLOB', 'NCLOB']:
                lob_columns_list.append(column_info['column_name'])
                self.print_log_message('DEBUG3', f"config_parser: get_table_lob_columns: Column {column_info['column_name']} in table {source_table_name} is of LOB type {column_info.get('data_type', '').upper()}. Added to LOB columns list.")
            else:
                # Check if this column is configured as a LOB column in the export settings
                lob_columns_config = self.get_source_data_export_lob_columns()
                for lob_config in lob_columns_config:
                    if len(lob_config) >= 2:
                        config_table_name = lob_config[0]
                        config_column_name = lob_config[1]
                        if (not config_table_name or config_table_name == source_table_name) and config_column_name == column_info['column_name']:
                            lob_columns_list.append(column_info['column_name'])
                            self.print_log_message('DEBUG3', f"config_parser: get_table_lob_columns: Column {column_info['column_name']} in table {source_table_name} is configured as LOB column. Added to LOB columns list.")
                            break
        return ','.join(lob_columns_list)

    ## the data types for which a comma in the exported value is a decimal separator and
    ## not part of the text
    NUMERIC_DATA_TYPES = ('DECIMAL', 'NUMERIC', 'NUMBER', 'FLOAT', 'REAL', 'DOUBLE',
                          'DOUBLE PRECISION', 'SMALLFLOAT', 'MONEY')

    def convert_decimal_separator(self, value):
        """
        Write a number exported with a decimal comma the way the target reads it.

        An export can be written with either convention - Db2 for i does it with
        DECPNT(*COMMA) - and PostgreSQL accepts the decimal point alone, so '1,00000000'
        is refused with 'invalid input syntax for type numeric'. Whichever separator comes
        last is the decimal one, the other one groups the digits:

            1,00000000  ->  1.00000000
            1.234,56    ->  1234.56
            1,234.56    ->  1234.56          (already valid apart from the grouping)

        A value which is not a number written in one of these two ways is returned
        unchanged - it is better migrated as it is and reported by the target than
        silently turned into a different number here.
        """
        number = value.strip()
        if not re.fullmatch(r'[+-]?[\d.,]+', number) or not re.search(r'\d', number):
            return value

        last_comma = number.rfind(',')
        last_dot = number.rfind('.')
        if last_comma > last_dot:
            ## the comma is the decimal separator, a dot groups the digits
            converted = number.replace('.', '').replace(',', '.')
        elif last_dot > last_comma:
            ## the dot is the decimal separator, a comma groups the digits
            converted = number.replace(',', '')
        else:
            return value

        ## only a result which really is a number is used - '1,2,3' is written in neither
        ## of the two conventions, and turning it into '1.2.3' would replace a value the
        ## target reports with one it silently misreads
        if not re.fullmatch(r'[+-]?\d+(?:\.\d+)?', converted):
            return value
        return converted

    def convert_csv_to_utf8(self, data_source_settings, source_columns=None, target_columns=None):
        part_name = 'convert_csv_to_utf8 start'
        self.print_log_message('DEBUG3', f"config_parser: convert_csv_to_utf8: ({part_name}): Starting conversion of CSV file '{data_source_settings.get('file_name')}' to UTF-8.")
        try:
            input_csv_data_file = data_source_settings['file_name']
            output_csv_data_file = data_source_settings.get('converted_file_name', input_csv_data_file) + '_utf8'
            source_table_name = data_source_settings.get('source_table_name', 'Unknown')
            file_size_bytes = data_source_settings.get('file_size', None)
            if file_size_bytes is not None:
                try:
                    file_size_bytes = int(file_size_bytes)
                    file_size_gb = file_size_bytes / (1024 ** 3)
                    source_file_size = f"{file_size_bytes} B / {file_size_gb:.2f} GB"
                except Exception:
                    source_file_size = str(file_size_bytes)
            else:
                source_file_size = "Unknown"

            character_set = data_source_settings.get('format_options', {}).get('character_set', 'UTF-8')
            csv_delimiter = data_source_settings.get('format_options', {}).get('delimiter', ',')
            null_symbol = data_source_settings.get('null_symbol', '\\N')

            processing_start_time = datetime.now()

            if not input_csv_data_file or not output_csv_data_file:
                self.print_log_message('ERROR', "config_parser: convert_csv_to_utf8: Both 'file_name' and 'converted_file_name' must be specified in the settings.")
                raise ValueError("Both 'file_name' and 'converted_file_name' must be specified in the settings.")
            if not os.path.exists(input_csv_data_file):
                self.print_log_message('ERROR', f"config_parser: convert_csv_to_utf8: Input CSV data file '{input_csv_data_file}' does not exist.")
                raise FileNotFoundError(f"Input CSV data file '{input_csv_data_file}' does not exist.")

            self.print_log_message('DEBUG', f"config_parser: convert_csv_to_utf8: ({part_name}): Converting CSV file '{input_csv_data_file}' (charset: {character_set}) to UTF-8 file '{output_csv_data_file}' - source file size: {source_file_size}")

            counter = 0

            if source_columns:
                expected_types = []
                for _, col in source_columns.items():
                    dtype = col.get('data_type', '').upper()
                    if 'source_column_data_type' in col:
                        dtype = col.get('source_column_data_type', '').upper()

                    scale = col.get('numeric_scale')
                    if scale is None:
                        scale = col.get('source_column_numeric_scale')

                    try:
                        scale = int(scale) if scale is not None else None
                    except ValueError:
                        scale = None

                    expected_types.append({'type': dtype, 'scale': scale})
            else:
                expected_types = []

            # DB2 timestamp format: YYYY-MM-DD-HH.MM.SS.mmmmmm
            ts_pattern = re.compile(r'^(\d{4}-\d{2}-\d{2})-(\d{2})\.(\d{2})\.(\d{2})(?:\.(\d+))?$')

            with open(input_csv_data_file, 'r', encoding=character_set, errors='replace', newline='') as infile, \
                 open(output_csv_data_file, 'w', encoding='utf-8', newline='') as outfile:

                ## An unquoted empty field of a CSV file is a NULL, a quoted empty one ("")
                ## is an empty string. The default reader returns '' for both, so a NULL of
                ## the source arrived in the target as an empty string and every column
                ## which is not text refused it: 'invalid input syntax for type integer: ""'.
                ## QUOTE_NOTNULL keeps the two apart - it returns None for the unquoted one.
                reader_quoting = getattr(csv, 'QUOTE_NOTNULL', None)
                if reader_quoting is None:
                    ## Python before 3.12 cannot tell them apart - an empty field is read as
                    ## an empty string and stays one
                    self.print_log_message('WARNING',
                        "config_parser: convert_csv_to_utf8: This Python cannot distinguish an unquoted empty CSV field from a quoted one (csv.QUOTE_NOTNULL needs Python 3.12). An empty field is migrated as an empty string, which a column that is not text refuses - Python 3.12 or newer is needed for such a file.")
                    reader = csv.reader(infile, delimiter=csv_delimiter)
                else:
                    reader = csv.reader(infile, delimiter=csv_delimiter, quoting=reader_quoting)
                writer = csv.writer(outfile, delimiter=csv_delimiter, quoting=csv.QUOTE_MINIMAL)

                for row in reader:
                    processed_row = []

                    if csv_delimiter == ',' and expected_types and len(row) > len(expected_types):
                        if counter == 0:
                            self.print_log_message('DEBUG3', f"config_parser: convert_csv_to_utf8: Table {source_table_name}: Row {counter+1} has {len(row)} columns, expected {len(expected_types)}. Attempting to heal decimal splits.")
                        merged_row = []
                        i = 0
                        col_idx = 0

                        while i < len(row):
                            field = row[i]

                            if col_idx < len(expected_types):
                                expected_type_info = expected_types[col_idx]
                                expected_type = expected_type_info['type']
                                expected_scale = expected_type_info['scale']

                                # Skip merging if scale is explicitly 0 or None (not specified) since no decimal parts exist
                                has_decimal_scale = expected_scale is not None and expected_scale > 0

                                if expected_type in ('FLOAT', 'REAL', 'DOUBLE', 'DECIMAL', 'NUMERIC') and has_decimal_scale and len(row) - i > len(expected_types) - col_idx:
                                    if i + 1 < len(row):
                                        next_field = row[i+1]
                                        # Only merge if BOTH parts are purely numeric (representing a split comma decimal).
                                        # A None is an empty field of the source, which stands for NULL and is never
                                        # part of a split decimal - it has no string methods either.
                                        is_int_part = field is not None and (field.isdigit() or (field.startswith('-') and field[1:].isdigit()))
                                        if is_int_part and next_field is not None and next_field.isdigit():
                                            if counter < 10:  # Only loudly log the first few occurrences to avoid log spam
                                                self.print_log_message('DEBUG3', f"config_parser: convert_csv_to_utf8: Table {source_table_name}: Row {counter+1}, Col {col_idx+1}: Merging split decimal parts '{field}' and '{next_field}' into '{field}.{next_field}'")
                                            field = f"{field}.{next_field}"
                                            i += 1

                            merged_row.append(field)
                            col_idx += 1
                            i += 1

                        if counter < 5 and len(merged_row) != len(row):
                             self.print_log_message('DEBUG3', f"config_parser: convert_csv_to_utf8: Table {source_table_name}: Row {counter+1} length reduced from {len(row)} to {len(merged_row)} after healing process.")
                        row = merged_row

                    if counter == 0 and source_columns:
                        types_str = ','.join([f"{t['type']}({t['scale']})" for t in expected_types])
                        self.print_log_message('DEBUG3', f"config_parser: convert_csv_to_utf8: Table {source_table_name}: Expected types: {types_str}")
                        self.print_log_message('DEBUG3', f"config_parser: convert_csv_to_utf8: Table {source_table_name}: First row fields: {row}")

                    for column_index, field in enumerate(row):
                        ## None is the unquoted empty field, which stands for NULL, and
                        ## '(null)' is what some exporters of the source write for it
                        if field is None or field == '(null)':
                            processed_row.append(null_symbol)
                        else:
                            expected_type = expected_types[column_index]['type'] if column_index < len(expected_types) else ''
                            if ',' in field and expected_type in self.NUMERIC_DATA_TYPES:
                                converted_number = self.convert_decimal_separator(field)
                                if converted_number != field:
                                    if counter < 5:
                                        self.print_log_message('DEBUG3', f"config_parser: convert_csv_to_utf8: Table {source_table_name}: Row {counter+1}, column {column_index+1}: number '{field}' written with a decimal comma converted to '{converted_number}'")
                                    processed_row.append(converted_number)
                                    continue
                            match = ts_pattern.match(field)
                            if match:
                                date_part = match.group(1)
                                hour = match.group(2)
                                minute = match.group(3)
                                second = match.group(4)
                                frac = match.group(5)
                                if frac:
                                    processed_row.append(f"{date_part} {hour}:{minute}:{second}.{frac}")
                                else:
                                    processed_row.append(f"{date_part} {hour}:{minute}:{second}")
                            else:
                                processed_row.append(field)

                    writer.writerow(processed_row)
                    counter += 1

            self.print_log_message('INFO', f"config_parser: convert_csv_to_utf8: Processed {counter} lines from {input_csv_data_file} and wrote to {output_csv_data_file} - source file size: {source_file_size} - processing time: {datetime.now() - processing_start_time}")

            data_source_settings['converted_file_name'] = output_csv_data_file

        except Exception as e:
            self.print_log_message('ERROR', f"config_parser: convert_csv_to_utf8: ({part_name}): {e}")
            raise e

    def convert_unl_to_csv(self, data_source_settings, source_columns, target_columns):
        part_name = 'convert_unl_to_csv start'
        try:
            input_unl_data_file = data_source_settings['file_name']
            output_csv_data_file = data_source_settings['converted_file_name']
            source_table_name = data_source_settings['source_table_name']
            file_size_bytes = data_source_settings.get('file_size', None)
            if file_size_bytes is not None:
                try:
                    file_size_bytes = int(file_size_bytes)
                    file_size_gb = file_size_bytes / (1024 ** 3)
                    source_file_size = f"{file_size_bytes} B / {file_size_gb:.2f} GB"
                except Exception:
                    source_file_size = str(file_size_bytes)
            else:
                source_file_size = "Unknown"

            unl_delimiter = data_source_settings['format_options'].get('delimiter', '|')
            null_symbol = data_source_settings.get('null_symbol', '\\N')
            processing_start_time = data_source_settings.get('processing_start_time', datetime.now())

            expected_types = []
            for ord_num, column_info in target_columns.items():
                expected_types.append(column_info['data_type'].upper())

            if not input_unl_data_file or not output_csv_data_file:
                self.print_log_message('ERROR', "config_parser: convert_unl_to_csv: Both 'unl_data_file' and 'csv_data_file' must be specified in the settings.")
                raise ValueError("Both 'unl_data_file' and 'csv_data_file' must be specified in the settings.")
            if not os.path.exists(input_unl_data_file):
                self.print_log_message('ERROR', f"config_parser: convert_unl_to_csv: Input UNL data file '{input_unl_data_file}' does not exist.")
                raise FileNotFoundError(f"Input UNL data file '{input_unl_data_file}' does not exist.")
            try:

                def conversion(s, expected_type=None):
                    if s == '':
                        return None
                    if expected_type in ('TEXT', 'VARCHAR', 'CHAR'):
                        if s == r'\ ':
                            return ''
                        return str(s)
                    if expected_type in ('INT', 'INTEGER', 'SMALLINT', 'BIGINT'):
                        try:
                            return int(s)
                        except ValueError:
                            return str(s)
                    if expected_type in ('FLOAT', 'REAL', 'DOUBLE', 'DECIMAL', 'NUMERIC'):
                        # try:
                        #     return float(s)
                        # except ValueError:
                        return str(s).replace(',', '.')
                    if expected_type in ('TIMESTAMP', 'DATETIME'):
                        if isinstance(s, datetime):
                            return s
                        else:
                            try:
                                return datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f')
                            except ValueError:
                                try:
                                    return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
                                except ValueError:
                                    try:
                                        return datetime.strptime(s, '%d-%m-%Y %H:%M:%S.%f')
                                    except ValueError:
                                        try:
                                            return datetime.strptime(s, '%d-%m-%Y %H:%M:%S')
                                        except ValueError:
                                            return str(s)
                    if expected_type in ('DATE', 'TIME'):
                        if isinstance(s, datetime):
                            return s
                        else:
                            try:
                                return datetime.strptime(s, '%Y-%m-%d').date()
                            except ValueError:
                                try:
                                    return datetime.strptime(s, '%Y.%m.%d').date()
                                except ValueError:
                                    try:
                                        return datetime.strptime(s, '%d-%m-%Y').date()
                                    except ValueError:
                                        try:
                                            return datetime.strptime(s, '%d.%m.%Y').date()
                                        except ValueError:
                                            return str(s)
                    if expected_type in ('BOOLEAN', 'BOOL'):
                        if s.lower() in ('true', '1', 'yes', 't'):
                            return True
                        elif s.lower() in ('false', '0', 'no', 'f'):
                            return False
                        else:
                            return str(s)
                    # if re.match(r'^0+\d+$', s):
                    # try:
                    #     if re.fullmatch(r'[0-9]+([.,][0-9]+)', s):
                    #         return float(s)
                    #     return int(s)
                    # except ValueError:
                    return s


                def determine_expected_delimiters():
                    sample_size = 100000
                    delimiter_counts = []

                    with open(input_unl_data_file, 'r', encoding='utf-8', newline='\n') as infile:
                        buffer = ""
                        for _, line in zip(range(sample_size), infile):
                            # self.print_log_message('DEBUG3', f"config_parser: determine_expected_delimiters: convert_unl_to_csv - determine_expected_delimiters: Reading line for sample: {line}")
                            if '\r' in line:
                                line = line.replace('\r', '')
                            line = line.rstrip('\n')
                            if line.endswith('\\'):
                                line = line.rstrip('\\')
                                line = line.rstrip('\r')  # remove windows and unix line endings
                                buffer += line
                                continue
                            else:
                                buffer += line.rstrip('\n').rstrip('\r')  # remove windows and unix line endings

                            # Record is complete when line does not end with backslash
                            # Replace double backslash (escaped backslash) with a placeholder
                            backslash_placeholder = "<<ESCAPED_BACKSLASH>>"
                            record_processed = buffer.replace('\\\\', backslash_placeholder)

                            # Replace escaped unl delimiter (e.g., \|) with a placeholder
                            delimiter_placeholder = "<<ESCAPED_DELIMITER>>"
                            escaped_delim_pattern = rf'\\{re.escape(unl_delimiter)}'
                            record_processed = re.sub(escaped_delim_pattern, delimiter_placeholder, record_processed)

                            # Now simply count the occurrences of the unl delimiter
                            delimiter_count = record_processed.count(unl_delimiter)
                            # self.print_log_message('DEBUG3', f"config_parser: determine_expected_delimiters: convert_unl_to_csv - determine_expected_delimiters: Processed record: {record_processed}, Delimiter count: {delimiter_count}")

                            # Only count records with at least one delimiter
                            if delimiter_count > 0:
                                delimiter_counts.append(delimiter_count)
                            buffer = ""
                    # self.print_log_message('DEBUG3', f"config_parser: determine_expected_delimiters: convert_unl_to_csv - determine_expected_delimiters: Sampled delimiter counts: {delimiter_counts}")
                    if not delimiter_counts:
                        return None
                    count_freq = Counter(delimiter_counts)
                    max_occurrence = max(count_freq.values())
                    self.print_log_message('DEBUG3', f"config_parser: determine_expected_delimiters: convert_unl_to_csv - determine_expected_delimiters: Delimiter counts frequency: {count_freq}, Max occurrence: {max_occurrence}")
                    # Find all delimiter counts with the highest occurrence
                    candidates = [count for count, freq in count_freq.items() if freq == max_occurrence]
                    # Return the largest delimiter count among the candidates
                    return max(candidates)

                self.print_log_message('DEBUG', f"config_parser: determine_expected_delimiters: convert_unl_to_csv: ({part_name}): Converting UNL file '{input_unl_data_file}' to CSV file '{output_csv_data_file}' with delimiter '{unl_delimiter}' - source file size: {source_file_size}")
                # First analyze the input file to determine the expected number of delimiters per line
                part_name = "determine_expected_delimiters"
                expected_delimiters = determine_expected_delimiters()
                self.print_log_message('DEBUG', f"config_parser: determine_expected_delimiters: convert_unl_to_csv: ({part_name}): UNL file '{input_unl_data_file}' - found delimiters count: {expected_delimiters} - source file size: {source_file_size}")

                with open(input_unl_data_file, 'r', encoding='utf-8', newline='\n') as infile, \
                    open(output_csv_data_file, 'w', newline='\n', encoding='utf-8') as outfile:

                    csv_writer = csv.writer(outfile, delimiter=unl_delimiter, quoting=csv.QUOTE_MINIMAL)
                    buffer = ""
                    counter = 0

                    for line in infile:
                        part_name = f"process inline {counter}"

                        # Must be done in this order - first escape backslashes, then escaped delimiters
                        # Due to a corner case - backslash at the end of the text column, which is stored as '\\|' in UNL

                        # Temporarily replace '\\' (escaped backslash) with a unique placeholder
                        # This happens when text in the column ends with a backslash
                        line = line.replace('\\\\', '<<ESCAPED_BACKSLASH>>')

                        # Replace escaped unl delimiter (e.g., \|) with a unique placeholder to avoid splitting inside text fields
                        delimiter_placeholder = "<<ESCAPED_DELIMITER>>"
                        escaped_delim_pattern = rf'\\{re.escape(unl_delimiter)}'
                        line = re.sub(escaped_delim_pattern, delimiter_placeholder, line)

                        # Remove any trailing whitespace characters
                        # UNL lines have clear endings, so we can safely strip them
                        line = line.rstrip()
                        counter += 1

                        # self.print_log_message('DEBUG3', f"config_parser: determine_expected_delimiters: convert_unl_to_csv: ({part_name}): line: {line}")
                        # self.print_log_message('DEBUG3', f"config_parser: determine_expected_delimiters: convert_unl_to_csv: ({part_name}): buffer: {buffer}")

                        # If line ends with a backslash, it means the line continues
                        # We append it to the buffer without the backslash at the end and continue to the next line
                        if line.endswith('\\'):
                            buffer += line[:-1] + '\n'
                            continue
                        else:
                            buffer += line

                        # Count the number of unl delimiters (escaped delimiters are already replaced with placeholders)
                        delimiter_count = buffer.count(unl_delimiter)
                        if delimiter_count < expected_delimiters:
                            continue

                        # self.print_log_message('DEBUG3', f"config_parser: determine_expected_delimiters: convert_unl_to_csv: ({part_name}): AFTER buffer: {buffer}")

                        # Remove only the last trailing unl_delimiter
                        # only at the end of the last line in the buffer
                        lines = buffer.rstrip('\n').split('\n')
                        if lines and lines[-1].endswith(unl_delimiter):
                            lines[-1] = re.sub(re.escape(unl_delimiter) + r'$', '', lines[-1])
                        record = '\n'.join(lines)

                        # Replace "^M" text with carriage return character (\r) if present
                        record = record.replace('^M', '\r')
                        # replace "\r" characters with empty string to avoid breaking CSV format
                        record = record.replace('\r', '')

                        # Split on '|' not preceded by a backslash (escaped pipe inside text column)
                        # fields = re.split(r'(?<!\\)\|', record)
                        fields = re.split(rf'(?<!\\){re.escape(unl_delimiter)}', record, flags=re.MULTILINE)

                        # Replace escaped unl_delimiter (e.g., '\|') inside texts with unl_delimiter
                        # fields = [field.replace(r'\|', '|') for field in fields]
                        fields = [field.replace(f'\\{unl_delimiter}', unl_delimiter) for field in fields]

                        # Restore escaped backslash characters '\\'
                        fields = [field.replace('<<ESCAPED_BACKSLASH>>', '\\') for field in fields]

                        # Restore escaped unl_delimiter characters
                        fields = [field.replace('<<ESCAPED_DELIMITER>>', unl_delimiter) for field in fields]

                        processed_fields = [conversion(field, expected_types[i]) if i < len(expected_types) else conversion(field) for i, field in enumerate(fields)]
                        processed_fields = [null_symbol if field is None and field != '' else field for field in processed_fields]

                        if counter == 1:
                            types_str = ','.join([type(field).__name__ for field in processed_fields])
                            self.print_log_message('DEBUG3', f"config_parser: determine_expected_delimiters: convert_unl_to_csv: Table {source_table_name}: Field types: {types_str}")
                            self.print_log_message('DEBUG3', f"config_parser: determine_expected_delimiters: convert_unl_to_csv: Table {source_table_name}: Expected types: {expected_types}")
                            self.print_log_message('DEBUG3', f"config_parser: determine_expected_delimiters: convert_unl_to_csv: Table {source_table_name}: row: {counter}: Processed fields: {processed_fields}")

                        part_name = f"writerow {counter}"
                        csv_writer.writerow(processed_fields)
                        buffer = ""

                self.print_log_message('INFO', f"config_parser: determine_expected_delimiters: convert_unl_to_csv: Processed {counter} lines from {input_unl_data_file} and wrote to {output_csv_data_file} - source file size: {source_file_size} - processing time: {datetime.now() - processing_start_time}")

            except Exception as e:
                self.print_log_message('ERROR', f"config_parser: determine_expected_delimiters: convert_unl_to_csv: ({part_name}) Error converting UNL to CSV: {e}")
                raise e
        except Exception as e:
            self.print_log_message('ERROR', f"config_parser: determine_expected_delimiters: convert_unl_to_csv: ({part_name}): {e}")
            raise e

    def split_big_unl_file(self, data_source_settings):
        split_threshold_bytes = self.get_source_data_export_big_files_split_threshold_bytes()
        chunk_size_bytes = self.get_source_data_export_big_files_split_chunk_size_bytes()
        source_file_size = data_source_settings.get('file_size', None)
        source_file_name = data_source_settings.get('file_name', None)
        source_file_basename = os.path.basename(source_file_name)
        converted_file_name = data_source_settings.get('converted_file_name', None)
        converted_file_path = os.path.dirname(os.path.abspath(converted_file_name))
        delimiter = data_source_settings.get('format_options', {}).get('delimiter', '|').encode('utf-8')
        part_size_bytes = self.get_source_data_export_big_files_split_chunk_size_bytes()
        continuation_seq = b'\r\\'
        source_file_parts = []
        converted_file_parts = []

        if source_file_size is not None and source_file_size > split_threshold_bytes:

            with open(source_file_name, 'rb') as infile:
                part_num = 1
                line_count = 0
                part_bytes = 0

                out_name = os.path.join(converted_file_path, f"{source_file_basename}.{str(part_num).zfill(4)}")
                source_file_parts.append(out_name)
                converted_file_part = f"{converted_file_name}.{str(part_num).zfill(4)}"
                converted_file_parts.append(converted_file_part)

                outfile = open(out_name, 'wb')
                buffer = b''
                for line in infile:
                    buffer += line
                    if buffer.rstrip(b'\n').endswith(delimiter):
                        row_bytes = len(buffer)
                        if part_bytes + row_bytes > part_size_bytes and line_count > 0:
                            self.print_log_message('INFO', f"config_parser: split_big_unl_file: Writing part {part_num} to {out_name} - logical rows: {line_count}, bytes: {part_bytes}")
                            outfile.close()
                            part_num += 1
                            out_name = os.path.join(converted_file_path, f"{source_file_basename}.{str(part_num).zfill(4)}")
                            source_file_parts.append(out_name)
                            converted_file_part = f"{converted_file_name}.{str(part_num).zfill(4)}"
                            converted_file_parts.append(converted_file_part)

                            self.print_log_message('DEBUG', f"config_parser: split_big_unl_file: Creating new output file {out_name} for part {part_num} - size: {part_size_bytes} bytes")
                            outfile = open(out_name, 'wb')
                            part_bytes = 0
                            line_count = 0
                        outfile.write(buffer)
                        part_bytes += row_bytes
                        buffer = b''
                        line_count += 1
                    elif buffer.rstrip(b'\n').endswith(continuation_seq):
                        continue
                    else:
                        continue
                if buffer:
                    self.print_log_message('INFO', f"config_parser: split_big_unl_file: Writing remaining part {part_num} to {out_name} - logical rows: {line_count + 1}, bytes: {part_bytes + len(buffer)}")
                    outfile.write(buffer)
                outfile.close()

        else:
            self.print_log_message('DEBUG', f"config_parser: split_big_unl_file: Source file {source_file_name} is smaller than split threshold {split_threshold_bytes} bytes. No splitting needed.")
            source_file_parts.append(source_file_name)
            converted_file_parts.append(converted_file_name)

        return source_file_parts, converted_file_parts

    @staticmethod
    def const_connectivity_ddl():
        return 'ddl'

    @staticmethod
    def const_connectivity_odbc():
        return 'odbc'

    @staticmethod
    def const_connectivity_jdbc():
        return 'jdbc'

    @staticmethod
    def const_connectivity_native():
        return 'native'

### Main entry point

if __name__ == "__main__":
    print("This script is not meant to be run directly")
