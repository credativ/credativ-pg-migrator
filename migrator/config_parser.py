import yaml
import constants

class ConfigParser:
    def __init__(self, args):
        self.args = args
        with open(self.args.config, 'r') as file:
            self.config = yaml.safe_load(file)
        self.validate_config()

    def validate_config(self):

        ## //TODO source.schema or source.owner is required - not both

        include_tables = self.config['include_tables']
        exclude_tables = self.config['exclude_tables']

        if type(include_tables) is str and include_tables == '.*' and exclude_tables is None:
            self.logger.info("Check of include_tables and exclude_tables passed - all tables will be included")
        elif type(include_tables) is list and exclude_tables is None:
            self.logger.info("Check of include_tables and exclude_tables passed - selected tables will be included")
        elif type(include_tables) is str and include_tables == '.*' and type(exclude_tables) is list:
            self.logger.info("Check of include_tables and exclude_tables passed - all tables will be included except for the ones specified")
        elif type(include_tables) is list and type(exclude_tables) is list:
            if include_tables & exclude_tables:
                raise ValueError("Configuration error: There are tables specified in both 'include_tables.specific_tables' and 'exclude_tables.specific_tables'.")

        if self.config.get('log_level') == 'DEBUG':
            self.logger.debug(f"Configuration validated: {self.config}")

    ## Databases
    def get_db_config(self, source_or_target):
        return self.config[source_or_target]

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

    def get_target_schema(self):
        target_config = self.get_target_config()
        return target_config.get('schema', target_config.get('owner', 'public'))

    def get_connect_string(self, source_or_target):
        if source_or_target not in ['source', 'target']:
            raise ValueError(f"Invalid source_or_target: {source_or_target}")
        connectivity = self.get_connectivity(source_or_target)
        db_config = self.config[source_or_target]
        if db_config['type'] == 'postgresql':
            if connectivity == None:
                return f"dbname={db_config['database']} user={db_config['username']} password={db_config['password']} host={db_config.get('host', 'localhost')} port={db_config['port']} sslmode={db_config.get('sslmode', 'prefer')}"
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
        else:
            raise ValueError(f"Unsupported database type: {db_config['type']}")

    def get_source_connect_string(self):
        return self.get_connect_string('source')

    def get_target_connect_string(self):
        return self.get_connect_string('target')


    ## Migrator
    def get_migrator_config(self):
        return self.config.get('migrator', {})

    def get_migrator_db_type(self):
        return self.get_migrator_config().get('type', None)

    def get_migrator_schema(self):
        return self.get_migrator_config().get('schema', constants.MIGRATOR_DEFAULT_SCHEMA)

    def get_migration_settings(self):
        return self.config['migration']

    def get_tables_config(self):
        return self.config.get('tables', []) # Default to empty list if not specified

    def get_protocol_name(self):
        return constants.MIGRATOR_TASKS_TABLE

    def get_protocol_name_main(self):
        return f"{self.get_protocol_name()}_main"

    def get_protocol_name_tables(self):
        return f"{self.get_protocol_name()}_tables"

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
        return self.config.get('default_values_substitution', {})

    ## Migration settings
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

    def get_include_tables(self):
        include_tables = self.config['include_tables']
        if type(include_tables) is str:
            return '.*'  # Pattern matching all table names
        elif type(include_tables) is list:
            return include_tables
        else:
            return []

    def get_exclude_tables(self):
        return self.config['exclude_tables']

    def get_include_funcprocs(self):
        include_funcprocs = self.config.get('include_funcprocs', [])
        if type(include_funcprocs) is str:
            return '.*'
        elif type(include_funcprocs) is list:
            return include_funcprocs
        else:
            return []

    def get_exclude_funcprocs(self):
        return self.config.get('exclude_funcprocs', [])

    def get_log_file(self):
        return self.args.log_file or constants.MIGRATOR_DEFAULT_LOG

    def get_log_level(self):
        if self.args.log_level:
            return self.args.log_level.upper()
        return 'INFO'


    def get_indent(self):
        return self.config.get('migrator', {}).get('indent', constants.MIGRATOR_DEFAULT_INDENT)

    def get_settings(self):
        return self.config.get('settings', {})
