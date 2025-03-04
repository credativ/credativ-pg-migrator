import os
from migrator_logging import MigratorLogger
from postgresql_connector import PostgreSQLConnector
from sybase_ase_connector import SybaseASEConnector
from informix_connector import InformixConnector
from migrator_tables import MigratorTables
import fnmatch
import traceback

class Planner:
    def __init__(self, config_parser):
        self.config_parser = config_parser
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger
        self.source_connection = self.connect_to_source_db()
        self.target_connection = self.connect_to_target_db()
        self.migrator_tables = MigratorTables(self.logger, self.config_parser)
        self.on_error_action = self.config_parser.get_on_error_action()
        self.source_schema = self.config_parser.get_source_schema()
        self.target_schema = self.config_parser.get_target_schema()
        self.pre_script = self.config_parser.get_pre_migration_script()
        self.post_script = self.config_parser.get_post_migration_script()

    def create_plan(self):
        try:
            self.pre_planning()

            source_tables = self.source_connection.fetch_table_names(self.source_schema)
            include_tables = self.config_parser.get_include_tables()
            exclude_tables = self.config_parser.get_exclude_tables() or []

            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Source schema: {self.source_schema}")
                self.logger.debug(f"Source tables: {source_tables}")
                self.logger.debug(f"Include tables: {include_tables}")
                self.logger.debug(f"Exclude tables: {exclude_tables}")

            for order_num, table_info in source_tables.items():
                self.logger.info(f"Processing table ({order_num}/{table_info['id']}): {table_info['table_name']}")
                if not any(fnmatch.fnmatch(table_info['table_name'], pattern) for pattern in include_tables):
                    continue
                if any(fnmatch.fnmatch(table_info['table_name'], pattern) for pattern in exclude_tables):
                    self.logger.info(f"Table {table_info['table_name']} is excluded from migration.")
                    continue

                source_columns = []
                target_columns = []
                target_table_sql = None
                try:
                    source_columns = self.source_connection.fetch_table_columns(self.source_schema, table_info['table_name'], self.migrator_tables)
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Source columns: {source_columns}")
                    target_columns, target_table_sql = self.source_connection.convert_table_columns(self.config_parser.get_target_db_type(), self.target_schema, table_info['table_name'], source_columns)
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Target columns: {target_columns}")
                        self.logger.debug(f"Target table SQL: {target_table_sql}")
                    self.migrator_tables.insert_tables(self.source_schema, table_info['table_name'], table_info['id'], source_columns, self.target_schema, table_info['table_name'], target_columns, target_table_sql)
                except Exception as e:
                    self.migrator_tables.insert_tables(self.source_schema, table_info['table_name'], table_info['id'], source_columns, self.target_schema, table_info['table_name'], target_columns, target_table_sql)
                    self.handle_error(e, f"Table {table_info['table_name']}")
                    continue

                if self.config_parser.should_migrate_indexes():
                    indexes = self.source_connection.fetch_indexes(table_info['id'], self.target_schema, table_info['table_name'])
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Indexes: {indexes}")
                    if indexes:
                        for _, index_details in indexes.items():
                            self.migrator_tables.insert_indexes(
                                self.source_schema,
                                table_info['table_name'],
                                table_info['id'],
                                index_details['name'],
                                index_details['type'],
                                self.target_schema,
                                table_info['table_name'],
                                index_details['sql'],
                                index_details['columns']
                            )
                        self.logger.info(f"Index {index_details['name']} for table {table_info['table_name']}")
                    else:
                        self.logger.info(f"No indexes found for table {table_info['table_name']}.")
                else:
                    self.logger.info("Skipping index migration.")

                if self.config_parser.should_migrate_constraints():
                    constraints = self.source_connection.fetch_constraints(table_info['id'], self.target_schema, table_info['table_name'])
                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Constraints: {constraints}")
                    if constraints:
                        for _, constraint_details in constraints.items():
                            self.migrator_tables.insert_constraints(
                                self.source_schema,
                                table_info['table_name'],
                                table_info['id'],
                                constraint_details['name'],
                                constraint_details['type'],
                                self.target_schema,
                                table_info['table_name'],
                                constraint_details['sql']
                            )
                        self.logger.info(f"Constraint {constraint_details['name']} for table {table_info['table_name']}")
                    else:
                        self.logger.info(f"No constraints found for table {table_info['table_name']}.")
                else:
                    self.logger.info("Skipping constraint migration.")

                self.logger.info(f"Table {table_info['table_name']} processed successfully.")

            self.migrator_tables.update_main_status('Planner', True, 'finished OK')

            try:
                self.source_connection.disconnect()
            except Exception as e:
                pass
            try:
                self.target_connection.disconnect()
            except Exception as e:
                pass

            self.logger.info("Planner phase done successfully.")
        except Exception as e:
            self.handle_error(e, "Planner")

    def pre_planning(self):
        try:
            self.logger.info("Running pre-planning actions...")

            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Target schema: {self.target_schema}")
                self.logger.debug(f"Pre migration script: {self.pre_script}")
                self.logger.debug(f"Post migration script: {self.post_script}")

            self.check_database_connection(self.source_connection, "Source Database")
            self.check_database_connection(self.target_connection, "Target Database")

            self.check_script_accessibility(self.pre_script)
            self.check_script_accessibility(self.post_script)

            self.target_connection.connect()
            self.target_connection.execute_query(f"CREATE SCHEMA IF NOT EXISTS {self.target_schema}")
            self.target_connection.disconnect()

            self.run_pre_migration_script()

            self.logger.info("Creating migration plan...")
            self.migrator_tables.create_all()
            self.migrator_tables.insert_main('Planner')
            self.migrator_tables.prepare_data_types_substitution()
            self.migrator_tables.prepare_default_values_substitution()

            self.logger.info("Pre-planning part done successfully.")
        except Exception as e:
            self.handle_error(e, "Pre-planning runs")

    def run_pre_migration_script(self):
        pre_migration_script = self.config_parser.get_pre_migration_script()
        if pre_migration_script:
            self.logger.info(f"Running pre-migration script '{pre_migration_script}' in target database.")
            try:
                self.target_connection.connect()
                self.target_connection.execute_sql_script(pre_migration_script)
                self.target_connection.disconnect()
                self.logger.info("Pre-migration script executed successfully.")
            except Exception as e:
                self.handle_error(e, "Pre-migration script")
        else:
            self.logger.info("No pre-migration script specified.")

    def connect_to_source_db(self):
        source_db_type = self.config_parser.get_source_db_type()
        if self.config_parser.get_log_level() == 'DEBUG':
            self.logger.debug(f"Connecting to source database with connection string: {self.config_parser.get_source_connect_string()}")
        if source_db_type == 'postgresql':
            return PostgreSQLConnector(self.config_parser, 'source')
        elif source_db_type == 'informix':
            return InformixConnector(self.config_parser, 'source')
        elif source_db_type == 'sybase_ase':
            return SybaseASEConnector(self.config_parser, 'source')
        else:
            raise ValueError(f"Unsupported source database type: {source_db_type}")

    def connect_to_target_db(self):
        target_db_type = self.config_parser.get_target_db_type()
        if self.config_parser.get_log_level() == 'DEBUG':
            self.logger.debug(f"Connecting to target database with connection string: {self.config_parser.get_target_connect_string()}")
        if target_db_type == 'postgresql':
            return PostgreSQLConnector(self.config_parser, 'target')
        # elif target_db_type == 'informix':
        #     return InformixConnector(self.config_parser, 'target')
        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

    def check_script_accessibility(self, script_path):
        if not script_path:
            return
        if not os.path.isfile(script_path):
            self.logger.error(f"Script {script_path} does not exist or is not accessible.")
            if self.config_parser.get_on_error_action() == 'stop':
                self.logger.error("Stopping execution due to error.")
                exit(1)
        self.logger.info(f"Script {script_path} is accessible.")

    def check_database_connection(self, connector, db_name):
        try:
            connector.connect()
            connector.execute_query("SELECT 1")
            self.logger.info(f"Connection to {db_name} is OK.")
            connector.disconnect()
        except Exception as e:
            raise ConnectionError(f"Failed to connect to {db_name}: {e}")

    def handle_error(self, e, description=None):
        self.logger.error(f"An error in {self.__class__.__name__} ({description}): {e}")
        self.logger.error(traceback.format_exc())
        if self.on_error_action == 'stop':
            self.logger.error("Stopping due to error.")
            exit(1)
