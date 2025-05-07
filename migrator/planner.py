import os
import importlib
import constants
from migrator_logging import MigratorLogger
from migrator_tables import MigratorTables
import fnmatch
import traceback
import re

class Planner:
    def __init__(self, config_parser):
        self.config_parser = config_parser
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger
        self.source_connection = self.load_connector('source')
        self.target_connection = self.load_connector('target')
        self.migrator_tables = MigratorTables(self.logger, self.config_parser)
        self.on_error_action = self.config_parser.get_on_error_action()
        self.source_schema = self.config_parser.get_source_schema()
        self.target_schema = self.config_parser.get_target_schema()
        self.pre_script = self.config_parser.get_pre_migration_script()
        self.post_script = self.config_parser.get_post_migration_script()
        self.user_defined_types = {}

    def create_plan(self):
        try:
            self.pre_planning()

            self.run_prepare_user_defined_types()
            self.run_prepare_tables()
            self.run_prepare_views()

            self.migrator_tables.update_main_status('Planner', '', True, 'finished OK')

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
            self.migrator_tables.update_main_status('Planner', '', False, f'ERROR: {e}')
            self.handle_error(e, "Planner")

    def load_connector(self, source_or_target):
        """Dynamically load the database connector."""
        # Get the database type from the config
        database_type = self.config_parser.get_db_type(source_or_target)
        if self.config_parser.get_log_level() == 'DEBUG':
            self.logger.debug(f"Loading connector for {source_or_target} with database type: {database_type}")
        if source_or_target == 'target' and database_type != 'postgresql':
            raise ValueError("Target database type must be 'postgresql'")
        # Check if the database type is supported
        database_module = constants.MIGRATOR_MODULES.get(database_type)
        if not database_module:
            raise ValueError(f"Unsupported database type: {database_type}")
        # Import the module and get the class
        module_name, class_name = database_module.split(':')
        if not module_name or not class_name:
            raise ValueError(f"Invalid module format: {database_module}")
        # Import the module and get the class
        module = importlib.import_module(module_name)
        connector_class = getattr(module, class_name)
        return connector_class(self.config_parser, source_or_target)

    def pre_planning(self):
        try:
            self.logger.info("Running pre-planning actions...")

            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Target schema: {self.target_schema}")
                self.logger.debug(f"Pre migration script: {self.pre_script}")
                self.logger.debug(f"Post migration script: {self.post_script}")


            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug("Connecting to source and target databases...")
            self.check_database_connection(self.source_connection, "Source Database")
            self.check_database_connection(self.target_connection, "Target Database")

            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug("Checking scripts accessibility...")
            self.check_script_accessibility(self.pre_script)
            self.check_script_accessibility(self.post_script)

            self.target_connection.connect()
            if self.config_parser.should_drop_schema():
                self.logger.info(f"Dropping target schema '{self.target_schema}'...")
                self.target_connection.execute_query(f"DROP SCHEMA IF EXISTS {self.target_schema} CASCADE")

            self.target_connection.execute_query(f"CREATE SCHEMA IF NOT EXISTS {self.target_schema}")
            self.target_connection.disconnect()

            self.run_pre_migration_script()

            self.logger.info("Creating migration plan...")
            self.migrator_tables.create_all()
            self.migrator_tables.insert_main('Planner', '')
            self.migrator_tables.prepare_data_types_substitution()
            self.migrator_tables.prepare_default_values_substitution()
            self.migrator_tables.prepare_data_migration_limitation()
            self.migrator_tables.prepare_remote_objects_substitution()

            self.logger.info("Pre-planning part done successfully.")
        except Exception as e:
            self.handle_error(e, "Pre-planning runs")

    def run_prepare_tables(self):
        self.logger.info("Planner - Preparing tables...")
        source_tables = self.source_connection.fetch_table_names(self.source_schema)
        include_tables = self.config_parser.get_include_tables()
        exclude_tables = self.config_parser.get_exclude_tables() or []

        if self.config_parser.get_log_level() == 'DEBUG':
            self.logger.debug(f"Source schema: {self.source_schema}")
            self.logger.debug(f"Source tables: {source_tables}")
            self.logger.debug(f"Include tables: {include_tables}")
            self.logger.debug(f"Exclude tables: {exclude_tables}")

        for order_num, table_info in source_tables.items():
            self.logger.info(f"Processing table ({order_num}/{len(source_tables)}): {table_info['table_name']}")
            if not any(fnmatch.fnmatch(table_info['table_name'], pattern) for pattern in include_tables):
                continue
            if any(fnmatch.fnmatch(table_info['table_name'], pattern) for pattern in exclude_tables):
                self.logger.info(f"Table {table_info['table_name']} is excluded from migration.")
                continue

            source_columns = []
            target_columns = []
            target_table_sql = None
            settings = {}
            try:
                source_columns = self.source_connection.fetch_table_columns(self.source_schema, table_info['table_name'], self.migrator_tables)
                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Fetched source columns: {source_columns}")

                for col_order_num, column_info in source_columns.items():

                    if self.config_parser.get_log_level() == 'DEBUG':
                        self.logger.debug(f"Checking for data types / default values substitutions for column {column_info}...")
                    substitution = self.migrator_tables.check_data_types_substitution(column_info['type'])
                    if substitution and substitution != (None, None):
                        column_info['type'], column_info['length'] = self.migrator_tables.check_data_types_substitution(column_info['default'])
                        if self.config_parser.get_log_level() == 'DEBUG':
                            self.logger.debug(f"Substituted data type: {column_info['type']}, length: {column_info['length']}")

                    # checking for default values substitution with the new data type
                    if column_info['default'] != '':
                        substitution = self.migrator_tables.check_default_values_substitution(column_info['name'], column_info['type'], column_info['default'])
                        if substitution and substitution != None:
                            column_info['default'] = substitution
                            if self.config_parser.get_log_level() == 'DEBUG':
                                self.logger.debug(f"Substituted default value: {column_info['default']}")

                    # if self.config_parser.get_log_level() == 'DEBUG':
                    #     self.logger.debug(f"1 default: {column_info['default']}")

                settings = {
                    'target_db_type': self.config_parser.get_target_db_type(),
                    'target_schema': self.target_schema,
                    'target_table_name': table_info['table_name'],
                    'source_columns': source_columns,
                }
                target_columns, target_table_sql = self.source_connection.convert_table_columns(settings)
                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Target columns: {target_columns}")
                    self.logger.debug(f"Target table SQL: {target_table_sql}")
                self.migrator_tables.insert_tables(self.source_schema, table_info['table_name'], table_info['id'], source_columns,
                                                   self.target_schema, table_info['table_name'], target_columns, target_table_sql, table_info['comment'])
            except Exception as e:
                self.migrator_tables.insert_tables(self.source_schema, table_info['table_name'], table_info['id'], source_columns,
                                                   self.target_schema, table_info['table_name'], target_columns, target_table_sql, table_info['comment'])
                self.handle_error(e, f"Table {table_info['table_name']}")
                continue

            if self.config_parser.should_migrate_indexes():
                settings['source_table_id'] = table_info['id']
                settings['source_table_name'] = table_info['table_name']
                settings['source_schema'] = self.source_schema
                settings['target_schema'] = self.target_schema
                settings['target_table_name'] = table_info['table_name']
                settings['target_columns'] = target_columns

                indexes = self.source_connection.fetch_indexes(settings)
                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Indexes: {indexes}")
                if indexes:
                    for _, index_details in indexes.items():
                        values = {}
                        values['source_schema'] = self.source_schema
                        values['source_table'] = table_info['table_name']
                        values['source_table_id'] = table_info['id']
                        values['index_owner'] = index_details['owner']
                        values['index_name'] = index_details['name']
                        values['index_type'] = index_details['type']
                        values['target_schema'] = self.target_schema
                        values['target_table'] = table_info['table_name']
                        values['index_sql'] = index_details['sql']
                        values['index_columns'] = index_details['columns']
                        values['index_columns_count'] = index_details['columns_count']
                        values['index_columns_data_types'] = index_details['columns_data_types']
                        values['index_comment'] = index_details['comment']
                        self.migrator_tables.insert_indexes( values )
                    self.logger.info(f"Index {index_details['name']} for table {table_info['table_name']}")
                else:
                    self.logger.info(f"No indexes found for table {table_info['table_name']}.")
            else:
                self.logger.info("Skipping index migration.")

            if self.config_parser.should_migrate_constraints():
                settings = {
                    'source_table_id': table_info['id'],
                    'source_schema': self.source_schema,
                    'source_table_name': table_info['table_name'],
                    'target_schema': self.target_schema,
                    'target_table_name': table_info['table_name'],
                }
                constraints = self.source_connection.fetch_constraints(settings)
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
                            constraint_details['sql'],
                            constraint_details['comment']
                        )
                    self.logger.info(f"Constraint {constraint_details['name']} for table {table_info['table_name']}")
                else:
                    self.logger.info(f"No constraints found for table {table_info['table_name']}.")
            else:
                self.logger.info("Skipping constraint migration.")

            if self.config_parser.should_migrate_triggers():
                triggers = self.source_connection.fetch_triggers(table_info['id'], self.source_schema, table_info['table_name'])
                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Triggers: {triggers}")
                if triggers:
                    for _, trigger_details in triggers.items():

                        settings = {
                            'source_schema': self.config_parser.get_source_schema(),
                            'target_schema': self.config_parser.get_target_schema(),
                        }
                        converted_code = self.source_connection.convert_trigger(trigger_details['sql'], settings)

                        if self.config_parser.get_log_level() == 'DEBUG':
                            self.logger.debug(f"Source trigger code: {trigger_details['sql']}")
                            self.logger.debug(f"Converted trigger code: {converted_code}")

                        self.migrator_tables.insert_trigger(
                            self.source_schema,
                            table_info['table_name'],
                            table_info['id'],
                            self.target_schema,
                            table_info['table_name'],
                            trigger_details['id'],
                            trigger_details['name'],
                            trigger_details['event'],
                            trigger_details['new'],
                            trigger_details['old'],
                            trigger_details['sql'],
                            converted_code,
                            trigger_details['comment']
                        )
                    self.logger.info(f"Trigger {trigger_details['name']} for table {table_info['table_name']}")
                else:
                    self.logger.info(f"No triggers found for table {table_info['table_name']}.")
            else:
                self.logger.info("Skipping trigger migration.")

            self.logger.info(f"Table {table_info['table_name']} processed successfully.")
        self.logger.info("Planner - Tables processed successfully.")

    def run_prepare_views(self):
        self.logger.info("Planner - Preparing views...")
        if self.config_parser.should_migrate_views():
            self.logger.info("Processing views...")
            views = self.source_connection.fetch_views_names(self.source_schema)

            include_views = self.config_parser.get_include_views()
            exclude_views = self.config_parser.get_exclude_views() or []

            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Source views: {views}")
                self.logger.debug(f"Include views: {include_views}")
                self.logger.debug(f"Exclude views: {exclude_views}")

            for order_num, view_info in views.items():
                self.logger.info(f"Processing view ({order_num}): {view_info}")
                if not any(fnmatch.fnmatch(view_info['view_name'], pattern) for pattern in include_views):
                    self.logger.info(f"View {view_info['view_name']} does not match patterns for migration.")
                    continue
                if any(fnmatch.fnmatch(view_info['view_name'], pattern) for pattern in exclude_views):
                    self.logger.info(f"View {view_info['view_name']} is excluded from migration.")
                    continue
                self.logger.info(f"View {view_info['view_name']} is included for migration.")
                settings = {
                    'view_id': view_info['id'],
                    'source_schema': self.config_parser.get_source_schema(),
                    'source_view_name': view_info['view_name'],
                    'target_schema': self.config_parser.get_target_schema(),
                    'target_view_name': view_info['view_name'],
                }
                view_sql = self.source_connection.fetch_view_code(settings)
                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Source view SQL: {view_sql}")
                settings = {
                    'source_database': self.config_parser.get_source_db_name(),
                    'source_schema': self.config_parser.get_source_schema(),
                    'target_schema': self.config_parser.get_target_schema(),
                }
                converted_view_sql = self.source_connection.convert_view_code(view_sql, settings)

                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug("Checking for remote objects substitution in view SQL...")
                rows = self.migrator_tables.get_records_remote_objects_substitution()
                if rows:
                    for row in rows:
                        if self.config_parser.get_log_level() == 'DEBUG':
                            self.logger.debug(f"Views - remote objects substituting {row[0]} with {row[1]}")
                        converted_view_sql = re.sub(re.escape(row[0]), row[1], converted_view_sql, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)

                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Converted view SQL: {converted_view_sql}")
                self.migrator_tables.insert_view(self.source_schema, view_info['view_name'], view_info['id'], view_sql,
                                                 self.target_schema, view_info['view_name'], converted_view_sql, view_info['comment'])
                self.logger.info(f"View {view_info['view_name']} processed successfully.")
            self.logger.info("Views processed successfully.")
        else:
            self.logger.info("Skipping views migration.")
        self.logger.info("Planner - Views processed successfully.")

    def run_prepare_user_defined_types(self):
        self.logger.info("Planner - Preparing user defined types...")
        user_defined_types = self.source_connection.fetch_user_defined_types(self.source_schema)
        if self.config_parser.get_log_level() == 'DEBUG':
            self.logger.debug(f"User defined types: {user_defined_types}")
        if user_defined_types:
            for order_num, type_info in user_defined_types.items():
                type_sql = type_info['sql']
                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Source type SQL: {type_sql}")
                converted_type_sql = type_sql.replace(f'{self.source_schema}.', f'{self.target_schema}.')
                if self.config_parser.get_log_level() == 'DEBUG':
                    self.logger.debug(f"Converted type SQL: {converted_type_sql}")
                self.migrator_tables.insert_user_defined_type(self.source_schema, type_info['type_name'], type_sql,
                                                            self.target_schema, type_info['type_name'], converted_type_sql, type_info['comment'])
                self.logger.info(f"User defined type {type_info['type_name']} processed successfully.")
            self.logger.info("Planner - User defined types processed successfully.")
        else:
            self.logger.info("No user defined types found.")

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
            cursor = connector.connection.cursor()
            query = connector.testing_select()
            cursor.execute(query)
            result = cursor.fetchone()
            if result[0] != 1:
                raise ConnectionError(f"Connection to {db_name} failed.")
            self.logger.info(f"Connection to {db_name} is OK.")
            cursor.close()
            connector.disconnect()
        except Exception as e:
            raise ConnectionError(f"Failed to connect to {db_name}: {e}")

    def handle_error(self, e, description=None):
        self.logger.error(f"An error in {self.__class__.__name__} ({description}): {e}")
        self.logger.error(traceback.format_exc())
        if self.on_error_action == 'stop':
            self.logger.error("Stopping due to error.")
            exit(1)
