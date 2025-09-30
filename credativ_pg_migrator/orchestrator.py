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

import concurrent.futures
import importlib
from credativ_pg_migrator.migrator_logging import MigratorLogger
from credativ_pg_migrator.migrator_tables import MigratorTables
from credativ_pg_migrator.constants import MigratorConstants
import traceback
import uuid
import fnmatch
import re
import time
import json
import os

class Orchestrator:
    def __init__(self, config_parser):
        self.config_parser = config_parser
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger
        self.source_connection = self.load_connector('source')
        self.target_connection = self.load_connector('target')
        self.migrator_tables = MigratorTables(self.logger, self.config_parser)
        self.on_error_action = self.config_parser.get_on_error_action()
        self.source_schema = self.config_parser.get_source_schema()
        self.target_schema = self.config_parser.get_target_schema()
        if self.config_parser.is_resume_after_crash():
            self.migrator_tables.insert_main('Orchestrator', 'Resume after crash')
            self.config_parser.print_log_message('INFO', "#############################################################################")
            self.config_parser.print_log_message('INFO', "Orchestration: Resuming migration after crash - stats from crashed migration:")
            self.config_parser.print_log_message('INFO', f"Orchestration: drop_unfinished-tables set to: {self.config_parser.should_drop_unfinished_tables()}")
            self.migrator_tables.print_migration_summary()
            self.config_parser.print_log_message('INFO', "#############################################################################")
            self.config_parser.print_log_message('INFO', "Orchestration: Continuing migration...")
        else:
            self.migrator_tables.insert_main('Orchestrator', '')

    def run(self):
        try:
            self.config_parser.print_log_message('INFO', "Starting Orchestrator...")

            if self.config_parser.is_resume_after_crash():
                self.config_parser.print_log_message('INFO', "Orchestrator: In current version of crash recovery we assume user defined types and domains already exist, so we skip them.")
            else:
                self.run_create_user_defined_types()
                self.check_pausing_resuming()

                ## migration of domains is a bit unclear currently
                ## domains in PostgreSQL are special data types
                ## But in Sybase ASE they are defined as sort of additional check constraint on the column
                # self.run_create_domains()

            # In case of crash recovery, we currently continue from this point as in normal migration
            if not self.config_parser.is_dry_run():
                self.run_migrate_tables()
                self.check_pausing_resuming()

                self.run_migrate_indexes('standard')
                self.check_pausing_resuming()

                self.run_migrate_constraints()
                self.check_pausing_resuming()

                self.run_migrate_views()
                self.check_pausing_resuming()

                self.run_migrate_funcprocs()
                self.check_pausing_resuming()

                self.run_migrate_triggers()
                self.check_pausing_resuming()

                self.run_migrate_indexes('function_based')
                self.check_pausing_resuming()

                self.run_migrate_comments()
                self.check_pausing_resuming()

                self.run_post_migration_script()
            else:
                self.config_parser.print_log_message('INFO', "Dry run mode enabled. No data migration performed.")

            self.config_parser.print_log_message('INFO', "Orchestration complete.")
            self.migrator_tables.update_main_status('Orchestrator', '', True, 'finished OK')

            self.migrator_tables.print_migration_summary()

            try:
                self.source_connection.disconnect()
            except Exception as e:
                pass
            try:
                self.target_connection.disconnect()
            except Exception as e:
                pass

        except Exception as e:
            self.migrator_tables.update_main_status('Orchestrator', '', False, f'ERROR: {e}')
            self.handle_error(e, 'orchestration')

    def load_connector(self, source_or_target):
        """Dynamically load the database connector."""
        # Get the database type from the config
        database_type = self.config_parser.get_db_type(source_or_target)
        self.config_parser.print_log_message( 'DEBUG', f"Loading connector for {source_or_target} with database type: {database_type}")
        if source_or_target == 'target' and database_type != 'postgresql':
            raise ValueError("Target database type must be 'postgresql'")
        # Check if the database type is supported
        database_module = MigratorConstants.get_modules().get(database_type)
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

    def run_post_migration_script(self):
        post_migration_script = self.config_parser.get_post_migration_script()
        if post_migration_script:
            self.config_parser.print_log_message('INFO', "Running post-migration script in target database.")
            try:
                self.target_connection.connect()
                self.target_connection.execute_sql_script(post_migration_script)
                self.target_connection.disconnect()
                self.config_parser.print_log_message('INFO', "Post-migration script executed successfully.")
            except Exception as e:
                self.handle_error(e, 'post-migration script')

    def run_migrate_tables(self):
        self.migrator_tables.insert_main('Orchestrator', 'tables migration')
        workers_requested = self.config_parser.get_parallel_workers_count()
        settings = {
            'source_db_type': self.config_parser.get_source_db_type(),
            'target_db_type': self.config_parser.get_target_db_type(),
            'create_tables': self.config_parser.should_create_tables(),
            'drop_tables': self.config_parser.should_drop_tables(),
            'truncate_tables': self.config_parser.should_truncate_tables(),
            'migrate_data': self.config_parser.should_migrate_data(),
            'batch_size': self.config_parser.get_batch_size(),
            'migrator_tables': self.migrator_tables,
            'resume_after_crash': self.config_parser.is_resume_after_crash(),
            'drop_unfinished_tables': self.config_parser.should_drop_unfinished_tables(),
        }

        self.config_parser.print_log_message('INFO', f"Starting {workers_requested} parallel workers to create tables in target database.")

        ## in case of crash recovery, we only migrate tables that are not yet migrated -> success is not True
        migrate_tables = self.migrator_tables.fetch_all_tables(self.config_parser.is_resume_after_crash())

        if len(migrate_tables) > 0:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers_requested) as executor:
                futures = {}
                for table_row in migrate_tables:
                    table_data = self.migrator_tables.decode_table_row(table_row)
                    # (table_data['primary_key_columns'],
                    # table_data['primary_key_columns_count'],
                    # table_data['primary_key_columns_types']) = self.migrator_tables.select_primary_key(table_data['target_schema'], table_data['target_table'])
                    # Submit tasks until we have workers_requested running, then as soon as one finishes, submit a new one
                    self.config_parser.print_log_message('DEBUG3', f"run_migrate_tables: futures running count: {len(futures)}")
                    while len(futures) >= workers_requested:
                        # Wait for the first completed future
                        done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                        for future in done:
                            table_done = futures.pop(future)
                            if future.result() == False:
                                if self.on_error_action == 'stop':
                                    self.config_parser.print_log_message('ERROR', "Stopping execution due to error.")
                                    exit(1)
                            else:
                                self.migrator_tables.update_table_status(table_done['id'], True, 'migrated OK')

                    # Submit the next task
                    future = executor.submit(self.table_worker, table_data, settings)
                    futures[future] = table_data

                # Process remaining futures
                self.config_parser.print_log_message('INFO', "Processing remaining futures")
                for future in concurrent.futures.as_completed(futures):
                    table_done = futures[future]
                    if future.result() == False:
                        if self.on_error_action == 'stop':
                            self.config_parser.print_log_message('ERROR', "Stopping execution due to error.")
                            exit(1)
                    else:
                        self.migrator_tables.update_table_status(table_done['id'], True, 'migrated OK')

            self.config_parser.print_log_message('INFO', "Tables processed successfully.")
        else:
            self.config_parser.print_log_message('INFO', "No tables to create.")

        self.migrator_tables.update_main_status('Orchestrator', 'tables migration', True, 'finished OK')

    def run_create_user_defined_types(self):
        self.migrator_tables.insert_main('Orchestrator', 'user defined types migration')
        self.config_parser.print_log_message('INFO', "Migrating user defined types.")
        user_defined_types = self.migrator_tables.fetch_all_user_defined_types()
        if len(user_defined_types) > 0:
            for type_row in user_defined_types:
                type_data = self.migrator_tables.decode_user_defined_type_row(type_row)
                self.config_parser.print_log_message('INFO', f"Creating user defined type {type_data['target_type_name']} in target database.")
                try:
                    self.target_connection.connect()
                    self.target_connection.execute_query(type_data['target_type_sql'])
                    self.migrator_tables.update_user_defined_type_status(type_data['id'], True, 'migrated OK')
                    self.config_parser.print_log_message('INFO', f"User defined type {type_data['target_type_name']} created successfully.")
                    self.target_connection.disconnect()
                except Exception as e:
                    self.migrator_tables.update_user_defined_type_status(type_data['id'], False, f'ERROR: {e}')
                    self.handle_error(e, f"create_user_defined_type {type_data['target_type_name']}")
            self.config_parser.print_log_message('INFO', "User defined types migrated successfully.")
        else:
            self.config_parser.print_log_message('INFO', "No user defined types found to migrate.")
        self.migrator_tables.update_main_status('Orchestrator', 'user defined types migration', True, 'finished OK')

    def run_create_domains(self):
        self.migrator_tables.insert_main('Orchestrator', 'domains migration')
        self.config_parser.print_log_message('INFO', "Migrating domains.")
        domains = self.migrator_tables.fetch_all_domains()
        if len(domains) > 0:
            for domain_row in domains:
                domain_data = self.migrator_tables.decode_domain_row(domain_row)
                self.config_parser.print_log_message('INFO', f"Creating domain {domain_data['target_domain_name']} in target database using SQL: {domain_data['target_domain_sql']}")
                try:
                    self.target_connection.connect()
                    self.target_connection.execute_query(domain_data['target_domain_sql'])
                    self.migrator_tables.update_domain_status(domain_data['id'], True, 'migrated OK')
                    self.config_parser.print_log_message('INFO', f"Domain {domain_data['target_domain_name']} created successfully.")
                    self.target_connection.disconnect()
                except Exception as e:
                    self.migrator_tables.update_domain_status(domain_data['id'], False, f'ERROR: {e}')
                    self.handle_error(e, f"create_domain {domain_data['target_domain_name']}")
            self.config_parser.print_log_message('INFO', "Domains migrated successfully.")
        else:
            self.config_parser.print_log_message('INFO', "No domains found to migrate.")
        self.migrator_tables.update_main_status('Orchestrator', 'domains migration', True, 'finished OK')

    def run_migrate_indexes(self, run_mode='standard'):
        self.migrator_tables.insert_main('Orchestrator', 'indexes migration')
        workers_requested = self.config_parser.get_parallel_workers_count()
        target_db_type = self.config_parser.get_target_db_type()

        self.config_parser.print_log_message('INFO', f"Starting {workers_requested} parallel workers to create indexes in target database.")
        migrate_indexes = self.migrator_tables.fetch_all_indexes()
        if len(migrate_indexes) > 0:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers_requested) as executor:
                futures = {}
                for index_row in migrate_indexes:
                    index_data = self.migrator_tables.decode_index_row(index_row)

                    if run_mode == 'function_based' and not index_data['is_function_based']:
                        self.config_parser.print_log_message( 'DEBUG3', f"Function based run mode: Skipping index {index_data['index_name']} as it is not a function based index.")
                        continue
                    elif run_mode == 'standard' and index_data['is_function_based']:
                        self.config_parser.print_log_message( 'INFO', f"Standard run mode: Skipping function based index {index_data['index_name']} ")
                        continue
                    if not self.config_parser.should_migrate_indexes(index_data['source_table']):
                        self.config_parser.print_log_message('DEBUG3', f"Skipping index {index_data['index_name']} as it is not configured for migration.")
                        continue

                    self.config_parser.print_log_message('DEBUG3', f"run_migrate_indexes: futures running count: {len(futures)}")
                    while len(futures) >= workers_requested:
                        done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                        for future in done:
                            index_done = futures.pop(future)
                            if future.result() == False:
                                if self.on_error_action == 'stop':
                                    self.config_parser.print_log_message('ERROR', "Stopping execution due to error.")
                                    exit(1)
                            else:
                                self.migrator_tables.update_index_status(index_done['id'], True, 'migrated OK')

                    # Submit the next task
                    future = executor.submit(self.index_worker, index_data, target_db_type)
                    futures[future] = index_data

                # Process remaining futures
                for future in concurrent.futures.as_completed(futures):
                    index_done = futures[future]
                    if future.result() == False:
                        if self.on_error_action == 'stop':
                            self.config_parser.print_log_message('ERROR', "Stopping execution due to error.")
                            exit(1)
                    else:
                        self.migrator_tables.update_index_status(index_done['id'], True, 'migrated OK')

            self.config_parser.print_log_message('INFO', "Indexes processed successfully.")
        else:
            self.config_parser.print_log_message('INFO', "No indexes to create.")

        self.migrator_tables.update_main_status('Orchestrator', 'indexes migration', True, 'finished OK')

    def run_migrate_constraints(self):
        self.migrator_tables.insert_main('Orchestrator', 'constraints migration')
        workers_requested = self.config_parser.get_parallel_workers_count()
        target_db_type = self.config_parser.get_target_db_type()

        self.config_parser.print_log_message('INFO', f"Starting {workers_requested} parallel workers to create constraints in target database.")
        migrate_constraints = self.migrator_tables.fetch_all_constraints()
        if len(migrate_constraints) > 0:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers_requested) as executor:
                futures = {}
                for constraint_row in migrate_constraints:
                    constraint_data = self.migrator_tables.decode_constraint_row(constraint_row)
                    self.config_parser.print_log_message('DEBUG3', f"run_migrate_constraints: futures running count: {len(futures)}")

                    if not self.config_parser.should_migrate_constraints(constraint_data['source_table']):
                        self.config_parser.print_log_message('DEBUG3', f"Skipping constraint {constraint_data['constraint_name']} as it is not configured for migration.")
                        continue

                    while len(futures) >= workers_requested:
                        done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                        for future in done:
                            constraint_done = futures.pop(future)
                            if future.result() == False:
                                if self.on_error_action == 'stop':
                                    self.config_parser.print_log_message('ERROR', "Stopping execution due to error.")
                                    exit(1)
                            else:
                                self.migrator_tables.update_constraint_status(constraint_done['id'], True, 'migrated OK')
                    # Submit the next task
                    future = executor.submit(self.constraint_worker, constraint_data, target_db_type)
                    futures[future] = constraint_data

                # Process remaining futures
                for future in concurrent.futures.as_completed(futures):
                    constraint_done = futures[future]
                    if future.result() == False:
                        if self.on_error_action == 'stop':
                            self.config_parser.print_log_message('ERROR', "Stopping execution due to error.")
                            exit(1)
                    else:
                        self.migrator_tables.update_constraint_status(constraint_done['id'], True, 'migrated OK')

            self.config_parser.print_log_message('INFO', "Constraints processed successfully.")
        else:
            self.config_parser.print_log_message('INFO', "No constraints to create.")

        self.migrator_tables.update_main_status('Orchestrator', 'constraints migration', True, 'finished OK')

    def table_worker(self, table_data, settings):
        worker_id = uuid.uuid4()
        part_name = 'start'
        worker_source_connection = None
        worker_target_connection = None
        rows_migrated = 0
        worker_start_time = time.time()
        worker_end_time = None
        try:
            target_schema = self.config_parser.convert_names_case(table_data['target_schema'])
            target_table = self.config_parser.convert_names_case(table_data['target_table'])
            create_table_sql = table_data['target_table_sql']
            migrator_tables = settings['migrator_tables']

            if create_table_sql is None:
                self.config_parser.print_log_message('INFO', f"Table {target_table} does not have a CREATE TABLE statement - skipping.")
                return False

            self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Creating table {target_table} in target database ({settings['source_db_type']}:{settings['target_db_type']}-{settings['drop_tables']}/{settings['truncate_tables']}/{settings['create_tables']}/{settings['migrate_data']}).")

            # Each worker uses its own separate connection to the target database
            if settings['target_db_type'] == 'postgresql':
                worker_target_connection = self.load_connector('target')
            else:
                raise ValueError(f"Unsupported target database type: {settings['target_db_type']}")

            part_name = 'connect target'
            worker_target_connection.connect()

            if worker_target_connection.session_settings:
                self.config_parser.print_log_message( 'DEBUG', f"Worker {worker_id}: Executing session settings: {worker_target_connection.session_settings}")
                worker_target_connection.execute_query(worker_target_connection.session_settings)

            if ((settings['drop_tables'] and not settings['resume_after_crash'])
                or (settings['resume_after_crash'] and settings['drop_unfinished_tables'])
                or (settings['resume_after_crash'] and not worker_target_connection.target_table_exists(target_schema, target_table))):
                self.config_parser.print_log_message( 'DEBUG', f"Worker {worker_id}: Dropping table {target_table}...")
                part_name = 'drop table'
                repeat_count = 0
                ## Retry dropping the table if it fails due to locks or other issues
                while True:
                    try:
                        worker_target_connection.execute_query(f"DROP TABLE IF EXISTS {target_schema}.{target_table} CASCADE")
                        break
                    except Exception as e:
                        if repeat_count > 5:
                            self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Error dropping table {target_table}: {e}")
                            self.migrator_tables.update_table_status(table_data['id'], False, f'ERROR: {e}')
                            return False
                        else:
                            repeat_count += 1
                            self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Retrying to drop table {target_table} ({repeat_count})...")
                            part_name = f'retry drop table ({repeat_count})'
                            time.sleep(10)
                self.config_parser.print_log_message('INFO', f"""Worker {worker_id}: Table "{target_table}" dropped successfully.""")

            if ((settings['create_tables'] and not settings['resume_after_crash'])
                or (settings['resume_after_crash'] and settings['drop_unfinished_tables'])
                or (settings['resume_after_crash'] and not worker_target_connection.target_table_exists(target_schema, target_table))):
                self.config_parser.print_log_message( 'DEBUG', f"Worker {worker_id}: Creating table with SQL: {create_table_sql}")
                part_name = 'create table'
                worker_target_connection.execute_query(create_table_sql)
                self.config_parser.print_log_message('INFO', f"""Worker {worker_id}: Table "{target_table}" created successfully.""")

                if table_data['partitioned']:
                    part_name = 'create partitions'
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Creating partitions for table {target_table} in target database.")

                    table_create_partitions_sql = json.loads(table_data['create_partitions_sql'])
                    for partition_sql in table_create_partitions_sql:
                        self.config_parser.print_log_message( 'DEBUG', f"Worker {worker_id}: Creating partition for table {target_table}: {partition_sql}")
                        worker_target_connection.execute_query(partition_sql)
                        self.config_parser.print_log_message('INFO', f"""Worker {worker_id}: Partition of "{target_table}" created successfully [{partition_sql}].""")

                ## now check alterations of columns due to FK IDENTITY dependency
                for result in migrator_tables.fk_find_dependent_columns_to_alter({
                    'target_schema': target_schema,
                    'target_table': target_table,
                }):
                    self.config_parser.print_log_message( 'DEBUG', f"Worker {worker_id}: Found dependency for column alteration: {result}")
                    alter_column_sql = f"""
                        ALTER TABLE "{self.config_parser.convert_names_case(target_schema)}"."{self.config_parser.convert_names_case(target_table)}"
                        ALTER COLUMN "{self.config_parser.convert_names_case(result['target_column'].replace('"',''))}"
                        TYPE {result['altered_data_type']}"""
                    self.config_parser.print_log_message( 'DEBUG', f"Worker {worker_id}: Altering column with SQL: {alter_column_sql}")
                    worker_target_connection.execute_query(alter_column_sql)
                    self.config_parser.print_log_message('INFO', f"""Worker {worker_id}: Column "{result['target_column']}" altered successfully.""")

            if ((settings['truncate_tables'] and not settings['resume_after_crash'])
                or (settings['resume_after_crash'] and settings['drop_unfinished_tables']) ):
                self.config_parser.print_log_message( 'DEBUG', f"Worker {worker_id}: Truncating table {target_table}...")
                part_name = 'truncate table'
                repeat_count = 0
                ## Retry truncating the table if it fails due to locks or other issues
                while True:
                    try:
                        worker_target_connection.execute_query(f'''TRUNCATE TABLE "{target_schema}"."{target_table}" CASCADE''')
                        break
                    except Exception as e:
                        if repeat_count > 5:
                            self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Error truncating table {target_table}: {e}")
                            self.migrator_tables.update_table_status(table_data['id'], False, f'ERROR: {e}')
                            return False
                        else:
                            repeat_count += 1
                            self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Error in {repeat_count} attempt to truncate table {target_table}: {e}")
                            self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Retrying to truncate table {target_table} ({repeat_count})...")
                            part_name = f'retry truncate table ({repeat_count})'
                            time.sleep(10)
                self.config_parser.print_log_message('INFO', f"""Worker {worker_id}: Table "{target_table}" truncated successfully.""")

            if self.config_parser.should_migrate_data(table_data['source_table']):
                # data migration

                part_name = 'connect source'
                worker_source_connection = self.load_connector('source')
                worker_source_connection.connect()

                data_source = self.migrator_tables.fetch_data_source(table_data['source_schema'], table_data['source_table'])
                self.config_parser.print_log_message('DEBUG3', f"Worker {worker_id}: Checking data source for table {table_data['source_schema']}.{table_data['source_table']}: {data_source}")

                use_source_table = False

                if data_source is not None:
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Data source for table {table_data['source_schema']}.{table_data['source_table']} is {data_source}.")
                    clean_objects = self.config_parser.get_source_database_export_clean()

                    if data_source['file_found'] and data_source['file_name'] is not None:
                        self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Data source file found: {data_source['file_name']} - proceeding with data migration.")

                        copy_command = f"""COPY "{target_schema}"."{target_table}"
                            FROM STDIN WITH
                            (FORMAT CSV, HEADER {data_source['format_options']['header']},
                            NULL '\\N',
                            DELIMITER '{data_source['format_options']['delimiter']}')"""
                            ## , QUOTE '{data_source['format_options']['quote']}'

                        ## for other formats relevant to other databases we simply add new data types
                        ## CSV format is common for all databases, but might be necessary to convert it to PostgreSQL CSV conventions
                        if data_source['format_options']['format'].upper() in ('CSV', 'UNL'):

                            source_table_rows = worker_source_connection.get_rows_count(table_data['source_schema'], table_data['source_table'])
                            target_table_rows = worker_target_connection.get_rows_count(target_schema, target_table)

                            protocol_id = migrator_tables.insert_data_migration({
                                'worker_id': worker_id,
                                'source_table_id': table_data['source_table_id'],
                                'source_schema': table_data['source_schema'],
                                'source_table': table_data['source_table'],
                                'target_schema': table_data['target_schema'],
                                'target_table': table_data['target_table'],
                                'source_table_rows': source_table_rows,
                                'target_table_rows': target_table_rows,
                                })

                            if data_source['file_size'] == 0:
                                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Data source for table {target_table} is {data_source['format_options']['format'].upper()} format, but file size is 0. Skipping data migration.")
                                # self.migrator_tables.update_table_status(table_data['id'], True, 'migrated OK (0 rows)')

                                migrator_tables.update_data_migration_status({
                                    'row_id': protocol_id,
                                    'success': True,
                                    'message': 'OK',
                                    'target_table_rows': target_table_rows,
                                    'batch_count': 0,
                                    'shortest_batch_seconds': 0,
                                    'longest_batch_seconds': 0,
                                    'average_batch_seconds': 0,
                                })
                            else:

                                data_import_start_time = time.time()
                                source_files_to_process = []
                                converted_files_to_process = []

                                ## Split of big files is currently implemented only for Informix UNL data files
                                ## And even here it can be not efficient if client uses slow disk(s)
                                if self.config_parser.get_source_db_type() == 'informix' and data_source['format_options']['format'].upper() == 'UNL':
                                    big_files_split_enabled = self.config_parser.get_source_database_export_big_files_split_enabled()
                                    data_source_file_size = data_source['file_size']

                                    data_source_file_size_str = ""
                                    if data_source_file_size is not None:
                                        data_source_file_size_str = f"{data_source_file_size} B ({data_source_file_size / (1024 ** 3):.2f} GB)"
                                    split_threshold_bytes = self.config_parser.get_source_database_export_big_files_split_threshold_bytes()
                                    split_threshold_bytes_str = ""
                                    if split_threshold_bytes is not None:
                                        split_threshold_bytes_str = f"{split_threshold_bytes} B ({split_threshold_bytes / (1024 ** 3):.2f} GB)"

                                    self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Table: {target_table}: Data source file size: {data_source_file_size_str}, split threshold: {split_threshold_bytes_str}, big files split enabled: {big_files_split_enabled}")

                                    if big_files_split_enabled and data_source_file_size > split_threshold_bytes:
                                        # Big files split enabled and file size exceeds threshold
                                        self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Table {target_table}: Data source for table {target_table} is a big file ({data_source_file_size} bytes). Splitting into smaller files.")
                                        source_files_to_process, converted_files_to_process = self.config_parser.split_big_unl_file(data_source)
                                        self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Table {target_table}: Split source files: {source_files_to_process}, converted target file names: {converted_files_to_process}")

                                    else:
                                        # Single file processing
                                        source_files_to_process.append(data_source['file_name'])
                                        converted_files_to_process.append(data_source['converted_file_name'])

                                data_source_settings = data_source.copy()
                                csv_file_name = None

                                for source_file_index, source_file_name in enumerate(source_files_to_process):
                                    data_source_settings['file_name'] = source_file_name
                                    data_source_settings['converted_file_name'] = converted_files_to_process[source_file_index]
                                    self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Table {target_table}: Processing source file: {data_source_settings['file_name']}")

                                    if data_source_settings['format_options']['format'].upper() == 'UNL':
                                        # UNL data source - must be converted to CSV first
                                        self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Table {target_table}: Data source for table {target_table} is UNL format. Converting to CSV.")
                                        part_name = 'convert UNL to CSV'
                                        csv_file_name = data_source_settings['converted_file_name']
                                        self.config_parser.convert_unl_to_csv(data_source_settings, table_data['source_columns'], table_data['target_columns'])

                                    elif data_source_settings['format_options']['format'].upper() == 'CSV':
                                        # CSV data source - use the file directly
                                        part_name = 'use CSV'
                                        csv_file_name = data_source_settings['file_name']
                                        self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Data source for table {target_table} is CSV format. Using file {csv_file_name}.")

                                    try:
                                        if data_source_settings['lob_columns'] != '' and self.config_parser.should_migrate_lob_values():

                                            part_name = 'migrate LOBs'
                                            table_name_for_lob_import = self.config_parser.get_table_name_for_lob_import(target_table)
                                            # LOB data migration - use the file directly
                                            self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Data source for table {target_table} has LOB columns: {data_source_settings['lob_columns']}. Migrating LOB data.")

                                            # drop table for lob import if exists
                                            drop_import_table_sql = f'DROP TABLE IF EXISTS "{target_schema}"."{table_name_for_lob_import}" CASCADE'
                                            worker_target_connection.execute_query(drop_import_table_sql)
                                            self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Intermediate import table {table_name_for_lob_import} dropped successfully.")

                                            # create intermediate table for rows without LOB data
                                            create_import_table_sql = re.sub(
                                                target_table,
                                                f"{table_name_for_lob_import}",
                                                table_data['target_table_sql'],
                                                flags=re.IGNORECASE
                                            )
                                            create_import_table_sql = re.sub( 'bytea', 'text', create_import_table_sql, flags=re.IGNORECASE)
                                            self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Creating intermediate import table with SQL: {create_import_table_sql}")
                                            worker_target_connection.execute_query(create_import_table_sql)

                                            # importing CSV (converted UNL) data into intermediate table
                                            imp_table_copy_command = f"""COPY "{target_schema}"."{table_name_for_lob_import}" FROM STDIN WITH
                                                (FORMAT CSV, HEADER {data_source_settings['format_options']['header']},
                                                NULL '\\N',
                                                DELIMITER '{data_source_settings['format_options']['delimiter']}')"""
                                            part_name = 'import to intermediate table'
                                            worker_target_connection.copy_from_file(imp_table_copy_command, csv_file_name)

                                            # Loop over import table row by row, select all columns, find column(s) specified in lob_columns and read their content
                                            lob_columns = [col.strip() for col in data_source_settings['lob_columns'].split(',') if col.strip()]
                                            lob_col_name = None
                                            lob_col_index = None
                                            lob_col_type = None

                                            for lob_col in lob_columns:
                                                lob_col_name = lob_col
                                                for lob_col_ind, col_info in table_data['target_columns'].items():
                                                    if col_info['column_name'].lower() == lob_col.lower():
                                                        lob_col_index = int(lob_col_ind)
                                                        lob_col_type = col_info['data_type']
                                                        break

                                                part_name = f'process LOB column {lob_col_name}/{lob_col_index}'
                                                if lob_col_name is not None and lob_col_index is not None:
                                                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: LOB column {lob_col_name} found in target table {target_table} - index: {lob_col_index}, type: {lob_col_type}")

                                                    select_datafiles_sql = f"""SELECT DISTINCT split_part({lob_col_name},',',3) as datafile, count(*) as occurrences FROM {target_schema}.{table_name_for_lob_import} group by 1 order by 1;"""

                                                    datafiles_cursor = worker_target_connection.connection.cursor()
                                                    datafiles_cursor.execute(select_datafiles_sql)
                                                    datafiles = datafiles_cursor.fetchall()
                                                    datafiles_cursor.close()
                                                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Found {len(datafiles)} distinct data files for LOB column {lob_col_name}")

                                                    max_lob_parallel_workers = self.config_parser.get_source_database_export_workers()
                                                    if len(datafiles) <= 10:
                                                        max_lob_parallel_workers = 1
                                                        self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Set max LOB parallel workers to {max_lob_parallel_workers} due to low data file count ({len(datafiles)})")
                                                    if len(datafiles) > 10 and len(datafiles) <= 50:
                                                        max_lob_parallel_workers = 2
                                                        self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Set max LOB parallel workers to {max_lob_parallel_workers} due to medium data file count ({len(datafiles)})")
                                                    if len(datafiles) > 200:
                                                        max_lob_parallel_workers = max_lob_parallel_workers * 2
                                                        self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Set max LOB parallel workers to {max_lob_parallel_workers} due to high data file count ({len(datafiles)})")

                                                    if len(datafiles) > 0:

                                                        self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Processing {len(datafiles)} data files for LOB column {lob_col_name}")
                                                        current_datafile_num = 0

                                                        with concurrent.futures.ThreadPoolExecutor(max_workers=max_lob_parallel_workers) as executor:
                                                            futures = {}
                                                            for datafile_row in datafiles:
                                                                datafile = datafile_row[0]
                                                                occurrences = datafile_row[1]
                                                                current_datafile_num += 1

                                                                settings = {
                                                                    'target_schema': table_data['target_schema'],
                                                                    'target_table': table_data['target_table'],
                                                                    'unl_import_table': table_name_for_lob_import,
                                                                    'lob_column': lob_col_name,
                                                                    'lob_col_index': lob_col_index,
                                                                    'lob_col_type': lob_col_type,
                                                                    'target_columns': table_data['target_columns'],
                                                                    'datafile': datafile,
                                                                    'datafiles_count': len(datafiles),
                                                                    'current_datafile_num': current_datafile_num,
                                                                    'occurrences': occurrences,
                                                                    'lob_files_path': self.config_parser.get_source_database_export_file_path(),
                                                                }

                                                                print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}: parallel LOB processing: futures running count: {len(futures)}")
                                                                while len(futures) >= max_lob_parallel_workers:

                                                                    done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                                                                    for future in done:
                                                                        datafile_done = futures.pop(future)
                                                                        if future.result() == False:
                                                                            if self.on_error_action == 'stop':
                                                                                print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}: parallel LOB processing: Stopping execution due to error.")
                                                                                exit(1)
                                                                        else:
                                                                            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}: parallel LOB processing: {datafile_done} LOBs migrated OK")

                                                                # Submit the next task
                                                                future = executor.submit(self.lob_worker, settings)
                                                                futures[future] = datafile

                                                            # Process remaining futures
                                                            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}: parallel LOB processing: Processing remaining futures")
                                                            for future in concurrent.futures.as_completed(futures):
                                                                datafile_done = futures[future]
                                                                if future.result() == False:
                                                                    if self.on_error_action == 'stop':
                                                                        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}: parallel LOB processing: Stopping execution due to error.")
                                                                        exit(1)
                                                                else:
                                                                    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}: parallel LOB processing: {datafile_done} LOBs migrated OK")

                                                        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}: parallel LOB processing: All datafiles processed successfully.")
                                                    else:
                                                        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}: parallel LOB processing: No datafiles to process.")


                                                else:
                                                    self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: LOB column {lob_col_name} not found in target table {target_table}. Skipping LOB data migration for this column.")
                                                    continue


                                            self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Data copied successfully from CSV file {csv_file_name} to table {target_table} with LOB columns.")
                                            # Drop the intermediate import table
                                            worker_target_connection.execute_query(f'DROP TABLE IF EXISTS "{target_schema}"."{table_name_for_lob_import}" CASCADE')
                                            self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Intermediate import table {table_name_for_lob_import} dropped successfully.")
                                            target_table_rows = worker_target_connection.get_rows_count(target_schema, target_table)
                                            data_import_end_time = time.time()
                                            data_import_duration = data_import_end_time - data_import_start_time
                                            self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Data import duration: {data_import_duration:.2f} seconds, rows migrated: {target_table_rows}.")

                                        else:

                                            if data_source_settings['lob_columns'] != '' and not self.config_parser.should_migrate_lob_values():
                                                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Data source for table {target_table} has LOB columns: {data_source_settings['lob_columns']}, but LOB migration is disabled. Skipping LOB data migration.")

                                            # No LOB columns - standard CSV import
                                            # CSV data source - directly import into target database
                                            part_name = 'copy data from CSV'
                                            self.config_parser.print_log_message('DEBUG', f"Worker {worker_id}: Executing COPY command: {copy_command}")
                                            worker_target_connection.copy_from_file(copy_command, csv_file_name)

                                            self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Data copied successfully from CSV file {csv_file_name} to table {target_table}.")

                                        target_table_rows = worker_target_connection.get_rows_count(target_schema, target_table)
                                        data_import_end_time = time.time()
                                        data_import_duration = data_import_end_time - data_import_start_time
                                        self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Data import duration: {data_import_duration:.2f} seconds, rows migrated: {target_table_rows}.")

                                        # self.migrator_tables.update_table_status(table_data['id'], True, f'migrated OK ({rows_migrated} rows)')
                                        migrator_tables.update_data_migration_status({
                                            'row_id': protocol_id,
                                            'success': True,
                                            'message': 'OK',
                                            'target_table_rows': target_table_rows,
                                            'batch_count': 0,
                                            'shortest_batch_seconds': data_import_duration,
                                            'longest_batch_seconds': data_import_duration,
                                            'average_batch_seconds': data_import_duration,
                                        })

                                        if data_source_settings['format_options']['format'].upper() == 'UNL' and clean_objects:
                                            # Delete the converted CSV file after import if clean_objects is True
                                            try:
                                                if csv_file_name and os.path.exists(csv_file_name):
                                                    os.remove(csv_file_name)
                                                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Deleted temporary CSV file {csv_file_name}.")
                                            except Exception as cleanup_exc:
                                                self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Failed to delete temporary CSV file {csv_file_name}: {cleanup_exc}")

                                    except Exception as e:
                                        self.migrator_tables.update_table_status(table_data['id'], False, f'ERROR: {e}')
                                        self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: ({part_name}) Error copying data from CSV file {csv_file_name} to table {target_table}: {e}")
                                        return False
                    else:
                        if not data_source['file_found'] and data_source['file_name'] is not None:
                            self.config_parser.print_log_message('INFO', f"Worker {worker_id}: ({part_name}) Data file {data_source['file_name']} not found for table {target_table}.")

                            on_missing_data_file = self.config_parser.get_source_database_export_on_missing_data_file()
                            if on_missing_data_file == 'skip':
                                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Skipping data migration for table {target_table} due to missing data file.")
                                use_source_table = False
                                rows_migrated = 0
                            else:
                                self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Data file {data_source['file_name']} not found for table {target_table}. Switching to source table.")
                                use_source_table = True

                else:
                    use_source_table = True

                if use_source_table:
                    part_name = 'migrate data'
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Migrating data for table {target_table} from source database.")

                    table_settings = {
                        'worker_id': worker_id,
                        'source_schema': table_data['source_schema'],
                        'source_table': table_data['source_table'],
                        'source_table_id': table_data['source_table_id'],
                        'source_columns': table_data['source_columns'],
                        'target_schema': target_schema,
                        'target_table': target_table,
                        'target_columns': table_data['target_columns'],
                        'table_comment': table_data['table_comment'],
                        # 'primary_key_columns': table_data['primary_key_columns'],
                        # 'primary_key_columns_count': table_data['primary_key_columns_count'],
                        # 'primary_key_columns_types': table_data['primary_key_columns_types'],
                        'batch_size': self.config_parser.get_table_batch_size(table_data['source_table']),
                        'migrator_tables': settings['migrator_tables'],
                        'migration_limitation': '',
                        'chunk_size': self.config_parser.get_table_chunk_size(table_data['source_table']),
                        'chunk_number': 1,
                        'resume_after_crash': settings['resume_after_crash'],
                        'drop_unfinished_tables': settings['drop_unfinished_tables'],
                    }

                    rows_migration_limitations = settings['migrator_tables'].get_records_data_migration_limitation(table_data['source_table'])
                    migration_limitations = []
                    if rows_migration_limitations:
                        self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Found data migration limitations matching table {target_table}: {rows_migration_limitations}")
                        for limitation in rows_migration_limitations:
                            where_clause = limitation[0]
                            where_clause = where_clause.replace('{source_schema}', table_data['source_schema']).replace('{source_table}', table_data['source_table'])
                            use_when_column_name = limitation[1]
                            for col_order_num, column_info in table_data['source_columns'].items():
                                column_name = column_info['column_name']
                                if column_name == use_when_column_name or re.match(use_when_column_name, column_name):
                                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Column {column_name} matches migration limitation.")
                                    migration_limitations.append(where_clause)
                        if migration_limitations:
                            table_settings['migration_limitation'] = f"{' AND '.join(migration_limitations)}" if migration_limitations else ''
                            self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Migration limitations for table {target_table}: {migration_limitations}")

                    while True:
                        if self.config_parser.pause_migration_fired():
                            self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Migration paused for table {target_table}.")
                            self.config_parser.wait_for_resume()
                            self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Resuming migration for table {target_table}.")
                        # proper pausing / stopping of the migration requires to connect and disconnect for each chunk
                        migration_stats = worker_source_connection.migrate_table(worker_target_connection, table_settings)

                        rows_migrated += migration_stats['rows_migrated']
                        if migration_stats['finished']:
                            break
                        table_settings['chunk_number'] += 1

                worker_source_connection.disconnect()

                if rows_migrated > 0:
                    # sequences setting
                    part_name = 'sequences'
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Setting sequences for table {target_table} in target database.")
                    sequences = worker_target_connection.fetch_sequences(target_schema, target_table)
                    if sequences:
                        for order_num, sequence_details in sequences.items():
                            sequence_id = sequence_details['id']
                            sequence_name = sequence_details['name']
                            column_name = sequence_details['column_name']
                            sequence_sql = sequence_details['set_sequence_sql']
                            self.migrator_tables.insert_sequence(sequence_id, target_schema, target_table, column_name, sequence_name, sequence_sql)
                            self.config_parser.print_log_message( 'DEBUG', f"Worker {worker_id}: Setting sequence with SQL: {sequence_sql}")
                            try:
                                worker_target_connection.execute_query(sequence_sql)
                                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Sequence ({order_num}) {sequence_name} set successfully for table {target_table}.")
                                seq_curr_val = worker_target_connection.get_sequence_current_value(sequence_id)
                                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Current value of sequence {sequence_name} is {seq_curr_val}.")
                                self.migrator_tables.update_sequence_status(sequence_id, True, 'migrated OK')
                            except Exception as e:
                                self.migrator_tables.update_sequence_status(sequence_id, False, f'ERROR: {e}')
                                self.migrator_tables.update_table_status(table_data['id'], False, f'ERROR: {e}')
                                self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Error setting sequence {sequence_name} for table {target_table}: {e}")
                    else:
                        self.config_parser.print_log_message('INFO', f"Worker {worker_id}: No sequences found for table {target_table}.")
                else:
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: No data found for table {target_table} - skipping sequences.")
            else:
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Skipping data migration for table {target_table} based on configuration")

            worker_end_time = time.time()
            elapsed_time = worker_end_time - worker_start_time
            self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Total time spent on migrating table {target_table} is {elapsed_time:.2f} seconds ({elapsed_time / 60:.2f} minutes)")
            try:
                worker_target_connection.disconnect()
            except Exception as e:
                pass
            return True
        except Exception as e_main:
            try:
                worker_source_connection.disconnect()
            except Exception as e:
                pass
            try:
                worker_target_connection.disconnect()
            except Exception as e:
                pass
            self.migrator_tables.update_table_status(table_data['id'], False, f'ERROR: {e_main}')
            self.handle_error(e_main, f"table_worker {worker_id} ({part_name}) {target_table}")
            return False

    def lob_worker(self, settings):
        target_schema = settings['target_schema']
        target_table = settings['target_table']
        unl_import_table = settings['unl_import_table']
        lob_column = settings['lob_column']
        lob_col_index = settings['lob_col_index']
        lob_col_type = settings['lob_col_type']
        target_columns = settings['target_columns']
        datafile = settings['datafile']
        datafiles_count = settings['datafiles_count']
        current_datafile_num = settings['current_datafile_num']
        occurrences = settings['occurrences']
        lob_files_path = settings['lob_files_path']

        try:
            worker_insert_connection = self.load_connector('target')
            worker_insert_connection.connect()
            worker_insert_connection.autocommit = True  # Enable autocommit for the insert connection

            processing_start_time = time.time()
            worker_id = uuid.uuid4()
            self.config_parser.print_log_message('INFO', f"{time.strftime('%Y-%m-%d %H:%M:%S')}: Worker {worker_id}: started for LOB file: {datafile} - occurrences: {occurrences} - {current_datafile_num}/{datafiles_count}")

            col_list = ', '.join([f'"{col_info["column_name"]}"' for _, col_info in target_columns.items()])
            placeholders = ', '.join(['%s'] * len(target_columns))
            insert_sql = f'INSERT INTO "{target_schema}"."{target_table}" ({col_list}) VALUES ({placeholders})'

            worker_select_connection = self.load_connector('target')
            worker_select_connection.connect()
            # cur_select = worker_select_connection.connection.cursor()

            select_cur = worker_select_connection.connection.cursor()
            select_sql = f"""SELECT {col_list} FROM "{target_schema}"."{unl_import_table}" WHERE split_part({lob_column},',',3) = '{datafile}'"""

            self.config_parser.print_log_message('DEBUG', f"{time.strftime('%Y-%m-%d %H:%M:%S')}: Worker {worker_id}: Fetching rows from '{unl_import_table}' with query: {select_sql}")
            self.config_parser.print_log_message('DEBUG', f"{time.strftime('%Y-%m-%d %H:%M:%S')}: Worker {worker_id}: Lob column: {lob_column} (index: {lob_col_index}, type: {lob_col_type})")
            counter = 0
            read_lines = 0

            select_cur.execute(select_sql)
            row = select_cur.fetchone()
            part_name = 'LOB processing start'
            while row is not None:
                row = list(row)
                read_lines += 1

                part_name = 'read lob pointer'
                if lob_col_index <= 0 or lob_col_index > len(row):
                    self.config_parser.print_log_message('ERROR', f"Worker: {worker_id}: Invalid lob_col_index {lob_col_index} for row of length {len(row)}. Skipping row.")
                    row = select_cur.fetchone()
                    continue
                content = row[lob_col_index-1]
                content_is_null = False

                part_name = 'decode LOB pointer'
                if content is not None and content != '' and content != '0,0,0':
                    parts = content.split(',')
                    start = int(parts[0], 16)
                    length = int(parts[1], 16)
                    datafile = parts[2]
                    filepath = os.path.join(lob_files_path, datafile)
                else:
                    content_is_null = True
                    start = 0
                    length = 0
                    filepath = None

                # if length > 1024*1024*1024:
                #   self.config_parser.print_log_message('INFO', f"{time.strftime('%Y-%m-%d %H:%M:%S')}: Worker: {worker_id}: LOB size exceeds 1GB: {length} bytes ({length / 1024 / 1024 / 1024}) GB - in row: {row} - setting to NULL.")
                #   content_is_null = True
                # if args.test_sizes:
                #   row = select_cur.fetchone() # Fetch in test mode
                #   continue

                if not content_is_null and os.path.exists(filepath):

                    part_name = 'read LOB file'
                    if lob_col_type.lower() == 'bytea':
                        # For BYTEA, read the file as binary
                        with open(filepath, 'rb') as f:
                            f.seek(start)
                            chunk = f.read(length)

                    elif lob_col_type.lower() == 'text':
                        # For TEXT, read the file as text
                        with open(filepath, 'r', encoding='utf-8') as f:
                            f.seek(start)
                            chunk = f.read(length)

                    row[lob_col_index-1] = chunk

                if content_is_null:
                    row[lob_col_index-1] = None

                try:
                    part_name = 'insert row'
                    cur_insert = worker_insert_connection.connection.cursor()
                    cur_insert.execute(insert_sql, row)
                    cur_insert.close()
                    counter += 1
                except Exception as e:
                    cur_insert.close()
                    self.config_parser.print_log_message('ERROR', f"Worker: {worker_id}: Error executing INSERT command for row: {row} (counter: {counter}), error: {e}")

                part_name = 'fetch next row'
                row = select_cur.fetchone()
            select_cur.close()
            self.config_parser.print_log_message('INFO', f"{time.strftime('%Y-%m-%d %H:%M:%S')}: Worker: {worker_id}: Processed {read_lines} rows from '{unl_import_table}', datafile: {datafile}, occurrences: {occurrences} - inserted {counter} into '{target_schema}.{target_table}'.")

            worker_select_connection.disconnect()
            worker_insert_connection.disconnect()

            self.config_parser.print_log_message('INFO', f"{time.strftime('%Y-%m-%d %H:%M:%S')}: Worker: {worker_id}: Processing completed in {time.time() - processing_start_time} seconds.")
            return True  # Indicate successful processing of this datafile
        except Exception as e:
            self.handle_error(e, f"lob_worker: Worker: {worker_id}: Datafile: {datafile}: Table: {target_table}: {part_name} - {e}")
            try:
                worker_select_connection.disconnect()
            except Exception as e:
                pass
            try:
                worker_insert_connection.disconnect()
            except Exception as e:
                pass
            return False  # Indicate failure in processing this datafile

    def index_worker(self, index_data, target_db_type):
        worker_id = uuid.uuid4()
        try:
            index_name = index_data['index_name']
            create_index_sql = index_data['index_sql']

            self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Creating index {index_name} in target database.")

            # Each worker uses its own separate connection to the target database
            worker_target_connection = self.load_connector('target')

            self.config_parser.print_log_message( 'DEBUG', f"Worker {worker_id}: Creating index with SQL: {create_index_sql}")

            worker_target_connection.connect()

            if worker_target_connection.session_settings:
                self.config_parser.print_log_message( 'DEBUG', f"Worker {worker_id}: Executing session settings: {worker_target_connection.session_settings}")
                worker_target_connection.execute_query(worker_target_connection.session_settings)

            worker_target_connection.execute_query(create_index_sql)
            self.config_parser.print_log_message('INFO', f"""Worker {worker_id}: Index "{index_name}" created successfully.""")

            worker_target_connection.disconnect()
            return True
        except Exception as e:
            self.migrator_tables.update_index_status(index_data['id'], False, f'ERROR: {e}')
            self.handle_error(e, f"index_worker {worker_id} {index_name}")
            return False

    def constraint_worker(self, constraint_data, target_db_type):
        worker_id = uuid.uuid4()
        try:
            constraint_name = constraint_data['constraint_name']
            self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Creating constraint {constraint_name} in target database.")
            create_constraint_sql = constraint_data['constraint_sql']

            target_schema = constraint_data['target_schema']
            target_table = constraint_data['target_table']
            referenced_table_schema = constraint_data['referenced_table_schema']
            referenced_table_name = constraint_data['referenced_table_name']

            if create_constraint_sql:

                # Each worker uses its own separate connection to the target database
                worker_target_connection = self.load_connector('target')
                worker_target_connection.connect()

                if not worker_target_connection.target_table_exists(target_schema, target_table):
                    self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Target table {target_schema}.{target_table} for constraint {constraint_name} does not exist - skipping constraint creation.")
                    self.migrator_tables.update_constraint_status(constraint_data['id'], False, f'ERROR: target table {target_schema}.{target_table} does not exist')
                    return False

                if not worker_target_connection.target_table_exists(referenced_table_schema, referenced_table_name):
                    self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Referenced table {referenced_table_schema}.{referenced_table_name} for constraint {constraint_name} does not exist - skipping constraint creation.")
                    self.migrator_tables.update_constraint_status(constraint_data['id'], False, f'ERROR: referenced table {referenced_table_schema}.{referenced_table_name} does not exist')
                    return False

                self.config_parser.print_log_message( 'DEBUG', f"Worker {worker_id}: Creating constraint with SQL: {create_constraint_sql}")

                query = f'''SET SESSION search_path TO {constraint_data['target_schema']};'''

                worker_target_connection.execute_query(query)

                if worker_target_connection.session_settings:
                    self.config_parser.print_log_message( 'DEBUG', f"Worker {worker_id}: Executing session settings: {worker_target_connection.session_settings}")
                    worker_target_connection.execute_query(worker_target_connection.session_settings)

                creation_try = 0
                while True:
                    try:
                        worker_target_connection.execute_query(create_constraint_sql)
                        break  # Exit loop if successful
                    except Exception as e:
                        self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Error creating constraint {constraint_name}: {e}")
                        error_texts = [
                            "no unique constraint matching",
                            "is violated by some row",
                            "violates foreign key constraint",
                            "does not exist",
                            "violates check constraint",
                        ]
                        if any(txt in str(e).lower() for txt in error_texts):
                            self.migrator_tables.update_constraint_status(constraint_data['id'], False, f'ERROR: {e}')
                            worker_target_connection.disconnect()
                            return False
                        self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Retrying ({creation_try}) to create constraint {constraint_name}...")
                        if creation_try > 5:
                            self.handle_error(e, f"constraint_worker {worker_id}: Failed to create {constraint_name} after {creation_try} attempts.")
                            self.migrator_tables.update_constraint_status(constraint_data['id'], False, f'ERROR: {e}')
                            worker_target_connection.disconnect()
                            return False
                        creation_try += 1
                        time.sleep(5)  # Wait before retrying

                query = 'RESET search_path;'
                worker_target_connection.execute_query(query)
                self.config_parser.print_log_message('INFO', f"""Worker {worker_id}: Constraint "{constraint_name}" created successfully.""")
                worker_target_connection.disconnect()
                return True
            else:
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Constraint {constraint_name} does not have a SQL statement - skipping.")
                worker_target_connection.disconnect()
                return False

        except Exception as e:
            self.handle_error(e, f"constraint_worker {worker_id} {constraint_name}")
            return False

    def run_migrate_funcprocs(self):
        self.migrator_tables.insert_main('Orchestrator', 'functions/procedures migration')
        include_funcprocs = self.config_parser.get_include_funcprocs()
        exclude_funcprocs = self.config_parser.get_exclude_funcprocs() or []

        if self.config_parser.should_migrate_funcprocs():
            self.config_parser.print_log_message('INFO', "Migrating functions and procedures.")
            funcproc_names = self.source_connection.fetch_funcproc_names(self.config_parser.get_source_schema())
            self.config_parser.print_log_message( 'DEBUG', f"Function/procedure names: {funcproc_names}")

            if funcproc_names:
                for order_num, funcproc_data in funcproc_names.items():
                    self.config_parser.print_log_message('INFO', f"Processing func/proc {order_num}/{len(funcproc_names)}: {funcproc_data['name']}")
                    if include_funcprocs == ['.*'] or '.*' in include_funcprocs:
                        pass
                    elif not any(fnmatch.fnmatch(funcproc_data['name'], pattern) for pattern in include_funcprocs):
                        continue
                    if any(fnmatch.fnmatch(funcproc_data['name'], pattern) for pattern in exclude_funcprocs):
                        self.config_parser.print_log_message('INFO', f"Func/proc {funcproc_data['name']} is excluded from migration.")
                        continue

                    funcproc_id = funcproc_data['id']
                    funcproc_type = funcproc_data['type']
                    self.config_parser.print_log_message('INFO', f"Migrating {funcproc_type} {funcproc_data['name']}.")
                    funcproc_code = self.source_connection.fetch_funcproc_code(funcproc_id)

                    table_names = []
                    view_names = []
                    converted_code = ''
                    try:
                        table_names = self.migrator_tables.fetch_all_target_table_names()
                    except Exception as e:
                        self.handle_error(e, 'fetching table names')
                    try:
                        view_names = self.migrator_tables.fetch_all_target_view_names()
                    except Exception as e:
                        self.handle_error(e, 'fetching view names')

                    try:
                        self.config_parser.print_log_message( 'DEBUG', f"Converting {funcproc_type} {funcproc_data['name']} code...")
                        converted_code = self.source_connection.convert_funcproc_code({
                            'funcproc_code': funcproc_code,
                            'target_db_type': self.config_parser.get_target_db_type(),
                            'source_schema': self.config_parser.get_source_schema(),
                            'target_schema': self.config_parser.get_target_schema(),
                            'table_list': table_names,
                            'view_list': view_names,
                            })

                        self.config_parser.print_log_message( 'DEBUG', "Checking for remote objects substitution in functions/procedures...")
                        rows = self.migrator_tables.get_records_remote_objects_substitution()
                        if rows:
                            for row in rows:
                                self.config_parser.print_log_message( 'DEBUG', f"Funcs/Procs - remote objects substituting {row[0]} with {row[1]}")
                                converted_code = re.sub(re.escape(row[0]), row[1], converted_code, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)

                        self.migrator_tables.insert_funcprocs(self.source_schema, funcproc_data['name'], funcproc_id, funcproc_code, self.target_schema, funcproc_data['name'], converted_code, funcproc_data['comment'])

                        if converted_code is not None and converted_code.strip():
                            self.config_parser.print_log_message('INFO', f"Creating {funcproc_type} {funcproc_data['name']} in target database.")
                            self.target_connection.connect()

                            if self.target_connection.session_settings:
                                self.config_parser.print_log_message( 'DEBUG', f"Executing session settings: {self.target_connection.session_settings}")
                                self.target_connection.execute_query(self.target_connection.session_settings)

                            self.target_connection.execute_query(converted_code)
                            self.config_parser.print_log_message( 'DEBUG', f"[OK] Source code for {funcproc_data['name']}: {funcproc_code}")
                            self.config_parser.print_log_message( 'DEBUG', f"[OK] Converted code for {funcproc_data['name']}: {converted_code}")
                            self.migrator_tables.update_funcproc_status(funcproc_id, True, 'migrated OK')
                        else:
                            self.config_parser.print_log_message('INFO', f"Skipping {funcproc_type} {funcproc_data['name']} - no conversion done")
                            self.migrator_tables.update_funcproc_status(funcproc_id, False, 'no conversion')
                        self.target_connection.disconnect()
                    except Exception as e:
                        self.config_parser.print_log_message( 'DEBUG', f"[ERROR] Migrating {funcproc_type} {funcproc_data['name']}.")
                        self.config_parser.print_log_message( 'DEBUG', f"[ERROR] Source code for {funcproc_data['name']}: {funcproc_code}")
                        self.config_parser.print_log_message( 'DEBUG', f"[ERROR] Converted code for {funcproc_data['name']}: {converted_code}")
                        self.migrator_tables.update_funcproc_status(funcproc_id, False, f'ERROR: {e}')
                        self.handle_error(e, f"migrate_funcproc {funcproc_type} {funcproc_data['name']}")

                self.config_parser.print_log_message('INFO', "Functions and procedures migrated successfully.")
            else:
                self.config_parser.print_log_message('INFO', "No functions or procedures found to migrate.")
        else:
            self.config_parser.print_log_message('INFO', "Skipping function and procedure migration as requested.")

        self.migrator_tables.update_main_status('Orchestrator', 'functions/procedures migration', True, 'finished OK')

    def run_migrate_triggers(self):
        self.migrator_tables.insert_main('Orchestrator', 'triggers migration')
        try:
            if self.config_parser.should_migrate_triggers():
                self.config_parser.print_log_message('INFO', "Migrating triggers.")

                all_triggers = self.migrator_tables.fetch_all_triggers()
                if all_triggers:
                    for one_trigger in all_triggers:
                        trigger_detail = self.migrator_tables.decode_trigger_row(one_trigger)

                        if self.config_parser.should_migrate_triggers(trigger_detail['source_table']):
                            self.config_parser.print_log_message('INFO', f"Processing trigger {trigger_detail['trigger_name']}")
                            self.config_parser.print_log_message( 'DEBUG', f"Trigger details: {trigger_detail}")

                            converted_code = trigger_detail['trigger_target_sql']

                            self.config_parser.print_log_message( 'DEBUG', "Checking for remote objects substitution in triggers...")
                            rows = self.migrator_tables.get_records_remote_objects_substitution()
                            if rows:
                                for row in rows:
                                    self.config_parser.print_log_message( 'DEBUG', f"Triggers - remote objects substituting {row[0]} with {row[1]}")
                                    converted_code = re.sub(re.escape(row[0]), row[1], converted_code, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)

                            try:
                                if converted_code is not None and converted_code.strip():
                                    self.config_parser.print_log_message('INFO', f"Creating trigger {trigger_detail['trigger_name']} in target database.")
                                    self.target_connection.connect()
                                    self.target_connection.execute_query(converted_code)
                                    self.config_parser.print_log_message( 'DEBUG', f"[OK] Source code for {trigger_detail['trigger_name']}: {trigger_detail['trigger_source_sql']}")
                                    self.config_parser.print_log_message( 'DEBUG', f"[OK] Converted code for {trigger_detail['trigger_name']}: {converted_code}")
                                    self.migrator_tables.update_trigger_status(trigger_detail['id'], True, 'migrated OK')
                                else:
                                    self.config_parser.print_log_message('INFO', f"Skipping trigger {trigger_detail['trigger_name']} - no conversion.")
                                    self.migrator_tables.update_trigger_status(trigger_detail['id'], False, 'no conversion')
                                self.target_connection.disconnect()
                            except Exception as e:
                                self.config_parser.print_log_message( 'DEBUG', f"[ERROR] Migrating trigger {trigger_detail['trigger_name']}.")
                                self.config_parser.print_log_message( 'DEBUG', f"[ERROR] Source code for {trigger_detail['trigger_name']}: {trigger_detail['trigger_source_sql']}")
                                self.config_parser.print_log_message( 'DEBUG', f"[ERROR] Converted code for {trigger_detail['trigger_name']}: {converted_code}")
                                self.migrator_tables.update_trigger_status(trigger_detail['id'], False, f'ERROR: {e}')
                                self.handle_error(e, f"migrate_trigger {trigger_detail['trigger_name']}")
                        else:
                            self.config_parser.print_log_message('INFO', f"Skipping trigger {trigger_detail['trigger_name']} for table {trigger_detail['table_name']} based on the migration configuration.")

                    self.config_parser.print_log_message('INFO', "Triggers migrated successfully.")
                else:
                    self.config_parser.print_log_message('INFO', "No triggers found to migrate.")
            else:
                self.config_parser.print_log_message('INFO', "Skipping trigger migration as requested.")

            self.migrator_tables.update_main_status('Orchestrator', 'triggers migration', True, 'finished OK')

        except Exception as e:
            self.handle_error(e, 'migrate_triggers')

    def run_migrate_views(self):
        self.migrator_tables.insert_main('Orchestrator', 'views migration')

        if self.config_parser.should_migrate_views():
            self.config_parser.print_log_message('INFO', "Migrating views.")

            all_views = self.migrator_tables.fetch_all_views()
            if all_views:
                for one_view in all_views:
                    view_detail = self.migrator_tables.decode_view_row(one_view)
                    self.config_parser.print_log_message('INFO', f"Processing view {view_detail['source_view_name']}")
                    self.config_parser.print_log_message( 'DEBUG', f"View details: {view_detail}")

                    try:
                        self.target_connection.connect()

                        if self.target_connection.session_settings:
                            self.config_parser.print_log_message( 'DEBUG', f"Executing session settings: {self.target_connection.session_settings}")
                            self.target_connection.execute_query(self.target_connection.session_settings)

                        query = f'''SET SESSION search_path TO {view_detail['target_schema']};'''
                        self.target_connection.execute_query(query)

                        self.target_connection.execute_query(view_detail['target_view_sql'])
                        self.migrator_tables.update_view_status(view_detail['id'], True, 'migrated OK')
                        self.config_parser.print_log_message('INFO', f"View {view_detail['source_view_name']} migrated successfully.")

                        query = f'''RESET search_path;'''
                        self.target_connection.execute_query(query)
                        self.target_connection.disconnect()
                    except Exception as e:
                        self.migrator_tables.update_view_status(view_detail['id'], False, f'ERROR: {e}')
                        self.handle_error(e, f"migrate_view {view_detail['source_view_name']}")
            else:
                self.config_parser.print_log_message('INFO', "No views found to migrate.")
        else:
            self.config_parser.print_log_message('INFO', "Skipping view migration as requested.")
        self.migrator_tables.update_main_status('Orchestrator', 'views migration', True, 'finished OK')

    def run_migrate_comments(self):
        self.migrator_tables.insert_main('Orchestrator', 'comments migration')
        self.config_parser.print_log_message('INFO', "Migrating comments.")
        all_tables = self.migrator_tables.fetch_all_tables()
        self.target_connection.connect()

        try:
            for table_detail in all_tables:
                table_data = self.migrator_tables.decode_table_row(table_detail)
                if table_data['table_comment']:
                    query = f"""COMMENT ON TABLE "{table_data['target_schema']}"."{table_data['target_table']}" IS '{table_data['table_comment']}'"""
                    self.config_parser.print_log_message('INFO', f"Setting comment for table {table_data['target_table']} in target database.")
                    self.config_parser.print_log_message( 'DEBUG', f"Executing comment query: {query}")
                    self.target_connection.execute_query(query)

                for col in table_data['target_columns'].keys():
                    column_comment = table_data['target_columns'][col]['column_comment']
                    if column_comment:
                        query = f"""COMMENT ON COLUMN "{table_data['target_schema']}"."{table_data['target_table']}"."{table_data['target_columns'][col]['column_name']}" IS '{column_comment}'"""
                        self.config_parser.print_log_message('INFO', f"Setting comment for column {table_data['target_columns'][col]['column_name']} in target database.")
                        self.config_parser.print_log_message( 'DEBUG', f"Executing comment query: {query}")
                        self.target_connection.execute_query(query)

            all_indexes = self.migrator_tables.fetch_all_indexes()
            for index_detail in all_indexes:
                index_data = self.migrator_tables.decode_index_row(index_detail)
                if index_data['index_comment']:
                    index_name = f"{index_data['index_name']}_tab_{index_data['target_table']}"
                    query = f"""COMMENT ON INDEX "{index_data['target_schema']}"."{index_name}" IS '{index_data['index_comment']}'"""
                    self.config_parser.print_log_message('INFO', f"Setting comment for index {index_name} in target database.")
                    self.config_parser.print_log_message( 'DEBUG', f"Executing comment query: {query}")
                    self.target_connection.execute_query(query)

            all_constraints = self.migrator_tables.fetch_all_constraints()
            for constraint_detail in all_constraints:
                constraint_data = self.migrator_tables.decode_constraint_row(constraint_detail)
                if constraint_data['constraint_comment']:
                    query = f"""COMMENT ON CONSTRAINT "{constraint_data['constraint_name']}" ON "{constraint_data['target_schema']}"."{constraint_data['target_table']}" IS '{constraint_data['constraint_comment']}'"""
                    self.config_parser.print_log_message('INFO', f"Setting comment for constraint {constraint_data['constraint_name']} in target database.")
                    self.config_parser.print_log_message( 'DEBUG', f"Executing comment query: {query}")
                    self.target_connection.execute_query(query)

            all_triggers = self.migrator_tables.fetch_all_triggers()
            for trigger_detail in all_triggers:
                trigger_data = self.migrator_tables.decode_trigger_row(trigger_detail)
                if trigger_data['trigger_comment']:
                    query = f"""COMMENT ON TRIGGER "{trigger_data['target_schema']}"."{trigger_data['trigger_name']}" IS '{trigger_data['trigger_comment']}'"""
                    self.config_parser.print_log_message('INFO', f"Setting comment for trigger {trigger_data['trigger_name']} in target database.")
                    self.config_parser.print_log_message( 'DEBUG', f"Executing comment query: {query}")
                    self.target_connection.execute_query(query)

            all_views = self.migrator_tables.fetch_all_views()
            for view_detail in all_views:
                view_data = self.migrator_tables.decode_view_row(view_detail)
                if view_data['view_comment']:
                    query = f"""COMMENT ON VIEW "{view_data['target_schema']}"."{view_data['view_name']}" IS '{view_data['view_comment']}'"""
                    self.config_parser.print_log_message('INFO', f"Setting comment for view {view_data['view_name']} in target database.")
                    self.config_parser.print_log_message( 'DEBUG', f"Executing comment query: {query}")
                    self.target_connection.execute_query(query)

            all_user_defined_types = self.migrator_tables.fetch_all_user_defined_types()
            for type_detail in all_user_defined_types:
                type_data = self.migrator_tables.decode_user_defined_type_row(type_detail)
                if type_data['type_comment']:
                    query = f"""COMMENT ON TYPE "{type_data['target_schema']}"."{type_data['type_name']}" IS '{type_data['type_comment']}'"""
                    self.config_parser.print_log_message('INFO', f"Setting comment for user defined type {type_data['type_name']} in target database.")
                    self.config_parser.print_log_message( 'DEBUG', f"Executing comment query: {query}")
                    self.target_connection.execute_query(query)

            self.target_connection.disconnect()
            self.migrator_tables.update_main_status('Orchestrator', 'comments migration', True, 'finished OK')
            self.config_parser.print_log_message('INFO', "Comments migrated successfully.")
        except Exception as e:
            self.migrator_tables.update_main_status('Orchestrator', 'comments migration', False, f'ERROR: {e}')
            self.handle_error(e, 'migrate_comments')
            self.target_connection.disconnect()
            return False

    def handle_error(self, e, description=None):
        self.config_parser.print_log_message('ERROR', f"An error in {self.__class__.__name__} ({description}): {e}")
        self.config_parser.print_log_message('ERROR', traceback.format_exc())
        if self.on_error_action == 'stop':
            self.config_parser.print_log_message('ERROR', "Stopping due to error.")
            exit(1)

    def check_pausing_resuming(self):
        if self.config_parser.pause_migration_fired():
            self.config_parser.print_log_message('INFO', f"Orchestrator paused. Waiting for resume signal...")
            self.config_parser.wait_for_resume()
            self.config_parser.print_log_message('INFO', f"Orchestrator resumed.")

if __name__ == "__main__":
    print("This script is not meant to be run directly")
