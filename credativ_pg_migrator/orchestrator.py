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
        self.migrator_tables.insert_main('Orchestrator','')

    def run(self):
        try:
            self.config_parser.print_log_message('INFO', "Starting orchestration...")

            self.run_create_user_defined_types()

            ## migration of domains is a bit unclear currently
            ## domains in PostgreSQL are special data types
            ## But in Sybase ASE they are defined as sort of additional check constraint on the column
            # self.run_create_domains()

            self.run_migrate_tables()
            self.run_migrate_indexes('standard')
            self.run_migrate_constraints()
            self.run_migrate_views()
            self.run_migrate_funcprocs()
            self.run_migrate_triggers()
            self.run_migrate_indexes('function_based')
            self.run_migrate_comments()

            self.run_post_migration_script()
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
        }

        self.config_parser.print_log_message('INFO', f"Starting {workers_requested} parallel workers to create tables in target database.")
        migrate_tables = self.migrator_tables.fetch_all_tables()
        if len(migrate_tables) > 0:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers_requested) as executor:
                futures = {}
                for table_row in migrate_tables:
                    table_data = self.migrator_tables.decode_table_row(table_row)
                    # (table_data['primary_key_columns'],
                    # table_data['primary_key_columns_count'],
                    # table_data['primary_key_columns_types']) = self.migrator_tables.select_primary_key(table_data['target_schema'], table_data['target_table'])
                    if len(futures) >= workers_requested:
                        done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                        for future in done:
                            table_done = futures[future]
                            if future.result() == False:
                                if self.on_error_action == 'stop':
                                    self.config_parser.print_log_message('ERROR', "Stopping execution due to error.")
                                    exit(1)
                            else:
                                self.migrator_tables.update_table_status(table_done['id'], True, 'migrated OK')

                            futures.pop(future)
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

                    if len(futures) >= workers_requested:
                        done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                        for future in done:
                            index_done = futures[future]
                            if future.result() == False:
                                if self.on_error_action == 'stop':
                                    self.config_parser.print_log_message('ERROR', "Stopping execution due to error.")
                                    exit(1)
                            else:
                                self.migrator_tables.update_index_status(index_done['id'], True, 'migrated OK')

                            futures.pop(future)

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
                    if len(futures) >= workers_requested:
                        done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                        for future in done:
                            constraint_done = futures[future]
                            if future.result() == False:
                                if self.on_error_action == 'stop':
                                    self.config_parser.print_log_message('ERROR', "Stopping execution due to error.")
                                    exit(1)
                            else:
                                self.migrator_tables.update_constraint_status(constraint_done['id'], True, 'migrated OK')

                            futures.pop(future)

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

            if settings['drop_tables']:
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

            if settings['create_tables']:
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

            if settings['truncate_tables']:
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

            if settings['migrate_data']:
                # data migration
                part_name = 'connect source'
                worker_source_connection = self.load_connector('source')

                part_name = 'migrate data'
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Migrating data for table {target_table} from source database.")

                worker_source_connection.connect()

                settings = {
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
                    'batch_size': settings['batch_size'],
                    'migrator_tables': settings['migrator_tables'],
                    'migration_limitation': '',
                }

                rows_migration_limitations = settings['migrator_tables'].get_records_data_migration_limitation(table_data['source_table'])
                migration_limitations = []
                if rows_migration_limitations:
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Found data migration limitations matching table {target_table}: {rows_migration_limitations}")
                    for limitation in rows_migration_limitations:
                        where_clause = limitation[0]
                        use_when_column_name = limitation[1]
                        for col_order_num, column_info in table_data['source_columns'].items():
                            column_name = column_info['column_name']
                            if column_name == use_when_column_name or re.match(use_when_column_name, column_name):
                                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Column {column_name} matches migration limitation.")
                                migration_limitations.append(where_clause)
                    if migration_limitations:
                        settings['migration_limitation'] = f"{' AND '.join(migration_limitations)}" if migration_limitations else ''
                        self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Migration limitations for table {target_table}: {migration_limitations}")
                rows_migrated = worker_source_connection.migrate_table(worker_target_connection, settings)
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
                                self.config_parser.print_log_message('ERROR', f"Worker {worker_id}: Error setting sequence {sequence_name} for table {target_table}: {e}")
                    else:
                        self.config_parser.print_log_message('INFO', f"Worker {worker_id}: No sequences found for table {target_table}.")
                else:
                    self.config_parser.print_log_message('INFO', f"Worker {worker_id}: No data found for table {target_table} - skipping sequences.")
            else:
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Skipping data migration for table {target_table}.")

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

            if create_constraint_sql:
                # Each worker uses its own separate connection to the target database
                worker_target_connection = self.load_connector('target')

                self.config_parser.print_log_message( 'DEBUG', f"Worker {worker_id}: Creating constraint with SQL: {create_constraint_sql}")

                worker_target_connection.connect()

                query = f'''SET SESSION search_path TO {constraint_data['target_schema']};'''
                worker_target_connection.execute_query(query)

                if worker_target_connection.session_settings:
                    self.config_parser.print_log_message( 'DEBUG', f"Worker {worker_id}: Executing session settings: {worker_target_connection.session_settings}")
                    worker_target_connection.execute_query(worker_target_connection.session_settings)

                worker_target_connection.execute_query(create_constraint_sql)
                query = 'RESET search_path;'
                worker_target_connection.execute_query(query)
                self.config_parser.print_log_message('INFO', f"""Worker {worker_id}: Constraint "{constraint_name}" created successfully.""")
                worker_target_connection.disconnect()
                return True
            else:
                self.config_parser.print_log_message('INFO', f"Worker {worker_id}: Constraint {constraint_name} does not have a SQL statement - skipping.")
                return False

        except Exception as e:
            self.migrator_tables.update_constraint_status(constraint_data['id'], False, f'ERROR: {e}')
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

if __name__ == "__main__":
    print("This script is not meant to be run directly")
