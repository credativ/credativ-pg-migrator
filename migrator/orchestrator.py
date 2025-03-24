import concurrent.futures
# from worker import Worker
from migrator_logging import MigratorLogger
from migrator_tables import MigratorTables
from postgresql_connector import PostgreSQLConnector
from informix_connector import InformixConnector
from sybase_ase_connector import SybaseASEConnector
import traceback
import uuid
import fnmatch

class Orchestrator:
    def __init__(self, config_parser):
        self.config_parser = config_parser
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger
        self.source_connection = self.connect_to_source_db()
        self.target_connection = self.connect_to_target_db()
        self.migrator_tables = MigratorTables(self.logger, self.config_parser)
        self.on_error_action = self.config_parser.get_on_error_action()
        self.source_schema = self.config_parser.get_source_schema()
        self.target_schema = self.config_parser.get_target_schema()
        self.migrator_tables.insert_main('Orchestrator')

    def run(self):
        try:
            self.logger.info("Starting orchestration...")

            self.run_migrate_tables()
            self.run_migrate_indexes()
            self.run_migrate_constraints()
            self.run_migrate_funcprocs()
            self.run_migrate_triggers()

            self.run_post_migration_script()
            self.logger.info("Orchestration complete.")
            self.migrator_tables.update_main_status('Orchestrator', True, 'finished OK')

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
            self.handle_error(e, 'orchestration')

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
        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

    def run_post_migration_script(self):
        post_migration_script = self.config_parser.get_post_migration_script()
        if post_migration_script:
            self.logger.info("Running post-migration script in target database.")
            try:
                self.target_connection.connect()
                self.target_connection.execute_sql_script(post_migration_script)
                self.target_connection.disconnect()
                self.logger.info("Post-migration script executed successfully.")
            except Exception as e:
                self.handle_error(e, 'post-migration script')

    def run_migrate_tables(self):
        self.migrator_tables.insert_main('Orchestrator - tables migration')
        workers_requested = self.config_parser.get_parallel_workers_count()
        settings = {
            'source_db_type': self.config_parser.get_source_db_type(),
            'target_db_type': self.config_parser.get_target_db_type(),
            'create_tables': self.config_parser.should_create_tables(),
            'drop_tables': self.config_parser.should_drop_tables(),
            'migrate_data': self.config_parser.should_migrate_data(),
            'batch_size': self.config_parser.get_batch_size()
        }

        self.logger.info(f"Starting {workers_requested} parallel workers to create tables in target database.")
        migrate_tables = self.migrator_tables.fetch_all_tables()
        if len(migrate_tables) > 0:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers_requested) as executor:
                futures = {}
                for table_row in migrate_tables:
                    table_data = self.migrator_tables.decode_table_row(table_row)
                    if len(futures) >= workers_requested:
                        done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                        for future in done:
                            table_done = futures[future]
                            if future.result() == False:
                                self.migrator_tables.update_table_status(table_done['id'], False, 'ERROR in migration - see log')
                                if self.on_error_action == 'stop':
                                    self.logger.error("Stopping execution due to error.")
                                    exit(1)
                            else:
                                self.migrator_tables.update_table_status(table_done['id'], True, 'migrated OK')

                            futures.pop(future)
                    table_data['primary_key_columns'] = self.migrator_tables.select_primary_key(table_data['target_schema'], table_data['target_table'])
                    future = executor.submit(self.table_worker, table_data, settings)
                    futures[future] = table_data

                # Process remaining futures
                self.logger.info("Processing remaining futures")
                for future in concurrent.futures.as_completed(futures):
                    table_done = futures[future]
                    if future.result() == False:
                        self.migrator_tables.update_table_status(table_done['id'], False, 'ERROR in migration - see log')
                        if self.on_error_action == 'stop':
                            self.logger.error("Stopping execution due to error.")
                            exit(1)
                    else:
                        self.migrator_tables.update_table_status(table_done['id'], True, 'migrated OK')

            self.logger.info("Tables processed successfully.")
        else:
            self.logger.info("No tables to create.")

        self.migrator_tables.update_main_status('Orchestrator - tables migration', True, 'finished OK')

    def run_migrate_indexes(self):
        self.migrator_tables.insert_main('Orchestrator - indexes migration')
        workers_requested = self.config_parser.get_parallel_workers_count()
        target_db_type = self.config_parser.get_target_db_type()

        self.logger.info(f"Starting {workers_requested} parallel workers to create indexes in target database.")
        migrate_indexes = self.migrator_tables.fetch_all_indexes()
        if len(migrate_indexes) > 0:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers_requested) as executor:
                futures = {}
                for index_row in migrate_indexes:
                    index_data = self.migrator_tables.decode_index_row(index_row)
                    if len(futures) >= workers_requested:
                        done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                        for future in done:
                            index_done = futures[future]
                            if future.result() == False:
                                self.migrator_tables.update_index_status(index_done['id'], False, 'ERROR in index creation - see log')
                                if self.on_error_action == 'stop':
                                    self.logger.error("Stopping execution due to error.")
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
                        self.migrator_tables.update_index_status(index_done['id'], False, 'ERROR in index creation - see log')
                        if self.on_error_action == 'stop':
                            self.logger.error("Stopping execution due to error.")
                            exit(1)
                    else:
                        self.migrator_tables.update_index_status(index_done['id'], True, 'migrated OK')

            self.logger.info("Indexes processed successfully.")
        else:
            self.logger.info("No indexes to create.")

        self.migrator_tables.update_main_status('Orchestrator - indexes migration', True, 'finished OK')

    def run_migrate_constraints(self):
        self.migrator_tables.insert_main('Orchestrator - constraints migration')
        workers_requested = self.config_parser.get_parallel_workers_count()
        target_db_type = self.config_parser.get_target_db_type()

        self.logger.info(f"Starting {workers_requested} parallel workers to create constraints in target database.")
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
                                self.migrator_tables.update_constraint_status(constraint_done['id'], False, 'ERROR in constraint creation - see log')
                                if self.on_error_action == 'stop':
                                    self.logger.error("Stopping execution due to error.")
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
                        self.migrator_tables.update_constraint_status(constraint_done['id'], False, 'ERROR in constraint creation - see log')
                        if self.on_error_action == 'stop':
                            self.logger.error("Stopping execution due to error.")
                            exit(1)
                    else:
                        self.migrator_tables.update_constraint_status(constraint_done['id'], True, 'migrated OK')

            self.logger.info("Constraints processed successfully.")
        else:
            self.logger.info("No constraints to create.")

        self.migrator_tables.update_main_status('Orchestrator - constraints migration', True, 'finished OK')

    def table_worker(self, table_data, settings):
        worker_id = uuid.uuid4()
        part_name = 'start'
        worker_source_connection = None
        worker_target_connection = None
        try:
            target_schema = table_data['target_schema']
            target_table = table_data['target_table']
            create_table_sql = table_data['target_table_sql']

            if create_table_sql is None:
                self.logger.info(f"Table {target_table} does not have a CREATE TABLE statement - skipping.")
                return False

            self.logger.info(f"Worker {worker_id}: Creating table {target_table} in target database ({settings['source_db_type']}:{settings['target_db_type']}-{settings['drop_tables']}/{settings['create_tables']}/{settings['migrate_data']}).")

            # Each worker uses its own separate connection to the target database
            if settings['target_db_type'] == 'postgresql':
                worker_target_connection = PostgreSQLConnector(self.config_parser, 'target')
            else:
                raise ValueError(f"Unsupported target database type: {settings['target_db_type']}")

            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Worker {worker_id}: Creating table with SQL: {create_table_sql}")

            part_name = 'connect target'
            worker_target_connection.connect()

            if settings['drop_tables']:
                part_name = 'drop table'
                worker_target_connection.execute_query(f"DROP TABLE IF EXISTS {target_schema}.{target_table}")
                self.logger.info(f"""Worker {worker_id}: Table "{target_table}" dropped successfully.""")

            if settings['create_tables']:
                part_name = 'create table'
                worker_target_connection.execute_query(create_table_sql)
                self.logger.info(f"""Worker {worker_id}: Table "{target_table}" created successfully.""")

            if settings['migrate_data']:
                # data migration
                part_name = 'connect source'
                if settings['source_db_type'] == 'postgresql':
                    worker_source_connection = PostgreSQLConnector(self.config_parser, 'source')
                elif settings['source_db_type'] == 'informix':
                    worker_source_connection = InformixConnector(self.config_parser, 'source')
                elif settings['source_db_type'] == 'sybase_ase':
                    worker_source_connection = SybaseASEConnector(self.config_parser, 'source')
                else:
                    raise ValueError(f"Unsupported source database type: {settings['source_db_type']}")

                part_name = 'migrate data'
                self.logger.info(f"Worker {worker_id}: Migrating data for table {target_table} from source database.")
                source_schema = table_data['source_schema']
                source_table = table_data['source_table']

                worker_source_connection.connect()

                settings = {
                    'worker_id': worker_id,
                    'source_schema': source_schema,
                    'source_table': source_table,
                    'source_columns': table_data['source_columns'],
                    'target_schema': target_schema,
                    'target_table': target_table,
                    'target_columns': table_data['target_columns'],
                    'primary_key_columns': table_data['primary_key_columns'],
                    'batch_size': settings['batch_size'],
                }
                rows_migrated = worker_source_connection.migrate_table(worker_target_connection, settings)
                worker_source_connection.disconnect()

                if rows_migrated > 0:
                    # sequences setting
                    part_name = 'sequences'
                    self.logger.info(f"Worker {worker_id}: Setting sequences for table {target_table} in target database.")
                    sequences = worker_target_connection.fetch_sequences(target_schema, target_table)
                    if sequences:
                        for order_num, sequence_details in sequences.items():
                            sequence_id = sequence_details['id']
                            sequence_name = sequence_details['name']
                            sequence_sql = sequence_details['set_sequence_sql']
                            if self.config_parser.get_log_level() == 'DEBUG':
                                self.logger.debug(f"Worker {worker_id}: Setting sequence with SQL: {sequence_sql}")
                            worker_target_connection.execute_query(sequence_sql)
                            self.logger.info(f"Worker {worker_id}: Sequence ({order_num}) {sequence_name} set successfully for table {target_table}.")
                            seq_curr_val = worker_target_connection.get_sequence_current_value(sequence_id)
                            self.logger.info(f"Worker {worker_id}: Current value of sequence {sequence_name} is {seq_curr_val}.")
                    else:
                        self.logger.info(f"Worker {worker_id}: No sequences found for table {target_table}.")
                else:
                    self.logger.info(f"Worker {worker_id}: No data found for table {target_table} - skipping sequences.")
            else:
                self.logger.info(f"Worker {worker_id}: Skipping data migration for table {target_table}.")

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
            self.handle_error(e_main, f"table_worker {worker_id} ({part_name}) {target_table}")
            return False

    def index_worker(self, index_data, target_db_type):
        worker_id = uuid.uuid4()
        try:
            index_name = index_data['index_name']
            create_index_sql = index_data['index_sql']

            self.logger.info(f"Worker {worker_id}: Creating index {index_name} in target database.")

            # Each worker uses its own separate connection to the target database
            if target_db_type == 'postgresql':
                worker_target_connection = PostgreSQLConnector(self.config_parser, 'target')
            else:
                raise ValueError(f"Unsupported target database type: {target_db_type}")

            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Worker {worker_id}: Creating index with SQL: {create_index_sql}")

            worker_target_connection.connect()

            worker_target_connection.execute_query(create_index_sql)
            self.logger.info(f"""Worker {worker_id}: Index "{index_name}" created successfully.""")

            worker_target_connection.disconnect()
            return True
        except Exception as e:
            self.handle_error(e, f"index_worker {worker_id} {index_name}")
            return False

    def constraint_worker(self, constraint_data, target_db_type):
        worker_id = uuid.uuid4()
        try:
            constraint_name = constraint_data['constraint_name']
            create_constraint_sql = constraint_data['constraint_sql']

            self.logger.info(f"Worker {worker_id}: Creating constraint {constraint_name} in target database.")

            # Each worker uses its own separate connection to the target database
            if target_db_type == 'postgresql':
                worker_target_connection = PostgreSQLConnector(self.config_parser, 'target')
            else:
                raise ValueError(f"Unsupported target database type: {target_db_type}")

            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Worker {worker_id}: Creating constraint with SQL: {create_constraint_sql}")

            worker_target_connection.connect()

            worker_target_connection.execute_query(create_constraint_sql)
            self.logger.info(f"""Worker {worker_id}: Constraint "{constraint_name}" created successfully.""")

            worker_target_connection.disconnect()
            return True
        except Exception as e:
            self.handle_error(e, f"constraint_worker {worker_id} {constraint_name}")
            return False

    def run_migrate_funcprocs(self):
        self.migrator_tables.insert_main('Orchestrator - functions/procedures migration')
        include_funcprocs = self.config_parser.get_include_funcprocs() or []
        exclude_funcprocs = self.config_parser.get_exclude_funcprocs() or []

        if self.config_parser.should_migrate_funcprocs():
            self.logger.info("Migrating functions and procedures.")
            funcproc_names = self.source_connection.fetch_funcproc_names(self.config_parser.get_source_schema())
            if self.config_parser.get_log_level() == 'DEBUG':
                self.logger.debug(f"Function/procedure names: {funcproc_names}")

            if funcproc_names:
                for order_num, funcproc_data in funcproc_names.items():
                    self.logger.info(f"Processing func/proc {order_num}: {funcproc_data['name']}")
                    if not any(fnmatch.fnmatch(funcproc_data['name'], pattern) for pattern in include_funcprocs):
                        continue
                    if any(fnmatch.fnmatch(funcproc_data['name'], pattern) for pattern in exclude_funcprocs):
                        self.logger.info(f"Table {funcproc_data['name']} is excluded from migration.")
                        continue

                    funcproc_id = funcproc_data['id']
                    funcproc_type = funcproc_data['type']
                    self.logger.info(f"Migrating {funcproc_type} {funcproc_data['name']}.")
                    funcproc_code = self.source_connection.fetch_funcproc_code(funcproc_id)

                    table_names = []
                    converted_code = ''
                    try:
                        table_names = self.migrator_tables.fetch_all_target_table_names()
                    except Exception as e:
                        self.handle_error(e, 'fetching table names')

                    try:
                        converted_code = self.source_connection.convert_funcproc_code(
                                            funcproc_code,
                                            self.config_parser.get_target_db_type(),
                                            self.config_parser.get_source_schema(),
                                            self.config_parser.get_target_schema(),
                                            table_names)

                        self.migrator_tables.insert_funcprocs(self.source_schema, funcproc_data['name'], funcproc_id, funcproc_code, self.target_schema, funcproc_data['name'], converted_code)

                        if converted_code is not None:
                            self.logger.info(f"Creating {funcproc_type} {funcproc_data['name']} in target database.")
                            self.target_connection.connect()
                            self.target_connection.execute_query(converted_code)
                            if self.config_parser.get_log_level() == 'DEBUG':
                                self.logger.debug(f"[OK] Source code for {funcproc_data['name']}: {funcproc_code}")
                                self.logger.debug(f"[OK] Converted code for {funcproc_data['name']}: {converted_code}")
                            self.migrator_tables.update_funcproc_status(funcproc_id, True, 'migrated OK')
                        else:
                            self.logger.info(f"Skipping {funcproc_type} {funcproc_data['name']} - no conversion.")
                            self.migrator_tables.update_funcproc_status(funcproc_id, False, 'no conversion')
                        self.target_connection.disconnect()
                    except Exception as e:
                        if self.config_parser.get_log_level() == 'DEBUG':
                            self.logger.debug(f"[ERROR] Migrating {funcproc_type} {funcproc_data['name']}.")
                            self.logger.debug(f"[ERROR] Source code for {funcproc_data['name']}: {funcproc_code}")
                            self.logger.debug(f"[ERROR] Converted code for {funcproc_data['name']}: {converted_code}")
                        self.migrator_tables.update_funcproc_status(funcproc_id, False, 'ERROR in migration - see log')
                        self.handle_error(e, f"migrate_funcproc {funcproc_type} {funcproc_data['name']}")

                self.logger.info("Functions and procedures migrated successfully.")
            else:
                self.logger.info("No functions or procedures found to migrate.")
        else:
            self.logger.info("Skipping function and procedure migration as requested.")

        self.migrator_tables.update_main_status('Orchestrator - functions/procedures migration', True, 'finished OK')

    def run_migrate_triggers(self):
        self.migrator_tables.insert_main('Orchestrator - functions/procedures migration')
        try:
            if self.config_parser.should_migrate_triggers():
                self.logger.info("Migrating triggers.")

                all_triggers = self.migrator_tables.select_triggers()
                if all_triggers:
                    for one_trigger in all_triggers:
                        trigger_detail = self.migrator_tables.decode_trigger_row(one_trigger)
                        self.logger.info(f"Processing trigger {trigger_detail['trigger_name']}")
                        if self.config_parser.get_log_level() == 'DEBUG':
                            self.logger.debug(f"Trigger details: {trigger_detail}")

                        settings = {
                            'source_schema': self.config_parser.get_source_schema(),
                            'target_schema': self.config_parser.get_target_schema(),
                        }
                        converted_code = self.source_connection.convert_trigger(trigger_detail['trigger_sql'], settings)
                        try:
                            if converted_code is not None:
                                self.logger.info(f"Creating trigger {trigger_detail['trigger_name']} in target database.")
                                self.target_connection.connect()
                                self.target_connection.execute_query(converted_code)
                                if self.config_parser.get_log_level() == 'DEBUG':
                                    self.logger.debug(f"[OK] Source code for {trigger_detail['trigger_name']}: {trigger_detail['trigger_sql']}")
                                    self.logger.debug(f"[OK] Converted code for {trigger_detail['trigger_name']}: {converted_code}")
                                self.migrator_tables.update_trigger_status(trigger_detail['id'], True, 'migrated OK')
                            else:
                                self.logger.info(f"Skipping trigger {trigger_detail['trigger_name']} - no conversion.")
                                self.migrator_tables.update_trigger_status(trigger_detail['id'], False, 'no conversion')
                            self.target_connection.disconnect()
                        except Exception as e:
                            if self.config_parser.get_log_level() == 'DEBUG':
                                self.logger.debug(f"[ERROR] Migrating trigger {trigger_detail['trigger_name']}.")
                                self.logger.debug(f"[ERROR] Source code for {trigger_detail['trigger_name']}: {trigger_detail['trigger_sql']}")
                                self.logger.debug(f"[ERROR] Converted code for {trigger_detail['trigger_name']}: {converted_code}")
                            self.migrator_tables.update_trigger_status(trigger_detail['id'], False, 'ERROR in migration - see log')
                            self.handle_error(e, f"migrate_trigger {trigger_detail['trigger_name']}")

                    self.logger.info("Triggers migrated successfully.")
                else:
                    self.logger.info("No triggers found to migrate.")
            else:
                self.logger.info("Skipping trigger migration as requested.")
        except Exception as e:
            self.handle_error(e, 'migrate_triggers')

    def handle_error(self, e, description=None):
        self.logger.error(f"An error in {self.__class__.__name__} ({description}): {e}")
        self.logger.error(traceback.format_exc())
        if self.on_error_action == 'stop':
            self.logger.error("Stopping due to error.")
            exit(1)
