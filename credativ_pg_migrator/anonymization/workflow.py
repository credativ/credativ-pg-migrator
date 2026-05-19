import concurrent.futures
import uuid
import psycopg2
import psycopg2.extras
from credativ_pg_migrator.anonymization.routing import MigratorAnonymizer

class AnonymizationWorkflow:
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.config_parser = orchestrator.config_parser
        self.migrator_tables = orchestrator.migrator_tables
        self.logger = orchestrator.logger

        # Validate that both source and target are PostgreSQL
        if self.config_parser.get_source_db_type() != 'postgresql' or self.config_parser.get_target_db_type() != 'postgresql':
            raise ValueError("Anonymization workflow currently only supports PostgreSQL to PostgreSQL migrations.")

        self.anonymizer = MigratorAnonymizer(self.config_parser.config)

    def run(self):
        self.config_parser.print_log_message('INFO', "anonymization_workflow: run: Starting PG-to-PG Anonymization Workflow.")

        if not self.anonymizer.is_active():
            self.config_parser.print_log_message('WARNING', "anonymization_workflow: No anonymization rules found. Acting as standard PG-to-PG copy.")

        self.orchestrator.mapping_drop_indexes_and_constraints()
        self.anonymization_copy_data()
        self.orchestrator.mapping_create_indexes_and_constraints()
        self.orchestrator.mapping_check_indexes_and_constraints()

    def anonymization_copy_data(self):
        self.migrator_tables.insert_main({'task_name': 'Orchestrator', 'subtask_name': 'anonymization data copy'})
        workers_requested = self.config_parser.get_parallel_workers_count()

        self.config_parser.print_log_message('INFO', f"anonymization_workflow: Starting {workers_requested} parallel workers for anonymization copy.")

        raw_tables = self.migrator_tables.fetch_all_tables(only_unfinished=False)
        migrate_tables = []
        for table_row in raw_tables:
            table_data = self.migrator_tables.decode_table_row(table_row)
            source_rows = table_data.get('source_table_rows', 0)
            
            # Simplified logic for example: just process tables with rows
            if source_rows > 0:
                migrate_tables.append(table_data)
            else:
                self.migrator_tables.update_table_status({'row_id': table_data['id'], 'success': True, 'message': 'anonymized data OK (0 rows)'})

        if migrate_tables:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers_requested) as executor:
                futures = {}
                for table_data in migrate_tables:
                    while len(futures) >= workers_requested:
                        done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                        for future in done:
                            table_done = futures.pop(future)
                            if not future.result():
                                self.config_parser.print_log_message('ERROR', "anonymization_workflow: Stopping execution due to error.")
                                exit(1)
                            else:
                                self.migrator_tables.update_table_status({'row_id': table_done['id'], 'success': True, 'message': 'anonymized data OK'})

                    future = executor.submit(self.anonymization_data_worker, table_data)
                    futures[future] = table_data

                for future in concurrent.futures.as_completed(futures):
                    table_done = futures[future]
                    if not future.result():
                        self.config_parser.print_log_message('ERROR', "anonymization_workflow: Stopping execution due to error.")
                        exit(1)
                    else:
                        self.migrator_tables.update_table_status({'row_id': table_done['id'], 'success': True, 'message': 'anonymized data OK'})
        
        self.migrator_tables.update_main_status({'task_name': 'Orchestrator', 'subtask_name': 'anonymization data copy', 'success': True, 'message': 'finished OK'})

    def anonymization_data_worker(self, table_data):
        worker_id = uuid.uuid4()
        source_schema = table_data['source_schema_name']
        source_table = table_data['source_table_name']
        target_schema = table_data['target_schema_name']
        target_table = table_data['target_table_name']
        
        try:
            self.config_parser.print_log_message('INFO', f"anonymization_workflow: Worker {worker_id}: Processing {target_table}")

            source_conn = self.orchestrator.load_connector('source')
            target_conn = self.orchestrator.load_connector('target')
            
            source_conn.connect()
            target_conn.connect()

            batch_size = self.config_parser.get_batch_size()
            
            target_columns = list(table_data.get('target_columns', {}).values())
            # Sort columns by column_id for correct ordering
            target_columns.sort(key=lambda x: x.get('column_id', 0))
            col_names = [col['column_name'] for col in target_columns]
            
            select_sql = f'SELECT {", ".join([f"{col}" for col in col_names])} FROM "{source_schema}"."{source_table}"'
            
            src_cursor = source_conn.connection.cursor(name=f"cursor_{worker_id}")
            src_cursor.execute(select_sql)
            
            conflict_action = self.config_parser.get_mapping_data_resolution(target_table)
            if conflict_action == 'replace':
                target_conn.execute_query(f'TRUNCATE TABLE "{target_schema}"."{target_table}"')

            while True:
                rows = src_cursor.fetchmany(batch_size)
                if not rows:
                    break
                
                # Convert rows to dicts for anonymizer
                formatted_data = []
                for row in rows:
                    row_dict = dict(zip(col_names, row))
                    if self.anonymizer.is_active():
                        row_dict = self.anonymizer.anonymize_row(target_table, row_dict)
                    formatted_data.append([row_dict[col] for col in col_names])
                
                # Determine __RAW_SQL__ injection
                if formatted_data:
                    first_row = formatted_data[0]
                    new_insert_values_list = []
                    raw_sql_indices = set()
                    
                    for i, val in enumerate(first_row):
                        if isinstance(val, str) and val.startswith('__RAW_SQL__:'):
                            raw_sql_content = val.split('__RAW_SQL__:', 1)[1]
                            new_insert_values_list.append(raw_sql_content)
                            raw_sql_indices.add(i)
                        else:
                            new_insert_values_list.append('%s')
                            
                    insert_values_str = ', '.join(new_insert_values_list)
                    insert_query = f'INSERT INTO "{target_schema}"."{target_table}" ({", ".join(col_names)}) VALUES ({insert_values_str})'
                    
                    # Remove RAW SQL items from tuples
                    if raw_sql_indices:
                        cleaned_data = []
                        for row in formatted_data:
                            new_row = tuple(val for i, val in enumerate(row) if i not in raw_sql_indices)
                            cleaned_data.append(new_row)
                        formatted_data = cleaned_data
                    
                    # Execute batch
                    tgt_cursor = target_conn.connection.cursor()
                    psycopg2.extras.execute_batch(tgt_cursor, insert_query, formatted_data, page_size=batch_size)
                    target_conn.connection.commit()
                    tgt_cursor.close()

            src_cursor.close()
            source_conn.disconnect()
            target_conn.disconnect()
            return True

        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"anonymization_workflow: Worker {worker_id}: Error processing {target_table} -> {e}")
            return False
