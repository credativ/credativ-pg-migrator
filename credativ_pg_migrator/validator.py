import concurrent.futures
import time
from credativ_pg_migrator.migrator_tables import MigratorTables
from credativ_pg_migrator.migrator_logging import MigratorLogger
from credativ_pg_migrator.connectors.postgresql_connector import PostgresqlConnector
from credativ_pg_migrator.connectors.oracle_connector import OracleConnector
import traceback

class Validator:
    def __init__(self, config_parser):
        self.config_parser = config_parser
        self.migrator_tables = MigratorTables(self.config_parser)
        
        log_file = self.config_parser.get_log_file()
        self.val_logger = MigratorLogger(log_file)
        self.val_logger.logger.info("Initializing Data Validator...")

    def _get_connector(self, source_or_target):
        db_type = self.config_parser.get_db_type(source_or_target)
        if db_type == 'postgresql':
            return PostgresqlConnector(self.config_parser, source_or_target)
        elif db_type == 'oracle':
            return OracleConnector(self.config_parser, source_or_target)
        else:
            self.val_logger.logger.warning(f"Validation functions might not be fully implemented for {db_type}")
            raise NotImplementedError(f"Validation not supported for {db_type}")

    def run(self):
        self.val_logger.logger.info("=========================================")
        self.val_logger.logger.info("      Starting Data Validator Module     ")
        self.val_logger.logger.info("=========================================")
        
        self.migrator_tables.create_table_for_validation()

        tables = self.migrator_tables.fetch_all_tables(only_unfinished=False)
        if not tables:
            self.val_logger.logger.info("No tables found in migrator tracking to validate.")
            return

        threads = self.config_parser.get_validator_workers()
        check_counts = self.config_parser.is_validation_row_counts_enabled()
        check_table_sum = self.config_parser.is_validation_table_checksums_enabled()
        check_random = self.config_parser.is_validation_random_sample_enabled()
        check_lob = self.config_parser.is_validation_lob_sizes_enabled()
        sample_size = self.config_parser.get_validation_sample_size()

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            futures = []
            for t in tables.values():
                if t.get('target_table_rows', 0) > 0 or t.get('source_table_rows', 0) > 0:
                    futures.append(executor.submit(
                        self.validate_table, 
                        t, check_counts, check_table_sum, check_random, check_lob, sample_size
                    ))
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    res = future.result()
                    if res:
                        results.append(res)
                except Exception as e:
                    self.val_logger.logger.error(f"Error validating table: {e}")
                    self.val_logger.logger.error(traceback.format_exc())

        self.migrator_tables.print_validation_summary(val_logger=self.val_logger.logger)
        self.val_logger.stop_logging()

    def validate_table(self, table_info, check_counts, check_table_sum, check_random, check_lob, sample_size):
        source_conn = self._get_connector('source')
        target_conn = self._get_connector('target')
        
        source_schema = table_info['source_schema_name']
        source_table = table_info['source_table_name']
        target_schema = table_info['target_schema_name']
        target_table = table_info['target_table_name']

        res = {
            'target_table': f"{target_schema}.{target_table}",
            'row_logic': None,
            'row_msg': '',
            'table_hash_logic': None,
            'table_msg': '',
            'row_hash_logic': None,
            'row_hash_msg': '',
            'lob_size_logic': None,
            'lob_size_msg': '',
            'passed': True
        }
        
        self.val_logger.logger.info(f"Validating {res['target_table']} ...")
        
        workflow = self.config_parser.get_workflow()
        if workflow == 'mapping':
            columns_dict = self.migrator_tables.fetch_mapping_target_columns({'table_id': table_info['id']})
            source_columns = self.migrator_tables.fetch_mapping_source_columns(table_info['id'])
        else:
            columns_dict = self.migrator_tables.fetch_table_columns_target(table_info['id'])
            source_columns = self.migrator_tables.fetch_table_columns_source(table_info['id'])

        target_cols = [col for col in columns_dict.values()]
        source_cols = [col for col in source_columns.values()]
        
        pk_cols = self.migrator_tables.select_primary_key({'source_schema_name': source_schema, 'source_table_name': source_table})
        if pk_cols:
            pk_cols_list = [c.strip('" ') for c in pk_cols.split(',')]
        else:
            pk_cols_list = []

        try:
            if check_counts:
                s_count = source_conn.get_rows_count(source_schema, source_table)
                t_count = target_conn.get_rows_count(target_schema, target_table)
                res['row_logic'] = (s_count == t_count)
                if not res['row_logic']:
                    res['passed'] = False
                    res['row_msg'] = f"Fail: Src={s_count}, Tgt={t_count}"
                else:
                    res['row_msg'] = f"Pass: {s_count} rows"

            if check_table_sum:
                s_sum = source_conn.get_table_checksum(source_schema, source_table, source_cols)
                t_sum = target_conn.get_table_checksum(target_schema, target_table, target_cols)
                res['table_hash_logic'] = (s_sum == t_sum and s_sum is not None)
                if not res['table_hash_logic']:
                    res['passed'] = False
                    res['table_msg'] = f"Fail: Src={s_sum}, Tgt={t_sum}"
                else:
                    res['table_msg'] = f"Pass: {s_sum}"

            if check_random and pk_cols_list:
                pks = target_conn.get_random_pks(target_schema, target_table, pk_cols_list, sample_size)
                if pks:
                    s_row_sums = source_conn.get_row_checksums(source_schema, source_table, pk_cols_list, pks, source_cols)
                    t_row_sums = target_conn.get_row_checksums(target_schema, target_table, pk_cols_list, pks, target_cols)
                    
                    mismatches = 0
                    for pk_val, t_hash in t_row_sums.items():
                        if s_row_sums.get(pk_val) != t_hash:
                            mismatches += 1
                    
                    res['row_hash_logic'] = (mismatches == 0)
                    if mismatches > 0:
                        res['passed'] = False
                        res['row_hash_msg'] = f"Fail: {mismatches}/{len(pks)} sample rows mismatched"
                    else:
                        res['row_hash_msg'] = f"Pass: {len(pks)} samples matched"
                else:
                    res['row_hash_msg'] = "Skip: No samples fetched"
            elif check_random and not pk_cols_list:
                res['row_hash_msg'] = "Skip: No PKs available"

            if check_lob and pk_cols_list:
                s_lobs = [c for c in source_cols if any(x in c.get('data_type', '').lower() for x in ['lob', 'text', 'bytea', 'image', 'xml', 'json'])]
                t_lobs = [c for c in target_cols if any(x in c.get('data_type', '').lower() for x in ['lob', 'text', 'bytea', 'image', 'xml', 'json'])]
                if s_lobs and t_lobs and len(s_lobs) == len(t_lobs):
                    pks = target_conn.get_random_pks(target_schema, target_table, pk_cols_list, sample_size)
                    if pks:
                        s_lob_sizes = source_conn.get_lob_sizes(source_schema, source_table, pk_cols_list, pks, s_lobs)
                        t_lob_sizes = target_conn.get_lob_sizes(target_schema, target_table, pk_cols_list, pks, t_lobs)
                        
                        mismatches = 0
                        for pk_val, t_sizes in t_lob_sizes.items():
                            s_sizes = s_lob_sizes.get(pk_val)
                            if s_sizes != t_sizes:
                                mismatches += 1
                                
                        res['lob_size_logic'] = (mismatches == 0)
                        if mismatches > 0:
                            res['passed'] = False
                            res['lob_size_msg'] = f"Fail: {mismatches}/{len(pks)} sample LOB sizes mismatched"
                        else:
                            res['lob_size_msg'] = f"Pass: {len(pks)} samples matched"
                    else:
                        res['lob_size_msg'] = "Skip: No samples fetched"
                else:
                    res['lob_size_msg'] = "Skip: No matching LOB columns identified"
            elif check_lob and not pk_cols_list:
                res['lob_size_msg'] = "Skip: No PKs available"

        except Exception as e:
            self.val_logger.logger.error(f"Validation crash on {res['target_table']}: {e}")
            res['passed'] = False
            res['row_msg'] = f"Error: {e}"

        if res['passed']:
            self.val_logger.logger.info(f"OK: {res['target_table']} passed all active validations.")
        else:
            self.val_logger.logger.warning(f"FAIL: {res['target_table']} failed validation.")

        try:
            self.migrator_tables.insert_validation_result({
                'target_schema_name': target_schema,
                'target_table_name': target_table,
                'row_logic': res['row_logic'],
                'row_msg': res['row_msg'],
                'table_hash_logic': res['table_hash_logic'],
                'table_msg': res['table_msg'],
                'row_hash_logic': res['row_hash_logic'],
                'row_hash_msg': res['row_hash_msg'],
                'lob_size_logic': res['lob_size_logic'],
                'lob_size_msg': res['lob_size_msg'],
                'passed': res['passed']
            })
        except Exception as e:
            self.val_logger.logger.error(f"Error persisting validation protocol for {res['target_table']}: {e}")

        return res


