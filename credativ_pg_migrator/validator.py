import concurrent.futures
import time
from credativ_pg_migrator.migrator_tables import MigratorTables
from credativ_pg_migrator.migrator_logging import MigratorLogger
import traceback

class Validator:
    def __init__(self, config_parser):
        self.config_parser = config_parser
        
        log_file = self.config_parser.get_log_file()
        self.val_logger = MigratorLogger(log_file)
        self.val_logger.logger.info("Initializing Data Validator...")

        self.migrator_tables = MigratorTables(self.val_logger, self.config_parser)

    def _get_connector(self, direction):
        import importlib
        from credativ_pg_migrator.constants import MigratorConstants
        
        database_type = self.config_parser.get_db_type(direction)
        database_module = MigratorConstants.get_modules().get(database_type)
        
        if not database_module:
            self.val_logger.logger.error(f"Unsupported database type: {database_type}")
            raise ValueError(f"Unsupported database type: {database_type}")
            
        module_name, class_name = database_module.split(':')
        module = importlib.import_module(module_name)
        connector_class = getattr(module, class_name)
        
        return connector_class(self.config_parser, direction)

    def run(self):
        self.val_logger.logger.info("=========================================")
        self.val_logger.logger.info("      Starting Data Validator Module     ")
        self.val_logger.logger.info("=========================================")
        
        try:
            self.migrator_tables.create_table_for_validation()
    
            tables_raw = self.migrator_tables.fetch_all_tables(only_unfinished=False)
            if not tables_raw:
                self.val_logger.logger.info("No tables found in migrator tracking to validate.")
                return
                
            tables = [self.migrator_tables.decode_table_row(t) for t in tables_raw]
    
            threads = self.config_parser.get_validator_workers()
            check_counts = self.config_parser.is_validation_row_counts_enabled()
            check_table_sum = self.config_parser.is_validation_table_checksums_enabled()
            check_random = self.config_parser.is_validation_random_sample_enabled()
            check_lob = self.config_parser.is_validation_lob_sizes_enabled()
            sample_size = self.config_parser.get_validation_sample_size()
    
            results = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
                futures = []
                for t in tables:
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

        except Exception as e:
            self.val_logger.logger.error(f"Fatal error in validator module: {e}")
            self.val_logger.logger.error(traceback.format_exc())
            raise
        finally:
            self.val_logger.stop_logging()

    def validate_table(self, table_info, check_counts, check_table_sum, check_random, check_lob, sample_size):
        source_conn = self._get_connector('source')
        target_conn = self._get_connector('target')
        
        try:
            source_conn.connect()
            target_conn.connect()
            return self._validate_table_inner(source_conn, target_conn, table_info, check_counts, check_table_sum, check_random, check_lob, sample_size)
        except Exception as e:
            self.val_logger.logger.error(f"Failed to connect to databases for validating table {table_info.get('target_table_name')}: {e}")
            self.val_logger.logger.error(traceback.format_exc())
            return None
        finally:
            if getattr(source_conn, 'connection', None):
                source_conn.disconnect()
            if getattr(target_conn, 'connection', None):
                target_conn.disconnect()

    def _validate_table_inner(self, source_conn, target_conn, table_info, check_counts, check_table_sum, check_random, check_lob, sample_size):
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
        
        t_cols_raw = table_info.get('target_columns') or []
        s_cols_raw = table_info.get('source_columns') or []
        
        target_cols = list(t_cols_raw.values()) if isinstance(t_cols_raw, dict) else t_cols_raw
        source_cols = list(s_cols_raw.values()) if isinstance(s_cols_raw, dict) else s_cols_raw
        
        pk_cols = self.migrator_tables.select_primary_key({'source_schema_name': source_schema, 'source_table_name': source_table})
        if pk_cols:
            pk_cols_list = [c.strip('" ') for c in pk_cols.split(',')]
        else:
            pk_cols_list = []

        try:
            if check_counts:
                migration_limitation = None
                limitations = self.migrator_tables.get_records_data_migration_limitation(source_table)
                if limitations:
                    migration_limitation = limitations[0][0]
                
                s_count = source_conn.get_rows_count(source_schema, source_table, migration_limitation)
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
                if s_sum is not None and t_sum is not None:
                    res['table_hash_logic'] = (s_sum == t_sum)
                    if not res['table_hash_logic']:
                        res['passed'] = False
                        res['table_msg'] = f"Fail: Src={s_sum}, Tgt={t_sum}"
                    else:
                        res['table_msg'] = f"Pass: {s_sum}"
                else:
                    res['table_hash_logic'] = None
                    res['table_msg'] = f"Skip: Table checksum unavailable (Src={s_sum}, Tgt={t_sum})"

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
            self.val_logger.logger.error(traceback.format_exc())
            res['passed'] = False
            res['row_msg'] = f"Error: {e}"

        details = []
        if res.get('row_logic') is not None:
            details.append(f"Row Counts ({res.get('row_msg', '').strip()})")
        if res.get('table_hash_logic') is not None:
            details.append(f"Table Checksum ({res.get('table_msg', '').strip()})")
        if res.get('row_hash_logic') is not None:
            details.append(f"Row Level Hash ({res.get('row_hash_msg', '').strip()})")
        if res.get('lob_size_logic') is not None:
            details.append(f"LOB Size Check ({res.get('lob_size_msg', '').strip()})")
            
        if not details and res.get('row_msg', '').startswith('Error:'):
            details.append(f"Fatal Execution Details ({res.get('row_msg', '').strip()})")
            
        fail_str = ", ".join(details)
        
        if res['passed']:
            self.val_logger.logger.info(f"OK: {res['target_table']} passed all active validations against source {source_schema}.{source_table}. Details: {fail_str}")
        else:
            self.val_logger.logger.warning(f"FAIL: {res['target_table']} failed validation against source {source_schema}.{source_table}. Details: {fail_str}")

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
            self.val_logger.logger.error(traceback.format_exc())

        return res


