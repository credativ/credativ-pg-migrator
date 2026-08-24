import concurrent.futures
import time
from credativ_pg_migrator.constants import MigratorConstants
from credativ_pg_migrator.migrator_tables import MigratorTables
from credativ_pg_migrator.migrator_logging import MigratorLogger
import traceback


## The four checks a table can be measured with: the key which holds the verdict of each, the
## key which holds its message, and the name it is called by. A check which did not run has
## None where its verdict would be, and that is what tells "we could not tell" apart from "it
## is correct".
DATA_CHECKS = (
    ('row_logic', 'row_msg', 'row counts'),
    ('table_hash_logic', 'table_msg', 'table checksum'),
    ('row_hash_logic', 'row_hash_msg', 'row sample'),
    ('lob_size_logic', 'lob_size_msg', 'LOB sizes'),
)


## The structural checks: what the schema of the target holds next to what the schema of the
## source holds. They are kept apart from the four data checks above on purpose - see
## outcome_of() - because they can fail a table and they cannot pass one.
STRUCTURAL_CHECKS = (
    ('columns_logic', 'columns_msg', 'column counts'),
    ('indexes_logic', 'indexes_msg', 'index counts'),
    ('constraints_logic', 'constraints_msg', 'constraint counts'),
)


def count_is_available(count):
    """
    Whether a connector really answered with a number.

    None is "this connector does not count that", and -1 is "it tried and the query failed" -
    both of them mean the check cannot run, and neither of them may be compared as if it were
    a count of zero.
    """
    return isinstance(count, int) and not isinstance(count, bool) and count >= 0


def compare_counts(source_count, target_count, what, exact):
    """
    One structural check: `(verdict, message)`, with the verdict None when it could not run.

    `exact` says which of the two comparisons this is, and the difference matters more than it
    looks. **The number of columns must match**: a migrated table holds the columns of the
    source, and one column fewer is data which did not arrive. **The number of indexes and of
    constraints must not FALL SHORT**, and may be larger, because the two sides do not count
    the same things and were never going to:

      * PostgreSQL creates an index for every primary key and every unique constraint, and the
        migrator adds one of its own to the parent side of a foreign key which has none;
      * the SQLite connector counts a foreign key and a check as a constraint and does not
        count the primary key or a unique constraint at all, so its source number is
        systematically smaller than the target number for the same table;
      * Oracle counts indexes which have no counterpart anywhere else.

    Comparing those as equal reports a table which arrived complete as broken, which is how a
    check earns being ignored. A shortfall, on the other hand, means something the source had
    is not in the target - and that is the one thing these numbers can say honestly.
    """
    if not count_is_available(source_count) or not count_is_available(target_count):
        return None, f"Skip: {what} not available on both sides (Src={source_count}, Tgt={target_count})"
    if exact:
        if source_count == target_count:
            return True, f"Pass: {source_count} {what}"
        return False, (f"Fail: Src={source_count}, Tgt={target_count} - the target does not "
                       f"hold the same {what} as the source")
    if target_count >= source_count:
        return True, f"Pass: Src={source_count}, Tgt={target_count}"
    return False, (f"Fail: Src={source_count}, Tgt={target_count} - {source_count - target_count} "
                   f"of the {what} of the source are NOT in the target. Which of them is in the "
                   f"protocol table of the objects, with the reason for each")


def outcome_of(res):
    """
    What the validation of one table ended in, derived from the checks which really ran.

    It used to be accumulated instead: `passed` started at **True** and the branches which
    found a mismatch set it to False. A table where no branch ran at all - no primary key, so
    no row sample and no LOB check; no checksum on that source; the checks switched off in the
    configuration - therefore ended the run reported exactly like a table which passed every
    one of them, and the line in the log said "passed all active validations", which was true
    and told the reader the opposite of what had happened. P2-2 of
    development/OPEN_ISSUES.md.

    Three outcomes:

      * FAILED - a check ran and said no, or the validation of the table crashed.
      * PASSED - at least one check of the data ran and every check which ran said yes.
      * NOT VALIDATED - nothing looked at the data. It is not a failure of the migration and it
        is not evidence of one either, and it has to be visible as itself: a green report
        which is green because nobody looked is the one thing a validator must not produce.

    The structural checks are asymmetric here, and deliberately so: **they can fail a table and
    they cannot pass one.** A table which arrived with half its indexes has not been validated
    whatever its row count says, so a structural mismatch fails it (P2-3); but the number of
    columns matching says nothing about whether the rows arrived, so it must not turn a table
    nothing looked into a table which passed (P2-2). One rule cannot be relaxed to make room
    for the other.
    """
    if res.get('error'):
        return MigratorConstants.VALIDATION_FAILED
    data = [res.get(verdict) for verdict, _, _ in DATA_CHECKS]
    structural = [res.get(verdict) for verdict, _, _ in STRUCTURAL_CHECKS]
    if any(verdict is False for verdict in data + structural):
        return MigratorConstants.VALIDATION_FAILED
    if any(verdict is True for verdict in data):
        return MigratorConstants.VALIDATION_PASSED
    return MigratorConstants.VALIDATION_NOT_VALIDATED


def checks_which_ran(res):
    """The names of the checks which produced a verdict, in the order they run."""
    return [name for verdict, _, name in DATA_CHECKS + STRUCTURAL_CHECKS
            if res.get(verdict) is not None]


def why_nothing_ran(res, requested):
    """
    Why a table could not be measured, per check - what a NOT VALIDATED outcome has to say.

    `requested` maps the name of a check to whether the configuration asked for it at all.
    A check which was asked for and produced nothing says why (it has written its own 'Skip:'
    message); one which was not asked for says so.
    """
    reasons = []
    for verdict, message, name in DATA_CHECKS:
        if res.get(verdict) is not None:
            continue
        if not requested.get(name, True):
            reasons.append(f"{name}: switched off in the configuration")
            continue
        written = str(res.get(message) or '').strip()
        reasons.append(f"{name}: {written}" if written else f"{name}: no result")
    return reasons


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
        
        report_filename = self.config_parser.get_validation_report_filename()
        if not report_filename:
            self.val_logger.logger.error("FATAL: 'report_filename' is missing in validator config. A detailed report file is mandatory.")
            return
        
        try:
            self.migrator_tables.create_table_for_validation()
    
            if self.config_parser.get_workflow() == 'mapping':
                tables = self.migrator_tables.fetch_mapping_tables_for_validation()
            else:
                tables_raw = self.migrator_tables.fetch_all_tables(only_unfinished=False)
                tables = [self.migrator_tables.decode_table_row(t) for t in tables_raw]
            
            if not tables:
                self.val_logger.logger.info("No tables found in migrator tracking to validate.")
                return
    
            threads = self.config_parser.get_validation_workers()
            check_counts = self.config_parser.is_validation_row_counts_enabled()
            check_table_sum = self.config_parser.is_validation_table_checksums_enabled()
            check_random = self.config_parser.is_validation_random_sample_enabled()
            check_lob = self.config_parser.is_validation_lob_sizes_enabled()
            sample_size = self.config_parser.get_validation_sample_size()
    
            results = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
                ## which table each worker was given, so that one which ends in an exception
                ## can still be named and still be written into the report. It was a list, so
                ## the error below could only say "Error validating table" and the table was
                ## gone from the report and from the count at the bottom of it. P2-4.
                futures = {}
                for t in tables:
                    if self.config_parser.get_workflow() == 'mapping' or t.get('target_table_rows', 0) > 0 or t.get('source_table_rows', 0) > 0:
                        future = executor.submit(
                            self.validate_table, 
                            t, check_counts, check_table_sum, check_random, check_lob, sample_size
                        )
                        futures[future] = t
                
                for future in concurrent.futures.as_completed(futures):
                    table_info = futures[future]
                    try:
                        res = future.result()
                        if res:
                            results.append(res)
                        else:
                            ## nothing came back at all - the table still belongs in the report
                            results.append(self.could_not_be_validated(
                                table_info, 'the validation returned nothing',
                                'the validation of this table ended without a result'))
                    except Exception as e:
                        self.val_logger.logger.error(
                            f"Error validating table {table_info.get('target_schema_name')}."
                            f"{table_info.get('target_table_name')}: {e}")
                        self.val_logger.logger.error(traceback.format_exc())
                        results.append(self.could_not_be_validated(
                            table_info, e, 'the validation of this table ended in an error'))
    
            self.migrator_tables.print_validation_summary(val_logger=self.val_logger.logger)

        except Exception as e:
            self.val_logger.logger.error(f"Fatal error in validator module: {e}")
            self.val_logger.logger.error(traceback.format_exc())
            raise
        finally:
            self.val_logger.stop_logging()

    def could_not_be_validated(self, table_info, error, what_happened):
        """
        The row a table gets when the validator could not measure it at all.

        It used to get none. `validate_table()` answered a table whose connection failed with
        `None`, and `run()` dropped a falsy result - so the table was **missing from the
        validation protocol table**, missing from the report built out of it, and missing from
        the count at the bottom of that report. What the reader saw was a report of the tables
        which happened to work, all green, and a total which did not say how many tables the
        validation had really been asked about. Together with the outcome which started at
        True (P2-2), that is why a green validation report could not be used as evidence.
        P2-4 of development/OPEN_ISSUES.md.

        The outcome is FAILED and not NOT VALIDATED, and the difference is deliberate:
        NOT VALIDATED means the checks do not apply to this table - no primary key, no
        checksum on that source - which is an ordinary state of an ordinary migration. An
        exception is not an ordinary state. The message says which of the two it is, so that
        the red row cannot be read as "this table is broken" when what broke was the
        validation of it.
        """
        source_schema = table_info.get('source_schema_name')
        source_table = table_info.get('source_table_name')
        target_schema = table_info.get('target_schema_name')
        target_table = table_info.get('target_table_name')
        message = (f"{what_happened}: {error}. This is a failure of the VALIDATION and not a "
                   f"measurement of the table - nothing about it was compared, so the run "
                   f"says nothing about whether it is correct.")
        res = {
            'target_table': f"{target_schema}.{target_table}",
            'source_schema_name': source_schema,
            'source_table_name': source_table,
            'target_schema_name': target_schema,
            'target_table_name': target_table,
            'outcome': MigratorConstants.VALIDATION_FAILED,
            'error': str(error),
            'checks_run': [],
            'validation_message': message,
        }
        self.val_logger.logger.error(f"FAILED: {res['target_table']} - {message}")
        try:
            self.migrator_tables.insert_validation_table_result(res)
        except Exception as e:
            ## the last place which could have recorded the table - if this fails too, the
            ## report really is short of a row and the log has to say so in as many words
            self.val_logger.logger.error(
                f"Error persisting validation protocol for {res['target_table']}, which "
                f"could not be validated: {e}. The table is MISSING from the validation "
                f"report.")
            self.val_logger.logger.error(traceback.format_exc())
        return res

    def validate_table(self, table_info, check_counts, check_table_sum, check_random, check_lob, sample_size):
        source_conn = None
        target_conn = None
        target_copy_conn = None
        try:
            source_conn = self._get_connector('source')
            target_conn = self._get_connector('target')
            if self.config_parser.get_workflow() == 'mapping':
                target_copy_conn = self._get_connector('target_copy')
        except Exception as e:
            ## building a connector raised - it used to happen outside every try in this
            ## method, so the exception travelled up into run(), which logged it without
            ## naming the table and wrote nothing anywhere
            self.val_logger.logger.error(traceback.format_exc())
            return self.could_not_be_validated(
                table_info, e, 'the connectors for the validation could not be built')

        try:
            source_conn.connect()
            target_conn.connect()
            if target_copy_conn:
                target_copy_conn.connect()
            return self._validate_table_inner(source_conn, target_conn, target_copy_conn, table_info, check_counts, check_table_sum, check_random, check_lob, sample_size)
        except Exception as e:
            self.val_logger.logger.error(f"Failed to connect to databases for validating table {table_info.get('target_table_name')}: {e}")
            self.val_logger.logger.error(traceback.format_exc())
            return self.could_not_be_validated(
                table_info, e, 'the databases could not be reached for this table')
        finally:
            if getattr(source_conn, 'connection', None):
                source_conn.disconnect()
            if getattr(target_conn, 'connection', None):
                target_conn.disconnect()
            if target_copy_conn and getattr(target_copy_conn, 'connection', None):
                target_copy_conn.disconnect()

    def _validate_table_inner(self, source_conn, target_conn, target_copy_conn, table_info, check_counts, check_table_sum, check_random, check_lob, sample_size):
        source_schema = table_info['source_schema_name']
        source_table = table_info['source_table_name']
        target_schema = table_info['target_schema_name']
        target_table = table_info['target_table_name']

        res = {
            'target_table': f"{target_schema}.{target_table}",
            'source_schema_name': source_schema,
            'source_table_name': source_table,
            'target_schema_name': target_schema,
            'target_table_name': target_table,
            'source_row_count': None,
            'target_row_count': None,
            'source_table_hash': None,
            'target_table_hash': None,
            'source_columns_count': None,
            'target_columns_count': None,
            'source_indexes_count': None,
            'target_indexes_count': None,
            'source_constraints_count': None,
            'target_constraints_count': None,
            'row_logic': None,
            'row_msg': '',
            'table_hash_logic': None,
            'table_msg': '',
            'row_hash_logic': None,
            'row_hash_msg': '',
            'lob_size_logic': None,
            'lob_size_msg': '',
            ## The structural checks. They were filled into the four *_count keys above and
            ## compared by nothing - a table which arrived with half its indexes was reported
            ## as validated. P2-3.
            'columns_logic': None,
            'columns_msg': '',
            'indexes_logic': None,
            'indexes_msg': '',
            'constraints_logic': None,
            'constraints_msg': '',
            ## Derived at the end from the seven verdicts above and never accumulated - see
            ## outcome_of(), and note that the three structural ones can only fail a table.
            ## 'error' holds the exception when the validation of this table crashed, which
            ## is a failure and not an absence of one.
            'outcome': None,
            'error': '',
            'checks_run': [],
            'validation_message': '',
        }
        
        self.val_logger.logger.info(f"Validating {res['target_table']} ...")
        
        t_cols_raw = table_info.get('target_columns') or []
        s_cols_raw = table_info.get('source_columns') or []
        
        target_cols = list(t_cols_raw.values()) if isinstance(t_cols_raw, dict) else t_cols_raw
        source_cols = list(s_cols_raw.values()) if isinstance(s_cols_raw, dict) else s_cols_raw
        
        res['source_columns_count'] = len(source_cols)
        res['target_columns_count'] = len(target_cols)
        
        try:
            res['source_indexes_count'] = source_conn.get_indexes_count(source_schema, source_table) if hasattr(source_conn, 'get_indexes_count') else 0
            res['target_indexes_count'] = target_conn.get_indexes_count(target_schema, target_table) if hasattr(target_conn, 'get_indexes_count') else 0
            
            res['source_constraints_count'] = source_conn.get_constraints_count(source_schema, source_table) if hasattr(source_conn, 'get_constraints_count') else 0
            res['target_constraints_count'] = target_conn.get_constraints_count(target_schema, target_table) if hasattr(target_conn, 'get_constraints_count') else 0
        except Exception as e:
            self.val_logger.logger.error(f"Error fetching structural validation metadata for {target_schema}.{target_table}: {e}")

        ## The four numbers above used to be recorded and compared by nothing at all, so a
        ## table which arrived with half its indexes was reported as validated. They are
        ## checks now - see compare_counts() for why the columns are compared exactly and the
        ## indexes and constraints only for a shortfall.
        res['columns_logic'], res['columns_msg'] = compare_counts(
            res['source_columns_count'], res['target_columns_count'], 'columns', exact=True)

        ## An object the configuration asked not to migrate is missing from the target on
        ## purpose, and a check which fails a table for doing what it was told is a check
        ## nobody will keep.
        if self.config_parser.should_migrate_indexes(source_table):
            res['indexes_logic'], res['indexes_msg'] = compare_counts(
                res['source_indexes_count'], res['target_indexes_count'], 'indexes', exact=False)
        else:
            res['indexes_msg'] = 'Skip: indexes are not migrated for this table'

        if self.config_parser.should_migrate_constraints(source_table):
            res['constraints_logic'], res['constraints_msg'] = compare_counts(
                res['source_constraints_count'], res['target_constraints_count'],
                'constraints', exact=False)
        else:
            res['constraints_msg'] = 'Skip: constraints are not migrated for this table'
            
        pk_cols = self.migrator_tables.select_primary_key({'source_schema_name': source_schema, 'source_table_name': source_table})
        if pk_cols:
            pk_cols_list = [c.strip('" ') for c in pk_cols.split(',')]
        else:
            pk_cols_list = []

        try:
            target_copy_schema = None
            if target_copy_conn:
                target_copy_config = self.config_parser.get_validation_target_copy_config()
                target_copy_schema = target_copy_config.get('schema', target_copy_config.get('owner', 'public'))

            for s_col, t_col in zip(source_cols, target_cols):
                s_type = s_col.get('data_type', '').lower()
                is_num = any(t in s_type for t in ['int', 'number', 'numeric', 'decimal', 'float', 'double', 'real', 'serial'])
                if is_num and s_col.get('numeric_precision') == 0:
                    s_col['_force_round_0'] = True
                    t_col['_force_round_0'] = True
                    
            t_copy_count = 0
            action = None

            if check_counts:
                ## the whole count first - a restriction carrying a row limit applies only to a
                ## table which exceeds it, and the count of the source has to be measured the
                ## same way the migration measured it
                s_count_unlimited = source_conn.get_rows_count(source_schema, source_table, None)
                migration_limitation = self.migrator_tables.resolve_data_migration_limitation({
                    'source_schema_name': source_schema,
                    'source_table_name': source_table,
                    'source_columns': source_cols,
                    'source_table_rows_all': s_count_unlimited,
                })

                if migration_limitation:
                    s_count = source_conn.get_rows_count(source_schema, source_table, migration_limitation)
                else:
                    s_count = s_count_unlimited
                t_count = target_conn.get_rows_count(target_schema, target_table)
                
                if target_copy_conn:
                    t_copy_count = target_copy_conn.get_rows_count(target_copy_schema, target_table)
                    if t_copy_count > 0:
                        action = self.config_parser.get_mapping_data_resolution(source_table)

                res['source_row_count'] = s_count
                res['target_row_count'] = t_count

                if not action or action == 'replace':
                    res['row_logic'] = (s_count == t_count)
                    if not res['row_logic']:
                        res['row_msg'] = f"Fail: Src={s_count}, Tgt={t_count}"
                    else:
                        res['row_msg'] = f"Pass: {s_count} rows"
                elif action == 'skip':
                    res['row_logic'] = (t_copy_count == t_count)
                    if not res['row_logic']:
                        res['row_msg'] = f"Fail (skip): OrigTgt={t_copy_count}, Tgt={t_count}"
                    else:
                        res['row_msg'] = f"Pass (skip): {t_count} rows untouched"
                elif action in ('merge_keep_target', 'merge_keep_source'):
                    min_rows = max(t_copy_count, s_count)
                    max_rows = t_copy_count + s_count
                    res['row_logic'] = (min_rows <= t_count <= max_rows)
                    if not res['row_logic']:
                        res['row_msg'] = f"Fail (merge): OrigTgt={t_copy_count}, Src={s_count}, Tgt={t_count} bounds [{min_rows}, {max_rows}]"
                    else:
                        res['row_msg'] = f"Pass (merge bounds): {t_count} rows"

            if check_table_sum:
                if action in ('merge_keep_target', 'merge_keep_source'):
                    res['table_hash_logic'] = None
                    res['table_msg'] = "Skip: Table checksum not supported for merged tables"
                else:
                    if action == 'skip':
                        s_sum = target_copy_conn.get_table_checksum(target_copy_schema, target_table, target_cols)
                        t_sum = target_conn.get_table_checksum(target_schema, target_table, target_cols)
                        conn_s = target_copy_conn
                        schema_s = target_copy_schema
                        cols_s = target_cols
                        table_s = target_table
                    else:
                        s_sum = source_conn.get_table_checksum(source_schema, source_table, source_cols)
                        t_sum = target_conn.get_table_checksum(target_schema, target_table, target_cols)
                        conn_s = source_conn
                        schema_s = source_schema
                        cols_s = source_cols
                        table_s = source_table

                    res['source_table_hash'] = s_sum
                    res['target_table_hash'] = t_sum
                    if s_sum is not None and t_sum is not None:
                        res['table_hash_logic'] = (s_sum == t_sum)
                        if not res['table_hash_logic']:
                            res['table_msg'] = f"Fail: Src={s_sum}, Tgt={t_sum}"
                            
                            self.val_logger.logger.warning(f"Validator: Table {source_table} hash mismatch. Inspecting columns...")
                            for i in range(min(len(cols_s), len(target_cols))):
                                s_col = [cols_s[i]]
                                t_col = [target_cols[i]]
                                s_col_sum = conn_s.get_table_checksum(schema_s, table_s, s_col)
                                t_col_sum = target_conn.get_table_checksum(target_schema, target_table, t_col)
                                
                                col_passed = (s_col_sum == t_col_sum)
                                if (s_count == 0 and t_count != 0) or (t_count == 0 and s_count != 0):
                                    col_passed = False
                                
                                s_prec = cols_s[i].get('numeric_precision')
                                t_prec = target_cols[i].get('numeric_precision')
                                force_round = cols_s[i].get('_force_round_0', False)
                                
                                s_stats = conn_s.get_column_statistics(schema_s, table_s, cols_s[i]['column_name'], cols_s[i].get('data_type', ''), force_round_0=force_round)
                                t_stats = target_conn.get_column_statistics(target_schema, target_table, target_cols[i]['column_name'], target_cols[i].get('data_type', ''), force_round_0=force_round)
                                
                                col_res = {
                                    'source_schema_name': source_schema,
                                    'source_table_name': source_table,
                                    'source_column_name': cols_s[i]['column_name'],
                                    'target_schema_name': target_schema,
                                    'target_table_name': target_table,
                                    'target_column_name': target_cols[i]['column_name'],
                                    'source_data_type': cols_s[i].get('data_type', ''),
                                    'target_data_type': target_cols[i].get('data_type', ''),
                                    'source_precision': s_prec,
                                    'target_precision': t_prec,
                                    'source_hash': s_col_sum,
                                    'target_hash': t_col_sum,
                                    'source_null_count': s_stats.get('null_count'),
                                    'target_null_count': t_stats.get('null_count'),
                                    'source_empty_string_count': s_stats.get('empty_string_count'),
                                    'target_empty_string_count': t_stats.get('empty_string_count'),
                                    'source_min_value': s_stats.get('min_value'),
                                    'target_min_value': t_stats.get('min_value'),
                                    'source_max_value': s_stats.get('max_value'),
                                    'target_max_value': t_stats.get('max_value'),
                                    'source_avg_value': s_stats.get('avg_value'),
                                    'target_avg_value': t_stats.get('avg_value'),
                                    'source_row_count': s_count,
                                    'target_row_count': t_count,
                                    'passed': col_passed
                                }
                                
                                try:
                                    self.migrator_tables.insert_validation_column_result(col_res)
                                except Exception as e:
                                    self.val_logger.logger.error(f"Error persisting column validation protocol for {target_table}.{cols_s[i]['column_name']}: {e}")
                                    
                                if not col_passed:
                                    if s_col_sum != t_col_sum:
                                        self.val_logger.logger.warning(f"Validator: Table {source_table} column {cols_s[i]['column_name']} hash mismatch: Src={s_col_sum}, Tgt={t_col_sum}")
                                    else:
                                        self.val_logger.logger.warning(f"Validator: Table {source_table} column {cols_s[i]['column_name']} row count mismatch: Src={s_count}, Tgt={t_count}")
                        else:
                            res['table_msg'] = f"Pass: {s_sum}"
                    else:
                        res['table_hash_logic'] = None
                        res['table_msg'] = f"Skip: Table checksum unavailable (Src={s_sum}, Tgt={t_sum})"

            if check_random and pk_cols_list:
                if action in ('merge_keep_target', 'merge_keep_source'):
                    res['row_hash_logic'] = None
                    res['row_hash_msg'] = "Skip: Random sample not supported for merged tables"
                else:
                    pks = target_conn.get_random_pks(target_schema, target_table, pk_cols_list, sample_size)
                    if pks:
                        if action == 'skip':
                            s_row_sums = target_copy_conn.get_row_checksums(target_copy_schema, target_table, pk_cols_list, pks, target_cols)
                        else:
                            s_row_sums = source_conn.get_row_checksums(source_schema, source_table, pk_cols_list, pks, source_cols)
                        t_row_sums = target_conn.get_row_checksums(target_schema, target_table, pk_cols_list, pks, target_cols)
                        
                        mismatches = 0
                        for pk_val, t_hash in t_row_sums.items():
                            if s_row_sums.get(pk_val) != t_hash:
                                mismatches += 1
                        
                        res['row_hash_logic'] = (mismatches == 0)
                        if mismatches > 0:
                            res['row_hash_msg'] = f"Fail: {mismatches}/{len(pks)} sample rows mismatched"
                        else:
                            res['row_hash_msg'] = f"Pass: {len(pks)} samples matched"
                    else:
                        res['row_hash_msg'] = "Skip: No samples fetched"
            elif check_random and not pk_cols_list:
                res['row_hash_msg'] = "Skip: No PKs available"

            if check_lob and pk_cols_list:
                if action in ('merge_keep_target', 'merge_keep_source'):
                    res['lob_size_logic'] = None
                    res['lob_size_msg'] = "Skip: LOB size check not supported for merged tables"
                else:
                    if action == 'skip':
                        s_lobs = [c for c in target_cols if any(x in c.get('data_type', '').lower() for x in ['lob', 'text', 'bytea', 'image', 'xml', 'json'])]
                        conn_s = target_copy_conn
                        schema_s = target_copy_schema
                        table_s = target_table
                    else:
                        s_lobs = [c for c in source_cols if any(x in c.get('data_type', '').lower() for x in ['lob', 'text', 'bytea', 'image', 'xml', 'json'])]
                        conn_s = source_conn
                        schema_s = source_schema
                        table_s = source_table
                    
                    t_lobs = [c for c in target_cols if any(x in c.get('data_type', '').lower() for x in ['lob', 'text', 'bytea', 'image', 'xml', 'json'])]
                    if s_lobs and t_lobs and len(s_lobs) == len(t_lobs):
                        pks = target_conn.get_random_pks(target_schema, target_table, pk_cols_list, sample_size)
                        if pks:
                            s_lob_sizes = conn_s.get_lob_sizes(schema_s, table_s, pk_cols_list, pks, s_lobs)
                            t_lob_sizes = target_conn.get_lob_sizes(target_schema, target_table, pk_cols_list, pks, t_lobs)
                            
                            mismatches = 0
                            for pk_val, t_sizes in t_lob_sizes.items():
                                s_sizes = s_lob_sizes.get(pk_val)
                                if s_sizes != t_sizes:
                                    mismatches += 1
                                    
                            res['lob_size_logic'] = (mismatches == 0)
                            if mismatches > 0:
                                res['lob_size_msg'] = f"Fail: {mismatches}/{len(pks)} sample LOB sizes mismatched"
                            else:
                                res['lob_size_msg'] = f"Pass: {len(pks)} samples matched"
                        else:
                            res['lob_size_msg'] = "Skip: No samples fetched"
                    else:
                        res['lob_size_msg'] = "Skip: No matching LOB columns identified"
            elif check_lob and not pk_cols_list:
                res['lob_size_msg'] = "Skip: No PKs available"

            # Index Validation
            try:
                source_indexes_raw = source_conn.fetch_indexes({'source_table_schema': source_schema, 'source_table_name': source_table, 'source_table_id': None}) if hasattr(source_conn, 'fetch_indexes') else {}
                source_indexes = list(source_indexes_raw.values()) if isinstance(source_indexes_raw, dict) else source_indexes_raw
                
                target_indexes = []
                try:
                    if hasattr(target_conn, 'fetch_mapping_target_indexes'):
                        target_indexes = target_conn.fetch_mapping_target_indexes(target_schema, target_table)
                except Exception as e:
                    self.val_logger.logger.error(f"Error calling fetch_mapping_target_indexes: {e}")
                
                # Match indexes by columns or primary key status
                matched_targets = set()
                
                for s_idx in source_indexes:
                    s_name = s_idx.get('index_name')
                    s_type = s_idx.get('index_type', '')
                    s_cols = s_idx.get('index_columns', '').replace('"', '').lower()
                    is_s_pk = 'PRIMARY' in s_type.upper()
                    
                    best_match = None
                    for t_idx in target_indexes:
                        t_name = t_idx.get('index_name')
                        if t_name in matched_targets:
                            continue
                            
                        is_t_pk = t_idx.get('is_primary_key', False)
                        t_def = (t_idx.get('index_def') or t_idx.get('index_columns') or '').lower()
                        
                        if is_s_pk and is_t_pk:
                            best_match = t_idx
                            break
                        
                        if s_cols and s_cols in t_def:
                            best_match = t_idx
                            break
                            
                    if best_match:
                        matched_targets.add(best_match.get('index_name'))
                        t_idx = best_match
                    else:
                        t_idx = {}
                        
                    idx_res = {
                        'source_schema_name': source_schema,
                        'source_table_name': source_table,
                        'source_index_name': s_idx.get('index_name'),
                        'target_schema_name': target_schema,
                        'target_table_name': target_table,
                        'target_index_name': t_idx.get('index_name'),
                        'source_index_type': s_idx.get('index_type'),
                        'target_index_type': t_idx.get('index_type'),
                        'source_index_columns': s_idx.get('index_columns'),
                        'target_index_columns': t_idx.get('index_def') or t_idx.get('index_columns'),
                        'passed': best_match is not None
                    }
                    self.migrator_tables.insert_validation_index_result(idx_res)
                    
                # Add any unmatched target indexes
                for t_idx in target_indexes:
                    if t_idx.get('index_name') not in matched_targets:
                        idx_res = {
                            'source_schema_name': source_schema,
                            'source_table_name': source_table,
                            'source_index_name': None,
                            'target_schema_name': target_schema,
                            'target_table_name': target_table,
                            'target_index_name': t_idx.get('index_name'),
                            'source_index_type': None,
                            'target_index_type': t_idx.get('index_type'),
                            'source_index_columns': None,
                            'target_index_columns': t_idx.get('index_def') or t_idx.get('index_columns'),
                            'passed': False
                        }
                        self.migrator_tables.insert_validation_index_result(idx_res)
            except Exception as e:
                self.val_logger.logger.error(f"Error validating indexes for {target_table}: {e}")

            # Constraint Validation
            try:
                source_constraints_raw = source_conn.fetch_constraints({'source_table_schema': source_schema, 'source_table_name': source_table, 'source_table_id': None}) if hasattr(source_conn, 'fetch_constraints') else {}
                source_constraints = list(source_constraints_raw.values()) if isinstance(source_constraints_raw, dict) else source_constraints_raw
                
                target_constraints = []
                try:
                    if hasattr(target_conn, 'fetch_mapping_target_constraints'):
                        target_constraints_raw = target_conn.fetch_mapping_target_constraints(target_schema, target_table)
                        target_constraints = [c for c in target_constraints_raw if c.get('constraint_type') not in ('PRIMARY KEY', 'UNIQUE')]
                except Exception as e:
                    self.val_logger.logger.error(f"Error calling fetch_mapping_target_constraints: {e}")
                
                # Match constraints by columns or primary key status
                matched_targets = set()
                
                for s_con in source_constraints:
                    s_type = s_con.get('constraint_type', '')
                    s_cols = s_con.get('constraint_columns', '').replace('"', '').lower()
                    is_s_pk = 'P' in s_type.upper() or 'PRIMARY' in s_type.upper()
                    
                    best_match = None
                    for t_con in target_constraints:
                        t_name = t_con.get('constraint_name')
                        if t_name in matched_targets:
                            continue
                            
                        t_type = t_con.get('constraint_type', '')
                        is_t_pk = 'P' in t_type.upper() or 'PRIMARY' in t_type.upper()
                        t_cols = (t_con.get('constraint_def') or t_con.get('constraint_sql') or t_con.get('constraint_columns') or '').lower()
                        
                        if is_s_pk and is_t_pk:
                            best_match = t_con
                            break
                            
                        if s_cols and s_cols in t_cols:
                            best_match = t_con
                            break
                            
                    if best_match:
                        matched_targets.add(best_match.get('constraint_name'))
                        t_con = best_match
                    else:
                        t_con = {}
                        
                    con_res = {
                        'source_schema_name': source_schema,
                        'source_table_name': source_table,
                        'source_constraint_name': s_con.get('constraint_name'),
                        'target_schema_name': target_schema,
                        'target_table_name': target_table,
                        'target_constraint_name': t_con.get('constraint_name'),
                        'source_constraint_type': s_con.get('constraint_type'),
                        'target_constraint_type': t_con.get('constraint_type'),
                        'source_constraint_columns': s_con.get('constraint_columns'),
                        'target_constraint_columns': t_con.get('constraint_def') or t_con.get('constraint_sql') or t_con.get('constraint_columns'),
                        'passed': best_match is not None
                    }
                    self.migrator_tables.insert_validation_constraint_result(con_res)
                    
                # Add any unmatched target constraints
                for t_con in target_constraints:
                    if t_con.get('constraint_name') not in matched_targets:
                        con_res = {
                            'source_schema_name': source_schema,
                            'source_table_name': source_table,
                            'source_constraint_name': None,
                            'target_schema_name': target_schema,
                            'target_table_name': target_table,
                            'target_constraint_name': t_con.get('constraint_name'),
                            'source_constraint_type': None,
                            'target_constraint_type': t_con.get('constraint_type'),
                            'source_constraint_columns': None,
                            'target_constraint_columns': t_con.get('constraint_def') or t_con.get('constraint_sql') or t_con.get('constraint_columns'),
                            'passed': False
                        }
                        self.migrator_tables.insert_validation_constraint_result(con_res)
            except Exception as e:
                self.val_logger.logger.error(f"Error validating constraints for {target_table}: {e}")


        except Exception as e:
            self.val_logger.logger.error(f"Validation crash on {res['target_table']}: {e}")
            self.val_logger.logger.error(traceback.format_exc())
            ## A table whose validation crashed has not been validated and has not passed
            ## either - it is a failure, and it is recorded as one rather than as an absence.
            res['error'] = str(e)
            res['row_msg'] = f"Error: {e}"

        details = []
        for verdict, message, name in DATA_CHECKS + STRUCTURAL_CHECKS:
            if res.get(verdict) is not None:
                details.append(f"{name} ({str(res.get(message) or '').strip()})")

        if res.get('error'):
            details.append(f"validation crashed ({res['error']})")

        res['outcome'] = outcome_of(res)
        res['checks_run'] = checks_which_ran(res)
        requested = {
            'row counts': check_counts,
            'table checksum': check_table_sum,
            'row sample': check_random,
            'LOB sizes': check_lob,
        }

        if res['outcome'] == MigratorConstants.VALIDATION_PASSED:
            res['validation_message'] = ", ".join(details)
            self.val_logger.logger.info(
                f"PASSED: {res['target_table']} passed the {len(res['checks_run'])} check(s) "
                f"which could be run against source {source_schema}.{source_table} "
                f"({', '.join(res['checks_run'])}). Details: {res['validation_message']}")
        elif res['outcome'] == MigratorConstants.VALIDATION_FAILED:
            res['validation_message'] = ", ".join(details)
            self.val_logger.logger.warning(
                f"FAILED: {res['target_table']} failed validation against source "
                f"{source_schema}.{source_table}. Details: {res['validation_message']}")
        else:
            ## Nothing could be measured. This used to be reported as "passed all active
            ## validations", which is the sentence P2-2 is about.
            reasons = why_nothing_ran(res, requested)
            structural = [name for verdict, _, name in STRUCTURAL_CHECKS
                          if res.get(verdict) is not None]
            if structural:
                ## The structure was compared and matched. It is not nothing, and it is not
                ## evidence that the rows arrived either - so the table is still not validated
                ## and the line says which of the two happened.
                reasons.append(f"the structure was compared ({', '.join(structural)}) and "
                               f"matched, but nothing looked at the data")
            res['validation_message'] = "; ".join(reasons)
            self.val_logger.logger.warning(
                f"NOT VALIDATED: {res['target_table']} - not one check of the DATA could be "
                f"run against source {source_schema}.{source_table}, so this run says NOTHING "
                f"about whether the rows are correct. It is not a table which passed. Why: "
                f"{res['validation_message']}")

        try:
            self.migrator_tables.insert_validation_table_result(res)
        except Exception as e:
            ## the table was measured and the measurement could not be written down, so the
            ## report is short of a row - said in as many words, because a report which is
            ## missing a table looks exactly like a report of a migration with fewer tables
            self.val_logger.logger.error(
                f"Error persisting validation protocol for {res['target_table']}: {e}. The "
                f"table is MISSING from the validation report.")
            self.val_logger.logger.error(traceback.format_exc())

        return res


