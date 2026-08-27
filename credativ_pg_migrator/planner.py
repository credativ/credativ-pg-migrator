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

import os
import importlib
from credativ_pg_migrator.migrator_logging import MigratorLogger
from credativ_pg_migrator import identifier_case
from credativ_pg_migrator import partitioning
from credativ_pg_migrator.migrator_tables import MigratorTables
from credativ_pg_migrator.constants import MigratorConstants
import fnmatch
import traceback
import re
import json

class Planner:
    def __init__(self, config_parser):
        self.config_parser = config_parser
        self.source_db_config = self.config_parser.get_source_config()
        self.logger = MigratorLogger(self.config_parser.get_log_file()).logger
        # self.config_parser.print_log_message('DEBUG3', f"planner: __init__: Loading connectors...")
        self.source_connection = self.load_connector('source')
        self.target_connection = self.load_connector('target')
        self.migrator_tables = MigratorTables(self.logger, self.config_parser)
        self.on_error_action = self.config_parser.get_on_error_action()
        self.source_schema_name = self.config_parser.get_source_schema()
        self.target_schema_name = self.config_parser.get_target_schema()
        self.pre_script = self.config_parser.get_pre_migration_script()
        self.post_script = self.config_parser.get_post_migration_script()
        self.user_defined_types = {}
        # source collation name -> collation recreated in the target schema,
        # filled by stdwf_prepare_collations and used when the DDL is generated
        self.migrated_collations = {}
        # source text search object name -> object recreated in the target schema,
        # filled by stdwf_prepare_text_search and used when the DDL is generated
        self.migrated_text_search = {}
        # What happens to every partitioned table of this migration - read once, by the
        # pre-migration analysis or by stdwf_prepare_tables, whichever asks first. The two
        # must not read the source twice: a report and a run which disagree are worse than
        # neither.
        self.partitioning_plan = None
        self.partitioning_table_ids = {}
        # why the partitioning of this source is not reported, when it is not
        self.partitioning_note = ''
        # and which of the two reasons it is: the source HAS none, or this run cannot see it
        self.partitioning_is_absent = False
        self.sql_functions_mapping = self.source_connection.get_sql_functions_mapping({
            'target_db_type': self.config_parser.get_target_db_type()
        })

    def create_plan(self):
        if self.config_parser.is_resume_after_crash():
            self.migrator_tables.insert_main({'task_name': 'Planner', 'subtask_name': 'Resume after crash'})
            self.config_parser.print_log_message('INFO', "planner: create_plan: Resuming migration after crash...")
            self.config_parser.print_log_message('INFO', "planner: create_plan: In current version of crash recovery, we skip planner phase, assuming all protocol tables already exist.")

            self.config_parser.print_log_message( 'INFO', "planner: create_plan: Connecting to source and target databases...")
            self.check_database_connection(self.source_connection, "Source Database")
            self.check_database_connection(self.target_connection, "Target Database")

            self.run_check_tables_migration_status()

            self.migrator_tables.update_main_status({'task_name': 'Planner', 'subtask_name': 'Resume after crash', 'success': True, 'message': 'finished OK'})
        else:

            self.pre_planning()

            self.check_pausing_resuming()

            self.run_premigration_analysis()

            self.check_pausing_resuming()

            ## The row of the planner as a whole, opened by pre_planning(). Every branch below
            ## used to close THIS row instead of the phase row it had opened itself, so the
            ## phase of the workflow was never closed at all - no duration, no result, forever
            ## - and this one was closed with 'finished OK' whatever the branch had done. P2-7.
            planning_failed = False

            if self.config_parser.is_standard_workflow():
                self.migrator_tables.insert_main({'task_name': 'Planner', 'subtask_name': 'Standard workflow'})
                try:

                    if self.source_db_config['connectivity'] == 'ddl':
                        self.config_parser.print_log_message('DEBUG3', f"planner: create_plan: starting ddl connectivity")
                        self.source_connection.parse_ddl_files({ 'migrator_tables': self.migrator_tables})
                        self.source_schema_name = self.config_parser.get_source_schema()

                    self.stdwf_prepare_collations()
                    self.stdwf_prepare_text_search()
                    self.stdwf_prepare_domains()
                    self.stdwf_prepare_user_defined_types()
                    self.stdwf_prepare_defaults()

                    self.check_pausing_resuming()

                    self.stdwf_prepare_aliases()
                    self.stdwf_prepare_sequences()
                    self.stdwf_prepare_tables()
                    self.stdwf_prepare_data_sources()

                    self.check_pausing_resuming()

                    self.stdwf_prepare_views()

                    self.check_pausing_resuming()

                    ## the plan is complete and nothing in the target has been touched yet -
                    ## the last moment at which a run which cannot come out right costs nothing
                    self.check_target_name_collisions()

                    self.migrator_tables.update_main_status({'task_name': 'Planner', 'subtask_name': 'Standard workflow', 'success': True, 'message': 'finished OK'})

                    try:
                        self.source_connection.disconnect()
                    except Exception as e:
                        pass
                    try:
                        self.target_connection.disconnect()
                    except Exception as e:
                        pass

                    self.config_parser.print_log_message('INFO', "planner: create_plan: phase done successfully.")
                except Exception as e:
                    planning_failed = True
                    self.migrator_tables.update_main_status({'task_name': 'Planner', 'subtask_name': 'Standard workflow', 'success': False, 'message': f'ERROR: {e}'})
                    self.handle_error(e, "Planner")

            elif self.config_parser.is_mapping_workflow():
                self.migrator_tables.insert_main({'task_name': 'Planner', 'subtask_name': 'Mapping workflow'})
                try:

                    self.mapping_match_tables()

                    self.migrator_tables.update_main_status({'task_name': 'Planner', 'subtask_name': 'Mapping workflow', 'success': True, 'message': 'finished OK'})

                except Exception as e:
                    planning_failed = True
                    self.migrator_tables.update_main_status({'task_name': 'Planner', 'subtask_name': 'Mapping workflow', 'success': False, 'message': f'ERROR: {e}'})
                    self.handle_error(e, "Planner")

            elif self.config_parser.is_anonymization_workflow():
                self.migrator_tables.insert_main({'task_name': 'Planner', 'subtask_name': 'Anonymization workflow'})
                try:

                    if self.source_db_config['connectivity'] == 'ddl':
                        self.config_parser.print_log_message('DEBUG3', f"planner: create_plan: starting ddl connectivity")
                        self.source_connection.parse_ddl_files({ 'migrator_tables': self.migrator_tables})
                        self.source_schema_name = self.config_parser.get_source_schema()

                    self.stdwf_prepare_collations()
                    self.stdwf_prepare_text_search()
                    self.stdwf_prepare_domains()
                    self.stdwf_prepare_user_defined_types()
                    self.stdwf_prepare_defaults()

                    self.check_pausing_resuming()

                    self.stdwf_prepare_aliases()
                    self.stdwf_prepare_sequences()
                    self.stdwf_prepare_tables()
                    self.stdwf_prepare_data_sources()

                    self.check_pausing_resuming()

                    self.stdwf_prepare_views()

                    self.check_pausing_resuming()

                    self.migrator_tables.update_main_status({'task_name': 'Planner', 'subtask_name': 'Anonymization workflow', 'success': True, 'message': 'finished OK'})

                    try:
                        self.source_connection.disconnect()
                    except Exception as e:
                        pass
                    try:
                        self.target_connection.disconnect()
                    except Exception as e:
                        pass

                    self.config_parser.print_log_message('INFO', "planner: create_plan: phase done successfully.")
                except Exception as e:
                    planning_failed = True
                    self.migrator_tables.update_main_status({'task_name': 'Planner', 'subtask_name': 'Anonymization workflow', 'success': False, 'message': f'ERROR: {e}'})
                    self.handle_error(e, "Planner")

            else:
                self.config_parser.print_log_message('ERROR', f"planner: create_plan: Unknown workflow type: {self.config_parser.get_workflow()}")
                exit(1)

            ## and the planner as a whole says what its workflow did, rather than 'finished OK'
            ## over a branch which ended in an error the configuration told the run to survive
            if planning_failed:
                self.migrator_tables.update_main_status({
                    'task_name': 'Planner', 'subtask_name': '', 'success': False,
                    'message': 'the planning of the workflow FAILED - see the phase of the workflow above'})
            else:
                self.migrator_tables.update_main_status({
                    'task_name': 'Planner', 'subtask_name': '', 'success': True,
                    'message': 'finished OK'})

    def load_connector(self, source_or_target):
        """Dynamically load the database connector."""
        # Get the database type from the config
        database_type = self.config_parser.get_db_type(source_or_target)
        self.config_parser.print_log_message( 'DEBUG', f"planner: load_connector: Loading connector for {source_or_target} with database type: {database_type}")
        if source_or_target == 'target' and database_type != 'postgresql':
            raise ValueError("Target database type must be 'postgresql'")
        # Check if the database type is supported
        database_module = MigratorConstants.get_modules().get(database_type)
        if not database_module:
            raise ValueError(f"Unsupported database type: {database_type}")
        # Import the module and get the class
        module_name, class_name = database_module.split(':')
        self.config_parser.print_log_message( 'DEBUG3', f"planner: load_connector: Will load modules {module_name} - {class_name} for {source_or_target} database")
        if not module_name or not class_name:
            raise ValueError(f"Invalid module format: {database_module}")
        # Import the module and get the class
        module = importlib.import_module(module_name)
        connector_class = getattr(module, class_name)
        return connector_class(self.config_parser, source_or_target)

    def pre_planning(self):
        try:
            self.config_parser.print_log_message('INFO', "planner: pre_planning: Running pre-planning actions...")

            self.config_parser.print_log_message( 'DEBUG', f"planner: pre_planning: Target schema: {self.target_schema_name}")
            self.config_parser.print_log_message( 'DEBUG', f"planner: pre_planning: Pre migration script: {self.pre_script}")
            self.config_parser.print_log_message( 'DEBUG', f"planner: pre_planning: Post migration script: {self.post_script}")

            self.config_parser.print_log_message( 'DEBUG', "planner: pre_planning: Connecting to source and target databases...")
            self.check_database_connection(self.source_connection, "Source Database")
            self.check_database_connection(self.target_connection, "Target Database")

            self.config_parser.print_log_message( 'DEBUG', "planner: pre_planning: Checking scripts accessibility...")
            self.check_script_accessibility(self.pre_script)
            self.check_script_accessibility(self.post_script)

            ## connect() applies the session settings of the configuration, the role among
            ## them - which is the owner of the schema created below
            self.target_connection.connect()

            if self.config_parser.should_drop_schema():
                if self.config_parser.is_mapping_workflow():
                    self.config_parser.print_log_message('WARNING', "planner: pre_planning: Migration workflow is set to 'mapping', skipping drop of target schema.")
                elif self.target_schema_name.lower() == 'public':
                    self.config_parser.print_log_message('INFO', "planner: pre_planning: Cannot drop the 'public' schema - skipping drop of schema.")
                else:
                    self.config_parser.print_log_message('INFO', f"planner: pre_planning: Dropping target schema '{self.target_schema_name}'...")
                    self.target_connection.execute_query(f'DROP SCHEMA IF EXISTS "{self.target_schema_name}" CASCADE')

            self.config_parser.print_log_message( 'DEBUG', f"planner: pre_planning: Creating target schema '{self.target_schema_name}' if it does not exist...")
            self.target_connection.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{self.target_schema_name}"')
            self.target_connection.disconnect()

            self.run_pre_migration_script()

            self.config_parser.print_log_message('INFO', "planner: pre_planning: Creating migration plan...")
            self.migrator_tables.create_all()
            self.migrator_tables.insert_main({'task_name': 'Planner', 'subtask_name': ''})
            self.migrator_tables.prepare_data_types_substitution()
            self.migrator_tables.prepare_default_values_substitution()

            if self.sql_functions_mapping:
                for src_func, tgt_func in self.sql_functions_mapping.items():
                    pattern = self.default_value_pattern_for_function(src_func)
                    self.migrator_tables.insert_default_values_substitution({
                        'column_name': '',
                        'source_column_data_type': '',
                        'default_value_value': pattern,
                        'target_default_value': tgt_func
                    })

            self.migrator_tables.prepare_data_migration_limitation()
            self.migrator_tables.prepare_remote_objects_substitution()

            self.config_parser.print_log_message('INFO', "planner: pre_planning: Pre-planning part done successfully.")
        except Exception as e:
            self.handle_error(e, "Pre-planning runs")

    def run_premigration_analysis(self):
        self.config_parser.print_log_message('INFO', "planner: run_premigration_analysis: Running pre-migration analysis...")
        if self.source_db_config.get('connectivity') == 'ddl':
            ## The MEASUREMENTS below - the version, the size, the top tables by rows - need an
            ## instance to ask, and a DDL migration has none: its structure comes out of `.sql`
            ## extracts and its rows out of CSV files. The CHECKS do not. They read what the
            ## connector parsed out of the DDL and what the target can express, and both are
            ## there. Skipping the whole method skipped them too, so a `target_partitioning`
            ## entry against Db2 for z/OS or Db2 for i was never looked at - the one entry those
            ## two sources cannot carry out, because generating a range of partitions needs the
            ## values the column really holds. The refusal is written and was never reached.
            self.config_parser.print_log_message(
                'INFO', "planner: run_premigration_analysis: the source is read from DDL, so "
                        "there is no instance to measure - the checks which do not need one "
                        "are made all the same.")
            self.stop_on(self.analyse_the_target_only())
            return
        blocking_issues = []
        try:
            self.source_connection.connect()
            self.target_connection.connect()

            self.config_parser.print_log_message('INFO', "planner: run_premigration_analysis: ***** Source database *****")
            source_db_version = self.source_connection.get_database_version()
            self.config_parser.print_log_message('INFO', f"planner: run_premigration_analysis: Version: {source_db_version}")
            source_db_size = self.source_connection.get_database_size()
            self.config_parser.print_log_message('INFO', f"planner: run_premigration_analysis: Size: {source_db_size}")

            source_db_top10_tables = self.source_connection.get_top_n_tables({'source_schema_name': self.source_schema_name})
            self.config_parser.print_log_message('INFO', "planner: run_premigration_analysis: Top tables in source database (by various metrics):")
            if source_db_top10_tables:
                for metric, tables in source_db_top10_tables.items():
                    self.config_parser.print_log_message('INFO', f"planner: run_premigration_analysis: Top tables by {metric}:")
                    # Collect rows for table output
                    table_rows = []
                    if metric == 'by_rows':
                        headers = ["#", "Owner", "Table Name", "Rows", "Row Size", "Table Size", "FK", "Date/Time Columns", "PK Columns", "RowID", "Ref FK"]
                        table_rows.append(headers)
                        for idx, table in tables.items():
                            table_rows.append([
                                idx,
                                table['owner'],
                                table['table_name'],
                                f"{table['row_count']:,}" if 'row_count' in table and table['row_count'] is not None else '',
                                f"{table['row_size']:,}" if 'row_size' in table and table['row_size'] is not None else '',
                                f"{table['table_size']:,}" if 'table_size' in table and table['table_size'] is not None else '',
                                f"{table['fk_count']:,}" if 'fk_count' in table and (table['fk_count'] is not None or table['fk_count'] != 0) else '',
                                f"{table['date_time_columns']}" if 'date_time_columns' in table and table['date_time_columns'] is not None else '',
                                f"{table['pk_columns']}" if 'pk_columns' in table and table['pk_columns'] is not None else '',
                                f"{table['has_rowid']}" if 'has_rowid' in table and table['has_rowid'] is not None else '',
                                f"{table['ref_fk_count']}" if 'ref_fk_count' in table and (table['ref_fk_count'] is not None or table['ref_fk_count'] != 0) else '',
                            ])
                    elif metric == 'by_size':
                        headers = ["#", "Owner", "Table Name", "Size", "Rows", "Row Size", "FK", "Date/Time Columns", "PK Columns", "RowID", "Ref FK"]
                        table_rows.append(headers)
                        for idx, table in tables.items():
                            table_rows.append([
                                idx,
                                table['owner'],
                                table['table_name'],
                                f"{table['table_size']:,}",
                                f"{table['row_count']:,}",
                                f"{table['row_size']:,}",
                                f"{table['fk_count']:,}" if table['fk_count'] != 0 else '',
                                f"{table['date_time_columns']}" if table['date_time_columns'] is not None else '',
                                f"{table['pk_columns']}" if table['pk_columns'] is not None else '',
                                f"{table['has_rowid']}" if table['has_rowid'] is not None else '',
                                f"{table['ref_fk_count']}" if table['ref_fk_count'] != 0 else '',
                            ])
                    elif metric == 'by_columns':
                        headers = ["#", "Owner", "Table Name", "Columns", "Rows", "Row Size", "Table Size", "FK", "Date/Time Columns", "PK Columns", "RowID", "Ref FK"]
                        table_rows.append(headers)
                        for idx, table in tables.items():
                            table_rows.append([
                                idx,
                                table['owner'],
                                table['table_name'],
                                f"{table['column_count']:,}",
                                f"{table['row_count']:,}",
                                f"{table['row_size']:,}",
                                f"{table['table_size']:,}",
                                f"{table['fk_count']:,}" if table['fk_count'] != 0 else '',
                                f"{table['date_time_columns']}" if table['date_time_columns'] is not None else '',
                                f"{table['pk_columns']}" if table['pk_columns'] is not None else '',
                                f"{table['has_rowid']}" if table['has_rowid'] is not None else '',
                                f"{table['ref_fk_count']}" if table['ref_fk_count'] != 0 else '',
                            ])
                    elif metric == 'by_indexes':
                        headers = ["#", "Owner", "Table Name", "Indexes", "Rows", "Row Size", "Table Size", "FK", "Date/Time Columns", "PK Columns", "RowID", "Ref FK"]
                        table_rows.append(headers)
                        for idx, table in tables.items():
                            table_rows.append([
                                idx,
                                table['owner'],
                                table['table_name'],
                                f"{table['index_count']:,}",
                                f"{table['row_count']:,}",
                                f"{table['row_size']:,}",
                                f"{table['table_size']:,}",
                                f"{table['fk_count']:,}" if table['fk_count'] != 0 else '',
                                f"{table['date_time_columns']}" if table['date_time_columns'] is not None else '',
                                f"{table['pk_columns']}" if table['pk_columns'] is not None else '',
                                f"{table['has_rowid']}" if table['has_rowid'] is not None else '',
                                f"{table['ref_fk_count']}" if table['ref_fk_count'] != 0 else '',
                            ])
                    elif metric == 'by_constraints':
                        headers = ["#", "Owner", "Table Name", "Type", "Constraints", "Rows", "Row Size", "Table Size", "Date/Time Columns", "PK Columns", "RowID", "Ref FK"]
                        table_rows.append(headers)
                        for idx, table in tables.items():
                            table_rows.append([
                                idx,
                                table['owner'],
                                table['table_name'],
                                table['constraint_type'],
                                f"{table.get('constraint_count', 0):,}",
                                f"{table['row_count']:,}",
                                f"{table['row_size']:,}",
                                f"{table['table_size']:,}",
                                f"{table['date_time_columns']}" if table['date_time_columns'] is not None else '',
                                f"{table['pk_columns']}" if table['pk_columns'] is not None else '',
                                f"{table['has_rowid']}" if table['has_rowid'] is not None else '',
                                f"{table['ref_fk_count']}" if table['ref_fk_count'] != 0 else '',
                            ])
                    else:
                        headers = ["#", "Table"]
                        table_rows.append(headers)
                        for idx, table in tables.items():
                            table_rows.append([idx, str(table)])

                    # Format as a table (simple padding)
                    col_widths = [max(len(str(row[i])) for row in table_rows) for i in range(len(table_rows[0]))]
                    for row in table_rows:
                        formatted_row = " | ".join(
                            str(cell).ljust(col_widths[i]) if i < 3 or i >= len(row) - 3 else str(cell).rjust(col_widths[i])
                            for i, cell in enumerate(row)
                        )
                        self.config_parser.print_log_message('INFO', f"planner: run_premigration_analysis: {formatted_row}")
            else:
                self.config_parser.print_log_message('INFO', "planner: run_premigration_analysis: No top tables data available.")

            # list Top foreign key dependencies
            source_db_top_fk_dependencies = self.source_connection.get_top_fk_dependencies({'source_schema_name': self.source_schema_name})
            self.config_parser.print_log_message('INFO', "planner: run_premigration_analysis: Top foreign key dependencies in source database:")
            if source_db_top_fk_dependencies:
                # Print as a nice table
                headers = ["#", "Table Name", "Foreign Keys", "Dependencies"]
                table_rows = [headers]
                for ord_num, fk_deps in source_db_top_fk_dependencies.items():
                    table_rows.append([
                        ord_num,
                        fk_deps['table_name'],
                        fk_deps['fk_count'],
                        fk_deps['dependencies']
                    ])
                # Calculate column widths
                col_widths = [max(len(str(row[i])) for row in table_rows) for i in range(len(headers))]
                for row in table_rows:
                    formatted_row = " | ".join(
                        str(cell).ljust(col_widths[i]) for i, cell in enumerate(row)
                    )
                    self.config_parser.print_log_message('INFO', f"planner: run_premigration_analysis: {formatted_row}")
            else:
                self.config_parser.print_log_message('INFO', "planner: run_premigration_analysis: No foreign key dependencies found in source database.")

            self.config_parser.print_log_message('INFO', "planner: run_premigration_analysis: ***** Target database *****")
            target_db_version = self.target_connection.get_database_version()
            self.config_parser.print_log_message('INFO', f"planner: run_premigration_analysis: Version: {target_db_version}")
            target_db_size = self.target_connection.get_database_size()
            # self.config_parser.print_log_message('INFO', f"planner: run_premigration_analysis: Size: {target_db_size}")
            # target_db_top10_tables = self.target_connection.get_top_n_tables({'source_schema_name': self.target_schema_name})
            # self.config_parser.print_log_message('INFO', "planner: run_premigration_analysis: Top largest tables in target database:")
            # self.config_parser.print_log_message('DEBUG', f"planner: run_premigration_analysis: Target database Top tables: {target_db_top10_tables}")
            # for ord_num, table in target_db_top10_tables.items():
            #     self.config_parser.print_log_message('INFO', f"planner: run_premigration_analysis: Table: {table['table_name']}, Size: {table['size_bytes']}, Rows: {table['total_rows'] if 'total_rows' in table else 'N/A'}")

            blocking_issues = self.check_target_capabilities()

            self.config_parser.print_log_message('INFO', "planner: run_premigration_analysis: Pre-migration analysis completed successfully.")
        except Exception as e:
            self.handle_error(e, "Pre-migration analysis")
        finally:
            self.source_connection.disconnect()
            self.target_connection.disconnect()

        # Reported after the analysis finished, so the whole report is available in the log,
        # and independently of on_error_action - the migration cannot succeed in these cases.
        self.stop_on(blocking_issues)

    def analyse_the_target_only(self):
        """
        The checks of the analysis which do not need an instance behind the source - what the
        target can express, and what the configuration asks of it. Everything they read comes
        out of the connector's own readers, which a DDL connector answers from the parsed
        extracts, and out of the target, which is a live server in every migration.
        """
        try:
            self.source_connection.connect()
            self.target_connection.connect()
            return self.check_target_capabilities()
        except Exception as e:
            self.handle_error(e, "Pre-migration analysis")
            return []
        finally:
            ## The partitioning plan is read once and kept, so that the report and the run
            ## cannot disagree. The one built HERE was built before the DDL of the source
            ## existed in the migration, so it says nothing is partitioned - and keeping it
            ## would make the run agree with a report which established nothing: the z/OS
            ## ORDERS, partitioned by range over ORDER_DATE in its own CREATE TABLE, arrived
            ## as one ordinary table. It is thrown away and read again once the DDL is parsed.
            self.partitioning_plan = None
            self.partitioning_table_ids = {}
            self.source_connection.disconnect()
            self.target_connection.disconnect()

    def stop_on(self, blocking_issues):
        """The findings which make the migration fail later, reported once and acted on once."""
        if blocking_issues:
            self.config_parser.print_log_message('ERROR', "planner: run_premigration_analysis: The target database does not support features required by the source schema:")
            for issue in blocking_issues:
                self.config_parser.print_log_message('ERROR', f"planner: run_premigration_analysis: - {issue}")
            self.config_parser.print_log_message('ERROR', "planner: run_premigration_analysis: Stopping the migrator after the pre-migration analysis.")
            exit(1)

    def check_target_capabilities(self):
        """
        Check that the target database can express what the source schema needs.
        Returns a list of blocking issues - findings that would make the migration fail later,
        during the creation of the target objects.
        """
        blocking_issues = []
        if self.config_parser.get_target_db_type() != 'postgresql':
            return blocking_issues

        target_version_num = self.target_connection.get_server_version_num()
        if target_version_num is None:
            self.config_parser.print_log_message('WARNING', "planner: check_target_capabilities: Target version could not be determined - skipping target capability checks.")
            return blocking_issues

        # Generated columns (GENERATED ALWAYS AS (...) STORED) require PostgreSQL 12 or newer.
        generated_columns_count = self.source_connection.get_generated_columns_count(self.source_schema_name)
        self.config_parser.print_log_message('INFO', f"planner: check_target_capabilities: Source schema {self.source_schema_name} has {generated_columns_count} generated (computed/virtual) columns.")
        if generated_columns_count > 0 and target_version_num < 120000:
            blocking_issues.append(
                f"Generated columns: the source schema has {generated_columns_count} generated (computed/virtual) column(s), "
                f"which are migrated as PostgreSQL generated columns (GENERATED ALWAYS AS (...) STORED). "
                f"This requires PostgreSQL 12 or newer, but the target runs version {target_version_num // 10000}. "
                f"Upgrade the target database, or exclude the affected tables from the migration.")

        # Extensions of the source, their availability in the target, and whether the
        # configured list covers what the migrated objects really need.
        blocking_issues.extend(self.check_extensions())

        # §4 of development/PARTITIONING_STRATEGY.md - what the source partitions, what this
        # run will do with each of them, and whether what was asked for can be built at all.
        try:
            blocking_issues.extend(self.check_partitioning())
        except Exception as e:
            self.config_parser.print_log_message(
                'WARNING', f"planner: check_target_capabilities: the partitioning of the source "
                           f"could not be analysed ({e}) - it is reported as NOT checked, which "
                           f"is not the same as a schema with nothing partitioned in it.")

        # Check and attempt creation of required PostgreSQL extensions
        required_extensions = self.config_parser.get_required_extensions()
        if required_extensions:
            self.config_parser.print_log_message('INFO', f"planner: check_target_capabilities: Checking required PostgreSQL extensions: {required_extensions}")
            for ext in required_extensions:
                success, msg = self.target_connection.check_and_create_extension(ext)
                if not success:
                    blocking_issues.append(msg)

        return blocking_issues

    def get_tables_selected_for_migration(self):
        """
        Names of the source tables which the configuration selects for migration - the same
        include_tables / exclude_tables evaluation as in stdwf_prepare_tables.
        """
        selected = []
        source_tables = self.source_connection.fetch_table_names(self.source_schema_name)
        for _, table_info in (source_tables or {}).items():
            table_name = table_info['table_name']
            included, _reason = self.config_parser.is_object_selected('table', table_name)
            if not included:
                continue
            selected.append(table_name)
        return selected

    def check_extensions(self):
        """
        Report the extensions of the source database together with their availability in the
        target, and verify that everything the migrated objects depend on is covered - either
        already installed in the target, or listed in migration.required_extensions so that
        the migrator creates it.

        Returns a list of blocking issues. A missing dependency is blocking: the object using
        it would fail to be created later, in the middle of the migration.
        """
        blocking_issues = []

        source_extensions = self.source_connection.fetch_installed_extensions() or {}
        target_extensions = self.target_connection.fetch_installed_extensions() or {}
        target_available = self.target_connection.fetch_available_extensions() or {}
        configured = {name.lower() for name in (self.config_parser.get_required_extensions() or [])}

        if not source_extensions:
            self.config_parser.print_log_message('INFO', "planner: check_extensions: The source database reports no extensions - nothing to check.")
        else:
            self.config_parser.print_log_message('INFO', "planner: check_extensions: Extensions of the source database and their state in the target database:")
            header = ["Extension", "Source version", "Source schema", "In target", "Available in target"]
            rows = [header]
            for name in sorted(source_extensions):
                info = source_extensions[name]
                in_target = target_extensions[name]['version'] if name in target_extensions else '--'
                available = target_available.get(name, '--')
                rows.append([name, info['version'] or '', info['schema'] or '', in_target, available])
            widths = [max(len(str(row[index])) for row in rows) for index in range(len(header))]
            for row in rows:
                self.config_parser.print_log_message(
                    'INFO', "planner: check_extensions: " + " | ".join(str(cell).ljust(widths[index]) for index, cell in enumerate(row)))

        # What the objects selected for migration really depend on
        table_names = self.get_tables_selected_for_migration()
        dependencies = self.source_connection.fetch_extension_dependencies({
            'source_schema_name': self.source_schema_name,
            'table_names': table_names,
            'migrate_indexes': self.config_parser.should_migrate_indexes(),
            'migrate_constraints': self.config_parser.should_migrate_constraints(),
            'migrate_triggers': self.config_parser.should_migrate_triggers(),
            'migrate_views': self.config_parser.should_migrate_views(),
            'migrate_funcprocs': self.config_parser.should_migrate_funcprocs(),
        }) or {}

        if not dependencies:
            self.config_parser.print_log_message('INFO', "planner: check_extensions: The objects selected for migration do not depend on any extension.")
            return blocking_issues

        self.config_parser.print_log_message('INFO', f"planner: check_extensions: The objects selected for migration depend on {len(dependencies)} extension(s):")
        for name in sorted(dependencies):
            required_by = dependencies[name]
            shown = ', '.join(required_by[:5])
            if len(required_by) > 5:
                shown += f", ... ({len(required_by)} objects in total)"
            state = 'installed in target' if name in target_extensions else (
                'listed in migration.required_extensions' if name in configured else 'NOT COVERED')
            self.config_parser.print_log_message('INFO', f"planner: check_extensions: - {name} [{state}]: required by {shown}")

        missing = []
        for name in sorted(dependencies):
            if name in target_extensions:
                # Already there, nothing has to be created and nothing has to be configured
                continue
            if name in configured:
                # The migrator creates it - check_and_create_extension reports a failure
                continue
            missing.append(name)

        for name in missing:
            required_by = dependencies[name]
            shown = ', '.join(required_by[:10])
            if len(required_by) > 10:
                shown += f", ... ({len(required_by)} objects in total)"
            availability = (f"it is available in the target and would be installed"
                            if name in target_available
                            else "it is NOT even available in the target - install the operating system package providing it first")
            blocking_issues.append(
                f"Extension '{name}' is required by objects selected for migration ({shown}), "
                f"but it is neither installed in the target database nor listed in "
                f"migration.required_extensions - {availability}.")

        if missing:
            self.config_parser.print_log_message('WARNING', "planner: check_extensions: Add the missing extensions to the configuration file:")
            self.config_parser.print_log_message('WARNING', "planner: check_extensions:   migration:")
            self.config_parser.print_log_message('WARNING', "planner: check_extensions:     required_extensions:")
            for name in sorted(set(missing) | configured):
                self.config_parser.print_log_message('WARNING', f"planner: check_extensions:       - {name}")

        return blocking_issues

    def stdwf_prepare_sequences(self):
        self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_sequences: Preparing sequences...")
        source_sequences = self.source_connection.fetch_sequences(self.source_schema_name)

        self.config_parser.print_log_message('DEBUG', f"planner: stdwf_prepare_sequences: Source schema: {self.source_schema_name}")
        self.config_parser.print_log_message('DEBUG', f"planner: stdwf_prepare_sequences: Source sequences: {source_sequences}")

        for order_num, sequence_info in source_sequences.items():
            self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_sequences: Processing sequence ({order_num}/{len(source_sequences)}): {sequence_info['sequence_name']}")
            target_sequence_name = sequence_info['sequence_name']
            if self.config_parser.get_use_aliases_as_target_names():
                target_sequence_name = self.config_parser.convert_names_case(sequence_info['sequence_name'])
                # Sequences don't generally have aliases like tables, but names-case-handling should still apply
            else:
                target_sequence_name = self.config_parser.convert_names_case(sequence_info['sequence_name'])

            settings = {
                'sequence_id': sequence_info.get('id', order_num),
                'source_schema_name': self.source_schema_name,
                'source_table_name': sequence_info.get('table_name', None),
                'source_column_name': sequence_info.get('column_name', None),
                ## a sequence object of the source is not an identity column - the flag says so
                ## explicitly, so the protocol tells the two origins of a sequence apart
                'source_is_identity': sequence_info.get('used_in_identity', False),
                'source_column_data_type': sequence_info.get('column_data_type', None),
                'source_sequence_name': sequence_info['sequence_name'],
                'source_sequence_sql': sequence_info.get('source_sequence_sql', ''),
                'source_start_value': sequence_info.get('source_start_value', None),
                'source_last_value': sequence_info.get('source_last_value', None),
                'source_increment_by': sequence_info.get('source_increment_by', None),
                'source_minvalue': sequence_info.get('source_minvalue', None),
                'source_maxvalue': sequence_info.get('source_maxvalue', None),
                'source_cache': sequence_info.get('source_cache', None),
                'source_is_cycled': sequence_info.get('source_is_cycled', None),
                'source_sequence_comment': '',
                'target_schema_name': self.target_schema_name,
                'target_table_name': sequence_info.get('target_table_name', None),
                'target_column_name': sequence_info.get('target_column_name', None),
                'target_sequence_name': target_sequence_name,
                'target_sequence_sql': '',
                'target_sequence_comment': ''
            }
            try:
                self.migrator_tables.insert_sequence(settings)
                self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_sequences: Sequence {sequence_info['sequence_name']} prepared successfully.")
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"planner: stdwf_prepare_sequences: Error processing sequence {sequence_info['sequence_name']}: {e}")

        self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_sequences: Sequences processed successfully.")

    ## ------------------------------------------------------------------ partitioning

    def get_partitioning_plan(self):
        """
        What happens to every partitioned table of this migration, decided once.

        The pre-migration analysis asks for it to report and to check it, and
        stdwf_prepare_tables() asks for it to build the tables. It is read once and kept: a
        second read would ask the source the same questions again and could answer them
        differently, which is how a report and a run stop agreeing.
        """
        if self.partitioning_plan is None:
            self.partitioning_plan = self.build_partitioning_plan()
        return self.partitioning_plan

    def build_partitioning_plan(self):
        """
        The partitioning of the source, read, and the decision taken for every selected table.

        A source whose connector does not read partitioning - or which has none to read - says so
        once, and the plan is then built over EMPTY schemes rather than not built at all. The
        two halves of this feature are independent: reading how the source partitions is per
        connector, and `target_partitioning` reads a configuration and writes PostgreSQL. A
        SQLite or SQL Anywhere source has no scheme to report and can still be given one, and
        returning no plan at all used to make every `target_partitioning` entry against such a
        source both refused by the analysis and, had it passed, silently never applied.
        """
        self.partitioning_note = ''
        self.partitioning_is_absent = False
        source_tables = self.source_connection.fetch_table_names(self.source_schema_name)
        selected = []
        table_ids = {}
        for _, table_info in (source_tables or {}).items():
            table_name = table_info['table_name']
            included, _reason = self.config_parser.is_object_selected('table', table_name)
            if included:
                selected.append(table_name)
                table_ids[table_name] = table_info.get('id')
        self.partitioning_table_ids = table_ids

        absent = self.source_connection.object_kind_is_absent('table_partitioning')
        not_read = self.source_connection.object_kind_not_read('table_partitioning')
        if absent:
            self.partitioning_note = self.source_connection.OBJECT_KINDS_ABSENT.get(
                'table_partitioning', 'this source has no partitioning')
            ## P2-8 once more, and one level finer than the note itself. Both cases end here
            ## with nothing read, and they are not the same answer: SQL Anywhere HAS no table
            ## partitioning, while a Db2 read through SYSIBM has some that this run cannot see.
            ## The note says which, and the sentence it is printed under has to agree with it.
            self.partitioning_is_absent = True
        elif not_read:
            self.partitioning_note = not_read
            self.partitioning_is_absent = False
        if self.partitioning_note:
            ## nothing to read, and still a plan to build: what `target_partitioning` names is
            ## decided here too, and a source with no scheme of its own is exactly the source
            ## most likely to be given one
            return self.plan_over(schemes={}, selected=selected)

        ## the tables which are worth asking about. A connector which can answer it in one
        ## query answers it; one which cannot says so, and every table is asked.
        candidates = self.source_connection.fetch_partitioning_candidates(self.source_schema_name)
        schemes = {}
        for table_name in selected if candidates is None else [
                name for name in selected if name in candidates]:
            scheme = self.source_connection.fetch_table_partitioning({
                'source_schema_name': self.source_schema_name,
                'source_table_name': table_name,
            })
            if scheme:
                schemes[table_name] = scheme

        ## a partition which is ITSELF partitioned and which the filters left out is still part
        ## of the scheme, so its own partitioning has to be read as well - otherwise a
        ## sub-partitioned partition comes out with no children. A partition which is not
        ## partitioned is not asked about: everything the plan needs of it is in the row its
        ## parent already answered, and asking would be one catalogue query per partition -
        ## 960 of them for one Oracle table of §2.2.
        pending = [partition.get('name') for scheme in schemes.values()
                   for partition in scheme.get('partitions') or [] if partition.get('is_partitioned')]
        while pending:
            table_name = pending.pop()
            if not table_name or table_name in schemes:
                continue
            scheme = self.source_connection.fetch_table_partitioning({
                'source_schema_name': self.source_schema_name,
                'source_table_name': table_name,
            })
            if not scheme:
                continue
            schemes[table_name] = scheme
            pending.extend(partition.get('name') for partition in scheme.get('partitions') or []
                           if partition.get('is_partitioned'))

        return self.plan_over(schemes, selected)

    def plan_over(self, schemes, selected):
        """
        The decision for every selected table, out of whatever was read about the source.

        The `target_partitioning` entries are resolved against the tables the migration really
        has, case-insensitively - an entry is written by hand and the source spells its tables
        the way its engine does, which for Oracle and Db2 is upper case. `table_settings`
        already matches that way, and the two must not disagree about which table an entry means.
        """
        repartitioned = {self.selected_table_named(entry.get('table_name'), selected)
                         for entry in self.config_parser.get_target_partitioning()
                         if entry.get('table_name')}
        return partitioning.build_plan(
            schemes, selected,
            mode_of=self.config_parser.get_source_partitioning,
            repartitioned_tables={name for name in repartitioned if name},
            target_version_num=self.target_connection.get_server_version_num())

    def source_columns_named(self, written_columns, table_name):
        """
        The columns of the source one entry's `partitioning_columns` mean, in the source's own
        spelling.

        The entry is written by hand and the source spells its columns the way its engine does:
        an Oracle entry naming `rate_date` means `RATE_DATE`, and asking the source for
        `min("rate_date")` is ORA-00904. The clause of the TARGET is a different spelling again -
        `names_case_handling` decides that one - so the source's is resolved here once and each
        side is written from it.

        A column which matches nothing is answered as it was written: the checks of §4.4 report
        it as a column the table does not have, which is the message the user needs.
        """
        try:
            source_columns = self.source_connection.fetch_table_columns({
                'table_schema': self.source_schema_name,
                'table_name': table_name,
                'target_db_type': self.config_parser.get_target_db_type(),
            })
            real = [column['column_name'] for column in (source_columns or {}).values()]
        except Exception as e:
            self.config_parser.print_log_message(
                'DEBUG', f"planner: source_columns_named: the columns of {table_name} could not "
                         f"be read ({e}) - the entry is used as it was written.")
            return list(written_columns)
        by_lower = {str(name).strip().lower(): name for name in real}
        return [by_lower.get(str(name).strip().lower(), name) for name in written_columns]

    @staticmethod
    def selected_table_named(written, selected):
        """
        The table of the migration one configured name means, or None where there is none.

        The comparison is case-insensitive because the name is written by hand and the source
        spells its own tables the way its engine does.
        """
        if not written:
            return None
        wanted = str(written).strip().lower()
        for name in selected:
            if str(name).strip().lower() == wanted:
                return name
        return None

    def check_partitioning(self):
        """
        The partitioning block of the pre-migration analysis - §4 of
        development/PARTITIONING_STRATEGY.md.

        Two halves. The inventory says what the source partitions and what this run will do
        with each of them, and is printed whether or not anything is configured. The
        feasibility check answers whether what was asked for can be built, and what it finds
        is blocking: every one of those is a run which fails somewhere in the middle
        otherwise, and here nothing has been created yet.
        """
        blocking_issues = []
        plan = self.get_partitioning_plan()

        self.config_parser.print_log_message('INFO', "planner: check_partitioning: ***** Partitioning *****")
        if self.partitioning_note:
            opening = ("this source has no table partitioning to report"
                       if self.partitioning_is_absent else
                       "the partitioning of this source is not reported")
            self.config_parser.print_log_message(
                'INFO', f"planner: check_partitioning: {opening}: {self.partitioning_note}")

        partitioned = [decision for decision in plan.values()
                       if decision.action in (partitioning.PRESERVE, partitioning.FLATTEN)]
        parts = [decision for decision in plan.values()
                 if decision.action == partitioning.PART_OF_PARENT]
        orphans = [decision for decision in plan.values()
                   if decision.action == partitioning.ORPHAN_PARTITION]
        repartitioned = [decision for decision in plan.values()
                         if decision.action == partitioning.REPARTITION]

        if not (partitioned or parts or orphans or repartitioned):
            ## P2-8: "not read" and "there is none" must never look alike. Where the note is
            ## set the migrator did not look - or looked through a catalogue which does not
            ## answer - and saying "no table is partitioned" underneath it states as a fact
            ## the one thing that was not established.
            self.config_parser.print_log_message(
                'INFO', "planner: check_partitioning: no table of the source schema is "
                        "partitioned, and none is partitioned by target_partitioning."
                        if not self.partitioning_note else
                        "planner: check_partitioning: no table of the source schema is "
                        "partitioned, and none is partitioned by target_partitioning."
                        if self.partitioning_is_absent else
                        "planner: check_partitioning: whether any table of the source schema is "
                        "partitioned was not established, and none is partitioned by "
                        "target_partitioning.")
        else:
            ## §4.2's headline: a number a reader can act on, before the detail underneath it.
            ## A scheme of more than one level is worth counting on its own - §2.2 is about
            ## what it costs to reproduce one.
            deep = len([decision for decision in partitioned if decision.source_level_count > 1])
            ## the partitions of the source: the ones which are tables of the schema in their
            ## own right where they are - postgresql - and the ones the catalogue counts where
            ## they are not, which is every other source
            partition_count = len(parts) or sum(
                decision.scheme.get('partition_count', 0) or 0 for decision in partitioned)
            headline = (f"{len(partitioned)} of {len(plan)} table(s) are partitioned on the "
                        f"source, holding {partition_count} partition(s)")
            if deep:
                headline += f"; {deep} of them are partitioned on more than one level"
            headline += (f". {len(repartitioned)} table(s) are partitioned by "
                         f"target_partitioning.")
            self.config_parser.print_log_message(
                'INFO', f"planner: check_partitioning: {headline}")
            rows = [["Table", "Source scheme", "Partitions", "What happens"]]
            for decision in sorted(partitioned + repartitioned + orphans,
                                   key=lambda item: item.table_name):
                rows.append([
                    decision.table_name,
                    decision.key_definition or '-',
                    str(decision.scheme.get('partition_count', 0) or 0),
                    decision.describe(),
                ])
            widths = [max(len(str(row[index])) for row in rows) for index in range(len(rows[0]))]
            for row in rows:
                self.config_parser.print_log_message(
                    'INFO', "planner: check_partitioning: " + " | ".join(
                        str(cell).ljust(widths[index]) for index, cell in enumerate(row)))

        ## §3.1 for a scheme which is carried over rather than asked for. It is not the
        ## smaller case: Oracle keeps a primary key which does not contain the partitioning
        ## column in a GLOBAL index, which is legal and ordinary there and has no counterpart
        ## here, so a table which has run that way for years is refused - now, rather than at
        ## the end of the run when the constraint is added to a table already holding the data
        self.check_preserved_keys(plan)

        for decision in sorted(plan.values(), key=lambda item: item.table_name):
            for warning in decision.warnings:
                self.config_parser.print_log_message('WARNING', f"planner: check_partitioning: {warning}.")
            blocking_issues.extend(decision.issues)

        blocking_issues.extend(self.check_repartitioning(plan))
        self.record_partitioning(plan)
        return blocking_issues

    def check_preserved_keys(self, plan):
        """
        The primary key, every unique constraint and every unique index of each table whose
        scheme is carried over, against the partitioning columns - §3.1.

        One read per preserved table, and there are few of them: §4.2's headline is 12 of 340.
        A source whose keys cannot be read has the check reported as one which was NOT made,
        never as one which passed.
        """
        for decision in plan.values():
            if decision.action != partitioning.PRESERVE:
                continue
            partitioning.check_preserved_keys(decision, self.read_unique_keys(decision.table_name))

    def check_repartitioning(self, plan):
        """
        Every `target_partitioning` entry, against the table it names - §4.4 of the design.

        Everything it reads comes from the SOURCE: the protocol tables are still empty at this
        point in the run, which is what makes this the early copy of the check the planner
        makes again when it prepares the table. Everything it refuses is a run which otherwise
        fails somewhere in the middle - and most of them at the very end, after the data has
        been loaded, which is the worst moment to find out.
        """
        blocking_issues = []
        entries = self.config_parser.get_target_partitioning()
        if not entries:
            return blocking_issues

        existing_target_names = self.target_schema_object_names()
        ## the tables the migration really has - which is what an entry names, and which is not
        ## the same list as the plan for a source whose partitioning was never read
        selected = list((self.partitioning_table_ids or {}).keys()) or list(plan.keys())
        ## A source read from DDL has no table list at this point: its extracts are parsed
        ## into the migration AFTER this analysis. An empty list is not a schema which holds
        ## nothing, and answering "the source schema does not hold CURRENCY_RATES" about a
        ## schema which does hold it refuses the entry for a reason that is not true - and
        ## hides the reason which is: there is no instance to ask for the smallest and the
        ## largest value the column holds, which is exactly what a date_range needs.
        table_list_was_read = bool(selected)
        for entry in entries:
            written = entry.get('table_name')
            table_name = self.selected_table_named(written, selected) or written
            decision = plan.get(table_name)
            table_exists = (self.selected_table_named(written, selected) is not None
                            or not table_list_was_read)
            columns = []
            facts = None
            first_value = last_value = None
            bounds_were_read = False

            if table_exists and table_list_was_read:
                columns, facts = self.read_partitioning_facts(table_name)
                first_value, last_value, bounds_were_read = self.read_partitioning_bounds(
                    entry, table_name)

            verdict = partitioning.check_repartitioning(
                entry, columns, None,
                target_version_num=self.target_connection.get_server_version_num(),
                table_exists=table_exists,
                table_is_partition=bool(decision and decision.scheme.get('is_partition')),
                facts=facts,
                first_value=first_value, last_value=last_value,
                existing_target_names=existing_target_names,
                bounds_were_read=bounds_were_read,
                bounds_can_be_read=getattr(
                    self.source_connection, 'CAN_PROBE_COLUMN_VALUES', True))

            self.report_partitioning_verdict(entry, verdict)
            blocking_issues.extend(verdict.issues)
        return blocking_issues

    def report_partitioning_verdict(self, entry, verdict):
        """
        One block per `target_partitioning` entry: what it asks for, what was checked and found
        good, what is worth saying, and what stops the run.

        An entry which passes says so as plainly as one which fails. A report which only speaks
        up when it is unhappy is one nobody trusts when it is silent.
        """
        columns = ', '.join(partitioning.partitioning_columns_of(entry)) or '-'
        headline = (f"target_partitioning: {verdict.table_name} -> "
                    f"{str(entry.get('partition_by') or '?').upper()} ({columns})"
                    + (f", {entry.get('date_range')}" if entry.get('date_range') else ''))
        self.config_parser.print_log_message('INFO', f"planner: check_repartitioning: {headline}")
        for note in verdict.notes:
            self.config_parser.print_log_message('INFO', f"planner: check_repartitioning:     ok       {note}")
        for warning in verdict.warnings:
            self.config_parser.print_log_message('WARNING', f"planner: check_repartitioning:     note     {warning}.")
        for issue in verdict.issues:
            self.config_parser.print_log_message('ERROR', f"planner: check_repartitioning:     BLOCKING {issue}.")
        if verdict.can_be_built:
            self.config_parser.print_log_message(
                'INFO', f"planner: check_repartitioning:     -> {verdict.table_name} can be "
                        f"partitioned as asked.")

    def read_partitioning_facts(self, table_name):
        """
        The columns of one source table and everything about it which decides whether it can be
        partitioned. Returns (column names, facts) - and facts is None where this connector
        does not read them, which the checks report as NOT made.
        """
        columns = []
        try:
            source_columns = self.source_connection.fetch_table_columns({
                'table_schema': self.source_schema_name,
                'table_name': table_name,
                'target_db_type': self.config_parser.get_target_db_type(),
            })
            columns = [column['column_name'] for column in (source_columns or {}).values()]
        except Exception as e:
            self.config_parser.print_log_message(
                'WARNING', f"planner: read_partitioning_facts: the columns of {table_name} could "
                           f"not be read ({e}) - the entry is not checked against them.")
        try:
            facts = self.source_connection.fetch_partitioning_facts({
                'source_schema_name': self.source_schema_name,
                'source_table_name': table_name,
            })
        except Exception as e:
            self.config_parser.print_log_message(
                'WARNING', f"planner: read_partitioning_facts: the facts of {table_name} could "
                           f"not be read ({e}) - the checks which need them are NOT made.")
            facts = None
        if facts is None:
            ## the connector reads no facts of its own - the keys are still worth asking for
            ## through the indexes, which every connector answers
            keys = self.read_unique_keys(table_name)
            if keys is not None:
                facts = {'columns': {}, 'unique_keys': keys, 'exclusion_constraints': [],
                         'referenced_by': [], 'row_estimate': None,
                         'inherits_from_a_plain_table': False,
                         'is_a_plain_inheritance_parent': False, 'date_range_types': ()}
        return columns, facts

    def read_partitioning_bounds(self, entry, table_name):
        """
        The smallest and the largest value of the partitioning column, for an entry which asks
        for a range of dates. Returns (first, last, whether they were read).
        """
        if not entry.get('date_range'):
            return None, None, False
        columns = self.source_columns_named(
            partitioning.partitioning_columns_of(entry), table_name)
        if not columns:
            return None, None, False
        try:
            first_value, last_value = self.source_connection.probe_column_bounds({
                'source_schema_name': self.source_schema_name,
                'source_table_name': table_name,
                'column_name': columns[0],
            })
            return first_value, last_value, True
        except Exception as e:
            self.config_parser.print_log_message(
                'WARNING', f"planner: read_partitioning_bounds: the smallest and the largest "
                           f"value of {table_name}.{columns[0]} could not be read ({e}).")
            return None, None, False

    def target_schema_object_names(self):
        """
        What the target schema already holds, so that a generated partition name which would
        collide with it is refused before anything is created rather than in the middle of it.

        A run which drops what it is about to create has nothing to collide with: the objects
        in the target now are the ones the previous run of this same migration left there, and
        they are dropped before the first one is created. Checking against them would refuse
        every re-run of a configuration which worked the first time.
        """
        if self.config_parser.should_drop_schema() or self.config_parser.should_drop_tables():
            self.config_parser.print_log_message(
                'DEBUG', "planner: target_schema_object_names: the run drops the target objects "
                         "before it creates them, so a generated partition name is not checked "
                         "against what the schema holds now.")
            return set()
        try:
            self.target_connection.connect()
            cursor = self.target_connection.connection.cursor()
            cursor.execute("""
                SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s
            """, (self.target_schema_name,))
            names = {row[0] for row in cursor.fetchall()}
            cursor.close()
            return names
        except Exception as e:
            self.config_parser.print_log_message(
                'DEBUG', f"planner: target_schema_object_names: the objects of "
                         f"{self.target_schema_name} could not be listed ({e}) - a generated "
                         f"partition name is not checked against them.")
            return set()

    def read_unique_keys(self, table_name):
        """
        The primary key and the unique constraints of one source table, as
        partitioning.check_repartitioning() wants them - or None where they could not be read,
        which is reported as a check which was NOT made rather than as one which passed.
        """
        table_id = (self.partitioning_table_ids or {}).get(table_name)
        try:
            indexes = self.source_connection.fetch_indexes({
                'source_table_id': table_id,
                'source_table_name': table_name,
                'source_table_schema': self.source_schema_name,
                'source_db_type': self.config_parser.get_source_db_type(),
                'source_db_version': self.config_parser.get_source_db_version(),
                'target_table_schema': self.target_schema_name,
                'target_table_name': table_name,
                'target_columns': {},
            })
        except Exception as e:
            self.config_parser.print_log_message(
                'DEBUG', f"planner: read_unique_keys: the indexes of {table_name} could not be "
                         f"read ({e}) - the unique keys are reported as not checked.")
            return None

        keys = []
        for _, index in (indexes or {}).items():
            index_type = str(index.get('index_type') or '').upper()
            if index_type not in ('PRIMARY KEY', 'UNIQUE'):
                continue
            raw_columns = index.get('index_columns') or ''
            if isinstance(raw_columns, (list, tuple)):
                columns = [str(name).strip().strip('"') for name in raw_columns]
            else:
                columns = [name.strip().strip('"') for name in str(raw_columns).split(',')]
            keys.append({
                'name': index.get('index_name'),
                'columns': [name for name in columns if name],
                'is_primary': index_type == 'PRIMARY KEY',
            })
        return keys

    def record_partitioning(self, plan):
        """
        The scheme of the source and the scheme of the target, written into the two protocol
        tables which have existed - and been created empty at the start of every run - since
        before this was built.
        """
        if self.migrator_tables is None:
            return
        for decision in sorted(plan.values(), key=lambda item: item.table_name):
            scheme = decision.scheme
            if scheme.get('is_partitioned'):
                try:
                    self.migrator_tables.insert_source_table_partitioning({
                        'source_schema_name': self.source_schema_name,
                        'source_table_name': decision.table_name,
                        'source_table_id': (self.partitioning_table_ids or {}).get(decision.table_name),
                        'source_table_partitioning_level': scheme.get('level', 1),
                        'source_partitioning_method': scheme.get('method', ''),
                        ## the table at the top of the tree, so that the rows of a scheme of
                        ## more than one level can be read back as one scheme. A parent is its
                        ## own root; a partition which is itself partitioned carries the root
                        ## the plan resolved for it.
                        'source_root_table_name': decision.root_table or decision.table_name,
                        'source_partition_columns': ', '.join(scheme.get('columns') or []),
                        'source_partition_ranges': '; '.join(
                            f"{partition.get('name')}: {partition.get('bound')}"
                            for partition in scheme.get('partitions') or []),
                        ## what only this source has, and which nothing reads back: the
                        ## INTERVAL expression, the sub-partitioning which is not carried over,
                        ## the tablespaces the partitions sit in. §5.1 of the design
                        'source_partitioning_engine_specific': scheme.get('engine_specific') or {},
                    })
                    ## the levels under it whose partitions are not relations of their own, so
                    ## nothing walked into them - one row each, which is what the level column
                    ## is for. They are recorded because the source has them, and they are not
                    ## built: §2.2
                    for level in scheme.get('levels_below') or []:
                        self.migrator_tables.insert_source_table_partitioning({
                            'source_schema_name': self.source_schema_name,
                            'source_table_name': decision.table_name,
                            'source_table_id': (self.partitioning_table_ids or {}).get(decision.table_name),
                            'source_table_partitioning_level': level.get('level', 2),
                            'source_partitioning_method': level.get('method', ''),
                            'source_root_table_name': decision.root_table or decision.table_name,
                            'source_partition_columns': ', '.join(level.get('columns') or []),
                            'source_partition_ranges': '',
                        })
                except Exception as e:
                    self.config_parser.print_log_message(
                        'WARNING', f"planner: record_partitioning: the scheme of {decision.table_name} "
                                   f"could not be recorded: {e}")
            if decision.action != partitioning.PRESERVE:
                continue
            try:
                self.migrator_tables.insert_target_table_partitioning({
                    'target_schema_name': self.target_schema_name,
                    ## the name the tables protocol holds for the same table, which is what the
                    ## summary joins the two rows on - names_case_handling is applied when the
                    ## object is created, not when it is recorded
                    'target_table_name': decision.table_name,
                    'target_table_id': (self.partitioning_table_ids or {}).get(decision.table_name),
                    ## how deep the scheme the target really got is - which is as deep as the
                    ## source for a PostgreSQL one and one level for an Oracle composite, and
                    ## the difference is what the summary reports as not carried over
                    'target_table_partitioning_level': decision.target_level_count or 1,
                    'target_partition_columns': ', '.join(
                        self.config_parser.convert_names_case(column)
                        for column in scheme.get('columns') or []),
                    ## the bound the target was given, which is not always the one the source
                    ## wrote - the source's own spelling is in the source partitioning table
                    'target_partition_ranges': '; '.join(
                        f"{self.config_parser.convert_names_case(partition.name)}: {partition.bound}"
                        for partition in decision.partitions),
                })
            except Exception as e:
                self.config_parser.print_log_message(
                    'WARNING', f"planner: record_partitioning: the target scheme of "
                               f"{decision.table_name} could not be recorded: {e}")

    def repartitioning_entry(self, source_table_name):
        """
        The `target_partitioning` entry which names one table, or None.

        Matched case-insensitively, the way `table_settings` matches: the entry is written by
        hand and the source spells its tables the way its engine does.
        """
        wanted = str(source_table_name or '').strip().lower()
        for entry in self.config_parser.get_target_partitioning():
            if str(entry.get('table_name') or '').strip().lower() == wanted:
                return entry
        return None

    def repartitioning_sql_for(self, entry, source_table_name, target_table_name):
        """
        The PARTITION BY clause and the partitions of one `target_partitioning` entry.

        Returns (clause, [statements], [columns]). An entry which asks for no partitions to be
        generated - no `date_range` - answers the clause and an empty list, and the table is
        created partitioned with nothing under it. That is a table which refuses every INSERT
        with `no partition of relation … found for row`, so it is said out loud here.
        """
        ## resolved to the spelling the SOURCE really uses, so that the probe below asks for a
        ## column which exists; the clause is then written in the names of the TARGET, which
        ## names_case_handling decides. §4.4 of development/PARTITIONING_STRATEGY.md
        columns = self.source_columns_named(
            partitioning.partitioning_columns_of(entry), source_table_name)
        quoted = ', '.join(f'"{self.config_parser.convert_names_case(column)}"'
                           for column in columns)
        clause = f" PARTITION BY {str(entry.get('partition_by') or '').upper()} ({quoted})"

        date_range = entry.get('date_range')
        if not date_range:
            self.config_parser.print_log_message(
                'WARNING', f"planner: repartitioning_sql_for: target_partitioning for "
                           f"{source_table_name} names no date_range, so no partition is created "
                           f"for it. The table is created partitioned and EVERY row is refused "
                           f"with 'no partition of relation ... found for row' - write a "
                           f"date_range, or take the entry out.")
            return clause, [], columns

        try:
            first_value, last_value = self.source_connection.probe_column_bounds({
                'source_schema_name': self.source_schema_name,
                'source_table_name': source_table_name,
                'column_name': columns[0],
            })
        except Exception as e:
            ## the pre-migration analysis refuses an entry whose bounds cannot be read at all,
            ## so this is a source which could be asked and answered with an error. The table is
            ## created partitioned with nothing under it either way; saying which of the two it
            ## was is the difference between a report and a traceback
            self.config_parser.print_log_message(
                'ERROR', f"planner: repartitioning_sql_for: the smallest and the largest value "
                         f"of {source_table_name}.{columns[0]} could not be read ({e}), so no "
                         f"partition could be generated. {source_table_name} is created "
                         f"partitioned and EMPTY, and every row of it will be refused.")
            return clause, [], columns
        self.config_parser.print_log_message(
            'INFO', f"planner: repartitioning_sql_for: {source_table_name}.{columns[0]} holds "
                    f"{first_value} .. {last_value}; the partitions are generated by {date_range}.")
        partitions = partitioning.generate_range_partitions(
            entry, target_table_name, first_value, last_value)
        statements = [self.target_connection.get_create_partition_sql({
            'target_schema_name': self.target_schema_name,
            'target_table_name': partition.name,
            'parent_table_name': target_table_name,
            'partition_bound': partition.bound,
        }) for partition in partitions]
        if not statements:
            self.config_parser.print_log_message(
                'WARNING', f"planner: repartitioning_sql_for: {source_table_name} holds no row in "
                           f"{columns[0]}, so no partition could be generated from its values. The "
                           f"table is created partitioned and empty.")
        return clause, statements, columns

    def partitioning_clause_for(self, decision, target_table_name):
        """
        What has to be appended to the CREATE TABLE of a preserved parent, and the statements
        which create its partitions. Returns (partition_by_clause, [statements]).
        """
        if decision is None or decision.action != partitioning.PRESERVE:
            return '', []
        statements = []
        for partition in decision.partitions:
            parent_name = (target_table_name if partition.parent == decision.table_name
                           else partition.parent)
            statements.append(self.target_connection.get_create_partition_sql({
                'target_schema_name': self.target_schema_name,
                'target_table_name': partition.name,
                'parent_table_name': parent_name,
                'partition_bound': partition.bound,
                'key_definition': partition.key_definition,
            }))
        ## the key as the TARGET has to be given it - the same string for a PostgreSQL source
        ## and not for any other, because Oracle holds ORDER_DATE where the target holds
        ## order_date
        return f" PARTITION BY {decision.target_key_definition}", statements

    def stdwf_prepare_tables(self):
        self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_tables: Preparing tables...")
        # if self.source_db_config.get('connectivity') == 'ddl':
        #     self.config_parser.print_log_message('DEBUG', "planner: stdwf_prepare_tables: skipping source db fetch for tables due to DDL connectivity")
        #     return
        source_tables = self.source_connection.fetch_table_names(self.source_schema_name)
        include_tables = self.config_parser.get_include_tables()
        exclude_tables = self.config_parser.get_exclude_tables() or []
        ## what happens to every partitioned table - read once, and the same answer the
        ## pre-migration analysis reported
        table_partitioning_plan = self.get_partitioning_plan()

        self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Source schema: {self.source_schema_name}")
        self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Source tables: {source_tables}")
        self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Include tables: {include_tables}")
        self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Exclude tables: {exclude_tables}")

        for order_num, table_info in source_tables.items():
            source_table_rows = 0
            target_table_rows = 0
            self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_tables: Processing table ({order_num}/{len(source_tables)}): {table_info['table_name']}")
            target_table_name = table_info['table_name']
            target_alias_name = ''
            if self.config_parser.get_use_aliases_as_target_names():
                alias_dict = self.migrator_tables.get_alias_for_table(self.source_schema_name, table_info['table_name'])
                if alias_dict:
                    alias_name = alias_dict.get('target_alias_name')
                    target_table_name = alias_name
                    target_alias_name = alias_name
                    self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_tables: Source table {table_info['table_name']} mapped to target alias {target_table_name}")
                    if 'id' in alias_dict:
                        self.migrator_tables.update_aliases_status({
                            'row_id': alias_dict['id'],
                            'success': True,
                            'message': f"Alias used as target name for table {table_info['table_name']}"
                        })
            if not self.config_parser.report_object_selection(
                    'table', table_info['table_name'], 'planner: stdwf_prepare_tables'):
                continue

            ## A partition of a table which is being migrated is not a table of its own: it is
            ## created with its parent and its rows arrive through it. Migrating it separately
            ## wrote every row twice - the parent answers all of them - and tried to attach a
            ## partition to a parent nothing had partitioned.
            partitioning_decision = table_partitioning_plan.get(table_info['table_name'])
            if partitioning_decision is not None and not partitioning_decision.migrated_as_table:
                self.config_parser.print_log_message(
                    'INFO', f"planner: stdwf_prepare_tables: {table_info['table_name']} is not "
                            f"migrated as a table of its own - {partitioning_decision.reason}.")
                continue

            source_columns = []
            target_columns = []
            target_table_sql = None
            settings = {}
            table_partitioned = False
            table_partitioning_columns = ''
            table_partitioned_by = ''
            create_partitions_sql = ''
            try:
                settings = {
                    'table_schema': self.source_schema_name,
                    'table_name': table_info['table_name'],
                    'target_db_type': self.config_parser.get_target_db_type(),
                }
                table_description = self.source_connection.get_table_description(settings)
                self.config_parser.print_log_message( 'DEBUG3', f"planner: stdwf_prepare_tables: Table description: {table_description}")
                table_description = table_description['table_description'] if 'table_description' in table_description else ''
                self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Table description: {table_description}")
                source_columns = self.source_connection.fetch_table_columns(settings)
                self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Fetched source columns: {source_columns}")

                for _, column_info in source_columns.items():
                    self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Checking for data types / default values substitutions for column {column_info}...")
                    substitution = self.migrator_tables.check_data_types_substitution({
                                                                'table_name': table_info['table_name'],
                                                                'column_name': column_info['column_name'],
                                                                'check_type': column_info['data_type'],
                                                            })
                    if substitution:
                        self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Substitution based on data_type ({column_info['data_type']}): {substitution}")
                        column_info['column_type_substitution'] = substitution
                    else:
                        substitution = self.migrator_tables.check_data_types_substitution({
                                                                'table_name': table_info['table_name'],
                                                                'column_name': column_info['column_name'],
                                                                'check_type': column_info['column_type'],
                                                            })
                        if substitution:
                            self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Substitution based on column_type ({column_info['column_type']}): {substitution}")
                            column_info['column_type_substitution'] = substitution
                        else:
                            if 'basic_data_type' in column_info and column_info['basic_data_type'] != '':
                                substitution = self.migrator_tables.check_data_types_substitution({
                                                                'table_name': table_info['table_name'],
                                                                'column_name': column_info['column_name'],
                                                                'check_type': column_info['basic_data_type']
                                                            })
                                if substitution:
                                    self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Substitution based on basic_data_type ({column_info['basic_data_type']}): {substitution}")
                                    column_info['column_type_substitution'] = substitution

                    # checking for default values substitution with the new data type
                    if column_info['column_default_value'] != '':
                        substitution = self.migrator_tables.check_default_values_substitution({
                            'check_column_name': column_info['column_name'],
                            'check_column_data_type': column_info['data_type'],
                            'check_default_value': column_info['column_default_value'],
                        })
                        if substitution is not None and column_info['column_default_value'] != substitution:
                            column_info['replaced_column_default_value'] = substitution
                            self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Substituted default value: {column_info['column_default_value']} -> {substitution}")

                self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Source columns: {source_columns}")
                settings = {
                    'source_db_type': self.config_parser.get_source_db_type(),
                    'source_schema_name': self.source_schema_name,
                    'source_table_name': table_info['table_name'],
                    'source_table_id': table_info['id'],
                    'target_db_type': self.config_parser.get_target_db_type(),
                    'target_schema_name': self.target_schema_name,
                    'target_table_name': target_table_name,
                    'source_columns': source_columns,
                    'migrator_tables': self.migrator_tables,
                    'user_collations': self.migrated_collations,
                    'text_search_objects': self.migrated_text_search,
                }
                self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: convert_table_columns - settings: {settings}")
                target_columns = self.convert_table_columns(settings)

                settings['target_columns'] = target_columns
                self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: convert_table_columns - target_columns: {target_columns}")

                target_table_sql = self.target_connection.get_create_table_sql(settings)
                self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Target table SQL: {target_table_sql}")

                ## the scheme of the source, kept as it stands - the partitions are created
                ## in the same worker which creates the parent, through the create_partitions_sql
                ## the orchestrator already executes right behind the CREATE TABLE
                partition_by_clause, partition_statements = self.partitioning_clause_for(
                    partitioning_decision, target_table_name)
                if partition_by_clause:
                    target_table_sql += partition_by_clause
                    create_partitions_sql = json.dumps(partition_statements)
                    table_partitioned = True
                    table_partitioned_by = partitioning_decision.method
                    ## the row describes the TARGET table, so its columns are named as the
                    ## target names them - Oracle holds ORDER_DATE where the target holds
                    ## order_date, and the summary prints this beside the scheme of the source
                    table_partitioning_columns = ', '.join(
                        self.config_parser.convert_names_case(column)
                        for column in partitioning_decision.scheme.get('columns') or [])
                    self.config_parser.print_log_message(
                        'INFO', f"planner: stdwf_prepare_tables: {table_info['table_name']} keeps the "
                                f"partitioning of the source: {partitioning_decision.key_definition}, "
                                f"{len(partition_statements)} partition(s).")
                elif partitioning_decision is not None and partitioning_decision.action == partitioning.FLATTEN:
                    self.config_parser.print_log_message(
                        'INFO', f"planner: stdwf_prepare_tables: {table_info['table_name']} is "
                                f"partitioned on the source ({partitioning_decision.key_definition}) "
                                f"and is created as ONE ordinary table - source_partitioning: flatten.")

                ## §5.3 - a scheme the source never had. The bounds are computed from the
                ## smallest and the largest value of the column, which the connector of the
                ## SOURCE reads in its own quoting, and the calendar is arithmetic done here
                ## rather than a generate_series() the target is asked to run over values read
                ## from the source.
                if partitioning_decision is not None and partitioning_decision.action == partitioning.REPARTITION:
                    entry = self.repartitioning_entry(table_info['table_name'])
                    if entry:
                        clause, statements, columns = self.repartitioning_sql_for(
                            entry, table_info['table_name'], target_table_name)
                        target_table_sql += clause
                        create_partitions_sql = json.dumps(statements)
                        table_partitioned = True
                        table_partitioned_by = str(entry.get('partition_by') or '').upper()
                        table_partitioning_columns = ', '.join(
                            self.config_parser.convert_names_case(column) for column in columns)
                        self.config_parser.print_log_message(
                            'INFO', f"planner: stdwf_prepare_tables: {table_info['table_name']} is "
                                    f"partitioned by target_partitioning:{clause}, "
                                    f"{len(statements)} partition(s).")

                self.config_parser.print_log_message( 'INFO', f"planner: stdwf_prepare_tables: Counting rows in source table {table_info['table_name']}...")
                self.source_connection.connect()
                ## the whole count is read first - a restriction can carry a row limit, which
                ## decides whether it applies to this table at all
                source_table_rows_all = self.source_connection.get_rows_count(
                    self.source_schema_name,
                    table_info['table_name'],
                    None
                )

                migration_limitation = self.migrator_tables.resolve_data_migration_limitation({
                    'source_schema_name': self.source_schema_name,
                    'source_table_name': table_info['table_name'],
                    'source_columns': source_columns,
                    'source_table_rows_all': source_table_rows_all,
                })

                source_table_rows_limited = source_table_rows_all
                if migration_limitation:
                    source_table_rows_limited = self.source_connection.get_rows_count(
                        self.source_schema_name,
                        table_info['table_name'],
                        migration_limitation
                    )
                    
                self.source_connection.disconnect()
                self.config_parser.print_log_message( 'INFO', f"planner: stdwf_prepare_tables: Source table {table_info['table_name']} has {source_table_rows_all} total rows ({source_table_rows_limited} limited).")

                self.migrator_tables.insert_tables({
                    'source_schema_name': self.source_schema_name,
                    'source_table_name': table_info['table_name'],
                    'source_table_id': table_info['id'],
                    'source_columns': source_columns,
                    'source_table_rows_all': source_table_rows_all,
                    'source_table_rows_limited': source_table_rows_limited,
                    'source_table_description': table_description,
                    'source_table_sql': table_info.get('source_table_sql', ''),
                    'target_schema_name': self.target_schema_name,
                    'target_table_name': target_table_name,
                    'target_alias_name': target_alias_name,
                    'target_columns': target_columns,
                    'target_table_rows': target_table_rows,
                    'target_table_sql': target_table_sql,
                    'table_comment': table_info['comment'],
                    'partitioned': table_partitioned,
                    'partitioned_by': table_partitioned_by,
                    'partitioning_columns': table_partitioning_columns,
                    'create_partitions_sql': create_partitions_sql,
                })

            except Exception as e:
                self.migrator_tables.insert_tables({
                    'source_schema_name': self.source_schema_name,
                    'source_table_name': table_info['table_name'],
                    'source_table_id': table_info['id'],
                    'source_columns': source_columns,
                    'source_table_rows_all': source_table_rows_all if 'source_table_rows_all' in locals() else 0,
                    'source_table_rows_limited': source_table_rows_limited if 'source_table_rows_limited' in locals() else 0,
                    'source_table_description': table_description,
                    'source_table_sql': table_info.get('source_table_sql', ''),
                    'target_schema_name': self.target_schema_name,
                    'target_table_name': target_table_name,
                    'target_alias_name': target_alias_name,
                    'target_columns': target_columns,
                    'target_table_rows': target_table_rows,
                    'target_table_sql': target_table_sql,
                    'table_comment': table_info['comment'],
                    'partitioned': False,
                    'partitioned_by': '',
                    'partitioning_columns': '',
                    'create_partitions_sql': '',
                })
                self.handle_error(e, f"Table {table_info['table_name']}")
                continue

            if self.config_parser.should_migrate_indexes():
                indexes = self.source_connection.fetch_indexes({
                    'source_table_id': table_info['id'],
                    'source_table_name': table_info['table_name'],
                    'source_table_schema': self.source_schema_name,
                    'source_db_type': self.config_parser.get_source_db_type(),
                    'source_db_version': self.config_parser.get_source_db_version(),
                    'target_table_schema': self.target_schema_name,
                    'target_table_name': target_table_name,
                    'target_columns': target_columns,
                })
                self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Indexes: {indexes}")
                if indexes:
                    for _, index_details in indexes.items():
                        values = {}
                        values['source_schema_name'] = self.source_schema_name
                        values['source_table_name'] = table_info['table_name']
                        values['source_table_id'] = table_info['id']
                        values['index_owner'] = index_details['index_owner']
                        values['index_name'] = index_details['index_name']
                        values['index_type'] = index_details['index_type']
                        values['target_schema_name'] = self.target_schema_name
                        values['target_table_name'] = target_table_name
                        values['target_alias_name'] = target_alias_name
                        values['index_columns'] = index_details['index_columns']
                        values['index_comment'] = index_details['index_comment']
                        # Set before the DDL is generated - the target connector needs to know
                        # that the index columns are expressions, and against which columns of
                        # the table their identifiers have to be resolved.
                        values['is_function_based'] = index_details.get('is_function_based', 'NO')
                        # Access method of the source index (gin, gist, hash, brin, ...) -
                        # without it every index would be created as the default btree.
                        values['using_method'] = index_details.get('using_method', '')
                        # INCLUDE columns, NULLS NOT DISTINCT, storage parameters and the
                        # WHERE predicate of a partial index
                        values['index_tail'] = index_details.get('index_tail', '')
                        values['index_sql'] = self.target_connection.get_create_index_sql(
                            {**values, 'target_columns': target_columns,
                             'source_index_sql': index_details.get('index_sql', ''),
                             # Definition of the constraint implemented by the index, when
                             # the object is a constraint rather than a plain index
                             'constraint_def': index_details.get('constraint_def', ''),
                             'user_collations': self.migrated_collations,
                             'text_search_objects': self.migrated_text_search})
                        self.migrator_tables.insert_indexes( values )
                        self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Processed index: {values}")
                else:
                    self.config_parser.print_log_message( 'INFO', f"planner: stdwf_prepare_tables: No indexes found for table {table_info['table_name']}.")
            else:
                self.config_parser.print_log_message( 'INFO', "planner: stdwf_prepare_tables: Skipping index migration.")

            if self.config_parser.should_migrate_constraints():
                constraints = self.source_connection.fetch_constraints({
                    'source_table_id': table_info['id'],
                    'source_table_schema': self.source_schema_name,
                    'source_table_name': table_info['table_name'],
                })
                self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Constraints: {constraints}")
                if constraints:
                    for _, constraint_details in constraints.items():
                        constraint_name = constraint_details['constraint_name'] if 'constraint_name' in constraint_details else ''

                        referenced_table_schema = constraint_details['referenced_table_schema'] if 'referenced_table_schema' in constraint_details else ''
                        referenced_table_name = constraint_details['referenced_table_name'] if 'referenced_table_name' in constraint_details else ''
                        aliased_referenced_table_name = referenced_table_name

                        if referenced_table_name and self.config_parser.get_use_aliases_as_target_names():
                            alias_dict = self.migrator_tables.get_alias_for_table(referenced_table_schema, referenced_table_name)
                            if alias_dict:
                                alias_name = alias_dict.get('target_alias_name')
                                aliased_referenced_table_name = alias_name
                                self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_tables: Constraint referenced table {constraint_details['referenced_table_name']} mapped to target alias {aliased_referenced_table_name}")

                        ## Where the referenced table lands in the TARGET, decided here rather
                        ## than assumed downstream. A migration reads one schema of the source
                        ## and writes one schema of the target, so a reference INSIDE that
                        ## schema - which is what an empty referenced schema means, and most
                        ## connectors report it that way - lands in the target schema. A
                        ## reference to another schema of the source is not part of this
                        ## migration: it is recorded empty, and the worker says so instead of
                        ## building a REFERENCES clause pointing at a table which is not there.
                        target_referenced_table_schema = ''
                        target_referenced_table_name = ''
                        if referenced_table_name:
                            if (not referenced_table_schema
                                    or referenced_table_schema.lower() == self.source_schema_name.lower()):
                                target_referenced_table_schema = self.target_schema_name
                                target_referenced_table_name = self.config_parser.convert_names_case(
                                    aliased_referenced_table_name)
                            else:
                                self.config_parser.print_log_message('WARNING',
                                    f"planner: stdwf_prepare_tables: Constraint {constraint_name} of "
                                    f"{table_info['table_name']} points at "
                                    f"{referenced_table_schema}.{referenced_table_name}, which is not in the "
                                    f"migrated schema {self.source_schema_name} - the migration does not create "
                                    f"that table and the constraint is created only if the target already has it.")

                        target_db_constraint_sql = self.target_connection.get_create_constraint_sql({
                            'source_db_type': self.config_parser.get_source_db_type(),
                            'source_schema_name': self.source_schema_name,
                            'source_table_name': table_info['table_name'],
                            'target_schema_name': self.target_schema_name,
                            'target_table_name': target_table_name,
                            'target_columns': target_columns,
                            'constraint_name': constraint_name,
                            'constraint_type': constraint_details['constraint_type'] if 'constraint_type' in constraint_details else '',
                            'constraint_columns': constraint_details['constraint_columns'] if 'constraint_columns' in constraint_details else '',
                            'referenced_table_schema': referenced_table_schema,
                            'referenced_table_name': aliased_referenced_table_name,
                            ## the schema the REFERENCES clause has to name - see above
                            'target_referenced_table_schema': target_referenced_table_schema,
                            'referenced_columns': constraint_details['referenced_columns'] if 'referenced_columns' in constraint_details else '',
                            'constraint_owner': constraint_details['constraint_owner'] if 'constraint_owner' in constraint_details else '',
                            'constraint_sql': constraint_details['constraint_sql'] if 'constraint_sql' in constraint_details else '',
                            'constraint_comment': constraint_details['constraint_comment'] if 'constraint_comment' in constraint_details else '',
                            'delete_rule': constraint_details['delete_rule'] if 'delete_rule' in constraint_details else '',
                            'update_rule': constraint_details['update_rule'] if 'update_rule' in constraint_details else '',
                            'constraint_status': constraint_details['constraint_status'] if 'constraint_status' in constraint_details else '',
                        })

                        self.migrator_tables.insert_constraint( {
                            'source_table_id': table_info['id'],
                            'source_schema_name': self.source_schema_name,
                            'source_table_name': table_info['table_name'],
                            'target_schema_name': self.target_schema_name,
                            'target_table_name': target_table_name,
                            'target_alias_name': target_alias_name,
                            'constraint_name': constraint_name,
                            'constraint_type': constraint_details['constraint_type'],
                            'constraint_owner': constraint_details['constraint_owner'] if 'constraint_owner' in constraint_details else '',
                            'constraint_columns': constraint_details['constraint_columns'] if 'constraint_columns' in constraint_details else '',
                            'source_referenced_table_schema': referenced_table_schema,
                            'source_referenced_table_name': referenced_table_name,
                            'target_referenced_table_schema': target_referenced_table_schema,
                            'target_referenced_table_name': target_referenced_table_name,
                            'referenced_columns': constraint_details['referenced_columns'] if 'referenced_columns' in constraint_details else '',
                            'delete_rule': constraint_details['delete_rule'] if 'delete_rule' in constraint_details else '',
                            'update_rule': constraint_details['update_rule'] if 'update_rule' in constraint_details else '',
                            'constraint_sql': target_db_constraint_sql,
                            'constraint_comment': constraint_details['constraint_comment'],
                            'constraint_status': constraint_details['constraint_status'] if 'constraint_status' in constraint_details else '',
                            }
                        )
                    self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_tables: Constraint {constraint_name} for table {target_table_name}")
                else:
                    self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_tables: No constraints found for table {table_info['table_name']}.")
            else:
                self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_tables: Skipping constraint migration.")

            if self.config_parser.should_migrate_triggers():
                triggers = self.source_connection.fetch_triggers(table_info['id'], self.source_schema_name, table_info['table_name'])
                self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Number of triggers: {len(triggers) if triggers else 0}, Triggers: {triggers}")
                if triggers:
                    for _, trigger_details in triggers.items():
                        trigger_name = trigger_details['name']

                        ## The target names are handed over already spelled the way
                        ## names_case_handling spells them, so a connector which interpolates
                        ## them into its DDL cannot get it wrong: ms_sql wrote
                        ## CREATE TRIGGER "TR_AuditSales" ... ON "migtest"."SalesOrders" while
                        ## the table is `salesorders`. The connectors which convert them again
                        ## are unharmed - the conversion is idempotent. 'source_*' stays the
                        ## spelling of the source, which is what a connector needs to find
                        ## anything in the code it was given.
                        converted_code = self.source_connection.convert_trigger({
                                'source_schema_name': self.config_parser.get_source_schema(),
                                'source_table_name': table_info['table_name'],
                                'target_schema_name': self.config_parser.get_target_schema(),
                                'target_table_name': self.config_parser.convert_names_case(target_table_name),
                                'trigger_name': self.config_parser.convert_names_case(trigger_name),
                                'trigger_sql': trigger_details['sql'],
                                'table_list': [],
                                'target_db_type': self.config_parser.get_target_db_type(),
                                'migrator_tables': self.migrator_tables,
                            })

                        self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Source trigger code: {trigger_details['sql']}")
                        self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_tables: Converted trigger code: {converted_code}")

                        ## The conversion could not express part of this trigger and left it in
                        ## the code as the source wrote it. The code is stored so that it can be
                        ## completed by hand, and the trigger is kept out of the target - a
                        ## trigger which does less than the trigger of the source did must not be
                        ## created and counted as migrated.
                        requires_manual_adjustment = self.source_connection.trigger_needs_manual_adjustment(converted_code)
                        manual_adjustment_details = None
                        if requires_manual_adjustment:
                            manual_adjustment_details = self.source_connection.trigger_manual_adjustment_details(converted_code)
                            self.config_parser.print_log_message('WARNING',
                                f"planner: stdwf_prepare_tables: Trigger {trigger_name} of table "
                                f"{table_info['table_name']} could not be converted completely and will NOT be "
                                f"created in the target - it is reported as failed and its code is stored for "
                                f"the migration by hand: {manual_adjustment_details}")

                        self.migrator_tables.insert_trigger({
                            'source_schema_name': self.source_schema_name,
                            'source_table_name': table_info['table_name'],
                            'source_table_id': table_info['id'],
                            'target_schema_name': self.target_schema_name,
                            'target_table_name': table_info['table_name'],
                            'trigger_id': trigger_details['id'],
                            'trigger_name': trigger_details['name'],
                            'trigger_event': trigger_details['event'],
                            'trigger_new': trigger_details['new'],
                            'trigger_old': trigger_details['old'],
                            'trigger_source_sql': trigger_details['sql'],
                            'trigger_target_sql': converted_code,
                            'trigger_comment': trigger_details['comment'],
                            'requires_manual_adjustment': requires_manual_adjustment,
                            'manual_adjustment_details': manual_adjustment_details,
                        })
                    self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_tables: Trigger {trigger_details['name']} for table {table_info['table_name']}")
                else:
                    self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_tables: No triggers found for table {table_info['table_name']}.")
            else:
                self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_tables: Skipping trigger migration.")

            self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_tables: Table {table_info['table_name']} processed successfully.")
        self.config_parser.log_object_selection_summary('table', 'planner: stdwf_prepare_tables')
        self.stdwf_ensure_parent_fk_indexes()
        self.stdwf_sync_fk_column_types()

    def stdwf_sync_fk_column_types(self):
        """
        Ensures that child columns in Foreign Key constraints match the resolved
        target data_type of their referenced parent table columns. If a parent column's
        data_type differs (e.g. parent is BIGINT while child is TEXT/INTEGER), the child
        column's data_type and the child table's DDL SQL are updated to match the parent.
        """
        if not self.config_parser.should_migrate_constraints():
            return

        self.config_parser.print_log_message('INFO', "planner: stdwf_sync_fk_column_types: Synchronizing Foreign Key child column data types with parent columns...")

        all_constraints = self.migrator_tables.fetch_all_decoded_constraints()
        if not all_constraints:
            return

        fk_constraints = [c for c in all_constraints if str(c.get('constraint_type', '')).upper() in ('FOREIGN KEY', 'FK')]
        if not fk_constraints:
            return

        all_tables = self.migrator_tables.fetch_all_decoded_tables()
        if not all_tables:
            return

        tables_by_target = {}
        for t in all_tables:
            tgt_name = t.get('target_alias_name') if self.config_parser.get_use_aliases_as_target_names() and t.get('target_alias_name') else t.get('target_table_name')
            tables_by_target[tgt_name] = t
            tables_by_target.setdefault(t.get('target_table_name'), t)

        updated_tables = set()

        def clean_col_list(cols_str):
            if not cols_str:
                return []
            return [c.strip().strip('"').strip("'") for c in str(cols_str).split(',') if c.strip()]

        for fk in fk_constraints:
            ## 'tables_by_target' is keyed by the name of the TARGET, so the referenced table
            ## has to be looked up by the name the target gives it. It was looked up by the
            ## name of the SOURCE, which found nothing whenever names_case_handling changed the
            ## spelling - and a foreign key whose parent is not found is silently left with the
            ## column types of the source on both sides.
            parent_tbl_name = (fk.get('target_referenced_table_name')
                               or self.config_parser.convert_names_case(fk.get('source_referenced_table_name') or ''))
            child_tbl_name = fk.get('target_table_name', '')
            fk_name = fk.get('constraint_name', '')

            parent_table_rec = tables_by_target.get(parent_tbl_name)
            child_table_rec = tables_by_target.get(child_tbl_name)

            if not parent_table_rec or not child_table_rec:
                continue

            parent_cols_json = parent_table_rec.get('target_columns', {})
            child_cols_json = child_table_rec.get('target_columns', {})

            if isinstance(parent_cols_json, str):
                try: parent_cols_json = json.loads(parent_cols_json)
                except Exception: parent_cols_json = {}

            if isinstance(child_cols_json, str):
                try: child_cols_json = json.loads(child_cols_json)
                except Exception: child_cols_json = {}

            parent_col_names = clean_col_list(fk.get('referenced_columns', ''))
            child_col_names = clean_col_list(fk.get('constraint_columns', ''))

            if len(parent_col_names) != len(child_col_names) or not parent_col_names:
                continue

            parent_col_map = {cinfo['column_name'].lower(): cinfo for cinfo in parent_cols_json.values() if isinstance(cinfo, dict) and 'column_name' in cinfo}
            child_col_map = {cinfo['column_name'].lower(): (cid, cinfo) for cid, cinfo in child_cols_json.items() if isinstance(cinfo, dict) and 'column_name' in cinfo}

            child_modified = False
            for p_name, c_name in zip(parent_col_names, child_col_names):
                p_info = parent_col_map.get(p_name.lower())
                c_entry = child_col_map.get(c_name.lower())

                if not p_info or not c_entry:
                    continue

                cid, c_info = c_entry
                parent_type = p_info.get('data_type', '').upper()
                child_type = c_info.get('data_type', '').upper()

                if parent_type and child_type and parent_type != child_type:
                    self.config_parser.print_log_message(
                        'INFO',
                        f"planner: stdwf_sync_fk_column_types: FK '{fk_name}' - Syncing child column '{child_tbl_name}.{c_name}' data_type from '{child_type}' to '{parent_type}' to match parent '{parent_tbl_name}.{p_name}'."
                    )
                    c_info['data_type'] = parent_type
                    c_info['column_type_substitution'] = parent_type
                    child_modified = True

            if child_modified:
                target_columns_json_str = json.dumps(child_cols_json)
                new_table_sql = self.target_connection.get_create_table_sql({
                    'source_schema_name': child_table_rec['source_schema_name'],
                    'source_table_name': child_table_rec['source_table_name'],
                    'source_table_id': child_table_rec['source_table_id'],
                    'target_schema_name': child_table_rec['target_schema_name'],
                    'target_table_name': child_table_rec['target_table_name'],
                    'target_columns': child_cols_json,
                    'migrator_tables': self.migrator_tables
                })
                self.migrator_tables.update_table_target_columns_and_sql(
                    child_table_rec['id'],
                    target_columns_json_str,
                    new_table_sql
                )
                child_table_rec['target_columns'] = child_cols_json
                child_table_rec['target_table_sql'] = new_table_sql
                updated_tables.add(child_tbl_name)

        if updated_tables:
            self.config_parser.print_log_message('INFO', f"planner: stdwf_sync_fk_column_types: Successfully synchronized FK column data types for {len(updated_tables)} table(s): {', '.join(updated_tables)}.")

    def stdwf_ensure_parent_fk_indexes(self):
        """
        Scans all Foreign Key constraints in protocol_constraints across all tables.
        For any Foreign Key referencing a parent table on columns that lack a PRIMARY KEY,
        UNIQUE constraint, or UNIQUE index, automatically generates and inserts a UNIQUE index
        on the parent table into protocol_indexes.
        """
        if not self.config_parser.should_migrate_constraints() or not self.config_parser.should_migrate_indexes():
            return

        self.config_parser.print_log_message('INFO', "planner: stdwf_ensure_parent_fk_indexes: Checking Foreign Key constraints for missing parent table unique indexes...")

        all_constraints = self.migrator_tables.fetch_all_decoded_constraints()
        if not all_constraints:
            return

        fk_constraints = [c for c in all_constraints if str(c.get('constraint_type', '')).upper() in ('FOREIGN KEY', 'FK')]
        if not fk_constraints:
            return

        def normalize_cols(cols_str):
            if not cols_str:
                return ()
            parts = []
            for c in str(cols_str).split(','):
                cleaned = re.sub(r'(?i)\b(ASC|DESC|NULLS\s+FIRST|NULLS\s+LAST)\b', '', c)
                cleaned = cleaned.strip().strip('"').strip("'").lower()
                if cleaned:
                    parts.append(cleaned)
            return tuple(parts)

        table_unique_cols = {}

        all_indexes = self.migrator_tables.fetch_all_decoded_indexes()
        for idx in all_indexes:
            tgt_tbl = idx.get('target_table_name', '')
            idx_type = str(idx.get('index_type', '')).upper()
            idx_cols = normalize_cols(idx.get('index_columns', ''))
            if tgt_tbl and idx_cols:
                if 'UNIQUE' in idx_type or 'PRIMARY' in idx_type:
                    table_unique_cols.setdefault(tgt_tbl, set()).add(idx_cols)

        for c in all_constraints:
            tgt_tbl = c.get('target_table_name', '')
            c_type = str(c.get('constraint_type', '')).upper()
            c_cols = normalize_cols(c.get('constraint_columns', ''))
            if tgt_tbl and c_cols:
                if 'PRIMARY' in c_type or 'UNIQUE' in c_type:
                    table_unique_cols.setdefault(tgt_tbl, set()).add(c_cols)

        added_count = 0
        for fk in fk_constraints:
            ref_tbl = fk.get('source_referenced_table_name', '')
            ref_cols_str = fk.get('referenced_columns', '')
            fk_name = fk.get('constraint_name', '')

            if not ref_tbl or not ref_cols_str:
                continue

            ## the name the referenced table really has in the target. This used to be the
            ## name of the source, so with names_case_handling: lower the lookup in
            ## table_unique_cols - which is keyed by the target name - never matched, and the
            ## index below was created ON a table spelled the way the source spells it.
            ref_tbl_target = fk.get('target_referenced_table_name') or self.config_parser.convert_names_case(ref_tbl)
            if self.config_parser.get_use_aliases_as_target_names():
                ref_schema = fk.get('source_referenced_table_schema', '') or self.source_schema_name
                alias_dict = self.migrator_tables.get_alias_for_table(ref_schema, ref_tbl)
                if alias_dict and alias_dict.get('target_alias_name'):
                    ref_tbl_target = self.config_parser.convert_names_case(alias_dict.get('target_alias_name'))

            norm_ref_cols = normalize_cols(ref_cols_str)
            if not norm_ref_cols:
                continue

            existing_uniques = table_unique_cols.get(ref_tbl_target, set())
            has_matching_unique = False
            for unique_cols in existing_uniques:
                if norm_ref_cols == unique_cols or norm_ref_cols[:len(unique_cols)] == unique_cols:
                    has_matching_unique = True
                    break

            if not has_matching_unique:
                cols_suffix = "_".join(norm_ref_cols)
                idx_name = self.config_parser.convert_names_case(
                    f"idx_fk_parent_{ref_tbl_target}_{cols_suffix}"[:63])

                ## the columns are read from the constraint of the source, so they carry its
                ## spelling - the index is created in the target and has to name them the way
                ## the target has them
                clean_cols = [self.config_parser.convert_names_case(c.strip().strip('"').strip("'"))
                              for c in str(ref_cols_str).split(',')]
                quoted_cols = ", ".join(f'"{c}"' for c in clean_cols if c)
                index_sql = f'CREATE UNIQUE INDEX "{idx_name}" ON "{self.target_schema_name}"."{ref_tbl_target}" ({quoted_cols});'

                index_record = {
                    'source_schema_name': self.source_schema_name,
                    'source_table_name': ref_tbl,
                    'source_table_id': fk.get('source_table_id', 0),
                    'index_owner': '',
                    'index_name': idx_name,
                    'index_type': 'UNIQUE [AUTO-FK]',
                    'target_schema_name': self.target_schema_name,
                    'target_table_name': ref_tbl_target,
                    'target_alias_name': ref_tbl_target if ref_tbl_target != ref_tbl else '',
                    'index_columns': quoted_cols,
                    'index_comment': f'[AUTO-FK-PARENT-INDEX] Unique index added for FK constraint {fk_name}',
                    'is_function_based': False,
                    'index_sql': index_sql
                }

                self.migrator_tables.insert_indexes(index_record)
                table_unique_cols.setdefault(ref_tbl_target, set()).add(norm_ref_cols)
                added_count += 1

                self.config_parser.print_log_message(
                    'INFO',
                    f"planner: stdwf_ensure_parent_fk_indexes: Auto-added UNIQUE index {idx_name} on parent table '{ref_tbl_target}' ({quoted_cols}) for Foreign Key constraint '{fk_name}'."
                )

        if added_count > 0:
            self.config_parser.print_log_message('INFO', f"planner: stdwf_ensure_parent_fk_indexes: Successfully auto-added {added_count} unique index(es) on parent tables for foreign keys.")

    def default_value_pattern_for_function(self, src_func):
        """
        The pattern under which one entry of sql_functions_mapping is offered as a
        substitution of a whole column default.

        A row of default_values_substitution replaces the default of the column entirely, so
        the pattern has to describe a default which IS that function and not one which merely
        contains it. Unanchored, a mapping such as 'suser_name() -> current_user' collapsed a
        default of "'[' + suser_name() + '@' + host_name() + ']'" to the bare 'current_user',
        throwing away the brackets, the '@' and host_name() - and, because these rows carry
        '(?i)' and are preferred by the ORDER BY of the lookup, it did so in front of a
        whole-value substitution the user had written for exactly that default.

        A function inside a larger expression is not the business of this table: every
        connector translates the functions of a default token by token in its own
        convert_default_value() / apply_sql_functions_mapping().

        The parentheses a source writes around a default of its own - MS SQL stores
        '(getdate())' - are part of the default and not of the function, so they are allowed
        around it.
        """
        escaped_src_func = re.escape(str(src_func).strip())
        return rf"(?i)^\(*\s*{escaped_src_func}\s*\)*$"

    def promote_string_type_to_text(self, coltype, character_maximum_length):
        """
        The type a string column gets in the target: itself, or TEXT when the configuration
        asks for the long ones to be migrated as TEXT.

        A varchar column is governed by varchar_to_text_length and a char column by
        char_to_text_length, and by nothing else. 'CHAR' is a substring of 'VARCHAR', so the
        varchar family - univarchar and nvarchar among them, which are mapped to VARCHAR -
        has to be recognised first and kept out of the char branch. Each limit guards its own
        branch too: with only one of the two configured, the other branch compared the length
        against its default of -1, which is true for every column, and turned a whole family
        into TEXT although nothing asked for it.

        A length below zero says the source reports no length for the column at all - a LOB,
        or a type the mapping does not size - and such a column is TEXT whatever is configured.
        """
        if character_maximum_length < 0:
            if self.source_connection.is_string_type(coltype) or any(t in coltype for t in ('CHAR', 'TEXT', 'STRING', 'CLOB', 'VARCHAR')):
                return 'TEXT'
            return coltype

        if not self.source_connection.is_string_type(coltype):
            return coltype

        coltype_upper = coltype.upper()
        varchar_to_text_length = self.config_parser.get_varchar_to_text_length()
        char_to_text_length = self.config_parser.get_char_to_text_length()
        if 'VARCHAR' in coltype_upper:
            if varchar_to_text_length >= 0 and character_maximum_length >= varchar_to_text_length:
                return 'TEXT'
        elif 'CHAR' in coltype_upper:
            if char_to_text_length >= 0 and character_maximum_length >= char_to_text_length:
                return 'TEXT'
        return coltype

    def convert_table_columns(self, settings):
        target_db_type = settings['target_db_type']
        source_db_type = settings['source_db_type']
        source_columns = settings['source_columns']
        types_mapping = {}
        converted = {}
        if target_db_type == 'postgresql':
            if source_db_type != 'postgresql':
                types_mapping = self.source_connection.get_types_mapping(settings)

            for order_num, column_info in source_columns.items():
                if column_info.get('column_type_substitution'):
                    coltype = column_info['column_type_substitution'].upper()
                    character_maximum_length = 0
                    ## we presume substitution contains also length/ precision, scale
                    ## and proper data type, so we can use it directly
                    self.config_parser.print_log_message( 'DEBUG', f"planner: convert_table_columns: Column {column_info['column_name']} - using substitution: {coltype}")
                else:
                    coltype = column_info['data_type'].upper()
                    try:
                        character_maximum_length = int(column_info['character_maximum_length']) if column_info['character_maximum_length'] is not None else 0
                    except (ValueError, TypeError):
                        character_maximum_length = 0
                    # USER-DEFINED marks an object type / VARRAY / nested-table column; keep the
                    # marker (do not run the scalar type mapping, which would collapse it to
                    # TEXT) so the DDL builder emits the composite type / domain via udt_name.
                    if source_db_type != 'postgresql' and coltype != 'USER-DEFINED':
                        if types_mapping.get(coltype, 'UNKNOWN').startswith('UNKNOWN'):
                            self.config_parser.print_log_message('INFO', f"planner: convert_table_columns: Column {column_info['column_name']} - unknown data type: {column_info['data_type']} - checking column_type...")
                            if 'column_type' in column_info and column_info['column_type']:
                                coltype = column_info['column_type'].upper()
                                if types_mapping.get(coltype, 'UNKNOWN').startswith('UNKNOWN'):
                                    self.config_parser.print_log_message('INFO', f"planner: convert_table_columns: Column {column_info['column_name']} - unknown column type: {column_info['column_type']} - checking basic_data_type...")
                                    if 'basic_data_type' in column_info and column_info['basic_data_type']:
                                        coltype = column_info['basic_data_type'].upper()
                                        if types_mapping.get(coltype, 'UNKNOWN').startswith('UNKNOWN'):
                                            self.config_parser.print_log_message('INFO', f"planner: convert_table_columns: Column {column_info['column_name']} - unknown basic data type: {column_info['basic_data_type']} - mapping missing, using TEXT...")
                                            coltype = types_mapping.get(coltype, 'TEXT').upper()
                                        else:
                                            coltype = types_mapping.get(coltype, 'TEXT').upper()
                                    else:
                                        coltype = types_mapping.get(coltype, 'TEXT').upper()
                                else:
                                    coltype = types_mapping.get(coltype, 'TEXT').upper()
                            else:
                                # Nothing to fall back to - the source data type is used in the DDL
                                # unchanged and the target rejects it unless it knows a type of the
                                # same name (`type "bson" does not exist`). Reported as a warning,
                                # because the table is created only later, by a worker.
                                self.config_parser.print_log_message('WARNING', f"planner: convert_table_columns: Column {column_info['column_name']} - data type {column_info['data_type']} has no mapping to the target database and no column type or basic data type to fall back to - it is used unchanged and will fail unless the target knows it. Configure a substitution in 'data_types_substitution' for it.")
                        else:
                            coltype = types_mapping.get(coltype, coltype).upper()

                    coltype = self.promote_string_type_to_text(coltype, character_maximum_length)

                self.config_parser.print_log_message( 'DEBUG', f"planner: convert_table_columns: Column {column_info['column_name']} - using data type: {coltype}")

                converted[order_num] = {
                    'column_name': column_info['column_name'],
                    'is_nullable': column_info['is_nullable'],
                    'column_default_name': column_info['column_default_name'] if 'column_default_name' in column_info else '',
                    'column_default_value': self.source_connection.convert_default_value({'extracted_default_value': column_info['column_default_value'], 'column_type': coltype}) if 'column_default_value' in column_info else '',
                    'replaced_column_default_value': self.source_connection.convert_default_value({'extracted_default_value': column_info['replaced_column_default_value'], 'column_type': coltype}) if 'replaced_column_default_value' in column_info else '',
                    'data_type': coltype,
                    'target_alias_name': settings.get('target_alias_name', ''),
                    'column_type': column_info['column_type'] if 'column_type' in column_info else '',
                    'column_type_substitution': column_info['column_type_substitution'] if 'column_type_substitution' in column_info else '',
                    'character_maximum_length': '' if coltype == 'TEXT' or character_maximum_length < 0 else column_info['character_maximum_length'] if column_info['character_maximum_length'] is not None else '',
                    'numeric_precision': column_info['numeric_precision'] if 'numeric_precision' in column_info else '',
                    'numeric_scale': column_info['numeric_scale'] if 'numeric_scale' in column_info else '',
                    'basic_data_type': column_info['basic_data_type'] if 'basic_data_type' in column_info else '',
                    'basic_character_maximum_length': column_info['basic_character_maximum_length'] if 'basic_character_maximum_length' in column_info else '',
                    'basic_numeric_precision': column_info['basic_numeric_precision'] if 'basic_numeric_precision' in column_info else '',
                    'basic_numeric_scale': column_info['basic_numeric_scale'] if 'basic_numeric_scale' in column_info else '',
                    'basic_column_type': column_info['basic_column_type'].strip() if column_info.get('basic_column_type') else '',
                    'is_identity': column_info['is_identity'],
                    'column_comment': column_info['column_comment'] if 'column_comment' in column_info else '',
                    'is_generated_virtual': column_info['is_generated_virtual'] if 'is_generated_virtual' in column_info else '',
                    'is_generated_stored': column_info['is_generated_stored'] if 'is_generated_stored' in column_info else '',
                    'generation_expression': column_info['generation_expression'] if 'generation_expression' in column_info else '',
                    'udt_schema': column_info['udt_schema'] if 'udt_schema' in column_info else '',
                    'udt_name': column_info['udt_name'] if 'udt_name' in column_info else '',
                    'domain_schema': column_info['domain_schema'] if 'domain_schema' in column_info else '',
                    'domain_name': column_info['domain_name'] if 'domain_name' in column_info else '',
                    'collation_schema': column_info['collation_schema'] if 'collation_schema' in column_info else '',
                    'collation_name': column_info['collation_name'] if 'collation_name' in column_info else '',
                    'is_hidden_column': column_info['is_hidden_column'] if 'is_hidden_column' in column_info else '',
                    'stripped_generation_expression': column_info['stripped_generation_expression'] if 'stripped_generation_expression' in column_info else '',
                }
        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

        return converted

    ## What is checked for a collision, once the plan is written and before anything is
    ## created: the protocol table, the column holding the name of the source, the column
    ## holding the name the target will have, and what the name has to be unique within.
    ## PostgreSQL keeps tables, views, sequences, types, domains and indexes unique per schema,
    ## and constraints, triggers and columns unique per table.
    COLLISION_CHECKS = (
        ('tables',             'source_table_name',      'target_table_name',      ('target_schema_name',),                      'table'),
        ('columns',            'source_column_name',     'target_column_name',     ('target_schema_name', 'target_table_name'),  'column'),
        ('views',              'source_view_name',       'target_view_name',       ('target_schema_name',),                      'view'),
        ('sequences',          'source_sequence_name',   'target_sequence_name',   ('target_schema_name',),                      'sequence'),
        ('user_defined_types', 'source_type_name',       'target_type_name',       ('target_schema_name',),                      'user defined type'),
        ('domains',            'source_domain_name',     'target_domain_name',     ('target_schema_name',),                      'domain'),
        ('collations',         'source_collation_name',  'target_collation_name',  ('target_schema_name',),                      'collation'),
        ('text_search',        'source_object_name',     'target_object_name',     ('target_schema_name',),                      'text search object'),
        ('indexes',            'index_name',             'target_index_name',      ('target_schema_name',),                      'index'),
        ('constraints',        'constraint_name',        'target_constraint_name', ('target_schema_name', 'target_table_name'),  'constraint'),
        ('triggers',           'trigger_name',           'target_trigger_name',    ('target_schema_name', 'target_table_name'),  'trigger'),
    )

    def check_target_name_collisions(self):
        """
        Whether names_case_handling collapses two objects of the source into one on the target.

        Case folding is not injective. A source holding CUSTOMER and Customer is holding two
        different tables; with names_case_handling: lower both of them want to be "customer",
        and the migrator used to notice nothing at all - it dropped "customer" once per table
        in the loop which prepares the target, created it for the first, and answered the
        second with "already exists". What the user saw was one failed table and a message
        which says nothing about the case of a name.

        The check runs when the plan is complete and before anything in the target is dropped
        or created, so a run which cannot come out right stops before it has done anything.
        It reads the protocol tables, which by then hold both spellings of every name - so it
        costs no query against the source.

        With names_case_handling: keep nothing can collapse and the check is skipped.
        """
        case_handling = self.config_parser.get_names_case_handling()
        if case_handling == 'keep':
            self.config_parser.print_log_message(
                'DEBUG', "planner: check_target_name_collisions: names_case_handling is 'keep' - "
                         "no two names of the source can become one name in the target.")
            return

        collisions = []
        for table_key, source_column, target_column, scope_columns, label in self.COLLISION_CHECKS:
            protocol_table = getattr(self.config_parser, f'get_protocol_name_{table_key}')()
            scope_list = ', '.join(f'"{column}"' for column in scope_columns)
            query = f'''
                SELECT {scope_list}, "{target_column}",
                       string_agg(DISTINCT "{source_column}", ', ' ORDER BY "{source_column}")
                FROM "{self.migrator_tables.protocol_schema}"."{protocol_table}"
                WHERE "{target_column}" IS NOT NULL AND "{target_column}" <> ''
                GROUP BY {scope_list}, "{target_column}"
                HAVING count(DISTINCT "{source_column}") > 1
            '''
            try:
                cursor = self.migrator_tables.protocol_connection.connection.cursor()
                cursor.execute(query)
                rows = cursor.fetchall()
                cursor.close()
            except Exception as e:
                ## a protocol table which does not exist means that kind of object was not
                ## planned - it is not a reason to stop, and it is not passed over silently
                self.config_parser.print_log_message(
                    'DEBUG', f"planner: check_target_name_collisions: {protocol_table} could not "
                             f"be read ({e}) - no {label} was checked.")
                continue
            for row in rows:
                ## every part of the name on its own quotes, the way the target is addressed:
                ## "migtest"."orders"."total" and not "migtest.orders"."total"
                parts = [str(value) for value in row[:len(scope_columns)] if value]
                parts.append(str(row[len(scope_columns)]))
                where = '.'.join(f'"{part}"' for part in parts)
                collisions.append(f"{label}s {row[-1]} of the source all become {where}")

        if not collisions:
            self.config_parser.print_log_message(
                'INFO', f"planner: check_target_name_collisions: names_case_handling is "
                        f"'{case_handling}' and no two names of the source become one in the target.")
            return

        listed = '\n  - '.join(collisions)
        raise ValueError(
            f"names_case_handling is '{case_handling}', and it would make one target object out "
            f"of two or more different objects of the source:\n  - {listed}\n"
            f"The source tells them apart by the case of their letters and the target would not. "
            f"Nothing has been created or dropped in the target - the run stops here rather than "
            f"dropping the same object twice and reporting the second one as 'already exists'. "
            f"Use names_case_handling: keep, or rename the objects which clash.")

    def convert_view_identifier_case(self, converted_view_sql, source_view_name):
        """
        The identifiers of a converted view, spelled the way names_case_handling made the
        objects they name.

        A statement which cannot be read as PostgreSQL is answered exactly as it came in and
        reported: the conversion of a view is allowed to fail, and a view whose text no parser
        understands is one the user has to look at anyway. What must not happen is a guess -
        a name changed by a search and replace inside a text nobody could parse would be the
        kind of quiet damage this migrator treats as a bug.
        """
        if not converted_view_sql or not converted_view_sql.strip():
            return converted_view_sql
        converted, ok = identifier_case.convert_identifiers(
            converted_view_sql,
            self.config_parser.convert_names_case,
            self.config_parser.get_source_db_type())
        if not ok:
            self.config_parser.print_log_message(
                'WARNING', f"planner: stdwf_prepare_views: the converted query of view "
                           f"{source_view_name} could not be read as PostgreSQL, so the case of "
                           f"the names in it was left as the conversion wrote it. With "
                           f"names_case_handling: {self.config_parser.get_names_case_handling()} "
                           f"it may name objects which are spelled differently in the target.")
            return converted_view_sql
        return converted

    def stdwf_prepare_views(self):
        self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_views: Preparing views...")
        # if self.source_db_config.get('connectivity') == 'ddl':
        #     self.config_parser.print_log_message('DEBUG', "planner: stdwf_prepare_views: skipping source db fetch for views due to DDL connectivity")
        #     return
        if self.config_parser.should_migrate_views():
            self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_views: Processing views...")
            views = self.source_connection.fetch_views_names(self.source_schema_name)

            include_views = self.config_parser.get_include_views()
            exclude_views = self.config_parser.get_exclude_views() or []

            self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_views: Source views: {views}")
            self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_views: Include views: {include_views}")
            self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_views: Exclude views: {exclude_views}")

            for order_num, view_info in views.items():
                self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_views: Processing view ({order_num}): {view_info}")
                if not self.config_parser.report_object_selection(
                        'view', view_info['view_name'], 'planner: stdwf_prepare_views'):
                    continue
                self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_views: View {view_info['view_name']} is included for migration.")
                target_view_name = view_info.get('target_view_name', view_info['view_name'])
                target_alias_name = ''
                if self.config_parser.get_use_aliases_as_target_names():
                    alias_dict = self.migrator_tables.get_alias_for_table(self.source_schema_name, view_info['view_name'])
                    if alias_dict:
                        alias_name = alias_dict.get('target_alias_name')
                        target_view_name = alias_name
                        target_alias_name = alias_name
                        self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_views: View {view_info['view_name']} mapped to target alias {target_alias_name}")
                        if 'id' in alias_dict:
                            self.migrator_tables.update_aliases_status({
                                'row_id': alias_dict['id'],
                                'success': True,
                                'message': f"Alias used as target name for view {view_info['view_name']}"
                            })

                view_sql = self.source_connection.fetch_view_code({
                    'view_id': view_info['id'],
                    'source_schema_name': self.config_parser.get_source_schema(),
                    'source_view_name': view_info['view_name'],
                    'target_schema_name': view_info.get('target_schema_name', ''),
                    'target_view_name': view_info.get('target_view_name', ''),
                })
                self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_views: Source view SQL data: {view_info}")
                self.config_parser.print_log_message( 'DEBUG3', f"planner: stdwf_prepare_views: Source view SQL: {view_sql}")
                converted_view_sql = self.source_connection.convert_view_code({
                    'view_code': view_sql,
                    'source_database': self.config_parser.get_source_db_name(),
                    'source_schema_name': self.config_parser.get_source_schema(),
                    'target_schema_name': self.config_parser.get_target_schema(),
                    'target_db_type': self.config_parser.get_target_db_type(),
                    'target_view_name': self.config_parser.convert_names_case(target_view_name), # Pass name
                    'view_type': view_info.get('view_type', 'VIEW'), # Pass type
                    'migrator_tables': self.migrator_tables,
                    'alias_view': view_info.get('is_alias', False),
                    'text_search_objects': self.migrated_text_search,
                })
                self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_views: Converted view SQL: {converted_view_sql}")

                ## one mechanism, which records what it replaced. This runs AFTER the
                ## conversion while the connectors run their pass before it - the two stages
                ## are unchanged here, and the record now shows when both fire on one view.
                converted_view_sql, _ = self.source_connection.apply_remote_objects_substitution(
                    converted_view_sql, 'view', target_view_name)

                ## The names inside the query, spelled the way the target has them. Three of
                ## the twelve connectors did this themselves and nine did not - ms_sql and
                ## sybase_ase wrote the identifiers of the source in double quotes, so a view
                ## of a migration with `lower` asked for "CUSTOMERS" while the table is
                ## `customers`, and the other seven wrote them bare, which is right for
                ## `lower` and wrong for `upper` and for `keep` over a mixed case source. It
                ## is one transformation for all of them now - see identifier_case.py.
                converted_view_sql = self.convert_view_identifier_case(
                    converted_view_sql, view_info['view_name'])

                self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_views: Converted view SQL: {converted_view_sql}")
                self.migrator_tables.insert_view({
                    'source_schema_name': self.source_schema_name,
                    'source_view_name': view_info['view_name'],
                    'source_view_id': view_info['id'],
                    'source_view_sql': view_sql,
                    'target_schema_name': self.target_schema_name,
                    'target_view_name': self.config_parser.convert_names_case(target_view_name),
                    'target_view_alias': self.config_parser.convert_names_case(target_alias_name) if target_alias_name else '',
                    'target_view_sql': converted_view_sql,
                    'alias_view': view_info.get('is_alias', False),
                    'view_comment': view_info['comment']
                })
                self.config_parser.print_log_message( 'INFO', f"planner: stdwf_prepare_views: View {view_info['view_name']} processed successfully.")
            self.config_parser.log_object_selection_summary('view', 'planner: stdwf_prepare_views')
            self.config_parser.print_log_message( 'INFO', "planner: stdwf_prepare_views: Views processed successfully.")
        else:
            self.config_parser.print_log_message( 'INFO', "planner: stdwf_prepare_views: Skipping views migration.")
        self.config_parser.print_log_message( 'INFO', "planner: stdwf_prepare_views: Views processed successfully.")

    def stdwf_prepare_aliases(self):
        self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_aliases: Preparing aliases...")
        self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_aliases: Processing aliases...")

        try:
            aliases = self.source_connection.get_aliases({'source_schema_name': self.source_schema_name})
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"planner: stdwf_prepare_aliases: Cannot fetch aliases: {e}")
            aliases = {}

        if aliases:
            self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_aliases: Source aliases count: {len(aliases)}")
            for order_num, alias_info in aliases.items():
                self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_aliases: Processing alias ({order_num}): {alias_info.get('alias_name')}")

                self.migrator_tables.insert_aliases({
                    'source_schema_name': self.source_schema_name,
                    'source_alias_name': alias_info.get('alias_name', ''),
                    'source_alias_id': alias_info.get('id', 0),
                    'source_alias_sql': alias_info.get('alias_sql', ''),
                    'source_referenced_schema_name': alias_info.get('aliased_schema_name', ''),
                    'source_referenced_table_name': alias_info.get('aliased_table_name', ''),
                    'source_referenced_column_name': alias_info.get('aliased_column_name', ''),
                    'source_alias_comment': alias_info.get('alias_comment', ''),
                    'target_schema_name': self.target_schema_name,
                    'target_alias_name': self.config_parser.convert_names_case(alias_info.get('alias_name', '')),
                    'alias_target_type': alias_info.get('alias_target_type', 'UNKNOWN'),
                    'target_referenced_schema_name': self.config_parser.convert_names_case(alias_info.get('aliased_schema_name', '')),
                    'target_referenced_table_name': self.config_parser.convert_names_case(alias_info.get('aliased_table_name', '')),
                    'target_referenced_column_name': self.config_parser.convert_names_case(alias_info.get('aliased_column_name', '')),
                    'target_alias_sql': '' # PostgreSQL does not implement pure aliases
                })
                self.config_parser.print_log_message( 'INFO', f"planner: stdwf_prepare_aliases: Alias {alias_info.get('alias_name')} processed successfully.")
        else:
            self.config_parser.print_log_message( 'INFO', "planner: stdwf_prepare_aliases: No aliases found.")

        self.config_parser.print_log_message( 'INFO', "planner: stdwf_prepare_aliases: Aliases processing completed.")

    def report_kind_not_read(self, kind, singular, phase):
        """
        Say that a kind of object was not read, where the source has such objects.

        A fetch which answers {} says "the source holds none of these", and several connectors
        answered that for objects their sources certainly do hold. The planner then wrote
        "No user defined types found" and the summary showed 0 - and a reader who takes the
        summary at its word migrates a schema which is missing the objects nobody said were
        missing. P2-8 of development/OPEN_ISSUES.md.

        Answers True when the kind was not read, so the caller can say the other thing when it
        really was. The note is written into the journal of the run as well, with the row type
        `not read`, which is where the summary picks it up.
        """
        what_is_there = self.source_connection.object_kind_not_read(kind)
        if not what_is_there:
            return False
        message = (f"planner: {phase}: {kind.replace('_', ' ')} were NOT READ from this source. "
                   f"This is not the same as the source having none: {what_is_there} Whatever "
                   f"is there has to be migrated by hand.")
        self.config_parser.print_log_message('WARNING', message)
        try:
            self.migrator_tables.insert_protocol({
                'object_type': singular,
                'object_name': f'({kind.replace("_", " ")} were not read)',
                'object_action': 'not read',
                'object_ddl': None,
                'execution_timestamp': None,
                'execution_success': None,
                'execution_error_message': what_is_there,
                'row_type': 'not read',
                'execution_results': None,
                'object_protocol_id': None,
            })
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"planner: {phase}: the note that {kind} were not read could not be written into the protocol: {e}")
        return True

    def stdwf_prepare_user_defined_types(self):
        self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_user_defined_types: Preparing user defined types...")
        # if self.source_db_config.get('connectivity') == 'ddl':
        #     self.config_parser.print_log_message('DEBUG', "planner: stdwf_prepare_user_defined_types: skipping source db fetch for user defined types due to DDL connectivity")
        #     return
        user_defined_types = self.source_connection.fetch_user_defined_types(self.source_schema_name)

        # Get types mapping for type conversion
        settings = {'target_db_type': self.config_parser.get_target_db_type()}
        types_mapping = self.source_connection.get_types_mapping(settings)
        # Create case-insensitive mapping
        types_mapping = {k.lower(): v for k, v in types_mapping.items()}

        self.config_parser.print_log_message('DEBUG', f"planner: stdwf_prepare_user_defined_types: User defined types: {user_defined_types}")

        if user_defined_types:
            for order_num, type_info in user_defined_types.items():
                type_name = type_info['type_name']
                base_type = type_info.get('base_type', '')
                length = type_info.get('length', '')
                prec = type_info.get('prec', '')
                scale = type_info.get('scale', '')
                source_type_sql = type_info['sql']

                self.config_parser.print_log_message('DEBUG', f"planner: stdwf_prepare_user_defined_types: Source type: {type_name}, Base: {base_type}")

                # Resolve target type
                base_lower = base_type.lower()
                target_base_type = types_mapping.get(base_lower, base_type.upper()).upper()

                # Construct definition part
                # Check for types that usually don't need length/prec in PG
                no_length_types = ('BOOLEAN', 'BOOL', 'TEXT', 'BYTEA', 'DATE', 'TIMESTAMP', 'TIME', 'INTEGER', 'BIGINT', 'SMALLINT', 'Double Precision')

                definition = target_base_type
                if target_base_type not in no_length_types:
                    if base_lower in ('varchar', 'char', 'nvarchar', 'nchar', 'varbinary', 'binary', 'univarchar', 'unichar'):
                         definition += f"({length})"
                    elif base_lower in ('numeric', 'decimal'):
                         definition += f"({prec},{scale})"

                # Construct DDL: CREATE DOMAIN "target_schema_name"."type_name" AS definition;
                # Note: IF NOT EXISTS is not standard for CREATE DOMAIN in all versions, but we can try exception handling or standard CREATE
                # Using simple CREATE DOMAIN as insert_user_defined_type expects SQL to execute.
                # However, planner usually prepares 'target_type_sql' which is then executed.

                # Using 'AS' syntax for domains
                if definition:
                    target_type_sql = f'CREATE DOMAIN "{self.target_schema_name}"."{type_name}" AS {definition};'
                else:
                    target_type_sql = source_type_sql.replace(f'"{type_info.get("schema_name", self.source_schema_name)}".', f'"{self.target_schema_name}".') if source_type_sql else ''

                self.config_parser.print_log_message('DEBUG', f"planner: stdwf_prepare_user_defined_types: Converted type SQL: {target_type_sql}")

                self.migrator_tables.insert_user_defined_type({
                    'source_schema_name': self.source_schema_name,
                    'source_type_name': type_name,
                    'source_type_sql': source_type_sql,
                    'target_schema_name': self.target_schema_name,
                    'target_type_name': type_name,
                    'target_type_sql': target_type_sql,
                    'target_basic_type': target_base_type,
                    'type_comment': type_info['comment'],
                })
                self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_user_defined_types: User defined type {type_name} processed successfully.")
            self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_user_defined_types: User defined types processed successfully.")
        elif not self.report_kind_not_read('user_defined_types', 'user_defined_type',
                                           'stdwf_prepare_user_defined_types'):
            self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_user_defined_types: No user defined types found in the source.")

    def stdwf_prepare_collations(self):
        """
        Collations have to be prepared as the first objects - tables, columns and indexes
        reference them, and the generated DDL must point to the collations recreated in the
        target schema.
        """
        self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_collations: Preparing collations...")
        self.migrated_collations = {}
        collations = self.source_connection.fetch_collations(self.source_schema_name)
        self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_collations: Collations found in source database: {collations}")
        if not collations:
            self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_collations: No collations found.")
            return

        for order_num, collation_info in collations.items():
            self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_collations: Processing collation: {collation_info}")
            target_collation_name = self.config_parser.convert_names_case(collation_info['collation_name'])
            collation_info['target_schema_name'] = self.target_schema_name
            collation_info['target_collation_name'] = target_collation_name
            converted_collation_sql = self.target_connection.get_create_collation_sql(collation_info)
            self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_collations: Converted collation SQL: {converted_collation_sql}")
            if not converted_collation_sql:
                self.config_parser.print_log_message('WARNING', f"planner: stdwf_prepare_collations: Collation {collation_info['collation_name']} cannot be recreated in the target database - skipped.")
                continue

            self.migrator_tables.insert_collation({
                'source_schema_name': collation_info.get('collation_schema') or self.source_schema_name,
                'source_collation_name': collation_info['collation_name'],
                'source_collation_sql': collation_info.get('source_collation_sql', ''),
                'target_schema_name': self.target_schema_name,
                'target_collation_name': target_collation_name,
                'target_collation_sql': converted_collation_sql,
                'collation_provider': collation_info.get('collation_provider', ''),
                'collation_comment': collation_info.get('collation_comment'),
            })
            self.migrated_collations[collation_info['collation_name']] = {
                'target_schema_name': self.target_schema_name,
                'target_collation_name': target_collation_name,
            }
            self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_collations: Collation {collation_info['collation_name']} processed successfully.")
        self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_collations: Collations processed successfully.")

    def stdwf_prepare_text_search(self):
        """
        Full text search dictionaries and configurations have to be prepared before tables -
        a generated tsvector column references a configuration, and so do views, indexes and
        functions.
        """
        self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_text_search: Preparing full text search objects...")
        self.migrated_text_search = {}
        text_search_objects = self.source_connection.fetch_text_search_objects(self.source_schema_name)
        self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_text_search: Text search objects found in source database: {text_search_objects}")
        if not text_search_objects:
            self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_text_search: No full text search objects found.")
            return

        for order_num, object_info in text_search_objects.items():
            self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_text_search: Processing text search object: {object_info}")
            target_object_name = self.config_parser.convert_names_case(object_info['object_name'])
            object_info['target_schema_name'] = self.target_schema_name
            object_info['target_object_name'] = target_object_name
            converted_sql = self.target_connection.get_create_text_search_sql(object_info)
            self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_text_search: Converted text search SQL: {converted_sql}")
            if not converted_sql:
                self.config_parser.print_log_message('WARNING', f"planner: stdwf_prepare_text_search: Text search object {object_info['object_name']} cannot be recreated in the target database - skipped.")
                continue

            self.migrator_tables.insert_text_search({
                'source_schema_name': object_info.get('object_schema') or self.source_schema_name,
                'source_object_name': object_info['object_name'],
                'source_object_sql': object_info.get('source_object_sql', ''),
                'target_schema_name': self.target_schema_name,
                'target_object_name': target_object_name,
                'target_object_sql': converted_sql,
                'object_type': object_info.get('object_type', ''),
                'object_comment': object_info.get('object_comment'),
            })
            self.migrated_text_search[object_info['object_name']] = {
                'target_schema_name': self.target_schema_name,
                'target_object_name': target_object_name,
                'object_type': object_info.get('object_type', ''),
            }
            self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_text_search: Text search {object_info.get('object_type', 'object').lower()} {object_info['object_name']} processed successfully.")
        self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_text_search: Full text search objects processed successfully.")

    def stdwf_prepare_domains(self):
        self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_domains: Preparing domains...")
        # if self.source_db_config.get('connectivity') == 'ddl':
        #     self.config_parser.print_log_message('DEBUG', "planner: stdwf_prepare_domains: skipping source db fetch for domains due to DDL connectivity")
        #     return
        migrated_as = 'CHECK CONSTRAINT'
        if self.config_parser.get_target_db_type() == 'postgresql':
            migrated_as = 'DOMAIN'
        domains = self.source_connection.fetch_domains(self.source_schema_name)
        self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_domains: Domains found in source database: {domains}")
        if domains:
            for order_num, domain_info in domains.items():
                self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_domains: Processing domain: {domain_info}")
                domain_info['target_schema_name'] = self.target_schema_name
                domain_info['migrated_as'] = migrated_as
                converted_domain_sql = self.target_connection.get_create_domain_sql(domain_info)
                self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_domains: Converted domain SQL: {converted_domain_sql}")

                # If the source domain SQL contains 'CREATE RULE', set 'migrated_as' accordingly
                self.migrator_tables.insert_domain({
                    'source_schema_name': domain_info['domain_schema'] if 'domain_schema' in domain_info and domain_info['domain_schema'] is not None else self.source_schema_name,
                    'source_domain_name': domain_info['domain_name'],
                    'source_domain_sql': domain_info['source_domain_sql'],
                    'source_domain_check_sql': domain_info['source_domain_check_sql'] if 'source_domain_check_sql' in domain_info and domain_info['source_domain_check_sql'] is not None else '',
                    'target_schema_name': self.target_schema_name,
                    'target_domain_name': domain_info['domain_name'],
                    'target_domain_sql': converted_domain_sql,
                    'migrated_as': migrated_as,
                    'domain_comment':  domain_info['domain_comment'],
                })
                self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_domains: Domain {domain_info['domain_name']} processed successfully.")
            self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_domains: Domains processed successfully.")
        else:
            if not self.report_kind_not_read('domains', 'domain', 'stdwf_prepare_domains'):
                self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_domains: No domains found in the source.")

    def stdwf_prepare_defaults(self):
        self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_defaults: Preparing defaults...")
        # if self.source_db_config.get('connectivity') == 'ddl':
        #     self.config_parser.print_log_message('DEBUG', "planner: stdwf_prepare_defaults: skipping source db fetch for defaults due to DDL connectivity")
        #     return
        defaults = self.source_connection.fetch_default_values({ 'source_schema_name': self.source_schema_name})
        if defaults:
            self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_defaults: Defaults found in source database: {defaults}")
            for order_num, default_info in defaults.items():
                self.config_parser.print_log_message( 'DEBUG', f"planner: stdwf_prepare_defaults: Processing default: {default_info}")

                self.migrator_tables.insert_default_value({
                    'default_value_schema': default_info['default_value_schema'],
                    'default_value_name': default_info['default_value_name'],
                    'default_value_sql': default_info['default_value_sql'],
                    'extracted_default_value': default_info['extracted_default_value'],
                    'default_value_data_type': default_info['default_value_data_type'] if 'default_value_data_type' in default_info else '',
                    'default_value_comment':  default_info['default_value_comment'] if 'default_value_comment' in default_info else '',
                })
                self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_defaults: Default {default_info['default_value_name']} processed successfully.")
            self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_defaults: Defaults processed successfully.")
        else:
            self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_defaults: No defaults found.")

    def run_pre_migration_script(self):
        pre_migration_script = self.config_parser.get_pre_migration_script()
        if pre_migration_script:
            self.config_parser.print_log_message('INFO', f"planner: run_pre_migration_script: Running pre-migration script '{pre_migration_script}' in target database.")
            try:
                self.target_connection.connect()
                self.target_connection.execute_sql_script(pre_migration_script)
                self.target_connection.disconnect()
                self.config_parser.print_log_message('INFO', "planner: run_pre_migration_script: Pre-migration script executed successfully.")
            except Exception as e:
                self.handle_error(e, "Pre-migration script")
        else:
            self.config_parser.print_log_message('INFO', "planner: run_pre_migration_script: No pre-migration script specified.")

    def check_script_accessibility(self, script_path):
        if not script_path:
            return
        if not os.path.isfile(script_path):
            self.config_parser.print_log_message('ERROR', f"planner: check_script_accessibility: Script {script_path} does not exist or is not accessible.")
            if self.config_parser.get_on_error_action() == 'stop':
                self.config_parser.print_log_message('ERROR', "planner: check_script_accessibility: Stopping execution due to error.")
                exit(1)
        self.config_parser.print_log_message('INFO', f"planner: check_script_accessibility: Script {script_path} is accessible.")

    def check_database_connection(self, connector, db_name):
        if db_name == "Source Database" and self.source_db_config.get('connectivity') == 'ddl':
            self.config_parser.print_log_message('DEBUG', f"planner: check_database_connection: Skipping connection check for {db_name} due to DDL connectivity.")
            return

        try:
            connector.connect()
            cursor = connector.connection.cursor()
            query = connector.testing_select()
            cursor.execute(query)
            result = cursor.fetchone()
            if result[0] != 1:
                raise ConnectionError(f"Connection to {db_name} failed.")
            self.config_parser.print_log_message('INFO', f"planner: check_database_connection: Connection to {db_name} is OK.")
            cursor.close()
            connector.disconnect()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"planner: check_database_connection: Failed to connect to {db_name}: {e}")
            self.config_parser.print_log_message('ERROR', traceback.format_exc())
            exit(1)

    def handle_error(self, e, description=None):
        self.config_parser.print_log_message('ERROR', f"planner: handle_error: An error in {self.__class__.__name__} ({description}): {e}")
        self.config_parser.print_log_message('ERROR', traceback.format_exc())
        if self.on_error_action == 'stop':
            self.config_parser.print_log_message('ERROR', "planner: handle_error: Stopping due to error.")
            exit(1)
        else:
            self.config_parser.print_log_message('WARNING', f"planner: handle_error: Error caught, but continuing as requested by configuration (on_error_action='{self.on_error_action}').")

    def check_pausing_resuming(self):
        if self.config_parser.pause_migration_fired():
            self.config_parser.print_log_message('INFO', f"planner: check_pausing_resuming: paused. Waiting for resume signal...")
            self.config_parser.wait_for_resume()
            self.config_parser.print_log_message('INFO', f"planner: check_pausing_resuming: resumed.")

    def run_check_tables_migration_status(self):
        self.config_parser.print_log_message('INFO', "planner: run_check_tables_migration_status: Resume: Checking tables migration status...")

        try:
            part_name = 'fetch_all_tables'
            tables = self.migrator_tables.fetch_all_tables()
            self.source_connection.connect()
            self.target_connection.connect()
            self.config_parser.print_log_message('DEBUG', f"planner: run_check_tables_migration_status: Fetched all tables - found: {len(tables)}")
            for table in tables:
                table_info = self.migrator_tables.decode_table_row(table)
                part_name = 'fetch data migrations for table ' + table_info['source_table_name']
                self.config_parser.print_log_message('DEBUG', f"planner: run_check_tables_migration_status: Checking migration status for table {table_info['source_table_name']}...")
                data_migration_rows = self.migrator_tables.fetch_all_data_migrations({'source_schema_name': table_info['source_schema_name'], 'source_table_name': table_info['source_table_name']})
                self.config_parser.print_log_message('DEBUG', f"planner: run_check_tables_migration_status: Data migration rows for table {table_info['source_table_name']}: {data_migration_rows}")
                for record in data_migration_rows:
                    data_migration_info = self.migrator_tables.decode_data_migration_row(record)

                    part_name = 'check row counts for table ' + data_migration_info['source_table_name']
                    if self.config_parser.get_source_db_type() in ('ibm_db2_zos', 'ibm_db2_i'):
                        source_table_rows_all = data_migration_info.get('source_table_rows_all', 0)
                        source_table_rows_limited = data_migration_info.get('source_table_rows_limited', 0)
                    else:
                        source_table_rows_all = self.source_connection.get_rows_count(
                            data_migration_info['source_schema_name'],
                            data_migration_info['source_table_name'],
                            None
                        )
                        migration_limitation = self.migrator_tables.resolve_data_migration_limitation({
                            'source_schema_name': data_migration_info['source_schema_name'],
                            'source_table_name': data_migration_info['source_table_name'],
                            'source_columns': table_info.get('source_columns'),
                            'source_table_rows_all': source_table_rows_all,
                        })
                        source_table_rows_limited = source_table_rows_all
                        if migration_limitation:
                            source_table_rows_limited = self.source_connection.get_rows_count(
                                data_migration_info['source_schema_name'],
                                data_migration_info['source_table_name'],
                                migration_limitation
                            )
                    target_table_rows = self.target_connection.get_rows_count(
                        data_migration_info['target_schema_name'],
                        data_migration_info['target_table_name']
                    )
                    self.config_parser.print_log_message('DEBUG', f"planner: run_check_tables_migration_status: Row counts for table {data_migration_info['source_table_name']}: source={source_table_rows_limited}, target={target_table_rows}")

                    if source_table_rows_limited != target_table_rows:
                        self.config_parser.print_log_message('INFO', f"planner: run_check_tables_migration_status: Row counts do not match for table {data_migration_info['source_table_name']}: source={source_table_rows_limited}, target={target_table_rows}. Marking as not fully migrated.")
                        self.migrator_tables.update_table_status({'row_id': table_info['id'], 'success': False, 'message': ''})
                        self.migrator_tables.update_table_rows_counts({
                            "row_id": table_info['id'],
                            "source_table_rows_all": source_table_rows_all,
                            "source_table_rows_limited": source_table_rows_limited,
                            "target_table_rows": target_table_rows,
                        })
                        self.migrator_tables.update_data_migration_rows({
                            "row_id": data_migration_info['id'],
                            "source_table_rows_all": source_table_rows_all,
                            "source_table_rows_limited": source_table_rows_limited,
                            "target_table_rows": target_table_rows,
                        } )
                        self.migrator_tables.update_data_migration_status({
                            "row_id": data_migration_info['id'],
                            "success": False,
                            "message": '',
                            'target_table_rows': target_table_rows,
                        })
                    else:
                        self.config_parser.print_log_message('DEBUG', f"planner: run_check_tables_migration_status: Row counts match for table {data_migration_info['source_table_name']}: source={source_table_rows_limited}, target={target_table_rows}. Marking as fully migrated.")
                        self.migrator_tables.update_table_status({'row_id': table_info['id'], 'success': True, 'message': 'Fully migrated'})
                        self.migrator_tables.update_table_rows_counts({
                            "row_id": table_info['id'],
                            "source_table_rows_all": source_table_rows_all,
                            "source_table_rows_limited": source_table_rows_limited,
                            "target_table_rows": target_table_rows,
                        })
                        self.migrator_tables.update_data_migration_rows({
                            "row_id": data_migration_info['id'],
                            "source_table_rows_all": source_table_rows_all,
                            "source_table_rows_limited": source_table_rows_limited,
                            "target_table_rows": target_table_rows,
                        } )
                        self.migrator_tables.update_data_migration_status({
                            "row_id": data_migration_info['id'],
                            "success": True,
                            "message": 'Fully migrated',
                            'target_table_rows': target_table_rows,
                        })

            self.config_parser.print_log_message('INFO', "planner: run_check_tables_migration_status: Resume: Tables migration status check completed.")
            self.source_connection.disconnect()
            self.target_connection.disconnect()

        except Exception as e:
            self.source_connection.disconnect()
            self.target_connection.disconnect()
            self.config_parser.print_log_message('ERROR', f"planner: run_check_tables_migration_status: An error occurred while checking tables migration status - part: {part_name}: {e}")
            self.config_parser.print_log_message('ERROR', traceback.format_exc())
            if self.on_error_action == 'stop':
                self.config_parser.print_log_message('ERROR', "planner: run_check_tables_migration_status: Stopping due to error.")
                exit(1)

    def stdwf_prepare_data_sources(self):
        self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_data_sources: Preparing data sources...")

        data_export = self.config_parser.get_source_data_export()

        if not data_export:
            self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_data_sources: No settings for database export found. Migrator will use source tables as data sources.")
            return
        self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_data_sources: Using database export: {data_export}")

        if data_export['format'] in ('CSV', 'UNL'):
            for table in self.migrator_tables.fetch_all_tables():
                self.config_parser.print_log_message('DEBUG', f"planner: stdwf_prepare_data_sources: Processing table: {table}")
                settings_source = 'global'
                table_info = self.migrator_tables.decode_table_row(table)
                table_data_export = self.config_parser.get_table_data_export(table_info['source_schema_name'], table_info['source_table_name'])
                if table_data_export:
                    settings_source = 'table_specific'
                    self.config_parser.print_log_message('DEBUG', f"planner: stdwf_prepare_data_sources: Table {table_info['source_table_name']} has specific database export settings: {table_data_export}")

                file_name = data_export.get('file', None)
                if table_data_export and 'file' in table_data_export:
                    file_name = table_data_export['file']

                if file_name:
                    def replace_placeholders(text, replacements, case_mode='configured'):
                        """
                        Replaces the placeholders of a data file name by their values. With
                        case_mode 'configured' the value follows the case in which the
                        placeholder is written in the configuration, the other modes use the
                        value as it is, in upper case or in lower case - the export tool decides
                        how the files are named, which the configuration cannot always express.
                        """
                        for placeholder, value in replacements.items():
                            if value is None:
                                continue
                            def replacer(match, replacement=value):
                                if case_mode == 'asis':
                                    return replacement
                                if case_mode == 'upper':
                                    return replacement.upper()
                                if case_mode == 'lower':
                                    return replacement.lower()
                                return replacement.upper() if match.group(0).isupper() else replacement.lower()
                            text = re.sub(re.escape(placeholder), replacer, text, flags=re.IGNORECASE)
                        return text

                    def resolve_file_name(replacements):
                        """Returns the file name which exists, trying the case of the values as
                        they are, in upper case and in lower case, or None when none of them does."""
                        for case_mode in ('configured', 'asis', 'upper', 'lower'):
                            candidate_file_name = replace_placeholders(file_name, replacements, case_mode)
                            if '{{' not in candidate_file_name and os.path.exists(candidate_file_name):
                                return candidate_file_name
                        return None

                    table_replacements = {
                        '{{source_schema_name}}': table_info['source_schema_name'],
                        '{{source_table_name}}': table_info['source_table_name'],
                    }
                    table_file_name = replace_placeholders(file_name, table_replacements)

                    resolved_file_name = resolve_file_name(table_replacements)
                    if resolved_file_name:
                        table_file_name = resolved_file_name
                        data_file_found = True
                    else:
                        if re.search(re.escape('{{source_alias_name}}'), table_file_name, flags=re.IGNORECASE):
                            valid_alias_name = table_info['source_table_name']
                            aliases = self.migrator_tables.fetch_all_aliases({'source_schema_name': table_info['source_schema_name']})
                            # self.config_parser.print_log_message('DEBUG3', f"planner: stdwf_prepare_data_sources: Aliases found: {aliases}")
                            # A table can have several aliases (on IBM i also its short system
                            # name) - every one of them is tried against the template, which must
                            # therefore not be overwritten before a matching file was found.
                            for row in aliases:
                                alias_info = self.migrator_tables.decode_aliases_row(row)
                                # self.config_parser.print_log_message('DEBUG3', f"planner: stdwf_prepare_data_sources: Processing alias: {alias_info}")
                                ref_schema = alias_info.get('source_referenced_schema_name') or ''
                                ref_table = alias_info.get('source_referenced_table_name') or ''
                                if ref_schema and ref_table:
                                    if ((ref_table == table_info['source_table_name'].lower() or ref_table == table_info['source_table_name'].upper()) and
                                        (ref_schema == table_info['source_schema_name'].lower() or ref_schema == table_info['source_schema_name'].upper())):
                                        valid_alias_name = alias_info['source_alias_name']
                                        alias_replacements = dict(table_replacements)
                                        alias_replacements['{{source_alias_name}}'] = valid_alias_name
                                        candidate_file_name = resolve_file_name(alias_replacements)
                                        self.config_parser.print_log_message('DEBUG3', f"planner: stdwf_prepare_data_sources: Testing alias {valid_alias_name} of table {table_info['source_table_name']} - data source file name {candidate_file_name or replace_placeholders(file_name, alias_replacements)}")
                                        if candidate_file_name:
                                            table_file_name = candidate_file_name
                                            break

                    if os.path.exists(table_file_name):
                        self.config_parser.print_log_message('INFO', f"planner: stdwf_prepare_data_sources: Testing data source file name - {table_file_name} exists.")
                        data_file_found = True
                    else:
                        self.config_parser.print_log_message('WARNING', f"planner: stdwf_prepare_data_sources: Testing data source file name - {table_file_name} does not exist or is not accessible.")
                        if '{{' in table_file_name or '}}' in table_file_name:
                            self.config_parser.print_log_message('WARNING', f"planner: stdwf_prepare_data_sources: Data source file name {table_file_name} contains placeholder(s) - value was most likely not found for replacement.")
                        data_file_found = False
                        if self.config_parser.get_source_data_export_on_missing_data_file() == 'error':
                            self.config_parser.print_log_message('ERROR', f"planner: stdwf_prepare_data_sources: Data source file {table_file_name} does not exist or is not accessible. Stopping execution.")
                            exit(1)

                    conversion_path = self.config_parser.get_source_data_export_conversion_path()
                    if table_data_export and 'conversion_path' in table_data_export:
                        conversion_path = self.config_parser.get_table_data_export_conversion_path(table_info['source_schema_name'], table_info['source_table_name'])

                    converted_file_name = os.path.join(
                        conversion_path,
                        re.sub(r'(?i)(\.csv)+$', '.csv', os.path.basename(table_file_name) + ".csv")
                    )

                    header = data_export.get('header', False)
                    if table_data_export and 'header' in table_data_export:
                        header = table_data_export['header']

                    format = data_export.get('format', None)
                    if table_data_export and 'format' in table_data_export:
                        format = table_data_export['format']

                    delimiter = data_export.get('delimiter', '|')
                    if table_data_export and 'delimiter' in table_data_export:
                        delimiter = table_data_export['delimiter']

                    character_set = data_export.get('character_set', 'UTF-8')
                    if table_data_export and 'character_set' in table_data_export:
                        character_set = table_data_export['character_set']

                    ## the order of the parts of a date in the file - the file itself does
                    ## not state it, and a wrong reading of '01/04/22' migrates a different
                    ## date. When nothing is configured it is worked out from the values.
                    date_format = data_export.get('date_format', None)
                    if table_data_export and 'date_format' in table_data_export:
                        date_format = table_data_export['date_format']
                    ## a name which is not one of the known formats stops the run here,
                    ## while it can still be corrected, instead of at the first date
                    self.config_parser.date_format_to_order(date_format)

                    self.config_parser.print_log_message('DEBUG3',f"planner: stdwf_prepare_data_sources: Table {table_info['source_table_name']} - file_name: {table_file_name}, converted_file_name: {converted_file_name}, data_file_found: {data_file_found}, format: {format}, delimiter: {delimiter}, header: {header}, character_set: {character_set}, date_format: {date_format}")
                    data_source = {
                        'source_schema_name': table_info['source_schema_name'],
                        'source_table_name': table_info['source_table_name'],
                        'source_table_id': table_info['id'],
                        'file_name': table_file_name,
                        'file_size': os.path.getsize(table_file_name) if data_file_found else -1,
                        'file_lines': None, ## count of lines was too slow - sum(1 for _ in open(table_file_name, 'r', encoding='utf-8')) if data_file_found else -1,
                        'file_found': data_file_found,
                        'lob_columns': self.config_parser.get_table_lob_columns(table_info['source_schema_name'], table_info['source_table_name'], table_info['source_columns']) if table_info else '',
                        'converted_file_name': converted_file_name,
                        'format_options': {
                            'settings_source': settings_source,
                            'format': format,
                            'delimiter': delimiter,
                            'header': header,
                            'character_set': character_set,
                            'date_format': date_format,
                        }
                    }
                    self.migrator_tables.insert_data_source(data_source)
                    self.config_parser.print_log_message('DEBUG', f"planner: stdwf_prepare_data_sources: Table {table_info['source_table_name']} - inserted data source: {data_source}")

        elif data_export['format'] == 'SQL':
            if self.config_parser.get_source_db_type() not in ('informix',):
                self.config_parser.print_log_message('ERROR', f"planner: stdwf_prepare_data_sources: SQL data source is NOT supported for source database {self.config_parser.get_source_db_type()}")
                exit(1)
            sql_file = data_export.get('file', None)
            if not sql_file:
                self.config_parser.print_log_message('ERROR', f"planner: stdwf_prepare_data_sources: SQL dump file is not specified.")
                exit(1)
            if not os.path.exists(sql_file):
                self.config_parser.print_log_message('ERROR', f"planner: stdwf_prepare_data_sources: SQL dump file {sql_file} does not exist or is not accessible.")
                exit(1)

            sql_dump_path = os.path.abspath(sql_file)
            with open(sql_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            table_re = re.compile(r'^\{\s*TABLE\s+"?([\w\d_]+)"?\."?([\w\d_]+)"?')
            unload_re = re.compile(r'^\{\s*unload file name\s*=\s*([^\s]+)')

            i = 0
            while i < len(lines):
                table_match = table_re.match(lines[i].strip())
                if table_match:
                    schema = table_match.group(1)
                    table = table_match.group(2)
                    # Look for the next unload line
                    j = i + 1
                    while j < len(lines):
                        unload_match = unload_re.match(lines[j].strip())
                        if unload_match:
                            file_name = unload_match.group(1)

                            unl_dump_file = os.path.join(os.path.dirname(sql_dump_path), file_name)
                            data_file_found = True
                            if not os.path.exists(unl_dump_file):
                                self.config_parser.print_log_message('ERROR', f"planner: stdwf_prepare_data_sources: UNL dump file {unl_dump_file} for table {schema}.{table} does not exist or is not accessible.")
                                data_file_found = False

                            converted_file_name = os.path.join(
                                self.config_parser.get_source_data_export_conversion_path(),
                                file_name + ".csv"
                            )

                            table_info = self.migrator_tables.fetch_table({'source_schema_name': schema, 'source_table_name': table})
                            # dump might contain tables that are not in protocol
                            # But we still want to insert data source for them for debugging purposes
                            if table_info:
                                table_id = table_info['id']
                            else:
                                table_id = None

                            data_source = {
                                'source_schema_name': schema,
                                'source_table_name': table,
                                'source_table_id': table_id,
                                'file_name': unl_dump_file,
                                'file_size': os.path.getsize(unl_dump_file) if data_file_found else -1,
                                'file_lines': sum(1 for _ in open(unl_dump_file, 'r', encoding='utf-8')) if data_file_found else -1,
                                'file_found': data_file_found,
                                'lob_columns': self.config_parser.get_table_lob_columns(table_info['source_schema_name'], table_info['source_table_name'], table_info['source_columns']) if table_info else '',
                                'converted_file_name': converted_file_name,
                                'format_options': {
                                    'format': 'UNL',
                                    'delimiter': data_export.get('delimiter', '|'),
                                    'header': False
                                }
                            }
                            self.migrator_tables.insert_data_source(data_source)
                            self.config_parser.print_log_message('DEBUG', f"planner: stdwf_prepare_data_sources: Table {schema}.{table} data source: {data_source}")

                            break
                        # Stop if another { TABLE is found before { unload
                        if lines[j].strip().startswith('{ TABLE'):
                            break
                        j += 1
                    i = j
                else:
                    i += 1

        self.config_parser.print_log_message('INFO', "planner: stdwf_prepare_data_sources: Data sources prepared successfully.")

    def mapping_match_tables(self):
        self.config_parser.print_log_message('INFO', "planner: mapping_match_tables: Matching tables...")
        from credativ_pg_migrator.connectors import match_schemas
        import json

        source_tables_raw = self.source_connection.fetch_table_names(self.source_schema_name)
        target_tables_raw = self.target_connection.fetch_table_names(self.target_schema_name)

        source_tables = [v['table_name'] for v in source_tables_raw.values()]
        target_tables = [v['table_name'] for v in target_tables_raw.values()]

        self.config_parser.print_log_message('INFO', f"planner: mapping_match_tables: source_tables: {source_tables}")
        self.config_parser.print_log_message('INFO', f"planner: mapping_match_tables: target_tables: {target_tables}")

        self.migrator_tables.insert_mapping_pre_stat('source', 'tables', len(source_tables))
        self.migrator_tables.insert_mapping_pre_stat('target', 'tables', len(target_tables))
        self.migrator_tables.insert_mapping_pre_stat('source', 'indexes', self.source_connection.get_schema_indexes_count(self.source_schema_name))
        self.migrator_tables.insert_mapping_pre_stat('target', 'indexes', self.target_connection.get_schema_indexes_count(self.target_schema_name))
        self.migrator_tables.insert_mapping_pre_stat('source', 'constraints', self.source_connection.get_schema_constraints_count(self.source_schema_name))
        self.migrator_tables.insert_mapping_pre_stat('target', 'constraints', self.target_connection.get_schema_constraints_count(self.target_schema_name))

        source_columns_map = {}
        target_columns_map = {}
        source_cols_raw = {}
        target_cols_raw = {}

        self.config_parser.print_log_message('INFO', "planner: mapping_match_tables: Fetching source/target metadata...")
        for _, t in source_tables_raw.items():
            self.config_parser.print_log_message('DEBUG3', f"planner: mapping_match_tables: Fetching columns for source table: {t['table_name']}")
            cols = self.source_connection.fetch_table_columns({'table_schema': self.source_schema_name, 'table_name': t['table_name']})
            source_cols_raw[t['table_name']] = cols
            source_columns_map[t['table_name']] = [{'name': c['column_name'], **c} for c in cols.values()]

        for _, t in target_tables_raw.items():
            self.config_parser.print_log_message('DEBUG3', f"planner: mapping_match_tables: Fetching columns for target table: {t['table_name']}")
            cols = self.target_connection.fetch_table_columns({'table_schema': self.target_schema_name, 'table_name': t['table_name']})
            target_cols_raw[t['table_name']] = cols
            target_columns_map[t['table_name']] = [{'name': c['column_name'], **c} for c in cols.values()]

        heuristics = self.config_parser.get_mapping_workflow_heuristics()
        migration_settings = self.config_parser.get_migration_settings()
        
        settings = {
            'config_parser': self.config_parser,
            'source_tables': source_tables,
            'target_tables': target_tables,
            'source_internal': {},
            'target_internal': {},
            'source_columns_map': source_columns_map,
            'target_columns_map': target_columns_map,
            'column_prefixes': heuristics.get('column_prefixes_to_strip', migration_settings.get('column_prefixes', ["gov_", "log_"])),
            'table_normalization_rules': heuristics.get('table_normalization_rules', migration_settings.get('table_normalization_rules', ['lowercase', 'strip_trailing_numbers'])),
            'column_normalization_rules': heuristics.get('column_normalization_rules', migration_settings.get('column_normalization_rules', ['lowercase', 'strip_trailing_numbers'])),
            'normalization_settings': heuristics.get('normalization_settings', migration_settings.get('normalization_settings', {}))
        }

        internal_mappings_table = self.config_parser.get_migration_settings().get('internal_mappings_table')

        if internal_mappings_table:
            try:
                query = f"SELECT name, table_name, column_name FROM {self.source_schema_name}.{internal_mappings_table}"
                self.config_parser.print_log_message('DEBUG', f"planner: mapping_match_tables: Fetching source internal mappings using query: {query}")
                self.source_connection.connect()
                cursor = self.source_connection.connection.cursor()
                cursor.execute(query)
                for row in cursor.fetchall():
                    prop_name = row[0].lower() if row[0] else None
                    t_name = row[1].lower() if row[1] else None
                    c_name = row[2].lower() if row[2] else None
                    if prop_name and t_name and c_name:
                        settings['source_internal'][prop_name] = f"{t_name}.{c_name}"
                cursor.close()
                self.source_connection.disconnect()
                self.config_parser.print_log_message('DEBUG', f"planner: mapping_match_tables: Loaded {len(settings['source_internal'])} source internal mapping properties.")
            except Exception as e:
                self.config_parser.print_log_message('DEBUG', f"planner: mapping_match_tables: Failed to fetch source internal mappings: {e}")

        if internal_mappings_table:
            try:
                query = f"SELECT name, table_name, column_name FROM {self.target_schema_name}.{internal_mappings_table}"
                self.config_parser.print_log_message('DEBUG', f"planner: mapping_match_tables: Fetching target internal mappings using query: {query}")
                self.target_connection.connect()
                cursor = self.target_connection.connection.cursor()
                cursor.execute(query)
                for row in cursor.fetchall():
                    prop_name = row[0].lower() if row[0] else None
                    t_name = row[1].lower() if row[1] else None
                    c_name = row[2].lower() if row[2] else None
                    if prop_name and t_name and c_name:
                        settings['target_internal'][prop_name] = f"{t_name}.{c_name}"
                cursor.close()
                self.target_connection.disconnect()
                self.config_parser.print_log_message('DEBUG', f"planner: mapping_match_tables: Loaded {len(settings['target_internal'])} target internal mapping properties.")
            except Exception as e:
                self.config_parser.print_log_message('DEBUG', f"planner: mapping_match_tables: Failed to fetch target internal mappings: {e}")

        forced_mappings = self.config_parser.get_forced_table_mappings()
        forced_pairs = []
        
        if forced_mappings:
            import re
            for f in forced_mappings:
                if 'source' in f and 'target' in f:
                    src = f['source']
                    tgt = f['target']
                    if src in source_tables and tgt in target_tables:
                        source_cols = source_columns_map.get(src, [])
                        target_cols = target_columns_map.get(tgt, [])
                        jaccard = match_schemas.calculate_enhanced_jaccard(source_cols, target_cols, heuristics.get('column_prefixes_to_strip', migration_settings.get('column_prefixes', ["gov_", "log_"])), heuristics.get('column_normalization_rules', migration_settings.get('column_normalization_rules', ['lowercase', 'strip_trailing_numbers'])), heuristics.get('normalization_settings', migration_settings.get('normalization_settings', {})))
                        score = int(jaccard * 100)
                        forced_pairs.append({'source_table': src, 'target_table': tgt, 'method': 'Forced Exact', 'details': f"Mapped explicitly to {tgt}", 'stats': {'jaccard': jaccard}, 'score': score, 'is_forced_mapping': True})
                elif 'source_regex' in f and 'target' in f:
                    src_re = re.compile(f['source_regex'])
                    for src in list(source_tables):
                        if src_re.match(src):
                            tgt = src_re.sub(f['target'], src)
                            if tgt in target_tables:
                                source_cols = source_columns_map.get(src, [])
                                target_cols = target_columns_map.get(tgt, [])
                                jaccard = match_schemas.calculate_enhanced_jaccard(source_cols, target_cols, heuristics.get('column_prefixes_to_strip', migration_settings.get('column_prefixes', ["gov_", "log_"])), heuristics.get('column_normalization_rules', migration_settings.get('column_normalization_rules', ['lowercase', 'strip_trailing_numbers'])), heuristics.get('normalization_settings', migration_settings.get('normalization_settings', {})))
                                score = int(jaccard * 100)
                                forced_pairs.append({'source_table': src, 'target_table': tgt, 'method': 'Forced Regex Sub', 'details': f"Mapped via regex {f['source_regex']}", 'stats': {'jaccard': jaccard}, 'score': score, 'is_forced_mapping': True})

        for pair in forced_pairs:
            if pair['source_table'] in source_tables:
                source_tables.remove(pair['source_table'])
            if pair['target_table'] in target_tables:
                target_tables.remove(pair['target_table'])
                
        settings['source_tables'] = source_tables
        settings['target_tables'] = target_tables

        match_result = match_schemas.match_tables(settings)
        match_result['matched_pairs'] = forced_pairs + match_result.get('matched_pairs', [])
        
        self.config_parser.print_log_message('INFO', f"planner: mapping_match_tables: Found {len(match_result['matched_pairs'])} matched tables ({len(forced_pairs)} forced).")

        import difflib
        
        def get_col_match_stats(cols1, cols2):
            rules = settings.get('column_normalization_rules')
            norm_settings = settings.get('normalization_settings')
            names1 = set(match_schemas.normalize_name(c.get('name', ''), rules, norm_settings) for c in cols1)
            names2 = set(match_schemas.normalize_name(c.get('name', ''), rules, norm_settings) for c in cols2)
            return len(names1), len(names2), len(names1.intersection(names2))

        unmatched_objs = []
        for t in match_result.get('unmatched_source', []):
            try:
                self.source_connection.connect()
                rows = self.source_connection.get_rows_count(self.source_schema_name, t, None)
                self.source_connection.disconnect()
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"planner: mapping_match_tables: Failed to fetch row count for unmapped source table {t}: {e}")
                rows = -1
                
            similarities = []
            for target_t in target_tables:
                ratio = difflib.SequenceMatcher(None, t.lower(), target_t.lower()).ratio()
                similarities.append((ratio, target_t))
            similarities.sort(reverse=True)
            
            top_5 = []
            for ratio, target_t in similarities[:5]:
                len_src, len_tgt, intersection = get_col_match_stats(
                    source_columns_map.get(t, []), 
                    target_columns_map.get(target_t, [])
                )
                top_5.append(f"{target_t} (name match: {ratio*100:.1f}%, cols match: {intersection} [src: {len_src}, tgt: {len_tgt}])")
                
            info_json = json.dumps({'top_5_suggestions': top_5})
            
            unmatched_objs.append({'object_type': 'table', 'side': 'source', 'object_name': t, 'row_count': rows, 'info': info_json})
            
        for t in match_result.get('unmatched_target', []):
            try:
                self.target_connection.connect()
                rows = self.target_connection.get_rows_count(self.target_schema_name, t, None)
                self.target_connection.disconnect()
            except Exception as e:
                self.config_parser.print_log_message('ERROR', f"planner: mapping_match_tables: Failed to fetch row count for unmapped target table {t}: {e}")
                rows = -1
                
            similarities = []
            for source_t in source_tables:
                ratio = difflib.SequenceMatcher(None, t.lower(), source_t.lower()).ratio()
                similarities.append((ratio, source_t))
            similarities.sort(reverse=True)
            
            top_5 = []
            for ratio, source_t in similarities[:5]:
                len_tgt, len_src, intersection = get_col_match_stats(
                    target_columns_map.get(t, []), 
                    source_columns_map.get(source_t, [])
                )
                top_5.append(f"{source_t} (name match: {ratio*100:.1f}%, cols match: {intersection} [src: {len_src}, tgt: {len_tgt}])")
                
            info_json = json.dumps({'top_5_suggestions': top_5})
                
            unmatched_objs.append({'object_type': 'table', 'side': 'target', 'object_name': t, 'row_count': rows, 'info': info_json})
        target_to_source_table = {pair['target_table']: pair['source_table'] for pair in match_result['matched_pairs']}
        target_col_to_source_col = {}

        for pair in match_result['matched_pairs']:
            source_t = pair['source_table']
            target_t = pair['target_table']
            self.config_parser.print_log_message('DEBUG', f"planner: mapping_match_tables: Processing paired tables '{source_t}' -> '{target_t}' (method: {pair['method']})")

            info_json = json.dumps({
                'details': pair['details'],
                'evidence': pair.get('evidence', []),
                'stats': pair.get('stats', {})
            })

            # Define variables for the new structure
            source_schema_name = self.source_schema_name
            target_schema_name = self.target_schema_name
            mapped_table = pair # Assuming 'pair' itself represents the mapped table info

            self.source_connection.connect()
            source_table_rows_all = self.source_connection.get_rows_count(
                source_schema_name,
                source_t,
                None
            )

            migration_limitation = self.migrator_tables.resolve_data_migration_limitation({
                'source_schema_name': source_schema_name,
                'source_table_name': source_t,
                'source_columns': source_columns_map.get(source_t, []),
                'source_table_rows_all': source_table_rows_all,
            })
            
            source_table_rows_limited = source_table_rows_all
            if migration_limitation:
                source_table_rows_limited = self.source_connection.get_rows_count(
                    source_schema_name,
                    source_t,
                    migration_limitation
                )
            self.source_connection.disconnect()

            self.target_connection.connect()
            target_table_rows = self.target_connection.get_rows_count(
                target_schema_name,
                target_t
            )
            self.target_connection.disconnect()

            self.migrator_tables.insert_mapping_tables({
                'source_schema_name': source_schema_name,
                'source_table_name': source_t, # Use source_t from the loop
                'target_schema_name': target_schema_name,
                'target_table_name': target_t, # Use target_t from the loop
                'match_type': mapped_table['method'], # Use 'method' from 'pair'
                'similarity_score': mapped_table.get('score', 0.0), # Use 'score' from 'pair'
                'source_table_rows_all': source_table_rows_all,
                'source_table_rows_limited': source_table_rows_limited,
                'target_table_rows': target_table_rows,
                'info': info_json, # Use the already prepared info_json
                'is_forced_mapping': mapped_table.get('is_forced_mapping', False)
            })

            col_settings = {
                'config_parser': self.config_parser,
                'source_columns': source_columns_map[source_t],
                'target_columns': target_columns_map[target_t],
                'column_prefixes': settings['column_prefixes'],
                'column_normalization_rules': settings['column_normalization_rules'],
                'normalization_settings': settings['normalization_settings']
            }
            col_match_res = match_schemas.match_columns(col_settings)
            self.config_parser.print_log_message('DEBUG', f"planner: mapping_match_tables: Matched {len(col_match_res['matched_columns'])} columns for pair '{source_t}' -> '{target_t}'")

            for c in col_match_res.get('unmatched_source', []):
                unmatched_objs.append({'object_type': 'column', 'side': 'source', 'parent_object': source_t, 'object_name': c.get('name', '')})
            for c in col_match_res.get('unmatched_target', []):
                unmatched_objs.append({'object_type': 'column', 'side': 'target', 'parent_object': target_t, 'object_name': c.get('name', '')})

            source_columns_dict = {}
            target_columns_dict = {}

            for idx, cpair in enumerate(col_match_res['matched_columns']):
                source_c = cpair['source_column']
                target_c = cpair['target_column']

                # Define variables for the new structure
                source_column_name = source_c['name']
                target_column_name = target_c['name']
                source_col = source_c
                target_col = target_c
                match_type = cpair['method']

                target_col_to_source_col[(target_t, target_column_name)] = source_column_name

                self.migrator_tables.insert_mapping_columns({
                    'source_schema_name': source_schema_name,
                    'source_table_name': source_t, # Use source_t from the outer loop
                    'source_column_name': source_column_name,
                    'target_schema_name': target_schema_name,
                    'target_table_name': target_t, # Use target_t from the outer loop
                    'target_column_name': target_column_name,
                    'source_ordinal_number': source_col.get('ordinal_position', 0) if source_col else 0,
                    'target_ordinal_number': target_col.get('ordinal_position', 0) if target_col else 0,
                    'source_data_type': source_col.get('data_type', '') if source_col else '',
                    'target_data_type': target_col.get('data_type', '') if target_col else '',
                    'match_type': match_type,
                    'source_is_identity': source_col.get('is_identity') in ('YES', True) if source_col else False,
                    'target_is_identity': target_col.get('is_identity') in ('YES', True) if target_col else False
                })

                source_columns_dict[idx] = source_c
                target_columns_dict[idx] = target_c

            source_t_info = next((v for v in source_tables_raw.values() if v['table_name'] == source_t), {})

            self.config_parser.print_log_message('DEBUG3', f"planner: mapping_match_tables: Fetching source rows count for '{source_t}'")
            self.source_connection.connect()
            self.source_connection.connect()
            migration_limitation = self.migrator_tables.resolve_data_migration_limitation({
                'source_schema_name': self.source_schema_name,
                'source_table_name': source_t,
                'source_columns': source_columns_map.get(source_t, []),
                'source_table_rows_all': source_table_rows_all,
            })
            
            # Since this section only seems to be re-fetching or is redundant for the log message,
            # we'll fetch just limited for the message or rely on what's available
            source_table_rows_limited = self.source_connection.get_rows_count(self.source_schema_name, source_t, migration_limitation)
            self.source_connection.disconnect()

            self.config_parser.print_log_message('DEBUG3', f"planner: mapping_match_tables: Fetching target rows count for '{target_t}'")
            self.target_connection.connect()
            target_table_rows = self.target_connection.get_rows_count(self.target_schema_name, target_t)
            self.target_connection.disconnect()



            self.migrator_tables.insert_tables({
                'source_schema_name': self.source_schema_name,
                'source_table_name': source_t,
                'source_table_id': source_t_info.get('id', source_t),
                'source_columns': source_columns_dict,
                'source_table_rows_all': source_table_rows_all,
                'source_table_rows_limited': source_table_rows_limited,
                'source_table_description': '',
                'source_table_sql': getattr(source_t_info, 'source_table_sql', ''),
                'target_schema_name': self.target_schema_name,
                'target_table_name': target_t,
                'target_alias_name': '',
                'target_columns': target_columns_dict,
                'target_table_rows': target_table_rows,
                'target_table_sql': '',
                'table_comment': source_t_info.get('comment', ''),
                'partitioned': False,
                'partitioned_by': '',
                'partitioning_columns': '',
                'create_partitions_sql': ''
            })

        self.migrator_tables.insert_mapping_unmatched_objects(unmatched_objs)

        if self.config_parser.get_target_db_type() == 'postgresql':
            self.config_parser.print_log_message('INFO', "planner: mapping_match_tables: Fetching target indexes, constraints and sequences for all target tables")
            self.target_connection.connect()
            if self.config_parser.get_source_db_type() == 'postgresql':
                self.source_connection.connect()

            for _, target_table_info in target_tables_raw.items():
                target_t = target_table_info['table_name']
                self.config_parser.print_log_message('DEBUG', f"planner: mapping_match_tables: Fetching target indexes, constraints and sequences for PG table '{target_t}'")
                target_indexes = self.target_connection.fetch_mapping_target_indexes(self.target_schema_name, target_t)
                for idx_info in target_indexes:
                    self.migrator_tables.insert_mapping_target_indexes({
                        'target_schema_name': self.target_schema_name,
                        'target_table_name': target_t,
                        'index_name': idx_info['index_name'],
                        'index_def': idx_info['index_def'],
                        'is_primary_key': idx_info['is_primary_key'],
                        'index_type': idx_info.get('index_type', 'UNKNOWN')
                    })

                target_constraints = self.target_connection.fetch_mapping_target_constraints(self.target_schema_name, target_t)
                for col_info in target_constraints:
                    self.migrator_tables.insert_mapping_target_constraints({
                        'target_schema_name': self.target_schema_name,
                        'target_table_name': target_t,
                        'constraint_name': col_info['constraint_name'],
                        'constraint_type': col_info['constraint_type'],
                        'constraint_def': col_info['constraint_def']
                    })

                source_t = target_to_source_table.get(target_t)
                source_sequences = []
                if source_t and self.config_parser.get_source_db_type() == 'postgresql':
                    source_sequences = self.source_connection.fetch_mapping_target_sequences(self.source_schema_name, source_t)

                target_sequences = self.target_connection.fetch_mapping_target_sequences(self.target_schema_name, target_t)
                for seq_info in target_sequences:
                    source_sequence_schema_name = None
                    source_sequence_name = None

                    if seq_info.get('used_in_identity') and seq_info.get('column_name'):
                        target_col = seq_info['column_name']
                        source_col = target_col_to_source_col.get((target_t, target_col))
                        if source_col:
                            for s_seq in source_sequences:
                                if s_seq.get('used_in_identity') and s_seq.get('column_name') == source_col:
                                    source_sequence_schema_name = s_seq.get('sequence_schema_name')
                                    source_sequence_name = s_seq.get('sequence_name')
                                    break

                    self.migrator_tables.insert_mapping_target_sequences({
                        'target_schema_name': self.target_schema_name,
                        'target_table_name': target_t,
                        'sequence_schema_name': seq_info['sequence_schema_name'],
                        'sequence_name': seq_info['sequence_name'],
                        'used_in_default': seq_info['used_in_default'],
                        'used_in_identity': seq_info['used_in_identity'],
                        'used_in_trigger': seq_info['used_in_trigger'],
                        'trigger_name': seq_info['trigger_name'],
                        'column_name': seq_info['column_name'],
                        'source_sequence_schema_name': source_sequence_schema_name,
                        'source_sequence_name': source_sequence_name
                    })
            if self.config_parser.get_source_db_type() == 'postgresql':
                self.source_connection.disconnect()
            self.target_connection.disconnect()

if __name__ == "__main__":
    print("This script is not meant to be run directly")
