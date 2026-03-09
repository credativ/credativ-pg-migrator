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
            try:
                self.pre_planning()

                self.check_pausing_resuming()

                self.run_premigration_analysis()

                self.check_pausing_resuming()

                if self.source_db_config['connectivity'] == 'ddl':
                    self.config_parser.print_log_message('DEBUG3', f"planner: create_plan: starting ddl connectivity")
                    self.source_connection.parse_ddl_files({ 'migrator_tables': self.migrator_tables})
                    self.source_schema_name = self.config_parser.get_source_schema()

                self.run_prepare_user_defined_types()
                self.run_prepare_domains()
                self.run_prepare_defaults()

                self.check_pausing_resuming()

                self.run_prepare_aliases()
                self.run_prepare_tables()
                self.run_prepare_data_sources()

                self.check_pausing_resuming()

                self.run_prepare_views()

                self.check_pausing_resuming()

                self.migrator_tables.update_main_status({'task_name': 'Planner', 'subtask_name': '', 'success': True, 'message': 'finished OK'})

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
                self.migrator_tables.update_main_status({'task_name': 'Planner', 'subtask_name': '', 'success': False, 'message': f'ERROR: {e}'})
                self.handle_error(e, "Planner")

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

            self.target_connection.connect()
            if self.target_connection.session_settings:
                self.config_parser.print_log_message( 'DEBUG', f"planner: pre_planning: CREATE SCHEMA - Executing session settings for target database: {self.target_connection.session_settings}")
                self.target_connection.execute_query(self.target_connection.session_settings)

            if self.config_parser.should_drop_schema():
                if self.target_schema_name.lower() == 'public':
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
                    # Escape parentheses in src_func for regex usage
                    escaped_src_func = re.escape(src_func)
                    self.migrator_tables.insert_default_values_substitution({
                        'column_name': '',
                        'source_column_data_type': '',
                        'default_value_value': rf"(?i){escaped_src_func}",
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
            self.config_parser.print_log_message('DEBUG', "planner: run_premigration_analysis: skipping source db pre-migration analysis due to DDL connectivity")
            return
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

            self.config_parser.print_log_message('INFO', "planner: run_premigration_analysis: Pre-migration analysis completed successfully.")
        except Exception as e:
            self.handle_error(e, "Pre-migration analysis")
        finally:
            self.source_connection.disconnect()
            self.target_connection.disconnect()

    def run_prepare_tables(self):
        self.config_parser.print_log_message('INFO', "planner: run_prepare_tables: Preparing tables...")
        # if self.source_db_config.get('connectivity') == 'ddl':
        #     self.config_parser.print_log_message('DEBUG', "planner: run_prepare_tables: skipping source db fetch for tables due to DDL connectivity")
        #     return
        source_tables = self.source_connection.fetch_table_names(self.source_schema_name)
        include_tables = self.config_parser.get_include_tables()
        exclude_tables = self.config_parser.get_exclude_tables() or []

        self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Source schema: {self.source_schema_name}")
        self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Source tables: {source_tables}")
        self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Include tables: {include_tables}")
        self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Exclude tables: {exclude_tables}")

        for order_num, table_info in source_tables.items():
            source_table_rows = 0
            target_table_rows = 0
            self.config_parser.print_log_message('INFO', f"planner: run_prepare_tables: Processing table ({order_num}/{len(source_tables)}): {table_info['table_name']}")
            # If include_tables is empty, include all tables
            # If include_tables is ['.*'] or contains '.*', include all tables
            if include_tables == ['.*'] or '.*' in include_tables:
                pass
            elif include_tables and not any(fnmatch.fnmatch(table_info['table_name'], pattern) for pattern in include_tables):
                continue
            if any(fnmatch.fnmatch(table_info['table_name'], pattern) for pattern in exclude_tables):
                self.config_parser.print_log_message('INFO', f"planner: run_prepare_tables: Table {table_info['table_name']} is excluded from migration.")
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
                self.config_parser.print_log_message( 'DEBUG3', f"planner: run_prepare_tables: Table description: {table_description}")
                table_description = table_description['table_description'] if 'table_description' in table_description else ''
                self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Table description: {table_description}")
                source_columns = self.source_connection.fetch_table_columns(settings)
                self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Fetched source columns: {source_columns}")

                for _, column_info in source_columns.items():
                    self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Checking for data types / default values substitutions for column {column_info}...")
                    substitution = self.migrator_tables.check_data_types_substitution({
                                                                'table_name': table_info['table_name'],
                                                                'column_name': column_info['column_name'],
                                                                'check_type': column_info['data_type'],
                                                            })
                    if substitution:
                        self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Substitution based on data_type ({column_info['data_type']}): {substitution}")
                        column_info['column_type_substitution'] = substitution
                    else:
                        substitution = self.migrator_tables.check_data_types_substitution({
                                                                'table_name': table_info['table_name'],
                                                                'column_name': column_info['column_name'],
                                                                'check_type': column_info['column_type'],
                                                            })
                        if substitution:
                            self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Substitution based on column_type ({column_info['column_type']}): {substitution}")
                            column_info['column_type_substitution'] = substitution
                        else:
                            if 'basic_data_type' in column_info and column_info['basic_data_type'] != '':
                                substitution = self.migrator_tables.check_data_types_substitution({
                                                                'table_name': table_info['table_name'],
                                                                'column_name': column_info['column_name'],
                                                                'check_type': column_info['basic_data_type']
                                                            })
                                if substitution:
                                    self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Substitution based on basic_data_type ({column_info['basic_data_type']}): {substitution}")
                                    column_info['column_type_substitution'] = substitution

                    # checking for default values substitution with the new data type
                    if column_info['column_default_value'] != '':
                        substitution = self.migrator_tables.check_default_values_substitution({
                            'check_column_name': column_info['column_name'],
                            'check_column_data_type': column_info['data_type'],
                            'check_default_value': column_info['column_default_value'],
                        })
                        if substitution and substitution != None and column_info['column_default_value'] != substitution:
                            column_info['replaced_column_default_value'] = substitution
                            self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Substituted default value: {column_info['column_default_value']} -> {substitution}")

                self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Source columns: {source_columns}")
                settings = {
                    'source_db_type': self.config_parser.get_source_db_type(),
                    'source_schema_name': self.source_schema_name,
                    'source_table_name': table_info['table_name'],
                    'source_table_id': table_info['id'],
                    'target_db_type': self.config_parser.get_target_db_type(),
                    'target_schema_name': self.target_schema_name,
                    'target_table_name': table_info['table_name'],
                    'source_columns': source_columns,
                    'migrator_tables': self.migrator_tables,
                }
                self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: convert_table_columns - settings: {settings}")
                target_columns = self.convert_table_columns(settings)
                settings['target_columns'] = target_columns
                self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: convert_table_columns - target_columns: {target_columns}")
                target_table_sql = self.target_connection.get_create_table_sql(settings)
                self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Target table SQL: {target_table_sql}")

                target_partitioning = self.config_parser.get_target_partitioning()
                if target_partitioning:
                    for partitioning_case in target_partitioning:
                        if partitioning_case['table_name'] == table_info['table_name']:
                            table_partitioning_columns = ', '.join(
                                f'"{col.strip()}"' if not col.strip().startswith('"') and not col.strip().endswith('"') else col.strip()
                                for col in partitioning_case['partitioning_columns'].split(',')
                            )
                            target_table_sql += f" PARTITION BY {partitioning_case['partition_by']} ({table_partitioning_columns})"
                            table_partitioned = True
                            table_partitioned_by = partitioning_case['partition_by']
                            self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Adding partitioning to table {table_info['table_name']}: {target_table_sql}")
                            if 'date_range' in partitioning_case:
                                if partitioning_case['date_range'] in ('year', 'month', 'week', 'day'):
                                    query = f"""
                                        SELECT min({table_partitioning_columns}) as min_value,
                                        max({table_partitioning_columns}) as max_value
                                        FROM "{self.source_schema_name}"."{table_info['table_name']}"
                                        """
                                    self.source_connection.connect()
                                    self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Query to get min/max values for partitioning: {query}")
                                    cursor = self.source_connection.connection.cursor()
                                    cursor.execute(query)
                                    min_max = cursor.fetchall()
                                    cursor.close()
                                    self.source_connection.disconnect()
                                    if min_max and len(min_max) > 0:
                                        min_value = min_max[0][0]
                                        max_value = min_max[0][1]
                                        self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Min/Max values for partitioning: {min_value}, {max_value}")
                                        if partitioning_case['date_range'] in ('year', 'month', 'week'):
                                            query = f"""
                                                SELECT
                                                    'CREATE TABLE IF NOT EXISTS "{self.target_schema_name}"."{table_info['table_name']}_{partitioning_case['date_range']}_' ||
                                                    to_char(gs.start_date, 'YYYYMMDD') || '" ' ||
                                                    ' PARTITION OF "{self.target_schema_name}"."{table_info['table_name']}" ' ||
                                                    ' FOR VALUES FROM (''' || to_char(gs.start_date, 'YYYY-MM-DD') ||
                                                    ''') TO (''' || to_char(gs.end_date, 'YYYY-MM-DD') || ''')' AS create_partition_sql
                                                FROM (
                                                    SELECT
                                                        date_trunc('{partitioning_case['date_range']}', generate_series)::date AS start_date,
                                                        (date_trunc('{partitioning_case['date_range']}', generate_series) + interval '1 {partitioning_case['date_range']} - 1 day')::date AS end_date
                                                    FROM generate_series(
                                                        date_trunc('{partitioning_case['date_range']}', '{min_value}'::date), -- Replace '2023-01-15' with your start date
                                                        date_trunc('{partitioning_case['date_range']}', '{max_value}0'::date), -- Replace '2023-05-10' with your end date
                                                        '1 {partitioning_case['date_range']}'::interval)
                                                ) gs
                                            """
                                            self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Create partitions SQL: {query}")
                                            self.target_connection.connect()
                                            cursor = self.target_connection.connection.cursor()
                                            cursor.execute(query)
                                            create_partitions_sql = cursor.fetchall()
                                            # Convert create_partitions_sql into a JSON-encoded string for easy storage and decoding later

                                            create_partitions_sql = json.dumps([row[0] for row in create_partitions_sql])
                                            cursor.close()
                                            self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Create partitions SQL: {create_partitions_sql}")

                self.config_parser.print_log_message( 'INFO', f"planner: run_prepare_tables: Counting rows in source table {table_info['table_name']}...")
                self.source_connection.connect()
                source_table_rows = self.source_connection.get_rows_count(
                    self.source_schema_name,
                    table_info['table_name'],
                )
                self.source_connection.disconnect()
                self.config_parser.print_log_message( 'INFO', f"planner: run_prepare_tables: Source table {table_info['table_name']} has {source_table_rows} rows.")

                self.migrator_tables.insert_tables({
                    'source_schema_name': self.source_schema_name,
                    'source_table_name': table_info['table_name'],
                    'source_table_id': table_info['id'],
                    'source_columns': source_columns,
                    'source_table_rows': source_table_rows,
                    'source_table_description': table_description,
                    'source_table_sql': table_info.get('source_table_sql', ''),
                    'target_schema_name': self.target_schema_name,
                    'target_table_name': table_info['table_name'],
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
                    'source_table_rows': source_table_rows,
                    'source_table_description': table_description,
                    'source_table_sql': table_info.get('source_table_sql', ''),
                    'target_schema_name': self.target_schema_name,
                    'target_table_name': table_info['table_name'],
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
                    'target_table_name': table_info['table_name'],
                    'target_columns': target_columns,
                })
                self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Indexes: {indexes}")
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
                        values['target_table_name'] = table_info['table_name']
                        values['index_columns'] = index_details['index_columns']
                        values['index_comment'] = index_details['index_comment']
                        values['index_sql'] = self.target_connection.get_create_index_sql(values)
                        values['is_function_based'] = index_details.get('is_function_based', 'NO')
                        self.migrator_tables.insert_indexes( values )
                        self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Processed index: {values}")
                else:
                    self.config_parser.print_log_message( 'INFO', f"planner: run_prepare_tables: No indexes found for table {table_info['table_name']}.")
            else:
                self.config_parser.print_log_message( 'INFO', "planner: run_prepare_tables: Skipping index migration.")

            if self.config_parser.should_migrate_constraints():
                constraints = self.source_connection.fetch_constraints({
                    'source_table_id': table_info['id'],
                    'source_table_schema': self.source_schema_name,
                    'source_table_name': table_info['table_name'],
                })
                self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Constraints: {constraints}")
                if constraints:
                    for _, constraint_details in constraints.items():

                        target_db_constraint_sql = self.target_connection.get_create_constraint_sql({
                            'source_db_type': self.config_parser.get_source_db_type(),
                            'source_schema_name': self.source_schema_name,
                            'source_table_name': table_info['table_name'],
                            'target_schema_name': self.target_schema_name,
                            'target_table_name': table_info['table_name'],
                            'target_columns': target_columns,
                            'constraint_name': constraint_details['constraint_name'] if 'constraint_name' in constraint_details else '',
                            'constraint_type': constraint_details['constraint_type'] if 'constraint_type' in constraint_details else '',
                            'constraint_columns': constraint_details['constraint_columns'] if 'constraint_columns' in constraint_details else '',
                            'referenced_table_schema': constraint_details['referenced_table_schema'] if 'referenced_table_schema' in constraint_details else '',
                            'referenced_table_name': constraint_details['referenced_table_name'] if 'referenced_table_name' in constraint_details else '',
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
                            'target_table_name': table_info['table_name'],
                            'constraint_name': constraint_details['constraint_name'],
                            'constraint_type': constraint_details['constraint_type'],
                            'constraint_owner': constraint_details['constraint_owner'] if 'constraint_owner' in constraint_details else '',
                            'constraint_columns': constraint_details['constraint_columns'] if 'constraint_columns' in constraint_details else '',
                            'referenced_table_schema': constraint_details['referenced_table_schema'] if 'referenced_table_schema' in constraint_details else '',
                            'referenced_table_name': constraint_details['referenced_table_name'] if 'referenced_table_name' in constraint_details else '',
                            'referenced_columns': constraint_details['referenced_columns'] if 'referenced_columns' in constraint_details else '',
                            'delete_rule': constraint_details['delete_rule'] if 'delete_rule' in constraint_details else '',
                            'update_rule': constraint_details['update_rule'] if 'update_rule' in constraint_details else '',
                            'constraint_sql': target_db_constraint_sql,
                            'constraint_comment': constraint_details['constraint_comment'],
                            'constraint_status': constraint_details['constraint_status'] if 'constraint_status' in constraint_details else '',
                            }
                        )
                    self.config_parser.print_log_message('INFO', f"planner: run_prepare_tables: Constraint {constraint_details['constraint_name']} for table {table_info['table_name']}")
                else:
                    self.config_parser.print_log_message('INFO', f"planner: run_prepare_tables: No constraints found for table {table_info['table_name']}.")
            else:
                self.config_parser.print_log_message('INFO', "planner: run_prepare_tables: Skipping constraint migration.")

            if self.config_parser.should_migrate_triggers():
                triggers = self.source_connection.fetch_triggers(table_info['id'], self.source_schema_name, table_info['table_name'])
                self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Number of triggers: {len(triggers) if triggers else 0}, Triggers: {triggers}")
                if triggers:
                    for _, trigger_details in triggers.items():

                        converted_code = self.source_connection.convert_trigger({
                                'source_schema_name': self.config_parser.get_source_schema(),
                                'source_table_name': table_info['table_name'],
                                'target_schema_name': self.config_parser.get_target_schema(),
                                'target_table_name': table_info['table_name'],
                                'trigger_name': trigger_details['name'],
                                'trigger_sql': trigger_details['sql'],
                                'table_list': [],
                                'target_db_type': self.config_parser.get_target_db_type(),
                            })

                        self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Source trigger code: {trigger_details['sql']}")
                        self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_tables: Converted trigger code: {converted_code}")

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
                            'trigger_comment': trigger_details['comment']
                        })
                    self.config_parser.print_log_message('INFO', f"planner: run_prepare_tables: Trigger {trigger_details['name']} for table {table_info['table_name']}")
                else:
                    self.config_parser.print_log_message('INFO', f"planner: run_prepare_tables: No triggers found for table {table_info['table_name']}.")
            else:
                self.config_parser.print_log_message('INFO', "planner: run_prepare_tables: Skipping trigger migration.")

            self.config_parser.print_log_message('INFO', f"planner: run_prepare_tables: Table {table_info['table_name']} processed successfully.")
        self.config_parser.print_log_message('INFO', "planner: run_prepare_tables: Tables processed successfully.")

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
                    if source_db_type != 'postgresql':
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

                    if self.config_parser.get_varchar_to_text_length() >= 0 or self.config_parser.get_char_to_text_length() >= 0:
                        if (self.source_connection.is_string_type(coltype)
                            and 'VARCHAR' in coltype.upper()
                            and character_maximum_length >= self.config_parser.get_varchar_to_text_length()):
                            coltype = 'TEXT'
                        elif (self.source_connection.is_string_type(coltype)
                              and 'CHAR' in coltype.upper()
                              and character_maximum_length >= self.config_parser.get_char_to_text_length()):
                            coltype = 'TEXT'

                self.config_parser.print_log_message( 'DEBUG', f"planner: convert_table_columns: Column {column_info['column_name']} - using data type: {coltype}")

                converted[order_num] = {
                    'column_name': column_info['column_name'],
                    'is_nullable': column_info['is_nullable'],
                    'column_default_name': column_info['column_default_name'] if 'column_default_name' in column_info else '',
                    'column_default_value': self.source_connection.convert_default_value({'extracted_default_value': column_info['column_default_value'], 'column_type': coltype}) if 'column_default_value' in column_info else '',
                    'replaced_column_default_value': self.source_connection.convert_default_value({'extracted_default_value': column_info['replaced_column_default_value'], 'column_type': coltype}) if 'replaced_column_default_value' in column_info else '',
                    'data_type': coltype,
                    'column_type': column_info['column_type'] if 'column_type' in column_info else '',
                    'column_type_substitution': column_info['column_type_substitution'] if 'column_type_substitution' in column_info else '',
                    'character_maximum_length': '' if coltype == 'TEXT' else column_info['character_maximum_length'] if column_info['character_maximum_length'] is not None else '',
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
                    'is_hidden_column': column_info['is_hidden_column'] if 'is_hidden_column' in column_info else '',
                    'stripped_generation_expression': column_info['stripped_generation_expression'] if 'stripped_generation_expression' in column_info else '',
                }
        else:
            raise ValueError(f"Unsupported target database type: {target_db_type}")

        return converted

    def run_prepare_views(self):
        self.config_parser.print_log_message('INFO', "planner: run_prepare_views: Preparing views...")
        # if self.source_db_config.get('connectivity') == 'ddl':
        #     self.config_parser.print_log_message('DEBUG', "planner: run_prepare_views: skipping source db fetch for views due to DDL connectivity")
        #     return
        if self.config_parser.should_migrate_views():
            self.config_parser.print_log_message('INFO', "planner: run_prepare_views: Processing views...")
            views = self.source_connection.fetch_views_names(self.source_schema_name)

            include_views = self.config_parser.get_include_views()
            exclude_views = self.config_parser.get_exclude_views() or []

            self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_views: Source views: {views}")
            self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_views: Include views: {include_views}")
            self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_views: Exclude views: {exclude_views}")

            for order_num, view_info in views.items():
                self.config_parser.print_log_message('INFO', f"planner: run_prepare_views: Processing view ({order_num}): {view_info}")
                if include_views == ['.*'] or '.*' in include_views:
                    pass
                elif not any(fnmatch.fnmatch(view_info['view_name'], pattern) for pattern in include_views):
                    self.config_parser.print_log_message('INFO', f"planner: run_prepare_views: View {view_info['view_name']} does not match patterns for migration.")
                    continue
                if any(fnmatch.fnmatch(view_info['view_name'], pattern) for pattern in exclude_views):
                    self.config_parser.print_log_message('INFO', f"planner: run_prepare_views: View {view_info['view_name']} is excluded from migration.")
                    continue
                self.config_parser.print_log_message('INFO', f"planner: run_prepare_views: View {view_info['view_name']} is included for migration.")
                view_sql = self.source_connection.fetch_view_code({
                    'view_id': view_info['id'],
                    'source_schema_name': self.config_parser.get_source_schema(),
                    'source_view_name': view_info['view_name'],
                    'target_schema_name': self.config_parser.get_target_schema(),
                    'target_view_name': view_info['view_name'],
                })
                self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_views: Source view SQL: {view_sql}")
                converted_view_sql = self.source_connection.convert_view_code({
                    'view_code': view_sql,
                    'source_database': self.config_parser.get_source_db_name(),
                    'source_schema_name': self.config_parser.get_source_schema(),
                    'target_schema_name': self.config_parser.get_target_schema(),
                    'target_db_type': self.config_parser.get_target_db_type(),
                    'target_view_name': view_info['view_name'], # Pass name
                    'view_type': view_info.get('view_type', 'VIEW'), # Pass type
                })

                self.config_parser.print_log_message( 'DEBUG', "planner: run_prepare_views: Checking for remote objects substitution in view SQL...")
                rows = self.migrator_tables.get_records_remote_objects_substitution()
                if rows:
                    for row in rows:
                        self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_views: Views - remote objects substituting {row[0]} with {row[1]}")
                        converted_view_sql = re.sub(re.escape(row[0]), row[1], converted_view_sql, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)

                self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_views: Converted view SQL: {converted_view_sql}")
                self.migrator_tables.insert_view({
                    'source_schema_name': self.source_schema_name,
                    'source_view_name': view_info['view_name'],
                    'source_view_id': view_info['id'],
                    'source_view_sql': view_sql,
                    'target_schema_name': self.target_schema_name,
                    'target_view_name': view_info['view_name'],
                    'target_view_sql': converted_view_sql,
                    'view_comment': view_info['comment']
                })
                self.config_parser.print_log_message( 'INFO', f"planner: run_prepare_views: View {view_info['view_name']} processed successfully.")
            self.config_parser.print_log_message( 'INFO', "planner: run_prepare_views: Views processed successfully.")
        else:
            self.config_parser.print_log_message( 'INFO', "planner: run_prepare_views: Skipping views migration.")
        self.config_parser.print_log_message( 'INFO', "planner: run_prepare_views: Views processed successfully.")

    def run_prepare_aliases(self):
        self.config_parser.print_log_message('INFO', "planner: run_prepare_aliases: Preparing aliases...")
        self.config_parser.print_log_message('INFO', "planner: run_prepare_aliases: Processing aliases...")

        try:
            aliases = self.source_connection.get_aliases({'source_schema_name': self.source_schema_name})
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"planner: run_prepare_aliases: Cannot fetch aliases: {e}")
            aliases = {}

        if aliases:
            self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_aliases: Source aliases count: {len(aliases)}")
            for order_num, alias_info in aliases.items():
                self.config_parser.print_log_message('INFO', f"planner: run_prepare_aliases: Processing alias ({order_num}): {alias_info.get('alias_name')}")

                self.migrator_tables.insert_aliases({
                    'source_schema_name': self.source_schema_name,
                    'source_alias_name': alias_info.get('alias_name', ''),
                    'source_alias_id': alias_info.get('id', 0),
                    'source_alias_sql': alias_info.get('alias_sql', ''),
                    'source_referenced_schema_name': alias_info.get('aliased_schema_name', ''),
                    'source_referenced_table_name': alias_info.get('aliased_table_name', ''),
                    'source_referenced_column_name': alias_info.get('aliased_column_name', ''),
                    'source_alias_comment': alias_info.get('alias_comment', '')
                })
                self.config_parser.print_log_message( 'INFO', f"planner: run_prepare_aliases: Alias {alias_info.get('alias_name')} processed successfully.")
        else:
            self.config_parser.print_log_message( 'INFO', "planner: run_prepare_aliases: No aliases found.")

        self.config_parser.print_log_message( 'INFO', "planner: run_prepare_aliases: Aliases processing completed.")

    def run_prepare_user_defined_types(self):
        self.config_parser.print_log_message('INFO', "planner: run_prepare_user_defined_types: Preparing user defined types...")
        # if self.source_db_config.get('connectivity') == 'ddl':
        #     self.config_parser.print_log_message('DEBUG', "planner: run_prepare_user_defined_types: skipping source db fetch for user defined types due to DDL connectivity")
        #     return
        user_defined_types = self.source_connection.fetch_user_defined_types(self.source_schema_name)

        # Get types mapping for type conversion
        settings = {'target_db_type': self.config_parser.get_target_db_type()}
        types_mapping = self.source_connection.get_types_mapping(settings)
        # Create case-insensitive mapping
        types_mapping = {k.lower(): v for k, v in types_mapping.items()}

        self.config_parser.print_log_message('DEBUG', f"planner: run_prepare_user_defined_types: User defined types: {user_defined_types}")

        if user_defined_types:
            for order_num, type_info in user_defined_types.items():
                type_name = type_info['type_name']
                base_type = type_info.get('base_type', '')
                length = type_info.get('length', '')
                prec = type_info.get('prec', '')
                scale = type_info.get('scale', '')
                source_type_sql = type_info['sql']

                self.config_parser.print_log_message('DEBUG', f"planner: run_prepare_user_defined_types: Source type: {type_name}, Base: {base_type}")

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

                self.config_parser.print_log_message('DEBUG', f"planner: run_prepare_user_defined_types: Converted type SQL: {target_type_sql}")

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
                self.config_parser.print_log_message('INFO', f"planner: run_prepare_user_defined_types: User defined type {type_name} processed successfully.")
            self.config_parser.print_log_message('INFO', "planner: run_prepare_user_defined_types: User defined types processed successfully.")
        else:
            self.config_parser.print_log_message('INFO', "planner: run_prepare_user_defined_types: No user defined types found.")

    def run_prepare_domains(self):
        self.config_parser.print_log_message('INFO', "planner: run_prepare_domains: Preparing domains...")
        # if self.source_db_config.get('connectivity') == 'ddl':
        #     self.config_parser.print_log_message('DEBUG', "planner: run_prepare_domains: skipping source db fetch for domains due to DDL connectivity")
        #     return
        migrated_as = 'CHECK CONSTRAINT'
        if self.config_parser.get_target_db_type() == 'postgresql':
            migrated_as = 'DOMAIN'
        domains = self.source_connection.fetch_domains(self.source_schema_name)
        self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_domains: Domains found in source database: {domains}")
        if domains:
            for order_num, domain_info in domains.items():
                self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_domains: Processing domain: {domain_info}")
                domain_info['target_schema_name'] = self.target_schema_name
                domain_info['migrated_as'] = migrated_as
                converted_domain_sql = self.target_connection.get_create_domain_sql(domain_info)
                self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_domains: Converted domain SQL: {converted_domain_sql}")

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
                self.config_parser.print_log_message('INFO', f"planner: run_prepare_domains: Domain {domain_info['domain_name']} processed successfully.")
            self.config_parser.print_log_message('INFO', "planner: run_prepare_domains: Domains processed successfully.")
        else:
            self.config_parser.print_log_message('INFO', "planner: run_prepare_domains: No domains found.")

    def run_prepare_defaults(self):
        self.config_parser.print_log_message('INFO', "planner: run_prepare_defaults: Preparing defaults...")
        # if self.source_db_config.get('connectivity') == 'ddl':
        #     self.config_parser.print_log_message('DEBUG', "planner: run_prepare_defaults: skipping source db fetch for defaults due to DDL connectivity")
        #     return
        defaults = self.source_connection.fetch_default_values({ 'source_schema_name': self.source_schema_name})
        if defaults:
            self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_defaults: Defaults found in source database: {defaults}")
            for order_num, default_info in defaults.items():
                self.config_parser.print_log_message( 'DEBUG', f"planner: run_prepare_defaults: Processing default: {default_info}")

                self.migrator_tables.insert_default_value({
                    'default_value_schema': default_info['default_value_schema'],
                    'default_value_name': default_info['default_value_name'],
                    'default_value_sql': default_info['default_value_sql'],
                    'extracted_default_value': default_info['extracted_default_value'],
                    'default_value_data_type': default_info['default_value_data_type'] if 'default_value_data_type' in default_info else '',
                    'default_value_comment':  default_info['default_value_comment'] if 'default_value_comment' in default_info else '',
                })
                self.config_parser.print_log_message('INFO', f"planner: run_prepare_defaults: Default {default_info['default_value_name']} processed successfully.")
            self.config_parser.print_log_message('INFO', "planner: run_prepare_defaults: Defaults processed successfully.")
        else:
            self.config_parser.print_log_message('INFO', "planner: run_prepare_defaults: No defaults found.")

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
                    source_table_rows = self.source_connection.get_rows_count(
                        data_migration_info['source_schema_name'],
                        data_migration_info['source_table_name']
                    )
                    target_table_rows = self.target_connection.get_rows_count(
                        data_migration_info['target_schema_name'],
                        data_migration_info['target_table_name']
                    )
                    self.config_parser.print_log_message('DEBUG', f"planner: run_check_tables_migration_status: Row counts for table {data_migration_info['source_table_name']}: source={source_table_rows}, target={target_table_rows}")

                    if source_table_rows != target_table_rows:
                        self.config_parser.print_log_message('INFO', f"planner: run_check_tables_migration_status: Row counts do not match for table {data_migration_info['source_table_name']}: source={source_table_rows}, target={target_table_rows}. Marking as not fully migrated.")
                        self.migrator_tables.update_table_status({'row_id': table_info['id'], 'success': False, 'message': ''})
                        self.migrator_tables.update_data_migration_rows({
                            "row_id": data_migration_info['id'],
                            "source_table_rows": source_table_rows,
                            "target_table_rows": target_table_rows,
                        } )
                        self.migrator_tables.update_data_migration_status({
                            "row_id": data_migration_info['id'],
                            "success": False,
                            "message": '',
                            'target_table_rows': target_table_rows,
                        })
                    else:
                        self.config_parser.print_log_message('DEBUG', f"planner: run_check_tables_migration_status: Row counts match for table {data_migration_info['source_table_name']}: source={source_table_rows}, target={target_table_rows}. Marking as fully migrated.")
                        self.migrator_tables.update_table_status({'row_id': table_info['id'], 'success': True, 'message': 'Fully migrated'})
                        self.migrator_tables.update_data_migration_rows({
                            "row_id": data_migration_info['id'],
                            "source_table_rows": source_table_rows,
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

    def run_prepare_data_sources(self):
        self.config_parser.print_log_message('INFO', "planner: run_prepare_data_sources: Preparing data sources...")

        database_export = self.config_parser.get_source_database_export()

        if not database_export:
            self.config_parser.print_log_message('INFO', "planner: run_prepare_data_sources: No settings for database export found. Migrator will use source tables as data sources.")
            return
        self.config_parser.print_log_message('INFO', f"planner: run_prepare_data_sources: Using database export: {database_export}")

        if database_export['format'] in ('CSV', 'UNL'):
            for table in self.migrator_tables.fetch_all_tables():
                self.config_parser.print_log_message('DEBUG', f"planner: run_prepare_data_sources: Processing table: {table}")
                settings_source = 'global'
                table_info = self.migrator_tables.decode_table_row(table)
                table_database_export = self.config_parser.get_table_database_export(table_info['source_schema_name'], table_info['source_table_name'])
                if table_database_export:
                    settings_source = 'table_specific'
                    self.config_parser.print_log_message('DEBUG', f"planner: run_prepare_data_sources: Table {table_info['source_table_name']} has specific database export settings: {table_database_export}")

                file_name = database_export.get('file', None)
                if table_database_export and 'file' in table_database_export:
                    file_name = table_database_export['file']

                if file_name:
                    def replace_placeholder(text, placeholder, value):
                        def replacer(match):
                            return value.upper() if match.group(0).isupper() else value.lower()
                        return re.sub(re.escape(placeholder), replacer, text, flags=re.IGNORECASE)

                    table_file_name = file_name
                    table_file_name = replace_placeholder(table_file_name, '{{source_schema_name}}', table_info['source_schema_name'])
                    table_file_name = replace_placeholder(table_file_name, '{{source_table_name}}', table_info['source_table_name'])

                    if os.path.exists(table_file_name):
                        data_file_found = True
                    else:
                        if re.search(re.escape('{{source_alias_name}}'), table_file_name, flags=re.IGNORECASE):
                            valid_alias_name = table_info['source_table_name']
                            aliases = self.migrator_tables.fetch_all_aliases({'source_schema_name': table_info['source_schema_name']})
                            # self.config_parser.print_log_message('DEBUG3', f"planner: run_prepare_data_sources: Aliases found: {aliases}")
                            for row in aliases:
                                alias_info = self.migrator_tables.decode_aliases_row(row)
                                # self.config_parser.print_log_message('DEBUG3', f"planner: run_prepare_data_sources: Processing alias: {alias_info}")
                                ref_schema = alias_info.get('source_referenced_schema_name') or ''
                                ref_table = alias_info.get('source_referenced_table_name') or ''
                                if ref_schema and ref_table:
                                    if ((ref_table == table_info['source_table_name'].lower() or ref_table == table_info['source_table_name'].upper()) and
                                        (ref_schema == table_info['source_schema_name'].lower() or ref_schema == table_info['source_schema_name'].upper())):
                                        valid_alias_name = alias_info['source_alias_name']
                                        table_file_name = replace_placeholder(table_file_name, '{{source_alias_name}}', valid_alias_name)
                                        if os.path.exists(table_file_name):
                                            break

                    if os.path.exists(table_file_name):
                        data_file_found = True
                    else:
                        self.config_parser.print_log_message('ERROR', f"planner: run_prepare_data_sources: Data source file {table_file_name} does not exist or is not accessible.")
                        data_file_found = False
                        if self.config_parser.get_source_database_export_on_missing_data_file() == 'error':
                            self.config_parser.print_log_message('ERROR', f"planner: run_prepare_data_sources: Data source file {table_file_name} does not exist or is not accessible. Stopping execution.")
                            exit(1)

                    conversion_path = self.config_parser.get_source_database_export_conversion_path()
                    if table_database_export and 'conversion_path' in table_database_export:
                        conversion_path = self.config_parser.get_table_database_export_conversion_path(table_info['source_schema_name'], table_info['source_table_name'])

                    converted_file_name = os.path.join(
                        conversion_path,
                        re.sub(r'(?i)(\.csv)+$', '.csv', os.path.basename(table_file_name) + ".csv")
                    )

                    header = database_export.get('header', False)
                    if table_database_export and 'header' in table_database_export:
                        header = table_database_export['header']

                    format = database_export.get('format', None)
                    if table_database_export and 'format' in table_database_export:
                        format = table_database_export['format']

                    delimiter = database_export.get('delimiter', '|')
                    if table_database_export and 'delimiter' in table_database_export:
                        delimiter = table_database_export['delimiter']

                    self.config_parser.print_log_message('DEBUG3',f"planner: run_prepare_data_sources: Table {table_info['source_table_name']} - file_name: {table_file_name}, converted_file_name: {converted_file_name}, data_file_found: {data_file_found}, format: {format}, delimiter: {delimiter}, header: {header}")
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
                        }
                    }
                    self.migrator_tables.insert_data_source(data_source)
                    self.config_parser.print_log_message('DEBUG', f"planner: run_prepare_data_sources: Table {table_info['source_table_name']} - inserted data source: {data_source}")

        elif database_export['format'] == 'SQL':
            if self.config_parser.get_source_db_type() not in ('informix',):
                self.config_parser.print_log_message('ERROR', f"planner: run_prepare_data_sources: SQL data source is NOT supported for source database {self.config_parser.get_source_db_type()}")
                exit(1)
            sql_file = database_export.get('file', None)
            if not sql_file:
                self.config_parser.print_log_message('ERROR', f"planner: run_prepare_data_sources: SQL dump file is not specified.")
                exit(1)
            if not os.path.exists(sql_file):
                self.config_parser.print_log_message('ERROR', f"planner: run_prepare_data_sources: SQL dump file {sql_file} does not exist or is not accessible.")
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
                                self.config_parser.print_log_message('ERROR', f"planner: run_prepare_data_sources: UNL dump file {unl_dump_file} for table {schema}.{table} does not exist or is not accessible.")
                                data_file_found = False

                            converted_file_name = os.path.join(
                                self.config_parser.get_source_database_export_conversion_path(),
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
                                    'delimiter': database_export.get('delimiter', '|'),
                                    'header': False
                                }
                            }
                            self.migrator_tables.insert_data_source(data_source)
                            self.config_parser.print_log_message('DEBUG', f"planner: run_prepare_data_sources: Table {schema}.{table} data source: {data_source}")

                            break
                        # Stop if another { TABLE is found before { unload
                        if lines[j].strip().startswith('{ TABLE'):
                            break
                        j += 1
                    i = j
                else:
                    i += 1

        self.config_parser.print_log_message('INFO', "planner: run_prepare_data_sources: Data sources prepared successfully.")

if __name__ == "__main__":
    print("This script is not meant to be run directly")
