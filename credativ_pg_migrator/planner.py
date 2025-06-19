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
        self.sql_functions_mapping = self.source_connection.get_sql_functions_mapping({
            'target_db_type': self.config_parser.get_target_db_type()
        })

    def create_plan(self):
        try:
            self.pre_planning()

            self.run_premigration_analysis()

            self.run_prepare_user_defined_types()
            self.run_prepare_domains()
            self.run_prepare_defaults()
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

            self.config_parser.print_log_message('INFO', "Planner phase done successfully.")
        except Exception as e:
            self.migrator_tables.update_main_status('Planner', '', False, f'ERROR: {e}')
            self.handle_error(e, "Planner")

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

    def pre_planning(self):
        try:
            self.config_parser.print_log_message('INFO', "Running pre-planning actions...")

            self.config_parser.print_log_message( 'DEBUG', f"Target schema: {self.target_schema}")
            self.config_parser.print_log_message( 'DEBUG', f"Pre migration script: {self.pre_script}")
            self.config_parser.print_log_message( 'DEBUG', f"Post migration script: {self.post_script}")


            self.config_parser.print_log_message( 'DEBUG', "Connecting to source and target databases...")
            self.check_database_connection(self.source_connection, "Source Database")
            self.check_database_connection(self.target_connection, "Target Database")

            self.config_parser.print_log_message( 'DEBUG', "Checking scripts accessibility...")
            self.check_script_accessibility(self.pre_script)
            self.check_script_accessibility(self.post_script)

            self.target_connection.connect()
            if self.target_connection.session_settings:
                self.config_parser.print_log_message( 'DEBUG', f"Planner CREATE SCHEMA - Executing session settings for target database: {self.target_connection.session_settings}")
                self.target_connection.execute_query(self.target_connection.session_settings)

            if self.config_parser.should_drop_schema():
                if self.target_schema.lower() == 'public':
                    self.config_parser.print_log_message('INFO', "Cannot drop the 'public' schema - skipping drop of schema.")
                else:
                    self.config_parser.print_log_message('INFO', f"Dropping target schema '{self.target_schema}'...")
                    self.target_connection.execute_query(f"DROP SCHEMA IF EXISTS {self.target_schema} CASCADE")

            self.config_parser.print_log_message( 'DEBUG', f"Creating target schema '{self.target_schema}' if it does not exist...")
            self.target_connection.execute_query(f"CREATE SCHEMA IF NOT EXISTS {self.target_schema}")
            self.target_connection.disconnect()

            self.run_pre_migration_script()

            self.config_parser.print_log_message('INFO', "Creating migration plan...")
            self.migrator_tables.create_all()
            self.migrator_tables.insert_main('Planner', '')
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

            self.config_parser.print_log_message('INFO', "Pre-planning part done successfully.")
        except Exception as e:
            self.handle_error(e, "Pre-planning runs")

    def run_premigration_analysis(self):
        self.config_parser.print_log_message('INFO', "Running pre-migration analysis...")
        try:
            self.source_connection.connect()
            self.target_connection.connect()

            self.config_parser.print_log_message('INFO', "***** Source database *****")
            source_db_version = self.source_connection.get_database_version()
            self.config_parser.print_log_message('INFO', f"Version: {source_db_version}")
            source_db_size = self.source_connection.get_database_size()
            self.config_parser.print_log_message('INFO', f"Size: {source_db_size}")

            source_db_top10_tables = self.source_connection.get_top10_biggest_tables({'source_schema': self.source_schema})
            self.config_parser.print_log_message('INFO', "Top 10 largest tables in source database:")
            self.config_parser.print_log_message('DEBUG', f"Source database top 10 tables: {source_db_top10_tables}")
            # for ord_num, table in source_db_top10_tables.items():
            #     self.config_parser.print_log_message('INFO', f"Table: {table['table_name']}, Size: {table['size_bytes']}, Rows: {table['total_rows'] if 'total_rows' in table else 'N/A'}")

            self.config_parser.print_log_message('INFO', "***** Target database *****")
            target_db_version = self.target_connection.get_database_version()
            self.config_parser.print_log_message('INFO', f"Version: {target_db_version}")
            target_db_size = self.target_connection.get_database_size()
            # self.config_parser.print_log_message('INFO', f"Size: {target_db_size}")
            # target_db_top10_tables = self.target_connection.get_top10_biggest_tables({'source_schema': self.target_schema})
            # self.config_parser.print_log_message('INFO', "Top 10 largest tables in target database:")
            # self.config_parser.print_log_message('DEBUG', f"Target database top 10 tables: {target_db_top10_tables}")
            # for ord_num, table in target_db_top10_tables.items():
            #     self.config_parser.print_log_message('INFO', f"Table: {table['table_name']}, Size: {table['size_bytes']}, Rows: {table['total_rows'] if 'total_rows' in table else 'N/A'}")

            self.config_parser.print_log_message('INFO', "Pre-migration analysis completed successfully.")
        except Exception as e:
            self.handle_error(e, "Pre-migration analysis")
        finally:
            self.source_connection.disconnect()
            self.target_connection.disconnect()

    def run_prepare_tables(self):
        self.config_parser.print_log_message('INFO', "Planner - Preparing tables...")
        source_tables = self.source_connection.fetch_table_names(self.source_schema)
        include_tables = self.config_parser.get_include_tables()
        exclude_tables = self.config_parser.get_exclude_tables() or []

        self.config_parser.print_log_message( 'DEBUG', f"Source schema: {self.source_schema}")
        self.config_parser.print_log_message( 'DEBUG', f"Source tables: {source_tables}")
        self.config_parser.print_log_message( 'DEBUG', f"Include tables: {include_tables}")
        self.config_parser.print_log_message( 'DEBUG', f"Exclude tables: {exclude_tables}")

        for order_num, table_info in source_tables.items():
            self.config_parser.print_log_message('INFO', f"Processing table ({order_num}/{len(source_tables)}): {table_info['table_name']}")
            # If include_tables is empty, include all tables
            # If include_tables is ['.*'] or contains '.*', include all tables
            if include_tables == ['.*'] or '.*' in include_tables:
                pass
            elif include_tables and not any(fnmatch.fnmatch(table_info['table_name'], pattern) for pattern in include_tables):
                continue
            if any(fnmatch.fnmatch(table_info['table_name'], pattern) for pattern in exclude_tables):
                self.config_parser.print_log_message('INFO', f"Table {table_info['table_name']} is excluded from migration.")
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
                    'table_schema': self.source_schema,
                    'table_name': table_info['table_name'],
                }
                table_description = self.source_connection.get_table_description(settings)
                table_description = table_description['table_description'] if 'table_description' in table_description else ''
                self.config_parser.print_log_message( 'DEBUG', f"Table description: {table_description}")
                source_columns = self.source_connection.fetch_table_columns(settings)
                self.config_parser.print_log_message( 'DEBUG', f"Fetched source columns: {source_columns}")

                for _, column_info in source_columns.items():
                    self.config_parser.print_log_message( 'DEBUG', f"Checking for data types / default values substitutions for column {column_info}...")
                    substitution = self.migrator_tables.check_data_types_substitution({
                                                                'table_name': table_info['table_name'],
                                                                'column_name': column_info['column_name'],
                                                                'check_type': column_info['data_type'],
                                                            })
                    if substitution:
                        self.config_parser.print_log_message( 'DEBUG', f"Substitution based on data_type ({column_info['data_type']}): {substitution}")
                        column_info['column_type_substitution'] = substitution
                    else:
                        substitution = self.migrator_tables.check_data_types_substitution({
                                                                'table_name': table_info['table_name'],
                                                                'column_name': column_info['column_name'],
                                                                'check_type': column_info['column_type'],
                                                            })
                        if substitution:
                            self.config_parser.print_log_message( 'DEBUG', f"Substitution based on column_type ({column_info['column_type']}): {substitution}")
                            column_info['column_type_substitution'] = substitution
                        else:
                            if 'basic_data_type' in column_info and column_info['basic_data_type'] != '':
                                substitution = self.migrator_tables.check_data_types_substitution({
                                                                'table_name': table_info['table_name'],
                                                                'column_name': column_info['column_name'],
                                                                'check_type': column_info['basic_data_type']
                                                            })
                                if substitution:
                                    self.config_parser.print_log_message( 'DEBUG', f"Substitution based on basic_data_type ({column_info['basic_data_type']}): {substitution}")
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
                            self.config_parser.print_log_message( 'DEBUG', f"Substituted default value: {column_info['column_default_value']} -> {substitution}")

                settings = {
                    'source_db_type': self.config_parser.get_source_db_type(),
                    'source_schema': self.source_schema,
                    'source_table': table_info['table_name'],
                    'source_table_id': table_info['id'],
                    'target_db_type': self.config_parser.get_target_db_type(),
                    'target_schema': self.target_schema,
                    'target_table': table_info['table_name'],
                    'source_columns': source_columns,
                    'migrator_tables': self.migrator_tables,
                }
                target_columns = self.convert_table_columns(settings)
                settings['target_columns'] = target_columns
                target_table_sql = self.target_connection.get_create_table_sql(settings)
                self.config_parser.print_log_message( 'DEBUG', f"Target columns: {target_columns}")
                self.config_parser.print_log_message( 'DEBUG', f"Target table SQL: {target_table_sql}")

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
                            self.config_parser.print_log_message( 'DEBUG', f"Adding partitioning to table {table_info['table_name']}: {target_table_sql}")
                            if 'date_range' in partitioning_case:
                                if partitioning_case['date_range'] in ('year', 'month', 'week', 'day'):
                                    query = f"""
                                        SELECT min({table_partitioning_columns}) as min_value,
                                        max({table_partitioning_columns}) as max_value
                                        FROM {self.source_schema}.{table_info['table_name']}
                                        """
                                    self.source_connection.connect()
                                    self.config_parser.print_log_message( 'DEBUG', f"Query to get min/max values for partitioning: {query}")
                                    cursor = self.source_connection.connection.cursor()
                                    cursor.execute(query)
                                    min_max = cursor.fetchall()
                                    cursor.close()
                                    self.source_connection.disconnect()
                                    if min_max and len(min_max) > 0:
                                        min_value = min_max[0][0]
                                        max_value = min_max[0][1]
                                        self.config_parser.print_log_message( 'DEBUG', f"Min/Max values for partitioning: {min_value}, {max_value}")
                                        if partitioning_case['date_range'] in ('year', 'month', 'week'):
                                            query = f"""
                                                SELECT
                                                    'CREATE TABLE IF NOT EXISTS "{self.target_schema}"."{table_info['table_name']}_{partitioning_case['date_range']}_' ||
                                                    to_char(gs.start_date, 'YYYYMMDD') || '" ' ||
                                                    ' PARTITION OF "{self.target_schema}"."{table_info['table_name']}" ' ||
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
                                            self.config_parser.print_log_message( 'DEBUG', f"Create partitions SQL: {query}")
                                            self.target_connection.connect()
                                            cursor = self.target_connection.connection.cursor()
                                            cursor.execute(query)
                                            create_partitions_sql = cursor.fetchall()
                                            # Convert create_partitions_sql into a JSON-encoded string for easy storage and decoding later

                                            create_partitions_sql = json.dumps([row[0] for row in create_partitions_sql])
                                            cursor.close()
                                            self.target_connection.disconnect()
                                            self.config_parser.print_log_message( 'DEBUG', f"Create partitions SQL: {create_partitions_sql}")

                self.migrator_tables.insert_tables({
                    'source_schema': self.source_schema,
                    'source_table': table_info['table_name'],
                    'source_table_id': table_info['id'],
                    'source_columns': source_columns,
                    'source_table_description': table_description,
                    'target_schema': self.target_schema,
                    'target_table': table_info['table_name'],
                    'target_columns': target_columns,
                    'target_table_sql': target_table_sql,
                    'table_comment': table_info['comment'],
                    'partitioned': table_partitioned,
                    'partitioned_by': table_partitioned_by,
                    'partitioning_columns': table_partitioning_columns,
                    'create_partitions_sql': create_partitions_sql,
                })

            except Exception as e:
                self.migrator_tables.insert_tables({
                    'source_schema': self.source_schema,
                    'source_table': table_info['table_name'],
                    'source_table_id': table_info['id'],
                    'source_columns': source_columns,
                    'source_table_description': table_description,
                    'target_schema': self.target_schema,
                    'target_table': table_info['table_name'],
                    'target_columns': target_columns,
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
                    'source_table_schema': self.source_schema,
                    'target_table_schema': self.target_schema,
                    'target_table_name': table_info['table_name'],
                    'target_columns': target_columns,
                })
                self.config_parser.print_log_message( 'DEBUG', f"Indexes: {indexes}")
                if indexes:
                    for _, index_details in indexes.items():
                        values = {}
                        values['source_schema'] = self.source_schema
                        values['source_table'] = table_info['table_name']
                        values['source_table_id'] = table_info['id']
                        values['index_owner'] = index_details['index_owner']
                        values['index_name'] = index_details['index_name']
                        values['index_type'] = index_details['index_type']
                        values['target_schema'] = self.target_schema
                        values['target_table'] = table_info['table_name']
                        values['index_columns'] = index_details['index_columns']
                        values['index_comment'] = index_details['index_comment']
                        values['index_sql'] = self.target_connection.get_create_index_sql(values)
                        values['is_function_based'] = index_details.get('is_function_based', 'NO')
                        self.migrator_tables.insert_indexes( values )
                        self.config_parser.print_log_message( 'DEBUG', f"Processed index: {values}")
                else:
                    self.config_parser.print_log_message( 'INFO', f"No indexes found for table {table_info['table_name']}.")
            else:
                self.config_parser.print_log_message( 'INFO', "Skipping index migration.")

            if self.config_parser.should_migrate_constraints():
                constraints = self.source_connection.fetch_constraints({
                    'source_table_id': table_info['id'],
                    'source_table_schema': self.source_schema,
                    'source_table_name': table_info['table_name'],
                })
                self.config_parser.print_log_message( 'DEBUG', f"Constraints: {constraints}")
                if constraints:
                    for _, constraint_details in constraints.items():

                        target_db_constraint_sql = self.target_connection.get_create_constraint_sql({
                            'source_db_type': self.config_parser.get_source_db_type(),
                            'source_schema': self.source_schema,
                            'source_table': table_info['table_name'],
                            'target_schema': self.target_schema,
                            'target_table': table_info['table_name'],
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
                            'source_schema': self.source_schema,
                            'source_table': table_info['table_name'],
                            'target_schema': self.target_schema,
                            'target_table': table_info['table_name'],
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
                    self.config_parser.print_log_message('INFO', f"Constraint {constraint_details['constraint_name']} for table {table_info['table_name']}")
                else:
                    self.config_parser.print_log_message('INFO', f"No constraints found for table {table_info['table_name']}.")
            else:
                self.config_parser.print_log_message('INFO', "Skipping constraint migration.")

            if self.config_parser.should_migrate_triggers():
                triggers = self.source_connection.fetch_triggers(table_info['id'], self.source_schema, table_info['table_name'])
                self.config_parser.print_log_message( 'DEBUG', f"Triggers: {triggers}")
                if triggers:
                    for _, trigger_details in triggers.items():

                        converted_code = self.source_connection.convert_trigger({
                                'source_schema': self.config_parser.get_source_schema(),
                                'target_schema': self.config_parser.get_target_schema(),
                                'trigger_name': trigger_details['name'],
                                'trigger_sql': trigger_details['sql'],
                                'table_list': []
                            })

                        self.config_parser.print_log_message( 'DEBUG', f"Source trigger code: {trigger_details['sql']}")
                        self.config_parser.print_log_message( 'DEBUG', f"Converted trigger code: {converted_code}")

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
                    self.config_parser.print_log_message('INFO', f"Trigger {trigger_details['name']} for table {table_info['table_name']}")
                else:
                    self.config_parser.print_log_message('INFO', f"No triggers found for table {table_info['table_name']}.")
            else:
                self.config_parser.print_log_message('INFO', "Skipping trigger migration.")

            self.config_parser.print_log_message('INFO', f"Table {table_info['table_name']} processed successfully.")
        self.config_parser.print_log_message('INFO', "Planner - Tables processed successfully.")

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
                if 'column_type_substitution' in column_info and column_info['column_type_substitution'] != '':
                    coltype = column_info['column_type_substitution'].upper()
                    character_maximum_length = 0
                    ## we presume substitution contains also length/ precision, scale
                    ## and proper data type, so we can use it directly
                    self.config_parser.print_log_message( 'DEBUG', f"Column {column_info['column_name']} - using substitution: {coltype}")
                else:
                    coltype = column_info['data_type'].upper()
                    character_maximum_length = column_info['character_maximum_length'] if column_info['character_maximum_length'] is not None else 0
                    if source_db_type != 'postgresql':
                        if types_mapping.get(coltype, 'UNKNOWN').startswith('UNKNOWN'):
                            self.config_parser.print_log_message('INFO', f"Column {column_info['column_name']} - unknown data type: {column_info['data_type']} - checking column_type...")
                            if 'column_type' in column_info and column_info['column_type']:
                                coltype = column_info['column_type'].upper()
                                if types_mapping.get(coltype, 'UNKNOWN').startswith('UNKNOWN'):
                                    self.config_parser.print_log_message('INFO', f"Column {column_info['column_name']} - unknown column type: {column_info['column_type']} - checking basic_data_type...")
                                    if 'basic_data_type' in column_info and column_info['basic_data_type']:
                                        coltype = column_info['basic_data_type'].upper()
                                        if types_mapping.get(coltype, 'UNKNOWN').startswith('UNKNOWN'):
                                            self.config_parser.print_log_message('INFO', f"Column {column_info['column_name']} - unknown basic data type: {column_info['basic_data_type']} - mapping missing, using TEXT...")

                    coltype = types_mapping.get(coltype, 'TEXT').upper()

                    if self.config_parser.get_varchar_to_text_length() >= 0:
                        if self.source_connection.is_string_type(coltype) and character_maximum_length >= self.config_parser.get_varchar_to_text_length():
                            coltype = 'TEXT'

                self.config_parser.print_log_message( 'DEBUG', f"Column {column_info['column_name']} - using data type: {coltype}")

                converted[order_num] = {
                    'column_name': column_info['column_name'],
                    'is_nullable': column_info['is_nullable'],
                    'column_default_name': column_info['column_default_name'] if 'column_default_name' in column_info else '',
                    'column_default_value': column_info['column_default_value'],
                    'replaced_column_default_value': column_info['replaced_column_default_value'] if 'replaced_column_default_value' in column_info else '',
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
                    'basic_column_type': column_info['basic_column_type'].strip() if 'basic_column_type' in column_info else '',
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
        self.config_parser.print_log_message('INFO', "Planner - Preparing views...")
        if self.config_parser.should_migrate_views():
            self.config_parser.print_log_message('INFO', "Processing views...")
            views = self.source_connection.fetch_views_names(self.source_schema)

            include_views = self.config_parser.get_include_views()
            exclude_views = self.config_parser.get_exclude_views() or []

            self.config_parser.print_log_message( 'DEBUG', f"Source views: {views}")
            self.config_parser.print_log_message( 'DEBUG', f"Include views: {include_views}")
            self.config_parser.print_log_message( 'DEBUG', f"Exclude views: {exclude_views}")

            for order_num, view_info in views.items():
                self.config_parser.print_log_message('INFO', f"Processing view ({order_num}): {view_info}")
                if include_views == ['.*'] or '.*' in include_views:
                    pass
                elif not any(fnmatch.fnmatch(view_info['view_name'], pattern) for pattern in include_views):
                    self.config_parser.print_log_message('INFO', f"View {view_info['view_name']} does not match patterns for migration.")
                    continue
                if any(fnmatch.fnmatch(view_info['view_name'], pattern) for pattern in exclude_views):
                    self.config_parser.print_log_message('INFO', f"View {view_info['view_name']} is excluded from migration.")
                    continue
                self.config_parser.print_log_message('INFO', f"View {view_info['view_name']} is included for migration.")
                view_sql = self.source_connection.fetch_view_code({
                    'view_id': view_info['id'],
                    'source_schema': self.config_parser.get_source_schema(),
                    'source_view_name': view_info['view_name'],
                    'target_schema': self.config_parser.get_target_schema(),
                    'target_view_name': view_info['view_name'],
                })
                self.config_parser.print_log_message( 'DEBUG', f"Source view SQL: {view_sql}")
                converted_view_sql = self.source_connection.convert_view_code({
                    'view_code': view_sql,
                    'source_database': self.config_parser.get_source_db_name(),
                    'source_schema': self.config_parser.get_source_schema(),
                    'target_schema': self.config_parser.get_target_schema(),
                    'target_db_type': self.config_parser.get_target_db_type(),
                })

                self.config_parser.print_log_message( 'DEBUG', "Checking for remote objects substitution in view SQL...")
                rows = self.migrator_tables.get_records_remote_objects_substitution()
                if rows:
                    for row in rows:
                        self.config_parser.print_log_message( 'DEBUG', f"Views - remote objects substituting {row[0]} with {row[1]}")
                        converted_view_sql = re.sub(re.escape(row[0]), row[1], converted_view_sql, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)

                self.config_parser.print_log_message( 'DEBUG', f"Converted view SQL: {converted_view_sql}")
                self.migrator_tables.insert_view(self.source_schema, view_info['view_name'], view_info['id'], view_sql,
                                                 self.target_schema, view_info['view_name'], converted_view_sql, view_info['comment'])
                self.config_parser.print_log_message( 'INFO', f"View {view_info['view_name']} processed successfully.")
            self.config_parser.print_log_message( 'INFO', "Views processed successfully.")
        else:
            self.config_parser.print_log_message( 'INFO', "Skipping views migration.")
        self.config_parser.print_log_message( 'INFO', "Planner - Views processed successfully.")

    def run_prepare_user_defined_types(self):
        self.config_parser.print_log_message( 'INFO', "Planner - Preparing user defined types...")
        user_defined_types = self.source_connection.fetch_user_defined_types(self.source_schema)
        self.config_parser.print_log_message( 'DEBUG', f"User defined types: {user_defined_types}")
        if user_defined_types:
            for order_num, type_info in user_defined_types.items():
                type_sql = type_info['sql']
                self.config_parser.print_log_message( 'DEBUG', f"Source type SQL: {type_sql}")
                converted_type_sql = type_sql.replace(f'{self.source_schema}.', f'{self.target_schema}.')
                self.config_parser.print_log_message( 'DEBUG', f"Converted type SQL: {converted_type_sql}")

                self.migrator_tables.insert_user_defined_type({
                    'source_schema_name': self.source_schema,
                    'source_type_name': type_info['type_name'],
                    'source_type_sql': type_sql,
                    'target_schema_name': self.target_schema,
                    'target_type_name': type_info['type_name'],
                    'target_type_sql': converted_type_sql,
                    'type_comment':  type_info['comment'],
                })
                self.config_parser.print_log_message('INFO', f"User defined type {type_info['type_name']} processed successfully.")
            self.config_parser.print_log_message('INFO', "Planner - User defined types processed successfully.")
        else:
            self.config_parser.print_log_message('INFO', "No user defined types found.")

    def run_prepare_domains(self):
        self.config_parser.print_log_message('INFO', "Planner - Preparing domains...")
        migrated_as = 'CHECK CONSTRAINT'
        domains = self.source_connection.fetch_domains(self.source_schema)
        self.config_parser.print_log_message( 'DEBUG', f"Domains found in source database: {domains}")
        if domains:
            for order_num, domain_info in domains.items():
                self.config_parser.print_log_message( 'DEBUG', f"Processing domain: {domain_info}")
                domain_info['target_schema'] = self.target_schema
                domain_info['migrated_as'] = migrated_as
                converted_domain_sql = self.target_connection.get_create_domain_sql(domain_info)
                self.config_parser.print_log_message( 'DEBUG', f"Converted domain SQL: {converted_domain_sql}")

                # If the source domain SQL contains 'CREATE RULE', set 'migrated_as' accordingly
                self.migrator_tables.insert_domain({
                    'source_schema_name': domain_info['domain_schema'] if 'domain_schema' in domain_info and domain_info['domain_schema'] is not None else self.source_schema,
                    'source_domain_name': domain_info['domain_name'],
                    'source_domain_sql': domain_info['source_domain_sql'],
                    'source_domain_check_sql': domain_info['source_domain_check_sql'] if 'source_domain_check_sql' in domain_info and domain_info['source_domain_check_sql'] is not None else '',
                    'target_schema_name': self.target_schema,
                    'target_domain_name': domain_info['domain_name'],
                    'target_domain_sql': converted_domain_sql,
                    'migrated_as': migrated_as,
                    'domain_comment':  domain_info['domain_comment'],
                })
                self.config_parser.print_log_message('INFO', f"Domain {domain_info['domain_name']} processed successfully.")
            self.config_parser.print_log_message('INFO', "Planner - Domains processed successfully.")
        else:
            self.config_parser.print_log_message('INFO', "No domains found.")

    def run_prepare_defaults(self):
        self.config_parser.print_log_message('INFO', "Planner - Preparing defaults...")
        defaults = self.source_connection.fetch_default_values({ 'source_schema': self.source_schema})
        if defaults:
            self.config_parser.print_log_message( 'DEBUG', f"Defaults found in source database: {defaults}")
            for order_num, default_info in defaults.items():
                self.config_parser.print_log_message( 'DEBUG', f"Processing default: {default_info}")

                self.migrator_tables.insert_default_value({
                    'default_value_schema': default_info['default_value_schema'],
                    'default_value_name': default_info['default_value_name'],
                    'default_value_sql': default_info['default_value_sql'],
                    'extracted_default_value': default_info['extracted_default_value'],
                    'default_value_data_type': default_info['default_value_data_type'] if 'default_value_data_type' in default_info else '',
                    'default_value_comment':  default_info['default_value_comment'] if 'default_value_comment' in default_info else '',
                })
                self.config_parser.print_log_message('INFO', f"Default {default_info['default_value_name']} processed successfully.")
            self.config_parser.print_log_message('INFO', "Planner - Defaults processed successfully.")
        else:
            self.config_parser.print_log_message('INFO', "No defaults found.")

    def run_pre_migration_script(self):
        pre_migration_script = self.config_parser.get_pre_migration_script()
        if pre_migration_script:
            self.config_parser.print_log_message('INFO', f"Running pre-migration script '{pre_migration_script}' in target database.")
            try:
                self.target_connection.connect()
                self.target_connection.execute_sql_script(pre_migration_script)
                self.target_connection.disconnect()
                self.config_parser.print_log_message('INFO', "Pre-migration script executed successfully.")
            except Exception as e:
                self.handle_error(e, "Pre-migration script")
        else:
            self.config_parser.print_log_message('INFO', "No pre-migration script specified.")

    def check_script_accessibility(self, script_path):
        if not script_path:
            return
        if not os.path.isfile(script_path):
            self.config_parser.print_log_message('ERROR', f"Script {script_path} does not exist or is not accessible.")
            if self.config_parser.get_on_error_action() == 'stop':
                self.config_parser.print_log_message('ERROR', "Stopping execution due to error.")
                exit(1)
        self.config_parser.print_log_message('INFO', f"Script {script_path} is accessible.")

    def check_database_connection(self, connector, db_name):
        try:
            connector.connect()
            cursor = connector.connection.cursor()
            query = connector.testing_select()
            cursor.execute(query)
            result = cursor.fetchone()
            if result[0] != 1:
                raise ConnectionError(f"Connection to {db_name} failed.")
            self.config_parser.print_log_message('INFO', f"Connection to {db_name} is OK.")
            cursor.close()
            connector.disconnect()
        except Exception as e:
            self.config_parser.print_log_message('ERROR', f"Failed to connect to {db_name}: {e}")
            self.config_parser.print_log_message('ERROR', traceback.format_exc())
            exit(1)

    def handle_error(self, e, description=None):
        self.config_parser.print_log_message('ERROR', f"An error in {self.__class__.__name__} ({description}): {e}")
        self.config_parser.print_log_message('ERROR', traceback.format_exc())
        if self.on_error_action == 'stop':
            self.config_parser.print_log_message('ERROR', "Stopping due to error.")
            exit(1)

if __name__ == "__main__":
    print("This script is not meant to be run directly")
