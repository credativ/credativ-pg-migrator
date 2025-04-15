"""
Data Check Tool
source venv/bin/activate
"""
from command_line import CommandLine
from config_parser import ConfigParser
from migrator_logging import MigratorLogger
from postgresql_connector import PostgreSQLConnector
from informix_connector import InformixConnector
from sybase_ase_connector import SybaseASEConnector
from ms_sql_connector import MsSQLConnector
import pandas as pd
import sys
import os
import traceback
import signal

def main():
    cmd = CommandLine()
    args = cmd.parse_arguments()

    signal.signal(signal.SIGINT, ctrlc_signal_handler)

    # Delete the log file if it exists
    if os.path.exists(args.log_file):
        os.remove(args.log_file)

    logger = MigratorLogger(args.log_file)

    total_tables = 0
    total_failed_tables = 0
    list_failed_tables = []
    total_error_tables = 0
    list_error_tables = []
    total_success_tables = 0
    try:
        logger.logger.info("Data Check Tool")

        cmd.print_all(logger.logger)

        logger.logger.info('Starting configuration parser...')
        config_parser = ConfigParser(args)

        if args.log_level == 'DEBUG':
            logger.logger.debug(f"Parsed configuration: {config_parser.config}")

        part_name = 'connect to source database'
        logger.logger.info('Connecting to source database...')
        source_connection = connect_to_db(logger, config_parser, 'source')

        part_name = 'connect to target database'
        logger.logger.info('Connecting to target database...')
        target_connection = connect_to_db(logger, config_parser, 'target')

        part_name = 'fetch source data'
        source_schema = config_parser.get_source_schema()
        source_tables = source_connection.fetch_table_names(source_schema)
        # Loop over source_tables to find the record with the specified table name
        source_table_record = None
        total_tables = len(source_tables)
        processed_tables = 0

        for _, source_table_data in source_tables.items():
            processed_tables += 1
            try:
                source_table_record = source_table_data
                source_table_name = source_table_data['table_name']
                logger.logger.info(f"Processing table {processed_tables}/{total_tables}: {source_table_name}")

                source_columns = source_connection.fetch_table_columns(source_schema, source_table_name, migrator_tables=None)
                # if config_parser.get_log_level() == 'DEBUG':
                #     logger.logger.debug(f"Source columns: {source_columns}")

                part_name = 'fetch target data'
                target_schema = config_parser.get_target_schema()
                target_tables = target_connection.fetch_table_names(target_schema)

                # if config_parser.get_log_level() == 'DEBUG':
                #     logger.logger.debug(f"Target tables: {target_tables}")

                target_table_record = None
                part_name = 'fetch target data - find table record'
                for order_num, record in target_tables.items():
                    if record['schema_name'] == target_schema and record['table_name'] == source_table_name:
                        target_table_record = record
                        break

                if target_table_record is None:
                    raise ValueError(f"Table {target_schema}.{source_table_name} not found in target database.")

                part_name = 'fetch target data - find target columns'
                target_columns = target_connection.fetch_table_columns(target_schema, source_table_name, migrator_tables=None)
                # if config_parser.get_log_level() == 'DEBUG':
                #     logger.logger.debug(f"Target columns: {target_columns}")

                part_name = f'''fetch source data - indexes {source_table_name} ({source_table_record['id']})'''
                source_table_indexes = source_connection.fetch_indexes(source_table_record['id'], source_schema, source_table_name, target_columns)
                # if config_parser.get_log_level() == 'DEBUG':
                #     logger.logger.debug(f"Source table indexes: {source_table_indexes}")

                part_name = 'fetch source data - find primary key'
                source_primary_key_columns = []
                for _, index in source_table_indexes.items():
                    if index['type'] == 'PRIMARY KEY':
                        source_primary_key_columns = index['columns']
                        break

                part_name = 'fetch source data - fetch target table indexes'
                target_table_indexes = target_connection.fetch_indexes(target_table_record['id'], target_schema, source_table_name, target_columns)
                target_primary_key_columns = []
                for _, index in target_table_indexes.items():
                    if index['type'] == 'PRIMARY KEY':
                        target_primary_key_columns = index['columns']
                        break

                # logger.logger.info(f"Fetching data from source table {source_schema}.{source_table_name}...")
                source_data = fetch_table_data(logger, source_connection, source_schema, source_table_name, source_columns, source_primary_key_columns)

                # logger.logger.info(f"Fetching data from target table {target_schema}.{source_table_name}...")
                target_data = fetch_table_data(logger, target_connection, target_schema, source_table_name, target_columns, target_primary_key_columns)

                part_name = 'compare data'
                # logger.logger.info("Comparing data...")
                if compare_data(source_data, target_data, logger, source_table_name):
                    total_success_tables += 1
                else:
                    total_failed_tables += 1
                    list_failed_tables.append(source_table_name)

                # if len(source_data) != 0 and len(target_data) != 0:
                #     logger.logger.info("Comparing sizes of columns for matching PK values...")
                #     source_data = source_data.sort_values(by=target_primary_key_columns)
                #     target_data = target_data.sort_values(by=target_primary_key_columns)

                #     for _, source_row in source_data.iterrows():
                #         source_id = source_row[target_primary_key_columns]
                #         source_row_df = source_data[source_data[target_primary_key_columns] == source_id]
                #         target_row_df = target_data[target_data[target_primary_key_columns] == source_id]

                #         if not source_row_df.reset_index(drop=True).equals(target_row_df.reset_index(drop=True)):
                #             logger.logger.error(f"Data mismatch found for id {source_id}:")
                #             logger.logger.info(f"Source row: {source_row_df}")
                #             logger.logger.info(f"Target row: {target_row_df}")

                #         # for column in source_row_df.columns:
                #         #     source_size = source_row_df[column][0].__sizeof__()
                #         #     target_size = target_row_df[column][0].__sizeof__()
                #         #     if (source_row_df[column][0] != target_row_df[column][0]) or (source_size != target_size):
                #         #         logger.logger.error(f"Data mismatch found for {source_id}: '{column}'")
                #         #     # logger.logger.info(f"{source_id}: '{column}' - source: {source_size} / target: {target_size}")

            except Exception as e:
                total_error_tables += 1
                list_error_tables.append(source_table_name)
                logger.logger.error(f"Error processing table {source_table_name}: {e}")
                logger.logger.error("Full stack trace:")
                logger.logger.error(traceback.format_exc())
                continue

        # source_connection.disconnect()
        # target_connection.disconnect()
        # logger.logger.info("Disconnected from databases.")

        logger.logger.info("*********** Data Check Done ***********")
        logger.logger.info(f"Total successful tables: {total_success_tables}")
        logger.logger.info(f"Total tables processed: {total_tables}")
        logger.logger.info(f"Total failed comparisons: {total_failed_tables}")
        if total_failed_tables > 0:
            logger.logger.info(f"Tables with data differences: {list_failed_tables}")
        logger.logger.info(f"Total error tables: {total_error_tables}")
        if total_error_tables > 0:
            logger.logger.info(f"Errors on tables: {list_error_tables}")
        logger.logger.info("***************************************")

    except Exception as e:
        logger.logger.error(f"An error in the main ({part_name}): {e}")
        sys.exit(1)

    finally:
        logger.stop_logging()
        exit()

def connect_to_db(logger, config_parser, source_or_target):
    db_type = config_parser.get_db_config(source_or_target)['type']
    logger.logger.debug(f"Connecting to {source_or_target} database of type {db_type}...")
    if db_type == 'postgresql':
        return PostgreSQLConnector(config_parser, source_or_target)
    elif db_type == 'informix':
        return InformixConnector(config_parser, source_or_target)
    elif db_type == 'sybase_ase':
        return SybaseASEConnector(config_parser, source_or_target)
    elif db_type == 'mssql':
        return MsSQLConnector(config_parser, source_or_target)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

def fetch_table_data(logger, connection, schema, table_name, columns, primary_key_columns):
    try:
        part_name = 'connect'
        connection.connect()

        if primary_key_columns:
            query = f"SELECT * FROM {schema}.{table_name} ORDER BY {primary_key_columns}"
        else:
            query = f"SELECT * FROM {schema}.{table_name}"
        # logger.logger.debug(f"Query: {query}")
        cursor = connection.connection.cursor()
        cursor.execute(query)

        part_name = 'fetch data'
        data = cursor.fetchall()
        records = [dict(zip([column['name'] for column in columns.values()], row)) for row in data]

        logger.logger.info(f"Fetched {len(records)} rows from table {schema}.{table_name} ({connection}).")

        part_name = 'convert data to DataFrame'
        # Convert to pandas DataFrame
        df = pd.DataFrame(records)

        if len(df) != 0:
            part_name = 'convert data types'
            # Adjust binary or bytea types
            for column in columns.values():
                column_name = column['name']
                column_type = column['type']
                # logger.logger.debug(f"Column: {column_name}, Type: {column_type}")

                if column_type.lower() in ['blob', 'bytea', 'binary', 'varbinary', 'image']:
                    df[column_name] = df[column_name].apply(lambda x: x.getBytes(1, int(x.length())) if hasattr(x, 'getBytes') else (bytes(x) if x is not None else None))
                elif column_type.lower() in ['clob']:
                    df[column_name] = df[column_name].apply(lambda x: str(x) if x is not None else None)
                elif column_type.lower() in ['date', 'datetime', 'time', 'timestamp', 'timestamp without time zone', 'timestamp with time zone']:
                    df[column_name] = pd.to_datetime(df[column_name], format='mixed', errors='coerce')
                elif column_type.lower() in ['boolean']:
                    df[column_name] = df[column_name].astype(bool)

        connection.disconnect()
        return df
    except Exception as e:
        logger.logger.error(f"An error in fetch_table_data ({part_name}): {e}")
        logger.logger.error("Full stack trace:")
        logger.logger.error(traceback.format_exc())
        connection.disconnect()
        sys.exit(1)

def compare_data(source_data, target_data, logger, table_name):
    if source_data.empty and target_data.empty:
        logger.logger.info(f"Table {table_name}: Both source and target tables are empty.")
        return True
    elif source_data.empty:
        logger.logger.info(f"Table {table_name}: Source table is empty.")
        return True
    elif target_data.empty:
        logger.logger.info(f"Table {table_name}: Target table is empty.")
        return True

    # # Ensure both columns have the same type
    # if 'content_blob' in source_data.columns:
    #     for column in source_data.columns:
    #         if column == 'content_blob':
    #             source_data = source_data.with_columns(pl.col(column).cast(pl.Binary))
    # if 'content_blob' in target_data.columns:
    #     for column in target_data.columns:
    #         if column == 'content_blob':
    #             target_data = target_data.with_columns(pl.col(column).cast(pl.Binary))

    common_columns = source_data.columns.intersection(target_data.columns)
    source_data = source_data[common_columns].astype(str)
    target_data = target_data[common_columns].astype(str)

    # Sort by primary key columns if available
    primary_key_columns = list(common_columns)  # Replace with actual primary key columns if known
    if primary_key_columns:
        source_data = source_data.sort_values(by=primary_key_columns).reset_index(drop=True)
        target_data = target_data.sort_values(by=primary_key_columns).reset_index(drop=True)

    if source_data.equals(target_data):
        logger.logger.info(f"Table {table_name}: Data matches between source and target tables.")
        return True
    else:
        logger.logger.error(f"Table {table_name}: Data MISMATCH found between source and target tables.")
        differences = pd.concat([source_data, target_data]).drop_duplicates(keep=False)
        if not differences.empty:
            logger.logger.info(f"Table {table_name}: Differences found between source and target tables:")
            logger.logger.info(differences)
            logger.logger.info(f"Table {table_name}: In total, {len(differences)/2} differences found between source and target tables.")
        return False

def ctrlc_signal_handler(sig, frame):
    print("Program interrupted with Ctrl+C")
    traceback.print_stack(frame)
    sys.exit(0)

if __name__ == "__main__":
    main()
