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
import polars as pl
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

    try:
        logger.logger.info("Data Check Tool")

        cmd.print_all(logger.logger)

        logger.logger.info('Starting configuration parser...')
        config_parser = ConfigParser(args)

        table_name = "t_dokumente"

        # Print the parsed configuration
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
        for _, record in source_tables.items():
            if record['schema_name'] == source_schema and record['table_name'] == table_name:
                source_table_record = record
                break

        if not source_table_record:
            raise ValueError(f"Table {source_schema}.{table_name} not found in source database.")

        source_table_indexes = source_connection.fetch_indexes(source_table_record['id'], source_schema, table_name)
        for _, index in source_table_indexes.items():
            if index['type'] == 'PRIMARY KEY':
                source_primary_key_columns = index['columns']
                break

        source_columns = source_connection.fetch_table_columns(source_schema, table_name, migrator_tables=None)
        logger.logger.debug(f"Source columns: {source_columns}")

        logger.logger.info(f"Fetching data from source table {source_schema}.{table_name}...")
        source_data = fetch_table_data(logger, source_connection, source_schema, table_name, source_columns, source_primary_key_columns)

        part_name = 'fetch target data'
        target_schema = config_parser.get_target_schema()
        target_tables = target_connection.fetch_table_names(target_schema)

        target_table_record = None
        for _, record in target_tables.items():
            if record['schema_name'] == target_schema and record['table_name'] == table_name:
                target_table_record = record
                break

        if not target_table_record:
            raise ValueError(f"Table {target_schema}.{table_name} not found in target database.")

        target_table_indexes = target_connection.fetch_indexes(target_table_record['id'], target_schema, table_name)
        for _, index in target_table_indexes.items():
            if index['type'] == 'PRIMARY KEY':
                target_primary_key_columns = index['columns']
                break

        target_columns = target_connection.fetch_table_columns(target_schema, table_name, migrator_tables=None)
        logger.logger.debug(f"Target columns: {target_columns}")

        logger.logger.info(f"Fetching data from target table {target_schema}.{table_name}...")
        target_data = fetch_table_data(logger, target_connection, target_schema, table_name, target_columns, target_primary_key_columns)

        part_name = 'compare data'
        logger.logger.info("Comparing data...")
        compare_data(source_data, target_data, logger)

        logger.logger.info("Data Check Done")

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
        logger.logger.debug(f"Query: {query}")
        cursor = connection.connection.cursor()
        cursor.execute(query)

        part_name = 'fetch data'
        data = cursor.fetchall()
        records = [dict(zip([column['name'] for column in columns.values()], row)) for row in data]
        # logger.logger.debug(f"Data fetched from {schema}.{table_name}:")
        # logger.logger.debug(data)
        # connection.disconnect()

        # df = pl.read_database(query, connection)

        logger.logger.info(f"Fetched {len(records)} rows from source table {table_name}.")
        # self.logger.info(f"Worker {worker_id}: Migrating batch starting at offset {offset} for table {table_name}.")

        part_name = 'convert data'
        # Convert Polars DataFrame to list of tuples for insertion
        # records = df.to_dicts()

        # Adjust binary or bytea types
        for record in records:
            for _, column in columns.items():
                column_name = column['name']
                column_type = column['type']
                # logger.logger.debug(f"Column {column_name} type: {column_type}")
                if column_type.lower() in ['blob']:
                    record[column_name] = bytes(record[column_name].getBytes(1, int(record[column_name].length())))  # Convert 'com.informix.jdbc.IfxCblob' to bytes
                elif column_type.lower() in ['bytea']:
                    record[column_name] = bytes(record[column_name])
                elif column_type.lower() in ['clob']:
                    record[column_name] = record[column_name].getSubString(1, int(record[column_name].length()))  # Convert IfxCblob to string
                elif column_type.lower() in ['date', 'datetime', 'time', 'timestamp', 'timestamp without time zone', 'timestamp with time zone']:
                    if not isinstance(record[column_name], str):
                        record[column_name] = str(record[column_name])

        return pl.DataFrame(records)
    except Exception as e:
        logger.logger.error(f"An error in fetch_table_data ({part_name}): {e}")
        connection.disconnect()
        sys.exit(1)


def compare_data(source_data, target_data, logger):
    tables_empty = source_data.is_empty() and target_data.is_empty()
    if tables_empty:
        if source_data.is_empty():
            logger.logger.info("Source table is empty.")
        if target_data.is_empty():
            logger.logger.info("Target table is empty.")
        return

    # Ensure both columns have the same type
    if 'content_blob' in source_data.columns:
        for column in source_data.columns:
            if column == 'content_blob':
                source_data = source_data.with_columns(pl.col(column).cast(pl.Binary))
    if 'content_blob' in target_data.columns:
        for column in target_data.columns:
            if column == 'content_blob':
                target_data = target_data.with_columns(pl.col(column).cast(pl.Binary))

    if source_data.equals(target_data):
        logger.logger.info("Data matches between source and target tables.")
    else:
        logger.logger.error("Data mismatch found between source and target tables.")
        differences = pl.concat([source_data, target_data]).unique(keep="none")
        if not differences.is_empty():
            logger.logger.info(f"Differences found between source and target tables:")
            logger.logger.info(differences)
            logger.logger.info(f"In total, {int(len(differences)/2)} differences found between source and target tables.")
        else:
            logger.logger.info("No differences found between source and target tables.")

def ctrlc_signal_handler(sig, frame):
    print("Program interrupted with Ctrl+C")
    traceback.print_stack(frame)
    sys.exit(0)

if __name__ == "__main__":
    main()
