HELP_TEXTS = {
    'mig_host': {
        'short': 'Hostname or IP address for Migrator metadata database.',
        'full': 'The hostname or IP address of the database server where Migrator will store its metadata. Usually, this is the same as the target PostgreSQL database.'
    },
    'mig_port': {
        'short': 'Port for Migrator metadata database.',
        'full': 'The connection port for the Migrator metadata database.'
    },
    'mig_db': {
        'short': 'Database name for Migrator metadata.',
        'full': 'The name of the database where Migrator metadata will be stored.'
    },
    'mig_schema': {
        'short': 'Schema for Migrator metadata.',
        'full': 'The schema name for Migrator metadata. It is recommended to use a separate schema (e.g. "migration") to isolate metadata from actual application data.'
    },
    'mig_user': {
        'short': 'Username for Migrator database.',
        'full': 'Username used to connect to the Migrator metadata database.'
    },
    'mig_pass': {
        'short': 'Password for Migrator database.',
        'full': 'Password used to connect to the Migrator metadata database.'
    },
    's_type': {
        'short': 'Type of the source database engine.',
        'full': 'Supported types: informix, sybase_ase, mssql, ibm_db2_luw, ibm_db2_zos, mysql, sql_anywhere, postgresql, oracle.'
    },
    's_conn': {
        'short': 'Connectivity method for the source database.',
        'full': '"jdbc", "odbc", "native", or "ddl". "native" does not require additional drivers. "ddl" reads objects from a DDL file instead of the source database.'
    },
    's_host': {
        'short': 'Source database hostname or IP address.',
        'full': 'The hostname or IP address of the source database server.'
    },
    's_port': {
        'short': 'Source database port.',
        'full': 'The connection port for the source database.'
    },
    's_db': {
        'short': 'Source database name or service name.',
        'full': 'The name of the source database. For Oracle, this can be the service name.'
    },
    's_schema': {
        'short': 'Source schema or owner name.',
        'full': 'The schema or owner name from which to read objects. Note: schema and owner are synonyms depending on the source database - only one can be used.'
    },
    's_user': {
        'short': 'Source database username.',
        'full': 'Username used to connect to the source database.'
    },
    's_pass': {
        'short': 'Source database password.',
        'full': 'Password used to connect to the source database.'
    },
    's_conn_opts': {
        'short': 'Additional connection string options.',
        'full': 'Currently used only for MS SQL Server (JDBC connectivity). Appended to the connection string. Multiple options should be separated by semicolons (e.g., "encrypt=true;trustServerCertificate=true").'
    },
    's_system_catalog': {
        'short': 'System catalog simulation mapping.',
        'full': 'Used only for some databases. ibm_db2_luw uses SYSCAT or SYSIBM. mssql uses SYS or INFORMATION_SCHEMA.'
    },
    's_db_locale': {
        'short': 'Database locale for date/time formatting.',
        'full': 'Database locale used for date and time formatting. Currently supported and working primarily for Informix databases.'
    },
    't_type': {
        'short': 'Type of the target database.',
        'full': 'The target database is currently always PostgreSQL.'
    },
    't_host': {
        'short': 'Target database hostname or IP address.',
        'full': 'The hostname or IP address of the target PostgreSQL database server.'
    },
    't_port': {
        'short': 'Target database port.',
        'full': 'The connection port for the target PostgreSQL database.'
    },
    't_db': {
        'short': 'Target database name.',
        'full': 'The name of the target PostgreSQL database.'
    },
    't_schema': {
        'short': 'Target schema name.',
        'full': 'The schema where the migrated objects will be created in the target database.'
    },
    't_user': {
        'short': 'Target database username.',
        'full': 'Username used to connect to the target PostgreSQL database.'
    },
    't_pass': {
        'short': 'Target database password.',
        'full': 'Password used to connect to the target PostgreSQL database.'
    },
    't_work_mem': {
        'short': 'Work memory limit for the target database.',
        'full': 'Sets the "work_mem" parameter for the target PostgreSQL session, optimizing sort operations during data loading.'
    },
    't_maint_work_mem': {
        'short': 'Maintenance work memory limit.',
        'full': 'Sets the "maintenance_work_mem" parameter for the target PostgreSQL session, speeding up index and constraint creation.'
    },
    't_role': {
        'short': 'Role to assume in the target database.',
        'full': 'Sets the role for the target PostgreSQL session (e.g. SET ROLE my_role).'
    },
    't_search_path': {
        'short': 'Search path for the target database.',
        'full': 'Sets the schema search path for the target PostgreSQL session.'
    },
    'j_driver': {
        'short': 'JDBC driver class name.',
        'full': 'The full class name of the JDBC driver (e.g. "com.sybase.jdbc4.jdbc.SybDriver").'
    },
    'j_libs': {
        'short': 'Path to JDBC library files.',
        'full': 'Path to the .jar files required by the JDBC driver. Separate multiple paths with appropriate OS separators.'
    },
    'o_driver': {
        'short': 'ODBC driver name.',
        'full': 'The name of the ODBC driver to use (e.g. "FreeTDS").'
    },
    'o_libs': {
        'short': 'Path to ODBC library files.',
        'full': 'Absolute path to the specific ODBC .so or .dll library files.'
    },
    'd_path': {
        'short': 'Path to DDL files.',
        'full': 'Either a directory, a directory with a mask, or a specific file path from which to read DDL objects. No placeholders are currently supported.'
    },
    'env_vars': {
        'short': 'Environment variables (NAME=VALUE format).',
        'full': 'List of environment variables to set before running the migration. Useful for setting up library paths (e.g., LD_LIBRARY_PATH) or locale settings (e.g., LANG).'
    },
    'm_workflow': {
        'short': 'Migration workflow type.',
        'full': 'Select the workflow to execute. "standard" for full migration, "mapping" for schema matching analysis, or "anonymization" for data masking routines.'
    },
    'm_on_error': {
        'short': 'Error handling behavior.',
        'full': '"continue" ignores non-critical errors and proceeds. "stop" halts the entire migration upon encountering the first error.'
    },
    'm_names_case': {
        'short': 'Object naming case normalization.',
        'full': 'Controls how object names are transferred: "lower" to force all lowercase, "upper" to force uppercase, or "keep" to preserve original casing.'
    },
    'm_batch_size': {
        'short': 'Rows per data batch.',
        'full': 'Number of rows to process in a single batch during data migration.'
    },
    'm_chunk_size': {
        'short': 'Rows per partitioned chunk (-1 disables).',
        'full': 'Size of chunks to split data into for parallel extraction. Must be larger than batch size. Set to -1 to disable chunking.'
    },
    'm_workers': {
        'short': 'Number of parallel workers.',
        'full': 'Number of parallel processes used for extracting data and recreating indexes.'
    },
    'm_varchar_len': {
        'short': 'Varchar to Text conversion threshold.',
        'full': 'VARCHAR columns longer than this length will be converted to TEXT. Set to -1 to disable conversion.'
    },
    'm_char_len': {
        'short': 'Char to Text conversion threshold.',
        'full': 'CHAR columns longer than this length will be converted to TEXT. Set to -1 to disable conversion.'
    },
    'm_drop_schema': {
        'short': 'Drop schema if exists.',
        'full': 'Drops the target schema if it already exists before starting the migration.'
    },
    'm_recreate_schema': {
        'short': 'Recreate target schema.',
        'full': 'Creates the target schema if it does not exist.'
    },
    'm_drop_tables': {
        'short': 'Drop target tables.',
        'full': 'Drops target tables before migrating data.'
    },
    'm_truncate_tables': {
        'short': 'Truncate target tables.',
        'full': 'Truncates (empties) target tables before migrating data instead of dropping them.'
    },
    'm_create_tables': {
        'short': 'Create target tables.',
        'full': 'Creates the schema tables in the target database.'
    },
    'm_migrate_data': {
        'short': 'Migrate data rows.',
        'full': 'Extracts data from the source and loads it into the target tables.'
    },
    'm_migrate_indexes': {
        'short': 'Migrate indexes.',
        'full': 'Translates and creates indexes on the target database.'
    },
    'm_migrate_constraints': {
        'short': 'Migrate constraints.',
        'full': 'Translates and creates primary, foreign, and unique constraints.'
    },
    'm_migrate_funcprocs': {
        'short': 'Migrate functions and procedures.',
        'full': 'Attempts to migrate stored procedures and functions to PostgreSQL.'
    },
    'm_migrate_triggers': {
        'short': 'Migrate triggers.',
        'full': 'Attempts to migrate triggers to PostgreSQL.'
    },
    'm_migrate_views': {
        'short': 'Migrate views.',
        'full': 'Attempts to migrate view definitions to PostgreSQL.'
    },
    'm_set_sequences': {
        'short': 'Update sequence values.',
        'full': 'Updates PostgreSQL sequences to match the maximum IDs from migrated tables.'
    },
    'm_use_aliases': {
        'short': 'Use aliases for target names.',
        'full': 'If enabled, mapping aliases are used to rename tables and columns in the target database.'
    },
    'm_migrate_lob': {
        'short': 'Migrate LOB (Large Object) values.',
        'full': 'Extracts and migrates large binary or character objects (BLOBs/CLOBs).'
    },
    'v_row_counts': {
        'short': 'Verify total row counts.',
        'full': 'Compares total row counts between corresponding source and target tables.'
    },
    'v_table_checksums': {
        'short': 'Verify table checksum hashes.',
        'full': 'Computes explicit hashes containing aggregates of string-casted structures across the entire tables.'
    },
    'v_random_sample': {
        'short': 'Verify random sample row data.',
        'full': 'Validates individual random row arrays by querying mapped precise primary keys bridging origin to destination.'
    },
    'v_lob_sizes': {
        'short': 'Verify exact LOB byte sizes.',
        'full': 'Validates exact byte size allocations for migrated LOB (BLOB/CLOB) instances on the sampled rows.'
    },
    'v_workers': {
        'short': 'Number of parallel validator workers.',
        'full': 'Number of parallel processes used for validation tasks.'
    },
    'v_sample_size': {
        'short': 'Size of random row sample.',
        'full': 'Number of random rows to fetch and verify when "Check Random Sample" is enabled.'
    },
    'inc_tables': {
        'short': 'List of tables to include (one per line).',
        'full': 'Specify tables to include in the migration. You can use regular expressions. An empty list means all tables will be included. Use "all" as a shortcut.'
    },
    'exc_tables': {
        'short': 'List of tables to exclude (one per line).',
        'full': 'Specify tables to exclude from the migration. You can use regular expressions. An empty list means no tables will be excluded.'
    },
    'inc_views': {
        'short': 'List of views to include (one per line).',
        'full': 'Specify views to include in the migration. You can use regular expressions. An empty list means all views will be included. Use "all" as a shortcut.'
    },
    'exc_views': {
        'short': 'List of views to exclude (one per line).',
        'full': 'Specify views to exclude from the migration. You can use regular expressions. An empty list means no views will be excluded.'
    },
    'inc_funcprocs': {
        'short': 'List of functions to include.',
        'full': 'Specify functions/procedures to include in the migration. You can use regular expressions. Use "all" as a shortcut.'
    },
    'exc_funcprocs': {
        'short': 'List of functions to exclude.',
        'full': 'Specify functions/procedures to exclude from the migration.'
    },
    'use_validator': {
        'short': 'Enable Validator Block.',
        'full': 'If checked, the Validator section will be generated and used during migration.'
    },
    'use_premigration': {
        'short': 'Enable Pre-Migration Analysis.',
        'full': 'If checked, the Pre-Migration Analysis block will be executed.'
    },
    'pma_by_rows': {
        'short': 'List TOP N tables by row count.',
        'full': 'Number of TOP N tables from the source database to list, sorted by the total number of rows. Set to 0 to skip.'
    },
    'pma_by_size': {
        'short': 'List TOP N tables by disk size.',
        'full': 'Number of TOP N tables from the source database to list, sorted by their total size on disk. Set to 0 to skip.'
    },
    'pma_by_columns': {
        'short': 'List TOP N tables by column count.',
        'full': 'Number of TOP N tables from the source database to list, sorted by the number of columns. Set to 0 to skip.'
    },
    'pma_by_indexes': {
        'short': 'List TOP N tables by index count.',
        'full': 'Number of TOP N tables from the source database to list, sorted by the number of indexes attached. Set to 0 to skip.'
    },
    'pma_by_constraints': {
        'short': 'List TOP N tables by constraint count.',
        'full': 'Number of TOP N tables from the source database to list, sorted by the number of constraints (PK, FK, Unique). Set to 0 to skip.'
    },
    'de_use_data_export': {
        'short': 'Enable Data Export Migration.',
        'full': 'If checked, data will be migrated from export files instead of querying the source database directly.'
    },
    'de_on_missing': {
        'short': 'Action on missing data file.',
        'full': '"error" to stop, "skip" to skip table, "source_table_name" to query source database instead.'
    },
    'de_format': {
        'short': 'Data files format.',
        'full': '"CSV", "UNL" (Informix unload), or "SQL" (dump files).'
    },
    'de_file': {
        'short': 'Path pattern for data files.',
        'full': 'Path to data files. Can contain {{source_schema_name}} and {{source_table_name}} placeholders.'
    },
    'de_delimiter': {
        'short': 'Data file delimiter.',
        'full': 'Character used to separate columns in the data file (e.g., "," or "|").'
    },
    'de_header': {
        'short': 'File contains header.',
        'full': 'Check if the CSV/data file contains a header row with column names.'
    },
    'de_charset': {
        'short': 'Character set of data files.',
        'full': 'Character encoding of the files (e.g., "ISO-8859-1" or "UTF-8").'
    },
    'de_conv_path': {
        'short': 'Conversion path for UNL/SQL files.',
        'full': 'Directory where converted CSV files will be temporarily stored.'
    },
    'de_clean': {
        'short': 'Clean up converted files.',
        'full': 'If checked, removes converted CSV files after migration completes.'
    },
    'de_use_split': {
        'short': 'Enable Big Files Split.',
        'full': 'If checked, large export files will be split into smaller chunks for parallel processing.'
    },
    'de_threshold': {
        'short': 'File split threshold.',
        'full': 'Threshold size above which a file is split (e.g., "5GB").'
    },
    'de_chunk': {
        'short': 'Chunk size.',
        'full': 'Size of the resulting split chunks (e.g., "2GB").'
    },
    'de_workers': {
        'short': 'Parallel workers for chunks.',
        'full': 'Number of workers used to process split chunks in parallel.'
    },
    'adv_yaml': {
        'short': 'Advanced configuration properties in YAML.',
        'full': 'Text-based YAML editor for complex configurations such as pre_migration_analysis, mapping_workflow, data_types_substitution, default_values_substitution, data_migration_limitation, remote_objects_substitution, and anonymization scripts.'
    }
}
