# credativ-pg-migrator Configuration Map

This document provides a simplified overview and hierarchy of the configuration parameters available in `config_sample.yaml`.

## 1. Pre-Migration Analysis (`pre_migration_analysis`)
- `top_n_tables`: Settings for listing the top N tables from the source database.
  - `by_rows`: List by number of rows.
  - `by_size`: List by table size.
  - `by_columns`: List by number of columns.
  - `by_indexes`: List by number of indexes.
  - `by_constraints`: List by number of constraints.

## 2. Environment Variables (`env_variables`)
- List of system environment variables to set before migration (e.g., `LD_LIBRARY_PATH`, `LANG`).
  - `name`: Variable name.
  - `value`: Variable value.

## 3. Databases
### Migrator Database (`migrator`)
- `type`, `host`, `port`, `username`, `password`, `database`, `schema`: Connection details for metadata storage (must be PostgreSQL). Migrator creates staging tables here.

### Source Database (`source`)
- `type`, `host`, `port`, `username`, `password`, `database`, `schema`: Basic connection details.
- `connectivity`: Connection method (`jdbc`, `odbc`, `native`, `ddl`).
- `connection_string_options`: Extra options (e.g., for MS SQL JDBC).
- `jdbc` / `odbc`: Driver and specific library paths.
- `ddl`: Directory for DDL files (if connectivity is `ddl`).
- `system_catalog`: Specific system catalog catalog to use (e.g., `SYSIBM` for DB2 LUW).
- `db_locale`: Database locale (e.g., `en_US.utf8`).
- `data_export`: Global settings for using external export files (CSV, UNL, SQL) instead of direct DB connection.
  - `on_missing_data_file`: Action on missing file (`error`, `skip`, `source_table_name`).
  - `format`, `file`, `delimiter`, `header`, `character_set`, `conversion_path`, `clean`: Format and processing settings.
  - `big_files_split`: Settings for parallel chunk processing of big files (`enabled`, `threshold`, `chunk_size`, `workers`).

### Target Database (`target`)
- `type`, `host`, `port`, `username`, `password`, `database`, `schema`: Connection details (must be PostgreSQL).
- `settings`: Target database session configuration parameters (e.g., `work_mem`, `maintenance_work_mem`, `role`, `search_path`).

## 4. Migration Recipe (`migration`)
- `workflow`: Type of migration (`standard` or `mapping`).
- `suspend_indexes_constraints`: Analyze and recreate indexes/constraints (specific to `mapping` workflow).
- `drop_schema`: If true, drops schema utilizing `CASCADE` before migration.
- **Operations toggles**: `drop_tables`, `truncate_tables`, `create_tables`, `migrate_data`, `migrate_indexes`, `migrate_constraints`, `migrate_funcprocs`, `migrate_triggers`, `migrate_views`, `set_sequences`.
- `on_error`: Action on runtime error (`stop`, `continue`).
- `parallel_workers`: Number of workers for parallel table execution.
- `batch_size`: Number of rows per insert batch.
- `chunk_size`: Number of rows per query chunk (used to split migration of huge tables into independent parts).
- `names_case_handling`: Target naming case conversion (`lower`, `upper`, `keep`).
- `varchar_to_text_length` / `char_to_text_length`: Thresholds to convert varying character types to `TEXT` type dynamically.
- `migrate_lob_values`: Whether to migrate Large Objects (BLOB, CLOB).
- `scheduled_actions`: List of time-based execution controls (`pause`, `stop`, `continue`).

## 5. Table-Specific Settings (`table_settings`)
- Overrides global `migration` and `data_export` settings for individual tables (selected by exact name or regex pattern).
- Supported overrides include:
  - `table_name`, `table_schema`, `batch_size`, `chunk_size`
  - Operation flags (e.g. `migrate_data`, `migrate_indexes`).
  - `data_export`: Table-specific export file properties (format, delimiter, `lob_columns`, conversion parameters).
    - `mapping_rules`: *Mapping Workflow Only.* Defines `target_schema`, `target_table`, and `column_mapping` linking specific source columns to target columns.

## 6. Inclusion / Exclusion Filters
- Lists supporting exact names or regular expressions to refine the objects migrating to the target:
  - `include_tables`, `exclude_tables`
  - `include_views`, `exclude_views`
  - `include_funcprocs`, `exclude_funcprocs`

## 7. Custom Data Substitutions & Mappings
- `data_types_substitution`: Rules for replacing specific legacy DB data types with PostgreSQL compliant types. (Matches via regex or LIKE).
- `default_values_substitution`: Rules for replacing specific default values or legacy DB functions in column constraints (e.g. `%getdate()%` to `statement_timestamp()`).
- `remote_objects_substitution`: Mappings for cross-database linked object references used in views/functions.
- `data_migration_limitation`: Targetable arbitrary SQL `WHERE` conditions to limit dataset rows migrated from tables on the fly.
