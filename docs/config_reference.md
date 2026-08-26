<!-- GENERATED FILE - DO NOT EDIT.
     Generated from credativ_pg_migrator/config.schema.json by tools/generate_config_docs.py.
     Edit the schema and re-run the generator. -->

# credativ-pg-migrator - configuration reference

Every option the migrator understands, with its type, its allowed values, its default and where it applies. This file is generated from `credativ_pg_migrator/config.schema.json`, which the migrator also validates your configuration against at startup - so what is written here is what the code reads.

**Looking for a file to start from?** Copy the example matching your source database from [`docs/configs/`](configs/) - those are complete, valid, runnable configurations. This reference is for looking options up, not for copying whole.

## How to read the tables

- **Type** `block` is a nested mapping and has its own section; `list of entries` is a list of such mappings. A type written `a \| b` accepts either form.
- **Default** is what the migrator uses when the key is absent. An empty cell means the option has no default and is simply not applied.
- **Notes** carries `required`, `deprecated`, `not implemented`, and the source engines an option applies to. An option with no engine listed applies to all of them.
- Keys marked **required** must be present; the migrator stops without them.

## Required keys

The following top-level keys must be present: `migrator`, `source`, `target`.

## Top-level keys

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `workflow` | string | `standard`, `mapping`, `anonymization` | `standard` | Which workflow the run executes. 'standard' reads the source objects, converts and creates them on the target and copies the data. 'mapping' maps existing target objects onto existing source objects and copies only data. 'anonymization' is a standalone data-masking copy. |
| `pattern_syntax` | string | `glob`, `regex`, `like` | `glob` | The syntax the patterns of include_tables / exclude_tables and the same pairs for views and functions/procedures are written in. It applies to all six alike.   glob  (default) - shell wildcards: * is any sequence, ? is one character, [abc] a set. This is what the migrator has always applied, so a configuration written before this setting existed keeps its meaning.   regex - Python regular expressions, e.g. 'BIN\\$.*'.   like  - SQL LIKE: % is any sequence, _ is one character, a backslash escapes either. In every syntax a pattern must match the WHOLE object name and matching ignores case. A pattern which looks as if it were written in one of the other syntaxes is reported at startup, because such a pattern is valid in its own right and simply matches nothing. The value is read case-insensitively. Accepted aliases: `fnmatch`, `shell`, `wildcard`, `wildcards` = `glob`; `sql`, `sql_like` = `like`; `re`, `regexp`, `regular_expression` = `regex`. |
| [`pre_migration_analysis`](#pre_migration_analysis) | block |  |  | Read-only survey of the source database, printed before the migration starts. |
| `top_n_tables` | block |  |  | **deprecated**. use `pre_migration_analysis.top_n_tables` instead. Legacy top-level position of the pre-migration rankings. Only get_top_n_tables() reads it; every individual ranking is read from pre_migration_analysis.top_n_tables. Use that instead. |
| [`env_variables`](#env_variables) | list of entries |  |  | Environment variables exported before the migration starts, for libraries that need them (driver search paths, locale). Applied in main.py before any connection is opened. |
| [`migrator`](#migrator) | block |  |  | **required**. PostgreSQL database in which the migrator keeps its own metadata tables. Usually the target database with a separate schema, so the migration protocol lives next to the migrated data. |
| [`source`](#source) | block |  |  | **required**. The database being migrated away from. |
| [`target`](#target) | block |  |  | **required**. The PostgreSQL database being migrated into. |
| [`migration`](#migration) | block |  |  | What the migration does and how it does it. |
| `table_settings` | list of entries \| null |  |  | Per-table overrides of the global settings. An entry applies to every table whose name matches its table_name. A switch is overridden only when the entry really carries it - a table listed here for an unrelated reason keeps the global value rather than silently losing its data, indexes, constraints or triggers. The key may also be left empty, which means the same as an empty list. |
| `include_tables` | "all" \| list of string \| null | `all` | `all` | Which tables to migrate. Absent, empty, null and 'all' all mean every object; so does a pattern which matches everything ('*', '.*', '%'). Patterns are written in the syntax chosen by the top level 'pattern_syntax' (default glob), must match the whole name, and ignore case. exclude_tables is applied afterwards and wins. |
| `exclude_tables` | "all" \| list of string \| null | `all` | `[]` | Tables to leave out, applied after include_tables and winning over it. An absent or empty list excludes nothing. Patterns are written in the syntax chosen by the top level 'pattern_syntax' (default glob), must match the whole name, and ignore case. |
| `include_views` | "all" \| list of string \| null | `all` | `all` | Which views to migrate. Absent, empty, null and 'all' all mean every object; so does a pattern which matches everything ('*', '.*', '%'). Patterns are written in the syntax chosen by the top level 'pattern_syntax' (default glob), must match the whole name, and ignore case. exclude_views is applied afterwards and wins. |
| `exclude_views` | "all" \| list of string \| null | `all` | `[]` | Views to leave out, applied after include_views and winning over it. An absent or empty list excludes nothing. Patterns are written in the syntax chosen by the top level 'pattern_syntax' (default glob), must match the whole name, and ignore case. |
| `include_funcprocs` | "all" \| list of string \| null | `all` | `all` | Which functions and procedures to migrate. Absent, empty, null and 'all' all mean every object; so does a pattern which matches everything ('*', '.*', '%'). Patterns are written in the syntax chosen by the top level 'pattern_syntax' (default glob), must match the whole name, and ignore case. exclude_funcprocs is applied afterwards and wins. |
| `exclude_funcprocs` | "all" \| list of string \| null | `all` | `[]` | Functions and procedures to leave out, applied after include_funcprocs and winning over it. An absent or empty list excludes nothing. Patterns are written in the syntax chosen by the top level 'pattern_syntax' (default glob), must match the whole name, and ignore case. |
| `tables` | list \| null |  | `[]` | Explicit table list used by get_tables_config(). Rarely set - include_tables is the normal way to choose tables. The key may also be left empty, which means the same as an empty list. |
| `data_types_substitution` | list of 5-element lists \| null |  |  | Replaces the data type the migrator would choose. Each entry is [table_name, column_name, source_data_type, target_data_type, comment]. table_name, column_name and source_data_type are individually optional - leave them '' - but at least one must be given, normally column_name or source_data_type. The source type is matched as an exact value, then as a LIKE pattern, then as a regex; matching is case-insensitive and a regex wins over LIKE. The target type is mandatory and is written out exactly as given, length included. The key may also be left empty, which means the same as an empty list. |
| `default_values_substitution` | list of 4-element lists \| null |  |  | Replaces the DEFAULT clause of a column. Each entry is [column_name, source_column_data_type, source_default_value, target_default_value]. The replacement is written into the DDL as it stands, so a string literal inside it uses single apostrophes; doubling them is needed only inside a YAML value quoted with apostrophes, which YAML undoes again. A value readable only with halved apostrophes is corrected with a warning. The key may also be left empty, which means the same as an empty list. |
| `remote_objects_substitution` | list of 2-element lists \| null |  |  | **deprecated**. Rewrites references to objects living in another database, which PostgreSQL cannot express. Each entry is [source_object, target_object] and is applied as a plain search and replace over the whole statement - so it also rewrites what stands inside a string literal or a comment, it matches a substring rather than a name (a rule for 'arch' fires inside 'archive_2024'), the result depends on the order the entries are written in, and the query of a view is given the list twice. A reference to the database being MIGRATED no longer needs it: 'db..table' and 'db.owner.table' naming the migrated database are resolved by the conversion itself. Use it only for a reference to ANOTHER database, and read the protocol table remote_objects_applied to see what it did. development/REMOTE_OBJECTS_SUBSTITUTION.md describes what is meant to replace it. The key may also be left empty, which means the same as an empty list. |
| `data_migration_limitation` | list of lists \| null |  |  | Copies only part of a table. Each entry is [table_name_or_pattern, condition, column_name_or_pattern] and may carry a row limit as its fourth element - the condition is used only for tables that match the pattern, really have the column, and have more rows than the limit. The condition is written without WHERE and may contain the placeholders {source_schema_name} and {source_table_name}. Several entries matching one table are combined with AND. The key may also be left empty, which means the same as an empty list. (Three elements, or four with the row limit. Anything else stops the run.) |
| `target_partitioning` | list of entries \| null |  |  | Creates the target table partitioned, whether or not the source table was. One entry per table. The key may also be left empty, which means the same as an empty list. |
| [`validation`](#validation) | block |  |  | Post-migration data-integrity check, run by the --validate switch instead of a migration. |
| [`query_conversion`](#query_conversion) | block |  |  | only for `mssql`, `sybase_ase`, `informix`, `ibm_db2_luw`, `ibm_db2_zos`, `ibm_db2_i`, `mysql`, `mariadb`, `oracle`, `sql_anywhere`, `sqlite`, `postgresql`. Conversion of the SELECT statements an application holds as text. A separate step over a finished migration - it creates nothing and moves no data: it reads files of statements, converts every SELECT for the migrated PostgreSQL schema, tests the result against the target and writes the answer into new files. Started by --convert-queries, or as the closing step of a migration when run_after_migration is true. |
| [`mapping`](#mapping) | block |  |  | Settings of the 'mapping' workflow, which matches existing target objects onto existing source objects and copies only data. |
| [`anonymization`](#anonymization) | block |  |  | Settings of the 'anonymization' workflow, which copies the data while masking the columns named here. A method name that is not registered stops the run before any data is read. |
| [`summary`](#summary) | block |  |  | The closing summary of a migration: how many rows each ranking shows, and where the detailed part of it is written. |

---

## `pre_migration_analysis`

Read-only survey of the source database, printed before the migration starts.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| [`top_n_tables`](#pre_migration_analysistop_n_tables) | block |  |  | How many tables to list in each ranking. 0 skips the ranking; a large number lists all tables. |

### `pre_migration_analysis.top_n_tables`

How many tables to list in each ranking. 0 skips the ranking; a large number lists all tables.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `by_rows` | integer | >= 0 | `0` | List the N largest tables by row count. |
| `by_size` | integer | >= 0 | `0` | List the N largest tables by on-disk size. |
| `by_columns` | integer | >= 0 | `0` | List the N tables with the most columns. |
| `by_indexes` | integer | >= 0 | `0` | List the N tables with the most indexes. |
| `by_constraints` | integer | >= 0 | `0` | List the N tables with the most constraints. |

---

## `env_variables[]`

Environment variables exported before the migration starts, for libraries that need them (driver search paths, locale). Applied in main.py before any connection is opened.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `name` | string |  |  | **required**. Name of the environment variable, e.g. LD_LIBRARY_PATH. |
| `value` | string |  |  | **required**. Value to export. |

---

## `migrator`

PostgreSQL database in which the migrator keeps its own metadata tables. Usually the target database with a separate schema, so the migration protocol lives next to the migrated data.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `type` | string | `postgresql` | `postgresql` | **required**. Only PostgreSQL is supported for the migrator metadata database. |
| `host` | string |  |  | **required**. Host name or address. |
| `port` | integer | >= 1, <= 65535 | `5432` | **required**. TCP port. |
| `username` | string |  |  | **required**. Login role. |
| `password` | string |  |  | **required**. Password for the login role. |
| `database` | string |  |  | **required**. Database holding the migrator metadata tables. |
| `schema` | string |  | `migration` | **required**. Schema for the migrator metadata tables. Created if it does not exist. It must be a schema of its own: it is dropped with everything in it at the start of every run, so 'public' and an empty value are refused. |
| `indent` | string |  |  | String used as one indentation level when the migrator formats generated PL/pgSQL. Defaults to the value of MigratorConstants.get_default_indent(). |
| `sslmode` | string | `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full` | `prefer` | sslmode of the PostgreSQL connection URI. |

---

## `source`

The database being migrated away from.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `type` | string | `informix`, `sybase_ase`, `mssql`, `ibm_db2_luw`, `ibm_db2_zos`, `ibm_db2_i`, `mysql`, `mariadb`, `sql_anywhere`, `postgresql`, `oracle`, `sqlite` |  | **required**. Source database engine. Selects the connector and with it which of the engine-specific options below apply. |
| `host` | string |  |  | Host name or address. Not used when type is sqlite. |
| `port` | integer | >= 1, <= 65535 |  | TCP port. Not used when type is sqlite. |
| `username` | string |  |  | Login name. Not used when type is sqlite. |
| `password` | string |  |  | Password. Not used when type is sqlite. |
| `database` | string |  |  | Database name. For sqlite this is the path to the database file - a relative path is resolved against the directory of the config file. |
| `server` | string |  |  | only for `informix`. INFORMIXSERVER name. Informix needs both database and server - the server name cannot be derived from host and port, and is read by direct indexing, so an Informix source without it fails to build its connection string. |
| `schema` | string |  | `public` | Source schema. For sqlite the only valid values are 'main' and the name of an attached database. Synonym of 'owner' - set one, not both. |
| `owner` | string |  |  | Synonym of 'schema', for engines that call it the owner. Read only when 'schema' is absent. |
| `version` | string |  |  | Source server version. Normally detected at connect time and written back; set it only to override the detection. |
| `settings` | map |  |  | PostgreSQL settings applied to every session the migrator opens on the source, as name: value. Only for a PostgreSQL source - the settings of one side are never applied on the connection to the other. A name PostgreSQL does not know is reported as a warning and not applied. |
| `connectivity` | string | `jdbc`, `odbc`, `native`, `ddl` |  | How the source is reached. 'native' needs no further sub-block. 'jdbc' and 'odbc' take the sub-block of the same name. 'ddl' reads the objects from script files instead of a live database. |
| `connection_string_options` | string |  |  | only for `mssql`. Extra options appended to the connection string, as 'key1=value1;key2=value2'. Currently used only by MS SQL Server over JDBC. Repeated semicolons are cleaned up. |
| [`jdbc`](#sourcejdbc) | block |  |  | JDBC driver for connectivity: jdbc. |
| [`odbc`](#sourceodbc) | block |  |  | ODBC driver for connectivity: odbc. |
| [`ddl`](#sourceddl) | block |  |  | Script files for connectivity: ddl. |
| `system_catalog` | string |  | `NONE` | only for `ibm_db2_luw`, `ibm_db2_zos`, `ibm_db2_i`, `mssql`. Which system catalog the connector queries. IBM DB2 LUW: SYSCAT or SYSIBM (SYSIBM simulates the information_schema). MS SQL Server: SYS or INFORMATION_SCHEMA. Upper-cased when read. |
| `db_locale` | string |  | `en_US.utf8` | only for `informix`. Database locale used for date and time formatting. |
| `client_locale` | string |  | `en_US.utf8` | only for `informix`. Client locale announced to the source server. |
| `oracle_thick_mode` | boolean |  | `false` | only for `oracle`. false connects in Thin mode, needing no Oracle Client. true requires the Oracle Client libraries to be installed (Thick mode). |
| `zero_datetime_default` | string \| null |  |  | use `migration.zero_datetime_default` instead. Per-source fallback for migration.zero_datetime_default. Read only when migration.zero_datetime_default is absent. |
| [`data_export`](#sourcedata_export) | block |  |  | Read the table data from export files instead of from the source database. Set exactly one format and the file pattern belonging to it; the CSV, UNL and SQL forms are alternatives, not a sequence. When a file is found it is used in place of the source table. For Informix, a table with a CLOB or BLOB column takes a separate import path so the LOB values are handled properly. |
| `sslmode` | string | `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full` | `prefer` | only for `postgresql`. sslmode of the PostgreSQL connection URI. |

### `source.jdbc`

JDBC driver for connectivity: jdbc.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `driver` | string |  |  | Fully qualified driver class, e.g. com.sybase.jdbc4.jdbc.SybDriver. |
| `libraries` | string |  |  | Path to the driver jar, or several separated by the platform path separator. |

### `source.odbc`

ODBC driver for connectivity: odbc.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `driver` | string |  |  | ODBC driver name as registered with the driver manager, e.g. FreeTDS. |
| `libraries` | string |  |  | Path to the driver shared object. |

### `source.ddl`

Script files for connectivity: ddl.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `path` | string |  |  | A directory, a directory with a file mask, or one file. Relative paths are resolved against the directory of the config file. No placeholders are supported here. |

### `source.data_export`

Read the table data from export files instead of from the source database. Set exactly one format and the file pattern belonging to it; the CSV, UNL and SQL forms are alternatives, not a sequence. When a file is found it is used in place of the source table. For Informix, a table with a CLOB or BLOB column takes a separate import path so the LOB values are handled properly.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `on_missing_data_file` | string | `error`, `skip`, `source_table_name` | `source_table_name` | What to do when the data file for a table does not exist. 'error' stops the migration; 'skip' leaves the table out; 'source_table_name' reads that table from the source database instead. |
| `format` | string | `CSV`, `UNL`, `SQL` |  | Format of the export files. CSV is fed straight into COPY. UNL is the Informix unload format and is converted to CSV first. SQL is a schema dump carrying DDL - the data itself sits where the source engine puts it (for Informix, UNL files in the same directory), and a live source connection is still required because the data model is not read from the dump. |
| `file` | string |  |  | Path of the data files. May contain the placeholders {{source_schema_name}}, {{source_table_name}} and {{source_alias_name}} - those three, and no others. {{source_alias_name}} resolves through the registered aliases of the table, which on IBM i is also its 10-character system name, and is tried when the name built from the table name does not exist. The extension does not have to match the format. For an Informix SQL dump this is one file for the whole database, without placeholders. |
| `delimiter` | string |  | `|` | Field delimiter. Conventionally ',' for CSV and '|' for UNL and for the UNL files belonging to an SQL dump. |
| `header` | boolean |  | `false` | The CSV files carry a header line with the column names. CSV files converted from UNL never have one. |
| `character_set` | string |  | `UTF-8` | Character set of the export files. Must be a name the system knows, e.g. ISO-8859-1. A non-UTF-8 set makes conversion_path advisable. |
| `date_format` | string |  |  | The order of the parts of a date in the files - the DATFMT the export was taken with. MDY or USA reads 01/04/22 as 4 January, DMY or EUR as 1 April, YMD, ISO or JIS as 22 April. The leading '*' of the Db2 names is accepted ('*MDY'). When absent the order is worked out from the values themselves; a column whose values fit more than one order stops that table with a message naming the column, because reading such a date the wrong way would migrate a different date without any error. A two-digit year is expanded as Db2 for i does it: 40-99 is 1940-1999, 00-39 is 2000-2039. |
| `conversion_path` | string |  |  | Directory for the CSV files converted from UNL. Defaults to the directory of the data files. |
| `clean` | boolean |  | `false` | Delete the converted CSV files when the migration is finished. |
| `workers` | integer | >= 1 | `4` | Workers used while reading the export files. |
| `lob_columns` | list of 2-element lists |  |  | Columns to treat as LOB columns during the import, given as [table_name, column_name] pairs. Needed where the declared type does not say so - the column holds a reference to the file with the value rather than the value itself. |
| [`big_files_split`](#sourcedata_exportbig_files_split) | block |  |  | Split an export file larger than the threshold into chunks processed in parallel. Relevant for UNL files and SQL dumps; importing a large CSV into PostgreSQL is fast enough that splitting does not pay. |

#### `source.data_export.big_files_split`

Split an export file larger than the threshold into chunks processed in parallel. Relevant for UNL files and SQL dumps; importing a large CSV into PostgreSQL is fast enough that splitting does not pay.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `enabled` | boolean |  | `false` | Turn splitting on. |
| `threshold` | string |  |  | Size above which a file is split, as a number with a unit: '5GB', '500MB'. |
| `chunk_size` | string |  |  | Size of one chunk, same notation. Around '2GB' is a reasonable value. |
| `workers` | integer | >= 1 | `4` | Workers processing the chunks in parallel. |

---

## `target`

The PostgreSQL database being migrated into.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `type` | string | `postgresql` | `postgresql` | **required**. Only PostgreSQL is supported as a target. |
| `host` | string |  |  | **required**. Host name or address. |
| `port` | integer | >= 1, <= 65535 | `5432` | **required**. TCP port. |
| `username` | string |  |  | **required**. Login role. Needs the rights to create the target schema and its objects. |
| `password` | string |  |  | **required**. Password for the login role. |
| `database` | string |  |  | **required**. Target database. Must already exist. |
| `schema` | string |  | `public` | **required**. Target schema. Synonym of 'owner' - set one, not both. An empty value is refused: the objects of the migration would be created wherever the search_path happens to point. |
| `owner` | string |  |  | Synonym of 'schema'. Read only when 'schema' is absent. |
| `settings` | map |  |  | PostgreSQL settings applied to every session the migrator opens on the target, as name: value. Because every connection runs with them, 'role' is also the owner of every object created in the target - the login role has to be a member of it. 'role' is applied last, so a setting needing more rights is not blocked by the switch to it. A name PostgreSQL does not know is reported as a warning and not applied. |
| `sslmode` | string | `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full` | `prefer` | sslmode of the PostgreSQL connection URI. |

---

## `migration`

What the migration does and how it does it.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `drop_schema` | boolean |  | `false` | Drop the target schema with CASCADE before migrating. Destructive. |
| `drop_tables` | boolean |  | `false` | Drop each target table before creating it. |
| `truncate_tables` | boolean |  | `false` | Truncate each target table before copying data into it. |
| `create_tables` | boolean |  | `false` | Create the target tables. |
| `migrate_data` | boolean |  | `false` | Copy the table data. Can be overridden per table in table_settings. |
| `migrate_indexes` | boolean |  | `false` | Migrate the indexes. Can be overridden per table in table_settings. |
| `migrate_constraints` | boolean |  | `false` | Migrate primary, unique, foreign key and check constraints. Can be overridden per table in table_settings. |
| `migrate_triggers` | boolean |  | `false` | Migrate the triggers. Can be overridden per table in table_settings. |
| `migrate_funcprocs` | boolean |  | `false` | Migrate the functions and procedures. Global only. |
| `migrate_views` | boolean |  | `false` | Migrate the views. Global only. |
| `source_partitioning` | string | `preserve`, `flatten` | `preserve` | What becomes of a table which the SOURCE partitions. 'preserve' (the default) builds the same scheme on the target - every partition of the source becomes a partition of the target, sub-partitions included, and the rows go in through the parent so that PostgreSQL routes each one. 'flatten' builds one ordinary table out of it and says so, per table, in the log and in the pre-migration analysis. A partition is never migrated as a table of its own: it is created with its parent. This is not the same setting as target_partitioning, which builds a scheme the source never had - a table named there is re-partitioned whatever this says. Read case-insensitively. A source whose connector does not read partitioning reports that instead, and its partitioned tables arrive flattened. Accepted aliases: `false`, `merge`, `monolith`, `monolithic`, `no`, `none`, `off`, `single`, `single_table` = `flatten`; `as-is`, `as_is`, `copy`, `keep`, `on`, `same`, `true`, `yes` = `preserve`. |
| `set_sequences` | boolean |  | `false` | Set each target sequence to the highest value present in its column after the data is copied. |
| `migrate_lob_values` | boolean |  | `true` | Copy the contents of BLOB and CLOB columns. false migrates them as NULL, which is much faster for a structural trial run. |
| `validate_objects` | string | `retry`, `check`, `off` | `retry` | Final validity pass over views, functions/procedures and triggers, run at the very end of a standard migration. An object whose creation failed because a dependency did not exist yet can become creatable once the whole schema is present. 'retry' re-runs the stored DDL of objects that are still missing, then verifies and records validity; 'check' only verifies; 'off' skips the pass. true is accepted as 'retry', false and null as 'off'. Read case-insensitively. Accepted aliases: `check_only`, `verify` = `check`; `false`, `no`, `none`, `skip` = `off`; `on`, `true`, `yes` = `retry`. |
| `on_error` | string | `stop`, `continue` | `stop` | Whether a failed object or table stops the migration or is recorded and skipped. |
| `on_undecodable_bytes` | string | `substitute`, `fail`, `remove` | `substitute` | What happens to a value, or to a byte of an exported file, which the encoding expected for it cannot read. 'substitute' keeps it with the last resort encoding latin1, which maps every one of the 256 byte values and therefore loses no byte - the characters may be spelled wrongly and the original bytes can be read again - and reports every occurrence. 'fail' refuses it, so the table, chunk or file is recorded as failed and nothing is guessed. 'remove' deletes the byte, which is what the migrator did before 0.16.0; it is reported now rather than silent. Applied where bytes have to become text: the MS SQL Server connector (the wide and extended ODBC types), the SQLite connector (TEXT values, which SQLite does not enforce the encoding of, and the DDL scripts of connectivity 'ddl') and the CSV reader of a file data source, which reads the file in the encoding format_options.character_set declares. Read case-insensitively, validated at startup. |
| `parallel_workers` | integer | >= 1 | `1` | Number of tables migrated concurrently. |
| `batch_size` | integer | >= 1 | `100000` | Rows read from the source and written to the target in one round trip. |
| `chunk_size` | integer |  | `-1` | Copies one table with several SELECTs of this many rows instead of one. It is NOT a performance setting: every chunk repeats the sort and re-reads the rows it skips, so a chunked table is normally SLOWER than an unchunked one, and the more chunks the worse. It is for a source whose driver or server builds the whole result set of one SELECT before the first row arrives, where a single query over a huge table exhausts client memory or the temporary space of the server - chunking bounds what one statement has to hold. The paging needs an order which is unique: the migrator orders by the primary key of the table, or, when it has none, by every non-LOB column - where neither is unique, rows can be read twice or missed between chunks. -1 (the default) copies each table with one SELECT. Must be larger than batch_size - a smaller value disables chunking with a warning. IMPLEMENTED for Oracle, PostgreSQL, MS SQL Server, MySQL, MariaDB, SQLite, Informix and SQL Anywhere. IGNORED - the setting changes no statement - by Sybase ASE and by DB2 for i and DB2 for z/OS, which load their data from export files. For DB2 LUW it depends on the server: the connector pages with LIMIT/OFFSET, which Db2 accepts only with the MySQL compatibility vector enabled. See 'Chunked reading of large tables' in docs/user_guide.md. |
| `names_case_handling` | string | `lower`, `upper`, `keep` | `keep` | Whether object names are lower-cased, upper-cased or kept as the source spells them. Validated at startup - any other value stops the run. Read case-insensitively. |
| `use_aliases_as_target_names` | boolean |  | `false` | When the source maps aliases onto tables, name the target table after the alias instead of the underlying table. |
| `varchar_to_text_length` | integer |  | `-1` | Convert VARCHAR to TEXT when its declared length reaches this value. 0 converts every VARCHAR; -1 keeps every VARCHAR as it is. |
| `char_to_text_length` | integer |  | `-1` | The same for CHAR and NCHAR. 0 converts every CHAR; -1 keeps them. |
| `zero_datetime_default` | string \| null |  | `remove` | only for `mysql`, `mariadb`. What to do with an all-zero MySQL/MariaDB date or datetime DEFAULT ('0000-00-00', '0000-00-00 00:00:00'), which PostgreSQL cannot store. 'remove', null or '' drops the default clause; any other value is used as the replacement default, e.g. '1970-01-01' or 'CURRENT_TIMESTAMP'. |
| `zero_datetime_value` | string \| null |  | `null` | only for `mysql`, `mariadb`. Replacement for all-zero date and datetime *values* in the data, which arrive as NULL. null keeps them NULL; a value such as '1970-01-01' is written instead. |
| `relax_not_null_datetime` | boolean |  | `true` | only for `mysql`, `mariadb`. Drop NOT NULL from target date and datetime columns so that rows carrying an all-zero date, which MySQL allowed in a NOT NULL column, can be stored as NULL instead of failing the copy. |
| `uuid_default_function` | string |  | `gen_random_uuid()` | SQL function generating default UUID values on the target, used where the source had a UUID, SYS_GUID or NEWID default. Adapted automatically to the target column type - ::text is appended for a text column and stripped for a uuid column. Common values: gen_random_uuid() (built in), uuidv7() (PostgreSQL 18+), uuid_generate_v4() (uuid-ossp). |
| `required_extensions` | list of string \| string |  | `[]` | PostgreSQL extensions the migration needs. Checked before any object is created, with CREATE EXTENSION IF NOT EXISTS attempted for each; a missing extension that cannot be created stops the run. The extension implied by uuid_default_function is added automatically. A comma-separated string is accepted and split. |
| `packages_as` | string | `functions`, `schemas` | `functions` | only for `oracle`. How Oracle packages are represented on a target that has none. 'functions' creates one function per routine in the target schema, named <package>_<routine>. 'schemas' creates one schema per package holding the routines under their own names. Calls are rewritten to match in every migrated routine and trigger. Package state - package level variables, constants and cursors - has no equivalent and is not migrated in either mode. Read case-insensitively. Accepted aliases: `function`, `prefix`, `prefixed_functions` = `functions`; `package_schema`, `package_schemas`, `schema` = `schemas`. |
| `map_numeric_1_to_boolean` | boolean |  | `false` | Map every narrow numeric column (precision 1, scale 0) to BOOLEAN, ignoring numeric_1_boolean_columns. Restores the pre-0.16.1 behaviour. Such a column is ambiguous - it carries either a 0/1 flag or a small integer code - so the default maps it to SMALLINT, which is always lossless. |
| `numeric_1_boolean_columns` | list of entries |  | `[]` | Opt individual narrow numeric columns in to BOOLEAN. Values are coerced on insert (0 to false, anything else truthy to true). Matching is a case-insensitive full-match regex, as in table_settings. |
| `pre_migration_script` | string \| null |  | `null` | Path to a SQL script run on the target before the migration starts. |
| `post_migration_script` | string \| null |  | `null` | Path to a SQL script run on the target after the migration finishes. |
| [`scheduled_actions`](#migrationscheduled_actions) | list of entries |  |  | Actions taken at a given wall-clock time, to fit a migration into a maintenance window. An entry without a time is ignored. |
| [`target_lob_storage`](#migrationtarget_lob_storage) | block |  |  | **not implemented**. Where the contents of BLOB and CLOB columns are put on the target. Not implemented - LOB values are always stored in the target database, in bytea or text columns. |
| `mysql_zero_datetime_default` | string \| null |  |  | **deprecated**. use `migration.zero_datetime_default` instead. Former name of zero_datetime_default. Read only when the current name is absent. |
| `zero_datetime_data_value` | string \| null |  |  | **deprecated**. use `migration.zero_datetime_value` instead. Former name of zero_datetime_value. Read only when the current name is absent. |
| `uuid_function` | string |  |  | **deprecated**. use `migration.uuid_default_function` instead. Former name of uuid_default_function. Read only when the current name is absent. |
| `extensions` | list \| string |  |  | **deprecated**. use `migration.required_extensions` instead. Former name of required_extensions. Read only when the current name is absent. |
| `mapping_report_filename` | string |  |  | **deprecated**. use `mapping.report_filename` instead. Former position of mapping.report_filename. Read only when mapping.report_filename is absent. |

### `migration.scheduled_actions[]`

Actions taken at a given wall-clock time, to fit a migration into a maintenance window. An entry without a time is ignored.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `name` | string |  |  | Free-text label shown in the log. |
| `datetime` | string |  |  | When to act, as 'YYYY.MM.DD HH:MM'. |
| `timer_hours` | number |  |  | **not implemented**. Hours from the start of the run. Not implemented - use datetime. |
| `action` | string | `pause`, `stop`, `continue` |  | What to do at that time. |

### `migration.target_lob_storage`

Where the contents of BLOB and CLOB columns are put on the target. Not implemented - LOB values are always stored in the target database, in bytea or text columns.

> **Not implemented.** The block is read but has no effect yet.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `storage` | string | `database`, `file` | `database` | Store the values in the target database, or in files beside it. |
| `path` | string |  |  | Directory for the files. Used only with storage: file. |
| `name` | string |  |  | Name pattern of one file, e.g. '{{source_schema_name}}.{{source_table_name}}.lob'. Used only with storage: file. |

---

## `validation`

Post-migration data-integrity check, run by the --validate switch instead of a migration.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `workers` | integer | >= 1 | `4` | Tables checked concurrently. |
| `batch_size` | integer | >= 1 | `10000` | Rows per batch when a table checksum is computed in Python. |
| `check_row_counts` | boolean |  | `true` | Compare the row counts of the source and target tables. |
| `check_table_checksums` | boolean |  | `false` | Compare a hash aggregated over the string form of every row of the whole table. |
| `check_random_sample` | boolean |  | `false` | Compare individual rows drawn at random, matched by primary key. |
| `check_lob_sizes` | boolean |  | `false` | Compare the byte size of migrated BLOB and CLOB values on the sampled rows. |
| `random_sample_size` | integer | >= 1 | `1000` | Upper bound on the number of rows sampled, used by check_random_sample and check_lob_sizes. |
| `report_filename` | string |  |  | File for the detailed tabular report. The console shows only the summary. |
| [`target_copy`](#validationtarget_copy) | block |  |  | Connection to an untouched copy of the target database from before the migration. Required when workflow is 'mapping' and data conflicts exist; ignored for the standard workflow. |

### `validation.target_copy`

Connection to an untouched copy of the target database from before the migration. Required when workflow is 'mapping' and data conflicts exist; ignored for the standard workflow.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `type` | string | `postgresql` | `postgresql` | Only PostgreSQL is supported. |
| `host` | string |  |  | Host name or address. |
| `port` | integer | >= 1, <= 65535 | `5432` | TCP port. |
| `username` | string |  |  | Login role. |
| `password` | string |  |  | Password. |
| `database` | string |  |  | Database holding the untouched copy. |
| `schema` | string |  |  | Schema holding the untouched copy. |
| `sslmode` | string | `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full` | `prefer` | sslmode of the PostgreSQL connection URI. |

---

## `query_conversion`

Conversion of the SELECT statements an application holds as text. A separate step over a finished migration - it creates nothing and moves no data: it reads files of statements, converts every SELECT for the migrated PostgreSQL schema, tests the result against the target and writes the answer into new files. Started by --convert-queries, or as the closing step of a migration when run_after_migration is true.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `enabled` | boolean |  | `false` | Whether the step may run at all. --convert-queries stops when it is false, so a configuration cannot convert queries by accident. |
| `run_after_migration` | boolean |  | `false` | Also run the step as the closing step of a migration, after the objects were validated. False means it is run only by --convert-queries, which can be repeated without migrating again. |
| `input` | list of string \| null |  |  | The files holding the statements, as paths or glob patterns resolved against the directory of this configuration file. A pattern which names no file is reported as a warning. A bare directory name is not read recursively - write the pattern out, so the log says which files were taken. |
| `encoding` | string |  | `utf-8` | Encoding of the input files. A file which cannot be read in this encoding stops the run instead of being read as damaged text. |
| `statement_separator` | string | `auto`, `semicolon`, `go`, `blank_line`, `whole_file` | `auto` | How a file is cut into statements. 'auto' takes both the semicolon and GO on a line of its own, which is what a file exported from a client of Sybase ASE or MS SQL Server holds. 'whole_file' is one statement per file. A separator inside a string literal, a comment or a $$ quoted body is never a separator. |
| `parameter_style` | string | `auto`, `qmark`, `named`, `at`, `pyformat`, `numeric`, `none` | `auto` | The bind parameter markers the statements use: '?' (qmark), ':name' (named), '@name' (at), '%s' (pyformat), '$1' (numeric) or none. 'auto' recognises them from the file and reports a file which mixes two kinds. |
| `parameter_output` | string | `original`, `numeric` | `original` | How the markers are written back into the converted statement. 'original' gives back what the application holds today, 'numeric' writes $1..$n for an application which is being ported to a PostgreSQL driver at the same time. |
| `source_test` | string | `off`, `prepare` | `prepare` | Whether every statement is compiled against the SOURCE database before it is converted. It separates a statement the migrator could not convert from one which was already broken, or which reads an object the application creates at run time - an answer nothing on the target side can give. It is COMPILE ONLY and never executes: PREPARE, EXPLAIN, SET NOEXEC ON or the prepareStatement of the JDBC driver, whichever the connector of the source has; a source whose connector has none reports 'not run' and the run goes on. 'off' does not connect to the source at all. Read case-insensitively; false and null are accepted as 'off', true as 'prepare'. Accepted aliases: `false`, `no`, `none`, `skip` = `off`; `on`, `parse`, `true`, `yes` = `prepare`. |
| `target_test` | string | `off`, `parse`, `explain` | `explain` | How much of the converted statement is proven against the target. 'parse' sends PREPARE - syntax, every table, column and function, and the types. 'explain' adds that a plan can be produced. Both run inside a read only transaction which is rolled back; neither reads any data. A statement with bind parameters is always tested with PREPARE, because EXPLAIN of one is refused by PostgreSQL as well. Read case-insensitively; false and null are accepted as 'off', true as 'explain'. Accepted aliases: `on`, `true`, `yes` = `explain`; `false`, `no`, `none`, `skip` = `off`. |
| `timeout` | string |  | `30s` | statement_timeout of the test transaction, as PostgreSQL writes it ('30s', '2min'). The unit is required: PostgreSQL reads a bare number as milliseconds, so '30' would end every test after 30 ms and report every statement as one the target refused. |
| `workers` | integer | >= 1 | `4` | Statements converted and tested concurrently, each with a connection of its own. |
| `on_error` | string | `continue`, `stop` | `continue` | 'continue' converts the whole file whatever a single statement does - the file is the deliverable and has to be complete. 'stop' ends the run at the first statement which could not be converted or failed its test, for a pipeline which gates on it. |
| [`output`](#query_conversionoutput) | block |  |  | Where the converted statements are written, and what the files hold. |

### `query_conversion.output`

Where the converted statements are written, and what the files hold.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `directory` | string |  | `` | Directory for the output files, resolved against the directory of this configuration file when it is relative - the same way the input patterns are. Empty writes them next to the file they came from. It is created when it does not exist. |
| `prefix` | string |  | `` | Written in front of the name of the input file. |
| `suffix` | string |  | `_pg` | Written behind the name of the input file, in front of its extension: queries.sql becomes queries_pg.sql. |
| `overwrite` | boolean |  | `false` | Whether an output file which exists already may be replaced. False refuses rather than overwriting. An output path which names an input file is always refused - the files of the user are never written to. |
| `include_original` | boolean |  | `true` | Whether the statement of the source is written into the comment block above the converted one. |
| `sidecar` | string | `json`, `csv`, `off` | `json` | A machine readable file next to the output file, holding one record per statement - what a CI job or a script which patches application sources reads. 'off' writes none. Read case-insensitively; false and null are accepted as 'off', true as 'json'. Accepted aliases: `on`, `true`, `yes` = `json`; `false`, `no`, `none`, `skip` = `off`. |

---

## `mapping`

Settings of the 'mapping' workflow, which matches existing target objects onto existing source objects and copies only data.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `report_filename` | string |  |  | File for the detailed tabular mapping report. |
| `suspend_indexes_constraints` | boolean |  |  | Drop the indexes and constraints of a target table before copying into it and recreate them afterwards. |
| `data_conflict_action` | string | `skip`, `replace`, `merge_keep_target`, `merge_keep_source` |  | Global default for what to do when the target table already holds data. table_settings[].data_conflict_action overrides it per table. |
| [`heuristics`](#mappingheuristics) | block |  |  | How names are normalised before source and target objects are matched. |
| `forced_table_mappings` | list of entries \| null |  | `[]` | Explicit table pairs, overriding the heuristics. An entry carries either source and target, or source_regex and target with backreferences. |
| `forced_column_mappings` | list of entries \| null |  | `[]` | **not implemented**. Explicit column pairs, meant to override the heuristics. Not applied to the column matching - the entries are only echoed into the mapping report. Scoped either to one table or to every table matching a regex. |

### `mapping.heuristics`

How names are normalised before source and target objects are matched.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `table_normalization_rules` | list of string | items: `lowercase`, `strip_trailing_numbers` | `["lowercase", "strip_trailing_numbers"]` | Normalisations applied to table names before matching. |
| `column_normalization_rules` | list of string | items: `lowercase`, `strip_trailing_numbers` | `["lowercase", "strip_trailing_numbers"]` | Normalisations applied to column names before matching. |
| `column_prefixes_to_strip` | list of string |  | `["gov_", "log_"]` | Prefixes removed from column names before matching. |

---

## `anonymization`

Settings of the 'anonymization' workflow, which copies the data while masking the columns named here. A method name that is not registered stops the run before any data is read.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `on_value_too_long` | string | `error`, `fit`, `find_fitting_value` | `error` | What to do when a masked value does not fit the length of the target column. 'error' stops and names the column; 'fit' cuts the value, counted and reported, never silently; 'find_fitting_value' calls the method again until the result fits. |
| `find_fitting_value_attempts` | integer | >= 1 | `10` | How many times a method is retried under on_value_too_long: find_fitting_value before the run stops. |
| `tables` | map |  |  | The columns to mask, as table name -> column name -> rule. |
| `regex_mappings` | list of entries \| null |  | `[]` | Columns to mask chosen by pattern instead of by name. Each entry masks every column whose table and column name match both regexes. Applied in addition to anonymization.tables; a run needs at least one of the two. |

---

## `summary`

The closing summary of a migration: how many rows each ranking shows, and where the detailed part of it is written.

| Key | Type | Allowed values | Default | Notes |
|---|---|---|---|---|
| `report_filename` | string |  |  | Where the detailed part of the summary is written - the [ PARTITIONING ] and [ DETAILED MIGRATION REPORT ] blocks, which name every object of the migration rather than counting it. Without it they are printed with the rest of the summary, which is as long as the schema is. |
| `top_migrated_tables` | integer | >= 0 | `5` | Largest migrated tables. |
| `top_mismatched_tables` | integer | >= 0 | `5` | Tables whose source and target row counts differ. |
| `top_longest_batches` | integer | >= 0 | `10` | Slowest batches. |
| `top_anonymized_tables` | integer | >= 0 | `5` | Tables with the most anonymized values. |
| `top_anonymized_columns` | integer | >= 0 | `5` | Columns with the most anonymized values. |
| `show_anonymization_examples` | integer | >= 0 | `0` | Before/after examples printed per anonymized column. 0 prints none. The examples contain real source values - keep it 0 unless the summary is being read by someone entitled to see them. |
