# Configuration examples

Ready-to-use configuration files for credativ-pg-migrator, one per source
database plus one per special workflow. **Copy the file matching your source,
adjust the marked lines, run it.**

Every place that must or should be changed carries the marker `>>> ADJUST`, so
you can find all of them at once:

```bash
grep -n '>>> ADJUST' oracle_to_postgresql.yaml
```

A file with no `>>> ADJUST` left unreviewed is ready to run:

```bash
credativ-pg-migrator --config my_migration.yaml
```

Always start with a dry run — it does the whole analysis and planning, creates
nothing, and tells you what the real run would do:

```bash
credativ-pg-migrator --config my_migration.yaml --dry-run
```

---

## Which file do I take?

### Live connection to the source

| File | Source | Connectivity | Prerequisite to install |
|---|---|---|---|
| [postgresql_to_postgresql.yaml](postgresql_to_postgresql.yaml) | PostgreSQL | native | – |
| [oracle_to_postgresql.yaml](oracle_to_postgresql.yaml) | Oracle | native / jdbc / odbc | `pip install oracledb` |
| [mysql_to_postgresql.yaml](mysql_to_postgresql.yaml) | MySQL | native / jdbc / odbc | `pip install mysql-connector-python` |
| [mariadb_to_postgresql.yaml](mariadb_to_postgresql.yaml) | MariaDB | native / jdbc / odbc | `libmariadb-dev` + `pip install mariadb` |
| [mssql_to_postgresql.yaml](mssql_to_postgresql.yaml) | MS SQL Server | odbc / jdbc | an ODBC driver + unixodbc |
| [sybase_ase_to_postgresql.yaml](sybase_ase_to_postgresql.yaml) | Sybase / SAP ASE | jdbc / odbc | a JRE + `jconn4.jar` |
| [informix_to_postgresql.yaml](informix_to_postgresql.yaml) | IBM Informix | jdbc / odbc / native | a JRE + the Informix jars |
| [ibm_db2_luw_to_postgresql.yaml](ibm_db2_luw_to_postgresql.yaml) | IBM Db2 LUW | native / jdbc | `pip install ibm_db` |
| [sql_anywhere_to_postgresql.yaml](sql_anywhere_to_postgresql.yaml) | SAP SQL Anywhere | native / odbc | `pip install sqlanydb` + client libs |
| [sqlite_to_postgresql.yaml](sqlite_to_postgresql.yaml) | SQLite (`.db` file) | native | **nothing** |

### Offline — the source is a set of files

Used when there is no network route to the source and you receive a delivery:
DDL text dumps plus unloaded data files. There is no `host`, `port`,
`username` or `password` in the source block at all.

| File | Source | What the delivery looks like |
|---|---|---|
| [ibm_db2_zos_to_postgresql.yaml](ibm_db2_zos_to_postgresql.yaml) | IBM Db2 for z/OS | object DDL + `UNLOAD DELIMITED` files, usually EBCDIC |
| [ibm_db2_i_to_postgresql.yaml](ibm_db2_i_to_postgresql.yaml) | IBM Db2 for i (AS/400) | extracted DDL + `CPYTOIMPF` files named after the 10-char system names |
| [sqlite_ddl_to_postgresql.yaml](sqlite_ddl_to_postgresql.yaml) | SQLite | `.schema` output + CSV files (or a full `.dump`) |

### Workflows other than the standard one

| File | Workflow | Purpose |
|---|---|---|
| [anonymization_workflow.yaml](anonymization_workflow.yaml) | `anonymization` | copy a schema and mask selected columns, to build a test database from production data |
| [mapping_workflow.yaml](mapping_workflow.yaml) | `mapping` | the target schema **already exists**; only the data is loaded into it, matching tables and columns by name |

### Not a source — an option catalogue

| File | Purpose |
|---|---|
| [advanced_options.yaml](advanced_options.yaml) | the options for large, long-running or partial migrations: pre/post SQL scripts, partial data migration, target partitioning, creating the target partitioned, splitting huge data files, scheduled pause/stop, remote-object rewriting. Runnable as it is (everything advanced is commented out) — but meant as a source of **blocks to paste** into the example for your engine. They work with any source. |
| [config_all_options_reference.yaml](config_all_options_reference.yaml) | **not runnable.** Every option that exists, with all mutually exclusive alternatives next to each other. Look options up here; do not use it as a template. |

---

## What is the same in every example

All of them are complete, in this order:

1. **`workflow`** — `standard` (or `anonymization` / `mapping`).
2. **`pre_migration_analysis`** — how many tables to list in the analysis. `0`
   disables a section; the offline examples use all zeros, because the analysis
   asks the *source database* for row counts and there is none.
3. **`migrator`** — where the protocol tables are created. Normally the target
   database with a schema of its own, so the metadata stays next to the result.
   It is a full connection of its own: it may be a completely different server.
4. **`source`** — the engine, the connection, and the schema being migrated.
5. **`target`** — always PostgreSQL, plus `settings` applied with `SET` on
   every connection the migrator opens.
6. **`migration`** — the recipe: what to migrate, how fast, how to convert.
7. **`include_* / exclude_*`** — which tables, views and routines to touch.
8. **`data_types_substitution` / `default_values_substitution`** — the two
   conversion tables, prefilled with the entries typical for that engine.
9. **`validation`** and **`summary`** where useful.

### Three things worth understanding before you start

**`target.settings.search_path` matters more than it looks.** View bodies and
routine bodies are migrated with their names unqualified, exactly as the source
stores them. If your target schema is not on the `search_path`, those objects
fail to be created even though the tables are fine. Set it to your target
schema plus the schema holding your extensions.

**Extensions are never created from the source automatically.** List them under
`migration.required_extensions`. You do not have to guess: the pre-migration
analysis reports every extension of the source, whether it is installed or at
least available in the target, and which objects actually need it — and it
**stops the migrator** if one is missing, before anything is created.

**`set_sequences`, not `set_sequnces`.** The misspelling is silently ignored,
which means sequences quietly keep starting at 1 while the data is already
there. Worth grepping for if you inherit a configuration from somewhere.

---

## Which example demonstrates which option

Every option is shown at least once. If you need one that your engine's file
does not contain, take it from the file listed here — the syntax is identical
for every source.

| Option group | Shown in |
|---|---|
| `env_variables` | oracle, sql_anywhere, advanced_options |
| `sslmode`, unix-socket host | postgresql |
| `jdbc:` block | sybase_ase, informix, mysql, mssql, ibm_db2_luw |
| `odbc:` block, `connection_string_options` | mssql, mysql, mariadb, sybase_ase |
| `system_catalog` | mssql (`INFORMATION_SCHEMA`/`SYS`), ibm_db2_luw (`SYSIBM`/`SYSCAT`) |
| `server`, `db_locale`, `client_locale` | informix |
| `ddl:` block (offline source) | ibm_db2_zos, ibm_db2_i, sqlite_ddl |
| `data_export:` (rows from files) | ibm_db2_zos, ibm_db2_i, sqlite_ddl, informix |
| `character_set` (EBCDIC etc.), `conversion_path`, `on_missing_data_file` | ibm_db2_zos, ibm_db2_i |
| `lob_columns`, `migrate_lob_values` | informix, ibm_db2_zos |
| `big_files_split` | advanced_options |
| `target.settings` incl. `role` | postgresql, sybase_ase |
| `names_case_handling` | oracle, ibm_db2_*, mssql, sybase_ase |
| `varchar_to_text_length`, `char_to_text_length` | oracle and all engine examples |
| `packages_as` | oracle |
| `numeric_1_boolean_columns`, `map_numeric_1_to_boolean` | oracle, mysql |
| `zero_datetime_default`, `zero_datetime_data_value`, `relax_not_null_datetime` | mysql, mariadb |
| `uuid_default_function`, `required_extensions` | postgresql, oracle, mssql |
| `use_aliases_as_target_names` | ibm_db2_i (the key decision there), ibm_db2_zos |
| `validate_objects` | all |
| `chunk_size`, `batch_size`, `parallel_workers` | all; per-table in informix, advanced_options |
| `table_settings` | informix, ibm_db2_zos, ibm_db2_i, advanced_options, mapping |
| `data_types_substitution` | sybase_ase, mssql, oracle, sqlite |
| `default_values_substitution` | mysql, mariadb, mssql, sybase_ase |
| `data_migration_limitation` | advanced_options |
| `target_partitioning` | advanced_options |
| `pre_migration_script`, `post_migration_script` | advanced_options |
| `scheduled_actions` | informix, advanced_options |
| `remote_objects_substitution` | mssql, advanced_options |
| `anonymization`, `regex_mappings` | anonymization_workflow |
| `mapping`, `heuristics`, `forced_table_mappings`, `data_conflict_action`, `suspend_indexes_constraints` | mapping_workflow |
| `validation`, `target_copy` | postgresql, mapping_workflow, advanced_options |
| `summary` (incl. the anonymization counters) | all / anonymization_workflow |

---

## Known gaps in the option set

Checked against the code, not against the reference file:

- **`target_lob_storage`** (storing LOB values as files instead of in the
  database) is documented in `config_all_options_reference.yaml` but is read by
  no code. It has no effect and is therefore not used in any example.
- **`mapping.forced_column_mappings`** is only echoed into the mapping report;
  it is not applied to the column matching.
- **`scheduled_actions.timer_hours`** is not implemented — only `datetime` is
  evaluated.
- The reference file documents target partitioning as `partitioning:` with
  `partitioning_type:`. The code reads **`target_partitioning:`** with
  **`partition_by:`** and **`partitioning_columns:`**, which is what
  `advanced_options.yaml` uses.

## How complete is the migration for my engine?

The examples switch off what a connector cannot do — `migrate_funcprocs: false`
for MySQL, MariaDB, Db2 LUW and SQL Anywhere, for instance — and say so in a
comment. `FEATURE_MATRIX.md` in the repository root carries the full
per-connector, per-feature status, and section 4 of [../README.md](../README.md)
describes each connector's limitations in prose.
