# Migration Database Tables

The `credativ-pg-migrator` keeps its metadata in a PostgreSQL schema of its own - the *migration
database*, configured in the `migrator` section of the YAML. It records what the migration
planned, what it created, how much data moved, and what it could not carry over. It is also what
the "resume after crash" mode reads.

Two things to know before reading further:

* **The schema is dropped and recreated at the start of every migration run** (`DROP SCHEMA …
  CASCADE`). It holds the record of the *current* run, not a history of all of them. A name of
  `public` is refused for exactly that reason.
* **Every table and every column carries a `COMMENT` written by the migrator itself.** `\dt+`
  and `\d+` in psql show the same descriptions, and the web GUI shows them as hints. The texts
  live in [`protocol_comments.py`](../credativ_pg_migrator/protocol_comments.py), which is the
  source of truth - where a description below and the comment in the database differ, the
  comment is the current one.

**Names.** The journal is called `protocol` and the tables belonging to one migration carry the
`protocol_` prefix. The registers filled from the configuration, the tables of the mapping
workflow, of the validation and of the DDL-file connectors carry fixed names without it.

## 1. The run itself

| table | what it holds |
|---|---|
| `protocol` | The journal - one row per object planned and per action carried out on it, in the order things happened. `object_type` says which kind of object a row is about and `object_protocol_id` points at the row of the detail table for that kind, so a run can be followed from here into the DDL, the counts and the errors. |
| `protocol_main` | The phases of the run and how long each took - one row per task and subtask. Reading it top to bottom says where a run stands and which step was expensive. |

## 2. Objects planned and created

| table | what it holds |
|---|---|
| `protocol_tables` | One row per table: both names, the DDL of both sides, the columns of both as JSON, and the row counts. Comparing `source_table_rows_limited` with `target_table_rows` says whether the data really arrived. |
| `protocol_columns` | One row per column, everything the source declares and everything the column became. The standard workflow keeps the columns as JSON in `protocol_tables`, so this table stays empty unless a workflow fills it. |
| `protocol_indexes` | The indexes of the source and those created in the target. They are created **after** the data, so a table can hold all its rows while its indexes are still missing. |
| `protocol_constraints` | Primary keys, unique, foreign key and check constraints, on both sides. Also created after the data, so a failure here usually points at data the source itself no longer satisfies. |
| `protocol_views` | The views of the source and of the target. Views are created after the tables and checked again at the end of the run - see `final_valid`. |
| `protocol_funcprocs` | Functions and procedures with their conversion to PL/pgSQL. Both sides are kept: what could not be converted is reported as failed, and `target_funcproc_sql` is where the work by hand starts. |
| `protocol_triggers` | The triggers and their conversion. One trigger of the source becomes a function *and* a trigger in PostgreSQL. |
| `protocol_sequences` | The sequences of the target and where each came from - a sequence of the source, or an identity / autoincrement column (`source_is_identity`). `target_sequence_last_value` is what decides whether the first row inserted after the migration collides with an existing key. |
| `protocol_aliases` | Aliases and synonyms. PostgreSQL has no such object, so an alias becomes a view over what it points at. |
| `protocol_domains` | The domains and how their rule was expressed - as a domain, or attached to every column using it (`migrated_as`). |
| `protocol_user_defined_types` | The user defined types. What PostgreSQL can hold as a type is created; the rest is resolved to the type behind it (`target_basic_type`). |
| `protocol_collations` | The collations of the source and those created for them. A column migrated without its collation sorts differently than it did. |
| `protocol_text_search` | Full text search configurations, dictionaries, parsers and templates - the whole set, because a configuration is worthless without the dictionaries it names. |
| `protocol_default_values` | The named default objects of the source (Sybase ASE, SQL Anywhere), whose value is written into every column bound to them. |
| `protocol_new_objects` | Objects which do not come from the source at all but are asked for in the configuration and created with everything else. |
| `protocol_target_columns_alterations` | Columns whose type had to be altered after the tables existed - most often the two sides of a foreign key, which PostgreSQL accepts only with matching types. Every row is a place where the first choice of the mapping did not hold. |
| `protocol_source_table_partitioning` | How a partitioned table is partitioned in the source - one row per level. |
| `protocol_target_table_partitioning` | How it was really partitioned in the target - one row per level, which is not always what the source had. |

## 3. The data

| table | what it holds |
|---|---|
| `protocol_data_migration` | One row per table, written when the migration of that table is planned and filled in as it runs: the counts on both sides and how the batches behaved. |
| `protocol_data_chunks` | The pieces a large table is split into so several workers can move it at once - one row per chunk, each a range of rows of the source. |
| `protocol_batches_stats` | One row per batch really written, with its time split into reading, transforming and writing - which is what says where a slow migration loses its time. |
| `protocol_data_sources` | The files a table is migrated from when the data comes out of an export rather than a live source. `file_found = false` means that table had nothing to read. |
| `protocol_anonymization_stats` | Written by the anonymization workflow: one row per table, column and method with the number of values really replaced, and those cut or regenerated to fit the target column. The summary reports out of this table, so a rule which never fired cannot be shown as a job done. |
| `protocol_pk_ranges` | Ranges of the primary key handed to the workers. The analysis which fills it is switched off in this version and the table is not created. |

## 4. What the configuration overrules

Filled from the YAML while the plan is made, and consulted during it.

| table | what it holds |
|---|---|
| `data_types_substitution` | The type replacements which overrule the standard mapping. Every column is looked up here before its type is decided; the first match wins. |
| `default_values_substitution` | Replacements for column defaults the target cannot take over as written - a function the source has and PostgreSQL does not being the usual case. |
| `data_migration_limitation` | The restrictions on which rows of a table are migrated. A table listed here is migrated only in part, and the counts of the run are measured against the restricted number. |
| `remote_objects_substitution` | **Deprecated.** The configured replacements for references to another server or another database. A plain search and replace over the whole statement - it rewrites a name inside a string literal or a comment as readily as one in the SQL. A reference to the database *being migrated* is resolved by the conversion itself and needs no entry. |
| `remote_objects_applied` | What that substitution really replaced, as opposed to what it was configured to replace - one row per object and rule, with how often it fired. The table above holds the rules, this one the outcome. |

## 5. Sources read from DDL files

Filled by the connectors which migrate from a DDL export instead of a live connection (Db2 for i,
Db2 for z/OS): the script files are parsed once and these tables then take the place of the
catalog of the source for the whole run.

`ddl_tables`, `ddl_columns`, `ddl_indexes`, `ddl_foreign_keys` (every kind of constraint, not
only foreign keys - `source_constraint_type` says which), `ddl_sequences`, `ddl_views`,
`ddl_aliases`, `ddl_triggers`, `ddl_funcprocs`, `ddl_variables` (the global variables of Db2,
which PostgreSQL has no counterpart for).

## 6. The mapping workflow

Used when the target schema **already exists** and the migration has to find out which table and
which column of the target belongs to which of the source. Described in full, with the matching
itself, in [the mapping workflow document](workflow/mapping.md).

| table | what it holds |
|---|---|
| `mapping_pre_stats` | How many objects of each kind each side holds, counted before anything is matched - the measure the result is read against. |
| `mapping_tables` | One row per matched pair of tables. `match_type` says how the pair was found; a pair found by similarity deserves a look before the data is moved. |
| `mapping_columns` | One row per matched pair of columns - what says which column of the target a value of the source is written into. |
| `mapping_target_indexes` | The indexes already in the target, read before the data is migrated. They are dropped for the load and created again from `index_def`. |
| `mapping_target_constraints` | The same for the constraints - a foreign key would refuse rows whose counterpart has not arrived yet. |
| `mapping_target_sequences` | The sequences of the existing target and what uses them, through a default, an identity column or a trigger. They have to be set past the migrated data afterwards. |
| `mapping_unmatched_objects` | Everything which could not be paired. A source object here is data which will **not** be migrated; a target object here is a column or table the migration will not fill. |

## 7. The validation

Written by the validator, which compares the two databases after the migration.

| table | what it holds |
|---|---|
| `validation_tables` | One row per migrated table: rows, number of columns, indexes and constraints, and a hash over the whole content. |
| `validation_columns` | One row per column: the hash of the column, the NULLs and empty strings, and the smallest, largest and average value - where a type conversion which silently changed the data becomes visible. |
| `validation_indexes` | One row per index compared. A missing index costs no data and a great deal of speed. |
| `validation_constraints` | One row per constraint compared. A missing constraint is a rule the data is no longer held to. |

## 8. The query conversion

| table | what it holds |
|---|---|
| `protocol_queries` | The statements of an application which `--convert-queries` read, and what became of each. `status` says it in one word: `CONVERTED`, `UNCHANGED`, `CONVERTED_FAILING` (converted, and the target refused it), `NOT CONVERTED` or `SKIPPED`. The step runs over a finished migration, so this table is created when it is missing and is **never dropped** - unlike the rest of the schema, it survives. |
