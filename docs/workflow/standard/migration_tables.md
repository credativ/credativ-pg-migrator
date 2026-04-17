# Migration Database Tables

The `credativ-pg-migrator` uses a dedicated PostgreSQL schema (referred to as the migration database) to store metadata, track progress, map schema conversions, and enable the "resume after crash" functionality. These tables are initialized by the `MigratorTables` class.

This document categorizes the protocol tables into their functional groups.

## 1. State & Protocol Logging

*   **`migration_protocol`** (or configured protocol name): The central logging table that tracks the start, execution, success, or failure of every major step and schema object migration.
*   **`main`**: Tracks high-level tasks and subtasks execution status (`task_started`, `task_completed`, `success`).

## 2. Schema Transformation Mapping

These tables store metadata about objects discovered in the source database and map them to their PostgreSQL equivalents, alongside generated DDL statements:
*   **`tables`**: Maps source to target table names and generated `CREATE TABLE` DDL.
*   **`columns`**: Maps column names, data types, nullability, defaults, and identity properties.
*   **`indexes`**: Tracks standard and function-based indexes, including unique constraints.
*   **`constraints`**: Tracks foreign keys and check constraints.
*   **`views`**: Source and transpiled view queries.
*   **`triggers`**: Source and converted trigger definitions.
*   **`funcprocs`**: Source and converted functions and stored procedures.
*   **`sequences`**: Sequence states, increments, and boundary values.
*   **`aliases`**, **`domains`**, **`user_defined_types`**: Mappings for extended structure definitions.
*   **`source_table_partitioning`** / **`target_table_partitioning`**: Strategies for recreating partitions.

## 3. Data Transfer Metrics & Chunks

Critical for massive parallel imports and the ability to resume aborted migrations:
*   **`data_migration`**: Tracks overall row counts dynamically fetched from source vs. rows successfully inserted into the target table, marking if the load completed.
*   **`data_chunks`**: Tracks discrete batches of data boundaries (e.g., `chunk_start`, `chunk_end`, `batch_size`) assigned to individual multithreaded workers (`worker_id`).
*   **`batches_stats`**: Extremely granular performance metrics detailing exact read (`reading_seconds`), transform (`transforming_seconds`), and write (`writing_seconds`) times for each chunk.

## 4. Substitution & Filtering Registers

Populated from the YAML configuration, these tables enforce explicit overriding rules during planning:
*   **`data_types_substitution`**: Rules mapping source types (e.g., `NUMBER`) to target types (e.g., `BIGINT`), filterable down to specific columns.
*   **`data_migration_limitation`**: Stores filtering `WHERE` clauses for partial data migration.
*   **`remote_objects_substitution`**: Remaps database link references.
*   **`default_values_substitution`**: Rewrites specific default data values during schema conversion.

## 5. Offline DDL & Discovery Features

Primarily utilized by specific connectors (like IBM DB2 z/OS) that do not connect to a running source, but instead parse native `.sql` configuration files.
*   **`ddl_tables`**, **`ddl_columns`**, **`ddl_indexes`**, **`ddl_foreign_keys`**, etc.: Staging tables containing the parsed outcome of read `.sql` files before they are translated into the standard schema transformation mapping.

## 6. Schema Matching Engines

Used extensively in recurring or standardized migration projects (e.g., nscale egov migrations) to run automated heuristics:
*   **`matching_tables`**: Maps source and target tables alongside a `similarity_score` (Jaccard). Does not rely on a central run ID, instead embedding source and target schema contexts directly.
*   **`matching_columns`**: Granular tracking mapping fields based exactly on data type, ordinality, and naming overlap, linked completely by text-based schema and table references.
