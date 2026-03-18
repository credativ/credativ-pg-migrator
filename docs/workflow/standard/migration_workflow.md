# credativ-pg-migrator Standard Workflow

This document provides a detailed technical description of the standard offline migration workflow implemented in `credativ-pg-migrator`. 

## 1. Overview Map

The migration is executed in three main phases. Click on any phase or step to jump to its deep technical description.

*   **[Phase 1: Initialization & Configuration](#phase-1-initialization--configuration)**
    *   [1.1 CLI & YAML Configuration Parsing](#11-cli--yaml-configuration-parsing)
    *   [1.2 Environment Setup & Check](#12-environment-setup--check)
*   **[Phase 2: Planning (Metadata Discovery & Transformation)](#phase-2-planning-metadata-discovery--transformation)**
    *   [2.1 Pre-Planning & Protocol Initialization](#21-pre-planning--protocol-initialization)
    *   [2.2 Pre-Migration Analysis](#22-pre-migration-analysis)
    *   [2.3 Dependencies & Type Preparation](#23-dependencies--type-preparation)
    *   [2.4 Table, Sequence, and View Preparation](#24-table-sequence-and-view-preparation)
*   **[Phase 3: Orchestration (Execution & Data Transfer)](#phase-3-orchestration-execution--data-transfer)**
    *   [3.1 Type & Domain Creation](#31-type--domain-creation)
    *   [3.2 Base Structure Creation (Sequences & Tables)](#32-base-structure-creation-sequences--tables)
    *   [3.3 Parallel Data Migration](#33-parallel-data-migration)
    *   [3.4 Post-Data Structure (Indexes & Constraints)](#34-post-data-structure-indexes--constraints)
    *   [3.5 Logic & Post-Processing (Views, Triggers, Scripts)](#35-logic--post-processing-views-triggers-scripts)

---

## Deep Technical Description

### Phase 1: Initialization & Configuration
The tool acts as a CLI application orchestrating the entire migration using a modular architecture.

#### 1.1 CLI & YAML Configuration Parsing
*   **Implementation**: [`credativ_pg_migrator/main.py:main()`](../../../credativ_pg_migrator/main.py#L32) and [`credativ_pg_migrator/config_parser.py`](../../../credativ_pg_migrator/config_parser.py)
*   **Process**: The orchestrator is heavily driven by a single YAML configuration file, passed via the CLI arguments (`--config`). `ConfigParser` transforms the YAML dictionary into accessible properties used across the application to define source, target, migration behavior (e.g., parallel workers, batch size), and specific data filters/mappings.

#### 1.2 Environment Setup & Check
*   **Implementation**: [`credativ_pg_migrator/main.py:main()`](../../../credativ_pg_migrator/main.py#L65-L73) 
*   **Process**: Evaluates `env_variables` from the configuration to prepare the correct execution environment (e.g. library paths for JDBC/ODBC drivers). Initializes the `MigratorLogger` which handles console and file outputs concurrently.

### Phase 2: Planning (Metadata Discovery & Transformation)
The Planning phase reads the source constraints, applies YAML mapping rules, translates SQL, and logs the execution roadmap into `MigratorTables` (PostgreSQL protocol schema).

#### 2.1 Pre-Planning & Protocol Initialization
*   **Implementation**: [`credativ_pg_migrator/planner.py:pre_planning()`](../../../credativ_pg_migrator/planner.py#L128)
*   **Process**: Plugs into target databases using dynamic connector loading in [`load_connector()`](../../../credativ_pg_migrator/planner.py#L107). Drop/Creates target schemas if required. Initializes metadata tables natively in the migration database by initializing `MigratorTables`. Also populates substitution rules (data types, default values) defined in the configuration. 

#### 2.2 Pre-Migration Analysis
*   **Implementation**: [`credativ_pg_migrator/planner.py:run_premigration_analysis()`](../../../credativ_pg_migrator/planner.py#L186)
*   **Process**: Issues statistical queries to the source database. Checks the database size, top N tables by dimensions (rows, size, indexes), and evaluates complex foreign key dependency chains to assist the DB administrator in spotting migration bottlenecks.

#### 2.3 Dependencies & Type Preparation
*   **Implementation**: `run_prepare_user_defined_types()`, `run_prepare_domains()`, `run_prepare_aliases()` inside [`credativ_pg_migrator/planner.py`](../../../credativ_pg_migrator/planner.py#L74-L80)
*   **Process**: Discovers schema definitions of custom datatypes and aliases natively from the source and prepares transformed PostgreSQL-compliant CREATE statements.

#### 2.4 Table, Sequence, and View Preparation
*   **Implementation**: [`credativ_pg_migrator/planner.py:run_prepare_tables()`](../../../credativ_pg_migrator/planner.py#L400)
*   **Process**: 
    1. Fetches all table names dynamically and filters them based on config (include/exclude rules).
    2. Runs `convert_table_columns()` ([planner.py#L783](../../../credativ_pg_migrator/planner.py#L783)) to fetch column metadata and map types/defaults using the predefined substitution dictionaries.
    3. Analyzes partitioning conditions and automatically produces partitioned table structures.
    4. Simultaneously discovers index and constraint dependencies and schedules them for post-data migration.

### Phase 3: Orchestration (Execution & Data Transfer)
The Orchestrator physically constructs objects on the PostgreSQL target and manages multithreaded loading.

#### 3.1 Type & Domain Creation
*   **Implementation**: [`credativ_pg_migrator/orchestrator.py:run_create_user_defined_types()`](../../../credativ_pg_migrator/orchestrator.py#L213)
*   **Process**: Creates global dependencies sequentially before table generation. Validates whether equivalent types already exist.

#### 3.2 Base Structure Creation (Sequences & Tables)
*   **Implementation**: [`credativ_pg_migrator/orchestrator.py:run_migrate_tables()`](../../../credativ_pg_migrator/orchestrator.py#L149)
*   **Process**: Initiates a `ThreadPoolExecutor` corresponding to the `parallel_workers_count`. The `table_worker` handles concurrent creation. Standard sequence updates (`run_migrate_sequences()`) are synced simultaneously.

#### 3.3 Parallel Data Migration
*   **Implementation**: [`credativ_pg_migrator/orchestrator.py:table_worker()`](../../../credativ_pg_migrator/orchestrator.py#L530-L557)
*   **Process**: 
    1. If `data_source` exists (CSV/UNL configurations), it constructs PostgreSQL `COPY FROM STDIN` processes via the target connector.
    2. Splits exceedingly large UNL exports in chunks (`config_parser.split_big_unl_file()`) for massive parallel imports.
    3. Handles complex LOB operations (Large Objects) by extracting IDs from the base columns then running separate `lob_worker` threads that concurrently pull external fragments.

#### 3.4 Post-Data Structure (Indexes & Constraints)
*   **Implementation**: [`credativ_pg_migrator/orchestrator.py:run_migrate_indexes()`](../../../credativ_pg_migrator/orchestrator.py#L311) and [`run_migrate_constraints()`](../../../credativ_pg_migrator/orchestrator.py#L366)
*   **Process**: To prevent indexing slowdowns during massive `COPY` imports, constraints and indexes (Standard & Function-based) are generated using identical `ThreadPoolExecutor` multithreading *after* the raw data has landed safely.

#### 3.5 Logic & Post-Processing (Views, Triggers, Scripts)
*   **Implementation**: [`credativ_pg_migrator/orchestrator.py:run_migrate_funcprocs()`](../../../credativ_pg_migrator/orchestrator.py#L83), `run_post_migration_script()` ([#L137](../../../credativ_pg_migrator/orchestrator.py#L137))
*   **Process**: Sequentially executes logic conversions. Validates successful procedure transpiling to PL/pgSQL natively. Upon clean completion, it optionally runs user-defined post-migration scripts on the newly formed cluster to adapt user privileges, logging exact termination points in the migration log tables.
