# credativ-pg-migrator Releases

## 0.17.0 - 25.08.2026

- Partitioning for PostgreSQL to PostgreSQL migrations: the scheme of the source is carried over as it is, flattened into one ordinary table, or replaced by a scheme the source never had - globally and per table
- Partitioning for Oracle to PostgreSQL migrations: the scheme of the source is read and reported before anything is created, and a RANGE, LIST or HASH scheme is carried over. What Oracle has and PostgreSQL does not is named rather than quietly dropped - a sub-partitioned second level is not reproduced and the run says how many segments were left behind, an INTERVAL scheme keeps its partitions and the run says that the automatic extension stops, and REFERENCE and SYSTEM partitioning stop the run instead of arriving as something else. Read against a mocked catalogue; not yet run against a live Oracle instance
- Db2 for LUW read through `SYSCAT` no longer reports every ordinary table as partitioned - `SYSCAT.DATAPARTITIONS` holds a row for every table there is, and the partitioning key in `DATAPARTITIONEXPRESSION` is what separates the two
- `target_partitioning` now works on a source which has no partitioning of its own - SQLite and SQL Anywhere returned no partitioning plan at all, so an entry against them was refused by the analysis and would never have been applied. Those two are exactly the sources most likely to be given a scheme
- `target_partitioning` names its table and its columns however the source spells them, the way `table_settings` always has: an entry naming `currency_rates` against Oracle or Db2, whose catalogues answer `CURRENCY_RATES`, used to be refused as a table the schema does not hold
- Partitioning verified against live servers for five of the eleven sources - PostgreSQL 18, Sybase ASE 16.0 SP02, SQL Server 2022, MySQL 9 and MariaDB 10.11 - and two defects which no unit test could have found came out of it: Sybase ASE keeps its partition bounds in `sp_helpartition` and not in `syspartitions`, which has no such column at all, and SQL Server boundary values were being mangled by this connector's own `sql_variant` output converter, so every range scheme of every SQL Server source was given nonsense bounds. Both are fixed
- New documentation page [Partitioning](docs/partitioning.md): one section per source database saying what that engine has, what is carried over, what is reported rather than reproduced and what stops a run - and what has and has not been tried against a real server
- Partitioning for MS SQL Server to PostgreSQL migrations, which completes **all twelve source databases**: every connector of an engine which partitions tables now reads how the source does it and says so before anything is created, and the two engines which have no partitioning declare that instead. For SQL Server the whole difficulty is one bit: `RANGE RIGHT` means what PostgreSQL's ranges mean and maps untouched, `RANGE LEFT` puts a boundary value in the partition below it and is the opposite at both ends, so every bound moves by one value - a scheme copied across unchanged would load its rows into the partition next door without a single error. Filegroups, per-partition compression and non-aligned indexes are reported and not carried over. Read against a mocked catalogue; not yet run against a live SQL Server
- Partitioning for Sybase ASE to PostgreSQL migrations. ASE spreads a table over segments for I/O and for parallel scans, so the placement is half the reason a scheme exists and none of it is carried over; the ranges and the lists are. `VALUES <= (100)` puts 100 in the partition and PostgreSQL's upper bound never does, so each end is converted rather than copied. ROUND ROBIN has no key at all and stops the run. Where the conditions of the partitions cannot be read out of a given server - and this is the one connector whose catalogue reading is written from the documentation of the engine rather than against a live server - the scheme is reported in full with its method stated as not known and nothing is built from it, because a hash scheme built out of a range nobody could read would load every row into the wrong partition without a single step of the run failing. Marked as needing confirmation against a real ASE in the feature matrix
- Partitioning for MySQL and MariaDB to PostgreSQL migrations, as one implementation for the pair. RANGE and LIST schemes are carried over from `information_schema.PARTITIONS`; a hash scheme keeps its partition count but not the placement of a row, because MySQL hashes with its own function and PostgreSQL with its own, and the run says so per table. A table partitioned by an *expression* - `PARTITION BY RANGE (YEAR(hired))`, the commonest MySQL scheme there is - stops the run: PostgreSQL can partition by an expression and a table which does can then have no primary key at all, so the migration says so before anything is created instead of loading the rows and refusing the key at the end. Read against a mocked catalogue; not yet run against a live server
- Partitioning for Informix to PostgreSQL migrations. Fragmentation is not partitioning - a table is fragmented across dbspaces to spread its I/O over devices - so most of what this brings is the report: what each table is fragmented by, over which dbspaces, and how the rows are really spread over the fragments, which is what says whether a scheme prunes anything at all. Where the fragments really are a range or a list over one column they are carried over, and where they are not - ROUND ROBIN, which has no key of any kind, a hybrid scheme, an arbitrary boolean expression - the run stops and says so with the expression quoted. Read against a mocked catalogue; not yet run against a live Informix
- Partitioning for IBM Db2 to PostgreSQL migrations, all three flavours - LUW from a live catalogue, z/OS and for i from their DDL extracts. The trap of the family is that Db2's upper bound is INCLUSIVE by default and PostgreSQL's is never inclusive, so a scheme copied bound for bound refuses every row of the last day of every partition; each end is converted rather than copied, and where the column type has no next value the scheme is refused rather than moved by a guess. DPF and multi-dimensional clustering are named as the mechanisms they are - neither of them is table partitioning - and z/OS partition-by-growth, which has no key at all, stops the run instead of arriving as something else. Read against mocked catalogues; not yet run against a live Db2
- A primary key or unique constraint which does not contain the partitioning columns stops the run before anything is created - now also for a scheme carried over from the source, which is how an Oracle table with a global unique index used to reach the end of a migration and fail there
- The pre-migration analysis reports what the source partitions and stops the run for a partitioning configuration which cannot be built
- The closing summary names rather than counts: every table with its row counts and its duration, every object which did not arrive with what the target said about it, what was never attempted, and what each table is partitioned by on both sides. `summary.report_filename` writes it into a file

## 0.16.0 - 25.08.2026

- New step: conversion of the SELECT statements an application holds as text (`--convert-queries`), implemented for all twelve source databases. It reads files of statements, converts every SELECT with the same code which converts the views of the migration, tests the result against the migrated target and writes one output file per input file plus a machine readable sidecar. Only SELECT is ever converted, and a statement which cannot be converted is reported as such - nothing is handed back unchanged as if it had been converted
- Query conversion: a converted statement names the objects the target really has - the schema, the table and column names `names_case_handling` and the aliases produced - and every object the statement names which the migration does not know is reported before the target answers with a bare `relation does not exist`
- Query conversion: every statement can be compiled against the source first (`source_test`), compile only and never executed, so a statement which was already broken is told apart from one the migrator broke
- New source database connectors: SQLite (a database file, or SQL scripts with the data as CSV) and IBM Db2 for i (DDL and unload files)
- Oracle: sequences, user defined types, domains, CHECK constraints, triggers, routines, views and materialized views, packages as standalone functions, virtual columns and comments are migrated - most of them were not migrated at all
- PostgreSQL as a source: user defined collations, full text search dictionaries and configurations, user defined aggregates, generated columns and the whole index definition (`INCLUDE`, partial, expression, access method, operator class) are migrated
- SAP SQL Anywhere: procedures, functions, triggers and sequences are migrated, and the foreign keys are read from the right side of the catalogue
- IBM Informix: the system catalogue is no longer migrated together with the user data model, and the routines, triggers, views, indexes, collection types, intervals and SQL functions were repaired
- IBM Db2 (LUW, for i, for z/OS): views, recursive CTEs, materialized query tables, functions, procedures, triggers and global variables
- Sybase ASE and MS SQL Server: procedure groups, cursors, the pseudo tables of a trigger, the `*=` outer joins of the old Transact-SQL, and a long round of repairs to the T-SQL parser
- `names_case_handling` is applied to every name a migration creates - routines, triggers, the bodies of converted views and the statements of an application - and a run stops when the setting would make one target object out of two of the source
- Data fidelity: a byte the assumed encoding cannot read is no longer deleted from the value (`migration.on_undecodable_bytes`), a functional index keeps its collation, and column defaults which used to be dropped without a word are carried over
- The pre-migration analysis checks the PostgreSQL extensions the migrated objects need, the capabilities of the target, the foreign key dependencies of the source and the partitioning
- The test suite is published with the repository - more than 2000 tests, none of which needs a database

## 0.15.0 - 03.07.2026

- Comprehensive Validation upgrades: Column-level checksums, cross-engine Python hashing, structural validation, and detailed side-by-side reporting
- New Anonymization workflow: Standalone module for data masking using Python libraries or PostgreSQL extensions
- Major enhancements to IBM Db2 LUW connector: Deep translation of views, constraints, triggers, and sequences to PostgreSQL equivalents
- Enhancements in Mapping workflow: Forced Table Mappings, intelligent name matching for unmapped tables, and identity sequence mapping
- Extensive upgrades to the native T-SQL Parser and Sybase ASE Connector for procedure and trigger conversion: Supports dynamic mixed-return flattening, `#temp` table transpilations, native `EXEC` assignments, `GOTO` and `CURSOR` processing, and block-parity preservation.
- Numerous stability and reporting fixes across all connectors

## 0.14.0 - 20.05.2026

- Extensive upgrades to the native T-SQL parser for Sybase ASE migrations Added handling of procedures yielding implicit data sets into cache, improved injection of command / procedural terminators Improved replacement of native SQL functions
- Mapping Workflow enhancements, including explicit `data_conflict_action` rules (`replace`, `merge_keep_target`, etc.) Enhanced configuration management, natively accepting arrays and regex patterns for table scoping directives

## 0.13.0 - 20.04.2026

- New migration workflow for migrating applications data between installations on different databases Typical use case are ticketing systems or accounting software which supports proprietary database and PostgreSQL Workflow maps tables and columns from original installation to PostgreSQL installation and migrates just data
- New TSQL parser for MS SQL Server and Sybase ASE Our new custom built parser is able to process even very messy source code of stored procedures and triggers and convert them to PL/pgSQL code for PostgreSQL
- New migration summary output
- Multiple fixes in all connectors

## 0.12.2 - 15.04.2026

- Fix syntax warning

## 0.12.1 - 15.04.2026

- Fix migration of check constraints (#57)
- Fix fetching sequences on Informix, MySQL and Sybase ASE/Anywhere (#63)
- Fix migration of views for most connectors (#65)
- Fix data type substitution for postgres->postgres migrations (#67)
- Add missing validation dummy methods to DB connectors

## 0.12.0 - 17.03.2026

- Add support for IBM DB2 z/OS via the new ibm_db2_zos connector (IBM DB2 connector got renamed to ibm_db2_luw)
- Implemented ddl connectivity type for reading database objects directly from DDL file(s)
- Improvements in CSV data parsing/conversion by dynamically merging fields where commas are incorrectly acting as decimal separators
- Implemented ALIAS migration for tables and views for DB2, including prevention of circular dependencies

## 0.11.0 - 09.01.2026

- Sybase: Significantly Improved Code Conversion: Rewrite of function, procedure, and trigger conversion logic (convert_funcproc_code, convert_trigger_code) using a proper SQL parser
- Sybase: Legacy SQL Support: Added support for legacy Sybase outer join syntax (= and =), which is now correctly parsed and converted to ANSI standard LEFT OUTER JOIN
- Sybase: User Defined Types (UDTs): Implemented fetching of UDTs and their automated substitution with base types or custom types defined in the configuration
- Sybase: Repaired fetching of trigger source code from system tables
- Sybase: Fixed empty schema issues in function definitions (fallback to target schema)
- Sybase: Fixed schema handling in foreign key constraint migration
- Informix: Speed improvements for importing tables with multiple LOB columns from UNL files

## 0.10.2 - 19.11.2025

- Informix: Fixed LOB imports to allow multiple LOB columns per table and properly handle NULL values (placeholder 0,0,0 or explicit NULLs)
- PostgreSQL: Fixed quoting for column lists in indexes and constraints to preserve case sensitivity
- Foreign Keys: Fixed the existence check for referenced tables to ensure the correct target schema/table is validated before creating constraints
- Casing: Improved handling of object name casing (based on migration.names_case_handling) for comments and schema validation
- Planning: Source table row counts are now stored in the protocol table during the planning phase. This supports data imports even when the source database is inaccessible (e.g., offline CSV/UNL imports)
- UNL to CSV Conversion: Fixed parsing issues where text values ended with backslashes or contained Windows line endings (rn)
- Informix LOB Handling: Fixed errors where 0,0,0 placeholders caused import failures. Fixed error catching for unreadable CLOB/BLOB files (sets value to NULL and logs the error).

## 0.10.0 - 09.10.2025

- Add support for reading data from Informix UNL files
- Added resume functionality to resume in case the source or target crashed or were restarted
- Introduced scheduled actions to pause and resume migration of data
- Improved timing statistics
- Improved usage of dry-run command line parameter
- Many additional bug fixes and migration improvements

## 0.9.1 - 24.06.2025

- Add project logo and architecture diagram to PyPI
- Implemented better conversion of views in Sybase ASE connector
- Started implementation of functions for premigration analysis of the source databases

## 0.9.0 - 19.06.2025

- Add support for PyPi distribution via pyproject.toml
- Constants transformed into a class with static methods
- Refactoring of log levels for different messages in the migrator
- Improvements in Informix connector: improved handling of default values for columns, fix in is_nullable flag, updates in data migration for special data types, fix in interpretation of numeric precision and scale, implemented proper handling of function based indexes
- Improvements in Oracle connector: added missing data types, added conversion of different special variants of NUMBER to BOOLEAN, INTEGER, BIGINT, DOUBLE PRECISION, improvements in handling altered data types
- Fixes in Oracle connector: migration of function-based indexes
- Fixes in MySQL data model migration: added missing migration of comments for columns, tables, indexes, repairs in migration of special data types, fixed migration of geometry data type and set data type
- Fixes in MS SQL connector: fix in column types conversion, fix in foreign key migrations, fix in VARCHAR to TEXT conversion
- Fixes in IBM DB2 LUW connector: fix in column types conversion, fix in primary key migrations, fix in foreign key migrations, fix in VARCHAR to TEXT conversion
- Fixes in SQL Anywhere connector: added handling of duplicated foreign key names in the source database

## 0.8.2 - 12.06.2025

- Multiple fixes in connectors
- Added description of migrated tables
- Improvements in Informix user defined functions conversion
- Improvements in VARCHAR columns migration

## 0.8.1 - 05.06.2025

- Fixed numeric precision and scale in Sybase ASE connector
- Fixed issue with using numeric precision and scale in PostgreSQL connector
- Fixed wrongly interpreted numeric precision and scale in Informix connector

## 0.8.0 - 03.06.2025

- Initial Public release
- Move connectors into their own module/sub directory