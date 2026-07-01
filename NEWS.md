credativ-pg-migrator Releases
=============================

0.15.0 - 01.07.2026
-------------------

* Comprehensive Validation upgrades: Column-level checksums, cross-engine Python hashing, structural validation, and detailed side-by-side reporting
* New Anonymization workflow: Standalone module for data masking using Python libraries or PostgreSQL extensions
* Major enhancements to IBM Db2 LUW connector: Deep translation of views, constraints, triggers, and sequences to PostgreSQL equivalents
* Enhancements in Mapping workflow: Forced Table Mappings, intelligent name matching for unmapped tables, and identity sequence mapping
* Extensive upgrades to the native T-SQL Parser and Sybase ASE Connector for procedure and trigger conversion: Supports dynamic mixed-return flattening, `#temp` table transpilations, native `EXEC` assignments, `GOTO` and `CURSOR` processing, and block-parity preservation.
* Numerous stability and reporting fixes across all connectors

0.14.0 - 20.05.2026
-------------------

* Extensive upgrades to the native T-SQL parser for Sybase ASE migrations
  Added handling of procedures yielding implicit data sets into cache, improved injection of command / procedural terminators
  Improved replacement of native SQL functions
* Mapping Workflow enhancements, including explicit `data_conflict_action` rules (`replace`, `merge_keep_target`, etc.)
  Enhanced configuration management, natively accepting arrays and regex patterns for table scoping directives

0.13.0 - 20.04.2026
-------------------

* New migration workflow for migrating applications data between installations on different databases
  Typical use case are ticketing systems or accounting software which supports proprietary database and PostgreSQL
  Workflow maps tables and columns from original installation to PostgreSQL installation and migrates just data
* New TSQL parser for MS SQL Server and Sybase ASE
  Our new custom built parser is able to process even very messy source code of stored procedures and triggers and convert them to PL/pgSQL code for PostgreSQL
* New migration summary output
* Multiple fixes in all connectors

0.12.2 - 15.04.2026
-------------------

* Fix syntax warning

0.12.1 - 15.04.2026
-------------------

* Fix migration of check constraints (#57)
* Fix fetching sequences on Informix, MySQL and Sybase ASE/Anywhere (#63)
* Fix migration of views for most connectors (#65)
* Fix data type substitution for postgres->postgres migrations (#67)
* Add missing validation dummy methods to DB connectors

0.12.0 - 17.03.2026
-------------------

* Add support for IBM DB2 z/OS via the new ibm_db2_zos connector (IBM DB2 connector got renamed to ibm_db2_luw)
* Implemented ddl connectivity type for reading database objects directly from DDL file(s)
* Improvements in CSV data parsing/conversion by dynamically merging fields where commas are incorrectly acting as decimal separators
* Implemented ALIAS migration for tables and views for DB2, including prevention of circular dependencies

0.11.0 - 09.01.2026
-------------------

* Sybase: Significantly Improved Code Conversion: Rewrite of function, procedure, and trigger conversion logic (convert_funcproc_code, convert_trigger_code) using a proper SQL parser
* Sybase: Legacy SQL Support: Added support for legacy Sybase outer join syntax (= and =), which is now correctly parsed and converted to ANSI standard LEFT OUTER JOIN
* Sybase: User Defined Types (UDTs): Implemented fetching of UDTs and their automated substitution with base types or custom types defined in the configuration
* Sybase: Repaired fetching of trigger source code from system tables
* Sybase: Fixed empty schema issues in function definitions (fallback to target schema)
* Sybase: Fixed schema handling in foreign key constraint migration
* Informix: Speed improvements for importing tables with multiple LOB columns from UNL files

0.10.2 - 19.11.2025
-------------------

* Informix: Fixed LOB imports to allow multiple LOB columns per table and properly handle NULL values (placeholder 0,0,0 or explicit NULLs)
* PostgreSQL: Fixed quoting for column lists in indexes and constraints to preserve case sensitivity
* Foreign Keys: Fixed the existence check for referenced tables to ensure the correct target schema/table is validated before creating constraints
* Casing: Improved handling of object name casing (based on migration.names_case_handling) for comments and schema validation
* Planning: Source table row counts are now stored in the protocol table during the planning phase. This supports data imports even when the source database is inaccessible (e.g., offline CSV/UNL imports)
* UNL to CSV Conversion: Fixed parsing issues where text values ended with backslashes or contained Windows line endings (\r\n)
* Informix LOB Handling: Fixed errors where 0,0,0 placeholders caused import failures. Fixed error catching for unreadable CLOB/BLOB files (sets value to NULL and logs the error).

0.10.0 - 09.10.2025
-------------------

* Add support for reading data from Informix UNL files
* Added resume functionality to resume in case the source or target crashed or were restarted
* Introduced scheduled actions to pause and resume migration of data
* Improved timing statistics
* Improved usage of dry-run command line parameter
* Many additional bug fixes and migration improvements

0.9.1 - 24.06.2025
------------------

* Add project logo and architecture diagram to PyPI
* Implemented better conversion of views in Sybase ASE connector
* Started implementation of functions for premigration analysis of the source databases

0.9.0 - 19.06.2025
------------------

* Add support for PyPi distribution via pyproject.toml
* Constants transformed into a class with static methods
* Refactoring of log levels for different messages in the migrator
* Improvements in Informix connector: improved handling of default values for columns, fix in is_nullable flag, updates in data migration for special data types, fix in interpretation of numeric precision and scale, implemented proper handling of function based indexes
* Improvements in Oracle connector: added missing data types, added conversion of different special variants of NUMBER to BOOLEAN, INTEGER, BIGINT, DOUBLE PRECISION, improvements in handling altered data types
* Fixes in Oracle connector: migration of function-based indexes
* Fixes in MySQL data model migration: added missing migration of comments for columns, tables, indexes, repairs in migration of special data types, fixed migration of geometry data type and set data type
* Fixes in MS SQL connector: fix in column types conversion, fix in foreign key migrations, fix in VARCHAR to TEXT conversion
* Fixes in IBM DB2 LUW connector: fix in column types conversion, fix in primary key migrations, fix in foreign key migrations, fix in VARCHAR to TEXT conversion
* Fixes in SQL Anywhere connector: added handling of duplicated foreign key names in the source database

0.8.2 - 12.06.2025
------------------

* Multiple fixes in connectors
* Added description of migrated tables
* Improvements in Informix user defined functions conversion
* Improvements in VARCHAR columns migration

0.8.1 - 05.06.2025
------------------

* Fixed numeric precision and scale in Sybase ASE connector
* Fixed issue with using numeric precision and scale in PostgreSQL connector
* Fixed wrongly interpreted numeric precision and scale in Informix connector

0.8.0 - 03.06.2025
------------------

* Initial Public release
* Move connectors into their own module/sub directory
