# Changelog

## (not released yet) 0.7.2 - 2025.05.xx

- 2025.05.19:

  - Updates in Sybase ASE testing databases
  - Added migration of check rules/domains in Sybase ASE. Definitions are read from Sybase rules and are migrated as additinal check constraints to PostgreSQL.
    - These constraints are created only after data are migrated, because in some cases they need manual adjustments in syntax and could block migration of data.
  - Updates for handling Sybase ASE default values created explicitly using CREATE DEFAULT command

- 2025.05.18:

  - Added new testing databases for Sybase ASE, improved desctriptions for Sybase ASE
  - Properly implemented migration of CHECK constraints for Sybase ASE

- 2025.05.17:

  - Refactored function fetch_indexes in all connectors
    - Rationale: Source database should return only info about indexes, not generate DDL statements
    - DDL statements are generated in the planner, which allows to modify indexes if needed
    - Modification of PRIMARY KEY in planner is necessary for PostgreSQL partitioning, because it must contain partitioning column(s)
  - Refactored function fetch_constraints in all connectors
    - Rationale: The same as for indexes
  - Created corresponing functions in PostgreSQL connector for creation of indexes and constraints DDL statements
  - Started feature matrix file as overview of supported features in all connectors

- 2025.05.16:

  - Serial and Serial8 data types in Informix migration are now replaced with INTEGER / BIGINT IDENTITY
  - IDENTITY columns are now properly supported for Sybase ASE
  - Added basic support for configurable system catalog for MS SQL Server and IBM DB2 LUW
    - Rationale: newest versions support INFORMATION_SCHEMA so we can use it instead of system catalog, but older versions still need to use old system tables
    - Getting values directly from INFORMATION_SCHEMA is easier, cleaner and more readable because we internally work with values used in from INFORMATION_SCHEMA objects
    - Not fully implemented yet, in all parts of the code
  - Preparations for support of generated columns - MySQL allows both virtual and stored generated columns, PostgreSQL 17 has stored generated columns, PG 18 should add virtual generated columns
  - Full refactoring of the function convert_table_columns - redundant code removed from connectors, function replaced with a database specific function get_types_mapping, conversion of types moved to the planner
    - Reason: code was redundant in all connectors, there were issues with custom replacements and IDENTITY columns
    - Rationale: previous solution was repeating the same code in all connectors and complicated custom replacements and handling of IDENTITY columns
    - This change will also simplify the replacement of data types for Forein Key columns

- 2025.05.15:

  - Added experimental support for target table partitioning by range for date/timestamp columns
    - Remaining issue: PRIMARY KEY on PostgreSQL must contain partitioning column
  - Replacement of NUMBER primary keys with sequence as default value in Oracle connector with BIGINT IDENTITY column
    - Remaining issue: if used in FK, migrator must change also dependent columns to BIGINT
  - Updates in Oracle connector - implemented migration of the full data model

- 2025.05.14:
  - Fixed issues with running Oracle in container, added testing databases for Oracle
- 2025.05.12:
  - Fixed issue in the config parser logic when both include and exclude tables patterns are defined

## 0.7.1 - 2025.05.07

- Fixed issue with migration limitations in Sybase ASE connector
- Cleaned code of table migration in Sybase ASE connector - removed commented old code
- Fixed migration summary - wrongly reported count of rows in target table for not fully migrated tables
- Updated header of help command, added version of the code
- Fixed issue with finding replacements for default values in migrator_tables
- Added new debug messages to planner to better see custom defined substitutions

## 0.7.0 - 2025.05.06

- Added versioning of code in constants
