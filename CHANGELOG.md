# Backlog

## (not released yet) 0.7.2 - 2025.05.xx

- 2025.05.16: Full refactoring of the function convert_table_columns - redundant code removed from connectors, function replaced with a database specific function get_types_mapping, conversion of types moved to the planner
  - Rationale: previous solution was repeating the same code in all connectors and complicated custom replacements and handling of IDENTITY columns
  - This change will also simplify the replacement of data types for Forein Key columns
- 2025.05.15: Added experimental support for target table partitioning by range for date/timestamp columns
  - Remaining issue: PRIMARY KEY on PostgreSQL must contain partitioning column
- 2025.05.15: Replacement of NUMBER primary keys with sequence as default value in Oracle connector with BIGINT IDENTITY column
  - Remaining issue: if used in FK, migrator must change also dependent columns to BIGINT
- 2025.05.15: Updates in Oracle connector - implemented migration of the full data model
- 2025.05.14: Fixed issues with running Oracle in container, added testing databases for Oracle
- 2025.05.12: Fixed issue in the config parser logic when both include and exclude tables patterns are defined

## 0.7.1 - 2025.05.07

- Fixed issue with migration limitations in Sybase ASE connector
- Cleaned code of table migration in Sybase ASE connector - removed commented old code
- Fixed migration summary - wrongly reported count of rows in target table for not fully migrated tables
- Updated header of help command, added version of the code
- Fixed issue with finding replacements for default values in migrator_tables
- Added new debug messages to planner to better see custom defined substitutions

## 0.7.0 - 2025.05.06

- Added versioning of code in constants
