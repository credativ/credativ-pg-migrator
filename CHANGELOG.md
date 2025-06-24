# Changelog

## 0.9.1 - 2025.06.24

- 2025.06.24:
  - Add project logo and architecture diagram to PyPI page (@mbanck)

- 2025.06.19:
  - Implemented better conversion of views in Sybase ASE connector - added parsing of view code using sqlglot library - change significantly improves success rate of views migration
    - Remaining issue: conversion of special operators \*= and =\* in conditions which in Sybase ASE mean LEFT OUTER JOIN and RIGHT OUTER JOIN respectively - parser fails on these operators
  - Library sqlglot added to requirements and setup.py - will be used for parsing of SQL code / view code in other connectors too
  - Started implementation of functions for premigration analysis of the source databases - in this step code returns only values readily available in the source database without effort to standardize it and just output results into log file - this will be improved in future steps once we have more data available
    - Rationale: We ask clients still the same questions about the source database, so we can automate this process and provide better overview of the source database, not to mention that clients often do not know the answers / do not know how to extract the information

## 0.9.0 - 2025.06.18

- 2025.06.18:

  - Add support for PyPi distribution via pyproject.toml (@mbanck-cd)

- 2025.06.17:

  - Constants transformed into a class with static methods - this allows to use constants in the code without importing them, just using the class name
    - Rationale: This is more pythonic way of using constants, allows to use constants in the code without importing them, just using the class name
  - Refactoring in migrator_tables.py - removed import and usage of PostgreSQL connector, added new local class and methods for usage in the MigratorTables class
    - Rationale: MigratorTables class cannot depend on PostgreSQL connector, it breaks dependencies
  - Library 'importlib' removed from requirements and setup.py - it is an implicit python package, when pip tries to explicitly install it, it fails with a misleading error in setuptools library
  - Fix in constants - added missing path to connectors in modules

- 2025.06.16:

  - Improvements in Informix connector - improved handling of default values for columns, fix in is_nullable flag, updates in data migration for special data types, fix in interpretation of numeric precision and scale, implemented proper handling of function based indexes
  - Change in Orchestrator - run migration of function based indexes only after the migration of user defined functions because these indexes can reference some of these functions
    - Note: Currently fully relevant only for Informix, where we migrate functions/procedures - however, it is now prepared for other connectors as well
  - Change in all connectors - data are now selected using explicitly defined list of columns in the SELECT statement, not using SELECT \* - this allows to use casting or other transformations for some specific data types in the SELECT statement
    - Rationale: Some special data types like geometry, set, some types of LOBs, user defined data types, complex data types etc. are hard to handle in the python code, but can be easily manipulated in the SQL SELECT statement in the source database
  - Fix in SQL Anywhere connector - added handling of duplicated foreign key names in the source database (duplicates are possible due to different scope of uniqueness in the source database)

- 2025.06.15

  - Fixes in MySQL data model migration - added missing migration of comments for columns, tables, indexes, repairs in migration of special data types, fixed migration of geometry data type and set data type
  - Multiple improvements in MySQL tests, added Sakila testing database (dev repository)
  - Breaking change: custom replacements for data types in the config file now require table name and column name to be specified - new format is checked in the config parser and error is raised if not enough parameters are specified - new parameters can be empty strings, but must be present
    - Rational: Tests with Sakila database showed issue with migration of encoded passwords - in the table staff column password is varchar(40) but migrated value exceeds this length -> we need to be able to specify replacements for specific columns, existing solution was not flexible enough
  - Refactoring of exception handling in connectors - too specific exceptions masked some errors, generic "Exception" is now used in most cases
  - Refactoring of log levels for different messages in the migrator - added deeper DEBUG levels DEBUG2 and DEBUG3 for better granularity, old calls replaced with new function
    - Refactoring of all calls to print log messages in the whole code

- 2025.06.13:

  - Sybase ASE connector - added new functions into SQL functions mapping (solves issues in migration of views like replacement of isNull etc)
  - Function convert_funcproc_code in any connector cannot return None - it causes issues in Orchestrator
  - Fixed not working setting for truncation of tables in the target database - parameter migration.truncate_tables
    - Truncation now works, but migration of data into existing data model might fail due to foreign key constraints
  - Fixed automatic boolean cast of integer source default values like 0::boolean or 1::boolean - replaced with proper TRUE or FALSE
  - Improvements in Oracle connector - added missing data types, added conversion of different special variants of NUMBER to BOOLEAN, INTEGER, BIGINT, DOUBLE PRECISION, improvements in handling altered data types

- 2025.06.12:

  - Created fully automated test for MS SQL Server connector (dev repository)
  - Fixes in MS SQL Server connector after previous refactoring changes in 0.7.x releases - fix in column types conversion, fix in foreign key migrations, fix in VARCHAR to TEXT conversion
  - Proper implementation of handling of names casing - parameter migration.names_case_handling (lower, upper, preserve) is now used when CREATE DDL statements are generated
    - Rationale: legacy and proprietary databases have different rules for names casing, users might want to preserve original casing or convert names to lower or upper case based on their use cases
  - Fix in Oracle connector - migration of indexes - function based indexes contain in system tables hidden columns SYS_N% which must be replaced with their values in the DDL statements

- 2025.06.11:

  - Created automated test for IBM DB2 LUW connector (dev repository)
  - Fixes in IBM DB2 LUW connector after previous refactoring changes in 0.7.x releases - fix in column types conversion, fix in primary key migrations, fix in foreign key migrations, fix in VARCHAR to TEXT conversion
  - Improvements in IBM DB2 LUW connector for migration of comments

## 0.8.2 - 2025.06.11

- 2025.06.11:

  - Fix in Informix funcs/procs migration - fetch of table names for replacements of schemas was broken due to previous changes in the migrator protocol table
  - Fix in include views logic - migrator in some cases excluded all views from migration
  - Changed call of convert_funcproc_code function in all connectors - list of parameters replaced with JSON object
  - Implemented replacement of schemas for views in the function convert_funcproc_code of Informix connector
    - creation some functions failed in the target database because they did not find views referenced in the code
  - Changed order of actions in the Orchestrator - views must be migrated before functions/procedures/triggers, because these objects can reference views
    - View can be created with errors, if it uses some user defined functions/procedures which are not yet migrated - PostgreSQL validates them once missing objects are created
  - Fix in the migration of VARCHAR columns - added new parameter migration.varchar_to_text_length to the config file
    - Rationale: different use cases might require different handling on how to migrate VARCHAR columns, either as TEXT or as VARCHAR based on length or always or never
    - Usage - see config file example

- 2025.06.08:

  - Started implementation of get_table_description function - description of table structure and eventually other properties, using native source database functions
    - Rationale: Added for better observability of the migration process and as simplification for the post migration checks
    - Added for Sybase ASE - function sp_help
    - Added for MySQL - function DESCRIBE table_name
    - Added for Oracle - function DBMS_METADATA.GET_DDL
    - Added for SQL Anywhere - function sa_get_table_definition
  - Fixes in MySQL connector after previous refactoring changes in 0.7.x releases
  - Created fully automated test for MySQL connector (dev repository)
  - Fixes in Oracle connector after previous refactoring changes in 0.7.x releases, fix in primary key migration, fix in data type alterations due to IDENTITY columns
  - Created fully automated test for Oracle connector (dev repository)
  - Fixes in SQL Anywhere connector after previous refactoring changes in 0.7.x releases, fix in primary key migration, fix in foreign key migration
    - Remaining issue: Some Foreign keys fail because of missing primary key / unique indexes - requires further investigation
  - Created fully automated test for SQL Anywhere connector (dev repository)

- 2025.06.07:

  - Fixed size of UNIVARCHAR/UNICHAR and NVARCHAR/NCHAR columns in Sybase ASE connector - added proper usage of global variables @@unicharsize, @@ncharsize for calculation of sizes

## 0.8.1 - 2025.06.05

- 2025.06.04

  - Fixed numeric precision and scale in Sybase ASE connector
  - Fixed issue with using numeric precision and scale in PostgreSQL connector
  - Fixed wrongly interpreted numeric precision and scale in Informix connector

## 0.8.0 - 2025.06.03

- 2025.06.03

  - Public release
  - Move connectors into their own module/sub directory (@mbanck-cd)

## 0.7.6 - 2025.05.30

- 2025.05.28

  - Started implementation of SQL functions mapping between source database and PostgreSQL
    - Rationale: This is needed for migration of views and stored procedures/functions/triggers, it is most versatile solution similar to the one used for data types
    - currently added only for Sybase ASE, used in this step for default values of columns
  - Rewrite of custom data types substitution - can use direct match, LIKE format or regexp, simplified format, 3rd value in config file is now taken only as a comment
    - Substitution is now checked for data_type, column_type or basic_data_type (if exists)
  - Fix in casting of default values for type TEXT
  - Fix in Planner - added execution of session settings before attempting to create schema in the target database
  - Implemented SQL function replacement for Sybase ASE views - takes mapping from the function mentioned above

- 2025.05.21

  - Adjustments for providing credativ-pg-migrator as executable in a package
  - Created GitHub workflow for automated tests of database migrations - see details in the main README file
  - Python directory credativ-pg-migrator renamed to credativ_pg_migrator - dashes made issues with packaging
  - Repaired "SET ROLE" setting for the target PostgreSQL database
  - Added implicit embedded default values substitution for Sybase ASE - getdate, db_name, suser_name, datetime, BIT 0/1

## 0.7.5 - 2025.05.21

- cumulative release of changes from 0.7.1 to 0.7.4

- 2025.05.20:

  - Implemented proper handling of Sybase ASE named default values created explicitly using CREATE DEFAULT command vs custom defined replacements for default values on columns.
    - Code extracts default value from CREATE DEFAULT command and uses it for migration unless there is a custom defined replacement for the default value in the config file. Custom replacement has higher priority.
  - Implemented migration of Sybase ASE computed columns. These are currently migrated into PostgreSQL as stored generated columns.
    - Remaining issues: adjustments of functional indexes which use computed hidden columns
  - Fix in data type alterations for IDENTITY columns, NUMERIC must be changed to BIGINT for PostgreSQL to allow IDENTITY attribute - if altered column is used in FK, migrator must change also dependent columns for FKs to work properly
    - Remaining issue: improved reporting of altered columns in the summary

- 2025.05.19:

  - Updates in Sybase ASE testing databases
  - Added migration of check rules/domains in Sybase ASE. Definitions are read from Sybase rules and are migrated as additional check constraints to PostgreSQL.
    - These constraints are created only after data are migrated, because in some cases they need manual adjustments in syntax and could block migration of data.

- 2025.05.18:

  - Added new testing databases for Sybase ASE, improved descriptions for Sybase ASE
  - Properly implemented migration of CHECK constraints for Sybase ASE

- 2025.05.17:

  - Refactored function fetch_indexes in all connectors
    - Rationale: Source database should return only info about indexes, not generate DDL statements
    - DDL statements are generated in the planner, which allows to modify indexes if needed
    - Modification of PRIMARY KEY in planner is necessary for PostgreSQL partitioning, because it must contain partitioning column(s)
  - Refactored function fetch_constraints in all connectors
    - Rationale: The same as for indexes
  - Created corresponding functions in PostgreSQL connector for creation of indexes and constraints DDL statements
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
    - This change will also simplify the replacement of data types for Foreign Key columns

- 2025.05.15:

  - Added experimental support for target table partitioning by range for date/timestamp columns
    - Remaining issue: PRIMARY KEY on PostgreSQL must contain partitioning column
  - Replacement of NUMBER primary keys with sequence as default value with BIGINT IDENTITY column
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
