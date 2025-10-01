# Changelog

## 0.10.0 - 2025.10.01

- 2025.09.30:

  - Fix in check_data_types_substitution function - repaired construction of select query, it now covers all cases of defining table name and/or column name including global replacements (empty table name and/or column name)
  - Skip creation of foreign key constraint if either target table or referenced table do not exist in the target database - migrator now tests existence of both tables before attempting to create the constraint
  - Fixes in Informix data import from UNL files - repaired misplaced else statement in the block handling splitting of big UNL files into smaller parts for parallel processing; repaired processing of LOB columns, improved debug and error messages

- 2025.08.14:

  - Implemented parallel import of LOB values in Informix connector - table worker now starts in config file defined number of parallel workers to import LOB data in parallel. This option is necessary mainly for tables with several hundreds of clob* / blob* export files.
    - Rationale: Informix UNLOAD / backup exports CLOB / BLOB values into separate files, each max 2 GB of size, and their names puts into pointers in the main UNL export data file. Simple sequential processing of these files is doable up to 10 distinct files. With higher counts, parallel processing is necessary to avoid long delays in the migration process and better use available resources.
  - Split of big UNL files into multiple parts was tested on real live data and its effectiveness is questionable especially if client uses slow disks or all is processed on single disk with limited I/O bandwidth. So, although option is implemented, it may not provide the expected performance improvements in all cases.

- 2025.08.06:

  - Significant improvements in UNL to CSV conversion - added check for expected target data types for better validation of processed data
  - UNL import can now also skip import of LOB values based on migration.migrate_lob_values setting - LOB value will contain UNL pointer to external LOB data
  - added missing option "table_schema" into individual table_settings in the config file
  - added setting database_export.on_missing_data_file to define globally action on missing data files if database_export is specified in the config file
    - Possible values: "error", "skip", "source_table" - use source table from the source database instead of the data file

- 2025.07.31:

  - Fix in "resume" functionality - planner must check row counts of fully migrated tables to ensure they were not mistakenly marked as fully migrated
    - Implementation still covers only optimistic variant of resuming migration, i.e. it presumes original planning phase was done correctly and data in the source database did not change since the interruption of the migration
  - Added experimental support for Informix text dump UNL files as data source for inserting data into target tables.
    - This is necessary because Informix database can be extremely slow in reading data from tables - depends on license and underlying hardware
    - UNLOAD dump can be slow too, but still faster than batch select-insert from the source Informix database into the target PostgreSQL database
    - Includes conversion of UNL format into proper PostgreSQL readable CSV format, compensates for NULL values vs empty strings, and multiline text values
    - Includes import of LOB values from secondary text/binary files referenced in the UNL file
    - Includes splitting of UNL files into smaller chunks for parallel processing

- 2025.07.22:

  - Trim CHAR and NCHAR columns in Informix connector - these columns are fixed-length, so we need to trim them to avoid issues with trailing spaces

- 2025.07.18 - bugfixing party:

  - Config parameter migration.data_chunk_size renamed to migration.chunk_size - this is more obvious intuitive name. Renaming also all internal variables and functions.
  - Properly implemented option chunk_size = -1 to disable chunking - this means that all data will be migrated in one chunk, i.e. chunk as big as the table

- 2025.07.15:

  - Repaired issue #11 - exiting with 0 when error is caught in the main function - now it exits with 1 as expected
  - Updated debug3 messages in migrator_tables.py - added more debug messages for better tracking of the migration process
  - Repair in logging data migration - if database did not expose internal table ID like MySQL, logging created duplicates
  - Improvements in error messages in the migrate table function
  - Unfortunately, we cannot fully implement "resume after crash" functionality for Sybase ASE, because it does not support LIMIT with OFFSET in older versions. Therefore partially migrated tables must be always dropped and restarted. I.e. Sybase ASE in the "--resume" mode will always set "--drop-unfinished-tables" option to True.

- 2025.07.14:

  - Implemented "optimistic variant" of resuming migration of partially copied tables - code now resumes migration based on row count in the target table, does not check if data in the source table changed
    - This is possible because select of data from the source database is now done with defined sort order - option was implemented together with migration chunks
    - Tables are sorted either by primary key column(s), or by column(s) used in some unique index, or by all columns in the table
    - This sorting of course slows down the migration a bit, but gives us the advantage to pause and resume migration or continue migration after crash
    - Using sorting by all columns is similar to the PostgreSQL idea of "replica identity full" - it is reasonable to presume that combination of values in the all columns is unique - if not, then having some additional duplicates would most likely not cause any issues and such cases can be fixed later
    - Explicit Warning: this "optimistic variant" of "resume" is usable only when data in the source table did not change since the crash

- 2025.07.13

  - Implemented very basic "resume after crash" functionality - if a migration crashes, is killed, instance was restarted or similar, then it can be resumed with command line option "--resume"
    - How it works: currently migrator implements only "optimistic" approach - it assumes that all important protocol tables already exist and migrator was interrupted during migration of data
    - Fully migrated tables are skipped, partially migrated tables are truncated and re-migrated - this can be repeated multiple times
    - After the data migration is finished, migrator will continue as usual with migration of indexes, constraints, triggers, views and functions/procedures

- 2025.07.12

  - Improvements in pausing and canceling actions - migration can now be paused or canceled on demand by creating a file "pause_migration" or "cancel_migration" in the working directory
    - This makes pausing and canceling more flexible, allows to react to unexpected situations
    - Migration can be resumed by creating a file "resume_migration" in the working directory
  - Improvements in logging - log empty tables as successfully migrated with message "Skipped"

- 2025.07.11

  - Added migration parameter migration.char_to_text_length, similar to migration.varchar_to_text_length - allows to set the length of CHAR columns which should be migrated as TEXT
    - Rationale: Some legacy databases handle CHAR columns with large length slightly differently, therefore we need to convert them to TEXT only if they exceed the defined length
  - Due to many changes and longer time since the last release, we skip versioning directly to 0.9.5
  - Small repairs in pre-migration analysis in multiple connectors
  - Improvements in migration limitations - added support for row limit - only tables with more than this number of rows will be limited by the condition
    - Intended for special use cases, when we need to limit only big tables, but not small ones
    - If row limit is not specified, limitation will be applied to all tables matching the pattern and having the specified column
  - Debugging of real migration cases showed that migrator always runs requested number of parallel workers (unless only smaller than requested number of tables are not migrated yet), but there is a delay between start of the worker and insertion of new record into the protocol table. If database is extremely slow, this delay can be significant
  - Config file option "table_batch_size" renamed to "table_settings" and additional options beside of batch_size have been allowed - table name can now be specified as a full name or regular expression
    - Rationale: This is necessary for better control of the migration process - some tables should be created on the target database but we do not want to migrate data, or we want to skip indexes/constraints/triggers for some tables, or we want to set different batch size for some tables because of limitations on the source database
  - Added config file option "migration.data_chunk_size" - this allows to set the size of the data chunk which will be migrated in one go, default is 1000000 rows
    - Rationale: This allows to divide migration of huge tables into smaller independent parts - chunks are logged and processed separately
    - Intention is to allow to pause the migration process and continue later, in case if the source database contains really huge tables or if maintenance window is limited only to some daily hours
    - Chunk is processed in batches of batch_size, so batch_size is supposed to be smaller than data_chunk_size
    - Dividing migration of huge tables into chunks requires ordering of the rows in the source database, so it can lead to performance issues on some source databases
    - Ordering is done either by primary key or by columns used in some unique index, if non of these is available, then by all columns in the table in the order as they are defined in the source database
    - currently implemented only for Infomix and PostgreSQL connectors, support for other connectors will be added on demand
  - Implemented option to pause and resume migration of data - added new section into migration part of config file - "scheduled_actions" - see description in the config file example
    - Orchestrator checks before running migration of next data chunk if there is a scheduled action to pause the migration, and if so, it waits until the pause is removed
    - Migrator will continue with migration of the next data chunk only after work is resumed - this is done by creating file "resume_migration" in the working directory - info message about pausing the migration contains the whole path and name of the file, so simle "touch" command with this full name will resume the migration
    - Migrator immediately deletes the file "resume_migration" after resuming the migration, so it can later pause again based on other pause actions

- 2025.07.10

  - Added config file part for configuring pre-migration analysis of the source database, added section for TOP N tables - user can define how many TOP N tables should be listed in the output based on row count, total size, column count, index count and constraint count
    - Rationale: This will allow to better understand the source database and its structure, and to identify tables which might need special handling during migration
    - Old function get_top10_biggest_tables was refactored to get_top_n_tables, which returns a dictionary with all TOP N tables based on the defined criteria - this way we also standarize the output of the function across all connectors
    - Function skips tables which are excluded from migration (exclude_tables parameter in the config file)
  - Added new part of pre-migration analysis - function get_top_fk_dependencies which returns details about foreign key dependencies for the tables with the biggest count of foreign keys
    - This is very useful in case we need to migrate only some parts of data - allows to identify tables which are heavily dependent on other tables and have therefore the biggest probability of breaking foreign key constraints during migration
  - Improved usage of dry-run command line parameter - if "--dry-run" is used, migrator will do pre-migration analysis of the source database, read all objects of the data model and store them in protocol tables but will not migrate any data - this allows to better understand the source database and its structure before starting the actual migration
  - Added listing of FK and PK columns in the output of the Informix pre-migration analysis - this is useful for further analysis / setting migration limitations
  - Added check if table contains ROWID column in the pre-migration analysis of Informix
  - Migration limitations in config file now allow placeholders {source_schema} and {source_table} - referencing current table to which the limitation applies

- 2025.07.06:

  - Adjusted setting of cursor.arraysize in all connectors based on the batch size defined in the config file
    - Rationale: According to documentation, this should allow to better control the performance of data migration, influence in local tests was small, but measurable
    - Only Oracle needs special handling, performance degrades significantly with too high arraysize, so we set it to 1000 for larger batch sizes
  - Started to add simple wiki pages describing how to set up connectivity

- 2025.07.03:

  - Added db_locale setting into config file for source database - currently used only for Informix
  - Added commits to protocol tables for better tracking of migration progress during long operations
  - Added even more detailed time stats for reading, transforming and writing data inside the batch including their logging into the protocol tables
    - Rationale: On some legacy systems we see huge oscillations in performance of data migration, so we need to see where the time is spent
  - Reimplemented mistakenly missing migration of data into Oracle connector
  - Repairs in implementation of names case conversion - added missing handling of column names in the INSERT statement in some connectors
  - Repair in handling Oracle LOB data type during data migration - LOBs are now properly converted to bytes before inserting into the target database
  - Added general section into config file for setting of environmental variables
    - Rationale: some libraries might require setting of environmental variables, and this must be done in a transparent, documented and easily configurable way
  - Implemented possibility to set individual batch size for a table in the config file
    - Rationale: Some tables might benefit from different batch size than the default one, either smaller or larger, depending on the size of the table and performance of the source database
    - Usual use case is to set smaller batch size for tables with LOBs or other special data types, but user can also set larger batch size for large tables with simple data types and very small rows

- 2025.07.01:

  - Added detection of errors (including deadlocks) into Orchestrator - creation of constraints; If creation of constraints fails, worker re-tries the action after a short delay
    - Rationale: When only data model without data is migrated, deadlock can happen if multiple workers try to create at the same time constraints referencing the same table
    - This happens only on some testing databases like Sakila, where data model heavily relies on constraints, but was seen repeatedly during tests

- 2025.06.30:
  - Time stats for migration of tables and data added into all connectors, results are also stored in the protocol tables
    - Rationale: This will allow to better understand performance of the migration process and identify bottlenecks or situations when migration literally hangs because of some issues on the source database
    - Migrator now stores detailed time stats for each batch in the new protocol table, and reports basic batch stats at the end of the migration, shortest, longest and average batch time is stored and reported for every table
  - Adjustments in running parallel workers for migration of tables, data, indexes and constraints to avoid delays in starting the next worker after some of the previous ones finished

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
