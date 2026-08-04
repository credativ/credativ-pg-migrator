credativ-pg-migrator Releases
=============================

0.16.0 - 04.08.2026
-------------------

* Added new source database connector `ibm_db2_i` for IBM DB2 for i (IBM i / AS/400) supporting structure migration from DDL SQL files (parsing `FOR SYSTEM NAME`, `FOR COLUMN`, `CCSID`, `RECORD FORMAT`, `LABEL ON`) and data migration from CSV files
* Fixed IBM DB2 for i migration startup failure (`Can't instantiate abstract class IbmDb2IConnector without an implementation for abstract methods ...`) by completing the connector interface and aligning method signatures and exchanged dictionary keys with the planner and orchestrator
* Fixed IBM DB2 for i tables being created without any columns (`CREATE TABLE "public"."regions" ()`) and the resulting view errors (`column "customer_id" does not exist`) - `migration.names_case_handling` was applied to source names used as the lookup key for the parsed DDL metadata, and is now applied only when the target DDL is built
* Fixed IBM DB2 z/OS tables silently missing from the migration (4 of 26 in the test database) because the DDL files were split into statements on semicolons inside comments and string literals
* Enforced `migration.names_case_handling` consistently in IBM DB2 z/OS view conversion - unquoted identifiers are normalized before the conversion (DB2 folds them to upper case), and the conversion is applied to the table qualifier of a column, to table aliases and to the alias and column list of a CTE header, which were left out and broke views with `upper` and `keep`
* Fixed IBM DB2 z/OS trigger conversion, which put the whole `CREATE TRIGGER` statement into the body of the generated function - the z/OS only clauses (`VERSION` of an advanced trigger, `MODE DB2SQL`, `NO CASCADE`, the `@` terminator) are removed, statement level triggers keep their scope and transition tables, `DECLARE` of a local variable is moved into the declaration section, names follow `migration.names_case_handling`, and every trigger is created only on the table it is defined on
* Added migration of IBM DB2 z/OS global variables (`CREATE VARIABLE`) used by triggers, mapped to PostgreSQL session settings
* Fixed IBM DB2 z/OS migration from DDL and unload files aborting on the first table (`'NoneType' object has no attribute 'cursor'`) - the connector tried to count the rows in a source database which does not exist with DDL connectivity; the same was corrected for the next identity value and for the validation checksums
* Fixed IBM DB2 for i data migration from the unload files, which never started because no data file was found - the 10 character system name of a table (`FOR SYSTEM NAME`), which the unload files are named after and which the `{{source_alias_name}}` placeholder resolves, was not registered as an alias
* Fixed the data source file of a table not being found when the table has more than one alias - only the first one was ever tried
* Fixed data source files not being found when their name is not written in the same case as the placeholder in the configuration (`{{source_alias_name}}` looked for `regions.csv` instead of the unload file `REGIONS.csv`) - the name of the object as it is in the source database is now tried as well
* Fixed the crash of the comments migration (`cannot access local variable 'target_view_name'`), which aborted the step for all source databases and left the comments of views, user defined types and domains unmigrated, and set the comment of a materialized view with `COMMENT ON MATERIALIZED VIEW`
* Added migration of IBM DB2 for i global variables (`CREATE VARIABLE`) used by triggers - PostgreSQL has no global variables, so they are mapped to session settings with the same session scope (`set_config()` / `current_setting()` falling back to the declared default)
* Fixed IBM DB2 for i trigger migration (`syntax error at or near "MODE"`), which failed for every trigger, by removing the IBM i only clauses (`MODE DB2ROW` / `MODE DB2SQL`, `SET OPTION`, the `@` terminator), ending the trigger text at the statement terminator, delimiting the `UPDATE OF` column list by the `ON` keyword, converting `VARCHAR(<expression>)` to a `CAST`, carrying over transition tables of statement level triggers, and attaching every trigger only to the table it is defined on
* Fixed indexes, constraints, triggers and data being silently skipped for every table listed in `table_settings` - a per table entry made for a completely different reason (character set, delimiter, header row) disabled them, which then broke foreign keys referencing such a table (`there is no unique constraint matching given keys for referenced table`); a `table_settings` entry now overrides a `migrate_*` switch only when it really contains it
* Fixed foreign key creation for all source databases (`syntax error at or near "COMMENT"`) by removing the MySQL only `COMMENT '<text>'` clause from `ALTER TABLE ... ADD CONSTRAINT`, and corrected the constraint name used by the comments migration
* Fixed IBM DB2 for i constraints inheriting the descriptive comment of their table
* Fixed IBM DB2 for i expression based index creation (`column "upper(email)" does not exist`) by marking such indexes as function based, and added support for the `UNIQUE WHERE NOT NULL` and `ENCODED VECTOR` index variants, which were previously dropped without any message
* Fixed complete loss of IBM DB2 for i primary keys, foreign keys, unique and check constraints, which are declared as named table level constraints inside `CREATE TABLE` (`CONSTRAINT PK_REGIONS PRIMARY KEY (REGION_ID)`) - including the `ON DELETE` / `ON UPDATE` referential rules
* Fixed IBM DB2 for i tables silently missing from the migration because the DDL files were split into statements on semicolons inside comments and string literals
* Fixed IBM DB2 for i view conversion (`syntax error at or near "RCDFMT"`) by stripping DB2 for i only clauses (`FOR SYSTEM NAME`, `RCDFMT`, `CCSID`) while preserving `WITH CASCADED CHECK OPTION`
* Fixed IBM DB2 for i view conversion of the infix operator `A CONCAT B` (converted to `A || B`) and of correlated table functions `TABLE (SELECT ...)` (converted to `LATERAL (SELECT ...)`)
* Fixed IBM DB2 for i recursive CTE views by emitting `WITH RECURSIVE` and aligning column types across `UNION ALL` arms
* Fixed IBM DB2 for i materialized query tables (MQT), which were migrated as ordinary tables with unusable columns, and are now migrated as PostgreSQL materialized views
* Views which cannot be converted are now reported as an error right away instead of silently passing the untranslated source code on to the target database
* Fixed inverted `NULL` ordering (`NULLS FIRST` / `NULLS LAST`) in views migrated by all IBM DB2 connectors (for i, z/OS, LUW)
* Fixed IBM DB2 LUW view transpilation (`relation "customers" does not exist`) by schema-qualifying un-qualified table references (`target_schema_name`), double-quoting AST identifiers, and converting case according to `migration.names_case_handling`
* Fixed IBM DB2 LUW recursive CTE view transpilation (`column 5 has type character varying(500)... but type character varying overall`) by aligning and wrapping un-casted `UNION` / `UNION ALL` term expressions with matching `CAST(... AS VARCHAR(N))` types
* Enforced `migration.names_case_handling` case conversion consistently across the whole IBM DB2 LUW connector for all database objects and attributes (tables, columns, indexes, PK/FK columns, referenced tables, constraints, triggers, sequences, aliases, views)
* Fixed IBM DB2 LUW trigger and procedure error signaling (`SIGNAL SQLSTATE 'code' SET MESSAGE_TEXT = 'msg'`) by rewriting into PostgreSQL `RAISE EXCEPTION 'msg' USING ERRCODE = 'code'`
* Fixed IBM DB2 LUW trigger conversion (`column new.list_price does not exist`) by normalizing unquoted column identifiers in trigger DDL source text (e.g. `n.list_price`) to uppercase prior to `convert_names_case`, guaranteeing `OLD."<COL>"` and `NEW."<COL>"` field references match target table column casing across all `names_case_handling` modes (`keep`, `upper`, `lower`)
* Fixed IBM DB2 LUW lateral subquery view conversion (`syntax error at or near "TABLE"`) by rewriting DB2 `TABLE(SELECT ...)` constructs into PostgreSQL `LATERAL (SELECT ...)` syntax
* Fixed IBM DB2 LUW CTE view transpilation (`column T.DEPTH does not exist`) by double-quoting and converting case for CTE column alias lists in `WITH` header definitions (e.g. `WITH "TREE"("DEPTH")`)
* Fixed IBM DB2 LUW string aggregation view conversion (`function listagg(...) does not exist`) by mapping `LISTAGG(...) WITHIN GROUP (ORDER BY ...)` to PostgreSQL `STRING_AGG(...)` and expanded SQL function mappings in `get_sql_functions_mapping`
* Fixed IBM DB2 LUW MQT (Materialized Query Table) view migration by stripping DB2 storage/refresh clauses (`DATA INITIALLY DEFERRED REFRESH IMMEDIATE`), converting to `CREATE MATERIALIZED VIEW`, and schema-qualifying referenced underlying tables for target PostgreSQL
* Fixed IBM DB2 LUW recursive CTE view transpilation (`relation "TREE" does not exist` / `HINT: Use WITH RECURSIVE`) by detecting self-referencing CTEs and emitting `WITH RECURSIVE` in target PostgreSQL SQL, and removed incorrect numeric-to-string literal conversion
* Fixed IBM DB2 LUW view conversion errors (`relation "MIGTEST .CUSTOMERS" does not exist`) by sanitizing trailing whitespace inside catalog quoted schema identifiers, and excluded DB2 internal expression-index statistical views from view migration
* Fixed CHECK constraint DDL double-quoting errors (`zero-length delimited identifier at or near """"`) by preventing re-quoting of already quoted identifiers in CHECK expressions
* Fixed IBM DB2 LUW foreign key composite column name parsing (`column "ORDER_ID            ORDER_DATE" does not exist`) by properly splitting space-delimited column name strings in `SYSCAT.REFERENCES.FK_COLNAMES` and `PK_COLNAMES`
* Fixed IBM DB2 LUW query and row count errors (`SQL1668N` reason code `5`) on column-organized (`ORGANIZE BY COLUMN`) tables by adding diagnostic error logging instructing users to enable intra-partition parallelism (`db2 update dbm cfg using INTRA_PARALLEL YES`) in DB2 LUW, and corrected `BLU: "true"` environment setting in the DB2 test container configuration
* Fixed IBM DB2 LUW table creation failures for `CURRENT TIMESTAMP` column defaults by enforcing word boundaries in `default_values_substitution` patterns and sorting matches by length descending, preventing `CURRENT TIME` from matching inside `CURRENT TIMESTAMP` (`cannot cast type time with time zone to timestamp without time zone`)
* Fixed IBM DB2 LUW function-based and XML/columnar index migration by joining `SYSCAT.INDEXES` with `SYSCAT.INDEXCOLUSE`, filtering out internal Db2 XML/block/columnar index types (`XPTH`, `XRGN`, `CPMA`, etc.), skipping unindexable column types (`XML`, `CLOB`, `BLOB`, `DBCLOB`) and placeholder columns (`SQLNOTAPPLICABLE`), and extracting expression definitions from `SYSCAT.INDEXCOLUSE.TEXT` with `is_function_based` set, resolving PostgreSQL index creation errors on Db2 internal virtual key columns (`column "K00" does not exist`, `column "SQLNOTAPPLICABLE" does not exist`) and duplicate XML index creation errors (`could not create unique index` on `SPEC_XML`)
* Fixed MS SQL Server table creation failures for `VARCHAR(MAX)` / `NVARCHAR(MAX)` columns by mapping negative catalog lengths (`-1`) to PostgreSQL `TEXT` and extracting underlying default expressions from legacy `CREATE DEFAULT` bound objects
* Fixed MS SQL Server connection errors (`Connection is busy with results for another command`) by setting `autocommit=True` directly at `pyodbc.connect()` initialization
* Fixed MS SQL Server data fetch error `ODBC SQL type -155 is not yet supported` on `DATETIMEOFFSET` columns by registering a pyodbc output converter that unpacks binary timestamp-with-offset structs into ISO format strings
* Fixed MS SQL Server function/procedure header conversion errors (`syntax error at or near "AS"`) for parameters with multi-argument type specs like `decimal(18,8)`
* Fixed TSQL parser function conversion syntax errors on multi-line `CASE` / `RETURN` statements and string literal brackets
* Fixed MS SQL Server data migration for `ROWVERSION` / `TIMESTAMP` columns (mapping binary rowversion to PostgreSQL `BYTEA`) and `XML` columns (decoding UTF-16 XML byte streams cleanly)
* Fixed MS SQL Server index creation errors by filtering out XML/spatial indexes and indexes on unindexable column types (`xml`, `image`, `text`, UDTs)
* Added systematic automatic creation of parent table `UNIQUE` indexes for Foreign Key constraints across all connectors (`planner.stdwf_ensure_parent_fk_indexes`), resolving PostgreSQL foreign key creation errors (`there is no unique constraint matching given keys for referenced table`)
* Fixed SQL Anywhere column default values and view transpilation by populating `get_sql_functions_mapping` for SQL Anywhere date/time keywords (`current date`, `current timestamp`), transpiling `SELECT TOP <N>` constructs to `SELECT ... LIMIT <N>` (including inside `LATERAL` derived tables), converting Sybase `IF ... THEN ... ELSE ... ENDIF` expressions to ANSI `CASE WHEN`, translating `LIST()` string aggregates to `string_agg()`, supporting `uuid_default_function` configuration for `NEWID()` defaults, converting double-quoted text literals (`"ACTIVE"` -> `'ACTIVE'`), and dropping column-referencing expressions from DEFAULT clauses
* Fixed MariaDB sequence migration and column defaults (`nextval(`schema`.`seq_name`)` / `NEXT VALUE FOR ...`) converting to PostgreSQL `nextval('seq_name')`
* Fixed MariaDB view transpilation for inline `IF(...)` functions and resolved PostgreSQL `CASE` expression mixed-type errors by auto-casting non-string arms (`CAST(expr AS VARCHAR)`) when paired with string literals
* Fixed MySQL 9 native `VECTOR` data migration by converting Python `array.array` objects to JSON string representations for target PostgreSQL insertion
* Major Oracle connector expansion: full schema and data migration including functions/procedures, triggers, standalone sequences, user-defined types and domains, CHECK constraints, and views/materialized views with real Oracle→PostgreSQL query conversion (including `(+)` outer joins)
* Much broader Oracle data-type coverage (BINARY_FLOAT/DOUBLE, RAW, XMLTYPE, JSON, INTERVAL, timestamps with time zone, SDO_GEOMETRY point geometries) plus table/column comments, and hardened connection handling
* New final object-validity pass: after migration it re-attempts objects that failed only because a dependency did not yet exist, and reports which views, functions and triggers are valid at the end (configurable via `migration.validate_objects`)
* Configurable NUMBER(1,0) mapping: narrow numeric columns now default to SMALLINT (lossless), with an opt-in for mapping true 0/1 flags to BOOLEAN
* Mapping workflow and validation improvements: `data_conflict_action`-aware validation, redesigned mapping report, and configuration cleanup
* Numerous Oracle conversion fixes across PL/SQL parameters, triggers, views and boolean data handling
* Oracle packages are now migrated: each package is split into standalone functions and all calls into the package (in functions, procedures and triggers) are rewritten to them - either as `<package>_<routine>` functions in the target schema or as `<routine>` functions in a schema named after the package, selectable with `migration.packages_as`
* New target capability check in the pre-migration analysis: the migrator now stops before creating any object if the target PostgreSQL version cannot support what the source schema requires (first case: generated columns need PostgreSQL 12+)
* Oracle virtual (computed) columns are migrated as PostgreSQL generated columns, and Oracle-specific column defaults (`SYS_CONTEXT('USERENV', ...)`, `USER`, `SYSDATE`, `SYS_GUID()`) are translated to their PostgreSQL equivalents instead of producing invalid DDL
* Fixed MySQL and MariaDB index and constraint fetching for expression-based functional indexes where column names are NULL
* Fixed index creation errors by adding MySQL 8.0+ expression/functional index extraction (`S.EXPRESSION`) and adding guards against empty index column lists across connectors and orchestrator
* Fixed spatial data type migration (POINT, GEOMETRY, etc.) from MySQL/MariaDB to PostgreSQL with automatic WKB/WKT parsing and automatic `USING gist` index generation for spatial indexes and columns
* Fixed batch insertion formatting errors on tables with generated/computed columns by filtering generated columns from data migration payloads and aligning placeholder counts
* Added configurable target UUID generator function via `migration.uuid_default_function` (`gen_random_uuid()` by default, `uuidv7()`, `uuid_generate_v4()`, etc.) across MySQL, MariaDB, Oracle and MS SQL Server connectors, with automatic data-type awareness for native `UUID` vs `TEXT`/`VARCHAR` target columns
* Added automatic stripping of MySQL/MariaDB `CHARACTER SET` and `COLLATE` specifications, `WITH ROLLUP` to `ROLLUP (...)` conversion, `FIND_IN_SET` to native array functions, and `YEAR`/`MONTH`/`DAY` date extract conversion when transpiling to PostgreSQL
* Enhanced the migration summary report `[ OBJECTS MIGRATION RESULTS ]` to display `total / success` breakdown counts in the details column for Indexes and Constraints (e.g. `INDEX migtest: 21/20`, `FOREIGN KEY: 10/10`)

0.15.0 - 03.07.2026
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
