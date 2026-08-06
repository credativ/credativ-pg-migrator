credativ-pg-migrator Releases
=============================

0.16.0 - 05.08.2026
-------------------

* Added migration of user defined aggregates. They were never even listed, because only `prokind IN ('f','p')` was selected, and `pg_get_functiondef()` cannot describe an aggregate either - it has no body, it is described by `pg_aggregate`. The complete definition is now rebuilt: `SFUNC`, `STYPE`, `SSPACE`, `FINALFUNC` with `FINALFUNC_EXTRA` / `FINALFUNC_MODIFY`, the parallel support functions `COMBINEFUNC` / `SERIALFUNC` / `DESERIALFUNC`, `INITCOND`, the whole moving aggregate implementation used by window frames, `SORTOP`, `HYPOTHETICAL` and `PARALLEL`, including ordered-set and hypothetical-set aggregates. Aggregates are created after all functions and procedures, because they reference their state transition and final functions; routines belonging to an extension are skipped, they come with the extension
* Fixed comments on functions, procedures and aggregates never reaching the target - they were read and stored but never applied, because `COMMENT ON` is not part of `CREATE FUNCTION` and the comments migration did not cover routines. The comment is now set right after the object is created, with the matching keyword (`COMMENT ON AGGREGATE` / `PROCEDURE` / `FUNCTION`) and the identity arguments needed to address an overloaded routine
* Fixed the data migration failing with `cannot insert a non-DEFAULT value into column "..." - Column "..." is a generated column` on tables with generated columns. Their values are computed by the target, so they are now excluded from both the `SELECT` on the source and the `INSERT` into the target - in the standard workflow, in the anonymization workflow, which had the same problem in its own data copy, and in the LOB import from separate data files. For the LOB import the intermediate staging table is additionally created without the `GENERATED ALWAYS AS (...)` clauses, so that the `COPY` of the data file is accepted, and the positional index of the LOB column is re-derived from the columns actually selected - otherwise removing a generated column in front of it would shift every following value
* Added a check of PostgreSQL extensions to the pre-migration analysis. Extensions are never created from the source automatically - they must be listed in `migration.required_extensions` - and a forgotten one used to surface much later as a failing table, index or view. The analysis now reports every extension of the source database with its version and schema, whether it is installed in the target and whether it is at least available there, and it verifies that the configured list covers everything the migrated objects need. The dependencies are read from `pg_depend` rather than guessed from the SQL text, so each finding names the objects requiring the extension (`pgcrypto: required by column documents.checksum (generated)`), and only the objects the configuration actually selects are analysed (`include_tables` / `exclude_tables` and the per-object-class switches are honoured). An extension already installed in the target counts as covered. If a dependency is missing, the complete report is written together with a ready to paste `required_extensions:` block and the migrator stops before anything is created
* Added migration of user defined full text search objects - dictionaries and configurations - as a new object type, created right after the collations and before the tables, because generated `tsvector` columns, views, indexes and functions reference them. This resolves `text search configuration "migtest_english" does not exist`. A configuration is rebuilt from its parser plus its full token type mapping (`ALTER ... ADD MAPPING FOR hword WITH ext.unaccent, english_stem`) rather than as a `COPY` of another configuration, so the tailoring survives even when the configuration it was copied from does not exist in the target; comments are transferred. As with collations all non system schemas are searched, and objects belonging to an extension are left out because they come with the extension
* Fixed references to text search objects pointing nowhere after the migration. They live inside a string literal (`'migtest_english'::regconfig`), so they cannot be repaired by rewriting identifiers - and they do not arrive schema qualified either, because `pg_get_viewdef()` and `pg_get_expr()` print the bare name whenever the object is visible in the source `search_path`, normalizing away even an explicitly written `'public.migtest_english'`. Such literals are now rewritten to the target schema in view bodies, generated column expressions, index expressions and partial index predicates, and function bodies; built-in configurations and extension owned dictionaries are left alone
* Fixed generated columns being silently turned into ordinary, permanently NULL columns in PostgreSQL-to-PostgreSQL migration - the generation expression was never read from the source, and the flag delivered by `information_schema` (`ALWAYS`) never matched the value the DDL builder tested for (`YES`), so no `GENERATED ALWAYS AS ... STORED` clause was ever emitted and no warning was logged either. The expression now comes from the catalog, `STORED` and `VIRTUAL` (PostgreSQL 18) are distinguished, and a virtual column is created as stored with a warning when the target is older than 18
* Fixed indexes losing everything behind their key list - `INCLUDE` columns, `NULLS NOT DISTINCT`, `WITH (...)` storage parameters such as the BRIN `pages_per_range`, and the `WHERE` predicate of a partial index, which silently made the index cover all rows. Fixed a plain `CREATE UNIQUE INDEX` losing its uniqueness, because such an index is not a constraint and therefore not listed in `information_schema.table_constraints` - `pg_index.indisunique` is used now
* Fixed a `CREATE CONSTRAINT TRIGGER` being migrated as a table constraint, producing the invalid statement `ALTER TABLE ... ADD CONSTRAINT "orders_must_have_items" TRIGGER DEFERRABLE INITIALLY DEFERRED`. A constraint trigger is listed in `pg_constraint`, but it is a trigger and has no `ALTER TABLE ... ADD CONSTRAINT` form; it is now left to the triggers migration, which recreates it complete with `DEFERRABLE INITIALLY DEFERRED` and its comment
* Fixed constraints implemented by an index being migrated as bare indexes in PostgreSQL-to-PostgreSQL migration, resolving `access method "gist" does not support unique indexes` on the temporal constraint `room_assignments_no_overlap` (`UNIQUE (room, occupied WITHOUT OVERLAPS)`, PostgreSQL 18) - such a constraint is backed by a gist index and cannot be recreated as a unique index at all. `UNIQUE` and `EXCLUDE` constraints are now left to the constraints migration, which builds them from the constraint definition and so keeps `WITHOUT OVERLAPS`, `NULLS NOT DISTINCT`, `INCLUDE`, the `WHERE` predicate and `DEFERRABLE`; as a side effect every unique constraint is no longer created twice (once as a unique index and once as the constraint), and an `EXCLUDE` constraint no longer degrades to a plain gist index without its exclusion operators and `WHERE` clause. Primary keys are still created together with the indexes, but from the constraint definition, so a temporal `PRIMARY KEY (product_id, valid_at WITHOUT OVERLAPS)` and a primary key with `INCLUDE` columns are migrated correctly
* Fixed functional indexes being destroyed in PostgreSQL-to-PostgreSQL migration - `CREATE INDEX customers_lower_company_idx ON customers (lower(company_name))` was migrated as `CREATE INDEX ... ("lower(company_name")`, because the index keys were read with a regular expression stopping at the first closing parenthesis. The keys are now read by counting parentheses, so expressions with parentheses of their own (`lower((email)::text)`, `((billing_address).city)`, `((metadata ->> 'reference'::text))`) stay intact, and an `INCLUDE`, `WITH` or `WHERE` clause behind them is no longer mistaken for an index key. Expression indexes are also recognized as such - the information comes from `pg_index.indexprs`, so no list of known SQL functions is needed - which additionally moves their creation behind the migration of functions they may call
* Fixed the index access method being lost in PostgreSQL-to-PostgreSQL migration, so every index was created as a btree - resolving `operator class "gin_trgm_ops" does not exist for access method "btree"` on `customer_events_type_payload_idx` and `index row size ... exceeds btree version 4 maximum ...` on the GIN index `customer_notes_body_gin_idx`. The access method was read from the source and the DDL builder expected it, but the planner never passed it between the two, so `USING gin`, `USING gist`, `USING spgist`, `USING hash` and `USING brin` were all silently dropped
* Added migration of user defined collations as a new object type. They are discovered, planned and created as the first objects of the migration, because columns, indexes and domains reference them, and they are reported in the migration summary like every other object type. For PostgreSQL sources `pg_collation` is read (across all non system schemas, because a table regularly uses a collation created in `public`) and recreated in the target schema with `CREATE COLLATION IF NOT EXISTS`, including the ICU provider, locale, tailoring rules, non-deterministic collations and the collation comment. This resolves `collation "natural_numeric" for encoding "UTF8" does not exist` when creating an index such as `countries_name_natural_idx`. References to collations are rewritten to the target schema, because `pg_get_indexdef()` emits them unqualified and they would otherwise be looked up in the source `search_path`; built-in collations (`C`, `POSIX`, `en_US.utf8`) are kept as they are, and a collation which cannot be provided by the target is dropped from the DDL with a warning instead of failing the whole object
* Fixed column collations being lost in PostgreSQL-to-PostgreSQL migration - a column declared as `name text COLLATE german_phonebook` was created with the default collation, because `fetch_table_columns` did not read the collation at all. Fixed a collation in a functional index expression being silently removed, a quoted collation name containing a dot (`COLLATE "en_US.utf8"`) being split into `"en_US"."utf8"`, an operator class behind a `COLLATE` clause being swallowed, and the MySQL charset introducer cleanup corrupting PostgreSQL quoted identifiers with an underscore (`"natural_numeric"` became `"natural"`)
* Fixed PostgreSQL-to-PostgreSQL migration errors:
  - Corrected execution order so domains are created before composite user-defined types (resolving `type "iso_currency" does not exist`), and added topological dependency sorting for composite types
  - Fixed range type DDL syntax by changing `SUBDIFF` to `SUBTYPE_DIFF` (resolving syntax errors when creating range types like `weight_range`)
  - Deduplicated `NOT NULL` in domain creation DDL when check constraints already contain `NOT NULL` (resolving `redundant NOT NULL constraint definition` on `non_empty_text`)
  - Aligned column key extraction order in `insert_batch` and added automatic JSON serialization for `json`/`jsonb` target columns including Python dicts, lists, booleans, numbers, JSON `null` (`None` → `'null'`), and raw strings (resolving `can't adapt type 'dict'`, `column "value" is of type jsonb but expression is of type boolean`, and NOT NULL violations on `app_settings`)
  - Implemented `fetch_sequences` in the PostgreSQL connector to discover and migrate standalone sequences before table creation (resolving `relation "..._seq" does not exist` on `customer_events`)
  - Resolved `syntax error at or near "ARRAY"` by using `pg_catalog.format_type` to resolve array element types (e.g. `text[]`) in `fetch_table_columns`
  - Preserved length modifiers for `BIT(N)` and `VARBIT(N)` columns (resolving `bit string length 8 does not match type bit(1)` on `network_devices.flags`), and fixed `cannot cast type bit to boolean` by checking `source_db_type` so PostgreSQL `BIT` default expressions (`'00000000'::"bit"`) are emitted without illegal `::BOOLEAN` casts while preserving MySQL/MSSQL bit-to-boolean mappings
  - Added NULL fallback and case-insensitive key lookup in `insert_batch` for `NOT NULL` target columns when source rows contain legacy NULL data (resolving `null value in column "reference" violates not-null constraint` on `partial_records`)
  - Rewrote `migrate_sequences` to migrate a single sequence per worker invocation instead of a schema-wide loop, preventing parallel worker collisions (`duplicate key value violates unique constraint "pg_class_relname_nsp_index"`)
  - Registered string type casters for DATE, TIMESTAMP, TIMESTAMPTZ, TIME, TIMETZ, and INTERVAL OIDs to preserve BC dates and infinity values (resolving `ValueError: year -1 is out of range` on `type_zoo.c_date`)
  - Parsed `COLLATE`, operator class (e.g. `gin_trgm_ops`), and `NULLS FIRST`/`LAST` clauses separately from column names in `get_create_index_sql` (resolving `column "event_type gin_trgm_ops" does not exist` on `customer_events_type_payload_idx` and `column "name COLLATE natural_numeric" does not exist` on `countries_name_natural_idx`)
  - Preserved `USING` access method clauses (`USING gin`, `USING gist`, `USING hash`, `USING brin`, `USING spgist`) in `fetch_indexes` and `get_create_index_sql` (resolving `index row size 3144 exceeds btree version 4 maximum 2704` on GIN index `customer_notes_body_gin_idx`)
* Added `ddl` connectivity to the SQLite connector, so a migration can also start from SQL script files (`sqlite3 db .schema` / `.dump` output or hand-maintained scripts) with the data coming from CSV files, next to the existing `native` mode reading a SQLite database file. Instead of a second DDL parser the scripts are replayed into a staging SQLite database which is then read by exactly the same code as a live file, so nothing is lost in translation; a statement that cannot be executed is skipped and reported instead of costing every object in the file. This also fixes the migration failing with `'SQLiteConnector' object has no attribute 'parse_ddl_files'` when `connectivity: "ddl"` was configured
* Added new source database connector `sqlite` for SQLite, using the `sqlite3` module of the Python standard library - the only source engine needing no driver installation. The source is a plain local file (`database` holds its path, there is no host, port, username or password), opened read-only whenever possible. Migrates tables and data, primary keys (including `WITHOUT ROWID` and composite keys), identity columns (`INTEGER PRIMARY KEY` rowid aliases and `AUTOINCREMENT`), indexes including functional ones, foreign keys with their referential rules, CHECK constraints, generated columns, defaults, views and triggers. Because SQLite has no data dictionary for a part of this, CHECK constraints, generated column expressions, `AUTOINCREMENT` and index expressions are parsed out of the `CREATE` statements stored in `sqlite_master`; and because SQLite is dynamically typed, values are coerced to the target column type on insert (0/1 to boolean, Unix timestamps and Julian days to timestamp). Views and triggers are translated with `sqlglot`, a trigger becoming a PL/pgSQL trigger function plus a `CREATE TRIGGER`. Virtual tables (FTS, RTREE) and their shadow tables are skipped, and partial indexes are migrated without their `WHERE` condition (a partial `UNIQUE` index is degraded to a non unique one) with the original condition recorded in the index comment
* Added new source database connector `ibm_db2_i` for IBM DB2 for i (IBM i / AS/400) supporting structure migration from DDL SQL files (parsing `FOR SYSTEM NAME`, `FOR COLUMN`, `CCSID`, `RECORD FORMAT`, `LABEL ON`) and data migration from CSV files
* Fixed IBM DB2 for i migration startup failure (`Can't instantiate abstract class IbmDb2IConnector without an implementation for abstract methods ...`) by completing the connector interface and aligning method signatures and exchanged dictionary keys with the planner and orchestrator
* Fixed IBM DB2 for i tables being created without any columns (`CREATE TABLE "public"."regions" ()`) and the resulting view errors (`column "customer_id" does not exist`) - `migration.names_case_handling` was applied to source names used as the lookup key for the parsed DDL metadata, and is now applied only when the target DDL is built
* Fixed complete loss of IBM DB2 z/OS primary keys, foreign keys, unique and check constraints (the summary reported zero constraints), which are declared as named table level constraints inside `CREATE TABLE` - including the `ON DELETE` / `ON UPDATE` referential rules - and fixed expression based index creation (`column "upper(email)" does not exist`)
* Fixed the error `can't execute an empty query` when setting the sequence of a table after its data migration - the statement is now derived from the migrated rows (`setval(<sequence>, MAX(<column>) + 1)`) instead of relying on the source database reporting its next identity value, which a source with DDL connectivity cannot do
* Added migration of IBM DB2 z/OS functions and procedures, which were not migrated at all - SQL routines are converted to PL/pgSQL (parameters and their modes, scalar and table functions, local variables, `NOT FOUND` handlers, `SIGNAL SQLSTATE`, sequence values, dynamic SQL, global variables), while external routines (COBOL, Assembler) are reported and left out because their load module is not part of the DDL
* Fixed IBM DB2 z/OS view conversion of `WITH CHECK OPTION` views, recursive CTEs, `LISTAGG` and the correlated table function `TABLE (SELECT ...)`, and added migration of a materialized query table as a materialized view
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
