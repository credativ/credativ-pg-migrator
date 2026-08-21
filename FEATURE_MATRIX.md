# Feature Matrix

The table below was last reconciled against the connector sources on 2026.08.04 - every entry
corresponds to code that is actually present in the connector, not to a plan.

Different features and differently supported across various database connectors. This file provides overview of the supported features and their status.

Legend:

- WIP = work in progress, feature is not yet supported but is being worked on
- yes = feature is supported and was successfully tested
- ? = status unclear, feature is generally implemented, must be better tested for the specific database
- -- = feature is not implemented yet
- N/A = feature is not supported by the specific database ("\*" = requires deeper checking in documentation)

Note to the unclear status - the biggest issue is to find reasonable testing database with the features properly used.

```
| Feature                                   | IBM DB2 | IBM DB2 | IBM DB2 | Informix | MSSQL  | MySQL | MariaDB | Oracle | PostgreSQL | SQL      | SQLite | Sybase |
| description                               | LUW     | z/OS    | i       |          | Server |       |         |        |            | Anywhere |        | ASE    |
|-------------------------------------------|---------|---------|---------|----------|--------|-------|---------|--------|------------|----------|--------|--------|
| Pre-migration analysis                    | WIP     | --[9]   | --[9]   | yes      | WIP    | WIP   | WIP     | WIP    | WIP        | WIP      | yes    | WIP    |
| Migration of data                         | yes     | yes[9]  | yes[9]  | yes      | yes    | yes   | yes     | yes    | yes        | yes      | yes    | yes    |
| NOT NULL constraints                      | yes     | yes     | yes     | yes      | yes    | yes   | yes     | yes    | yes        | yes      | yes    | yes    |
| Default values on columns                 | yes     | yes     | yes     | WIP      | yes    | yes   | yes     | yes    | yes        | yes      | yes    | yes[4] |
| IDENTITY columns                          | yes     | yes     | yes     | yes      | yes    | yes   | yes     | yes[1] | yes        | yes      | yes[8] | yes    |
| Computed(generated) columns               | --      | --      | --      | --       | --     | yes   | yes     | yes    | yes        | --       | yes[8] | yes[5] |
| Custom defined replacements of data types | yes     | yes     | yes     | yes      | yes    | yes   | yes     | yes    | yes        | yes      | yes    | yes    |
| Implicit default values replacements[6]   | yes     | yes     | yes     | --       | yes    | yes   | yes     | yes    | N/A        | yes      | yes    | yes    |
| Custom repl. of default values            | yes     | yes     | yes     | yes      | yes    | yes   | yes     | yes    | yes        | yes      | yes    | yes    |
| Primary Keys                              | yes     | yes     | yes     | yes      | yes    | yes   | yes     | yes    | yes        | yes      | yes    | yes    |
| Secondary Indexes                         | yes     | yes     | yes     | yes      | yes    | yes   | yes     | yes    | yes        | yes      | yes[8] | yes    |
| Foreign Keys                              | yes     | yes     | yes     | yes      | yes    | yes   | yes     | yes    | yes        | yes      | yes    | yes    |
| FK on delete action                       | --      | yes     | yes     | --       | --     | --    | --      | yes    | yes        | --       | yes    | N/A*   |
| Check Constraints                         | yes     | yes     | yes     | yes      | --     | --    | --      | ?[7]   | yes        | --       | yes[8] | yes    |
| Check Rules/Domains[3]                    | --      | --      | --      | --       | --     | N/A   | N/A     | ?[7]   | ?          | --       | N/A    | yes    |
| User-defined types                        | --      | --      | --      | --       | ?      | N/A   | N/A     | ?[7]   | yes        | --       | N/A    | yes    |
| User-defined collations[12]               | --      | --      | --      | --       | --     | --    | --      | --     | yes        | --       | N/A    | --     |
| Full text search objects[13]              | --      | --      | --      | --       | --     | --    | --      | --     | yes        | --       | N/A    | --     |
| User-defined aggregates[14]               | --      | --      | --      | --       | --     | N/A   | N/A     | --     | yes        | --       | N/A    | --     |
| Comments on columns                       | yes     | --[9]   | --[9]   | N/A*     | --     | yes   | yes     | ?[7]   | yes        | --       | N/A    | N/A*   |
| Comments on tables                        | yes     | --[9]   | --[9]   | N/A*     | --     | yes   | yes     | ?[7]   | yes        | --       | N/A    | N/A*   |
| Migration of views                        | ?       | ?       | ?       | WIP      | ?      | WIP   | WIP     | ?[7]   | yes        | WIP      | ?[8]   | ?      |
| Conversion of user defined funcs/procs    | --      | ?       | --      | yes      | ?      | --    | --      | ?[7]   | yes        | --       | N/A    | ?      |
| Conversion of user defined triggers       | ?       | ?       | ?       | yes      | ?      | --    | --      | ?[7]   | yes        | --       | ?[8]   | ?      |
| Sequences[2]                              | ?       | ?       | ?       | --       | ?      | N/A   | ?[10]   | ?[7]   | yes        | --       | N/A    | N/A*   |
| Aliases / synonyms                        | ?       | ?       | ?       | ?        | ?      | N/A   | N/A     | ?      | N/A        | --       | N/A    | N/A    |
| SQL functions mapping[11]                 | WIP     | WIP     | WIP     | WIP      | WIP    | WIP   | WIP     | WIP    | N/A        | WIP      | WIP    | WIP    |
| Conversion of application queries[15]     | yes     | yes     | yes     | yes      | yes    | yes   | yes     | yes    | --         | yes      | --     | yes    |
| Validation - row counts & checksums       | yes     | yes     | --[9]   | yes      | yes    | yes   | yes     | yes    | yes        | yes      | yes    | yes    |
| Validation - random sample & LOB sizes    | --      | --      | --      | --       | --     | --    | --      | yes    | yes        | --       | yes    | --     |
```

Notes:

- [1]: IDENTITY columns are recognized based on sequence used as the default value. But there is still an issue with data types. Oracle allows PRIMARY KEY on NUMBER with sequence. But IDENTITY column in PostgresSQL must be INT or BIGINT.
- [2]: Sequences are not explicitly migrated (presuming source database implements them). But SERIAL/BIGSERIAL and IDENTITY columns and columns with a sequence as default value are migrated into PostgreSQL as IDENTITY columns. Which means the sequence is created in PostgreSQL automatically. The current value of the sequence is set to the last value found in migrated data after the data migration is finished. Exception: for Oracle, standalone sequences (not attached to a table column) are additionally migrated as independent PostgreSQL sequences - see note [7].
- [3]: Check rules/domains are addiional checks externally defined and bound to specific column or data type. In PostgreSQL they are implemented as [domains](https://www.postgresql.org/docs/current/sql-createdomain.html), in some other databases as rules bind to columns/data types. Currently we work on implementing this feature for Sybase ASE migration.
- [4]: Sybase ASE has SQL command CREATE DEFAULT which creates independent named default value and this can be attached to a multiple columns using its name. PostgreSQL does not support this, therefore we attach corresponding underlying default value directly to the target column.
- [5]: Sybase ASE in some cases creates internal computed columns, not visible in selects, but documented in system tables. One example is column for this index: CREATE NONCLUSTERED INDEX IX_Products_LowerProductName ON dbo.Products (LOWER(ProductName)) - Sybase created internal calculated materialized column "sybfi4_1" with computation formula "AS LOWER(ProductName) MATERIALIZED". There internal computed columns have status3 = 1 – Indicates a hidden computed column for a function-based index key. This feature also means that the index has different DDL command in system tables - uses the hidden column: CREATE INDEX IX_Products_LowerProductName_608002166_4 ON Products (sybfi4_1);
- [6]: Typical most commonly used default values not compatible with target PostgreSQL syntax are replaced implicitly during migration.
- [7]: Oracle - CHECK constraints, standalone sequences, user-defined types, domains, table/column comments, views/materialized views and best-effort PL/SQL function/procedure/trigger conversion are implemented but not yet validated against a live database. PL/SQL conversion is heuristic (packages are split into standalone `<package>_<routine>` functions with their call sites rewritten, but package state is not migrated; triggers are split into a PL/pgSQL trigger function + CREATE TRIGGER; complex constructs are flagged for manual review). Standalone sequences (`ALL_SEQUENCES`) are migrated as independent PostgreSQL sequences, with bounds clamped to PostgreSQL's `bigint` range. Oracle object types are migrated as PostgreSQL composite types and collection types (VARRAY / nested tables) as array-based domains; SQL domains exist only in Oracle 23ai (`ALL_DOMAINS`) and that path is best-effort. See section 4.3 of `docs/README.md` for the full list of Oracle limitations.
- [8]: SQLite has no data dictionary for these objects - CHECK constraints, generated column expressions, AUTOINCREMENT markers and the expressions of functional indexes are parsed out of the CREATE statements stored in `sqlite_master`. Views, triggers and expressions are translated to PostgreSQL with `sqlglot` plus a SQLite specific function mapping; a SQLite trigger becomes a PL/pgSQL trigger function + CREATE TRIGGER. Partial indexes are migrated without their WHERE condition (a partial UNIQUE index is degraded to a non-unique index) and the original condition is recorded in the index comment. An INTEGER PRIMARY KEY (rowid alias) and AUTOINCREMENT columns become PostgreSQL identity columns. Virtual tables (FTS, RTREE, ...) and their shadow tables are skipped. SQLite is dynamically typed, so values are coerced to the target column type during data migration (0/1 to boolean, Unix timestamps and Julian days to timestamp).
- [9]: IBM DB2 z/OS and IBM DB2 for i are **offline** connectors - they never connect to the source instance. The structure is read from `.sql`/DDL extracts (`connectivity: "ddl"`) and the data from source-generated CSV files, so anything that requires a live source (pre-migration analysis, random-sample and LOB-size validation) is not available. `COMMENT ON` / `LABEL ON` statements *are* parsed out of the DDL and stored in the protocol tables, but they are not yet handed back as table/column comments, so no comment reaches the target.
- [10]: MySQL and MariaDB use separate connectors. They are largely identical, but MariaDB additionally migrates standalone `SEQUENCE` objects (MariaDB 10.3+), which MySQL does not have. Neither connector converts functions, procedures or triggers.
- [15]: The separate step `--convert-queries`, which converts the SELECT statements an application holds as text for the migrated schema and tests each of them against the target - not a part of a migration, it runs over one which is already done. A connector which does not have it stops the step at its start with "query conversion is not implemented for source type x"; the statements are never passed through unconverted. 
- [11]: Every connector ships a mapping of the most common source SQL functions to their PostgreSQL equivalents, applied when defaults, views, constraints and routine bodies are converted. Coverage differs per engine and is extended on demand, hence WIP everywhere. For PostgreSQL as a source no mapping is needed.
- [14]: Aggregate functions created with [CREATE AGGREGATE](https://www.postgresql.org/docs/current/sql-createaggregate.html). Migrated for a PostgreSQL source with their state transition, final, parallel and moving-aggregate support functions, initial conditions, sort operator and parallel safety; created after the functions and procedures they reference. Aggregates provided by an extension are not migrated, they come with the extension.
- [13]: Full text search dictionaries and configurations ([CREATE TEXT SEARCH CONFIGURATION](https://www.postgresql.org/docs/current/sql-createtsconfig.html)) referenced by generated `tsvector` columns, views, indexes and functions. They are migrated for a PostgreSQL source, including the complete token type mapping of a configuration, and recreated in the target schema; the references inside `'name'::regconfig` literals are rewritten accordingly. Objects belonging to an extension are not migrated, they come with the extension itself.
- [12]: Collations created as standalone objects ([CREATE COLLATION](https://www.postgresql.org/docs/current/sql-createcollation.html)) and referenced by columns and indexes. They are migrated for a PostgreSQL source (ICU and libc provider, locale, tailoring rules, non-deterministic collations and the comment) and recreated in the target schema. Collations of the other engines are named differently (`utf8mb4_general_ci`, `Latin1_General_CI_AS`, ...) and have no PostgreSQL counterpart, so a reference to them is dropped and the column keeps the default collation of the target database.

## Tested versions of databases

- IBM DB2 LUW: (latest)
- IBM DB2 z/OS: DDL + CSV extracts (offline, no live instance)
- IBM DB2 for i: DDL + CSV extracts (offline, no live instance)
- Informix: 14.10
- MS SQL Server: 2022
- MySQL: 5.7
- MariaDB: not yet validated against a live instance
- Oracle: 21.3
- PostgreSQL: 14, 17
- SQL Anywhere: 17
- SQLite: 3.46
- Sybase ASE: 16.0

## Strange findings during testing

### Informix to PostgreSQL - iwadb

#### PostgreSQL does not allow to create foreign key constraint on column which is part of composite primary key?

2025-05-22 12:40:33,060: [DEBUG] Target table SQL: CREATE TABLE "iwadb"."inventory" ("i_artid" BIGSERIAL , "i_suppid" INTEGER , "i_quantity" INTEGER , "i_descr" VARCHAR )

2025-05-22 12:40:33,094: [DEBUG] Processed index: {'source_schema_name': 'dwa', 'source_table_name': 'inventory', 'source_table_id': 108, 'index_owner': 'informix', 'index_name': 'f10', 'index_type': 'INDEX', 'target_schema_name': 'iwadb', 'target_table_name': 'inventory', 'index_columns': '"i_suppid"', 'index_comment': '', 'index_sql': 'CREATE INDEX "f10_tab_inventory" ON "iwadb"."inventory" ("i_suppid");'}
2025-05-22 12:40:33,098: [DEBUG] Processed index: {'source_schema_name': 'dwa', 'source_table_name': 'inventory', 'source_table_id': 108, 'index_owner': 'informix', 'index_name': 'p11', 'index_type': 'PRIMARY KEY', 'target_schema_name': 'iwadb', 'target_table_name': 'inventory', 'index_columns': '"i_artid", "i_suppid"', 'index_comment': '', 'index_sql': 'ALTER TABLE "iwadb"."inventory" ADD CONSTRAINT "p11_tab_inventory" PRIMARY KEY ("i_artid", "i_suppid");'}

2025-05-22 12:40:44,093: [DEBUG] Worker 92b76014-c1fe-41ae-a9db-6e7aaab0cc9f: Creating constraint with SQL: ALTER TABLE "iwadb"."partlist" ADD CONSTRAINT "f15_tab_partlist" FOREIGN KEY (p_artid) REFERENCES "iwadb"."inventory" (i_artid)
2025-05-22 12:40:44,127: [ERROR] An error in Orchestrator (constraint_worker 92b76014-c1fe-41ae-a9db-6e7aaab0cc9f f15): there is no unique constraint matching given keys for referenced table "inventory"

2025-05-22 12:40:44,129: [ERROR] Traceback (most recent call last):
File "/home/josef/github.com/credativ/credativ-pg-migrator-dev/credativ_pg_migrator/orchestrator.py", line 520, in constraint_worker
worker_target_connection.execute_query(create_constraint_sql)
File "/home/josef/github.com/credativ/credativ-pg-migrator-dev/credativ_pg_migrator/postgresql_connector.py", line 502, in execute_query
cursor.execute(query, params)
psycopg2.errors.InvalidForeignKey: there is no unique constraint matching given keys for referenced table "inventory"
