# Feature Matrix

// TODO - table is NOT complete

Different features and differently supported across various database connectors. This file provides overview of the supported features and their status.

Legend:

- WIP = work in progress, feature is not yet supported but is being worked on
- yes = feature is supported and was successfully tested
- ? = status unclear, feature is generally implemented, must be better tested for the specific database
- -- = feature is not implemented yet
- N/A = feature is not supported by the specific database ("\*" = requires deeper checking in documentation)

Note to the unclear status - the biggest issue is to find reasonable testing database with the features properly used.

```
| Feature                                   | IBM DB2 | Informix | MSSQL  | MySQL | Oracle | PostgreSQL | SQL      | Sybase |
| description                               | LUW     |          | Server |       |        |            | Anywhere | ASE    |
|-------------------------------------------|---------|----------|--------|-------|--------|------------|----------|--------|
| Pre-migration analysis                    | WIP     | WIP      | WIP    | WIP   | WIP    | WIP        | WIP      | WIP    |
| Migration of data                         | yes     | yes      | yes    | yes   | yes    | yes        | yes      | yes    |
| NOT NULL constraints                      | yes     | yes      | yes    | yes   | yes    | yes        | yes      | yes    |
| Default values on columns                 | WIP     | WIP      | WIP    | WIP   | WIP    | WIP        | WIP      | yes[4] |
| IDENTITY columns                          | --      | yes      | ?      | ?     | yes[1] | WIP        | ?        | yes    |
| Computed(generated) columns               | --      | --       | --     | --    | --     | WIP        | --       | yes[5] |
| Custom defined replacements of data types | yes     | yes      | yes    | yes   | yes    | yes        | yes      | yes    |
| Implicit default values replacements[6]   | --      | --       | --     | --    | --     | --         | --       | yes    |
| Custom repl. of default values            | yes     | yes      | yes    | yes   | yes    | yes        | yes      | yes    |
| Primary Keys                              | yes     | yes      | yes    | yes   | yes    | yes        | yes      | yes    |
| Secondary Indexes                         | yes     | yes      | yes    | yes   | yes    | yes        | yes      | yes    |
| Foreign Keys                              | yes     | yes      | yes    | yes   | yes    | yes        | yes      | yes    |
| FK on delete action                       | --      | --       | --     | --    | yes    | WIP        | --       | N/A*   |
| Check Constraints                         | --      | yes      | --     | --    | --     | WIP        | --       | yes    |
| Check Rules/Domains[3]                    | --      | --       | --     | --    | --     | --         | --       | yes    |
| Comments on columns                       | --      | --       | --     | --    | --     | WIP        | --       | N/A*   |
| Comments on tables                        | --      | --       | --     | --    | --     | WIP        | --       | N/A*   |
| Migration of views                        | WIP     | WIP      | WIP    | WIP   | WIP    | WIP        | WIP      | WIP    |
| Conversion of user defined funcs/procs    | --      | yes      | --     | --    | --     | yes        | --       | --     |
| Conversion of user defined triggers       | --      | yes      | --     | --    | --     | yes        | --       | --     |
| Sequences[2]                              | --      | --       | --     | --    | --     | --         | --       | N/A*   |
| SQL functions mapping                     | WIP     | WIP      | WIP    | WIP   | WIP    | WIP        | WIP      | WIP    |
| ....                                      | --      | --       | --     | --    | --     | --         | --       | --     |

```

Notes:

- [1]: IDENTITY columns are recognized based on sequence used as the default value. But there is still an issue with data types. Oracle allows PRIMARY KEY on NUMBER with sequence. But IDENTITY column in PostgresSQL must be INT or BIGINT.
- [2]: Sequences are not explicitly migrated (presuming source database implements them). But SERIAL/BIGSERIAL and IDENTITY columns and columns with a sequence as default value are migrated into PostgreSQL as IDENTITY columns. Which means the sequence is created in PostgreSQL automatically. The current value of the sequence is set to the last value found in migrated data after the data migration is finished.
- [3]: Check rules/domains are addiional checks externally defined and bound to specific column or data type. In PostgreSQL they are implemented as [domains](https://www.postgresql.org/docs/current/sql-createdomain.html), in some other databases as rules bind to columns/data types. Currently we work on implementing this feature for Sybase ASE migration.
- [4]: Sybase ASE has SQL command CREATE DEFAULT which creates independent named default value and this can be attached to a multiple columns using its name. PostgreSQL does not support this, therefore we attach corresponding underlying default value directly to the target column.
- [5]: Sybase ASE in some cases creates internal computed columns, not visible in selects, but documented in system tables. One example is column for this index: CREATE NONCLUSTERED INDEX IX_Products_LowerProductName ON dbo.Products (LOWER(ProductName)) - Sybase created internal calculated materialized column "sybfi4_1" with computation formula "AS LOWER(ProductName) MATERIALIZED". There internal computed columns have status3 = 1 – Indicates a hidden computed column for a function-based index key. This feature also means that the index has different DDL command in system tables - uses the hidden column: CREATE INDEX IX_Products_LowerProductName_608002166_4 ON Products (sybfi4_1);
- [6]: Typical most commonly used default values not compatible with target PostgreSQL syntax are replaced implicitly during migration.

## Tested versions of databases

- IBM DB2 LUW: (latest)
- Informix: 14.10
- MS SQL Server: 2022
- MySQL: 5.7
- Oracle: 21.3
- PostgreSQL: 14, 17
- SQL Anywhere: 17
- Sybase ASE: 16.0

## Strange findings during testing

### Informix to PostgreSQL - iwadb

#### PostgreSQL does not allow to create foreign key constraint on column which is part of composite primary key?

2025-05-22 12:40:33,060: [DEBUG] Target table SQL: CREATE TABLE "iwadb"."inventory" ("i_artid" BIGSERIAL , "i_suppid" INTEGER , "i_quantity" INTEGER , "i_descr" VARCHAR )

2025-05-22 12:40:33,094: [DEBUG] Processed index: {'source_schema': 'dwa', 'source_table': 'inventory', 'source_table_id': 108, 'index_owner': 'informix', 'index_name': 'f10', 'index_type': 'INDEX', 'target_schema': 'iwadb', 'target_table': 'inventory', 'index_columns': '"i_suppid"', 'index_comment': '', 'index_sql': 'CREATE INDEX "f10_tab_inventory" ON "iwadb"."inventory" ("i_suppid");'}
2025-05-22 12:40:33,098: [DEBUG] Processed index: {'source_schema': 'dwa', 'source_table': 'inventory', 'source_table_id': 108, 'index_owner': 'informix', 'index_name': 'p11', 'index_type': 'PRIMARY KEY', 'target_schema': 'iwadb', 'target_table': 'inventory', 'index_columns': '"i_artid", "i_suppid"', 'index_comment': '', 'index_sql': 'ALTER TABLE "iwadb"."inventory" ADD CONSTRAINT "p11_tab_inventory" PRIMARY KEY ("i_artid", "i_suppid");'}

2025-05-22 12:40:44,093: [DEBUG] Worker 92b76014-c1fe-41ae-a9db-6e7aaab0cc9f: Creating constraint with SQL: ALTER TABLE "iwadb"."partlist" ADD CONSTRAINT "f15_tab_partlist" FOREIGN KEY (p_artid) REFERENCES "iwadb"."inventory" (i_artid)
2025-05-22 12:40:44,127: [ERROR] An error in Orchestrator (constraint_worker 92b76014-c1fe-41ae-a9db-6e7aaab0cc9f f15): there is no unique constraint matching given keys for referenced table "inventory"

2025-05-22 12:40:44,129: [ERROR] Traceback (most recent call last):
File "/home/josef/github.com/credativ/credativ-pg-migrator-dev/credativ_pg_migrator/orchestrator.py", line 520, in constraint_worker
worker_target_connection.execute_query(create_constraint_sql)
File "/home/josef/github.com/credativ/credativ-pg-migrator-dev/credativ_pg_migrator/postgresql_connector.py", line 502, in execute_query
cursor.execute(query, params)
psycopg2.errors.InvalidForeignKey: there is no unique constraint matching given keys for referenced table "inventory"
