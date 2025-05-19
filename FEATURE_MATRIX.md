# Feature Matrix

// TODO - table is NOT complete

Different features and differently supported across various database connectors. This file provides overview of the supported features and their status.

Legend:

- WIP = work in progress, feature is not yet supported but is being worked on
- yes = feature is supported and was successfully tested
- -- = status unclear, must be checked in code and tested
- N/A - feature is not supported by the specific database (? = requires deeper checking in sources)

```
| Feature                   | IBM DB2 | Informix | MSSQL  | MySQL | Oracle | PostgreSQL | SQL      | Sybase |
| description               | LUW     |          | Server |       |        |            | Anywhere | ASE    |
|---------------------------|---------|----------|--------|-------|--------|------------|----------|--------|
| Migration of data         | yes     | yes      | yes    | yes   | yes    | yes        | yes      | yes    |
| NOT NULL constraints      | yes     | yes      | yes    | yes   | yes    | yes        | yes      | yes    |
| IDENTITY columns          | --      | --       | --     | --    | yes[1] | --         | --       | yes    |
| Primary Keys              | yes     | yes      | yes    | yes   | yes    | yes        | yes      | yes    |
| Secondary Indexes         | yes     | yes      | yes    | yes   | yes    | yes        | yes      | yes    |
| Foreign Keys              | yes     | yes      | yes    | yes   | yes    | yes        | yes      | yes    |
| FK on delete action       | --      | --       | --     | --    | yes    | --         | --       | N/A?   |
| Check Constraints         | --      | yes      | --     | --    | --     | --         | --       | yes    |
| Check Rules/Domains[3]    | --      | --       | --     | --    | --     | --         | --       | yes    |
| Comments on columns       | --      | --       | --     | --    | --     | --         | --       | N/A?   |
| Comments on tables        | --      | --       | --     | --    | --     | --         | --       | N/A?   |
| Conversion of funcs/procs | --      | yes      | --     | --    | --     | yes        | --       | --     |
| Conversion of triggers    | --      | yes      | --     | --    | --     | yes        | --       | --     |
| Sequences[2]              | --      | --       | --     | --    | --     | --         | --       | N/A?   |
| ....                      | --      | --       | --     | --    | --     | --         | --       | --     |

```

Notes:

- [1]: IDENTITY columns are recognized based on sequence used as the default value. But there is still an issue with data types. Oracle allows PRIMARY KEY on NUMBER with sequence. But IDENTITY column in PostgresSQL must be INT or BIGINT.
- [2]: Sequences are not explicitly migrated (presuming source database implements them). But SERIAL/BIGSERIAL and IDENTITY columns and columns with a sequence as default value are migrated into PostgreSQL as IDENTITY columns. Which means the sequence is created in PostgreSQL automatically. The current value of the sequence is set to the last value found in migrated data after the data migration is finished.
- [3]: Check rules/domains are addiional checks externally defined and bound to specific column or data type. In PostgreSQL they are implemented as [domains](https://www.postgresql.org/docs/current/sql-createdomain.html), in some other databases as rules bind to columns/data types. Currently we work on implementing this feature for Sybase ASE migration.

## Tested versions of databases

- IBM DB2 LUW: (latest)
- Informix: 14.10
- MS SQL Server: 2022
- MySQL: 5.7
- Oracle: 21.3
- PostgreSQL: 14, 17
- SQL Anywhere: 17
- Sybase ASE: 16.0
