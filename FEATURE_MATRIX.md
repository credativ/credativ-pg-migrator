# Feature Matrix

// TODO - table is NOT complete

Different features and differently supported across various database connectors. This file provides overview of the supported features and their status.

Legend:

- YES - feature is supported
- NO - feature is not supported / or we are not sure (must be tested)
- N/A - feature is not supported by the specific database (? = need to be checked)

```
| Feature                   | IBM DB2 | Informix | MSSQL  | MySQL | Oracle | PostgreSQL | SQL      | Sybase |
| description               | LUW     |          | Server |       |        |            | Anywhere | ASE    |
|---------------------------|---------|----------|--------|-------|--------|------------|----------|--------|
| Migration of data         | YES     | YES      | YES    | YES   | YES    | YES        | YES      | YES    |
| NOT NULL constraints      | YES     | YES      | YES    | YES   | YES    | YES        | YES      | YES    |
| IDENTITY columns          | NO      | NO       | NO     | NO    | YES[1] | NO         | NO       | YES    |
| Primary Keys              | YES     | YES      | YES    | YES   | YES    | YES        | YES      | YES    |
| Secondary Indexes         | YES     | YES      | YES    | YES   | YES    | YES        | YES      | YES    |
| Foreign Keys              | YES     | YES      | YES    | YES   | YES    | YES        | YES      | YES    |
| FK on delete action       | NO      | NO       | NO     | NO    | YES    | NO         | NO       | N/A?   |
| Check Constraints         | NO      | YES      | NO     | NO    | NO     | NO         | NO       | YES    |
| Comments on columns       | NO      | NO       | NO     | NO    | NO     | NO         | NO       | N/A?   |
| Comments on tables        | NO      | NO       | NO     | NO    | NO     | NO         | NO       | N/A?   |
| Conversion of funcs/procs | NO      | YES      | NO     | NO    | NO     | YES        | NO       | NO     |
| Conversion of triggers    | NO      | YES      | NO     | NO    | NO     | YES        | NO       | NO     |
| Sequences[2]              | --      | --       | --     | --    | --     | YES        | --       | N/A?   |
| ....                      | NO      | NO       | NO     | NO    | NO     | NO         | NO       | NO     |

```

Notes:
[1]: IDENTITY columns are recognized based on sequence used as the default value. But there is still an issue with data types. Oracle allows PRIMARY KEY on NUMBER with sequence. But IDENTITY column in PostgresSQL must be INT or BIGINT.
[2]: Sequences are not explicitly migrated (presuming source database implements them). But SERIAL/BIGSERIAL and IDENTITY columns are migrated as IDENTITY columns into PostgreSQL which means that the sequence is created in PostgreSQL automatically. The current value of the sequence is set to the last value found in migrated data after the data migration is finished.

## Tested versions of databases

- IBM DB2 LUW: (latest)
- Informix: 14.10
- MS SQL Server: 2022
- MySQL: 5.7
- Oracle: 21.3
- PostgreSQL: 14, 17
- SQL Anywhere: 17
- Sybase ASE: 16.0
