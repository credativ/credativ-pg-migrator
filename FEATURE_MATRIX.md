# Feature Matrix

// TODO - table is NOT complete

Different features and differently supported across various database connectors. This file provides overview of the supported features and their status.

```
| Feature                   | IBM DB2 | Informix | MSSQL  | MySQL | Oracle | PostgreSQL | SQL      | Sybase |
| description               | LUW     |          | Server |       |        |            | Anywhere | ASE    |
|---------------------------|---------|----------|--------|-------|--------|------------|----------|--------|
| Migration of data         | YES     | YES      | YES    | YES   | YES    | YES        | YES      | YES    |
| NOT NULL constraints      | YES     | YES      | YES    | YES   | YES    | YES        | YES      | YES    |
| Primary Keys              | YES     | YES      | YES    | YES   | YES    | YES        | YES      | YES    |
| Foreign Keys              | YES     | YES      | YES    | YES   | YES    | YES        | YES      | YES    |
| FK on delete action       | NO      | NO       | NO     | NO    | YES    | NO         | NO       | NO     |
| Check Constraints         | NO      | YES      | NO     | NO    | NO     | NO         | NO       | NO     |
| Comments on columns       | NO      | NO       | NO     | NO    | NO     | NO         | NO       | NO     |
| Comments on tables        | NO      | NO       | NO     | NO    | NO     | NO         | NO       | NO     |
| Conversion of funcs/procs | NO      | YES      | NO     | NO    | NO     | YES        | NO       | NO     |
| Conversion of triggers    | NO      | YES      | NO     | NO    | NO     | YES        | NO       | NO     |
| ....                      | NO      | NO       | NO     | NO    | NO     | NO         | NO       | NO     |

```
