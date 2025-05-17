# Feature Matrix

// TODO - talbe is not complete

Different features and differently supported across various database connectors. This file provides overview of the supported features and their status.

```
| Feature                   | IBM DB2 | Informix | MSSQL  | MySQL | Oracle | PostgreSQL | SQL      | Sybase |
| description               | LUW     |          | Server |       |        |            | Anywhere | ASE    |
|---------------------------|---------|----------|--------|-------|--------|------------|----------|--------|
| Migration of data         | YES     | YES      | YES    | YES   | YES    | YES        | YES      | YES    |
| NOT NULL constraints      | YES     | YES      | YES    | YES   | YES    | YES        | YES      | YES    |
| Primary Keys              | YES     | YES      | YES    | YES   | YES    | YES        | YES      | YES    |
| Foreign Keys              | YES     | YES      | YES    | YES   | YES    | YES        | YES      | YES    |
| Check Constraints         | No      | YES      | No     | No    | No     | No         | No       | No     |
| Comments on columns       | No      | NO       | No     | No    | No     | No         | No       | No     |
| Comments on tables        | No      | NO       | No     | No    | No     | No         | No       | No     |
| Conversion of funcs/procs | No      | YES      | No     | No    | No     | YES        | No       | No     |
| Conversion of triggers    | No      | YES      | No     | No    | No     | YES        | No       | No     |
```
