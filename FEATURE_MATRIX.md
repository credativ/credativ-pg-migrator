# Feature Matrix

What each of the twelve source connectors really does, measured against the connector sources on
**2026-08-28**. Every cell corresponds to code which is present and reachable - not to a plan,
and not to a docstring. How it was measured is in [§11](#11-how-this-was-measured), and it can be
re-run.

## How to read it

| | |
|---|---|
| `yes` | the connector implements it |
| `part` | implemented with a limit which the note names |
| `-` | **not implemented**: the engine has this and the connector does not read it |
| `n/a` | there is nothing of the kind to read: the engine has no such object - or, for the two offline connectors, there is no live source to ask at all |
| `n/a*` | the same, but the engine's support was not re-verified in the documentation |

**Implemented is not the same as tested.** These tables say what the code does; how far each
connector has been taken against a real server is a property of the connector and not of every
single feature, and it stands once, in [§8](#8-how-far-each-connector-has-been-taken). A `yes`
against a connector which has never met a live instance of its engine means the code is there,
not that it has been proven.

Where a connector *declares in its own code* that the source has no such objects - the mechanism
which keeps "we do not read it" apart from "there is none" - the declaration is quoted in the
per-connector notes of [§9](#9-per-connector-notes).

Columns are: **LUW** = Db2 LUW, **z/OS** = Db2 for z/OS, **i** = Db2 for i, **IFX** = Informix,
**MSSQL** = MS SQL Server, **ORA** = Oracle, **PG** = PostgreSQL, **SQLA** = SQL Anywhere,
**ASE** = Sybase ASE.

---

## 1. What every connector does

Twelve out of twelve, so they are not repeated in the tables below: **tables and their data**;
**NOT NULL**; **column defaults**, including the implicit rewrites of the defaults PostgreSQL
cannot take over and the replacements configured in `data_types_substitution` /
`default_values_substitution`; **identity / auto-increment columns**; **primary keys, unique
constraints and secondary indexes**; **foreign keys** (the actions they carry are §3);
**views**, fetched and converted; the **type mapping** of the engine and the configured type
substitutions; **`--convert-queries`**, the separate conversion of an application's statements;
and the **row count and checksum** of the validation - with the one exception named in §5.

## 2. Columns

| | LUW | z/OS | i | IFX | MSSQL | MySQL | MariaDB | ORA | PG | SQLA | SQLite | ASE |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Generated / computed columns | - | - | - | - | - | yes | yes | part¹ | yes | - | yes | yes |
| Column comments | yes | -² | -² | n/a* | - | yes | yes | yes | yes | - | n/a* | n/a* |
| Table comments | yes | yes | yes | n/a* | - | yes | yes | yes | yes | - | n/a* | n/a* |
| Column collation carried over | - | - | - | - | - | - | - | - | yes | - | - | - |
| Hidden / internal columns recognised | - | - | - | - | - | - | - | part¹ | - | - | yes | yes |
| Named default objects (`CREATE DEFAULT`) | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a* | n/a | yes |

¹ Oracle: **virtual** columns only, migrated as PostgreSQL generated columns; a hidden virtual
column (the one Oracle creates for a function-based index) is excluded deliberately.
² Db2 z/OS and Db2 for i: the DDL parser stores the real comment in `ddl_columns`, and the
connector then writes the literal `'Primary Key'` into `column_comment` for every key column
instead of reading it back - so the target gets that text and not the comment of the source.

## 3. Constraints

| | LUW | z/OS | i | IFX | MSSQL | MySQL | MariaDB | ORA | PG | SQLA | SQLite | ASE |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| CHECK constraints | yes | yes | yes | yes | -¹ | - | - | yes | yes | - | yes | yes |
| `ON DELETE` action of a foreign key | - | yes | yes | - | - | - | - | yes | yes² | yes | yes | n/a* |
| `ON UPDATE` action of a foreign key | - | yes | yes | - | - | - | - | n/a | yes² | yes | yes | n/a* |

¹ MS SQL Server: the docstring of `fetch_constraints()` names `sys.check_constraints` and
`sys.key_constraints`; the code queries neither - only the foreign keys.
² PostgreSQL carries the whole `pg_get_constraintdef()` text, the actions with it.

## 4. Schema objects besides tables

| | LUW | z/OS | i | IFX | MSSQL | MySQL | MariaDB | ORA | PG | SQLA | SQLite | ASE |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Standalone sequences | yes | yes | yes | - | yes | n/a | yes | yes | yes | yes | n/a | n/a* |
| Functions / procedures | -¹ | yes | -¹ | yes | yes | - | - | yes | yes | yes | n/a | yes |
| Triggers | yes | yes | yes | yes | yes | - | - | yes | yes | yes | yes | yes |
| Aliases / synonyms | yes | yes | yes | yes | yes | n/a | n/a | yes | n/a | - | n/a | n/a* |
| User-defined types | - | - | - | - | yes | n/a | n/a | yes | yes | - | n/a | yes |
| Domains / rules | n/a | n/a | n/a | n/a | - | n/a | n/a | yes | yes | - | n/a | yes |
| Materialized views | -² | -² | -² | n/a* | n/a* | n/a | n/a | yes | yes | - | n/a | n/a* |
| Collations (`CREATE COLLATION`) | - | - | - | - | - | - | - | - | yes | - | n/a | - |
| Full text search objects | - | - | - | - | - | - | - | - | yes | - | n/a | - |
| Aggregates (`CREATE AGGREGATE`) | - | - | - | - | - | n/a | n/a | - | yes | - | n/a | - |
| Extensions | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | yes | n/a | n/a | n/a |

¹ Db2 LUW and Db2 for i have a routine *converter*, and `fetch_funcproc_names()` answers an
empty dictionary - so no routine is ever found for it to convert.
² Db2's counterpart is the materialized query table (MQT), which none of the three reads.

**Collations and full text search are the one place where an empty answer is still ambiguous.**
Only the PostgreSQL connector reads them; the other eleven inherit the base method, which answers
an empty dictionary, and none of them declares whether that means *absent* or *not read*. For
most of the engines *absent* is the likely truth - there is no `CREATE COLLATION` in MySQL, SQL
Server, Db2, Informix or SQLite - but it has to be declared per engine and it is not, so the `-`
above is what the code can be held to today. A column's own collation is a different thing: it
travels with the column for PostgreSQL, and for the other engines the name has no PostgreSQL
counterpart, so the reference is dropped and reported and the column keeps the default collation.

## 5. Reading the source, analysing it, validating the result

| | LUW | z/OS | i | IFX | MSSQL | MySQL | MariaDB | ORA | PG | SQLA | SQLite | ASE |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Live catalogue (rather than DDL files) | yes | n/a¹ | n/a¹ | yes | yes | yes | yes | yes | yes | yes | yes | yes |
| Chunked reading (`chunk_size`) | yes | n/a¹ | n/a¹ | yes | yes | yes | yes | yes | yes | yes | yes | -² |
| Database size for the report | yes | - | - | - | yes | yes | yes | yes | yes | yes | yes | yes |
| Top-N tables for the report | yes | - | - | yes | yes | yes | yes | yes | yes | yes | yes | yes |
| Foreign-key dependency ranking | - | - | - | yes | - | - | - | yes | - | - | - | - |
| Row count + table checksum | yes | yes | - | yes | yes | yes | yes | yes | yes | yes | yes | yes |
| Random-sample row hashing | - | - | - | - | - | - | - | yes | yes | - | yes | - |
| LOB size verification | - | - | - | - | - | - | - | yes | yes | - | yes | - |

¹ Db2 for z/OS and Db2 for i are **offline** connectors: they never open a connection to the
source. The structure comes from `.sql` / DDL extracts (`connectivity: "ddl"`) and the data from
CSV files, so everything which needs a live instance is not a gap but the shape of the connector.
² Sybase ASE reads `chunk_size` and cannot page: older ASE has no `LIMIT … OFFSET`, so a table is
read in one pass whatever the setting says.

## 6. Converting code

| | LUW | z/OS | i | IFX | MSSQL | MySQL | MariaDB | ORA | PG | SQLA | SQLite | ASE |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| View text converted | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes |
| Routine bodies converted | -¹ | yes | -¹ | yes | yes | - | - | yes | yes | yes | n/a | yes |
| Trigger bodies converted | yes | yes | yes | yes | yes | - | - | yes | yes | yes | yes | yes |
| `--convert-queries` | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes |
| Compile-only test on the **source** | - | n/a | n/a | jdbc² | `SET NOEXEC` | `EXPLAIN` | `EXPLAIN` | `Cursor.parse` | `PREPARE` | - | `EXPLAIN` | `SET NOEXEC` |
| Names inside a routine body converted | - | - | - | part³ | yes | n/a | n/a | - | yes | - | n/a | yes |

¹ the converter is there and no routine ever reaches it, because the names are not fetched - §4.
² only when the connection is configured with `connectivity: "jdbc"`, where the driver's
`prepareStatement` compiles the statement. Db2 LUW connects natively only and has no native
mechanism, so it has none at all; SQL Anywhere has neither; the offline pair has no source to
compile against.
³ the connectors declare this themselves, in `ROUTINE_BODY_NAMES_NOT_CONVERTED`. Informix gives
the tables and views of a body the names of the target and leaves the **columns** as the source
wrote them; Db2 z/OS, Oracle and SQL Anywhere leave tables and columns both and re-point only the
schema in front of them. Unquoted names are folded to lower case by PostgreSQL, which is why this
works with `names_case_handling: lower` and breaks with `upper` and `keep` - the run warns per
routine when it does.

## 7. Partitioning

| | LUW | z/OS | i | IFX | MSSQL | MySQL | MariaDB | ORA | PG | SQLA | SQLite | ASE |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| The partitioning of the source is read | yes | yes | yes | yes | yes | yes | yes | yes | yes | n/a | n/a | yes |
| Facts for the feasibility check | yes | -¹ | -¹ | yes | yes | yes | yes | yes | yes | n/a | n/a | yes |
| Bounds probed in the data | yes | n/a¹ | n/a¹ | yes | yes | yes | yes | yes | yes | yes | yes | yes |
| Target built partitioned (`target_partitioning`) | yes | part² | part² | yes | yes | yes | yes | yes | yes | yes | yes | yes |

¹ the offline pair: there is no instance to ask, so the feasibility facts are not gathered, and
the bound probe **refuses** with a message naming the alternative rather than sending a SELECT
down a connection which does not exist.
² an entry which generates its partitions from a `date_range` is refused for these two before
anything is created, because the range would have to be read from data nobody can query.

What each engine calls partitioning, what survives the trip and what is reported rather than
reproduced is one section per source in **[docs/partitioning.md](docs/partitioning.md)**. In
short: five of the engines write an upper bound the source counts as *inside* the partition
(Db2's `ENDING AT … INCLUSIVE`, ASE's `VALUES <= (x)`, SQL Server's `RANGE LEFT`, Informix's
`col <= v`) and PostgreSQL has no inclusive upper bound at all, so every such bound is converted
to the exclusive one holding the same rows; a hash scheme keeps its partition **count** and not
the placement of a row, because every engine hashes with its own function; and a scheme
PostgreSQL has no counterpart for - Oracle's REFERENCE, SYSTEM and INTERVAL, Db2's DPF and MDC,
z/OS's partition-by-growth, Informix's ROUND ROBIN and HYBRID, SQL Server's filegroups - is named
per table rather than dropped in silence, and stops the run when it cannot be built.

---

## 8. How far each connector has been taken

| connector | connectivity | tested against | partitioning read from a live server |
|---|---|---|---|
| Db2 LUW | native (`ibm_db`) | latest | not yet - parsers run over the DDL of the examples |
| Db2 for z/OS | **offline**: DDL + CSV extracts | no live instance by design | not yet |
| Db2 for i | **offline**: DDL + CSV extracts | no live instance by design | not yet |
| Informix | odbc, jdbc | 14.10 | not yet |
| MS SQL Server | odbc, jdbc | 2022 | **yes** - SQL Server 2022 |
| MySQL | native, odbc, jdbc | 5.7; 9 for the partitioning | **yes** - MySQL 9 |
| MariaDB | native, odbc, jdbc | 10.11, partitioning only | **yes** - MariaDB 10.11 |
| Oracle | native (`oracledb`), jdbc | 21.3 / 21c | **yes** - Oracle 21c, all six flavours |
| PostgreSQL | native (`psycopg2`) | 14, 17, 18 | **yes** - PostgreSQL 18 |
| SQL Anywhere | native (`sqlanydb`), odbc | 17 | n/a - the engine has no partitioning |
| SQLite | native (`sqlite3`), ddl | 3.46 | n/a - the engine has no partitioning |
| Sybase ASE | odbc, jdbc | 16.0 SP02 | **yes** - ASE 16.0 SP02 |

The two live runs which found what no unit test could: ASE's `syspartitions` holds no partition
condition at all (the bounds are in `sp_helpartition`), and SQL Server's
`sys.partition_range_values.value` is a `sql_variant` which this connector's own output converter
was turning into nonsense - every RANGE scheme of every SQL Server source had wrong bounds.

## 9. Per-connector notes

**Db2 LUW** · Distinct types are declared as *present and not read* (`SYSCAT.DATATYPES`); Db2 has
no `CREATE DOMAIN` and says so. Routines are not fetched, so the routine converter never runs.
`migrate_sequences()` is defined **twice** in the class - only the second definition is used.

**Db2 for z/OS**, **Db2 for i** · Offline. Everything comes from the DDL extract and the CSV
files, which is why the analysis, the live probes and (for Db2 for i) the checksum are absent
rather than missing. Distinct types are declared as not read. Db2 for i does not fetch routines.

**Informix** · Distinct and named ROW types declared as not read; no `CREATE DOMAIN`. The view
conversion has no SQL parser behind it - no parser of this migrator models Informix - so the
defining query is rewritten construct by construct and anything outside that list is left as it
stands and reported. One of the two connectors which rank foreign-key dependencies.

**MS SQL Server** · Rules (`CREATE RULE`) declared as *present and not read*. CHECK constraints
and computed columns are not read either, and the docstring of `fetch_constraints()` claims two
catalogue views the code never opens.

**MySQL**, **MariaDB** · No `CREATE TYPE` and no `CREATE DOMAIN`, both declared. Triggers and
routines are **placeholders**: `fetch_triggers()`, `fetch_funcproc_names()` and
`fetch_funcproc_code()` are `pass`, and `convert_funcproc_code()` returns the empty string.
MariaDB additionally migrates standalone `SEQUENCE` objects (10.3+), which MySQL does not have.

**Oracle** · The widest connector after PostgreSQL: CHECK constraints, standalone sequences with
their bounds clamped to the PostgreSQL `bigint` range, object and collection types, 23ai domains,
materialized views, virtual columns, packages split into `<package>_<routine>` functions with
their call sites rewritten - **package state is not migrated**. The second connector which ranks
foreign-key dependencies.

**PostgreSQL** · The only connector which reads collations, full text search objects, aggregates,
extensions and the collation of a column; the only one whose foreign keys carry their actions
through the full `pg_get_constraintdef()` text.

**SQL Anywhere** · `CREATE DOMAIN` and `CREATE DATATYPE` make one and the same object there, and
both the user-defined types and the domains are declared as *present and not read*. Partitioning
is declared absent. It does read the referential actions of a foreign key, which most connectors
do not. It has no compile-only source test at all.

**SQLite** · No data dictionary for CHECK constraints, generated expressions, `AUTOINCREMENT` or
functional index expressions - all of them are parsed out of the `CREATE` statements in
`sqlite_master`. Partitioning, user-defined types and domains are declared absent. Values are
coerced to the target type during the data migration, because SQLite is dynamically typed.
Partial indexes lose their `WHERE` condition, which is recorded in the index comment.

**Sybase ASE** · The richest connector after PostgreSQL and Oracle for schema objects: named
default objects, rules migrated as domains or CHECK constraints, user-defined types, procedure
groups (`p;1`, `p;2`) split into separate routines, computed columns, and the hidden computed
columns ASE creates for function-based indexes. It cannot page a result, so `chunk_size` has no
effect.

## 10. A gotcha which is not a connector's fault

PostgreSQL refuses a foreign key which references columns that are not covered by a unique
constraint on their own. A source which allows a foreign key to reference *part* of a composite
primary key - Informix does - produces
`there is no unique constraint matching given keys for referenced table` when the constraint is
created. The reference has to be widened to the whole key, or a unique constraint added to the
referenced columns, in the source or by hand in the target.
