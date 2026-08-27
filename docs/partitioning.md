# Partitioning

How `credativ-pg-migrator` handles partitioned tables: what it reads from each of the twelve
source databases, what it builds on PostgreSQL, what it refuses, and what it tells you before it
creates anything.

> **State of this feature.** Only the PostgreSQL source has been run against a live server
> (PostgreSQL 18). The other ten implementations were written against the documented catalogue
> of their engine and are exercised by an extensive test suite, but **have not yet met a real
> database of their own kind**. Sybase ASE is the least certain of them and is marked `?` rather
> than `yes` in [`FEATURE_MATRIX.md`](../FEATURE_MATRIX.md). Read the `[ PARTITIONING ]` block of
> your first run before trusting it.

---

## 1. Two halves which are worth telling apart

**Reading how the source partitions** is per connector. It answers *what is there* and *what this
run will do about it*, and it is reported before anything is created. Every one of the twelve
sources whose engine partitions tables reads it; SQL Anywhere and SQLite have none, and say so.

**Creating the target partitioned** — the `target_partitioning` block — reads a configuration and
writes PostgreSQL. It looks at nothing of the source and therefore works for every connector.

The two are independent. You can partition a target whose source was not partitioned, and you can
flatten a source which was.

---

## 2. What the configuration says

### 2.1 `migration.source_partitioning`

What becomes of a table which the **source** partitions.

```yaml
migration:
  source_partitioning: "preserve"   # preserve (default) | flatten
```

| | |
|---|---|
| `preserve` *(default)* | build the same scheme on the target, as far as the source's scheme has a PostgreSQL counterpart. What each engine carries over is section 5. |
| `flatten` | build one ordinary table. The scheme is dropped, and the run says so per table rather than doing it quietly. |

Per table, through `table_settings`:

```yaml
table_settings:
  - table_name: "audit_log"
    source_partitioning: "flatten"    # this one table arrives as one ordinary table
```

A **partition** is never migrated as a table of its own. It is created with its parent and its
rows arrive through it.

### 2.2 `target_partitioning`

A scheme the source never had. It **wins** over `source_partitioning` for the tables it names.

```yaml
target_partitioning:
  - table_name: "orders"
    partition_by: "RANGE"             # RANGE | LIST | HASH
    partitioning_columns: "created_at"
    date_range: "month"               # year | quarter | month | week | day
    default_partition: true           # optional
    partition_name: "{table}_{start:%Y%m}"   # optional
```

`date_range` reads the smallest and the largest value the column really holds in the source and
generates one partition per interval, one interval past the newest row and one before the oldest.
See [the configuration reference](config_reference.md) for every key.

> `date_range` needs to ask the source what a column holds. **IBM Db2 for z/OS and IBM Db2 for i
> cannot be asked** — they are offline connectors reading `.sql` extracts and CSV files — so such
> an entry is refused against those two before anything is created.

---

## 3. What PostgreSQL requires

These are properties of PostgreSQL, not of this migrator, and they decide most of what follows.

1. **Every primary key, unique constraint and unique index must contain all partitioning
   columns.** This is the rule which breaks migrations. It is checked before anything is created —
   for a `target_partitioning` entry *and* for a scheme carried over from the source — and a key
   which does not contain them stops the run, naming the key, its columns and what to write
   instead.
2. **There are no global indexes.** An index on a partitioned table is a partitioned index.
   Oracle's global index and SQL Server's non-aligned index have no counterpart.
3. **A row which fits no partition is refused.** Either the partitions cover everything, or there
   is a `DEFAULT` partition, or inserts fail after the migration.
4. **A RANGE partition takes no NULL.** Only a `DEFAULT` partition does.
5. **The upper bound of a range is always exclusive**, and the lower bound always inclusive.
   Five of the twelve engines write it the other way round — see section 4.
6. **A LIST key takes exactly one column.**
7. **A table partitioned by an expression can have no primary key or unique constraint at all**,
   because no constraint can contain an expression.

---

## 4. The one mismatch which runs through the whole feature

PostgreSQL's `FOR VALUES FROM (a) TO (b)` includes `a` and excludes `b`. Several engines write an
upper bound which **includes** the value:

| engine | how it writes an upper bound | inclusive? |
|---|---|---|
| Oracle | `VALUES LESS THAN (x)` | no — maps directly |
| MySQL / MariaDB | `VALUES LESS THAN (x)` | no — maps directly |
| SQL Server `RANGE RIGHT` | boundary belongs to the partition above | no — maps directly |
| **IBM Db2** | `ENDING AT (x) INCLUSIVE` — **and INCLUSIVE is the default** | **yes** |
| **Sybase ASE** | `VALUES <= (x)` | **yes** |
| **SQL Server `RANGE LEFT`** | boundary belongs to the partition below | **yes** |
| **Informix** | `col <= v` in a fragment expression | **yes** |

Where the bound is inclusive, the migrator converts it to the exclusive bound which holds the
same rows — `ENDING AT '2024-12-31'` becomes `TO ('2025-01-01')`, `VALUES <= (100)` becomes
`TO (101)`. That needs the **next value of the type**, which only a date and a whole number have.
Where the type has no next value — a `DECIMAL` with a scale, a `VARCHAR`, a `TIMESTAMP`, SQL
Server's `datetime` counted in units of 1/300 of a second — **the scheme is refused rather than
moved by a guess**.

This matters more than it looks. A bound copied across unchanged does not fail: the rows still
load, into the partition next door, and nothing in the run says a word.

---

## 5. The twelve sources

Each section says what the engine has, what is read, what `preserve` carries over, what is
reported but not carried over, and what stops the run.

---

### 5.1 PostgreSQL

**The reference implementation, and the only one verified against a live server.**

| | |
|---|---|
| **has** | declarative RANGE, LIST and HASH, and sub-partitioning of any of them |
| **read from** | `pg_partitioned_table`, `pg_get_partkeydef()`, `pg_inherits`, `pg_class.relispartition`, `pg_stats` |
| **carried over** | the whole scheme, **sub-partitions and all**. The bounds are already PostgreSQL's and are not rewritten |
| **reported** | a `DEFAULT` partition and what it costs — attaching a partition later makes PostgreSQL scan it to prove no row belongs in the new one |
| **stops the run** | a partition with no bound in the catalogue; a scheme whose key could not be read; a method the target's version is too old for (HASH and `DEFAULT` need 11) |

A partition of the source is a relation of its own, so the connector answers for it, and the
planner leaves it out of the table list — it is created with its parent.

**Verified** against PostgreSQL 18: 40 tables, 77 indexes, every row count matching, with
`preserve`, `flatten` and `target_partitioning` all exercised in one migration.

---

### 5.2 Oracle

| | |
|---|---|
| **has** | RANGE, LIST, HASH, INTERVAL, REFERENCE, SYSTEM, and composite schemes |
| **read from** | `ALL_PART_TABLES`, `ALL_PART_KEY_COLUMNS`, `ALL_SUBPART_KEY_COLUMNS`, `ALL_TAB_PARTITIONS`, `ALL_INDEXES` |
| **carried over** | RANGE, LIST and HASH — **the first level only** |

**Bounds.** `TO_DATE(' 2024-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')`
becomes `'2024-01-01 00:00:00'`; `TIMESTAMP'…'`, `TO_TIMESTAMP()`, numbers, string literals,
`N'…'`, `NULL`, `MAXVALUE` and `HEXTORAW()` are all read. A bound this migrator does not
recognise is **refused rather than guessed** — a bound guessed wrong is a partition which quietly
takes the rows of the one beside it.

**Reported, not carried over:**

* **sub-partitions.** A monthly range sub-partitioned by hash into 16 is 960 relations on the
  target — each with its own relcache entry, statistics and indexes, and autovacuum with 960
  tables to think about instead of 60. The first level is built and the run says how many
  segments were left behind.
* **INTERVAL.** The partitions which exist are carried over — they are ordinary range partitions.
  What stops is the *extending*: Oracle creates the next partition by itself and PostgreSQL does
  not, so every INSERT past the newest bound will be refused. The message names both ways out: a
  `target_partitioning` entry, or `pg_partman` on the target.
* **AUTOMATIC LIST**, for the same reason.
* **HASH placement.** The partition *count* carries over; the placement does not, because Oracle
  hashes with its own function. Nothing is lost — rows go in through the parent and the target
  routes each of them — but a partition does not hold the rows it held on Oracle.
* **global indexes**, per index. A global **unique** index is the one which cannot be reproduced
  at all.
* **per-partition tablespaces.**
* a bound carrying a **time of day**: an Oracle `DATE` holds one and the `date` the migration
  gives it does not.

**Stops the run:** `REFERENCE` partitioning (the child takes its partitions from the parent
through a foreign key and has no bound of its own) and `SYSTEM` partitioning (the application
names the partition on every INSERT — there is no key at all). Both are refused only where the
scheme would be *built*: `source_partitioning: flatten` refuses nothing.

**The Oracle case worth knowing about.** Oracle keeps a `PRIMARY KEY (ORDER_ID)` on a table
partitioned by `ORDER_DATE` in a **global index** — legal there, and ordinary. PostgreSQL has no
global index, so without a check the table would be created, every row loaded, and `ADD PRIMARY
KEY` refused at the very end of the run. It is refused before anything is created instead.

---

### 5.3 IBM Db2 for LUW

| | |
|---|---|
| **has** | table partitioning by range, **DPF** (database partitioning) and **MDC** (multi-dimensional clustering) — three mechanisms which all say "partition" and only one of which is one |
| **read from** | `SYSCAT.DATAPARTITIONS`, `SYSCAT.DATAPARTITIONEXPRESSION`, `SYSCAT.TABLES.PARTITION_MODE`, `SYSCAT.INDEXES`, `SYSCAT.COLUMNS` |
| **carried over** | table partitioning by range, with the inclusive bounds converted (section 4) |

**Reported, not carried over:**

* **DPF.** The rows are spread over the physical nodes of the instance by a hash of the
  distribution key — which is named. PostgreSQL has no counterpart of any kind, and **nothing
  about the table needs changing for it**: the target is one server holding all the rows.
* **MDC.** A storage layout deciding which rows share a block; its dimensions are named. The
  nearest PostgreSQL things are `CLUSTER` and a BRIN index, and neither of them is this.
* **table spaces**, where the partitions are spread over more than one.
* a data partition which is **not in the attached state** on the source.

A table which has DPF or MDC and **no data partition** is not reported as partitioned — it is not
a partitioned table in the sense PostgreSQL means, and answering that it is would build a
partitioned target for a scheme which is not one. What there is to say about it is still said.

**Stops the run:** an inclusive bound whose column type has no next value; a range over more than
one column.

**`system_catalog: SYSIBM`.** The standard views describe none of the three mechanisms, so a
migration configured that way reports the partitioning as **not read** rather than answering that
nothing is partitioned. Set `source.system_catalog: SYSCAT` to have it read.

---

### 5.4 IBM Db2 for z/OS and IBM Db2 for i

Both are **offline** connectors: the structure comes from `.sql` extracts and the data from CSV
files. The `PARTITION BY` clause of the `CREATE TABLE` text *is* the catalogue.

| | |
|---|---|
| **has** | z/OS: partitioned table spaces, partition-by-range, partition-by-growth. For i: `PARTITION BY RANGE` and `PARTITION BY HASH … INTO n PARTITIONS` |
| **read from** | the parsed DDL, stored in the `ddl_tables` protocol table |
| **carried over** | RANGE (bounds converted per section 4) and HASH (count only) |

`PARTITION n ENDING AT (x)` — z/OS's ordinary spelling, which says nothing about where a partition
*begins* — is read with Db2's own rule that the ranges of a partitioned table space are contiguous
and ordered by partition number: it starts where the one above it stopped. A numbered partition is
given a name, because a PostgreSQL relation cannot be called `3`.

**Stops the run:** **partition-by-growth** (`PARTITION BY SIZE EVERY 4G`) — a partition exists
because the one before it filled up, there is no key, and PostgreSQL routes a row by its value; a
partition list written with `EVERY`, which generates its partitions from one entry (write it with
`target_partitioning`'s `date_range` instead, which generates a calendar from the data); a range
over more than one column; and a `target_partitioning` entry with a `date_range`, because there
is no source instance to ask what a column holds.

> **Two things changed for these connectors in this release.** The partitioning of the source
> used to be written into the target's `COMMENT ON TABLE` as free text — it is read as a scheme
> now, and the comment column holds the comment the DDL really carried. And the DDL parser did
> not know the word `RANGE` at all, so `PARTITION BY RANGE (col) (…)` — the spelling of every
> release since Db2 9 — was never matched; where it did match, its bracket stopped at the first
> closing one and a list of three partitions came back as one.

---

### 5.5 Informix

**The source where the honest report is worth more than a translation.** A table is *fragmented*
across **dbspaces** to spread its I/O over devices — the fragment is a place, not a class of row —
and PostgreSQL does not need a partition to put a table in a tablespace.

| | |
|---|---|
| **has** | fragmentation by ROUND ROBIN, by EXPRESSION, by RANGE/INTERVAL and LIST (12.10+), and HYBRID |
| **read from** | `sysfragments`, `systables`, `syscolumns`, `sysindexes`, `sysconstraints` |
| **carried over** | a fragmentation whose expressions really are a range or a list over one column |

**The expression chain, which is the common case.**

```sql
FRAGMENT BY EXPRESSION
  order_date < DATE('2023-01-01')  IN dbs1,
  order_date < DATE('2024-01-01')  IN dbs2,
  REMAINDER                        IN dbs3
```

is a range scheme spelled Informix's way, and it is carried over. What makes it readable is
**first-match**: Informix evaluates the fragments in order and stops at the first true one, so the
second fragment really holds 2023 — the first already took everything below it. Read literally,
the second expression *covers* the first, and copying the pair into PostgreSQL bounds would be
refused by the target as overlapping partitions. `REMAINDER` becomes the `DEFAULT` partition.

`col IN (…)` and `col = v` become a LIST scheme the same way.

**Reported:** the strategy, the fragments, the dbspaces (not carried over), and — free from
`sysfragments.nrows` — the **skew**: *"990,000 of 990,010 rows sit in the single fragment `big`. A
scheme that skewed prunes nothing."* Empty fragments are counted; a table nobody has run `UPDATE
STATISTICS` over says its spread is **not known** rather than reporting zeros.

**Stops the run:** **ROUND ROBIN** (no key, no expression, nothing about a row decides where it
goes); **HYBRID** (two strategies at once, the inner one a dbspace spread); an **arbitrary boolean
expression**, refused with its own text quoted; fragments over different columns; fragments which
**overlap**; two bounds this migrator cannot order; `col <= v` and `col > v`, which need the next
value of a type a fragment expression does not carry; and a bound which is not a literal —
`TODAY` in a bound is a boundary which moves.

---

### 5.6 MySQL and MariaDB

One implementation for both — the two are one dialect, as their query conversion already is.

| | |
|---|---|
| **has** | `RANGE`, `RANGE COLUMNS`, `LIST`, `LIST COLUMNS`, `HASH`, `LINEAR HASH`, `KEY`, `LINEAR KEY`, and sub-partitions of the first two |
| **read from** | `information_schema.PARTITIONS`, `.COLUMNS`, `.STATISTICS`, `.KEY_COLUMN_USAGE` — the whole scheme in one query |
| **carried over** | RANGE and RANGE COLUMNS (`VALUES LESS THAN` maps directly), LIST and LIST COLUMNS over one column, and the *count* of a hash scheme |

**Reported, not carried over:**

* **hash placement.** MySQL hashes an integer expression with its own function, `LINEAR HASH` uses
  a power-of-two variant, and `KEY` uses the internal hashing of the storage engine. PostgreSQL
  hashes the column value with its own. The same column and the same partition count do **not**
  put the same rows in the same partition.
* **sub-partitions.** The first level is built; the segments left behind are counted, with the
  rows of each segment added back into the partition it belongs to.

**Stops the run:**

* **a partitioning expression which is not a bare column** — and the reason is not that PostgreSQL
  cannot express it. It can. What a table partitioned by an expression cannot then have is a
  primary key or a unique constraint **of any kind**, because every unique constraint of a
  partitioned table must contain all its partitioning *columns* and no constraint can contain an
  expression. MySQL's rule runs the other way and lets `PARTITION BY RANGE (YEAR(hired))` sit
  happily beside `PRIMARY KEY (id, hired)` — so the tables which use the commonest MySQL idiom of
  all are precisely the tables with a key to lose. The message names the short way out:

  ```yaml
  target_partitioning:
    - table_name: "employees"
      partition_by: "RANGE"
      partitioning_columns: "hired"
      date_range: "year"
  ```

* **`PARTITION BY KEY()` with no column list**, which means the primary key and which
  `information_schema` writes as no expression at all.
* a **LIST key over more than one column**.

> `information_schema.PARTITIONS` holds a row for an *unpartitioned* table as well, with no method
> in it — so an empty answer is not what says a table has no scheme.

---

### 5.7 Sybase ASE

**The connector whose catalogue reading is written from the documentation of the engine rather
than against a live server.** That is not a footnote here — it shapes what it does.

| | |
|---|---|
| **has** | semantic partitioning since ASE 15: RANGE, HASH, LIST and ROUND ROBIN, over **segments**, for I/O across devices and for parallel scans |
| **read from** | `syspartitions`, `syspartitionkeys`, `syscolumns`, `sysindexes`, `sysreferences` |
| **carried over** | RANGE (bounds converted per section 4), LIST over one column, and the count of a HASH scheme |

**How the method is worked out.** ASE keeps it in a place this migrator cannot point at with
confidence, so it is derived from two things which it can read:

| | |
|---|---|
| **no partitioning key at all** | ROUND ROBIN — the one method of the four with no key |
| a key **and** conditions | RANGE or LIST, told apart by the shape of the condition |
| a key and conditions which are **empty** | HASH, which has none |
| a key and conditions which **could not be read** | not known, and nothing is built |

**That last row is the point.** A condition which came back *empty* is not a condition which
could not be *read*. The first means the scheme has no conditions, which is HASH; the second means
nothing at all. A HASH built out of a RANGE nobody could read is the one wrong answer with **no
symptom**: every row would be loaded into the wrong partition and not a single step of the run
would fail. So the condition is read in tiers — the column `syspartitions` is documented to carry
it, and then `sp_helpartition`, ASE's own report, whose result set is read *by the names of its
columns* because their order differs between releases — and when neither answers, the scheme is
reported in full with its method stated as **not known**, and the run stops.

**Stops the run:** ROUND ROBIN; a scheme whose conditions could not be read; an inclusive bound
whose column type has no next value; a LIST key over more than one column.

**What a live ASE still has to confirm:** that `syspartitions` and `syspartitionkeys` carry the
columns read here under these names; that a data partition is `indid IN (0, 1)`; the meanings of
the `syscolumns.status` and `sysindexes.status` bits; and above all **where the partition
condition really lives**. The run says per table which of its reads answered, so a first migration
reports what it got rather than failing.

---

### 5.8 MS SQL Server

A SQL Server partitioning is not written on the table. It is a partition **function**, which says
where the boundaries are, and a partition **scheme**, which maps the partitions onto filegroups;
the table is created *on* the scheme.

| | |
|---|---|
| **has** | RANGE and nothing else — no LIST, no HASH |
| **read from** | `sys.partition_functions`, `sys.partition_range_values`, `sys.partition_schemes`, `sys.partitions`, `sys.destination_data_spaces`, `sys.indexes` |
| **carried over** | the ranges |

**`RANGE LEFT` and `RANGE RIGHT`, which is the whole difficulty.** A function with boundaries
b1 < b2 < … makes one more partition than it has boundaries, and one bit decides which side of a
boundary the boundary value falls on:

| | partition 1 | partition k | last |
|---|---|---|---|
| **RANGE RIGHT** | `(-inf, b1)` | `[b(k-1), bk)` | `[bn, +inf)` |
| **RANGE LEFT** | `(-inf, b1]` | `(b(k-1), bk]` | `(bn, +inf)` |

`RANGE RIGHT` **is** `FROM (a) TO (b)` and maps with nothing done to it — and it is the direction
almost every sliding-window scheme uses, so the common case is free. `RANGE LEFT` is the opposite
at both ends, so every bound moves to the next value of the type.

**Reported, not carried over:** the **filegroups** the scheme maps onto; the **per-partition
compression**, which PostgreSQL has no counterpart for; a **non-aligned** index — SQL Server's
answer to the question Oracle answers with a global index, and PostgreSQL has neither — with a
non-aligned *unique* index called out as the one which cannot exist at all; the skew, out of
`sys.partitions.rows`, which SQL Server keeps exactly for a rowstore; and empty partitions, where
one at each end is how a sliding window is kept.

**A nullable partitioning column is named** for what it costs: SQL Server puts a row whose key is
NULL in the lowest partition, PostgreSQL puts it in none at all. Whether the column really holds a
NULL is not something the catalogue answers, so it is said rather than refused — write the scheme
with `target_partitioning` and `default_partition: true`, or make the column `NOT NULL` first.

**Stops the run:** a `RANGE LEFT` function over a type with no next value — notably `datetime`,
which SQL Server counts in units of 1/300 of a second (which is *why* such boundaries are written
`'2023-12-31 23:59:59.997'`); and a catalogue whose partition count and boundary count do not add
up, because a function with n boundaries makes n + 1 partitions and a scheme read wrongly is a
different scheme wearing the right number of partitions.

---

### 5.9 SAP SQL Anywhere and SQLite

Neither engine has table partitioning. That is a fact about the engine, not a gap in this
migrator, and both connectors declare it — so the report says *"SQL Anywhere has no table
partitioning at all"* rather than showing an empty table which could mean either.

`target_partitioning` works normally for both: a SQLite database can be migrated into a
partitioned PostgreSQL table like any other source.

---

## 6. What the run tells you

**Before anything is created**, the pre-migration analysis prints a `[ PARTITIONING ]` block:

```
planner: check_partitioning: ***** Partitioning *****
planner: check_partitioning: 12 of 340 table(s) are partitioned on the source, holding 640 partition(s); 4 of them are partitioned on more than one level. 1 table(s) are partitioned by target_partitioning.
planner: check_partitioning: Table    | Source scheme      | Partitions | What happens
planner: check_partitioning: orders   | RANGE (ORDER_DATE) | 60         | RANGE (ORDER_DATE) - preserved, 60 partition(s)
planner: check_partitioning: payments | LIST (METHOD)      | 5          | LIST (METHOD) on the source - FLATTENED into one table, 5 partition(s) dropped
```

followed by every note each table earned, every `target_partitioning` entry with what was checked
about it, and every finding which stops the run.

**After the run**, the closing summary has a `[ PARTITIONING ]` block naming what each table was
partitioned by on both sides and how many partitions each side got:

```
[ PARTITIONING ]
--------------------------------------------------------------------------------------------
Table    | Source scheme                           | Parts | Target scheme      | Parts | What happened
--------------------------------------------------------------------------------------------
ORDERS   | RANGE (ORDER_DATE) / HASH (CUSTOMER_ID) |     2 | RANGE (order_date) |     2 | first 1 level(s) preserved, 1 NOT carried over
payments | LIST (method)                           |     3 | -                  |     - | FLATTENED into one table

1 table(s) partitioned on the source arrive as one ordinary table: payments.
```

The source scheme is shown in the names the source has and the target scheme in the names the
target has — the two columns are of two different databases. A scheme of more than one level is
written as the levels it has, so a reader who sees only the first one still knows the table is
two levels deep. `summary.report_filename` writes the whole summary into a file.

**In the migration database**, `<protocol>_source_table_partitioning` holds one row per level of
the source scheme — with the bounds as the source wrote them and a
`source_partitioning_engine_specific` column of JSON holding what only that engine has — and
`<protocol>_target_table_partitioning` holds what PostgreSQL was really given. See
[Migration Database Tables](migration_tables.md).

**A check which could not be made is reported as not made**, never as one which passed. A column
nobody has gathered statistics for has a null count which is *not known*; a catalogue read which
failed says so; an estimate is marked as an estimate.

---

## 7. Summary

| source | reads the source scheme | carried over by `preserve` | refuses |
|---|---|---|---|
| PostgreSQL | yes — **verified live** | RANGE, LIST, HASH, sub-partitions and all | a bound the catalogue does not hold |
| Oracle | yes | RANGE, LIST, HASH — first level only | REFERENCE, SYSTEM, an unreadable bound |
| Db2 LUW | yes | range partitioning | inclusive bound with no next value, multi-column range |
| Db2 z/OS | yes (from DDL) | RANGE | partition-by-growth, `EVERY`, `date_range` entries |
| Db2 for i | yes (from DDL) | RANGE, HASH | as above |
| Informix | yes | expression chains which are a range or a list | ROUND ROBIN, HYBRID, arbitrary expressions, overlaps |
| MySQL / MariaDB | yes | RANGE, LIST, HASH — first level only | a partitioning expression, `KEY()`, multi-column LIST |
| MS SQL Server | yes | the ranges | RANGE LEFT with no next value, an inconsistent catalogue |
| Sybase ASE | yes — **catalogue unverified** | RANGE, LIST, HASH | ROUND ROBIN, conditions which could not be read |
| SQL Anywhere | n/a — has none | — | — |
| SQLite | n/a — has none | — | — |

`target_partitioning` works for **every** source, except that its `date_range` cannot be used
against the two offline Db2 connectors.

---

## 8. What is not built yet

* the `primary_key: extend | keep | drop` decision — today a key which does not contain the
  partitioning columns **stops the run and names what to write**, which is the check and not yet
  the repair;
* a `future:` window on a generated scheme, and handing its maintenance to `pg_partman`;
* `LIST` and `HASH` entries in `target_partitioning` — only `RANGE` with a `date_range` generates
  its partitions today;
* sub-partitioning of a target scheme;
* the nullable-partitioning-column finding of section 5.8 for the sources other than SQL Server;
* running any of the eleven non-PostgreSQL implementations against a live server of its engine.
