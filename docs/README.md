# credativ‑pg‑migrator – User Guide

## 1. What is credativ‑pg‑migrator?

credativ-pg-migrator is an offline migration tool for moving schemas and data from legacy or proprietary databases into PostgreSQL. It is written in Python and uses modular connectors for different source databases.

Supported source databases (one connector each):
- IBM DB2 LUW (live connection), IBM DB2 z/OS and IBM DB2 for i (both offline, from DDL + CSV extracts)
- Informix
- MS SQL Server
- MySQL and MariaDB (engines with INFORMATION_SCHEMA; separate connectors)
- Oracle
- PostgreSQL (special use cases)
- SQL Anywhere
- SQLite (a local file; no driver installation needed)
- Sybase ASE

How complete each connector is differs considerably — `FEATURE_MATRIX.md` in the repo carries the
per-connector, per-feature status and is reconciled against the connector sources.

Target database: PostgreSQL only.

High‑level features:

- Migrates:
  - tables & data
  - primary keys, unique constraints
  - defaults & check constraints
  - secondary indexes
  - foreign keys
  - functions / procedures (complete for Informix; best-effort for Oracle, Sybase ASE, MS SQL Server and DB2 z/OS; not implemented for MySQL, MariaDB, SQL Anywhere, DB2 LUW and DB2 for i)
  - triggers (complete for Informix; best-effort for Oracle, Sybase ASE, MS SQL Server, all three DB2 connectors and SQLite; not implemented for MySQL, MariaDB and SQL Anywhere)
  - views (schema-name replacement only for Informix and SQL Anywhere; SQL-level transpilation for the other engines)
  - Adjusts sequences on the target to match the imported data (with robust fetch routines across all supported engines).
  - Schema Mapping Workflow handling data normalization, renaming/matching functions, and managed target index/constraint creation.
- Validates:
  - Actively validates migrated data establishing independent parity checks for row counts, complete table checksum hashing, random tuple hashes, and exact LOB size byte-verification.
  - Generates comprehensive validation summaries with structured reporting, strict error handling, and log size thresholds.
- Customizable:
  - data type mappings
  - default value mappings
  - per-table data filters / WHERE conditions
- Rich logging:
  - to console + log file
  - to a migration database (PostgreSQL) holding detailed protocol tables.

---

## 2. Core Concepts & Architecture

### 2.1 Offline migration

The tool operates offline: it connects to the source database, introspects the model and data, generates PostgreSQL‑compatible structures, and writes into the target PostgreSQL instance. The speed of migration is primarily limited by underlying hardware and connectivity. Practical experience showed that weak hardware of the source database server is usually the biggest bottleneck.

### 2.2 Components

Architecture:
- Parser – parses configuration file and command‑line arguments.
- Planner – reads metadata and object definitions from the source database, converts them to PostgreSQL versions.
- Orchestrator – runs migration steps and parallel workers.
- Workers – execute data transfer and object creation in parallel.

Configuration is done via the YAML config file and the command line.

For a deep dive into the technical execution and process mapping of these components, see the [Standard Migration Workflow](workflow/standard/migration_workflow.md) and the [Data Anonymization Workflow](workflow/anonymization.md).

### 2.3 Databases involved

There are three logical databases in a migration:

- Source database
  - Any of the supported engines listed above.
  - Accessed via ODBC, JDBC, or native Python drivers (depending on connector).
  - It does not have to be a server: a SQLite source is a plain local file, and a DB2 z/OS source can be a set of offline DDL and CSV files.

- Target database
  - A PostgreSQL instance where your migrated schema and data are created.

- Migration database
  - A PostgreSQL database that stores:
    - migration protocol tables
    - original source code (e.g. procedures)
    - generated PostgreSQL code
    - success/failure indicators and timestamps for each migrated object
  - Often this is the same database as the target (same cluster), but it can also be a separate PostgreSQL database.
  - For the exact details on tracking tables and multithreading chunk coordination, see the [Migration Database Tables](workflow/standard/migration_tables.md) document.

---

## 3. Installation
### 3.1 Python package (recommended)

Requires Python ≥ 3.6.

```
python3 -m venv migrator_venv
. ./migrator_venv/bin/activate
pip install credativ-pg-migrator
```

The package installs a console script called credativ-pg-migrator.


#### Python dependencies

The package depends on:
- pyyaml – YAML config parsing
- pyodbc – ODBC connections
- tabulate – tabular output
- sqlglot – SQL parsing / transformation
- psycopg2 – PostgreSQL driver
- pandas – data handling
- jaydebeapi – JDBC connections from Python

These are installed automatically when using pip.

Some connectors additionally need a vendor driver which is **not** a declared dependency of the
package — install it only for the source engine you actually migrate from:

| Source engine | Additional Python package | Imported |
|---|---|---|
| IBM DB2 LUW | `ibm_db` (provides `ibm_db_dbi`) | at connector import |
| Oracle | `oracledb` | at connector import |
| SQL Anywhere | `sqlanydb` | at connector import |
| MySQL (native connectivity only) | `mysql-connector-python` | on connect |
| MariaDB (native connectivity only) | `mariadb` — needs `libmariadb-dev` on Debian/Ubuntu first | on connect |

The first three are imported when the connector module is loaded, so a missing package produces an
`ImportError` at start-up rather than a connection error. The two native MySQL/MariaDB drivers are
imported only when `connectivity: "native"` is actually used; with JDBC or ODBC they are not needed.
Informix, MS SQL Server, Sybase ASE, IBM DB2 z/OS, IBM DB2 for i, PostgreSQL and SQLite need nothing
beyond the packages listed above (SQLite uses the `sqlite3` module of the standard library).

### 3.2 Debian/Ubuntu packages

credativ-pg-migrator is available via the PostgreSQL community APT repository (apt.postgresql.org).

Follow the instructions on the PostgreSQL wiki to enable the repository, then install the package:

```
sudo apt-get update
sudo apt-get install credativ-pg-migrator
```

---

## 4. Supported Source Databases & Connectivity

### 4.1 General connectivity options

The tool supports connecting to source databases using multiple strategies depending on the engine:
- **ODBC**: via `pyodbc` and system ODBC drivers.
- **JDBC**: via `jaydebeapi` and JDBC `.jar` files.
- **Native Python drivers**: via engine-specific modules (e.g., `ibm_db`, `oracledb`, `psycopg2`, `sqlite3`).
- **DDL Parsing (Offline)**: Parses `.sql` schema files offline without an active network connection to the source database.

Which option is used is controlled in the YAML config for that source. At minimum specify:
- `connectivity`: "odbc", "jdbc", "native", "ddl", or a connector-specific keyword.
- A subsection with driver configuration (e.g., `odbc:`, `jdbc:`, or `ddl:`).

### 4.2 IBM DB2

IBM DB2 is supported via two fundamentally different connectors, heavily depending on the deployment target (LUW vs. z/OS):

#### 4.2.1 DB2 LUW (Linux, UNIX, Windows)
- **Mode**: Native Connection
- **Python Module**: `ibm_db` (using `ibm_db_dbi`) — install separately, see section 3.1.
- **Configuration**: Uses native connect strings. Set `connectivity: "native"`.
- **Migrated**: tables and data, primary keys, indexes, foreign keys, CHECK constraints, identity columns, table and column comments, sequences, views and triggers (converted to a PL/pgSQL trigger function + `CREATE TRIGGER`).
- **Not migrated**: functions and procedures — the connector can convert routine code, but it has no way to read the routines out of the catalog yet (`fetch_funcproc_names` is a placeholder), so nothing is ever fetched. Generated/computed columns and foreign-key `ON DELETE` actions are not handled either.

#### 4.2.2 DB2 z/OS (Mainframe)
- **Mode**: DDL Parsing and File-Based Integration (Offline Connectivity)
- **Python Module**: `psycopg2`
- **Configuration**: Uses `connectivity: "ddl"`. Unlike LUW, the z/OS connector does not connect directly to the mainframe instance. Instead, it reads provided `.sql`/DDL schema extracts offline to discover structure. Data migration is handled purely offline using source-generated `.csv` exports. It uses the `psycopg2` connection strictly to interact with the PostgreSQL `migrator_tables` for protocol persistence and mapping.
- **Usage**: You must define a `ddl:` -> `path:` attribute pointing to the directory containing your source schema DDL files and your data CSV exports.
- **Migrated**: tables and data (from CSV), primary keys, indexes (including expression-based ones), foreign keys with their `ON DELETE` / `ON UPDATE` rules, CHECK constraints, identity columns, sequences, aliases, views (including `WITH CHECK OPTION` views, recursive CTEs, `LISTAGG`, `TABLE (SELECT ...)` and materialized query tables), functions and procedures (SQL routines converted to PL/pgSQL), triggers and DB2 global variables (mapped to PostgreSQL session settings).
- **Not migrated**: table and column comments — `COMMENT ON` / `LABEL ON` statements are parsed out of the DDL and stored in the protocol tables, but they are not yet returned as comments, so none reaches the target. Generated/computed columns are not handled. External routines (COBOL, Assembler) are reported and skipped, because their load module is not part of the DDL.
- **Not available offline**: pre-migration analysis (there is no live source to measure) and the random-sample / LOB-size validation checks. Row counts and table checksums do work.

#### 4.2.3 DB2 for i (IBM i / AS/400)
- **Mode**: DDL Parsing and File-Based Integration (Offline Connectivity), like z/OS
- **Configuration**: `connectivity: "ddl"` with a `ddl:` -> `path:` attribute. Data is migrated from source-generated CSV files.
- **DB2 for i specifics**: the DDL parser understands `FOR SYSTEM NAME`, `FOR COLUMN`, `CCSID`, `RECORD FORMAT` and `LABEL ON`. The 10-character system name of a table is registered as an alias, which is what the `{{source_alias_name}}` placeholder resolves when the unload files are named after it.
- **Migrated**: tables and data, primary keys, indexes, foreign keys with their referential rules, CHECK constraints, identity columns, sequences, aliases, views, triggers and global variables.
- **Not migrated**: functions and procedures (the routine code converter exists, but `fetch_funcproc_names` is a placeholder, so nothing is fetched), table/column comments (parsed but not surfaced — same as z/OS), and generated/computed columns.
- **Not available offline**: pre-migration analysis and all validation checks.

### 4.3 Oracle
- **Mode**: Native Connection
- **Python Module**: `oracledb`
- **Configuration**: Set `connectivity: "native"`. Configures natively via Oracle DSN strings. Supports `SYSDBA` connections when the username is `SYS`. Set `oracle_thick_mode: true` in the source configuration to enable the Oracle thick client (Instant Client) when a thin-mode connection is not sufficient.
- **Tested version**: Oracle 21.3 (see `FEATURE_MATRIX.md`).

**Current status (Oracle as source):**

- Migrated: tables and data, columns with data-type mapping, primary keys, unique/secondary indexes, foreign keys (including `ON DELETE CASCADE`), CHECK constraints, and identity columns (recognized both via sequence-based defaults and 12c+ `GENERATED ... AS IDENTITY`).
- Table and column comments (`ALL_TAB_COMMENTS` / `ALL_COL_COMMENTS`) are migrated as PostgreSQL `COMMENT ON` statements.
- Data types: broad mapping to PostgreSQL, including `BINARY_FLOAT`/`BINARY_DOUBLE`, `RAW`, `NCLOB`, `ROWID`/`UROWID`, `XMLTYPE`, `JSON`, `TIMESTAMP WITH [LOCAL] TIME ZONE` (→ `TIMESTAMPTZ`) and `INTERVAL YEAR/DAY` variants. `NUMBER(p,s)` is mapped to the most appropriate PostgreSQL numeric/integer type based on precision and scale.
- Standalone sequences: Oracle sequences (`ALL_SEQUENCES`) are migrated as independent PostgreSQL sequences (`CREATE SEQUENCE`), preserving increment, min/max, cache and cycle, and continuing from the source's current position.
- User-defined types: Oracle **object types** are migrated as PostgreSQL composite types (`CREATE TYPE ... AS (...)`); **collection types** (VARRAY / nested tables) are migrated as array-based domains (`CREATE DOMAIN ... AS <element_type>[]`).
- Domains: Oracle **23ai** SQL domains (`ALL_DOMAINS`) are migrated as PostgreSQL domains.
- Stored **functions and procedures** are converted from PL/SQL to PL/pgSQL on a best-effort basis (header/parameter/data-type conversion plus common construct rewrites); see the limitations below. **Packages** are split into standalone functions, one per package routine (a package procedure becomes a function returning `void`). `migration.packages_as` selects where they end up: `functions` (default) creates them in the target schema as `<package>_<routine>` (`pkg_audit_log_change()`), `schemas` creates a schema per package (`CREATE SCHEMA IF NOT EXISTS`) holding the routines under their own names (`pkg_audit.log_change()`). Every call into a package — in functions, procedures and triggers — is rewritten to match the chosen layout, with the `PERFORM` that PL/pgSQL requires for a call used as a statement.
- **Triggers** are converted to a PL/pgSQL trigger function plus a `CREATE TRIGGER` (`:NEW`/`:OLD`→`NEW`/`OLD`, `INSERTING`/`UPDATING`/`DELETING`→`TG_OP`, timing/events/`WHEN` preserved), also best-effort.
- Views and **materialized views** (`ALL_VIEWS` / `ALL_MVIEWS`) are migrated as PostgreSQL `CREATE VIEW` / `CREATE MATERIALIZED VIEW`. The defining query is transpiled from Oracle to PostgreSQL SQL via `sqlglot` (e.g. `NVL`→`COALESCE`, `DECODE`→`CASE`, `SYSDATE`/`SYSTIMESTAMP`→`CURRENT_TIMESTAMP`, `SUBSTR`/`INSTR`→`SUBSTRING`/`POSITION`, `MINUS`→`EXCEPT`, `REGEXP_LIKE`→`~`, `LISTAGG`→`STRING_AGG`, `seq.NEXTVAL`→`nextval('seq')`, `FROM dual` removed). Materialized view container tables are excluded from base-table migration.

**Known limitations (Oracle):**

- **PL/SQL conversion is best-effort**: functions/procedures are converted heuristically (`sqlglot` cannot parse PL/SQL bodies), so generated routines should be reviewed and tested. **Package state** (package level variables, constants and cursors) has no PostgreSQL equivalent and is **not** migrated — a package that relies on state between calls needs manual work (e.g. session settings or a table). Constructs with no automatic translation — `BULK COLLECT`, `FORALL`, `PRAGMA AUTONOMOUS_TRANSACTION`, other `DBMS_*` calls, `CONNECT BY`, `%TYPE` in a `RETURNS` clause, `SYS_REFCURSOR` returns — are logged as `WARNING`s. **Triggers** are converted best-effort (compound triggers and custom `REFERENCING` names are flagged; column-level `UPDATING('col')` loses column specificity). Enable function/procedure migration with `migration.migrate_funcprocs: true` and trigger migration with `migration.migrate_triggers: true`.
- **Standalone sequences**: migrated as independent PostgreSQL sequences. Oracle bounds that exceed PostgreSQL's `bigint` range (e.g. the default `MAXVALUE`) are dropped so PostgreSQL applies its own defaults, and the start position is captured at planning time (not re-read at migration time). Sequence-backed table columns continue to be handled separately via PostgreSQL identity columns.
- **CHECK constraints**: Oracle's internal `NOT NULL` checks are intentionally excluded (they are part of the column definition). `DISABLED` checks are still migrated as enforced constraints, and expressions using Oracle-specific functions/pseudocolumns are copied verbatim and may need manual adjustment.
- **User-defined types** conversion is best-effort: attributes that reference other object types are emitted unqualified (may not resolve on the target), and type inheritance (`UNDER`/supertypes) and VARRAY upper bounds are not modeled.
- **Domains** exist only in Oracle 23ai; on older releases (11g/12c/19c/21c) there are no domain objects to migrate, and the 23ai path is best-effort and has not been validated against a live 23ai instance.
- **Data-type coverage**: `SDO_GEOMETRY` (spatial) is not mapped and falls back to `TEXT` (it requires PostGIS on the target); `BFILE` (external file locator) is not migrated. `INTERVAL YEAR TO MONTH` is mapped to PostgreSQL `INTERVAL` but its value semantics differ, so such columns are worth verifying. Any type can still be overridden with custom data-type replacement rules.
- **Large-table extraction**: data is fetched in chunks using `OFFSET … FETCH NEXT`, which becomes less efficient at very large offsets. Keyset/ROWID-range pagination is a planned optimization.
- **Views / materialized views**: the defining query is parsed with `sqlglot` and generated as PostgreSQL, including rewriting Oracle **`(+)` outer joins** into ANSI `LEFT`/`RIGHT JOIN`s. A few constructs still cannot be auto-converted reliably and are logged as `WARNING`s for manual review: **`CONNECT BY` / `START WITH`** hierarchical queries (need a recursive CTE), **`ROWNUM`** (use `LIMIT`), and complex **`LISTAGG`** forms. An unusual `(+)` predicate that could not be mapped to a join is also warned about individually. There is no separate toggle for materialized vs. regular views (both follow `migrate_views`), and view dependency ordering is not topologically resolved.

### 4.4 PostgreSQL
- **Mode**: Native Connection
- **Python Module**: `psycopg2`
- **Configuration**: Set `connectivity: "native"`. Utilized for migrations between PostgreSQL instances.
- **Tested versions**: PostgreSQL 14, 17.

Because source and target speak the same dialect, this is the most complete connector: no type
mapping, default-value rewriting or SQL function mapping is needed, and object definitions are taken
from the catalog with `pg_get_viewdef`, `pg_get_functiondef`, `pg_get_triggerdef`,
`pg_get_constraintdef` and `pg_get_indexdef`, so they are already valid PostgreSQL.

- **Migrated**: tables (including **partitioned tables** — a partitioned parent is recreated with its `PARTITION BY` clause and each partition as `PARTITION OF ... FOR VALUES ...`), data, primary keys, indexes, foreign keys with their `ON DELETE`/`ON UPDATE` rules, CHECK constraints, identity columns, generated columns, collations, full text search dictionaries and configurations, domains, user-defined types (enums, composite types, range types), sequences (`CREATE SEQUENCE` + `setval`, continuing from the source position), views and materialized views, functions and procedures, triggers, and table/column comments.
- **Full text search**: user-defined text search dictionaries and configurations are created right after the collations and before the tables, because generated `tsvector` columns, views, indexes and functions reference them. All non-system schemas are searched (a table regularly uses a configuration created in `public`), and objects belonging to an extension — such as the `unaccent` dictionary — are skipped, since they come with the extension itself and must be listed in `migration.required_extensions` instead. A configuration is rebuilt from its parser plus one `ALTER … ADD MAPPING FOR <token type> WITH <dictionaries>` per token type read from `pg_ts_config_map`, rather than as `COPY = <other configuration>`: the configuration it was copied from need not exist in the target, and its mappings were altered afterwards anyway. Comments are transferred.
  - References to these objects need special handling. They sit **inside a string literal** (`to_tsvector('migtest_english'::regconfig, body)`), so they cannot be schema-qualified by rewriting identifiers — and the source does not hand them over qualified either: `pg_get_viewdef()` and `pg_get_expr()` print the bare name whenever the object is visible in the source `search_path`, so even an explicitly written `'public.migtest_english'` is normalized away before the migrator sees it. Such literals are therefore rewritten to `'<target_schema>.<name>'` in view bodies, generated column expressions, index expressions and partial index predicates, and function bodies. Built-in configurations (`pg_catalog.english`) and extension-owned dictionaries (`ext.unaccent`) are left untouched.
- **Generated columns**: the generation expression is read from the catalog, and `STORED` and `VIRTUAL` (PostgreSQL 18) are distinguished. A virtual column is created as `VIRTUAL` on a PostgreSQL 18 target and as `STORED` on an older one — the values are identical, it only costs storage instead of computing time — with a warning.
- **Indexes**: beside the access method (`USING gin`/`gist`/`spgist`/`hash`/`brin`), the operator class, the collation and expression keys, an index keeps its `INCLUDE` columns, `NULLS NOT DISTINCT`, `WITH (...)` storage parameters and the `WHERE` predicate of a partial index. A `TABLESPACE` clause is deliberately dropped, because the tablespace of the source need not exist in the target. An index implementing a `UNIQUE` or `EXCLUDE` constraint is created by the constraints migration from the constraint definition, not as a bare index — so a temporal `UNIQUE (room, occupied WITHOUT OVERLAPS)` and an `EXCLUDE … WHERE … DEFERRABLE` survive; primary keys stay with the indexes but are likewise built from the constraint definition.
- **Collations**: user-defined collations (`CREATE COLLATION`) are created as the very first objects of the migration, because columns, indexes and domains reference them. All non-system schemas of the source are searched, not only the migrated one — a table regularly uses a collation created in `public` — and collations are matched by name, the migrated schema winning if the same name exists twice. The ICU and libc provider, the locale (or `lc_collate` / `lc_ctype`), ICU tailoring `rules`, `deterministic = false` and the collation comment are all carried over; the recorded collation *version* is deliberately not copied, because the target can be built with a different ICU / libc version. Every collation is recreated **in the target schema**, and the references in the column and index DDL are rewritten accordingly — `pg_get_indexdef()` emits them unqualified (`COLLATE natural_numeric`), where they would be resolved through the source `search_path`. Built-in collations (`C`, `POSIX`, `en_US.utf8`) are kept untouched; if the target database cannot provide a referenced collation, the reference is dropped with a warning and the column keeps the default collation, instead of the whole table or index failing.
- **Mostly used for**: moving a schema between instances, re-homing a schema into a different schema name, and the mapping workflow.

### 4.5 Informix (JDBC example)

See more on the “Connection to Informix” wiki page:

- Install prerequisites:
  - Python library jaydebeapi (already a dependency).
  - Two JAR files, e.g. jdbc-4.50.10.1.jar and bson-3.8.0.jar.
  - Place the JARs in a shared directory (e.g. /usr/share/java).

- In the YAML config for the source database, set for this Informix connection:
  - connectivity: "jdbc"
  - Under a jdbc block:
    - driver: "com.informix.jdbc.IfxDriver"
    - libraries: a colon‑separated classpath with your JAR files, e.g.:
	/usr/share/java/jdbc-4.50.10.1.jar:/usr/share/java/bson-3.8.0.jar

Host, port, database name, and credentials are specified in other fields of the same source‑DB section (see config_sample.yaml in the repo for the exact parameter names). Informix also supports ODBC connectivity via `pyodbc`.

**File-based Import (UNL):** For environments where direct connectivity is limited, the Informix connector supports offline file-based ingest using native `.unl` export files via the `data_export` configuration.

**Current status (Informix as source):** Informix is the reference connector for procedural code — it is the only engine whose functions, procedures and triggers are converted completely rather than best-effort, and the only one whose pre-migration analysis fills all five TOP-N metrics (rows, size, columns, indexes, constraints) including foreign-key dependency ranking. Tables, data, primary keys, indexes, foreign keys, CHECK constraints and SERIAL/identity columns are migrated.

**Known limitations (Informix):** views are migrated in the rudimentary way — only the schema name inside the definition is replaced, with no SQL transpilation, so view bodies using Informix-specific syntax need review. Column defaults are passed through unchanged (no implicit rewriting of Informix-specific default expressions). Foreign-key `ON DELETE` actions, standalone sequences, user-defined types, domains, generated columns and comments are not migrated.

### 4.6 Sybase ASE (ODBC example)

See more at the “Connection to Sybase ASE” wiki page:

- Install prerequisites:
  - Python library pyodbc (already a dependency).
  - Linux libraries FreeTDS and unixODBC.

- Verify ODBC config locations:
  - odbcinst -j

- This shows which files are used for drivers and system data sources (e.g. /etc/odbcinst.ini, /etc/odbc.ini).

- Configure a FreeTDS driver for Sybase ASE in odbcinst.ini and a DSN in odbc.ini (the wiki shows example content).

- In the YAML config for the source database, set:
  - connectivity: "odbc"
  - Under an odbc block:
    - driver: "FreeTDS"

Other ODBC parameters such as DSN or connection string are configured alongside the driver (see config_sample.yaml in the repo for the exact parameter names). Sybase ASE also supports JDBC connectivity via `jaydebeapi`.

**Current status (Sybase ASE as source):** the richest connector after PostgreSQL for schema objects. Besides tables, data, primary keys, indexes, foreign keys and CHECK constraints it is the **only** connector implementing:
- **Named default objects** (`CREATE DEFAULT ... AS ...`, bound to several columns by name, note [4] in `FEATURE_MATRIX.md`). PostgreSQL has no such object, so the underlying default expression is attached directly to each target column.
- **Rules / domains** (note [3]) — externally defined checks bound to a column or data type, migrated as PostgreSQL domains or CHECK constraints depending on `migrate_domains_as`.
- User-defined types, and **hidden computed columns** that Sybase creates for function-based indexes (note [5]) — the index is rewritten to use the underlying expression instead of the internal `sybfi*` column.

Functions, procedures and triggers are converted (T-SQL → PL/pgSQL, via the shared T-SQL parser), as are views.

**Known limitations (Sybase ASE):** foreign-key `ON DELETE` actions, table/column comments and standalone sequences have no Sybase counterpart or are not migrated. Note that older ASE versions do not support `LIMIT ... OFFSET`, so the migrator always drops and reloads unfinished tables when resuming after a crash for this source (it cannot skip already-loaded rows reliably).

### 4.7 SQLite

- **Mode**: local file — there is no server, no network connection and no authentication.
- **Python Module**: `sqlite3` from the Python standard library. **No driver has to be installed**, which makes SQLite the only source engine with no external prerequisites.
- **Tested version**: SQLite 3.46 (see `FEATURE_MATRIX.md`).
- **Connectivity**: two modes,
  - `native` — the objects *and* the data are read from a SQLite database file (`database:`). This is the default when `connectivity` is left out.
  - `ddl` — the objects are read from SQL script files (`ddl: path:`), and the data usually comes from CSV files configured under `data_export`. Use it when you were given a schema dump instead of the database file.

#### 4.7.1 Native connectivity — migrating from a database file

```yaml
source:
  type: "sqlite"
  # path to the database file - absolute, or relative to the directory of this config file
  database: "/path/to/application.sqlite"
  # SQLite has no schemas - "main" (default) or the name of an attached database
  schema: "main"
  connectivity: "native"
```

Notes on the connection parameters:

- `database` is a **file path**, not a database name. A relative path is resolved against the directory holding the config file, so a config file can be moved together with the database it describes. `~` is expanded.
- `host`, `port`, `username` and `password` are **not used** and can be left out entirely.
- `schema` must be `main` (the default) or the name of a database attached to the file. Any other value — including the migrator's generic default `public` — is treated as `main`. The value is only a label; SQLite has no schemas, and the target schema is taken from the `target` section as usual.
- The file is opened **read-only** whenever possible, so a migration cannot modify the source. If the read-only open fails — which happens when the file carries a hot journal or a WAL that needs recovery — the connector logs a `WARNING` and reopens read-write, because SQLite must be able to replay the journal before it can read consistently.
- TEXT values that are not valid UTF-8 (common in old databases, since SQLite does not enforce the encoding of what is stored) are decoded with replacement characters instead of aborting the batch.

#### 4.7.2 DDL connectivity — migrating from SQL script files

When the source is a set of `.sql` scripts rather than a database file — a `sqlite3 db .schema`
or `sqlite3 db .dump` output, or hand-maintained schema scripts — set `connectivity: "ddl"`:

```yaml
source:
  type: "sqlite"
  connectivity: "ddl"
  schema: "main"
  ddl:
    # a directory, a file mask, or one specific file
    path: "/path/to/ddl/*.sql"

  # data usually comes from CSV exports; omit this block when the scripts
  # themselves contain the INSERT statements (a full .dump)
  data_export:
    format: "CSV"
    file: "/path/to/data/{{source_table_name}}.csv"
    delimiter: ","
    header: true
```

`path` accepts a directory (all files in it), a mask, or a single file, and a relative path is
resolved against the directory of the config file. Files are processed in sorted order.

**How it works.** Rather than re-implementing a SQLite parser, the connector replays the scripts
into a **staging SQLite database** and then introspects that database with exactly the same code
used for native connectivity. SQLite is the only parser that understands its own DDL completely,
so nothing is lost in translation: CHECK constraints, generated columns, `AUTOINCREMENT`,
functional and partial indexes, views and triggers are all recognized the same way as from a live
file. Consequences worth knowing:

- The staging database is created once by the planner and reused by every parallel worker. Its path
  is derived from the list of script files and is logged at `INFO`; it is placed in
  `data_export.conversion_path` when that is configured, otherwise in the system temp directory. It
  is written atomically, so workers never observe a half-built file. It is **not** deleted after the
  run — it is useful for inspecting what the scripts actually produced.
- The scripts are first executed in one go; if that fails, they are replayed **statement by
  statement** so that one bad statement does not cost you every object in the file. Skipped
  statements are counted and reported at `INFO` (`ATTENTION: n statement(s) ... were SKIPPED`), with
  each individual statement logged at `WARNING`. Because this tool ranks `WARNING` as *more* verbose
  than `INFO`, run with `--log-level=WARNING` to see them.
- **If the scripts contain `INSERT` statements** (i.e. a full `.dump`), that data lands in the
  staging database and is migrated from there — no `data_export` block is needed. When a CSV data
  source *is* configured for a table, the CSV takes precedence, exactly as for the other DDL based
  connectors.
- `database` is ignored in this mode; `host`, `port`, `username` and `password` remain unused.
- As with the other DDL based connectors, the planner skips the pre-migration analysis in this mode.

#### 4.7.3 Why SQLite needs a different approach

Two properties of SQLite shape this connector and explain most of its behavior:

**1. There is no data dictionary.** SQLite exposes columns, indexes and foreign keys through `PRAGMA` statements, but a number of things exist *only* inside the original `CREATE` statements kept in `sqlite_master`: CHECK constraints, generated-column expressions, the `AUTOINCREMENT` marker, the expressions of a functional index, and the `WHERE` condition of a partial index. The connector therefore contains a small **DDL parser** that reads those statements back. The parser is quote- and parenthesis-aware, so commas inside string defaults (`DEFAULT 'a,b'`), nested function calls and all four identifier quoting styles SQLite accepts (`"name"`, `` `name` ``, `[name]`, bare) are handled correctly, and `--` / `/* */` comments are stripped first.

**2. SQLite is dynamically typed.** A column has a *declared type* and a *type affinity*, but any row may store any storage class — a column declared `TEXT` can hold an integer, and a column declared `DATE` can hold an ISO string, a Unix timestamp or a Julian day number. The connector uses the **declared type to choose the PostgreSQL column type**, and then **coerces every value to what the target column actually expects** while the data is migrated (see 4.7.6). A column declared with no type at all is migrated as `TEXT`.

#### 4.7.4 Current status (SQLite as source)

- **Tables and data** are migrated, with the usual batching, chunking, parallel workers and resume-after-crash support.
- **Primary keys** are always taken from the column metadata, because SQLite does not create an index for an `INTEGER PRIMARY KEY` on a rowid table. Composite primary keys and `WITHOUT ROWID` tables are handled.
- **Identity columns**: an `INTEGER PRIMARY KEY` (the rowid alias, which SQLite fills in automatically) and any `AUTOINCREMENT` column are migrated as PostgreSQL identity columns (`GENERATED BY DEFAULT AS IDENTITY`). The rowid alias is only recognized for the declared type `INTEGER` on a rowid table — that is exactly the rule SQLite itself applies, so a column declared `INT` or a key on a `WITHOUT ROWID` table is correctly *not* treated as an identity. After the data load the identity is advanced past the migrated rows; the source position is read from `sqlite_sequence` when the column is `AUTOINCREMENT`, otherwise it is derived from the data.
- **Secondary indexes**, including **UNIQUE** indexes and **functional/expression indexes**. The expressions of a functional index are read from the `CREATE INDEX` statement (the PRAGMA reports `NULL` for them) and translated to PostgreSQL. Auto-generated index names (`sqlite_autoindex_<table>_<n>`, created by a `UNIQUE` table constraint) are replaced with readable `uq_<n>` names.
- **Foreign keys**, including `ON DELETE` / `ON UPDATE` actions. A short form such as `REFERENCES parent` without a column list is resolved against the parent's primary key. Note that SQLite genuinely creates two foreign keys when a table declares both a column-level `REFERENCES` and a matching table-level `FOREIGN KEY` clause; the connector reports both, faithfully reproducing the source.
- **CHECK constraints**, both table-level (named via `CONSTRAINT x CHECK (...)`) and column-level, parsed from the DDL and translated to PostgreSQL. Constraints SQLite does not name itself get a generated name that is reduced to plain characters and shortened, so it survives PostgreSQL's 63-byte identifier limit without colliding.
- **Generated columns**, both `STORED` and `VIRTUAL`. PostgreSQL only has stored generated columns, so both kinds become `GENERATED ALWAYS AS (...) STORED`. Their expressions are translated to PostgreSQL, and the columns are excluded from the data `INSERT` — PostgreSQL computes them itself and rejects supplied values. Note that PostgreSQL 12+ is required; the migrator's pre-migration capability check enforces this.
- **Default values**: literals are taken over unchanged, `CURRENT_TIMESTAMP` / `CURRENT_DATE` / `CURRENT_TIME` are preserved, the parentheses SQLite wraps expression defaults in are removed, a blob literal `X'AABB'` becomes `'\xaabb'::bytea`, and any remaining expression is translated to PostgreSQL. `NULL` defaults are dropped.
- **Views** are migrated. The defining `SELECT` is isolated from the stored `CREATE VIEW` statement and transpiled to PostgreSQL with `sqlglot` (`ifnull`→`coalesce`, `substr`→`substring`, `instr`→`strpos`, and so on). Because SQLite statements reference tables without any schema qualification, the names of migrated tables and views in the query are rewritten to `"target_schema"."name"`, so the view does not depend on the target `search_path`.
- **Triggers** are converted to a PL/pgSQL **trigger function plus a `CREATE TRIGGER`** statement. Timing (`BEFORE` / `AFTER` / `INSTEAD OF`), the event, `UPDATE OF <columns>` and the `WHEN` condition are preserved; `NEW.` / `OLD.` work unchanged in PL/pgSQL; `SELECT RAISE(ABORT, 'text')` becomes `RAISE EXCEPTION 'text'` and `RAISE(IGNORE)` becomes `RETURN NULL`. A correct `RETURN` is appended for the trigger kind (`NEW` for `BEFORE`/`INSTEAD OF` insert and update, `OLD` for delete, `NULL` for `AFTER`). The generated function carries `SET search_path = "target_schema", pg_catalog`, so the unqualified table names typical of a SQLite trigger body resolve in the migrated schema.
- **Pre-migration analysis** is fully supported for all five metrics (`by_rows`, `by_size`, `by_columns`, `by_indexes`, `by_constraints`).
- **Post-migration validation** (`--validate`) is supported: row counts, table checksums, random row sampling and LOB byte sizes.

#### 4.7.5 Data type mapping

SQLite accepts *any* declared type, so the mapping deliberately covers the type names of the dialects SQLite databases are commonly created from, not just SQLite's own five storage classes. The main entries:

| SQLite declared type | PostgreSQL type | Note |
|---|---|---|
| `INTEGER` | `BIGINT` | a SQLite `INTEGER` holds up to 8 bytes, so `BIGINT` is the lossless target |
| `INT`, `INT4`, `MEDIUMINT` | `INTEGER` | |
| `TINYINT`, `SMALLINT`, `INT2` | `SMALLINT` | |
| `BIGINT`, `INT8` | `BIGINT` | |
| `UNSIGNED BIG INT` | `NUMERIC` | PostgreSQL has no unsigned 64-bit integer |
| `TEXT`, `CLOB`, `STRING`, `*TEXT` | `TEXT` | |
| `VARCHAR(n)`, `VARYING CHARACTER(n)`, `NVARCHAR(n)` | `VARCHAR(n)` | subject to `varchar_to_text_length` |
| `CHARACTER(n)`, `CHAR(n)`, `NCHAR(n)` | `CHAR(n)` | subject to `char_to_text_length` |
| `BLOB`, `BINARY`, `VARBINARY`, `IMAGE` | `BYTEA` | |
| `REAL`, `DOUBLE`, `DOUBLE PRECISION`, `FLOAT` | `DOUBLE PRECISION` | SQLite stores all of these as an 8-byte IEEE float |
| `NUMERIC(p,s)`, `DECIMAL(p,s)`, `NUMBER`, `MONEY` | `NUMERIC(p,s)` | |
| `DATE` | `DATE` | |
| `DATETIME`, `TIMESTAMP`, `SMALLDATETIME` | `TIMESTAMP` | |
| `TIME` | `TIME` | |
| `BOOLEAN`, `BOOL`, `BIT` | `BOOLEAN` | |
| `JSON` | `JSONB` | |
| `UUID`, `GUID`, `UNIQUEIDENTIFIER` | `UUID` | |
| *(no declared type)* | `TEXT` | |

Any of these can be overridden per table/column with the usual `data_types_substitution` rules.

#### 4.7.6 Value conversion during data migration

Because the declared type is not a guarantee, values are converted on the way out based on the **target** column type:

| Target column | Conversion applied |
|---|---|
| `BOOLEAN` | `0`/`1` and the usual text forms (`t`/`true`/`yes`/`on`, `f`/`false`/`no`/`off`) become a Python `bool`; `NULL` stays `NULL` |
| `TIMESTAMP` / `DATE` / `TIME` | ISO text is passed through for PostgreSQL to parse; an `INTEGER` is interpreted as a **Unix timestamp** and a `REAL` as a **Julian day number**, both converted to a `datetime` |
| `BYTEA` | `BLOB` values are passed through as bytes; text is encoded as UTF-8 |
| `TEXT` / `VARCHAR` / `CHAR` / `JSONB` / `UUID` | non-string storage classes (e.g. an integer stored in a `TEXT` column) are stringified; a `BLOB` is decoded with replacement characters |
| numeric types | passed through unchanged; a value stored as text is handed to PostgreSQL, which casts it |

When `migration.migrate_lob_values` is `false`, `BLOB`-backed columns are migrated as `NULL`.

#### 4.7.7 Known limitations (SQLite)

- **Partial indexes lose their condition.** The migrator's internal index model carries only a column list, with no way for a source connector to supply a complete `CREATE INDEX` statement. A partial index is therefore created **over the whole table**, and a **partial `UNIQUE` index is degraded to a non-unique index** — keeping it unique would reject rows that SQLite legitimately accepted. Both cases are logged as a `WARNING`, and the original `WHERE` condition is stored in the index comment in the migration database so the index can be recreated by hand afterwards.
- **Virtual tables are skipped.** Virtual tables (FTS3/4/5, RTREE, and any other module) have no PostgreSQL equivalent and are skipped with a `WARNING`, together with their shadow tables (`<name>_data`, `<name>_idx`, …), which would otherwise be migrated as meaningless internal data. Full-text search on the target has to be rebuilt with `tsvector`/GIN indexes. Hidden columns of a virtual table are likewise not migrated.
- **Objects SQLite does not have**: there are no stored functions or procedures, no standalone sequences, no user-defined types, no domains and no named default objects. The corresponding migration steps are no-ops, and `migration.migrate_funcprocs` / `set_sequences` have no effect. Any *application-defined* SQL function (registered by the application at runtime through the SQLite API) is unknown to the database file; if such a function appears in a view, a CHECK constraint or a generated column, the generated PostgreSQL code will reference a function that does not exist on the target and must be provided manually.
- **No comments**: SQLite stores no table or column comments, so nothing is migrated.
- **`COLLATE` clauses are dropped.** SQLite's collations (`NOCASE`, `RTRIM`, `BINARY`) do not correspond to PostgreSQL collations. In particular a `NOCASE` unique index becomes case-*sensitive* on the target, which can allow rows the source would have rejected — consider a `CITEXT` column or an expression index on `lower(...)` where this matters.
- **View, trigger and expression conversion is best-effort.** Translation is done by `sqlglot` plus a SQLite-specific function mapping. Constructs without a clean equivalent should be reviewed: `strftime()` and the other date/time functions have only partial counterparts, `group_concat()` differs from `string_agg()` in its argument handling, `last_insert_rowid()` is mapped to `lastval()`, and `changes()` has no equivalent. When `sqlglot` cannot parse a fragment, the original text is kept and a `DEBUG` message is logged, so the resulting object may fail to create. Every generated statement is stored in the migration database, so failures can be inspected and fixed there.
- **Trigger bodies** are limited to what SQLite itself allows (`INSERT`/`UPDATE`/`DELETE`/`SELECT` plus `RAISE`), which maps cleanly, but a trigger whose header cannot be parsed is skipped with a `WARNING` and must be migrated manually.
- **Table sizes** are only available when the SQLite library was compiled with `SQLITE_ENABLE_DBSTAT_VTAB` (the `dbstat` virtual table). Otherwise the pre-migration analysis reports a size of `0`; row counts are always accurate, but note that they are obtained with a real `count(*)` per table, since SQLite keeps no row estimates.
- **File-based ingest**: supported through `connectivity: "ddl"` (section 4.7.2) — objects from SQL scripts, data from CSV via `data_export`, or from the scripts themselves when they are a full dump. The Informix `UNL` format and the `big_files_split` parallel chunking are not implemented for SQLite.
- **Attached databases** are not migrated automatically. Only one schema is migrated per run; to move several attached databases, run the migrator once per database.

### 4.8 MS SQL Server

- **Connectivity**: JDBC (`jaydebeapi`) or ODBC (`pyodbc`). `connection_string_options` is appended to the JDBC URL and is where `encrypt`, `trustServerCertificate`, `integratedSecurity`, `domain` and similar options go.
- **Configuration**: `system_catalog` selects the metadata source — `SYS` (the `sys.*` catalog views) or `INFORMATION_SCHEMA`.
- **Tested version**: MS SQL Server 2022.
- **Migrated**: tables and data, primary keys, indexes, foreign keys, IDENTITY columns, sequences (SQL Server 2012+ standalone sequence objects), user-defined types, aliases/synonyms, views, functions, procedures and triggers (T-SQL converted to PL/pgSQL through the shared T-SQL parser).
- **Not migrated**: CHECK constraints, foreign-key `ON DELETE` actions, table and column comments (extended properties such as `MS_Description`), computed columns and domains/rules.

### 4.9 MySQL and MariaDB

MySQL and MariaDB have **separate connectors** with almost the same behavior; the differences are noted below.

- **Connectivity**:
  - **MySQL**: Native (`mysql-connector-python`), JDBC (`jaydebeapi`) or ODBC (`pyodbc`)
  - **MariaDB**: Native (`mariadb`), JDBC (`jaydebeapi`) or ODBC (`pyodbc`). For native connectivity on Debian/Ubuntu the C development headers are required first: `sudo apt install libmariadb-dev`, then `pip install mariadb`.
- **Tested version**: MySQL 5.7. MariaDB has not yet been validated against a live instance.
- **Migrated**: tables and data, primary keys, indexes (including **function-based** indexes, whose expressions are transpiled with `sqlglot`), foreign keys, `AUTO_INCREMENT` as identity columns, generated columns (`STORED` and `VIRTUAL`), table and column comments, and views (transpiled from the MySQL dialect to PostgreSQL, with `CHARACTER SET` / `COLLATE` clauses stripped and `GROUP BY ... WITH ROLLUP` rewritten to `GROUP BY ROLLUP (...)`).
- **MariaDB only**: standalone `SEQUENCE` objects (MariaDB 10.3+) are migrated as PostgreSQL sequences. MySQL has no sequences.
- **Not migrated**: functions, procedures and triggers — the connectors contain placeholders only, so nothing is fetched or converted. CHECK constraints, foreign-key `ON DELETE` actions, user-defined types and domains are not handled either.
- **Zero dates**: MySQL/MariaDB accept `'0000-00-00'`, which PostgreSQL rejects. Three settings control this: `migration.zero_datetime_default` (what happens to such a *default*), `migration.zero_datetime_value` (what such a *value* becomes; `NULL` by default) and `migration.relax_not_null_datetime` (drop `NOT NULL` on target date/time columns so the `NULL`s can be stored). See section 5.

### 4.10 SQL Anywhere

- **Connectivity**: Native (`sqlanydb`, install separately — see section 3.1) or ODBC (`pyodbc`).
- **Tested version**: SQL Anywhere 17.
- **Migrated**: tables and data, primary keys, indexes, foreign keys, `AUTOINCREMENT` columns as identity columns, and views.
- **Not migrated**: functions, procedures and triggers (placeholders only), CHECK constraints, foreign-key `ON DELETE` actions, sequences, user-defined types, domains, generated columns and comments. This is currently the least complete of the live-connection connectors.

### 4.11 Feature status per connector

How complete each connector is differs a lot, and the summary above is deliberately short. The
authoritative, per-feature overview is `FEATURE_MATRIX.md` in the repo — it lists every connector
against every feature (identity columns, generated columns, CHECK constraints, comments, sequences,
views, routines, triggers, validation depth, …), is reconciled against the connector sources, and
carries numbered notes for the engine-specific caveats.

---

## 5. Configuration File (.yaml)

### 5.1 General characteristics

- The config file is a YAML document.
- All configuration settings are documented by example in config_sample.yaml in the repository.

The usual workflow is:
- Copy config_sample.yaml to a new file, e.g. my_migration.yaml.
- Edit what you need:
  - connection details
  - schemas / objects to include or exclude
  - type and default value mappings
  - per‑table filters.

### 5.2 Sections

- Source database settings
  - Database engine type (e.g. informix, sybase_ase, oracle, …)
  - Host, port, database name / service name
  - User and password
  - connectivity type (odbc, jdbc, or native)
  - Driver/JAR/DSN settings (see section 4).

- Target PostgreSQL settings
  - Host/port
  - Database name
  - User/password or connection string
  - Default schema for migrated objects (if applicable).

- Migration database settings
  - PostgreSQL host/port/database/user/password for protocol tables.
  - Optional: whether to reuse the target DB or a separate DB instance.

- Object selection
  - List of schemas to migrate.
  - Include / exclude lists for tables, views, sequences, functions, etc.
  - Possibly object‑type flags such as “migrate functions / triggers”.

- Data transfer options
  - Per‑table filters like WHERE conditions to restrict migrated rows (e.g. only newest data).
  - Options controlling foreign key checks (e.g. whether FK creation is delayed until after data load).
- Type mappings
  - Rules that map source data types to PostgreSQL types, possibly with conditions (e.g. by length/precision or specific schemas).
- Default value mappings
  - Rules replacing vendor‑specific default expressions with PostgreSQL equivalents (e.g. legacy date functions).

Use config_sample.yaml as the authoritative reference for the exact field names and their meanings – it is maintained along with the code and kept up to date.

### 5.3 Advanced Configuration

Beyond the basics, the configuration file supports several advanced features:

- **Environment Setting (`env_variables`)**:
  - Allows defining environment variables (e.g. `LD_LIBRARY_PATH`, `LANG`) that need to be set before the migration starts. This is useful for drivers or libraries that depend on specific environment settings.

- **Scheduled Actions (`scheduled_actions`)**:
  - You can schedule actions to `pause`, `stop`, or `continue` the migration at specific times.
  - Useful for pausing migration during business hours and resuming during maintenance windows.

- **File-based Data Source (`data_export`)**:
  - Allows using heavily optimized exported data files (like standard Informix `.unl` or DB2 z/OS `.csv` / `.sql` dumps) as the source of data instead of reading directly from the live database.
  - Useful for very large databases where parallel export/import via files is faster or when direct connectivity is limited.
  - Supports features like `big_files_split` to process large files in parallel chunks.

- **Pre-migration Analysis (`pre_migration_analysis`)**:
  - Settings to list TOP N tables by rows, size, columns, indexes, etc. to help plan the migration strategy.

---

## 6. Running the Migrator
### 6.1 Basic command

The standard invocation:

```
credativ-pg-migrator \
  --config=./my_migration.yaml \
  --log-file=./my_migration_$(date +%Y%m%d).log \
  --log-level=DEBUG
```

Parameters:
- --config
  - Path to your YAML configuration file.
- --log-file
  - Path to the log file. The log is also printed to the console by default.
- --log-level
  - Logging verbosity for the CLI output and log file. The tool supports at least:
    - INFO – high‑level progress and important messages
    - DEBUG – detailed internal operations
	- DEBUG2 – very verbose, low‑level details (may produce large logs)
	- DEBUG3 – maximum verbosity, for deep troubleshooting
    - --dry-run
      - Run the tool in dry-run mode (no changes to target).
    - --resume
      - Resume the migration process after a crash or stop (default: False = start from scratch).
    - --drop-unfinished-tables
      - Drop and recreate unfinished tables when resuming after a crash. Works only together with --resume parameter (default: False = continue with partially migrated tables without dropping them).
    - --validate
      - Run the parallel data validation verification suite post-migration, matching table geometries mathematically between the source and target endpoints.
    - --version
      - Show the version of the tool.

Start with DEBUG, should be sufficient for most use cases. Deeper levels are only needed for troubleshooting specific issues.

### 6.2 Typical migration workflow

- Prepare PostgreSQL
  - Provision a PostgreSQL instance for the target.
  - Decide whether to use the same database or a separate one as the migration database; create both if needed.

- Prepare connectivity
  - Install JDBC / ODBC / native drivers as required for the source DB.
  - Test connectivity independently (e.g. using isql for ODBC or a DB client).

- Prepare the YAML configuration
  - Start from config_sample.yaml.
  - Fill in connection details for source, target, and migration DBs.
  - Define schemas/tables to migrate.
  - Configure any necessary type/default mappings and data filters.

- Run a test migration
  - Use a non‑production target DB.
  - Run the migrator at INFO log level.
  - Inspect:
    - CLI/log output
	- the migration database protocol tables
	- the resulting PostgreSQL schema and data.

- Adjust & re‑run
  - Tweak mappings, filters, and object selection based on test results.
  - Repeat until results are acceptable.

- Run production migration
  - Schedule downtime or read‑only window on the source DB (the tool is designed for offline use).
  - Run the migrator against your production target PostgreSQL instance.
  - Validate result, then switch applications to PostgreSQL.

---

## 7. Understanding the Migration Database

The migration database is one of the key strengths of credativ‑pg‑migrator: it stores detailed protocol information for every migrated object.

- List of migrated tables, including:
  - source name and schema
  - target name and schema
  - SQL used to create the target
  - status (success, skipped, error)
  - timestamps.

- Similar tables for:
  - indexes
  - foreign keys
  - views
  - sequences
  - functions/procedures/triggers, including their original source code and generated PostgreSQL code.

This allows you to:
- Audit exactly what was migrated and how.
- Compare source vs generated PL/pgSQL for functions/procedures and triggers wherever conversion is supported (see section 8.2 for which engines those are) — this is the practical way to review the best-effort conversions before trusting them.
- Rerun or manually fix individual objects without redoing the entire migration.

Treat the migration database as read‑only metadata. You can query it freely for analysis, but avoid modifying its tables directly unless instructed by the tool’s maintainers.

---

## 8. Feature Highlights & Limitations

### 8.1 Schema & data migration

- Tables, constraints, indexes, foreign keys are migrated by every connector.
- Sequences are created for serial/identity columns. Standalone sequence *objects* are additionally migrated for Oracle, MS SQL Server, MariaDB, PostgreSQL and the three DB2 connectors; MySQL, SQLite and Sybase ASE have no such objects, and they are not implemented for Informix or SQL Anywhere.
- Sequences on the target can be set to match the highest existing values in migrated tables.
- View migration depth differs per connector: PostgreSQL takes definitions straight from the catalog, Oracle, SQLite, MySQL/MariaDB, MS SQL Server, Sybase ASE and the DB2 connectors transpile the defining query to PostgreSQL SQL (with `sqlglot` or the shared T-SQL parser), while for Informix and SQL Anywhere only the schema name inside the definition is replaced.
- Beyond the common set, some connectors reach further: PostgreSQL migrates partitioned tables, collations, full text search objects, domains and user-defined types; Sybase ASE migrates named default objects, rules/domains and user-defined types; Oracle migrates packages, object/collection types and 23ai domains.
- Not every source feature has a PostgreSQL counterpart. Where a construct cannot be reproduced exactly, the connector degrades it deliberately, logs a `WARNING`, and records what was changed in the migration database — for example SQLite partial indexes (section 4.7.7) or Oracle package state (section 4.3).
- The per-connector, per-feature status is maintained in `FEATURE_MATRIX.md`.

### 8.2 PL/SQL / procedural code

Conversion of functions, procedures and triggers is the feature that varies most between connectors:

- **Complete**: Informix — the reference implementation, converted and validated.
- **Native**: PostgreSQL — definitions come from `pg_get_functiondef` / `pg_get_triggerdef`, so only schema names are rewritten.
- **Best-effort, implemented but not fully validated**: Oracle (functions, procedures, packages split into standalone functions, triggers — section 4.3), Sybase ASE and MS SQL Server (T-SQL through the shared parser), IBM DB2 z/OS (SQL routines and triggers), IBM DB2 LUW and DB2 for i (**triggers only** — routine conversion code exists but the routines are never fetched), and SQLite (**triggers only**; it has no stored routines — section 4.7).
- **Not implemented**: MySQL, MariaDB and SQL Anywhere — these connectors contain placeholders only, so neither routines nor triggers are migrated.
- Support for other engines can be added based on real‑world migration projects.

### 8.3 Customization

The tool provides several customization layers:

- Data type mappings
  - Replace source data types with PostgreSQL-specific ones.
  - Rules can be scoped to particular schemas/tables or based on type parameters.

- Default value mappings
  - Transform vendor-specific default expressions into PostgreSQL equivalents

- Data filters
  - Per-table WHERE conditions that restrict migrated data (e.g., only “recent” rows).
  - If filters omit rows referenced by foreign keys, FK constraints may fail on the target. Dependency analysis of source model is strongly recommended.

### 8.4 Schema Mapping Workflow

The tool integrates a comprehensive Mapping Workflow managed by the central orchestrator, built to facilitate advanced data migrations with differing geometries. Features include:
- Execution of multi-metric schema matching functions and customizable data normalization rules.
- Systematic orchestration of data loads that intelligently drops and recreates target indexes, primary keys, and constraints to maximize transfer performance and mapping integrity.
- Detailed tracking of mapped objects into the migration protocols.

### 8.5 Data Anonymization Workflow

The tool includes a pluggable anonymization engine designed to obscure sensitive data during migration without modifying core ETL pipelines. It natively supports Python in-memory transformations and can offload logic directly to the PostgreSQL engine.
For full details on the available transformation methods, see the [Data Anonymization Workflow](workflow/anonymization.md) documentation.

### 8.6 Roadmap

Planned features:
- Partitioning support for target tables.
- Pre‑migration analysis to suggest partitioning strategies based on source data distribution.

---

## 9. Troubleshooting

### 9.1 Connectivity problems

Symptoms:
- The tool fails early with connection errors.
- Logs mention ODBC / JDBC driver loading problems.

Checklist:
- Test drivers independently
  - ODBC: use isql or a similar tool with the same DSN.
  - JDBC: test via another program (e.g. DBeaver) with the same driver JARs.
- Verify driver configuration
  - For Sybase ASE, double‑check FreeTDS and unixODBC configuration (odbcinst -j, odbcinst.ini, odbc.ini).
  - For Informix, ensure the JAR paths in the libraries setting are correct and readable.
  - For SQLite there is no driver at all. A failure here means the file itself: the migrator stops with `SQLite database file not found` when `database` does not point at an existing file — remember that a relative path is resolved against the directory of the **config file**, not the current working directory. A `WARNING` about opening the file read-write instead of read-only means a leftover journal/WAL had to be recovered; verify that the file is not in use by a running application. `file is not a database` means the file is not SQLite (or is encrypted — SQLCipher and other encrypted variants are not supported).
  - Also for SQLite: the valid `connectivity` values are `"native"` (a database file, the default) and `"ddl"` (SQL script files). Anything else is rejected at start-up with an explanatory message. With `"ddl"`, a missing `ddl: path:` block, a path matching no file, and scripts that produce no objects at all are each reported with their own message; if objects *are* created but some statements failed, look for `ATTENTION: n statement(s) ... were SKIPPED` in the log and re-run with `--log-level=WARNING` to see which ones.
- Check YAML formatting
  - YAML is whitespace‑sensitive; wrong indentation or quoting can cause subtle errors.
  - Validate your config with a YAML linter if you suspect a formatting issue.

### 9.2 Incomplete or incorrect schema

Symptoms:
- Some tables, views or other objects appear to be missing.
- Constraints / foreign keys were not created.

Checklist:
- Inspect the log file at DEBUG (or lower) level for detailed error messages.
- Review the object selection section of your config:
  - Are the relevant schemas included?
  - Are there object/include/exclude lists that might filter them out?
- Check the migration database tables:
  - Look for rows with error status explaining why certain objects were skipped or failed.

### 9.3 Data issues / foreign key violations

Symptoms:
- Data load fails with FK violations on the target PostgreSQL database.

Checklist:
- Review data filters for the affected tables; ensure that parent rows are not filtered out while child rows are kept.
- Try to manually repeat creation of the foreign keys on the target after data load to see if the issue persists. SQL commands can be found in the migration database protocol tables and in the log file.

