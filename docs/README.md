# credativ‑pg‑migrator – User Guide

## 1. What is credativ‑pg‑migrator?

credativ-pg-migrator is an offline migration tool for moving schemas and data from legacy or proprietary databases into PostgreSQL. It is written in Python and uses modular connectors for different source databases.

Supported source databases:
- IBM DB2 LUW
- Informix
- MS SQL Server
- MySQL / MariaDB (engines with INFORMATION_SCHEMA)
- Oracle
- PostgreSQL (special use cases)
- SQL Anywhere
- Sybase ASE 

Target database: PostgreSQL only.

High‑level features:

- Migrates:
  - tables & data
  - primary keys, unique constraints
  - defaults & check constraints
  - secondary indexes
  - foreign keys
  - functions / procedures (fully implemented for Informix)
  - triggers (Informix, Sybase ASE)
  - views (currently simple parsing and schema-name replacement)
  - Adjusts sequences on the target to match the imported data.
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

### 2.3 Databases involved

There are three logical databases in a migration:

- Source database
  - Any of the supported engines listed above.
  - Accessed via ODBC, JDBC, or native Python drivers (depending on connector).

- Target database
  - A PostgreSQL instance where your migrated schema and data are created.

- Migration database
  - A PostgreSQL database that stores:
    - migration protocol tables
    - original source code (e.g. procedures)
    - generated PostgreSQL code
    - success/failure indicators and timestamps for each migrated object
  - Often this is the same database as the target (same cluster), but it can also be a separate PostgreSQL database.

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

The tool supports connecting to source databases using:
- ODBC (via pyodbc and system ODBC drivers)
- JDBC (via jaydebeapi and JDBC .jar files)
- Native Python drivers (where available)

Which option is used is controlled in the YAML config for that source. At minimum specify:
- connectivity: "odbc", "jdbc", or a connector-specific keyword
- A subsection with driver configuration (e.g. odbc: or jdbc:).

### 4.2 Informix (JDBC example)

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

Host, port, database name, and credentials are specified in other fields of the same source‑DB section (see config_sample.yaml in the repo for the exact parameter names).

### 4.3 Sybase ASE (ODBC example)

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

Other ODBC parameters such as DSN or connection string are configured alongside the driver (see config_sample.yaml in the repo for the exact parameter names).

### 4.4 Other databases

Other supported engines (Oracle, DB2 LUW, MS SQL Server, MySQL/MariaDB, SQL Anywhere, PostgreSQL) are accessed via their respective connectors using ODBC, JDBC, or native Python drivers. The exact features supported per connector (e.g. whether stored procedures or triggers are handled) are summarized in FEATURE_MATRIX.md in the repo.

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
- Compare source vs generated PL/pgSQL for functions/procedures where conversion is supported (currently Informix only).
- Rerun or manually fix individual objects without redoing the entire migration.

Treat the migration database as read‑only metadata. You can query it freely for analysis, but avoid modifying its tables directly unless instructed by the tool’s maintainers.

---

## 8. Feature Highlights & Limitations

### 8.1 Schema & data migration

- Tables, constraints, indexes, foreign keys are migrated.
- Sequences are created for serial/identity columns.
- Sequences on the target can be set to match the highest existing values in migrated tables.
- Views are migrated in a rudimentary way: the tool makes basic parsing and replaces schema names inside the view definition; deeper semantic rewrites are not yet implemented.

### 8.2 PL/SQL / procedural code

- Complete conversion and migration of functions, procedures, and triggers is currently implemented for Informix.
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

### 8.4 Roadmap

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

