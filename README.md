# credativ-pg-migrator

<img src="https://raw.githubusercontent.com/credativ/credativ-pg-migrator/main/docs/images/credativ-pg-migrator.png" alt="credativ-pg-migrator Logo" width="200"/>

`credativ-pg-migrator` is a solution for the migration of databases from legacy systems to PostgreSQL.
It is is written in Python in multiple classes and modules.

## Main features

- Pure python solution, uses only standard libraries, structured in modules, written in object-oriented style in classes
- Currently supported source databases are:
  - IBM DB2 LUW (live connection), DB2 z/OS and DB2 i (offline, from DDL + CSV extracts)
  - Informix
  - MS SQL Server
  - MySQL and MariaDB (engines with INFORMATION_SCHEMA; separate connectors)
  - Oracle
  - PostgreSQL (mainly for special use cases)
  - SQL Anywhere
  - SQLite (a plain local file - the only source engine needing no driver installation)
  - Sybase ASE
- Supports migration of tables, column constraints and defaults, data, primary keys, secondary indexes, foreign keys, functions/procedures, triggers and views from source to target database
- If tables have sequences, migrator sets current values of sequences on the target database. Standalone sequence objects are additionally migrated for Oracle, MS SQL Server, MariaDB, PostgreSQL and the DB2 connectors.
- Migration of views is in a rudimentary version for Informix and SQL Anywhere, just replacing source schema names in code with target schema names. The other connectors transpile the view query into PostgreSQL SQL (using `sqlglot` or the shared T-SQL parser).
- Conversion and migration of functions, procedures and triggers fully works for Informix. It is best-effort for Oracle, Sybase ASE, MS SQL Server and DB2 z/OS, triggers-only for DB2 LUW, DB2 i and SQLite, and not implemented for MySQL, MariaDB and SQL Anywhere. Can be added on demand for other databases too.
- **How complete each connector is differs considerably - see [FEATURE_MATRIX.md](FEATURE_MATRIX.md) for the per-connector, per-feature status.**
- Migrator allows customizable substitutions of data types, default values of columns, calls of remote objects.
- Supports offline, file-based data ingestion for restricted environments (e.g., using offline DDL SQL scripts for schema discovery and CSV files for data migrations from IBM DB2 z/OS, or proactively processing Informix `.unl` export files).
- User can also define limitations for migration of data - as where conditions for tables. This option requires good analysis of dependencies in the source database. Missing data can break Foreign Key constraints in the target database. See further in the documentation.
- Migrator features an advanced Mapping Workflow directed by the central orchestrator, enabling complex schema matching, application of customizable normalization rules, and automated dropping and recreating of target indexes and constraints during mapped data migrations.
- Migrator supports strict post-migration verification utilizing discrete parallel tests measuring row counts, table checksums, random row hashes, and explicit byte-size telemetry. The enhanced validation engine includes detailed validation summaries, structured error handling, and built-in log size monitoring. Row counts and table checksums work for every live-connection source; the random-sample and LOB-size checks currently require Oracle, PostgreSQL or SQLite as the source.
- Migrator provides reach logging and error handling, has 2 levels of logging - INFO and DEBUG, in case of error, detailed error message is printed.
- By default logging messages are printed both to console and to log file, name of the log file is configurable in command line arguments.
- Rich information is also logged to the migration database - see below.

## Documentation

Detailed documentation and technical insights are available in the `docs/` directory:
- [User Guide & Connectivity Options](docs/README.md) - per-connector status, connectivity and limitations
- [Feature Matrix](FEATURE_MATRIX.md) - which feature is supported by which connector
- [Standard Migration Workflow](docs/workflow/standard/migration_workflow.md)
- [Configuration Parameters Map](docs/config_parameters_map.md)

## Installation

Via PyPI:

`credativ-pg-migrator` is available from PyPI: <https://pypi.org/project/credativ-pg-migrator/>

```
python3 -m venv migrator_venv
. ./migrator_venv/bin/activate
pip install credativ-pg-migrator
```

Via Debian/Ubuntu packages:

`credativ-pg-migrator` is included in `apt.postgresql.org` PostgreSQL community repository, see <https://wiki.postgresql.org/wiki/Apt> for details.

## Usage

```
credativ-pg-migrator --config=./test_database.yaml --log-file=./test_database_$(date +%Y%m%d).log --log-level=INFO
```

## Configuration file

- Configuration file is a YAML file
- Settings available in the config file are described in [config_sample.yaml](./config_sample.yaml) file

## Architecture

![Architecture](https://raw.githubusercontent.com/credativ/credativ-pg-migrator/main/docs/images/architecture.jpg)

## Source databases

- Source database connector must implement DatabaseConnector class from the [credativ_pg_migrator.database_connector](./credativ_pg_migrator/database_connector.py) module
- Methods of this class contain descriptions of actions and return values that are expected by the migrator tool
- Supported databases should be accessed via ODBC, JDBC or native python libraries. Not all options are available for all databases.
- See feature matrix in [FEATURE MATRIX](./FEATURE_MATRIX.md) for supported features in different database connectors.

## Target databases

- Target database connector must implement DatabaseConnector class from the migrator.database_connector module
- Solution currently supports only PostgreSQL as target database

## Migration database

- Solution uses a migration database to store metadata about the migration process
- Migration database is a PostgreSQL database, credentials must be configured in the configuration file
- In most cases we assume that the migration database will the same as the target database, but it is fully possible to use a different database from the same or different PostgreSQL server
- Migration protocol tables contain detailed information about all migrated objects, like source code, target code, success or failure of migration, etc.

## Changes

See [CHANGELOG](./CHANGELOG.md).

## Authors

`credativ-pg-migrator` has been primarily developped and is maintained by Josef Machytka, see [AUTHORS](AUTHORS.md).

## License

`credativ-pg-migrator` is released under the GNU General Public License, version 3 (or any later version).
See the file [COPYING](./COPYING) in the distribution for details.
