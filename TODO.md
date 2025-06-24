- Add check if necessary libraries for native connectivity are installed and raise error if not - do not allow code to fail with cryptic error messages

- comments on all objects

- SAP HANA ???

- dlt library ???

  - https://dlthub.com/docs/pipelines/sql_database_oracle/load-data-with-python-from-sql_database_oracle-to-duckdb
  - https://dlthub.com/docs/intro

- From online tests:
  - time stats of reads and writes for batches -> slowest vs fastest for both for each table
  - repeated attempts for locked tables
  - debug messages into table workers futures handling to see how many are running at the same time, when new is started, etc.
  - recognize long time delays in the migrator run - minutes of delays can be easily lost in the log file -> migrator must be able to report it
