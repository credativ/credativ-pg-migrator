# Known Issues in credativ-pg-migrator

## Foreign Key Constraint fails due to missing unique index

- If migrator tries to create a foreign key on a column that does not have a unique index or is not a primary key, the attempt will fail

```
2025-04-10 19:29:28,776: [DEBUG] Worker 71e0f61f-6b44-40fa-9431-b61a9ef1535a: Creating constraint with SQL: ALTER TABLE "cubi_cdr_migration"."rel_extsys_relation" ADD CONSTRAINT "fk_rel_extsys_relation02" FOREIGN KEY ("rel_customer_cdr_id") REFERENCES "cubi_cdr_migration"."extsys_relation" ("customer_cdr_id");
2025-04-10 19:29:30,045: [ERROR] An error in Orchestrator (constraint_worker 71e0f61f-6b44-40fa-9431-b61a9ef1535a fk_rel_extsys_relation02): there is no unique constraint matching given keys for referenced table "extsys_relation"

```

## Deadlock in creation of constraints

- If data model contains multiple foreign key constraints which all refer to the same table, deadlocks can occur when creating these constraints in parallel.

```
2025-06-17 07:38:06,284: [DEBUG] Worker a09ba00b-16ef-413f-8cd8-5d4c02198b16: Creating constraint with SQL: ALTER TABLE "public"."staff" ADD CONSTRAINT "fk_staff_store_tab_staff" FOREIGN KEY (store_id) REFERENCES "public"."store" (store_id)
2025-06-17 07:38:07,317: [ERROR] An error in Orchestrator (constraint_worker a09ba00b-16ef-413f-8cd8-5d4c02198b16 fk_staff_store): deadlock detected
DETAIL:  Process 242 waits for ShareRowExclusiveLock on relation 56702 of database 56412; blocked by process 243.
Process 243 waits for ShareRowExclusiveLock on relation 56691 of database 56412; blocked by process 242.
HINT:  See server log for query details.

2025-06-17 07:38:07,319: [ERROR] Traceback (most recent call last):
  File "/home/josef/github.com/credativ/credativ-pg-migrator/credativ_pg_migrator/orchestrator.py", line 555, in constraint_worker
    worker_target_connection.execute_query(create_constraint_sql)
  File "/home/josef/github.com/credativ/credativ-pg-migrator/credativ_pg_migrator/connectors/postgresql_connector.py", line 604, in execute_query
    cursor.execute(query, params)
psycopg2.errors.DeadlockDetected: deadlock detected
DETAIL:  Process 242 waits for ShareRowExclusiveLock on relation 56702 of database 56412; blocked by process 243.
Process 243 waits for ShareRowExclusiveLock on relation 56691 of database 56412; blocked by process 242.
HINT:  See server log for query details.
```

## Other

- In Informix we currently do not support ROW data type (corresponds with PostgreSQL composite type)
- Not all types of default values are currently supported simply because we do not know all possible values/functions. Every database is somehow unique, so these will be added on demand.
- Support for partitioning of target tables is only in the experimental stage.
