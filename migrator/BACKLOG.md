# Backlog

# added retries for drop table due to deadlocks or other errors

2025-05-02 13:17:42,519: [ERROR] An error in Orchestrator (table_worker bc89a768-dfb2-481c-84d1-bb955edbd1ad (drop table) category_names): deadlock detected
DETAIL: Process 450 waits for AccessExclusiveLock on object 31229 of class 2606 of database 30001; blocked by process 451.
Process 451 waits for AccessExclusiveLock on relation 31073 of database 30001; blocked by process 450.
HINT: See server log for query details.
