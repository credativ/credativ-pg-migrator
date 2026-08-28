# To do — what is not built yet

What remains open in `credativ-pg-migrator`, in the order it is worth doing. Every entry is a
short statement of what is *missing*; the reasoning, the measurements and the proposed design
behind each of them live in the internal design notes named in brackets — `OPEN_ISSUES`,
`PARTITIONING_STRATEGY`, `CROSS_DATABASE_REFERENCES`, `REMOTE_OBJECTS_SUBSTITUTION`,
`SYBASE_NUMERIC_TYPE_MAPPING` and `TARGET_OBJECT_OWNERSHIP` in the `development/` directory of
the repository (working notes, not part of the distribution).

What was **repaired** is not repeated here — that is what [CHANGELOG.md](../CHANGELOG.md) is for.
This page holds only what a future version still has to do.

The bands are the ones the project orders by: what a wrong outcome costs, not what it costs to
build. An entry marked **(verified)** was checked against the code of `main` on 2026-08-28; the
others come from the design notes and have **not** been re-checked - check one before starting on
it, because some of them are older than the last repairs.

| band | what it means |
|---|---|
| **P1** | decisions which shape everything below them - not code |
| **P2** | the run still cannot say what it did not do |
| **P3** | a feature is offered and is half-built |
| **P4** | breadth, hygiene, and what will bite later |
| **Docs** | what the code does and this documentation does not say |

---

## P1 — decide first

- **The deviations table and the fidelity flag.** A protocol table of everything a migration
  could not carry over exactly, a fidelity flag per migrated object, a summary which cannot
  report success over it, and a strict mode with a meaningful exit code. Most of what has been
  repaired one item at a time would have been a row in it. *(OPEN_ISSUES P4-6)*
- **Partitioning: the seven questions for the users.** Above all: should the migrator maintain
  the partitions it creates (a `future:` window) or hand the scheme over to `pg_partman`? Then:
  the default for `primary_key`, whether the analysis may read data by default, and whether a
  feasibility finding stops the run or only warns. *(PARTITIONING_STRATEGY §10)*
- **Object ownership on the target.** Whether the session `role` stays the whole mechanism, or
  `target.object_owner` with a preflight, an enforcement sweep and a closing verification is
  built. *(TARGET_OBJECT_OWNERSHIP §4, §8)*
- **What an unresolved reference to another database should do by default** — `report` (today),
  `fail`, or `strip_qualifier`. *(CROSS_DATABASE_REFERENCES §7)*
- **Whether the configuration check blocks the run.** It reports every difference from the schema
  as a warning and continues; an unknown key would have caught `partitioning` vs
  `target_partitioning` at once. *(OPEN_ISSUES P4-3)*

## P2 — the run cannot yet say what it did not do

- **Only the PostgreSQL connector reads collations and text search objects.** The other eleven
  inherit the base methods, which answer an empty dict - and an empty answer reads as *the source
  has none*. For most engines the honest answer probably **is** *absent* (there is no
  `CREATE COLLATION` in MySQL, SQL Server, Db2, Informix or SQLite), but it has to be declared
  per engine rather than assumed. *(OPEN_ISSUES P2-8, F-27 — verified)*
- **An index dropped at fetch time is not recorded** — including the index whose expression could
  not be converted. Needs the decision about what `on_error: stop` does with such an index.
  *(OPEN_ISSUES P2-8, F-25)*
- **A reference to another database is a WARNING and nothing else.** No protocol table, so the
  summary cannot count what was carried over unresolved.
  *(CROSS_DATABASE_REFERENCES §4.3)*
- **A column type which matched nothing becomes `TEXT`**, and the run barely says so: the
  fallback chain in `convert_table_columns()` writes two `INFO` lines and the three branches
  which end in `TEXT` without passing through them say nothing at all. It should be a `WARNING`
  naming the column and the type, for every connector.
  *(SYBASE_NUMERIC_TYPE_MAPPING §6.2 — verified)*
- **Ten of the twelve connectors answer nothing for the foreign-key dependency ranking**, so the
  pre-migration analysis reports no dependencies for them. Only Informix and Oracle really read
  it, and they are the model. *(OPEN_ISSUES §6.7 — verified)*

## P3 — offered and half-built

### Partitioning

- **Merge the partitioning work.** The reading of the source scheme for all twelve sources,
  `migration.source_partitioning` and the repaired `date_range` generator are on the
  `added_partitioning_20260825` branch; `main` has neither the code nor the configuration keys,
  while the changelog already announces them.
- `primary_key: extend | keep | drop` and the same for unique indexes. The analysis refuses a key
  which cannot carry the partitioning columns, which is the check and not the repair.
  *(§5.5)*
- LIST and HASH entries, the `from` / `future` window and the `partition_name` template.
  *(§6 (2), (3), (4))*
- The feasibility checks, built together with the configuration they check. *(§4.4)*
- The insert error rewritten when a row fits no partition, and the summary block. *(§5.4, §5.6)*
- The `probe` depth of the analysis, and the nullable-key finding which one source reports and
  five do not. *(§4.5, §0.10)*
- The three Db2 connectors and Informix have never been read from a live server — only from the
  DDL of the examples. Two of the six sources which *were* read that way turned up a defect no
  unit test could find. *(§0.11)*

### Query conversion

- **W2 to W12 of the warning catalogue.** W6, W7 and W12 need nothing which is not already there;
  W2, W3 and W9 need one more field carried into the name map. *(OPEN_ISSUES P3-1)*
- Phase 4 — `target_test: execute`, `incremental`, `include_warnings`, `target_test_user`, the
  pretty printer and the result comparison. Deferred until users ask. *(OPEN_ISSUES P3-6)*
- The generic rewrites of `apply_sql_functions_mapping()` do not reach MS SQL Server and Db2,
  which map through the parsed statement instead. Needs view fixtures to prove it changes
  nothing. *(OPEN_ISSUES P3-7)*
- Five of the twelve source-test mechanisms have never met a real server, and five sources have
  none unless they are configured with `jdbc`. *(OPEN_ISSUES §6.8)*

### Routines

- **MySQL and MariaDB do not convert routines at all** — `convert_funcproc_code()` is a
  placeholder which returns an empty string. It is declared as unsupported in note [10] of
  `FEATURE_MATRIX.md`, so nothing is presented as converted, but a migration which needs the
  routines has to write them by hand. *(verified)*
- **Db2 LUW and Db2 for i convert only at the text level** — a mapping of function names over the
  body, with no parsing of it. *(verified)*
- A PL/pgSQL-aware statement splitter for the four sources whose bodies still carry the column
  names of the source. *(OPEN_ISSUES §6.6)*

### References to another database

- The structured rule addressed by name parts, which replaces the deprecated
  `remote_objects_substitution` string pairs. *(REMOTE_OBJECTS_SUBSTITUTION §6.3)*
- One call site and one stage: the substitution is still applied twice for a view and at two
  different stages depending on the path. *(§6.4, D4, D6)*
- The literal-safe, name-anchored text layer — until it exists the old form still rewrites string
  literals and comments and matches substrings. *(§6.6, D1, D2)*
- The qualifier resolution for the other ten sources; today only Sybase ASE and MS SQL Server
  have it. *(CROSS_DATABASE_REFERENCES §8)*
- `exec otherdb..sp_x` and a qualified name in a routine **header** are measured as not covered.
- Tests and examples for the option as it really is written. *(§6.8, §6.9)*

### Data types

- The opt-in domain mode for unsigned integers — `migration.unsigned_types: widen | domain`.
  Today they are widened, which drops the lower bound. *(SYBASE_NUMERIC_TYPE_MAPPING §6.3)*
- **MySQL and MariaDB have the same unsigned gap**: their type mapping holds no `UNSIGNED` key at
  all, so `INT UNSIGNED` is mapped through `int` to `INTEGER` and overflows, while `COLUMN_TYPE`
  — which spells out `int unsigned` — is already read from the catalogue. *(§7 — verified)*
- The `max()` measurement for `numeric(p,0)` in the pre-migration analysis, which also puts a
  number on the two unsafe demotions applied by default for every source. *(§6.4)*

### Ownership of the migrated objects

- Everything in the proposal: `target.object_owner`, the startup preflight, the `current_user`
  assertion per connection, `CREATE SCHEMA … AUTHORIZATION`, the catalogue-driven
  `ALTER … OWNER TO` sweep and the closing verification block.
  *(TARGET_OBJECT_OWNERSHIP §4, staged in §6)*

## P4 — breadth, hygiene, and what will bite later

- `SET SESSION search_path TO <schema>` is written **unquoted** when a constraint and when a view
  is created, so a target schema which needs delimiting is folded or refused.
  *(OPEN_ISSUES P4-1 — verified)*
- The four bare name columns of the protocol tables (`index_name`, `constraint_name`,
  `trigger_name`, `default_value_name`) hold the **source** spelling next to their `target_*`
  counterparts and should be `source_*` like everything else — a mechanical sweep of ~147
  references. *(OPEN_ISSUES P4-2 — verified)*
- The session-settings plumbing: a `target_copy` connector cannot be built at all (the settings
  accessor refuses any name but `source` and `target`, which breaks the validation of the whole
  mapping workflow), and the `SET` statements are built by string interpolation instead of
  `set_config()`. *(TARGET_OBJECT_OWNERSHIP §2.8 — verified)*
- Oracle parity, long term: partition-parallel extraction, package state, virtual and invisible
  columns, NLS / `NVARCHAR2` semantics. *(OPEN_ISSUES P4-5)*
- `migrator_tables.get_records_remote_objects_substitution()` has no caller, and a test asserts
  that it has none. Remove it or give it a purpose. *(verified)*
- Re-verify the three inherited configuration items of *OPEN_ISSUES P4-4* before working on them
  — at least the `row_limit` threshold of `data_migration_limitation` and `get_top_n_tables()`
  are live in the code today.

## Docs — what the code does and this documentation does not say

The six gaps this section opened with were repaired on 2026-08-28:
[migration_tables.md](migration_tables.md) was rewritten against the catalogue in
`protocol_comments.py` (it called the mapping-workflow tables `matching_tables` /
`matching_columns`, which is not what they are named, and 23 of the 53 tables the migrator
creates were not named on the page at all - the whole validation and mapping sets among them); the
user guide gained the Sybase ASE unsigned integer mappings, the `db..table` resolution for both
Transact-SQL connectors, and what `target.settings` really decides about the **owner** of the
migrated objects; and §8.7 "Roadmap" now points here. What is left:

- **`migration_tables.md` will drift again.** Every protocol table and column already carries its
  description in `protocol_comments.py`, which the migrator writes into the database as
  `COMMENT ON`. The page should be **generated** from it, the way
  [config_reference.md](config_reference.md) is generated from the JSON schema - a second
  `tools/generate_*_docs.py`, and the hand-written page stops being a copy which can go stale.
- **`FEATURE_MATRIX.md`** was last reconciled against the connectors on 2026-08-04 and has no
  partitioning row. Everything repaired since then - the unsigned integers, the referenced tables
  of a foreign key, the routine bodies, the object validation - is outside its window.
- **Partitioning.** The user guide and the generated reference describe the partitioning of
  `main`, which is correct today: `target_partitioning` with `date_range`, where `day` is
  accepted and creates nothing. The reading of the source scheme for all twelve sources,
  `migration.source_partitioning` and the repaired generator are on the
  `added_partitioning_20260825` branch, and **`main`'s CHANGELOG already announces them** - the
  entry of 2026.08.25 describes a feature `main` does not have. Both pages, and that entry, want
  a pass when the branch merges.
