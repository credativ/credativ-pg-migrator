# credativ-pg-migrator Mapping Workflow

The **mapping** workflow is the opposite of the standard one. The standard workflow reads the
source catalog, converts every object and *creates* the target. The mapping workflow assumes the
target schema **already exists** — built by an application installer, by a different tool or by an
earlier migration — and only moves the **data** into it. It creates no table, no index, no
constraint and no sequence; its whole job is to decide **which target table belongs to which source
table, and which target column receives which source column**, and then to copy the rows.

Typical uses:

* a staged cut-over, where the application ships its own schema and only the legacy data is missing,
* reloading one schema out of a larger migration that already ran,
* a target that was normalized or renamed on the way, so the two catalogs no longer look alike.

Because nothing is created, everything depends on the **matching**. A wrong match writes data into
the wrong table or the wrong column, and no validation afterwards can undo that. The workflow is
therefore built around a mandatory report: the run refuses to start without
`mapping.report_filename` ([`main.py:114`](../../credativ_pg_migrator/main.py#L114)).

---

## 1. Overview map

* **[Phase 1: Start-up and planning](#phase-1-start-up-and-planning)**
  * [1.1 What the planner does *not* do here](#11-what-the-planner-does-not-do-here)
  * [1.2 Reading both catalogs](#12-reading-both-catalogs)
  * [1.3 Matching the tables](#13-matching-the-tables)
  * [1.4 Matching the columns](#14-matching-the-columns)
  * [1.5 Reading the target indexes, constraints and sequences](#15-reading-the-target-indexes-constraints-and-sequences)
* **[Phase 2: Orchestration](#phase-2-orchestration)**
  * [2.1 Suspending indexes and constraints](#21-suspending-indexes-and-constraints)
  * [2.2 Copying the data](#22-copying-the-data)
  * [2.3 Setting the sequences](#23-setting-the-sequences)
  * [2.4 Recreating and verifying indexes and constraints](#24-recreating-and-verifying-indexes-and-constraints)
* **[Phase 3: Report, summary, validation](#phase-3-report-summary-validation)**

Pipeline in one picture:

```
  source catalog                     target catalog (already exists)
        |                                        |
        +--------------- fetch_table_names ------+
        |                                        |
        +------------ fetch_table_columns -------+
                          |
              forced_table_mappings          (configuration wins, removed from both lists)
                          |
          Phase 1  internal mapping table    (optional, migration.internal_mappings_table)
          Phase 2  exact name / normalized name
          Phase 3  column fingerprint (Jaccard > 0.5)
                          |
                 match_columns per matched pair
                          |
        +-----------------+---------------------------+
        |                 |                           |
   mapping_tables    mapping_columns        mapping_unmatched_objects
   + tables          (+ tables.source/target_columns JSON)
                          |
             drop non-PK indexes + non-PK constraints        (suspend_indexes_constraints)
                          |
            parallel data copy, per table, per conflict action
                          |
                  set the target sequences
                          |
             recreate indexes + constraints, then verify them
                          |
              mapping report  +  summary  +  optional --validate
```

---

## Phase 1: Start-up and planning

**Implementation:** [`planner.py:create_plan()`](../../credativ_pg_migrator/planner.py#L131) →
[`planner.py:mapping_match_tables()`](../../credativ_pg_migrator/planner.py#L2409), with the
matching itself in [`connectors/match_schemas.py`](../../credativ_pg_migrator/connectors/match_schemas.py).

`create_plan()` first runs the common `pre_planning()` and `run_premigration_analysis()` steps and
then branches on `workflow`. For `mapping` the branch consists of exactly one call —
`mapping_match_tables()` — recorded in the `main` protocol table as
`Planner / Mapping workflow`.

### 1.1 What the planner does *not* do here

`pre_planning()` behaves differently for this workflow in one place: `migration.drop_schema` is
**refused**, with a warning, so a mapping run can never drop the target schema it is supposed to
load into ([`planner.py:246`](../../credativ_pg_migrator/planner.py#L246)):

```
planner: pre_planning: Migration workflow is set to 'mapping', skipping drop of target schema.
```

`CREATE SCHEMA IF NOT EXISTS <target schema>` is still executed, which is a no-op for an existing
schema.

The migrator (protocol) schema, on the other hand, **is** dropped and recreated on every run —
`create_protocol()` starts with `DROP SCHEMA ... CASCADE`
([`migrator_tables.py:610`](../../credativ_pg_migrator/migrator_tables.py#L610)). The result of a
previous mapping run is therefore gone as soon as the next one starts. Keep the report file if you
need the history.

None of the standard planning steps run: no collations, no domains, no user defined types, no
aliases, no sequences, no views, no data sources, no partitioning analysis. The `indexes`,
`constraints`, `views`, `triggers`, `funcprocs`, `domains`, `user_defined_types` and
`source_table_partitioning` protocol tables are created but stay **empty** — which has one
practical consequence, see [`merge_keep_source`](#82-the-four-conflict-actions).

### 1.2 Reading both catalogs

The planner reads, over the two live connections:

| Step | Source | Target |
|---|---|---|
| table names | `fetch_table_names(source schema)` | `fetch_table_names(target schema)` |
| columns of every table | `fetch_table_columns()` | `fetch_table_columns()` |
| number of indexes | `get_schema_indexes_count()` | `get_schema_indexes_count()` |
| number of constraints | `get_schema_constraints_count()` | `get_schema_constraints_count()` |

The four counts are written to `mapping_pre_stats` and printed by the summary as
**PRE-MAPPING OBJECT STATISTICS**. They are the yardstick the matching result is read against:
40 matched tables mean something different against 42 than against 400.
`get_schema_indexes_count()` / `get_schema_constraints_count()` return `-1` on every connector that
does not implement them (only PostgreSQL and Oracle do); `-1` is printed as `N/A`
([`database_connector.py:2031`](../../credativ_pg_migrator/database_connector.py#L2031)).

### 1.3 Matching the tables

The matching runs in four steps. A table that is matched in one step is removed from both candidate
lists, so no source table and no target table can ever be used twice.

#### Step 0 — forced mappings (configuration)

Done in the planner *before* the heuristics
([`planner.py:2505`](../../credativ_pg_migrator/planner.py#L2505)), from
`mapping.forced_table_mappings`. Two forms:

```yaml
mapping:
  forced_table_mappings:
    - source: "SYS_CONFIG"            # exact name, exactly as the catalog spells it
      target: "app_config"
    - source_regex: "^LEGACY_(.*)$"   # regex over the source names
      target: "new_\\1"               # substitution template, backreferences allowed
```

* `source` / `target` — both names must exist in their catalog, otherwise the entry is silently
  ignored and the pair falls back to the heuristics.
* `source_regex` / `target` — `target` is a **substitution template** applied with `re.sub()`, not a
  plain name. Every source table matching the regex is paired with the name the substitution
  produces, if such a target table exists.
* `match_type` becomes `Forced Exact` or `Forced Regex Sub`, and `is_forced_mapping` is `true`.
* `similarity_score` is **not** 100 for a forced pair. It is the column Jaccard of the two tables,
  computed only so that you can see in the report how well the pair actually fits. A forced pair
  with a low score is exactly the pair worth looking at.

#### Step 1 — internal mapping table

Only active when `migration.internal_mappings_table` names a table that exists **in both** schemas
with the columns `(name, table_name, column_name)`. This is meant for applications that keep their
own object dictionary. Every property name present on both sides casts a vote for the pair
(source table of the property → target table of the property); the target with the most votes wins.

* `match_type` = `Internal Mapping`, `similarity_score` = 100.
* `stats.internal_mapping` in `mapping_tables.info` records how many properties voted.
* A missing or unreadable table is not an error — it is logged at `DEBUG` and the step is skipped.
* **Note:** `internal_mappings_table` is not part of `config.schema.json`, so the startup check
  reports it as an unknown key (a warning, never fatal). It works; it is simply undocumented in the
  generated reference.

#### Step 2 — name matching

[`match_schemas.py:159`](../../credativ_pg_migrator/connectors/match_schemas.py#L159)

* **Exact Name** — the two names are equal, case-insensitively. `similarity_score` = 100.
* **Normalized Name** — the two names are equal after `table_normalization_rules` were applied to
  both. `similarity_score` = 95.

The original spelling of both catalogs is preserved for everything downstream, so an uppercase
Oracle `CUSTOMERS` matched to a lowercase PostgreSQL `customers` is still addressed with the right
identifier on each side.

#### Step 3 — column fingerprint

[`match_schemas.py:184`](../../credativ_pg_migrator/connectors/match_schemas.py#L184)

For every source table still unmatched, every target table still unmatched is scored with the
enhanced Jaccard similarity of their **column name sets** (see
[section 6.2](#62-the-column-fingerprint-jaccard)). The best target wins, but only if the score is
**above 0.5**; otherwise the source table stays unmatched.

* `match_type` = `Column Fingerprint`, `similarity_score` = `int(jaccard * 100)`, i.e. 51–100.
* The loop is greedy and works in catalog order: the first source table takes the best target it can
  find, and a later source table that would have fitted that target even better no longer gets it.
  This is the step that produces surprises — read those rows in the report first.

### 1.4 Matching the columns

For every matched pair of tables,
[`match_schemas.py:match_columns()`](../../credativ_pg_migrator/connectors/match_schemas.py#L219)
pairs the columns in two passes:

1. **Exact** — the column names are equal, case-insensitively.
2. **Fuzzy/Normalized** — the names are equal after `column_normalization_rules` were applied. The
   lookup table built from the target columns additionally contains, for every target column whose
   name starts with one of `column_prefixes_to_strip`, the normalized form **without** that prefix.
   So a target `gov_id` is found by a source `id`.

A column already used on either side is never reused. **Data types and ordinal positions play no
role in the matching** — they are recorded in `mapping_columns` for you to check, nothing more.
`match_type` is therefore always `Exact` or `Fuzzy/Normalized`.

Columns left over on either side are written to `mapping_unmatched_objects`. An unmatched **source**
column is data that will not be migrated; an unmatched **target** column is a column nothing is
written into (it keeps its default, or stays `NULL` — and if it is `NOT NULL` without a default, the
insert of that table fails).

### 1.5 Reading the target indexes, constraints and sequences

Finally the planner reads, **for every table in the target schema — not only the matched ones**:

* `fetch_mapping_target_indexes()` → `mapping_target_indexes` (name, full `CREATE INDEX` text,
  `is_primary_key`, index type),
* `fetch_mapping_target_constraints()` → `mapping_target_constraints` (name, type, definition text),
* `fetch_mapping_target_sequences()` → `mapping_target_sequences` (which sequence feeds which column,
  and whether through a `DEFAULT`, an identity column or a trigger).

If the **source** is PostgreSQL too, its sequences are read as well and matched to the target
sequences through the matched identity columns, so `source_sequence_name` says which source sequence
a target sequence corresponds to.

> **This is the scope of the index/constraint suspension.** Because the loop covers the whole target
> schema, `suspend_indexes_constraints: true` drops and recreates the indexes and constraints of
> tables the migration never touches. See [section 8.1](#81-suspending-indexes-and-constraints).

---

## Phase 2: Orchestration

**Implementation:** [`orchestrator.py:run()`](../../credativ_pg_migrator/orchestrator.py#L134).

The mapping branch is short and fixed:

```python
if get_suspend_indexes_constraints():
    mapping_drop_indexes_and_constraints()
mapping_copy_data()
if get_suspend_indexes_constraints():
    mapping_create_indexes_and_constraints()
    mapping_check_indexes_and_constraints()
generate_mapping_report(report_filename)
print_migration_summary()
```

### 2.1 Suspending indexes and constraints

See [section 8.1](#81-suspending-indexes-and-constraints).

### 2.2 Copying the data

See [section 8.2](#82-the-four-conflict-actions) and
[section 8.3](#83-how-a-table-is-copied).

### 2.3 Setting the sequences

See [section 8.4](#84-sequences).

### 2.4 Recreating and verifying indexes and constraints

See [section 8.1](#81-suspending-indexes-and-constraints).

---

## Phase 3: Report, summary, validation

* the **mapping report** (`mapping.report_filename`, mandatory) — section [9](#9-the-mapping-report),
* the **summary** printed at the end of every run — section [10](#10-the-summary),
* the optional `--validate` pass — section [11](#11-validating-a-mapping-run).

---

## 4. Running it

```bash
credativ-pg-migrator --config mapping_workflow.yaml
```

A ready-made, commented example lives in
[`docs/configs/mapping_workflow.yaml`](../configs/mapping_workflow.yaml).

Validation is a separate run over the same configuration:

```bash
credativ-pg-migrator --config mapping_workflow.yaml --validate
```

> **`--dry-run` does not protect a mapping run.** The flag is only honoured by the standard workflow
> ([`orchestrator.py:88`](../../credativ_pg_migrator/orchestrator.py#L88)). A mapping run started with
> `--dry-run` still drops the target indexes and constraints and still copies the data. To see the
> matching before any data moves, run the workflow against a **copy** of the target, read the report,
> and only then run it against the real one. This is a known gap, listed again in
> [section 12](#12-known-limitations-and-traps).

---

## 5. Configuration

### 5.1 Selecting the workflow

```yaml
workflow: "mapping"
```

Top level, not inside `migration`. The three accepted values are `standard`, `mapping` and
`anonymization` ([`config_parser.py:578`](../../credativ_pg_migrator/config_parser.py#L578)).

### 5.2 The `mapping` block

```yaml
mapping:
  report_filename: "/path/to/mapping_report.md"   # MANDATORY
  suspend_indexes_constraints: true
  data_conflict_action: "skip"
  heuristics:
    table_normalization_rules: ["lowercase", "strip_trailing_numbers"]
    column_normalization_rules: ["lowercase"]
    column_prefixes_to_strip: ["gov_", "log_"]
  forced_table_mappings: []
  forced_column_mappings: []      # read, reported, NOT applied
```

| Option | Type | Default | Meaning |
|---|---|---|---|
| `report_filename` | string | — | Where the detailed mapping report is written. **The run aborts at startup if it is missing.** Deprecated former position: `migration.mapping_report_filename`, read only when this one is absent. |
| `suspend_indexes_constraints` | boolean | `true` | Drop every non-primary-key index and non-primary-key constraint of the **whole target schema** before the copy and recreate them afterwards. |
| `data_conflict_action` | `skip` \| `replace` \| `merge_keep_target` \| `merge_keep_source` | `skip` | What to do with a target table that already holds rows. Overridable per table in `table_settings`. |
| `heuristics` | block | see below | How names are normalized before they are compared. |
| `forced_table_mappings` | list | `[]` | Table pairs stated explicitly, applied before the heuristics. |
| `forced_column_mappings` | list | `[]` | **Not implemented.** See [5.5](#55-forced_column_mappings-is-not-applied). |

Accessors: [`config_parser.py:547`](../../credativ_pg_migrator/config_parser.py#L547) and
[`config_parser.py:584-604`](../../credativ_pg_migrator/config_parser.py#L584-L604).

### 5.3 The heuristics in detail

```yaml
mapping:
  heuristics:
    table_normalization_rules:
      - "lowercase"
      - "strip_trailing_numbers"
    column_normalization_rules:
      - "lowercase"
    column_prefixes_to_strip: ["gov_", "log_"]
```

Resolution order for each of the three settings — the first one present wins:

1. `mapping.heuristics.<setting>`
2. `migration.<setting>` (legacy position: `table_normalization_rules`,
   `column_normalization_rules`, `column_prefixes`, `normalization_settings`; all of them are
   unknown keys for the schema check, so they are reported as warnings)
3. the built-in default

Built-in defaults, applied when you configure **nothing at all**:

| Setting | Default |
|---|---|
| `table_normalization_rules` | `['lowercase', 'strip_trailing_numbers']` |
| `column_normalization_rules` | `['lowercase', 'strip_trailing_numbers']` |
| `column_prefixes_to_strip` | `['gov_', 'log_']` |
| `normalization_settings` | `{}` |

> The default prefixes `gov_` and `log_` are **not neutral**. They come from the project family this
> matching engine was first written for. If your target has a column `gov_id` and your source has
> `id`, they will be matched even though you never asked for it. Set
> `column_prefixes_to_strip: []` if you do not want that.

`normalize_name()` ([`match_schemas.py:5`](../../credativ_pg_migrator/connectors/match_schemas.py#L5))
applies the rules **in the order they are listed**. The implemented rules are:

| Rule | Effect |
|---|---|
| `lowercase` | `ORDERS` → `orders` |
| `uppercase` | `orders` → `ORDERS` |
| `strip_trailing_numbers` | `invoices_2024` → `invoices`, `part3` → `part` |
| `strip_leading_numbers` | `2024_invoices` → `_invoices` … removes a leading `_\d+` or `\d+` |
| `remove_underscores` | `order_items` → `orderitems` |
| `alphanumeric_only` | keeps `[A-Za-z0-9]` only |
| `remove_vowels` | `orders` → `rdrs` |
| `strip_prefixes` | removes the prefixes listed in `normalization_settings.prefixes` |
| `strip_suffixes` | removes the suffixes listed in `normalization_settings.suffixes` |

> **Schema caveat.** `config.schema.json` only allows `lowercase` and `strip_trailing_numbers` in
> the two rule lists, and it does not know `normalization_settings` at all. The other rules **work**
> — the code implements all of them — but the startup schema check reports them as blocking errors
> (a value outside the allowed set), so a configuration using them has to be started with
> `--ignore-config-schema-errors`. `normalization_settings` under `heuristics` is only an unknown
> key, i.e. a warning, and is read normally.

### 5.4 `forced_table_mappings`

Covered in [step 0 of the matching](#step-0--forced-mappings-configuration). Use it for everything
the heuristics cannot be made to guess. It is the only reliable way to correct a table match.

### 5.5 `forced_column_mappings` is not applied

The key is parsed, echoed into the mapping report and **applied to nothing**. The column matching
never sees it. A configuration that carries entries is told so at startup
([`config_parser.py:232`](../../credativ_pg_migrator/config_parser.py#L232)):

```
config_parser: validate_config: mapping.forced_column_mappings names N column pair(s), which are
NOT applied to the column matching - they are only written into the mapping report. Correct a
column match with mapping.heuristics, or by renaming the column in the target.
```

To correct a **column** match you have exactly two options: adjust `heuristics`, or rename the
column in the target.

### 5.6 `data_conflict_action` and `table_settings`

```yaml
mapping:
  data_conflict_action: "skip"          # global default

table_settings:
  - table_name: ".*"                    # regex, matched with re.fullmatch, case-insensitive
    data_conflict_action: "skip"
  - table_name: "reference_data"
    data_conflict_action: "replace"
```

`get_mapping_data_resolution()`
([`config_parser.py:1865`](../../credativ_pg_migrator/config_parser.py#L1865)) walks
`table_settings` in order and returns the `data_conflict_action` of the **first** entry whose
`table_name` regex fully matches; otherwise the global `mapping.data_conflict_action`; otherwise
`skip`. Because the first match wins, put the specific entries **before** a catch-all `".*"`.

> **Which name the pattern is matched against differs by caller.** The data copy resolves it with the
> **target** table name ([`orchestrator.py:315`](../../credativ_pg_migrator/orchestrator.py#L315)),
> while the mapping report and the validator resolve it with the **source** table name
> ([`migrator_tables.py:4654`](../../credativ_pg_migrator/migrator_tables.py#L4654),
> [`validator.py:503`](../../credativ_pg_migrator/validator.py#L503)). For identically named tables
> that makes no difference — the match is case-insensitive. For a renamed pair
> (`SYS_CONFIG` → `app_config`) a pattern can apply to the copy but not to the report, or the other
> way round. Write patterns that cover **both** names of a renamed pair.

### 5.7 Which `migration:` options the mapping workflow actually reads

Only these:

| Option | Effect in the mapping workflow |
|---|---|
| `parallel_workers` | number of table workers in `mapping_copy_data()` |
| `batch_size` | rows per `INSERT` batch (global value only — the per-table override in `table_settings` is **not** used here) |
| `chunk_size` | chunking of huge tables (global value only, same restriction) |
| `on_error` | `stop` aborts the run on the first failed table, `continue` carries on |
| `drop_schema` | read, and explicitly **refused** with a warning |

Everything else in the `migration:` block is ignored by this workflow, including the ones the
example config lists:

| Option | Why it does nothing here |
|---|---|
| `create_tables`, `drop_tables` | the workflow never creates or drops a table |
| `truncate_tables` | only read by the standard `table_worker`. Use `data_conflict_action: replace` instead — that is the mapping equivalent |
| `migrate_data` | the mapping workflow always copies; there is no switch |
| `migrate_indexes`, `migrate_constraints`, `migrate_views`, `migrate_funcprocs`, `migrate_triggers` | no such phase exists in this workflow |
| `set_sequences` | not consulted anywhere; the target sequences are **always** set after a table is copied |
| `names_case_handling`, `data_types_substitution`, `default_values_substitution` | there is no type conversion — the target types come from the target catalog |
| `target_partitioning` | the planner records `partitioned: false` for every mapped table |

`include_tables` / `exclude_tables` are **also not applied** — see
[section 12](#12-known-limitations-and-traps).

### 5.8 `validation` and `target_copy`

```yaml
validation:
  workers: 8
  check_row_counts: true
  check_table_checksums: true
  check_random_sample: true
  check_lob_sizes: true
  random_sample_size: 1000
  report_filename: "/path/to/validator_report.md"
  target_copy:
    type: "postgresql"
    host: "localhost"
    port: 5432
    username: "postgres"
    password: "postgres"
    database: "target_db_copy"
    schema: "public"
```

`target_copy` is a **third** database: an untouched copy of the target from *before* the run. It is
only connected for the mapping workflow ([`validator.py:313`](../../credativ_pg_migrator/validator.py#L313))
and it is what makes `skip` and the two `merge_*` actions verifiable at all — see
[section 11](#11-validating-a-mapping-run).

It takes the same keys as the `target` block, `owner` and `settings` included: it is an ordinary
PostgreSQL connection, and a copy which needs a role or a `search_path` of its own to be read is
configured here. The settings of the target are **not** reused for it — it is a different
database, and a role which exists in one need not exist in the other.

---

## 6. How the matching decides

### 6.1 Normalization

Every comparison of names happens on normalized forms, never on the raw catalog spelling. Table
names use `table_normalization_rules`, column names use `column_normalization_rules`. The rules and
their order are listed in [5.3](#53-the-heuristics-in-detail).

### 6.2 The column fingerprint (Jaccard)

[`calculate_jaccard_similarity()`](../../credativ_pg_migrator/connectors/match_schemas.py#L50) builds
the **set** of normalized column names of each side and returns

```
| intersection |  /  | union |
```

Two empty tables score `1.0`; one empty side scores `0.0`.

[`calculate_enhanced_jaccard()`](../../credativ_pg_migrator/connectors/match_schemas.py#L61) adds one
retry: when the plain score is **below 0.8**, the target-side set is rebuilt with the prefix-stripped
variants of every target column starting with one of `column_prefixes_to_strip` added alongside the
originals, and the score is computed again. The recomputed value **replaces** the first one.

This one number is used in three places: as the decision in step 3 of the table matching, as the
`similarity_score` of a forced pair, and as `stats.jaccard` inside `mapping_tables.info` for every
pair however it was found.

### 6.3 Unmatched objects and the suggestions

Everything left over is written to `mapping_unmatched_objects` — unmatched tables on both sides with
their row count, and unmatched columns with the table they belong to.

For an unmatched **table**, the planner additionally computes the five most similar names on the
other side, using `difflib.SequenceMatcher` over the lowercased names, and stores them as JSON in
`info.top_5_suggestions`:

```
customers_old (name match: 86.7%, cols match: 11 [src: 13, tgt: 12])
```

`cols match` is the size of the intersection of the two normalized column-name sets, with the size of
each set beside it. These suggestions are what you turn into `forced_table_mappings` after reading
the report.

---

## 7. The migrator tables the workflow uses

All of them live in the migrator (protocol) schema — `migrator.schema` in the configuration, e.g.
`mapping_migration`. Every table carries a `COMMENT ON` description in the database itself
(`\dt+`, `\d+` in psql), written from
[`protocol_comments.py`](../../credativ_pg_migrator/protocol_comments.py#L599). The general reference
is [Migration Database Tables](../migration_tables.md).

### 7.1 Tables specific to this workflow

Created by
[`create_table_for_mapping()`](../../credativ_pg_migrator/migrator_tables.py#L638).

#### `mapping_pre_stats`
How many objects each side holds, counted before the matching.

| Column | Meaning |
|---|---|
| `side` | `source` or `target` |
| `object_type` | `tables`, `indexes`, `constraints` |
| `object_count` | the count; `-1` = the connector cannot count it (shown as `N/A`) |

#### `mapping_tables`
One row per **matched pair of tables** — the central result of the workflow.

| Column | Meaning |
|---|---|
| `source_schema_name`, `source_table_name` | the source side of the pair |
| `target_schema_name`, `target_table_name` | the target side of the pair |
| `match_type` | `Forced Exact`, `Forced Regex Sub`, `Internal Mapping`, `Exact Name`, `Normalized Name`, `Column Fingerprint` |
| `similarity_score` | 0–100. 100 = names equal or internal mapping; 95 = normalized names; 51–100 = column fingerprint; for a forced pair it is the column Jaccard, purely informational |
| `source_table_rows_all` | rows in the source table |
| `source_table_rows_limited` | rows after `data_migration_limitation` — but see the warning in [section 12](#12-known-limitations-and-traps) |
| `target_table_rows` | rows the target table held **before** the copy. Anything but 0 triggers the conflict action |
| `info` | JSON: `details`, `evidence`, `stats` (`exact_name`, `normalized_name`, `internal_mapping`, `jaccard`) |
| `is_forced_mapping` | `true` = the pair comes from the configuration, not from the matching |

#### `mapping_columns`
One row per **matched pair of columns** inside a matched pair of tables. This is what says which
target column a source value is written into.

| Column | Meaning |
|---|---|
| `source_column_name` / `target_column_name` | the pair |
| `source_ordinal_number` / `target_ordinal_number` | the positions; they need not agree |
| `source_data_type` / `target_data_type` | recorded for inspection — **not** used by the matching. A pair whose types do not fit is where the copy fails |
| `match_type` | `Exact` or `Fuzzy/Normalized` |
| `source_is_identity` / `target_is_identity` | a target identity column cannot simply be written into |

#### `mapping_unmatched_objects`
Dropped and recreated on every run. Everything the matching could not pair.

| Column | Meaning |
|---|---|
| `object_type` | `table` or `column` |
| `side` | `source` (its data is not migrated) or `target` (nothing is written into it) |
| `parent_object` | the table an unmatched column belongs to; empty for a table |
| `object_name` | the unmatched object |
| `row_count` | rows of an unmatched table — how much data is at stake |
| `info` | JSON with `top_5_suggestions` for tables |

#### `mapping_target_indexes`
Every index of the target schema, read before the copy, plus the state of the drop/recreate cycle.

| Column | Meaning |
|---|---|
| `index_name`, `index_def` | name and the full statement the index is recreated from |
| `is_primary_key` | primary-key indexes are never dropped |
| `index_type` | as the target reports it |
| `dropped` | `true` dropped for the copy, `false` the drop failed, `NULL` not attempted |
| `success` | `true` recreated, `false` the recreate failed, `NULL` not attempted (e.g. it is owned by a `UNIQUE`/`EXCLUSION` constraint) |
| `message` | the error the target answered with |

#### `mapping_target_constraints`
The same for constraints. `constraint_def` holds the definition the constraint is recreated from,
`constraint_type` is `PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY`, `CHECK` or `EXCLUSION`. Primary keys are
never dropped.

#### `mapping_target_sequences`
Every sequence of the target and what uses it.

| Column | Meaning |
|---|---|
| `sequence_schema_name`, `sequence_name` | the target sequence |
| `used_in_default` / `used_in_identity` / `used_in_trigger` | how the column reaches it |
| `trigger_name`, `column_name` | which trigger, which column |
| `source_sequence_schema_name`, `source_sequence_name` | the corresponding source sequence, filled only when the source is PostgreSQL and the identity column could be matched |

### 7.2 Shared tables the workflow writes into

| Table | What the mapping workflow puts there |
|---|---|
| `main` | `Planner / Mapping workflow`, `Orchestrator / mapping data copy`, and the run-level rows — start, duration, result |
| `migration_protocol` (`migrator.protocol_name`) | the general per-object protocol |
| `tables` | one row per matched pair, with `source_columns` / `target_columns` as JSON dictionaries keyed by the matching order. This is what the data workers read. `partitioned` is always `false` |
| `data_migration` | one row per table: rows expected, rows inserted, batch statistics, and the "skipped" rows for tables the conflict action left alone |
| `batches_stats` | per-batch read / transform / write seconds |
| `data_chunks` | only when `chunk_size` is in effect |
| `sequences` | one row per target sequence that was set after a table was copied |
| `validation*` | written by `--validate` |

The remaining protocol tables are created and stay empty — see
[1.1](#11-what-the-planner-does-not-do-here).

---

## 8. The data copy in detail

### 8.1 Suspending indexes and constraints

`suspend_indexes_constraints: true` (the default) wraps the copy in a drop/recreate cycle:

**Before the copy** —
[`mapping_drop_indexes_and_constraints()`](../../credativ_pg_migrator/orchestrator.py#L175):

1. every constraint in `mapping_target_constraints` whose type is **not** `PRIMARY KEY`:
   `ALTER TABLE ... DROP CONSTRAINT IF EXISTS ...`
2. every index in `mapping_target_indexes` that is not a primary key **and is not the index behind a
   `UNIQUE` or `EXCLUSION` constraint** (those went away with their constraint):
   `DROP INDEX IF EXISTS ...`

Each outcome is recorded in the `dropped` / `message` columns.

**After the copy** —
[`mapping_create_indexes_and_constraints()`](../../credativ_pg_migrator/orchestrator.py#L214):
the indexes are recreated from `index_def` first, then the constraints from `constraint_def`. Each
outcome lands in `success` / `message`. A constraint the target now refuses is the interesting case:
it usually means the copied data violates it.

**Then** —
[`mapping_check_indexes_and_constraints()`](../../credativ_pg_migrator/orchestrator.py#L254) reads
the target catalog again and compares it against what was recorded, so an object that silently did
not come back is named:

```
orchestrator: Target database is missing index 'idx_orders_customer' on public.orders
orchestrator: mapping_check_indexes_and_constraints: Constraints Summary: 84 checked, 0 missing.
orchestrator: mapping_check_indexes_and_constraints: Indexes Summary: 131 checked, 1 missing.
```

> **Scope warning.** Both tables were filled for **every** table of the target schema, not only for
> the matched ones (see [1.5](#15-reading-the-target-indexes-constraints-and-sequences)). With
> `suspend_indexes_constraints: true` the run therefore drops and recreates the indexes and
> constraints of tables it never writes into. On a schema shared with a running application that is
> a real outage risk. Set it to `false` if the target schema contains anything outside the migration.

### 8.2 The four conflict actions

`mapping_copy_data()` ([`orchestrator.py:299`](../../credativ_pg_migrator/orchestrator.py#L299))
decides per table, from the row counts the planner recorded:

| `source_table_rows_limited` | `target_table_rows` | What happens |
|---|---|---|
| 0 | anything | skipped, recorded as `mapped data OK (0 rows)` |
| > 0 | 0 | copied, no conflict handling needed |
| > 0 | > 0 | the conflict action decides |

| Action | Behaviour | SQL it produces |
|---|---|---|
| `skip` | the target table is left exactly as it is; nothing is copied. Recorded as `mapped data skipped (source_rows_limited=…, target_rows=…, action=skip)` | — |
| `replace` | `TRUNCATE TABLE <target>` first, then a plain insert | `INSERT INTO … VALUES (…)` |
| `merge_keep_target` | insert what is missing, keep every value the target already has | `INSERT INTO … ON CONFLICT DO NOTHING` |
| `merge_keep_source` | insert what is missing, overwrite the existing rows with the source values | `INSERT INTO … ON CONFLICT (<pk>) DO UPDATE SET col = EXCLUDED.col … WHERE <any column IS DISTINCT FROM>` |

Built in [`postgresql_connector.py:2712`](../../credativ_pg_migrator/connectors/postgresql_connector.py#L2712).
The `WHERE ... IS DISTINCT FROM ...` tail of `merge_keep_source` means an identical row is not
rewritten, which keeps the table from bloating.

> **`merge_keep_source` needs the primary key columns, and the mapping planner does not record
> them.** The list is read with `select_primary_key()`
> ([`migrator_tables.py:4394`](../../credativ_pg_migrator/migrator_tables.py#L4394)), which queries
> the `indexes` protocol table — a table the mapping planner never fills. The copy therefore fails
> with `Table <name> lacks a primary key required for 'merge_keep_source'`. Use
> `merge_keep_target` (which needs no column list — PostgreSQL infers the conflict target) or
> `replace` until this is fixed.

### 8.3 How a table is copied

[`mapping_data_worker()`](../../credativ_pg_migrator/orchestrator.py#L403), one worker per table, up
to `parallel_workers` at a time:

1. Open a private source and target connection.
2. `TRUNCATE` the target table if the action is `replace`.
3. Build the insert value list from the **target** column types recorded in `tables.target_columns`
   ([`orchestrator.py:320`](../../credativ_pg_migrator/orchestrator.py#L320)) — every placeholder is
   cast explicitly: `%s::<target type>`, `cast(%s as text)::boolean` for booleans, and
   `lo_from_bytea(0, %s::bytea)` for `oid` (large object) columns.
4. Call the source connector's `migrate_table()`, which reads in batches of `batch_size` and, when
   `chunk_size` is set, in chunks; each batch is timed and written to `batches_stats`.
5. Read the next identity value from the source **while the source is still connected**.
6. Set the sequences — see below.

`on_error: stop` aborts the whole run when a table worker returns a failure; `continue` records the
failure and carries on.

### 8.4 Sequences

After a table is copied, the worker reads the sequences the target attaches to that table and
advances each of them. `set_sequences` is **not** consulted — this always happens.

* If the table has identity columns, only the sequence of an identity column receives the source's
  next identity value; if it has none, and there is exactly one sequence, that one receives it.
* The sequence is set to the **greater** of two values: the next identity value the source reported,
  and what the data actually in the target requires. So a sequence is never left behind the data,
  whichever of the two is higher.
* If the next identity value cannot be read from the source, a `WARNING` says so and the sequences
  are set from the target data alone.
* Every sequence and the exact SQL used is written to the `sequences` protocol table, with the value
  the sequence really ended up at (`target_sequence_last_value`).

---

## 9. The mapping report

Written by
[`generate_mapping_report()`](../../credativ_pg_migrator/migrator_tables.py#L4530) to
`mapping.report_filename`, in Markdown (whatever the extension says). It is generated at the end of
the orchestration, so it shows both the plan and the outcome. Sections:

1. **Configuration Settings** — the effective `data_conflict_action` (global and every table
   override), the `heuristics` block, `forced_table_mappings` and `forced_column_mappings`, each
   echoed as YAML. This is what makes the report self-contained: it says which settings produced the
   result below it.
2. **Mapped Tables Summary** — one line per pair:

   | Source Table | Target Table | Source Rows (Start) | Target Rows (Start) | Target Rows (End) | Match Type | Similarity | Data Conflict Action |
   |---|---|---|---|---|---|---|---|

   *Target Rows (End)* comes from the `data_migration` protocol table, so the table shows what the
   copy actually did. The conflict action column also names its origin — `skip (global)` or
   `replace (table specific)`.
3. **Mapped Columns Details** — per pair of tables, the full source-column → target-column list,
   followed by **Unmapped Source Columns** and **Unmapped Target Columns** for that table. A pair
   that came from the configuration is headed `… mapped to … (FORCED)`.
4. **Unmapped Source Tables** — with the row count at stake and the five closest target names.
5. **Unmapped Target Tables** — the same, the other way round.

**Read sections 4 and 5 and every `Column Fingerprint` row of section 2 before you trust the run.**
That is where the wrong matches are.

---

## 10. The summary

`print_migration_summary()` prints two extra blocks for this workflow.

**Before the object results** — `[ PRE-MAPPING OBJECT STATISTICS ]`, from `mapping_pre_stats`:

```
[ PRE-MAPPING OBJECT STATISTICS ]
--------------------------------------------------------------------------------
Object Type              |    Source DB |    Target DB
--------------------------------------------------------------------------------
Tables                   |          214 |          207
Indexes                  |          N/A |          431
Constraints              |          N/A |          198
--------------------------------------------------------------------------------
```

**After them** — `[ MAPPING WORKFLOW RESULTS ]`
([`migrator_tables.py:5216`](../../credativ_pg_migrator/migrator_tables.py#L5216)):

```
[ MAPPING WORKFLOW RESULTS ]
--------------------------------------------------------------------------------
Mapped Tables: 203
    Explicitly Forced: 4
    Found via Exact Name: 171
    Found via Normalized Name: 21
    Found via Column Fingerprint: 7
    Found via Forced Exact: 4
Mapped Columns: 2417
    Found via Exact: 2380
    Found via Fuzzy/Normalized: 37
Target Sequences: 96
    Identity Sequences: 96
        Mapped to Source: 0

Unmapped Objects:
    Source Tables: 11
    Target Tables: 4
    Source Columns: 63
    Target Columns: 29

Detailed Mapping Report generated at: /path/to/mapping_report.md

Target Indexes: 431
    Primary Keys (kept) - btree: 203
    successfully dropped (btree): 228
    successfully recreated (btree): 227
    error recreating (btree): 1
Target Constraints: 198
    ('PRIMARY KEY',): 203
    successfully dropped (FOREIGN KEY): 142
    successfully recreated (FOREIGN KEY): 142
```

Every counter comes from a protocol table, never from a running total kept in memory. `Found via
Column Fingerprint`, the unmapped counts and any `error recreating` line are the three things to
read first.

---

## 11. Validating a mapping run

```bash
credativ-pg-migrator --config mapping_workflow.yaml --validate
```

The validator takes its table list from `mapping_tables` rather than from the standard `tables`
protocol path ([`validator.py:181`](../../credativ_pg_migrator/validator.py#L181)), so it compares
exactly the pairs the matching produced, column by column in the matched order.

Because source and target differ by design here, the comparison depends on the conflict action, and
on `validation.target_copy` — the untouched pre-run copy of the target:

| Action | Row counts | Table checksum | Random sample | LOB sizes |
|---|---|---|---|---|
| none / `replace` | source = target | source vs target | source vs target | source vs target |
| `skip` | *target copy* = target (the table must be untouched) | *target copy* vs target | *target copy* vs target | *target copy* vs target |
| `merge_keep_target`, `merge_keep_source` | `max(copy, source) ≤ target ≤ copy + source` | skipped — not defined for a merge | skipped | skipped |

Without `target_copy` there is nothing to compare a `skip`ped or merged table against, and the
validator falls back to the source comparison, which such a table is *expected* to fail. Configure
`target_copy` whenever any table can end up skipped or merged.

A checksum mismatch triggers a column-by-column comparison — hashes, null counts, empty-string
counts, min/max/avg — written to the validation protocol tables and to
`validation.report_filename`.

---

## 12. Known limitations and traps

| # | Limitation | Consequence and workaround |
|---|---|---|
| 1 | **`--dry-run` is ignored** — only the standard workflow honours it ([`orchestrator.py:88`](../../credativ_pg_migrator/orchestrator.py#L88)) | A mapping run started with `--dry-run` still drops the target indexes and still copies the data. Rehearse against a copy of the target instead. |
| 2 | **`include_tables` / `exclude_tables` are not applied** — the filters are only evaluated in `stdwf_prepare_tables()` ([`planner.py:693`](../../credativ_pg_migrator/planner.py#L693)); `mapping_match_tables()` never calls them | Every table of the source schema takes part in the matching and every matched table is copied. To leave a table out, set `data_conflict_action` for it and make sure the target is not empty, or run against a source view/schema that does not contain it. |
| 3 | **`data_migration_limitation` is not applied to the copy** — the planner uses it to compute `source_table_rows_limited`, but the data worker passes `migration_limitation: None` ([`orchestrator.py:437`](../../credativ_pg_migrator/orchestrator.py#L437)) | The `WHERE` clause changes the *reported* row counts but not what is copied — **all** rows are moved. Do not rely on it to load a subset in this workflow. |
| 4 | **`merge_keep_source` fails for lack of a primary key** — see [8.2](#82-the-four-conflict-actions) | Use `merge_keep_target` or `replace`. |
| 5 | **`forced_column_mappings` is not applied** — see [5.5](#55-forced_column_mappings-is-not-applied) | Adjust `heuristics` or rename the column in the target. |
| 6 | **The index/constraint suspension covers the whole target schema**, not only the matched tables — see [8.1](#81-suspending-indexes-and-constraints) | Set `suspend_indexes_constraints: false` when the target schema holds objects outside the migration. |
| 7 | **Normalization rules beyond `lowercase` and `strip_trailing_numbers` fail the schema check** although the code implements them — see [5.3](#53-the-heuristics-in-detail) | Start with `--ignore-config-schema-errors`. |
| 8 | **The default `column_prefixes_to_strip` is `["gov_", "log_"]`** even when you configure no heuristics at all | Set it to `[]` explicitly if you do not want prefix stripping. |
| 9 | **Per-table `batch_size` / `chunk_size` from `table_settings` are ignored**; the global values are used | Tune globally. |
| 10 | **The protocol schema is dropped at the start of every run** | Archive the mapping report; the previous run's `mapping_*` tables are gone. |
| 11 | **`table_settings.table_name` is matched against the target name during the copy but against the source name in the report and the validator** — see [5.6](#56-data_conflict_action-and-table_settings) | Write patterns that match both names of a renamed pair. |
| 12 | **`--resume` skips the planner entirely**, so a resumed mapping run works from the row counts of the crashed run | Prefer a fresh run with a deliberate `data_conflict_action` over `--resume` here. |

---

## 13. Checklist for a mapping run

1. Point `migrator.schema` at a schema of its own — never `public`; it is dropped on every run.
2. Set `mapping.report_filename` to a real path. Without it the run refuses to start.
3. Leave `create_tables`, `drop_tables`, `drop_schema` at `false`.
4. Decide `data_conflict_action` deliberately, globally and per table. `skip` is the safe default;
   `replace` is the destructive one.
5. Set `column_prefixes_to_strip` explicitly — `[]` if you want no prefix stripping.
6. Run against a **copy** of the target first, and read the report:
   * every `Column Fingerprint` row,
   * every unmapped source table with a row count > 0,
   * every unmapped target column that is `NOT NULL` without a default.
7. Turn what the report got wrong into `forced_table_mappings`; correct column matches through
   `heuristics` or by renaming in the target.
8. Decide `suspend_indexes_constraints` from whether the target schema holds anything outside the
   migration.
9. Run for real, then read the summary: `error recreating`, `Unmapped Objects`, and the missing
   index/constraint warnings.
10. Take a copy of the target *before* the run and configure it as `validation.target_copy`, then
    run `--validate`.

---

## See also

* [Standard Workflow](standard.md) — the workflow that builds the target from the source
* [Anonymization Workflow](anonymization.md)
* [Migration Database Tables](../migration_tables.md) — all protocol tables
* [Configuration Reference](../config_reference.md) — every option, generated from the schema
* [`docs/configs/mapping_workflow.yaml`](../configs/mapping_workflow.yaml) — a runnable example
