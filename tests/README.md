# credativ-pg-migrator — test suite

462 test functions in 32 files. **No test in this directory needs a database, a driver
connection, or a network.** Everything that would talk to a server is either constructed
with `Class.__new__(Class)` and fed a fake config, or replaced with `unittest.mock`. A
full run touches nothing outside the repository and finishes in seconds.

Two frameworks are in use. The newer files are `pytest` (fixtures, `parametrize`); the
older ones are `unittest.TestCase` with a `__main__` block. `pytest` runs both, so it is
the recommended runner.

---

## Running the tests

```bash
# everything (from the repository root)
python3 -m pytest tests/ -q

# one file, verbose
python3 -m pytest tests/test_object_filters.py -v

# one test
python3 -m pytest tests/test_logging_levels.py::test_the_default_level_shows_warnings -v

# a unittest file without pytest
python3 tests/test_pg_collations.py
```

**Expected result: everything passes.** There are no known-failing, skipped or flaky
tests, and no test depends on the order of the others.

### What has to be installed

| group | needs | files |
|---|---|---|
| configuration & logging | `pyyaml`, `jsonschema`, `pytest` | the 5 `test_config_*` / `test_logging_*` / `test_object_*` files, and `test_schema_names.py` |
| query conversion | `sqlglot`, `pytest` | the 4 `test_query_*` files and `test_db2_query_conversion.py` |
| anonymization, Db2 CSV | nothing beyond the standard library | 3 files |
| everything else | the package's own dependencies — `psycopg2`, `tabulate`, `jaydebeapi` | 18 files, `test_informix_query_conversion.py` among them |

The third group imports the connectors, which import their drivers at module level, so
those files fail to **collect** if the drivers are absent — the message is
`ModuleNotFoundError`, not a test failure. `pip install -r requirements.txt` fixes it.

To run only the part that needs nothing but `pytest`, `pyyaml` and `jsonschema`:

```bash
python3 -m pytest tests/ -q \
  --ignore=tests/test_charset_collate_stripping.py --ignore=tests/test_extension_check.py \
  --ignore=tests/test_mysql_default_values.py \
  --ignore=tests/test_mysql_fetch_indexes.py     --ignore=tests/test_mysql_spatial_point.py \
  --ignore=tests/test_mysql_zero_datetime_default.py --ignore=tests/test_pg_aggregates.py \
  --ignore=tests/test_pg_collations.py           --ignore=tests/test_pg_extensions.py \
  --ignore=tests/test_pg_lob_worker.py           --ignore=tests/test_pg_udt_ordering.py \
  --ignore=tests/test_sybase_rowcount_return.py  --ignore=tests/test_pg_session_settings.py \
  --ignore=tests/test_data_migration_limitation.py --ignore=tests/test_varchar_to_text.py \
  --ignore=tests/test_default_value_substitution_patterns.py \
  --ignore=tests/test_sequence_protocol_columns.py
```

**Expected result: `545 passed, 6 skipped`** (13 files; the count is higher than the number
of test functions because many are `parametrize`d. The 6 skipped are the ones in
`test_schema_names.py` which build `MigratorTables`, and that module imports `psycopg2`).

---

## 1. Configuration language

These six check the configuration language itself against
`credativ_pg_migrator/config.schema.json`, which is the single source of truth: the
migrator validates your configuration against it at startup and
`docs/config_reference.md` is generated from it. They exist so that the schema, the
generated reference and the code cannot drift apart. Each of them found a real defect the
first time it ran.

### `test_config_docs.py` — 11 functions, 91 tests

**Purpose.** The schema is well formed and describes the same configuration language the
code reads, and the checked-in reference is what the generator produces.

**Covers.** The schema is a valid Draft 2020-12 document; every property has a
description; every file in `docs/configs/` loads under `ruamel.yaml` in duplicate-strict
mode (PyYAML silently keeps the last of a duplicated key — that is how three export-format
examples once merged into one nonsensical block), matches the schema, has no key left
empty outside a small allow-list, uses only the three placeholders that are really
substituted (`{{source_schema_name}}`, `{{source_table_name}}`, `{{source_alias_name}}`),
and gives the right number of elements to the fixed-arity lists
(`data_types_substitution` 5, `default_values_substitution` 4,
`remote_objects_substitution` 2, `data_migration_limitation` 3). Then both directions
between schema and code: no schema key that no code reads (unless marked
`"x-implemented": false`), and no key `config_parser` reads that the schema omits.
Finally `tools/generate_config_docs.py --check` and the internal links of the reference.

**Expected result.** All pass. A failure means one of: the schema is malformed, a sample
config drifted, the reference was not regenerated after a schema change, or an option was
added to the code without being documented.

### `test_config_schema_validation.py` — 19 functions, 64 tests

**Purpose.** Startup validation behaves as designed: a setting the migrator cannot carry
out stops the run; an unknown key does not.

**Covers.** Six kinds of unusable setting (value outside the allowed set, wrong type for a
block and for a scalar, missing required block, wrong list arity) each raise `ValueError`
and name the offending setting by its path; the message says `--ignore-config-schema-errors`
runs anyway; that flag downgrades the errors to warnings; a caller that does not know the
flag still gets the blocking behaviour; the CLI registers it. Unknown keys — at the top
level, inside `migration`, inside a nested block — are reported as warnings and do **not**
stop the run, so a configuration written for a later version stays usable. Every file in
`docs/configs/` is parametrised in and must pass the blocking validation. Finally the
schema-versus-code enum checks: the schema accepts every value the code accepts, it names
the same standard values, no value is both standard and alias, every alias points at a
real standard value, an alias is read as its standard value, and a wrong value is told
which standard values exist instead of the bare `is not valid under any of the given schemas`.

**Expected result.** All pass. A failure in the enum group means the code started
accepting a spelling the schema refuses — a blocking validator that is stricter than the
code would reject configurations that work.

### `test_config_defaults.py` — 5 functions, 68 tests

**Purpose.** The default documented in the schema is the default the code applies.

**Covers.** A configuration containing nothing but what the schema requires is built, so
every optional key really is absent, and each documented default is compared with what
its accessor returns. The comparison is of the **effective** default: several accessors
pass `None` into `.get()` as a sentinel and resolve the real default afterwards
(`pattern_syntax`, `varchar_to_text_length`, `char_to_text_length`), so reading the
literal out of the source would report differences that do not exist. Two tables drive
it — `DEFAULT_READERS` (64 of the 95 documented defaults, checked against their accessor)
and `UNMAPPED_DEFAULTS` (31, each with the reason no accessor applies it). A default in
neither table fails the suite, so one added to the schema must be checked or consciously
written off; a stale entry fails too; and the unmapped list must stay the minority.

**Expected result.** All pass. Adding an option with a `default` to the schema will fail
this file until you add it to one of the two tables — that is the point.

### `test_object_filters.py` — 19 functions, 67 tests

**Purpose.** `include_tables` / `exclude_tables` and the same pairs for views and
functions/procedures behave **identically**, and `pattern_syntax` is honoured.

**Covers.** Every semantic assertion is parametrised over all three object kinds, so
"they behave the same" is enforced rather than claimed: absent, `all`, `[]`, null and a
match-everything pattern all select everything (an empty include list used to skip every
view and every routine, silently); an absent exclude removes nothing; exclude wins over
include; `exclude_*: ['.*']` really excludes everything (as a glob it used to exclude
nothing); matching ignores case and must match the whole name; a bare string other than
`all` is refused rather than read as a one-element list. Then `pattern_syntax`: `glob` is
the default, the aliases resolve, and `glob` / `regex` / `like` each have their own
wildcard semantics — including that the same text means different things in different
syntaxes (`log_.*` excludes `log_2024` as a regex and nothing at all as a glob). Finally
the refusals (an uncompilable pattern, an unknown syntax, a scalar) and the advisory that
reports a pattern written in the wrong syntax.

**Expected result.** All pass.

### `test_logging_levels.py` — 14 functions, 28 tests

**Purpose.** `--log-level` is a severity threshold, so warnings are visible by default.

**Covers.** The exact set of levels written at each `--log-level`; that raising the level
may only ever *add* messages; that `ERROR` is shown at every level; that each level
reaches the matching `logger` method (a warning must reach `logger.warning`, so the log
renders `[WARNING]`); that an `INFO` message is not prefixed with its own level; that
`DEBUG2` / `DEBUG3` mark themselves; case-insensitivity of the level name; refusal of an
unknown message level; and that an unusable `--log-level` falls back to `INFO` instead of
silencing the run.

**Expected result.** All pass. The first test is the regression itself — until 0.16.1 the
levels were compared as positions in a list beginning with `INFO`, so the default level
showed `INFO` alone and every `WARNING` message in the migrator — around two hundred of
them — was invisible.

### `test_schema_names.py` — 8 functions, 16 tests

**Purpose.** The two schema names the migrator cannot work without: `migrator -> schema`
and `target -> schema`.

**Covers.** `public` as the schema of the migrator metadata stops the run, in any spelling
— that schema is dropped with everything in it at the start of every run, so the name
would take the public schema of the database with it. An empty name stops the run on both
sides. `public` as the *target* schema is allowed, and a name which merely contains the
word (`public_migration`) is too. The same two refusals are asserted a second time against
`MigratorTables`, which checks them itself before it opens its connection — that is the
gate directly in front of the `DROP SCHEMA`.

**Expected result.** All pass; 6 are skipped when `psycopg2` is not installed
(`MigratorTables` imports it).

---

## 2. Anonymization

### `test_anonymization_unknown_method.py` — 9 tests

**Purpose.** An unknown anonymization method must never end as a silently skipped
column — the original personal data would be copied to a target everybody can read.

**Covers.** An unknown method in a table rule and in a `regex_mappings` entry, a missing
method and an invalid regex are each fatal at startup; all problems are reported at once
rather than one per run; a valid configuration is accepted and its rules counted; during
the copy, a method that cannot be resolved raises instead of skipping; values really are
replaced and counted; `rules_for_columns` reports the matching rules.

**Expected result.** All pass. No standard-library dependency.

### `test_anonymization_value_too_long.py` — 13 tests

**Purpose.** A masked value that does not fit the target column is never cut without a
trace.

**Covers.** The default policy is `error`; an unknown policy and an invalid attempt count
are fatal; `error` raises both for an anonymized column and for one copied unchanged;
`fit` cuts and counts; `find_fitting_value` repeats until the value fits, and raises when
the method is deterministic, when the configured number of attempts is exhausted, and for
a column without a rule; values that fit are untouched; a `__RAW_SQL__:` value (a function
call for the server, not the data) and a NULL are not measured.

**Expected result.** All pass.

---

## 3. Source connectors

All of these construct the connector with `__new__` and feed it a fake config or a
`MagicMock`, then assert on the SQL or the converted code that comes out. None connects
anywhere.

### `test_db2_csv_temporal_values.py` — 16 tests

**Purpose.** A date, time or timestamp in a Db2 export is written the way Db2 writes it,
and PostgreSQL reads none of those notations.

**Covers.** Per value: the Db2 timestamp form in any column, a fraction longer than
PostgreSQL stores, the z/OS timestamp with time zone, the Db2 for i date formats, the
two-digit-year window (40–99 → 1940–1999, 00–39 → 2000–2039), time formats, text columns
left alone, a value that is no date passed through, and an unknown configured
`date_format` being fatal. Per file: the order of the date parts is worked out from the
whole file; a column whose values fit more than one order **stops that table** instead of
guessing (reading such a date the wrong way would migrate a different date with no error);
a configured order is used without reading the file twice; a header line is not read as a
value.

**Expected result.** All pass. No third-party dependency.

### `test_mysql_zero_datetime_default.py` — 5 tests

**Purpose.** MySQL/MariaDB all-zero date defaults (`'0000-00-00'`), which PostgreSQL
cannot store, are handled as `migration.zero_datetime_default` says.

**Covers.** The default `remove` drops the clause; a string value and a SQL keyword
(`CURRENT_TIMESTAMP`) are used as replacements; a non-zero default is preserved; and the
generated PostgreSQL DDL is correct.

### `test_mysql_default_values.py` — 2 tests

**Purpose.** UUID defaults are converted to the configured target function.

**Covers.** The conversion of a MySQL UUID default, and a custom
`uuid_default_function` such as `uuidv7()`.

### `test_mysql_fetch_indexes.py` — 3 tests

**Purpose.** Index and constraint fetching survives the rows MySQL returns for functional
indexes.

**Covers.** An expression index whose `column_name` is `NULL`; the stripping of
`utf8mb4` and other charset noise from an index expression; the same `NULL` column case
for constraints.

### `test_mysql_spatial_point.py` — 3 tests

**Purpose.** MySQL `POINT` values and spatial indexes reach PostgreSQL intact.

**Covers.** A `POINT` delivered as WKB bytes and as a WKT string both convert to
`(9.6, 50.6)` / `(13.404954, 52.520008)`; a spatial index is created `USING gist`.

### `test_charset_collate_stripping.py` — 7 tests

**Purpose.** MySQL-only syntax is removed or translated when a view or function body is
converted.

**Covers.** `CHARACTER SET` / `COLLATE` stripped from a view and from the SQL-function
mapping; `WITH ROLLUP` becomes `GROUP BY ROLLUP`; `FIND_IN_SET` becomes a
`string_to_array` construct; the date-extract functions; the MySQL-internal rollup
functions; `CHAR` casts and grouping booleans.

### `test_sybase_rowcount_return.py` — assertions at import, no test function

**Purpose.** A Sybase ASE procedure returning `@@rowcount` becomes a PostgreSQL function
that returns a table.

**Covers.** The converted DDL contains `RETURNS TABLE` and `RETURN QUERY`, `@@rowcount`
is translated into a variable filled by `GET DIAGNOSTICS`, and the status code the
procedure returned next to its rows is commented out rather than emitted — a function
returning a set cannot return a scalar as well ("RETURN cannot have a parameter in
function returning set").

**Expected result.** Passes. Note the assertions are at module level, so they run at
**collection** time — a failure appears as a collection error, not a test failure. Worth
turning into proper test functions when this file is next touched.

---

## 4. PostgreSQL target

### `test_pg_collations.py` — 60 tests, the largest file

**Purpose.** The parts of the PostgreSQL target that are easy to get subtly wrong:
collations, text search, index definitions, generated columns.

**Covers.** Identifier parsing (dots inside quotes); a migrated collation is qualified
with the target schema while a built-in one is untouched, an unknown one is dropped, one
belonging to another engine is dropped, and one is kept when the target cannot be
queried; index SQL referencing a migrated collation, keeping the operator class, and
function-based indexes; primary keys built from the constraint definition rather than the
key list, and following `names_case_handling`; index key-list extraction, functional
expressions, access methods and operator classes; `CREATE COLLATION` for ICU,
non-deterministic with rules, libc split locale, and the empty case. Text search:
configurations and dictionaries qualified and remapped, built-ins untouched, names
quoted, view DDL rewritten. Index tails including partial-index predicates. Generated
columns: stored and virtual read from a PostgreSQL source, excluded from the SELECT and
INSERT of the data migration, and their clauses stripped for the LOB staging table.
Finally that a `CONSTRAINT TRIGGER` is left to the trigger migration and a primary key to
the index migration, and that the index type is read from `pg_constraint` rather than
from `information_schema`.

**Expected result.** All 60 pass.

### `test_pg_udt_ordering.py` — 16 tests

**Purpose.** User defined types, domains, ranges and sequences are created in an order
that works, and the data migration handles their values.

**Covers.** The execution order in the orchestrator and the preparation order in the
planner; a topological sort of composite types; `subtype_diff` syntax for range types;
`NOT NULL` de-duplication in `CREATE DOMAIN`; JSONB serialisation and a `NOT NULL`
fallback in the insert batch; sequence fetching and migration; array-typed columns; `BIT`
defaults from PostgreSQL and from MySQL; type caster registration; index SQL with
collation, with an operator class and `USING gin`.

### `test_pg_aggregates.py` — 13 tests

**Purpose.** `CREATE AGGREGATE` is generated correctly, and aggregates are recognised
among the routines.

**Covers.** State and final functions, the minimal form, parallel safety (`unsafe` not
emitted, `restricted` emitted), `FINALFUNC_EXTRA` and `FINALFUNC_MODIFY`, parallel
support functions, moving-aggregate implementation, sort operator, ordered-set and
hypothetical-set aggregates, and escaping of a quote in `INITCOND`. Plus: aggregates are
recognised with their arguments, ordered last, and routines belonging to an extension are
skipped.

### `test_pg_extensions.py` — 11 tests

**Purpose.** The pre-migration extension check blocks only when it should, and looks only
at the tables actually being migrated.

**Covers.** A dependency already installed in the target, or listed in the configuration,
is not blocking; a missing one is; a missing *and* unavailable one says so; a
configuration snippet is logged for the missing ones; no dependencies means no issues; a
source without extensions passes; the extension name is matched case-insensitively.
Also that the table selection honours `include_tables` / `exclude_tables` and defaults to
all, and that the dependency query is limited to the selected tables.

### `test_pg_lob_worker.py` — 4 tests

**Purpose.** The LOB import worker copes with generated columns and broken pointers.

**Covers.** Generated columns are excluded from the SELECT and the INSERT; the LOB value
is still read although the column index shifted because of that exclusion; a broken LOB
pointer leaves the row with `NULL` rather than failing the table; a table without
generated columns keeps all its columns.

### `test_extension_check.py` — 3 tests

**Purpose.** `get_required_extensions()` and `check_and_create_extension()`.

**Covers.** `uuid-ossp` is inferred from `uuid_default_function: uuid_generate_v4()`; an
explicit `required_extensions` list is returned; `check_and_create_extension` reports
success and a message containing `present` when the extension is already there.

### `test_pg_session_settings.py` — 9 tests

**Purpose.** `target -> settings` (and the same key for a PostgreSQL source): what is
prepared out of them, and that every connection runs with them.

**Covers.** The settings of one side are never applied on the connection to the other; a
name is recognised whatever its case (`Role`, `WORK_MEM` used to raise `KeyError`); `role`
is always the last statement, so a setting needing more rights is not blocked by the
switch to it; `search_path` is written without quotes of its own; a name PostgreSQL does
not know is reported and left out. Then that `connect()` applies them — which is what
decides who owns the objects the migration creates — and that preparing them does not
recurse, since `prepare_session_settings()` opens a connection of its own.

### `test_varchar_to_text.py` — 11 functions, 17 tests

**Purpose.** `migration.varchar_to_text_length` and `migration.char_to_text_length`.

**Covers.** Each setting decides its own family and nothing else: with only
`char_to_text_length` configured, a `varchar` column keeps its length — it used to become
`TEXT`, because `CHAR` is a substring of `VARCHAR` and both settings default to `-1`,
which compares true against every length. Also the promotion itself at and above the
limit, `nvarchar` / `univarchar` counting as the varchar family, a column the source
reports no length for becoming `TEXT`, and a non-string type never being promoted.

### `test_data_migration_limitation.py` — 14 functions, 17 tests

**Purpose.** How one entry of `data_migration_limitation` decides which rows of a table
are migrated. The planner, the orchestrator and the validator all ask the same resolver,
so one entry cannot mean one thing while the rows are counted and another while they are
copied.

**Covers.** The condition is used only for a table which really has the column named in
the entry (as a name or as a pattern); a table not larger than the optional row limit is
migrated whole; `{source_schema_name}` and `{source_table_name}` are substituted; several
entries matching one table are combined with `AND`, and only those which apply are;
an unusable column pattern is reported instead of ending the run; and the columns of a
table are accepted in each of the shapes the callers hold them in.

### `test_sequence_protocol_columns.py` — 6 functions, 7 tests

**Purpose.** The sequences protocol table against the code which reads and writes it.
`decode_sequence_row()` reads a row by position, so a column added in the middle shifts
every following one and nothing says so — the migration keeps running and writes the
increment into the minimum value.

**Covers.** The test reads the `CREATE TABLE` the migrator issues and compares it with the
decoder position by position (not only in the same order: a decoder reading two names out
of one position keeps the order and is still wrong), checks that the `INSERT` names only
columns which exist and passes one value per column, and that the declared start of a
sequence and the value it stands at are two separate columns, both clamped to what a
`BIGINT` column can hold.

### `test_default_value_substitution_patterns.py` — 7 functions, 9 tests

**Purpose.** The patterns the planner writes into `default_values_substitution` for every
entry of `sql_functions_mapping`. Such a row replaces the default of a column *entirely*,
so it has to describe a default which IS that function.

**Covers.** A default which is the function is substituted, with the parentheses a source
writes around its own defaults (`(getdate())`) allowed. A default which only contains the
function is not: `'[' + suser_name() + '@' + host_name() + ']'` used to collapse to the
bare `current_user`. Neither is a longer name which starts with the mapped one.

---

## 5. Query conversion

The step which converts the SELECT statements an application holds as text
(`--convert-queries`). None of these needs a database: what they exercise is the reading of
a file, the decision whether a statement may be converted at all, the bind parameters and
the file which is handed over.

### `test_query_splitter.py` — 25 tests

**Purpose.** Cutting a file of application SQL into the statements it holds.

**Covers.** Every way a separator turns out not to be one: a semicolon inside a string
literal, a doubled quote, a line comment, a block comment, a `$$` quoted body and a quoted
identifier in each of the three spellings the sources use. `GO` alone on its line cuts, with
or without a repeat count, while `go` as a column name does not. The other four separator
modes, CRLF and a byte order mark, the line numbers a statement is reported at, the
`-- name:` annotation, and the hash which recognises the same statement written twice.

### `test_query_classifier.py` — 23 functions, 65 tests

**Purpose.** The four gates which decide whether a statement is a read. This is the safety
property of the whole step, so it is asserted construct by construct.

**Covers.** Nineteen statements which write, each refused - and a write the parser cannot
read at all, which is the one case gate 2 decides on its own: "the migrator does not
understand it" and "it writes" are very different answers. The constructs which begin with
`SELECT` and still write or lock: `SELECT ... INTO` a table and into a host variable, a
data-modifying CTE, `FOR UPDATE`, `HOLDLOCK`, `UPDLOCK`, `nextval()`, `setval()` and two
statements in one entry. `NOLOCK`, which is not a write and is reported instead of refused.
A statement the parser cannot read is neither converted nor called a write. Gate 4 asks the
same questions of the converted statement, in the dialect of the target.

### `test_query_parameters.py` — 18 functions, 33 tests

**Purpose.** The bind parameters of an application, through the conversion and back.

**Covers.** The five marker styles and the round trip of each. What is not a parameter: a
marker inside a string literal or a comment, a `::` cast, a `@@` global variable of the
source. The name the converters see instead of `$1`, because a parser reads `$1` as a column
and writes it back quoted. And the order: a conversion which moved the parameters -
`TOP (?)` becomes `LIMIT $1` at the other end - or lost one is reported as BLOCKING.

### `test_db2_query_conversion.py` — 22 functions, 47 tests

**Purpose.** The dialect of Db2, which is one dialect behind three connectors and therefore
stands once, in `connectors/db2_query_conversion.py`.

**Covers.** The special registers written without parentheses, the labelled durations, the
isolation clause and the optimizer hints which are removed, `SYSIBM.SYSDUMMY1`,
`DAYS(a) - DAYS(b)` and the single `DAYS()` which is reported rather than guessed at. Then
the two properties which matter: a statement of a Db2 application **is a read once it has
been prepared** and would not parse at all without the preparation, and the preparation
**does not make a write look like a read** - the gates read the text of the application and
none of it is changed. The wrapper around the connector's converter is asserted through a
fake connector, and the function mapping is asserted to be the one mapping the three
flavours share.

### `test_informix_query_conversion.py` — 40 functions, 77 tests

**Purpose.** The dialect of Informix, which has more spellings of its own than any other
source here, and the line between what is converted and what stops the conversion.

**Covers.** `FIRST` / `SKIP` moved to the end of the statement as `LIMIT` / `OFFSET`,
including inside a subquery and including the query combined with a set operator, where
nothing is moved because whether the number limits the branch or the result is written
nowhere. `TODAY` and `CURRENT` with their field qualifiers - and the `CURRENT ROW` of a
window frame, which is not the register. The durations counted in `UNITS`, the `DATETIME` /
`INTERVAL` literals and types, the `sysmaster:sysdual` which is left out, the subscript which
becomes `SUBSTR`, `MATCHES` in all four of its forms, the `OUTER()` join and the one which
cannot be attributed and is therefore refused, and `DECODE` / `MDY` / `EXTEND` / `LAST_DAY`.
Then the same two properties as for Db2 - the statement is a read once it is prepared, and no
write is made to look like one - the constructs which stop the conversion with a reason, and
the names which only read like them: `SUM(x) AS units`, `AS rowid`, `note = 'MATCHES TODAY'`.
The preparation is also asserted to work on a connector built with `Class.__new__(Class)`,
because that is how the test suite of the migration repository asks for it.

### `test_query_conversion_workflow.py` — 27 functions, 31 tests

**Purpose.** The two ends of the step: what is sent to the target, and what is written into
the file.

**Covers.** The probe is a transaction which is read only, bounded by a timeout, and rolled
back - the last of the four layers which keep this step from writing, asserted statement by
statement. Each test level sends what it promises, a prepared statement is deallocated
again, and a statement with bind parameters is always prepared because EXPLAIN of one is
refused. Then the output file: a statement which may not be used is commented out so the
file stays runnable, every block says what both tests answered, a warning cannot be missed,
the counts are in the head of the file, and the same input produces the same file. An output
path which would write over an input file is refused, and so is replacing an existing output
file without being told to.

---

## Adding a test

- Prefer `pytest` and plain `assert`; `parametrize` when the same rule must hold for
  several inputs — that is how "the six object filters behave identically" is enforced
  rather than asserted in prose.
- Do not connect to anything. Build the object with `Class.__new__(Class)` and give it a
  fake config, or use `unittest.mock`, as every file here does.
- Name the test after the behaviour, not the function under test:
  `test_the_default_level_shows_warnings`, not `test_print_log_message_2`.
- Give it a docstring when the reason is not obvious from the name — especially when the
  test exists because of a specific past defect.
- **Never write a script that patches the source.** This directory previously held 19 such
  scripts (`patch*.py`, `fix_config.py`) which rewrote modules under
  `credativ_pg_migrator/` when imported, so merely collecting the suite modified the
  working tree. They have been removed; do not
  reintroduce that pattern.
- **A test must be able to fail.** Two files were removed from this directory because they
  asserted on a string they built themselves rather than on the output of the code they
  named: they passed with that code returning wrong results. If you are unsure whether a
  test really covers something, break the production code on purpose and check that the
  test notices.
