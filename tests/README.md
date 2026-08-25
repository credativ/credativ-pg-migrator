# credativ-pg-migrator — test suite

1241 test functions in 63 files. **No test in this directory needs a database, a driver
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
| query conversion | `sqlglot`, `pytest` | the 6 `test_query_*` files and the `test_*_query_conversion.py` of Db2, Oracle and SQL Anywhere |
| anonymization, Db2 CSV | nothing beyond the standard library | 3 files |
| everything else | the package's own dependencies — `psycopg2`, `tabulate`, `jaydebeapi`, `pyodbc` | 29 files, `test_informix_query_conversion.py`, `test_mysql_query_conversion.py`, `test_ms_sql_query_conversion.py` and `test_sybase_query_conversion.py`, `test_tsql_outer_joins.py`, `test_oracle_outer_joins.py`, `test_oracle_fk_dependencies.py`, `test_sqlite_query_conversion.py`, `test_postgresql_query_conversion.py` and `test_query_source_test.py` among them |

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
  --ignore=tests/test_sequence_protocol_columns.py \
  --ignore=tests/test_oracle_fk_dependencies.py \
  --ignore=tests/test_sqlite_query_conversion.py \
  --ignore=tests/test_postgresql_query_conversion.py \
  --ignore=tests/test_query_source_test.py \
  --ignore=tests/test_partitioning.py \
  --ignore=tests/test_comments_migration.py \
  --ignore=tests/test_constraint_order.py \
  --ignore=tests/test_migration_report.py
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

### `test_off_valued_settings.py` — 9 functions, 32 tests

**Purpose.** The three settings whose values include the word `off` are read as the words
they were written as. The configuration is read with PyYAML, which follows YAML 1.1: an
unquoted `off`, `on`, `yes` and `no` are booleans there, so `target_test: off` — the
documented value of a documented option, written the way it reads best — arrived as
`False` and stopped the run at the start.

**Covers.** `migration.validate_objects`, `query_conversion.target_test` and
`query_conversion.output.sidecar`, each written as the bare word, quoted, as `no`/`false`,
as `on`/`true`, in capitals and left empty; the defaults when the key is absent; that an
unquoted `off` passes the startup schema check; that a value the migrator cannot carry out
still stops the run; and that a typo, run with `--ignore-config-schema-errors`, reaches
`probe_statements()` unchanged instead of being turned into the default. The configuration
is written as text, not dumped from a dict — `yaml.safe_dump` quotes `off` by itself, which
is the whole trap.

**Expected result.** All pass. Needs `jsonschema` for the startup check.

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

### `test_anonymization_method_parameters.py` — 10 functions, 41 tests

**Purpose.** A method parameter written as text is read as what it says. Every parameter
comes out of YAML and a form-driven editor writes each of them as a string, so
`pass_original: "false"` was a non-empty string, therefore truthy, and
`postgres_anon_native` passed the original value into the call of the `anon` extension —
in the one workflow whose point is that the original value does not travel.

**Covers.** `pass_original` written as `false`, `"false"`, `no`, `off`, `0`, empty and as
each of their opposites, with the arguments kept in both cases; the `flag()` helper against
every spelling YAML itself would read as a boolean; `out_type` as `int` and as `integer`
(the two documents named different ones), read case-insensitively; that the hash stays
deterministic and salted; and `prefix_len` / `suffix_len` of `partial_mask` given as text.

**Expected result.** All pass. No standard-library dependency.

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

### `test_oracle_fk_dependencies.py` — 10 tests

**Purpose.** The foreign key ranking of the pre-migration analysis, for Oracle. P3-4 of
`development/OPEN_ISSUES.md`: `get_top_fk_dependencies()` returned `{}`, so an Oracle schema
was surveyed as if it held no foreign keys, whatever it held.

**Covers.** The ranking is by the keys defined **on** each table, most first and then by name
so two runs read the same; both ends of every key are named, a composite one as a list; a
referenced table of another schema and one removed by the filters are marked `[not migrated]`;
a key whose referenced constraint this account cannot read still counts, and says so, because
dropping it would make the count short by exactly the keys which fail later; a table the
filters removed is not ranked at all; a failed read is reported at ERROR and does not raise,
so the empty ranking cannot be mistaken for "no foreign keys".

**Note.** `oracledb` is not a dependency of this migrator — it is installed by whoever migrates
an Oracle database — and the connector imports it at module level. The file stubs the module
when it is absent rather than skipping itself, so these 10 tests run everywhere. Nothing here
reaches the driver: the connector is built with `__new__` and its cursor is a `MagicMock`.

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

### `test_column_defaults.py` — 28 functions, 52 tests

**Purpose.** A column keeps the DEFAULT it had, or the run says what it lost. P1-4 of
`development/OPEN_ISSUES.md`. A default is not decoration: it is the value **every row
inserted after the migration** gets, so a dropped one is a column full of NULLs where the
source generated something, and a half-converted one is a column full of a different value.

**Covers.** **Oracle** — `SYS_CONTEXT('USERENV', ...)` translated where PostgreSQL has a
counterpart and reported where it has none; `SYS_GUID()` on a UUID, a text and a BYTEA column,
the last of which used to be dropped although PostgreSQL can express it (a RAW(16) is 16 random
bytes, which is what the hexadecimal of a generated UUID decodes to), and reported with what
the column loses where it can be expressed by nothing. **MS SQL Server** — the **style**
argument of `CONVERT`, which is what the value looks like: thirteen styles asserted against
the `to_char()` format which writes the same string, both directions (a datetime written as
text and a string read into a date), that the CAST is kept around the styled value because
Transact-SQL truncates it to the length of the target type, and that the styles which no
single format can write — Transact-SQL pads the hour with a space, `Aug 24 2026  9:30AM` — are
reported with what they mean rather than converted into something nearly right. **SQL
Anywhere** — that a double-quoted token in a DEFAULT is a *string* and not a column reference,
which is what the grammar of that DEFAULT settles, so `'a' || "b"` is converted instead of
being thrown away at INFO; the special values of the grammar, including the two UTC ones and
the bare `TIMESTAMP` which used to be handed to PostgreSQL as a type name; and that
`DEFAULT TIMESTAMP` and `DEFAULT LAST USER` say that only their INSERT half was migrated,
because the UPDATE half needs a trigger nobody creates.

**Expected result.** Passes. Each connector group is skipped where its driver is not installed
— with all of them present the file is 52 tests, without Oracle and SQL Anywhere it is 28.

### `test_index_collations.py` — 26 functions, 46 tests

**Purpose.** A functional index means in the target what it meant in the source. P1-3 of
`development/OPEN_ISSUES.md`, and what reading it turned up next to it.

**Covers.** Three things which happened to the expression of a functional index without a word
being written. **The collation was deleted** — by `clean_index_expression()` of the MySQL and
MariaDB connectors, and by `get_create_index_sql()` of the PostgreSQL target, which does it to
every source which is not PostgreSQL. A collation decides which strings count as equal, so a
case-insensitive index became a case-sensitive one: it answers a query with fewer rows, and a
UNIQUE one stops refusing two values which differ only in case. What `credativ_pg_migrator/
collations.py` decides per collation is asserted here — a `_bin` collation is carried over as
`COLLATE "C"`, which compares byte for byte exactly as MySQL does; a `_ci` or `_ai` one cannot
be said in PostgreSQL without a non-deterministic ICU collation and is reported with what
changes and how to get it back; a `_cs` one becomes the default of the target, which compares
the same way; and a name nobody knows is reported rather than assumed harmless. That the
catalogue of the target is asked before the name is read, because `fr_CI.utf8` is Côte d'Ivoire
and not a case-insensitive collation. **A `sqlglot.transpile` which raised** was answered with
`except Exception: pass`, which left the raw MySQL expression standing as the PostgreSQL one —
`a || b` is an OR in MySQL and a concatenation in PostgreSQL, and both are valid. Such an
expression is refused now, with the text of the source in the message. **Every identifier
became a string literal**: the backticks were replaced with double quotes before the text was
handed to sqlglot as MySQL, where a double quote starts a STRING, so an index on `lower(email)`, with the column
name in backticks as MySQL writes it, was converted into `lower('email')` — an index on a
constant, on every functional index of every MySQL and MariaDB source. And that the name of a collation is not folded by
`names_case_handling`: `COLLATE "C"` became `COLLATE "c"`, which PostgreSQL does not have.

**Expected result.** Passes. The connector groups are skipped where their drivers are not
installed; the decision itself needs nothing beyond the standard library.

### `test_undecodable_bytes.py` — 53 functions, 58 tests

**Purpose.** A byte the assumed encoding cannot read is never deleted from a value, and
U+FFFD is never written into the target. P1-1 and P1-2 of `development/OPEN_ISSUES.md` —
nine places, one decision. The MS SQL Server connector decoded the values pyodbc hands over
as bytes with `errors='ignore'` three times, so such a byte was removed from the value: the
row reached the target shorter than it left the source and nothing said so, not the row
counts and not the validator, which reads both sides through the same decoder. The other six
— four in the SQLite connector, two in the CSV reader of a file data source — wrote the
replacement character, which cannot be told apart from one which was really in the data and
cannot be turned back into the byte it stood for.

**Covers.** `migration.on_undecodable_bytes` in all three of its settings, over
`text_decoding.TextDecoder`: that `substitute` loses no byte (what comes out re-encodes to
exactly the bytes which went in), that `fail` refuses the value in a message which names the
setting, and that `remove` — the behaviour from before the repair — is still reachable and is
now reported for every value it happens to. That U+FFFD is never written, whatever the
setting. That a value the first expected encoding reads is not reported at all and one a
later expected encoding reads is counted without a line per value, with the detailed reports
limited per place and the totals in the summary. The limit of the detection is asserted
rather than left to be discovered: **utf-16 reads almost any byte string of even length**, so
four bytes of Windows-1252 come out as two characters nobody wrote, and the count is the only
evidence there is.

Then each of the three places. **MS SQL Server**: every ODBC converter goes through the
decision, all five are callable the way pyodbc calls them — with the value alone — a
`datetimeoffset` which is not the 20 byte structure is read as text rather than as the repr
of its bytes, the summary is written when the connection is closed, and a connection which
read nothing odd says nothing. **SQLite**: a real SQLite file holding a TEXT value which is
not valid UTF-8 is read through the text factory in every setting, the two value coercions go
through the decision as well, a DDL script which is not UTF-8 is read as latin-1 and reported
for it, and one with a byte order mark loses the mark instead of handing `\ufeff` to SQLite as
part of its first statement. **A file**: the CSV reader keeps every byte of a file declared as
the wrong encoding, refuses it under `fail`, names the file in the summary, restores the
previous decoder when a second file is read inside the first — which
`convert_csv_to_utf8()` really does, to work out the order the dates are written in — and
`convert_csv_to_utf8()` itself is run end to end over a Windows-1252 export to assert that no
U+FFFD reaches the UTF-8 file the target is loaded from.

Three tests read `ms_sql_connector.py`, `sqlite_connector.py` and `config_parser.py` with
`ast` and fail if any call there passes `errors='ignore'` or `errors='replace'` again.

**Expected result.** Passes. The two connector groups are skipped where their drivers are not
installed; everything else needs nothing beyond the standard library.

---

## 3b. Validation

### `test_protocol_task_finished.py` — 13 functions, 15 tests

**Purpose.** The method the orchestrator calls has to exist. P2-1 of
`development/OPEN_ISSUES.md`: `index_worker()` answered an index whose SQL came out empty with
`self.migrator_tables.update_protocol_task_finished(...)` and nothing defined that method, so
the call raised `AttributeError`, the `except` around the worker caught it, and the index was
recorded as failed with the AttributeError as its message — the right outcome for the wrong
reason, and a line which says nothing about the index.

**Covers.** The check which would have caught it the day it was written: every
`migrator_tables.<name>(...)` in the orchestrator, the planner, the validator and all twelve
connectors — around 390 calls — must name a method `MigratorTables` really has. Then what the
new method writes: the end of the task, the reason and the verdict; that an object which was
not created is not a success by default; that the journal of the run is finished as well,
because every object is written there when it is planned and one which is never finished
leaves a row saying the work began and never saying what came of it; that sequences are keyed
by their own id column; and that an unknown object type is reported and writes nothing. That
the two vocabularies — the plural name which selects the protocol table, the singular one the
journal uses — are written down rather than guessed, with both sides checked against what
really exists, and that every table the method can be pointed at has the three columns it
sets. Finally the worker: an index with no statement is recorded as not created, with the
index and its table named, and is **not** answered with True — the caller writes
`'migrated OK'` over the row of every worker which did not answer False, so answering True
would have reported an index which does not exist as migrated (F-24, which had only stopped
happening because the missing method crashed first).

**Expected result.** Passes. Nothing connects to anything: the protocol connection is a stub
which records the SQL it was given.

### `test_routine_body_names.py` — 10 functions, 13 tests (15 with every driver installed)

**Purpose.** The names inside the body of a migrated routine — measured, and then repaired.
P3-2 of `development/OPEN_ISSUES.md` said **measure before building anything**: the
`names_case_handling` repair reaches the statements inside a routine for ms_sql and sybase_ase,
because those go through the connector's statement converter one at a time, and the other
sources had never been looked at.

**Covers.** What the measurement showed, locked in so it cannot drift. **Informix** wrote
`"migtest"."Orders"` into every body — the source spelling, *in quotes*, which freezes it: with
`lower` the migration created `orders` and the routine named `Orders`, valid PL/pgSQL which
fails the moment it runs. It names the table as the target has it now, in both directions.
**Db2 for i** read `settings['code']` while the one caller passes `funcproc_code`, so it
answered the empty string for every routine of every Db2 for i migration; the key is right and
what that connector really does — rewrite `SIGNAL SQLSTATE` and nothing else — is said out loud
instead of being reached by accident. Then the declaration every connector carries: a source
whose statements go through the statement converter declares nothing, and one whose body keeps
its own names says **which** names and why `lower` is the one setting under which such a body
finds the objects it names. And that the run warns only where the setting makes the body wrong,
and not for a routine which was not converted at all.

**Expected result.** Passes. Connectors whose driver is not installed are skipped.

### `test_kinds_not_read.py` — 13 functions, 63 tests (81 with every driver installed)

**Purpose.** "Not implemented" is not "the source has none". P2-8 of
`development/OPEN_ISSUES.md`: `fetch_user_defined_types()` and `fetch_domains()` answered `{}`
in connectors whose sources certainly do hold such objects — Db2 distinct types, the DISTINCT
and named ROW types of Informix, the user-defined data types of SQL Anywhere, the rules of SQL
Server which the **Sybase ASE connector of this same migrator** reads as domains. An empty
answer says *the source holds none of these*, so the planner wrote "No user defined types
found" and the summary showed `0`.

**Covers.** The two declarations every connector now carries — `OBJECT_KINDS_NOT_READ` (the
source has them and this connector does not read them, with what is really there and where it
is kept) and `OBJECT_KINDS_ABSENT` (the source has none at all, so `{}` is the truth) — that no
kind is called both, and that a reason of two words is refused. Then the guard which is what
this repair is really made of, parametrised over **every connector and both kinds**: a fetch
which answers without asking the source (`pass`, `return {}`, `return None` — read out of the
method with `ast`, so a connector which grows a real implementation stops being a stub without
anybody having to remember this file) and whose kind is in neither table **fails the suite**.
The other direction too: a declaration which outlived the stub it described. And what the run
says — that a kind which was not read is reported as that rather than as "none found", with a
row of type `not read` written into the journal where the summary picks it up, that a source
which really has none is left to say so, that a journal which cannot be written does not take
the warning with it, and that the summary shows `?` rather than `0` for such a kind.

**Expected result.** Passes. Connectors whose driver is not installed are skipped, which is why
the count differs between a bare environment and one with everything installed.

### `test_constraint_order.py` — 8 tests

**Purpose.** The order the constraints of a migration are created in. A foreign key needs the
UNIQUE constraint it references to exist already, and the two belong to **different tables** — so
the order the protocol holds them in, which is the order the planner read the tables in, decided
whether the key could be created at all. `fk_children` sorts in front of `fk_parent`, so the key
referencing it failed with *there is no unique constraint matching given keys for referenced
table*. The eight parallel workers made it worse rather than better: a key which happened to
stand behind its unique constraint failed too, in some runs and not in others.

**Covers.** That every foreign key is created after every other constraint, in the order the
migtest example really holds them; that the first wave is finished before the second starts —
parallel inside a wave is what makes the phase fast, parallel across the two is what made the key
fail intermittently; that the case of the type name is not what decides; that a constraint the
configuration excludes is not created, and that a constraint which failed is not recorded as
migrated OK and does not stop the wave behind it.

### `test_comments_migration.py` — 24 tests

**Purpose.** `COMMENT ON …` for everything a migration created — the phase which ended on its
first refusal and reported success anyway. `COMMENT ON TRIGGER "migtest"."c_customers_audit"` is
a syntax error at the dot: a trigger is not a schema object, it belongs to its table. The whole
phase stood in one `try/except`, so that one statement ended it and every comment behind it — the
views, and the user defined types, whose branch had a `KeyError: 'type_name'` of its own which
nobody had ever reached — was never attempted. And the summary counted the comments the protocol
holds and printed that as the number set, with a hard-coded `0` failed.

**Covers.** The statements PostgreSQL really takes, per kind, asserted as text. That one refusal
is one refusal: the phase goes on, the transaction is rolled back so the next statement does not
answer `current transaction is aborted`, the object is named in the log and in the phase status.
That a comment whose object was never created is **skipped** rather than counted as a failed
comment — the failed view is already recorded as one. And two guards read with `ast`, which are
the durable half: every key the phase reads out of a decoded protocol row has to exist in the
decoder it came from, and every decoder it uses has to be able to say whether its object was
created. The second one found `decode_index_row()`, which had the same gap as the view decoder
and which nobody had noticed.

### `test_migration_report.py` — 19 tests

**Purpose.** The two blocks of the closing summary which **name** rather than count.
`Indexes 77 | 75 | 2` says two indexes are missing and nothing about which two — the answer sits
in the protocol tables, one query away, which is not where somebody reads a migration report
from.

**Covers.** `[ DETAILED MIGRATION REPORT ]`: every table with its row counts and its duration; a
row count which does not match is not smoothed over; a table whose data was never copied is not a
table which lost its rows — reading the counts from `tables.target_table_rows`, which is what the
target held when the plan was made, reported every table of a fresh migration as having lost all
of them, which is how that was found; a row limit from `data_migration_limitation` is what the
table is measured against; an object which did not arrive is named with what the target said, and
an object which was never **attempted** is told apart from one which failed. And
`[ PARTITIONING ]`: a scheme shown on both sides, a scheme of more than one level written as the
levels it has with both `Parts` columns counting the same thing, and the flattened table — the
line which matters most, because something the source had is not there any more.

### `test_phase_status.py` — 17 functions, 17 tests

**Purpose.** A phase of a migration says what it really did. P2-7 of
`development/OPEN_ISSUES.md`: nineteen places wrote
`update_main_status({..., 'success': True, 'message': 'finished OK'})` on the way out of a
phase, whatever had happened inside it — so the migration of the indexes reported `finished OK`
over a run in which indexes had failed, and the failures were in the indexes protocol table all
along, one query away.

**Covers.** That a phase whose objects all arrived finishes OK and one which lost objects does
not, with the message saying how many of how many and where to read which; that a phase
reporting a failure of its own keeps it; that a phase creating no objects is left alone; and
that an object which was never *attempted* (`success IS NULL` — an index the configuration
excluded, a run which stopped before it) is not counted as a failure, because a deliberate skip
is not a broken migration. That the check sits in the one method every phase closes itself
through, so a new phase cannot forget it — with a guard which fails if a phase is added that is
neither counted nor written off with a reason. Then the two defects reading it turned up: **the
planner closed a row it had not opened** (each workflow branch opened `Planner / Standard
workflow` and closed `Planner / ''`, so the phase row of the workflow was never closed at all —
no duration, no result, in every migration ever run) and the orchestrator did the same for a
resumed run; asserted by pairing every `insert_main` with an `update_main_status` in both
modules. And that the summary **shows** it: the timing table was `Phase / Step | Duration |
Start Time` with no status at all, so a phase which failed looked exactly like one which
succeeded, and a phase which never finished is now shown as such.

**Expected result.** Passes. Nothing connects to anything: the protocol connection is a stub
which answers the counting query with whatever a test wants.

### `test_object_presence_check.py` — 16 functions, 18 tests

**Purpose.** The closing pass over the objects establishes **presence**, and says so. P2-6 of
`development/OPEN_ISSUES.md`: at the end of a standard migration the orchestrator asks the
catalogue of the target whether each migrated view, routine and trigger is there — worth having,
because an object whose creation failed only because a dependency did not exist yet becomes
creatable once the whole schema is present. What it recorded was `final_valid = true` and the
word **"valid"**, and being in a catalogue is not doing what the object of the source did.

**Covers.** That the word "valid" is not used for a name lookup, and that the message of every
object says what presence means **for its kind** — one word could not carry it: a view which is
there has had its query resolved by PostgreSQL and can be read; a PL/pgSQL routine which is
there has had the *syntax* of its body parsed and nothing else, so a body which reads a table
that is not there was created without complaint and fails at the first call; a trigger which is
there is attached to its table, which is the wiring and not the behaviour. Then the defect
reading it turned up, which is P2-2's shape one step down: the message was **carried over** from
a step the next one overtook, so a retry which raised no exception left `valid after retry`
standing even when the existence check right after it answered no — the row said an object was
valid after a retry which had not put it there. The message is composed at the end now, from
what really happened: a retry which worked, one which failed and carries its reason, one which
raised nothing and put nothing there, and a check which could not be run at all and must not be
read as an absent object. Finally that the stored DDL of a trigger which needs a hand is never
executed, and that the summary and the run say `In target` / `Missing` rather than
`Valid` / `Invalid`.

**Expected result.** Passes. Nothing connects to anything: the target connector and the protocol
tables are stubs.

### `test_validation_outcome.py` — 53 functions, 58 tests

**Purpose.** "We could not tell" is not "it is correct". P2-2 of
`development/OPEN_ISSUES.md`: the validator started every table at `passed = True` and the
branches which found a mismatch set it to False, so a table where **no branch ran at all** —
no primary key, so no row sample and no LOB check; no checksum on that source; the checks
switched off — ended the run reported exactly like a table which passed every one of them, and
the log said *"passed all active validations"*.

**Covers.** The rule itself, over `outcome_of()`: a check which said no fails the table, a
table passes when at least one check ran and every check which ran said yes, a table no check
could run against is `NOT VALIDATED`, and a crash is a failure and not an absence. That the
outcome is **derived and never accumulated**, with a source guard which fails if a `passed`
flag is put back into the table result. Then the real `_validate_table_inner()` over stubbed
connectors, for the situations which produce each outcome, including the one this repair is
about — and that the sentence it used to print is not reachable any more. Finally the summary,
which is where the verdict is read: that a table nobody could measure is marked `?` and counted
on its own, that a table which failed the row sample is no longer shown as `PASS` (those two
checks were recorded in no column at all, so the summary could not see them), that a `SKIP` is
not counted as a check which passed, and that a protocol row written before the outcome existed
is still rendered.

It also covers **P2-3**, the structural counts, which were recorded from the first day and
compared by nothing — a table which arrived with half its indexes was reported as validated.
That the columns are compared **exactly** (one column fewer is data which did not arrive) and
the indexes and constraints only for a **shortfall**, because the two sides do not count the
same things and were never going to: PostgreSQL creates an index for every primary key and
every unique constraint, the migration adds one to the parent of a foreign key which has none,
and the SQLite connector counts neither the primary key nor a unique constraint as a
constraint at all — comparing those for equality reports a table which arrived complete as
broken, which is how a check earns being ignored. That a count of `None` or `-1` is not a
count. That objects the configuration did not ask to migrate are not missed. And the asymmetry
which keeps P2-3 from undoing P2-2: **a structural check can fail a table and cannot pass
one**, because the number of columns matching says nothing about whether the rows arrived.
Finally that the summary reads the recorded verdict instead of comparing the numbers for
equality again, and shows what the comparison said next to them — `6/8 PASS` and `6/5 X`,
because `5/6` alone does not say which of the two it is.

And **P2-4**, the tables which used to vanish: a table whose connectors could not be built or
whose databases could not be reached was answered with `None` and dropped, so it was missing
from the protocol table, from the report and from the count at the bottom of it. That such a
table now gets a row; that the row says the **validation** failed and not the table, so a red
line cannot be read as "this table is broken" when what broke was the measurement of it; that
it is `FAILED` and not `NOT VALIDATED`, because an exception is not the ordinary state which
that outcome is for; that a whole `run()` with one table which dies still reports both tables
and names the one which died; and that a table which could not even be recorded says in as many
words that the report is short of a row.

And **P2-5**, which tables are looked at in the first place: a table was submitted for
validation only when the row counts the *migration* had recorded said it held rows, and the
decoded protocol row has no `source_table_rows` key at all — asserted here, because it is what
made half of that condition dead and left a standard migration validating a table only when
rows had already been recorded in the **target** for it. That the row counts of the migration
now decide nothing about which tables are looked at (an `ast` check over `run()`), that an
empty source against an empty target is a pass *reached by looking*, that a source with rows
against an empty target fails, that a count nobody could read is not a count of zero, and that
a table whose `migrate_data` is off is not reported as a mismatch for holding none of the rows
it was never meant to hold — with its structure still compared, and with the reason told apart
from "the check was switched off", which is a different thing.

**Expected result.** Passes. Nothing here connects to anything: the connectors, the protocol
tables and the log are stubs, and the summary is driven through a fake cursor.

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

### `test_partitioning.py` — 137 tests

**Purpose.** What becomes of a table which is partitioned, PostgreSQL to PostgreSQL.
`development/PARTITIONING_STRATEGY.md` §0.4. Three answers — the scheme of the source
**preserved**, **flattened** into one ordinary table, or replaced by the `target_partitioning`
somebody wrote by hand — and one which is not an answer: a **partition** is never a table of its
own. That last one is the defect it was built on. `fetch_table_names()` answers the parent and
every partition of it, so every row was migrated twice — the parent answers all of them — and
each partition was answered with `CREATE TABLE … PARTITION OF` against a parent nothing had
partitioned.

**Covers.** The decision, which is a pure function of what the catalogue said and therefore needs
no database: a partition of a partition belongs to the table at the top; the statements come out
parents-before-children so they can be executed in order; a sub-partitioned partition carries its
own key; a `DEFAULT` partition is carried over with a word about what attaching a partition
later will cost; the mode can be set globally and per table, and a table listed in
`table_settings` for another reason keeps the global one. Then the feasibility, which runs before
anything is created: `HASH` and `DEFAULT` need PostgreSQL 11, a scheme whose key could not be
read is refused with the alternative named, a target whose version is unknown is reported as
**not checked** rather than as checked, and — the rule which breaks migrations — a primary key or
unique constraint which does not contain every partitioning column stops the run instead of
failing after the data is loaded. And the connector: what it reads out of `pg_partitioned_table`
and `pg_inherits`, that `reltuples = -1` is *unknown* and not zero, that an expression key
(`RANGE (date_trunc('month'::text, created_at))`) is one column and not two, and that
`get_create_table_sql()` no longer writes a `PARTITION BY` of its own — asserted against the
parsed code, because the comments there explain what used to be.

Then the **generator** of a scheme the source never had, which is where the defects of §0.3 of
the design lived: the end of a partition is the start of the next one (the old one wrote
`start + 1 interval - 1 day`, so the last day of every month fell between two partitions), all
five ranges including `day` and `quarter`, one interval of headroom at each end, and a name
which does not fit into an identifier or which two partitions would share is refused rather than
silently truncated into its neighbour. And that the min/max of the column is asked in the quoting
of the SOURCE, one column at a time, with MySQL's backtick and the Transact-SQL bracket asserted
per connector: a double quote is a string literal in MySQL, so `min("col")` there answers the
constant.

And the **diagnosis**, which is the half that stops a run: a generated column as the partition
key, a type with no default btree operator class for a RANGE key, `date_range` over a column
which carries no date, a nullable key column whose statistics hold NULLs with no `DEFAULT`
partition, an inheritance parent, an exclusion constraint, a foreign key referencing the table on
a target older than 12, a partition count past what a scheme can carry, a generated name the
target schema already holds, and an entry whose partitions cannot be worked out at all — which
would build a partitioned table with nothing under it and refuse every row. Each of them is
asserted as blocking, with the message which names what to write instead; and each of the things
which could **not** be checked — a column nobody has analysed, a source which reads no facts — is
asserted to be reported as not checked rather than as good.

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

### `test_mysql_query_conversion.py` — 32 functions, 148 tests

**Purpose.** The dialect of MySQL and MariaDB - two connectors and one dialect, so every test
here is run against both - and the line between what a transpiler may be trusted with and
what it may not.

**Covers.** This is the source the parser of the migrator really models, so the tests are
about the edges of that. What the transpiler writes correctly is asserted as such, so that a
rewrite added later cannot quietly take it over. What it writes as something PostgreSQL does
not have or does not mean is asserted expression by expression: `CONCAT_WS`, which it wraps
in a CASE which answers NULL where MySQL skips the NULL; `DATEDIFF`; the date fields, each of
which is counted from another end in the two dialects; `TIMESTAMPDIFF` in five units and in
one it cannot count; `SUBSTRING_INDEX` forwards, backwards and with a count which is not a
literal; the casts to an unsigned integer; a `DATE_FORMAT` whose format holds a code with no
counterpart. Then the constructs which stop the conversion, each with the reason it gives -
and the names which only read like one: `AS hex`, `t.user`, a call named in a comment or in
a literal. Finally the entry point, the view path which keeps the text of the source when the
statement cannot be parsed, and the assertion that both connectors really do share one
conversion and one function mapping.

### `test_oracle_query_conversion.py` — 34 functions, 63 tests

**Purpose.** The dialect of Oracle, which leaves more standing after a transpilation than any
other source here. It needs no Oracle client: the conversion lives in
`connectors/oracle_query_conversion.py` for that reason, and the tests build it with the base
connector behind it.

**Covers.** The `(+)` outer join in the three shapes it comes in — the one which becomes a
`LEFT JOIN`, the one under an `OR` and the one written inside a call, both of which are
refused because dropping them leaves an inner join that answers fewer rows and looks healthy.
`ROWNUM`: a limit where the query block does not sort, refused where it does, and refused in
the select list, under an `OR` and as `ROWNUM = 2`. The format models of `TRUNC`, the ones
which have an exact field of PostgreSQL and the ones which do not; `ADD_MONTHS`; `ROUND` of a
date. What the transpiler already writes correctly, asserted so that a rewrite added later
cannot take it over. The constructs which stop the conversion, each with the reason it gives,
and the names which only read like one. The five warnings. And the view path, which still
wraps its `CREATE VIEW` and still keeps the text of the source when the statement cannot be
parsed.

### `test_sql_anywhere_query_conversion.py` — 37 functions, 55 tests

**Purpose.** The dialect of SAP SQL Anywhere, which is read as T-SQL — and the two halves
that follow from it. It needs no SQL Anywhere client, for the same reason the Oracle tests
need no Oracle one.

**Covers.** What a T-SQL parser cannot read at all, asserted the way the Db2 tests assert it:
the statement is a read once it has been prepared and would not parse without it, and the
preparation makes no write look like a read. `TOP n START AT m`, `IF … ENDIF` (including one
inside a literal, which is text), `STRING()`, `COUNT()` and the pseudo functions written with
a star. The `*=` outer join, which becomes a `LEFT JOIN` or a reported failure. Then what the
parser reads and answers differently: `LOCATE`, whose arguments are the other way round, and
`TIMESTAMP`, which is a date and a time there and a row version in T-SQL. The format of
`DATEFORMAT` code by code, `LIST`, the constructs which stop the conversion, the `+` which
concatenates — reported from the parsed statement, so that the '+' in the comment above a
statement does not fire it — and the six entries of the function mapping which were defects.

### `test_ms_sql_query_conversion.py` — 20 tests

**Purpose.** The conversion of MS SQL Server, which was the first source of the step and had
no tests of its own until the review of 2026-08-21. `convert_statement_code()` is what the
view path and the query path both call, so what stands here holds for the views of a
migration as well.

**Covers.** The contract of `convert_query_code()` - four keys, and a statement which cannot
be parsed answered with `converted: False` and an empty `code`, never with the text it was
given. That the user defined types of the source are read **once** per connector and no
longer once per statement: this is the only conversion in the tree which asks a database
while it converts, and the query conversion converts a whole file of them with a pool of
workers over one connector. `TOP`, the `+` which concatenates, the function mapping, the
niladic functions which must not come out as calls, `datepart`, and the schema of the source
replaced by the schema of the target. And the limitation which is recorded rather than
discovered: `*=`, which sybase_ase rewrites and this connector does not, so such a statement
is reported - never handed back unconverted.

### `test_sybase_query_conversion.py` — 21 tests

**Purpose.** The conversion of Sybase ASE - the source the whole step was designed for, and
the other one which had no tests of its own. It needs no server: the conversion is a
transformation of text.

**Covers.** The outer join above all. Sybase writes it in the WHERE clause as `*=` and `=*`,
no parser reads that, and the rewrite turns the marked equality into a `LEFT` or `RIGHT
JOIN`. That rewrite had been written against a model of `sqlglot` in which the tables behind
the comma of a FROM clause stood in `From.expressions`; they are implicit joins on the SELECT
now, so the table was never found and **every** `*=` statement went through with the marker
still in it - a view kept it in its text and a query of an application was reported as one
whose outer join could not be rewritten. It goes through
`query_conversion/outer_joins.py` now, the module Oracle and SQL Anywhere use. Asserted in
both directions: the join becomes a `LEFT JOIN`, the conditions which are not the join stay
in the WHERE clause, the `TRUE` left behind is taken out, and a condition standing under an
`OR` is still refused rather than answered with fewer rows. The example §10.2 of the strategy
shows is one of the cases. Then `TOP`, the `+`, the function mapping, the schema replacement,
and the gates in this dialect.

### `test_identifier_case.py` — 60 tests

**Purpose.** The names inside a converted statement, spelled the way the target has them —
the other half of `names_case_handling`. The tables and columns are created the way the
setting says; a view's defining query names those objects, so the name in the query has to be
the name the object got. Three of the twelve connectors did that and nine did not.

**Covers.** What is converted: tables, columns, the aliases they name each other by, output
aliases (which are the columns of the view), and the names a common table expression
introduces together with its column list. What is not: the target **schema**, which comes from
the configuration; the names of functions, which belong to PostgreSQL; the data types; the
keywords; and everything inside a string literal, which is data. That names come out
delimited, which is what makes `upper` work at all — bare `CUSTOMERS` folds to `customers` on
PostgreSQL and would not be found. How each source folds an undelimited name before the
setting is applied: Db2 and Oracle to upper, Informix and PostgreSQL to lower, the
Transact-SQL family, MySQL and SQLite as written — so `keep` keeps the name the object really
has, not the case the DDL was typed in. That a statement which cannot be read as PostgreSQL is
answered exactly as it came in, because a name changed by a search and replace inside a text
nobody could parse is not a conversion. The bind parameters of an application statement
(`$1`…`$n`) are not identifiers and survive the whole round trip. And the matrix the repair was
measured against: **every connector, both settings**, asserting the table is named as the
target has it and the schema was not converted — a connector whose driver is not installed is
skipped by name.

Then the routines and the triggers, which are the other place a name of the target is written.
That `NEW`, `OLD`, `TG_OP` and their siblings are **not** renamed — they are variables of
PL/pgSQL and are folded to lower case whatever the setting says — while the *field* of such a
record is the column of the table the trigger is on and does follow it. That a trigger names its
function, the table it is on and everything in its body the way the target has them, in both
settings, and that the schema is still left alone. That the trigger and the function it calls
always agree, because a trigger which names a function nobody created is not created either. And
that no connector builds the name of a trigger function without the case handling — seven of
them did, and `"TR_AUDITSALES_func"` was consistent and still a name nobody meant.

### `test_names_case_handling.py` — 47 tests

**Purpose.** `names_case_handling`, and the rule the whole migrator follows about names.

**Covers.** The rule has two halves and both are asserted: the target **schema** is used
exactly as the configuration spells it and is never case-converted, and every object name
inside it follows the setting. Then the record: `source_*` is what was read and is **never**
converted — a source can hold `CUSTOMER` and `Customer` as two different tables, and the
protocol has to be able to tell them apart — while `target_*` is what was created. That every
kind of target name is covered by the boundary which converts them, that the conversion is
idempotent (several callers convert on their own and may stay as they are), and that the
caller's own dictionary is not changed. That all twelve connectors read the source names
unchanged, which is what `ibm_db2_luw` used to break. That the four tables which had a single
bare name column now record the target spelling too, and — the guard which keeps this from
rotting — that every decoder still matches the column order of its table, since the decoders
read a row by position and a column added in the middle silently shifts every key behind it.
Finally the collision check: two tables which become one stop the run, every clash is named
and not only the first, columns are checked within their table, a protocol table which is not
there is reported rather than fatal, `keep` is never checked because nothing can collapse
under it, and two routines of one name with different arguments are not a collision —
PostgreSQL tells overloads apart exactly as the source does.

### `test_oracle_outer_joins.py` — 16 tests

**Purpose.** Oracle's `(+)`, and the one thing which makes it different from every other
dialect this migrator reads.

**Covers.** In the Transact-SQL family the marker sits on the join operator and says nothing
about the other conditions, so which of them belongs to the join has to be inferred. Oracle
writes the marker on the column, condition by condition, and therefore says which of the two
readings it means — so nothing is inferred here: `AND o.status(+) = 'X'` moves into the `ON`
clause and `AND o.status = 'X'` stays in the `WHERE` clause, where Oracle applies it too. Both
halves are asserted, because getting either wrong produces a statement which is valid, looks
healthy and answers other rows. Then the shapes the textual marking never reached and which
were counted as outer joins that could not be rewritten: a marked comparison, a marked `IN`
list, and a marker inside a call (`UPPER(o.cid(+))`) — the parsed statement keeps the marker on
the column, so all three are attributed now. A `(+)` under an `OR` is still refused, which is
what Oracle itself does with ORA-01719. And the view path, which converts and moves the same
way.

### `test_tsql_outer_joins.py` — 79 tests

**Purpose.** The `*=` and `=*` outer joins of the Transact-SQL family, for **all three**
connectors and **both** paths. Sybase ASE, MS SQL Server and SQL Anywhere wrote the same
operator and read it the same way — SQL Anywhere has it as the Transact-SQL compatibility
syntax of ASE — so every case runs against all three and against the view path as well as the
query path; this file is what keeps them from drifting apart again. The assertions are made on
the statement without its quoting, because the three do not agree about quoting identifiers or
about the schema of the source and none of that is what the file is about; the view helper
gives each connector the shape its own view path reads (§2.1 of the strategy measured that they
differ).

**Covers.** The join itself — `*=` as a `LEFT JOIN`, `=*` as a `RIGHT JOIN`, the asterisk
standing next to the table whose rows are kept, two joins in one statement, and the operator
never surviving. Then the half which decides whether the answer is right: a WHERE condition
which restricts the **inner** table belongs to the join in this dialect and undoes the outer
join if it is left in the WHERE clause of PostgreSQL, so it moves into the `ON` clause — while
`AND inner.col IS NULL` must **not** move, because that is how the dialect asks for the rows
without a match and inside an `ON` clause it is never true. A condition reading two tables does
not move, a parenthesised `OR` keeps its parentheses on both sides of the move, and a join
written as ANSI in the source is not touched at all — its WHERE clause means the same in both
dialects. Every move is reported, and nothing is reported when nothing moved. What cannot be
attributed is refused in both paths rather than converted into the inner join it would otherwise
become, and it is reported as an outer join which could not be done and not as a statement which
could not be read. Finally the shared marking: the operator inside a string literal is text, and
`UPDATE t SET x *= 2` is the compound assignment MS SQL Server has read since 2008, not a join.

### `test_query_gates_literals.py` — 34 tests

**Purpose.** The textual gates and the parts of a statement they may read.

**Covers.** Gate 2 decides from the text, because a statement the parser could not read still
has to be refused when it writes - and four of the five gates read the whole text, including
string literals and comments. `SELECT id FROM customer -- the report for update of the
pricing sheet` was answered with "the statement takes row locks" and was never converted and
never tested. Every case stands in both directions: the word inside a literal or a comment
must not refuse, the same word standing in the SQL must still refuse. Also the functions
whose effects the migrator does not know, which are named in a warning rather than refused,
and the tables the converted statement carries without a schema, which the target test
resolves through a `search_path` the application may not have.

### `test_query_output_paths.py` — 7 tests

**Purpose.** The output paths of the run, decided before the first file is read.

**Covers.** An output which would be written over its own input, an output which exists
already and may not be replaced, and two input files whose outputs would land on the same
path. None of these needs a conversion to be answered; they used to be answered by the
writer, which runs after a file has been converted and tested, so an existing output file on
the first of twenty inputs threw that file's work away and stopped the run before the rest
were read. Also that the check itself writes nothing.

### `test_query_name_map.py` — 32 functions, 50 tests

**Purpose.** A converted statement names the objects the target really has. P3-1 of
`development/OPEN_ISSUES.md`, §7.3 and W1 of §9 of the conversion strategy: a statement names
the objects of the **source**, the target holds them under another schema, under the case
`names_case_handling` gave them and, where `use_aliases_as_target_names` is set, under the
alias the mapping chose — and every output file said *"name mapping: off"*.

**Covers.** The rewrite — schema, table and column, through the parsed statement, so a column
named like a common word is not rewritten inside a string literal; a column resolved through
the alias of its table and through the scopes above it, which is what a correlated `EXISTS`
needs; an unqualified column of a join left alone, because guessing which of two tables it came
from is how a rewrite breaks a statement which worked; a name without a schema left without
one, because which schema it resolves to is the `search_path` of the application. The
**unresolved-reference report** — the query which reads `AUDIT_LOG` in a run whose
`exclude_tables` left `AUDIT_LOG` behind, named before the target test answers with a bare
`relation does not exist`. **W1** — a column whose type *class* changed, in both directions,
with the message saying what to write instead; and that a type which only changed its name
(`VARCHAR2(30)` → `VARCHAR(30)`, `NUMBER` → `NUMERIC`) is **not** reported, because reporting
those would bury the one which matters. Then the map itself: what it says when the protocol
tables are not there, when they hold nothing, when they cannot be read; that a name is looked
up without regard to case or quoting; that a view is an object a statement may read like a
table. And the pipeline, through the real `convert_statement()`: that the names, the unresolved
objects and W1 all reach the result, and that a run without a map converts as before.

**Expected result.** Passes. Nothing connects to anything: the protocol tables and the source
connector are stubs.

### `test_sqlite_query_conversion.py` — 41 tests

**Purpose.** The query conversion of SQLite, one of the two sources P3-5 added. sqlglot reads
SQLite and writes PostgreSQL, so the transpilation is not what is asserted here.

**Covers.** The line between the three answers a transpilation can give. What it writes
correctly, left alone. What it writes as valid PostgreSQL which answers something **else** —
`total()` becoming `sum()` (0.0 over no rows against NULL), `random()` keeping its name and
answering another range, `printf()` becoming a `format()` PostgreSQL really has, `changes()`
becoming the literal `0` — every one of which runs on the target and passes its test, so every
one of which is refused with the reason. That the refusals are read on the statement of the
**source**, because the function mapping renames several of them and leaves nothing to
recognise. What has no counterpart at all: ROWID (bare and qualified), the SQLite catalogue,
`GLOB`, `MATCH`, `REGEXP`, `COLLATE NOCASE`, the PRAGMA table functions. The four warnings which
survive a green target test, including the NULL ordering — read on the CONVERTED statement,
because sqlglot writes the `NULLS` clause itself and warning about something the conversion has
already done is how a warning earns being ignored. And that the view path goes through the same
converter, and still keeps the text of the source for a query no parser can read.

### `test_postgresql_query_conversion.py` — 22 tests

**Purpose.** PostgreSQL to PostgreSQL, the other source P3-5 added and the one §7.1 calls
trivial. There is no dialect to convert; what there is, is a migration into another database.

**Covers.** A statement of the source, answered as it stands, with the schema left to the name
map of §7.3. The system columns — `ctid`, `xmin`, `xmax`, `cmin`, `cmax`, `tableoid` — refused,
because the migration inserted the rows again in an order of its own and every one of those
values is another one on the target, while the target accepts the statement without a word.
There is no name clash to fear: PostgreSQL refuses to create a user column of any of those
names, which is asserted as the reason. `currval()` / `lastval()` with them. The four warnings —
the catalogue, the session functions, a name inside a string literal, an explicit `COLLATE` —
and that a plain statement carries none, because a warning on every statement is a warning
nobody reads.

### `test_query_source_test.py` — 64 tests

**Purpose.** §8.1, built by P3-5: the migrator asks the SOURCE whether a statement was broken
before it read it. The rule the whole feature stands on is that it is compile only — the source
of a migration is a production database — so most of what looks like a list of strings here is
really the assertion that nothing else was sent.

**Covers.** **SQLite against a real database file**, created by the test: EXPLAIN answers the
byte code and none of the rows (asserted over a table which holds one), it still refuses a
column which is not there, and a statement with markers is compiled with NULL bound to each.
That is the one source whose driver is in the standard library, and therefore the one place the
whole route to a server's answer is proven without an engagement's infrastructure. The others at
their statements: `PREPARE` inside a read only transaction which is rolled back even when the
PREPARE failed; `SET NOEXEC ON` with the session taken back out whatever the statement did, and
the connection dropped when it cannot be — a connection left compile-only answers everything
behind it with nothing at all; `EXPLAIN` and never `EXPLAIN ANALYZE`; `Cursor.parse()` with
nothing executed; and the JDBC `prepareStatement`, preferred over the mechanism of the source
where both exist. Then the promises: a statement with parameters is not compiled with literals
in their place, `ERROR` is not `FAILED`, the connection is opened once and not per statement, a
refusal does not cost it, the run decides once whether the test can happen at all, and a
connector which declares neither a mechanism nor a reason for having none fails the suite.

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
