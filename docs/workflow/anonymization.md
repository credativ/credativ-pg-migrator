# Data Anonymization Workflow

The `credativ-pg-migrator` features a standalone, pluggable anonymization module. Configured via the `anonymization` workflow, this executes a dedicated PostgreSQL-to-PostgreSQL migration pipeline that obscures sensitive data in-flight. 

It natively supports Python in-memory transformations (leveraging optional libraries like `Faker` and `Mimesis`) and can also offload logic directly to the PostgreSQL engine using `__RAW_SQL__` tokens.

## Current Limitations & Architecture

Currently, the anonymization workflow only supports **PostgreSQL-to-PostgreSQL migrations**. 

This is because the anonymizer is built as a standalone extraction and loading pipeline that explicitly bypasses the heavy ETL data transformation pipelines (like data type casting, date formatting, and LOB extraction) required for legacy databases like Informix, Oracle, or Sybase. It utilizes PostgreSQL-specific server-side cursors for efficient data extraction and directly injects raw PostgreSQL functions into the target database. 

To support all source databases in the future, the anonymizer logic would need to be refactored from an isolated workflow into a "middleware transformation step" embedded within each database connector's core `migrate_table` data pipeline.

## Configuration
To enable the anonymization workflow, set the workflow type in your configuration file:
```yaml
migration:
  workflow: anonymization
```

You can define rules either explicitly per table and column or via regular expressions.

```yaml
anonymization:
  tables:
    public.people:
      name:
        method: faker_name
  regex_mappings:
    - table_pattern: ".*"
      column_pattern: "(?i).*password.*"
      method: deterministic_hash_mask
      params:
        salt: "my_secret_salt"
```

## Available Anonymization Methods

The following methods are pre-registered and ready for use in your configuration files.

### Library-Driven Generation (Requires `Faker` / `Mimesis`)

1. **`faker_name`**
   - **Description:** Generates a realistic fake name using the `Faker` library (default locale: `de_DE`).
   - **Parameters:** None.

2. **`faker_email`**
   - **Description:** Generates a realistic fake email address using the `Faker` library.
   - **Parameters:** None.

3. **`mimesis_address`**
   - **Description:** Generates realistic addresses using the `mimesis` library.
   - **Parameters:**
     - `part` (optional): Determines what part of the address to return. Options include `full`, `street_address`, `street_name_house_number`, `street_name`, `house_number`, `postal_code`, `city`. Default is `full`.

4. **`custom_iban`**
   - **Description:** Generates a valid fake IBAN format using the `Faker` library.
   - **Parameters:** None.

5. **`custom_german_bank`**
   - **Description:** Generates fake German Bank details (BLZ and Kontonummer).
   - **Parameters:**
     - `part` (optional): `blz`, `kontonummer`, or `combined`. Default is `combined`.

### Algorithmic Transformations

6. **`consistent_integer_mask`**
   - **Description:** Replaces an integer with a random integer, but maintains an in-memory cache so that the same input integer will always map to the same randomized integer during the run.
   - **Parameters:**
     - `domain` (optional): An internal cache key namespace. Default is `default`.

7. **`static_mask`**
   - **Description:** Replaces every character of the original value with a repeating static mask character. The output string matches the length of the original string.
   - **Parameters:**
     - `mask_char` (optional): The character to repeat. Default is `X`.

8. **`deterministic_hash_mask`**
   - **Description:** Generates a deterministic SHA256 hash of the input value.
   - **Parameters:**
     - `salt` (optional): A string to append to the value before hashing.
     - `out_type` (optional): `string` for the hex digest, or `int` for a numerical representation. Default is `string`.

9. **`numeric_noise`**
   - **Description:** Adjusts a numeric value by adding or subtracting a randomized percentage of the original value, obscuring the exact number while keeping it proportionally accurate.
   - **Parameters:**
     - `ratio` (optional): The maximum variance ratio. Default is `0.20` (±20%).

10. **`partial_mask`**
    - **Description:** Masks the middle of a string while leaving a specified number of prefix or suffix characters visible.
    - **Parameters:**
      - `prefix_len` (optional): Number of characters to leave visible at the start. Default is `0`.
      - `suffix_len` (optional): Number of characters to leave visible at the end. Default is `0`.
      - `mask_str` (optional): The string to insert in the middle. Default is `***`.

### PostgreSQL Native Delegation

11. **`postgres_anon_native`**
    - **Description:** Bypasses Python-side generation by intercepting the transformation and passing a raw SQL function call down to PostgreSQL. Useful for integrating with server-side extensions like `postgresql_anonymizer`.
    - **Parameters:**
      - `func_name` (optional): The SQL function to call. Default is `anon.fake_city`.
      - `pass_original` (optional): If `true`, injects the original column value into the function call via string formatting (`%s`). Default is `false`.

## Example Summary Output

If the `show_anonymization_examples` configuration limit is greater than 0, the final migrator summary will display random cross-database validation examples:

```text
================================================================================
                       CREDATIV PG-MIGRATOR SUMMARY                             
================================================================================

[ DATABASE CONTEXT ]
Source: omdb, schema: public (postgresql)
Target: omdb, schema: public (postgresql)
Workflow: anonymization

[ TIMING & EXECUTION PROFILES ]
--------------------------------------------------------------------------------
Phase / Step                                 | Duration       | Start Time
--------------------------------------------------------------------------------
Planner                                      | 0:00:03.46     | 11:25:30
  Anonymization workflow                     | -              | 11:25:30
Orchestrator                                 | -              | 11:25:33
  anonymization data copy                    | 0:02:47.87     | 11:25:33

[ OBJECTS MIGRATION RESULTS ]
--------------------------------------------------------------------------------
Object Type              | Source | Success | Failed | Details
--------------------------------------------------------------------------------
User Defined Types       |      0 |       0 |      0 | 
Domains                  |      0 |       0 |      0 | 
Sequences                |      0 |       0 |      0 | 
Tables                   |     24 |      24 |      0 | Empty: 1, With Data: 23
Table Partitions         |      0 |       0 |      0 | 
Columns                  |      0 |       0 |      0 | 
Altered Columns          |      0 |       - |      - | 
Indexes                  |      0 |       0 |      0 | 
Constraints              |      0 |       0 |      0 | 
Functions / Procedures   |      0 |       0 |      0 | 
Triggers                 |      0 |       0 |      0 | 
Views                    |      0 |       0 |      0 | 
Aliases                  |      0 |       0 |      0 | 

[ DATA MIGRATION RESULTS ]
--------------------------------------------------------------------------------
Total Tables Processed   : 23
Empty Tables (0 rows)    : 0
Tables with Data         : 23
Fully Migrated (Matched) : 23
Row Count Mismatches     : 0

Biggest Successfully Migrated Tables (Top 10):
Table Name                          |       Row Count |  Time Spent (s)
--------------------------------------------------------------------------------
public.casts                        |       1,253,498 |           45.80
public.movie_keywords               |         408,524 |           15.12
public.movie_links                  |         349,858 |           17.24
public.movie_aliases_iso            |         304,077 |           15.48
public.people                       |         294,184 |          161.88
public.movies                       |         277,249 |           22.54
public.movie_categories             |         227,189 |            8.70
public.people_links                 |         213,028 |           10.02
public.movie_languages              |         144,560 |            4.74
public.movie_countries              |         103,011 |            2.94

[ ANONYMIZATION WORKFLOW RESULTS ]
--------------------------------------------------------------------------------
Anonymized 6 columns in 3 tables.

Top Tables with Most Anonymized Columns:
1. public.movies (4 columns anonymized)
   Column Name | Data Type | Method
   ------------+-----------+---------------------
   budget      | numeric   | numeric_noise
   homepage    | text      | partial_mask
   name        | text      | deterministic_hash_mask
   revenue     | numeric   | consistent_integer_mask

   Examples (Original => Anonymized):
   Row 1:
     - budget: 'None' => 'None'
     - homepage: 'None' => 'None'
     - name: 'Homme de peu de foi' => '3f68fbf8ee5ced2a1a725a1c37a803...'
     - revenue: 'None' => 'None'
   Row 2:
     - budget: 'None' => 'None'
     - homepage: 'None' => 'None'
     - name: 'The Name' => 'f6317bf7fecdf94296466d1c5d7798...'
     - revenue: 'None' => 'None'
   Row 3:
     - budget: 'None' => 'None'
     - homepage: 'None' => 'None'
     - name: 'The Labrador' => 'a5695bb0c3a99c6f353ac2aeed86e4...'
     - revenue: 'None' => 'None'
   Row 4:
     - budget: 'None' => 'None'
     - homepage: 'None' => 'None'
     - name: 'All Dressed Up and Nowhere to ...' => '005146aa88460049863d49732daa6a...'
     - revenue: 'None' => 'None'
   Row 5:
     - budget: 'None' => 'None'
     - homepage: 'None' => 'None'
     - name: 'Smoking in the Girls' Room' => '8722945cea77f7e2216b2376dbb793...'
     - revenue: 'None' => 'None'

2. public.people (1 columns anonymized)
   Column Name | Data Type | Method
   ------------+-----------+---------------------
   name        | text      | faker_name

   Examples (Original => Anonymized):
   Row 1:
     - name: 'Maximum Indifference' => 'Dr. Luka Winkler'
   Row 2:
     - name: 'Amanda Knapic' => 'Abraham Striebitz'
   Row 3:
     - name: 'Mark Clare' => 'Marija Gierschner B.Eng.'
   Row 4:
     - name: 'Meryl Swartz' => 'Hans-Martin Anders'
   Row 5:
     - name: 'Tamara Smart' => 'Anna Lorch'

3. public.trailers (1 columns anonymized)
   Column Name | Data Type | Method
   ------------+-----------+---------------------
   key         | text      | static_mask

   Examples (Original => Anonymized):
   Row 1:
     - key: 'ZOhSbNJ7MqI' => 'YYYYYYYYYYY'
   Row 2:
     - key: 'q34GSXHvJvI' => 'YYYYYYYYYYY'
   Row 3:
     - key: 'Mc4sz6neHDs' => 'YYYYYYYYYYY'
   Row 4:
     - key: 'FQRgJTEw_OA' => 'YYYYYYYYYYY'
   Row 5:
     - key: 'atbuBxDO1Go' => 'YYYYYYYYYYY'
================================================================================
```
