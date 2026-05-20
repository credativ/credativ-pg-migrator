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
      - `args` (optional): Explicit string of arguments to pass to the function.
      - `pass_original` (optional): If `true`, injects the original column value into the function call via string formatting (`%s`). Default is `false`.
