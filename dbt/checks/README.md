# Custom dbt-bouncer checks

This folder holds custom [dbt-bouncer](https://github.com/godatadriver/dbt-bouncer)
`manifest_checks` copied from the `RMP-Analysis` project's
`ai_file_review/bouncer_checks` governance setup, adapted for use in this
repo.

> **Note:** These checks are **not yet wired into dbt-bouncer
> configuration**. They are staged here for review before being referenced
> from a `dbt-bouncer.yml`/`bouncer_checks` config (e.g. via a
> `custom_checks_dir` or equivalent setting).

## Layout

```
dbt/checks/
└── manifest/
    ├── check_data_tests.py
    ├── check_seeds.py
    ├── check_unit_tests.py
    └── model_naming.py
```

## Checks

### `check_data_tests.py`

- **`check_data_test_description_populated`** — Fails if a singular SQL data
  test under `tests/` does not have a populated description (default minimum
  length: 20 characters). Ensures every custom data test documents the
  business rule it enforces, keeping test intent clear for reviewers.

### `check_seeds.py`

- **`check_seed_has_unique_test`** — Fails if a seed does not have at least
  one test asserting uniqueness of a column (or combination of columns).
  Accepts `unique`, `dbt_utils.unique_combination_of_columns`, and
  `dbt_expectations.expect_compound_columns_to_be_unique` by default
  (configurable via `accepted_uniqueness_tests`). Prevents duplicate rows in
  seed data from silently fanning out downstream joins.

### `check_unit_tests.py`

- **`check_model_unit_test_location`** — Fails if a model's dbt unit tests
  are defined in a YAML file outside the model's own directory. Keeps unit
  test ownership co-located with the model it tests.
- **`check_unit_test_file_name`** — Fails if a unit-test YAML file's name
  does not match a configured `file_name_pattern` regex. Enforces consistent
  unit-test file naming conventions.

### `model_naming.py`

- **`check_model_property_file_name`** — Fails if a model's properties
  (`.yml`) file name does not match a configured `file_name_pattern` regex.
  Enforces consistent property-file naming conventions.

## Usage (once enabled)

Each check can be added to a dbt-bouncer config under `manifest_checks`,
e.g.:

```yaml
manifest_checks:
  - name: check_seed_has_unique_test
    include: ^seeds/
  - name: check_data_test_description_populated
    min_description_length: 20
  - name: check_model_unit_test_location
    include: ^models/
  - name: check_model_property_file_name
    file_name_pattern: ^_.*\.yml$
  - name: check_unit_test_file_name
    file_name_pattern: ^_.*\.yml$
```

Bouncer must also be configured to discover these custom checks (e.g. a
`custom_checks_dir: dbt/checks` setting, or equivalent), which is
intentionally **not** configured yet — this will be a follow-up change once
the checks have been reviewed for this project.
