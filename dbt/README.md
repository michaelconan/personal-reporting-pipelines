# Personal Reporting

This dbt project is designed to transform and model data from various sources, including HubSpot, Notion, and Fitbit. The project follows the dbt best practices for structuring a dbt project, with clear separation between staging, intermediate, and mart layers.

## Data Ingestion

The raw data is ingested using `dlt` (data load tool), which means that the raw tables have a specific structure that the staging models are designed to handle. The raw data is loaded into the `raw` schema in the production environment (BigQuery), and into the `main` schema in the local testing environment (DuckDB).

## Layers

Model layers have been implemented as recommended by [dbt's project structure guide](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview).

| Layer        | Description                               | Scope                                                  | Notes                                                              |
|--------------|-------------------------------------------|--------------------------------------------------------|--------------------------------------------------------------------|
| Staging      | Foundational models organised by source   | Renaming, type casting, basic computations, categorising | Standardise names to snake case, deduplicate for change data loading |
| Intermediate | Apply complex transformations by focus area | Structural simplification, re-graining, isolating complex operations |                                                                    |
| Marts        | Entity or concept layer, denormalised     | Standard entity concepts, built wide, and extended thoughtfully |                                                                    |

## Local Testing

This project is set up with a local testing environment using DuckDB. To run the project locally, you need to:
1.  Install the dependencies from `Pipfile` using `pipenv install --dev`.
2.  Run `dbt seed --select tag:local` to load the mock data into your local DuckDB database.
3.  Run `dbt build` to execute the models and tests.

The local testing setup uses a `profiles.yml` file located in `dbt/profiles`, which is configured to use a local DuckDB database file (`dbt.duckdb`).

## Data Quality

Data quality is enforced through a series of data tests defined in the `properties.yml` files. These tests include:
-   Standard generic tests (e.g., `unique`, `not_null`, `relationships`).
-   Tests from the `dbt_expectations` package.
-   Custom generic tests.

All data tests are run as part of the `dbt build` command.
