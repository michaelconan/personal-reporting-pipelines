# dbt Models

This project uses dbt for data transformation. The dbt documentation, including model definitions and lineage, is available at the following link:

[dbt Documentation](/dbt)

## dbt Development

1.  **Open dbt project** as root directory for SQLFluff and other utilities
2.  **Local profile**: Copy BigQuery service account key to `~/.dbt/profiles.yml`
3.  **Model development**: Use `uv run dbt run --select model_name` for iterative development
4.  **Documentation**: Generate with `uv run dbt docs generate` and `uv run dbt docs serve`

## dbt Transform Workflow

The dbt transformation pipeline is defined in `.github/workflows/dbt-transform.yml`:

-   **Daily execution**: Runs at 4 AM UTC, after the data ingestion pipelines.
-   **Triggers**: Automatically triggered after successful pipeline runs.
-   **Actions**:
    -   Runs dbt models.
    -   Runs dbt tests.
    -   Generates dbt documentation.
-   **Commands**: Uses `make install` and `pipenv` for dbt operations.