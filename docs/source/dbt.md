# dbt Models

This project uses dbt for data transformation. The dbt documentation, including model definitions and lineage, is available at the following link:

[dbt Documentation](dbt/index.html)

## dbt Development

1.  **Open dbt project** as root directory for SQLFluff and other utilities
2.  **Local profile**: Copy BigQuery service account key to `~/.dbt/profiles.yml`
3.  **Model development**: Use `pipenv run dbt run --select model_name` for iterative development
4.  **Documentation**: Generate with `pipenv run dbt docs generate` and `pipenv run dbt docs serve`

## dbt Transform Workflow

The dbt transformation pipeline is defined in `.github/workflows/dbt-transform.yml`:

-   **Daily execution**: Runs at 4 AM UTC, after the data ingestion pipelines.
-   **Triggers**: Automatically triggered after successful pipeline runs.
-   **Actions**:
    -   Runs dbt models.
    -   Runs dbt tests.
    -   Generates dbt documentation.
-   **Commands**: Uses `make install` and `pipenv` for dbt operations.