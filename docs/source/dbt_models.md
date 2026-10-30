# dbt Models

This project uses dbt for data transformation. The dbt documentation, including model definitions and lineage, is available at the following link:

[dbt Documentation](/dbt)

## dbt Development

1.  **Install tools and dependencies**: `mise install` and `mise run setup`
2.  **Model development**: Use `mise run dbt -- run --target mock --select model_name`
3.  **Databricks development**: Use `mise run dbt-build` (starts the warehouse; uses `DBT_TARGET`, default `dev`)
4.  **Documentation**: Generate with `mise run docs`

## dbt Transform Workflow

The dbt transformation pipeline is defined in `.github/workflows/run-transforms.yml`:

-   **Weekly execution**: Runs Mondays at 6 AM UTC, after the data ingestion pipelines.
-   **Triggers**: Scheduled Mondays at 6 AM UTC and manually via `workflow_dispatch`; no automatic chaining from ingestion.
-   **Actions**:
    -   Runs dbt models.
    -   Runs dbt tests.
    -   Generates dbt documentation.
-   **Commands**: Uses mise setup and tasks, with credentials supplied by a 1Password Environment.
