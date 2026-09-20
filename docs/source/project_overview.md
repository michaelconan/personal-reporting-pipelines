# Project Overview

This repository contains the infrastructure and workflows for a personal data platform. It uses Databricks (Unity Catalog) as the primary data warehouse for raw data ingestion, storage, and transformations, with Google BigQuery retained as a rollback path. GCP Secret Manager and 1Password are used for secure credential management, with automated orchestration through GitHub Actions.

## Architecture

### Data Pipeline Stack

1.  **[dlt hub](https://dlthub.com/docs/intro)** - Extract, load, and transform source data into Databricks raw layer (Unity Catalog)
2.  **[dbt core](https://docs.getdbt.com/)** - Transform raw data into analytics-ready models and views
3.  **[Databricks](https://www.databricks.com/)** - Lakehouse platform for storage and analysis (Unity Catalog)
4.  **[Google BigQuery](https://cloud.google.com/bigquery)** - Retained as rollback target for dbt transformations
5.  **[GCP Secret Manager / 1Password](https://cloud.google.com/secret-manager)** - Secure credential management for API keys and connections
6.  **[GitHub Actions](https://github.com/features/actions)** - Automated orchestration and scheduling of data pipelines

### Project Structure

The project follows modern data engineering best practices with clear separation of concerns:

```
├── pipelines/           # dlt data extraction pipelines
│   ├── hubspot.py      # HubSpot CRM data pipeline
│   ├── notion.py       # Notion habits data pipeline
│   ├── google_health.py # Google Health data pipeline
│   └── runner.py       # Pipeline runner CLI
├── dbt/                # dbt transformation models (personal project)
├── .github/            # GitHub Actions workflows
│   └── workflows/      # CI/CD and orchestration
├── scripts/            # Utility scripts and helpers
│   └── fixtures/       # Mock data export and scrubbing scripts
└── tests/              # Unit, integration, and e2e tests
```

### Naming Conventions

- **dlt pipelines**: `{source}__{entity}` (e.g., `hubspot__contacts`, `google_health__steps`)
- **dbt models**: `{layer}_{source}__{entity}` (e.g., `staging_hubspot__contacts`, `core_habit_events`)
- **GitHub Actions**: `{action}-{frequency}` (e.g., `dlt-daily`, `dbt-weekly`)