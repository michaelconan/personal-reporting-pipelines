# Project Overview

This repository contains the infrastructure and workflows for a personal data platform. It leverages Google Cloud Platform services including BigQuery for data warehousing and Secret Manager for secure credential management, with automated orchestration through GitHub Actions.

## Architecture

### Data Pipeline Stack

1.  **[dlt hub](https://dlthub.com/docs/intro)** - Extract, load, and transform source data into BigQuery raw layer
2.  **[dbt core](https://docs.getdbt.com/)** - Transform raw data into analytics-ready models and views
3.  **[BigQuery](https://cloud.google.com/bigquery)** - Cloud data warehouse for storage and analysis
4.  **[GCP Secret Manager](https://cloud.google.com/secret-manager)** - Secure credential management for API keys and connections
5.  **[GitHub Actions](https://github.com/features/actions)** - Automated orchestration and scheduling of data pipelines

### Project Structure

The project follows modern data engineering best practices with clear separation of concerns:

```
├── pipelines/           # dlt data extraction pipelines
│   ├── hubspot.py      # HubSpot CRM data pipeline
│   ├── fitbit.py       # Fitbit health data pipeline
│   ├── notion.py       # Notion habits data pipeline
│   └── common/         # Shared utilities and helpers
├── dbt/                # dbt transformation models
│   └── michael/        # Personal dbt project
├── .github/            # GitHub Actions workflows
│   └── workflows/      # CI/CD and orchestration
├── scripts/            # Utility scripts and helpers
└── config/             # Configuration files and templates
```

### Naming Conventions

- **dlt pipelines**: `{source}__{entity}` (e.g., `hubspot__contacts`, `fitbit__sleep`)
- **dbt models**: `{layer}_{source}__{entity}` (e.g., `staging_hubspot__contacts`, `contacts`)
- **GitHub Actions**: `{actions}-{frequency}` (e.g., `dlt-daily`)