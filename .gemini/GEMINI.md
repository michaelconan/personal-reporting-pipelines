# Personal Reporting Data Platform - GEMINI Context

## Project Overview

This is a personal data integration and analytics platform that ingests data from multiple sources (Notion, HubSpot, Fitbit) into BigQuery using dlt (data load tool) and transforms it with dbt (data build tool). The platform is orchestrated through GitHub Actions workflows.

**Tech Stack:**
- **Data Ingestion**: dlt (Python library for data loading)
- **Data Transformation**: dbt core
- **Data Warehouse**: Google BigQuery
- **Orchestration**: GitHub Actions
- **Secret Management**: GCP Secret Manager
- **Development**: Python 3.12, pipenv, VSCode Dev Containers

## Project Structure Standards

### File Organization
```
├── pipelines/              # dlt data extraction pipelines
│   ├── common/            # Shared utilities and helpers
│   │   ├── utils.py       # Common pipeline utilities
│   │   └── dlt_rest.py    # REST API helpers for dlt
│   ├── hubspot.py         # HubSpot CRM data pipeline
│   ├── fitbit.py          # Fitbit health data pipeline
│   ├── notion.py          # Notion habits data pipeline
│   └── __init__.py        # Schema/environment configuration
├── dbt/                   # dbt transformation models
│   ├── models/           # SQL transformation models
│   │   ├── staging/      # Raw data staging models
│   │   └── intermediate/ # Business logic transformations
│   ├── .sqlfluff         # SQL linting configuration
│   └── dbt_project.yml   # dbt project configuration
├── tests/                 # Python test suite
├── .github/workflows/     # GitHub Actions orchestration
├── .devcontainer/        # VSCode Dev Container config
└── .gemini/              # GEMINI AI context files
```

### Naming Conventions

**dlt Pipelines:**
- Format: `{source}__{entity}` (e.g., `hubspot__contacts`, `fitbit__sleep`)
- Function names: `refresh_{source}()` (e.g., `refresh_hubspot()`)
- Pipeline names: `{source}_{type}_pipeline` (e.g., `hubspot_crm_pipeline`)

**dbt Models:**
- Staging: `stg_{source}__{entity}` (e.g., `stg_hubspot__contacts`)
- Intermediate: `int_{domain}_{description}` (e.g., `int_habits_unpivoted`)
- Final tables: `{entity}` (e.g., `contacts`, `engagements`)

**GitHub Actions:**
- Format: `{action}-{frequency}` (e.g., `dlt-daily`, `pipeline-tests`)
- Workflow files: `{pipeline-name}.yml`

**Environment Variables:**
- Global refresh: `FORCE_FULL_REFRESH`
- Pipeline-specific: `{PIPELINE_NAME}_FULL_REFRESH`
- Schema names: `RAW_SCHEMA`, `DBT_SCHEMA_NAME`

## Code Standards & Best Practices

### Python Standards

**Code Style:**
- **Black** formatting (line length: 100 characters)
- **flake8** linting (max-line-length: 100)
- **mypy** type checking
- **bandit** security scanning
- **pre-commit** hooks for automated checks

**Import Organization:**
```python
# Base/standard library imports
from logging import getLogger, Logger
from typing import Optional

# Third-party imports (PyPI)
import pendulum
import dlt

# Local imports
from pipelines.common.utils import get_refresh_mode
```

**Function Patterns:**
- All pipeline functions accept `is_incremental: Optional[bool] = None`
- Use environment-based refresh mode detection when `is_incremental` is None
- Log refresh mode decisions with `log_refresh_mode()`
- Return dlt pipeline info objects for debugging

**Error Handling:**
- Use structured logging with contextual information
- Handle API rate limits and retry logic in dlt sources
- Validate data quality with dbt tests

### SQL/dbt Standards

**SQL Style (SQLFluff Configuration):**
- **Dialect**: BigQuery
- **Keywords**: lowercase
- **Identifiers**: lowercase with underscores
- **Line length**: 80 characters
- **Indentation**: 4 spaces
- **Comma placement**: trailing
- **Aliasing**: explicit for tables and columns

**dbt Model Patterns:**
```sql
-- Standard staging model structure
with source_data as (
    select
        id,
        original_field as renamed_field,
        created_at,
        updated_at,
        row_number() over (
            partition by id
            order by updated_at desc
        ) as row_num
    from {{ source('source_name', 'table_name') }}
)

select
    id,
    renamed_field,
    created_at,
    updated_at
from source_data
where row_num = 1
```

**dbt Configuration Standards:**
- **Staging models**: materialized as views
- **Intermediate models**: materialized as ephemeral
- **Final models**: materialized as tables (default)
- Always include comprehensive testing in schema.yml files

### Testing Standards

**Python Testing (pytest):**
- Test file prefix: `test_`
- Integration tests for each pipeline: `test_{source}_load()`
- Use `is_incremental=False` for full refresh testing
- Mock external API calls for unit tests
- Coverage reporting with pytest-cov

**dbt Testing:**
- **Source tests**: not_null, unique, relationships, accepted_values
- **Model tests**: Custom business logic validation
- **Advanced tests**: Use dbt-expectations package for data quality
- **Array validation**: Custom tests for JSON array fields

## Data Architecture Patterns

### Schema Structure

**Raw Layer (`raw` schema):**
- Direct API data dumps from dlt pipelines
- Table names: `{source}__{entity}` (e.g., `hubspot__contacts`)
- Minimal transformation, preserve original structure

**Staging Layer (`staging` schema):**
- Clean, typed, deduplicated data
- Consistent naming conventions
- Row-level deduplication using window functions
- Source-specific business logic

**Analytics Layer (`reporting` schema):**
- Business-friendly models
- Cross-source joins and aggregations
- Analytics-ready datasets

### Incremental Loading Patterns

**Environment-Based Control:**
```python
# Global override
export FORCE_FULL_REFRESH=true

# Pipeline-specific override  
export PIPELINE_NAME=HUBSPOT
export HUBSPOT_FULL_REFRESH=true
```

**Pipeline Implementation:**
- Default to incremental loading (`is_incremental=True`)
- Use `get_refresh_mode()` for environment-based detection
- Log refresh decisions with context
- Handle API limitations (e.g., HubSpot 30-day limit)

### Secret Management

**GCP Secret Manager Integration:**
- Store API keys in TOML format:
  ```toml
  [sources.hubspot]
  api_key = "your-key"
  
  [sources.fitbit]
  client_id = "your-id"
  client_secret = "your-secret"
  ```
- OAuth refresh tokens in dedicated secrets
- Service account JSON for GitHub Actions

## Development Workflow

### Local Development Setup
1. Use VSCode Dev Container for consistent environment
2. Run `make install` to install dependencies
3. Configure GCP authentication: `gcloud auth application-default login`
4. Set environment variables for local testing

### Testing Workflow
```bash
# Run full test suite with coverage
make test

# Run specific pipeline tests
pipenv run pytest tests/test_load_pipelines.py::test_hubspot_load -v

# dbt testing
cd dbt && pipenv run dbt test --target dev
```

### Code Quality Checks
- Pre-commit hooks run automatically
- GitHub Actions run tests on PRs
- SQLFluff validates SQL formatting
- Coverage reports uploaded as artifacts

## GitHub Actions Orchestration

### Pipeline Scheduling
- **HubSpot**: Daily at 2 AM UTC
- **Fitbit**: Daily at 3 AM UTC  
- **Notion**: Weekly on Sundays at 9 AM UTC
- **dbt Transform**: Daily at 4 AM UTC (after ingestion)

### Workflow Patterns
- Manual triggers with `workflow_dispatch`
- Full refresh option via workflow inputs
- Artifact uploads for debugging (dlt state files)
- Comprehensive error handling and notifications

### Environment Variables
- `GOOGLE_APPLICATION_CREDENTIALS`: Service account JSON
- Pipeline-specific environment variables passed through secrets

## Common Issues & Solutions

### dlt Pipeline Issues
- **State management**: dlt stores state in `~/.dlt/` directory
- **API limits**: Handle rate limiting with backoff strategies
- **Schema evolution**: Use `write_disposition="merge"` for schema changes

### dbt Model Issues  
- **Incremental models**: Use proper `is_incremental()` logic
- **BigQuery specifics**: Handle nested JSON and arrays appropriately
- **Testing failures**: Always validate referential integrity

### GitHub Actions Issues
- **Authentication**: Ensure GCP service account has proper permissions
- **Dependencies**: Use `make install` consistently across workflows
- **Artifacts**: Upload state files for debugging pipeline issues

## Code Review Guidelines

### Python Code Review
- Verify proper error handling and logging
- Check incremental vs full refresh logic
- Validate environment variable handling
- Ensure proper typing and documentation

### SQL Code Review  
- Verify SQLFluff compliance
- Check for proper aliasing and formatting
- Validate business logic in transformations
- Ensure comprehensive testing coverage

### Architecture Review
- Validate data lineage and dependencies
- Check for proper separation of concerns
- Verify scalability and performance considerations
- Ensure security best practices

## Performance & Monitoring

### dlt Performance
- Monitor pipeline execution times
- Track API rate limit usage
- Optimize batch sizes for BigQuery loading

### dbt Performance  
- Monitor model compilation and execution times
- Use incremental models for large datasets
- Optimize BigQuery partitioning and clustering

### Data Quality Monitoring
- dbt test results tracking  
- Pipeline success/failure alerts
- Data freshness monitoring
- Schema drift detection

---

*Last updated: August 2025*
*For questions or updates to this context, see the project README.md or GitHub discussions.*
