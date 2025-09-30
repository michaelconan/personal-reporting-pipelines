# Pipelines

This section describes the data pipelines in this project, their data sources, and how to manage data refresh patterns.

## Data Sources

The following data will be ingested from my personal systems into a BigQuery warehouse for automation and analysis.

1.  Notion
2.  HubSpot
3.  Fitbit

## Pipeline Refresh Patterns

Your pipelines support flexible refresh modes for data loading:

-   **Incremental (default)**: Only loads new/changed data since last run
-   **Full refresh**: Completely reloads all data, useful for data quality issues or schema changes

### How to Trigger Full Refresh

#### Method 1: Environment Variable Override (Global)

```bash
export FORCE_FULL_REFRESH=true
pipenv run python -m pipelines.hubspot
```

#### Method 2: Pipeline-Specific Override

```bash
# Force full refresh for HubSpot only
export PIPELINE_NAME=HUBSPOT
export HUBSPOT_FULL_REFRESH=true
pipenv run python -m pipelines.hubspot
```

#### Method 3: Direct Function Parameter

```python
from pipelines.hubspot import refresh_hubspot

# Force full refresh
refresh_hubspot(is_incremental=False)

# Use environment-based detection (default)
refresh_hubspot()  # or refresh_hubspot(is_incremental=None)
```

### Environment Variables Reference

| Variable                       | Description                              | Example                          |
| ------------------------------ | ---------------------------------------- | -------------------------------- |
| `FORCE_FULL_REFRESH`           | Global override for all pipelines        | `export FORCE_FULL_REFRESH=true`   |
| `PIPELINE_NAME`                | Pipeline identifier for specific overrides | `export PIPELINE_NAME=HUBSPOT`     |
| `{PIPELINE_NAME}_FULL_REFRESH` | Pipeline-specific full refresh flag      | `export HUBSPOT_FULL_REFRESH=true` |