# Tests

Unit, integration, and end-to-end tests should be developed for each component of the project.

## data load tool (dlt)

### Unit Tests

Individual unit tests should be created for each step in the [dlt pipeline](https://dlthub.com/docs/reference/explainers/how-dlt-works#the-three-phases) (extract, normalize, load). Sources (e.g., APIs) should be mocked and loaded to local duckdb destinations for full offline support.

#### REST APIs

Mock endpoints should be used in test cases to validate pagination and incremental loading.