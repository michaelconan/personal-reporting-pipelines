# base imports
import os

# PyPI imports
import pytest
import dlt

# Ensure Google Secrets are enabled for E2E tests
# This counteracts any unit test configuration that might disable them
os.environ["PROVIDERS__ENABLE_GOOGLE_SECRETS"] = "true"
os.environ["DLT_TELEMETRY_DISABLED"] = "1"


@pytest.fixture(scope="class")
def bigquery_pipeline() -> dlt.Pipeline:
    # Test pipeline
    pipeline = dlt.pipeline(
        pipeline_name="live_e2e_test",
        destination="bigquery",
        dataset_name="live_data",
        dev_mode=True,
    )
    yield pipeline
    # Cleanup
    pipeline.drop()
