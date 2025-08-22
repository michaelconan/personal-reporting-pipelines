import pytest
import dlt


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
