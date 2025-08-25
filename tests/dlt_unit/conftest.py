# base imports
import os
import json
from typing import Any

# PyPI imports
import pytest
import dlt

# For responses fixture
import pytest_responses

MOCK_FOLDER = "tests/mock_data"


# This environment variable is set to disable the Google Secrets provider for all dlt unit tests.
# It needs to be set before any dlt modules are imported, which is why it's at the top of this file.
# Only applies to unit tests - E2E tests should not be affected
os.environ["PROVIDERS__ENABLE_GOOGLE_SECRETS"] = "false"
os.environ["DLT_TELEMETRY_DISABLED"] = "1"


def sample_resource(
    file_name: str,
    fallback: str = None,
    data_selector: str = None,
    resource_configs: dict = {},
) -> Any:
    @dlt.resource(**resource_configs)
    def sample_resource():
        source = sample_data(file_name=file_name, fallback=fallback)
        if data_selector:
            source = source[data_selector]
        return source

    return sample_resource


def sample_data(
    file_name: str,
    fallback: str = None,
) -> dict:
    file = os.path.join(MOCK_FOLDER, file_name)
    if not os.path.exists(file) and fallback is not None:
        file = os.path.join(MOCK_FOLDER, fallback)
    with open(file, "r") as f:
        return json.load(f)


def sample_response(file_name: str) -> tuple[int, dict, str]:
    with open(os.path.join(MOCK_FOLDER, file_name), "r") as f:
        return (200, {}, f.read())


@pytest.fixture(scope="class")
def duckdb_pipeline() -> dlt.Pipeline:
    # Test pipeline
    pipeline = dlt.pipeline(
        pipeline_name="local_unit_test",
        destination="duckdb",
        dataset_name="local_data",
        dev_mode=True,
    )
    yield pipeline
    # Cleanup
    pipeline.drop()
