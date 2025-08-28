"""Configuration and fixtures for DLT end-to-end tests.

This module configures end-to-end testing environment with real external
services like Google Cloud and BigQuery.
"""

# base imports
import os

# PyPI imports
import pytest
import dlt

# Ensure Google Secrets are enabled for E2E tests
# This counteracts any unit test configuration that might disable them
os.environ["PROVIDERS__ENABLE_GOOGLE_SECRETS"] = "true"
os.environ["PROVIDERS__GOOGLE_SECRETS__ONLY_SECRETS"] = "true"
os.environ["PROVIDERS__GOOGLE_SECRETS__LIST_SECRETS"] = "true"
os.environ["PROVIDERS__GOOGLE_SECRETS__ONLY_TOML_FRAGMENTS"] = "false"
os.environ["DLT_TELEMETRY_DISABLED"] = "1"


@pytest.fixture(scope="module", autouse=True)
def check_config():
    """Verify DLT configuration for end-to-end tests.

    Ensures that Google Secrets provider is properly configured
    and available for accessing real credentials during E2E testing.
    """
    # Get providers of dlt secrets
    providers = dlt.secrets.config_providers
    # Environment, Secrets.toml, Google Secrets
    assert len(providers) == 3
    # Allow direct value secrets (like Fitbit refresh token)
    assert providers[2].only_toml_fragments is False


@pytest.fixture(scope="class")
def bigquery_pipeline() -> dlt.Pipeline:
    """Pytest fixture providing a BigQuery pipeline for E2E testing.

    Creates a DLT pipeline configured to use BigQuery as the destination
    for end-to-end testing with real cloud services. The pipeline is
    automatically cleaned up after each test class.

    Yields:
        dlt.Pipeline: Configured DLT pipeline for E2E testing.
    """
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
