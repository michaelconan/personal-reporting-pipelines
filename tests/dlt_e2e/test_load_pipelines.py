"""End-to-end tests for data loading pipelines.

This module contains integration tests that run the complete pipeline
from data extraction to loading into BigQuery, using real external APIs
and cloud services.
"""

# e2e tests for pipelines

import pytest
import dlt


# label as end-to-end and disable response mock plugin
pytestmark = [pytest.mark.e2e]


class TestPipelines:
    """End-to-end test suite for all data loading pipelines.

    This class contains tests that verify complete pipeline functionality
    from source to destination using real external services.
    """

    # Define limited date interval to run tests
    REFRESH_ARGS = {
        "is_incremental": False,
        "initial_date": "2025-01-01",
        "end_date": "2025-06-30",
    }

    def test_notion_refresh(self, bigquery_pipeline):
        """Test end-to-end refresh of the Notion habits pipeline.

        Args:
            bigquery_pipeline: BigQuery pipeline fixture for E2E testing.
        """
        # Delayed import to capture DBT target variable
        from pipelines.runner import refresh_pipeline

        # WHEN
        # Run the pipeline
        info = refresh_pipeline("notion", **self.REFRESH_ARGS, pipeline=bigquery_pipeline)

        # THEN
        # Validate jobs were successful
        assert info.has_failed_jobs is False

    def test_hubspot_refresh(self, bigquery_pipeline):
        """Test end-to-end refresh of the HubSpot CRM pipeline.

        Args:
            bigquery_pipeline: BigQuery pipeline fixture for E2E testing.
        """
        from pipelines.runner import refresh_pipeline

        # WHEN
        info = refresh_pipeline("hubspot", **self.REFRESH_ARGS, pipeline=bigquery_pipeline)

        # THEN
        # Validate jobs were successful
        assert info.has_failed_jobs is False

    def test_google_health_refresh(self, bigquery_pipeline):
        """Test end-to-end refresh of the Google health pipeline.

        Args:
            bigquery_pipeline: BigQuery pipeline fixture for E2E testing.
        """
        # Delayed import to capture DBT target variable
        from pipelines.runner import refresh_pipeline

        # GIVEN
        secrets = dlt.secrets
        # secrets.config_providers[2].only_toml_fragments = False
        secrets["sources.google_health.refresh_token"]

        # WHEN
        # Run the pipeline
        info = refresh_pipeline("google_health", **self.REFRESH_ARGS, pipeline=bigquery_pipeline)

        # THEN
        # Validate jobs were successful
        assert info.has_failed_jobs is False

    def test_fitbit_refresh(self, bigquery_pipeline):
        """Test end-to-end refresh of the Fitbit health pipeline.

        Args:
            bigquery_pipeline: BigQuery pipeline fixture for E2E testing.
        """
        # Delayed import to capture DBT target variable
        from pipelines.runner import refresh_pipeline

        # GIVEN
        secrets = dlt.secrets
        # secrets.config_providers[2].only_toml_fragments = False
        secrets["sources.fitbit.refresh_token"]

        # WHEN
        # Run the pipeline
        info = refresh_pipeline("fitbit", **self.REFRESH_ARGS, pipeline=bigquery_pipeline)

        # THEN
        # Validate jobs were successful
        assert info.has_failed_jobs is False
