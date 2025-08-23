# e2e tests for pipelines
# NOTE: these tests require credentials to run
# and will be skipped by default
import pytest


# label as end-to-end and disable response mock plugin
pytestmark = [pytest.mark.e2e, pytest.mark.withoutresponses]


class TestPipelines:

    # Define limited date interval to run tests
    REFRESH_ARGS = {
        "is_incremental": False,
        "initial_date": "2025-01-01",
        "end_date": "2025-06-30",
    }

    def test_notion_refresh(self, bigquery_pipeline):

        # Delayed import to capture DBT target variable
        from pipelines.notion import refresh_notion

        # GIVEN

        # WHEN
        # Run the pipeline
        info = refresh_notion(**self.REFRESH_ARGS, pipeline=bigquery_pipeline)

        # THEN
        # Validate jobs were successful
        assert info.has_failed_jobs is False

    def test_hubspot_refresh(self, bigquery_pipeline):
        """
        Test a full refresh of the HubSpot pipeline.
        """
        from pipelines.hubspot import refresh_hubspot

        # WHEN
        info = refresh_hubspot(**self.REFRESH_ARGS, pipeline=bigquery_pipeline)

        # THEN
        # Validate jobs were successful
        assert info.has_failed_jobs is False

    def test_fitbit_refresh(self, bigquery_pipeline):

        # Delayed import to capture DBT target variable
        from pipelines.fitbit import refresh_fitbit

        # GIVEN

        # WHEN
        # Run the pipeline
        info = refresh_fitbit(**self.REFRESH_ARGS, pipeline=bigquery_pipeline)

        # THEN
        # Validate jobs were successful
        assert info.has_failed_jobs is False
