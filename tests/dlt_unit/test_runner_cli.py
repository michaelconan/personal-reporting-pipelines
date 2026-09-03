"""Unit tests for the central pipeline runner and CLI interface."""

from unittest.mock import MagicMock, patch
import pytest

from pipelines.run_pipeline import parse_select, main
from pipelines.runner import refresh_pipeline, PIPELINE_CONFIG
from pipelines import RAW_SCHEMA


def test_parse_select():
    """Test the parse_select helper function."""
    assert parse_select(None) is None
    assert parse_select([]) is None
    assert parse_select([""]) is None

    # Simple list
    assert parse_select(["hubspot__contacts"]) == ["hubspot__contacts"]

    # Comma-separated strings
    assert parse_select(["hubspot__contacts,hubspot__companies"]) == [
        "hubspot__contacts",
        "hubspot__companies",
    ]

    # Repeated list items with whitespace
    assert parse_select(
        [" hubspot__contacts ,  hubspot__companies ", "  notion__daily_habits  "]
    ) == [
        "hubspot__contacts",
        "hubspot__companies",
        "notion__daily_habits",
    ]


@patch("pipelines.run_pipeline.refresh_pipeline")
def test_cli_main_success(mock_refresh):
    """Test that main succeeds with valid parameters."""
    mock_refresh.return_value = "Loaded 5 rows"

    # Test Notion full refresh with select
    code = main(["notion", "--select", "notion__data_sources", "--full"])
    assert code == 0
    mock_refresh.assert_called_once_with(
        "notion",
        is_incremental=False,
        initial_date=None,
        end_date=None,
        select=["notion__data_sources"],
    )

    mock_refresh.reset_mock()

    # Test HubSpot incremental refresh with custom date ranges
    code = main(
        [
            "hubspot",
            "--incremental",
            "--initial-date",
            "2023-01-01",
            "--end-date",
            "2023-12-31",
        ]
    )
    assert code == 0
    mock_refresh.assert_called_once_with(
        "hubspot",
        is_incremental=True,
        initial_date="2023-01-01",
        end_date="2023-12-31",
        select=None,
    )


def test_cli_main_unknown_pipeline():
    """Test that main returns 2 for unknown pipeline choice."""
    # Since argparse has choices defined, passing an invalid choice raises SystemExit / error
    with pytest.raises(SystemExit):
        main(["unknown_pipeline"])


@patch("pipelines.run_pipeline.refresh_pipeline")
def test_cli_main_exception(mock_refresh):
    """Test that main returns 1 on general exception."""
    mock_refresh.side_effect = ValueError("Some parsing error")
    code = main(["notion"])
    assert code == 1


@patch("pipelines.runner.dlt.pipeline")
@patch("pipelines.runner.validate_required_secrets")
def test_runner_refresh_pipeline(mock_validate, mock_dlt_pipeline):
    """Test the centralized runner's refresh_pipeline and run_refresh dispatching."""
    mock_pipeline_instance = MagicMock()
    mock_dlt_pipeline.return_value = mock_pipeline_instance
    mock_pipeline_instance.run.return_value = "Loaded 10 rows"

    # Mocking the source factories so we don't query actual APIs
    mock_notion_source = MagicMock()
    mock_notion_source.with_resources.return_value = mock_notion_source

    with patch.dict(
        PIPELINE_CONFIG,
        {
            "notion": PIPELINE_CONFIG["notion"].__class__(
                source_factory=MagicMock(return_value=mock_notion_source),
                pipeline_name="notion_habits_pipeline",
                display_name="Notion Habits",
                required_secret_keys=["sources.notion.api_key"],
                source_kwargs={"db_name": "Disciplines"},
            )
        },
    ):
        config = PIPELINE_CONFIG["notion"]

        # Run refresh
        info = refresh_pipeline(
            "notion",
            is_incremental=True,
            initial_date="2024-01-01",
            end_date="2024-06-30",
            select=["notion__data_sources"],
        )

        assert info == "Loaded 10 rows"
        mock_validate.assert_called_once_with(
            secret_store="1password",  # defaults locally to 1password or google depending on environment
            required_secret_keys=["sources.notion.api_key"],
            pipeline_name="Notion Habits",
        )

        # Verify the source factory was called with correct db_name and date limits
        config.source_factory.assert_called_once()
        kwargs = config.source_factory.call_args[1]
        assert kwargs["db_name"] == "Disciplines"
        assert kwargs["initial_date"] == "2024-01-01"
        assert kwargs["end_date"] == "2024-06-30"

        # Verify resources selection
        mock_notion_source.with_resources.assert_called_once_with("notion__data_sources")

        # Verify dlt.pipeline creation and run
        mock_dlt_pipeline.assert_called_once_with(
            pipeline_name="notion_habits_pipeline",
            dataset_name=RAW_SCHEMA,
            destination="bigquery",
            progress="log",
        )
        mock_pipeline_instance.run.assert_called_once_with(
            mock_notion_source,
            write_disposition=None,  # None for is_incremental=True as per get_write_disposition
            loader_file_format="jsonl",
        )
