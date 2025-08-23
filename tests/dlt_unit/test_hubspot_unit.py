# base imports
import re
import json
from typing import Callable
from urllib.parse import parse_qs, urlparse

# PyPI imports
import pytest
from pytest import MonkeyPatch
import dlt
import pytest_responses
from responses import RequestsMock

# local imports
from pipelines.hubspot import hubspot_source, iso_to_unix
from tests.dlt_unit.conftest import sample_data, sample_response


pytestmark = pytest.mark.local


@pytest.fixture
def mock_hs_apis(monkeypatch: MonkeyPatch, responses: RequestsMock) -> Callable:

    BASE_URL = "https://api.hubapi.com"

    # mock the dlt.secrets.value to prevent credential errors
    monkeypatch.setenv("SOURCES__HUBSPOT__API_KEY", "dummy_api_key")

    def search_callback(request, object: str):
        payload = json.loads(request.body)
        after = payload.get("after")
        filters = payload["filterGroups"][0]["filters"]
        start_filter = [f for f in filters if f["operator"] == "GTE"][0]
        if after is None:
            # Return data for first page
            return sample_response(f"hubspot_{object}_run1-page1.json")
        elif int(start_filter["value"]) > iso_to_unix("2025-01-01"):
            # Return data for subsequent run
            return sample_response(f"hubspot_{object}_run2.json")
        else:
            # Return data for second page
            return sample_response(f"hubspot_{object}_run1-page2.json")

    def engagement_callback(request):
        parsed_url = urlparse(request.url)
        params = parse_qs(parsed_url.query)
        offset = int(params.get("offset", [0])[0])
        limit = int(params.get("limit", [0])[0])
        if offset == 0:
            # Return data for first page
            return sample_response("hubspot_engagements_run1-page1.json")
        elif offset == limit:
            # Return data for second page
            return sample_response("hubspot_engagements_run1-page2.json")
        else:
            return (200, {}, json.dumps({"results": []}))

    def setup(endpoints=[]):
        """Nested function to only register mock endpoints for tests.

        Args:
            endpoints (list, optional): Specific endpoints to register. Defaults to [] (all).
        """
        # Mock the API responses
        if not endpoints or "engagements" in endpoints:
            responses.add_callback(
                responses.GET,
                BASE_URL + "/engagements/v1/engagements/paged",
                callback=engagement_callback,
                content_type="application/json",
            )
        if not endpoints or "contacts" in endpoints:
            responses.add_callback(
                responses.POST,
                BASE_URL + "/crm/v3/objects/contacts/search",
                callback=lambda r: search_callback(r, "contacts"),
                content_type="application/json",
            )
        if not endpoints or "companies" in endpoints:
            responses.add_callback(
                responses.POST,
                BASE_URL + "/crm/v3/objects/companies/search",
                callback=lambda r: search_callback(r, "companies"),
                content_type="application/json",
            )
        if not endpoints or "schemas_contacts" in endpoints:
            responses.add(
                responses.GET,
                re.compile(BASE_URL + r"/crm-object-schemas/v3/schemas/\w+"),
                json=sample_data("hubspot_schemas_contacts.json"),
                status=200,
            )

    return setup


@pytest.mark.parametrize(
    ("resource", "expected_tables", "configs"),
    (
        ("contacts", 1, {}),
        ("companies", 1, {}),
        ("engagements", 3, {}),
        (
            "schemas_contacts",
            5,
            {
                # TODO: Figure out how to test this
                # "max_table_nesting": 1,
                "columns": {
                    "searchable_properties": {"data_type": "json"},
                    "secondary_display_properties": {"data_type": "json"},
                },
            },
        ),
    ),
)
class TestHubspotPhases:

    def test_extract(
        self,
        mock_hs_apis,
        duckdb_pipeline: dlt.Pipeline,
        resource: str,
        expected_tables: int,
        configs: dict | None,
    ):

        # GIVEN
        # Mocked APIs
        mock_hs_apis(endpoints=[resource])

        # WHEN
        source = hubspot_source().with_resources(f"hubspot__{resource}")
        info = duckdb_pipeline.extract(source)

        # THEN
        assert len(info.loads_ids) == 1

    def test_normalize(
        self,
        duckdb_pipeline: dlt.Pipeline,
        resource: str,
        expected_tables: int,
        configs: dict | None,
    ):

        # GIVEN
        expected_rows = 3
        file_name = f"hubspot_{resource}_run1-page1.json"
        file_name2 = f"hubspot_{resource}.json"
        source = sample_data(file_name, fallback=file_name2)
        if "results" in source:
            source = source["results"]
        else:
            source = [source]
            expected_rows = 1
        duckdb_pipeline.extract(source, table_name=resource, **configs)

        # WHEN
        info = duckdb_pipeline.normalize()

        # THEN
        # _dlt_state table, source table, and any child tables
        assert (
            len([r for r in info.row_counts if r.startswith(resource)])
            == expected_tables
        )
        # first page record count
        assert info.row_counts[resource] == expected_rows

    def test_load(
        self,
        duckdb_pipeline: dlt.Pipeline,
        resource: str,
        expected_tables: int,
        configs: dict | None,
    ):
        # GIVEN
        # Files to load for sample test
        file_name = f"hubspot_{resource}_run1-page1.json"
        file_name2 = f"hubspot_{resource}.json"
        source = sample_data(file_name, fallback=file_name2)
        if "results" in source:
            source = source["results"]
        else:
            source = [source]
        duckdb_pipeline.extract(source, table_name=resource, **configs)
        duckdb_pipeline.normalize()

        # WHEN
        info = duckdb_pipeline.load()

        # THEN
        assert info.has_failed_jobs is False
        assert all(p.state == "loaded" for p in info.load_packages)


@pytest.mark.parametrize(
    ("resource", "increment"),
    (
        ("contacts", True),
        ("companies", True),
        ("contacts", False),
        ("companies", False),
        ("engagements", False),
        ("schemas_contacts", False),
    ),
)
def test_hubspot_refresh(
    mock_hs_apis,
    duckdb_pipeline: dlt.Pipeline,
    resource: str,
    increment: bool,
):
    # GIVEN (1)
    # Mock APIs
    mock_hs_apis(endpoints=[resource])
    expected_rows = 1 if "schemas" in resource else 5
    # Dataset from pipeline (dev mode)
    dataset = duckdb_pipeline.dataset_name
    table = resource.split("_")[0]
    # Defaults to append for most resources
    write_disposition = None if increment else "replace"

    # WHEN (1)
    source = hubspot_source().with_resources(f"hubspot__{resource}")
    info = duckdb_pipeline.run(source)

    # THEN (1)
    # Run pipeline the first time
    assert info.first_run is True
    assert info.has_failed_jobs is False
    # Validate loaded data from initial run
    with duckdb_pipeline.sql_client() as client:
        table_rows = client.execute_sql(f"SELECT 1 FROM {dataset}.hubspot__{table}")
    assert len(table_rows) == expected_rows

    # GIVEN (2)
    expected_rows += 1 if increment else 0

    # WHEN (2)
    info2 = duckdb_pipeline.run(source, write_disposition=write_disposition)

    # THEN (2)
    # Run pipeline again
    assert info2.first_run is False
    assert info2.has_failed_jobs is False
    # Validate loaded data from incremental run
    with duckdb_pipeline.sql_client() as client:
        table_rows2 = client.execute_sql(f"SELECT 1 FROM {dataset}.hubspot__{table}")
    assert len(table_rows2) == expected_rows


def test_hubspot_pipeline(mock_hs_apis, duckdb_pipeline):
    """
    Test that the HubSpot pipeline runs and loads data correctly.
    """
    # GIVEN

    # WHEN
    # Run the pipeline
    source = hubspot_source()
    info = duckdb_pipeline.run(source)

    # THEN
    # Assert that the pipeline ran successfully
    assert info.first_run is True
    assert info.has_failed_jobs is False
    assert len(info.loads_ids) == 1

    # Check the loaded data
    dataset = duckdb_pipeline.dataset_name
    with duckdb_pipeline.sql_client() as client:
        # Check schemas table
        schema_table = client.execute_sql(f"SELECT 1 FROM {dataset}.hubspot__schemas")

        assert len(schema_table) == 1

        # Check contacts table
        contacts_table = client.execute_sql(
            f"SELECT 1 FROM {dataset}.hubspot__contacts"
        )
        assert len(contacts_table) == 5

        # Check companies table
        companies_table = client.execute_sql(
            f"SELECT 1 FROM {dataset}.hubspot__companies"
        )
        assert len(companies_table) == 5

        # Check engagements table
        engagements_table = client.execute_sql(
            f"""SELECT 1 FROM {dataset}.hubspot__engagements"""
        )
        assert len(engagements_table) == 5
