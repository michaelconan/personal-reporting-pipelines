import pytest
import dlt
import re
import json

# import responses
import pytest_responses
from urllib.parse import parse_qs, urlparse

from pipelines.hubspot import hubspot_source, iso_to_unix
from tests.dlt_unit.conftest import sample_data, sample_response


pytestmark = pytest.mark.local


@pytest.fixture
def mock_hs_apis(monkeypatch, responses):

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

    # Mock the API responses
    responses.add_callback(
        responses.GET,
        BASE_URL + "/engagements/v1/engagements/paged",
        callback=engagement_callback,
        content_type="application/json",
    )
    responses.add_callback(
        responses.POST,
        BASE_URL + "/crm/v3/objects/contacts/search",
        callback=lambda r: search_callback(r, "contacts"),
        content_type="application/json",
    )
    responses.add_callback(
        responses.POST,
        BASE_URL + "/crm/v3/objects/companies/search",
        callback=lambda r: search_callback(r, "companies"),
        content_type="application/json",
    )
    responses.add(
        responses.GET,
        re.compile(BASE_URL + r"/crm-object-schemas/v3/schemas/\w+"),
        json=sample_data("hubspot_schemas_contacts.json"),
        status=200,
    )


@pytest.fixture
def hs_pipeline() -> dlt.Pipeline:
    # Test pipeline
    pipeline = dlt.pipeline(
        pipeline_name="hubspot_unit_test",
        destination="duckdb",
        dataset_name="hubspot_data",
        dev_mode=True,
    )
    yield pipeline
    # Cleanup
    pipeline.drop()


@pytest.mark.parametrize(
    ("resource", "expected_tables", "configs"),
    (
        ("contacts", 2, {}),
        ("companies", 2, {}),
        ("engagements", 4, {}),
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
        hs_pipeline: dlt.Pipeline,
        resource: str,
        expected_tables: int,
        configs: dict | None,
    ):

        # GIVEN
        # Mocked APIs

        # WHEN
        source = hubspot_source().with_resources(f"hubspot__{resource}")
        info = hs_pipeline.extract(source)

        # THEN
        assert info.first_run
        assert len(info.loads_ids) == 1

    def test_normalize(
        self,
        hs_pipeline: dlt.Pipeline,
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
        hs_pipeline.extract(source, table_name=resource, **configs)

        # WHEN
        info = hs_pipeline.normalize()

        # THEN
        assert info.first_run
        # _dlt_state table, source table, and any child tables
        assert len(info.row_counts) == expected_tables
        # first page record count
        assert info.row_counts[resource] == expected_rows

    def test_load(
        self,
        hs_pipeline: dlt.Pipeline,
        resource: str,
        expected_tables: int,
        configs: dict | None,
    ):
        # GIVEN
        file_name = f"hubspot_{resource}_run1-page1.json"
        file_name2 = f"hubspot_{resource}.json"
        source = sample_data(file_name, fallback=file_name2)
        if "results" in source:
            source = source["results"]
        else:
            source = [source]
        hs_pipeline.extract(source, table_name=resource, **configs)
        hs_pipeline.normalize()

        # WHEN
        info = hs_pipeline.load()

        # THEN
        assert info.first_run
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
    hs_pipeline: dlt.Pipeline,
    resource: str,
    increment: bool,
):
    # GIVEN (1)
    # Dataset from pipeline (dev mode)
    dataset = hs_pipeline.dataset_name
    # Defaults to append for most resources
    write_disposition = None if increment else "replace"

    # WHEN (1)
    source = hubspot_source().with_resources(f"hubspot__{resource}")
    info = hs_pipeline.run(source)

    # THEN (1)
    # Run pipeline the first time
    assert info.first_run is True
    assert info.has_failed_jobs is False
    # Validate loaded data from initial run
    with hs_pipeline.sql_client() as client:
        table_rows = client.execute_sql(f"SELECT 1 FROM {dataset}.hubspot__{resource}")
    assert len(table_rows) == 5

    # GIVEN (2)
    expected_rows = 6 if increment else 5

    # WHEN (2)
    info2 = hs_pipeline.run(source, write_disposition=write_disposition)

    # THEN (2)
    # Run pipeline again
    assert info2.first_run is False
    assert info2.has_failed_jobs is False
    # Validate loaded data from incremental run
    with hs_pipeline.sql_client() as client:
        table_rows2 = client.execute_sql(f"SELECT 1 FROM {dataset}.hubspot__{resource}")
    assert len(table_rows2) == expected_rows


def test_hubspot_pipeline(mock_hs_apis, hs_pipeline):
    """
    Test that the HubSpot pipeline runs and loads data correctly.
    """
    # GIVEN

    # WHEN
    # Run the pipeline
    source = hubspot_source()
    info = hs_pipeline.run(source)

    # THEN
    # Assert that the pipeline ran successfully
    assert info.first_run is True
    assert info.has_failed_jobs is False
    assert len(info.loads_ids) == 1

    # Check the loaded data
    dataset = hs_pipeline.dataset_name
    with hs_pipeline.sql_client() as client:
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
