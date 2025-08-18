import pytest
import dlt
import re
import json

# import responses
import pytest_responses
from urllib.parse import parse_qs, urlparse

from pipelines.hubspot import hubspot_source
from tests.dlt_unit.conftest import MOCK_FOLDER, sample_data, sample_response


@pytest.fixture
def mock_hs_apis(monkeypatch, responses):

    BASE_URL = "https://api.hubapi.com"

    # mock the dlt.secrets.value to prevent credential errors
    monkeypatch.setenv("SOURCES__HUBSPOT__API_KEY", "dummy_api_key")

    def search_callback(request, object: str):
        payload = json.loads(request.body)
        after = payload.get("after")
        if after is None:
            # Return data for first page
            return sample_response(f"hubspot_{object}_run1-page1.json")
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
        ("contacts", 2, None),
        ("companies", 2, None),
        ("engagements", 4, None),
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
class TestHubspot:

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
        expected_rows = 2
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
        # TODO
        pass


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
    assert info.has_failed_jobs is False
    assert len(info.loads_ids) == 1

    # Check the loaded data
    dataset = hs_pipeline.dataset_name
    with hs_pipeline.sql_client() as client:
        # Check schemas table
        schema_table = client.execute_sql(
            f"SELECT name FROM {dataset}.hubspot__schemas"
        )

        assert len(schema_table) == 1

        # Check contacts table
        contacts_table = client.execute_sql(
            f"SELECT id FROM {dataset}.hubspot__contacts"
        )
        assert len(contacts_table) == 4
        assert contacts_table[0][0] == "512"

        # Check companies table
        companies_table = client.execute_sql(
            f"SELECT id FROM {dataset}.hubspot__companies"
        )
        assert len(companies_table) == 2
        assert companies_table[0][0] == "512"

        # Check engagements table
        engagements_table = client.execute_sql(
            f"""SELECT engagement__id, engagement__type
            FROM {dataset}.hubspot__engagements"""
        )
        assert len(engagements_table) == 4
        assert engagements_table[0][0] == 29090716
        assert engagements_table[0][1] == "NOTE"
