import pytest
import dlt
import re
import json
import responses
from urllib.parse import parse_qs, urlparse

from pipelines.hubspot import hubspot_source


def hubspot_contact_callback(request):
    payload = json.loads(request.body)
    after = payload.get("after")
    if after == "NTI1Cg%3D%3D":
        # Return actual data for second page
        with open("tests/mock_data/hubspot_contacts_2.json", "r") as f:
            return (200, {}, json.dumps(json.load(f)))
    else:
        # Return actual data for first page
        with open("tests/mock_data/hubspot_contacts_1.json", "r") as f:
            return (200, {}, json.dumps(json.load(f)))


def hubspot_engagement_callback(request):
    parsed_url = urlparse(request.url)
    params = parse_qs(parsed_url.query)
    offset = int(params.get("offset", [0])[0])
    if offset == 0:
        # Return data for first page
        with open("tests/mock_data/hubspot_engagements_1.json", "r") as f:
            return (200, {}, json.dumps(json.load(f)))
    else:
        # Return data for second page
        with open("tests/mock_data/hubspot_engagements_2.json", "r") as f:
            return (200, {}, json.dumps(json.load(f)))


@responses.activate
def test_hubspot_pipeline(monkeypatch):
    """
    Test that the HubSpot pipeline runs and loads data correctly.
    """
    # GIVEN
    # Mock the API responses
    with open("tests/mock_data/hubspot_schema.json", "r") as f:
        responses.add(
            responses.GET,
            re.compile(r"https://api.hubapi.com/crm-object-schemas/v3/schemas/\w+"),
            json=json.load(f),
            status=200,
        )
    responses.add_callback(
        responses.GET,
        "https://api.hubapi.com/engagements/v1/engagements/paged",
        callback=hubspot_engagement_callback,
        content_type="application/json",
    )
    responses.add_callback(
        responses.POST,
        "https://api.hubapi.com/crm/v3/objects/contacts/search",
        callback=hubspot_contact_callback,
        content_type="application/json",
    )
    with open("tests/mock_data/hubspot_companies.json", "r") as f:
        responses.add(
            responses.POST,
            "https://api.hubapi.com/crm/v3/objects/companies/search",
            json=json.load(f),
            status=200,
        )

    # mock the dlt.secrets.value to prevent credential errors
    monkeypatch.setenv("SOURCES__HUBSPOT__API_KEY", "dummy_api_key")

    # Define the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="hubspot_unit_test",
        destination="duckdb",
        dataset_name="hubspot_data",
        dev_mode=True,
    )

    # WHEN
    # Run the pipeline
    source = hubspot_source()
    info = pipeline.run(source)

    # THEN
    # Assert that the pipeline ran successfully
    assert info.has_failed_jobs is False
    assert len(info.loads_ids) == 1

    # Check the loaded data
    with pipeline.sql_client() as client:
        # Check schemas table
        schema_table = client.execute_sql(
            f"SELECT name FROM {pipeline.dataset_name}.hubspot__schemas"
        )

        assert len(schema_table) == 1

        # Check contacts table
        contacts_table = client.execute_sql(
            f"SELECT id FROM {pipeline.dataset_name}.hubspot__contacts"
        )
        assert len(contacts_table) == 4
        assert contacts_table[0][0] == "512"

        # Check companies table
        companies_table = client.execute_sql(
            f"SELECT id FROM {pipeline.dataset_name}.hubspot__companies"
        )
        assert len(companies_table) == 2
        assert companies_table[0][0] == "512"

        # Check engagements table
        engagements_table = client.execute_sql(
            f"""SELECT engagement__id, engagement__type
            FROM {pipeline.dataset_name}.hubspot__engagements"""
        )
        assert len(engagements_table) == 4
        assert engagements_table[0][0] == 29090716
        assert engagements_table[0][1] == "NOTE"
