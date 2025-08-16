import pytest
import dlt
import os
import json
import responses

from pipelines.hubspot import hubspot_source

@responses.activate
def test_hubspot_pipeline(monkeypatch):
    """
    Test that the HubSpot pipeline runs and loads data correctly.
    """
    # Mock the API responses
    with open("tests/mock_data/hubspot_schema.json", "r") as f:
        responses.add(
            responses.GET,
            "https://api.hubapi.com/crm-object-schemas/v3/schemas/contacts",
            json=json.load(f),
            status=200,
        )
    with open("tests/mock_data/hubspot_schema.json", "r") as f:
        # a bit of a hack, but we need a second schema response for companies
        schema_data = json.load(f)
        schema_data['name'] = 'companies'
        responses.add(
            responses.GET,
            "https://api.hubapi.com/crm-object-schemas/v3/schemas/companies",
            json=schema_data,
            status=200,
        )
    with open("tests/mock_data/hubspot_engagements.json", "r") as f:
        responses.add(
            responses.GET,
            "https://api.hubapi.com/engagements/v1/engagements/paged",
            json=json.load(f),
            status=200,
        )
    with open("tests/mock_data/hubspot_contacts.json", "r") as f:
        responses.add(
            responses.POST,
            "https://api.hubapi.com/crm/v3/objects/contacts/search",
            json=json.load(f),
            status=200,
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
    )

    # Run the pipeline
    source = hubspot_source()
    info = pipeline.run(source)

    # Assert that the pipeline ran successfully
    assert info.has_failed_jobs is False
    assert info.loads_count == 1

    # Check the loaded data
    with pipeline.sql_client() as client:
        # Check schemas table
        schema_table = client.sql_query("SELECT name FROM hubspot_data.hubspot__schemas")
        assert len(schema_table) == 2

        # Check contacts table
        contacts_table = client.sql_query("SELECT id, email FROM hubspot_data.hubspot__contacts")
        assert len(contacts_table) == 2
        assert contacts_table[0][0] == "1"
        assert contacts_table[0][1] == "contact1@example.com"

        # Check companies table
        companies_table = client.sql_query("SELECT id, name FROM hubspot_data.hubspot__companies")
        assert len(companies_table) == 2
        assert companies_table[0][0] == "1"
        assert companies_table[0][1] == "Company A"

        # Check engagements table
        engagements_table = client.sql_query("SELECT engagement__id, engagement__type FROM hubspot_data.hubspot__engagements")
        assert len(engagements_table) == 2
        assert engagements_table[0][0] == 1
        assert engagements_table[0][1] == "CALL"
