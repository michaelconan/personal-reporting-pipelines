"""
raw_hubspot__crm.py

DAGs to load Hubspot CRM data into BigQuery.
"""

# Basic imports
import os
import pendulum
import jsonlines

# Standard airflow imports
from airflow.decorators import task
from airflow.models import DAG
from airflow.datasets import Dataset

# Airflow hooks and operators
from airflow.hooks.base import BaseHook

# Common custom tasks
from michael.common.bigquery import load_file_to_bq
from michael.datasets import (
    HUBSPOT_CONTACTS_DS,
    HUBSPOT_COMPANIES_DS,
    HUBSPOT_ENGAGEMENTS_DS,
)

IS_TEST = os.getenv("TEST") or os.getenv("CI")

# Hubspot connection
HUBSPOT_CONN_ID = "hubspot_app"

# BigQuery connection details
BQ_CONN_ID = "bigquery_reporting"
BQ_CONTACT_TABLE = "hubspot__contact"
BQ_COMPANY_TABLE = "hubspot__company"
BQ_ENGAGEMENT_TABLE = "hubspot__engagement"

# Hubspot CRM object properties
HUBSPOT_PROPERTIES = [
    "id",
    "createdAt",
    "updatedAt",
]
OBJECT_KEY = {
    "contacts": {
        "filter_key": "lastmodifieddate",
        "properties": [
            "email",
            "associatedcompanyid",
            "firstname",
            "lastname",
        ],
    },
    "companies": {
        "filter_key": "hs_lastmodifieddate",
        "properties": [
            "name",
            "hs_ideal_customer_profile",
        ],
    },
    "engagements": {
        "types": ["CALL", "WHATS_APP", "MEETING", "NOTE", "SMS"],
        "properties": [
            "lastUpdated",
            "type",
            "timestamp",
            "bodyPreview",
            "contactIds",
            "companyIds",
        ],
    },
}

# Configurations to generated DAGs
DAG_CONFIGS = [
    {
        "dag_id": "raw_hubspot__contacts__full",
        "schedule": "@once",
        "bq_table": BQ_CONTACT_TABLE,
        "dataset": HUBSPOT_CONTACTS_DS,
        "crm_object": "contacts",
    },
    {
        "dag_id": "raw_hubspot__contacts__changed",
        "schedule": "@daily",
        "bq_table": BQ_CONTACT_TABLE,
        "dataset": HUBSPOT_CONTACTS_DS,
        "crm_object": "contacts",
    },
    {
        "dag_id": "raw_hubspot__companies__full",
        "schedule": "@once",
        "bq_table": BQ_COMPANY_TABLE,
        "dataset": HUBSPOT_COMPANIES_DS,
        "crm_object": "companies",
    },
    {
        "dag_id": "raw_hubspot__companies__changed",
        "schedule": "@daily",
        "bq_table": BQ_COMPANY_TABLE,
        "dataset": HUBSPOT_COMPANIES_DS,
        "crm_object": "companies",
    },
    {
        "dag_id": "raw_hubspot__engagements__full",
        "schedule": "@once",
        "bq_table": BQ_ENGAGEMENT_TABLE,
        "dataset": HUBSPOT_ENGAGEMENTS_DS,
        "crm_object": "engagements",
    },
    {
        "dag_id": "raw_hubspot__engagements__changed",
        "schedule": "@daily",
        "bq_table": BQ_ENGAGEMENT_TABLE,
        "dataset": HUBSPOT_ENGAGEMENTS_DS,
        "crm_object": "engagements",
    },
]


def create_hubspot_dag(
    dag_id: str,
    schedule: str,
    bq_table: str,
    dataset: Dataset,
    crm_object: str,
) -> DAG:
    with DAG(
        dag_id,
        schedule=schedule,
        start_date=pendulum.datetime(2024, 10, 1),
        catchup=False,
        params={"raw_schema": "raw"},
        user_defined_macros={"BQ_TABLE": bq_table},
        tags=["hubspot", "crm", "raw"],
    ) as dag:
        DATA_FILE = f"/tmp/{dag_id}.jsonl"
        object_info = OBJECT_KEY[crm_object]

        def get_object_properties(record_data: dict, properties: list[str]) -> dict:
            """
            Get properties from Hubspot CRM object

            Args:
                record_data (dict): Record data from Hubspot CRM
                properties (list[str]): List of properties to get

            Returns:
                dict: Record properties as dictionary
            """
            record_properties = dict()
            all_properties = properties + HUBSPOT_PROPERTIES
            for prop in all_properties:
                # Check if property is in record data root
                if prop in record_data:
                    record_properties[prop] = record_data[prop]
                # Check if property is in record data properties
                elif prop in record_data.get("properties", dict()):
                    record_properties[prop] = record_data["properties"][prop]
                # Check if property is in record data engagement
                elif prop in record_data.get("engagement", dict()):
                    record_properties[prop] = record_data["engagement"][prop]
                # Check if property is in record data associations
                elif prop in record_data.get("associations", dict()):
                    record_properties[prop] = record_data["associations"][prop]
                else:
                    dag.log.warning(f"Property {prop} not found in record data")
            return record_properties

        @task(
            task_id="get_hubspot_data",
        )
        def get_hubspot_data(
            conn_id: str,
            crm_object: str,
            file_path: str,
            # Context variables
            dag: DAG,
            data_interval_start: pendulum.DateTime,
            data_interval_end: pendulum.DateTime,
        ) -> int:
            """Shared task to export Notion habit page data to a file

            Args:
                conn_id (str): Airflow connection identifier for Notion API key
                crm_object (str): Hubspot CRM object to query
                file_path (str): JSON file path to export data
                dag (DAG): DAG calling task, from context
                data_interval_start (pendulum.DateTime): Start of interval, from context
                data_interval_end (pendulum.DateTime): End of interval, from context
            """
            from hubspot import HubSpot

            # Connect to Hubspot API
            connection = BaseHook.get_connection(conn_id)
            api_client = HubSpot(access_token=connection.password)

            def search_hubspot_crm(
                api_client: HubSpot,
                crm_object: str,
                start_date: pendulum.DateTime,
                end_date: pendulum.DateTime,
            ):
                # Define data interval details
                is_incremental = start_date != end_date
                unix_start = str(int(start_date.timestamp() * 1000))
                unix_end = str(int(end_date.timestamp() * 1000))

                # Build request for object search
                object_url = f"/crm/v3/objects/{crm_object}/search"
                hs_request = {
                    "method": "POST",
                    "path": object_url,
                    "body": {
                        "limit": 100,
                        "properties": object_info["properties"],
                    },
                }

                # Define request path and filter parameters
                if is_incremental:
                    hs_request["body"]["filterGroups"] = [
                        {
                            "filters": [
                                {
                                    "propertyName": object_info["filter_key"],
                                    "operator": "GTE",
                                    "value": unix_start,
                                },
                                {
                                    "propertyName": object_info["filter_key"],
                                    "operator": "LTE",
                                    "value": unix_end,
                                },
                            ]
                        }
                    ]

                # Call API with request details, paginate with provided paging link
                results = list()
                while True:
                    response = api_client.api_request(hs_request)
                    if response.ok:
                        page = response.json()
                        results.extend(page["results"])
                        if "paging" in page:
                            hs_request["body"]["after"] = page["paging"]["next"][
                                "after"
                            ]
                        else:
                            break
                    else:
                        raise Exception(
                            f"Error fetching data from Hubspot: {response.text}"
                        )

                # Get properties from each record
                results = [
                    get_object_properties(r, object_info["properties"]) for r in results
                ]

                return results

            def get_hubspot_engagements(
                api_client: HubSpot,
                start_date: pendulum.DateTime,
                end_date: pendulum.DateTime,
            ):
                # Define data interval details
                is_incremental = start_date != end_date
                unix_start = int(start_date.timestamp() * 1000)
                unix_end = int(end_date.timestamp() * 1000)

                # Use full paged endpoint as default
                object_url = "/engagements/v1/engagements/paged"
                hs_request = {
                    "method": "GET",
                    "path": object_url,
                    "body": {
                        "properties": object_info["properties"],
                    },
                }

                # Use recent engagements endpoint if incremental with data interval start
                # Filter by end date after queries due to limited API filters
                if is_incremental:
                    object_url = f"/engagements/v1/engagements/recent/modified?since={unix_start}"
                    hs_request["path"] = object_url

                # Get all engagements, paginate using offset reference
                all_results = list()
                while True:
                    response = api_client.api_request(hs_request)
                    if response.ok:
                        page = response.json()
                        all_results.extend(page["results"])
                        if page["hasMore"]:
                            hs_request["path"] = f"{object_url}?offset={page['offset']}"
                        else:
                            break
                    else:
                        raise Exception(
                            f"Error fetching data from Hubspot: {response.text}"
                        )
                dag.log.info(f"Found {len(all_results)} engagements before filters")

                # Filter results to only include those updated within the data interval
                results = [
                    r
                    for r in all_results
                    if r["engagement"]["lastUpdated"] <= unix_end
                    and r["engagement"]["type"] in object_info["types"]
                ]
                dag.log.info(f"{len(results)} engagements after filters")

                # Get properties from each engagement
                mapped_results = [
                    get_object_properties(r, object_info["properties"]) for r in results
                ]

                return mapped_results

            # Get data based on object type
            if crm_object == "engagements":
                results = get_hubspot_engagements(
                    api_client, data_interval_start, data_interval_end
                )
            else:
                results = search_hubspot_crm(
                    api_client,
                    crm_object,
                    data_interval_start,
                    data_interval_end,
                )

            # Check if pages were returned for data interval
            if results:
                with jsonlines.open(file_path, mode="w") as writer:
                    writer.write_all(results)

            return len(results)

        @task(
            task_id="load_file_to_bq",
            outlets=[dataset],
        )
        def load_data_file(rows: int, params: dict, outlet_events=None):
            if rows > 0:
                job_state = load_file_to_bq(
                    conn_id=BQ_CONN_ID,
                    file_path=DATA_FILE,
                    table_id=f"{params['raw_schema']}.{bq_table}",
                )
                # Update outlet dataset extras
                outlet_events[dataset].extra = {
                    "state": job_state,
                    "rows": rows,
                }

        # Define task flow - task functions must be called ()
        hubspot_data = get_hubspot_data(
            conn_id=HUBSPOT_CONN_ID,
            crm_object=crm_object,
            file_path=DATA_FILE,
        )
        load_data_file(hubspot_data)

        return dag


# Create DAGs dynamically
for config in DAG_CONFIGS:
    globals()[config["dag_id"]] = create_hubspot_dag(**config)
