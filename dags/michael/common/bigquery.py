"""
bigquery.py

Common functions to interact with Google BigQuery in DAG tasks.
"""

# Airflow hooks and operators
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Additional libraries
from google.cloud import bigquery


def load_file_to_bq(conn_id: str, file_path: str, table_id: str) -> str:
    """Insert newline-delimited JSON file (JSONL) into BigQuery table

    Args:
        conn_id (str): Airflow connection ID for BigQuery.
        file_path (str): Path to the file to load.
        table_name (str): Table name to load the data into.

    Returns:
        str: State of completed insert job.
    """
    # Load the data into BigQuery
    connection = BigQueryHook(gcp_conn_id=conn_id)
    client = connection.get_client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    job.result()

    return job.state
