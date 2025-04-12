"""Create BigQuery schemas

Revision ID:
    fe52ae40c7c1
Revises:
    None
Create Date:
    2025-01-12 20:40:26.956798
"""

# Base imports
import os
from typing import Sequence, Union
import logging

# PyPI imports
from google.auth import default
import google.cloud.bigquery as bigquery

# Local imports
from dags.michael.datasets import RAW_SCHEMA, DBT_SCHEMA


# revision identifiers, used by Alembic.
revision: str = "fe52ae40c7c1"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


logger = logging.getLogger(__name__)

# Raw schema to copy system data as-is
BQ_LOCATION = os.getenv("BQ_LOCATION", default="US")


def upgrade() -> None:
    """Create BigQuery schemas (datasets)"""
    # Initialize client with default credentials
    credentials, project_id = default()
    client = bigquery.Client(credentials=credentials, project=project_id)

    # Add schemas in specified location
    for schema in [RAW_SCHEMA, DBT_SCHEMA]:
        logger.info(f"Creating BigQuery schema (dataset): {schema}")
        bq_dataset = bigquery.Dataset(f"{project_id}.{schema}")
        bq_dataset.location = BQ_LOCATION
        client.create_dataset(bq_dataset, exists_ok=True)


def downgrade() -> None:
    """Drop BigQuery schemas (datasets)"""

    # Initialize client with default credentials
    client = bigquery.Client()

    # Remove tables and schema, use client library to avoid errors
    for schema in [RAW_SCHEMA, DBT_SCHEMA]:
        logger.info(f"Dropping BigQuery schema (dataset) with contents: {schema}")
        client.delete_dataset(schema, delete_contents=True)
