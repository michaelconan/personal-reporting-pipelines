"""BigQuery schemas

Revision ID: fe52ae40c7c1
Revises:
Create Date: 2025-01-12 20:40:26.956798

"""

import os
from typing import Sequence, Union
import logging

from alembic import op
import sqlalchemy as sa
import google.cloud.bigquery as bigquery


# revision identifiers, used by Alembic.
revision: str = "fe52ae40c7c1"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


logger = logging.getLogger(__name__)

# Raw schema to copy system data as-is
RAW_SCHEMA = os.getenv("RAW_SCHEMA", default="raw")
DBT_SCHEMA = os.getenv("DBT_SCHEMA", default="reporting")


def upgrade() -> None:

    logger.info(f"Creating raw tables in schema: {RAW_SCHEMA}")
    logger.info(f"Creating dbt schema: {DBT_SCHEMA}")
    # Add schema and raw tables
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};")
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {DBT_SCHEMA};")


def downgrade() -> None:

    client = bigquery.Client()

    logger.info(f"Dropping raw tables in schema: {RAW_SCHEMA}")
    logger.info(f"Dropping dbt schema: {DBT_SCHEMA}")
    # Remove tables and schema, use client library to avoid errors
    client.delete_dataset(RAW_SCHEMA, delete_contents=True)
    client.delete_dataset(DBT_SCHEMA, delete_contents=True)
