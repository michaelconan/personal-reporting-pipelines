"""hubspot raw tables

Revision ID:
    fe3875770e8b
Revises:
    c0ea7b45da9c
Create Date:
    2025-02-08 00:35:48.123541
"""

from typing import Sequence, Union
import os
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "fe3875770e8b"
down_revision: Union[str, None] = "c0ea7b45da9c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Raw schema to copy system data as-is
RAW_SCHEMA = os.getenv("RAW_SCHEMA", default="raw")


def upgrade() -> None:
    """Create Hubspot Raw Tables for key objects and intersection tables for associations"""
    op.create_table(
        "company",
        sa.Column("id", sa.String(25), nullable=False),
        sa.Column("name", sa.String(100), nullable=False),
        # Profile tier used for target contact cadence
        sa.Column("hs_ideal_customer_profile", sa.String(10), nullable=True),
        sa.Column("createdAt", sa.TIMESTAMP(), nullable=False),
        sa.Column("updatedAt", sa.TIMESTAMP(), nullable=False),
        schema=RAW_SCHEMA,
        if_not_exists=True,
    )

    # Only primary company is associated with contact
    op.create_table(
        "contact",
        sa.Column("id", sa.String(25), nullable=False),
        sa.Column("associatedcompanyid", sa.String(25), nullable=False),
        sa.Column("email", sa.String(100), nullable=False),
        sa.Column("firstname", sa.String(50), nullable=False),
        sa.Column("lastname", sa.String(50), nullable=False),
        sa.Column("createdAt", sa.TIMESTAMP(), nullable=False),
        sa.Column("updatedAt", sa.TIMESTAMP(), nullable=False),
        schema=RAW_SCHEMA,
        if_not_exists=True,
    )

    # NOTE: timestamp is stored as a unix timestamp in milliseconds based on API format
    op.create_table(
        "engagement",
        sa.Column("id", sa.String(25), nullable=False),
        sa.Column("type", sa.String(50), nullable=False),
        sa.Column("timestamp", sa.BigInteger(), nullable=False),
        sa.Column("bodyPreview", sa.Text(), nullable=False),
        sa.Column("contactIds", sa.ARRAY(sa.String(25)), nullable=False),
        sa.Column("companyIds", sa.ARRAY(sa.String(25)), nullable=False),
        sa.Column("createdAt", sa.BigInteger(), nullable=False),
        sa.Column("lastUpdated", sa.BigInteger(), nullable=False),
        schema=RAW_SCHEMA,
        if_not_exists=True,
    )


def downgrade() -> None:
    """Drop Hubspot Raw Tables for key objects and intersection tables for associations"""
    op.drop_table("contact_company")
    op.drop_table("contact_engagement")
    op.drop_table("company_engagement")
    op.drop_table("engagement")
    op.drop_table("company")
    op.drop_table("contact")
