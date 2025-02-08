"""notion raw tables

Revision ID: c0ea7b45da9c
Revises: fe52ae40c7c1
Create Date: 2025-02-08 00:25:53.845234

"""

from typing import Sequence, Union
import os
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c0ea7b45da9c"
down_revision: Union[str, None] = "fe52ae40c7c1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Raw schema to copy system data as-is
RAW_SCHEMA = os.getenv("RAW_SCHEMA", default="raw")


def upgrade() -> None:
    op.create_table(
        "daily_habit",
        sa.Column("database_id", sa.String(50), nullable=False),
        sa.Column("id", sa.String(50), nullable=False),
        sa.Column("Name", sa.String(255), nullable=False),
        sa.Column("Date", sa.Date, nullable=False),
        sa.Column("Devotional", sa.Boolean(), nullable=False),
        sa.Column("Journal", sa.Boolean(), nullable=False),
        sa.Column("Prayer", sa.Boolean(), nullable=False),
        sa.Column("Read Bible", sa.Boolean(), nullable=False),
        sa.Column("Workout", sa.Boolean(), nullable=False),
        sa.Column("Language", sa.Boolean(), nullable=False),
        sa.Column("created_time", sa.TIMESTAMP(), nullable=False),
        sa.Column("last_edited_time", sa.TIMESTAMP(), nullable=False),
        schema=RAW_SCHEMA,
        if_not_exists=True,
    )

    op.create_table(
        "weekly_habit",
        sa.Column("database_id", sa.String(50), nullable=False),
        sa.Column("id", sa.String(50), nullable=False),
        sa.Column("Name", sa.String(255), nullable=False),
        sa.Column("Date", sa.Date, nullable=False),
        sa.Column("Church", sa.Boolean(), nullable=False),
        sa.Column("Fast", sa.Boolean(), nullable=False),
        sa.Column("Community", sa.Boolean(), nullable=False),
        sa.Column("Prayer Minutes", sa.Integer(), nullable=True),
        sa.Column("Screen Minutes", sa.Integer(), nullable=True),
        sa.Column("created_time", sa.TIMESTAMP(), nullable=False),
        sa.Column("last_edited_time", sa.TIMESTAMP(), nullable=False),
        schema=RAW_SCHEMA,
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_table("daily_habit", schema=RAW_SCHEMA)
    op.drop_table("weekly_habit", schema=RAW_SCHEMA)
