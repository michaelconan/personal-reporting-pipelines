"""raw notion tables

Revision ID: fe52ae40c7c1
Revises: 
Create Date: 2025-01-12 20:40:26.956798

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "fe52ae40c7c1"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Raw schema to copy system data as-is
SCHEMA = "raw"


def upgrade() -> None:
    # Add schema and raw tables
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")

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
        schema=SCHEMA,
    )

    op.create_table(
        "weekly_habit",
        sa.Column("database_id", sa.String(50), nullable=False),
        sa.Column("id", sa.String(50), nullable=False),
        sa.Column("Name", sa.String(255), nullable=False),
        sa.Column("Date", sa.Date, nullable=False),
        sa.Column("Church", sa.Boolean(), nullable=False),
        sa.Column("Fast", sa.Boolean(), nullable=False),
        sa.Column("Time Prayed", sa.Integer(), nullable=False),
        sa.Column("created_time", sa.TIMESTAMP(), nullable=False),
        sa.Column("last_edited_time", sa.TIMESTAMP(), nullable=False),
        schema=SCHEMA,
    )


def downgrade() -> None:
    # Remove tables and schema
    op.drop_table("weekly_habit", schema=SCHEMA)
    op.drop_table("daily_habit", schema=SCHEMA)
    op.execute(f"DROP SCHEMA IF EXISTS {SCHEMA};")
