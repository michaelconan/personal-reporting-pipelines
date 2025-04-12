"""Create fitbit raw tables

Revision ID:
    6fd5a3229fb8
Revises:
    fe3875770e8b
Create Date:
    2025-04-10 23:16:49.495902
"""

# Base imports
from typing import Sequence, Union

# PyPI imports
from alembic import op
import sqlalchemy as sa

# Local imports
from dags.michael.datasets import RAW_SCHEMA


# revision identifiers, used by Alembic.
revision: str = "6fd5a3229fb8"
down_revision: Union[str, None] = "fe3875770e8b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create Fitbit Raw Tables for key objects"""
    op.create_table(
        "fitbit__sleep",
        sa.Column("logId", sa.String(50), nullable=False),
        sa.Column("dateOfSleep", sa.Date(), nullable=False),
        sa.Column("duration", sa.Integer(), nullable=False),
        sa.Column("startTime", sa.TIMESTAMP(), nullable=False),
        sa.Column("endTime", sa.TIMESTAMP(), nullable=False),
        sa.Column("logType", sa.String(10), nullable=False),
        sa.Column("type", sa.String(10), nullable=False),
        schema=RAW_SCHEMA,
        if_not_exists=True,
    )


def downgrade() -> None:
    """Drop Fitbit Raw Tables for key objects"""
    op.drop_table("fitbit__sleep", schema=RAW_SCHEMA, if_exists=True)
