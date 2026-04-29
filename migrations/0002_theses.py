"""Add theses table.

Revision ID: 0002_theses
Revises: 0001_initial

NOTE: If the theses table already exists (created by init_thesis_db()),
stamp the DB to this revision without re-running DDL:
    alembic stamp 0002_theses
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy import inspect

revision = "0002_theses"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    existing = inspect(bind).get_table_names()

    if "theses" in existing:
        return  # already created by init_db() — nothing to do

    op.create_table(
        "theses",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("source_id", sa.String(128), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("abstract", sa.Text()),
        sa.Column("author", sa.Text()),
        sa.Column("institution", sa.Text()),
        sa.Column("country", sa.String(8)),
        sa.Column("year", sa.SmallInteger()),
        sa.Column("language", sa.String(8)),
        sa.Column("degree", sa.String(32)),
        sa.Column("keywords", postgresql.JSONB()),
        sa.Column("subjects", postgresql.JSONB()),
        sa.Column("url", sa.Text()),
        sa.Column("doi", sa.String(256)),
        sa.Column("hardware_relevant", sa.Boolean(), server_default="false"),
        sa.Column("software_relevant", sa.Boolean(), server_default="false"),
        sa.Column("matched_query", sa.Text()),
        sa.Column("raw_payload", postgresql.JSONB()),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source", "source_id", name="uq_thesis_source_id"),
    )
    op.create_index("ix_thesis_year", "theses", ["year"])
    op.create_index("ix_thesis_hardware", "theses", ["hardware_relevant"])
    op.create_index("ix_thesis_software", "theses", ["software_relevant"])
    op.create_index("ix_thesis_doi", "theses", ["doi"])
    op.create_index("ix_thesis_country", "theses", ["country"])
    op.create_index("ix_thesis_keywords", "theses", ["keywords"], postgresql_using="gin")


def downgrade() -> None:
    op.drop_table("theses")
