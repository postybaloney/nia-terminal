"""Initial patent schema.

Revision ID: 0001_initial
Revises: —
Creates: patent_families, raw_patents, ingest_runs, analysis_results

NOTE: If tables already exist (created by init_db()), stamp the DB
instead of running this:
    alembic stamp 0001_initial
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy import inspect

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    existing = inspect(bind).get_table_names()

    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    if "patent_families" not in existing:
        op.create_table(
            "patent_families",
            sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column("family_id", sa.String(64), nullable=False),
            sa.Column("title", sa.Text()),
            sa.Column("abstract", sa.Text()),
            sa.Column("earliest_filing_date", sa.DateTime(timezone=True)),
            sa.Column("earliest_grant_date", sa.DateTime(timezone=True)),
            sa.Column("assignees", postgresql.JSONB()),
            sa.Column("inventors", postgresql.JSONB()),
            sa.Column("cpc_codes", postgresql.JSONB()),
            sa.Column("ipc_codes", postgresql.JSONB()),
            sa.Column("sources", postgresql.JSONB()),
            sa.Column("first_seen_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("family_id"),
        )
        op.create_index("ix_families_earliest_filing", "patent_families", ["earliest_filing_date"])
        op.create_index("ix_families_assignees", "patent_families", ["assignees"], postgresql_using="gin")
        op.create_index("ix_families_cpc", "patent_families", ["cpc_codes"], postgresql_using="gin")

    if "raw_patents" not in existing:
        op.create_table(
            "raw_patents",
            sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column("source", sa.String(32), nullable=False),
            sa.Column("source_id", sa.String(64), nullable=False),
            sa.Column("family_id", sa.String(64), sa.ForeignKey("patent_families.family_id")),
            sa.Column("title", sa.Text()),
            sa.Column("abstract", sa.Text()),
            sa.Column("filing_date", sa.DateTime(timezone=True)),
            sa.Column("grant_date", sa.DateTime(timezone=True)),
            sa.Column("assignees", postgresql.JSONB()),
            sa.Column("inventors", postgresql.JSONB()),
            sa.Column("cpc_codes", postgresql.JSONB()),
            sa.Column("ipc_codes", postgresql.JSONB()),
            sa.Column("raw_payload", postgresql.JSONB()),
            sa.Column("matched_query", sa.Text()),
            sa.Column("first_seen_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("source", "source_id", name="uq_raw_source_id"),
        )
        op.create_index("ix_raw_filing_date", "raw_patents", ["filing_date"])
        op.create_index("ix_raw_family_id", "raw_patents", ["family_id"])
        op.create_index("ix_raw_source", "raw_patents", ["source"])

    if "ingest_runs" not in existing:
        op.create_table(
            "ingest_runs",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
            sa.Column("finished_at", sa.DateTime(timezone=True)),
            sa.Column("sources", postgresql.JSONB()),
            sa.Column("queries", postgresql.JSONB()),
            sa.Column("new_patents", sa.Integer(), default=0),
            sa.Column("updated_patents", sa.Integer(), default=0),
            sa.Column("errors", postgresql.JSONB()),
            sa.Column("success", sa.Boolean(), default=False),
            sa.PrimaryKeyConstraint("id"),
        )

    if "analysis_results" not in existing:
        op.create_table(
            "analysis_results",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("ingest_run_id", sa.Integer(), sa.ForeignKey("ingest_runs.id")),
            sa.Column("query", sa.Text()),
            sa.Column("patent_count", sa.Integer(), default=0),
            sa.Column("model", sa.String(64)),
            sa.Column("analysis_text", sa.Text()),
            sa.Column("themes", postgresql.JSONB()),
            sa.Column("top_assignees", postgresql.JSONB()),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
            sa.PrimaryKeyConstraint("id"),
        )


def downgrade() -> None:
    op.drop_table("analysis_results")
    op.drop_table("ingest_runs")
    op.drop_table("raw_patents")
    op.drop_table("patent_families")
