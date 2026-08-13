"""Add signals table (grants, trials, FDA actions).

Revision ID: 0004_signals
Revises: 0003_backfill_orcid

NOTE: If the signals table already exists (created by init_db() /
init_signal_db()), stamp the DB to this revision without re-running DDL:
    alembic stamp 0004_signals
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy import inspect

revision = "0004_signals"
down_revision = "0003_backfill_orcid"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    existing = inspect(bind).get_table_names()

    if "signals" in existing:
        return  # already created by init_db() — nothing to do

    op.create_table(
        "signals",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("source_id", sa.String(128), nullable=False),
        sa.Column("signal_type", sa.String(16), nullable=False),
        sa.Column("title", sa.Text()),
        sa.Column("summary", sa.Text()),
        sa.Column("organization", sa.Text()),
        sa.Column("people", postgresql.JSONB()),
        sa.Column("amount", sa.BigInteger()),
        sa.Column("event_date", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(64)),
        sa.Column("tags", postgresql.JSONB()),
        sa.Column("url", sa.Text()),
        sa.Column("matched_query", sa.Text()),
        sa.Column("raw_payload", postgresql.JSONB()),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source", "source_id", name="uq_signal_source_id"),
    )
    op.create_index("ix_signal_event_date", "signals", ["event_date"])
    op.create_index("ix_signal_type", "signals", ["signal_type"])
    op.create_index("ix_signal_source", "signals", ["source"])
    op.create_index("ix_signal_tags", "signals", ["tags"], postgresql_using="gin")


def downgrade() -> None:
    op.drop_table("signals")
