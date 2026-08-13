"""
SQLAlchemy model for current-signal records (grants, trials, FDA actions).

Kept in a separate file so it can be imported alongside db/models.py
without circular imports — same pattern as db/thesis_models.py.
Signal registers itself on Base.metadata so calling init_db() creates
this table automatically.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    DateTime,
    Index,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from db.models import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Signal(Base):
    """
    One row per current-signal record from any source.

    Sources / types:
      nih_reporter   → grant       (NIH RePORTER v2 API)
      clinicaltrials → trial       (ClinicalTrials.gov v2 API)
      fda_510k       → clearance   (openFDA device/510k)
      fda_pma        → approval    (openFDA device/pma)

    Deduplication key: (source, source_id).
    """

    __tablename__ = "signals"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    source_id: Mapped[str] = mapped_column(String(128), nullable=False)
    signal_type: Mapped[str] = mapped_column(String(16), nullable=False, index=True)

    title: Mapped[str | None] = mapped_column(Text)
    summary: Mapped[str | None] = mapped_column(Text)
    organization: Mapped[str | None] = mapped_column(Text)
    people: Mapped[list | None] = mapped_column(JSONB)      # [{name, role}]
    amount: Mapped[int | None] = mapped_column(BigInteger)  # USD, grants only
    event_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    status: Mapped[str | None] = mapped_column(String(64))
    tags: Mapped[list | None] = mapped_column(JSONB)        # conditions, product codes

    url: Mapped[str | None] = mapped_column(Text)
    matched_query: Mapped[str | None] = mapped_column(Text)
    raw_payload: Mapped[dict | None] = mapped_column(JSONB)

    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("source", "source_id", name="uq_signal_source_id"),
        Index("ix_signal_event_date", "event_date"),
        Index("ix_signal_type", "signal_type"),
        Index("ix_signal_source", "source"),
        Index("ix_signal_tags", "tags", postgresql_using="gin"),
    )
