"""
SQLAlchemy model for PhD thesis records.

Kept in a separate file so it can be imported alongside db/models.py
without circular imports. Call init_thesis_db() after init_db().
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Index,
    Integer,
    SmallInteger,
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


class Thesis(Base):
    """
    One row per PhD thesis record from any source.

    Deduplication key: (source, source_id).
    Cross-source deduplication uses DOI when available, otherwise
    title + author + year fuzzy match (handled in thesis_pipeline.py).
    """

    __tablename__ = "theses"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    source_id: Mapped[str] = mapped_column(String(128), nullable=False)

    title: Mapped[str] = mapped_column(Text, nullable=False)
    abstract: Mapped[str | None] = mapped_column(Text)
    author: Mapped[str | None] = mapped_column(Text)
    institution: Mapped[str | None] = mapped_column(Text)
    country: Mapped[str | None] = mapped_column(String(8))
    year: Mapped[int | None] = mapped_column(SmallInteger, index=True)
    language: Mapped[str | None] = mapped_column(String(8))
    degree: Mapped[str | None] = mapped_column(String(32))

    keywords: Mapped[list | None] = mapped_column(JSONB)
    subjects: Mapped[list | None] = mapped_column(JSONB)

    url: Mapped[str | None] = mapped_column(Text)
    doi: Mapped[str | None] = mapped_column(String(256), index=True)

    hardware_relevant: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    software_relevant: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    matched_query: Mapped[str | None] = mapped_column(Text)

    raw_payload: Mapped[dict | None] = mapped_column(JSONB)

    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("source", "source_id", name="uq_thesis_source_id"),
        Index("ix_thesis_year", "year"),
        Index("ix_thesis_hardware", "hardware_relevant"),
        Index("ix_thesis_software", "software_relevant"),
        Index("ix_thesis_keywords", "keywords", postgresql_using="gin"),
        Index("ix_thesis_country", "country"),
    )
