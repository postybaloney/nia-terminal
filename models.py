"""
SQLAlchemy 2.0 ORM models.

Core design decisions:
- PatentFamily is the canonical unit of deduplication (groups same invention
  across USPTO / EPO / WIPO filings via DOCDB family ID).
- RawPatent stores one row per source record with JSONB for flexible fields.
- AnalysisResult stores Claude landscape summaries per run + query.
- IngestRun tracks every scheduler execution for observability.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class PatentFamily(Base):
    """
    Deduplicated patent family — one row per invention across all sources.
    family_id is the DOCDB family ID assigned by EPO; for USPTO-only records
    that lack a family ID we generate a surrogate from the patent number.
    """

    __tablename__ = "patent_families"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    family_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    title: Mapped[str | None] = mapped_column(Text)
    abstract: Mapped[str | None] = mapped_column(Text)
    earliest_filing_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    earliest_grant_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    assignees: Mapped[list | None] = mapped_column(JSONB)          # [{name, country}]
    inventors: Mapped[list | None] = mapped_column(JSONB)          # [{name}]
    cpc_codes: Mapped[list | None] = mapped_column(JSONB)          # ["A61N1/36", ...]
    ipc_codes: Mapped[list | None] = mapped_column(JSONB)
    sources: Mapped[list | None] = mapped_column(JSONB)            # ["patentsview","epo"]
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, server_default=func.now()
    )

    raw_patents: Mapped[list[RawPatent]] = relationship(back_populates="family")

    __table_args__ = (
        Index("ix_families_earliest_filing", "earliest_filing_date"),
        Index("ix_families_assignees", "assignees", postgresql_using="gin"),
        Index("ix_families_cpc", "cpc_codes", postgresql_using="gin"),
    )


class RawPatent(Base):
    """
    One row per patent publication record from each source.
    Multiple rows can share the same patent_family_id.
    """

    __tablename__ = "raw_patents"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)   # patentsview|epo|lens|bigquery
    source_id: Mapped[str] = mapped_column(String(64), nullable=False) # patent number in that system
    family_id: Mapped[str | None] = mapped_column(
        String(64), ForeignKey("patent_families.family_id"), index=True
    )
    title: Mapped[str | None] = mapped_column(Text)
    abstract: Mapped[str | None] = mapped_column(Text)
    filing_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    grant_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    assignees: Mapped[list | None] = mapped_column(JSONB)
    inventors: Mapped[list | None] = mapped_column(JSONB)
    cpc_codes: Mapped[list | None] = mapped_column(JSONB)
    ipc_codes: Mapped[list | None] = mapped_column(JSONB)
    raw_payload: Mapped[dict | None] = mapped_column(JSONB)  # full source response
    matched_query: Mapped[str | None] = mapped_column(Text)  # which search query produced this
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=func.now()
    )

    family: Mapped[PatentFamily | None] = relationship(back_populates="raw_patents")

    __table_args__ = (
        UniqueConstraint("source", "source_id", name="uq_raw_source_id"),
        Index("ix_raw_filing_date", "filing_date"),
        Index("ix_raw_source", "source"),
    )


class IngestRun(Base):
    """Audit log of every pipeline execution."""

    __tablename__ = "ingest_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    sources: Mapped[list | None] = mapped_column(JSONB)      # which sources ran
    queries: Mapped[list | None] = mapped_column(JSONB)      # which queries ran
    new_patents: Mapped[int] = mapped_column(Integer, default=0)
    updated_patents: Mapped[int] = mapped_column(Integer, default=0)
    errors: Mapped[list | None] = mapped_column(JSONB)
    success: Mapped[bool] = mapped_column(Boolean, default=False)


class AnalysisResult(Base):
    """Claude landscape analysis snapshots, stored per run."""

    __tablename__ = "analysis_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ingest_run_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("ingest_runs.id"))
    query: Mapped[str | None] = mapped_column(Text)
    patent_count: Mapped[int] = mapped_column(Integer, default=0)
    model: Mapped[str] = mapped_column(String(64), default="claude-sonnet-4-20250514")
    analysis_text: Mapped[str | None] = mapped_column(Text)
    themes: Mapped[list | None] = mapped_column(JSONB)        # extracted theme list
    top_assignees: Mapped[list | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=func.now()
    )
