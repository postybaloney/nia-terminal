"""
Patent ingestion pipeline.

Responsibilities:
  1. Run all enabled ingestors concurrently.
  2. Deduplicate within the batch (same source_id seen from multiple queries).
  3. Upsert RawPatent rows — skip existing, update if abstract changed.
  4. Resolve / create PatentFamily rows using family_id; where none exists,
     generate a surrogate family_id from "{source}:{source_id}".
  5. Merge family metadata (prefer EPO/Lens records which carry family IDs).
  6. Return counts for the IngestRun audit log.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import settings
from db import get_session
from db.models import IngestRun, PatentFamily, RawPatent
from ingestors.base import NormalizedPatent
from ingestors.bigquery_ingestor import BigQueryIngestor
from ingestors.epo import EPOIngestor
from ingestors.lens import LensIngestor
from ingestors.patentsview import PatentsViewIngestor

log = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    ingest_run_id: int
    new_patents: int
    updated_patents: int
    total_fetched: int
    errors: list[str]
    new_records: list[NormalizedPatent]  # only newly inserted — for AI analysis


def _surrogate_family_id(source: str, source_id: str) -> str:
    """
    Generate a stable surrogate family_id for records without a real DOCDB ID.
    Uses a short SHA-1 prefix to keep it compact.
    """
    raw = f"{source}:{source_id}"
    return "S-" + hashlib.sha1(raw.encode()).hexdigest()[:16]


def _pick_family_id(patent: NormalizedPatent) -> str:
    if patent.family_id:
        return patent.family_id
    return _surrogate_family_id(patent.source, patent.source_id)


def _best_value(*values):
    """Return first non-None, non-empty value."""
    for v in values:
        if v:
            return v
    return None


async def run_pipeline() -> PipelineResult:
    """Main entry point — called by scheduler and CLI."""
    queries = settings.query_list
    since = settings.backfill_from
    per_page = settings.per_page

    ingestors = [
        PatentsViewIngestor(queries, since, per_page),
        EPOIngestor(queries, since, per_page),
        LensIngestor(queries, since, per_page),
        BigQueryIngestor(queries, since, per_page),
    ]

    # Create IngestRun row
    with get_session() as session:
        run = IngestRun(
            started_at=datetime.now(timezone.utc),
            sources=[i.name for i in ingestors],
            queries=queries,
        )
        session.add(run)
        session.flush()
        run_id = run.id

    errors: list[str] = []
    all_patents: list[NormalizedPatent] = []

    # Fetch from all sources concurrently
    fetch_tasks = [ingestor.fetch() for ingestor in ingestors]
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

    for ingestor, result in zip(ingestors, results):
        if isinstance(result, Exception):
            msg = f"{ingestor.name}: {type(result).__name__}: {result}"
            log.error(msg)
            errors.append(msg)
        else:
            all_patents.extend(result)

    log.info("pipeline: total fetched across all sources: %d", len(all_patents))

    # Quality gate — require an abstract.
    # Records without an abstract are useless for AI analysis and search.
    # (ODP/USPTO records have titles but no abstracts — Lens covers them better.)
    quality_filtered = [p for p in all_patents if p.abstract and p.abstract.strip()]
    dropped = len(all_patents) - len(quality_filtered)
    if dropped:
        by_source: dict[str, int] = {}
        for p in all_patents:
            if not (p.abstract and p.abstract.strip()):
                by_source[p.source] = by_source.get(p.source, 0) + 1
        log.info(
            "pipeline: quality gate dropped %d records without abstract: %s",
            dropped, by_source,
        )
    all_patents = quality_filtered

    # Deduplicate within batch by (source, source_id)
    seen: dict[tuple, NormalizedPatent] = {}
    for p in all_patents:
        key = (p.source, p.source_id)
        if key not in seen:
            seen[key] = p
    deduped = list(seen.values())
    log.info("pipeline: after dedup: %d unique records", len(deduped))

    # Write to DB
    new_count, updated_count, new_records = _upsert_patents(deduped)

    # Finalize IngestRun
    with get_session() as session:
        run = session.get(IngestRun, run_id)
        if run:
            run.finished_at = datetime.now(timezone.utc)
            run.new_patents = new_count
            run.updated_patents = updated_count
            run.errors = errors or None
            run.success = len(errors) == 0

    log.info(
        "pipeline: done — new=%d  updated=%d  errors=%d",
        new_count, updated_count, len(errors),
    )
    return PipelineResult(
        ingest_run_id=run_id,
        new_patents=new_count,
        updated_patents=updated_count,
        total_fetched=len(deduped),
        errors=errors,
        new_records=new_records,
    )


def _upsert_patents(
    patents: list[NormalizedPatent],
) -> tuple[int, int, list[NormalizedPatent]]:
    """
    Upsert RawPatent rows. For each new record, ensure a PatentFamily exists.
    Returns (new_count, updated_count, newly_inserted_records).
    """
    new_count = 0
    updated_count = 0
    new_records: list[NormalizedPatent] = []

    with get_session() as session:
        for patent in patents:
            family_id = _pick_family_id(patent)

            # Ensure PatentFamily exists
            family = (
                session.query(PatentFamily)
                .filter_by(family_id=family_id)
                .first()
            )
            if family is None:
                family = PatentFamily(
                    family_id=family_id,
                    title=patent.title,
                    abstract=patent.abstract,
                    earliest_filing_date=patent.filing_date.replace(tzinfo=None) if patent.filing_date else None,
                    earliest_grant_date=patent.grant_date.replace(tzinfo=None) if patent.grant_date else None,
                    assignees=patent.assignees or [],
                    inventors=patent.inventors or [],
                    cpc_codes=patent.cpc_codes or [],
                    ipc_codes=patent.ipc_codes or [],
                    sources=[patent.source],
                )
                session.add(family)
                session.flush()
            else:
                # Merge: prefer records with real family IDs (EPO/Lens) over surrogates
                _merge_family(family, patent)

            # Upsert RawPatent
            existing = (
                session.query(RawPatent)
                .filter_by(source=patent.source, source_id=patent.source_id)
                .first()
            )
            if existing is None:
                raw = RawPatent(
                    source=patent.source,
                    source_id=patent.source_id,
                    family_id=family_id,
                    title=patent.title,
                    abstract=patent.abstract,
                    filing_date=patent.filing_date.replace(tzinfo=None) if patent.filing_date else None,
                    grant_date=patent.grant_date.replace(tzinfo=None) if patent.grant_date else None,
                    assignees=patent.assignees,
                    inventors=patent.inventors,
                    cpc_codes=patent.cpc_codes,
                    ipc_codes=patent.ipc_codes,
                    matched_query=patent.matched_query,
                    raw_payload=patent.raw_payload,
                )
                session.add(raw)
                new_count += 1
                new_records.append(patent)
            else:
                # Update abstract if it changed (e.g., source corrected data)
                if patent.abstract and existing.abstract != patent.abstract:
                    existing.abstract = patent.abstract
                    updated_count += 1

    return new_count, updated_count, new_records


def _merge_family(family: PatentFamily, patent: NormalizedPatent) -> None:
    """Merge incoming patent data into an existing family row."""
    # Fill in missing fields
    if not family.title and patent.title:
        family.title = patent.title
    if not family.abstract and patent.abstract:
        family.abstract = patent.abstract

    # Extend CPC/IPC code lists
    existing_cpcs = set(family.cpc_codes or [])
    new_cpcs = set(patent.cpc_codes or [])
    if new_cpcs - existing_cpcs:
        family.cpc_codes = list(existing_cpcs | new_cpcs)

    # Track sources
    existing_sources = set(family.sources or [])
    if patent.source not in existing_sources:
        family.sources = list(existing_sources | {patent.source})

    # Update earliest dates
    # Strip timezone info before comparing — the DB stores naive datetimes
    # but some ingestors (EPO, Lens) return timezone-aware ones.
    if patent.filing_date:
        filing = patent.filing_date.replace(tzinfo=None)
        existing = family.earliest_filing_date
        existing_naive = existing.replace(tzinfo=None) if existing else None
        if not existing_naive or filing < existing_naive:
            family.earliest_filing_date = filing
