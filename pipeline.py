"""
Patent ingestion pipeline.

Responsibilities:
  1. Run all enabled ingestors concurrently.
  2. Apply the neurotech RELEVANCE GATE (added 2026-08-18).
  3. Deduplicate within the batch (same source_id seen from multiple queries).
  4. Upsert RawPatent rows — skip existing, update if abstract changed.
  5. Resolve / create PatentFamily rows using family_id; where none exists,
     generate a surrogate family_id from "{source}:{source_id}".
  6. Merge family metadata (prefer EPO records which carry family IDs).
  7. Return counts for the IngestRun audit log.

────────────────────────────────────────────────────────────────────────────
2026-08-18 changes:

  * RELEVANCE GATE. Every record is scored by neuro_taxonomy.score_record and
    anything scoring below the accept threshold is dropped with a logged
    reason. This is what stops a robot-vacuum patent from being counted as
    neurotech. Rejects are counted by reason so the run explains itself.

  * PARTIAL-FAILURE REPORTING. Ingestors now surface per-query failures via
    .query_errors instead of raising. A run where 11 of 12 queries succeeded
    reports as a partial success with 11 queries' worth of data, rather than
    an exception that discards all of it.

  * HONEST COUNTS. total_fetched now means what it says. The previous version
    assigned len(deduped) to a field named total_fetched, so the number shrank
    silently as dedup improved.
────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone

from config import settings
from db import get_session
from db.models import IngestRun, PatentFamily, RawPatent
from ingestors.base import NormalizedPatent
from ingestors.bigquery_ingestor import BigQueryIngestor
from ingestors.epo import EPOIngestor
from neuro_taxonomy import score_record

# Lens.org removed 2026-08: its free API is licensed for NONCOMMERCIAL use only.
# LensIngestor and the Lens-backed PatentsViewIngestor are unregistered but the
# files are retained — re-add here only under a signed Lens Commercial Use
# Agreement (https://about.lens.org/individual-commercial-use/).
# US coverage now comes from EPO OPS (worldwide DOCDB) + BigQuery Google Patents.

log = logging.getLogger(__name__)

# Accept "core" and "adjacent"; drop "reject"; route "cardiac" out of the
# neurotech corpus (it is real medtech, just not what NIA claims to cover).
ACCEPT_TIERS = {"core", "adjacent"}


@dataclass
class PipelineResult:
    ingest_run_id: int
    new_patents: int
    updated_patents: int
    total_fetched: int                 # raw records returned by all sources
    total_relevant: int                # survivors of the relevance gate
    total_unique: int                  # survivors of dedup
    errors: list[str]
    new_records: list[NormalizedPatent]  # only newly inserted — for AI analysis
    rejected_by_reason: dict = field(default_factory=dict)
    partial_failures: list[str] = field(default_factory=list)


def _surrogate_family_id(source: str, source_id: str) -> str:
    """Stable surrogate family_id for records without a real DOCDB ID."""
    raw = f"{source}:{source_id}"
    return "S-" + hashlib.sha1(raw.encode()).hexdigest()[:16]


def _pick_family_id(patent: NormalizedPatent) -> str:
    if patent.family_id:
        return patent.family_id
    return _surrogate_family_id(patent.source, patent.source_id)


async def run_pipeline() -> PipelineResult:
    """Main entry point — called by scheduler and CLI."""
    queries = settings.query_list
    since = settings.backfill_from
    per_page = settings.per_page

    ingestors = [
        EPOIngestor(queries, since, per_page),
        BigQueryIngestor(queries, since, per_page),
    ]

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
    partial_failures: list[str] = []
    all_patents: list[NormalizedPatent] = []

    fetch_tasks = [ingestor.fetch() for ingestor in ingestors]
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

    for ingestor, result in zip(ingestors, results):
        if isinstance(result, Exception):
            msg = f"{ingestor.name}: {type(result).__name__}: {result}"
            log.error(msg)
            errors.append(msg)
        else:
            all_patents.extend(result)
            # Per-query failures are partial, not fatal — report separately so
            # a run isn't marked failed just because one query 403'd.
            for qe in getattr(ingestor, "query_errors", []) or []:
                partial_failures.append(f"{ingestor.name}: {qe}")

    total_fetched = len(all_patents)
    log.info("pipeline: total fetched across all sources: %d", total_fetched)
    if partial_failures:
        log.warning(
            "pipeline: %d query-level failures (run continues): %s",
            len(partial_failures), "; ".join(partial_failures[:3]),
        )

    # ── Quality gate — require an abstract ───────────────────────────────────
    quality_filtered = [p for p in all_patents if p.abstract and p.abstract.strip()]
    dropped = total_fetched - len(quality_filtered)
    if dropped:
        by_source: dict[str, int] = {}
        for p in all_patents:
            if not (p.abstract and p.abstract.strip()):
                by_source[p.source] = by_source.get(p.source, 0) + 1
        log.info(
            "pipeline: quality gate dropped %d records without abstract: %s",
            dropped, by_source,
        )

    # ── RELEVANCE GATE ───────────────────────────────────────────────────────
    relevant: list[NormalizedPatent] = []
    reject_reasons: Counter = Counter()
    tier_counts: Counter = Counter()

    for p in quality_filtered:
        rel = score_record(p.title, p.abstract, p.cpc_codes, p.ipc_codes)
        p.relevance_score = rel.score
        p.relevance_tier = rel.tier
        p.relevance_reasons = rel.reasons
        tier_counts[rel.tier] += 1

        if rel.tier in ACCEPT_TIERS:
            relevant.append(p)
        else:
            reason = rel.reasons[0] if rel.reasons else "no neurotech signal"
            reject_reasons[reason] += 1

    log.info(
        "pipeline: relevance gate — kept %d/%d (%s)",
        len(relevant), len(quality_filtered), dict(tier_counts),
    )
    if reject_reasons:
        top = ", ".join(f"{r} x{n}" for r, n in reject_reasons.most_common(5))
        log.info("pipeline: top rejection reasons — %s", top)

    # ── Deduplicate within batch by (source, source_id) ──────────────────────
    seen: dict[tuple, NormalizedPatent] = {}
    for p in relevant:
        key = (p.source, p.source_id)
        if key not in seen:
            seen[key] = p
    deduped = list(seen.values())
    log.info("pipeline: after dedup: %d unique records", len(deduped))

    new_count, updated_count, new_records = _upsert_patents(deduped)

    with get_session() as session:
        run = session.get(IngestRun, run_id)
        if run:
            run.finished_at = datetime.now(timezone.utc)
            run.new_patents = new_count
            run.updated_patents = updated_count
            run.errors = (errors + partial_failures) or None
            # Query-level failures don't make the run a failure. Source-level
            # ones do.
            run.success = len(errors) == 0

    log.info(
        "pipeline: done — new=%d  updated=%d  errors=%d  partial=%d",
        new_count, updated_count, len(errors), len(partial_failures),
    )
    return PipelineResult(
        ingest_run_id=run_id,
        new_patents=new_count,
        updated_patents=updated_count,
        total_fetched=total_fetched,
        total_relevant=len(relevant),
        total_unique=len(deduped),
        errors=errors,
        new_records=new_records,
        rejected_by_reason=dict(reject_reasons.most_common(10)),
        partial_failures=partial_failures,
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
                    earliest_filing_date=(
                        patent.filing_date.replace(tzinfo=None)
                        if patent.filing_date else None
                    ),
                    earliest_grant_date=(
                        patent.grant_date.replace(tzinfo=None)
                        if patent.grant_date else None
                    ),
                    assignees=patent.assignees or [],
                    inventors=patent.inventors or [],
                    cpc_codes=patent.cpc_codes or [],
                    ipc_codes=patent.ipc_codes or [],
                    sources=[patent.source],
                )
                session.add(family)
                session.flush()
            else:
                _merge_family(family, patent)

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
                    filing_date=(
                        patent.filing_date.replace(tzinfo=None)
                        if patent.filing_date else None
                    ),
                    grant_date=(
                        patent.grant_date.replace(tzinfo=None)
                        if patent.grant_date else None
                    ),
                    assignees=patent.assignees,
                    inventors=patent.inventors,
                    cpc_codes=patent.cpc_codes,
                    ipc_codes=patent.ipc_codes,
                    matched_query=patent.matched_query,
                    raw_payload={
                        **(patent.raw_payload or {}),
                        "relevance_score": patent.relevance_score,
                        "relevance_tier": patent.relevance_tier,
                        "relevance_reasons": patent.relevance_reasons,
                    },
                )
                session.add(raw)
                new_count += 1
                new_records.append(patent)
            else:
                if patent.abstract and existing.abstract != patent.abstract:
                    existing.abstract = patent.abstract
                    updated_count += 1

    return new_count, updated_count, new_records


def _merge_family(family: PatentFamily, patent: NormalizedPatent) -> None:
    """Merge incoming patent data into an existing family row."""
    if not family.title and patent.title:
        family.title = patent.title
    if not family.abstract and patent.abstract:
        family.abstract = patent.abstract

    existing_cpcs = set(family.cpc_codes or [])
    new_cpcs = set(patent.cpc_codes or [])
    if new_cpcs - existing_cpcs:
        family.cpc_codes = list(existing_cpcs | new_cpcs)

    existing_sources = set(family.sources or [])
    if patent.source not in existing_sources:
        family.sources = list(existing_sources | {patent.source})

    # Strip timezone info before comparing — the DB stores naive datetimes but
    # some ingestors return timezone-aware ones.
    if patent.filing_date:
        filing = patent.filing_date.replace(tzinfo=None)
        existing = family.earliest_filing_date
        existing_naive = existing.replace(tzinfo=None) if existing else None
        if not existing_naive or filing < existing_naive:
            family.earliest_filing_date = filing
