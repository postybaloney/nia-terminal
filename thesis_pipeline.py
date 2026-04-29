"""
Thesis ingestion pipeline.

Responsibilities:
  1. Run all thesis ingestors concurrently.
  2. Deduplicate: first by (source, source_id), then by DOI across sources.
  3. Upsert Thesis rows — skip existing, update abstract if it changed.
  4. Return ThesisPipelineResult for the scheduler/CLI.

Runs alongside (but independently of) the patent pipeline.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from config import settings
from db import get_session
from db.thesis_models import Thesis
from ingestors.theses.base import NormalizedThesis
from ingestors.theses.openalex import OpenAlexIngestor

# NDLTD (harvest.ndltd.org) — ConnectError, host unreachable as of 2026-04
# DART-Europe (dart-europe.eu) — domain redirects to dartcrypto.com (squatted)
# EThOS (ethos.bl.uk/api) — 302 redirect to www.bl.uk, API no longer active
# All three removed from the active pipeline; ingestor files retained for when
# the services recover.

log = logging.getLogger(__name__)


@dataclass
class ThesisPipelineResult:
    new_theses: int
    updated_theses: int
    total_fetched: int
    errors: list[str]
    new_records: list[NormalizedThesis]


def init_thesis_db() -> None:
    """
    Create the theses table. Safe to call repeatedly.

    Thesis is registered on Base.metadata at import time (above), so
    calling init_db() will create it alongside all other tables.
    """
    from db import init_db
    init_db()
    log.info("thesis db: tables ready")


async def run_thesis_pipeline() -> ThesisPipelineResult:
    """Main entry point — called by scheduler and CLI."""
    queries = settings.thesis_query_list
    since_year = settings.thesis_since_year
    per_page = settings.per_page
    extra_kw = settings.thesis_extra_keywords_list

    ingestors = [
        OpenAlexIngestor(queries, since_year, per_page, extra_kw),
        # NDLTD, DART-Europe, EThOS all have dead/squatted endpoints as of 2026-04
    ]

    errors: list[str] = []
    all_theses: list[NormalizedThesis] = []

    fetch_tasks = [ing.fetch() for ing in ingestors]
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

    for ingestor, result in zip(ingestors, results):
        if isinstance(result, Exception):
            msg = f"{ingestor.name}: {type(result).__name__}: {result}"
            log.error(msg)
            errors.append(msg)
        else:
            relevant = [t for t in result if t.hardware_relevant or t.software_relevant]
            log.info(
                "thesis pipeline: %s → %d total, %d relevant",
                ingestor.name, len(result), len(relevant),
            )
            all_theses.extend(relevant)

    log.info("thesis pipeline: total relevant fetched: %d", len(all_theses))

    # Dedup pass 1: by (source, source_id)
    seen_source: dict[tuple, NormalizedThesis] = {}
    for t in all_theses:
        key = (t.source, t.source_id)
        if key not in seen_source:
            seen_source[key] = t
    deduped = list(seen_source.values())

    # Dedup pass 2: by DOI across sources
    seen_doi: dict[str, NormalizedThesis] = {}
    final: list[NormalizedThesis] = []
    for t in deduped:
        if t.doi:
            doi_clean = t.doi.lower().strip()
            if doi_clean in seen_doi:
                continue
            seen_doi[doi_clean] = t
        final.append(t)

    log.info("thesis pipeline: after dedup: %d unique records", len(final))

    new_count, updated_count, new_records = _upsert_theses(final)

    log.info(
        "thesis pipeline: done — new=%d updated=%d errors=%d",
        new_count, updated_count, len(errors),
    )
    return ThesisPipelineResult(
        new_theses=new_count,
        updated_theses=updated_count,
        total_fetched=len(final),
        errors=errors,
        new_records=new_records,
    )


def _upsert_theses(
    theses: list[NormalizedThesis],
) -> tuple[int, int, list[NormalizedThesis]]:
    new_count = 0
    updated_count = 0
    new_records: list[NormalizedThesis] = []

    with get_session() as session:
        for t in theses:
            existing = (
                session.query(Thesis)
                .filter_by(source=t.source, source_id=t.source_id)
                .first()
            )
            if existing is None:
                row = Thesis(
                    source=t.source,
                    source_id=t.source_id,
                    title=t.title,
                    abstract=t.abstract,
                    author=t.author,
                    institution=t.institution,
                    country=t.country,
                    year=t.year,
                    language=t.language,
                    degree=t.degree,
                    keywords=t.keywords or [],
                    subjects=t.subjects or [],
                    url=t.url,
                    doi=t.doi,
                    hardware_relevant=t.hardware_relevant,
                    software_relevant=t.software_relevant,
                    matched_query=t.matched_query,
                    raw_payload=t.raw_payload,
                )
                session.add(row)
                new_count += 1
                new_records.append(t)
            else:
                if t.abstract and existing.abstract != t.abstract:
                    existing.abstract = t.abstract
                    updated_count += 1

    return new_count, updated_count, new_records
