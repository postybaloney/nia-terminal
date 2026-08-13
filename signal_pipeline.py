"""
Current-signal ingestion pipeline (grants, trials, FDA actions).

Responsibilities:
  1. Run all signal ingestors concurrently.
  2. Deduplicate by (source, source_id).
  3. Upsert Signal rows — skip existing, update status/summary if changed.
  4. Return SignalPipelineResult for the scheduler/CLI.

Runs alongside (but independently of) the patent and thesis pipelines.
Why this exists: patents lag reality by ~18 months. Grants, trial
registrations, and FDA clearances are what's happening *now* — they are
the layer that makes the digest intelligence rather than archaeology.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from config import settings
from db import get_session
from db.signal_models import Signal
from ingestors.signals.base import NormalizedSignal
from ingestors.signals.clinicaltrials import ClinicalTrialsIngestor
from ingestors.signals.nih_reporter import NIHReporterIngestor
from ingestors.signals.openfda import OpenFDAIngestor

log = logging.getLogger(__name__)


@dataclass
class SignalPipelineResult:
    new_signals: int
    updated_signals: int
    total_fetched: int
    errors: list[str]
    new_records: list[NormalizedSignal]


def init_signal_db() -> None:
    """
    Create the signals table. Safe to call repeatedly.

    Signal is registered on Base.metadata at import time (above), so
    calling init_db() will create it alongside all other tables.
    """
    from db import init_db
    init_db()
    log.info("signal db: tables ready")


async def run_signal_pipeline() -> SignalPipelineResult:
    """Main entry point — called by scheduler and CLI."""
    queries = settings.signal_query_list
    since = settings.signal_since
    per_page = settings.per_page

    ingestors = [
        NIHReporterIngestor(queries, since, per_page),
        ClinicalTrialsIngestor(queries, since, per_page),
        OpenFDAIngestor(queries, since, per_page),
    ]

    errors: list[str] = []
    all_signals: list[NormalizedSignal] = []

    fetch_tasks = [ing.fetch() for ing in ingestors]
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

    for ingestor, result in zip(ingestors, results):
        if isinstance(result, Exception):
            msg = f"{ingestor.name}: {type(result).__name__}: {result}"
            log.error(msg)
            errors.append(msg)
        else:
            log.info("signal pipeline: %s → %d records", ingestor.name, len(result))
            all_signals.extend(result)

    log.info("signal pipeline: total fetched: %d", len(all_signals))

    # Dedup by (source, source_id) — a record can match multiple queries
    seen: dict[tuple, NormalizedSignal] = {}
    for s in all_signals:
        if not s.source_id:
            continue
        key = (s.source, s.source_id)
        if key not in seen:
            seen[key] = s
    deduped = list(seen.values())
    log.info("signal pipeline: after dedup: %d unique records", len(deduped))

    new_count, updated_count, new_records = _upsert_signals(deduped)

    log.info(
        "signal pipeline: done — new=%d updated=%d errors=%d",
        new_count, updated_count, len(errors),
    )
    return SignalPipelineResult(
        new_signals=new_count,
        updated_signals=updated_count,
        total_fetched=len(deduped),
        errors=errors,
        new_records=new_records,
    )


def _upsert_signals(
    signals: list[NormalizedSignal],
) -> tuple[int, int, list[NormalizedSignal]]:
    new_count = 0
    updated_count = 0
    new_records: list[NormalizedSignal] = []

    with get_session() as session:
        for s in signals:
            existing = (
                session.query(Signal)
                .filter_by(source=s.source, source_id=s.source_id)
                .first()
            )
            if existing is None:
                row = Signal(
                    source=s.source,
                    source_id=s.source_id,
                    signal_type=s.signal_type,
                    title=s.title,
                    summary=s.summary,
                    organization=s.organization,
                    people=s.people or [],
                    amount=s.amount,
                    event_date=s.event_date.replace(tzinfo=None) if s.event_date else None,
                    status=s.status,
                    tags=s.tags or [],
                    url=s.url,
                    matched_query=s.matched_query,
                    raw_payload=s.raw_payload,
                )
                session.add(row)
                new_count += 1
                new_records.append(s)
            else:
                changed = False
                # Trials change status (Recruiting → Completed); track it.
                if s.status and existing.status != s.status:
                    existing.status = s.status
                    changed = True
                if s.summary and existing.summary != s.summary:
                    existing.summary = s.summary
                    changed = True
                if changed:
                    updated_count += 1

    return new_count, updated_count, new_records
