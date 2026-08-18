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
# Added 2026-08-18 — the narrative / leading-indicator layer.
from ingestors.signals.edgar import EdgarIngestor
from ingestors.signals.feeds import FeedIngestor
from ingestors.signals.jobs import JobBoardIngestor
from ingestors.signals.preprints import PreprintIngestor
from neuro_taxonomy import score_record

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
        # Narrative + leading-indicator sources. Each is free and
        # unauthenticated; each isolates its own per-source failures so one
        # dead feed can never zero a run.
        FeedIngestor(queries, since, per_page),
        PreprintIngestor(queries, since, per_page),
        JobBoardIngestor(queries, since, per_page),
        EdgarIngestor(queries, since, per_page,
                      contact_email=getattr(settings, "contact_email", "")
                      or "research@epsilonsolutionsllc.com"),
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

    # ── RELEVANCE GATE (added 2026-08-18) ────────────────────────────────────
    # NIH RePORTER and ClinicalTrials.gov do loose text matching, so a query
    # for "neurostimulation" returns things like "Integrated Multimodal Digital
    # Health Intervention for Type 2 Diabetes" and "The Unintended Consequences
    # of Consumer Wearables". Both appeared in the 2026-08-17 digest as
    # neurotech signals. Same class of error as the robot-vacuum patents.
    #
    # Sources that are neurotech-by-construction, or that already ran the gate
    # inside their own ingestor, bypass it here rather than being scored twice.
    PRE_FILTERED_PREFIXES = ("rss:", "jobs:", "arxiv", "biorxiv", "medrxiv", "sec_edgar")

    gated: list[NormalizedSignal] = []
    rejected = 0
    reject_examples: list[str] = []

    for s in all_signals:
        if any((s.source or "").startswith(p) for p in PRE_FILTERED_PREFIXES):
            gated.append(s)
            continue
        rel = score_record(s.title, s.summary, list(s.tags or []))
        if rel.tier in ("core", "adjacent"):
            s.raw_payload = {**(s.raw_payload or {}),
                             "relevance_score": rel.score,
                             "relevance_tier": rel.tier,
                             "relevance_reasons": rel.reasons}
            gated.append(s)
        else:
            rejected += 1
            if len(reject_examples) < 3 and s.title:
                reject_examples.append(s.title[:70])

    if rejected:
        log.info("signal pipeline: relevance gate dropped %d/%d off-topic records",
                 rejected, len(all_signals))
        for ex in reject_examples:
            log.info("signal pipeline:   rejected e.g. %r", ex)
    all_signals = gated

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


# Column length limits from db/signal_models.py. Anything longer is truncated
# before insert rather than being allowed to abort the whole stage.
#
# Added 2026-08-18 after a run died with:
#   DataError: value too long for type character varying(64)
# — an ATS job posting whose location string ("Remote - United States;
# San Francisco, CA; ...") overflowed `status`. One over-long string from one
# posting took down the entire signals stage, which is the same failure shape
# as the EPO 403 that zeroed the patent run: a single bad record must never
# cost the batch. Clamping centrally guards every current AND future ingestor,
# since field lengths are exactly the thing nobody remembers to check.
_MAXLEN = {"source": 32, "source_id": 128, "signal_type": 16, "status": 64}


def _clamp(sig: NormalizedSignal) -> NormalizedSignal:
    for field_name, limit in _MAXLEN.items():
        val = getattr(sig, field_name, None)
        if isinstance(val, str) and len(val) > limit:
            log.debug("signal pipeline: truncating %s (%d>%d) on %s:%s",
                      field_name, len(val), limit, sig.source, sig.source_id[:40])
            setattr(sig, field_name, val[:limit])
    return sig


def _upsert_signals(
    signals: list[NormalizedSignal],
) -> tuple[int, int, list[NormalizedSignal]]:
    new_count = 0
    updated_count = 0
    new_records: list[NormalizedSignal] = []

    signals = [_clamp(s) for s in signals]

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
