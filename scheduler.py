"""
Scheduler — wraps the pipeline in APScheduler.

Parses the SCHEDULE_CRON env var and runs the full ingestion
+ analysis pipeline on that cadence.

Run with:  python scheduler.py

For production, wrap this in a systemd service or Docker container.
The process is long-lived; APScheduler handles the cron loop internally.
"""
from __future__ import annotations

import asyncio
import logging

import structlog
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from analysis import analyze_batch, generate_weekly_digest
from config import settings
from db import init_db
from db.models import AnalysisResult
from notifiers import dispatch_digest
from pipeline import run_pipeline

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ]
)
log = structlog.get_logger()

logging.basicConfig(level=logging.INFO)


def _parse_cron(expr: str) -> dict:
    """Convert '0 2 * * *' to APScheduler CronTrigger kwargs."""
    parts = expr.strip().split()
    if len(parts) != 5:
        raise ValueError(f"Invalid cron expression: {expr!r}")
    minute, hour, day, month, day_of_week = parts
    return dict(
        minute=minute,
        hour=hour,
        day=day,
        month=month,
        day_of_week=day_of_week,
    )


async def _send_weekly_digest() -> None:
    """Send the weekly digest email/Slack regardless of new patent count."""
    from db import get_session
    from db.models import AnalysisResult, IngestRun
    from sqlalchemy import func

    log.info("scheduler: generating weekly digest")
    with get_session() as session:
        latest = (
            session.query(AnalysisResult)
            .order_by(AnalysisResult.created_at.desc())
            .first()
        )
        if latest:
            session.expunge(latest)
        total_new = session.query(func.sum(IngestRun.new_patents)).scalar() or 0

    digest = await generate_weekly_digest(
        new_count=total_new,
        sources=["patentsview", "epo", "lens", "bigquery"],
        queries=settings.query_list,
        latest_analysis=latest,
    )
    await dispatch_digest(digest_text=digest, new_count=total_new, run_id=0)
    log.info("scheduler: weekly digest dispatched")


async def _run_full_pipeline() -> None:
    log.info("scheduler: pipeline starting")

    result = await run_pipeline()

    log.info(
        "scheduler: pipeline complete",
        new=result.new_patents,
        updated=result.updated_patents,
        errors=len(result.errors),
    )

    if result.errors:
        for err in result.errors:
            log.error("scheduler: ingestion error", error=err)

    latest_analysis = None

    if result.new_patents >= settings.analysis_min_new:
        # Run analysis per query group using new records
        for query in settings.query_list:
            relevant = [
                p for p in result.new_records if p.matched_query == query
            ]
            if relevant:
                analysis = await analyze_batch(relevant, query, result.ingest_run_id)
                if analysis:
                    latest_analysis = analysis

        digest = await generate_weekly_digest(
            new_count=result.new_patents,
            sources=["patentsview", "epo", "lens", "bigquery"],
            queries=settings.query_list,
            latest_analysis=latest_analysis,
        )
        log.info("scheduler: dispatching digest (%d chars)", len(digest))
        await dispatch_digest(
            digest_text=digest,
            new_count=result.new_patents,
            run_id=result.ingest_run_id,
        )


def _sync_wrapper() -> None:
    """Synchronous wrapper for APScheduler — runs the async pipeline."""
    asyncio.run(_run_full_pipeline())


def main() -> None:
    init_db()
    log.info("scheduler: initializing", cron=settings.schedule_cron)

    scheduler = BlockingScheduler(timezone="UTC")
    cron_kwargs = _parse_cron(settings.schedule_cron)
    scheduler.add_job(
        _sync_wrapper,
        trigger=CronTrigger(**cron_kwargs),
        id="patent_pipeline",
        name="Patent ingestion + analysis",
        max_instances=1,
        misfire_grace_time=3600,
    )
    scheduler.add_job(
        lambda: asyncio.run(_send_weekly_digest()),
        trigger=CronTrigger(day_of_week="mon", hour=8, minute=0),
        id="weekly_digest",
        name="Weekly digest email/Slack",
        max_instances=1,
        misfire_grace_time=3600,
    )
    log.info("scheduler: pipeline cron=%s  digest=every Monday 08:00 UTC", settings.schedule_cron)

    try:
        scheduler.start()
    except KeyboardInterrupt:
        log.info("scheduler: shutting down")
        scheduler.shutdown()


if __name__ == "__main__":
    main()
