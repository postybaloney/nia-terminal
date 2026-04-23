"""
CLI entry point for manual operations.

Usage:
  python main.py init          # create tables
  python main.py run           # single pipeline run (all sources)
  python main.py run --source patentsview   # single source
  python main.py backfill --from 2020-01-01 # re-index from a date
  python main.py digest        # generate and print weekly digest
  python main.py scheduler     # start the cron scheduler (blocking)
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from rich.console import Console
from rich.table import Table

console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)


async def cmd_run(source_filter: str | None) -> None:
    from analysis import analyze_batch
    from config import settings
    from pipeline import run_pipeline

    console.print("[bold]Running patent ingestion pipeline...[/bold]")
    result = await run_pipeline()

    table = Table(title="Pipeline Result")
    table.add_column("Metric", style="dim")
    table.add_column("Value", style="bold")
    table.add_row("New patents", str(result.new_patents))
    table.add_row("Updated", str(result.updated_patents))
    table.add_row("Total fetched", str(result.total_fetched))
    table.add_row("Errors", str(len(result.errors)))
    console.print(table)

    if result.errors:
        for err in result.errors:
            console.print(f"[red]  Error: {err}[/red]")

    if result.new_patents >= settings.analysis_min_new:
        console.print("\n[bold]Running AI analysis...[/bold]")
        for query in settings.query_list:
            relevant = [p for p in result.new_records if p.matched_query == query]
            if relevant:
                analysis = await analyze_batch(relevant, query, result.ingest_run_id)
                if analysis and analysis.themes:
                    console.print(f"  [green]✓[/green] {query!r}: themes={analysis.themes}")
    else:
        console.print(
            f"\n[dim]Skipping analysis ({result.new_patents} new patents < "
            f"min threshold {settings.analysis_min_new})[/dim]"
        )


async def cmd_backfill(since: str) -> None:
    from config import settings
    original = settings.backfill_from
    settings.backfill_from = since  # override for this run
    console.print(f"[bold]Backfill from {since}...[/bold]")
    await cmd_run(None)
    settings.backfill_from = original


async def cmd_analyze() -> None:
    """Run AI analysis on patents already in the database, grouped by query."""
    from analysis import analyze_batch
    from config import settings
    from db import get_session
    from db.models import IngestRun, RawPatent
    from ingestors.base import NormalizedPatent

    with get_session() as session:
        latest_run = (
            session.query(IngestRun.id)
            .order_by(IngestRun.started_at.desc())
            .limit(1)
            .scalar()
        )
        if not latest_run:
            console.print("[red]No ingest runs found. Run 'python main.py run' first.[/red]")
            return

        rows = (
            session.query(RawPatent)
            .filter(RawPatent.matched_query.isnot(None))
            .all()
        )
        # Convert ORM rows to NormalizedPatent so analyze_batch can format them
        patents_by_query: dict[str, list[NormalizedPatent]] = {}
        for r in rows:
            np = NormalizedPatent(
                source=r.source or "",
                source_id=r.source_id or "",
                family_id=None,
                title=r.title,
                abstract=r.abstract,
                filing_date=r.filing_date,
                grant_date=r.grant_date,
                assignees=r.assignees or [],
                inventors=[],
                cpc_codes=r.cpc_codes or [],
                ipc_codes=[],
                matched_query=r.matched_query or "",
                raw_payload={},
            )
            key = r.matched_query or ""
            patents_by_query.setdefault(key, []).append(np)

    run_id = latest_run
    console.print(f"[bold]Running AI analysis on existing records (ingest_run_id={run_id})...[/bold]")

    for query, patents in patents_by_query.items():
        console.print(f"  Analyzing {len(patents)} patents for query {query!r}")
        analysis = await analyze_batch(patents, query, run_id)
        if analysis and analysis.themes:
            console.print(f"  [green]✓[/green] themes={analysis.themes}")
        else:
            console.print(f"  [dim]skipped (too few patents or LLM error)[/dim]")


async def cmd_digest(send: bool = False) -> None:
    from config import settings
    from analysis import generate_weekly_digest
    from db import get_session
    from db.models import AnalysisResult, IngestRun
    from notifiers import dispatch_digest

    with get_session() as session:
        from sqlalchemy import func
        latest = (
            session.query(AnalysisResult)
            .order_by(AnalysisResult.created_at.desc())
            .first()
        )
        if latest:
            session.expunge(latest)
        total_new = (
            session.query(func.sum(IngestRun.new_patents))
            .scalar() or 0
        )

    digest = await generate_weekly_digest(
        new_count=total_new,
        sources=["patentsview", "epo", "lens", "bigquery"],
        queries=settings.query_list,
        latest_analysis=latest,
    )
    console.print("\n[bold]--- WEEKLY PATENT INTELLIGENCE DIGEST ---[/bold]\n")
    console.print(digest)

    if send:
        console.print("\n[bold]Dispatching digest via email/Slack...[/bold]")
        await dispatch_digest(digest_text=digest, new_count=total_new, run_id=0)
        console.print("[green]✓ Dispatch complete[/green]")


def cmd_init() -> None:
    from db import init_db
    init_db()
    console.print("[green]✓ Database tables created[/green]")


def cmd_scheduler() -> None:
    import scheduler as sched_module
    sched_module.main()


def main() -> None:
    parser = argparse.ArgumentParser(description="Patent Intelligence CLI")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("init", help="Initialize database tables")

    run_p = sub.add_parser("run", help="Run ingestion pipeline once")
    run_p.add_argument("--source", help="Limit to one source (patentsview|epo|lens|bigquery)")

    bf_p = sub.add_parser("backfill", help="Re-ingest from a historical date")
    bf_p.add_argument("--from", dest="since", required=True, help="Start date YYYY-MM-DD")

    sub.add_parser("analyze", help="Run AI analysis on patents already in the database")
    digest_p = sub.add_parser("digest", help="Generate weekly digest")
    digest_p.add_argument("--send", action="store_true", help="Send via email/Slack after printing")
    sub.add_parser("scheduler", help="Start cron scheduler (blocking)")

    args = parser.parse_args()

    if args.command == "init":
        cmd_init()
    elif args.command == "analyze":
        asyncio.run(cmd_analyze())
    elif args.command == "run":
        asyncio.run(cmd_run(getattr(args, "source", None)))
    elif args.command == "backfill":
        asyncio.run(cmd_backfill(args.since))
    elif args.command == "digest":
        asyncio.run(cmd_digest(send=getattr(args, "send", False)))
    elif args.command == "scheduler":
        cmd_scheduler()
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
