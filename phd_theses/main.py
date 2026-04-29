"""
CLI entry point.

Usage:
  python main.py init              # create all tables (patents + theses)
  python main.py run               # patent ingestion run
  python main.py run-theses        # thesis ingestion run
  python main.py run-all           # patents + theses together
  python main.py backfill --from 2020-01-01
  python main.py digest            # patent weekly digest
  python main.py digest-theses     # thesis weekly digest
  python main.py scheduler         # start cron scheduler (blocking)
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

    table = Table(title="Patent Pipeline Result")
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
        console.print("\n[bold]Running AI analysis on patents...[/bold]")
        for query in settings.query_list:
            relevant = [p for p in result.new_records if p.matched_query == query]
            if relevant:
                analysis = await analyze_batch(relevant, query, result.ingest_run_id)
                if analysis and analysis.themes:
                    console.print(f"  [green]✓[/green] {query!r}: themes={analysis.themes}")


async def cmd_run_theses() -> None:
    from config import settings
    from thesis_analysis import analyze_thesis_batch
    from thesis_pipeline import run_thesis_pipeline

    console.print("[bold]Running thesis ingestion pipeline...[/bold]")
    result = await run_thesis_pipeline()

    table = Table(title="Thesis Pipeline Result")
    table.add_column("Metric", style="dim")
    table.add_column("Value", style="bold")
    table.add_row("New theses", str(result.new_theses))
    table.add_row("Updated", str(result.updated_theses))
    table.add_row("Total fetched", str(result.total_fetched))
    hw = sum(1 for t in result.new_records if t.hardware_relevant)
    sw = sum(1 for t in result.new_records if t.software_relevant)
    table.add_row("Hardware-relevant", str(hw))
    table.add_row("Software-relevant", str(sw))
    table.add_row("Errors", str(len(result.errors)))
    console.print(table)

    if result.errors:
        for err in result.errors:
            console.print(f"[red]  Error: {err}[/red]")

    if result.new_theses >= settings.analysis_min_new:
        console.print("\n[bold]Running AI analysis on theses...[/bold]")
        analysis = await analyze_thesis_batch(result.new_records)
        if analysis and analysis.themes:
            console.print(f"  [green]✓[/green] Thesis themes: {analysis.themes}")


async def cmd_run_all() -> None:
    console.print("[bold]Running full pipeline: patents + theses...[/bold]")
    await asyncio.gather(cmd_run(None), cmd_run_theses())


async def cmd_backfill(since: str) -> None:
    from config import settings
    settings.backfill_from = since
    console.print(f"[bold]Patent backfill from {since}...[/bold]")
    await cmd_run(None)


async def cmd_digest() -> None:
    from analysis import generate_weekly_digest
    from config import settings
    from db import get_session
    from db.models import AnalysisResult

    with get_session() as session:
        latest = (
            session.query(AnalysisResult)
            .filter(AnalysisResult.query != "[thesis_batch]")
            .order_by(AnalysisResult.created_at.desc())
            .first()
        )

    digest = await generate_weekly_digest(
        new_count=0,
        sources=["patentsview", "epo", "lens", "bigquery"],
        queries=settings.query_list,
        latest_analysis=latest,
    )
    console.print("\n[bold]--- PATENT INTELLIGENCE DIGEST ---[/bold]\n")
    console.print(digest)


async def cmd_digest_theses() -> None:
    from db import get_session
    from db.models import AnalysisResult
    from db.thesis_models import Thesis
    from thesis_analysis import generate_thesis_digest

    with get_session() as session:
        latest = (
            session.query(AnalysisResult)
            .filter(AnalysisResult.query == "[thesis_batch]")
            .order_by(AnalysisResult.created_at.desc())
            .first()
        )
        hw_count = session.query(Thesis).filter_by(hardware_relevant=True).count()
        sw_count = session.query(Thesis).filter_by(software_relevant=True).count()
        total = session.query(Thesis).count()

    digest = await generate_thesis_digest(
        new_count=total,
        hw_count=hw_count,
        sw_count=sw_count,
        latest_analysis=latest,
    )
    console.print("\n[bold]--- THESIS RESEARCH INTELLIGENCE DIGEST ---[/bold]\n")
    console.print(digest)


def cmd_init() -> None:
    from db import init_db
    from thesis_pipeline import init_thesis_db
    init_db()
    init_thesis_db()
    console.print("[green]✓ All database tables created (patents + theses)[/green]")


def cmd_scheduler() -> None:
    import scheduler as sched_module
    sched_module.main()


def main() -> None:
    parser = argparse.ArgumentParser(description="Patent & Thesis Intelligence CLI")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("init", help="Initialize all database tables")

    run_p = sub.add_parser("run", help="Run patent ingestion pipeline")
    run_p.add_argument("--source", help="Limit to one source")

    sub.add_parser("run-theses", help="Run thesis ingestion pipeline")
    sub.add_parser("run-all", help="Run patents + theses together")

    bf_p = sub.add_parser("backfill", help="Re-ingest patents from historical date")
    bf_p.add_argument("--from", dest="since", required=True, help="YYYY-MM-DD")

    sub.add_parser("digest", help="Generate patent weekly digest")
    sub.add_parser("digest-theses", help="Generate thesis research digest")
    sub.add_parser("scheduler", help="Start cron scheduler (blocking)")

    args = parser.parse_args()

    if args.command == "init":
        cmd_init()
    elif args.command == "run":
        asyncio.run(cmd_run(getattr(args, "source", None)))
    elif args.command == "run-theses":
        asyncio.run(cmd_run_theses())
    elif args.command == "run-all":
        asyncio.run(cmd_run_all())
    elif args.command == "backfill":
        asyncio.run(cmd_backfill(args.since))
    elif args.command == "digest":
        asyncio.run(cmd_digest())
    elif args.command == "digest-theses":
        asyncio.run(cmd_digest_theses())
    elif args.command == "scheduler":
        cmd_scheduler()
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
