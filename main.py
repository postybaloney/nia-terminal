"""
CLI entry point for manual operations.

Usage:
  python main.py init              # create all tables (patents + theses)
  python main.py run               # patent ingestion run
  python main.py run --source patentsview   # single source
  python main.py backfill --from 2020-01-01 # re-index from a date
  python main.py run-theses        # thesis ingestion run
  python main.py run-signals       # current-signal run (NIH grants / trials / FDA)
  python main.py run-all           # patents + theses + signals together
  python main.py digest            # generate and print patent weekly digest
  python main.py digest --send     # generate + send via email/Slack
  python main.py digest-theses     # thesis research digest
  python main.py digest-signals    # current-signal digest (add --send to dispatch)
  python main.py scheduler         # start the cron scheduler (blocking)
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
    """
    Weekly patent digest.

    FIXED 2026-08-18. The previous version computed
        total_new = sum(IngestRun.new_patents)          # ALL TIME
    and passed it into a prompt reading "This week's data: New patents
    ingested: {n}". On 2026-08-17 that made the model write "This week, we
    ingested 1645 new patents" for a week whose real total was 0. It also
    hardcoded sources=[..., "lens", ...] — claiming a data source dropped for
    non-commercial licensing reasons. Both are now derived from what actually
    happened in the window.
    """
    from datetime import datetime, timedelta, timezone

    from config import settings
    from analysis import generate_weekly_digest
    from db import get_session
    from db.models import AnalysisResult, IngestRun, RawPatent
    from notifiers import dispatch_digest

    window_days = 7
    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)

    with get_session() as session:
        from sqlalchemy import func

        latest = (
            session.query(AnalysisResult)
            .order_by(AnalysisResult.created_at.desc())
            .first()
        )
        if latest:
            session.expunge(latest)

        # Only runs inside the window, and only runs that actually happened.
        week_new = (
            session.query(func.coalesce(func.sum(IngestRun.new_patents), 0))
            .filter(IngestRun.started_at >= cutoff)
            .scalar()
        ) or 0

        corpus_total = session.query(func.count(RawPatent.id)).scalar() or 0

        # Sources that genuinely contributed in the window — never hardcoded.
        rows = (
            session.query(IngestRun.sources)
            .filter(IngestRun.started_at >= cutoff)
            .all()
        )
        used: set[str] = set()
        for (srcs,) in rows:
            for x in (srcs or []):
                used.add(x)
        real_sources = sorted(used) or ["(no ingest runs in window)"]

    console.print(
        f"[dim]window={window_days}d  new_this_week={week_new}  "
        f"corpus_total={corpus_total}  sources={', '.join(real_sources)}[/dim]"
    )

    if week_new == 0:
        # Do not ask a language model to narrate a week that had no intake.
        # It will invent one — that is exactly how "1645 new patents" happened.
        console.print(
            "\n[bold]--- WEEKLY PATENT INTELLIGENCE DIGEST ---[/bold]\n"
        )
        digest = (
            f"No new patents were ingested in the last {window_days} days. "
            f"The corpus currently holds {corpus_total:,} records from "
            f"{', '.join(real_sources)}. No digest narrative is generated for "
            f"an empty window."
        )
        console.print(digest)
        if send:
            await dispatch_digest(digest_text=digest, new_count=0, run_id=0)
            console.print("[green]OK Dispatch complete[/green]")
        return

    digest = await generate_weekly_digest(
        new_count=week_new,
        sources=real_sources,
        queries=settings.query_list,
        latest_analysis=latest,
    )
    console.print("\n[bold]--- WEEKLY PATENT INTELLIGENCE DIGEST ---[/bold]\n")
    console.print(digest)

    if send:
        console.print("\n[bold]Dispatching digest via email/Slack...[/bold]")
        await dispatch_digest(digest_text=digest, new_count=week_new, run_id=0)
        console.print("[green]OK Dispatch complete[/green]")


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


async def cmd_run_signals() -> None:
    from signal_pipeline import run_signal_pipeline

    console.print("[bold]Running current-signal pipeline (NIH / ClinicalTrials / FDA)...[/bold]")
    result = await run_signal_pipeline()

    table = Table(title="Signal Pipeline Result")
    table.add_column("Metric", style="dim")
    table.add_column("Value", style="bold")
    table.add_row("New signals", str(result.new_signals))
    table.add_row("Updated", str(result.updated_signals))
    table.add_row("Total fetched", str(result.total_fetched))
    by_type: dict[str, int] = {}
    for s in result.new_records:
        by_type[s.signal_type] = by_type.get(s.signal_type, 0) + 1
    for stype, count in sorted(by_type.items()):
        table.add_row(f"  new {stype}s", str(count))
    table.add_row("Errors", str(len(result.errors)))
    console.print(table)

    for err in result.errors:
        console.print(f"[red]  Error: {err}[/red]")


async def cmd_digest_signals(send: bool = False) -> None:
    from notifiers import dispatch_digest
    from signal_analysis import generate_signal_digest

    digest = await generate_signal_digest(days=7)
    console.print("\n[bold]--- CURRENT-SIGNAL INTELLIGENCE DIGEST ---[/bold]\n")
    console.print(digest)

    if send:
        console.print("\n[bold]Dispatching digest via email/Slack...[/bold]")
        await dispatch_digest(digest_text=digest, new_count=0, run_id=0)
        console.print("[green]✓ Dispatch complete[/green]")


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
        if latest:
            session.expunge(latest)
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


async def cmd_backfill_orcid(dry_run: bool = False) -> None:
    """
    Backfill ORCID and OpenAlex author profile URLs for existing thesis records.

    Fetches authorship data from api.openalex.org in batches of 50.
    Safe to run multiple times — only rows missing author_orcid are touched.
    """
    import httpx
    from db import get_session
    from db.thesis_models import Thesis

    _BATCH = 50
    _DELAY = 1.0
    _OA_BASE = "https://api.openalex.org/works"

    with get_session() as session:
        rows = (
            session.query(Thesis.id, Thesis.source_id, Thesis.raw_payload)
            .filter(
                Thesis.source == "openalex",
                Thesis.raw_payload["author_orcid"].as_string().is_(None),
            )
            .order_by(Thesis.id)
            .all()
        )

    if not rows:
        console.print("[green]✓ All thesis records already have ORCID data[/green]")
        return

    console.print(f"[bold]Backfilling ORCID for {len(rows)} thesis records...[/bold]")
    if dry_run:
        console.print("[dim](dry run — no writes)[/dim]")

    id_map: dict[str, tuple[int, dict]] = {
        r.source_id: (r.id, r.raw_payload or {}) for r in rows
    }
    source_ids = list(id_map.keys())
    updated = 0
    skipped = 0

    async with httpx.AsyncClient(timeout=30) as client:
        for i in range(0, len(source_ids), _BATCH):
            batch = source_ids[i : i + _BATCH]
            filter_str = "|".join(f"https://openalex.org/{sid}" for sid in batch)

            try:
                resp = await client.get(
                    _OA_BASE,
                    params={
                        "filter": f"ids.openalex:{filter_str}",
                        "select": "id,authorships",
                        "per-page": _BATCH,
                    },
                )
                resp.raise_for_status()
                works = resp.json().get("results") or []
            except Exception as exc:
                console.print(f"[yellow]  Batch {i // _BATCH + 1} failed: {exc}[/yellow]")
                await asyncio.sleep(_DELAY * 3)
                continue

            batch_updates: list[tuple[int, dict]] = []
            for work in works:
                sid = work.get("id", "").replace("https://openalex.org/", "")
                if sid not in id_map:
                    continue
                row_id, payload = id_map[sid]
                authorships = work.get("authorships") or []
                if not authorships:
                    skipped += 1
                    continue

                first = authorships[0].get("author") or {}
                orcid  = first.get("orcid")
                oa_url = first.get("id")

                if not orcid and not oa_url:
                    skipped += 1
                    continue

                new_payload = {**payload}
                if orcid:
                    new_payload["author_orcid"] = orcid
                if oa_url:
                    new_payload["author_openalex_url"] = oa_url
                batch_updates.append((row_id, new_payload))

            if not dry_run and batch_updates:
                with get_session() as session:
                    for row_id, new_payload in batch_updates:
                        session.query(Thesis).filter(Thesis.id == row_id).update(
                            {"raw_payload": new_payload}
                        )
                updated += len(batch_updates)

            total_batches = -(-len(source_ids) // _BATCH)
            console.print(
                f"  Batch {i // _BATCH + 1}/{total_batches} — "
                f"updated: [green]{updated}[/green]  no author data: [dim]{skipped}[/dim]"
            )
            await asyncio.sleep(_DELAY)

    console.print(
        f"\n[green]✓ Backfill complete — "
        f"{updated} updated, {skipped} had no authorship data[/green]"
    )


def cmd_init() -> None:
    from db import init_db
    from db.signal_models import Signal  # noqa: F401 — registers Signal on Base.metadata
    from db.thesis_models import Thesis  # noqa: F401 — registers Thesis on Base.metadata
    init_db()
    console.print("[green]✓ Database tables created (patents + theses + signals)[/green]")


def cmd_scheduler() -> None:
    import scheduler as sched_module
    sched_module.main()


async def cmd_doctor(only: str | None = None) -> None:
    """
    Probe every data source independently and print a pass/fail table.

    Exists because this pipeline runs unattended and its failures are quiet:
    a 403 on one query used to zero an entire run while still printing eleven
    lines of "fetched=50". When something breaks, run this and paste the table.
    """
    import time

    from config import settings

    checks: list[tuple[str, str, str, str]] = []   # source, status, detail, timing

    async def probe(label: str, coro):
        t0 = time.time()
        try:
            n = await coro
            dt = f"{time.time() - t0:.1f}s"
            if n is None:
                checks.append((label, "SKIP", "not configured", dt))
            elif n == 0:
                checks.append((label, "WARN", "reachable, 0 records", dt))
            else:
                checks.append((label, "OK", f"{n} records", dt))
        except Exception as exc:
            checks.append((label, "FAIL", f"{type(exc).__name__}: {str(exc)[:70]}",
                           f"{time.time() - t0:.1f}s"))

    q = settings.signal_query_list[:2]
    since = settings.signal_since

    want = (only or "").lower()

    def run(name: str) -> bool:
        return not want or want == name

    if run("epo"):
        from ingestors.epo import EPOIngestor
        async def _epo():
            if not settings.epo_enabled:
                return None
            ing = EPOIngestor(settings.query_list, settings.backfill_from, 10)
            from neuro_taxonomy import NEURO_QUERIES
            ing._queries = lambda: list(NEURO_QUERIES[:1])
            recs = await ing.fetch()
            return len(recs)
        await probe("epo (patents)", _epo())

    if run("feeds"):
        from ingestors.signals.feeds import FeedIngestor, FEED_REGISTRY
        for f in FEED_REGISTRY:
            async def _one(f=f):
                ing = FeedIngestor(q, since, 20, feeds=(f,))
                recs = await ing.fetch()
                if ing.query_errors:
                    raise RuntimeError(ing.query_errors[0])
                return len(recs)
            await probe(f"feed:{f.key}", _one())

    if run("preprints"):
        from ingestors.signals.preprints import PreprintIngestor
        async def _pre():
            ing = PreprintIngestor(q, since, 20)
            import ingestors.signals.preprints as m
            orig = m.ARXIV_QUERIES
            m.ARXIV_QUERIES = orig[:1]
            try:
                recs = await ing.fetch()
            finally:
                m.ARXIV_QUERIES = orig
            return len(recs)
        await probe("arxiv + biorxiv", _pre())

    if run("jobs"):
        from ingestors.signals.jobs import JobBoardIngestor, BOARDS
        async def _jobs():
            ing = JobBoardIngestor(q, since, 20)
            recs = await ing.fetch()
            console.print(
                f"[dim]  boards resolved: {len(ing.resolved)}/{len(BOARDS)}[/dim]"
            )
            if ing.unresolved:
                console.print(
                    f"[yellow]  unresolved slugs (edit ingestors/signals/jobs.py): "
                    f"{', '.join(ing.unresolved)}[/yellow]"
                )
            return len(recs)
        await probe("ats job boards", _jobs())

    if run("edgar"):
        from ingestors.signals.edgar import EdgarIngestor
        async def _sec():
            ing = EdgarIngestor(q, since, 20)
            import ingestors.signals.edgar as m
            orig = m.EDGAR_PHRASES
            m.EDGAR_PHRASES = orig[:1]
            try:
                recs = await ing.fetch()
            finally:
                m.EDGAR_PHRASES = orig
            return len(recs)
        await probe("sec edgar", _sec())

    if run("db"):
        async def _db():
            from db import get_session
            from db.models import RawPatent
            from sqlalchemy import func
            with get_session() as s_:
                return s_.query(func.count(RawPatent.id)).scalar() or 0
        await probe("database", _db())

    table = Table(title="NIA Source Doctor")
    table.add_column("Source", style="dim")
    table.add_column("Status")
    table.add_column("Detail")
    table.add_column("Time", justify="right", style="dim")
    for label, status, detail, dt in checks:
        colour = {"OK": "green", "WARN": "yellow",
                  "FAIL": "red", "SKIP": "dim"}[status]
        table.add_row(label, f"[{colour}]{status}[/{colour}]", detail, dt)
    console.print(table)

    bad = [c for c in checks if c[1] == "FAIL"]
    if bad:
        console.print(f"[red]{len(bad)} source(s) failing.[/red] "
                      "Paste this table when reporting the problem.")
    else:
        console.print("[green]All probed sources reachable.[/green]")


def cmd_graph(demo: bool = False, out: str = "nia_graph.sqlite",
              html: str = "nia_graph.html", max_nodes: int = 650) -> None:
    """Build the knowledge graph and render the self-contained HTML view."""
    import subprocess
    import sys as _sys

    build = [_sys.executable, "graph_build.py", "--out", out]
    if demo:
        build.append("--demo")
    console.print("[bold]Building knowledge graph...[/bold]")
    r = subprocess.run(build)
    if r.returncode != 0:
        console.print("[red]graph build failed[/red]")
        _sys.exit(1)

    console.print("[bold]Rendering graph HTML...[/bold]")
    r = subprocess.run([_sys.executable, "graph_render.py", "--db", out,
                        "--out", html, "--max-nodes", str(max_nodes)])
    if r.returncode != 0:
        console.print("[red]graph render failed[/red]")
        _sys.exit(1)
    console.print(f"[green]OK {html}[/green]")


def main() -> None:
    parser = argparse.ArgumentParser(description="Patent Intelligence CLI")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("init", help="Initialize database tables")

    run_p = sub.add_parser("run", help="Run ingestion pipeline once")
    run_p.add_argument("--source", help="Limit to one source (epo|bigquery)")

    bf_p = sub.add_parser("backfill", help="Re-ingest from a historical date")
    bf_p.add_argument("--from", dest="since", required=True, help="Start date YYYY-MM-DD")

    sub.add_parser("analyze", help="Run AI analysis on patents already in the database")
    digest_p = sub.add_parser("digest", help="Generate patent weekly digest")
    digest_p.add_argument("--send", action="store_true", help="Send via email/Slack after printing")
    sub.add_parser("run-theses", help="Run thesis ingestion pipeline")
    sub.add_parser("run-signals", help="Run current-signal pipeline (NIH grants / trials / FDA)")
    sub.add_parser("run-all", help="Run patents + theses + signals together")
    sub.add_parser("digest-theses", help="Generate thesis research digest")
    dsig_p = sub.add_parser("digest-signals", help="Generate current-signal digest")
    dsig_p.add_argument("--send", action="store_true", help="Send via email/Slack after printing")
    orcid_p = sub.add_parser("backfill-orcid", help="Backfill ORCID/OpenAlex author URLs for existing theses")
    orcid_p.add_argument("--dry-run", action="store_true", help="Preview without writing to DB")
    doc_p = sub.add_parser("doctor", help="Probe every data source, print pass/fail")
    doc_p.add_argument("--only", help="probe one: epo|feeds|preprints|jobs|edgar|db")
    g_p = sub.add_parser("graph", help="Build + render the knowledge graph")
    g_p.add_argument("--demo", action="store_true", help="synthetic corpus, no DB needed")
    g_p.add_argument("--out", default="nia_graph.sqlite")
    g_p.add_argument("--html", default="nia_graph.html")
    g_p.add_argument("--max-nodes", type=int, default=650)
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
    elif args.command == "run-theses":
        asyncio.run(cmd_run_theses())
    elif args.command == "run-signals":
        asyncio.run(cmd_run_signals())
    elif args.command == "run-all":
        async def _run_all() -> None:
            stages = (
                ("patents", cmd_run, (None,)),
                ("theses", cmd_run_theses, ()),
                ("signals", cmd_run_signals, ()),
            )
            failed = []
            for name, fn, fn_args in stages:
                try:
                    await fn(*fn_args)
                except Exception as exc:  # isolate each stage; a nightly job wants partial success
                    failed.append(name)
                    console.print(f"[red]run-all: {name} stage failed:[/red] {exc!r}")
            if failed:
                console.print(f"[red]run-all finished with failures: {', '.join(failed)}[/red]")
                sys.exit(1)
        asyncio.run(_run_all())
    elif args.command == "digest":
        asyncio.run(cmd_digest(send=getattr(args, "send", False)))
    elif args.command == "digest-theses":
        asyncio.run(cmd_digest_theses())
    elif args.command == "digest-signals":
        asyncio.run(cmd_digest_signals(send=getattr(args, "send", False)))
    elif args.command == "backfill-orcid":
        asyncio.run(cmd_backfill_orcid(dry_run=getattr(args, "dry_run", False)))
    elif args.command == "doctor":
        asyncio.run(cmd_doctor(getattr(args, "only", None)))
    elif args.command == "graph":
        cmd_graph(demo=getattr(args, "demo", False), out=args.out,
                  html=args.html, max_nodes=getattr(args, "max_nodes", 650))
    elif args.command == "scheduler":
        cmd_scheduler()
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
