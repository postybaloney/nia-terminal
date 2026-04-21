"""
FastAPI REST API for querying stored patent intelligence data.

Endpoints:
  GET  /health                      — liveness check
  GET  /patents                     — paginated RawPatent list with filters
  GET  /patents/{source}/{source_id} — single patent detail
  GET  /families                    — PatentFamily list with filters
  GET  /families/{family_id}        — single family + all source records
  GET  /families/top-assignees      — ranked assignee frequency
  GET  /families/cpc-breakdown      — CPC code frequency
  GET  /runs                        — IngestRun history
  GET  /runs/{run_id}               — single run detail
  GET  /analysis                    — AnalysisResult list
  GET  /analysis/latest             — most recent analysis per query
  POST /pipeline/trigger            — manually kick off ingestion
  GET  /digest                      — generate on-demand weekly digest

Run with:
  uvicorn api.app:app --host 0.0.0.0 --port 8000 --reload
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session

from db import SessionLocal
from db.models import AnalysisResult, IngestRun, PatentFamily, RawPatent

log = logging.getLogger(__name__)

app = FastAPI(
    title="Patent Intelligence API",
    description="Medtech & neurotech patent data from PatentsView, EPO, Lens, and BigQuery",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ── Dependency ────────────────────────────────────────────────────────────────

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── Response models ───────────────────────────────────────────────────────────

class PatentOut(BaseModel):
    id: int
    source: str
    source_id: str
    family_id: str | None
    title: str | None
    abstract: str | None
    filing_date: datetime | None
    grant_date: datetime | None
    assignees: list | None
    cpc_codes: list | None
    matched_query: str | None
    first_seen_at: datetime

    model_config = {"from_attributes": True}


class FamilyOut(BaseModel):
    id: int
    family_id: str
    title: str | None
    abstract: str | None
    earliest_filing_date: datetime | None
    earliest_grant_date: datetime | None
    assignees: list | None
    cpc_codes: list | None
    sources: list | None
    first_seen_at: datetime

    model_config = {"from_attributes": True}


class FamilyDetailOut(FamilyOut):
    raw_patents: list[PatentOut]


class RunOut(BaseModel):
    id: int
    started_at: datetime
    finished_at: datetime | None
    sources: list | None
    new_patents: int
    updated_patents: int
    errors: list | None
    success: bool

    model_config = {"from_attributes": True}


class AnalysisOut(BaseModel):
    id: int
    ingest_run_id: int | None
    query: str | None
    patent_count: int
    model: str
    analysis_text: str | None
    themes: list | None
    top_assignees: list | None
    created_at: datetime

    model_config = {"from_attributes": True}


class PaginatedResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[Any]


class TriggerResponse(BaseModel):
    message: str
    status: str


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["system"])
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


# ── Patents ───────────────────────────────────────────────────────────────────

@app.get("/patents", response_model=PaginatedResponse, tags=["patents"])
def list_patents(
    source: str | None = Query(None, description="Filter by source: patentsview|epo|lens|bigquery"),
    query: str | None = Query(None, description="Filter by matched search query (partial match)"),
    since: str | None = Query(None, description="Patents filed/granted after YYYY-MM-DD"),
    cpc: str | None = Query(None, description="Filter by CPC code prefix (e.g. A61N1)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=200),
    db: Session = Depends(get_db),
):
    q = db.query(RawPatent)

    if source:
        q = q.filter(RawPatent.source == source)
    if query:
        q = q.filter(RawPatent.matched_query.ilike(f"%{query}%"))
    if since:
        q = q.filter(RawPatent.grant_date >= since)
    if cpc:
        q = q.filter(RawPatent.cpc_codes.cast("text").ilike(f"%{cpc}%"))

    total = q.count()
    items = (
        q.order_by(RawPatent.grant_date.desc().nullslast(), RawPatent.first_seen_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )
    return PaginatedResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[PatentOut.model_validate(p) for p in items],
    )


@app.get("/patents/{source}/{source_id}", response_model=PatentOut, tags=["patents"])
def get_patent(source: str, source_id: str, db: Session = Depends(get_db)):
    patent = (
        db.query(RawPatent)
        .filter_by(source=source, source_id=source_id)
        .first()
    )
    if not patent:
        raise HTTPException(status_code=404, detail="Patent not found")
    return PatentOut.model_validate(patent)


# ── Families ──────────────────────────────────────────────────────────────────

@app.get("/families", response_model=PaginatedResponse, tags=["families"])
def list_families(
    assignee: str | None = Query(None, description="Filter by assignee name (partial)"),
    cpc: str | None = Query(None, description="Filter by CPC code prefix"),
    source: str | None = Query(None, description="Filter to families seen in this source"),
    since: str | None = Query(None, description="Earliest filing date >= YYYY-MM-DD"),
    multi_source: bool = Query(False, description="Only return families seen in 2+ sources"),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=200),
    db: Session = Depends(get_db),
):
    q = db.query(PatentFamily)

    if assignee:
        q = q.filter(PatentFamily.assignees.cast("text").ilike(f"%{assignee}%"))
    if cpc:
        q = q.filter(PatentFamily.cpc_codes.cast("text").ilike(f"%{cpc}%"))
    if source:
        q = q.filter(PatentFamily.sources.cast("text").ilike(f"%{source}%"))
    if since:
        q = q.filter(PatentFamily.earliest_filing_date >= since)
    if multi_source:
        q = q.filter(PatentFamily.sources.cast("text").op("~")(r'\[.*,'))

    total = q.count()
    items = (
        q.order_by(PatentFamily.earliest_filing_date.desc().nullslast())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )
    return PaginatedResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[FamilyOut.model_validate(f) for f in items],
    )


@app.get("/families/top-assignees", tags=["families"])
def top_assignees(
    limit: int = Query(20, ge=1, le=100),
    since: str | None = Query(None),
    db: Session = Depends(get_db),
):
    """
    Returns assignee name + family count, ranked by frequency.
    Uses PostgreSQL jsonb_array_elements for the unnesting.
    """
    from sqlalchemy import func, text

    since_clause = f"AND earliest_filing_date >= '{since}'" if since else ""
    sql = text(f"""
        SELECT
            assignee->>'name' AS name,
            COUNT(DISTINCT family_id) AS family_count
        FROM patent_families,
             jsonb_array_elements(assignees) AS assignee
        WHERE assignees IS NOT NULL
          AND assignees != 'null'
          {since_clause}
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT :limit
    """)
    rows = db.execute(sql, {"limit": limit}).fetchall()
    return [{"name": r.name, "family_count": r.family_count} for r in rows]


@app.get("/families/cpc-breakdown", tags=["families"])
def cpc_breakdown(
    prefix: str | None = Query(None, description="Filter to codes starting with prefix"),
    limit: int = Query(30, ge=1, le=200),
    db: Session = Depends(get_db),
):
    from sqlalchemy import text

    prefix_clause = f"AND code LIKE '{prefix}%'" if prefix else ""
    sql = text(f"""
        SELECT code, COUNT(DISTINCT family_id) AS family_count
        FROM patent_families,
             jsonb_array_elements_text(cpc_codes) AS code
        WHERE cpc_codes IS NOT NULL
          {prefix_clause}
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT :limit
    """)
    rows = db.execute(sql, {"limit": limit}).fetchall()
    return [{"cpc_code": r.code, "family_count": r.family_count} for r in rows]


@app.get("/families/{family_id}", response_model=FamilyDetailOut, tags=["families"])
def get_family(family_id: str, db: Session = Depends(get_db)):
    family = db.query(PatentFamily).filter_by(family_id=family_id).first()
    if not family:
        raise HTTPException(status_code=404, detail="Family not found")
    return FamilyDetailOut.model_validate(family)


# ── Ingest Runs ───────────────────────────────────────────────────────────────

@app.get("/runs", response_model=PaginatedResponse, tags=["runs"])
def list_runs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    q = db.query(IngestRun).order_by(IngestRun.started_at.desc())
    total = q.count()
    items = q.offset((page - 1) * page_size).limit(page_size).all()
    return PaginatedResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[RunOut.model_validate(r) for r in items],
    )


@app.get("/runs/{run_id}", response_model=RunOut, tags=["runs"])
def get_run(run_id: int, db: Session = Depends(get_db)):
    run = db.get(IngestRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return RunOut.model_validate(run)


# ── Analysis ──────────────────────────────────────────────────────────────────

@app.get("/analysis", response_model=PaginatedResponse, tags=["analysis"])
def list_analysis(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    q = db.query(AnalysisResult).order_by(AnalysisResult.created_at.desc())
    total = q.count()
    items = q.offset((page - 1) * page_size).limit(page_size).all()
    return PaginatedResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[AnalysisOut.model_validate(a) for a in items],
    )


@app.get("/analysis/latest", tags=["analysis"])
def latest_analysis(db: Session = Depends(get_db)):
    """Returns the most recent analysis result per query string."""
    from sqlalchemy import func

    subq = (
        db.query(
            AnalysisResult.query,
            func.max(AnalysisResult.created_at).label("max_created"),
        )
        .group_by(AnalysisResult.query)
        .subquery()
    )
    results = (
        db.query(AnalysisResult)
        .join(
            subq,
            (AnalysisResult.query == subq.c.query)
            & (AnalysisResult.created_at == subq.c.max_created),
        )
        .all()
    )
    return [AnalysisOut.model_validate(r) for r in results]


# ── Pipeline trigger ──────────────────────────────────────────────────────────

_pipeline_running = False


@app.post("/pipeline/trigger", response_model=TriggerResponse, tags=["system"])
async def trigger_pipeline(background_tasks: BackgroundTasks):
    """
    Manually kick off a full ingestion + analysis run in the background.
    Returns immediately; poll /runs to track progress.
    """
    global _pipeline_running
    if _pipeline_running:
        return TriggerResponse(message="Pipeline already running", status="skipped")

    async def _run():
        global _pipeline_running
        _pipeline_running = True
        try:
            from analysis import analyze_batch
            from config import settings
            from pipeline import run_pipeline

            result = await run_pipeline()
            if result.new_patents >= settings.analysis_min_new:
                for query in settings.query_list:
                    relevant = [p for p in result.new_records if p.matched_query == query]
                    if relevant:
                        await analyze_batch(relevant, query, result.ingest_run_id)
        except Exception as exc:
            log.error("background pipeline error: %s", exc)
        finally:
            _pipeline_running = False

    background_tasks.add_task(_run)
    return TriggerResponse(message="Pipeline triggered", status="running")


# ── On-demand digest ──────────────────────────────────────────────────────────

@app.get("/digest", tags=["analysis"])
async def get_digest(db: Session = Depends(get_db)):
    """Generate and return a weekly digest on demand."""
    from analysis import generate_weekly_digest
    from config import settings

    latest = (
        db.query(AnalysisResult)
        .order_by(AnalysisResult.created_at.desc())
        .first()
    )
    digest = await generate_weekly_digest(
        new_count=db.query(RawPatent).count(),
        sources=["patentsview", "epo", "lens", "bigquery"],
        queries=settings.query_list,
        latest_analysis=latest,
    )
    return {"digest": digest, "generated_at": datetime.utcnow().isoformat()}
