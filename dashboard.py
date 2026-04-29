"""
Patent Intelligence Terminal — Bloomberg-style dashboard.

Run with:
    .venv\\Scripts\\python.exe dashboard.py

Opens at http://localhost:8050
Auto-refreshes every 5 minutes. All data is read live from PostgreSQL.
"""
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timedelta, timezone

import dash
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Input, Output, dash_table, dcc, html

from db import get_session, init_db
from db.models import AnalysisResult, IngestRun, RawPatent
from sqlalchemy import cast, func, or_
from sqlalchemy import Text as SAText

# Ensure tables exist (safe to call repeatedly — CREATE TABLE IF NOT EXISTS)
try:
    init_db()
except Exception as _db_init_err:
    import logging
    logging.getLogger(__name__).warning("DB init skipped: %s", _db_init_err)

# ── Colour palette (Bloomberg amber-on-black) ─────────────────────────────────
BG     = "#050810"
CARD   = "#0d1117"
CARD2  = "#111827"
BORDER = "#1f2937"
TEXT   = "#e5e7eb"
DIM    = "#6b7280"
AMBER  = "#f59e0b"
GREEN  = "#10b981"
RED    = "#ef4444"
BLUE   = "#3b82f6"
TEAL   = "#14b8a6"
PURPLE = "#8b5cf6"

CHART_BG = dict(paper_bgcolor=CARD, plot_bgcolor=CARD)
CHART_FONT = dict(font=dict(color=TEXT, family="'Courier New', monospace", size=11))
CHART_MARGIN = dict(margin=dict(l=10, r=10, t=36, b=10))
GRID = dict(gridcolor=BORDER, zerolinecolor=BORDER)

SERIES_COLORS = [AMBER, TEAL, BLUE, PURPLE, GREEN]

MONO = "'Courier New', monospace"

# ── Period helpers ────────────────────────────────────────────────────────────

def _period_since(period: str) -> datetime | None:
    """Return a UTC cutoff datetime for distribution/ranking filters."""
    now = datetime.now(timezone.utc)
    return {"daily": now - timedelta(days=1),
            "weekly": now - timedelta(weeks=1),
            "monthly": now - timedelta(days=30)}.get(period)


def _period_bucket(dt, period: str) -> str:
    """Return a string bucket key for a date/datetime given a period."""
    if period == "daily":
        return dt.strftime("%Y-%m-%d")
    elif period == "weekly":
        week_start = dt - timedelta(days=dt.weekday())
        return week_start.strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m")


def _period_label(period: str) -> str:
    return {"daily": "Last 24 h", "weekly": "Last 7 days", "monthly": "Last 30 days"}.get(period, "")


def _period_toggle(toggle_id: str, default: str = "monthly") -> html.Div:
    """Small D/W/M radio toggle for chart cards."""
    return html.Div(
        dcc.RadioItems(
            id=toggle_id,
            options=[{"label": l, "value": v} for l, v in
                     [("DAILY", "daily"), ("WEEKLY", "weekly"), ("MONTHLY", "monthly")]],
            value=default,
            inline=True,
            labelStyle={"color": DIM, "fontSize": "8px", "fontFamily": MONO,
                        "marginLeft": "10px", "cursor": "pointer", "letterSpacing": "1px"},
            inputStyle={"marginRight": "2px", "accentColor": AMBER},
        ),
        style={"textAlign": "right", "paddingBottom": "2px"},
    )


# ── CPC code descriptions (4-char subclass prefix) ────────────────────────────
_CPC_DESC: dict[str, str] = {
    "A61B": "Diagnosis; surgery; identification",
    "A61F": "Implantable filters/prostheses; bandages; wound treatment",
    "A61K": "Medical/dental/cosmetic preparations",
    "A61L": "Sterilization or disinfection; bandages/dressings",
    "A61M": "Devices for introducing media into or onto the body",
    "A61N": "Electrotherapy; magnetotherapy; radiation therapy; ultrasound",
    "A61P": "Specific therapeutic activity of chemical compounds",
    "A61Q": "Specific use of cosmetics or similar preparations",
    "B33Y": "Additive manufacturing (3D printing)",
    "C12N": "Microorganisms; enzymes; genetic engineering",
    "C12Q": "Measuring/testing processes involving nucleic acids/enzymes",
    "G06F": "Electric digital data processing",
    "G06N": "Computing using AI / neural network models",
    "G06T": "Image data processing or generation",
    "G16H": "Healthcare informatics — medical data/records",
    "H04L": "Transmission of digital information",
    "H04W": "Wireless communications networks",
}

def _cpc_label(code: str) -> str:
    """Return 'CODE — description' for a 4-char CPC prefix."""
    return _CPC_DESC.get(code, f"CPC subclass {code}")

def _patent_url(source: str, source_id: str) -> str:
    """Return a public URL for a patent given its source and ID."""
    sid = (source_id or "").strip()
    if source == "lens":
        return f"https://lens.org/lens/patent/{sid}"
    elif source == "epo":
        return f"https://worldwide.espacenet.com/patent/search?q=pn%3D{sid}"
    else:  # patentsview / uspto
        return f"https://patents.google.com/patent/US{sid}/en"


# ── Data helpers  (all return plain Python — no detached ORM objects) ─────────

def _kpis() -> dict:
    with get_session() as s:
        total    = s.query(func.count(RawPatent.id)).scalar() or 0
        last_run = (
            s.query(IngestRun.started_at, IngestRun.new_patents)
            .order_by(IngestRun.started_at.desc())
            .first()
        )
        last_analysis = (
            s.query(AnalysisResult.created_at, AnalysisResult.query)
            .order_by(AnalysisResult.created_at.desc())
            .first()
        )
    last_run_new = last_run.new_patents or 0 if last_run else 0
    last_run_at  = last_run.started_at.strftime("%m/%d %H:%M") if last_run else "—"
    return {
        "total": total,
        "last_run_new": last_run_new,
        "last_run_at": last_run_at,
        "last_analysis_at": last_analysis.created_at.strftime("%m/%d %H:%M") if last_analysis else "—",
        "last_analysis_query": (last_analysis.query or "")[:40] if last_analysis else "",
    }


def _cpc_distribution(limit: int = 20, period: str = "monthly") -> list[tuple[str, int]]:
    since = _period_since(period)
    with get_session() as s:
        q = s.query(RawPatent.cpc_codes).filter(
            RawPatent.cpc_codes.isnot(None),
            or_(RawPatent.title.isnot(None), RawPatent.abstract.isnot(None)),
        )
        if since:
            since_naive = since.replace(tzinfo=None)
            q = q.filter(RawPatent.first_seen_at >= since_naive)
        rows = q.all()
    counter: Counter = Counter()
    for (codes,) in rows:
        if isinstance(codes, list):
            for c in codes:
                p = c[:4] if c else None
                if p:
                    counter[p] += 1
    return counter.most_common(limit)


def _cpc_over_time(top_n: int = 5, period: str = "monthly") -> tuple[list, list]:
    since = _period_since(period)
    with get_session() as s:
        q = s.query(
            RawPatent.grant_date, RawPatent.filing_date,
            RawPatent.first_seen_at, RawPatent.cpc_codes,
        ).filter(
            RawPatent.cpc_codes.isnot(None),
            or_(RawPatent.title.isnot(None), RawPatent.abstract.isnot(None)),
        )
        if since:
            since_naive = since.replace(tzinfo=None)
            q = q.filter(RawPatent.first_seen_at >= since_naive)
        rows = q.all()

    all_codes: Counter = Counter()
    for _, _, _, codes in rows:
        if isinstance(codes, list):
            for c in codes:
                p = c[:4] if c else None
                if p:
                    all_codes[p] += 1
    top = [code for code, _ in all_codes.most_common(top_n)]

    buckets: dict[str, Counter] = {}
    for grant_date, filing_date, first_seen_at, codes in rows:
        date = grant_date or filing_date or first_seen_at
        if not date or not isinstance(codes, list):
            continue
        bucket = _period_bucket(date, period)
        if bucket not in buckets:
            buckets[bucket] = Counter()
        for c in codes:
            p = c[:4] if c else None
            if p in top:
                buckets[bucket][p] += 1
    return sorted(buckets.items()), top


def _top_assignees(limit: int = 15, period: str = "monthly") -> list[tuple[str, int]]:
    since = _period_since(period)
    with get_session() as s:
        q = s.query(RawPatent.assignees).filter(
            RawPatent.assignees.isnot(None),
            or_(RawPatent.title.isnot(None), RawPatent.abstract.isnot(None)),
        )
        if since:
            since_naive = since.replace(tzinfo=None)
            q = q.filter(RawPatent.first_seen_at >= since_naive)
        rows = q.all()
    counter: Counter = Counter()
    for (assignees,) in rows:
        if isinstance(assignees, list):
            for a in assignees:
                name = (a.get("name") or "").strip()
                if name and name.lower() not in ("", "unknown"):
                    counter[name] += 1
    return counter.most_common(limit)


def _ingestion_history(limit: int = 60, period: str = "monthly") -> list[dict]:
    with get_session() as s:
        runs = (
            s.query(IngestRun.started_at, IngestRun.new_patents, IngestRun.updated_patents,
                    IngestRun.errors, IngestRun.success)
            .order_by(IngestRun.started_at.asc())
            .all()
        )

    if period in ("monthly", "weekly"):
        buckets: dict[str, dict] = {}
        for r in runs:
            if not r.started_at:
                continue
            bucket = _period_bucket(r.started_at, period)
            if bucket not in buckets:
                buckets[bucket] = {"date": bucket, "new": 0, "updated": 0, "errors": 0}
            buckets[bucket]["new"] += r.new_patents or 0
            buckets[bucket]["updated"] += r.updated_patents or 0
            buckets[bucket]["errors"] += len(r.errors or [])
        return list(buckets.values())[-limit:]
    else:  # daily — individual runs
        return [
            {
                "date": r.started_at.strftime("%m/%d %H:%M") if r.started_at else "",
                "new": r.new_patents or 0,
                "updated": r.updated_patents or 0,
                "errors": len(r.errors or []),
            }
            for r in runs
        ][-limit:]


def _patent_table(limit: int = 500) -> list[dict]:
    with get_session() as s:
        rows = (
            s.query(
                RawPatent.grant_date, RawPatent.filing_date, RawPatent.first_seen_at,
                RawPatent.source, RawPatent.source_id, RawPatent.title,
                RawPatent.assignees, RawPatent.matched_query, RawPatent.cpc_codes,
            )
            # Only show records that have at least a title or abstract (quality gate)
            .filter(or_(RawPatent.title.isnot(None), RawPatent.abstract.isnot(None)))
            .order_by(RawPatent.first_seen_at.desc())
            .limit(limit)
            .all()
        )
    data = []
    for r in rows:
        date = r.grant_date or r.filing_date or r.first_seen_at
        assignee = ((r.assignees or [{}])[0].get("name") or "") if r.assignees else ""
        url = _patent_url(r.source or "", r.source_id or "")
        data.append({
            "Date": date.strftime("%Y-%m-%d") if date else "",
            "Source": r.source or "",
            "Patent ID": r.source_id or "",
            "Title": (r.title or "—")[:80],
            "Assignee": assignee[:40],
            "Query": (r.matched_query or "")[:50],
            "CPC": ", ".join((r.cpc_codes or [])[:3]),
            "Link": f"[↗ View]({url})" if url else "",
        })
    return data


def _latest_analyses(limit: int = 5) -> list[dict]:
    with get_session() as s:
        rows = (
            s.query(
                AnalysisResult.query, AnalysisResult.themes, AnalysisResult.top_assignees,
                AnalysisResult.analysis_text, AnalysisResult.created_at,
                AnalysisResult.patent_count,
            )
            .order_by(AnalysisResult.created_at.desc())
            .limit(limit)
            .all()
        )
    results = []
    for r in rows:
        try:
            structured = json.loads(r.analysis_text or "{}")
        except Exception:
            structured = {}
        results.append({
            "query": r.query or "",
            "themes": r.themes or [],
            "takeaway": structured.get("strategic_takeaway", ""),
            "created_at": r.created_at.strftime("%Y-%m-%d %H:%M") if r.created_at else "",
            "patent_count": r.patent_count or 0,
        })
    return results


# ── Search ────────────────────────────────────────────────────────────────────

def _search_patents(
    query: str = "",
    cpc_filter: str = "",
    source_filter: str = "all",
    matched_query_filter: str = "",
    limit: int = 100,
) -> list[dict]:
    """Full-text ILIKE search over title + abstract, with optional CPC/source/matched-query filters."""
    q = (query or "").strip()
    cpc = (cpc_filter or "").strip().upper()
    mq = (matched_query_filter or "").strip()
    if not q and not cpc and not mq:
        return []

    with get_session() as s:
        stmt = s.query(
            RawPatent.source, RawPatent.source_id,
            RawPatent.title, RawPatent.abstract,
            RawPatent.assignees, RawPatent.cpc_codes,
            RawPatent.filing_date, RawPatent.grant_date, RawPatent.first_seen_at,
            RawPatent.matched_query,
        )

        filters = [
            # Quality gate — exclude bare stubs with neither title nor abstract
            or_(RawPatent.title.isnot(None), RawPatent.abstract.isnot(None)),
        ]
        if mq:
            # Exact matched_query lookup (from "N patents" click in analysis panel)
            filters.append(RawPatent.matched_query == mq)
        elif q:
            filters.append(or_(
                RawPatent.title.ilike(f"%{q}%"),
                RawPatent.abstract.ilike(f"%{q}%"),
            ))
        if cpc:
            filters.append(cast(RawPatent.cpc_codes, SAText).ilike(f'%"{cpc}%'))
        if source_filter and source_filter != "all":
            filters.append(RawPatent.source == source_filter)

        if filters:
            stmt = stmt.filter(*filters)

        rows = stmt.order_by(RawPatent.first_seen_at.desc()).limit(limit).all()

    results = []
    for r in rows:
        date = r.grant_date or r.filing_date or r.first_seen_at
        assignee = ((r.assignees or [{}])[0].get("name") or "") if r.assignees else ""
        results.append({
            "source": r.source or "",
            "source_id": r.source_id or "",
            "title": r.title or "—",
            "abstract": r.abstract or "",
            "assignee": assignee,
            "cpc_codes": r.cpc_codes or [],
            "date": date.strftime("%Y-%m-%d") if date else "",
            "url": _patent_url(r.source or "", r.source_id or ""),
            "matched_query": r.matched_query or "",
        })
    return results


def _render_search_results(results: list[dict]) -> list:
    """Render search results as Bloomberg-styled patent cards."""
    if not results:
        return [html.Div(
            "No results. Try broader terms or clear the CPC filter.",
            style={"color": DIM, "fontFamily": MONO, "fontSize": "12px", "padding": "12px 0"},
        )]

    source_color = {"lens": TEAL, "epo": GREEN, "patentsview": BLUE, "bigquery": PURPLE}
    cards = []

    for r in results:
        color = source_color.get(r["source"], DIM)

        # CPC badges — show up to 5, each with description on hover
        cpc_badges = []
        for code in (r["cpc_codes"] or [])[:5]:
            prefix = code[:4] if code else ""
            desc = _CPC_DESC.get(prefix, f"CPC {prefix}")
            cpc_badges.append(html.Span(
                code[:8],
                title=desc,
                style={
                    "background": "#1a2035", "color": AMBER,
                    "fontSize": "9px", "fontFamily": MONO,
                    "padding": "1px 6px", "borderRadius": "2px",
                    "marginRight": "4px",
                    "border": f"1px solid {AMBER}44",
                    "cursor": "default",
                },
            ))

        abstract = (r["abstract"] or "").strip()
        abstract_preview = abstract[:400] + ("…" if len(abstract) > 400 else "")

        cards.append(html.Div([
            # ── Top row: source · CPC badges · date ──
            html.Div([
                html.Span(
                    r["source"].upper(),
                    style={"color": color, "fontSize": "9px", "fontFamily": MONO,
                           "fontWeight": "bold", "letterSpacing": "1px",
                           "marginRight": "10px"},
                ),
                *cpc_badges,
                html.Span(
                    r["date"],
                    style={"color": DIM, "fontSize": "9px", "fontFamily": MONO,
                           "float": "right"},
                ),
            ], style={"marginBottom": "6px"}),

            # ── Title as clickable link ──
            html.A(
                [r["title"], html.Span(" ↗", style={"fontSize": "10px", "opacity": "0.6"})],
                href=r["url"],
                target="_blank",
                style={
                    "color": AMBER, "fontFamily": MONO, "fontSize": "13px",
                    "fontWeight": "bold", "textDecoration": "none",
                    "lineHeight": "1.4", "display": "block", "marginBottom": "4px",
                },
            ),

            # ── Assignee ──
            html.Div(
                r["assignee"] or "Assignee unknown",
                style={"color": TEAL if r["assignee"] else DIM,
                       "fontSize": "11px", "fontFamily": MONO, "marginBottom": "6px"},
            ),

            # ── Abstract ──
            html.Div(
                abstract_preview or "No abstract available.",
                style={"color": TEXT, "fontSize": "11px", "fontFamily": MONO,
                       "lineHeight": "1.7", "opacity": "0.85"},
            ),

            # ── Patent ID (small, bottom) ──
            html.Div(
                f"ID: {r['source_id']}  ·  matched query: {r['matched_query'][:60]}" if r["matched_query"] else f"ID: {r['source_id']}",
                style={"color": DIM, "fontSize": "9px", "fontFamily": MONO,
                       "marginTop": "8px", "letterSpacing": "0.5px"},
            ),
        ], style={
            "background": CARD2,
            "border": f"1px solid {BORDER}",
            "borderLeft": f"3px solid {color}",
            "borderRadius": "2px",
            "padding": "12px 16px",
            "marginBottom": "8px",
        }))

    return cards


# ── Chart builders ────────────────────────────────────────────────────────────

def _chart_layout(**extra):
    base = {**CHART_BG, **CHART_FONT, **CHART_MARGIN}
    base.update(extra)  # extra overrides base keys (e.g. custom margin)
    return base


def _fig_cpc_bar(top_codes: list[tuple[str, int]], period: str = "monthly") -> go.Figure:
    if not top_codes:
        return _empty_fig("No CPC data yet — run pipeline to ingest Lens/EPO patents")
    codes, counts = zip(*top_codes)
    descs = [_cpc_label(c) for c in codes]
    fig = go.Figure(go.Bar(
        x=list(counts), y=list(codes), orientation="h",
        marker_color=AMBER, marker_line_width=0,
        customdata=descs,
        hovertemplate="<b>%{y}</b>  ·  %{x} patents<br>%{customdata}<extra></extra>",
    ))
    period_note = _period_label(period)
    fig.update_layout(
        title=dict(
            text=f"CPC Code Distribution (top 20)  ·  {period_note}<br><sup>Cooperative Patent Classification — international standard for categorising patent technology</sup>",
            font=dict(color=AMBER, size=12),
        ),
        yaxis=dict(autorange="reversed", tickfont=dict(size=9), **GRID),
        xaxis=dict(tickfont=dict(size=9, color=DIM), **GRID),
        **_chart_layout(margin=dict(l=10, r=10, t=52, b=10)),
    )
    return fig


def _fig_cpc_trends(monthly_data: list, top_codes: list[str], period: str = "monthly") -> go.Figure:
    if not monthly_data or not top_codes:
        return _empty_fig("No CPC trend data yet — run pipeline to ingest Lens/EPO patents")
    months = [m for m, _ in monthly_data]
    fig = go.Figure()
    for i, code in enumerate(top_codes):
        y = [counts.get(code, 0) for _, counts in monthly_data]
        fig.add_trace(go.Scatter(
            x=months, y=y,
            name=f"{code} — {_cpc_label(code)}",
            mode="lines+markers",
            line=dict(color=SERIES_COLORS[i % len(SERIES_COLORS)], width=2),
            marker=dict(size=5),
            hovertemplate=f"<b>{code}</b>  %{{x}}<br>%{{y}} patents<br>{_cpc_label(code)}<extra></extra>",
        ))
    period_note = _period_label(period)
    x_label = {"daily": "date", "weekly": "week start", "monthly": "month"}.get(period, "period")
    fig.update_layout(
        title=dict(
            text=f"CPC Family Trends  ·  {period_note}<br><sup>Patent volume per CPC subclass by {x_label} (grant › filing › ingest date)</sup>",
            font=dict(color=AMBER, size=12),
        ),
        legend=dict(font=dict(color=TEXT, size=9), bgcolor="rgba(0,0,0,0)", orientation="h", y=-0.2),
        xaxis=dict(tickfont=dict(size=9, color=DIM), **GRID),
        yaxis=dict(tickfont=dict(size=9, color=DIM), **GRID),
        **_chart_layout(margin=dict(l=10, r=10, t=52, b=50)),
    )
    return fig


def _fig_assignees(top: list[tuple[str, int]], period: str = "monthly") -> go.Figure:
    if not top:
        return _empty_fig("No assignee data yet")
    names, counts = zip(*top)
    names = [n[:30] + "…" if len(n) > 30 else n for n in names]
    period_note = _period_label(period)
    fig = go.Figure(go.Bar(
        x=list(counts), y=list(names), orientation="h",
        marker_color=TEAL, marker_line_width=0,
        hovertemplate="%{y}: %{x}<extra></extra>",
    ))
    fig.update_layout(
        title=dict(text=f"Top Assignees  ·  {period_note}", font=dict(color=AMBER, size=12)),
        yaxis=dict(autorange="reversed", tickfont=dict(size=9), **GRID),
        xaxis=dict(tickfont=dict(size=9, color=DIM), **GRID),
        **_chart_layout(),
    )
    return fig


def _fig_ingestion(runs: list[dict], period: str = "monthly") -> go.Figure:
    if not runs:
        return _empty_fig("No ingestion runs yet")
    dates = [r["date"] for r in runs]
    period_note = _period_label(period)
    fig = go.Figure()
    fig.add_trace(go.Bar(x=dates, y=[r["new"] for r in runs], name="New", marker_color=GREEN))
    fig.add_trace(go.Bar(x=dates, y=[r["updated"] for r in runs], name="Updated", marker_color=BLUE))
    fig.update_layout(
        title=dict(text=f"Ingestion History  ·  {period_note}", font=dict(color=AMBER, size=12)),
        barmode="stack",
        legend=dict(font=dict(color=TEXT, size=10), bgcolor="rgba(0,0,0,0)"),
        xaxis=dict(tickfont=dict(size=9, color=DIM), **GRID),
        yaxis=dict(tickfont=dict(size=9, color=DIM), **GRID),
        **_chart_layout(),
    )
    return fig


def _empty_fig(msg: str) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(text=msg, xref="paper", yref="paper", x=0.5, y=0.5,
                       showarrow=False, font=dict(color=DIM, size=12, family=MONO))
    fig.update_layout(**_chart_layout())
    return fig


# ── UI component helpers ──────────────────────────────────────────────────────

def _kpi_card(label: str, value: str, sub: str = "", accent: str = AMBER) -> dbc.Col:
    return dbc.Col(
        html.Div([
            html.Div(label, style={"color": DIM, "fontSize": "9px", "letterSpacing": "2px",
                                   "textTransform": "uppercase", "fontFamily": MONO, "marginBottom": "4px"}),
            html.Div(value, style={"color": accent, "fontSize": "30px", "fontWeight": "bold",
                                   "fontFamily": MONO, "lineHeight": "1.1"}),
            html.Div(sub, style={"color": DIM, "fontSize": "10px", "fontFamily": MONO, "marginTop": "4px"}),
        ], style={
            "background": CARD, "border": f"1px solid {BORDER}",
            "borderLeft": f"3px solid {accent}",
            "padding": "14px 18px", "borderRadius": "2px",
        }),
        xs=6, md=3,
    )


def _fmt_query_title(q: str) -> str:
    """Turn a raw query string into a readable title."""
    # Remove common filler stop-words that clutter a display title
    stops = {"and", "or", "the", "of", "in", "for", "with", "a", "an"}
    words = q.replace(",", " ").replace("-", " ").split()
    kept = [w for w in words if w.lower() not in stops] or words
    return " · ".join(w.title() for w in kept[:8])


def _analysis_panel(analyses: list[dict]) -> html.Div:
    if not analyses:
        return html.Div(
            "No analysis results yet. Run: python main.py run",
            style={"color": DIM, "fontFamily": MONO, "padding": "16px", "fontSize": "12px"},
        )
    items = []
    for idx, a in enumerate(analyses[:5]):
        title = _fmt_query_title(a["query"])
        themes = a["themes"] or []

        badges = [
            html.Button(
                t,
                id={"type": "theme-click", "q": f"{a['query']}|||{t}"},
                n_clicks=0,
                className="theme-badge",
                style={
                    "background": "#1a2035", "color": AMBER, "fontSize": "10px",
                    "padding": "3px 10px", "borderRadius": "3px",
                    "marginRight": "5px", "marginBottom": "5px",
                    "fontFamily": MONO,
                    "border": f"1px solid {AMBER}44",
                },
            )
            for t in themes  # show ALL themes
        ]

        items.append(html.Div([
            # ── Header row ──
            html.Div([
                html.Div(
                    title,
                    style={"color": AMBER, "fontSize": "13px", "fontFamily": MONO,
                           "fontWeight": "bold", "letterSpacing": "0.5px",
                           "marginBottom": "2px"},
                ),
                html.Div(
                    a["query"],
                    style={"color": DIM, "fontSize": "9px", "fontFamily": MONO,
                           "letterSpacing": "0.3px", "marginBottom": "6px"},
                ),
                html.Div([
                    html.Button(
                        f"{a['patent_count']} patents",
                        id={"type": "query-link", "q": a["query"]},
                        n_clicks=0,
                        className="patents-link",
                        style={"color": TEAL, "fontSize": "10px", "fontFamily": MONO,
                               "marginRight": "12px"},
                    ),
                    html.Span(
                        a["created_at"],
                        style={"color": DIM, "fontSize": "10px", "fontFamily": MONO,
                               "float": "right"},
                    ),
                ]),
            ], style={"marginBottom": "10px"}),

            # ── Theme badges (all of them) ──
            html.Div(badges, style={"marginBottom": "10px", "lineHeight": "2"}),

            # ── Strategic takeaway ──
            html.Div(
                a.get("takeaway") or "—",
                style={"color": TEXT, "fontSize": "11px", "fontFamily": MONO,
                       "fontStyle": "italic", "lineHeight": "1.7",
                       "borderLeft": f"2px solid {AMBER}55", "paddingLeft": "10px"},
            ),
        ], style={"marginBottom": "20px", "paddingBottom": "20px",
                  "borderBottom": f"1px solid {BORDER}"}))
    return html.Div(items)


def _chart_card(child, toggle: html.Div | None = None) -> html.Div:
    contents = ([toggle] if toggle else []) + [child]
    return html.Div(contents, style={
        "background": CARD, "border": f"1px solid {BORDER}",
        "borderRadius": "2px", "padding": "8px",
    })


# ── App layout ────────────────────────────────────────────────────────────────

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.CYBORG],
    title="Patent Intelligence Terminal",
    update_title=None,
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
server = app.server  # expose for gunicorn / Docker

app.layout = html.Div(
    style={"background": BG, "minHeight": "100vh"},
    children=[
        dcc.Interval(id="tick", interval=5 * 60 * 1000, n_intervals=0),
        dcc.Store(id="analysis-search-store"),

        # ── Header bar ────────────────────────────────────────────────────────
        html.Div(
            dbc.Container(dbc.Row([
                dbc.Col(html.Div([
                    html.Span("◈ ", style={"color": AMBER}),
                    html.Span("PATENT INTELLIGENCE TERMINAL",
                              style={"color": AMBER, "fontSize": "14px", "fontWeight": "bold",
                                     "fontFamily": MONO, "letterSpacing": "3px"}),
                    html.Span("  MEDTECH & NEUROTECH  |  POWERED BY LENS · USPTO ODP · EPO",
                              style={"color": DIM, "fontSize": "10px", "fontFamily": MONO,
                                     "marginLeft": "12px"}),
                ]), width="auto"),
                dbc.Col(html.Div([
                    html.Span("● LIVE  ", style={"color": GREEN, "fontSize": "10px", "fontFamily": MONO}),
                    html.Span(id="last-updated",
                              style={"color": DIM, "fontSize": "10px", "fontFamily": MONO}),
                ]), width="auto", className="ms-auto"),
            ], align="center"), fluid=True),
            style={"background": "#020508", "borderBottom": f"2px solid {AMBER}",
                   "padding": "10px 16px"},
        ),

        # ── Body ──────────────────────────────────────────────────────────────
        dbc.Container([

            # KPI row
            html.Div(style={"height": "14px"}),
            dbc.Row(id="kpi-row", className="g-2"),
            html.Div(style={"height": "14px"}),

            # Row 1: CPC distribution | Ingestion history
            dbc.Row([
                dbc.Col(
                    _chart_card(
                        dcc.Graph(id="cpc-bar", config={"displayModeBar": False},
                                  style={"height": "320px"}),
                        toggle=_period_toggle("cpc-bar-period"),
                    ),
                    md=6,
                ),
                dbc.Col(
                    _chart_card(
                        dcc.Graph(id="ingestion-chart", config={"displayModeBar": False},
                                  style={"height": "320px"}),
                        toggle=_period_toggle("ingestion-period"),
                    ),
                    md=6,
                ),
            ], className="g-2"),
            html.Div(style={"height": "10px"}),

            # Row 2: CPC trends (wide) | Top assignees
            dbc.Row([
                dbc.Col(
                    _chart_card(
                        dcc.Graph(id="cpc-trends", config={"displayModeBar": False},
                                  style={"height": "300px"}),
                        toggle=_period_toggle("cpc-trend-period"),
                    ),
                    md=7,
                ),
                dbc.Col(
                    _chart_card(
                        dcc.Graph(id="assignees-bar", config={"displayModeBar": False},
                                  style={"height": "300px"}),
                        toggle=_period_toggle("assignees-period"),
                    ),
                    md=5,
                ),
            ], className="g-2"),
            html.Div(style={"height": "10px"}),

            # Row 3: AI Analysis panel
            dbc.Row([dbc.Col(html.Div([
                html.Div([
                    html.Span("AI LANDSCAPE ANALYSIS",
                              style={"color": AMBER, "fontSize": "10px", "fontFamily": MONO,
                                     "letterSpacing": "2px"}),
                    html.Span("  ·  latest results per query",
                              style={"color": DIM, "fontSize": "10px", "fontFamily": MONO}),
                ], style={"marginBottom": "14px", "paddingBottom": "8px",
                           "borderBottom": f"1px solid {BORDER}"}),
                html.Div(id="analysis-panel"),
            ], style={"background": CARD, "border": f"1px solid {BORDER}",
                       "borderRadius": "2px", "padding": "16px 20px"}))], className="g-2"),
            html.Div(style={"height": "10px"}),

            # Row 4: Patent Search
            dbc.Row([dbc.Col(html.Div([

                # ── Section header ──
                html.Div([
                    html.Span("PATENT SEARCH",
                              style={"color": AMBER, "fontSize": "10px", "fontFamily": MONO,
                                     "letterSpacing": "2px"}),
                    html.Span("  ·  full-text search across title & abstract",
                              style={"color": DIM, "fontSize": "10px", "fontFamily": MONO}),
                ], style={"marginBottom": "14px", "paddingBottom": "8px",
                           "borderBottom": f"1px solid {BORDER}"}),

                # ── Search bar row ──
                html.Div([
                    dcc.Input(
                        id="search-input",
                        type="text",
                        placeholder="Search patents by keyword, technology, assignee…",
                        debounce=False,
                        style={
                            "flex": "1", "background": BG, "color": TEXT,
                            "border": f"1px solid {BORDER}", "borderRadius": "2px",
                            "padding": "8px 12px", "fontFamily": MONO, "fontSize": "12px",
                            "outline": "none",
                        },
                    ),
                    dcc.Input(
                        id="search-cpc",
                        type="text",
                        placeholder="CPC prefix  e.g. A61B",
                        maxLength=8,
                        style={
                            "width": "160px", "background": BG, "color": TEXT,
                            "border": f"1px solid {BORDER}", "borderRadius": "2px",
                            "padding": "8px 12px", "fontFamily": MONO, "fontSize": "12px",
                            "outline": "none",
                        },
                    ),
                    dcc.Dropdown(
                        id="search-source",
                        options=[
                            {"label": "All sources", "value": "all"},
                            {"label": "Lens.org",    "value": "lens"},
                            {"label": "EPO",         "value": "epo"},
                            {"label": "USPTO ODP",   "value": "patentsview"},
                        ],
                        value="all",
                        clearable=False,
                        style={
                            "width": "150px", "fontFamily": MONO, "fontSize": "11px",
                            "background": BG,
                        },
                    ),
                    html.Button(
                        "SEARCH",
                        id="search-btn",
                        n_clicks=0,
                        style={
                            "background": AMBER, "color": BG,
                            "border": "none", "borderRadius": "2px",
                            "padding": "8px 20px", "fontFamily": MONO,
                            "fontSize": "11px", "fontWeight": "bold",
                            "letterSpacing": "1px", "cursor": "pointer",
                        },
                    ),
                ], style={"display": "flex", "gap": "8px",
                           "alignItems": "center", "marginBottom": "10px"}),

                # ── Result count ──
                html.Div(id="search-count",
                         style={"color": DIM, "fontSize": "10px", "fontFamily": MONO,
                                "marginBottom": "10px"}),

                # ── Results ──
                html.Div(id="search-results"),

            ], style={"background": CARD, "border": f"1px solid {BORDER}",
                       "borderRadius": "2px", "padding": "16px 20px"}))],
                className="g-2"),

            html.Div(style={"height": "10px"}),

            # Row 5: Patent browser table
            dbc.Row([dbc.Col(html.Div([
                html.Div([
                    html.Span("PATENT BROWSER",
                              style={"color": AMBER, "fontSize": "10px", "fontFamily": MONO,
                                     "letterSpacing": "2px"}),
                    html.Span(id="table-count",
                              style={"color": DIM, "fontSize": "10px", "fontFamily": MONO,
                                     "marginLeft": "12px"}),
                ], style={"marginBottom": "12px", "paddingBottom": "8px",
                           "borderBottom": f"1px solid {BORDER}"}),
                dash_table.DataTable(
                    id="patent-table",
                    columns=[
                        *[{"name": c, "id": c} for c in
                          ["Date", "Source", "Patent ID", "Title", "Assignee", "Query", "CPC"]],
                        {"name": "Link", "id": "Link", "presentation": "markdown"},
                    ],
                    data=[],
                    page_size=20,
                    filter_action="native",
                    sort_action="native",
                    markdown_options={"link_target": "_blank"},
                    style_table={"overflowX": "auto"},
                    style_cell={
                        "background": CARD2, "color": TEXT,
                        "border": f"1px solid {BORDER}",
                        "fontFamily": MONO, "fontSize": "11px",
                        "padding": "6px 10px", "textAlign": "left",
                        "maxWidth": "220px", "overflow": "hidden",
                        "textOverflow": "ellipsis",
                    },
                    style_header={
                        "background": BG, "color": AMBER,
                        "fontWeight": "bold", "fontSize": "9px",
                        "letterSpacing": "1px", "border": f"1px solid {BORDER}",
                        "textTransform": "uppercase", "fontFamily": MONO,
                    },
                    style_filter={
                        "background": BG, "color": TEXT,
                        "border": f"1px solid {BORDER}", "fontFamily": MONO,
                    },
                    style_data_conditional=[
                        {"if": {"row_index": "odd"}, "backgroundColor": "#0a1020"},
                        {"if": {"filter_query": '{Source} = "lens"'}, "color": TEAL},
                        {"if": {"filter_query": '{Source} = "patentsview"'}, "color": BLUE},
                        {"if": {"filter_query": '{Source} = "epo"'}, "color": GREEN},
                    ],
                ),
            ], style={"background": CARD, "border": f"1px solid {BORDER}",
                       "borderRadius": "2px", "padding": "16px 20px"}))], className="g-2"),

            html.Div(style={"height": "28px"}),
        ], fluid=True),
    ],
)


# ── Callbacks — one per chart so each toggle fires independently ──────────────

def _error_fig(exc: Exception) -> go.Figure:
    return _empty_fig(f"DB error: {exc}")


@app.callback(
    Output("kpi-row", "children"),
    Output("analysis-panel", "children"),
    Output("patent-table", "data"),
    Output("table-count", "children"),
    Output("last-updated", "children"),
    Input("tick", "n_intervals"),
)
def refresh_static(_n: int):
    try:
        kpis     = _kpis()
        patents  = _patent_table(500)
        analyses = _latest_analyses(5)
    except Exception as exc:
        no_data_msg = html.Div(
            f"Database error: {exc}",
            style={"color": RED, "fontFamily": MONO, "padding": "16px", "fontSize": "12px"},
        )
        kpi_row = [
            _kpi_card("Total Patents",  "—", "waiting for data", DIM),
            _kpi_card("New (Last Run)", "—", "waiting for data", DIM),
            _kpi_card("Last Ingest",   "—", "no runs yet",      DIM),
            _kpi_card("Last Analysis", "—", "no runs yet",      DIM),
        ]
        return (kpi_row, no_data_msg, [],
                "— no data yet —",
                f"Last refreshed {datetime.now().strftime('%H:%M:%S')}")

    kpi_row = [
        _kpi_card("Total Patents",   f"{kpis['total']:,}",          "in database",                  AMBER),
        _kpi_card("New (Last Run)",  f"{kpis['last_run_new']:,}",   f"ingested {kpis['last_run_at']}", GREEN),
        _kpi_card("Last Ingest Run", kpis["last_run_at"],           "pipeline completed",           BLUE),
        _kpi_card("Last Analysis",   kpis["last_analysis_at"],      kpis["last_analysis_query"],    TEAL),
    ]
    return (
        kpi_row,
        _analysis_panel(analyses),
        patents,
        f"— showing latest {len(patents):,} records — filter by any column",
        f"Last refreshed {datetime.now().strftime('%H:%M:%S')}",
    )


@app.callback(
    Output("cpc-bar", "figure"),
    Input("tick", "n_intervals"),
    Input("cpc-bar-period", "value"),
)
def refresh_cpc_bar(_n: int, period: str):
    try:
        return _fig_cpc_bar(_cpc_distribution(20, period), period)
    except Exception as exc:
        return _error_fig(exc)


@app.callback(
    Output("cpc-trends", "figure"),
    Input("tick", "n_intervals"),
    Input("cpc-trend-period", "value"),
)
def refresh_cpc_trends(_n: int, period: str):
    try:
        monthly, top = _cpc_over_time(5, period)
        return _fig_cpc_trends(monthly, top, period)
    except Exception as exc:
        return _error_fig(exc)


@app.callback(
    Output("assignees-bar", "figure"),
    Input("tick", "n_intervals"),
    Input("assignees-period", "value"),
)
def refresh_assignees(_n: int, period: str):
    try:
        return _fig_assignees(_top_assignees(15, period), period)
    except Exception as exc:
        return _error_fig(exc)


@app.callback(
    Output("ingestion-chart", "figure"),
    Input("tick", "n_intervals"),
    Input("ingestion-period", "value"),
)
def refresh_ingestion(_n: int, period: str):
    try:
        return _fig_ingestion(_ingestion_history(60, period), period)
    except Exception as exc:
        return _error_fig(exc)


@app.callback(
    Output("analysis-search-store", "data"),
    Input({"type": "query-link", "q": dash.ALL}, "n_clicks"),
    Input({"type": "theme-click", "q": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def handle_analysis_click(_qclicks, _tclicks):
    tid = dash.ctx.triggered_id
    if not tid:
        return dash.no_update
    raw = tid["q"]
    if tid["type"] == "query-link":
        # "N patents" click — find all patents for this matched_query
        return {"mode": "query", "term": raw}
    else:
        # Theme badge click — search title+abstract for the theme text
        # raw is "original_query|||theme_text"
        theme = raw.split("|||", 1)[-1] if "|||" in raw else raw
        return {"mode": "theme", "term": theme}


@app.callback(
    Output("search-results", "children"),
    Output("search-count", "children"),
    Output("search-input", "value"),
    Input("search-btn", "n_clicks"),
    Input("search-input", "n_submit"),
    Input("analysis-search-store", "data"),
    dash.dependencies.State("search-input", "value"),
    dash.dependencies.State("search-cpc", "value"),
    dash.dependencies.State("search-source", "value"),
    prevent_initial_call=True,
)
def do_search(_clicks, _submit, store_data, query: str, cpc: str, source: str):
    matched_query_filter = ""

    if dash.ctx.triggered_id == "analysis-search-store" and store_data:
        mode = store_data.get("mode", "theme")
        term = store_data.get("term", "")
        if mode == "query":
            matched_query_filter = term
            query = ""          # bypass title/abstract search
            label_term = f"query: {term[:60]}"
        else:
            query = term
            label_term = f"'{term}'"
        cpc = ""
        source = "all"
    else:
        label_term = f"'{(query or '').strip()}'" if (query or "").strip() else ""

    q = (query or "").strip()
    cpc = (cpc or "").strip()

    if not q and not cpc and not matched_query_filter:
        return (
            [],
            html.Span("Enter a keyword or CPC prefix to search.",
                      style={"color": DIM, "fontFamily": MONO, "fontSize": "11px"}),
            query or "",
        )
    try:
        results = _search_patents(q, cpc, source or "all", matched_query_filter, limit=100)
    except Exception as exc:
        return (
            [html.Div(f"Search error: {exc}",
                      style={"color": RED, "fontFamily": MONO, "fontSize": "12px"})],
            "",
            query or "",
        )
    count_label = (
        f"-- {len(results)} result{'s' if len(results) != 1 else ''}"
        + (f"  for {label_term}" if label_term else "")
        + (f"  |  CPC: {cpc.upper()}" if cpc else "")
        + ("  |  showing first 100" if len(results) == 100 else "")
    )
    display_query = q if q else (store_data or {}).get("term", "") if store_data else ""
    return _render_search_results(results), count_label, display_query


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8050))
    app.run(debug=False, host="0.0.0.0", port=port)
