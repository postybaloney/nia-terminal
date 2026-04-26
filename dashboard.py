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
from datetime import datetime, timezone

import dash
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Input, Output, dash_table, dcc, html

from db import get_session, init_db
from db.models import AnalysisResult, IngestRun, RawPatent
from sqlalchemy import func

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


def _cpc_distribution(limit: int = 20) -> list[tuple[str, int]]:
    with get_session() as s:
        rows = s.query(RawPatent.cpc_codes).filter(RawPatent.cpc_codes.isnot(None)).all()
    counter: Counter = Counter()
    for (codes,) in rows:
        if isinstance(codes, list):
            for c in codes:
                p = c[:4] if c else None
                if p:
                    counter[p] += 1
    return counter.most_common(limit)


def _cpc_over_time(top_n: int = 5) -> tuple[list, list]:
    with get_session() as s:
        rows = (
            s.query(RawPatent.grant_date, RawPatent.cpc_codes)
            .filter(RawPatent.grant_date.isnot(None), RawPatent.cpc_codes.isnot(None))
            .all()
        )
    all_codes: Counter = Counter()
    for _, codes in rows:
        if isinstance(codes, list):
            for c in codes:
                p = c[:4] if c else None
                if p:
                    all_codes[p] += 1
    top = [code for code, _ in all_codes.most_common(top_n)]
    monthly: dict[str, Counter] = {}
    for grant_date, codes in rows:
        if not grant_date or not isinstance(codes, list):
            continue
        month = grant_date.strftime("%Y-%m")
        if month not in monthly:
            monthly[month] = Counter()
        for c in codes:
            p = c[:4] if c else None
            if p in top:
                monthly[month][p] += 1
    return sorted(monthly.items()), top


def _top_assignees(limit: int = 15) -> list[tuple[str, int]]:
    with get_session() as s:
        rows = s.query(RawPatent.assignees).filter(RawPatent.assignees.isnot(None)).all()
    counter: Counter = Counter()
    for (assignees,) in rows:
        if isinstance(assignees, list):
            for a in assignees:
                name = (a.get("name") or "").strip()
                if name and name.lower() not in ("", "unknown"):
                    counter[name] += 1
    return counter.most_common(limit)


def _ingestion_history(limit: int = 30) -> list[dict]:
    with get_session() as s:
        runs = (
            s.query(IngestRun.started_at, IngestRun.new_patents, IngestRun.updated_patents,
                    IngestRun.errors, IngestRun.success)
            .order_by(IngestRun.started_at.asc())
            .limit(limit)
            .all()
        )
    return [
        {
            "date": r.started_at.strftime("%m/%d %H:%M") if r.started_at else "",
            "new": r.new_patents or 0,
            "updated": r.updated_patents or 0,
            "errors": len(r.errors or []),
        }
        for r in runs
    ]


def _patent_table(limit: int = 500) -> list[dict]:
    with get_session() as s:
        rows = (
            s.query(
                RawPatent.grant_date, RawPatent.filing_date, RawPatent.first_seen_at,
                RawPatent.source, RawPatent.source_id, RawPatent.title,
                RawPatent.assignees, RawPatent.matched_query, RawPatent.cpc_codes,
            )
            .order_by(RawPatent.first_seen_at.desc())
            .limit(limit)
            .all()
        )
    data = []
    for r in rows:
        date = r.grant_date or r.filing_date or r.first_seen_at
        assignee = ((r.assignees or [{}])[0].get("name") or "") if r.assignees else ""
        data.append({
            "Date": date.strftime("%Y-%m-%d") if date else "",
            "Source": r.source or "",
            "Patent ID": r.source_id or "",
            "Title": (r.title or "—")[:80],
            "Assignee": assignee[:40],
            "Query": (r.matched_query or "")[:50],
            "CPC": ", ".join((r.cpc_codes or [])[:3]),
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


# ── Chart builders ────────────────────────────────────────────────────────────

def _chart_layout(**extra):
    return dict(**CHART_BG, **CHART_FONT, **CHART_MARGIN, **extra)


def _fig_cpc_bar(top_codes: list[tuple[str, int]]) -> go.Figure:
    if not top_codes:
        return _empty_fig("No CPC data yet")
    codes, counts = zip(*top_codes)
    fig = go.Figure(go.Bar(
        x=list(counts), y=list(codes), orientation="h",
        marker_color=AMBER, marker_line_width=0,
        hovertemplate="%{y}: %{x}<extra></extra>",
    ))
    fig.update_layout(
        title=dict(text="CPC Code Distribution (top 20)", font=dict(color=AMBER, size=12)),
        yaxis=dict(autorange="reversed", tickfont=dict(size=9), **GRID),
        xaxis=dict(tickfont=dict(size=9, color=DIM), **GRID),
        **_chart_layout(),
    )
    return fig


def _fig_cpc_trends(monthly_data: list, top_codes: list[str]) -> go.Figure:
    if not monthly_data or not top_codes:
        return _empty_fig("No CPC trend data yet")
    months = [m for m, _ in monthly_data]
    fig = go.Figure()
    for i, code in enumerate(top_codes):
        y = [counts.get(code, 0) for _, counts in monthly_data]
        fig.add_trace(go.Scatter(
            x=months, y=y, name=code, mode="lines+markers",
            line=dict(color=SERIES_COLORS[i % len(SERIES_COLORS)], width=2),
            marker=dict(size=5),
        ))
    fig.update_layout(
        title=dict(text="CPC Family Trends by Grant Month", font=dict(color=AMBER, size=12)),
        legend=dict(font=dict(color=TEXT, size=10), bgcolor="rgba(0,0,0,0)", orientation="h", y=-0.15),
        xaxis=dict(tickfont=dict(size=9, color=DIM), **GRID),
        yaxis=dict(tickfont=dict(size=9, color=DIM), **GRID),
        **_chart_layout(margin=dict(l=10, r=10, t=36, b=40)),
    )
    return fig


def _fig_assignees(top: list[tuple[str, int]]) -> go.Figure:
    if not top:
        return _empty_fig("No assignee data yet")
    names, counts = zip(*top)
    names = [n[:30] + "…" if len(n) > 30 else n for n in names]
    fig = go.Figure(go.Bar(
        x=list(counts), y=list(names), orientation="h",
        marker_color=TEAL, marker_line_width=0,
        hovertemplate="%{y}: %{x}<extra></extra>",
    ))
    fig.update_layout(
        title=dict(text="Top Assignees by Patent Count", font=dict(color=AMBER, size=12)),
        yaxis=dict(autorange="reversed", tickfont=dict(size=9), **GRID),
        xaxis=dict(tickfont=dict(size=9, color=DIM), **GRID),
        **_chart_layout(),
    )
    return fig


def _fig_ingestion(runs: list[dict]) -> go.Figure:
    if not runs:
        return _empty_fig("No ingestion runs yet")
    dates = [r["date"] for r in runs]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=dates, y=[r["new"] for r in runs], name="New", marker_color=GREEN))
    fig.add_trace(go.Bar(x=dates, y=[r["updated"] for r in runs], name="Updated", marker_color=BLUE))
    fig.update_layout(
        title=dict(text="Ingestion History", font=dict(color=AMBER, size=12)),
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


def _analysis_panel(analyses: list[dict]) -> html.Div:
    if not analyses:
        return html.Div(
            "No analysis results yet. Run: python main.py run",
            style={"color": DIM, "fontFamily": MONO, "padding": "16px", "fontSize": "12px"},
        )
    items = []
    for a in analyses[:4]:
        badges = [
            html.Span(t, style={
                "background": "#1a2035", "color": AMBER, "fontSize": "10px",
                "padding": "2px 8px", "borderRadius": "2px",
                "marginRight": "4px", "marginBottom": "4px",
                "display": "inline-block", "fontFamily": MONO,
                "border": f"1px solid {AMBER}44",
            })
            for t in (a["themes"] or [])[:6]
        ]
        items.append(html.Div([
            html.Div([
                html.Span(a["query"][:55], style={"color": AMBER, "fontSize": "11px",
                                                   "fontFamily": MONO, "fontWeight": "bold"}),
                html.Span(f"  ·  {a['patent_count']} patents",
                          style={"color": DIM, "fontSize": "10px", "fontFamily": MONO}),
                html.Span(a["created_at"],
                          style={"color": DIM, "fontSize": "10px", "fontFamily": MONO,
                                 "float": "right"}),
            ], style={"marginBottom": "8px"}),
            html.Div(badges, style={"marginBottom": "8px"}),
            html.Div(
                a.get("takeaway") or "—",
                style={"color": TEXT, "fontSize": "11px", "fontFamily": MONO,
                       "fontStyle": "italic", "lineHeight": "1.6",
                       "borderLeft": f"2px solid {AMBER}55", "paddingLeft": "10px"},
            ),
        ], style={"marginBottom": "18px", "paddingBottom": "18px",
                  "borderBottom": f"1px solid {BORDER}"}))
    return html.Div(items)


# ── UI component helpers (cont.) ─────────────────────────────────────────────

def _chart_card(child) -> html.Div:
    return html.Div(child, style={
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
                dbc.Col(_chart_card(dcc.Graph(id="cpc-bar", config={"displayModeBar": False},
                                              style={"height": "320px"})), md=6),
                dbc.Col(_chart_card(dcc.Graph(id="ingestion-chart", config={"displayModeBar": False},
                                              style={"height": "320px"})), md=6),
            ], className="g-2"),
            html.Div(style={"height": "10px"}),

            # Row 2: CPC trends (wide) | Top assignees
            dbc.Row([
                dbc.Col(_chart_card(dcc.Graph(id="cpc-trends", config={"displayModeBar": False},
                                              style={"height": "300px"})), md=7),
                dbc.Col(_chart_card(dcc.Graph(id="assignees-bar", config={"displayModeBar": False},
                                              style={"height": "300px"})), md=5),
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

            # Row 4: Patent browser table
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
                    columns=[{"name": c, "id": c}
                             for c in ["Date", "Source", "Patent ID", "Title",
                                       "Assignee", "Query", "CPC"]],
                    data=[],
                    page_size=20,
                    filter_action="native",
                    sort_action="native",
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


# ── Single callback refreshes everything ──────────────────────────────────────

@app.callback(
    Output("kpi-row", "children"),
    Output("cpc-bar", "figure"),
    Output("cpc-trends", "figure"),
    Output("assignees-bar", "figure"),
    Output("ingestion-chart", "figure"),
    Output("analysis-panel", "children"),
    Output("patent-table", "data"),
    Output("table-count", "children"),
    Output("last-updated", "children"),
    Input("tick", "n_intervals"),
)
def refresh(_n: int):
    try:
        kpis         = _kpis()
        cpc_dist     = _cpc_distribution(20)
        monthly, top = _cpc_over_time(5)
        assignees    = _top_assignees(15)
        runs         = _ingestion_history(30)
        patents      = _patent_table(500)
        analyses     = _latest_analyses(5)
    except Exception as exc:
        empty = _empty_fig(f"DB error: {exc}")
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
        return (kpi_row, empty, empty, empty, empty, no_data_msg, [],
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
        _fig_cpc_bar(cpc_dist),
        _fig_cpc_trends(monthly, top),
        _fig_assignees(assignees),
        _fig_ingestion(runs),
        _analysis_panel(analyses),
        patents,
        f"— showing latest {len(patents):,} records — filter by any column",
        f"Last refreshed {datetime.now().strftime('%H:%M:%S')}",
    )


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8050))
    app.run(debug=False, host="0.0.0.0", port=port)
