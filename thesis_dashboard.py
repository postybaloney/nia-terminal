"""
Research Intelligence Terminal — PhD & Academic Literature Dashboard.

Mirrors the Bloomberg-style design of the patent dashboard.

Run standalone:
    python thesis_dashboard.py          → http://localhost:8051

Or alongside the patent dashboard (both can run simultaneously):
    python dashboard.py          → http://localhost:8050
    python thesis_dashboard.py   → http://localhost:8051
"""
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone

import dash
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Input, Output, State, dash_table, dcc, html
from sqlalchemy import func, or_

from db import get_session
from db.models import AnalysisResult

# Register Thesis on Base.metadata before init_db
from db.thesis_models import Thesis  # noqa: F401

try:
    from thesis_pipeline import init_thesis_db
    init_thesis_db()
except Exception as _e:
    import logging as _logging
    _logging.getLogger(__name__).warning("Thesis DB init skipped: %s", _e)

# ── Colour palette (identical to patent dashboard) ────────────────────────────
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
CYAN   = "#06b6d4"
ORANGE = "#f97316"

CHART_BG     = dict(paper_bgcolor=CARD, plot_bgcolor=CARD)
CHART_FONT   = dict(font=dict(color=TEXT, family="'Courier New', monospace", size=11))
CHART_MARGIN = dict(margin=dict(l=10, r=10, t=36, b=10))
GRID         = dict(gridcolor=BORDER, zerolinecolor=BORDER)
MONO         = "'Courier New', monospace"

SOURCE_COLORS = {
    "openalex": AMBER,
    "ndltd":    TEAL,
    "dart":     BLUE,
    "ethos":    PURPLE,
}


# ── Chart layout helper ───────────────────────────────────────────────────────

def _chart_layout(**extra) -> dict:
    base = {**CHART_BG, **CHART_FONT, **CHART_MARGIN}
    base.update(extra)
    return base


# ── Data helpers ──────────────────────────────────────────────────────────────

def _thesis_kpis() -> dict:
    with get_session() as s:
        total   = s.query(func.count(Thesis.id)).scalar() or 0
        hw      = s.query(func.count(Thesis.id)).filter(Thesis.hardware_relevant.is_(True)).scalar() or 0
        sw      = s.query(func.count(Thesis.id)).filter(Thesis.software_relevant.is_(True)).scalar() or 0
        sources = s.query(Thesis.source).distinct().count()
        min_yr  = s.query(func.min(Thesis.year)).scalar()
        max_yr  = s.query(func.max(Thesis.year)).scalar()
        latest_analysis = (
            s.query(AnalysisResult.created_at)
            .filter(AnalysisResult.query == "[thesis_batch]")
            .order_by(AnalysisResult.created_at.desc())
            .first()
        )
    return {
        "total":   total,
        "hw":      hw,
        "sw":      sw,
        "sources": sources,
        "min_yr":  min_yr or "—",
        "max_yr":  max_yr or "—",
        "last_analysis": (
            latest_analysis.created_at.strftime("%m/%d %H:%M") if latest_analysis else "—"
        ),
    }


def _theses_by_year() -> list[dict]:
    with get_session() as s:
        rows = (
            s.query(Thesis.year, Thesis.hardware_relevant, func.count(Thesis.id))
            .filter(Thesis.year.isnot(None))
            .group_by(Thesis.year, Thesis.hardware_relevant)
            .order_by(Thesis.year)
            .all()
        )
    return [{"year": r[0], "hw": r[1], "count": r[2]} for r in rows]


def _theses_by_country(limit: int = 15) -> list[tuple]:
    with get_session() as s:
        rows = (
            s.query(Thesis.country, func.count(Thesis.id))
            .filter(Thesis.country.isnot(None))
            .group_by(Thesis.country)
            .order_by(func.count(Thesis.id).desc())
            .limit(limit)
            .all()
        )
    return [(r[0], r[1]) for r in rows]


def _theses_by_source() -> list[tuple]:
    with get_session() as s:
        rows = (
            s.query(Thesis.source, func.count(Thesis.id))
            .group_by(Thesis.source)
            .order_by(func.count(Thesis.id).desc())
            .all()
        )
    return [(r[0], r[1]) for r in rows]


def _top_institutions(limit: int = 12) -> list[tuple]:
    with get_session() as s:
        rows = (
            s.query(Thesis.institution, func.count(Thesis.id))
            .filter(Thesis.institution.isnot(None))
            .group_by(Thesis.institution)
            .order_by(func.count(Thesis.id).desc())
            .limit(limit)
            .all()
        )
    return [(r[0][:50], r[1]) for r in rows]


def _latest_thesis_analysis() -> dict | None:
    with get_session() as s:
        result = (
            s.query(AnalysisResult)
            .filter(AnalysisResult.query == "[thesis_batch]")
            .order_by(AnalysisResult.created_at.desc())
            .first()
        )
        if not result:
            return None
        text    = result.analysis_text
        created = result.created_at
    if not text:
        return None
    try:
        data = json.loads(text)
        data["_created_at"] = created.strftime("%Y-%m-%d %H:%M UTC") if created else ""
        return data
    except Exception:
        return None


def _all_sources() -> list[str]:
    with get_session() as s:
        rows = s.query(Thesis.source).distinct().all()
    return [r[0] for r in rows if r[0]]


def _search_theses(
    query: str = "",
    year_from: int | None = None,
    year_to: int | None = None,
    relevance: str = "all",
    source_filter: str = "all",
    limit: int = 100,
) -> list[dict]:
    with get_session() as s:
        q = s.query(Thesis)

        if query.strip():
            term = f"%{query.strip()}%"
            q = q.filter(
                or_(
                    Thesis.title.ilike(term),
                    Thesis.abstract.ilike(term),
                    Thesis.author.ilike(term),
                    Thesis.institution.ilike(term),
                )
            )
        if year_from:
            q = q.filter(Thesis.year >= year_from)
        if year_to:
            q = q.filter(Thesis.year <= year_to)
        if relevance == "hardware":
            q = q.filter(Thesis.hardware_relevant.is_(True))
        elif relevance == "software":
            q = q.filter(Thesis.software_relevant.is_(True))
        if source_filter != "all":
            q = q.filter(Thesis.source == source_filter)

        rows = q.order_by(Thesis.year.desc().nullslast(), Thesis.id.desc()).limit(limit).all()
        return [_row_to_dict(r) for r in rows]


def _row_to_dict(r: Thesis) -> dict:
    import urllib.parse as _up

    doi = r.doi or ""
    raw = r.raw_payload or {}

    # Thesis URL: stored url → DOI → OpenAlex record page
    url = (
        r.url
        or (f"https://doi.org/{doi}" if doi else None)
        or ""
    )

    # Author profile links —————————————————————————————————————————————
    author_name = r.author or ""

    # From raw_payload (populated for records ingested after this update)
    author_openalex_url = raw.get("author_openalex_url") or ""
    author_orcid        = raw.get("author_orcid") or ""

    # Constructed search URLs (work for ALL existing records from stored author name)
    google_scholar_url = (
        f"https://scholar.google.com/scholar?q={_up.quote(chr(34) + author_name + chr(34))}"
        if author_name and author_name != "—" else ""
    )
    linkedin_url = (
        f"https://www.linkedin.com/search/results/people/?keywords={_up.quote(author_name)}"
        if author_name and author_name != "—" else ""
    )

    return {
        "title":              r.title or "",
        "author":             author_name or "—",
        "institution":        (r.institution or "—")[:60],
        "country":            r.country or "—",
        "year":               r.year or "—",
        "source":             r.source or "",
        "url":                url,
        "abstract":           (r.abstract or "")[:300],
        "type":               "/".join(filter(None, [
            "HW" if r.hardware_relevant else "",
            "SW" if r.software_relevant else "",
        ])) or "—",
        "doi":                doi,
        "author_openalex_url": author_openalex_url,
        "author_orcid":        author_orcid,
        "google_scholar_url":  google_scholar_url,
        "linkedin_url":        linkedin_url,
    }


# ── UI helpers ────────────────────────────────────────────────────────────────

def _kpi_card(label: str, value: str, color: str = AMBER) -> dbc.Col:
    return dbc.Col(
        html.Div(
            [
                html.Div(label, style={
                    "fontSize": "9px", "color": DIM, "letterSpacing": "2px",
                    "fontFamily": MONO, "marginBottom": "4px",
                }),
                html.Div(str(value), style={
                    "fontSize": "26px", "fontWeight": "700",
                    "color": color, "fontFamily": MONO, "lineHeight": "1",
                }),
            ],
            style={
                "background": CARD, "border": f"1px solid {BORDER}",
                "borderTop": f"2px solid {color}",
                "padding": "14px 18px", "borderRadius": "4px",
            },
        ),
        width=True,
    )


def _section_header(title: str) -> html.Div:
    return html.Div(
        [
            html.Span("▶ ", style={"color": AMBER}),
            html.Span(title, style={
                "fontSize": "10px", "letterSpacing": "3px",
                "color": DIM, "fontFamily": MONO,
            }),
            html.Hr(style={"borderColor": BORDER, "margin": "6px 0 14px"}),
        ]
    )


def _render_thesis_analysis(data: dict) -> list:
    """Render the AI research landscape panel from thesis analysis JSON."""
    clusters     = data.get("research_clusters") or []
    institutions = data.get("top_institutions") or []
    breakout     = data.get("breakout_research") or []
    emerging     = data.get("emerging_methods") or []
    proximity    = data.get("patent_proximity", "")
    insight      = data.get("strategic_insight", "")
    created      = data.get("_created_at", "")

    COLORS = [AMBER, TEAL, BLUE, PURPLE, GREEN, CYAN, ORANGE, RED]

    children = []

    if created:
        children.append(
            html.Div(f"Analysis generated: {created}", style={
                "fontSize": "9px", "color": DIM, "fontFamily": MONO,
                "marginBottom": "12px",
            })
        )

    # ── Strategic insight ──────────────────────────────────────────────────
    if insight:
        children.append(
            html.Div(
                [
                    html.Span("◈ STRATEGIC INSIGHT  ", style={
                        "color": AMBER, "fontSize": "9px",
                        "fontFamily": MONO, "letterSpacing": "2px",
                    }),
                    html.Span(insight, style={
                        "color": TEXT, "fontSize": "12px", "fontFamily": MONO,
                    }),
                ],
                style={
                    "background": "#0a0f1a", "border": f"1px solid {AMBER}",
                    "borderLeft": f"3px solid {AMBER}",
                    "padding": "10px 14px", "borderRadius": "3px",
                    "marginBottom": "16px",
                },
            )
        )

    # ── Research clusters ──────────────────────────────────────────────────
    if clusters:
        children.append(
            html.Div("RESEARCH CLUSTERS", style={
                "fontSize": "9px", "color": DIM, "letterSpacing": "2px",
                "fontFamily": MONO, "marginBottom": "8px",
            })
        )
        cluster_cards = []
        for i, cl in enumerate(clusters):
            color = COLORS[i % len(COLORS)]
            hw_sw = cl.get("hardware_or_software", "")
            hw_sw_label = {"hardware": "HW", "software": "SW", "both": "HW+SW"}.get(hw_sw, "")
            cluster_cards.append(
                html.Div(
                    [
                        html.Div(
                            [
                                html.Span(cl.get("theme", ""), style={
                                    "color": color, "fontWeight": "700",
                                    "fontSize": "11px", "fontFamily": MONO,
                                }),
                                html.Span(
                                    f"  {cl.get('thesis_count', '')} theses",
                                    style={"color": DIM, "fontSize": "9px", "fontFamily": MONO},
                                ),
                                html.Span(
                                    f"  [{hw_sw_label}]" if hw_sw_label else "",
                                    style={"color": color, "fontSize": "9px", "fontFamily": MONO, "opacity": "0.7"},
                                ),
                            ],
                            style={"marginBottom": "4px"},
                        ),
                        html.Div(
                            cl.get("description", ""),
                            style={"color": DIM, "fontSize": "10px", "fontFamily": MONO, "lineHeight": "1.5"},
                        ),
                    ],
                    id={"type": "cluster-click", "q": cl.get("theme", "")},
                    className="theme-badge",
                    style={
                        "background": CARD2, "border": f"1px solid {color}33",
                        "borderLeft": f"3px solid {color}",
                        "padding": "10px 12px", "borderRadius": "3px",
                        "marginBottom": "8px", "cursor": "pointer",
                    },
                )
            )
        children.append(html.Div(cluster_cards, style={"marginBottom": "16px"}))

    # ── Top institutions ───────────────────────────────────────────────────
    if institutions:
        children.append(
            html.Div("TOP INSTITUTIONS", style={
                "fontSize": "9px", "color": DIM, "letterSpacing": "2px",
                "fontFamily": MONO, "marginBottom": "8px",
            })
        )
        inst_items = []
        for inst in institutions[:6]:
            inst_items.append(
                html.Div(
                    [
                        html.Span(inst.get("name", ""), style={
                            "color": TEXT, "fontSize": "11px", "fontFamily": MONO,
                        }),
                        html.Span(
                            f"  ·  {inst.get('country', '')}  ·  {inst.get('count', '')} theses",
                            style={"color": DIM, "fontSize": "9px", "fontFamily": MONO},
                        ),
                        html.Div(
                            inst.get("focus", ""),
                            style={"color": DIM, "fontSize": "9px", "fontFamily": MONO, "marginTop": "2px"},
                        ),
                    ],
                    style={
                        "borderBottom": f"1px solid {BORDER}",
                        "padding": "6px 0",
                    },
                )
            )
        children.append(html.Div(inst_items, style={"marginBottom": "16px"}))

    # ── Emerging methods ───────────────────────────────────────────────────
    if emerging:
        children.append(
            html.Div("EMERGING METHODS", style={
                "fontSize": "9px", "color": DIM, "letterSpacing": "2px",
                "fontFamily": MONO, "marginBottom": "8px",
            })
        )
        badges = []
        for method in emerging:
            badges.append(
                html.Span(
                    method,
                    style={
                        "display": "inline-block",
                        "background": CARD2, "border": f"1px solid {TEAL}55",
                        "color": TEAL, "fontSize": "9px", "fontFamily": MONO,
                        "padding": "2px 8px", "borderRadius": "10px",
                        "margin": "2px 4px 2px 0",
                    },
                )
            )
        children.append(html.Div(badges, style={"marginBottom": "16px"}))

    # ── Patent proximity ───────────────────────────────────────────────────
    if proximity:
        children.append(
            html.Div(
                [
                    html.Div("PATENT PROXIMITY SIGNAL", style={
                        "fontSize": "9px", "color": DIM, "letterSpacing": "2px",
                        "fontFamily": MONO, "marginBottom": "6px",
                    }),
                    html.Div(proximity, style={
                        "color": TEXT, "fontSize": "11px", "fontFamily": MONO,
                        "lineHeight": "1.6",
                        "borderLeft": f"3px solid {TEAL}",
                        "paddingLeft": "10px",
                    }),
                ],
                style={"marginBottom": "16px"},
            )
        )

    # ── Breakout research ──────────────────────────────────────────────────
    if breakout:
        children.append(
            html.Div("BREAKOUT RESEARCH", style={
                "fontSize": "9px", "color": DIM, "letterSpacing": "2px",
                "fontFamily": MONO, "marginBottom": "8px",
            })
        )
        for b in breakout[:4]:
            pot = b.get("commercialization_potential", "medium")
            pot_color = {"high": GREEN, "medium": AMBER, "low": DIM}.get(pot, DIM)
            children.append(
                html.Div(
                    [
                        html.Div(
                            [
                                html.Span(b.get("title", "")[:80], style={
                                    "color": TEXT, "fontSize": "11px", "fontFamily": MONO,
                                    "fontWeight": "600",
                                }),
                                html.Span(
                                    f"  ● {pot.upper()} COMMERCIAL POTENTIAL",
                                    style={"color": pot_color, "fontSize": "8px", "fontFamily": MONO},
                                ),
                            ],
                            style={"marginBottom": "3px"},
                        ),
                        html.Div(
                            f"{b.get('author', '')}  ·  {b.get('institution', '')}",
                            style={"color": DIM, "fontSize": "9px", "fontFamily": MONO, "marginBottom": "3px"},
                        ),
                        html.Div(
                            b.get("why_notable", ""),
                            style={"color": DIM, "fontSize": "10px", "fontFamily": MONO, "lineHeight": "1.4"},
                        ),
                    ],
                    style={
                        "background": CARD2, "border": f"1px solid {BORDER}",
                        "borderLeft": f"3px solid {pot_color}",
                        "padding": "8px 12px", "borderRadius": "3px",
                        "marginBottom": "8px",
                    },
                )
            )

    if not children:
        return [
            html.Div(
                "No thesis analysis available yet. Run: python main.py run-theses",
                style={"color": DIM, "fontSize": "11px", "fontFamily": MONO, "padding": "20px 0"},
            )
        ]
    return children


def _link_pill(label: str, href: str, color: str = DIM) -> html.A:
    """Small clickable pill link."""
    return html.A(
        label,
        href=href,
        target="_blank",
        style={
            "display": "inline-block",
            "background": CARD2,
            "border": f"1px solid {color}55",
            "color": color,
            "fontSize": "8px",
            "fontFamily": MONO,
            "padding": "1px 7px",
            "borderRadius": "10px",
            "marginRight": "5px",
            "textDecoration": "none",
            "letterSpacing": "0.5px",
        },
    )


_PAGE_SIZE = 10


def _btn_style(disabled: bool) -> dict:
    return {
        "background": "none",
        "border": f"1px solid {BORDER if disabled else AMBER}",
        "color": DIM if disabled else AMBER,
        "fontFamily": MONO, "fontSize": "9px",
        "padding": "3px 12px",
        "cursor": "default" if disabled else "pointer",
        "borderRadius": "2px", "letterSpacing": "1px",
        "opacity": "0.4" if disabled else "1",
    }


def _pagination_props(page: int, total: int):
    """Return (label_text, prev_disabled, next_disabled)."""
    total_pages = max(1, -(-total // _PAGE_SIZE))
    start = page * _PAGE_SIZE + 1
    end = min((page + 1) * _PAGE_SIZE, total)
    label = f"  {start}–{end} of {total}  ·  page {page + 1}/{total_pages}  "
    return label, page == 0, page >= total_pages - 1


def _render_search_results(rows: list[dict]) -> list:
    if not rows:
        return [html.Div("No results.", style={"color": DIM, "fontFamily": MONO, "fontSize": "11px", "padding": "10px 0"})]

    items = []
    for r in rows:
        url        = r.get("url", "")
        type_label = r.get("type", "")
        type_color = TEAL if "HW" in type_label else (BLUE if "SW" in type_label else DIM)

        # ── Title with thesis link ─────────────────────────────────────────
        title_el = (
            html.A(r["title"], href=url, target="_blank", style={
                "color": AMBER, "fontFamily": MONO, "fontSize": "12px",
                "fontWeight": "600", "textDecoration": "none",
            })
            if url else
            html.Span(r["title"], style={
                "color": AMBER, "fontFamily": MONO, "fontSize": "12px", "fontWeight": "600",
            })
        )

        # ── Author profile link pills ──────────────────────────────────────
        author_links = []
        if r.get("author_orcid"):
            author_links.append(_link_pill("ORCID", r["author_orcid"], TEAL))
        if r.get("author_openalex_url"):
            author_links.append(_link_pill("OpenAlex", r["author_openalex_url"], BLUE))
        if r.get("google_scholar_url"):
            author_links.append(_link_pill("Google Scholar", r["google_scholar_url"], GREEN))
        if r.get("linkedin_url"):
            author_links.append(_link_pill("LinkedIn", r["linkedin_url"], "#0077b5"))

        items.append(
            html.Div(
                [
                    # Title row
                    html.Div(title_el, style={"marginBottom": "3px"}),

                    # Metadata row
                    html.Div(
                        [
                            html.Span(str(r.get("year", "—")), style={"color": DIM, "fontSize": "9px", "fontFamily": MONO}),
                            html.Span("  ·  ", style={"color": BORDER}),
                            # Author name: ORCID link if available, plain text otherwise
                            html.A(r.get("author", "—"), href=r["author_orcid"], target="_blank", style={
                                "color": TEAL, "fontSize": "9px", "fontFamily": MONO, "textDecoration": "none",
                            }) if r.get("author_orcid") else
                            html.Span(r.get("author", "—"), style={"color": TEXT, "fontSize": "9px", "fontFamily": MONO}),
                            html.Span("  ·  ", style={"color": BORDER}),
                            html.Span(r.get("institution", "—"), style={"color": DIM, "fontSize": "9px", "fontFamily": MONO}),
                            html.Span("  ·  ", style={"color": BORDER}),
                            html.Span(r.get("country", "—"), style={"color": DIM, "fontSize": "9px", "fontFamily": MONO}),
                            html.Span("  ·  ", style={"color": BORDER}),
                            html.Span(type_label, style={"color": type_color, "fontSize": "9px", "fontFamily": MONO}),
                            html.Span("  ·  ", style={"color": BORDER}),
                            html.Span(r.get("source", ""), style={"color": PURPLE, "fontSize": "9px", "fontFamily": MONO}),
                        ],
                        style={"margin": "2px 0 5px"},
                    ),

                    # Author profile links row (shown only when links exist)
                    html.Div(author_links, style={"marginBottom": "5px"}) if author_links else html.Span(),

                    # Abstract
                    html.Div(
                        r.get("abstract", ""),
                        style={"color": DIM, "fontSize": "10px", "fontFamily": MONO, "lineHeight": "1.5"},
                    ),
                ],
                style={
                    "borderBottom": f"1px solid {BORDER}",
                    "padding": "10px 0",
                },
            )
        )
    return items


# ── Charts ─────────────────────────────────────────────────────────────────────

def _build_year_chart() -> go.Figure:
    rows = _theses_by_year()
    if not rows:
        fig = go.Figure()
        fig.update_layout(**_chart_layout(title="Theses by Year"), **GRID)
        return fig

    years = sorted({r["year"] for r in rows})
    hw_counts = {r["year"]: r["count"] for r in rows if r["hw"]}
    sw_counts = {r["year"]: r["count"] for r in rows if not r["hw"]}

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=years, y=[hw_counts.get(y, 0) for y in years],
        name="Hardware", marker_color=AMBER, opacity=0.85,
    ))
    fig.add_trace(go.Bar(
        x=years, y=[sw_counts.get(y, 0) for y in years],
        name="Software", marker_color=TEAL, opacity=0.85,
    ))
    fig.update_layout(
        barmode="stack",
        **_chart_layout(
            title="Theses by Year",
            legend=dict(orientation="h", y=1.1, x=0, font=dict(color=DIM, size=9, family=MONO)),
            xaxis=dict(color=DIM, **GRID),
            yaxis=dict(color=DIM, **GRID),
        ),
    )
    return fig


def _build_country_chart() -> go.Figure:
    rows = _theses_by_country(15)
    if not rows:
        fig = go.Figure()
        fig.update_layout(**_chart_layout(title="Top Research Countries"))
        return fig

    countries, counts = zip(*rows)
    fig = go.Figure(go.Bar(
        x=list(counts), y=list(countries),
        orientation="h",
        marker_color=BLUE, opacity=0.85,
        text=list(counts), textposition="outside",
        textfont=dict(color=DIM, size=9, family=MONO),
    ))
    fig.update_layout(
        **_chart_layout(
            title="Top Research Countries",
            xaxis=dict(color=DIM, **GRID),
            yaxis=dict(color=DIM, autorange="reversed"),
            margin=dict(l=60, r=30, t=36, b=10),
        )
    )
    return fig


def _build_source_chart() -> go.Figure:
    rows = _theses_by_source()
    if not rows:
        fig = go.Figure()
        fig.update_layout(**_chart_layout(title="By Source"))
        return fig

    sources, counts = zip(*rows)
    colors = [SOURCE_COLORS.get(s, DIM) for s in sources]
    fig = go.Figure(go.Bar(
        x=list(sources), y=list(counts),
        marker_color=colors, opacity=0.9,
        text=list(counts), textposition="outside",
        textfont=dict(color=DIM, size=10, family=MONO),
    ))
    fig.update_layout(
        **_chart_layout(
            title="Records by Source",
            xaxis=dict(color=DIM, **GRID),
            yaxis=dict(color=DIM, **GRID),
        )
    )
    return fig


def _build_institutions_chart() -> go.Figure:
    rows = _top_institutions(12)
    if not rows:
        fig = go.Figure()
        fig.update_layout(**_chart_layout(title="Top Institutions"))
        return fig

    names, counts = zip(*rows)
    fig = go.Figure(go.Bar(
        x=list(counts), y=list(names),
        orientation="h",
        marker_color=PURPLE, opacity=0.85,
        text=list(counts), textposition="outside",
        textfont=dict(color=DIM, size=9, family=MONO),
    ))
    fig.update_layout(
        **_chart_layout(
            title="Top Institutions",
            xaxis=dict(color=DIM, **GRID),
            yaxis=dict(color=DIM, autorange="reversed", tickfont=dict(size=9)),
            margin=dict(l=180, r=30, t=36, b=10),
        )
    )
    return fig


# ── App layout ────────────────────────────────────────────────────────────────

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    title="Research Intelligence Terminal",
    assets_folder="assets",
    suppress_callback_exceptions=True,
)


def _layout() -> html.Div:
    kpis = _thesis_kpis()

    return html.Div(
        style={"backgroundColor": BG, "minHeight": "100vh", "padding": "0 20px 40px"},
        children=[
            dcc.Interval(id="thesis-refresh", interval=5 * 60 * 1000, n_intervals=0),
            dcc.Store(id="thesis-search-store"),
            dcc.Store(id="thesis-results-store", data=[]),
            dcc.Store(id="thesis-search-page", data=0),

            # ── Header ─────────────────────────────────────────────────────
            html.Div(
                [
                    html.Div(
                        [
                            html.Span("◈ ", style={"color": AMBER, "fontSize": "18px"}),
                            html.Span("RESEARCH INTELLIGENCE TERMINAL", style={
                                "color": AMBER, "fontSize": "14px", "fontWeight": "700",
                                "fontFamily": MONO, "letterSpacing": "4px",
                            }),
                            html.Span("  —  PhD & Academic Literature", style={
                                "color": DIM, "fontSize": "10px",
                                "fontFamily": MONO, "letterSpacing": "2px",
                            }),
                        ],
                        style={"display": "inline-block"},
                    ),
                    html.Div(
                        [
                            html.A("◀ Patent Dashboard", href="https://nia-patent.railway.up.app", style={
                                "color": DIM, "fontSize": "9px", "fontFamily": MONO,
                                "textDecoration": "none", "letterSpacing": "1px",
                            }),
                            html.Span("  ·  ", style={"color": BORDER}),
                            html.Span(
                                f"Last analysis: {kpis['last_analysis']}",
                                style={"color": DIM, "fontSize": "9px", "fontFamily": MONO},
                            ),
                        ],
                        style={"float": "right", "paddingTop": "4px"},
                    ),
                ],
                style={
                    "borderBottom": f"1px solid {BORDER}",
                    "padding": "14px 0 12px",
                    "marginBottom": "16px",
                },
            ),

            # ── KPIs ────────────────────────────────────────────────────────
            dbc.Row(
                [
                    _kpi_card("TOTAL THESES", f"{kpis['total']:,}"),
                    _kpi_card("HARDWARE-RELEVANT", f"{kpis['hw']:,}", TEAL),
                    _kpi_card("SOFTWARE-RELEVANT", f"{kpis['sw']:,}", BLUE),
                    _kpi_card("DATA SOURCES", str(kpis["sources"]), PURPLE),
                    _kpi_card("YEAR RANGE", f"{kpis['min_yr']}–{kpis['max_yr']}", DIM),
                ],
                className="g-2",
                style={"marginBottom": "16px"},
            ),

            # ── Charts row 1: Year + Country ─────────────────────────────
            dbc.Row(
                [
                    dbc.Col(
                        html.Div(
                            dcc.Graph(
                                id="thesis-year-chart",
                                figure=_build_year_chart(),
                                config={"displayModeBar": False},
                                style={"height": "260px"},
                            ),
                            style={"background": CARD, "border": f"1px solid {BORDER}", "borderRadius": "4px", "padding": "8px"},
                        ),
                        width=7,
                    ),
                    dbc.Col(
                        html.Div(
                            dcc.Graph(
                                id="thesis-country-chart",
                                figure=_build_country_chart(),
                                config={"displayModeBar": False},
                                style={"height": "260px"},
                            ),
                            style={"background": CARD, "border": f"1px solid {BORDER}", "borderRadius": "4px", "padding": "8px"},
                        ),
                        width=5,
                    ),
                ],
                className="g-2",
                style={"marginBottom": "12px"},
            ),

            # ── Charts row 2: Source + Institutions ──────────────────────
            dbc.Row(
                [
                    dbc.Col(
                        html.Div(
                            dcc.Graph(
                                id="thesis-source-chart",
                                figure=_build_source_chart(),
                                config={"displayModeBar": False},
                                style={"height": "220px"},
                            ),
                            style={"background": CARD, "border": f"1px solid {BORDER}", "borderRadius": "4px", "padding": "8px"},
                        ),
                        width=4,
                    ),
                    dbc.Col(
                        html.Div(
                            dcc.Graph(
                                id="thesis-inst-chart",
                                figure=_build_institutions_chart(),
                                config={"displayModeBar": False},
                                style={"height": "220px"},
                            ),
                            style={"background": CARD, "border": f"1px solid {BORDER}", "borderRadius": "4px", "padding": "8px"},
                        ),
                        width=8,
                    ),
                ],
                className="g-2",
                style={"marginBottom": "20px"},
            ),

            # ── AI Research Landscape ─────────────────────────────────────
            html.Div(
                [
                    _section_header("AI RESEARCH LANDSCAPE ANALYSIS"),
                    html.Div(
                        id="thesis-analysis-panel",
                        children=_render_thesis_analysis(_latest_thesis_analysis() or {}),
                    ),
                ],
                style={
                    "background": CARD, "border": f"1px solid {BORDER}",
                    "borderTop": f"2px solid {AMBER}",
                    "padding": "16px 20px", "borderRadius": "4px",
                    "marginBottom": "20px",
                },
            ),

            # ── Thesis Search ─────────────────────────────────────────────
            html.Div(
                [
                    _section_header("THESIS SEARCH"),
                    dbc.Row(
                        [
                            dbc.Col(
                                dcc.Input(
                                    id="thesis-search-input",
                                    placeholder="Search title, abstract, author, institution...",
                                    debounce=False,
                                    style={
                                        "width": "100%", "background": CARD2,
                                        "border": f"1px solid {BORDER}", "color": TEXT,
                                        "fontFamily": MONO, "fontSize": "11px",
                                        "padding": "8px 12px", "borderRadius": "3px",
                                    },
                                ),
                                width=4,
                            ),
                            dbc.Col(
                                dcc.Input(
                                    id="thesis-year-from",
                                    placeholder="Year from",
                                    type="number",
                                    min=2000, max=2030, step=1,
                                    style={
                                        "width": "100%", "background": CARD2,
                                        "border": f"1px solid {BORDER}", "color": TEXT,
                                        "fontFamily": MONO, "fontSize": "11px",
                                        "padding": "8px 12px", "borderRadius": "3px",
                                    },
                                ),
                                width=1,
                            ),
                            dbc.Col(
                                dcc.Input(
                                    id="thesis-year-to",
                                    placeholder="Year to",
                                    type="number",
                                    min=2000, max=2030, step=1,
                                    style={
                                        "width": "100%", "background": CARD2,
                                        "border": f"1px solid {BORDER}", "color": TEXT,
                                        "fontFamily": MONO, "fontSize": "11px",
                                        "padding": "8px 12px", "borderRadius": "3px",
                                    },
                                ),
                                width=1,
                            ),
                            dbc.Col(
                                dcc.Dropdown(
                                    id="thesis-relevance-filter",
                                    options=[
                                        {"label": "All", "value": "all"},
                                        {"label": "Hardware", "value": "hardware"},
                                        {"label": "Software", "value": "software"},
                                    ],
                                    value="all",
                                    clearable=False,
                                    style={"fontFamily": MONO, "fontSize": "11px"},
                                ),
                                width=2,
                            ),
                            dbc.Col(
                                dcc.Dropdown(
                                    id="thesis-source-filter",
                                    options=[{"label": "All Sources", "value": "all"}] + [
                                        {"label": s, "value": s} for s in _all_sources()
                                    ],
                                    value="all",
                                    clearable=False,
                                    style={"fontFamily": MONO, "fontSize": "11px"},
                                ),
                                width=2,
                            ),
                            dbc.Col(
                                html.Button(
                                    "SEARCH",
                                    id="thesis-search-btn",
                                    style={
                                        "background": AMBER, "color": BG,
                                        "border": "none", "padding": "8px 20px",
                                        "fontFamily": MONO, "fontSize": "10px",
                                        "fontWeight": "700", "letterSpacing": "2px",
                                        "cursor": "pointer", "borderRadius": "3px",
                                        "width": "100%",
                                    },
                                ),
                                width=2,
                            ),
                        ],
                        className="g-2",
                        style={"marginBottom": "14px"},
                    ),
                    html.Div(id="thesis-search-results"),
                    html.Div(
                        [
                            html.Button(
                                "← PREV", id="thesis-prev-btn", n_clicks=0,
                                style={
                                    "background": "none", "border": f"1px solid {BORDER}",
                                    "color": DIM, "fontFamily": MONO,
                                    "fontSize": "9px", "padding": "3px 12px",
                                    "cursor": "default", "borderRadius": "2px",
                                    "letterSpacing": "1px", "opacity": "0.4",
                                },
                            ),
                            html.Span(
                                id="thesis-page-label",
                                style={"color": DIM, "fontSize": "9px",
                                       "fontFamily": MONO, "padding": "0 10px",
                                       "verticalAlign": "middle"},
                            ),
                            html.Button(
                                "NEXT →", id="thesis-next-btn", n_clicks=0,
                                style={
                                    "background": "none", "border": f"1px solid {BORDER}",
                                    "color": DIM, "fontFamily": MONO,
                                    "fontSize": "9px", "padding": "3px 12px",
                                    "cursor": "default", "borderRadius": "2px",
                                    "letterSpacing": "1px", "opacity": "0.4",
                                },
                            ),
                        ],
                        id="thesis-search-pagination",
                        style={"display": "none", "marginTop": "12px",
                               "paddingTop": "10px", "borderTop": f"1px solid {BORDER}",
                               "textAlign": "center"},
                    ),
                ],
                style={
                    "background": CARD, "border": f"1px solid {BORDER}",
                    "borderTop": f"2px solid {TEAL}",
                    "padding": "16px 20px", "borderRadius": "4px",
                    "marginBottom": "20px",
                },
            ),

            # ── Thesis Browser ────────────────────────────────────────────
            html.Div(
                [
                    _section_header("THESIS BROWSER"),
                    dbc.Row(
                        [
                            dbc.Col(
                                dcc.Dropdown(
                                    id="browser-relevance",
                                    options=[
                                        {"label": "All Relevance", "value": "all"},
                                        {"label": "Hardware", "value": "hardware"},
                                        {"label": "Software", "value": "software"},
                                    ],
                                    value="all",
                                    clearable=False,
                                    style={"fontFamily": MONO, "fontSize": "11px"},
                                ),
                                width=2,
                            ),
                            dbc.Col(
                                dcc.Dropdown(
                                    id="browser-source",
                                    options=[{"label": "All Sources", "value": "all"}] + [
                                        {"label": s, "value": s} for s in _all_sources()
                                    ],
                                    value="all",
                                    clearable=False,
                                    style={"fontFamily": MONO, "fontSize": "11px"},
                                ),
                                width=2,
                            ),
                        ],
                        className="g-2",
                        style={"marginBottom": "10px"},
                    ),
                    html.Div(id="thesis-browser-table"),
                ],
                style={
                    "background": CARD, "border": f"1px solid {BORDER}",
                    "borderTop": f"2px solid {PURPLE}",
                    "padding": "16px 20px", "borderRadius": "4px",
                },
            ),
        ],
    )


app.layout = _layout


# ── Callbacks ─────────────────────────────────────────────────────────────────

@app.callback(
    Output("thesis-analysis-panel", "children"),
    Output("thesis-year-chart", "figure"),
    Output("thesis-country-chart", "figure"),
    Output("thesis-source-chart", "figure"),
    Output("thesis-inst-chart", "figure"),
    Input("thesis-refresh", "n_intervals"),
)
def refresh_all(_n):
    analysis_data = _latest_thesis_analysis() or {}
    return (
        _render_thesis_analysis(analysis_data),
        _build_year_chart(),
        _build_country_chart(),
        _build_source_chart(),
        _build_institutions_chart(),
    )


@app.callback(
    Output("thesis-search-store", "data"),
    Input({"type": "cluster-click", "q": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def handle_cluster_click(_clicks):
    tid = dash.ctx.triggered_id
    if not tid:
        return dash.no_update
    return {"term": tid["q"]}


@app.callback(
    Output("thesis-search-input", "value"),
    Input("thesis-search-store", "data"),
    prevent_initial_call=True,
)
def populate_search_from_cluster(store_data):
    if not store_data:
        return dash.no_update
    return store_data.get("term", "")


@app.callback(
    Output("thesis-results-store", "data"),
    Output("thesis-search-page", "data"),
    Input("thesis-search-btn", "n_clicks"),
    Input("thesis-search-store", "data"),
    State("thesis-search-input", "value"),
    State("thesis-year-from", "value"),
    State("thesis-year-to", "value"),
    State("thesis-relevance-filter", "value"),
    State("thesis-source-filter", "value"),
    prevent_initial_call=True,
)
def do_search(_btn, store_data, query, year_from, year_to, relevance, source_filter):
    if dash.ctx.triggered_id == "thesis-search-store" and store_data:
        query = store_data.get("term", query or "")

    rows = _search_theses(
        query=query or "",
        year_from=year_from,
        year_to=year_to,
        relevance=relevance or "all",
        source_filter=source_filter or "all",
        limit=500,
    )
    return rows, 0


@app.callback(
    Output("thesis-search-results", "children"),
    Output("thesis-page-label", "children"),
    Output("thesis-prev-btn", "disabled"),
    Output("thesis-prev-btn", "style"),
    Output("thesis-next-btn", "disabled"),
    Output("thesis-next-btn", "style"),
    Output("thesis-search-pagination", "style"),
    Output("thesis-search-page", "data", allow_duplicate=True),
    Input("thesis-results-store", "data"),
    Input("thesis-prev-btn", "n_clicks"),
    Input("thesis-next-btn", "n_clicks"),
    State("thesis-search-page", "data"),
    prevent_initial_call=True,
)
def render_thesis_page(results, _prev, _next, current_page):
    triggered = dash.ctx.triggered_id
    results = results or []
    total = len(results)
    page = current_page or 0

    if triggered == "thesis-results-store":
        page = 0
    elif triggered == "thesis-prev-btn":
        page = max(0, page - 1)
    elif triggered == "thesis-next-btn":
        total_pages = max(1, -(-total // _PAGE_SIZE))
        page = min(page + 1, total_pages - 1)

    _hidden = {"display": "none", "marginTop": "12px", "paddingTop": "10px",
               "borderTop": f"1px solid {BORDER}", "textAlign": "center"}
    _visible = {**_hidden, "display": "block"}

    if not results:
        return (
            [html.Div("No results.", style={"color": DIM, "fontFamily": MONO,
                                            "fontSize": "11px", "padding": "10px 0"})],
            "", True, _btn_style(True), True, _btn_style(True), _hidden, page,
        )

    start = page * _PAGE_SIZE
    end = start + _PAGE_SIZE
    page_results = results[start:end]

    header = html.Div(
        f"{total} result{'s' if total != 1 else ''}",
        style={"color": DIM, "fontSize": "9px", "fontFamily": MONO, "marginBottom": "6px"},
    )
    label, prev_dis, next_dis = _pagination_props(page, total)

    return (
        [header] + _render_search_results(page_results),
        label,
        prev_dis, _btn_style(prev_dis),
        next_dis, _btn_style(next_dis),
        _visible,
        page,
    )


@app.callback(
    Output("thesis-browser-table", "children"),
    Input("browser-relevance", "value"),
    Input("browser-source", "value"),
    Input("thesis-refresh", "n_intervals"),
)
def refresh_browser(relevance, source_filter, _n):
    rows = _search_theses(
        relevance=relevance or "all",
        source_filter=source_filter or "all",
        limit=200,
    )
    if not rows:
        return html.Div(
            "No theses in database yet. Run: python main.py run-theses",
            style={"color": DIM, "fontFamily": MONO, "fontSize": "11px", "padding": "10px 0"},
        )

    table_data = []
    for r in rows:
        url = r.get("url", "")
        orc = r.get("author_orcid", "")
        gs  = r.get("google_scholar_url", "")
        li  = r.get("linkedin_url", "")

        # Title → markdown link to thesis (plain text if no URL)
        title_md = f"[{r['title'][:80]}]({url})" if url else r["title"][:80]

        # Author → markdown link to ORCID if available, else plain text
        author_name = r["author"]
        author_md = f"[{author_name}]({orc})" if orc else author_name

        # Secondary links column: Google Scholar + LinkedIn
        link_parts = []
        if gs:
            link_parts.append(f"[Scholar]({gs})")
        if li:
            link_parts.append(f"[LinkedIn]({li})")

        table_data.append({
            "Title":       title_md,
            "Author":      author_md,
            "Institution": r["institution"],
            "Country":     r["country"],
            "Year":        str(r["year"]),
            "Type":        r["type"],
            "Source":      r["source"],
            "Links":       "  ·  ".join(link_parts),
        })

    return dash_table.DataTable(
        data=table_data,
        columns=[
            {"name": "Title",       "id": "Title",  "presentation": "markdown"},
            {"name": "Author",      "id": "Author", "presentation": "markdown"},
            {"name": "Institution", "id": "Institution"},
            {"name": "Country",     "id": "Country"},
            {"name": "Year",        "id": "Year"},
            {"name": "Type",        "id": "Type"},
            {"name": "Source",      "id": "Source"},
            {"name": "Links",       "id": "Links",  "presentation": "markdown"},
        ],
        style_table={"overflowX": "auto"},
        style_cell={
            "backgroundColor": CARD2, "color": TEXT,
            "fontFamily": MONO, "fontSize": "10px",
            "border": f"1px solid {BORDER}",
            "padding": "6px 10px", "textAlign": "left",
            "overflow": "hidden", "textOverflow": "ellipsis",
        },
        style_cell_conditional=[
            {"if": {"column_id": "Title"},       "maxWidth": "320px"},
            {"if": {"column_id": "Author"},      "maxWidth": "160px", "whiteSpace": "normal"},
            {"if": {"column_id": "Institution"}, "maxWidth": "180px"},
            {"if": {"column_id": "Links"},       "maxWidth": "140px", "whiteSpace": "normal"},
        ],
        style_header={
            "backgroundColor": CARD, "color": AMBER,
            "fontFamily": MONO, "fontSize": "9px",
            "fontWeight": "700", "letterSpacing": "1px",
            "border": f"1px solid {BORDER}",
        },
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": CARD},
            {"if": {"filter_query": '{Type} contains "HW"'}, "borderLeft": f"3px solid {AMBER}"},
            {"if": {"filter_query": '{Type} contains "SW"'}, "borderLeft": f"3px solid {TEAL}"},
        ],
        page_size=25,
        page_action="native",
        sort_action="native",
        filter_action="native",
        tooltip_data=[
            {"Title": {"value": r["abstract"], "type": "markdown"}} for r in rows
        ],
        tooltip_delay=0,
        tooltip_duration=None,
        markdown_options={"link_target": "_blank"},
    )


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8051)
