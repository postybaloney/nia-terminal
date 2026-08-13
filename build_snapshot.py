"""
Static snapshot generator for the NIA terminal.

Replaces the always-on Dash server (dashboard.py, localhost:8050) with a single
self-contained HTML file that reflects the latest nightly ingest. No server, no
CDN, no runtime JS — inline CSS + inline SVG only, so it hosts for free anywhere
(GitHub Pages, Cloudflare Pages) and opens offline.

Usage:
    python build_snapshot.py                 # read live DB -> site/index.html
    python build_snapshot.py --out docs/index.html
    python build_snapshot.py --demo          # mock data, no DB (preview the design)

Design: matches the existing Bloomberg amber-on-black terminal palette. Every
chart is a single-hue magnitude chart whose bars are directly labelled, so
identity is carried by text, never by colour alone (colourblind-safe by
construction — see the dataviz palette validator).
"""
from __future__ import annotations

import argparse
import html
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

# ── Palette (matches dashboard.py: Bloomberg amber-on-black) ──────────────────
BG = "#050810"
CARD = "#0d1117"
CARD2 = "#111827"
BORDER = "#1f2937"
TEXT = "#e5e7eb"
DIM = "#6b7280"
AMBER = "#f59e0b"
GREEN = "#10b981"
RED = "#ef4444"
BLUE = "#3b82f6"
TEAL = "#14b8a6"
PURPLE = "#8b5cf6"
MONO = "'Courier New', ui-monospace, monospace"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ══════════════════════════════════════════════════════════════════════════════
#  DATA
# ══════════════════════════════════════════════════════════════════════════════

def fetch_from_db() -> dict:
    """Pull the snapshot payload from the live database, mirroring dashboard.py."""
    from sqlalchemy import func

    from db import get_session, init_db
    from db.models import IngestRun, RawPatent
    from db.signal_models import Signal
    from db.thesis_models import Thesis

    try:
        init_db()  # CREATE TABLE IF NOT EXISTS — safe to call repeatedly
    except Exception as e:  # pragma: no cover - defensive
        print(f"[snapshot] init_db skipped: {e}", file=sys.stderr)

    with get_session() as s:
        total_patents = s.query(func.count(RawPatent.id)).scalar() or 0
        total_families = (
            s.query(func.count(func.distinct(RawPatent.family_id))).scalar() or 0
        )
        total_theses = s.query(func.count(Thesis.id)).scalar() or 0
        total_signals = s.query(func.count(Signal.id)).scalar() or 0

        last_run = (
            s.query(IngestRun.started_at, IngestRun.new_patents, IngestRun.success)
            .order_by(IngestRun.started_at.desc())
            .first()
        )

        # New patents per day (last 30 days) from first_seen_at — pipeline activity.
        cutoff = utcnow().replace(tzinfo=None) - timedelta(days=30)
        seen_rows = (
            s.query(RawPatent.first_seen_at)
            .filter(RawPatent.first_seen_at >= cutoff)
            .all()
        )

        # Top assignees (all-time), same extraction as dashboard._top_assignees.
        assignee_rows = (
            s.query(RawPatent.assignees)
            .filter(RawPatent.assignees.isnot(None))
            .all()
        )

        # Signals grouped by type.
        signal_type_rows = (
            s.query(Signal.signal_type, func.count(Signal.id))
            .group_by(Signal.signal_type)
            .all()
        )

        # Theses grouped by year.
        thesis_year_rows = (
            s.query(Thesis.year, func.count(Thesis.id))
            .filter(Thesis.year.isnot(None))
            .group_by(Thesis.year)
            .order_by(Thesis.year)
            .all()
        )

        # Recent items.
        recent_patents = (
            s.query(
                RawPatent.grant_date, RawPatent.filing_date, RawPatent.first_seen_at,
                RawPatent.source, RawPatent.source_id, RawPatent.title,
                RawPatent.assignees,
            )
            .filter(RawPatent.abstract.isnot(None))
            .order_by(RawPatent.first_seen_at.desc())
            .limit(15)
            .all()
        )
        recent_signals = (
            s.query(
                Signal.event_date, Signal.first_seen_at, Signal.signal_type,
                Signal.organization, Signal.title, Signal.amount,
            )
            .order_by(Signal.first_seen_at.desc())
            .limit(15)
            .all()
        )
        recent_theses = (
            s.query(
                Thesis.year, Thesis.author, Thesis.institution, Thesis.title,
                Thesis.first_seen_at,
            )
            .order_by(Thesis.first_seen_at.desc())
            .limit(15)
            .all()
        )

    # ── shape the payload ────────────────────────────────────────────────────
    # patents/day
    day_counts: Counter = Counter()
    for (fs,) in seen_rows:
        if fs:
            day_counts[fs.strftime("%m/%d")] += 1
    # fill last 14 days for a stable axis
    series = []
    today = utcnow()
    for i in range(13, -1, -1):
        d = (today - timedelta(days=i)).strftime("%m/%d")
        series.append({"label": d, "value": day_counts.get(d, 0)})

    # assignees
    counter: Counter = Counter()
    for (assignees,) in assignee_rows:
        if isinstance(assignees, list):
            for a in assignees:
                name = (a.get("name") or "").strip() if isinstance(a, dict) else ""
                if name and name.lower() not in ("", "unknown"):
                    counter[name] += 1
    top_assignees = [{"label": n, "value": c} for n, c in counter.most_common(10)]

    signals_by_type = [
        {"label": (t or "?"), "value": c}
        for t, c in sorted(signal_type_rows, key=lambda r: r[1], reverse=True)
    ]
    theses_by_year = [
        {"label": str(y), "value": c} for y, c in thesis_year_rows
    ]

    def _assignee(row_assignees):
        if row_assignees and isinstance(row_assignees, list) and row_assignees:
            first = row_assignees[0]
            if isinstance(first, dict):
                return (first.get("name") or "")[:38]
        return ""

    patents_tbl = []
    for r in recent_patents:
        d = r.grant_date or r.filing_date or r.first_seen_at
        patents_tbl.append({
            "date": d.strftime("%Y-%m-%d") if d else "",
            "source": (r.source or "").upper(),
            "id": r.source_id or "",
            "title": (r.title or "—")[:64],
            "assignee": _assignee(r.assignees),
        })
    signals_tbl = []
    for r in recent_signals:
        d = r.event_date or r.first_seen_at
        amt = f"${r.amount:,.0f}" if r.amount else ""
        signals_tbl.append({
            "date": d.strftime("%Y-%m-%d") if d else "",
            "type": (r.signal_type or "").upper(),
            "org": (r.organization or "")[:34],
            "title": (r.title or "—")[:52],
            "amount": amt,
        })
    theses_tbl = []
    for r in recent_theses:
        theses_tbl.append({
            "year": str(r.year) if r.year else "",
            "author": (r.author or "")[:26],
            "institution": (r.institution or "")[:30],
            "title": (r.title or "—")[:56],
        })

    return {
        "generated_at": utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "kpis": {
            "families": total_families,
            "patents": total_patents,
            "theses": total_theses,
            "signals": total_signals,
        },
        "last_run_at": last_run.started_at.strftime("%Y-%m-%d %H:%M") if last_run and last_run.started_at else "—",
        "last_run_ok": bool(last_run.success) if last_run else False,
        "series_new_patents": series,
        "top_assignees": top_assignees,
        "signals_by_type": signals_by_type,
        "theses_by_year": theses_by_year,
        "recent_patents": patents_tbl,
        "recent_signals": signals_tbl,
        "recent_theses": theses_tbl,
    }


def demo_data() -> dict:
    """Realistic mock payload — lets you preview the design without a database."""
    import math
    series = []
    today = utcnow()
    for i in range(13, -1, -1):
        d = (today - timedelta(days=i)).strftime("%m/%d")
        # gentle wave so the sample looks alive
        v = int(6 + 5 * abs(math.sin(i / 2.3)) + (i % 3))
        series.append({"label": d, "value": v})
    return {
        "generated_at": utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "kpis": {"families": 1284, "patents": 2117, "theses": 486, "signals": 613},
        "last_run_at": today.strftime("%Y-%m-%d 07:00"),
        "last_run_ok": True,
        "series_new_patents": series,
        "top_assignees": [
            {"label": "Medtronic", "value": 143},
            {"label": "Neuralink", "value": 121},
            {"label": "Synchron", "value": 96},
            {"label": "Boston Scientific", "value": 88},
            {"label": "Blackrock Neurotech", "value": 71},
            {"label": "Precision Neuroscience", "value": 63},
            {"label": "Paradromics", "value": 54},
            {"label": "Cognixion", "value": 41},
            {"label": "Neurable", "value": 33},
            {"label": "Kernel", "value": 28},
        ],
        "signals_by_type": [
            {"label": "grant", "value": 271},
            {"label": "trial", "value": 188},
            {"label": "clearance", "value": 104},
            {"label": "approval", "value": 50},
        ],
        "theses_by_year": [
            {"label": "2021", "value": 61},
            {"label": "2022", "value": 78},
            {"label": "2023", "value": 94},
            {"label": "2024", "value": 121},
            {"label": "2025", "value": 108},
            {"label": "2026", "value": 24},
        ],
        "recent_patents": [
            {"date": "2026-08-12", "source": "EPO", "id": "EP4123456", "title": "Closed-loop deep brain stimulation with adaptive biomarker tracking", "assignee": "Medtronic"},
            {"date": "2026-08-12", "source": "PATENTSVIEW", "id": "US12034567", "title": "Flexible intracortical microelectrode array and method", "assignee": "Neuralink"},
            {"date": "2026-08-11", "source": "EPO", "id": "EP4119872", "title": "Endovascular neural interface delivery system", "assignee": "Synchron"},
            {"date": "2026-08-11", "source": "PATENTSVIEW", "id": "US12029981", "title": "Thin-film cortical electrode with integrated amplifier", "assignee": "Precision Neuroscience"},
            {"date": "2026-08-10", "source": "EPO", "id": "EP4115003", "title": "Wireless power and telemetry for implantable neurostimulators", "assignee": "Boston Scientific"},
            {"date": "2026-08-10", "source": "PATENTSVIEW", "id": "US12022114", "title": "High-density feedthrough for implantable recording devices", "assignee": "Blackrock Neurotech"},
            {"date": "2026-08-09", "source": "EPO", "id": "EP4110778", "title": "Speech decoding from motor cortex signals", "assignee": "Paradromics"},
        ],
        "recent_signals": [
            {"date": "2026-08-12", "type": "GRANT", "org": "UCSF", "title": "BRAIN Initiative: speech neuroprosthesis scale-up", "amount": "$3,200,000"},
            {"date": "2026-08-12", "type": "TRIAL", "org": "Synchron", "title": "COMMAND early feasibility — endovascular BCI", "amount": ""},
            {"date": "2026-08-11", "type": "CLEARANCE", "org": "Blackrock Neurotech", "title": "510(k) — cortical recording system", "amount": ""},
            {"date": "2026-08-11", "type": "GRANT", "org": "Johns Hopkins APL", "title": "Sensory feedback for upper-limb neuroprosthetics", "amount": "$1,750,000"},
            {"date": "2026-08-10", "type": "APPROVAL", "org": "Medtronic", "title": "PMA supplement — adaptive DBS algorithm", "amount": ""},
            {"date": "2026-08-10", "type": "TRIAL", "org": "Precision Neuroscience", "title": "Layer 7 cortical interface — first-in-human", "amount": ""},
        ],
        "recent_theses": [
            {"year": "2026", "author": "R. Delgado", "institution": "Stanford University", "title": "Adaptive decoding for chronic intracortical BCIs"},
            {"year": "2026", "author": "M. Chen", "institution": "ETH Zurich", "title": "Flexible electronics for conformal neural interfaces"},
            {"year": "2025", "author": "A. Okafor", "institution": "Imperial College London", "title": "Closed-loop stimulation for treatment-resistant depression"},
            {"year": "2025", "author": "S. Virtanen", "institution": "KU Leuven", "title": "Low-power spike-sorting ASICs for implantable recording"},
            {"year": "2025", "author": "L. Moreau", "institution": "EPFL", "title": "Endovascular electrode arrays: modelling and validation"},
        ],
    }


# ══════════════════════════════════════════════════════════════════════════════
#  SVG CHARTS  (single-hue, direct-labelled → colourblind-safe by construction)
# ══════════════════════════════════════════════════════════════════════════════

def _esc(s) -> str:
    return html.escape(str(s), quote=True)


def svg_vbars(series: list[dict], *, height: int = 190, hue: str = AMBER) -> str:
    """Vertical magnitude bars over time. Single hue; recessive grid; native tooltips."""
    if not series:
        return _empty_chart(height)
    n = len(series)
    W, H = 640, height
    pad_l, pad_r, pad_t, pad_b = 34, 12, 14, 26
    plot_w, plot_h = W - pad_l - pad_r, H - pad_t - pad_b
    vmax = max((d["value"] for d in series), default=0) or 1
    slot = plot_w / n
    bw = min(slot * 0.62, 30)
    parts = [f'<svg viewBox="0 0 {W} {H}" width="100%" preserveAspectRatio="xMidYMid meet" role="img" aria-label="New patents per day">']
    # gridlines + y ticks (3 lines)
    for k in range(4):
        gy = pad_t + plot_h * k / 3
        val = round(vmax * (3 - k) / 3)
        parts.append(f'<line x1="{pad_l}" y1="{gy:.1f}" x2="{W-pad_r}" y2="{gy:.1f}" stroke="{BORDER}" stroke-width="1"/>')
        parts.append(f'<text x="{pad_l-6}" y="{gy+3:.1f}" fill="{DIM}" font-size="9" text-anchor="end" font-family="{MONO}">{val}</text>')
    # bars
    for i, d in enumerate(series):
        x = pad_l + slot * i + (slot - bw) / 2
        bh = plot_h * (d["value"] / vmax)
        y = pad_t + plot_h - bh
        r = min(4, bw / 2)
        parts.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{max(bh,0.1):.1f}" rx="{r}" fill="{hue}">'
            f'<title>{_esc(d["label"])}: {d["value"]}</title></rect>'
        )
        if i % 2 == 0:  # label every other day to avoid collisions
            parts.append(f'<text x="{x+bw/2:.1f}" y="{H-8}" fill="{DIM}" font-size="9" text-anchor="middle" font-family="{MONO}">{_esc(d["label"])}</text>')
    parts.append("</svg>")
    return "".join(parts)


def svg_hbars(rows: list[dict], *, hue: str = AMBER, row_h: int = 22, max_rows: int = 10) -> str:
    """Horizontal ranking bars. Category label left, value right — identity is textual."""
    rows = rows[:max_rows]
    if not rows:
        return _empty_chart(120)
    W = 640
    label_w, val_w, pad_t = 150, 40, 6
    plot_w = W - label_w - val_w
    vmax = max((d["value"] for d in rows), default=0) or 1
    H = pad_t * 2 + row_h * len(rows)
    parts = [f'<svg viewBox="0 0 {W} {H}" width="100%" preserveAspectRatio="xMidYMid meet" role="img" aria-label="ranking">']
    for i, d in enumerate(rows):
        y = pad_t + i * row_h
        bw = plot_w * (d["value"] / vmax)
        cy = y + row_h / 2
        parts.append(f'<text x="0" y="{cy+3:.1f}" fill="{TEXT}" font-size="11" font-family="{MONO}">{_esc(d["label"][:20])}</text>')
        parts.append(
            f'<rect x="{label_w}" y="{y+3:.1f}" width="{max(bw,1):.1f}" height="{row_h-8}" rx="3" fill="{hue}">'
            f'<title>{_esc(d["label"])}: {d["value"]}</title></rect>'
        )
        parts.append(f'<text x="{W}" y="{cy+3:.1f}" fill="{DIM}" font-size="10" text-anchor="end" font-family="{MONO}">{d["value"]}</text>')
    parts.append("</svg>")
    return "".join(parts)


def _empty_chart(height: int) -> str:
    return (
        f'<svg viewBox="0 0 640 {height}" width="100%" role="img" aria-label="no data">'
        f'<text x="320" y="{height//2}" fill="{DIM}" font-size="12" text-anchor="middle" '
        f'font-family="{MONO}">— no data yet —</text></svg>'
    )


# ══════════════════════════════════════════════════════════════════════════════
#  HTML
# ══════════════════════════════════════════════════════════════════════════════

def _kpi_tile(label: str, value, accent: str) -> str:
    return f"""      <div class="kpi">
        <div class="kpi-label">{_esc(label)}</div>
        <div class="kpi-value" style="color:{accent}">{_esc(f'{value:,}' if isinstance(value,int) else value)}</div>
      </div>"""


def _table(headers: list[str], rows: list[list], aligns: list[str] | None = None) -> str:
    aligns = aligns or ["left"] * len(headers)
    thead = "".join(f'<th style="text-align:{a}">{_esc(h)}</th>' for h, a in zip(headers, aligns))
    body = []
    if not rows:
        body.append(f'<tr><td colspan="{len(headers)}" class="empty">— no rows yet —</td></tr>')
    for r in rows:
        tds = "".join(f'<td style="text-align:{a}">{_esc(c)}</td>' for c, a in zip(r, aligns))
        body.append(f"<tr>{tds}</tr>")
    return f'<table><thead><tr>{thead}</tr></thead><tbody>{"".join(body)}</tbody></table>'


def _card(title: str, accent: str, inner: str) -> str:
    return f"""    <section class="card">
      <div class="card-hd"><span class="dot" style="background:{accent}"></span>{_esc(title)}</div>
      {inner}
    </section>"""


def render_html(d: dict) -> str:
    k = d["kpis"]
    status_color = GREEN if d.get("last_run_ok") else RED
    status_word = "OPERATIONAL" if d.get("last_run_ok") else "CHECK LOGS"

    kpis = "\n".join([
        _kpi_tile("PATENT FAMILIES", k["families"], AMBER),
        _kpi_tile("RAW PATENTS", k["patents"], TEAL),
        _kpi_tile("PhD THESES", k["theses"], BLUE),
        _kpi_tile("CURRENT SIGNALS", k["signals"], PURPLE),
    ])

    patents_rows = [[r["date"], r["source"], r["id"], r["title"], r["assignee"]] for r in d["recent_patents"]]
    signals_rows = [[r["date"], r["type"], r["org"], r["title"], r["amount"]] for r in d["recent_signals"]]
    theses_rows = [[r["year"], r["author"], r["institution"], r["title"]] for r in d["recent_theses"]]

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NIA Terminal — Snapshot {_esc(d['generated_at'])}</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ margin:0; background:{BG}; color:{TEXT}; font-family:{MONO};
         -webkit-font-smoothing:antialiased; padding:22px; }}
  a {{ color:{AMBER}; text-decoration:none; }}
  .wrap {{ max-width:1200px; margin:0 auto; }}
  header {{ display:flex; align-items:baseline; justify-content:space-between;
            gap:16px; flex-wrap:wrap; border-bottom:1px solid {BORDER}; padding-bottom:14px; }}
  .brand {{ font-size:20px; letter-spacing:3px; color:{TEXT}; }}
  .brand b {{ color:{AMBER}; font-weight:700; }}
  .sub {{ color:{DIM}; font-size:11px; letter-spacing:2px; margin-top:4px; }}
  .status {{ font-size:11px; letter-spacing:1px; color:{DIM}; text-align:right; }}
  .status .live {{ color:{status_color}; }}
  .pill {{ display:inline-block; width:8px; height:8px; border-radius:50%;
           background:{status_color}; margin-right:6px; box-shadow:0 0 8px {status_color}; }}
  .kpis {{ display:grid; grid-template-columns:repeat(4,1fr); gap:14px; margin:18px 0; }}
  .kpi {{ background:{CARD}; border:1px solid {BORDER}; border-radius:10px; padding:16px 18px; }}
  .kpi-label {{ color:{DIM}; font-size:9px; letter-spacing:2px; }}
  .kpi-value {{ font-size:30px; font-weight:700; margin-top:6px; }}
  .grid {{ display:grid; grid-template-columns:1fr 1fr; gap:14px; }}
  .card {{ background:{CARD}; border:1px solid {BORDER}; border-radius:10px; padding:14px 16px; margin-bottom:14px; }}
  .card-hd {{ font-size:11px; letter-spacing:2px; color:{TEXT}; margin-bottom:12px; }}
  .card.full {{ grid-column:1 / -1; }}
  .dot {{ display:inline-block; width:7px; height:7px; border-radius:50%; margin-right:8px; vertical-align:middle; }}
  table {{ width:100%; border-collapse:collapse; font-size:11px; }}
  th {{ color:{DIM}; font-weight:400; letter-spacing:1px; text-transform:uppercase;
        font-size:9px; padding:7px 8px; border-bottom:1px solid {BORDER}; }}
  td {{ padding:7px 8px; border-bottom:1px solid {CARD2}; color:{TEXT}; vertical-align:top; }}
  tbody tr:hover td {{ background:{CARD2}; }}
  .empty {{ color:{DIM}; text-align:center; padding:18px; }}
  footer {{ color:{DIM}; font-size:10px; letter-spacing:1px; margin-top:22px;
            border-top:1px solid {BORDER}; padding-top:12px; line-height:1.7; }}
  @media (max-width:820px) {{ .kpis{{grid-template-columns:repeat(2,1fr)}} .grid{{grid-template-columns:1fr}} }}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div>
      <div class="brand"><b>NIA</b> &middot; NEUROTECH INTELLIGENCE TERMINAL</div>
      <div class="sub">STATIC SNAPSHOT &middot; PATENTS &middot; THESES &middot; SIGNALS</div>
    </div>
    <div class="status">
      <div><span class="pill"></span><span class="live">{status_word}</span></div>
      <div>SNAPSHOT AS OF {_esc(d['generated_at'])}</div>
      <div>LAST INGEST {_esc(d['last_run_at'])}</div>
    </div>
  </header>

  <div class="kpis">
{kpis}
  </div>

  <div class="grid">
{_card("NEW PATENT RECORDS · LAST 14 DAYS", AMBER, svg_vbars(d['series_new_patents']))}
{_card("TOP ASSIGNEES · ALL TIME", TEAL, svg_hbars(d['top_assignees']))}
{_card("CURRENT SIGNALS BY TYPE", PURPLE, svg_hbars(d['signals_by_type']))}
{_card("PhD THESES BY YEAR", BLUE, svg_hbars(d['theses_by_year']))}
  </div>

{_card("LATEST PATENTS", AMBER, _table(["Date","Source","ID","Title","Assignee"], patents_rows))}
{_card("LATEST SIGNALS", PURPLE, _table(["Date","Type","Organization","Title","Amount"], signals_rows, ["left","left","left","left","right"]))}
{_card("LATEST THESES", BLUE, _table(["Year","Author","Institution","Title"], theses_rows))}

  <footer>
    Generated by build_snapshot.py from the NIA PostgreSQL store &middot; regenerated nightly by the GitHub Actions ingest.<br>
    This is a point-in-time static snapshot — no live server. Neurotech Intelligence Agency.
  </footer>
</div>
</body>
</html>"""


# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    ap = argparse.ArgumentParser(description="Build the static NIA terminal snapshot.")
    ap.add_argument("--out", default="site/index.html", help="output HTML path")
    ap.add_argument("--demo", action="store_true", help="use mock data (no DB needed)")
    args = ap.parse_args()

    data = demo_data() if args.demo else fetch_from_db()
    htmldoc = render_html(data)

    import os
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(htmldoc)
    print(f"[snapshot] wrote {args.out}  ({len(htmldoc):,} bytes)")


if __name__ == "__main__":
    main()
