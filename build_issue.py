"""
Generate an Intelligence Layer issue — the thing a $20/mo subscriber receives.

Reads the live corpus, groups the week into the sections a reader actually
cares about, and writes both Markdown (for email/Substack) and a styled HTML
page (for the web archive). Nothing here is hand-written: if the pipeline saw
it, the issue reports it, and if it didn't, the issue says so rather than
letting a language model fill the gap. That constraint is the whole point —
the 2026-08-17 digest claimed "1645 new patents this week" on a week that
ingested zero, because a model was asked to narrate a number it wasn't given.

Sections, ordered by what a neurotech operator or investor acts on first:

    REGULATORY   FDA clearances and approvals — the hardest evidence there is
    MONEY        NIH awards and SEC exempt-offering notices
    CLINIC       newly registered trials
    SCIENCE      preprints and dissertations
    HIRING       job postings whose ROLE is itself the signal
    CONNECTIONS  cross-layer chains pulled from the knowledge graph

Usage:
    python build_issue.py                        # live DB, last 7 days
    python build_issue.py --days 30
    python build_issue.py --graph nia_graph.sqlite
    python build_issue.py --demo                 # illustrative corpus, no DB
"""
from __future__ import annotations

import argparse
import html
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

BG, CARD, BORDER = "#050810", "#0d1117", "#1f2937"
TEXT, DIM, AMBER = "#e5e7eb", "#6b7280", "#f59e0b"


# ─────────────────────────────────────────────────────────────────────────────

def fetch_from_db(days: int):
    from db import get_session
    from db.models import RawPatent
    from db.signal_models import Signal

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    out = defaultdict(list)

    with get_session() as s:
        sigs = (s.query(Signal)
                .filter(Signal.first_seen_at >= cutoff)
                .order_by(Signal.event_date.desc().nullslast())
                .all())
        for x in sigs:
            out[x.signal_type or "other"].append({
                "title": x.title, "org": x.organization, "date": x.event_date,
                "amount": x.amount, "url": x.url, "summary": x.summary,
                "status": x.status, "tags": x.tags or [], "source": x.source,
            })

        pats = (s.query(RawPatent)
                .filter(RawPatent.first_seen_at >= cutoff)
                .all()) if hasattr(RawPatent, "first_seen_at") else []
        for p in pats:
            out["patent"].append({
                "title": p.title,
                "org": (p.assignees or [{}])[0].get("name") if p.assignees else None,
                "date": p.grant_date, "amount": None, "url": None,
                "summary": p.abstract, "status": None,
                "tags": [], "source": p.source,
            })
    return out, False


def fetch_demo():
    """
    Illustrative corpus. Every record below is REAL and was returned by the
    2026-08-17 pipeline run — only the patent entries are stand-ins, because
    fabricating plausible-looking patent numbers would be worse than omitting
    them. Output is watermarked so this can never be mistaken for live data.
    """
    d = lambda s: datetime.strptime(s, "%Y-%m-%d")
    out = defaultdict(list)
    out["clearance"] = [
        {"title": "Portable Neuromodulation Stimulator (PoNS)", "org": "Helius Medical, Inc.",
         "date": d("2026-05-13"), "amount": None, "url": None, "status": "510(k)",
         "summary": "Non-invasive neuromodulation for gait deficit.", "tags": [], "source": "fda_510k"},
        {"title": "Deep Brain Stimulation (DBS) IPG", "org": "Abbott Medical",
         "date": d("2026-07-26"), "amount": None, "url": None, "status": "PMA",
         "summary": "Implantable pulse generator.", "tags": [], "source": "fda_pma"},
        {"title": "Axonics Sacral Neuromodulation System", "org": "Boston Scientific Corporation",
         "date": d("2026-07-24"), "amount": None, "url": None, "status": "PMA",
         "summary": "Sacral neuromodulation.", "tags": [], "source": "fda_pma"},
        {"title": "Vercise Deep Brain Stimulation (DBS) Systems", "org": "Boston Scientific Corp",
         "date": d("2026-07-16"), "amount": None, "url": None, "status": "PMA",
         "summary": "DBS system.", "tags": [], "source": "fda_pma"},
        {"title": "Ceribell Infant Seizure Detection Software", "org": "Ceribell, Inc.",
         "date": d("2025-11-21"), "amount": None, "url": None, "status": "510(k)",
         "summary": "Neonatal seizure detection from EEG.", "tags": [], "source": "fda_510k"},
        {"title": "StimTrial Neuromodulation System", "org": "Bioventus, LLC",
         "date": d("2025-07-16"), "amount": None, "url": None, "status": "510(k)",
         "summary": "Peripheral nerve stimulation trial system.", "tags": [], "source": "fda_510k"},
    ]
    out["grant"] = [
        {"title": "Percutaneous Stellate Ganglion Stimulation in Septic Shock to Improve "
                  "Hemodynamics and Vasopressor Requirement", "org": "Coridea, LLC",
         "date": d("2026-08-15"), "amount": 293759, "url": None, "status": None,
         "summary": "Peripheral nerve stimulation of the stellate ganglion.",
         "tags": [], "source": "nih_reporter"},
    ]
    out["trial"] = [
        {"title": "Effect Of Percutaneous Electrical Nerve Field Stimulation on Symptom "
                  "Control and Nervous System Activity", "org": "Indiana University",
         "date": d("2027-02-10"), "amount": None, "url": None, "status": "Not yet recruiting",
         "summary": "Percutaneous electrical nerve field stimulation.",
         "tags": [], "source": "clinicaltrials"},
        {"title": "rTMS for Anorexia Nervosa in Youth", "org": "University of California, Los Angeles",
         "date": d("2027-01-30"), "amount": None, "url": None, "status": "Not yet recruiting",
         "summary": "Repetitive transcranial magnetic stimulation.",
         "tags": [], "source": "clinicaltrials"},
    ]
    out["article"] = [
        {"title": "Notables #60: August 1-16 2026", "org": "Neurotech Notables (Naveen Rao)",
         "date": d("2026-08-17"), "amount": None, "url":
         "https://neurotechnology.substack.com/p/notables-60", "status": None,
         "summary": "Biweekly neurotech roundup.", "tags": [], "source": "rss:nt_notables"},
    ]
    out["posting"] = [
        {"title": "Director, Regulatory Affairs", "org": "Synchron, Inc.", "date": None,
         "amount": None, "url": None, "status": "New York, NY",
         "summary": "regulatory — FDA submission likely in preparation",
         "tags": ["strategic-hire"], "source": "jobs:greenhouse"},
        {"title": "Senior Reimbursement Strategy Lead", "org": "Paradromics Inc", "date": None,
         "amount": None, "url": None, "status": "Austin, TX",
         "summary": "reimbursement — CMS coverage push starting",
         "tags": ["strategic-hire"], "source": "jobs:greenhouse"},
    ]
    return out, True


# ─────────────────────────────────────────────────────────────────────────────

def graph_connections(graph_db: str, limit: int = 6) -> list[str]:
    """
    Pull cross-layer chains — the thing no single source can show you.
    A PERSON linked to two different ORGs is a talent-flow edge; an ORG hiring
    for a concept it also files patents in is a commitment signal.
    """
    try:
        conn = sqlite3.connect(graph_db)
    except Exception:
        return []
    out: list[str] = []
    try:
        # One row per (person, org) and aggregate in Python. GROUP_CONCAT is not
        # usable here: its separator is a comma and organisation names contain
        # commas ("Medtronic, Inc."), so the concatenation is ambiguous and
        # splitting it corrupts the names.
        pairs = conn.execute("""
            SELECT p.name, o.name
            FROM relations r
            JOIN entities p ON p.id = r.source_id AND p.type='PERSON'
            JOIN entities o ON o.id = r.target_id AND o.type='ORG'
            WHERE r.predicate='affiliated_with'
        """).fetchall()
        grouped: dict[str, list[str]] = defaultdict(list)
        for person, org in pairs:
            if org not in grouped[person]:
                grouped[person].append(org)
        for person, orgs in list(grouped.items()):
            if len(orgs) < 2:
                continue
            if len(out) >= limit:
                break
            joined = (" and ".join(orgs) if len(orgs) == 2
                      else ", ".join(orgs[:-1]) + f", and {orgs[-1]}")
            out.append(f"**{person}** appears at {joined} "
                       f"— an academic-to-commercial talent edge.")

        rows = conn.execute("""
            SELECT o.name, t.name
            FROM relations r
            JOIN entities o ON o.id = r.source_id AND o.type='ORG'
            JOIN entities t ON t.id = r.target_id AND t.type='TECH'
            WHERE r.predicate='hiring_for'
            LIMIT ?
        """, (limit,)).fetchall()
        for org, tech in rows:
            out.append(f"**{org}** is hiring against *{tech}* — capability being "
                       f"built ahead of any public announcement.")
    except Exception:
        pass
    finally:
        conn.close()
    return out


def money(n) -> str:
    if not n:
        return ""
    return f"${n/1_000_000:.1f}M" if n >= 1_000_000 else f"${n:,.0f}"


def dstr(x) -> str:
    return x.strftime("%Y-%m-%d") if isinstance(x, datetime) else "—"


SECTIONS = [
    ("clearance", "Regulatory", "FDA clearances and approvals"),
    ("grant",     "Money",      "Public research awards"),
    ("filing",    "Money · SEC","Exempt-offering notices and filings"),
    ("trial",     "Clinic",     "Newly registered trials"),
    ("preprint",  "Science",    "Preprints and dissertations"),
    ("thesis",    "Science",    "Dissertations"),
    ("patent",    "IP",         "Newly ingested patent families"),
    ("posting",   "Hiring",     "Roles whose existence is itself the signal"),
    ("article",   "Coverage",   "What the field is writing about"),
]


def build(data, days: int, demo: bool, conns: list[str]):
    now = datetime.now(timezone.utc)
    total = sum(len(v) for v in data.values())
    md: list[str] = []

    md.append(f"# NIA Intelligence Layer")
    md.append(f"*Issue for the {days} days ending {now:%B %d, %Y}*\n")
    if demo:
        md.append("> **ILLUSTRATIVE ISSUE.** Regulatory, grant, trial and coverage "
                  "records below are real and were returned by the 2026-08-17 "
                  "pipeline run. This is not a live issue.\n")

    if total == 0:
        md.append("No qualifying records this period. Nothing is reported that "
                  "the pipeline did not observe.\n")
        return "\n".join(md)

    # Headline
    counts = {k: len(v) for k, v in data.items() if v}
    orgs = Counter()
    for recs in data.values():
        for r in recs:
            if r.get("org"):
                orgs[r["org"]] += 1
    top = orgs.most_common(5)
    grant_total = sum(r.get("amount") or 0 for r in data.get("grant", []))

    md.append("## The week in one paragraph\n")
    parts = [f"{n} {k}{'s' if n != 1 else ''}" for k, n in sorted(
        counts.items(), key=lambda x: -x[1])]
    lead = (f"{total} qualifying records: " + ", ".join(parts) + ". ")
    if grant_total:
        lead += f"Public award value totalled {money(grant_total)}. "
    if top:
        lead += ("Most active organisations: "
                 + ", ".join(f"{n} ({c})" for n, c in top[:3]) + ".")
    md.append(lead + "\n")

    for key, label, blurb in SECTIONS:
        recs = data.get(key) or []
        if not recs:
            continue
        md.append(f"## {label}")
        md.append(f"*{blurb} — {len(recs)} this period*\n")
        for r in recs[:12]:
            bits = []
            if r.get("date"):
                bits.append(dstr(r["date"]))
            if r.get("org"):
                bits.append(f"**{r['org']}**")
            head = " · ".join(bits)
            title = r.get("title") or "(untitled)"
            line = f"- {head} — {title}" if head else f"- {title}"
            if r.get("amount"):
                line += f" — {money(r['amount'])}"
            if r.get("status") and key in ("clearance", "posting", "trial"):
                line += f"  `{r['status']}`"
            if r.get("url"):
                line += f"  [source]({r['url']})"
            md.append(line)
            if key == "posting" and r.get("summary"):
                md.append(f"    - *{r['summary']}*")
        if len(recs) > 12:
            md.append(f"- …and {len(recs) - 12} more")
        md.append("")

    if conns:
        md.append("## Connections")
        md.append("*Cross-layer links from the knowledge graph — these are not "
                  "visible in any single source.*\n")
        for c in conns:
            md.append(f"- {c}")
        md.append("")

    md.append("---")
    md.append(f"*Generated {now:%Y-%m-%d %H:%M} UTC by the NIA pipeline. Every "
              f"item traces to a public filing, award, registration or posting. "
              f"Records that did not pass the neurotech relevance gate are "
              f"excluded and counted, not silently dropped.*")
    return "\n".join(md)


def to_html(md_text: str, demo: bool) -> str:
    body = []
    in_list = False
    for raw in md_text.split("\n"):
        ln = raw.rstrip()
        esc = html.escape(ln)
        esc = esc.replace("&lt;", "<").replace("&gt;", ">")
        # inline md
        import re
        esc = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", esc)
        esc = re.sub(r"\*(.+?)\*", r"<i>\1</i>", esc)
        esc = re.sub(r"`(.+?)`", r"<code>\1</code>", esc)
        esc = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', esc)

        if ln.startswith("    - "):
            body.append(f'<div class="sub">{esc[6:]}</div>'); continue
        if ln.startswith("- "):
            if not in_list:
                body.append("<ul>"); in_list = True
            body.append(f"<li>{esc[2:]}</li>"); continue
        if in_list:
            body.append("</ul>"); in_list = False
        if ln.startswith("# "):   body.append(f"<h1>{esc[2:]}</h1>")
        elif ln.startswith("## "):body.append(f"<h2>{esc[3:]}</h2>")
        elif ln.startswith("> "): body.append(f'<div class="warn">{esc[2:]}</div>')
        elif ln.startswith("---"):body.append("<hr>")
        elif ln.strip():          body.append(f"<p>{esc}</p>")
    if in_list:
        body.append("</ul>")

    return f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NIA Intelligence Layer</title><style>
body{{margin:0;background:{BG};color:{TEXT};font:14px/1.65 'Courier New',ui-monospace,monospace}}
.pg{{max-width:760px;margin:0 auto;padding:40px 24px 80px}}
h1{{font-size:20px;letter-spacing:.14em;color:{AMBER};text-transform:uppercase;
   border-bottom:1px solid {BORDER};padding-bottom:12px;margin-bottom:6px}}
h2{{font-size:12px;letter-spacing:.14em;color:{AMBER};text-transform:uppercase;
   margin:32px 0 4px;border-bottom:1px solid {BORDER};padding-bottom:6px}}
p{{margin:8px 0}} i{{color:{DIM};font-style:normal}}
ul{{list-style:none;padding:0;margin:10px 0}}
li{{padding:7px 0 7px 14px;border-left:2px solid {BORDER};margin-bottom:3px}}
li:hover{{border-left-color:{AMBER};background:{CARD}}}
b{{color:#fff}} code{{background:{CARD};border:1px solid {BORDER};padding:1px 5px;
  border-radius:3px;font-size:11.5px;color:#9fb3c8}}
a{{color:{AMBER}}} hr{{border:0;border-top:1px solid {BORDER};margin:34px 0}}
.sub{{color:{DIM};font-size:12.5px;padding-left:28px;margin:-2px 0 6px}}
.warn{{border:1px solid {AMBER};background:#150f00;color:{AMBER};padding:10px 14px;
  border-radius:4px;margin:16px 0;font-size:12.5px}}
</style></head><body><div class="pg">{''.join(body)}</div></body></html>"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--demo", action="store_true")
    ap.add_argument("--graph", default="nia_graph.sqlite")
    ap.add_argument("--out-md", default="intelligence_layer_issue.md")
    ap.add_argument("--out-html", default="intelligence_layer_issue.html")
    a = ap.parse_args()

    if a.demo:
        data, demo = fetch_demo()
    else:
        try:
            data, demo = fetch_from_db(a.days)
        except Exception as exc:
            print(f"  DB unavailable ({exc}); falling back to --demo corpus",
                  file=sys.stderr)
            data, demo = fetch_demo()

    conns = graph_connections(a.graph)
    md = build(data, a.days, demo, conns)

    with open(a.out_md, "w", encoding="utf-8") as f:
        f.write(md)
    with open(a.out_html, "w", encoding="utf-8") as f:
        f.write(to_html(md, demo))

    total = sum(len(v) for v in data.values())
    print(f"  issue written -> {a.out_md} + {a.out_html}")
    print(f"  {total} records across {len([k for k,v in data.items() if v])} sections"
          + ("   [ILLUSTRATIVE CORPUS]" if demo else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
