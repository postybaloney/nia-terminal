"""
Assemble the full public site — dashboard, knowledge graph, and the current
Intelligence Layer issue — into one folder with shared navigation.

Each page is produced by its own generator; this script runs them, links them
together, and degrades gracefully. If one generator fails, the others still
publish. A partially-successful night should still leave a live site, not a
404 — the same principle the ingest pipeline now follows.

Output (all self-contained, no CDN, no server):

    site/index.html   dashboard snapshot   (build_snapshot.py)
    site/graph.html   knowledge graph      (graph_build.py + graph_render.py)
    site/issue.html   Intelligence Layer   (build_issue.py)
    site/issue.md     same, as Markdown

Usage:
    python build_site.py                 # live DB
    python build_site.py --demo          # illustrative corpus, no DB needed
    python build_site.py --out public/
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone

AMBER, BG, BORDER, DIM = "#f59e0b", "#050810", "#1f2937", "#6b7280"

PAGES = [
    ("index.html", "Dashboard"),
    ("graph.html", "Knowledge Graph"),
    ("issue.html", "Intelligence Layer"),
]

# Appended to the nav only when LEGAL_LINK_IN_NAV is true. Kept separate from
# PAGES because PAGES also drives the "generator failed, leave a placeholder"
# loop — and a missing legal page should mean "not configured", not "broken".
LEGAL_PAGES = [
    ("privacy.html", "Privacy"),
    ("terms.html", "Terms"),
    ("sources.html", "Sources"),
]

_EXTRA_NAV: list[tuple[str, str]] = []


def nav_html(current: str) -> str:
    parts = []
    for href, label in PAGES + _EXTRA_NAV:
        cls = ' class="on"' if href == current else ""
        parts.append(f'<a href="{href}"{cls}>{label}</a>')
    links = "".join(parts)
    return f"""<nav class="nia-nav">
<span class="nia-brand">NIA</span>{links}
<span class="nia-built">updated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC</span>
</nav>
<style>
.nia-nav{{display:flex;align-items:center;gap:20px;flex-wrap:wrap;
  background:{BG};border-bottom:1px solid {BORDER};padding:10px 20px;
  font:12px 'Courier New',ui-monospace,monospace;position:sticky;top:0;z-index:999}}
.nia-brand{{color:{AMBER};letter-spacing:.2em;font-weight:700}}
.nia-nav a{{color:{DIM};text-decoration:none;letter-spacing:.06em}}
.nia-nav a:hover{{color:{AMBER}}}
.nia-nav a.on{{color:#e5e7eb;border-bottom:1px solid {AMBER};padding-bottom:2px}}
.nia-built{{margin-left:auto;color:{DIM};font-size:11px}}
</style>
"""


def inject_nav(path: str, current: str) -> bool:
    """Insert the shared nav immediately after <body>."""
    try:
        with open(path, encoding="utf-8") as f:
            html = f.read()
    except FileNotFoundError:
        return False
    if "nia-nav" in html:
        return True                     # already injected
    i = html.lower().find("<body")
    if i == -1:
        return False
    j = html.find(">", i)
    if j == -1:
        return False
    out = html[:j + 1] + "\n" + nav_html(current) + html[j + 1:]
    with open(path, "w", encoding="utf-8") as f:
        f.write(out)
    return True


def run(label: str, cmd: list[str]) -> bool:
    print(f"  ── {label}")
    r = subprocess.run(cmd)
    if r.returncode != 0:
        print(f"     FAILED ({label}) — continuing so the rest still publishes",
              file=sys.stderr)
        return False
    return True


def clear(*paths: str) -> None:
    """
    Delete a generator's output before running it.

    This exists because of a silent three-week failure. `site/` is committed to
    the repo, so a CI checkout restores the PREVIOUS build's HTML before the
    generators run. When build_issue.py then failed, the placeholder logic
    below did nothing — it only writes a placeholder when the file is ABSENT,
    and the stale file from the last successful run was sitting right there.
    That stale page was uploaded to Pages and served as current.

    The result: parthrudesai.com served an "Intelligence Layer" dated
    2026-08-18 for three weeks, beside a dashboard dated 2026-09-11, while
    every workflow run reported success. A failing generator was invisible.

    Deleting first makes failure look like failure. The placeholder then fires
    as it was always supposed to, and the page says so rather than lying.
    """
    for p in paths:
        try:
            os.remove(p)
        except FileNotFoundError:
            pass
        except OSError as exc:
            print(f"  !! could not clear {p}: {exc}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="site")
    ap.add_argument("--demo", action="store_true")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--max-nodes", type=int, default=650)
    a = ap.parse_args()

    py = sys.executable
    os.makedirs(a.out, exist_ok=True)
    demo = ["--demo"] if a.demo else []
    ok: dict[str, bool] = {}

    # 1 · knowledge graph — FIRST, because everything downstream reads it.
    #     The dashboard's scored leaderboard and affect layer come out of this
    #     SQLite file; building the dashboard before the graph (the original
    #     order) meant those cards could only ever render "not available".
    gdb = os.path.join(a.out, "nia_graph.sqlite")
    clear(os.path.join(a.out, "graph.html"))
    built = run("knowledge graph (build)", [py, "graph_build.py", "--out", gdb, *demo])
    ok["graph.html"] = built and run("knowledge graph (render)", [
        py, "graph_render.py", "--db", gdb,
        "--out", os.path.join(a.out, "graph.html"),
        "--max-nodes", str(a.max_nodes)])

    # 2 · dashboard — reads the graph for establishment/frontier/affect.
    #     --graph is passed even when the build failed: build_snapshot degrades
    #     to an explicit "not available" note, which is the correct output.
    clear(os.path.join(a.out, "index.html"))
    ok["index.html"] = run("dashboard snapshot", [
        py, "build_snapshot.py", "--out", os.path.join(a.out, "index.html"),
        "--graph", gdb, *demo])

    # 3 · intelligence layer issue
    # The markdown goes OUTSIDE the published directory. A .md file cannot
    # carry <meta name="robots">, so a copy inside site/ is reachable and
    # indexable no matter what the HTML pages declare — and it contains every
    # name the HTML does. Nothing links to it, so publishing it bought nothing
    # and quietly defeated the noindex on issue.html.
    clear(os.path.join(a.out, "issue.html"))
    ok["issue.html"] = run("intelligence layer issue", [
        py, "build_issue.py", "--days", str(a.days), "--graph", gdb,
        "--out-md", "issue.md",
        "--out-html", os.path.join(a.out, "issue.html"), *demo])

    # 4 · legal pages — generated whenever they are configured.
    #     Deliberately NOT fatal when unconfigured: a half-set-up legal identity
    #     must not be able to take the whole nightly build down.
    legal_pages: list[tuple[str, str]] = []
    try:
        from config import settings
        cfg = [settings.legal_controller, settings.legal_location,
               settings.legal_state, settings.legal_contact]
    except Exception as exc:
        cfg = []
        print(f"  ── legal pages skipped (config unavailable: {exc})")
    if cfg and all(cfg):
        if run("legal pages", [
                py, "build_legal.py", "--out", a.out,
                "--controller", settings.legal_controller,
                "--location", settings.legal_location,
                "--state", settings.legal_state,
                "--contact", settings.legal_contact]):
            legal_pages = LEGAL_PAGES
            if not settings.legal_link_in_nav:
                print("     built but NOT linked — set LEGAL_LINK_IN_NAV=true "
                      "to publish them")
    elif cfg:
        missing = [n for n, v in zip(
            ("LEGAL_CONTROLLER", "LEGAL_LOCATION", "LEGAL_STATE",
             "LEGAL_CONTACT"), cfg) if not v]
        print(f"  ── legal pages skipped — unset: {', '.join(missing)}")

    # 5 · shared nav
    print("  ── linking pages")
    global _EXTRA_NAV
    if legal_pages:
        try:
            from config import settings as _s
            if _s.legal_link_in_nav:
                _EXTRA_NAV = legal_pages
        except Exception:
            pass
    for href, _ in PAGES + _EXTRA_NAV:
        p = os.path.join(a.out, href)
        if os.path.exists(p):
            inject_nav(p, href)

    # 5 · if a page failed, leave a real placeholder rather than a broken link
    stale_guard = []
    for href, label in PAGES:
        p = os.path.join(a.out, href)
        if not os.path.exists(p):
            stale_guard.append(href)
            with open(p, "w", encoding="utf-8") as f:
                f.write(f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="robots" content="noindex, nofollow, noarchive">
<title>{label} — unavailable</title></head>
<body style="margin:0;background:{BG};color:#e5e7eb;
 font:14px 'Courier New',monospace">{nav_html(href)}
<div style="max-width:640px;margin:60px auto;padding:0 24px">
<h1 style="color:{AMBER};font-size:16px;letter-spacing:.14em">
{label.upper()} UNAVAILABLE</h1>
<p style="color:{DIM}">This page could not be generated on the last run.
The other sections are current. Nothing is shown here rather than showing
stale data as if it were fresh.</p></div></body></html>""")

    # the SQLite file is a build artifact, not something to serve
    if os.path.exists(gdb):
        try:
            os.remove(gdb)
        except OSError:
            pass

    print()
    for href, label in PAGES:
        status = "ok" if ok.get(href) else "FAILED -> placeholder"
        print(f"  {label:<20} {a.out}/{href:<12} {status}")
    print(f"\n  site ready -> {a.out}/")
    # Never fail the build over one bad page; publishing something beats nothing.
    return 0


if __name__ == "__main__":
    sys.exit(main())
