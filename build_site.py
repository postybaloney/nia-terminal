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


def nav_html(current: str) -> str:
    parts = []
    for href, label in PAGES:
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
    built = run("knowledge graph (build)", [py, "graph_build.py", "--out", gdb, *demo])
    ok["graph.html"] = built and run("knowledge graph (render)", [
        py, "graph_render.py", "--db", gdb,
        "--out", os.path.join(a.out, "graph.html"),
        "--max-nodes", str(a.max_nodes)])

    # 2 · dashboard — reads the graph for establishment/frontier/affect.
    #     --graph is passed even when the build failed: build_snapshot degrades
    #     to an explicit "not available" note, which is the correct output.
    ok["index.html"] = run("dashboard snapshot", [
        py, "build_snapshot.py", "--out", os.path.join(a.out, "index.html"),
        "--graph", gdb, *demo])

    # 3 · intelligence layer issue
    ok["issue.html"] = run("intelligence layer issue", [
        py, "build_issue.py", "--days", str(a.days), "--graph", gdb,
        "--out-md", os.path.join(a.out, "issue.md"),
        "--out-html", os.path.join(a.out, "issue.html"), *demo])

    # 4 · shared nav
    print("  ── linking pages")
    for href, _ in PAGES:
        p = os.path.join(a.out, href)
        if os.path.exists(p):
            inject_nav(p, href)

    # 5 · if a page failed, leave a real placeholder rather than a broken link
    for href, label in PAGES:
        p = os.path.join(a.out, href)
        if not os.path.exists(p):
            with open(p, "w", encoding="utf-8") as f:
                f.write(f"""<!DOCTYPE html><html><head><meta charset="utf-8">
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
