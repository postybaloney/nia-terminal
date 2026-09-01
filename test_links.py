"""
test_links.py — the pages are fed by RSS, so hostile input is the normal case.

NIA publishes to parthrudesai.com from a corpus assembled out of RSS feeds,
job boards and third-party APIs. Every title, organisation name, summary and
URL on those pages is attacker-influenceable: anyone who can get an item into
a monitored feed can choose what text NIA renders.

Two real holes existed in build_issue.to_html before 2026-08-20:

  1. `esc.replace("&lt;", "<").replace("&gt;", ">")` undid the escaping that
     html.escape had applied one line earlier, re-enabling raw markup from
     any ingested string.
  2. The markdown-link regex interpolated the captured URL straight into an
     href with no scheme check, so `[source](javascript:...)` became a live
     payload.

Both are closed. This file is what stops them reopening — the fix is one line
in each case and both look like harmless cleanup to anyone who does not know
why they are there.

Run:  python test_links.py
"""
from __future__ import annotations

import html
import re
from html.parser import HTMLParser
import sys

PASS, FAIL = 0, 0


def check(label: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"    ok    {label}")
    else:
        FAIL += 1
        print(f"    FAIL  {label}")
        if detail:
            print(f"          {detail}")


# Payloads chosen to cover the distinct mechanisms, not just to be many.
HOSTILE_URLS = [
    "javascript:alert(1)",
    "JaVaScRiPt:alert(1)",
    "java\tscript:alert(1)",
    "data:text/html;base64,PHNjcmlwdD5hbGVydCgxKTwvc2NyaXB0Pg==",
    "vbscript:msgbox(1)",
    "file:///etc/passwd",
    'https://ok.example/"onmouseover="alert(1)',
]

HOSTILE_TEXT = [
    "<script>alert(1)</script>",
    '<img src=x onerror="alert(1)">',
    "</td><td><script>alert(1)</script>",
    '" onload="alert(1)',
    "<svg/onload=alert(1)>",
]


class _Auditor(HTMLParser):
    """
    Walk the produced DOM and report anything the browser would execute.

    A string search is the wrong tool here and gives false positives that
    train you to ignore the test: a correctly escaped
    `&lt;img src=x onerror=&quot;alert(1)&quot;&gt;` still contains the
    substring "onerror=", but it is inert text, not an element. What matters
    is whether the PARSER sees a dangerous tag or attribute — so this checks
    the parse, not the bytes.
    """

    # `svg` and `script` are flagged unconditionally: to_html and the snapshot
    # renderer emit neither into the body, so one appearing means injected
    # markup. `meta` and `link` are NOT — the generated pages legitimately
    # carry <meta charset> and a stylesheet link, and flagging those trains
    # you to ignore the test. They are checked by attribute instead.
    DANGEROUS_TAGS = {"script", "svg", "iframe", "object", "embed", "base",
                      "form"}
    URL_ATTRS = {"href", "src", "action", "formaction", "data", "xlink:href"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.findings: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag in self.DANGEROUS_TAGS:
            self.findings.append(f"<{tag}> element present")
        if tag == "meta":
            # A charset or viewport meta is the page's own. http-equiv is the
            # one that can redirect the reader off-site.
            if any((n or "").lower() == "http-equiv" for n, _ in attrs):
                self.findings.append("<meta http-equiv> present")
        for name, value in attrs:
            n = (name or "").lower()
            v = (value or "")
            if n.startswith("on"):
                self.findings.append(f"event handler {n}={v[:40]!r} on <{tag}>")
            if n in self.URL_ATTRS:
                scheme = v.split(":", 1)[0].lower().strip() if ":" in v else ""
                # Strip the whitespace browsers ignore inside a scheme.
                scheme = re.sub(r"\s+", "", scheme)
                if scheme and scheme not in ("http", "https"):
                    self.findings.append(f"{n} scheme {scheme!r} on <{tag}>")


def _has_live_markup(doc: str) -> str | None:
    """Return a description of the offending element if any survived."""
    a = _Auditor()
    a.feed(doc)
    a.close()
    return "; ".join(a.findings[:3]) if a.findings else None


# ─────────────────────────────────────────────────────────────────────────────

def test_sourcelinks() -> None:
    from sourcelinks import safe_url
    print("\n  ── sourcelinks.safe_url is the boundary ──")
    for u in HOSTILE_URLS:
        got = safe_url(u)
        check(f"rejects {u[:38]!r}", got is None, f"returned {got!r}")
    check("passes a normal https URL",
          safe_url("https://clinicaltrials.gov/study/NCT06120491")
          == "https://clinicaltrials.gov/study/NCT06120491")


def test_issue_renderer() -> None:
    """
    build_issue.to_html is the one that had both holes.

    The markdown it renders is generated from ingested fields, so the test
    feeds hostile strings through the same markdown shapes the real generator
    emits — a bullet with a [source](url) link, and a bare title.
    """
    import build_issue
    print("\n  ── build_issue.to_html ──")

    lines = []
    for u in HOSTILE_URLS:
        lines.append(f"- Some Org — A title  [source]({u})")
    for t in HOSTILE_TEXT:
        lines.append(f"- {t} — another title  [source](https://ok.example/x)")
        lines.append(f"- Org — {t}")

    doc = build_issue.to_html("\n".join(lines), demo=True)
    bad = _has_live_markup(doc)
    check("no executable markup survives", bad is None, bad or "")

    # A legitimate link must still work, or the fix has broken the feature.
    good = build_issue.to_html(
        "- Org — Title  [source](https://reporter.nih.gov/project-details/1)",
        demo=True)
    check("legitimate link still renders",
          'href="https://reporter.nih.gov/project-details/1"' in good)
    check("legitimate link is rel-protected",
          'rel="noopener noreferrer"' in good)
    check("hostile link degrades to plain text, not a dead anchor",
          "javascript:" not in doc and "source" in doc)


def test_snapshot_cell() -> None:
    import build_snapshot
    print("\n  ── build_snapshot._cell ──")

    docs = []
    for u in HOSTILE_URLS:
        # _cell trusts sourcelinks upstream, so the realistic attack is a URL
        # that PASSED safe_url but still carries attribute-breaking characters.
        docs.append(build_snapshot._cell(("title", u)))
    for t in HOSTILE_TEXT:
        docs.append(build_snapshot._cell((t, "https://ok.example/x")))
        docs.append(build_snapshot._cell(t))
    doc = "".join(docs)
    bad = _has_live_markup(doc)
    check("no executable markup survives", bad is None, bad or "")
    check("quote in URL cannot break the href",
          doc.count('href="') == doc.count('rel="noopener noreferrer"'))

    ok = build_snapshot._cell(("A patent", "https://patents.google.com/patent/EP1A1/en"))
    check("legitimate link still renders",
          'href="https://patents.google.com/patent/EP1A1/en"' in ok)
    check("plain cell stays plain", build_snapshot._cell("just text") == "just text")


def test_graph_payload() -> None:
    """
    graph_render embeds its node payload as JSON inside a <script> block, so a
    hostile string must not be able to close that block.
    """
    import json
    from graph_render import _script_safe_json
    print("\n  ── graph_render payload ──")

    # The realistic attack: an assignee or organisation name that closes the
    # inline <script> the payload is embedded in. json.dumps alone does not
    # stop this, because it has no reason to escape < or >.
    hostile = ["Acme </script><script>alert(1)</script>",
               "</SCRIPT ><svg onload=alert(1)>",
               "Norm & Sons \u2028 Ltd"]
    payload = _script_safe_json({"nodes": [{"n": t} for t in hostile]})
    check("no raw </script> in payload", "</script>" not in payload.lower())
    check("no raw angle brackets at all", "<" not in payload and ">" not in payload)
    check("payload still parses back to the original text",
          json.loads(payload)["nodes"][0]["n"] == hostile[0])
    check("naive json.dumps would have failed (test is meaningful)",
          "</script>" in json.dumps({"n": hostile[0]}).lower())


def test_graph_meta_roundtrip() -> None:
    from graph_build import read_meta, work_meta
    print("\n  ── graph_build meta round-trip ──")
    for u in HOSTILE_URLS:
        m = read_meta(work_meta(url=u, title="t"))
        check(f"hostile url dropped from meta {u[:30]!r}", m.get("url") is None,
              f"kept {m.get('url')!r}")
    m = read_meta(work_meta(url="https://ok.example/x", title="Title"))
    check("good url survives", m.get("url") == "https://ok.example/x")
    check("legacy bare-url row still readable",
          read_meta("https://ok.example/y").get("url") == "https://ok.example/y")
    check("legacy bare-title row still readable",
          read_meta("Some patent title").get("title") == "Some patent title")


def main() -> int:
    print("\n" + "=" * 72)
    print("  NIA link-safety regression tests")
    print("=" * 72)
    for fn in (test_sourcelinks, test_issue_renderer, test_snapshot_cell,
               test_graph_payload, test_graph_meta_roundtrip):
        try:
            fn()
        except Exception as exc:
            global FAIL
            FAIL += 1
            print(f"    FAIL  {fn.__name__} raised {type(exc).__name__}: {exc}")
    print("\n" + "-" * 72)
    print(f"  {PASS} passed, {FAIL} failed")
    print("-" * 72 + "\n")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
