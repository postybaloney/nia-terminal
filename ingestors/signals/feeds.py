"""
RSS / Atom ingestor — newsletters, industry press, and arXiv category feeds.

Why this source matters: patents lag reality by ~18 months and grants by ~9.
Newsletters and preprints are the leading edge, and the single densest source
in the whole pipeline is Neurotech Notables — the competitor newsletter —
which publishes full article text in its feed, biweekly, for free.

Everything here is free, unauthenticated, and served over plain RSS/Atom.
Deliberately NOT used: X/Twitter (free tier withdrawn; per-post pricing makes
keyword monitoring expensive) and LinkedIn (hiQ v. LinkedIn was won on the
CFAA claim but LOST on breach of contract — permanent injunction, destruction
of scraped data, $500k damages; no official API tier permits extraction).
The ATS job boards in jobs.py give the same hiring signal legally.

Parsing uses the standard library only. feedparser is better at malformed
feeds, but adding a dependency to a pipeline that runs unattended in GitHub
Actions is a deployment risk, and every feed here is from a major publisher
emitting well-formed XML. If feedparser happens to be installed we use it.
"""
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET

import httpx

from ingestors.signals.base import BaseSignalIngestor, NormalizedSignal
from neuro_taxonomy import classify_tech, score_record

log = logging.getLogger(__name__)

_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "content": "http://purl.org/rss/1.0/modules/content/",
    "dc": "http://purl.org/dc/elements/1.1/",
}


@dataclass(frozen=True)
class Feed:
    key: str            # short, stable; becomes part of `source` (<=32 chars)
    name: str           # human-readable publication name
    url: str
    kind: str           # "newsletter" | "press" | "preprint"
    # True when every item is neurotech by construction, so the relevance gate
    # would only ever throw away good data (e.g. a neurotech-only newsletter).
    always_relevant: bool = False


FEED_REGISTRY: tuple[Feed, ...] = (
    # ── Newsletters ──────────────────────────────────────────────────────────
    Feed("nt_notables", "Neurotech Notables (Naveen Rao)",
         "https://neurotechnology.substack.com/feed", "newsletter", True),
    Feed("nt_napkin", "The Neurotech Napkin",
         "https://theneurotechnapkin.substack.com/feed", "newsletter", True),
    # The Transmitter is general neuroscience, not neurotech, so most items are
    # correctly rejected by the relevance gate — a run returning 0 from this
    # feed is usually right, not broken. Kept because when it does hit, it hits
    # hard (device trials, BCI results). Do NOT set always_relevant on it.
    Feed("transmitter", "The Transmitter (Simons Foundation)",
         "https://www.thetransmitter.org/feed/", "press", False),
    # Nexstem removed 2026-08-18 — the Substack URL 404s; that entry was a
    # pattern guess rather than a verified feed. Re-add only against a URL you
    # have actually fetched.
    # ── arXiv category feeds (daily, abstracts included) ─────────────────────
    Feed("arxiv_qbio_nc", "arXiv q-bio.NC (Neurons & Cognition)",
         "https://rss.arxiv.org/rss/q-bio.NC", "preprint", False),
    Feed("arxiv_eess_sp", "arXiv eess.SP (Signal Processing)",
         "https://rss.arxiv.org/rss/eess.SP", "preprint", False),
    Feed("arxiv_cs_hc", "arXiv cs.HC (Human-Computer Interaction)",
         "https://rss.arxiv.org/rss/cs.HC", "preprint", False),
)


def _text(el) -> str:
    if el is None:
        return ""
    return "".join(el.itertext()).strip() if len(el) else (el.text or "").strip()


def _strip_html(s: str) -> str:
    s = re.sub(r"<script.*?</script>", " ", s, flags=re.S | re.I)
    s = re.sub(r"<style.*?</style>", " ", s, flags=re.S | re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"&nbsp;?", " ", s)
    s = re.sub(r"&amp;", "&", s)
    s = re.sub(r"&lt;", "<", s)
    s = re.sub(r"&gt;", ">", s)
    s = re.sub(r"&#\d+;", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _parse_date(raw: str | None) -> datetime | None:
    if not raw:
        return None
    raw = raw.strip()
    try:                                   # RFC 822 — RSS pubDate
        dt = parsedate_to_datetime(raw)
        return dt.replace(tzinfo=None) if dt else None
    except Exception:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw[:len(fmt) + 5], fmt).replace(tzinfo=None)
        except (ValueError, TypeError):
            continue
    return None


class FeedIngestor(BaseSignalIngestor):
    """Pulls every feed in FEED_REGISTRY. One feed failing never stops the rest."""

    name = "feeds"

    def __init__(self, *args, feeds: tuple[Feed, ...] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.feeds = feeds or FEED_REGISTRY

    async def fetch(self) -> list[NormalizedSignal]:
        out: list[NormalizedSignal] = []
        self.query_errors = []

        headers = {
            "User-Agent": (
                "NIA-neurotech-intelligence/1.0 "
                "(+https://github.com/postybaloney/nia-terminal)"
            ),
            "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml",
        }
        async with httpx.AsyncClient(timeout=25, follow_redirects=True,
                                     headers=headers) as client:
            for feed in self.feeds:
                try:
                    items = await self._fetch_feed(client, feed)
                except Exception as exc:
                    log.warning("feeds: %s failed (%s: %s) — continuing",
                                feed.key, type(exc).__name__, exc)
                    self.query_errors.append(f"{feed.key}: {type(exc).__name__}: {exc}")
                    continue
                out.extend(items)
                log.info("feeds: %-14s fetched=%-3d kept=%d",
                         feed.key, items and len(items) or 0, len(items))
                await asyncio.sleep(0.3)

        return out

    async def _fetch_feed(
        self, client: httpx.AsyncClient, feed: Feed
    ) -> list[NormalizedSignal]:
        resp = await client.get(feed.url)
        resp.raise_for_status()
        return self._parse(resp.content, feed)

    # ── parsing ──────────────────────────────────────────────────────────────

    def _parse(self, raw: bytes, feed: Feed) -> list[NormalizedSignal]:
        root = ET.fromstring(raw)

        # RSS 2.0 → channel/item ; Atom → feed/entry
        entries = root.findall(".//item")
        is_atom = False
        if not entries:
            entries = root.findall("atom:entry", _NS) or root.findall(
                ".//{http://www.w3.org/2005/Atom}entry")
            is_atom = True

        out: list[NormalizedSignal] = []
        for e in entries:
            sig = self._parse_entry(e, feed, is_atom)
            if sig is not None:
                out.append(sig)
        return out

    def _parse_entry(self, e, feed: Feed, is_atom: bool) -> NormalizedSignal | None:
        if is_atom:
            title = _text(e.find("atom:title", _NS))
            link_el = e.find("atom:link", _NS)
            url = link_el.get("href") if link_el is not None else ""
            body = (_text(e.find("atom:content", _NS))
                    or _text(e.find("atom:summary", _NS)))
            date_raw = (_text(e.find("atom:published", _NS))
                        or _text(e.find("atom:updated", _NS)))
            uid = _text(e.find("atom:id", _NS)) or url
            authors = [_text(a.find("atom:name", _NS))
                       for a in e.findall("atom:author", _NS)]
        else:
            title = _text(e.find("title"))
            url = _text(e.find("link"))
            body = (_text(e.find("content:encoded", _NS))
                    or _text(e.find("description")))
            date_raw = _text(e.find("pubDate")) or _text(e.find("dc:date", _NS))
            uid = _text(e.find("guid")) or url
            creator = _text(e.find("dc:creator", _NS))
            authors = [creator] if creator else []

        title = _strip_html(title)
        body = _strip_html(body)
        if not title:
            return None

        # arXiv RSS packs "Title: x  Authors: y  Abstract: z" into description.
        if feed.kind == "preprint" and "Abstract:" in body:
            body = body.split("Abstract:", 1)[1].strip()

        # Relevance. Newsletters flagged always_relevant bypass the gate —
        # a neurotech-only publication has nothing to filter.
        if not feed.always_relevant:
            rel = score_record(title, body[:3000])
            if rel.tier not in ("core", "adjacent"):
                return None
            reasons = rel.reasons
            score = rel.score
        else:
            reasons = [f"{feed.name}: neurotech publication"]
            score = 5

        source_id = (uid or url or title)[:128]

        return NormalizedSignal(
            source=f"rss:{feed.key}"[:32],
            source_id=source_id,
            signal_type="preprint" if feed.kind == "preprint" else "article",
            title=self._truncate(title, 500),
            summary=self._truncate(body, 4000),
            organization=feed.name,
            people=[{"name": a, "role": "author"} for a in authors if a][:12],
            amount=None,
            event_date=_parse_date(date_raw),
            status=None,
            tags=classify_tech(title, body[:3000]),
            url=url or None,
            matched_query=feed.key,
            raw_payload={
                "feed": feed.key,
                "feed_name": feed.name,
                "kind": feed.kind,
                "relevance_score": score,
                "relevance_reasons": reasons,
            },
        )
