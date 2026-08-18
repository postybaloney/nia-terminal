"""
Preprint ingestor — arXiv REST + bioRxiv/medRxiv.

Complements the arXiv RSS feeds in feeds.py: RSS gives the daily heartbeat
(only the most recent items), this gives dated backfill and keyword sweeps.

arXiv REST      http://export.arxiv.org/api/query   (no key; be polite — the
                published guidance is ~1 request every 3 seconds)
bioRxiv/medRxiv https://api.biorxiv.org/details/{server}/{from}/{to}/{cursor}
                (no key, no documented rate limit, 30 records per page)

The bioRxiv `pubs` endpoint is an underused maturity signal: it reports which
preprints have since been published in a journal, which is a cleaner "this
work survived peer review" marker than citation counts at this timescale.
"""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET

import httpx

from ingestors.signals.base import BaseSignalIngestor, NormalizedSignal
from neuro_taxonomy import classify_tech, score_record

log = logging.getLogger(__name__)

_ARXIV_URL = "http://export.arxiv.org/api/query"
_ATOM = {"a": "http://www.w3.org/2005/Atom"}

# Phrase queries — same precision discipline as the patent side. Single words
# like "neural" are exactly what poisoned the patent corpus.
ARXIV_QUERIES: tuple[str, ...] = (
    'abs:"brain-computer interface"',
    'abs:"brain machine interface"',
    'abs:"deep brain stimulation"',
    'abs:"neural decoding"',
    'abs:"spinal cord stimulation"',
    'abs:"electrocorticography"',
    'abs:"microelectrode array"',
    'abs:"neural prosthesis" OR abs:"neuroprosthesis"',
    'abs:"closed-loop neuromodulation" OR abs:"adaptive stimulation"',
    'abs:"transcranial magnetic stimulation"',
)

_BIORXIV_CATEGORIES = {"neuroscience", "bioengineering", "biomedical engineering"}


def _clean(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


class PreprintIngestor(BaseSignalIngestor):
    name = "preprints"

    def __init__(self, *args, days_back: int = 45, **kwargs):
        super().__init__(*args, **kwargs)
        self.days_back = days_back

    async def fetch(self) -> list[NormalizedSignal]:
        out: list[NormalizedSignal] = []
        self.query_errors = []

        headers = {"User-Agent": "NIA-neurotech-intelligence/1.0"}
        async with httpx.AsyncClient(timeout=30, follow_redirects=True,
                                     headers=headers) as client:
            for q in ARXIV_QUERIES:
                try:
                    items = await self._arxiv(client, q)
                except Exception as exc:
                    log.warning("preprints: arxiv %r failed (%s) — continuing", q, exc)
                    self.query_errors.append(f"arxiv {q}: {type(exc).__name__}: {exc}")
                    continue
                out.extend(items)
                log.info("preprints: arxiv q=%-46s fetched=%d", q[:46], len(items))
                await asyncio.sleep(3.0)   # arXiv asks for ~1 req / 3s

            for server in ("biorxiv", "medrxiv"):
                try:
                    items = await self._biorxiv(client, server)
                except Exception as exc:
                    log.warning("preprints: %s failed (%s) — continuing", server, exc)
                    self.query_errors.append(f"{server}: {type(exc).__name__}: {exc}")
                    continue
                out.extend(items)
                log.info("preprints: %-8s kept=%d", server, len(items))

        return out

    # ── arXiv ────────────────────────────────────────────────────────────────

    async def _arxiv(self, client: httpx.AsyncClient, query: str) -> list[NormalizedSignal]:
        params = {
            "search_query": query,
            "sortBy": "submittedDate",     # MUST pair with date intent; relevance
            "sortOrder": "descending",     # sort silently reorders results
            "start": "0",
            "max_results": str(min(self.per_page, 100)),
        }
        resp = await client.get(_ARXIV_URL, params=params)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)

        cutoff = datetime.utcnow() - timedelta(days=self.days_back)
        out: list[NormalizedSignal] = []

        for e in root.findall("a:entry", _ATOM):
            title = _clean("".join(e.find("a:title", _ATOM).itertext())
                           if e.find("a:title", _ATOM) is not None else "")
            summary = _clean("".join(e.find("a:summary", _ATOM).itertext())
                             if e.find("a:summary", _ATOM) is not None else "")
            if not title:
                continue

            pub = e.find("a:published", _ATOM)
            when = self._safe_date(pub.text if pub is not None else None)
            if when and when < cutoff:
                continue

            idn = e.find("a:id", _ATOM)
            url = idn.text.strip() if idn is not None and idn.text else ""
            arxiv_id = url.rsplit("/", 1)[-1] if url else title[:60]

            authors = [
                _clean(a.find("a:name", _ATOM).text)
                for a in e.findall("a:author", _ATOM)
                if a.find("a:name", _ATOM) is not None
            ]
            cats = [c.get("term") for c in e.findall("a:category", _ATOM) if c.get("term")]

            rel = score_record(title, summary)
            if rel.tier not in ("core", "adjacent"):
                continue

            out.append(NormalizedSignal(
                source="arxiv",
                source_id=arxiv_id[:128],
                signal_type="preprint",
                title=self._truncate(title, 500),
                summary=self._truncate(summary, 4000),
                organization=None,
                people=[{"name": a, "role": "author"} for a in authors[:12]],
                amount=None,
                event_date=when,
                status=None,
                tags=classify_tech(title, summary) + cats[:4],
                url=url or None,
                matched_query=query,
                raw_payload={
                    "categories": cats,
                    "relevance_score": rel.score,
                    "relevance_reasons": rel.reasons,
                },
            ))
        return out

    # ── bioRxiv / medRxiv ────────────────────────────────────────────────────

    async def _biorxiv(self, client: httpx.AsyncClient, server: str) -> list[NormalizedSignal]:
        end = datetime.utcnow().date()
        start = end - timedelta(days=self.days_back)
        out: list[NormalizedSignal] = []
        cursor = 0

        while cursor < 150:   # cap — 30 records per page
            url = f"https://api.biorxiv.org/details/{server}/{start}/{end}/{cursor}"
            resp = await client.get(url)
            if resp.status_code != 200:
                break
            data = resp.json()
            batch = data.get("collection") or []
            if not batch:
                break

            for rec in batch:
                cat = (rec.get("category") or "").lower()
                title = _clean(rec.get("title"))
                abstract = _clean(rec.get("abstract"))
                if not title:
                    continue
                # Cheap pre-filter, then the real relevance gate.
                if cat and not any(c in cat for c in _BIORXIV_CATEGORIES):
                    rel = score_record(title, abstract)
                    if rel.tier != "core":
                        continue
                else:
                    rel = score_record(title, abstract)
                    if rel.tier not in ("core", "adjacent"):
                        continue

                doi = rec.get("doi") or ""
                authors = [a.strip() for a in (rec.get("authors") or "").split(";") if a.strip()]

                out.append(NormalizedSignal(
                    source=server,
                    source_id=(doi or title)[:128],
                    signal_type="preprint",
                    title=self._truncate(title, 500),
                    summary=self._truncate(abstract, 4000),
                    organization=_clean(rec.get("author_corresponding_institution")) or None,
                    people=[{"name": a, "role": "author"} for a in authors[:12]],
                    amount=None,
                    event_date=self._safe_date(rec.get("date")),
                    status=rec.get("version"),
                    tags=classify_tech(title, abstract) + ([cat] if cat else []),
                    url=f"https://doi.org/{doi}" if doi else None,
                    matched_query=f"{server}:{cat or 'all'}",
                    raw_payload={
                        "server": server,
                        "category": cat,
                        "relevance_score": rel.score,
                        "relevance_reasons": rel.reasons,
                    },
                ))

            cursor += 30
            await asyncio.sleep(0.3)

        return out
