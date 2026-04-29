"""
OpenAlex thesis ingestor.
Docs: https://docs.openalex.org

OpenAlex is a completely free, open index of 250M+ scholarly works.
It covers dissertations from institutions worldwide.
No API key required (but add your email as a polite-pool identifier).

Filtering strategy:
  - type = "dissertation" (OpenAlex work type)
  - publication_year >= since_year
  - keyword search across title + abstract via /works?search=
  - post-filter by our hardware/software keyword lists

Rate limit: 100k requests/day for polite pool (email in header).
"""
from __future__ import annotations

import logging

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestors.theses.base import BaseThesisIngestor, NormalizedThesis, is_relevant

log = logging.getLogger(__name__)

_BASE = "https://api.openalex.org/works"
# Add your email here or in OPENALEX_EMAIL env var to join the polite pool
_EMAIL = "patent-intel@yourdomain.com"


class OpenAlexIngestor(BaseThesisIngestor):
    name = "openalex"

    async def fetch(self) -> list[NormalizedThesis]:
        results: list[NormalizedThesis] = []
        async with httpx.AsyncClient(timeout=30) as client:
            for query in self.queries:
                batch = await self._fetch_query(client, query)
                results.extend(batch)
                log.info("openalex: query=%r fetched=%d relevant=%d",
                         query, len(batch), sum(1 for t in batch if t.hardware_relevant or t.software_relevant))
        return results

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    async def _fetch_query(
        self, client: httpx.AsyncClient, query: str
    ) -> list[NormalizedThesis]:
        params = {
            "search": query,
            "filter": f"type:dissertation,publication_year:>{self.since_year - 1}",
            "per-page": self.per_page,
            "sort": "publication_year:desc",
            "select": ",".join([
                "id", "title", "abstract_inverted_index", "publication_year",
                "authorships", "host_venue", "keywords", "concepts",
                "doi", "primary_location", "language",
                "open_access",
            ]),
            "mailto": _EMAIL,
        }
        resp = await client.get(_BASE, params=params)
        resp.raise_for_status()
        data = resp.json()

        theses: list[NormalizedThesis] = []
        for raw in data.get("results") or []:
            try:
                t = self._normalize(raw, query)
                # Post-filter: only keep hardware/software relevant
                if t.hardware_relevant or t.software_relevant:
                    theses.append(t)
            except Exception as exc:
                log.warning("openalex: normalize error: %s", exc)
        return theses

    def _normalize(self, raw: dict, query: str) -> NormalizedThesis:
        # Reconstruct abstract from OpenAlex's inverted index format
        abstract = _reconstruct_abstract(raw.get("abstract_inverted_index"))

        # Primary author
        authorships = raw.get("authorships") or []
        author = None
        institution = None
        country = None
        if authorships:
            first = authorships[0]
            author = first.get("author", {}).get("display_name")
            insts = first.get("institutions") or []
            if insts:
                institution = insts[0].get("display_name")
                country = insts[0].get("country_code")

        # Keywords from OpenAlex concepts (ranked by relevance score)
        concepts = raw.get("concepts") or []
        keywords = [
            c["display_name"]
            for c in sorted(concepts, key=lambda x: x.get("score", 0), reverse=True)
            if c.get("score", 0) > 0.3
        ][:10]

        # Landing URL
        url = None
        primary = raw.get("primary_location") or {}
        url = primary.get("landing_page_url") or raw.get("open_access", {}).get("oa_url")

        openalex_id = raw.get("id", "").replace("https://openalex.org/", "")

        thesis = NormalizedThesis(
            source=self.name,
            source_id=openalex_id,
            title=self._truncate(raw.get("title", "Untitled"), 500),
            abstract=self._truncate(abstract),
            author=author,
            institution=institution,
            country=country,
            year=self._safe_year(raw.get("publication_year")),
            language=raw.get("language"),
            keywords=keywords,
            subjects=[c["display_name"] for c in concepts[:5]],
            url=url,
            doi=raw.get("doi"),
            degree="PhD",
            matched_query=query,
            raw_payload={
                "openalex_id": openalex_id,
                "open_access": raw.get("open_access", {}),
            },
        )
        return self._tag_relevance(thesis)


def _reconstruct_abstract(inverted_index: dict | None) -> str | None:
    """
    OpenAlex stores abstracts as inverted indexes: {word: [position, ...], ...}
    We reconstruct the original text by sorting words by their positions.
    """
    if not inverted_index:
        return None
    try:
        positions: dict[int, str] = {}
        for word, pos_list in inverted_index.items():
            for pos in pos_list:
                positions[pos] = word
        return " ".join(positions[i] for i in sorted(positions))
    except Exception:
        return None
