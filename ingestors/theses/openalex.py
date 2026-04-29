"""
OpenAlex thesis ingestor.
Docs: https://docs.openalex.org

OpenAlex is a completely free, open index of 250M+ scholarly works.
It covers dissertations from institutions worldwide.
No API key required (but add your email as OPENALEX_EMAIL env var
to join the polite pool for higher rate limits).

Rate limit: 100k requests/day (polite pool with email header).
"""
from __future__ import annotations

import logging
import os
import urllib.parse

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestors.theses.base import BaseThesisIngestor, NormalizedThesis, is_relevant

log = logging.getLogger(__name__)

_BASE = "https://api.openalex.org/works"
# Add your email as OPENALEX_EMAIL env var to join the polite pool
_EMAIL = os.environ.get("OPENALEX_EMAIL", "")


class OpenAlexIngestor(BaseThesisIngestor):
    name = "openalex"

    async def fetch(self) -> list[NormalizedThesis]:
        results: list[NormalizedThesis] = []
        async with httpx.AsyncClient(timeout=30) as client:
            for query in self.queries:
                try:
                    batch = await self._fetch_query(client, query)
                    results.extend(batch)
                    log.info(
                        "openalex: query=%r fetched=%d relevant=%d",
                        query, len(batch),
                        sum(1 for t in batch if t.hardware_relevant or t.software_relevant),
                    )
                except Exception as exc:
                    log.warning("openalex: query=%r error=%s", query, exc)
        return results

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    async def _fetch_query(
        self, client: httpx.AsyncClient, query: str
    ) -> list[NormalizedThesis]:
        params: dict = {
            "search": query,
            "filter": f"type:dissertation,publication_year:>{self.since_year - 1}",
            "per-page": min(self.per_page, 200),
            "sort": "publication_year:desc",
            "select": ",".join([
                "id", "title", "abstract_inverted_index", "publication_year",
                "authorships", "keywords", "concepts",
                "doi", "primary_location", "language", "open_access",
            ]),
        }
        if _EMAIL:
            params["mailto"] = _EMAIL

        resp = await client.get(_BASE, params=params)
        resp.raise_for_status()
        data = resp.json()

        theses: list[NormalizedThesis] = []
        for raw in data.get("results") or []:
            try:
                t = self._normalize(raw, query)
                if t.hardware_relevant or t.software_relevant:
                    theses.append(t)
            except Exception as exc:
                log.warning("openalex: normalize error: %s", exc)
        return theses

    def _normalize(self, raw: dict, query: str) -> NormalizedThesis:
        abstract = _reconstruct_abstract(raw.get("abstract_inverted_index"))

        authorships = raw.get("authorships") or []
        author = None
        institution = None
        country = None
        author_openalex_url = None
        author_orcid = None
        if authorships:
            first = authorships[0]
            author_obj = first.get("author") or {}
            author = author_obj.get("display_name")
            author_orcid = author_obj.get("orcid")  # full URL e.g. https://orcid.org/...
            author_oa_id = author_obj.get("id", "")  # full URL e.g. https://openalex.org/A...
            if author_oa_id:
                author_openalex_url = author_oa_id  # already a full URL
            insts = first.get("institutions") or []
            if insts:
                institution = insts[0].get("display_name")
                country = insts[0].get("country_code")

        concepts = raw.get("concepts") or []
        keywords = [
            c["display_name"]
            for c in sorted(concepts, key=lambda x: x.get("score", 0), reverse=True)
            if c.get("score", 0) > 0.3
        ][:10]

        doi = raw.get("doi")
        openalex_id = raw.get("id", "").replace("https://openalex.org/", "")

        # URL priority: landing page → DOI → OA PDF → OpenAlex record (always valid)
        primary = raw.get("primary_location") or {}
        url = (
            primary.get("landing_page_url")
            or (f"https://doi.org/{doi}" if doi else None)
            or (raw.get("open_access") or {}).get("oa_url")
            or f"https://openalex.org/{openalex_id}"
        )

        title = raw.get("title", "Untitled") or "Untitled"

        thesis = NormalizedThesis(
            source=self.name,
            source_id=openalex_id,
            title=self._truncate(title, 500),
            abstract=self._truncate(abstract),
            author=author,
            institution=institution,
            country=country,
            year=self._safe_year(raw.get("publication_year")),
            language=raw.get("language"),
            keywords=keywords,
            subjects=[c["display_name"] for c in concepts[:5]],
            url=url,
            doi=doi,
            degree="PhD",
            matched_query=query,
            raw_payload={
                "openalex_id": openalex_id,
                "open_access": raw.get("open_access", {}),
                "author_openalex_url": author_openalex_url,
                "author_orcid": author_orcid,
            },
        )
        return self._tag_relevance(thesis)


def _reconstruct_abstract(inverted_index: dict | None) -> str | None:
    """
    OpenAlex stores abstracts as inverted indexes: {word: [position, ...], ...}
    Reconstruct original text by sorting words by their positions.
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
