"""
EThOS (Electronic Theses Online Service) ingestor.
UK's national thesis service run by the British Library.
~600k theses from all UK universities.

Free to search. API key optional (register at https://ethos.bl.uk/Register.do
for higher rate limits).
"""
from __future__ import annotations

import logging

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestors.theses.base import BaseThesisIngestor, NormalizedThesis

log = logging.getLogger(__name__)

_BASE = "https://ethos.bl.uk/api/search"


class EThOSIngestor(BaseThesisIngestor):
    name = "ethos"

    def __init__(self, *args, ethos_api_key: str = "", **kwargs):
        super().__init__(*args, **kwargs)
        self.ethos_api_key = ethos_api_key

    async def fetch(self) -> list[NormalizedThesis]:
        results: list[NormalizedThesis] = []
        async with httpx.AsyncClient(timeout=30) as client:
            for query in self.queries:
                try:
                    batch = await self._fetch_query(client, query)
                    results.extend(batch)
                    log.info("ethos: query=%r fetched=%d", query, len(batch))
                except Exception as exc:
                    log.warning("ethos: query=%r error=%s", query, exc)
        return results

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    async def _fetch_query(
        self, client: httpx.AsyncClient, query: str
    ) -> list[NormalizedThesis]:
        params: dict = {
            "query": query,
            "pageSize": self.per_page,
            "pageNumber": 1,
            "sortField": "YEAR_OF_AWARD",
            "sortOrder": "DESC",
        }
        if self.ethos_api_key:
            params["apiKey"] = self.ethos_api_key

        try:
            resp = await client.get(_BASE, params=params)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log.warning("ethos: request failed: %s", exc)
            return []

        theses: list[NormalizedThesis] = []
        for raw in (data.get("data") or {}).get("records", []) or []:
            try:
                t = self._normalize(raw, query)
                if t and (t.hardware_relevant or t.software_relevant):
                    theses.append(t)
            except Exception as exc:
                log.debug("ethos: normalize error: %s", exc)
        return theses

    def _normalize(self, raw: dict, query: str) -> NormalizedThesis | None:
        title = raw.get("title", "")
        if not title:
            return None

        year = self._safe_year(raw.get("yearOfAward") or raw.get("year"))
        if year and year < self.since_year:
            return None

        abstract = raw.get("abstract") or raw.get("description") or ""

        author_obj = raw.get("author") or {}
        if isinstance(author_obj, dict):
            author = f"{author_obj.get('firstName', '')} {author_obj.get('lastName', '')}".strip()
        else:
            author = str(author_obj)

        institution = raw.get("institution") or raw.get("awardingInstitution")
        if isinstance(institution, dict):
            institution = institution.get("name")

        source_id = str(raw.get("id") or raw.get("eThosId") or title[:64])

        thesis = NormalizedThesis(
            source=self.name,
            source_id=source_id,
            title=self._truncate(title, 500),
            abstract=self._truncate(abstract) if abstract else None,
            author=author or None,
            institution=institution,
            country="GB",
            year=year,
            language="en",
            keywords=raw.get("keywords", [])[:10],
            subjects=raw.get("subjects", [])[:5],
            url=raw.get("url") or f"https://ethos.bl.uk/OrderDetails.do?uin={source_id}",
            doi=raw.get("doi"),
            degree=raw.get("qualificationName") or "PhD",
            matched_query=query,
            raw_payload={"ethos_id": source_id},
        )
        return self._tag_relevance(thesis)
