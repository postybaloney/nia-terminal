"""
DART-Europe ingestor.
Docs: https://www.dart-europe.org/basic-search.php
API:  http://www.dart-europe.eu/api/

DART-Europe provides access to 700k+ open-access European PhD theses
from 600+ universities in 28 countries.

Free, no API key required.
"""
from __future__ import annotations

import logging

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestors.theses.base import BaseThesisIngestor, NormalizedThesis

log = logging.getLogger(__name__)

_BASE = "http://www.dart-europe.eu/api/"

# DART-Europe country codes for reference (all included by default)
# DE, FR, GB, NL, SE, NO, FI, DK, PT, ES, IT, CH, AT, BE, CZ, PL, ...


class DARTEuropeIngestor(BaseThesisIngestor):
    name = "dart"

    async def fetch(self) -> list[NormalizedThesis]:
        results: list[NormalizedThesis] = []
        async with httpx.AsyncClient(timeout=30) as client:
            for query in self.queries:
                batch = await self._fetch_query(client, query)
                results.extend(batch)
                log.info("dart: query=%r fetched=%d", query, len(batch))
        return results

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    async def _fetch_query(
        self, client: httpx.AsyncClient, query: str
    ) -> list[NormalizedThesis]:
        params = {
            "q": query,
            "of": "json",
            "rg": self.per_page,
            "sf": "year",
            "so": "d",
            "fct__status": "full_text",  # only include theses with full text
        }
        try:
            resp = await client.get(_BASE, params=params)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log.warning("dart: request failed: %s", exc)
            return []

        theses: list[NormalizedThesis] = []
        records = data if isinstance(data, list) else data.get("records", [])
        for raw in records:
            try:
                t = self._normalize(raw, query)
                if t and (t.hardware_relevant or t.software_relevant):
                    theses.append(t)
            except Exception as exc:
                log.debug("dart: normalize error: %s", exc)
        return theses

    def _normalize(self, raw: dict, query: str) -> NormalizedThesis | None:
        title = raw.get("title_en") or raw.get("title") or ""
        if not title:
            return None

        year = self._safe_year(raw.get("year") or raw.get("date_defence", "")[:4])
        if year and year < self.since_year:
            return None

        abstract = (
            raw.get("abstract_en")
            or raw.get("abstract")
            or ""
        )

        # Author: DART returns "Firstname Lastname" or "Lastname, Firstname"
        author = raw.get("author_name") or raw.get("author")

        institution = raw.get("institution_name") or raw.get("university")
        country = raw.get("country_code") or raw.get("country")

        keywords_raw = raw.get("keywords_en") or raw.get("keywords") or []
        keywords = (
            [k.strip() for k in keywords_raw.split(";") if k.strip()]
            if isinstance(keywords_raw, str)
            else keywords_raw
        )

        source_id = str(raw.get("record_id") or raw.get("id") or title[:64])
        url = raw.get("url") or raw.get("full_text_url")

        thesis = NormalizedThesis(
            source=self.name,
            source_id=source_id,
            title=self._truncate(title, 500),
            abstract=self._truncate(abstract),
            author=author,
            institution=institution,
            country=country,
            year=year,
            language=raw.get("language"),
            keywords=keywords[:10],
            subjects=[],
            url=url,
            doi=raw.get("doi"),
            degree="PhD",
            matched_query=query,
            raw_payload={"record_id": source_id, "country": country},
        )
        return self._tag_relevance(thesis)
