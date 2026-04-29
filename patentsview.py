"""
USPTO patent ingestor — uses Lens.org with jurisdiction="US" filter.

Background
----------
The USPTO Open Data Portal (ODP) file-wrapper API returns title and CPC codes
but NO abstract (confirmed via live testing).  The ODP grants-search endpoint
requires a higher-tier API key (returns 403 with the free key).

Lens.org aggregates USPTO, EPO, WIPO and other offices and provides full-text
records (title + abstract + CPC) for US patents.  By filtering to jurisdiction
"US" we get clean USPTO records stored under source="patentsview" so they
appear as a distinct source in the dashboard.

Rate-limit note
---------------
Both LensIngestor and this ingestor share the Lens API.  The free tier allows
10 req/min combined.  Each ingestor sleeps 12 s between queries (5 req/min),
keeping total combined rate to ~10 req/min.
"""
from __future__ import annotations

import asyncio
import logging

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from config import settings
from ingestors.base import BaseIngestor, NormalizedPatent

log = logging.getLogger(__name__)

_BASE = "https://api.lens.org/patent/search"
_MAX_SIZE = 100          # Lens free-tier hard cap
_REQUEST_DELAY = 12.0    # seconds between queries (shared Lens rate limit)


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return isinstance(exc, (httpx.ConnectError, httpx.TimeoutException, httpx.RemoteProtocolError))


class PatentsViewIngestor(BaseIngestor):
    name = "patentsview"

    async def fetch(self) -> list[NormalizedPatent]:
        if not settings.lens_enabled:
            log.info("patentsview: skipped (LENS_API_KEY not configured — needed for US patent pull)")
            return []

        results: list[NormalizedPatent] = []
        async with httpx.AsyncClient(timeout=30) as client:
            for i, query_str in enumerate(self.queries):
                if i > 0:
                    await asyncio.sleep(_REQUEST_DELAY)
                try:
                    patents = await self._fetch_query(client, query_str)
                    results.extend(patents)
                    log.info("patentsview(us): query=%r  fetched=%d", query_str, len(patents))
                except Exception as exc:
                    log.warning("patentsview(us): query=%r  error=%s", query_str, exc)
        return results

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=2, max=15),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    async def _fetch_query(
        self, client: httpx.AsyncClient, query_str: str
    ) -> list[NormalizedPatent]:
        headers = {
            "Authorization": f"Bearer {settings.lens_api_key}",
            "Content-Type": "application/json",
        }
        size = min(self.per_page, _MAX_SIZE)
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "query_string": {
                                "query": query_str,
                                "fields": ["title", "abstract", "claim"],
                                "default_operator": "OR",
                            }
                        },
                        {
                            "range": {"date_published": {"gte": self.since}}
                        },
                        {
                            # Restrict to US patents only
                            "term": {"jurisdiction": "US"}
                        },
                    ]
                }
            },
            "size": size,
            "sort": [{"date_published": "desc"}],
            "include": [
                "lens_id",
                "doc_number",
                "jurisdiction",
                "date_published",
                "abstract",
                "biblio",
            ],
        }

        resp = await client.post(_BASE, headers=headers, json=body)
        if not resp.is_success:
            log.error(
                "patentsview(us): HTTP %d for query=%r — %s",
                resp.status_code, query_str, resp.text[:300],
            )
        resp.raise_for_status()
        data = resp.json()

        patents: list[NormalizedPatent] = []
        for raw in data.get("data") or []:
            try:
                patents.append(self._normalize(raw, query_str))
            except Exception as exc:
                log.warning("patentsview(us): normalize error: %s", exc)
        return patents

    @staticmethod
    def _first_text(items: list[dict], lang_pref: str = "EN") -> str | None:
        if not items:
            return None
        preferred = next(
            (i["text"] for i in items if i.get("lang", "").upper() == lang_pref),
            None,
        )
        return preferred or items[0].get("text")

    def _normalize(self, raw: dict, query: str) -> NormalizedPatent:
        biblio = raw.get("biblio") or {}

        # Use the US patent number (doc_number) as source_id so links go to
        # the correct USPTO/Google Patents page.
        doc_number = raw.get("doc_number") or raw.get("lens_id") or ""
        lens_id = raw.get("lens_id", "")

        title = self._first_text(biblio.get("invention_title") or [])
        abstract = self._first_text(raw.get("abstract") or [])

        # CPC codes
        cpc_raw = biblio.get("classifications_cpc") or {}
        cpc_codes = [
            c["symbol"]
            for c in (cpc_raw.get("classifications") or [])
            if c.get("symbol")
        ]

        # Assignees
        parties = biblio.get("parties") or {}
        assignees = [
            {
                "name": a.get("extracted_name", {}).get("value", ""),
                "country": a.get("residence", ""),
            }
            for a in (parties.get("applicants") or [])
            if a.get("extracted_name", {}).get("value")
        ]

        inventors = [
            {"name": i.get("extracted_name", {}).get("value", "")}
            for i in (parties.get("inventors") or [])
            if i.get("extracted_name", {}).get("value")
        ]

        return NormalizedPatent(
            source=self.name,
            source_id=doc_number,
            family_id=lens_id,          # use lens_id for family dedup with lens source
            title=self._truncate(title),
            abstract=self._truncate(abstract),
            filing_date=None,
            grant_date=self._safe_date(raw.get("date_published")),
            assignees=assignees,
            inventors=inventors,
            cpc_codes=cpc_codes,
            ipc_codes=[],
            matched_query=query,
            raw_payload={"lens_id": lens_id, "jurisdiction": "US"},
        )
