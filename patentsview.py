"""
USPTO patent ingestor — uses the Enhanced Full-Text Search (EFTS) system
at efts.uspto.gov, which is the Elasticsearch backend powering
patent.uspto.gov/patents/search.

Returns:  title, abstract, CPC codes, grant/filing dates, assignees.
Auth:     none required (public endpoint).
Rate:     informal — we add a small inter-request delay to be respectful.

The old ODP file-wrapper endpoint (api.uspto.gov/api/v1/patent/applications/search)
was replaced here because it returns metadata ONLY (no title, no abstract, no CPC).
EFTS gives us the full record.
"""
from __future__ import annotations

import asyncio
import logging

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from config import settings
from ingestors.base import BaseIngestor, NormalizedPatent

log = logging.getLogger(__name__)

_EFTS_URL = "https://efts.uspto.gov/LATEST/search-views/all"

# EFTS imposes informal rate limits — 1 req/s is safe.
_REQUEST_DELAY = 1.1  # seconds between queries


class PatentsViewIngestor(BaseIngestor):
    name = "patentsview"

    async def fetch(self) -> list[NormalizedPatent]:
        results: list[NormalizedPatent] = []
        async with httpx.AsyncClient(timeout=30) as client:
            for i, query_str in enumerate(self.queries):
                if i > 0:
                    await asyncio.sleep(_REQUEST_DELAY)
                try:
                    patents = await self._fetch_query(client, query_str)
                    results.extend(patents)
                    log.info("patentsview(efts): query=%r  fetched=%d", query_str, len(patents))
                except Exception as exc:
                    log.warning("patentsview(efts): query=%r  error=%s", query_str, exc)
        return results

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=15))
    async def _fetch_query(
        self, client: httpx.AsyncClient, query_str: str
    ) -> list[NormalizedPatent]:
        params = {
            "q": query_str,
            "dateRangeData": f"grantDate:[{self.since} TO *]",
            "hits": min(self.per_page, 500),
            "offset": 0,
            "searchType": 1,
        }
        headers = {
            "Accept": "application/json",
            "User-Agent": "patent-intel-research/1.0",
        }

        resp = await client.get(_EFTS_URL, params=params, headers=headers)
        resp.raise_for_status()
        data = resp.json()

        hits = (data.get("hits") or {}).get("hits") or []
        patents: list[NormalizedPatent] = []
        for hit in hits:
            try:
                src = hit.get("_source") or {}
                p = self._normalize(src, query_str)
                # Quality gate: skip records without both title and abstract
                if not p.title and not p.abstract:
                    log.debug("patentsview(efts): skipping %s — no title or abstract", p.source_id)
                    continue
                patents.append(p)
            except Exception as exc:
                log.warning("patentsview(efts): normalize error: %s — hit: %s", exc, hit.get("_id"))
        return patents

    def _normalize(self, src: dict, query: str) -> NormalizedPatent:
        # EFTS field names — handle minor variations across API versions
        patent_number = (
            src.get("patentNumber")
            or src.get("patent_number")
            or src.get("_id")
            or ""
        )
        title = (
            src.get("inventionTitle")
            or src.get("patent_title")
            or src.get("title")
            or None
        )
        abstract = (
            src.get("abstractText")
            or src.get("abstract_text")
            or src.get("abstract")
            or None
        )

        # CPC codes — EFTS may return a string or a list
        raw_cpc = src.get("cpcCode") or src.get("cpc_group_id") or src.get("cpc") or []
        if isinstance(raw_cpc, str):
            raw_cpc = [raw_cpc]
        cpc_codes = [c.strip() for c in raw_cpc if c and c.strip()]

        # Assignees
        raw_assignees = src.get("assigneeEntityName") or src.get("assignee_organization") or []
        if isinstance(raw_assignees, str):
            raw_assignees = [raw_assignees]
        assignees = [{"name": a, "country": ""} for a in raw_assignees if a]

        # Inventors
        raw_inventors = src.get("inventorName") or []
        if isinstance(raw_inventors, str):
            raw_inventors = [raw_inventors]
        inventors = [{"name": n} for n in raw_inventors if n]

        return NormalizedPatent(
            source=self.name,
            source_id=patent_number,
            family_id=None,
            title=title,
            abstract=abstract,
            filing_date=self._safe_date(
                src.get("filingDate") or src.get("filing_date") or src.get("app_date")
            ),
            grant_date=self._safe_date(
                src.get("grantDate") or src.get("grant_date") or src.get("patent_date")
            ),
            assignees=assignees,
            inventors=inventors,
            cpc_codes=cpc_codes,
            ipc_codes=[],
            matched_query=query,
            raw_payload={"patentNumber": patent_number},
        )
