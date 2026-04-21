"""
USPTO Open Data Portal (ODP) patent ingestor.

Replaces the old PatentsView search.patentsview.org API, which was
permanently shut down during the March 2026 migration to ODP.

New API:  https://api.uspto.gov/api/v1/patent/applications/search
Docs:     https://data.uspto.gov/apis/patent-file-wrapper/search
Auth:     X-API-KEY header — register free at https://data.uspto.gov/apis/getting-started

Query format: Lucene boolean string via the `q` parameter.
  Supports AND, OR, NOT, wildcards (*), and exact phrases.
Date filter: rangeFilters=applicationMetaData.grantDate:[YYYY-MM-DD TO *]

Response field note: the ODP file-wrapper API returns patent metadata
(number, dates, applicant/inventor names) but does NOT return full
title or abstract text — those are available only via bulk download.
Keyword search via `q` still performs full-text matching server-side.

Register for an ODP API key:
  https://data.uspto.gov/apis/getting-started
Then add to your .env:
  USPTO_API_KEY=your_key_here
"""
from __future__ import annotations

import logging

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from config import settings
from ingestors.base import BaseIngestor, NormalizedPatent

log = logging.getLogger(__name__)

_BASE = "https://api.uspto.gov/api/v1/patent/applications/search"


class PatentsViewIngestor(BaseIngestor):
    name = "patentsview"

    async def fetch(self) -> list[NormalizedPatent]:
        if not settings.uspto_enabled:
            log.info(
                "patentsview: skipped (no USPTO_API_KEY configured). "
                "Register free at https://data.uspto.gov/apis/getting-started"
            )
            return []

        results: list[NormalizedPatent] = []
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                for query_str in self.queries:
                    patents = await self._fetch_query(client, query_str)
                    results.extend(patents)
                    log.info(
                        "patentsview: query=%r  fetched=%d", query_str, len(patents)
                    )
        except Exception as exc:
            log.warning("patentsview: fetch failed (%s)", exc)
        return results

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    async def _fetch_query(
        self, client: httpx.AsyncClient, query_str: str
    ) -> list[NormalizedPatent]:
        # Date filter must be embedded in the q string (Lucene syntax).
        # The rangeFilters param does not accept any known format — embed instead.
        # Sort uses space-separated "field direction" (colon format returns 400).
        dated_q = f"({query_str}) AND applicationMetaData.grantDate:[{self.since} TO *]"
        params = {
            "q": dated_q,
            "sort": "applicationMetaData.grantDate desc",
            "limit": self.per_page,
            "offset": 0,
            "fields": (
                "applicationNumberText,"
                "applicationMetaData.patentNumber,"
                "applicationMetaData.grantDate,"
                "applicationMetaData.filingDate,"
                "applicationMetaData.firstApplicantName,"
                "applicationMetaData.inventorBag,"
                "applicationMetaData.applicationTypeLabelName"
            ),
        }
        headers = {"X-API-KEY": settings.uspto_api_key}

        resp = await client.get(_BASE, params=params, headers=headers)
        resp.raise_for_status()
        data = resp.json()

        patents: list[NormalizedPatent] = []
        for record in data.get("patentFileWrapperDataBag") or []:
            try:
                patents.append(self._normalize(record, query_str))
            except Exception as exc:
                log.warning("patentsview: normalize error: %s", exc)
        return patents

    def _normalize(self, record: dict, query: str) -> NormalizedPatent:
        app_num = record.get("applicationNumberText", "")
        meta = record.get("applicationMetaData") or {}

        patent_number = meta.get("patentNumber") or app_num

        # Applicant (first assignee name only — ODP returns firstApplicantName)
        first_applicant = meta.get("firstApplicantName", "")
        assignees = [{"name": first_applicant, "country": ""}] if first_applicant else []

        # Inventors: applicationMetaData.inventorBag[].inventorNameText
        inventors = [
            {"name": inv.get("inventorNameText", "")}
            for inv in (meta.get("inventorBag") or [])
            if inv.get("inventorNameText")
        ]

        return NormalizedPatent(
            source=self.name,
            source_id=patent_number,
            family_id=None,         # ODP file-wrapper does not expose DOCDB family IDs
            title=None,             # not returned by the file-wrapper search endpoint
            abstract=None,          # not returned by the file-wrapper search endpoint
            filing_date=self._safe_date(meta.get("filingDate")),
            grant_date=self._safe_date(meta.get("grantDate")),
            assignees=assignees,
            inventors=inventors,
            cpc_codes=[],
            ipc_codes=[],
            matched_query=query,
            raw_payload={
                "applicationNumberText": app_num,
                "applicationTypeLabelName": meta.get("applicationTypeLabelName"),
            },
        )
