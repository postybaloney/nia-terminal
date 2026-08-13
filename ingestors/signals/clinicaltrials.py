"""
ClinicalTrials.gov ingestor — newly registered studies (signal_type="trial").

API v2: https://clinicaltrials.gov/api/v2/studies  (GET, free, no key)
Docs:   https://clinicaltrials.gov/data-api/api

A new device trial is the clearest "this company is spending real money"
signal between grant and clearance. We filter to studies whose start date
falls after `since`.
"""
from __future__ import annotations

import asyncio
import logging

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from ingestors.signals.base import BaseSignalIngestor, NormalizedSignal

log = logging.getLogger(__name__)

_BASE = "https://clinicaltrials.gov/api/v2/studies"
_MAX_PAGE = 100
_REQUEST_DELAY = 1.0


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500 or exc.response.status_code == 429
    return isinstance(exc, (httpx.ConnectError, httpx.TimeoutException, httpx.RemoteProtocolError))


class ClinicalTrialsIngestor(BaseSignalIngestor):
    name = "clinicaltrials"

    async def fetch(self) -> list[NormalizedSignal]:
        results: list[NormalizedSignal] = []
        async with httpx.AsyncClient(timeout=45) as client:
            for i, query_str in enumerate(self.queries):
                if i > 0:
                    await asyncio.sleep(_REQUEST_DELAY)
                try:
                    signals = await self._fetch_query(client, query_str)
                    results.extend(signals)
                    log.info("clinicaltrials: query=%r  fetched=%d", query_str, len(signals))
                except Exception as exc:
                    log.warning("clinicaltrials: query=%r  error=%s", query_str, exc)
        return results

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=2, max=15),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    async def _fetch_query(
        self, client: httpx.AsyncClient, query_str: str
    ) -> list[NormalizedSignal]:
        params = {
            "query.term": query_str,
            "filter.advanced": f"AREA[StartDate]RANGE[{self.since},MAX]",
            "pageSize": min(self.per_page, _MAX_PAGE),
        }
        resp = await client.get(_BASE, params=params)
        if not resp.is_success:
            log.error(
                "clinicaltrials: HTTP %d for query=%r — %s",
                resp.status_code, query_str, resp.text[:300],
            )
        resp.raise_for_status()
        data = resp.json()

        signals: list[NormalizedSignal] = []
        for study in data.get("studies") or []:
            try:
                signals.append(self._normalize(study, query_str))
            except Exception as exc:
                log.warning("clinicaltrials: normalize error: %s", exc)
        return signals

    def _normalize(self, study: dict, query: str) -> NormalizedSignal:
        proto = study.get("protocolSection") or {}
        ident = proto.get("identificationModule") or {}
        status_mod = proto.get("statusModule") or {}
        sponsor_mod = proto.get("sponsorCollaboratorsModule") or {}
        desc = proto.get("descriptionModule") or {}
        design = proto.get("designModule") or {}
        cond = proto.get("conditionsModule") or {}

        nct_id = ident.get("nctId") or ""
        start = (status_mod.get("startDateStruct") or {}).get("date")
        sponsor = (sponsor_mod.get("leadSponsor") or {}).get("name")
        phases = design.get("phases") or []
        conditions = cond.get("conditions") or []

        return NormalizedSignal(
            source=self.name,
            source_id=nct_id,
            signal_type="trial",
            title=self._truncate(ident.get("briefTitle"), 1000),
            summary=self._truncate(desc.get("briefSummary")),
            organization=sponsor,
            people=[],
            amount=None,
            event_date=self._safe_date(start),
            status=status_mod.get("overallStatus"),
            tags=list(conditions[:10]) + list(phases) + ([design.get("studyType")] if design.get("studyType") else []),
            url=f"https://clinicaltrials.gov/study/{nct_id}" if nct_id else None,
            matched_query=query,
            raw_payload={
                "organization": (ident.get("organization") or {}).get("fullName"),
                "studyType": design.get("studyType"),
            },
        )
