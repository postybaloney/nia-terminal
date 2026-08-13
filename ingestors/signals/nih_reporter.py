"""
NIH RePORTER ingestor — federal grant awards (signal_type="grant").

API: https://api.reporter.nih.gov/v2/projects/search  (POST, free, no key)
Docs: https://api.reporter.nih.gov

Grants are the earliest public signal in medtech/neurotech: an R01 or SBIR
award today is a product, paper, or startup 1–3 years out. SBIR/STTR awards
in particular flag commercial intent.

Rate-limit note: RePORTER asks for ~1 request/second. We sleep between
queries and never parallelize within this ingestor.
"""
from __future__ import annotations

import asyncio
import logging

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from ingestors.signals.base import BaseSignalIngestor, NormalizedSignal

log = logging.getLogger(__name__)

_BASE = "https://api.reporter.nih.gov/v2/projects/search"
_MAX_LIMIT = 500
_REQUEST_DELAY = 1.5  # seconds between queries


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500 or exc.response.status_code == 429
    return isinstance(exc, (httpx.ConnectError, httpx.TimeoutException, httpx.RemoteProtocolError))


class NIHReporterIngestor(BaseSignalIngestor):
    name = "nih_reporter"

    async def fetch(self) -> list[NormalizedSignal]:
        results: list[NormalizedSignal] = []
        async with httpx.AsyncClient(timeout=45) as client:
            for i, query_str in enumerate(self.queries):
                if i > 0:
                    await asyncio.sleep(_REQUEST_DELAY)
                try:
                    signals = await self._fetch_query(client, query_str)
                    results.extend(signals)
                    log.info("nih_reporter: query=%r  fetched=%d", query_str, len(signals))
                except Exception as exc:
                    log.warning("nih_reporter: query=%r  error=%s", query_str, exc)
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
        body = {
            "criteria": {
                "advanced_text_search": {
                    "operator": "and",
                    "search_field": "projecttitle,terms,abstracttext",
                    "search_text": query_str,
                },
                "project_start_date": {"from_date": self.since},
            },
            "include_fields": [
                "ApplId",
                "ProjectNum",
                "ProjectTitle",
                "AbstractText",
                "Organization",
                "PrincipalInvestigators",
                "AwardAmount",
                "ProjectStartDate",
                "ProjectEndDate",
                "FiscalYear",
                "ActivityCode",
            ],
            "limit": min(self.per_page, _MAX_LIMIT),
            "offset": 0,
            "sort_field": "project_start_date",
            "sort_order": "desc",
        }

        resp = await client.post(_BASE, json=body)
        if not resp.is_success:
            log.error(
                "nih_reporter: HTTP %d for query=%r — %s",
                resp.status_code, query_str, resp.text[:300],
            )
        resp.raise_for_status()
        data = resp.json()

        signals: list[NormalizedSignal] = []
        for raw in data.get("results") or []:
            try:
                signals.append(self._normalize(raw, query_str))
            except Exception as exc:
                log.warning("nih_reporter: normalize error: %s", exc)
        return signals

    def _normalize(self, raw: dict, query: str) -> NormalizedSignal:
        appl_id = str(raw.get("appl_id") or raw.get("project_num") or "")
        org = (raw.get("organization") or {}).get("org_name")

        people = []
        for pi in raw.get("principal_investigators") or []:
            name = pi.get("full_name") or " ".join(
                x for x in [pi.get("first_name"), pi.get("last_name")] if x
            )
            if name:
                people.append({"name": name.strip(), "role": "PI"})

        amount = raw.get("award_amount")
        activity = raw.get("activity_code") or ""
        tags = [t for t in [activity, f"FY{raw.get('fiscal_year')}" if raw.get("fiscal_year") else None] if t]

        return NormalizedSignal(
            source=self.name,
            source_id=appl_id,
            signal_type="grant",
            title=self._truncate(raw.get("project_title"), 1000),
            summary=self._truncate(raw.get("abstract_text")),
            organization=org,
            people=people,
            amount=int(amount) if amount else None,
            event_date=self._safe_date(raw.get("project_start_date")),
            status=activity or None,   # R01 / R44 / U01 etc. — SBIR codes flag commercial intent
            tags=tags,
            url=f"https://reporter.nih.gov/project-details/{appl_id}" if appl_id else None,
            matched_query=query,
            raw_payload={
                "project_num": raw.get("project_num"),
                "fiscal_year": raw.get("fiscal_year"),
                "project_end_date": raw.get("project_end_date"),
            },
        )
