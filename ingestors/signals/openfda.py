"""
openFDA device ingestor — 510(k) clearances and PMA approvals
(signal_type="clearance" / "approval").

APIs (GET, free; optional API key raises rate limits):
  https://api.fda.gov/device/510k.json
  https://api.fda.gov/device/pma.json
Docs: https://open.fda.gov/apis/device/

An FDA clearance is the moment a device becomes sellable — the single
highest-value current signal for BD teams and investors.

Quirk: openFDA returns HTTP 404 (NOT_FOUND) when a search matches zero
records. We treat 404 as an empty result, not an error.

Without an API key openFDA allows 1,000 requests/day per IP — far more
than a daily run needs. Set OPENFDA_API_KEY to lift limits.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from config import settings
from ingestors.signals.base import BaseSignalIngestor, NormalizedSignal

log = logging.getLogger(__name__)

_BASE_510K = "https://api.fda.gov/device/510k.json"
_BASE_PMA = "https://api.fda.gov/device/pma.json"
_MAX_LIMIT = 100
_REQUEST_DELAY = 1.0


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500 or exc.response.status_code == 429
    return isinstance(exc, (httpx.ConnectError, httpx.TimeoutException, httpx.RemoteProtocolError))


class OpenFDAIngestor(BaseSignalIngestor):
    name = "openfda"

    async def fetch(self) -> list[NormalizedSignal]:
        results: list[NormalizedSignal] = []
        today = date.today().isoformat()
        async with httpx.AsyncClient(timeout=45) as client:
            for i, query_str in enumerate(self.queries):
                if i > 0:
                    await asyncio.sleep(_REQUEST_DELAY)
                try:
                    cleared = await self._fetch_endpoint(
                        client, _BASE_510K,
                        f'device_name:"{query_str}" AND decision_date:[{self.since} TO {today}]',
                        query_str, self._normalize_510k,
                    )
                    results.extend(cleared)
                    await asyncio.sleep(_REQUEST_DELAY)
                    approved = await self._fetch_endpoint(
                        client, _BASE_PMA,
                        f'(trade_name:"{query_str}" OR generic_name:"{query_str}") AND decision_date:[{self.since} TO {today}]',
                        query_str, self._normalize_pma,
                    )
                    results.extend(approved)
                    log.info(
                        "openfda: query=%r  510k=%d  pma=%d",
                        query_str, len(cleared), len(approved),
                    )
                except Exception as exc:
                    log.warning("openfda: query=%r  error=%s", query_str, exc)
        return results

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=2, max=15),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    async def _fetch_endpoint(
        self, client: httpx.AsyncClient, base: str, search: str, query_str: str, normalize
    ) -> list[NormalizedSignal]:
        params = {"search": search, "limit": min(self.per_page, _MAX_LIMIT)}
        api_key = getattr(settings, "openfda_api_key", "")
        if api_key:
            params["api_key"] = api_key

        resp = await client.get(base, params=params)
        if resp.status_code == 404:
            return []          # openFDA's "no matches" response
        if not resp.is_success:
            log.error(
                "openfda: HTTP %d for search=%r — %s",
                resp.status_code, search, resp.text[:300],
            )
        resp.raise_for_status()
        data = resp.json()

        signals: list[NormalizedSignal] = []
        for raw in data.get("results") or []:
            try:
                signals.append(normalize(raw, query_str))
            except Exception as exc:
                log.warning("openfda: normalize error: %s", exc)
        return signals

    def _normalize_510k(self, raw: dict, query: str) -> NormalizedSignal:
        k_number = raw.get("k_number") or ""
        return NormalizedSignal(
            source="fda_510k",
            source_id=k_number,
            signal_type="clearance",
            title=self._truncate(raw.get("device_name"), 1000),
            summary=self._truncate(raw.get("statement_or_summary")),
            organization=raw.get("applicant"),
            people=[],
            amount=None,
            event_date=self._safe_date(raw.get("decision_date")),
            status=raw.get("decision_description") or raw.get("decision_code"),
            tags=[t for t in [raw.get("product_code"), raw.get("advisory_committee_description")] if t],
            url=(
                "https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpmn/pmn.cfm"
                f"?ID={k_number}" if k_number else None
            ),
            matched_query=query,
            raw_payload={"clearance_type": raw.get("clearance_type")},
        )

    def _normalize_pma(self, raw: dict, query: str) -> NormalizedSignal:
        pma_number = raw.get("pma_number") or ""
        supplement = raw.get("supplement_number") or ""
        source_id = f"{pma_number}{('/' + supplement) if supplement else ''}"
        return NormalizedSignal(
            source="fda_pma",
            source_id=source_id,
            signal_type="approval",
            title=self._truncate(raw.get("trade_name") or raw.get("generic_name"), 1000),
            summary=self._truncate(raw.get("ao_statement")),
            organization=raw.get("applicant"),
            people=[],
            amount=None,
            event_date=self._safe_date(raw.get("decision_date")),
            status=raw.get("decision_code"),
            tags=[t for t in [raw.get("product_code"), raw.get("generic_name")] if t],
            url=(
                "https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpma/pma.cfm"
                f"?id={pma_number}" if pma_number else None
            ),
            matched_query=query,
            raw_payload={"supplement_number": supplement},
        )
