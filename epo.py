"""
EPO Open Patent Services (OPS) ingestor.
Docs: https://developers.epo.org/ops-v3-2/apis

Covers European, PCT, and 100+ national patent offices.
Requires free registration at https://developers.epo.org to obtain
client_id and client_secret. Rate limit: 4 req/sec, 2.5 GB/week.

Response format: XML (Bibliographic Data Service). Parsed with lxml.

────────────────────────────────────────────────────────────────────────────
Rewritten 2026-08-18 to fix two defects found in the 2026-08-17 run:

  1. PRECISION. The old query builder did:
         terms = " OR ".join(f'ta="{w}"' for w in query_str.split()[:5])
     which shattered a multi-word query into independent single-word ORs and
     discarded everything past the 5th word, producing:
         (ta="neural" OR ta="stimulation" OR ta="brain" ...) AND pd>=...
     That matches any document containing the word "neural" — including every
     "neural network" patent in existence. It is why a robot-vacuum maker
     (BEIJING ROBOROCK) and a surgical-stapler maker (CILAG) ranked as top
     "neurotech" assignees. Queries now come from neuro_taxonomy.NEURO_QUERIES
     as quoted phrases narrowed by CPC classification.

  2. PARTIAL-FAILURE DATA LOSS. fetch() looped queries with no per-query error
     handling. On 2026-08-17 the 12th query returned 403; the exception escaped
     fetch(), pipeline.py caught it at the asyncio.gather boundary, and all 550
     records already fetched by the first 11 queries were discarded — hence
     "total fetched: 0" printed directly beneath eleven "fetched=50" lines.
     Each query is now isolated: one failure costs one query, never the run.

Also added: real 403 handling. OPS signals throttling and quota state in an
X-Rejection-Reason header. Blind-retrying a quota rejection three times (which
the old @retry decorator did) makes throttling strictly worse and burns the
weekly allowance faster.
────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from io import BytesIO

import httpx
from lxml import etree

from config import settings
from ingestors.base import BaseIngestor, NormalizedPatent
from neuro_taxonomy import NEURO_QUERIES, NeuroQuery

log = logging.getLogger(__name__)

_TOKEN_URL = "https://ops.epo.org/3.2/auth/accesstoken"
_SEARCH_URL = "https://ops.epo.org/3.2/rest-services/published-data/search/biblio"
_NS = {
    "ops": "http://ops.epo.org",
    "epo": "http://www.epo.org/exchange",
    "atom": "http://www.w3.org/2005/Atom",
}

# X-Rejection-Reason values that mean "stop asking" rather than "try again".
_QUOTA_REASONS = {
    "registeredquotaperweek",
    "individualquotaperhour",
    "anonymousquotaperminute",
    "quotaexceeded",
}
# Values that mean "slow down" — worth a backoff and one more attempt.
_THROTTLE_REASONS = {"individualquotaperminute", "systemquotaperminute", "throttling"}


class EPOQuotaExceeded(RuntimeError):
    """Raised when OPS reports the weekly/hourly allowance is spent."""


class EPOIngestor(BaseIngestor):
    name = "epo"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._token: str | None = None
        self._token_expires: datetime = datetime.now(timezone.utc)
        # Populated per run so the pipeline can report what actually happened
        # instead of silently returning a short list.
        self.query_errors: list[str] = []

    # ── public ───────────────────────────────────────────────────────────────

    async def fetch(self) -> list[NormalizedPatent]:
        if not settings.epo_enabled:
            log.info("epo: skipped (no credentials configured)")
            return []

        results: list[NormalizedPatent] = []
        self.query_errors = []

        async with httpx.AsyncClient(timeout=30) as client:
            try:
                await self._ensure_token(client)
            except Exception as exc:
                log.error("epo: authentication failed: %s", exc)
                self.query_errors.append(f"auth: {exc}")
                return []

            for nq in self._queries():
                try:
                    patents = await self._fetch_query(client, nq)
                except EPOQuotaExceeded as exc:
                    # Quota is a run-level condition, not a query-level one.
                    # Stop cleanly and KEEP everything gathered so far.
                    log.warning(
                        "epo: quota exhausted at query %r — stopping early, "
                        "keeping %d records already fetched",
                        nq.label, len(results),
                    )
                    self.query_errors.append(f"{nq.label}: quota exhausted ({exc})")
                    break
                except Exception as exc:
                    # THE 2026-08-17 FIX: one bad query must never zero the run.
                    log.warning(
                        "epo: query %r failed (%s: %s) — continuing with %d "
                        "queries remaining",
                        nq.label, type(exc).__name__, exc,
                        len(self._queries()) - self._queries().index(nq) - 1,
                    )
                    self.query_errors.append(f"{nq.label}: {type(exc).__name__}: {exc}")
                    continue

                results.extend(patents)
                log.info("epo: query=%s  fetched=%d", nq.label, len(patents))
                # OPS allows 4 req/sec; stay well under it.
                await asyncio.sleep(0.4)

        if self.query_errors:
            log.info(
                "epo: completed with %d/%d queries failing — %d records retained",
                len(self.query_errors), len(self._queries()), len(results),
            )
        return results

    # ── internals ────────────────────────────────────────────────────────────

    def _queries(self) -> list[NeuroQuery]:
        """
        Precise, curated queries from the taxonomy.

        settings.query_list (free-text strings) is deliberately NOT used here
        any more — it is what produced the single-word OR explosion. It remains
        in config for the other ingestors and for reference.
        """
        return list(NEURO_QUERIES)

    async def _ensure_token(self, client: httpx.AsyncClient) -> None:
        """OAuth2 client-credentials token, refreshed before expiry."""
        if self._token and datetime.now(timezone.utc) < self._token_expires:
            return
        resp = await client.post(
            _TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=(settings.epo_client_id, settings.epo_client_secret),
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._token_expires = datetime.now(timezone.utc) + timedelta(
            seconds=int(data.get("expires_in", 1200)) - 60
        )

    async def _fetch_query(
        self, client: httpx.AsyncClient, nq: NeuroQuery, _attempt: int = 0
    ) -> list[NormalizedPatent]:
        cql = nq.epo_cql(self.since)
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/xml",
        }
        params = {"q": cql, "Range": f"1-{min(self.per_page, 100)}"}

        resp = await client.get(_SEARCH_URL, headers=headers, params=params)

        if resp.status_code == 404:
            return []  # no results for this query — normal, not an error

        if resp.status_code == 403:
            reason = (resp.headers.get("X-Rejection-Reason") or "").strip().lower()

            if reason in _QUOTA_REASONS:
                raise EPOQuotaExceeded(reason or "unspecified")

            if reason in _THROTTLE_REASONS and _attempt < 2:
                wait = 5 * (_attempt + 1)
                log.info("epo: throttled (%s) — backing off %ds", reason, wait)
                await asyncio.sleep(wait)
                return await self._fetch_query(client, nq, _attempt + 1)

            # A 403 with no rejection reason is usually an expired/invalid
            # token. Refresh once, then give up on this query.
            if _attempt < 1:
                log.info("epo: 403 (reason=%r) — refreshing token and retrying once",
                         reason or "none")
                self._token = None
                self._token_expires = datetime.now(timezone.utc)
                await self._ensure_token(client)
                return await self._fetch_query(client, nq, _attempt + 1)

            raise httpx.HTTPStatusError(
                f"403 from OPS (X-Rejection-Reason={reason or 'none'})",
                request=resp.request, response=resp,
            )

        if resp.status_code in (500, 502, 503, 504) and _attempt < 2:
            wait = 3 * (_attempt + 1)
            log.info("epo: %d from OPS — retrying in %ds", resp.status_code, wait)
            await asyncio.sleep(wait)
            return await self._fetch_query(client, nq, _attempt + 1)

        resp.raise_for_status()
        return self._parse_xml(resp.content, nq.label)

    # ── parsing (unchanged behaviour, only the query label differs) ──────────

    def _parse_xml(self, xml_bytes: bytes, query: str) -> list[NormalizedPatent]:
        tree = etree.parse(BytesIO(xml_bytes))
        root = tree.getroot()
        patents: list[NormalizedPatent] = []

        for doc in root.findall(".//epo:exchange-document", _NS):
            try:
                patents.append(self._parse_doc(doc, query))
            except Exception as exc:
                log.warning("epo: failed to parse doc: %s", exc)

        return patents

    def _parse_doc(self, doc: etree._Element, query: str) -> NormalizedPatent:
        def text(xpath: str) -> str | None:
            el = doc.find(xpath, _NS)
            return el.text.strip() if el is not None and el.text else None

        def texts(xpath: str) -> list[str]:
            return [
                el.text.strip()
                for el in doc.findall(xpath, _NS)
                if el.text and el.text.strip()
            ]

        doc_id = doc.get("doc-number", "")
        country = doc.get("country", "")
        kind = doc.get("kind", "")
        source_id = f"{country}{doc_id}{kind}"
        family_id = doc.get("family-id")

        abstract = None
        for abs_el in doc.findall(".//epo:abstract", _NS):
            if abs_el.get("lang", "").lower() in ("en", ""):
                abstract = " ".join(
                    p.text or "" for p in abs_el.findall("epo:p", _NS)
                ).strip()
                break

        title = None
        for t_el in doc.findall(".//epo:invention-title", _NS):
            if t_el.get("lang", "").lower() in ("en", ""):
                title = t_el.text
                break

        filing_date = self._safe_date(text(".//epo:application-reference//epo:date"))
        pub_date = self._safe_date(text(".//epo:publication-reference//epo:date"))

        assignees = []
        for party in doc.findall(".//epo:applicant", _NS):
            name_el = party.find(".//epo:name", _NS)
            country_el = party.find(".//epo:country", _NS)
            if name_el is not None and name_el.text:
                assignees.append({
                    "name": name_el.text.strip(),
                    "country": (
                        country_el.text.strip()
                        if country_el is not None and country_el.text else ""
                    ),
                })

        inventors = []
        for inv in doc.findall(".//epo:inventor", _NS):
            name_el = inv.find(".//epo:name", _NS)
            if name_el is not None and name_el.text:
                inventors.append({"name": name_el.text.strip()})

        ipc_codes = texts(".//epo:classification-ipc//epo:text")
        cpc_codes = [
            el.text.strip()
            for el in doc.findall(".//epo:patent-classification//epo:symbol", _NS)
            if el.text
        ]

        return NormalizedPatent(
            source=self.name,
            source_id=source_id,
            family_id=family_id,
            title=self._truncate(title),
            abstract=self._truncate(abstract),
            filing_date=filing_date,
            grant_date=pub_date,
            assignees=assignees,
            inventors=inventors,
            cpc_codes=cpc_codes,
            ipc_codes=ipc_codes,
            matched_query=query,
            raw_payload={"doc_id": source_id, "family_id": family_id},
        )
