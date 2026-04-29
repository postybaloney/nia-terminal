"""
Lens.org patent ingestor.
API docs: https://docs.api.lens.org/request-patent.html
Response schema: https://docs.api.lens.org/response-patent.html

Aggregates USPTO, EPO, WIPO, CNIPA, and others.
Key advantage: links patents to citing academic literature via NPL citations —
essential for neurotech where university research transitions directly to IP.

Free tier: 10 req/min, 500 results/page, 50k requests/day.
API key required (free at https://www.lens.org/lens/user/subscriptions).

Response field notes (v1.6.5 schema):
  - title     → biblio.invention_title  (list of {text, lang})
  - abstract  → top-level abstract      (list of {text, lang})
  - applicants→ biblio.parties.applicants[].extracted_name.value
  - inventors → biblio.parties.inventors[].extracted_name.value
  - CPC codes → biblio.classifications_cpc.classifications[].symbol
  - IPC codes → biblio.classifications_ipcr.classifications[].symbol
  - NPL cites → biblio.references_cited.citations[].nplcit.text
  - families  → families.simple_family.members[]  (no direct DOCDB family_id)
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
_MAX_SIZE = 100          # Lens free tier hard cap — requests with size>100 return 400
_REQUEST_DELAY = 6.5     # seconds between queries (free tier: 10 req/min)


def _is_retryable(exc: BaseException) -> bool:
    """Only retry on network errors and 5xx responses — never on 400/401/403."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return isinstance(exc, (httpx.ConnectError, httpx.TimeoutException, httpx.RemoteProtocolError))


class LensIngestor(BaseIngestor):
    name = "lens"

    async def fetch(self) -> list[NormalizedPatent]:
        if not settings.lens_enabled:
            log.info("lens: skipped (no API key configured)")
            return []

        results: list[NormalizedPatent] = []
        async with httpx.AsyncClient(timeout=30) as client:
            for i, query_str in enumerate(self.queries):
                if i > 0:
                    await asyncio.sleep(_REQUEST_DELAY)
                try:
                    patents = await self._fetch_query(client, query_str)
                    results.extend(patents)
                    log.info("lens: query=%r  fetched=%d", query_str, len(patents))
                except Exception as exc:
                    log.warning("lens: query=%r  error=%s", query_str, exc)
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
        size = min(self.per_page, _MAX_SIZE)   # never exceed Lens free-tier cap
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
                            "range": {
                                "date_published": {"gte": self.since}
                            }
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
                "publication_type",
                "date_published",
                "abstract",
                "biblio",
                "families",
            ],
        }

        resp = await client.post(_BASE, headers=headers, json=body)
        if not resp.is_success:
            log.error(
                "lens: HTTP %d for query=%r — %s",
                resp.status_code, query_str, resp.text[:300],
            )
        resp.raise_for_status()
        data = resp.json()

        patents: list[NormalizedPatent] = []
        for raw in data.get("data") or []:
            try:
                patents.append(self._normalize(raw, query_str))
            except Exception as exc:
                log.warning("lens: normalize error: %s — record: %s", exc, raw.get("lens_id"))

        return patents

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _first_text(items: list[dict], lang_pref: str = "EN") -> str | None:
        """Return the text field of the first item matching lang_pref, or the
        first item regardless of language, or None if the list is empty."""
        if not items:
            return None
        preferred = next(
            (i["text"] for i in items if i.get("lang", "").upper() == lang_pref),
            None,
        )
        return preferred or items[0].get("text")

    # ── normalization ─────────────────────────────────────────────────────────

    def _normalize(self, raw: dict, query: str) -> NormalizedPatent:
        lens_id = raw.get("lens_id", "")
        biblio = raw.get("biblio") or {}

        # Title — biblio.invention_title is a list of {text, lang}
        title = self._first_text(biblio.get("invention_title") or [])

        # Abstract — top-level list of {text, lang}
        abstract = self._first_text(raw.get("abstract") or [])

        # Family ID — Lens groups by simple_family; no DOCDB family_id exposed.
        # Use the first member's lens_id as a stable cross-record key.
        families = raw.get("families") or {}
        simple_members = (families.get("simple_family") or {}).get("members") or []
        family_id = simple_members[0].get("lens_id") if simple_members else None

        # Parties
        parties = biblio.get("parties") or {}

        # Applicants → assignees: biblio.parties.applicants[].extracted_name.value
        assignees = [
            {
                "name": a.get("extracted_name", {}).get("value", ""),
                "country": a.get("residence", ""),
            }
            for a in (parties.get("applicants") or [])
            if a.get("extracted_name", {}).get("value")
        ]

        # Inventors: biblio.parties.inventors[].extracted_name.value
        inventors = [
            {"name": i.get("extracted_name", {}).get("value", "")}
            for i in (parties.get("inventors") or [])
            if i.get("extracted_name", {}).get("value")
        ]

        # CPC codes: biblio.classifications_cpc.classifications[].symbol
        cpc_raw = biblio.get("classifications_cpc") or {}
        cpc_codes = [
            c["symbol"]
            for c in (cpc_raw.get("classifications") or [])
            if c.get("symbol")
        ]

        # IPC codes: biblio.classifications_ipcr.classifications[].symbol
        ipc_raw = biblio.get("classifications_ipcr") or {}
        ipc_codes = [
            c["symbol"]
            for c in (ipc_raw.get("classifications") or [])
            if c.get("symbol")
        ]

        # NPL (non-patent literature) citations: biblio.references_cited.citations[].nplcit
        refs = biblio.get("references_cited") or {}
        npl = [c for c in (refs.get("citations") or []) if c.get("nplcit")]
        npl_count = len(npl)
        npl_sample = [
            c["nplcit"].get("text", "")[:200]
            for c in npl[:5]
            if c.get("nplcit", {}).get("text")
        ]

        return NormalizedPatent(
            source=self.name,
            source_id=lens_id,
            family_id=family_id,
            title=self._truncate(title),
            abstract=self._truncate(abstract),
            filing_date=None,   # not returned by the include fields above
            grant_date=self._safe_date(raw.get("date_published")),
            assignees=assignees,
            inventors=inventors,
            cpc_codes=cpc_codes,
            ipc_codes=ipc_codes,
            matched_query=query,
            raw_payload={
                "lens_id": lens_id,
                "npl_citation_count": npl_count,
                "npl_sample": npl_sample,
                "jurisdiction": raw.get("jurisdiction"),
                "publication_type": raw.get("publication_type"),
            },
        )
