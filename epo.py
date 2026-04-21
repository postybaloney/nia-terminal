"""
EPO Open Patent Services (OPS) ingestor.
Docs: https://developers.epo.org/ops-v3-2/apis

Covers European, PCT, and 100+ national patent offices.
Requires free registration at https://developers.epo.org to obtain
client_id and client_secret. Rate limit: 4 req/sec, 2.5 GB/week.

Response format: XML (Bibliographic Data Service).
We parse with lxml for performance.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from io import BytesIO

import httpx
from lxml import etree
from tenacity import retry, stop_after_attempt, wait_exponential

from config import settings
from ingestors.base import BaseIngestor, NormalizedPatent

log = logging.getLogger(__name__)

_TOKEN_URL = "https://ops.epo.org/3.2/auth/accesstoken"
_SEARCH_URL = "https://ops.epo.org/3.2/rest-services/published-data/search/biblio"
_NS = {
    "ops": "http://ops.epo.org",
    "epo": "http://www.epo.org/exchange",
    "atom": "http://www.w3.org/2005/Atom",
}


class EPOIngestor(BaseIngestor):
    name = "epo"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._token: str | None = None
        self._token_expires: datetime = datetime.now(timezone.utc)

    async def fetch(self) -> list[NormalizedPatent]:
        if not settings.epo_enabled:
            log.info("epo: skipped (no credentials configured)")
            return []

        results: list[NormalizedPatent] = []
        async with httpx.AsyncClient(timeout=30) as client:
            await self._ensure_token(client)
            for query_str in self.queries:
                patents = await self._fetch_query(client, query_str)
                results.extend(patents)
                log.info("epo: query=%r  fetched=%d", query_str, len(patents))
        return results

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

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=15))
    async def _fetch_query(
        self, client: httpx.AsyncClient, query_str: str
    ) -> list[NormalizedPatent]:
        # Build CQL query
        since_compact = self.since.replace("-", "")
        terms = " OR ".join(f'ta="{w}"' for w in query_str.split()[:5])
        cql = f"({terms}) AND pd>={since_compact}"

        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/xml",
        }
        params = {
            "q": cql,
            "Range": f"1-{min(self.per_page, 100)}",
        }

        resp = await client.get(_SEARCH_URL, headers=headers, params=params)
        if resp.status_code == 404:
            return []  # no results for this query
        resp.raise_for_status()

        return self._parse_xml(resp.content, query_str)

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

        # Family ID is in the exchange-document family attribute
        family_id = doc.get("family-id")

        # Abstract (prefer English)
        abstract = None
        for abs_el in doc.findall(".//epo:abstract", _NS):
            if abs_el.get("lang", "").lower() in ("en", ""):
                abstract = " ".join(
                    p.text or "" for p in abs_el.findall("epo:p", _NS)
                ).strip()
                break

        # Title (prefer English)
        title = None
        for t_el in doc.findall(".//epo:invention-title", _NS):
            if t_el.get("lang", "").lower() in ("en", ""):
                title = t_el.text
                break

        # Dates
        filing_date = self._safe_date(text(".//epo:application-reference//epo:date"))
        pub_date = self._safe_date(text(".//epo:publication-reference//epo:date"))

        # Assignees / applicants
        assignees = []
        for party in doc.findall(".//epo:applicant", _NS):
            name_el = party.find(".//epo:name", _NS)
            country_el = party.find(".//epo:country", _NS)
            if name_el is not None and name_el.text:
                assignees.append({
                    "name": name_el.text.strip(),
                    "country": country_el.text.strip() if country_el is not None and country_el.text else "",
                })

        # Inventors
        inventors = []
        for inv in doc.findall(".//epo:inventor", _NS):
            name_el = inv.find(".//epo:name", _NS)
            if name_el is not None and name_el.text:
                inventors.append({"name": name_el.text.strip()})

        # IPC codes
        ipc_codes = texts(".//epo:classification-ipc//epo:text")
        # CPC codes
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
