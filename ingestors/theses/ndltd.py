"""
NDLTD (Networked Digital Library of Theses and Dissertations) ingestor.

NDLTD aggregates theses from 400+ institutions across 50+ countries
via the OAI-PMH protocol. Responses are XML.

No API key required. Free.
"""
from __future__ import annotations

import logging
from io import BytesIO

import httpx
from lxml import etree
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestors.theses.base import BaseThesisIngestor, NormalizedThesis

log = logging.getLogger(__name__)

_OAI_BASE = "https://harvest.ndltd.org/oai.do"

_NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "dc":  "http://purl.org/dc/elements/1.1/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
    "etd": "http://www.ndltd.org/standards/metadata/etd-ms/1.1/",
}


class NDLTDIngestor(BaseThesisIngestor):
    name = "ndltd"

    async def fetch(self) -> list[NormalizedThesis]:
        results: list[NormalizedThesis] = []
        async with httpx.AsyncClient(timeout=45) as client:
            for query in self.queries:
                try:
                    batch = await self._fetch_query(client, query)
                    results.extend(batch)
                    log.info("ndltd: query=%r fetched=%d", query, len(batch))
                except Exception as exc:
                    log.warning("ndltd: query=%r error=%s", query, exc)
        return results

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=3, max=15))
    async def _fetch_query(
        self, client: httpx.AsyncClient, query: str
    ) -> list[NormalizedThesis]:
        params = {
            "verb": "Search",
            "query": query,
            "startRecord": "1",
            "maximumRecords": str(self.per_page),
        }
        try:
            resp = await client.get(_OAI_BASE, params=params)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            log.warning("ndltd: Search verb failed (%s), trying ListRecords", exc)
            return await self._list_records(client, query)

        return self._parse_response(resp.content, query)

    async def _list_records(
        self, client: httpx.AsyncClient, query: str
    ) -> list[NormalizedThesis]:
        params = {
            "verb": "ListRecords",
            "metadataPrefix": "oai_dc",
            "from": f"{self.since_year}-01-01",
        }
        try:
            resp = await client.get(_OAI_BASE, params=params)
            resp.raise_for_status()
            return self._parse_response(resp.content, query)
        except Exception as exc:
            log.error("ndltd: ListRecords also failed: %s", exc)
            return []

    def _parse_response(self, xml_bytes: bytes, query: str) -> list[NormalizedThesis]:
        try:
            root = etree.parse(BytesIO(xml_bytes)).getroot()
        except etree.XMLSyntaxError as exc:
            log.error("ndltd: XML parse failed: %s", exc)
            return []

        theses: list[NormalizedThesis] = []
        for record in root.findall(".//{http://www.openarchives.org/OAI/2.0/}record"):
            try:
                t = self._parse_record(record, query)
                if t and (t.hardware_relevant or t.software_relevant):
                    theses.append(t)
            except Exception as exc:
                log.debug("ndltd: record parse error: %s", exc)
        return theses

    def _parse_record(self, record: etree._Element, query: str) -> NormalizedThesis | None:
        def dc(tag: str) -> str | None:
            el = record.find(f".//{{{_NS['dc']}}}{tag}")
            return el.text.strip() if el is not None and el.text else None

        def dc_all(tag: str) -> list[str]:
            return [
                el.text.strip()
                for el in record.findall(f".//{{{_NS['dc']}}}{tag}")
                if el.text and el.text.strip()
            ]

        title = dc("title")
        if not title:
            return None

        identifier = dc("identifier") or ""
        source_id = identifier.replace("https://", "").replace("http://", "")[:64]

        description = dc("description")
        year_str = dc("date") or ""
        year = self._safe_year(year_str[:4] if year_str else None)

        if year and year < self.since_year:
            return None

        subjects = dc_all("subject")
        creators = dc_all("creator")
        author = creators[0] if creators else None
        institution = dc("publisher")
        language = dc("language")
        url = next((i for i in dc_all("identifier") if i.startswith("http")), None)

        thesis = NormalizedThesis(
            source=self.name,
            source_id=source_id or title[:64],
            title=self._truncate(title, 500),
            abstract=self._truncate(description),
            author=author,
            institution=institution,
            country=None,
            year=year,
            language=language,
            keywords=subjects[:10],
            subjects=subjects[:5],
            url=url,
            doi=next((i for i in dc_all("identifier") if "doi" in i.lower()), None),
            degree=dc("type") or "PhD",
            matched_query=query,
            raw_payload={"identifier": identifier},
        )
        return self._tag_relevance(thesis)
