"""
SEC EDGAR full-text search ingestor.

Two distinct signals, both free and unauthenticated:

  1. NARRATIVE  — 8-K / 10-K / S-1 filings that mention neurotech phrases.
     Public companies disclose partnerships, acquisitions, trial results and
     reimbursement wins here BEFORE the trade press writes them up, and the
     language is legally constrained so it is unusually reliable.

  2. FUNDING    — Form D is the notice of an exempt securities offering, i.e.
     a private raise. It is the only free, structured, near-real-time private
     funding feed in existence. Crunchbase withdrew its free API in 2025;
     this replaces a large part of what it was used for.

Endpoint: https://efts.sec.gov/LATEST/search-index?q=...&forms=...&startdt=...
The SEC REQUIRES a descriptive User-Agent containing a contact address, and
rate-limits to 10 requests/second. Requests without it are refused.
"""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta

import httpx

from ingestors.signals.base import BaseSignalIngestor, NormalizedSignal
from neuro_taxonomy import classify_tech

log = logging.getLogger(__name__)

_FTS_URL = "https://efts.sec.gov/LATEST/search-index"

EDGAR_PHRASES: tuple[str, ...] = (
    '"brain-computer interface"',
    '"deep brain stimulation"',
    '"spinal cord stimulation"',
    '"neuromodulation"',
    '"neurostimulation"',
    '"cochlear implant"',
    '"vagus nerve stimulation"',
    '"neural interface"',
)

NARRATIVE_FORMS = "8-K,10-K,10-Q,S-1,424B4"
FUNDING_FORMS = "D,D/A"


class EdgarIngestor(BaseSignalIngestor):
    name = "edgar"

    def __init__(self, *args, contact_email: str = "", days_back: int = 90, **kwargs):
        super().__init__(*args, **kwargs)
        self.contact_email = contact_email or "research@example.com"
        self.days_back = days_back

    async def fetch(self) -> list[NormalizedSignal]:
        out: list[NormalizedSignal] = []
        self.query_errors = []

        end = datetime.utcnow().date()
        start = end - timedelta(days=self.days_back)

        headers = {
            # The SEC refuses requests without a contact-bearing User-Agent.
            "User-Agent": f"NIA Neurotech Intelligence {self.contact_email}",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
        }
        async with httpx.AsyncClient(timeout=25, follow_redirects=True,
                                     headers=headers) as client:
            for phrase in EDGAR_PHRASES:
                for forms, kind in ((NARRATIVE_FORMS, "narrative"),
                                    (FUNDING_FORMS, "funding")):
                    try:
                        items = await self._search(client, phrase, forms, kind,
                                                   str(start), str(end))
                    except Exception as exc:
                        log.warning("edgar: %s/%s failed (%s) — continuing",
                                    phrase, kind, exc)
                        self.query_errors.append(
                            f"edgar {phrase}/{kind}: {type(exc).__name__}: {exc}")
                        continue
                    out.extend(items)
                    if items:
                        log.info("edgar: %-32s %-9s hits=%d", phrase, kind, len(items))
                    await asyncio.sleep(0.15)   # SEC allows 10 req/s

        log.info("edgar: %d filings total", len(out))
        return out

    async def _search(
        self, client: httpx.AsyncClient, phrase: str, forms: str,
        kind: str, start: str, end: str,
    ) -> list[NormalizedSignal]:
        params = {
            "q": phrase,
            "forms": forms,
            "startdt": start,
            "enddt": end,
        }
        resp = await client.get(_FTS_URL, params=params)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        data = resp.json()

        hits = ((data or {}).get("hits") or {}).get("hits") or []
        out: list[NormalizedSignal] = []

        for h in hits:
            src = h.get("_source") or {}
            display = src.get("display_names") or []
            # EDGAR appends "  (CIK 0001899123)" to display names; strip it so
            # the ORG string matches what the other sources emit and resolves
            # to the same graph node.
            company = re.sub(r"\s*\(CIK\s+\d+\)\s*$", "", display[0]).strip() if display else None
            form = src.get("root_form") or src.get("file_type") or ""
            filed = src.get("file_date")
            adsh = src.get("adsh") or h.get("_id") or ""
            ciks = src.get("ciks") or []

            # Reconstruct the canonical filing URL from the accession number.
            url = None
            if adsh and ciks:
                acc = str(adsh).replace("-", "")
                cik = str(ciks[0]).lstrip("0")
                url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/"

            title = f"{company or 'Unknown filer'} — {form}"
            summary = (
                f"Form {form} filed {filed} mentioning {phrase}."
                if kind == "narrative"
                else f"Form {form} (exempt offering notice) filed {filed}. "
                     f"Private raise disclosure mentioning {phrase}."
            )

            out.append(NormalizedSignal(
                source="sec_edgar",
                source_id=str(adsh or f"{company}:{filed}")[:128],
                signal_type="filing",
                title=self._truncate(title, 500),
                summary=self._truncate(summary, 4000),
                organization=company,
                people=[],
                amount=None,
                event_date=self._safe_date(filed),
                status=form,
                tags=([kind] + classify_tech(phrase.strip('"'), "")),
                url=url,
                matched_query=f"{phrase} [{forms}]",
                raw_payload={"form": form, "kind": kind, "ciks": ciks[:3],
                             "phrase": phrase},
            ))
        return out
