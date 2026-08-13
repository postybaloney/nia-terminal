"""
Abstract base class for all *current-signal* ingestors.

"Signals" are the fast-moving complement to patents: NIH grants,
clinical-trial registrations, and FDA device clearances/approvals.
Patents lag reality by ~18 months; these move in days-to-weeks.

Each concrete ingestor implements `fetch()` and returns a list of
NormalizedSignal dataclasses. The signal pipeline layer handles
deduplication and DB writes — ingestors are pure data fetchers,
mirroring ingestors/base.py for patents.
"""
from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class NormalizedSignal:
    """
    Source-agnostic current-signal record.
    Every ingestor maps its native format to this shape.
    """

    source: str                # "nih_reporter" | "clinicaltrials" | "fda_510k" | "fda_pma"
    source_id: str             # appl_id / NCT number / K number / PMA number
    signal_type: str           # "grant" | "trial" | "clearance" | "approval"
    title: str | None
    summary: str | None
    organization: str | None   # grantee org / lead sponsor / applicant
    people: list[dict] = field(default_factory=list)   # [{name, role}]
    amount: int | None = None                          # grant award amount (USD)
    event_date: datetime | None = None                 # award / start / decision date
    status: str | None = None                          # trial status, decision code
    tags: list[str] = field(default_factory=list)      # conditions, product codes
    url: str | None = None
    matched_query: str = ""
    raw_payload: dict = field(default_factory=dict)


class BaseSignalIngestor(abc.ABC):
    """
    All signal ingestors extend this. Concrete classes implement `fetch()`.
    """

    name: str = "signal_base"

    def __init__(self, queries: list[str], since: str, per_page: int = 50):
        self.queries = queries
        self.since = since            # ISO date string "YYYY-MM-DD"
        self.per_page = per_page

    @abc.abstractmethod
    async def fetch(self) -> list[NormalizedSignal]:
        """
        Fetch signals for all configured queries.
        Must be idempotent — called on every scheduled run.
        """
        ...

    def _safe_date(self, val: str | None) -> datetime | None:
        if not val:
            return None
        s = str(val).strip()
        if "T" in s:                      # ISO datetime → keep the date part
            s = s.split("T", 1)[0]
        for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y-%m", "%Y"):
            try:
                return datetime.strptime(s, fmt)
            except (ValueError, TypeError):
                continue
        return None

    def _truncate(self, text: str | None, chars: int = 4000) -> str | None:
        if not text:
            return None
        return text[:chars]
