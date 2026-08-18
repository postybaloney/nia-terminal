"""
Abstract base class for all patent source ingestors.

Each concrete ingestor must implement `fetch()` and return a list of
NormalizedPatent dataclasses. The pipeline layer handles deduplication,
relevance scoring and DB writes — ingestors are pure data fetchers.
"""
from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class NormalizedPatent:
    """
    Source-agnostic patent record.
    Every ingestor maps its native format to this shape.
    """

    source: str                        # "patentsview" | "epo" | "bigquery"
    source_id: str                     # native patent number / pub number
    family_id: str | None              # DOCDB family ID if available
    title: str | None
    abstract: str | None
    filing_date: datetime | None
    grant_date: datetime | None
    assignees: list[dict] = field(default_factory=list)   # [{name, country}]
    inventors: list[dict] = field(default_factory=list)   # [{name}]
    cpc_codes: list[str] = field(default_factory=list)
    ipc_codes: list[str] = field(default_factory=list)
    matched_query: str = ""
    raw_payload: dict = field(default_factory=dict)

    # Populated by the pipeline's relevance gate, not by ingestors.
    # Carried on the record so every downstream consumer (digest, dashboard,
    # graph) can show WHY a record is in the corpus.
    relevance_score: int = 0
    relevance_tier: str = ""           # core | adjacent | cardiac | reject
    relevance_reasons: list[str] = field(default_factory=list)


class BaseIngestor(abc.ABC):
    """
    All ingestors extend this. Concrete classes implement `fetch()`.
    """

    name: str = "base"

    def __init__(self, queries: list[str], since: str, per_page: int = 50):
        self.queries = queries
        self.since = since            # ISO date string "YYYY-MM-DD"
        self.per_page = per_page
        # Ingestors append human-readable failures here. The pipeline surfaces
        # them so a partially-successful run reports honestly instead of
        # looking clean.
        self.query_errors: list[str] = []

    @abc.abstractmethod
    async def fetch(self) -> list[NormalizedPatent]:
        """
        Fetch patents for all configured queries.
        Must be idempotent — called on every scheduled run.

        CONTRACT: must never raise for a single-query failure. Catch per query,
        record in self.query_errors, and return whatever succeeded. A run that
        loses 550 good records because query 12 got a 403 is a bug, not
        robustness. (See the 2026-08-17 incident.)
        """
        ...

    def _safe_date(self, val: str | None) -> datetime | None:
        if not val:
            return None
        for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(val[:len(fmt)], fmt)
            except (ValueError, TypeError):
                continue
        return None

    def _truncate(self, text: str | None, chars: int = 4000) -> str | None:
        if not text:
            return None
        return text[:chars]
