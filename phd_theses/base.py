"""
Base classes for thesis ingestors.

NormalizedThesis is the source-agnostic representation of a PhD thesis.
All ingestors map their native format to this shape before the pipeline
writes to the DB.

Hardware/software relevance filtering is handled here via keyword
matching against title + abstract, so individual ingestors don't need
to implement it — they just fetch and normalize.
"""
from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import datetime

# ── Relevance filter ──────────────────────────────────────────────────────────
# Theses matching ANY of these keyword groups are kept.
# Groups are OR'd; words within a group are AND'd.
# Add/remove terms freely in your .env via THESIS_KEYWORDS.

DEFAULT_HARDWARE_KEYWORDS = [
    "neural interface", "brain computer interface", "bci",
    "implantable device", "neural implant", "cochlear implant",
    "deep brain stimulation", "cortical electrode", "electrode array",
    "neuroprosthetic", "prosthetic limb", "exoskeleton",
    "wearable sensor", "biosensor", "microelectrode",
    "optical coherence", "medical imaging", "ultrasound transducer",
    "retinal prosthesis", "spinal cord stimulation",
    "printed circuit", "fpga", "asic", "neuromorphic chip",
    "semiconductor", "photonic", "mems", "lab on chip",
]

DEFAULT_SOFTWARE_KEYWORDS = [
    "neural network", "deep learning", "machine learning",
    "signal processing", "eeg classification", "ecog decoding",
    "spike sorting", "neural decoding", "brain signal",
    "medical image segmentation", "pathology detection",
    "drug discovery algorithm", "protein structure prediction",
    "federated learning healthcare", "digital health",
    "clinical decision support", "ehr", "electronic health record",
    "reinforcement learning robotics", "surgical robot",
    "real time embedded", "firmware medical",
]


def is_relevant(title: str, abstract: str, extra_keywords: list[str] | None = None) -> bool:
    """
    Return True if the thesis is relevant to hardware or software innovation
    in medtech/neurotech. Checks title and abstract (case-insensitive).
    """
    text = f"{title} {abstract}".lower()
    all_keywords = DEFAULT_HARDWARE_KEYWORDS + DEFAULT_SOFTWARE_KEYWORDS
    if extra_keywords:
        all_keywords = all_keywords + [k.lower() for k in extra_keywords]
    return any(kw in text for kw in all_keywords)


# ── Normalized thesis ─────────────────────────────────────────────────────────

@dataclass
class NormalizedThesis:
    """Source-agnostic PhD thesis record."""

    source: str                        # "openalex" | "ndltd" | "dart" | "ethos"
    source_id: str                     # native ID in that system
    title: str
    abstract: str | None
    author: str | None
    institution: str | None
    country: str | None
    year: int | None
    language: str | None
    keywords: list[str] = field(default_factory=list)
    subjects: list[str] = field(default_factory=list)
    url: str | None = None             # landing page or PDF link
    doi: str | None = None
    degree: str | None = None          # "PhD" | "Doctoral" | etc.
    matched_query: str = ""
    raw_payload: dict = field(default_factory=dict)
    hardware_relevant: bool = False
    software_relevant: bool = False


# ── Abstract base ingestor ────────────────────────────────────────────────────

class BaseThesisIngestor(abc.ABC):
    """All thesis ingestors extend this."""

    name: str = "base"

    def __init__(
        self,
        queries: list[str],
        since_year: int = 2018,
        per_page: int = 50,
        extra_keywords: list[str] | None = None,
    ):
        self.queries = queries
        self.since_year = since_year
        self.per_page = per_page
        self.extra_keywords = extra_keywords or []

    @abc.abstractmethod
    async def fetch(self) -> list[NormalizedThesis]:
        """Fetch and return normalized thesis records."""
        ...

    def _tag_relevance(self, thesis: NormalizedThesis) -> NormalizedThesis:
        """Annotate hardware_relevant and software_relevant flags."""
        text = f"{thesis.title} {thesis.abstract or ''}".lower()
        thesis.hardware_relevant = any(
            kw in text for kw in DEFAULT_HARDWARE_KEYWORDS + self.extra_keywords
        )
        thesis.software_relevant = any(
            kw in text for kw in DEFAULT_SOFTWARE_KEYWORDS + self.extra_keywords
        )
        return thesis

    def _safe_year(self, val: str | int | None) -> int | None:
        if val is None:
            return None
        try:
            return int(str(val)[:4])
        except (ValueError, TypeError):
            return None

    def _truncate(self, text: str | None, chars: int = 4000) -> str | None:
        if not text:
            return None
        return text[:chars]
