"""
ATS job-board ingestor — Greenhouse, Lever, Ashby.

This is the legal, free replacement for LinkedIn people-monitoring, and it is
a genuine LEADING indicator rather than a lagging one:

    first Regulatory Affairs hire   -> an FDA submission is being prepared
    first Reimbursement/HEOR hire   -> a CMS coverage push is starting
    first Clinical Operations hire  -> a trial is about to open
    first QA / Manufacturing hire   -> scale-up for commercialisation
    surge in Firmware/DSP hires     -> next-gen device in development

None of that is visible in patents (18-month lag) or press releases. It is
public, structured, unauthenticated, and free.

All three ATS platforms expose public JSON job boards:
    Greenhouse  https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true
    Lever       https://api.lever.co/v0/postings/{slug}?mode=json
    Ashby       https://api.ashbyhq.com/posting-api/job-board/{slug}

NOTE ON SLUGS: a company's board slug cannot be derived from its name and has
to be observed. The registry below is a best-effort starting set; unknown or
renamed slugs simply 404 and are skipped. Run `python main.py doctor --jobs`
to see which slugs actually resolve, then prune or correct this list. Failing
loudly here rather than silently pretending coverage is the point.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

from ingestors.signals.base import BaseSignalIngestor, NormalizedSignal
from neuro_taxonomy import classify_tech

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Board:
    company: str
    ats: str        # "greenhouse" | "lever" | "ashby"
    slug: str


# Registry — every entry below was VERIFIED on 2026-08-18 by fetching the API
# and reading the JSON, not inferred from a careers page. Job counts at
# verification are noted so drift is visible later.
#
# The first pass guessed 20 slugs and only 3 resolved. The reason wasn't bad
# guessing: most neurotech startups are NOT on Greenhouse/Lever/Ashby at all.
# Two of the three that do work were on a different ATS than assumed.
#
# ⚠ A 200 response is NOT proof you have the right company. Three slugs return
# perfectly valid JSON for an entirely different business:
#     ashby:kernel      -> an AI enterprise-data company, not neurotech Kernel
#     ashby:brainco     -> "Brain Co.", an AI-OS company, not BrainCo
#     greenhouse:setpoint -> a fintech, not Setpoint Medical
# All three are deliberately excluded. Before adding any slug, confirm the
# company identity from the response body. A wrong slug doesn't fail loudly —
# it silently poisons the dataset with another industry's hiring signal.
BOARDS: tuple[Board, ...] = (
    # ── Confirmed working since first run ────────────────────────────────────
    Board("Neuralink", "greenhouse", "neuralink"),
    Board("NeuroPace", "greenhouse", "neuropace"),
    Board("Ceribell", "greenhouse", "ceribell"),
    # ── Corrected 2026-08-18 (right company, wrong ATS or wrong slug) ────────
    Board("Motif Neurotech", "greenhouse", "motifneurotech"),   # 2 jobs — was ashby
    Board("Subsense", "lever", "subsense"),                     # 4 jobs — was ashby
    Board("iota Biosciences", "ashby", "iota-bio"),             # 3 jobs — slug differs
    # ── Added 2026-08-18, all verified ───────────────────────────────────────
    Board("Nudge", "ashby", "nudge"),                                    # 18 jobs — ultrasound neuromodulation
    Board("Echo Neurotechnologies", "ashby", "echo"),                    # 11 jobs — implantable BCI
    Board("Epia Neuro", "ashby", "epianeuro"),                           # 10 jobs — intent-driven neural systems
    Board("Dreem Health", "greenhouse", "dreemhealth"),                  #  9 jobs — EEG / sleep
    Board("Inspire Medical Systems", "greenhouse", "inspiremedicalsystemsinc"),  # 8 jobs — hypoglossal nerve stim
    Board("Beacon Biosignals", "greenhouse", "beaconbiosignals"),        #  5 jobs — EEG foundation models
    Board("Cala Health", "greenhouse", "calahealth"),                    #  4 jobs — wearable neuromodulation
    Board("CVRx", "lever", "CVRx"),                                      #  4 jobs — NOTE: capitalised slug
    Board("Synaptrix Labs", "greenhouse", "synaptrixlabs"),              #  3 jobs — non-invasive BCI
    Board("Piramidal", "greenhouse", "piramidalinc"),                    #  2 jobs — EEG foundation model
)

# ── Companies deliberately NOT in BOARDS, and where they actually are ────────
# Recorded so this research isn't lost. Each would need a new ingestor; the
# public-API ones (Workable, Personio) are the cheapest to add next, and
# Precision Neuroscience in particular is worth recovering.
#
#   Synchron                  ADP Workforce Now   (old Greenhouse board is dead)
#   Paradromics               JazzHR              paradromicsinc.applytojob.com
#   Precision Neuroscience    Workable            apply.workable.com/precision-neuroscience
#   INBRAIN Neuroelectronics  Personio            inbrain-neuroelectronics.jobs.personio.com
#   Blackrock Neurotech       Rippling
#   Onward Medical            HiBob               onwardmedical.careers.hibob.com
#   Axoft                     Polymer             jobs.polymer.co/axoft
#   Neurosoft Bioelectronics  JOIN                join.com/companies/neurosoft-bio2
#   Somnee                    ShopHire            somneesleep.com/a/careers
#   Science Corporation       bespoke             science.xyz/careers
#   Cognixion                 bespoke             per-role pages, no ATS
#   Openwater                 none                LinkedIn + email only
#   Forest Neurotech          none discoverable   (FRO under Convergent Research;
#                                                  lever:convergentresearch has 7 roles,
#                                                  none Forest-branded right now)
#   Kernel                    no live board       (do NOT use ashby:kernel — wrong company)
#   Medtronic / Abbott / Boston Scientific / LivaNova / Axonics — Workday, unsupported
#   Nevro, Neurable, Saluda Medical, Setpoint Medical — no supported board found

# Roles whose FIRST appearance at a company is a strategic event.
SIGNAL_ROLES: dict[str, str] = {
    "regulatory": "regulatory — FDA submission likely in preparation",
    "reimbursement": "reimbursement — CMS coverage push starting",
    "health economics": "HEOR — payer strategy forming",
    "market access": "market access — commercialisation starting",
    "clinical operations": "clinical ops — trial opening",
    "clinical trial": "clinical ops — trial opening",
    "quality assurance": "QA — design controls / scale-up",
    "quality engineer": "QA — design controls / scale-up",
    "manufacturing": "manufacturing — scale-up",
    "medical affairs": "medical affairs — commercial stage",
    "biostatistic": "biostats — pivotal trial analysis",
    "firmware": "firmware — next-gen device build",
    "dsp": "DSP — signal chain development",
    "neural engineer": "neural engineering — core R&D",
}


def _role_signal(title: str) -> str | None:
    low = (title or "").lower()
    for marker, meaning in SIGNAL_ROLES.items():
        if marker in low:
            return meaning
    return None


class JobBoardIngestor(BaseSignalIngestor):
    name = "jobs"

    def __init__(self, *args, boards: tuple[Board, ...] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.boards = boards or BOARDS
        self.resolved: list[str] = []
        self.unresolved: list[str] = []

    async def fetch(self) -> list[NormalizedSignal]:
        out: list[NormalizedSignal] = []
        self.query_errors = []
        self.resolved, self.unresolved = [], []

        headers = {"User-Agent": "NIA-neurotech-intelligence/1.0",
                   "Accept": "application/json"}
        async with httpx.AsyncClient(timeout=20, follow_redirects=True,
                                     headers=headers) as client:
            for b in self.boards:
                try:
                    jobs = await self._fetch_board(client, b)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code in (404, 403):
                        self.unresolved.append(f"{b.company} ({b.ats}:{b.slug})")
                        log.info("jobs: %s — no public board at %s:%s (skipped)",
                                 b.company, b.ats, b.slug)
                    else:
                        self.query_errors.append(f"{b.company}: HTTP {exc.response.status_code}")
                    continue
                except Exception as exc:
                    log.warning("jobs: %s failed (%s) — continuing", b.company, exc)
                    self.query_errors.append(f"{b.company}: {type(exc).__name__}: {exc}")
                    continue

                self.resolved.append(f"{b.company} ({len(jobs)})")
                out.extend(jobs)
                log.info("jobs: %-26s %-10s postings=%d", b.company, b.ats, len(jobs))
                await asyncio.sleep(0.25)

        log.info("jobs: %d/%d boards resolved, %d postings total",
                 len(self.resolved), len(self.boards), len(out))
        if self.unresolved:
            log.info("jobs: unresolved slugs (fix in ingestors/signals/jobs.py): %s",
                     ", ".join(self.unresolved))
        return out

    async def _fetch_board(
        self, client: httpx.AsyncClient, b: Board
    ) -> list[NormalizedSignal]:
        if b.ats == "greenhouse":
            url = f"https://boards-api.greenhouse.io/v1/boards/{b.slug}/jobs?content=true"
        elif b.ats == "lever":
            url = f"https://api.lever.co/v0/postings/{b.slug}?mode=json"
        elif b.ats == "ashby":
            url = f"https://api.ashbyhq.com/posting-api/job-board/{b.slug}"
        else:
            return []

        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

        if b.ats == "greenhouse":
            raw_jobs = data.get("jobs", []) if isinstance(data, dict) else []
        elif b.ats == "lever":
            raw_jobs = data if isinstance(data, list) else []
        else:
            raw_jobs = data.get("jobs", []) if isinstance(data, dict) else []

        return [s for s in (self._normalize(j, b) for j in raw_jobs) if s]

    def _normalize(self, j: dict, b: Board) -> NormalizedSignal | None:
        if b.ats == "greenhouse":
            jid = str(j.get("id", ""))
            title = j.get("title") or ""
            url = j.get("absolute_url")
            loc = (j.get("location") or {}).get("name")
            posted = j.get("updated_at") or j.get("first_published")
            body = j.get("content") or ""
        elif b.ats == "lever":
            jid = str(j.get("id", ""))
            title = j.get("text") or ""
            url = j.get("hostedUrl")
            loc = (j.get("categories") or {}).get("location")
            ts = j.get("createdAt")
            posted = (
                datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
                if isinstance(ts, (int, float)) else None
            )
            body = j.get("descriptionPlain") or j.get("description") or ""
        else:  # ashby
            jid = str(j.get("id", ""))
            title = j.get("title") or ""
            url = j.get("jobUrl") or j.get("applyUrl")
            loc = j.get("location")
            posted = j.get("publishedAt") or j.get("updatedAt")
            body = j.get("descriptionPlain") or j.get("description") or ""

        if not title or not jid:
            return None

        meaning = _role_signal(title)

        return NormalizedSignal(
            source=f"jobs:{b.ats}"[:32],
            source_id=f"{b.slug}:{jid}"[:128],
            signal_type="posting",
            title=self._truncate(title, 500),
            summary=self._truncate(meaning or (body[:600] if body else None), 4000),
            organization=b.company,
            people=[],
            amount=None,
            event_date=self._safe_date(posted),
            # `status` maps to a varchar(64) column; job locations are often
            # longer than that ("Remote - United States; San Francisco, CA; ...").
            status=(loc or None) and loc[:64],
            tags=(["strategic-hire"] if meaning else []) + classify_tech(title, body[:2000]),
            url=url,
            matched_query=b.company,
            raw_payload={
                "ats": b.ats,
                "slug": b.slug,
                "location": loc,
                "role_signal": meaning,
            },
        )
