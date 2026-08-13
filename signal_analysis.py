"""
Signal digest generation.

Two layers:
  1. A deterministic scorecard built straight from the data — always works,
     no LLM required, never hallucinates a number.
  2. An optional LLM narrative on top (same multi-backend `_llm` dispatcher
     as analysis.py) that interprets the week's signals.

The deterministic layer is the source of truth for every figure; the LLM
only writes connective prose around it.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from db import get_session
from db.signal_models import Signal

log = logging.getLogger(__name__)

_TYPE_LABELS = {
    "grant": "NIH grants",
    "trial": "New clinical trials",
    "clearance": "FDA 510(k) clearances",
    "approval": "FDA PMA approvals",
}

_NARRATIVE_SYSTEM = """You are a medtech/neurotech market intelligence analyst writing for founders, BD teams, and investors.
Write in clear, direct prose. Be specific — cite organization names, dollar amounts, and dates exactly as given.
Never invent a fact that is not in the data provided. No bullet points, no markdown headers. Plain paragraphs only."""

_NARRATIVE_PROMPT = """Below is this period's verified current-signal scorecard for the medtech/neurotech space
(grants, clinical trials, FDA actions). Write a 2-paragraph interpretation:

Paragraph 1 — What moved: who is spending, which organizations show up across multiple
signal types, notable dollar amounts.
Paragraph 2 — What it implies: which technology areas are heating up, and one thing a
BD team or investor should watch next period.

Scorecard:
{scorecard}
"""


def build_signal_scorecard(days: int = 7) -> str:
    """
    Deterministic digest of signals first seen in the last `days` days.
    Every number comes straight from SQL — safe to publish as-is.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    lines: list[str] = []

    with get_session() as session:
        recent = (
            session.query(Signal)
            .filter(Signal.first_seen_at >= cutoff)
            .order_by(Signal.event_date.desc().nullslast())
            .all()
        )

        if not recent:
            return f"No new signals ingested in the last {days} days."

        # Counts by type
        by_type: dict[str, list[Signal]] = {}
        for s in recent:
            by_type.setdefault(s.signal_type, []).append(s)

        lines.append(f"CURRENT SIGNALS — last {days} days — {len(recent)} new records")
        lines.append("")

        for stype in ("grant", "trial", "clearance", "approval"):
            rows = by_type.get(stype) or []
            if not rows:
                continue
            label = _TYPE_LABELS.get(stype, stype)
            header = f"{label}: {len(rows)}"
            if stype == "grant":
                total = sum(r.amount or 0 for r in rows)
                if total:
                    header += f" (${total:,.0f} awarded)"
            lines.append(header)
            for r in rows[:5]:
                when = r.event_date.date().isoformat() if r.event_date else "n.d."
                org = r.organization or "Unknown org"
                title = (r.title or "(untitled)").strip()
                extra = f" — ${r.amount:,.0f}" if (stype == "grant" and r.amount) else ""
                lines.append(f"  • {when}  {org}: {title[:110]}{extra}")
            if len(rows) > 5:
                lines.append(f"  … and {len(rows) - 5} more")
            lines.append("")

        # Orgs appearing across multiple signal types = the real movers
        orgs: dict[str, set] = {}
        for s in recent:
            if s.organization:
                orgs.setdefault(s.organization.strip(), set()).add(s.signal_type)
        movers = sorted(
            ((o, ts) for o, ts in orgs.items() if len(ts) > 1),
            key=lambda x: -len(x[1]),
        )
        if movers:
            lines.append("Cross-signal movers (active in more than one category):")
            for org, types in movers[:8]:
                lines.append(f"  • {org} — {', '.join(sorted(types))}")

    return "\n".join(lines)


async def generate_signal_digest(days: int = 7, narrative: bool = True) -> str:
    """
    Full digest: deterministic scorecard, plus an LLM narrative when a
    backend is configured. Falls back to scorecard-only on any LLM error.
    """
    scorecard = build_signal_scorecard(days=days)

    if not narrative or scorecard.startswith("No new signals"):
        return scorecard

    try:
        from analysis import _llm  # same multi-backend dispatcher as patents
        prose = await _llm(
            _NARRATIVE_SYSTEM,
            _NARRATIVE_PROMPT.format(scorecard=scorecard),
            max_tokens=700,
        )
        return f"{prose}\n\n---\n\n{scorecard}"
    except Exception as exc:
        log.warning("signal digest: LLM narrative skipped (%s)", exc)
        return scorecard
