"""
AI analysis for thesis batches.

Prompts are tuned for academic research context — focus on methodology,
university clusters, emerging research directions, and patent-thesis
proximity signals (research that is likely to become IP within 2-5 years).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from db import get_session
from db.models import AnalysisResult
from ingestors.theses.base import NormalizedThesis

log = logging.getLogger(__name__)

_THESIS_LANDSCAPE_SYSTEM = """You are a research intelligence analyst specializing in medtech and neurotech.
You track emerging academic research that is likely to influence hardware and software innovation
and eventually become commercial IP.
Respond ONLY with valid JSON — no markdown, no preamble."""

_THESIS_LANDSCAPE_PROMPT = """Analyze these {count} PhD theses in medtech/neurotech hardware and software.

Theses:
{thesis_list}

Return a JSON object with exactly these fields:
{{
  "research_clusters": [
    {{
      "theme": "string",
      "description": "2-3 sentences on the research approach and significance",
      "thesis_count": int,
      "example_titles": ["title1", "title2"],
      "hardware_or_software": "hardware" | "software" | "both"
    }}
  ],
  "top_institutions": [
    {{"name": "string", "count": int, "country": "string", "focus": "string"}}
  ],
  "breakout_research": [
    {{
      "title": "string from list",
      "author": "string",
      "institution": "string",
      "why_notable": "string",
      "commercialization_potential": "high" | "medium" | "low"
    }}
  ],
  "emerging_methods": ["string list of novel techniques or approaches appearing across multiple theses"],
  "patent_proximity": "paragraph describing which research areas are closest to patentable innovations",
  "geographic_hotspots": [{{"country": "string", "institution_count": int, "focus_area": "string"}}],
  "strategic_insight": "one sentence for an R&D director or VP of Innovation"
}}
"""

_THESIS_DIGEST_SYSTEM = """You are a research intelligence analyst writing a concise academic landscape digest
for an R&D and innovation strategy team at a medtech or neurotech company.
Focus on research that could become commercial technology within 2-5 years.
Plain prose only — no headers, no bullets."""

_THESIS_DIGEST_PROMPT = """Write a 3-paragraph academic research intelligence digest.

Data:
- New theses indexed: {new_count}
- Sources: {sources}
- Hardware-relevant: {hw_count}
- Software-relevant: {sw_count}
- Top institutions: {top_institutions}
- Emerging themes: {themes}

Paragraph 1 — Volume and institutional landscape: who is producing this research and from where.
Paragraph 2 — Dominant technical themes and methodology shifts in hardware and software.
Paragraph 3 — Commercial pipeline signals: which research areas are closest to IP and what companies should be watching.
"""


async def analyze_thesis_batch(
    theses: list[NormalizedThesis],
    ingest_run_id: int | None = None,
) -> AnalysisResult | None:
    from config import settings
    from analysis import _llm

    if not theses:
        return None

    thesis_list = _format_thesis_list(theses[:25])
    prompt = _THESIS_LANDSCAPE_PROMPT.format(
        count=len(theses),
        thesis_list=thesis_list,
    )

    log.info("thesis analysis: analyzing %d theses", len(theses))

    raw_text = ""
    try:
        raw_text = await _llm(_THESIS_LANDSCAPE_SYSTEM, prompt, max_tokens=2000)
        clean = raw_text.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        structured = json.loads(clean)
    except json.JSONDecodeError as exc:
        log.error("thesis analysis: JSON parse failed: %s\nRaw: %.400s", exc, raw_text)
        structured = {}
    except Exception as exc:
        log.error("thesis analysis: LLM error: %s", exc)
        return None

    model_label = f"{settings.llm_backend}/{settings.llm_model or 'default'}"
    result = AnalysisResult(
        ingest_run_id=ingest_run_id,
        query="[thesis_batch]",
        patent_count=len(theses),
        model=model_label,
        analysis_text=json.dumps(structured, indent=2),
        themes=[c["theme"] for c in structured.get("research_clusters", [])],
        top_assignees=[i["name"] for i in structured.get("top_institutions", [])],
        created_at=datetime.now(timezone.utc),
    )

    with get_session() as session:
        session.add(result)
        session.flush()
        result_id = result.id
        # Expunge before the session closes so the returned object can be
        # accessed freely without triggering a lazy-load on a closed session.
        session.expunge(result)
        log.info("thesis analysis: stored result id=%d", result_id)

    return result


async def generate_thesis_digest(
    new_count: int,
    hw_count: int,
    sw_count: int,
    latest_analysis: AnalysisResult | None,
) -> str:
    from analysis import _llm

    top_institutions: list[str] = []
    themes: list[str] = []
    if latest_analysis and latest_analysis.analysis_text:
        try:
            data = json.loads(latest_analysis.analysis_text)
            top_institutions = [i["name"] for i in data.get("top_institutions", [])[:4]]
            themes = [c["theme"] for c in data.get("research_clusters", [])]
        except Exception:
            pass

    prompt = _THESIS_DIGEST_PROMPT.format(
        new_count=new_count,
        sources="OpenAlex, NDLTD, DART-Europe, EThOS",
        hw_count=hw_count,
        sw_count=sw_count,
        top_institutions=", ".join(top_institutions) or "various",
        themes=", ".join(themes) or "not yet analyzed",
    )
    return await _llm(_THESIS_DIGEST_SYSTEM, prompt, max_tokens=700)


def _format_thesis_list(theses: list[NormalizedThesis]) -> str:
    lines = []
    for i, t in enumerate(theses, 1):
        hw = "HW" if t.hardware_relevant else ""
        sw = "SW" if t.software_relevant else ""
        tags = "/".join(filter(None, [hw, sw]))
        lines.append(
            f"{i}. [{t.year or '?'}] [{tags}] \"{t.title}\"\n"
            f"   Author: {t.author or 'unknown'} | Institution: {t.institution or 'unknown'} | Country: {t.country or '?'}\n"
            f"   Abstract: {(t.abstract or '')[:300]}"
        )
    return "\n\n".join(lines)
