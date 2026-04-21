"""
AI analysis layer — multi-backend LLM support.

Supported backends (set LLM_BACKEND in .env):

  groq        — FREE. Llama 3.3 70B via Groq Cloud. Sign up at console.groq.com.
                No credit card required. Fast. Best free option.
                pip install groq

  gemini      — FREE tier. Gemini 2.0 Flash via Google AI Studio.
                Get a key at aistudio.google.com (no billing needed).
                pip install google-generativeai

  ollama      — FREE, runs locally. No account, no rate limits, fully private.
                Install Ollama from ollama.com, then: ollama pull llama3.2
                pip install ollama

  huggingface — FREE tier via HF Inference API. Slower, less reliable.
                Get a token at huggingface.co/settings/tokens.
                pip install huggingface_hub

  anthropic   — PAID. Claude Sonnet via Anthropic API.
                pip install anthropic

All backends implement the same interface: system prompt + user prompt -> text.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from config import settings
from db import get_session
from db.models import AnalysisResult
from ingestors.base import NormalizedPatent

log = logging.getLogger(__name__)

# ── Prompts (shared across all backends) ─────────────────────────────────────

_LANDSCAPE_SYSTEM = """You are a senior patent intelligence analyst specializing in medtech and neurotech.
Your role is to identify technology clusters, key actors, and strategic opportunities from raw patent data.
Always ground observations in specific evidence from the patents provided.
Respond ONLY with valid JSON — no markdown, no explanation outside the JSON structure."""

_LANDSCAPE_PROMPT = """Analyze the following {count} recently filed/granted patents from a "{query}" search.

Patents:
{patent_list}

Return a JSON object with exactly these fields:
{{
  "technology_clusters": [
    {{"theme": "string", "description": "2-3 sentence description", "patent_count": int, "example_titles": ["title1", "title2"]}}
  ],
  "top_assignees": [
    {{"name": "string", "count": int, "focus_area": "string"}}
  ],
  "notable_innovations": [
    {{"title": "string from the list", "why_notable": "string", "assignee": "string"}}
  ],
  "white_space": [
    {{"opportunity": "string", "rationale": "string"}}
  ],
  "strategic_takeaway": "one sentence for R&D leadership",
  "coverage_period": {{"from": "YYYY-MM-DD", "to": "YYYY-MM-DD"}}
}}
"""

_DIGEST_SYSTEM = """You are a patent intelligence analyst writing a concise weekly digest for an R&D and IP strategy team.
Write in clear, direct prose. Be specific — cite company names, technology approaches, and dates.
No bullet points. No markdown headers. Plain paragraphs only."""

_DIGEST_PROMPT = """Write a 3-paragraph weekly patent intelligence digest for the medtech and neurotech space.

This week's data:
- New patents ingested: {new_count}
- Sources: {sources}
- Queries monitored: {queries}
- Analysis summary: {analysis_summary}

Structure:
Paragraph 1 — What's new this week: volume, most active assignees, newest filings.
Paragraph 2 — Key technology themes and any notable shifts from prior weeks.
Paragraph 3 — Strategic signals: white space, competitive moves, academic-to-commercial transitions.
"""


# ── Backend implementations ───────────────────────────────────────────────────

async def _call_groq(system: str, prompt: str, max_tokens: int = 2000) -> str:
    """
    Groq Cloud — FREE tier, no credit card.
    Models: llama-3.3-70b-versatile, llama-3.1-8b-instant, mixtral-8x7b-32768
    Sign up: console.groq.com
    """
    from groq import AsyncGroq
    client = AsyncGroq(api_key=settings.groq_api_key)
    response = await client.chat.completions.create(
        model=settings.llm_model or "llama-3.3-70b-versatile",
        max_tokens=max_tokens,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
    )
    return response.choices[0].message.content


async def _call_gemini(system: str, prompt: str, max_tokens: int = 2000) -> str:
    """
    Google Gemini — FREE tier via AI Studio.
    Models: gemini-2.0-flash, gemini-1.5-flash
    Get key: aistudio.google.com (no billing needed for free tier)
    """
    import google.generativeai as genai
    genai.configure(api_key=settings.gemini_api_key)
    model = genai.GenerativeModel(
        model_name=settings.llm_model or "gemini-2.0-flash",
        system_instruction=system,
    )
    # Gemini's async generate
    response = await model.generate_content_async(
        prompt,
        generation_config={"max_output_tokens": max_tokens},
    )
    return response.text


async def _call_ollama(system: str, prompt: str, max_tokens: int = 2000) -> str:
    """
    Ollama — 100% local, FREE, no account needed.
    Install: https://ollama.com
    Pull a model first: ollama pull llama3.2
    Runs on CPU (slow) or GPU (fast).
    Default model: llama3.2 — change LLM_MODEL in .env for others.
    Other good choices: mistral, phi4, qwen2.5
    """
    import ollama
    response = await ollama.AsyncClient().chat(
        model=settings.llm_model or "llama3.2",
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        options={"num_predict": max_tokens},
    )
    return response["message"]["content"]


async def _call_huggingface(system: str, prompt: str, max_tokens: int = 2000) -> str:
    """
    Hugging Face Inference API — FREE tier, but slow and rate-limited.
    Get a token: huggingface.co/settings/tokens (read access is free)
    Default model: mistralai/Mistral-7B-Instruct-v0.3
    Better free alternatives on HF: microsoft/Phi-3.5-mini-instruct
    """
    import httpx
    model = settings.llm_model or "mistralai/Mistral-7B-Instruct-v0.3"
    url = f"https://api-inference.huggingface.co/models/{model}"
    full_prompt = f"[INST] <<SYS>>\n{system}\n<</SYS>>\n\n{prompt} [/INST]"
    headers = {"Authorization": f"Bearer {settings.huggingface_token}"}
    payload = {
        "inputs": full_prompt,
        "parameters": {"max_new_tokens": max_tokens, "return_full_text": False},
    }
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and data:
            return data[0].get("generated_text", "")
        return str(data)


async def _call_anthropic(system: str, prompt: str, max_tokens: int = 2000) -> str:
    """Anthropic Claude — paid API."""
    import anthropic as ant
    client = ant.AsyncAnthropic(api_key=settings.anthropic_api_key)
    message = await client.messages.create(
        model=settings.llm_model or "claude-sonnet-4-20250514",
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


async def _llm(system: str, prompt: str, max_tokens: int = 2000) -> str:
    """
    Route to the configured backend. Set LLM_BACKEND in .env.
    Defaults to groq if not set.
    """
    backend = (settings.llm_backend or "groq").lower()
    model_label = settings.llm_model or "(default)"
    log.info("analysis: using backend=%s model=%s", backend, model_label)

    dispatch = {
        "groq": _call_groq,
        "gemini": _call_gemini,
        "ollama": _call_ollama,
        "huggingface": _call_huggingface,
        "anthropic": _call_anthropic,
    }

    if backend not in dispatch:
        raise ValueError(
            f"Unknown LLM_BACKEND={backend!r}. "
            f"Choose from: {', '.join(dispatch)}"
        )

    return await dispatch[backend](system, prompt, max_tokens)


# ── Public API ────────────────────────────────────────────────────────────────

async def analyze_batch(
    patents: list[NormalizedPatent],
    query: str,
    ingest_run_id: int,
) -> AnalysisResult | None:
    """
    Run LLM landscape analysis on a batch of new patents.
    Stores result to DB and returns the ORM object.
    """
    if len(patents) < settings.analysis_min_new:
        log.info(
            "analysis: skipping — only %d new patents (min: %d)",
            len(patents), settings.analysis_min_new,
        )
        return None

    patent_list = _format_patent_list(patents[:30])
    prompt = _LANDSCAPE_PROMPT.format(
        count=len(patents),
        query=query,
        patent_list=patent_list,
    )

    log.info("analysis: analyzing %d patents, query=%r", len(patents), query)

    raw_text = ""
    try:
        raw_text = await _llm(_LANDSCAPE_SYSTEM, prompt, max_tokens=2000)
        # Strip markdown fences some models add despite instructions
        clean = raw_text.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        structured = json.loads(clean)
    except json.JSONDecodeError as exc:
        log.error("analysis: JSON parse failed: %s\nRaw: %.500s", exc, raw_text)
        structured = {}
    except Exception as exc:
        log.error("analysis: LLM error: %s", exc, exc_info=True)
        return None

    model_label = f"{settings.llm_backend}/{settings.llm_model or 'default'}"
    result = AnalysisResult(
        ingest_run_id=ingest_run_id,
        query=query,
        patent_count=len(patents),
        model=model_label,
        analysis_text=json.dumps(structured, indent=2),
        themes=[c["theme"] for c in structured.get("technology_clusters", [])],
        top_assignees=[a["name"] for a in structured.get("top_assignees", [])],
        created_at=datetime.now(timezone.utc),
    )

    with get_session() as session:
        session.add(result)
        session.flush()
        result_id = result.id
        session.expunge(result)  # detach before close so attributes remain accessible

    log.info("analysis: stored id=%d themes=%s", result_id, result.themes)
    return result


async def generate_weekly_digest(
    new_count: int,
    sources: list[str],
    queries: list[str],
    latest_analysis: AnalysisResult | None,
) -> str:
    """Generate a plain-text weekly digest for Slack/email distribution."""
    analysis_summary = "No structured analysis available."
    if latest_analysis and latest_analysis.analysis_text:
        try:
            data = json.loads(latest_analysis.analysis_text)
            themes = [c["theme"] for c in data.get("technology_clusters", [])]
            top = [a["name"] for a in data.get("top_assignees", [])[:3]]
            analysis_summary = (
                f"Top themes: {', '.join(themes)}. "
                f"Most active assignees: {', '.join(top)}. "
                f"Strategic takeaway: {data.get('strategic_takeaway', '')}"
            )
        except Exception:
            pass

    prompt = _DIGEST_PROMPT.format(
        new_count=new_count,
        sources=", ".join(sources),
        queries=", ".join(queries),
        analysis_summary=analysis_summary,
    )
    return await _llm(_DIGEST_SYSTEM, prompt, max_tokens=800)


def _format_patent_list(patents: list[NormalizedPatent]) -> str:
    lines = []
    for i, p in enumerate(patents, 1):
        assignees = ", ".join(a["name"] for a in (p.assignees or [])[:3]) or "unknown"
        date = (p.grant_date or p.filing_date or datetime.now()).strftime("%Y-%m-%d")
        abstract_snippet = (p.abstract or "")[:300]
        lines.append(
            f"{i}. [{date}] \"{p.title}\"\n"
            f"   Source: {p.source} | Assignee: {assignees} | CPC: {', '.join((p.cpc_codes or [])[:4])}\n"
            f"   Abstract: {abstract_snippet}"
        )
    return "\n\n".join(lines)
