"""
Entity affect extraction — is this news GOOD or BAD for the entity named in it?

NIA cannot currently answer that. A company with five FDA mentions might have
five clearances or five recalls; in the graph they are identical edges. A trial
that missed its primary endpoint and one that met it both count as "a trial".
For an intelligence product that is a serious blind spot: the corpus knows what
happened and not whether it went well.

────────────────────────────────────────────────────────────────────────────
PROVENANCE OF THE DESIGN

Ported from the entity-affect work in the Microsoft / neurotech@Berkeley MSN
project. Two things there were worth keeping and one was worth discarding.

KEPT — the schema. Per entity: `text, type, sentiment, valence, arousal,
evidence`. Three properties make it good:

  * DIMENSIONAL, not just polarity. Valence (good/bad) and arousal (how
    charged) are separate axes — the circumplex model of affect. A routine
    510(k) clearance is mildly positive and low arousal; a Class I recall is
    negative and high arousal. Polarity alone collapses those.
  * EVIDENCE SPAN REQUIRED. Every judgement quotes the text it came from.
    That matches NIA's provenance discipline exactly — no claim without a
    source — and it makes hallucinated affect visible rather than silent.
  * ENTITY-LEVEL, not document-level. One article can be good for one company
    and bad for another, which document sentiment cannot express.

DISCARDED — the training approach. That project LoRA-fine-tuned
Qwen2.5-0.5B-Instruct on FinancialPhraseBank, Twitter financial sentiment, and
a custom entity-affect file. That custom file is **13 records** (10 train, 3
validation) — a schema illustration, not a dataset. Fine-tuning a 0.5B model on
it would learn the format and nothing about neurotech, and it would add a GPU
dependency to a pipeline whose entire design goal is running unattended and
free in GitHub Actions.

So the 13 records are used the way their size actually supports: as FEW-SHOT
EXEMPLARS for the LLM NIA already calls. No training, no serving, no GPU.

────────────────────────────────────────────────────────────────────────────
COST DISCIPLINE

The MSN classifier notebook describes itself as "the upstream gate for a
downstream LLM-routing pipeline" — a cheap model deciding what the expensive
one is allowed to see. Same pattern applies here, because running an LLM over
every ingested record would be slow and pointless.

`should_extract()` is that gate. Patents are excluded by construction: a patent
is a claim of invention, not an evaluation, and its affect is definitionally
neutral. Affect is extracted only for record types that can carry a verdict —
regulatory actions, trials, articles, filings, postings.

────────────────────────────────────────────────────────────────────────────
WHERE IT FEEDS

Deliberately NOT into the establishment score. A recall still corroborates that
a company is real, active and shipping — negative news is still evidence. Affect
is a THIRD, orthogonal dimension reported alongside establishment and frontier,
which is the honest modelling: "heavily corroborated, trending negative" is a
different and more useful statement than a single blended number.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

# Translated from the MSN project's FINANCE_ENTITY_TYPES. Kept deliberately
# short for the same reason the graph ontology is short — a taxonomy nobody can
# hold in their head is one the model will apply inconsistently.
NEUROTECH_ENTITY_TYPES = (
    "company",
    "research_institution",
    "regulator",             # FDA, EMA, MHRA, CMS, notified bodies
    "technology",
    "device",
    "indication",            # the condition being treated
    "clinical_outcome",
    "funding_event",
    "regulatory_action",
    "adverse_event",
    "person",
    "publication",
)

_SYSTEM = (
    "You are an information extraction system for a neurotechnology "
    "intelligence pipeline. You return strict JSON and nothing else. You never "
    "invent entities, and every judgement you make must quote the span of text "
    "that supports it."
)

# Few-shot exemplars, adapted from entity_affect_train_normalized.jsonl with the
# domain moved from finance to neurotech. Three is enough to fix the format;
# more crowds the context for no gain.
_FEWSHOT = [
    {
        "article": "Abbott received FDA approval for its deep brain stimulation "
                   "implantable pulse generator, while a separate Class I recall "
                   "was issued for a competitor's spinal cord stimulator lead.",
        "entities": [
            {"text": "Abbott", "type": "company", "sentiment": "positive",
             "valence": 0.81, "arousal": 0.62,
             "evidence": "Abbott received FDA approval for its deep brain stimulation implantable pulse generator"},
            {"text": "Class I recall", "type": "adverse_event", "sentiment": "negative",
             "valence": -0.88, "arousal": 0.83,
             "evidence": "a separate Class I recall was issued for a competitor's spinal cord stimulator lead"},
            {"text": "FDA", "type": "regulator", "sentiment": "neutral",
             "valence": 0.05, "arousal": 0.30,
             "evidence": "received FDA approval"},
        ],
    },
    {
        "article": "The pivotal trial of the speech neuroprosthesis missed its "
                   "primary endpoint, though investigators noted decoding "
                   "accuracy improved in a prespecified subgroup.",
        "entities": [
            {"text": "speech neuroprosthesis", "type": "device", "sentiment": "negative",
             "valence": -0.64, "arousal": 0.58,
             "evidence": "The pivotal trial of the speech neuroprosthesis missed its primary endpoint"},
            {"text": "primary endpoint", "type": "clinical_outcome", "sentiment": "negative",
             "valence": -0.71, "arousal": 0.55,
             "evidence": "missed its primary endpoint"},
            {"text": "decoding accuracy", "type": "clinical_outcome", "sentiment": "positive",
             "valence": 0.34, "arousal": 0.41,
             "evidence": "decoding accuracy improved in a prespecified subgroup"},
        ],
    },
]


# JSON Schema handed to the backend for CONSTRAINED DECODING. Ollama enforces
# it natively via grammar-constrained generation; Groq enforces it with
# strict:true on the gpt-oss family. Where enforced, malformed output becomes
# impossible rather than merely unlikely — the parser below stays as the
# fallback for backends that don't enforce, and as a guard against a model that
# satisfies the schema while writing nonsense.
#
# Groq's strict mode requires every property listed in `required` and
# additionalProperties:false at every level.
AFFECT_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "entities": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "type": {"type": "string", "enum": list(NEUROTECH_ENTITY_TYPES)},
                    "sentiment": {"type": "string",
                                  "enum": ["positive", "neutral", "negative"]},
                    "valence": {"type": "number"},
                    "arousal": {"type": "number"},
                    "evidence": {"type": "string"},
                },
                "required": ["text", "type", "sentiment", "valence",
                             "arousal", "evidence"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["entities"],
    "additionalProperties": False,
}


@dataclass
class AffectEntity:
    text: str
    type: str = "company"
    sentiment: str = "neutral"
    valence: float = 0.0      # -1 bad .. +1 good
    arousal: float = 0.0      #  0 routine .. 1 charged
    evidence: str = ""

    @property
    def grounded(self) -> bool:
        """A judgement with no supporting quote is not usable."""
        return bool(self.evidence and len(self.evidence) > 8)


@dataclass
class AffectResult:
    entities: list = field(default_factory=list)
    ok: bool = False
    error: str = ""

    @property
    def grounded_entities(self) -> list:
        return [e for e in self.entities if e.grounded]

    def summary(self) -> dict:
        g = self.grounded_entities
        if not g:
            return {"n": 0, "mean_valence": 0.0, "mean_arousal": 0.0,
                    "negative": 0, "positive": 0}
        return {
            "n": len(g),
            "mean_valence": round(sum(e.valence for e in g) / len(g), 3),
            "mean_arousal": round(sum(e.arousal for e in g) / len(g), 3),
            "negative": sum(1 for e in g if e.valence < -0.25),
            "positive": sum(1 for e in g if e.valence > 0.25),
        }


# ── the cheap gate ───────────────────────────────────────────────────────────

_VALENCED_TYPES = {"clearance", "approval", "trial", "article", "filing",
                   "posting", "grant"}
_VALENCE_CUES = (
    "recall", "withdraw", "terminated", "halted", "suspended", "warning letter",
    "adverse", "death", "injur", "failed", "missed", "did not meet",
    "approval", "cleared", "granted", "breakthrough designation", "de novo",
    "met the primary", "statistically significant", "acquisition", "acquired",
    "raised", "series a", "series b", "series c", "layoff", "shut down",
)


def should_extract(signal_type: str | None, title: str | None,
                   summary: str | None) -> bool:
    """
    The upstream gate: is affect extraction worth an LLM call on this record?

    Patents are excluded by construction — a patent asserts an invention, it
    does not evaluate one, so its affect is neutral by definition and paying for
    a call to discover that is waste.
    """
    st = (signal_type or "").lower()
    if st in ("patent", "thesis", "preprint"):
        return False
    if st in _VALENCED_TYPES:
        return True
    blob = f"{title or ''} {summary or ''}".lower()
    return any(c in blob for c in _VALENCE_CUES)


# ── prompt + parsing ─────────────────────────────────────────────────────────

def build_prompt(title: str | None, body: str | None, max_entities: int = 5) -> str:
    shots = "\n\n".join(
        f"Article:\n{ex['article']}\n\nJSON:\n"
        f"{json.dumps({'entities': ex['entities']}, indent=2)}"
        for ex in _FEWSHOT
    )
    article = f"{title or ''}. {body or ''}".strip()[:2400]
    return f"""Task: extract_entities_and_affect

Extract at most {max_entities} neurotech-relevant entities from the article.

Use only these entity types:
{list(NEUROTECH_ENTITY_TYPES)}

For each entity return:
- text      the entity as written
- type      one of the types above
- sentiment "positive" | "neutral" | "negative"  (for the ENTITY, not the article)
- valence   -1.0 to 1.0, how good or bad this is FOR THAT ENTITY
- arousal   0.0 to 1.0, how consequential or charged
- evidence  a VERBATIM span from the article supporting the judgement

Rules:
- Judge each entity separately. One article can be good for one company and bad
  for another.
- If the article states no evaluation of an entity, mark it neutral with
  valence near 0. Do not invent a verdict.
- evidence must be copied from the article verbatim. If you cannot quote it,
  omit the entity.
- Return strict JSON only, shaped {{"entities": [...]}}. No prose, no fences.

{shots}

Article:
{article}

JSON:"""


def parse(raw: str) -> AffectResult:
    """Parse a model response into entities, tolerating the usual mess."""
    if not raw or not raw.strip():
        return AffectResult(ok=False, error="empty response")
    txt = raw.strip()
    txt = re.sub(r"^```(?:json)?\s*", "", txt)
    txt = re.sub(r"\s*```$", "", txt)
    start = txt.find("{")
    if start == -1:
        return AffectResult(ok=False, error="no JSON object found")
    # walk braces so trailing prose after the object does not break parsing
    depth, end = 0, None
    for i, ch in enumerate(txt[start:], start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end is None:
        return AffectResult(ok=False, error="unterminated JSON object")
    try:
        data = json.loads(txt[start:end])
    except json.JSONDecodeError as exc:
        return AffectResult(ok=False, error=f"invalid JSON: {exc}")

    rows = data.get("entities") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return AffectResult(ok=False, error="no 'entities' list")

    def clamp(v, lo, hi, default=0.0):
        try:
            return max(lo, min(hi, float(v)))
        except (TypeError, ValueError):
            return default

    out = []
    for r in rows:
        if not isinstance(r, dict) or not r.get("text"):
            continue
        t = str(r.get("type", "company")).lower().strip()
        if t not in NEUROTECH_ENTITY_TYPES:
            t = "company"
        s = str(r.get("sentiment", "neutral")).lower().strip()
        if s not in ("positive", "neutral", "negative"):
            s = "neutral"
        val = clamp(r.get("valence"), -1.0, 1.0)
        # Sentiment label and valence sign must agree; the label is the more
        # reliable of the two, so it wins and the magnitude is preserved.
        if s == "positive" and val < 0:
            val = abs(val)
        elif s == "negative" and val > 0:
            val = -abs(val)
        out.append(AffectEntity(
            text=str(r["text"])[:160], type=t, sentiment=s, valence=round(val, 3),
            arousal=round(clamp(r.get("arousal"), 0.0, 1.0), 3),
            evidence=str(r.get("evidence", ""))[:400],
        ))
    return AffectResult(entities=out, ok=True)


def verify_grounding(result: AffectResult, title: str | None,
                     body: str | None) -> AffectResult:
    """
    Drop entities whose evidence span is not actually in the source text.

    The model is told to quote verbatim; this checks that it did. Without the
    check, `evidence` is decorative and a fabricated verdict looks exactly like
    a real one.
    """
    src = re.sub(r"\s+", " ", f"{title or ''}. {body or ''}").lower()
    kept = []
    for e in result.entities:
        ev = re.sub(r"\s+", " ", e.evidence).lower().strip().strip('."')
        if not ev:
            continue
        if ev in src:
            kept.append(e)
        else:
            # allow light paraphrase: most content words must be present
            words = [w for w in re.findall(r"[a-z]{4,}", ev)]
            if words and sum(1 for w in words if w in src) / len(words) >= 0.8:
                kept.append(e)
    result.entities = kept
    return result


async def extract(title: str | None, body: str | None,
                  max_entities: int = 5) -> AffectResult:
    """
    Extract entity affect using whichever LLM backend NIA is configured for.

    Fatal errors are re-raised, not swallowed. An unknown model or a bad key
    fails identically on every record, so a caller looping over a corpus must
    be able to stop rather than convert one configuration mistake into N
    identical failures.
    """
    try:
        from analysis import FatalLLMError, _llm
    except Exception as exc:
        return AffectResult(ok=False, error=f"LLM unavailable: {exc}")
    try:
        raw = await _llm(_SYSTEM, build_prompt(title, body, max_entities),
                         max_tokens=900, schema=AFFECT_SCHEMA)
    except FatalLLMError:
        raise
    except Exception as exc:
        return AffectResult(ok=False, error=f"{type(exc).__name__}: {exc}")
    return verify_grounding(parse(raw), title, body)


async def preflight() -> tuple[bool, str]:
    """
    One cheap call to prove the backend works before processing a batch.

    This exists because of a real incident: a retired Groq model produced 100
    consecutive identical 404s, one per record, and the run reported
    "100 attempted, 0 succeeded". The information needed to stop was present
    after the first call. Failing in one call rather than a hundred is the
    whole job of this function.
    """
    try:
        from analysis import FatalLLMError, _llm
    except Exception as exc:
        return False, f"LLM unavailable: {exc}"
    probe = {"type": "object",
             "properties": {"ok": {"type": "boolean"}},
             "required": ["ok"], "additionalProperties": False}
    try:
        await _llm("You reply with strict JSON only.",
                   'Reply exactly {"ok": true}', max_tokens=32, schema=probe)
        return True, ""
    except FatalLLMError as exc:
        return False, str(exc)
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


# ── offline self-test: everything except the model call ─────────────────────

def selftest() -> int:
    ok = True

    print("  ── gate: which records are worth an LLM call ──")
    cases = [
        ("patent", "Adaptive deep brain stimulation", "An implantable pulse generator...", False),
        ("clearance", "Ceribell Seizure Detection Software", "K243117 cleared.", True),
        ("trial", "Pivotal trial of speech neuroprosthesis", "Primary endpoint.", True),
        ("preprint", "Speech decoding from arrays", "We decode attempted speech.", False),
        ("article", "Synchron raises Series C", "The company raised $75M.", True),
        ("posting", "Director, Regulatory Affairs", "Lead FDA submissions.", True),
        ("thesis", "Closed-loop control policies", "A dissertation.", False),
    ]
    for st, t, b, want in cases:
        got = should_extract(st, t, b)
        mark = "  " if got == want else "!!"
        if got != want:
            ok = False
        print(f"    {mark}{st:<11} -> {'extract' if got else 'skip':<8} (want {'extract' if want else 'skip'})")

    print("\n  ── parser: the shapes models actually emit ──")
    ARTICLE_T = "Abbott received FDA approval for its DBS system"
    ARTICLE_B = ("Abbott received FDA approval for its DBS system, while a Class I "
                 "recall was issued for a competitor's lead.")
    variants = [
        ("clean JSON", '{"entities":[{"text":"Abbott","type":"company","sentiment":"positive","valence":0.8,"arousal":0.6,"evidence":"Abbott received FDA approval for its DBS system"}]}'),
        ("fenced", '```json\n{"entities":[{"text":"Abbott","type":"company","sentiment":"positive","valence":0.8,"arousal":0.6,"evidence":"Abbott received FDA approval for its DBS system"}]}\n```'),
        ("trailing prose", '{"entities":[{"text":"Abbott","type":"company","sentiment":"positive","valence":0.8,"arousal":0.6,"evidence":"Abbott received FDA approval for its DBS system"}]}\n\nHope this helps!'),
        ("bad type coerced", '{"entities":[{"text":"Abbott","type":"conglomerate","sentiment":"positive","valence":0.8,"arousal":0.6,"evidence":"Abbott received FDA approval for its DBS system"}]}'),
        ("sign disagrees", '{"entities":[{"text":"Class I recall","type":"adverse_event","sentiment":"negative","valence":0.9,"arousal":0.8,"evidence":"a Class I recall was issued for a competitor\'s lead"}]}'),
        ("out of range", '{"entities":[{"text":"Abbott","type":"company","sentiment":"positive","valence":7.5,"arousal":-3,"evidence":"Abbott received FDA approval for its DBS system"}]}'),
    ]
    for label, raw in variants:
        r = verify_grounding(parse(raw), ARTICLE_T, ARTICLE_B)
        good = r.ok and len(r.entities) == 1
        e = r.entities[0] if r.entities else None
        detail = (f"{e.type:<14} val={e.valence:+.2f} aro={e.arousal:.2f}" if e
                  else f"ERROR {r.error}")
        mark = "  " if good else "!!"
        if not good:
            ok = False
        print(f"    {mark}{label:<18} {detail}")
        if label == "sign disagrees" and e and e.valence > 0:
            print("      !! sentiment/valence disagreement not reconciled")
            ok = False
        if label == "out of range" and e and not (-1 <= e.valence <= 1 and 0 <= e.arousal <= 1):
            print("      !! values not clamped")
            ok = False

    print("\n  ── grounding: fabricated evidence must be dropped ──")
    fabricated = ('{"entities":[{"text":"Medtronic","type":"company","sentiment":"positive",'
                  '"valence":0.9,"arousal":0.7,"evidence":"Medtronic announced record '
                  'revenue growth across all segments"}]}')
    r = verify_grounding(parse(fabricated), ARTICLE_T, ARTICLE_B)
    dropped = len(r.entities) == 0
    print(f"    {'  ' if dropped else '!!'}quote absent from source -> "
          f"{'dropped' if dropped else 'KEPT (bad)'}")
    if not dropped:
        ok = False

    paraphrase = ('{"entities":[{"text":"Abbott","type":"company","sentiment":"positive",'
                  '"valence":0.8,"arousal":0.6,"evidence":"Abbott received approval for '
                  'its DBS system"}]}')
    r = verify_grounding(parse(paraphrase), ARTICLE_T, ARTICLE_B)
    kept = len(r.entities) == 1
    print(f"    {'  ' if kept else '!!'}close paraphrase          -> "
          f"{'kept' if kept else 'DROPPED (too strict)'}")
    if not kept:
        ok = False

    print("\n  ── aggregation ──")
    mixed = ('{"entities":['
             '{"text":"Abbott","type":"company","sentiment":"positive","valence":0.8,"arousal":0.6,"evidence":"Abbott received FDA approval for its DBS system"},'
             '{"text":"Class I recall","type":"adverse_event","sentiment":"negative","valence":-0.9,"arousal":0.8,"evidence":"a Class I recall was issued for a competitor\'s lead"}]}')
    r = verify_grounding(parse(mixed), ARTICLE_T, ARTICLE_B)
    s = r.summary()
    print(f"    n={s['n']}  mean_valence={s['mean_valence']:+.3f}  "
          f"mean_arousal={s['mean_arousal']:.3f}  pos={s['positive']} neg={s['negative']}")
    if not (s["n"] == 2 and s["positive"] == 1 and s["negative"] == 1):
        ok = False
        print("      !! aggregation wrong")

    print("\n  " + ("PASS — all deterministic paths correct" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    import sys
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    print(build_prompt("Abbott wins FDA approval",
                       "Abbott received FDA approval for its DBS system.")[:1500])


# ─────────────────────────────────────────────────────────────────────────────
# batch pass over the corpus
# ─────────────────────────────────────────────────────────────────────────────

async def run_batch(limit: int = 200, force: bool = False) -> dict:
    """
    Extract affect for signals that don't have it yet.

    Deliberately a SEPARATE pass rather than part of ingestion. Ingestion should
    stay fast, free and dependency-light so the nightly job is reliable; affect
    costs LLM calls and can fail without taking the corpus with it. Resumable by
    construction — it only looks at records with no stored affect.
    """
    import asyncio
    import logging

    from db import get_session
    from db.signal_models import Signal

    from analysis import FatalLLMError

    log = logging.getLogger("affect")
    stats = {"considered": 0, "gated_out": 0, "attempted": 0,
             "ok": 0, "failed": 0, "entities": 0, "ungrounded_dropped": 0,
             "aborted": "", }

    ok, why = await preflight()
    if not ok:
        stats["aborted"] = why
        log.error("affect: backend unusable, nothing attempted — %s", why)
        return stats

    with get_session() as s:
        rows = s.query(Signal).order_by(Signal.first_seen_at.desc()).limit(limit * 4).all()
        work = []
        for r in rows:
            stats["considered"] += 1
            rp = r.raw_payload or {}
            if not force and "affect" in rp:
                continue
            if not should_extract(r.signal_type, r.title, r.summary):
                stats["gated_out"] += 1
                continue
            work.append((r.id, r.title, r.summary))
            if len(work) >= limit:
                break

    # Circuit breaker. Even past preflight a backend can fail mid-run — a key
    # revoked, a rate limit that never clears. Repeating the same error for the
    # rest of the batch produces noise, burns quota and teaches nobody
    # anything, so consecutive failures stop the run.
    MAX_CONSECUTIVE = 5
    consecutive = 0

    for sid, title, summary in work:
        stats["attempted"] += 1
        try:
            res = await extract(title, summary)
        except FatalLLMError as exc:
            stats["failed"] += 1
            stats["aborted"] = str(exc)
            log.error("affect: fatal backend error, aborting batch — %s", exc)
            break
        if not res.ok:
            stats["failed"] += 1
            consecutive += 1
            log.warning("affect: signal %s failed: %s", sid, res.error)
            if consecutive >= MAX_CONSECUTIVE:
                stats["aborted"] = (
                    f"{MAX_CONSECUTIVE} consecutive failures; last: {res.error}")
                log.error("affect: %s — aborting batch", stats["aborted"])
                break
            continue
        consecutive = 0
        stats["ok"] += 1
        g = res.grounded_entities
        stats["entities"] += len(g)
        stats["ungrounded_dropped"] += len(res.entities) - len(g)
        summ = res.summary()
        with get_session() as s:
            row = s.get(Signal, sid)
            if row is not None:
                rp = dict(row.raw_payload or {})
                rp["affect"] = {
                    "summary": summ,
                    "entities": [
                        {"text": e.text, "type": e.type, "sentiment": e.sentiment,
                         "valence": e.valence, "arousal": e.arousal,
                         "evidence": e.evidence[:200]}
                        for e in g
                    ],
                }
                row.raw_payload = rp
        await asyncio.sleep(0.2)     # be polite to the backend

    return stats
