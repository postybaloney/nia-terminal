"""
Text substance scoring — does this document contain evidence, or announcement?

Ported from the CLAN work in the Microsoft/neurotech@Berkeley MSN project,
which computed syntactic-complexity features (clause density, mean dependency
length, Yngve and Frazier depth) over MIND news abstracts, and from the
clickbait classifier in the same repo.

WHY NIA NEEDS THIS. The pipeline now ingests newsletters, trade press and
company announcements alongside patents, grants, trials and FDA actions. To the
relevance gate they all look alike: a vendor press release saying "revolutionary
breakthrough in neuromodulation" scores exactly like an FDA summary saying
"K243117, cleared 2025-04-09". One is evidence. The other is marketing that
happens to use the right nouns. Ranking that treats them equally is how an
intelligence product loses the reader.

WHAT THIS IS, HONESTLY. The original CLAN features need a full constituency and
dependency parse (stanza). That is a heavy dependency for a job that runs
unattended in GitHub Actions, and parsing every ingested abstract nightly is not
worth the minutes. So this implements DEFENSIBLE PROXIES with no dependencies,
and a hook to consume the real CLAN feature JSON where it exists:

    proxy                          stands in for
    ------------------------------ --------------------------------------
    clause-marker density          CLAN "Density of Clauses"
    mean words between commas      CLAN "Mean Dependency Length"
    subordination ratio            Yngve / Frazier embedding depth
    mean sentence length           (used directly by CLAN too)

They correlate with the real thing, but they are not the real thing, and any
claim built on them should say "proxy". `from_clan()` swaps in the true
features when the parsed JSON is available.

The scores are deliberately NOT combined into one number the way the
establishment/frontier axes are — substance and promotion are different
questions and a document can score high on both (a well-written launch
announcement with real trial data in it).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

# ── Promotional register ─────────────────────────────────────────────────────
# Drawn from the clickbait work, narrowed to the language that actually shows up
# in medtech and neurotech announcements.
HYPE_PHRASES = (
    "revolutionary", "revolutionize", "revolutionise", "groundbreaking",
    "game-chang", "game chang", "breakthrough", "unprecedented",
    "world's first", "world first", "first-ever", "first ever",
    "leading provider", "market leader", "best-in-class", "best in class",
    "cutting-edge", "cutting edge", "state-of-the-art", "state of the art",
    "next-generation", "next generation", "paradigm shift", "disrupt",
    "excited to announce", "thrilled to announce", "proud to announce",
    "pleased to announce", "poised to", "set to transform", "transformative",
    "pioneering", "visionary", "unlock the power", "redefine", "supercharge",
    "unmatched", "unrivaled", "unrivalled", "seamlessly", "effortlessly",
)

# ── Evidence register ────────────────────────────────────────────────────────
# Things that are hard to write unless you actually have a result.
EVIDENCE_PATTERNS = (
    (r"\bNCT\d{8}\b", 3.0, "trial registration"),
    (r"\bK\d{6}\b", 3.0, "510(k) number"),
    (r"\bP\d{6}\b", 3.0, "PMA number"),
    (r"\b10\.\d{4,9}/\S+", 2.5, "DOI"),
    (r"\b[A-Z]{2}\d{6,}[A-Z]?\d?\b", 2.0, "patent number"),
    (r"\bp\s*[=<>]\s*0?\.\d+", 3.0, "p-value"),
    (r"\bn\s*=\s*\d+", 2.5, "sample size"),
    (r"\b\d+(\.\d+)?\s*%", 1.2, "percentage"),
    (r"\b\d+(\.\d+)?\s?(mm|cm|µm|um|nm|hz|khz|mhz|ma|mv|µv|uv|ms|kg|mg)\b", 1.5, "measurement"),
    (r"\bconfidence interval\b|\b95%\s*ci\b", 3.0, "confidence interval"),
    (r"\brandomi[sz]ed\b", 2.5, "randomised"),
    (r"\bdouble[- ]blind\b|\bsham[- ]controlled\b", 3.0, "controlled design"),
    (r"\bcohort\b|\blongitudinal\b|\bcrossover\b", 1.5, "study design"),
    (r"\bprimary endpoint\b|\bsecondary endpoint\b", 2.5, "endpoint"),
    (r"\bpre[- ]?registered\b", 2.0, "preregistration"),
)

# Scientific hedging is a QUALITY signal, not a weakness — it marks calibrated
# claims. Its absence alongside heavy hype is the thing worth flagging.
HEDGES = (
    "suggests", "suggest that", "may indicate", "consistent with", "appears to",
    "preliminary", "we hypothesi", "further work", "remains unclear",
    "limitations", "not statistically significant", "requires validation",
)

CLAUSE_MARKERS = (
    " that ", " which ", " because ", " although ", " whereas ", " while ",
    " when ", " where ", " after ", " before ", " unless ", " whether ",
    " since ", " though ", " if ", " who ", " whom ", " whose ",
)


@dataclass
class TextQuality:
    substance: float = 0.0        # 0..1  evidence density, length-normalised
    promotion: float = 0.0        # 0..1  promotional register
    complexity: float = 0.0       # 0..1  syntactic complexity (proxy)
    hedging: float = 0.0          # 0..1  calibrated-claim markers
    n_words: int = 0
    evidence_found: list = field(default_factory=list)
    hype_found: list = field(default_factory=list)
    proxy: bool = True            # False once real CLAN features are supplied

    @property
    def is_announcement(self) -> bool:
        """High promotional register with little to back it."""
        return self.promotion > 0.35 and self.substance < 0.20

    @property
    def weight(self) -> float:
        """
        Multiplier for how much this document should count as evidence.
        Bounded to [0.4, 1.3]: a promotional item is discounted, never erased —
        a launch announcement is still a real event, just weaker evidence than
        a clearance record.
        """
        w = 1.0 + 0.45 * self.substance - 0.5 * self.promotion + 0.15 * self.hedging
        return max(0.4, min(1.3, w))


def _sentences(text: str) -> list[str]:
    parts = re.split(r"(?<=[.!?])\s+", text or "")
    return [p.strip() for p in parts if p.strip()]


def score(text: str | None, title: str | None = None) -> TextQuality:
    blob = f"{title or ''}. {text or ''}".strip()
    low = " " + blob.lower() + " "
    words = re.findall(r"[A-Za-z][A-Za-z'-]*", blob)
    n = len(words)
    q = TextQuality(n_words=n)
    if n < 12:
        return q      # too short to judge; stay neutral rather than guess

    # ── evidence ────────────────────────────────────────────────────────────
    ev = 0.0
    for pat, wgt, label in EVIDENCE_PATTERNS:
        hits = len(re.findall(pat, blob, flags=re.I))
        if hits:
            ev += wgt * min(hits, 3)
            q.evidence_found.append(label)
    # per-100-words so a long puff piece cannot out-score a short precise one
    q.substance = min(1.0, (ev / max(n, 1)) * 100.0 / 12.0)

    # ── promotion ───────────────────────────────────────────────────────────
    hy = 0
    for p in HYPE_PHRASES:
        if p in low:
            hy += 1
            q.hype_found.append(p)
    q.promotion = min(1.0, (hy / max(n, 1)) * 100.0 / 1.6)

    # ── hedging ─────────────────────────────────────────────────────────────
    hd = sum(1 for h in HEDGES if h in low)
    q.hedging = min(1.0, (hd / max(n, 1)) * 100.0 / 1.2)

    # ── complexity proxies (stand-ins for the CLAN parse features) ──────────
    sents = _sentences(blob) or [blob]
    mean_len = n / max(len(sents), 1)
    clause_density = sum(low.count(m) for m in CLAUSE_MARKERS) / max(len(sents), 1)
    commas = blob.count(",")
    mean_span = n / max(commas + len(sents), 1)      # ~ mean dependency length
    q.complexity = min(1.0, (
        0.40 * min(mean_len / 28.0, 1.0)
        + 0.35 * min(clause_density / 2.2, 1.0)
        + 0.25 * min(mean_span / 14.0, 1.0)
    ))
    return q


def from_clan(clan_features: dict, base: TextQuality | None = None) -> TextQuality:
    """
    Replace the complexity proxy with real CLAN features.

    Expects one document's entry from the MSN project's features JSON, e.g.
    small_train_features.json — the shape with "Density of Clauses",
    "Mean Dependency Length" and per-sentence Yngve/Frazier scores.
    """
    q = base or TextQuality()
    try:
        dens = clan_features.get("Density of Clauses") or []
        mdl = clan_features.get("Mean Dependency Length") or []
        sf = (clan_features.get("sentence_features") or {}).values()
        yngve = [s.get("yngve_mean", 0.0) for s in sf if isinstance(s, dict)]
        frazier = [s.get("frazier_mean", 0.0) for s in sf if isinstance(s, dict)]
        mean = lambda xs: (sum(xs) / len(xs)) if xs else 0.0
        q.complexity = min(1.0, (
            0.30 * min(mean(dens) / 3.0, 1.0)
            + 0.30 * min(mean(mdl) / 4.0, 1.0)
            + 0.20 * min(mean(yngve) / 3.5, 1.0)
            + 0.20 * min(mean(frazier) / 2.5, 1.0)
        ))
        q.proxy = False
    except Exception:
        pass
    return q


if __name__ == "__main__":
    SAMPLES = [
        ("PRESS RELEASE",
         "NeuroCorp Announces Revolutionary Breakthrough in Neuromodulation",
         "NeuroCorp, a leading provider of cutting-edge neurotechnology, is "
         "thrilled to announce a groundbreaking, world's first platform poised "
         "to revolutionize the treatment landscape. This state-of-the-art, "
         "next-generation system seamlessly transforms patient care and is "
         "set to disrupt the industry with unmatched performance."),
        ("TRIAL RESULT",
         "Adaptive deep brain stimulation in Parkinson disease",
         "In this randomized, double-blind crossover study (n = 128, "
         "NCT06120491), adaptive stimulation reduced motor fluctuation time by "
         "31.4% versus continuous stimulation (p = 0.003, 95% CI 18.2-44.6). "
         "The primary endpoint was met. Results suggest benefit may extend to "
         "patients with tremor-dominant presentation, although further work is "
         "required and the sample limits generalisation."),
        ("FDA CLEARANCE",
         "Ceribell Infant Seizure Detection Software",
         "Device K243117 cleared 2025-11-21. Software analyses "
         "electroencephalography acquired at 256 Hz from a 10-electrode array "
         "to detect electrographic seizure in neonates."),
        ("PREPRINT ABSTRACT",
         "Speech decoding from intracortical microelectrode arrays",
         "We decode attempted speech from a 128-channel array implanted in "
         "motor cortex, which achieves 62.1% word accuracy on a 1024-word "
         "vocabulary, although performance degrades when the participant is "
         "fatigued. These preliminary results suggest that chronic recording "
         "stability remains the limiting factor."),
    ]
    print(f"{'DOCUMENT':<18} {'SUBST':>6} {'PROMO':>6} {'CPLX':>6} {'HEDGE':>6} {'WEIGHT':>7}  VERDICT")
    print("-" * 92)
    for label, title, body in SAMPLES:
        q = score(body, title)
        verdict = "ANNOUNCEMENT" if q.is_announcement else "evidence-bearing"
        print(f"{label:<18} {q.substance:6.2f} {q.promotion:6.2f} {q.complexity:6.2f} "
              f"{q.hedging:6.2f} {q.weight:7.2f}  {verdict}")
        if q.evidence_found:
            print(f"{'':18} evidence: {', '.join(sorted(set(q.evidence_found))[:6])}")
        if q.hype_found:
            print(f"{'':18} hype:     {', '.join(q.hype_found[:6])}")
