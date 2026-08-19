"""
Relevance-gate audit — is the "adjacent" tier an uncertainty sink?

METHOD BORROWED FROM the MSN classifier work. Building an 11-class article
classifier on MIND, that project found via confusion-matrix analysis that the
`news` category was acting as an "uncertainty sink" — a catch-all that absorbed
everything the model was unsure about, inflating apparent performance while
hiding the fact that it had learned very little about those items. They dropped
the class.

NIA's relevance gate has exactly the same failure mode available to it. It sorts
records into core / adjacent / cardiac / reject by score, and `adjacent` sits
between the accept threshold (2) and the core threshold (4). If most adjacent
records are accepted on ONE weak signal and cluster right at the boundary, then
"adjacent" is not a meaningful category — it is where the gate puts things it
cannot decide about, and every count that includes it is inflated.

This does not assume the answer. It measures:

  1. TIER DISTRIBUTION      how much of the corpus is in each tier
  2. BOUNDARY DENSITY       what fraction of accepted records sit within ±1 of
                            the accept threshold. High = the gate is deciding
                            close calls, not clear ones.
  3. SINGLE-SIGNAL RATE     what fraction of accepted records rest on exactly
                            one reason. A category that is mostly single-signal
                            is a category the gate is guessing at.
  4. REASON CONCENTRATION   if one reason explains most of a tier, that reason
                            IS the tier, and should be named as such.
  5. CPC-FREE RATE          accepted on phrases alone, with no classification
                            support at all.

Usage:
    python gate_audit.py                 # audit the live corpus
    python gate_audit.py --demo          # audit a synthetic corpus
    python main.py audit-gate
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter

from neuro_taxonomy import score_record

ACCEPT_MIN = 2
CORE_MIN = 4


def audit(records) -> dict:
    """records: iterable of (title, abstract, cpc_codes)."""
    tiers = Counter()
    scores_by_tier = {}
    reasons_by_tier = {}
    single_signal = Counter()
    boundary = Counter()
    no_cpc = Counter()
    total = 0

    for title, abstract, cpc in records:
        total += 1
        r = score_record(title, abstract, cpc)
        t = r.tier
        tiers[t] += 1
        scores_by_tier.setdefault(t, []).append(r.score)
        reasons_by_tier.setdefault(t, Counter()).update(
            [x.split(" ")[0] + " " + x.split(" ")[1] if len(x.split(" ")) > 1 else x
             for x in r.reasons[:1]])
        if len(r.reasons) <= 1:
            single_signal[t] += 1
        if t in ("core", "adjacent") and abs(r.score - ACCEPT_MIN) <= 1:
            boundary[t] += 1
        if t in ("core", "adjacent") and not any(
                "CPC" in x for x in r.reasons):
            no_cpc[t] += 1

    return {"total": total, "tiers": tiers, "scores": scores_by_tier,
            "reasons": reasons_by_tier, "single": single_signal,
            "boundary": boundary, "no_cpc": no_cpc}


def report(a: dict) -> bool:
    total = a["total"] or 1
    print(f"\n  corpus: {total:,} records\n")
    print(f"  {'TIER':<10} {'COUNT':>7} {'SHARE':>7} {'MEAN':>6} {'MIN':>5} {'MAX':>5}")
    print("  " + "-" * 48)
    for t in ("core", "adjacent", "cardiac", "reject"):
        n = a["tiers"].get(t, 0)
        if not n:
            continue
        ss = a["scores"].get(t, [0])
        print(f"  {t:<10} {n:>7,} {100*n/total:>6.1f}% "
              f"{sum(ss)/len(ss):>6.1f} {min(ss):>5} {max(ss):>5}")

    accepted = a["tiers"].get("core", 0) + a["tiers"].get("adjacent", 0)
    print(f"\n  accepted: {accepted:,} ({100*accepted/total:.1f}%)\n")

    verdicts = []
    for t in ("core", "adjacent"):
        n = a["tiers"].get(t, 0)
        if not n:
            continue
        b = 100 * a["boundary"].get(t, 0) / n
        s = 100 * a["single"].get(t, 0) / n
        c = 100 * a["no_cpc"].get(t, 0) / n
        top = a["reasons"].get(t, Counter()).most_common(3)
        conc = 100 * top[0][1] / n if top else 0
        print(f"  ── {t} ──")
        print(f"     within +-1 of accept threshold : {b:5.1f}%")
        print(f"     accepted on a single reason    : {s:5.1f}%")
        print(f"     accepted with no CPC support   : {c:5.1f}%")
        print(f"     top reason concentration       : {conc:5.1f}%  "
              f"({top[0][0] if top else '-'})")
        # A tier is a sink when its members are mostly close calls resting on
        # one signal. Either alone is tolerable; together they mean the tier is
        # where undecidable records go.
        sink = (b > 60 and s > 60)
        verdicts.append((t, sink))
        print(f"     verdict: {'UNCERTAINTY SINK' if sink else 'meaningful category'}\n")

    bad = [t for t, s in verdicts if s]
    if bad:
        print(f"  ACTION: {', '.join(bad)} is absorbing undecidable records.")
        print("  Either raise the accept threshold, add signal so these records")
        print("  are decided on evidence, or report the tier separately instead")
        print("  of folding it into headline counts.")
    else:
        print("  No tier is behaving as an uncertainty sink.")
    return not bad


DEMO = [
    # unambiguous core
    ("Adaptive deep brain stimulation using local field potentials",
     "An implantable pulse generator senses local field potentials from a lead in "
     "the subthalamic nucleus and adjusts stimulation in closed loop.",
     ["A61N1/36064", "A61N1/05"]),
    ("Implantable brain computer interface with flexible electrodes",
     "A neural probe carrying microelectrode arrays is inserted into motor cortex "
     "for neural decoding.", ["A61B5/24", "G06F3/015"]),
    ("Seizure detection from scalp electroencephalography",
     "Electroencephalogram signals are analysed to detect epileptiform discharges.",
     ["A61B5/291"]),
    # genuinely borderline — one weak signal, no classification
    ("Wearable sleep monitor", "A headband measures sleep stages.", []),
    ("Nerve stimulation device", "A device provides stimulation.", []),
    ("Neuromodulation accessory", "A cable assembly for a neuromodulation system.", []),
    ("Electrode connector", "A connector for an electrode array.", []),
    ("Signal filter for biopotential", "Filters a biopotential signal.", []),
    # clear rejects
    ("Mobile robot and control method",
     "A cleaning module and a neural network model. The robot cleaner returns to a "
     "docking station.", ["A47L11/40", "G05D1/02"]),
    ("System for training a deep neural network",
     "A convolutional neural network for image classification.", ["G06N3/045"]),
    ("Integrated digital health intervention for type 2 diabetes", "", []),
    ("Consumer wearables and psychological mechanisms", "", []),
    # cardiac
    ("Leadless cardiac pacemaker with rate response",
     "A leadless heart stimulator delivers pacing pulses.",
     ["A61N1/362", "A61N1/375"]),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--demo", action="store_true")
    ap.add_argument("--limit", type=int, default=20000)
    a = ap.parse_args()

    if a.demo:
        recs = DEMO
    else:
        try:
            from db import get_session
            from db.models import RawPatent
            with get_session() as s:
                rows = s.query(RawPatent).limit(a.limit).all()
                recs = [(r.title, r.abstract, (r.cpc_codes or []) + (r.ipc_codes or []))
                        for r in rows]
            if not recs:
                print("  corpus is empty — run `python main.py run-all` first")
                return 1
        except Exception as exc:
            print(f"  database unavailable ({exc}); use --demo")
            return 1

    ok = report(audit(recs))
    return 0 if ok else 2


if __name__ == "__main__":
    sys.exit(main())
