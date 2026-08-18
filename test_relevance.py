"""
Relevance-gate tests.

Every case here is a REAL record that the pipeline actually returned on
2026-08-17, or a real-world archetype of one. They exist because that run
reported BEIJING ROBOROCK (a robot-vacuum maker) and CILAG (surgical staplers)
as leading neurotech patent assignees, and reported a Type-2-diabetes digital
health trial and a consumer-wearables psychology study as neurotech signals.

Run:  python -m pytest test_relevance.py -q
 or:  python test_relevance.py
"""
from __future__ import annotations

from neuro_taxonomy import score_record

ACCEPT = ("core", "adjacent")


def kept(title, abstract="", cpc=None) -> bool:
    return score_record(title, abstract, cpc or []).tier in ACCEPT


# ── Contamination that must be rejected ─────────────────────────────────────

def test_robot_vacuum_rejected():
    """The literal cause of the 2026-08-17 credibility failure."""
    assert not kept(
        "Mobile robot and control method thereof",
        "A mobile robot includes a cleaning module and a neural network model "
        "trained to recognize obstacles. The robot cleaner returns to a docking "
        "station when the battery is low.",
        ["A47L11/40", "G05D1/02", "G06N3/08"],
    )


def test_surgical_stapler_rejected():
    assert not kept(
        "Surgical instrument with articulating end effector",
        "A surgical stapler comprising a shaft assembly and a firing member. A "
        "neural network may classify tissue thickness during the firing stroke.",
        ["A61B17/072", "A61B34/30"],
    )


def test_pure_ml_rejected():
    assert not kept(
        "System for training a deep neural network",
        "A convolutional neural network architecture for image classification "
        "using gradient descent and backpropagation.",
        ["G06N3/045"],
    )


def test_offtopic_nih_grants_rejected():
    """All four were reported as neurotech signals on 2026-08-17."""
    for title in (
        "Integrated Multimodal Digital Health Intervention for Type 2 Diabetes",
        "The Unintended Consequences of Consumer Wearables: A Mechanistic Trial",
        "Recovery, EPigenetics And Inflammaging Research after critical illness",
        "Precision quantification of diurnal activity to promote cognitive health",
    ):
        assert not kept(title), f"should reject: {title}"


# ── Genuine neurotech that must survive ─────────────────────────────────────

def test_core_neurotech_accepted():
    cases = [
        ("Adaptive deep brain stimulation based on local field potentials",
         "An implantable pulse generator senses local field potentials from a "
         "lead in the subthalamic nucleus.", ["A61N1/36064", "A61N1/05"]),
        ("Implantable brain computer interface with flexible electrode threads",
         "A neural probe carrying microelectrode arrays is inserted into motor "
         "cortex for neural decoding.", ["A61B5/24", "G06F3/015"]),
        ("Seizure detection from scalp electroencephalography",
         "Electroencephalogram signals are analysed with a convolutional neural "
         "network to detect epileptiform discharges.", ["A61B5/291"]),
        ("Sound processing strategy for a cochlear implant",
         "Maps acoustic input onto electrode stimulation channels.",
         ["H04R25/606"]),
    ]
    for t, a, c in cases:
        assert kept(t, a, c), f"should accept: {t}"


def test_real_fda_and_trial_records_accepted():
    """Real records from the 2026-08-17 run that the first gate wrongly dropped."""
    cases = [
        ("Percutaneous Stellate Ganglion Stimulation in Septic Shock",
         "peripheral nerve stimulation of the stellate ganglion"),
        ("Portable Neuromodulation Stimulator (PoNS)",
         "neuromodulation device for gait deficit"),
        ("rTMS for Anorexia Nervosa in Youth",
         "repetitive transcranial magnetic stimulation"),
        ("Effect Of Percutaneous Electrical Nerve Field Stimulation",
         "percutaneous electrical nerve field stimulation of the ear"),
        ("Axonics Sacral Neuromodulation System",
         "sacral neuromodulation for urinary urgency"),
    ]
    for t, a in cases:
        assert kept(t, a), f"should accept: {t}"


def test_no_cpc_record_still_accepted():
    """Many EPO records carry no CPC — phrases alone must be sufficient."""
    assert kept(
        "Vagus nerve stimulation system for treatment-resistant depression",
        "A nerve cuff electrode is placed on the cervical vagus nerve.",
        [],
    )


# ── Cardiac must be separable from neuro ────────────────────────────────────

def test_cardiac_not_counted_as_neuro():
    r = score_record(
        "Leadless cardiac pacemaker with rate response",
        "A leadless heart stimulator implanted in the right ventricle delivers "
        "pacing pulses based on an accelerometer signal.",
        ["A61N1/362", "A61N1/375"],
    )
    assert r.tier == "cardiac", f"expected cardiac, got {r.tier}"


# ── Query construction ──────────────────────────────────────────────────────

def test_epo_queries_use_phrases_not_single_words():
    """Regression guard for the single-word OR explosion."""
    from neuro_taxonomy import NEURO_QUERIES
    for q in NEURO_QUERIES:
        cql = q.epo_cql("2020-01-01")
        for phrase in q.phrases[:8]:
            if " " in phrase:
                assert f'ta="{phrase}"' in cql, f"{q.label}: phrase not quoted whole"
        # the old builder emitted bare single words like ta="neural"
        assert 'ta="neural"' not in cql
        assert 'ta="brain"' not in cql


def test_every_core_phrase_is_multiword_or_specific():
    """Guard the invariant that broke precision: no bare generic single words."""
    from neuro_taxonomy import CORE_PHRASES
    banned = {"neural", "brain", "electrode", "implant", "stimulation", "signal"}
    for p in CORE_PHRASES:
        assert p not in banned, f"generic single word in CORE_PHRASES: {p!r}"


if __name__ == "__main__":
    import sys, traceback
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"  PASS  {fn.__name__}")
        except Exception:
            failed += 1
            print(f"  FAIL  {fn.__name__}")
            traceback.print_exc()
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    sys.exit(1 if failed else 0)
