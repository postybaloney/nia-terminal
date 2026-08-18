"""
Controlled vocabulary for NIA — the single source of truth for
"what counts as neurotech" and "what the knowledge graph is allowed to contain".

Two jobs:

  1. RELEVANCE  — score a record 0..N so the pipeline can reject the
     non-neurotech noise that broke the 2026-08-17 run (a query of
     `ta="neural" OR ta="stimulation" OR ...` matched every "neural
     network" patent on earth, which is why BEIJING ROBOROCK — a robot
     vacuum company — ranked as a top "neurotech" assignee).

  2. ONTOLOGY   — a deliberately SMALL, FIXED set of node types and
     predicates for the knowledge graph. A graph with forty node types is
     not interpretable by a human. Four types and nine predicates is.

Design note on the relevance gate: it SCORES rather than hard-ANDs. A hard
`phrase AND cpc` gate silently destroys recall whenever a classification
code is wrong or missing (many EPO records carry no CPC at all). Scoring
degrades gracefully, and — more useful for an intelligence product — every
accept/reject carries a human-readable reason you can show to a skeptic.

CPC codes below were verified against the official USPTO CPC scheme
(uspto.gov/web/patents/classification/cpc/html/) on 2026-08-18, not recalled
from memory. Note in particular that EEG is A61B5/291 — NOT A61B5/369, which
is a commonly repeated error.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

# ─────────────────────────────────────────────────────────────────────────────
# CPC / IPC classification codes
# ─────────────────────────────────────────────────────────────────────────────
# CPC subgroup hierarchy is by INDENTATION, not numeric prefix — A61B5/291 is a
# child of A61B5/24 despite not sharing a numeric prefix. So we enumerate
# explicitly rather than relying on startswith() against a parent.

CORE_NEURO_CPC: tuple[str, ...] = (
    # ── Neurostimulation (A61N1 — "Electrotherapy") ──────────────────────────
    "A61N1/36",      # "for stimulation" (parent of the neuromod subtree)
    "A61N1/05",      # "for implantation or insertion into the body"
    "A61N2/00",      # Magnetotherapy  (this is where TMS lives)
    "A61N2/02",      # using magnetic fields produced by coils
    # ── Bioelectric sensing (A61B5/24 subtree) ───────────────────────────────
    "A61B5/24",      # "Detecting, measuring or recording bioelectric or
                     #  biomagnetic signals of the body or parts thereof"
    "A61B5/25",      # bioelectric electrodes
    "A61B5/268",     # electrodes containing conductive polymers (PEDOT:PSS)
    "A61B5/291",     # "for electroencephalography [EEG]"      <-- NOT /369
    "A61B5/293",     # "Invasive"  (ECoG / depth electrodes)
    "A61B5/294",
    "A61B5/296",
    "A61B5/369",     # EEG under the post-2021 reorganisation (kept for safety —
                     # an unused code costs nothing, a missing one costs recall)
    "A61B5/372",     # magnetoencephalography [MEG]
    "A61B5/374",     # evoked potentials
    "A61B5/375",
    "A61B5/377",
    "A61B5/378",
    "A61B5/389",     # electromyography [EMG]
    "A61B5/397",
    "A61B5/398",     # electroretinography [ERG]
    # ── Brain-computer interfaces ────────────────────────────────────────────
    "G06F3/015",     # "Input arrangements based on nervous system activity
                     #  detection, e.g. brain waves [EEG] detection,
                     #  electromyograms [EMG] detection"  <-- THE BCI class
    # ── Neuroprosthetics ─────────────────────────────────────────────────────
    "A61F2/72",      # myoelectric / bioelectric control of prosthetic limbs
)

# Adjacent — real neurotech, but a weaker signal on its own.
#
# A61N1/372, /375, /378 sit here rather than in CORE on purpose: they describe
# generic implantable-stimulator hardware (telemetry, casings, power) and are
# carried by cardiac pacemakers just as often as by neurostimulators. Scoring
# them as core filed every leadless pacemaker as neurotech.
ADJACENT_NEURO_CPC: tuple[str, ...] = (
    "A61N1/372",     # arrangements in connection with implantation of stimulators
    "A61N1/375",     # constructional arrangements, e.g. casings
    "A61N1/378",     # power supply for implantable stimulators
    "H04R25",        # hearing aids (cochlear implants live in /606)
    "A61F9/08",      # ophthalmic / retinal prosthetic territory
    "A61B5/0476",    # legacy pre-2021 EEG code, still on older records
    "A61B5/04",      # legacy pre-2021 bioelectric parent
    "A61M21",        # devices influencing mental state (sleep/relaxation)
    "A61B5/16",      # psychotechnic / mental-state testing
    # Added 2026-08-18 — non-electrical neuro modalities. Verified against the
    # official USPTO CPC scheme; codes I could NOT verify (A61N7 ultrasound
    # therapy, A61B8 ultrasound diagnostics) are deliberately omitted rather
    # than guessed. The scoring gate degrades gracefully without them because
    # the phrase list below carries these modalities on its own.
    "A61B5/055",     # "involving electronic [EMR] or nuclear [NMR] magnetic
                     #  resonance, e.g. magnetic resonance imaging"  -> fMRI
    "A61B5/0059",    # "using light, e.g. diagnosis by transillumination,
                     #  diascopy, fluorescence"  -> fNIRS / optical brain imaging
    "A61B5/1455",    # optical sensors, spectrophotometric oximeters
)

# Hard exclusions — the actual source of the Roborock/Cilag contamination.
# A record carrying ONLY these is not neurotech no matter what words it uses.
EXCLUDE_CPC: tuple[str, ...] = (
    "A47L",          # domestic washing/cleaning — ROBOT VACUUMS
    "G05D",          # control of position/course of vehicles & robots
    "B25J",          # manipulators / industrial robots
    "G06N",          # artificial neural networks & ML per se
    "A61B17",        # general surgical instruments (Cilag/J&J territory)
    "A61B18",        # surgical instruments using heat/electricity for cutting
    "A01D", "A01B",  # agricultural machinery
    "B60W", "B60R",  # vehicle control
    "H04N",          # pictorial communication / TV
    "G06Q",          # commerce / business methods
)

# Cardiac stimulation shares A61N1/36 with neurostimulation. Keep them
# separable so "neurotech" and "medtech" can be reported as distinct universes
# — NIA is the *Neurotech* Intelligence Agency, and a corpus half full of
# pacemakers undermines exactly that claim.
CARDIAC_CPC: tuple[str, ...] = (
    "A61N1/362",     # "Heart stimulators"
    "A61N1/365",
    "A61N1/368",
    "A61N1/39",      # defibrillators
    "A61B5/308",     # ECG
    "A61B5/33",
)


# ─────────────────────────────────────────────────────────────────────────────
# Phrases
# ─────────────────────────────────────────────────────────────────────────────
# Multi-word phrases ONLY. Single words like "neural" or "brain" are what
# destroyed precision in the first place and must never appear here alone.

CORE_PHRASES: tuple[str, ...] = (
    "brain computer interface", "brain-computer interface", "brain machine interface",
    "brain-machine interface", "neural interface", "neural implant",
    "deep brain stimulation", "spinal cord stimulation", "vagus nerve stimulation",
    "sacral neuromodulation", "peripheral nerve stimulation", "cortical stimulation",
    "transcranial magnetic stimulation", "transcranial direct current",
    "responsive neurostimulation", "closed loop stimulation", "closed-loop stimulation",
    "adaptive stimulation", "neuromodulation", "neurostimulation", "neurostimulator",
    "implantable pulse generator", "stimulation lead", "electrode array",
    "microelectrode array", "intracortical electrode", "depth electrode",
    "cochlear implant", "auditory brainstem implant", "retinal prosthesis",
    "retinal implant", "visual prosthesis", "neuroprosthesis", "neuroprosthetic",
    "motor cortex", "somatosensory cortex", "basal ganglia", "subthalamic nucleus",
    "electroencephalography", "electroencephalogram", "electrocorticography",
    "magnetoencephalography", "local field potential", "action potential",
    "spike sorting", "neural decoding", "neural signal", "neuronal activity",
    "evoked potential", "seizure detection", "seizure prediction",
    "epileptiform", "optogenetic", "neural dust", "neuropixels",
    "peripheral nerve", "cranial nerve", "neural recording", "neural probe",
    "brain stimulation", "cortical implant", "nerve cuff",
    # Added after testing against real NIH/ClinicalTrials records from the
    # 2026-08-17 run, which this list was silently dropping.
    "nerve stimulation", "ganglion stimulation", "stellate ganglion",
    "sphenopalatine", "occipital nerve", "hypoglossal nerve",
    "trigeminal nerve", "phrenic nerve", "tibial nerve", "sacral nerve",
    "rtms", "repetitive transcranial", "electrical nerve", "nerve field",
    "autonomic modulation", "bioelectronic medicine", "neural circuit",
    # ── Modalities added 2026-08-18 ──────────────────────────────────────────
    # Prompted by two 2026 events the original list would have missed entirely:
    #   * Apple/Q.ai ($1.6B, Jan 2026) — facial skin micromovements decoding
    #     mouthed speech. A non-invasive neural interface by any reasonable
    #     definition, and NOT matched by a single phrase in the original list.
    #   * the general drift of the field toward optical, acoustic and
    #     radar-based sensing rather than electrodes.
    # Discipline is unchanged: multi-word phrases or highly specific acronyms
    # only. Never a bare generic word — that is what broke precision the first
    # time.
    "functional near-infrared", "near-infrared spectroscopy", "fnirs",
    "diffuse optical tomography", "functional magnetic resonance", "fmri",
    "functional ultrasound imaging", "focused ultrasound", "transcranial ultrasound",
    "acoustoelectric", "photoacoustic", "photobiomodulation",
    "silent speech", "subvocal", "sub-vocal", "sub-vocalization",
    "facial micromovement", "facial skin micromovement", "mouthed speech",
    "electromyography", "surface electromyography", "myoelectric",
    "millimeter wave radar", "radar vital sign", "contactless vital sign",
    "eye tracking neural", "pupillometry",
)

# Words that make "neural"/"network" mean the ML sense rather than the
# biological one. Used only to break ties, never as a sole rejection.
ML_SENSE_MARKERS: tuple[str, ...] = (
    "convolutional neural network", "recurrent neural network",
    "artificial neural network", "deep neural network", "neural network model",
    "neural network architecture", "graph neural network", "transformer model",
)

ROBOT_MARKERS: tuple[str, ...] = (
    "cleaning robot", "robot cleaner", "vacuum cleaner", "mobile robot",
    "autonomous vehicle", "lawn mower", "self-propelled", "docking station",
    "surgical stapler", "suture", "trocar", "endoscope shaft",
)

ANATOMY_MARKERS: tuple[str, ...] = (
    "brain", "cortex", "cortical", "neuron", "neuronal", "neural tissue",
    "nerve", "spinal", "cranial", "scalp", "cerebral", "thalamus",
    "hippocamp", "patient", "implant",
)


# ─────────────────────────────────────────────────────────────────────────────
# Query construction  (replaces the broken single-word OR builder)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class NeuroQuery:
    """One precise search, expressed per-backend."""
    label: str
    phrases: tuple[str, ...]          # quoted phrases, OR'd
    cpc: tuple[str, ...] = ()         # optional classification narrowing
    tier: str = "core"                # core | adjacent | medtech

    def epo_cql(self, since: str) -> str:
        """
        Build a CQL query for EPO OPS.

        The old builder produced:
            (ta="neural" OR ta="stimulation" OR ta="brain") AND pd>=20200101
        which matches any document containing ANY of those words.

        This produces:
            (ta="deep brain stimulation" OR ta="subthalamic nucleus") AND pd>=...
        Quoted multi-word values are phrase-matched by OPS, so precision comes
        from the phrase itself rather than from a downstream filter.
        """
        since_compact = since.replace("-", "")
        # OPS rejects very long queries; keep well inside the limit.
        phrases = self.phrases[:8]
        terms = " OR ".join(f'ta="{p}"' for p in phrases)
        cql = f"({terms}) AND pd>={since_compact}"
        if self.cpc:
            cpc_terms = " OR ".join(f'cpc="{c}"' for c in self.cpc[:4])
            cql = f"({terms}) AND ({cpc_terms}) AND pd>={since_compact}"
        return cql


NEURO_QUERIES: tuple[NeuroQuery, ...] = (
    NeuroQuery(
        "deep-brain-stimulation",
        ("deep brain stimulation", "subthalamic nucleus", "globus pallidus",
         "implantable pulse generator", "stimulation lead"),
        cpc=("A61N1/36",),
    ),
    NeuroQuery(
        "brain-computer-interface",
        ("brain computer interface", "brain-computer interface",
         "brain machine interface", "neural decoding", "intracortical electrode",
         "motor cortex", "cortical implant"),
        cpc=("G06F3/015", "A61B5/24"),
    ),
    NeuroQuery(
        "spinal-and-peripheral",
        ("spinal cord stimulation", "peripheral nerve stimulation",
         "vagus nerve stimulation", "sacral neuromodulation", "nerve cuff",
         "dorsal root ganglion"),
        cpc=("A61N1/36", "A61N1/05"),
    ),
    NeuroQuery(
        "neural-recording-hardware",
        ("microelectrode array", "neural probe", "neural recording",
         "local field potential", "spike sorting", "electrocorticography",
         "depth electrode"),
        cpc=("A61B5/24", "A61B5/25"),
    ),
    NeuroQuery(
        "eeg-and-seizure",
        ("electroencephalography", "electroencephalogram", "seizure detection",
         "seizure prediction", "epileptiform", "responsive neurostimulation"),
        cpc=("A61B5/291", "A61B5/369"),
    ),
    NeuroQuery(
        "noninvasive-stimulation",
        ("transcranial magnetic stimulation", "transcranial direct current",
         "transcranial electrical stimulation", "focused ultrasound neuromodulation"),
        cpc=("A61N2/00", "A61N2/02"),
    ),
    NeuroQuery(
        "sensory-prosthetics",
        ("cochlear implant", "auditory brainstem implant", "retinal prosthesis",
         "retinal implant", "visual prosthesis", "epiretinal"),
        cpc=("H04R25", "A61F9/08"),
    ),
    NeuroQuery(
        "neuroprosthetics",
        ("neuroprosthesis", "neuroprosthetic", "myoelectric control",
         "functional electrical stimulation", "limb prosthesis bioelectric"),
        cpc=("A61F2/72",),
    ),
    NeuroQuery(
        "closed-loop-adaptive",
        ("closed loop stimulation", "closed-loop neuromodulation",
         "adaptive deep brain stimulation", "responsive neurostimulation",
         "biomarker guided stimulation"),
        cpc=("A61N1/36",),
    ),
    NeuroQuery(
        "optogenetics-and-novel",
        ("optogenetic stimulation", "neural dust", "ultrasonic neuromodulation",
         "magnetothermal stimulation", "injectable electrode"),
        tier="adjacent",
    ),
)


# ─────────────────────────────────────────────────────────────────────────────
# Relevance scoring
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Relevance:
    score: int
    tier: str                     # core | adjacent | cardiac | reject
    reasons: list[str] = field(default_factory=list)

    @property
    def accepted(self) -> bool:
        return self.tier != "reject"


def _norm_code(code: str) -> str:
    """EPO/USPTO emit codes in many shapes: 'A61N   1/  36', 'A61N1/36'."""
    return re.sub(r"\s+", "", (code or "").upper()).replace("A61N1/036", "A61N1/36")


def _matches(
    codes: list[str],
    prefixes: tuple[str, ...],
    exclude: tuple[str, ...] = (),
) -> list[str]:
    """
    Prefix-match classification codes.

    `exclude` exists because CPC prefixes overlap across meanings: A61N1/362
    ("Heart stimulators") starts with A61N1/36 ("for stimulation"), so a naive
    prefix match files every pacemaker as core neurotech. The more specific
    code must win, so cardiac subgroups are subtracted before scoring.
    """
    normed = [_norm_code(c) for c in codes or []]
    hits = []
    for c in normed:
        if exclude and any(c.startswith(e.upper()) for e in exclude):
            continue
        for p in prefixes:
            if c.startswith(p.upper()):
                hits.append(c)
                break
    return hits


def _phrase_hits(text: str, phrases: tuple[str, ...]) -> list[str]:
    if not text:
        return []
    low = text.lower()
    return [p for p in phrases if p in low]


def score_record(
    title: str | None,
    abstract: str | None,
    cpc_codes: list[str] | None = None,
    ipc_codes: list[str] | None = None,
) -> Relevance:
    """
    Score a record for neurotech relevance.

    Accept at >= 2. Every decision carries a reason string, so the pipeline can
    explain itself — which matters when someone asks "why is this in here?"
    """
    title = title or ""
    abstract = abstract or ""
    codes = list(cpc_codes or []) + list(ipc_codes or [])
    r = Relevance(score=0, tier="reject")

    # Cardiac first — those codes are more specific than the neuro prefixes
    # they sit under, so they must be subtracted before core matching.
    card_cpc = _matches(codes, CARDIAC_CPC)
    core_cpc = _matches(codes, CORE_NEURO_CPC, exclude=CARDIAC_CPC)
    adj_cpc = _matches(codes, ADJACENT_NEURO_CPC, exclude=CARDIAC_CPC)
    excl_cpc = _matches(codes, EXCLUDE_CPC)

    title_hits = _phrase_hits(title, CORE_PHRASES)
    abs_hits = _phrase_hits(abstract, CORE_PHRASES)

    if core_cpc:
        r.score += 3
        r.reasons.append(f"core CPC {core_cpc[0]}")
    if adj_cpc:
        r.score += 1
        r.reasons.append(f"adjacent CPC {adj_cpc[0]}")
    if title_hits:
        r.score += 2
        r.reasons.append(f"title phrase '{title_hits[0]}'")
    if abs_hits:
        r.score += 2
        r.reasons.append(f"abstract phrase '{abs_hits[0]}'")

    # ── Penalties ────────────────────────────────────────────────────────────
    blob = f"{title} {abstract}".lower()

    if excl_cpc and not core_cpc:
        r.score -= 4
        r.reasons.append(f"excluded CPC {excl_cpc[0]}")

    ml_hits = _phrase_hits(blob, ML_SENSE_MARKERS)
    anatomy = _phrase_hits(blob, ANATOMY_MARKERS)
    if ml_hits and not anatomy and not core_cpc:
        r.score -= 3
        r.reasons.append(f"ML sense of 'neural' ('{ml_hits[0]}'), no anatomy")

    robot_hits = _phrase_hits(blob, ROBOT_MARKERS)
    if robot_hits and not core_cpc:
        r.score -= 4
        r.reasons.append(f"robotics/surgical marker '{robot_hits[0]}'")

    # ── Tier ─────────────────────────────────────────────────────────────────
    # Cardiac verdict keys off PHRASES, not codes. A record carrying cardiac
    # classification with zero neuro phrases anywhere is cardiac, even if it
    # also carries a shared implantable-electrode code like A61N1/05
    # (whose own CPC text reads "e.g. heart electrode").
    if card_cpc and not (title_hits or abs_hits):
        r.tier = "cardiac"
        r.reasons.append("cardiac classification, no neuro phrase present")
        return r

    if r.score >= 4:
        r.tier = "core"
    elif r.score >= 2:
        r.tier = "adjacent"
    else:
        r.tier = "reject"
    return r


# ─────────────────────────────────────────────────────────────────────────────
# Graph ontology
# ─────────────────────────────────────────────────────────────────────────────
# Deliberately tiny. Modelled on Glitch-Cat-Club/graph-memory-starter, whose
# central lesson is that a FIXED, SMALL ontology is what makes a knowledge graph
# interpretable and cheap to traverse — spend the intelligence at build time so
# query time is a structural lookup rather than a reasoning chain.
#
# Four node types. Nine predicates. If you cannot draw the legend on one line,
# the graph is not interpretable.

NODE_TYPES: tuple[str, ...] = (
    "ORG",      # company, university, hospital, agency
    "PERSON",   # inventor, author, principal investigator, advisor
    "TECH",     # a technology concept (from the controlled list above)
    "WORK",     # any dated artefact — see WORK_SUBTYPES
)

WORK_SUBTYPES: tuple[str, ...] = (
    "patent", "paper", "preprint", "thesis", "trial",
    "grant", "clearance", "posting", "filing", "article",
)

PREDICATES: tuple[str, ...] = (
    "filed_by",         # WORK  -> ORG     patent assignee
    "authored_by",      # WORK  -> PERSON  inventor / author / PI
    "affiliated_with",  # PERSON-> ORG     employer / institution
    "about",            # WORK  -> TECH    subject matter
    "funded_by",        # WORK  -> ORG     grant or trial sponsor
    "sponsored_by",     # WORK  -> ORG     trial sponsor
    "hiring_for",       # ORG   -> TECH    job posting as capability signal
    "advances_to",      # WORK  -> WORK    preprint->paper, thesis->patent
    "cites",            # WORK  -> WORK
)

# Canonical technology concepts — graph TECH nodes are drawn only from here,
# so the concept layer stays readable instead of turning into a keyword cloud.
TECH_CONCEPTS: dict[str, tuple[str, ...]] = {
    "Deep Brain Stimulation": ("deep brain stimulation", "subthalamic nucleus",
                               "globus pallidus", "adaptive deep brain stimulation"),
    "Brain-Computer Interface": ("brain computer interface", "brain-computer interface",
                                 "brain machine interface", "neural decoding",
                                 "cortical implant", "intracortical electrode"),
    "Spinal Cord Stimulation": ("spinal cord stimulation", "dorsal root ganglion"),
    "Vagus Nerve Stimulation": ("vagus nerve stimulation", "cranial nerve"),
    "Neural Recording Hardware": ("microelectrode array", "neural probe",
                                  "neural recording", "spike sorting",
                                  "local field potential", "neuropixels"),
    "EEG & Seizure": ("electroencephalography", "electroencephalogram",
                      "seizure detection", "seizure prediction", "epileptiform",
                      "electrocorticography"),
    "Non-invasive Stimulation": ("transcranial magnetic stimulation",
                                 "transcranial direct current",
                                 "focused ultrasound neuromodulation"),
    "Cochlear & Auditory": ("cochlear implant", "auditory brainstem implant"),
    "Retinal & Visual": ("retinal prosthesis", "retinal implant",
                         "visual prosthesis", "epiretinal"),
    "Neuroprosthetics": ("neuroprosthesis", "neuroprosthetic", "myoelectric control",
                         "functional electrical stimulation"),
    "Closed-Loop Neuromodulation": ("closed loop stimulation",
                                    "closed-loop stimulation", "adaptive stimulation",
                                    "responsive neurostimulation",
                                    "biomarker guided stimulation"),
    "Optogenetics": ("optogenetic", "optogenetics", "photostimulation"),
    "Functional Neuroimaging": ("functional near-infrared", "near-infrared spectroscopy",
                                "fnirs", "diffuse optical tomography",
                                "functional magnetic resonance", "fmri",
                                "magnetoencephalography", "functional ultrasound imaging"),
    "Focused Ultrasound": ("focused ultrasound", "transcranial ultrasound",
                           "acoustoelectric", "histotripsy"),
    "Silent Speech & Facial Sensing": ("silent speech", "subvocal", "sub-vocal",
                                       "sub-vocalization", "facial micromovement",
                                       "facial skin micromovement", "mouthed speech",
                                       "surface electromyography"),
    "Contactless Sensing": ("millimeter wave radar", "radar vital sign",
                            "contactless vital sign", "photoacoustic",
                            "photobiomodulation"),
}

# ── Deliberately OUT of scope ────────────────────────────────────────────────
# NIA is the *Neurotech* Intelligence Agency, and scope discipline is what makes
# the corpus trustworthy. Things that are medtech but not neurotech stay out
# even when they are the loudest story of the month — e.g. Midjourney Medical's
# $74M whole-body ultrasound screening business (June 2026) is medical imaging,
# not a neural interface, and including it would start the same slide that put
# robot vacuums in the patent corpus.
#
# The judgement call to re-examine periodically: a company entering from
# outside the sector with a genuinely neural product (Apple/Q.ai) SHOULD be
# caught; one entering adjacent-but-not-neural (Midjourney) should not.


def classify_tech(title: str | None, abstract: str | None) -> list[str]:
    """Map free text onto the canonical concept list. Empty = no confident match."""
    blob = f"{title or ''} {abstract or ''}".lower()
    out = []
    for concept, markers in TECH_CONCEPTS.items():
        if any(m in blob for m in markers):
            out.append(concept)
    return out
