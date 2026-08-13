# NSF SBIR Phase I — Project Pitch (Draft)
## Neurotech Intelligence Agency (NIA) — the NIA Terminal

*Working draft — to be sharpened, fact-checked, and trimmed before submission through NSF's Project Pitch portal (seedfund.nsf.gov). The pitch window is open (opened ~June 2, 2026); NSF typically responds to a pitch in 1–2 months, which would position NIA for the Nov 4, 2026 full-proposal deadline. Everything below the divider ("Internal working notes") is for the team only and is **not** submitted to NSF. Verify every date, amount, and eligibility fact at the source before relying on it — grant terms change.*

---

**Snapshot**

| | |
|---|---|
| Program | NSF SBIR Phase I (America's Seed Fund) |
| Award ceiling | Up to **$305,000**, non-dilutive (no equity taken) |
| First step | Required ~3-page **Project Pitch** → if invited, full proposal |
| Topic fit | Artificial Intelligence / Advanced Analytics / scalable software ("scalable analytics" is an explicitly funded area — confirm the exact current topic code) |
| Eligibility (high level) | US for-profit small business, ≥51% US-individual-owned, no VC/PE majority, <500 employees — **NIA qualifies once the entity is formed** |
| Two review criteria | **Intellectual Merit** and **Broader Impacts** — both must be answered explicitly |

> **Framing discipline (read this first):** SBIR funds *research and development on a technical innovation with real technical risk* — not a newsletter, not a data-aggregation product, not "we built a Terminal." Every section below is deliberately built around the one genuinely hard, unsolved technical problem inside NIA: **automated entity resolution and signal fusion across five heterogeneous, messy innovation datasets, plus a validated early-signal predictive layer.** If, in honest internal review, that problem turns out to be solvable with off-the-shelf tools, NIA should *not* force this pitch (see the eligibility & framing-risk note below).

---

## 1 · The Technology Innovation

The world's primary signals of where a deep-technology field is heading are scattered across five public but structurally incompatible datasets: **patents** (EPO OPS, Google Patents/BigQuery), **research grants** (NIH RePORTER), **clinical trials** (ClinicalTrials.gov), **regulatory device actions** (FDA 510(k)/PMA), and **doctoral output** (PhD theses via OpenAlex). Each records the *same underlying entities* — the same companies, labs, universities, and inventors — but under different names, schemas, identifiers, and text genres, with **no shared join key** between them. A patent assignee ("Paradromics, Inc."), an NIH grantee institution, a trial sponsor, an FDA applicant, and a dissertation's affiliation may all denote one organization and never once match on a string.

The innovation NIA proposes is a system that **resolves these five sources into a single, unified entity graph and fuses their signals to detect commercialization activity earlier than a human analyst can.** NIA has built an initial pipeline (Python/PostgreSQL) that ingests all five sources, deduplicates patents by **DOCDB family**, performs first-pass organization/person resolution across sources, and surfaces "cross-signal movers" — entities that are simultaneously active in grants, trials, and clearances. That pipeline is ingesting and has passed integration testing. **What does not yet exist — and is the actual R&D — is (a) entity resolution accurate enough across all five genres to trust automatically, and (b) a predictive layer that moves from detecting present-day co-occurrence to validly forecasting emerging technology clusters and commercialization events before they are obvious.**

**Why it is technically new and risky.** Record linkage and entity resolution are established fields, but they are typically solved *within* one well-structured domain. The unsolved problem here is cross-*genre* resolution and fusion where the text ranges from terse legal patent claims to grant abstracts to regulatory summaries to dissertation prose, where organizational identity fractures across parents, subsidiaries, spin-outs, and acquisitions, and where there is **no labeled ground truth** for what a "correct" cross-source match even is. Existing patent- and market-intelligence tools (Clarivate/Derwent, PatSnap, Lens.org, Dimensions, CB Insights) largely operate on one or two of these sources; none unify research funding → academic output → IP → clinical validation → regulatory clearance into one resolved graph with a *validated* early-signal predictor. The regulatory and thesis/talent layers are especially underserved. The intellectual merit is a defensible advance in cross-genre entity resolution and signal fusion under weak supervision.

## 2 · The Technical Objectives and Challenges

Phase I is a feasibility study organized around three research questions, each with a pre-registered measurement plan and a go/no-go threshold. (*Thresholds below are proposed targets to be calibrated during Phase I planning, not results already achieved.*)

**Objective 1 — Cross-source entity resolution, quantified.** *Can organizations and people be resolved across all five sources at commercially useful precision with no shared key?* The R&D task is to build a resolution model (blocking/indexing for scale, learned similarity over names + context features, and parent/subsidiary/spin-out hierarchy handling) and to **construct the gold-standard evaluation set that does not currently exist.** Challenge: ground-truth creation under ambiguity; controlling the O(n²) comparison space; distinguishing genuinely distinct entities with near-identical names. *Proposed go/no-go: measured precision/recall on a hand-labeled neurotech gold set above a threshold to be set at kickoff.*

**Objective 2 — Cross-genre invention/technology linking.** *Can a claimed invention be linked to its funding, clinical, and academic antecedents across text genres that share almost no vocabulary?* The R&D task is domain-adapted semantic linking (embedding + retrieval) between legal claims, grant aims, trial descriptions, FDA summaries, and thesis abstracts, with an accuracy measurement against curated true links. Challenge: the genre gap; polysemy of technical terms across communities; avoiding spurious links that a downstream predictor would amplify.

**Objective 3 — A validated early-signal predictor.** *Can a fused-signal model flag emerging technology clusters and commercialization events earlier than a human-analyst baseline?* The R&D task is to define "emergence" operationally, then **backtest** on historical data with strict lookahead controls, measuring lead-time against a human/heuristic baseline. Challenge: no clean labels for "the future"; survivorship and selection bias; and **error propagation** — quantifying how entity-resolution errors from Objectives 1–2 degrade predictive reliability, which is itself an open question.

**Cross-cutting challenge — uncertainty budgeting.** Because each stage feeds the next, Phase I must produce not just point accuracies but an end-to-end error model: how much upstream resolution error the predictive layer can tolerate before its signal is no better than chance. This uncertainty analysis is the technical heart of the feasibility question.

## 3 · The Market Opportunity

The buyers are the people who must make expensive bets on a fast-moving field before the evidence is public: **corporate strategy and business-development teams, investors (venture, corporate development, PE), technology-transfer offices, market-research firms, and technical/clinical recruiters.** Today they either pay for single-source tools (patents *or* deals *or* trials) and stitch the picture together by hand, or they miss the cross-source signal entirely.

Neurotech is the beachhead, and the demand signal is concrete: NIA's own tracking of public reporting shows on the order of **~$200M in disclosed private neurotech rounds in a single recent quarter** (excluding several nine-figure mega-rounds), alongside multiple FDA clearances and marquee clinical milestones in the same window. A field moving that fast, with capital, regulation, IP, and talent all in motion at once, is precisely the environment where a cross-signal early-warning tool is worth paying for. NIA's planned revenue is aligned with a bootstrapped model: **subscription access to the Terminal, one-off landscape/positioning reports, and sponsorships** — none of which require venture capital.

Crucially, **the method is domain-general.** Cross-source entity resolution and early-signal fusion apply to any regulated deep-tech vertical (medtech, biotech, semiconductors, energy, defense-adjacent hardware) where the same five signal types exist. Neurotech proves the method; adjacent verticals are the scale-up. (*Formal TAM/SAM sizing and pricing validation is itself a Phase I commercialization task — to be completed with customer discovery, not asserted here.*)

## 4 · The Company and Team

**Neurotech Intelligence Agency (NIA)** is a bootstrapped, independent venture (legal entity currently **being formalized** — a prerequisite for SBIR eligibility and registration; see checklist). Three founders each own a layer end to end:

- **Parth Desai — Data & Engineering (proposed Principal Investigator).** Builds the ingestion pipeline and the NIA Terminal on top of it. Background in **applied mathematics and data science (UC Berkeley)** — the entity-resolution, machine-learning, and evaluation-methodology work at the center of this proposal is his domain.
- **Daniel Kim — Editorial & Domain.** Neurotech domain expertise and the editorial layer that turns resolved signals into intelligence; industry network for customer discovery.
- **Luc LaMontagne — Clinical & Regulatory.** Clinical and regulatory-affairs analysis (the FDA 510(k)/PMA and trials layers) and industry go-to-market relationships.

The team pairs the technical depth to do the research (Parth) with the domain judgment needed to build ground truth and validate signals (Daniel, Luc) — an advantage for a problem where evaluation labels must be created by people who understand the field. **Gaps to close honestly:** the entity is not yet formed; PI employment terms must be set to meet NSF's requirement that the PI is primarily employed (>50%) by the company at award; and the team may add or contract specialized ML/record-linkage expertise for Phase I.

## 5 · Why NSF (vs. Other Funding)

NIA is explicitly **funded by grants and sponsorships, not venture capital** — a deliberate, founder-owned, independent model. That choice makes NSF SBIR uniquely well-fit and most alternatives ill-fit:

- **Non-dilutive and equity-free.** SBIR funds the R&D without taking ownership, preserving the independence that is core to NIA's identity and credibility as a neutral intelligence source.
- **NSF's structure rewards exactly this model.** NSF SBIR is designed for VC-independent small businesses (it restricts majority VC/PE ownership) — the same constraint NIA already lives by.
- **It funds the part nothing else will.** Sponsorship and report revenue can fund the *media* product; they cannot fund a multi-month research program on cross-genre entity resolution and predictive validation. Journalism/media grants fund content, not data-science R&D. Venture capital would fund the Terminal but demand dilution and a growth trajectory NIA has chosen against. **NSF SBIR is the only aligned source for the Terminal's core research engine.**
- **Broader Impacts (NSF criterion).** If it works, the system **democratizes access to innovation intelligence** — letting universities, small companies, public-interest actors, and independent analysts see the full neurotech landscape without an enterprise Clarivate/PatSnap seat. NIA proposes to contribute an **open benchmark/evaluation set** for cross-source innovation-signal entity resolution (a public good the field currently lacks), a transparent **talent layer** surfacing where expertise is moving, and methods generalizable to public-interest technology monitoring. It also supports an independent, non-VC deep-tech information venture led by early-career founders.

---
---

# Internal working notes — NOT part of the submitted pitch

## ⚠️ Eligibility & framing risk (candid)

**The core risk is framing, not eligibility.** Per the funding research, NIA is "two things wearing one coat": a media product (the Napkin + Intelligence Layer) and a data/software product (the Terminal). **SBIR funds R&D on a technical innovation — not content, not media, not straightforward data aggregation or integration.** Reviewers will actively probe whether this is *research* (novel, uncertain, publishable-grade methodology) or *engineering* (competent integration of known tools). The pitch above deliberately foregrounds the defensible technical unknowns — cross-genre entity resolution with no ground truth, and a backtested early-signal predictor with an end-to-end uncertainty budget — because those are what carry real technical risk.

**The honest test before submitting:** In internal review, do Objectives 1–3 feel genuinely uncertain — could a competent team plausibly *fail* to hit the thresholds using existing methods? If yes, the pitch is real R&D and worth submitting. If the answer is "we could basically build this with standard record-linkage libraries and some glue code," then the technical unknown is too thin, and **NIA should not contort a media/data product into an SBIR pitch** (per funding_targets.md: *"If you can't write the pitch around a genuine technical unknown, skip SBIR; don't contort a media product into it."*). In that case, redirect to journalism/media grants (Lenfest, GFMD) via a fiscal sponsor, and to sponsorship revenue.

**Secondary honesty checks:**
- No metrics, accuracy figures, user/revenue numbers, or prior results are claimed above — none exist yet and none must be invented. Keep it that way through every revision.
- The predictive layer is described as *proposed Phase I research*, not a shipped feature. Do not let it drift into present tense.
- The ~$200M/quarter figure is from NIA's tracking of public reporting; label it as such and re-verify before use.
- Market-size (TAM) is intentionally not asserted; framed as a Phase I customer-discovery task. Don't add a fabricated number to make it look stronger.

## NSF review-criteria mapping (for the full proposal)
- **Intellectual Merit** lives in Sections 1–2: the advance in cross-genre entity resolution and signal fusion under weak supervision, and the uncertainty-budgeting methodology.
- **Broader Impacts** live in Section 5: democratized innovation intelligence, an open benchmark dataset, talent-layer transparency, generalizable public-interest monitoring, and support for an independent early-career team.

## Format discipline before submission
- NSF Project Pitch is ~3 pages total; each of the five prompts has its own field with an approximate **500-word** limit. Trim each section to fit its field — several sections above run long and must be cut.
- Confirm the exact current Project Pitch prompts and word limits on the NSF portal (they are revised periodically) and match section headings/order to the live form.
- Plain language over jargon where possible; lead each section with the answer.

## ✅ Pre-submission checklist — what NIA must confirm/complete

**A. Entity & eligibility**
- [ ] **Form the for-profit legal entity** (LLC or C-corp) — currently "being formalized." Nothing below can happen until the entity legally exists. This is the critical-path item.
- [ ] Obtain an **EIN** from the IRS; open a business bank account.
- [ ] Confirm SBA eligibility: **≥51% owned by US citizens/permanent-resident individuals**, **no VC/PE/hedge-fund majority**, **<500 employees**, US-based, US-performed work.
- [ ] Confirm **PI eligibility**: Principal Investigator (Parth) **primarily employed (>50%) by the company** at time of award and during the project. (No PhD required for NSF SBIR.)

**B. Federal registrations (start early — these have multi-week lead times)**
- [ ] **SAM.gov** registration → obtain the **UEI (Unique Entity Identifier)**. (DUNS is deprecated; UEI replaces it. Entity validation can take weeks — begin immediately after entity formation.)
- [ ] **SBA Company Registry** at SBIR.gov → obtain the **SBC Control ID** (required for all SBIR submissions).

**C. NSF-specific**
- [ ] Create the **NSF Project Pitch** account at **seedfund.nsf.gov** and submit the pitch there first (invitation required before a full proposal).
- [ ] Register the organization and PI in **Research.gov** (where the invited full proposal is submitted); set up AOR/SPO and PI roles and NSF IDs.
- [ ] Confirm the innovation maps to a **current NSF SBIR topic/subtopic** (AI / advanced analytics / software) and note the topic code.
- [ ] Verify the **current cycle dates**: Project Pitch window (open ~June 2, 2026), NSF response window (1–2 months), and the target **full-proposal deadline (Nov 4, 2026)** — confirm all at NSF before planning around them.

**D. Technical-readiness (the go/no-go before spending effort)**
- [ ] Internal review: do Objectives 1–3 contain genuine, potentially-failing technical risk (not just engineering)? If no → do not submit; pursue media grants + sponsorship instead.
- [ ] Draft the Phase I gold-standard/evaluation plan and set concrete go/no-go thresholds (Objectives 1–3).
- [ ] Confirm data-source terms of use permit the intended research use (EPO OPS, Google Patents/BigQuery, NIH RePORTER, ClinicalTrials.gov, FDA, OpenAlex).
