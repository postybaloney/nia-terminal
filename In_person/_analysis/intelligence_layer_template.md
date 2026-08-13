# NIA Weekly Brief — Issue Template

*Every number below comes from the pipeline (`digest-signals` scorecard + patent/thesis digests). The LLM narrates; it never invents figures. Editor (Daniel) rewrites voice, cuts, ships.*

---

**Subject line pattern:** `{most notable mover}: {one-phrase why} — plus {n} grants, {n} trials, {n} clearances this week`

## 1 · The Signal (top item, 2 paragraphs)
The single most consequential move of the week, chosen from cross-signal movers — an org active in 2+ categories beats any single filing. State what happened, the dates and dollars exactly, then one paragraph of "why it matters" mechanism (who it pressures, what it unlocks).

## 2 · The Board (scorecard, verbatim from pipeline)
NIH grants: {n} (${total}) · New trials: {n} · 510(k) clearances: {n} · PMA actions: {n} · New patent families: {n}
Then the top 3–5 line items per category exactly as the scorecard prints them (date · org · title · $).

## 3 · Money (grants + funding)
SBIR/STTR awards flagged first (R43/R44/SB1 = commercial intent). Private rounds from editorial monitoring get one line each with source link — private funding isn't in the pipeline yet; mark it [editorial].

## 4 · Regulatory (FDA actions)
Each clearance/approval: device, applicant, product code, pathway, and the one-line strategic read (Luc's lane — what clinics/BD teams do differently because of it).

## 5 · Talent (the moat — nobody else has this)
New PhD theses in scope this week from the thesis pipeline: author, institution, topic, hardware/software flag. Frame: "who just entered the field." Quarterly: aggregate view (which labs are producing, where they land).

## 6 · Watchlist (one line each)
Trials that changed status · orgs that appeared for the first time · anything the verifier flagged as uncertain (never publish unverified claims — cut or caveat).

---

**Production runbook per issue:** `python main.py run-all` → `digest-signals` → paste scorecard into template → LLM narrative pass (strix-verifier check) → Daniel edit → beehiiv schedule Mon 6am ET. Target: ≤90 min human time.
