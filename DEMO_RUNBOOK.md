# NIA — Demo Runbook
### Ali Algain · Entrepreneurs First · Wed 19 Aug 2026, 12:00 PM PDT

---

## 0 · The one thing to hold onto

**He is a screener, not a check-writer.** EF's Investment Committee decides months later, at the end of a 12-week FORM programme. So the goal tomorrow is not to be funded — it's for Ali to leave thinking *"that person is an outlier, I want them in the pipeline."*

EF scores four things and says outright that it does **not** assess your background, industry, or business plan:

| | |
|---|---|
| **Intelligence** | shows up in how you reason about the problem, unprompted |
| **Skill** | the artifact does this for you — you built a working system alone |
| **Edge** | your unfair advantage. **You have two at once.** See §4. |
| **Commitment** | would you actually drop everything? Have an answer ready. |

Your instinct — *"I love this field so I built this, look how cool it is"* — is the correct play, and not just tonally. It routes around all three traps NIA otherwise walks into: the pre-formed team, the bootstrapped model, and pitching the wrong person.

**The one unforgivable move is concealment.** Understate NIA if you like. Never hide it. A hidden cofounder is reportedly an instant no.

---

## 1 · Tonight — 25 minutes of actual work

Run these in PowerShell from `E:\Work\NIA` with your venv active.

```powershell
# 1. Confirm the precision fix holds (10 seconds, no network)
python test_relevance.py
#    -> expect "10/10 passed"

# 2. Probe every source independently (~2 min)
python main.py doctor
#    -> a pass/fail table. WARN on a feed is fine. Paste the table to me if anything FAILs.

# 3. Refresh the corpus with the fixed queries (~10 min)
python main.py run-all

# 4. Build the graph + the deliverable + the dashboard
python main.py graph
python build_issue.py --days 30
python build_snapshot.py --out site/index.html
```

**Then do the Roborock test — this is the gate that matters:**

```powershell
python graph_build.py --explain "Medtronic"
```

Open `nia_graph.html` and switch the filter to **organisations**. Read the top names out loud. If you see a robot-vacuum company, a lawnmower company, or a surgical-stapler maker, **stop and tell me** — something regressed. If the names are Medtronic, Boston Scientific, Abbott, Neuralink, Synchron, LivaNova, Ceribell and universities, you're clean.

**If step 3 fails or returns very little,** you still have a full demo:

```powershell
python main.py graph --demo
python build_issue.py --demo
```

Everything renders from a built-in corpus. The demo cannot be killed by a dead API.

---

## 2 · The demo — five minutes, then stop talking

Have three tabs open before the call: `nia_graph.html`, `intelligence_layer_issue.html`, `site/index.html`.

**Open with a question, not a pitch:**

> "Do you look at neurotech at all? I've been obsessed with it for a while and I built something I'd love to show you — mostly because I think it's cool, not because I'm pitching you."

That line does real work. It sets the frame, it's true, and it gives him permission to be curious instead of evaluative.

### Beat 1 — the graph (90 seconds)

Open `nia_graph.html`. Don't explain the architecture. Click **Boston Scientific**.

> "Every neurotech patent, paper, thesis, grant, trial, FDA clearance and job posting, in one graph. The hard part isn't pulling the data — it's that this company appears in the sources under three different spellings."

Point at **MERGED FROM 3 SPELLINGS** in the panel.

> "Until you fix that, none of the connections exist."

Point at the provenance lines under each connection.

> "And every edge says where it came from. If you don't believe a claim, you can click through to the filing."

### Beat 2 — the insight nobody else has (90 seconds)

Filter to **organisations**, click a company with a `hiring_for` edge.

> "This is the part I like most. A company's first regulatory-affairs hire means an FDA submission is coming. Their first reimbursement hire means they're starting a CMS push. That's public, it's free, and it shows up months before any announcement. Patents lag reality by about eighteen months. Job postings lead it."

**This is your Market Edge in one sentence.** If he only remembers one thing, make it this.

### Beat 3 — the deliverable (60 seconds)

Open `intelligence_layer_issue.html`.

> "That all compiles into this every week. Generated, not written — if the pipeline didn't see it, it isn't in here."

### Then stop. Ask him something.

> "You've seen CoMind go through EF — how do you think about deeptech where the science is real but the commercial timeline is long?"

Asking a good question beats any pitch. It shows you did homework and it makes the conversation two-way.

---

## 3 · Two tracks — pick by reading the room

Both share the opening above. They diverge only when he asks **"so what is this — a company?"**

### Track A · The artifact leads *(default)*

Use when he's curious, technical, asking how it works.

> "There's a small LLC — me plus two others. Daniel's at Somnee and writes a neurotech newsletter, Luc's a clinical engineer at Ampa. Right now it's honestly three people who like the problem. I'm not raising anything; I built the pipeline because I wanted it to exist."

Then hand the conversation back: what does he think is missing, who should you talk to.

**Why this works:** it's completely transparent, it doesn't ask for anything, and it leaves you looking like someone who builds things — which is precisely what EF buys.

### Track B · Venture-scale *(only if he leans in)*

Use if he asks about market size, defensibility, or the business model. He's testing whether you think at fund-returner scale.

> "The newsletter is a wedge, not the business. What accumulates is a resolved, cross-layer graph of the entire field — the join between who trained where, who filed what, who's funding it, and who's hiring for it. PitchBook has companies. Google Patents has patents. Nobody has the edges between them for this vertical.
>
> And it compounds in a way a new entrant can't copy. Job postings vanish when they're filled. The historical graph can only be built by having run the pipeline for years. Every week widens that gap.
>
> The wedge is subscriptions. The platform is the graph — licensing, diligence for investors, competitive intelligence for the neuromod companies themselves, and eventually the same structure for any regulated deeptech vertical where the public record is fragmented."

**Words to never say:** *bootstrapped · profitable · lifestyle business · capital efficient · we don't need much money.* Every one is an explicit EF anti-pattern. They passed on a company with real traction for being "a $200m company" rather than a "$10bn fund returner."

---

## 4 · Your Edge, in one sentence

Have this ready — it is the single most likely question.

> "I'm an applied math and data science grad from Berkeley, and I've spent enough time in neurotech to know which signals actually matter. Most people who can build the data infrastructure don't know that a first reimbursement hire predicts a CMS push — and most people who know that can't build the infrastructure."

That's **Technical Edge and Market Edge simultaneously.** EF's own framework treats holding both as rare.

---

## 5 · The eight questions to have answers for

**1. "Why you?"** → §4, verbatim.

**2. "Isn't this a newsletter? Naveen Rao already does Neurotech Notables."**
> "Notables is good — I ingest it as a source. But it's human-curated commentary. This is structured: every entity resolved, every claim traceable, and queryable. Different products. He tells you what happened; I can tell you who's connected to whom and what that implies."

**3. "What's defensible? Anyone can call these APIs."**
> "The APIs are commodity — I'd say that myself. Two things aren't. Entity resolution: getting six spellings of one company down to one node is where the actual work is, and it's what makes any cross-source question answerable. And time: the historical graph can't be backfilled, because postings disappear and full-text search windows are limited. Someone starting today starts at zero history."

**4. "Who pays for this?"**
> "The honest answer is I have a waitlist and hypotheses, not signed contracts. Best guesses are BD teams at neuromod companies, medtech investors doing diligence, and IP counsel. That's the thing I most need to go find out."
*Say it this plainly. EF interviewers reward calibration and punish bluffing.*

**5. "Would you go full-time? Would you relocate?"**
> **Decide your real answer before the call.** EF is full-time, in-person, London/Bangalore then San Francisco. Hedging here reads as low Commitment — the attribute their own data says correlates most with outcomes.

**6. "Tell me about your cofounders."** → Track A language. Straight, brief, no defensiveness. If he suggests you'd be assessed individually or might re-form, don't argue — say you understand that's how EF works and you're interested in the process.

**7. "How is this different from CB Insights or PitchBook?"**
> "Vertical depth. Neurotech is too small for them to build a specific ontology for, and a generic one doesn't work — you can't tell a neurostimulation patent from a cardiac pacemaker patent without knowing that A61N1/362 is the heart subgroup. That distinction is the whole product."

**8. "What went wrong / what have you learned?"**
> "The first version was broken in a way I didn't catch for weeks. My patent queries were matching any document containing the word 'neural' — so a robot-vacuum company came out as a top neurotech patent filer. I only found it because the weekly digest read strangely. Now the queries are phrase-plus-classification, there's a scored relevance gate that logs why every record was kept or dropped, and there's a test suite built from the exact records that fooled it."

**Question 8 is your best answer in the whole list.** It's specific, it's honest, it shows you audit your own work, and it demonstrates engineering judgment better than any success story. Volunteer it if he doesn't ask.

---

## 6 · Honest read on fit

Worth going in clear-eyed: **NIA as it stands is not an EF-shaped company.** A three-person team with an existing entity and a subscription model is close to the profile EF explicitly says it doesn't want, and they prefer applicants *without* an existing startup.

That does not make the meeting a waste. Two outcomes are genuinely valuable:

1. **Ali flags you as an individual worth tracking.** That's what a Talent Investor's job is, and you fit the individual profile far better than NIA fits the company profile.
2. **You get a serious operator's read on the product**, plus intros. EF has CoMind in neurotech; that network is worth more to you right now than a term sheet.

The failure mode to avoid is pitching NIA as an investable company and getting a polite no on the business — which then closes the door on you as a person. Lead with the artifact and yourself. Let NIA be evidence, not the ask.

---

## 7 · Ten minutes before the call

- [ ] Three tabs open and already loaded: graph, issue, dashboard
- [ ] Click Boston Scientific once so you know where it is on the canvas
- [ ] Notifications off, phone off
- [ ] Water. You'll talk more than you expect.
- [ ] Say your Edge sentence out loud once
- [ ] Decide your relocation / full-time answer **before** he asks

---

## 8 · After the meeting (not before — these are deploy risks)

1. **GitHub Actions has no email secrets.** Your `.env` has SMTP credentials but Actions doesn't, so every nightly digest dispatches into the void. Add `SMTP_USER` / `SMTP_PASSWORD` / `DIGEST_EMAIL_TO` (or `RESEND_API_KEY`) as repo secrets. Two minutes.
2. **`DATABASE_URL` still points at Railway**, not the Neon instance the hosting pivot called for. It works — don't migrate near a deadline.
3. **Verify the ATS slugs.** `python main.py doctor --only jobs` prints which company job boards actually resolve; fix the wrong ones in `ingestors/signals/jobs.py`. Every unresolved slug is a company you have no hiring signal for.
4. **Add the graph + issue to the nightly workflow** so both regenerate automatically.
5. **Resolve LLC-vs-C-corp** before Stripe. Still open from the partnership review.
