# NIA — what each command does, and how to update the site

---

## 1 · Does the terminal interfere with parthrudesai.com?

**No.** They are two separate repositories with two separate GitHub Pages deployments, and neither can overwrite the other.

| | repo | serves |
|---|---|---|
| Personal site | `postybaloney/postybaloney.github.io` | `parthrudesai.com/` |
| NIA terminal | `postybaloney/nia-terminal` | `parthrudesai.com/nia-terminal/` |

Your personal-site repo is the **user site** and carries `public/CNAME → parthrudesai.com`. When a user site has a custom domain, GitHub serves every *project* site for that account underneath it at `/<repo-name>/`. That's why `postybaloney.github.io/nia-terminal/` redirects to `parthrudesai.com/nia-terminal/` — normal behaviour, not a misconfiguration.

The NIA repo has **no CNAME**, which is correct.

**The one rule: never add a CNAME to the NIA repo.** Two repositories claiming the same apex domain is the one way to actually break this — GitHub would serve one and fail the other, and which one is not something you get to choose.

Deploys don't collide either. Concurrency groups are per-repository, so a NIA nightly run and a personal-site deploy can happen at the same moment without interacting.

### The thing to decide deliberately

`parthrudesai.com/nia-terminal/` is **public and indexable**. Your `robots.txt` says `Allow: /` with no exclusion for project paths, and it declares a sitemap — so Google will index the terminal under your personal domain. Nothing links to it, but "unlinked" is not "private".

That may be exactly what you want: a live, working intelligence terminal under your own name is good evidence. If it isn't, you have two options:

- **Keep it reachable but out of search** — add to `postybaloney.github.io/public/robots.txt`:
  ```
  User-agent: *
  Disallow: /nia-terminal/
  ```
- **Make it genuinely private** — move NIA off GitHub Pages to Cloudflare Pages with Access in front of it. The workflow already has a commented-out Cloudflare block for this.

Note your `robots.txt` already blocks GPTBot, ClaudeBot, Google-Extended, CCBot, anthropic-ai and PerplexityBot from training. That covers the terminal too.

---

## 2 · What each command does

### Data in

| command | what it does |
|---|---|
| `python main.py run-all` | Ingests everything: patents (EPO), theses (OpenAlex), and signals (NIH, ClinicalTrials, openFDA, newsletters, arXiv/bioRxiv, ATS job boards, SEC EDGAR). Every source isolates its own failures — one dead API can't zero the run. |
| `python main.py doctor` | Probes each source independently and prints a pass/fail table. Run this first when something looks wrong. |
| `python main.py init` | Creates database tables. Idempotent, safe to re-run. |

### Judgement

| command | what it does |
|---|---|
| `python main.py affect --limit 100` | Extracts **entity-level sentiment** — was this good or bad *for the company named in it*. A clearance and a recall are both FDA records; only this tells them apart. Costs LLM calls, so it's capped and resumable, and it skips patents entirely (a patent asserts an invention, it doesn't evaluate one). |
| `python main.py score --stance established` | Ranks by **corroboration** — how many independent evidence layers and source systems attest to something. Returns incumbents. |
| `python main.py score --stance frontier` | Ranks by **structural novelty** — unusual technology pairings, bridging position, early evidence with nothing downstream yet. Returns labs and outliers. |
| `python main.py score --query "speech decoding"` | Same, but re-ranked by graph proximity to your prompt. Something can rank highly because of what it's *connected* to, not just what words it uses. |
| `python main.py score --explain "Medtronic"` | Breaks one entity into every component so you can see *why* it scores what it does. |
| `python main.py audit-gate` | Checks whether any relevance tier has become an "uncertainty sink" — a bucket where undecidable records pile up and inflate your counts. Run after any taxonomy change. |

### Output

| command | what it does |
|---|---|
| `python build_site.py` | Builds all three pages — dashboard, knowledge graph, Intelligence Layer issue — with shared navigation. A generator that fails leaves a placeholder rather than a broken link. |
| `python main.py digest-signals --send` | Generates and emails the current-signal digest. |

### Tests

```powershell
python test_relevance.py        # 10 cases from real records that once fooled the gate
python metrics.py --selftest    # proves the score is not explained by age
python affect.py --selftest     # parser, grounding check, aggregation
python gate_audit.py --demo     # uncertainty-sink check
```

---

## 3 · How to update the website

### Normally: do nothing

The nightly workflow already does the whole thing — ingest → affect → build → deploy. It runs on schedule and publishes to `parthrudesai.com/nia-terminal/`.

### To publish now

**Push, then trigger.** This is the path to prefer, because it runs in the same environment as the nightly job, so if it works here it works tonight.

```powershell
cd E:\Work\NIA
git add -A
git commit -m "Entity affect, scoring, gate audit"
git push
```

Then **Actions → NIA nightly ingest → Run workflow**.

### To rebuild locally first

Order matters. `build_site.py` rebuilds the graph, and the graph reads stored affect to carry sentiment onto its nodes — so affect must run **before** the site build or the graph ships with no sentiment in it.

```powershell
cd E:\Work\NIA
python main.py run-all              # ~10 min
python main.py affect --limit 100   # ~2 min, needs GROQ_API_KEY
python build_site.py                # ~1 min  -> site/
```

Then open `site/graph.html` and check it before pushing. The site only reaches the web via the workflow — `site/` is a build artifact, and pushing it does nothing on its own.

### Prerequisites

- **`DATABASE_URL`** must be set as a GitHub secret and point at Neon, not the dead Railway instance.
- **`GROQ_API_KEY`** must be set, or the affect and digest steps fail. The site still builds without them.
- **Pages source** must be set to "GitHub Actions" in repo Settings → Pages. Without it the build succeeds and the deploy fails.

### When something breaks

```powershell
python main.py doctor          # which source is failing
python main.py audit-gate      # is the corpus being filtered sensibly
python build_site.py --demo    # full site from the built-in corpus, no database
```

That last one always works. The demo path exists so a broken database can never leave you with nothing to show.

---

## 4 · Order of operations, and why

```
run-all            ingest — must be first, everything reads the corpus
    ↓
affect             needs records to exist; writes sentiment back onto them
    ↓
build_site         internally: graph_build → graph_render → dashboard → issue
    ↓                the graph is built FIRST because the dashboard's scored
    ↓                leaderboard and affect card read that SQLite file
deploy             workflow only
```

The three orderings that matter:

1. **run-all before affect** — otherwise there is nothing to score.
2. **affect before build_site** — otherwise the graph ships with no sentiment
   and the dashboard's affect card renders an honest "not extracted yet".
3. **graph before dashboard**, *inside* `build_site.py`. This was wrong until
   2026-08-20: the dashboard was generated first, so it could never see the
   graph and the scored cards had nothing to read. Fixed by reordering.

### `main.py score` needs its own graph

`build_site.py` builds `site/nia_graph.sqlite` and **deletes it** afterwards —
it is a build artifact, not something to serve. So building the site does not
leave a scorable graph behind. For the CLI, build one at the repo root:

```bash
python main.py graph                       # -> nia_graph.sqlite
python main.py score --stance frontier
python main.py score --explain "Synchron"
```

`cmd_score` now refuses to run against a missing path rather than letting
`sqlite3.connect` create an empty database and return a confident empty
ranking.

---

## 5 · LLM backend — local Ollama, hosted in CI

### Why it's split

Ollama is the better choice locally, for a reason bigger than model size: its
native `format` parameter takes a **JSON Schema and enforces it with
grammar-constrained decoding**. The runtime cannot emit tokens that violate the
schema, so malformed extraction output becomes *impossible* rather than merely
unlikely. That removes an entire class of failure.

It is not viable in GitHub Actions. A standard runner is 4 vCPU / 16 GB RAM /
**~14 GB disk**, no GPU. `gpt-oss:20b` is a 14 GB download — it does not fit,
and nothing that does fit (4–8B at roughly 4–8 tokens/sec on 4 cores) is
anywhere near "closer to frontier". Each nightly run would also spend 8–12
minutes just pulling the model.

**Ollama Cloud is not the bridge.** It accepts `format`/`json_schema` and
**silently ignores it** — Ollama's own docs state cloud does not support
structured outputs, and the issue is still open. Using it would discard the one
property that makes Ollama the right local choice.

So: **Ollama locally, Groq in CI.** `LLM_BACKEND` already switches between them,
so this needs no code — only different values in two places.

### Local setup

```powershell
# pick by VRAM:
ollama pull gemma4:12b        #  ~8 GB — good, fast
ollama pull qwen3.6:27b       # ~24 GB — stronger reasoning
ollama pull gemma4:31b        # ~24 GB — best JSON accuracy of the three
ollama pull gpt-oss:120b      # ~65 GB — only with serious VRAM
```

Then in `E:\Work\NIA\.env`:

```
LLM_BACKEND=ollama
LLM_MODEL=gemma4:31b
```

### Rate limiting — why the affect pass used to take 15 minutes

Groq's free tier is **8,000 tokens per minute, per model**, and `max_tokens`
is charged as *requested* tokens whether or not the model uses them — the 429
body literally says `Requested 2342`. Reserving 900 output tokens for a reply
that averages 350 therefore throttled throughput by nearly a third, and the
run spent most of its wall-clock retrying 429s rather than working.

Two knobs now exist:

| Variable | Default | What it does |
|---|---|---|
| `LLM_TOKENS_PER_MINUTE` | `8000` | The budget the affect pass paces itself against. Raise it if you upgrade the Groq tier; it only affects spacing between calls. |
| `AFFECT_MODEL` | *(empty)* | Model override for the affect pass only. **Empty means "same model as everything else"** — the safe default. Set it (e.g. `openai/gpt-oss-20b`) to give affect its own 8k budget, since Groq's limits are per-model. Preflight probes whichever model this resolves to, so a bad value fails on call 1, not call 100. |

Measured effect of the pacing change: 1,860 → 1,361 tokens per call, which is
4.3 → 5.9 calls/minute, or roughly 23 → 17 minutes for 100 records.

Avoid `deepseek-r1` for this: its reasoning traces fight schema constraints.

### CI setup

GitHub → Settings → Secrets → Actions:

```
LLM_BACKEND = groq
LLM_MODEL   =            (leave EMPTY)
```

Empty is deliberate. The backend then picks its own current default
(`openai/gpt-oss-120b`), so the next time a provider retires a model you get
its successor instead of a 404 on every call. If `LLM_MODEL` is still set to
`llama-3.3-70b-versatile` anywhere, clear it — the code remaps known-dead names
and logs a warning, but the clean fix is to stop naming it.

### What broke, and what now prevents it

Groq retired `llama-3.3-70b-versatile` on **2026-08-16**. Every call returned
404 `model_not_found`. Three fixes:

- **Retired-model remap** — known-dead names map to their named successors with
  a warning, so an old `.env` degrades instead of failing.
- **`preflight()`** — one cheap call proves the backend works before a batch
  starts. The incident consumed 100 calls to learn what the first one knew.
- **Fatal vs transient** — 404/401/403 abort immediately; 429/500/timeout still
  retry. Plus a circuit breaker at 5 consecutive failures for anything that
  degrades mid-run.

`python main.py affect` now reports `ABORTED` with the reason and writes
nothing, rather than reporting "100 attempted, 0 succeeded".
