# patent_intel

Production medtech/neurotech intelligence pipeline: patents + PhD theses +
current signals (NIH grants, clinical trials, FDA device actions).

Patents: EPO OPS + Google BigQuery, deduplicated by patent family.
Signals: NIH RePORTER, ClinicalTrials.gov v2, openFDA 510(k)/PMA.
Everything lands in PostgreSQL; an LLM layer writes landscape analysis
and digests on each batch.

> **Licensing note (2026-08):** Lens.org was removed from the active
> pipeline — its free API tier is licensed for noncommercial use only.
> `ingestors/lens.py` and the Lens-backed `ingestors/patentsview.py`
> are retained but unregistered; re-add them in `pipeline.py` only under
> a signed [Lens Commercial Use Agreement](https://about.lens.org/individual-commercial-use/).
> All active sources (EPO OPS, BigQuery public data, NIH, ClinicalTrials.gov,
> openFDA, OpenAlex) are free for commercial use.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Scheduler (cron)                    │
│                     scheduler.py / main.py               │
└───────────────────────────┬─────────────────────────────┘
                            │ asyncio.gather()
          ┌─────────────────┼──────────────────────┐
          ▼                 ▼          ▼            ▼
   PatentsView           EPO OPS    Lens.org    BigQuery
   (USPTO grants)    (EU + PCT)  (multi-src)  (bulk SQL)
          │                 │          │            │
          └─────────────────┴──────────┴────────────┘
                            │ NormalizedPatent[]
                            ▼
                    pipeline.py
                    ├── deduplicate (source, source_id)
                    ├── resolve PatentFamily (DOCDB family_id)
                    └── upsert RawPatent + PatentFamily
                            │
                            ▼
                    PostgreSQL (patent_intel)
                    ├── patent_families   ← canonical deduplicated units
                    ├── raw_patents       ← one row per source record
                    ├── ingest_runs       ← audit log
                    └── analysis_results  ← Claude summaries
                            │
                            ▼
                    analysis.py (Claude claude-sonnet-4-20250514)
                    ├── landscape analysis (themes, assignees, white-space)
                    └── weekly digest (plain text for email/Slack)
```

---

## Setup

### 1. Prerequisites

- Python 3.11+
- PostgreSQL 15+
- API credentials (see below)

### 2. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env with your credentials
```

Required:
- `DATABASE_URL` — PostgreSQL connection string
- `ANTHROPIC_API_KEY` — from console.anthropic.com

Optional (enables additional sources):
- `EPO_CLIENT_ID` + `EPO_CLIENT_SECRET` — from developers.epo.org (free)
- `LENS_API_KEY` — from lens.org/lens/user/subscriptions (free tier)
- `BIGQUERY_PROJECT_ID` + `GOOGLE_APPLICATION_CREDENTIALS` — GCP service account

### 4. Initialize database

```bash
python main.py init
```

### 5. Run first ingestion

```bash
python main.py run
```

### 6. Start scheduler

```bash
python main.py scheduler
```

---

## API Credentials

| Source      | Auth           | URL                                      | Cost       |
|-------------|----------------|------------------------------------------|------------|
| PatentsView | None required  | search.patentsview.org                   | Free       |
| EPO OPS     | OAuth2 key     | developers.epo.org                       | Free       |
| Lens.org    | Bearer token   | lens.org/lens/user/subscriptions         | Free tier  |
| BigQuery    | Service account| console.cloud.google.com                 | 1TB/mo free|

---

## CLI Reference

```bash
python main.py init                        # create DB tables (patents + theses + signals)
python main.py run                         # patent pipeline run
python main.py backfill --from 2022-01-01  # historical backfill
python main.py run-theses                  # thesis pipeline run
python main.py run-signals                 # current-signal run (NIH / trials / FDA)
python main.py run-all                     # everything
python main.py digest                      # weekly patent digest
python main.py digest-signals --send       # current-signal digest → email/Slack
python main.py scheduler                   # start cron (blocking)
```

## Current signals (v2 addition)

The `signals` table holds the fast-moving layer patents can't see:

| Source | Type | API | Auth |
|---|---|---|---|
| NIH RePORTER | grant | api.reporter.nih.gov/v2 | none |
| ClinicalTrials.gov | trial | clinicaltrials.gov/api/v2 | none |
| openFDA 510(k) | clearance | api.fda.gov/device/510k | optional key |
| openFDA PMA | approval | api.fda.gov/device/pma | optional key |

Configure via `SIGNAL_QUERIES` (short phrases), `SIGNAL_SINCE`, and
`SIGNAL_SCHEDULE_CRON`. The weekly digest (`digest-signals`) is a
deterministic scorecard — counts, dollars, cross-signal movers — with an
optional LLM narrative on top; numbers never come from the LLM.

Smoke test after setup:

```bash
python main.py run-signals   # should ingest without any API keys configured
python main.py digest-signals
```

---

## Database Schema

### `patent_families`
Canonical deduplicated patent families. One row per invention, regardless
of how many jurisdictions it was filed in.

| Column                | Type        | Notes                                  |
|-----------------------|-------------|----------------------------------------|
| family_id             | varchar(64) | DOCDB family ID, or surrogate          |
| title                 | text        | Best available (prefers English)       |
| abstract              | text        |                                        |
| earliest_filing_date  | timestamptz |                                        |
| assignees             | jsonb       | [{name, country}]                      |
| cpc_codes             | jsonb       | ["A61N1/36", ...]                      |
| sources               | jsonb       | ["patentsview", "epo"]                 |

### `raw_patents`
One row per source record. Multiple rows share a `family_id`.

### `ingest_runs`
Audit log of every scheduler execution.

### `analysis_results`
Claude landscape analysis — stored as JSON + extracted theme/assignee lists.

---

## Useful Queries

```sql
-- Top assignees across all time
SELECT a->>'name' AS assignee, COUNT(*) AS families
FROM patent_families, jsonb_array_elements(assignees) AS a
GROUP BY 1 ORDER BY 2 DESC LIMIT 20;

-- New filings per week
SELECT date_trunc('week', earliest_filing_date) AS week, COUNT(*)
FROM patent_families
GROUP BY 1 ORDER BY 1 DESC;

-- Families appearing in multiple sources (high-confidence records)
SELECT family_id, title, jsonb_array_length(sources) AS source_count
FROM patent_families
WHERE jsonb_array_length(sources) > 1
ORDER BY source_count DESC;

-- CPC code frequency
SELECT c AS cpc_code, COUNT(*) AS count
FROM patent_families, jsonb_array_elements_text(cpc_codes) AS c
GROUP BY 1 ORDER BY 2 DESC LIMIT 30;

-- Patents with academic citations (via Lens NPL data)
SELECT r.title, r.raw_payload->>'npl_citation_count' AS citations
FROM raw_patents r
WHERE r.source = 'lens'
  AND (r.raw_payload->>'npl_citation_count')::int > 0
ORDER BY citations DESC;
```

---

## Production Deployment

### Docker

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "main.py", "scheduler"]
```

### systemd service

```ini
[Unit]
Description=Patent Intelligence Pipeline
After=network.target postgresql.service

[Service]
WorkingDirectory=/opt/patent_intel
EnvironmentFile=/opt/patent_intel/.env
ExecStart=/opt/patent_intel/.venv/bin/python main.py scheduler
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

### Recommended schedule

- Nightly at 2 AM UTC: `SCHEDULE_CRON=0 2 * * *`
- PatentsView lag: ~2-4 weeks behind grant date
- EPO OPS: near real-time (days after publication)
- Lens.org: 1-2 weeks lag
- BigQuery: weekly dataset refresh

---

## Extending

**Add a new source**: extend `BaseIngestor`, implement `fetch()` returning
`list[NormalizedPatent]`, register in `pipeline.py`.

**Add Slack notifications**: call a webhook in `scheduler.py` after
`generate_weekly_digest()`.

**Add email**: use `smtplib` or `sendgrid` to ship the digest string.

**Add deduplication by title similarity**: use `pg_trgm` (already enabled)
to fuzzy-match titles as a fallback when family IDs are unavailable.
