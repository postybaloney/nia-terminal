# NIA Hosting — pivot off Railway to ~$0

**Bottom line:** don't restart Railway. NIA's core is a *nightly batch job* + a Postgres DB + a mostly-static front end. Railway charges for always-on uptime you don't use. Move to: **Neon (DB) + GitHub Actions (nightly ingest) + Cloudflare/GitHub Pages (landing).** Fixed cost ≈ $0, and it fits "grants & sponsorships only."

## Why this is better, not just cheaper
- Your `scheduler.py` runs a blocking always-on process just to fire a daily cron. A GitHub Actions schedule does the same thing with free minutes, real logs, retries, and no crash-loop risk.
- A daily ingest touching a scale-to-zero Postgres uses almost nothing — Neon's free tier covers it with headroom.
- The landing page is static HTML; static hosts are free and faster than a server.

## The stack

| Piece | Was (Railway) | Now (free) |
|---|---|---|
| Postgres | Railway PG add-on | **Neon** free tier — 0.5 GB/project, scale-to-zero, no card needed to start |
| Nightly ingest | `python main.py scheduler` (always-on) | **GitHub Actions** cron (`.github/workflows/nia-ingest.yml`, included) |
| Landing page | Railway web service | **Cloudflare Pages** or **GitHub Pages** (static, free) |
| Terminal dashboard | Railway web service | see "Dashboard" below |

## Migration steps (about 30–40 min)
1. **Neon:** create a free project at neon.tech → copy the connection string (looks like `postgresql://user:pass@ep-xxx.neon.tech/neondb?sslmode=require`). Your `config.py` already normalizes `postgres://`→`postgresql://` and requires SSL-friendly URLs, so it drops in.
2. **GitHub Actions:** commit the included `.github/workflows/nia-ingest.yml`. In the repo → Settings → Secrets and variables → Actions, add `DATABASE_URL` (the Neon string), `LLM_BACKEND`, `GROQ_API_KEY`, and any optional keys. Run it once from the Actions tab (`workflow_dispatch`) to verify, then it runs nightly at 00:00 PT.
3. **Retire the Railway scheduler:** you no longer run `main.py scheduler`. Keep `scheduler.py` in the repo (harmless), just don't deploy it. Delete `railway.toml` only if you want a clean break — or leave it for the fallback below.
4. **Landing page:** push `website/index.html` to a repo and connect Cloudflare Pages (or enable GitHub Pages on the folder). Point epsilonsolutionsllc.com / a `nia.` subdomain at it later via GoDaddy DNS.

## Dashboard — the one real decision
The Dash apps (`dashboard.py`, `thesis_dashboard.py`) need a running server; static hosts can't run them. Three options, cheapest first:
1. **Static snapshot (recommended for the MVP):** have the nightly Action also export the key board views to static HTML/JSON and publish them to Pages next to the landing. The waitlist lands on a page that refreshes daily — enough for launch, still $0. (I can write the exporter next.)
2. **Free always-on-ish host:** deploy the Dash app to **Render** free tier or **Fly.io** — works, but free web services sleep on idle and cold-start slowly.
3. **Pay only when it pays:** once the $50 Terminal has paying subscribers, put the live interactive dashboard on a $5–7/mo host (Render Starter, Fly, or yes, Railway Hobby). Let revenue, not launch, justify the server.

## When restarting Railway *is* the right call
If you'd rather not touch the architecture this week, Railway Hobby (~$5/mo, everything already configured via `railway.toml`) is a legitimate "just make it run" choice. It's only $5. But for a pre-revenue grants-only venture, the free stack above is the better long-term fit and isn't much more setup — and it removes a recurring bill from your budget doc.

## My recommendation
Do the **Neon + GitHub Actions** move now (it's the ingest, the important part, and it's ~30 min). Host the landing static. Start the Terminal as a **daily static snapshot**; upgrade to a live server only when subscribers pay for it. Net: $0/mo until revenue, and the pipeline runs more reliably than it did on Railway.
