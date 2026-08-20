# Neon setup — replacing the dead Railway database

**~10 minutes.** Nothing in the code changes; only `DATABASE_URL` moves.

---

## Why this is fine

The Railway corpus was built with the broken queries — it contains the robot-vacuum patents, the surgical staplers, and the off-topic NIH grants. The relevance gate only filters records *on the way in*, so those rows would have survived a migration anyway and you'd have had to purge them.

**Starting clean on Neon is the outcome you wanted.** Everything re-ingests from the source APIs (`backfill_from=2020-01-01`), this time through the fixed queries. The only true loss is `first_seen_at` history — i.e. "when did NIA first notice this" — which matters for nothing you're doing this week.

---

## Steps

**1 · Create the database**

Go to [neon.tech](https://neon.tech) → sign in with GitHub → **Create project**.

- Name: `nia`
- Postgres version: default is fine
- Region: **US East (Ohio)** — GitHub Actions runners are there, so this is the lowest-latency choice for the nightly job

**2 · Copy the connection string**

Neon shows it right after creation (Dashboard → **Connect**). It looks like:

```
postgresql://nia_owner:npg_XXXXXXXX@ep-cool-name-12345678.us-east-2.aws.neon.tech/nia?sslmode=require
```

Take the **direct** connection string, not the pooled one — the `-pooler` variant is for serverless functions opening thousands of short connections. This is a nightly batch job; direct is correct and simpler.

**Keep `?sslmode=require` on the end.** Neon rejects unencrypted connections, and psycopg2 won't add it for you.

**3 · Point your local `.env` at it**

Edit `E:\Work\NIA\.env`, replace the `DATABASE_URL=` line:

```
DATABASE_URL=postgresql://nia_owner:npg_XXXX@ep-xxx.us-east-2.aws.neon.tech/nia?sslmode=require
```

Leave every other variable alone.

**4 · Create the schema and verify**

```powershell
cd E:\Work\NIA
python main.py doctor --only db     # expect OK, "0 records" — empty but reachable
python main.py init                 # creates every table
python main.py doctor --only db     # expect OK again
```

If step one fails, the error tells you which: `could not translate host name` = typo in the host; `password authentication failed` = truncated password; `SSL required` = you dropped `?sslmode=require`.

**5 · Fill it**

```powershell
python main.py run-all
```

Expect roughly 10 minutes. Watch for the new log lines that didn't exist before:

```
pipeline: relevance gate — kept N/M (...)
pipeline: top rejection reasons — ...
signal pipeline: relevance gate dropped N/M off-topic records
```

Those are the fix working. If the "kept" ratio is near 100%, tell me — the gate should be rejecting a visible fraction.

**6 · Build the site**

```powershell
python build_site.py
```

Then the **Roborock test**: open `site/graph.html`, filter to organisations, read the top names. Real neurotech = clean. A vacuum or lawnmower company = tell me.

**7 · Update the GitHub secret**

GitHub → repo **Settings → Secrets and variables → Actions** → `DATABASE_URL` → **Update** → paste the same Neon string.

Until you do this, the nightly workflow still tries Railway and fails.

---

## Things worth knowing about Neon's free tier

- **0.5 GB storage.** This corpus is far under that — patents with abstracts run tens of MB.
- **Autosuspend after ~5 minutes idle.** The first query after a quiet period takes about half a second to wake the compute. Irrelevant for a batch job; it would matter if you ever put a live dashboard on it, which is exactly why the dashboard is a static snapshot.
- **No credit card.** Unlike Railway, which is what killed this one.
- Point-in-time restore is included, so an accidental bad ingest is recoverable.

---

## If you'd rather not do this tonight

`python build_site.py --demo` produces the whole site from the built-in corpus with no database at all. It's watermarked as illustrative, so it can't be mistaken for live data — but it means the demo path is never blocked on infrastructure.
