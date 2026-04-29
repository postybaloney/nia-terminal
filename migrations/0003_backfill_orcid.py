"""Backfill ORCID and OpenAlex author URLs for existing thesis records.

Revision ID: 0003_backfill_orcid
Revises: 0002_theses

This is a DATA migration (no DDL changes). For each openalex thesis row
whose raw_payload lacks 'author_orcid', it:
  1. Batch-fetches the work records from api.openalex.org using the
     stored source_id (OpenAlex work ID).
  2. Extracts authorships[0].author.orcid and authorships[0].author.id.
  3. Merges those values into the existing raw_payload JSONB.

To run standalone (outside alembic):
    python main.py backfill-orcid

To run via alembic (requires a live DB connection):
    alembic upgrade 0003_backfill_orcid

NOTE: alembic runs migrations synchronously; this migration uses
requests (synchronous httpx) so it can run inside the alembic runner.
"""
from __future__ import annotations

import json
import logging
import time

from alembic import op
import sqlalchemy as sa

revision = "0003_backfill_orcid"
down_revision = "0002_theses"
branch_labels = None
depends_on = None

log = logging.getLogger(__name__)

_BATCH = 50           # OpenAlex supports up to 100 IDs per filter request
_DELAY = 1.0          # seconds between batches (stay in polite pool)
_OA_BASE = "https://api.openalex.org/works"


def upgrade() -> None:
    """Fetch and write ORCID / OpenAlex author URLs for all openalex theses."""
    import httpx

    bind = op.get_bind()

    # Find rows that need backfilling
    result = bind.execute(
        sa.text(
            """
            SELECT id, source_id, raw_payload
            FROM theses
            WHERE source = 'openalex'
              AND (
                raw_payload IS NULL
                OR raw_payload->>'author_orcid' IS NULL
              )
            ORDER BY id
            """
        )
    )
    rows = result.fetchall()

    if not rows:
        log.info("backfill_orcid: nothing to backfill")
        return

    log.info("backfill_orcid: %d rows need ORCID backfill", len(rows))

    # Index by source_id for fast lookup after batch fetch
    id_map: dict[str, tuple[int, dict]] = {
        r.source_id: (r.id, r.raw_payload or {}) for r in rows
    }
    source_ids = list(id_map.keys())

    updated = 0
    for i in range(0, len(source_ids), _BATCH):
        batch = source_ids[i : i + _BATCH]
        filter_str = "|".join(f"https://openalex.org/{sid}" for sid in batch)

        try:
            resp = httpx.get(
                _OA_BASE,
                params={
                    "filter": f"ids.openalex:{filter_str}",
                    "select": "id,authorships",
                    "per-page": _BATCH,
                },
                timeout=30,
            )
            resp.raise_for_status()
            works = resp.json().get("results") or []
        except Exception as exc:
            log.warning("backfill_orcid: batch %d failed: %s", i // _BATCH, exc)
            time.sleep(_DELAY * 3)
            continue

        for work in works:
            raw_id = work.get("id", "")
            sid = raw_id.replace("https://openalex.org/", "")
            if sid not in id_map:
                continue

            row_id, payload = id_map[sid]
            authorships = work.get("authorships") or []
            if not authorships:
                continue

            first_author = authorships[0].get("author") or {}
            orcid = first_author.get("orcid")          # full URL or None
            oa_url = first_author.get("id")             # e.g. https://openalex.org/A123

            if not orcid and not oa_url:
                continue

            new_payload = {**payload}
            if orcid:
                new_payload["author_orcid"] = orcid
            if oa_url:
                new_payload["author_openalex_url"] = oa_url

            bind.execute(
                sa.text(
                    "UPDATE theses SET raw_payload = :p WHERE id = :id"
                ),
                {"p": json.dumps(new_payload), "id": row_id},
            )
            updated += 1

        log.info(
            "backfill_orcid: batch %d/%d — updated so far: %d",
            i // _BATCH + 1, -(-len(source_ids) // _BATCH), updated,
        )
        time.sleep(_DELAY)

    log.info("backfill_orcid: complete — %d/%d rows updated", updated, len(rows))


def downgrade() -> None:
    """Remove author_orcid and author_openalex_url from all thesis raw_payloads."""
    bind = op.get_bind()
    bind.execute(
        sa.text(
            """
            UPDATE theses
            SET raw_payload = raw_payload
                - 'author_orcid'
                - 'author_openalex_url'
            WHERE source = 'openalex'
              AND raw_payload IS NOT NULL
            """
        )
    )
    log.info("backfill_orcid: downgrade complete — ORCID fields removed")
