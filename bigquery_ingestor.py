"""
Google BigQuery ingestor — patents-public-data dataset.
Dataset: `patents-public-data.patents.publications`
Docs: https://cloud.google.com/blog/topics/public-datasets/google-patents-public-data

Covers ~100M publications across 17+ patent offices.
First 1 TB of queries/month is free; ~10GB per typical run here.

Setup:
  1. Create a GCP project and enable BigQuery API
  2. Create a service account, download JSON key
  3. Set GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
  4. Set BIGQUERY_PROJECT_ID=your-project-id

Unlike the REST ingestors, BigQuery runs a SQL query and streams results
back as a DataFrame. Ideal for bulk trend analysis, not real-time lookups.
"""
from __future__ import annotations

import logging
from datetime import datetime

from config import settings
from ingestors.base import BaseIngestor, NormalizedPatent

log = logging.getLogger(__name__)

# Medtech/neurotech CPC codes to filter by — much more precise than keyword
# search alone. Combine with keyword search using AND for high recall + precision.
MEDTECH_CPC_PREFIXES = [
    "A61B",   # diagnosis; surgery; identification
    "A61F2",  # filters implantable into blood vessels; prostheses
    "A61N1",  # electrotherapy; magnetotherapy; radiation therapy; ultrasound therapy
    "A61N2",  # magnetotherapy
    "A61B5",  # measuring for diagnostic purposes
    "H04R25", # hearing aids
    "G16H",   # healthcare informatics
]

_SQL_TEMPLATE = """
SELECT
  publication_number,
  family_id,
  application_number,
  country_code,
  ARRAY_TO_STRING(
    ARRAY(SELECT title.text FROM UNNEST(title_localized) AS title
          WHERE title.language = 'en' LIMIT 1), ''
  ) AS title,
  ARRAY_TO_STRING(
    ARRAY(SELECT ab.text FROM UNNEST(abstract_localized) AS ab
          WHERE ab.language = 'en' LIMIT 1), ''
  ) AS abstract,
  CAST(filing_date AS STRING) AS filing_date,
  CAST(grant_date AS STRING)  AS grant_date,
  ARRAY_TO_STRING(
    ARRAY(SELECT CONCAT(a.name, '|', IFNULL(a.country_code,''))
          FROM UNNEST(assignee_harmonized) AS a LIMIT 5), ';'
  ) AS assignees,
  ARRAY_TO_STRING(
    ARRAY(SELECT CONCAT(i.name)
          FROM UNNEST(inventor_harmonized) AS i LIMIT 5), ';'
  ) AS inventors,
  ARRAY_TO_STRING(
    ARRAY(SELECT c.code FROM UNNEST(cpc) AS c LIMIT 20), ';'
  ) AS cpc_codes,
  ARRAY_TO_STRING(
    ARRAY(SELECT c.code FROM UNNEST(ipc) AS c LIMIT 10), ';'
  ) AS ipc_codes
FROM
  `patents-public-data.patents.publications`
WHERE
  REGEXP_CONTAINS(
    LOWER(
      ARRAY_TO_STRING(
        ARRAY(SELECT ab.text FROM UNNEST(abstract_localized) AS ab
              WHERE ab.language = 'en' LIMIT 1), ''
      )
    ),
    r'{keyword_pattern}'
  )
  AND EXISTS (
    SELECT 1 FROM UNNEST(cpc) AS c
    WHERE ({cpc_filter})
  )
  AND filing_date >= {since_int}
ORDER BY filing_date DESC
LIMIT {limit}
"""


def _build_cpc_filter() -> str:
    return " OR ".join(
        f"STARTS_WITH(c.code, '{prefix}')" for prefix in MEDTECH_CPC_PREFIXES
    )


class BigQueryIngestor(BaseIngestor):
    name = "bigquery"

    async def fetch(self) -> list[NormalizedPatent]:
        if not settings.bigquery_enabled:
            log.info("bigquery: skipped (no project configured)")
            return []

        # BigQuery client is synchronous; in production wrap with asyncio.to_thread
        try:
            from google.cloud import bigquery  # type: ignore
        except ImportError:
            log.warning("bigquery: google-cloud-bigquery not installed — skipping")
            return []

        client = bigquery.Client(project=settings.bigquery_project_id)
        results: list[NormalizedPatent] = []

        for query_str in self.queries:
            patents = self._run_query(client, query_str)
            results.extend(patents)
            log.info("bigquery: query=%r  fetched=%d", query_str, len(patents))

        return results

    def _run_query(self, client, query_str: str) -> list[NormalizedPatent]:
        # Build keyword regex from query string
        keywords = [w.lower() for w in query_str.split() if len(w) > 4]
        if not keywords:
            return []
        keyword_pattern = "|".join(keywords[:8])

        # Convert YYYY-MM-DD to int YYYYMMDD for BigQuery filing_date
        since_int = int(self.since.replace("-", ""))

        sql = _SQL_TEMPLATE.format(
            keyword_pattern=keyword_pattern,
            cpc_filter=_build_cpc_filter(),
            since_int=since_int,
            limit=self.per_page,
        )

        try:
            df = client.query(sql).to_dataframe()
        except Exception as exc:
            log.error("bigquery query failed: %s", exc)
            return []

        patents: list[NormalizedPatent] = []
        for _, row in df.iterrows():
            try:
                patents.append(self._normalize(row, query_str))
            except Exception as exc:
                log.warning("bigquery: normalize error: %s", exc)

        return patents

    def _normalize(self, row, query: str) -> NormalizedPatent:
        def split_entities(s: str) -> list[dict]:
            if not s:
                return []
            result = []
            for item in s.split(";"):
                parts = item.split("|")
                result.append({
                    "name": parts[0].strip(),
                    "country": parts[1].strip() if len(parts) > 1 else "",
                })
            return result

        return NormalizedPatent(
            source=self.name,
            source_id=str(row.get("publication_number", "")),
            family_id=str(row.get("family_id", "")) or None,
            title=self._truncate(str(row.get("title", "")) or None),
            abstract=self._truncate(str(row.get("abstract", "")) or None),
            filing_date=self._safe_date(str(row.get("filing_date", ""))),
            grant_date=self._safe_date(str(row.get("grant_date", ""))),
            assignees=split_entities(str(row.get("assignees", ""))),
            inventors=[
                {"name": n.strip()}
                for n in str(row.get("inventors", "")).split(";")
                if n.strip()
            ],
            cpc_codes=[
                c.strip()
                for c in str(row.get("cpc_codes", "")).split(";")
                if c.strip()
            ],
            ipc_codes=[
                c.strip()
                for c in str(row.get("ipc_codes", "")).split(";")
                if c.strip()
            ],
            matched_query=query,
            raw_payload={
                "publication_number": row.get("publication_number"),
                "country_code": row.get("country_code"),
                "family_id": row.get("family_id"),
            },
        )
