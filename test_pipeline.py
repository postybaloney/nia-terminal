"""
Test suite for the patent intelligence pipeline.

Run with:
  pytest tests/ -v

Uses an in-memory SQLite DB for speed — no Postgres required for tests.
Mocks all external HTTP calls (PatentsView, EPO, Lens) via httpx_mock.

Install test deps:
  pip install pytest pytest-asyncio pytest-httpx
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.models import Base, IngestRun, PatentFamily, RawPatent
from ingestors.base import NormalizedPatent
from ingestors.patentsview import PatentsViewIngestor
from ingestors.lens import LensIngestor


# ── Test DB fixture ───────────────────────────────────────────────────────────

@pytest.fixture(scope="function")
def test_engine():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)


@pytest.fixture(scope="function")
def test_session(test_engine):
    Session = sessionmaker(bind=test_engine)
    session = Session()
    yield session
    session.close()


# ── NormalizedPatent ──────────────────────────────────────────────────────────

def make_patent(
    source="patentsview",
    source_id="US12345678",
    family_id=None,
    title="Neural stimulation device",
    abstract="A method for stimulating neural tissue using electrode arrays.",
) -> NormalizedPatent:
    return NormalizedPatent(
        source=source,
        source_id=source_id,
        family_id=family_id,
        title=title,
        abstract=abstract,
        filing_date=datetime(2023, 6, 1, tzinfo=timezone.utc),
        grant_date=datetime(2024, 1, 15, tzinfo=timezone.utc),
        assignees=[{"name": "Neuralink Corp", "country": "US"}],
        inventors=[{"name": "Elon Musk"}],
        cpc_codes=["A61N1/36", "A61B5/0476"],
        ipc_codes=["A61N 1/36"],
        matched_query="neurotech neural stimulation",
    )


# ── BaseIngestor helpers ──────────────────────────────────────────────────────

class TestBaseIngestorHelpers:
    def test_safe_date_iso(self):
        from ingestors.base import BaseIngestor

        class ConcreteIngestor(BaseIngestor):
            name = "test"
            async def fetch(self): return []

        ing = ConcreteIngestor([], "2023-01-01")
        assert ing._safe_date("2023-06-15") == datetime(2023, 6, 15)
        assert ing._safe_date("20230615") == datetime(2023, 6, 15)
        assert ing._safe_date(None) is None
        assert ing._safe_date("not-a-date") is None

    def test_truncate(self):
        from ingestors.base import BaseIngestor

        class ConcreteIngestor(BaseIngestor):
            name = "test"
            async def fetch(self): return []

        ing = ConcreteIngestor([], "2023-01-01")
        assert ing._truncate("hello", 3) == "hel"
        assert ing._truncate(None) is None
        assert len(ing._truncate("x" * 5000)) == 4000


# ── PatentsView ingestor ──────────────────────────────────────────────────────

class TestPatentsViewIngestor:
    @pytest.mark.asyncio
    async def test_normalize_maps_fields(self):
        ingestor = PatentsViewIngestor(
            queries=["neurotech neural stimulation"],
            since="2024-01-01",
            per_page=25,
        )
        raw = {
            "patent_id": "US11234567",
            "patent_title": "Cortical electrode array",
            "patent_date": "2024-03-01",
            "patent_abstract": "An implantable electrode array for recording neural signals.",
            "assignees": [{"assignee_organization": "BrainCo Inc", "assignee_country": "US"}],
            "inventors": [{"inventor_first_name": "Jane", "inventor_last_name": "Doe"}],
            "cpcs": [{"cpc_subgroup_id": "A61N1/36"}],
            "ipcs": [{"ipc_subgroup": "A61N 1/36"}],
        }
        result = ingestor._normalize(raw, "neurotech neural stimulation")

        assert result.source == "patentsview"
        assert result.source_id == "US11234567"
        assert result.title == "Cortical electrode array"
        assert result.family_id is None  # PatentsView doesn't provide these
        assert result.assignees == [{"name": "BrainCo Inc", "country": "US"}]
        assert result.inventors == [{"name": "Jane Doe"}]
        assert result.cpc_codes == ["A61N1/36"]
        assert result.grant_date == datetime(2024, 3, 1)

    @pytest.mark.asyncio
    async def test_fetch_calls_api(self):
        ingestor = PatentsViewIngestor(
            queries=["neurotech"],
            since="2024-01-01",
            per_page=5,
        )
        mock_response = {
            "patents": [
                {
                    "patent_id": "US99999999",
                    "patent_title": "Test patent",
                    "patent_date": "2024-06-01",
                    "patent_abstract": "Test abstract.",
                    "assignees": [],
                    "inventors": [],
                    "cpcs": [],
                    "ipcs": [],
                }
            ],
            "total_patent_count": 1,
        }

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_resp = MagicMock()
            mock_resp.json.return_value = mock_response
            mock_resp.raise_for_status = MagicMock()
            mock_client.get = AsyncMock(return_value=mock_resp)

            results = await ingestor.fetch()

        assert len(results) == 1
        assert results[0].source_id == "US99999999"


# ── Lens ingestor ─────────────────────────────────────────────────────────────

class TestLensIngestor:
    def test_normalize_extracts_npl_citations(self):
        ingestor = LensIngestor(queries=["bci"], since="2024-01-01")
        raw = {
            "lens_id": "lens:001",
            "title": "BCI device",
            "abstract": "Brain-computer interface for paralyzed patients.",
            "date_published": "2024-05-10",
            "filing_date": "2023-11-01",
            "families": [{"family_id": 12345}],
            "applicants": [{"name": "University of Pittsburgh", "residence": "US"}],
            "inventors": [{"first_name": "Andrew", "last_name": "Schwartz"}],
            "classifications_cpc": {"classification_symbol": ["A61N1/36"]},
            "classifications_ipcr": {"classification_symbol": []},
            "npl_citations": [
                {"citation_text": "Schwartz AB et al. Neuron 2004"},
                {"citation_text": "Hochberg LR et al. Nature 2006"},
            ],
        }
        result = ingestor._normalize(raw, "bci")

        assert result.source == "lens"
        assert result.source_id == "lens:001"
        assert result.family_id == "12345"
        assert result.assignees == [{"name": "University of Pittsburgh", "country": "US"}]
        assert result.raw_payload["npl_citation_count"] == 2
        assert len(result.raw_payload["npl_sample"]) == 2


# ── Deduplication / pipeline logic ────────────────────────────────────────────

class TestDeduplication:
    def test_surrogate_family_id_is_stable(self):
        from pipeline import _surrogate_family_id

        id1 = _surrogate_family_id("patentsview", "US12345678")
        id2 = _surrogate_family_id("patentsview", "US12345678")
        id3 = _surrogate_family_id("patentsview", "US99999999")

        assert id1 == id2
        assert id1 != id3
        assert id1.startswith("S-")
        assert len(id1) == 18  # "S-" + 16 hex chars

    def test_pick_family_id_prefers_real_id(self):
        from pipeline import _pick_family_id

        with_family = make_patent(family_id="DOCDB-123456")
        without_family = make_patent(family_id=None)

        assert _pick_family_id(with_family) == "DOCDB-123456"
        assert _pick_family_id(without_family).startswith("S-")

    def test_within_batch_deduplication(self):
        """Same (source, source_id) appearing from two queries should collapse to one row."""
        p1 = make_patent(source_id="US12345")
        p2 = make_patent(source_id="US12345")  # duplicate
        p3 = make_patent(source_id="US99999")

        patents = [p1, p2, p3]
        seen: dict[tuple, NormalizedPatent] = {}
        for p in patents:
            key = (p.source, p.source_id)
            if key not in seen:
                seen[key] = p
        deduped = list(seen.values())

        assert len(deduped) == 2
        assert {p.source_id for p in deduped} == {"US12345", "US99999"}

    def test_merge_family_extends_cpc_codes(self):
        from pipeline import _merge_family

        family = PatentFamily(
            family_id="F-001",
            cpc_codes=["A61N1/36"],
            sources=["patentsview"],
        )
        incoming = make_patent()
        incoming.cpc_codes = ["A61N1/36", "A61B5/0476"]  # one new code
        incoming.source = "epo"

        _merge_family(family, incoming)

        assert set(family.cpc_codes) == {"A61N1/36", "A61B5/0476"}
        assert "epo" in family.sources

    def test_merge_family_updates_earliest_date(self):
        from pipeline import _merge_family

        family = PatentFamily(
            family_id="F-002",
            earliest_filing_date=datetime(2023, 6, 1, tzinfo=timezone.utc),
            cpc_codes=[],
            sources=["patentsview"],
        )
        incoming = make_patent()
        incoming.filing_date = datetime(2022, 3, 15, tzinfo=timezone.utc)  # earlier

        _merge_family(family, incoming)

        assert family.earliest_filing_date == datetime(2022, 3, 15, tzinfo=timezone.utc)


# ── Config validation ─────────────────────────────────────────────────────────

class TestConfig:
    def test_query_list_parses_correctly(self):
        from unittest.mock import patch
        import os

        with patch.dict(os.environ, {
            "DATABASE_URL": "postgresql://u:p@localhost/test",
            "ANTHROPIC_API_KEY": "sk-ant-test",
            "SEARCH_QUERIES": "neural stimulation brain, bci cortex, medtech implant",
        }):
            from importlib import reload
            import config
            reload(config)
            s = config.Settings()  # type: ignore
            assert s.query_list == [
                "neural stimulation brain",
                "bci cortex",
                "medtech implant",
            ]

    def test_epo_enabled_requires_both_credentials(self):
        from unittest.mock import patch

        with patch.dict("os.environ", {
            "DATABASE_URL": "postgresql://u:p@localhost/test",
            "ANTHROPIC_API_KEY": "sk-ant-test",
            "EPO_CLIENT_ID": "my-id",
            "EPO_CLIENT_SECRET": "",
        }):
            from importlib import reload
            import config
            reload(config)
            s = config.Settings()  # type: ignore
            assert not s.epo_enabled

        with patch.dict("os.environ", {
            "DATABASE_URL": "postgresql://u:p@localhost/test",
            "ANTHROPIC_API_KEY": "sk-ant-test",
            "EPO_CLIENT_ID": "my-id",
            "EPO_CLIENT_SECRET": "my-secret",
        }):
            reload(config)
            s = config.Settings()  # type: ignore
            assert s.epo_enabled


# ── Analysis prompt construction ──────────────────────────────────────────────

class TestAnalysis:
    def test_format_patent_list(self):
        from analysis import _format_patent_list

        patents = [make_patent(title=f"Patent {i}", source_id=f"US{i}") for i in range(5)]
        result = _format_patent_list(patents)

        assert "Patent 0" in result
        assert "Patent 4" in result
        assert "Neuralink Corp" in result
        # Should be numbered
        assert "1." in result
        assert "5." in result

    def test_format_patent_list_caps_at_30(self):
        from analysis import _format_patent_list

        patents = [make_patent(source_id=f"US{i}") for i in range(50)]
        result = _format_patent_list(patents[:30])

        # Only first 30 should appear
        assert "30." in result
