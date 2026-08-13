"""
Unit tests for the current-signal ingestors — pure normalization, no network.

Run:  pytest test_signals.py -q
"""
from __future__ import annotations

from datetime import datetime

from ingestors.signals.base import BaseSignalIngestor
from ingestors.signals.clinicaltrials import ClinicalTrialsIngestor
from ingestors.signals.nih_reporter import NIHReporterIngestor
from ingestors.signals.openfda import OpenFDAIngestor


def _mk(cls):
    return cls(queries=["neurostimulation"], since="2025-01-01", per_page=10)


class TestSafeDate:
    def test_iso_date(self):
        ing = _mk(NIHReporterIngestor)
        assert ing._safe_date("2025-09-01") == datetime(2025, 9, 1)

    def test_iso_datetime(self):
        ing = _mk(NIHReporterIngestor)
        assert ing._safe_date("2025-09-01T12:09:00Z") == datetime(2025, 9, 1)

    def test_year_month(self):
        ing = _mk(NIHReporterIngestor)
        assert ing._safe_date("2025-09") == datetime(2025, 9, 1)

    def test_garbage(self):
        ing = _mk(NIHReporterIngestor)
        assert ing._safe_date("not a date") is None
        assert ing._safe_date(None) is None


class TestNIHNormalize:
    RAW = {
        "appl_id": 11072253,
        "project_num": "1R44NS135064-01",
        "project_title": "Closed-loop vagus nerve stimulation for epilepsy",
        "abstract_text": "We propose a closed-loop VNS system...",
        "organization": {"org_name": "NEUROSIGNAL INC", "org_state": "CA"},
        "principal_investigators": [{"first_name": "Ada", "last_name": "Lovelace"}],
        "award_amount": 1250000,
        "project_start_date": "2025-09-01T00:00:00",
        "fiscal_year": 2025,
        "activity_code": "R44",
    }

    def test_normalize(self):
        s = _mk(NIHReporterIngestor)._normalize(self.RAW, "vagus nerve stimulation")
        assert s.source == "nih_reporter"
        assert s.source_id == "11072253"
        assert s.signal_type == "grant"
        assert s.organization == "NEUROSIGNAL INC"
        assert s.amount == 1250000
        assert s.people == [{"name": "Ada Lovelace", "role": "PI"}]
        assert s.event_date == datetime(2025, 9, 1)
        assert s.status == "R44"  # SBIR phase II — commercial intent flag
        assert s.url.endswith("/11072253")
        assert s.matched_query == "vagus nerve stimulation"


class TestClinicalTrialsNormalize:
    STUDY = {
        "protocolSection": {
            "identificationModule": {
                "nctId": "NCT06999999",
                "briefTitle": "Adaptive DBS for Parkinson's Disease",
                "organization": {"fullName": "Example University"},
            },
            "statusModule": {
                "overallStatus": "RECRUITING",
                "startDateStruct": {"date": "2026-03"},
            },
            "sponsorCollaboratorsModule": {"leadSponsor": {"name": "Medtronic"}},
            "descriptionModule": {"briefSummary": "A study of adaptive DBS."},
            "designModule": {"studyType": "INTERVENTIONAL", "phases": ["NA"]},
            "conditionsModule": {"conditions": ["Parkinson Disease"]},
        }
    }

    def test_normalize(self):
        s = _mk(ClinicalTrialsIngestor)._normalize(self.STUDY, "deep brain stimulation")
        assert s.source == "clinicaltrials"
        assert s.source_id == "NCT06999999"
        assert s.signal_type == "trial"
        assert s.organization == "Medtronic"
        assert s.status == "RECRUITING"
        assert s.event_date == datetime(2026, 3, 1)
        assert "Parkinson Disease" in s.tags
        assert s.url == "https://clinicaltrials.gov/study/NCT06999999"


class TestOpenFDANormalize:
    RAW_510K = {
        "k_number": "K261234",
        "device_name": "NeuroWave EEG Monitoring System",
        "applicant": "NeuroWave Systems Inc.",
        "decision_date": "2026-05-14",
        "decision_description": "Substantially Equivalent",
        "product_code": "OMC",
        "clearance_type": "Traditional",
    }
    RAW_PMA = {
        "pma_number": "P250001",
        "supplement_number": "S002",
        "trade_name": "Percept RC",
        "generic_name": "Deep brain stimulator",
        "applicant": "Medtronic",
        "decision_date": "2026-04-02",
        "decision_code": "APPR",
        "product_code": "MHY",
    }

    def test_510k(self):
        ing = _mk(OpenFDAIngestor)
        s = ing._normalize_510k(self.RAW_510K, "EEG monitoring")
        assert s.source == "fda_510k"
        assert s.source_id == "K261234"
        assert s.signal_type == "clearance"
        assert s.organization == "NeuroWave Systems Inc."
        assert s.event_date == datetime(2026, 5, 14)
        assert "OMC" in s.tags
        assert "K261234" in s.url

    def test_pma(self):
        ing = _mk(OpenFDAIngestor)
        s = ing._normalize_pma(self.RAW_PMA, "deep brain stimulation")
        assert s.source == "fda_pma"
        assert s.source_id == "P250001/S002"
        assert s.signal_type == "approval"
        assert s.status == "APPR"
        assert s.event_date == datetime(2026, 4, 2)


class TestDedupKeyStability:
    def test_pma_supplement_distinct(self):
        ing = _mk(OpenFDAIngestor)
        a = ing._normalize_pma(TestOpenFDANormalize.RAW_PMA, "q")
        b_raw = dict(TestOpenFDANormalize.RAW_PMA, supplement_number="S003")
        b = ing._normalize_pma(b_raw, "q")
        assert a.source_id != b.source_id  # supplements are separate FDA actions
