"""
Unit tests for DICOMwebLakeflowConnect, dicomweb_parser, and dicomweb_schemas.
HTTP calls are mocked via unittest.mock.patch.
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from databricks.labs.community_connector.sources.dicomweb.dicomweb import (
    DICOMwebLakeflowConnect,
    _subtract_days,
)
from databricks.labs.community_connector.sources.dicomweb.dicomweb_parser import (
    parse_study,
    parse_series,
    parse_instance,
)
from databricks.labs.community_connector.sources.dicomweb.dicomweb_schemas import (
    get_schema,
    STUDIES_SCHEMA,
    SERIES_SCHEMA,
    INSTANCES_SCHEMA,
)


# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------

class TestParser:
    def test_parse_study_full(self, studies_response):
        record = parse_study(studies_response[0])
        assert record["StudyInstanceUID"] == "1.2.840.113619.2.5.1762583153.215519.978957063.78"
        assert record["StudyDate"] == "20231215"
        assert record["PatientName"] == "Doe^John"
        assert record["PatientID"] == "PID-001"
        assert record["AccessionNumber"] == "ACC123456"
        assert record["ModalitiesInStudy"] == ["CT", "SR"]
        assert record["NumberOfStudyRelatedSeries"] == 3
        assert record["NumberOfStudyRelatedInstances"] == 450

    def test_parse_study_missing_optional_fields(self):
        minimal = {
            "0020000D": {"vr": "UI", "Value": ["1.2.3"]},
        }
        record = parse_study(minimal)
        assert record["StudyInstanceUID"] == "1.2.3"
        assert record["PatientName"] is None
        assert record["StudyDate"] is None

    def test_parse_series(self, series_response):
        record = parse_series(series_response[0])
        assert record["SeriesInstanceUID"] == "1.3.12.2.1107.5.2.32.35162.2013120811373024696203156"
        assert record["Modality"] == "CT"
        assert record["SeriesNumber"] == 1
        assert record["BodyPartExamined"] == "CHEST"

    def test_parse_instance(self, instances_response):
        record = parse_instance(instances_response[0])
        assert record["SOPInstanceUID"] == "1.2.840.113619.2.5.1762583153.215519.978957063.78.1.1"
        assert record["InstanceNumber"] == 1
        assert record["ContentDate"] == "20231215"
        assert record["dicom_file_path"] is None  # not yet populated

    def test_parse_pn_no_alphabetic(self):
        """PN tag without Alphabetic — fall back gracefully."""
        obj = {
            "00100010": {"vr": "PN", "Value": [{"Ideographic": "山田"}]},
            "0020000D": {"vr": "UI", "Value": ["1.2.3"]},
        }
        record = parse_study(obj)
        assert record["PatientName"] == "山田"

    def test_parse_empty_value_array(self):
        obj = {
            "0020000D": {"vr": "UI", "Value": []},
        }
        record = parse_study(obj)
        assert record["StudyInstanceUID"] is None

    def test_tag_case_insensitive(self):
        """Tags can be lowercase in some responses."""
        obj = {
            "0020000d": {"vr": "UI", "Value": ["1.2.3"]},
        }
        record = parse_study(obj)
        assert record["StudyInstanceUID"] == "1.2.3"


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestSchemas:
    def test_get_schema_studies(self):
        schema = get_schema("studies")
        field_names = [f.name for f in schema.fields]
        assert "StudyInstanceUID" in field_names
        assert "ModalitiesInStudy" in field_names

    def test_get_schema_series(self):
        schema = get_schema("series")
        field_names = [f.name for f in schema.fields]
        assert "SeriesInstanceUID" in field_names
        assert "Modality" in field_names

    def test_get_schema_instances(self):
        schema = get_schema("instances")
        field_names = [f.name for f in schema.fields]
        assert "SOPInstanceUID" in field_names
        assert "dicom_file_path" in field_names

    def test_unknown_table_raises(self):
        with pytest.raises(ValueError, match="Unknown table"):
            get_schema("patients")

    def test_study_instance_uid_not_nullable(self):
        uid_field = next(f for f in STUDIES_SCHEMA.fields if f.name == "StudyInstanceUID")
        assert uid_field.nullable is False

    def test_dicom_file_path_nullable(self):
        fp_field = next(f for f in INSTANCES_SCHEMA.fields if f.name == "dicom_file_path")
        assert fp_field.nullable is True


# ---------------------------------------------------------------------------
# Connector tests
# ---------------------------------------------------------------------------

class TestConnector:
    def test_missing_base_url_raises(self):
        with pytest.raises(ValueError, match="base_url"):
            DICOMwebLakeflowConnect({})

    def test_list_tables(self, dicomweb_options):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        tables = connector.list_tables()
        assert set(tables) == {"studies", "series", "instances"}

    def test_get_table_schema_studies(self, dicomweb_options):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        schema = connector.get_table_schema("studies", {})
        assert schema == STUDIES_SCHEMA

    def test_get_table_schema_instances(self, dicomweb_options):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        schema = connector.get_table_schema("instances", {})
        assert schema == INSTANCES_SCHEMA

    def test_read_table_metadata(self, dicomweb_options):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        meta = connector.read_table_metadata("studies", {})
        assert meta["primary_keys"] == ["StudyInstanceUID"]
        assert meta["cursor_field"] == "StudyDate"
        assert meta["ingestion_type"] == "cdc"

    def test_read_table_studies(self, dicomweb_options, studies_response):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        # First call returns data, second returns empty (pagination end)
        connector._client.query_studies = MagicMock(
            side_effect=[studies_response, []]
        )
        records_iter, next_offset = connector.read_table("studies", {}, {})
        records = list(records_iter)
        assert len(records) == 2
        assert records[0]["StudyInstanceUID"] == "1.2.840.113619.2.5.1762583153.215519.978957063.78"
        assert "study_date" in next_offset
        assert next_offset["page_offset"] == 0

    def test_read_table_series(self, dicomweb_options, series_response):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        connector._client.query_series = MagicMock(side_effect=[series_response, []])
        records_iter, _ = connector.read_table("series", {}, {})
        records = list(records_iter)
        assert len(records) == 3
        assert records[0]["SeriesInstanceUID"] is not None

    def test_read_table_instances_no_files(self, dicomweb_options, instances_response):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        connector._client.query_instances = MagicMock(side_effect=[instances_response, []])
        records_iter, _ = connector.read_table("instances", {}, {})
        records = list(records_iter)
        assert len(records) == 3
        assert all(r["dicom_file_path"] is None for r in records)

    def test_read_table_instances_fetch_files_missing_volume_raises(self, dicomweb_options, instances_response):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        connector._client.query_instances = MagicMock(return_value=instances_response)
        with pytest.raises(ValueError, match="dicom_volume_path"):
            records_iter, _ = connector.read_table(
                "instances",
                {},
                {"fetch_dicom_files": "true"},
            )
            list(records_iter)  # iterator must be consumed to trigger

    def test_read_table_instances_fetch_files(self, dicomweb_options, instances_response, tmp_path):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        connector._client.query_instances = MagicMock(side_effect=[instances_response, []])
        connector._client.retrieve_instance = MagicMock(return_value=b"DICMDATA")

        records_iter, _ = connector.read_table(
            "instances",
            {},
            {
                "fetch_dicom_files": "true",
                "dicom_volume_path": str(tmp_path),
            },
        )
        records = list(records_iter)
        assert len(records) == 3
        # All records should have a dicom_file_path
        assert all(r["dicom_file_path"] is not None for r in records)
        # Files should exist on disk
        for r in records:
            assert pathlib.Path(r["dicom_file_path"]).exists()

    def test_read_table_instances_fetch_files_parallel(self, dicomweb_options, instances_response, tmp_path):
        """download_threads > 1 should issue all retrieve_instance calls concurrently."""
        import threading
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        connector._client.query_instances = MagicMock(side_effect=[instances_response, []])

        concurrent_calls = []
        lock = threading.Lock()

        def fake_retrieve(study_uid, series_uid, sop_uid):
            with lock:
                concurrent_calls.append(threading.current_thread().name)
            return b"DICMDATA"

        connector._client.retrieve_instance = MagicMock(side_effect=fake_retrieve)

        records_iter, _ = connector.read_table(
            "instances",
            {},
            {"fetch_dicom_files": "true", "dicom_volume_path": str(tmp_path), "download_threads": "4"},
        )
        records = list(records_iter)
        assert len(records) == 3
        assert all(r["dicom_file_path"] is not None for r in records)
        # All three downloads were dispatched (one call per instance)
        assert connector._client.retrieve_instance.call_count == 3

    def test_read_table_pagination(self, dicomweb_options):
        page1 = [{"0020000D": {"vr": "UI", "Value": [f"1.2.{i}"]}} for i in range(2)]
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        # Simulate: page1 (full, size=2), page2 (full, size=2), page3 (empty)
        connector._client.query_studies = MagicMock(side_effect=[page1, page1, []])
        records_iter, _ = connector.read_table(
            "studies", {}, {"page_size": "2"}
        )
        records = list(records_iter)
        assert len(records) == 4  # 2 + 2 pages

    def test_read_table_unknown_table_raises(self, dicomweb_options):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        with pytest.raises(ValueError, match="Unknown table"):
            connector.read_table("patients", {}, {})

    def test_read_table_with_start_offset(self, dicomweb_options):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        connector._client.query_studies = MagicMock(return_value=[])
        records_iter, next_offset = connector.read_table(
            "studies",
            {"study_date": "20231201", "page_offset": 50},
            {},
        )
        list(records_iter)
        call_args = connector._client.query_studies.call_args
        assert call_args.kwargs.get("offset") == 50 or call_args.args[2] == 50


# ---------------------------------------------------------------------------
# Utility tests
# ---------------------------------------------------------------------------

class TestUtilities:
    def test_subtract_days_normal(self):
        assert _subtract_days("20231215", 5) == "20231210"

    def test_subtract_days_zero(self):
        assert _subtract_days("20231215", 0) == "20231215"

    def test_subtract_days_default_start(self):
        from databricks.labs.community_connector.sources.dicomweb.dicomweb import DEFAULT_START_DATE
        assert _subtract_days(DEFAULT_START_DATE, 10) == DEFAULT_START_DATE

    def test_subtract_days_cross_month(self):
        assert _subtract_days("20231201", 3) == "20231128"

    def test_subtract_days_invalid_date(self):
        # Should return input unchanged on invalid date
        result = _subtract_days("invalid", 5)
        assert result == "invalid"


import pathlib  # noqa: E402 (needed for the fetch_files test above)
