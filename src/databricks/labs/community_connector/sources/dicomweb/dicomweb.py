"""
DICOMwebLakeflowConnect — main Lakeflow Community Connector class.

Exposes three tables from any DICOMweb-compliant VNA/PACS system:
    studies    QIDO-RS /studies
    series     QIDO-RS /series
    instances  QIDO-RS /instances  (+ optional WADO-RS file retrieval)

Incremental strategy
--------------------
Cursor = ISO date string YYYYMMDD tracking the max StudyDate seen.
QIDO-RS filter: StudyDate={cursor_date minus lookback_days}-{today}
Offset format:  {"study_date": "20231215", "page_offset": 0}

Usage (Databricks notebook)
---------------------------
    from databricks.labs.community_connector.sources.dicomweb import DICOMwebLakeflowConnect
    connector = DICOMwebLakeflowConnect({
        "base_url": "https://orthanc.uclouvain.be/demo/dicom-web",
        "auth_type": "none",
    })
    records, next_offset = connector.read_table("studies", {}, {})
"""

from __future__ import annotations

import logging
import os
import pathlib
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from typing import Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect
from databricks.labs.community_connector.sources.dicomweb.dicomweb_client import DICOMwebClient
from databricks.labs.community_connector.sources.dicomweb.dicomweb_parser import (
    parse_instance,
    parse_series,
    parse_study,
)
from databricks.labs.community_connector.sources.dicomweb.dicomweb_schemas import get_schema

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SUPPORTED_TABLES = ("studies", "series", "instances")
DEFAULT_START_DATE = "19000101"  # Effectively "all history" on first run
DEFAULT_PAGE_SIZE = 100
DEFAULT_LOOKBACK_DAYS = 1
DEFAULT_DOWNLOAD_THREADS = 8


# ---------------------------------------------------------------------------
# Main connector
# ---------------------------------------------------------------------------

class DICOMwebLakeflowConnect(LakeflowConnect):
    """Lakeflow connector for DICOMweb VNA/PACS systems."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        base_url = options.get("base_url")
        if not base_url:
            raise ValueError("Connection option 'base_url' is required")

        self._client = DICOMwebClient(
            base_url=base_url,
            auth_type=options.get("auth_type", "none"),
            username=options.get("username"),
            password=options.get("password"),
            token=options.get("token"),
        )

    # ------------------------------------------------------------------
    # Schema / metadata
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        return list(SUPPORTED_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        return get_schema(table_name)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        return {
            "primary_keys": [_primary_key(table_name)],
            "cursor_field": "StudyDate",
            "ingestion_type": "cdc",
        }

    # ------------------------------------------------------------------
    # Data reading
    # ------------------------------------------------------------------

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """
        Incrementally read records from the given table.

        Args:
            table_name:   One of "studies", "series", "instances".
            start_offset: Previous offset dict, or {} for first run.
            table_options: Per-table options from the pipeline spec.

        Returns:
            (record_iterator, next_offset_dict)
        """
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(f"Unknown table '{table_name}'. Valid: {SUPPORTED_TABLES}")

        page_size = int(table_options.get("page_size", DEFAULT_PAGE_SIZE))
        lookback_days = int(table_options.get("lookback_days", DEFAULT_LOOKBACK_DAYS))
        date_cursor = start_offset.get("study_date", DEFAULT_START_DATE)
        page_offset = start_offset.get("page_offset", 0)

        today_str = date.today().strftime("%Y%m%d")
        effective_start = _subtract_days(date_cursor, lookback_days)
        date_range = f"{effective_start}-{today_str}"

        logger.info(
            "read_table table=%s date_range=%s page_offset=%d page_size=%d",
            table_name, date_range, page_offset, page_size,
        )

        fetch_files = table_options.get("fetch_dicom_files", "false").lower() == "true"
        volume_path = table_options.get("dicom_volume_path", "")
        download_threads = int(table_options.get("download_threads", DEFAULT_DOWNLOAD_THREADS))

        if fetch_files and not volume_path and table_name == "instances":
            raise ValueError(
                "fetch_dicom_files=true requires dicom_volume_path to be set"
            )

        records_iter = self._paginate(
            table_name=table_name,
            date_range=date_range,
            page_size=page_size,
            start_offset=page_offset,
            fetch_files=fetch_files,
            volume_path=volume_path,
            download_threads=download_threads,
        )
        next_offset = {"study_date": today_str, "page_offset": 0}
        return records_iter, next_offset

    # ------------------------------------------------------------------
    # Internal pagination
    # ------------------------------------------------------------------

    def _paginate(
        self,
        table_name: str,
        date_range: str,
        page_size: int,
        start_offset: int,
        fetch_files: bool,
        volume_path: str,
        download_threads: int,
    ) -> Iterator[dict]:
        offset = start_offset
        query_fn = _get_query_fn(self._client, table_name)
        parse_fn = _get_parse_fn(table_name)

        while True:
            raw_records = query_fn(date_range, limit=page_size, offset=offset)
            if not raw_records:
                logger.debug("Empty page at offset=%d — pagination complete", offset)
                break

            records = [parse_fn(raw) for raw in raw_records]

            if fetch_files and table_name == "instances":
                with ThreadPoolExecutor(max_workers=download_threads) as pool:
                    records = list(pool.map(
                        lambda r: self._attach_dicom_file(r, volume_path),
                        records,
                    ))

            yield from records

            if len(raw_records) < page_size:
                # Partial page → last page
                break

            offset += page_size

    def _attach_dicom_file(self, record: dict, volume_path: str) -> dict:
        """Retrieve the .dcm file via WADO-RS and write it to the Volume."""
        study_uid = record.get("StudyInstanceUID")
        series_uid = record.get("SeriesInstanceUID")
        sop_uid = record.get("SOPInstanceUID")

        if not all([study_uid, series_uid, sop_uid]):
            logger.warning("Skipping WADO-RS: missing UIDs in record %s", record)
            return record

        dest_path = pathlib.Path(volume_path) / study_uid / series_uid / f"{sop_uid}.dcm"
        try:
            dicom_bytes = self._client.retrieve_instance(study_uid, series_uid, sop_uid)
            try:
                dest_path.parent.mkdir(parents=True, exist_ok=True)
            except OSError:
                # UC Volume FUSE mounts do not support mkdir via POSIX syscalls;
                # the write_bytes() call below works regardless.
                pass
            dest_path.write_bytes(dicom_bytes)
            record["dicom_file_path"] = str(dest_path)
            logger.debug("Wrote %d bytes → %s", len(dicom_bytes), dest_path)
        except Exception as exc:
            logger.error("WADO-RS retrieval failed for %s: %s", sop_uid, exc)
            record["dicom_file_path"] = None

        return record


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _primary_key(table_name: str) -> str:
    pk_map = {
        "studies": "StudyInstanceUID",
        "series": "SeriesInstanceUID",
        "instances": "SOPInstanceUID",
    }
    return pk_map[table_name]


def _get_query_fn(client: DICOMwebClient, table_name: str):
    return {
        "studies": client.query_studies,
        "series": client.query_series,
        "instances": client.query_instances,
    }[table_name]


def _get_parse_fn(table_name: str):
    return {
        "studies": parse_study,
        "series": parse_series,
        "instances": parse_instance,
    }[table_name]


def _subtract_days(date_str: str, days: int) -> str:
    """Subtract `days` from a YYYYMMDD date string; clamp at DEFAULT_START_DATE."""
    if date_str == DEFAULT_START_DATE or days == 0:
        return date_str
    try:
        d = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
        d = d - timedelta(days=days)
        return d.strftime("%Y%m%d")
    except (ValueError, IndexError):
        return date_str
