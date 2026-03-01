# Changelog

## v1.0.0 — 2026-03-01

Initial release of the Lakeflow DICOMweb Community Connector.

### Features

#### DICOM Metadata Ingestion (QIDO-RS)
- Incremental ingestion of **studies**, **series**, and **instances** tables from any DICOMweb-compliant VNA or PACS system
- `StudyDate`-based cursor strategy — each run queries only new/updated records since the last ingestion
- Configurable `lookback_days` to catch late-finalised studies
- Automatic pagination with configurable `page_size`
- VR-aware DICOM JSON tag parser supporting `DA`, `TM`, `PN`, `CS`, `UI`, `LO`, `IS`, `DS`, and more

#### Raw DICOM File Retrieval (WADO-RS)
- Optional retrieval of `.dcm` or `.jpg` files via WADO-RS (`fetch_dicom_files=true`)
- Files written directly to a Unity Catalog Volume (`dicom_volume_path`)
- `wado_mode` option: `auto` (default — tries full `.dcm`, falls back to frame retrieval on 404/406/415), `full`, `frames`
- Graceful handling of Unity Catalog Volume FUSE restrictions (`mkdir` EPERM)
- `dicom_file_path` column populated per instance record on successful write

#### Full DICOM JSON Metadata (`fetch_metadata=true`)
- Fetches full DICOM JSON per instance via `WADO-RS GET .../series/{uid}/metadata`
- Stored in the `metadata` column as native `VariantType` (Spark 4.0+ / DBR 15.x+)
- Query with Spark semi-structured syntax: `metadata:00080060.Value[0]`

#### Spark-Native Parallel Downloads
- Switches to `DataSourceStreamReader` with one `DicomBatchPartition` per worker when `fetch_dicom_files=true`
- `partitions()` collects all instance metadata on the driver via QIDO-RS, then divides work across Spark tasks
- `read(partition)` runs on each worker — recreates the HTTP client and downloads files independently
- `max_concurrent_requests` option (default: `16`) bounds the number of simultaneous WADO-RS connections to the PACS

#### `diagnostics` Table
- Built-in table that probes every QIDO-RS and WADO-RS endpoint on every pipeline trigger
- Reports `supported`, `status_code`, `content_type`, `latency_ms`, and `notes` per endpoint
- Covers 9 endpoints: flat and hierarchical QIDO-RS, series metadata, full instance retrieval, frame retrieval, and rendered image
- Useful for initial connectivity validation and ongoing PACS health monitoring

#### `object_catalog` Presentation View
- Created automatically after each ingestion run as Step 4 of the pipeline notebook
- Exposes all `instances` columns under consistent snake_case names
- Key aliases: `dicom_file_path` → `local_path`, `metadata` → `meta`, and all PascalCase DICOM columns (e.g. `SOPInstanceUID` → `sop_instance_uid`)

#### `connection_name` Lineage Column
- Every table includes a `connection_name` column (defaults to `base_url`)
- Enables multi-source ingestion into shared Delta tables with full lineage tracing

#### Static DICOMweb / S3 Static WADO Server Support
- Compatible with [RadicalImaging/static-dicomweb](https://github.com/RadicalImaging/static-dicomweb) and AWS S3 Static WADO deployments
- Flat `/series` and `/instances` QIDO-RS endpoints handled gracefully when blocked (HTTP 403)
- Use `wado_mode=frames` to force frame-based retrieval and skip the auto-detection probe

#### Authentication
- `auth_type=none` — open endpoints (Orthanc dev, public demo servers)
- `auth_type=basic` — HTTP Basic authentication (`username` + `password`)
- `auth_type=bearer` — Bearer token authentication (`token`)

#### Lakeflow Framework Integration
- Full `register(spark, "dicomweb")` + `ingest(spark, pipeline_spec)` support
- `_generated_dicomweb_python_source.py` self-contained merged file for Spark DataSource registration
- `SCD_TYPE_1` upsert on primary key by default; `SCD_TYPE_2` and `APPEND_ONLY` supported
- `connector_spec.yaml` for Unity Catalog connection configuration
- Lakeflow Community Connectors framework vendored directly — no external PyPI dependency

#### Developer Experience
- 66 unit tests with mocked HTTP responses (no network required)
- Integration tests against the public Orthanc demo (`orthanc.uclouvain.be/demo/dicom-web`)
- OSS declarative pipeline tests using `pyspark.pipelines` (Spark 4.0+)
- GitHub Actions: CI matrix (Python 3.10 / 3.11 / 3.12), Lint (ruff + pylint ≥ 8.0), Release on tag push
- Integration tests excluded from CI with `pytest.mark.integration` (require live network)

### Supported Systems

| System | Status |
|--------|--------|
| Any DICOMweb-compliant server (QIDO-RS / WADO-RS) | Supported |
| Orthanc (via DICOMweb plugin) | Supported |
| Static DICOMweb / AWS S3 Static WADO | Supported |
| dcm4chee | Supported (standard DICOMweb endpoints) |
| Sectra, Agfa, Philips PACS | Supported (confirm QIDO-RS `StudyDate` filter) |

### Schema

| Object | Type | Primary Key | Cursor Field |
|--------|------|-------------|--------------|
| `studies` | Table | `StudyInstanceUID` | `StudyDate` |
| `series` | Table | `SeriesInstanceUID` | `StudyDate` |
| `instances` | Table | `SOPInstanceUID` | `StudyDate` |
| `diagnostics` | Table | `endpoint` | `probe_timestamp` |
| `object_catalog` | View | — | — |

### Installation

```bash
%pip install git+https://github.com/erinaldidb/lakeflow_dicomweb_connector.git@v1.0.0
```
