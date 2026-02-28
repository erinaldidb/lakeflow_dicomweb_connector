# Changelog

## v0.1.0 — 2026-02-28

Initial release of the Lakeflow DICOMweb Community Connector.

### Features

#### DICOM Metadata Ingestion (QIDO-RS)
- Incremental ingestion of **studies**, **series**, and **instances** tables from any DICOMweb-compliant VNA or PACS system
- `StudyDate`-based cursor strategy — each run queries only new/updated records since the last ingestion
- Configurable `lookback_days` to catch late-finalised studies
- Automatic pagination with configurable `page_size`
- VR-aware DICOM JSON tag parser supporting `DA`, `TM`, `PN`, `CS`, `UI`, `LO`, `IS`, `DS`, and more

#### Raw DICOM File Retrieval (WADO-RS)
- Optional retrieval of `.dcm` files via WADO-RS (`fetch_dicom_files=true`)
- Files written directly to a Unity Catalog Volume (`dicom_volume_path`)
- Graceful handling of Unity Catalog Volume FUSE restrictions (`mkdir` EPERM)
- `dicom_file_path` column populated per instance record on successful write

#### Spark-Native Parallel Downloads
- Switches to `DataSourceStreamReader` with one `DicomBatchPartition` per worker when `fetch_dicom_files=true`
- `partitions()` collects all instance metadata on the driver via QIDO-RS, then divides work across Spark tasks
- `read(partition)` runs on each worker — recreates the HTTP client and downloads files independently
- `max_concurrent_requests` option (default: `16`) bounds the number of simultaneous WADO-RS connections to the PACS, preventing source system overload

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
- 53 unit tests with mocked HTTP responses (no network required)
- Integration tested against the public Orthanc demo (`orthanc.uclouvain.be/demo/dicom-web`)
- GitHub Actions: CI matrix (Python 3.10 / 3.11 / 3.12), Lint (ruff + pylint ≥ 8.0), Release on tag push
- Ruff formatting + lint enforced; pylint minimum score 8.0

### Supported Systems

| System | Status |
|--------|--------|
| Any DICOMweb-compliant server (QIDO-RS / WADO-RS) | Supported |
| Orthanc | Supported (DICOMweb plugin) |
| dcm4chee | Supported (standard DICOMweb endpoints) |
| Sectra, Agfa, Philips PACS | Supported (confirm QIDO-RS `StudyDate` filter) |

### Schema

| Table | Primary Key | Cursor Field |
|-------|-------------|--------------|
| `studies` | `StudyInstanceUID` | `StudyDate` |
| `series` | `SeriesInstanceUID` | `StudyDate` |
| `instances` | `SOPInstanceUID` | `StudyDate` |

### Installation

```bash
%pip install git+https://github.com/erinaldidb/lakeflow_dicomweb_connector.git@v0.1.0
```
