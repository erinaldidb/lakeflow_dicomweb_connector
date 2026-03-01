# Changelog

## v0.2.0 — 2026-02-28

### New Features

#### `diagnostics` Table
- New built-in table that probes every QIDO-RS and WADO-RS endpoint on every pipeline trigger
- Reports `supported`, `status_code`, `content_type`, `latency_ms`, and `notes` per endpoint
- Covers 9 endpoints: flat and hierarchical QIDO-RS, series metadata, full instance retrieval, frame retrieval, and rendered image
- Useful for initial connectivity validation and ongoing PACS health monitoring

#### Static DICOMweb / S3 Static WADO Server Support
- New `wado_mode` option: `auto` (default), `full` (`.dcm`), `frames` (`.jpg` frame 1)
- `wado_mode=auto` tries full DICOM retrieval first; automatically falls back to frame retrieval on HTTP 404/406/415 and caches the detected mode for the rest of the run
- Flat `/series` and `/instances` QIDO-RS endpoints gracefully handled when blocked (HTTP 403) — connector falls back to hierarchical queries
- Verified against [RadicalImaging/static-dicomweb](https://github.com/RadicalImaging/static-dicomweb) and AWS S3 Static WADO deployments

#### Full DICOM JSON Metadata (`fetch_metadata=true`)
- New `fetch_metadata` table option (default: `false`)
- When enabled, fetches the full DICOM JSON object for each instance via `WADO-RS GET .../series/{uid}/metadata`
- Stored in the `metadata` column as native `VariantType` (Spark 4.0+ / DBR 15.x+)
- `parse_value()` utility handles `JSON string → VariantVal` conversion transparently — no `selectExpr` step needed in the pipeline view
- Query with Spark semi-structured syntax: `metadata:00080060.Value[0]`

#### `connection_name` Lineage Column
- Every table (`studies`, `series`, `instances`, `diagnostics`) now includes a `connection_name` column
- Defaults to `base_url`; override with the `connection_name` connection option
- Enables multi-source ingestion into shared Delta tables with full lineage tracing back to the originating PACS

#### `object_catalog` Presentation View
- Created automatically after each pipeline run as Step 4 of the ingestion notebook
- Exposes all `instances` columns under consistent snake_case names
- `dicom_file_path` → `local_path`, `metadata` → `meta`; all PascalCase DICOM columns aliased (e.g. `SOPInstanceUID` → `sop_instance_uid`)

### Improvements

- **`max_concurrent_requests`** option (default: `16`): bounds simultaneous WADO-RS connections per Spark task to prevent source PACS overload
- **`download_threads`** option (default: `8`): thread-level parallelism for standalone (non-Spark) use
- Unit test count increased from 53 → 66; integration tests added for VariantType chain and OSS declarative pipeline (`pyspark.pipelines`)
- Integration tests marked with `pytest.mark.integration` and excluded from CI (require live network access to Orthanc demo)
- Ruff formatting enforced across all source files and notebooks

### Schema Changes

| Table | Change |
|-------|--------|
| `studies` | Added `connection_name` column |
| `series` | Added `connection_name` column |
| `instances` | Added `connection_name` column; `metadata` now `VariantType` (was `StringType`) |
| `diagnostics` | New table |
| `object_catalog` | New view over `instances` with snake_case column names |

### Installation

```bash
%pip install git+https://github.com/erinaldidb/lakeflow_dicomweb_connector.git@v0.2.0
```

---

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
