# Lakeflow DICOMweb Connector

[![CI](https://github.com/erinaldidb/lakeflow_dicomweb_connector/actions/workflows/ci.yml/badge.svg)](https://github.com/erinaldidb/lakeflow_dicomweb_connector/actions/workflows/ci.yml)
[![Lint](https://github.com/erinaldidb/lakeflow_dicomweb_connector/actions/workflows/lint.yml/badge.svg)](https://github.com/erinaldidb/lakeflow_dicomweb_connector/actions/workflows/lint.yml)
[![Release](https://github.com/erinaldidb/lakeflow_dicomweb_connector/actions/workflows/release.yml/badge.svg)](https://github.com/erinaldidb/lakeflow_dicomweb_connector/actions/workflows/release.yml)
[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Lakeflow-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![DICOM](https://img.shields.io/badge/standard-DICOMweb%20QIDO--RS%20%2F%20WADO--RS-blue)](https://dicom.nema.org/medical/dicom/current/output/chtml/part18/PS3.18.html)
[![License](https://img.shields.io/badge/license-Databricks-lightgrey)](LICENSE)

A [Lakeflow Community Connector](https://github.com/databrickslabs/lakeflow-community-connectors) that incrementally ingests DICOM study, series, and instance metadata from any DICOMweb-compliant VNA or PACS system into Databricks Delta tables.

## Features

- **QIDO-RS** based incremental ingestion with a `StudyDate` cursor
- **Four tables + one view**: `studies`, `series`, `instances`, `diagnostics`, and `object_catalog` (snake_case presentation view over `instances`)
- **Optional WADO-RS** file retrieval — stores `.dcm` files in a Unity Catalog Volume (verified: 526 KB CT slice from Orthanc demo)
- **Optional full DICOM JSON metadata** per instance via `fetch_metadata=true` (stored as `VariantType` on DBR 15.x+)
- **Authentication**: `none`, `basic`, `bearer`
- **Pagination**: configurable page size with automatic looping
- **Lookback window**: catch late-arriving studies with `lookback_days`
- **Diagnostics table**: probes all QIDO-RS + WADO-RS endpoints on every run to report server capabilities and latency

## Supported Systems

| System | Notes | Status |
|--------|-------|--------|
| Any DICOMweb-compliant server (QIDO-RS / WADO-RS) | Hierarchical QIDO-RS endpoints | Available |
| [Orthanc](https://www.orthanc-server.com/) | Via DICOMweb plugin | Available |
| [Static DICOMweb Server](https://github.com/RadicalImaging/static-dicomweb) (incl. AWS S3 Static WADO) | Auto-detected frame retrieval | Available |
| [dcm4chee](https://www.dcm4che.org/) | `dcm4chee` + Keycloak OAuth2 | Phase 3 — planned |

### Static DICOMweb Server Compatibility

The connector is compatible with [RadicalImaging/static-dicomweb](https://github.com/RadicalImaging/static-dicomweb) and its AWS S3-hosted deployments (S3 Static WADO). These servers use frame-based WADO-RS retrieval instead of full DICOM file downloads, and only expose hierarchical QIDO-RS endpoints (flat `/series` and `/instances` return 403).

**Endpoint pattern (Static DICOMweb / S3 Static WADO):**
```
GET /studies?limit=101&offset=0&...                                      ← flat studies (same)
GET /studies/{uid}/series?includefield=...                               ← hierarchical series only
GET /studies/{uid}/series/{uid}/metadata                                 ← WADO-RS series metadata
GET /studies/{uid}/series/{uid}/instances/{uid}/frames/1                 ← frame retrieval (JPEG-LS)
```

Set `wado_mode=frames` to force frame retrieval, or leave it at the default `wado_mode=auto` to detect automatically on the first WADO-RS call.

---

## Installation

```bash
pip install git+https://github.com/erinaldidb/lakeflow_dicomweb_connector.git
```

The `lakeflow-community-connectors` framework is **vendored** into this package — no additional PyPI dependencies beyond `pyspark`, `requests`, and `pydantic`.

Or add to your cluster's library configuration.

---

## Quick Start

```python
from databricks.labs.community_connector.sources.dicomweb import DICOMwebLakeflowConnect

connector = DICOMwebLakeflowConnect({
    "base_url": "https://orthanc.uclouvain.be/demo/dicom-web",
    "auth_type": "none",
})

records_iter, next_offset = connector.read_table(
    table_name="studies",
    start_offset={},
    table_options={"page_size": "100", "lookback_days": "1"},
)

for record in records_iter:
    print(record["StudyInstanceUID"], record["PatientName"])
```

---

## Connection Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `base_url` | Yes | — | DICOMweb endpoint base URL |
| `auth_type` | No | `none` | `none`, `basic`, or `bearer` |
| `username` | No | — | Basic auth username |
| `password` | No | — | Basic auth password (use Databricks Secret) |
| `token` | No | — | Bearer token (use Databricks Secret) |
| `connection_name` | No | `base_url` | Human-readable name injected as `connection_name` in every row of every table. Set this when ingesting from multiple DICOMweb sources into the same Delta tables to enable full lineage tracing. |

## Table Options (per-table in pipeline spec)

> **Important:** every option listed below must be included in the `externalOptionsAllowList` value of your Unity Catalog connection, otherwise Lakeflow rejects it at runtime with *"Option X is not allowed by connection Y and cannot be provided externally."* Use the full allowlist value: `fetch_dicom_files,dicom_volume_path,lookback_days,page_size,start_date,download_threads,max_concurrent_requests,fetch_metadata,wado_mode`

| Option | Default | Description |
|--------|---------|-------------|
| `page_size` | `100` | Records per QIDO-RS request (applies to study-level pagination) |
| `lookback_days` | `1` | Days to subtract from cursor to catch late arrivals |
| `start_date` | `19000101` | Initial cursor date (first run only) |
| `fetch_dicom_files` | `false` | Also retrieve DICOM content via WADO-RS |
| `dicom_volume_path` | — | Required when `fetch_dicom_files=true`; Unity Catalog Volume path |
| `wado_mode` | `auto` | WADO-RS retrieval mode: `auto` (detect), `full` (`.dcm` file), `frames` (`.jpg` frame) |
| `fetch_metadata` | `false` | Fetch full DICOM JSON metadata per instance via WADO-RS `/metadata`; stored in the `metadata` column |
| `max_concurrent_requests` | `16` | Max simultaneous WADO-RS connections to the PACS. Instances are grouped into this many Spark tasks so the cluster never opens more than this many connections at once. Reduce for sensitive PACS systems. |
| `download_threads` | `8` | Thread-level parallelism within a page when running outside Spark (standalone use). |

### `wado_mode` details

| Value | Endpoint used | Output file |
|-------|---------------|-------------|
| `auto` *(default)* | Tries full instance; falls back to frames on 404/406/415 | `.dcm` or `.jpg` |
| `full` | `GET .../instances/{uid}` (multipart/related) | `.dcm` |
| `frames` | `GET .../instances/{uid}/frames/1` (image/jpeg) | `.jpg` |

Use `wado_mode=frames` explicitly for Static DICOMweb / S3 Static WADO Server deployments to skip the auto-detection probe.

---

## Tables

### `studies`
| Column | Type | Source Tag | Description |
|--------|------|-----------|-------------|
| `StudyInstanceUID` | string (PK) | 0020000D | DICOM Study UID |
| `PatientID` | string | 00100020 | Patient identifier |
| `PatientName` | string | 00100010 | Patient name (Alphabetic) |
| `StudyDate` | string | 00080020 | Study date YYYYMMDD |
| `StudyTime` | string | 00080030 | Study time HHMMSS |
| `AccessionNumber` | string | 00080050 | Accession number |
| `StudyDescription` | string | 00081030 | Study description |
| `ModalitiesInStudy` | array\<string\> | 00080061 | Modalities present |
| `NumberOfStudyRelatedSeries` | int | 00201206 | Series count |
| `NumberOfStudyRelatedInstances` | int | 00201208 | Instance count |
| `connection_name` | string | — | UC connection name (lineage) |

### `series`
| Column | Type | Source Tag | Description |
|--------|------|-----------|-------------|
| `SeriesInstanceUID` | string (PK) | 0020000E | DICOM Series UID |
| `StudyInstanceUID` | string | 0020000D | Parent Study UID |
| `StudyDate` | string | 00080020 | Study date YYYYMMDD (cursor field) |
| `SeriesNumber` | int | 00200011 | Series number |
| `SeriesDescription` | string | 0008103E | Series description |
| `Modality` | string | 00080060 | Modality (CT, MR, …) |
| `BodyPartExamined` | string | 00180015 | Body part |
| `SeriesDate` | string | 00080021 | Series date YYYYMMDD |
| `connection_name` | string | — | UC connection name (lineage) |

### `instances`
| Column | Type | Source Tag | Description |
|--------|------|-----------|-------------|
| `SOPInstanceUID` | string (PK) | 00080018 | SOP Instance UID |
| `SeriesInstanceUID` | string | 0020000E | Parent Series UID |
| `StudyInstanceUID` | string | 0020000D | Parent Study UID |
| `SOPClassUID` | string | 00080016 | SOP Class UID |
| `InstanceNumber` | int | 00200013 | Instance number |
| `StudyDate` | string | 00080020 | Study date YYYYMMDD (cursor field) |
| `ContentDate` | string | 00080023 | Content date YYYYMMDD |
| `ContentTime` | string | 00080033 | Content time HHMMSS |
| `dicom_file_path` | string (nullable) | — | Path to `.dcm` or `.jpg` in Volume |
| `metadata` | variant/string (nullable) | — | Full DICOM JSON for this instance; populated when `fetch_metadata=true`. `VariantType` on DBR 15.x+, JSON string on older runtimes. |
| `connection_name` | string | — | UC connection name (lineage) |

### `object_catalog` (view)

A presentation view created automatically after each pipeline run. It selects all columns from `instances` and exposes them under snake_case names, making the table easy to query from SQL or BI tools without case-sensitivity issues.

| View column | Source column | Type |
|-------------|---------------|------|
| `sop_instance_uid` | `SOPInstanceUID` | string (PK) |
| `series_instance_uid` | `SeriesInstanceUID` | string |
| `study_instance_uid` | `StudyInstanceUID` | string |
| `sop_class_uid` | `SOPClassUID` | string |
| `instance_number` | `InstanceNumber` | int |
| `study_date` | `StudyDate` | string |
| `content_date` | `ContentDate` | string |
| `content_time` | `ContentTime` | string |
| `local_path` | `dicom_file_path` | string |
| `meta` | `metadata` | variant |
| `connection_name` | `connection_name` | string |

```sql
-- Example: list all CT instances with their file path
SELECT sop_instance_uid, local_path, meta:00080060.Value[0] AS modality
FROM main.dicom_bronze.object_catalog
WHERE meta:00080060.Value[0] = 'CT';
```

### `diagnostics`

A special read-only table that probes your DICOMweb server on every pipeline trigger and reports which endpoints are reachable and what capabilities are supported. Useful for initial setup validation and ongoing health monitoring.

| Column | Type | Description |
|--------|------|-------------|
| `endpoint` | string (PK) | URL path pattern probed, e.g. `/studies?limit=1` |
| `category` | string | Service category: `QIDO-RS` or `WADO-RS` |
| `description` | string | Human-readable description of the endpoint |
| `supported` | string | `yes`, `no`, `partial`, `unknown`, or `error` |
| `status_code` | int | HTTP status code returned (null on network error) |
| `content_type` | string | Content-Type header from the response |
| `latency_ms` | int | Round-trip latency in milliseconds |
| `notes` | string | Additional context: error message, access-denied reason, etc. |
| `probe_timestamp` | string (PK) | ISO-8601 UTC timestamp of this probe run |
| `connection_name` | string | UC connection name (lineage) |

**Endpoint coverage:**

| Endpoint | Category | What it tests |
|----------|----------|---------------|
| `GET /studies?limit=1` | QIDO-RS | Flat study query |
| `GET /studies/{uid}/series` | QIDO-RS | Hierarchical series query |
| `GET /studies/{uid}/series/{uid}/instances` | QIDO-RS | Hierarchical instances query |
| `GET /series?limit=1` | QIDO-RS | Flat series query (optional — blocked on some servers) |
| `GET /instances?limit=1` | QIDO-RS | Flat instances query (optional — blocked on some servers) |
| `GET /studies/{uid}/series/{uid}/metadata` | WADO-RS | Series-level metadata |
| `GET /studies/{uid}/series/{uid}/instances/{uid}/metadata` | WADO-RS | Instance-level metadata |
| `GET /studies/{uid}/series/{uid}/instances/{uid}` | WADO-RS | Full DICOM file retrieval |
| `GET /studies/{uid}/series/{uid}/instances/{uid}/frames/1` | WADO-RS | Frame retrieval (S3 Static WADO style) |
| `GET /studies/{uid}/series/{uid}/instances/{uid}/rendered` | WADO-RS | Rendered image |

**Example query (Databricks SQL):**
```sql
SELECT endpoint, supported, status_code, latency_ms, notes
FROM my_catalog.my_schema.diagnostics
ORDER BY category, endpoint;
```

---

## Incremental Strategy

```
cursor = last StudyDate ingested (default: "19000101")
each run:
    effective_start = cursor - lookback_days
    date_range = f"{effective_start}-{today}"

    # studies — flat pagination
    QIDO-RS GET /studies?StudyDate={date_range}&limit={page_size}&offset={n}

    # series — hierarchical per study
    for each study:
        QIDO-RS GET /studies/{study_uid}/series

    # instances — hierarchical per study → series
    for each study:
        for each series:
            QIDO-RS GET /studies/{study_uid}/series/{series_uid}/instances
            if fetch_metadata:
                WADO-RS GET /studies/{study_uid}/series/{series_uid}/metadata
            if fetch_dicom_files:
                WADO-RS GET .../instances/{sop_uid}          (wado_mode=full)
                WADO-RS GET .../instances/{sop_uid}/frames/1 (wado_mode=frames)

    → next cursor = today
```

Offset format stored by Lakeflow:
```json
{"study_date": "20231215", "page_offset": 0}
```

---

## Development

```bash
# Clone
git clone https://github.com/databrickslabs/lakeflow-dicomweb-connector
cd lakeflow-dicomweb-connector

# Create venv
python -m venv .venv && source .venv/bin/activate

# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run against the public Orthanc demo (integration test)
python - <<'EOF'
import sys; sys.path.insert(0, "src")
from databricks.labs.community_connector.sources.dicomweb import DICOMwebLakeflowConnect

c = DICOMwebLakeflowConnect({"base_url": "https://orthanc.uclouvain.be/demo/dicom-web"})
records, offset = c.read_table("studies", {}, {"page_size": "10"})
for r in records:
    print(r.get("StudyInstanceUID"), r.get("StudyDate"), r.get("PatientName"))
print("Next offset:", offset)
EOF
```

---

## Project Structure

```
src/databricks/labs/community_connector/
  interface/
    lakeflow_connect.py        LakeflowConnect ABC (vendored from official framework)
  libs/
    spec_parser.py             Pipeline spec parser (vendored)
    utils.py                   PySpark type conversion utilities (vendored)
  pipeline/
    ingestion_pipeline.py      ingest() entry point (vendored)
  sparkpds/
    registry.py                register() function (vendored)
    lakeflow_datasource.py     Spark Python DataSource wrappers (vendored)
  sources/dicomweb/
    __init__.py
    dicomweb.py                Main connector class
    dicomweb_client.py         HTTP client (QIDO-RS + WADO-RS)
    dicomweb_schemas.py        PySpark StructType definitions
    dicomweb_parser.py         DICOM JSON tag → Python dict parser
    connector_spec.yaml        Connector parameter specification
    _generated_dicomweb_python_source.py  Self-contained merged file for register()
notebooks/
  dicomweb_pipeline.py         Example ingestion notebook
tests/
  conftest.py
  test_dicomweb_client.py
  test_dicomweb_connector.py
  fixtures/
    studies.json
    series.json
    instances.json
```

---

## Roadmap

- **Phase 2**: Orthanc native REST API backend (`backend=orthanc`)
- **Phase 3**: dcm4chee + Keycloak OAuth2 (`auth_type=keycloak`)
- **Phase 4**: DICOM SR (Structured Report) parsing
- **Phase 5**: Pixel data extraction pipeline (PNG/JPEG from WADO-RS)

---

## License

[Databricks License](LICENSE) — same license as the official [lakeflow-community-connectors](https://github.com/databrickslabs/lakeflow-community-connectors) repository. Use is permitted solely in connection with the Databricks Services.
