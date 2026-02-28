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
- **Three tables**: `studies`, `series`, `instances`
- **Optional WADO-RS** file retrieval — stores `.dcm` files in a Unity Catalog Volume
- **Authentication**: `none`, `basic`, `bearer`
- **Pagination**: configurable page size with automatic looping
- **Lookback window**: catch late-arriving studies with `lookback_days`

## Supported Systems

| System | Backend | Status |
|--------|---------|--------|
| Any DICOMweb-compliant server | `dicomweb` (default) | Phase 1 — available |
| [Orthanc](https://www.orthanc-server.com/) | `orthanc` native API | Phase 2 — planned |
| [dcm4chee](https://www.dcm4che.org/) | `dcm4chee` + Keycloak OAuth2 | Phase 3 — planned |

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

## Table Options (per-table in pipeline spec)

> **Important:** every option listed below must be included in the `externalOptionsAllowList` value of your Unity Catalog connection, otherwise Lakeflow rejects it at runtime with *"Option X is not allowed by connection Y and cannot be provided externally."* Use the full allowlist value: `fetch_dicom_files,dicom_volume_path,lookback_days,page_size,start_date,download_threads,max_concurrent_requests`

| Option | Default | Description |
|--------|---------|-------------|
| `page_size` | `100` | Records per QIDO-RS request |
| `lookback_days` | `1` | Days to subtract from cursor to catch late arrivals |
| `start_date` | `19000101` | Initial cursor date (first run only) |
| `fetch_dicom_files` | `false` | Also retrieve `.dcm` files via WADO-RS |
| `dicom_volume_path` | — | Required when `fetch_dicom_files=true`; Unity Catalog Volume path |
| `max_concurrent_requests` | `16` | Max simultaneous WADO-RS connections to the PACS. Instances are grouped into this many Spark tasks so the cluster never opens more than this many connections at once. Reduce for sensitive PACS systems. |
| `download_threads` | `8` | Thread-level parallelism within a page when running outside Spark (standalone use). |

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
| `dicom_file_path` | string (nullable) | — | Path to `.dcm` in Volume |

---

## Incremental Strategy

```
cursor = last StudyDate ingested (default: "19000101")
each run:
    effective_start = cursor - lookback_days
    date_range = f"{effective_start}-{today}"
    QIDO-RS GET /studies?StudyDate={date_range}&limit={page_size}&offset={n}
    → loop pages until empty
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
