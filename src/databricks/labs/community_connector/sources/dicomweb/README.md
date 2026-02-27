# Lakeflow DICOMweb Community Connector

## Authors

- emanuele.rinaldi@databricks.com

This documentation provides setup instructions and reference information for the DICOMweb source connector, which ingests DICOM study, series, and instance metadata from any DICOMweb-compliant VNA or PACS system.

## Prerequisites

- A DICOMweb-compliant VNA or PACS system (e.g., Orthanc, dcm4chee, Sectra, Agfa Enterprise Imaging)
- Network access from your Databricks cluster to the DICOMweb endpoint
- Credentials if your system requires authentication (Basic or Bearer token)
- (Optional) A Unity Catalog Volume if you want to retrieve raw `.dcm` files via WADO-RS

## Local Development Setup

For local testing and development:

1. **Clone the repository and install dependencies**:
   ```bash
   cd ~/code/lakeflow-dicomweb-connector
   python -m venv .venv && source .venv/bin/activate
   pip install -e ".[dev]"
   ```

2. **Run the unit test suite** (no network required — all HTTP is mocked):
   ```bash
   pytest tests/ -v
   ```

3. **Run a quick integration test** against the public Orthanc demo:
   ```bash
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

**Security Note**: Never commit files containing real endpoint URLs or credentials. Use Databricks Secrets or Unity Catalog connection parameters to manage sensitive values.

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters when creating your Unity Catalog connection:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `base_url` | string | Yes | Base URL of the DICOMweb endpoint. Must include the path prefix if any (e.g., `/dicom-web`). | `https://pacs.example.com/dicom-web` |
| `auth_type` | string | No | Authentication method. One of: `none`, `basic`, `bearer`. Defaults to `none`. | `basic` |
| `username` | string | No | Username for Basic authentication. Required when `auth_type=basic`. | `svc-dicom` |
| `password` | string | No | Password for Basic authentication. Use a Databricks Secret. Required when `auth_type=basic`. | `secret('scope','dicom-password')` |
| `token` | string | No | Bearer token for token-based authentication. Use a Databricks Secret. Required when `auth_type=bearer`. | `secret('scope','dicom-token')` |

### Obtaining Your Endpoint URL and Credentials

**Orthanc:**
1. Open the Orthanc Explorer UI (usually at `http://host:8042`)
2. The DICOMweb base URL is `http://host:8042/dicom-web`
3. If REST API authentication is enabled, use `auth_type=basic` with the configured Orthanc credentials

**dcm4chee:**
1. The DICOMweb base URL is typically `https://host:8443/dcm4chee-arc/aets/DCM4CHEE/rs`
2. dcm4chee uses Keycloak for auth — obtain a long-lived token or service account credentials from your Keycloak admin

**Commercial VNA/PACS (Sectra, Agfa, Philips PACS, etc.):**
1. Contact your PACS administrator for the DICOMweb (QIDO-RS/WADO-RS) endpoint URL
2. Request a service account with read-only access to studies, series, and instances
3. Confirm that QIDO-RS queries support `StudyDate` as a filter parameter

### Create a Unity Catalog Connection

A Unity Catalog connection can be created using SQL:

```sql
CREATE CONNECTION `dicomweb-fevm`
TYPE dicomweb
OPTIONS (
  base_url  'https://your-pacs.example.com/dicom-web',
  auth_type 'none'
  -- For Basic auth:
  -- auth_type 'basic',
  -- username  'svc-dicom',
  -- password  secret('my-scope', 'dicom-password')
  -- For Bearer token:
  -- auth_type 'bearer',
  -- token     secret('my-scope', 'dicom-token')
);
```

Or follow the Lakeflow Community Connector UI flow from the Databricks **Add Data** page and select **DICOMweb VNA/PACS** from the connector list.

## Supported Objects

The DICOMweb connector exposes the following tables, corresponding to the three levels of the DICOM information hierarchy:

### `studies`
- **Description**: Study-level metadata. One row per DICOM study (a patient visit or imaging session).
- **Primary Key**: `StudyInstanceUID`
- **Cursor Field**: `StudyDate`
- **Ingestion Type**: CDC (Change Data Capture)
- **Source endpoint**: `QIDO-RS GET /studies?StudyDate={range}`

| Column | Type | DICOM Tag | Description |
|--------|------|-----------|-------------|
| `StudyInstanceUID` | STRING (PK) | 0020000D | Globally unique study identifier |
| `PatientID` | STRING | 00100020 | Patient identifier in the PACS |
| `PatientName` | STRING | 00100010 | Patient name (Alphabetic component) |
| `StudyDate` | STRING | 00080020 | Study date `YYYYMMDD` |
| `StudyTime` | STRING | 00080030 | Study time `HHMMSS` |
| `AccessionNumber` | STRING | 00080050 | RIS/HIS accession number |
| `StudyDescription` | STRING | 00081030 | Free-text study description |
| `ModalitiesInStudy` | ARRAY\<STRING\> | 00080061 | Modalities present (e.g., `["CT","SR"]`) |
| `NumberOfStudyRelatedSeries` | INT | 00201206 | Number of series in this study |
| `NumberOfStudyRelatedInstances` | INT | 00201208 | Total number of DICOM instances |

### `series`
- **Description**: Series-level metadata. One row per DICOM series within a study.
- **Primary Key**: `SeriesInstanceUID`
- **Cursor Field**: `StudyDate`
- **Ingestion Type**: CDC
- **Source endpoint**: `QIDO-RS GET /series?StudyDate={range}`

| Column | Type | DICOM Tag | Description |
|--------|------|-----------|-------------|
| `SeriesInstanceUID` | STRING (PK) | 0020000E | Globally unique series identifier |
| `StudyInstanceUID` | STRING | 0020000D | Parent study UID |
| `SeriesNumber` | INT | 00200011 | Series number within the study |
| `SeriesDescription` | STRING | 0008103E | Free-text series description |
| `Modality` | STRING | 00080060 | Imaging modality (CT, MR, PT, …) |
| `BodyPartExamined` | STRING | 00180015 | Body part (CHEST, BRAIN, …) |
| `SeriesDate` | STRING | 00080021 | Series acquisition date `YYYYMMDD` |

### `instances`
- **Description**: SOP instance (image) metadata. One row per DICOM file.
- **Primary Key**: `SOPInstanceUID`
- **Cursor Field**: `StudyDate`
- **Ingestion Type**: CDC
- **Source endpoint**: `QIDO-RS GET /instances?StudyDate={range}`

| Column | Type | DICOM Tag | Description |
|--------|------|-----------|-------------|
| `SOPInstanceUID` | STRING (PK) | 00080018 | Globally unique SOP instance UID |
| `SeriesInstanceUID` | STRING | 0020000E | Parent series UID |
| `StudyInstanceUID` | STRING | 0020000D | Parent study UID |
| `SOPClassUID` | STRING | 00080016 | SOP class (storage type) UID |
| `InstanceNumber` | INT | 00200013 | Instance number within the series |
| `ContentDate` | STRING | 00080023 | Content creation date `YYYYMMDD` |
| `ContentTime` | STRING | 00080033 | Content creation time `HHMMSS` |
| `dicom_file_path` | STRING | — | Path to `.dcm` file in UC Volume (populated when `fetch_dicom_files=true`) |

## Data Type Mapping

| DICOM VR | VR Name | Databricks Type | Notes |
|----------|---------|-----------------|-------|
| `DA` | Date | STRING | Format `YYYYMMDD` |
| `TM` | Time | STRING | Format `HHMMSS.FFFFFF` |
| `UI` | Unique Identifier | STRING | DICOM UID dot-notation |
| `LO` | Long String | STRING | Max 64 chars |
| `SH` | Short String | STRING | Max 16 chars |
| `CS` | Code String | STRING or ARRAY\<STRING\> | Multi-valued CS (e.g., ModalitiesInStudy) stored as array |
| `PN` | Person Name | STRING | Alphabetic component extracted |
| `IS` | Integer String | INT | Parsed from string |
| `DS` | Decimal String | DOUBLE | Parsed from string |
| `OB`/`OW` | Binary data | — | Not included in metadata tables |

**Important Notes:**
- All date/time fields are stored as raw DICOM strings (`YYYYMMDD`, `HHMMSS`) — convert with `to_date(StudyDate, 'yyyyMMdd')` in SQL as needed
- Patient name is extracted from the `Alphabetic` component of the PN VR; ideographic/phonetic components are used as fallback
- Tags in QIDO-RS responses may be uppercase or lowercase hex — the connector normalises both

## How to Run

### Step 1: Install the Connector

Add the connector as a cluster library or install it at the top of your notebook:

```python
%pip install git+https://github.com/erinaldidb/lakeflow_dicomweb_connector.git
```

### Step 2: Configure Your Pipeline

Update `pipeline_spec` in your `ingest.py` notebook with your connection name, destination catalog/schema, and table-specific options:

```python
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")

pipeline_spec = {
    "connection_name": "dicomweb-fevm",
    "objects": [
        {
            "table": {
                "source_table": "studies",
                "destination_catalog": "main",
                "destination_schema":  "dicom_bronze",
                "table_configuration": {
                    "scd_type":      "SCD_TYPE_1",
                    "primary_keys":  ["StudyInstanceUID"],
                    "lookback_days": "1",
                    "page_size":     "200",
                },
            }
        },
        {
            "table": {
                "source_table": "series",
                "destination_catalog": "main",
                "destination_schema":  "dicom_bronze",
                "table_configuration": {
                    "scd_type":      "SCD_TYPE_1",
                    "primary_keys":  ["SeriesInstanceUID"],
                    "lookback_days": "1",
                    "page_size":     "200",
                },
            }
        },
        {
            "table": {
                "source_table": "instances",
                "destination_catalog": "main",
                "destination_schema":  "dicom_bronze",
                "table_configuration": {
                    "scd_type":      "SCD_TYPE_1",
                    "primary_keys":  ["SOPInstanceUID"],
                    "lookback_days": "1",
                    "page_size":     "200",
                    # Optional: retrieve raw .dcm files via WADO-RS
                    # "fetch_dicom_files": "true",
                    # "dicom_volume_path": "/Volumes/main/dicom_bronze/dicom_files",
                },
            }
        },
    ],
}

register(spark, "dicomweb")
ingest(spark, pipeline_spec)
```

**Table-Specific Options:**

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `lookback_days` | No | `1` | Days to subtract from the cursor on each run to catch late-arriving studies |
| `page_size` | No | `100` | Number of records per QIDO-RS request (increase for large PACS systems) |
| `start_date` | No | `19000101` | Initial cursor date for the very first run (`YYYYMMDD`). Use a recent date (e.g., `20240101`) to avoid full-history scans. |
| `fetch_dicom_files` | No | `false` | When `true`, retrieves each `.dcm` file via WADO-RS and writes it to `dicom_volume_path` |
| `dicom_volume_path` | No | — | Required when `fetch_dicom_files=true`. Unity Catalog Volume path where `.dcm` files are written. |

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start with `studies` only**: Run the studies table first to verify connectivity and data volume before enabling series and instances
- **Set a realistic `start_date`**: On the first run, use a recent `start_date` (e.g., 30–90 days ago) to avoid a full PACS history scan which can return millions of instances
- **Tune `page_size`**: Large PACS systems may have hundreds of thousands of instances per day — increase `page_size` to `500` or `1000` if the PACS supports it
- **Use `lookback_days`**: DICOM worklist systems often finalise studies several hours after acquisition. A `lookback_days` of `1`–`2` ensures late-finalised studies are captured
- **Enable `fetch_dicom_files` selectively**: Raw DICOM retrieval is bandwidth-intensive. Enable it only for the modalities and date ranges you need for AI/ML workflows
- **Schedule appropriately**: For near-real-time analytics, run every 15–30 minutes. For overnight batch loads, a daily schedule is sufficient
- **Monitor Volume storage**: Each `.dcm` file ranges from a few KB (reports) to several hundred MB (CT/MR volumes). Plan Volume capacity accordingly

#### Troubleshooting

**Common Issues:**

1. **HTTP 400 on WADO-RS retrieval**
   - The connector uses `Accept: multipart/related; type="application/dicom"` — this is the correct DICOMweb standard header. If your PACS returns 400, check that WADO-RS is enabled in the PACS configuration.

2. **Empty QIDO-RS results**
   - Verify the `base_url` includes the correct path prefix (e.g., `/dicom-web`, `/wado`, `/rs`)
   - Confirm the endpoint supports the `StudyDate` QIDO-RS filter parameter
   - Try accessing `{base_url}/studies` directly in a browser or with `curl` to check connectivity

3. **Authentication errors (HTTP 401 / 403)**
   - For Basic auth: verify the username/password are stored correctly in Databricks Secrets
   - For Bearer token: check the token has not expired; some PACS systems issue short-lived tokens
   - Confirm the service account has read access to studies, series, and instances

4. **Slow ingestion on first run**
   - Set `start_date` to a recent date to limit the initial scan window
   - Reduce `page_size` if the PACS is timing out on large result sets
   - Run `studies` and `series` before enabling `instances`, which is typically the largest table

5. **`dicom_file_path` is NULL despite `fetch_dicom_files=true`**
   - Check the connector logs for WADO-RS errors (the connector logs a WARNING and continues rather than failing the batch)
   - Verify the Volume path exists and the cluster's service principal has write access
   - Confirm WADO-RS is enabled on the PACS (some systems enable QIDO-RS but not WADO-RS)

6. **Duplicate records after re-run**
   - `SCD_TYPE_1` (upsert on primary key) is the recommended `scd_type` — it prevents duplicates
   - Avoid `APPEND_ONLY` unless you specifically need a full audit log

## References

- [DICOMweb Standard (DICOM PS3.18)](https://dicom.nema.org/medical/dicom/current/output/chtml/part18/PS3.18.html)
- [QIDO-RS Specification](https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_10.6.html)
- [WADO-RS Specification](https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_10.4.html)
- [Orthanc DICOMweb Plugin](https://orthanc.uclouvain.be/book/plugins/dicomweb.html)
- [dcm4chee DICOMweb Documentation](https://dcm4che.atlassian.net/wiki/spaces/d2/pages/1835038/DICOMweb)
- [Lakeflow Community Connectors](https://github.com/databrickslabs/lakeflow-community-connectors)
