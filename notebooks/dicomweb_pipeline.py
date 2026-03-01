# Databricks notebook source
# MAGIC %md
# MAGIC # DICOMweb Lakeflow Connector — Ingestion Pipeline
# MAGIC
# MAGIC Incrementally ingests DICOM **studies**, **series**, and **instances** metadata
# MAGIC from a DICOMweb-compliant VNA/PACS into Unity Catalog Delta tables.
# MAGIC
# MAGIC **Prerequisites**
# MAGIC 1. Install this connector on the cluster (or as a cluster-scoped library):
# MAGIC    ```
# MAGIC    %pip install git+https://github.com/erinaldidb/lakeflow_dicomweb_connector.git
# MAGIC    ```
# MAGIC 2. Create a Unity Catalog **connection** pointing at your DICOMweb endpoint
# MAGIC    (see the cell below).
# MAGIC 3. (Optional) Create a Unity Catalog **Volume** to store raw `.dcm` files.

# COMMAND ----------

# MAGIC %pip install --quiet git+https://github.com/erinaldidb/lakeflow_dicomweb_connector.git

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the Unity Catalog connection
# MAGIC
# MAGIC Run this **once** to register your DICOMweb endpoint as a UC connection.
# MAGIC Skip if the connection `dicomweb-fevm` already exists.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE CONNECTION `dicomweb-fevm`
# MAGIC TYPE dicomweb
# MAGIC OPTIONS (
# MAGIC   base_url  'https://your-pacs.example.com/dicom-web',
# MAGIC   auth_type 'none'          -- or 'basic' / 'bearer'
# MAGIC   -- username 'svc-account',
# MAGIC   -- password secret('my-scope', 'dicom-password'),
# MAGIC   -- token    secret('my-scope', 'dicom-token')
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configure the ingestion pipeline

# COMMAND ----------

from databricks.labs.community_connector import register
from databricks.labs.community_connector.pipeline import ingest

# Required: allows the connector to pick up credentials from the UC connection
spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")

source_name = "dicomweb"

# ---------------------------------------------------------------------------
# Destination — edit to match your Unity Catalog layout
# ---------------------------------------------------------------------------
DESTINATION_CATALOG = "main"
DESTINATION_SCHEMA = "dicom_bronze"

# Optional: Unity Catalog Volume path for raw .dcm file storage.
# Only used when fetch_dicom_files is enabled on the instances table.
DICOM_VOLUME_PATH = f"/Volumes/{DESTINATION_CATALOG}/{DESTINATION_SCHEMA}/dicom_files"

# ---------------------------------------------------------------------------
# Pipeline specification
#
# pipeline_spec
# ├── connection_name        UC connection created above
# └── objects[]
#     └── table
#         ├── source_table             Table name in the source system
#         ├── destination_catalog      Target catalog  (defaults to pipeline default)
#         ├── destination_schema       Target schema   (defaults to pipeline default)
#         ├── destination_table        Target table    (defaults to source_table)
#         └── table_configuration
#             ├── scd_type             SCD_TYPE_1 (upsert) | SCD_TYPE_2 | APPEND_ONLY
#             ├── primary_keys         Override connector default primary key(s)
#             └── <connector options>  Any option from connector_spec.yaml allowlist:
#                                      lookback_days, page_size, start_date,
#                                      fetch_dicom_files, dicom_volume_path
# ---------------------------------------------------------------------------

pipeline_spec = {
    "connection_name": "dicomweb-fevm",
    "objects": [
        # ------------------------------------------------------------------
        # studies — study-level metadata (one row per DICOM study)
        # Primary key: StudyInstanceUID
        # ------------------------------------------------------------------
        {
            "table": {
                "source_table": "studies",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": "studies",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1",
                    "primary_keys": ["StudyInstanceUID"],
                    # Fetch studies updated in the last 1 day on each run
                    "lookback_days": "1",
                    "page_size": "200",
                },
            }
        },
        # ------------------------------------------------------------------
        # series — series-level metadata (one row per DICOM series)
        # Primary key: SeriesInstanceUID
        # ------------------------------------------------------------------
        {
            "table": {
                "source_table": "series",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": "series",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1",
                    "primary_keys": ["SeriesInstanceUID"],
                    "lookback_days": "1",
                    "page_size": "200",
                },
            }
        },
        # ------------------------------------------------------------------
        # instances — SOP instance metadata (one row per DICOM file)
        # Primary key: SOPInstanceUID
        #
        # Set fetch_dicom_files to "true" to also retrieve the raw .dcm file
        # via WADO-RS and write it to the Volume at dicom_volume_path.
        # Remove or set to "false" to ingest metadata only.
        # ------------------------------------------------------------------
        {
            "table": {
                "source_table": "instances",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": "instances",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1",
                    "primary_keys": ["SOPInstanceUID"],
                    "lookback_days": "1",
                    "page_size": "200",
                    # -- WADO-RS file retrieval (optional) --
                    # "fetch_dicom_files":  "true",
                    # "dicom_volume_path":  DICOM_VOLUME_PATH,
                },
            }
        },
    ],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Register the connector and run ingestion

# COMMAND ----------

# Register the DICOMweb connector with the Lakeflow framework
register(spark, source_name)

# Run the incremental ingestion pipeline.
# On first run: fetches all available data from start_date (default: all history).
# On subsequent runs: resumes from the last StudyDate cursor stored by Lakeflow.
ingest(spark, pipeline_spec)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create presentation views
# MAGIC
# MAGIC Thin views over the raw Delta tables that expose user-friendly column names.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.instances_view AS
SELECT
  SOPInstanceUID  AS sop_instance_uid,
  SeriesInstanceUID AS series_instance_uid,
  StudyInstanceUID AS study_instance_uid,
  SOPClassUID     AS sop_class_uid,
  InstanceNumber  AS instance_number,
  StudyDate       AS study_date,
  ContentDate     AS content_date,
  ContentTime     AS content_time,
  dicom_file_path AS local_path,
  metadata        AS meta,
  connection_name
FROM {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.instances
""")
