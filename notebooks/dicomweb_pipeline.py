# Databricks notebook source
# MAGIC %md
# MAGIC # DICOMweb Lakeflow Connector — Example Ingestion Notebook
# MAGIC
# MAGIC This notebook demonstrates how to register and run the DICOMweb Lakeflow connector
# MAGIC against a DICOMweb-compliant VNA or PACS system.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Install the connector package:
# MAGIC   ```
# MAGIC   %pip install git+https://github.com/databrickslabs/lakeflow-dicomweb-connector.git
# MAGIC   ```
# MAGIC - (Optional) A running DICOMweb server; the public Orthanc demo is used below for testing.

# COMMAND ----------

# MAGIC %pip install --quiet requests pyspark

# COMMAND ----------

# Configuration — edit these values for your environment
BASE_URL = "https://orthanc.uclouvain.be/demo/dicom-web"   # Public Orthanc demo
AUTH_TYPE = "none"
TARGET_CATALOG = "main"
TARGET_SCHEMA = "dicom_staging"
DICOM_VOLUME_PATH = f"/Volumes/{TARGET_CATALOG}/{TARGET_SCHEMA}/dicom_files"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Instantiate the connector

# COMMAND ----------

import sys
sys.path.insert(0, "../src")  # local dev; remove when installed as package

from databricks.labs.community_connector.sources.dicomweb import DICOMwebLakeflowConnect

connector = DICOMwebLakeflowConnect({
    "base_url": BASE_URL,
    "auth_type": AUTH_TYPE,
    # "username": dbutils.secrets.get("scope", "dicom-username"),
    # "password": dbutils.secrets.get("scope", "dicom-password"),
    # "token":    dbutils.secrets.get("scope", "dicom-token"),
})

print("Tables exposed:", connector.list_tables())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Inspect schemas

# COMMAND ----------

for table in connector.list_tables():
    schema = connector.get_table_schema(table, {})
    print(f"\n=== {table} ===")
    schema.printTreeString()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read studies incrementally (first run — no prior offset)

# COMMAND ----------

from datetime import date

# start_offset={} means "start from DEFAULT_START_DATE (beginning of time)"
# In production Lakeflow handles this automatically; here we call read_table directly.
records_iter, next_offset = connector.read_table(
    table_name="studies",
    start_offset={},
    table_options={
        "lookback_days": "1",
        "page_size": "100",
        # Use a recent start date to avoid downloading everything from the demo
        "start_date": "20230101",
    },
)

records = list(records_iter)
print(f"Fetched {len(records)} studies")
print("Next offset:", next_offset)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Convert to Spark DataFrame and write to Delta

# COMMAND ----------

if records:
    from databricks.labs.community_connector.sources.dicomweb.dicomweb_schemas import STUDIES_SCHEMA

    df = spark.createDataFrame(records, schema=STUDIES_SCHEMA)
    df.printSchema()
    display(df)

    # Write to Delta (merge-on-read CDC pattern)
    df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.studies")

    print(f"Written {df.count()} rows to {TARGET_CATALOG}.{TARGET_SCHEMA}.studies")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Read series

# COMMAND ----------

records_iter, next_offset = connector.read_table(
    table_name="series",
    start_offset={},
    table_options={"page_size": "100"},
)

series_records = list(records_iter)
print(f"Fetched {len(series_records)} series")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Read instances with optional DICOM file retrieval
# MAGIC
# MAGIC Set `fetch_dicom_files=true` to also retrieve `.dcm` files via WADO-RS and write them
# MAGIC to the Unity Catalog Volume at `dicom_volume_path`.

# COMMAND ----------

# Uncomment to enable full DICOM file retrieval:
# records_iter, next_offset = connector.read_table(
#     table_name="instances",
#     start_offset={},
#     table_options={
#         "fetch_dicom_files": "true",
#         "dicom_volume_path": DICOM_VOLUME_PATH,
#         "page_size": "50",
#     },
# )

records_iter, next_offset = connector.read_table(
    table_name="instances",
    start_offset={},
    table_options={"page_size": "100"},
)

instance_records = list(records_iter)
print(f"Fetched {len(instance_records)} instances")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Incremental run (resume from saved offset)
# MAGIC
# MAGIC In production, Lakeflow persists `next_offset` automatically between pipeline runs.
# MAGIC You can simulate this manually:

# COMMAND ----------

saved_offset = next_offset   # from a previous run
print("Resuming from offset:", saved_offset)

records_iter, next_offset = connector.read_table(
    table_name="studies",
    start_offset=saved_offset,
    table_options={"lookback_days": "1"},
)
incremental_records = list(records_iter)
print(f"Incremental run fetched {len(incremental_records)} new/updated studies")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Register with Lakeflow pipeline (production pattern)
# MAGIC
# MAGIC When the Lakeflow SDK is available in the cluster, use `register` + `ingest`:
# MAGIC
# MAGIC ```python
# MAGIC from databricks.labs.lakeflow.connect import register, ingest
# MAGIC
# MAGIC register(spark, DICOMwebLakeflowConnect, connection_options={
# MAGIC     "base_url": BASE_URL,
# MAGIC     "auth_type": AUTH_TYPE,
# MAGIC })
# MAGIC
# MAGIC pipeline_spec = {
# MAGIC     "target_catalog": TARGET_CATALOG,
# MAGIC     "target_schema": TARGET_SCHEMA,
# MAGIC     "tables": {
# MAGIC         "studies":   {},
# MAGIC         "series":    {},
# MAGIC         "instances": {
# MAGIC             "fetch_dicom_files": "true",
# MAGIC             "dicom_volume_path": DICOM_VOLUME_PATH,
# MAGIC         },
# MAGIC     },
# MAGIC }
# MAGIC ingest(spark, pipeline_spec)
# MAGIC ```
