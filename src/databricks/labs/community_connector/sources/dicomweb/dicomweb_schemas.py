"""
PySpark StructType schema definitions for the three DICOM hierarchy levels.

These schemas align with the DICOM Information Object hierarchy:
    Study → Series → Instance (SOP)
"""

from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Study-level schema
# QIDO-RS /studies
# ---------------------------------------------------------------------------

STUDIES_SCHEMA = StructType(
    [
        StructField("StudyInstanceUID", StringType(), nullable=False),
        StructField("PatientID", StringType(), nullable=True),
        StructField("PatientName", StringType(), nullable=True),
        StructField("StudyDate", StringType(), nullable=True),
        StructField("StudyTime", StringType(), nullable=True),
        StructField("AccessionNumber", StringType(), nullable=True),
        StructField("StudyDescription", StringType(), nullable=True),
        # CS VR — can contain multiple modalities, e.g. ["CT", "SR"]
        StructField("ModalitiesInStudy", ArrayType(StringType()), nullable=True),
        StructField("NumberOfStudyRelatedSeries", IntegerType(), nullable=True),
        StructField("NumberOfStudyRelatedInstances", IntegerType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Series-level schema
# QIDO-RS /series
# ---------------------------------------------------------------------------

SERIES_SCHEMA = StructType(
    [
        StructField("SeriesInstanceUID", StringType(), nullable=False),
        StructField("StudyInstanceUID", StringType(), nullable=True),
        StructField("SeriesNumber", IntegerType(), nullable=True),
        StructField("SeriesDescription", StringType(), nullable=True),
        StructField("Modality", StringType(), nullable=True),
        StructField("BodyPartExamined", StringType(), nullable=True),
        StructField("SeriesDate", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Instance (SOP) level schema
# QIDO-RS /instances
# ---------------------------------------------------------------------------

INSTANCES_SCHEMA = StructType(
    [
        StructField("SOPInstanceUID", StringType(), nullable=False),
        StructField("SeriesInstanceUID", StringType(), nullable=True),
        StructField("StudyInstanceUID", StringType(), nullable=True),
        StructField("SOPClassUID", StringType(), nullable=True),
        StructField("InstanceNumber", IntegerType(), nullable=True),
        StructField("ContentDate", StringType(), nullable=True),
        StructField("ContentTime", StringType(), nullable=True),
        # Populated when fetch_dicom_files=true; path inside Unity Catalog Volume
        StructField("dicom_file_path", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------

TABLE_SCHEMAS: dict[str, StructType] = {
    "studies": STUDIES_SCHEMA,
    "series": SERIES_SCHEMA,
    "instances": INSTANCES_SCHEMA,
}


def get_schema(table_name: str) -> StructType:
    """Return the PySpark StructType for the given table name."""
    try:
        return TABLE_SCHEMAS[table_name]
    except KeyError:
        raise ValueError(f"Unknown table '{table_name}'. Valid tables: {list(TABLE_SCHEMAS)}")
