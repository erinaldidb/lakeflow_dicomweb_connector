# pylint: disable=no-member
import json
from dataclasses import dataclass
from typing import List

from pyspark import pipelines as sdp
from pyspark.sql.functions import col, expr

from databricks.labs.community_connector.libs.spec_parser import SpecParser


@dataclass
class SdpTableConfig:  # pylint: disable=too-many-instance-attributes
    source_table: str
    destination_table: str
    view_name: str
    table_config: dict[str, str]
    primary_keys: List[str]
    sequence_by: str
    scd_type: str
    with_deletes: bool = False
    # Optional per-column SQL expressions applied after .load() and before the
    # SCD merge.  Keys are column names; values are SQL expressions whose result
    # replaces that column, e.g. {"metadata": "parse_json(metadata)"}.
    column_expressions: dict = None


def _build_view_name(source_table: str, flow_type: str) -> str:
    return f"source_{source_table}_{flow_type}"


def _apply_column_expressions(df, column_expressions: dict):
    """Apply per-column SQL expressions to a DataFrame (e.g. parse_json(metadata)).

    Each key in column_expressions is a column name; each value is a SQL
    expression whose result replaces that column in the same position.
    """
    if not column_expressions:
        return df
    exprs = [f"{column_expressions[c]} AS {c}" if c in column_expressions else c for c in df.columns]
    return df.selectExpr(*exprs)


def _create_cdc_table(spark, connection_name: str, config: SdpTableConfig) -> None:
    @sdp.view(name=config.view_name)
    def v():
        df = (
            spark.readStream.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", config.source_table)
            .options(**config.table_config)
            .load()
        )
        return _apply_column_expressions(df, config.column_expressions)

    sdp.create_streaming_table(name=config.destination_table)
    sdp.apply_changes(
        target=config.destination_table,
        source=config.view_name,
        keys=config.primary_keys,
        sequence_by=col(config.sequence_by),
        stored_as_scd_type=config.scd_type,
    )

    if config.with_deletes:
        delete_view_name = _build_view_name(config.source_table, "delete")

        @sdp.view(name=delete_view_name)
        def delete_view():
            return (
                spark.readStream.format("lakeflow_connect")
                .option("databricks.connection", connection_name)
                .option("tableName", config.source_table)
                .option("isDeleteFlow", "true")
                .options(**config.table_config)
                .load()
            )

        sdp.apply_changes(
            target=config.destination_table,
            source=delete_view_name,
            keys=config.primary_keys,
            sequence_by=col(config.sequence_by),
            stored_as_scd_type=config.scd_type,
            apply_as_deletes=expr("true"),
            name=delete_view_name + "_flow",
        )


def _create_snapshot_table(spark, connection_name: str, config: SdpTableConfig) -> None:
    @sdp.view(name=config.view_name)
    def snapshot_view():
        df = (
            spark.read.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", config.source_table)
            .options(**config.table_config)
            .load()
        )
        return _apply_column_expressions(df, config.column_expressions)

    sdp.create_streaming_table(name=config.destination_table)
    sdp.apply_changes_from_snapshot(
        target=config.destination_table,
        source=config.view_name,
        keys=config.primary_keys,
        stored_as_scd_type=config.scd_type,
    )


def _create_append_table(spark, connection_name: str, config: SdpTableConfig) -> None:
    @sdp.view(name=config.view_name)
    def v():
        df = (
            spark.readStream.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", config.source_table)
            .options(**config.table_config)
            .load()
        )
        return _apply_column_expressions(df, config.column_expressions)

    sdp.create_streaming_table(name=config.destination_table)

    @sdp.append_flow(name=config.view_name + "_flow", target=config.destination_table)
    def af():
        return spark.readStream.table(config.view_name)


def _get_table_metadata(spark, connection_name: str, table_list: list[str], table_configs: dict) -> dict:
    df = (
        spark.read.format("lakeflow_connect")
        .option("databricks.connection", connection_name)
        .option("tableName", "_lakeflow_metadata")
        .option("tableNameList", ",".join(table_list))
        .option("tableConfigs", json.dumps(table_configs))
        .load()
    )
    metadata = {}
    for row in df.collect():
        table_metadata = {}
        if row["primary_keys"] is not None:
            table_metadata["primary_keys"] = row["primary_keys"]
        if row["cursor_field"] is not None:
            table_metadata["cursor_field"] = row["cursor_field"]
        if row["ingestion_type"] is not None:
            table_metadata["ingestion_type"] = row["ingestion_type"]
        col_exprs = row.asDict().get("column_expressions")
        if col_exprs is not None:
            table_metadata["column_expressions"] = dict(col_exprs)
        metadata[row["tableName"]] = table_metadata
    return metadata


def ingest(spark, pipeline_spec: dict) -> None:
    """Ingest a list of tables."""
    spec = SpecParser(pipeline_spec)
    connection_name = spec.connection_name()
    table_list = spec.get_table_list()
    table_configs = spec.get_table_configurations()
    metadata = _get_table_metadata(spark, connection_name, table_list, table_configs)

    def _ingest_table(table: str) -> None:
        primary_keys = metadata[table].get("primary_keys")
        cursor_field = metadata[table].get("cursor_field")
        ingestion_type = metadata[table].get("ingestion_type", "cdc")
        table_config = spec.get_table_configuration(table)
        destination_table = spec.get_full_destination_table_name(table)

        primary_keys = spec.get_primary_keys(table) or primary_keys
        sequence_by = spec.get_sequence_by(table) or cursor_field
        scd_type_raw = spec.get_scd_type(table)
        if scd_type_raw == "APPEND_ONLY":
            ingestion_type = "append"
        scd_type = "2" if scd_type_raw == "SCD_TYPE_2" else "1"

        flow_type_map = {
            "cdc": "upsert",
            "cdc_with_deletes": "upsert",
            "snapshot": "snapshot",
            "append": "append",
        }
        view_name = _build_view_name(table, flow_type_map.get(ingestion_type, "upsert"))

        config = SdpTableConfig(
            source_table=table,
            destination_table=destination_table,
            view_name=view_name,
            table_config=table_config,
            primary_keys=primary_keys,
            sequence_by=sequence_by,
            scd_type=scd_type,
            with_deletes=(ingestion_type == "cdc_with_deletes"),
            column_expressions=metadata[table].get("column_expressions"),
        )

        if ingestion_type in ("cdc", "cdc_with_deletes"):
            _create_cdc_table(spark, connection_name, config)
        elif ingestion_type == "snapshot":
            _create_snapshot_table(spark, connection_name, config)
        elif ingestion_type == "append":
            _create_append_table(spark, connection_name, config)

    for table_name in table_list:
        _ingest_table(table_name)
