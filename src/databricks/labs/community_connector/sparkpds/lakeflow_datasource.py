import json
from typing import Iterator

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    SimpleDataSourceStreamReader,
)
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.libs.utils import parse_value

# fmt: off
LakeflowConnectImpl = LakeflowConnect  # __LAKEFLOW_CONNECT_IMPL__
# fmt: on

METADATA_TABLE = "_lakeflow_metadata"
TABLE_NAME = "tableName"
TABLE_NAME_LIST = "tableNameList"
TABLE_CONFIGS = "tableConfigs"
IS_DELETE_FLOW = "isDeleteFlow"


class LakeflowStreamReader(SimpleDataSourceStreamReader):
    def __init__(self, options: dict[str, str], schema: StructType, lakeflow_connect: LakeflowConnect):
        self.options = options
        self.lakeflow_connect = lakeflow_connect
        self.schema = schema

    def initialOffset(self):
        return {}

    def read(self, start: dict) -> (Iterator[tuple], dict):
        is_delete_flow = self.options.get(IS_DELETE_FLOW) == "true"
        table_options = {k: v for k, v in self.options.items() if k != IS_DELETE_FLOW}
        if is_delete_flow:
            records, offset = self.lakeflow_connect.read_table_deletes(self.options[TABLE_NAME], start, table_options)
        else:
            records, offset = self.lakeflow_connect.read_table(self.options[TABLE_NAME], start, table_options)
        rows = map(lambda x: parse_value(x, self.schema), records)
        return rows, offset

    def readBetweenOffsets(self, start: dict, end: dict) -> Iterator:
        return self.read(start)[0]


class LakeflowBatchReader(DataSourceReader):
    def __init__(self, options: dict[str, str], schema: StructType, lakeflow_connect: LakeflowConnect):
        self.options = options
        self.schema = schema
        self.lakeflow_connect = lakeflow_connect
        self.table_name = options[TABLE_NAME]

    def read(self, partition):
        if self.table_name == METADATA_TABLE:
            all_records = self._read_table_metadata()
        else:
            all_records, _ = self.lakeflow_connect.read_table(self.table_name, None, self.options)
        return iter(map(lambda x: parse_value(x, self.schema), all_records))

    def _read_table_metadata(self):
        table_names = [o.strip() for o in self.options.get(TABLE_NAME_LIST, "").split(",") if o.strip()]
        table_configs = json.loads(self.options.get(TABLE_CONFIGS, "{}"))
        return [
            {TABLE_NAME: t, **self.lakeflow_connect.read_table_metadata(t, table_configs.get(t, {}))}
            for t in table_names
        ]


class LakeflowSource(DataSource):
    def __init__(self, options):
        self.options = options
        self.lakeflow_connect = LakeflowConnectImpl(options)  # pylint: disable=abstract-class-instantiated

    @classmethod
    def name(cls):
        return "lakeflow_connect"

    def schema(self):
        table = self.options[TABLE_NAME]
        if table == METADATA_TABLE:
            return StructType(
                [
                    StructField(TABLE_NAME, StringType(), False),
                    StructField("primary_keys", ArrayType(StringType()), True),
                    StructField("cursor_field", StringType(), True),
                    StructField("ingestion_type", StringType(), True),
                ]
            )
        return self.lakeflow_connect.get_table_schema(table, self.options)

    def reader(self, schema: StructType):
        return LakeflowBatchReader(self.options, schema, self.lakeflow_connect)

    def simpleStreamReader(self, schema: StructType):
        return LakeflowStreamReader(self.options, schema, self.lakeflow_connect)
