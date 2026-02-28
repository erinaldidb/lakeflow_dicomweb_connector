"""Parser for LakeFlow connector pipeline specifications."""

# pylint: disable=too-few-public-methods
from typing import List, Dict, Any, Optional
import json

from pydantic import (
    BaseModel,
    ConfigDict,
    StrictStr,
    ValidationError,
    field_validator,
)

SCD_TYPE = "scd_type"
PRIMARY_KEYS = "primary_keys"
SEQUENCE_BY = "sequence_by"

SCD_TYPE_1 = "SCD_TYPE_1"
SCD_TYPE_2 = "SCD_TYPE_2"
APPEND_ONLY = "APPEND_ONLY"
VALID_SCD_TYPES = {SCD_TYPE_1, SCD_TYPE_2, APPEND_ONLY}


class TableSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_table: StrictStr
    destination_catalog: Optional[StrictStr] = None
    destination_schema: Optional[StrictStr] = None
    destination_table: Optional[StrictStr] = None
    table_configuration: Optional[Dict[str, StrictStr]] = None

    @field_validator("table_configuration", mode="before")
    @classmethod
    def normalize_table_configuration(cls, v: Optional[Dict[str, Any]]) -> Optional[Dict[str, str]]:
        if v is None:
            return None
        if not isinstance(v, dict):
            raise ValueError("'table_configuration' must be a dictionary if provided")
        normalized: Dict[str, str] = {}
        for key, value in v.items():
            str_key = str(key)
            if isinstance(value, (dict, list)):
                normalized[str_key] = json.dumps(value)
            else:
                normalized[str_key] = str(value)
        return normalized


class ObjectSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    table: TableSpec


class PipelineSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    connection_name: StrictStr
    objects: List[ObjectSpec]

    @field_validator("connection_name")
    @classmethod
    def connection_name_not_empty(cls, v: StrictStr) -> StrictStr:
        if not v.strip():
            raise ValueError("'connection_name' must be a non-empty string")
        return v

    @field_validator("objects")
    @classmethod
    def objects_must_not_be_empty(cls, v: List[ObjectSpec]) -> List[ObjectSpec]:
        if not v:
            raise ValueError("'objects' must be a non-empty list")
        return v


class SpecParser:
    def __init__(self, spec: Dict[str, Any]):
        if not isinstance(spec, dict):
            raise ValueError("Spec must be a dictionary")
        try:
            self._model = PipelineSpec(**spec)
        except ValidationError as e:
            raise ValueError(f"Invalid pipeline spec: {e}") from e

    def connection_name(self) -> str:
        return self._model.connection_name

    def get_table_list(self) -> List[str]:
        return [obj.table.source_table for obj in self._model.objects]

    def get_table_configurations(self) -> Dict[str, Dict[str, Any]]:
        return {t: self.get_table_configuration(t) for t in self.get_table_list()}

    def get_table_configuration(self, table_name: str) -> Dict[str, Any]:
        special_keys = {SCD_TYPE, PRIMARY_KEYS, SEQUENCE_BY}
        for obj in self._model.objects:
            if obj.table.source_table == table_name:
                config = obj.table.table_configuration or {}
                return {k: v for k, v in config.items() if k not in special_keys}
        return {}

    def get_scd_type(self, table_name: str) -> Optional[str]:
        for obj in self._model.objects:
            if obj.table.source_table == table_name:
                config = obj.table.table_configuration or {}
                scd_type_value = config.get(SCD_TYPE)
                if scd_type_value is None:
                    return None
                normalized = scd_type_value.upper()
                if normalized not in VALID_SCD_TYPES:
                    raise ValueError(
                        f"Invalid SCD type '{scd_type_value}' for table '{table_name}'. "
                        f"Must be one of: {', '.join(sorted(VALID_SCD_TYPES))}"
                    )
                return normalized
        return None

    def get_primary_keys(self, table_name: str) -> Optional[List[str]]:
        for obj in self._model.objects:
            if obj.table.source_table == table_name:
                config = obj.table.table_configuration or {}
                primary_keys_value = config.get(PRIMARY_KEYS)
                if primary_keys_value is None:
                    return None
                if isinstance(primary_keys_value, str) and primary_keys_value.startswith("["):
                    return json.loads(primary_keys_value)
                return [primary_keys_value] if isinstance(primary_keys_value, str) else primary_keys_value
        return None

    def get_sequence_by(self, table_name: str) -> Optional[str]:
        for obj in self._model.objects:
            if obj.table.source_table == table_name:
                config = obj.table.table_configuration or {}
                return config.get(SEQUENCE_BY)
        return None

    def get_full_destination_table_name(self, table_name: str) -> str:
        for obj in self._model.objects:
            if obj.table.source_table == table_name:
                catalog = obj.table.destination_catalog
                schema = obj.table.destination_schema
                table = obj.table.destination_table or table_name
                if catalog is None or schema is None:
                    return table
                return f"`{catalog}`.`{schema}`.`{table}`"
        raise ValueError(f"Table '{table_name}' not found in the pipeline spec")
