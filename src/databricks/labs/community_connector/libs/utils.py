"""Utility functions for data type conversion and parsing."""

import base64
from datetime import datetime
from decimal import Decimal
from typing import Any

from pyspark.sql import Row
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructType,
    TimestampType,
)


def _parse_struct(value: Any, field_type: StructType) -> Row:
    if not isinstance(value, dict):
        raise ValueError(f"Expected a dictionary for StructType, got {type(value)}")
    if value == {}:
        raise ValueError(
            "field in StructType cannot be an empty dict. Please assign None as the default value instead."
        )
    field_dict = {}
    for field in field_type.fields:
        if field.name in value:
            field_dict[field.name] = parse_value(value.get(field.name), field.dataType)
        elif field.nullable:
            field_dict[field.name] = None
        else:
            raise ValueError(f"Field {field.name} is not nullable but not found in the input")
    return Row(**field_dict)


def _parse_array(value: Any, field_type: ArrayType) -> list:
    if not isinstance(value, list):
        if field_type.containsNull:
            return [parse_value(value, field_type.elementType)]
        raise ValueError(f"Expected a list for ArrayType, got {type(value)}")
    return [parse_value(v, field_type.elementType) for v in value]


def _parse_map(value: Any, field_type: MapType) -> dict:
    if not isinstance(value, dict):
        raise ValueError(f"Expected a dictionary for MapType, got {type(value)}")
    return {parse_value(k, field_type.keyType): parse_value(v, field_type.valueType) for k, v in value.items()}


def _parse_string(value: Any) -> str:
    return str(value)


def _parse_integer(value: Any) -> int:
    if isinstance(value, str) and value.strip():
        return int(float(value)) if "." in value else int(value)
    if isinstance(value, (int, float)):
        return int(value)
    raise ValueError(f"Cannot convert {value} to integer")


def _parse_float(value: Any) -> float:
    return float(value)


def _parse_decimal(value: Any) -> Decimal:
    return Decimal(value) if isinstance(value, str) and value.strip() else Decimal(str(value))


def _parse_boolean(value: Any) -> bool:
    if isinstance(value, str):
        lowered = value.lower()
        if lowered in ("true", "t", "yes", "y", "1"):
            return True
        if lowered in ("false", "f", "no", "n", "0"):
            return False
    return bool(value)


def _parse_date(value: Any) -> datetime.date:
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        return datetime.fromisoformat(value).date()
    if isinstance(value, datetime):
        return value.date()
    raise ValueError(f"Cannot convert {value} to date")


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, str):
        ts_value = value.replace("Z", "+00:00") if value.endswith("Z") else value
        try:
            return datetime.fromisoformat(ts_value)
        except ValueError:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
                try:
                    return datetime.strptime(ts_value, fmt)
                except ValueError:
                    continue
    elif isinstance(value, (int, float)):
        return datetime.fromtimestamp(value)
    elif isinstance(value, datetime):
        return value
    raise ValueError(f"Cannot convert {value} to timestamp")


def _decode_string_to_bytes(value: str) -> bytes:
    try:
        return base64.b64decode(value)
    except Exception:
        pass
    try:
        return bytes.fromhex(value)
    except Exception:
        pass
    return value.encode("utf-8")


def _parse_binary(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, str):
        return _decode_string_to_bytes(value)
    if isinstance(value, list):
        return bytes(value)
    return str(value).encode("utf-8")


_PRIMITIVE_PARSERS = {
    StringType: _parse_string,
    IntegerType: _parse_integer,
    LongType: _parse_integer,
    FloatType: _parse_float,
    DoubleType: _parse_float,
    DecimalType: _parse_decimal,
    BooleanType: _parse_boolean,
    DateType: _parse_date,
    TimestampType: _parse_timestamp,
    BinaryType: _parse_binary,
}


def parse_value(value: Any, field_type: DataType) -> Any:
    """Converts a JSON value into a PySpark-compatible data type."""
    if value is None:
        return None
    if isinstance(field_type, StructType):
        return _parse_struct(value, field_type)
    if isinstance(field_type, ArrayType):
        return _parse_array(value, field_type)
    if isinstance(field_type, MapType):
        return _parse_map(value, field_type)
    try:
        field_type_class = type(field_type)
        if field_type_class in _PRIMITIVE_PARSERS:
            return _PRIMITIVE_PARSERS[field_type_class](value)
        if hasattr(field_type, "fromJson"):
            return field_type.fromJson(value)
        raise TypeError(f"Unsupported field type: {field_type}")
    except (ValueError, TypeError) as e:
        raise ValueError(f"Error converting '{value}' ({type(value)}) to {field_type}: {str(e)}")
