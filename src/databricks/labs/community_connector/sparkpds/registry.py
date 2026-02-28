"""Registry module for registering LakeflowSource with Spark's DataSource API."""

import importlib
from types import ModuleType
from typing import Type, Union

from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import LakeflowSource

_BASE_PKG = "databricks.labs.community_connector.sources"


def _get_class_fqn(cls: Type) -> str:
    return f"{cls.__module__}.{cls.__name__}"


def _import_class(fqn: str) -> Type:
    module_name, class_name = fqn.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


def _get_source_module(source_name: str, module_name: str) -> ModuleType:
    try:
        importlib.import_module(f"{_BASE_PKG}.{source_name}")
    except ModuleNotFoundError:
        raise ValueError(
            f"Source '{source_name}' not found. "
            f"Make sure the directory 'src/databricks/labs/community_connector/sources/{source_name}/' exists."
        )
    module_path = f"{_BASE_PKG}.{source_name}.{module_name}"
    try:
        return importlib.import_module(module_path)
    except ModuleNotFoundError:
        raise ImportError(
            f"Could not import '{module_name}.py' from source '{source_name}'. "
            f"Please ensure 'src/databricks/labs/community_connector/sources/{source_name}/{module_name}.py' exists."
        )


def _get_register_function(source_name: str):
    module_name = f"_generated_{source_name}_python_source"
    module = _get_source_module(source_name, module_name)
    if not hasattr(module, "register_lakeflow_source"):
        raise ImportError(f"Module '{module_name}' does not have a 'register_lakeflow_source' function.")
    return module.register_lakeflow_source


def register(
    spark: SparkSession,
    source: Union[str, Type[DataSource], Type[LakeflowConnect]],
) -> None:
    """Register a source with Spark's DataSource API."""
    if isinstance(source, str):
        register_fn = _get_register_function(source)
        register_fn(spark)
        return

    if isinstance(source, type) and issubclass(source, DataSource):
        spark.dataSource.register(source)
        return

    if isinstance(source, type) and issubclass(source, LakeflowConnect):
        class_fqn = _get_class_fqn(source)

        class RegisterableLakeflowSource(LakeflowSource):
            def __init__(self, options):
                self.options = options
                lakeflow_connect_cls = _import_class(class_fqn)
                self.lakeflow_connect = lakeflow_connect_cls(options)

        spark.dataSource.register(RegisterableLakeflowSource)
        return

    raise TypeError(f"source must be a string, DataSource subclass, or LakeflowConnect subclass, got {type(source)}")
