from abc import ABC, abstractmethod
from typing import Iterator

from pyspark.sql.types import StructType


class LakeflowConnect(ABC):
    """Base interface that each source connector must implement.

    Subclass this and implement all abstract methods to create a connector that
    integrates with the community connector library and ingestion pipeline.
    """

    def __init__(self, options: dict[str, str]) -> None:
        self.options = options

    @abstractmethod
    def list_tables(self) -> list[str]:
        """List names of all the tables supported by the source connector."""

    @abstractmethod
    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        """Fetch the schema of a table."""

    @abstractmethod
    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        """Fetch the metadata of a table.

        Returns a dict with keys:
            - primary_keys: List[str]
            - cursor_field: str
            - ingestion_type: "snapshot" | "cdc" | "cdc_with_deletes" | "append"
        """

    @abstractmethod
    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read records and return (iterator, next_offset)."""

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        raise NotImplementedError("read_table_deletes() must be implemented when ingestion_type is 'cdc_with_deletes'")
