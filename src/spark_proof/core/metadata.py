from dataclasses import dataclass
from enum import StrEnum
from typing import Generic, Literal, TypeVar, Protocol, runtime_checkable, Any
import pyspark.sql.types as T


@runtime_checkable
class SupportsOrder(Protocol):
    def __lt__(self, other: Any, /) -> bool: ...
    def __le__(self, other: Any, /) -> bool: ...
    def __gt__(self, other: Any, /) -> bool: ...
    def __ge__(self, other: Any, /) -> bool: ...
    def __eq__(self, value: Any, /) -> bool: ...


# TODO: move to types.py?
DT = TypeVar("DT", bound=SupportsOrder)
DT_in = TypeVar("DT_in", bound=SupportsOrder, contravariant=True)
Width = Literal[32, 64]


@dataclass(frozen=True, slots=True)
class DomainMetadata(Generic[DT]):
    name: str
    spark_type: T.DataType
    python_type: type[DT]


class TypeName(StrEnum):
    SHORT = "short"
    INTEGER = "integer"
    LONG = "long"
    FLOAT32 = "float32"
    DOUBLE = "double"
    DATE = "date"
    TIMESTAMP = "timestamp"
