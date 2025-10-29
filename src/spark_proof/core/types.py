from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, Any, TypeVar, Literal, Generic
import pyspark.sql.types as T
from spark_proof.core.limits import FLOAT32_MAX, FLOAT32_MIN, INT32_MAX, INT32_MIN


class Comparable(Protocol):
    """
    Structural protocol for things we can order.

    Scalar[CT] needs to compare CT values (e.g. bounds checks, min()/max()),
    so CT must implement < and >.
    """

    def __lt__(self, other: Any, /) -> bool: ...
    def __gt__(self, other: Any, /) -> bool: ...


CT = TypeVar("CT", bound=Comparable)
Width = Literal[16, 32, 64]


class TypeName(StrEnum):
    SHORT = "short"
    INTEGER = "integer"
    LONG = "long"
    FLOAT32 = "float32"
    DOUBLE = "double"


@dataclass(frozen=True, slots=True)
class DomainType(Generic[CT]):
    name: TypeName
    spark: T.DataType
    python: type[CT]
    min: CT
    max: CT


integer_type = DomainType(
    name=TypeName.INTEGER,
    spark=T.IntegerType(),
    python=int,
    min=INT32_MIN,
    max=INT32_MAX,
)

float_type = DomainType(
    name=TypeName.FLOAT32,
    spark=T.FloatType(),
    python=float,
    min=FLOAT32_MIN,
    max=FLOAT32_MAX,
)
