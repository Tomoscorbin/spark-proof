from dataclasses import dataclass
from typing import Any

import pyspark.sql.types as T
from hypothesis.strategies import SearchStrategy


@dataclass(frozen=True, slots=True)
class ValueSpec:
    """Private pairing of a Hypothesis strategy with its Spark type.

    Users never see this class; public constructor functions return it.
    """

    strategy: SearchStrategy[Any]
    spark_type: T.DataType
    nullable: bool
