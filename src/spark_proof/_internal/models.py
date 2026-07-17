from dataclasses import dataclass
from typing import Any, Mapping

import hypothesis.strategies as st
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


@dataclass(frozen=True, slots=True)
class DataFrameSpec:
    """Private generation plan: explicit Spark schema + bounded rows strategy."""

    schema: T.StructType
    rows_strategy: SearchStrategy[list[dict[str, Any]]]


def build_data_frame_spec(
    *, columns: Mapping[str, ValueSpec], min_rows: int, max_rows: int
) -> DataFrameSpec:
    if not columns:
        raise ValueError("data_frame: columns must contain at least one column")
    for name in columns:
        if not isinstance(name, str) or not name:
            raise ValueError(
                f"data_frame: column names must be non-empty strings (got {name!r})"
            )
    if min_rows < 0:
        raise ValueError(f"data_frame: min_rows must be >= 0 (got {min_rows})")
    if min_rows > max_rows:
        raise ValueError(
            f"data_frame: min_rows must be <= max_rows (got {min_rows} > {max_rows})"
        )

    schema = T.StructType(
        [
            T.StructField(name, spec.spark_type, spec.nullable)
            for name, spec in columns.items()
        ]
    )
    row_strategy = st.fixed_dictionaries(
        {name: spec.strategy for name, spec in columns.items()}
    )
    rows_strategy = st.lists(row_strategy, min_size=min_rows, max_size=max_rows)
    return DataFrameSpec(schema=schema, rows_strategy=rows_strategy)
