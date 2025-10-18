from dataclasses import dataclass
from hypothesis import strategies as st
import pyspark.sql.types as T

INT32_MIN = -2_147_483_648
INT32_MAX = 2_147_483_647


@dataclass(frozen=True, slots=True)
class Generator:
    strategy: st.SearchStrategy
    spark_type: T.DataType


def integer(min_value: int, max_value: int):
    if min_value > max_value:
        raise ValueError("min_value must be <= max_value")
    if min_value < INT32_MIN or max_value > INT32_MAX:
        raise ValueError(
            f"integer() must fit int32: [{INT32_MIN, INT32_MAX}];",
            f" got [{min_value, max_value}]",
        )
    return Generator(
        strategy=st.integers(min_value=min_value, max_value=max_value),
        spark_type=T.IntegerType(),
    )


print(dir(T.IntegerType()))
