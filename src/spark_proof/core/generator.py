from dataclasses import dataclass
import hypothesis.strategies as st
import pyspark.sql.types as T


@dataclass(frozen=True, slots=True)
class Generator:
    strategy: st.SearchStrategy
    spark_type: T.DataType
