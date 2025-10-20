from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from typing import Sequence


def assert_one_row_per_key(df: DataFrame, key_columns: Sequence[str]) -> None:
    if not key_columns:
        raise ValueError("key_columns must not be empty")

    missing = [c for c in key_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in DataFrame: {missing}")

    counts_per_key = df.groupBy(*key_columns).count()
    violations = counts_per_key.filter(F.col("count") > 1)

    if not violations.isEmpty():
        raise AssertionError("One row per key violated")
