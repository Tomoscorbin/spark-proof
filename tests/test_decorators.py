import spark_proof as sp
from pyspark.sql import DataFrame
import pyspark.sql.types as T
import pyspark.sql.functions as F


@sp.data_frame(
    rows=5,
    schema={"x": sp.gen.integer(min_value=0, max_value=10)},
)
def test_data_frame_is_spark_dataframe(spark, df: DataFrame):
    # Runtime check
    assert isinstance(df, DataFrame)


@sp.data_frame(rows=0, schema={"x": sp.gen.integer(min_value=0, max_value=10)})
def test_data_frame_builds_dataframe_even_when_empty(spark, df: DataFrame):
    assert df.schema == T.StructType([T.StructField("x", T.IntegerType(), True)])


@sp.data_frame(rows=5, schema={"a": sp.gen.integer(min_value=0, max_value=10)})
def test_data_frame_respects_value_ranges(spark, df):
    assert 0 <= df.count() <= 5  # decorator bound
    assert df.filter((F.col("a") < 0) | (F.col("a") > 10)).count() == 0


@sp.data_frame(
    rows=20,  # Hypothesis can shrink 0..20; we force non-empty below
    schema={"b": sp.gen.integer(min_value=0, max_value=10)},
)
def test_shrinks_through_decorator_plain(spark, df: DataFrame):
    # Intentionally impossible property (0 is in-domain) -> will fail and shrink.
    violating = df.filter(F.col("b") == 0).limit(5).collect()
    assert len(violating) == 0
