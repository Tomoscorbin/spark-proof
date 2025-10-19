import spark_proof as sp
from pyspark.sql import DataFrame
import pyspark.sql.types as T
import pyspark.sql.functions as F


# TODO: write test to prove shrinking occurs


@sp.data_frame(
    rows=5,
    schema={"x": sp.gen.integer(min_value=0, max_value=10)},
)
def test_data_frame_is_spark_dataframe(spark, df: DataFrame):
    assert isinstance(df, DataFrame)


@sp.data_frame(rows=0, schema={"x": sp.gen.integer(min_value=0, max_value=10)})
def test_dataframe_is_built_even_when_empty(spark, df: DataFrame):
    assert df.schema == T.StructType([T.StructField("x", T.IntegerType(), True)])


@sp.data_frame(rows=5, schema={"a": sp.gen.integer(min_value=0, max_value=10)})
def test_data_frame_respects_value_ranges(spark, df):
    assert 0 <= df.count() <= 5  # decorator bound
    assert df.filter((F.col("a") < 0) | (F.col("a") > 10)).count() == 0
