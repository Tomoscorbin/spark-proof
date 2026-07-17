import pyspark.sql.types as T
import pytest

from spark_proof._internal.materialisation import MaterialisationError, materialise

SCHEMA = T.StructType(
    [
        T.StructField("a", T.IntegerType(), False),
        T.StructField("b", T.IntegerType(), False),
    ]
)


def test_rows_materialise_with_values_and_column_order_intact(spark):
    df = materialise(spark, SCHEMA, [{"a": 1, "b": -1}, {"a": 2, "b": 0}])

    assert df.columns == ["a", "b"]
    assert [(r["a"], r["b"]) for r in df.collect()] == [(1, -1), (2, 0)]


def test_empty_rows_produce_an_empty_dataframe_with_the_full_schema(spark):
    df = materialise(spark, SCHEMA, [])

    assert df.count() == 0
    assert df.schema == SCHEMA


def test_incompatible_value_raises_with_schema_and_rows_context(spark):
    bad_rows = [{"a": "not-an-int", "b": 0}]

    with pytest.raises(MaterialisationError) as excinfo:
        materialise(spark, SCHEMA, bad_rows)

    message = str(excinfo.value)
    assert "a:int" in message  # schema context (simpleString form)
    assert "not-an-int" in message  # offending rows context
