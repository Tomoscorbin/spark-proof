import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest
from pyspark.sql import DataFrame

import spark_proof as sp


@sp.data_frame(columns={"x": sp.integer(min_value=0, max_value=10)}, max_rows=5)
def test_decorated_test_receives_a_real_spark_dataframe(df):
    assert isinstance(df, DataFrame)


@sp.data_frame(columns={"x": sp.integer(min_value=0, max_value=10)}, max_rows=0)
def test_empty_dataframes_are_built_with_the_declared_schema(df):
    assert df.schema == T.StructType([T.StructField("x", T.IntegerType(), False)])
    assert df.count() == 0


@sp.data_frame(columns={"a": sp.integer(min_value=0, max_value=10)}, max_rows=5)
def test_row_counts_and_values_respect_the_declaration(df):
    assert 0 <= df.count() <= 5
    assert df.filter((F.col("a") < 0) | (F.col("a") > 10)).count() == 0


@sp.data_frame(
    columns={"a": sp.integer(min_value=0, max_value=10)}, min_rows=2, max_rows=4
)
def test_min_rows_lower_bound_is_respected(df):
    assert 2 <= df.count() <= 4


@sp.data_frame(columns={"x": sp.integer()}, max_rows=3)
def test_other_pytest_fixtures_still_resolve(df, tmp_path):
    assert tmp_path.exists()
    assert isinstance(df, DataFrame)


@pytest.fixture(scope="session")
def my_spark(spark):
    return spark


@sp.data_frame(
    columns={"x": sp.integer()}, max_rows=3, spark_fixture="my_spark"
)
def test_a_custom_spark_fixture_name_is_honoured(df):
    assert isinstance(df, DataFrame)


def test_missing_spark_fixture_fails_with_a_clear_error(request):
    @sp.data_frame(
        columns={"x": sp.integer()}, max_rows=3, spark_fixture="no_such_fixture"
    )
    def probe(df):
        pass

    with pytest.raises(RuntimeError, match="no_such_fixture"):
        probe(request=request)


def test_invalid_declarations_fail_at_decoration_time_not_test_time():
    with pytest.raises(ValueError, match="data_frame"):

        @sp.data_frame(
            columns={"x": sp.integer()}, min_rows=5, max_rows=1
        )
        def never_runs(df):
            pass
