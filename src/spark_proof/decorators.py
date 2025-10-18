from pyspark.sql import SparkSession
import pyspark.sql.types as T
from typing import Mapping, TypeAlias
from spark_proof.gen import Generator
from hypothesis import given
from hypothesis import strategies as st
from typing import Callable

# TODO: create a dictionary of settings and unpack them in the settings decorator!!!
ColumnName: TypeAlias = str
InputSchema: TypeAlias = Mapping[ColumnName, Generator]


def wraps(original: Callable) -> Callable[[Callable], Callable]:
    """Copy of functools.wraps but doesn't set __wrapped__."""

    # TODO: copy actual functools.wraps code
    # TODO: since we have used Verbosity.quiet, this is no longer needed
    def decorate(wrapper: Callable) -> Callable:
        for attr in ("__name__", "__qualname__", "__doc__", "__module__"):
            try:
                setattr(wrapper, attr, getattr(original, attr))
            except Exception:
                pass
        return wrapper

    return decorate


def _to_spark_schema(schema: InputSchema) -> T.StructType:
    types = [(name, gen.spark_type) for name, gen in schema.items()]
    return T.StructType([T.StructField(name, t, nullable=True) for name, t in types])


def data_frame(rows: int = 100, *, schema: InputSchema):
    row_strategy = st.fixed_dictionaries(
        {name: gen.strategy for name, gen in schema.items()}
    )
    rows_strategy = st.lists(row_strategy, min_size=0, max_size=rows)
    spark_schema = _to_spark_schema(schema)

    def outer(test):
        @given(rows=rows_strategy)
        @wraps(test)
        def wrapper(spark: SparkSession, *args, rows, **kwargs):
            df = spark.createDataFrame(rows, schema=spark_schema)
            return test(spark, df, *args, **kwargs)

        return wrapper

    return outer
