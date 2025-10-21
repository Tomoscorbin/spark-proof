from pyspark.sql import SparkSession
import pyspark.sql.types as T
from typing import Mapping, TypeAlias
from spark_proof.gen import Generator
from hypothesis import given, settings, Phase, Verbosity, HealthCheck
from hypothesis import strategies as st
from pytest import FixtureRequest


SETTINGS = {
    "deadline": None,  # Spark can be slow
    "verbosity": Verbosity.quiet,  # less chatter
    "phases": (Phase.reuse, Phase.generate, Phase.shrink),  # drop Phase.explain
    "suppress_health_check": (HealthCheck.too_slow,),  # Spark can be slow
}

ColumnName: TypeAlias = str
InputSchema: TypeAlias = Mapping[ColumnName, Generator]


def _build_spark_schema(schema: InputSchema) -> T.StructType:
    types = [(name, gen.spark_type) for name, gen in schema.items()]
    return T.StructType([T.StructField(name, t, nullable=True) for name, t in types])


def _build_rows_strategy(schema: InputSchema, max_rows: int):
    row_strategy = st.fixed_dictionaries(
        {name: gen.strategy for name, gen in schema.items()}
    )
    return st.lists(row_strategy, min_size=0, max_size=max_rows)


def _resolve_session(
    session: str,
    request: FixtureRequest,
) -> SparkSession:
    try:
        return request.getfixturevalue(session)
    except Exception as e:
        raise RuntimeError(
            "Could not obtain SparkSession from pytest fixture"
            f" '{session}'. Ensure you're running under pytest with a SparkSession fixture."
        ) from e


def data_frame(
    rows: int = 100,
    *,
    schema: InputSchema,
    session: str = "spark",
):
    rows_strategy = _build_rows_strategy(schema, max_rows=rows)
    spark_schema = _build_spark_schema(schema)

    def outer(test_function):
        @given(rows=rows_strategy)
        @settings(**SETTINGS)
        def wrapper(request, *args, rows, **kwargs):
            spark = _resolve_session(session, request)
            df = spark.createDataFrame(rows, schema=spark_schema)
            return test_function(df, *args, **kwargs)

        return wrapper

    return outer
