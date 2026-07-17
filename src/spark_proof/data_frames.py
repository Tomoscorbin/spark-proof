from typing import Any, Callable, Mapping

from spark_proof._internal.materialisation import materialise
from spark_proof._internal.models import ValueSpec, build_data_frame_spec
from spark_proof._internal.runtime import Rows, wrap_test


def data_frame(
    *,
    columns: Mapping[str, ValueSpec],
    min_rows: int = 0,
    max_rows: int = 50,
    spark_fixture: str = "spark",
) -> Callable[[Callable[..., None]], Callable[..., None]]:
    """Run a test against Hypothesis-generated Spark DataFrames.

    The generated DataFrame is passed as the test's first parameter; the
    SparkSession comes from the pytest fixture named by `spark_fixture`.
    """
    spec = build_data_frame_spec(columns=columns, min_rows=min_rows, max_rows=max_rows)

    def decorate(test_function: Callable[..., None]) -> Callable[..., None]:
        def build_input(request: Any, rows: Rows) -> Any:
            spark = _resolve_spark_session(request, spark_fixture)
            return materialise(spark, spec.schema, rows)

        return wrap_test(test_function, spec.rows_strategy, build_input)

    return decorate


def _resolve_spark_session(request: Any, fixture_name: str) -> Any:
    try:
        return request.getfixturevalue(fixture_name)
    except Exception as error:
        raise RuntimeError(
            f"data_frame: could not resolve a SparkSession from the pytest "
            f"fixture '{fixture_name}'. Define a fixture with that name that "
            f"returns a SparkSession, or pass spark_fixture='<your fixture>'."
        ) from error
