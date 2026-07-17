import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from spark_proof._internal.types import Rows


class MaterialisationError(Exception):
    """Spark rejected generated rows during DataFrame creation."""


def materialise(spark: SparkSession, schema: T.StructType, rows: Rows) -> DataFrame:
    try:
        return spark.createDataFrame(rows, schema=schema)
    except Exception as error:
        raise MaterialisationError(
            "Spark rejected the generated rows.\n"
            f"  schema: {schema.simpleString()}\n"
            f"  rows: {rows!r}\n"
            f"  cause: {error}"
        ) from error
