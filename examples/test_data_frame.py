import spark_proof as sp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F


def rank_and_filter(
    df: DataFrame, partition_by: list[str], order_by: list[str]
) -> DataFrame:
    """
    Keeps the first (top-ranked) row in every partition group,
    where ranking is based on all order columns in descending order,
    using `row_number()`.

    Args:
        df: Input DataFrame.
        partition_column_name: Column name for the group / partition.
        order_column_names: One or more column names whose descending
                        values decide the rank. The first column has
                        highest precedence, second entry breaks ties, etc.

    Returns:
        DataFrame with only the highest-ranked row for every partition.
    """
    # build a list of descending order expressions for each column
    order_exprs = [F.col(c).desc() for c in order_by]
    w = Window.partitionBy(*partition_by).orderBy(*order_exprs)
    return (
        df.withColumn("_rank", F.row_number().over(w))
        # .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


@sp.data_frame(
    schema={
        "customer": sp.string(max_size=10),
        "date": sp.date(),
    }
)
def test_rank_and_filter(spark, df):
    actual = rank_and_filter(df, ["customer"], ["date"])
    sp.assert_one_row_per_key(actual, ["customer"])
