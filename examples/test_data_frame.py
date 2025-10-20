import spark_proof as sp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F


def rank_and_filter(
    df: DataFrame, partition_by: list[str], order_by: list[str]
) -> DataFrame:
    # build a list of descending order expressions for each column
    order_exprs = [F.col(c).desc() for c in order_by]
    w = Window.partitionBy(*partition_by).orderBy(*order_exprs)
    return (
        df.withColumn("_rank", F.row_number().over(w))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


@sp.data_frame(
    schema={
        "customer": sp.string(min_size=3, max_size=10),
        "date": sp.date(),
    }
)
def test_rank_and_filter(df):
    actual = rank_and_filter(df, ["customer"], ["date"])
    sp.assert_one_row_per_key(actual, ["customer"])
