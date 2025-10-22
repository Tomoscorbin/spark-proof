from typing import Iterator
from pyspark.sql import SparkSession
import pytest
import logging
import os


def quiet_py4j() -> None:
    """Turn down Spark logging during tests."""
    logging.getLogger("py4j").setLevel(logging.WARN)


# this is to stop spark creating an artifacts/ folder
# TODO: figure out why spark creates an artifacts/ folder
@pytest.fixture(scope="session", autouse=True)
def _session_cwd(tmp_path_factory):
    os.chdir(tmp_path_factory.mktemp("wd"))


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Minimal, fast SparkSession for tests."""
    quiet_py4j()
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("spark-proof-tests")
        .master("local[1]")
        .config("spark.network.timeout", "10000")
        .config("spark.executor.heartbeatInterval", "1000")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.dagGraph.retainedRootRDDs", "1")
        .config("spark.ui.retainedJobs", "1")
        .config("spark.ui.retainedStages", "1")
        .config("spark.ui.retainedTasks", "1")
        .config("spark.sql.ui.retainedExecutions", "1")
        .config("spark.worker.ui.retainedExecutors", "1")
        .config("spark.worker.ui.retainedDrivers", "1")
        .config("spark.eventLog.enabled", "false")
        .config("spark.default.parallelism", "1")
        .config("spark.rdd.compress", "false")
        .config("spark.shuffle.compress", "false")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .getOrCreate()
    )

    yield spark

    spark.stop()
