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
        # Small amounts of data in tests so no need for more than 1 CPU
        # More CPUs would actually require more overhead in this instance.
        .master("local[1]")
        # fail faster if there's an issue with initial [local] conections
        .config("spark.network.timeout", "10000")
        .config("spark.executor.heartbeatInterval", "1000")
        # Locally, the driver shares the memory with the executors.
        # So best to constrain it somewhat!
        .config("spark.driver.memory", "2g")
        # Default partitions is 200 which is good for _big data_. The shuffle
        # overhead for our small, test data sets will be more expensive than the
        # computation.
        .config("spark.sql.shuffle.partitions", "1")
        # No need for any UI components, or keeping history
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.dagGraph.retainedRootRDDs", "1")
        .config("spark.ui.retainedJobs", "1")
        .config("spark.ui.retainedStages", "1")
        .config("spark.ui.retainedTasks", "1")
        .config("spark.sql.ui.retainedExecutions", "1")
        .config("spark.worker.ui.retainedExecutors", "1")
        .config("spark.worker.ui.retainedDrivers", "1")
        #
        .config("spark.default.parallelism", "1")
        # Disable compression (pointless on small datasets)
        .config("spark.rdd.compress", "false")
        .config("spark.shuffle.compress", "false")
        #
        .config("spark.dynamicAllocation.enabled", "false")
        # Control the executor resources
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        # # fail faster if there's an issue with initial [local] conections
        # .config("spark.network.timeout", "10000")
        # .config("spark.executor.heartbeatInterval", "1000")
        # # Locally, the driver shares the memory with the executors.
        # # So best to constrain it somewhat!
        # .config("spark.driver.memory", "2g")
        # # Kill anything non-essential
        # .config("spark.ui.showConsoleProgress", "false")
        # .config("spark.ui.enabled", "false")
        # .config("spark.ui.dagGraph.retainedRootRDDs", "1")
        # .config("spark.ui.retainedJobs", "1")
        # .config("spark.ui.retainedStages", "1")
        # .config("spark.ui.retainedTasks", "1")
        # .config("spark.sql.ui.retainedExecutions", "1")
        # .config("spark.worker.ui.retainedExecutors", "1")
        # .config("spark.worker.ui.retainedDrivers", "1")
        # .config("spark.eventLog.enabled", "false")
        # .config("spark.sql.streaming.ui.enabled", "false")
        # .config("spark.dynamicAllocation.enabled", "false")
        # .config("spark.shuffle.service.enabled", "false")
        # .config("spark.speculation", "false")
        # # Disable compression (pointless on small datasets)
        # .config("spark.rdd.compress", "false")
        # .config("spark.shuffle.compress", "false")
        # # Control the executor resources
        # .config("spark.executor.cores", "1")
        # .config("spark.executor.instances", "1")
        # # Avoid Hive; use in-memory catalog
        # .config("spark.sql.catalogImplementation", "in-memory")
        # .config("spark.sql.legacy.createHiveTableByDefault", "false")
        .getOrCreate()
    )

    yield spark

    spark.stop()
