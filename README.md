# Spark Proof

Spark Proof is a property-based testing toolkit for PySpark. It wraps
[Hypothesis](https://hypothesis.readthedocs.io/) so your pytest tests receive
generated Spark DataFrames with explicit schemas — and when a test fails,
Hypothesis shrinks the input to a minimal failing example you can read.

> **Status: early development.** The API below is the complete current
> surface. Strings, dates, decimals, nullability, and more generators are
> planned; see the roadmap note at the bottom.

## Example

```python
import spark_proof as sp


@sp.data_frame(
    columns={
        "customer_id": sp.integer(min_value=1, max_value=1_000_000),
        "score": sp.integer(min_value=0, max_value=100),
    },
    min_rows=0,
    max_rows=50,
)
def test_scores_never_exceed_bounds(df):
    result = normalise_scores(df)  # your pipeline code under test

    assert result.filter(result.score > 100).count() == 0
```

Each test run explores many generated DataFrames — including the empty
one — and any failure is reported with the shrunk minimal rows that
caused it.

## Current API

- `sp.integer(min_value=None, max_value=None)` — values for a Spark
  `IntegerType` column. Bounds are validated against the 32-bit signed
  domain up front, with errors that name the offending argument.
- `@sp.data_frame(columns=..., min_rows=0, max_rows=50, spark_fixture="spark")`
  — decorates a pytest test. `columns` maps column names to generators;
  column order is preserved in the schema. The generated DataFrame is
  passed as the test's first parameter; other pytest fixtures keep
  working as normal.

## pytest setup

Spark Proof resolves your `SparkSession` from a pytest fixture (named
`spark` by default — override with `spark_fixture="my_fixture"`):

```python
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[1]").getOrCreate()
    yield session
    session.stop()
```

## Background

For more on how property-based testing applies to ETL and data
pipelines, see
[Property-Based Testing for Data Pipelines](docs/pbt_for_data_pipelines.md).

## Roadmap

In rough order: the remaining scalar generators (`long`, `short`,
`double`, `decimal`, `date`, `timestamp`, …), nullability, first-class
string generation (length/alphabet bounds, regex, email, UUID), explicit
value choices and constants, and a custom-Hypothesis-strategy escape
hatch.
