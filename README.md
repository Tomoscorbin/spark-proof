# Spark Proof

Spark Proof is a property based testing toolkit for PySpark data pipelines. It wraps Hypothesis so that you can generate large and varied DataFrame samples with minimal boilerplate while staying in the familiar pytest workflow. Use it to harden data transformations, catch edge cases that example based tests miss, and keep confidence high as your pipeline evolves.

## Why property based testing helps big data teams

Working with data at scale makes it impossible to reason about every possible input manually. Property based testing flips the mindset: instead of writing one or two fixtures, you describe the rules your pipeline must respect. Spark Proof generates many random examples, automatically shrinks any failing case to the smallest counter example, and makes it straightforward to replay and debug the issue. The result is faster feedback and fewer production surprises.

## Key features

- **Purpose built generators for Spark types.** Use helpers such as `integer`, `decimal`, `date`, and `string_from_regex` to produce realistic values that respect Spark type limits.
- **Automatic DataFrame construction.** The `@data_frame` decorator builds Spark DataFrames from your schema definition so you can focus on expressing the property you care about.
- **Minimal pytest setup.** Point the decorator at your SparkSession fixture (defaults to `spark`) and Spark Proof will resolve it, populate a DataFrame, and hand it to your test function.
- **Scalable randomness.** Configure the maximum row count for each test. Hypothesis explores the space for you, from empty DataFrames through to dense samples.
- **Tight Spark integration.** Generated values always conform to the Spark schema, which keeps Catalyst happy and avoids brittle casting logic inside your pipeline.
- **Actionable failures.** Hypothesis shrinking and tracing notes ensure that failing inputs are small, repeatable, and easy to copy into a notebook or a debugging session.


## Example property test

Below is a test that verifies a transformation never produces duplicate customer records. The decorator handles DataFrame creation while you assert the property you care about.

```python
import pyspark.sql.functions as F
import spark_proof as sp

@sp.data_frame(
    rows=500,
    schema={
        "customer_id": sp.integer(min_value=1, max_value=1_000_000),
        "signup_date": sp.date(),
        "email": sp.string(min_size=5, max_size=120),
    },
)
def test_customer_pipeline_enforces_unique_ids(df):
    result = transform_customers(df)  # your pipeline code under test

    sp.assert_one_row_per_key(result, ["customer_id"])
```

When the property fails Hypothesis prints the smallest counter example and the generated rows so that you can reproduce the bug quickly.

## Generating rich data sets

Every generator balances Spark compatibility with Hypothesis flexibility:

- Control numeric bounds with `integer`, `short`, `long`, `float32`, `double`, and `decimal`. Spark Proof validates the limits so you stay within each Spark type window.
- Produce date and timestamp ranges that honour Spark constraints.
- Generate text with either length bounds through `string` or pattern based content with `string_from_regex`.
- Compose generators into nested dictionaries to model realistic schemas for wide tables.

Because all generators return standard Hypothesis strategies you can extend them with `map`, `filter`, or any other Hypothesis combinator when you need bespoke data.

## Integration with pytest and Spark

Spark Proof expects a pytest fixture that returns a `SparkSession`. By default the `@data_frame` decorator looks for a fixture named `spark`, but you can change that by passing `session="another_fixture"`. The fixture in `tests/conftest.py` shows a lean local `SparkSession` configuration that keeps tests fast.

Behind the scenes the decorator requests rows from Hypothesis, converts them into a Spark schema, and builds a DataFrame for every test invocation. This keeps your test bodies short and expressive while ensuring every generated dataset is valid Spark data.

## Assertions and helpers

In addition to generators, Spark Proof ships with focused assertions that capture common data quality checks. For example, `assert_one_row_per_key` groups by the supplied key columns and fails when duplicates slip through. These helpers make it easy to express invariants right next to your transformations.