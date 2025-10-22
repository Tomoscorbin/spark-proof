# Property-Based Testing for Data Pipelines

## Why Property-Based Testing Matters

Traditional tests check that **specific examples** work.  
Property-based testing (PBT) checks that **general rules** always hold.

For data engineering, this difference is important.  
Example tests might prove that one sample dataset works; PBT proves that your logic is correct for *all* datasets within realistic bounds.

> If you’re new to property-based testing, [this series by Scott Wlaschin](https://fsharpforfunandprofit.com/series/property-based-testing/) gives a great introduction.
---

## What PBT Brings to ETL and Big Data

| Benefit | Why It Matters in ETL |
|----------|----------------------|
| **Finds edge-case bugs automatically** | With millions or billions of rows, it's impossible to anticipate every edge case manually. Real datasets contain unexpected combinations of nulls, duplicate keys, out-of-order timestamps, strange encodings, and unseen categories. Property-based testing explores that space for you, generating hundred or thousands (or more) of small but diverse input datasets that simulate the kinds of data pathologies your pipeline might face. This uncovers subtle bugs like mis-handled nulls, join fan-outs, and inconsistent deduplication before they reach production. |
| **Reduces test boilerplate** | Traditional example-based tests require writing explicit input/output fixtures for every scenario. As the number of cases grows, so does the noise and maintenance overhead. With PBT, data is generated automatically according to schema rules and constraints, keeping your tests compact and focused on behaviour rather than setup. The result is smaller, more readable, more maintainable tests that explore a far broader input space. |
| **Encodes behaviour, not examples** | Instead of asserting what specific output should look like, properties describe what must always be true: invariants like "each key is unique", "aggregates are consistent regardless of partitioning", or "values stay within defined ranges". These invariants rarely change when the implementation changes, making property-based tests stable documentation of the intended behaviour of your pipeline. |
| **Debuggable failures** | When a property fails, the test framework automatically "shrinks" the data to the smallest example that still reproduces the failure. You get a tiny handful of rows that expose the issue. This makes failure analysis fast and keeps test feedback loops tight. |
| **Broader coverage** | Property-based tests don't try to simulate production scale, but they do exercise a wider variety of data than hand-written tests can. Because thousands of different datasets are explored automatically, you're far more likely to catch incorrect logic or incorrect assumptions about the data. |
| **Aligns with data quality principles** | The invariants you test in PBT - schema validity, key uniqueness, referential integrity, deterministic transformations - are the same principles that underpin good data-quality practice. In other words, property-based tests let you assert the same or similar rules that you'd monitor in production, but at the code level. This creates a clean bridge between data-quality assurance and automated testing. |
| **Serves as documentation** | Good tests explain why the code exists and what guarantees it provides. Property-based tests take this a step further by  expressing the fundamental truths about your data logic. This makes your ETL system self-describing and easier to reason about. |

---

## Core Property Categories

`spark-proof` expresses typical data-pipeline guarantees as testable invariants.

| Category | Example Properties | Related Data-Quality Dimension |
|-----------|--------------------|-------------------------------|
| **Schema contracts** | The output schema matches the declared specification. All columns have valid data types, expected nullability, and precision or scale constraints. Extra or missing columns are detected. | *Validity* |
| **Key uniqueness** | For any natural key or unique identifier, the resulting dataset contains no duplicates. Where duplicates are present in the source, the transformation defines a clear and deterministic rule for resolution. | *Uniqueness* |
| **Referential integrity** | Relationships between datasets remain consistent. Joins do not multiply rows unexpectedly, missing foreign keys are handled predictably, and parent/child links remain valid. | *Consistency* |
| **Aggregation invariants** | Aggregations, groupings, or rollups produce stable results regardless of how data is chunked, partitioned, or ordered. Partial aggregations followed by unions are equivalent to a single global aggregation. | *Accuracy* |
| **Domain and range constraints** | Values fall within valid, expected ranges or enumerations. Outliers, invalid categories, and impossible values (e.g. negative quantities or future timestamps) are either cleaned, imputed, or rejected according to defined rules. | *Validity* |
| **Idempotence** | Running the same transformation multiple times with identical input yields the same result. Duplicate ingestion or reruns do not introduce new or inconsistent data. | *Consistency* |
| **Temporal and sequential correctness** | Any ordering, windowing, or stateful logic behaves deterministically across time. For example, "latest per key", "earliest event", or "rolling window" rules always produce consistent, non-contradictory outcomes. | *Accuracy* |
| **Completeness and presence** | All mandatory fields and expected entities appear in the output. Optional fields obey nullability rules, and missing data is flagged or filled according to defined policy. | *Completeness* |

---

## Beyond Classical Data-Quality

Property-based tests also enforce *engineering* guarantees that DQ tools can't:

- **Determinism** — results don't depend on Spark partitioning or order.
- **Idempotence** — reruns don't duplicate or mutate data unexpectedly.
- **Compositional correctness** — processing data in chunks equals processing it all at once.
- **Metamorphic consistency** — the result behaves predictably under input transformations.

---

## Metamorphic Relations in ETL

Property-based testing lets you verify behavioural correctness even when there's no fixed "expected output".
Instead of asserting that `result = X`, you define relationships that must always hold when the input changes in predictable ways.

These metamorphic relations are useful for large-scale data processing, where results depend on transformations rather than specific values.

| Relation                              | Description & Why It **Matters**                                                                      |
| ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Permutation-invariance**            | Reordering input rows (e.g. due to different partitioning or file load order) should not change the logical result. This checks that your transformations don’t depend on implicit ordering or unstable aggregations.                                     |
| **Chunk-consistency**                 | Splitting data into multiple batches, processing each separately, and then unioning the results should produce the same output as processing it all at once. This ensures aggregations and deduplication logic behave correctly across incremental loads. |
| **Duplicate-handling**                | Duplicating input rows should either leave the result unchanged (if deduplication is intended) or change it in a predictable, linear way (e.g. counts doubling). This reveals non-idempotent logic and helps validate merge behaviour.                    |
| **Null and missing-data resilience**  | Injecting additional nulls, blanks, or optional fields should not crash the pipeline, and should trigger consistent fallback or cleaning behaviour. This validates your null-handling and schema-evolution logic.                                         |
| **Late-data stability**               | Introducing events that arrive after the fact (within a defined watermark or processing window) should only update affected records and leave unaffected ones unchanged. This is essential for stateful and time-based pipelines.                         |
| **Filter and threshold monotonicity** | Making a filter stricter (e.g. `amount > 100` instead of `> 50`) should never *add* rows to the output; loosening it should never *remove* rows. This checks that your filtering and business rules behave logically.                                     |
| **Aggregation coherence**             | Changing grouping granularity (e.g. from daily to weekly) should yield compatible totals — weekly sums must equal the sum of daily totals. This guards against silent double-counting or loss during re-aggregation.                                      |
| **Schema-evolution safety**           | Adding, renaming, or dropping unused columns in the input should not change the computed results of unaffected fields. This protects downstream consumers from unintended side effects when schemas evolve.                                               |

---

## Example: Behavioural Test

```python
from spark_proof import sp

@given(df=sp.data_frame(schema={
    "customer": sp.string(max_size=10),
    "date": sp.date(),
    "amount": sp.decimal(precision=12, scale=2),
}))
def test_one_row_per_customer_after_rank(spark, df):
    # Given arbitrary messy input (dupes, nulls, out-of-order)
    cleaned = cleanse(df)

    # When keeping the latest transaction per customer
    actual = rank_and_filter(cleaned, key_cols=["customer"], order_cols=["date"])

    # Then
    sp.assert_one_row_per_key(actual, ["customer"])  # uniqueness invariant

