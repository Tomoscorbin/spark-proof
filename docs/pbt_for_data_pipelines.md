# Property-Based Testing for Data Pipelines

## Why Property-Based Testing?

Traditional tests check that **specific examples** work.  
Property-based testing (PBT) checks that **general rules** always hold.

For data engineering, this difference is important.  
Example tests might prove that one sample dataset works, but PBT proves that your logic is correct for *all* datasets within realistic bounds.

> If you’re new to property-based testing, [this series by Scott Wlaschin](https://fsharpforfunandprofit.com/series/property-based-testing/) gives a great introduction.
---

## What PBT Brings to ETL and Big Data

| Benefit | Why It Matters in ETL |
|----------|----------------------|
| **Finds edge-case bugs automatically** | With millions or billions of rows, it's impossible to anticipate every edge case manually. Real datasets contain unexpected combinations of nulls, duplicate keys, out-of-order timestamps, strange encodings, unseen categories, etc. PBT explores that space for you by generating hundred or thousands (or more) of eivetse input records that simulate the kinds of data pathologies your pipeline might face. This can uncover subtle bugs like mis-handled nulls, join fan-outs, and inconsistent deduplication before they reach production. |
| **Reduces test boilerplate** | Traditional example-based tests require writing explicit input/output tests for every scenario. As the number of cases grows, so does the noise and maintenance overhead. With PBT, data is generated automatically according to schema rules and constraints, keeping your tests clean and focused on behaviour rather than setup. The result is smaller, more readable, more maintainable tests that explore a far broader input space. |
| **Debuggable failures** | When a property fails, the test framework automatically "shrinks" the data to the smallest example that still reproduces the failure. You get a tiny handful of rows that expose the issue. This makes failure analysis fast and keeps test feedback loops tight. |
| **Broader coverage** | Property-based tests don't try to simulate production scale, but they do exercise a wider variety of data than hand-written tests can. Because thousands of different records are generated automatically, you're far more likely to catch incorrect logic or incorrect assumptions about the data. |
| **Aligns with data quality principles** | The invariants you test in PBT - schema validity, key uniqueness, referential integrity, deterministic transformations - are the same principles that underpin good data-quality practice. In other words, property-based tests let you assert the same or similar rules that you'd monitor in production, but at the code level. This creates a clean bridge between data-quality assurance and automated testing. |
| **Serves as documentation** | Good tests explain why the code exists and what guarantees it provides. Property-based tests take this a step further by expressing the fundamental truths about your data logic. This makes your ETL system self-describing and easier to reason about. |

---

| **Property** | **Description** | **ETL example** |
| --------------------- | --------------------- | --------------------- |
| **Commutativity** | Order doesn’t matter. Doing `A` then `B` gives the same result as `B` then `A`. | Inner joins are commutative: `A ⋈ B = B ⋈ A`. Left/right joins are not, since the order matters. |
| **Associativity** | How you group the same operation doesn't change the result. `A + (B + C)` is the same as `C + (B + A)` | Filter chains are associative: `(filter p ∘ filter q) ∘ filter r = filter p ∘ (filter q ∘ filter r)` |
| **Identity** | An identity is a "do-nothing" value for an operation: doing `x ⊕ identity` leaves `x` unchanged. | A write with an empty source DataFrame makes no changes to the target table. |
| **Idempotence** | Doing the same thing twice is the same as doing it once. | Multiple writes will not change the target table beyond the first write. |
| **Distributivity** | Doing something to the whole is the same as doing it to each part and then putting the parts back together. | Filtering over a union: `filter(X ⊎ Y) = filter(X) ⊎ filter(Y)`. |
| **Permutation invariance** | Changing the order of the inputs does not change the result. | Calculating the latest record per key: the input order doesn’t change which row is chosen. |
| **Monotonicity** | When you add more input (or move a threshold in one direction), a result can only move one way. | Dropping duplicates can only result in the same number of rows or fewer. There can never be more rows. |
| **Homomorphism** | Doing something to the whole is the same as doing it to each part and then combining the results. | Aggregation rollups: a weekly total equals the sum of daily totals. |
| **Absorption / Fusion** | Combining like ops collapses. | **Filter fusion**: `filter(p)∘filter(q) = filter(p∧q)`. · **Projection fusion**: `select(A)∘select(B) = select(A∩B)`. · **Map fusion**: `map(f)∘map(g) = map(f∘g)`. |
| **Compositionality (locality)** | Per-key results depend only on that key’s rows. | **Per-key latest**: `latest_k(X⊎Y) = latest_k(X) ⊎ latest_k(Y)` if `X` and `Y` have disjoint key sets. · **Per-key join caps**: multiplicity bounds hold independently per key. |
| **Conservation (row accounting)** | Inputs transform with explicit adds/drops; nothing vanishes or appears “by magic.” | `out = in − filtered + inserted + updated` per partition/date; numbers reconcile to policy. |
| **Determinism / Purity** | Same inputs & config ⇒ same outputs. | No hidden `now()`/randomness/unstable UDF. Repartitioning doesn’t change results (aside from row order). |
| **Partial inverse (cancellativity, when designed)** | A later step can undo a specific earlier step. | **Staging → dedupe**: `inflate ∘ deflate ≈ id` for reversible encodings; **decode∘encode = id`. (Only if you design it so.) |
| **Order-select determinism** | Chosen row is a pure function of declared order & tie-breakers. | **Latest/earliest per key**: adding strictly older data doesn’t change result; strictly newer replaces deterministically; ties resolve via `(ts, priority, id)`—never random.                                                |
| **Join cardinality cap** | Output multiplicity bounded by design. | **Left 1:1**: `count(child ⟕ parent) = count(child)`; per-child matches ≤ 1. · **Inner 1:1**: `count(A⋈B) ≤ min(count(A), count(B))`. · **Semi/Anti complement**: matched ⊎ unmatched = child.                               |
| **Type/normalization invariance** | Normalizing keys doesn’t change logical joins. | Trimming/casing both sides then joining gives same links as pre-normalized pipeline. |



## Testable ETL Properties

| **Property**                            | **Examples** |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------|
| **Schema contract**                     | Columns, types, and nullability are exactly as expected; no surprise extra/missing columns. |
| **Keys are unique**                     | No duplicate keys after the transformation. |
| **Joins don’t fan-out**                 | An inner join never increases row count. |
| **Duplicate handling is explicit**      | If we dedupe, re-ingesting the same file changes nothing; if we preserve multiplicity, counts double predictably. |
| **Idempotent reruns**                   | Running the same batch twice yields the same output (after canonical sort/hash). |
| **Row accounting holds**                | `out_rows = in_rows − filtered + inserted + updated` per partition/date; nothing silently disappears or multiplies. |
| **Domain & range respected**            | Quantities aren’t negative; status ∈ {“open”, “closed”}; emails match regex; out-of-range values go to a dead-letter table. |
| **Completeness / coverage**             | All expected customers for the day appear; required columns are populated; optional columns stay within allowed null rates. |
| **Late-data policy is enforced**        | Events older than the watermark don’t alter SCD1 records (or they create SCD2 versions) exactly as documented. |
| **Partition/parallelism invariant**     | Repartitioning/shuffling the same input doesn’t change results (aside from row order). |
| **Deterministic, no hidden randomness** | No `now()`/random/unstable UDFs without being part of the contract; same inputs ⇒ same outputs. |
| **Schema evolution stays compatible**   | Adding a nullable column with a default doesn’t break existing readers; renames are applied via a declared mapping. |
| **Type/normalization alignment**        | Join keys are trimmed/cased consistently; `customer_id` types match on both sides. |
| **Semi/anti semantics are correct**     | `left_semi` returns exactly the set of orders that have a customer; `left_anti` returns exactly those without. |


| Property Type | Example Properties |
|-----------|--------------------|
| **Schema contracts** | The output has the expected columns, types, nullability, precision/scale, with no extra/missing columns. |
| **Key uniqueness** | Natural/primary keys are unique after a transformation, with deterministic dedupe if needed. | 
| **Joins** | Joins preserve the intended cardinality. For example, an inner join never increases row count. |
| **Aggregation invariants** | Aggregations, groupings, or rollups produce stable results regardless of how data is chunked, partitioned, or ordered. Partial aggregations followed by unions are equivalent to a single global aggregation. |
| **Values within allowed ranges/categories** | Values fall within valid, expected ranges or enumerations. Outliers, invalid categories, and impossible values (e.g. negative quantities or future timestamps) are handled appropriately. |
| **Idempotence** | Running the same transformation multiple times with identical input yields the same result. Duplicate ingestion or reruns do not introduce new or inconsistent data. |
| **Temporal and sequential correctness** | Any ordering, windowing, or stateful logic behaves deterministically across time. For example, "latest per key", "earliest event", or "rolling window" rules always produce consistent, non-contradictory outcomes. |
| **Completeness and presence** | All mandatory fields and expected entities appear in the output. Optional fields obey nullability rules, and missing data is flagged or filled according to defined policy. |

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

