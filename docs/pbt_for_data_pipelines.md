# Property-Based Testing for Data Pipelines

## Why Property-Based Testing?

Traditional tests check that **specific examples** work.  
Property-based testing (PBT) checks that **general rules** always hold.

For data engineering, this difference is important.  
Example tests might prove that one sample dataset works, but PBT proves that your logic is correct for *all* datasets within realistic bounds.

> If you’re new to property-based testing, [this series by Scott Wlaschin](https://fsharpforfunandprofit.com/series/property-based-testing/) gives a great introduction.
---

## What PBT brings to ETL and big data

| Benefit | Why It Matters in ETL |
|----------|----------------------|
| **Finds edge-case bugs automatically** | With millions or billions of rows, it's impossible to anticipate every edge case manually. Real datasets contain unexpected combinations of nulls, duplicate keys, out-of-order timestamps, strange encodings, unseen categories, etc. PBT explores that space for you by generating hundred or thousands (or more) of eivetse input records that simulate the kinds of data pathologies your pipeline might face. This can uncover subtle bugs like mis-handled nulls, join fan-outs, and inconsistent deduplication before they reach production. |
| **Reduces test boilerplate** | Traditional example-based tests require writing explicit input/output tests for every scenario. As the number of cases grows, so does the noise and maintenance overhead. With PBT, data is generated automatically according to schema rules and constraints, keeping your tests clean and focused on behaviour rather than setup. The result is smaller, more readable, more maintainable tests that explore a far broader input space. |
| **Debuggable failures** | When a property fails, the test framework automatically "shrinks" the data to the smallest example that still reproduces the failure. You get a tiny handful of rows that expose the issue. This makes failure analysis fast and keeps test feedback loops tight. |
| **Broader coverage** | Property-based tests don't try to simulate production scale, but they do exercise a wider variety of data than hand-written tests can. Because thousands of different records are generated automatically, you're far more likely to catch incorrect logic or incorrect assumptions about the data. |
| **Aligns with data quality principles** | The invariants you test in PBT - schema validity, key uniqueness, referential integrity, deterministic transformations - are the same principles that underpin good data-quality practice. In other words, property-based tests let you assert the same or similar rules that you'd monitor in production, but at the code level. This creates a clean bridge between data-quality assurance and automated testing. |
| **Serves as documentation** | Good tests explain why the code exists and what guarantees it provides. Property-based tests take this a step further by expressing the fundamental truths about your data logic. This makes your ETL system self-describing and easier to reason about. |

## What Properties can you test in data pipelines?
Finding good properties is notoriously hard. Even outside ETL, it is hard to articulate behaviours that should hold for all inputs. To make this easier, PBT practitioners often draw from algebraic laws like commutativity, associativity, and idempotence. These laws also apply to data pipelines, and can be used to express the properties of our pipelines that sould always remain true.

| **Law** | **Description** | **ETL example** |
| --------------------- | --------------------- | --------------------- |
| **Commutativity** | Order doesn’t matter. Doing `A` then `B` gives the same result as `B` then `A`. | Inner joins are commutative: `A ⋈ B = B ⋈ A`. Left/right joins are not, since the order matters. |
| **Associativity** | How you group the same operation doesn't change the result. `A + (B + C)` is the same as `C + (B + A)` | Filter chains are associative: `(filter p ∘ filter q) ∘ filter r = filter p ∘ (filter q ∘ filter r)` |
| **Identity** | An identity is a "do-nothing" value for an operation: doing `x ⊕ identity` leaves `x` unchanged. | A write with an empty source DataFrame makes no changes to the target table. |
| **Idempotence** | Doing the same thing twice is the same as doing it once. | Multiple writes will not change the target table beyond the first write. |
| **Distributivity** | Doing something to the whole is the same as doing it to each part and then putting the parts back together. | Filtering over a union: `filter(X ⊎ Y) = filter(X) ⊎ filter(Y)`. |
| **Permutation invariance** | Changing the order of the inputs does not change the result. | Calculating the latest record per key: the input order doesn’t change which row is chosen. |
| **Monotonicity** | When you add more input (or move a threshold in one direction), a result can only move one way. | Dropping duplicates can only result in the same number of rows or fewer. There can never be more rows. |
| **Homomorphism** | Doing something to the whole is the same as doing it to each part and then combining the results. | Aggregation rollups: a weekly total equals the sum of daily totals. |
| **Fusion** | Combining equivalent steps into a single step produces the same result. | Combining multiple filters into one is equivalent: `filter(p) ∘ filter(q) = filter(p ∧ q)`. |
| **Determinism** | The same inputs always produce the same result. | If the ordering columns in a window function are not unique, the ouptput can change from run to run. | 


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

