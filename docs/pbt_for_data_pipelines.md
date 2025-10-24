# Property-Based Testing for Data Pipelines

Property-based testing (PBT) is a technique that asserts high-level truths about your code. In contrast to example-based testing, which asserts that specific cases are correct, PBT defines general "properties" (or invariants) that hold up against wide range of data. Instead of handpicking a few example cases and their expected outcome, by stating general rules and testing them against a variety of randomly generated data, PBT aims to prove that these rules are always true, as opposed to showing that a few scenarios are correct. 

In Python, PBT is usually done using [Hypothesis](https://hypothesis.readthedocs.io/en/latest/), a library that generates randomised data for you. Here is a Hypothesis example of a simple property test for a string reverse function:

```python
@given(st.text())
def test_reverse_preserves_length(text):
    result = reverse_string(text)
    assert len(result) == len(text) # the length is unchanged
```

This test tells Hypothesis to generate many different strings (`(@given(st.text()))`), including empty strings and unusual Unicode, and for each one it calls `reverse_string(text)` and checks a single rule: reversing a string must not change its length. There are no hard-coded examples or expected outputs; instead, you’re asserting an invariant across a wide input space, and the test fails as soon as Hypothesis finds any string that breaks that rule.

Property based testing lends itself particularly well to the big data space, where we rarely know the "right" output for every possible input. Real-life data is messy, with missing values, weird characters, duplicates, out-of-order timestamps, new categories you've never seen, and so on. It's impossible to conceive of every possible issue, and even if it were, our tests would be ginormous with all of the boilerplate required for the input/output dataframes. It would be a maintenance nightmare.

PBT is good for data pipelines because it allows us to explore the messy data and edge cases we'd never think of or be able to write by hand.

> If you’re new to property-based testing, [this series by Scott Wlaschin](https://fsharpforfunandprofit.com/series/property-based-testing/) gives a great introduction.
---

## What PBT brings to ETL and big data

| Benefit | Why It Matters in ETL |
|----------|----------------------|
| **Finds edge-case bugs automatically** | With millions or billions of rows, it's impossible to anticipate every edge case manually. Real datasets contain unexpected combinations of nulls, duplicate keys, out-of-order timestamps, strange encodings, unseen categories, etc. PBT explores that space for you by generating hundred or thousands (or more) of diverse input records that simulate the kinds of data pathologies your pipeline might face. This can uncover subtle bugs like mis-handled nulls, join fan-outs, and inconsistent deduplication before they reach production. |
| **Reduces test boilerplate** | Traditional example-based tests require writing explicit input/output tests for every scenario. As the number of cases grows, so does the noise and maintenance overhead. With PBT, data is generated automatically according to schema rules and constraints, keeping your tests clean and focused on behaviour rather than setup. The result is smaller, more readable, more maintainable tests that explore a far broader input space. |
| **Debuggable failures** | When a property fails, the test framework automatically "shrinks" the data to the smallest example that still reproduces the failure. You get a tiny handful of rows that expose the issue. This makes failure analysis fast and keeps test feedback loops tight. |
| **Broader coverage** | Property-based tests don't try to simulate production scale, but they do exercise a wider variety of data than hand-written tests can. Because thousands of different records are generated automatically, you're far more likely to catch incorrect logic or incorrect assumptions about the data. |
| **Aligns with data quality principles** | The invariants you test in PBT - schema validity, key uniqueness, referential integrity, deterministic transformations - are the same principles that underpin good data-quality practice. In other words, property-based tests let you assert the same or similar rules that you'd monitor in production, but at the code level. This creates a clean bridge between data-quality assurance and automated testing. |
| **Serves as documentation** | Good tests explain why the code exists and what guarantees it provides. Property-based tests take this a step further by expressing the fundamental truths about your data logic. This makes your ETL system self-describing and easier to reason about. |

---

## What Properties can you test in data pipelines?
Finding good properties is notoriously hard. Even outside ETL, it is hard to articulate behaviours that should hold for all inputs. To make this easier, PBT practitioners often draw from algebraic laws like commutativity, associativity, and idempotence. These laws also apply to data pipelines, and can be used to express the properties of our pipelines that should always remain true. Below is a table of core algebraic laws and how they translate into testable properties in ETL pipelines.

| **Law** | **Description** | **ETL example** |
| --------------------- | --------------------- | --------------------- |
| **Commutativity** | Order doesn’t matter. Doing `A` then `B` gives the same result as `B` then `A`. | Inner joins are commutative: `A ⋈ B = B ⋈ A`. Left/right joins are not, since the order matters. |
| **Associativity** | How you group the same operation doesn't change the result. `A + (B + C)` is the same as `(A + B) + C` | Filter chains are associative: `(filter p ∘ filter q) ∘ filter r = filter p ∘ (filter q ∘ filter r)` |
| **Identity** | An identity is a "do-nothing" value for an operation: doing `x + identity` leaves `x` unchanged. | Writing an empty source DataFrame to a table acts as an identity - it makes no changes to the target table. Similarly, filtering with a predicate that's always true leaves the dataset unchanged. |
| **Idempotence** | Doing the same thing twice is the same as doing it once. | A write is idempotent if re-running it produces the same state as the first run. Once the data has been written, running the same operation again does not change the target table further. |
| **Distributivity** | Doing something to the whole is the same as doing it to each part and then putting the parts back together. | Filtering distributes over unions: filtering the union of two datasets is equivalent to filtering each one and then uniting the results: `filter(X ∪ Y) = filter(X) ∪ filter(Y)`. |
| **Permutation invariance** | Changing the order of the inputs does not change the result. | Order-insensitive transformations like deduplication or "latest-record-per-key" selection are permutation invariant since the result depends only on data values, not on input ordering. |
| **Monotonicity** | When you add more input (or move a threshold in one direction), a result can only move one way. | Dropping duplicates is monotonic: the output can only have the same number of rows or fewer, never more. Similarly, applying a filter can only reduce results, not increase them. |
| **Homomorphism** | Doing something to the whole is the same as doing it to each part and then combining the results. | Some aggregations are homomorphic: computing totals for subsets (e.g. daily sales) and then summing them produces the same result as aggregating all data at once (e.g. weekly total). |
| **Fusion** | Combining equivalent steps into a single step produces the same result. | Merging consecutive filters is a fusion law: `filter(p) ∘ filter(q)` is equivalent to `filter(p ∧ q)`. |
| **Determinism** | The same inputs always produce the same result. | A transformation is deterministic if it gives consistent results every run. For example, a window function with non-unique ordering columns can be non-deterministic, since record order can change between runs. | 

---
## Data-specific properties
While algebraic laws describe more generally how transformations behave, there are some common data-specific properties that focus on the content and structure of the datasets themselves. 

| **Property**                                 | **Example** |
| -------------------------------------------- | ----------------------------- |
| **Schema contracts**                         | The output schema always matches its declared specification: all columns exist with the correct names, data types, and nullability, with no extra or missing columns. |
| **Key uniqueness**                           | Each natural key or unique identifier appears at most once in the output. |
| **Referential integrity**                    | Foreign keys in the output correspond to valid primary keys in reference datasets. For example, every `customer_id` in `orders` exists in `customers`. |
| **Join cardinality**                         | Joins preserve the expected multiplicity. For example, an inner 1:1 join does not produce join explosion, and a left 1:1 join does not increase the row count beyond the left input. |
| **Key coverage on left joins**               | After a left join, every left key is present in the output. |
| **Primary key uniqueness**                   | Primary keys are unique after deduplication: no group has `count(*)>1` for `primary_key`. |
| **Dedupe stability** | Applying a deduplication twice is the same as applying it once. |
| **As-of uniqueness (SCD2)**                  | For any given key and timestamp, exactly one record is valid (`start ≤ ts < end`). There are never overlapping or missing periods. |
| **Row accounting**                           | The number of rows in the output can be reconciled as `rows_out  =  rows_in  − rows_removed  + rows_inserted`. No silent duplication or loss. |
| **Budget reconciliation**                    | Aggregations reconcile: the sum of each category equals the overall; or group subtotals adds to totals. |
| **Values within allowed ranges/categories**  | Values fall within valid, expected ranges or enumerations. For example, negative quantities, future timestamps, or invalid categories do not exist in the output. |
| **Type-cast round-trip**                     | Casting values to another type and back yields an equivalent value. E.g.: `A → B → A`. |

