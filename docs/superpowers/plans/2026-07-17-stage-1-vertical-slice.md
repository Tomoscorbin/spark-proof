# Stage 1 Vertical Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the broken `RangeField` migration with one working end-to-end path: `sp.integer(...)` → private `ValueSpec` → `@sp.data_frame(columns=...)` → Hypothesis-generated rows → real PySpark DataFrame in the user's test, with shrinking intact.

**Architecture:** Public constructor functions return a private frozen `ValueSpec` (strategy + Spark type + nullability). The `@data_frame` decorator builds a private `DataFrameSpec` (StructType + bounded rows strategy), and a Spark-free runtime module does the pytest/Hypothesis signature surgery so the generated DataFrame arrives as the test's first parameter while other fixtures keep working. This follows `docs/` architecture proposal stage 1 (§17) with amendments from review: corrected integer limits, slimmer `DataFrameSpec`, no faker dependency.

**Tech Stack:** Python 3.14 (verified working), pyspark 4.0.0 (SparkSession + `createDataFrame(list[dict], schema=StructType)` + empty-DataFrame path all smoke-tested on this machine), hypothesis 6.142.1, pytest 8.4.2, poetry with in-project venv, Java 17 present.

## Global Constraints

- Public API after this plan is exactly `sp.integer` and `sp.data_frame` — nothing else exported.
- Naming from the proposal, verbatim: decorator kwargs `columns`, `min_rows`, `max_rows`, `spark_fixture`; constructor kwargs `min_value`, `max_value`. The decorator parameter is `columns=`, NOT `schema=`.
- Dependency rules (proposal §12): nothing under `src/` imports pytest; `_internal/runtime.py` must not import pyspark; the decorator contains no scalar type logic.
- Dependencies: `hypothesis==6.142.1`, `pytest==8.4.2`, `pyspark==4.0.0`. `faker` is removed and must not come back. No new dependencies.
- Spark IntegerType bounds are `-2_147_483_648 .. 2_147_483_647` (the old `limits.py` had min = `-max`, off by one — do not copy it).
- Conventional commit messages. Work happens on the current worktree branch (`claude/heuristic-meninsky-b2e23d`), never on `main`. No `Co-authored-by` trailers.
- Error messages from validation must name the public call the user made (e.g. start with `integer:` or `data_frame:`).
- The signature-surgery mechanism in Task 2 was spiked and proven against these exact library versions (6 tests, including shrink-to-minimal). Key mechanic discovered in the spike: Hypothesis calls the inner function with fixture arguments **positionally**, so the inner function must accept `*args` and re-bind them against the advertised `inspect.Signature`. Do not "simplify" it to `**kwargs`-only — that fails with `TypeError: raw() takes 0 positional arguments`.

---

### Task 1: Tear down the dead migration and settle the environment

The package currently does not import: `api.py` and `domain/range.py` import strategy classes that exist only as comments, `__init__.py` is empty, the decorator is fully commented out, and `tests/core` tests methods that don't exist. Nothing here is load-bearing. Delete it all; git history preserves everything (including `assertions/`, which the proposal defers to post-MVP).

**Files:**

- Delete: `src/spark_proof/api.py`, `src/spark_proof/core/` (entire dir), `src/spark_proof/domain/` (entire dir), `src/spark_proof/decorators/` (entire dir), `src/spark_proof/assertions/` (entire dir), `examples/` (entire dir), `tests/core/` (entire dir), `tests/decorators/` (entire dir)
- Keep unchanged: `src/spark_proof/__init__.py` (empty), `tests/__init__.py`, `tests/conftest.py` (the session-scoped `spark` fixture — reused as-is in Task 5), `docs/pbt_for_data_pipelines.md`
- Modify: `pyproject.toml` (remove faker), `.gitignore` (add `.venv/`)

**Interfaces:**

- Consumes: nothing.
- Produces: an importable empty `spark_proof` package, a populated `.venv`, and a green (empty) test run for later tasks to build on.

- [ ] **Step 1: Delete the dead code**

```bash
git rm -r src/spark_proof/api.py src/spark_proof/core src/spark_proof/domain \
  src/spark_proof/decorators src/spark_proof/assertions examples tests/core tests/decorators
```

- [ ] **Step 2: Remove faker from pyproject.toml**

In `pyproject.toml`, change the dependencies list from:

```toml
dependencies = [
  "hypothesis==6.142.1",
  "faker==37.11.0",
  "pytest==8.4.2",
  "pyspark==4.0.0",
]
```

to:

```toml
dependencies = [
  "hypothesis==6.142.1",
  "pytest==8.4.2",
  "pyspark==4.0.0",
]
```

- [ ] **Step 3: Ignore the venv**

Append to `.gitignore`:

```gitignore
# Virtualenv
.venv/
```

(An empty `.venv/` already sits untracked in this worktree from an earlier `poetry run`; the next step populates it.)

- [ ] **Step 4: Re-lock and install**

```bash
poetry lock && poetry install
```

Expected: lock succeeds without faker; install completes (wheels are already in poetry's cache from the main checkout, so this is fast).

- [ ] **Step 5: Verify the package imports and the suite is empty-green**

```bash
poetry run python -c "import spark_proof; print('import ok')"
poetry run pytest -q
```

Expected: `import ok`, then pytest reports `no tests ran` (exit code 5 — that is correct here, not an error).

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore: remove incomplete RangeField migration, drop faker"
```

---

### Task 2: Spark-free runtime — signature surgery for `@data_frame`

The riskiest piece, already de-risked by a spike. `wrap_test` takes the user's test function, a rows strategy, and a `build_input` callback; it returns a function pytest can collect where: the generated value arrives as the test's first parameter, remaining parameters keep resolving as pytest fixtures, a user-declared `request` passes through, and Hypothesis shrinking works. No pyspark imports anywhere in this task.

**Files:**

- Create: `src/spark_proof/_internal/__init__.py` (empty file)
- Create: `src/spark_proof/_internal/runtime.py`
- Test: `tests/test_runtime.py`

**Interfaces:**

- Consumes: nothing from other tasks.
- Produces: `Rows = list[dict[str, Any]]` and `wrap_test(test_function: Callable[..., None], rows_strategy: SearchStrategy[Rows], build_input: Callable[[Any, Rows], Any]) -> Callable[..., None]` — Task 6 calls this. `build_input` receives pytest's `request` object and the generated rows, returns the value injected as the test's first argument.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_runtime.py`:

```python
import inspect

import pytest
from hypothesis import strategies as st

from spark_proof._internal.runtime import wrap_test

ROWS = st.lists(
    st.fixed_dictionaries({"x": st.integers(0, 10)}), min_size=0, max_size=5
)


def fake_build(request, rows):
    return ("FAKE_DF", rows)


# The generated value arrives as the first parameter.
def _receives_value(df):
    tag, rows = df
    assert tag == "FAKE_DF"
    assert all(0 <= r["x"] <= 10 for r in rows)


test_receives_value = wrap_test(_receives_value, ROWS, fake_build)


# Ordinary pytest fixtures beyond the first parameter keep working.
def _keeps_fixture(df, tmp_path):
    assert tmp_path.exists()
    assert df[0] == "FAKE_DF"


test_keeps_fixture = wrap_test(_keeps_fixture, ROWS, fake_build)


# A user-declared `request` parameter is passed through, not swallowed.
def _passes_request(df, request):
    assert request.node.name == "test_passes_request"


test_passes_request = wrap_test(_passes_request, ROWS, fake_build)


def test_signature_advertised_for_pytest():
    # pytest must see (request, tmp_path): df param gone, rows stripped by @given
    assert list(inspect.signature(test_keeps_fixture).parameters) == [
        "request",
        "tmp_path",
    ]


def test_test_function_without_parameters_is_rejected():
    def t():
        pass

    with pytest.raises(TypeError, match="first parameter"):
        wrap_test(t, ROWS, fake_build)


def test_reserved_rows_parameter_name_is_rejected():
    def t(df, rows):
        pass

    with pytest.raises(TypeError, match="reserved"):
        wrap_test(t, ROWS, fake_build)


def test_failures_shrink_to_the_minimal_counterexample(request):
    seen = []

    def build(req, rows):
        seen.append(rows)
        return rows

    def prop(rows_value):
        assert not any(r["x"] >= 3 for r in rows_value)

    wrapped = wrap_test(prop, ROWS, build)
    with pytest.raises(AssertionError):
        wrapped(request=request)
    # Hypothesis replays the shrunk example last: one row, at the boundary.
    assert seen[-1] == [{"x": 3}]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
poetry run pytest tests/test_runtime.py -q
```

Expected: collection error — `ModuleNotFoundError: No module named 'spark_proof._internal'`.

- [ ] **Step 3: Implement the runtime**

Create empty `src/spark_proof/_internal/__init__.py`, then `src/spark_proof/_internal/runtime.py`:

```python
"""pytest/Hypothesis coordination. Must stay free of pyspark imports.

A decorated test needs three parties to agree on one function signature:
pytest (to resolve fixtures), Hypothesis (to inject generated rows), and
the user's test (which declares the DataFrame as its first parameter).
`wrap_test` advertises an explicit Signature — (request, <user fixtures>,
rows) — and re-binds the actual call against it, because Hypothesis
passes fixture arguments positionally.
"""

import inspect
from typing import Any, Callable

from hypothesis import HealthCheck, Phase, given, note, settings
from hypothesis.strategies import SearchStrategy

Rows = list[dict[str, Any]]

_SETTINGS = settings(
    deadline=None,  # Spark calls are far too slow for Hypothesis's default deadline
    phases=(Phase.reuse, Phase.generate, Phase.shrink),  # Phase.explain re-runs slowly
    suppress_health_check=(
        HealthCheck.too_slow,
        # The spark fixture is session-scoped, but passed-through user
        # fixtures (e.g. tmp_path) are function-scoped and shared across
        # examples; without this Hypothesis refuses to run them at all.
        # Revisit in stage 5 diagnostics hardening.
        HealthCheck.function_scoped_fixture,
    ),
)


def wrap_test(
    test_function: Callable[..., None],
    rows_strategy: SearchStrategy[Rows],
    build_input: Callable[[Any, Rows], Any],
) -> Callable[..., None]:
    parameters = list(inspect.signature(test_function).parameters.values())
    if not parameters:
        raise TypeError(
            f"data_frame: test function '{test_function.__name__}' must declare a "
            "first parameter to receive the generated DataFrame"
        )
    fixture_parameters = parameters[1:]
    fixture_names = {p.name for p in fixture_parameters}
    if "rows" in fixture_names:
        raise TypeError(
            f"data_frame: '{test_function.__name__}' declares a parameter named "
            "'rows', which is reserved for the generated examples"
        )
    user_wants_request = "request" in fixture_names

    advertised = []
    if not user_wants_request:
        advertised.append(
            inspect.Parameter("request", inspect.Parameter.POSITIONAL_OR_KEYWORD)
        )
    advertised.extend(fixture_parameters)
    advertised.append(inspect.Parameter("rows", inspect.Parameter.KEYWORD_ONLY))
    advertised_signature = inspect.Signature(advertised)

    def raw(*args: Any, **kwargs: Any) -> None:
        # Hypothesis passes fixture arguments positionally; recover names by
        # binding against the signature we advertised to it and to pytest.
        arguments = dict(advertised_signature.bind(*args, **kwargs).arguments)
        rows = arguments.pop("rows")
        request = (
            arguments["request"] if user_wants_request else arguments.pop("request")
        )
        value = build_input(request, rows)
        note(f"Generated rows (shrunk minimal example on failure): {rows!r}")
        test_function(value, **arguments)

    raw.__signature__ = advertised_signature  # type: ignore[attr-defined]
    raw.__name__ = test_function.__name__

    wrapped = _SETTINGS(given(rows=rows_strategy)(raw))
    wrapped.__name__ = test_function.__name__
    wrapped.__module__ = test_function.__module__
    wrapped.__doc__ = test_function.__doc__
    return wrapped
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
poetry run pytest tests/test_runtime.py -q
```

Expected: `7 passed` (fast — no Spark involved).

- [ ] **Step 5: Commit**

```bash
git add src/spark_proof/_internal tests/test_runtime.py
git commit -m "feat: add spark-free test runtime with fixture-preserving signature surgery"
```

---

### Task 3: `ValueSpec` and the public `integer()` constructor

First public generator. `integer()` validates its arguments eagerly (naming the public call in every error) and returns a frozen `ValueSpec`. Also ports the type-domain constants from the deleted `core/limits.py` — with the off-by-one minimums corrected — and creates the reusable bounds validator that stage 2 generators will share (this is `RangeConstraint` folded into functions, per proposal §14.3).

**Files:**

- Create: `src/spark_proof/_internal/limits.py`
- Create: `src/spark_proof/_internal/validation.py`
- Create: `src/spark_proof/_internal/models.py`
- Create: `src/spark_proof/values.py`
- Modify: `src/spark_proof/__init__.py`
- Test: `tests/test_values.py`

**Interfaces:**

- Consumes: nothing from other tasks.
- Produces: `ValueSpec` frozen dataclass with fields `strategy: SearchStrategy[Any]`, `spark_type: pyspark.sql.types.DataType`, `nullable: bool` (Tasks 4 and 6 consume it); public `integer(min_value: int | None = None, max_value: int | None = None) -> ValueSpec` exported as `spark_proof.integer`; `validation.validate_bounds(min_value, max_value, *, domain_min, domain_max, constructor: str) -> None` (stage 2 reuses it); limits constants `INT16_MIN/MAX`, `INT32_MIN/MAX`, `INT64_MIN/MAX`, `FLOAT32_MIN/MAX`, `FLOAT64_MIN/MAX`, `DATE_MIN/MAX`, `TIMESTAMP_MIN/MAX`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_values.py`:

```python
import pyspark.sql.types as T  # types only — no SparkSession in this module
import pytest
from hypothesis import given

import spark_proof as sp


def test_integer_carries_spark_integer_type_and_is_not_nullable():
    spec = sp.integer(min_value=0, max_value=10)

    assert spec.spark_type == T.IntegerType()
    assert spec.nullable is False


def test_generated_values_stay_within_custom_bounds():
    spec = sp.integer(min_value=-5, max_value=5)

    @given(spec.strategy)
    def check(value):
        assert isinstance(value, int)
        assert -5 <= value <= 5

    check()


def test_single_point_domain_generates_that_value():
    spec = sp.integer(min_value=7, max_value=7)

    @given(spec.strategy)
    def check(value):
        assert value == 7

    check()


def test_defaults_cover_the_full_spark_integer_domain():
    # Spark IntegerType is 32-bit signed; endpoints must be legal arguments.
    spec = sp.integer(min_value=-2_147_483_648, max_value=2_147_483_647)
    assert spec.spark_type == T.IntegerType()
    # And the no-argument form accepts the same domain implicitly.
    sp.integer()


def test_min_above_max_is_rejected_naming_the_public_call():
    with pytest.raises(ValueError, match="integer"):
        sp.integer(min_value=1, max_value=0)


def test_bounds_below_spark_integer_domain_are_rejected():
    with pytest.raises(ValueError, match="integer"):
        sp.integer(min_value=-2_147_483_649)


def test_bounds_above_spark_integer_domain_are_rejected():
    with pytest.raises(ValueError, match="integer"):
        sp.integer(max_value=2_147_483_648)


def test_booleans_are_rejected_even_though_bool_subclasses_int():
    with pytest.raises(TypeError, match="integer"):
        sp.integer(min_value=True)


def test_non_integers_are_rejected():
    with pytest.raises(TypeError, match="integer"):
        sp.integer(max_value=1.5)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
poetry run pytest tests/test_values.py -q
```

Expected: collection error — `ImportError: cannot import name 'integer'` (or `AttributeError: module 'spark_proof' has no attribute 'integer'`).

- [ ] **Step 3: Implement limits, validation, model, and constructor**

Create `src/spark_proof/_internal/limits.py` (ported from the deleted `core/limits.py`; integer minimums corrected — the old file negated the max, excluding the true bottom value of each domain):

```python
import datetime as dt

INT16_MIN, INT16_MAX = -32_768, 32_767
INT32_MIN, INT32_MAX = -2_147_483_648, 2_147_483_647
INT64_MIN, INT64_MAX = -9_223_372_036_854_775_808, 9_223_372_036_854_775_807
FLOAT32_MAX = 3.4028234663852886e38
FLOAT32_MIN = -FLOAT32_MAX
FLOAT64_MAX = 1.7976931348623157e308
FLOAT64_MIN = -FLOAT64_MAX
DATE_MIN = dt.date.min
DATE_MAX = dt.date.max
TIMESTAMP_MIN = dt.datetime.min
TIMESTAMP_MAX = dt.datetime.max
```

Create `src/spark_proof/_internal/validation.py`:

```python
from typing import Any


def validate_bounds(
    min_value: Any,
    max_value: Any,
    *,
    domain_min: Any,
    domain_max: Any,
    constructor: str,
) -> None:
    """Reject an unusable [min_value, max_value] range, naming the public call."""
    if min_value > max_value:
        raise ValueError(
            f"{constructor}: min_value must be <= max_value "
            f"(got {min_value} > {max_value})"
        )
    if min_value < domain_min:
        raise ValueError(
            f"{constructor}: min_value {min_value} is below the smallest "
            f"legal value {domain_min}"
        )
    if max_value > domain_max:
        raise ValueError(
            f"{constructor}: max_value {max_value} is above the largest "
            f"legal value {domain_max}"
        )
```

Create `src/spark_proof/_internal/models.py`:

```python
from dataclasses import dataclass
from typing import Any

import pyspark.sql.types as T
from hypothesis.strategies import SearchStrategy


@dataclass(frozen=True, slots=True)
class ValueSpec:
    """Private pairing of a Hypothesis strategy with its Spark type.

    Users never see this class; public constructor functions return it.
    """

    strategy: SearchStrategy[Any]
    spark_type: T.DataType
    nullable: bool
```

Create `src/spark_proof/values.py`:

```python
import hypothesis.strategies as st
import pyspark.sql.types as T

from spark_proof._internal.limits import INT32_MAX, INT32_MIN
from spark_proof._internal.models import ValueSpec
from spark_proof._internal.validation import validate_bounds


def integer(
    min_value: int | None = None, max_value: int | None = None
) -> ValueSpec:
    """Generate values for a Spark IntegerType (32-bit signed) column."""
    lo = INT32_MIN if min_value is None else min_value
    hi = INT32_MAX if max_value is None else max_value
    for label, value in (("min_value", lo), ("max_value", hi)):
        if isinstance(value, bool):
            raise TypeError(
                f"integer: {label} must be an int, not bool (got {value!r})"
            )
        if not isinstance(value, int):
            raise TypeError(
                f"integer: {label} must be an int (got {type(value).__name__})"
            )
    validate_bounds(
        lo, hi, domain_min=INT32_MIN, domain_max=INT32_MAX, constructor="integer"
    )
    return ValueSpec(
        strategy=st.integers(min_value=lo, max_value=hi),
        spark_type=T.IntegerType(),
        nullable=False,
    )
```

Replace the (empty) `src/spark_proof/__init__.py` content with:

```python
from spark_proof.values import integer

__all__ = ["integer"]
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
poetry run pytest tests/test_values.py tests/test_runtime.py -q
```

Expected: `16 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/spark_proof tests/test_values.py
git commit -m "feat: add ValueSpec model and public integer() constructor"
```

---

### Task 4: `DataFrameSpec` — schema and bounded rows strategy from a columns mapping

Converts the public `columns` mapping into the private generation plan: an ordered `StructType` and a `st.lists(st.fixed_dictionaries(...))` rows strategy. Deliberately slim — schema and strategy only; both are derived once here rather than carried redundantly (review amendment to proposal §9.2).

**Files:**

- Modify: `src/spark_proof/_internal/models.py`
- Test: `tests/test_data_frame_spec.py`

**Interfaces:**

- Consumes: `ValueSpec` from Task 3.
- Produces: `DataFrameSpec` frozen dataclass with fields `schema: pyspark.sql.types.StructType`, `rows_strategy: SearchStrategy[Rows]`; `build_data_frame_spec(*, columns: Mapping[str, ValueSpec], min_rows: int, max_rows: int) -> DataFrameSpec`. Task 6 consumes both.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_data_frame_spec.py` (this file tests an internal seam directly; the public decorator path is covered black-box in Task 6):

```python
import pyspark.sql.types as T
import pytest
from hypothesis import given

import spark_proof as sp
from spark_proof._internal.models import build_data_frame_spec


def _columns():
    return {
        "a": sp.integer(min_value=0, max_value=3),
        "b": sp.integer(min_value=-1, max_value=1),
    }


def test_schema_preserves_column_insertion_order_and_flags():
    spec = build_data_frame_spec(columns=_columns(), min_rows=0, max_rows=5)

    assert spec.schema == T.StructType(
        [
            T.StructField("a", T.IntegerType(), False),
            T.StructField("b", T.IntegerType(), False),
        ]
    )


def test_generated_rows_respect_bounds_and_column_domains():
    spec = build_data_frame_spec(columns=_columns(), min_rows=1, max_rows=4)

    @given(spec.rows_strategy)
    def check(rows):
        assert 1 <= len(rows) <= 4
        for row in rows:
            assert list(row) == ["a", "b"]
            assert 0 <= row["a"] <= 3
            assert -1 <= row["b"] <= 1

    check()


def test_zero_rows_bound_generates_only_empty_row_lists():
    spec = build_data_frame_spec(columns=_columns(), min_rows=0, max_rows=0)

    @given(spec.rows_strategy)
    def check(rows):
        assert rows == []

    check()


def test_empty_columns_mapping_is_rejected():
    with pytest.raises(ValueError, match="data_frame"):
        build_data_frame_spec(columns={}, min_rows=0, max_rows=5)


def test_negative_min_rows_is_rejected():
    with pytest.raises(ValueError, match="data_frame"):
        build_data_frame_spec(columns=_columns(), min_rows=-1, max_rows=5)


def test_min_rows_above_max_rows_is_rejected():
    with pytest.raises(ValueError, match="data_frame"):
        build_data_frame_spec(columns=_columns(), min_rows=6, max_rows=5)


def test_non_string_column_name_is_rejected():
    with pytest.raises(ValueError, match="data_frame"):
        build_data_frame_spec(
            columns={1: sp.integer()}, min_rows=0, max_rows=5
        )
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
poetry run pytest tests/test_data_frame_spec.py -q
```

Expected: collection error — `ImportError: cannot import name 'build_data_frame_spec'`.

- [ ] **Step 3: Implement DataFrameSpec and its builder**

Append to `src/spark_proof/_internal/models.py` (add `Mapping` to the `typing` import and `hypothesis.strategies as st` to the imports):

```python
@dataclass(frozen=True, slots=True)
class DataFrameSpec:
    """Private generation plan: explicit Spark schema + bounded rows strategy."""

    schema: T.StructType
    rows_strategy: SearchStrategy[list[dict[str, Any]]]


def build_data_frame_spec(
    *, columns: Mapping[str, ValueSpec], min_rows: int, max_rows: int
) -> DataFrameSpec:
    if not columns:
        raise ValueError("data_frame: columns must contain at least one column")
    for name in columns:
        if not isinstance(name, str) or not name:
            raise ValueError(
                f"data_frame: column names must be non-empty strings (got {name!r})"
            )
    if min_rows < 0:
        raise ValueError(f"data_frame: min_rows must be >= 0 (got {min_rows})")
    if min_rows > max_rows:
        raise ValueError(
            f"data_frame: min_rows must be <= max_rows (got {min_rows} > {max_rows})"
        )

    schema = T.StructType(
        [
            T.StructField(name, spec.spark_type, spec.nullable)
            for name, spec in columns.items()
        ]
    )
    row_strategy = st.fixed_dictionaries(
        {name: spec.strategy for name, spec in columns.items()}
    )
    rows_strategy = st.lists(row_strategy, min_size=min_rows, max_size=max_rows)
    return DataFrameSpec(schema=schema, rows_strategy=rows_strategy)
```

The final import block of `models.py` should read:

```python
from dataclasses import dataclass
from typing import Any, Mapping

import hypothesis.strategies as st
import pyspark.sql.types as T
from hypothesis.strategies import SearchStrategy
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
poetry run pytest tests/test_data_frame_spec.py -q
```

Expected: `7 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/spark_proof/_internal/models.py tests/test_data_frame_spec.py
git commit -m "feat: build DataFrameSpec (schema + bounded rows strategy) from columns mapping"
```

---

### Task 5: Spark materialisation

One focused operation: `spark.createDataFrame(rows, schema=...)`, wrapped so that when Spark rejects a generated value the error shows the schema and the offending rows (proposal §9.3). First task that needs a live SparkSession — the existing session-scoped `spark` fixture in `tests/conftest.py` is reused unchanged. Expect ~15–30s of one-off session startup when running these tests.

**Files:**

- Create: `src/spark_proof/_internal/materialisation.py`
- Test: `tests/test_materialisation.py`
- Keep unchanged: `tests/conftest.py`

**Interfaces:**

- Consumes: `Rows` type shape from Task 2 (plain `list[dict[str, Any]]`).
- Produces: `materialise(spark: SparkSession, schema: StructType, rows: list[dict[str, Any]]) -> DataFrame` and `MaterialisationError(Exception)`. Task 6 consumes `materialise`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_materialisation.py`:

```python
import pyspark.sql.types as T
import pytest

from spark_proof._internal.materialisation import MaterialisationError, materialise

SCHEMA = T.StructType(
    [
        T.StructField("a", T.IntegerType(), False),
        T.StructField("b", T.IntegerType(), False),
    ]
)


def test_rows_materialise_with_values_and_column_order_intact(spark):
    df = materialise(spark, SCHEMA, [{"a": 1, "b": -1}, {"a": 2, "b": 0}])

    assert df.columns == ["a", "b"]
    assert [(r["a"], r["b"]) for r in df.collect()] == [(1, -1), (2, 0)]


def test_empty_rows_produce_an_empty_dataframe_with_the_full_schema(spark):
    df = materialise(spark, SCHEMA, [])

    assert df.count() == 0
    assert df.schema == SCHEMA


def test_incompatible_value_raises_with_schema_and_rows_context(spark):
    bad_rows = [{"a": "not-an-int", "b": 0}]

    with pytest.raises(MaterialisationError) as excinfo:
        materialise(spark, SCHEMA, bad_rows)

    message = str(excinfo.value)
    assert "a:int" in message  # schema context (simpleString form)
    assert "not-an-int" in message  # offending rows context
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
poetry run pytest tests/test_materialisation.py -q
```

Expected: collection error — `ModuleNotFoundError: No module named 'spark_proof._internal.materialisation'`.

- [ ] **Step 3: Implement materialisation**

Create `src/spark_proof/_internal/materialisation.py`:

```python
from typing import Any

import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession


class MaterialisationError(Exception):
    """Spark rejected generated rows during DataFrame creation."""


def materialise(
    spark: SparkSession, schema: T.StructType, rows: list[dict[str, Any]]
) -> DataFrame:
    try:
        return spark.createDataFrame(rows, schema=schema)
    except Exception as error:
        raise MaterialisationError(
            "Spark rejected the generated rows.\n"
            f"  schema: {schema.simpleString()}\n"
            f"  rows: {rows!r}\n"
            f"  cause: {error}"
        ) from error
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
poetry run pytest tests/test_materialisation.py -q
```

Expected: `3 passed` (slow first run while the SparkSession boots; Spark's chatter on stderr is normal).

- [ ] **Step 5: Commit**

```bash
git add src/spark_proof/_internal/materialisation.py tests/test_materialisation.py
git commit -m "feat: materialise generated rows as Spark DataFrames with error context"
```

---

### Task 6: The public `@data_frame` decorator

Wires everything: build the `DataFrameSpec` eagerly at decoration time (so invalid declarations fail immediately, not at test run), resolve the SparkSession fixture by name, materialise, and hand off to `wrap_test`. The decorator itself contains zero type or strategy logic. Integration tests port the three behaviours from the deleted `tests/decorators/test_data_frame.py` onto the new API and add fixture preservation, custom fixture names, and the missing-fixture error.

**Files:**

- Create: `src/spark_proof/data_frames.py`
- Modify: `src/spark_proof/__init__.py`
- Test: `tests/test_data_frame_decorator.py`

**Interfaces:**

- Consumes: `build_data_frame_spec` (Task 4), `materialise`/`MaterialisationError` (Task 5), `wrap_test` (Task 2), `ValueSpec` (Task 3).
- Produces: public `data_frame(*, columns: Mapping[str, ValueSpec], min_rows: int = 0, max_rows: int = 50, spark_fixture: str = "spark")` decorator, exported as `spark_proof.data_frame`. This completes the stage-1 public API.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_data_frame_decorator.py`:

```python
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest
from pyspark.sql import DataFrame

import spark_proof as sp


@sp.data_frame(columns={"x": sp.integer(min_value=0, max_value=10)}, max_rows=5)
def test_decorated_test_receives_a_real_spark_dataframe(df):
    assert isinstance(df, DataFrame)


@sp.data_frame(columns={"x": sp.integer(min_value=0, max_value=10)}, max_rows=0)
def test_empty_dataframes_are_built_with_the_declared_schema(df):
    assert df.schema == T.StructType([T.StructField("x", T.IntegerType(), False)])
    assert df.count() == 0


@sp.data_frame(columns={"a": sp.integer(min_value=0, max_value=10)}, max_rows=5)
def test_row_counts_and_values_respect_the_declaration(df):
    assert 0 <= df.count() <= 5
    assert df.filter((F.col("a") < 0) | (F.col("a") > 10)).count() == 0


@sp.data_frame(
    columns={"a": sp.integer(min_value=0, max_value=10)}, min_rows=2, max_rows=4
)
def test_min_rows_lower_bound_is_respected(df):
    assert 2 <= df.count() <= 4


@sp.data_frame(columns={"x": sp.integer()}, max_rows=3)
def test_other_pytest_fixtures_still_resolve(df, tmp_path):
    assert tmp_path.exists()
    assert isinstance(df, DataFrame)


@pytest.fixture(scope="session")
def my_spark(spark):
    return spark


@sp.data_frame(
    columns={"x": sp.integer()}, max_rows=3, spark_fixture="my_spark"
)
def test_a_custom_spark_fixture_name_is_honoured(df):
    assert isinstance(df, DataFrame)


def test_missing_spark_fixture_fails_with_a_clear_error(request):
    @sp.data_frame(
        columns={"x": sp.integer()}, max_rows=3, spark_fixture="no_such_fixture"
    )
    def probe(df):
        pass

    with pytest.raises(RuntimeError, match="no_such_fixture"):
        probe(request=request)


def test_invalid_declarations_fail_at_decoration_time_not_test_time():
    with pytest.raises(ValueError, match="data_frame"):

        @sp.data_frame(
            columns={"x": sp.integer()}, min_rows=5, max_rows=1
        )
        def never_runs(df):
            pass
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
poetry run pytest tests/test_data_frame_decorator.py -q
```

Expected: collection error — `AttributeError: module 'spark_proof' has no attribute 'data_frame'`.

- [ ] **Step 3: Implement the decorator**

Create `src/spark_proof/data_frames.py`:

```python
from typing import Any, Callable, Mapping

from spark_proof._internal.materialisation import materialise
from spark_proof._internal.models import ValueSpec, build_data_frame_spec
from spark_proof._internal.runtime import Rows, wrap_test


def data_frame(
    *,
    columns: Mapping[str, ValueSpec],
    min_rows: int = 0,
    max_rows: int = 50,
    spark_fixture: str = "spark",
) -> Callable[[Callable[..., None]], Callable[..., None]]:
    """Run a test against Hypothesis-generated Spark DataFrames.

    The generated DataFrame is passed as the test's first parameter; the
    SparkSession comes from the pytest fixture named by `spark_fixture`.
    """
    spec = build_data_frame_spec(columns=columns, min_rows=min_rows, max_rows=max_rows)

    def decorate(test_function: Callable[..., None]) -> Callable[..., None]:
        def build_input(request: Any, rows: Rows) -> Any:
            spark = _resolve_spark_session(request, spark_fixture)
            return materialise(spark, spec.schema, rows)

        return wrap_test(test_function, spec.rows_strategy, build_input)

    return decorate


def _resolve_spark_session(request: Any, fixture_name: str) -> Any:
    try:
        return request.getfixturevalue(fixture_name)
    except Exception as error:
        raise RuntimeError(
            f"data_frame: could not resolve a SparkSession from the pytest "
            f"fixture '{fixture_name}'. Define a fixture with that name that "
            f"returns a SparkSession, or pass spark_fixture='<your fixture>'."
        ) from error
```

Replace `src/spark_proof/__init__.py` content with:

```python
from spark_proof.data_frames import data_frame
from spark_proof.values import integer

__all__ = ["data_frame", "integer"]
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
poetry run pytest tests/test_data_frame_decorator.py -q
```

Expected: `8 passed`. (Each decorated test runs ~100 Hypothesis examples against Spark, so allow one to a few minutes — it is not hung.)

- [ ] **Step 5: Verify the failure note end-to-end, then remove the demo**

Create a throwaway file `tests/test_shrink_demo.py`:

```python
import spark_proof as sp


@sp.data_frame(columns={"x": sp.integer(min_value=0, max_value=100)}, max_rows=10)
def test_demo_failure_shows_shrunk_rows(df):
    assert df.filter(df.x >= 3).count() == 0
```

Run it and inspect the output:

```bash
poetry run pytest tests/test_shrink_demo.py -q 2>&1 | grep -E "Generated rows|Falsifying"
```

Expected output (this is the stage-1 "minimal failure note" acceptance check):

```
Falsifying example: test_demo_failure_shows_shrunk_rows(
Generated rows (shrunk minimal example on failure): [{'x': 3}]
```

If the shrunk rows show exactly one row with `x` at the boundary value 3, the note works. Then delete the demo:

```bash
rm tests/test_shrink_demo.py
```

(Automated assertion of shrink output is stage 5 scope, per proposal §17; the runtime's shrink behaviour is already asserted without Spark in `tests/test_runtime.py`.)

- [ ] **Step 6: Verify the dependency-direction rules**

```bash
rg -n "import pytest" src/ ; rg -n "pyspark" src/spark_proof/_internal/runtime.py ; echo "clean if both empty"
```

Expected: no matches from either search — nothing in `src/` imports pytest, and the runtime is Spark-free.

- [ ] **Step 7: Commit**

```bash
git add src/spark_proof/data_frames.py src/spark_proof/__init__.py tests/test_data_frame_decorator.py
git commit -m "feat: add public @data_frame decorator completing the vertical slice"
```

---

### Task 7: Truthful README and final verification

The README currently documents an API that does not exist (`sp.string`, `sp.date`, `min_size=`, `assert_one_row_per_key`, `schema=`). Rewrite it to describe exactly what stage 1 ships, with the roadmap named as such. Then run everything.

**Files:**

- Modify: `README.md` (full rewrite)
- Keep: `docs/pbt_for_data_pipelines.md` (still linked)

**Interfaces:**

- Consumes: the public API from Tasks 3 and 6 (`sp.integer`, `sp.data_frame`).
- Produces: documentation matching reality; a fully green suite.

- [ ] **Step 1: Replace README.md with**

````markdown
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
````

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

````

- [ ] **Step 2: Run the full suite and check the tree is clean**

```bash
poetry run pytest -q && git status --short
````

Expected: `34 passed` (7 runtime + 9 values + 7 spec + 3 materialisation + 8 decorator — the Spark-backed decorator tests dominate the runtime, allow a few minutes), then only `README.md` listed as modified.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: rewrite README to match the stage 1 API"
```

---

## Deferred beyond this plan (deliberately)

- Stage 2+ generators (`long`, `double`, `decimal`, dates, `from_values`, `constant`, `nullable=`) — the `validate_bounds`/`limits` seam is ready for them. The §20.4 nullability-default decision and §20.5 `from_values` inference rule must be settled when stage 2 starts.
- `assertions/assert_one_row_per_key` — deleted in Task 1 per proposal §2.3; recoverable from git history if wanted post-MVP.
- Automated assertion on shrink-note output (pytester-based) and richer diagnostics — stage 5.
- `HealthCheck.function_scoped_fixture` suppression is a stage-1 compromise documented in `runtime.py` — revisit in stage 5.
