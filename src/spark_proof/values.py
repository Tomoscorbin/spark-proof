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
