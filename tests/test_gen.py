import datetime as dt
from decimal import Decimal

import pytest
import re
from hypothesis.strategies import SearchStrategy
from hypothesis import given, find
import pyspark.sql.types as T

from spark_proof.gen import (
    DATE_MAX,
    DATE_MIN,
    Generator,
    FLOAT32_MAX,
    FLOAT32_MIN,
    FLOAT64_MAX,
    FLOAT64_MIN,
    INT16_MAX,
    INT16_MIN,
    INT32_MAX,
    INT32_MIN,
    INT64_MAX,
    INT64_MIN,
    TIMESTAMP_MAX,
    TIMESTAMP_MIN,
    boolean,
    date,
    decimal,
    double,
    float32,
    integer,
    long,
    short,
    timestamp,
    string,
    string_from_regex,
)


def test_returns_generator_with_integer_strategy_and_spark_type():
    # Given a valid range
    min_value = 1
    max_value = 5

    # When creating the generator
    gen = integer(min_value=min_value, max_value=max_value)

    # Then we get the right structure and types
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, SearchStrategy)
    assert isinstance(gen.spark_type, T.IntegerType)


def test_raises_when_min_greater_than_max():
    # Given an invalid range where min_value > max_value
    min_value = 10
    max_value = 9

    # When / Then
    with pytest.raises(ValueError):
        integer(min_value=min_value, max_value=max_value)


@pytest.mark.parametrize(
    "min_value,max_value",
    [
        (INT32_MIN - 1, 0),  # min underflows int32
        (0, INT32_MAX + 1),  # max overflows int32
        (INT32_MIN - 1, INT32_MAX + 1),  # both invalid
        (INT32_MIN - 1, INT32_MIN - 1),  # equal but invalid
        (INT32_MAX + 1, INT32_MAX + 1),  # equal but invalid
    ],
)
def test_integer_raises_when_bounds_outside_int32(min_value, max_value):
    # Given bounds outside of int32
    # When / Then
    with pytest.raises(ValueError):
        integer(min_value=min_value, max_value=max_value)


@pytest.mark.parametrize(
    "min_value,max_value",
    [
        (INT16_MIN - 1, 0),  # min underflows int16
        (0, INT16_MAX + 1),  # max overflows int16
        (INT16_MIN - 1, INT16_MAX + 1),  # both invalid
        (INT16_MIN - 1, INT16_MIN - 1),  # equal but invalid
        (INT16_MAX + 1, INT16_MAX + 1),  # equal but invalid
    ],
)
def test_short_raises_when_bounds_outside_int16(min_value, max_value):
    # Given bounds outside of int16
    # When / Then
    with pytest.raises(ValueError):
        short(min_value=min_value, max_value=max_value)


@pytest.mark.parametrize(
    "min_value,max_value",
    [
        (INT64_MIN - 1, 0),  # min underflows int64
        (0, INT64_MAX + 1),  # max overflows int64
        (INT64_MIN - 1, INT64_MAX + 1),  # both invalid
        (INT64_MIN - 1, INT64_MIN - 1),  # equal but invalid
        (INT64_MAX + 1, INT64_MAX + 1),  # equal but invalid
    ],
)
def test_long_raises_when_bounds_outside_int64(min_value, max_value):
    # Given bounds outside of int64
    # When / Then
    with pytest.raises(ValueError):
        long(min_value=min_value, max_value=max_value)


def test_returns_generator_with_short_strategy_and_spark_type():
    # Given a valid int16 range
    min_value = -100
    max_value = 100

    # When creating the generator
    gen = short(min_value=min_value, max_value=max_value)

    # Then we get the right structure and types
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, SearchStrategy)
    assert isinstance(gen.spark_type, T.ShortType)


def test_returns_generator_with_long_strategy_and_spark_type():
    # Given a valid int64 range
    min_value = INT64_MIN + 1
    max_value = INT64_MAX - 1

    # When creating the generator
    gen = long(min_value=min_value, max_value=max_value)

    # Then we get the right structure and types
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, SearchStrategy)
    assert isinstance(gen.spark_type, T.LongType)


@pytest.mark.parametrize(
    "min_value,max_value",
    [
        (FLOAT32_MIN * 2, 0.0),  # min underflows float32
        (0.0, FLOAT32_MAX * 2),  # max overflows float32
        (FLOAT32_MIN * 2, FLOAT32_MAX * 2),  # both invalid
    ],
)
def test_float32_raises_when_bounds_outside_float32(min_value, max_value):
    # Given bounds outside of float32
    # When / Then
    with pytest.raises(ValueError):
        float32(min_value=min_value, max_value=max_value)


def test_float32_raises_when_min_greater_than_max():
    # Given an invalid range where min_value > max_value
    min_value = 1.0
    max_value = 0.0

    # When / Then
    with pytest.raises(ValueError):
        float32(min_value=min_value, max_value=max_value)


def test_returns_generator_with_float32_strategy_and_spark_type():
    # Given the default float32 bounds
    # When creating the generator
    gen = float32()

    # Then we get the right structure and types
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, SearchStrategy)
    assert isinstance(gen.spark_type, T.FloatType)


@pytest.mark.parametrize(
    "min_value,max_value",
    [
        (FLOAT64_MIN * 2, 0.0),  # min underflows float64
        (0.0, FLOAT64_MAX * 2),  # max overflows float64
        (FLOAT64_MIN * 2, FLOAT64_MAX * 2),  # both invalid
    ],
)
def test_double_raises_when_bounds_outside_float64(min_value, max_value):
    # Given bounds outside of float64
    # When / Then
    with pytest.raises(ValueError):
        double(min_value=min_value, max_value=max_value)


def test_double_raises_when_min_greater_than_max():
    # Given an invalid range where min_value > max_value
    min_value = 1.0
    max_value = 0.0

    # When / Then
    with pytest.raises(ValueError):
        double(min_value=min_value, max_value=max_value)


def test_returns_generator_with_double_strategy_and_spark_type():
    # Given a valid float64 range
    min_value = -1.0
    max_value = 1.0

    # When creating the generator
    gen = double(min_value=min_value, max_value=max_value)

    # Then we get the right structure and types
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, SearchStrategy)
    assert isinstance(gen.spark_type, T.DoubleType)


def test_decimal_returns_generator_with_decimal_type():
    # Given decimal precision and scale
    precision = 10
    scale = 2

    # When creating the generator
    gen = decimal(precision=precision, scale=scale)

    # Then we get the right structure and types
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, SearchStrategy)
    assert isinstance(gen.spark_type, T.DecimalType)
    assert gen.spark_type == T.DecimalType(precision, scale)


def test_decimal_raises_when_precision_not_positive():
    # Given precision that is not positive
    precision = 0

    # When / Then
    with pytest.raises(ValueError):
        decimal(precision=precision)


def test_decimal_raises_when_scale_exceeds_precision():
    # Given a scale larger than the precision
    precision = 2
    scale = 3

    # When / Then
    with pytest.raises(ValueError):
        decimal(precision=precision, scale=scale)


def test_decimal_raises_when_bounds_outside_precision():
    # Given bounds outside of the representable range for the precision/scale
    precision = 4
    scale = 2
    max_value = Decimal("100.00")

    # When / Then
    with pytest.raises(ValueError):
        decimal(precision=precision, scale=scale, max_value=max_value)


def test_decimal_raises_when_bounds_not_ordered():
    # Given decimal bounds where min_value > max_value
    precision = 4
    scale = 2
    min_value = Decimal("1.00")
    max_value = Decimal("0.00")

    # When / Then
    with pytest.raises(ValueError):
        decimal(
            precision=precision,
            scale=scale,
            min_value=min_value,
            max_value=max_value,
        )


def test_decimal_raises_when_value_has_too_many_decimal_places():
    # Given a bound with more decimal places than allowed
    precision = 6
    scale = 2
    min_value = Decimal("0.001")

    # When / Then
    with pytest.raises(ValueError):
        decimal(precision=precision, scale=scale, min_value=min_value)


def test_decimal_raises_when_min_outside_precision():
    # Given a min bound below the window for DECIMAL(4,2) (window ~ [-99.99, 99.99])
    precision = 4
    scale = 2
    min_value = Decimal("-100.00")

    # When / Then
    with pytest.raises(ValueError):
        decimal(precision=precision, scale=scale, min_value=min_value)


@pytest.mark.parametrize(
    "min_raw,max_raw,scale",
    [
        (-1, 2, 2),  # ints
        (0.1, 0.3, 1),  # floats aligned to one decimal place
        (Decimal("-1.25"), Decimal("3.50"), 2),  # Decimals
    ],
)
def test_decimal_accepts_int_float_and_decimal_when_aligned(min_raw, max_raw, scale):
    # Given aligned bounds in various types
    precision = 6

    # When
    gen = decimal(
        precision=precision, scale=scale, min_value=min_raw, max_value=max_raw
    )

    # Then: all generated values are within bounds and aligned to scale
    step = Decimal(1).scaleb(-scale)
    lo = Decimal(str(min_raw)) if not isinstance(min_raw, Decimal) else min_raw
    hi = Decimal(str(max_raw)) if not isinstance(max_raw, Decimal) else max_raw

    @given(gen.strategy)
    def _prop(v: Decimal):
        assert lo <= v <= hi
        assert v == v.quantize(step)

    _prop()


@pytest.mark.parametrize(
    "bad_bound,scale",
    [
        (0.105, 2),  # float not aligned to 2 dp (requires _to_decimal using str())
        (Decimal("1.001"), 2),  # Decimal not aligned
        (-1.5, 0),  # float fractional with scale 0
    ],
)
def test_decimal_rejects_bounds_when_not_aligned(bad_bound, scale):
    # Given a misaligned bound
    precision = 10

    # When / Then
    with pytest.raises(ValueError):
        decimal(precision=precision, scale=scale, min_value=bad_bound)


def test_decimal_handles_negative_ranges():
    # Given a negative-only range
    precision = 6
    scale = 2
    min_value = Decimal("-5.00")
    max_value = Decimal("-1.00")

    # When
    gen = decimal(
        precision=precision, scale=scale, min_value=min_value, max_value=max_value
    )

    # Then
    step = Decimal("0.01")

    @given(gen.strategy)
    def _prop(v: Decimal):
        assert min_value <= v <= max_value
        assert v == v.quantize(step)

    _prop()


def test_decimal_handles_cross_zero_ranges():
    # Given a range crossing zero
    precision = 6
    scale = 2
    min_value = Decimal("-1.25")
    max_value = Decimal("2.50")

    # When
    gen = decimal(
        precision=precision, scale=scale, min_value=min_value, max_value=max_value
    )

    # Then
    step = Decimal("0.01")

    @given(gen.strategy)
    def _prop(v: Decimal):
        assert min_value <= v <= max_value
        assert v == v.quantize(step)

    _prop()


def test_decimal_default_bounds_cover_type_window_endpoints():
    # Given no user bounds for DECIMAL(4,2)
    precision = 4
    scale = 2
    gen = decimal(precision=precision, scale=scale)

    # Then the strategy should include both endpoints (window [-99.99, 99.99])
    type_min = Decimal("-99.99")
    type_max = Decimal("99.99")

    # Note: if implementation computes window differently, update these two lines
    # TODO: do not tie this to implementation
    found_lo = find(gen.strategy, lambda x: x == type_min)
    found_hi = find(gen.strategy, lambda x: x == type_max)

    assert found_lo == type_min
    assert found_hi == type_max


def test_decimal_raises_on_fractional_bound_when_scale_zero():
    # Given scale 0 and fractional bound
    precision = 10
    scale = 0

    # When / Then
    with pytest.raises(ValueError):
        decimal(precision=precision, scale=scale, min_value=Decimal("1.5"))


def test_decimal_rejects_negative_fractional_alignment():
    # Given misaligned negative bound for scale=2
    precision = 10
    scale = 2

    # When / Then
    with pytest.raises(ValueError):
        decimal(precision=precision, scale=scale, min_value=Decimal("-1.234"))


def test_boolean_returns_generator_with_boolean_type():
    # Given no explicit configuration
    # When creating the generator
    gen = boolean()

    # Then we get the right structure and types
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, SearchStrategy)
    assert isinstance(gen.spark_type, T.BooleanType)


def test_date_returns_generator_with_date_type():
    # Given a valid date range
    min_value = DATE_MIN
    max_value = DATE_MAX

    # When creating the generator
    gen = date(min_value=min_value, max_value=max_value)

    # Then we get the right structure and types
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, SearchStrategy)
    assert isinstance(gen.spark_type, T.DateType)


def test_date_raises_when_bounds_not_ordered():
    # Given date bounds where min_value > max_value
    min_value = DATE_MAX
    max_value = DATE_MIN

    # When / Then
    with pytest.raises(ValueError):
        date(min_value=min_value, max_value=max_value)


def test_timestamp_returns_generator_with_timestamp_type():
    # Given a valid timestamp range
    min_value = TIMESTAMP_MIN
    max_value = TIMESTAMP_MAX

    # When creating the generator
    gen = timestamp(min_value=min_value, max_value=max_value)

    # Then we get the right structure and types
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, SearchStrategy)
    assert isinstance(gen.spark_type, T.TimestampType)


def test_timestamp_raises_when_bounds_not_ordered():
    # Given timestamp bounds where min_value > max_value
    min_value = dt.datetime(2020, 1, 2)
    max_value = dt.datetime(2020, 1, 1)

    # When / Then
    with pytest.raises(ValueError):
        timestamp(min_value=min_value, max_value=max_value)


def test_timestamp_raises_when_timezone_aware_values_provided():
    # Given timezone-aware bounds
    tz_aware_min = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
    tz_aware_max = dt.datetime(2020, 1, 2, tzinfo=dt.timezone.utc)

    # When / Then
    with pytest.raises(ValueError):
        timestamp(min_value=tz_aware_min, max_value=tz_aware_max)


def test_string_returns_generator_with_string_type():
    # Given valid bounds
    gen = string(min_size=0, max_size=5)

    # Then wrapper and Spark type are correct
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, SearchStrategy)
    assert isinstance(gen.spark_type, T.StringType)


def test_string_raises_when_min_size_negative():
    # Given an invalid lower bound
    # When / Then
    with pytest.raises(ValueError):
        string(min_size=-1, max_size=0)


def test_string_raises_when_max_less_than_min():
    # Given reversed bounds
    # When / Then
    with pytest.raises(ValueError):
        string(min_size=5, max_size=4)


def test_string_inclusive_length_bounds():
    # Given inclusive bounds
    gen = string(min_size=2, max_size=4)

    # Then endpoints are attainable
    assert len(find(gen.strategy, lambda s: len(s) == 2)) == 2
    assert len(find(gen.strategy, lambda s: len(s) == 4)) == 4


def test_string_singleton_length():
    # Given min == max
    gen = string(min_size=3, max_size=3)

    # Then all draws have exact length
    @given(gen.strategy)
    def _prop(s: str):
        assert len(s) == 3
        assert isinstance(s, str)

    _prop()


def test_string_zero_length_only_generates_empty():
    # Given zero-length only
    gen = string(min_size=0, max_size=0)

    # Then empty string is the only value
    s = gen.strategy.example()  # TODO: should we use exanple() or given()?
    assert s == ""

    @given(gen.strategy)
    def _prop(x: str):
        assert x == ""

    _prop()


def test_string_unbounded_upper_can_reach_target_length():
    # Given no max_size
    gen = string(min_size=0, max_size=None)

    # Then we can find a string of a specific length (e.g., 10)
    target = 10
    found = find(gen.strategy, lambda s: len(s) == target)
    assert len(found) == target


def test_string_from_regex_returns_generator_with_string_type():
    # Given a simple pattern
    gen = string_from_regex(pattern=r"[A-Z]{2}\d{3}", full_match=True)

    # Then wrapper and Spark type are correct
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, SearchStrategy)
    assert isinstance(gen.spark_type, T.StringType)


def test_string_from_regex_search_mode_contains_match():
    # Given search (not full-match)
    gen = string_from_regex(pattern=r"\d+", full_match=False)

    # Then result contains a matching substring
    pat = re.compile(r"\d+")

    @given(gen.strategy)
    def _prop(s: str):
        assert pat.search(s)

    _prop()


def test_string_from_regex_dot_fullmatch_produces_single_codepoint():
    # Given '.' with full-match (equivalent to anchoring \A.\Z)
    gen = string_from_regex(pattern=r".", full_match=True)

    # Then every draw is exactly one codepoint
    @given(gen.strategy)
    def _prop(s: str):
        assert len(s) == 1

    _prop()


def test_string_from_regex_nullable_pattern_can_be_empty():
    # Given a pattern that can match empty
    gen = string_from_regex(pattern=r"a*", full_match=True)

    # Then empty string is a valid (and shrunk) example
    assert find(gen.strategy, lambda s: s == "") == ""


def test_string_from_regex_invalid_pattern_raises():
    # Given an invalid regex
    # When / Then: Python's re will raise on compile inside Hypothesis
    with pytest.raises(re.error):
        string_from_regex(pattern="(")  # unbalanced parenthesis
