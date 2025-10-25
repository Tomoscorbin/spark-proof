import datetime as dt
from decimal import Decimal

import pytest
import re
import hypothesis.strategies as st
from hypothesis import given, find
import pyspark.sql.types as T
from spark_proof.core import (
    DATE_MAX,
    DATE_MIN,
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
)
from spark_proof.gen import (
    Generator,
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


def test_boolean_generator_exposes_strategy_and_spark_type():
    # Given a boolean generator request
    gen = boolean()

    # Then it returns our Generator wrapper with a Hypothesis strategy
    # And the Spark type is BooleanType
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, st.SearchStrategy)
    assert isinstance(gen.spark_type, T.BooleanType)


def test_integer_generator_exposes_strategy_and_spark_type():
    # Given a valid integer range
    min_value = 1
    max_value = 5
    gen = integer(min_value=min_value, max_value=max_value)

    # Then it returns our Generator wrapper with a Hypothesis strategy
    # And the Spark type is IntegerType
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, st.SearchStrategy)
    assert isinstance(gen.spark_type, T.IntegerType)


def test_integer_min_cannot_be_greater_than_max():
    # Given a range where min_value > max_value
    min_value = 10
    max_value = 9

    # Then invalid ranges (min_value must be <= max_value) are rejected
    with pytest.raises(ValueError):
        integer(min_value=min_value, max_value=max_value)


@pytest.mark.parametrize(
    argnames="min_value,max_value",
    argvalues=[
        (INT32_MIN - 1, 0),  # min underflows int32
        (0, INT32_MAX + 1),  # max overflows int32
        (INT32_MIN - 1, INT32_MAX + 1),  # both invalid
        (INT32_MIN - 1, INT32_MIN - 1),  # equal but both out of range
        (INT32_MAX + 1, INT32_MAX + 1),  # equal but both out of range
    ],
)
def test_integer_respects_bounds(min_value, max_value):
    # Given min_value or max_value outside the valid int32 range
    # Then out-of-range bounds are rejected
    with pytest.raises(ValueError):
        integer(min_value=min_value, max_value=max_value)


def test_short_generator_exposes_strategy_and_spark_type():
    # Given a short (int16) generator request
    gen = short()

    # Then it returns our Generator wrapper with a Hypothesis strategy
    # And the Spark type is ShortType
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, st.SearchStrategy)
    assert isinstance(gen.spark_type, T.ShortType)


def test_short_min_cannot_be_greater_than_max():
    # Given a range where min_value > max_value
    min_value = 10
    max_value = 9

    # Then invalid ranges (min_value must be <= max_value) are rejected
    with pytest.raises(ValueError):
        short(min_value=min_value, max_value=max_value)


@pytest.mark.parametrize(
    argnames="min_value,max_value",
    argvalues=[
        (INT16_MIN - 1, 0),  # min underflows int16
        (0, INT16_MAX + 1),  # max overflows int16
        (INT16_MIN - 1, INT16_MAX + 1),  # both invalid
        (INT16_MIN - 1, INT16_MIN - 1),  # equal but both out of range
        (INT16_MAX + 1, INT16_MAX + 1),  # equal but both out of range
    ],
)
def test_short_respects_bounds(min_value, max_value):
    # Given min_value or max_value outside the valid int16 range
    # Then out-of-range bounds are rejected
    with pytest.raises(ValueError):
        short(min_value=min_value, max_value=max_value)


def test_long_generator_exposes_strategy_and_spark_type():
    # Given a long (int64) generator request
    gen = long()

    # Then it returns our Generator wrapper with a Hypothesis strategy
    # And the Spark type is LongType
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, st.SearchStrategy)
    assert isinstance(gen.spark_type, T.LongType)


def test_long_min_cannot_be_greater_than_max():
    # Given a range where min_value > max_value
    min_value = 10
    max_value = 9

    # Then invalid ranges (min_value must be <= max_value) are rejected
    with pytest.raises(ValueError):
        long(min_value=min_value, max_value=max_value)


@pytest.mark.parametrize(
    argnames="min_value,max_value",
    argvalues=[
        (INT64_MIN - 1, 0),  # min underflows int64
        (0, INT64_MAX + 1),  # max overflows int64
        (INT64_MIN - 1, INT64_MAX + 1),  # both invalid
        (INT64_MIN - 1, INT64_MIN - 1),  # equal but both out of range
        (INT64_MAX + 1, INT64_MAX + 1),  # equal but both out of range
    ],
)
def test_long_respects_bounds(min_value, max_value):
    # Given min_value or max_value outside the valid int64 range
    # Then out-of-range bounds are rejected
    with pytest.raises(ValueError):
        long(min_value=min_value, max_value=max_value)


def test_float32_generator_exposes_strategy_and_spark_type():
    # Given a float32 generator request
    gen = float32()

    # Then it returns our Generator wrapper with a Hypothesis strategy
    # And the Spark type is FloatType
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, st.SearchStrategy)
    assert isinstance(gen.spark_type, T.FloatType)


@pytest.mark.parametrize(
    argnames="min_value,max_value",
    argvalues=[
        (FLOAT32_MIN * 2, 0.0),  # min underflows float32
        (0.0, FLOAT32_MAX * 2),  # max overflows float32
        (FLOAT32_MIN * 2, FLOAT32_MAX * 2),  # both invalid
    ],
)
def test_float32_respects_bounds(min_value, max_value):
    # Given min_value or max_value outside the valid float32 range
    # Then out-of-range bounds are rejected
    with pytest.raises(ValueError):
        float32(min_value=min_value, max_value=max_value)


def test_float32_min_cannot_be_greater_than_max():
    # Given a range where min_value > max_value
    min_value = 1.0
    max_value = 0.0

    # Then invalid ranges (min_value must be <= max_value) are rejected
    with pytest.raises(ValueError):
        float32(min_value=min_value, max_value=max_value)


def test_double_generator_exposes_strategy_and_spark_type():
    # Given a double (float64) generator request
    gen = double()

    # Then it returns our Generator wrapper with a Hypothesis strategy
    # And the Spark type is DoubleType
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, st.SearchStrategy)
    assert isinstance(gen.spark_type, T.DoubleType)


@pytest.mark.parametrize(
    argnames="min_value,max_value",
    argvalues=[
        (FLOAT64_MIN * 2, 0.0),  # min underflows float64
        (0.0, FLOAT64_MAX * 2),  # max overflows float64
        (FLOAT64_MIN * 2, FLOAT64_MAX * 2),  # both invalid
    ],
)
def test_double_respects_bounds(min_value, max_value):
    # Given min_value or max_value outside the valid float64 range
    # Then out-of-range bounds are rejected
    with pytest.raises(ValueError):
        double(min_value=min_value, max_value=max_value)


def test_double_min_cannot_be_greater_than_max():
    # Given a range where min_value > max_value
    min_value = 1.0
    max_value = 0.0

    # Then invalid ranges (min_value must be <= max_value) are rejected
    with pytest.raises(ValueError):
        double(min_value=min_value, max_value=max_value)


def test_decimal_generator_exposes_strategy_and_spark_type():
    # Given a DECIMAL(p,s) generator request
    precision = 10
    scale = 2
    gen = decimal(precision=precision, scale=scale)

    # Then it returns our Generator wrapper with a Hypothesis strategy
    # And the Spark type is DecimalType(precision, scale)
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, st.SearchStrategy)
    assert isinstance(gen.spark_type, T.DecimalType)
    assert gen.spark_type == T.DecimalType(precision, scale)


@pytest.mark.parametrize(
    argnames="precision",
    argvalues=[0, -1],
)
def test_decimal_precision_must_be_positive(precision):
    # Given an invalid precision (precision must be >= 1)
    # Then invalid precision values are rejected
    with pytest.raises(ValueError):
        decimal(precision=precision)


def test_decimal_scale_cannot_exceed_precision():
    # Given a (precision, scale) pair where scale is larger than precision
    # (scale must be in [0, precision])
    precision = 2
    scale = 3

    # Then invalid (precision, scale) pairs are rejected
    with pytest.raises(ValueError):
        decimal(precision=precision, scale=scale)


@pytest.mark.parametrize(
    argnames="max_value",
    argvalues=[
        Decimal("100.00"),  # requires 3 integer digits with DECIMAL(4,2)
        Decimal("-100.00"),  # same on the negative side
        Decimal("0.123"),  # too many fractional digits for scale=2
        Decimal("9.999"),  # too many fractional digits for scale=2
        Decimal("-0.001"),  # too many fractional digits for scale=2
    ],
)
def test_decimal_bounds_cannot_exceed_precision(max_value):
    # Given a bound that cannot be represented by DECIMAL(4,2)
    # DECIMAL(4,2) only allows two integer digits and two fractional digits
    precision = 4
    scale = 2

    # Then out-of-range or misaligned bounds are rejected
    with pytest.raises(ValueError):
        decimal(precision=precision, scale=scale, max_value=max_value)


def test_decimal_respects_bounds():
    # Given a range where min_value > max_value
    precision = 4
    scale = 2
    min_value = Decimal("1.00")
    max_value = Decimal("0.00")

    # Then invalid ranges (min_value must be <= max_value) are rejected
    with pytest.raises(ValueError):
        decimal(
            precision=precision,
            scale=scale,
            min_value=min_value,
            max_value=max_value,
        )


def test_decimal_value_cannot_exceed_scale():
    # Given a bound that has more fractional digits than the declared scale allows
    precision = 6
    scale = 2
    min_value = Decimal("0.001")  # scale=2 means values must be steps of 0.01

    # Then bounds that don't match the scale are rejected
    with pytest.raises(ValueError):
        decimal(precision=precision, scale=scale, min_value=min_value)


def test_decimal_min_value_cannot_exceed_precision_bounds():
    # Given a min_value below what DECIMAL(4,2) can represent
    # DECIMAL(4,2) is limited to the window [-99.99, 99.99]
    precision = 4
    scale = 2
    min_value = Decimal("-100.00")

    # Then bounds outside the DECIMAL(p,s) window are rejected
    with pytest.raises(ValueError):
        decimal(precision=precision, scale=scale, min_value=min_value)


@pytest.mark.parametrize(
    argnames="min_raw,max_raw,scale",
    argvalues=[
        (-1, 2, 2),  # ints
        (0.1, 0.3, 1),  # floats aligned to one decimal place
        (Decimal("-1.25"), Decimal("3.50"), 2),  # Decimals
    ],
)
@given(data=st.data())
def test_decimal_accepts_int_float_and_decimal_bounds(min_raw, max_raw, scale, data):
    # Given aligned bounds provided as int, float, or Decimal
    precision = 6

    # When creating the generator for this (min_raw, max_raw, scale)
    gen = decimal(
        precision=precision,
        scale=scale,
        min_value=min_raw,
        max_value=max_raw,
    ).strategy

    # And drawing a sample from it
    value: Decimal = data.draw(gen)

    # Then all generated values lie within [min_value, max_value]
    def to_decimal(x: Decimal | int | float) -> Decimal:
        return x if isinstance(x, Decimal) else Decimal(str(x))

    lo = to_decimal(min_raw)
    hi = to_decimal(max_raw)

    assert lo <= value <= hi


@pytest.mark.parametrize(
    argnames="bad_bound,scale",
    argvalues=[
        (0.105, 2),  # float not aligned to 2 decimal places
        (Decimal("1.001"), 2),  # Decimal not aligned to 2 decimal places
        (-1.5, 0),  # fractional value when scale is 0
    ],
)
def test_decimal_bounds_must_align_with_scale(bad_bound, scale):
    # Given a bound that is not aligned to the requested scale
    precision = 10

    # Then misaligned bounds are rejected
    with pytest.raises(ValueError):
        decimal(precision=precision, scale=scale, min_value=bad_bound)


@given(
    decimal(
        precision=6,
        scale=2,
        min_value=Decimal("-5.00"),
        max_value=Decimal("-1.00"),
    ).strategy
)
def test_decimal_accepts_negative_ranges(v: Decimal):
    # Given negative min/max values
    # Then every generated value lies within [min_value, max_value]
    assert Decimal("-5.00") <= v <= Decimal("-1.00")


@given(
    decimal(
        precision=6,
        scale=2,
        min_value=Decimal("-1.25"),
        max_value=Decimal("2.50"),
    ).strategy
)
def test_decimal_accepts_ranges_crossing_zero(v: Decimal):
    # Given a range crossing zero
    # Then every generated value lies within [min_value, max_value]
    assert Decimal("-1.25") <= v <= Decimal("2.50")


@given(
    decimal(
        precision=6,
        scale=2,
        min_value=Decimal("-1.25"),
        max_value=Decimal("2.50"),
    ).strategy
)
def test_decimal_values_have_exact_scale(v: Decimal):
    assert v.as_tuple().exponent == -2


def test_decimal_default_bounds_include_type_min_and_max():
    # Given no user-supplied bounds for DECIMAL(4,2)
    precision = 4
    scale = 2
    gen = decimal(precision=precision, scale=scale).strategy

    # Then the generator should cover the entire DECIMAL(p,s) window
    type_min, type_max = _expected_window(precision, scale)

    # And both endpoints should be reachable
    found_lo = find(gen, lambda x: x == type_min)
    found_hi = find(gen, lambda x: x == type_max)

    assert found_lo == type_min
    assert found_hi == type_max


def test_decimal_bounds_must_be_integer_when_scale_zero():
    # Given a DECIMAL(10,0), which does not allow any fractional component
    precision = 10
    scale = 0

    # Then passing a fractional bound is invalid
    with pytest.raises(ValueError):
        decimal(
            precision=precision,
            scale=scale,
            min_value=Decimal("1.5"),  # not an integer value
        )


def test_decimal_rejects_misaligned_negative_bound():
    # Given a negative min_value with more fractional digits than the scale allows
    # scale=2 means values must be in steps of 0.01
    precision = 10
    scale = 2

    # Then misaligned bounds are rejected
    with pytest.raises(ValueError):
        decimal(
            precision=precision,
            scale=scale,
            min_value=Decimal("-1.234"),  # not aligned to 2 decimal places
        )


def test_date_generator_exposes_strategy_and_spark_type():
    # Given a date generator request
    gen = date()

    # Then it returns our Generator wrapper with a Hypothesis strategy
    # And the Spark type is DateType
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, st.SearchStrategy)
    assert isinstance(gen.spark_type, T.DateType)


def test_date_min_cannot_be_greater_than_max():
    # Given a range where min_value > max_value
    min_value = DATE_MAX
    max_value = DATE_MIN

    # Then invalid ranges (min_value must be <= max_value) are rejected
    with pytest.raises(ValueError):
        date(min_value=min_value, max_value=max_value)


def test_timestamp_generator_exposes_strategy_and_spark_type():
    # Given a timestamp generator request
    gen = timestamp()

    # Then it returns our Generator wrapper with a Hypothesis strategy
    # And the Spark type is TimestampType
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, st.SearchStrategy)
    assert isinstance(gen.spark_type, T.TimestampType)


def test_timestamp_min_cannot_be_greater_than_max():
    # Given a range where min_value > max_value
    min_value = dt.datetime(2020, 1, 2)
    max_value = dt.datetime(2020, 1, 1)

    # Then invalid ranges (min_value must be <= max_value) are rejected
    with pytest.raises(ValueError):
        timestamp(min_value=min_value, max_value=max_value)


def test_timestamp_rejects_timezone_aware_bounds():
    # Given timezone-aware datetime bounds (tzinfo is set)
    tz_aware_min = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
    tz_aware_max = dt.datetime(2020, 1, 2, tzinfo=dt.timezone.utc)

    # Then timezone-aware bounds are rejected (timestamps must be naive)
    with pytest.raises(ValueError):
        timestamp(min_value=tz_aware_min, max_value=tz_aware_max)


def test_string_generator_exposes_strategy_and_spark_type():
    # Given a valid string generator request
    gen = string(min_size=0, max_size=5)

    # Then it returns our Generator wrapper with a Hypothesis strategy
    # And the Spark type is StringType
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, st.SearchStrategy)
    assert isinstance(gen.spark_type, T.StringType)


def test_string_min_size_cannot_be_negative():
    # Given a min_size below zero (string length cannot be negative)
    # Then invalid length bounds are rejected
    with pytest.raises(ValueError):
        string(min_size=-1, max_size=0)


def test_string_max_size_must_be_greater_than_or_equal_to_min_size():
    # Given bounds where max_size < min_size
    # Then invalid length ranges are rejected
    with pytest.raises(ValueError):
        string(min_size=5, max_size=4)


def test_string_inclusive_length_bounds():
    # Given explicit length bounds [min_size, max_size]
    min_size = 2
    max_size = 4
    gen = string(min_size=min_size, max_size=max_size)

    # Then strings of exactly min_size and exactly max_size are both attainable
    shortest = find(gen.strategy, lambda s: len(s) == min_size)
    longest = find(gen.strategy, lambda s: len(s) == max_size)

    assert len(shortest) == min_size
    assert len(longest) == max_size


@given(string(min_size=3, max_size=3).strategy)
def test_string_fixed_length_is_respected(s: str):
    # Given min_size == max_size
    # Then every generated string has exactly that length
    assert isinstance(s, str)
    assert len(s) == 3


@given(string(min_size=0, max_size=0).strategy)
def test_string_zero_length_only_generates_empty(s: str):
    # Given min_size == max_size == 0
    # Then every generated value is exactly the empty string
    assert isinstance(s, str)
    assert s == ""


def test_string_unbounded_max_allows_arbitrarily_long_strings():
    # Given no max_size (unbounded upper length)
    gen = string(min_size=0, max_size=None)

    # Then the generator can produce strings at least as long as some target length
    target_length = 10
    sample = find(gen.strategy, lambda s: len(s) == target_length)

    assert len(sample) == target_length


def test_string_from_regex_generator_exposes_strategy_and_spark_type():
    # Given a regex pattern that should generate matching strings
    gen = string_from_regex(pattern=r"[A-Z]{2}\d{3}", full_match=True)

    # Then it returns our Generator wrapper with a Hypothesis strategy
    # And the Spark type is StringType
    assert isinstance(gen, Generator)
    assert isinstance(gen.strategy, st.SearchStrategy)
    assert isinstance(gen.spark_type, T.StringType)


@given(string_from_regex(pattern=r"\d+", full_match=False).strategy)
def test_string_from_regex_search_mode_embeds_pattern(s: str):
    # Given a regex used in search mode (full_match=False)
    # Then every generated string must contain a substring that matches the pattern
    pat = re.compile(r"\d+")
    assert isinstance(s, str)
    assert pat.search(s)


@given(string_from_regex(pattern=r".", full_match=True).strategy)
def test_string_from_regex_fullmatch_dot_produces_single_character(s: str):
    # Given the pattern '.' with full_match=True
    # Then every generated string is exactly one character
    assert isinstance(s, str)
    assert len(s) == 1


def test_string_from_regex_nullable_pattern_can_be_empty():
    # Given a pattern that can legally match the empty string
    gen = string_from_regex(pattern=r"a*", full_match=True)

    # Then the generator can produce the empty string
    # (and Hypothesis considers "" a minimal/shrunk example)
    assert find(gen.strategy, lambda s: s == "") == ""


def test_string_from_regex_rejects_invalid_pattern():
    # Given an invalid regex pattern (unbalanced parenthesis)
    # Then invalid patterns are rejected
    with pytest.raises(re.error):
        string_from_regex(pattern="(")


# --------------- Helpers


def _expected_window(precision: int, scale: int) -> tuple[Decimal, Decimal]:
    # DECIMAL(p,s):
    #   step     = 10^-s
    #   max_abs  = 10^(p-s) - step
    #   window   = [-max_abs, max_abs]
    step = Decimal(1).scaleb(-scale)
    max_abs = (Decimal(10) ** (precision - scale)) - step
    return (-max_abs, max_abs)
