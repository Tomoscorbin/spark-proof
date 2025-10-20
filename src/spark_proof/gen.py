import datetime as dt
from decimal import Decimal
from hypothesis import strategies as st
from spark_proof.builders import (
    Generator,
    build_decimal_generator,
    build_float_generator,
    build_integer_generator,
)
import pyspark.sql.types as T
from typing import Pattern
import re

# TODO: add realistic date range
# TODO: should validation go here or builders?

# TODO: put in separaete file?
FLOAT32_MIN = -3.4028235e38
FLOAT32_MAX = 3.4028235e38
FLOAT64_MIN = -1.7976931348623157e308
FLOAT64_MAX = 1.7976931348623157e308
INT16_MIN = -32_768
INT16_MAX = 32_767
INT32_MIN = -2_147_483_648
INT32_MAX = 2_147_483_647
INT64_MIN = -9_223_372_036_854_775_808
INT64_MAX = 9_223_372_036_854_775_807
DATE_MIN = dt.date.min
DATE_MAX = dt.date.max
TIMESTAMP_MIN = dt.datetime.min
TIMESTAMP_MAX = dt.datetime.max


def boolean() -> Generator:
    return Generator(
        strategy=st.booleans(),
        spark_type=T.BooleanType(),
    )


def integer(*, min_value: int = INT32_MIN, max_value: int = INT32_MAX) -> Generator:
    return build_integer_generator(
        min_value=min_value,
        max_value=max_value,
        type_min=INT32_MIN,
        type_max=INT32_MAX,
        spark_type=T.IntegerType(),
        generator_name="integer",
    )


def short(*, min_value: int = INT16_MIN, max_value: int = INT16_MAX) -> Generator:
    return build_integer_generator(
        min_value=min_value,
        max_value=max_value,
        type_min=INT16_MIN,
        type_max=INT16_MAX,
        spark_type=T.ShortType(),
        generator_name="short",
    )


def long(*, min_value: int = INT64_MIN, max_value: int = INT64_MAX) -> Generator:
    return build_integer_generator(
        min_value=min_value,
        max_value=max_value,
        type_min=INT64_MIN,
        type_max=INT64_MAX,
        spark_type=T.LongType(),
        generator_name="long",
    )


def float32(
    *,
    min_value: float = FLOAT32_MIN,
    max_value: float = FLOAT32_MAX,
) -> Generator:
    return build_float_generator(
        min_value=min_value,
        max_value=max_value,
        type_min=FLOAT32_MIN,
        type_max=FLOAT32_MAX,
        width=32,
        spark_type=T.FloatType(),
        generator_name="float32",
    )


def double(
    *,
    min_value: float = FLOAT64_MIN,
    max_value: float = FLOAT64_MAX,
) -> Generator:
    return build_float_generator(
        min_value=min_value,
        max_value=max_value,
        type_min=FLOAT64_MIN,
        type_max=FLOAT64_MAX,
        width=64,
        spark_type=T.DoubleType(),
        generator_name="double",
    )


def decimal(
    *,
    precision: int,
    scale: int = 0,
    min_value: Decimal | int | float | None = None,
    max_value: Decimal | int | float | None = None,
) -> Generator:
    return build_decimal_generator(
        precision=precision,
        scale=scale,
        min_value=min_value,
        max_value=max_value,
    )


def date(
    *,
    min_value: dt.date = DATE_MIN,
    max_value: dt.date = DATE_MAX,
) -> Generator:
    if min_value > max_value:
        raise ValueError("min_value must be <= max_value")
    if min_value < DATE_MIN or max_value > DATE_MAX:
        raise ValueError(
            f"date() bounds must be within [{DATE_MIN}, {DATE_MAX}] but got"
            f" [{min_value}, {max_value}]"
        )

    strategy = st.dates(
        min_value=min_value,
        max_value=max_value,
    )
    return Generator(strategy=strategy, spark_type=T.DateType())


def timestamp(
    *,
    min_value: dt.datetime = TIMESTAMP_MIN,
    max_value: dt.datetime = TIMESTAMP_MAX,
) -> Generator:
    if min_value.tzinfo is not None or max_value.tzinfo is not None:
        raise ValueError("timestamp() only supports naive datetimes")
    if min_value > max_value:
        raise ValueError("min_value must be <= max_value")
    if min_value < TIMESTAMP_MIN or max_value > TIMESTAMP_MAX:
        raise ValueError(
            f"timestamp() bounds must be within [{TIMESTAMP_MIN}, {TIMESTAMP_MAX}] but got"
            f" [{min_value}, {max_value}]"
        )

    strategy = st.datetimes(
        min_value=min_value,
        max_value=max_value,
    )
    return Generator(strategy=strategy, spark_type=T.TimestampType())


# TODO: figure out how to apply alphabet
def string(
    *,
    min_size: int = 0,
    max_size: int | None,
) -> Generator:
    if min_size < 0:
        raise ValueError("min_length must be >= 0")
    if max_size is not None and max_size < min_size:
        raise ValueError("max_length must be >= min_length")

    strategy = st.text(min_size=min_size, max_size=max_size)
    return Generator(strategy=strategy, spark_type=T.StringType())


def string_from_regex(
    *,
    pattern: str | Pattern[str],
    full_match: bool = True,
) -> Generator:
    if isinstance(pattern, str):
        compiled = re.compile(pattern)  # raises re.error for invalid patterns
    else:
        compiled = pattern
    strategy = st.from_regex(regex=compiled, fullmatch=full_match)
    return Generator(strategy=strategy, spark_type=T.StringType())
