import datetime as dt
from decimal import Decimal
from hypothesis import strategies as st
from spark_proof.core import (
    INT32_MAX,
    INT32_MIN,
    INT16_MAX,
    INT16_MIN,
    INT64_MAX,
    INT64_MIN,
    FLOAT32_MAX,
    FLOAT32_MIN,
    FLOAT64_MAX,
    FLOAT64_MIN,
    DATE_MAX,
    DATE_MIN,
    TIMESTAMP_MAX,
    TIMESTAMP_MIN,
)
from spark_proof.gen.builders import (
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
        compiled = re.compile(pattern)
    else:
        compiled = pattern
    strategy = st.from_regex(regex=compiled, fullmatch=full_match)
    return Generator(strategy=strategy, spark_type=T.StringType())
