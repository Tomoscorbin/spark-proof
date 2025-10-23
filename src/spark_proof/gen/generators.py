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
    build_date_generator,
    build_timestamp_generator,
    build_string_generator,
    build_string_from_regex_generator,
)
import pyspark.sql.types as T
from typing import Pattern

# TODO: add realistic date range


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
    return build_date_generator(
        min_value=min_value,
        max_value=max_value,
        type_min=DATE_MIN,
        type_max=DATE_MAX,
)


def timestamp(
    *,
    min_value: dt.datetime = TIMESTAMP_MIN,
    max_value: dt.datetime = TIMESTAMP_MAX,
) -> Generator:
    return build_timestamp_generator(
        min_value=min_value,
        max_value=max_value,
        type_min=TIMESTAMP_MIN,
        type_max=TIMESTAMP_MAX,
    )


# TODO: figure out how to apply alphabet
def string(
    *,
    min_size: int = 0,
    max_size: int | None,
) -> Generator:
    return build_string_generator(
        min_size=min_size,
        max_size=max_size,
        spark_type=T.StringType(),
    )


def string_from_regex(
    *,
    pattern: str | Pattern[str],
    full_match: bool = True,
) -> Generator:
    return build_string_from_regex_generator(
        pattern=pattern,
        full_match=full_match,
        spark_type=T.StringType(),
    )
