from dataclasses import dataclass
from typing import Literal, Pattern
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, InvalidOperation
from hypothesis import strategies as st
import pyspark.sql.types as T
import datetime as dt
import re

Width = Literal[16, 32, 64]


@dataclass(frozen=True, slots=True)
class Generator:
    strategy: st.SearchStrategy
    spark_type: T.DataType


@dataclass(frozen=True)
class DecimalSpec:
    precision: int
    scale: int
    type_min: Decimal
    type_max: Decimal
    step: Decimal
    factor: int  # 10**scale


@dataclass(frozen=True)
class Bounds:
    lo: Decimal
    hi: Decimal
    has_min: bool
    has_max: bool


def build_integer_generator(
    min_value: int,
    max_value: int,
    type_min: int,
    type_max: int,
    spark_type: T.DataType,
    generator_name: str,
) -> Generator:
    if min_value > max_value:
        raise ValueError("min_value must be <= max_value")
    if min_value < type_min or max_value > type_max:
        raise ValueError(
            f"{generator_name}() bounds must be within [{type_min}, {type_max}] but got"
            f" [{min_value}, {max_value}]",
        )

    strategy = st.integers(min_value=min_value, max_value=max_value)
    return Generator(strategy=strategy, spark_type=spark_type)


def build_float_generator(
    *,
    min_value: float,
    max_value: float,
    type_min: float,
    type_max: float,
    width: Width,
    spark_type: T.DataType,
    generator_name: str,
) -> Generator:
    if min_value > max_value:
        raise ValueError("min_value must be <= max_value")
    if min_value < type_min or max_value > type_max:
        raise ValueError(
            f"{generator_name}() bounds must be within [{type_min}, {type_max}] but got"
            f" [{min_value}, {max_value}]",
        )

    strategy = st.floats(
        min_value=min_value,
        max_value=max_value,
        width=width,
    )
    return Generator(strategy=strategy, spark_type=spark_type)


def build_decimal_generator(
    *,
    precision: int,
    scale: int,
    min_value: Decimal | int | float | None = None,
    max_value: Decimal | int | float | None = None,
) -> Generator:
    spec = _make_decimal_spec(precision, scale)
    bounds = _coerce_bounds(spec, min_value, max_value)
    _validate_bounds(spec, bounds)
    lo_i, hi_i = _as_integer_window(spec, bounds)
    strategy = _strategy_from_window(lo_i, hi_i, spec.scale)
    return Generator(
        strategy=strategy, spark_type=T.DecimalType(spec.precision, spec.scale)
    )


def build_date_generator(
    *,
    min_value: dt.date,
    max_value: dt.date,
    type_min: dt.date,
    type_max: dt.date,
) -> Generator:
    if min_value > max_value:
        raise ValueError("min_value must be <= max_value")
    if min_value < type_min or max_value > type_max:
        raise ValueError(
            f"date bounds must be within [{type_min}, {type_max}] but got"
            f" [{min_value}, {max_value}]"
        )
    strategy = st.dates(
        min_value=min_value,
        max_value=max_value,
    )
    return Generator(strategy=strategy, spark_type=T.DateType())


def build_timestamp_generator(
    *,
    min_value: dt.datetime,
    max_value: dt.datetime,
    type_min: dt.datetime,
    type_max: dt.datetime,
) -> Generator:
    if min_value.tzinfo is not None or max_value.tzinfo is not None: # TODO: test this
        raise ValueError(f"Only naive datetimes supported")
    if min_value > max_value:
        raise ValueError("min_value must be <= max_value")
    if min_value < type_min or max_value > type_max:
        raise ValueError(
            f"timestamp bounds must be within [{type_min}, {type_max}] but got"
            f" [{min_value}, {max_value}]"
        )
    strategy = st.datetimes(
        min_value=min_value,
        max_value=max_value,
    )
    return Generator(strategy=strategy, spark_type=T.TimestampType())


def build_string_generator(
    *,
    min_size: int,
    max_size: int | None,
    spark_type: T.DataType,
) -> Generator:
    if min_size < 0:
        raise ValueError(f" min_size must be >= 0")
    if max_size is not None and max_size < min_size:
        raise ValueError(
            f"max_size must be >= min_size (min_size={min_size},"
            f" max_size={max_size})"
        )
    strategy = st.text(min_size=min_size, max_size=max_size)
    return Generator(strategy=strategy, spark_type=spark_type)


def build_string_from_regex_generator(
    *,
    pattern: str | Pattern[str],
    full_match: bool,
    spark_type: T.DataType,
) -> Generator:
    compiled = re.compile(pattern) if isinstance(pattern, str) else pattern
    strategy = st.from_regex(regex=compiled, fullmatch=full_match)
    return Generator(strategy=strategy, spark_type=spark_type)


# ---------------- Helpers


def _to_decimal(value: Decimal | int | float) -> Decimal:
    """Convert to Decimal via str() to dodge binary-float artefacts."""
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _step_for_scale(scale: int) -> Decimal:
    """Return 10^-scale as a Decimal (e.g., s=2 -> Decimal('0.01'))."""
    return Decimal(1).scaleb(-scale)


def _type_window(precision: int, scale: int) -> tuple[Decimal, Decimal, Decimal]:
    """
    Spark DECIMAL(p,s) range:
      step = 10^-s
      max_abs = 10^(p-s) - step
      inclusive window = [-max_abs, +max_abs]
    """
    step = _step_for_scale(scale)
    max_abs = (
        Decimal(10) ** (precision - scale)
    ) - step  # TODO: abstract this. meaningful function name
    return -max_abs, max_abs, step


def _coerce_bounds(
    spec: DecimalSpec,
    min_value: Decimal | int | float | None,
    max_value: Decimal | int | float | None,
) -> Bounds:
    has_min = min_value is not None
    has_max = max_value is not None
    lo = spec.type_min if min_value is None else _to_decimal(min_value)
    hi = spec.type_max if max_value is None else _to_decimal(max_value)
    return Bounds(lo=lo, hi=hi, has_min=has_min, has_max=has_max)


def _make_decimal_spec(precision: int, scale: int) -> DecimalSpec:
    if not (1 <= precision <= 38):
        raise ValueError("precision must be in [1, 38]")
    if not (0 <= scale <= precision):
        raise ValueError("scale must be in [0, precision]")
    type_min, type_max, step = _type_window(precision, scale)
    return DecimalSpec(
        precision=precision,
        scale=scale,
        type_min=type_min,
        type_max=type_max,
        step=step,
        factor=10**scale,
    )


def _validate_bounds(spec: DecimalSpec, b: Bounds) -> None:
    # window check (only for user-supplied bounds)
    if b.has_min and not (spec.type_min <= b.lo <= spec.type_max): #TODO: make this more readable
        raise ValueError(
            f"min_value {b.lo} outside DECIMAL({spec.precision},{spec.scale}) "
            f"window [{spec.type_min}, {spec.type_max}]"
        )
    if b.has_max and not (spec.type_min <= b.hi <= spec.type_max):
        raise ValueError(
            f"max_value {b.hi} outside DECIMAL({spec.precision},{spec.scale}) "
            f"window [{spec.type_min}, {spec.type_max}]"
        )
    # alignment to scale
    if b.has_min and not _is_multiple_of_step(b.lo, spec.step):
        raise ValueError(
            f"min_value {b.lo} is not aligned to scale {spec.scale} (step {spec.step})"
        )
    if b.has_max and not _is_multiple_of_step(b.hi, spec.step):
        raise ValueError(
            f"max_value {b.hi} is not aligned to scale {spec.scale} (step {spec.step})"
        )
    # order
    if b.lo > b.hi:
        raise ValueError("min_value must be <= max_value after alignment")


def _as_integer_window(spec: DecimalSpec, b: Bounds) -> tuple[int, int]:
    lo_i = int((b.lo * spec.factor).to_integral_value(rounding=ROUND_CEILING))
    hi_i = int((b.hi * spec.factor).to_integral_value(rounding=ROUND_FLOOR))
    return lo_i, hi_i


def _strategy_from_window(lo_i: int, hi_i: int, scale: int):
    return st.integers(min_value=lo_i, max_value=hi_i).map(
        lambda n: Decimal(n).scaleb(-scale)
    )


def _is_multiple_of_step(x: Decimal, step: Decimal) -> bool:
    # Accept values that are exact multiples (trailing zeros allowed)
    try:
        return (x % step) == 0
    except InvalidOperation:
        return False
