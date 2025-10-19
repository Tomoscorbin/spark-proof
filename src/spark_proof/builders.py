from dataclasses import dataclass
from typing import Literal
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, InvalidOperation
from hypothesis import strategies as st
import pyspark.sql.types as T


# TODO: rename this file!

# TODO: rename to numeric generator
# TODO: consider numeric geneaator that can do float/double with pattern matching


Width = Literal[16, 32, 64]


@dataclass(frozen=True, slots=True)
class Generator:
    strategy: st.SearchStrategy
    spark_type: T.DataType


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
            f"{generator_name}() bounds must be within [{type_min}, {type_max}] but got "
            f"[{min_value}, {max_value}]",
        )

    strategy = st.floats(
        min_value=min_value,
        max_value=max_value,
        width=width,
    )
    return Generator(strategy=strategy, spark_type=spark_type)


# TODO: clean this function up
def build_decimal_generator(
    *,
    precision: int,
    scale: int,
    min_value: Decimal | int | float | None = None,
    max_value: Decimal | int | float | None = None,
) -> Generator:
    if not (1 <= precision <= 38):
        raise ValueError("precision must be in [1, 38]")
    if not (0 <= scale <= precision):
        raise ValueError("scale must be in [0, precision]")

    type_min, type_max, step = _type_window(precision, scale)

    lo = type_min if min_value is None else _to_decimal(min_value)
    hi = type_max if max_value is None else _to_decimal(max_value)

    # Clamp to type window, then snap to the grid
    # lo = _snap_min(max(lo, type_min), step)
    # hi = _snap_max(min(hi, type_max), step)
    if min_value is not None:
        if lo < type_min or lo > type_max:
            raise ValueError(
                f"min_value {lo} outside DECIMAL({precision},{scale}) window "
                f"[{type_min}, {type_max}]"
            )
        if not _is_multiple_of_step(lo, step):
            raise ValueError(
                f"min_value {lo} is not aligned to scale {scale} (step {step})"
            )
    if max_value is not None:
        if hi < type_min or hi > type_max:
            raise ValueError(
                f"max_value {hi} outside DECIMAL({precision},{scale}) window "
                f"[{type_min}, {type_max}]"
            )
        if not _is_multiple_of_step(hi, step):
            raise ValueError(
                f"max_value {hi} is not aligned to scale {scale} (step {step})"
            )
    if lo > hi:
        raise ValueError("min_value must be <= max_value after alignment")

    factor = 10**scale
    lo_i = int((lo * factor).to_integral_value(rounding=ROUND_CEILING))
    hi_i = int((hi * factor).to_integral_value(rounding=ROUND_FLOOR))

    strategy = st.integers(min_value=lo_i, max_value=hi_i).map(
        lambda n: Decimal(n).scaleb(-scale)
    )
    return Generator(strategy=strategy, spark_type=T.DecimalType(precision, scale))


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


def _is_multiple_of_step(x: Decimal, step: Decimal) -> bool:
    # Accept values that are exact multiples (trailing zeros allowed)
    try:
        return (x % step) == 0
    except InvalidOperation:
        return False
