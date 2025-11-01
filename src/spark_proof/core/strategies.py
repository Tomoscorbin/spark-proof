from typing import overload, Any, Callable, Sequence
import hypothesis.strategies as st
from hypothesis.strategies import SearchStrategy
from spark_proof.core.metadata import DT
from datetime import date, datetime


# TODO: for_values doesnt need to be overloaded. just ensure correct types elsewhere in validation
# that the correct types are passed.


# TODO: make this an actual type?
class Double(float):
    """Marker type for double-precision floats."""

    pass


class Strategies:
    """Double must come before float and datetime before date due to subtypes!!"""

    @staticmethod
    def for_values(values: Sequence[DT]) -> SearchStrategy[DT]:
        return _build_values_strategy(values)

    @overload
    def for_range(
        self, lo: int, hi: int
    ) -> SearchStrategy[int]: ...  # TODO: should these be staticmethods?

    @overload
    def for_range(self, lo: Double, hi: Double) -> SearchStrategy[float]: ...

    @overload
    def for_range(self, lo: float, hi: float) -> SearchStrategy[float]: ...

    @overload
    def for_range(self, lo: datetime, hi: datetime) -> SearchStrategy[datetime]: ...

    @overload
    def for_range(self, lo: date, hi: date) -> SearchStrategy[date]: ...

    def for_range(self, lo: Any, hi: Any) -> SearchStrategy[Any]:
        builder = _RANGE_BUILDERS.get(type(lo))
        if builder is None:
            raise TypeError(
                f"Unsupported type for for_range: {type(lo)!r}."
                " Use int, float, Double, date, or datetime."
            )

        return builder(lo, hi)


def _build_int_range(lo: int, hi: int) -> SearchStrategy[int]:
    return st.integers(min_value=lo, max_value=hi)


def _build_float32_range(lo: float, hi: float) -> SearchStrategy[float]:
    return st.floats(
        min_value=lo,
        max_value=hi,
        width=32,
        allow_nan=False,
        allow_infinity=False,
    )


def _build_double_range(lo: Double, hi: Double) -> SearchStrategy[float]:
    return st.floats(
        min_value=lo,
        max_value=hi,
        width=64,
        allow_nan=False,
        allow_infinity=False,
    )


def _build_date_range(lo: date, hi: date) -> SearchStrategy[date]:
    return st.dates(min_value=lo, max_value=hi)


def _build_datetime_range(lo: datetime, hi: datetime) -> SearchStrategy[datetime]:
    return st.datetimes(min_value=lo, max_value=hi)


_RANGE_BUILDERS: dict[
    type, Callable[[Any, Any], SearchStrategy[Any]]
] = {  # TODO: make this a class variable?
    int: _build_int_range,
    float: _build_float32_range,
    Double: _build_double_range,
    date: _build_date_range,
    datetime: _build_datetime_range,
}


def _build_values_strategy(values: Sequence[DT]) -> SearchStrategy[DT]:
    return st.sampled_from(list(values))


#
#
# class RangeStrategies(Protocol[DT]):
#     def for_range(self, lo: DT, hi: DT) -> SearchStrategy[DT]: ...
#     def for_values(
#         self, values: Sequence[DT]
#     ) -> SearchStrategy[DT]: ...  # TODO: This doesnt just apply to ranges
#
#
# class IntegerStrategies:
#     def for_range(self, lo: int, hi: int) -> SearchStrategy[int]:
#         return st.integers(
#             min_value=lo, max_value=hi
#         )  # TODO: this is exposing details to the public api
#
#     def for_values(self, values: Sequence[int]) -> SearchStrategy[int]:
#         return st.sampled_from(
#             list(values)
#         )  # TODO: this is exposing details to the public api
#
#
# class FloatStrategies:
#     def __init__(self, width: Width) -> None:
#         self._width: Width = width
#
#     def for_range(self, lo: float, hi: float) -> SearchStrategy[float]:
#         return st.floats(
#             min_value=lo,
#             max_value=hi,
#             width=self._width,
#             allow_nan=False,
#             allow_infinity=False,
#         )
#
#     def for_values(self, values: Sequence[float]) -> SearchStrategy[float]:
#         return st.sampled_from(list(values))
#
#
# class DateStrategies(RangeStrategies[date]):
#     def for_range(self, lo: date, hi: date) -> SearchStrategy[date]:
#         return st.dates(min_value=lo, max_value=hi)
#
#     def for_values(self, values: Sequence[date]) -> SearchStrategy[date]:
#         return st.sampled_from(list(values))
#
#
# class TimestampStrategies(RangeStrategies[datetime]):
#     def for_range(self, lo: datetime, hi: datetime) -> SearchStrategy[datetime]:
#         return st.datetimes(
#             min_value=lo,
#             max_value=hi,
#         )
#
#     def for_values(self, values: Sequence[datetime]) -> SearchStrategy[datetime]:
#         return st.sampled_from(list(values))
