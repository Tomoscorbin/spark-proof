from typing import Protocol, Sequence
import hypothesis.strategies as st
from hypothesis.strategies import SearchStrategy
from spark_proof.core.metadata import DT, Width
from datetime import date, datetime


class RangeStrategies(Protocol[DT]):
    def for_range(self, lo: DT, hi: DT) -> SearchStrategy[DT]: ...
    def for_values(self, values: Sequence[DT]) -> SearchStrategy[DT]: ...


class IntegerStrategies:
    def for_range(self, lo: int, hi: int) -> SearchStrategy[int]:
        return st.integers(min_value=lo, max_value=hi)

    def for_values(self, values: Sequence[int]) -> SearchStrategy[int]:
        return st.sampled_from(list(values))


class FloatStrategies:
    def __init__(self, width: Width) -> None:
        self._width: Width = width

    def for_range(self, lo: float, hi: float) -> SearchStrategy[float]:
        return st.floats(
            min_value=lo,
            max_value=hi,
            width=self._width,
            allow_nan=False,
            allow_infinity=False,
        )

    def for_values(self, values: Sequence[float]) -> SearchStrategy[float]:
        return st.sampled_from(list(values))


class DateStrategies(RangeStrategies[date]):
    def for_range(self, lo: date, hi: date) -> SearchStrategy[date]:
        return st.dates(min_value=lo, max_value=hi)

    def for_values(self, values: Sequence[date]) -> SearchStrategy[date]:
        return st.sampled_from(list(values))


class TimestampStrategies(RangeStrategies[datetime]):
    def for_range(self, lo: datetime, hi: datetime) -> SearchStrategy[datetime]:
        return st.datetimes(
            min_value=lo,
            max_value=hi,
        )

    def for_values(self, values: Sequence[datetime]) -> SearchStrategy[datetime]:
        return st.sampled_from(list(values))
