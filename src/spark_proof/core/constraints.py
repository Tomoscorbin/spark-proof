from typing import Sequence, Generic
from spark_proof.core.metadata import DT


# TODO: improve error messages with callers


class RangeConstraint(Generic[DT]):
    def __init__(
        self,
        domain_min: DT,
        domain_max: DT,
    ) -> None:
        self._domain_min = domain_min
        self._domain_max = domain_max

    def validate_single(self, value: DT) -> None:
        if isinstance(value, bool):
            raise TypeError("bool is not allowed; use an integer literal")

        if (
            value < self._domain_min or value > self._domain_max
        ):  # TODO: we are doing this validation twice with validate_bounds
            raise ValueError(
                f"Value {value} outside [{self._domain_min}, {self._domain_max}]"
            )

    def validate_values(self, values: Sequence[DT]) -> None:
        if not values:
            raise ValueError("Requires at least one value")

        for v in values:
            self.validate_single(v)

        self.validate_bounds(min(values), max(values))

    def validate_bounds(self, lo: DT, hi: DT) -> None:
        if lo > hi:
            raise ValueError("min_value must be <= max_value")

        if lo < self._domain_min or hi > self._domain_max:
            raise ValueError(
                f"Bounds must be within"
                f" [{self._domain_min}, {self._domain_max}]"
                f" but got [{lo}, {hi}]"
            )

    @property
    def domain_min(self) -> DT:
        return self._domain_min

    @property
    def domain_max(self) -> DT:
        return self._domain_max
