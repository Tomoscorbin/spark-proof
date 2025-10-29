from typing import Sequence, Generic
from spark_proof.core.metadata import DT


# TODO: improve error messages with callers
# TODO: can we arrange the validations so that we dont have to call reject_bool multiple times


class RangeConstraint(Generic[DT]):
    def __init__(
        self,
        domain_min: DT,
        domain_max: DT,
    ) -> None:
        self._domain_min = domain_min
        self._domain_max = domain_max

    def _validate_single(self, value: DT) -> None:
        """
        Validate that a single candidate literal is allowed
        for this domain.
        """
        self._reject_bool(value)
        self._check_within_domain(value)

    def _validate_values(self, values: Sequence[DT]) -> None:
        """
        Validate that a sequence of explicit allowed values is OK.

        Rules:
        - must not be empty
        - every value must be within the domain
        """
        if not values:
            raise ValueError("values() requires at least one value")

        for v in values:
            self._validate_single(v)

    def _validate_bounds(self, min_value: DT, max_value: DT) -> None:
        """
        Validate that a [min_value, max_value] range is usable.

        Rules:
        - min_value <= max_value
        - both endpoints are inside the domain
        """
        self._reject_bool(min_value)
        self._reject_bool(max_value)

        if min_value > max_value:
            raise ValueError(
                f"min_value must be <= max_value (got {min_value} > {max_value})"
            )

        self._check_within_domain(max_value)

    def _reject_bool(self, value: DT) -> None:
        if isinstance(value, bool):
            raise TypeError(
                "bool is not allowed; use an integer literal (e.g. 1 instead of True)"
            )

    def _check_within_domain(self, value: DT) -> None:
        if value < self._domain_min or value > self._domain_max:
            raise ValueError(
                f"{value} is outside the allowed range [{self._domain_min}, {self._domain_max}]"
            )

    @property
    def domain_min(self) -> DT:
        return self._domain_min

    @property
    def domain_max(self) -> DT:
        return self._domain_max
