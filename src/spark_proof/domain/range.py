from typing import Generic, Sequence
from spark_proof.core.metadata import DT, DomainMetadata
from spark_proof.core.constraints import RangeConstraint
from spark_proof.core.strategies import RangeStrategies
from spark_proof.core.generator import Generator


class RangeField(Generic[DT]):
    def __init__(
        self,
        *,
        meta: DomainMetadata[DT],
        constraint: RangeConstraint[
            DT
        ],  # TODO: consider a protocol/interface instead of taking this directly
        strategies: RangeStrategies[DT],
    ) -> None:
        self._meta = meta
        self._constraint = constraint
        self._strategies = strategies

    def __call__(self) -> Generator:
        lo = self._constraint.domain_min
        hi = self._constraint.domain_max
        return self.range(lo, hi)

    def range(self, min_value: DT, max_value: DT) -> Generator:
        self._constraint.validate_bounds(min_value, max_value)
        strategy = self._strategies.for_range(min_value, max_value)
        return Generator(strategy=strategy, spark_type=self._meta.spark_type)

    def values(self, values: Sequence[DT]) -> Generator:
        self._constraint.validate_values(values)
        strategy = self._strategies.for_values(values)
        return Generator(strategy=strategy, spark_type=self._meta.spark_type)
