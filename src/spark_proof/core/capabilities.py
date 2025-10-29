from typing import Protocol, Sequence
from spark_proof.core.generator import Generator
from spark_proof.core.metadata import DT_in


class SupportsRange(Protocol[DT_in]):
    def range(self, min_value: DT_in, max_value: DT_in) -> Generator: ...


class SupportsValues(Protocol[DT_in]):
    def values(self, values: Sequence[DT_in]) -> Generator: ...


class SupportsRegex(Protocol):
    def from_regex(self, pattern: str) -> Generator: ...
