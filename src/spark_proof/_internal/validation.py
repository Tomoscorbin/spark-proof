from typing import Any


def validate_bounds(
    min_value: Any,
    max_value: Any,
    *,
    domain_min: Any,
    domain_max: Any,
    constructor: str,
) -> None:
    """Reject an unusable [min_value, max_value] range, naming the public call."""
    if min_value > max_value:
        raise ValueError(
            f"{constructor}: min_value must be <= max_value "
            f"(got {min_value} > {max_value})"
        )
    if min_value < domain_min:
        raise ValueError(
            f"{constructor}: min_value {min_value} is below the smallest "
            f"legal value {domain_min}"
        )
    if max_value > domain_max:
        raise ValueError(
            f"{constructor}: max_value {max_value} is above the largest "
            f"legal value {domain_max}"
        )
