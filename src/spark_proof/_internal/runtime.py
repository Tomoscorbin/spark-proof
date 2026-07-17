"""pytest/Hypothesis coordination. Must stay free of pyspark imports.

A decorated test needs three parties to agree on one function signature:
pytest (to resolve fixtures), Hypothesis (to inject generated rows), and
the user's test (which declares the DataFrame as its first parameter).
`wrap_test` advertises an explicit Signature — (request, <user fixtures>,
rows) — and re-binds the actual call against it, because Hypothesis
passes fixture arguments positionally.
"""

import inspect
from typing import Any, Callable

from hypothesis import HealthCheck, Phase, given, note, settings
from hypothesis.strategies import SearchStrategy

Rows = list[dict[str, Any]]

_SETTINGS = settings(
    deadline=None,  # Spark calls are far too slow for Hypothesis's default deadline
    phases=(Phase.reuse, Phase.generate, Phase.shrink),  # Phase.explain re-runs slowly
    suppress_health_check=(
        HealthCheck.too_slow,
        # The spark fixture is session-scoped, but passed-through user
        # fixtures (e.g. tmp_path) are function-scoped and shared across
        # examples; without this Hypothesis refuses to run them at all.
        # Revisit in stage 5 diagnostics hardening.
        HealthCheck.function_scoped_fixture,
    ),
)


def wrap_test(
    test_function: Callable[..., None],
    rows_strategy: SearchStrategy[Rows],
    build_input: Callable[[Any, Rows], Any],
) -> Callable[..., None]:
    parameters = list(inspect.signature(test_function).parameters.values())
    if not parameters:
        raise TypeError(
            f"data_frame: test function '{test_function.__name__}' must declare a "
            "first parameter to receive the generated DataFrame"
        )
    fixture_parameters = parameters[1:]
    fixture_names = {p.name for p in fixture_parameters}
    if "rows" in fixture_names:
        raise TypeError(
            f"data_frame: '{test_function.__name__}' declares a parameter named "
            "'rows', which is reserved for the generated examples"
        )
    user_wants_request = "request" in fixture_names

    advertised = []
    if not user_wants_request:
        advertised.append(
            inspect.Parameter("request", inspect.Parameter.POSITIONAL_OR_KEYWORD)
        )
    advertised.extend(fixture_parameters)
    advertised.append(inspect.Parameter("rows", inspect.Parameter.KEYWORD_ONLY))
    advertised_signature = inspect.Signature(advertised)

    def raw(*args: Any, **kwargs: Any) -> None:
        # Hypothesis passes fixture arguments positionally; recover names by
        # binding against the signature we advertised to it and to pytest.
        arguments = dict(advertised_signature.bind(*args, **kwargs).arguments)
        rows = arguments.pop("rows")
        request = (
            arguments["request"] if user_wants_request else arguments.pop("request")
        )
        value = build_input(request, rows)
        note(f"Generated rows (shrunk minimal example on failure): {rows!r}")
        test_function(value, **arguments)

    raw.__signature__ = advertised_signature  # type: ignore[attr-defined]
    raw.__name__ = test_function.__name__

    wrapped = _SETTINGS(given(rows=rows_strategy)(raw))
    wrapped.__name__ = test_function.__name__
    wrapped.__module__ = test_function.__module__
    wrapped.__doc__ = test_function.__doc__
    return wrapped
