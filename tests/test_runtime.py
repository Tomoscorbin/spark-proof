import inspect

import pytest
from hypothesis import strategies as st

from spark_proof._internal.runtime import wrap_test

ROWS = st.lists(
    st.fixed_dictionaries({"x": st.integers(0, 10)}), min_size=0, max_size=5
)


def fake_build(request, rows):
    return ("FAKE_DF", rows)


# The generated value arrives as the first parameter.
def _receives_value(df):
    tag, rows = df
    assert tag == "FAKE_DF"
    assert all(0 <= r["x"] <= 10 for r in rows)


test_receives_value = wrap_test(_receives_value, ROWS, fake_build)


# Ordinary pytest fixtures beyond the first parameter keep working.
def _keeps_fixture(df, tmp_path):
    assert tmp_path.exists()
    assert df[0] == "FAKE_DF"


test_keeps_fixture = wrap_test(_keeps_fixture, ROWS, fake_build)


# A user-declared `request` parameter is passed through, not swallowed.
def _passes_request(df, request):
    assert request.node.name == "test_passes_request"


test_passes_request = wrap_test(_passes_request, ROWS, fake_build)


def test_signature_advertised_for_pytest():
    # pytest must see (request, tmp_path): df param gone, rows stripped by @given
    assert list(inspect.signature(test_keeps_fixture).parameters) == [
        "request",
        "tmp_path",
    ]


def test_test_function_without_parameters_is_rejected():
    def t():
        pass

    with pytest.raises(TypeError, match="first parameter"):
        wrap_test(t, ROWS, fake_build)


def test_reserved_rows_parameter_name_is_rejected():
    def t(df, rows):
        pass

    with pytest.raises(TypeError, match="reserved"):
        wrap_test(t, ROWS, fake_build)


def test_failures_shrink_to_the_minimal_counterexample(request):
    seen = []

    def build(req, rows):
        seen.append(rows)
        return rows

    def prop(rows_value):
        assert not any(r["x"] >= 3 for r in rows_value)

    wrapped = wrap_test(prop, ROWS, build)
    with pytest.raises(AssertionError):
        wrapped(request=request)
    # Hypothesis replays the shrunk example last: one row, at the boundary.
    assert seen[-1] == [{"x": 3}]
