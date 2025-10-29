import pytest
from datetime import date, datetime
from spark_proof.core.constraints import RangeConstraint


def test_range_accepts_values_within_bounds():
    # Given a domain [0, 10]
    constraint = RangeConstraint(domain_min=0, domain_max=10)

    # When we validate values at the boundaries and inside
    # Then no exception is raised
    constraint._validate_single(0)
    constraint._validate_single(5)
    constraint._validate_single(10)


@pytest.mark.parametrize(argnames="invalid", argvalues=[-1, 11])
def test_range_rejects_value_outside_bounds(invalid):
    # Given a numeric domain [0, 10]
    constraint = RangeConstraint(domain_min=0, domain_max=10)

    # Then a value below the domain is rejected with an "outside [...]" message
    with pytest.raises(ValueError):
        constraint._validate_single(invalid)


def test_range_booleans_are_disallowed():
    # Given an integer-like domain where bool could sneak in (because bool is a subclass of int)
    constraint = RangeConstraint(domain_min=-5, domain_max=5)

    # Then boolean inputs are explicitly rejected
    with pytest.raises(TypeError):
        constraint._validate_single(True)


def test_range_sequence_must_not_be_empty():
    # Given a numeric domain [0, 10]
    constraint = RangeConstraint(domain_min=0, domain_max=10)

    # Then an empty sequence is rejected
    with pytest.raises(ValueError):
        constraint._validate_values([])


def test_range_sequence_must_not_contain_boolean():
    # Given a numeric domain [0, 10] and a sequence containing a boolean
    constraint = RangeConstraint(domain_min=0, domain_max=10)
    values = [0, 1, True, 3]

    # Then presence of a boolean causes a type error
    with pytest.raises(TypeError):
        constraint._validate_values(values)


def test_range_sequence_with_member_below_domain_raises_error():
    # Given a numeric domain [0, 10] and a sequence including -1
    constraint = RangeConstraint(domain_min=0, domain_max=10)
    values = [-1, 0, 1]

    # When validating the sequence
    # Then an error is raised because at least one member is out of range
    with pytest.raises(ValueError):
        constraint._validate_values(values)


def test_range_sequence_with_member_above_domain_raises_error():
    # Given a numeric domain [0, 10] and a sequence including 11
    constraint = RangeConstraint(domain_min=0, domain_max=10)
    values = [9, 10, 11]

    # When validating the sequence
    # Then an error is raised because at least one member is out of range
    with pytest.raises(ValueError):
        constraint._validate_values(values)


def test_range_sequence_with_all_members_within_domain_is_valid_for_integers():
    # Given a numeric domain [-3, 3] and a sequence entirely within the domain
    constraint = RangeConstraint(domain_min=-3, domain_max=3)
    values = [-3, -1, 0, 1, 3]

    # When validating the sequence
    # Then no error is raised
    constraint._validate_values(values)


def test_range_sequence_with_all_members_within_domain_is_valid_for_dates():
    # Given a date domain [2020-01-01, 2020-01-10] and a sequence within that interval
    constraint = RangeConstraint(
        domain_min=date(2020, 1, 1), domain_max=date(2020, 1, 10)
    )
    values = [date(2020, 1, 1), date(2020, 1, 3), date(2020, 1, 10)]

    # When validating the sequence
    # Then no error is raised
    constraint._validate_values(values)


def test_range_sequence_with_all_members_within_domain_is_valid_for_timestamps():
    # Given a date domain [2020-01-01, 2020-01-10] and a sequence within that interval
    constraint = RangeConstraint(
        domain_min=datetime(2020, 1, 1), domain_max=datetime(2020, 1, 10)
    )
    values = [datetime(2020, 1, 1), datetime(2020, 1, 3), datetime(2020, 1, 10)]

    # When validating the sequence
    # Then no error is raised
    constraint._validate_values(values)
