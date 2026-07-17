import pyspark.sql.types as T  # types only — no SparkSession in this module
import pytest
from hypothesis import given

import spark_proof as sp


def test_integer_carries_spark_integer_type_and_is_not_nullable():
    spec = sp.integer(min_value=0, max_value=10)

    assert spec.spark_type == T.IntegerType()
    assert spec.nullable is False


def test_generated_values_stay_within_custom_bounds():
    spec = sp.integer(min_value=-5, max_value=5)

    @given(spec.strategy)
    def check(value):
        assert isinstance(value, int)
        assert -5 <= value <= 5

    check()


def test_single_point_domain_generates_that_value():
    spec = sp.integer(min_value=7, max_value=7)

    @given(spec.strategy)
    def check(value):
        assert value == 7

    check()


def test_defaults_cover_the_full_spark_integer_domain():
    # Spark IntegerType is 32-bit signed; endpoints must be legal arguments.
    spec = sp.integer(min_value=-2_147_483_648, max_value=2_147_483_647)
    assert spec.spark_type == T.IntegerType()
    # And the no-argument form accepts the same domain implicitly.
    sp.integer()


def test_min_above_max_is_rejected_naming_the_public_call():
    with pytest.raises(ValueError, match="integer"):
        sp.integer(min_value=1, max_value=0)


def test_bounds_below_spark_integer_domain_are_rejected():
    with pytest.raises(ValueError, match="integer"):
        sp.integer(min_value=-2_147_483_649)


def test_bounds_above_spark_integer_domain_are_rejected():
    with pytest.raises(ValueError, match="integer"):
        sp.integer(max_value=2_147_483_648)


def test_booleans_are_rejected_even_though_bool_subclasses_int():
    with pytest.raises(TypeError, match="integer"):
        sp.integer(min_value=True)


def test_non_integers_are_rejected():
    with pytest.raises(TypeError, match="integer"):
        sp.integer(max_value=1.5)
