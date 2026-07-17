import pyspark.sql.types as T
import pytest
from hypothesis import given

import spark_proof as sp
from spark_proof._internal.models import build_data_frame_spec


def _columns():
    return {
        "a": sp.integer(min_value=0, max_value=3),
        "b": sp.integer(min_value=-1, max_value=1),
    }


def test_schema_preserves_column_insertion_order_and_flags():
    spec = build_data_frame_spec(columns=_columns(), min_rows=0, max_rows=5)

    assert spec.schema == T.StructType(
        [
            T.StructField("a", T.IntegerType(), False),
            T.StructField("b", T.IntegerType(), False),
        ]
    )


def test_generated_rows_respect_bounds_and_column_domains():
    spec = build_data_frame_spec(columns=_columns(), min_rows=1, max_rows=4)

    @given(spec.rows_strategy)
    def check(rows):
        assert 1 <= len(rows) <= 4
        for row in rows:
            assert list(row) == ["a", "b"]
            assert 0 <= row["a"] <= 3
            assert -1 <= row["b"] <= 1

    check()


def test_zero_rows_bound_generates_only_empty_row_lists():
    spec = build_data_frame_spec(columns=_columns(), min_rows=0, max_rows=0)

    @given(spec.rows_strategy)
    def check(rows):
        assert rows == []

    check()


def test_empty_columns_mapping_is_rejected():
    with pytest.raises(ValueError, match="data_frame"):
        build_data_frame_spec(columns={}, min_rows=0, max_rows=5)


def test_negative_min_rows_is_rejected():
    with pytest.raises(ValueError, match="data_frame"):
        build_data_frame_spec(columns=_columns(), min_rows=-1, max_rows=5)


def test_min_rows_above_max_rows_is_rejected():
    with pytest.raises(ValueError, match="data_frame"):
        build_data_frame_spec(columns=_columns(), min_rows=6, max_rows=5)


def test_non_string_column_name_is_rejected():
    with pytest.raises(ValueError, match="data_frame"):
        build_data_frame_spec(
            columns={1: sp.integer()}, min_rows=0, max_rows=5
        )
