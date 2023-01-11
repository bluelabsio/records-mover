from mock import patch
from records_mover.records.schema.field import RecordsSchemaField
from records_mover.records.schema.field.constraints import (
    RecordsSchemaFieldIntegerConstraints,
    RecordsSchemaFieldDecimalConstraints,
)
import numpy as np
import pandas as pd
import pytest


def with_nullable(nullable: bool, fn):
    def wrapfn(*args, **kwargs):
        with patch(
            "records_mover.records.schema.field.pandas.supports_nullable_ints",
            return_value=nullable,
        ):
            fn(*args, **kwargs)

    return wrapfn


def check_dtype(field_type, constraints, expectation):
    field = RecordsSchemaField(
        name="test",
        field_type=field_type,
        constraints=constraints,
        statistics=None,
        representations=None,
    )
    out = field.cast_series_type(pd.Series(1, dtype=np.int8))
    assert out.dtype == expectation

class Test_to_pandas_dtype_integer_no_nullable:
    expectations = {
        (-100, 100): pd.Int8Dtype(),
        (0, 240): pd.UInt8Dtype(),
        (-10000, 10000): pd.Int16Dtype(),
        (500, 40000): pd.UInt16Dtype(),
        (-200000000, 200000000): pd.Int32Dtype(),
        (25, 4000000000): pd.UInt32Dtype(),
        (-9000000000000000000, 2000000000): pd.Int64Dtype(),
        (25, 10000000000000000000): pd.UInt64Dtype(),
        (25, 1000000000000000000000000000): np.float128,
        (None, None): pd.Int64Dtype(),
    }
    @pytest.mark.parametrize("expectation", expectations.items())
    def test_to_pandas_dtype_integer_no_nullable(self, expectation):
        (min_,max_),expected_pandas_type = expectation
        constraints = RecordsSchemaFieldIntegerConstraints(
            required=True, unique=None, min_=min_, max_=max_
        )
        with_nullable(False, check_dtype("integer", constraints, expected_pandas_type))

# def test_to_pandas_dtype_integer_no_nullable():
#     expectations = {
#         (-100, 100): np.int8,
#         (0, 240): np.uint8,
#         (-10000, 10000): np.int16,
#         (500, 40000): np.uint16,
#         (-200000000, 200000000): np.int32,
#         (25, 4000000000): np.uint32,
#         (-9000000000000000000, 2000000000): np.int64,
#         (25, 10000000000000000000): np.uint64,
#         (25, 1000000000000000000000000000): np.float128,
#         (None, None): np.int64,
#     }
#     @pytest.mark.parametrize("expectation",expectations.items())
#     for (min_, max_), expected_pandas_type in expectations.items():
#         constraints = RecordsSchemaFieldIntegerConstraints(
#             required=True, unique=None, min_=min_, max_=max_
#         )
#         yield with_nullable(
#             False, check_dtype
#         ), "integer", constraints, expected_pandas_type


def test_to_pandas_dtype_integer_nullable():
    expectations = {
        (-100, 100): pd.Int8Dtype(),
        (0, 240): pd.UInt8Dtype(),
        (-10000, 10000): pd.Int16Dtype(),
        (500, 40000): pd.UInt16Dtype(),
        (-200000000, 200000000): pd.Int32Dtype(),
        (25, 4000000000): pd.UInt32Dtype(),
        (-9000000000000000000, 2000000000): pd.Int64Dtype(),
        (25, 10000000000000000000): pd.UInt64Dtype(),
        (25, 1000000000000000000000000000): np.float128,
        (None, None): pd.Int64Dtype(),
    }
    for (min_, max_), expected_pandas_type in expectations.items():
        constraints = RecordsSchemaFieldIntegerConstraints(
            required=True, unique=None, min_=min_, max_=max_
        )
        yield with_nullable(
            True, check_dtype
        ), "integer", constraints, expected_pandas_type


def test_to_pandas_dtype_decimal_float():
    expectations = {
        (8, 4): np.float16,
        (20, 10): np.float32,
        (40, 20): np.float64,
        (80, 64): np.float128,
        (500, 250): np.float128,
        (None, None): np.float64,
    }
    for (
        fp_total_bits,
        fp_significand_bits,
    ), expected_pandas_type in expectations.items():
        constraints = RecordsSchemaFieldDecimalConstraints(
            required=False,
            unique=None,
            fixed_precision=None,
            fixed_scale=None,
            fp_total_bits=fp_total_bits,
            fp_significand_bits=fp_significand_bits,
        )
        yield check_dtype, "decimal", constraints, expected_pandas_type


def test_to_pandas_dtype_misc():
    expectations = {
        "boolean": np.bool_,
        "string": np.object_,
        "date": np.object_,
        "datetime": "datetime64[ns]",
        "datetimetz": "datetime64[ns, UTC]",
        "time": np.object_,
    }
    for field_type, expected_pandas_type in expectations.items():
        yield check_dtype, field_type, None, expected_pandas_type


def test_to_pandas_dtype_fixed_precision_():
    check_dtype(
        "decimal",
        RecordsSchemaFieldDecimalConstraints(
            required=False,
            unique=None,
            fixed_precision=1,
            fixed_scale=1,
            fp_total_bits=None,
            fp_significand_bits=None,
        ),
        np.float64,
    )


def test_to_pandas_dtype_decimal_no_constraints():
    check_dtype("decimal", None, np.float64)
