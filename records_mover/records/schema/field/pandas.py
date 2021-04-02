import pandas as pd
from pandas import Series, Index
from typing import Any, Type, TYPE_CHECKING, Optional, Mapping, Union
from .statistics import RecordsSchemaFieldStringStatistics
from ...processing_instructions import ProcessingInstructions
from .representation import RecordsSchemaFieldRepresentation
from ....utils.limits import IntegerType
from .numpy import details_from_numpy_dtype
import numpy as np
if TYPE_CHECKING:
    from ..field import RecordsSchemaField  # noqa
    from ..schema import RecordsSchema  # noqa
    from pandas.core.dtypes.dtypes import ExtensionDtype # noqa

# Cribbed from non-public https://github.com/pandas-dev/pandas/blob/v1.2.1/pandas/_typing.py
Dtype = Union[
    "ExtensionDtype", str, np.dtype, Type[Union[str, float, int, complex, bool, object]]
]
DtypeObj = Union[np.dtype, "ExtensionDtype"]


def supports_nullable_ints() -> bool:
    """Detects if this version of pandas supports nullable int extension types."""
    return 'Int64Dtype' in dir(pd)


def integer_type_mapping(use_extension_types: bool) -> Mapping[IntegerType, DtypeObj]:
    if use_extension_types:
        return {
            IntegerType.INT8: pd.Int8Dtype(),
            IntegerType.UINT8: pd.UInt8Dtype(),
            IntegerType.INT16: pd.Int16Dtype(),
            IntegerType.UINT16: pd.UInt16Dtype(),
            IntegerType.INT24: pd.Int32Dtype(),
            IntegerType.UINT24: pd.Int32Dtype(),
            IntegerType.INT32: pd.Int32Dtype(),
            IntegerType.UINT32: pd.UInt32Dtype(),
            IntegerType.INT64: pd.Int64Dtype(),
            IntegerType.UINT64: pd.UInt64Dtype(),
        }
    else:
        return {
            IntegerType.INT8: np.int8,
            IntegerType.UINT8: np.uint8,
            IntegerType.INT16: np.int16,
            IntegerType.UINT16: np.uint16,
            IntegerType.INT24: np.int32,
            IntegerType.UINT24: np.uint32,
            IntegerType.INT32: np.int32,
            IntegerType.UINT32: np.uint32,
            IntegerType.INT64: np.int64,
            IntegerType.UINT64: np.uint64,
        }


def integer_type_for_range(min_: int, max_: int, has_extension_types: bool) -> Optional[DtypeObj]:
    int_type = IntegerType.smallest_cover_for(min_, max_)
    if int_type:
        return integer_type_mapping(has_extension_types).get(int_type)
    else:
        return None


def field_from_index(index: Index,
                     processing_instructions: ProcessingInstructions) -> 'RecordsSchemaField':

    from ..field import RecordsSchemaField  # noqa
    field_type, constraints = details_from_numpy_dtype(index.dtype, unique=True)
    representations = {
        'origin': RecordsSchemaFieldRepresentation.from_index(index)
    }

    return RecordsSchemaField(name=index.name,
                              field_type=field_type,
                              constraints=constraints,
                              statistics=None,  # call refine_from_dataframe() for stats
                              representations=representations)


def field_from_series(series: Series,
                      processing_instructions: ProcessingInstructions) -> 'RecordsSchemaField':
    from ..field import RecordsSchemaField  # noqa
    field_type, constraints = details_from_numpy_dtype(series.dtype, unique=False)
    representations = {
        'origin': RecordsSchemaFieldRepresentation.from_series(series)
    }

    return RecordsSchemaField(name=series.name,
                              field_type=field_type,
                              constraints=constraints,
                              statistics=None,  # call refine_from_dataframe() for stats
                              representations=representations)


def refine_field_from_series(field: 'RecordsSchemaField',
                             series: Series,
                             total_rows: int,
                             rows_sampled: int) -> 'RecordsSchemaField':
    from ..field import RecordsSchemaField  # noqa
    #
    # if the series is full of object types that aren't numpy
    # types that show up directly as `.dtype` already, we can find
    # that out.
    #
    unique_python_types = series.map(type).unique()
    if unique_python_types.size == 1:
        unique_python_type: Type[Any] = unique_python_types[0]
        field_type = field.python_type_to_field_type(unique_python_type)
        if field_type is not None:
            if RecordsSchemaField.is_more_specific_type(field_type, field.field_type):
                field = field.cast(field_type)

    if field.field_type == 'string':
        max_column_length = series.astype('str').map(len).max()
        if not np.isnan(max_column_length):
            statistics =\
                RecordsSchemaFieldStringStatistics(rows_sampled=rows_sampled,
                                                   total_rows=total_rows,
                                                   max_length_bytes=None,
                                                   max_length_chars=max_column_length)
            if field.statistics is None:
                field.statistics = statistics
            elif not isinstance(field.statistics, RecordsSchemaFieldStringStatistics):
                raise ValueError("Did not expect to see existing statistics "
                                 f"for string type: {field.statistics}")
            else:
                field.statistics.merge(statistics)
    return field
