from pandas import Series, Index
from typing import Any, Type, TYPE_CHECKING
from .statistics import RecordsSchemaFieldStringStatistics
from ...processing_instructions import ProcessingInstructions
from .representation import RecordsSchemaFieldRepresentation
from .numpy import details_from_numpy_dtype
import numpy as np
if TYPE_CHECKING:
    from ..field import RecordsSchemaField  # noqa
    from ..schema import RecordsSchema  # noqa


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
                             rows_sampled: int) -> None:
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
                field.field_type = field_type

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
                raise SyntaxError("Did not expect to see existing statistics "
                                  f"for string type: {field.statistics}")
            else:
                field.statistics.merge(statistics)
