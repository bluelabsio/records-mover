from pandas import DataFrame
import datetime
import pandas as pd
from records_mover.records import ProcessingInstructions
from records_mover.records.schema import RecordsSchema
from records_mover.records.schema.field import RecordsSchemaField
from records_mover.records import DelimitedRecordsFormat
from records_mover.records.delimited import (python_date_format_from_hints,
                                             python_time_format_from_hints,
                                             cant_handle_hint)
import logging
from typing import Optional, Union, TypeVar

logger = logging.getLogger(__name__)


T = TypeVar('T', bound=Union[pd.Series, pd.Index])


def _convert_series_or_index(series_or_index: T,
                             field: RecordsSchemaField,
                             records_format: DelimitedRecordsFormat,
                             processing_instructions: ProcessingInstructions) -> Optional[T]:
    if field.field_type == 'date':
        if (isinstance(series_or_index[0], pd.Timestamp) or
           isinstance(series_or_index[0], datetime.date)):
            logger.info(f"Converting {series_or_index.name} from np.datetime64 to "
                        "string in CSV's format")
            logger.debug("Dtype is %s, first element type %s", series_or_index.dtype,
                         type(series_or_index[0]))
            hint_date_format = records_format.hints['dateformat']
            assert isinstance(hint_date_format, str)
            pandas_date_format = python_date_format_from_hints.get(hint_date_format)
            if pandas_date_format is None:
                cant_handle_hint(processing_instructions.fail_if_cant_handle_hint,
                                 'dateformat',
                                 records_format.hints)
                pandas_date_format = '%Y-%m-%d'
            if isinstance(series_or_index[0], pd.Timestamp):
                if isinstance(series_or_index, pd.Series):
                    return series_or_index.dt.strftime(pandas_date_format)
                else:
                    return series_or_index.strftime(pandas_date_format)
            else:
                return series_or_index.apply(pd.Timestamp).dt.strftime(pandas_date_format)
    elif field.field_type == 'time':
        if (not (isinstance(series_or_index[0], pd.Timestamp) or
                 isinstance(series_or_index[0], datetime.time))):
            logger.warning(f"Found {series_or_index.name} as unexpected "
                           f"type {type(series_or_index[0])}")
        else:
            logger.info(f"Converting {series_or_index.name} from np.datetime64 to string "
                        "in CSV's format")
            logger.debug("Dtype is %s, first element type %s", series_or_index.dtype,
                         type(series_or_index[0]))
            hint_time_format = records_format.hints['timeonlyformat']
            assert isinstance(hint_time_format, str)
            pandas_time_format = python_time_format_from_hints.get(hint_time_format)
            if pandas_time_format is None:
                cant_handle_hint(processing_instructions.fail_if_cant_handle_hint,
                                 'timeonlyformat',
                                 records_format.hints)
                pandas_time_format = '%H:%M:%S'

            if isinstance(series_or_index[0], datetime.time):
                return series_or_index.apply(lambda d: d.strftime(pandas_time_format))
            elif isinstance(series_or_index, pd.Series):
                return series_or_index.dt.strftime(pandas_time_format)
            else:
                return series_or_index.strftime(pandas_time_format)
    else:
        logger.debug(f"Not converting field type {field.field_type}")

    return None


def prep_df_for_csv_output(df: DataFrame,
                           include_index: bool,
                           records_schema: RecordsSchema,
                           records_format: DelimitedRecordsFormat,
                           processing_instructions: ProcessingInstructions) -> DataFrame:
    #
    # Pandas dataframes only have a native 'datetime'/'datetimetz'
    # datatype (pd.Timestamp), not an individal 'date', 'time' or
    # 'timetz' class.  To generate the correct thing when writing out
    # a 'date' or 'time' type to a CSV with Pandas' .to_csv() method,
    # we need to convert those values to strings that represent
    # exactly what we want.
    #
    # An example of when we can get these pd.Timestamp values inside a
    # dataframe from read_csv() is when we tell it that a given column
    # represents a date and/or time, allowing us to pick the format on
    # the way out.
    #
    formatted_df = df.copy(deep=False)
    remaining_fields = records_schema.fields.copy()
    if include_index:
        field = remaining_fields.pop(0)
        formatted_index = _convert_series_or_index(formatted_df.index,
                                                   field,
                                                   records_format,
                                                   processing_instructions)
        if formatted_index is not None:
            formatted_df.index = formatted_index

    for index, field in enumerate(remaining_fields):
        series = formatted_df.iloc[:, index]
        formatted_series = _convert_series_or_index(series,
                                                    field,
                                                    records_format,
                                                    processing_instructions)
        if formatted_series is not None:
            formatted_df.iloc[:, index] = formatted_series
    return formatted_df
