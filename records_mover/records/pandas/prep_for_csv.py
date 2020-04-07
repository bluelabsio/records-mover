from pandas import DataFrame
import pandas as pd
from records_mover.records.schema import RecordsSchema
from records_mover.records import DelimitedRecordsFormat
import logging

logger = logging.getLogger(__name__)


def prep_df_for_csv_output(df: DataFrame,
                           include_index: bool,
                           records_schema: RecordsSchema,
                           records_format: DelimitedRecordsFormat) -> DataFrame:
    # TODO: Should this take into account index situation?

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

    for index, field in enumerate(records_schema.fields):
        if field.field_type == 'date':
            series = formatted_df.iloc[:, index]
            if not isinstance(series[0], pd.Timestamp):
                logger.warning(f"Found {series.name} as unexpected type {type(series[0])}")
            else:
                logger.info(f"Converting {series.name} from np.datetime64 to "
                            "string in CSV's format")
                hint_date_format = records_format.hints['dateformat']
                assert isinstance(hint_date_format, str)
                pandas_date_format_conversion = {
                    'YYYY-MM-DD': '%Y-%m-%d',
                    'MM/DD/YY': '%m/%d/%Y',
                    'DD/MM/YY': '%d/%m/%Y',
                }
                pandas_date_format = pandas_date_format_conversion.get(hint_date_format,
                                                                       '%Y-%m-%d')
                formatted_series = series.dt.strftime(pandas_date_format)
                formatted_df.iloc[:, index] = formatted_series
        elif field.field_type == 'time':
            series = formatted_df.iloc[:, index]
            if not isinstance(series[0], pd.Timestamp):
                logger.warning(f"Found {series.name} as unexpected type {type(series[0])}")
            else:
                logger.info(f"Converting {series.name} from np.datetime64 to string "
                            "in CSV's format")
                hint_time_format = records_format.hints['timeonlyformat']
                assert isinstance(hint_time_format, str)
                pandas_time_format_conversion = {
                    'HH24:MI:SS': '%H:%M:%S',
                    'HH12:MI AM': '%I:%M:%S %p',
                }
                pandas_time_format = pandas_time_format_conversion.get(hint_time_format,
                                                                       '%H:%M:%S')
                formatted_series = series.dt.strftime(pandas_time_format)
                formatted_df.iloc[:, index] = formatted_series
    return formatted_df
