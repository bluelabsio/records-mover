from pandas import DataFrame

__all__ = [
    'pandas_to_csv_options',
    'pandas_read_csv_options',
    # TODO: Should the following two be similar?  parallel?  the same?
    'format_df_for_csv_output',
    'prep_df_for_loading',
]
from .to_csv_options import pandas_to_csv_options
from .read_csv_options import pandas_read_csv_options
from .format_for_csv import format_df_for_csv_output


def _lowercase_column_names(df: DataFrame) -> DataFrame:
    df.columns = map(str.lower, df.columns)
    return df


def prep_df_for_loading(df: DataFrame) -> DataFrame:
    return _lowercase_column_names(df)
