from pandas import DataFrame
from .to_csv_options import pandas_to_csv_options  # noqa
from .read_csv_options import pandas_read_csv_options  # noqa


def _lowercase_column_names(df: DataFrame) -> DataFrame:
    df.columns = map(str.lower, df.columns)
    return df


def prep_df_for_loading(df: DataFrame) -> DataFrame:
    return _lowercase_column_names(df)
