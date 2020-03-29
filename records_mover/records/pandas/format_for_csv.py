from pandas import DataFrame
from records_mover.records.schema import RecordsSchema
from records_mover.records import DelimitedRecordsFormat


def format_df_for_csv_output(df: DataFrame,
                             records_schema: RecordsSchema,
                             records_format: DelimitedRecordsFormat) -> DataFrame:
    # TODO implement me
    return df
