import pandas as pd
# import pytz
import unittest
from records_mover.records.pandas import prep_df_for_csv_output
from records_mover.records.schema import RecordsSchema
from records_mover.records import DelimitedRecordsFormat, ProcessingInstructions
from ..datetime_cases import (
    DATE_CASES, DATETIMEFORMATTZ_CASES, DATETIMEFORMAT_CASES,
    create_sample, SAMPLE_YEAR, SAMPLE_MONTH, SAMPLE_DAY, SAMPLE_HOUR, SAMPLE_MINUTE, SAMPLE_SECOND
)


class TestPrepForCsv(unittest.TestCase):
    def test_prep_df_for_csv_output_no_include_index(self):
        schema_data = {
            'schema': "bltypes/v1",
            'fields': {
                "date": {
                    "type": "date",
                    "index": 1,
                },
                "time": {
                    "type": "time",
                    "index": 2,
                },
                "timetz": {
                    "type": "timetz",
                    "index": 3,
                },
            }
        }
        records_format = DelimitedRecordsFormat(variant='bluelabs')
        records_schema = RecordsSchema.from_data(schema_data)
        processing_instructions = ProcessingInstructions()
        # us_eastern = pytz.timezone('US/Eastern')
        data = {
            'date': [pd.Timestamp(year=1970, month=1, day=1)],
            'time': [
                pd.Timestamp(year=1970, month=1, day=1,
                             hour=12, minute=33, second=53, microsecond=1234)
            ],
            # timetz is not well supported in records mover yet.  For
            # instance, specifying how it's turned into a CSV is not
            # currently part of the records spec:
            #
            #   https://github.com/bluelabsio/records-mover/issues/76
            #
            # In addition, Vertica suffers from a driver limitation:
            #
            #   https://github.com/bluelabsio/records-mover/issues/77
            #
            # 'timetz': [
            #     us_eastern.localize(pd.Timestamp(year=1970, month=1, day=1,
            #                                      hour=12, minute=33, second=53,
            #                                      microsecond=1234)),
            # ],
        }
        df = pd.DataFrame(data,
                          columns=['date', 'time', 'timetz'])

        new_df = prep_df_for_csv_output(df=df,
                                        include_index=False,
                                        records_schema=records_schema,
                                        records_format=records_format,
                                        processing_instructions=processing_instructions)
        self.assertEqual(new_df['date'][0], '1970-01-01')
        self.assertEqual(new_df['time'][0], '12:33:53')
        # self.assertEqual(new_df['timetz'][0], '12:33:53-05')
        self.assertIsNotNone(new_df)

    def test_prep_df_for_csv_output_include_index(self):
        schema_data = {
            'schema': "bltypes/v1",
            'fields': {
                "date": {
                    "type": "date",
                    "index": 1,
                },
                "time": {
                    "type": "time",
                    "index": 2,
                },
                "timetz": {
                    "type": "timetz",
                    "index": 3,
                },
            }
        }
        records_format = DelimitedRecordsFormat(variant='bluelabs')
        records_schema = RecordsSchema.from_data(schema_data)
        processing_instructions = ProcessingInstructions()
        # us_eastern = pytz.timezone('US/Eastern')
        data = {
            'time': [
                pd.Timestamp(year=1970, month=1, day=1,
                             hour=12, minute=33, second=53, microsecond=1234)
            ],
            # timetz is not well supported in records mover yet.  For
            # instance, specifying how it's turned into a CSV is not
            # currently part of the records spec:
            #
            #   https://github.com/bluelabsio/records-mover/issues/76
            #
            # In addition, Vertica suffers from a driver limitation:
            #
            #   https://github.com/bluelabsio/records-mover/issues/77
            #
            # 'timetz': [
            #     us_eastern.localize(pd.Timestamp(year=1970, month=1, day=1,
            #                                      hour=12, minute=33, second=53,
            #                                      microsecond=1234)),
            # ],
        }
        df = pd.DataFrame(data,
                          index=[pd.Timestamp(year=1970, month=1, day=1)],
                          columns=['time', 'timetz'])

        new_df = prep_df_for_csv_output(df=df,
                                        include_index=True,
                                        records_schema=records_schema,
                                        records_format=records_format,
                                        processing_instructions=processing_instructions)
        self.assertEqual(new_df.index[0], '1970-01-01')
        self.assertEqual(new_df['time'][0], '12:33:53')
        # self.assertEqual(new_df['timetz'][0], '12:33:53-05')
        self.assertIsNotNone(new_df)

    def test_dateformat(self):
        schema_data = {
            'schema': "bltypes/v1",
            'fields': {
                "date": {
                    "type": "date",
                    "index": 1,
                },
            }
        }
        records_schema = RecordsSchema.from_data(schema_data)
        processing_instructions = ProcessingInstructions()
        for dateformat in DATE_CASES:
            records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                    hints={
                                                        'dateformat': dateformat
                                                    })
            # us_eastern = pytz.timezone('US/Eastern')
            data = {
                'date': [
                    pd.Timestamp(year=SAMPLE_YEAR, month=SAMPLE_MONTH, day=SAMPLE_DAY)
                ],
            }
            df = pd.DataFrame(data, columns=['date'])

            new_df = prep_df_for_csv_output(df=df,
                                            include_index=False,
                                            records_schema=records_schema,
                                            records_format=records_format,
                                            processing_instructions=processing_instructions)
            self.assertEqual(new_df['date'][0],
                             create_sample(dateformat))
            # self.assertEqual(new_df['timetz'][0], '12:33:53-05')
            self.assertIsNotNone(new_df)

    def test_datetimeformattz(self):
        schema_data = {
            'schema': "bltypes/v1",
            'fields': {
                "datetimetz": {
                    "type": "datetimetz",
                    "index": 1,
                },
            }
        }
        records_schema = RecordsSchema.from_data(schema_data)
        processing_instructions = ProcessingInstructions()
        for datetimeformattz in DATETIMEFORMATTZ_CASES:
            records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                    hints={
                                                        'datetimeformattz': datetimeformattz
                                                    })
            # us_eastern = pytz.timezone('US/Eastern')
            timestamp = pd.Timestamp(year=SAMPLE_YEAR, month=SAMPLE_MONTH, day=SAMPLE_DAY,
                                     hour=SAMPLE_HOUR, minute=SAMPLE_MINUTE,
                                     second=SAMPLE_SECOND)

            data = {
                'datetimetz': [
                    timestamp
                ],
            }
            df = pd.DataFrame(data, columns=['datetimetz'])

            new_df = prep_df_for_csv_output(df=df,
                                            include_index=False,
                                            records_schema=records_schema,
                                            records_format=records_format,
                                            processing_instructions=processing_instructions)
            # No conversion is done of datetimetz as pandas' CSV
            # outputter handles it properly, so we should expect the
            # original again
            self.assertEqual(new_df['datetimetz'][0],
                             timestamp,
                             create_sample(datetimeformattz))
            # self.assertEqual(new_df['timetz'][0], '12:33:53-05')
            self.assertIsNotNone(new_df)

    def test_datetimeformat(self):
        schema_data = {
            'schema': "bltypes/v1",
            'fields': {
                "datetimez": {
                    "type": "datetime",
                    "index": 1,
                },
            }
        }
        records_schema = RecordsSchema.from_data(schema_data)
        processing_instructions = ProcessingInstructions()
        for datetimeformat in DATETIMEFORMAT_CASES:
            records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                    hints={
                                                        'datetimeformat': datetimeformat
                                                    })
            # us_eastern = pytz.timezone('US/Eastern')
            timestamp = pd.Timestamp(year=SAMPLE_YEAR, month=SAMPLE_MONTH, day=SAMPLE_DAY,
                                     hour=SAMPLE_HOUR, minute=SAMPLE_MINUTE,
                                     second=SAMPLE_SECOND)

            data = {
                'datetime': [
                    timestamp
                ],
            }
            df = pd.DataFrame(data, columns=['datetime'])

            new_df = prep_df_for_csv_output(df=df,
                                            include_index=False,
                                            records_schema=records_schema,
                                            records_format=records_format,
                                            processing_instructions=processing_instructions)
            # No conversion is done of datetime as pandas' CSV
            # outputter handles it properly, so we should expect the
            # original again
            self.assertEqual(new_df['datetime'][0],
                             timestamp,
                             create_sample(datetimeformat))
            # self.assertEqual(new_df['timetz'][0], '12:33:53-05')
            self.assertIsNotNone(new_df)
