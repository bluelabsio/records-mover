import pandas as pd
import unittest
from records_mover.records.pandas import prep_df_for_csv_output
from records_mover.records.schema import RecordsSchema
from records_mover.records import DelimitedRecordsFormat


class TestFormatForCsv(unittest.TestCase):
    def test_prep_df_for_csv_output(self):
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
                # TODO:
                # "timestamp": {
                #     "type": "datetime",
                #     "index": 3,
                # },
                # "timestamptz": {
                #     "type": "datetimetz",
                #     "index": 4,
                # }
            }
        }
        records_format = DelimitedRecordsFormat()
        records_schema = RecordsSchema.from_data(schema_data)
        data = {
            'date': [pd.Timestamp(year=1970, month=1, day=1)],
            # TODO: This is an ugly way to represent a time alone.
            # Can I figure out why I needed this code and leave a
            # comment explaining it?
            'time': [pd.Timestamp(year=1970, month=1, day=1,
                                  hour=12, minute=33, second=53, microsecond=1234)]
        }
        df = pd.DataFrame(data,
                          columns=['date', 'time'])

        new_df = prep_df_for_csv_output(df,
                                        records_schema,
                                        records_format)
        self.assertEqual(new_df['date'][0], '1970-01-01')
        self.assertEqual(new_df['time'][0], '12:33:53')
        self.assertIsNotNone(new_df)
