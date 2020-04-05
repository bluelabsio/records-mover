import pandas as pd
# import pytz
import unittest
from records_mover.records.pandas import prep_df_for_csv_output
from records_mover.records.schema import RecordsSchema
from records_mover.records import DelimitedRecordsFormat


class TestPrepForCsv(unittest.TestCase):
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
                "timetz": {
                    "type": "timetz",
                    "index": 3,
                },
            }
        }
        records_format = DelimitedRecordsFormat(variant='bluelabs')
        records_schema = RecordsSchema.from_data(schema_data)
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
            #   https://app.asana.com/0/1128138765527694/1169941483931186
            #
            # In addition, Vertica suffers from a driver limitation:
            #
            #   https://app.asana.com/0/search/1169941483931185/1126315736470782
            #
            # 'timetz': [
            #     us_eastern.localize(pd.Timestamp(year=1970, month=1, day=1,
            #                                      hour=12, minute=33, second=53, microsecond=1234)),
            # ],
        }
        df = pd.DataFrame(data,
                          columns=['date', 'time', 'timetz'])

        new_df = prep_df_for_csv_output(df,
                                        records_schema,
                                        records_format)
        self.assertEqual(new_df['date'][0], '1970-01-01')
        self.assertEqual(new_df['time'][0], '12:33:53')
        # self.assertEqual(new_df['timetz'][0], '12:33:53-05')
        self.assertIsNotNone(new_df)
