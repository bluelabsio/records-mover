import json
import unittest

import pandas as pd

from records_mover.records import ProcessingInstructions
from records_mover.records.schema import RecordsSchema


class TestDataframeIndexSchema(unittest.TestCase):
    def test_from_dataframe_with_real_index_serializes(self):
        df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']},
                          index=pd.Index([10, 20], name='row_id'))
        schema = RecordsSchema.from_dataframe(df, ProcessingInstructions(),
                                              include_index=True)
        data = json.loads(schema.to_json())
        self.assertEqual(list(data['fields']), ['row_id', 'a', 'b'])
        origin = data['fields']['row_id']['representations']['origin']
        self.assertEqual(origin['pd_df_coltype'], 'index')
        self.assertEqual(origin['pd_df_dtype'], 'int64')
