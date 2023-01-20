import unittest
from mock import patch, Mock
from pandas import DataFrame
from records_mover.records.schema.schema.known_representation\
    import RecordsSchemaKnownRepresentation


class TestRecordsSchemaKnownRepresentation(unittest.TestCase):
    maxDiff = None

    @patch('records_mover.records.schema.schema.known_representation.logger')
    def test_from_data_forward_compatibility(self,
                                             mock_logger):
        out = RecordsSchemaKnownRepresentation.from_data({
            'type': 'delimited/bluelabs',
            'mumble': 'whatever'
        })
        self.assertEqual(out, None)
        mock_logger.warning.assert_called()

    def test_from_dataframe(self):
        data = [
            {'Country': 'Belgium', 'Capital': 'Brussels', 'Population': 11190846},
            {'Country': 'India', 'Capital': 'New Delhi', 'Population': 1303171035},
            {'Country': 'Brazil', 'Capital': 'Brasília', 'Population': 207847528},
        ]
        df = DataFrame.from_dict(data)

        processing_instructions = Mock(name='processing_instructions')

        expected_data = {
            'pd_df_dtypes': {'Country': 'object', 'Capital': 'object', 'Population': 'int64'},
            'type': 'dataframe/pandas'
        }

        rep = RecordsSchemaKnownRepresentation.from_dataframe(df,
                                                              processing_instructions)

        data = rep.to_data()
        if 'pd_df_ftypes' in data:
            del data['pd_df_ftypes']  # varies based on what version of pandas we're testing...
        self.assertEqual(data, expected_data)
