import unittest
from pandas import DataFrame
from mock import Mock, MagicMock, patch
import numpy as np
import pandas as pd
from records_mover.records.schema.schema.pandas import (schema_from_dataframe,
                                                          refine_schema_from_dataframe)
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.records.schema import RecordsSchema


class TestPandas(unittest.TestCase):
    maxDiff = None

    @patch('records_mover.records.schema.schema.pandas.RecordsSchemaKnownRepresentation')
    @patch('records_mover.records.schema.field.RecordsSchemaField')
    @patch('records_mover.records.schema.schema.RecordsSchema')
    def test_schema_from_dataframe(self,
                                   mock_RecordsSchema,
                                   mock_RecordsSchemaField,
                                   mock_RecordsSchemaKnownRepresentation):
        data = [
            {'Country': 'Belgium', 'Capital': 'Brussels', 'Population': 11190846},
            {'Country': 'India', 'Capital': 'New Delhi', 'Population': 1303171035},
            {'Country': 'Brazil', 'Capital': 'Brasília', 'Population': 207847528},
        ]
        df = DataFrame.from_dict(data)
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_origin_representation =\
            mock_RecordsSchemaKnownRepresentation.from_dataframe.return_value
        mock_known_representations = {
            'origin': mock_origin_representation
        }
        mock_index_field = mock_RecordsSchemaField.from_index.return_value
        mock_series_field = mock_RecordsSchemaField.from_series.return_value
        out = schema_from_dataframe(df=df,
                                    processing_instructions=mock_processing_instructions,
                                    include_index=True)
        mock_RecordsSchemaField.from_index.\
            assert_called_with(df.index,
                               processing_instructions=mock_processing_instructions)
        self.assertEqual(out.fields, [
            mock_index_field,
            mock_series_field,
            mock_series_field,
            mock_series_field,
        ])
        self.assertEqual(out.known_representations, mock_known_representations)

    def test_refine_schema_from_dataframe_large_sample(self):
        mock_records_schema = Mock(name='records_schema')
        mock_df = MagicMock(name='df')
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_processing_instructions.max_inference_rows = 200
        mock_total_rows = 100
        mock_df.index.__len__.return_value = mock_total_rows
        mock_rows_sampled = 100
        mock_field = Mock(name='field')
        mock_records_schema.fields = [mock_field]
        refine_schema_from_dataframe(mock_records_schema,
                                     mock_df,
                                     mock_processing_instructions)
        mock_df.sample.assert_not_called()
        mock_field.refine_from_series.assert_called_with(mock_df.__getitem__.return_value,
                                                         rows_sampled=mock_rows_sampled,
                                                         total_rows=mock_total_rows)

    def test_pandas_numeric_types_and_constraints(self):
        self.maxDiff = None
        # https://docs.scipy.org/doc/numpy/reference/arrays.scalars.html
        # https://stackoverflow.com/a/53828986/9795956
        dtypes = np.dtype([
            ('int8', np.int8),
            ('int16', np.int16),
            ('int32', np.int32),
            ('int64', np.int64),
            ('ubyte', np.ubyte),
            ('uint8', np.uint8),
            ('uint16', np.uint16),
            ('uint32', np.uint32),
            ('uint64', np.uint64),
            ('float16', np.float16),
            ('float32', np.float32),
            ('float64', np.float64),
            # 'float96', np.float96), # not supported by numpy on macOS on amd64, apparantly
            ('float128', np.float128),
        ])
        data = np.empty(0, dtype=dtypes)
        df = pd.DataFrame(data)
        processing_instructions = ProcessingInstructions()
        schema = RecordsSchema.from_dataframe(df, processing_instructions, include_index=False)
        data = schema.to_data()
        fields = data['fields']
        fields_and_constraints = {
            field_name: fields[field_name]['constraints']
            for field_name in fields
        }
        expected_fields = {
            'int8': {
                'required': False, 'unique': False,
                'min': '-128', 'max': '127'
            },
            'float128': {
                'fp_significand_bits': 64,
                'fp_total_bits': 80,
                'required': False,
                'unique': False
            },
            'float16': {
                'fp_significand_bits': 11,
                'fp_total_bits': 16,
                'required': False,
                'unique': False
            },
            'float32': {
                'fp_significand_bits': 23,
                'fp_total_bits': 32,
                'required': False,
                'unique': False
            },
            'float64': {
                'fp_significand_bits': 53,
                'fp_total_bits': 64,
                'required': False,
                'unique': False
            },
            'int16': {
                'max': '32767',
                'min': '-32768',
                'required': False,
                'unique': False
            },
            'int32': {
                'max': '2147483647',
                'min': '-2147483648',
                'required': False,
                'unique': False
            },
            'int64': {
                'max': '9223372036854775807',
                'min': '-9223372036854775808',
                'required': False,
                'unique': False
            },
            'ubyte': {
                'max': '255',
                'min': '0',
                'required': False,
                'unique': False
            },
            'uint16': {
                'max': '65535',
                'min': '0',
                'required': False,
                'unique': False
            },
            'uint32': {
                'max': '4294967295',
                'min': '0',
                'required': False,
                'unique': False
            },
            'uint64': {
                'max': '18446744073709551615',
                'min': '0',
                'required': False,
                'unique': False
            },
            'uint8': {
                'max': '255',
                'min': '0',
                'required': False,
                'unique': False
            }
        }
        self.assertEqual(fields_and_constraints, expected_fields)
