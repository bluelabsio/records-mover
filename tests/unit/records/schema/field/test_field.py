import unittest
from mock import Mock, patch  # , ANY
from records_mover.records.schema.field import RecordsSchemaField
import numpy as np
import pandas as pd


class TestField(unittest.TestCase):
    maxDiff = None

    @patch('records_mover.records.schema.field.pandas.refine_field_from_series')
    def test_refine_from_series(self, mock_refine_field_from_series):
        mock_name = Mock(name='name')
        mock_field_type = Mock(name='field_type')
        mock_constraints = Mock(name='constraints')
        mock_statistics = Mock(name='statistics')
        mock_representations = Mock(name='representations')
        mock_series = Mock(name='series')
        mock_total_rows = Mock(name='total_rows')
        mock_rows_sampled = Mock(name='rows_sampled')
        field = RecordsSchemaField(name=mock_name,
                                   field_type=mock_field_type,
                                   constraints=mock_constraints,
                                   statistics=mock_statistics,
                                   representations=mock_representations)
        field.refine_from_series(mock_series, mock_total_rows, mock_rows_sampled)
        mock_refine_field_from_series.assert_called_with(field, mock_series, mock_total_rows,
                                                         mock_rows_sampled)

    def test_is_more_specific_type_true(self):
        self.assertTrue(RecordsSchemaField.is_more_specific_type('integer', 'string'))

    def test_is_more_specific_type_false_same(self):
        self.assertFalse(RecordsSchemaField.is_more_specific_type('string', 'string'))

    def test_is_more_specific_type_false(self):
        self.assertFalse(RecordsSchemaField.is_more_specific_type('string', 'integer'))

    @patch('records_mover.records.schema.field.pandas.field_from_index')
    def test_from_index(self, mock_field_from_index):
        mock_index = Mock(name='index')
        mock_processing_instructions = Mock(name='processing_instructions')
        out = RecordsSchemaField.from_index(mock_index, mock_processing_instructions)
        mock_field_from_index.\
            assert_called_with(index=mock_index,
                               processing_instructions=mock_processing_instructions)
        self.assertEqual(out, mock_field_from_index.return_value)

    @patch('records_mover.records.schema.field.sqlalchemy.field_from_sqlalchemy_column')
    def test_from_sqlalchemy_column(self, mock_field_from_sqlalchemy_column):
        mock_column = Mock(name='column')
        mock_driver = Mock(name='driver')
        mock_rep_type = Mock(name='rep_type')
        out = RecordsSchemaField.from_sqlalchemy_column(column=mock_column,
                                                        driver=mock_driver,
                                                        rep_type=mock_rep_type)
        mock_field_from_sqlalchemy_column.\
            assert_called_with(column=mock_column,
                               driver=mock_driver,
                               rep_type=mock_rep_type)
        self.assertEqual(out, mock_field_from_sqlalchemy_column.return_value)

    @patch('records_mover.records.schema.field.sqlalchemy.field_to_sqlalchemy_type')
    def test_to_sqlalchemy_type(self, mock_field_to_sqlalchemy_type):
        mock_driver = Mock(name='driver')
        mock_name = Mock(name='name')
        mock_field_type = Mock(name='field_type')
        mock_constraints = Mock(name='constraints')
        mock_statistics = Mock(name='statistics')
        mock_representations = Mock(name='representations')
        field = RecordsSchemaField(name=mock_name,
                                   field_type=mock_field_type,
                                   constraints=mock_constraints,
                                   statistics=mock_statistics,
                                   representations=mock_representations)
        out = field.to_sqlalchemy_type(mock_driver)
        mock_field_to_sqlalchemy_type.assert_called_with(field, mock_driver)
        self.assertEqual(out, mock_field_to_sqlalchemy_type.return_value)

    @patch('records_mover.records.schema.field.sqlalchemy.field_to_sqlalchemy_column')
    def test_to_sqlalchemy_column(self, mock_field_to_sqlalchemy_column):
        mock_driver = Mock(name='driver')
        mock_name = Mock(name='name')
        mock_field_type = Mock(name='field_type')
        mock_constraints = Mock(name='constraints')
        mock_statistics = Mock(name='statistics')
        mock_representations = Mock(name='representations')
        field = RecordsSchemaField(name=mock_name,
                                   field_type=mock_field_type,
                                   constraints=mock_constraints,
                                   statistics=mock_statistics,
                                   representations=mock_representations)
        out = field.to_sqlalchemy_column(mock_driver)
        mock_field_to_sqlalchemy_column.assert_called_with(field, mock_driver)
        self.assertEqual(out, mock_field_to_sqlalchemy_column.return_value)

    def test_to_numpy_dtype_integer(self):
        mock_name = Mock(name='name')
        mock_statistics = Mock(name='statistics')
        mock_representations = Mock(name='representations')
        mock_field_type = 'integer'
        expectations = {
            (-100, 100): np.int8,
            (0, 240): np.uint8,
            (-10000, 10000): np.int16,
            (500, 40000): np.uint16,
            (-200000000, 200000000): np.int32,
            (25, 4000000000): np.uint32,
            (-9000000000000000000, 2000000000): np.int64,
            (25, 10000000000000000000): np.uint64,
            (25, 1000000000000000000000000000): np.float128,
            (None, None): np.int64
        }
        for (mock_min, mock_max), expected_pandas_type in expectations.items():
            mock_constraints = Mock(name='constraints')
            mock_constraints.min_ = mock_min
            mock_constraints.max_ = mock_max
            field = RecordsSchemaField(name=mock_name,
                                       field_type=mock_field_type,
                                       constraints=mock_constraints,
                                       statistics=mock_statistics,
                                       representations=mock_representations)

            out = field.to_numpy_dtype()
            self.assertEqual(out, expected_pandas_type, f"min={mock_min}, max={mock_max}")

    def test_to_numpy_dtype_decimal_float(self):
        mock_name = Mock(name='name')
        mock_statistics = Mock(name='statistics')
        mock_representations = Mock(name='representations')
        mock_field_type = 'decimal'
        expectations = {
            (8, 4): np.float16,
            (20, 10): np.float32,
            (40, 20): np.float64,
            (80, 64): np.float128,
            (500, 250): np.float128,
            (None, None): np.float64,
        }
        for (fp_total_bits, fp_significand_bits), expected_pandas_type in expectations.items():
            mock_constraints = Mock(name='constraints')
            mock_constraints.fixed_precision = None
            mock_constraints.fixed_scale = None
            mock_constraints.fp_total_bits = fp_total_bits
            mock_constraints.fp_significand_bits = fp_significand_bits
            field = RecordsSchemaField(name=mock_name,
                                       field_type=mock_field_type,
                                       constraints=mock_constraints,
                                       statistics=mock_statistics,
                                       representations=mock_representations)

            out = field.to_numpy_dtype()
            self.assertEqual(out, expected_pandas_type)

    def test_to_numpy_dtype_decimal_no_constraints(self):
        mock_name = Mock(name='name')
        mock_statistics = Mock(name='statistics')
        mock_representations = Mock(name='representations')
        mock_field_type = 'decimal'
        field = RecordsSchemaField(name=mock_name,
                                   field_type=mock_field_type,
                                   constraints=None,
                                   statistics=mock_statistics,
                                   representations=mock_representations)

        out = field.to_numpy_dtype()
        self.assertEqual(out, np.float64)

    def test_to_numpy_dtype_fixed_precision_(self):
        mock_name = Mock(name='name')
        mock_statistics = Mock(name='statistics')
        mock_representations = Mock(name='representations')
        mock_constraints = Mock(name='constraints')
        mock_constraints.fixed_precision = 1
        mock_constraints.fixed_scale = 1
        mock_field_type = 'decimal'
        field = RecordsSchemaField(name=mock_name,
                                   field_type=mock_field_type,
                                   constraints=mock_constraints,
                                   statistics=mock_statistics,
                                   representations=mock_representations)

        out = field.to_numpy_dtype()
        self.assertEqual(out, np.float64)

    def test_to_numpy_dtype_misc(self):
        mock_name = Mock(name='name')
        mock_constraints = Mock(name='constraints')
        mock_statistics = Mock(name='statistics')
        mock_representations = Mock(name='representations')
        expectations = {
            'boolean': np.bool_,
            'string': np.object_,
            'date': np.object_,
            'datetime': 'datetime64[ns]',
            'datetimetz': 'datetime64[ns, UTC]',
            'time': np.object_,
        }
        for field_type, expected_pandas_type in expectations.items():
            field = RecordsSchemaField(name=mock_name,
                                       field_type=field_type,
                                       constraints=mock_constraints,
                                       statistics=mock_statistics,
                                       representations=mock_representations)

            out = field.to_numpy_dtype()
            self.assertEqual(out, expected_pandas_type)

    def test_python_type_to_field_type(self):
        mock_unknown_type = Mock(name='unknown_type')
        out = RecordsSchemaField.python_type_to_field_type(mock_unknown_type)
        self.assertIsNone(out)

    def test_cast_series_type_time_empty(self):
        mock_name = Mock(name='name')
        mock_field_type = 'time'
        mock_constraints = Mock(name='constraints')
        mock_statistics = Mock(name='statistics')
        mock_representations = Mock(name='representations')
        field = RecordsSchemaField(name=mock_name,
                                   field_type=mock_field_type,
                                   constraints=mock_constraints,
                                   statistics=mock_statistics,
                                   representations=mock_representations)
        data = np.array([])
        series = pd.Series(data)
        new_series = field.cast_series_type(series)
        self.assertIsNotNone(new_series)

    def test_cast_series_type_time_timedelta_entries(self):
        mock_name = Mock(name='name')
        mock_field_type = 'time'
        mock_constraints = Mock(name='constraints')
        mock_statistics = Mock(name='statistics')
        mock_representations = Mock(name='representations')
        field = RecordsSchemaField(name=mock_name,
                                   field_type=mock_field_type,
                                   constraints=mock_constraints,
                                   statistics=mock_statistics,
                                   representations=mock_representations)
        data = np.array([pd.Timedelta(hours=1, minutes=23, seconds=45)])
        series = pd.Series(data)
        new_series = field.cast_series_type(series)
        self.assertEqual(new_series[0], '01:23:45')

    # def test_cast_series_type_time_timedelta_entries_zeroed(self):
    #     mock_name = Mock(name='name')
    #     mock_field_type = 'time'
    #     mock_constraints = Mock(name='constraints')
    #     mock_statistics = Mock(name='statistics')
    #     mock_representations = Mock(name='representations')
    #     field = RecordsSchemaField(name=mock_name,
    #                                field_type=mock_field_type,
    #                                constraints=mock_constraints,
    #                                statistics=mock_statistics,
    #                                representations=mock_representations)
    #     data = np.array([pd.Timedelta(hours=0, minutes=0, seconds=0)])
    #     series = pd.Series(data)
    #     new_series = field.cast_series_type(series)
    #     self.assertEqual(new_series[0], '00:00:00')
