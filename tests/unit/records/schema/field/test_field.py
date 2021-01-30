import unittest
import datetime
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
        self.assertEqual(new_series[0], datetime.time(1, 23, 45))

    def test_cast_series_type_time_timedelta_entries_zeroed(self):
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
        data = np.array([pd.Timedelta(hours=0, minutes=0, seconds=0)])
        series = pd.Series(data)
        new_series = field.cast_series_type(series)
        self.assertEqual(new_series[0], datetime.time(0, 0, 0))
