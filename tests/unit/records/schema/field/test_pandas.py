import unittest
from mock import Mock, patch
from records_mover.records.schema.field.pandas import (field_from_index,
                                                       field_from_series)


class TestPandas(unittest.TestCase):
    maxDiff = None

    @patch('records_mover.records.schema.field.RecordsSchemaField')
    @patch('records_mover.records.schema.field.pandas.RecordsSchemaFieldRepresentation')
    @patch('records_mover.records.schema.field.pandas.details_from_numpy_dtype')
    def test_field_from_index(self,
                              mock_details_from_numpy_dtype,
                              mock_RecordsSchemaFieldRepresentation,
                              mock_RecordsSchemaField):
        mock_field_type = Mock(name='field_type')
        mock_constraints = Mock(name='constraints')
        mock_details_from_numpy_dtype.return_value = (mock_field_type, mock_constraints)
        mock_index = Mock(name='index')
        mock_processing_instructions = Mock(name='processing_instructions')
        out = field_from_index(mock_index, mock_processing_instructions)
        mock_RecordsSchemaFieldRepresentation.from_index.assert_called_with(mock_index)
        mock_representations = {
            'origin': mock_RecordsSchemaFieldRepresentation.from_index.return_value
        }
        mock_RecordsSchemaField.assert_called_with(constraints=mock_constraints,
                                                   field_type=mock_field_type,
                                                   name=mock_index.name,
                                                   representations=mock_representations,
                                                   statistics=None)
        self.assertEqual(out, mock_RecordsSchemaField.return_value)

    @patch('records_mover.records.schema.field.RecordsSchemaField')
    @patch('records_mover.records.schema.field.pandas.RecordsSchemaFieldRepresentation')
    @patch('records_mover.records.schema.field.pandas.details_from_numpy_dtype')
    def test_field_from_series(self,
                               mock_details_from_numpy_dtype,
                               mock_RecordsSchemaFieldRepresentation,
                               mock_RecordsSchemaField):
        mock_field_type = Mock(name='field_type')
        mock_constraints = Mock(name='constraints')
        mock_details_from_numpy_dtype.return_value = (mock_field_type, mock_constraints)
        mock_series = Mock(name='series')
        mock_processing_instructions = Mock(name='processing_instructions')
        out = field_from_series(mock_series, mock_processing_instructions)
        mock_RecordsSchemaFieldRepresentation.from_series.assert_called_with(mock_series)
        mock_representations = {
            'origin': mock_RecordsSchemaFieldRepresentation.from_series.return_value
        }
        mock_RecordsSchemaField.assert_called_with(constraints=mock_constraints,
                                                   field_type=mock_field_type,
                                                   name=mock_series.name,
                                                   representations=mock_representations,
                                                   statistics=None)
        self.assertEqual(out, mock_RecordsSchemaField.return_value)
