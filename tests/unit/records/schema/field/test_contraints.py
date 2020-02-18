import unittest
from mock import Mock, patch
from records_mover.records.schema.field.constraints import RecordsSchemaFieldConstraints
import numpy


class TestConstraints(unittest.TestCase):
    maxDiff = None

    def test_from_data_no_data(self):
        mock_field_type = Mock(name='field_type')
        out = RecordsSchemaFieldConstraints.from_data(data=None, field_type=mock_field_type)
        self.assertIsNone(out)

    def test_from_data_new_field_type(self):
        mock_field_type = Mock(name='field_type')
        data = {
            'unique': True,
            'required': True
        }
        out = RecordsSchemaFieldConstraints.from_data(data=data,
                                                      field_type=mock_field_type)
        self.assertEqual(out.required, True)
        self.assertEqual(out.unique, True)

    def test_from_data_integer_with_constraints_type(self):
        mock_field_type = 'integer'
        data = {
            'unique': True,
            'required': True,
            'min': '-123',
            'max': '123'
        }
        out = RecordsSchemaFieldConstraints.from_data(data=data,
                                                      field_type=mock_field_type)
        self.assertEqual(out.required, True)
        self.assertEqual(out.unique, True)
        self.assertEqual(out.min_, -123)
        self.assertEqual(out.max_, 123)

    @patch('records_mover.records.schema.field.constraints.constraints.'
           'RecordsSchemaFieldConstraints')
    def test_from_sqlalchemy_type(self, mock_RecordsSchemaFieldConstraints):
        mock_required = Mock(name='required')
        mock_unique = Mock(name='unique')
        mock_type_ = Mock(name='type_')
        mock_driver = Mock(name='driver')
        out = RecordsSchemaFieldConstraints.from_sqlalchemy_type(mock_required,
                                                                 mock_unique,
                                                                 mock_type_,
                                                                 mock_driver)
        mock_RecordsSchemaFieldConstraints.assert_called_with(required=mock_required,
                                                              unique=mock_unique)
        self.assertEqual(out, mock_RecordsSchemaFieldConstraints.return_value)

    @patch('records_mover.records.schema.field.constraints.string.'
           'RecordsSchemaFieldStringConstraints')
    def test_from_numpy_dtype_string(self, mock_RecordsSchemaFieldStringConstraints):
        mock_dtype = numpy.dtype("O")
        mock_unique = Mock(name='unique')
        out = RecordsSchemaFieldConstraints.from_numpy_dtype(mock_dtype,
                                                             mock_unique)
        mock_RecordsSchemaFieldStringConstraints.\
            assert_called_with(required=False,
                               unique=mock_unique,
                               max_length_bytes=None,
                               max_length_chars=None)
        self.assertEqual(out, mock_RecordsSchemaFieldStringConstraints.return_value)
