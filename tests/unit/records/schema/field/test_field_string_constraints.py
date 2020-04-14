import unittest
import sqlalchemy
from mock import Mock, patch
from records_mover.records.schema.field.constraints import RecordsSchemaFieldStringConstraints


class TestFieldStringConstraints(unittest.TestCase):
    maxDiff = None

    @patch('sqlalchemy.schema.CreateColumn')
    def test_from_sqlalchemy_typein_chars(self,
                                          mock_CreateColumn):
        mock_required = Mock(name='required')
        mock_unique = Mock(name='unique')
        mock_type_ = sqlalchemy.types.String(length=123)
        mock_driver = Mock(name='driver')
        mock_driver.varchar_length_is_in_chars.return_value = True
        out = RecordsSchemaFieldStringConstraints.from_sqlalchemy_type(required=mock_required,
                                                                       unique=mock_unique,
                                                                       type_=mock_type_,
                                                                       driver=mock_driver)
        self.assertEqual(out.required, mock_required)
        self.assertEqual(out.unique, mock_unique)
        self.assertEqual(out.max_length_chars, mock_type_.length)

    def test_str(self):
        mock_required = 'required'
        mock_unique = 'unique'
        mock_type_ = sqlalchemy.types.String(length=123)
        mock_driver = Mock(name='driver')
        mock_driver.varchar_length_is_in_chars.return_value = True
        out = RecordsSchemaFieldStringConstraints.from_sqlalchemy_type(required=mock_required,
                                                                       unique=mock_unique,
                                                                       type_=mock_type_,
                                                                       driver=mock_driver)
        self.assertEqual(str(out),
                         "RecordsSchemaFieldStringConstraints({'required': 'required', "
                         "'unique': 'unique', 'max_length_chars': 123})")
