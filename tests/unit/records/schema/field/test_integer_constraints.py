import unittest
from mock import Mock, patch
from records_mover.records.schema.field.constraints import RecordsSchemaFieldIntegerConstraints
import sqlalchemy


class TestIntegerConstraints(unittest.TestCase):
    maxDiff = None

    @patch('records_mover.records.schema.field.constraints.integer.'
           'RecordsSchemaFieldIntegerConstraints')
    def test_from_sqlalchemy_type_with_limtis(self, mock_RecordsSchemaFieldIntegerConstraints):
        mock_required = Mock(name='required')
        mock_unique = Mock(name='unique')
        mock_type_ = Mock(name='type_', spec=sqlalchemy.types.Integer)
        mock_driver = Mock(name='driver')
        mock_min_ = Mock(name='min_')
        mock_max_ = Mock(name='max_')
        mock_driver.integer_limits.return_value = (mock_min_, mock_max_)
        out = RecordsSchemaFieldIntegerConstraints.from_sqlalchemy_type(mock_required,
                                                                        mock_unique,
                                                                        mock_type_,
                                                                        mock_driver)
        mock_RecordsSchemaFieldIntegerConstraints.\
            assert_called_with(required=mock_required,
                               unique=mock_unique,
                               min_=mock_min_,
                               max_=mock_max_)
        self.assertEqual(out, mock_RecordsSchemaFieldIntegerConstraints.return_value)
