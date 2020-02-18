import unittest
from mock import Mock, patch
from records_mover.records.schema.field.constraints import RecordsSchemaFieldDecimalConstraints
import sqlalchemy


class TestDecimalConstraints(unittest.TestCase):
    maxDiff = None

    @patch('records_mover.records.schema.field.constraints.decimal.'
           'RecordsSchemaFieldDecimalConstraints')
    def test_from_sqlalchemy_type_float(self, mock_RecordsSchemaFieldDecimalConstraints):
        mock_required = Mock(name='required')
        mock_unique = Mock(name='unique')
        mock_type_ = Mock(name='type_', spec=sqlalchemy.types.Float)
        mock_driver = Mock(name='driver')
        mock_fp_total_bits = Mock(name='fp_total_bits')
        mock_fp_significand_bits = Mock(name='fp_significand_bits')
        mock_driver.fp_constraints.return_value = (mock_fp_total_bits, mock_fp_significand_bits)
        out = RecordsSchemaFieldDecimalConstraints.from_sqlalchemy_type(mock_required,
                                                                        mock_unique,
                                                                        mock_type_,
                                                                        mock_driver)
        mock_RecordsSchemaFieldDecimalConstraints.\
            assert_called_with(required=mock_required,
                               unique=mock_unique,
                               fp_significand_bits=mock_fp_significand_bits,
                               fp_total_bits=mock_fp_total_bits)
        self.assertEqual(out, mock_RecordsSchemaFieldDecimalConstraints.return_value)

    @patch('records_mover.records.schema.field.constraints.decimal.'
           'RecordsSchemaFieldDecimalConstraints')
    def test_from_sqlalchemy_type_fixed(self, mock_RecordsSchemaFieldDecimalConstraints):
        mock_required = Mock(name='required')
        mock_unique = Mock(name='unique')
        mock_type_ = Mock(name='type_', spec=sqlalchemy.types.Numeric)
        mock_driver = Mock(name='driver')

        mock_fixed_precision = Mock(name='fixed_precision')
        mock_fixed_scale = Mock(name='fixed_scale')
        mock_driver.fixed_point_constraints.return_value = (mock_fixed_precision, mock_fixed_scale)
        out = RecordsSchemaFieldDecimalConstraints.from_sqlalchemy_type(mock_required,
                                                                        mock_unique,
                                                                        mock_type_,
                                                                        mock_driver)
        mock_RecordsSchemaFieldDecimalConstraints.\
            assert_called_with(required=mock_required,
                               unique=mock_unique,
                               fixed_precision=mock_fixed_precision,
                               fixed_scale=mock_fixed_scale)
        self.assertEqual(out, mock_RecordsSchemaFieldDecimalConstraints.return_value)

    def test_to_data_fixed(self):
        constraints = RecordsSchemaFieldDecimalConstraints(required=False,
                                                           unique=None,
                                                           fixed_precision=123,
                                                           fixed_scale=45)
        out = constraints.to_data()
        self.assertEqual(out, {
            'required': False,
            'fixed_precision': 123,
            'fixed_scale': 45
        })
