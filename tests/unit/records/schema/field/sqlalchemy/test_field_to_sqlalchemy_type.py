import unittest
from mock import Mock, patch
import sqlalchemy
from records_mover.records.schema.field.sqlalchemy import field_to_sqlalchemy_type
from records_mover.records.schema.field.constraints import (RecordsSchemaFieldStringConstraints,
                                                            RecordsSchemaFieldIntegerConstraints)
from records_mover.records.schema.field.statistics import RecordsSchemaFieldStringStatistics


class TestSqlAlchemyFieldToSqlalchemyType(unittest.TestCase):
    def test_integer(self):
        mock_field = Mock(name='field')

        mock_driver = Mock(name='driver')

        mock_field.field_type = 'integer'
        mock_int_constraints = Mock(name='int_constraints',
                                    spec=RecordsSchemaFieldIntegerConstraints)

        mock_field.constraints = mock_int_constraints
        mock_min_ = Mock(name='min_')
        mock_int_constraints.min_ = mock_min_
        mock_max_ = Mock(name='max_')
        mock_int_constraints.max_ = mock_max_

        out = field_to_sqlalchemy_type(field=mock_field,
                                       driver=mock_driver)
        mock_driver.type_for_integer.assert_called_with(min_value=mock_min_,
                                                        max_value=mock_max_)
        self.assertEqual(out, mock_driver.type_for_integer.return_value)

    def test_decimal_float(self):
        mock_field = Mock(name='field')
        mock_driver = Mock(name='driver')

        mock_field.field_type = 'decimal'
        mock_decimal_constraints = mock_field.constraints
        mock_decimal_constraints.fixed_precision = None
        mock_decimal_constraints.fixed_scale = None
        mock_decimal_constraints.fp_total_bits = 123
        mock_decimal_constraints.fp_significand_bits = 45

        out = field_to_sqlalchemy_type(field=mock_field,
                                       driver=mock_driver)
        mock_driver.type_for_floating_point.\
            assert_called_with(fp_total_bits=mock_decimal_constraints.fp_total_bits,
                               fp_significand_bits=mock_decimal_constraints.fp_significand_bits)
        self.assertEqual(out, mock_driver.type_for_floating_point.return_value)

    def test_decimal_fixed(self):
        mock_field = Mock(name='field')
        mock_driver = Mock(name='driver')

        mock_field.field_type = 'decimal'
        mock_decimal_constraints = mock_field.constraints
        mock_decimal_constraints.fixed_precision = 123
        mock_decimal_constraints.fixed_scale = 45
        mock_decimal_constraints.fp_total_bits = None
        mock_decimal_constraints.fp_significand_bits = None

        out = field_to_sqlalchemy_type(field=mock_field,
                                       driver=mock_driver)
        mock_driver.type_for_fixed_point.\
            assert_called_with(precision=mock_decimal_constraints.fixed_precision,
                               scale=mock_decimal_constraints.fixed_scale)
        self.assertEqual(out, mock_driver.type_for_fixed_point.return_value)

    def test_decimal_default_float(self):
        mock_field = Mock(name='field')
        mock_driver = Mock(name='driver')

        mock_field.field_type = 'decimal'
        mock_decimal_constraints = mock_field.constraints
        mock_decimal_constraints.fixed_precision = None
        mock_decimal_constraints.fixed_scale = None
        mock_decimal_constraints.fp_total_bits = None
        mock_decimal_constraints.fp_significand_bits = None

        out = field_to_sqlalchemy_type(field=mock_field,
                                       driver=mock_driver)
        mock_driver.type_for_floating_point.\
            assert_called_with(fp_total_bits=64,
                               fp_significand_bits=53)
        self.assertEqual(out, mock_driver.type_for_floating_point.return_value)

    def test_boolean(self):
        mock_field = Mock(name='field')
        mock_driver = Mock(name='driver')

        mock_field.field_type = 'boolean'

        out = field_to_sqlalchemy_type(field=mock_field,
                                       driver=mock_driver)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.BOOLEAN)

    @patch('records_mover.records.schema.field.sqlalchemy.generate_string_length')
    def test_string(self,
                    mock_generate_string_length):
        mock_field = Mock(name='field')
        mock_driver = Mock(name='driver')

        mock_field.field_type = 'string'
        mock_field.constraints = Mock(name='constraints', spec=RecordsSchemaFieldStringConstraints)
        mock_field.statistics = Mock(name='statistics', spec=RecordsSchemaFieldStringStatistics)
        mock_string_constraints = mock_field.constraints
        mock_string_statistics = mock_field.statistics
        mock_n = mock_generate_string_length.return_value

        out = field_to_sqlalchemy_type(field=mock_field,
                                       driver=mock_driver)
        mock_generate_string_length.assert_called_with(mock_string_constraints,
                                                       mock_string_statistics,
                                                       mock_driver)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.String)
        self.assertEqual(out.length, mock_n)

    def test_date(self):
        mock_field = Mock(name='field')
        mock_driver = Mock(name='driver')

        mock_field.field_type = 'date'

        out = field_to_sqlalchemy_type(field=mock_field,
                                       driver=mock_driver)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.DATE)

    def test_datetime(self):
        mock_field = Mock(name='field')
        mock_driver = Mock(name='driver')

        mock_field.field_type = 'datetime'

        out = field_to_sqlalchemy_type(field=mock_field,
                                       driver=mock_driver)
        mock_driver.type_for_date_plus_time.assert_called_with(has_tz=False)
        self.assertEqual(out, mock_driver.type_for_date_plus_time.return_value)
        self.assertTrue(out.timezone)

    def test_datetimetz(self):
        mock_field = Mock(name='field')
        mock_driver = Mock(name='driver')

        mock_field.field_type = 'datetimetz'

        out = field_to_sqlalchemy_type(field=mock_field,
                                       driver=mock_driver)
        mock_driver.type_for_date_plus_time.assert_called_with(has_tz=True)
        self.assertEqual(out, mock_driver.type_for_date_plus_time.return_value)
        self.assertTrue(out.timezone)

    def test_time_no_time_type(self):
        mock_field = Mock(name='field')
        mock_driver = Mock(name='driver')
        mock_driver.supports_time_type.return_value = False

        mock_field.field_type = 'time'

        out = field_to_sqlalchemy_type(field=mock_field,
                                       driver=mock_driver)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.VARCHAR)

    def test_time(self):
        mock_field = Mock(name='field')
        mock_driver = Mock(name='driver')
        mock_driver.supports_time_type.return_value = True

        mock_field.field_type = 'time'

        out = field_to_sqlalchemy_type(field=mock_field,
                                       driver=mock_driver)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.TIME)
        self.assertFalse(out.timezone)

    def test_timetz(self):
        mock_field = Mock(name='field')
        mock_driver = Mock(name='driver')
        mock_driver.supports_time_type.return_value = True

        mock_field.field_type = 'timetz'

        out = field_to_sqlalchemy_type(field=mock_field,
                                       driver=mock_driver)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.TIME)
        self.assertTrue(out.timezone)
