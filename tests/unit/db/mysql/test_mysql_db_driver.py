import unittest
from records_mover.db.mysql.mysql_db_driver import MySQLDBDriver
from mock import MagicMock, Mock
import sqlalchemy


class TestMySQLDBDriver(unittest.TestCase):
    def setUp(self):
        self.mock_db_engine = MagicMock(name='db_engine')
        self.mock_url_resolver = Mock(name='url_resolver')
        self.mock_db_engine.engine = self.mock_db_engine
        self.mysql_db_driver = MySQLDBDriver(db=self.mock_db_engine,
                                             url_resolver=self.mock_url_resolver)

    def test_integer_limits(self):
        expectations = {
            sqlalchemy.dialects.mysql.TINYINT(): (-128, 127),
            sqlalchemy.sql.sqltypes.SMALLINT(): (-32768, 32767),
            sqlalchemy.dialects.mysql.MEDIUMINT(): (-8388608, 8388607),
            sqlalchemy.sql.sqltypes.INTEGER(): (-2147483648, 2147483647),
            sqlalchemy.sql.sqltypes.BIGINT(): (-9223372036854775808, 9223372036854775807),
        }
        for mock_type, (expected_min_int, expected_max_int) in expectations.items():
            min_int, max_int = self.mysql_db_driver.integer_limits(mock_type)
            self.assertEqual(min_int, expected_min_int)
            self.assertEqual(max_int, expected_max_int)

    def test_integer_limits_unexpected_type(self):
        out = self.mysql_db_driver.integer_limits(Mock(name='unexpected'))
        self.assertEqual(None, out)

    def test_fp_constraints_double(self):
        db_col_type = sqlalchemy.dialects.mysql.DOUBLE()
        total_bits, significand_bits = self.mysql_db_driver.fp_constraints(db_col_type)
        self.assertEqual(total_bits, 64)
        self.assertEqual(significand_bits, 53)

    def test_fp_constraints_float(self):
        db_col_type = sqlalchemy.sql.sqltypes.FLOAT()
        total_bits, significand_bits = self.mysql_db_driver.fp_constraints(db_col_type)
        self.assertEqual(total_bits, 32)
        self.assertEqual(significand_bits, 23)

    def test_fp_constraints_unexpected_type(self):
        out = self.mysql_db_driver.fp_constraints(Mock(name='unexpected'))
        self.assertEqual(None, out)

    def test_type_for_tiny_fits(self):
        out = self.mysql_db_driver.type_for_integer(-123, 123)
        self.assertEqual(type(out), sqlalchemy.dialects.mysql.TINYINT)
        self.assertEqual(out.unsigned, False)

    def test_type_for_unsigned_tiny_fits(self):
        out = self.mysql_db_driver.type_for_integer(123, 253)
        self.assertEqual(type(out), sqlalchemy.dialects.mysql.TINYINT)
        self.assertEqual(out.unsigned, True)

    def test_type_for_smallint_fits(self):
        out = self.mysql_db_driver.type_for_integer(-123, 300)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.SMALLINT)

    def test_type_for_unsigned_smallint_fits(self):
        out = self.mysql_db_driver.type_for_integer(0, 60000)
        self.assertEqual(type(out), sqlalchemy.dialects.mysql.SMALLINT)
        self.assertEqual(out.unsigned, True)

    def test_type_for_integer_fits(self):
        out = self.mysql_db_driver.type_for_integer(-123, 123000)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.INTEGER)

    def test_type_for_unsigned_integer_fits(self):
        out = self.mysql_db_driver.type_for_integer(123, 2147483658)
        self.assertEqual(type(out), sqlalchemy.dialects.mysql.INTEGER)
        self.assertEqual(out.unsigned, True)

    def test_type_for_bigint_fits(self):
        out = self.mysql_db_driver.type_for_integer(-123, 9223372036854775807)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.BIGINT)

    def test_type_for_unsigned_bigint_fits(self):
        out = self.mysql_db_driver.type_for_integer(123, 9223372036854775808)
        self.assertEqual(type(out), sqlalchemy.dialects.mysql.types.BIGINT)
        self.assertEqual(out.unsigned, True)

    def test_type_for_integer_too_big(self):
        out = self.mysql_db_driver.type_for_integer(-12300000000000000000, 123000000000000000000)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.Numeric)

    def test_type_for_integer_unspecified(self):
        out = self.mysql_db_driver.type_for_integer(None, None)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.Integer)

    def test_type_for_floating_point_too_big(self):
        out = self.mysql_db_driver.type_for_floating_point(100, 80)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.Float)
        self.assertEqual(out.precision, 53)

    def test_type_for_floating_point_fits(self):
        out = self.mysql_db_driver.type_for_floating_point(12, 8)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.Float)
        self.assertEqual(out.precision, 8)

    def test_type_for_fixed_point_big(self):
        type_ = self.mysql_db_driver.type_for_fixed_point(123, 45)
        self.assertEqual(type(type_), sqlalchemy.dialects.mysql.types.DOUBLE)

    def test_type_for_fixed_point_small(self):
        type_ = self.mysql_db_driver.type_for_fixed_point(12, 3)
        self.assertEqual(type(type_), sqlalchemy.types.Numeric)

    def test_varchar_length_is_in_chars(self):
        out = self.mysql_db_driver.varchar_length_is_in_chars()
        self.assertEqual(out, True)

    def test_type_for_date_plus_time_with_tz(self):
        out = self.mysql_db_driver.type_for_date_plus_time(has_tz=True)
        self.assertEqual(type(out), sqlalchemy.dialects.mysql.DATETIME)
        self.assertEqual(out.fsp, 6)

    def test_type_for_date_plus_time_with_no_tz(self):
        out = self.mysql_db_driver.type_for_date_plus_time(has_tz=False)
        self.assertEqual(type(out), sqlalchemy.dialects.mysql.DATETIME)
        self.assertEqual(out.fsp, 6)
