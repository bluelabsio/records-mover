import unittest
from records_mover.db.postgres.postgres_db_driver import PostgresDBDriver
from mock import MagicMock
import sqlalchemy


class TestPostgresDBDriver(unittest.TestCase):
    def setUp(self):
        self.mock_db_engine = MagicMock()
        self.mock_db_engine.engine = self.mock_db_engine
        self.postgres_db_driver = PostgresDBDriver(db=self.mock_db_engine)

    def test_integer_limits(self):
        expectations = {
            sqlalchemy.sql.sqltypes.SMALLINT(): (-32768, 32767),
            sqlalchemy.sql.sqltypes.INTEGER(): (-2147483648, 2147483647),
            sqlalchemy.sql.sqltypes.BIGINT(): (-9223372036854775808, 9223372036854775807),
        }
        for mock_type, (expected_min_int, expected_max_int) in expectations.items():
            min_int, max_int = self.postgres_db_driver.integer_limits(mock_type)
            self.assertEqual(min_int, expected_min_int)
            self.assertEqual(max_int, expected_max_int)

    def test_fp_constraints_double(self):
        db_col_type = sqlalchemy.dialects.postgresql.base.DOUBLE_PRECISION()
        total_bits, significand_bits = self.postgres_db_driver.fp_constraints(db_col_type)
        self.assertEqual(total_bits, 64)
        self.assertEqual(significand_bits, 53)

    def test_fp_constraints_real(self):
        db_col_type = sqlalchemy.dialects.postgresql.base.REAL()
        total_bits, significand_bits = self.postgres_db_driver.fp_constraints(db_col_type)
        self.assertEqual(total_bits, 32)
        self.assertEqual(significand_bits, 23)

    def test_type_for_smallint_fits(self):
        out = self.postgres_db_driver.type_for_integer(-123, 123)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.SMALLINT)

    def test_type_for_integer_fits(self):
        out = self.postgres_db_driver.type_for_integer(-123, 123000)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.INTEGER)

    def test_type_for_integer_too_big(self):
        out = self.postgres_db_driver.type_for_integer(-12300000000000000000, 123000000000000000000)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.Numeric)

    def test_type_for_floating_point_too_big(self):
        out = self.postgres_db_driver.type_for_floating_point(100, 80)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.Float)
        self.assertEqual(out.precision, 53)

    def test_type_for_floating_point_fits(self):
        out = self.postgres_db_driver.type_for_floating_point(12, 8)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.Float)
        self.assertEqual(out.precision, 8)
