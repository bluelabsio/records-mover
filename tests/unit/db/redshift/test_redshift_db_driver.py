from .base_test_redshift_db_driver import BaseTestRedshiftDBDriver
import sqlalchemy


class TestRedshiftDBDriver(BaseTestRedshiftDBDriver):
    maxDiff = None

    def test_schema_sql(self):
        sql = self.redshift_db_driver.schema_sql('myschema', 'mytable')
        self.assertEqual(sql, '\nCREATE TABLE myschema.mytable (\n)\n\n')

    def test_schema_sql_no_admin_views(self):
        self.mock_db_engine.execute.return_value.fetchall.return_value = []
        mock_schema_sql = str(self.mock_db_engine.dialect.ddl_compiler.return_value)
        sql = self.redshift_db_driver.schema_sql('myschema', 'mytable')
        self.assertEqual(sql, mock_schema_sql)

    def test_set_grant_permissions_for_group(self):
        mock_schema = 'mock_schema'
        mock_table = 'mock_table'
        groups = {'all': ['a_group']}
        mock_conn = self.mock_db_engine.engine.connect.return_value.__enter__.return_value
        self.redshift_db_driver.set_grant_permissions_for_groups(mock_schema,
                                                                 mock_table,
                                                                 groups,
                                                                 None,
                                                                 db_conn=mock_conn)
        mock_conn.execute.assert_called_with(
            f'GRANT ALL ON {mock_schema}.{mock_table} TO "a_group"\n')

    def test_best_scheme_to_load_from(self):
        out = self.redshift_db_driver.loader().best_scheme_to_load_from()
        self.assertEqual(out, "s3")

    def test_supports_time_type(self):
        out = self.redshift_db_driver.supports_time_type()
        self.assertEqual(out, False)

    def test_temporary_loadable_directory_loc(self):
        with self.redshift_db_driver.loader().temporary_loadable_directory_loc() as loc:
            self.assertEqual(loc,
                             self.mock_s3_temp_base_loc.temporary_directory.return_value.
                             __enter__.return_value)

    def test_integer_limits(self):
        expectations = {
            sqlalchemy.sql.sqltypes.SMALLINT(): (-32768, 32767),
            sqlalchemy.sql.sqltypes.INTEGER(): (-2147483648, 2147483647),
            sqlalchemy.sql.sqltypes.BIGINT(): (-9223372036854775808, 9223372036854775807),
        }
        for mock_type, (expected_min_int, expected_max_int) in expectations.items():
            min_int, max_int = self.redshift_db_driver.integer_limits(mock_type)
            self.assertEqual(min_int, expected_min_int)
            self.assertEqual(max_int, expected_max_int)

    def test_fp_constraints(self):
        expectations = {
            sqlalchemy.dialects.postgresql.base.DOUBLE_PRECISION(): (64, 53),
            sqlalchemy.sql.sqltypes.REAL(): (32, 23),
        }
        for mock_type, (expected_total_bits, expected_significand_bits) in expectations.items():
            total_bits, significand_bits =\
                self.redshift_db_driver.fp_constraints(mock_type)
            self.assertEqual(total_bits, expected_total_bits)
            self.assertEqual(significand_bits, expected_significand_bits)

    def test_type_for_integer(self):
        expectations = {
            (-1, 1): sqlalchemy.sql.sqltypes.SMALLINT,
            (-100000, 100000): sqlalchemy.sql.sqltypes.INTEGER,
            (-10000000000, 10000000000): sqlalchemy.sql.sqltypes.BIGINT,
            (-100000000000000000000, 100000000000000000000): sqlalchemy.sql.sqltypes.Numeric,
        }
        for (min_value, max_value), expected_type in expectations.items():
            actual_col_type = self.redshift_db_driver.type_for_integer(min_value, max_value)
            self.assertEqual(type(actual_col_type), expected_type)

    def test_type_for_fixed_point(self):
        expectations = {
            (10, 3): sqlalchemy.sql.sqltypes.Numeric,
            (100, 50): sqlalchemy.dialects.postgresql.base.DOUBLE_PRECISION,
        }
        for (precision, scale), expected_type in expectations.items():
            actual_col_type = self.redshift_db_driver.type_for_fixed_point(precision, scale)
            self.assertEqual(type(actual_col_type), expected_type)

    def test_type_for_floating_point(self):
        expectations = {
            (64, 49): 49,
            (100, 80): 53,
        }
        for (input_fp_total_bits, input_fp_significand_bits), \
                expected_fp_significand_bits in expectations.items():
            actual_col_type =\
                self.redshift_db_driver.type_for_floating_point(input_fp_total_bits,
                                                                input_fp_significand_bits)
            self.assertEqual(type(actual_col_type), sqlalchemy.sql.sqltypes.Float)
            self.assertEqual(actual_col_type.precision, expected_fp_significand_bits)
