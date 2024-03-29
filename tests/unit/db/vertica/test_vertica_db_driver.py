from .base_test_vertica_db_driver import BaseTestVerticaDBDriver
from mock import Mock
from ...records.format_hints import (vertica_format_hints)
import sqlalchemy


class TestVerticaDBDriver(BaseTestVerticaDBDriver):
    def test_unload(self):
        mock_result = Mock(name='result')
        mock_result.rows = 579
        self.mock_db_engine.execute.return_value.fetchall.return_value = [mock_result]
        self.mock_records_unload_plan.processing_instructions.fail_if_dont_understand = True
        self.mock_records_unload_plan.processing_instructions.fail_if_cant_handle_hint = True

        self.mock_records_unload_plan.records_format.hints = vertica_format_hints
        self.mock_directory.scheme = 's3'
        export_count = self.vertica_db_driver\
            .unloader().unload(schema='myschema',
                               table='mytable',
                               unload_plan=self.mock_records_unload_plan,
                               directory=self.mock_directory)

        self.assertEqual(579, export_count)

    def test_unload_to_non_s3(self):
        mock_result = Mock(name='result')
        mock_result.rows = 579
        self.mock_db_engine.execute.return_value.fetchall.return_value = [mock_result]
        self.mock_records_unload_plan.processing_instructions.fail_if_dont_understand = True
        self.mock_records_unload_plan.processing_instructions.fail_if_cant_handle_hint = True

        self.mock_records_unload_plan.records_format.hints = vertica_format_hints
        self.mock_directory.scheme = 'mumble'
        mock_load_loc = self.mock_s3_temp_base_loc.temporary_directory().__enter__()
        mock_load_creds = mock_load_loc.aws_creds()
        mock_load_creds.token = None
        export_count = self.vertica_db_driver\
            .unloader().unload(schema='myschema',
                               table='mytable',
                               unload_plan=self.mock_records_unload_plan,
                               directory=self.mock_directory)

        self.assertEqual(579, export_count)

    def test_schema_sql(self):
        mock_result = Mock(name='result')
        self.mock_db_engine.execute.return_value.fetchall.return_value = [mock_result]
        sql = self.vertica_db_driver.schema_sql('myschema', 'mytable')
        self.assertEqual(sql, mock_result.EXPORT_OBJECTS)

    def test_schema_sql_but_not_from_export_objects(self):
        self.mock_db_engine.execute.return_value.fetchall.return_value = []
        sql = self.vertica_db_driver.schema_sql('myschema', 'mytable')
        self.assertTrue(sql is not None)

    def test_has_table_true(self):
        mock_schema = 'myschema'
        mock_table = 'mytable'
        self.assertEqual(True,
                         self.vertica_db_driver.has_table(mock_schema, mock_table))

    def test_has_table_false(self):
        mock_schema = 'myschema'
        mock_table = 'mytable'
        self.mock_db_engine.execute.side_effect = sqlalchemy.exc.ProgrammingError('statement', {},
                                                                                  'orig')
        self.assertEqual(False,
                         self.vertica_db_driver.has_table(mock_schema, mock_table))

    def test_can_load_this_format(self):
        mock_source_records_format = Mock(name='source_records_format')
        out = self.vertica_db_driver.loader().can_load_this_format(mock_source_records_format)
        self.assertEqual(self.mock_vertica_loader.can_load_this_format.return_value,
                         out)
        self.mock_vertica_loader.can_load_this_format.assert_called_with(mock_source_records_format)

    def test_integer_limits(self):
        int_min, int_max = self.vertica_db_driver.integer_limits(sqlalchemy.sql.sqltypes.INTEGER())
        self.assertEqual(int_min, -9223372036854775808)
        self.assertEqual(int_max, 9223372036854775807)

    def test_fp_constraints(self):
        total_bits, significand_bits =\
            self.vertica_db_driver.fp_constraints(sqlalchemy.sql.sqltypes.FLOAT())
        self.assertEqual(total_bits, 64)
        self.assertEqual(significand_bits, 53)

    def test_type_for_integer_fits(self):
        out = self.vertica_db_driver.type_for_integer(-123, 123)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.INTEGER)

    def test_type_for_integer_too_big(self):
        out = self.vertica_db_driver.type_for_integer(-12300000000000000000, 123000000000000000000)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.Numeric)

    def test_type_for_floating_point_too_big(self):
        out = self.vertica_db_driver.type_for_floating_point(100, 80)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.Float)
        self.assertEqual(out.precision, 53)

    def test_type_for_floating_point_fits(self):
        out = self.vertica_db_driver.type_for_floating_point(12, 8)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.Float)
        self.assertEqual(out.precision, 8)
