import unittest

from records_mover.db.bigquery.bigquery_db_driver import BigQueryDBDriver
from records_mover.records.records_format import DelimitedRecordsFormat
from mock import MagicMock, Mock, patch
import sqlalchemy


class TestBigQueryDBDriver(unittest.TestCase):
    @patch('records_mover.db.bigquery.bigquery_db_driver.BigQueryLoader')
    def setUp(self, mock_BigQueryLoader):
        self.mock_db_engine = MagicMock(name='db_engine')
        self.mock_url_resolver = Mock(name='url_resolver')
        self.mock_BigQueryLoader = mock_BigQueryLoader
        self.bigquery_db_driver = BigQueryDBDriver(db=self.mock_db_engine,
                                                   url_resolver=self.mock_url_resolver)

    def test_load_implemented(self):
        mock_schema = Mock(name='mock_schema')
        mock_table = Mock(name='mock_table')
        mock_load_plan = Mock(name='mock_load_plan')
        mock_load_plan.records_format = Mock(name='records_format', spec=DelimitedRecordsFormat)
        mock_load_plan.records_format.hints = {}
        mock_directory = Mock(name='mock_directory')
        ret = self.bigquery_db_driver.loader_from_records_directory().\
            load(schema=mock_schema,
                 table=mock_table,
                 load_plan=mock_load_plan,
                 directory=mock_directory)
        self.assertEqual(ret, self.mock_BigQueryLoader.return_value.load.return_value)

    def test_can_load_this_format(self):
        mock_source_records_format = Mock(name='source_records_format', spec=DelimitedRecordsFormat)
        out = self.bigquery_db_driver.loader_from_fileobj().\
            can_load_this_format(mock_source_records_format)
        self.mock_BigQueryLoader.return_value.can_load_this_format.\
            assert_called_with(mock_source_records_format)
        self.assertEqual(out,
                         self.mock_BigQueryLoader.return_value.can_load_this_format.return_value)

    def test_known_supported_records_formats_for_load(self):
        out = self.bigquery_db_driver.loader().known_supported_records_formats_for_load()
        self.mock_BigQueryLoader.return_value.known_supported_records_formats_for_load.\
            assert_called_with()
        self.assertEqual(out,
                         self.mock_BigQueryLoader.return_value.
                         known_supported_records_formats_for_load.return_value)

    def test_best_records_format_variant_delimited(self):
        out = self.bigquery_db_driver.best_records_format_variant('delimited')
        self.assertEqual(out, 'bigquery')

    def test_best_records_format_variant_non_delimited(self):
        out = self.bigquery_db_driver.best_records_format_variant('whatevs')
        self.assertIsNone(out)

    def test_type_for_date_plus_time_with_tz(self):
        out = self.bigquery_db_driver.type_for_date_plus_time(has_tz=True)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.TIMESTAMP)

    def test_type_for_date_plus_time_with_no_tz(self):
        out = self.bigquery_db_driver.type_for_date_plus_time(has_tz=False)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.DATETIME)

    def test_make_column_name_valid(self):
        expected = {
            'foo bar': 'foo_bar',
            'foo-bar': 'foo_bar',
        }
        for colname_input, expected_colname_output in expected.items():
            colname_output = self.bigquery_db_driver.make_column_name_valid(colname_input)
            self.assertEqual(colname_output, expected_colname_output)

    def test_integer_limits(self):
        self.assertEqual(self.bigquery_db_driver.integer_limits(sqlalchemy.types.Integer()),
                         (-9223372036854775808, 9223372036854775807))

    def test_fp_constraints(self):
        self.assertEqual(self.bigquery_db_driver.fp_constraints(sqlalchemy.types.Float()),
                         (64, 53))

    def test_fixed_point_constraints(self):
        constraints = self.bigquery_db_driver.fixed_point_constraints(sqlalchemy.types.DECIMAL())
        self.assertEqual(constraints, (38, 9))

    def test_fixed_point_constraints_new_type(self):
        constraints = self.bigquery_db_driver.fixed_point_constraints(Mock())
        self.assertIsNone(constraints, None)

    def test_type_for_fixed_point_big(self):
        type_ = self.bigquery_db_driver.type_for_fixed_point(123, 45)
        self.assertEqual(type(type_), sqlalchemy.types.Float)

    def test_type_for_fixed_point_small(self):
        type_ = self.bigquery_db_driver.type_for_fixed_point(12, 3)
        self.assertEqual(type(type_), sqlalchemy.types.Numeric)

    def test_type_for_integer(self):
        type_ = self.bigquery_db_driver.type_for_integer(min_value=1, max_value=4)
        self.assertEqual(type(type_), sqlalchemy.types.Integer)

    def test_load_from_fileobj(self):
        mock_schema = Mock(name='schema')
        mock_table = Mock(name='table')
        mock_load_plan = Mock(name='load_plan')
        mock_fileobj = Mock(name='fileobj')
        mock_bigquery_loader = self.mock_BigQueryLoader.return_value
        out = self.bigquery_db_driver.loader_from_fileobj().\
            load_from_fileobj(mock_schema, mock_table,
                              mock_load_plan, mock_fileobj)
        mock_bigquery_loader.load_from_fileobj.assert_called_with(mock_schema,
                                                                  mock_table,
                                                                  mock_load_plan,
                                                                  mock_fileobj)
        self.assertEqual(out, mock_bigquery_loader.load_from_fileobj.return_value)

    def test_type_for_integer_small_type(self):
        INT64_MIN = -9223372036854775808
        min_value = INT64_MIN - 100
        max_value = 200
        out = self.bigquery_db_driver.type_for_integer(min_value, max_value)
        self.assertEqual(type(out), sqlalchemy.types.Numeric)
