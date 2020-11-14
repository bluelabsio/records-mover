import unittest
import sqlalchemy
from records_mover.db.postgres.loader import PostgresLoader
from records_mover.records import DelimitedRecordsFormat
from mock import MagicMock, Mock, patch


class TestPostgresLoader(unittest.TestCase):
    def setUp(self):
        self.mock_url_resolver = Mock(name='url_resolver')
        self.mock_meta = Mock(name='meta')
        self.mock_db = MagicMock(name='db')
        self.loader = PostgresLoader(self.mock_url_resolver,
                                     self.mock_meta,
                                     self.mock_db)

    @patch('records_mover.db.postgres.loader.quote_value')
    @patch('records_mover.db.postgres.loader.copy_from')
    @patch('records_mover.db.postgres.loader.complain_on_unhandled_hints')
    @patch('records_mover.db.postgres.loader.Table')
    @patch('records_mover.db.postgres.loader.postgres_copy_from_options')
    def test_load_from_fileobj(self,
                               mock_postgres_copy_from_options,
                               mock_Table,
                               mock_complain_on_unhandled_hints,
                               mock_copy_from,
                               mock_quote_value):
        mock_schema = Mock(name='schema')
        mock_table = Mock(name='table')
        mock_load_plan = Mock(name='load_plan')
        mock_fileobj = Mock(name='fileobj')

        mock_records_format = Mock(name='records_format',
                                   spec=DelimitedRecordsFormat)
        mock_records_format.hints = {}
        mock_load_plan.records_format = mock_records_format
        mock_date_order_style = "DATE_ORDER_STYLE"
        mock_postgres_options = {
            'abc': 123
        }
        mock_postgres_copy_from_options.return_value = (
            mock_date_order_style,
            mock_postgres_options,
        )
        mock_quote_value.return_value = "ABC"
        self.loader.load_from_fileobj(mock_schema,
                                      mock_table,
                                      mock_load_plan,
                                      mock_fileobj)
        mock_processing_instructions = mock_load_plan.processing_instructions
        mock_unhandled_hints = set(mock_records_format.hints.keys())
        mock_complain_on_unhandled_hints.\
            assert_called_with(mock_processing_instructions.fail_if_dont_understand,
                               mock_unhandled_hints,
                               mock_records_format.hints)
        mock_table_obj = mock_Table.return_value
        mock_Table.assert_called_with(mock_table,
                                      self.mock_meta,
                                      schema=mock_schema,
                                      autoload=True,
                                      autoload_with=self.mock_db)
        mock_conn = self.mock_db.engine.begin.return_value.__enter__.return_value
        mock_quote_value.assert_called_with(mock_conn, 'ISO, DATE_ORDER_STYLE')
        mock_conn.execute.assert_called_with('SET LOCAL DateStyle = ABC')
        mock_copy_from.assert_called_with(mock_fileobj,
                                          mock_table_obj,
                                          mock_conn,
                                          abc=123)

    @patch('records_mover.db.postgres.loader.quote_value')
    @patch('records_mover.db.postgres.loader.copy_from')
    @patch('records_mover.db.postgres.loader.complain_on_unhandled_hints')
    @patch('records_mover.db.postgres.loader.Table')
    @patch('records_mover.db.postgres.loader.postgres_copy_from_options')
    def test_load_from_fileobj_default_date_order_style(self,
                                                        mock_postgres_copy_from_options,
                                                        mock_Table,
                                                        mock_complain_on_unhandled_hints,
                                                        mock_copy_from,
                                                        mock_quote_value):
        mock_schema = Mock(name='schema')
        mock_table = Mock(name='table')
        mock_load_plan = Mock(name='load_plan')
        mock_fileobj = Mock(name='fileobj')

        mock_records_format = Mock(name='records_format',
                                   spec=DelimitedRecordsFormat)
        mock_records_format.hints = {}
        mock_load_plan.records_format = mock_records_format
        mock_date_order_style = None
        mock_postgres_options = {
            'abc': 123
        }
        mock_postgres_copy_from_options.return_value = (
            mock_date_order_style,
            mock_postgres_options,
        )
        mock_quote_value.return_value = "ABC"
        self.loader.load_from_fileobj(mock_schema,
                                      mock_table,
                                      mock_load_plan,
                                      mock_fileobj)
        mock_processing_instructions = mock_load_plan.processing_instructions
        mock_unhandled_hints = set(mock_records_format.hints.keys())
        mock_complain_on_unhandled_hints.\
            assert_called_with(mock_processing_instructions.fail_if_dont_understand,
                               mock_unhandled_hints,
                               mock_records_format.hints)
        mock_table_obj = mock_Table.return_value
        mock_Table.assert_called_with(mock_table,
                                      self.mock_meta,
                                      schema=mock_schema,
                                      autoload=True,
                                      autoload_with=self.mock_db)
        mock_conn = self.mock_db.engine.begin.return_value.__enter__.return_value
        mock_quote_value.assert_called_with(mock_conn, 'ISO, MDY')
        mock_conn.execute.assert_called_with('SET LOCAL DateStyle = ABC')
        mock_copy_from.assert_called_with(mock_fileobj,
                                          mock_table_obj,
                                          mock_conn,
                                          abc=123)

    @patch('records_mover.db.loader.ConcatFiles')
    @patch('records_mover.db.postgres.loader.quote_value')
    @patch('records_mover.db.postgres.loader.copy_from')
    @patch('records_mover.db.postgres.loader.complain_on_unhandled_hints')
    @patch('records_mover.db.postgres.loader.Table')
    @patch('records_mover.db.postgres.loader.postgres_copy_from_options')
    def test_load(self,
                  mock_postgres_copy_from_options,
                  mock_Table,
                  mock_complain_on_unhandled_hints,
                  mock_copy_from,
                  mock_quote_value,
                  mock_ConcatFiles):
        mock_directory = Mock(name='directory')
        mock_url = Mock(name='url')
        mock_directory.manifest_entry_urls.return_value = [mock_url]
        mock_loc = MagicMock(name='loc')
        self.mock_url_resolver.file_url.return_value = mock_loc

        mock_schema = Mock(name='schema')
        mock_table = Mock(name='table')
        mock_load_plan = Mock(name='load_plan')

        mock_records_format = Mock(name='records_format',
                                   spec=DelimitedRecordsFormat)
        mock_records_format.hints = {}
        mock_load_plan.records_format = mock_records_format
        mock_date_order_style = "DATE_ORDER_STYLE"
        mock_postgres_options = {
            'abc': 123
        }
        mock_postgres_copy_from_options.return_value = (
            mock_date_order_style,
            mock_postgres_options,
        )
        mock_quote_value.return_value = "ABC"

        self.loader.load(mock_schema,
                         mock_table,
                         mock_load_plan,
                         mock_directory)

        self.mock_url_resolver.file_url.assert_called_with(mock_url)

        mock_processing_instructions = mock_load_plan.processing_instructions
        mock_unhandled_hints = set(mock_records_format.hints.keys())
        mock_complain_on_unhandled_hints.\
            assert_called_with(mock_processing_instructions.fail_if_dont_understand,
                               mock_unhandled_hints,
                               mock_records_format.hints)
        mock_table_obj = mock_Table.return_value
        mock_Table.assert_called_with(mock_table,
                                      self.mock_meta,
                                      schema=mock_schema,
                                      autoload=True,
                                      autoload_with=self.mock_db)
        mock_conn = self.mock_db.engine.begin.return_value.__enter__.return_value
        mock_quote_value.assert_called_with(mock_conn, 'ISO, DATE_ORDER_STYLE')
        mock_conn.execute.assert_called_with('SET LOCAL DateStyle = ABC')
        mock_copy_from.assert_called_with(mock_loc.open.return_value.__enter__.return_value,
                                          mock_table_obj,
                                          mock_conn,
                                          abc=123)

    @patch('records_mover.db.postgres.loader.complain_on_unhandled_hints')
    @patch('records_mover.db.postgres.loader.postgres_copy_from_options')
    def test_can_load_this_format_true(self,
                                       mock_postgres_copy_from_options,
                                       mock_complain_on_unhandled_hints):
        source_records_format = Mock(name='source_records_format',
                                     spec=DelimitedRecordsFormat)
        source_records_format.hints = {}
        out = self.loader.can_load_this_format(source_records_format)
        self.assertTrue(out)

    def test_load_failure_exception(self):
        self.assertEqual(sqlalchemy.exc.InternalError,
                         self.loader.load_failure_exception())

    def test_best_scheme_to_load_from(self):
        self.assertEqual('file',
                         self.loader.best_scheme_to_load_from())

    @patch('records_mover.db.loader.TemporaryDirectory')
    @patch('records_mover.db.loader.FilesystemDirectoryUrl')
    def test_temporary_loadable_directory_loc(self,
                                              mock_FilesystemDirectoryUrl,
                                              mock_TemporaryDirectory):
        with self.loader.temporary_loadable_directory_loc() as loc:
            self.assertEqual(loc, mock_FilesystemDirectoryUrl.return_value)
