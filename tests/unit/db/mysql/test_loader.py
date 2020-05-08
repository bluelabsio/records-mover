from unittest.mock import MagicMock, patch, Mock
import unittest
from records_mover.url.filesystem import FilesystemDirectoryUrl, FilesystemFileUrl
from records_mover.records import DelimitedRecordsFormat
from records_mover.db.mysql.loader import MySQLLoader


class TestLoader(unittest.TestCase):
    def setUp(self):
        self.mock_url_resolver = Mock(name='url_resolver')
        self.mock_db_engine = MagicMock(name='db_engine')
        self.mock_db_engine.engine = self.mock_db_engine
        self.loader = MySQLLoader(db=self.mock_db_engine,
                                  url_resolver=self.mock_url_resolver)

    @patch('records_mover.db.mysql.loader.complain_on_unhandled_hints')
    @patch('records_mover.db.mysql.loader.mysql_load_options')
    def test_can_load_this_format_true(self,
                                       mock_mysql_load_options,
                                       mock_complain_on_unhandled_hints):
        source_records_format = Mock(name='source_records_format',
                                     spec=DelimitedRecordsFormat)
        source_records_format.hints = {}
        out = self.loader.can_load_this_format(source_records_format)
        self.assertTrue(out)

    @patch('records_mover.db.mysql.loader.complain_on_unhandled_hints')
    @patch('records_mover.db.mysql.loader.mysql_load_options')
    def test_can_load_this_format_false(self,
                                        mock_mysql_load_options,
                                        mock_complain_on_unhandled_hints):
        source_records_format = Mock(name='source_records_format',
                                     spec=DelimitedRecordsFormat)
        source_records_format.hints = {}
        mock_mysql_load_options.side_effect = NotImplementedError
        out = self.loader.can_load_this_format(source_records_format)
        self.assertFalse(out)

    @patch('records_mover.db.mysql.loader.mysql_load_options')
    @patch('records_mover.db.mysql.loader.complain_on_unhandled_hints')
    @patch('records_mover.db.mysql.loader.tempfile')
    @patch('records_mover.db.mysql.loader.Path')
    def test_load_happy_path(self,
                             mock_Path,
                             mock_tempfile,
                             mock_complain_on_unhandled_hints,
                             mock_mysql_load_options):
        mock_schema = Mock(name='schema')
        mock_table = Mock(name='table')
        mock_load_plan = Mock(name='load_plan')
        mock_records_format = Mock(name='records_format', spec=DelimitedRecordsFormat)
        mock_load_plan.records_format = mock_records_format
        mock_hints = {}
        mock_records_format.hints = mock_hints
        mock_directory = Mock(name='directory')
        mock_directory.loc = Mock(name='loc', spec=FilesystemDirectoryUrl)
        mock_url = Mock(name='url')
        mock_directory.manifest_entry_urls.return_value = [mock_url]
        mock_loc = Mock(name='loc', spec=FilesystemFileUrl)
        mock_loc.local_file_path = Mock(name='local_file_path')
        self.mock_url_resolver.file_url.return_value = mock_loc
        self.mock_db_engine.execute.return_value = 123

        mock_load_options = mock_mysql_load_options.return_value
        mock_sql = mock_load_options.generate_load_data_sql.return_value
        out = self.loader.load(mock_schema,
                               mock_table,
                               mock_load_plan,
                               mock_directory)
        self.mock_db_engine.execute.assert_called_with(mock_sql)
        self.assertEqual(out, None)
