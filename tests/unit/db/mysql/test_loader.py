from unittest.mock import MagicMock, patch, Mock
import unittest
from records_mover.records import DelimitedRecordsFormat
from records_mover.db.mysql.loader import MySQLLoader


class TestLoader(unittest.TestCase):
    def setUp(self):
        self.mock_db_engine = MagicMock(name='db_engine')
        self.mock_db_engine.engine = self.mock_db_engine
        self.loader = MySQLLoader(db=self.mock_db_engine)

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
