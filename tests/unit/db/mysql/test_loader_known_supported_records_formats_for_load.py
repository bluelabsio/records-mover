import unittest
from records_mover.db.mysql.loader import MySQLLoader
from records_mover.db.mysql.load_options import mysql_load_options
from mock import Mock


class TestMySQLLoadOptionsKnown(unittest.TestCase):
    def test_load_known_formats(self):
        mock_db = Mock(name='db')
        loader = MySQLLoader(db=mock_db)
        known_load_formats = loader.known_supported_records_formats_for_load()
        for records_format in known_load_formats:
            unhandled_hints = set(records_format.hints.keys())
            # ensure no exception thrown
            mysql_load_options(unhandled_hints,
                               records_format,
                               fail_if_cant_handle_hint=True)
            self.assertEqual(len(unhandled_hints), 0)
