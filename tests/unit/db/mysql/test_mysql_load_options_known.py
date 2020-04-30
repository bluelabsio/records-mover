import unittest
from records_mover.records.hints import complain_on_unhandled_hints
from records_mover.db.mysql.loader import MySQLLoader
from records_mover.db.mysql.load_options import mysql_load_options
from mock import Mock


class TestMySQLLoadOptionsKnown(unittest.TestCase):
    def test_load_known_formats(self):
        mock_db = Mock(name='db')
        mock_url_resolver = Mock(name='url_resolver')
        loader = MySQLLoader(db=mock_db,
                             url_resolver=mock_url_resolver)
        known_load_formats = loader.known_supported_records_formats_for_load()
        for records_format in known_load_formats:
            unhandled_hints = set()
            # ensure no exception thrown
            mysql_load_options(unhandled_hints,
                               records_format,
                               fail_if_cant_handle_hint=True)
            complain_on_unhandled_hints(fail_if_dont_understand=True,
                                        unhandled_hints=unhandled_hints,
                                        hints=records_format.hints)
