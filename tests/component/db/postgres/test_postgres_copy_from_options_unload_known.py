import unittest
from records_mover.db.postgres.unloader import PostgresUnloader
from records_mover.db.postgres.copy_options import postgres_copy_to_options
from mock import Mock


class TestPostgresCopyOptionsUnloadKnown(unittest.TestCase):
    def test_unload_known_formats(self):
        mock_db = Mock(name='db')
        loader = PostgresUnloader(db=mock_db)
        known_unload_formats = loader.known_supported_records_formats_for_unload()
        for records_format in known_unload_formats:
            unhandled_hints = set(records_format.hints)
            # ensure no exception thrown
            postgres_copy_to_options(unhandled_hints,
                                     records_format,
                                     fail_if_cant_handle_hint=True)
            self.assertFalse(unhandled_hints)
