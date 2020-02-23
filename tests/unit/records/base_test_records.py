from records_mover.records.records import (Records)
from mock import Mock
import unittest


class BaseTestRecords(unittest.TestCase):
    def fake_quote_schema_and_table(self, engine, schema, table):
        return f"[{schema}].[{table}]"

    def setUp(self):
        self.mock_logger = Mock(name='logger')

        self.mock_db_driver = Mock(name='db_driver')
        self.mock_db_driver.temporary_loadable_directory_loc.return_value.__enter__ = Mock()
        self.mock_db_driver.temporary_loadable_directory_loc.return_value.__exit__ = Mock()

        self.mock_url_resolver = Mock(name='url_resolver')
        self.mock_creds = Mock(name='creds')
        self.mock_file_url = self.mock_url_resolver.mock_file_url
        self.mock_file_url.return_value.open.return_value.__enter__ = Mock()
        self.mock_file_url.return_value.open.return_value.__exit__ = Mock()

        self.mock_directory_url = self.mock_url_resolver.directory_url

        # Default to writing to an empty directory
        self.mock_directory_url.return_value.files_in_directory.return_value = []

        def fake_db_driver(_db_engine):
            return self.mock_db_driver

        self.records = Records(db_driver=fake_db_driver,
                               url_resolver=self.mock_url_resolver)
