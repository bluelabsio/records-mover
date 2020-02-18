import unittest
from records_mover.records.targets.table import TableRecordsTarget
from mock import Mock, MagicMock


class TestTableFormatNegotiation(unittest.TestCase):
    def setUp(self):
        self.mock_schema_name = Mock(name='schema_name')
        self.mock_table_name = Mock(name='table_name')
        self.mock_db_engine = MagicMock(name='db_engine')
        self.mock_db_driver = Mock(name='db_driver')
        self.mock_permissions_users = Mock(name='permissions_users')
        self.mock_permissions_groups = Mock(name='permissions_groups')
        self.mock_existing_table_handling = Mock(name='existing_table_handling')
        mock_driver = self.mock_db_driver.return_value
        mock_records_format = Mock(name='records_format')
        mock_driver.known_supported_records_formats_for_load.return_value = [mock_records_format]
        self.table = TableRecordsTarget(self.mock_schema_name,
                                        self.mock_table_name,
                                        self.mock_db_engine,
                                        self.mock_db_driver,
                                        add_user_perms_for=self.mock_permissions_users,
                                        add_group_perms_for=self.mock_permissions_groups,
                                        existing_table_handling=self.mock_existing_table_handling)

    def test_known_supported_records_formats(self):
        out = self.table.known_supported_records_formats()
        self.assertEqual(out,
                         self.mock_db_driver.return_value.
                         known_supported_records_formats_for_load.return_value)
