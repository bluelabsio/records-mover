import unittest
from records_mover.records.targets.table import TableRecordsTarget
from mock import Mock, MagicMock


class FakeDBException(Exception):
    pass


class TestTableFileObjects(unittest.TestCase):
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
        self.mock_loader = mock_driver.loader.return_value
        self.mock_loader_from_fileobj = mock_driver.loader_from_fileobj.return_value
        self.mock_loader.known_supported_records_formats_for_load.return_value = [mock_records_format]
        self.table = TableRecordsTarget(self.mock_schema_name,
                                        self.mock_table_name,
                                        self.mock_db_engine,
                                        self.mock_db_driver,
                                        add_user_perms_for=self.mock_permissions_users,
                                        add_group_perms_for=self.mock_permissions_groups,
                                        existing_table_handling=self.mock_existing_table_handling)

    def test_can_move_from_fileobjs_source(self):
        out = self.table.can_move_from_fileobjs_source()
        self.assertEqual(out, self.mock_loader_from_fileobj.can_load_from_fileobjs.return_value)

    def test_can_load_direct(self):
        mock_scheme = Mock(name='scheme')
        mock_driver = self.mock_db_driver.return_value
        mock_loader_from_records_directory = mock_driver.loader_from_records_directory.return_value
        mock_loader_from_records_directory.best_scheme_to_load_from.return_value = mock_scheme
        self.assertEqual(True, self.table.can_load_direct(mock_scheme))
        self.mock_db_driver.assert_called_with(self.mock_db_engine)
