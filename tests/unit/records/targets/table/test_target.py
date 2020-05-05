import unittest
from mock import Mock
from records_mover.records.targets.table import TableRecordsTarget


class TestTarget(unittest.TestCase):
    def setUp(self):

        self.mock_schema_name = Mock(name='schema_name')
        self.mock_table_name = Mock(name='table_name')
        self.mock_db_engine = Mock(name='db_engine')
        self.mock_db_driver = Mock(name='db_engine')
        self.mock_add_user_perms_for = Mock(name='add_user_perms_for')
        self.mock_add_group_perms_for = Mock(name='add_group_perms_for')
        self.mock_existing_table_handling = Mock(name='existing_table_handling')
        self.mock_drop_and_recreate_on_load_error =\
            Mock(name='drop_and_recreate_on_load_error')

        mock_driver = self.mock_db_driver.return_value
        mock_loader = mock_driver.loader.return_value
        mock_supported_records_format_1 = Mock(name='supported_records_format_1')
        mock_known_supported_records_formats = [mock_supported_records_format_1]
        mock_loader.known_supported_records_formats_for_load.return_value =\
            mock_known_supported_records_formats

        self.target =\
            TableRecordsTarget(schema_name=self.mock_schema_name,
                               table_name=self.mock_table_name,
                               db_engine=self.mock_db_engine,
                               db_driver=self.mock_db_driver,
                               add_user_perms_for=self.mock_add_user_perms_for,
                               add_group_perms_for=self.mock_add_group_perms_for,
                               existing_table_handling=self.mock_existing_table_handling,
                               drop_and_recreate_on_load_error=
                               self.mock_drop_and_recreate_on_load_error)

    def test_can_move_from_fileobjs_source_yes(self):
        self.assertTrue(self.target.can_move_from_fileobjs_source())

        self.mock_db_driver.assert_called_with(self.mock_db_engine)

    def test_can_load_direct_no_loader(self):
        mock_driver = self.mock_db_driver.return_value
        mock_driver.loader.return_value = None
        self.assertFalse(self.target.can_load_direct('whatever'))

        self.mock_db_driver.assert_called_with(self.mock_db_engine)

    def test_known_supported_records_formats_no_loader(self):
        mock_driver = self.mock_db_driver.return_value
        mock_driver.loader.return_value = None
        self.assertEqual([], self.target.known_supported_records_formats())

        self.mock_db_driver.assert_called_with(self.mock_db_engine)

    def test_can_move_from_this_format_no_loader(self):
        mock_driver = self.mock_db_driver.return_value
        mock_source_records_format = Mock(name='source_records_format')
        mock_driver.loader.return_value = None
        self.assertFalse(self.target.can_move_from_this_format(mock_source_records_format))

        self.mock_db_driver.assert_called_with(self.mock_db_engine)
