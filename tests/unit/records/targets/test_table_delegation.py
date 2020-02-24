import unittest
from records_mover.records.targets.table import TableRecordsTarget
from mock import Mock, MagicMock, patch


class FakeDBException(Exception):
    pass


class TestTableFileObjects(unittest.TestCase):
    @patch('records_mover.records.targets.table.target.TablePrep')
    def setUp(self, mock_TablePrep):
        self.mock_schema_name = Mock(name='schema_name')
        self.mock_table_name = Mock(name='table_name')
        self.mock_db_engine = MagicMock(name='db_engine')
        self.mock_db_driver = Mock(name='db_driver')
        self.mock_permissions_users = Mock(name='permissions_users')
        self.mock_permissions_groups = Mock(name='permissions_groups')
        self.mock_existing_table_handling = Mock(name='existing_table_handling')
        self.mock_prep = mock_TablePrep.return_value
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

    @patch('records_mover.records.targets.table.target.DoMoveFromRecordsDirectory')
    def test_move_from_records_directory(self, mock_DoMoveFromRecordsDirectory):
        mock_directory = Mock(name='directory')
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_override_records_format = Mock(name='override_records_format')
        out = self.table.move_from_records_directory(mock_directory,
                                                     mock_processing_instructions,
                                                     mock_override_records_format)
        self.assertEqual(out, mock_DoMoveFromRecordsDirectory.return_value.move.return_value)
        mock_DoMoveFromRecordsDirectory.assert_called_with(self.mock_prep,
                                                           self.table,
                                                           mock_directory,
                                                           mock_processing_instructions,
                                                           mock_override_records_format)
        mock_DoMoveFromRecordsDirectory.return_value.move.assert_called_with()

    @patch('records_mover.records.targets.table.target.DoMoveFromFileobjsSource')
    def test_move_from_fileobjs_source(self, mock_DoMoveFromFileobjsSource):
        mock_fileobjs_source = Mock(name='fileobjs_source')
        mock_processing_instructions = Mock(name='processing_instructions')
        out = self.table.move_from_fileobjs_source(mock_fileobjs_source,
                                                   mock_processing_instructions)
        self.assertEqual(out, mock_DoMoveFromFileobjsSource.return_value.move.return_value)
        mock_DoMoveFromFileobjsSource.assert_called_with(self.mock_prep,
                                                         self.table,
                                                         mock_fileobjs_source,
                                                         mock_processing_instructions)
        mock_DoMoveFromFileobjsSource.return_value.move.assert_called_with()

    @patch('records_mover.records.targets.table.target.DoMoveFromTempLocAfterFillingIt')
    def test_move_from_temp_loc_after_filling_it(self, mock_DoMoveFromTempLocAfterFillingIt):
        mock_records_source = Mock(name='records_source')
        mock_processing_instructions = Mock(name='processing_instructions')
        out = self.table.move_from_temp_loc_after_filling_it(mock_records_source,
                                                             mock_processing_instructions)
        self.assertEqual(out, mock_DoMoveFromTempLocAfterFillingIt.return_value.move.return_value)
        mock_DoMoveFromTempLocAfterFillingIt.assert_called_with(self.mock_prep,
                                                                self.table,
                                                                self.table,
                                                                mock_records_source,
                                                                mock_processing_instructions)
        mock_DoMoveFromTempLocAfterFillingIt.return_value.move.assert_called_with()
