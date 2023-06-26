from records_mover.records.targets.factory import RecordsTargets
from records_mover.records.existing_table_handling import ExistingTableHandling
from mock import MagicMock, Mock, patch
import unittest


class TestFactory(unittest.TestCase):
    def setUp(self):
        self.mock_db_driver = Mock(name='db_driver')
        self.mock_url_resolver = MagicMock(name='url_resolver')
        self.mock_creds = MagicMock(name='creds')

        self.records_targets = RecordsTargets(db_driver=self.mock_db_driver,
                                              url_resolver=self.mock_url_resolver)

    @patch('records_mover.records.targets.directory_from_url.DirectoryFromUrlRecordsTarget')
    def test_directory_from_url(self, mock_DirectoryFromUrlRecordsTarget):
        mock_output_url = Mock(name='output_url')
        mock_records_format = Mock(name='records_format')
        directory = self.records_targets.\
            directory_from_url(output_url=mock_output_url,
                               records_format=mock_records_format)
        mock_DirectoryFromUrlRecordsTarget.\
            assert_called_with(output_url=mock_output_url,
                               records_format=mock_records_format,
                               url_resolver=self.mock_url_resolver)
        self.assertEqual(directory, mock_DirectoryFromUrlRecordsTarget.return_value)

    @patch('records_mover.records.targets.table.TableRecordsTarget')
    def test_table(self, mock_TableRecordsTarget):
        mock_schema_name = Mock(name='schema_name')
        mock_table_name = Mock(name='table_name')
        mock_db_engine = Mock(name='db_engine')
        existing_table_handling = ExistingTableHandling.DELETE_AND_OVERWRITE

        table = self.records_targets.\
            table(schema_name=mock_schema_name,
                  table_name=mock_table_name,
                  db_engine=mock_db_engine)
        mock_TableRecordsTarget.\
            assert_called_with(schema_name=mock_schema_name,
                               table_name=mock_table_name,
                               db_engine=mock_db_engine,
                               db_driver=self.mock_db_driver,
                               drop_and_recreate_on_load_error=False,
                               existing_table_handling=existing_table_handling,
                               add_group_perms_for=None,
                               add_user_perms_for=None,
                               db_conn=None)
        self.assertEqual(table, mock_TableRecordsTarget.return_value)

    @patch('records_mover.records.targets.google_sheets.GoogleSheetsRecordsTarget')
    def test_google_sheet(self, mock_GoogleSheetsRecordsTarget):
        mock_spreadsheet_id = Mock(name='spreadsheet_id')
        mock_sheet_name = Mock(name='sheet_name')
        mock_google_cloud_creds = Mock(name='google_cloud_creds')

        google_sheets_target = self.records_targets.\
            google_sheet(spreadsheet_id=mock_spreadsheet_id,
                         sheet_name=mock_sheet_name,
                         google_cloud_creds=mock_google_cloud_creds)
        mock_GoogleSheetsRecordsTarget.\
            assert_called_with(spreadsheet_id=mock_spreadsheet_id,
                               sheet_name=mock_sheet_name,
                               google_cloud_creds=mock_google_cloud_creds)
        self.assertEqual(google_sheets_target, mock_GoogleSheetsRecordsTarget.return_value)

    @patch('records_mover.records.targets.factory.FileobjTarget')
    def test_fileobj(self, mock_FileobjTarget):
        mock_output_fileobj = Mock(name='output_fileobj')
        mock_records_format = Mock(name='records_format')

        fileobj_target = self.records_targets.\
            fileobj(output_fileobj=mock_output_fileobj,
                    records_format=mock_records_format)
        self.assertEqual(fileobj_target, mock_FileobjTarget.return_value)
        mock_FileobjTarget.\
            assert_called_with(fileobj=mock_output_fileobj,
                               records_format=mock_records_format)

    @patch('records_mover.records.targets.factory.DataUrlTarget')
    def test_data_url(self, mock_DataUrlTarget):
        mock_output_url = Mock(name='output_url')
        mock_records_format = Mock(name='records_format')
        mock_output_loc = self.mock_url_resolver.file_url.return_value
        data_url_target = self.records_targets.\
            data_url(output_url=mock_output_url,
                     records_format=mock_records_format)
        self.assertEqual(data_url_target, mock_DataUrlTarget.return_value)
        mock_DataUrlTarget.\
            assert_called_with(output_loc=mock_output_loc,
                               records_format=mock_records_format)
        self.mock_url_resolver.file_url.assert_called_with(mock_output_url)
