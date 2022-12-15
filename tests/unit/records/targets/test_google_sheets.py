import unittest
from records_mover.records.targets.google_sheets import GoogleSheetsRecordsTarget
from records_mover.records.results import MoveResult
from googleapiclient.errors import HttpError
from mock import patch, Mock, MagicMock, ANY
import numpy as np


class TestGoogleSheets(unittest.TestCase):
    def setUp(self):
        self.mock_spreadsheet_id = Mock(name='spreadsheet_id')
        self.mock_sheet_name = Mock(name='sheet_name')
        self.mock_google_cloud_creds = MagicMock(name='google_cloud_creds')
        self.google_sheets = GoogleSheetsRecordsTarget(self.mock_spreadsheet_id,
                                                       self.mock_sheet_name,
                                                       self.mock_google_cloud_creds)

    @patch('records_mover.records.targets.google_sheets.build')
    @patch('records_mover.records.targets.google_sheets.httplib2')
    @patch('records_mover.records.targets.google_sheets.google_auth_httplib2')
    @patch('records_mover.records.targets.google_sheets.np')
    def test_move_from_dataframe_sheet_exists(self,
                                              mock_np,
                                              mock_google_auth_httplib2,
                                              mock_httplib2,
                                              mock_build):

        mock_df = Mock(name='df')
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_df.to_records.return_value = [np.array([1])]
        mock_np.generic = np.generic
        mock_dfs_source = Mock(name='dfs_source')
        mock_dfs_source.dfs = [mock_df]
        out = self.google_sheets.move_from_dataframes_source(mock_dfs_source,
                                                             mock_processing_instructions)
        mock_df.to_records.assert_called_with(index=mock_dfs_source.include_index)
        mock_json_encodable_datum = 1
        mock_http = mock_httplib2.Http.return_value
        mock_authed_http = mock_google_auth_httplib2.AuthorizedHttp.return_value
        mock_google_auth_httplib2.AuthorizedHttp.\
            assert_called_with(self.mock_google_cloud_creds, http=mock_http)
        mock_build.assert_called_with('sheets', 'v4', http=mock_authed_http)
        mock_service = mock_build.return_value
        mock_clear = mock_service.spreadsheets.return_value.values.return_value.clear
        mock_clear.assert_called_with(spreadsheetId=self.mock_spreadsheet_id,
                                      range=self.mock_sheet_name,
                                      body={})
        mock_clear.return_value.execute.assert_called()
        mock_body = {
            'values': [[mock_json_encodable_datum]]
        }
        mock_update = mock_service.spreadsheets.return_value.values.return_value.update
        mock_update.assert_called_with(spreadsheetId=self.mock_spreadsheet_id,
                                       range=self.mock_sheet_name,
                                       valueInputOption='RAW',
                                       body=mock_body)
        mock_update.return_value.execute.assert_called
        self.assertEqual(MoveResult(move_count=1, output_urls=None), out)

    @patch('records_mover.records.targets.google_sheets.build')
    @patch('records_mover.records.targets.google_sheets.httplib2')
    @patch('records_mover.records.targets.google_sheets.google_auth_httplib2')
    @patch('records_mover.records.targets.google_sheets.np')
    def test_move_from_dataframe_sheet_new(self,
                                           mock_np,
                                           mock_google_auth_httplib2,
                                           mock_httplib2,
                                           mock_build):

        mock_df = Mock(name='df')
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_df.to_records.return_value = [np.array([1])]
        mock_np.generic = np.generic
        mock_service = mock_build.return_value
        mock_clear = mock_service.spreadsheets.return_value.values.return_value.clear
        mock_resp = Mock(name='resp')
        mock_clear.return_value.execute.side_effect = HttpError(mock_resp, b'content')
        mock_batch_update = mock_service.spreadsheets.return_value.batchUpdate
        mock_dfs_source = Mock(name='dfs_source')
        mock_dfs_source.dfs = [mock_df]

        out = self.google_sheets.move_from_dataframes_source(mock_dfs_source,
                                                             mock_processing_instructions)
        mock_df.to_records.assert_called_with(index=mock_dfs_source.include_index)
        mock_json_encodable_datum = 1
        mock_http = mock_httplib2.Http.return_value
        mock_authed_http = mock_google_auth_httplib2.AuthorizedHttp.return_value
        mock_google_auth_httplib2.AuthorizedHttp.\
            assert_called_with(self.mock_google_cloud_creds, http=mock_http)
        mock_build.assert_called_with('sheets', 'v4', http=mock_authed_http)
        mock_clear.assert_called_with(spreadsheetId=self.mock_spreadsheet_id,
                                      range=self.mock_sheet_name,
                                      body={})
        mock_batch_update.assert_called_with(spreadsheetId=self.mock_spreadsheet_id,
                                             body=ANY)
        mock_batch_update.return_value.execute.assert_called()
        mock_clear.return_value.execute.assert_called()
        mock_body = {
            'values': [[mock_json_encodable_datum]]
        }
        mock_update = mock_service.spreadsheets.return_value.values.return_value.update
        mock_update.assert_called_with(spreadsheetId=self.mock_spreadsheet_id,
                                       range=self.mock_sheet_name,
                                       valueInputOption='RAW',
                                       body=mock_body)
        mock_update.return_value.execute.assert_called
        self.assertEqual(MoveResult(move_count=1, output_urls=None), out)
