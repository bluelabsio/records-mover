from records_mover.records.sources.google_sheets import GoogleSheetsRecordsSource
from mock import Mock, patch, ANY
import unittest


class TestGoogleSheetsRecordsSource(unittest.TestCase):
    @patch('records_mover.records.sources.google_sheets.build')
    @patch('records_mover.records.sources.google_sheets.google_auth_httplib2')
    @patch('records_mover.records.sources.google_sheets.httplib2')
    @patch('records_mover.records.sources.google_sheets.DataframesRecordsSource')
    @patch('pandas.DataFrame')
    def test_dataframes_source_headers_from_sheet(self,
                                                  mock_DataFrame,
                                                  mock_DataframesRecordsSource,
                                                  mock_httplib2,
                                                  mock_google_auth_httplib2,
                                                  mock_build):
        mock_spreadsheet_id = Mock(name='spreadsheet_id')
        mock_sheet_name_or_range = Mock(name='sheet_name_or_range')
        mock_google_cloud_creds = Mock(name='google_cloud_creds')
        source = GoogleSheetsRecordsSource(mock_spreadsheet_id,
                                           mock_sheet_name_or_range,
                                           mock_google_cloud_creds)
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_values = [
            ['a', 1, 2],
            ['b', 2, 'abc']
        ]
        mock_service = mock_build.return_value
        mock_spreadsheets_fn = mock_service.spreadsheets
        mock_values_fn = mock_spreadsheets_fn.return_value.values
        mock_get_fn = mock_values_fn.return_value.get
        mock_execute_fn = mock_get_fn.return_value.execute
        mock_result = mock_execute_fn.return_value
        mock_result.get.return_value = mock_values
        with source.to_dataframes_source(mock_processing_instructions) as source:
            mock_http = mock_httplib2.Http.return_value
            mock_httplib2.Http.assert_called_with()
            mock_http_authorized = mock_google_auth_httplib2.AuthorizedHttp.return_value
            mock_google_auth_httplib2.AuthorizedHttp.assert_called_with(mock_google_cloud_creds,
                                                                        http=mock_http)
            mock_build.assert_called_with('sheets', 'v4', http=mock_http_authorized)
            mock_spreadsheets_fn.assert_called_with()
            mock_values_fn.assert_called_with()
            mock_get_fn.assert_called_with(spreadsheetId=mock_spreadsheet_id,
                                           range=mock_sheet_name_or_range,
                                           majorDimension="COLUMNS",
                                           valueRenderOption="UNFORMATTED_VALUE",
                                           dateTimeRenderOption='FORMATTED_STRING')
            mock_execute_fn.assert_called_with()
            mock_result.get.assert_called_with('values', [])
            mock_DataFrame.assert_called_with([(1, 2), (2, 'abc')], columns=['a', 'b'])
            mock_DataframesRecordsSource.\
                assert_called_with(dfs=ANY,
                                   processing_instructions=mock_processing_instructions)
            expected_out = mock_DataframesRecordsSource.return_value
            self.assertEqual(source, expected_out)

    @patch('records_mover.records.sources.google_sheets.build')
    @patch('records_mover.records.sources.google_sheets.google_auth_httplib2')
    @patch('records_mover.records.sources.google_sheets.httplib2')
    @patch('records_mover.records.sources.google_sheets.DataframesRecordsSource')
    @patch('pandas.DataFrame')
    def test_dataframe_duplicate_sheet_headers(self,
                                               mock_DataFrame,
                                               mock_DataframesRecordsSource,
                                               mock_httplib2,
                                               mock_google_auth_httplib2,
                                               mock_build):
        mock_spreadsheet_id = Mock(name='spreadsheet_id')
        mock_sheet_name_or_range = Mock(name='sheet_name_or_range')
        mock_google_cloud_creds = Mock(name='google_cloud_creds')
        source = GoogleSheetsRecordsSource(mock_spreadsheet_id,
                                           mock_sheet_name_or_range,
                                           mock_google_cloud_creds)
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_values = [
            ['a', 1, 2],
            ['a', 2, 'abc'],
            ['b', 3, 'zxy'],
        ]
        mock_service = mock_build.return_value
        mock_spreadsheets_fn = mock_service.spreadsheets
        mock_values_fn = mock_spreadsheets_fn.return_value.values
        mock_get_fn = mock_values_fn.return_value.get
        mock_execute_fn = mock_get_fn.return_value.execute
        mock_result = mock_execute_fn.return_value
        mock_result.get.return_value = mock_values
        with self.assertRaises(TypeError) as e:
            with source.to_dataframes_source(mock_processing_instructions) as source:
                pass
        self.assertEqual(str(e.exception),
                         "The following column names are duplicated - "
                         "please make them unique: ['a']")

    @patch('records_mover.records.sources.google_sheets.build')
    @patch('records_mover.records.sources.google_sheets.google_auth_httplib2')
    @patch('records_mover.records.sources.google_sheets.httplib2')
    @patch('records_mover.records.sources.google_sheets.DataframesRecordsSource')
    @patch('pandas.DataFrame')
    def test_dataframe_out_of_band_headers(self,
                                           mock_DataFrame,
                                           mock_DataframesRecordsSource,
                                           mock_httplib2,
                                           mock_google_auth_httplib2,
                                           mock_build):
        mock_spreadsheet_id = Mock(name='spreadsheet_id')
        mock_sheet_name_or_range = Mock(name='sheet_name_or_range')
        mock_google_cloud_creds = Mock(name='google_cloud_creds')
        source = GoogleSheetsRecordsSource(mock_spreadsheet_id,
                                           mock_sheet_name_or_range,
                                           mock_google_cloud_creds,
                                           out_of_band_column_headers=['c', 'd'])
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_values = [
            ['a', 1, 2],
            ['b', 2, 'abc']
        ]
        mock_service = mock_build.return_value
        mock_spreadsheets_fn = mock_service.spreadsheets
        mock_values_fn = mock_spreadsheets_fn.return_value.values
        mock_get_fn = mock_values_fn.return_value.get
        mock_execute_fn = mock_get_fn.return_value.execute
        mock_result = mock_execute_fn.return_value
        mock_result.get.return_value = mock_values
        with source.to_dataframes_source(mock_processing_instructions) as source:
            mock_http = mock_httplib2.Http.return_value
            mock_httplib2.Http.assert_called_with()
            mock_http_authorized = mock_google_auth_httplib2.AuthorizedHttp.return_value
            mock_google_auth_httplib2.AuthorizedHttp.assert_called_with(mock_google_cloud_creds,
                                                                        http=mock_http)
            mock_build.assert_called_with('sheets', 'v4', http=mock_http_authorized)
            mock_spreadsheets_fn.assert_called_with()
            mock_values_fn.assert_called_with()
            mock_get_fn.assert_called_with(spreadsheetId=mock_spreadsheet_id,
                                           range=mock_sheet_name_or_range,
                                           majorDimension="COLUMNS",
                                           valueRenderOption="UNFORMATTED_VALUE",
                                           dateTimeRenderOption='FORMATTED_STRING')
            mock_execute_fn.assert_called_with()
            mock_result.get.assert_called_with('values', [])
            mock_DataFrame.assert_called_with([('a', 'b'), (1, 2), (2, 'abc')],
                                              columns=['c', 'd'])
            mock_DataframesRecordsSource.\
                assert_called_with(dfs=ANY,
                                   processing_instructions=mock_processing_instructions)
            expected_out = mock_DataframesRecordsSource.return_value
            self.assertEqual(source, expected_out)
