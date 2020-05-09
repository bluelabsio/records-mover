from records_mover.records.sources.factory import RecordsSources
from records_mover.records.sources.data_url import DataUrlRecordsSource
from mock import MagicMock, Mock, patch, ANY
import unittest


class TestFactory(unittest.TestCase):
    def setUp(self):
        self.mock_db_driver = Mock(name='db_driver')
        self.mock_url_resolver = MagicMock(name='url_resolver')
        self.records_sources = RecordsSources(self.mock_db_driver,
                                              url_resolver=self.mock_url_resolver)

    @patch('records_mover.records.sources.dataframes.DataframesRecordsSource')
    def test_dataframe(self, mock_DataframesRecordsSource):
        mock_df = Mock(name='df')
        mock_processing_instructions = Mock(name='processing_instructions')
        df = self.records_sources.\
            dataframe(df=mock_df,
                      processing_instructions=mock_processing_instructions)
        mock_DataframesRecordsSource.\
            assert_called_with(dfs=ANY,
                               include_index=False,
                               records_schema=None,
                               processing_instructions=mock_processing_instructions)
        self.assertEqual(df, mock_DataframesRecordsSource.return_value)

    @patch('records_mover.records.sources.factory.UninferredFileobjsRecordsSource')
    def test_fileobjs(self, mock_UninferredFileobjsSource):
        mock_target_names_to_fileobjs = Mock(name='target_names_to_fileobjs')
        mock_records_schema = Mock(name='records_schema')
        self.records_sources.\
            fileobjs(target_names_to_input_fileobjs=mock_target_names_to_fileobjs,
                     records_format=None,
                     records_schema=mock_records_schema)
        mock_UninferredFileobjsSource.\
            assert_called_with(initial_hints=None,
                               records_format=None,
                               records_schema=mock_records_schema,
                               target_names_to_input_fileobjs=mock_target_names_to_fileobjs)

    @patch('records_mover.records.sources.factory.FileobjsSource')
    def test_data_url(self,
                      mock_FileobjsSource):
        mock_input_url = 'foo://host/path'
        mock_records_format = Mock(name='records_format')
        out = self.records_sources.data_url(input_url=mock_input_url,
                                            records_format=mock_records_format)
        self.assertEqual(type(out),
                         DataUrlRecordsSource)

    @patch('records_mover.records.sources.table.TableRecordsSource')
    def test_table(self,
                   mock_TableRecordsSource):
        mock_schema_name = Mock(name='schema_name')
        mock_table_name = Mock(name='table_name')
        mock_db_engine = Mock(name='db_engine')
        out = self.records_sources.table(schema_name=mock_schema_name,
                                         table_name=mock_table_name,
                                         db_engine=mock_db_engine)
        mock_TableRecordsSource.assert_called_with(schema_name=mock_schema_name,
                                                   table_name=mock_table_name,
                                                   driver=self.mock_db_driver.return_value,
                                                   url_resolver=self.mock_url_resolver)
        self.assertEqual(out, mock_TableRecordsSource.return_value)

    @patch('records_mover.records.sources.google_sheets.GoogleSheetsRecordsSource')
    def test_google_sheet(self, mock_GoogleSheetsRecordsSource):
        mock_spreadsheet_id = Mock(name='spreadsheet_id')
        mock_sheet_name_or_range = Mock(name='sheet_name_or_range')
        mock_google_cloud_creds = Mock(name='google_cloud_creds')
        out = self.records_sources.\
            google_sheet(spreadsheet_id=mock_spreadsheet_id,
                         sheet_name_or_range=mock_sheet_name_or_range,
                         google_cloud_creds=mock_google_cloud_creds)
        mock_GoogleSheetsRecordsSource.\
            assert_called_with(spreadsheet_id=mock_spreadsheet_id,
                               sheet_name_or_range=mock_sheet_name_or_range,
                               google_cloud_creds=mock_google_cloud_creds,
                               header_translator=None,
                               out_of_band_column_headers=None)
        self.assertEqual(out, mock_GoogleSheetsRecordsSource.return_value)

    @patch('records_mover.records.hints.compression.os')
    @patch('records_mover.records.hints.compression.urlparse')
    @patch('records_mover.records.sources.factory.pathlib')
    def test_local_file(self,
                        mock_pathlib,
                        mock_urlparse,
                        mock_os):
        mock_filename = Mock(name='filename')
        mock_records_format = Mock(name='record_format')
        out = self.records_sources.local_file(filename=mock_filename,
                                              records_format=mock_records_format)
        self.assertEqual(type(out), DataUrlRecordsSource)
