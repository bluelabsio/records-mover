from records_mover.records.sources.data_url import DataUrlRecordsSource
from mock import Mock, MagicMock, patch
import unittest


class TestDataUrlRecordsSource(unittest.TestCase):
    @patch('records_mover.records.sources.data_url.FileobjsSource')
    def test_to_delimited_fileobjs_source(self,
                                          mock_FileobjsSource):
        mock_input_url = 'foo://bar/baz/bazzle'
        mock_url_resolver = MagicMock(name='url_resolver')
        mock_records_format = Mock(name='records_format')
        mock_records_schema = Mock(name='records_schema')
        mock_initial_hints = {
            'compression': 'GZIP'
        }
        source = DataUrlRecordsSource(mock_input_url, mock_url_resolver, mock_records_format,
                                      mock_records_schema, mock_initial_hints)
        mock_records_format_if_possible = Mock(name='records_format_if_possible')
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_fileobjs_source =\
            mock_FileobjsSource.infer_if_needed.return_value.__enter__.return_value
        with source.to_fileobjs_source(mock_processing_instructions,
                                       mock_records_format_if_possible) as fileobjs_source:

            self.assertEqual(fileobjs_source, mock_fileobjs_source)
