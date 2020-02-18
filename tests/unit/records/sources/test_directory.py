from records_mover.records.sources.directory import RecordsDirectoryRecordsSource
from records_mover.records.records_format import DelimitedRecordsFormat
from mock import Mock, MagicMock, patch, ANY
import unittest


class TestRecordsDirectoryRecordsSource(unittest.TestCase):
    def setUp(self):
        self.mock_directory = Mock(name='directory')
        self.mock_records_format = Mock(name='records_format', spec=DelimitedRecordsFormat)
        self.mock_directory.load_format.return_value = self.mock_records_format
        mock_url_resolver = MagicMock(name='url_resolver')
        self.mock_override_hints = Mock(name='overrride_hints')
        self.source =\
            RecordsDirectoryRecordsSource(directory=self.mock_directory,
                                          fail_if_dont_understand=True,
                                          url_resolver=mock_url_resolver,
                                          override_hints=self.mock_override_hints)

    def test_init(self):
        self.mock_records_format.alter_hints.assert_called_with(self.mock_override_hints)
        self.assertEqual(self.source.records_format,
                         self.mock_records_format.alter_hints.return_value)
        self.assertIsNotNone(self.source)

    def test_records_directory(self):
        self.assertEqual(self.source.records_directory(),
                         self.mock_directory)

    @patch('records_mover.records.sources.fileobjs.FileobjsSource.infer_if_needed')
    def test_to_fileobjs_source(self, mock_infer_if_needed):
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_url = MagicMock(name='url')
        mock_all_urls = [mock_url]
        self.mock_directory.manifest_entry_urls.return_value = mock_all_urls
        with self.source.\
                to_fileobjs_source(processing_instructions=mock_processing_instructions) as f:
            self.assertEqual(mock_infer_if_needed.return_value.__enter__.return_value, f)
            mock_records_schema = self.mock_directory.load_schema_json_obj.return_value
            mock_infer_if_needed.\
                assert_called_with(ANY,
                                   initial_hints=None,
                                   processing_instructions=mock_processing_instructions,
                                   records_format=self.mock_records_format.alter_hints.return_value,
                                   records_schema=mock_records_schema)
