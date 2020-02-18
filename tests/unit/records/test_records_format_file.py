from records_mover.records.records_format_file import RecordsFormatFile
import unittest
from mock import Mock, patch


class TestRecordsFormatFile(unittest.TestCase):
    def setUp(self):
        self.mock_records_loc = Mock(name='records_loc')
        self.records_format_file = RecordsFormatFile(records_loc=self.mock_records_loc)

    @patch('records_mover.records.records_format_file.ProcessingInstructions')
    @patch('records_mover.records.records_format_file.DelimitedRecordsFormat')
    def test_load_format(self, mock_DelimitedRecordsFormat,
                         mock_ProcessingInstructions):
        mock_fail_if_dont_understand = Mock(name='fail_if_dont_understand')
        mock_format_loc = Mock(name='mock_format_loc')
        self.mock_records_loc.files_matching_prefix.return_value = [mock_format_loc]
        mock_format_loc.filename.return_value = '_format_delimited'
        mock_variant = 'csv'
        mock_format_loc.json_contents.return_value = {
            'variant': mock_variant,
            'hints': {},
        }
        out = self.records_format_file.\
            load_format(fail_if_dont_understand=mock_fail_if_dont_understand)
        mock_DelimitedRecordsFormat.\
            assert_called_with(variant=mock_variant,
                               hints={},
                               processing_instructions=mock_ProcessingInstructions.return_value)
        mock_ProcessingInstructions.\
            assert_called_with(fail_if_dont_understand=mock_fail_if_dont_understand)
        self.assertEqual(mock_DelimitedRecordsFormat.return_value, out)

    def test_save_format(self):
        mock_records_format = Mock(name='records_format')
        mock_file_loc = self.mock_records_loc.file_in_this_directory.return_value
        mock_contents = mock_records_format.json.return_value
        self.records_format_file.save_format(mock_records_format)
        mock_file_loc.store_string.assert_called_with(mock_contents)
