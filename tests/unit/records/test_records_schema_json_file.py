import unittest
from mock import Mock
from records_mover.records.records_schema_json_file import RecordsSchemaJsonFile


class TestRecordsSchemaJsonFile(unittest.TestCase):
    def test_save_schema_json(self):
        mock_records_loc = Mock(name='records_loc')
        records_schema_json_file = RecordsSchemaJsonFile(mock_records_loc)
        mock_schema_loc = mock_records_loc.file_in_this_directory.return_value
        mock_json = Mock(name='json')
        records_schema_json_file.save_schema_json(mock_json)
        mock_schema_loc.store_string.assert_called_with(mock_json)
        mock_records_loc.file_in_this_directory.assert_called_with('_schema.json')

    def test_load_schema_json(self):
        mock_records_loc = Mock(name='records_loc')
        records_schema_json_file = RecordsSchemaJsonFile(mock_records_loc)
        mock_schema_loc = mock_records_loc.file_in_this_directory.return_value
        out = records_schema_json_file.load_schema_json()
        self.assertEqual(out, mock_schema_loc.string_contents.return_value)
        mock_records_loc.file_in_this_directory.assert_called_with('_schema.json')
