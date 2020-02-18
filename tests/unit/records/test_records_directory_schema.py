import unittest
from records_mover.records.records_directory import RecordsDirectory
from mock import Mock, patch


class TestRecordsDirectorySchema(unittest.TestCase):
    def setUp(self):
        with patch('records_mover.records.records_directory.RecordsFormatFile') as\
             mock_RecordsFormatFile, \
             patch('records_mover.records.records_directory.RecordsSchemaSqlFile') as\
             mock_RecordsSchemaSqlFile, \
             patch('records_mover.records.records_directory.RecordsSchemaJsonFile') as\
             mock_RecordsSchemaJsonFile:
            self.mock_record_format_file = mock_RecordsFormatFile.return_value
            self.mock_schema_sql_file = mock_RecordsSchemaSqlFile.return_value
            self.mock_schema_json_file = mock_RecordsSchemaJsonFile.return_value
            self.mock_records_loc = Mock(name='records_loc')
            self.records_directory = RecordsDirectory(records_loc=self.mock_records_loc)
            mock_RecordsFormatFile.assert_called_with(self.mock_records_loc)
            mock_RecordsSchemaSqlFile.assert_called_with(self.mock_records_loc)
            mock_RecordsSchemaJsonFile.assert_called_with(self.mock_records_loc)

    def test_save_schema(self):
        mock_schema = Mock(name='schema')
        self.records_directory.save_schema(mock_schema)
        self.mock_schema_json_file.save_schema_json.\
            assert_called_with(mock_schema.to_json.return_value)

    def test_load_schema_sql_from_sql_file(self):
        out = self.records_directory.load_schema_sql_from_sql_file()
        self.mock_schema_sql_file.load_schema_sql.assert_called_with()
        self.assertEqual(out, self.mock_schema_sql_file.load_schema_sql.return_value)

    def test_save_format(self):
        mock_records_format = Mock(name='records_format')
        self.records_directory.save_format(mock_records_format)
        self.mock_record_format_file.save_format.assert_called_with(mock_records_format)

    def test_load_format(self):
        mock_fail_if_dont_understand = Mock(name='fail_if_dont_understand')
        out = self.records_directory.\
            load_format(fail_if_dont_understand=mock_fail_if_dont_understand)
        self.mock_record_format_file.load_format.assert_called_with(mock_fail_if_dont_understand)
        self.assertEqual(out, self.mock_record_format_file.load_format.return_value)

    def test_save_data_from_fileobjs(self):
        mock_fileobj = Mock(name='fileobj')
        fileobjs_by_target_names = {
            'name.csv': mock_fileobj
        }
        expected_return_value = {
            self.mock_records_loc.file_in_this_directory.return_value.url:
            {
                "content_length":
                (self.mock_records_loc.file_in_this_directory.
                 return_value.upload_fileobj.return_value)
            }
        }
        self.assertEqual(expected_return_value,
                         self.records_directory.save_data_from_fileobjs(fileobjs_by_target_names))
        mock_target_loc = self.mock_records_loc.file_in_this_directory.return_value
        self.mock_records_loc.file_in_this_directory.assert_called_with('name.csv')
        mock_target_loc.upload_fileobj.assert_called_with(mock_fileobj)
