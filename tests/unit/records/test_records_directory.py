import unittest
from records_mover.records.records_directory import RecordsDirectory
from records_mover.url.base import BaseDirectoryUrl
from mock import Mock, patch, call
import json


class TestRecordsDirectory(unittest.TestCase):
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

    def test_save_preliminary_manifest_with_no_urls(self):
        mock_manifest_loc = Mock()
        self.mock_records_loc.file_in_this_directory.return_value = mock_manifest_loc
        mock_file_1 = Mock(url='s3://bucket/key1.csv')
        mock_file_1.filename.return_value = 'key1.csv'
        mock_file_1.size.return_value = 123
        mock_file_2 = Mock(url='s3://bucket/key2.csv')
        mock_file_2.size.return_value = 456
        mock_file_2.filename.return_value = 'key2.csv'
        self.mock_records_loc.files_in_directory.return_value = [
            mock_file_1,
            mock_file_2,
        ]
        self.records_directory.save_preliminary_manifest()
        mock_manifest_loc.store_string.assert_called_once_with(json.dumps({
            "entries": [
                {
                    "url": "s3://bucket/key1.csv",
                    "mandatory": True,
                    'meta': {
                        'content_length': 123
                    }
                },
                {
                    "url": "s3://bucket/key2.csv",
                    "mandatory": True,
                    'meta': {
                        'content_length': 456
                    }
                }
            ]
        }))

    def test_save_preliminary_manifest_with_urls(self):
        mock_manifest_loc = Mock()
        self.mock_records_loc.file_in_this_directory.return_value = mock_manifest_loc
        url_details = {
            's3://bucket/asdf.csv': {
                'content_length': 123
            },
            's3://bucket/bsdf.csv': {
                'content_length': 456
            },
        }
        self.records_directory.save_preliminary_manifest(url_details)
        mock_manifest_loc.store_string.assert_called_once_with(json.dumps({
            "entries": [
                {
                    "url": "s3://bucket/asdf.csv", "mandatory": True,
                    "meta": {
                        "content_length": 123
                    }
                },
                {
                    "url": "s3://bucket/bsdf.csv",
                    "mandatory": True,
                    "meta": {
                        "content_length": 456
                    }
                }
            ]
        }))

    def test_save_preliminary_manifest(self):
        mock_manifest_loc = self.mock_records_loc.file_in_this_directory.return_value
        mock_file_loc = Mock(name='file_loc')
        mock_file_loc.filename.return_value = 'foo.csv'
        mock_file_loc.url = 'my://url/foo.csv'
        mock_file_loc.size.return_value = 123
        self.mock_records_loc.files_in_directory.return_value = [mock_file_loc]
        self.records_directory.save_preliminary_manifest()
        expected_manifest =\
            '{"entries": [{"url": "my://url/foo.csv", "mandatory": true, '\
            '"meta": {"content_length": 123}}]}'
        mock_manifest_loc.store_string.\
            assert_called_with(expected_manifest)
        self.mock_records_loc.file_in_this_directory.assert_called_with('manifest')

    @patch('records_mover.records.records_directory.RecordsDirectory')
    @patch('records_mover.records.records_directory.json')
    def test_copy_to(self, mock_json, mock_RecordsDirectory):
        mock_new_loc = Mock(name='new_loc')
        self.mock_records_loc.copy_to.return_value = mock_new_loc
        mock_new_directory = mock_RecordsDirectory.return_value
        mock_manifest_loc = self.mock_records_loc.file_in_this_directory.return_value
        mock_manifest = {
            'entries': [
                {'url': 'vince://dir1/file1'},
                {'url': 'vince://dir1/file2'},
            ]
        }
        mock_manifest_loc.json_contents.return_value = mock_manifest
        mock_url_details = {
            mock_new_loc.file_in_this_directory.return_value.url: {
                "content_length": mock_new_loc.file_in_this_directory.return_value.size.return_value
            },
        }
        mock_old_finalized_manifest = self.mock_records_loc.file_in_this_directory.return_value
        mock_old_finalized_manifest.exists.return_value = True

        out = self.records_directory.copy_to(mock_new_loc)
        mock_new_loc.file_in_this_directory.\
            assert_has_calls([call('file1'),
                              call().size(),
                              call('file2'),
                              call().size()])
        mock_RecordsDirectory.assert_called_with(records_loc=mock_new_loc)
        mock_new_directory.save_preliminary_manifest.assert_called_with(mock_url_details)
        mock_new_directory.finalize_manifest.assert_called_with()

        self.mock_records_loc.copy_to.assert_called_with(mock_new_loc)
        self.assertEqual(out, mock_new_directory)

    def test_save_fileobjs(self):
        mock_fileobj = Mock(name='fileobj')
        fileobjs_by_target_names = {
            'name.csv': mock_fileobj
        }
        expected_return_value = {
            's3://bucket/asdf.csv': {'content_length': 123}
        }
        mock_records_format = Mock(name='records_format')
        mock_records_schema = Mock(name='records_schema')
        mock_records_schema_json = mock_records_schema.to_json.return_value
        mock_manifest_loc = Mock()
        self.mock_records_loc.file_in_this_directory.return_value = mock_manifest_loc
        self.mock_records_loc.file_in_this_directory.return_value.url = 's3://bucket/asdf.csv'
        self.mock_records_loc.file_in_this_directory.return_value.upload_fileobj.return_value = 123
        out = self.records_directory.\
            save_fileobjs(fileobjs_by_target_names=fileobjs_by_target_names,
                          records_schema=mock_records_schema,
                          records_format=mock_records_format)
        mock_target_loc = self.mock_records_loc.file_in_this_directory.return_value
        self.mock_schema_json_file.save_schema_json.assert_called_with(mock_records_schema_json)
        mock_manifest_loc.store_string.assert_called_once_with(json.dumps({
            "entries": [
                {"url": "s3://bucket/asdf.csv", "mandatory": True,
                 "meta": {"content_length": 123}},
            ]
        }))
        mock_target_loc.upload_fileobj.assert_called_with(mock_fileobj)

        self.assertEqual(out, expected_return_value)
        self.mock_record_format_file.save_format.assert_called_with(mock_records_format)

    def test_load_schema_json(self):
        out = self.records_directory.load_schema_json()
        self.assertEqual(out, self.mock_schema_json_file.load_schema_json.return_value)

    @patch('records_mover.records.records_directory.RecordsSchema')
    def test_load_schema_json_obj_None(self, mock_RecordsSchema):
        self.mock_schema_json_file.load_schema_json.return_value = None
        out = self.records_directory.load_schema_json_obj()
        self.assertEqual(out, None)

    @patch('records_mover.records.records_directory.RecordsSchema')
    def test_load_schema_json_obj(self, mock_RecordsSchema):
        out = self.records_directory.load_schema_json_obj()
        mock_file = self.mock_schema_json_file.load_schema_json.return_value
        self.assertEqual(out, mock_RecordsSchema.from_json.return_value)
        mock_RecordsSchema.from_json.assert_called_with(mock_file)

    @patch('records_mover.records.records_directory.RecordsDirectory.copy_to')
    def test_copy_from(self, mock_copy_to):
        mock_old_loc = Mock(name='old_loc', spec=BaseDirectoryUrl)
        mock_old_loc.scheme = Mock(name='scheme', spec=str)
        out = self.records_directory.copy_from(mock_old_loc)
        self.assertEqual(out, mock_copy_to.return_value)
        mock_copy_to.assert_called_with(self.mock_records_loc)

    def test_get_manifest_not_renamed(self):
        underscore_manifest_loc = Mock(name='underscore_manifest_loc')
        manifest_loc = Mock(name='manifest_loc')
        self.mock_records_loc.file_in_this_directory.side_effect = [
            underscore_manifest_loc,
            manifest_loc
        ]
        underscore_manifest_loc.exists.return_value = False
        out = self.records_directory.get_manifest()
        self.assertEqual(out, manifest_loc.json_contents.return_value)

    def test_manifest_entry_urls_manifest_empty(self):
        mock_manifest_loc = Mock(name='manifest_loc')
        self.mock_records_loc.file_in_this_directory.return_value = mock_manifest_loc
        mock_manifest_loc.exists.return_value = True
        mock_manifest_loc.json_contents.return_value = None
        with self.assertRaises(TypeError):
            self.records_directory.manifest_entry_urls()

    def test_save_to_url_one_file(self):
        mock_records_format = Mock(name='records_format')
        self.mock_record_format_file.load_format.return_value = mock_records_format
        mock_manifest_loc = Mock(name='manifest_loc')
        mock_manifest_loc.exists.return_value = True
        mock_csv_loc = Mock(name='csv_loc')
        self.mock_records_loc.file_in_this_directory.side_effect = [
            mock_manifest_loc,
            mock_csv_loc
        ]
        mock_manifest_loc.exists.return_value = True
        mock_manifest_loc.json_contents.return_value = {
            "entries": [
                {"url": "s3://bucket/path/file.csv"}
            ]
        }
        mock_output_loc = Mock(name='output_loc')
        self.records_directory.save_to_url(mock_output_loc)
        mock_csv_loc.copy_to.assert_called_with(mock_output_loc)

    def test_str(self):
        self.assertEqual(str(self.records_directory),
                         f"RecordsDirectory({self.mock_records_loc.url})")
