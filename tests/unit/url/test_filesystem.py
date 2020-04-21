from records_mover.url.filesystem import FilesystemDirectoryUrl, FilesystemFileUrl
from records_mover.url.s3.s3_file_url import S3FileUrl
from records_mover.url.base import BaseFileUrl
from mock import patch, Mock, MagicMock, mock_open
import unittest
import os


class TestFilesystemFileUrl(unittest.TestCase):
    def setUp(self):
        self.mock_boto3_session = Mock(name='boto3_session')
        self.filesystem_file_url = FilesystemFileUrl('file:///topdir/bottomdir/file',
                                                     boto3_session=self.mock_boto3_session)

    def test_invalid_url(self):
        with self.assertRaises(ValueError):
            FilesystemFileUrl('file://bucket/bar')

    @patch("builtins.open", new_callable=mock_open)
    def test_store_string(self,
                          mock_open):
        mock_string = Mock(name='string')
        self.filesystem_file_url.store_string(mock_string)
        file_loc = Mock(name='file_loc')
        file_loc.local_file_path = '/my/file'
        mock_open.assert_called_with('/topdir/bottomdir/file', 'wt')
        mock_open.return_value.write.assert_called_with(mock_string)

    @patch("builtins.open", new_callable=mock_open)
    @patch("records_mover.url.base.blcopyfileobj")
    def test_upload_fileobj(self,
                            mock_copyfileobj,
                            mock_open):
        mock_fileobj = Mock(name='fileobj')
        self.filesystem_file_url.upload_fileobj(mock_fileobj)
        mock_copyfileobj.assert_called_with(mock_fileobj, mock_open.return_value)
        mock_open.assert_called_with('/topdir/bottomdir/file', 'wb')

    @patch("builtins.open", new_callable=mock_open)
    def test_json_contents(self,
                           mock_open):
        mock_f = mock_open.return_value.__enter__.return_value
        mock_f.read.return_value = Mock()
        mock_f.read.return_value.decode.return_value = '{"a":1}'
        out = self.filesystem_file_url.json_contents()
        mock_open.assert_called_with('/topdir/bottomdir/file', 'rb')
        self.assertEqual(out, {'a': 1})

    @patch("builtins.open", new_callable=mock_open)
    def test_json_contents_empty(self,
                                 mock_open):
        mock_f = mock_open.return_value.__enter__.return_value
        mock_f.read.return_value = Mock()
        mock_f.read.return_value.decode.return_value = ''
        out = self.filesystem_file_url.json_contents()
        self.assertEqual(out, None)

    @patch("builtins.open", new_callable=mock_open)
    def test_exists_yes(self, mock_open):
        mock_f = mock_open.return_value.__enter__.return_value
        mock_f.read.return_value = Mock()
        mock_f.read.return_value.decode.return_value = '{"a":1}'
        out = self.filesystem_file_url.exists()
        mock_open.assert_called_with('/topdir/bottomdir/file', 'rb')
        self.assertEqual(out, True)

    @patch("builtins.open", new_callable=mock_open)
    def test_exists_no(self, mock_open):
        mock_open.side_effect = FileNotFoundError
        out = self.filesystem_file_url.exists()
        mock_open.assert_called_with('/topdir/bottomdir/file', 'rb')
        self.assertEqual(out, False)

    def test_str(self):
        self.assertEqual('file:///topdir/bottomdir/file', str(self.filesystem_file_url))

    def test_repr(self):
        self.assertEqual('FilesystemFileUrl(file:///topdir/bottomdir/file)',
                         repr(self.filesystem_file_url))

    def test_wait_to_exist(self):
        self.filesystem_file_url.wait_to_exist()
        # ensure it returns

    def test_is_directory(self):
        self.assertFalse(self.filesystem_file_url.is_directory())

    @patch("builtins.open", new_callable=mock_open)
    @patch("records_mover.url.base.blcopyfileobj")
    def test_concatenate_from(self,
                              mock_blcopyfileobj,
                              mock_open):
        mock_loc_1 = MagicMock(name='loc_1', spec=BaseFileUrl)
        mock_loc_2 = MagicMock(name='loc_2', spec=BaseFileUrl)
        mock_other_locs = [mock_loc_1, mock_loc_2]
        mock_output_fileobj = Mock(name='output_fileobj')
        mock_input_fileobj_1 = Mock(name='input_fileobj_1')
        mock_input_fileobj_2 = Mock(name='input_fileobj_2')
        mock_open.return_value.__enter__.return_value = mock_output_fileobj
        mock_loc_1.open.return_value.__enter__.return_value = mock_input_fileobj_1
        mock_loc_2.open.return_value.__enter__.return_value = mock_input_fileobj_2
        self.filesystem_file_url.concatenate_from(mock_other_locs)
        mock_blcopyfileobj.assert_any_call(mock_input_fileobj_1, mock_output_fileobj)
        mock_blcopyfileobj.assert_any_call(mock_input_fileobj_2, mock_output_fileobj)

    @patch("records_mover.url.filesystem.os")
    def test_rename_to(self, mock_os):
        mock_new_loc = Mock(name='new_loc', spec=FilesystemFileUrl)
        mock_new_loc.filesystem_file_url = 'file:///newtopdir/newbottomdir/newfile'
        mock_new_loc.local_file_path = '/newtopdir/newbottomdir/newfile'
        out = self.filesystem_file_url.rename_to(mock_new_loc)
        mock_os.rename.assert_called_with('/topdir/bottomdir/file',
                                          '/newtopdir/newbottomdir/newfile')
        self.assertEqual(out, mock_new_loc)

    def test_rename_to_bad_target(self):
        mock_new_loc = Mock(name='new_loc', spec=S3FileUrl)
        with self.assertRaises(TypeError):
            self.filesystem_file_url.rename_to(mock_new_loc)

    @patch("records_mover.url.filesystem.os")
    def test_delete(self, mock_os):
        self.filesystem_file_url.delete()
        mock_os.remove.assert_called_with('/topdir/bottomdir/file')

    @patch("records_mover.url.filesystem.os")
    def test_size(self, mock_os):
        out = self.filesystem_file_url.size()
        self.assertEqual(out, mock_os.stat.return_value.st_size)
        mock_os.stat.assert_called_with('/topdir/bottomdir/file')

    @patch("records_mover.url.filesystem.FilesystemDirectoryUrl")
    def test_containing_directory(self,
                                  mock_FilesystemDirectoryUrl):
        out = self.filesystem_file_url.containing_directory()
        self.assertEqual(out, mock_FilesystemDirectoryUrl.return_value)
        mock_FilesystemDirectoryUrl.assert_called_with('file:///topdir/bottomdir')


class TestFilesystemDirectoryUrl(unittest.TestCase):
    def setUp(self):
        self.mock_boto3_session = Mock(name='boto3_session')
        self.filesystem_directory_url =\
            FilesystemDirectoryUrl('file:///topdir/bottomdir/',
                                   boto3_session=self.mock_boto3_session)

    def test_is_directory(self):
        self.assertTrue(self.filesystem_directory_url.is_directory())

    @patch("records_mover.url.filesystem.os")
    @patch("records_mover.url.filesystem.Path")
    def test_file_in_this_directory(self, mock_Path, mock_os):
        mock_joined_path = mock_os.path.join.return_value
        mock_Path.return_value.as_uri.return_value = 'file:///topdir/bottomdir/newfile'
        ret = self.filesystem_directory_url.file_in_this_directory('newfile')
        self.assertEqual('file:///topdir/bottomdir/newfile', ret.url)
        mock_Path.assert_called_with(mock_joined_path)
        mock_os.path.join.assert_called_with('/topdir/bottomdir/', 'newfile')

    @patch("records_mover.url.filesystem.os")
    @patch("records_mover.url.filesystem.Path")
    def test_files_in_directory(self, mock_Path, mock_os):
        mock_file = Mock(name='file')
        mock_os.listdir.return_value = [mock_file]
        mock_os.path.isfile.return_value = True
        mock_Path.return_value.as_uri.return_value = 'file:///topdir/bottomdir/newfile'
        ret = self.filesystem_directory_url.files_in_directory()
        self.assertEqual(1, len(ret))
        self.assertEqual('file:///topdir/bottomdir/newfile', ret[0].url)

    @patch("records_mover.url.filesystem.os")
    @patch("records_mover.url.filesystem.Path")
    def test_files_matching_prefix(self, mock_Path, mock_os):
        mock_file = Mock(name='file')
        mock_os.listdir.return_value = [mock_file]
        mock_os.path.isfile.return_value = True
        mock_Path.return_value.as_uri.return_value = 'file:///topdir/bottomdir/newfile'
        ret = self.filesystem_directory_url.files_matching_prefix('newf')
        self.assertEqual(1, len(ret))
        self.assertEqual('file:///topdir/bottomdir/newfile', ret[0].url)

    @patch("records_mover.url.filesystem.os")
    @patch("records_mover.url.filesystem.FilesystemFileUrl")
    @patch("records_mover.url.filesystem.FilesystemDirectoryUrl")
    def test_copy_to(self, mock_FilesystemDirectoryUrl, mock_FilesystemFileUrl, mock_os):
        mock_file = Mock(name='file')
        mock_FilesystemFileUrl.return_value = mock_file
        mock_file.is_directory.return_value = False
        mock_file.local_file_path = '/topdir/bottomdir/file'

        mock_subdirectory = Mock(name='subdirectory')
        mock_FilesystemDirectoryUrl.return_value = mock_subdirectory
        mock_subdirectory.local_file_path = '/topdir/bottomdir/subdir'
        mock_subdirectory.is_directory.return_value = True

        mock_os.path.join = os.path.join

        def mock_isfile(path):
            if path == '/topdir/bottomdir/file':
                return True
            elif path == '/topdir/bottomdir/subdir':
                return False
            else:
                raise SyntaxError(path)

        def mock_isdir(path):
            if path == '/topdir/bottomdir/subdir':
                return True
            raise SyntaxError(path)

        mock_os.path.isfile = mock_isfile
        mock_os.path.isdir = mock_isdir

        mock_os.listdir.return_value = ['file', 'subdir']
        mock_target_directory = Mock(name='target_directory')
        ret = self.filesystem_directory_url.copy_to(mock_target_directory)
        mock_os.listdir.assert_called_with('/topdir/bottomdir/')
        mock_FilesystemFileUrl.assert_called_with('file:///topdir/bottomdir/file')
        self.assertEqual(ret, mock_target_directory)

    def test_str(self):
        self.assertEqual('file:///topdir/bottomdir/', str(self.filesystem_directory_url))

    def test_repr(self):
        self.assertEqual('FilesystemDirectoryUrl(file:///topdir/bottomdir/)',
                         repr(self.filesystem_directory_url))
