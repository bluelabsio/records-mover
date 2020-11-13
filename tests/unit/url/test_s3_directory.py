from records_mover.url.s3.s3_directory_url import S3DirectoryUrl
from records_mover.url.filesystem import FilesystemDirectoryUrl
from mock import patch, Mock, call
import unittest


class TestS3DirectoryUrl(unittest.TestCase):
    def setUp(self):
        self.mock_S3Url = Mock(name='S3Url')

        self.mock_boto3_session = Mock(name='boto3_session')
        self.s3_directory_url = S3DirectoryUrl('s3://bucket/topdir/bottomdir/',
                                               S3Url=self.mock_S3Url,
                                               boto3_session=self.mock_boto3_session)

    @patch('records_mover.url.optimizer.awscli.aws_cli')
    def test_copy_to_dir(self,
                         mock_aws_cli):
        file_loc = Mock(name='file_loc', spec=FilesystemDirectoryUrl)
        file_loc.local_file_path = '/my/dir/'
        file_loc.scheme = 'file'
        self.s3_directory_url.copy_to(file_loc)
        mock_aws_cli.assert_called_with('s3', 'sync', 's3://bucket/topdir/bottomdir/',
                                        '/my/dir/')

    def test_files_in_directory(self):
        mock_s3_client = self.mock_boto3_session.client.return_value
        mock_s3_client.list_objects.return_value = {
            'Contents': []
        }
        out = self.s3_directory_url.files_in_directory()
        mock_s3_client.list_objects.assert_called_with(Bucket='bucket',
                                                       Delimiter='/',
                                                       Prefix='topdir/bottomdir/')
        self.assertEqual([], out)

    def test_files_matching_prefix_none(self):
        mock_s3_client = self.mock_boto3_session.client.return_value
        mock_s3_client.list_objects.return_value = {
        }
        out = self.s3_directory_url.files_matching_prefix('format_')
        mock_s3_client.list_objects.assert_called_with(Bucket='bucket',
                                                       Prefix='topdir/bottomdir/format_',
                                                       Delimiter='/')
        self.assertEqual([], out)

    def test_temporary_directory_cleans_up_upon_exception(self):
        mock_s3_url = Mock(name='s3_url', spec=S3DirectoryUrl)
        self.mock_S3Url.return_value = mock_s3_url
        try:
            with self.s3_directory_url.temporary_directory():
                self.mock_S3Url.assert_called()
                raise NotImplementedError  # arbitrary exception to raise
        except NotImplementedError:
            mock_s3_url.purge_directory.assert_called()
            return
        self.assertFalse()

    def test_temporary_directory_cleans_up_upon_success(self):
        mock_s3_url = Mock(name='s3_url', spec=S3DirectoryUrl)
        self.mock_S3Url.return_value = mock_s3_url
        with self.s3_directory_url.temporary_directory():
            self.mock_S3Url.assert_called()
        mock_s3_url.purge_directory.assert_called()

    def test_directories_in_directory(self):
        mock_s3_url = Mock(name='s3_url', spec=S3DirectoryUrl)
        self.mock_S3Url.return_value = mock_s3_url
        mock_s3_client = self.mock_boto3_session.client.return_value
        mock_response = {
            'CommonPrefixes': [
                {
                    'Prefix': 'prefix1/',
                },
                {
                    'Prefix': 'prefix2/',
                }
            ]
        }
        mock_s3_client.list_objects_v2.return_value = mock_response
        out = self.s3_directory_url.directories_in_directory()
        mock_s3_client.list_objects_v2.assert_called_with(Bucket='bucket',
                                                          Prefix='topdir/bottomdir/',
                                                          Delimiter='/')
        self.mock_S3Url.assert_has_calls([call('s3://bucket/topdir/bottomdir/prefix1/',
                                               self.mock_boto3_session),
                                          call('s3://bucket/topdir/bottomdir/prefix2/',
                                               self.mock_boto3_session)])
        self.assertEqual(out, [mock_s3_url, mock_s3_url])
