from records_mover.url.gcs.gcs_directory_url import GCSDirectoryUrl
from mock import patch, Mock
import unittest


class TestGCSDirectoryURL(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock(name='client')
        self.mock_gcp_credentials = Mock(name='gcp_credentials')
        self.mock_url = 'gs://bucket/dir/'
        self.loc = GCSDirectoryUrl(url=self.mock_url,
                                   gcs_client=self.mock_client,
                                   gcp_credentials=self.mock_gcp_credentials)

    def test_init(self):
        self.assertIsNotNone(self.loc)

    def test_directory_in_this_directory(self):
        out = self.loc.directory_in_this_directory('newdir')
        self.assertEqual(out.url, 'gs://bucket/dir/newdir/')

    @patch('records_mover.url.gcs.gcs_file_url.GCSFileUrl')
    def test_files_in_directory(self,
                                mock_GCSFileUrl):
        mock_blob_1 = Mock(name='blob_1')
        mock_blob_2 = Mock(name='blob_1')
        self.mock_client.list_blobs.return_value = [mock_blob_1, mock_blob_2]

        out = self.loc.files_in_directory()

        self.mock_client.list_blobs.assert_called_with(bucket_or_name='bucket',
                                                       prefix='dir/',
                                                       delimiter='/')
        self.assertEqual(out,
                         [mock_GCSFileUrl.return_value, mock_GCSFileUrl.return_value])

    @patch('records_mover.url.gcs.gcs_directory_url.googleapiclient')
    def test_directories_in_directory(self,
                                      mock_googleapiclient):
        mock_service = mock_googleapiclient.discovery.build.return_value
        mock_folders_req = mock_service.objects.return_value.list.return_value
        mock_prefix = 'dir/'
        mock_folders_resp = mock_folders_req.execute.return_value
        mock_folders_resp.get.return_value = ['dir/bing/', 'dir/bazzle/']
        out = self.loc.directories_in_directory()
        mock_googleapiclient.discovery.build.\
            assert_called_with('storage', 'v1',
                               credentials=self.mock_gcp_credentials)
        mock_service.objects.return_value.list.assert_called_with(bucket='bucket',
                                                                  prefix=mock_prefix,
                                                                  delimiter='/')
        mock_folders_req.execute.assert_called_with()
        mock_folders_resp.get.assert_called_with('prefixes', [])
        self.assertEqual(list(map(lambda loc: loc.url, out)),
                         ['gs://bucket/dir/bing/',
                          'gs://bucket/dir/bazzle/'])
