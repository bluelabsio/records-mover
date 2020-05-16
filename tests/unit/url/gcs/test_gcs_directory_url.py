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
