from records_mover.url.gcs.gcs_file_url import GCSFileUrl
from mock import patch, Mock
import unittest
import google.api_core.exceptions


class TestGCSFileURL(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock(name='client')
        self.mock_gcp_credentials = Mock(name='gcp_credentials')
        self.mock_url = 'gs://bucket/dir/file.csv'
        self.mock_bucket_obj = self.mock_client.bucket.return_value
        self.mock_blob_obj = self.mock_bucket_obj.blob.return_value
        self.loc = GCSFileUrl(url=self.mock_url,
                              gcs_client=self.mock_client,
                              gcp_credentials=self.mock_gcp_credentials)

    def test_init(self):
        self.assertIsNotNone(self.loc)

    @patch('records_mover.url.gcs.gcs_file_url.gs_open')
    def test_open(self, mock_gs_open):
        mock_mode = Mock(name='mode')
        out = self.loc.open(mock_mode)
        mock_gs_open.assert_called_with(bucket_id='bucket',
                                        blob_id='dir/file.csv',
                                        mode=mock_mode,
                                        client=self.mock_client)
        self.assertEqual(out, mock_gs_open.return_value)

    @patch('records_mover.url.gcs.gcs_file_url.gs_open')
    def test_open_not_found_redirects_smartopen_exception(self, mock_gs_open):
        mock_mode = Mock(name='mode')
        mock_gs_open.side_effect = google.api_core.exceptions.NotFound('foo')
        with self.assertRaises(FileNotFoundError):
            self.loc.open(mock_mode)

    def test_size(self):
        out = self.loc.size()
        self.assertEqual(out, self.mock_blob_obj.size)

    def test_delete(self):
        self.loc.delete()
        self.mock_blob_obj.delete.assert_called_with()

    def test_filename(self):
        out = self.loc.filename()
        self.assertEqual(out, 'file.csv')
