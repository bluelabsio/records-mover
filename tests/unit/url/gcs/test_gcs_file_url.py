from records_mover.url.gcs.gcs_file_url import GCSFileUrl
from mock import patch, Mock
import unittest


class TestGCSFileURL(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock(name='client')
        self.mock_gcp_credentials = Mock(name='gcp_credentials')
        self.mock_url = 'gs://bucket/dir/file.csv'
        self.loc = GCSFileUrl(url=self.mock_url,
                              gcs_client=self.mock_client,
                              gcp_credentials=self.mock_gcp_credentials)

    def test_init(self):
        self.assertIsNotNone(self.loc)
