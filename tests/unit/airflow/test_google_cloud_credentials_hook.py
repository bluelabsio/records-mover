from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from records_mover.airflow.hooks.google_cloud_credentials_hook import GoogleCloudCredentialsHook
from mock import Mock
import unittest


class TestGoogleCloudCredentialsHook(unittest.TestCase):
    def test_get_conn(self):
        mock_init = Mock('__init__')
        GoogleCloudBaseHook.__init__ = mock_init
        mock_init.return_value = None
        hook = GoogleCloudCredentialsHook()
        mock_get_credentials = Mock('get_credentials')
        hook._get_credentials = mock_get_credentials
        conn = hook.get_conn()
        self.assertEqual(conn, mock_get_credentials.return_value)
