from records_mover import Session
import unittest
from mock import patch, ANY, Mock


@patch.dict('os.environ', {
    'AWS_SECRET_ACCESS_KEY': 'aws secret key',
    'AWS_SESSION_TOKEN': 'aws session token',
    'AWS_ACCESS_KEY_ID': 'aws access key',
})
class TestCLISessionRecords(unittest.TestCase):
    @patch('google.cloud.storage.Client')
    @patch('google.auth.default')
    @patch('records_mover.session.Records')
    def test_records(self,
                     mock_Records,
                     mock_google_auth_default,
                     mock_google_cloud_storage_Client):
        mock_credentials = Mock(name='credentials')
        mock_project = Mock(name='project')
        mock_google_auth_default.return_value = (mock_credentials, mock_project)
        session = Session(session_type='cli',
                          default_db_creds_name=None,
                          default_aws_creds_name=None,
                          default_gcp_creds_name=None)
        self.assertEqual(mock_Records.return_value,
                         session.records)
        mock_Records.assert_called_with(db_driver=ANY,
                                        url_resolver=ANY)

    @patch('records_mover.session.Records')
    @patch.dict('os.environ', {'SCRATCH_S3_URL': 's3://different-scratch-bucket/'})
    def test_records_with_overridden_scratch_bucket(self,
                                                    mock_Records):
        session = Session(session_type='cli',
                          default_db_creds_name=None,
                          default_aws_creds_name=None)
        self.assertEqual(session.creds.default_scratch_s3_url(), 's3://different-scratch-bucket/')
