from records_mover import Session
import unittest
from mock import patch, Mock, ANY


@patch('records_mover.session.os')
@patch('records_mover.session.subprocess')
@patch.dict('os.environ', {
    'AWS_SECRET_ACCESS_KEY': 'aws secret key',
    'AWS_SESSION_TOKEN': 'aws session token',
    'AWS_ACCESS_KEY_ID': 'aws access key',
})
class TestCLISession(unittest.TestCase):
    @patch('records_mover.db.factory.db_driver')
    @patch('records_mover.session.UrlResolver')
    @patch('boto3.session')
    @patch('google.auth.default')
    @patch('google.cloud.storage.Client')
    @patch.dict('os.environ', {}, clear=True)
    def test_db_driver_with_guessed_bucket_url(self,
                                               mock_storage_Client,
                                               mock_google_auth_default,
                                               mock_boto3_session,
                                               mock_UrlResolver,
                                               mock_db_driver,
                                               mock_subprocess,
                                               mock_os):
        mock_subprocess.check_output.return_value = b"s3://chrisp-scratch/"
        session = Session(session_type='cli',
                          default_db_creds_name=None,
                          default_aws_creds_name=None)
        mock_subprocess.check_output.return_value = b"s3://chrisp-scratch/"
        mock_db = Mock(name='db')
        mock_gcp_credentials = Mock(name='gcp_credentials')
        mock_gcp_project = Mock(name='gcp_project')
        mock_google_auth_default.return_value = (mock_gcp_credentials, mock_gcp_project)

        driver = session.db_driver(mock_db)
        mock_url_resolver = mock_UrlResolver.return_value
        mock_url_resolver.directory_url.assert_called_with('s3://chrisp-scratch/')

        mock_boto3_session.Session.assert_not_called()
        mock_UrlResolver.assert_called_with(boto3_session_getter=ANY,
                                            gcp_credentials_getter=ANY,
                                            gcs_client_getter=ANY)
        mock_directory_url = mock_UrlResolver.return_value.directory_url
        mock_db_driver.assert_called_with(db=mock_db,
                                          url_resolver=session.url_resolver,
                                          s3_temp_base_loc=mock_directory_url.return_value)
        mock_subprocess.check_output.assert_called_with('scratch-s3-url')
        self.assertEqual(mock_db_driver.return_value, driver)

    @patch('records_mover.session.CredsViaLastPass')
    def test_creds(self,
                   mock_CredsViaLastPass,
                   mock_subprocess,
                   mock_os):
        session = Session(session_type='lpass',
                          default_db_creds_name=None,
                          default_aws_creds_name=None)
        self.assertEqual(mock_CredsViaLastPass.return_value, session.creds)

    @patch('records_mover.session.CredsViaLastPass')
    @patch('records_mover.db.connect.engine_from_db_facts')
    def test_get_default_db_engine_from_name(self,
                                             mock_engine_from_db_facts,
                                             mock_CredsViaLastPass,
                                             mock_subprocess,
                                             mock_os):
        session = Session(session_type='lpass',
                          default_db_creds_name='foo',
                          default_aws_creds_name=None)
        mock_creds = mock_CredsViaLastPass.return_value
        out = session.get_default_db_engine()
        self.assertEqual(out, mock_engine_from_db_facts.return_value)
        mock_engine_from_db_facts.assert_called_with(mock_creds.default_db_facts.return_value)
