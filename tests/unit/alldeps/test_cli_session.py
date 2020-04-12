from records_mover import Session
import unittest
from mock import patch


@patch('records_mover.session.os')
@patch('records_mover.session.subprocess')
@patch.dict('os.environ', {
    'AWS_SECRET_ACCESS_KEY': 'aws secret key',
    'AWS_SESSION_TOKEN': 'aws session token',
    'AWS_ACCESS_KEY_ID': 'aws access key',
})
class TestCLISession(unittest.TestCase):
    @patch('records_mover.session.db_driver')
    @patch('records_mover.session.UrlResolver')
    @patch('records_mover.session.boto3')
    @patch.dict('os.environ', {}, clear=True)
    def test_db_driver_with_guessed_bucket_url(self,
                                               mock_boto3,
                                               mock_UrlResolver,
                                               mock_db_driver,
                                               mock_subprocess,
                                               mock_os):
        mock_subprocess.check_output.return_value = b"s3://chrisp-scratch/"
        context = Session(session_type='cli',
                          default_db_creds_name=None,
                          default_aws_creds_name=None)
        mock_subprocess.check_output.return_value = b"s3://chrisp-scratch/"
        mock_db = Mock(name='db')

        driver = context.db_driver(mock_db)
        mock_url_resolver = mock_UrlResolver.return_value
        mock_url_resolver.directory_url.assert_called_with('s3://chrisp-scratch/')

        mock_session = mock_boto3.session.Session.return_value
        mock_boto3.session.Session.assert_called_with()
        mock_UrlResolver.assert_called_with(boto3_session=mock_session)
        mock_directory_url = mock_UrlResolver.return_value.directory_url
        mock_db_driver.assert_called_with(db=mock_db,
                                          url_resolver=context.url_resolver,
                                          s3_temp_base_loc=mock_directory_url.return_value)
        mock_subprocess.check_output.assert_called_with('scratch-s3-url')
        self.assertEqual(mock_db_driver.return_value, driver)

    @patch('records_mover.session.CredsViaLastPass')
    @patch('records_mover.db.connect.engine_from_db_facts')
    def test_get_default_db_engine_from_name(self,
                                             mock_engine_from_db_facts,
                                             mock_CredsViaLastPass,
                                             mock_subprocess,
                                             mock_os):
        context = Session(session_type='cli',
                          default_db_creds_name='foo',
                          default_aws_creds_name=None)
        mock_creds = mock_CredsViaLastPass.return_value
        out = context.get_default_db_engine()
        self.assertEqual(out, mock_engine_from_db_facts.return_value)
        mock_engine_from_db_facts.assert_called_with(mock_creds.db_facts.return_value)
        mock_creds.db_facts.assert_called_with('foo')
