import unittest
from unittest.mock import patch, Mock
from records_mover.creds.creds_via_env import CredsViaEnv


class TestCredsViaEnv(unittest.TestCase):
    @patch.dict('os.environ', {
        'GCP_SERVICE_ACCOUNT_JSON_BASE64': 'eyJhIjogInNla3JpdCJ9Cg==',
    })
    @patch('google.oauth2.service_account.Credentials')
    def test_google_sheets(self, mock_Credentials):
        creds_via_env = CredsViaEnv()
        mock_gcp_creds_name = Mock(name='gcp_creds_name')
        mock_cred_details = {"a": 'sekrit'}  # unbase64ed unjsoned version of the above
        out = creds_via_env.google_sheets(mock_gcp_creds_name)
        mock_Credentials.from_service_account_info.\
            assert_called_with(mock_cred_details,
                               scopes=('https://www.googleapis.com/auth/spreadsheets',))
        self.assertEqual(out, mock_Credentials.from_service_account_info.return_value)

    @patch('records_mover.creds.creds_via_env.db')
    def test_db_facts(self, mock_db):
        creds_via_env = CredsViaEnv()
        out = creds_via_env.db_facts('foo-bar-baz')
        mock_db.assert_called_with(['foo', 'bar', 'baz'])
        self.assertEqual(out, mock_db.return_value)

    @patch('boto3.session')
    def test_boto3_session(self, mock_boto3_session):
        creds_via_env = CredsViaEnv()
        out = creds_via_env.boto3_session(None)
        self.assertEqual(out, mock_boto3_session.Session.return_value)
