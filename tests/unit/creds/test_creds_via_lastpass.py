import unittest
from unittest.mock import patch, Mock
from records_mover.creds.creds_via_lastpass import CredsViaLastPass


class TestCredsViaLPass(unittest.TestCase):
    @patch('google.oauth2.service_account.Credentials')
    @patch('records_mover.creds.creds_via_lastpass.lpass_field')
    def test_google_sheets(self, mock_lpass_field, mock_Credentials):
        creds_via_last_pass = CredsViaLastPass()
        mock_gcp_creds_name = Mock(name='gcp_creds_name')
        mock_lpass_field.return_value = '{"a": 1}'
        mock_cred_details = {"a": 1}
        out = creds_via_last_pass.google_sheets(mock_gcp_creds_name)
        mock_lpass_field.assert_called_with(mock_gcp_creds_name, 'notes')
        mock_Credentials.from_service_account_info.\
            assert_called_with(mock_cred_details,
                               scopes=('https://www.googleapis.com/auth/spreadsheets',))
        self.assertEqual(out, mock_Credentials.from_service_account_info.return_value)

    @patch('records_mover.creds.creds_via_lastpass.db')
    def test_db_facts(self, mock_db):
        creds_via_lastpass = CredsViaLastPass()
        out = creds_via_lastpass.db_facts('foo-bar-baz')
        mock_db.assert_called_with(['foo', 'bar', 'baz'])
        self.assertEqual(out, mock_db.return_value)

    @patch('records_mover.creds.creds_via_lastpass.boto3')
    def test_boto3_session(self, mock_boto3):
        creds_via_lastpass = CredsViaLastPass()
        out = creds_via_lastpass.boto3_session(None)
        self.assertEqual(out, mock_boto3.session.Session.return_value)
