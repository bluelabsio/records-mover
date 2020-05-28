from records_mover import Session
import unittest
from mock import patch


@patch('records_mover.session.os')
@patch.dict('os.environ', {
    'AWS_SECRET_ACCESS_KEY': 'aws secret key',
    'AWS_SESSION_TOKEN': 'aws session token',
    'AWS_ACCESS_KEY_ID': 'aws access key',
})
class TestCLISession(unittest.TestCase):
    @patch('records_mover.session.CredsViaLastPass')
    def test_creds(self,
                   mock_CredsViaLastPass,
                   mock_os):
        session = Session(session_type='cli',
                          default_db_creds_name=None,
                          default_aws_creds_name=None)
        self.assertEqual(mock_CredsViaLastPass.return_value, session.creds)

    @patch('records_mover.session.CredsViaLastPass')
    @patch('records_mover.db.connect.engine_from_db_facts')
    def test_get_default_db_engine_from_name(self,
                                             mock_engine_from_db_facts,
                                             mock_CredsViaLastPass,
                                             mock_os):
        session = Session(session_type='cli',
                          default_db_creds_name='foo',
                          default_aws_creds_name=None)
        mock_creds = mock_CredsViaLastPass.return_value
        out = session.get_default_db_engine()
        self.assertEqual(out, mock_engine_from_db_facts.return_value)
        mock_engine_from_db_facts.assert_called_with(mock_creds.default_db_facts.return_value)
