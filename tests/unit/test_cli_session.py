from records_mover import Session
import unittest
from mock import patch, Mock


@patch('records_mover.session.os')
@patch('records_mover.session.subprocess')
@patch.dict('os.environ', {
    'AWS_SECRET_ACCESS_KEY': 'aws secret key',
    'AWS_SESSION_TOKEN': 'aws session token',
    'AWS_ACCESS_KEY_ID': 'aws access key',
})
class TestCLISession(unittest.TestCase):
    @patch('records_mover.session.CredsViaLastPass')
    def test_creds(self,
                   mock_CredsViaLastPass,
                   mock_subprocess,
                   mock_os):
        context = Session(session_type='cli',
                          default_db_creds_name=None,
                          default_aws_creds_name=None)
        self.assertEqual(mock_CredsViaLastPass.return_value, context.creds)
