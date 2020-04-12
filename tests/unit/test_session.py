from mock import patch, Mock
from records_mover import Session
import unittest


@patch('records_mover.session.subprocess')
@patch('records_mover.session.os')
class TestSession(unittest.TestCase):
    @patch('records_mover.session.CredsViaEnv')
    def test_itest_type_uses_creds_via_env(self,
                                           mock_CredsViaEnv,
                                           mock_os,
                                           mock_subprocess):
        mock_creds = mock_CredsViaEnv.return_value
        session = Session(session_type='itest')
        self.assertEqual(session.creds, mock_creds)

    @patch('records_mover.session.CredsViaEnv')
    def test_env_type_uses_creds_via_env(self,
                                         mock_CredsViaEnv,
                                         mock_os,
                                         mock_subprocess):
        mock_creds = mock_CredsViaEnv.return_value
        session = Session(session_type='env')
        self.assertEqual(session.creds, mock_creds)
