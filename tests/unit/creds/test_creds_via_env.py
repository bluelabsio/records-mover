import unittest
from unittest.mock import patch
from records_mover.creds.creds_via_env import CredsViaEnv


class TestCredsViaEnv(unittest.TestCase):
    @patch('records_mover.creds.creds_via_env.db')
    def test_db_facts(self, mock_db):
        creds_via_env = CredsViaEnv(default_db_creds_name=None,
                                    default_aws_creds_name=None,
                                    default_gcp_creds_name=None)
        out = creds_via_env.db_facts('foo-bar-baz')
        mock_db.assert_called_with(['foo', 'bar', 'baz'])
        self.assertEqual(out, mock_db.return_value)

    @patch('boto3.session')
    def test_boto3_session(self, mock_boto3_session):
        creds_via_env = CredsViaEnv(default_db_creds_name=None,
                                    default_aws_creds_name=None,
                                    default_gcp_creds_name=None)
        with self.assertRaises(NotImplementedError):
            creds_via_env.boto3_session('anything')
