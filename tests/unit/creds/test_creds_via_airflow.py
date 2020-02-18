import unittest
from unittest.mock import patch, Mock
from records_mover.creds.creds_via_airflow import CredsViaAirflow


class TestCredsViaAirflow(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.creds_via_airflow = CredsViaAirflow()

    @patch('airflow.contrib.hooks.aws_hook.AwsHook')
    def test_boto3_session(self, mock_AwsHook):
        mock_aws_creds_name = Mock(name='aws_creds_name')
        out = self.creds_via_airflow.boto3_session(mock_aws_creds_name)
        mock_AwsHook.assert_called_with(mock_aws_creds_name)
        self.assertEqual(mock_AwsHook.return_value.get_session.return_value, out)

    @patch('airflow.hooks.BaseHook')
    def test_db_facts(self, mock_BaseHook):
        mock_db_creds_name = Mock(name='db_creds_name')
        out = self.creds_via_airflow.db_facts(mock_db_creds_name)
        mock_BaseHook.get_connection.assert_called_with(mock_db_creds_name)
        mock_conn = mock_BaseHook.get_connection.return_value
        expected_db_facts = {
            'host': mock_conn.host,
            'port': mock_conn.port,
            'database': mock_conn.schema,
            'password': mock_conn.password,
            'type': mock_conn.extra_dejson.get.return_value,
            'user': mock_conn.login
        }
        mock_conn.extra_dejson.get.assert_called_with('type',
                                                      mock_conn.conn_type.lower.return_value)
        self.assertEqual(expected_db_facts, out)
