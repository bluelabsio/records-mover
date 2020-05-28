import unittest
from unittest.mock import patch, Mock
from records_mover.creds.creds_via_airflow import CredsViaAirflow


class TestCredsViaAirflow(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.creds_via_airflow = CredsViaAirflow(default_db_creds_name=None,
                                                 default_aws_creds_name=None,
                                                 default_gcp_creds_name=None)

    @patch('airflow.contrib.hooks.aws_hook.AwsHook')
    def test_boto3_session(self, mock_AwsHook):
        mock_aws_creds_name = Mock(name='aws_creds_name')
        out = self.creds_via_airflow.boto3_session(mock_aws_creds_name)
        mock_AwsHook.assert_called_with(mock_aws_creds_name)
        self.assertEqual(mock_AwsHook.return_value.get_session.return_value, out)

    @patch('airflow.hooks.BaseHook')
    def test_db_facts_normcoredb(self, mock_BaseHook):
        mock_db_creds_name = Mock(name='db_creds_name')
        mock_conn = mock_BaseHook.get_connection.return_value
        mock_conn.extra_dejson = {
            'type': 'jsontype'
        }
        out = self.creds_via_airflow.db_facts(mock_db_creds_name)
        mock_BaseHook.get_connection.assert_called_with(mock_db_creds_name)
        expected_db_facts = {
            'host': mock_conn.host,
            'port': mock_conn.port,
            'database': mock_conn.schema,
            'password': mock_conn.password,
            'type': 'jsontype',
            'user': mock_conn.login
        }
        self.assertEqual(expected_db_facts, out)

    @patch('airflow.hooks.BaseHook')
    def test_db_facts_bigquery_serviceaccount(self, mock_BaseHook):
        mock_db_creds_name = Mock(name='db_creds_name')
        mock_conn = mock_BaseHook.get_connection.return_value
        mock_conn.type = 'google_cloud_platform'
        mock_conn.schema = None
        mock_conn.host = None
        mock_conn.port = None
        mock_conn.password = None
        mock_conn.login = None
        mock_conn.extra_dejson = {
            'type': 'bigquery',
            'bq_default_dataset_id': 'mydataset',
            'protocol': 'bigquery',
            'extra__google_cloud_platform__keyfile_dict': 'big json block',
            'extra__google_cloud_platform__project': 'myproject',
        }
        out = self.creds_via_airflow.db_facts(mock_db_creds_name)
        mock_BaseHook.get_connection.assert_called_with(mock_db_creds_name)
        expected_db_facts = {
            'bq_default_dataset_id': 'mydataset',
            'bq_default_project_id': 'myproject',
            'bq_service_account_json': 'big json block',
            'protocol': 'bigquery',
            'type': 'bigquery',
        }
        self.assertEqual(expected_db_facts, out)
