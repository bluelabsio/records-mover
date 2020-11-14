from mock import patch
from records_mover.mover_types import PleaseInfer
import unittest


@patch('records_mover.session.CredsViaLastPass')
@patch('records_mover.session.CredsViaAirflow')
@patch('records_mover.session.CredsViaEnv')
@patch('records_mover.session.get_config')
class TestSessionChoices(unittest.TestCase):
    def mock_session(self, **kwargs):
        from records_mover import session

        return session.Session(scratch_s3_url='s3://foo/',
                               scratch_gcs_url='gs://bar/',
                               **kwargs)

    @patch.dict('os.environ', {
        'AIRFLOW__CORE__EXECUTOR': 'whoop',
    })
    def test_select_airflow_session_by_implicit_env_variable(self,
                                                             mock_get_config,
                                                             mock_CredsViaEnv,
                                                             mock_CredsViaAirflow,
                                                             mock_CredsViaLastPass):
        session = self.mock_session()
        self.assertEqual(session.creds, mock_CredsViaAirflow.return_value)
        mock_CredsViaAirflow.assert_called_with(default_db_creds_name=None,
                                                default_aws_creds_name='aws_default',
                                                default_gcp_creds_name='google_cloud_default',
                                                default_db_facts=PleaseInfer.token,
                                                default_boto3_session=PleaseInfer.token,
                                                default_gcp_creds=PleaseInfer.token,
                                                default_gcs_client=PleaseInfer.token,
                                                scratch_s3_url='s3://foo/',
                                                scratch_gcs_url='gs://bar/')

    def test_select_cli_session_by_default(self,
                                           mock_get_config,
                                           mock_CredsViaEnv,
                                           mock_CredsViaAirflow,
                                           mock_CredsViaLastPass):
        session = self.mock_session()
        self.assertEqual(session.creds, mock_CredsViaEnv.return_value)
        mock_CredsViaEnv.assert_called_with(default_db_creds_name=None,
                                            default_aws_creds_name=None,
                                            default_gcp_creds_name=None,
                                            default_db_facts=PleaseInfer.token,
                                            default_boto3_session=PleaseInfer.token,
                                            default_gcp_creds=PleaseInfer.token,
                                            default_gcs_client=PleaseInfer.token,
                                            scratch_s3_url='s3://foo/',
                                            scratch_gcs_url='gs://bar/')

    @patch.dict('os.environ', {
        'RECORDS_MOVER_SESSION_TYPE': 'lpass',
    })
    def test_select_lpass_session_by_explicit_env_variable(self,
                                                           mock_get_config,
                                                           mock_CredsViaEnv,
                                                           mock_CredsViaAirflow,
                                                           mock_CredsViaLastPass):
        session = self.mock_session()
        self.assertEqual(session.creds, mock_CredsViaLastPass.return_value)
        mock_CredsViaLastPass.assert_called_with(default_db_creds_name=None,
                                                 default_aws_creds_name=None,
                                                 default_gcp_creds_name=None,
                                                 default_db_facts=PleaseInfer.token,
                                                 default_boto3_session=PleaseInfer.token,
                                                 default_gcp_creds=PleaseInfer.token,
                                                 default_gcs_client=PleaseInfer.token,
                                                 scratch_s3_url='s3://foo/',
                                                 scratch_gcs_url='gs://bar/')

    @patch.dict('os.environ', {
        'RECORDS_MOVER_SESSION_TYPE': 'airflow',
    })
    def test_select_airflow_session_by_explicit_env_variable(self,
                                                             mock_get_config,
                                                             mock_CredsViaEnv,
                                                             mock_CredsViaAirflow,
                                                             mock_CredsViaLastPass):
        session = self.mock_session()
        self.assertEqual(session.creds, mock_CredsViaAirflow.return_value)
        mock_CredsViaAirflow.assert_called_with(default_db_creds_name=None,
                                                default_aws_creds_name='aws_default',
                                                default_gcp_creds_name='google_cloud_default',
                                                default_db_facts=PleaseInfer.token,
                                                default_boto3_session=PleaseInfer.token,
                                                default_gcp_creds=PleaseInfer.token,
                                                default_gcs_client=PleaseInfer.token,
                                                scratch_s3_url='s3://foo/',
                                                scratch_gcs_url='gs://bar/')

    @patch.dict('os.environ', {
        'RECORDS_MOVER_SESSION_TYPE': 'bogus',
    })
    def test_select_invalid_session_by_explicit_env_variable(self,
                                                             mock_get_config,
                                                             mock_CredsViaEnv,
                                                             mock_CredsViaAirflow,
                                                             mock_CredsViaLastPass):
        with self.assertRaises(ValueError) as r:
            session = self.mock_session()
            print(f"Got session: {session}")
        self.assertEqual(str(r.exception),
                         'Valid session types: cli, lpass, airflow, itest, env - '
                         "consider upgrading records-mover if you're looking for bogus.")

    def test_select_airflow_session_by_parameter(self,
                                                 mock_get_config,
                                                 mock_CredsViaEnv,
                                                 mock_CredsViaAirflow,
                                                 mock_CredsViaLastPass):
        session = self.mock_session(session_type='airflow')
        self.assertEqual(session.creds, mock_CredsViaAirflow.return_value)
        mock_CredsViaAirflow.assert_called_with(default_db_creds_name=None,
                                                default_aws_creds_name='aws_default',
                                                default_gcp_creds_name='google_cloud_default',
                                                default_db_facts=PleaseInfer.token,
                                                default_boto3_session=PleaseInfer.token,
                                                default_gcp_creds=PleaseInfer.token,
                                                default_gcs_client=PleaseInfer.token,
                                                scratch_s3_url='s3://foo/',
                                                scratch_gcs_url='gs://bar/')

    def test_select_cli_session_by_parameter(self,
                                             mock_get_config,
                                             mock_CredsViaEnv,
                                             mock_CredsViaAirflow,
                                             mock_CredsViaLastPass):
        session = self.mock_session(session_type='cli')
        self.assertEqual(session.creds, mock_CredsViaEnv.return_value)
        mock_CredsViaEnv.assert_called_with(default_db_creds_name=None,
                                            default_aws_creds_name=None,
                                            default_gcp_creds_name=None,
                                            default_db_facts=PleaseInfer.token,
                                            default_boto3_session=PleaseInfer.token,
                                            default_gcp_creds=PleaseInfer.token,
                                            default_gcs_client=PleaseInfer.token)

    def test_select_lastpass_session_by_config(self,
                                               mock_get_config,
                                               mock_CredsViaEnv,
                                               mock_CredsViaAirflow,
                                               mock_CredsViaLastPass):
        mock_get_config.return_value.config = {
            'session': {
                'session_type': 'lpass'
            }
        }
        session = self.mock_session()
        self.assertEqual(session.creds, mock_CredsViaLastPass.return_value)
        mock_CredsViaLastPass.assert_called_with(default_db_creds_name=None,
                                                 default_aws_creds_name=None,
                                                 default_gcp_creds_name=None,
                                                 default_db_facts=PleaseInfer.token,
                                                 default_boto3_session=PleaseInfer.token,
                                                 default_gcp_creds=PleaseInfer.token,
                                                 default_gcs_client=PleaseInfer.token,
                                                 scratch_s3_url='s3://foo/',
                                                 scratch_gcs_url='gs://bar/')

    def test_select_invalid_session_by_parameter(self,
                                                 mock_get_config,
                                                 mock_CredsViaEnv,
                                                 mock_CredsViaAirflow,
                                                 mock_CredsViaLastPass):
        with self.assertRaises(ValueError) as r:
            self.mock_session(session_type='bogus')
        self.assertEqual(str(r.exception),
                         "Valid session types: cli, lpass, airflow, itest, env - "
                         "consider upgrading records-mover if you're looking for bogus.")
