from mock import patch
import unittest

# TODO: Walk through and confirm each of these test cases
@patch('records_mover.job_context.CredsViaLastPass')
@patch('records_mover.job_context.CredsViaAirflow')
@patch('records_mover.job_context.BaseJobContext')
class TestJobContextChoices(unittest.TestCase):
    def mock_job_context(self, **kwargs):
        from records_mover import job_context

        return job_context.get_job_context(scratch_s3_url='s3://foo/', **kwargs)

    @patch.dict('os.environ', {
        'AIRFLOW__CORE__EXECUTOR': 'whoop',
    })
    def test_select_airflow_job_context_by_implicit_env_variable(self,
                                                                 mock_BaseJobContext,
                                                                 mock_CredsViaAirflow,
                                                                 mock_CredsViaLastPass):
        job_context = self.mock_job_context()
        self.assertEqual(job_context, mock_BaseJobContext.return_value)
        mock_BaseJobContext.assert_called_with(creds=mock_CredsViaAirflow.return_value,
                                               default_db_creds_name=None,
                                               default_aws_creds_name='aws_default',
                                               scratch_s3_url='s3://foo/')

    def test_select_cli_job_context_by_default(self,
                                               mock_BaseJobContext,
                                               mock_CredsViaAirflow,
                                               mock_CredsViaLastPass):
        job_context = self.mock_job_context()
        self.assertEqual(job_context, mock_BaseJobContext.return_value)
        mock_BaseJobContext.assert_called_with(creds=mock_CredsViaLastPass.return_value,
                                               default_db_creds_name=None,
                                               default_aws_creds_name=None,
                                               scratch_s3_url='s3://foo/')

    @patch.dict('os.environ', {
        'PY_JOB_CONTEXT': 'cli',
    })
    def test_select_cli_job_context_by_explicit_env_variable(self,
                                                             mock_BaseJobContext,
                                                             mock_CredsViaAirflow,
                                                             mock_CredsViaLastPass):
        job_context = self.mock_job_context()
        self.assertEqual(job_context, mock_BaseJobContext.return_value)
        mock_BaseJobContext.assert_called_with(creds=mock_CredsViaLastPass.return_value,
                                               default_db_creds_name=None,
                                               default_aws_creds_name=None,
                                               scratch_s3_url='s3://foo/')

    @patch.dict('os.environ', {
        'PY_JOB_CONTEXT': 'airflow',
    })
    def test_select_airflow_job_context_by_explicit_env_variable(self,
                                                                 mock_BaseJobContext,
                                                                 mock_CredsViaAirflow,
                                                                 mock_CredsViaLastPass):
        job_context = self.mock_job_context()
        self.assertEqual(job_context, mock_BaseJobContext.return_value)
        mock_BaseJobContext.assert_called_with(creds=mock_CredsViaAirflow.return_value,
                                               default_db_creds_name=None,
                                               default_aws_creds_name='aws_default',
                                               scratch_s3_url='s3://foo/')

    @patch.dict('os.environ', {
        'PY_JOB_CONTEXT': 'bogus',
    })
    def test_select_invalid_job_context_by_explicit_env_variable(self,
                                                                 mock_BaseJobContext,
                                                                 mock_CredsViaAirflow,
                                                                 mock_CredsViaLastPass):
        with self.assertRaises(ValueError) as r:
            jc = self.mock_job_context()
            print(f"Got jc: {jc}")
        self.assertEqual(str(r.exception),
                         'Valid job context types: cli, airflow, docker-itest, env - '
                         "consider upgrading records-mover if you're looking for bogus.")

    def test_select_airflow_job_context_by_parameter(self,
                                                     mock_BaseJobContext,
                                                     mock_CredsViaAirflow,
                                                     mock_CredsViaLastPass):
        job_context = self.mock_job_context(job_context_type='airflow')
        self.assertEqual(job_context, mock_BaseJobContext.return_value)
        mock_BaseJobContext.assert_called_with(creds=mock_CredsViaAirflow.return_value,
                                               default_db_creds_name=None,
                                               default_aws_creds_name='aws_default',
                                               scratch_s3_url='s3://foo/')

    def test_select_cli_job_context_by_parameter(self,
                                                 mock_BaseJobContext,
                                                 mock_CredsViaAirflow,
                                                 mock_CredsViaLastPass):
        job_context = self.mock_job_context(job_context_type='cli')
        self.assertEqual(job_context, mock_BaseJobContext.return_value)
        mock_BaseJobContext.assert_called_with(creds=mock_CredsViaLastPass.return_value,
                                               default_db_creds_name=None,
                                               default_aws_creds_name=None,
                                               scratch_s3_url='s3://foo/')

    def test_select_invalid_job_context_by_parameter(self,
                                                     mock_BaseJobContext,
                                                     mock_CredsViaAirflow,
                                                     mock_CredsViaLastPass):
        with self.assertRaises(ValueError) as r:
            job_context = self.mock_job_context(job_context_type='bogus')
            self.assertEqual(job_context, mock_BaseJobContext.return_value)
        self.assertEqual(str(r.exception),
                         "Valid job context types: cli, airflow, docker-itest, env - "
                         "consider upgrading records-mover if you're looking for bogus.")
