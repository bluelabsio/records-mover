from mock import patch
import unittest

# TODO: Walk through and confirm each of these test cases
@patch('records_mover.job_context.BaseJobContext')
@patch('records_mover.job_context.CLIJobContext')
class TestJobContextChoices(unittest.TestCase):
    def mock_job_context(self, **kwargs):
        from records_mover import job_context

        return job_context.get_job_context(**kwargs)

    @patch.dict('os.environ', {
        'AIRFLOW__CORE__EXECUTOR': 'whoop',
    })
    def test_select_airflow_job_context_by_implicit_env_variable(self,
                                                                 mock_CLIJobContext,
                                                                 mock_BaseJobContext):
        job_context = self.mock_job_context()
        self.assertEqual(job_context, mock_BaseJobContext.return_value)

    def test_select_cli_job_context_by_default(self,
                                               mock_CLIJobContext,
                                               mock_BaseJobContext):
        job_context = self.mock_job_context()
        self.assertEqual(job_context, mock_CLIJobContext.return_value)

    @patch.dict('os.environ', {
        'PY_JOB_CONTEXT': 'cli',
    })
    def test_select_cli_job_context_by_explicit_env_variable(self,
                                                             mock_CLIJobContext,
                                                             mock_BaseJobContext):
        job_context = self.mock_job_context()
        self.assertEqual(job_context, mock_CLIJobContext.return_value)

    @patch.dict('os.environ', {
        'PY_JOB_CONTEXT': 'airflow',
    })
    def test_select_airflow_job_context_by_explicit_env_variable(self,
                                                                 mock_CLIJobContext,
                                                                 mock_BaseJobContext):
        job_context = self.mock_job_context()
        self.assertEqual(job_context, mock_BaseJobContext.return_value)
        self.assertEqual(type(job_context.creds), mock_BaseJobContext.return_value)

    @patch.dict('os.environ', {
        'PY_JOB_CONTEXT': 'bogus',
    })
    def test_select_invalid_job_context_by_explicit_env_variable(self,
                                                                 mock_CLIJobContext,
                                                                 mock_BaseJobContext):
        with self.assertRaises(ValueError) as r:
            jc = self.mock_job_context()
            print(f"Got jc: {jc}")
        self.assertEqual(str(r.exception),
                         'Valid job context types: cli, airflow, docker-itest, env - '
                         "consider upgrading records-mover if you're looking for bogus.")

    def test_select_airflow_job_context_by_parameter(self,
                                                     mock_CLIJobContext,
                                                     mock_BaseJobContext):
        with self.mock_job_context(job_context_type='airflow') as injected_job_context:
            self.assertEqual(injected_job_context, mock_BaseJobContext.return_value)
        job_context = self.mock_job_context(job_context_type='airflow')
        self.assertEqual(job_context, mock_BaseJobContext.return_value)
        self.assertEqual(type(job_context.creds), 123)

    def test_select_cli_job_context_by_parameter(self,
                                                 mock_CLIJobContext,
                                                 mock_BaseJobContext):
        job_context = self.mock_job_context(job_context_type='cli')
        self.assertEqual(job_context, mock_CLIJobContext.return_value)

    def test_select_invalid_job_context_by_parameter(self,
                                                     mock_CLIJobContext,
                                                     mock_BaseJobContext):
        with self.assertRaises(ValueError) as r:
            job_context = self.mock_job_context(job_context_type='bogus')
            self.assertEqual(job_context, mock_CLIJobContext.return_value)
        self.assertEqual(str(r.exception),
                         "Valid job context types: cli, airflow, docker-itest, env - "
                         "consider upgrading records-mover if you're looking for bogus.")
