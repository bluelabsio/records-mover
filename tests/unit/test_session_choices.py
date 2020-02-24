from mock import patch
import unittest

# TODO: Walk through and confirm each of these test cases
@patch('records_mover.session.CredsViaLastPass')
@patch('records_mover.session.CredsViaAirflow')
class TestSessionChoices(unittest.TestCase):
    def mock_session(self, **kwargs):
        from records_mover import session

        return session.Session(scratch_s3_url='s3://foo/', **kwargs)

    @patch.dict('os.environ', {
        'AIRFLOW__CORE__EXECUTOR': 'whoop',
    })
    def test_select_airflow_session_by_implicit_env_variable(self,
                                                             mock_CredsViaAirflow,
                                                             mock_CredsViaLastPass):
        session = self.mock_session()
        self.assertEqual(session._default_aws_creds_name, 'aws_default')
        self.assertIsNone(session._default_db_creds_name)
        self.assertEqual(session.creds, mock_CredsViaAirflow.return_value)
        self.assertEqual(session._scratch_s3_url, 's3://foo/')

    def test_select_cli_session_by_default(self,
                                           mock_CredsViaAirflow,
                                           mock_CredsViaLastPass):
        session = self.mock_session()
        self.assertIsNone(session._default_aws_creds_name)
        self.assertIsNone(session._default_db_creds_name)
        self.assertEqual(session.creds, mock_CredsViaLastPass.return_value)
        self.assertEqual(session._scratch_s3_url, 's3://foo/')

    @patch.dict('os.environ', {
        'RECORDS_MOVER_SESSION_TYPE': 'cli',
    })
    def test_select_cli_session_by_explicit_env_variable(self,
                                                         mock_CredsViaAirflow,
                                                         mock_CredsViaLastPass):
        session = self.mock_session()
        self.assertIsNone(session._default_aws_creds_name)
        self.assertIsNone(session._default_db_creds_name)
        self.assertEqual(session.creds, mock_CredsViaLastPass.return_value)
        self.assertEqual(session._scratch_s3_url, 's3://foo/')

    @patch.dict('os.environ', {
        'RECORDS_MOVER_SESSION_TYPE': 'airflow',
    })
    def test_select_airflow_session_by_explicit_env_variable(self,
                                                             mock_CredsViaAirflow,
                                                             mock_CredsViaLastPass):
        session = self.mock_session()
        self.assertEqual(session._default_aws_creds_name, 'aws_default')
        self.assertIsNone(session._default_db_creds_name)
        self.assertEqual(session.creds, mock_CredsViaAirflow.return_value)
        self.assertEqual(session._scratch_s3_url, 's3://foo/')

    @patch.dict('os.environ', {
        'RECORDS_MOVER_SESSION_TYPE': 'bogus',
    })
    def test_select_invalid_session_by_explicit_env_variable(self,
                                                             mock_CredsViaAirflow,
                                                             mock_CredsViaLastPass):
        with self.assertRaises(ValueError) as r:
            session = self.mock_session()
            print(f"Got session: {session}")
        self.assertEqual(str(r.exception),
                         'Valid job context types: cli, airflow, docker-itest, env - '
                         "consider upgrading records-mover if you're looking for bogus.")

    def test_select_airflow_session_by_parameter(self,
                                                 mock_CredsViaAirflow,
                                                 mock_CredsViaLastPass):
        session = self.mock_session(session_type='airflow')
        self.assertEqual(session._default_aws_creds_name, 'aws_default')
        self.assertIsNone(session._default_db_creds_name)
        self.assertEqual(session.creds, mock_CredsViaAirflow.return_value)
        self.assertEqual(session._scratch_s3_url, 's3://foo/')

    def test_select_cli_session_by_parameter(self,
                                             mock_CredsViaAirflow,
                                             mock_CredsViaLastPass):
        session = self.mock_session(session_type='cli')
        self.assertIsNone(session._default_aws_creds_name)
        self.assertIsNone(session._default_db_creds_name)
        self.assertEqual(session.creds, mock_CredsViaLastPass.return_value)
        self.assertEqual(session._scratch_s3_url, 's3://foo/')

    def test_select_invalid_session_by_parameter(self,
                                                 mock_CredsViaAirflow,
                                                 mock_CredsViaLastPass):
        with self.assertRaises(ValueError) as r:
            self.mock_session(session_type='bogus')
        self.assertEqual(str(r.exception),
                         "Valid job context types: cli, airflow, docker-itest, env - "
                         "consider upgrading records-mover if you're looking for bogus.")
