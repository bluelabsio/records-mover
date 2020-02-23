from records_mover.airflow.airflow_job_context import AirflowJobContext
import unittest
from mock import patch


class TestAirflowJobContext(unittest.TestCase):
    @patch('records_mover.airflow.airflow_job_context.CredsViaAirflow')
    def test_creds(self, mock_CredsViaAirflow):
        context = AirflowJobContext(default_db_creds_name=None,
                                    default_aws_creds_name=None)
        self.assertEqual(mock_CredsViaAirflow.return_value, context.creds)
