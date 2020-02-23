from records_mover.cli.cli_job_context import CLIJobContext
import unittest
from mock import patch


@patch.dict('os.environ', {
    'AWS_SECRET_ACCESS_KEY': 'aws secret key',
    'AWS_SESSION_TOKEN': 'aws session token',
    'AWS_ACCESS_KEY_ID': 'aws access key',
})
class TestCLIJobContextEngine(unittest.TestCase):
    @patch('records_mover.base_job_context.db_engine')
    def test_get_default_db_engine(self, mock_db_engine):
        context = CLIJobContext('name',
                                default_db_creds_name=None,
                                default_aws_creds_name=None)
        self.assertEqual(mock_db_engine.return_value, context.get_default_db_engine())
        mock_db_engine.assert_called_with(context)
