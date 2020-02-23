from .test_cli_job_context import test_config_schema, args
from records_mover.cli.cli_job_context import CLIJobContext
from records_mover.creds.creds_via_lastpass import CredsViaLastPass
import unittest
from mock import patch, ANY


@patch.dict('os.environ', {
    'AWS_SECRET_ACCESS_KEY': 'aws secret key',
    'AWS_SESSION_TOKEN': 'aws session token',
    'AWS_ACCESS_KEY_ID': 'aws access key',
})
class TestCLIJobContextRecords(unittest.TestCase):
    @patch('records_mover.base_job_context.Records')
    @patch('records_mover.cli.cli_job_context.subprocess')
    def test_records(self,
                     mock_subprocess,
                     mock_Records):
        mock_subprocess.check_output.return_value = 'jdoe'.encode('utf-8')
        context = CLIJobContext('name',
                                creds=CredsViaLastPass(),
                                config_json_schema=test_config_schema,
                                default_db_creds_name=None,
                                default_aws_creds_name=None,
                                args=args)
        self.assertEqual(mock_Records.return_value,
                         context.records)
        mock_Records.assert_called_with(db_driver=ANY, logger=ANY,
                                        url_resolver=ANY)

    @patch('records_mover.base_job_context.Records')
    @patch('records_mover.cli.cli_job_context.subprocess')
    @patch.dict('os.environ', {'SCRATCH_S3_URL': 's3://different-scratch-bucket/'})
    def test_records_with_overridden_scratch_bucket(self,
                                                    mock_subprocess,
                                                    mock_Records):
        mock_subprocess.check_output.return_value = 'jdoe'.encode('utf-8')
        context = CLIJobContext('name',
                                creds=CredsViaLastPass(),
                                config_json_schema=test_config_schema,
                                default_db_creds_name=None,
                                default_aws_creds_name=None,
                                args=args)
        self.assertEqual(mock_Records.return_value,
                         context.records)
        mock_Records.assert_called_with(db_driver=ANY, logger=ANY,
                                        url_resolver=ANY)
