from records_mover import Session
import unittest
from mock import patch, ANY


@patch('records_mover.session.subprocess')
@patch.dict('os.environ', {
    'AWS_SECRET_ACCESS_KEY': 'aws secret key',
    'AWS_SESSION_TOKEN': 'aws session token',
    'AWS_ACCESS_KEY_ID': 'aws access key',
})
class TestCLISessionRecords(unittest.TestCase):
    @patch('records_mover.session.Records')
    def test_records(self,
                     mock_Records,
                     mock_subprocess):
        mock_subprocess.check_output.return_value = 'jdoe'.encode('utf-8')
        context = Session(session_type='cli',
                          default_db_creds_name=None,
                          default_aws_creds_name=None)
        self.assertEqual(mock_Records.return_value,
                         context.records)
        mock_Records.assert_called_with(db_driver=ANY,
                                        url_resolver=ANY)

    @patch('records_mover.session.Records')
    @patch.dict('os.environ', {'SCRATCH_S3_URL': 's3://different-scratch-bucket/'})
    def test_records_with_overridden_scratch_bucket(self,
                                                    mock_Records,
                                                    mock_subprocess):
        mock_subprocess.check_output.return_value = 'jdoe'.encode('utf-8')
        context = Session(session_type='cli',
                          default_db_creds_name=None,
                          default_aws_creds_name=None)
        self.assertEqual(context._scratch_s3_url, 's3://different-scratch-bucket/')
