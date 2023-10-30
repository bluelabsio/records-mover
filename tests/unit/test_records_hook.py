from records_mover.airflow.hooks.records_hook import RecordsHook
import unittest
from mock import patch, Mock


@patch.dict('os.environ', {
    'AWS_SECRET_ACCESS_KEY': 'aws secret key',
    'AWS_SESSION_TOKEN': 'aws session token',
    'AWS_ACCESS_KEY_ID': 'aws access key',
})
class TestRecordsHook(unittest.TestCase):
    @patch('records_mover.airflow.hooks.records_hook.Records')
    @patch('records_mover.airflow.hooks.records_hook.db_driver')
    @patch('records_mover.airflow.hooks.records_hook.UrlResolver')
    @patch('records_mover.airflow.hooks.records_hook.AwsBaseHook')
    def test_get_conn(self,
                      mock_AwsBaseHook,
                      mock_UrlResolver,
                      mock_db_driver,
                      mock_Records):
        mock_s3_temp_base_url = Mock(name='s3_temp_base_url')
        records_hook = RecordsHook(s3_temp_base_url=mock_s3_temp_base_url)
        out = records_hook.get_conn()
        self.assertEqual(out, mock_Records.return_value)
        args, kwargs = mock_Records.call_args
        mock_db = Mock(name='db')
        self.assertEqual(mock_db_driver.return_value, kwargs['db_driver'](db_conn=mock_db))
        self.assertEqual(mock_UrlResolver.return_value, kwargs['url_resolver'])
