import unittest
from records_mover.airflow.hooks.records_hook import RecordsHook
from mock import patch, Mock, ANY


class TestRecordsHook(unittest.TestCase):
    def setUp(self):
        self.mock_s3_temp_base_url = Mock(name='s3_temp_base_url')
        self.mock_aws_conn_id = Mock(name='aws_conn_id')
        self.records_hook = RecordsHook(s3_temp_base_url=self.mock_s3_temp_base_url,
                                        aws_conn_id=self.mock_aws_conn_id)

    @patch('records_mover.airflow.hooks.records_hook.UrlResolver')
    @patch('records_mover.airflow.hooks.records_hook.Records')
    @patch('records_mover.airflow.hooks.records_hook.base_aws.AwsBaseHook')
    def test_get_conn(self,
                      mock_AwsBaseHook,
                      mock_Records,
                      mock_UrlResolver):
        conn = self.records_hook.get_conn()
        mock_Records.assert_called_with(db_driver=ANY,
                                        url_resolver=ANY)
        self.assertEqual(mock_Records.return_value, conn)

    @patch('records_mover.airflow.hooks.records_hook.UrlResolver')
    @patch('records_mover.airflow.hooks.records_hook.Records')
    @patch('records_mover.airflow.hooks.records_hook.base_aws.AwsBaseHook')
    @patch('records_mover.airflow.hooks.records_hook.db_driver')
    def test_get_conn_invalid_s3_url(self,
                                     mock_db_driver,
                                     mock_AwsBaseHook,
                                     mock_Records,
                                     mock_UrlResolver):
        records_hook = RecordsHook(s3_temp_base_url='foo',
                                   aws_conn_id=self.mock_aws_conn_id)
        conn = records_hook.get_conn()
        mock_Records.assert_called_with(db_driver=ANY,
                                        url_resolver=ANY)
        name, args, kwargs = mock_Records.mock_calls[0]
        db_driver = kwargs['db_driver']
        self.assertEqual(mock_Records.return_value, conn)
        with self.assertRaises(ValueError):
            mock_db = Mock(name='db')
            db_driver(mock_db)
            mock_db_driver.assert_called_with()
