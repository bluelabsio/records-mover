from records_mover.airflow.hooks.records_hook import RecordsHook
from mock import patch
import unittest


class TestRecordsHook(unittest.TestCase):
    @patch('records_mover.airflow.hooks.records_hook.base_aws')
    def test_validate_and_prepare_target_directory(self,
                                                   mock_base_aws):
        target_url = 's3://bluelabs-fake-bucket'
        mock_boto3_session = mock_base_aws.return_value.get_session.return_value
        mock_s3 = mock_boto3_session.client.return_value
        mock_s3.list_objects_v2.return_value.get.return_value =\
            [{
                'Key': 'key1'
            }]
        RecordsHook().validate_and_prepare_target_directory(target_url,
                                                            allow_overwrite=True)
        mock_boto3_session.client.assert_called_with('s3')
        mock_s3.delete_objects.assert_called_with(Bucket='bluelabs-fake-bucket',
                                                  Delete={"Objects": [{"Key": 'key1'}]})
