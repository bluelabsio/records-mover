import unittest
import googleapiclient.discovery
from unittest.mock import Mock, patch
from records_mover.url.optimizer.gcp_data_transfer_service import GcpDataTransferService


class TestGcpDataTransferServiceCopy(unittest.TestCase):
    def test_copy_false_unmatching_paths(self):
        service = GcpDataTransferService()
        mock_loc = Mock(name='loc')
        mock_other_loc = Mock(name='other_loc')
        mock_loc.key = 'foo/'
        mock_other_loc.blob = 'bar/'
        self.assertEqual(False, service.copy(mock_loc, mock_other_loc))

    def test_copy_false_too_small(self):
        service = GcpDataTransferService()
        mock_loc = Mock(name='loc')
        mock_other_loc = Mock(name='other_loc')
        mock_loc.key = 'foo/'
        mock_other_loc.blob = 'foo/'
        mock_loc.size.return_value = 500_000
        self.assertEqual(False, service.copy(mock_loc, mock_other_loc))

    def test_copy_false_no_aws_creds(self):
        service = GcpDataTransferService()
        mock_loc = Mock(name='loc')
        mock_loc.aws_creds.return_value = None
        mock_other_loc = Mock(name='other_loc')
        mock_loc.key = 'foo/'
        mock_other_loc.blob = 'foo/'
        mock_loc.size.return_value = 500_000_000_000
        self.assertEqual(False, service.copy(mock_loc, mock_other_loc))

    def test_copy_false_temporary_aws_creds(self):
        service = GcpDataTransferService()
        mock_loc = Mock(name='loc')
        mock_aws_creds = mock_loc.aws_creds.return_value
        mock_aws_creds.token = Mock(name='token')
        mock_other_loc = Mock(name='other_loc')
        mock_loc.key = 'foo/'
        mock_other_loc.blob = 'foo/'
        mock_loc.size.return_value = 500_000_000_000
        self.assertEqual(False, service.copy(mock_loc, mock_other_loc))

    @patch('googleapiclient.discovery')
    def test_copy_false_rejected_request(self,
                                         mock_googleapiclient_discovery):
        service = GcpDataTransferService()
        mock_loc = Mock(name='loc')
        mock_aws_creds = mock_loc.aws_creds.return_value
        mock_aws_creds.token = None
        mock_other_loc = Mock(name='other_loc')
        mock_loc.key = 'foo/'
        mock_other_loc.blob = 'foo/'
        mock_loc.size.return_value = 500_000_000_000

        mock_gcp_credentials = mock_other_loc.credentials
        mock_storagetransfer = mock_googleapiclient_discovery.build.return_value
        mock_resp = Mock(name='resp')
        mock_storagetransfer.transferJobs.return_value.create.return_value.execute.side_effect =\
            googleapiclient.errors.HttpError(resp=mock_resp, content=b'bar')
        self.assertEqual(False, service.copy(mock_loc, mock_other_loc))
        mock_googleapiclient_discovery.build.assert_called_with('storagetransfer',
                                                                'v1',
                                                                credentials=mock_gcp_credentials)

    @patch('googleapiclient.discovery')
    @patch('records_mover.url.optimizer.gcp_data_transfer_service.time.sleep')
    def test_copy_true(self,
                       mock_sleep,
                       mock_googleapiclient_discovery):
        service = GcpDataTransferService()
        mock_loc = Mock(name='loc')
        mock_aws_creds = mock_loc.aws_creds.return_value
        mock_aws_creds.token = None
        mock_other_loc = Mock(name='other_loc')
        mock_loc.key = 'foo/'
        mock_other_loc.blob = 'foo/'
        mock_loc.size.return_value = 500_000_000_000

        mock_gcp_credentials = mock_other_loc.credentials
        mock_storagetransfer = mock_googleapiclient_discovery.build.return_value
        mock_job_name = 'my_job_name'
        mock_result = {
            'name': mock_job_name
        }
        mock_storagetransfer.transferJobs.return_value.create.return_value.execute.return_value =\
            mock_result

        mock_wait_result = {
            'operations': [
                {
                    'metadata': {
                        'status': 'DONE'
                    }
                }
            ]
        }
        mock_storagetransfer.transferOperations.return_value.list.\
            return_value.execute.return_value = mock_wait_result

        self.assertEqual(True, service.copy(mock_loc, mock_other_loc))
        mock_googleapiclient_discovery.build.assert_called_with('storagetransfer',
                                                                'v1',
                                                                credentials=mock_gcp_credentials)
