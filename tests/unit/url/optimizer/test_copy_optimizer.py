import unittest
from unittest.mock import MagicMock, Mock, patch
from records_mover.url.gcs.gcs_directory_url import GCSDirectoryUrl
from records_mover.url.s3.s3_directory_url import S3DirectoryUrl
from records_mover.url.optimizer import CopyOptimizer


class TestCopyOptimizer(unittest.TestCase):
    @patch('records_mover.url.optimizer.GcpDataTransferService')
    def test_copy_s3_to_gcs(self,
                            mock_GcpDataTransferService):
        optimizer = CopyOptimizer()
        mock_loc = Mock(name='loc', spec=S3DirectoryUrl)
        mock_other_loc = Mock(name='other_loc', spec=GCSDirectoryUrl)
        mock_loc.scheme = 's3'
        mock_other_loc.scheme = 'gs'
        mock_gcp_data_transfer = mock_GcpDataTransferService.return_value
        self.assertEqual(optimizer.copy(mock_loc, mock_other_loc),
                         mock_gcp_data_transfer.copy.return_value)
        mock_gcp_data_transfer.copy.assert_called_with(mock_loc,
                                                       mock_other_loc)

    @patch('records_mover.url.optimizer.GcpDataTransferService')
    def test_optimize_temp_locations_s3_gcs(self,
                                            mock_GcpDataTransferService):
        mock_gcp_data_transfer = MagicMock(name='gcp_data_transfer')
        mock_GcpDataTransferService.return_value = mock_gcp_data_transfer
        optimizer = CopyOptimizer()
        mock_temp_first_loc = Mock(name='temp_first_loc', spec=S3DirectoryUrl)
        mock_temp_second_loc = Mock(name='temp_second_loc', spec=GCSDirectoryUrl)
        mock_temp_first_loc.scheme = 's3'
        mock_temp_second_loc.scheme = 'gs'
        mock_optimized_temp_first_loc = Mock(name='optimized_temp_first_loc')
        mock_optimized_temp_second_loc = Mock(name='optimized_temp_second_loc')
        yield_value = (mock_optimized_temp_first_loc, mock_optimized_temp_second_loc)
        mock_gcp_data_transfer.optimize_temp_locations.return_value.__enter__.return_value =\
            yield_value
        with optimizer.optimize_temp_locations(mock_temp_first_loc,
                                               mock_temp_second_loc) as (optimized_temp_first_loc,
                                                                         optimized_temp_second_loc):
            self.assertEqual(optimized_temp_first_loc, mock_optimized_temp_first_loc)
            self.assertEqual(optimized_temp_second_loc, mock_optimized_temp_second_loc)

        mock_gcp_data_transfer.optimize_temp_locations.assert_called_with(mock_temp_first_loc,
                                                                          mock_temp_second_loc)

    def test_optimize_temp_locations_same_scheme(self):
        optimizer = CopyOptimizer()
        mock_temp_first_loc = Mock(name='temp_first_loc')
        mock_temp_second_loc = Mock(name='temp_second_loc')
        mock_temp_second_loc.scheme = mock_temp_first_loc.scheme
        with optimizer.optimize_temp_locations(mock_temp_first_loc,
                                               mock_temp_second_loc) as (optimized_temp_first_loc,
                                                                         optimized_temp_second_loc):
            self.assertEqual(optimized_temp_first_loc, optimized_temp_second_loc)
            self.assertEqual(optimized_temp_first_loc, mock_temp_first_loc)
            self.assertEqual(optimized_temp_second_loc, mock_temp_first_loc)

    def test_optimize_temp_locations_no_optimizations(self):
        optimizer = CopyOptimizer()
        mock_temp_first_loc = Mock(name='temp_first_loc')
        mock_temp_second_loc = Mock(name='temp_second_loc')
        with optimizer.optimize_temp_locations(mock_temp_first_loc,
                                               mock_temp_second_loc) as (optimized_temp_first_loc,
                                                                         optimized_temp_second_loc):
            self.assertEqual(optimized_temp_first_loc, mock_temp_first_loc)
            self.assertEqual(optimized_temp_second_loc, mock_temp_second_loc)

    def test_optimize_temp_second_location_no_optimizations(self):
        optimizer = CopyOptimizer()
        mock_permanent_first_loc = Mock(name='permanent_first_loc')
        mock_temp_second_loc = Mock(name='temp_second_loc')
        with optimizer.optimize_temp_second_location(mock_permanent_first_loc,
                                                     mock_temp_second_loc) as\
                optimized_temp_second_loc:
            self.assertEqual(optimized_temp_second_loc, mock_temp_second_loc)

    def test_optimize_temp_second_location_same_scheme(self):
        optimizer = CopyOptimizer()
        mock_permanent_first_loc = Mock(name='permanent_first_loc')
        mock_temp_second_loc = Mock(name='temp_second_loc')
        mock_temp_second_loc.scheme = mock_permanent_first_loc.scheme
        with optimizer.optimize_temp_second_location(mock_permanent_first_loc,
                                                     mock_temp_second_loc) as\
                optimized_temp_second_loc:
            self.assertEqual(optimized_temp_second_loc, mock_permanent_first_loc)

    @patch('records_mover.url.optimizer.GcpDataTransferService')
    def test_optimize_temp_second_location_s3_gcs(self,
                                                  mock_GcpDataTransferService):
        mock_gcp_data_transfer = MagicMock(name='gcp_data_transfer')
        mock_GcpDataTransferService.return_value = mock_gcp_data_transfer
        optimizer = CopyOptimizer()
        mock_permanent_first_loc = Mock(name='temp_first_loc', spec=S3DirectoryUrl)
        mock_temp_second_loc = Mock(name='temp_second_loc', spec=GCSDirectoryUrl)
        mock_permanent_first_loc.scheme = 's3'
        mock_temp_second_loc.scheme = 'gs'
        mock_optimized_temp_second_loc = Mock(name='optimized_temp_second_loc')
        yield_value = mock_optimized_temp_second_loc
        mock_gcp_data_transfer.optimize_temp_second_location.return_value.__enter__.return_value =\
            yield_value
        with optimizer.optimize_temp_second_location(mock_permanent_first_loc,
                                                     mock_temp_second_loc) as\
                optimized_temp_second_loc:
            self.assertEqual(optimized_temp_second_loc, mock_optimized_temp_second_loc)

        mock_gcp_data_transfer.optimize_temp_second_location.\
            assert_called_with(mock_permanent_first_loc,
                               mock_temp_second_loc)
