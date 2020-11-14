import unittest
from unittest.mock import Mock
from records_mover.url.optimizer.gcp_data_transfer_service import GcpDataTransferService


class TestGcpDataTransferService(unittest.TestCase):
    def test_optimize_temp_second_location_optimized(self):
        service = GcpDataTransferService()
        mock_permanent_first_loc = Mock(name='permanent_first_loc')
        mock_temp_second_loc = Mock(name='temp_second_loc')
        mock_optimized_directory = mock_temp_second_loc.directory_in_this_bucket.return_value
        mock_optimized_directory.empty.return_value = True
        mock_optimized_directory.writable.return_value = True
        with service.optimize_temp_second_location(mock_permanent_first_loc,
                                                   mock_temp_second_loc) as\
                optimized_temp_second_loc:
            self.assertEqual(optimized_temp_second_loc, mock_optimized_directory)
        mock_temp_second_loc.directory_in_this_bucket.\
            assert_called_with(mock_permanent_first_loc.key)

    def test_optimize_temp_second_location_unoptimized_not_empty(self):
        service = GcpDataTransferService()
        mock_permanent_first_loc = Mock(name='permanent_first_loc')
        mock_temp_second_loc = Mock(name='temp_second_loc')
        mock_optimized_directory = mock_temp_second_loc.directory_in_this_bucket.return_value
        mock_optimized_directory.empty.return_value = False
        mock_optimized_directory.writable.return_value = True
        with service.optimize_temp_second_location(mock_permanent_first_loc,
                                                   mock_temp_second_loc) as\
                optimized_temp_second_loc:
            self.assertEqual(optimized_temp_second_loc, mock_temp_second_loc)
        mock_temp_second_loc.directory_in_this_bucket.\
            assert_called_with(mock_permanent_first_loc.key)

    def test_optimize_temp_second_location_unoptimized_not_writable(self):
        service = GcpDataTransferService()
        mock_permanent_first_loc = Mock(name='permanent_first_loc')
        mock_temp_second_loc = Mock(name='temp_second_loc')
        mock_optimized_directory = mock_temp_second_loc.directory_in_this_bucket.return_value
        mock_optimized_directory.empty.return_value = True
        mock_optimized_directory.writable.return_value = False
        with service.optimize_temp_second_location(mock_permanent_first_loc,
                                                   mock_temp_second_loc) as\
                optimized_temp_second_loc:
            self.assertEqual(optimized_temp_second_loc, mock_temp_second_loc)
        mock_temp_second_loc.directory_in_this_bucket.\
            assert_called_with(mock_permanent_first_loc.key)

    def test_optimize_temp_locations_first_bucket_optimized(self):
        service = GcpDataTransferService()
        mock_temp_first_loc = Mock(name='temp_first_loc')
        mock_temp_second_loc = Mock(name='temp_second_loc')
        mock_optimized_directory = mock_temp_first_loc.directory_in_this_bucket.return_value
        mock_optimized_directory.empty.return_value = True
        mock_optimized_directory.writable.return_value = True
        with service.optimize_temp_locations(mock_temp_first_loc,
                                             mock_temp_second_loc) as\
                (optimized_temp_first_loc, optimized_temp_second_loc):
            self.assertEqual(optimized_temp_first_loc, mock_optimized_directory)
            self.assertEqual(optimized_temp_second_loc, mock_temp_second_loc)
        mock_temp_first_loc.directory_in_this_bucket.\
            assert_called_with(mock_temp_second_loc.key)

    def test_optimize_temp_locations_second_bucket_optimized_first_not_empty(self):
        service = GcpDataTransferService()
        mock_temp_first_loc = Mock(name='temp_first_loc')
        mock_temp_second_loc = Mock(name='temp_second_loc')

        mock_first_optimized_directory =\
            mock_temp_first_loc.directory_in_this_bucket.return_value
        mock_first_optimized_directory.empty.return_value = False
        mock_first_optimized_directory.writable.return_value = True

        mock_second_optimized_directory =\
            mock_temp_second_loc.directory_in_this_bucket.return_value
        mock_second_optimized_directory.empty.return_value = True
        mock_second_optimized_directory.writable.return_value = True

        with service.optimize_temp_locations(mock_temp_first_loc,
                                             mock_temp_second_loc) as\
                (optimized_temp_first_loc, optimized_temp_second_loc):
            self.assertEqual(optimized_temp_first_loc, mock_temp_first_loc)
            self.assertEqual(optimized_temp_second_loc, mock_second_optimized_directory)
        mock_temp_first_loc.directory_in_this_bucket.\
            assert_called_with(mock_temp_second_loc.key)
        mock_temp_second_loc.directory_in_this_bucket.\
            assert_called_with(mock_temp_first_loc.key)

    def test_optimize_temp_locations_not_optimized_both_not_empty(self):
        service = GcpDataTransferService()
        mock_temp_first_loc = Mock(name='temp_first_loc')
        mock_temp_second_loc = Mock(name='temp_second_loc')

        mock_first_optimized_directory =\
            mock_temp_first_loc.directory_in_this_bucket.return_value
        mock_first_optimized_directory.empty.return_value = False
        mock_first_optimized_directory.writable.return_value = True

        mock_second_optimized_directory =\
            mock_temp_second_loc.directory_in_this_bucket.return_value
        mock_second_optimized_directory.empty.return_value = False
        mock_second_optimized_directory.writable.return_value = True

        with service.optimize_temp_locations(mock_temp_first_loc,
                                             mock_temp_second_loc) as\
                (optimized_temp_first_loc, optimized_temp_second_loc):
            self.assertEqual(optimized_temp_first_loc, mock_temp_first_loc)
            self.assertEqual(optimized_temp_second_loc, mock_temp_second_loc)
        mock_temp_first_loc.directory_in_this_bucket.\
            assert_called_with(mock_temp_second_loc.key)
        mock_temp_second_loc.directory_in_this_bucket.\
            assert_called_with(mock_temp_first_loc.key)
