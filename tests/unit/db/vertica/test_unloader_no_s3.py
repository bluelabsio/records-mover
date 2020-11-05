from records_mover.db.vertica.unloader import VerticaUnloader
from records_mover.db.errors import NoTemporaryBucketConfiguration
from records_mover.records.records_format import DelimitedRecordsFormat
import unittest
from mock import patch, Mock


class TestVerticaUnloaderNoS3(unittest.TestCase):
    maxDiff = None

    def test_temporary_loadable_directory_load_with_no_s3_temp_bucket_configured(self):
        mock_db = Mock(name='db')
        mock_s3_temp_base_loc = None
        vertica_unloader = VerticaUnloader(db=mock_db, s3_temp_base_loc=mock_s3_temp_base_loc)
        with self.assertRaises(NoTemporaryBucketConfiguration):
            with vertica_unloader.temporary_loadable_directory_loc():
                pass

    def test_can_unload_to_scheme_s3_but_no_s3_export_false(self):
        mock_db = Mock(name='db')
        mock_resultset = Mock(name='resultset')
        mock_db.execute.return_value = mock_resultset
        mock_resultset.fetchall.return_value = []

        mock_s3_temp_base_loc = None
        vertica_unloader = VerticaUnloader(db=mock_db, s3_temp_base_loc=mock_s3_temp_base_loc)
        self.assertFalse(vertica_unloader.can_unload_to_scheme('s3'))

        mock_db.execute.\
            assert_called_with("SELECT lib_name from user_libraries where lib_name = 'awslib'")

    def test_can_unload_to_scheme_s3_but_with_s3_export_true(self):
        mock_db = Mock(name='db')
        mock_resultset = Mock(name='resultset')
        mock_db.execute.return_value = mock_resultset
        mock_resultset.fetchall.return_value = ['awslib']

        mock_s3_temp_base_loc = None
        vertica_unloader = VerticaUnloader(db=mock_db, s3_temp_base_loc=mock_s3_temp_base_loc)
        self.assertTrue(vertica_unloader.can_unload_to_scheme('s3'))

        mock_db.execute.\
            assert_called_with("SELECT lib_name from user_libraries where lib_name = 'awslib'")

    def test_s3_temp_bucket_available_false(self):
        mock_db = Mock(name='db')
        vertica_unloader = VerticaUnloader(db=mock_db, s3_temp_base_loc=None)
        self.assertFalse(vertica_unloader.s3_temp_bucket_available())
