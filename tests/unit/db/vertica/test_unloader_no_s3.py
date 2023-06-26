from records_mover.db.vertica.unloader import VerticaUnloader
from records_mover.db.errors import NoTemporaryBucketConfiguration
import unittest
from mock import Mock


class TestVerticaUnloaderNoS3(unittest.TestCase):
    maxDiff = None

    def test_temporary_unloadable_directory_load_with_no_s3_temp_bucket_configured(self):
        mock_db = Mock(name='db')
        mock_s3_temp_base_loc = None
        vertica_unloader = VerticaUnloader(db=None, s3_temp_base_loc=mock_s3_temp_base_loc,
                                           db_conn=mock_db)
        with self.assertRaises(NoTemporaryBucketConfiguration):
            with vertica_unloader.temporary_unloadable_directory_loc():
                pass

    def test_can_unload_to_scheme_s3_but_no_s3_export_false(self):
        mock_db = Mock(name='db')
        mock_resultset = Mock(name='resultset')
        mock_db.execute.return_value = mock_resultset
        mock_resultset.fetchall.return_value = []

        mock_s3_temp_base_loc = None
        vertica_unloader = VerticaUnloader(db=None, s3_temp_base_loc=mock_s3_temp_base_loc,
                                           db_conn=mock_db)
        self.assertFalse(vertica_unloader.can_unload_to_scheme('s3'))

    def test_can_unload_to_scheme_s3_but_with_s3_export_true(self):
        mock_db = Mock(name='db')
        mock_resultset = Mock(name='resultset')
        mock_db.execute.return_value = mock_resultset
        mock_resultset.fetchall.return_value = ['awslib']

        mock_s3_temp_base_loc = None
        vertica_unloader = VerticaUnloader(db=None, s3_temp_base_loc=mock_s3_temp_base_loc,
                                           db_conn=mock_db)
        self.assertTrue(vertica_unloader.can_unload_to_scheme('s3'))

        str_arg = str(mock_db.execute.call_args.args[0])
        self.assertEqual(str_arg, "SELECT lib_name from user_libraries where lib_name = 'awslib'")

    def test_s3_temp_bucket_available_false(self):
        mock_db = Mock(name='db')
        vertica_unloader = VerticaUnloader(db=None, s3_temp_base_loc=None, db_conn=mock_db)
        self.assertFalse(vertica_unloader.s3_temp_bucket_available())
