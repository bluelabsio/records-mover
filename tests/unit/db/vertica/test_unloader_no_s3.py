from records_mover.db.vertica.unloader import VerticaUnloader
from records_mover.db.errors import NoTemporaryBucketConfiguration
from records_mover.records.records_format import DelimitedRecordsFormat
import unittest
from mock import patch, Mock


class TestVerticaUnloaderNoS3(unittest.TestCase):
    maxDiff = None

    @patch('records_mover.db.unloader.RecordsUnloadPlan')
    def test_no_temp_bucket_can_unload_this_format_true(self,
                                                        mock_RecordsUnloadPlan):
        mock_db = Mock(name='db')
        mock_s3_temp_base_loc = None
        mock_target_records_format = Mock(name='target_records_format', spec=DelimitedRecordsFormat)
        mock_target_records_format.hints = {}
        unloader = VerticaUnloader(db=mock_db, s3_temp_base_loc=mock_s3_temp_base_loc)

        out = unloader.can_unload_this_format(mock_target_records_format)
        self.assertFalse(out)

    def test_no_temp_bucket_unload(self):
        mock_db = Mock(name='db')
        mock_s3_temp_base_loc = None
        unloader = VerticaUnloader(db=mock_db, s3_temp_base_loc=mock_s3_temp_base_loc)

        mock_schema = Mock(name='schema')
        mock_table = Mock(name='table')
        mock_unload_plan = Mock(name='unload_plan')
        mock_unload_plan.records_format = Mock(spec=DelimitedRecordsFormat)
        mock_unload_plan.records_format.hints = {
            'compression': 'GZIP'
        }
        mock_directory = Mock(name='directory')

        with self.assertRaises(NotImplementedError):
            unloader.unload(schema=mock_schema,
                            table=mock_table,
                            unload_plan=mock_unload_plan,
                            directory=mock_directory)

    def test_no_temp_bucket_known_supported_records_formats_for_unload(self):
        mock_db = Mock(name='db')
        mock_s3_temp_base_loc = None
        unloader = VerticaUnloader(db=mock_db, s3_temp_base_loc=mock_s3_temp_base_loc)

        out = unloader.known_supported_records_formats_for_unload()
        self.assertEqual([], out)

    def test_temporary_loadable_directory_load_with_no_s3_temp_bucket_configured(self):
        mock_db = Mock(name='db')
        mock_s3_temp_base_loc = None
        vertica_unloader = VerticaUnloader(db=mock_db, s3_temp_base_loc=mock_s3_temp_base_loc)
        with self.assertRaises(NoTemporaryBucketConfiguration):
            with vertica_unloader.temporary_loadable_directory_loc():
                pass
