import unittest
from records_mover.db.redshift.unloader import RedshiftUnloader
from records_mover.db.errors import NoTemporaryBucketConfiguration
from records_mover.records.records_format import DelimitedRecordsFormat, ParquetRecordsFormat
from mock import patch, Mock, MagicMock


class TestRedshiftUnloader(unittest.TestCase):
    @patch('records_mover.db.redshift.unloader.redshift_unload_options')
    @patch('records_mover.db.redshift.unloader.RecordsUnloadPlan')
    def test_can_unload_format_true(self,
                                    mock_RecordsUnloadPlan,
                                    mock_redshift_unload_options):
        mock_db = Mock(name='db')
        mock_table = Mock(name='table')

        mock_target_records_format = Mock(name='target_records_format', spec=DelimitedRecordsFormat)
        mock_unload_plan = mock_RecordsUnloadPlan.return_value
        mock_unload_plan.records_format = mock_target_records_format

        mock_processing_instructions = mock_unload_plan.processing_instructions
        mock_s3_temp_base_loc = Mock(name='s3_temp_base_loc')
        mock_target_records_format.hints = {}

        redshift_unloader =\
            RedshiftUnloader(db=None,
                             table=mock_table,
                             s3_temp_base_loc=mock_s3_temp_base_loc,
                             db_conn=mock_db)
        out = redshift_unloader.can_unload_format(mock_target_records_format)
        mock_RecordsUnloadPlan.\
            assert_called_with(records_format=mock_target_records_format)
        mock_redshift_unload_options.\
            assert_called_with(set(),
                               mock_unload_plan.records_format,
                               mock_processing_instructions.fail_if_cant_handle_hint)
        self.assertEqual(True, out)

    @patch('records_mover.db.redshift.unloader.redshift_unload_options')
    @patch('records_mover.db.redshift.unloader.RecordsUnloadPlan')
    def test_can_unload_format_delimite_false(self,
                                              mock_RecordsUnloadPlan,
                                              mock_redshift_unload_options):
        mock_db = Mock(name='db')
        mock_table = Mock(name='table')

        mock_target_records_format = Mock(name='target_records_format', spec=DelimitedRecordsFormat)
        mock_unload_plan = mock_RecordsUnloadPlan.return_value
        mock_unload_plan.records_format = mock_target_records_format

        mock_processing_instructions = mock_unload_plan.processing_instructions
        mock_s3_temp_base_loc = Mock(name='s3_temp_base_loc')
        mock_target_records_format.hints = {}
        mock_redshift_unload_options.side_effect = NotImplementedError

        redshift_unloader =\
            RedshiftUnloader(db=None,
                             table=mock_table,
                             s3_temp_base_loc=mock_s3_temp_base_loc,
                             db_conn=mock_db)
        out = redshift_unloader.can_unload_format(mock_target_records_format)
        mock_RecordsUnloadPlan.\
            assert_called_with(records_format=mock_target_records_format)
        mock_redshift_unload_options.\
            assert_called_with(set(),
                               mock_unload_plan.records_format,
                               mock_processing_instructions.fail_if_cant_handle_hint)
        self.assertEqual(False, out)

    def test_can_unload_to_scheme_s3_true(self):
        mock_db = Mock(name='db')
        mock_table = Mock(name='table')

        redshift_unloader =\
            RedshiftUnloader(db=None,
                             table=mock_table,
                             s3_temp_base_loc=None,
                             db_conn=mock_db)
        self.assertTrue(redshift_unloader.can_unload_to_scheme('s3'))

    def test_can_unload_to_scheme_file_without_temp_bucket_true(self):
        mock_db = Mock(name='db')
        mock_table = Mock(name='table')

        redshift_unloader =\
            RedshiftUnloader(db=None,
                             table=mock_table,
                             s3_temp_base_loc=None,
                             db_conn=mock_db)
        self.assertFalse(redshift_unloader.can_unload_to_scheme('file'))

    def test_can_unload_to_scheme_file_with_temp_bucket_true(self):
        mock_db = Mock(name='db')
        mock_table = Mock(name='table')
        mock_s3_temp_base_loc = Mock(name='s3_temp_base_loc')

        redshift_unloader =\
            RedshiftUnloader(db=None,
                             table=mock_table,
                             s3_temp_base_loc=mock_s3_temp_base_loc,
                             db_conn=mock_db)
        self.assertTrue(redshift_unloader.can_unload_to_scheme('file'))

    def test_known_supported_records_formats_for_unload(self):
        mock_db = Mock(name='db')
        mock_table = Mock(name='table')
        mock_s3_temp_base_loc = Mock(name='s3_temp_base_loc')

        redshift_unloader =\
            RedshiftUnloader(db=None,
                             table=mock_table,
                             s3_temp_base_loc=mock_s3_temp_base_loc,
                             db_conn=mock_db)
        formats = redshift_unloader.known_supported_records_formats_for_unload()

        self.assertEqual([f.__class__ for f in formats],
                         [DelimitedRecordsFormat, ParquetRecordsFormat])

    def test_temporary_unloadable_directory_loc(self):
        mock_db = Mock(name='db')
        mock_table = Mock(name='table')
        mock_s3_temp_base_loc = MagicMock(name='s3_temp_base_loc')

        redshift_unloader =\
            RedshiftUnloader(db=None,
                             table=mock_table,
                             s3_temp_base_loc=mock_s3_temp_base_loc,
                             db_conn=mock_db)
        with redshift_unloader.temporary_unloadable_directory_loc() as loc:
            self.assertEqual(loc,
                             mock_s3_temp_base_loc.temporary_directory.return_value.__enter__.
                             return_value)

    def test_temporary_unloadable_directory_loc_unset(self):
        mock_db = Mock(name='db')
        mock_table = Mock(name='table')
        mock_s3_temp_base_loc = None

        redshift_unloader =\
            RedshiftUnloader(db=None,
                             table=mock_table,
                             s3_temp_base_loc=mock_s3_temp_base_loc,
                             db_conn=mock_db)
        with self.assertRaises(NoTemporaryBucketConfiguration):
            with redshift_unloader.temporary_unloadable_directory_loc():
                pass
