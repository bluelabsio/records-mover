import unittest
from records_mover.db.redshift.unloader import RedshiftUnloader
from records_mover.records.records_format import DelimitedRecordsFormat
from mock import patch, Mock


class TestRedshiftUnloader(unittest.TestCase):
    @patch('records_mover.db.redshift.unloader.redshift_unload_options')
    @patch('records_mover.db.redshift.unloader.RecordsUnloadPlan')
    def test_can_unload_this_format_true(self,
                                         mock_RecordsUnloadPlan,
                                         mock_redshift_unload_options):
        mock_db = Mock(name='db')
        mock_table = Mock(name='table')
        mock_temporary_s3_directory_loc = Mock(name='temporary_s3_directory_loc')

        mock_target_records_format = Mock(name='target_records_format', spec=DelimitedRecordsFormat)
        mock_unload_plan = mock_RecordsUnloadPlan.return_value
        mock_unload_plan.records_format = mock_target_records_format

        mock_processing_instructions = mock_unload_plan.processing_instructions
        mock_temporary_s3_directory_loc = Mock(name='temporary_s3_directory_loc')
        mock_target_records_format.hints = {}

        redshift_unloader =\
            RedshiftUnloader(db=mock_db,
                             table=mock_table,
                             temporary_s3_directory_loc=mock_temporary_s3_directory_loc)
        out = redshift_unloader.can_unload_this_format(mock_target_records_format)
        mock_RecordsUnloadPlan.\
            assert_called_with(records_format=mock_target_records_format)
        mock_redshift_unload_options.\
            assert_called_with(set(),
                               mock_unload_plan.records_format,
                               mock_processing_instructions.fail_if_cant_handle_hint)
        self.assertEqual(True, out)
