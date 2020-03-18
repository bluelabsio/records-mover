import unittest
from records_mover.db.redshift.loader import RedshiftLoader
from records_mover.records.records_format import DelimitedRecordsFormat
from mock import patch, Mock


class TestRedshiftLoader(unittest.TestCase):
    def setUp(self):
        mock_db = Mock(name='db')
        mock_meta = Mock(name='meta')
        mock_temporary_s3_directory_loc = Mock(name='temporary_s3_directory_loc')

        self.redshift_loader =\
            RedshiftLoader(db=mock_db,
                           meta=mock_meta,
                           temporary_s3_directory_loc=mock_temporary_s3_directory_loc)

    @patch('records_mover.db.redshift.loader.redshift_copy_options')
    @patch('records_mover.db.redshift.loader.ProcessingInstructions')
    @patch('records_mover.db.redshift.loader.RecordsLoadPlan')
    def test_can_load_this_format_true(self,
                                       mock_RecordsLoadPlan,
                                       mock_ProcessingInstructions,
                                       mock_redshift_copy_options):
        mock_processing_instructions = mock_ProcessingInstructions.return_value
        mock_source_records_format = Mock(name='source_records_format',
                                          spec=DelimitedRecordsFormat)
        mock_source_records_format.hints = {}

        mock_load_plan = mock_RecordsLoadPlan.return_value
        mock_load_plan.records_format = Mock(name='records_format',
                                                  spec=DelimitedRecordsFormat)
        mock_load_plan.records_format.hints = {}
        mock_load_plan.processing_instructions = mock_processing_instructions

        out = self.redshift_loader.can_load_this_format(mock_source_records_format)
        mock_ProcessingInstructions.assert_called_with()
        mock_RecordsLoadPlan.\
            assert_called_with(records_format=mock_source_records_format,
                               processing_instructions=mock_processing_instructions)
        mock_redshift_copy_options.\
            assert_called_with(set(),
                               mock_load_plan.records_format.hints,
                               mock_processing_instructions.fail_if_cant_handle_hint,
                               mock_processing_instructions.fail_if_row_invalid,
                               mock_processing_instructions.max_failure_rows)
        self.assertEqual(True, out)

    def test_known_supported_records_formats_for_load(self):
        out = self.redshift_loader.known_supported_records_formats_for_load()
        self.assertEqual(out, [
            DelimitedRecordsFormat(variant='bigquery',
                                   hints={
                                       'datetimeformat': 'YYYY-MM-DD HH:MI:SS',
                                       'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
                                   }),
            DelimitedRecordsFormat(variant='bigquery'),
            DelimitedRecordsFormat(variant='csv'),
            DelimitedRecordsFormat(variant='bluelabs', hints={'quoting': 'all'}),
            DelimitedRecordsFormat(variant='bluelabs'),
        ])
