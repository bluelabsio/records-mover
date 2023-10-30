from records_mover.db.vertica.unloader import VerticaUnloader
from records_mover.records.records_format import DelimitedRecordsFormat
import unittest
from mock import patch, Mock, ANY


class TestVerticaUnloader(unittest.TestCase):
    maxDiff = None

    @patch('records_mover.db.vertica.unloader.vertica_export_options')
    def test_can_unload_format_true(self, mock_vertica_export_options):
        mock_db = Mock(name='db')
        mock_source_records_format = Mock(name='source_records_format', spec=DelimitedRecordsFormat)
        mock_s3_temp_base_loc = Mock(name='s3_temp_base_loc')
        vertica_unloader = VerticaUnloader(db=None, s3_temp_base_loc=mock_s3_temp_base_loc,
                                           db_conn=mock_db)
        mock_source_records_format.hints = {}
        out = vertica_unloader.can_unload_format(mock_source_records_format)
        mock_vertica_export_options.assert_called_with(set(), ANY)
        self.assertEqual(True, out)

    @patch('records_mover.db.vertica.unloader.vertica_export_options')
    def test_can_unload_format_false(self, mock_vertica_export_options):
        mock_db = Mock(name='db')
        mock_source_records_format = Mock(name='source_records_format', spec=DelimitedRecordsFormat)
        mock_s3_temp_base_loc = Mock(name='s3_temp_base_loc')
        vertica_unloader = VerticaUnloader(db=None, s3_temp_base_loc=mock_s3_temp_base_loc,
                                           db_conn=mock_db)
        mock_source_records_format.hints = {}
        mock_vertica_export_options.side_effect = NotImplementedError
        out = vertica_unloader.can_unload_format(mock_source_records_format)
        mock_vertica_export_options.assert_called_with(set(), ANY)
        self.assertEqual(False, out)

    def test_known_supported_records_formats_for_unload(self):
        mock_db = Mock(name='db')
        mock_source_records_format = Mock(name='source_records_format', spec=DelimitedRecordsFormat)
        mock_s3_temp_base_loc = Mock(name='s3_temp_base_loc')
        vertica_unloader = VerticaUnloader(db=None, s3_temp_base_loc=mock_s3_temp_base_loc,
                                           db_conn=mock_db)
        mock_source_records_format.hints = {}
        out = vertica_unloader.known_supported_records_formats_for_unload()
        self.assertEqual(out, [DelimitedRecordsFormat(variant='vertica')])
