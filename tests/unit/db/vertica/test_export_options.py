import unittest
from records_mover.db.vertica.records_export_options import vertica_export_options
from ...records.format_hints import csv_format_hints
from mock import Mock, call, patch
from records_mover.records.records_format import DelimitedRecordsFormat
from records_mover.records.hintutils import logger as driver_logger


class TestRecordsExportOptions(unittest.TestCase):
    maxDiff = None

    def test_vertica_export_options_csv(self):
        mock_unhandled_hints = Mock(name='unhandled_hints')
        mock_unload_plan = Mock(name='unload_plan')
        mock_hints = csv_format_hints.copy()
        mock_unload_plan.records_format = Mock(spec=DelimitedRecordsFormat)
        mock_unload_plan.records_format.hints = mock_hints
        mock_unload_plan.processing_instructions.fail_if_cant_handle_hint = False
        with patch.object(driver_logger, 'warning') as mock_warning:
            out = vertica_export_options(unhandled_hints=mock_unhandled_hints,
                                         unload_plan=mock_unload_plan)
            self.assertCountEqual(mock_warning.mock_calls,
                                  [
                                      call("Ignoring hint compression = 'GZIP'"),
                                      call("Ignoring hint quoting = 'minimal'"),
                                      call("Ignoring hint dateformat = 'MM/DD/YY'"),
                                      call("Ignoring hint datetimeformattz = 'MM/DD/YY HH24:MI'"),
                                      call("Ignoring hint datetimeformat = 'MM/DD/YY HH24:MI'"),
                                      call('Ignoring hint header-row = True')
                                  ])

            expected = {'to_charset': 'UTF8', 'delimiter': ',', 'record_terminator': '\n'}
            self.assertEqual(out, expected)
