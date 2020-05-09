from records_mover.records.records_format import DelimitedRecordsFormat
from records_mover.records.load_plan import RecordsLoadPlan
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.db.vertica.records_import_options import vertica_import_options
from ...records.format_hints import christmas_tree_format_1_hints
from records_mover.records.hintutils import logger as driver_logger
import unittest
from mock import call, patch


class TestVerticaImportOptions(unittest.TestCase):
    maxDiff = None

    def test_bluelabs_format(self):
        bluelabs_format = DelimitedRecordsFormat(variant='bluelabs')
        load_plan = RecordsLoadPlan(processing_instructions=ProcessingInstructions(),
                                    records_format=bluelabs_format)
        unhandled_hints = set(load_plan.records_format.hints.keys())
        options = vertica_import_options(unhandled_hints, load_plan)
        expected_options = {
            'abort_on_error': True,
            'delimiter': ',',
            'enclosed_by': None,
            'enforcelength': True,
            'error_tolerance': False,
            'escape_as': '\\',
            'gzip': True,
            'load_method': 'AUTO',
            'no_commit': False,
            'null_as': None,
            'record_terminator': '\n',
            'rejectmax': 1,
            'skip': 0,
            'trailing_nullcols': False,
        }
        self.assertDictEqual(options, expected_options)
        self.assertEqual(unhandled_hints, set())

    def test_vertica_format(self):
        vertica_format = DelimitedRecordsFormat(variant='vertica')
        load_plan = RecordsLoadPlan(processing_instructions=ProcessingInstructions(),
                                    records_format=vertica_format)
        unhandled_hints = set(load_plan.records_format.hints.keys())
        options = vertica_import_options(unhandled_hints, load_plan)
        expected_options = {
            'abort_on_error': True,
            'delimiter': '\x01',
            'enclosed_by': None,
            'enforcelength': True,
            'error_tolerance': False,
            'escape_as': None,
            'gzip': False,
            'load_method': 'AUTO',
            'no_commit': False,
            'null_as': None,
            'record_terminator': '\x02',
            'rejectmax': 1,
            'skip': 0,
            'trailing_nullcols': False,
        }
        self.assertDictEqual(options, expected_options)
        self.assertEqual(unhandled_hints, set())

    def test_vertica_format_permissive(self):
        vertica_format = DelimitedRecordsFormat(variant='vertica')
        processing_instructions = ProcessingInstructions(fail_if_row_invalid=False)
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=vertica_format)
        unhandled_hints = set(load_plan.records_format.hints.keys())
        options = vertica_import_options(unhandled_hints, load_plan)
        expected_options = {
            'abort_on_error': False,
            'delimiter': '\x01',
            'enclosed_by': None,
            'enforcelength': False,
            'error_tolerance': True,
            'escape_as': None,
            'gzip': False,
            'load_method': 'AUTO',
            'no_commit': False,
            'null_as': None,
            'record_terminator': '\x02',
            'rejectmax': None,
            'skip': 0,
            'trailing_nullcols': True,
        }
        self.assertDictEqual(options, expected_options)
        self.assertEqual(unhandled_hints, set())

    def test_christmas_tree_format_1_permissive(self):
        vertica_format = DelimitedRecordsFormat(variant='dumb', hints=christmas_tree_format_1_hints)
        processing_instructions = ProcessingInstructions(fail_if_cant_handle_hint=False)
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=vertica_format)
        unhandled_hints = set(load_plan.records_format.hints.keys())
        with patch.object(driver_logger, 'warning') as mock_warning:
            options = vertica_import_options(unhandled_hints, load_plan)
        expected_options = {
            'abort_on_error': True,
            'delimiter': '\x01',
            'enforcelength': True,
            'error_tolerance': False,
            'escape_as': '\\',
            'load_method': 'AUTO',
            'no_commit': False,
            'null_as': None,
            'record_terminator': '\x02',
            'rejectmax': 1,
            'skip': 1,
            'trailing_nullcols': False,
        }
        self.assertDictEqual(options, expected_options)
        self.assertCountEqual(mock_warning.mock_calls,
                              [call("Ignoring hint compression = 'LZO'"),
                               call("Ignoring hint dateformat = None"),
                               call("Ignoring hint datetimeformat = None"),
                               call("Ignoring hint quoting = 'nonnumeric'")])
        self.assertEqual(unhandled_hints, set())

    def test_weird_timeonlyformat(self):
        vertica_format = DelimitedRecordsFormat(variant='dumb', hints={
            'timeonlyformat': 'something else'
        })
        processing_instructions = ProcessingInstructions()
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=vertica_format)
        unhandled_hints = set(load_plan.records_format.hints.keys())
        with self.assertRaisesRegexp(NotImplementedError,
                                     "Implement hint timeonlyformat='something else' or try again "
                                     "with fail_if_cant_handle_hint=False"):
            vertica_import_options(unhandled_hints, load_plan)

    def test_quote_all_with_doublequote(self):
        vertica_format = DelimitedRecordsFormat(variant='csv', hints={
            'quoting': 'all'
        })
        processing_instructions = ProcessingInstructions()
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=vertica_format)
        unhandled_hints = set(load_plan.records_format.hints.keys())
        with self.assertRaisesRegexp(NotImplementedError,
                                     r"Implement hint doublequote=True or try again with "
                                     "fail_if_cant_handle_hint=False"):
            vertica_import_options(unhandled_hints, load_plan)

    def test_quote_all_without_doublequote(self):
        vertica_format = DelimitedRecordsFormat(variant='csv', hints={
            'quoting': 'all',
            'doublequote': False,
            # Vertica doesn't support exporting CSV variant style dates by
            # default, so let's pick some it can for purposes of this
            # test:
            'dateformat': 'YYYY-MM-DD',
            'datetimeformat': 'YYYY-MM-DD HH:MI:SS',
            'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
        })
        processing_instructions = ProcessingInstructions()
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=vertica_format)
        unhandled_hints = set(load_plan.records_format.hints.keys())
        out = vertica_import_options(unhandled_hints, load_plan)
        self.assertEqual(out['enclosed_by'], '"')
