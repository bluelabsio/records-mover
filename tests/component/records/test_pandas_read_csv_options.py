import unittest
from .format_hints import bluelabs_format_hints, csv_format_hints, vertica_format_hints
from records_mover.records.pandas import pandas_read_csv_options
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.records.records_format import DelimitedRecordsFormat
from records_mover.records.schema import RecordsSchema


class TestPandasReadCsvOptions(unittest.TestCase):
    def setUp(self):
        self.records_schema = RecordsSchema.from_data({
            'schema': 'bltypes/v1',
            'fields': {
                "date": {
                    "type": "date",
                    "index": 1,
                },
                "time": {
                    "type": "time",
                    "index": 2,
                },
                "timestamp": {
                    "type": "datetime",
                    "index": 3,
                },
                "timestamptz": {
                    "type": "datetimetz",
                    "index": 4,
                }
            }
        })

    def test_pandas_read_csv_options_bluelabs(self):
        expected = {
            'dayfirst': False,
            'compression': 'gzip',
            'delimiter': ',',
            'doublequote': False,
            'encoding': 'UTF8',
            'engine': 'python',
            'on_bad_lines': 'error',
            'escapechar': '\\',
            'header': None,
            'quotechar': '"',
            'quoting': 3,
            'parse_dates': [0, 1, 2, 3],
        }
        processing_instructions = ProcessingInstructions()
        records_format = DelimitedRecordsFormat(hints=bluelabs_format_hints)
        unhandled_hints = set(records_format.hints)
        actual = pandas_read_csv_options(records_format,
                                         self.records_schema,
                                         unhandled_hints,
                                         processing_instructions)
        self.assertEqual(expected, actual)
        self.assertFalse(unhandled_hints)

    # MM-DD not yet fully supported - see https://github.com/bluelabsio/records-mover/issues/75
    #
    # def test_pandas_read_csv_options_bleulabs(self):
    #     expected = {
    #         'dayfirst': True,
    #         'compression': 'gzip',
    #         'delimiter': ',',
    #         'doublequote': False,
    #         'encoding': 'UTF8',
    #         'engine': 'python',
    #         'on_bad_lines': True,
    #         'escapechar': '\\',
    #         'header': None,
    #         'quotechar': '"',
    #         'quoting': 3,
    #         'warn_bad_lines': True,
    #         'parse_dates': [0, 1, 2, 3],
    #     }
    #     processing_instructions = ProcessingInstructions()
    #     hints = bluelabs_format_hints.copy()
    #     hints.update({
    #         'dateformat': 'DD-MM-YYYY',
    #         'datetimeformattz': 'DD-MM-YYYY HH24:MIOF',
    #         'datetimeformat': 'DD-MM-YYYY HH24:MI',
    #     })
    #     records_format = DelimitedRecordsFormat(hints=hints)
    #     unhandled_hints = set(records_format.hints)
    #     actual = pandas_read_csv_options(records_format,
    #                                      self.records_schema,
    #                                      unhandled_hints,
    #                                      processing_instructions)
    #     self.assertEqual(expected, actual)
    #     self.assertFalse(unhandled_hints)

    def test_pandas_read_csv_options_inconsistent_date_format(self):
        processing_instructions = ProcessingInstructions()
        hints = bluelabs_format_hints.copy()
        hints.update({
            'dateformat': 'DD-MM-YYYY',
            'datetimeformattz': 'MM-DD-YYYY HH24:MIOF',
            'datetimeformat': 'DD-MM-YYYY HH24:MI',
        })
        records_format = DelimitedRecordsFormat(hints=hints)
        unhandled_hints = set(records_format.hints)
        with self.assertRaises(NotImplementedError):
            pandas_read_csv_options(records_format,
                                    self.records_schema,
                                    unhandled_hints,
                                    processing_instructions)

    def test_pandas_read_csv_options_csv(self):
        expected = {
            'dayfirst': False,
            'compression': 'gzip',
            'delimiter': ',',
            'doublequote': True,
            'encoding': 'UTF8',
            'engine': 'python',
            'on_bad_lines': 'error',
            'header': 0,
            'quotechar': '"',
            'quoting': 0,
            'parse_dates': [0, 1, 2, 3],
        }
        processing_instructions = ProcessingInstructions()
        records_format = DelimitedRecordsFormat(hints=csv_format_hints)
        unhandled_hints = set(records_format.hints)
        actual = pandas_read_csv_options(records_format,
                                         self.records_schema,
                                         unhandled_hints,
                                         processing_instructions)
        self.assertEqual(expected, actual)
        self.assertFalse(unhandled_hints)

    def test_pandas_read_csv_options_vertica(self):
        self.maxDiff = None
        expected = {
            'dayfirst': False,
            'compression': None,
            'delimiter': '\x01',
            'doublequote': False,
            'engine': 'c',
            'on_bad_lines': 'error',
            'header': None,
            'lineterminator': '\x02',
            'quotechar': '"',
            'quoting': 3,
            'parse_dates': [0, 1, 2, 3],
        }
        processing_instructions = ProcessingInstructions()
        records_format = DelimitedRecordsFormat(hints=vertica_format_hints)
        unhandled_hints = set(records_format.hints)
        actual = pandas_read_csv_options(records_format,
                                         self.records_schema,
                                         unhandled_hints,
                                         processing_instructions)
        self.assertEqual(expected, actual)
        self.assertFalse(unhandled_hints)
