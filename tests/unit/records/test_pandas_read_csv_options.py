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
            'compression': 'gzip',
            'delimiter': ',',
            'doublequote': False,
            'encoding': 'UTF8',
            'engine': 'python',
            'error_bad_lines': True,
            'escapechar': '\\',
            'header': None,
            'prefix': 'untitled_',
            'quotechar': '"',
            'quoting': 3,
            'warn_bad_lines': True,
            'parse_dates': [1, 2, 3, 4],
        }
        processing_instructions = ProcessingInstructions()
        records_format = DelimitedRecordsFormat(hints=bluelabs_format_hints)
        unhandled_hints = set()
        actual = pandas_read_csv_options(records_format,
                                         self.records_schema,
                                         unhandled_hints,
                                         processing_instructions)
        self.assertEqual(expected, actual)
        self.assertFalse(unhandled_hints)

    def test_pandas_read_csv_options_csv(self):
        expected = {
            'compression': 'gzip',
            'delimiter': ',',
            'doublequote': True,
            'encoding': 'UTF8',
            'engine': 'python',
            'error_bad_lines': True,
            'header': 0,
            'prefix': 'untitled_',
            'quotechar': '"',
            'quoting': 0,
            'warn_bad_lines': True
        }
        processing_instructions = ProcessingInstructions()
        records_format = DelimitedRecordsFormat(hints=csv_format_hints)
        unhandled_hints = set()
        actual = pandas_read_csv_options(records_format,
                                         self.records_schema,
                                         unhandled_hints,
                                         processing_instructions)
        self.assertEqual(expected, actual)
        self.assertFalse(unhandled_hints)

    def test_pandas_read_csv_options_vertica(self):
        self.maxDiff = None
        expected = {
            'compression': None,
            'delimiter': '\x01',
            'doublequote': False,
            'engine': 'c',
            'error_bad_lines': True,
            'header': None,
            'lineterminator': '\x02',
            'prefix': 'untitled_',
            'quotechar': '"',
            'quoting': 3,
            'warn_bad_lines': True
        }
        processing_instructions = ProcessingInstructions()
        records_format = DelimitedRecordsFormat(hints=vertica_format_hints)
        unhandled_hints = set()
        actual = pandas_read_csv_options(records_format,
                                         self.records_schema,
                                         unhandled_hints,
                                         processing_instructions)
        self.assertEqual(expected, actual)
        self.assertFalse(unhandled_hints)
