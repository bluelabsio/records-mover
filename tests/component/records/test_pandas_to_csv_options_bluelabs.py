import unittest
from .format_hints import bluelabs_format_hints
from records_mover.records.pandas import pandas_to_csv_options
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.records.records_format import DelimitedRecordsFormat
from packaging import version
import pandas as pd


class TestPandasToCsvOptionsBlueLabs(unittest.TestCase):
    def test_pandas_to_csv_options_bluelabs(self):
        if version.parse(pd.__version__) >= version.parse('1.5.0'):
            expected = {
                'compression': 'gzip',
                'date_format': '%Y-%m-%d %H:%M:%S.%f%z',
                'doublequote': False,
                'encoding': 'UTF8',
                'escapechar': '\\',
                'header': False,
                'lineterminator': '\n',
                'quotechar': '"',
                'quoting': 3,
                'sep': ',',
            }
        else:
            expected = {
                'compression': 'gzip',
                'date_format': '%Y-%m-%d %H:%M:%S.%f%z',
                'doublequote': False,
                'encoding': 'UTF8',
                'escapechar': '\\',
                'header': False,
                'line_terminator': '\n',
                'quotechar': '"',
                'quoting': 3,
                'sep': ',',
            }
        processing_instructions = ProcessingInstructions()
        records_format = DelimitedRecordsFormat(hints=bluelabs_format_hints)
        unhandled_hints = set(records_format.hints)
        actual = pandas_to_csv_options(records_format, unhandled_hints, processing_instructions)
        self.assertEqual(expected, actual)
        self.assertFalse(unhandled_hints)
