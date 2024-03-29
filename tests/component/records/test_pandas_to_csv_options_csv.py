import unittest
from .format_hints import csv_format_hints
from records_mover.records.pandas import pandas_to_csv_options
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.records.records_format import DelimitedRecordsFormat
from packaging import version
import pandas as pd


class TestPandasToCsvOptionsCsv(unittest.TestCase):
    def test_pandas_to_csv_options_csv(self):
        if version.parse(pd.__version__) >= version.parse('1.5.0'):
            expected = {
                'compression': 'gzip',
                'date_format': '%m/%d/%y %H:%M',
                'doublequote': True,
                'encoding': 'UTF8',
                'header': True,
                'lineterminator': '\n',
                'quotechar': '"',
                'quoting': 0,
                'sep': ','
            }
        else:
            expected = {
                'compression': 'gzip',
                'date_format': '%m/%d/%y %H:%M',
                'doublequote': True,
                'encoding': 'UTF8',
                'header': True,
                'line_terminator': '\n',
                'quotechar': '"',
                'quoting': 0,
                'sep': ','
            }
        processing_instructions =\
            ProcessingInstructions(fail_if_cant_handle_hint=True)
        records_format = DelimitedRecordsFormat(hints=csv_format_hints)
        unhandled_hints = set(records_format.hints)
        actual = pandas_to_csv_options(records_format, unhandled_hints, processing_instructions)
        self.assertEqual(expected, actual)
        self.assertFalse(unhandled_hints)
