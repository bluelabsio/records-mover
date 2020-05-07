import unittest
from .format_hints import vertica_format_hints
from records_mover.records.pandas import pandas_to_csv_options
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.records.records_format import DelimitedRecordsFormat


class TestPandasToCsvOptionsVertica(unittest.TestCase):
    def test_pandas_to_csv_options_vertica(self):
        expected = {
            'date_format': '%Y-%m-%d %H:%M:%S.%f%z',
            'doublequote': False,
            'encoding': 'UTF8',
            'header': False,
            'line_terminator': '\x02',
            'quotechar': '"',
            'quoting': 3,
            'sep': '\x01',
        }
        processing_instructions = ProcessingInstructions()
        records_format = DelimitedRecordsFormat(hints=vertica_format_hints)
        unhandled_hints = set(records_format.hints)
        actual = pandas_to_csv_options(records_format, unhandled_hints, processing_instructions)
        self.assertEqual(expected, actual)
        self.assertFalse(unhandled_hints)
