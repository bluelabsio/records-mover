from mock import patch, call
import unittest
from .format_hints import (christmas_tree_format_1_hints, christmas_tree_format_2_hints,
                           christmas_tree_format_3_hints, christmas_tree_format_4_hints)
from records_mover.records.hintutils import logger as driver_logger
from records_mover.records.pandas import pandas_to_csv_options
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.records.records_format import DelimitedRecordsFormat


class TestPandasToCsvOptionsChristmasTree(unittest.TestCase):
    def test_pandas_to_csv_options_christmas_tree_format_1(self):
        expected = {
            'date_format': '%Y-%m-%d %H:%M:%S.%f%z',
            'doublequote': False,
            'encoding': 'UTF8',
            'escapechar': '\\',
            'header': True,
            'line_terminator': '\x02',
            'quotechar': '"',
            'quoting': 2,
            'sep': '\x01'
        }
        processing_instructions =\
            ProcessingInstructions(fail_if_cant_handle_hint=False)
        records_format = DelimitedRecordsFormat(hints=christmas_tree_format_1_hints)
        unhandled_hints = set(records_format.hints)
        with patch.object(driver_logger, 'warning') as mock_warning:
            actual = pandas_to_csv_options(records_format, unhandled_hints, processing_instructions)
            self.assertEqual(expected, actual)
            self.assertCountEqual(mock_warning.mock_calls,
                                  [call("Ignoring hint datetimeformat = None"),
                                   call("Ignoring hint datetimeformattz = 'YYYY-MM-DD HH:MI:SSOF'"),
                                   call("Ignoring hint compression = 'LZO'")])
            self.assertFalse(unhandled_hints)

    def test_pandas_to_csv_options_christmas_tree_format_2(self):
        expected = {
            'compression': 'bz2',
            'date_format': '%m-%d-%Y %H:%M:%S.%f%z',
            'doublequote': True,
            'encoding': 'UTF8',
            'escapechar': '@',
            'header': False,
            'line_terminator': '\x02',
            'quotechar': '"',
            'quoting': 1,
            'sep': '\x01',
        }
        processing_instructions =\
            ProcessingInstructions(fail_if_cant_handle_hint=False)
        records_format = DelimitedRecordsFormat(hints=christmas_tree_format_2_hints)
        unhandled_hints = set(records_format.hints)
        with patch.object(driver_logger, 'warning') as mock_warning:
            actual = pandas_to_csv_options(records_format, unhandled_hints, processing_instructions)
            self.assertEqual(expected, actual)
            self.\
                assertCountEqual(mock_warning.mock_calls,
                                 [call("Ignoring hint datetimeformat = None"),
                                  call("Ignoring hint datetimeformattz = 'HH:MI:SSOF YYYY-MM-DD'")])
            self.assertFalse(unhandled_hints)

    def test_pandas_to_csv_options_christmas_tree_format_3(self):
        expected = {
            'compression': 'bz2',
            'date_format': '%d-%m-%Y %H:%M:%S.%f%z',
            'doublequote': True,
            'encoding': 'UTF8',
            'escapechar': '@',
            'header': False,
            'line_terminator': '\x02',
            'quotechar': '"',
            'sep': '\x01',
        }
        processing_instructions =\
            ProcessingInstructions(fail_if_cant_handle_hint=False)
        records_format = DelimitedRecordsFormat(hints=christmas_tree_format_3_hints)
        unhandled_hints = set(records_format.hints)
        with patch.object(driver_logger, 'warning') as mock_warning:
            actual = pandas_to_csv_options(records_format, unhandled_hints, processing_instructions)
            self.assertEqual(expected, actual)
            self.assertCountEqual(mock_warning.mock_calls,
                                  [call("Ignoring hint datetimeformat = None"),
                                   call("Ignoring hint datetimeformattz = 'HH:MI:SSOF YYYY-MM-DD'"),
                                   call("Ignoring hint quoting = "
                                        "'some_future_option_not_supported_now'")])
            self.assertFalse(unhandled_hints)

    def test_pandas_to_csv_options_christmas_tree_format_4(self):
        expected = {
            'compression': 'bz2',
            'doublequote': True,
            'encoding': 'UTF8',
            'escapechar': '@',
            'header': False,
            'line_terminator': '\x02',
            'quotechar': '"',
            'sep': '\x01',
        }
        processing_instructions =\
            ProcessingInstructions(fail_if_cant_handle_hint=False)
        records_format = DelimitedRecordsFormat(hints=christmas_tree_format_4_hints)
        unhandled_hints = set(records_format.hints)
        with patch.object(driver_logger, 'warning') as mock_warning:
            actual = pandas_to_csv_options(records_format, unhandled_hints, processing_instructions)
            self.assertEqual(expected, actual)
            self.assertCountEqual(mock_warning.mock_calls,
                                  [call("Ignoring hint datetimeformat = None"),
                                   call("Ignoring hint dateformat = "
                                        "'totally_bogus_just_made_this_up'"),
                                   call("Ignoring hint datetimeformattz = 'HH:MI:SSOF YYYY-MM-DD'"),
                                   call("Ignoring hint quoting = "
                                        "'some_future_option_not_supported_now'")])
            self.assertFalse(unhandled_hints)
