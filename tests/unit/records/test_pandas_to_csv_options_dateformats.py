import unittest
# from records_mover.records.pandas import pandas_to_csv_options
# from records_mover.records.processing_instructions import ProcessingInstructions
# from records_mover.records.records_format import DelimitedRecordsFormat


class TestPandasToCsvOptionsDateformats(unittest.TestCase):
    pass
    # YYYY-MM-DD HH24:MI:SS is not valid for datetimeformattz
    #
    # def test_pandas_dateformat_YYYY_MM_DD_no_tz(self):
    #     expected = {
    #         'compression': 'gzip',
    #         'date_format': '%Y-%m-%d %H:%M:%S.%f',
    #         'doublequote': False,
    #         'encoding': 'UTF8',
    #         'escapechar': '\\',
    #         'header': False,
    #         'line_terminator': '\n',
    #         'quotechar': '"',
    #         'quoting': 3,
    #         'sep': ',',
    #     }
    #     processing_instructions = ProcessingInstructions()
    #     records_format =\
    #         DelimitedRecordsFormat(variant='bluelabs',
    #                                hints={
    #                                    'dateformat': 'YYYY-MM-DD',
    #                                    'datetimeformattz': 'YYYY-MM-DD HH24:MI:SS',
    #                                    'datetimeformat': 'YYYY-MM-DD HH24:MI:SS',
    #                                })
    #     unhandled_hints = set(records_format.hints)
    #     actual = pandas_to_csv_options(records_format, unhandled_hints, processing_instructions)
    #     self.assertEqual(expected, actual)
    #     self.assertFalse(unhandled_hints)

    # MM-DD-YYYY HH24:MI:SS datetimeformat not supported in spec
    #
    # def test_pandas_dateformat_MM_DD_YYYY_no_tz(self):
    #     expected = {
    #         'compression': 'gzip',
    #         'date_format': '%m-%d-%Y %H:%M:%S.%f',
    #         'doublequote': False,
    #         'encoding': 'UTF8',
    #         'escapechar': '\\',
    #         'header': False,
    #         'line_terminator': '\n',
    #         'quotechar': '"',
    #         'quoting': 3,
    #         'sep': ',',
    #     }
    #     processing_instructions = ProcessingInstructions()
    #     records_format =\
    #         DelimitedRecordsFormat(variant='bluelabs',
    #                                hints={
    #                                    'dateformat': 'MM-DD-YYYY',
    #                                    'datetimeformattz': 'MM-DD-YYYY HH24:MI:SS',
    #                                    'datetimeformat': 'MM-DD-YYYY HH24:MI:SS',
    #                                })
    #     unhandled_hints = set(records_format.hints)
    #     actual = pandas_to_csv_options(records_format, unhandled_hints, processing_instructions)
    #     self.assertEqual(expected, actual)
    #     self.assertFalse(unhandled_hints)

    # DD-MM format not yet fully supported - see
    # https://app.asana.com/0/1128138765527694/1173779659264666
    #
    # def test_pandas_dateformat_DD_MM_YYYY_no_tz(self):
    #     expected = {
    #         'compression': 'gzip',
    #         'date_format': '%d-%m-%Y %H:%M:%S.%f',
    #         'doublequote': False,
    #         'encoding': 'UTF8',
    #         'escapechar': '\\',
    #         'header': False,
    #         'line_terminator': '\n',
    #         'quotechar': '"',
    #         'quoting': 3,
    #         'sep': ',',
    #     }
    #     processing_instructions = ProcessingInstructions()
    #     records_format =\
    #         DelimitedRecordsFormat(variant='bluelabs',
    #                                hints={
    #                                    'dateformat': 'DD-MM-YYYY',
    #                                    'datetimeformattz': 'DD-MM-YYYY HH24:MI:SS',
    #                                    'datetimeformat': 'DD-MM-YYYY HH24:MI:SS',
    #                                })
    #     unhandled_hints = set(records_format.hints)
    #     actual = pandas_to_csv_options(records_format, unhandled_hints, processing_instructions)
    #     self.assertEqual(expected, actual)
    #     self.assertFalse(unhandled_hints)

    # MM/DD/YY HH24:MI:SS not supported
    #
    # def test_pandas_dateformat_MMsDDsYY_no_tz(self):
    #     expected = {
    #         'compression': 'gzip',
    #         'date_format': '%m/%d/%y %H:%M:%S.%f',
    #         'doublequote': False,
    #         'encoding': 'UTF8',
    #         'escapechar': '\\',
    #         'header': False,
    #         'line_terminator': '\n',
    #         'quotechar': '"',
    #         'quoting': 3,
    #         'sep': ',',
    #     }
    #     processing_instructions = ProcessingInstructions()
    #     records_format =\
    #         DelimitedRecordsFormat(variant='bluelabs',
    #                                hints={
    #                                    'dateformat': 'MM/DD/YY',
    #                                    'datetimeformattz': 'MM/DD/YY HH24:MI:SS',
    #                                    'datetimeformat': 'MM/DD/YY HH24:MI:SS',
    #                                })
    #     unhandled_hints = set(records_format.hints)
    #     actual = pandas_to_csv_options(records_format, unhandled_hints, processing_instructions)
    #     self.assertEqual(expected, actual)
    #     self.assertFalse(unhandled_hints)
