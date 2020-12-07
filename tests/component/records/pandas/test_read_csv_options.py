import unittest
import pandas
import io
from typing import Dict
from typing_extensions import TypedDict
from records_mover.records.delimited.types import (
    HintDateFormat, HintDateTimeFormatTz, HintDateTimeFormat, HintTimeOnlyFormat
)
from ..datetime_cases import (
    DATE_CASES, DATETIMETZ_CASES, DATETIME_CASES, TIMEONLY_CASES,
    create_sample,
    SAMPLE_YEAR, SAMPLE_MONTH, SAMPLE_DAY, SAMPLE_HOUR, SAMPLE_HOUR_12H,
    SAMPLE_MINUTE, SAMPLE_SECOND
)
from records_mover.records.pandas.read_csv_options import pandas_read_csv_options
from records_mover.records.schema import RecordsSchema
from records_mover.records import DelimitedRecordsFormat, ProcessingInstructions


class TestReadCsvOptions(unittest.TestCase):
    def test_pandas_read_csv_options_bzip(self):
        records_format = DelimitedRecordsFormat(hints={
            'compression': 'BZIP'
        })
        records_schema = RecordsSchema.from_data({
            'schema': 'bltypes/v1'
        })
        unhandled_hints = set(records_format.hints)
        processing_instructions = ProcessingInstructions()
        expectations = {
            'compression': 'bz2'
        }
        out = pandas_read_csv_options(records_format,
                                      records_schema,
                                      unhandled_hints,
                                      processing_instructions)
        self.assertTrue(all(item in out.items() for item in expectations.items()))

    def test_dateformat(self) -> None:
        class DateFormatExpectations(TypedDict):
            # Use the datetimeformat/datetimeformattz which is
            # compatible, as pandas doesn't let you configure those
            # separately
            dayfirst: bool

        testcases: Dict[HintDateFormat, DateFormatExpectations] = {
            'YYYY-MM-DD': {
                'dayfirst': False,
            },
            'MM-DD-YYYY': {
                'dayfirst': False,
            },
            'DD-MM-YYYY': {
                'dayfirst': True,
            },
            'MM/DD/YY': {
                'dayfirst': False,
            },
            'DD/MM/YY': {
                'dayfirst': True,
            },
            'DD-MM-YY': {
                'dayfirst': True,
            },
        }
        for dateformat in DATE_CASES:
            records_format = DelimitedRecordsFormat(hints={
                'dateformat': dateformat,
                'datetimeformat': f"{dateformat} HH:MI:SS",
                'datetimeformattz': f"{dateformat} HH:MI:SSOF",
                'compression': None,
            })
            records_schema = RecordsSchema.from_data({
                'schema': 'bltypes/v1',
                'fields': {
                    'first': {
                        'type': 'date'
                    }
                },
            })
            unhandled_hints = set(records_format.hints)
            processing_instructions = ProcessingInstructions()
            expectations = testcases[dateformat]
            try:
                options = pandas_read_csv_options(records_format,
                                                  records_schema,
                                                  unhandled_hints,
                                                  processing_instructions)
            except NotImplementedError:
                self.fail(f'Could not handle combination for {dateformat}')
            self.assertTrue(all(item in options.items() for item in expectations.items()))
            fileobj = io.StringIO(create_sample(dateformat))
            df = pandas.read_csv(filepath_or_buffer=fileobj,
                                 **options)
            timestamp = df['untitled_0'][0]
            self.assertEqual(timestamp.year, SAMPLE_YEAR)
            self.assertEqual(timestamp.month, SAMPLE_MONTH)
            self.assertEqual(timestamp.day, SAMPLE_DAY)

    def test_datetimeformattz(self) -> None:
        class DateTimeFormatTzExpectations(TypedDict):
            # Use the datetimeformat/datetimeformattz which is
            # compatible, as pandas doesn't let you configure those
            # separately
            dayfirst: bool

        testcases: Dict[HintDateTimeFormatTz, DateTimeFormatTzExpectations] = {
            'YYYY-MM-DD HH:MI:SSOF': {
                'dayfirst': False,
            },
            'YYYY-MM-DD HH:MI:SS': {
                'dayfirst': False,
            },
            'YYYY-MM-DD HH24:MI:SSOF': {
                'dayfirst': False,
            },
            'MM/DD/YY HH24:MI': {
                'dayfirst': False,
            },
        }
        for datetimeformattz in DATETIMETZ_CASES:
            records_format = DelimitedRecordsFormat(hints={
                'datetimeformattz': datetimeformattz,
                'compression': None,
            })
            records_schema = RecordsSchema.from_data({
                'schema': 'bltypes/v1',
                'fields': {
                    'first': {
                        'type': 'datetimetz'
                    }
                },
            })
            unhandled_hints = set(records_format.hints)
            processing_instructions = ProcessingInstructions()
            expectations = testcases[datetimeformattz]
            try:
                options = pandas_read_csv_options(records_format,
                                                  records_schema,
                                                  unhandled_hints,
                                                  processing_instructions)
            except NotImplementedError:
                self.fail(f'Could not handle combination for {datetimeformattz}')
            self.assertEqual(options['parse_dates'], [0])
            self.assertTrue(all(item in options.items() for item in expectations.items()))
            datetimetz = create_sample(datetimeformattz)
            fileobj = io.StringIO(datetimetz)
            df = pandas.read_csv(filepath_or_buffer=fileobj,
                                 **options)
            timestamp = df['untitled_0'][0]
            self.assertIsInstance(timestamp, pandas.Timestamp,
                                  f"Pandas did not parse {datetimetz} as a timestamp object")
            self.assertEqual(timestamp.year, SAMPLE_YEAR)
            self.assertEqual(timestamp.month, SAMPLE_MONTH)
            self.assertEqual(timestamp.day, SAMPLE_DAY)
            self.assertEqual(timestamp.hour, SAMPLE_HOUR)
            self.assertEqual(timestamp.minute, SAMPLE_MINUTE)
            if 'SS' in datetimeformattz:
                self.assertEqual(timestamp.second, SAMPLE_SECOND)
            else:
                self.assertEqual(timestamp.second, 0)

    def test_datetimeformat(self) -> None:
        class DateTimeFormatExpectations(TypedDict):
            # Use the datetimeformat/datetimeformattz which is
            # compatible, as pandas doesn't let you configure those
            # separately
            dayfirst: bool

        testcases: Dict[HintDateTimeFormat, DateTimeFormatExpectations] = {
            'YYYY-MM-DD HH:MI:SS': {
                'dayfirst': False,
            },
            'YYYY-MM-DD HH24:MI:SS': {
                'dayfirst': False,
            },
            'MM/DD/YY HH24:MI': {
                'dayfirst': False,
            },
            'YYYY-MM-DD HH12:MI AM': {
                'dayfirst': False,
            }
        }
        for datetimeformat in DATETIME_CASES:
            records_format = DelimitedRecordsFormat(hints={
                'datetimeformat': datetimeformat,
                'compression': None,
            })
            records_schema = RecordsSchema.from_data({
                'schema': 'bltypes/v1',
                'fields': {
                    'first': {
                        'type': 'datetime'
                    }
                },
            })
            unhandled_hints = set(records_format.hints)
            processing_instructions = ProcessingInstructions()
            expectations = testcases[datetimeformat]
            try:
                options = pandas_read_csv_options(records_format,
                                                  records_schema,
                                                  unhandled_hints,
                                                  processing_instructions)
            except NotImplementedError:
                self.fail(f'Could not handle combination for {datetimeformat}')
            self.assertEqual(options['parse_dates'], [0])
            self.assertTrue(all(item in options.items() for item in expectations.items()))
            datetimetz = create_sample(datetimeformat)
            fileobj = io.StringIO(datetimetz)
            df = pandas.read_csv(filepath_or_buffer=fileobj,
                                 **options)
            timestamp = df['untitled_0'][0]
            self.assertIsInstance(timestamp, pandas.Timestamp,
                                  f"Pandas did not parse {datetimetz} as a timestamp object")
            self.assertEqual(timestamp.year, SAMPLE_YEAR)
            self.assertEqual(timestamp.month, SAMPLE_MONTH)
            self.assertEqual(timestamp.day, SAMPLE_DAY)
            self.assertEqual(timestamp.hour, SAMPLE_HOUR)
            self.assertEqual(timestamp.minute, SAMPLE_MINUTE)
            if 'SS' in datetimeformat:
                self.assertEqual(timestamp.second, SAMPLE_SECOND)
            else:
                self.assertEqual(timestamp.second, 0)

    def test_timeonlyformat(self) -> None:
        for timeonlyformat in TIMEONLY_CASES:
            records_format = DelimitedRecordsFormat(hints={
                'timeonlyformat': timeonlyformat,
                'compression': None,
            })
            records_schema = RecordsSchema.from_data({
                'schema': 'bltypes/v1',
                'fields': {
                    'first': {
                        'type': 'time'
                    }
                },
            })
            unhandled_hints = set(records_format.hints)
            processing_instructions = ProcessingInstructions()
            try:
                options = pandas_read_csv_options(records_format,
                                                  records_schema,
                                                  unhandled_hints,
                                                  processing_instructions)
            except NotImplementedError:
                self.fail(f'Could not handle combination for {timeonlyformat}')
            self.assertEqual(options['parse_dates'], [0])
            timeonly = create_sample(timeonlyformat)
            fileobj = io.StringIO(timeonly)
            df = pandas.read_csv(filepath_or_buffer=fileobj,
                                 **options)
            timestamp = df['untitled_0'][0]
            self.assertIsInstance(timestamp, pandas.Timestamp,
                                  f"Pandas did not parse {timeonly} as a timestamp object")
            self.assertEqual(timestamp.hour, SAMPLE_HOUR)
            self.assertEqual(timestamp.minute, SAMPLE_MINUTE)
            if 'SS' in timeonlyformat:
                self.assertEqual(timestamp.second, SAMPLE_SECOND)
            else:
                self.assertEqual(timestamp.second, 0,
                                 timeonly)
