import unittest
import pandas
import io
from typing import Dict
from typing_extensions import TypedDict
from records_mover.records.delimited.types import HintDateFormat
from ..datetime_cases import (
    DATE_CASES, create_sample, SAMPLE_YEAR, SAMPLE_MONTH, SAMPLE_DAY
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
