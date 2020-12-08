import unittest
import pandas
import io
from typing import Dict
from typing_extensions import TypedDict
from records_mover.records.delimited.types import (
    HintDateFormat, HintDateTimeFormatTz, HintDateTimeFormat
)
from ..datetime_cases import (
    DATE_CASES, DATETIMETZ_CASES, DATETIME_CASES, TIMEONLY_CASES, create_sample,
    SAMPLE_YEAR, SAMPLE_MONTH, SAMPLE_DAY, SAMPLE_HOUR, SAMPLE_MINUTE, SAMPLE_SECOND
)
from records_mover.records.pandas.to_csv_options import pandas_to_csv_options
from records_mover.records.schema import RecordsSchema
from records_mover.records import DelimitedRecordsFormat, ProcessingInstructions


class TestToCsvOptions(unittest.TestCase):
    def test_dateformat(self) -> None:
        expectations = {
            'YYYY-MM-DD': '%Y-%m-%d %H:%M:%S.%f%z',
            'MM-DD-YY': '%m-%d-%y %H:%M:%S.%f%z',
            'MM-DD-YYYY': '%m-%d-%Y %H:%M:%S.%f%z',
            'DD-MM-YYYY': '%d-%m-%Y %H:%M:%S.%f%z',
            'MM/DD/YY': '%m/%d/%y %H:%M:%S.%f%z',
            'DD/MM/YY': '%d/%m/%y %H:%M:%S.%f%z',
            'DD-MM-YY': '%d-%m-%y %H:%M:%S.%f%z',
        }
        for dateformat in DATE_CASES:
            records_format = DelimitedRecordsFormat(hints={
                # Pandas doesn't consider dateformats to be separate
                # from datetime/datetimetz formats, so they need to be
                # consistent
                'dateformat': dateformat,
                'datetimeformat': f"{dateformat} HH:MI:SS",
                'datetimeformattz': f"{dateformat} HH:MI:SSOF",
                'compression': None,
            })
            unhandled_hints = set(records_format.hints)
            processing_instructions = ProcessingInstructions()
            options = pandas_to_csv_options(records_format,
                                            unhandled_hints,
                                            processing_instructions)
            self.assertEqual(options['date_format'], expectations[dateformat])

            fileobj = io.StringIO(create_sample(dateformat))
            df = pandas.DataFrame(data={'date': [pandas.Timestamp(day=2,
                                                                  month=1,
                                                                  year=1983)]},
                                  columns=['date'])
            df.to_csv(path_or_buf=fileobj,
                      index=False,
                      **options)
            output = fileobj.getvalue()
            # In reality this isn't used raw, as Pandas doesn't really
            # try to handle lone dates or times.  Instead, we use
            # prep_for_csv() to preconvert these Serieses into strings.
            sample = create_sample(dateformat)
            self.assertEqual(output, f"{sample} 00:00:00.000000\n")

    def test_datetimeformattz(self) -> None:
        raise

    def test_datetimeformat(self) -> None:
        raise

    def test_timeonlyformat(self) -> None:
        raise
