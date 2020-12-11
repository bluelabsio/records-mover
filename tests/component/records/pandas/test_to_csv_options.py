import unittest
import pandas
import io
from typing import Set
from ..datetime_cases import (
    DATE_CASES, DATETIMETZ_CASES, DATETIME_CASES, TIMEONLY_CASES, create_sample,
    SAMPLE_YEAR, SAMPLE_MONTH, SAMPLE_DAY, SAMPLE_HOUR, SAMPLE_MINUTE, SAMPLE_SECOND
)
from records_mover.records.pandas.to_csv_options import pandas_to_csv_options
from records_mover.records import DelimitedRecordsFormat, ProcessingInstructions


class TestToCsvOptions(unittest.TestCase):
    def test_datetimeformat(self) -> None:
        known_failures: Set[str] = set()
        expectations = {
            'YYYY-MM-DD HH:MI:SSOF': '%Y-%m-%d %H:%M:%S.%f%z',
            'YYYY-MM-DD HH:MI:SS': '%Y-%m-%d %H:%M:%S.%f',
            'YYYY-MM-DD HH24:MI:SSOF': '%Y-%m-%d %H:%M:%S.%f%z',
            'MM/DD/YY HH24:MI': '%m/%d/%y %H:%M',
        }
        compatible_dateformat = {
            'YYYY-MM-DD HH:MI:SSOF': 'YYYY-MM-DD',
            'YYYY-MM-DD HH:MI:SS': 'YYYY-MM-DD',
            'YYYY-MM-DD HH24:MI:SSOF': 'YYYY-MM-DD',
            'MM/DD/YY HH24:MI': 'MM/DD/YY',
        }
        for datetimeformattz in DATETIMETZ_CASES:
            records_format = DelimitedRecordsFormat(hints={
                # Pandas doesn't consider dateformats to be separate
                # from datetime/datetimetz formats, so they need to be
                # consistent
                'dateformat': compatible_dateformat[datetimeformattz],
                'datetimeformat': datetimeformattz.replace('OF', ''),
                'datetimeformattz': datetimeformattz,
                'compression': None,
            })
            unhandled_hints = set(records_format.hints)
            processing_instructions = ProcessingInstructions()
            try:
                options = pandas_to_csv_options(records_format,
                                                unhandled_hints,
                                                processing_instructions)
            except NotImplementedError:
                if datetimeformattz in known_failures:
                    continue
                else:
                    raise
            self.assertEqual(options['date_format'], expectations[datetimeformattz],
                             datetimeformattz)
            self.assertNotIn(datetimeformattz, known_failures)

            fileobj = io.StringIO(create_sample(datetimeformattz))
            df = pandas.DataFrame(data={'datetime':
                                        [pandas.Timestamp(day=SAMPLE_DAY,
                                                          month=SAMPLE_MONTH,
                                                          year=SAMPLE_YEAR,
                                                          hour=SAMPLE_HOUR,
                                                          minute=SAMPLE_MINUTE,
                                                          second=SAMPLE_SECOND)]},
                                  columns=['datetime'])
            df.to_csv(path_or_buf=fileobj,
                      index=False,
                      **options)
            output = fileobj.getvalue()
            # In reality this isn't used raw, as Pandas doesn't really
            # try to handle lone dates or times.  Instead, we use
            # prep_for_csv() to preconvert these Serieses into strings.

            # Pandas drops the timezone offset if it's zero, which it
            # is in our example
            datetimeformattz_minus_offset = datetimeformattz.replace('OF', '')
            sample = create_sample(datetimeformattz_minus_offset)
            if 'SS' in datetimeformattz:
                self.assertEqual(output, f"{sample}.000000\n")
            else:
                self.assertEqual(output, sample + '\n')

    def test_datetimeformattz(self) -> None:
        known_failures: Set[str] = set()
        expectations = {
            'YYYY-MM-DD HH24:MI:SS': '%Y-%m-%d %H:%M:%S.%f',
            'YYYY-MM-DD HH:MI:SS': '%Y-%m-%d %H:%M:%S.%f',
            'MM/DD/YY HH24:MI': '%m/%d/%y %H:%M',
            'YYYY-MM-DD HH12:MI AM': '%Y-%m-%d %I:%M %p',
        }
        compatible_dateformat = {
            'YYYY-MM-DD HH24:MI:SS': 'YYYY-MM-DD',
            'YYYY-MM-DD HH:MI:SS': 'YYYY-MM-DD',
            'YYYY-MM-DD HH12:MI AM': 'YYYY-MM-DD',
            'MM/DD/YY HH24:MI': 'MM/DD/YY',
        }
        for datetimeformat in DATETIME_CASES:
            records_format = DelimitedRecordsFormat(hints={
                # Pandas doesn't consider dateformats to be separate
                # from datetime/datetimetz formats, so they need to be
                # consistent
                'dateformat': compatible_dateformat[datetimeformat],
                'datetimeformat': datetimeformat,
                'datetimeformattz': datetimeformat,
                'compression': None,
            })
            unhandled_hints = set(records_format.hints)
            processing_instructions = ProcessingInstructions()
            try:
                options = pandas_to_csv_options(records_format,
                                                unhandled_hints,
                                                processing_instructions)
            except NotImplementedError:
                if datetimeformat in known_failures:
                    continue
                else:
                    raise
            self.assertEqual(options['date_format'], expectations[datetimeformat],
                             datetimeformat)
            self.assertNotIn(datetimeformat, known_failures)

            fileobj = io.StringIO(create_sample(datetimeformat))
            df = pandas.DataFrame(data={'datetime':
                                        [pandas.Timestamp(day=SAMPLE_DAY,
                                                          month=SAMPLE_MONTH,
                                                          year=SAMPLE_YEAR,
                                                          hour=SAMPLE_HOUR,
                                                          minute=SAMPLE_MINUTE,
                                                          second=SAMPLE_SECOND)]},
                                  columns=['datetime'])
            df.to_csv(path_or_buf=fileobj,
                      index=False,
                      **options)
            output = fileobj.getvalue()
            # In reality this isn't used raw, as Pandas doesn't really
            # try to handle lone dates or times.  Instead, we use
            # prep_for_csv() to preconvert these Serieses into strings.
            sample = create_sample(datetimeformat)
            if 'SS' in datetimeformat:
                # Pandas doesn't truncate fractional seconds in the
                # same way other tools do.
                self.assertEqual(output, f"{sample}.000000\n")
            else:
                self.assertEqual(output, f"{sample}\n",
                                 datetimeformat)
