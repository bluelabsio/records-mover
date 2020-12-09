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
    def test_dateformat(self) -> None:
        # This behavior isn't right; to_csv_options uses
        # prep_for_csv.py to preformat date columns as strings.  See
        # https://github.com/bluelabsio/records-mover/issues/142
        #
        # This test can be radically simplified once datetime behavior
        # is removed entirely from to_csv_options.
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
            df = pandas.DataFrame(data={'date': [pandas.Timestamp(day=SAMPLE_DAY,
                                                                  month=SAMPLE_MONTH,
                                                                  year=SAMPLE_YEAR)]},
                                  columns=['date'])
            df.to_csv(path_or_buf=fileobj,
                      index=False,
                      **options)
            output = fileobj.getvalue()
            # In reality this isn't used raw, as Pandas doesn't really
            # try to handle lone dates or times.  Instead, we use
            # prep_for_csv() to preconvert these Serieses into strings.
            sample = create_sample(dateformat)
            #
            # As noted above, Pandas doesn't really have a concept of
            # formatting just a date, so it appends the time
            # information regardless.
            self.assertEqual(output, f"{sample} 00:00:00.000000\n")

    def test_datetimeformat(self) -> None:
        known_failures: Set[str] = set()
        expectations = {
            'YYYY-MM-DD HH:MI:SSOF': '%Y-%m-%d %H:%M:%S.%f%z',
            'YYYY-MM-DD HH:MI:SS': '%Y-%m-%d %H:%M:%S.%f',
            'YYYY-MM-DD HH24:MI:SSOF': '%Y-%m-%d %H:%M:%S.%f%z',
            # TODO: Why are we including seconds here are at all?
            'MM/DD/YY HH24:MI': '%m/%d/%y %H:%M:%S.%f',
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
            self.assertEqual(options['date_format'], expectations[datetimeformattz])

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
                # TODO: Why is second included when hint didn't include it?
                self.assertEqual(output, f"{sample}:{SAMPLE_SECOND:02d}.000000\n")

    def test_datetimeformattz(self) -> None:
        known_failures = {
            # We could probably support this once to_csv_options.py is
            # less an if/else lookup and more of a function.
            'YYYY-MM-DD HH12:MI AM',
        }
        expectations = {
            'YYYY-MM-DD HH24:MI:SS': '%Y-%m-%d %H:%M:%S.%f',
            'YYYY-MM-DD HH:MI:SS': '%Y-%m-%d %H:%M:%S.%f',
            'MM/DD/YY HH24:MI': '%m/%d/%y %H:%M:%S.%f',
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
            self.assertEqual(options['date_format'], expectations[datetimeformat])

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
                # TODO: Why is second included when hint didn't include it?
                self.assertEqual(output, f"{sample}:{SAMPLE_SECOND:02d}.000000\n")

    def test_timeonlyformat(self) -> None:
        known_failures = {
            # https://github.com/bluelabsio/records-mover/issues/142
            'HH12:MI AM',
            'HH:MI:SS',
        }
        for timeonlyformat in TIMEONLY_CASES:
            records_format = DelimitedRecordsFormat(hints={
                # Pandas doesn't consider dateformats to be separate
                # from datetime/datetimetz formats, so they need to be
                # consistent
                'dateformat': 'YYYY-MM-DD',
                'datetimeformat': f'YYYY-MM-DD {timeonlyformat}',
                'datetimeformattz': f'YYYY-MM-DD {timeonlyformat}',
                'timeonlyformat': timeonlyformat,
                'compression': None,
            })
            unhandled_hints = set(records_format.hints)
            processing_instructions = ProcessingInstructions()
            try:
                options = pandas_to_csv_options(records_format,
                                                unhandled_hints,
                                                processing_instructions)
            except NotImplementedError:
                if timeonlyformat in known_failures:
                    continue
                else:
                    raise
            fileobj = io.StringIO(create_sample(timeonlyformat))

            # Just make sure to_csv() doesn't reject any of the
            # options.  In reality this isn't used raw, as Pandas
            # doesn't really try to handle lone dates or times.
            # Instead, we use prep_for_csv() to preconvert these
            # Serieses into strings.
            df = pandas.DataFrame(data={'unrelated': [1]},
                                  columns=['unrelated'])
            df.to_csv(path_or_buf=fileobj,
                      index=False,
                      **options)
            output = fileobj.getvalue()
            self.assertTrue(output)
