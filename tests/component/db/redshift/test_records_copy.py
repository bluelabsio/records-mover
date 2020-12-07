import unittest
from records_mover.db.redshift.records_copy import redshift_copy_options
from records_mover.records import DelimitedRecordsFormat, AvroRecordsFormat
from sqlalchemy_redshift.commands import Encoding, Format
from ...records.datetime_cases import DATE_CASES, DATETIMEFORMATTZ_CASES


class TestRecordsCopy(unittest.TestCase):
    def test_redshift_copy_options_encodings(self):
        tests = {
            'UTF16': Encoding.utf16,
            'UTF16LE': Encoding.utf16le,
            'UTF16BE': Encoding.utf16be
        }
        for hint_spelling, redshift_sqlalchemy_spelling in tests.items():

            records_format =\
                DelimitedRecordsFormat(variant='bluelabs',
                                       hints={
                                           'encoding': hint_spelling
                                       })
            unhandled_hints = set(records_format.hints.keys())
            out = redshift_copy_options(unhandled_hints,
                                        records_format,
                                        fail_if_cant_handle_hint=True,
                                        fail_if_row_invalid=True,
                                        max_failure_rows=0)
            self.assertIs(out['encoding'], redshift_sqlalchemy_spelling)

    def test_redshift_copy_options_avro(self):
        records_format = AvroRecordsFormat()
        unhandled_hints = set()
        out = redshift_copy_options(unhandled_hints,
                                    records_format,
                                    fail_if_cant_handle_hint=True,
                                    fail_if_row_invalid=True,
                                    max_failure_rows=0)
        self.assertIs(out['format'], Format.avro)

    def test_redshift_copy_options_dateformat(self):
        # The records spec's date/time formats are based on Redshift's
        # spec originally, so it's expected that everything here would
        # be accepted as-is, but please double-check with Redshift's
        # docs as new test cases are added
        accept_as_is = {
            'YYYY-MM-DD': True,
            'MM-DD-YYYY': True,
            'DD-MM-YYYY': True,
            'MM/DD/YY': True,
            'DD/MM/YY': True,
            'DD-MM-YY': True,
        }
        for dateformat in DATE_CASES:
            records_format =\
                DelimitedRecordsFormat(variant='bluelabs',
                                       hints={
                                           'dateformat': dateformat
                                       })
            unhandled_hints = set(records_format.hints.keys())
            out = redshift_copy_options(unhandled_hints,
                                        records_format,
                                        fail_if_cant_handle_hint=True,
                                        fail_if_row_invalid=True,
                                        max_failure_rows=0)
            if accept_as_is[dateformat]:
                self.assertIs(out['date_format'], dateformat)
            else:
                self.fail('define what to expect here')

    def test_redshift_copy_options_datetimeformattz(self):
        # Redshift's time_format doesn't support separte configuration
        # for datetimeformat vs datetimeformattz, but the 'auto' flag
        # seems to work with specific things (see tests run in
        # records_copy.py).
        #
        # Please verify new formats have a test run
        # and documented in records_copy.py before putting an entry in
        # here.
        expectations = {
            'YYYY-MM-DD HH:MI:SSOF': 'auto',
            'YYYY-MM-DD HH:MI:SS': 'YYYY-MM-DD HH:MI:SS',
            'YYYY-MM-DD HH24:MI:SSOF': 'auto',
            'MM/DD/YY HH24:MIOF': 'auto',
            'MM/DD/YY HH24:MI': 'MM/DD/YY HH24:MI',
        }
        for datetimeformattz in DATETIMEFORMATTZ_CASES:
            hints = {
                'datetimeformattz': datetimeformattz,
                'datetimeformat': datetimeformattz.replace('OF', '')
            }
            records_format =\
                DelimitedRecordsFormat(variant='bluelabs',
                                       hints=hints)
            unhandled_hints = set(records_format.hints.keys())
            out = redshift_copy_options(unhandled_hints,
                                        records_format,
                                        fail_if_cant_handle_hint=True,
                                        fail_if_row_invalid=True,
                                        max_failure_rows=0)
            self.assertEquals(out['time_format'], expectations[datetimeformattz])
