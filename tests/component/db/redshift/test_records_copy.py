import unittest
from records_mover.db.redshift.records_copy import redshift_copy_options
from records_mover.records import DelimitedRecordsFormat, AvroRecordsFormat
from sqlalchemy_redshift.commands import Encoding, Format
from ...records.datetime_cases import DATE_CASES


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
            self.assertIs(out['date_format'], dateformat)
