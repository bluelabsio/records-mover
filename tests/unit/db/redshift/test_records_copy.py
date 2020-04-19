import unittest
from records_mover.db.redshift.records_copy import redshift_copy_options
from records_mover.records import DelimitedRecordsFormat
from sqlalchemy_redshift.commands import Encoding


class TestRecordsCopy(unittest.TestCase):
    def test_redshift_copy_options_bad_compression_roll_with_it(self):
        records_format =\
            DelimitedRecordsFormat(variant='bluelabs',
                                   hints={
                                       'compression': 'somethingnew'
                                   })
        unhandled_hints = set(records_format.hints.keys())
        # This isn't in the Enum...for now.
        with self.assertRaises(ValueError):
            redshift_copy_options(unhandled_hints,
                                  records_format.hints,
                                  fail_if_cant_handle_hint=False,
                                  fail_if_row_invalid=True,
                                  max_failure_rows=0)

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
                                        records_format.hints,
                                        fail_if_cant_handle_hint=True,
                                        fail_if_row_invalid=True,
                                        max_failure_rows=0)
            self.assertIs(out['encoding'], redshift_sqlalchemy_spelling)
