import records_mover
from records_mover.records import DelimitedRecordsFormat
import unittest


class TestValidatedRecordsHints(unittest.TestCase):
    def test_valid(self):
        variants = records_mover.records.types.VALID_VARIANTS
        for variant in variants:
            records_format = DelimitedRecordsFormat(variant=variant)
            records_format.validate(fail_if_cant_handle_hint=True)

    def test_invalid_dateformattz(self):
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={
                                                    'datetimeformattz': 'invalid'
                                                })
        with self.assertRaises(NotImplementedError):
            records_format.validate(fail_if_cant_handle_hint=True)
