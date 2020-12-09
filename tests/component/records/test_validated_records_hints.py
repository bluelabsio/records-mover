from typing_inspect import get_args
from records_mover.records.records_types import DelimitedVariant
from records_mover.records import DelimitedRecordsFormat
import unittest


class TestValidatedRecordsHints(unittest.TestCase):
    def test_valid(self):
        variants = list(get_args(DelimitedVariant))
        for variant in variants:
            records_format = DelimitedRecordsFormat(variant=variant)
            records_format.validate(fail_if_cant_handle_hint=True)


    # TOOD: Is there an effective dynamic validation I can do?
    #
    # def test_invalid_dateformattz(self):
    #     records_format = DelimitedRecordsFormat(variant='bluelabs',
    #                                             hints={
    #                                                 'datetimeformattz': 'invalid'
    #                                             })
    #     with self.assertRaises(NotImplementedError):
    #         records_format.validate(fail_if_cant_handle_hint=True)
