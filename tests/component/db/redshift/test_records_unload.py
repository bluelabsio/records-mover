import unittest
from typing import Set
from records_mover.db.redshift.records_unload import redshift_unload_options
from records_mover.records import DelimitedRecordsFormat
from ...records.datetime_cases import (
    DATE_CASES, DATETIMETZ_CASES, DATETIME_CASES, TIMEONLY_CASES
)


class TestRecordsUnload(unittest.TestCase):
    def test_redshift_unload_options_datetimeformattz(self):
        # Redshift offers no options and only unloads YYYY-MM-DD
        # HH:MI:SSOF, so we should reject everything else.  Double
        # check with the docs just in case, though--maybe that's
        # changed!
        expected_failures = {
            'YYYY-MM-DD HH:MI:SS',
            'MM/DD/YY HH24:MI',
        }
        for datetimeformattz in DATETIMETZ_CASES:
            hints = {
                'datetimeformattz': datetimeformattz,
            }
            records_format =\
                DelimitedRecordsFormat(variant='bluelabs',
                                       hints=hints)
            unhandled_hints = set(records_format.hints.keys())
            try:
                redshift_unload_options(unhandled_hints,
                                        records_format,
                                        fail_if_cant_handle_hint=True)
            except NotImplementedError:
                if datetimeformattz in expected_failures:
                    continue
                else:
                    raise
            self.assertNotIn(datetimeformattz, expected_failures)

    def test_redshift_unload_options_datetimeformat(self):
        # Redshift offers no options and only unloads YYYY-MM-DD
        # HH:MI:SS, so we should reject everything else.  Double
        # check with the docs just in case, though--maybe that's
        # changed!
        expected_failures = {
            'YYYY-MM-DD HH12:MI AM',
            'MM/DD/YY HH24:MI',
        }
        for datetimeformat in DATETIME_CASES:
            hints = {
                'datetimeformat': datetimeformat,
            }
            records_format =\
                DelimitedRecordsFormat(variant='bluelabs',
                                       hints=hints)
            unhandled_hints = set(records_format.hints.keys())
            try:
                redshift_unload_options(unhandled_hints,
                                        records_format,
                                        fail_if_cant_handle_hint=True)
            except NotImplementedError:
                if datetimeformat in expected_failures:
                    continue
                else:
                    raise
            self.assertNotIn(datetimeformat, expected_failures)

    def test_redshift_unload_options_timeonlyformat(self):
        # Redshift didn't used to have a time only format, so Records
        # Mover doesn't yet support anything here and just treats
        # these as strings.  Nothing should raise.
        expected_failures: Set[str] = set()
        for timeonlyformat in TIMEONLY_CASES:
            hints = {
                'timeonlyformat': timeonlyformat,
            }
            records_format =\
                DelimitedRecordsFormat(variant='bluelabs',
                                       hints=hints)
            unhandled_hints = set(records_format.hints.keys())
            try:
                redshift_unload_options(unhandled_hints,
                                        records_format,
                                        fail_if_cant_handle_hint=True)
            except NotImplementedError:
                if timeonlyformat in expected_failures:
                    continue
                else:
                    raise
            self.assertNotIn(timeonlyformat, expected_failures)

    def test_redshift_unload_options_dateformat(self):
        # Redshift offers no options and only unloads YYYY-MM-DD, so
        # we should reject everything else.  Double check with the
        # docs just in case, though--maybe that's changed!
        expected_failures = {
            'MM-DD-YYYY',
            'DD-MM-YYYY',
            'MM/DD/YY',
            'DD/MM/YY',
            'DD-MM-YY',
        }
        for dateformat in DATE_CASES:
            hints = {
                'dateformat': dateformat,
            }
            records_format =\
                DelimitedRecordsFormat(variant='bluelabs',
                                       hints=hints)
            unhandled_hints = set(records_format.hints.keys())
            try:
                redshift_unload_options(unhandled_hints,
                                        records_format,
                                        fail_if_cant_handle_hint=True)
            except NotImplementedError:
                if dateformat in expected_failures:
                    continue
                else:
                    raise
            self.assertNotIn(dateformat, expected_failures)
