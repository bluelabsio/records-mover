import unittest
from records_mover.records import DelimitedRecordsFormat
from records_mover.db.mysql.load_options import mysql_load_options
from ...records.datetime_cases import (
    DATE_CASES, create_sample, SAMPLE_YEAR, SAMPLE_MONTH, SAMPLE_DAY
)
from typing import Set


class TestMySQLLoadOptions(unittest.TestCase):
    def test_mysql_load_options_encoding_utf8bom_fail(self) -> None:
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={
                                                    'encoding': 'UTF8BOM',
                                                    'compression': None,
                                                })
        unhandled_hints = set(records_format.hints.keys())
        with self.assertRaises(NotImplementedError) as r:
            mysql_load_options(unhandled_hints,
                               records_format,
                               fail_if_cant_handle_hint=True)
        self.assertIn('UTF8BOM', str(r.exception))

    def test_mysql_load_options_encoding_utf8bom_fallback(self) -> None:
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={
                                                    'encoding': 'UTF8BOM',
                                                    'compression': None,
                                                })
        unhandled_hints = set(records_format.hints.keys())
        out = mysql_load_options(unhandled_hints,
                                 records_format,
                                 fail_if_cant_handle_hint=False)
        self.assertEqual(out.character_set, 'utf8')

    def test_mysql_load_options_all_quoting(self) -> None:
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={
                                                    'quoting': 'all',
                                                    'doublequote': True,
                                                    'compression': None,
                                                })
        unhandled_hints = set(records_format.hints.keys())
        out = mysql_load_options(unhandled_hints,
                                 records_format,
                                 fail_if_cant_handle_hint=True)
        self.assertEqual(out.fields_enclosed_by, '"')

    def test_mysql_load_options_nonnumeric_quoting(self) -> None:
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={
                                                    'quoting': 'nonnumeric',
                                                    'doublequote': True,
                                                    'compression': None,
                                                })
        unhandled_hints = set(records_format.hints.keys())
        out = mysql_load_options(unhandled_hints,
                                 records_format,
                                 fail_if_cant_handle_hint=True)
        self.assertEqual(out.fields_optionally_enclosed_by, '"')

    def test_mysql_load_options_bogus_quoting(self) -> None:
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={
                                                    'quoting': 'no thanks',  # type: ignore
                                                    'doublequote': True,
                                                    'compression': None,
                                                })
        unhandled_hints = set(records_format.hints.keys())
        with self.assertRaises(NotImplementedError) as r:
            mysql_load_options(unhandled_hints,
                               records_format,
                               fail_if_cant_handle_hint=True)

        self.assertIn('no thanks', str(r.exception))

    def test_mysql_load_options_valid_quoting_no_doublequote(self) -> None:
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={
                                                    'quoting': 'all',
                                                    'doublequote': False,
                                                    'compression': None,
                                                })
        unhandled_hints = set(records_format.hints.keys())
        with self.assertRaises(NotImplementedError) as r:
            mysql_load_options(unhandled_hints,
                               records_format,
                               fail_if_cant_handle_hint=True)

        self.assertIn('doublequote=False', str(r.exception))

    def test_mysql_load_options_valid_quoting_bogus_doublequote(self) -> None:
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={
                                                    'quoting': 'all',
                                                    'doublequote': 'mumble',  # type: ignore
                                                    'compression': None,
                                                })
        unhandled_hints = set(records_format.hints.keys())
        with self.assertRaises(NotImplementedError) as r:
            mysql_load_options(unhandled_hints,
                               records_format,
                               fail_if_cant_handle_hint=True)

        self.assertIn("doublequote='mumble'", str(r.exception))

    def test_mysql_load_options_dateformat(self) -> None:
        # See comment in load_options.py - this list should be longer.
        expected_failures: Set[str] = set()

        for dateformat in DATE_CASES:
            records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                    hints={
                                                        'dateformat': dateformat,
                                                        'compression': None,
                                                    })
            unhandled_hints = set(records_format.hints.keys())
            try:
                mysql_load_options(unhandled_hints,
                                   records_format,
                                   fail_if_cant_handle_hint=True)
            except NotImplementedError:
                if dateformat in expected_failures:
                    pass
                else:
                    raise
