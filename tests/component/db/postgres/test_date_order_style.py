import unittest
from records_mover.records import DelimitedRecordsFormat
from records_mover.db.postgres.copy_options.date_input_style import determine_input_date_order_style
from ...records.datetime_cases import DATE_CASES, DATETIMEFORMATTZ_CASES


class TestDateOrderStyle(unittest.TestCase):
    def test_determine_date_order_style(self):
        unhandled_hints = set()
        tests = [
            (
                # No ambiguity, can handle all
                {
                    'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
                    'datetimeformat': "YYYY-MM-DD HH12:MI AM",
                    'timeonlyformat': "HH12:MI AM",
                    'dateformat': "YYYY-MM-DD",
                },
                None
            ),
            (
                # No ambiguity, can handle all
                {
                    'datetimeformattz': 'YYYY-MM-DD HH24:MI:SSOF',
                    'datetimeformat': "YYYY-MM-DD HH24:MI:SS",
                    'timeonlyformat': "HH24:MI",
                    'dateformat': "YYYY-MM-DD",
                },
                None
            ),            (
                # No ambiguity, can handle all
                {
                    'datetimeformattz': 'YYYY-MM-DD HH:MI:SS',
                    'datetimeformat': "YYYY-MM-DD HH12:MI AM",
                    'timeonlyformat': "HH12:MI AM",
                    'dateformat': "YYYY-MM-DD",
                },
                None
            ),
            (
                {
                    'datetimeformattz': 'DD-MM-YY HH:MI:SSOF',
                    'datetimeformat': "DD-MM-YY HH12:MI AM",
                    'timeonlyformat': "HH12:MI AM",
                    'dateformat': "DD-MM-YY",
                },
                'DMY'
            ),
            (
                {
                    'datetimeformattz': 'DD/MM/YY HH:MI:SSOF',
                    'datetimeformat': "DD/MM/YY HH12:MI AM",
                    'timeonlyformat': "HH12:MI AM",
                    'dateformat': "DD/MM/YY",
                },
                'DMY'
            ),
            (
                {
                    'datetimeformattz': 'DD-MM-YYYY HH:MI:SSOF',
                    'datetimeformat': "DD-MM-YYYY HH12:MI AM",
                    'timeonlyformat': "HH12:MI AM",
                    'dateformat': "DD-MM-YYYY",
                },
                'DMY'
            ),
            (
                {
                    'datetimeformattz': 'MM/DD/YY HH:MI:SSOF',
                    'datetimeformat': "MM/DD/YY HH12:MI AM",
                    'timeonlyformat': "HH12:MI AM",
                    'dateformat': "MM/DD/YY",
                },
                'MDY'
            ),
            (
                {
                    'datetimeformattz': 'MM/DD/YY HH24:MI',
                    'datetimeformat': "MM/DD/YY HH24:MI",
                    'timeonlyformat': "HH24:MI",
                    'dateformat': "MM/DD/YY",
                },
                'MDY'
            ),
            (
                {
                    'datetimeformattz': 'MM-DD-YYYY HH:MI:SSOF',
                    'datetimeformat': "MM-DD-YYYY HH12:MI AM",
                    'timeonlyformat': "HH12:MI AM",
                    'dateformat': "MM-DD-YYYY",
                },
                'MDY'
            ),
            (
                # No ambiguity, can handle all
                {
                    'datetimeformattz': 'INVALID',
                    'datetimeformat': "YYYY-MM-DD HH12:MI AM",
                    'timeonlyformat': "HH12:MI AM",
                    'dateformat': "YYYY-MM-DD",
                },
                NotImplementedError
            ),
            (
                # Can't parse MDY and DMY at the same time
                {
                    'datetimeformattz': 'MM/DD/YY HH24:MI',
                    'datetimeformat': "MM/DD/YY HH24:MI",
                    'timeonlyformat': "HH12:MI AM",
                    'dateformat': "DD-MM-YYYY",
                },
                NotImplementedError
            ),
            (
                # Can't parse MDY and DMY at the same time
                {
                    'datetimeformattz': 'MM/DD/YY HH24:MI',
                    'datetimeformat': "other",
                    'timeonlyformat': "HH12:MI AM",
                    'dateformat': "MM-DD-YYYY",
                },
                NotImplementedError
            ),
        ]
        unhandled_date_cases = set(DATE_CASES)
        unhandled_datetimeformattz_cases = set(DATETIMEFORMATTZ_CASES)
        fail_if_cant_handle_hint = True
        for raw_hints, expected_result in tests:
            records_format = DelimitedRecordsFormat(hints=raw_hints)
            if expected_result == NotImplementedError:
                with self.assertRaises(NotImplementedError):
                    validated_hints = records_format.\
                        validate(fail_if_cant_handle_hint=fail_if_cant_handle_hint)
                    determine_input_date_order_style(unhandled_hints,
                                                     validated_hints,
                                                     fail_if_cant_handle_hint)
            else:
                validated_hints = records_format.\
                    validate(fail_if_cant_handle_hint=fail_if_cant_handle_hint)
                out = determine_input_date_order_style(unhandled_hints,
                                                       validated_hints,
                                                       fail_if_cant_handle_hint)
                self.assertEqual(out, expected_result)
                unhandled_date_cases.discard(raw_hints['dateformat'])
                unhandled_datetimeformattz_cases.discard(raw_hints['datetimeformattz'])
        self.assertFalse(unhandled_date_cases)
        self.assertFalse(unhandled_datetimeformattz_cases)
