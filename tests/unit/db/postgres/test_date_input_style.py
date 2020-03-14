import unittest
from records_mover.db.postgres.copy_options.date_input_style import determine_date_input_style


class TestDateInputStyle(unittest.TestCase):
    def test_determine_date_input_style_(self):
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
        ]
        fail_if_cant_handle_hint = True
        for hints, expected_result in tests:
            if expected_result == NotImplementedError:
                with self.assertRaises(NotImplementedError):
                    determine_date_input_style(unhandled_hints,
                                               hints,
                                               fail_if_cant_handle_hint)
            else:
                out = determine_date_input_style(unhandled_hints,
                                                 hints,
                                                 fail_if_cant_handle_hint)
                self.assertEqual(out, expected_result)
