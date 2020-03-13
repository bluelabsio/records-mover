import unittest
from records_mover.db.postgres.date_input_style import determine_date_input_style
from mock import MagicMock, Mock


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
                'RAISE'
            ),
        ]
        fail_if_cant_handle_hint = True
        for hints, expected_result in tests:
            if expected_result == 'RAISE':
                with self.assertRaises(NotImplementedError):
                    determine_date_input_style(unhandled_hints,
                                               hints,
                                               fail_if_cant_handle_hint)
            else:
                out = determine_date_input_style(unhandled_hints,
                                                 hints,
                                                 fail_if_cant_handle_hint)
                self.assertEqual(out, expected_result)
