import unittest
from records_mover.db.postgres.copy_options.date_output_style import\
    determine_date_output_style


class TestPostgresCopyOptionsDateOutputStyle(unittest.TestCase):
    def test_determine_output_date_order_style_iso(self):
        unhandled_hints = set()
        hints = {
            'dateformat': 'YYYY-MM-DD',
            'timeonlyformat': 'HH24:MI:SS',
            'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
            'datetimeformat': 'YYYY-MM-DD HH24:MI:SS'
        }
        fail_if_cant_handle_hint = True

        out = determine_date_output_style(unhandled_hints,
                                          hints,
                                          fail_if_cant_handle_hint)
        self.assertEqual(out, ('ISO', None))
