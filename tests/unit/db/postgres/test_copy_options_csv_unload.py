import unittest
from records_mover.records import DelimitedRecordsFormat
from records_mover.db.postgres.copy_options.csv import\
    postgres_copy_options_csv
from records_mover.db.postgres.copy_options.mode import CopyOptionsMode


class TestCopyOptionsCsvUnload(unittest.TestCase):
    def test_postgres_copy_options_csv_minimal_quoting(self):
        unhandled_hints = set()
        records_format = DelimitedRecordsFormat(variant='csv',
                                                hints={
                                                    'quoting': 'minimal',
                                                    'compression': None,
                                                })
        fail_if_cant_handle_hint = True
        mode = CopyOptionsMode.UNLOADING

        out = postgres_copy_options_csv(unhandled_hints,
                                        records_format.hints,
                                        fail_if_cant_handle_hint,
                                        mode)
        self.assertEqual(out, {
            'format': 'csv',
            'quote': '"',
            'delimiter': ',',
            'encoding': 'UTF8',
            'format': 'csv',
            'header': True,
        })

    def test_postgres_copy_options_csv_all_quoting(self):
        unhandled_hints = set()
        records_format = DelimitedRecordsFormat(variant='csv',
                                                hints={
                                                    'quoting': 'all',
                                                    'compression': None,
                                                })
        fail_if_cant_handle_hint = True
        mode = CopyOptionsMode.UNLOADING

        out = postgres_copy_options_csv(unhandled_hints,
                                        records_format.hints,
                                        fail_if_cant_handle_hint,
                                        mode)
        self.assertEqual(out, {
            'format': 'csv',
            'quote': '"',
            'force_quote': '*',
            'delimiter': ',',
            'encoding': 'UTF8',
            'format': 'csv',
            'header': True,
        })

    def test_postgres_copy_options_csv_no_quoting(self):
        unhandled_hints = set()
        records_format = DelimitedRecordsFormat(variant='csv',
                                                hints={
                                                    'quoting': None,
                                                    'compression': None,
                                                })
        fail_if_cant_handle_hint = True

        with self.assertRaises(NotImplementedError):
            postgres_copy_options_csv(unhandled_hints,
                                      records_format.hints,
                                      fail_if_cant_handle_hint,
                                      CopyOptionsMode.UNLOADING)
