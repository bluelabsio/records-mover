from .base_test_redshift_db_driver import BaseTestRedshiftDBDriver
from ...records.format_hints import vertica_format_hints


class TestRedshiftDBDriverImport(BaseTestRedshiftDBDriver):
    def test_load_vertica_fails(self):
        with self.assertRaises(NotImplementedError):
            self.load(vertica_format_hints, fail_if=True)
