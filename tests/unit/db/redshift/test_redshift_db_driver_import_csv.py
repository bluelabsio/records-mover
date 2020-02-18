from unittest.mock import patch
from .base_test_redshift_db_driver import BaseTestRedshiftDBDriver
from ...records.format_hints import (csv_format_hints)
from records_mover.db.redshift.redshift_db_driver import Table
from records_mover.db.redshift.records_copy import Format


class TestRedshiftDBDriverImport(BaseTestRedshiftDBDriver):
    @patch('records_mover.db.redshift.loader.CopyCommand')
    def test_load_csv(self, mock_CopyCommand):
        lines_scanned = self.load(csv_format_hints, fail_if=True)

        expected_args = {
            'access_key_id': 'fake_aws_id',
            'compression': 'GZIP',
            'data_location': 's3://mybucket/myparent/mychild/_manifest',
            'date_format': 'MM/DD/YY',
            'encoding': 'UTF8',
            'format': Format.csv,
            'ignore_header': 1,
            'manifest': True,
            'max_error': 0,
            'quote': '"',
            'secret_access_key': 'fake_aws_secret',
            'session_token': 'fake_aws_token',
            'time_format': 'MM/DD/YY HH24:MI',
            'to': Table('mytable', self.redshift_db_driver.meta, schema='myschema'),
            'region': self.mock_directory.loc.region,
            'empty_as_null': True,
        }

        mock_CopyCommand.assert_called_with(**expected_args)
        self.assertIsNone(lines_scanned)
