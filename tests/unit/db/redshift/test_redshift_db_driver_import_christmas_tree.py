from .base_test_redshift_db_driver import BaseTestRedshiftDBDriver
from records_mover.db.redshift.redshift_db_driver import Table
from mock import call, patch
from records_mover.records.delimited.utils import logger as driver_logger
from ...records.format_hints import (christmas_tree_format_1_hints,
                                     christmas_tree_format_2_hints)
from sqlalchemy_redshift.commands import Encoding, Compression


class TestRedshiftDBDriverImportBlueLabs(BaseTestRedshiftDBDriver):
    maxDiff = None

    @patch('records_mover.db.redshift.loader.CopyCommand')
    def test_load_vertica_christmas_tree_unsupported_options_with_fast_warns_1(self,
                                                                               mock_CopyCommand):
        with patch.object(driver_logger, 'warning') as mock_warning:

            lines_scanned = self.load(christmas_tree_format_1_hints, fail_if=False)

            self.assertListEqual(mock_warning.mock_calls,
                                 [call("Ignoring hint quoting = 'nonnumeric'"),
                                  call("Ignoring hint record-terminator = '\\x02'")])

        expected_best_effort_args = {
            'access_key_id': 'fake_aws_id',
            'compression': Compression.lzop,
            'data_location': 's3://mybucket/myparent/mychild/_manifest',
            'date_format': 'YYYY-MM-DD',
            'encoding': Encoding.utf8,
            'delimiter': '\x01',
            'escape': True,
            'ignore_header': 1,
            'manifest': True,
            'max_error': 100000,
            'quote': '"',
            'secret_access_key': 'fake_aws_secret',
            'session_token': 'fake_aws_token',
            'time_format': 'auto',
            'to': Table('mytable', self.redshift_db_driver.meta, schema='myschema'),
            'region': self.mock_directory.loc.region,
            'empty_as_null': True,
        }

        mock_CopyCommand.assert_called_with(**expected_best_effort_args)
        self.assertIsNone(lines_scanned)

    @patch('records_mover.db.redshift.loader.CopyCommand')
    def test_load_christmas_tree_unsupported_options_with_fast_warns_2(self,
                                                                       mock_CopyCommand):
        with patch.object(driver_logger, 'warning') as mock_warning:
            lines_scanned = self.load(christmas_tree_format_2_hints, fail_if=False)

            self.assertListEqual(mock_warning.mock_calls,
                                 [call("Ignoring hint escape = '@'"),
                                  call("Ignoring hint datetimeformattz = 'HH:MI:SSOF YYYY-MM-DD'"),
                                  call("Ignoring hint doublequote = True"),
                                  call("Ignoring hint record-terminator = '\\x02'")])

        expected_best_effort_args = {
            'access_key_id': 'fake_aws_id',
            'compression': Compression.bzip2,
            'data_location': 's3://mybucket/myparent/mychild/_manifest',
            'date_format': 'MM-DD-YYYY',
            'delimiter': '\x01',
            'encoding': Encoding.utf8,
            'escape': True,
            'ignore_header': 0,
            'manifest': True,
            'max_error': 100000,
            'quote': '"',
            'remove_quotes': True,
            'secret_access_key': 'fake_aws_secret',
            'session_token': 'fake_aws_token',
            'time_format': 'auto',
            'to': Table('mytable', self.redshift_db_driver.meta, schema='myschema'),
            'region': self.mock_directory.loc.region,
            'empty_as_null': True,
        }

        mock_CopyCommand.assert_called_with(**expected_best_effort_args)
        self.assertIsNone(lines_scanned)
