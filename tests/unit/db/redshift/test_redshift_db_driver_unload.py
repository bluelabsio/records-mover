from .base_test_redshift_db_driver import BaseTestRedshiftDBDriver
from ...records.format_hints import (bluelabs_format_hints,
                                     christmas_tree_format_1_hints,
                                     christmas_tree_format_2_hints)
from records_mover.records.delimited.utils import logger as driver_logger
from mock import call, patch


def fake_text(s):
    return (s,)


class TestRedshiftDBDriverUnload(BaseTestRedshiftDBDriver):
    maxDiff = None

    @patch('records_mover.db.redshift.unloader.UnloadFromSelect')
    @patch('records_mover.db.redshift.unloader.text')
    def test_unload_to_non_s3(self,
                              mock_text,
                              mock_UnloadFromSelect):
        mock_text.side_effect = fake_text
        self.mock_records_unload_plan.processing_instructions.fail_if_dont_understand = True
        self.mock_records_unload_plan.processing_instructions.fail_if_cant_handle_hint = True
        self.mock_records_unload_plan.records_format.hints = bluelabs_format_hints
        self.mock_directory.scheme = 'mumble'
        self.mock_db_engine.execute.return_value.scalar.return_value = 456
        rows = self.redshift_db_driver.unloader().\
            unload(schema='myschema',
                   table='mytable',
                   unload_plan=self.mock_records_unload_plan,
                   directory=self.mock_directory)

        mock_aws_creds = self.mock_s3_temp_base_loc.temporary_directory().__enter__().aws_creds()
        mock_access_key_id = mock_aws_creds.access_key
        mock_secret_key = mock_aws_creds.secret_key
        mock_token = mock_aws_creds.token
        expected_args = {
            'access_key_id': mock_access_key_id,
            'add_quotes': False,
            'delimiter': ',',
            'escape': True,
            'gzip': True,
            'manifest': True,
            'secret_access_key': mock_secret_key,
            'select': ('SELECT * FROM myschema.mytable',),
            'session_token': mock_token,
            'unload_location': self.mock_s3_temp_base_loc.temporary_directory().__enter__().url
        }
        mock_UnloadFromSelect.assert_called_with(**expected_args)
        self.assertEqual(456, rows)

    @patch('records_mover.db.redshift.unloader.UnloadFromSelect')
    @patch('records_mover.db.redshift.unloader.text')
    def test_unload(self,
                    mock_text,
                    mock_UnloadFromSelect):
        mock_text.side_effect = fake_text
        self.mock_records_unload_plan.processing_instructions.fail_if_dont_understand = True
        self.mock_records_unload_plan.processing_instructions.fail_if_cant_handle_hint = True
        self.mock_records_unload_plan.records_format.hints = bluelabs_format_hints
        self.mock_directory.scheme = 's3'
        self.mock_db_engine.execute.return_value.scalar.return_value = 456
        rows = self.redshift_db_driver.unloader().\
            unload(schema='myschema',
                   table='mytable',
                   unload_plan=self.mock_records_unload_plan,
                   directory=self.mock_directory)

        expected_args = {
            'access_key_id': 'fake_aws_id',
            'add_quotes': False,
            'delimiter': ',',
            'escape': True,
            'gzip': True,
            'manifest': True,
            'secret_access_key': 'fake_aws_secret',
            'select': ('SELECT * FROM myschema.mytable',),
            'session_token': 'fake_aws_token',
            'unload_location': 's3://mybucket/myparent/mychild/'
        }
        mock_UnloadFromSelect.assert_called_with(**expected_args)
        self.assertEqual(456, rows)

    @patch('records_mover.db.redshift.unloader.UnloadFromSelect')
    @patch('records_mover.db.redshift.unloader.text')
    def test_unload_christmas_tree_unsupported_options_with_fast_warns_1(self,
                                                                         mock_text,
                                                                         mock_UnloadFromSelect):
        mock_text.side_effect = fake_text
        self.mock_directory.scheme = 's3'
        with patch.object(driver_logger, 'warning') as mock_warning:
            self.mock_records_unload_plan.processing_instructions.fail_if_dont_understand = False
            self.mock_records_unload_plan.processing_instructions.fail_if_cant_handle_hint = False

            self.mock_records_unload_plan.records_format.hints = christmas_tree_format_1_hints
            self.mock_db_engine.execute.return_value.scalar.return_value = 456
            rows = self.redshift_db_driver.unloader().\
                unload(schema='myschema',
                       table='mytable',
                       unload_plan=self.mock_records_unload_plan,
                       directory=self.mock_directory)
            self.assertCountEqual(mock_warning.mock_calls,
                                  [call("Ignoring hint record-terminator = '\\x02'"),
                                   call("Ignoring hint quoting = 'nonnumeric'"),
                                   call("Ignoring hint datetimeformat = None"),
                                   call("Ignoring hint dateformat = None"),
                                   call("Ignoring hint header-row = True"),
                                   call("Ignoring hint compression = 'LZO'"),
                                   call("Did not understand these hints: header-row=True")])

        expected_args = {
            'access_key_id': 'fake_aws_id',
            'delimiter': '\x01',
            'escape': True,
            'manifest': True,
            'secret_access_key': 'fake_aws_secret',
            'select': ('SELECT * FROM myschema.mytable',),
            'session_token': 'fake_aws_token',
            'unload_location': 's3://mybucket/myparent/mychild/'
        }
        mock_UnloadFromSelect.assert_called_with(**expected_args)
        self.assertEqual(456, rows)

    @patch('records_mover.db.redshift.unloader.UnloadFromSelect')
    @patch('records_mover.db.redshift.unloader.text')
    def test_unload_christmas_tree_unsupported_options_with_fast_warns_2(self,
                                                                         mock_text,
                                                                         mock_UnloadFromSelect):
        mock_text.side_effect = fake_text
        self.mock_directory.scheme = 's3'
        with patch.object(driver_logger, 'warning') as mock_warning:
            self.mock_records_unload_plan.processing_instructions.fail_if_dont_understand = False
            self.mock_records_unload_plan.processing_instructions.fail_if_cant_handle_hint = False

            self.mock_records_unload_plan.records_format.hints = christmas_tree_format_2_hints
            self.mock_db_engine.execute.return_value.scalar.return_value = 456
            rows = self.redshift_db_driver.unloader().\
                unload(schema='myschema',
                       table='mytable',
                       unload_plan=self.mock_records_unload_plan,
                       directory=self.mock_directory)
            self.assertCountEqual(mock_warning.mock_calls,
                                  [call("Ignoring hint escape = '@'"),
                                   call("Ignoring hint doublequote = True"),
                                   call("Ignoring hint compression = 'BZIP'"),
                                   call("Ignoring hint datetimeformattz = 'HH:MI:SSOF YYYY-MM-DD'"),
                                   call("Ignoring hint record-terminator = '\\x02'"),
                                   call("Ignoring hint datetimeformat = None"),
                                   call("Ignoring hint dateformat = 'MM-DD-YYYY'")])

        expected_args = {
            'access_key_id': 'fake_aws_id',
            'add_quotes': True,
            'delimiter': '\x01',
            'manifest': True,
            'secret_access_key': 'fake_aws_secret',
            'select': ('SELECT * FROM myschema.mytable',),
            'session_token': 'fake_aws_token',
            'unload_location': 's3://mybucket/myparent/mychild/'
        }
        mock_UnloadFromSelect.assert_called_with(**expected_args)
        self.assertEqual(456, rows)
