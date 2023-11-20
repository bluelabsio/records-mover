from .base_test_redshift_db_driver import BaseTestRedshiftDBDriver
from ...records.format_hints import bluelabs_format_hints
from records_mover.records import DelimitedRecordsFormat
from mock import patch


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
        self.mock_records_unload_plan.records_format =\
            DelimitedRecordsFormat(variant='bluelabs',
                                   hints=bluelabs_format_hints)
        self.mock_directory.scheme = 'mumble'
        self.mock_db_engine.connect.return_value \
            .execute.return_value \
            .scalar.return_value = 456
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
            'select': ('SELECT * \nFROM myschema.mytable',),
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
        self.mock_records_unload_plan.records_format =\
            DelimitedRecordsFormat(variant='bluelabs',
                                   hints=bluelabs_format_hints)
        self.mock_directory.scheme = 's3'
        self.mock_db_engine.connect.return_value \
            .execute.return_value \
            .scalar.return_value = 456
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
            'select': ('SELECT * \nFROM myschema.mytable',),
            'session_token': 'fake_aws_token',
            'unload_location': 's3://mybucket/myparent/mychild/'
        }
        mock_UnloadFromSelect.assert_called_with(**expected_args)
        self.assertEqual(456, rows)
