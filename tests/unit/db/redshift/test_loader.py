import unittest
import sqlalchemy
from records_mover.db.redshift.loader import RedshiftLoader
from records_mover.records.records_format import DelimitedRecordsFormat
from mock import patch, Mock, MagicMock


class TestRedshiftLoader(unittest.TestCase):
    def setUp(self):
        self.mock_db = Mock(name='db')
        self.mock_meta = Mock(name='meta')
        self.s3_temp_base_loc = MagicMock(name='s3_temp_base_loc')

        self.redshift_loader =\
            RedshiftLoader(db=self.mock_db,
                           meta=self.mock_meta,
                           s3_temp_base_loc=self.s3_temp_base_loc)

    @patch('records_mover.db.redshift.loader.redshift_copy_options')
    @patch('records_mover.db.redshift.loader.ProcessingInstructions')
    @patch('records_mover.db.redshift.loader.RecordsLoadPlan')
    def test_can_load_this_format_true(self,
                                       mock_RecordsLoadPlan,
                                       mock_ProcessingInstructions,
                                       mock_redshift_copy_options):
        mock_processing_instructions = mock_ProcessingInstructions.return_value
        mock_source_records_format = Mock(name='source_records_format',
                                          spec=DelimitedRecordsFormat)
        mock_source_records_format.hints = {}

        mock_load_plan = mock_RecordsLoadPlan.return_value
        mock_load_plan.records_format = Mock(name='records_format',
                                                  spec=DelimitedRecordsFormat)
        mock_load_plan.records_format.hints = {}
        mock_load_plan.processing_instructions = mock_processing_instructions

        out = self.redshift_loader.can_load_this_format(mock_source_records_format)
        mock_ProcessingInstructions.assert_called_with()
        mock_RecordsLoadPlan.\
            assert_called_with(records_format=mock_source_records_format,
                               processing_instructions=mock_processing_instructions)
        mock_redshift_copy_options.\
            assert_called_with(set(),
                               mock_load_plan.records_format.validate.return_value,
                               mock_processing_instructions.fail_if_cant_handle_hint,
                               mock_processing_instructions.fail_if_row_invalid,
                               mock_processing_instructions.max_failure_rows)
        self.assertEqual(True, out)

    def test_known_supported_records_formats_for_load(self):
        out = self.redshift_loader.known_supported_records_formats_for_load()
        self.assertEqual(out, [
            DelimitedRecordsFormat(variant='csv',
                                   hints={
                                       'dateformat': 'YYYY-MM-DD',
                                       'timeonlyformat': 'HH24:MI:SS',
                                       'datetimeformat': 'YYYY-MM-DD HH:MI:SS',
                                       'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
                                   }),
            DelimitedRecordsFormat(variant='bigquery'),
            DelimitedRecordsFormat(variant='csv'),
            DelimitedRecordsFormat(variant='bluelabs', hints={'quoting': 'all'}),
            DelimitedRecordsFormat(variant='bluelabs'),
        ])

    @patch('records_mover.db.redshift.loader.CopyCommand')
    @patch('records_mover.db.redshift.loader.complain_on_unhandled_hints')
    @patch('records_mover.db.redshift.loader.redshift_copy_options')
    @patch('records_mover.db.redshift.loader.Table')
    def test_load_non_s3(self,
                         mock_Table,
                         mock_redshift_copy_options,
                         mock_complain_on_unhandled_hints,
                         mock_CopyCommand):
        mock_schema = Mock(name='schema')
        mock_table = Mock(name='table')
        mock_load_plan = Mock(name='load_plan')
        mock_load_plan.records_format = Mock(name='records_format', spec=DelimitedRecordsFormat)
        mock_load_plan.records_format.hints = {}
        mock_directory = Mock(name='directory')
        mock_directory.scheme = 'mumble'

        mock_temp_s3_loc = self.s3_temp_base_loc.temporary_directory().__enter__()
        mock_s3_directory = mock_directory.copy_to.return_value
        mock_s3_directory.scheme = 's3'

        mock_aws_creds = mock_s3_directory.loc.aws_creds.return_value
        mock_redshift_options = {'abc': 123}
        mock_redshift_copy_options.return_value = mock_redshift_options
        mock_copy = mock_CopyCommand.return_value
        mock_s3_directory.loc.url = 's3://foo/bar/baz/'
        self.redshift_loader.load(schema=mock_schema,
                                  table=mock_table,
                                  load_plan=mock_load_plan,
                                  directory=mock_directory)
        mock_directory.copy_to.assert_called_with(mock_temp_s3_loc)
        mock_to = mock_Table.return_value
        mock_Table.assert_called_with(mock_table, self.mock_meta, schema=mock_schema)
        mock_CopyCommand.\
            assert_called_with(to=mock_to, data_location=mock_s3_directory.loc.url + '_manifest',
                               access_key_id=mock_aws_creds.access_key,
                               secret_access_key=mock_aws_creds.secret_key,
                               session_token=mock_aws_creds.token,
                               manifest=True,
                               region=mock_s3_directory.loc.region,
                               empty_as_null=True,
                               abc=123)
        self.mock_db.execute.assert_called_with(mock_copy)

    @patch('records_mover.db.redshift.loader.CopyCommand')
    @patch('records_mover.db.redshift.loader.complain_on_unhandled_hints')
    @patch('records_mover.db.redshift.loader.redshift_copy_options')
    @patch('records_mover.db.redshift.loader.Table')
    def test_load_load_error(self,
                             mock_Table,
                             mock_redshift_copy_options,
                             mock_complain_on_unhandled_hints,
                             mock_CopyCommand):
        mock_schema = Mock(name='schema')
        mock_table = Mock(name='table')
        mock_load_plan = Mock(name='load_plan')
        mock_load_plan.records_format = Mock(name='records_format', spec=DelimitedRecordsFormat)
        mock_load_plan.records_format.hints = {}
        mock_directory = Mock(name='directory')
        mock_directory.scheme = 'mumble'

        mock_s3_directory = mock_directory.copy_to.return_value
        mock_s3_directory.scheme = 's3'

        mock_redshift_options = {'abc': 123}
        mock_redshift_copy_options.return_value = mock_redshift_options
        mock_copy = mock_CopyCommand.return_value
        mock_s3_directory.loc.url = 's3://foo/bar/baz/'

        def db_execute(command):
            if command == 'SELECT pg_backend_pid();':
                mock_result_proxy = Mock(name='result_proxy')
                mock_result_proxy.scalar.return_value = 123
                return mock_result_proxy
            elif command == mock_copy:
                raise sqlalchemy.exc.InternalError(command, {}, '')
            raise NotImplementedError(command)

        self.mock_db.execute.side_effect = db_execute
        with self.assertRaises(sqlalchemy.exc.InternalError):
            self.redshift_loader.load(schema=mock_schema,
                                      table=mock_table,
                                      load_plan=mock_load_plan,
                                      directory=mock_directory)
