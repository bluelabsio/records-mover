from records_mover.db.vertica.vertica_db_driver import (VerticaDBDriver)
from records_mover.records.unload_plan import RecordsUnloadPlan
from records_mover.records.records_format import DelimitedRecordsFormat
from records_mover.records.processing_instructions import ProcessingInstructions
from mock import MagicMock, create_autospec, Mock, patch, call, ANY
from sqla_vertica_python.vertica_python import VerticaDialect
from .vertica_sql_expectations import fake_quote
from .base_test_vertica_db_driver import BaseTestVerticaDBDriver
from ...records.format_hints import (vertica_format_hints)


class TestVerticaDBDriverLoad(BaseTestVerticaDBDriver):
    def setUp(self):
        self.mock_db_engine = MagicMock()
        self.mock_db_engine.dialect = create_autospec(VerticaDialect)
        self.mock_db_engine.dialect.preparer.return_value.quote = fake_quote
        self.mock_db_engine.engine = self.mock_db_engine
        self.mock_s3_temp_base_loc = MagicMock(name='s3_temp_base_loc')
        self.mock_url_resolver = Mock(name='url_resolver')
        self.mock_s3_temp_base_loc.url = 's3://fakebucket/fakedir/fakesubdir/'
        self.vertica_db_driver = VerticaDBDriver(db=None,
                                                 s3_temp_base_loc=self.mock_s3_temp_base_loc,
                                                 url_resolver=self.mock_url_resolver,
                                                 db_conn=self.mock_db_engine)

        mock_records_unload_plan = create_autospec(RecordsUnloadPlan)
        mock_records_unload_plan.records_format = create_autospec(DelimitedRecordsFormat)
        mock_records_unload_plan.records_format.format_type = 'delimited'
        mock_records_unload_plan.records_format.variant = None
        mock_records_unload_plan.processing_instructions = ProcessingInstructions()
        self.mock_records_unload_plan = mock_records_unload_plan

        mock_records_load_plan = Mock()
        mock_records_load_plan.records_format = create_autospec(DelimitedRecordsFormat)
        mock_records_load_plan.processing_instructions = ProcessingInstructions()
        self.mock_records_load_plan = mock_records_load_plan

        mock_directory = Mock()

        mock_directory.loc.url = 's3://mybucket/myparent/mychild/'
        mock_directory.loc.aws_creds.return_value = Mock(name='aws creds')
        mock_directory.loc.aws_creds.return_value.access_key = 'fake_aws_id'
        mock_directory.loc.aws_creds.return_value.secret_key = 'fake_aws_secret'
        mock_directory.loc.aws_creds.return_value.token = None

        self.mock_directory = mock_directory

    @patch('records_mover.db.vertica.loader.vertica_import_sql')
    @patch('records_mover.db.vertica.loader.vertica_import_options')
    def test_load(self, mock_vertica_import_options, mock_vertica_import_sql):
        def fake_vertica_import_options(unhandled_hints, load_plan):
            unhandled_hints.clear()
            return {'my_fake_option': True}

        mock_vertica_import_options.side_effect = fake_vertica_import_options
        self.mock_records_load_plan.records_format.hints = vertica_format_hints
        self.mock_directory.scheme = 's3'
        mock_data_url = Mock(name='data_url')
        mock_loc = self.mock_url_resolver.file_url.return_value
        mock_loc.open = MagicMock(name='open')
        self.mock_directory.manifest_entry_urls.return_value = [mock_data_url]
        export_count = self.vertica_db_driver.loader().load(
            schema='myschema',
            table='mytable',
            load_plan=self.mock_records_load_plan,
            directory=self.mock_directory)
        load_call = call(mock_vertica_import_sql.return_value, ANY)
        mock_cursor = self.mock_db_engine.raw_connection.return_value.cursor.return_value
        mock_cursor.copy.assert_has_calls([load_call])
        self.assertEqual(None, export_count)
