import unittest
from records_mover.db.vertica.vertica_db_driver import (VerticaDBDriver)
from records_mover.records.unload_plan import RecordsUnloadPlan
from records_mover.records.records_format import DelimitedRecordsFormat
from records_mover.records.processing_instructions import ProcessingInstructions
from mock import MagicMock, create_autospec, Mock, patch
from sqla_vertica_python.vertica_python import VerticaDialect
from .vertica_sql_expectations import fake_quote


class BaseTestVerticaDBDriver(unittest.TestCase):
    def setUp(self):
        self.mock_db_engine = MagicMock()
        self.mock_db_engine.dialect = create_autospec(VerticaDialect)
        self.mock_db_engine.dialect.preparer.return_value.quote = fake_quote
        self.mock_db_engine.engine = self.mock_db_engine
        self.mock_s3_temp_base_loc = MagicMock(name='s3_temp_base_loc')
        self.mock_url_resolver = Mock(name='url_resolver')
        self.mock_directory_url = self.mock_url_resolver.directory_url
        self.mock_s3_temp_base_loc.url = 's3://fakebucket/fakedir/fakesubdir/'
        with patch('records_mover.db.vertica.vertica_db_driver.VerticaLoader') \
                as mock_VerticaLoader:
            self.vertica_db_driver = VerticaDBDriver(db=None,
                                                     s3_temp_base_loc=self.mock_s3_temp_base_loc,
                                                     url_resolver=self.mock_url_resolver,
                                                     db_conn=self.mock_db_engine)
            self.mock_VerticaLoader = mock_VerticaLoader
            self.mock_vertica_loader = mock_VerticaLoader.return_value

        mock_records_unload_plan = create_autospec(RecordsUnloadPlan)
        mock_records_unload_plan.records_format = create_autospec(DelimitedRecordsFormat)
        mock_records_unload_plan.records_format.format_type = 'delimited'
        mock_records_unload_plan.records_format.variant = None
        mock_records_unload_plan.processing_instructions = ProcessingInstructions()
        self.mock_records_unload_plan = mock_records_unload_plan

        mock_records_load_plan = Mock()
        mock_records_load_plan.processing_instructions = ProcessingInstructions()
        self.mock_records_load_plan = mock_records_load_plan

        mock_directory = Mock()

        mock_directory.loc.url = 's3://mybucket/myparent/mychild/'
        mock_directory.loc.aws_creds.return_value = Mock(name='aws creds')
        mock_directory.loc.aws_creds.return_value.access_key = 'fake_aws_id'
        mock_directory.loc.aws_creds.return_value.secret_key = 'fake_aws_secret'
        mock_directory.loc.aws_creds.return_value.token = None

        self.mock_directory = mock_directory
