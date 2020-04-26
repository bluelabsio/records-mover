from records_mover.db.redshift.redshift_db_driver import RedshiftDBDriver
from records_mover.records.unload_plan import RecordsUnloadPlan
from records_mover.records.load_plan import RecordsLoadPlan
from records_mover.records.records_format import DelimitedRecordsFormat
from records_mover.records.processing_instructions import ProcessingInstructions
from mock import Mock, MagicMock, create_autospec
import unittest


def fake_text(s):
    return (s,)


class BaseTestRedshiftDBDriver(unittest.TestCase):
    def setUp(self):
        self.mock_db_engine = MagicMock(name='db_engine')
        self.mock_db_engine.engine = self.mock_db_engine
        self.mock_s3_temp_base_loc = MagicMock(name='s3_temp_base_loc')
        self.mock_s3_temp_base_loc.url = 's3://fakebucket/fakedir/fakesubdir/'
        self.mock_db_engine.dialect.\
            preparer.return_value.\
            quote.return_value.\
            __add__.return_value.\
            __add__.return_value = 'myschema.mytable'
        self.redshift_db_driver = RedshiftDBDriver(db=self.mock_db_engine,
                                                   s3_temp_base_loc=self.mock_s3_temp_base_loc)

        mock_records_unload_plan = create_autospec(RecordsUnloadPlan)
        mock_records_unload_plan.records_format = create_autospec(DelimitedRecordsFormat)
        mock_records_unload_plan.records_format.format_type = 'delimited'
        mock_records_unload_plan.records_format.variant = None
        mock_records_unload_plan.processing_instructions = create_autospec(ProcessingInstructions)
        self.mock_records_unload_plan = mock_records_unload_plan

        mock_records_load_plan = create_autospec(RecordsLoadPlan)
        mock_records_load_plan.records_format = create_autospec(DelimitedRecordsFormat)
        self.mock_records_load_plan = mock_records_load_plan

        mock_directory = Mock()
        mock_directory.scheme = 's3'

        mock_directory.loc.url = 's3://mybucket/myparent/mychild/'
        mock_directory.loc.aws_creds.return_value.secret_key = 'fake_aws_secret'
        mock_directory.loc.aws_creds.return_value.token = 'fake_aws_token'
        mock_directory.loc.aws_creds.return_value.access_key = 'fake_aws_id'

        self.mock_directory = mock_directory

    def load(self, hints, fail_if):
        processing_instructions = ProcessingInstructions()
        processing_instructions.fail_if_cant_handle_hint = fail_if
        processing_instructions.fail_if_dont_understand = fail_if
        processing_instructions.fail_if_row_invalid = fail_if
        self.mock_records_load_plan.records_format.hints = hints
        self.mock_records_load_plan.processing_instructions = processing_instructions
        return self.redshift_db_driver.loader_from_fileobj().\
            load(schema='myschema',
                 table='mytable',
                 load_plan=self.mock_records_load_plan,
                 directory=self.mock_directory)
