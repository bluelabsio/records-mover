from records_mover.db.vertica.unloader import VerticaUnloader
from records_mover.db.errors import CredsDoNotSupportS3Export
from records_mover.records.records_format import DelimitedRecordsFormat
import unittest
from mock import patch, MagicMock, Mock, mock_open


class TestVerticaUnloaderNoAwsCreds(unittest.TestCase):
    def test_unload_to_s3_directory_with_token(self):
        mock_db = Mock(name='db')
        mock_s3_temp_base_loc = Mock(name='s3_temp_base_loc')
        mock_out = mock_db.execute.return_value
        mock_out.fetchall.return_value = []
        unloader = VerticaUnloader(db=mock_db, s3_temp_base_loc=mock_s3_temp_base_loc)
        mock_table = Mock(name='table')
        mock_unload_plan = Mock(name='unload_plan')
        mock_schema = Mock(name='schema')
        mock_directory = Mock(name='directory')
        mock_unload_plan.records_format = Mock(spec=DelimitedRecordsFormat)
        mock_unload_plan.records_format.hints = {}
        mock_unload_plan.records_format.format_type = 'delimited'

        with self.assertRaises(CredsDoNotSupportS3Export):
            unloader.unload_to_s3_directory(schema=mock_schema,
                                            table=mock_table,
                                            unload_plan=mock_unload_plan,
                                            directory=mock_directory)

    @patch("builtins.open", new_callable=mock_open)
    def test_unload_with_no_aws_creds(self,
                                      mock_open):
        mock_db = MagicMock(name='db')
        mock_s3_temp_base_loc = MagicMock(name='s3_temp_base_loc')
        mock_target_records_format = Mock(name='target_records_format', spec=DelimitedRecordsFormat)
        mock_target_records_format.hints = {}
        mock_connection = MagicMock(name='connection')
        mock_db.connect.return_value \
               .__enter__.return_value = mock_connection
        mock_out = mock_connection.execute.return_value
        mock_out.fetchall.return_value = ['awslib']
        unloader = VerticaUnloader(db=mock_db, s3_temp_base_loc=mock_s3_temp_base_loc)
        mock_table = Mock(name='table')
        mock_unload_plan = Mock(name='unload_plan')
        mock_unload_plan.records_format = Mock(spec=DelimitedRecordsFormat)
        mock_unload_plan.records_format.hints = {
        }
        mock_schema = Mock(name='schema')
        mock_directory = Mock(name='directory')
        mock_unload_plan.records_format = Mock(spec=DelimitedRecordsFormat)
        mock_unload_plan.records_format.hints = {}
        mock_unload_plan.records_format.format_type = 'delimited'

        mock_column = Mock(name='column')
        mock_db.dialect.get_columns.return_value = [mock_column]
        with self.assertRaises(NotImplementedError):
            unloader.unload(schema=mock_schema,
                            table=mock_table,
                            unload_plan=mock_unload_plan,
                            directory=mock_directory)

    def test_s3_export_available_false_no_awslib(self):
        mock_db = MagicMock(name='db')
        mock_s3_temp_base_loc = Mock(name='s3_temp_base_loc')
        mock_target_records_format = Mock(name='target_records_format', spec=DelimitedRecordsFormat)
        mock_target_records_format.hints = {}
        mock_connection = MagicMock(name='connection')
        mock_db.connect.return_value \
               .__enter__.return_value = mock_connection
        mock_out = mock_connection.execute.return_value
        mock_out.fetchall.return_value = []
        unloader = VerticaUnloader(db=mock_db, s3_temp_base_loc=mock_s3_temp_base_loc)
        self.assertEqual(False, unloader.s3_export_available())
