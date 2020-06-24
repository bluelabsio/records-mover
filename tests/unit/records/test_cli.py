import unittest
from mock import patch, call, Mock
from records_mover.records.cli import main


@patch('google.cloud.storage.Client')
@patch('google.auth.default')
@patch('records_mover.records.cli.argparse')
@patch('records_mover.records.cli.method_to_json_schema')
@patch('records_mover.records.cli.run_records_mover_job')
@patch('records_mover.records.cli.JobConfigSchemaAsArgsParser')
@patch('records_mover.records.cli.arguments_output_to_config')
@patch('records_mover.session.get_config')
class TestCLI(unittest.TestCase):
    def test_main(self,
                  mock_get_config,
                  mock_arguments_output_to_config,
                  mock_JobConfigSchemaAsArgsParser,
                  mock_run_records_mover_job,
                  mock_method_to_json_schema,
                  mock_argparse,
                  mock_google_auth_default,
                  mock_google_cloud_storage_Client):
        mock_credentials = Mock(name='credentials')
        mock_project = Mock(name='project')
        mock_google_auth_default.return_value = (mock_credentials, mock_project)
        mock_parser = mock_argparse.ArgumentParser.return_value
        mock_subparsers = mock_parser.add_subparsers.return_value
        main()
        # pick an example
        mock_subparsers.add_parser.assert_has_calls([call('table2recordsdir',
                                                          help='Copy from table to recordsdir')])
        mock_parser.parse_args.assert_called_with()
