from records_mover.cli.cli_job_context import CLIJobContext
from records_mover.creds.creds_via_lastpass import CredsViaLastPass
import unittest
from odictliteral import odict
from mock import patch, Mock

test_config_schema = {
    "type": "object",
    "properties": odict[
        'sourceType': {
            'enum': ['url_file', 'github_file']
        },
        'url': {
            'type': 'string'
        },
        'fire_phasers': {
            'type': 'boolean',
        },
        'phaserIntensity': {
            'type': 'integer',
            'default': 50
        },
        'a': {
            'type': 'object',
            'properties': odict[
                'b': {
                    'type': 'string',
                }
            ]
        },
        'outputType': {
            'enum': ['sql'],
            'default': 'sql',
        }
    ],
    "required": ["sourceType"]
}
args = [
        '--url', 'http://abc',
        '--fire_phasers',
        '--a.b', 'c',
        'url_file',
]


@patch.dict('os.environ', {
    'AWS_SECRET_ACCESS_KEY': 'aws secret key',
    'AWS_SESSION_TOKEN': 'aws session token',
    'AWS_ACCESS_KEY_ID': 'aws access key',
})
class TestCLIJobContext(unittest.TestCase):
    def test_logger_set(self):
        context = CLIJobContext('name',
                                creds=CredsViaLastPass(),
                                default_db_creds_name=None,
                                default_aws_creds_name=None,
                                config_json_schema=None)
        assert context.logger is not None

    def test_args_parsed(self):
        context = CLIJobContext('name',
                                config_json_schema=test_config_schema,
                                creds=CredsViaLastPass(),
                                default_db_creds_name=None,
                                default_aws_creds_name=None,
                                args=args)
        assert context.logger is not None

        config = context.request_config
        self.assertEqual(config, {
            'fire_phasers': True,
            'a': {
                'b': 'c'
            },
            'phaserIntensity': 50,
            'sourceType': 'url_file',
            'url': 'http://abc',
            'outputType': 'sql'
        })

    @patch('records_mover.base_job_context.db_driver')
    @patch('records_mover.base_job_context.UrlResolver')
    @patch('records_mover.base_job_context.boto3')
    @patch('records_mover.cli.cli_job_context.subprocess')
    @patch.dict('os.environ', {}, clear=True)
    def test_db_driver_with_guessed_bucket_url(self,
                                               mock_subprocess,
                                               mock_boto3,
                                               mock_UrlResolver,
                                               mock_db_driver):
        mock_subprocess.check_output.return_value = b"s3://chrisp-scratch/"
        context = CLIJobContext('name',
                                creds=CredsViaLastPass(),
                                config_json_schema=test_config_schema,
                                default_db_creds_name=None,
                                default_aws_creds_name=None,
                                args=args)
        mock_db = Mock(name='db')

        driver = context.db_driver(mock_db)
        mock_url_resolver = mock_UrlResolver.return_value
        mock_url_resolver.directory_url.assert_called_with('s3://chrisp-scratch/')

        mock_session = mock_boto3.session.Session.return_value
        mock_boto3.session.Session.assert_called_with()
        mock_UrlResolver.assert_called_with(boto3_session=mock_session)
        mock_directory_url = mock_UrlResolver.return_value.directory_url
        mock_db_driver.assert_called_with(db=mock_db,
                                          url_resolver=context.url_resolver,
                                          s3_temp_base_loc=mock_directory_url.return_value)
        mock_subprocess.check_output.assert_called_with('scratch-s3-url')
        self.assertEqual(mock_db_driver.return_value, driver)

    @patch('records_mover.cli.cli_job_context.CredsViaLastPass')
    def test_creds(self, mock_CredsViaLastPass):
        context = CLIJobContext('name',
                                creds=mock_CredsViaLastPass.return_value,
                                default_db_creds_name=None,
                                default_aws_creds_name=None,
                                config_json_schema=test_config_schema,
                                args=args)
        self.assertEqual(mock_CredsViaLastPass.return_value, context.creds)

    @patch('records_mover.base_job_context.engine_from_db_facts')
    @patch('records_mover.cli.cli_job_context.CredsViaLastPass')
    def test_get_default_db_engine_from_name(self,
                                             mock_CredsViaLastPass,
                                             mock_engine_from_db_facts):
        context = CLIJobContext('name',
                                creds=mock_CredsViaLastPass.return_value,
                                default_db_creds_name='foo',
                                default_aws_creds_name=None,
                                config_json_schema=test_config_schema,
                                args=args)
        out = context.get_default_db_engine()
        self.assertEqual(out, mock_engine_from_db_facts.return_value)
