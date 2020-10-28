from mock import patch, Mock
from records_mover import Session
from records_mover.mover_types import PleaseInfer
import unittest


@patch('google.cloud.storage.Client')
@patch('google.auth.default')
@patch('records_mover.creds.base_creds.get_config')
@patch('records_mover.creds.base_creds.os')
class TestSession(unittest.TestCase):
    @patch('records_mover.db.connect.engine_from_db_facts')
    def test_get_db_engine(self,
                           mock_engine_from_db_facts,
                           mock_os,
                           mock_get_config,
                           mock_google_auth_default,
                           mock_google_cloud_storage_Client):
        mock_db_creds_name = Mock(name='db_creds_name')
        mock_creds = Mock(name='creds')
        session = Session()
        out = session.get_db_engine(mock_db_creds_name,
                                    creds_provider=mock_creds)
        mock_creds.db_facts.assert_called_with(mock_db_creds_name)
        mock_engine_from_db_facts.assert_called_with(mock_creds.db_facts.return_value)
        self.assertEqual(mock_engine_from_db_facts.return_value, out)

    @patch('records_mover.db.connect.engine_from_db_facts')
    def test_get_db_engine_use_sesssion_creds(self,
                                              mock_engine_from_db_facts,
                                              mock_os,
                                              mock_get_config,
                                              mock_google_auth_default,
                                              mock_google_cloud_storage_Client):
        mock_db_creds_name = Mock(name='db_creds_name')
        mock_creds = Mock(name='creds')
        session = Session(creds=mock_creds)
        out = session.get_db_engine(mock_db_creds_name)
        mock_creds.db_facts.assert_called_with(mock_db_creds_name)
        mock_engine_from_db_facts.assert_called_with(mock_creds.db_facts.return_value)
        self.assertEqual(mock_engine_from_db_facts.return_value, out)

    @patch('records_mover.session.UrlResolver')
    @patch('records_mover.db.factory.db_driver')
    def test_db_driver(self,
                       mock_db_driver,
                       mock_UrlResolver,
                       mock_os,
                       mock_get_config,
                       mock_google_auth_default,
                       mock_google_cloud_storage_Client):
        mock_creds = Mock(name='creds')
        mock_db = Mock(name='db')
        mock_url_resolver = mock_UrlResolver.return_value
        mock_scratch_s3_url = mock_creds.default_scratch_s3_url.return_value
        mock_s3_temp_base_loc = mock_url_resolver.directory_url.return_value
        session = Session(creds=mock_creds)
        out = session.db_driver(mock_db)
        self.assertEqual(out, mock_db_driver.return_value)
        mock_url_resolver.directory_url.assert_called_with(mock_scratch_s3_url)
        mock_db_driver.assert_called_with(db=mock_db,
                                          url_resolver=mock_url_resolver,
                                          s3_temp_base_loc=mock_s3_temp_base_loc)

    @patch('records_mover.session.CredsViaEnv')
    def test_itest_type_uses_creds_via_env(self,
                                           mock_CredsViaEnv,
                                           mock_os,
                                           mock_get_config,
                                           mock_google_auth_default,
                                           mock_google_cloud_storage_Client):
        mock_creds = mock_CredsViaEnv.return_value
        session = Session(session_type='itest')
        self.assertEqual(session.creds, mock_creds)

    @patch('records_mover.session.CredsViaEnv')
    def test_env_type_uses_creds_via_env(self,
                                         mock_CredsViaEnv,
                                         mock_os,
                                         mock_get_config,
                                         mock_google_auth_default,
                                         mock_google_cloud_storage_Client):
        mock_creds = mock_CredsViaEnv.return_value
        session = Session(session_type='env')
        self.assertEqual(session.creds, mock_creds)

    @patch('records_mover.creds.base_creds.db_facts_from_env')
    @patch('records_mover.db.connect.engine_from_db_facts')
    def test_get_default_db_engine_no_default(self,
                                              mock_engine_from_db_facts,
                                              mock_db_facts_from_env,
                                              mock_os,
                                              mock_get_config,
                                              mock_google_auth_default,
                                              mock_google_cloud_storage_Client):
        session = Session()
        self.assertEqual(session.get_default_db_engine(), mock_engine_from_db_facts.return_value)
        mock_db_facts_from_env.assert_called_with()
        mock_db_facts = mock_db_facts_from_env.return_value
        mock_engine_from_db_facts.assert_called_with(mock_db_facts)

    @patch('records_mover.creds.base_creds.db_facts_from_env')
    @patch('records_mover.db.connect.engine_from_db_facts')
    def test_get_default_db_facts_no_default(self,
                                             mock_engine_from_db_facts,
                                             mock_db_facts_from_env,
                                             mock_os,
                                             mock_get_config,
                                             mock_google_auth_default,
                                             mock_google_cloud_storage_Client):
        session = Session()
        self.assertEqual(session.creds.default_db_facts(), mock_db_facts_from_env.return_value)
        mock_db_facts_from_env.assert_called_with()

    @patch('records_mover.session.set_stream_logging')
    def test_set_stream_logging(self,
                                mock_set_stream_logging,
                                mock_os,
                                mock_get_config,
                                mock_google_auth_default,
                                mock_google_cloud_storage_Client):
        session = Session()
        mock_name = Mock(name='name')
        mock_level = Mock(name='level')
        mock_stream = Mock(name='stream')
        mock_fmt = Mock(name='fmt')
        mock_datefmt = Mock(name='datefmt')
        session.set_stream_logging(name=mock_name,
                                   level=mock_level,
                                   stream=mock_stream,
                                   fmt=mock_fmt,
                                   datefmt=mock_datefmt)
        mock_set_stream_logging.assert_called_with(name=mock_name,
                                                   level=mock_level,
                                                   stream=mock_stream,
                                                   fmt=mock_fmt,
                                                   datefmt=mock_datefmt)

    @patch('records_mover.session.set_stream_logging')
    def test_s3_url_from_get_config(self,
                                    mock_set_stream_logging,
                                    mock_os,
                                    mock_get_config,
                                    mock_google_auth_default,
                                    mock_google_cloud_storage_Client):
        mock_os.environ = {}
        mock_config_result = mock_get_config.return_value
        mock_config_result.config = {'aws': {'s3_scratch_url': 's3://foundit/'}}
        session = Session()
        self.assertEqual(session.creds.default_scratch_s3_url(), 's3://foundit/')

    @patch('records_mover.session.UrlResolver')
    def test_file_url(self,
                      mock_UrlResolver,
                      mock_os,
                      mock_get_config,
                      mock_google_auth_default,
                      mock_google_cloud_storage_Client):
        mock_credentials = Mock(name='credentials')
        mock_project = Mock(name='project')
        mock_google_auth_default.return_value = (mock_credentials, mock_project)
        session = Session(scratch_s3_url='s3://bar/baz')
        self.assertEqual(session.file_url('s3://bar/baz'),
                         mock_UrlResolver.return_value.file_url.return_value)

    @patch('boto3.session')
    def test_session_boto3_session_via_url_resolver_default(self,
                                                            mock_boto3_session,
                                                            mock_os,
                                                            mock_get_config,
                                                            mock_google_auth_default,
                                                            mock_google_cloud_storage_Client):
        session = Session()
        boto3_session = session.url_resolver.boto3_session_getter()
        self.assertEqual(boto3_session,
                         mock_boto3_session.Session.return_value)

    @patch('records_mover.session.CredsViaEnv')
    @patch('boto3.session')
    def test_session_boto3_session_via_url_resolver_specified(self,
                                                              mock_boto3_session,
                                                              mock_CredsViaEnv,
                                                              mock_os,
                                                              mock_get_config,
                                                              mock_google_auth_default,
                                                              mock_google_cloud_storage_Client):
        mock_default_aws_creds_name = Mock(name='default_aws_creds_name')
        session = Session(default_aws_creds_name=mock_default_aws_creds_name,
                          session_type='env')
        boto3_session = session.url_resolver.boto3_session_getter()
        self.assertEqual(boto3_session,
                         mock_CredsViaEnv.return_value.default_boto3_session.return_value)
        mock_CredsViaEnv.assert_called_with(default_db_creds_name=None,
                                            default_aws_creds_name=mock_default_aws_creds_name,
                                            default_gcp_creds_name=None,
                                            default_db_facts=PleaseInfer.token,
                                            default_boto3_session=PleaseInfer.token,
                                            default_gcp_creds=PleaseInfer.token,
                                            default_gcs_client=PleaseInfer.token,
                                            scratch_s3_url=PleaseInfer.token)
        mock_CredsViaEnv.return_value.default_boto3_session.assert_called()

    @patch('boto3.session')
    def test_session_boto3_session_via_url_resolver_cached(self,
                                                           mock_boto3_session,
                                                           mock_os,
                                                           mock_get_config,
                                                           mock_google_auth_default,
                                                           mock_google_cloud_storage_Client):
        session = Session()
        boto3_session = session.url_resolver.boto3_session_getter()
        self.assertEqual(boto3_session,
                         mock_boto3_session.Session.return_value)
        second_boto3_session = session.url_resolver.boto3_session_getter()
        self.assertEqual(second_boto3_session,
                         mock_boto3_session.Session.return_value)
        mock_boto3_session.Session.assert_called_once_with()

    def test_session_gcp_creds_via_url_resolver_default(self,
                                                        mock_os,
                                                        mock_get_config,
                                                        mock_google_auth_default,
                                                        mock_google_cloud_storage_Client):
        mock_credentials = Mock(name='credentials')
        mock_project = Mock(name='project')
        mock_google_auth_default.return_value = (mock_credentials, mock_project)
        session = Session()
        gcp_credentials = session.url_resolver.gcp_credentials_getter()
        self.assertEqual(gcp_credentials, mock_credentials)

    def test_session_gcp_creds_via_url_resolver_cached(self,
                                                       mock_os,
                                                       mock_get_config,
                                                       mock_google_auth_default,
                                                       mock_google_cloud_storage_Client):
        mock_credentials = Mock(name='credentials')
        mock_project = Mock(name='project')
        mock_google_auth_default.return_value = (mock_credentials, mock_project)
        session = Session()
        gcp_credentials = session.url_resolver.gcp_credentials_getter()
        self.assertEqual(gcp_credentials, mock_credentials)
        second_gcp_credentials = session.url_resolver.gcp_credentials_getter()
        self.assertEqual(second_gcp_credentials, mock_credentials)
        mock_google_auth_default.assert_called_once_with()

    def test_session_gcs_client_via_url_resolver_default(self,
                                                         mock_os,
                                                         mock_get_config,
                                                         mock_google_auth_default,
                                                         mock_google_cloud_storage_Client):
        mock_credentials = Mock(name='credentials')
        mock_project = Mock(name='project')
        mock_google_auth_default.return_value = (mock_credentials, mock_project)
        session = Session()
        gcs_client = session.url_resolver.gcs_client_getter()
        self.assertEqual(gcs_client, mock_google_cloud_storage_Client.return_value)

    def test_session_gcs_client_via_url_resolver_cached(self,
                                                        mock_os,
                                                        mock_get_config,
                                                        mock_google_auth_default,
                                                        mock_google_cloud_storage_Client):
        mock_credentials = Mock(name='credentials')
        mock_project = Mock(name='project')
        mock_google_auth_default.return_value = (mock_credentials, mock_project)
        session = Session()
        gcs_client = session.url_resolver.gcs_client_getter()
        self.assertEqual(gcs_client, mock_google_cloud_storage_Client.return_value)

        second_gcs_client = session.url_resolver.gcs_client_getter()
        self.assertEqual(second_gcs_client, mock_google_cloud_storage_Client.return_value)

        mock_google_auth_default.assert_called_once_with()
