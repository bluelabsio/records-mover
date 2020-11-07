import unittest
import google.auth.credentials
from typing import Iterable
from db_facts.db_facts_types import DBFacts
from unittest.mock import Mock, patch
from records_mover.creds.base_creds import BaseCreds
import boto3


class ExampleCredsSubclass(BaseCreds):
    def _gcp_creds(self,
                   gcp_creds_name: str,
                   scopes: Iterable[str]) -> 'google.auth.credentials.Credentials':
        mock_gcp_creds = Mock(name='gcp_creds')
        mock_gcp_creds.gcp_creds_name = gcp_creds_name
        mock_gcp_creds.scopes = scopes
        return mock_gcp_creds

    def db_facts(self, db_creds_name: str) -> DBFacts:
        mock_db_facts = Mock(name='db_facts')
        mock_db_facts.db_creds_name = db_creds_name
        return mock_db_facts

    def boto3_session(self, aws_creds_name: str) -> 'boto3.session.Session':
        assert aws_creds_name == 'my_default_creds'
        self.boto3_session_called = True
        return Mock(name='boto3_session')


class TestBaseCreds(unittest.TestCase):
    maxDiff = None

    def test_gcs(self):
        creds = ExampleCredsSubclass(default_db_creds_name=None,
                                     default_aws_creds_name=None,
                                     default_gcp_creds_name=None)
        mock_gcp_creds_name = Mock(name='gcp_creds_name')
        out = creds.gcs(mock_gcp_creds_name)
        self.assertEqual(out.gcp_creds_name, mock_gcp_creds_name)
        self.assertTupleEqual(out.scopes, (
            'https://www.googleapis.com/auth/devstorage.full_control',
            'https://www.googleapis.com/auth/devstorage.read_only',
            'https://www.googleapis.com/auth/devstorage.read_write'
        ))

    def test_default_default_db_facts_name_specified(self):
        creds = ExampleCredsSubclass(default_db_creds_name='my_db_creds',
                                     default_aws_creds_name=None,
                                     default_gcp_creds_name=None)
        out = creds.default_db_facts()
        self.assertEqual(out.db_creds_name, 'my_db_creds')

    def test_default_default_db_facts_name_specified_cached(self):
        creds = ExampleCredsSubclass(default_db_creds_name='my_db_creds',
                                     default_aws_creds_name=None,
                                     default_gcp_creds_name=None)
        out_1 = creds.default_db_facts()
        self.assertEqual(out_1.db_creds_name, 'my_db_creds')
        out_2 = creds.default_db_facts()
        self.assertEqual(out_2.db_creds_name, 'my_db_creds')
        self.assertIs(out_1, out_2)

    def test_default_default_gcs_creds_name_specified(self):
        mock_gcp_creds_name = Mock(name='gcp_creds_name')
        creds = ExampleCredsSubclass(default_db_creds_name=None,
                                     default_aws_creds_name=None,
                                     default_gcp_creds_name=mock_gcp_creds_name)
        out = creds.default_gcs_creds()
        self.assertEqual(out.gcp_creds_name, mock_gcp_creds_name)
        self.assertTupleEqual(out.scopes, (
            'https://www.googleapis.com/auth/devstorage.full_control',
            'https://www.googleapis.com/auth/devstorage.read_only',
            'https://www.googleapis.com/auth/devstorage.read_write'
        ))

    @patch('google.auth.default')
    def test_default_gcs_client_no_default_gcs_creds(self,
                                                     mock_google_auth_default):
        creds = ExampleCredsSubclass(default_db_creds_name=None,
                                     default_aws_creds_name=None,
                                     default_gcp_creds_name=None)
        from google.auth.exceptions import DefaultCredentialsError
        mock_google_auth_default.side_effect = DefaultCredentialsError
        out = creds.default_gcs_client()
        self.assertIsNone(out)

    @patch('google.auth.default')
    @patch('google.cloud.storage.Client')
    def test_default_gcs_client_not_configured(self,
                                               mock_Client,
                                               mock_google_auth_default):
        mock_gcp_creds = Mock(name='gcp_creds')
        creds = ExampleCredsSubclass(default_gcp_creds=mock_gcp_creds)
        mock_Client.side_effect = OSError
        out = creds.default_gcs_client()
        self.assertIsNone(out)

    @patch('records_mover.creds.base_creds.get_config')
    @patch('records_mover.creds.base_creds.os')
    def test_s3_scratch_bucket_via_prefix(self,
                                          mock_os,
                                          mock_get_config):
        mock_boto3_session = Mock(name='boto3_session')
        mock_get_config.return_value.config = {
            'aws': {
                's3_scratch_url_appended_with_iam_username': 's3://group_bucket/subdir/'
            }
        }
        mock_sts_client = mock_boto3_session.client.return_value
        mock_sts_client.get_caller_identity.return_value = {
            'Arn': 'arn:aws:iam::accountid:user/my.name'
        }
        creds = ExampleCredsSubclass(default_boto3_session=mock_boto3_session)
        out = creds.default_scratch_s3_url()
        self.assertEqual(out, 's3://group_bucket/subdir/my.name/')
        mock_get_config.assert_called_with('records_mover', 'bluelabs')

    @patch('records_mover.creds.base_creds.get_config')
    @patch('records_mover.creds.base_creds.os')
    def test_s3_scratch_bucket_via_prefix_assumed_role(self,
                                                       mock_os,
                                                       mock_get_config):
        mock_boto3_session = Mock(name='boto3_session')
        mock_get_config.return_value.config = {
            'aws': {
                's3_scratch_url_appended_with_iam_username': 's3://group_bucket/subdir/'
            }
        }
        mock_sts_client = mock_boto3_session.client.return_value
        mock_sts_client.get_caller_identity.return_value = {
            'Arn': 'arn:aws:sts::accountid:assumed-role/SomeAssumedRole/some-session-name'
        }
        creds = ExampleCredsSubclass(default_boto3_session=mock_boto3_session)
        out = creds.default_scratch_s3_url()
        self.assertIsNone(out)
        mock_get_config.assert_called_with('records_mover', 'bluelabs')

    @patch('records_mover.creds.base_creds.get_config')
    @patch('records_mover.creds.base_creds.os')
    def test_gcs_scratch_bucket_configured(self,
                                           mock_os,
                                           mock_get_config):
        mock_get_config.return_value.config = {
            'gcp': {
                'gcs_scratch_url': 'gs://group_bucket/subdir/'
            }
        }
        creds = ExampleCredsSubclass()
        out = creds.default_scratch_gcs_url()
        self.assertEqual(out, 'gs://group_bucket/subdir/')
        mock_get_config.assert_called_with('records_mover', 'bluelabs')

    @patch('records_mover.creds.base_creds.get_config')
    @patch('records_mover.creds.base_creds.os')
    def test_gcs_scratch_bucket_not_configured(self,
                                               mock_os,
                                               mock_get_config):
        mock_get_config.return_value.config = {
            'gcp': {}
        }
        creds = ExampleCredsSubclass()
        out = creds.default_scratch_gcs_url()
        self.assertIsNone(out)
        mock_get_config.assert_called_with('records_mover', 'bluelabs')

    @patch('records_mover.creds.base_creds.get_config')
    @patch('records_mover.creds.base_creds.os')
    def test_s3_scratch_bucket_via_prefix_no_boto3_session(self,
                                                           mock_os,
                                                           mock_get_config):
        mock_boto3_session = None
        mock_get_config.return_value.config = {
            'aws': {
                's3_scratch_url_appended_with_iam_username': 's3://group_bucket/subdir/'
            }
        }
        creds = ExampleCredsSubclass(default_boto3_session=mock_boto3_session)
        out = creds.default_scratch_s3_url()
        self.assertIsNone(out)
        mock_get_config.assert_called_with('records_mover', 'bluelabs')

    @patch('records_mover.creds.base_creds.get_config')
    @patch('records_mover.creds.base_creds.os')
    def test_s3_scratch_bucket_no_config_file(self,
                                              mock_os,
                                              mock_get_config):
        mock_boto3_session = None
        mock_get_config.return_value.config = {}
        creds = ExampleCredsSubclass(default_boto3_session=mock_boto3_session)
        out = creds.default_scratch_s3_url()
        self.assertIsNone(out)
        mock_get_config.assert_called_with('records_mover', 'bluelabs')

    @patch('records_mover.creds.base_creds.get_config')
    @patch('records_mover.creds.base_creds.os')
    def test_gcs_scratch_bucket_no_config_file(self,
                                               mock_os,
                                               mock_get_config):
        mock_boto3_session = None
        mock_get_config.return_value.config = {}
        creds = ExampleCredsSubclass(default_boto3_session=mock_boto3_session)
        out = creds.default_scratch_gcs_url()
        self.assertIsNone(out)
        mock_get_config.assert_called_with('records_mover', 'bluelabs')

    @patch('records_mover.creds.base_creds.os')
    def test_gcs_scratch_bucket_no_env(self,
                                       mock_os):
        mock_os.environ = {}
        creds = ExampleCredsSubclass()
        out = creds.default_scratch_gcs_url()
        self.assertIsNone(out)

    @patch('records_mover.creds.base_creds.os')
    def test_gcs_scratch_bucket_env_set(self,
                                        mock_os):
        mock_os.environ = {
            'SCRATCH_GCS_URL': 'gs://whatever/'
        }
        creds = ExampleCredsSubclass()
        out = creds.default_scratch_gcs_url()
        self.assertEqual(out, 'gs://whatever/')

    @patch('records_mover.creds.base_creds.get_config')
    @patch('records_mover.creds.base_creds.os')
    def test_default_boto3_session_with_default_creds_name(self,
                                                           mock_os,
                                                           mock_get_config):
        mock_get_config.return_value.config = {}
        creds = ExampleCredsSubclass(default_aws_creds_name='my_default_creds')
        creds.default_boto3_session()
        self.assertTrue(creds.boto3_session_called)
