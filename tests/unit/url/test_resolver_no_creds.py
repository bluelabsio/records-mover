from records_mover.url.resolver import file_url_ctors, UrlResolver
from records_mover.url.base import BaseFileUrl
from mock import Mock
import unittest


class NeedyFileUrl(BaseFileUrl):
    def __init__(self,
                 url,
                 boto3_session,
                 gcs_client,
                 gcp_credentials):
        self.boto3_session = boto3_session
        self.gcs_client = gcs_client
        self.gcp_credentials = gcp_credentials


class TestUrlResolverNoCreds(unittest.TestCase):
    def test_NeedyFileUrl_with_no_boto3(self):
        mock_gcs_client = Mock(name='gcs_client')
        mock_gcp_credentials = Mock(name='gcp_credentials')
        resolver = UrlResolver(boto3_session_getter=lambda: None,
                               gcs_client_getter=lambda: mock_gcs_client,
                               gcp_credentials_getter=lambda: mock_gcp_credentials)
        file_url_ctors['needy'] = NeedyFileUrl
        needy_url = 'needy://foo/bar/baz?a=b&d=f'
        with self.assertRaises(EnvironmentError):
            resolver.file_url(needy_url)

    def test_NeedyFileUrl_with_no_gcs_client(self):
        mock_boto3_session = Mock(name='boto3_session')
        mock_gcp_credentials = Mock(name='gcp_credentials')
        resolver = UrlResolver(boto3_session_getter=lambda: mock_boto3_session,
                               gcs_client_getter=lambda: None,
                               gcp_credentials_getter=lambda: mock_gcp_credentials)
        file_url_ctors['needy'] = NeedyFileUrl
        needy_url = 'needy://foo/bar/baz?a=b&d=f'
        with self.assertRaises(EnvironmentError):
            resolver.file_url(needy_url)

    def test_NeedyFileUrl_with_no_gcp_credentials(self):
        mock_gcs_client = Mock(name='gcs_client')
        mock_boto3_session = Mock(name='boto3_session')
        resolver = UrlResolver(boto3_session_getter=lambda: mock_boto3_session,
                               gcs_client_getter=lambda: mock_gcs_client,
                               gcp_credentials_getter=lambda: None)
        file_url_ctors['needy'] = NeedyFileUrl
        needy_url = 'needy://foo/bar/baz?a=b&d=f'
        with self.assertRaises(EnvironmentError):
            resolver.file_url(needy_url)
