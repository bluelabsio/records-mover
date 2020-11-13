from records_mover.url.resolver import directory_url_ctors, file_url_ctors, UrlResolver
from records_mover.url.base import BaseFileUrl, BaseDirectoryUrl
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


class TestUrlResolver(unittest.TestCase):
    def tearDown(self):
        if 'dummy' in directory_url_ctors:
            del directory_url_ctors['dummy']
        if 'dummy' in file_url_ctors:
            del file_url_ctors['dummy']
        if 'needy' in directory_url_ctors:
            del directory_url_ctors['needy']
        if 'needy' in file_url_ctors:
            del file_url_ctors['needy']

    def setUp(self):
        self.mock_DummyFileUrl = Mock(name='DummyFileUrl', spec=BaseFileUrl)
        self.mock_DummyFileUrl.return_value = Mock(name='dummyfileurl',
                                                        spec=BaseFileUrl)

        self.mock_DummyDirectoryUrl = Mock(name='DummyDirectoryUrl')
        self.mock_DummyDirectoryUrl.return_value = Mock(name='dummydirectoryurl',
                                                        spec=BaseDirectoryUrl)
        self.mock_boto3_session = Mock(name='boto3_session')
        self.mock_gcs_client = Mock(name='gcs_client')
        self.mock_gcp_credentials = Mock(name='gcp_credentials')
        self.mock_gcp_project_id = Mock(name='gcp_project_id')
        self.resolver = UrlResolver(boto3_session_getter=lambda: self.mock_boto3_session,
                                    gcs_client_getter=lambda: self.mock_gcs_client,
                                    gcp_credentials_getter=lambda: self.mock_gcp_credentials,
                                    gcp_project_id=self.mock_gcp_project_id)
        file_url_ctors['dummy'] = self.mock_DummyFileUrl
        file_url_ctors['needy'] = NeedyFileUrl
        directory_url_ctors['dummy'] = self.mock_DummyDirectoryUrl

    def test_FileUrl(self):
        dummy_url = 'dummy://foo/bar/baz?a=b&d=f'
        file_url = self.resolver.file_url(dummy_url)
        self.mock_DummyFileUrl.assert_called_with('dummy://foo/bar/baz?a=b&d=f')
        self.assertEqual(self.mock_DummyFileUrl.return_value, file_url)

    def test_FileUrl_with_lots_of_needs(self):
        needy_url = 'needy://foo/bar/baz?a=b&d=f'
        file_url = self.resolver.file_url(needy_url)
        self.assertEqual(type(file_url), NeedyFileUrl)
        self.assertEqual(file_url.boto3_session, self.mock_boto3_session)
        self.assertEqual(file_url.gcs_client, self.mock_gcs_client)
        self.assertEqual(file_url.gcp_credentials, self.mock_gcp_credentials)

    def test_DirectoryUrl(self):
        dummy_url = 'dummy://foo/bar/?a=b&d=f'
        directory_url = self.resolver.directory_url(dummy_url)
        self.mock_DummyDirectoryUrl.assert_called_with('dummy://foo/bar/?a=b&d=f')
        self.assertEqual(self.mock_DummyDirectoryUrl.return_value, directory_url)
