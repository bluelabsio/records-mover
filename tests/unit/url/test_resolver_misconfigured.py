from records_mover.url.resolver import file_url_ctors, directory_url_ctors, UrlResolver
from records_mover.url.base import BaseFileUrl, BaseDirectoryUrl
from mock import Mock
import unittest


class SimpleFileUrl(BaseFileUrl):
    def __init__(self, url):
        pass


class SimpleDirectoryUrl(BaseDirectoryUrl):
    def __init__(self, url):
        pass


class TestUrlResolverMisconfigured(unittest.TestCase):
    def tearDown(self):
        if 'simple' in directory_url_ctors:
            del directory_url_ctors['simple']
        if 'simple' in file_url_ctors:
            del file_url_ctors['simple']

    def test_file_url_misconfigured(self):
        resolver = UrlResolver(boto3_session_getter=lambda: None,
                               gcs_client_getter=lambda: None,
                               gcp_credentials_getter=lambda: None)
        directory_url_ctors['simple'] = SimpleFileUrl
        simple_url = 'simple://foo/bar/baz?a=b&d=f'
        with self.assertRaises(TypeError):
            resolver.directory_url(simple_url)

    def test_directory_url_misconfigured(self):
        resolver = UrlResolver(boto3_session_getter=lambda: None,
                               gcs_client_getter=lambda: None,
                               gcp_credentials_getter=lambda: None)
        file_url_ctors['simple'] = SimpleDirectoryUrl
        simple_url = 'simple://foo/bar/baz?a=b&d=f'
        with self.assertRaises(TypeError):
            resolver.file_url(simple_url)
